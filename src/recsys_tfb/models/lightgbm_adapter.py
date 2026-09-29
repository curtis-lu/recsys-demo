"""LightGBM implementation of ModelAdapter."""

import json
import logging
import shutil
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np

from recsys_tfb.io.handles import (
    GROUP_FILTER_COUNTS_NAME,
    WEIGHT_KEYS_META,
    WEIGHT_ROWS_META,
    LgbDatasetHandle,
    ParquetHandle,
    weight_keys_sidecar,
)
from recsys_tfb.models.base import ADAPTER_REGISTRY, AlgorithmRules, ModelAdapter

# `io.extract`, `core.logging` and `core.group_utils` are imported inside the
# functions that use them, not up here. Anything under `core` runs
# `core/__init__`, which imports `core.catalog`, which imports
# `io.model_adapter_dataset` — the module that imports this one. A cold
# `import recsys_tfb.io.model_adapter_dataset` would then reach that module
# again half-built and fail (measured when `io/__init__` stopped re-exporting
# `ModelAdapterDataset`, ADR-0030 decision 15: with these three at the top,
# that one entry point raises ImportError and the others still load).

logger = logging.getLogger(__name__)

# Route LightGBM's internal _log_info / _log_warning (including the
# log_evaluation callback's per-iteration metric output) through Python
# logging instead of the default print-to-stdout _DummyLogger. Process-wide
# side effect; safe to set once at module import.
lgb.register_logger(logger)

#: LightGBM's objectives and metrics, as the framework reads them.
#:
#: **Ranking objectives.** ``lambdarank`` and ``rank_xendcg`` train on query
#: groups; every other objective scores rows one at a time.
#:
#: **Ranking metrics.** The eval metrics LightGBM accepts for a ranking
#: objective. Anything else (``binary_logloss``, say) makes ranking early
#: stopping silently meaningless, which is why A7 rejects it before the run.
#:
#: **Which objective drops zero-positive query groups: ``lambdarank`` and only
#: ``lambdarank``.** A query group whose labels are all zero yields no pair of
#: differing labels, so its lambdarank gradient contribution is *exactly* zero
#: — measured on LightGBM 4.6.0 with an all-zero-label group over 50 rounds:
#: 1 tree, 0 splits, zero variance in the predictions. Those rows cost
#: training time and teach nothing. ``rank_xendcg`` genuinely learns from them
#: and must keep every row: its target distribution
#: ``q_i = (2^y_i - g_i) / sum_j (2^y_j - g_j)`` draws a fresh random ``g``
#: each round, so an all-zero-label group becomes a *random* ranking to fit
#: rather than a flat one — the same data grew 50 trees, all with splits,
#: prediction std 0.045.
LIGHTGBM_RULES = AlgorithmRules(
    ranking_objectives=frozenset({"lambdarank", "rank_xendcg"}),
    ranking_metrics=frozenset({"ndcg", "map", "lambdarank"}),
    default_ranking_metric="ndcg",
    zero_positive_group_dropping_objectives=frozenset({"lambdarank"}),
)

#: The one construct param a Dataset may never be built without, in one place
#: for every Dataset this adapter builds, reads and trains on.
#:
#: ``feature_pre_filter=True`` (LightGBM's default) drops, at construct time,
#: features with fewer than ``min_data_in_leaf`` samples in any bin. The
#: cached ``.bin`` binaries are binned once and trained on by every trial, so
#: a filter keyed to the first ``min_data_in_leaf`` would decide which
#: features the whole search may split on; and a refit built with the default
#: would drop features the winning trial was allowed to split on — a
#: different model, reported under the search's hyperparameters, with nothing
#: raised. :meth:`LightGBMAdapter.train` also sets it on the training params,
#: because LightGBM refuses a constructed Dataset whose training params say
#: otherwise.
_CONSTRUCT_PARAMS = {"feature_pre_filter": False}

#: Keys of ``params`` that name the framework's round cap and early-stopping
#: patience. :meth:`LightGBMAdapter.train` takes both as arguments; a copy
#: inside ``params`` is dropped, because LightGBM would read ``num_iterations``
#: from ``params`` in preference to the round cap it is handed.
_FRAMEWORK_ROUND_KEYS = ("num_iterations", "early_stopping_rounds")


def _objective_cache_key(objective: str | None) -> str:
    """Sub-path segment isolating one objective's on-disk lgb binary.

    Every ranking objective gets its **own** segment: ``lgb/lambdarank/``,
    ``lgb/rank_xendcg/``, ``lgb/binary/``.

    The two ranking objectives no longer build the same rows: lambdarank
    drops zero-positive query groups and rank_xendcg keeps them (see
    :data:`LIGHTGBM_RULES`). The lgb cache is not keyed by ``model_version``,
    so a shared segment would silently serve a .bin built for the other
    objective's rows. The segment split landed one commit ahead of the row
    split (#314 before #315), so that collision was never live.

    The segment does **not** separate a .bin built before the row split from
    one built after — same objective, same path, different rows. That one is
    caught in :meth:`LightGBMAdapter.prepare_train_inputs` instead, by the
    presence of the counts sidecar, because it is a property of when the
    directory was written rather than of the objective naming it.

    Non-ranking objectives all map to ``"binary"``. That collision is
    deliberate: they build a byte-identical .bin (same X/y/weight, no group
    vector) with no divergence planned, and it keeps existing
    ``lgb/binary/`` dirs valid with no migration.

    Returned values are drawn from the ranking objectives plus the literal
    ``"binary"``, never arbitrary config text, so the segment is path-safe.
    """
    if LIGHTGBM_RULES.is_ranking_objective(objective):
        return objective
    return "binary"


def _feature_selection_subpath(parameters: dict, feature_columns: list[str]) -> str:
    """Cache sub-segment isolating a training-stage feature subset's .bin.

    Returns "" when ``training.feature_selection`` is inactive, so the lgb cache
    path stays ``lgb/<objective>/`` (byte-identical to pre-feature-selection
    runs; no migration). When active, returns ``fs_<hash8>`` keyed by the
    *surviving* ``feature_columns`` — different subsets get different ``.bin``
    files under the same base/train_variant/objective dir, which would
    otherwise collide and silently reuse a stale full-feature binary.
    """
    from recsys_tfb.models.feature_selection import feature_selection_exclude

    if not feature_selection_exclude(parameters):
        return ""
    import hashlib

    digest = hashlib.sha256("\n".join(feature_columns).encode()).hexdigest()[:8]
    return f"fs_{digest}"


def _write_weight_keys(frame, bin_path: Path, weight_keys: list[str]) -> None:
    """Persist one split's sample-weight key columns beside its ``.bin``.

    In the binary's own row order, so a later run resolves *its*
    ``training.sample_weights`` against exactly the rows that binary holds —
    the zero-positive filter and the group permutation are already baked into
    the order handed in here, and nothing re-derives them on the read side.

    Written even when no weight table is configured and even when the frame
    has no columns at all: presence is what tells ``prepare_train_inputs``
    that a cached directory was built by a version that keeps weights *out*
    of the ``.bin``. A build that skipped the file whenever weights happened
    to be inactive would make the marker mean "no weights were configured
    that time", which is unknowable later and would send every unweighted
    cache through a needless rebuild.

    The row count is recorded separately from the data because a frame with
    no columns — every configured key column missing from this model_input —
    writes as ``num_rows=0`` and reads back empty. See
    :data:`~recsys_tfb.io.handles.WEIGHT_ROWS_META` for what that costs.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(frame, preserve_index=False)
    table = table.replace_schema_metadata({
        **(table.schema.metadata or {}),
        WEIGHT_KEYS_META: json.dumps(list(weight_keys)).encode(),
        WEIGHT_ROWS_META: json.dumps(len(frame)).encode(),
    })
    pq.write_table(table, weight_keys_sidecar(str(bin_path)))


def _weight_keys_cache_gap(lgb_dir: Path, parameters: dict) -> str | None:
    """Why this cached directory cannot answer today's weight config, or ``None``.

    Two ways a hit would be wrong, and neither raises on its own:

    - **No sidecar.** The directory was written before weights moved out of
      the ``.bin`` (#318), so its binaries carry whichever weights that run
      was configured with. They cannot be re-weighted and there is nothing
      on disk to say what they hold.
    - **Different key columns.** ``training.sample_weight_keys`` changed, so
      the cached columns do not spell today's lookup key. The resolver's
      "weight-key column absent" backstop would quietly return all-ones —
      a whole search trained unweighted, reported as if weighted.

    A changed weight *table* is not a gap: same keys, new values, resolved
    fresh on every read. That is the entire point of caching the keys.
    """
    import pyarrow.parquet as pq

    from recsys_tfb.io.extract import weight_key_columns

    wanted = list(weight_key_columns(parameters))
    for name in ("train.bin", "train_dev.bin"):
        sidecar = Path(weight_keys_sidecar(str(lgb_dir / name)))
        if not sidecar.exists():
            return f"no weight-key sidecar for {name}"
        meta = (pq.read_schema(sidecar).metadata or {}).get(WEIGHT_KEYS_META)
        if meta is None:
            return f"{name} sidecar records no weight-key list"
        built_for = json.loads(meta)
        if built_for != wanted:
            return (
                f"{name} sidecar was built for weight keys {built_for}, "
                f"config asks for {wanted}"
            )
    return None


def _log_group_filter(split: str, counts: dict) -> None:
    """One line per split saying what the zero-positive filter removed."""
    logger.info(
        "lambdarank zero-positive filter [%s]: dropped %d/%d groups "
        "(%d/%d rows); %d groups / %d rows remain",
        split, counts["groups_dropped"], counts["groups_total"],
        counts["rows_dropped"], counts["rows_total"],
        counts["groups_kept"], counts["rows_kept"],
    )


def _log_single_label_groups(split: str, objective: str, y, group_ids) -> None:
    """One line per split: the share of query groups a ranking objective can
    draw no pair from (ADR-0025 decision H). Counted on the rows the binary
    holds — after lambdarank's zero-positive filter — so it describes what
    the objective actually trains on. Logged at build time only: a cache hit
    builds nothing and prints the line of the run that built the binary."""
    from recsys_tfb.core.group_utils import single_label_group_counts

    counts = single_label_group_counts(y, group_ids)
    total = counts["groups_total"]
    single = counts["groups_single_label"]
    logger.info(
        "%s single-label query groups [%s]: %d/%d groups (%.1f%%) hold one "
        "label only and give the objective no pair to compare",
        objective, split, single, total, 100.0 * single / total if total else 0.0,
    )


class LightGBMAdapter(ModelAdapter):
    """ModelAdapter wrapping LightGBM Booster."""

    rules = LIGHTGBM_RULES

    def __init__(self) -> None:
        self._booster: lgb.Booster | None = None

    def build_train_data(
        self,
        X: np.ndarray,
        y: np.ndarray,
        *,
        feature_names: list[str],
        categorical_features: list[str],
        group: np.ndarray | None = None,
        weight: np.ndarray | None = None,
        reference: "lgb.Dataset | None" = None,
    ) -> "lgb.Dataset":
        """An unconstructed ``lgb.Dataset`` over the arrays.

        Left unconstructed, as the refit's Dataset always was: ``lgb.train``
        then bins it with the training params merged in, so the refit's
        construction is unchanged by this method existing. A caller that
        saves it constructs it first.

        The feature names are baked in so the booster reports real names in
        ``feature_importance()`` instead of LightGBM's positional
        ``Column_N``. Categorical columns go in by index, which LightGBM uses
        for Fisher / one-vs-rest splits instead of treating the codes as
        ordered numbers; LightGBM takes ``None`` for "no categorical columns".
        """
        cat_idx = [
            feature_names.index(c) for c in categorical_features
            if c in feature_names
        ] or None
        return lgb.Dataset(
            X,
            label=y,
            weight=weight,
            group=group,
            reference=reference,
            feature_name=list(feature_names),
            categorical_feature=cat_idx,
            params=dict(_CONSTRUCT_PARAMS),
            free_raw_data=True,
        )

    def save_train_data(self, data: "lgb.Dataset", path: str) -> None:
        # A weight in the file would outlive the config that set it: reading
        # back with an all-ones vector does not replace it (set_weight maps
        # all-ones to None and leaves the stored field alone), so a later
        # unweighted run would train on the old weights under a model_version
        # that says unweighted (#318). get_weight() needs a constructed
        # Dataset; save_binary would construct it anyway.
        if data.construct().get_weight() is not None:
            raise ValueError(
                f"refusing to save training data with per-row weights to "
                f"{path}: weights are applied when the file is read "
                "(load_train_data), never stored in it."
            )
        data.save_binary(path)

    def load_train_data(
        self,
        path: str,
        *,
        weight: np.ndarray,
        reference: "lgb.Dataset | None" = None,
    ) -> "lgb.Dataset":
        """The ``.bin`` at ``path``, constructed, with ``weight`` set on it.

        The length check is the reason this is more than one line: LightGBM's
        own check lives in ``set_field``, and ``set_weight`` never gets there
        for an all-ones vector — it maps any array satisfying
        ``np.all(weight == 1)`` to ``None``, which a **length-zero** array
        satisfies vacuously. A weight vector that came out empty would be
        discarded in silence and the search would train unweighted, under a
        ``model_version`` keyed by the very ``sample_weights`` it ignored. A
        non-uniform vector of the wrong length does raise inside LightGBM;
        this covers the half that does not.

        Checked against the constructed binary's ``num_data()`` rather than
        against anything the weight-key sidecar records, because the sidecar
        is where a wrong length would come from — a self-consistent count
        cannot catch it.
        """
        data = lgb.Dataset(path, reference=reference, params=dict(_CONSTRUCT_PARAMS))
        data.set_weight(weight)
        data = data.construct()
        n_rows = data.num_data()
        if len(weight) != n_rows:
            raise ValueError(
                f"sample weights for {path} have {len(weight)} entries but the "
                f"binary holds {n_rows} rows; the weight-key sidecar does not "
                "describe this .bin. Clear the lgb cache directory so it is "
                "rebuilt."
            )
        return data

    def train(
        self,
        train_data: "lgb.Dataset",
        params: dict,
        *,
        num_iterations: int,
        early_stopping_rounds: int = 0,
        valid_data: "lgb.Dataset | None" = None,
    ) -> None:
        params = {
            k: v for k, v in params.items() if k not in _FRAMEWORK_ROUND_KEYS
        }
        # 0 = silent. Positive N prints the valid metric every N boosting
        # rounds. Popped before lgb.train so the booster's saved params do not
        # carry this non-native key.
        log_period = int(params.pop("log_period", 0))
        params.update(_CONSTRUCT_PARAMS)

        valid_sets: list[lgb.Dataset] = []
        valid_names: list[str] = []
        callbacks = [lgb.log_evaluation(period=log_period)]
        if valid_data is not None:
            valid_sets = [valid_data]
            valid_names = ["val"]
            if early_stopping_rounds and early_stopping_rounds > 0:
                callbacks.insert(
                    0, lgb.early_stopping(stopping_rounds=early_stopping_rounds)
                )

        self._booster = lgb.train(
            params,
            train_data,
            num_boost_round=num_iterations,
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=callbacks,
        )

    @property
    def best_iteration(self) -> int:
        if self._booster is None:
            raise RuntimeError("Model not trained. Call train() first.")
        return self._booster.best_iteration

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._booster is None:
            raise RuntimeError("Model not trained or loaded. Call train() or load() first.")
        return self._booster.predict(X)

    def feature_names(self) -> list[str] | None:
        if self._booster is None:
            return None
        return list(self._booster.feature_name())

    def save(self, filepath: str) -> None:
        if self._booster is None:
            raise RuntimeError("No model to save. Call train() first.")
        self._booster.save_model(filepath)

    def load(self, filepath: str) -> None:
        self._booster = lgb.Booster(model_file=filepath)

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        if kind not in ("split", "gain"):
            raise ValueError(f"kind must be 'split' or 'gain', got {kind!r}")
        names = self._booster.feature_name()
        importances = self._booster.feature_importance(importance_type=kind).astype(float)
        return dict(zip(names, importances))

    def log_to_mlflow(self) -> None:
        if self._booster is None:
            raise RuntimeError("No model to log.")
        mlflow.lightgbm.log_model(self._booster, name="model")

    def prepare_train_inputs(
        self,
        train_handle: ParquetHandle,
        train_dev_handle: ParquetHandle,
        preprocessor_metadata: dict,
        parameters: dict,
        cache_dir: str,
    ) -> tuple[LgbDatasetHandle, LgbDatasetHandle]:
        """Materialize lgb.Dataset binaries for train + train_dev.

        Skip-if-exists: returns handles without rebuilding when
        cache_dir/lgb/<objective>/_SUCCESS already exists, where <objective>
        is ``_objective_cache_key`` ("lambdarank", "rank_xendcg" or "binary"
        for anything non-ranking). On miss, builds train first
        (with binning), saves binary, then builds train_dev with
        reference=train so dev binning aligns to train. For a ranking
        objective each Dataset also carries the per-query group.

        Under ``objective: lambdarank`` both splits are also stripped of query
        groups holding no positive, and the counts are written next to the
        .bin as ``group_filter_counts.json`` so a later cache hit can still
        report them (:meth:`LgbDatasetHandle.group_filter_counts`). That file
        doubles as the marker that a cached .bin was built under the rule: a
        directory without it is rebuilt rather than served. Which objectives
        filter, and why it is derived rather than configured:
        :data:`LIGHTGBM_RULES`.

        **The .bin carries no sample weights.** ``training.sample_weights``
        feeds ``model_version`` and nothing in this cache path, so a weight
        vector inside the binary would be served unchanged to a run configured
        with different weights (#318). Written beside each .bin instead is a
        ``*.weight_keys.parquet`` sidecar holding that binary's rows' weight-key
        columns, in the binary's own row order; the HPO node resolves today's
        table against it (:meth:`LgbDatasetHandle.sample_weights`) and every
        trial hands the result to :meth:`load_train_data`. A cached directory whose
        sidecar is missing, or was built for different
        ``training.sample_weight_keys``, is rebuilt rather than served.
        """
        # Lazy import: see module-top comment about circular-import chain.
        from recsys_tfb.core.logging import log_data_volume

        from recsys_tfb.core.group_utils import (
            drop_zero_positive_groups,
            to_contiguous_groups,
        )

        objective = (
            parameters.get("training", {})
            .get("algorithm_params", {})
            .get("objective")
        )
        objective_key = _objective_cache_key(objective)
        ranking = self.rules.is_ranking_objective(objective)
        # train and train_dev take the SAME rule. train_dev is only the
        # early-stopping valid set today, but `final_model_strategy:
        # refit_on_full` concats it into the training matrix -- one rule is
        # correct under both strategies.
        filter_zero_positive = self.rules.objective_drops_zero_positive_groups(
            objective)
        filter_counts: dict = (
            {"objective": objective} if filter_zero_positive else {}
        )

        # Per-objective sub-path: the lgb-binary cache is NOT keyed by
        # model_version, so a binary built under one objective must never be
        # served for another. Why each ranking objective gets its own segment
        # even though their .bin are identical today: see the rationale in
        # _objective_cache_key.
        lgb_dir = Path(cache_dir) / "lgb" / objective_key
        # Training-stage feature selection: the binned .bin reflects the subset
        # feature set, but the cache dir is keyed by base/train_variant/objective
        # only — NOT by selection. Add a feature-hash sub-segment so a subset
        # binary never collides with (or silently reuses) the full-feature one
        # under the same train_variant. Empty when no selection -> unchanged.
        fs_sub = _feature_selection_subpath(
            parameters, list(preprocessor_metadata["feature_columns"])
        )
        if fs_sub:
            lgb_dir = lgb_dir / fs_sub
        success = lgb_dir / "_SUCCESS"
        train_bin = lgb_dir / "train.bin"
        dev_bin = lgb_dir / "train_dev.bin"

        # Two ways a binary can sit at exactly the path today's run wants and
        # still be the wrong thing to serve. Both are silent if served, and
        # neither is visible from the path — the lgb cache is keyed by
        # base/train_variant/objective and not by model_version, so nothing
        # else in the run would notice.
        stale = False
        if success.exists() and filter_zero_positive and not (
            lgb_dir / GROUP_FILTER_COUNTS_NAME
        ).exists():
            # A .bin from before the zero-positive filter existed (#315).
            # #314 already gave lambdarank its own segment, so this sits at
            # exactly the path today's run wants, carrying every row. Nothing
            # else would catch it: the one place the row count is reported
            # reads this very directory. Serving it would put HPO on the full
            # matrix while finalize_model's refit branch — which re-reads the
            # parquet — trains on the filtered one, under one set of
            # hyperparameters and with nothing raised. Rebuild instead.
            logger.warning(
                "lgb binary at %s predates the zero-positive group filter "
                "(no %s); it holds every row, which is not what "
                "objective=%s trains on. Rebuilding.",
                lgb_dir, GROUP_FILTER_COUNTS_NAME, objective,
            )
            stale = True

        weight_gap = (
            _weight_keys_cache_gap(lgb_dir, parameters)
            if success.exists() else None
        )
        if weight_gap:
            # The rows are fine; what is missing is the material to re-weight
            # them, and every weight-shaped alternative is silent. Serving a
            # pre-#318 binary trains on whatever weights *that* run was
            # configured with; falling back to all-ones trains unweighted.
            # Either way it is reported under today's model_version, which is
            # keyed by sample_weights and so claims the opposite.
            logger.warning(
                "lgb binary at %s cannot serve this run's sample weights (%s); "
                "rebuilding.", lgb_dir, weight_gap,
            )
            stale = True

        if success.exists() and not stale:
            logger.info("lgb binary cache hit at %s", lgb_dir)
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            return (
                LgbDatasetHandle(bin_path=str(train_bin), role="train"),
                LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
            )

        if lgb_dir.exists():
            logger.warning(
                "Partial lgb cache at %s, clearing before rebuild", lgb_dir
            )
            shutil.rmtree(lgb_dir)
        lgb_dir.mkdir(parents=True, exist_ok=True)

        # Real feature names, in the order of the numpy columns extract_Xy
        # returns (post-join feature_table order). build_train_data bakes them
        # into the .bin and turns the categorical names into indices.
        feat_names = list(preprocessor_metadata["feature_columns"])
        cat_cols = list(preprocessor_metadata.get("categorical_columns", []))

        # Lazy import: see module-top comment about circular-import chain.
        from recsys_tfb.io.extract import weight_key_columns

        # Sample weights are deliberately NOT baked into the .bin: the cache
        # path does not mention training.sample_weights, so a baked vector
        # would be served to a later run configured with different weights and
        # nothing would say so (#318). What goes beside the binary is the key
        # columns the weights are resolved *from*; each run resolves its own
        # table against them and applies it on read (load_train_data).
        weight_keys = weight_key_columns(parameters)

        if ranking:
            # Lazy import: see module-top comment about circular-import chain.
            from recsys_tfb.io.extract import extract_Xy_with_groups

            # Ranking objectives need a per-query group; rows must be ordered
            # so each group is one contiguous block. Build → save train, free
            # raw arrays, then dev with reference=train. save_binary persists
            # the group into the .bin so the trial/early-stopping loader gets
            # it back for free.
            #
            # `row_tr` rides through the filter as one more aligned array so
            # the surviving rows' *original* positions come back, and the
            # sidecar can be cut to the binary's rows by taking them — one
            # mask and one permutation, the same two the matrix went through.
            # A second derivation of "which rows survived, in what order"
            # would re-assign every weight to another row with nothing raised,
            # which is the failure drop_zero_positive_groups' varargs
            # signature exists to prevent.
            X_tr, y_tr, gid_tr, wk_tr = extract_Xy_with_groups(
                train_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            row_tr = np.arange(len(y_tr))
            if filter_zero_positive:
                (y_tr, gid_tr, X_tr, row_tr), counts = drop_zero_positive_groups(
                    y_tr, gid_tr, X_tr, row_tr)
                filter_counts["train"] = counts
                _log_group_filter("train", counts)
            _log_single_label_groups("train", objective, y_tr, gid_tr)
            perm_tr, grp_tr = to_contiguous_groups(gid_tr)
            ds_train = self.build_train_data(
                X_tr[perm_tr], y_tr[perm_tr], group=grp_tr,
                feature_names=feat_names, categorical_features=cat_cols,
            ).construct()
            log_data_volume(logger, "prepare.ds_train", ds_train)
            self.save_train_data(ds_train, str(train_bin))
            _write_weight_keys(wk_tr.take(row_tr[perm_tr]), train_bin, weight_keys)
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            del X_tr, y_tr, gid_tr, perm_tr, row_tr, wk_tr

            X_dev, y_dev, gid_dev, wk_dev = extract_Xy_with_groups(
                train_dev_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            row_dev = np.arange(len(y_dev))
            if filter_zero_positive:
                (y_dev, gid_dev, X_dev, row_dev), counts = (
                    drop_zero_positive_groups(y_dev, gid_dev, X_dev, row_dev))
                filter_counts["train_dev"] = counts
                _log_group_filter("train_dev", counts)
            _log_single_label_groups("train_dev", objective, y_dev, gid_dev)
            perm_dev, grp_dev = to_contiguous_groups(gid_dev)
            ds_dev = self.build_train_data(
                X_dev[perm_dev], y_dev[perm_dev], group=grp_dev,
                reference=ds_train,
                feature_names=feat_names, categorical_features=cat_cols,
            ).construct()
            log_data_volume(logger, "prepare.ds_dev", ds_dev)
            self.save_train_data(ds_dev, str(dev_bin))
            _write_weight_keys(wk_dev.take(row_dev[perm_dev]), dev_bin, weight_keys)
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            del X_dev, y_dev, gid_dev, perm_dev, row_dev, wk_dev, ds_train, ds_dev
        else:
            # Lazy import: see module-top comment about circular-import chain.
            from recsys_tfb.io.extract import extract_Xy

            # Extract → build → save train, then free raw arrays before dev is
            # read. Keeps ds_train alive (it's small) for dev's reference.
            X_tr, y_tr, wk_tr = extract_Xy(
                train_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            ds_train = self.build_train_data(
                X_tr, y_tr,
                feature_names=feat_names, categorical_features=cat_cols,
            ).construct()
            log_data_volume(logger, "prepare.ds_train", ds_train)
            self.save_train_data(ds_train, str(train_bin))
            # No filter and no permutation on this branch, so the rows are
            # already the binary's rows in the binary's order.
            _write_weight_keys(wk_tr, train_bin, weight_keys)
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            del X_tr, y_tr, wk_tr

            X_dev, y_dev, wk_dev = extract_Xy(
                train_dev_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            ds_dev = self.build_train_data(
                X_dev, y_dev, reference=ds_train,
                feature_names=feat_names, categorical_features=cat_cols,
            ).construct()
            log_data_volume(logger, "prepare.ds_dev", ds_dev)
            self.save_train_data(ds_dev, str(dev_bin))
            _write_weight_keys(wk_dev, dev_bin, weight_keys)
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            del X_dev, y_dev, wk_dev, ds_train, ds_dev

        if filter_counts:
            # Next to the .bin, and written before _SUCCESS: a rebuild that
            # died partway leaves no marker, so no later run can read these
            # counts as a description of a binary that was never finished.
            with open(lgb_dir / GROUP_FILTER_COUNTS_NAME, "w") as f:
                json.dump(filter_counts, f, indent=2)

        success.touch()
        logger.info(
            "lgb binary cache written: train=%s, train_dev=%s",
            train_bin, dev_bin,
        )

        return (
            LgbDatasetHandle(bin_path=str(train_bin), role="train"),
            LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
        )

    @property
    def booster(self) -> "lgb.Booster":
        """Access the underlying LightGBM Booster.

        For the diagnostics under ``diagnosis/model/`` only, until they reach
        the model through the adapter too (ADR-0030 decision 1, the
        diagnostics' share). The training pipeline itself never reads it.
        """
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        return self._booster


ADAPTER_REGISTRY["lightgbm"] = LightGBMAdapter
