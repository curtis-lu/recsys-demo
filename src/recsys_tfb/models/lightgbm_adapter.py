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
from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter

# extract_Xy is imported lazily inside prepare_train_inputs (see method body).
# Importing it at module top creates a circular chain at io/__init__-load
# time: io → model_adapter_dataset → models → this file → io.extract →
# core.catalog → io.model_adapter_dataset (still mid-init).

logger = logging.getLogger(__name__)

# Route LightGBM's internal _log_info / _log_warning (including the
# log_evaluation callback's per-iteration metric output) through Python
# logging instead of the default print-to-stdout _DummyLogger. Process-wide
# side effect; safe to set once at module import.
lgb.register_logger(logger)


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


class LightGBMAdapter(ModelAdapter):
    """ModelAdapter wrapping LightGBM Booster."""

    def __init__(self) -> None:
        self._booster: lgb.Booster | None = None

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        params: dict,
        *,
        train_dataset: "lgb.Dataset | None" = None,
        val_dataset: "lgb.Dataset | None" = None,
    ) -> None:
        # `early_stopping_rounds <= 0` (or no val provided) → run the full
        # num_iterations with no early-stopping callback. Used by the
        # `refit_on_full` final-model strategy where best_iteration is fixed.
        num_iterations = params.pop("num_iterations", 500)
        early_stopping_rounds = params.pop("early_stopping_rounds", 50)
        # 0 = silent (existing behavior). Positive N prints val metric every N
        # boosting rounds. Popped before lgb.train so the booster's saved
        # params don't carry this non-native key.
        log_period = int(params.pop("log_period", 0))

        if train_dataset is None:
            train_dataset = lgb.Dataset(
                X_train, label=y_train, free_raw_data=False
            )

        has_val = val_dataset is not None or X_val is not None
        valid_sets: list[lgb.Dataset] = []
        valid_names: list[str] = []
        callbacks = [lgb.log_evaluation(period=log_period)]

        if has_val:
            if val_dataset is None:
                val_dataset = lgb.Dataset(
                    X_val, label=y_val, reference=train_dataset, free_raw_data=False
                )
            valid_sets = [val_dataset]
            valid_names = ["val"]
            if early_stopping_rounds and early_stopping_rounds > 0:
                callbacks.insert(
                    0, lgb.early_stopping(stopping_rounds=early_stopping_rounds)
                )

        self._booster = lgb.train(
            params,
            train_dataset,
            num_boost_round=num_iterations,
            valid_sets=valid_sets,
            valid_names=valid_names,
            callbacks=callbacks,
        )

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

    @staticmethod
    def _categorical_indices(preprocessor_metadata: dict):
        """Index positions of categorical columns within feature_columns.

        Returns None if no categoricals are present (lgb.Dataset accepts None).
        """
        feat_cols = preprocessor_metadata["feature_columns"]
        cat_cols = preprocessor_metadata.get("categorical_columns", [])
        idx = [feat_cols.index(c) for c in cat_cols if c in feat_cols]
        return idx or None

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
        is ``objective_cache_key`` ("lambdarank", "rank_xendcg" or "binary"
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
        ``core.group_utils.objective_drops_zero_positive_groups``.

        **The .bin carries no sample weights.** ``training.sample_weights``
        feeds ``model_version`` and nothing in this cache path, so a weight
        vector inside the binary would be served unchanged to a run configured
        with different weights (#318). Written beside each .bin instead is a
        ``*.weight_keys.parquet`` sidecar holding that binary's rows' weight-key
        columns, in the binary's own row order; the trial loop resolves today's
        table against it and ``set_weight``s the result
        (:meth:`LgbDatasetHandle.sample_weights`). A cached directory whose
        sidecar is missing, or was built for different
        ``training.sample_weight_keys``, is rebuilt rather than served.
        """
        # Lazy import: see module-top comment about circular-import chain.
        # core/__init__ pulls core.catalog -> io.model_adapter_dataset, which
        # is mid-init when this file loads via io/__init__; a top-level
        # `from recsys_tfb.core.logging import ...` here re-enters that cycle.
        from recsys_tfb.core.logging import log_data_volume

        from recsys_tfb.core.group_utils import (
            drop_zero_positive_groups,
            is_ranking_objective,
            objective_cache_key,
            objective_drops_zero_positive_groups,
            to_contiguous_groups,
        )

        objective = (
            parameters.get("training", {})
            .get("algorithm_params", {})
            .get("objective")
        )
        objective_key = objective_cache_key(objective)
        ranking = is_ranking_objective(objective)
        # train and train_dev take the SAME rule. train_dev is only the
        # early-stopping valid set today, but `final_model_strategy:
        # refit_on_full` concats it into the training matrix -- one rule is
        # correct under both strategies. The calibration set is deliberately
        # not filtered: calibration reads the whole score distribution.
        filter_zero_positive = objective_drops_zero_positive_groups(objective)
        filter_counts: dict = (
            {"objective": objective} if filter_zero_positive else {}
        )

        # Per-objective sub-path: the lgb-binary cache is NOT keyed by
        # model_version, so a binary built under one objective must never be
        # served for another. Why each ranking objective gets its own segment
        # even though their .bin are identical today: see the rationale in
        # group_utils.objective_cache_key.
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

        # PR2: enable native LightGBM categorical handling.
        # categorical_feature names columns by index; lgb uses Fisher / one-vs-rest
        # splits instead of treating int codes as ordered numerics.
        cat_idx = self._categorical_indices(preprocessor_metadata)

        # Real feature names (post-join feature_table column order, same order as
        # the numpy columns from extract_Xy). Baked into the .bin so the booster
        # trained on it reports real names in feature_importance() instead of
        # LightGBM's positional Column_N defaults. lgb persists these into the
        # binary; categorical_feature stays index-based (cat_idx) — the two are
        # independent.
        feat_names = list(preprocessor_metadata["feature_columns"])

        # Lazy import: see module-top comment about circular-import chain.
        from recsys_tfb.io.extract import weight_key_columns

        # Sample weights are deliberately NOT baked into the .bin: the cache
        # path does not mention training.sample_weights, so a baked vector
        # would be served to a later run configured with different weights and
        # nothing would say so (#318). What goes beside the binary is the key
        # columns the weights are resolved *from*; the trial loop resolves its
        # own table against them (steps/hpo_scoring.py).
        weight_keys = weight_key_columns(parameters)

        # feature_pre_filter=False at construct time: features with
        # <min_data_in_leaf samples per bin are NOT silently dropped from the
        # binned dataset. The pre-cache training path (numpy → lgb.Dataset built
        # by lgb.train at trial time) inherits feature_pre_filter=False from
        # trial params; the cached binary path must opt out explicitly to match.
        construct_params = {"feature_pre_filter": False}

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
            perm_tr, grp_tr = to_contiguous_groups(gid_tr)
            ds_train = lgb.Dataset(
                X_tr[perm_tr],
                label=y_tr[perm_tr],
                group=grp_tr,
                feature_name=feat_names,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_train", ds_train)
            ds_train.save_binary(str(train_bin))
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
            perm_dev, grp_dev = to_contiguous_groups(gid_dev)
            ds_dev = lgb.Dataset(
                X_dev[perm_dev],
                label=y_dev[perm_dev],
                group=grp_dev,
                reference=ds_train,
                feature_name=feat_names,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_dev", ds_dev)
            ds_dev.save_binary(str(dev_bin))
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
            ds_train = lgb.Dataset(
                X_tr,
                label=y_tr,
                feature_name=feat_names,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_train", ds_train)
            ds_train.save_binary(str(train_bin))
            # No filter and no permutation on this branch, so the rows are
            # already the binary's rows in the binary's order.
            _write_weight_keys(wk_tr, train_bin, weight_keys)
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            del X_tr, y_tr, wk_tr

            X_dev, y_dev, wk_dev = extract_Xy(
                train_dev_handle, preprocessor_metadata, parameters,
                with_weight_keys=True,
            )
            ds_dev = lgb.Dataset(
                X_dev,
                label=y_dev,
                reference=ds_train,
                feature_name=feat_names,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_dev", ds_dev)
            ds_dev.save_binary(str(dev_bin))
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
        """Access the underlying LightGBM Booster (for diagnostics and SHAP)."""
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        return self._booster


ADAPTER_REGISTRY["lightgbm"] = LightGBMAdapter
