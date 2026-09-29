"""LightGBM implementation of ModelAdapter.

Only what needs LightGBM: arrays to a ``lgb.Dataset`` and back to and from
disk, training, scoring, the model file. Which rows go into the cached
``.bin``, in what order, whether the cache is usable and why weights stay out
of it are the training pipeline's decisions and live in its
``prepare_train_inputs`` node (ADR-0030 decisions 1 and 10); the two rules
that node asks this library about — which objectives rank, which drop
zero-positive groups — are declared in :data:`LIGHTGBM_RULES`.

The training diagnostics' questions are answered here too (ADR-0030 decision
4): SHAP values through ``shap.TreeExplainer``, the tree table with
LightGBM's categorical split format decoded, split / gain importance. They
get plain arrays and a plain table back, never the booster.
"""

import logging
from typing import TYPE_CHECKING

import lightgbm as lgb
import mlflow
import numpy as np

if TYPE_CHECKING:
    import pandas as pd

from recsys_tfb.models.base import (
    ADAPTER_REGISTRY,
    TREE_STRUCTURE_COLUMNS,
    AlgorithmRules,
    ModelAdapter,
    UnsupportedCapability,
)

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
        construction is unchanged by this method existing. Saving it
        (:meth:`save_train_data`) constructs it.

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
                "describe this .bin. Clear its cache directory so it is "
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

    def log_to_mlflow(self) -> None:
        if self._booster is None:
            raise RuntimeError("No model to log.")
        mlflow.lightgbm.log_model(self._booster, name="model")

    # -- what the diagnostics ask of the model ----------------------------------

    def _fitted_booster(self) -> "lgb.Booster":
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        return self._booster

    def feature_attributions(
        self, X: np.ndarray, *, background: np.ndarray | None = None,
    ) -> np.ndarray:
        """SHAP values from ``shap.TreeExplainer``: tree-path-dependent with no
        ``background``, interventional against it otherwise.

        Whatever shap raises while **building the explainer** becomes
        :class:`UnsupportedCapability`: that is shap failing to read this
        model, and what it raises then is not one type. The case that
        motivated this: shap 0.42.1's ``SingleTree`` stores thresholds as
        floats, so an interventional explainer cannot represent a categorical
        split (``"2||3||4"``) and fails on every model that splits on the item
        — 129 of 161 trees on a real one (2026-07-08); it fails in the
        constructor (``AttributeError``, checked 2026-09-29).

        What ``shap_values(X)`` raises is left alone. Once the explainer
        exists, the model is readable and a failure is about ``X`` — a matrix
        of the wrong width, an unencoded column — which is a bug upstream, and
        a bug has to stop the run rather than be reported as "the model
        cannot" (ADR-0030 decision 4). On the path-dependent route shap hands
        ``X`` straight to ``Booster.predict(pred_contrib=True)``, so those
        errors are LightGBM's own.
        """
        import shap

        booster = self._fitted_booster()
        try:
            if background is None:
                explainer = shap.TreeExplainer(booster)
            else:
                explainer = shap.TreeExplainer(
                    booster, data=background,
                    feature_perturbation="interventional")
        except Exception as exc:
            raise UnsupportedCapability(
                f"shap could not read this LightGBM model "
                f"({'interventional' if background is not None else 'tree path dependent'}): "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        values = np.asarray(explainer.shap_values(X))
        if values.ndim == 3:  # some shap versions return [classes, n, feat]
            values = values[-1]
        # A trailing bias column, when the version appends one, is not a feature.
        return values[:, : np.shape(X)[1]]

    def attribution_cost(self) -> int:
        """The number of trees: TreeSHAP's time per row grows with it."""
        return int(self._fitted_booster().num_trees())

    def tree_structure(self) -> "pd.DataFrame":
        """``Booster.trees_to_dataframe()``, with LightGBM's categorical split
        format decoded.

        LightGBM writes a categorical split as ``decision_type == "=="`` and a
        ``threshold`` string of the codes sent left, joined by ``||``
        (``"2||3||4"``; one code is ``"1"``). A numeric split is ``"<="`` with
        a float threshold. The codes are the ones training encoded the
        categories with, which is what the gain ledger maps back to items.
        """
        trees = self._fitted_booster().trees_to_dataframe()
        categorical = (trees["decision_type"] == "==").to_numpy()
        out = trees[[c for c in TREE_STRUCTURE_COLUMNS if c != "categories_left"]].copy()
        out["categories_left"] = [
            _category_codes(threshold) if is_categorical else None
            for threshold, is_categorical in zip(trees["threshold"], categorical)
        ]
        return out

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        if kind not in ("split", "gain"):
            raise ValueError(f"kind must be 'split' or 'gain', got {kind!r}")
        booster = self._fitted_booster()
        names = booster.feature_name()
        importances = booster.feature_importance(importance_type=kind).astype(float)
        return dict(zip(names, importances))


def _category_codes(threshold) -> tuple[int, ...]:
    """``"2||3||4"`` -> ``(2, 3, 4)``. Raises ``ValueError`` on a token that is
    not a number: that would be a LightGBM format this adapter does not know,
    and guessing would put rows on the wrong side of the split."""
    return tuple(int(float(token)) for token in str(threshold).split("||"))


ADAPTER_REGISTRY["lightgbm"] = LightGBMAdapter
