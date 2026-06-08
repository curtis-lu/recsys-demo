"""CompositeModelAdapter — two-stage (per-grouping Stage-1 + LTR Stage-2).

Implements the ModelAdapter inference + persistence contract. Training is NOT
done through the numpy `train()` (it cannot express customer-disjoint K-fold
OOF); it is driven by `composite_train.train_composite` and the parts are
injected via `_from_parts`. See docs/pipelines/training.md.
"""
from __future__ import annotations

import json
import os

import lightgbm as lgb
import numpy as np

from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter

MANIFEST_FILENAME = "composite_manifest.json"
STAGE2_FILENAME = "model.txt"  # the conventional model path = Stage-2 booster
_STAGE1_PREFIX = "stage1_"


class CompositeModelAdapter(ModelAdapter):
    def __init__(self) -> None:
        self._stage1: dict[str, lgb.Booster] = {}
        self._stage2: lgb.Booster | None = None
        self._item_col_index: int | None = None
        self._item_code_to_group: dict[int, str] = {}
        self._group_to_code: dict[str, int] = {}
        self._n_features: int | None = None

    # -- construction ----------------------------------------------------
    @classmethod
    def _from_parts(cls, *, stage1_boosters, stage2_booster, item_col_index,
                    item_code_to_group, group_to_code, n_features):
        self = cls()
        self._stage1 = dict(stage1_boosters)
        self._stage2 = stage2_booster
        self._item_col_index = item_col_index
        self._item_code_to_group = dict(item_code_to_group)
        self._group_to_code = dict(group_to_code)
        self._n_features = n_features
        return self

    # -- inference -------------------------------------------------------
    def _stage1_scores(self, X: np.ndarray) -> np.ndarray:
        codes = X[:, self._item_col_index].astype(np.int64)
        out = np.empty(len(X), dtype=np.float64)
        groups: dict[str, list[int]] = {}
        for i, code in enumerate(codes):
            groups.setdefault(self._item_code_to_group[int(code)], []).append(i)
        for group, idx in groups.items():
            booster = self._stage1[group]
            out[idx] = booster.predict(X[idx])
        return out

    def _stage2_matrix(self, X: np.ndarray, s1: np.ndarray) -> np.ndarray:
        codes = X[:, self._item_col_index].astype(np.int64)
        group_code = np.array(
            [self._group_to_code[self._item_code_to_group[int(c)]] for c in codes],
            dtype=np.float64,
        )
        cust_feats = np.delete(X, self._item_col_index, axis=1)
        return np.column_stack([s1, cust_feats, group_code])

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._stage2 is None:
            raise RuntimeError("Composite model not trained or loaded.")
        s1 = self._stage1_scores(X)
        return self._stage2.predict(self._stage2_matrix(X, s1))

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        # Composite importance is reported on the Stage-2 booster (which mixes
        # the Stage-1 score, customer features, and grouping id). Per-Stage-1
        # importance is a future diagnostics enhancement.
        if self._stage2 is None:
            raise RuntimeError("No model loaded.")
        names = self._stage2.feature_name()
        imp = self._stage2.feature_importance(importance_type=kind).astype(float)
        return dict(zip(names, imp))

    def log_to_mlflow(self) -> None:
        import mlflow
        if self._stage2 is None:
            raise RuntimeError("No model to log.")
        mlflow.lightgbm.log_model(self._stage2, name="model")

    # -- not on the composite path --------------------------------------
    def train(self, X_train, y_train, X_val, y_val, params) -> None:
        raise NotImplementedError(
            "CompositeModelAdapter is trained via composite_train.train_composite, "
            "not the numpy train() (which cannot express customer-disjoint K-fold OOF)."
        )

    def prepare_train_inputs(self, *args, **kwargs):
        raise NotImplementedError(
            "CompositeModelAdapter does not use prepare_train_inputs; its training "
            "node reads the parquet handles directly."
        )

    # -- persistence -----------------------------------------------------
    def save(self, filepath: str) -> None:
        if self._stage2 is None:
            raise RuntimeError("No model to save.")
        d = os.path.dirname(filepath) or "."
        os.makedirs(d, exist_ok=True)
        self._stage2.save_model(filepath)  # model.txt == Stage-2
        group_to_file: dict[str, str] = {}
        for group, booster in self._stage1.items():
            fname = f"{_STAGE1_PREFIX}{group}.txt"
            booster.save_model(os.path.join(d, fname))
            group_to_file[group] = fname
        manifest = {
            "model_structure": "per_group_plus_rank",
            "item_col_index": self._item_col_index,
            "n_features": self._n_features,
            # JSON keys are strings: store code<->group as lists of pairs.
            "item_code_to_group": [[int(k), v] for k, v in self._item_code_to_group.items()],
            "group_to_code": self._group_to_code,
            "group_to_file": group_to_file,
        }
        with open(os.path.join(d, MANIFEST_FILENAME), "w") as f:
            json.dump(manifest, f, indent=2)

    def load(self, filepath: str) -> None:
        d = os.path.dirname(filepath) or "."
        with open(os.path.join(d, MANIFEST_FILENAME)) as f:
            m = json.load(f)
        self._stage2 = lgb.Booster(model_file=filepath)
        self._item_col_index = m["item_col_index"]
        self._n_features = m["n_features"]
        self._item_code_to_group = {int(k): v for k, v in m["item_code_to_group"]}
        self._group_to_code = dict(m["group_to_code"])
        self._stage1 = {
            group: lgb.Booster(model_file=os.path.join(d, fname))
            for group, fname in m["group_to_file"].items()
        }


ADAPTER_REGISTRY["composite"] = CompositeModelAdapter
