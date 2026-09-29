"""A second ``ModelAdapter`` that is not LightGBM, for proving the seam is real.

ADR-0030 decision 1: one adapter makes the interface a guess; the second one
is what shows training asks nothing of a model beyond the ABC. No second
real algorithm is wanted (the ADR's out-of-scope list), so this one lives in
the tests.

The model is a weighted logistic regression fit by plain gradient descent,
one step per "round", so early stopping and a best round mean what they mean
for a boosted model. Its rules are its own: the objective names, the metric
names and which objective drops zero-positive groups deliberately differ from
LightGBM's, so a check that reads LightGBM's table instead of asking the
configured adapter gets a different answer.

Register it for one test with
``monkeypatch.setitem(ADAPTER_REGISTRY, FAKE_ALGORITHM, FakeAdapter)``.

It has no cache logic of its own: ``prepare_train_inputs`` decides which rows
go into the cached files, in what order, and writes the weight-key sidecar for
every adapter; the adapter only builds and saves (ADR-0030 decisions 1 and
10). What it keeps from a ranking build is the row order — it has no use for
the per-group counts, which is allowed: ``build_train_data`` is handed them,
not required to store them.

None of the diagnostics' optional capabilities are implemented, so every one
raises ``UnsupportedCapability``: the double for "a model the diagnostics
cannot read".
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from recsys_tfb.models.base import AlgorithmRules, ModelAdapter

FAKE_ALGORITHM = "fake_logistic"

FAKE_RULES = AlgorithmRules(
    ranking_objectives=frozenset({"fake_rank"}),
    ranking_metrics=frozenset({"fake_ndcg"}),
    default_ranking_metric="fake_ndcg",
    zero_positive_group_dropping_objectives=frozenset({"fake_rank"}),
)


@dataclass
class FakeTrainData:
    X: np.ndarray
    y: np.ndarray
    weight: np.ndarray | None
    feature_names: list

    # What core.logging.log_data_volume reads off a native dataset.
    def num_data(self) -> int:
        return len(self.y)

    def num_feature(self) -> int:
        return self.X.shape[1]


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def _weighted_logloss(data: FakeTrainData, coef: np.ndarray) -> float:
    p = np.clip(_sigmoid(data.X @ coef[:-1] + coef[-1]), 1e-12, 1 - 1e-12)
    w = np.ones(len(data.y)) if data.weight is None else data.weight
    loss = -(data.y * np.log(p) + (1 - data.y) * np.log(1 - p))
    return float(np.sum(w * loss) / np.sum(w))


class FakeAdapter(ModelAdapter):
    rules = FAKE_RULES

    def __init__(self) -> None:
        self._coef: np.ndarray | None = None
        self._best_iteration: int | None = None
        self._feature_names: list | None = None

    # -- native training data -------------------------------------------------

    def build_train_data(self, X, y, *, feature_names, categorical_features,
                         group=None, weight=None, reference=None):
        return FakeTrainData(
            X=np.asarray(X, dtype=np.float64),
            y=np.asarray(y, dtype=np.float64),
            weight=None if weight is None else np.asarray(weight, dtype=np.float64),
            feature_names=list(feature_names),
        )

    def save_train_data(self, data, path):
        if data.weight is not None:
            raise ValueError(
                f"refusing to save training data with per-row weights to {path}")
        # A file handle, not a path: np.savez appends ".npz" to a bare path.
        with open(path, "wb") as f:
            np.savez(f, X=data.X, y=data.y,
                     feature_names=np.asarray(data.feature_names, dtype=object))

    def load_train_data(self, path, *, weight, reference=None):
        with np.load(path, allow_pickle=True) as z:
            X, y, names = z["X"], z["y"], list(z["feature_names"])
        weight = np.asarray(weight, dtype=np.float64)
        if len(weight) != len(y):
            raise ValueError(
                f"sample weights for {path} have {len(weight)} entries but the "
                f"binary holds {len(y)} rows"
            )
        return FakeTrainData(X=X, y=y, weight=weight, feature_names=names)

    # -- fitting --------------------------------------------------------------

    def train(self, train_data, params, *, num_iterations,
              early_stopping_rounds=0, valid_data=None):
        lr = float(params.get("learning_rate", 0.1))
        rng = np.random.default_rng(int(params.get("seed", 0)))
        coef = rng.normal(scale=1e-3, size=train_data.X.shape[1] + 1)
        w = (np.ones(len(train_data.y)) if train_data.weight is None
             else train_data.weight)
        Xb = np.hstack([train_data.X, np.ones((len(train_data.y), 1))])

        snapshots, best_loss, best_round, since_best = [], np.inf, 0, 0
        for step in range(1, num_iterations + 1):
            p = _sigmoid(Xb @ coef)
            coef = coef - lr * Xb.T @ (w * (p - train_data.y)) / np.sum(w)
            snapshots.append(coef.copy())
            if valid_data is None:
                continue
            loss = _weighted_logloss(valid_data, coef)
            if loss < best_loss:
                best_loss, best_round, since_best = loss, step, 0
            else:
                since_best += 1
            if early_stopping_rounds > 0 and since_best >= early_stopping_rounds:
                break

        stopped_early = valid_data is not None and early_stopping_rounds > 0
        self._best_iteration = best_round if stopped_early else 0
        self._coef = snapshots[best_round - 1] if stopped_early else coef
        self._feature_names = list(train_data.feature_names)

    @property
    def best_iteration(self) -> int:
        if self._best_iteration is None:
            raise RuntimeError("Model not trained. Call train() first.")
        return self._best_iteration

    def predict(self, X):
        if self._coef is None:
            raise RuntimeError("Model not trained or loaded.")
        return _sigmoid(np.asarray(X, dtype=np.float64) @ self._coef[:-1] + self._coef[-1])

    def feature_names(self):
        return None if self._feature_names is None else list(self._feature_names)

    # -- the fitted model on disk ---------------------------------------------

    def save(self, filepath):
        Path(filepath).write_text(json.dumps(
            {"coef": self._coef.tolist(), "feature_names": self._feature_names}))

    def load(self, filepath):
        payload = json.loads(Path(filepath).read_text())
        self._coef = np.asarray(payload["coef"])
        self._feature_names = payload["feature_names"]

    # No feature_attributions, attribution_cost, tree_structure or
    # feature_importance: a logistic regression has no trees and keeps no
    # split / gain counts, so it takes the ABC's defaults, which raise
    # UnsupportedCapability. That is what lets the diagnostics' "the model
    # cannot" path run end to end (ADR-0030 decision 4).

    def log_to_mlflow(self):
        """No MLflow flavour for a test double; nothing to log."""
