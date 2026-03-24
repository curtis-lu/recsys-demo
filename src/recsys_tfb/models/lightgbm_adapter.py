"""LightGBM implementation of ModelAdapter."""

import logging

import lightgbm as lgb
import mlflow
import numpy as np

from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter

logger = logging.getLogger(__name__)


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
    ) -> None:
        num_iterations = params.pop("num_iterations", 500)
        early_stopping_rounds = params.pop("early_stopping_rounds", 50)

        train_data = lgb.Dataset(X_train, label=y_train, free_raw_data=False)
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data, free_raw_data=False)

        callbacks = [
            lgb.early_stopping(stopping_rounds=early_stopping_rounds),
            lgb.log_evaluation(period=0),
        ]
        self._booster = lgb.train(
            params,
            train_data,
            num_boost_round=num_iterations,
            valid_sets=[val_data],
            valid_names=["val"],
            callbacks=callbacks,
        )

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._booster is None:
            raise RuntimeError("Model not trained or loaded. Call train() or load() first.")
        return self._booster.predict(X)

    def save(self, filepath: str) -> None:
        if self._booster is None:
            raise RuntimeError("No model to save. Call train() first.")
        self._booster.save_model(filepath)

    def load(self, filepath: str) -> None:
        self._booster = lgb.Booster(model_file=filepath)

    def feature_importance(self) -> dict[str, float]:
        if self._booster is None:
            raise RuntimeError("No model loaded.")
        names = self._booster.feature_name()
        importances = self._booster.feature_importance().astype(float)
        return dict(zip(names, importances))

    def log_to_mlflow(self) -> None:
        if self._booster is None:
            raise RuntimeError("No model to log.")
        mlflow.lightgbm.log_model(self._booster, artifact_path="model")

    @property
    def booster(self) -> lgb.Booster | None:
        """Access the underlying LightGBM Booster (for diagnostics)."""
        return self._booster


ADAPTER_REGISTRY["lightgbm"] = LightGBMAdapter
