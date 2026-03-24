"""ModelAdapter ABC and adapter registry."""

from abc import ABC, abstractmethod

import numpy as np


class ModelAdapter(ABC):
    """Abstract base class for model adapters.

    All algorithm-specific adapters inherit from this class and implement
    train/predict/save/load/feature_importance/log_to_mlflow.
    """

    @abstractmethod
    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        params: dict,
    ) -> None:
        """Train the model. After calling, the adapter holds the trained model."""
        ...

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Return probability scores as a 1-D numpy array."""
        ...

    @abstractmethod
    def save(self, filepath: str) -> None:
        """Save the model to the given filepath using the algorithm's native format."""
        ...

    @abstractmethod
    def load(self, filepath: str) -> None:
        """Load a model from the given filepath into this adapter."""
        ...

    @abstractmethod
    def feature_importance(self) -> dict[str, float]:
        """Return {feature_name: importance_score}."""
        ...

    @abstractmethod
    def log_to_mlflow(self) -> None:
        """Log the model artifact using the algorithm's MLflow integration."""
        ...


ADAPTER_REGISTRY: dict[str, type[ModelAdapter]] = {}


def get_adapter(algorithm: str) -> ModelAdapter:
    """Create and return an adapter instance for the given algorithm name."""
    cls = ADAPTER_REGISTRY.get(algorithm)
    if cls is None:
        available = ", ".join(sorted(ADAPTER_REGISTRY.keys())) or "(none)"
        raise ValueError(
            f"Unknown algorithm '{algorithm}'. Available: {available}"
        )
    return cls()
