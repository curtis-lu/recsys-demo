"""CalibratedModelAdapter — wrapper that adds probability calibration to any ModelAdapter."""

import logging
import os
import pickle

import numpy as np

from recsys_tfb.models.base import ModelAdapter

logger = logging.getLogger(__name__)


class CalibratedModelAdapter(ModelAdapter):
    """Wraps a base ModelAdapter to add post-hoc probability calibration.

    Calibration is fitted separately via ``fit_calibrator()`` on a held-out
    calibration set.  ``predict()`` then returns calibrated probabilities.

    This class is intentionally **not** registered in ``ADAPTER_REGISTRY``
    because it is a wrapper, not a standalone algorithm.
    """

    CALIBRATOR_FILENAME = "calibrator.pkl"

    def __init__(self, base: ModelAdapter, method: str = "isotonic") -> None:
        self._base = base
        self._method = method
        self._calibrator = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def base(self) -> ModelAdapter:
        return self._base

    @property
    def method(self) -> str:
        return self._method

    # ------------------------------------------------------------------
    # Calibration
    # ------------------------------------------------------------------

    def fit_calibrator(self, X_cal: np.ndarray, y_cal: np.ndarray) -> None:
        """Fit calibrator on calibration data using raw scores from the base model."""
        raw_scores = self._base.predict(X_cal)

        if self._method == "isotonic":
            from sklearn.isotonic import IsotonicRegression

            self._calibrator = IsotonicRegression(out_of_bounds="clip")
            self._calibrator.fit(raw_scores, y_cal)
        elif self._method == "sigmoid":
            from sklearn.linear_model import LogisticRegression

            self._calibrator = LogisticRegression()
            self._calibrator.fit(raw_scores.reshape(-1, 1), y_cal)
        else:
            raise ValueError(
                f"Unknown calibration method '{self._method}'. "
                "Supported: 'isotonic', 'sigmoid'."
            )
        logger.info("Calibrator fitted (method=%s) on %d samples.", self._method, len(y_cal))

    # ------------------------------------------------------------------
    # ModelAdapter interface
    # ------------------------------------------------------------------

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        params: dict,
    ) -> None:
        """Delegate training to the base adapter."""
        self._base.train(X_train, y_train, X_val, y_val, params)

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Return calibrated scores, or raw scores if calibrator is not fitted."""
        raw = self._base.predict(X)
        if self._calibrator is None:
            return raw
        if self._method == "isotonic":
            return self._calibrator.predict(raw)
        # sigmoid
        return self._calibrator.predict_proba(raw.reshape(-1, 1))[:, 1]

    def predict_uncalibrated(self, X: np.ndarray) -> np.ndarray:
        """Return raw (uncalibrated) scores from the base model."""
        return self._base.predict(X)

    def save(self, filepath: str) -> None:
        """Save base model and calibrator sidecar."""
        self._base.save(filepath)
        cal_path = os.path.join(os.path.dirname(filepath), self.CALIBRATOR_FILENAME)
        with open(cal_path, "wb") as f:
            pickle.dump({"method": self._method, "calibrator": self._calibrator}, f)
        logger.info("Calibrator saved to %s", cal_path)

    def load(self, filepath: str) -> None:
        """Load base model and calibrator sidecar."""
        self._base.load(filepath)
        self._load_calibrator(filepath)

    def _load_calibrator(self, model_filepath: str) -> None:
        """Load calibrator pickle sidecar from the same directory as the model file."""
        cal_path = os.path.join(
            os.path.dirname(model_filepath), self.CALIBRATOR_FILENAME
        )
        with open(cal_path, "rb") as f:
            data = pickle.load(f)  # noqa: S301
        self._method = data["method"]
        self._calibrator = data["calibrator"]
        logger.info("Calibrator loaded from %s (method=%s)", cal_path, self._method)

    def feature_importance(self) -> dict[str, float]:
        """Delegate to the base adapter."""
        return self._base.feature_importance()

    def log_to_mlflow(self) -> None:
        """Delegate to the base adapter."""
        self._base.log_to_mlflow()
