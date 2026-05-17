"""LightGBM implementation of ModelAdapter."""

import logging
import shutil
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np

from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
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

        Skip-if-exists: returns handles without rebuilding when cache_dir/lgb/_SUCCESS
        already exists. On miss, builds train first (with binning), saves binary,
        then builds train_dev with reference=train so dev binning aligns to train.
        """
        lgb_dir = Path(cache_dir) / "lgb"
        success = lgb_dir / "_SUCCESS"
        train_bin = lgb_dir / "train.bin"
        dev_bin = lgb_dir / "train_dev.bin"

        if success.exists():
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

        # feature_pre_filter=False at construct time: features with
        # <min_data_in_leaf samples per bin are NOT silently dropped from the
        # binned dataset. The pre-cache training path (numpy → lgb.Dataset built
        # by lgb.train at trial time) inherits feature_pre_filter=False from
        # trial params; the cached binary path must opt out explicitly to match.
        construct_params = {"feature_pre_filter": False}

        # Lazy import: see module-top comment about circular-import chain.
        from recsys_tfb.io.extract import extract_Xy

        # Extract → build → save train, then free raw arrays before dev is read.
        # Keeps the constructed ds_train alive (it's small) for dev's reference.
        X_tr, y_tr = extract_Xy(train_handle, preprocessor_metadata, parameters)
        ds_train = lgb.Dataset(
            X_tr,
            label=y_tr,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
        log_data_volume(logger, "prepare.ds_train", ds_train)
        ds_train.save_binary(str(train_bin))
        log_data_volume(logger, "prepare.train.bin", str(train_bin))
        del X_tr, y_tr

        X_dev, y_dev = extract_Xy(train_dev_handle, preprocessor_metadata, parameters)
        ds_dev = lgb.Dataset(
            X_dev,
            label=y_dev,
            reference=ds_train,
            categorical_feature=cat_idx,
            params=construct_params,
            free_raw_data=True,
        ).construct()
        log_data_volume(logger, "prepare.ds_dev", ds_dev)
        ds_dev.save_binary(str(dev_bin))
        log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
        del X_dev, y_dev, ds_train, ds_dev

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
    def booster(self) -> lgb.Booster | None:
        """Access the underlying LightGBM Booster (for diagnostics)."""
        return self._booster


ADAPTER_REGISTRY["lightgbm"] = LightGBMAdapter
