"""I/O adapter for ModelAdapter instances with model_meta.json sidecar."""

import json
import logging
import os
from datetime import datetime, timezone

from recsys_tfb.io.base import AbstractDataset
from recsys_tfb.models.base import ModelAdapter, get_adapter

logger = logging.getLogger(__name__)

META_FILENAME = "model_meta.json"


class ModelAdapterDataset(AbstractDataset):
    """Dataset that saves/loads ModelAdapter instances.

    On save, writes the model file (via adapter.save) plus a model_meta.json
    sidecar recording the algorithm name and adapter class. On load, reads the
    meta file to auto-select the correct adapter.

    Filepath should point to the model file (e.g. data/models/v1/model.txt).
    The meta sidecar is written alongside it in the same directory.
    """

    def __init__(self, filepath: str):
        self._filepath = filepath

    @property
    def _meta_filepath(self) -> str:
        return os.path.join(os.path.dirname(self._filepath), META_FILENAME)

    def load(self) -> ModelAdapter:
        meta_path = self._meta_filepath
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            algorithm = meta["algorithm"]
            adapter = get_adapter(algorithm)
        else:
            logger.warning(
                "No %s found at %s — falling back to LightGBM.",
                META_FILENAME,
                meta_path,
            )
            adapter = get_adapter("lightgbm")
            meta = {}

        # Refuse a model this framework can no longer honour. Calibration was
        # removed in #411, so the wrapper class that turned the sidecar's
        # `calibrated: true` back into a calibrated model does not exist — and
        # the `calibrator.pkl` beside it has no reader. Two worse endings are
        # what this replaces: an ImportError naming a module the operator never
        # configured, or loading the base model in silence and publishing raw
        # scores under a model whose consumers were promised probabilities.
        #
        # Presence is not the test, truth is: every model saved while
        # calibration was off carries `calibrated: false`, and those load
        # normally. So does a sidecar written after #411, which omits the key.
        if meta.get("calibrated", False):
            raise ValueError(
                f"{meta_path} says this model was saved with a calibrator "
                f"attached. Calibration was removed from the framework (#411) "
                f"and there is nothing left to load the calibrator with. "
                f"Retrain with the current version — the new model scores the "
                f"same rows in the same order, without the probability "
                f"rescaling."
            )

        adapter.load(self._filepath)
        return adapter

    def save(self, data: ModelAdapter) -> None:
        os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
        data.save(self._filepath)

        # Determine algorithm name from registry
        from recsys_tfb.models.base import ADAPTER_REGISTRY

        algorithm = "unknown"
        adapter_class = type(data)
        for name, cls in ADAPTER_REGISTRY.items():
            if cls is adapter_class:
                algorithm = name
                break

        # No calibration flag, not even `false`: the framework has no such
        # concept any more, and writing `false` would keep the key alive in
        # every new sidecar for a reader that no longer exists.
        meta = {
            "algorithm": algorithm,
            "adapter_class": f"{adapter_class.__module__}.{adapter_class.__qualname__}",
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }

        with open(self._meta_filepath, "w") as f:
            json.dump(meta, f, indent=2)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
