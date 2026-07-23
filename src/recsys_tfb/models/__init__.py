from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter, get_adapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged import adapter as _staged_adapter  # noqa: F401  (registry side effect)
