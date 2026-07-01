"""Training diagnostics 套件（feature stats / importance / SHAP）。

對外維持與舊 diagnostics.py 相容的 import 介面（pipeline.py、nodes.py、既有測試）。
"""
from .feature_stats import compute_feature_statistics
from .importance import compute_feature_importance
from .paths import diagnostics_dir
from .shap_cases import compute_quadrant_profiles
from .shap_per_item import compute_shap_diagnostics

__all__ = [
    "compute_feature_statistics",
    "compute_feature_importance",
    "compute_quadrant_profiles",
    "compute_shap_diagnostics",
    "diagnostics_dir",
]
