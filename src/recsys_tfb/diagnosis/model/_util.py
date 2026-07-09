"""JSON-safe scalar conversion helper。"""

import numpy as np


def _to_native(v):
    """np scalar / NaN → JSON-safe python scalar（NaN → None）。"""
    if v is None:
        return None
    f = float(v)
    return None if np.isnan(f) else f
