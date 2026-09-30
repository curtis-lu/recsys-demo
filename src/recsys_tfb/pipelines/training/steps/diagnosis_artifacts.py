"""What goes into a diagnosis's JSON: JSON-safe scalars, and the artifact a
diagnosis lands when the model cannot answer it."""

import numpy as np


def to_native(v):
    """A numpy scalar or NaN as a JSON-safe Python scalar: ``None`` and NaN
    become ``None``, anything else a ``float``."""
    if v is None:
        return None
    f = float(v)
    return None if np.isnan(f) else f


def unsupported_artifact(exc: Exception) -> dict:
    """What a diagnosis lands when the model raised ``UnsupportedCapability``.

    A fourth shape, next to "no file", ``{"enabled": False}`` and the full
    result (ADR-0030 decision 4): "switched off" and "never ran" each already
    mean something to a reader, and a skipped diagnosis landing as either
    would be reported with the wrong cause and the wrong fix. ``enabled``
    stays ``True`` because nobody switched it off; ``supported: False`` is
    the key readers test for — evaluation's ``model_capacity`` for
    ``gain_ledger.json``, ``log_experiment`` for the rest.
    """
    return {"enabled": True, "supported": False, "reason": str(exc)}
