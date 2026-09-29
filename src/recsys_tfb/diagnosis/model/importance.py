"""The model's built-in feature importance (split count and gain)."""

import logging

from recsys_tfb.models.base import UnsupportedCapability

from ._util import unsupported_artifact

logger = logging.getLogger(__name__)


def compute_feature_importance(model, parameters: dict) -> dict:
    """Split + gain importance, ranked by gain, with the features no split uses.

    A model that keeps no such counts (``UnsupportedCapability``) lands the
    "model cannot" shape and the run goes on; anything else stops it
    (ADR-0030 decision 4).
    """
    cfg = parameters.get("diagnostics", {}).get("feature_importance", {})
    if not cfg.get("enabled", True):
        return {}
    # Decision — a model without split / gain bookkeeping skips this and says
    # so, rather than stopping a run whose model is fine.
    try:
        split = model.feature_importance(kind="split")
        gain = model.feature_importance(kind="gain")
    except UnsupportedCapability as exc:
        logger.warning("feature_importance: skipped, the model cannot provide it: %s", exc)
        return unsupported_artifact(exc)
    ranked = sorted(
        ({"feature": f, "split": float(split[f]), "gain": float(gain[f])} for f in split),
        key=lambda r: r["gain"],
        reverse=True,
    )
    dead = sorted(f for f, v in split.items() if v == 0)
    logger.info("feature_importance: %d features, %d dead", len(ranked), len(dead))
    return {"ranked": ranked, "dead_features": dead}
