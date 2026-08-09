"""Which feature columns the model actually wants, and in what order.

Two artifacts disagree about this and only one of them is authoritative. The
model records the view a training run used; ``preprocessor.json`` records the
full set and how each column is encoded (ADR-0011 §5). Every function here
exists to keep that asymmetry from being resolved the convenient way — by
intersecting, realigning or re-deriving the view from the *current* config —
because each of those produces an X the booster silently mis-reads rather than
an error.
"""

from __future__ import annotations

import logging

from recsys_tfb.models.base import ModelAdapter

logger = logging.getLogger(__name__)


def require_ordered_subsequence(
    model_features: list[str],
    artifact_features: list[str],
) -> None:
    """Raise unless ``model_features`` is an order-preserving subsequence.

    Pre-check on two artifacts, run before either is used.

    The model is the authority on *which* features and in *what order*; the
    preprocessor artifact is the authority on how they are *encoded*
    (ADR-0011 §5). Only the model records which view of the full feature set a
    given training run actually used — ``preprocessor.json`` is the full set and
    cannot tell whether ``training.feature_selection.exclude`` was in play.

    So the artifact is allowed to hold *more* columns, in the same relative
    order, and nothing else. A stale artifact (model has a column the artifact
    lacks) and a mismatched model (same columns, permuted) both raise. No
    automatic realignment: an intersection here would silently slice X by the
    full set and hand it to a booster that expects a subset.
    """
    remaining = iter(artifact_features)
    for name in model_features:
        if name not in remaining:
            raise ValueError(
                "model.feature_names() is not an order-preserving subsequence of "
                f"the preprocessor's feature_columns: stuck at {name!r}. "
                f"model={list(model_features)} artifact={list(artifact_features)}"
            )


def model_feature_columns(model: ModelAdapter, preprocessor: dict) -> list[str]:
    """Which feature columns to slice, and in what order — the model decides.

    The preprocessor artifact holds the full set and cannot tell whether
    ``training.feature_selection.exclude`` was in play; the model is the only
    artifact that records which view a given training run actually used
    (ADR-0011 section 5).
    """
    artifact_feature_columns = list(preprocessor["feature_columns"])
    feature_names_fn = getattr(model, "feature_names", None)
    model_feature_names = (
        feature_names_fn() if callable(feature_names_fn) else None
    )
    if model_feature_names:
        feature_columns = list(model_feature_names)
        require_ordered_subsequence(feature_columns, artifact_feature_columns)
        return feature_columns
    # No declaration, so there is no second opinion to check the artifact
    # against and the assertion is skipped rather than run against itself.
    # Every adapter in this repo declares one once it holds a fitted model
    # (LightGBMAdapter returns None only before load(); CalibratedModelAdapter
    # forwards to its base), so this branch is doubles in tests, not
    # production — and it is logged rather than silent for that reason.
    logger.info(
        "Model declares no feature_names(); falling back to the "
        "preprocessor's %d feature columns", len(artifact_feature_columns),
    )
    return artifact_feature_columns


def model_columns_to_collect(
    keep_identity: list[str],
    feature_columns: list[str],
    identity_cols: list[str],
) -> list[str]:
    """The columns one bucket brings into the driver, identity first.

    Narrower than the landed table on purpose: that table stores the
    preprocessor's full feature set so it can be reused across ``model_version``
    (ADR-0010 §5), while a scoring run only pulls the columns *this* model
    declared. Widening it back to the stored set would cross into the driver
    once per bucket carrying columns nothing reads.

    The item is absent from both sides — the landed table is not exploded by it,
    and the scoring loop assigns it per iteration — so an identity column that
    happens to be the item must not be requested here.
    """
    return keep_identity + [
        c for c in feature_columns if c not in identity_cols
    ]


def require_population_has_model_columns(
    available: list[str], required: list[str],
) -> None:
    """Pre-check: the landed population table holds every column the model wants.

    Fails before the first bucket is read rather than inside ``_pdf_to_X``,
    where the same mismatch surfaces as a KeyError halfway through a long run.
    The realistic cause is a stale ``inference_population_features`` — built
    under a preprocessor that predates this model — reachable via
    ``--from-node predict_and_write_scores``, which skips the node that would
    have rebuilt it.
    """
    missing_features = sorted(set(required) - set(available))
    if missing_features:
        raise ValueError(
            "inference_population_features is missing columns required by the "
            f"model: {missing_features}"
        )
