"""Which feature columns the model actually wants, and in what order.

Two artifacts disagree about this and only one of them is authoritative. The
model records the view a training run used; ``preprocessor.json`` records the
full set and how each column is encoded (ADR-0011 §5). Every function here
exists to keep that asymmetry from being resolved the convenient way — by
intersecting, realigning or re-deriving the view from the *current* config —
because each of those produces an X the booster silently mis-reads rather than
an error.

Lives in ``models/`` rather than under a pipeline for the same reason as
``feature_selection.py``: both the inference scoring node and the training
diagnosis nodes read it (ADR-0014 decision 7), so it is not one pipeline's
internal step. It is the *other half* of ``feature_selection.py``: that module
derives the view from config at training time, this one recovers it from the
trained model afterwards, and only the second answer survives a config edit.
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


def model_feature_view(model: ModelAdapter, preprocessor: dict) -> dict:
    """The preprocessor artifact with the model's own column list swapped in.

    Callers hand the result to anything that reads ``feature_columns`` to slice
    X (``io.extract.pdf_to_X``) — the model answers *which* columns, the
    artifact keeps answering *how* they are encoded.

    ``categorical_columns`` is deliberately left at the artifact's full set
    rather than narrowed to match. ``pdf_to_X`` intersects it with the sliced
    frame's own columns, so extra names there cannot reach the output; copying
    the narrowing here would add a second place to keep in step with no
    observable difference.

    Not interchangeable with ``feature_selection.apply_feature_selection``: that
    one re-derives the view from the *current* config. Which of the two is right
    depends on when you ask. Before a model exists there is no second opinion and
    config is the only answer — that is what the training nodes use. After it
    exists the model is the record of what was actually used, and it needs no
    memory-only input to say so, which is why the diagnosis nodes ask it
    (ADR-0014 decision 7). In training the two answers cannot silently disagree
    (the exclude list bumps ``model_version``); on the inference side they can,
    and there the distinction is load-bearing (ADR-0011 §5).
    """
    return {
        **preprocessor,
        "feature_columns": model_feature_columns(model, preprocessor),
    }
