"""Reconciling the model's feature view with the landed population table.

The view itself — which columns the model wants and in what order — is decided
by ``models.feature_view``, which moved out of this module once training
diagnostics became a second consumer (ADR-0014 decision 7). What stays here is
inference-only: narrowing that view to what one bucket carries into the driver,
and checking the landed table can supply it before the first bucket is read.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


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

    Fails before the first bucket is read rather than inside ``pdf_to_X``,
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
