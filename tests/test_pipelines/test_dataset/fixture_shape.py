"""The shape every dataset-pipeline test fixture is generated from.

The fixtures themselves are in ``conftest.py``; this module holds the lists and
row rules they are built from — plus the one helper that varies ``parameters``
off them — so a test module can derive a count assertion from the same constants
the fixture used instead of writing a literal. Resizing the fixture is then a
one-line edit here rather than a hunt for a dozen hand-computed constants — and
a constant that silently stops matching the fixture can no longer make an
assertion vacuous.

Kept out of ``conftest.py`` because these are ordinary module-level values, not
fixtures: importing them is how ``test_nodes_spark.py`` derives its counts, and
pytest discourages importing ``conftest``.

Entity count (24) is chosen so train / train-dev actually split: at the old size
of 4, train_dev_ratio=0.2 rounded the dev side to empty, and every assertion in
TestSplitTrainKeys held for a `split` that did nothing. See
TestSplitTrainKeys.test_both_sides_are_non_empty for how that was verified.
Item count stays at 3 and is deliberately NOT aligned with the 8 in conf/ —
that gap is orthogonal to this one (see #140).
"""

_ENTITIES = [f"C{i:03d}" for i in range(1, 25)]
_PRODUCTS = ["exchange_fx", "exchange_usd", "fund_stock"]
_SNAP_DATES = ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]

# Per-entity attribute values, cycled so any fixture size stays covered. These
# are inert with respect to every assertion in the test modules (they are
# neither identity nor feature columns); they exist so the frames look like the
# real tables.
_SEGMENTS = ["mass", "affluent", "hnw"]
_CHANNELS = ["digital", "branch", "both"]


def _entity_index(cid: str) -> int:
    return _ENTITIES.index(cid)


def _segment(cid: str) -> str:
    return _SEGMENTS[_entity_index(cid) % len(_SEGMENTS)]


def _channel(cid: str) -> str:
    return _CHANNELS[_entity_index(cid) % len(_CHANNELS)]


# The one entity with no positive on any product. Keeps a zero-positive query
# group in the fixture (so the group filter still has something to drop) while
# every other entity carries one.
_ALL_NEGATIVE_ENTITY = _ENTITIES[-1]


def _is_positive(cid: str, prod: str) -> bool:
    """Positive on the first product for every entity but one.

    Not concentrated on a single entity, because the train / train-dev split
    partitions **by entity**: one positive entity lands wholly on one side and
    leaves the other with an all-zero label column — the state
    ``TestModelInputSchemaPerSplit.test_positive_rate_is_strictly_inside_its_bounds``
    exists to reject, and train_dev is the worst place to tolerate it since it
    is every HPO trial's early-stopping set (ADR-0005 §2). Verified by narrowing
    this back to ``_ENTITIES[0]``: train_dev then holds 0 positives in 18 rows.
    Spreading it over every *third* entity was not enough either — train_dev's
    three entities happened to miss all of them, and "which entities land in
    dev" is a hash detail no assertion should depend on.

    Two properties this shape guarantees for any split, whatever the hash does:

    - at most one positive per query group, so no group is all-positive;
    - at least one positive in any split holding two or more entities.
    """
    return cid != _ALL_NEGATIVE_ENTITY and prod == _PRODUCTS[0]


def _explicit_preprocessing_params(
    parameters, *, drop_extra=(), categorical_extra=(), carry=None
) -> dict:
    """``parameters`` with the preprocessing keys spelled out instead of defaulted.

    The ``parameters`` fixture leaves ``prepare_model_input`` unset, so anything
    reading it — the Layer-2 gate and ``fit_preprocessor_metadata`` alike —
    falls back to the defaults in ``preprocessing/_common.py``. Those defaults
    are read from that module rather than restated here: a hand-written copy
    would be equal to the real list only as long as feature_table happens to
    lack the columns where the two differ (``apply_start_date`` /
    ``apply_end_date`` / ``cust_segment_typ``), and would then silently stop
    dropping them the day a fixture grows one — turning every count assertion
    that rests on it into a different question without turning anything red.

    Callers add to the defaults rather than replacing them, so a test says which
    columns *it* cares about and inherits the rest.
    """
    from recsys_tfb.preprocessing._common import _get_preprocessing_config

    default_drop, default_categorical = _get_preprocessing_config(parameters)
    dataset = {
        **parameters["dataset"],
        "prepare_model_input": {
            "drop_columns": [*default_drop, *drop_extra],
            "categorical_columns": [*default_categorical, *categorical_extra],
        },
    }
    if carry is not None:
        dataset["carry_columns"] = list(carry)
    return {**parameters, "dataset": dataset}
