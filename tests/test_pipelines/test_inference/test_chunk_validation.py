"""The chunk layer of inference validation — pure pandas, no SparkSession.

These run in milliseconds for the same reason ``test_chunk_plans.py`` does: the
checks they cover are the ones that decide whether a chunk reaches the table at
all, so they have to be cheap enough to run on every edit rather than behind a
2-4 minute Spark start-up. The module under test imports no pyspark.
"""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.inference.steps.validation import (
    BATCH_CHECKS,
    CHUNK_CHECKS,
    ValidationError,
    validate_scored_chunk,
)

SCHEMA = get_schema({})
PRODUCTS = ["exchange_fx", "fund_stock", "fund_bond"]
SNAP_DATE = "2024-03-31"


def _chunk(cust_ids=("C001", "C002", "C003"), scores=None, item="exchange_fx"):
    """One chunk's pair of frames, built the way the scoring node builds them.

    ``source_pdf`` is the bucket's features as they came back from Spark;
    ``out_pdf`` is what ``save()`` is about to receive, including the
    ``astype(str)`` the node applies to the entity columns.
    """
    cust_ids = list(cust_ids)
    source = pd.DataFrame({
        "cust_id": cust_ids,
        "total_aum": [float(i) for i in range(len(cust_ids))],
    })
    out = pd.DataFrame({
        "cust_id": source["cust_id"].astype(str).values,
        "score": [0.5] * len(cust_ids) if scores is None else list(scores),
        "snap_date": SNAP_DATE,
        "prod_name": item,
    })
    return out, source


def _validate(out, source, **overrides):
    kwargs = {"schema": SCHEMA, "known_items": PRODUCTS}
    kwargs.update(overrides)
    validate_scored_chunk(out, source, **kwargs)


def _failed_checks(out, source, **overrides):
    with pytest.raises(ValidationError) as exc_info:
        _validate(out, source, **overrides)
    return {failure["check"] for failure in exc_info.value.failures}


class TestValidScoredChunkPasses:
    def test_a_well_formed_chunk_raises_nothing(self):
        out, source = _chunk()
        _validate(out, source)


class TestChunkRowCount:
    def test_fewer_scored_rows_than_entities_read(self):
        """The by-chunk half of the row accounting.

        The whole-table half is ``partition_completeness``, which compares
        partition *sets* and so cannot see a partition that is present but
        short.
        """
        out, source = _chunk()
        assert _failed_checks(out.iloc[:2], source) == {"chunk_row_count"}


class TestNoMissing:
    def test_a_null_score(self):
        out, source = _chunk(scores=[0.5, np.nan, 0.7])
        assert _failed_checks(out, source) == {"no_missing"}

    def test_a_null_entity_identity_is_read_off_the_source_frame(self):
        """Why this check takes ``source_pdf`` and not just ``out_pdf``.

        The node writes the entity identity through ``astype(str)``, which turns
        a null into the *string* ``"None"``. So the output frame has nothing to
        find, and a null check pointed at it would be exactly the structurally
        un-reddable assertion ADR-0011 exists to delete.
        """
        out, source = _chunk(cust_ids=["C001", None, "C003"])
        assert not out["cust_id"].isna().any(), (
            "the coerced output must look clean, or this test is not pinning "
            "the reason source_pdf is a parameter"
        )
        assert _failed_checks(out, source) == {"no_missing"}

    def test_a_null_item_literal(self):
        """``item_col`` is a literal from the loop, so a null means a bad loop."""
        out, source = _chunk()
        out["prod_name"] = None
        assert _failed_checks(out, source) == {
            "no_missing", "item_values_are_known",
        }


class TestNoDuplicates:
    def test_the_same_entity_twice_in_one_chunk(self):
        """Structurally impossible today, and pinned because the structure moves.

        The landed table's grain is ``(time, entity)`` and buckets partition the
        entities, so nothing can produce a repeat — until someone changes the
        grain or the bucket hash, which is when a cheap assertion earns its keep.
        """
        out, source = _chunk(cust_ids=["C001", "C001", "C002"])
        assert _failed_checks(out, source) == {"no_duplicates"}


class TestChunkRowCountIsAConstructionGuard:
    """Named honestly: the node cannot produce a short chunk.

    ``out_pdf`` is built by handing pandas the entity arrays and the score
    array in one call, so a length disagreement raises ``All arrays must be of
    the same length`` before this check runs, and ``entity`` cannot be empty
    (A7). Like ``item_values_are_known`` it guards the construction, not the
    data — this test exists so nobody reads it as protection against a chunk
    that silently lost rows. Nothing in either layer sees one.
    """

    def test_pandas_refuses_the_mismatch_before_validation_can_see_it(self):
        source = pd.DataFrame({"cust_id": ["C001", "C002", "C003"]})
        with pytest.raises(ValueError, match="All arrays must be of the same"):
            pd.DataFrame({
                "cust_id": source["cust_id"].astype(str).values,
                "score": [0.5, 0.6],
                "snap_date": SNAP_DATE,
                "prod_name": "exchange_fx",
            })


class TestItemValuesAreKnown:
    def test_an_integer_code_in_the_identity_column(self):
        """The failure ADR-0011 §1 reproduced on a real run.

        ``prod_name`` written as the ``category_mappings`` index instead of the
        name: the partition directories come out ``prod_name=0``…``7``, and every
        shape check passes because the scores themselves are fine.
        """
        out, source = _chunk()
        out["prod_name"] = "0"
        assert _failed_checks(out, source) == {"item_values_are_known"}

    def test_an_item_outside_the_configured_list(self):
        out, source = _chunk(item="fund_not_configured")
        assert _failed_checks(out, source) == {"item_values_are_known"}

    def test_a_configured_item_passes(self):
        for item in PRODUCTS:
            out, source = _chunk(item=item)
            _validate(out, source)


class TestFailuresAreCollected:
    def test_several_broken_things_are_reported_together(self):
        out, source = _chunk(scores=[np.nan, 0.5, 0.6], item="not_a_product")
        assert _failed_checks(out, source) == {
            "no_missing", "item_values_are_known",
        }


class TestWhichLayerEachCheckLivesIn:
    """ADR-0011 §3's table, pinned so the two layers cannot drift back into one.

    The batch-layer half of this pin is in ``test_validation.py``: it asserts
    that ``validate_predictions`` reports only :data:`BATCH_CHECKS`, and that a
    violation of a chunk-layer check is *not* re-reported there.
    """

    def test_the_two_registers_are_disjoint(self):
        assert set(CHUNK_CHECKS).isdisjoint(BATCH_CHECKS)

    def test_every_registered_chunk_check_is_reachable(self):
        """Each name in the register can actually be produced by a bad chunk.

        A register that lists a check nothing can trigger would let the layering
        assertions pass while the check itself had quietly been deleted.
        """
        triggered = set()
        out, source = _chunk()
        triggered |= _failed_checks(out.iloc[:1], source)
        out, source = _chunk(scores=[np.nan, 0.5, 0.6])
        triggered |= _failed_checks(out, source)
        out, source = _chunk(cust_ids=["C001", "C001", "C002"])
        triggered |= _failed_checks(out, source)
        out, source = _chunk(item="not_a_product")
        triggered |= _failed_checks(out, source)
        assert triggered == set(CHUNK_CHECKS)
        # Equality, not a subset: it is also the "reports nothing from the
        # batch register" assertion. A chunk holds one item, so a check named
        # `completeness` or `score_varies_within_group` here would be checking
        # something it structurally cannot see — and this would go red.
