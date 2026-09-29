"""The scored-frame mechanism both scoring pipelines write through — pure pandas.

No SparkSession and no model: ``recsys_tfb.score_output`` imports pandas,
numpy and the standard library only, so the checks that decide whether a
scored chunk reaches a table at all run in milliseconds.

What the two pipelines *decide* with it (which columns identify a row, which
columns must not be null, what to carry) is passed in by each caller; these
tests pin that the mechanism answers with exactly what it was handed.
"""

import ast
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.score_output import (
    ScoredChunkError,
    ScoredFrameLayout,
    require_scored_chunk,
    require_single_partition,
    scored_chunk_failures,
)

# The frames below spell these columns; the module under test never does.
TIME, ENTITY, ITEM, EVENT, LABEL = "snap_date", "cust_id", "prod_name", "imp_id", "label"
IDENTITY = [TIME, ENTITY, ITEM]


def _pair(cust_ids=("C1", "C2", "C3"), scores=None, events=None):
    """One chunk's (out, source) pair, entity stringified the way callers do."""
    cust_ids = list(cust_ids)
    source = pd.DataFrame({ENTITY: cust_ids, "f": [float(i) for i in range(len(cust_ids))]})
    out = pd.DataFrame({
        ENTITY: source[ENTITY].astype(str).values,
        "score": [0.5] * len(cust_ids) if scores is None else list(scores),
        TIME: "2025-01-31",
        ITEM: "fund",
    })
    if events is not None:
        source[EVENT] = list(events)
        out[EVENT] = list(events)
    return out, source


def _checks(out, source, *, identity=IDENTITY, not_null=(TIME, ITEM, "score")):
    return {
        f["check"]
        for f in scored_chunk_failures(
            out, source, entity_cols=[ENTITY], identity_cols=list(identity),
            not_null_cols=list(not_null),
        )
    }


class TestScoredChunkFailures:
    def test_a_well_formed_chunk_has_no_failures(self):
        out, source = _pair()
        assert _checks(out, source) == set()

    def test_row_count_mismatch(self):
        out, source = _pair()
        failures = scored_chunk_failures(
            out.iloc[:2], source, entity_cols=[ENTITY], identity_cols=IDENTITY,
            not_null_cols=["score"],
        )
        assert failures == [{
            "check": "chunk_row_count", "detail": "scored 2 rows from 3 entities",
        }]

    def test_a_null_in_a_not_null_column_is_read_off_the_output(self):
        out, source = _pair(scores=[0.1, np.nan, 0.3])
        failures = scored_chunk_failures(
            out, source, entity_cols=[ENTITY], identity_cols=IDENTITY,
            not_null_cols=[TIME, ITEM, "score"],
        )
        assert failures == [{
            "check": "no_missing", "detail": "NaN values found: {'score': 1}",
        }]

    def test_a_column_left_out_of_not_null_is_not_checked(self):
        """The caller decides which output columns must be filled; a null in
        one it did not name is not this mechanism's business."""
        out, source = _pair(scores=[0.1, np.nan, 0.3])
        assert _checks(out, source, not_null=(TIME, ITEM)) == set()

    def test_a_null_entity_is_read_off_the_source(self):
        """The output's entity went through ``astype(str)``, so the null is the
        *string* ``"None"`` there and an output-side check could never fire."""
        out, source = _pair(cust_ids=("C1", None, "C3"))
        assert out[ENTITY].tolist() == ["C1", "None", "C3"]
        failures = scored_chunk_failures(
            out, source, entity_cols=[ENTITY], identity_cols=IDENTITY,
            not_null_cols=["score"],
        )
        assert failures == [{
            "check": "no_missing", "detail": f"NaN values found: {{'{ENTITY}': 1}}",
        }]

    def test_the_entity_is_not_read_off_the_output(self):
        """A null in the output's entity column with a clean source is not
        reported: entity nulls are a source-side question only."""
        out, source = _pair()
        out.loc[1, ENTITY] = None
        assert _checks(out, source) == set()

    def test_duplicates_on_the_given_identity(self):
        out, source = _pair(cust_ids=("C1", "C1", "C2"))
        failures = scored_chunk_failures(
            out, source, entity_cols=[ENTITY], identity_cols=IDENTITY,
            not_null_cols=["score"],
        )
        assert failures == [{
            "check": "no_duplicates",
            "detail": f"1 duplicate rows on {IDENTITY}",
        }]

    def test_rows_distinct_only_by_event_are_duplicates_without_it(self):
        """Same (time, entity, item) twice, told apart only by the event column:
        a duplicate under the narrow identity, not under the one with event."""
        out, source = _pair(cust_ids=("C1", "C1"), events=("i1", "i2"))
        assert "no_duplicates" not in _checks(out, source, identity=IDENTITY + [EVENT])
        assert "no_duplicates" in _checks(out, source, identity=IDENTITY)

    def test_every_failure_is_reported_together(self):
        out, source = _pair(cust_ids=("C1", "C1", None), scores=[0.1, np.nan, 0.3])
        failures = scored_chunk_failures(
            out.iloc[:2], source, entity_cols=[ENTITY], identity_cols=IDENTITY,
            not_null_cols=["score"],
        )
        assert [f["check"] for f in failures] == [
            "chunk_row_count", "no_missing", "no_duplicates",
        ]


class TestRequireScoredChunk:
    def test_passes_quietly(self):
        out, source = _pair()
        require_scored_chunk(
            out, source, entity_cols=[ENTITY], identity_cols=IDENTITY,
            not_null_cols=["score"],
        )

    def test_raises_with_every_failure(self):
        out, source = _pair(cust_ids=("C1", "C1", "C2"), scores=[0.1, np.nan, 0.3])
        with pytest.raises(ScoredChunkError) as exc_info:
            require_scored_chunk(
                out, source, entity_cols=[ENTITY], identity_cols=IDENTITY,
                not_null_cols=["score"],
            )
        assert [f["check"] for f in exc_info.value.failures] == [
            "no_missing", "no_duplicates",
        ]
        assert str(exc_info.value) == (
            "2 sanity check(s) failed: no_missing, no_duplicates"
        )


class TestRequireSinglePartition:
    def test_one_partition_passes(self):
        out, _ = _pair()
        require_single_partition(out, [TIME, ITEM])

    def test_a_frame_spanning_two_partitions_is_refused(self):
        out, _ = _pair()
        out.loc[2, ITEM] = "ccard"
        with pytest.raises(ValueError, match="exactly one partition"):
            require_single_partition(out, [TIME, ITEM])


class TestScoredFrameLayout:
    def _source(self):
        return pd.DataFrame({
            ENTITY: pd.Series([101, 102, None], dtype=object),
            "acct": ["a1", "a2", "a3"],
            EVENT: pd.to_datetime(["2025-01-01 10:00", "2025-01-01 09:00",
                                   "2025-01-02 00:00"]),
            LABEL: [1, 0, 0],
            "f": [0.1, 0.2, 0.3],
        })

    def _layout(self, **kw):
        return ScoredFrameLayout(
            entity_cols=[ENTITY, "acct"], score_col="pred",
            carried_cols=[EVENT, LABEL], null_cols=["w"], **kw,
        )

    def test_source_columns_are_entity_then_carried_without_repeats(self):
        layout = ScoredFrameLayout(
            entity_cols=[ENTITY], score_col="pred", carried_cols=[LABEL, ENTITY, LABEL],
        )
        assert layout.source_columns() == [ENTITY, LABEL]

    def test_build(self):
        source = self._source()
        scores = np.array([0.9, 0.8, 0.7])
        out = self._layout().build(source, scores, {TIME: "2025-01-31", ITEM: "fund"})

        assert list(out.columns) == [
            ENTITY, "acct", "pred", "score_uncalibrated", EVENT, LABEL, "w",
            TIME, ITEM,
        ]
        # Entity columns become strings, a null included.
        assert out[ENTITY].tolist() == ["101", "102", "None"]
        assert all(isinstance(v, str) for v in out[ENTITY])
        # The score lands under the schema's name; the deprecated column equals it.
        np.testing.assert_array_equal(out["pred"].to_numpy(), scores)
        np.testing.assert_array_equal(out["score_uncalibrated"].to_numpy(), scores)
        # Carried columns keep their own type.
        assert out[EVENT].dtype == source[EVENT].dtype
        assert out[EVENT].tolist() == source[EVENT].tolist()
        assert out[LABEL].tolist() == [1, 0, 0]
        # Declared-but-absent columns are NULL on every row.
        assert out["w"].tolist() == [None, None, None]
        # Partition values are constants.
        assert out[TIME].tolist() == ["2025-01-31"] * 3
        assert out[ITEM].tolist() == ["fund"] * 3

    def test_the_score_column_is_the_one_it_was_given(self):
        out = ScoredFrameLayout(entity_cols=[ENTITY], score_col="pred").build(
            self._source(), np.zeros(3), {TIME: "t"})
        assert "pred" in out.columns
        assert "score" not in out.columns

    def test_build_ignores_source_row_labels(self):
        """A filtered source keeps its old index; the output is positional."""
        source = self._source().iloc[[2, 0]]
        out = ScoredFrameLayout(entity_cols=["acct"], score_col="pred").build(
            source, np.array([1.0, 2.0]), {TIME: "t"})
        assert out["acct"].tolist() == ["a3", "a1"]
        assert out["pred"].tolist() == [1.0, 2.0]


def test_the_module_imports_nothing_from_the_project_and_no_spark():
    """Both pipelines import it, so it sits below both and reaches back into
    neither; and it stays millisecond-testable."""
    import recsys_tfb.score_output as mod

    tree = ast.parse(Path(mod.__file__).read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported <= {"__future__", "collections", "dataclasses", "typing",
                        "numpy", "pandas"}, imported
