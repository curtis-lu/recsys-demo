"""Tests for the assembly of a split's ``model_input``: the two joins, the
output column rule, and the column-existence guard behind both.

The assembly itself is the ``build_model_input`` *node* (#170 lifted it out of
``model_input.py``, which now holds only the mechanisms each step is made of);
the guard is still a mechanism, so the two imports point at different modules.
Only the imports moved — every assertion below is the one that was written
against the helper.
"""

import pandas as pd
import pytest

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.steps.model_input import require_columns_present
from recsys_tfb.pipelines.dataset.nodes import build_model_input


class TestValidateColumns:
    """D1 — the required-column guard every caller leans on for its error text.

    ``build_model_input`` calls this twice with two different contexts, and
    ADR-0005 turned the missing-item case from a silent fallback into a raise —
    so the message is now the whole user-facing product of that decision, not a
    debugging aid. It is tested here rather than through a caller because the
    caller tests can only reach it with the column sets *they* happen to build.
    """

    def test_missing_columns_raise_naming_all_of_them(self):
        with pytest.raises(ValueError) as ei:
            require_columns_present(["a", "b"], ["a", "b", "c", "d"], "some_node")
        msg = str(ei.value)
        # Sorted, so the text is stable across set-iteration order rather than
        # matching whatever order this particular run produced.
        assert "['c', 'd']" in msg
        assert "some_node" in msg

    def test_context_appears_so_two_call_sites_are_distinguishable(self):
        """The same function guards two column sets inside build_model_input.

        A shared message would let a ``pytest.raises(match=...)`` aimed at one
        be satisfied by the other — the false-green form ADR-0005 calls out by
        name.
        """
        with pytest.raises(ValueError, match=r"build_model_input keys: \['prod_name'\]"):
            require_columns_present(
                ["snap_date", "cust_id"],
                ["snap_date", "cust_id", "prod_name"],
                "build_model_input keys",
            )

    def test_extra_columns_are_not_an_error(self):
        """Only *missing* required columns fail; the frame may carry more.

        Every caller passes a frame wider than ``required`` (keys carry carry
        columns, ``dataset`` carries the whole joined width), so a subset check
        in the other direction would reject every real call.
        """
        assert require_columns_present(["a", "b", "c"], ["a"], "ctx") is None

    def test_exact_match_passes(self):
        assert require_columns_present(["a", "b"], ["a", "b"], "ctx") is None


@pytest.mark.spark
class TestBuildModelInputCarry:
    def _prep(self):
        return {"feature_columns": ["prod_name", "f1"],
                "categorical_columns": ["prod_name"],
                "category_mappings": {"prod_name": ["a", "b"]},
                "drop_columns": []}

    def _params(self):
        return {"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}}}

    def _frames(self, spark, with_carry):
        kcols = {"snap_date": pd.to_datetime(["2025-01-31"] * 2),
                 "cust_id": [1, 2], "prod_name": ["a", "b"]}
        if with_carry:
            kcols["cust_segment_typ"] = ["mass", "hnw"]
        keys = spark.createDataFrame(pd.DataFrame(kcols))
        # apply_start_date is here to be *dropped*: it is not identity, not a
        # feature, not the label, and not in keys, so the output rule excludes
        # it. Without such a column the expected set degenerates into "the union
        # of every input column" — which is what `dataset` already is, so the
        # equality below could not tell a correct selection apart from no
        # selection at all. (label_table really does carry these; see the
        # label_table fixture in test_nodes.py.)
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "prod_name": ["a", "b"], "label": [1, 0],
            "apply_start_date": pd.to_datetime(["2025-02-01"] * 2)}))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 2], "f1": [0.1, 0.2]}))
        return keys, feats, labels

    def _expected_columns(self, keys):
        """identity ∪ {label} ∪ feature_columns ∪ whatever the keys carried in.

        Asserted as an equality rather than ``in`` / ``not in`` so an extra
        column — a join column leaking through, a carry column that should have
        been filtered out — fails too. Only has that power because the label
        frame carries a column the rule must drop; see ``_frames``.

        Takes ``feature_columns`` from the preprocessor, so it checks that
        build_model_input honours the metadata it was handed — NOT that the
        metadata itself is right. Whether compute_feature_columns picked the
        correct columns is a separate contract with its own tests.
        """
        schema = get_schema(self._params())
        return (
            set(schema["identity_columns"])
            | {schema["label"]}
            | set(self._prep()["feature_columns"])
            | set(keys.columns)
        )

    def test_carry_in_output_when_present_in_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=True)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" in out.columns
        assert set(out.columns) == self._expected_columns(keys)
        assert out.count() == keys.count()

    def test_no_carry_when_absent_from_keys(self, spark):
        keys, feats, labels = self._frames(spark, with_carry=False)
        out = build_model_input(keys, feats, labels, self._prep(), self._params())
        assert "cust_segment_typ" not in out.columns
        assert set(out.columns) == self._expected_columns(keys)
        assert out.count() == keys.count()


@pytest.mark.spark
class TestBuildModelInputJoinContract:
    """D14/D13/D10/D11 — what the two LEFT joins promise (ADR-0005 §3).

    Both joins are LEFT on purpose, and the reasons differ:

    - **label_table is sparse.** Only entities with a transaction have a row, so
      a key with no label row is the normal case and means "negative". The
      ``coalesce(label, 0)`` is that decision.
    - **feature_table has a different upstream population than sample_pool.**
      A key whose ``(time, entity)`` is absent produces an all-NULL *feature*
      row that stays in model_input; LightGBM handles the missing values. This
      is a legal output, not a bug — which is why the assertion is the contract
      itself rather than the fail-loud "no NULL features" the audit first
      proposed.

    Both are pinned by row count, so turning either join INNER turns this red —
    the mutation #140 names as currently passing 115 tests.

    The frame is built so the four miss/hit combinations are all present and
    separable. ``cust_id=9`` is absent from the *feature* frame but still has a
    label row on product "a" — that pairing is the point: it is what lets
    ``test_label_join_miss_becomes_a_negative`` show a feature miss does not
    cost a row its real label, which "fill 0 everywhere" would also satisfy if
    every feature-missing row happened to be label-missing too.
    """

    PREP = {
        "feature_columns": ["prod_name", "f1"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["a", "b"]},
        "drop_columns": [],
    }
    PARAMS = {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"],
        "item": "prod_name", "label": "label"}}}

    @pytest.fixture
    def built(self, spark):
        keys = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            # cust 1 is in feature_table, cust 9 is not.
            "cust_id": [1, 1, 9, 9],
            "prod_name": ["a", "b", "a", "b"],
        }))
        # Labels exist only for the "a" rows: the "b" rows are label-join misses.
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "cust_id": [1, 9], "prod_name": ["a", "a"], "label": [1, 1],
        }))
        feats = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"]),
            "cust_id": [1], "f1": [0.5],
        }))
        out = build_model_input(keys, feats, labels, self.PREP, self.PARAMS)
        return keys, out.orderBy("cust_id", "prod_name").toPandas()

    def test_row_count_equals_keys_regardless_of_either_miss(self, built):
        """D14 — keys' grain is the output's grain even when both joins miss.

        This is the single assertion that turns red for INNER on either side:
        INNER on features drops the cust 9 rows, INNER on labels drops the "b"
        rows.
        """
        keys, pdf = built
        assert len(pdf) == keys.count() == 4

    def test_feature_join_miss_yields_null_features_not_a_dropped_row(self, built):
        """D14 — the all-NULL feature row is the contract, not a defect.

        Asserted only on the feature_table-sourced column: ``prod_name`` is also
        in ``feature_columns`` but arrives from *keys*, so "every feature column
        is NULL" would be false here and would misstate the contract.
        """
        _, pdf = built
        missing = pdf[pdf["cust_id"] == 9]
        assert len(missing) == 2
        assert missing["f1"].isna().all()
        # The identity-sourced feature is untouched by the miss.
        assert missing["prod_name"].tolist() == ["a", "b"]
        # ...and the entity that *is* present did get its feature, so this is
        # evidence about the join rather than about f1 being NULL everywhere.
        assert pdf[pdf["cust_id"] == 1]["f1"].notna().all()

    def test_label_join_miss_becomes_a_negative(self, built):
        """D13 — "sparse label_table, a miss is a negative" is a contract.

        Both halves matter: the misses become 0 *and* the hits keep their 1. An
        implementation that dropped the label join entirely and filled 0
        everywhere would satisfy the first half alone.
        """
        _, pdf = built
        by_key = {(r.cust_id, r.prod_name): r.label for r in pdf.itertuples()}
        assert by_key[(1, "b")] == 0
        assert by_key[(9, "b")] == 0
        assert by_key[(1, "a")] == 1
        # A feature-join miss must not cost the row its real label.
        assert by_key[(9, "a")] == 1

    def test_label_is_never_null(self, built):
        """D10 — training must not see a NULL label; coalesce is what stops it."""
        _, pdf = built
        assert pdf["label"].notna().all()

    def test_label_domain_is_zero_or_one(self, built):
        """D11 — a binary label column, asserted as a set so a stray 2 fails."""
        _, pdf = built
        assert set(pdf["label"].unique()) <= {0, 1}
