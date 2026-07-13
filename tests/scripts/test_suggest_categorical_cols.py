"""Tests for suggest_categorical_cols script."""

from pathlib import Path

import pytest
import yaml

from scripts.suggest_categorical_cols import (
    ColumnScan,
    SubsetInfo,
    _apply_subset,
    _render_summary_lines,
    format_yaml_output,
    suggest_categorical_columns_spark,
)


# ---------------------------------------------------------------------------
# Spark function tests
# ---------------------------------------------------------------------------


@pytest.mark.spark
class TestSuggestCategoricalColumnsSpark:
    def test_string_type_is_categorical(self, spark):
        df = spark.createDataFrame([("a",), ("b",), ("c",)], ["s"])
        result = suggest_categorical_columns_spark(df)
        assert result.categorical == ["s"]
        assert result.implicit == []

    def test_boolean_type_is_categorical(self, spark):
        df = spark.createDataFrame([(True,), (False,), (True,)], ["b"])
        result = suggest_categorical_columns_spark(df)
        assert result.categorical == ["b"]
        assert result.implicit == []

    def test_low_cardinality_numeric_is_implicit(self, spark):
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            LongType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("i", IntegerType()),
                StructField("l", LongType()),
                StructField("d", DoubleType()),
            ]
        )
        rows = [(1, 10, 1.5), (2, 20, 2.5), (1, 10, 1.5), (2, 20, 2.5)]
        df = spark.createDataFrame(rows, schema)
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=5
        )
        assert set(result.categorical) == {"i", "l", "d"}
        # Preserves schema order
        assert result.categorical == ["i", "l", "d"]
        assert {name for name, _ in result.implicit} == {"i", "l", "d"}

    def test_high_cardinality_numeric_is_a_numeric_feature(self, spark):
        rows = [(i,) for i in range(200)]
        df = spark.createDataFrame(rows, ["x"])
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=20
        )
        assert result.categorical == []
        assert result.implicit == []
        assert result.numeric_features == ["x"]  # kept as numeric, not dropped

    def test_no_numeric_columns_does_not_crash(self, spark):
        df = spark.createDataFrame([("a", True), ("b", False)], ["s", "b"])
        result = suggest_categorical_columns_spark(df)
        assert result.categorical == ["s", "b"]
        assert result.implicit == []

    def test_multiple_numeric_columns_single_pass(self, spark):
        """Verifies correctness when multiple numeric columns are aggregated together.

        This is a correctness proxy for the design contract: all numeric
        columns are computed in one agg. If the implementation accidentally
        used per-column aggs, the results would still be correct, but this
        test at least ensures mixed-cardinality numeric columns are handled.
        """
        rows = [(i, i % 3) for i in range(50)]
        df = spark.createDataFrame(rows, ["high", "low"])
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10
        )
        assert result.categorical == ["low"]
        assert len(result.implicit) == 1
        assert result.implicit[0][0] == "low"
        # approx_count_distinct with rsd=0.05 should be exact (or near-exact)
        # on 3 distinct values
        assert result.implicit[0][1] == 3

    def test_mixed_types_preserves_schema_order(self, spark):
        from pyspark.sql.types import (
            BooleanType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("z_int", IntegerType()),
                StructField("a_str", StringType()),
                StructField("m_bool", BooleanType()),
            ]
        )
        rows = [(1, "x", True), (2, "y", False), (1, "x", True)]
        df = spark.createDataFrame(rows, schema)
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10
        )
        assert result.categorical == ["z_int", "a_str", "m_bool"]


# ---------------------------------------------------------------------------
# format_yaml_output tests
# ---------------------------------------------------------------------------


class TestFormatYamlOutput:
    def test_typical_list_emits_both_blocks(self):
        out = format_yaml_output(["a", "b", "c"])
        assert out.startswith('categorical_columns:\n  - "a"\n  - "b"\n  - "c"\n')
        assert "drop_columns:" in out  # 空 drop 仍列出，標明已檢查

    def test_empty_categorical_still_has_headers(self):
        out = format_yaml_output([])
        assert out.startswith("categorical_columns:\n")
        assert "drop_columns:" in out

    def test_output_is_valid_yaml(self):
        out = format_yaml_output(["col_a", "col_b", "status_code"])
        parsed = yaml.safe_load(out)
        # 空 drop 塊（只有註解）→ drop_columns: None
        assert parsed == {
            "categorical_columns": ["col_a", "col_b", "status_code"],
            "drop_columns": None,
        }

    def test_drop_suggestions_render_with_cardinality(self):
        out = format_yaml_output(["seg"], [("raw_id", 4200)])
        assert "categorical_columns:" in out
        assert "drop_columns:" in out
        assert '- "raw_id"' in out
        assert "4200" in out  # cardinality 註解
        parsed = yaml.safe_load(out)
        assert parsed == {"categorical_columns": ["seg"], "drop_columns": ["raw_id"]}


@pytest.mark.spark
class TestStringDropRouting:
    def test_high_cardinality_string_routed_to_drop(self, spark):
        rows = [(f"id_{i}", "seg_a") for i in range(60)]
        df = spark.createDataFrame(rows, ["raw_id", "seg"])
        result = suggest_categorical_columns_spark(
            df, max_string_cardinality=10
        )
        drops = result.drop_suggestions
        assert result.categorical == ["seg"]        # 低卡字串仍是 categorical
        assert [c for c, _ in drops] == ["raw_id"]  # 高卡字串導向 drop
        assert dict(drops)["raw_id"] >= 50          # 附 cardinality（approx，容忍誤差）

    def test_low_cardinality_string_stays_categorical(self, spark):
        df = spark.createDataFrame([("a",), ("b",), ("a",)], ["s"])
        result = suggest_categorical_columns_spark(df)
        assert result.categorical == ["s"]
        assert result.drop_suggestions == []


# ---------------------------------------------------------------------------
# Column completeness (req 3a): every schema column lands in exactly one
# bucket; non-numeric / non-string types (date/timestamp/binary/complex) are
# surfaced for human review rather than silently ignored (they are the B6
# object-dtype OOM footgun when left as un-encoded features).
# ---------------------------------------------------------------------------


def _mixed_type_df(spark):
    import datetime

    from pyspark.sql.types import (
        BinaryType,
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("low_num", IntegerType()),   # 2 distinct  -> categorical
            StructField("high_num", IntegerType()),  # 60 distinct -> numeric feature
            StructField("low_str", StringType()),    # 2 distinct  -> categorical
            StructField("high_str", StringType()),   # 60 distinct -> drop
            StructField("d", DateType()),            # non-numeric -> review
            StructField("ts", TimestampType()),      # non-numeric -> review
            StructField("blob", BinaryType()),       # non-numeric -> review
        ]
    )
    rows = [
        (
            i % 2,
            i,
            f"s{i % 2}",
            f"id{i}",
            datetime.date(2026, 1, 1),
            datetime.datetime(2026, 1, 1, 0, 0, 0),
            b"x",
        )
        for i in range(60)
    ]
    return spark.createDataFrame(rows, schema)


@pytest.mark.spark
class TestColumnCompleteness:
    def test_nonnumeric_nonstring_columns_surface_in_review(self, spark):
        df = _mixed_type_df(spark)
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10, max_string_cardinality=10
        )
        assert {c for c, _type in result.review} == {"d", "ts", "blob"}

    def test_review_entries_carry_the_spark_type(self, spark):
        df = _mixed_type_df(spark)
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10, max_string_cardinality=10
        )
        review = dict(result.review)
        assert review["d"] == "date"
        assert review["ts"] == "timestamp"
        assert review["blob"] == "binary"

    def test_every_schema_column_lands_in_exactly_one_bucket(self, spark):
        df = _mixed_type_df(spark)
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10, max_string_cardinality=10
        )
        cat = set(result.categorical)
        drop = {c for c, _n in result.drop_suggestions}
        numf = set(result.numeric_features)
        rev = {c for c, _t in result.review}
        # (a) union covers every schema column — nothing silently dropped
        assert cat | drop | numf | rev == set(df.columns)
        # (b) pairwise disjoint — no column double-counted
        buckets = [cat, drop, numf, rev]
        for a in range(len(buckets)):
            for b in range(a + 1, len(buckets)):
                assert buckets[a].isdisjoint(buckets[b])

    def test_expected_bucket_membership(self, spark):
        df = _mixed_type_df(spark)
        result = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10, max_string_cardinality=10
        )
        assert result.categorical == ["low_num", "low_str"]  # schema order
        assert [c for c, _n in result.drop_suggestions] == ["high_str"]
        assert result.numeric_features == ["high_num"]


# ---------------------------------------------------------------------------
# Subset scan (req 1 partition pruning via --where; req 2 --sample-fraction).
# ``where`` is a Spark SQL predicate (Spark prunes partitions when it targets a
# partition column); ``fraction`` is a random Bernoulli row sample. Both
# compose: filter to partitions first, then sample within.
# ---------------------------------------------------------------------------


@pytest.mark.spark
class TestApplySubset:
    def test_no_args_is_identity(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["x"])
        assert _apply_subset(df).count() == 2

    def test_where_filters_rows(self, spark):
        df = spark.createDataFrame(
            [(1, "a"), (2, "a"), (3, "b")], ["id", "grp"]
        )
        assert _apply_subset(df, where="grp = 'a'").count() == 2

    def test_sample_fraction_reduces_rows(self, spark):
        df = spark.createDataFrame([(i,) for i in range(1000)], ["x"])
        n = _apply_subset(df, fraction=0.3).count()
        assert 0 < n < 1000

    def test_where_and_sample_compose(self, spark):
        rows = [(i, "a" if i % 2 == 0 else "b") for i in range(1000)]
        df = spark.createDataFrame(rows, ["x", "grp"])
        # where -> 500 rows, then fraction 0.5 -> fewer than the filtered 500
        n = _apply_subset(df, where="grp = 'a'", fraction=0.5).count()
        assert 0 < n < 500

    def test_fraction_out_of_range_raises(self, spark):
        df = spark.createDataFrame([(1,)], ["x"])
        with pytest.raises(ValueError):
            _apply_subset(df, fraction=1.5)
        with pytest.raises(ValueError):
            _apply_subset(df, fraction=0.0)

    def test_sample_is_reproducible(self, spark):
        """Fixed seed → same subset across runs (a config-derivation tool must
        give the same suggestion when re-run on the same data)."""
        df = spark.createDataFrame([(i,) for i in range(1000)], ["x"])
        n1 = _apply_subset(df, fraction=0.3).count()
        n2 = _apply_subset(df, fraction=0.3).count()
        assert n1 == n2


# ---------------------------------------------------------------------------
# Terminal summary <-> YAML consistency (req 3b) + review block in output.
# Both surfaces render from the SAME ColumnScan, so the actionable column set
# (categorical + drop + review) is identical in both — this is the regression
# guard for the observed drift where string categoricals were counted in the
# terminal but only enumerated in the YAML file. Pure (no Spark).
# ---------------------------------------------------------------------------


def _sample_scan():
    return ColumnScan(
        categorical=["low_num", "low_str"],
        drop_suggestions=[("high_str", 99)],
        implicit=[("low_num", 3)],
        numeric_features=["high_num"],
        review=[("d", "date"), ("blob", "binary")],
        n_rows=100,
    )


class TestSummaryYamlConsistency:
    def test_terminal_and_yaml_enumerate_the_same_actionable_columns(self):
        scan = _sample_scan()
        yaml_out = format_yaml_output(
            scan.categorical, scan.drop_suggestions, scan.review
        )
        summary = "\n".join(
            _render_summary_lines(
                scan, source="s", max_cardinality=20, n_cols=6,
                output_path=Path("out.yaml"),
            )
        )
        for col in ["low_num", "low_str", "high_str", "d", "blob"]:
            assert col in yaml_out, f"{col} missing from YAML"
            assert col in summary, f"{col} missing from terminal"

    def test_terminal_lists_string_categoricals_by_name(self):
        # regression: string categoricals were previously only counted, not named
        summary = "\n".join(
            _render_summary_lines(
                _sample_scan(), "s", 20, 6, Path("o.yaml")
            )
        )
        assert "low_str" in summary

    def test_reconciliation_line_accounts_for_every_column(self):
        summary = "\n".join(
            _render_summary_lines(
                _sample_scan(), "s", 20, 6, Path("o.yaml")
            )
        )
        # 2 categorical + 1 numeric-feature + 1 drop + 2 review = 6 columns
        assert "6 columns" in summary


class TestReviewBlockInYaml:
    def test_review_block_present_but_commented(self):
        out = format_yaml_output(["seg"], [("raw_id", 99)], [("d", "date")])
        assert "d" in out       # surfaced in the text
        assert "date" in out    # with its Spark type
        # ...but as comments only, so the config keys stay categorical + drop
        parsed = yaml.safe_load(out)
        assert parsed == {"categorical_columns": ["seg"], "drop_columns": ["raw_id"]}

    def test_no_review_leaves_output_unchanged(self):
        with_none = format_yaml_output(["seg"], [("raw_id", 99)], None)
        assert yaml.safe_load(with_none) == {
            "categorical_columns": ["seg"],
            "drop_columns": ["raw_id"],
        }


# ---------------------------------------------------------------------------
# Subset provenance (Q4): the summary ALWAYS states the scan scope; a subset
# scan under-estimates cardinality, so its YAML carries a lower-bound warning
# ("verify before trusting"). A full scan carries no warning. Pure (no Spark).
# ---------------------------------------------------------------------------


class TestProvenance:
    def test_summary_states_full_scan_scope(self):
        text = "\n".join(
            _render_summary_lines(
                _sample_scan(), "s", 20, 6, Path("o.yaml"), subset=SubsetInfo()
            )
        )
        assert "full" in text.lower()  # provenance line present, says full

    def test_summary_states_subset_provenance(self):
        subset = SubsetInfo(where="snap_date = '2026-06-30'", fraction=0.1)
        text = "\n".join(
            _render_summary_lines(
                _sample_scan(), "s", 20, 6, Path("o.yaml"), subset=subset
            )
        )
        assert "snap_date = '2026-06-30'" in text
        assert "0.1" in text

    def test_yaml_warns_low_card_is_lower_bound_when_subset(self):
        out = format_yaml_output(
            ["seg"], None, None, subset=SubsetInfo(fraction=0.1)
        )
        low = out.lower()
        assert "lower bound" in low or "verify" in low
        # warning is a comment — YAML still parses to the same keys
        assert yaml.safe_load(out) == {
            "categorical_columns": ["seg"],
            "drop_columns": None,
        }

    def test_yaml_has_no_warning_on_full_scan(self):
        out = format_yaml_output(["seg"], None, None, subset=None)
        assert "lower bound" not in out.lower()
