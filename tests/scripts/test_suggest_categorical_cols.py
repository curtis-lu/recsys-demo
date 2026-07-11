"""Tests for suggest_categorical_cols script."""

import pytest
import yaml

from scripts.suggest_categorical_cols import (
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
        cats, _drops, implicit, n_rows = suggest_categorical_columns_spark(df)
        assert cats == ["s"]
        assert implicit == []

    def test_boolean_type_is_categorical(self, spark):
        df = spark.createDataFrame([(True,), (False,), (True,)], ["b"])
        cats, _drops, implicit, n_rows = suggest_categorical_columns_spark(df)
        assert cats == ["b"]
        assert implicit == []

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
        cats, _drops, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=5
        )
        assert set(cats) == {"i", "l", "d"}
        # Preserves schema order
        assert cats == ["i", "l", "d"]
        assert {name for name, _ in implicit} == {"i", "l", "d"}

    def test_high_cardinality_numeric_excluded(self, spark):
        rows = [(i,) for i in range(200)]
        df = spark.createDataFrame(rows, ["x"])
        cats, _drops, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=20
        )
        assert cats == []
        assert implicit == []

    def test_no_numeric_columns_does_not_crash(self, spark):
        df = spark.createDataFrame([("a", True), ("b", False)], ["s", "b"])
        cats, _drops, implicit, n_rows = suggest_categorical_columns_spark(df)
        assert cats == ["s", "b"]
        assert implicit == []

    def test_multiple_numeric_columns_single_pass(self, spark):
        """Verifies correctness when multiple numeric columns are aggregated together.

        This is a correctness proxy for the design contract: all numeric
        columns are computed in one agg. If the implementation accidentally
        used per-column aggs, the results would still be correct, but this
        test at least ensures mixed-cardinality numeric columns are handled.
        """
        rows = [(i, i % 3) for i in range(50)]
        df = spark.createDataFrame(rows, ["high", "low"])
        cats, _drops, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10
        )
        assert cats == ["low"]
        assert len(implicit) == 1
        assert implicit[0][0] == "low"
        # approx_count_distinct with rsd=0.05 should be exact (or near-exact)
        # on 3 distinct values
        assert implicit[0][1] == 3

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
        cats, _, _, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10
        )
        assert cats == ["z_int", "a_str", "m_bool"]


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
        cats, drops, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_string_cardinality=10
        )
        assert cats == ["seg"]                      # 低卡字串仍是 categorical
        assert [c for c, _ in drops] == ["raw_id"]  # 高卡字串導向 drop
        assert dict(drops)["raw_id"] >= 50          # 附 cardinality（approx，容忍誤差）

    def test_low_cardinality_string_stays_categorical(self, spark):
        df = spark.createDataFrame([("a",), ("b",), ("a",)], ["s"])
        cats, drops, implicit, n_rows = suggest_categorical_columns_spark(df)
        assert cats == ["s"]
        assert drops == []
