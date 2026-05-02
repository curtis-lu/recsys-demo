"""Tests for suggest_categorical_cols script."""

import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from scripts.suggest_categorical_cols import (
    format_yaml_output,
    suggest_categorical_columns_pandas,
    suggest_categorical_columns_spark,
)

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "suggest_categorical_cols.py"


# ---------------------------------------------------------------------------
# Pandas function tests
# ---------------------------------------------------------------------------


class TestSuggestCategoricalColumnsPandas:
    def test_string_dtype_is_categorical(self):
        df = pd.DataFrame({"s": pd.Series(["a", "b", "c"], dtype="string")})
        cats, implicit = suggest_categorical_columns_pandas(df)
        assert cats == ["s"]
        assert implicit == []

    def test_object_dtype_is_categorical(self):
        df = pd.DataFrame({"o": ["a", "b", "c"]})  # default object dtype
        cats, implicit = suggest_categorical_columns_pandas(df)
        assert cats == ["o"]
        assert implicit == []

    def test_bool_dtype_is_categorical(self):
        df = pd.DataFrame({"b": [True, False, True, False]})
        cats, implicit = suggest_categorical_columns_pandas(df)
        assert cats == ["b"]
        assert implicit == []

    def test_pandas_categorical_dtype(self):
        df = pd.DataFrame({"c": pd.Categorical(["x", "y", "x", "z"])})
        cats, implicit = suggest_categorical_columns_pandas(df)
        assert cats == ["c"]
        assert implicit == []

    def test_low_cardinality_numeric_is_implicit_categorical(self):
        df = pd.DataFrame({"status": [1, 2, 3, 1, 2, 3, 1]})
        cats, implicit = suggest_categorical_columns_pandas(
            df, max_numerical_cardinality=20
        )
        assert cats == ["status"]
        assert implicit == [("status", 3)]

    def test_high_cardinality_numeric_is_excluded(self):
        df = pd.DataFrame({"x": list(range(100))})
        cats, implicit = suggest_categorical_columns_pandas(
            df, max_numerical_cardinality=20
        )
        assert cats == []
        assert implicit == []

    def test_cardinality_equal_to_threshold_included(self):
        # nunique == max_numerical_cardinality should be included (<=)
        df = pd.DataFrame({"x": list(range(5))})
        cats, implicit = suggest_categorical_columns_pandas(
            df, max_numerical_cardinality=5
        )
        assert cats == ["x"]
        assert implicit == [("x", 5)]

    def test_cardinality_one_above_threshold_excluded(self):
        df = pd.DataFrame({"x": list(range(6))})
        cats, implicit = suggest_categorical_columns_pandas(
            df, max_numerical_cardinality=5
        )
        assert cats == []
        assert implicit == []

    def test_mixed_dataframe_preserves_column_order(self):
        df = pd.DataFrame(
            {
                "z_status": [1, 2, 1],           # implicit numeric (last letter z)
                "a_name": ["x", "y", "z"],        # string
                "m_value": np.linspace(0, 100, 3),  # float, low nunique but still numeric
                "b_flag": [True, False, True],    # bool
            }
        )
        cats, _ = suggest_categorical_columns_pandas(df, max_numerical_cardinality=20)
        # Must preserve original column order, not alphabetical
        assert cats == ["z_status", "a_name", "m_value", "b_flag"]

    def test_nan_not_counted_in_cardinality(self):
        # Without dropna: 3 distinct (1, 2, NaN). With dropna: 2 distinct.
        df = pd.DataFrame({"x": [1, 2, np.nan, 1, 2]})
        cats, implicit = suggest_categorical_columns_pandas(
            df, max_numerical_cardinality=2
        )
        assert cats == ["x"]
        assert implicit == [("x", 2)]

    def test_implicit_contains_name_and_nunique(self):
        df = pd.DataFrame({"flag": [0, 1, 0, 1, 0]})
        cats, implicit = suggest_categorical_columns_pandas(df)
        assert cats == ["flag"]
        assert len(implicit) == 1
        col, n = implicit[0]
        assert col == "flag"
        assert n == 2

    def test_mixed_numeric_and_string_classification(self):
        n = 100
        df = pd.DataFrame(
            {
                "cat_str": ["a", "b", "c"] * (n // 3) + ["a"],  # 3 uniques
                "cont_num": list(range(n)),                      # 100 uniques
                "low_num": [1, 2, 3] * (n // 3) + [1],           # 3 uniques
                "cat_bool": [True, False] * (n // 2),            # 2 uniques
            }
        )
        cats, implicit = suggest_categorical_columns_pandas(
            df, max_numerical_cardinality=5
        )
        assert cats == ["cat_str", "low_num", "cat_bool"]
        assert implicit == [("low_num", 3)]


# ---------------------------------------------------------------------------
# Spark function tests
# ---------------------------------------------------------------------------


class TestSuggestCategoricalColumnsSpark:
    def test_string_type_is_categorical(self, spark):
        df = spark.createDataFrame([("a",), ("b",), ("c",)], ["s"])
        cats, implicit, n_rows = suggest_categorical_columns_spark(df)
        assert cats == ["s"]
        assert implicit == []

    def test_boolean_type_is_categorical(self, spark):
        df = spark.createDataFrame([(True,), (False,), (True,)], ["b"])
        cats, implicit, n_rows = suggest_categorical_columns_spark(df)
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
        cats, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=5
        )
        assert set(cats) == {"i", "l", "d"}
        # Preserves schema order
        assert cats == ["i", "l", "d"]
        assert {name for name, _ in implicit} == {"i", "l", "d"}

    def test_high_cardinality_numeric_excluded(self, spark):
        rows = [(i,) for i in range(200)]
        df = spark.createDataFrame(rows, ["x"])
        cats, implicit, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=20
        )
        assert cats == []
        assert implicit == []

    def test_no_numeric_columns_does_not_crash(self, spark):
        df = spark.createDataFrame([("a", True), ("b", False)], ["s", "b"])
        cats, implicit, n_rows = suggest_categorical_columns_spark(df)
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
        cats, implicit, n_rows = suggest_categorical_columns_spark(
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
        cats, _, n_rows = suggest_categorical_columns_spark(
            df, max_numerical_cardinality=10
        )
        assert cats == ["z_int", "a_str", "m_bool"]


# ---------------------------------------------------------------------------
# format_yaml_output tests
# ---------------------------------------------------------------------------


class TestFormatYamlOutput:
    def test_typical_list(self):
        out = format_yaml_output(["a", "b", "c"])
        assert out == 'categorical_columns:\n  - "a"\n  - "b"\n  - "c"\n'

    def test_empty_list(self):
        out = format_yaml_output([])
        assert out == "categorical_columns:\n"

    def test_output_is_valid_yaml(self):
        out = format_yaml_output(["col_a", "col_b", "status_code"])
        parsed = yaml.safe_load(out)
        assert parsed == {"categorical_columns": ["col_a", "col_b", "status_code"]}

    def test_empty_list_parses_to_none(self):
        parsed = yaml.safe_load(format_yaml_output([]))
        # A header with no items parses to None in YAML
        assert parsed == {"categorical_columns": None}


# ---------------------------------------------------------------------------
# CLI end-to-end tests (via subprocess)
# ---------------------------------------------------------------------------


def _make_fixture_parquet(path: Path) -> None:
    df = pd.DataFrame(
        {
            "name": ["alice", "bob", "carol", "dave"],
            "status": [1, 2, 1, 2],
            "age": [25, 30, 35, 40],  # 4 distinct values
            "score": np.linspace(0.0, 100.0, 4),  # 4 distinct floats
        }
    )
    df.to_parquet(path)


def _run_cli(cwd: Path, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )


class TestCLI:
    def test_pandas_end_to_end_writes_output_file(self, tmp_path):
        parquet_path = tmp_path / "toy.parquet"
        _make_fixture_parquet(parquet_path)

        result = _run_cli(tmp_path, [str(parquet_path)])
        assert result.returncode == 0, f"stderr: {result.stderr}"

        output_file = tmp_path / "data" / "profiling" / "toy_categorical.yaml"
        assert output_file.exists(), "Output YAML file was not created"

    def test_output_yaml_is_loadable_and_contains_expected_columns(self, tmp_path):
        parquet_path = tmp_path / "toy.parquet"
        _make_fixture_parquet(parquet_path)

        result = _run_cli(tmp_path, [str(parquet_path), "-k", "10"])
        assert result.returncode == 0, f"stderr: {result.stderr}"

        output_file = tmp_path / "data" / "profiling" / "toy_categorical.yaml"
        parsed = yaml.safe_load(output_file.read_text())
        assert "categorical_columns" in parsed
        cols = parsed["categorical_columns"]
        # name is a string -> categorical
        assert "name" in cols
        # status has nunique=2 -> implicit categorical (<=10)
        assert "status" in cols
        # age has nunique=4 -> implicit categorical (<=10)
        assert "age" in cols

    def test_max_cardinality_zero_excludes_all_numerics(self, tmp_path):
        parquet_path = tmp_path / "toy.parquet"
        _make_fixture_parquet(parquet_path)

        result = _run_cli(tmp_path, [str(parquet_path), "--max-cardinality", "0"])
        assert result.returncode == 0, f"stderr: {result.stderr}"

        output_file = tmp_path / "data" / "profiling" / "toy_categorical.yaml"
        parsed = yaml.safe_load(output_file.read_text())
        cols = parsed["categorical_columns"] or []
        # Only 'name' (string) should remain; no numeric columns
        assert cols == ["name"]

    def test_nonexistent_input_path_exits_with_error(self, tmp_path):
        result = _run_cli(tmp_path, [str(tmp_path / "does_not_exist.parquet")])
        assert result.returncode != 0
        assert "not found" in result.stderr.lower()

    def test_unknown_backend_exits_with_error(self, tmp_path):
        parquet_path = tmp_path / "toy.parquet"
        _make_fixture_parquet(parquet_path)

        result = _run_cli(tmp_path, [str(parquet_path), "--backend", "invalid"])
        assert result.returncode != 0
        assert "unknown backend" in result.stderr.lower()

    def test_output_stem_derived_from_parquet_filename(self, tmp_path):
        parquet_path = tmp_path / "customer_profile.parquet"
        _make_fixture_parquet(parquet_path)

        result = _run_cli(tmp_path, [str(parquet_path)])
        assert result.returncode == 0, f"stderr: {result.stderr}"

        expected = tmp_path / "data" / "profiling" / "customer_profile_categorical.yaml"
        assert expected.exists()
