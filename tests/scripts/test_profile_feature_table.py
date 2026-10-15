"""Tests for scripts/profile_feature_table.py (stage one: basic checks).

The judgement rules are tested on hand-built stats (no Spark): each problem type
gets one column that is flagged and one that only just is not. The Spark side
gets its own tests for the empty-value cleaning (NaN, ±Infinity, empty string,
all-empty periods) and the batching.
"""

import json
import math

import pytest
from typer.testing import CliRunner

from scripts.profile_feature_table import (
    DEFAULT_THRESHOLDS,
    INT_EXACT,
    PROFILE_FILENAME,
    ProfileConfig,
    app,
    check_against_table,
    judge,
    judge_column,
    parse_config,
    profile_table,
    write_report,
)

TH = dict(DEFAULT_THRESHOLDS)
ROWS = [100, 100, 100]


def _col(null, mins, maxs=None, *, kind="numeric", dtype="bigint", has_decimal=False):
    return {
        "name": "x",
        "dtype": dtype,
        "kind": kind,
        "has_decimal": has_decimal,
        "null": list(null),
        "min": list(mins),
        "max": list(mins if maxs is None else maxs),
    }


def _varying(n=3):
    """Mins/maxs that differ inside every period: no 'only one value' anywhere."""
    return [0] * n, [10] * n


# ---------------------------------------------------------------------------
# Judgement rules (no Spark)
# ---------------------------------------------------------------------------


class TestNullFlags:
    def test_every_period_at_threshold_is_null_all(self):
        lo, hi = _varying()
        col = _col([99, 99, 100], lo, hi)
        assert judge_column(col, ROWS, TH) == ["null_all"]

    def test_one_period_just_below_threshold_is_not_null_all(self):
        lo, hi = _varying()
        col = _col([99, 98, 100], lo, hi)
        assert judge_column(col, ROWS, TH) == ["null_some"]

    def test_some_periods_at_threshold_is_null_some(self):
        lo, hi = _varying()
        col = _col([0, 99, 0], lo, hi)
        assert judge_column(col, ROWS, TH) == ["null_some"]

    def test_just_below_threshold_is_not_null_some(self):
        lo, hi = _varying()
        col = _col([0, 98, 0], lo, hi)
        assert judge_column(col, ROWS, TH) == []


class TestOneValueFlags:
    def test_same_single_value_everywhere_is_const_all(self):
        col = _col([0, 0, 0], [5, 5, 5])
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_one_period_with_a_second_value_is_not_const_all(self):
        col = _col([0, 0, 0], [5, 5, 5], [5, 6, 5])
        assert judge_column(col, ROWS, TH) == ["const_some"]

    def test_some_periods_with_one_value_is_const_some(self):
        col = _col([0, 0, 0], [0, 7, 0], [10, 7, 10])
        assert judge_column(col, ROWS, TH) == ["const_some"]

    def test_every_period_varying_is_not_const_some(self):
        lo, hi = _varying()
        assert judge_column(_col([0, 0, 0], lo, hi), ROWS, TH) == []

    def test_one_value_per_period_but_different_values_is_const_each(self):
        col = _col([0, 0, 0], [31.0, 31.5, 32.0], dtype="double", has_decimal=True)
        assert judge_column(col, ROWS, TH) == ["const_each"]

    def test_one_value_per_period_and_all_equal_is_not_const_each(self):
        col = _col([0, 0, 0], [31.0, 31.0, 31.0], dtype="double", has_decimal=True)
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_string_columns_compare_their_values(self):
        col = _col([0, 0, 0], ["N", "N", "N"], kind="string", dtype="string",
                   has_decimal=None)
        assert judge_column(col, ROWS, TH) == ["const_all"]
        col = _col([0, 0, 0], ["A", "B", "A"], kind="string", dtype="string",
                   has_decimal=None)
        assert judge_column(col, ROWS, TH) == ["const_each"]


class TestPrecisionFlag:
    def test_decimal_column_at_threshold_is_flagged(self):
        col = _col([0, 0, 0], [0.5, 0.5, 0.5], [10.5, 100000.0, 10.5],
                   dtype="double", has_decimal=True)
        assert judge_column(col, ROWS, TH) == ["precision"]

    def test_decimal_column_just_below_threshold_is_not_flagged(self):
        col = _col([0, 0, 0], [0.5, 0.5, 0.5], [10.5, 99999.99, 10.5],
                   dtype="double", has_decimal=True)
        assert judge_column(col, ROWS, TH) == []

    def test_integer_column_above_2_to_the_24_is_flagged(self):
        col = _col([0, 0, 0], [0, 0, 0], [1, INT_EXACT + 1, 1])
        assert judge_column(col, ROWS, TH) == ["precision"]

    def test_integer_column_at_2_to_the_24_is_not_flagged(self):
        # 2^24 本身存成 float32 一個不差
        col = _col([0, 0, 0], [0, 0, 0], [1, INT_EXACT, 1])
        assert judge_column(col, ROWS, TH) == []

    def test_large_negative_minimum_counts(self):
        col = _col([0, 0, 0], [0, -(INT_EXACT + 1), 0], [1, 1, 1])
        assert judge_column(col, ROWS, TH) == ["precision"]

    def test_float_typed_column_is_already_float32(self):
        col = _col([0, 0, 0], [0.5, 0.5, 0.5], [10.5, 100000.367, 10.5],
                   dtype="float", has_decimal=True)
        assert judge_column(col, ROWS, TH) == []

    def test_whole_valued_double_uses_the_integer_limit(self):
        # 沒有小數的數值欄位當整數看，不管宣告的型別
        col = _col([0, 0, 0], [0.0, 0.0, 0.0], [1.0, 200000.0, 1.0],
                   dtype="double", has_decimal=False)
        assert judge_column(col, ROWS, TH) == []

    def test_strings_are_never_flagged(self):
        col = _col([0, 0, 0], ["a", "a", "a"], ["zzz", "zzz", "zzz"],
                   kind="string", dtype="string", has_decimal=None)
        assert judge_column(col, ROWS, TH) == []

    def test_const_each_skips_precision(self):
        col = _col([0, 0, 0], [INT_EXACT, INT_EXACT + 1, INT_EXACT + 2])
        assert judge_column(col, ROWS, TH) == ["const_each"]


class TestOrderAndEdges:
    def test_const_all_drops_null_some(self):
        col = _col([0, 99, 0], [5, 5, 5])
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_null_some_and_const_each_coexist(self):
        col = _col([0, 99, 0], [1, 2, 3])
        assert judge_column(col, ROWS, TH) == ["null_some", "const_each"]

    def test_all_empty_period_is_neither_one_value_nor_varying(self):
        # 中間那期全空：不算「只有一個值」，所以另外兩期一樣是同一個值→整欄只有一個值
        col = _col([0, 100, 0], [5, None, 5])
        assert judge_column(col, ROWS, TH) == ["const_all"]
        # 另外兩期都有變化：全空那期不能讓它變成「部分期別只有一個值」
        col = _col([0, 100, 0], [0, None, 0], [9, None, 9])
        assert judge_column(col, ROWS, TH) == ["null_some"]

    def test_single_period_one_value_is_const_all_not_const_each(self):
        col = _col([0], [5])
        assert judge_column(col, [100], TH) == ["const_all"]

    def test_single_period_mostly_empty_is_null_all_not_null_some(self):
        col = _col([99], [0], [9])
        assert judge_column(col, [100], TH) == ["null_all"]

    def test_unchecked_columns_are_not_judged(self):
        profile = {
            "rows": ROWS,
            "columns": [
                {"name": "d", "dtype": "date", "kind": "unchecked"},
                dict(_col([0, 0, 0], [5, 5, 5]), name="c"),
            ],
        }
        assert judge(profile, TH) == {"c": ["const_all"]}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_GOOD = """
table: mydb.feature_table
time_col: snap_date
key_cols: [cust_id]
"""


class TestParseConfig:
    def test_minimal_config_uses_defaults(self):
        cfg, problems = parse_config(_GOOD)
        assert problems == []
        assert cfg.table == "mydb.feature_table"
        assert cfg.exclude_cols == []
        assert cfg.time_start is None and cfg.time_end is None
        assert cfg.where is None
        assert cfg.thresholds == DEFAULT_THRESHOLDS
        assert cfg.spark == {}

    def test_thresholds_override_only_what_is_written(self):
        cfg, problems = parse_config(_GOOD + "thresholds:\n  null_ratio: 0.9\n")
        assert problems == []
        assert cfg.thresholds == {**DEFAULT_THRESHOLDS, "null_ratio": 0.9}

    def test_unquoted_dates_in_time_range_become_strings(self):
        cfg, problems = parse_config(
            _GOOD + "time_range:\n  start: 2024-01-31\n  end: 2025-02-28\n"
        )
        assert problems == []
        assert (cfg.time_start, cfg.time_end) == ("2024-01-31", "2025-02-28")

    def test_every_problem_is_listed_at_once(self):
        text = """
time_col: snap_date
exlude_cols: [x]
thresholds:
  null_ratio: 1.5
  psi: 0.25
spark:
  spark.executor.cores: 1.5
"""
        _, problems = parse_config(text)
        joined = "\n".join(problems)
        for needle in ("table", "key_cols", "exlude_cols", "psi", "null_ratio",
                       "spark.executor.cores"):
            assert needle in joined, needle
        assert len(problems) == 6

    def test_broken_yaml_is_one_problem(self):
        _, problems = parse_config("table: [unclosed\n")
        assert len(problems) == 1 and "yaml" in problems[0]

    def test_template_parses_and_lists_every_key(self):
        from pathlib import Path

        import yaml

        from scripts import profile_feature_table as pft

        path = Path(pft.__file__).with_name("profile_feature_table.template.yaml")
        text = path.read_text(encoding="utf-8")
        _, problems = parse_config(text)
        assert problems == []
        raw = yaml.safe_load(text)
        assert set(raw) == set(pft._REQUIRED_KEYS) | set(pft._OPTIONAL_KEYS)
        assert set(raw["thresholds"]) == set(DEFAULT_THRESHOLDS)


class TestCheckAgainstTable:
    def test_missing_table(self):
        cfg, _ = parse_config(_GOOD)
        assert check_against_table(cfg, None) == ["表 mydb.feature_table 不存在"]

    def test_missing_columns_listed_together(self):
        cfg, _ = parse_config(_GOOD + "exclude_cols: [etl_ts, gone]\n")
        problems = check_against_table(cfg, ["SNAP_DATE", "etl_ts", "f1"])
        assert problems == [
            "key_cols 的 cust_id 不在表 mydb.feature_table 裡",
            "exclude_cols 的 gone 不在表 mydb.feature_table 裡",
        ]


# ---------------------------------------------------------------------------
# render: profile.json → judgement → report, no Spark
# ---------------------------------------------------------------------------


def _profile(tmp_path):
    profile = {
        "schema_version": 1,
        "generated_at": "2026-10-15T14:02:00",
        "config_text": _GOOD,
        "config": {
            "table": "mydb.feature_table", "time_col": "snap_date",
            "key_cols": ["cust_id"], "exclude_cols": [],
            "time_range": {"start": None, "end": None}, "where": None,
            "thresholds": dict(DEFAULT_THRESHOLDS),
        },
        "periods": ["2024-01-31", "2024-02-29", "2024-03-31"],
        "rows": ROWS,
        "null_time_rows": 0,
        "elapsed_seconds": 75.0,
        "batch_size": 200,
        "columns": [
            dict(_col([95, 0, 0], [0, 0, 0], [9, 9, 9]), name="half_empty"),
            dict(_col([0, 0, 0], [3, 3, 3]), name="sys_version"),
            {"name": "load_dt", "dtype": "date", "kind": "unchecked"},
        ],
    }
    (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
    return profile


class TestRender:
    def test_new_thresholds_change_the_judgement(self, tmp_path):
        _profile(tmp_path)
        runner = CliRunner()
        result = runner.invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        flags_page = (tmp_path / "01-flags.html").read_text()
        assert "部分期別幾乎是空值（0 欄）" in flags_page

        cfg = tmp_path / "loose.yaml"
        cfg.write_text(_GOOD + "thresholds:\n  null_ratio: 0.95\n")
        result = runner.invoke(app, ["render", str(tmp_path), "--config", str(cfg)])
        assert result.exit_code == 0, result.output
        flags_page = (tmp_path / "01-flags.html").read_text()
        assert "部分期別幾乎是空值（1 欄）" in flags_page
        assert "half_empty" in flags_page
        index = (tmp_path / "index.html").read_text()
        assert "0.95" in index

    def test_report_pages_and_paste_ready_exclude_list(self, tmp_path):
        profile = _profile(tmp_path)
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        names = sorted(p.name for p in tmp_path.iterdir())
        assert names == ["01-flags.html", "02-basic.html", "05-all.html",
                         "index.html", "plotly.min.js", PROFILE_FILENAME]
        flags_page = (tmp_path / "01-flags.html").read_text()
        assert "exclude: [sys_version]" in flags_page
        all_page = (tmp_path / "05-all.html").read_text()
        assert "load_dt" in all_page
        # 設計文件第六節：不用箭頭
        for page in tmp_path.glob("*.html"):
            assert "→" not in page.read_text(), page.name

    def test_table_name_is_escaped_in_the_index(self, tmp_path):
        profile = _profile(tmp_path)
        profile["config"]["table"] = "db.<b>t</b>"
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        index = (tmp_path / "index.html").read_text()
        assert "<b>t</b>" not in index
        assert "&lt;b&gt;t&lt;/b&gt;" in index


# ---------------------------------------------------------------------------
# Spark: statistics
# ---------------------------------------------------------------------------


def _cfg(view, **kw):
    base = dict(text=_GOOD, table=view, time_col="t", key_cols=["k"])
    base.update(kw)
    return ProfileConfig(**base)


def _by_name(profile):
    return {c["name"]: c for c in profile["columns"]}


@pytest.mark.spark
class TestProfileTable:
    def test_nan_infinity_and_empty_string_count_as_empty(self, spark):
        rows = [
            ("2024-01", 1, 1.5, "a", True),
            ("2024-01", 2, float("nan"), "", False),
            ("2024-01", 3, float("inf"), None, None),
            ("2024-01", 4, float("-inf"), "a", True),
        ]
        df = spark.createDataFrame(rows, "t string, k int, d double, s string, b boolean")
        df.createOrReplaceTempView("fp_empty_values")
        profile = profile_table(spark, _cfg("fp_empty_values"), batch_size=10)
        cols = _by_name(profile)
        assert profile["periods"] == ["2024-01"] and profile["rows"] == [4]
        assert cols["d"]["null"] == [3]
        assert cols["d"]["min"] == [1.5] and cols["d"]["max"] == [1.5]
        assert cols["d"]["has_decimal"] is True
        assert cols["s"]["null"] == [2]
        assert cols["s"]["kind"] == "string"
        assert cols["b"]["kind"] == "string"
        assert cols["b"]["min"] == ["false"] and cols["b"]["max"] == ["true"]
        assert "k" not in cols and "t" not in cols

    def test_all_empty_period_and_one_value_column(self, spark):
        rows = [
            ("2024-01", 1, None, 3.0),
            ("2024-01", 2, None, 3.0),
            ("2024-02", 3, 10, 3.0),
            ("2024-02", 4, 20, 3.0),
        ]
        df = spark.createDataFrame(rows, "t string, k int, v bigint, c double")
        df.createOrReplaceTempView("fp_empty_period")
        profile = profile_table(spark, _cfg("fp_empty_period"), batch_size=10)
        cols = _by_name(profile)
        assert cols["v"]["null"] == [2, 0]
        assert cols["v"]["min"] == [None, 10] and cols["v"]["max"] == [None, 20]
        assert cols["c"]["has_decimal"] is False
        flags = judge(profile, DEFAULT_THRESHOLDS)
        assert flags["v"] == ["null_some"]
        assert flags["c"] == ["const_all"]

    def test_batches_give_the_same_stats(self, spark):
        rows = [("2024-01", i, i, i * 2, i * 0.5, "s") for i in range(5)]
        df = spark.createDataFrame(rows, "t string, k int, a int, b bigint, c double, s string")
        df.createOrReplaceTempView("fp_batches")
        one = profile_table(spark, _cfg("fp_batches"), batch_size=1)
        many = profile_table(spark, _cfg("fp_batches"), batch_size=100)
        assert one["columns"] == many["columns"]

    def test_decimal_fraction_and_unchecked_types(self, spark):
        df = spark.sql(
            "SELECT '2024-01' AS t, 1 AS k, CAST(12.50 AS DECIMAL(18,2)) AS dec_frac, "
            "CAST(12 AS DECIMAL(18,2)) AS dec_whole, DATE'2024-01-31' AS load_dt"
        )
        df.createOrReplaceTempView("fp_types")
        cols = _by_name(profile_table(spark, _cfg("fp_types"), batch_size=10))
        assert cols["dec_frac"]["has_decimal"] is True
        assert cols["dec_whole"]["has_decimal"] is False
        assert cols["dec_frac"]["max"] == [12.5]
        assert cols["load_dt"] == {"name": "load_dt", "dtype": "date", "kind": "unchecked"}

    def test_time_range_where_and_null_time_rows(self, spark):
        rows = [
            ("2024-01", 1, 1), ("2024-02", 2, 2), ("2024-03", 3, 3),
            ("2024-03", 4, 99), (None, 5, 5), (None, 6, 6),
        ]
        df = spark.createDataFrame(rows, "t string, k int, v int")
        df.createOrReplaceTempView("fp_filters")
        cfg = _cfg("fp_filters", time_start="2024-02", time_end="2024-03", where="v < 50")
        profile = profile_table(spark, cfg, batch_size=10)
        assert profile["periods"] == ["2024-02", "2024-03"]
        assert profile["rows"] == [1, 1]
        assert profile["null_time_rows"] == 2
        assert math.isclose(profile["config"]["thresholds"]["null_ratio"], 0.99)


@pytest.mark.spark
class TestRunCommand:
    def test_config_problems_listed_at_once_then_exit(self, spark, tmp_path):
        spark.createDataFrame([("2024-01", 1, 1)], "t string, k int, v int") \
            .createOrReplaceTempView("fp_cli_bad")
        cfg = tmp_path / "bad.yaml"
        cfg.write_text(
            "table: fp_cli_bad\ntime_col: t\nexclude_cols: [gone]\n"
            "thresholds:\n  psi: 0.2\n"
        )
        result = CliRunner().invoke(app, ["run", "--config", str(cfg)])
        assert result.exit_code == 1
        for needle in ("key_cols", "gone", "psi"):
            assert needle in result.output, result.output

    def test_run_writes_profile_and_report(self, spark, tmp_path):
        rows = [("2024-01", i, i, None) for i in range(3)] + \
               [("2024-02", i, i, None) for i in range(3)]
        spark.createDataFrame(rows, "t string, k int, v int, dead double") \
            .createOrReplaceTempView("fp_cli_ok")
        out = tmp_path / "out"
        cfg = tmp_path / "ok.yaml"
        cfg.write_text(
            f"table: fp_cli_ok\ntime_col: t\nkey_cols: [k]\noutput_dir: {out}\n"
        )
        result = CliRunner().invoke(app, ["run", "--config", str(cfg)])
        assert result.exit_code == 0, result.output
        profile = json.loads((out / PROFILE_FILENAME).read_text())
        assert profile["periods"] == ["2024-01", "2024-02"]
        assert "dead" in (out / "01-flags.html").read_text()
