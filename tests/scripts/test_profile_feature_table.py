"""Tests for scripts/profile_feature_table.py (stage one: basic checks).

The judgement rules are tested on hand-built stats (no Spark): each problem type
gets one column that is flagged and one that only just is not. The report is a
single page; its tests read the HTML the way the page's JS does (ids, data-*
attributes). The Spark side gets its own tests for the empty-value cleaning
(NaN, ±Infinity, empty string, all-empty periods) and the batching.
"""

import json
import math

import pytest
from typer.testing import CliRunner

from scripts.profile_feature_table import (
    DEFAULT_THRESHOLDS,
    PROFILE_FILENAME,
    ProfileConfig,
    actions,
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


def _col(null, mins, maxs=None, *, kind="numeric", dtype="bigint"):
    return {
        "name": "x",
        "dtype": dtype,
        "kind": kind,
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
        col = _col([0, 0, 0], [31.0, 31.5, 32.0], dtype="double")
        assert judge_column(col, ROWS, TH) == ["const_each"]

    def test_one_value_per_period_and_all_equal_is_not_const_each(self):
        col = _col([0, 0, 0], [31.0, 31.0, 31.0], dtype="double")
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_string_columns_compare_their_values(self):
        col = _col([0, 0, 0], ["N", "N", "N"], kind="string", dtype="string")
        assert judge_column(col, ROWS, TH) == ["const_all"]
        col = _col([0, 0, 0], ["A", "B", "A"], kind="string", dtype="string")
        assert judge_column(col, ROWS, TH) == ["const_each"]


class TestNoFloat32Check:
    def test_large_values_are_not_a_problem(self):
        # float32 精度檢查已拿掉：對 LightGBM 幾乎沒影響（設計文件附錄二）
        col = _col([0, 0, 0], [0.5, 0.5, 0.5], [1.5, 272_528_123.98, 1.5], dtype="double")
        assert judge_column(col, ROWS, TH) == []
        assert judge_column(_col([0, 0, 0], [0, 0, 0], [1, 2**24 + 1, 1]), ROWS, TH) == []


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


class TestActions:
    """判斷結果到總結裡的三種做法。"""

    def _actions(self, col, rows=ROWS):
        return actions(col, rows, judge_column(col, rows, TH), TH)

    def test_jump_of_at_least_10_points_means_upstream(self):
        # 99% − 89% 剛好 10 個百分點（浮點數相減是 0.0999…，要算成 10）
        lo, hi = _varying()
        assert self._actions(_col([89, 99, 89], lo, hi)) == ["upstream"]

    def test_jump_below_10_points_means_judge(self):
        lo, hi = _varying()
        assert self._actions(_col([90, 99, 90], lo, hi)) == ["judge"]

    def test_const_some_means_upstream_and_const_each_means_judge(self):
        assert self._actions(_col([0, 0, 0], [0, 7, 0], [10, 7, 10])) == ["upstream"]
        assert self._actions(_col([0, 0, 0], [1, 2, 3])) == ["judge"]

    def test_two_actions_are_both_listed(self):
        # 中間那期斷掉（跳幅 96 個百分點），另外兩期各只有一個值、值不同
        assert self._actions(_col([4, 100, 4], [1, None, 2])) == ["upstream", "judge"]

    def test_excludable_columns_only_get_exclude(self):
        assert self._actions(_col([0, 0, 0], [5, 5, 5])) == ["exclude"]
        lo, hi = _varying()
        assert self._actions(_col([99, 100, 99], lo, hi)) == ["exclude"]

    def test_no_problem_no_action(self):
        lo, hi = _varying()
        assert self._actions(_col([0, 0, 0], lo, hi)) == []


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
"""
        _, problems = parse_config(text)
        joined = "\n".join(problems)
        for needle in ("table", "key_cols", "exlude_cols", "psi", "null_ratio"):
            assert needle in joined, needle
        assert len(problems) == 5

    def test_removed_float32_threshold_is_an_unknown_key(self):
        _, problems = parse_config(_GOOD + "thresholds:\n  precision_max_abs: 100000\n")
        assert len(problems) == 1 and "precision_max_abs" in problems[0]

    def test_spark_settings_are_not_a_config_key(self):
        # 同 scripts/sampling_overrides_editor.py：Spark 設定全由 SPARK_CONF_DIR 決定
        _, problems = parse_config(_GOOD + "spark:\n  spark.executor.memory: 8g\n")
        assert len(problems) == 1 and "spark" in problems[0]

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


def _section_html(page, sid):
    return page.split(f'<section id="{sid}">')[1].split("</section>")[0]


class TestRender:
    def test_new_thresholds_change_the_judgement(self, tmp_path):
        _profile(tmp_path)
        runner = CliRunner()
        result = runner.invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        page = (tmp_path / "index.html").read_text()
        assert "部分期別幾乎是空值（0 欄）" in page

        cfg = tmp_path / "loose.yaml"
        cfg.write_text(_GOOD + "thresholds:\n  null_ratio: 0.95\n")
        result = runner.invoke(app, ["render", str(tmp_path), "--config", str(cfg)])
        assert result.exit_code == 0, result.output
        page = (tmp_path / "index.html").read_text()
        assert "部分期別幾乎是空值（1 欄）" in page
        assert 'data-name="half_empty"' in _section_html(page, "s-null-some")
        assert "0.95" in _section_html(page, "s-scope")

    def test_old_profile_with_the_removed_float32_threshold_still_renders(self, tmp_path):
        # 上一版存的 profile.json 帶著 precision_max_abs 與 has_decimal
        profile = _profile(tmp_path)
        profile["config"]["thresholds"]["precision_max_abs"] = 100000
        for c in profile["columns"]:
            if c["kind"] != "unchecked":
                c["has_decimal"] = False
        (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
        result = CliRunner().invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "precision_max_abs" not in _section_html((tmp_path / "index.html").read_text(), "s-scope")

    def test_single_page_without_framework_wording(self, tmp_path):
        profile = _profile(tmp_path)
        path = write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        assert path == tmp_path / "index.html"
        assert sorted(p.name for p in tmp_path.iterdir()) == ["index.html", PROFILE_FILENAME]
        page = path.read_text()
        assert "load_dt" in _section_html(page, "s-scope")
        # 使用者要求：報告不跟框架耦合，不提框架的設定或 pipeline
        for word in ("dataset", "training", "feature_selection", "drop_columns", "float32", "LightGBM"):
            assert word not in page, word
        # 設計文件第六節：不用箭頭
        assert "→" not in page

    def test_element_ids_are_unique(self, tmp_path):
        # 重複的 id 讓 getElementById 拿到 section 而不是表格，整段 JS 在載入時就壞掉
        import re
        from collections import Counter

        profile = _profile(tmp_path)
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        ids = re.findall(r'\sid="([^"]+)"', (tmp_path / "index.html").read_text())
        assert [i for i, n in Counter(ids).items() if n > 1] == []
        assert {"t-all", "col-data", "col-half_empty"} <= set(ids)

    def test_summary_groups_columns_and_lists_upstream_periods(self, tmp_path):
        profile = _profile(tmp_path)
        profile["columns"] += [
            dict(_col([4, 100, 4], [0, None, 0], [9, None, 9]), name="cc_bal"),     # 跳幅 96：上游
            dict(_col([95, 99, 95], [0, 0, 0], [9, 9, 9]), name="loan_amt"),        # 跳幅 4：看業務意義
        ]
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        summary = _section_html(page, "s-actions")
        exclude, upstream, judge_row = summary.split("<tr>")[2:5]
        assert 'data-copy-text="sys_version"' in exclude
        assert 'data-copy-text="cc_bal"' in upstream and "<b>2024-02</b>" in upstream
        assert "loan_amt" not in upstream
        assert 'data-copy-text="loan_amt"' in judge_row
        # 期別篩選用期別的序號當代號，期別字串裡有空白也不會切壞
        some = _section_html(page, "s-null-some")
        assert 'data-name="cc_bal" data-bad="p1"' in some
        assert '<option value="p1">2024-02</option>' in some

    def test_copied_names_are_one_per_line(self, tmp_path):
        profile = _profile(tmp_path)
        profile["columns"].append(dict(_col([0, 0, 0], [1, 1, 1]), name="flag_b"))
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        exclude = _section_html((tmp_path / "index.html").read_text(), "s-actions").split("<tr>")[2]
        assert 'data-copy-text="sys_version\nflag_b"' in exclude or 'data-copy-text="flag_b\nsys_version"' in exclude

    def test_table_name_is_escaped(self, tmp_path):
        profile = _profile(tmp_path)
        profile["config"]["table"] = "db.<b>t</b>"
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        assert "<b>t</b>" not in page
        assert "&lt;b&gt;t&lt;/b&gt;" in page

    def test_string_values_cannot_close_the_embedded_json(self, tmp_path):
        profile = _profile(tmp_path)
        profile["columns"].append(dict(_col([0, 0, 0], ["</script>"] * 3, ["z"] * 3, kind="string",
                                            dtype="string"), name="s"))
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        # 只有兩個 <script>：內嵌的 JSON 與主程式；值裡的 </script> 被換成 <\/script>
        assert page.count("</script>") == 2
        assert "<\\/script>" in page


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

    def test_decimal_and_unchecked_types(self, spark):
        df = spark.sql(
            "SELECT '2024-01' AS t, 1 AS k, CAST(12.50 AS DECIMAL(18,2)) AS dec_frac, "
            "CAST(12 AS DECIMAL(18,2)) AS dec_whole, DATE'2024-01-31' AS load_dt"
        )
        df.createOrReplaceTempView("fp_types")
        cols = _by_name(profile_table(spark, _cfg("fp_types"), batch_size=10))
        assert cols["dec_frac"]["max"] == [12.5]
        assert cols["dec_whole"]["min"] == [12.0]
        assert "has_decimal" not in cols["dec_frac"]  # 只給 float32 檢查用，已拿掉
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
        page = (out / "index.html").read_text()
        assert 'data-name="dead"' in _section_html(page, "s-null-all")
