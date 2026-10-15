"""特徵表檢查工具：算一張特徵表每一期、每一欄的統計量，標出要處理的欄位。

設計文件：``docs/notes/2026-10-15-feature-table-profiling-design.md``；使用說明：
``docs/operations/user-guides/feature-table-profiling.md``。這一版是第一階段，
只有基本檢查（設計文件第五節的前六種問題）；分布偏移與極端值、相關性是後兩
階段，所以設定檔還沒有 ``checks`` 區塊，``thresholds`` 也只認這一階段用得到的鍵。

Usage:
    python scripts/profile_feature_table.py run --config my_profile.yaml
    python scripts/profile_feature_table.py render <輸出資料夾> [--config my_profile.yaml]

``run`` 開 Spark 算統計量，存成 ``profile.json`` 再畫報告；``render`` 只讀
``profile.json`` 重新判斷、重畫，不開 Spark。所以 ``profile.json`` 只存統計量、
不存判斷結果：判斷全在 ``judge``，吃統計量與門檻。

跟框架的關係：不讀 ``conf/``，判斷規則也不沿用框架的函式（例如 float32 精度直接
依 float32 本身算，不用 dataset 的精度閘門 B8），只借 ``recsys_tfb.report`` 畫頁面、
``get_or_create_spark_session`` 開 Spark。
"""

from __future__ import annotations

import datetime as dt
import html
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import typer
import yaml

from recsys_tfb.report import Page, ReportSection
from recsys_tfb.report.figures import heatmap
from recsys_tfb.report.pages import write_pages

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame as SparkDataFrame, SparkSession

SCHEMA_VERSION = 1
PROFILE_FILENAME = "profile.json"
OUTPUT_ROOT = Path("data/profiling/feature_profile")

# float32 能一個不差存下來的最大整數（2^24）。是 float32 本身的性質，所以不開設定鍵。
INT_EXACT = 2**24
DEFAULT_THRESHOLDS: dict[str, float] = {"null_ratio": 0.99, "precision_max_abs": 100000}
# 每批幾欄。一次 groupBy 帶上千個聚合式會讓執行計畫過大；確切大小要在正式環境的
# 真表上量過再定（設計文件第八節），量之前先用 --batch-size 試。
DEFAULT_BATCH_SIZE = 200
# 02 的空值熱圖只畫至少有一期空值比例到這個值的欄位（設計文件第六節）
NULL_HEATMAP_MIN = 0.005

_REQUIRED_KEYS = ("table", "time_col", "key_cols")
_OPTIONAL_KEYS = ("exclude_cols", "time_range", "where", "thresholds", "spark", "output_dir")
_TIME_ALIAS = "__profile_time__"

app = typer.Typer(help="檢查一張特徵表每一期的狀況，產出 HTML 報告", add_completion=False)


# ---------------------------------------------------------------------------
# 設定檔
# ---------------------------------------------------------------------------


@dataclass
class ProfileConfig:
    """解析過的設定檔。欄位寫錯時留預設值，問題另外列在 ``parse_config`` 的回傳。"""

    text: str = ""
    table: str | None = None
    time_col: str | None = None
    key_cols: list[str] = field(default_factory=list)
    exclude_cols: list[str] = field(default_factory=list)
    time_start: str | int | None = None
    time_end: str | int | None = None
    where: str | None = None
    thresholds: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_THRESHOLDS))
    spark: dict[str, Any] = field(default_factory=dict)
    output_dir: str | None = None


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _column_list(raw: Any, key: str, problems: list[str]) -> list[str]:
    if not isinstance(raw, list) or not all(isinstance(x, str) and x for x in raw):
        problems.append(f"{key} 要是欄名清單，例如 [cust_id]；現在是 {raw!r}")
        return []
    return list(raw)


def parse_config(text: str) -> tuple[ProfileConfig, list[str]]:
    """檢查不用開 Spark 就能檢查的部分，回傳設定與**所有**問題。

    不在第一個問題就停：工具使用者改一個、跑一次、再看到下一個，很浪費時間。
    表與欄位存不存在要開 Spark 才知道，在 ``check_against_table``。
    """
    cfg = ProfileConfig(text=text)
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return cfg, [f"設定檔不是合法的 yaml：{exc}"]
    if not isinstance(raw, dict):
        return cfg, ["設定檔最外層要是「鍵: 值」的對照表"]

    problems: list[str] = []
    for key in _REQUIRED_KEYS:
        if raw.get(key) is None:
            problems.append(f"必填鍵 {key} 沒寫")
    unknown = sorted(str(k) for k in set(raw) - set(_REQUIRED_KEYS) - set(_OPTIONAL_KEYS))
    if unknown:
        problems.append(
            f"不認得的鍵：{', '.join(unknown)}"
            "（認得的鍵見 scripts/profile_feature_table.template.yaml）"
        )

    for key in ("table", "time_col"):
        v = raw.get(key)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            setattr(cfg, key, v.strip())
        else:
            problems.append(f"{key} 要是非空的字串；現在是 {v!r}")

    if raw.get("key_cols") is not None:
        cfg.key_cols = _column_list(raw["key_cols"], "key_cols", problems)
        if raw["key_cols"] == []:
            problems.append("key_cols 至少要寫一欄")
    if raw.get("exclude_cols") is not None:
        cfg.exclude_cols = _column_list(raw["exclude_cols"], "exclude_cols", problems)

    time_range = raw.get("time_range")
    if time_range is not None:
        if not isinstance(time_range, dict):
            problems.append("time_range 要是含 start、end 的對照表")
        else:
            extra = sorted(str(k) for k in set(time_range) - {"start", "end"})
            if extra:
                problems.append(f"time_range 不認得的鍵：{', '.join(extra)}（只有 start、end）")
            for k in ("start", "end"):
                v = time_range.get(k)
                if v is None:
                    continue
                if isinstance(v, dt.date):  # yaml 沒加引號的日期會被讀成 date
                    v = v.isoformat()
                if isinstance(v, (str, int)) and not isinstance(v, bool):
                    setattr(cfg, f"time_{k}", v)
                else:
                    problems.append(f"time_range.{k} 要是字串或整數；現在是 {v!r}")

    for key in ("where", "output_dir"):
        v = raw.get(key)
        if v is None:
            continue
        if isinstance(v, str):
            setattr(cfg, key, v)
        else:
            problems.append(f"{key} 要是字串；現在是 {v!r}")

    thresholds = raw.get("thresholds")
    if thresholds is not None:
        if not isinstance(thresholds, dict):
            problems.append("thresholds 要是「鍵: 值」的對照表")
        else:
            extra = sorted(str(k) for k in set(thresholds) - set(DEFAULT_THRESHOLDS))
            if extra:
                problems.append(
                    f"thresholds 不認得的鍵：{', '.join(extra)}"
                    f"（這一版只有 {'、'.join(DEFAULT_THRESHOLDS)}）"
                )
            for k in DEFAULT_THRESHOLDS:
                v = thresholds.get(k)
                if v is None:
                    continue
                if not _is_number(v):
                    problems.append(f"thresholds.{k} 要是數字；現在是 {v!r}")
                elif k == "null_ratio" and not 0 < v <= 1:
                    problems.append(f"thresholds.null_ratio 要大於 0、不超過 1；現在是 {v!r}")
                elif k == "precision_max_abs" and v <= 0:
                    problems.append(f"thresholds.precision_max_abs 要大於 0；現在是 {v!r}")
                else:
                    cfg.thresholds[k] = v

    spark = raw.get("spark")
    if spark is not None:
        if not isinstance(spark, dict):
            problems.append("spark 要是「鍵: 值」的對照表")
        else:
            # 同 recsys_tfb.utils.spark._validate_values：值只能是字串、整數或布林
            bad = [str(k) for k, v in spark.items() if not isinstance(v, (str, int, bool))]
            if bad:
                problems.append(f"spark 的值只能是字串、整數或布林：{', '.join(bad)}")
            else:
                cfg.spark = dict(spark)

    return cfg, problems


def check_against_table(cfg: ProfileConfig, columns: list[str] | None) -> list[str]:
    """``columns`` 是表的欄名；表不存在時傳 ``None``。欄名比對不分大小寫（同 Spark 預設）。"""
    if columns is None:
        return [f"表 {cfg.table} 不存在"]
    have = {c.lower() for c in columns}
    named = [("time_col", cfg.time_col)] if cfg.time_col else []
    named += [("key_cols", c) for c in cfg.key_cols]
    named += [("exclude_cols", c) for c in cfg.exclude_cols]
    return [
        f"{key} 的 {col} 不在表 {cfg.table} 裡"
        for key, col in named
        if col.lower() not in have
    ]


# ---------------------------------------------------------------------------
# Spark：統計量
# ---------------------------------------------------------------------------

_INTEGER_TYPES = {"tinyint", "smallint", "int", "bigint"}
_FLOAT_TYPES = {"float", "double"}


def column_kind(dtype: str) -> str:
    """``numeric``／``string``／``unchecked``。boolean 轉成字串，當字串欄位處理。"""
    if dtype in _INTEGER_TYPES or dtype in _FLOAT_TYPES or dtype.startswith("decimal"):
        return "numeric"
    if dtype in ("string", "boolean"):
        return "string"
    return "unchecked"


def _may_have_fraction(dtype: str) -> bool:
    return dtype in _FLOAT_TYPES or dtype.startswith("decimal")


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _cleaned(name: str, dtype: str) -> "Column":
    """把空值統一成 NULL：float／double 的 NaN 與 ±Infinity、字串的空字串。"""
    from pyspark.sql import functions as F

    c = F.col(_quote(name))
    if dtype in _FLOAT_TYPES:
        return F.when(F.isnan(c) | (F.abs(c) == float("inf")), F.lit(None)).otherwise(c)
    if dtype == "string":
        return F.when(c == "", F.lit(None)).otherwise(c)
    if dtype == "boolean":
        return c.cast("string")
    return c


def _json_value(v: Any) -> Any:
    # decimal 存成 float：只用來顯示與比門檻；兩個 decimal 要到第 16 位有效數字
    # 以後才不同，才會被當成同一個值。
    return float(v) if isinstance(v, Decimal) else v


def _period_label(v: Any) -> str:
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return v.isoformat()
    return str(v)


def read_table(spark: "SparkSession", cfg: ProfileConfig) -> "SparkDataFrame":
    """套上 ``where`` 與 ``time_range``。``time_col`` 是 NULL 的列刻意留著（見下）。"""
    from pyspark.sql import functions as F

    df = spark.table(cfg.table)
    if cfg.where:
        df = df.where(cfg.where)
    t = F.col(_quote(cfg.time_col))
    cond = None
    if cfg.time_start is not None:
        cond = t >= F.lit(cfg.time_start)
    if cfg.time_end is not None:
        upper = t <= F.lit(cfg.time_end)
        cond = upper if cond is None else cond & upper
    if cond is not None:
        # 期別是 NULL 的列在 groupBy 自成一組，拿來數「沒讀的列」有幾筆，
        # 不必多掃一趟；它們不進任何統計。
        df = df.where(cond | t.isNull())
    return df


def compute_basic_stats(
    df: "SparkDataFrame", time_col: str, columns: list[dict], batch_size: int
) -> tuple[list[str], list[int], int, dict[str, dict]]:
    """欄位分批，每批一趟 ``groupBy(time_col)``，只讀自己那幾欄。只用 Spark 內建函式。

    回傳（期別、每期筆數、期別是 NULL 的筆數、每欄的統計量）。``min = max``
    就表示扣掉空值後只有一個值，所以不用 ``count(distinct)``。
    """
    from pyspark.sql import functions as F

    feats = [c for c in columns if c["kind"] != "unchecked"]
    batches = [feats[i:i + batch_size] for i in range(0, len(feats), batch_size)] or [[]]
    periods: list = []
    rows: list[int] = []
    null_time_rows = 0
    stats: dict[str, dict] = {}
    for b, batch in enumerate(batches):
        exprs = [F.count(F.lit(1)).alias("_n")]
        for i, c in enumerate(batch):
            x = _cleaned(c["name"], c["dtype"])
            exprs += [
                F.count(x).alias(f"_{i}_nn"),
                F.min(x).alias(f"_{i}_min"),
                F.max(x).alias(f"_{i}_max"),
            ]
            if _may_have_fraction(c["dtype"]):
                # 設計文件寫 max(abs(x − floor(x))) > 0；double 的 floor 回傳 bigint，
                # 超過 bigint 範圍的整數值會被誤判成有小數，所以改比 round(x, 0)。
                exprs.append(F.max((x != F.round(x, 0)).cast("int")).alias(f"_{i}_frac"))
        selected = df.select(
            F.col(_quote(time_col)).alias(_TIME_ALIAS),
            *[F.col(_quote(c["name"])) for c in batch],
        )
        result = selected.groupBy(_TIME_ALIAS).agg(*exprs).collect()
        by_time = {r[_TIME_ALIAS]: r for r in result if r[_TIME_ALIAS] is not None}
        if b == 0:
            periods = sorted(by_time)
            rows = [by_time[p]["_n"] for p in periods]
            null_time_rows = sum(r["_n"] for r in result if r[_TIME_ALIAS] is None)
        for i, c in enumerate(batch):
            per = [by_time[p] for p in periods]
            stat = {
                "null": [n - r[f"_{i}_nn"] for n, r in zip(rows, per)],
                "min": [_json_value(r[f"_{i}_min"]) for r in per],
                "max": [_json_value(r[f"_{i}_max"]) for r in per],
            }
            if c["kind"] == "string":
                stat["has_decimal"] = None
            elif _may_have_fraction(c["dtype"]):
                stat["has_decimal"] = any(r[f"_{i}_frac"] == 1 for r in per)
            else:
                stat["has_decimal"] = False
            stats[c["name"]] = stat
    return [_period_label(p) for p in periods], rows, null_time_rows, stats


def profile_table(spark: "SparkSession", cfg: ProfileConfig, batch_size: int) -> dict:
    """算出 ``profile.json`` 的內容。只有統計量，沒有判斷結果。"""
    started = time.monotonic()
    df = read_table(spark, cfg)
    skip = {c.lower() for c in [cfg.time_col, *cfg.key_cols, *cfg.exclude_cols]}
    columns = [
        {"name": f.name, "dtype": f.dataType.simpleString(),
         "kind": column_kind(f.dataType.simpleString())}
        for f in df.schema.fields
        if f.name.lower() not in skip
    ]
    periods, rows, null_time_rows, stats = compute_basic_stats(
        df, cfg.time_col, columns, batch_size
    )
    for c in columns:
        c.update(stats.get(c["name"], {}))
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "config_text": cfg.text,
        "config": {
            "table": cfg.table,
            "time_col": cfg.time_col,
            "key_cols": cfg.key_cols,
            "exclude_cols": cfg.exclude_cols,
            "time_range": {"start": cfg.time_start, "end": cfg.time_end},
            "where": cfg.where,
            "thresholds": cfg.thresholds,
        },
        "periods": periods,
        "rows": rows,
        "null_time_rows": null_time_rows,
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "batch_size": batch_size,
        "columns": columns,
    }


# ---------------------------------------------------------------------------
# 判斷（不開 Spark；render 換門檻就是重跑這一段）
# ---------------------------------------------------------------------------

FLAG_ORDER = ("null_all", "const_all", "null_some", "const_some", "const_each", "precision")
EXCLUDABLE = ("null_all", "const_all")


def null_ratios(col: dict, rows: list[int]) -> list[float]:
    return [nl / n for nl, n in zip(col["null"], rows)]


def max_abs(col: dict) -> tuple[float, int] | None:
    """各期最小值、最大值裡絕對值最大的那個，和它出現的期別索引。"""
    best: tuple[float, int] | None = None
    for t, (lo, hi) in enumerate(zip(col["min"], col["max"])):
        for v in (lo, hi):
            if v is not None and (best is None or abs(v) > best[0]):
                best = (abs(v), t)
    return best


def one_value_periods(col: dict, rows: list[int]) -> tuple[list[int], list[int]]:
    """（有值的期別、其中只有一個值的期別）。全空的期別兩邊都不算，直接略過。"""
    valued = [t for t, (nl, n) in enumerate(zip(col["null"], rows)) if nl < n]
    const = [t for t in valued if col["min"][t] == col["max"][t]]
    return valued, const


def judge_column(col: dict, rows: list[int], thresholds: dict[str, float]) -> list[str]:
    """一欄的問題類型，依 ``FLAG_ORDER`` 排。先後順序見設計文件第五節。"""
    high = [r >= thresholds["null_ratio"] for r in null_ratios(col, rows)]
    if all(high):
        return ["null_all"]  # 已可直接排除，不再判斷其他問題
    flags = ["null_some"] if any(high) else []

    valued, const = one_value_periods(col, rows)
    if len(const) == len(valued):
        if len({col["min"][t] for t in const}) == 1:
            return ["const_all"]  # 已可直接排除，不再判斷其他問題
        return flags + ["const_each"]  # 不判斷 float32 精度
    if const:
        flags.append("const_some")

    if float32_lossy(col, thresholds):
        flags.append("precision")
    return flags


def float32_lossy(col: dict, thresholds: dict[str, float]) -> bool:
    """存成 float32 會不會改變數值。

    宣告成 float 的欄位本來就是 float32，不會再變。沒有小數的欄位當整數看：2^24
    本身存得一個不差，超過才會被改掉（設計文件寫 ≥，差在 2^24 這一個值）。
    """
    if col["kind"] != "numeric" or col["dtype"] == "float":
        return False
    m = max_abs(col)
    if m is None:
        return False
    if col["has_decimal"]:
        return m[0] >= thresholds["precision_max_abs"]
    return m[0] > INT_EXACT


def judge(profile: dict, thresholds: dict[str, float]) -> dict[str, list[str]]:
    return {
        c["name"]: judge_column(c, profile["rows"], thresholds)
        for c in profile["columns"]
        if c["kind"] != "unchecked"
    }


# ---------------------------------------------------------------------------
# 報告
# ---------------------------------------------------------------------------


def _pct_threshold(v: float) -> str:
    return f"{v * 100:g}%"


def _fmt_num(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, int) or float(v).is_integer():
        return f"{int(v):,}"
    return f"{v:,.6f}".rstrip("0").rstrip(".")


def _pct(x: float | None) -> str:
    """空值比例。不會把「不到 100%」顯示成 100%，也不會把「大於 0」顯示成 0%。"""
    if x is None:
        return ""
    if x == 0:
        return "0%"
    if x == 1:
        return "100%"
    digits = 2 if x < 0.01 or x > 0.99 else 1
    s = f"{x * 100:.{digits}f}%"
    if s.startswith("100"):
        return "> 99.99%"
    if float(s[:-1]) == 0:
        return "< 0.01%"
    return s


def _duration(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    return f"{m} 分 {s} 秒" if m else f"{s} 秒"


def _flag_texts(th: dict[str, float]) -> dict[str, tuple[str, str, str]]:
    """問題類型 →（名稱、判斷標準、建議處理方式）。字句待正式環境 demo 時定（設計文件第八節）。"""
    null = _pct_threshold(th["null_ratio"])
    prec = _fmt_num(th["precision_max_abs"])
    exclude = "欄位沒有資訊，可以排除"
    upstream = "欄位本身正常，請檢查這幾期的上游資料"
    return {
        "null_all": ("每一期都幾乎是空值", f"每一期的空值比例都 ≥ {null}", exclude),
        "const_all": ("整欄只有一個值", "所有期別合計只有一個值，不含空值", exclude),
        "null_some": ("部分期別幾乎是空值", f"只有部分期別的空值比例 ≥ {null}", upstream),
        "const_some": ("部分期別只有一個值", "只有部分期別只有一個值，不含空值", upstream),
        "const_each": (
            "每期各只有一個值", "每一期只有一個值，但各期的值不同",
            "通常是匯率這類每期一個數字的總體指標，不一定要排除",
        ),
        "precision": (
            "float32 精度不足",
            f"有小數且絕對值最大 ≥ {prec}；或整數且絕對值最大超過 {INT_EXACT:,}",
            "對 LightGBM 幾乎沒有影響。但 numeric_precision_policy 預設是 block，"
            "整數與 decimal 欄位會被 dataset 的精度檢查擋下；可請上游四捨五入，或改成 truncate 接受失真",
        ),
    }


def _table(rows: list[list[Any]], columns: list[str]) -> pd.DataFrame:
    # object：同一欄混著整數與小數時，pandas 會把整數也轉成 float，大整數就失真了
    return pd.DataFrame(rows, columns=columns, dtype=object)


def _section(title: str, description: str, table: pd.DataFrame | None = None,
             **kwargs: Any) -> ReportSection:
    if table is not None and table.empty:
        return ReportSection(title=title, description="這次沒有。")
    return ReportSection(title=title, description=description,
                         tables=[] if table is None else [table], **kwargs)


def _kv_table(rows: list[tuple[str, str]]) -> str:
    """首頁的兩欄表。值是已經 escape 過的 HTML。"""
    body = "".join(f"<tr><th>{html.escape(k)}</th><td>{v}</td></tr>" for k, v in rows)
    return f"<table>{body}</table>"


class _Report:
    """把 profile 與判斷結果整理成各頁要用的量，每頁一個方法。"""

    def __init__(self, profile: dict, thresholds: dict[str, float]):
        self.profile = profile
        self.thresholds = thresholds
        self.periods: list[str] = profile["periods"]
        self.rows: list[int] = profile["rows"]
        self.feats = [c for c in profile["columns"] if c["kind"] != "unchecked"]
        self.unchecked = [c for c in profile["columns"] if c["kind"] == "unchecked"]
        self.flags = judge(profile, thresholds)
        self.texts = _flag_texts(thresholds)

    def with_flag(self, key: str) -> list[dict]:
        return [c for c in self.feats if key in self.flags[c["name"]]]

    def excludable(self) -> list[dict]:
        return [c for c in self.feats if set(EXCLUDABLE) & set(self.flags[c["name"]])]

    def flag_names(self, col: dict) -> str:
        return "、".join(self.texts[k][0] for k in self.flags[col["name"]]) or "—"

    def ratios(self, col: dict) -> list[float]:
        return null_ratios(col, self.rows)

    # --- 首頁 ---------------------------------------------------------------

    def index_intro(self, threshold_source: str | None) -> str:
        cfg = self.profile["config"]
        e = html.escape
        code = lambda s: f"<code>{e(str(s))}</code>"  # noqa: E731
        n_num = sum(c["kind"] == "numeric" for c in self.feats)
        flagged = [c for c in self.feats if self.flags[c["name"]]]
        excludable = self.excludable()
        if self.periods:
            span = f"{e(self.periods[0])} 至 {e(self.periods[-1])}，共 {len(self.periods)} 期"
            per = f"{min(self.rows):,} 至 {max(self.rows):,} 筆，全部納入計算"
        else:
            span = per = "無"
        overview = [
            ("特徵欄位", f"{len(self.feats)} 欄（數值 {n_num} 欄、字串 {len(self.feats) - n_num} 欄）"),
            ("需要處理", f"{len(flagged)} 欄"),
            ("其中可直接排除", f"{len(excludable)} 欄"),
        ]
        if self.unchecked:
            overview.append(("未檢查", f"{len(self.unchecked)} 欄，型別不支援，見〈05 所有欄位明細〉"))
        scope = [
            ("資料表", code(cfg["table"])),
            ("期別欄位", code(cfg["time_col"])),
            ("主鍵欄位", "、".join(code(c) for c in cfg["key_cols"])),
            ("排除欄位", "、".join(code(c) for c in cfg["exclude_cols"]) or "無"),
            ("期間", span),
            ("篩選條件", code(cfg["where"]) if cfg["where"] else "無"),
            ("每期筆數", per),
            ("期別欄位是空值的列", f"{self.profile['null_time_rows']:,} 筆，不讀"),
            ("執行項目", "基本檢查"),
            ("執行時間", f"{_duration(self.profile['elapsed_seconds'])}，"
                         f"每批 {self.profile['batch_size']} 欄"),
            ("產出時間", e(self.profile["generated_at"].replace("T", " "))),
        ]
        th_rows = [
            [self.texts["null_all"][0] + "、" + self.texts["null_some"][0],
             f"空值比例 ≥ {_pct_threshold(self.thresholds['null_ratio'])}", "null_ratio",
             str(self.thresholds["null_ratio"])],
            [self.texts["precision"][0],
             f"有小數且絕對值最大 ≥ {_fmt_num(self.thresholds['precision_max_abs'])}；"
             f"整數且絕對值最大超過 {INT_EXACT:,}，這個值是 float32 本身的性質，不能調",
             "precision_max_abs", str(self.thresholds["precision_max_abs"])],
        ]
        th_note = "以下是這次使用的標準，可以在設定檔的 <code>thresholds</code> 區塊調整。"
        if threshold_source:
            th_note += f"這次的值取自重畫時指定的設定檔 {code(threshold_source)}。"
        th_html = _table(th_rows, ["問題類型", "判斷標準", "設定鍵", "這次的值"]).to_html(index=False)
        return (
            f"<p>這份報告列出 {code(cfg['table'])} 中需要處理的欄位，以及建議的處理方式。"
            "請先看〈01 需要處理的欄位〉；想知道判斷依據，再看〈02 空值與精度〉。</p>"
            f"<h2>總覽</h2>{_kv_table(overview)}"
            f"<h2>資料範圍</h2>{_kv_table(scope)}"
            f"<h2>判斷標準</h2><p>{th_note}</p>{th_html}"
            '<details class="section"><summary>設定檔</summary>'
            f'<div class="formula">{e(self.profile["config_text"])}</div></details>'
        )

    # --- 01 需要處理的欄位 ---------------------------------------------------

    def page_flags(self) -> Page:
        t = self.texts
        summary = _table(
            [[t[k][0], t[k][1], len(self.with_flag(k)), t[k][2]] for k in FLAG_ORDER],
            ["問題類型", "判斷標準", "欄位數", "建議處理方式"],
        )
        sections = [ReportSection(
            title="總表", description="各類問題的欄位數與建議處理方式。各類的欄位清單在下方。",
            tables=[summary],
        )]

        excl = self.excludable()
        names = ", ".join(c["name"] for c in excl)
        sections.append(_section(
            f"可直接排除的欄位（{len(excl)} 欄）",
            "這些欄位每一期都幾乎是空值，或從頭到尾只有一個值，模型從中學不到東西。",
            _table([[c["name"], c["dtype"], self.flag_names(c), self._exclude_reason(c)]
                    for c in excl], ["欄位", "型別", "問題類型", "依據"]),
            formula=(
                "# 只重新訓練、不重建 dataset：加到 conf/base/parameters_training.yaml\n"
                f"training:\n  feature_selection:\n    exclude: [{names}]\n\n"
                "# 要連 dataset 一起拿掉：改加到 dataset.prepare_model_input.drop_columns，"
                "需要重建 dataset"
            ),
        ))

        null_th = self.thresholds["null_ratio"]
        rows = []
        for c in self.with_flag("null_some"):
            r = self.ratios(c)
            others = [v for v in r if v < null_th]
            rows.append([c["name"], c["dtype"],
                         "、".join(p for p, v in zip(self.periods, r) if v >= null_th),
                         f"{_pct(min(others))} 至 {_pct(max(others))}"])
        sections.append(_section(
            f"{t['null_some'][0]}（{len(rows)} 欄）",
            "平常有值，只有少數幾期幾乎全空，通常是那幾期的上游資料沒有進來。",
            _table(rows, ["欄位", "型別", f"空值比例 ≥ {_pct_threshold(null_th)} 的期別",
                          "其他期別的空值比例"]),
            bullets=[f"建議處理方式：{t['null_some'][2]}"],
        ))

        rows = []
        for c in self.with_flag("const_some"):
            _, const = one_value_periods(c, self.rows)
            rows.append([c["name"], c["dtype"],
                         "、".join(self.periods[i] for i in const),
                         "、".join(_fmt_num(c["min"][i]) for i in const)])
        sections.append(_section(
            f"{t['const_some'][0]}（{len(rows)} 欄）",
            "平常有變化，只有少數幾期全部是同一個值，通常是那幾期的上游資料填了預設值。",
            _table(rows, ["欄位", "型別", "只有一個值的期別", "那一期的值"]),
            bullets=[f"建議處理方式：{t['const_some'][2]}"],
        ))

        rows = []
        for c in self.with_flag("const_each"):
            _, const = one_value_periods(c, self.rows)
            values = [c["min"][i] for i in const]
            rows.append([c["name"], c["dtype"], values[0], values[-1], len(set(values))])
        sections.append(_section(
            f"{t['const_each'][0]}（{len(rows)} 欄）",
            "同一期裡每一筆的值都相同，但各期的值不同。",
            _table(rows, ["欄位", "型別", "最早一期的值", "最新一期的值", "不同的值個數"]),
            bullets=[f"建議處理方式：{t['const_each'][2]}"],
        ))

        flagged = self.with_flag("precision")
        sections.append(_section(
            f"{t['precision'][0]}（{len(flagged)} 欄）",
            f"float32 只保留大約 7 位有效數字。有小數的欄位數值一大，小數就留不住；整數超過 "
            f"{INT_EXACT:,}，個位數就可能被改掉。表中列出最大值存成 float32 後會變成多少。",
            self._precision_table(flagged),
            bullets=[f"建議處理方式：{t['precision'][2]}"],
        ))
        return Page(slug="01-flags", title="01 需要處理的欄位", scope=None,
                    sections=tuple(sections))

    def _exclude_reason(self, col: dict) -> str:
        if "null_all" in self.flags[col["name"]]:
            r = self.ratios(col)
            if min(r) == 1:
                return "每一期都 100% 空值"
            return f"空值比例 {_pct(min(r))} 至 {_pct(max(r))}"
        valued, const = one_value_periods(col, self.rows)
        where = "每一期" if len(valued) == len(self.periods) else "有值的期別"
        return f"{where}都是 {_fmt_num(col['min'][const[0]])}"

    def _precision_table(self, cols: list[dict]) -> pd.DataFrame:
        rows = []
        for c in cols:
            m, at = max_abs(c)
            rows.append((m, [c["name"], c["dtype"], m, float(np.float32(m)), self.periods[at]]))
        rows.sort(key=lambda x: -x[0])
        return _table([r for _, r in rows], ["欄位", "型別", "絕對值最大", "存成 float32 後", "出現期別"])

    # --- 02 空值與精度 -------------------------------------------------------

    def page_basic(self) -> Page:
        null_th = self.thresholds["null_ratio"]
        shown = sorted(
            (c for c in self.feats if max(self.ratios(c)) >= NULL_HEATMAP_MIN),
            key=lambda c: -max(self.ratios(c)),
        )
        floor = _pct_threshold(NULL_HEATMAP_MIN)
        n_shown = len(shown)
        # 熱圖每格一個點，report.figures 一張圖最多 2000 個點
        cap = max(1, 2000 // max(1, len(self.periods)))
        shown = shown[:cap]
        empty_note = "空值包含 NULL、float／double 欄位的 NaN 與 ±Infinity，以及字串欄位的空字串。"
        if shown:
            label = f"≥{_pct_threshold(null_th)}"
            fig = heatmap(
                [self.ratios(c) for c in shown], self.periods, [c["name"] for c in shown],
                title="", colorbar_title="空值比例",
                text=[[label if v >= null_th else "" for v in self.ratios(c)] for c in shown],
            )
            fig.update_traces(zmin=0, zmax=1,
                              hovertemplate="%{y}<br>%{x}<br>空值比例 %{z:.2%}<extra></extra>")
            fig.update_layout(height=max(320, 120 + 28 * len(shown)))
            fig.update_xaxes(type="category")
            fig.update_yaxes(type="category", autorange="reversed")
            desc = (
                f"列出至少有一期空值比例 ≥ {floor} 的 {n_shown} 個欄位，最空的排最上面；"
                f"格子裡標 {label} 的，表示那一期幾乎都是空值。"
                f"其餘 {len(self.feats) - n_shown} 個欄位每一期都低於 {floor}。"
            )
            if len(shown) < n_shown:
                desc += f"欄位太多，圖中只畫最空的 {len(shown)} 個，完整數字在下方。"
            null_section = ReportSection(title="各期空值比例", description=desc + empty_note,
                                         figures=[fig])
        else:
            null_section = ReportSection(
                title="各期空值比例",
                description=f"每個欄位每一期的空值比例都低於 {floor}。" + empty_note,
            )

        skip = {*EXCLUDABLE, "const_each"}
        candidates = []
        for c in self.feats:
            if c["kind"] != "numeric" or c["dtype"] == "float" or skip & set(self.flags[c["name"]]):
                continue
            m = max_abs(c)
            if m is not None and (c["has_decimal"] or m[0] > INT_EXACT):
                candidates.append(c)
        precision = _section(
            "float32 精度",
            f"有小數的欄位全部列出，整數欄位只列絕對值最大超過 {INT_EXACT:,} 的，依絕對值最大值排序，"
            "並列出最大值存成 float32 後會變成多少。達到標準的欄位會列在〈01 需要處理的欄位〉的"
            "「float32 精度不足」。可直接排除的欄位、每期各只有一個值的欄位、"
            "本來就是 float 的欄位不列。",
            self._precision_table(candidates),
            formula=(
                "float32 只保留大約 7 位有效數字，數值越大，留得下的位數越少：\n"
                "100,000.37 存成 100,000.3672，第二位小數開始不準\n"
                "23,016,210.71 存成 23,016,210，小數全部不見\n"
                f"25,000,001 存成 25,000,000，整數超過 {INT_EXACT:,} 後個位數會被改掉"
            ),
        )

        full = _table([[c["name"], *[_pct(v) for v in self.ratios(c)]] for c in self.feats],
                      ["欄位", *self.periods])
        return Page(slug="02-basic", title="02 空值與精度", scope=None, sections=(
            null_section,
            precision,
            ReportSection(title="所有欄位的各期空值比例", description="上方熱圖的完整數字。",
                          tables=[full], collapsible=True),
        ))

    # --- 05 所有欄位明細 -----------------------------------------------------

    def page_all(self) -> Page:
        summary = []
        for c in self.feats:
            r = self.ratios(c)
            is_numeric = c["kind"] == "numeric"
            summary.append([
                c["name"], c["dtype"], self.flag_names(c), f"{_pct(min(r))} 至 {_pct(max(r))}",
                c["min"][-1] if is_numeric else None, c["max"][-1] if is_numeric else None,
            ])
        numeric = [c for c in self.feats if c["kind"] == "numeric"]
        return Page(slug="05-all", title="05 所有欄位明細", scope=None, sections=(
            ReportSection(
                title="各欄位摘要",
                description="每個欄位一列。最小值與最大值取最新一期，只列數值欄位。",
                tables=[_table(summary, ["欄位", "型別", "問題類型", "各期空值比例的範圍",
                                         "最小值", "最大值"])],
            ),
            ReportSection(
                title="各期最大值", description="數值欄位每一期的最大值。", collapsible=True,
                tables=[_table([[c["name"], *c["max"]] for c in numeric], ["欄位", *self.periods])],
            ),
            _section(
                "未檢查的欄位",
                "型別不支援（date、timestamp、array、map、struct 等）而沒有檢查的欄位。",
                _table([[c["name"], c["dtype"]] for c in self.unchecked], ["欄位", "型別"]),
            ),
        ))


def write_report(profile: dict, out_dir: Path, thresholds: dict[str, float],
                 threshold_source: str | None) -> list[Path]:
    """依 ``thresholds`` 判斷後畫成 HTML 資料夾：``index.html``、01、02、05 與 ``plotly.min.js``。"""
    report = _Report(profile, thresholds)
    return write_pages(
        [report.page_flags(), report.page_basic(), report.page_all()],
        out_dir,
        index_title=f"特徵表檢查報告：{profile['config']['table']}",
        index_intro=report.index_intro(threshold_source),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _exit_with_problems(problems: list[str]) -> None:
    typer.echo(f"設定檔有 {len(problems)} 個問題：", err=True)
    for p in problems:
        typer.echo(f"  - {p}", err=True)
    raise typer.Exit(code=1)


def _default_output_dir(table: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return OUTPUT_ROOT / f"{table.split('.')[-1]}_{stamp}"


@app.command()
def run(
    config: Path = typer.Option(..., "--config", help="工具的設定檔（範本：scripts/profile_feature_table.template.yaml）"),
    batch_size: int = typer.Option(
        DEFAULT_BATCH_SIZE, "--batch-size", min=1,
        help="每一趟 Spark 聚合帶幾欄。正式環境量過耗時再定預設值",
    ),
) -> None:
    """算統計量、存 profile.json、畫報告。"""
    from pyspark.sql.utils import AnalysisException

    from recsys_tfb.utils.spark import get_or_create_spark_session, stop_spark_session

    cfg, problems = parse_config(config.read_text(encoding="utf-8"))
    if cfg.table is None:
        _exit_with_problems(problems)  # 沒有表名，表與欄位就檢查不了

    spark = get_or_create_spark_session({"app_name": "feature-profile", **cfg.spark})
    try:
        try:
            columns = spark.table(cfg.table).columns
        except AnalysisException:
            columns = None
        problems += check_against_table(cfg, columns)
        if problems:
            _exit_with_problems(problems)
        profile = profile_table(spark, cfg, batch_size)
    finally:
        stop_spark_session()

    if not profile["periods"]:
        typer.echo("一期都沒讀到，請檢查 time_range 與 where。", err=True)
        raise typer.Exit(code=1)
    out_dir = Path(cfg.output_dir) if cfg.output_dir else _default_output_dir(cfg.table)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / PROFILE_FILENAME).write_text(
        json.dumps(profile, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    write_report(profile, out_dir, cfg.thresholds, None)
    typer.echo(f"報告：{out_dir / 'index.html'}")


@app.command()
def render(
    out_dir: Path = typer.Argument(..., help="run 產出的資料夾（裡面要有 profile.json）"),
    config: Path | None = typer.Option(
        None, "--config", help="用這份設定檔的 thresholds 重新判斷；不給就用 profile.json 裡存的",
    ),
) -> None:
    """讀既有的 profile.json 重新判斷、重畫報告，不開 Spark。"""
    profile = json.loads((out_dir / PROFILE_FILENAME).read_text(encoding="utf-8"))
    if profile.get("schema_version") != SCHEMA_VERSION:
        typer.echo(
            f"profile.json 的 schema_version 是 {profile.get('schema_version')!r}，"
            f"這一版只讀 {SCHEMA_VERSION}；請重跑 run。", err=True,
        )
        raise typer.Exit(code=1)
    thresholds = profile["config"]["thresholds"]
    source = None
    if config is not None:
        cfg, problems = parse_config(config.read_text(encoding="utf-8"))
        if problems:
            _exit_with_problems(problems)
        thresholds, source = cfg.thresholds, str(config)
    write_report(profile, out_dir, thresholds, source)
    typer.echo(f"報告：{out_dir / 'index.html'}")


if __name__ == "__main__":
    app()
