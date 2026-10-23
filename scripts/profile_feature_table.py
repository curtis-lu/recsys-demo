"""特徵表檢查工具：算一張特徵表每一期、每一欄的統計量，標出要處理的欄位。

設計文件：``docs/notes/2026-10-15-feature-table-profiling-design.md``；使用說明：
``docs/operations/user-guides/feature-table-profiling.md``。基本檢查（空值與「只有一個值」
五種問題）一定跑；``checks.distribution`` 打開時加分布偏移（每一期和最早一期比的 PSI）與
極端值；``checks.correlation`` 打開時加相關性（最新一期抽約 50,000 筆，數值欄位兩兩的
Spearman 相關係數）。

Spark 的趟數（每一趟都依 ``--batch-size`` 把欄位分批）：基本檢查一趟（有跑分布時順便算
分位數）；分布再加兩趟——只讀基準期、找字串欄位最常見的值，與所有期別的 PSI 分組筆數；
相關性只讀最新一期抽樣、persist 後一批欄位 collect 一次，Spearman 在 driver 用 numpy 算。

Usage:
    python scripts/profile_feature_table.py run --config my_profile.yaml
    python scripts/profile_feature_table.py render <輸出資料夾> [--config my_profile.yaml]

``run`` 開 Spark 算統計量，存成 ``profile.json`` 再畫報告；``render`` 只讀
``profile.json`` 重新判斷、重畫，不開 Spark。所以 ``profile.json`` 只存統計量、
不存判斷結果：判斷全在 ``judge``，吃統計量與門檻。

跟框架的關係：不讀 ``conf/``，判斷規則也不沿用框架的函式，報告也不提框架的設定，
只借 ``get_or_create_spark_session`` 開 Spark。開 Spark 的方式同
``scripts/sampling_overrides_editor.py``：只給 ``app_name``，其餘 Spark 設定全由
``SPARK_CONF_DIR`` 決定，工具的設定檔不另設 Spark 區塊。報告是工具自己產生的單一頁面
（不用 ``recsys_tfb.report``）：要排序、篩選、複製欄名、頁內目錄，框架的報表外殼
只畫靜態表格。
"""

from __future__ import annotations

import datetime as dt
import html
import json
import math
import re
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import typer
import yaml

if TYPE_CHECKING:
    import numpy as np
    from pyspark.sql import Column, DataFrame as SparkDataFrame, SparkSession

SCHEMA_VERSION = 1
PROFILE_FILENAME = "profile.json"
OUTPUT_ROOT = Path("data/profiling/feature_profile")

DEFAULT_THRESHOLDS: dict[str, float] = {
    "null_ratio": 0.99, "psi": 0.25, "tail_ratio": 10, "tail_log_ratio": 1, "corr": 0.95,
}
# 每批幾欄。一次 groupBy 帶上千個聚合式會讓執行計畫過大；確切大小要在正式環境的
# 真表上量過再定（設計文件第八節），量之前先用 --batch-size 試。
DEFAULT_BATCH_SIZE = 200
# 分布檢查在基本檢查那一趟一起算的分位數。p1、p25、p50、p75、p99 給走勢圖與極端值用，
# 十分位數只用最早一期的，當 PSI 的切點。p1 一定排第一、p99 一定排最後（tail_ratios 靠這個）。
QUANTILE_PROBS = (0.01, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.99)
_BAND_PROBS = (0.01, 0.25, 0.5, 0.75, 0.99)
_DECILES = tuple(QUANTILE_PROBS.index(p) for p in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9))
# percentile_approx 的精度（Spark 的預設值）。越大越準、越耗記憶體；正式環境量過再定
# （設計文件第八節），量之前先用 --accuracy 試。
DEFAULT_QUANTILE_ACCURACY = 10000
PSI_TOP_VALUES = 20  # 字串欄位取基準期最常見的幾個值各成一組
# 相關性：最新一期抽約這麼多筆拉回 driver。筆數會不會撐爆 driver 記憶體，正式環境量過再定
# 要不要變成設定鍵（設計文件第八節）。
CORR_SAMPLE_ROWS = 50_000
CORR_SAMPLE_SEED = 42
CORR_MIN_ROWS = 1_000  # 兩欄都有值的筆數少於這個數，不給相關係數
# profile.json 只存絕對值 ≥ 這個值的配對，所以 render 的 corr 最低只能調到這裡
CORR_KEEP = 0.5
# 先用整欄名次的近似值篩，絕對值 ≥ CORR_KEEP 減這麼多的配對才逐對算精確值。近似和精確實測差到 0.03
# （空值的機率跟值的大小有關時），留三倍多的餘裕
CORR_SCREEN_MARGIN = 0.1

_REQUIRED_KEYS = ("table", "time_col", "key_cols", "checks")
# 每一項都必填：決定要多跑幾趟 Spark，耗時差很多，讓工具使用者每次都明確選
_CHECK_KEYS = ("distribution", "correlation")
_OPTIONAL_KEYS = ("exclude_cols", "time_range", "where", "thresholds", "output_dir")
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
    distribution: bool = False
    correlation: bool = False
    thresholds: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_THRESHOLDS))
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

    checks = raw.get("checks")
    if checks is not None:
        if not isinstance(checks, dict):
            problems.append(f"checks 要是含 {'、'.join(_CHECK_KEYS)} 的對照表")
        else:
            extra = sorted(str(k) for k in set(checks) - set(_CHECK_KEYS))
            if extra:
                problems.append(f"checks 不認得的鍵：{', '.join(extra)}（這一版只有 {'、'.join(_CHECK_KEYS)}）")
            for k in _CHECK_KEYS:
                v = checks.get(k)
                if v is None:
                    problems.append(f"必填鍵 checks.{k} 沒寫")
                elif not isinstance(v, bool):
                    problems.append(f"checks.{k} 要是 true 或 false；現在是 {v!r}")
                else:
                    setattr(cfg, k, v)

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
                elif k == "corr" and not CORR_KEEP <= v <= 1:
                    problems.append(f"thresholds.corr 要介於 {CORR_KEEP:g} 到 1 之間（profile.json 只存絕對值 "
                                    f"≥ {CORR_KEEP:g} 的配對）；現在是 {v!r}")
                elif v <= 0:
                    problems.append(f"thresholds.{k} 要大於 0；現在是 {v!r}")
                else:
                    cfg.thresholds[k] = v

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
    # decimal 存成 float：只用來顯示與判斷「只有一個值」；兩個 decimal 要到第 16 位
    # 有效數字以後才不同，才會被當成同一個值。
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
    df: "SparkDataFrame", time_col: str, columns: list[dict], batch_size: int,
    *, accuracy: int | None = None,
) -> tuple[list, list[int], int, dict[str, dict]]:
    """欄位分批，每批一趟 ``groupBy(time_col)``，只讀自己那幾欄。只用 Spark 內建函式。

    回傳（期別的原始值、每期筆數、期別是 NULL 的筆數、每欄的統計量）。``min = max``
    就表示扣掉空值後只有一個值，所以不用 ``count(distinct)``。給了 ``accuracy`` 時，
    數值欄位在同一趟多算 ``QUANTILE_PROBS`` 的分位數（整期都是空值的那一期是 None）。
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
            if accuracy is not None and c["kind"] == "numeric":
                exprs.append(F.percentile_approx(x, list(QUANTILE_PROBS), accuracy).alias(f"_{i}_q"))
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
            stats[c["name"]] = {
                "null": [n - r[f"_{i}_nn"] for n, r in zip(rows, per)],
                "min": [_json_value(r[f"_{i}_min"]) for r in per],
                "max": [_json_value(r[f"_{i}_max"]) for r in per],
            }
            if accuracy is not None and c["kind"] == "numeric":
                stats[c["name"]]["quantiles"] = [
                    None if r[f"_{i}_q"] is None else [_json_value(v) for v in r[f"_{i}_q"]] for r in per
                ]
    return periods, rows, null_time_rows, stats


def compute_top_values(
    df: "SparkDataFrame", time_col: str, base_period: Any, columns: list[dict], batch_size: int
) -> dict[str, list[str]]:
    """基準期每個字串欄位最常見的 ``PSI_TOP_VALUES`` 個值，次數多的在前、同次數依值排。

    只讀基準期那一期。一批一趟：每一列攤成（第幾欄、值）再數，不必一欄一趟。
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    strings = [c for c in columns if c["kind"] == "string"]
    out: dict[str, list[str]] = {c["name"]: [] for c in strings}
    base = df.where(F.col(_quote(time_col)) == F.lit(base_period))
    for b in range(0, len(strings), batch_size):
        batch = strings[b:b + batch_size]
        pairs = F.explode(F.array(*[
            F.struct(F.lit(i).alias("k"), _cleaned(c["name"], c["dtype"]).alias("v"))
            for i, c in enumerate(batch)
        ])).alias("p")
        counted = (base.select(pairs).select("p.k", "p.v").where(F.col("v").isNotNull())
                   .groupBy("k", "v").count())
        rank = F.row_number().over(Window.partitionBy("k").orderBy(F.desc("count"), F.asc("v")))
        top = counted.withColumn("_r", rank).where(F.col("_r") <= PSI_TOP_VALUES).collect()
        for r in sorted(top, key=lambda r: (r["k"], r["_r"])):
            out[batch[r["k"]]["name"]].append(r["v"])
    return out


def compute_bin_counts(
    df: "SparkDataFrame", time_col: str, periods: list, specs: list[tuple[dict, dict]], batch_size: int
) -> dict[str, list[list[int]]]:
    """PSI 的分組筆數：每一欄、每一期、每一組幾筆。

    ``specs`` 是（欄、分組方式）；分組方式是 ``{"edges": [...]}``（數值欄，每組含上界：
    (−∞, e1]、(e1, e2]、…、(ek, ∞)）或 ``{"values": [...]}``（字串欄，每個值一組、其他值
    一組）。最後一組都是空值。一批一趟：每一列先算出各欄的組別，攤成（期別、第幾欄、組別）
    再數，執行計畫只跟欄數成正比，不跟「欄數 × 組數」成正比。
    """
    from pyspark.sql import functions as F

    out: dict[str, list[list[int]]] = {}
    index = {p: t for t, p in enumerate(periods)}
    for b in range(0, len(specs), batch_size):
        batch = specs[b:b + batch_size]
        items = []
        for i, (c, spec) in enumerate(batch):
            x = _cleaned(c["name"], c["dtype"])
            if "edges" in spec:
                n = len(spec["edges"]) + 1
                group = F.when(x.isNull(), n)
                for k, e in enumerate(spec["edges"]):
                    group = group.when(x <= F.lit(e), k)
                group = group.otherwise(n - 1)
            else:
                n = len(spec["values"]) + 1
                lookup = F.create_map(*[F.lit(v) for k, val in enumerate(spec["values"]) for v in (val, k)])
                group = F.when(x.isNull(), n).otherwise(F.coalesce(lookup[x], F.lit(n - 1)))
            items.append(F.struct(F.lit(i).alias("k"), group.alias("g")))
            out[c["name"]] = [[0] * (n + 1) for _ in periods]
        exploded = df.select(F.col(_quote(time_col)).alias(_TIME_ALIAS), F.explode(F.array(*items)).alias("p"))
        counted = exploded.groupBy(_TIME_ALIAS, "p.k", "p.g").count().collect()
        for r in counted:
            t = index.get(r[_TIME_ALIAS])
            if t is not None:  # 期別是 NULL 的列不讀
                out[batch[r["k"]][0]["name"]][t][r["g"]] = r["count"]
    return out


def add_bins(df: "SparkDataFrame", time_col: str, periods: list, columns: list[dict],
             batch_size: int, passes: dict[str, float]) -> None:
    """替每個數值、字串欄位加上 ``bins``（PSI 的分組方式與每期各組筆數）。基準期整期都是
    空值的欄位沒辦法分組，``bins`` 是 None。數值欄的切點是基準期的十分位數，相同的合併。"""
    started = time.monotonic()
    top = compute_top_values(df, time_col, periods[0], columns, batch_size)
    passes["top_values"] = round(time.monotonic() - started, 1)

    started = time.monotonic()
    specs = []
    for c in columns:
        if c["kind"] == "numeric" and c["quantiles"][0] is not None:
            specs.append((c, {"edges": sorted({c["quantiles"][0][i] for i in _DECILES})}))
        elif c["kind"] == "string" and top[c["name"]]:
            specs.append((c, {"values": top[c["name"]]}))
    counts = compute_bin_counts(df, time_col, periods, specs, batch_size)
    for c in columns:
        if c["kind"] != "unchecked":
            c["bins"] = None
    for c, spec in specs:
        c["bins"] = {**spec, "counts": counts[c["name"]]}
    passes["bins"] = round(time.monotonic() - started, 1)


def corr_skip_reasons(columns: list[dict], rows: list[int]) -> dict[str, str]:
    """run 時就不算相關性的欄位與原因：``string``（字串欄位）、``empty``（最新一期整期都是空值）、
    ``const``（最新一期只有一個值）。都不靠門檻：最新一期「幾乎都是空值」的照樣算，由 ``render`` 依
    當下的 ``null_ratio`` 排除（``corr_excluded``），換門檻才不用重跑。"""
    out = {}
    for c in columns:
        if c["kind"] == "string":
            out[c["name"]] = "string"
        elif c["kind"] == "numeric":
            if c["null"][-1] == rows[-1]:
                out[c["name"]] = "empty"
            elif c["min"][-1] == c["max"][-1]:
                out[c["name"]] = "const"
    return out


def _sorted_groups(x: "np.ndarray") -> tuple["np.ndarray", "np.ndarray", "np.ndarray"]:
    """一欄有值的列依值排好：（列號、每一群同值在排序後的起點、排序後每一筆屬於第幾群）。"""
    import numpy as np

    idx = np.flatnonzero(~np.isnan(x))
    order = idx[np.argsort(x[idx])]  # 同值取平均名次，不需要穩定排序
    sv = x[order]
    new = np.r_[True, sv[1:] != sv[:-1]] if len(sv) else np.zeros(0, dtype=bool)  # 整欄都是空值時沒有任何一群
    # 抽樣最多約 5 萬筆，int32 就夠，記憶體省一半
    return order.astype(np.int32), np.flatnonzero(new).astype(np.int32), (np.cumsum(new) - 1).astype(np.int32)


def _ranks_within(groups: tuple, mask: "np.ndarray") -> "np.ndarray":
    """一欄在 ``mask`` 那些筆裡的名次（從 1 起算，同值取平均），依列號放回；``mask`` 以外的列沒有意義。

    沿用 ``_sorted_groups`` 排好的順序，只數 ``mask`` 裡的筆，不重新排序，每一對欄位只要正比於筆數的時間。
    """
    import numpy as np

    order, starts, gid = groups
    size = np.add.reduceat(mask[order].astype(np.int64), starts) if len(order) else np.zeros(0)
    before = np.cumsum(size) - size  # 每一群同值之前，mask 裡有幾筆
    out = np.empty(len(mask))
    out[order] = (before + (size + 1) / 2)[gid]
    return out


def _average_ranks(x: "np.ndarray") -> "np.ndarray":
    """一欄換成名次（從 1 起算，同值取平均名次）；NaN 留著。"""
    import numpy as np

    ok = ~np.isnan(x)
    return np.where(ok, _ranks_within(_sorted_groups(x), ok), np.nan)


def spearman_pairs(values: "np.ndarray", names: list[str], *, min_rows: int = CORR_MIN_ROWS,
                   keep: float = CORR_KEEP) -> dict:
    """``values`` 每一欄的兩兩 Spearman 相關係數（NaN 是空值），每一對只用兩欄都有值的筆數。

    分兩步。先篩：每一欄整欄排名次，兩兩的筆數、和、平方和、交叉乘積各用一次矩陣相乘算完，不逐對
    迴圈。這一步的名次是整欄一起排的，有空值時只是近似：空值的機率跟值的大小有關時（例如資產越多越
    可能有某產品的餘額），實測差到 0.03，會把 0.95 的門檻翻過去。所以只拿它篩：絕對值 ≥ ``keep`` −
    ``CORR_SCREEN_MARGIN`` 的配對，再逐對在兩欄都有值的那些筆裡重排名次、算精確值，存精確值 ≥ ``keep``
    的。兩欄都有值的筆就是兩欄各自有值的全部筆時，整欄的名次已經是精確的，不重算。

    兩欄都有值的筆數少於 ``min_rows``、或其中一欄在這些筆裡沒有變化，不給相關係數。回傳 ``pairs``：
    [欄, 欄, 相關係數, 兩欄都有值的筆數]；``sparse``：有值的筆數本身就少於 ``min_rows`` 的欄 → 有值的
    筆數（它的每一對都算不了）；``flat``：在這些筆裡只有一個值的欄；``few_pairs``、``flat_pairs``：
    兩欄各自都夠、這一對卻筆數不夠或沒有變化的對數。原因歸到欄位本身，報告才不會因為一欄很空，
    就把每一欄都列成「有配對沒算」。
    """
    import numpy as np

    values = np.asfortranarray(values)  # 一欄連續存放，逐欄排名次時才不會跳著讀；run 拉回來時就是這樣
    k = values.shape[1]
    valid = ~np.isnan(values)
    m = valid.astype(np.float64)
    x = np.zeros(values.shape, order="F")
    for j in range(k):
        rk = _average_ranks(values[:, j])[valid[:, j]]
        if len(rk):
            x[valid[:, j], j] = rk - rk.mean()  # 減掉平均，平方和小一點，浮點相減的誤差也小一點
    # 有些平台的 BLAS 在 matmul 會誤報 invalid value：輸入都是有限值，結果和逐對計算一致（測試對照過）。
    # 兩欄都沒有值時 n 是 0，除出來的 NaN 由下面的 few 擋掉
    with np.errstate(divide="ignore", invalid="ignore"):
        n = m.T @ m
        s = x.T @ m  # s[i, j]：兩欄都有值的那些筆裡，第 i 欄的和
        q = (x * x).T @ m  # 同上，平方和
        p = x.T @ x  # 交叉乘積；x 在空值的地方是 0，只剩兩欄都有值的筆
        var = q - s * s / n
        r = (p - s * s.T / n) / np.sqrt(var * var.T)
    # 「沒有變化」的 var 理論上是 0，浮點相減會留一點誤差。不同的值名次至少差同值筆數的一半，
    # 有變化的 var 遠大於平方和的十億分之一，用它當界線不會誤判
    flat = var <= 1e-9 * np.maximum(q, 1.0)
    few = n < min_rows
    no_var = ~few & (flat | flat.T)
    count = valid.sum(axis=0)
    sparse = count < min_rows
    col_flat = ~sparse & np.diagonal(flat)  # 對角線＝這一欄自己有值的那些筆
    iu, ju = np.triu_indices(k, 1)
    screen = ~few[iu, ju] & ~no_var[iu, ju] & (np.abs(r[iu, ju]) >= keep - CORR_SCREEN_MARGIN)
    by_col = valid.T  # valid 是一欄連續存放，轉置後一欄就是連續的一列
    groups: dict[int, tuple] = {}  # 只替要重排的欄再排一次序、留著，不是每一欄都留

    def ranks(c: int, both: "np.ndarray", n_both: int) -> "np.ndarray":
        if n_both == count[c]:  # 這一欄有值的筆就是兩欄都有值的筆：整欄的名次已經是精確的
            return x[both, c]
        if c not in groups:
            groups[c] = _sorted_groups(values[:, c])
        return _ranks_within(groups[c], both)[both]

    pairs = []
    for i, j in zip(iu[screen], ju[screen]):
        v = r[i, j]
        if not n[i, j] == count[i] == count[j]:  # 有一欄在對方空的地方有值：那一欄的名次要在兩欄都有值的筆裡重排
            both = by_col[i] & by_col[j]
            v = np.corrcoef(ranks(i, both, n[i, j]), ranks(j, both, n[i, j]))[0, 1]
        if round(abs(float(v)), 9) >= keep:
            pairs.append([names[i], names[j], round(float(np.clip(v, -1.0, 1.0)), 6), int(n[i, j])])
    bad = sparse | col_flat
    rest = ~bad[iu] & ~bad[ju]
    return {
        "pairs": pairs,
        "sparse": {names[i]: int(count[i]) for i in np.flatnonzero(sparse)},
        "flat": [names[i] for i in np.flatnonzero(col_flat)],
        "few_pairs": int((few[iu, ju] & rest).sum()),
        "flat_pairs": int((no_var[iu, ju] & rest).sum()),
    }


def compute_correlation(df: "SparkDataFrame", time_col: str, latest: Any, latest_rows: int,
                        columns: list[dict], rows: list[int], batch_size: int) -> dict:
    """最新一期抽約 ``CORR_SAMPLE_ROWS`` 筆、拉回 driver 算 Spearman。

    抽樣在 Spark 做、seed 固定：``sample(fraction = 50,000 ÷ 該期筆數)``，該期不到 50,000 筆就全拿；
    抽到的筆數會差幾百筆。不再 ``limit``：它把抽到的每一筆擠進同一個 task，500 欄就超過序列化的
    緩衝區上限，實測失敗。抽到的先 persist，再一批 ``batch_size`` 欄 collect 一次、填進預先開好的
    矩陣：一次拉完，幾千萬個值要同時變成 Python 物件，1,500 欄時 driver 記憶體多將近 3 倍。
    """
    import numpy as np
    from pyspark.sql import functions as F

    skipped = corr_skip_reasons(columns, rows)
    use = [c for c in columns if c["kind"] == "numeric" and c["name"] not in skipped]
    out = {"period_rows": latest_rows, "rows": 0, "columns": [c["name"] for c in use], "skipped": skipped,
           "pairs": [], "sparse": {}, "flat": [], "few_pairs": 0, "flat_pairs": 0}
    if not use:
        return out
    sample = df.where(F.col(_quote(time_col)) == F.lit(latest)).select(
        *[_cleaned(c["name"], c["dtype"]).cast("double").alias(f"_{i}") for i, c in enumerate(use)]
    )
    if latest_rows > CORR_SAMPLE_ROWS:
        sample = sample.sample(fraction=CORR_SAMPLE_ROWS / latest_rows, seed=CORR_SAMPLE_SEED)
    sample = sample.withColumn("_id", F.monotonically_increasing_id()).persist()
    try:
        ids, values = None, None
        for b in range(0, len(use), batch_size):
            cols = [f"_{i}" for i in range(b, min(len(use), b + batch_size))]
            got = sorted(sample.select("_id", *cols).collect(), key=lambda r: r[0])
            if values is None:
                ids, values = [r[0] for r in got], np.empty((len(got), len(use)), order="F")
            elif [r[0] for r in got] != ids:  # 每一批都要是同一批筆，否則欄和欄會錯位
                raise RuntimeError("相關性的抽樣在兩批欄位之間變了，請重跑")
            values[:, b:b + len(cols)] = np.array([r[1:] for r in got], dtype=float).reshape(len(got), len(cols))
    finally:
        sample.unpersist()
    out["rows"] = len(values)
    out.update(spearman_pairs(values, out["columns"]))
    return out


def profile_table(spark: "SparkSession", cfg: ProfileConfig, batch_size: int,
                  accuracy: int = DEFAULT_QUANTILE_ACCURACY) -> dict:
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
        df, cfg.time_col, columns, batch_size, accuracy=accuracy if cfg.distribution else None
    )
    passes = {"basic": round(time.monotonic() - started, 1)}
    for c in columns:
        c.update(stats.get(c["name"], {}))
    if cfg.distribution and periods:
        add_bins(df, cfg.time_col, periods, columns, batch_size, passes)
    corr = None
    if cfg.correlation and periods:
        t0 = time.monotonic()
        corr = compute_correlation(df, cfg.time_col, periods[-1], rows[-1], columns, rows, batch_size)
        passes["correlation"] = round(time.monotonic() - t0, 1)
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
            "checks": {"distribution": cfg.distribution, "correlation": cfg.correlation},
            "thresholds": cfg.thresholds,
        },
        "periods": [_period_label(p) for p in periods],
        "rows": rows,
        "null_time_rows": null_time_rows,
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "pass_seconds": passes,
        **({"quantile_accuracy": accuracy} if cfg.distribution else {}),
        "batch_size": batch_size,
        "columns": columns,
        **({"correlation": corr} if corr is not None else {}),
    }


# ---------------------------------------------------------------------------
# 判斷（不開 Spark；render 換門檻就是重跑這一段）
# ---------------------------------------------------------------------------

FLAG_ORDER = ("null_all", "const_all", "null_some", "const_some", "const_each", "drift", "tail", "corr")
FLAG_NAMES = {
    "null_all": "每一期都幾乎是空值",
    "null_some": "部分期別幾乎是空值",
    "const_all": "整欄只有一個值",
    "const_some": "部分期別只有一個值",
    "const_each": "每期各只有一個值",
    "drift": "分布偏移",
    "tail": "極端值",
    "corr": "高度相關",
}
NULL_FLAGS = ("null_all", "null_some")
# 不判斷分布偏移、極端值的欄位：已經可以排除，或每期各只有一個值（設計文件第五節）。
# 報告也不列它們的〈值幾乎都相同〉與 PSI。
NO_DIST_FLAGS = ("null_all", "const_all", "const_each")
CONST_FLAGS = ("const_all", "const_some", "const_each")
# 已經可以直接排除的欄位，不再判斷其他問題（設計文件第五節），也不列進高度相關的配對
EXCLUDE_FLAGS = ("null_all", "const_all")


def null_ratios(col: dict, rows: list[int]) -> list[float]:
    return [nl / n for nl, n in zip(col["null"], rows)]


def one_value_periods(col: dict, rows: list[int]) -> tuple[list[int], list[int]]:
    """（有值的期別、其中只有一個值的期別）。全空的期別兩邊都不算，直接略過。"""
    valued = [t for t, (nl, n) in enumerate(zip(col["null"], rows)) if nl < n]
    const = [t for t in valued if col["min"][t] == col["max"][t]]
    return valued, const


PSI_EMPTY_SHARE = 0.0001  # 占比是 0 的組用這個值代替，避免 ln(0)


def _shares(counts: list[int]) -> list[float]:
    total = sum(counts)
    return [n / total for n in counts]


def psi_terms(base: list[int], cur: list[int]) -> list[float]:
    """PSI 每一組的項：（本期占比 − 基準期占比）× ln（本期占比 ÷ 基準期占比）。"""
    out = []
    for p, q in zip(_shares(base), _shares(cur)):
        p, q = p or PSI_EMPTY_SHARE, q or PSI_EMPTY_SHARE
        out.append((q - p) * math.log(q / p))
    return out


def psi_by_period(col: dict) -> list[float] | None:
    """每一期和最早一期（基準期）比的 PSI。不算的情況回 None：沒跑分布檢查（舊的
    ``profile.json``）、只讀到一期、基準期這一欄全是空值（最後一組是空值）。"""
    counts = (col.get("bins") or {}).get("counts")
    if not counts or len(counts) < 2 or not any(counts[0][:-1]):
        return None
    return [sum(psi_terms(counts[0], c)) for c in counts]


def drift_periods(col: dict, thresholds: dict[str, float]) -> list[int]:
    psi = psi_by_period(col) or []
    # 四捨五入到遠小於顯示精度的位數再比門檻，浮點誤差才不會讓剛好等於門檻的值漏掉
    return [t for t, v in enumerate(psi) if round(v, 9) >= thresholds["psi"]]


def drift_source(col: dict, thresholds: dict[str, float]) -> str:
    """PSI 最大的那一期，扣掉空值、只比有值的那幾組再算一次 PSI：沒達門檻（或那一期整期
    都是空值）算 ``null``，是空值比例在變；達門檻算 ``value``，是值的分布本身在變。"""
    counts = col["bins"]["counts"]
    psi = psi_by_period(col)
    t = max(range(len(psi)), key=lambda k: psi[k])
    if not any(counts[t][:-1]):
        return "null"
    values_only = sum(psi_terms(counts[0][:-1], counts[t][:-1]))
    return "value" if round(values_only, 9) >= thresholds["psi"] else "null"


def _slog(x: float) -> float:
    """sign(x) × ln(1 + |x|)：有正有負、有 0 也能取 log，而且不改變大小順序。"""
    return math.copysign(math.log1p(abs(x)), x)


def tail_ratios(col: dict, t: int) -> dict[str, tuple[float, float]] | None:
    """第 t 期最大值、最小值兩側的（倍數、log 後倍數）。

    倍數＝最大值離 p99（或最小值離 p1）的距離 ÷（p99 − p1）；log 後倍數是四個數先取
    ``_slog`` 再用同樣的公式。沒有分位數（字串欄、舊的 ``profile.json``）、整期都是空值、
    p1 = p99 時回 None：p1 = p99 時寬度是 0，「離多遠」沒有意義。
    """
    qs = col.get("quantiles")
    if not qs or qs[t] is None:
        return None
    p1, p99 = qs[t][0], qs[t][-1]
    if p1 == p99:
        return None
    lo, hi = col["min"][t], col["max"][t]
    out = {}
    for side, far, near in (("max", hi, p99), ("min", lo, p1)):
        sign = 1 if side == "max" else -1
        raw = sign * (far - near) / (p99 - p1)
        log = sign * (_slog(far) - _slog(near)) / (_slog(p99) - _slog(p1))
        out[side] = (raw, log)
    return out


def near_const_periods(col: dict) -> list[int]:
    """p1 = p99、但不只一個值的期別：那一期至少約 98% 的值相同。只供參考，不算問題；
    只有一個值（最小值 = 最大值）的期別是另一種問題，不重複列。"""
    qs = col.get("quantiles") or []
    return [t for t, q in enumerate(qs)
            if q is not None and q[0] == q[-1] and col["min"][t] != col["max"][t]]


class TailHit(NamedTuple):
    period: int
    side: str  # max／min
    ratio: float
    log_ratio: float


def tail_hits(col: dict, thresholds: dict[str, float]) -> list[TailHit]:
    """同一期、同一側兩個條件都成立的，依期別排。"""
    hits = []
    for t in range(len(col["min"])):
        for side, (raw, log) in (tail_ratios(col, t) or {}).items():
            if round(raw, 9) >= thresholds["tail_ratio"] and round(log, 9) >= thresholds["tail_log_ratio"]:
                hits.append(TailHit(t, side, raw, log))
    return hits


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
        return flags + ["const_each"]
    if const:
        flags.append("const_some")
    if drift_periods(col, thresholds):
        flags.append("drift")
    if tail_hits(col, thresholds):
        flags.append("tail")
    return flags


CORR_REASONS = ("string", "empty", "null", "const", "sparse", "flat")  # 〈未計算的欄位〉的原因，依這個順序認


def corr_excluded(profile: dict, thresholds: dict[str, float]) -> dict[str, str]:
    """沒有相關係數、或這次不列進高度相關的欄位 → 原因代號（``CORR_REASONS``）。

    run 時就沒算的（字串欄位、最新一期整期都是空值、最新一期只有一個值），抽到的筆數裡有值的太少或
    只有一個值的，加上依這次的 ``null_ratio``、最新一期幾乎都是空值的（``null``）。最後這種 run 時
    照樣算，在這裡才排除：``render`` 換門檻時跟〈空值〉一起變，不用重跑。
    """
    corr = profile.get("correlation")
    if not corr:
        return {}
    out = dict(corr["skipped"])
    for c in profile["columns"]:
        name = c["name"]
        if name in out or c["kind"] != "numeric":
            continue
        if c["null"][-1] / profile["rows"][-1] >= thresholds["null_ratio"]:
            out[name] = "null"
        elif name in corr["sparse"]:
            out[name] = "sparse"
        elif name in corr["flat"]:
            out[name] = "flat"
    return out


def high_corr_pairs(profile: dict, thresholds: dict[str, float],
                    flags: dict[str, list[str]]) -> list[tuple[str, str, float, int]]:
    """（欄、欄、相關係數、兩欄都有值的筆數），絕對值 ≥ ``corr``。沒跑相關性（舊的
    ``profile.json``）回空清單；這次不列的欄（``corr_excluded``）與已經可以直接排除的欄位不列。"""
    corr = profile.get("correlation")
    if not corr:
        return []
    excluded = corr_excluded(profile, thresholds)
    return [
        (a, b, r, n) for a, b, r, n in corr["pairs"]
        # 四捨五入到遠小於顯示精度的位數再比門檻，剛好等於門檻的才不會因為浮點誤差漏掉
        if round(abs(r), 9) >= thresholds["corr"]
        and a not in excluded and b not in excluded
        and not set(EXCLUDE_FLAGS) & (set(flags[a]) | set(flags[b]))
    ]


def corr_groups(pairs: list[tuple[str, str, float, int]], order: list[str]) -> list[list[str]]:
    """高度相關的配對連成組：A 像 B、B 像 C，三欄同一組。大的組在前，組內依 ``order``（表裡的欄位順序）。"""
    parent: dict[str, str] = {}

    def root(x: str) -> str:
        while parent.setdefault(x, x) != x:
            x = parent[x]
        return x

    for a, b, _, _ in pairs:
        parent[root(a)] = root(b)
    groups: dict[str, list[str]] = {}
    for name in order:
        if name in parent:
            groups.setdefault(root(name), []).append(name)
    pos = {name: i for i, name in enumerate(order)}
    return sorted(groups.values(), key=lambda g: (-len(g), pos[g[0]]))


def judge(profile: dict, thresholds: dict[str, float]) -> dict[str, list[str]]:
    flags = {
        c["name"]: judge_column(c, profile["rows"], thresholds)
        for c in profile["columns"]
        if c["kind"] != "unchecked"
    }
    for a, b, _, _ in high_corr_pairs(profile, thresholds, flags):
        for name in (a, b):
            if "corr" not in flags[name]:
                flags[name].append("corr")
    return flags


# ---------------------------------------------------------------------------
# 行動建議：把判斷結果整理成總結裡的四種做法（render 換門檻時一起重算）
# ---------------------------------------------------------------------------

# 「部分期別幾乎是空值」的欄位，出事那幾期比其他期別至少高這麼多（0.10＝10 個百分點）
# 才算上游斷掉；不到的是平常就很稀疏、偶爾越過門檻的欄位，歸「看業務意義決定」。
# 只決定總結裡的分組，不改判斷，所以不進 thresholds。
UPSTREAM_JUMP_MIN = 0.10
# 極端值在至少這個比例的期別都被標，比較像欄位本來就長尾（例如資產），歸「看業務意義決定」；
# 不到的像那幾期的資料錯了，歸「請上游查」。同樣只決定總結裡的分組，不進 thresholds。
TAIL_LONG_SHARE = 0.5

ACTION_ORDER = ("exclude", "upstream", "check", "judge", "corr")
ACTION_TEXTS = {
    "exclude": "可以排除：欄位沒有資訊",
    "upstream": "請上游查這幾期的資料",
    "check": "確認分布為什麼變了",
    "judge": "看業務意義決定要不要留",
    "corr": "高度相關的欄位每組留一欄",
}


def null_jump(col: dict, rows: list[int], thresholds: dict[str, float]) -> float | None:
    """出事那幾期（空值比例 ≥ 門檻）最低的空值比例，減掉其他期別最高的；沒有出事的期別回 None。"""
    th = thresholds["null_ratio"]
    r = null_ratios(col, rows)
    hit = [v for v in r if v >= th]
    rest = [v for v in r if v < th]
    if not hit or not rest:
        return None
    # 浮點數相減會把剛好 10 個百分點算成 0.0999…，四捨五入到遠小於顯示精度的位數
    return round(min(hit) - max(rest), 9)


def worst_tail(col: dict, thresholds: dict[str, float]) -> TailHit:
    """標到的期別、側裡，log 後倍數最大的那一個。"""
    return max(tail_hits(col, thresholds), key=lambda h: h.log_ratio)


def long_tail(col: dict, thresholds: dict[str, float]) -> bool:
    """有判斷的期別裡，至少 ``TAIL_LONG_SHARE`` 被標成極端值：比較像欄位本來就長尾。"""
    judged = sum(1 for t in range(len(col["min"])) if tail_ratios(col, t) is not None)
    hit = len({h.period for h in tail_hits(col, thresholds)})
    return hit >= TAIL_LONG_SHARE * judged


def upstream_periods_of(col: dict, rows: list[int], flags: list[str],
                        thresholds: dict[str, float]) -> list[int]:
    """這一欄要請上游查的期別；不用請上游查時回空清單。"""
    ts: set[int] = set()
    jump = null_jump(col, rows, thresholds) if "null_some" in flags else None
    if jump is not None and jump >= UPSTREAM_JUMP_MIN:
        ts |= {t for t, r in enumerate(null_ratios(col, rows)) if r >= thresholds["null_ratio"]}
    if "const_some" in flags:
        ts |= set(one_value_periods(col, rows)[1])
    if "tail" in flags and not long_tail(col, thresholds):
        # 只放最離譜的那一期：同一欄的其他期別多半是同一個原因
        ts.add(worst_tail(col, thresholds).period)
    return sorted(ts)


def actions(col: dict, rows: list[int], flags: list[str],
            thresholds: dict[str, float]) -> list[str]:
    """這一欄在總結裡要做的事，依 ``ACTION_ORDER``；沒有問題回空清單。

    一欄可能要做兩件事（例如某一期斷掉、其他期別每期各只有一個值），兩件都列。
    """
    if {"null_all", "const_all"} & set(flags):
        return ["exclude"]
    out = []
    jump = null_jump(col, rows, thresholds) if "null_some" in flags else None
    upstream = upstream_periods_of(col, rows, flags, thresholds)
    if upstream:
        out.append("upstream")
    # 偏移的期別都已經列在「請上游查」：同一件事（例如那一期斷掉、填了預設值），不重複列
    if "drift" in flags and not set(drift_periods(col, thresholds)) <= set(upstream):
        out.append("check")
    if ("const_each" in flags or (jump is not None and jump < UPSTREAM_JUMP_MIN)
            or ("tail" in flags and long_tail(col, thresholds))):
        out.append("judge")
    if "corr" in flags:
        out.append("corr")
    return out


# ---------------------------------------------------------------------------
# 報告：單一頁面，表格可以排序、篩選、複製欄名（純手寫 JS，不連網、不加套件）
# ---------------------------------------------------------------------------

_VAR_CHOICES = (0, 0.5, 1, 5, 10)  # 〈有變動但沒達門檻〉的篩選選項，百分點
_VAR_DEFAULT = 1
SPARK_GAP_MAX_PERIODS = 75  # 小長條圖 150px：每根 1px 加 1px 空隙，最多 75 期
# 字串欄位各值占比的顏色，依基準期占比排；第 7 名以後淺灰褐、其他值深灰、空值淺灰
_CAT_COLORS = ("#4e79a7", "#76b7b2", "#b07aa1", "#9c755f", "#59a14f", "#edc948")
_CAT_REST, _CAT_OTHER, _CAT_NULL = "#d4ccc6", "#7b7470", "#e3e7ec"
# 相關係數矩陣：三欄以上的組各畫一張，一張最多畫這麼多欄（依表裡的欄位順序取前面的）。
# 正式環境量過再定（設計文件第八節）。格子的深淺：絕對值 0.5～0.8 淺、0.8～門檻深、達門檻橘。
CORR_MATRIX_MAX_COLS = 40
_CM_MID = 0.8
_CORR_SKIP_NAMES = {"string": "字串欄位", "empty": "最新一期整期都是空值", "null": "最新一期幾乎都是空值",
                    "const": "最新一期只有一個值", "sparse": "抽到的筆數裡有值的太少",
                    "flat": "抽到的筆數裡只有一個值"}


def _pct_threshold(v: float) -> str:
    return f"{v * 100:g}%"


def _fmt_num(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, int) or float(v).is_integer():
        return f"{int(v):,}"
    # 最多 6 位小數，但整數部分很長時少給幾位：double 只有約 15 位有效數字，再多就是浮點雜訊
    digits = max(0, min(6, 15 - len(str(int(abs(v))))))
    return f"{v:,.{digits}f}".rstrip("0").rstrip(".")


def _pct(x: float | None, digits: int | None = None) -> str:
    """空值比例。不會把「不到 100%」顯示成 100%，也不會把「大於 0」顯示成 0%。"""
    if x is None:
        return ""
    if x == 0:
        return "0%"
    if x == 1:
        return "100%"
    if digits is None:
        digits = 2 if x < 0.01 or x > 0.99 else 1
    s = f"{x * 100:.{digits}f}%"
    if s.startswith("100"):
        return "> 99.99%"
    if float(s[:-1]) == 0:
        return "< 0.01%"
    return s


def _pct_range(values: list[float]) -> str:
    """最低～最高；兩端用同樣的小數位數，一樣時只寫一個值。"""
    lo, hi = min(values), max(values)
    digits = 2 if any(0 < v < 0.01 or 0.99 < v < 1 for v in (lo, hi)) else 1
    a, b = _pct(lo, digits), _pct(hi, digits)
    return a if a == b else f"{a}～{b}"


def _duration(seconds: float) -> str:
    m, s = divmod(int(round(seconds)), 60)
    return f"{m} 分 {s} 秒" if m else f"{s} 秒"


def _period_labels(periods: list[str]) -> list[str]:
    """期別都是同月不重複的日期時，只寫到月（2025-06）。"""
    months = [p[:7] for p in periods]
    if all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", p) for p in periods) and len(set(months)) == len(months):
        return months
    return list(periods)


def _dtype_group(dtype: str) -> str:
    if dtype in _INTEGER_TYPES:
        return "int"
    if dtype.startswith("decimal"):
        return "decimal"
    if dtype in _FLOAT_TYPES:
        return dtype
    return "string"


def _shorten(items: list[str], unit: str) -> tuple[str, str]:
    """（顯示文字、完整清單）。超過三個只寫前兩個和總數，完整清單放滑鼠提示。"""
    full = "、".join(items)
    if len(items) <= 3:
        return full, full
    return f"{'、'.join(items[:2])}…共 {len(items)} {unit}", full


def _e(v: Any) -> str:
    return html.escape(str(v), quote=True)


def _attrs(d: dict[str, Any]) -> str:
    return "".join(f' {k}="{_e(v)}"' for k, v in d.items() if v is not None)


def _td(display: str, *, v: Any = None, num: bool = False, cls: str = "", title: str = "") -> str:
    """一格。``display`` 是已經 escape 過的 HTML；``v`` 是排序用的原始值（None＝依文字排）。"""
    classes = " ".join(c for c in ("n" if num else "", cls) if c)
    a = {"class": classes or None, "title": title or None}
    if v is not None:
        a["data-v"] = v
    return f"<td{_attrs(a)}>{display}</td>"


def _td_name(name: str) -> str:
    """欄名：點了跳到〈全部欄位〉那一列並展開各期明細。"""
    return _td(f'<a class="col" href="#col-{_e(name)}" data-col="{_e(name)}">{_e(name)}</a>',
               v=name.lower(), cls="name")


def _td_num(x: Any, display: str | None = None) -> str:
    return _td(_e(_fmt_num(x) if display is None else display), v="" if x is None else x, num=True)


def _table(tid: str, head: list[tuple[str, bool]], rows: list[tuple[dict, list[str]]],
           *, sort: tuple[int, str] | None = None, cls: str = "") -> str:
    """``head`` 是（表頭、是否數字欄）；``rows`` 是（列的屬性、各格 HTML）。

    列已經依預設排序排好，沒有 JS 也讀得懂；``sort`` 只標出表頭的箭頭。
    """
    th = "".join(
        f'<th{_attrs({"class": "n" if num else None, "data-dir": (sort[1] if sort and sort[0] == i else None)})}>'
        f"{_e(title)}</th>"
        for i, (title, num) in enumerate(head)
    )
    body = "".join(f"<tr{_attrs(a)}>{''.join(cells)}</tr>" for a, cells in rows)
    return (f'<div class="tbox"><table id="{_e(tid)}" class="t {cls}">'
            f"<thead><tr>{th}</tr></thead><tbody>{body}</tbody></table></div>")


def _toolbar(tid: str, *, selects: tuple = ()) -> str:
    """表格上方的工具列：欄名篩選、下拉選單、計數、複製目前列出的欄名。

    ``selects``：（列屬性、說明、[(值, 文字)]、預設值、mode）。mode 是 ``token``（列的屬性
    含這個值，空白分隔）或 ``min``（列的屬性 ≥ 這個值）。
    """
    parts = [f'<div class="toolbar" data-for="{_e(tid)}">',
             '<input class="q" type="search" placeholder="篩選欄名，空白分隔多個關鍵字">']
    for attr, label, options, default, mode in selects:
        opts = "".join(
            f'<option value="{_e(v)}"{" selected" if str(v) == str(default) else ""}>{_e(t)}</option>'
            for v, t in options
        )
        parts.append(f'<label>{_e(label)} <select data-attr="{_e(attr)}" data-mode="{mode}">{opts}</select></label>')
    parts.append('<span class="count"></span>')
    parts.append(f'<button type="button" data-copy-rows="{_e(tid)}">複製列出的欄名</button>')
    parts.append("</div>")
    return "".join(parts)


def _section(sid: str, title: str, body: str, *, level: int = 2) -> str:
    return f'<section id="{sid}"><h{level}>{_e(title)}</h{level}>{body}</section>'


def _p(text: str) -> str:
    return f'<p class="desc">{text}</p>'


def _note(text: str) -> str:
    return f'<p class="note">{text}</p>'


def _none() -> str:
    return '<p class="none">這次沒有。</p>'


def _not_run(check: str = "distribution") -> str:
    return (f'<p class="none">這次沒有跑。要看這一段，把設定檔的 <code>checks.{check}</code> 改成 '
            "<code>true</code> 再跑一次 <code>run</code>；<code>render</code> 不會補算。</p>")


def _spark_legend(items: list[tuple[str, str]], how_to_read: str = "") -> str:
    """小長條圖的圖例：（樣式、說明），最後可以再接一句怎麼讀。"""
    out = []
    for cls, label in items:
        h = "0%" if cls == "empty" else ("60%" if cls == "" else "100%")
        out.append(f'<span><span class="spark key"><i class="{cls}" style="--h:{h}"></i></span>{_e(label)}</span>')
    if how_to_read:
        out.append(f"<span>{_e(how_to_read)}</span>")
    return f'<div class="legend">{"".join(out)}</div>'


_CSS = """
:root{--fg:#1f2328;--muted:#59636e;--line:#d1d9e0;--soft:#f6f8fa;--link:#0969da;
--track:#eef1f4;--bar:#8fa7bf;--grey:#b8c4d0;--hit:#c2410c;--qo:#cfdae6;--qi:#8fa7bf}
*{box-sizing:border-box}
body{margin:0;color:var(--fg);background:#fff;font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",
"Microsoft JhengHei","PingFang TC","Noto Sans TC",sans-serif}
code,.name,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px}
a{color:var(--link);text-decoration:none}a:hover{text-decoration:underline}
header.top{position:sticky;top:0;z-index:20;background:#fff;border-bottom:1px solid var(--line);
padding:10px 24px;display:flex;flex-wrap:wrap;gap:4px 20px;align-items:center}
header.top .title{font-size:16px;font-weight:600}
header.top .scope{color:var(--muted);font-size:13px;flex:1 1 400px}
header.top input{width:280px;padding:5px 8px;border:1px solid var(--line);border-radius:6px}
.layout{display:grid;grid-template-columns:220px minmax(0,1fr);gap:32px;max-width:1500px;margin:0 auto;padding:20px 24px 80px}
nav.toc{position:sticky;top:76px;align-self:start;font-size:13px}
nav.toc a{display:flex;justify-content:space-between;color:var(--fg);padding:3px 8px;border-radius:6px}
nav.toc a.sub{padding-left:22px;color:var(--muted);font-size:12px}
nav.toc a:hover{background:var(--soft);text-decoration:none}
nav.toc a span{color:var(--muted);font-variant-numeric:tabular-nums}
nav.toc hr{border:0;border-top:1px solid var(--line);margin:8px 0}
@media (max-width:1100px){.layout{grid-template-columns:1fr}nav.toc{position:static}}
section{margin:0 0 40px;scroll-margin-top:80px}
section section{margin:0 0 28px}
h2{font-size:20px;margin:0 0 8px;padding-bottom:6px;border-bottom:1px solid var(--line)}
h3{font-size:16px;margin:24px 0 4px}
p.desc{max-width:80ch;color:#333;margin:6px 0 10px}
p.note{max-width:80ch;color:var(--muted);font-size:13px;margin:6px 0}
p.none{color:var(--muted)}
.tbox{max-height:72vh;overflow:auto;border:1px solid var(--line);border-radius:6px;margin:8px 0}
table.t{border-collapse:separate;border-spacing:0;width:100%;font-variant-numeric:tabular-nums}
table.t th,table.t td{padding:4px 10px;border-bottom:1px solid #eaeef2;text-align:left;vertical-align:middle;white-space:nowrap}
table.t td.wrap{white-space:normal;min-width:120px;max-width:460px}
table.t td.brk{white-space:normal;word-break:keep-all}
table.t th{position:sticky;top:0;z-index:2;background:var(--soft);font-weight:600;cursor:pointer;user-select:none}
table.t th[data-dir=asc]::after{content:" ▲";color:var(--muted)}
table.t th[data-dir=desc]::after{content:" ▼";color:var(--muted)}
table.t .n{text-align:right}
table.t tbody tr:hover td{background:#f3f8ff}
table.kv th{width:12em;cursor:default}
table.actions td{vertical-align:top;padding:8px 10px}
ul.names{margin:6px 0 0;padding-left:18px;font-weight:normal;font-size:13px}
ul.names li{margin:2px 0}ul.names button{margin-left:6px}
.toolbar{display:flex;flex-wrap:wrap;gap:6px 14px;align-items:center;margin:10px 0 0;font-size:13px}
.toolbar input.q{width:260px;padding:4px 8px;border:1px solid var(--line);border-radius:6px}
.toolbar select{padding:2px 4px}
.toolbar .count{color:var(--muted)}
button{font:inherit;font-size:12px;padding:3px 10px;border:1px solid var(--line);border-radius:6px;background:#fff;cursor:pointer}
button:hover{background:var(--soft)}
pre{background:var(--soft);border:1px solid var(--line);border-radius:6px;padding:10px 12px;margin:4px 0;overflow:auto;max-height:260px}
details>summary{cursor:pointer;color:var(--link)}
.legend{display:flex;flex-wrap:wrap;gap:4px 18px;font-size:12px;color:var(--muted);margin:6px 0 4px;align-items:center}
.spark{display:inline-flex;align-items:stretch;gap:1px;width:150px;height:20px;vertical-align:middle;cursor:crosshair}
.spark i{flex:1 1 0;min-width:1px;background:linear-gradient(to top,var(--bar) var(--h),var(--track) var(--h))}
.spark i.h{background:linear-gradient(to top,var(--hit) var(--h),var(--track) var(--h))}
.spark i.g{background:linear-gradient(to top,var(--grey) var(--h),var(--track) var(--h))}
.spark i.d{background:linear-gradient(to top,var(--muted) var(--h),var(--track) var(--h))}
.spark i.on{outline:1px solid #111}
.spark.dense{gap:0}.spark.dense i{min-width:0}
.spark.key{width:auto;cursor:default;margin-right:4px}.spark.key i{width:6px;flex:none}
.spark.tall{height:30px}
.spark i.q{--a:var(--log-a);--b:var(--log-b);--m:var(--log-m);--c:var(--log-c);--d:var(--log-d)}
.linq .spark i.q{--a:var(--lin-a);--b:var(--lin-b);--m:var(--lin-m);--c:var(--lin-c);--d:var(--lin-d)}
.spark i.q{background:linear-gradient(to top,transparent calc(var(--m) - 1px),var(--fg) calc(var(--m) - 1px),
var(--fg) calc(var(--m) + 1px),transparent calc(var(--m) + 1px)),linear-gradient(to top,var(--track) var(--a),
var(--qo) var(--a),var(--qo) var(--b),var(--qi) var(--b),var(--qi) var(--c),var(--qo) var(--c),var(--qo) var(--d),
var(--track) var(--d))}
.qkey{display:inline-block;width:14px;height:10px;margin-right:4px;vertical-align:middle}
button[data-scale].on{background:var(--soft);font-weight:600}
.qkey.o{background:var(--qo)}.qkey.i{background:var(--qi)}.qkey.m{height:2px;background:var(--fg)}
.tip{position:fixed;z-index:50;background:#1f2328;color:#fff;font-size:12px;padding:3px 8px;border-radius:4px;pointer-events:none;white-space:nowrap}
tr.detail td{background:var(--soft);white-space:normal}
table.mini{border-collapse:collapse;margin:4px 0 8px;font-size:12px}
table.mini th,table.mini td{padding:2px 10px;border-bottom:1px solid var(--line);text-align:right}
table.mini th:first-child,table.mini td:first-child{text-align:left}
button.exp{border:0;background:none;padding:0;color:var(--link);font-family:inherit;font-size:13px}
button.exp::before{content:"▸ ";color:var(--muted)}tr.open button.exp::before{content:"▾ "}
.cms{display:flex;flex-wrap:wrap;gap:20px 36px;align-items:flex-start;margin:10px 0}
.cmx{display:grid;grid-template-columns:auto auto;gap:2px 6px;font-size:11px}
.cmx .cap{grid-column:1/3;font-size:13px;font-weight:600}
.cmx .rl{display:grid;grid-auto-rows:15px;line-height:15px;text-align:right;white-space:nowrap}
.cmx .rl span b{display:inline-block;min-width:2em;margin-right:6px;color:var(--muted);font-weight:normal}
.cmx .cl{display:grid;grid-template-columns:repeat(var(--k),15px);color:var(--muted);text-align:center;font-size:9px}
.cm{display:grid;grid-template-columns:repeat(var(--k),14px);grid-auto-rows:14px;gap:1px;cursor:crosshair;align-self:start}
.cm i{background:var(--track)}.cm i.a{background:var(--qo)}.cm i.b{background:var(--bar)}
.cm i.h{background:var(--hit)}.cm i.d{background:var(--fg)}.cm i.on{outline:1px solid #111}
.qkey.cell{width:14px;height:14px}
"""

# 小長條圖的滑鼠提示從內嵌的 col-data 現算，不在每一根柱子上放文字：
# 上千欄 × 幾十期時，頁面會小很多。
_JS = r"""
(function(){"use strict";
function key(td){if(!td)return null;var v=td.getAttribute("data-v");
 if(v===null)return td.textContent.trim().toLowerCase();if(v==="")return null;
 var n=Number(v);return isNaN(n)?v:n;}
function rowsOf(t){return Array.prototype.filter.call(t.tBodies[0].rows,function(r){return !r.classList.contains("detail");});}
function sortBy(t,i,dir){var tb=t.tBodies[0],rs=rowsOf(t),s=dir==="asc"?1:-1;
 rs.forEach(function(r,k){r._k=k;});
 rs.sort(function(a,b){var x=key(a.cells[i]),y=key(b.cells[i]);
  if(x===null&&y===null)return a._k-b._k;if(x===null)return 1;if(y===null)return -1;
  var c=(typeof x==="number"&&typeof y==="number")?x-y:String(x).localeCompare(String(y));
  return c?c*s:a._k-b._k;});
 rs.forEach(function(r){tb.appendChild(r);if(r._detail)tb.appendChild(r._detail);});
 t.querySelectorAll("thead th").forEach(function(th,k){if(k===i)th.setAttribute("data-dir",dir);else th.removeAttribute("data-dir");});}
document.querySelectorAll("table.t").forEach(function(t){if(t.classList.contains("kv"))return;
 t.querySelectorAll("thead th").forEach(function(th,i){th.addEventListener("click",function(){
  sortBy(t,i,th.getAttribute("data-dir")==="asc"?"desc":"asc");});});});
function apply(bar){var t=document.getElementById(bar.getAttribute("data-for"));if(!t)return;
 var q=bar.querySelector("input.q"),terms=(q?q.value:"").toLowerCase().split(/\s+/).filter(Boolean);
 var sels=bar.querySelectorAll("select[data-attr]"),shown=0,total=0;
 rowsOf(t).forEach(function(r){total++;var name=(r.getAttribute("data-name")||"").toLowerCase();
  var ok=terms.every(function(w){return name.indexOf(w)>=0;});
  sels.forEach(function(s){if(!ok||s.value==="")return;var v=r.getAttribute("data-"+s.getAttribute("data-attr"))||"";
   ok=s.getAttribute("data-mode")==="min"?Number(v)>=Number(s.value):v.split(" ").indexOf(s.value)>=0;});
  r.hidden=!ok;if(r._detail)r._detail.hidden=!ok||!r.classList.contains("open");if(ok)shown++;});
 var cnt=bar.querySelector(".count");if(cnt)cnt.textContent="顯示 "+shown+"／"+total+" 列";}
var bars=document.querySelectorAll(".toolbar");
bars.forEach(function(bar){bar.querySelectorAll("input,select").forEach(function(el){
 el.addEventListener(el.type==="search"?"input":"change",function(){apply(bar);});});});
var g=document.getElementById("global-q");
if(g)g.addEventListener("input",function(){bars.forEach(function(bar){var q=bar.querySelector("input.q");if(q){q.value=g.value;apply(bar);}});});
function copy(text,btn){var old=btn.textContent;function say(s){btn.textContent=s;setTimeout(function(){btn.textContent=old;},1500);}
 function fallback(){var ta=document.createElement("textarea");ta.value=text;ta.style.position="fixed";ta.style.opacity="0";
  document.body.appendChild(ta);ta.select();var ok=false;try{ok=document.execCommand("copy");}catch(e){}
  document.body.removeChild(ta);say(ok?"已複製":"複製失敗");}
 if(navigator.clipboard&&window.isSecureContext)navigator.clipboard.writeText(text).then(function(){say("已複製");},fallback);else fallback();}
document.addEventListener("click",function(ev){var b=ev.target.closest("button");if(!b)return;
 if(b.hasAttribute("data-scale")){var lin=b.getAttribute("data-scale")==="lin";document.body.classList.toggle("linq",lin);
  document.querySelectorAll("button[data-scale]").forEach(function(x){x.classList.toggle("on",x.getAttribute("data-scale")===(lin?"lin":"log"));});return;}
 if(b.hasAttribute("data-copy-rows")){var t=document.getElementById(b.getAttribute("data-copy-rows"));
  copy(rowsOf(t).filter(function(r){return !r.hidden;}).map(function(r){return r.getAttribute("data-name");}).join("\n"),b);}
 else if(b.hasAttribute("data-copy-text")){copy(b.getAttribute("data-copy-text"),b);}
 else if(b.classList.contains("exp")){toggle(b.closest("tr"));}});
var DATA=JSON.parse((document.getElementById("fp-data")||{}).textContent||"{}");
function fmt(v){if(v===null||v===undefined)return "";if(typeof v==="number"){
 return Number.isInteger(v)?v.toLocaleString("en-US"):v.toLocaleString("en-US",{maximumFractionDigits:6});}return String(v);}
function pct(n,d){if(!d)return "";if(n===0)return "0%";if(n===d)return "100%";var x=n/d;
 var s=(x*100).toFixed(x<0.01||x>0.99?2:1);if(Number(s)>=100)return "> 99.99%";if(Number(s)===0)return "< 0.01%";return s+"%";}
function pctx(x){return pct(Math.round(x*10000),10000);}
function toggle(tr){if(!tr)return;var name=tr.getAttribute("data-name"),c=DATA.cols&&DATA.cols[name];if(!c)return;
 if(!tr._detail){var dist=(DATA.dist||{})[name],hq=dist&&dist.q,hp=dist&&dist.psi;
  var h="<table class='mini'><thead><tr><th>期別</th><th>總筆數</th><th>空值筆數</th><th>空值比例</th><th>最小值</th>"+
   (hq?"<th>p1</th><th>p25</th><th>中位數</th><th>p75</th><th>p99</th>":"")+"<th>最大值</th>"+(hp?"<th>PSI</th>":"")+"</tr></thead><tbody>";
  DATA.periods.forEach(function(p,i){var q=hq&&dist.q[i];h+="<tr><td>"+esc(p)+"</td><td>"+fmt(DATA.rows[i])+"</td><td>"+fmt(c[0][i])+"</td><td>"+pct(c[0][i],DATA.rows[i])+
   "</td><td>"+esc(fmt(c[1][i]))+"</td>"+(hq?[0,1,2,3,4].map(function(k){return "<td>"+(q?esc(fmt(q[k])):"")+"</td>";}).join(""):"")+
   "<td>"+esc(fmt(c[2][i]))+"</td>"+(hp?"<td>"+(i===0?"基準期":dist.psi[i].toFixed(3))+"</td>":"")+"</tr>";});
  var d=document.createElement("tr");d.className="detail";d.innerHTML="<td colspan='"+tr.cells.length+"'>"+h+"</tbody></table></td>";
  tr._detail=d;tr.parentNode.insertBefore(d,tr.nextSibling);tr.classList.add("open");return;}
 var open=!tr.classList.contains("open");tr.classList.toggle("open",open);tr._detail.hidden=!open;}
function esc(s){return String(s).replace(/[&<>"']/g,function(ch){return "&#"+ch.charCodeAt(0)+";";});}
document.addEventListener("click",function(ev){var a=ev.target.closest("a.col");if(!a)return;
 var tr=document.getElementById("col-"+a.getAttribute("data-col"));if(!tr)return;ev.preventDefault();
 var bar=document.querySelector('.toolbar[data-for="t-all"]');
 if(bar){bar.querySelectorAll("input.q").forEach(function(q){q.value="";});bar.querySelectorAll("select").forEach(function(s){s.selectedIndex=0;});apply(bar);}
 if(!tr.classList.contains("open"))toggle(tr);tr.scrollIntoView({block:"center"});});
function tipText(s,i){var name=s.getAttribute("data-col"),c=DATA.cols&&DATA.cols[name],d=(DATA.dist||{})[name],p=DATA.labels[i];if(!c)return p;
 var n=DATA.rows[i],nl=c[0][i],mode=s.getAttribute("data-mode");
 if(mode==="const"){if(nl===n)return p+"　整期都是空值";
  return c[1][i]===c[2][i]?p+"　只有一個值："+fmt(c[1][i]):p+"　"+fmt(c[1][i])+"～"+fmt(c[2][i]);}
 if(mode==="psi")return i===0?p+"　基準期":p+"　PSI "+d.psi[i].toFixed(3);
 if(mode==="q"){var q=d.q[i];if(!q)return p+"　整期都是空值";
  return p+"　p1 "+fmt(q[0])+"、p25 "+fmt(q[1])+"、中位數 "+fmt(q[2])+"、p75 "+fmt(q[3])+"、p99 "+fmt(q[4]);}
 if(mode==="cat"){var sh=d.sh[i],ks=sh.map(function(v,k){return k;}).filter(function(k){return sh[k]>0;});
  ks.sort(function(a,b){return sh[b]-sh[a];});
  return p+"　"+ks.slice(0,6).map(function(k){return d.cats[k]+" "+pctx(sh[k]);}).join("、")+(ks.length>6?"…":"");}
 if(mode==="tail"){if(nl===n)return p+"　整期都是空值";var r=d.tail[i];if(!r)return p+"　p1＝p99，不判斷";
  function x(v,d){return v.toLocaleString("en-US",{minimumFractionDigits:d,maximumFractionDigits:d});}
  return p+"　最大值 "+fmt(c[2][i])+"：倍數 "+x(r[0],1)+"、log 後倍數 "+x(r[1],2)+
   "｜最小值 "+fmt(c[1][i])+"：倍數 "+x(r[2],1)+"、log 後倍數 "+x(r[3],2);}
 if(mode==="near"){if(nl===n)return p+"　整期都是空值";if(c[1][i]===c[2][i])return p+"　只有一個值："+fmt(c[1][i]);
  var b=d.q[i];return b[0]===b[4]?p+"　p1＝p99＝"+fmt(b[0])+"（最小值 "+fmt(c[1][i])+"、最大值 "+fmt(c[2][i])+"）"
   :p+"　有變化：p1 "+fmt(b[0])+"、p99 "+fmt(b[4]);}
 return p+"　空值 "+pct(nl,n)+"（"+fmt(n)+" 筆中 "+fmt(nl)+" 筆）";}
function cmText(m,i){var g=DATA.cm[Number(m.getAttribute("data-cm"))],k=g.names.length,a=Math.floor(i/k),b=i%k;
 if(a===b)return g.names[a];var r=g.r[i];
 return g.names[a]+" 和 "+g.names[b]+"　"+(r===null?"絕對值不到 "+DATA.corrKeep+"，或沒算"
  :"相關係數 "+r.toFixed(3)+"（"+fmt(g.n[i])+" 筆兩欄都有值）");}
var tip=document.createElement("div");tip.className="tip";tip.hidden=true;document.body.appendChild(tip);var lastOn=null;
document.addEventListener("mousemove",function(ev){var s=ev.target.closest(".spark"),cell=ev.target.closest(".cm i");
 if(lastOn&&(!s||lastOn.parentNode!==s)&&lastOn!==cell){lastOn.classList.remove("on");lastOn=null;}
 if(cell){var m=cell.parentNode,i=Array.prototype.indexOf.call(m.children,cell);
  if(lastOn)lastOn.classList.remove("on");lastOn=cell;cell.classList.add("on");tip.textContent=cmText(m,i);tip.hidden=false;
  tip.style.left=Math.min(ev.clientX+12,window.innerWidth-tip.offsetWidth-8)+"px";tip.style.top=(ev.clientY+16)+"px";return;}
 if(!s||s.classList.contains("key")){tip.hidden=true;return;}
 var bs=s.children,r=s.getBoundingClientRect(),i=Math.min(bs.length-1,Math.max(0,Math.floor((ev.clientX-r.left)/r.width*bs.length)));
 if(lastOn)lastOn.classList.remove("on");lastOn=bs[i];lastOn.classList.add("on");
 tip.textContent=tipText(s,i);tip.hidden=false;
 tip.style.left=Math.min(ev.clientX+12,window.innerWidth-tip.offsetWidth-8)+"px";tip.style.top=(ev.clientY+16)+"px";});
bars.forEach(function(bar){try{apply(bar);}catch(e){if(window.console)console.error(e);}});
})();
"""


class _Report:
    """把 profile 與判斷結果整理成單一頁面。每一段一個方法，回傳 HTML。"""

    def __init__(self, profile: dict, thresholds: dict[str, float], threshold_source: str | None):
        self.profile = profile
        self.thresholds = thresholds
        self.threshold_source = threshold_source
        self.null_th = thresholds["null_ratio"]
        self.th_text = _pct_threshold(self.null_th)
        self.periods: list[str] = profile["periods"]
        self.labels = _period_labels(self.periods)
        self.rows: list[int] = profile["rows"]
        self.feats = [c for c in profile["columns"] if c["kind"] != "unchecked"]
        self.unchecked = [c for c in profile["columns"] if c["kind"] == "unchecked"]
        self.flags = judge(profile, thresholds)
        self.actions = {c["name"]: actions(c, self.rows, self.flags[c["name"]], thresholds)
                        for c in self.feats}
        # 有沒有跑分布偏移與極端值；只有基本檢查的舊 profile.json 沒有這個鍵
        self.dist = bool(profile["config"].get("checks", {}).get("distribution"))
        self.psis = {c["name"]: psi_by_period(c) for c in self.feats}
        self.tails = {c["name"]: tail_hits(c, thresholds) for c in self.feats}
        self.near = {c["name"]: near_const_periods(c) for c in self.feats
                     if not set(NO_DIST_FLAGS) & set(self.flags[c["name"]])}
        # 有沒有跑相關性；只有前兩階段的舊 profile.json 沒有這兩個鍵
        self.corr = (bool(profile["config"].get("checks", {}).get("correlation"))
                     and "correlation" in profile)
        self.corr_pairs = high_corr_pairs(profile, thresholds, self.flags)
        self.corr_groups = corr_groups(self.corr_pairs, [c["name"] for c in self.feats])
        self.corr_excluded = corr_excluded(profile, thresholds)
        self.matrices = self.build_matrices()

    # --- 共用 ---------------------------------------------------------------

    def with_flag(self, *keys: str) -> list[dict]:
        return [c for c in self.feats if set(keys) & set(self.flags[c["name"]])]

    def with_action(self, key: str) -> list[dict]:
        return [c for c in self.feats if key in self.actions[c["name"]]]

    def ratios(self, col: dict) -> list[float]:
        return null_ratios(col, self.rows)

    def bad_null(self, col: dict) -> list[int]:
        return [t for t, r in enumerate(self.ratios(col)) if r >= self.null_th]

    def variation(self, col: dict) -> float:
        """各期空值比例最高 − 最低，百分點。"""
        r = self.ratios(col)
        # 浮點數相減會把剛好 1 個百分點算成 0.999…，目錄的欄數就會跟表上的篩選對不上
        return round((max(r) - min(r)) * 100, 9)

    def varying(self) -> list[dict]:
        """〈有變動但沒達門檻〉的候選：空值兩種都沒被標到的欄位。"""
        return [c for c in self.feats if not set(NULL_FLAGS) & set(self.flags[c["name"]])]

    def periods_text(self, ts: list[int]) -> tuple[str, str]:
        return _shorten([self.labels[t] for t in ts], "期")

    @staticmethod
    def span(labels: list[str]) -> str:
        return labels[0] if len(labels) == 1 else f"{labels[0]}～{labels[-1]}"

    def spark_attrs(self, mode: str, col: dict, *, tall: bool = False) -> str:
        """150px 寬、每根至少 1px 加 1px 空隙，最多放 75 期；更多期就拿掉空隙、不設最小寬度，
        寬度照樣固定（超過 150 期時每根不到 1px，看單一期要點欄名展開明細）。"""
        cls = "spark dense" if len(self.periods) > SPARK_GAP_MAX_PERIODS else "spark"
        if tall:  # 分位數、各值占比：沒有空隙，一期接一期看起來才是連續的帶子
            cls = "spark dense tall"
        return f'class="{cls}" data-mode="{mode}" data-col="{_e(col["name"])}"'

    def spark_null(self, col: dict) -> str:
        """各期空值比例的小長條圖：一期一根，高度＝空值比例，達門檻橘色。"""
        bars = "".join(
            f'<i{" class=h" if r >= self.null_th else ""} style="--h:{r * 100:.1f}%"></i>'
            for r in self.ratios(col)
        )
        return f'<span {self.spark_attrs("null", col)}>{bars}</span>'

    def spark_marked(self, mode: str, col: dict, marked: set[int], cls: str) -> str:
        """各期有沒有某種情況：``cls`` 的顏色＝有，灰＝有值但沒有，空白＝整期都是空值。"""
        valued, _ = one_value_periods(col, self.rows)
        cells = []
        for t in range(len(self.periods)):
            if t in marked:
                cells.append(f'<i class="{cls}" style="--h:100%"></i>')
            elif t in valued:
                cells.append('<i class="g" style="--h:100%"></i>')
            else:
                cells.append('<i style="--h:0%"></i>')
        return f'<span {self.spark_attrs(mode, col)}>{"".join(cells)}</span>'

    def spark_const(self, col: dict) -> str:
        """各期是不是只有一個值：橘＝只有一個值，灰＝有變化，空白＝整期都是空值。"""
        return self.spark_marked("const", col, set(one_value_periods(col, self.rows)[1]), "h")

    def psi_cap(self) -> float:
        """PSI 小長條圖的滿格：門檻的兩倍（預設 0.5），門檻剛好在半高。"""
        return 2 * self.thresholds["psi"]

    def spark_psi(self, col: dict) -> str:
        """各期 PSI：一期一根，最左邊是基準期（0），達門檻橘色。"""
        cap, hit = self.psi_cap(), set(drift_periods(col, self.thresholds))
        bars = "".join(
            f'<i{" class=h" if t in hit else ""} style="--h:{min(v, cap) / cap * 100:.1f}%"></i>'
            for t, v in enumerate(self.psis[col["name"]])
        )
        return f'<span {self.spark_attrs("psi", col)}>{bars}</span>'

    def spark_q(self, col: dict) -> str:
        """分位數走勢：一期一根，淺色是 p1～p99、深色是 p25～p75、橫線是中位數。
        每欄自己的刻度，從各期最低的 p1 到最高的 p99。"""
        idx = [QUANTILE_PROBS.index(p) for p in _BAND_PROBS]
        qs = col["quantiles"]
        have = [q for q in qs if q is not None]
        scales = []
        for f in (lambda v: v, _slog):  # 一般刻度、log 刻度（頁面上切換）
            lo, hi = f(min(q[0] for q in have)), f(max(q[-1] for q in have))
            scales.append((f, lo, hi))

        cells = []
        for q in qs:
            if q is None:
                cells.append('<i style="--h:0%"></i>')
                continue
            style = []
            for prefix, (f, lo, hi) in zip(("lin-", "log-"), scales):
                ys = [50.0 if hi == lo else (f(q[i]) - lo) / (hi - lo) * 100 for i in idx]
                style += [f"--{prefix}{k}:{y:.1f}%" for k, y in zip("abmcd", ys)]
            cells.append(f'<i class="q" style="{";".join(style)}"></i>')
        return f'<span {self.spark_attrs("q", col, tall=True)}>{"".join(cells)}</span>'

    def spark_cat(self, col: dict) -> str:
        """字串欄位各值的占比：一期一根，由下往上依基準期的占比排，最上面是其他值、空值。"""
        n_values = len(col["bins"]["values"])
        colors = [_CAT_COLORS[k] if k < len(_CAT_COLORS) else _CAT_REST for k in range(n_values)]
        colors += [_CAT_OTHER, _CAT_NULL]
        cells = []
        for counts in col["bins"]["counts"]:
            acc, stops = 0.0, []
            for k, share in enumerate(_shares(counts)):
                if share:
                    start, acc = acc, acc + share * 100
                    stops.append(f"{colors[k]} {start:.1f}% {acc:.1f}%")
            cells.append(f'<i class="c" style="background:linear-gradient(to top,{",".join(stops)})"></i>')
        return f'<span {self.spark_attrs("cat", col, tall=True)}>{"".join(cells)}</span>'

    def tail_cap(self) -> float:
        """極端值小長條圖的滿格：log 後倍數門檻的兩倍，門檻剛好在半高。"""
        return 2 * self.thresholds["tail_log_ratio"]

    def spark_tail(self, col: dict) -> str:
        """各期兩側較大的 log 後倍數：高度到門檻的兩倍為止，同一側兩個條件都達門檻是橘色。"""
        cap = self.tail_cap()
        hit = {h.period for h in self.tails[col["name"]]}
        cells = []
        for t in range(len(self.periods)):
            r = tail_ratios(col, t)
            h = 0.0 if r is None else max(0.0, *(log for _, log in r.values()))
            cells.append(f'<i{" class=h" if t in hit else ""} style="--h:{min(h, cap) / cap * 100:.1f}%"></i>')
        return f'<span {self.spark_attrs("tail", col)}>{"".join(cells)}</span>'

    def spark_near(self, col: dict) -> str:
        """各期是不是 p1 = p99：深灰＝是（含只有一個值；只供參考，不用問題的橘色），淺灰＝有變化，
        空白＝整期都是空值。"""
        near = set(self.near[col["name"]]) | set(one_value_periods(col, self.rows)[1])
        return self.spark_marked("near", col, near, "d")

    def null_legend(self) -> str:
        return _spark_legend([("", f"沒達 {self.th_text}，越高越空"), ("h", f"≥ {self.th_text}，達門檻")],
                             "一期一根，由左到右是最早到最新；滑鼠移到圖上看那一期的數字")

    # --- 頁首與目錄 ---------------------------------------------------------

    def header(self) -> str:
        cfg = self.profile["config"]
        span = (f"{_e(cfg['time_col'])} {_e(self.span(self.labels))}，共 {len(self.periods)} 期"
                if self.periods else "沒有讀到任何一期")
        per = f"每期 {min(self.rows):,}～{max(self.rows):,} 筆" if self.rows else ""
        where = f"篩選：<code>{_e(cfg['where'])}</code>" if cfg["where"] else "篩選：無"
        made = _e(self.profile["generated_at"].replace("T", " ")[:16])
        return (
            '<header class="top">'
            f'<div class="title">特徵表檢查報告 <code>{_e(cfg["table"])}</code></div>'
            f'<div class="scope">{span}｜{per}｜{where}｜產出 {made}</div>'
            '<input id="global-q" type="search" placeholder="篩選欄名（套用到每一張表）">'
            "</header>"
        )

    def n_varying_default(self) -> int:
        return sum(1 for c in self.varying() if self.variation(c) >= _VAR_DEFAULT)

    def toc(self) -> str:
        items = [
            ("s-overview", "總覽", None, False),
            ("s-null", "空值", len(self.with_flag(*NULL_FLAGS)), False),
            ("s-null-all", FLAG_NAMES["null_all"], len(self.with_flag("null_all")), True),
            ("s-null-some", FLAG_NAMES["null_some"], len(self.with_flag("null_some")), True),
            ("s-null-var", "有變動但沒達門檻", self.n_varying_default(), True),
            ("s-const", "只有一個值", len(self.with_flag(*CONST_FLAGS)), False),
            ("s-const-all", FLAG_NAMES["const_all"], len(self.with_flag("const_all")), True),
            ("s-const-some", FLAG_NAMES["const_some"], len(self.with_flag("const_some")), True),
            ("s-const-each", FLAG_NAMES["const_each"], len(self.with_flag("const_each")), True),
            ("s-const-near", "值幾乎都相同", self.n_near() if self.dist else None, True),
            ("s-drift", "分布偏移", len(self.with_flag("drift")) if self.dist else None, False),
            *([("s-drift-num", "數值欄位", len(self.drift_cols("numeric")), True),
               ("s-drift-str", "字串欄位", len(self.drift_cols("string")), True)]
              if self.dist and len(self.periods) > 1 else []),
            ("s-tail", "極端值", len(self.with_flag("tail")) if self.dist else None, False),
            ("s-corr", "相關性", len(self.with_flag("corr")) if self.corr else None, False),
            *([("s-corr-high", "高度相關的欄位", len(self.with_flag("corr")), True),
               ("s-corr-matrix", "相關係數矩陣", len(self.matrices), True),
               ("s-corr-skip", "未計算的欄位", len(self.corr_skipped()), True)]
              if self.corr else []),
            ("s-all", "全部欄位", len(self.feats), False),
            None,
            ("s-actions", "行動建議總結", None, False),
            ("s-scope", "資料範圍與判斷標準", None, False),
        ]
        out = []
        for it in items:
            if it is None:
                out.append("<hr>")
                continue
            sid, label, n, sub = it
            count = f"<span>{n}</span>" if n is not None else ""
            out.append(f'<a href="#{sid}"{" class=sub" if sub else ""}>{_e(label)}{count}</a>')
        return f'<nav class="toc">{"".join(out)}</nav>'

    # --- 總覽 ---------------------------------------------------------------

    def overview(self) -> str:
        n_num = sum(c["kind"] == "numeric" for c in self.feats)
        n_null = len(self.with_flag(*NULL_FLAGS))
        n_const = len(self.with_flag(*CONST_FLAGS))
        ok = sum(1 for c in self.feats if not self.flags[c["name"]])
        null_detail = (f"{FLAG_NAMES['null_all']} {len(self.with_flag('null_all'))}、"
                       f"{FLAG_NAMES['null_some']} {len(self.with_flag('null_some'))}；"
                       f"另有 {self.n_varying_default()} 欄有變動但沒達門檻，只供參考")
        const_detail = "、".join(f"{FLAG_NAMES[k]} {len(self.with_flag(k))}" for k in CONST_FLAGS)
        if self.dist:
            const_detail += f"；另有 {self.n_near()} 欄值幾乎都相同，只供參考"
        n_drift, n_tail = len(self.with_flag("drift")), len(self.with_flag("tail"))
        n_corr = len(self.with_flag("corr"))
        sides = [worst_tail(c, self.thresholds).side for c in self.with_flag("tail")]
        n_by_null = sum(1 for c in self.with_flag("drift") if drift_source(c, self.thresholds) == "null")
        dist_rows = [
            ("s-drift", "分布偏移", n_drift,
             f"數值欄位 {len(self.drift_cols('numeric'))}、字串欄位 {len(self.drift_cols('string'))}；"
             f"其中 {n_by_null} 欄的變化主要來自空值比例"),
            ("s-tail", "極端值", n_tail,
             f"最大值那一側 {sides.count('max')}、最小值那一側 {sides.count('min')}"),
        ]
        corr_row = None
        if self.corr:
            reasons = [why for _, why, _ in self.corr_skipped()]
            corr_row = f"{len(self.corr_groups)} 組" + (
                f"，最大的一組 {len(self.corr_groups[0])} 欄" if self.corr_groups else "")
            if reasons:
                corr_row += f"；另有 {len(reasons)} 欄列在〈未計算的欄位〉：" + "、".join(
                    f"{_CORR_SKIP_NAMES[k]} {reasons.count(k)}" for k in CORR_REASONS if k in reasons)
        rows = [
            ({}, [_td('<a href="#s-null">空值</a>'), _td(str(n_null), v=n_null, num=True),
                  _td(_e(null_detail), cls="wrap")]),
            ({}, [_td('<a href="#s-const">只有一個值</a>'), _td(str(n_const), v=n_const, num=True),
                  _td(_e(const_detail), cls="wrap")]),
            *(({}, [_td(f'<a href="#{sid}">{label}</a>'),
                    _td(str(n), v=n, num=True) if self.dist else _td("—"),
                    _td(_e(detail if self.dist else "這次沒有跑"), cls="wrap")])
              for sid, label, n, detail in dist_rows),
            ({}, [_td('<a href="#s-corr">相關性</a>'),
                  _td(str(n_corr), v=n_corr, num=True) if self.corr else _td("—"),
                  _td(_e(corr_row or "這次沒有跑"), cls="wrap")]),
            ({}, [_td('<a href="#s-all">沒有發現問題</a>'), _td(str(ok), v=ok, num=True), _td("—")]),
            ({}, [_td('<a href="#s-scope">未檢查</a>'),
                  _td(str(len(self.unchecked)), v=len(self.unchecked), num=True),
                  _td("型別不支援，例如 date、timestamp", cls="wrap")]),
        ]
        return _section("s-overview", "總覽", (
            _p(f"這份報告檢查 <code>{_e(self.profile['config']['table'])}</code> 的 {len(self.feats)} 個特徵欄位"
               f"（數值 {n_num} 欄、字串 {len(self.feats) - n_num} 欄），每一期看"
               + ("四件事：空值多不多、是不是只有一個值、分布和最早一期差多少、有沒有離譜的最大值或最小值。"
                  if self.dist else "兩件事：空值多不多、是不是只有一個值。")
               + ("另外在最新一期抽樣，看哪幾欄幾乎是同一個資訊。" if self.corr else "")
               + "下面依檢視項目分段，最後的〈行動建議總結〉把結果整理成可以照做的清單。"
               "欄名點了會跳到〈全部欄位〉，展開看各期數字。")
            + _table("t-overview", [("檢視項目", False), ("有事的欄位", True), ("細項", False)], rows)
            + _note("同一欄可能同時有兩種問題（例如某一期幾乎全空、又有幾期只有一個值），兩段都會列出。")
        ))

    # --- 空值 ---------------------------------------------------------------

    def nulls(self) -> str:
        return _section("s-null", "空值", (
            _p("空值＝NULL、float／double 的 NaN 與 ±Infinity、字串的空字串。"
               "空值比例＝那一期的空值筆數 ÷ 那一期的總筆數。")
            + self.null_legend() + self.null_all() + self.null_some() + self.null_var()
        ))

    def null_all(self) -> str:
        cols = sorted(self.with_flag("null_all"), key=lambda c: (-min(self.ratios(c)), c["name"]))
        rows = []
        for c in cols:
            r = self.ratios(c)
            valued = [n - nl for n, nl in zip(self.rows, c["null"])]
            rows.append(({"data-name": c["name"]}, [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_null(c), v=min(r)),
                _td(_e(_pct_range(r)), v=min(r)),
                _td(_e(f"{min(valued):,}～{max(valued):,}"), v=min(valued), num=True),
            ]))
        body = _p(f"每一期的空值比例都 ≥ {self.th_text}。")
        if rows:
            body += _toolbar("t-null-all") + _table(
                "t-null-all", [("欄位", False), ("型別", False), ("各期", False), ("空值比例", False),
                               ("每期有值的筆數", True)], rows, sort=(3, "desc"))
        else:
            body += _none()
        return _section("s-null-all", f"{FLAG_NAMES['null_all']}（{len(rows)} 欄）", body, level=3)

    def null_some(self) -> str:
        order = []
        bad_periods: set[int] = set()
        for c in self.with_flag("null_some"):
            r = self.ratios(c)
            bad = self.bad_null(c)
            bad_periods.update(bad)
            rest = [v for t, v in enumerate(r) if t not in bad]
            shown, full = self.periods_text(bad)
            jump = null_jump(c, self.rows, self.thresholds) * 100
            order.append(((-jump, bad[0], c["name"]), (
                {"data-name": c["name"], "data-bad": " ".join(f"p{t}" for t in bad)}, [
                    _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_null(c), v=bad[0]),
                    _td(_e(shown), v=bad[0], title=full if shown != full else ""),
                    _td(_e(f"其他 {len(rest)} 期：{_pct_range(rest)}"), v=max(rest)),
                    _td_num(round(jump, 1), f"{jump:.1f}"),
                ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        body = _p(f"只有部分期別的空值比例 ≥ {self.th_text}。跳幅大的，是那幾期突然空掉，通常是上游沒進資料；"
                  f"跳幅小的，是本來就很空、偶爾越過 {self.th_text}。預設跳幅大的排前面；"
                  "想看某一期斷了哪些欄，用「出事的期別」篩。")
        if rows:
            options = [("", "全部")] + [(f"p{t}", self.labels[t]) for t in sorted(bad_periods)]
            body += (
                _toolbar("t-null-some", selects=(("bad", "出事的期別", options, "", "token"),))
                + _table("t-null-some", [("欄位", False), ("型別", False), ("各期", False),
                                         ("出事的期別", False), ("其他期別", False), ("跳幅（百分點）", True)],
                         rows, sort=(5, "desc"))
                + _note("跳幅＝出事那幾期最低的空值比例 − 其他期別最高的空值比例。"
                        "例如出事那期 100%、其他期別最高 4.2%，跳幅就是 95.8。")
            )
        else:
            body += _none()
        return _section("s-null-some", f"{FLAG_NAMES['null_some']}（{len(rows)} 欄）", body, level=3)

    def null_var(self) -> str:
        order = []
        for c in self.varying():
            r = self.ratios(c)
            var = self.variation(c)
            order.append(((-var, c["name"]), ({"data-name": c["name"], "data-var": round(var, 3)}, [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_null(c), v=round(var, 3)),
                _td(_e(_pct_range(r)), v=max(r)), _td_num(round(var, 1), f"{var:.1f}"),
            ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        options = [(v, f"{v:g} 個百分點") for v in _VAR_CHOICES]
        body = _p("上面兩段沒列到的欄位裡，各期空值比例有變動的，只供參考，不算問題、不進總結。"
                  f"某一期突然變多、但沒到 {self.th_text}，規則抓不到，可能是那一期少了一部分資料；"
                  "也可能是慢慢變多。")
        if rows:
            body += (
                _toolbar("t-null-var", selects=(("var", "只看變動幅度 ≥", options, _VAR_DEFAULT, "min"),))
                + _table("t-null-var", [("欄位", False), ("型別", False), ("各期", False), ("空值比例", False),
                                        ("變動幅度（百分點）", True)], rows, sort=(4, "desc"))
                + _note("變動幅度＝各期空值比例最高 − 最低。選「0 個百分點」會列出上面兩段以外的所有欄位。")
            )
        else:
            body += _none()
        return _section("s-null-var", "有變動但沒達門檻", body, level=3)

    # --- 只有一個值 ---------------------------------------------------------

    def consts(self) -> str:
        return _section("s-const", "只有一個值", (
            _p("只有一個值＝扣掉空值之後，那一期的最小值等於最大值。整期都是空值的那一期不算。")
            + self.const_all() + self.const_some() + self.const_each() + self.const_near()
        ))

    def const_all(self) -> str:
        rows = []
        for c in sorted(self.with_flag("const_all"), key=lambda c: c["name"]):
            _, const = one_value_periods(c, self.rows)
            value = c["min"][const[0]]
            rows.append(({"data-name": c["name"]}, [
                _td_name(c["name"]), _td(_e(c["dtype"])),
                _td_num(value) if isinstance(value, (int, float)) else _td(_e(value)),
                _td(_e(_pct_range(self.ratios(c)))),
            ]))
        body = _p("所有期別合計只有一個值。")
        if rows:
            body += _toolbar("t-const-all") + _table(
                "t-const-all", [("欄位", False), ("型別", False), ("值", False), ("空值比例", False)],
                rows, sort=(0, "asc"))
        else:
            body += _none()
        return _section("s-const-all", f"{FLAG_NAMES['const_all']}（{len(rows)} 欄）", body, level=3)

    def const_some(self) -> str:
        order = []
        for c in self.with_flag("const_some"):
            valued, const = one_value_periods(c, self.rows)
            others = [t for t in valued if t not in const]
            lo = min(c["min"][t] for t in others)
            hi = max(c["max"][t] for t in others)
            shown, full = self.periods_text(const)
            values = "、".join(dict.fromkeys(_fmt_num(c["min"][t]) for t in const))
            order.append(((const[0], c["name"]), (
                {"data-name": c["name"]}, [
                    _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_const(c), v=const[0]),
                    _td(_e(shown), v=const[0], title=full if shown != full else ""), _td(_e(values)),
                    _td(_e(f"{_fmt_num(lo)}～{_fmt_num(hi)}")),
                ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        body = (_p("平常有變化，只有某幾期全部是同一個值，通常是那幾期上游填了預設值。")
                + _spark_legend([("g", "有變化"), ("h", "只有一個值"), ("empty", "整期都是空值")]))
        if rows:
            body += _toolbar("t-const-some") + _table(
                "t-const-some", [("欄位", False), ("型別", False), ("各期", False), ("只有一個值的期別", False),
                                 ("那幾期的值", False), ("其他期別的範圍", False)], rows, sort=(3, "asc"))
        else:
            body += _none()
        return _section("s-const-some", f"{FLAG_NAMES['const_some']}（{len(rows)} 欄）", body, level=3)

    def const_each(self) -> str:
        rows = []
        for c in sorted(self.with_flag("const_each"), key=lambda c: c["name"]):
            _, const = one_value_periods(c, self.rows)
            values = [c["min"][t] for t in const]
            numeric = c["kind"] == "numeric"
            rows.append(({"data-name": c["name"]}, [
                _td_name(c["name"]), _td(_e(c["dtype"])),
                _td_num(values[0]) if numeric else _td(_e(values[0])),
                _td_num(values[-1]) if numeric else _td(_e(values[-1])),
                _td_num(len(set(values))),
            ]))
        body = _p("同一期裡每一筆的值都相同，但各期不同。通常是匯率、利率這類每期一個數字的總體指標。")
        if rows:
            body += _toolbar("t-const-each") + _table(
                "t-const-each", [("欄位", False), ("型別", False), ("最早一期的值", True),
                                 ("最新一期的值", True), ("不同的值個數", True)], rows, sort=(0, "asc"))
        else:
            body += _none()
        return _section("s-const-each", f"{FLAG_NAMES['const_each']}（{len(rows)} 欄）", body, level=3)

    def n_near(self) -> int:
        return sum(1 for ts in self.near.values() if ts)

    def const_near(self) -> str:
        title = "值幾乎都相同"
        if not self.dist:
            return _section("s-const-near", title, _not_run(), level=3)
        order = []
        for c in self.feats:
            ts = self.near.get(c["name"])
            if not ts:
                continue
            shown, full = self.periods_text(ts)
            values = "、".join(dict.fromkeys(_fmt_num(c["quantiles"][t][0]) for t in ts))
            order.append(((-len(ts), c["name"]), ({"data-name": c["name"]}, [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_near(c), v=len(ts)),
                _td(_e(shown), v=ts[0], title=full if shown != full else ""), _td(_e(values)),
                _td_num(len(ts)),
            ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        body = (_p("p1＝p99：那一期至少約 98% 的值相同，只有少數幾筆不一樣，例如很少發生的事件旗標。"
                   "只供參考，不算問題、不進總結；這幾期也不判斷極端值。")
                + _spark_legend([("g", "有變化"), ("d", "p1＝p99"), ("empty", "整期都是空值")]))
        if rows:
            body += _toolbar("t-const-near") + _table(
                "t-const-near", [("欄位", False), ("型別", False), ("各期", False), ("p1＝p99 的期別", False),
                                 ("多數的值", False), ("期數", True)], rows, sort=(5, "desc"))
        else:
            body += _none()
        return _section("s-const-near", f"{title}（{len(rows)} 欄）", body, level=3)

    # --- 分布偏移 -----------------------------------------------------------

    def drift_cols(self, kind: str) -> list[dict]:
        return [c for c in self.with_flag("drift") if c["kind"] == kind]

    def top_psi(self, col: dict) -> int:
        psi = self.psis[col["name"]]
        return max(range(len(psi)), key=lambda t: psi[t])

    def top_change(self, col: dict) -> str:
        """字串欄位：PSI 最大的那一期，和基準期比占比差最多的值（不含空值）。"""
        counts, t = col["bins"]["counts"], self.top_psi(col)
        base, cur = _shares(counts[0]), _shares(counts[t])
        names = col["bins"]["values"] + ["其他值"]
        k = max(range(len(names)), key=lambda i: abs(cur[i] - base[i]))
        return f"{names[k]}：{_pct(base[k])} 變 {_pct(cur[k])}"

    def drift(self) -> str:
        if not self.dist:
            return _section("s-drift", "分布偏移", _not_run())
        th = self.thresholds["psi"]
        if len(self.periods) < 2:
            return _section("s-drift", "分布偏移", '<p class="none">只讀到一期，沒有可以比的期別。</p>')
        base_empty = [c["name"] for c in self.feats
                      if self.psis[c["name"]] is None and not set(NO_DIST_FLAGS) & set(self.flags[c["name"]])]
        intro = _p(f"基準期是這次讀到的最早一期，也就是 {_e(self.labels[0])}。每一期都和基準期比，"
                   f"分布差多少用 PSI（Population Stability Index）表示：0 是一模一樣，越大差越多，"
                   f"≥ {th:g} 算明顯不同。數值欄位用基準期的十分位數切成 10 組，"
                   f"字串欄位取基準期最常見的 {PSI_TOP_VALUES} 個值各成一組、其他值合成一組，空值都另成一組；"
                   "PSI 比的是每一組的占比。")
        legend = _spark_legend([("", f"沒達 {th:g}"), ("h", f"≥ {th:g}，達門檻")],
                               f"一期一根，最左邊是基準期；高度到 PSI {self.psi_cap():g} 為止")
        notes = _note("變化主要來自＝PSI 最大的那一期，扣掉空值、只比有值的那幾組再算一次 PSI：沒達門檻是"
                      "「空值比例」，那幾期空值變多或變少，〈空值〉那一段也看得到；達門檻是「值的分布」。"
                      "兩張表預設「值的分布」排前面，同一種再依最大 PSI 排。")
        notes += _note(f"沒有比較：{'、'.join(FLAG_NAMES[k] for k in NO_DIST_FLAGS)}的欄位"
                       + (f"，以及基準期整期都是空值的 {len(base_empty)} 欄（{_e('、'.join(base_empty))}）"
                          if base_empty else "") + "。")
        return _section("s-drift", "分布偏移", intro + legend + notes
                        + self.drift_table("numeric") + self.drift_table("string"))

    def drift_table(self, kind: str) -> str:
        sid, tid = ("s-drift-num", "t-drift-num") if kind == "numeric" else ("s-drift-str", "t-drift-str")
        order, hit_periods = [], set()
        for c in self.drift_cols(kind):
            psi, ts, top = self.psis[c["name"]], drift_periods(c, self.thresholds), self.top_psi(c)
            hit_periods.update(ts)
            shown, full = self.periods_text(ts)
            src = drift_source(c, self.thresholds)
            cells = [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_psi(c), v=ts[0]),
                _td(_e(shown), v=ts[0], title=full if shown != full else "", cls="brk"),
                _td(_e(self.labels[top]), v=top), _td_num(round(psi[top], 3), f"{psi[top]:.3f}"),
                _td("空值比例" if src == "null" else "值的分布", v=int(src == "null")),
            ]
            if kind == "numeric":
                cells.append(_td(self.spark_q(c)))
            else:
                cells += [_td(self.spark_cat(c)), _td(_e(self.top_change(c)), cls="brk")]
            attrs = {"data-name": c["name"], "data-drift": " ".join(f"p{t}" for t in ts), "data-src": src}
            order.append(((src == "null", -psi[top], c["name"]), (attrs, cells)))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        head = [("欄位", False), ("型別", False), ("各期 PSI", False), ("PSI 達門檻的期別", False),
                ("PSI 最大的期別", False), ("最大 PSI", True), ("變化主要來自", False)]
        if kind == "numeric":
            title = "數值欄位"
            body = (_p("PSI 只說差多少，不說怎麼變，要看分位數走勢：整條突然跳好幾倍，通常是換了單位或計算方式；"
                       "慢慢移動，通常是客群在變。")
                    + '<div class="legend"><span><span class="qkey o"></span>p1～p99</span>'
                    '<span><span class="qkey i"></span>p25～p75</span><span><span class="qkey m"></span>中位數</span>'
                    "<span>每欄自己的刻度，從各期最低的 p1 到最高的 p99；滑鼠移到圖上看那一期的數字</span>"
                    '<span>刻度 <button type="button" data-scale="log" class="on">log</button>'
                    '<button type="button" data-scale="lin">一般</button></span></div>')
            head.append(("分位數走勢", False))
            note = ("log 刻度＝每個數先換成 sign(x) × ln(1 + |x|) 再畫，負數與 0 也能畫。金額這類長尾欄位用一般刻度時，"
                    "p99 會把中間壓扁；log 刻度看得到整條分布，整條跳好幾倍時是一段落差。")
        else:
            title = "字串欄位"
            swatches = "".join(f'<span class="qkey" style="background:{c}"></span>' for c in _CAT_COLORS)
            body = (_p("字串欄位看各值的占比怎麼變。")
                    + f'<div class="legend"><span>{swatches}基準期占比第 1～{len(_CAT_COLORS)} 名</span>'
                    f'<span><span class="qkey" style="background:{_CAT_REST}"></span>第 {len(_CAT_COLORS) + 1} 名以後</span>'
                    f'<span><span class="qkey" style="background:{_CAT_OTHER}"></span>其他值</span>'
                    f'<span><span class="qkey" style="background:{_CAT_NULL}"></span>空值</span>'
                    "<span>由下往上依基準期的占比排；滑鼠移到圖上看那一期各值的占比</span></div>")
            head += [("各值占比", False), ("占比變最多的值", False)]
            note = "占比變最多的值＝PSI 最大的那一期和基準期比，占比差最多的值。"
        if rows:
            options = [("", "全部")] + [(f"p{t}", self.labels[t]) for t in sorted(hit_periods)]
            src_opt = [("", "全部"), ("value", "值的分布"), ("null", "空值比例")]
            body += (
                _toolbar(tid, selects=(("drift", "PSI 達門檻的期別", options, "", "token"),
                                       ("src", "變化主要來自", src_opt, "", "token")))
                + _table(tid, head, rows, sort=(6, "asc")) + _note(note)
            )
        else:
            body += _none()
        return _section(sid, f"{title}（{len(rows)} 欄）", body, level=3)

    # --- 極端值 -------------------------------------------------------------

    def tail(self) -> str:
        if not self.dist:
            return _section("s-tail", "極端值", _not_run())
        th = self.thresholds
        order = []
        for c in self.with_flag("tail"):
            t, side, raw, log = worst_tail(c, self.thresholds)
            q = c["quantiles"][t]
            value = c["max"][t] if side == "max" else c["min"][t]
            n_hit = len({h.period for h in self.tails[c["name"]]})
            order.append(((n_hit, -log, c["name"]), ({"data-name": c["name"], "data-side": side}, [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.spark_tail(c), v=n_hit), _td_num(n_hit),
                _td(_e(f"{self.labels[t]} {'最大值' if side == 'max' else '最小值'}"), v=t, cls="brk"),
                # 表太寬時在「～」後面換行（<wbr> 不會被複製進去）
                _td_num(value), _td(f"{_e(_fmt_num(q[0]))}～<wbr>{_e(_fmt_num(q[-1]))}", v=q[-1], num=True, cls="brk"),
                _td_num(round(raw, 1), f"{raw:,.1f}"), _td_num(round(log, 2), f"{log:.2f}"),
            ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        r, lr = _fmt_num(th["tail_ratio"]), _fmt_num(th["tail_log_ratio"])
        body = (
            _p("看每一期的最大值、最小值離中間 98% 的值（p1～p99）有多遠。只看距離，金額這類天生長尾的欄位"
               f"幾乎都會被標，所以同一期、同一側要兩個條件都成立才標：倍數 ≥ {r}，確認真的離很遠；"
               f"而且 log 後倍數 ≥ {lr}，取 log 之後還是很遠，排除正常的長尾。p1＝p99 的那一期不判斷。")
            + "<pre>最小值 ···· p1 ═══════ p99 ····················· 最大值\n"
              "              └─寬度─┘    └─────── 距離 ───────┘\n"
              "倍數＝距離 ÷ 寬度；log 後倍數＝四個數先換成 sign(x) × ln(1 + |x|)，再用同樣的公式</pre>"
            + _spark_legend([("", "沒達門檻"), ("h", "同一側兩個條件都達門檻")],
                            f"一期一根；高度是兩側較大的 log 後倍數，到 {_fmt_num(self.tail_cap())} 為止；"
                            "空白是 p1＝p99 或整期都是空值，不判斷")
        )
        if rows:
            side_opt = [("", "全部"), ("max", "最大值"), ("min", "最小值")]
            body += (
                _toolbar("t-tail", selects=(("side", "哪一側", side_opt, "", "token"),))
                + _table("t-tail", [("欄位", False), ("型別", False), ("各期", False), ("標到的期數", True),
                                    ("最離譜的那一期", False), ("那個值", True), ("p1～p99", True),
                                    ("倍數", True), ("log 後倍數", True)], rows, sort=(3, "asc"))
                + _note("最離譜的那一期＝標到的期別裡，log 後倍數最大的那一期、那一側。只有一兩期被標的，"
                        "比較像那幾期的資料錯了；大部分期別都被標的，比較像欄位本來就是長尾。"
                        "預設標到的期數少的排前面。")
            )
        else:
            body += _none()
        return _section("s-tail", f"極端值（{len(rows)} 欄）", body)

    # --- 相關性 -------------------------------------------------------------

    def corr_partners(self) -> dict[str, list[tuple[str, float]]]:
        """欄 → 跟它高度相關的（欄、相關係數），絕對值大的在前。"""
        out: dict[str, list[tuple[str, float]]] = {}
        for a, b, r, _ in self.corr_pairs:
            out.setdefault(a, []).append((b, r))
            out.setdefault(b, []).append((a, r))
        return {k: sorted(v, key=lambda x: (-abs(x[1]), x[0])) for k, v in out.items()}

    def corr_section(self) -> str:
        if not self.corr:
            return _section("s-corr", "相關性", _not_run("correlation"))
        info, th = self.profile["correlation"], self.thresholds["corr"]
        if info["columns"]:
            took = (f"全部 {info['rows']:,} 筆都拿" if info["rows"] == info["period_rows"]
                    else f"抽 {info['rows']:,} 筆，該期 {info['period_rows']:,} 筆")
            intro = _p(f"在最新一期，也就是 {_e(self.labels[-1])}，{took}，數值欄位兩兩算 Spearman 相關係數。"
                       "它看兩欄的大小順序有多一致：1 是一欄越大、另一欄也越大，順序完全一樣；−1 是順序完全相反；"
                       "0 是看不出關係。只看順序、不看數值本身，所以同一個金額換成千元或取 log，和原欄還是 1。"
                       f"絕對值 ≥ {th:g} 算高度相關，兩欄幾乎是同一個資訊。")
        else:
            intro = _p(f"最新一期，也就是 {_e(self.labels[-1])}，沒有可以算相關係數的數值欄位，"
                       "原因見〈未計算的欄位〉。")
        note = _note(f"每一對欄位只用兩欄都有值的筆數；少於 {CORR_MIN_ROWS:,} 筆、或其中一欄在這些筆裡沒有變化，就沒算。")
        return _section("s-corr", "相關性", intro + note + self.corr_high() + self.corr_matrix() + self.corr_skip())

    def corr_high(self) -> str:
        th = self.thresholds["corr"]
        by_name = {c["name"]: c for c in self.feats}
        partners = self.corr_partners()
        rows = []
        for g, names in enumerate(self.corr_groups):
            for name in names:
                c, ps = by_name[name], partners[name]
                shown, full = _shorten([p for p, _ in ps], "欄")
                top = ps[0][1]
                latest = self.ratios(c)[-1]
                others = [FLAG_NAMES[k] for k in self.flags[name] if k != "corr"]
                rows.append(({"data-name": name, "data-grp": f"g{g}"}, [
                    _td(f"第 {g + 1} 組", v=g), _td_name(name), _td(_e(c["dtype"])),
                    _td(_e(shown), title=full if shown != full else "", cls="brk"),
                    _td(_e(f"{top:.3f}"), v=round(abs(top), 6), num=True),
                    _td(_e(_pct(latest)), v=latest, num=True),
                    _td(_e("、".join(others) or "—"), cls="wrap"),
                ]))
        body = _p(f"相關係數絕對值 ≥ {th:g} 的欄位。A 像 B、B 像 C 時，三欄算同一組，所以同一組裡也可能有兩欄"
                  f"沒到 {th:g}，下面的矩陣看得到。預設大的組排前面；想看某一組，用「組別」篩，再複製欄名。")
        if rows:
            options = [("", "全部")] + [(f"g{g}", f"第 {g + 1} 組（{len(n)} 欄）")
                                        for g, n in enumerate(self.corr_groups)]
            body += (
                _toolbar("t-corr-high", selects=(("grp", "組別", options, "", "token"),))
                + _table("t-corr-high", [("組別", True), ("欄位", False), ("型別", False),
                                         ("跟它高度相關的欄位", False), ("最高相關係數", True),
                                         ("最新一期空值比例", True), ("其他問題", False)], rows, sort=(0, "asc"))
                + _note("跟它高度相關的欄位依相關係數的絕對值由大到小排。相關係數是負的，表示一欄越大、另一欄越小，"
                        "資訊一樣多。其他問題是這一欄在前面各段被標到的問題。")
            )
        else:
            body += _none()
        return _section("s-corr-high", f"高度相關的欄位（{len(rows)} 欄，{len(self.corr_groups)} 組）", body,
                        level=3)

    def build_matrices(self) -> list[tuple[int, dict]]:
        """要畫的矩陣（組的序號、每一格的資料）：三欄以上的組，太大的取前 ``CORR_MATRIX_MAX_COLS`` 欄。

        每一格的相關係數與兩欄都有值的筆數一列接一列攤平；沒存的配對（絕對值不到 0.5 或沒算）是 None。
        """
        if not self.corr:
            return []
        index = {}
        for a, b, v, cnt in self.profile["correlation"]["pairs"]:
            index[(a, b)] = index[(b, a)] = (v, cnt)
        out = []
        for g, names in enumerate(self.corr_groups):
            if len(names) < 3:
                continue
            names = names[:CORR_MATRIX_MAX_COLS]
            cells = [index.get((a, b)) for a in names for b in names]
            out.append((g, {"names": names, "r": [c and c[0] for c in cells], "n": [c and c[1] for c in cells]}))
        return out

    def cm_bands(self) -> list[tuple[str, str]]:
        """矩陣格子的顏色與圖例：（class、說明）。門檻 ≤ 0.8 時沒有「0.8～門檻」那一段。"""
        th = self.thresholds["corr"]
        mid = min(_CM_MID, th)
        bands = [("h", f"≥ {th:g}，達門檻")]
        if mid < th:
            bands.append(("b", f"{mid:g}～{th:g}"))
        if CORR_KEEP < mid:
            bands.append(("a", f"{CORR_KEEP:g}～{mid:g}"))
        return bands + [("", f"不到 {CORR_KEEP:g}，或沒算"), ("d", "自己")]

    def matrix(self, m: int, g: int, data: dict) -> str:
        th, names = self.thresholds["corr"], data["names"]
        mid, k = min(_CM_MID, th), len(names)
        cells = []
        for i, v in enumerate(data["r"]):
            if i // k == i % k:
                cls = "d"
            elif v is None:
                cls = ""
            elif round(abs(v), 9) >= th:
                cls = "h"
            else:
                cls = "b" if abs(v) >= mid else "a"
            cells.append(f'<i class="{cls}"></i>' if cls else "<i></i>")
        total = len(self.corr_groups[g])
        cap = f"第 {g + 1} 組（{total} 欄" + (f"，只畫前 {k} 欄）" if total > k else "）")
        row_labels = "".join(f"<span><b>{i + 1}</b>{_e(n)}</span>" for i, n in enumerate(names))
        col_labels = "".join(f"<span>{i + 1}</span>" for i in range(k))
        return (f'<div class="cmx" style="--k:{k}"><div class="cap">{_e(cap)}</div><div></div>'
                f'<div class="cl">{col_labels}</div><div class="rl name">{row_labels}</div>'
                f'<div class="cm" data-cm="{m}">{"".join(cells)}</div></div>')

    def corr_matrix(self) -> str:
        swatch = {"h": "var(--hit)", "b": "var(--bar)", "a": "var(--qo)", "": "var(--track)", "d": "var(--fg)"}
        body = (_p("三欄以上的組各畫一張，看整組是不是彼此都很像：整片橘色，是每兩欄都幾乎一樣；有些格子不是橘色，"
                   "是一欄接一欄地像（A 像 B、B 像 C），A 和 C 差得比較多。兩欄一組的，上表就看得到。")
                + '<div class="legend">'
                + "".join(f'<span><span class="qkey cell" style="background:{swatch[c]}"></span>{_e(t)}</span>'
                          for c, t in self.cm_bands())
                + "<span>顏色看絕對值；滑鼠移到格子上看兩欄的名字、相關係數與兩欄都有值的筆數</span></div>")
        if self.matrices:
            body += ('<div class="cms">' + "".join(self.matrix(m, g, data) for m, (g, data) in enumerate(self.matrices))
                     + "</div>")
            if any(len(self.corr_groups[g]) > len(data["names"]) for g, data in self.matrices):
                body += _note(f"一張最多畫 {CORR_MATRIX_MAX_COLS} 欄，超過的依表裡的欄位順序取前面的。")
        else:
            body += _none()
        return _section("s-corr-matrix", f"相關係數矩陣（{len(self.matrices)} 組）", body, level=3)

    def corr_skipped(self) -> list[tuple[dict, str, str]]:
        """〈未計算的欄位〉：（欄、原因代號、原因），依表裡的欄位順序。"""
        info = self.profile["correlation"]
        out = []
        for c in self.feats:
            why = self.corr_excluded.get(c["name"])
            if why == "string":
                text = "字串欄位，沒有大小順序"
            elif why == "empty":
                text = "最新一期整期都是空值"
            elif why == "null":
                text = f"最新一期幾乎都是空值（{_pct(self.ratios(c)[-1])}）"
            elif why == "const":
                text = f"最新一期只有一個值：{_fmt_num(c['min'][-1])}"
            elif why == "sparse":
                text = (f"抽到的 {info['rows']:,} 筆裡只有 {info['sparse'][c['name']]:,} 筆有值，"
                        f"不到 {CORR_MIN_ROWS:,} 筆")
            elif why == "flat":
                text = f"抽到的 {info['rows']:,} 筆裡只有一個值；整期不只一個值，只是很少見"
            else:
                continue
            out.append((c, why, text))
        return out

    def corr_skip(self) -> str:
        info = self.profile["correlation"]
        items = self.corr_skipped()
        rows = [({"data-name": c["name"], "data-why": why},
                 [_td_name(c["name"]), _td(_e(c["dtype"])), _td(_e(text), cls="wrap")]) for c, why, text in items]
        body = _p("這些欄位和任何一欄都沒有相關係數，或這次不列進高度相關。")
        if rows:
            reasons = {why for _, why, _ in items}
            why_opt = [("", "全部")] + [(k, _CORR_SKIP_NAMES[k]) for k in CORR_REASONS if k in reasons]
            body += (_toolbar("t-corr-skip", selects=(("why", "原因", why_opt, "", "token"),))
                     + _table("t-corr-skip", [("欄位", False), ("型別", False), ("原因", False)], rows))
        else:
            body += _none()
        parts = ([f"兩欄都有值的不到 {CORR_MIN_ROWS:,} 筆的 {info['few_pairs']} 對"] if info["few_pairs"] else []) + (
            [f"其中一欄在兩欄都有值的那些筆裡沒有變化的 {info['flat_pairs']} 對"] if info["flat_pairs"] else [])
        if parts:
            body += _note("另外有幾對欄位沒有相關係數，兩欄都不在上表：" + "；".join(parts)
                          + "。筆數不夠的，通常是兩欄很少同時有值。")
        return _section("s-corr-skip", f"未計算的欄位（{len(rows)} 欄）", body, level=3)

    # --- 全部欄位 -----------------------------------------------------------

    def all_columns(self) -> str:
        rows = []
        for i, c in enumerate(self.feats):
            r = self.ratios(c)
            flags = self.flags[c["name"]]
            numeric = c["kind"] == "numeric"
            lo = [v for v in c["min"] if v is not None]
            hi = [v for v in c["max"] if v is not None]
            rows.append(({"id": f"col-{c['name']}", "data-name": c["name"],
                          "data-flag": " ".join(flags) or "ok", "data-dtype": _dtype_group(c["dtype"])}, [
                _td(str(i + 1), v=i, num=True),
                _td(f'<button type="button" class="exp">{_e(c["name"])}</button>', v=c["name"].lower(), cls="name"),
                _td(_e(c["dtype"])),
                _td(_e("、".join(FLAG_NAMES[k] for k in flags) or "—"), cls="wrap"),
                _td(self.spark_null(c), v=max(r)),
                _td(_e(_pct_range(r)), v=max(r)),
                _td_num(min(lo) if numeric and lo else None), _td_num(max(hi) if numeric and hi else None),
            ]))
        flag_opt = [("", "全部")] + [(k, FLAG_NAMES[k]) for k in FLAG_ORDER] + [("ok", "沒有發現問題")]
        type_opt = [("", "全部"), ("int", "整數"), ("decimal", "decimal"), ("double", "double"),
                    ("float", "float"), ("string", "字串")]
        data = {"periods": self.periods, "labels": self.labels, "rows": self.rows,
                "cols": {c["name"]: [c["null"], c["min"], c["max"]] for c in self.feats},
                "dist": {c["name"]: d for c in self.feats if (d := self.dist_data(c))},
                **({"cm": [data for _, data in self.matrices], "corrKeep": CORR_KEEP} if self.corr else {})}
        # JSON 裡的字串（欄名、字串欄位的值）可能含 </script> 或 <!--<script，前者提早結束
        # <script>，後者讓真正的 </script> 失效、吞掉後半頁；所有 < 都換成 \u003c
        blob = json.dumps(data, ensure_ascii=False).replace("<", "\\u003c")
        return _section("s-all", f"全部欄位（{len(self.feats)} 欄）", (
            _p("查單一欄位或一整族欄位（例如篩選 <code>cc_</code>）。點欄名展開各期的總筆數、空值筆數、"
               "最小值、最大值" + ("，以及分位數與 PSI" if self.dist else "") + "。"
               "表上的最小值、最大值是所有期別合計，只列數值欄位。預設依表裡的欄位順序。")
            + _toolbar("t-all", selects=(("flag", "問題", flag_opt, "", "token"),
                                       ("dtype", "型別", type_opt, "", "token")))
            + _table("t-all", [("#", True), ("欄位", False), ("型別", False), ("問題", False), ("各期空值", False),
                               ("空值比例", False), ("最小值", True), ("最大值", True)], rows, sort=(0, "asc"))
            + f'<script type="application/json" id="fp-data">{blob}</script>'
        ))

    def dist_data(self, col: dict) -> dict | None:
        """滑鼠提示與展開明細用的每期數字（JS 只負責排版，數字都在這裡算好）。"""
        out: dict[str, Any] = {}
        if col.get("quantiles"):
            idx = [QUANTILE_PROBS.index(p) for p in _BAND_PROBS]
            out["q"] = [None if q is None else [q[i] for i in idx] for q in col["quantiles"]]
            tails = [tail_ratios(col, t) for t in range(len(self.periods))]
            out["tail"] = [None if r is None else [round(v, 3) for side in ("max", "min") for v in r[side]]
                           for r in tails]
        psi = None if set(NO_DIST_FLAGS) & set(self.flags[col["name"]]) else self.psis[col["name"]]
        if psi:
            out["psi"] = [round(v, 4) for v in psi]
        if psi and col["kind"] == "string":
            out["cats"] = col["bins"]["values"] + ["其他值", "空值"]
            out["sh"] = [[round(v, 4) for v in _shares(cnt)] for cnt in col["bins"]["counts"]]
        return out or None

    # --- 行動建議總結 -------------------------------------------------------

    def upstream_periods(self) -> dict[int, list[str]]:
        """要請上游查的期別 → 欄名，只算這一欄被歸到「請上游查」的那種問題。"""
        out: dict[int, list[str]] = {}
        for c in self.with_action("upstream"):
            for t in upstream_periods_of(c, self.rows, self.flags[c["name"]], self.thresholds):
                out.setdefault(t, []).append(c["name"])
        return dict(sorted(out.items()))

    def drift_start_periods(self) -> dict[int, list[str]]:
        """PSI 第一次達門檻的期別 → 欄名：同一期開始變的一批欄，多半是同一件事。"""
        out: dict[int, list[str]] = {}
        for c in self.with_action("check"):
            out.setdefault(drift_periods(c, self.thresholds)[0], []).append(c["name"])
        return dict(sorted(out.items()))

    def period_list(self, by_period: dict[int, list[str]], button: str) -> str:
        return self.name_list([(self.labels[t], names) for t, names in by_period.items()], button)

    @staticmethod
    def name_list(items: list[tuple[str, list[str]]], button: str) -> str:
        """總結裡一列一批欄：（標題、欄名），每批一顆複製鈕。"""
        lis = "".join(
            f'<li><b>{_e(label)}</b>　{len(names)} 欄：{_e("、".join(names[:6]))}'
            f'{"…" if len(names) > 6 else ""}'
            f' <button type="button" data-copy-text="{_e(chr(10).join(names))}">{_e(button)}</button></li>'
            for label, names in items
        )
        return f'<ul class="names">{lis}</ul>'

    def action_summary(self) -> str:
        jump_txt = f"{UPSTREAM_JUMP_MIN * 100:g} 個百分點"
        long_txt = f"{TAIL_LONG_SHARE * 100:g}%"
        sources = {
            "exclude": f"{FLAG_NAMES['null_all']}、{FLAG_NAMES['const_all']}",
            "upstream": f"{FLAG_NAMES['null_some']}（跳幅 ≥ {jump_txt}）、{FLAG_NAMES['const_some']}"
                        + (f"、{FLAG_NAMES['tail']}（被標的期別不到 {long_txt}，只列最離譜的那一期）" if self.dist else ""),
            "check": f"{FLAG_NAMES['drift']}（偏移的期別都已經列在「請上游查」的除外）",
            "judge": f"{FLAG_NAMES['null_some']}（跳幅 < {jump_txt}）、{FLAG_NAMES['const_each']}"
                     + (f"、{FLAG_NAMES['tail']}（{long_txt} 以上的期別都被標）" if self.dist else ""),
        }
        if self.corr:
            sources["corr"] = f"{FLAG_NAMES['corr']}：最新一期的相關係數，絕對值 ≥ {self.thresholds['corr']:g}"
        rows = []
        for key in ACTION_ORDER:
            if (key == "check" and not self.dist) or (key == "corr" and not self.corr):
                continue
            cols = self.with_action(key)
            listing = "\n".join(c["name"] for c in cols)
            extra = ""
            if key == "upstream" and cols:
                extra = self.period_list(self.upstream_periods(), "複製這期的欄名")
            elif key == "check" and cols:
                extra = self.period_list(self.drift_start_periods(), "複製這期開始變的欄名")
            elif key == "corr" and cols:
                extra = self.name_list([(f"第 {g + 1} 組", names) for g, names in enumerate(self.corr_groups)],
                                       "複製這組的欄名")
            detail = (f"<details><summary>{len(cols)} 欄</summary><pre>{_e(listing)}</pre></details>"
                      if cols else "0 欄")
            copy = (f'<button type="button" data-copy-text="{_e(listing)}">複製欄名</button>' if cols else "")
            rows.append(({}, [
                _td(f"<b>{_e(ACTION_TEXTS[key])}</b>{extra}", cls="wrap"),
                _td(str(len(cols)), v=len(cols), num=True),
                _td(_e(sources[key]), cls="wrap"), _td(detail, cls="wrap"), _td(copy),
            ]))
        return _section("s-actions", "行動建議總結", (
            _p("把前面各段的結果整理成做法。複製的欄名一行一個。同一欄可能出現在兩列，"
               "例如某一期幾乎全空、其他期別每期各只有一個值。")
            + _table("t-actions", [("做法", False), ("欄數", True), ("來自", False), ("欄位", False),
                                   ("", False)], rows, cls="actions")
            + _note(f"「請上游查」和「看業務意義」的分界：空值看跳幅 {jump_txt}，出事那幾期比其他期別空很多，"
                    "才像是上游斷掉"
                    + (f"；極端值看被標的期別有沒有到 {long_txt}，只有少數幾期被標的像那幾期的資料錯了，"
                       "大部分期別都被標的像欄位本來就是長尾" if self.dist else "") + "。")
        ))

    # --- 資料範圍與判斷標準 -------------------------------------------------

    def scope(self) -> str:
        cfg = self.profile["config"]
        code = lambda s: f"<code>{_e(s)}</code>"  # noqa: E731
        span = f"{_e(self.span(self.periods))}，共 {len(self.periods)} 期" if self.periods else "無"
        kv = [
            ("資料表", code(cfg["table"])),
            ("期別欄位", code(cfg["time_col"])),
            ("主鍵欄位", "、".join(code(c) for c in cfg["key_cols"])),
            ("排除欄位", "、".join(code(c) for c in cfg["exclude_cols"]) or "無"),
            ("期間", span),
            ("篩選條件", code(cfg["where"]) if cfg["where"] else "無"),
            ("每期筆數", f"{min(self.rows):,}～{max(self.rows):,} 筆，全部納入計算" if self.rows else "無"),
            ("期別欄位是空值的列", f"{self.profile['null_time_rows']:,} 筆，不讀"),
            ("執行項目", "、".join(["基本檢查"] + (["分布偏移與極端值"] if self.dist else [])
                                   + (["相關性"] if self.corr else []))),
            *([("基準期", f"{_e(self.periods[0])}，這次讀到的最早一期；想換基準期，改 {code('time_range')} "
                          f"或 {code('where')}"),
               ("分位數", f"Spark 的 percentile_approx 估計，精度 {self.profile['quantile_accuracy']:,}"
                         f"（排名誤差約 1／{self.profile['quantile_accuracy']:,}）")]
              if self.dist and self.periods else []),
            *([("相關性", self.corr_scope())] if self.corr else []),
            ("執行時間", self.elapsed_text()),
            ("產出時間", _e(self.profile["generated_at"].replace("T", " "))),
        ]
        kv_html = ('<div class="tbox"><table class="t kv"><tbody>'
                   + "".join(f"<tr><th>{_e(k)}</th><td>{v}</td></tr>" for k, v in kv)
                   + "</tbody></table></div>")
        th = self.thresholds
        th_rows = [({}, [
            _td(_e(f"{FLAG_NAMES['null_all']}、{FLAG_NAMES['null_some']}")),
            _td(_e(f"空值比例 ≥ {self.th_text}")), _td(code("null_ratio")), _td(_e(self.null_th)),
        ])]
        if self.dist:
            th_rows += [
                ({}, [_td(_e(FLAG_NAMES["drift"])), _td(_e(f"有一期和基準期比的 PSI ≥ {th['psi']:g}")),
                      _td(code("psi")), _td(_e(th["psi"]))]),
                ({}, [_td(_e(FLAG_NAMES["tail"])),
                      _td(_e(f"同一期、同一側：倍數 ≥ {_fmt_num(th['tail_ratio'])}，"
                             f"而且 log 後倍數 ≥ {_fmt_num(th['tail_log_ratio'])}")),
                      _td(f"{code('tail_ratio')}、{code('tail_log_ratio')}"),
                      _td(_e(f"{th['tail_ratio']}、{th['tail_log_ratio']}"))]),
            ]
        if self.corr:
            th_rows.append(({}, [_td(_e(FLAG_NAMES["corr"])),
                                 _td(_e(f"最新一期的 Spearman 相關係數，絕對值 ≥ {th['corr']:g}")),
                                 _td(code("corr")), _td(_e(th["corr"]))]))
        note = "可以在這支工具設定檔的 <code>thresholds</code> 區塊調整，再用 <code>render</code> 重畫，不必重跑 Spark。"
        if self.corr:
            note += (f"<code>corr</code> 最低只能調到 {CORR_KEEP:g}：這次只存了絕對值 ≥ {CORR_KEEP:g} 的配對，"
                     "要更低得重跑。")
        if self.threshold_source:
            note += f"這次的值取自重畫時指定的設定檔 {code(self.threshold_source)}。"
        unchecked = (_toolbar("t-unchecked") + _table("t-unchecked", [("欄位", False), ("型別", False)],
                            [({"data-name": c["name"]}, [_td(_e(c["name"]), cls="name"), _td(_e(c["dtype"]))])
                             for c in self.unchecked])
                     if self.unchecked else _none())
        return _section("s-scope", "資料範圍與判斷標準", (
            kv_html
            + "<h3>判斷標準</h3>" + _p(note)
            + _table("t-thresholds", [("問題類型", False), ("判斷標準", False), ("設定鍵", False),
                                      ("這次的值", False)], th_rows)
            + _note("「只有一個值」的三種與「值幾乎都相同」沒有門檻。總結裡「請上游查」和「看業務意義」的分界"
                    f"寫在程式裡，不是設定鍵：空值看跳幅 {UPSTREAM_JUMP_MIN * 100:g} 個百分點"
                    + (f"，極端值看被標的期別有沒有到 {TAIL_LONG_SHARE * 100:g}%" if self.dist else "") + "。")
            + "<h3>未檢查的欄位</h3>"
            + _p("型別不支援（date、timestamp、array、map、struct 等）而沒有檢查的欄位。")
            + unchecked
            + f"<details><summary><b>設定檔</b></summary><pre>{_e(self.profile['config_text'])}</pre></details>"
        ))

    def corr_scope(self) -> str:
        info = self.profile["correlation"]
        if not info["columns"]:
            return f"最新一期 {_e(self.periods[-1])} 沒有可以算相關係數的數值欄位"
        took = (f"全部 {info['rows']:,} 筆" if info["rows"] == info["period_rows"]
                else f"抽 {info['rows']:,} 筆，該期 {info['period_rows']:,} 筆，抽樣的 seed 是 {CORR_SAMPLE_SEED}")
        return (f"最新一期 {_e(self.periods[-1])} {took}；數值欄位 {len(info['columns'])} 欄兩兩算 Spearman "
                f"相關係數，存絕對值 ≥ {CORR_KEEP:g} 的配對")

    def elapsed_text(self) -> str:
        text = f"{_duration(self.profile['elapsed_seconds'])}，每批 {self.profile['batch_size']} 欄"
        passes = self.profile.get("pass_seconds")
        if passes and len(passes) > 1:
            names = {"basic": "基本檢查與分位數", "top_values": "字串欄位的常見值", "bins": "PSI 分組筆數",
                     "correlation": "相關性"}
            text += "（" + "、".join(f"{names[k]} {_duration(v)}" for k, v in passes.items()) + "）"
        return text

    def page(self) -> str:
        table = self.profile["config"]["table"]
        main = "".join([self.overview(), self.nulls(), self.consts(), self.drift(), self.tail(),
                        self.corr_section(), self.all_columns(), self.action_summary(), self.scope()])
        return (
            "<!DOCTYPE html><html lang='zh-Hant'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>特徵表檢查報告：{_e(table)}</title><style>{_CSS}</style></head><body>"
            f"{self.header()}<div class='layout'>{self.toc()}<main>{main}</main></div>"
            f"<script>{_JS}</script></body></html>"
        )


def write_report(profile: dict, out_dir: Path, thresholds: dict[str, float],
                 threshold_source: str | None) -> Path:
    """依 ``thresholds`` 判斷後畫成單一頁面 ``index.html``，回傳它的路徑。

    只讀 ``thresholds`` 裡這一版認得的鍵，所以上一版存的 ``profile.json`` 帶著已經拿掉的
    ``precision_max_abs`` 也能重畫。
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "index.html"
    path.write_text(_Report(profile, thresholds, threshold_source).page(), encoding="utf-8")
    return path


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
    accuracy: int = typer.Option(
        DEFAULT_QUANTILE_ACCURACY, "--accuracy", min=1,
        help="分位數（percentile_approx）的精度，只在 checks.distribution: true 時用到。正式環境量過再定預設值",
    ),
) -> None:
    """算統計量、存 profile.json、畫報告。"""
    from pyspark.sql.utils import AnalysisException

    from recsys_tfb.utils.spark import get_or_create_spark_session

    cfg, problems = parse_config(config.read_text(encoding="utf-8"))
    if cfg.table is None:
        _exit_with_problems(problems)  # 沒有表名，表與欄位就檢查不了

    # 同 scripts/sampling_overrides_editor.py：其餘 Spark 設定全由 SPARK_CONF_DIR 決定
    spark = get_or_create_spark_session({"app_name": "feature-profile"})
    try:
        columns = spark.table(cfg.table).columns
    except AnalysisException:
        columns = None
    problems += check_against_table(cfg, columns)
    if problems:
        _exit_with_problems(problems)
    profile = profile_table(spark, cfg, batch_size, accuracy)

    if not profile["periods"]:
        typer.echo("一期都沒讀到，請檢查 time_range 與 where。", err=True)
        raise typer.Exit(code=1)
    out_dir = Path(cfg.output_dir) if cfg.output_dir else _default_output_dir(cfg.table)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / PROFILE_FILENAME).write_text(
        json.dumps(profile, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    path = write_report(profile, out_dir, cfg.thresholds, None)
    typer.echo(f"報告：{path}")


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
    path = write_report(profile, out_dir, thresholds, source)
    typer.echo(f"報告：{path}")


if __name__ == "__main__":
    app()
