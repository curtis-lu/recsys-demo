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
依 float32 本身算，不用 dataset 的精度閘門 B8），只借 ``get_or_create_spark_session``
開 Spark。報告是工具自己產生的單一頁面（不用 ``recsys_tfb.report``）：要排序、篩選、
複製欄名、頁內目錄，共用的多頁外框給不了，改它又會連帶改到 evaluation 的診斷頁。
開 Spark 的方式同
``scripts/sampling_overrides_editor.py``：只給 ``app_name``，其餘 Spark 設定全由
``SPARK_CONF_DIR`` 決定，工具的設定檔不另設 Spark 區塊。
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
from typing import TYPE_CHECKING, Any

import typer
import yaml

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

_REQUIRED_KEYS = ("table", "time_col", "key_cols")
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
    return m is not None and over_limit(m[0], col, thresholds)


def precision_limit(col: dict, thresholds: dict[str, float]) -> float:
    return thresholds["precision_max_abs"] if col["has_decimal"] else INT_EXACT


def over_limit(v: float, col: dict, thresholds: dict[str, float]) -> bool:
    """有小數的欄位到門檻就算（門檻是使用者定的）；整數要超過 2^24 才會被改掉。"""
    if col["has_decimal"]:
        return v >= thresholds["precision_max_abs"]
    return v > INT_EXACT


def judge(profile: dict, thresholds: dict[str, float]) -> dict[str, list[str]]:
    return {
        c["name"]: judge_column(c, profile["rows"], thresholds)
        for c in profile["columns"]
        if c["kind"] != "unchecked"
    }


# ---------------------------------------------------------------------------
# 要做的事：把判斷結果依讀者要動手的事分組（render 換門檻時一起重算）
# ---------------------------------------------------------------------------

# 「部分期別幾乎是空值」的欄位，出事那幾期比其他期別至少高這麼多（0.10＝10 個百分點）
# 才算上游斷掉；不到的是平常就很稀疏、偶爾越過門檻的欄位，歸「自行判斷」。
# 只決定分組，不改判斷，所以不進 thresholds；值在正式環境 demo 時再定（設計文件第八節）。
UPSTREAM_JUMP_MIN = 0.10

ACTION_ORDER = ("exclude", "upstream", "blocked", "judge", "ignore")
ACTION_LABELS = {
    "exclude": "直接排除",
    "upstream": "回頭查上游",
    "blocked": "dataset 會擋下",
    "judge": "自行判斷",
    "ignore": "可以不理",
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


def actions(col: dict, rows: list[int], flags: list[str],
            thresholds: dict[str, float]) -> list[str]:
    """這一欄要做的事，依 ``ACTION_ORDER``。

    一欄可能要做兩件事（例如某一期斷掉、又超過 float32 上限），兩件都列，
    讀者照任何一段做都不會漏。只有 double 的 float32 問題時才是「可以不理」：
    dataset 的精度閘門 B8 不檢查 float／double。
    """
    if set(flags) & set(EXCLUDABLE):
        return ["exclude"]
    out = []
    jump = null_jump(col, rows, thresholds) if "null_some" in flags else None
    if "const_some" in flags or (jump is not None and jump >= UPSTREAM_JUMP_MIN):
        out.append("upstream")
    if "precision" in flags and col["dtype"] not in _FLOAT_TYPES:
        out.append("blocked")
    if "const_each" in flags or (jump is not None and jump < UPSTREAM_JUMP_MIN):
        out.append("judge")
    if not out and "precision" in flags:
        out.append("ignore")
    return out


def float32_max_error(m: float) -> float:
    """絕對值最大是 ``m`` 的欄位存成 float32 最多差多少：m 那一段 float32 格距的一半。

    float32 有 24 位有效位數，[2^e, 2^(e+1)) 之間的格距是 2^(e−23)。不用
    ``np.spacing(np.float32(m))``：m 略小於 2 的次方、存成 float32 進位到那個
    次方時，它會多算一倍。
    """
    return 2.0 ** (math.floor(math.log2(m)) - 24) if m > 0 else 0.0




# ---------------------------------------------------------------------------
# 報告：單一頁面，表格可以排序、篩選、複製欄名（純手寫 JS，不連網、不加套件）
# ---------------------------------------------------------------------------

# 〈各期空值比例〉的色階：藍色由淺到深只表示多寡，達到門檻換成橘色
_NULL_CUTS = (0.01, 0.10, 0.50)
_VAR_CHOICES = (0, 0.5, 1, 5, 10)  # 變動幅度的篩選選項，百分點
_VAR_DEFAULT = 1


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


def _fmt_err(v: float) -> str:
    """float32 最大誤差：2 的次方，大的寫整數，小的留到看得出第一位有效數字。"""
    if v >= 1:
        return f"±{v:,.0f}"
    return f"±{v:.2g}" if v >= 1e-4 else f"±{v:.1e}"


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
    """期別都是同月不重複的日期時，表頭只寫到月（2025-06），完整日期放滑鼠提示。"""
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


def _null_cuts(th: float) -> list[float]:
    return [c for c in _NULL_CUTS if c < th]


def _null_class(r: float, th: float) -> str:
    if r >= th:
        return "bh"
    if r == 0:
        return "b0"
    return f"b{1 + sum(r >= c for c in _null_cuts(th))}"


def _e(v: Any) -> str:
    return html.escape(str(v), quote=True)


def _attrs(d: dict[str, Any]) -> str:
    return "".join(f' {k}="{_e(v)}"' for k, v in d.items() if v is not None)


def _td(display: str, *, v: Any = None, num: bool = False, cls: str = "", title: str = "") -> str:
    """一格。``display`` 是已經 escape 過的 HTML；``v`` 是排序用的原始值（None＝依文字排）。"""
    classes = " ".join(c for c in ("n" if num else "", cls) if c)
    a = {"class": classes or None, "title": title or None}
    if v is not None:
        a["data-v"] = "" if isinstance(v, float) and math.isnan(v) else v
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


def _toolbar(tid: str, *, selects: tuple = (), checks: tuple = (), copy: bool = True) -> str:
    """表格上方的工具列：欄名篩選、下拉選單、勾選框、計數、複製目前列出的欄名。

    ``selects``：（列屬性、說明、[(值, 文字)]、預設值、mode）。mode 是 ``token``（列的屬性
    含這個值）或 ``min``（列的屬性 ≥ 這個值）。``checks``：（列屬性、文字、預設勾選），
    勾選時藏起屬性為 1 的列；列屬性是 ``toggle`` 時改為切換表格的 ``shownum`` 樣式。
    """
    parts = [f'<div class="toolbar" data-for="{_e(tid)}">',
             '<input class="q" type="search" placeholder="篩選欄名，空白分隔多個關鍵字">']
    for attr, label, options, default, mode in selects:
        opts = "".join(
            f'<option value="{_e(v)}"{" selected" if str(v) == str(default) else ""}>{_e(t)}</option>'
            for v, t in options
        )
        parts.append(f'<label>{_e(label)} <select data-attr="{_e(attr)}" data-mode="{mode}">{opts}</select></label>')
    for attr, label, checked in checks:
        key = "data-toggle" if attr == "toggle" else "data-hide"
        val = "shownum" if attr == "toggle" else attr
        parts.append(f'<label><input type="checkbox" {key}="{val}"{" checked" if checked else ""}> {_e(label)}</label>')
    parts.append('<span class="count"></span>')
    if copy:
        parts.append(f'<button type="button" data-copy-rows="{_e(tid)}">複製列出的欄名</button>')
    parts.append("</div>")
    return "".join(parts)


def _copy_block(bid: str, text: str, label: str) -> str:
    return (f'<div class="copy"><button type="button" data-copy="{_e(bid)}">{_e(label)}</button>'
            f'<pre id="{_e(bid)}">{_e(text)}</pre></div>')


def _section(sid: str, title: str, body: str, *, collapsed: str | None = None) -> str:
    if collapsed is not None:
        return (f'<section id="{sid}"><details><summary><h2>{_e(title)}</h2>'
                f'<span class="sum">{_e(collapsed)}</span></summary>{body}</details></section>')
    return f'<section id="{sid}"><h2>{_e(title)}</h2>{body}</section>'


def _p(text: str) -> str:
    return f'<p class="desc">{text}</p>'


def _none() -> str:
    return '<p class="none">這次沒有。</p>'


def _flag_texts(th: dict[str, float]) -> dict[str, tuple[str, str]]:
    """問題類型 →（名稱、判斷標準）。"""
    null = _pct_threshold(th["null_ratio"])
    prec = _fmt_num(th["precision_max_abs"])
    return {
        "null_all": ("每一期都幾乎是空值", f"每一期的空值比例都 ≥ {null}"),
        "const_all": ("整欄只有一個值", "所有期別合計只有一個值，不含空值"),
        "null_some": ("部分期別幾乎是空值", f"只有部分期別的空值比例 ≥ {null}"),
        "const_some": ("部分期別只有一個值", "只有部分期別只有一個值，不含空值"),
        "const_each": ("每期各只有一個值", "每一期只有一個值，但各期的值不同"),
        "precision": (
            "float32 精度不足",
            f"有小數且絕對值最大 ≥ {prec}；或整數且絕對值最大超過 {INT_EXACT:,}；"
            "宣告成 float 的欄位不判斷",
        ),
    }


_CSS = """
:root{--fg:#1f2328;--muted:#59636e;--line:#d1d9e0;--soft:#f6f8fa;--link:#0969da;
--b0:#ffffff;--b1:#eef4fb;--b2:#c6dbef;--b3:#6baed6;--b4:#2171b5;--bh:#c2410c}
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
.layout{display:grid;grid-template-columns:210px minmax(0,1fr);gap:32px;max-width:1500px;margin:0 auto;padding:20px 24px 80px}
nav.toc{position:sticky;top:76px;align-self:start;font-size:13px}
nav.toc a{display:flex;justify-content:space-between;color:var(--fg);padding:3px 8px;border-radius:6px}
nav.toc a:hover{background:var(--soft);text-decoration:none}
nav.toc a span{color:var(--muted);font-variant-numeric:tabular-nums}
nav.toc hr{border:0;border-top:1px solid var(--line);margin:8px 0}
@media (max-width:1100px){.layout{grid-template-columns:1fr}nav.toc{position:static}}
section{margin:0 0 44px;scroll-margin-top:80px}
h2{font-size:20px;margin:0 0 8px;padding-bottom:6px;border-bottom:1px solid var(--line)}
h3{font-size:15px;margin:22px 0 6px}
details>summary{cursor:pointer;list-style:none}
details>summary h2{display:inline}details>summary .sum{display:block;color:var(--muted);margin:4px 0 8px}
details>summary::before{content:"▸ ";color:var(--muted)}details[open]>summary::before{content:"▾ "}
p.desc,ul.desc{max-width:80ch;color:#333;margin:6px 0 10px}
p.note{max-width:80ch;color:var(--muted);font-size:13px;margin:6px 0}
p.none{color:var(--muted)}
.tbox{max-height:72vh;overflow:auto;border:1px solid var(--line);border-radius:6px;margin:8px 0}
table.t{border-collapse:separate;border-spacing:0;width:100%;font-variant-numeric:tabular-nums}
table.t th,table.t td{padding:4px 10px;border-bottom:1px solid #eaeef2;text-align:left;vertical-align:top;white-space:nowrap}
table.t td.wrap{white-space:normal;min-width:160px;max-width:420px}
table.t th{position:sticky;top:0;z-index:2;background:var(--soft);font-weight:600;cursor:pointer;user-select:none}
table.t th[data-dir=asc]::after{content:" ▲";color:var(--muted)}
table.t th[data-dir=desc]::after{content:" ▼";color:var(--muted)}
table.t .n{text-align:right}
table.t tbody tr:hover td{background:#f3f8ff}
table.kv th{width:12em;cursor:default}
.toolbar{display:flex;flex-wrap:wrap;gap:6px 14px;align-items:center;margin:10px 0 0;font-size:13px}
.toolbar input.q{width:260px;padding:4px 8px;border:1px solid var(--line);border-radius:6px}
.toolbar select{padding:2px 4px}
.toolbar .count{color:var(--muted)}
button{font:inherit;font-size:12px;padding:3px 10px;border:1px solid var(--line);border-radius:6px;background:#fff;cursor:pointer}
button:hover{background:var(--soft)}
.copy{position:relative;margin:8px 0}
.copy button{position:absolute;right:8px;top:8px}
pre{background:var(--soft);border:1px solid var(--line);border-radius:6px;padding:10px 12px;margin:0;overflow:auto;max-height:320px}
.legend{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:12px;color:var(--muted);margin:6px 0}
.legend i{display:inline-block;width:14px;height:14px;vertical-align:-2px;margin-right:4px;border:1px solid var(--line)}
.b0{background:var(--b0)}.b1{background:var(--b1)}.b2{background:var(--b2)}
.b3{background:var(--b3);color:#fff}.b4{background:var(--b4);color:#fff}.bh{background:var(--bh);color:#fff}
table.grid td.c{padding:0;min-width:40px;height:24px;text-align:center;font-size:11px;border-left:1px solid #fff}
table.grid th{font-size:12px;padding:4px 6px}
table.grid td.c span{visibility:hidden}table.grid.shownum td.c span{visibility:visible}
table.grid td.name,table.grid th:first-child{position:sticky;left:0;z-index:1;background:#fff}
table.grid th:first-child{z-index:3;background:var(--soft)}
.strip{display:inline-flex;gap:1px}.strip i{display:inline-block;width:9px;height:14px;border:1px solid #e1e4e8}
tr.detail td{background:var(--soft);white-space:normal}
table.mini{border-collapse:collapse;margin:4px 0 8px;font-size:12px}
table.mini th,table.mini td{padding:2px 10px;border-bottom:1px solid var(--line);text-align:right}
table.mini th:first-child,table.mini td:first-child{text-align:left}
button.exp{border:0;background:none;padding:0;color:var(--link);font-family:inherit;font-size:13px}
button.exp::before{content:"▸ ";color:var(--muted)}tr.open button.exp::before{content:"▾ "}
"""

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
document.querySelectorAll("table.t").forEach(function(t){
 t.querySelectorAll("thead th").forEach(function(th,i){th.addEventListener("click",function(){
  sortBy(t,i,th.getAttribute("data-dir")==="asc"?"desc":"asc");});});});
function apply(bar){var t=document.getElementById(bar.getAttribute("data-for"));if(!t)return;
 var q=bar.querySelector("input.q"),terms=(q?q.value:"").toLowerCase().split(/\s+/).filter(Boolean);
 var sels=bar.querySelectorAll("select[data-attr]"),hides=bar.querySelectorAll("input[data-hide]");
 var shown=0,total=0;
 rowsOf(t).forEach(function(r){total++;var name=(r.getAttribute("data-name")||"").toLowerCase();
  var ok=terms.every(function(w){return name.indexOf(w)>=0;});
  sels.forEach(function(s){if(!ok||s.value==="")return;var v=r.getAttribute("data-"+s.getAttribute("data-attr"))||"";
   ok=s.getAttribute("data-mode")==="min"?Number(v)>=Number(s.value):v.split(" ").indexOf(s.value)>=0;});
  hides.forEach(function(c){if(ok&&c.checked&&r.getAttribute("data-"+c.getAttribute("data-hide"))==="1")ok=false;});
  r.hidden=!ok;if(r._detail)r._detail.hidden=!ok||!r.classList.contains("open");if(ok)shown++;});
 var cnt=bar.querySelector(".count");if(cnt)cnt.textContent="顯示 "+shown+"／"+total+" 列";}
var bars=document.querySelectorAll(".toolbar");
bars.forEach(function(bar){var t=document.getElementById(bar.getAttribute("data-for"));
 bar.querySelectorAll("input,select").forEach(function(el){
  el.addEventListener(el.type==="search"?"input":"change",function(){
   if(el.getAttribute("data-toggle")&&t)t.classList.toggle(el.getAttribute("data-toggle"),el.checked);apply(bar);});
  if(el.getAttribute("data-toggle")&&t)t.classList.toggle(el.getAttribute("data-toggle"),el.checked);});});
var g=document.getElementById("global-q");
if(g)g.addEventListener("input",function(){bars.forEach(function(bar){var q=bar.querySelector("input.q");if(q){q.value=g.value;apply(bar);}});});
function copy(text,btn){var old=btn.textContent;function done(){btn.textContent="已複製";setTimeout(function(){btn.textContent=old;},1500);}
 function fallback(){var ta=document.createElement("textarea");ta.value=text;ta.style.position="fixed";ta.style.opacity="0";
  document.body.appendChild(ta);ta.select();try{document.execCommand("copy");done();}catch(e){}document.body.removeChild(ta);}
 if(navigator.clipboard&&window.isSecureContext)navigator.clipboard.writeText(text).then(done,fallback);else fallback();}
document.addEventListener("click",function(ev){var b=ev.target.closest("button");if(!b)return;
 if(b.hasAttribute("data-copy")){copy(document.getElementById(b.getAttribute("data-copy")).textContent,b);}
 else if(b.hasAttribute("data-copy-rows")){var t=document.getElementById(b.getAttribute("data-copy-rows"));
  copy(rowsOf(t).filter(function(r){return !r.hidden;}).map(function(r){return r.getAttribute("data-name");}).join(", "),b);}
 else if(b.hasAttribute("data-copy-text")){copy(b.getAttribute("data-copy-text"),b);}
 else if(b.classList.contains("exp")){toggle(b.closest("tr"));}});
var DATA=JSON.parse((document.getElementById("col-data")||{}).textContent||"{}");
function fmt(v){if(v===null||v===undefined)return "";if(typeof v==="number"){
 return Number.isInteger(v)?v.toLocaleString("en-US"):v.toLocaleString("en-US",{maximumFractionDigits:6});}return String(v);}
function pct(n,d){if(!d)return "";var x=n/d;if(x===0)return "0%";if(x===1)return "100%";
 var s=(x*100).toFixed(x<0.01||x>0.99?2:1);if(Number(s)>=100)return "> 99.99%";if(Number(s)===0)return "< 0.01%";return s+"%";}
function toggle(tr){if(!tr)return;var name=tr.getAttribute("data-name"),c=DATA.cols&&DATA.cols[name];if(!c)return;
 if(!tr._detail){var h="<table class='mini'><thead><tr><th>期別</th><th>總筆數</th><th>空值筆數</th><th>空值比例</th><th>最小值</th><th>最大值</th></tr></thead><tbody>";
  DATA.periods.forEach(function(p,i){h+="<tr><td>"+p+"</td><td>"+fmt(DATA.rows[i])+"</td><td>"+fmt(c[0][i])+"</td><td>"+pct(c[0][i],DATA.rows[i])+
   "</td><td>"+fmt(c[1][i])+"</td><td>"+fmt(c[2][i])+"</td></tr>";});
  var d=document.createElement("tr");d.className="detail";d.innerHTML="<td colspan='"+tr.cells.length+"'>"+h+"</tbody></table></td>";
  tr._detail=d;tr.parentNode.insertBefore(d,tr.nextSibling);tr.classList.add("open");return;}
 var open=!tr.classList.contains("open");tr.classList.toggle("open",open);tr._detail.hidden=!open;}
document.addEventListener("click",function(ev){var a=ev.target.closest("a.col");if(!a)return;
 var tr=document.getElementById("col-"+a.getAttribute("data-col"));if(!tr)return;ev.preventDefault();
 var bar=document.querySelector('.toolbar[data-for="t-all"]');
 if(bar){bar.querySelectorAll("input.q").forEach(function(q){q.value="";});bar.querySelectorAll("select").forEach(function(s){s.selectedIndex=0;});apply(bar);}
 if(!tr.classList.contains("open"))toggle(tr);tr.scrollIntoView({block:"center"});});
bars.forEach(function(bar){try{apply(bar);}catch(e){if(window.console)console.error(e);}});
})();
"""


class _Report:
    """把 profile 與判斷結果整理成單一頁面。每一段一個方法，回傳 HTML。"""

    def __init__(self, profile: dict, thresholds: dict[str, float], threshold_source: str | None):
        self.profile = profile
        self.thresholds = thresholds
        self.threshold_source = threshold_source
        self.periods: list[str] = profile["periods"]
        self.labels = _period_labels(self.periods)
        self.rows: list[int] = profile["rows"]
        self.feats = [c for c in profile["columns"] if c["kind"] != "unchecked"]
        self.unchecked = [c for c in profile["columns"] if c["kind"] == "unchecked"]
        self.flags = judge(profile, thresholds)
        self.texts = _flag_texts(thresholds)
        self.actions = {c["name"]: actions(c, self.rows, self.flags[c["name"]], thresholds)
                        for c in self.feats}

    # --- 共用 ---------------------------------------------------------------

    def with_action(self, key: str) -> list[dict]:
        return [c for c in self.feats if key in self.actions[c["name"]]]

    def ratios(self, col: dict) -> list[float]:
        return null_ratios(col, self.rows)

    def flag_names(self, col: dict, skip: tuple[str, ...] = ()) -> str:
        return "、".join(self.texts[k][0] for k in self.flags[col["name"]] if k not in skip)

    def other_problems(self, col: dict, here: str) -> str:
        """這一欄在其他段落也要做的事，例如「dataset 會擋下」。"""
        return "、".join(ACTION_LABELS[a] for a in self.actions[col["name"]] if a != here)

    def strip(self, col: dict) -> str:
        th = self.thresholds["null_ratio"]
        cells = "".join(
            f'<i class="{_null_class(r, th)}" title="{_e(p)}：空值比例 {_e(_pct(r))}"></i>'
            for p, r in zip(self.periods, self.ratios(col))
        )
        return f'<span class="strip">{cells}</span>'

    def row_attrs(self, col: dict, **extra: Any) -> dict:
        return {"data-name": col["name"], **extra}

    # --- 頁首與目錄 ---------------------------------------------------------

    def header(self) -> str:
        cfg = self.profile["config"]
        span = (f"{_e(cfg['time_col'])} {_e(self.periods[0])}～{_e(self.periods[-1])}，共 {len(self.periods)} 期"
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

    def toc(self) -> str:
        items = [("overview", "總覽", None)]
        items += [(a, ACTION_LABELS[a], len(self.with_action(a))) for a in ACTION_ORDER]
        items += [None, ("nulls", "各期空值比例", None), ("all", "全部欄位", len(self.feats)),
                  ("scope", "資料範圍與判斷標準", None)]
        out = []
        for it in items:
            if it is None:
                out.append("<hr>")
                continue
            sid, label, n = it
            count = f"<span>{n}</span>" if n is not None else ""
            out.append(f'<a href="#{sid}">{_e(label)}{count}</a>')
        return f'<nav class="toc">{"".join(out)}</nav>'

    # --- 總覽 ---------------------------------------------------------------

    def overview(self) -> str:
        t = self.texts
        null_th = _pct_threshold(self.thresholds["null_ratio"])
        jump = f"{UPSTREAM_JUMP_MIN * 100:g} 個百分點"
        rows_spec = [
            ("exclude", f"{t['null_all'][0]}、{t['const_all'][0]}",
             "欄位沒有資訊，把欄名貼進設定檔排除"),
            ("upstream", f"{t['null_some'][0]}（比其他期別高 {jump}以上）、{t['const_some'][0]}",
             "欄位本身正常，回頭查那幾期的上游資料"),
            ("blocked", f"{t['precision'][0]}，而且是整數或 decimal",
             "dataset 的精度檢查會讓 dataset 跑失敗，要上游改單位或改設定"),
            ("judge", f"{t['null_some'][0]}（平常就很空、偶爾越過 {null_th}）、{t['const_each'][0]}",
             "不一定有錯，看業務意義決定要不要留"),
            ("ignore", f"{t['precision'][0]}，而且是 double、沒有其他要做的事",
             "dataset 不檢查 double，對 LightGBM 幾乎沒有影響"),
        ]
        body_rows = []
        for key, problems, todo in rows_spec:
            n = len(self.with_action(key))
            extra = ""
            if key == "upstream" and n:
                extra = f"（{len(self.upstream_periods())} 期）"
            body_rows.append(({}, [
                _td(f'<a href="#{key}">{_e(ACTION_LABELS[key])}</a>'),
                _td(f"{n}{extra}", v=n, num=True), _td(_e(problems), cls="wrap"), _td(_e(todo), cls="wrap"),
            ]))
        ok = sum(1 for c in self.feats if not self.actions[c["name"]])
        body_rows.append(({}, [_td('<a href="#all">沒有發現問題</a>'), _td(str(ok), v=ok, num=True),
                               _td("—"), _td("—")]))
        body_rows.append(({}, [_td('<a href="#scope">未檢查</a>'),
                               _td(str(len(self.unchecked)), v=len(self.unchecked), num=True),
                               _td("型別不支援，例如 date、timestamp", cls="wrap"), _td("—")]))
        hands_on = sum(1 for c in self.feats
                       if set(self.actions[c["name"]]) & {"exclude", "upstream", "blocked"})
        n_num = sum(c["kind"] == "numeric" for c in self.feats)
        return _section("overview", "總覽", (
            _p(f"這份報告依門檻檢查 <code>{_e(self.profile['config']['table'])}</code> 的 "
               f"{len(self.feats)} 個特徵欄位（數值 {n_num} 欄、字串 {len(self.feats) - n_num} 欄），"
               "照要做的事分組。每一組的段落都有欄名清單與依據，欄名點了會跳到〈全部欄位〉看各期數字。")
            + _table("t-overview", [("要做的事", False), ("欄位數", True), ("包含的問題", False),
                                    ("怎麼做", False)], body_rows)
            + f'<p class="note">需要動手的（直接排除、回頭查上游、dataset 會擋下）共 {hands_on} 欄。'
            "同一欄可能要做兩件事，例如某一期斷掉、又超過 float32 上限，兩段都會列出，所以各組加總可能比較多。</p>"
            + "<h3>名詞</h3><ul class='desc'>"
            "<li>空值：NULL、float／double 的 NaN 與 ±Infinity、字串的空字串。空值比例＝空值筆數 ÷ 那一期的總筆數。</li>"
            "<li>只有一個值：扣掉空值後，那一期的最小值等於最大值。整期都是空值的那一期不算。</li>"
            "<li>float32 最大誤差：絕對值最大的那個值附近，存成 float32 最多會差多少。"
            "例如 272,528,123.98 最多差 ±16，101,127.48 最多差 ±0.0039。</li></ul>"
        ))

    # --- 直接排除 -----------------------------------------------------------

    def exclude(self) -> str:
        cols = sorted(self.with_action("exclude"),
                      key=lambda c: (self.flags[c["name"]][0], c["name"]))
        if not cols:
            return _section("exclude", "直接排除（0 欄）", _none())
        rows = [(self.row_attrs(c), [_td_name(c["name"]), _td(_e(c["dtype"])),
                                     _td(_e(self.flag_names(c))), _td(_e(self._exclude_reason(c)))])
                for c in cols]
        names = "\n".join(f"      - {c['name']}" for c in cols)  # 縮排對齊兩份 conf 的清單
        return _section("exclude", f"直接排除（{len(cols)} 欄）", (
            _p("每一期都幾乎是空值，或從頭到尾只有一個值，模型從這些欄位學不到東西。")
            + _toolbar("t-exclude")
            + _table("t-exclude", [("欄位", False), ("型別", False), ("問題", False), ("依據", False)],
                     rows, sort=(2, "asc"))
            + "<h3>貼進設定檔</h3>"
            + _p("只重新訓練、不重建 dataset：貼到 <code>conf/base/parameters_training.yaml</code> 的 "
                 "<code>training.feature_selection.exclude</code> 清單，接在既有項目後面"
                 "（原本寫成 <code>exclude: []</code> 的話，先把 <code>[]</code> 拿掉）。")
            + _copy_block("yaml-train", names, "複製")
            + _p("連 dataset 一起拿掉，要重建 dataset：貼到 <code>conf/base/parameters_dataset.yaml</code> 的 "
                 "<code>dataset.prepare_model_input.drop_columns</code> 清單，接在既有項目後面。")
            + _copy_block("yaml-dataset", names, "複製")
        ))

    def _exclude_reason(self, col: dict) -> str:
        if "null_all" in self.flags[col["name"]]:
            r = self.ratios(col)
            return "每一期都 100% 空值" if min(r) == 1 else f"空值比例 {_pct_range(r)}"
        valued, const = one_value_periods(col, self.rows)
        where = "每一期" if len(valued) == len(self.periods) else "有值的期別"
        return f"{where}都是 {_fmt_num(col['min'][const[0]])}"

    # --- 回頭查上游 ---------------------------------------------------------

    def _bad_periods(self, col: dict) -> tuple[list[int], list[int]]:
        """（幾乎全空的期別、只有一個值的期別），只算這一欄被歸到「回頭查上游」的那種問題。"""
        th = self.thresholds["null_ratio"]
        nulls: list[int] = []
        jump = null_jump(col, self.rows, self.thresholds) if "null_some" in self.flags[col["name"]] else None
        if jump is not None and jump >= UPSTREAM_JUMP_MIN:
            nulls = [t for t, r in enumerate(self.ratios(col)) if r >= th]
        consts = one_value_periods(col, self.rows)[1] if "const_some" in self.flags[col["name"]] else []
        return nulls, consts

    def upstream_periods(self) -> dict[int, tuple[list[dict], list[dict]]]:
        out: dict[int, tuple[list[dict], list[dict]]] = {}
        for c in self.with_action("upstream"):
            nulls, consts = self._bad_periods(c)
            for t in nulls:
                out.setdefault(t, ([], []))[0].append(c)
            for t in consts:
                out.setdefault(t, ([], []))[1].append(c)
        return dict(sorted(out.items()))

    def upstream(self) -> str:
        cols = self.with_action("upstream")
        if not cols:
            return _section("upstream", "回頭查上游（0 欄）", _none())
        jump_txt = f"{UPSTREAM_JUMP_MIN * 100:g} 個百分點"
        by_period = self.upstream_periods()
        period_rows = []
        for t, (nulls, consts) in by_period.items():
            names = [_e(c["name"]) for c in nulls] + [
                f"{_e(c['name'])}（全部是 {_e(_fmt_num(c['min'][t]))}）" for c in consts]
            listing = "、".join(names)
            if len(names) > 10:
                listing = (f"<details><summary>{'、'.join(names[:10])}…等 {len(names)} 欄</summary>"
                           f"{listing}</details>")
            all_names = ", ".join(c["name"] for c in nulls + consts)
            period_rows.append(({"data-name": self.periods[t]}, [
                _td(_e(self.periods[t]), v=t), _td(str(len(nulls)), v=len(nulls), num=True),
                _td(str(len(consts)), v=len(consts), num=True), _td(listing, cls="wrap"),
                _td(f'<button type="button" data-copy-text="{_e(all_names)}">複製</button>'),
            ]))

        order = []
        for c in cols:
            nulls, consts = self._bad_periods(c)
            bad = sorted(set(nulls) | set(consts))
            r = self.ratios(c)
            rest = [v for t, v in enumerate(r) if t not in nulls]
            jump = null_jump(c, self.rows, self.thresholds) if nulls else None
            kind = "、".join(k for k, on in (("幾乎全空", nulls), ("只有一個值", consts)) if on)
            order.append(((bad[0] if bad else 0, c["name"]), (self.row_attrs(c), [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td(_e(kind)), _td(self.strip(c), v=bad[0] if bad else ""),
                _td(_e("、".join(self.periods[t] for t in bad)), cls="wrap"),
                _td(_e(_pct_range(rest)) if nulls and rest else ""),
                _td_num(None if jump is None else round(jump * 100, 1),
                        "" if jump is None else f"{jump * 100:.1f}"),
                _td(_e(self.other_problems(c, "upstream"))),
            ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        return _section("upstream", f"回頭查上游（{len(cols)} 欄、{len(by_period)} 期）", (
            _p(f"平常有值、只有某幾期幾乎全空（比其他期別高 {jump_txt}以上），或某幾期全部是同一個值。"
               "通常是那幾期上游沒有進資料，或填了預設值。上游一個來源通常對一批欄位，所以先看期別。")
            + "<h3>要回頭查的期別</h3>"
            + _table("t-periods", [("期別", False), ("幾乎全空", True), ("只有一個值", True),
                                   ("欄位", False), ("", False)], period_rows, sort=(0, "asc"))
            + "<h3>各欄位的依據</h3>"
            + _toolbar("t-upstream")
            + _table("t-upstream", [("欄位", False), ("型別", False), ("問題", False), ("各期", False),
                                    ("出事的期別", False), ("其他期別的空值比例", False),
                                    ("跳幅（百分點）", True), ("其他問題", False)],
                     rows, sort=(3, "asc"))
            + '<p class="note">依第一個出事的期別排序，同一期斷掉的欄位會排在一起。各期那一格的顏色同〈各期空值比例〉。'
            "跳幅＝出事那幾期最低的空值比例 − 其他期別最高的空值比例，例如 100% − 4.0% ＝ 96。</p>"
        ))

    # --- dataset 會擋下 ------------------------------------------------------

    def _beyond(self, col: dict) -> list[int]:
        """超過上限的期別。"""
        out = []
        for t, (lo, hi) in enumerate(zip(col["min"], col["max"])):
            vals = [abs(v) for v in (lo, hi) if v is not None]
            if vals and over_limit(max(vals), col, self.thresholds):
                out.append(t)
        return out

    def _beyond_text(self, col: dict) -> tuple[str, int]:
        ts = self._beyond(col)
        n = len(self.periods)
        if len(ts) == 1:
            return f"1／{n}（{self.periods[ts[0]]}）", 1
        return f"{len(ts)}／{n}", len(ts)

    def blocked(self) -> str:
        cols = self.with_action("blocked")
        if not cols:
            return _section("blocked", "dataset 會擋下（0 欄）", _none())
        order = []
        for c in cols:
            m, at = max_abs(c)
            limit = precision_limit(c, self.thresholds)
            text, n_beyond = self._beyond_text(c)
            times = m / limit
            order.append((-times, (self.row_attrs(c, **{"data-dtype": _dtype_group(c["dtype"])}), [
                _td_name(c["name"]), _td(_e(c["dtype"])), _td_num(m), _td(_e(self.periods[at])),
                _td_num(limit), _td_num(round(times, 2), f"{times:.2f}"), _td(_e(text), v=n_beyond, num=True),
                _td(_e(_fmt_err(float32_max_error(m))), v=float32_max_error(m), num=True),
                _td(_e(self.other_problems(c, "blocked"))),
            ])))
        rows = [row for _, row in sorted(order, key=lambda x: x[0])]
        has_decimal_type = any(c["dtype"].startswith("decimal") for c in cols)
        return _section("blocked", f"dataset 會擋下（{len(cols)} 欄）", (
            _p("整數或 decimal 欄位的絕對值最大超過上限，存成 float32 會被改掉。"
               "<code>dataset.numeric_precision_policy</code> 預設是 <code>block</code>，dataset 會跑失敗。"
               "處理方式，成本由低到高：")
            + "<ul class='desc'><li>上游改單位（例如改用千元）並四捨五入成整數，縮小的倍數至少要到「超過上限幾倍」。"
            "縮小後還留小數就沒用：小數位數越多，上限越低。</li>"
            "<li><code>dataset.numeric_feature_storage_type: float64</code>：所有特徵欄的空間加倍，"
            "也會換 <code>base_dataset_version</code>。</li>"
            "<li><code>dataset.numeric_precision_policy: truncate</code>：接受失真。</li></ul>"
            + _toolbar("t-blocked", selects=(("dtype", "型別", [("", "全部"), ("int", "整數"),
                                                              ("decimal", "decimal")], "", "token"),))
            + _table("t-blocked", [("欄位", False), ("型別", False), ("絕對值最大", True), ("出現期別", False),
                                   ("上限", True), ("超過上限幾倍", True), ("超過上限的期數", True),
                                   ("存成 float32 最大誤差", True), ("其他問題", False)],
                     rows, sort=(5, "desc"))
            + '<p class="note">只有一期超過上限，比較可能是一筆異常值，先查那一期；每一期都超過，是型別或單位的問題。'
            "各期的最大值點欄名看。"
            + ("decimal 欄位的上限是這支工具的門檻，dataset 依宣告的小數位數另算上限，"
               "以 dataset 跑出來的精度報告為準。" if has_decimal_type else "")
            + "</p>"
        ))

    # --- 自行判斷 -----------------------------------------------------------

    def judge_section(self) -> str:
        cols = self.with_action("judge")
        if not cols:
            return _section("judge", "自行判斷（0 欄）", _none())
        th = self.thresholds["null_ratio"]
        sparse, each = [], []
        for c in cols:
            jump = null_jump(c, self.rows, self.thresholds) if "null_some" in self.flags[c["name"]] else None
            if jump is not None and jump < UPSTREAM_JUMP_MIN:
                sparse.append(c)
            if "const_each" in self.flags[c["name"]]:
                each.append(c)
        parts = [_p("不一定有錯，要看業務意義決定留不留。")]
        parts.append(f"<h3>平常就很空、偶爾越過 {_e(_pct_threshold(th))} 的欄位（{len(sparse)} 欄）</h3>")
        if sparse:
            order = []
            for c in sparse:
                r = self.ratios(c)
                valued = [n - nl for n, nl in zip(self.rows, c["null"])]
                over = sum(v >= th for v in r)
                order.append((-min(r), (self.row_attrs(c), [
                    _td_name(c["name"]), _td(_e(c["dtype"])), _td(self.strip(c), v=min(r)),
                    _td(_e(_pct_range(r)), v=min(r)), _td(f"{over}／{len(r)}", v=over, num=True),
                    _td(_e(f"{min(valued):,}～{max(valued):,}"), v=min(valued), num=True),
                    _td(_e(self.other_problems(c, "judge"))),
                ])))
            parts.append(_p("這些欄位不是某一期斷掉，是本來就稀疏，例如只有房貸戶才有房貸還款額。"
                            "要不要留，看有值的那些筆夠不夠模型學。"))
            parts.append(_toolbar("t-sparse"))
            parts.append(_table("t-sparse", [("欄位", False), ("型別", False), ("各期", False),
                                             ("空值比例", False), ("越過門檻的期數", True),
                                             ("每期有值的筆數", True), ("其他問題", False)],
                                [row for _, row in sorted(order, key=lambda x: x[0])], sort=(3, "desc")))
        else:
            parts.append(_none())
        parts.append(f"<h3>{_e(self.texts['const_each'][0])}（{len(each)} 欄）</h3>")
        if each:
            rows = []
            for c in sorted(each, key=lambda c: c["name"]):
                _, const = one_value_periods(c, self.rows)
                values = [c["min"][t] for t in const]
                rows.append((self.row_attrs(c), [
                    _td_name(c["name"]), _td(_e(c["dtype"])), _td_num(values[0]), _td_num(values[-1]),
                    _td_num(len(set(values))), _td(_e(self.other_problems(c, "judge"))),
                ]))
            parts.append(_p("同一期裡每一筆的值都相同，但各期不同，通常是匯率這類每期一個數字的總體指標。"))
            parts.append(_table("t-each", [("欄位", False), ("型別", False), ("最早一期的值", True),
                                           ("最新一期的值", True), ("不同的值個數", True), ("其他問題", False)],
                                rows, sort=(0, "asc")))
        else:
            parts.append(_none())
        return _section("judge", f"自行判斷（{len(cols)} 欄）", "".join(parts))

    # --- 可以不理 -----------------------------------------------------------

    def ignore(self) -> str:
        cols = self.with_action("ignore")
        summary = (f"double 欄位存成 float32 的誤差（{len(cols)} 欄）：dataset 不檢查 double，"
                   "對 LightGBM 幾乎沒有影響。點開看各欄最多差多少。")
        if not cols:
            return _section("ignore", "可以不理（0 欄）", _none())
        order = []
        for c in cols:
            m, at = max_abs(c)
            err = float32_max_error(m)
            text, n_beyond = self._beyond_text(c)
            order.append((-err, c["name"], (self.row_attrs(c), [
                _td_name(c["name"]), _td_num(m), _td(_e(self.periods[at])),
                _td(_e(_fmt_err(err)), v=err, num=True), _td(_e(text), v=n_beyond, num=True),
            ])))
        rows = [row for *_, row in sorted(order, key=lambda x: (x[0], x[1]))]
        return _section("ignore", f"可以不理（{len(cols)} 欄）", (
            _p("拿「最大誤差」和這個欄位在意的那一位比：以元計的金額要準到分（0.01），"
               "±0.0039 就不用理；±16 表示個位、十位都會變。")
            + _toolbar("t-ignore")
            + _table("t-ignore", [("欄位", False), ("絕對值最大", True), ("出現期別", False),
                                  ("存成 float32 最大誤差", True), ("超過門檻的期數", True)],
                     rows, sort=(3, "desc"))
        ), collapsed=summary)

    # --- 各期空值比例 -------------------------------------------------------

    def nulls(self) -> str:
        th = self.thresholds["null_ratio"]
        legend = ['<div class="legend"><span><i class="b0"></i>0</span>']
        edges = [0.0, *_null_cuts(th), th]
        for i in range(len(edges) - 1):
            label = (f"< {_pct_threshold(edges[1])}" if i == 0
                     else f"{_pct_threshold(edges[i])}～{_pct_threshold(edges[i + 1])}")
            legend.append(f'<span><i class="b{i + 1}"></i>{_e(label)}</span>')
        legend.append(f'<span><i class="bh"></i>≥ {_e(_pct_threshold(th))}，達到門檻</span></div>')

        order = []
        for c in self.feats:
            r = self.ratios(c)
            var = (max(r) - min(r)) * 100
            cells = [
                _td(f"<span>{_e(_pct(v))}</span>", v=v, cls=f"c {_null_class(v, th)}",
                    title=f"{c['name']}｜{p}｜空值比例 {_pct(v)}（{n:,} 筆中 {nl:,} 筆）")
                for p, v, n, nl in zip(self.periods, r, self.rows, c["null"])
            ]
            excl = "1" if "exclude" in self.actions[c["name"]] else "0"
            order.append((-var, c["name"], (self.row_attrs(c, **{"data-var": round(var, 3), "data-excl": excl}), [
                _td_name(c["name"]), _td(f"{var:.1f}", v=round(var, 3), num=True),
                _td(_e(_pct(min(r))), v=min(r), num=True), _td(_e(_pct(max(r))), v=max(r), num=True),
                *cells,
            ])))
        rows = [row for *_, row in sorted(order, key=lambda x: (x[0], x[1]))]
        head = [("欄位", False), ("變動幅度（百分點）", True), ("最低", True), ("最高", True)] + [
            (lab, True) for lab in self.labels]
        sel = (("var", "只看變動幅度 ≥",
                [(v, f"{v:g} 個百分點") for v in _VAR_CHOICES], _VAR_DEFAULT, "min"),)
        checks = (("excl", "隱藏直接排除的欄位", True), ("toggle", "格子裡顯示數字", False))
        return _section("nulls", "各期空值比例", (
            _p("找出哪一期空掉。預設只列各期空值比例有變動的欄位，依變動幅度排序；"
               "直接排除的欄位已經在上面列過，預設隱藏。點表頭可以改依任何一期排序，滑鼠移到格子上看筆數。"
               "沒有達到門檻、但某一期顏色深了一兩段的欄位，是規則抓不到的部分缺資料。")
            + "".join(legend)
            + _toolbar("t-nulls", selects=sel, checks=checks)
            + _table("t-nulls", head, rows, sort=(1, "desc"), cls="grid")
            + '<p class="note">變動幅度＝各期空值比例最高 − 最低，單位是百分點。'
            "選「0 個百分點」並勾「格子裡顯示數字」就是所有欄位的完整數字。</p>"
        ))

    # --- 全部欄位 -----------------------------------------------------------

    def all_columns(self) -> str:
        rows = []
        for i, c in enumerate(self.feats):
            r = self.ratios(c)
            acts = self.actions[c["name"]]
            numeric = c["kind"] == "numeric"
            lo = [v for v in c["min"] if v is not None]
            hi = [v for v in c["max"] if v is not None]
            vmin = min(lo) if numeric and lo else None
            vmax = max(hi) if numeric and hi else None
            dec = "—" if not numeric else ("是" if c["has_decimal"] else "否")
            rows.append(({"id": f"col-{c['name']}", "data-name": c["name"],
                          "data-action": " ".join(acts) or "ok", "data-dtype": _dtype_group(c["dtype"])}, [
                _td(str(i + 1), v=i, num=True),
                _td(f'<button type="button" class="exp">{_e(c["name"])}</button>', v=c["name"].lower(), cls="name"),
                _td(_e(c["dtype"])),
                _td(_e("、".join(ACTION_LABELS[a] for a in acts) or "—")),
                _td(_e(self.flag_names(c) or "—"), cls="wrap"),
                _td(_e(_pct_range(r)), v=max(r)),
                _td_num(vmin), _td_num(vmax), _td(dec),
            ]))
        actions_opt = [("", "全部")] + [(a, ACTION_LABELS[a]) for a in ACTION_ORDER] + [("ok", "沒有發現問題")]
        types_opt = [("", "全部"), ("int", "整數"), ("decimal", "decimal"), ("double", "double"),
                     ("float", "float"), ("string", "字串")]
        data = {"periods": self.periods, "rows": self.rows,
                "cols": {c["name"]: [c["null"], c["min"], c["max"]] for c in self.feats}}
        # </ 會提早結束 <script>，JSON 裡的字串（欄名、字串欄位的值）要先換掉
        blob = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
        return _section("all", f"全部欄位（{len(self.feats)} 欄）", (
            _p("查單一欄位或一整族欄位（例如篩選 <code>cc_</code>）。點欄名展開各期的總筆數、空值筆數、最小值、最大值；"
               "最小值、最大值是所有期別合計。預設依表裡的欄位順序。")
            + _toolbar("t-all", selects=(("action", "要做的事", actions_opt, "", "token"),
                                       ("dtype", "型別", types_opt, "", "token")))
            + _table("t-all", [("#", True), ("欄位", False), ("型別", False), ("要做的事", False), ("問題", False),
                             ("空值比例", False), ("最小值", True), ("最大值", True), ("有小數", False)],
                     rows, sort=(0, "asc"))
            + f'<script type="application/json" id="col-data">{blob}</script>'
        ))

    # --- 資料範圍與判斷標準 -------------------------------------------------

    def scope(self) -> str:
        cfg = self.profile["config"]
        code = lambda s: f"<code>{_e(s)}</code>"  # noqa: E731
        per = (f"{min(self.rows):,}～{max(self.rows):,} 筆，全部納入計算" if self.rows else "無")
        kv = [
            ("資料表", code(cfg["table"])),
            ("期別欄位", code(cfg["time_col"])),
            ("主鍵欄位", "、".join(code(c) for c in cfg["key_cols"])),
            ("排除欄位", "、".join(code(c) for c in cfg["exclude_cols"]) or "無"),
            ("期間", (f"{_e(self.periods[0])}～{_e(self.periods[-1])}，共 {len(self.periods)} 期"
                      if self.periods else "無")),
            ("篩選條件", code(cfg["where"]) if cfg["where"] else "無"),
            ("每期筆數", per),
            ("期別欄位是空值的列", f"{self.profile['null_time_rows']:,} 筆，不讀"),
            ("執行項目", "基本檢查"),
            ("執行時間", f"{_duration(self.profile['elapsed_seconds'])}，每批 {self.profile['batch_size']} 欄"),
            ("產出時間", _e(self.profile["generated_at"].replace("T", " "))),
        ]
        kv_html = ('<div class="tbox"><table class="t kv"><tbody>'
                   + "".join(f"<tr><th>{_e(k)}</th><td>{v}</td></tr>" for k, v in kv)
                   + "</tbody></table></div>")
        th = self.thresholds
        th_rows = [
            ({}, [_td(_e(f"{self.texts['null_all'][0]}、{self.texts['null_some'][0]}")),
                  _td(_e(f"空值比例 ≥ {_pct_threshold(th['null_ratio'])}")), _td(code("null_ratio")),
                  _td(_e(th["null_ratio"]))]),
            ({}, [_td(_e(self.texts["precision"][0])), _td(_e(self.texts["precision"][1]), cls="wrap"),
                  _td(code("precision_max_abs")), _td(_e(th["precision_max_abs"]))]),
        ]
        note = "可以在設定檔的 <code>thresholds</code> 區塊調整，再用 <code>render</code> 重畫，不必重跑 Spark。"
        if self.threshold_source:
            note += f"這次的值取自重畫時指定的設定檔 {code(self.threshold_source)}。"
        unchecked = (_table("t-unchecked", [("欄位", False), ("型別", False)],
                            [({"data-name": c["name"]}, [_td(_e(c["name"]), cls="name"), _td(_e(c["dtype"]))])
                             for c in self.unchecked])
                     if self.unchecked else _none())
        return _section("scope", "資料範圍與判斷標準", (
            kv_html
            + "<h3>判斷標準</h3>" + _p(note)
            + _table("t-thresholds", [("問題類型", False), ("判斷標準", False), ("設定鍵", False),
                                      ("這次的值", False)], th_rows)
            + f'<p class="note">「回頭查上游」與「自行判斷」的分界（跳幅 {UPSTREAM_JUMP_MIN * 100:g} 個百分點）'
            "只決定分組，寫在程式裡，不是設定鍵。</p>"
            + "<h3>未檢查的欄位</h3>"
            + _p("型別不支援（date、timestamp、array、map、struct 等）而沒有檢查的欄位。")
            + unchecked
            + "<details><summary><h3 style='display:inline'>設定檔</h3></summary>"
            + f"<pre>{_e(self.profile['config_text'])}</pre></details>"
        ))

    def page(self) -> str:
        table = self.profile["config"]["table"]
        main = "".join([self.overview(), self.exclude(), self.upstream(), self.blocked(),
                        self.judge_section(), self.ignore(), self.nulls(), self.all_columns(), self.scope()])
        return (
            "<!DOCTYPE html><html lang='zh-Hant'><head><meta charset='utf-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'>"
            f"<title>特徵表檢查報告：{_e(table)}</title><style>{_CSS}</style></head><body>"
            f"{self.header()}<div class='layout'>{self.toc()}<main>{main}</main></div>"
            f"<script>{_JS}</script></body></html>"
        )


def write_report(profile: dict, out_dir: Path, thresholds: dict[str, float],
                 threshold_source: str | None) -> Path:
    """依 ``thresholds`` 判斷後畫成單一頁面 ``index.html``，回傳它的路徑。"""
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
    profile = profile_table(spark, cfg, batch_size)

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
