"""特徵表檢查工具：算一張特徵表每一期、每一欄的統計量，標出要處理的欄位。

設計文件：``docs/notes/2026-10-15-feature-table-profiling-design.md``；使用說明：
``docs/operations/user-guides/feature-table-profiling.md``。這一版是第一階段，
只有基本檢查（設計文件第五節的空值與「只有一個值」五種問題）；分布偏移與極端值、
相關性是後兩階段，所以設定檔還沒有 ``checks`` 區塊，``thresholds`` 也只認這一階段
用得到的鍵。

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
（不用 ``recsys_tfb.report``）：要排序、篩選、複製欄名、頁內目錄，共用的多頁外框
給不了，改它又會連帶改到 evaluation 的診斷頁。
"""

from __future__ import annotations

import datetime as dt
import html
import json
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

DEFAULT_THRESHOLDS: dict[str, float] = {"null_ratio": 0.99}
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

FLAG_ORDER = ("null_all", "const_all", "null_some", "const_some", "const_each")
FLAG_NAMES = {
    "null_all": "每一期都幾乎是空值",
    "null_some": "部分期別幾乎是空值",
    "const_all": "整欄只有一個值",
    "const_some": "部分期別只有一個值",
    "const_each": "每期各只有一個值",
}
NULL_FLAGS = ("null_all", "null_some")
CONST_FLAGS = ("const_all", "const_some", "const_each")


def null_ratios(col: dict, rows: list[int]) -> list[float]:
    return [nl / n for nl, n in zip(col["null"], rows)]


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
        return flags + ["const_each"]
    if const:
        flags.append("const_some")
    return flags


def judge(profile: dict, thresholds: dict[str, float]) -> dict[str, list[str]]:
    return {
        c["name"]: judge_column(c, profile["rows"], thresholds)
        for c in profile["columns"]
        if c["kind"] != "unchecked"
    }


# ---------------------------------------------------------------------------
# 行動建議：把判斷結果整理成總結裡的三種做法（render 換門檻時一起重算）
# ---------------------------------------------------------------------------

# 「部分期別幾乎是空值」的欄位，出事那幾期比其他期別至少高這麼多（0.10＝10 個百分點）
# 才算上游斷掉；不到的是平常就很稀疏、偶爾越過門檻的欄位，歸「看業務意義決定」。
# 只決定總結裡的分組，不改判斷，所以不進 thresholds。
UPSTREAM_JUMP_MIN = 0.10

ACTION_ORDER = ("exclude", "upstream", "judge")
ACTION_TEXTS = {
    "exclude": "可以排除：欄位沒有資訊",
    "upstream": "請上游查這幾期的資料",
    "judge": "看業務意義決定要不要留",
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
    """這一欄在總結裡要做的事，依 ``ACTION_ORDER``；沒有問題回空清單。

    一欄可能要做兩件事（例如某一期斷掉、又有幾期只有一個值），兩件都列。
    """
    if {"null_all", "const_all"} & set(flags):
        return ["exclude"]
    out = []
    jump = null_jump(col, rows, thresholds) if "null_some" in flags else None
    if "const_some" in flags or (jump is not None and jump >= UPSTREAM_JUMP_MIN):
        out.append("upstream")
    if "const_each" in flags or (jump is not None and jump < UPSTREAM_JUMP_MIN):
        out.append("judge")
    return out


# ---------------------------------------------------------------------------
# 報告：單一頁面，表格可以排序、篩選、複製欄名（純手寫 JS，不連網、不加套件）
# ---------------------------------------------------------------------------

_VAR_CHOICES = (0, 0.5, 1, 5, 10)  # 〈有變動但沒達門檻〉的篩選選項，百分點
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
--track:#eef1f4;--bar:#8fa7bf;--grey:#b8c4d0;--hit:#c2410c}
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
table.t th{position:sticky;top:0;z-index:2;background:var(--soft);font-weight:600;cursor:pointer;user-select:none}
table.t th[data-dir=asc]::after{content:" ▲";color:var(--muted)}
table.t th[data-dir=desc]::after{content:" ▼";color:var(--muted)}
table.t .n{text-align:right}
table.t tbody tr:hover td{background:#f3f8ff}
table.kv th{width:12em;cursor:default}
table.actions td{vertical-align:top;padding:8px 10px}
ul.periods{margin:6px 0 0;padding-left:18px;font-weight:normal;font-size:13px}
ul.periods li{margin:2px 0}ul.periods button{margin-left:6px}
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
.spark i.on{outline:1px solid #111}
.spark.key{width:auto;cursor:default;margin-right:4px}.spark.key i{width:6px;flex:none}
.tip{position:fixed;z-index:50;background:#1f2328;color:#fff;font-size:12px;padding:3px 8px;border-radius:4px;pointer-events:none;white-space:nowrap}
tr.detail td{background:var(--soft);white-space:normal}
table.mini{border-collapse:collapse;margin:4px 0 8px;font-size:12px}
table.mini th,table.mini td{padding:2px 10px;border-bottom:1px solid var(--line);text-align:right}
table.mini th:first-child,table.mini td:first-child{text-align:left}
button.exp{border:0;background:none;padding:0;color:var(--link);font-family:inherit;font-size:13px}
button.exp::before{content:"▸ ";color:var(--muted)}tr.open button.exp::before{content:"▾ "}
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
 if(b.hasAttribute("data-copy-rows")){var t=document.getElementById(b.getAttribute("data-copy-rows"));
  copy(rowsOf(t).filter(function(r){return !r.hidden;}).map(function(r){return r.getAttribute("data-name");}).join("\n"),b);}
 else if(b.hasAttribute("data-copy-text")){copy(b.getAttribute("data-copy-text"),b);}
 else if(b.classList.contains("exp")){toggle(b.closest("tr"));}});
var DATA=JSON.parse((document.getElementById("col-data")||{}).textContent||"{}");
function fmt(v){if(v===null||v===undefined)return "";if(typeof v==="number"){
 return Number.isInteger(v)?v.toLocaleString("en-US"):v.toLocaleString("en-US",{maximumFractionDigits:6});}return String(v);}
function pct(n,d){if(!d)return "";if(n===0)return "0%";if(n===d)return "100%";var x=n/d;
 var s=(x*100).toFixed(x<0.01||x>0.99?2:1);if(Number(s)>=100)return "> 99.99%";if(Number(s)===0)return "< 0.01%";return s+"%";}
function toggle(tr){if(!tr)return;var name=tr.getAttribute("data-name"),c=DATA.cols&&DATA.cols[name];if(!c)return;
 if(!tr._detail){var h="<table class='mini'><thead><tr><th>期別</th><th>總筆數</th><th>空值筆數</th><th>空值比例</th><th>最小值</th><th>最大值</th></tr></thead><tbody>";
  DATA.periods.forEach(function(p,i){h+="<tr><td>"+esc(p)+"</td><td>"+fmt(DATA.rows[i])+"</td><td>"+fmt(c[0][i])+"</td><td>"+pct(c[0][i],DATA.rows[i])+
   "</td><td>"+esc(fmt(c[1][i]))+"</td><td>"+esc(fmt(c[2][i]))+"</td></tr>";});
  var d=document.createElement("tr");d.className="detail";d.innerHTML="<td colspan='"+tr.cells.length+"'>"+h+"</tbody></table></td>";
  tr._detail=d;tr.parentNode.insertBefore(d,tr.nextSibling);tr.classList.add("open");return;}
 var open=!tr.classList.contains("open");tr.classList.toggle("open",open);tr._detail.hidden=!open;}
function esc(s){return String(s).replace(/[&<>"']/g,function(ch){return "&#"+ch.charCodeAt(0)+";";});}
document.addEventListener("click",function(ev){var a=ev.target.closest("a.col");if(!a)return;
 var tr=document.getElementById("col-"+a.getAttribute("data-col"));if(!tr)return;ev.preventDefault();
 var bar=document.querySelector('.toolbar[data-for="t-all"]');
 if(bar){bar.querySelectorAll("input.q").forEach(function(q){q.value="";});bar.querySelectorAll("select").forEach(function(s){s.selectedIndex=0;});apply(bar);}
 if(!tr.classList.contains("open"))toggle(tr);tr.scrollIntoView({block:"center"});});
function tipText(s,i){var c=DATA.cols&&DATA.cols[s.getAttribute("data-col")],p=DATA.labels[i];if(!c)return p;
 var n=DATA.rows[i],nl=c[0][i];
 if(s.getAttribute("data-mode")==="const"){if(nl===n)return p+"　整期都是空值";
  return c[1][i]===c[2][i]?p+"　只有一個值："+fmt(c[1][i]):p+"　"+fmt(c[1][i])+"～"+fmt(c[2][i]);}
 return p+"　空值 "+pct(nl,n)+"（"+fmt(n)+" 筆中 "+fmt(nl)+" 筆）";}
var tip=document.createElement("div");tip.className="tip";tip.hidden=true;document.body.appendChild(tip);var lastOn=null;
document.addEventListener("mousemove",function(ev){var s=ev.target.closest(".spark");
 if(lastOn&&(!s||lastOn.parentNode!==s)){lastOn.classList.remove("on");lastOn=null;}
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
        return (max(r) - min(r)) * 100

    def varying(self) -> list[dict]:
        """〈有變動但沒達門檻〉的候選：空值兩種都沒被標到的欄位。"""
        return [c for c in self.feats if not set(NULL_FLAGS) & set(self.flags[c["name"]])]

    def periods_text(self, ts: list[int]) -> tuple[str, str]:
        """（顯示文字、完整清單）。超過三期只寫前兩期和總數，完整清單放滑鼠提示。"""
        full = "、".join(self.labels[t] for t in ts)
        if len(ts) <= 3:
            return full, full
        return f"{'、'.join(self.labels[t] for t in ts[:2])}…共 {len(ts)} 期", full

    def spark_null(self, col: dict) -> str:
        """各期空值比例的小長條圖：一期一根，高度＝空值比例，達門檻橘色。"""
        bars = "".join(
            f'<i{" class=h" if r >= self.null_th else ""} style="--h:{r * 100:.1f}%"></i>'
            for r in self.ratios(col)
        )
        return f'<span class="spark" data-mode="null" data-col="{_e(col["name"])}">{bars}</span>'

    def spark_const(self, col: dict) -> str:
        """各期是不是只有一個值：橘＝只有一個值，灰＝有變化，空白＝整期都是空值。"""
        valued, const = one_value_periods(col, self.rows)
        cells = []
        for t in range(len(self.periods)):
            if t in const:
                cells.append('<i class="h" style="--h:100%"></i>')
            elif t in valued:
                cells.append('<i class="g" style="--h:100%"></i>')
            else:
                cells.append('<i style="--h:0%"></i>')
        return f'<span class="spark" data-mode="const" data-col="{_e(col["name"])}">{"".join(cells)}</span>'

    def null_legend(self) -> str:
        return _spark_legend([("", f"沒達 {self.th_text}，越高越空"), ("h", f"≥ {self.th_text}，達門檻")],
                             "一期一根，由左到右是最早到最新；滑鼠移到圖上看那一期的數字")

    # --- 頁首與目錄 ---------------------------------------------------------

    def header(self) -> str:
        cfg = self.profile["config"]
        span = (f"{_e(cfg['time_col'])} {_e(self.labels[0])}～{_e(self.labels[-1])}，共 {len(self.periods)} 期"
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
        rows = [
            ({}, [_td('<a href="#s-null">空值</a>'), _td(str(n_null), v=n_null, num=True),
                  _td(_e(null_detail), cls="wrap")]),
            ({}, [_td('<a href="#s-const">只有一個值</a>'), _td(str(n_const), v=n_const, num=True),
                  _td(_e(const_detail), cls="wrap")]),
            ({}, [_td('<a href="#s-all">沒有發現問題</a>'), _td(str(ok), v=ok, num=True), _td("—")]),
            ({}, [_td('<a href="#s-scope">未檢查</a>'),
                  _td(str(len(self.unchecked)), v=len(self.unchecked), num=True),
                  _td("型別不支援，例如 date、timestamp", cls="wrap")]),
        ]
        return _section("s-overview", "總覽", (
            _p(f"這份報告檢查 <code>{_e(self.profile['config']['table'])}</code> 的 {len(self.feats)} 個特徵欄位"
               f"（數值 {n_num} 欄、字串 {len(self.feats) - n_num} 欄），每一期看兩件事：空值多不多、"
               "是不是只有一個值。下面依檢視項目分段，最後的〈行動建議總結〉把結果整理成可以照做的清單。"
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
            + self.const_all() + self.const_some() + self.const_each()
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
                "cols": {c["name"]: [c["null"], c["min"], c["max"]] for c in self.feats}}
        # </ 會提早結束 <script>，JSON 裡的字串（欄名、字串欄位的值）要先換掉
        blob = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
        return _section("s-all", f"全部欄位（{len(self.feats)} 欄）", (
            _p("查單一欄位或一整族欄位（例如篩選 <code>cc_</code>）。點欄名展開各期的總筆數、空值筆數、"
               "最小值、最大值。表上的最小值、最大值是所有期別合計，只列數值欄位。預設依表裡的欄位順序。")
            + _toolbar("t-all", selects=(("flag", "問題", flag_opt, "", "token"),
                                       ("dtype", "型別", type_opt, "", "token")))
            + _table("t-all", [("#", True), ("欄位", False), ("型別", False), ("問題", False), ("各期空值", False),
                               ("空值比例", False), ("最小值", True), ("最大值", True)], rows, sort=(0, "asc"))
            + f'<script type="application/json" id="col-data">{blob}</script>'
        ))

    # --- 行動建議總結 -------------------------------------------------------

    def upstream_periods(self) -> dict[int, list[str]]:
        """要請上游查的期別 → 欄名，只算這一欄被歸到「請上游查」的那種問題。"""
        out: dict[int, list[str]] = {}
        for c in self.with_action("upstream"):
            flags = self.flags[c["name"]]
            jump = null_jump(c, self.rows, self.thresholds) if "null_some" in flags else None
            ts = self.bad_null(c) if jump is not None and jump >= UPSTREAM_JUMP_MIN else []
            if "const_some" in flags:
                ts = sorted(set(ts) | set(one_value_periods(c, self.rows)[1]))
            for t in ts:
                out.setdefault(t, []).append(c["name"])
        return dict(sorted(out.items()))

    def action_summary(self) -> str:
        jump_txt = f"{UPSTREAM_JUMP_MIN * 100:g} 個百分點"
        sources = {
            "exclude": f"{FLAG_NAMES['null_all']}、{FLAG_NAMES['const_all']}",
            "upstream": f"{FLAG_NAMES['null_some']}（跳幅 ≥ {jump_txt}）、{FLAG_NAMES['const_some']}",
            "judge": f"{FLAG_NAMES['null_some']}（跳幅 < {jump_txt}）、{FLAG_NAMES['const_each']}",
        }
        rows = []
        for key in ACTION_ORDER:
            cols = self.with_action(key)
            listing = "\n".join(c["name"] for c in cols)
            extra = ""
            if key == "upstream" and cols:
                items = "".join(
                    f'<li><b>{_e(self.labels[t])}</b>　{len(names)} 欄：{_e("、".join(names[:6]))}'
                    f'{"…" if len(names) > 6 else ""}'
                    f' <button type="button" data-copy-text="{_e(chr(10).join(names))}">複製這期的欄名</button></li>'
                    for t, names in self.upstream_periods().items()
                )
                extra = f'<ul class="periods">{items}</ul>'
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
               "例如某一期幾乎全空、又有另一期每期各只有一個值。")
            + _table("t-actions", [("做法", False), ("欄數", True), ("來自", False), ("欄位", False),
                                   ("", False)], rows, cls="actions")
            + _note(f"「請上游查」和「看業務意義」的分界是跳幅 {jump_txt}：出事那幾期比其他期別空很多，"
                    "才像是上游斷掉。")
        ))

    # --- 資料範圍與判斷標準 -------------------------------------------------

    def scope(self) -> str:
        cfg = self.profile["config"]
        code = lambda s: f"<code>{_e(s)}</code>"  # noqa: E731
        span = (f"{_e(self.periods[0])}～{_e(self.periods[-1])}，共 {len(self.periods)} 期"
                if self.periods else "無")
        kv = [
            ("資料表", code(cfg["table"])),
            ("期別欄位", code(cfg["time_col"])),
            ("主鍵欄位", "、".join(code(c) for c in cfg["key_cols"])),
            ("排除欄位", "、".join(code(c) for c in cfg["exclude_cols"]) or "無"),
            ("期間", span),
            ("篩選條件", code(cfg["where"]) if cfg["where"] else "無"),
            ("每期筆數", f"{min(self.rows):,}～{max(self.rows):,} 筆，全部納入計算" if self.rows else "無"),
            ("期別欄位是空值的列", f"{self.profile['null_time_rows']:,} 筆，不讀"),
            ("執行項目", "基本檢查"),
            ("執行時間", f"{_duration(self.profile['elapsed_seconds'])}，每批 {self.profile['batch_size']} 欄"),
            ("產出時間", _e(self.profile["generated_at"].replace("T", " "))),
        ]
        kv_html = ('<div class="tbox"><table class="t kv"><tbody>'
                   + "".join(f"<tr><th>{_e(k)}</th><td>{v}</td></tr>" for k, v in kv)
                   + "</tbody></table></div>")
        th_rows = [({}, [
            _td(_e(f"{FLAG_NAMES['null_all']}、{FLAG_NAMES['null_some']}")),
            _td(_e(f"空值比例 ≥ {self.th_text}")), _td(code("null_ratio")), _td(_e(self.null_th)),
        ])]
        note = "可以在這支工具設定檔的 <code>thresholds</code> 區塊調整，再用 <code>render</code> 重畫，不必重跑 Spark。"
        if self.threshold_source:
            note += f"這次的值取自重畫時指定的設定檔 {code(self.threshold_source)}。"
        unchecked = (_table("t-unchecked", [("欄位", False), ("型別", False)],
                            [({"data-name": c["name"]}, [_td(_e(c["name"]), cls="name"), _td(_e(c["dtype"]))])
                             for c in self.unchecked])
                     if self.unchecked else _none())
        return _section("s-scope", "資料範圍與判斷標準", (
            kv_html
            + "<h3>判斷標準</h3>" + _p(note)
            + _table("t-thresholds", [("問題類型", False), ("判斷標準", False), ("設定鍵", False),
                                      ("這次的值", False)], th_rows)
            + _note("「只有一個值」的三種沒有門檻。總結裡「請上游查」和「看業務意義」的分界"
                    f"（跳幅 {UPSTREAM_JUMP_MIN * 100:g} 個百分點）寫在程式裡，不是設定鍵。")
            + "<h3>未檢查的欄位</h3>"
            + _p("型別不支援（date、timestamp、array、map、struct 等）而沒有檢查的欄位。")
            + unchecked
            + f"<details><summary><b>設定檔</b></summary><pre>{_e(self.profile['config_text'])}</pre></details>"
        ))

    def page(self) -> str:
        table = self.profile["config"]["table"]
        main = "".join([self.overview(), self.nulls(), self.consts(), self.all_columns(),
                        self.action_summary(), self.scope()])
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
