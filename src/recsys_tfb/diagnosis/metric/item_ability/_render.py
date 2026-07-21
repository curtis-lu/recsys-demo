"""item_ability 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``config_shift/_render.py``（見該檔 docstring 的完整理由）：純函式、
不 import pyspark、不讀檔、不做任何計算——出現在報表上的每個數字都必須已經
在 JSON 裡。停用時回空 tuple；最後一節固定是「本次執行的完整性檢查」。

**版面（2026-07-21 依「診斷報表呈現原則」重排，與 suppression 同一套邏輯）**：
整份診斷圍繞一個基礎量 ``auc_gap_raw_minus_centered`` ＝ raw AUC − query-centered
AUC。頂部兩塊定向：概覽（規模＋關鍵數字＋如何讀）→ 核心概念（把兩個 AUC、
centering、與它們的差講清楚一次），後面每區都是這組數字的不同切面：

* **概覽**——一句話目的、規模、macro per-item mAP、外加「想回答什麼問題 →
  看哪一區」的中性導覽。
0. **核心概念**——within-item AUC 是什麼（隨機抽一正一負、正例分數較高的機率）、
   raw 與 query-centered 的唯一差別（centering 移除客戶整體分數水準）、兩者之差
   auc_gap 的方向意義，用本次資料裡 |gap| 最大的 item 走一遍。
1. **raw vs query-centered 散點**——把 auc_gap 畫成幾何（點沿 y 軸偏離對角線的
   落差）＋逐 item 精確值表（raw／centered／gap）。
2. **逐 item AUC 的信賴區間**——raw／centered 各一張條圖，含 95% CI 與 0.500 線。
3. **query-centered AUC vs AP**——能力 proxy 與實際排序指標對不對得上。
4. **正例名次分布**——heatmap，每格是名次（1＝排最前），非百分位。
5. **完整性檢查**（固定殿後）。

**兩個 AUC 的定義在核心概念講一次**，各區只補自己專屬的數字定義（集中在
:data:`_DEFS`，單一真實來源）。

**gap 保留正負號**：方向本身就是「客戶整體分數水準往哪個方向影響 raw AUC」的
訊號（見 ``_compute.py`` 同一句警語），取絕對值只有方向測試守得住。

**AUC 有缺席的 item 不強行畫成 0**：``raw_within_item_auc``／``query_centered_auc``
在該 item 正例或負例列數為 0 時是 ``None``；這裡一律先過濾，缺席的 item 不進圖，
但完整性檢查那一節會點名它們——「缺席」與「量到、剛好是某個值」在報表上必須
長得不一樣。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import (
    MAX_FIGURE_POINTS, bar, fits_budget, heatmap, scatter,
)
from recsys_tfb.report.fmt import fmt_ap, fmt_auc, fmt_count, fmt_delta, fmt_mean

_ITEM = "item"
_RAW_AUC = "raw_within_item_auc"
_CENTERED_AUC = "query_centered_auc"
_RAW_CI_LOW = "raw_within_item_auc_ci_low"
_RAW_CI_HIGH = "raw_within_item_auc_ci_high"
_CENTERED_CI_LOW = "query_centered_auc_ci_low"
_CENTERED_CI_HIGH = "query_centered_auc_ci_high"
_GAP = "auc_gap_raw_minus_centered"
_AP = "ap"

#: 隨機打散正負例的 AUC 期望值——唯一的對照點。within-item AUC（正負例都在
#: 同一 item 內、同分半分）底下，任何常數分數都恆等於 0.500，所以「item 全域
#: 購買率 baseline」與它逐位元重合、刻意不另列（2026-07-20 實測撤除）。
_RANDOM_AUC = 0.500

#: 名次分布 heatmap 的欄（百分位）：p10 較靠前、p90 較靠後。比舊版多一欄
#: （p10），且值是名次不是百分位（使用者回饋：0.125 讀不出是「排第 1」）。
_RANK_LEVELS: list[tuple[str, str]] = [
    ("p10_positive_rank", "p10（較前）"),
    ("p25_positive_rank", "p25"),
    ("median_positive_rank", "中位數"),
    ("p75_positive_rank", "p75"),
    ("p90_positive_rank", "p90（較後）"),
]

_DEFS_TITLE = "本區數字定義"

#: 數字 → 一句定義。集中維護、各區各自取用（見 :func:`_defs_table`）：定義貼在
#: 數字旁邊，而非頁首一張總表。兩個 AUC 與 gap 一律回指開頭「核心概念」。
_DEFS: dict[str, str] = {
    "within-item AUC":
        "隨機抽該 item 的一個正例列與一個負例列，模型給正例較高分的機率"
        "（同分各算半個、每列依 inclusion_weight 加權）。0.500＝正負例分數"
        "完全重疊、分不出誰會買；完整說明見開頭「核心概念」。",
    "raw AUC":
        "在原始 logit 分數上算的 within-item AUC，未扣 query 平均——混入了"
        "客戶整體分數水準（見核心概念）。",
    "query-centered AUC":
        "先把每個 query 的平均 logit 分數扣掉、再算的 within-item AUC——移除"
        "客戶整體分數水準，只留 item 在 query 內的相對排序能力（見核心概念）。",
    "auc_gap_raw_minus_centered":
        "raw AUC − query-centered AUC（基礎量，見核心概念）。正值＝raw 被"
        "「買家整體分數較高」撐高、負值＝相反；方向固定、不取絕對值。",
    "AP":
        "該 item 在它自己的正例列上的 average precision（依 evaluation.metric "
        "的 k／shrinkage 設定）；越高＝正例越常被排在前面。",
    "n_pos": "該 item 的正例列數（label=1 的列數）。",
    "median pos rank":
        "該 item 正例在各自 query 內名次（1＝排最前）的中位數；名次的分母是"
        "每 query 的候選數（見完整性檢查），名次越大＝越靠後。",
}


def _defs_table(*names: str) -> pd.DataFrame:
    return pd.DataFrame(
        [{"數字": n, "定義": _DEFS[n]} for n in names],
        columns=["數字", "定義"],
    )


def _rows_with(per_item: list[dict], *keys: str) -> list[dict]:
    """只保留指定鍵全部非 ``None`` 的列。

    AUC 缺席（該 item 正例或負例列數為 0）不強行畫成 0——那會讓「量不到」
    看起來像「量到了、結果是 0」，兩者在報表上必須長得不一樣。
    """
    return [r for r in per_item if all(r.get(k) is not None for k in keys)]


def _candidates_display(result: dict) -> str:
    """每 query 候選數（名次的分母）的人話版；min==max＝固定候選數。"""
    cpq = result.get("candidates_per_query") or {}
    lo, hi, med = cpq.get("min"), cpq.get("max"), cpq.get("median")
    if lo is None:
        return "—"
    if lo == hi:
        return f"固定 {fmt_count(lo)} 個"
    return f"{fmt_count(lo)}–{fmt_count(hi)}（中位數 {fmt_mean(med)}）"


def _fmt_rank(v: Any) -> str:
    """名次：整數就印整數，percentile 內插出的小數印 1 位。"""
    if v is None:
        return ""
    f = float(v)
    return str(int(round(f))) if abs(f - round(f)) < 1e-9 else f"{f:.1f}"


# ─────────────────────────────────────────────────────────────────────────
# 概覽——一眼看規模＋關鍵數字，以及「如何讀這頁」的導覽
# ─────────────────────────────────────────────────────────────────────────

def _summary_section(result: dict) -> ReportSection:
    scorecard = pd.DataFrame(
        [
            {"分類": "整體狀況", "指標": "排序品質 macro per-item mAP",
             "值": f"{fmt_ap(result.get('macro_per_item_map'))}（0–1，越高越好）"},
            {"分類": "規模與尺度（非好壞）", "指標": "規模",
             "值": f"{fmt_count(result.get('n_queries'))} queries · "
                   f"{fmt_count(result.get('n_items'))} items · "
                   f"{fmt_count(result.get('n_positive_rows'))} 正例列"},
            {"分類": "規模與尺度（非好壞）", "指標": "每 query 候選數（名次的分母）",
             "值": _candidates_display(result)},
            {"分類": "規模與尺度（非好壞）", "指標": "bootstrap 重抽次數",
             "值": fmt_count((result.get("ci") or {}).get("n_boot"))},
        ],
        columns=["分類", "指標", "值"],
    )
    return ReportSection(
        title="概覽",
        description=(
            "這份診斷回答：模型看起來的 item 排序能力，有多少是真的（query 內"
            "相對排序），有多少來自客戶整體分數水準。"
        ),
        bullets=[
            "下表分兩塊：「整體狀況」的 macro per-item mAP 是整體排序品質；"
            "「規模與尺度」只是規模與名次的分母，不代表好壞。",
            "macro per-item mAP ＝ 各 item 的 AP（見「query-centered AUC vs AP」"
            "區）跨 item 平均（加權方式見完整性檢查的 weight_alpha）；算在這份"
            "診斷抽樣上，與主報表全母體不必相同。",
            "如何讀這頁（想回答什麼 → 看哪一區，順序不必照排）：",
            "想知道看起來的排序能力是真的、還是來自客戶整體分數水準 → "
            "「核心概念」＋「raw vs query-centered 散點」。",
            "想逐 item 看兩個 AUC 有多少不確定性 → 「逐 item AUC 的信賴區間」。",
            "想看排序能力（query-centered AUC）跟實際排序指標（AP）對不對得起來 "
            "→ 「query-centered AUC vs AP」。",
            "想知道某個 item 的買家實際排在 query 內第幾名 → 「正例名次分布」。",
            "每個數字的定義就貼在它出現的那一區；貫穿全報表的基礎量 auc_gap "
            "在下一節「核心概念」講清楚一次。",
        ],
        tables=[scorecard],
        table_titles=["本次執行的關鍵數字"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 0. 核心概念——兩個 AUC、centering、與它們的差
# ─────────────────────────────────────────────────────────────────────────

_FORMULA_FOUNDATION = (
    "within-item AUC ＝ 隨機抽該 item 的 1 個正例列與 1 個負例列，正例分數較高"
    "的機率（同分各算半個、每列依 inclusion_weight 加權）；0.500 ＝ 沒有辨識力\n"
    "query-centered ＝ 分數先減掉該 query 的平均 logit 再算；raw ＝ 不減\n"
    "auc_gap_raw_minus_centered ＝ raw AUC − query-centered AUC"
)


def _foundation_illustration(rows: list[dict]) -> pd.DataFrame | None:
    """用本次資料裡 |gap| 最大的 item 走一遍——具體數字比抽象公式好懂，且讀者
    可以在後面的散點／表裡找到同一個 item 自行核對。"""
    scored = [r for r in rows if r.get(_GAP) is not None]
    if not scored:
        return None
    r = max(scored, key=lambda x: abs(float(x[_GAP])))
    gap = float(r[_GAP])
    reading = (
        "centered 比 raw 高：raw AUC 被客戶整體分數水準拉低（買家整體分數較低）"
        if gap < 0 else
        "raw 比 centered 高：raw AUC 被客戶整體分數水準撐高（買家整體分數較高）"
    )
    return pd.DataFrame(
        [{
            "item": r[_ITEM],
            "raw AUC": fmt_auc(r[_RAW_AUC]),
            "query-centered AUC": fmt_auc(r[_CENTERED_AUC]),
            "gap（raw − centered）": fmt_delta(gap),
            "怎麼讀": reading,
        }],
        columns=["item", "raw AUC", "query-centered AUC", "gap（raw − centered）", "怎麼讀"],
    )


def _foundation_section(result: dict) -> ReportSection:
    rows = _rows_with(result.get("per_item") or [], _RAW_AUC, _CENTERED_AUC, _GAP)
    illustration = _foundation_illustration(rows)
    tables = [_defs_table("within-item AUC", "raw AUC", "query-centered AUC",
                          "auc_gap_raw_minus_centered")]
    table_titles = [_DEFS_TITLE]
    if illustration is not None:
        tables.append(illustration)
        table_titles.append("示意：本次 |gap| 最大的 item，走一遍怎麼讀")

    return ReportSection(
        title="核心概念：兩個 AUC 與它們的差",
        description=(
            "整份診斷只有一組數字：兩個 AUC 與它們的差。先看懂這一節，後面都是"
            "同一組數字換個切法。"
        ),
        formula=_FORMULA_FOUNDATION,
        bullets=[
            "within-item AUC 只在同一個 item 內比較（該 item 的正例列 vs 負例列）；"
            "0.500＝正負例分數分布完全重疊、模型分不出誰會買。",
            "raw 與 query-centered 的唯一差別是 centering：query-centered 先把每個 "
            "query 內所有候選的平均分數扣掉，移除「這位客戶整體分數較高還是較低」；"
            "raw 沒扣，把這個與 item 無關的水準混了進來。",
            "auc_gap ＝ raw − centered，就是「客戶整體分數水準」對 raw AUC 的貢獻："
            "正值＝買家恰好是整體分數較高的客戶、把 raw 撐高；負值＝相反。方向就是"
            "訊號，不取絕對值。",
            "所以 query-centered AUC 才是 item 自己的排序能力；raw 與 gap 是用來看"
            "「有沒有混入、混入多少、哪個方向」。",
            "下面每一區都是這組數字的不同切面：散點看 raw 與 centered 的關係與 gap "
            "的幾何、CI 看每個 AUC 的不確定性、AUC vs AP 看能力對不對得上實際指標、"
            "名次分布看買家實際排第幾。",
        ],
        tables=tables,
        table_titles=table_titles,
    )


# ─────────────────────────────────────────────────────────────────────────
# 1. raw vs query-centered 散點——把 auc_gap 畫成幾何
# ─────────────────────────────────────────────────────────────────────────

def _scatter_section(result: dict) -> ReportSection | None:
    rows = _rows_with(result.get("per_item") or [], _RAW_AUC, _CENTERED_AUC, _GAP)
    if not rows:
        return None

    ordered = sorted(rows, key=lambda r: -abs(float(r[_GAP])))
    items = [str(r[_ITEM]) for r in ordered]
    raw = [float(r[_RAW_AUC]) for r in ordered]
    centered = [float(r[_CENTERED_AUC]) for r in ordered]

    figures: list[Any] = []
    bullets = [
        "對角線 y=x 上＝raw 與 centered 相等（gap=0）；點沿 y 軸（鉛直方向）離對角線"
        "多遠就是 |gap|——這張圖把基礎量 auc_gap 畫成幾何。",
        "點在對角線上方（centered > raw）＝gap 負：raw AUC 被客戶整體分數水準拉低；"
        "下方（raw > centered）＝gap 正：raw 被撐高。",
        "右表把每個 item 的 raw、centered 與精確 gap 列出（依 |gap| 由大到小）——"
        "散點看形狀、表看精確值。",
        "表上 gap 由未捨入的 AUC 算出，與顯示的 raw、centered（各 3 位小數）相減"
        "可能差在末位。",
        "這張圖看不到不確定性：AUC 的信賴區間在下一區，n_pos 小的 item 點位可能"
        "只是抽樣雜訊。",
    ]

    if fits_budget(len(rows)):
        fig = scatter(
            x=raw, y=centered, labels=items,
            title="raw vs query-centered AUC（逐 item）",
            x_title="raw AUC", y_title="query-centered AUC",
        )
        lo = min(min(raw), min(centered))
        hi = max(max(raw), max(centered))
        if lo == hi:  # 良性退化：所有點重合，仍要有一條看得見的參考線
            lo, hi = lo - 0.5, hi + 0.5
        fig.add_shape(
            type="line", x0=lo, y0=lo, x1=hi, y1=hi,
            line=dict(dash="dot", width=1),
        )
        figures.append(fig)
    else:
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "只以下表呈現。"
        )

    data_table = pd.DataFrame(
        [
            {
                _ITEM: r[_ITEM],
                "raw AUC": fmt_auc(r[_RAW_AUC]),
                "query-centered AUC": fmt_auc(r[_CENTERED_AUC]),
                "gap（raw − centered）": fmt_delta(r[_GAP]),
                "n_pos": fmt_count(r.get("n_pos")),
            }
            for r in ordered
        ],
        columns=[_ITEM, "raw AUC", "query-centered AUC", "gap（raw − centered）", "n_pos"],
    )

    return ReportSection(
        title="raw vs query-centered AUC 散點",
        description="每個 item 一個點：x＝raw AUC、y＝query-centered AUC；點沿 y 軸偏離對角線多少就是 auc_gap。",
        formula="auc_gap_raw_minus_centered = raw AUC − query-centered AUC（見核心概念）",
        bullets=bullets,
        figures=figures,
        tables=[
            _defs_table("raw AUC", "query-centered AUC",
                       "auc_gap_raw_minus_centered", "n_pos"),
            data_table,
        ],
        table_titles=[_DEFS_TITLE, "逐 item 的 raw／centered／gap（依 |gap| 降冪）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 2. 逐 item AUC 的信賴區間
# ─────────────────────────────────────────────────────────────────────────

def _per_item_auc_ci_section(result: dict) -> ReportSection | None:
    rows = _rows_with(result.get("per_item") or [], _RAW_AUC, _CENTERED_AUC)
    if not rows:
        return None

    items = [str(r[_ITEM]) for r in rows]
    n_boot = (result.get("ci") or {}).get("n_boot")

    figures: list[Any] = []
    tables: list[pd.DataFrame] = [_defs_table("raw AUC", "query-centered AUC")]
    table_titles: list[str] = [_DEFS_TITLE]
    bullets = [
        f"每根長條是一個 item 的 AUC，誤差線是 95% 信賴區間；0.500 的橫虛線"
        f"（{_RANDOM_AUC:.3f}）＝在這個 item 內完全分不出正負例。",
        "某 item 的 query-centered AUC 誤差線整段在 0.500 之上，代表在這份樣本"
        "上它的 query 內排序能力與「隨機」可區分；壓到 0.500 則不可區分——"
        "n_pos 小的 item 區間通常較寬。",
        "兩張圖分開看 raw 與 centered；同一個 item 兩者的落差就是前一區的 gap。",
        "長條依 item 的 AP 由低到高排列（與散點表的 |gap| 序、名次熱圖的名次序"
        "不同，跨區找同一個 item 要各區重找）。",
    ]

    if fits_budget(len(rows)):
        for auc_key, ci_lo_key, ci_hi_key, label in (
            (_RAW_AUC, _RAW_CI_LOW, _RAW_CI_HIGH, "raw AUC"),
            (_CENTERED_AUC, _CENTERED_CI_LOW, _CENTERED_CI_HIGH, "query-centered AUC"),
        ):
            fig = bar(
                x=items, y=[r[auc_key] for r in rows],
                title=f"逐 item 的 {label}（95% CI）",
                x_title=_ITEM, y_title=label,
                ci_low=[r.get(ci_lo_key) for r in rows],
                ci_high=[r.get(ci_hi_key) for r in rows],
            )
            fig.add_hline(y=_RANDOM_AUC, line=dict(dash="dot", width=1))
            figures.append(fig)
    else:
        tables.append(pd.DataFrame(
            [
                {
                    _ITEM: r[_ITEM],
                    "raw AUC": fmt_auc(r.get(_RAW_AUC)),
                    "raw CI": f"[{fmt_auc(r.get(_RAW_CI_LOW))}, {fmt_auc(r.get(_RAW_CI_HIGH))}]",
                    "centered AUC": fmt_auc(r.get(_CENTERED_AUC)),
                    "centered CI": f"[{fmt_auc(r.get(_CENTERED_CI_LOW))}, "
                                   f"{fmt_auc(r.get(_CENTERED_CI_HIGH))}]",
                }
                for r in rows
            ],
            columns=[_ITEM, "raw AUC", "raw CI", "centered AUC", "centered CI"],
        ))
        table_titles.append("逐 item 的 raw／query-centered AUC（95% CI）")
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "改以表格呈現。"
        )

    if n_boot is not None:
        bullets.append(f"CI 來自 {fmt_count(n_boot)} 次分層 cluster bootstrap 重抽。")

    return ReportSection(
        title="逐 item 的 AUC（含信賴區間）",
        description="raw 與 query-centered AUC 各自的點估計與 95% 區間，逐 item 對照。",
        formula="誤差線 ＝ 95% 區間（分層 cluster bootstrap，cluster＝entity）；橫虛線 0.500＝沒有辨識力",
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


# ─────────────────────────────────────────────────────────────────────────
# 3. query-centered AUC vs AP——能力 proxy 與實際排序指標
# ─────────────────────────────────────────────────────────────────────────

def _auc_vs_ap_section(result: dict) -> ReportSection | None:
    rows = _rows_with(result.get("per_item") or [], _CENTERED_AUC, _AP)
    if not rows:
        return None

    items = [str(r[_ITEM]) for r in rows]
    aucs = [float(r[_CENTERED_AUC]) for r in rows]
    aps = [float(r[_AP]) for r in rows]

    figures: list[Any] = []
    bullets = [
        "這張圖把「能力 proxy」（query-centered AUC）與「實際排序結果」（AP）擺"
        "在一起，看兩者對不對得上。",
        "橫軸靠近 0.500 的直虛線左側＝item 在買家 query 內與隨機難以區分；"
        "query-centered AUC 高、AP 卻低的 item，落差的來源不在這份報表衡量範圍內"
        "（AUC 與 AP 對名次的加權不同），另見「壓制帳本」那份診斷。",
        "AUC 只問「正例是否比負例高分」，AP 還看正例落在第幾名，所以兩者不必然"
        "同向。",
    ]

    if fits_budget(len(rows)):
        fig = scatter(
            x=aucs, y=aps, labels=items,
            title="query-centered AUC vs AP（逐 item）",
            x_title="query-centered AUC", y_title="AP",
        )
        fig.add_vline(x=_RANDOM_AUC, line=dict(dash="dot", width=1))
        figures.append(fig)
    else:
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "只以下表呈現。"
        )

    data_table = pd.DataFrame(
        [
            {
                _ITEM: r[_ITEM],
                "query-centered AUC": fmt_auc(r[_CENTERED_AUC]),
                "AP": fmt_ap(r[_AP]),
                "n_pos": fmt_count(r.get("n_pos")),
            }
            for r in sorted(rows, key=lambda r: float(r[_AP]))
        ],
        columns=[_ITEM, "query-centered AUC", "AP", "n_pos"],
    )

    return ReportSection(
        title="query-centered AUC vs AP",
        description="每個 item 一個點：x＝query-centered AUC（query 內排序能力），y＝AP（實際排序指標）。",
        formula="縱軸 AP＝該 item 的 average precision（k／shrinkage 見完整性檢查）；直虛線 x=0.500＝query 內沒有辨識力",
        bullets=bullets,
        figures=figures,
        tables=[_defs_table("query-centered AUC", "AP", "n_pos"), data_table],
        table_titles=[_DEFS_TITLE, "逐 item 的 query-centered AUC 與 AP（依 AP 升冪）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 4. 正例名次分布——heatmap，格子是名次（1＝排最前）
# ─────────────────────────────────────────────────────────────────────────

def _rank_section(result: dict) -> ReportSection | None:
    per_item = result.get("per_item") or []
    rows = [r for r in per_item if r.get("median_positive_rank") is not None]
    if not rows:
        return None

    # 依中位數名次由大到小排（最常墊底的 item 在最上面）；不進 compute 排序，
    # 排序鍵就是這張圖的主角。
    rows = sorted(rows, key=lambda r: -float(r["median_positive_rank"]))

    n_levels = len(_RANK_LEVELS)
    max_items = MAX_FIGURE_POINTS // n_levels
    n_all = len(rows)
    if n_all > max_items:
        rows = rows[:max_items]

    y = [str(r[_ITEM]) for r in rows]
    x = [label for _, label in _RANK_LEVELS]
    z = [[r.get(key) for key, _ in _RANK_LEVELS] for r in rows]
    text = [[_fmt_rank(r.get(key)) for key, _ in _RANK_LEVELS] for r in rows]

    fig = heatmap(
        z=z, x=x, y=y,
        title="正例名次分布（每格＝名次，1＝排最前）",
        colorbar_title="名次（1＝排最前）",
        text=text,
    )
    # 最常墊底的 item 排在最上面（y[0]），與「由大到小排」的直覺一致。
    fig.update_yaxes(autorange="reversed")

    bullets = [
        "每一列是一個 item，欄是它正例名次分布的百分位（p10 較靠前、p90 較靠後）；"
        "格子裡的數字就是名次，1＝排在自己 query 的最前面。",
        f"顏色越深＝名次越大（越靠後）；名次的分母是每 query 的候選數"
        f"（{_candidates_display(result)}，見完整性檢查），例如候選 8 個時名次 8＝排最後。",
        "百分位由名次分布內插，可能出現非整數（例 5.4＝落在第 5、6 名之間），"
        "不是算錯。",
        "依中位數名次由大到小排，最常被排到後段的 item 在最上面。",
        "只看得到名次落在哪、看不到是被誰擠到後面的——那要看「壓制帳本」那份診斷。",
    ]
    if n_all > len(rows):
        bullets.append(
            f"item 共 {n_all} 個，此圖只畫中位數名次最大（最靠後）的 {len(rows)} 個。"
        )

    return ReportSection(
        title="正例名次分布",
        description="每個 item 一列：它的正例平常排在自己 query 內第幾名（名次分布的幾個百分位）。",
        formula="名次 ＝ 依模型分數由高到低，1＝排最前；每格＝該 item 正例名次分布的某個百分位（越小＝越靠前）",
        bullets=bullets,
        figures=[fig],
        tables=[_defs_table("median pos rank")],
        table_titles=[_DEFS_TITLE],
    )


# ─────────────────────────────────────────────────────────────────────────
# 5. 完整性檢查
# ─────────────────────────────────────────────────────────────────────────

def _completeness_section(result: dict) -> ReportSection:
    """本次執行的完整性檢查——不可省，空的時候也照樣印「無」。

    與 ``config_shift`` 的 ``_visibility_section`` 同一份角色分工：頁首
    ``ScopeNote.blind_to`` 是這個指標**結構上**推論不到的事，這一節是三種
    **已知靜默失效**在**本次執行**的實際結果。
    """
    per_item = result.get("per_item") or []
    notes = result.get("notes") or []
    ci_info = result.get("ci") or {}
    mp = result.get("metric_params") or {}

    missing_auc = [
        str(r[_ITEM]) for r in per_item
        if r.get(_RAW_AUC) is None or r.get(_CENTERED_AUC) is None
    ]

    bullets = [
        f"AP 的參數：k = {mp.get('k', '未設')}、shrinkage_k = {mp.get('shrinkage_k')}、"
        f"weight_alpha = {mp.get('weight_alpha')}（名次分布與 AUC 與 k 無關；只有 AP 受 k 影響）。",
        f"每 query 候選數（名次的分母）：{_candidates_display(result)}。",
        "計算層 notes（含 logit 轉換的觀測）："
        + (f"{len(notes)} 則，列於下方" if notes else "無"),
        "本次抽樣中正例或負例列數為 0、未算出 AUC 的 item（不進散點圖與長條圖，"
        "但仍計入下面的 item 總數）："
        + (", ".join(missing_auc) if missing_auc else "無"),
        "本次執行是否啟用信賴區間 bootstrap："
        + (
            f"啟用，{fmt_count(ci_info.get('n_boot'))} 次重抽"
            if ci_info.get("enabled") else "未啟用"
        ),
    ]
    bullets.extend(str(n) for n in notes)
    bullets.append(
        "本次診斷抽樣的規模："
        f"{fmt_count(result.get('n_queries'))} 個 query、"
        f"{fmt_count(result.get('n_entities'))} 個 entity、"
        f"{fmt_count(result.get('n_items'))} 個 item、"
        f"{fmt_count(result.get('n_positive_rows'))} 列正例。"
    )

    return ReportSection(
        title="本次執行的完整性檢查",
        description="以下情況會讓上面的數字看起來正常、實際上沒量到或不完整。",
        bullets=bullets,
    )


def render(result: dict, parameters: dict) -> tuple[ReportSection, ...]:
    """把 ``compute`` 的輸出轉成一串報表章節；停用時回空 tuple。

    順序即閱讀順序：概覽 → 核心概念（兩個 AUC 與它們的差）→ raw vs centered
    散點（gap 的幾何）→ 逐 item AUC 的信賴區間 → query-centered AUC vs AP →
    正例名次分布 → 完整性檢查。概覽與核心概念永遠在最前、完整性檢查永遠殿後。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _summary_section(result),
        _foundation_section(result),
        _scatter_section(result),
        _per_item_auc_ci_section(result),
        _auc_vs_ap_section(result),
        _rank_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
