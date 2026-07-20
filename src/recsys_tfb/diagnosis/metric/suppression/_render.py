"""suppression 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``item_ability/_render.py``（見該檔 docstring 的完整理由）：純函式、
不 import pyspark、不讀檔、不做任何計算——出現在報表上的每個數字都必須已經
在 JSON 裡。停用時回空 tuple；最後一節固定是「本次執行的完整性檢查」。

**五節內容**：
1. 壓制矩陣熱圖——``matrices["target_gap_share"]``，列＝受害 item、欄＝
   壓制者 item。單向大小（∈[0,1]），不給 ``center``，走 ``sequential_scale``。
2. 交叉購買 lift 泡泡格圖——``cross_purchase``，限制在與矩陣同一組
   （截斷後的）軸序內，兩張圖才能同軸對照。大小＝``n_joint``，顏色＝
   ``lift``（``center=1.0``，獨立時的期望值）。
3. 具體案例表——``examples``，逐列列出 gap 最大的 (正例, 壓制者) 組合。
4. per-suppressor 彙總條圖——``by_suppressor`` 的 ``overall_ap_gap_share``。
5. 完整性檢查（固定殿後）。

⚠ **圖形點數預算**：矩陣／泡泡格圖都是 ``|axis_order|²``，item 一多就會撞
``figures.MAX_FIGURE_POINTS``（2000）。這裡不像 ``item_ability`` 的長條圖那樣
超量就整個退回表格——矩陣退回表格會變成 item 數平方那麼多列，沒人讀得下去，
而且兩張圖並排對照正是這項診斷的價值所在，拿掉圖等於拿掉診斷。改成**截斷**：
只畫分攤缺口最大的前 ``N = floor(sqrt(MAX_FIGURE_POINTS))`` 個 item（見
:func:`_ranked_axis`），完整成對資料仍以表格／JSON 形式附在旁邊，不損失
資訊，只損失「一眼掃過去」的範圍。``N`` 是由繪圖引擎硬上限推導出來的常數，
不是可調參數，不進 config（見三條鐵則之二：不設門檻）。
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import (
    MAX_FIGURE_POINTS, bar, bubble_grid, fits_budget, heatmap,
)
from recsys_tfb.report.fmt import fmt_ap, fmt_auc, fmt_count, fmt_logodds, fmt_ratio

#: 單張圖（矩陣／泡泡格圖，皆為 |axis|² 個點）能承受的最大軸長。由繪圖引擎
#: 的硬上限反推，不是可調的判斷門檻——見模組 docstring「圖形點數預算」。
_N_AXIS_MAX = math.isqrt(MAX_FIGURE_POINTS)

_FORMULA_MATRIX = (
    "target_gap_share(row, col) = pair_ledger[row, col] 的 allocated_ap_gap "
    "÷ row 這個受害 item 的 allocated_ap_gap 總和"
)

_FORMULA_CROSS_PURCHASE = (
    "p_k_given_j = n_joint ÷ n_j\n"
    "lift = p_k_given_j ÷ (n_k ÷ n_units)"
)

_FORMULA_EXAMPLES = (
    "row_ap_gap = max(0, 1 − 該正例列目前的 precision 貢獻)；"
    "allocated_ap_gap 依比例分攤自 row_ap_gap"
)

_FORMULA_BY_SUPPRESSOR = (
    "overall_ap_gap_share = 該壓制者分攤到的 allocated_ap_gap ÷ "
    "全部 pair_ledger 的 allocated_ap_gap 總和"
)


def _ranked_axis(result: dict) -> tuple[list[str], int, int]:
    """回傳 ``(顯示用軸序, 顯示個數, 全部個數)``。

    ``axis_order`` 本身已排序（見 ``_compute.py``）。超過 :data:`_N_AXIS_MAX`
    時，依該 item 在 ``target_summary`` 的 ``overall_ap_gap_share`` 由大到小
    只取前 ``_N_AXIS_MAX`` 個，取完再排序回字母序——截斷改變的是「畫哪些」，
    不改變「畫出來的軸怎麼排」，這樣矩陣與泡泡格圖才能繼續同軸對照。
    """
    axis_order = [str(a) for a in (result.get("axis_order") or [])]
    n_all = len(axis_order)
    if n_all <= _N_AXIS_MAX:
        return axis_order, n_all, n_all

    share_by_item = {
        str(t.get("positive_item")): float(t.get("overall_ap_gap_share") or 0.0)
        for t in (result.get("target_summary") or [])
    }
    ranked = sorted(axis_order, key=lambda it: -share_by_item.get(it, 0.0))
    shown = sorted(ranked[:_N_AXIS_MAX])
    return shown, len(shown), n_all


def _pair_ledger_table(result: dict) -> pd.DataFrame:
    """完整（未截斷）的壓制帳本——矩陣截斷時附在旁邊，資料不因截斷而遺失。"""
    rows = result.get("pair_ledger") or []
    return pd.DataFrame(
        [
            {
                "positive_item": r.get("positive_item"),
                "suppressor_item": r.get("suppressor_item"),
                "allocated_ap_gap": fmt_ap(r.get("allocated_ap_gap")),
                "target_ap_gap_share": fmt_auc(r.get("target_ap_gap_share")),
                "overall_ap_gap_share": fmt_auc(r.get("overall_ap_gap_share")),
                "affected_positive_rows": fmt_count(r.get("affected_positive_rows")),
                "affected_positive_rate": fmt_auc(r.get("affected_positive_rate")),
                "mean_score_margin": fmt_logodds(r.get("mean_score_margin")),
            }
            for r in rows
        ],
        columns=[
            "positive_item", "suppressor_item", "allocated_ap_gap",
            "target_ap_gap_share", "overall_ap_gap_share",
            "affected_positive_rows", "affected_positive_rate",
            "mean_score_margin",
        ],
    )


def _matrix_section(result: dict) -> ReportSection | None:
    """1. 壓制矩陣熱圖——本項的第一張核心圖。"""
    shown, n_shown, n_all = _ranked_axis(result)
    if not shown:
        return None

    matrix = (result.get("matrices") or {}).get("target_gap_share") or {}
    z = [
        [float((matrix.get(victim) or {}).get(sup, 0.0)) for sup in shown]
        for victim in shown
    ]
    fig = heatmap(
        z=z, x=shown, y=shown,
        title="壓制矩陣：target_gap_share（列＝受害 item，欄＝壓制者 item）",
        colorbar_title="target_ap_gap_share",
    )

    bullets = [
        "格子 (row, col) 的值＝壓制者 item（col）分攤走 row 這個受害 item 的"
        "AP 缺口佔比。",
        "顏色只編碼佔比大小（單向量，∈[0,1]），不是正負對照。",
    ]
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限"
            f"（矩陣是 item 數的平方），此圖只畫分攤缺口最大的 {n_shown} 個；"
            "完整成對資料見下方壓制帳本表格。"
        )
        tables.append(_pair_ledger_table(result))
        table_titles.append("完整壓制帳本（pair_ledger）")

    return ReportSection(
        title="壓制矩陣熱圖",
        description="列＝受害 item，欄＝壓制者 item；顏色深淺＝該壓制者分攤走的 AP 缺口佔比。",
        formula=_FORMULA_MATRIX,
        bullets=bullets,
        figures=[fig],
        tables=tables,
        table_titles=table_titles,
    )


def _cross_purchase_section(result: dict) -> ReportSection | None:
    """2. 交叉購買 lift 泡泡格圖——與矩陣同軸序，並排對照。"""
    shown, n_shown, n_all = _ranked_axis(result)
    if not shown:
        return None

    shown_set = set(shown)
    rows = [
        r for r in (result.get("cross_purchase") or [])
        if r.get("item_j") in shown_set and r.get("item_k") in shown_set
    ]
    if not rows:
        return None

    x = [str(r["item_j"]) for r in rows]
    y = [str(r["item_k"]) for r in rows]
    size = [float(r.get("n_joint") or 0.0) for r in rows]
    colour = [
        float(r["lift"]) if r.get("lift") is not None else float("nan")
        for r in rows
    ]
    hover = [
        f"{r['item_j']} × {r['item_k']}"
        f"<br>n_joint={fmt_count(r.get('n_joint'))}"
        f"<br>n_j={fmt_count(r.get('n_j'))}　n_k={fmt_count(r.get('n_k'))}"
        f"<br>p_k_given_j={fmt_auc(r.get('p_k_given_j'))}"
        f"<br>lift={fmt_ratio(r.get('lift'))}"
        for r in rows
    ]
    fig = bubble_grid(
        x=x, y=y, size=size, colour=colour, hover_text=hover,
        title="交叉購買 lift（同一 query 單位、label=1 的共現）",
        colorbar_title="lift",
    )

    bullets = [
        "大小＝n_joint（同買的 query 單位數），顏色＝lift；大小大不代表顏色深"
        "，兩者是不同的量。",
        "hover 附 n_joint／n_j／n_k／p_k_given_j／lift 五個數，供逐格核對。",
    ]
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限"
            f"（矩陣是 item 數的平方），此圖只畫分攤缺口最大的 {n_shown} 個；"
            "完整成對資料見 JSON 產物的 cross_purchase。"
        )

    return ReportSection(
        title="交叉購買 lift 泡泡格圖",
        description=(
            "左圖是模型的排序行為，右圖是這批 entity 的實際標籤共現；"
            "兩張圖的橫縱軸是同一組 item、同一個順序。"
        ),
        formula=_FORMULA_CROSS_PURCHASE,
        bullets=bullets,
        figures=[fig],
    )


_EXAMPLE_COLUMNS = [
    "query", "positive_item", "suppressor_item",
    "positive_rank", "suppressor_rank",
    "positive_score", "suppressor_score", "score_margin",
    "allocated_ap_gap",
]


def _examples_section(result: dict) -> ReportSection | None:
    """3. 具體案例表——gap 最大的 (正例, 壓制者) 組合，逐案核對用。"""
    examples = result.get("examples") or []
    if not examples:
        return None

    table = pd.DataFrame(
        [
            {
                "query": e.get("query"),
                "positive_item": e.get("positive_item"),
                "suppressor_item": e.get("suppressor_item"),
                "positive_rank": fmt_count(e.get("positive_rank")),
                "suppressor_rank": fmt_count(e.get("suppressor_rank")),
                "positive_score": fmt_logodds(e.get("positive_score")),
                "suppressor_score": fmt_logodds(e.get("suppressor_score")),
                "score_margin": fmt_logodds(e.get("score_margin")),
                "allocated_ap_gap": fmt_ap(e.get("allocated_ap_gap")),
            }
            for e in examples
        ],
        columns=_EXAMPLE_COLUMNS,
    )
    bullets = [
        "依 gap 由大到小排序（compute 已排序，這裡不重排）。",
        "score_margin ＝ 壓制者 logit − 正例 logit；正值代表壓制者分數確實較高。",
    ]

    return ReportSection(
        title="具體案例：被壓制的正例列",
        description="gap 最大的具體 (正例, 壓制者) 案例，逐列列出供核對，不做聚合證據用。",
        formula=_FORMULA_EXAMPLES,
        bullets=bullets,
        tables=[table],
        table_titles=["具體案例（依 gap 排序）"],
    )


def _by_suppressor_section(result: dict) -> ReportSection | None:
    """4. per-suppressor 彙總條圖——每個壓制者分攤走多少比例的 AP 缺口。"""
    rows = [
        r for r in (result.get("by_suppressor") or [])
        if r.get("overall_ap_gap_share") is not None
    ]
    if not rows:
        return None

    items = [str(r["suppressor_item"]) for r in rows]
    shares = [float(r["overall_ap_gap_share"]) for r in rows]

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        "依壓制者 item 彙總：這個負例 item 總共分攤走多少比例的全部 AP 缺口。",
    ]

    if fits_budget(len(rows)):
        figures.append(bar(
            x=items, y=shares,
            title="per-suppressor 彙總：overall_ap_gap_share",
            x_title="suppressor_item", y_title="overall_ap_gap_share",
        ))
    else:
        tables.append(pd.DataFrame(
            [
                {
                    "suppressor_item": r["suppressor_item"],
                    "overall_ap_gap_share": fmt_auc(r.get("overall_ap_gap_share")),
                    "affected_positive_items": fmt_count(r.get("affected_positive_items")),
                    "affected_positive_rows": fmt_count(r.get("affected_positive_rows")),
                    "mean_score_margin": fmt_logodds(r.get("mean_score_margin")),
                    "top_positive_items": r.get("top_positive_items"),
                }
                for r in rows
            ],
            columns=[
                "suppressor_item", "overall_ap_gap_share",
                "affected_positive_items", "affected_positive_rows",
                "mean_score_margin", "top_positive_items",
            ],
        ))
        table_titles.append("per-suppressor 彙總（依 overall_ap_gap_share）")
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "改以表格呈現。"
        )

    return ReportSection(
        title="per-suppressor 彙總",
        description="每個壓制者 item 一根長條：它總共分攤走多少比例的全部 AP 缺口。",
        formula=_FORMULA_BY_SUPPRESSOR,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _completeness_section(result: dict) -> ReportSection:
    """5. 本次執行的完整性檢查——固定殿後，空的時候也照樣印「無」。"""
    notes = result.get("notes") or []
    axis_order = result.get("axis_order") or []
    cross_purchase = result.get("cross_purchase") or []

    bullets = [
        "計算層 notes（含 logit 轉換的觀測）："
        + (f"{len(notes)} 則，列於下方" if notes else "無"),
        "axis_order 涵蓋的 item 數（出現在壓制成對表裡，才會進兩張圖）："
        + fmt_count(len(axis_order)),
        "cross_purchase 列數（限制在 axis_order 內、n_units="
        + fmt_count(result.get("n_units")) + "）："
        + fmt_count(len(cross_purchase)),
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

    順序即閱讀順序：先看壓制矩陣（模型排序行為），再看交叉購買（實際標籤
    共現，與矩陣同軸並排），然後是具體案例與 per-suppressor 彙總，完整性
    檢查永遠在最後。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _matrix_section(result),
        _cross_purchase_section(result),
        _examples_section(result),
        _by_suppressor_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
