"""model_capacity 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``item_ability/_render.py``（見該檔 docstring 的完整理由）：純函式、
不 import pyspark、不讀檔、不做任何計算——出現在報表上的每個數字都必須已經
在 JSON 裡。一個 section ＝ 一句 description ＋ 一行 Unicode 純文字 formula
＋ 最多 3 則 bullet ＋ 它自己的圖或表；停用時回空 tuple；最後一節固定是「本次
執行的完整性檢查」。

**四節內容**：
1. 全模型 Gain 三分：Item Prior／Post-Item Context／未分配，各占 total_gain
   的份額。``report.figures`` 沒有堆疊條圖的建構器（目前只有這一項診斷需要
   「三個類別各一根長條」的形狀），這裡用既有的 ``bar()`` 畫三個類別，不是
   真正的堆疊——三個份額本身就是圖上唯一要傳達的訊息，不需要疊在同一根柱子
   上才看得懂。
2. per-item context gain 分配條圖：依 ``compute`` 已排好的順序（遞減），不在
   這裡重排。
3. capacity vs ability 散點：x ＝ 該 item 的 context_gain_share、y ＝
   query_centered_auc。**``item_ability`` 缺席時這一節仍然存在**，只是沒有
   圖、改用文字說明原因——「略過畫圖」與「這一頁不存在」是兩件事，讀者要看
   得出「量過了、但沒有可對照的 ability 資料」與「根本沒有這一項診斷」的
   差別（本專案踩過的假綠形態：只斷言「圖沒出現」同時被「正確略過」與「根本
   沒嘗試」滿足，這裡用 bullet 文字把差異釘住）。
4. ``available: False``：整頁不得空白，顯示 ``reason``。

**三條鐵則**（整個 diag-redesign 的共同約定，逐項落實）：
1. 不下結論——沒有 severity／verdict／建議動作。
2. 不設門檻——不拿 config 門檻把連續量切成離散類別；顏色只編碼大小，不編碼
   好壞（gain share 全部非負，``bar()`` 因此不傳 ``center``，用單色，不是
   發散色階）。
3. 每個數字自帶說明——範圍說明由 ``SCOPE`` 擁有（見 ``__init__``）。

**「未分配」不是誤差**：``unaccounted_gain`` 是 item 切點**之前**的切點 gain，
結構上就無法歸給任何單一 item，不是這套記帳規則「漏算」或「算錯」的殘差。
本模組的呈現文字（description／bullets）刻意不使用「誤差／殘差／漏掉／未
解釋」這類字眼描述它——這條由 ``test_unaccounted_block_is_not_labelled_as_
error`` 釘住。``SCOPE.blind_to`` 裡明講「它不是誤差」是**範圍說明**，不在此
限（讀者要先讀到這句反向澄清，才知道為什麼看到「未分配」不必緊張）。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import MAX_FIGURE_POINTS, bar, scatter
from recsys_tfb.report.fmt import fmt_auc

_ITEM = "item"

_FORMULA_BREAKDOWN = (
    "item_id_gain_share = item_id_gain / total_gain\n"
    "context_gain_share = context_gain / total_gain\n"
    "unaccounted_gain_share = 1 − item_id_gain_share − context_gain_share"
)

_FORMULA_PER_ITEM = (
    "context_gain_share(item) = context_gain(item) / Σ_item context_gain(item)"
    "（分母是這批 item 的加總，不是全模型的 context_gain）"
)

_FORMULA_SCATTER = (
    "x = context_gain_share(item)（見上一節）\n"
    "y = query_centered_auc(item)（來自同一次執行的 item_ability 診斷）"
)


def _fits(n_points: int) -> bool:
    """``n_points`` 是否在單張圖的預算內（與 ``figures.assert_within_budget``
    同一常數、同一比較方向，避免「這裡判斷畫得下、那裡 raise」的死角）。"""
    return n_points <= MAX_FIGURE_POINTS


def _unavailable_section(result: dict) -> ReportSection:
    """``available: False`` 時的整頁內容——顯示 reason，不留白。"""
    reason = result.get("reason") or "gain_ledger 不可用（原因未提供）。"
    return ReportSection(
        title="gain_ledger 不可用",
        description=f"這次執行沒有可用的 gain_ledger，因此本項診斷未計算。{reason}",
        bullets=[reason],
    )


def _breakdown_section(result: dict) -> ReportSection | None:
    """1. 全模型 Gain 三分：Item Prior／Post-Item Context／未分配。"""
    summary = result.get("summary") or {}
    labels = ["Item Prior（item-id 切點）", "Post-Item Context（item 切點後）",
              "未分配（item 切點前）"]
    shares = [
        summary.get("item_id_gain_share"),
        summary.get("context_gain_share"),
        summary.get("unaccounted_gain_share"),
    ]
    rows = [(lab, s) for lab, s in zip(labels, shares) if s is not None]
    if not rows:
        return None

    labels_present = [r[0] for r in rows]
    shares_present = [float(r[1]) for r in rows]

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        "三份的分母都是全模型的 total_gain，加總為 1。",
        "「未分配」是 item 切點之前的切點，結構上無法歸給任何單一 item——"
        "見頁首範圍說明。",
    ]

    if _fits(len(rows)):
        figures.append(bar(
            x=labels_present, y=shares_present,
            title="全模型 split gain 三分",
            x_title="類別", y_title="占 total_gain 的份額",
        ))
    else:  # pragma: no cover - 三個類別不可能超過繪圖預算，防禦性保留
        tables.append(pd.DataFrame(
            [{"類別": lab, "份額": fmt_auc(s)} for lab, s in rows],
            columns=["類別", "份額"],
        ))
        table_titles.append("全模型 split gain 三分")

    return ReportSection(
        title="全模型 Gain 三分",
        description="全模型 split gain 分成 Item Prior、Post-Item Context、未分配三份。",
        formula=_FORMULA_BREAKDOWN,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _per_item_section(result: dict) -> ReportSection | None:
    """2. per-item context gain 分配條圖（依 compute 已排好的順序）。"""
    rows = [
        r for r in (result.get("per_item") or [])
        if r.get("context_gain_share") is not None
    ]
    if not rows:
        return None

    items = [str(r["item"]) for r in rows]
    shares = [float(r["context_gain_share"]) for r in rows]

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        "分母是這批 item 的 context_gain 加總，全部 item 的份額加總為 1。",
        "依份額遞減排序（沿用 compute 的順序，這裡不重排）。",
    ]

    if _fits(len(rows)):
        figures.append(bar(
            x=items, y=shares,
            title="per-item context gain 分配",
            x_title=_ITEM, y_title="context_gain_share",
        ))
    else:
        tables.append(pd.DataFrame(
            [{_ITEM: it, "context_gain_share": fmt_auc(s)}
             for it, s in zip(items, shares)],
            columns=[_ITEM, "context_gain_share"],
        ))
        table_titles.append("per-item context gain 分配")
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "改以表格呈現。"
        )

    return ReportSection(
        title="per-item context gain 分配",
        description="每個 item 分到的 Post-Item Context gain，占這批 item 加總的比例。",
        formula=_FORMULA_PER_ITEM,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _capacity_vs_ability_section(result: dict) -> ReportSection:
    """3. capacity vs ability 散點。``item_ability`` 缺席時略過畫圖，改用文字
    說明原因——這一節本身永遠存在（見模組 docstring 的假綠警語）。
    """
    rows = [
        r for r in (result.get("per_item") or [])
        if r.get("context_gain_share") is not None
        and r.get("query_centered_auc") is not None
    ]

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []

    if not rows:
        bullets = [
            "本次執行沒有可對照的 item_ability 資料（缺席、被關閉，或沒有任何"
            "一個 item 兩邊都有值），因此略過這張散點圖——原因見下方「本次"
            "執行的完整性檢查」的 notes。",
        ]
        description = (
            "沒有可對照的 item_ability 資料，本次未畫出 capacity vs ability 散點。"
        )
    else:
        items = [str(r["item"]) for r in rows]
        x = [float(r["context_gain_share"]) for r in rows]
        y = [float(r["query_centered_auc"]) for r in rows]
        bullets = [
            "x 與 y 來自兩項獨立的診斷（本項的 context_gain_share、"
            "item_ability 的 query_centered_auc），只在這裡並排對照。",
        ]
        description = (
            "每個 item 一個點：x 是分到的 context gain 份額，y 是同一次執行"
            "算出的 query-centered AUC。"
        )
        if _fits(len(rows)):
            figures.append(scatter(
                x=x, y=y, labels=items,
                title="context_gain_share vs query_centered_auc（逐 item）",
                x_title="context_gain_share", y_title="query_centered_auc",
            ))
        else:
            tables.append(pd.DataFrame(
                [
                    {_ITEM: it, "context_gain_share": fmt_auc(xi),
                     "query_centered_auc": fmt_auc(yi)}
                    for it, xi, yi in zip(items, x, y)
                ],
                columns=[_ITEM, "context_gain_share", "query_centered_auc"],
            ))
            table_titles.append("context_gain_share vs query_centered_auc（逐 item）")
            bullets.append(
                f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點"
                "上限，改以表格呈現。"
            )

    return ReportSection(
        title="capacity vs ability 散點",
        description=description,
        formula=_FORMULA_SCATTER,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _completeness_section(result: dict) -> ReportSection:
    """4. 本次執行的完整性檢查——這一節不可省，空的時候也照樣印「無」。"""
    notes = result.get("notes") or []
    summary = result.get("summary") or {}

    bullets = [
        "計算層 notes（含缺席資料的成因、item_ability join 狀況）："
        + (f"{len(notes)} 則，列於下方" if notes else "無"),
    ]
    bullets.extend(str(n) for n in notes)
    if summary.get("n_items") is not None:
        bullets.append(f"本次讀到的 item 數：{summary['n_items']}。")

    return ReportSection(
        title="本次執行的完整性檢查",
        description="以下情況會讓上面的數字看起來正常、實際上沒量到或不完整。",
        bullets=bullets,
    )


def render(result: dict, parameters: dict) -> tuple[ReportSection, ...]:
    """把 ``compute`` 的輸出轉成一串報表章節；停用時回空 tuple。

    ``available: False`` 時只回「gain_ledger 不可用」＋完整性檢查兩節（不留
    白頁，也不假裝畫出三份沒有資料的圖）。
    """
    if not result.get("enabled"):
        return ()

    if not result.get("available"):
        return (_unavailable_section(result), _completeness_section(result))

    sections = [
        _breakdown_section(result),
        _per_item_section(result),
        _capacity_vs_ability_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
