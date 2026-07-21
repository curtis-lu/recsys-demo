"""model_capacity 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``item_ability/_render.py``（見該檔 docstring 的完整理由）：純函式、
不 import pyspark、不讀檔、不做任何計算——出現在報表上的每個數字都必須已經
在 JSON 裡。停用時回空 tuple；最後一節固定是「本次執行的完整性檢查」。

**編排（依 diagnosis-report-presentation.md：概覽 → 核心概念 → 由粗到細 →
完整性檢查）**：

1. 概覽（定向）：這頁回答什麼、結果一句話、規模／分母、往哪找。
2. 全模型 Gain 三分（核心概念＋表）：把「地基量」（每個切點的 gain，依它相對
   item 切點的位置分三桶）講清楚一次，並用**表格**（不是長條圖）同時列 gain
   值、gain 佔比與 split 數。使用者回饋①：三個份額本身就是全部訊息，長條圖
   多餘；且 gain 與 split 都要看得到數值。
3. per-item context 容量 ledger：由粗到細的主體。一張分配長條圖看形狀＋一張
   完整明細表（使用者回饋②：ledger 記的 split 數、私有 context gain、item
   路由足跡都保留，不砍成只剩 gain）。
4. capacity vs ability 散點：x＝context_gain_share、y＝query_centered_auc。
   ``item_ability`` 缺席時這一節仍然存在，只是沒有圖、改用文字說明原因——
   「略過畫圖」與「這一頁不存在」是兩件事（本專案踩過的假綠形態：只斷言
   「圖沒出現」同時被「正確略過」與「根本沒嘗試」滿足）。
5. 本次執行的完整性檢查。

``available: False`` 時整頁不得空白，顯示 ``reason``。

**split 只有兩桶（誠實邊界）**：ledger 記了 item-id 與 post-item context 兩類
split 數，但**沒有**全域總 split 數／pre-item split 數（那需另讀 model.txt，
本診斷刻意不讀）。所以 gain 有完整三分佔比、split 沒有——三分表的未分配
split 欄明講「需 model.txt」，完整性檢查再交代一次，不讓空欄被讀成 0 或算錯。

**三條鐵則**（整個 diag-redesign 的共同約定，逐項落實）：
1. 不下結論——沒有 severity／verdict／建議動作。
2. 不設門檻——不拿 config 門檻把連續量切成離散類別；顏色只編碼大小，不編碼
   好壞（gain／split share 全部非負，``bar()`` 因此不傳 ``center``，用單色）。
3. 每個數字自帶說明——範圍說明由 ``SCOPE`` 擁有（見 ``__init__``），欄位定義
   貼在它出現的那一區。

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
from recsys_tfb.report.fmt import (
    fmt_auc, fmt_count, fmt_gain, fmt_mean, fmt_percent,
)

_ITEM = "item"

_FORMULA_BREAKDOWN = (
    "item_id_gain_share = item_id_gain / total_gain\n"
    "context_gain_share = context_gain / total_gain\n"
    "unaccounted_gain_share = 1 − item_id_gain_share − context_gain_share"
)

_FORMULA_PER_ITEM = (
    "context_gain_share(item) = context_gain(item) / sum_allocated_context_gain\n"
    "context_split_share(item) = context_split_count(item) / sum_allocated_context_split"
)

_FORMULA_SCATTER = (
    "x = context_gain_share(item)（見上一節）\n"
    "y = query_centered_auc(item)（來自同一次執行的 item_ability 診斷）"
)

_FORMULA_VS_TOTAL = (
    "gain 涵蓋% = context_gain(item) / total_gain\n"
    "gain 獨佔% = context_gain_isolated(item) / total_gain\n"
    "split 涵蓋% = context_split_count(item) / total_split_count\n"
    "split 獨佔% = context_split_isolated(item) / total_split_count"
)

_FORMULA_PRE_ITEM = (
    "占未分配 gain% = 特徵 gain / pre_item.gain_sum\n"
    "pre_item.gain_sum = 全模型 unaccounted_gain（item 切點前的切點 gain 加總）"
)

#: pre-item per-feature 表最多列幾個特徵（其餘併一列彙總，不靜默截斷）。
_MAX_PRE_ITEM_FEATURES = 15


def _fits(n_points: int) -> bool:
    """``n_points`` 是否在單張圖的預算內（與 ``figures.assert_within_budget``
    同一常數、同一比較方向，避免「這裡判斷畫得下、那裡 raise」的死角）。"""
    return n_points <= MAX_FIGURE_POINTS


def _fmt_depth(v) -> str:
    """深度分位：整數就不帶小數（1），非整數帶 1 位（1.5）。"""
    if v is None:
        return ""
    f = float(v)
    return str(int(f)) if f == int(f) else f"{f:.1f}"


def _unavailable_section(result: dict) -> ReportSection:
    """``available: False`` 時的整頁內容——顯示 reason，不留白。"""
    reason = result.get("reason") or "gain_ledger 不可用（原因未提供）。"
    return ReportSection(
        title="gain_ledger 不可用",
        description=f"這次執行沒有可用的 gain_ledger，因此本項診斷未計算。{reason}",
        bullets=[reason],
    )


def _overview_section(result: dict) -> ReportSection:
    """1. 概覽（定向）：這頁回答什麼、結果一句話、規模／分母、往哪找。"""
    s = result.get("summary") or {}
    bullets = [
        f"結果：item 欄切點（Item Prior）占 "
        f"{fmt_percent(s.get('item_id_gain_share'))}、item 切點後 context"
        f"（Post-Item Context）占 {fmt_percent(s.get('context_gain_share'))}、"
        f"item 切點前殘餘（未分配）{fmt_percent(s.get('unaccounted_gain_share'))}"
        "——三分細節見下一節。",
        f"總量：total_gain {fmt_gain(s.get('total_gain'))}、total splits "
        f"{fmt_count(s.get('total_split_count'))}（切點總數）、樹數 "
        f"{fmt_count(s.get('n_trees'))}、item 數 {fmt_count(s.get('n_items'))}"
        "——下面所有份額都以這兩個總量（gain／split）為分母，見三分表。",
        "導覽：整體怎麼分→三分表；哪個 item 分到多少 context→per-item ledger；"
        "item 在模型整體尺度上多大一塊→vs 全模型；未分配是哪些全域特徵→未分配"
        "拆解；容量份額 vs query-centered AUC→散點。",
    ]
    return ReportSection(
        title="概覽",
        description="這頁回答一個問題：模型的 split gain，分給 item 身分本身、"
                    "還是 item 之後才學到的 context？",
        bullets=bullets,
    )


def _breakdown_section(result: dict) -> ReportSection | None:
    """2. 全模型 Gain 三分（核心概念＋表）：Item Prior／Post-Item Context／未分配。

    使用者回饋①：改用**表格**（不是長條圖），同時列 gain 值、gain 佔比、
    split 數。split 只有 item_id／context 兩類（見模組 docstring），未分配的
    split 欄明講需 model.txt。
    """
    s = result.get("summary") or {}
    # 標籤中文在前、英文術語在括號：讀者 subagent P3——「Item Prior」「Pre-Item」
    # 兩個英文都讀作「item 之前」卻指相反的東西（Prior＝item 切點本身、Pre＝
    # item 切點之前的殘餘），中文機制描述在前可擋掉這個對調誤讀。
    specs = [
        ("item 欄切點本身（Item Prior）",
         s.get("item_id_gain"), s.get("item_id_gain_share"),
         s.get("item_id_split_count"), s.get("item_id_split_share")),
        ("item 切點後的 context（Post-Item Context）",
         s.get("context_gain"), s.get("context_gain_share"),
         s.get("context_split_count"), s.get("context_split_share")),
        ("item 切點前的殘餘（未分配）",
         s.get("unaccounted_gain"), s.get("unaccounted_gain_share"),
         s.get("unaccounted_split_count"), s.get("unaccounted_split_share")),
    ]
    present = [row for row in specs if row[2] is not None]  # 用 gain 佔比判在場
    if not present:
        return None

    table = pd.DataFrame(
        [{"類別": lab, "gain": fmt_gain(g), "占總 gain%": fmt_percent(gsh),
          "split 數": fmt_count(spc), "split 佔比": fmt_percent(ssh)}
         for (lab, g, gsh, spc, ssh) in present],
        columns=["類別", "gain", "占總 gain%", "split 數", "split 佔比"],
    )

    bullets = [
        f"以本次為例：total_gain {fmt_gain(s.get('total_gain'))} ＝ item 欄切點 "
        f"{fmt_gain(s.get('item_id_gain'))}（{fmt_percent(s.get('item_id_gain_share'))}）"
        f"＋ item 切點後 context {fmt_gain(s.get('context_gain'))}"
        f"（{fmt_percent(s.get('context_gain_share'))}）＋ item 切點前殘餘 "
        f"{fmt_gain(s.get('unaccounted_gain'))}"
        f"（{fmt_percent(s.get('unaccounted_gain_share'))}），三份加總為 1。",
        "判準：item 欄（prod_name）上的切點記 Item Prior；在 item 切點之後才"
        "出現的其他特徵切點記 Post-Item Context；其餘（item 切點之前）記未分配。",
        "名詞對照（下文交替使用、同指一塊）：未分配 ＝ Pre-Item ＝ item 切點前"
        "的殘餘 ＝ item 條件化之前的全域 context。",
        f"gain 與 split 兩套三分各自加總為 1，但形狀可以差很多：本次未分配只占 "
        f"gain {fmt_percent(s.get('unaccounted_gain_share'))}，卻占 split "
        f"{fmt_percent(s.get('unaccounted_split_share'))}——item 切點前有不少「便宜"
        "」的低 gain 切點。未分配那塊的拆解見下方專節。",
    ]

    return ReportSection(
        title="全模型 Gain 三分",
        description="全模型 split gain 分成三份：Item Prior、Post-Item Context、"
                    "未分配。下表列 gain 值、gain 佔比、split 數與 split 佔比。",
        formula=_FORMULA_BREAKDOWN,
        bullets=bullets,
        tables=[table],
        table_titles=["全模型 split gain 三分（gain 與 split 兩套佔比）"],
    )


#: per-item 明細表的欄位順序：身分 → gain（值/佔比/密度）→ split（數/佔比）→
#: 私有 → 路由足跡 → 能力橋接。fmt 器對每一欄照量的語意選（gain 用 fmt_gain、
#: 佔比用 fmt_percent、計數用 fmt_count、密度用 fmt_mean、AUC 用 fmt_auc）。
#: 欄名刻意與三分表的「占總 gain%」區隔：per-item 的份額分母是各 item 加總
#: （含共用切點重計），不是 total_gain——同名會讓讀者把 25.7% 誤讀成「占全模型
#: context 的 1/4」（讀者 subagent P1）。私有欄標「/本item」點出它的分母是該列
#: 自己的 context_gain，不是跨 item 加總（P4）。
_PER_ITEM_COLUMNS = [
    _ITEM, "名次", "context_gain", "gain 分配%", "/第一名", "/中位",
    "gain/split", "context 切點數", "split 分配%", "私有 gain", "私有/本item%",
    "item 切點(可達)", "first_tree", "觸及樹數", "qc_auc",
]


def _per_item_row(rank: int, r: dict) -> dict:
    return {
        _ITEM: str(r["item"]),
        "名次": rank,
        "context_gain": fmt_gain(r.get("context_gain")),
        "gain 分配%": fmt_percent(r.get("context_gain_share")),
        "/第一名": fmt_percent(r.get("gain_share_vs_max")),
        "/中位": fmt_percent(r.get("gain_share_vs_median")),
        "gain/split": fmt_mean(r.get("gain_per_split")),
        "context 切點數": fmt_count(r.get("context_split_count")),
        "split 分配%": fmt_percent(r.get("context_split_share")),
        "私有 gain": fmt_gain(r.get("context_gain_isolated")),
        "私有/本item%": fmt_percent(r.get("context_gain_isolated_share")),
        "item 切點(可達)": fmt_count(r.get("isolating_split_count")),
        "first_tree": fmt_count(r.get("first_tree_index")),
        "觸及樹數": fmt_count(r.get("n_trees_touched")),
        "qc_auc": fmt_auc(r.get("query_centered_auc")),
    }


def _per_item_section(result: dict) -> ReportSection | None:
    """3. per-item context 容量 ledger：分配長條圖（形狀）＋完整明細表。

    使用者回饋②：明細表保留 ledger 的全部容量原生欄位（split 數、私有
    context gain、item 路由足跡），不砍成只剩 gain；能力欄只放一欄
    ``query_centered_auc`` 當橋接（AP／n_pos／CI 留在 item_ability 報表，不
    在這裡重複）。
    """
    s = result.get("summary") or {}
    rows = [
        r for r in (result.get("per_item") or [])
        if r.get("context_gain_share") is not None
    ]
    if not rows:
        return None

    items = [str(r["item"]) for r in rows]
    shares = [float(r["context_gain_share"]) for r in rows]

    figures: list[Any] = []
    if _fits(len(rows)):
        figures.append(bar(
            x=items, y=shares,
            title="per-item context gain 佔比（遞減）",
            x_title=_ITEM, y_title="context_gain_share",
        ))

    table = pd.DataFrame(
        [_per_item_row(i, r) for i, r in enumerate(rows, 1)],
        columns=_PER_ITEM_COLUMNS,
    )

    bullets = [
        "「可達」＝順著一棵樹從根往下走、到某個節點時，這個 item 還沒被先前的 "
        "item 切點排除在候選之外。per-item 分帳全靠它：一個 context 切點記給"
        "「在該節點仍可達」的每個 item。",
        f"因此 per-item 的份額分母是「各 item 加總」（gain "
        f"{fmt_gain(s.get('sum_allocated_context_gain'))}、split "
        f"{fmt_count(s.get('sum_allocated_context_split'))}），不是全域"
        f"（{fmt_gain(s.get('context_gain'))}／"
        f"{fmt_count(s.get('context_split_count'))}）：一個共用切點被記給每個"
        "可達 item，所以加總必然大於全域，也不能與概覽的 556／2,229 直接相加對照。",
        "gain 分配%、split 分配% 是「占各 item 加總」的份額（分母已含共用重計），"
        "不是「占全模型」的比例——想知道每個 item 占全模型多少，看下一節「vs "
        "全模型」的涵蓋%（同一個 context_gain 換成 total_gain 當分母，數值不同"
        "不代表矛盾）。",
        "依 gain 分配% 遞減排序（沿用 compute 的順序），名次欄即此排名。",
        "gain 欄：context_gain＝分到的 context gain 值、gain 分配%＝占各 item "
        "加總比例、gain/split＝每個 context 切點平均分到的 gain。",
        "/第一名、/中位＝gain 分配% 換算成「相對第一名 item」「相對中位 item」"
        "的倍率（同一個量的兩個比較基準，看集中度用，不是獨立訊號）。",
        "split 欄：context 切點數＝該 item 可達時的 context 切點數、split 分配%"
        "＝占加總比例、item 切點(可達)＝該 item 尚可達時發生的 item 切點數。",
        "私有 gain＝該 item 是唯一可達 item 時的 context gain（非與他 item 共用）、"
        "私有/本item%＝私有占「該 item 自己的 context gain」比例（分母是本列的 "
        "context_gain，與前面跨 item 的份額不同尺）。",
        "first_tree＝首次作為 item 切點可達的樹序；觸及樹數＝被記到帳的樹數"
        "（可小於總樹數：某些樹裡這個 item 未被切點可達或未分到 context gain）。",
        "qc_auc＝同一次 item_ability 的 query-centered AUC（下一節散點的 y），"
        "放這裡只作對照，不是本項算出的量。",
    ]
    if not figures:
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "略過分配長條圖，只保留明細表。"
        )

    return ReportSection(
        title="per-item context 容量 ledger",
        description="每個 item 分到多少 post-item context 容量，加上它在 item "
                    "路由結構裡的足跡。",
        formula=_FORMULA_PER_ITEM,
        bullets=bullets,
        figures=figures,
        tables=[table],
        table_titles=["per-item context 容量明細"],
    )


def _vs_whole_model_section(result: dict) -> ReportSection | None:
    """(b) per-item context 容量跟**全模型**比：涵蓋（占 total_gain）＋獨佔
    （私有/total_gain）兩把尺。per-item ledger 的份額只能 item 之間互比；這一節
    讓讀者看到「item 在模型整體尺度上」有多大一塊。
    """
    rows = [
        r for r in (result.get("per_item") or [])
        if r.get("context_gain_vs_total") is not None
    ]
    if not rows:
        return None

    table = pd.DataFrame(
        [{_ITEM: str(r["item"]),
          "gain 涵蓋%": fmt_percent(r.get("context_gain_vs_total")),
          "gain 獨佔%": fmt_percent(r.get("context_gain_isolated_vs_total")),
          "split 涵蓋%": fmt_percent(r.get("context_split_vs_total")),
          "split 獨佔%": fmt_percent(r.get("context_split_isolated_vs_total"))}
         for r in rows],
        columns=[_ITEM, "gain 涵蓋%", "gain 獨佔%", "split 涵蓋%", "split 獨佔%"],
    )
    bullets = [
        "涵蓋%＝該 item 被切到的 context（gain 或切點數）相當於全模型總量的多少"
        "（gain 版分母＝total_gain、split 版＝total_split_count）。它是重疊量，"
        "跨 item 加總會超過 100%——共用切點記給每個可達 item。",
        "獨佔%＝僅該 item 唯一可達時的 context（不與他 item 共用）占全模型的多少。"
        "不重計，跨 item 加總並非 100%、而是遠小於——本模型絕大多數 context 是"
        "共用的。",
        "同一列 gain 與 split 兩套（涵蓋／獨佔）分母各是 total_gain／"
        "total_split_count；涵蓋 − 獨佔 ＝ 與他 item 共用的那部分。涵蓋高、獨佔低"
        "＝多來自共用切點；獨佔高＝較多是它單獨可達時分到的。只擺數字，判讀留你。",
    ]
    return ReportSection(
        title="per-item 容量 vs 全模型",
        description="把每個 item 的 context 容量放到全模型尺度上：gain 與 split "
                    "各給涵蓋（占全模型多少）與獨佔（私有部分占多少）兩把尺。",
        formula=_FORMULA_VS_TOTAL,
        bullets=bullets,
        tables=[table],
        table_titles=["per-item context 容量 vs 全模型"],
    )


def _pre_item_section(result: dict) -> ReportSection:
    """(d#1+d#2) 未分配（pre-item）拆解：按特徵的 gain 表 ＋ item 切點深度摘要。

    ``pre_item`` 為 None（舊版 ledger／粗帳本降級）時這一節仍存在，改用文字說明
    原因——「沒有拆解資料」與「這一節不存在」是兩件事（同 capacity_vs_ability
    的假綠警語）。
    """
    s = result.get("summary") or {}
    pre = result.get("pre_item")
    depth = result.get("first_item_split_depth")

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []

    def _depth_bullet() -> str | None:
        if not depth or depth.get("p50") is None:
            return None
        return (
            "item 切點坐落多深，決定它上方（即本節未分配這塊）壓了多少全域 "
            f"context：深度 node_depth（root=1）最淺 min={fmt_count(depth.get('min'))}"
            f"、中位 p50={_fmt_depth(depth.get('p50'))}、最深 max="
            f"{fmt_count(depth.get('max'))}，算在 "
            f"{fmt_count(depth.get('n_trees_with_item_split'))} 棵有 item 切點的樹上"
            "（越淺＝越早條件化到 item、未分配越薄）。"
        )

    if not pre or pre.get("gain_sum") is None:
        bullets = [
            "本次 ledger 沒有 pre-item 拆解（舊版 ledger 尚未重生，或粗帳本降級"
            "——原因見完整性檢查）。重生 gain_ledger 後這裡會列出未分配 gain 由"
            "哪些特徵撐起。",
        ]
        d = _depth_bullet()
        if d:
            bullets.append(d)
        return ReportSection(
            title="未分配（pre-item）拆解",
            description="未分配那塊由哪些特徵撐起——本次沒有可拆解的資料。",
            bullets=bullets,
        )

    by_feat = list((pre.get("by_feature") or {}).items())
    gain_sum = float(pre["gain_sum"])
    split_sum = int(pre.get("split_count") or 0)

    def _gpct(g):
        return fmt_percent(None if not gain_sum else float(g) / gain_sum)

    def _spct(c):
        return fmt_percent(None if not split_sum else float(c) / split_sum)

    shown = by_feat[:_MAX_PRE_ITEM_FEATURES]
    table_rows = [
        {"特徵": f, "gain": fmt_gain(v.get("gain")),
         "占未分配 gain%": _gpct(v.get("gain", 0.0)),
         "split 數": fmt_count(v.get("split_count")),
         "占未分配 split%": _spct(v.get("split_count", 0))}
        for f, v in shown
    ]
    rest = by_feat[_MAX_PRE_ITEM_FEATURES:]
    if rest:
        rest_gain = sum(float(v.get("gain", 0.0)) for _, v in rest)
        rest_split = sum(int(v.get("split_count", 0)) for _, v in rest)
        table_rows.append({
            "特徵": f"其餘 {len(rest)} 個特徵",
            "gain": fmt_gain(rest_gain),
            "占未分配 gain%": _gpct(rest_gain),
            "split 數": fmt_count(rest_split),
            "占未分配 split%": _spct(rest_split),
        })
    tables.append(pd.DataFrame(
        table_rows,
        columns=["特徵", "gain", "占未分配 gain%", "split 數", "占未分配 split%"],
    ))
    table_titles.append("未分配（pre-item）gain 按特徵")

    bullets = [
        f"未分配 gain 加總 {fmt_gain(gain_sum)}（{fmt_count(pre.get('split_count'))} "
        f"個切點）＝三分表的未分配那塊（占全模型 gain "
        f"{fmt_percent(s.get('unaccounted_gain_share'))}）；下表把它拆到特徵。",
        "這些是 item 切點之前（模型還沒經過任何 item 切點、還沒條件化到哪個 "
        "item——同 per-item 節「可達」的機制）的全域 context 切點，對所有 item "
        "一視同仁。下表列各特徵在這塊分到的 gain，分母＝未分配 gain 總和。",
    ]
    d = _depth_bullet()
    if d:
        bullets.append(d)
    if rest:
        bullets.append(
            f"特徵共 {len(by_feat)} 個，表只列 gain 前 {_MAX_PRE_ITEM_FEATURES} 個，"
            f"其餘 {len(rest)} 個併成一列，不靜默截斷。"
        )

    return ReportSection(
        title="未分配（pre-item）拆解",
        description="未分配那塊（item 切點之前的全域 context）由哪些特徵撐起，"
                    "加上 item 條件化坐落多深。",
        formula=_FORMULA_PRE_ITEM,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _capacity_vs_ability_section(result: dict) -> ReportSection:
    """4. capacity vs ability 散點。``item_ability`` 缺席時略過畫圖，改用文字
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
            "x（context_gain_share）＝上一節 ledger 的「gain 分配%」同一欄；y＝"
            "item_ability 的 query_centered_auc——兩項獨立診斷的量，只在這裡並排"
            "對照，散點不宣稱誰造成誰。",
        ]
        description = (
            "每個 item 一個點：x 是分到的 context gain 份額（＝ledger 的 gain "
            "分配%），y 是同一次執行算出的 query-centered AUC。"
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
    """5. 本次執行的完整性檢查——這一節不可省，空的時候也照樣印「無」。"""
    notes = result.get("notes") or []
    summary = result.get("summary") or {}

    bullets = [
        "計算層 notes（含缺席資料的成因、item_ability join 狀況）："
        + (f"{len(notes)} 則，列於下方" if notes else "無"),
    ]
    bullets.extend(str(n) for n in notes)
    if summary.get("n_items") is not None:
        bullets.append(f"本次讀到的 item 數：{summary['n_items']}。")
    if summary.get("n_trees") is not None:
        bullets.append(f"本次 booster 樹數：{fmt_count(summary['n_trees'])}。")
    if summary.get("total_split_count") is None:
        bullets.append(
            "這份 gain_ledger 沒有 total_split_count（舊版，早於 Route A）"
            "——split 三分佔比、未分配 split 數留空；重生 gain_ledger 即補上。"
        )
    if result.get("pre_item") is None:
        bullets.append(
            "這份 gain_ledger 沒有 pre-item 拆解與 item 切點深度（舊版或粗帳本"
            "降級）——「未分配拆解」節無資料可列；重生 gain_ledger 即補上。"
        )

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
        _overview_section(result),
        _breakdown_section(result),
        _per_item_section(result),
        _vs_whole_model_section(result),
        _pre_item_section(result),
        _capacity_vs_ability_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
