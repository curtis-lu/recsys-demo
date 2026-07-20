"""config_shift 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

**為什麼這一層是純函式**：呈現是「常常變動」的那一層（換圖、換排版、換措辭），
計算是「該持久化」的那一層。只要 ``render`` 只吃 ``compute`` 的 dict，使用者就
能把公司環境跑出來的 JSON 拷回本機、用離線工具秒級重繪，不必為了改一句說明
重跑一次 Spark。所以這個模組**不 import pyspark、不讀檔、不做任何計算**——
出現在報表上的每個數字都必須已經在 JSON 裡。

**為什麼是「多個 section」而不是一個**（版面定案，後四項診斷照抄這個形狀）：
第一版把全部說明串成一個 ``description`` 塞進單一 section，使用者的回饋是
「這整段太冗長，看報表的人不會有耐心看完全部」與「說明的地方集中在上面很難
知道你要描述的圖表是哪一個」。``report/pages.py`` 本來就支援每個 section 各自
一個 ``<h2>`` 與自己的說明——擠成一段是這一層的選擇，不是渲染層的限制。所以
現在的形狀是：

    一個 section ＝ 一句 description ＋ 一行 formula ＋ 最多 3 則 bullet
                    ＋ 它自己的那張圖或那張表

``formula`` 是第二點回饋（「強烈建議應該附上公式，讓讀者一眼就知道這個圖表的
數字是怎麼算出來的」）的執行點。數學符號一律用 **Unicode 純文字**
（``Δ``／``Σ``／``≠``／``ln``／``·``），**不引入 MathJax／KaTeX**——生產限制是
no network、no additional packages，外部 CDN 一定載不到，報表上只會留下一段
沒被渲染的原始碼。

**呈現層的三條鐵則**（整個 diag-redesign 的共同約定）：

1. 不下結論——沒有 severity／verdict／建議動作，也不替讀者判讀 CI 有沒有跨 0。
2. 不設門檻——不拿 config 門檻把連續量切成離散類別；顏色只編碼大小與正負。
   唯一的門檻是 ``MAX_FIGURE_POINTS``，它管的是**繪圖能力**不是資料的意義，
   所以超標時只換呈現形式（圖 → 表），一列資料都不丟。
3. 每個數字自帶說明——範圍說明由 :data:`SCOPE` 擁有（見 ``__init__``）。執行期
   才知道的抽樣設計由**組裝層**填進 ``SCOPE.sampling``，不在這裡：五項診斷共用
   同一份 ``diagnosis_sample``，各放一份填值 helper 只會得到五份會漂移的同義碼。

**bullet 不重複 ``ScopeNote`` 的內容**：ScopeNote 已經是頁首的獨立區塊，把它抄
一份進 bullets 只會讓頁面更長——那正是這次要修的問題。唯一的例外是「spread ＝ 0
代表零影響」，它在這裡的角色是**那張圖的判讀方式**（哪幾根條子代表什麼），不是
頁面層級的範圍宣告。

**圖形預算為什麼非處理不可**：公司環境的 context 群 ＝
``sample_group_keys ∪ sample_weight_keys − {item, label}`` 的笛卡兒積，乘上 item
數很容易破 2000 格，而 ``report.figures`` 的 ``assert_within_budget`` 是 raise
不是警告——沒有這裡的降級，第一次公司環境 real-run 會讓整個 evaluation 掛掉，
而那是整個開發迴路裡最貴的一次迭代。降級敘述放進**被降級的那個 section 自己的
bullets**，不是集中到頁首：讀者要在那張表旁邊就看到「為什麼這裡是表不是圖」。
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import MAX_FIGURE_POINTS, bar, heatmap
from recsys_tfb.report.fmt import (
    fmt_ap,
    fmt_count,
    fmt_delta,
    fmt_logodds,
    fmt_weighted_count,
)

_CTX = "context 群"
_ITEM = "item"
_OFFSET = "offset（log-odds）"
_SPREAD = "群內 offset 範圍 max − min（log-odds）"

#: offset 的定義。第二行是符號表——公式裡的每個字母都要能對回 config 的鍵名，
#: 否則讀者知道「怎麼算」卻不知道「算的是哪個設定」。
_FORMULA_OFFSET = (
    "offset(a, j) = ln(r₊(a,j) / r₋(a,j)) + ln(w₊(a,j) / w₋(a,j))\n"
    "r ＝ dataset.sample_ratio_overrides 的抽樣比例；"
    "w ＝ training.sample_weights 的樣本權重；下標 ＋/− ＝ label 1／0"
)


def _fits(n_points: int) -> bool:
    """``n_points`` 是否在單張圖的預算內。

    刻意與 ``figures.assert_within_budget`` 用同一個常數、同一個比較方向——
    兩邊各寫一份門檻的話，會出現「這裡判斷畫得下、那裡 raise」的死角。
    """
    return n_points <= MAX_FIGURE_POINTS


def _matrix_axes(result: dict, matrix: dict) -> tuple[list[str], list[str]]:
    """(context 群順序, item 順序)。兩者都沿用 ``compute`` 的順序，不重排。

    軸序原樣保留是 ``report.figures`` 的既有約定：之後若要兩張矩陣並排對照，
    任何一邊偷偷排序都會讓逐格比較失效。
    """
    contexts = [str(g) for g in matrix]
    items = [str(i) for i in (result.get("items") or [])]
    if not items:  # items 缺席時退回矩陣自己觀測到的 item（保順序、去重）
        seen: dict[str, None] = {}
        for per_item in matrix.values():
            for item in per_item:
                seen.setdefault(str(item), None)
        items = list(seen)
    return contexts, items


def _matrix_z(matrix: dict, contexts: list[str], items: list[str]) -> np.ndarray:
    """矩陣攤成 rect 陣列；未觀測到的 (context, item) 留 NaN（圖上是空白格）。"""
    return np.array(
        [[matrix.get(g, {}).get(i, np.nan) for i in items] for g in contexts],
        dtype=float,
    )


def _matrix_table(matrix: dict, contexts: list[str], items: list[str]) -> pd.DataFrame:
    """長格式（context 群, item, offset）。

    刻意用長格式而不是寬格式：這張表只在超過繪圖預算時出現，也就是矩陣「大到
    畫不下」的時候——寬格式在 item 多的那一側會變成上千個欄位，長格式至少還
    是三欄。**未觀測到的組合不補列**，補了會讓「沒出現」看起來像「offset ＝ 0」。
    """
    rows = [
        {_CTX: g, _ITEM: i, _OFFSET: fmt_logodds(matrix[g][i])}
        for g in contexts
        for i in items
        if i in matrix.get(g, {})
    ]
    return pd.DataFrame(rows, columns=[_CTX, _ITEM, _OFFSET])


def _offset_matrix_section(result: dict) -> ReportSection | None:
    """1. offset 矩陣：熱圖，或（超預算時）完整表格。"""
    matrix_centered = result.get("offset_centered") or {}
    matrix_raw = result.get("offset_matrix") or {}
    contexts, items = _matrix_axes(result, matrix_centered or matrix_raw)
    n_cells = len(contexts) * len(items)
    if not n_cells:
        return None

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []

    z = _matrix_z(matrix_centered, contexts, items)
    # 全 NaN（軸與矩陣鍵對不上）會讓色階無從計算，是壞資料而不是良性退化，
    # 但呈現層不該為此炸掉整份報表——退成表格，讓讀者自己看得到原始值。
    drawable = _fits(n_cells) and bool(np.isfinite(z).any())
    if drawable:
        figures.append(heatmap(
            z=z, x=items, y=contexts,
            title="每個 (context 群, item) 的理論 log-odds 偏移",
            colorbar_title="offset（已置中）",
            center=0.0,
        ))
        bullets = [
            "熱圖顯示的值已扣掉各 context 群內的中位數，未扣的原始值在 JSON 的 "
            "offset_matrix。",
            "空白格代表該 (context 群, item) 組合沒有出現在本次抽樣裡。",
        ]
    else:
        tables.append(_matrix_table(matrix_raw, contexts, items))
        table_titles.append("每個 (context 群, item) 的理論 log-odds 偏移")
        bullets = [
            f"context 群 {len(contexts)} × item {len(items)} ＝ {n_cells} 格，超過"
            f"單張圖的 {MAX_FIGURE_POINTS} 點上限，改以表格呈現。",
            "表格只列實際觀測到的組合，觀測到的一列都沒有省略。",
            "表格顯示的是未置中的原始 offset。",
        ]

    return ReportSection(
        title="offset 矩陣（context 群 × item）",
        description="抽樣比例與樣本權重在理論上對每個 (context 群, item) 引入的分數偏移。",
        formula=_FORMULA_OFFSET,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _context_spread_section(result: dict) -> ReportSection | None:
    """2. 群內 offset 範圍。"""
    spread = result.get("offset_spread_by_context") or {}
    if not spread:
        return None

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        "spread ＝ 0 的群，群內 offset 均勻，對 query 內名次零影響"
        "（可直接推導，不需估計）。",
    ]

    if _fits(len(spread)):
        # 不給 center：spread 是 max − min，非負量，不是有號量。
        figures.append(bar(
            x=list(spread), y=[float(v) for v in spread.values()],
            title="各 context 群的群內 offset 範圍",
            x_title=_CTX, y_title=_SPREAD,
        ))
    else:
        tables.append(pd.DataFrame(
            [{_CTX: g, _SPREAD: fmt_logodds(v)} for g, v in spread.items()],
            columns=[_CTX, _SPREAD],
        ))
        table_titles.append("各 context 群的群內 offset 範圍")
        bullets.append(
            f"context 群共 {len(spread)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點"
            "上限，改以表格呈現。"
        )

    return ReportSection(
        title="群內 offset 範圍",
        description="同一個 context 群內部，offset 從最小到最大的跨距。",
        formula="spread(a) = maxⱼ offset(a, j) − minⱼ offset(a, j)",
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _query_spread_section(result: dict) -> ReportSection | None:
    """3. 真正抵達排序的偏移：逐 query 的範圍分布。"""
    qspread = result.get("query_offset_spread") or {}
    if not qspread:
        return None

    table = pd.DataFrame(
        [
            {"統計量": stat, "值（log-odds）": fmt_logodds(qspread.get(stat))}
            for stat in ("mean", "p50", "p90", "max")
        ],
        columns=["統計量", "值（log-odds）"],
    )
    return ReportSection(
        title="逐 query 的 offset 範圍",
        description="在實際樣本上逐 query 算 offset 的 max − min，再看這些值的分布。",
        formula="qspread(q) = max offset − min offset（q 內各列）",
        bullets=[
            "分布依 inclusion_weight 加權。",
            "分位數採 inverse-CDF 定義，不插值。",
            f"涵蓋 {fmt_count(qspread.get('n_queries'))} 個 query，其中候選 item 數 "
            f"≥ 2 的有 {fmt_count(qspread.get('n_queries_multi_candidate'))} 個"
            "（單候選 query 的範圍結構性為 0）。",
        ],
        tables=[table],
        table_titles=["逐 query 的 offset 範圍（max − min）分布"],
    )


def _delta_section(result: dict) -> ReportSection:
    """4. Δ 與區間（只給數字與區間，判讀留給讀者）。

    這一節刻意沒有圖也沒有表：它就是兩三個數字，畫成圖只是把一個數字變成一根
    條子。數字放在 bullets 裡，讀者掃過去就看得完。
    """
    score_col = result.get("score_col_used") or "score"
    delta = result.get("delta")
    if delta is None:
        bullets = ["Δ 未計算。"]
    else:
        bullets = [
            f"Δ ＝ {fmt_delta(delta)}"
            f"（corrected_map {fmt_ap(result.get('corrected_map'))} − "
            f"baseline_map {fmt_ap(result.get('baseline_map'))}）。"
        ]
        lo, hi = result.get("delta_ci_low"), result.get("delta_ci_high")
        if lo is not None and hi is not None:
            n_boot = (result.get("ci") or {}).get("n_boot")
            bullets.append(
                f"95% 區間 [{fmt_delta(lo)}, {fmt_delta(hi)}]，來自分層配對 cluster "
                f"bootstrap {fmt_count(n_boot)} 次重抽。"
            )

    return ReportSection(
        title="扣掉 offset 之後的 mAP 變化",
        description="把理論 offset 從分數裡扣掉之後，macro per-item mAP 的變化量。",
        formula=(
            "Δ = mAP(F − offset) − mAP(F)\n"
            f"F ＝ {score_col} 的 log-odds"
        ),
        bullets=bullets,
    )


def _per_item_section(result: dict) -> ReportSection | None:
    """5. 逐 item 的替換實驗。"""
    per_item = result.get("per_item") or []
    if not per_item:
        return None

    figures: list[Any] = []
    deltas = [float(r["delta_j"]) for r in per_item]
    bullets = [
        "Σ Δⱼ ≠ Δ：同一個 query 內的名次互相耦合，逐 item 的量不可相加成整體的 Δ。",
        "n_pos_raw 是正例列數，未加權。",
        "n_pos_effective ＝ Σ inclusion_weight；mAP 與 min_positives／shrinkage_k／"
        "weight_alpha 吃的是後者。",
    ]

    if _fits(len(per_item)) and np.isfinite(deltas).any():
        figures.append(bar(
            x=[r["item"] for r in per_item], y=deltas,
            title="逐 item 替換：只扣掉這個 item 的 offset 之後的 Δⱼ",
            x_title=_ITEM, y_title="Δⱼ",
            center=0.0,  # 有號量 → 發散色階
        ))
    else:
        bullets.append(
            f"item 共 {len(per_item)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "Δⱼ 只以表格呈現。"
        )

    table = pd.DataFrame(
        [
            {
                _ITEM: r["item"],
                "Δⱼ": fmt_delta(r.get("delta_j")),
                "n_pos_raw": fmt_count(r.get("n_pos_raw")),
                # 加權和不是整數，捨入到整數會讓 61.5 與 28.5 往相反方向跑。
                "n_pos_effective": fmt_weighted_count(r.get("n_pos_effective")),
            }
            for r in per_item
        ],
        columns=[_ITEM, "Δⱼ", "n_pos_raw", "n_pos_effective"],
    )

    return ReportSection(
        title="逐 item 的替換實驗",
        description="一次只扣掉一個 item 的 offset，其餘 item 不動。",
        formula="Δⱼ = mAP(F − offset·1[item = j]) − mAP(F)",
        bullets=bullets,
        figures=figures,
        tables=[table],
        table_titles=["逐 item 的 Δⱼ 與正例規模"],
    )


def _visibility_section(result: dict) -> ReportSection:
    """6. 本次執行的完整性檢查。**這一節不可省，空的時候也照樣印「無」。**

    這一節與頁首 ``ScopeNote`` 的「推論不到什麼」是**不同性質的東西**，早期
    兩者都叫「看不見什麼」而使用者當場指出標題與內容對不起來：

    * ``ScopeNote.blind_to`` ＝ 這個指標**結構上**推論不到的事。與這次跑了什麼
      資料無關，永遠成立。
    * 這一節 ＝ 三種**已知的靜默失效**在**本次執行**的實際結果。會隨每次執行
      變動，正常情況下三項都是「無」。

    為什麼空的時候也要印：這三種失效都會讓數字看起來正常、實際上沒量到。
    Δ ≈ 0 是本項診斷宣稱可以排除整個方向的訊號，但查表全未命中會偽造出同樣的
    Δ ＝ 0；offset 矩陣少一列，跟該 item 沒有偏移也長得一樣。讀者要看得出這三
    件事被檢查過、結果是什麼——「沒有這一節」與「這一節全是無」對讀者是天差
    地別的兩件事。
    """
    unmatched = result.get("unmatched_override_keys") or []
    not_observed = result.get("items_declared_not_observed") or []
    notes = result.get("notes") or []
    sample = result.get("sample") or {}

    bullets = [
        "零命中的 override key（config 宣告了、本次樣本一次都沒查到，對 offset "
        "沒有作用）："
        + (f"{len(unmatched)} 個，見下表" if unmatched else "無"),
        "schema 宣告了、本次抽樣未出現的 item（不在 offset 矩陣、群內範圍與逐 "
        "item 表之中）："
        + (", ".join(map(str, not_observed)) if not_observed else "無"),
        "計算層 notes：" + (f"{len(notes)} 則，列於下方" if notes else "無"),
    ]
    # 計算層的 notes 原文照登——它們是 compute 對自己這次執行的觀測，改寫就失真。
    bullets.extend(str(n) for n in notes)

    if sample:
        bullets.append(
            "本次診斷抽樣的規模："
            f"{fmt_count(sample.get('n_queries'))} 個 query、"
            f"{fmt_count(sample.get('n_items'))} 個 item、"
            f"{fmt_count(sample.get('n_positive_rows'))} 列正例"
            f"（加權後 {fmt_weighted_count(sample.get('n_positive_rows_effective'))}）。"
        )

    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    if unmatched:
        tables.append(pd.DataFrame(
            [{"config": r.get("config"), "key": r.get("key")} for r in unmatched],
            columns=["config", "key"],
        ))
        table_titles.append("本次樣本零命中的 override key")

    return ReportSection(
        title="本次執行的完整性檢查",
        description=(
            "以下三種情況會讓上面的數字看起來正常、實際上沒量到。"
            "每項列出本次執行的實際結果。"
        ),
        bullets=bullets,
        tables=tables,
        table_titles=table_titles,
    )


def render(result: dict, parameters: dict) -> tuple[ReportSection, ...]:
    """把 ``compute`` 的輸出轉成一串報表章節；停用時回空 tuple。

    回**空 tuple** 而不是空 section 是刻意的：空 tuple 讓組裝層跳過整頁，空
    section 會在報表上長成「量過了、什麼都沒有」——讀者無從分辨「沒開這項診斷」
    與「開了但 Δ ≈ 0」，而那兩件事的結論完全相反。

    順序即閱讀順序，也是因果順序：先看 offset 長什麼樣（1–3），再看扣掉它之後
    mAP 動了多少（4–5），最後看這次量不到什麼（6）。**第 6 節永遠在**——「看不見」
    與「量到零」在報表上長得一樣，少了它，讀者會把前者讀成後者。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _offset_matrix_section(result),
        _context_spread_section(result),
        _query_spread_section(result),
        _delta_section(result),
        _per_item_section(result),
        _visibility_section(result),
    ]
    return tuple(s for s in sections if s is not None)
