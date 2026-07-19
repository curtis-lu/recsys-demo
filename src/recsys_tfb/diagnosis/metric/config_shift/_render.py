"""config_shift 的呈現層：把 ``compute`` 的 JSON 轉成一個 ``ReportSection``。

**為什麼這一層是純函式**：呈現是「常常變動」的那一層（換圖、換排版、換措辭），
計算是「該持久化」的那一層。只要 ``render`` 只吃 ``compute`` 的 dict，使用者就
能把公司環境跑出來的 JSON 拷回本機、用離線工具秒級重繪，不必為了改一句說明
重跑一次 Spark。所以這個模組**不 import pyspark、不讀檔、不做任何計算**——
出現在報表上的每個數字都必須已經在 JSON 裡。

**呈現層的三條鐵則**（整個 diag-redesign 的共同約定，後四項診斷照抄這個形狀）：

1. 不下結論——沒有 severity／verdict／建議動作，也不替讀者判讀 CI 有沒有跨 0。
2. 不設門檻——不拿 config 門檻把連續量切成離散類別；顏色只編碼大小與正負。
   唯一的門檻是 ``MAX_FIGURE_POINTS``，它管的是**繪圖能力**不是資料的意義，
   所以超標時只換呈現形式（圖 → 表），一列資料都不丟。
3. 每個數字自帶說明——範圍說明由 :data:`SCOPE` 擁有（見 ``__init__``）。執行期
   才知道的抽樣設計由**組裝層**填進 ``SCOPE.sampling``，不在這裡：五項診斷共用
   同一份 ``diagnosis_sample``，各放一份填值 helper 只會得到五份會漂移的同義碼。

**圖形預算為什麼非處理不可**：公司環境的 context 群 ＝
``sample_group_keys ∪ sample_weight_keys − {item, label}`` 的笛卡兒積，乘上 item
數很容易破 2000 格，而 ``report.figures`` 的 ``assert_within_budget`` 是 raise
不是警告——沒有這裡的降級，第一次公司環境 real-run 會讓整個 evaluation 掛掉，
而那是整個開發迴路裡最貴的一次迭代。
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


def _own_package():
    """本套件的 ``__init__``（``NAME``／``TITLE``／``SCOPE`` 的唯一定義處）。

    **為什麼是函式內 import**：``__init__`` 在 module 層級 ``from ._render import
    render``，本模組若在 module 層級反向 import 它就是循環 import。延到呼叫時才
    取，父套件早已初始化完畢。代價是多一次字典查找，換到的是「標題只有一份
    定義」——兩邊各寫一份字串，改了其中一邊不會有任何東西變紅。
    """
    import recsys_tfb.diagnosis.metric.config_shift as pkg

    return pkg


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


def _bullets(lines: list[str]) -> str:
    return "\n".join(f"- {line}" for line in lines)


def render(result: dict, parameters: dict) -> ReportSection | None:
    """把 ``compute`` 的輸出轉成一個報表章節；停用時回 ``None``。

    回 ``None`` 而不是空 section 是刻意的：``None`` 代表「這頁不存在」，空
    section 會在報表上長成「量過了、什麼都沒有」——讀者無從分辨「沒開這項診斷」
    與「開了但 Δ ≈ 0」，而那兩件事的結論完全相反。
    """
    if not result.get("enabled"):
        return None

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    paragraphs: list[str] = []

    matrix_centered = result.get("offset_centered") or {}
    matrix_raw = result.get("offset_matrix") or {}
    contexts, items = _matrix_axes(result, matrix_centered or matrix_raw)
    n_cells = len(contexts) * len(items)

    # ---- 1. offset 矩陣：熱圖，或（超預算時）完整表格 ----
    if n_cells:
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
            paragraphs.append(
                "熱圖顯示的值已扣掉各 context 群內的中位數（純美觀，不影響任何"
                "結論；未扣中位數的原始值在 JSON 的 offset_matrix）。空白格代表"
                "該 (context 群, item) 組合沒有出現在本次抽樣裡。"
            )
        else:
            tables.append(_matrix_table(matrix_raw, contexts, items))
            table_titles.append("每個 (context 群, item) 的理論 log-odds 偏移")
            paragraphs.append(
                f"context 群 {len(contexts)} × item {len(items)} ＝ {n_cells} 格，"
                f"超過單張圖的 {MAX_FIGURE_POINTS} 點上限，矩陣以表格呈現"
                "（只列實際觀測到的組合；觀測到的一列都沒有省略）。"
            )

    # ---- 2. 群內 offset 範圍 ----
    spread = result.get("offset_spread_by_context") or {}
    if spread:
        if _fits(len(spread)):
            # 不給 center：spread 是 max − min，非負量，不是有號量。
            figures.append(bar(
                x=list(spread), y=[float(v) for v in spread.values()],
                title="各 context 群的群內 offset 範圍",
                x_title="context 群", y_title=_SPREAD,
            ))
        else:
            tables.append(pd.DataFrame(
                [{_CTX: g, _SPREAD: fmt_logodds(v)} for g, v in spread.items()],
                columns=[_CTX, _SPREAD],
            ))
            table_titles.append("各 context 群的群內 offset 範圍")
            paragraphs.append(
                f"context 群共 {len(spread)} 個，超過單張圖的 {MAX_FIGURE_POINTS} "
                "點上限，群內 offset 範圍以表格呈現。"
            )

    # ---- 3. 真正抵達排序的偏移：逐 query 的範圍分布 ----
    qspread = result.get("query_offset_spread") or {}
    if qspread:
        tables.append(pd.DataFrame(
            [
                {"統計量": stat, "值（log-odds）": fmt_logodds(qspread.get(stat))}
                for stat in ("mean", "p50", "p90", "max")
            ],
            columns=["統計量", "值（log-odds）"],
        ))
        table_titles.append("逐 query 的 offset 範圍（max − min）分布")
        # 刻意不寫「上表」：description 與 tables 是兩個獨立欄位，版面上誰先誰後
        # 由報表層決定，位置性指稱會在改版時默默指到別張表。一律直呼表名。
        paragraphs.append(
            "「逐 query 的 offset 範圍」表是在實際樣本上逐 query 算 max − min 的"
            "分布，依 inclusion_weight 加權，分位數為 inverse-CDF 定義、不插值。涵蓋 "
            f"{fmt_count(qspread.get('n_queries'))} 個 query，其中候選 item 數 ≥ 2 "
            f"的有 {fmt_count(qspread.get('n_queries_multi_candidate'))} 個"
            "（單候選 query 的範圍結構性為 0）。"
        )

    # ---- 4. Δ 與區間（只給數字與區間，判讀留給讀者）----
    delta = result.get("delta")
    if delta is None:
        paragraphs.append("Δ 未計算。")
    else:
        line = (
            f"Δ ＝ {fmt_delta(delta)}"
            f"（corrected_map {fmt_ap(result.get('corrected_map'))} − "
            f"baseline_map {fmt_ap(result.get('baseline_map'))}）。"
        )
        lo, hi = result.get("delta_ci_low"), result.get("delta_ci_high")
        if lo is not None and hi is not None:
            n_boot = (result.get("ci") or {}).get("n_boot")
            line += (
                f"95% 區間 [{fmt_delta(lo)}, {fmt_delta(hi)}]"
                f"（分層配對 cluster bootstrap，{fmt_count(n_boot)} 次重抽）。"
            )
        paragraphs.append(line)

    # ---- 5./6. 逐 item 的 Δ_j ----
    per_item = result.get("per_item") or []
    if per_item:
        deltas = [float(r["delta_j"]) for r in per_item]
        if _fits(len(per_item)) and np.isfinite(deltas).any():
            figures.append(bar(
                x=[r["item"] for r in per_item], y=deltas,
                title="逐 item 替換：只扣掉這個 item 的 offset 之後的 Δ_j",
                x_title="item", y_title="Δ_j",
                center=0.0,  # 有號量 → 發散色階
            ))
        paragraphs.append(
            "Σ Δ_j ≠ Δ：Δ_j 是一次只扣掉一個 item 的替換實驗，同一個 query 內的"
            "名次互相耦合，逐 item 的量不可相加成整體的 Δ。"
        )
        tables.append(pd.DataFrame(
            [
                {
                    _ITEM: r["item"],
                    "Δ_j": fmt_delta(r.get("delta_j")),
                    "n_pos_raw": fmt_count(r.get("n_pos_raw")),
                    # 加權和不是整數，捨入到整數會讓 61.5 與 28.5 往相反方向跑。
                    "n_pos_effective": fmt_weighted_count(r.get("n_pos_effective")),
                }
                for r in per_item
            ],
            columns=[_ITEM, "Δ_j", "n_pos_raw", "n_pos_effective"],
        ))
        table_titles.append("逐 item 的 Δ_j 與正例規模")
        paragraphs.append(
            "n_pos_raw 是正例列數（未加權）；n_pos_effective 是那些列的 "
            "inclusion_weight 之和——mAP 與 min_positives／shrinkage_k／"
            "weight_alpha 吃的是後者。"
        )

    # ---- 7. 樣本規模 ----
    sample = result.get("sample") or {}
    if sample:
        paragraphs.append(
            "本次診斷抽樣的規模："
            f"{fmt_count(sample.get('n_queries'))} 個 query、"
            f"{fmt_count(sample.get('n_items'))} 個 item、"
            f"{fmt_count(sample.get('n_positive_rows'))} 列正例"
            f"（加權後 {fmt_weighted_count(sample.get('n_positive_rows_effective'))}）。"
        )

    # ---- 8. 可見性區塊 ----
    # 三者都是「診斷看不見某樣東西」的觀測，而看不見與量到零在報表上長得一模
    # 一樣：Δ ≈ 0 是本項診斷宣稱可以排除整個方向的訊號，但查表全未命中會偽造出
    # 同樣的 Δ ＝ 0；offset 矩陣少一列，跟該 item 沒有偏移也長得一樣。所以空的
    # 時候也照樣印「無」——讀者要看得出這三件事被檢查過。
    unmatched = result.get("unmatched_override_keys") or []
    not_observed = result.get("items_declared_not_observed") or []
    notes = result.get("notes") or []
    visibility = [
        "零命中的 override key（config 宣告了、本次樣本一次都沒查到；對 offset "
        "沒有作用）："
        + (f"{len(unmatched)} 個，見下表" if unmatched else "無"),
        "schema 宣告了、本次抽樣未出現的 item（不在 offset 矩陣、群內範圍與逐 "
        "item 表之中）：" + (", ".join(map(str, not_observed)) if not_observed else "無"),
        "計算層 notes：" + (f"{len(notes)} 則，列於下方" if notes else "無"),
    ]
    paragraphs.append("可見性（以下三件事被檢查過，內容如實列出）：\n" + _bullets(visibility))
    if notes:
        paragraphs.append(_bullets([str(n) for n in notes]))
    if unmatched:
        tables.append(pd.DataFrame(
            [{"config": r.get("config"), "key": r.get("key")} for r in unmatched],
            columns=["config", "key"],
        ))
        table_titles.append("本次樣本零命中的 override key")

    return ReportSection(
        title=_own_package().TITLE,
        description="\n\n".join(paragraphs),
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )
