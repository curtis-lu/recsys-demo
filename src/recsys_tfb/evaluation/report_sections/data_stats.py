"""基本統計 (資料統計 in ADR-0031 decision 16): the size of the evaluated
data and its positives, overall and per time value, item, segment and
category."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    kept_zero_positive_groups_note,
    read_dataset_overview,
    section_on,
    unmatched_last,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_row_facts",)


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    if not section_on(parameters, "dataset_overview"):
        return None
    facts = results["evaluation_row_facts"]
    ov = read_dataset_overview(facts)
    totals_d = ov.get("totals", {}) or {}
    totals = pd.DataFrame([totals_d]).T
    totals.columns = ["value"]
    by_snap = pd.DataFrame(ov.get("by_snap_date", {})).T

    # per-item 正例組成：正例數 / 正樣本率 / 正例佔比（＝n_positives÷總正例，
    # render 端純算術、無 Spark）。密集候選下三欄同序，範圍說明於 description。
    total_pos = totals_d.get("n_positives") or 0
    by_item_rows: dict = {}
    # per-item 列序統一按字母（與衡量指標、item-share 對齊）
    for item, d in sorted((ov.get("by_item", {}) or {}).items()):
        n_pos = d.get("n_positives")
        by_item_rows[item] = {
            "正例數": n_pos,
            "正樣本率(÷此item候選列)": d.get("positive_rate"),
            "正例佔比": (n_pos / total_pos)
            if (n_pos is not None and total_pos) else None,
        }
    by_item = pd.DataFrame(by_item_rows).T

    tables = [totals, by_snap, by_item]
    titles = ["整體 totals", "各期 by snap_date", "per-item 正例組成"]
    collapsed = [False, True, False]   # 各期單 snap 時＝totals，預設收合
    # per-segment 正例組成：正例數／正樣本率（÷該 segment 候選列）／query 數佔比。
    # 第 3 欄用「query 數佔比」（該 segment 佔多少 query，反映 segment 大小）——
    # segment 分的是 query、per-item 分的是正例，兩者不同軸，故不與 per-item 的
    # 「正例佔比」互換。by_segment 由 compute_dataset_overview 依 active_seg_col
    # 聚合；缺席（舊 artifact 或無 segment 欄）則不呈現此表。對不到值的
    # (unmatched) 排最後：它不是一個 segment，列出來是為了交代它有多少 query。
    by_seg = ov.get("by_segment", {}) or {}
    if by_seg:
        seg_rows = {}
        for seg, d in unmatched_last(dict(sorted(by_seg.items()))).items():
            seg_rows[seg] = {
                "正例數": d.get("n_positives"),
                "候選列數": d.get("n_rows"),
                "正樣本率(÷此segment候選列)": d.get("positive_rate"),
                "query 數": d.get("n_queries"),
                "query 數佔比": d.get("query_share"),
            }
        tables.append(pd.DataFrame(seg_rows).T)
        titles.append("per-segment 正例組成")
        collapsed.append(False)
    cat = facts.get("category")
    if cat:
        cat_by_item = read_dataset_overview(cat).get("by_item", {})
        if cat_by_item:
            tables.append(pd.DataFrame(cat_by_item).T)
            titles.append("by 大類（大類粒度，不與整體相加）")
            collapsed.append(False)   # 大類表預設展開（與 totals/by_item/by_segment 一致）

    return ReportSection(
        title="基本統計 — 資料集",
        description=(
            "整體規模與 per-item 的正例組成。per-item 三欄（正例數／正樣本率／"
            "正例佔比）在密集候選集下同序（僅換分母或讀法），非三個獨立軸；候選"
            "覆蓋率因每 item 覆蓋全部 query 恆為 100%，故不列。注意「正樣本率」有"
            "兩種分母：概覽的母體 positive_rate 除以全體候選列（此 run 5,232），"
            "此處 per-item 那欄除以「該 item 自己的候選列數」（此 run 每 item 654），"
            "量級不同。「by 大類」是大類粒度（每客戶每大類一列、label＝該大類任一"
            "子產品為正例、大類分數＝子產品最佳分數），故其正例數 ≤ item 粒度合計、"
            "不與整體 n_positives 相加。per-segment 正例組成：正例數、候選列數、"
            "正樣本率、query 數與 query 數佔比（該 segment 佔多少 query）。每-query "
            "正例數分佈為後續階段。"
            + (
                " " + note + "上面各項總數與比率（列數、item 數、正樣本率）都"
                "算進了這些組，而且**沒有加權**：它們描述的是 dataset 留下來的這張"
                "表，會隨 r 改變，不代表全部曝光（要估全部曝光的正例率，看預測品質"
                "段的加權數字）。與 r ＝ 0 時的報表相比數字變了是預期的，不是 "
                "regression；固定整數 K 的排序指標則逐值不變。"
                if (note := kept_zero_positive_groups_note(facts, parameters))
                else ""
            )
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
        bullets=_segment_notes(facts.get("segments"), by_seg),
    )


def _segment_notes(segments: dict | None, by_segment: dict) -> list[str]:
    """讀分群表前要知道的事：欄取自哪張表、哪張表缺欄、(unmatched) 怎麼算。

    ``segments`` 是 ``compute_row_facts`` 從 ``evaluation_segment_columns`` 帶來的
    ``joined``／``sources``／``missing``（ADR-0020 bug 6）。缺欄時分群表根本不
    出現，這裡是唯一交代「為什麼沒有」的地方，所以表名與欄名都要印——設定
    打錯欄名也會落到這裡。沒有 ``segments`` 的列的事實不寫。
    """
    if not segments:
        return []
    notes = []
    joined = segments.get("joined") or []
    sources = segments.get("sources") or {}
    if joined:
        notes.append(
            "分群欄："
            + "、".join(f"{c} 取自 {sources[c]}" for c in joined)
            + f"；per-segment 表以 {joined[0]} 分群。"
        )
    for col, table in (segments.get("missing") or {}).items():
        notes.append(f"母體表 {table} 無欄 {col}：這一欄本次不算 per-segment。")
    if UNMATCHED_SEGMENT in by_segment:
        notes.append(
            f"{UNMATCHED_SEGMENT}＝母體表有這一欄、但這些 query 在上面沒有值。"
            "表上列出它的 query 數與佔比，但它不進 macro 平均（per-segment "
            "指標表的 Macro 列不含它）。"
        )
    return notes
