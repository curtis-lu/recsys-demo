"""baseline (熱門度基準 in ADR-0031 decision 16): the model against the
popularity baseline scored on its own rows."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.metric_catalog import ITEM, QUERY_GROUP, entry
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    BASELINE_COMPARISON,
    DISPLAY_KS,
    compared_macro_coverage_suffix,
    compared_per_item_table,
    compared_value,
    count_items,
    has_entries,
    landed_value,
    section_on,
    unmatched_last,
)
from recsys_tfb.evaluation.result_readers import (
    CELLS,
    DIFF,
    DISPLAY_METRICS_SECTION,
    DISPLAY_PRIMARY_MAP_K,
    MACRO_ITEMS_KEY,
    POPULARITY_WINDOW_KEY,
    POPULATION_KEY,
    REFERENCE,
    THIS,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the window facts off the baseline's own result, the model's value, the
#: baseline's and their difference off the comparison (#536), and each
#: table's K off the cut.
READS = ("evaluation_row_facts", "baseline_metrics", BASELINE_COMPARISON,
         DISPLAY_KS)

#: The rows of each M/B/Δ table and the comparison side each one reads.
_SIDES = (("Model", THIS), ("Baseline", REFERENCE), ("Δ", DIFF))

#: What this section compares, by catalog entry (ADR-0031 decision 11: each
#: section declares its own list): the query-group metrics overall, the
#: per-item ones with a Macro row, and the one metric of the per-segment
#: and category tables. ``entry`` resolves them when the section is built,
#: so a name the catalog lacks raises there.
_COMPARED_OVERALL = (("map", QUERY_GROUP), ("recall", QUERY_GROUP),
                     ("precision", QUERY_GROUP))
_COMPARED_PER_ITEM = (("recall", ITEM), ("map_attr", ITEM))
_COMPARED_PER_SEGMENT_AND_CATEGORY = ("map", QUERY_GROUP)


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    baseline_metrics = results["baseline_metrics"]
    # compute_baseline_metrics writes {"enabled": False, "config_fingerprint":
    # ...} when the section is off, so the stub can carry its fingerprint;
    # None stays accepted for callers that pass no baseline at all.
    if (not section_on(parameters, "baseline") or baseline_metrics is None
            or baseline_metrics.get("enabled") is False):
        return None

    # Every number below is read: the model's value, the baseline's and their
    # difference off the comparison (compare_to_baseline, #536), every K
    # list off the cut (evaluation_display_ks). A value either lacks raises,
    # naming the node to re-run (landed_value).
    def compared(*path):
        return landed_value(results[BASELINE_COMPARISON], BASELINE_COMPARISON,
                            *path)

    def ks(*path):
        return landed_value(results[DISPLAY_KS], DISPLAY_KS,
                            BASELINE_COMPARISON, *path)

    n_items = count_items(results["evaluation_row_facts"])
    # The per-item tables' K: primary_map_k, cut at the model's "all" — the
    # baseline's is the same K, it is scored on the model's own rows.
    attr_ks = ks(DISPLAY_PRIMARY_MAP_K)
    # overall 三表用 k superset（使用者指定，k 放欄位）
    k_super = ks(DISPLAY_METRICS_SECTION)
    # The window as compute_baseline_metrics counted it (#536,
    # baselines.popularity_window): the lookback it used — read off the
    # result, not off parameters (bug 1, ADR-0020: this line once read the
    # key with no default and printed nothing while the node had used 12) —
    # and the coverage below.
    window = baseline_metrics[POPULARITY_WINDOW_KEY]
    lookback = window["lookback_months"]

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # bug 1 (ADR-0020) partial window and #374 several dates: whether the
    # window is partial, how many months it covered, and what the per-month
    # average divides by are the node's (popularity_window, #536), read here.
    # #397: rate 模式（node 寫了非空 popularity_rate）的月度表是 sample_pool 的
    # monthly_positives，count 模式是 label_table 的 monthly_counts；涵蓋月數
    # 由 node 照同一張表數好落地。
    rate_info = (baseline_metrics or {}).get("popularity_rate") or None
    rate_mode = bool(rate_info)

    monthly = (
        rate_info.get("monthly_positives") if rate_mode
        else (baseline_metrics or {}).get("monthly_counts")
    ) or {}
    # The trend table's month columns, in order.
    months = sorted({mo for per in monthly.values() for mo in per})
    covered = window["months_covered"]
    window_partial = window["window_partial"]
    per_month_divisor = window["per_month_divisor"]
    full_window_months = window["full_window_months"]
    windows = window["window_months_covered"]
    several_dates = len(windows) > 1

    # [1] popularity 排名組成：rate 模式印正例率／分母／分子（purchase_counts／
    #     monthly_counts 在 rate 模式仍由 label_table 算，這裡不拿來印）；
    #     count 模式維持原本的總計 count + 平均每月。
    if rate_mode:
        rates = rate_info.get("rate") or {}
        candidates = rate_info.get("candidates") or {}
        positives = rate_info.get("positives") or {}
        sorted_items = sorted(
            rates.items(), key=lambda kv: (-kv[1], kv[0])
        )
        # 4 位有效數字，不是 4 位小數：點擊率量級的比率（3e-5）四捨五入到
        # 小數第 4 位會全變成 0，看不出名次為什麼這樣排。
        pop_cols = {
            "正例率": [float(f"{v:.4g}") for _, v in sorted_items],
            "當候選次數": [candidates.get(k, 0) for k, _ in sorted_items],
            "正例數": [positives.get(k, 0) for k, _ in sorted_items],
            "rank": list(range(1, len(sorted_items) + 1)),
        }
        if sorted_items:
            _add(
                pd.DataFrame(pop_cols, index=[k for k, _ in sorted_items]),
                "popularity 排名組成", False,
            )

        # [1b] 月度趨勢：monthly_positives，列序與 [1] 相同（照正例率排）。
        if monthly:
            item_order = [k for k, _ in sorted_items]
            mdf = pd.DataFrame(
                {
                    mo: [monthly.get(it, {}).get(mo, 0) for it in item_order]
                    for mo in months
                },
                index=item_order,
            )
            mdf["合計"] = mdf.sum(axis=1)
            _add(mdf, "popularity 月度趨勢", True)
    else:
        pcounts = (baseline_metrics or {}).get("purchase_counts") or {}
        if pcounts:
            sorted_items = sorted(
                pcounts.items(), key=lambda kv: kv[1], reverse=True
            )
            pop_cols = {"count": [v for _, v in sorted_items]}
            if per_month_divisor:
                pop_cols["平均每月"] = [
                    round(v / per_month_divisor, 1) for _, v in sorted_items
                ]
            pop_cols["rank"] = list(range(1, len(sorted_items) + 1))
            _add(
                pd.DataFrame(pop_cols, index=[k for k, _ in sorted_items]),
                "popularity 排名組成", False,
            )

        # [1b] 月度趨勢：rows=item（總計降序，與 [1] 同序）、cols=月份升序＋合計。
        #      各 item 的「合計」＝該列月份和，逐 item 對齊 [1] 的 count。
        if monthly:
            item_order = sorted(
                monthly, key=lambda it: sum(monthly[it].values()), reverse=True
            )
            mdf = pd.DataFrame(
                {
                    mo: [monthly[it].get(mo, 0) for it in item_order]
                    for mo in months
                },
                index=item_order,
            )
            mdf["合計"] = mdf.sum(axis=1)
            _add(mdf, "popularity 月度趨勢", True)

    # [2] overall：mAP / recall / precision 各一張，rows=[Model,Baseline,Δ]、
    #     cols=@k（superset），明細收合。"all" 讀比較層配好的 name@all 格。
    overall = compared("overall", CELLS)
    for metric in (entry(*c) for c in _COMPARED_OVERALL):
        data = {
            who: {f"@{k}": compared_value(overall, metric.key(k), side)
                  for k in k_super}
            for who, side in _SIDES
        }
        _add(pd.DataFrame(data).T,
             f"overall {metric.display_name}@k (M/B/Δ)", True)

    # [3] per-item compare tables — only when baseline has per_item；明細收合。
    per_item = compared("per_item")
    macro = compared("macro_by_item")
    if has_entries(per_item, REFERENCE):
        # bug 5 (ADR-0020): either side's macro denominator can shrink
        # silently, so Model and Baseline are disclosed separately.
        item_cov = compared_macro_coverage_suffix(
            macro, compared(POPULATION_KEY, MACRO_ITEMS_KEY), n_items)
        # 兩張 per-item M/B/Δ 用同一組 k（attr_ks＝primary_map_k），彼此一致；
        # 為控寬用縮減集，與衡量指標 per-item 的完整 [1..5,all] 不同（描述封邊）。
        for metric in (entry(*c) for c in _COMPARED_PER_ITEM):
            label = metric.display_name
            _add(
                compared_per_item_table(
                    per_item, macro, attr_ks, metric.name, f"{label}@{{k}}"),
                # No "per-item" in front: the display name says it for
                # the one metric whose name a query-group one shares.
                f"{label}@k (M/B/Δ){item_cov}", True,
            )

    # [4] per-segment mAP@k M/B/Δ — 只比主指標 mAP（控寬，reference 段）；rows＝
    #     每個 segment 的 Model/Baseline/Δ 三列、cols＝@k。需 model 與 baseline
    #     兩側都有 per_segment（key 由同一 active_seg_col 對齊）。明細收合。
    one = entry(*_COMPARED_PER_SEGMENT_AND_CATEGORY)
    per_segment = compared("per_segment")
    if has_entries(per_segment, THIS) and has_entries(per_segment, REFERENCE):
        by_segment = per_segment[CELLS]
        rows: dict[str, dict] = {}
        for seg in unmatched_last({s: None for s in sorted(by_segment)}):
            for who, side in _SIDES:
                rows[f"{seg} · {who}"] = {
                    f"@{k}": compared_value(by_segment[seg], one.key(k), side)
                    for k in k_super
                }
        _add(pd.DataFrame(rows).T,
             f"per-segment {one.display_name}@k (M/B/Δ)", True)

    # [5] 大類 overall mAP@k M/B/Δ — 大類粒度的 overall mAP 對照；rows＝
    #     [Model,Baseline,Δ]、cols＝@k，K 是大類粒度切好的那份。比較層只在兩側
    #     都有大類時比（baseline 與 model 是同一批列，兩側同一 category 集）。
    if compared("category") is not None:
        cat_overall = compared("category", "overall")
        if (has_entries(cat_overall, THIS)
                and has_entries(cat_overall, REFERENCE)):
            cks = ks("category", DISPLAY_METRICS_SECTION)
            data = {
                who: {f"@{k}": compared_value(cat_overall[CELLS], one.key(k),
                                              side)
                      for k in cks}
                for who, side in _SIDES
            }
            _add(pd.DataFrame(data).T,
                 f"大類 overall {one.display_name}@k (M/B/Δ)", True)

    # Always printed (bug 1): the node lands the lookback it used, never
    # unset. A partially covered window also states how many
    # months it actually had (see window_partial above).
    trend_note = ""
    composition_note = (
        "popularity 排名組成為各 item 跨月合計（總計＋平均每月）；月度趨勢表把"
        "同一批計數拆到各 item 逐月（列＝item、欄＝月份，合計逐 item 對齊排名"
        "組成）。"
    )
    if rate_mode:
        # #397: rate 模式以正例率（正例數 ÷ 當過候選的次數）重排，分母從
        # sample_pool 數；文字裡一律不提「購買計數」「平均每月」。
        lookback_note = (
            f"popularity 以過去 {lookback} 個月內的正例率"
            f"（正例數 ÷ 當過候選的次數）重排"
            f"（sample_pool 在這個視窗內實際只涵蓋 {covered} 個月）。"
            if window_partial else
            f"popularity 以過去 {lookback} 個月內的正例率"
            "（正例數 ÷ 當過候選的次數）重排。"
        )
        if several_dates:
            # _positive_rate_block counts each period once however many
            # windows cover it, so there is no double counting to disclose.
            n_windows = len(windows)
            lookback_note = (
                f"popularity 對 {n_windows} 個評估日期各以該日期之前 {lookback} "
                "個月的正例率（正例數 ÷ 當過候選的次數）重排。排名組成印的是這 "
                f"{n_windows} 個視窗涵蓋的期合在一起（每期只算一次）的正例率，"
                "不是任何一個日期實際用的那一個"
                + (
                    f"；各視窗內 sample_pool 有候選列的月數加總實際只有 "
                    f"{per_month_divisor} 個視窗月，滿額 {full_window_months} 個。"
                    if window_partial else "。"
                )
            )
        composition_note = (
            "popularity 排名組成為各 item 的正例率、分母（當過候選的次數）與"
            "分子（正例數）；月度趨勢表把同一批正例數拆到各 item 逐月（列＝"
            "item、欄＝月份，合計逐 item 對齊排名組成的正例數）。"
        )
    else:
        lookback_note = (
            f"popularity 以過去 {lookback} 個月的歷史購買計數重排"
            f"（label_table 在這個視窗內實際只涵蓋 {covered} 個月）。"
            if window_partial else
            f"popularity 以過去 {lookback} 個月的歷史購買計數重排。"
        )
        if several_dates:
            n_windows = len(windows)
            lookback_note = (
                f"popularity 對 {n_windows} 個評估日期各以該日期之前 {lookback} 個月"
                f"的歷史購買計數重排；排名組成的 count 是這 {n_windows} 個視窗的"
                f"合計，平均每月＝count ÷ {per_month_divisor}"
                + (
                    f"（各視窗內 label_table 有資料的月數加總：實際只涵蓋 "
                    f"{per_month_divisor} 個視窗月，滿額 {full_window_months} 個）。"
                    if window_partial else
                    f"（{n_windows} 個視窗 × 每個 {lookback} 個月）。"
                )
            )
            trend_note = (
                "視窗彼此重疊時，同一個月會被每個涵蓋它的視窗各算一次，"
                "所以月度趨勢表的逐月數字是重複計數後的合計。"
            )
        if window["score"] == "rate":
            # score: rate 但 node 沒寫 popularity_rate：監控模式（#397）。
            lookback_note += (
                "設定是 evaluation.baseline.score: rate，但監控模式評的是離線推論"
                "的全網格（每個 entity × 全部 item，沒展示過的算負例），一列會不會"
                "是正例同時取決於被展示幾次，所以這裡照舊用正例數；正例率只在 "
                "--post-training 用。"
            )
    return ReportSection(
        title="baseline — popularity 對照",
        description=(
            f"Model 相對 popularity baseline 的位置。{lookback_note}"
            f"{composition_note}"
            f"{trend_note}"
            "overall 的 mAP／recall／precision 各一張表、"
            "k 放欄位、點標題展開。對照層級：overall（三家族）、per-item"
            "（recall／map_attr 兩張，k＝[1,3,5,all] 控寬）、per-segment 與大類"
            "overall（只比主指標 mAP，各一張，reference 段控寬）。per-segment／大類"
            "僅在 model 與 baseline 兩側都有該切片時出現（key 由同一 active segment／"
            "item_categories 對齊）。"
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )
