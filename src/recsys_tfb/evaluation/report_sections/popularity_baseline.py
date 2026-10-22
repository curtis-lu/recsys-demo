"""baseline (熱門度基準 in ADR-0031 decision 16): the model against the
popularity baseline scored on its own rows."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.baselines import baseline_score, resolve_lookback_months
from recsys_tfb.evaluation.metric_catalog import ITEM, QUERY_GROUP, entry
from recsys_tfb.evaluation.metrics import resolved_all_k
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    count_items,
    k_to_lookup,
    macro_coverage_suffix_mb,
    none_safe_diff,
    per_item_metric_compare_table,
    report_cfg,
    resolve_display_k,
    section_on,
    unmatched_last,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_metrics", "baseline_metrics")

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
    metrics = results["evaluation_metrics"]
    baseline_metrics = results["baseline_metrics"]
    # compute_baseline_metrics writes {"enabled": False, "config_fingerprint":
    # ...} when the section is off, so the stub can carry its fingerprint;
    # None stays accepted for callers that pass no baseline at all.
    if (not section_on(parameters, "baseline") or baseline_metrics is None
            or baseline_metrics.get("enabled") is False):
        return None
    from recsys_tfb.evaluation.compare import build_comparison_result

    comp = build_comparison_result(
        metrics, baseline_metrics, "Model", "Baseline"
    )
    disp = report_cfg(parameters).get("display", {}) or {}
    n_items = count_items(metrics)
    # One K for both sides, the model's: the baseline is scored on the model's
    # own rows (build_baseline_frame), so its "all" resolved to the same K,
    # and its slim bundle records neither that nor an item count (#434).
    all_k = resolved_all_k(metrics)
    attr_ks = resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), all_k
    )
    # overall 三表用 k superset（使用者指定，k 放欄位）
    k_super = resolve_display_k([1, 2, 3, 4, 5, "all"], all_k)
    # Same helper the node (compute_baseline_metrics) reads (bug 1,
    # ADR-0020): before it existed this line read .get("lookback_months")
    # with no default, so an unset key printed nothing here while the node
    # had in fact used 12.
    lookback = resolve_lookback_months(parameters)

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # bug 1 (ADR-0020), partial window: _lookback_window raises only when the
    # window has no label rows at all, so a label_table covering 2 of 12
    # lookback months passes. It used to print the plain 12-month sentence
    # and divide the per-month average by 12, understating it 6x with no
    # error. monthly_counts holds only months with label rows inside the
    # window, so its distinct months are the coverage; without it (older
    # results) the configured lookback stays both text and divisor.
    # #397: rate 模式（node 寫了非空 popularity_rate）的涵蓋月數看 sample_pool
    # 的候選列：monthly_positives 在每個有候選列的月份都有值（正例數可為 0），
    # window_months_covered 是各視窗有候選列的月數。count 模式（沒有這個鍵）
    # 照舊看 label_table 的 monthly_counts。下面的涵蓋算法兩種模式共用。
    rate_info = (baseline_metrics or {}).get("popularity_rate") or None
    rate_mode = bool(rate_info)
    source = rate_info if rate_mode else (baseline_metrics or {})

    monthly = (
        rate_info.get("monthly_positives") if rate_mode
        else (baseline_metrics or {}).get("monthly_counts")
    ) or {}
    months = sorted({mo for per in monthly.values() for mo in per})
    covered = len(months)
    window_partial = 0 < covered < lookback
    per_month_divisor = covered if window_partial else lookback
    # Several evaluated dates (#374): one window per date, and purchase_counts
    # sums them all, so the per-month average divides by the window-months
    # behind that sum — each window's covered months under the same partial
    # rule as above — not by one window's lookback (N times too high). The
    # node writes window_months_covered only then; the distinct months of
    # monthly_counts cannot stand in for it, since overlapping windows share
    # months and a boundary month can hold rows for one window and not the other.
    windows = source.get("window_months_covered") or {}
    several_dates = len(windows) > 1
    if several_dates:
        full_window_months = len(windows) * lookback
        per_month_divisor = sum(
            c if 0 < c < lookback else lookback for c in windows.values()
        )
        window_partial = per_month_divisor < full_window_months

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
    #     cols=@k（superset），明細收合。
    overall_a = comp["result_a"].get("overall", {}) or {}
    overall_b = comp["result_b"].get("overall", {}) or {}
    overall_delta = comp["overall_delta"]
    for metric in (entry(*c) for c in _COMPARED_OVERALL):
        data = {}
        for who, src in (("Model", overall_a), ("Baseline", overall_b),
                         ("Δ", overall_delta)):
            data[who] = {
                f"@{k}": src.get(metric.key(k_to_lookup(k, all_k)))
                for k in k_super
            }
        _add(pd.DataFrame(data).T,
             f"overall {metric.display_name}@k (M/B/Δ)", True)

    # [3] per-item compare tables — only when baseline has per_item；明細收合。
    per_item_a = comp["result_a"].get("per_item", {}) or {}
    per_item_b = comp["result_b"].get("per_item", {}) or {}
    per_item_delta = comp.get("per_item_delta", {}) or {}
    macro_a = (metrics.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (baseline_metrics.get("macro_avg", {}) or {}).get("by_item")
    if per_item_b:
        # bug 5 (ADR-0020): either side's macro denominator can shrink
        # silently, so Model and Baseline are disclosed separately.
        item_cov = macro_coverage_suffix_mb(
            per_item_a, per_item_b, n_items, parameters, macro_a, macro_b
        )
        # 兩張 per-item M/B/Δ 用同一組 k（attr_ks＝primary_map_k），彼此一致；
        # 為控寬用縮減集，與衡量指標 per-item 的完整 [1..5,all] 不同（描述封邊）。
        for metric in (entry(*c) for c in _COMPARED_PER_ITEM):
            label = metric.display_name
            _add(
                per_item_metric_compare_table(
                    per_item_a, per_item_b, per_item_delta,
                    attr_ks, all_k, metric.name, f"{label}@{{k}}",
                    macro_a=macro_a, macro_b=macro_b, all_k_b=all_k,
                ),
                # No "per-item" in front: the display name says it for
                # the one metric whose name a query-group one shares.
                f"{label}@k (M/B/Δ){item_cov}", True,
            )

    # [4] per-segment mAP@k M/B/Δ — 只比主指標 mAP（控寬，reference 段）；rows＝
    #     每個 segment 的 Model/Baseline/Δ 三列、cols＝@k。需 model 與 baseline
    #     兩側都有 per_segment（key 由同一 active_seg_col 對齊）。明細收合。
    one = entry(*_COMPARED_PER_SEGMENT_AND_CATEGORY)
    seg_a = metrics.get("per_segment", {}) or {}
    seg_b = (baseline_metrics or {}).get("per_segment", {}) or {}
    if seg_a and seg_b:
        rows: dict[str, dict] = {}
        for seg in unmatched_last(
            {s: None for s in sorted(set(seg_a) | set(seg_b))}
        ):
            a, b = seg_a.get(seg, {}) or {}, seg_b.get(seg, {}) or {}
            for who, src in ((f"{seg} · Model", a), (f"{seg} · Baseline", b)):
                rows[who] = {
                    f"@{k}": src.get(one.key(k_to_lookup(k, all_k)))
                    for k in k_super
                }
            rows[f"{seg} · Δ"] = {
                f"@{k}": none_safe_diff(a.get(one.key(k_to_lookup(k, all_k))),
                                        b.get(one.key(k_to_lookup(k, all_k))))
                for k in k_super
            }
        _add(pd.DataFrame(rows).T,
             f"per-segment {one.display_name}@k (M/B/Δ)", True)

    # [5] 大類 overall mAP@k M/B/Δ — 大類粒度的 overall mAP 對照；rows＝
    #     [Model,Baseline,Δ]、cols＝@k。cat_k 取 model 的 category bundle 的 K
    #     （baseline slim bundle 無 dataset_overview，兩側同一 category 集）。
    cat_a = (metrics.get("category") or {}).get("overall", {}) or {}
    cat_b = ((baseline_metrics or {}).get("category") or {}).get("overall", {}) or {}
    if cat_a and cat_b:
        cat_k = resolved_all_k(metrics.get("category") or {}) or all_k
        cks = resolve_display_k([1, 2, 3, 4, 5, "all"], cat_k)
        data = {}
        for who, src in (("Model", cat_a), ("Baseline", cat_b)):
            data[who] = {
                f"@{k}": src.get(one.key(k_to_lookup(k, cat_k))) for k in cks
            }
        data["Δ"] = {
            f"@{k}": none_safe_diff(cat_a.get(one.key(k_to_lookup(k, cat_k))),
                                    cat_b.get(one.key(k_to_lookup(k, cat_k))))
            for k in cks
        }
        _add(pd.DataFrame(data).T,
             f"大類 overall {one.display_name}@k (M/B/Δ)", True)

    # Always printed (bug 1): lookback is resolved via the shared helper above
    # and is never unset. A partially covered window also states how many
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
        if baseline_score(parameters) == "rate":
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
