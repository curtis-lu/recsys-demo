"""熱門度基準 (ADR-0031 decision 16): the model against the popularity
baseline scored on its own rows — the popularity it ranked by, whether the
two can be compared as they are, and one table per metric of this section's
list (:data:`_COMPARED`), each K three columns: 這次, 對照, 差.

Every number is read: the window and the popularity off the baseline's own
result, the model's value, the baseline's and their difference off the
comparison (``compare_to_baseline``, #536), the K of every table off the cut
(``resolve_display_ks``: one list, ``compare_k``, decision 16 item 7). The
one display division is 平均每月 (a landed count over the landed divisor).
"""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.evaluation.metric_catalog import ITEM, QUERY_GROUP, CompareEntry
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.metric_tables import (
    Comparability,
    SliceRow,
    comparability,
    comparability_table,
    definitions,
    metric_table,
)
from recsys_tfb.evaluation.report_sections.shared import (
    BASELINE_COMPARISON,
    DISPLAY_KS,
    MACRO_LABEL,
    count_items,
    landed_value,
    section_on,
    unmatched_last,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    ALL_LABEL,
    CELLS,
    DISPLAY_ALL_K,
    DISPLAY_COMPARE_K,
    LEDGER_RERUN_NODE,
    MACRO_ITEMS_KEY,
    POPULARITY_WINDOW_KEY,
    POPULATION_KEY,
    REFERENCE,
    SCORED_QUERIES_KEY,
    THIS,
)
from recsys_tfb.report.types import (
    Column,
    Heading,
    Note,
    Paragraph,
    Row,
    Table,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the window facts off the baseline's own result, the model's value, the
#: baseline's and their difference off the comparison (#536), and each
#: table's K off the cut; the row facts for the item count.
READS = ("evaluation_row_facts", "baseline_metrics", BASELINE_COMPARISON,
         DISPLAY_KS)

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "熱門度基準"
TITLE_EN = "Popularity baseline"
QUESTION = "比熱門度排序好多少？"

#: What this section compares, one table each, in this order (ADR-0031
#: decision 11, as decision 16 item 7 and the demo left it): ``map`` overall
#: and per segment, ``precision`` and ``recall`` overall, ``map_attr`` and
#: the per-item ``recall`` per item, the category grain's ``map`` overall.
#: The grain names a table's rows: ``per_segment`` leads with the 整體 row
#: and ``per_item`` with the Macro 平均 row, both pinned (decision 16 item
#: 6); ``category`` is the category grain's 整體, drawn when the comparison
#: has that grain. Every cell a table of the list draws must be in the
#: comparison: a missing one raises (``_require_cell``), it is never left
#: blank (decision 10, correction).
_COMPARED = (
    CompareEntry("map", QUERY_GROUP, "per_segment"),
    CompareEntry("precision", QUERY_GROUP, "overall"),
    CompareEntry("recall", QUERY_GROUP, "overall"),
    CompareEntry("map_attr", ITEM, "per_item"),
    CompareEntry("recall", ITEM, "per_item"),
    CompareEntry("map", QUERY_GROUP, "category"),
)

#: The grain whose tables are drawn off the comparison's category grain.
_CATEGORY = "category"

#: Both sides of a compared cell.
_BOTH = (THIS, REFERENCE)


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    baseline_metrics = results["baseline_metrics"]
    # compute_baseline_metrics writes {"enabled": False, "config_fingerprint":
    # ...} when the section is off, so the stub can carry its fingerprint;
    # None stays accepted for callers that pass no baseline at all.
    if (not section_on(parameters, "popularity_baseline") or baseline_metrics is None
            or baseline_metrics.get("enabled") is False):
        return None

    comparison = results[BASELINE_COMPARISON]
    facts = results["evaluation_row_facts"]
    window = baseline_metrics[POPULARITY_WINDOW_KEY]
    # #397: rate mode (the node wrote a non-empty popularity_rate) ranks by
    # sample_pool's positive rate; count mode by label_table's counts.
    rate = baseline_metrics.get("popularity_rate") or None

    blocks: list = [Paragraph(_description(window, rate), muted=True)]
    blocks += (_rate_tables(rate) if rate
               else _count_tables(baseline_metrics, window))
    population = landed_value(comparison, BASELINE_COMPARISON, POPULATION_KEY)
    blocks.append(comparability_table(population))

    cut = results[DISPLAY_KS]
    item_entries = [e for e in _COMPARED if e.grain != _CATEGORY]
    category_entries = [e for e in _COMPARED if e.grain == _CATEGORY]
    tables, drew_macro = _grain_blocks(item_entries, comparison, cut, (),
                                       count_items(facts))
    blocks += [Heading("item 粒度"), *tables]
    # The comparison has the category grain only when both sides have it
    # (comparison_layer): the baseline re-scores the model's rows with the
    # model's mapping, so that is whenever the model has one.
    if category_entries and landed_value(
            comparison, BASELINE_COMPARISON, _CATEGORY) is not None:
        tables, category_macro = _grain_blocks(
            category_entries, comparison, cut, (_CATEGORY,),
            count_items(facts.get(_CATEGORY) or {}))
        blocks += [Heading("大類粒度"), *tables]
        drew_macro = drew_macro or category_macro
    blocks.append(definitions([*_COMPARED, *(["macro"] if drew_macro else [])]))
    return ReportSection(title=TITLE, description="", title_en=TITLE_EN,
                         question=QUESTION, key="popularity_baseline",
                         blocks=blocks)


# ---------------------------------------------------------------------------
# The compared tables
# ---------------------------------------------------------------------------


def _grain_blocks(entries, comparison: dict, cut: dict, prefix: tuple,
                  n_items: int) -> tuple[list, bool]:
    """One grain's tables, in list order, then the lines under them: the ⚠
    of each annotation the two sides differ on (the comparison demo's four
    cases), and how many items the Macro averaged when that is fewer than all
    (decision 16 item 5). Returns the blocks and whether a Macro row was
    drawn. ``prefix`` is the grain's path in the comparison and in the cut's
    block (``()``, or ``("category",)``)."""

    def compared(*path):
        return landed_value(comparison, BASELINE_COMPARISON, *prefix, *path)

    def landed_k(key):
        return landed_value(cut, DISPLAY_KS, BASELINE_COMPARISON, *prefix, key)

    ks, all_k = landed_k(DISPLAY_COMPARE_K), landed_k(DISPLAY_ALL_K)
    flags = comparability(compared(POPULATION_KEY))
    blocks: list = []
    drew_macro = False
    for entry in entries:
        row_header, slices, rows = _slice_rows(entry, compared, flags)
        for row, path, sides in rows:
            for k in ks:
                _require_cell(entry, row, path, sides, k, all_k, prefix,
                              slices)
        drew_macro |= any(row.label == MACRO_LABEL for row, _, _ in rows)
        blocks.append(metric_table(
            entry, ks, [row for row, _, _ in rows], row_header=row_header,
            slices=slices, compare=True, all_k=all_k,
            warn_ks=[all_k] if flags.all_k_differs else []))
    notes = [flags.note(ALL_K_KEY),
             flags.note(MACRO_ITEMS_KEY) if drew_macro else None,
             flags.note(SCORED_QUERIES_KEY)]
    n_macro = len(compared(POPULATION_KEY, MACRO_ITEMS_KEY, THIS))
    if (drew_macro and not flags.macro_items_differs
            and 0 < n_macro < n_items):
        notes.append(Note(f"參與 macro 的 item 數：{n_macro}"))
    return blocks + [n for n in notes if n], drew_macro


def _slice_rows(entry: CompareEntry, compared,
                flags: Comparability) -> tuple[str, str, list]:
    """``(row_header, slices, rows)`` of ``entry``'s table; each row is
    ``(SliceRow, its cells' path in the grain, the sides it must hold)``.

    The sides a row must hold: both, but for the Macro row only the sides
    whose macro averaged any item (an empty ``macro_items`` lands no macro
    value; with none on either side there is no Macro row)."""
    overall = (SliceRow("整體", compared("overall", CELLS), pinned=True),
               ("overall", CELLS), _BOTH)
    if entry.grain in ("overall", _CATEGORY):
        return "分群", "整體", [overall]
    if entry.grain == "per_segment":
        # Decision — the two sides segment by different columns: no segment
        # row, only 整體; the 能不能比 table says why (ADR-0031 decision 16
        # item 15, the comparison report's 分群指標 section alike). A table
        # with one side blank cannot answer "which ranks better".
        if flags.segment_columns_differs:
            return "分群", "整體", [overall]
        cells = compared("per_segment", CELLS)
        return "分群", "整體與各分群", [overall] + [
            (SliceRow(seg, cells[seg]), ("per_segment", CELLS, seg), _BOTH)
            for seg in unmatched_last({s: None for s in sorted(cells)})]
    if entry.grain == "per_item":
        macro_items = compared(POPULATION_KEY, MACRO_ITEMS_KEY)
        macro_sides = tuple(side for side in _BOTH if macro_items[side])
        rows = []
        if macro_sides:
            rows.append((SliceRow(MACRO_LABEL, compared("macro_by_item", CELLS),
                                  pinned=True, warn=flags.macro_items_differs),
                         ("macro_by_item", CELLS), macro_sides))
        rows += [(SliceRow(str(item), cells), ("per_item", CELLS, item), _BOTH)
                 for item, cells in compared("per_item", CELLS).items()]
        return "item", "各 item", rows
    raise ValueError(
        f"popularity_baseline._COMPARED: no table for grain {entry.grain!r} "
        f"({entry.name!r}, {entry.method!r}); the section draws overall, "
        f"per_segment, per_item and category")


def _require_cell(entry: CompareEntry, row: SliceRow, path: tuple,
                  sides: tuple, k: int, all_k: int, prefix: tuple,
                  slices: str) -> None:
    """Raise when a side the row must hold has no value at ``k``.

    The list names every cell its tables draw, so a blank one is not "not
    computed" but a result that lacks what the list asks for (ADR-0031
    decision 10, correction: not left blank once the list exists). The
    values come from the metrics the comparison joined, so the advice is to
    re-compute them, not to re-run the comparison alone, which would join
    the same metrics again."""
    key = entry.metric.key(ALL_LABEL if k == all_k else k)
    cell = (row.values or {}).get(key)
    for side in sides:
        if isinstance(cell, Mapping) and cell.get(side) is not None:
            continue
        where = ".".join(str(p) for p in (*prefix, *path, key, side))
        raise ValueError(
            f"{BASELINE_COMPARISON} has no {where}, which the popularity "
            f"baseline section draws ({TITLE}: {entry.metric.display_name}@K "
            f"· {slices}, row {row.label!r}, @{k}). Its list "
            f"(popularity_baseline._COMPARED) names that cell, so it is not "
            f"left blank: the metrics compared did not hold it — landed by an "
            f"older compute_metrics / compute_baseline_metrics, or for other "
            f"settings. Re-compute them — `python -m recsys_tfb evaluation "
            f"--from-node {LEDGER_RERUN_NODE}`, with the flags of the run that "
            f"wrote them (e.g. --post-training) — then re-draw."
        )


# ---------------------------------------------------------------------------
# The popularity it ranked by
# ---------------------------------------------------------------------------


def _ranked(values: Mapping) -> list:
    """The items by value, highest first (ties by name): the popularity's
    own order, which the 名次 column numbers."""
    return sorted(values, key=lambda item: (-values[item], str(item)))


def _monthly_table(title: str, monthly: Mapping, order: list,
                   totals: Mapping) -> Table:
    """Items in ``order`` (then any only the monthly table has), months
    ascending, then 合計: the landed total (``totals``), the same counts
    summed over the months by the node. A month an item has no entry for
    held none of its rows: 0."""
    months = sorted({mo for per in monthly.values() for mo in per})
    items = order + sorted((i for i in monthly if i not in order), key=str)
    rows = [Row(str(item), (*(monthly.get(item, {}).get(mo, 0) for mo in months),
                            totals.get(item)))
            for item in items]
    columns = (*(Column(str(mo), "count") for mo in months),
               Column("合計", "count"))
    return Table(title, "item", columns, tuple(rows))


def _count_tables(baseline_metrics: Mapping, window: Mapping) -> list:
    """Count mode: 購買次數 per item with 平均每月 (÷ the node's divisor,
    ``popularity_window.per_month_divisor``) and 名次; then per month."""
    counts = baseline_metrics.get("purchase_counts") or {}
    monthly = baseline_metrics.get("monthly_counts") or {}
    divisor = window["per_month_divisor"]
    order = _ranked(counts)
    out = []
    if counts:
        out.append(Table(
            "購買次數與名次 · 各 item", "item",
            (Column("購買次數", "count"), Column("平均每月", "ratio"),
             Column("名次", "count")),
            tuple(Row(str(item), (counts[item],
                                  counts[item] / divisor if divisor else None,
                                  rank))
                  for rank, item in enumerate(order, start=1))))
    if monthly:
        out.append(_monthly_table("每月購買次數 · 各 item", monthly, order,
                                  counts))
    return out


def _rate_tables(rate: Mapping) -> list:
    """Rate mode (#397): the positive rate per item with its denominator
    (the times it was a candidate) and numerator, and 名次; then the
    positives per month."""
    rates = rate.get("rate") or {}
    candidates = rate.get("candidates") or {}
    positives = rate.get("positives") or {}
    monthly = rate.get("monthly_positives") or {}
    order = _ranked(rates)
    out = []
    if rates:
        out.append(Table(
            "正例率與名次 · 各 item", "item",
            (Column("正例率", "ratio"), Column("當候選次數", "count"),
             Column("正例數", "count"), Column("名次", "count")),
            tuple(Row(str(item), (rates[item], candidates.get(item),
                                  positives.get(item), rank))
                  for rank, item in enumerate(order, start=1))))
    if monthly:
        out.append(_monthly_table("每月正例數 · 各 item", monthly, order,
                                  positives))
    return out


def _description(window: Mapping, rate: Mapping | None) -> str:
    """The section's opening paragraph, from the window the node landed
    (``baselines.popularity_window``): the lookback it used, whether it was
    covered in full, and with several evaluated dates (#374) what the counts
    sum over.

    The one judgement left here (ADR-0031 decision 10, correction): which
    sentence to print, by how many evaluated dates the window lists — the
    rule the node's divisor follows too, so a change to one changes both."""
    lookback = window["lookback_months"]
    covered = window["months_covered"]
    partial = window["window_partial"]
    divisor = window["per_month_divisor"]
    full = window["full_window_months"]
    n_windows = len(window["window_months_covered"])
    same_order = "同一期每個 entity 拿到一樣的順序"
    if rate:
        by = "的正例率（正例數 ÷ 當過候選的次數）"
        if n_windows > 1:
            # _positive_rate_block counts each period once however many
            # windows cover it, so there is no double counting to disclose.
            text = (
                f"熱門度對 {n_windows} 個評估日期各用該日期之前 {lookback} 個月"
                f"每個 item {by}排出名次，{same_order}。正例率與名次印的是這 "
                f"{n_windows} 個視窗涵蓋的期合在一起（每期只算一次）的正例率，"
                "不是任何一個日期實際用的那一個"
                + (f"；各視窗內 sample_pool 有候選列的月數加總實際只有 "
                   f"{divisor} 個視窗月，滿額 {full} 個。" if partial else "。"))
        else:
            text = (f"熱門度用過去 {lookback} 個月每個 item {by}排出名次，"
                    f"{same_order}"
                    + (f"；sample_pool 在這個視窗內實際只涵蓋 {covered} 個月。"
                       if partial else "。"))
    else:
        if n_windows > 1:
            text = (
                f"熱門度對 {n_windows} 個評估日期各用該日期之前 {lookback} 個月"
                f"每個 item 的購買次數排出名次，{same_order}。購買次數是這 "
                f"{n_windows} 個視窗的合計，平均每月＝購買次數 ÷ {divisor}"
                + (f"（各視窗內 label_table 有資料的月數加總：實際只涵蓋 "
                   f"{divisor} 個視窗月，滿額 {full} 個）。" if partial else
                   f"（{n_windows} 個視窗 × 每個 {lookback} 個月）。")
                + "視窗彼此重疊時，同一個月會被每個涵蓋它的視窗各算一次，"
                  "所以每月購買次數的逐月數字是重複計數後的合計。")
        else:
            text = (f"熱門度用過去 {lookback} 個月每個 item 的購買次數排出名次，"
                    f"{same_order}"
                    + (f"；label_table 在這個視窗內實際只涵蓋 {covered} 個月，"
                       f"平均每月除以 {covered}。" if partial else "。"))
        if window["score"] == "rate":
            # score: rate but no popularity_rate: monitoring (#397).
            text += (
                "設定是 evaluation.baseline.score: rate，但監控模式評的是離線推論"
                "的全網格（每個 entity × 全部 item，沒展示過的算負例），一列會不會"
                "是正例同時取決於被展示幾次，所以這裡照舊用購買次數；正例率只在 "
                "--post-training 用。")
    return f"模型跟熱門度排序比。{text}兩邊算在同一批列上。差＝這次 − 對照。"
