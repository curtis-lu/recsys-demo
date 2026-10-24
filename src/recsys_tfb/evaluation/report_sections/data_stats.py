"""資料統計: how the positives are spread over each time value, item,
segment and category — the slices' counts and their shares of the totals
(ADR-0031 decision 16 item 6). The totals themselves are 核心概念's."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    kept_ratio_phrase,
    read_dataset_overview,
    section_on,
    unmatched_last,
    zero_positive_draw_on_test,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT
from recsys_tfb.report.types import Column, Note, Paragraph, Row, Table

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_row_facts",)

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "資料統計"
TITLE_EN = "Data statistics"
QUESTION = "資料多大？正例怎麼分布？"

#: The columns every 正例組成 table has: decision 16 item 6's five, and the
#: slice's entity count (ADR-0031 decision 1, the first row of the acceptance
#: table: a column added in this section's file only).
_COMPOSITION_COLUMNS = (
    Column("候選列", "count"),
    Column("候選列佔比", "ratio"),
    Column("正例數", "count"),
    Column("正例佔比", "ratio"),
    Column("正例率", "ratio"),
    Column("entity 數", "count"),
)


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection | None:
    if not section_on(parameters, "data_stats"):
        return None
    facts = results["evaluation_row_facts"]
    ov = read_dataset_overview(facts)
    totals = ov.get("totals", {}) or {}
    by_segment = ov.get("by_segment", {}) or {}
    category = facts.get("category") or None
    category_ov = read_dataset_overview(category) if category else {}

    tables = [
        Table("比例", "項目",
              (Column("值", "ratio"), Column("怎麼算", "text")), (
                  Row("母體正例率",
                      (totals.get("positive_rate"), "正例數 ÷ 候選列")),
                  Row("每個 entity 平均正例數",
                      (totals.get("avg_positives_per_entity"),
                       "正例數 ÷ entity 數")),
              )),
        _composition("各期", "期", _sorted(ov.get("by_snap_date")), totals),
        _composition("各 item", "item", _sorted(ov.get("by_item")), totals),
    ]
    slices = ["各期", "各 item"]
    # by_segment groups by the first segment column joined; absent when no
    # segment column was joined (an older run's facts, or none configured).
    # The unmatched group sits last: it is not a segment, it is listed to
    # say how many rows had no value.
    if by_segment:
        notes = ()
        if UNMATCHED_SEGMENT in by_segment:
            notes = (Note(
                f"{UNMATCHED_SEGMENT}＝母體表有這一欄、但這些 query group 在"
                "上面沒有值。它列在這裡，但不進任何 Macro 平均。"),)
        tables.append(_composition(
            "各分群", "分群", unmatched_last(_sorted(by_segment)), totals,
            notes=notes))
        slices.append("各分群")
    if category_ov.get("by_item"):
        # The category grain's own totals are the denominators: it has its
        # own rows (one per query group and category) and its own positives.
        tables.append(_composition(
            "各大類", "大類", _sorted(category_ov["by_item"]),
            category_ov.get("totals", {}) or {}))
        slices.append("各大類")

    return ReportSection(
        title=TITLE,
        description="",
        title_en=TITLE_EN,
        question=QUESTION,
        key="data_stats",
        blocks=[Paragraph(_description(facts, parameters, slices), muted=True),
                *tables],
    )


def _composition(slices: str, row_header: str, by: Mapping[str, dict],
                 totals: Mapping, notes: tuple = ()) -> Table:
    """「正例組成 · {slices}」: a row per slice of ``by``, in its order, the
    columns of :data:`_COMPOSITION_COLUMNS`. A share is the slice's
    landed count over the landed total — the one division the report does (a
    display of two landed numbers); blank when the total is missing or
    zero."""
    n_rows = totals.get("n_rows")
    n_positives = totals.get("n_positives")
    rows = []
    for label, cell in by.items():
        rows.append(Row(str(label), (
            cell.get("n_rows"),
            _share(cell.get("n_rows"), n_rows),
            cell.get("n_positives"),
            _share(cell.get("n_positives"), n_positives),
            cell.get("positive_rate"),
            cell.get("n_entities"),
        )))
    return Table(f"正例組成 · {slices}", row_header, _COMPOSITION_COLUMNS,
                 tuple(rows), notes=notes)


def _sorted(by: Mapping[str, dict] | None) -> dict:
    """``by`` ordered by its keys: a landed group-by comes in no order."""
    return dict(sorted((by or {}).items()))


def _share(part, whole):
    if part is None or not whole:
        return None
    return part / whole


def _description(facts: dict, parameters: dict, slices: list[str]) -> str:
    """The section's sentence: what the tables show, where the shares'
    denominators are, the category grain's rows, where the segment column
    came from, and — with zero-positive groups kept by the dataset draw —
    that every count includes them, unweighted."""
    text = (f"正例怎麼分布在{'、'.join(slices)}。這幾張表的欄都一樣；佔比的"
            "分母是核心概念的總量。")
    if "各大類" in slices:
        text += ("大類粒度是每個 query group 每個大類一列，label＝大類裡任一 "
                 "item 是正例，所以大類的正例數比 item 粒度少，佔比的分母用"
                 "大類粒度自己的合計。")
    text += _segment_sentence(facts.get("segments"))
    ratio = zero_positive_draw_on_test(parameters)
    if ratio:   # monitoring (None) or the dataset dropped them all (0.0): none
        text += (
            f"{kept_ratio_phrase(ratio)}（dataset.test_zero_positive_group_ratio"
            f"），每列帶權重 1／r＝{1 / ratio:.4g}（{ZERO_POSITIVE_GROUP_WEIGHT_COL}"
            "）。這裡的計數與比率都算進了這些組，而且沒有加權：它們描述的是 "
            "dataset 留下來的這張表，會隨 r 改變，不代表全部曝光。與 r ＝ 0 時"
            "相比數字變了是預期的，不是 regression。"
        )
    return text


def _segment_sentence(segments: dict | None) -> str:
    """Where the segment column came from, and which population table lacks
    one — the only place that says why a 各分群 table is missing, so the
    table and the column are both named (a misspelt column lands here).

    ``segments`` is what ``compute_row_facts`` carries from
    ``evaluation_segment_columns``: ``joined`` / ``sources`` / ``missing``
    (ADR-0020 bug 6). Facts without it say nothing.
    """
    if not segments:
        return ""
    out = ""
    joined = segments.get("joined") or []
    sources = segments.get("sources") or {}
    if joined:
        out += ("分群欄 " + "、".join(f"{c} 取自 {sources[c]}" for c in joined)
                + (f"；各分群以 {joined[0]} 分。" if len(joined) > 1 else "。"))
    for col, table in (segments.get("missing") or {}).items():
        out += f"母體表 {table} 無欄 {col}：這一欄本次不分群。"
    return out
