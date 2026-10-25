"""概覽 of the comparison report: whom this run is compared with, how much of
each side the comparison covers, and whether the two can be compared as they
are (ADR-0031 decision 16 items 12, 15 and 16). The comparison report has no
核心概念, so the two sides' totals are printed here, in the coverage table."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.pipelines.evaluation.report.sections.metric_tables import (
    REFERENCE_LABEL,
    THIS_LABEL,
    comparability_table,
)
from recsys_tfb.pipelines.evaluation.report.sections.shared import (
    ALL_POSITIVE_SWITCH,
    COMPARE_COVERAGE,
    SOURCE_COMPARISON,
    landed_value,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    POPULATION_KEY,
    THIS,
    drop_all_positive_groups,
)
from recsys_tfb.report.types import Column, Paragraph, Row, Table

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the coverage (both sides' sizes, the dropped items, what each side is) and
#: the comparison (what "all" resolved to, the four annotations).
READS = (COMPARE_COVERAGE, SOURCE_COMPARISON)

#: No switch turns this section off (ADR-0031 principle 3:
#: ``shared.sections_on``).
SWITCH = None

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "概覽"
TITLE_EN = "Overview"
QUESTION = "跟誰比？能不能直接比？"

#: The rows of 「跟誰比」: every key that decides what a side holds
#: (decision 16 item 16; ``compare_sources.compare_source_identity`` — label
#: is not one). A key the landed identity has beyond these is printed after
#: them, so the table never hides one.
SOURCE_KEYS = ("kind", "model_version", "table", "source", "columns",
               "prod_mapping", "unmapped_policy")

#: The coverage's keys for this run's side (``restrict_to_common``):
#: ``{row of 跟誰比: coverage key}``. The compared side's are its
#: ``compare_source``, which every comparison artifact records.
_THIS_SOURCE = {"kind": "kind_a", "model_version": "model_version_a",
                "source": "source_a"}

#: 「涵蓋」: ``(row, (this side's key, the compared side's, the common one's))``.
_COVERAGE_ROWS = (
    ("query group 數", ("n_query_group_A_full", "n_query_group_B_full",
                       "n_query_group_common")),
    ("item 數", ("n_item_A_full", "n_item_B_full", "n_item_common")),
)

#: A cell with nothing in it.
_NONE = "—"


def _text(value) -> str:
    """A source key's value as the table prints it: a mapping as
    ``{key: value, ...}`` in its landed order."""
    if value is None:
        return _NONE
    if isinstance(value, Mapping):
        return "{" + ", ".join(f"{k}: {_text(v)}" for k, v in value.items()) + "}"
    return str(value)


def _source_table(coverage: dict) -> Table:
    this = {row: landed_value(coverage, COMPARE_COVERAGE, key)
            for row, key in _THIS_SOURCE.items()}
    reference = landed_value(coverage, COMPARE_COVERAGE, "compare_source")
    keys = list(SOURCE_KEYS) + [k for k in reference if k not in SOURCE_KEYS]
    rows = tuple(Row(key, (_text(this.get(key)), _text(reference.get(key))))
                 for key in keys)
    return Table("跟誰比", "項目",
                 (Column(THIS_LABEL, "text"), Column(REFERENCE_LABEL, "text")),
                 rows)


def _coverage_table(coverage: dict) -> Table:
    rows = tuple(
        Row(label, tuple(landed_value(coverage, COMPARE_COVERAGE, key)
                         for key in keys))
        for label, keys in _COVERAGE_ROWS)
    return Table("涵蓋", "項目",
                 (Column(THIS_LABEL, "count"), Column(REFERENCE_LABEL, "count"),
                  Column("兩邊共同", "count")),
                 rows)


def _dropped_table(coverage: dict) -> Table:
    sides = [landed_value(coverage, COMPARE_COVERAGE, key)
             for key in ("dropped_items_A", "dropped_items_B")]
    # One column per side, a count row and a names row (the demo's shape),
    # so the columns are text: a column has one kind.
    return Table("拿掉的 item", "項目",
                 (Column(THIS_LABEL, "text"), Column(REFERENCE_LABEL, "text")),
                 (Row("數量", tuple(len(items) for items in sides)),
                  Row("是哪些", tuple("、".join(map(str, items)) or _NONE
                                      for items in sides))))


def _description(parameters: dict, n_items: int, all_k: int) -> str:
    """The query group in the reader's own columns (with ``occasion``
    declared it gains those, #428), and what "all" is on this page: the
    common items' count when that is what it resolved to, else the widest
    query group (``event`` rows, #434) — both read off landed results."""
    query_group = " × ".join(get_schema(parameters)["query_group_columns"])
    all_is = (f"，所以表頭的 @{all_k} 就是全部。" if all_k == n_items else
              f"；表頭的 @{all_k} 就是全部：一個 query group 裡最多的候選列數。")
    return (
        "跟誰比、兩邊涵蓋多少、能不能直接比。一個 query group＝一組 "
        f"{query_group}。之後每一段都在兩邊共同的 entity × item 上重排重算："
        "query group 只算兩邊都還在的組，item 取兩邊的交集，共 "
        f"{n_items:,} 個{all_is}"
    )


#: With the all-positive switch on (#376): both sides dropped their
#: all-positive query groups, each deciding on its own rows
#: (``compute_compare_metrics``), so a group whose candidate rows differ
#: between the sides can be dropped on one side only.
_ALL_POSITIVE_DROPPED = (
    f"{ALL_POSITIVE_SWITCH} 開著：兩邊都排除了全正的 query group，而且各自依"
    "自己的列判定；同一組在兩邊的候選列不對稱時，可能一邊排除、一邊保留。"
)


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """概覽：說明、「跟誰比」「涵蓋」「拿掉的 item」「能不能比」四張表。

    Every value is landed: this side's identity and both sides' sizes off
    the coverage (``restrict_to_common``), the compared side's identity off
    its ``compare_source``, "all" and the four annotations off the
    comparison. A value the coverage lacks is refused with the node to
    re-run (``shared.landed_value``), never printed as "n/a".
    """
    coverage = results[COMPARE_COVERAGE]
    comparison = results[SOURCE_COMPARISON]
    population = landed_value(comparison, SOURCE_COMPARISON, POPULATION_KEY)
    blocks: list = [Paragraph(_description(
        parameters, landed_value(coverage, COMPARE_COVERAGE, "n_item_common"),
        population[ALL_K_KEY][THIS]), muted=True)]
    if drop_all_positive_groups(parameters):
        blocks.append(Paragraph(_ALL_POSITIVE_DROPPED, muted=True))
    blocks += [
        _source_table(coverage),
        _coverage_table(coverage),
        _dropped_table(coverage),
        comparability_table(population),
    ]
    return ReportSection(title=TITLE, description="", title_en=TITLE_EN,
                         question=QUESTION, key="overview", blocks=blocks)
