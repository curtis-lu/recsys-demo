"""Tables both reports draw from landed metrics: the small tools the main
report's and the comparison report's sections share, so the two draw a metric
(ADR-0031 decision 16), a comparison and its caveats (the comparison's
``population``, decisions 4 and 15) the same way.

Reads only landed results — a ``metrics_spark.compute_all_metrics`` bundle, or
a comparison (``pipelines/evaluation/steps/comparison_layer``'s shape; its
keys are ``result_readers``' constants) — and the catalog. Imports nothing
that computes (S9, ADR-0031 principle 1).
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from recsys_tfb.evaluation.metric_catalog import (
    GRAIN_DEFINITIONS,
    MODEL_SELECTION_METRICS,
    CompareEntry,
    Metric,
    ModelSelectionMetric,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    ALL_LABEL,
    CELLS,
    DIFF,
    MACRO_ITEMS_KEY,
    N_SCORED_WITH_POSITIVE_KEY,
    N_SCORED_WITHOUT_POSITIVE_KEY,
    POPULATION_KEY,
    REFERENCE,
    SAME,
    SCORED_QUERIES_KEY,
    SEGMENT_COLUMNS_KEY,
    THIS,
)
from recsys_tfb.report.types import Cell, Column, Definitions, Note, Row, Table

#: The three column labels of a compared value (decision 16 item 8); 差 is
#: 這次 − 對照.
THIS_LABEL, REFERENCE_LABEL, DIFF_LABEL = "這次", "對照", "差"

#: The title of the model-selection table, the same in both reports.
MODEL_SELECTION_TITLE = "模型選擇指標"


# ---------------------------------------------------------------------------
# One metric, one table
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SliceRow:
    """One row of a metric table: a slice and its landed values.

    ``values`` is that slice's block, keyed by metric key (``map@3``): one
    side — a bundle's block for it; compared — the comparison's cells for it,
    each ``{this, reference, diff}``. ``None`` or a missing key is a blank
    cell. ``pinned`` keeps the row on top (整體, Macro 平均); ``warn`` marks
    its label ⚠.
    """

    label: str
    values: Mapping[str, Any] | None
    pinned: bool = False
    warn: bool = False


def _metric_of(metric: Metric | CompareEntry) -> Metric:
    return metric.metric if isinstance(metric, CompareEntry) else metric


def _sides(cell: Any) -> tuple:
    """A comparison cell's (這次, 對照, 差); blank when the key is absent."""
    if not isinstance(cell, Mapping):
        return (None, None, None)
    return (cell.get(THIS), cell.get(REFERENCE), cell.get(DIFF))


def metric_table(
    metric: Metric | CompareEntry,
    ks: Sequence[int],
    rows: Sequence[SliceRow],
    *,
    row_header: str,
    slices: str = "",
    compare: bool = False,
    all_k: int | None = None,
    warn_ks: Collection[int] = (),
    notes: Sequence[Note] = (),
) -> Table:
    """One metric's table: rows are ``rows`` (slices), columns the K in ``ks``.

    * ``metric`` — a catalog entry (or a :class:`CompareEntry`); its
      ``display_name`` titles the table, ``f"{display_name}@K · {slices}"``
      (no `` · …`` when ``slices`` is empty).
    * ``ks`` — the K to show, already resolved to integers (the table header
      writes the real K: ``@6``, not ``@all``). Ignored for a metric without
      K (``mean_pos``): the table then has one value column and is titled
      ``display_name`` alone.
    * One side (``compare=False``): a column ``@K`` per K, each cell
      ``values[metric.key(k)]``.
    * Compared (``compare=True``): per K three columns 這次 / 對照 / 差 under
      an ``@K`` header. ``all_k`` is required: the column ``k == all_k`` reads
      the cell pairing each side's own "all" (``name@all``, which may be a
      different K on the other side — the comparison layer pairs it), every
      other column ``name@{k}``.
    * ``warn_ks`` — K whose columns get ⚠ (a compared K's ⚠ sits on its
      ``@K`` header). Put the sentence saying why in ``notes``
      (:meth:`Comparability.note`).
    """
    m = _metric_of(metric)
    if compare and m.per_k and all_k is None:
        raise ValueError(
            f"metric_table({m.name!r}, compare=True) needs all_k: the column "
            "at all_k reads the cell pairing each side's own all")
    for k in ks:
        if isinstance(k, bool) or not isinstance(k, int) or k < 1:
            raise ValueError(
                f"metric_table({m.name!r}): ks are resolved positive integers; "
                f"got {k!r}")
    title = f"{m.display_name}@K" if m.per_k else m.display_name
    if slices:
        title = f"{title} · {slices}"

    if not m.per_k:
        if compare:
            columns = [Column(THIS_LABEL, "ratio"), Column(REFERENCE_LABEL, "ratio"),
                       Column(DIFF_LABEL, "diff")]
        else:
            columns = [Column("值", "ratio")]
        out_rows = []
        for r in rows:
            values = r.values or {}
            cells = (_sides(values.get(m.key())) if compare
                     else (values.get(m.key()),))
            out_rows.append(Row(r.label, cells, pinned=r.pinned, warn=r.warn))
        return Table(title, row_header, tuple(columns), tuple(out_rows),
                     tuple(notes))

    columns = []
    for k in ks:
        warn = k in warn_ks
        if compare:
            group = f"@{k}"
            columns += [Column(THIS_LABEL, "ratio", group=group, warn=warn),
                        Column(REFERENCE_LABEL, "ratio", group=group, warn=warn),
                        Column(DIFF_LABEL, "diff", group=group, warn=warn)]
        else:
            columns.append(Column(f"@{k}", "ratio", warn=warn))

    out_rows = []
    for r in rows:
        values = r.values or {}
        cells: list = []
        for k in ks:
            if compare:
                cells += _sides(values.get(compared_key(m, k, all_k)))
            else:
                cells.append(values.get(m.key(k)))
        out_rows.append(Row(r.label, tuple(cells), pinned=r.pinned, warn=r.warn))
    return Table(title, row_header, tuple(columns), tuple(out_rows),
                 tuple(notes))


def compared_key(metric: Metric | CompareEntry, k: int, all_k: int) -> str:
    """The cell a compared table's column at ``k`` reads: ``name@all`` at
    the grain's ``all_k`` — the comparison's pairing of each side's own
    "all", which may be another K on the other side — else ``name@{k}``; a
    metric without K, its one key.

    The one rule: :func:`metric_table` draws by it, and the sections that
    check a cell before drawing look up by it, so the cell checked is the
    cell drawn.
    """
    return _metric_of(metric).key(ALL_LABEL if k == all_k else k)


_MACRO_COUNT_LABEL = {False: "參與 macro 的 item 數", True: "參與 macro 的大類數"}


def macro_count_note(n_macro: int, n_items: int, *,
                     category: bool = False) -> Note | None:
    """「參與 macro 的 item 數：N」 (at the category grain 「參與 macro 的大類數：N」)
    under an item grain's tables, or ``None``: printed only when the Macro
    averaged fewer than all ``n_items`` (ADR-0031 decision 16 item 5;
    ``n_items`` is ``shared.count_items``, 0 = unknown: no line). The count
    alone, without the total: the whole data's totals are printed once, in
    核心概念 (decision 16 item 12).

    The caller decides whether the line belongs under its tables at all — a
    Macro row drawn, and in a comparison the two sides averaging the same
    items (else the ⚠ line says so).
    """
    if 0 < n_items and n_macro < n_items:
        return Note(f"{_MACRO_COUNT_LABEL[category]}：{n_macro:,}")
    return None


# ---------------------------------------------------------------------------
# Can the two be compared as they are
# ---------------------------------------------------------------------------

#: The four checks, in the order the table lists them.
_CHECK_LABELS = (
    (ALL_K_KEY, "全部＝@幾"),
    (MACRO_ITEMS_KEY, "進 Macro 的 item"),
    (SCORED_QUERIES_KEY, "算進指標的 query group"),
    (SEGMENT_COLUMNS_KEY, "分群欄"),
)

_ITEMS_LISTED = 5


def _scored(value: Mapping) -> int:
    """The query groups a side scored: with a positive, plus without (0 by
    construction, counted all the same)."""
    return int(value[N_SCORED_WITH_POSITIVE_KEY]) + int(
        value.get(N_SCORED_WITHOUT_POSITIVE_KEY) or 0)


def _names(items: Iterable) -> str:
    items = [str(i) for i in items]
    if len(items) > _ITEMS_LISTED:
        return f"{'、'.join(items[:_ITEMS_LISTED])} 等 {len(items)} 個"
    return "、".join(items)


def _item_difference(this: Sequence, reference: Sequence) -> str:
    """What the reference's macro items lack and add, from its side."""
    ref = set(map(str, reference))
    mine = set(map(str, this))
    fewer = sorted(mine - ref)
    more = sorted(ref - mine)
    parts = []
    if fewer:
        parts.append(f"少 {_names(fewer)}")
    if more:
        parts.append(f"多 {_names(more)}")
    return "、".join(parts)


def _columns_text(columns: Sequence) -> str:
    return "、".join(map(str, columns)) if columns else "沒有"


@dataclass(frozen=True)
class Comparability:
    """Which of the four annotations of a comparison's ``population`` differ
    between the two sides (``True`` = differs), with the values to say so.

    Built by :func:`comparability`. A section marks what each one affects
    (decision 16):

    * ``all_k_differs`` — the ``@{all_k}`` columns (``warn_ks`` of
      :func:`metric_table`).
    * ``macro_items_differs`` — the Macro 平均 row (``SliceRow(...,
      warn=True)``) and the macro model-selection row.
    * ``scored_queries_differs`` — a line under the table only.
    * ``segment_columns_differs`` — every segment row.

    and puts :meth:`note`'s sentence under the table.
    """

    all_k_differs: bool
    macro_items_differs: bool
    scored_queries_differs: bool
    segment_columns_differs: bool
    population: Mapping

    @property
    def differs(self) -> tuple[str, ...]:
        """The annotation names that differ, in table order."""
        flags = {ALL_K_KEY: self.all_k_differs,
                 MACRO_ITEMS_KEY: self.macro_items_differs,
                 SCORED_QUERIES_KEY: self.scored_queries_differs,
                 SEGMENT_COLUMNS_KEY: self.segment_columns_differs}
        return tuple(name for name, _ in _CHECK_LABELS if flags[name])

    def note(self, name: str, subject: str | None = None) -> Note | None:
        """The ⚠ line under a table for annotation ``name``, or ``None`` when
        the two sides agree on it. ``subject`` replaces the default lead-in
        (``Macro 平均``) with the row name it is about."""
        if name not in self.differs:
            return None
        a = self.population[name][THIS]
        b = self.population[name][REFERENCE]
        if name == ALL_K_KEY:
            body = (f"「全部」這次是 @{a}、對照是 @{b}，這一欄兩邊各取自己的"
                    "全部。")
            lead = f"@{a}"
        elif name == MACRO_ITEMS_KEY:
            body = (f"這次 {len(a)} 個 item、對照 {len(b)} 個（對照"
                    f"{_item_difference(a, b)}），平均的 item 不同。")
            lead = "Macro 平均"
        elif name == SCORED_QUERIES_KEY:
            body = (f"這次 {_scored(a):,} 組、對照 {_scored(b):,} 組，兩邊的"
                    "分母不同。")
            lead = "算進指標的 query group"
        elif name == SEGMENT_COLUMNS_KEY:
            if not b:
                body = f"對照沒有分群欄 {_columns_text(a)}，各分群的對照與差留空。"
            elif not a:
                body = f"這次沒有分群欄 {_columns_text(b)}，各分群的這次與差留空。"
            else:
                body = (f"分群欄這次是 {_columns_text(a)}、對照是 "
                        f"{_columns_text(b)}，兩邊的分群不同。")
            lead = None
        else:
            raise KeyError(f"no annotation {name!r}; the four are "
                           f"{[n for n, _ in _CHECK_LABELS]}")
        lead = subject if subject is not None else lead
        return Note(f"{lead}：{body}" if lead else body, warn=True)


def comparability(population: Mapping) -> Comparability:
    """The flags of a comparison's ``population`` (``comparison["population"]``,
    or ``comparison["category"]["population"]`` for the category grain): each
    annotation's ``same``, read as landed — never recomputed."""
    def differs(name: str) -> bool:
        return not population[name][SAME]

    return Comparability(
        all_k_differs=differs(ALL_K_KEY),
        macro_items_differs=differs(MACRO_ITEMS_KEY),
        scored_queries_differs=differs(SCORED_QUERIES_KEY),
        segment_columns_differs=differs(SEGMENT_COLUMNS_KEY),
        population=population,
    )


def comparability_table(population: Mapping, title: str = "能不能比") -> Table:
    """The 「能不能比」 table: one row per check, columns 這次 / 對照 / 結果.
    結果 is 「相同」, or 「⚠ 不同」 (the ⚠ is not copied) when the two sides
    differ; then the 對照 cell of the macro row also says which items."""
    flags = comparability(population)

    def text(name: str, side: str) -> str:
        value = population[name][side]
        if name == ALL_K_KEY:
            return f"@{value}"
        if name == MACRO_ITEMS_KEY:
            out = f"{len(value)} 個"
            if side == REFERENCE and name in flags.differs:
                diff = _item_difference(population[name][THIS], value)
                out = f"{out}，{diff}" if diff else out
            return out
        if name == SCORED_QUERIES_KEY:
            return f"{_scored(value):,} 組"
        return _columns_text(value)

    rows = []
    for name, label in _CHECK_LABELS:
        result = (Cell("不同", warn=True) if name in flags.differs else "相同")
        rows.append(Row(label, (text(name, THIS), text(name, REFERENCE), result)))
    columns = (Column(THIS_LABEL, "text"), Column(REFERENCE_LABEL, "text"),
               Column("結果", "text"))
    return Table(title, "檢查", columns, tuple(rows))


# ---------------------------------------------------------------------------
# The model-selection metrics
# ---------------------------------------------------------------------------


def _model_selection_value(result: Mapping, metric: ModelSelectionMetric,
                           compare: bool) -> tuple:
    source = metric.source
    m = source.metric
    if source.grain not in ("overall", "macro"):
        raise ValueError(
            f"model-selection metric {metric.name!r} reads grain "
            f"{source.grain!r}; this table reads overall and macro only")
    if compare:
        block = "overall" if source.grain == "overall" else "macro_by_item"
        cells = (result.get(block) or {}).get(CELLS) or {}
        return _sides(cells.get(m.key(ALL_LABEL)))
    all_k = result[POPULATION_KEY][ALL_K_KEY]
    values = (result.get("overall") if source.grain == "overall"
              else (result.get("macro_avg") or {}).get("by_item")) or {}
    return (values.get(m.key(all_k)),)


def model_selection_table(
    result: Mapping,
    *,
    compare: bool = False,
    metrics: Sequence[ModelSelectionMetric] = MODEL_SELECTION_METRICS,
) -> Table:
    """The model-selection metrics, one row each (its ``display_name``), at
    K = all.

    One side: ``result`` is a metrics bundle, read at the bundle's own
    ``all_k``. Compared: ``result`` is a comparison, read at ``name@all``
    (each side's own all). The values are untruncated, so a different ``all_k``
    on the two sides does not touch them; a different set of macro items marks
    the macro rows ⚠, and that or different query groups scored puts a ⚠ line
    under the table.
    """
    rows = []
    notes = []
    flags = (comparability(result[POPULATION_KEY])
             if compare and POPULATION_KEY in result else None)
    for metric in metrics:
        warn = bool(flags and flags.macro_items_differs
                    and metric.source.grain == "macro")
        rows.append(Row(metric.display_name,
                        _model_selection_value(result, metric, compare),
                        warn=warn))
        if warn:
            notes.append(flags.note(MACRO_ITEMS_KEY, subject=metric.display_name))
    if flags and flags.scored_queries_differs:
        notes.append(flags.note(SCORED_QUERIES_KEY))
    if compare:
        columns = (Column(THIS_LABEL, "ratio"), Column(REFERENCE_LABEL, "ratio"),
                   Column(DIFF_LABEL, "diff"))
    else:
        columns = (Column("值", "ratio"),)
    return Table(MODEL_SELECTION_TITLE, "指標", columns, tuple(rows),
                 tuple(notes))


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------


def _term(item: Any) -> tuple[str, str]:
    if isinstance(item, ModelSelectionMetric):
        return (item.display_name, item.definition)
    if isinstance(item, (Metric, CompareEntry)):
        m = _metric_of(item)
        return (f"{m.display_name}@K" if m.per_k else m.display_name,
                m.definition)
    if isinstance(item, str):
        if item not in GRAIN_DEFINITIONS:
            raise KeyError(f"no grain definition {item!r}; the catalog has "
                           f"{sorted(GRAIN_DEFINITIONS)}")
        return GRAIN_DEFINITIONS[item]
    raise TypeError(f"not a catalog entry, a model-selection metric or a "
                    f"grain name: {item!r}")


def definitions(items: Iterable) -> Definitions:
    """The definitions block under a group of tables, from the catalog: each
    item a catalog entry or :class:`CompareEntry`, a
    :class:`ModelSelectionMetric`, or a grain name of ``GRAIN_DEFINITIONS``.
    A term listed twice is kept once, where it first appears."""
    out: dict[str, str] = {}
    for item in items:
        term, text = _term(item)
        out.setdefault(term, text)
    return Definitions(tuple(out.items()))
