"""熱門度基準 (``report/sections/popularity_baseline``, #537): the popularity
tables, the 「能不能比」 table, one compared table per entry of the section's
list, the ⚠ of each annotation the two sides differ on, and the refusal of a
listed cell the comparison lacks.

The section is drawn off what the nodes land: ``compare_to_baseline`` and
``resolve_display_ks`` run here on the two results, and the window is
``baselines.popularity_window``'s. The two results are
:func:`_bundle`'s — the blocks ``compute_all_metrics`` writes (``overall``,
``per_segment``, ``per_item``, ``macro_avg.by_item``, the ledger, the category
grain), every catalog metric at every K of its grid (``k_values`` resolved ∪
``all_k``, ``metrics_spark._resolve_k_grids``); the numbers are made up. One
test computes both results on Spark: the Macro row's two sides (ADR-0031
decision 8).
"""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.metric_catalog import (
    ITEM,
    QUERY_GROUP,
    CompareEntry,
    metrics_of,
)
from recsys_tfb.pipelines.evaluation.report.sections import popularity_baseline as pb
from recsys_tfb.report.types import Cell, Definitions, Heading, Note, Table

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ITEMS = ("A", "B", "C")
_CATEGORIES = ("AB", "C")


def _grid(k_values, all_k: int) -> list[int]:
    """The K a bundle holds: ``k_values`` resolved ∪ ``all_k``."""
    return sorted({all_k if k == "all" else k for k in k_values} | {all_k})


def _grain(base: float, *, all_k: int, k_values, items, segments,
           macro_items, scored: int, segment_columns) -> dict:
    ks = _grid(k_values, all_k)

    def query_group(b: float) -> dict:
        return {m.key(k): round(b + i / 100 + k / 1000, 6)
                for i, m in enumerate(metrics_of(QUERY_GROUP)) for k in ks}

    def item(b: float) -> dict:
        out = {m.key(k): round(b + i / 100 + k / 1000, 6)
               for i, m in enumerate(metrics_of(ITEM)) if m.per_k
               for k in ks}
        out["mean_pos"] = round(1 + b, 6)
        return out

    return {
        "overall": query_group(base),
        "per_segment": ({s: query_group(base + (i + 1) / 10)
                         for i, s in enumerate(segments)}
                        if segment_columns else {}),
        "per_item": {it: item(base + (i + 1) / 20)
                     for i, it in enumerate(items)},
        "macro_avg": {"by_item": item(base + 0.02) if macro_items else {}},
        "population": {
            "all_k": all_k, "macro_items": list(macro_items),
            "n_scored_queries_with_positive": scored,
            "n_scored_queries_without_positive": 0,
            "segment_columns": list(segment_columns),
        },
    }


def _bundle(base: float, *, all_k: int = 3, k_values=(1, 2, "all"),
            items=_ITEMS, segments=("s1", "s2"), macro_items=_ITEMS,
            scored: int = 10, segment_columns=("segment",),
            category: bool = True) -> dict:
    """A metrics bundle: the item grain and, with ``category``, the category
    grain (two categories, so its ``"all"`` is 2). The ledger arguments
    change the item grain only; the category grain keeps the defaults, so a
    test that makes one annotation differ makes it differ in one place."""
    out = _grain(base, all_k=all_k, k_values=k_values, items=items,
                 segments=segments, macro_items=macro_items, scored=scored,
                 segment_columns=segment_columns)
    if category:
        out["category"] = _grain(
            base + 0.01, all_k=2, k_values=k_values, items=_CATEGORIES,
            segments=segments, macro_items=_CATEGORIES, scored=10,
            segment_columns=("segment",))
    return out


def _params(*, compare_k=(1, "all"), k_values=(1, 2, "all"), **evaluation):
    params = {"evaluation": {
        "k_values": list(k_values),
        "report": {"display": {"compare_k": list(compare_k)}},
        **evaluation}}
    if compare_k is None:
        del params["evaluation"]["report"]["display"]["compare_k"]
    return params


def _windowed(base: dict, parameters: dict, *, windows=None) -> dict:
    """``base`` with the popularity window ``compute_baseline_metrics`` lands
    for it (``baselines.popularity_window``): over the monthly table the
    section prints and each evaluated date's covered months — the rate
    block's own, ``windows`` for several dates in count mode, or one date's
    window holding the monthly table's months."""
    from recsys_tfb.evaluation.baselines import (
        baseline_score,
        popularity_window,
        resolve_lookback_months,
    )

    rate = base.get("popularity_rate")
    monthly = (rate.get("monthly_positives") if rate
               else base.get("monthly_counts")) or {}
    if rate:
        windows = rate.get("window_months_covered")
    return {**base, "popularity_window": popularity_window(
        monthly, windows, snap_dates=["2026-01-31"],
        lookback_months=resolve_lookback_months(parameters),
        score=baseline_score(parameters))}


def _baseline(parameters=None, *, counts=None, monthly=None, windows=None,
              rate=None, **changed) -> dict:
    """The baseline's metrics as ``compute_baseline_metrics`` lands them:
    the bundle (``changed`` makes one annotation differ from the model's),
    the popularity counts and the window."""
    base = _bundle(0.3, **changed)
    base["purchase_counts"] = ({"A": 240, "B": 120, "C": 0}
                               if counts is None else counts)
    # By default the twelve months of the default lookback, all covered.
    base["monthly_counts"] = ({item: {f"2025-{m:02d}": per for m in range(1, 13)}
                               for item, per in (("A", 20), ("B", 10), ("C", 0))}
                              if monthly is None else monthly)
    if rate is not None:
        base["popularity_rate"] = rate
    return _windowed(base, parameters or _params(), windows=windows)


#: ``evaluation_row_facts`` as the section reads it: the item count of each
#: grain (``shared.count_items``).
_FACTS = {"dataset_overview": {"totals": {"n_items": len(_ITEMS)}},
          "category": {"dataset_overview": {
              "totals": {"n_items": len(_CATEGORIES)}}}}


def _draw(model=None, baseline=None, parameters=None, facts=None):
    """The section drawn off what ``compare_to_baseline`` and
    ``resolve_display_ks`` land for ``model`` and ``baseline`` under
    ``parameters``."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        compare_to_baseline,
        resolve_display_ks,
    )
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
        fingerprint,
    )

    parameters = parameters or _params()
    model = {**(model or _bundle(0.5)), "config_fingerprint": fingerprint(
        parameters, "compute_metrics")}
    baseline = {**(baseline or _baseline(parameters)),
                "config_fingerprint": fingerprint(
                    parameters, "compute_baseline_metrics")}
    comparison = compare_to_baseline(model, baseline, parameters)
    results = {
        "evaluation_row_facts": facts or _FACTS,
        "baseline_metrics": baseline,
        "baseline_comparison": comparison,
        "evaluation_display_ks": resolve_display_ks(
            model, parameters, baseline_comparison=comparison),
    }
    return pb.build(results, parameters)


def _by_heading(section) -> dict[str, list]:
    """The section's blocks under each ``Heading`` (``""``: before the
    first), the definitions block left out."""
    out: dict[str, list] = {"": []}
    current = ""
    for block in section.blocks:
        if isinstance(block, Heading):
            current = block.text
            out[current] = []
        elif not isinstance(block, Definitions):
            out[current].append(block)
    return out


def _tables(blocks) -> list[Table]:
    return [b for b in blocks if isinstance(b, Table)]


def _table(section, title: str, heading: str = "") -> Table:
    (table,) = [t for t in _tables(_by_heading(section)[heading])
                if t.title == title]
    return table


def _row(table: Table, label: str):
    (row,) = [r for r in table.rows if r.label == label]
    return row


def _description(section) -> str:
    return section.blocks[0].text


_ITEM_TITLES = ["map@K · 整體與各分群", "precision@K · 整體", "recall@K · 整體",
                "map_attr@K · 各 item", "per-item recall@K · 各 item"]


# ---------------------------------------------------------------------------
# The tables follow the list
# ---------------------------------------------------------------------------


def test_each_table_is_one_entry_of_the_list_at_compare_k():
    """Six entries, six tables: five at the item grain, the category one
    under 大類粒度. compare_k [1, "all"] is @1 and @3 at the item grain (three
    items) and @1 and @2 at the category grain (two categories), each K
    three columns 這次 / 對照 / 差."""
    section = _draw()
    grains = _by_heading(section)
    assert list(grains) == ["", "item 粒度", "大類粒度"]
    item, category = _tables(grains["item 粒度"]), _tables(grains["大類粒度"])
    assert len(item) + len(category) == len(pb._COMPARED) == 6
    assert [t.title for t in item] == _ITEM_TITLES
    assert [t.title for t in category] == ["map@K · 整體"]
    for tables, ks in ((item, [1, 3]), (category, [1, 2])):
        for t in tables:
            assert [(c.group, c.label) for c in t.columns] == [
                (f"@{k}", label) for k in ks for label in ("這次", "對照", "差")
            ], t.title
    assert [r.label for r in item[0].rows] == ["整體", "s1", "s2"]
    assert [r.label for r in item[3].rows] == ["Macro 平均", "A", "B", "C"]
    assert [r.pinned for r in item[3].rows] == [True, False, False, False]
    assert [t.row_header for t in item] == ["分群"] * 3 + ["item"] * 2


def test_a_changed_list_changes_the_tables(monkeypatch):
    """The tables are the list's, not written out: another list, other
    tables, and no 大類粒度 without a category entry."""
    monkeypatch.setattr(pb, "_COMPARED", (
        CompareEntry("recall", QUERY_GROUP, "per_segment"),
        CompareEntry("map_attr", ITEM, "per_item")))
    grains = _by_heading(_draw())
    assert list(grains) == ["", "item 粒度"]
    assert [t.title for t in _tables(grains["item 粒度"])] == [
        "recall@K · 整體與各分群", "map_attr@K · 各 item"]


def test_tables_are_named_by_the_catalog_s_display_names(monkeypatch):
    """The two recalls share a name (ADR-0031 decision 6); the titles tell
    them apart by the catalog's display name, read when drawn."""
    from dataclasses import replace

    from recsys_tfb.evaluation import metric_catalog as mc

    renamed = {("recall", ITEM): "item-recall", ("map", QUERY_GROUP): "MAP!"}
    monkeypatch.setattr(mc, "METRICS", tuple(
        replace(m, display_name=renamed.get((m.name, m.method),
                                            m.display_name))
        for m in mc.METRICS))
    titles = [t.title for t in _tables(_by_heading(_draw())["item 粒度"])]
    assert titles == ["MAP!@K · 整體與各分群", "precision@K · 整體",
                      "recall@K · 整體", "map_attr@K · 各 item",
                      "item-recall@K · 各 item"]


def test_cells_are_the_comparison_s_this_reference_and_difference():
    """這次 is the model's value, 對照 the baseline's, 差 their difference;
    the @3 column (``"all"``) reads the cells pairing each side's own all."""
    model, baseline = _bundle(0.5), _baseline()
    section = _draw(model, baseline)
    seg = _row(_table(section, "map@K · 整體與各分群", "item 粒度"), "s2")
    this, reference = model["per_segment"]["s2"], baseline["per_segment"]["s2"]
    assert seg.cells == pytest.approx((
        this["map@1"], reference["map@1"], this["map@1"] - reference["map@1"],
        this["map@3"], reference["map@3"], this["map@3"] - reference["map@3"]))


def test_all_is_read_at_the_recorded_k_not_the_item_count():
    """With ``event`` rows "all" is the widest group (4), not the item count
    (3): the header writes @4 and the cells are the K=4 values."""
    model, baseline = _bundle(0.5, all_k=4), _baseline(all_k=4)
    section = _draw(model, baseline)
    table = _table(section, "recall@K · 整體", "item 粒度")
    assert [c.group for c in table.columns][-1] == "@4"
    assert _row(table, "整體").cells[3:] == pytest.approx((
        model["overall"]["recall@4"], baseline["overall"]["recall@4"],
        model["overall"]["recall@4"] - baseline["overall"]["recall@4"]))


def test_segments_are_sorted_with_unmatched_last():
    section = _draw(_bundle(0.5, segments=("s2", "(unmatched)", "s1")),
                    _baseline(segments=("s2", "(unmatched)", "s1")))
    table = _table(section, "map@K · 整體與各分群", "item 粒度")
    assert [r.label for r in table.rows] == ["整體", "s1", "s2", "(unmatched)"]


def test_the_definitions_come_from_the_catalog_with_the_macro():
    (defs,) = [b for b in _draw().blocks if isinstance(b, Definitions)]
    assert [term for term, _ in defs.entries] == [
        "map@K", "precision@K", "recall@K", "map_attr@K", "per-item recall@K",
        "Macro 平均"]


def test_the_macro_rows_have_both_sides_off_the_real_computation(spark):
    """ADR-0031 decision 8: the baseline goes through the model's metrics,
    so both item tables lead with a Macro 平均 row holding both sides — the
    first time the section has one. Computed on Spark, the rows of
    ``test_nodes.TestComputeBaselineMetrics``: per-item recall@1 of A, B, C
    is 1, 1, 0 for the model and 1, 0, 0 for popularity (A 3 > B 1 > C 0),
    so the Macro is 2/3 against 1/3. ``compare_k`` is not written: the K are
    ``k_values`` [1, 2, 3]."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        compare_to_baseline,
        compute_baseline_metrics,
        resolve_display_ks,
    )
    from tests.test_pipelines.test_evaluation import test_nodes as t

    params = t.TestComputeBaselineMetrics._parameters()
    frame = t.TestComputeBaselineMetrics._eval_predictions(spark)
    facts = t._row_facts(frame, params)
    metrics = t.TestCompareToBaseline._metrics(spark, frame, params)
    baseline = compute_baseline_metrics(
        frame, facts, t.TestComputeBaselineMetrics._label_table(spark),
        t._no_segments(params), params)
    comparison = compare_to_baseline(metrics, baseline, params)
    section = pb.build({
        "evaluation_row_facts": facts, "baseline_metrics": baseline,
        "baseline_comparison": comparison,
        "evaluation_display_ks": resolve_display_ks(
            metrics, params, baseline_comparison=comparison)}, params)

    for title in ("map_attr@K · 各 item", "per-item recall@K · 各 item"):
        table = _table(section, title, "item 粒度")
        assert [c.group for c in table.columns][::3] == ["@1", "@2", "@3"]
        macro = table.rows[0]
        assert (macro.label, macro.pinned) == ("Macro 平均", True)
        assert None not in macro.cells, title
    recall = _table(section, "per-item recall@K · 各 item", "item 粒度")
    assert recall.rows[0].cells[:3] == pytest.approx((2 / 3, 1 / 3, 1 / 3))
    can = _table(section, "能不能比")
    assert [r.cells[2] for r in can.rows] == ["相同"] * 4


# ---------------------------------------------------------------------------
# Can the two be compared: the table and the ⚠
# ---------------------------------------------------------------------------

#: Each case: the one annotation the baseline differs on, the bundle change
#: that makes it differ, where its ⚠ sits — columns (title, @K) and rows
#: (title, label) of the item-grain tables — and the line under them.
_CASES = {
    "all_k": (dict(all_k=2), {(t, "@3") for t in _ITEM_TITLES}, set(),
              "@3：「全部」這次是 @3、對照是 @2，這一欄兩邊各取自己的全部。"),
    "macro_items": (dict(macro_items=("A", "B")), set(),
                    {("map_attr@K · 各 item", "Macro 平均"),
                     ("per-item recall@K · 各 item", "Macro 平均")},
                    "Macro 平均：這次 3 個 item、對照 2 個（對照少 C），平均的 "
                    "item 不同。"),
    "scored_queries": (dict(scored=9), set(), set(),
                       "算進指標的 query group：這次 10 組、對照 9 組，兩邊的"
                       "分母不同。"),
    # Not marked under the tables: the segment rows are left out, and the
    # 能不能比 table is where it is said (decision 16 item 15).
    "segment_columns": (dict(segment_columns=()), set(), set(), None),
}
_CHECK_ROW = {"all_k": "全部＝@幾", "macro_items": "進 Macro 的 item",
              "scored_queries": "算進指標的 query group",
              "segment_columns": "分群欄"}


@pytest.mark.parametrize("name", list(_CASES))
def test_a_difference_is_marked_where_it_matters_and_nowhere_else(name):
    changed, columns, rows, sentence = _CASES[name]
    section = _draw(baseline=_baseline(**changed))
    can = _table(section, "能不能比")
    assert [(r.label, r.cells[2]) for r in can.rows] == [
        (label, Cell("不同", warn=True) if key == name else "相同")
        for key, label in _CHECK_ROW.items()]

    grains = _by_heading(section)
    item = _tables(grains["item 粒度"])
    assert {(t.title, c.group) for t in item for c in t.columns
            if c.warn} == columns
    assert {(t.title, r.label) for t in item for r in t.rows if r.warn} == rows
    assert [n.text for n in grains["item 粒度"]
            if isinstance(n, Note) and n.warn] == ([sentence] if sentence else [])
    # The category grain's two sides agree: nothing marked there.
    category = grains["大類粒度"]
    assert not any(c.warn for t in _tables(category) for c in t.columns)
    assert not any(r.warn for t in _tables(category) for r in t.rows)
    assert not [n for n in category if isinstance(n, Note)]


def test_segments_the_baseline_lacks_leave_only_the_overall_row():
    """ADR-0031 decision 16 item 15: with the two sides on different segment
    columns the segment rows are not drawn (not refused, not blank); the
    table keeps 整體 and is titled for it."""
    section = _draw(baseline=_baseline(segment_columns=()))
    item = _tables(_by_heading(section)["item 粒度"])
    assert "map@K · 整體與各分群" not in [t.title for t in item]
    table = _table(section, "map@K · 整體", "item 粒度")
    assert [r.label for r in table.rows] == ["整體"]


def test_the_macro_line_says_how_many_items_when_fewer_than_all():
    section = _draw(_bundle(0.5, macro_items=("A", "B")),
                    _baseline(macro_items=("A", "B")))
    notes = [n for n in _by_heading(section)["item 粒度"]
             if isinstance(n, Note)]
    assert notes == [Note("參與 macro 的 item 數：2")]
    assert not [n for n in _by_heading(_draw())["item 粒度"]
                if isinstance(n, Note)]


def test_no_macro_row_when_neither_side_averaged_an_item():
    """``metric.min_positives`` above every item's positives: both macros
    are empty, so there is no Macro row to draw and none is refused."""
    section = _draw(_bundle(0.5, macro_items=()), _baseline(macro_items=()))
    table = _table(section, "map_attr@K · 各 item", "item 粒度")
    assert [r.label for r in table.rows] == ["A", "B", "C"]
    (defs,) = [b for b in section.blocks if isinstance(b, Definitions)]
    assert "Macro 平均" not in [term for term, _ in defs.entries]


# ---------------------------------------------------------------------------
# A cell the list names and the comparison lacks
# ---------------------------------------------------------------------------


def _without(bundle: dict, *path) -> dict:
    """``bundle`` with the value at ``path`` removed."""
    cursor = bundle
    for key in path[:-1]:
        cursor = cursor[key]
    del cursor[path[-1]]
    return bundle


@pytest.mark.parametrize("side, path, where", [
    ("reference", ("per_item", "B", "recall@1"),
     "per_item.cells.B.recall@1.reference"),
    ("this", ("overall", "precision@3"), "overall.cells.precision@all.this"),
    ("reference", ("per_segment", "s1", "map@1"),
     "per_segment.cells.s1.map@1.reference"),
    ("reference", ("macro_avg", "by_item", "map_attr@1"),
     "macro_by_item.cells.map_attr@1.reference"),
    ("reference", ("category", "overall", "map@2"),
     "category.overall.cells.map@all.reference"),
])
def test_a_listed_cell_the_comparison_lacks_is_refused(side, path, where):
    model, baseline = _bundle(0.5), _baseline()
    _without(model if side == "this" else baseline, *path)
    with pytest.raises(ValueError) as exc:
        _draw(model, baseline)
    message = str(exc.value)
    assert f"baseline_comparison has no {where}," in message
    assert "--from-node compute_row_facts" in message


# ---------------------------------------------------------------------------
# The popularity it ranked by
# ---------------------------------------------------------------------------


def _lookback(months: int = 12, **baseline) -> dict:
    return _params(baseline={"lookback_months": months, **baseline})


def test_count_mode_ranks_by_the_count_with_the_monthly_average():
    section = _draw()
    table = _table(section, "購買次數與名次 · 各 item")
    assert [c.label for c in table.columns] == ["購買次數", "平均每月", "名次"]
    assert [(r.label, r.cells) for r in table.rows] == [
        ("A", (240, 20.0, 1)), ("B", (120, 10.0, 2)), ("C", (0, 0.0, 3))]
    assert "熱門度用過去 12 個月每個 item 的購買次數排出名次" in _description(
        section)
    assert "實際只涵蓋" not in _description(section)


def test_the_monthly_table_is_the_counts_by_month_in_the_same_order():
    """Rows in the ranking's order, months ascending, a month an item has no
    entry for 0, 合計 the item's count."""
    section = _draw(baseline=_baseline(
        counts={"A": 3, "B": 1},
        monthly={"A": {"2024-12": 1, "2024-06": 2}, "B": {"2024-06": 1}}))
    table = _table(section, "每月購買次數 · 各 item")
    assert [c.label for c in table.columns] == ["2024-06", "2024-12", "合計"]
    assert [(r.label, r.cells) for r in table.rows] == [
        ("A", (2, 1, 3)), ("B", (1, 0, 1))]
    assert len(_table(_draw(), "每月購買次數 · 各 item").columns) == 13


def test_a_configured_lookback_is_the_one_printed():
    params = _lookback(6)
    section = _draw(baseline=_baseline(params), parameters=params)
    assert "過去 6 個月" in _description(section)


def test_a_partly_covered_window_says_so_and_divides_by_what_it_covered():
    """bug 1 (ADR-0020): 2 of the 12 lookback months hold label rows, so the
    monthly average divides by 2, and the sentence says it."""
    section = _draw(baseline=_baseline(
        counts={"A": 24, "B": 6}, monthly={"A": {"2025-11": 12, "2025-12": 12},
                                           "B": {"2025-12": 6}}))
    assert "實際只涵蓋 2 個月" in _description(section)
    table = _table(section, "購買次數與名次 · 各 item")
    assert _row(table, "A").cells[1] == 12.0


def test_several_dates_divide_by_every_window_s_months():
    """#374: two full windows, so 240 over 2 × 12 window-months."""
    section = _draw(baseline=_baseline(
        counts={"A": 240}, monthly={"A": {f"2025-{m:02d}": 20
                                          for m in range(1, 13)}},
        windows={"2026-01-31": 12, "2026-02-28": 12}))
    text = _description(section)
    assert _row(_table(section, "購買次數與名次 · 各 item"),
                "A").cells[1] == 10.0
    for phrase in ("對 2 個評估日期各用該日期之前 12 個月", "÷ 24", "重複計數"):
        assert phrase in text, phrase
    assert "實際只涵蓋" not in text


def test_several_partly_covered_windows_divide_by_the_covered_months():
    section = _draw(baseline=_baseline(
        counts={"A": 240},
        monthly={"A": {"2025-11": 100, "2025-12": 100, "2026-01": 40}},
        windows={"2026-01-31": 2, "2026-02-28": 3}))
    text = _description(section)
    assert _row(_table(section, "購買次數與名次 · 各 item"),
                "A").cells[1] == 48.0
    assert "實際只涵蓋 5 個視窗月" in text and "滿額 24 個" in text


def test_one_date_says_nothing_about_several_dates():
    text = _description(_draw())
    for phrase in ("評估日期", "視窗月", "重複計數"):
        assert phrase not in text, phrase


def _rate(**changed) -> dict:
    """#397's rate block; the count-mode counts beside it differ on
    purpose (999), so reading the wrong source shows."""
    rate = {"rate": {"A": 0.5, "B": 0.8, "C": 0.2},
            "candidates": {"A": 100, "B": 50, "C": 40},
            "positives": {"A": 50, "B": 40, "C": 8},
            "monthly_positives": {"A": {"2025-11": 30, "2025-12": 20},
                                  "B": {"2025-11": 40}, "C": {"2025-11": 8}},
            "window_months_covered": {"2026-01-31": 2}}
    rate.update(changed)
    return rate


def _rate_baseline(params, **changed):
    return _baseline(params, counts={"A": 999, "B": 999, "C": 999},
                     monthly={"A": {"2025-11": 999}}, rate=_rate(**changed))


def test_rate_mode_ranks_by_the_rate_with_its_numerator_and_denominator():
    params = _lookback()
    section = _draw(baseline=_rate_baseline(params), parameters=params)
    table = _table(section, "正例率與名次 · 各 item")
    assert [c.label for c in table.columns] == [
        "正例率", "當候選次數", "正例數", "名次"]
    assert [(r.label, r.cells) for r in table.rows] == [
        ("B", (0.8, 50, 40, 1)), ("A", (0.5, 100, 50, 2)),
        ("C", (0.2, 40, 8, 3))]
    monthly = _table(section, "每月正例數 · 各 item")
    assert [(r.label, r.cells) for r in monthly.rows] == [
        ("B", (40, 0, 40)), ("A", (30, 20, 50)), ("C", (8, 0, 8))]
    text = _description(section)
    for banned in ("購買次數", "平均每月"):
        assert banned not in text, banned
    assert "sample_pool" in text and "label_table" not in text
    assert "實際只涵蓋 2 個月" in text


def test_rate_mode_with_several_dates_counts_each_period_once():
    params = _lookback()
    section = _draw(baseline=_rate_baseline(params, window_months_covered={
        "2026-01-31": 12, "2026-02-28": 12}), parameters=params)
    text = _description(section)
    assert "對 2 個評估日期" in text and "每期只算一次" in text
    assert "重複計數" not in text and "實際只有" not in text


def test_count_mode_under_score_rate_says_why():
    """Monitoring with ``score: rate``: the node ranked by the count."""
    params = _lookback(score="rate")
    text = _description(_draw(baseline=_baseline(params), parameters=params))
    assert "監控模式評的是離線推論的全網格" in text
    assert "全網格" not in _description(_draw())


def test_no_popularity_table_when_the_counts_are_absent():
    section = _draw(baseline=_baseline(counts={}, monthly={}))
    assert [t.title for t in _tables(_by_heading(section)[""])] == ["能不能比"]
