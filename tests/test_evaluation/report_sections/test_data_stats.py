"""資料統計 (``report_sections/data_stats``, #537): the ratio table and the
four 「正例組成」 tables — each period, item, segment and category — with the
same columns (ADR-0031 decision 16 item 6, and the entity count decision 1's
acceptance table adds). The totals are 核心概念's:
none is printed here, and the description no longer prints one run's
numbers whatever the data (decision 16 item 17).

The row facts are ``test_report_builder``'s, in the shape
``compute_row_facts`` lands; the category grain's overview is written in the
shape ``compute_dataset_overview`` gives it.
"""
from __future__ import annotations

import re

import pytest

from recsys_tfb.evaluation.report_sections import data_stats
from recsys_tfb.report.types import Paragraph, Table
from tests.test_evaluation import test_report_builder as rbt

_FIVE = [("候選列", "count"), ("候選列佔比", "ratio"), ("正例數", "count"),
         ("正例佔比", "ratio"), ("正例率", "ratio"), ("entity 數", "count")]


def _build(row_facts, params=None):
    return data_stats.build(rbt._results(evaluation_row_facts=row_facts),
                            params or rbt._params())


def _tables(section) -> dict[str, Table]:
    return {b.title: b for b in section.blocks if isinstance(b, Table)}


def _rows(table: Table) -> dict[str, tuple]:
    return {r.label: r.cells for r in table.rows}


def _description(section) -> str:
    (p,) = [b for b in section.blocks if isinstance(b, Paragraph)]
    assert p.muted
    return p.text


def _category_overview():
    """The category grain's overview: one row per (query group, category),
    label the max of the items folded in — so fewer positives than the
    item grain's 20."""
    return {
        "totals": {"n_rows": 40, "n_entities": 10, "n_items": 2,
                   "n_snap_dates": 1, "n_positives": 16,
                   "positive_rate": 0.4, "avg_positives_per_entity": 1.6},
        "by_snap_date": {"20240331": {"n_rows": 40, "n_positives": 16,
                                      "n_entities": 10, "positive_rate": 0.4}},
        "by_item": {"fund": {"n_rows": 20, "n_positives": 12,
                             "n_entities": 10, "positive_rate": 0.6},
                    "card": {"n_rows": 20, "n_positives": 4,
                             "n_entities": 10, "positive_rate": 0.2}},
    }


def test_the_tables_are_the_ratios_and_the_composition_by_each_slice():
    s = _build(rbt._row_facts())
    assert list(_tables(s)) == ["比例", "正例組成 · 各期", "正例組成 · 各 item",
                                "正例組成 · 各分群"]
    for title, header in (("正例組成 · 各期", "期"), ("正例組成 · 各 item", "item"),
                          ("正例組成 · 各分群", "分群")):
        table = _tables(s)[title]
        assert table.row_header == header
        assert [(c.label, c.kind) for c in table.columns] == _FIVE
    assert (s.key, s.title, s.title_en) == ("data_stats", "資料統計",
                                            "Data statistics")
    from recsys_tfb.evaluation.report import generate_html_report
    assert "<details" not in generate_html_report([s])  # nothing collapses


def test_each_slice_prints_its_landed_entity_count():
    """ADR-0031 decision 1, first row of the acceptance table: the entity
    count of each slice is a column of its 正例組成 table, read off the row
    facts (``compute_dataset_overview`` lands ``n_entities`` per slice)."""
    rf = rbt._row_facts()
    by_item = rf["dataset_overview"]["by_item"]
    for count, cell in zip((7, 3), by_item.values()):
        cell["n_entities"] = count
    table = _tables(_build(rf))["正例組成 · 各 item"]
    column = [c.label for c in table.columns].index("entity 數")
    assert [row.cells[column] for row in table.rows] == [
        cell["n_entities"] for cell in by_item.values()]


def test_the_ratio_table_reads_the_landed_rates_and_says_how():
    rf = rbt._row_facts()
    totals = rf["dataset_overview"]["totals"]
    totals.update(positive_rate=0.21, avg_positives_per_entity=2.5)
    table = _tables(_build(rf))["比例"]
    assert [(c.label, c.kind) for c in table.columns] == [
        ("值", "ratio"), ("怎麼算", "text")]
    assert _rows(table) == {
        "母體正例率": (0.21, "正例數 ÷ 候選列"),
        "每個 entity 平均正例數": (2.5, "正例數 ÷ entity 數"),
    }


def test_each_slice_s_counts_and_its_shares_of_the_totals():
    """Shares over the totals (100 rows, 20 positives): item A has 50 rows
    and 12 positives, so 0.5 and 0.6; its rate is the landed 0.24. Each
    share column adds up to 1."""
    tables = _tables(_build(rbt._row_facts()))
    assert _rows(tables["正例組成 · 各 item"]) == {
        "A": (50, 0.5, 12, 0.6, 0.24, 10),
        "B": (50, 0.5, 8, 0.4, 0.16, 10),
    }
    assert _rows(tables["正例組成 · 各分群"]) == {
        "X": (60, 0.6, 14, 0.7, pytest.approx(14 / 60), 6),
        "Y": (40, 0.4, 6, 0.3, 0.15, 4),
    }
    for title in ("正例組成 · 各期", "正例組成 · 各 item", "正例組成 · 各分群"):
        cells = [r.cells for r in tables[title].rows]
        assert sum(c[1] for c in cells) == pytest.approx(1.0), title
        assert sum(c[3] for c in cells) == pytest.approx(1.0), title


def test_the_periods_are_in_date_order_and_not_folded_away():
    """Today's report folded this table away; nothing folds now. Landed
    group-bys come in no order."""
    rf = rbt._row_facts()
    rf["dataset_overview"]["by_snap_date"] = {
        "20240430": {"n_rows": 30, "n_positives": 5, "n_entities": 10,
                     "positive_rate": 5 / 30},
        "20240331": {"n_rows": 70, "n_positives": 15, "n_entities": 10,
                     "positive_rate": 15 / 70},
    }
    s = _build(rf)
    rows = _rows(_tables(s)["正例組成 · 各期"])
    assert list(rows) == ["20240331", "20240430"]
    assert rows["20240331"][:4] == (70, 0.7, 15, 0.75)
    from recsys_tfb.evaluation.report import generate_html_report
    assert "<details" not in generate_html_report([s])  # nothing collapses


def test_the_category_table_divides_by_the_category_grain_s_own_totals():
    """40 category rows, 16 positives: fund's 20 and 12 are 0.5 and 0.75 —
    the item grain's 100 and 20 would give 0.2 and 0.6."""
    s = _build(rbt._row_facts_with_category(_category_overview()))
    table = _tables(s)["正例組成 · 各大類"]
    assert table.row_header == "大類"
    assert [(c.label, c.kind) for c in table.columns] == _FIVE
    assert _rows(table) == {"card": (20, 0.5, 4, 0.25, 0.2, 10),
                            "fund": (20, 0.5, 12, 0.75, 0.6, 10)}
    text = _description(s)
    assert "各大類" in text
    assert "佔比的分母用大類粒度自己的合計" in text
    assert "大類" not in _description(_build(rbt._row_facts()))


def test_no_totals_and_no_one_run_s_numbers():
    """Decision 16 item 12 and item 17: the count rows are 核心概念's, and
    the old description printed 「此 run 5,232」「此 run 每 item 654」 whatever
    the data. The description now holds no number at all."""
    s = _build(rbt._row_facts())
    text = _description(s)
    assert re.search(r"\d", text) is None, text
    assert "佔比的分母是核心概念的總量" in text
    # Facts without a ``segments`` block (an older run's) say nothing about
    # where a segment column came from.
    assert "分群欄" not in text and "(unmatched)" not in text
    labels = {r.label for t in _tables(s).values() for r in t.rows}
    for total in ("全部 query group", "候選列", "正例數", "item 數", "entity 數"):
        assert total not in labels


def test_the_unmatched_group_is_last_and_said_to_be_in_no_macro():
    """ADR-0020 bug 6: rows whose segment the population table could not
    supply are one visible row, after the real segments."""
    rf = rbt._row_facts()
    rf["dataset_overview"]["by_segment"]["(unmatched)"] = {
        "n_rows": 20, "n_positives": 2, "n_entities": 2,
        "positive_rate": 0.1, "n_queries": 2, "query_share": 2 / 12}
    rf["segments"] = {"joined": ["cust_segment_typ"],
                      "sources": {"cust_segment_typ": "sample_pool"},
                      "missing": {}}
    s = _build(rf)
    table = _tables(s)["正例組成 · 各分群"]
    assert [r.label for r in table.rows] == ["X", "Y", "(unmatched)"]
    (note,) = table.notes
    assert "(unmatched)" in note.text and "不進任何 Macro 平均" in note.text
    assert "分群欄 cust_segment_typ 取自 sample_pool。" in _description(s)


def test_a_population_table_without_the_segment_column_is_named():
    """No such column: no segment table, and the description says which
    table and which column, so a misspelt name is recognisable."""
    rf = rbt._row_facts()
    del rf["dataset_overview"]["by_segment"]
    rf["segments"] = {"joined": [], "sources": {},
                      "missing": {"cust_segment_typ": "inference_population"}}
    s = _build(rf)
    assert "正例組成 · 各分群" not in _tables(s)
    text = _description(s)
    assert "母體表 inference_population 無欄 cust_segment_typ" in text
    assert text.startswith("正例怎麼分布在各期、各 item。")


# ---- ADR-0025 decision 3: zero-positive groups kept by the dataset draw ----


def test_kept_zero_positive_groups_are_said_with_the_ratio_and_no_count():
    """The kept groups' count is a total (核心概念's 無正例的 query group), so
    only the ratio and its weight are said here."""
    rf = rbt._accounted({**rbt._row_facts(), "n_excluded_queries": 7})
    text = _description(_build(rf, rbt._kept_params(0.25)))
    assert "r＝0.25" in text and "1／r＝4" in text
    assert "沒有加權" in text and "不代表全部曝光" in text
    assert "7 個" not in text


@pytest.mark.parametrize("ratio, post_training", [(0.0, True), (0.25, False)])
def test_nothing_is_said_when_no_zero_positive_group_was_kept(ratio,
                                                              post_training):
    # ratio 0 kept none; monitoring never went through the dataset draw.
    text = _description(_build(rbt._row_facts(),
                               rbt._kept_params(ratio, post_training)))
    assert "test_zero_positive_group_ratio" not in text


def test_the_legacy_overview_is_refused():
    """#327: an overview with the old key names raises, naming the
    migration script, rather than drawing blanks."""
    rf = rbt._row_facts()
    rf["dataset_overview"]["totals"]["n_products"] = rf[
        "dataset_overview"]["totals"].pop("n_items")
    with pytest.raises(ValueError, match="predates the #327 key rename"):
        _build(rf)
