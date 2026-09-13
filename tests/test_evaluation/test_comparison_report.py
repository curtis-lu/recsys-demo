"""Tests for comparison.report — assemble_comparison_report (pure dict → HTML)."""

import pytest
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report


def _metrics(map_at_1: float = 0.5, hit_rate_at_3: float = 0.7) -> dict:
    """Minimal metrics dict (compute_all_metrics shape)."""
    return {
        "n_queries": 100, "n_excluded_queries": 0,
        "overall": {"map@1": map_at_1, "map@3": 0.6, "recall@3": 0.55},
        "per_item": {
            "p1": {"hit_rate@1": hit_rate_at_3, "hit_rate@3": 0.8,
                   "map_attr@1": 0.4, "map_attr@3": 0.5, "mean_pos": 1.5},
            "p2": {"hit_rate@1": 0.5, "hit_rate@3": 0.7,
                   "map_attr@1": 0.3, "map_attr@3": 0.4, "mean_pos": 2.0},
        },
        "macro_avg": {"by_item": {"hit_rate@1": 0.6, "hit_rate@3": 0.75,
                                  "map_attr@3": 0.45}},
        "dataset_overview": {"totals": {"n_items": 2}},
    }


def _comparison(a, b):
    from recsys_tfb.evaluation.compare import build_comparison_result
    return build_comparison_result(a, b, "Model", "ExtX")


def _params() -> dict:
    return {
        "evaluation": {
            "snap_date": "2026-01-31",
            "report": {
                "display": {
                    "primary_map_k": [1, 3, "all"],
                    "guardrail_recall_k": [1, 3],
                },
            },
            "item_categories": {"enabled": False},
        },
    }


def _coverage() -> dict:
    return {
        "n_query_group_A_full": 10000, "n_query_group_B_full": 5000,
        "n_query_group_common": 4800,
        "n_item_A_full": 22, "n_item_B_full": 18, "n_item_common": 12,
        "dropped_items_A": ["fund_misc", "ext_etc"],
        "dropped_items_B": ["ext_yet_another"],
        "kind_a": "model_version", "kind_b": "external_hive",
        "model_version_a": "2026-01-31_xxx_yyy",
        "table_b": "other_project.predictions",
    }


def test_returns_html_string():
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert isinstance(out, str)
    assert "<html" in out.lower()


def test_labels_visible_in_html():
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "Model" in out and "ExtX" in out


def test_coverage_numbers_in_html():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "4800" in out or "4,800" in out
    assert "10000" in out or "10,000" in out
    assert "fund_misc" in out  # dropped items listed


def test_overall_metrics_have_delta():
    m_a = _metrics(map_at_1=0.6)
    m_b = _metrics(map_at_1=0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    # Δ for map@1 = 0.2; rendered somewhere
    assert "0.2" in out or "+0.2" in out


def test_per_item_row_missing_on_one_side_has_blank_delta():
    """bug 4 (ADR-0020): an item only A has a positive for keeps its M cells
    and gets no Δ — not Δ = A's value."""
    import pandas as pd
    from recsys_tfb.evaluation.comparison.report import _build_per_item_section

    m_a, m_b = _metrics(), _metrics()
    del m_b["per_item"]["p2"]
    comp = _comparison(m_a, m_b)
    sec = _build_per_item_section(m_a, m_b, comp, _params())
    assert sec is not None
    recall = sec.tables[0]
    assert recall.loc["p2", "recall@1 M"] == pytest.approx(0.5)
    assert pd.isna(recall.loc["p2", "recall@1 B"])
    assert pd.isna(recall.loc["p2", "recall@1 Δ"])
    # both sides have p1 → Δ unchanged (0.7 − 0.7)
    assert recall.loc["p1", "recall@1 Δ"] == pytest.approx(0.0)


def test_macro_row_delta_blank_when_one_side_lacks_the_key():
    """bug 4 (ADR-0020), second copy of the same fallback: the Macro row's Δ
    is computed in the table builder, not in build_comparison_result, and it
    read a missing side as 0.0 too."""
    import pandas as pd
    from recsys_tfb.evaluation.comparison.report import _build_per_item_section

    def _macro_recall_at_1(m_b):
        m_a = _metrics()
        sec = _build_per_item_section(m_a, m_b, _comparison(m_a, m_b), _params())
        recall = sec.tables[0]  # 2 items → only recall@1 survives the K clamp
        return recall.loc[recall.index[0]]

    m_b = _metrics()
    m_b["macro_avg"]["by_item"]["hit_rate@1"] = 0.5
    both = _macro_recall_at_1(m_b)
    assert both["recall@1 Δ"] == pytest.approx(0.1)  # both sides → A − B

    del m_b["macro_avg"]["by_item"]["hit_rate@1"]
    one = _macro_recall_at_1(m_b)
    assert one["recall@1 M"] == pytest.approx(0.6)
    assert pd.isna(one["recall@1 Δ"])


def test_empty_overall_side_blanks_delta_column_and_says_why():
    """bug 4 (ADR-0020): every query on B had zero positives → B's overall is
    ``{}``. The Δ column is empty and a line above the table names the side."""
    import pandas as pd
    from recsys_tfb.evaluation.comparison.report import _build_overall_section

    m_a, m_b = _metrics(0.6), _metrics(0.4)
    m_b["overall"] = {}
    sec = _build_overall_section(m_a, _comparison(m_a, m_b))
    tbl = sec.tables[0]
    assert len(tbl.index) > 0, "no rows — the Δ column check would pass vacuously"
    assert tbl["Δ"].isna().all()
    assert "ExtX 側無可比的 query（全部零正例）" in sec.table_titles[0]
    assert "Model 側" not in sec.table_titles[0]


def test_overall_title_has_no_empty_side_note_when_both_sides_have_metrics():
    from recsys_tfb.evaluation.comparison.report import _build_overall_section

    m_a, m_b = _metrics(0.6), _metrics(0.4)
    sec = _build_overall_section(m_a, _comparison(m_a, m_b))
    assert "無可比的 query" not in sec.table_titles[0]
    assert sec.tables[0].loc["map@1", "Δ"] == pytest.approx(0.2)


def test_category_section_absent_when_disabled():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "大類" not in out  # category section is disabled in _params()


def test_category_section_present_when_enabled_and_present():
    m_a = _metrics(); m_b = _metrics()
    cat_metrics = {
        "overall": {"map@1": 0.5, "map@3": 0.55},
        "per_item": {"fund": {"hit_rate@1": 0.6, "hit_rate@3": 0.7,
                              "map_attr@1": 0.4, "map_attr@3": 0.5}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.6}},
        "dataset_overview": {"totals": {"n_items": 1}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["item_categories"]["enabled"] = True
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), p)
    assert "大類" in out


def test_category_section_columns_clamp_to_category_count():
    """bug 8 (ADR-0020): 3 categories, primary_map_k=[1,3,5,"all"] -> the
    大類 per-item map_attr@k table must not show @5 (5 > n_cat=3)."""
    from recsys_tfb.evaluation.comparison.report import _build_category_section

    m_a, m_b = _metrics(), _metrics()
    cat_metrics = {
        "overall": {"map@1": 0.4},
        "per_item": {
            "fund": {"hit_rate@1": 0.5, "map_attr@1": 0.4, "map_attr@3": 0.5,
                     "map_attr@5": 0.5, "mean_pos": 1.0},
            "exchange": {"hit_rate@1": 0.4, "map_attr@1": 0.3, "map_attr@3": 0.4,
                        "map_attr@5": 0.4, "mean_pos": 1.5},
            "ccard": {"hit_rate@1": 0.3, "map_attr@1": 0.2, "map_attr@3": 0.3,
                     "map_attr@5": 0.3, "mean_pos": 2.0},
        },
        "macro_avg": {"by_item": {"hit_rate@1": 0.4, "map_attr@3": 0.4}},
        "dataset_overview": {"totals": {"n_items": 3}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["item_categories"]["enabled"] = True
    p["evaluation"]["report"]["display"]["primary_map_k"] = [1, 3, 5, "all"]
    sec = _build_category_section(m_a, m_b, p)
    assert sec is not None
    tbl = sec.tables[sec.table_titles.index(
        next(t for t in sec.table_titles if t.startswith(
            "大類 per-item map_attr@k (M/B/Δ)"
        ))
    )]
    cols = [str(c) for c in tbl.columns]
    assert not any(c.startswith("map_attr@5") for c in cols)
    assert any(c.startswith("map_attr@3") for c in cols)


def test_per_item_section_discloses_macro_item_coverage():
    """bug 5 (ADR-0020): comparison report's per-item M/B/Δ titles state each
    side's macro item coverage, same as the main report."""
    from recsys_tfb.evaluation.comparison.report import _build_per_item_section

    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    sec = _build_per_item_section(m_a, m_b, comp, _params())
    assert sec is not None
    for title in sec.table_titles:
        assert "參與 macro 的 item 數" in title
        assert "M 2／B 2／全部 2）" in title  # both sides: p1,p2 in per_item, n_items=2


def test_category_section_discloses_macro_item_coverage():
    from recsys_tfb.evaluation.comparison.report import _build_category_section

    m_a, m_b = _metrics(), _metrics()
    cat_metrics = {
        "overall": {"map@1": 0.5, "map@3": 0.55},
        "per_item": {"fund": {"hit_rate@1": 0.6, "hit_rate@3": 0.7,
                              "map_attr@1": 0.4, "map_attr@3": 0.5}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.6}},
        "dataset_overview": {"totals": {"n_items": 1}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["item_categories"]["enabled"] = True
    sec = _build_category_section(m_a, m_b, p)
    assert sec is not None
    per_item_titles = [t for t in sec.table_titles if t != "大類 overall"]
    assert per_item_titles
    for title in per_item_titles:
        assert "M 1／B 1／全部 1）" in title


def test_category_overall_title_names_the_empty_side():
    """bug 4 (ADR-0020): the category overall table gets the same empty-side
    line as the fine-grained overall table. B's category overall is ``{}``
    (every query zero positives at category grain), so its Δ column is blank
    and the title says which side has nothing to compare."""
    from recsys_tfb.evaluation.comparison.report import _build_category_section

    m_a, m_b = _metrics(), _metrics()
    cat_a = {
        "overall": {"map@1": 0.5},
        "per_item": {"fund": {"hit_rate@1": 0.6, "map_attr@1": 0.4}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.6}},
        "dataset_overview": {"totals": {"n_items": 1}},
    }
    m_a["category"] = cat_a
    m_b["category"] = {**cat_a, "overall": {}}
    p = _params()
    p["evaluation"]["item_categories"]["enabled"] = True
    sec = _build_category_section(m_a, m_b, p)
    assert sec is not None, "section returned None — nothing was checked"
    title = next(t for t in sec.table_titles if t.startswith("大類 overall"))
    assert "Compare 側無可比的 query（全部零正例）" in title
    assert "Model 側" not in title
    tbl = sec.tables[sec.table_titles.index(title)]
    assert len(tbl.index) > 0, "no rows — the Δ column check would pass vacuously"
    assert tbl["Δ"].isna().all()


def _overall_up_to_k5() -> dict:
    return {
        f"{fam}@{k}": round(0.1 * k, 2)
        for fam in ("map", "precision", "recall") for k in (1, 2, 3, 4, 5)
    }


def test_category_overall_table_drops_keys_with_k_above_category_count():
    """bug 8 (ADR-0020): the category overall table lists every computed key,
    so with 3 categories it printed precision@4 / @5 etc. — K beyond the item
    count, where precision's denominator is K itself. K <= 3 stays."""
    from recsys_tfb.evaluation.comparison.report import _build_category_section

    m_a, m_b = _metrics(), _metrics()
    cat_metrics = {
        "overall": _overall_up_to_k5(),
        "per_item": {
            c: {"hit_rate@1": 0.5, "map_attr@1": 0.4}
            for c in ("fund", "exchange", "ccard")
        },
        "macro_avg": {"by_item": {"hit_rate@1": 0.5}},
        "dataset_overview": {"totals": {"n_items": 3}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["item_categories"]["enabled"] = True
    sec = _build_category_section(m_a, m_b, p)
    assert sec is not None
    overall = sec.tables[sec.table_titles.index("大類 overall")]
    idx = [str(i) for i in overall.index]
    assert "precision@4" not in idx
    assert [i for i in idx if i.endswith(("@4", "@5"))] == []
    assert {"map@3", "precision@3", "recall@3"} <= set(idx)


def test_overall_table_drops_keys_with_k_above_item_count():
    """The fine-grained overall table gets the same filter against its own
    n_items: 8 items keep every key up to @5; 3 items drop @4 / @5."""
    from recsys_tfb.evaluation.comparison.report import _build_overall_section

    full = _overall_up_to_k5()
    for n_items, expected in (
        (8, sorted(full)),
        (3, sorted(k for k in full if not k.endswith(("@4", "@5")))),
    ):
        m_a, m_b = _metrics(0.6), _metrics(0.4)
        for m in (m_a, m_b):
            m["overall"] = dict(full)
            m["dataset_overview"]["totals"]["n_items"] = n_items
        sec = _build_overall_section(m_a, _comparison(m_a, m_b))
        idx = [str(i) for i in sec.tables[0].index]
        assert sorted(idx) == expected, n_items


def test_glossary_section_present():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "詞彙" in out or "Glossary" in out


