"""Tests for comparison.report — assemble_comparison_report (pure dict → HTML)."""

import pytest
from recsys_tfb.evaluation.comparison.report import assemble_comparison_report


def _metrics(map_at_1: float = 0.5, hit_rate_at_3: float = 0.7) -> dict:
    """Minimal metrics dict (compute_all_metrics shape)."""
    return {
        "n_queries": 100, "n_excluded_queries": 0,
        "overall": {"map@1": map_at_1, "map@3": 0.6, "ndcg@3": 0.65, "recall@3": 0.55},
        "per_item": {
            "p1": {"hit_rate@1": hit_rate_at_3, "hit_rate@3": 0.8,
                   "map_attr@1": 0.4, "map_attr@3": 0.5,
                   "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.55, "mean_pos": 1.5},
            "p2": {"hit_rate@1": 0.5, "hit_rate@3": 0.7,
                   "map_attr@1": 0.3, "map_attr@3": 0.4,
                   "ndcg_attr@1": 0.35, "ndcg_attr@3": 0.45, "mean_pos": 2.0},
        },
        "macro_avg": {"by_item": {"hit_rate@1": 0.6, "hit_rate@3": 0.75,
                                  "map_attr@3": 0.45, "ndcg_attr@3": 0.5}},
        "dataset_overview": {"totals": {"n_products": 2}},
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
            "product_categories": {"enabled": False},
        },
    }


def _coverage() -> dict:
    return {
        "n_cust_A_full": 10000, "n_cust_B_full": 5000, "n_cust_common": 4800,
        "n_prod_A_full": 22, "n_prod_B_full": 18, "n_prod_common": 12,
        "dropped_prods_A": ["fund_misc", "ext_etc"],
        "dropped_prods_B": ["ext_yet_another"],
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
    assert "fund_misc" in out  # dropped prods listed


def test_overall_metrics_have_delta():
    m_a = _metrics(map_at_1=0.6)
    m_b = _metrics(map_at_1=0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    # Δ for map@1 = 0.2; rendered somewhere
    assert "0.2" in out or "+0.2" in out


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
                              "map_attr@1": 0.4, "map_attr@3": 0.5,
                              "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.5}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.6}},
        "dataset_overview": {"totals": {"n_products": 1}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["product_categories"]["enabled"] = True
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), p)
    assert "大類" in out


def test_glossary_section_present():
    m_a, m_b = _metrics(), _metrics()
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "詞彙" in out or "Glossary" in out


def test_overall_section_hides_ndcg():
    """_build_overall_section 用 set union 把 metrics key 攤成列（key-agnostic），
    程式碼裡沒有 "ndcg" 字樣也會渲染出來。fixture 帶 ndcg@3，必須被濾掉。"""
    from recsys_tfb.evaluation.comparison.report import _build_overall_section
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    comp = _comparison(m_a, m_b)
    sec = _build_overall_section(comp)
    idx = [str(i) for i in sec.tables[0].index]
    assert "map@3" in idx, "非 ndcg 的列不該被誤濾"
    assert not [i for i in idx if i.startswith("ndcg")]


def test_category_overall_section_hides_ndcg():
    """大類 overall 表是第二個 key-agnostic 洩漏點（與 _build_overall_section
    不同函式）。fixture 必須讓 product_categories.enabled=True 且 metrics 帶
    category.overall 含 ndcg，否則該段落回 None → 假綠。"""
    from recsys_tfb.evaluation.comparison.report import _build_category_section
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    cat_metrics = {
        "overall": {"map@1": 0.7, "map@3": 0.75, "ndcg@3": 0.8, "recall@3": 0.6},
        "per_item": {"fund": {"hit_rate@1": 0.6, "hit_rate@3": 0.7,
                              "map_attr@1": 0.4, "map_attr@3": 0.5,
                              "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.5}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.6}},
        "dataset_overview": {"totals": {"n_products": 1}},
    }
    m_a["category"] = cat_metrics
    m_b["category"] = cat_metrics
    p = _params()
    p["evaluation"]["product_categories"]["enabled"] = True
    sec = _build_category_section(m_a, m_b, p)
    assert sec is not None, "段落回 None 就什麼都沒測到（假綠）"
    overall = sec.tables[sec.table_titles.index("大類 overall")]
    idx = [str(i) for i in overall.index]
    assert "map@3" in idx, "非 ndcg 的列不該被誤濾"
    assert not [i for i in idx if i.startswith("ndcg")]


def test_comparison_html_has_no_ndcg():
    """端到端：report_comparison.html 整份不得出現 ndcg（含 glossary）。"""
    m_a, m_b = _metrics(0.6), _metrics(0.4)
    comp = _comparison(m_a, m_b)
    out = assemble_comparison_report(m_a, m_b, comp, _coverage(), _params())
    assert "ndcg" not in out.lower()
