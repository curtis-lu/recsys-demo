"""Pure-dict tests for report_builder section functions (no Spark)."""

import pytest

from recsys_tfb.evaluation import report_builder as rb


def _params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"report": {"display": {
            "primary_map_k": [1, 3, "all"],
            "guardrail_recall_k": [1, 2]}}}}


def _metrics():
    return {
        "overall": {"map@1": 0.5, "map@3": 0.6, "map@5": 0.65,
                    "map@10": 0.7, "precision@1": 0.4, "ndcg@1": 0.55,
                    "recall@1": 0.3},
        "per_item": {"A": {"hit_rate@1": 0.2, "hit_rate@2": 0.4,
                           "mean_pos": 3.0,
                           "map_attr@1": 0.5, "map_attr@3": 0.6,
                           "map_attr@2": 0.55,
                           "ndcg_attr@1": 0.45, "ndcg_attr@3": 0.5,
                           "ndcg_attr@2": 0.48},
                     "B": {"hit_rate@1": 0.1, "hit_rate@2": 0.3,
                           "mean_pos": 5.0,
                           "map_attr@1": 0.3, "map_attr@3": 0.35,
                           "map_attr@2": 0.32,
                           "ndcg_attr@1": 0.25, "ndcg_attr@3": 0.3,
                           "ndcg_attr@2": 0.28}},
        "dataset_overview": {
            "totals": {"n_rows": 100, "n_customers": 10, "n_products": 2,
                       "n_snap_dates": 1, "n_positives": 20,
                       "positive_rate": 0.2,
                       "avg_positives_per_customer": 2.0},
            "by_snap_date": {"20240331": {"n_rows": 100, "n_positives": 20,
                                          "n_customers": 10,
                                          "positive_rate": 0.2}},
            "by_item": {"A": {"n_rows": 50, "n_positives": 12,
                              "n_customers": 10, "positive_rate": 0.24},
                        "B": {"n_rows": 50, "n_positives": 8,
                              "n_customers": 10, "positive_rate": 0.16}}},
        "macro_avg": {
            "by_item": {
                "hit_rate@1": 0.15, "hit_rate@2": 0.35, "mean_pos": 4.0,
                "map_attr@1": 0.4, "map_attr@2": 0.435, "map_attr@3": 0.475,
                "ndcg_attr@1": 0.35, "ndcg_attr@2": 0.38, "ndcg_attr@3": 0.4,
            },
        },
        "n_queries": 10, "n_excluded_queries": 0,
    }


def test_headline_section_has_map_card():
    s = rb.build_headline_section(_metrics(), _params())
    txt = " ".join(str(t.to_dict()) for t in s.tables)
    assert "map@1" in txt and "map@all" in txt   # "all" resolves via display
    assert "map@5" not in txt                     # not in display list


def test_dataset_overview_section_tables():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    assert len(s.tables) == 3   # totals / by_snap_date / by_item
    assert s.title


class TestVisibleMetricKeys:
    def test_drops_ndcg_keys(self):
        keys = ["map@3", "ndcg@3", "precision@3", "recall@3", "ndcg@all"]
        assert rb._visible_metric_keys(keys) == [
            "map@3", "precision@3", "recall@3"
        ]

    def test_preserves_input_order(self):
        assert rb._visible_metric_keys(["recall@1", "ndcg@1", "map@1"]) == [
            "recall@1", "map@1"
        ]

    def test_does_not_drop_unrelated_keys_that_merely_contain_ndcg(self):
        """只濾「以 prefix 起頭」的 key，不是子字串比對。"""
        assert rb._visible_metric_keys(["my_ndcg@1"]) == ["my_ndcg@1"]


def test_segment_section_hides_ndcg():
    # per_segment 的每個 seg dict 的 key 會被直接攤成表格的欄（key-agnostic），
    # 程式碼裡沒有 "ndcg" 字樣也會渲染出來。fixture 刻意帶 ndcg@1，否則假綠。
    m = _metrics()
    m["per_segment"] = {
        "young": {"map@1": 0.6, "ndcg@1": 0.55, "recall@1": 0.3},
        "old": {"map@1": 0.4, "ndcg@1": 0.35, "recall@1": 0.2},
    }
    s = rb.build_segment_section(m, _params())
    cols = [str(c) for c in s.tables[0].columns]
    assert "map@1" in cols, "非 ndcg 的欄不該被誤濾"
    assert not [c for c in cols if c.startswith("ndcg")]


def test_baseline_overall_table_hides_ndcg():
    # _metrics()["overall"] 與 _baseline_metrics_full()["overall"] 都含 ndcg@1；
    # overall 表用 set union 攤成列，ndcg@1 必須被濾掉。
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    overall = s.tables[s.table_titles.index("overall metrics")]
    idx = [str(i) for i in overall.index]
    assert "map@1" in idx, "非 ndcg 的列不該被誤濾"
    assert not [i for i in idx if i.startswith("ndcg")]


def test_primary_map_section_slices_k():
    s = rb.build_primary_map_section(_metrics(), _params())
    # families on the row index, @k slices as columns (one set shared by all)
    assert set(s.tables[0].index) == {"map", "precision", "recall"}
    cols = " ".join(map(str, s.tables[0].columns))
    assert "@1" in cols and "@3" in cols


def test_guardrail_section_renames_hitrate_and_has_heatmap():
    s = rb.build_guardrail_recall_section(_metrics(), _params())
    cols = " ".join(map(str, s.tables[0].columns))
    assert "recall@1 (per-item)" in cols
    assert "hit_rate" not in cols
    assert len(s.figures) == 1            # plotly heatmap


def test_category_section_none_when_absent():
    assert rb.build_category_section(_metrics(), _params()) is None


def test_category_section_present_when_category_key():
    m = _metrics()
    m["category"] = {"overall": {"map@1": 0.7},
                     "per_item": {"fund": {"hit_rate@1": 0.5,
                                           "mean_pos": 2.0}},
                     "dataset_overview": m["dataset_overview"]}
    s = rb.build_category_section(m, _params())
    assert s is not None and s.tables


def test_glossary_section_always_built():
    s = rb.build_glossary_section(_params())
    assert "recall@k (per-item)" in " ".join(
        map(str, s.tables[0].to_dict().values()))


def test_assemble_report_is_html():
    html = rb.assemble_report(_metrics(), _params())
    assert html.startswith("<!DOCTYPE html>")
    assert "摘要 Headline" in html


def test_primary_map_orientation_locked():
    s = rb.build_primary_map_section(_metrics(), _params())
    assert "map" in s.tables[0].index   # families are the row index


def _baseline_metrics_full():
    """Baseline metrics dict mirroring _metrics() per_item / macro shape."""
    return {
        "overall": {"map@1": 0.4, "map@3": 0.5, "ndcg@1": 0.45,
                    "precision@1": 0.3, "recall@1": 0.25},
        "per_item": {
            "A": {"hit_rate@1": 0.15, "hit_rate@2": 0.30, "mean_pos": 3.5,
                  "map_attr@1": 0.40, "map_attr@2": 0.45, "map_attr@3": 0.50,
                  "ndcg_attr@1": 0.35, "ndcg_attr@2": 0.40, "ndcg_attr@3": 0.42},
            "B": {"hit_rate@1": 0.08, "hit_rate@2": 0.20, "mean_pos": 5.5,
                  "map_attr@1": 0.25, "map_attr@2": 0.28, "map_attr@3": 0.30,
                  "ndcg_attr@1": 0.20, "ndcg_attr@2": 0.22, "ndcg_attr@3": 0.25}},
        "macro_avg": {"by_item": {
            "hit_rate@1": 0.115, "hit_rate@2": 0.25, "mean_pos": 4.5,
            "map_attr@1": 0.325, "map_attr@2": 0.365, "map_attr@3": 0.40,
            "ndcg_attr@1": 0.275, "ndcg_attr@2": 0.31, "ndcg_attr@3": 0.335,
        }},
    }


def test_assemble_metadata_has_model_version_and_generated_at():
    p = _params()
    p["model_version"] = "v_test"
    html = rb.assemble_report(_metrics(), p)
    assert "v_test" in html
    assert "Generated At" in html


def test_dataset_overview_adds_by_category_when_present():
    m = _metrics()
    m["category"] = {"dataset_overview": {"by_item": {
        "fund": {"n_rows": 10, "n_positives": 3, "n_customers": 5,
                 "positive_rate": 0.3}}}}
    s = rb.build_dataset_overview_section(m, _params())
    assert "by 大類" in s.table_titles


def test_category_section_has_composition_table():
    m = _metrics()
    m["category"] = {"overall": {"map@1": 0.7},
                     "per_item": {"fund": {"hit_rate@1": 0.5,
                                           "mean_pos": 2.0}},
                     "dataset_overview": m["dataset_overview"]}
    p = _params()
    p["evaluation"]["product_categories"] = {
        "mapping": {"fund": ["fund_stock", "fund_bond"]}}
    s = rb.build_category_section(m, p)
    assert "大類組成" in s.table_titles
    joined = " ".join(str(t.to_dict()) for t in s.tables)
    assert "fund_stock" in joined


def test_baseline_section_no_per_item_delta_omits_table():
    m = _metrics()
    base = {"overall": {"map@1": 0.4}}          # no per_item -> per_item_delta empty
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert s.table_titles == ["overall metrics"]
    assert len(s.tables) == 1


def test_category_section_omits_composition_when_no_mapping():
    m = _metrics()
    m["category"] = {"overall": {"map@1": 0.7},
                     "per_item": {"fund": {"hit_rate@1": 0.5,
                                           "mean_pos": 2.0}},
                     "dataset_overview": m["dataset_overview"]}
    p = _params()  # _params() has no product_categories.mapping
    s = rb.build_category_section(m, p)
    assert "大類組成" not in s.table_titles


def test_per_item_attr_section_built():
    s = rb.build_per_item_attr_section(_metrics(), _params())
    assert s is not None
    assert len(s.tables) == 1 and len(s.figures) == 1
    map_tbl = s.tables[0]
    assert set(map_tbl.index) == {"Macro 平均", "A", "B"}
    cols = " ".join(map(str, map_tbl.columns))
    assert "map_attr@1" in cols and "map_attr@3" in cols
    # ndcg 仍由 metrics_spark 算出(fixture 帶 ndcg_attr@1)，但刻意不呈現
    assert "ndcg" not in cols


def test_per_item_attr_section_off():
    p = _params()
    p["evaluation"]["report"].setdefault("sections", {})["per_item_attr"] = False
    assert rb.build_per_item_attr_section(_metrics(), p) is None


def test_per_item_attr_heatmap_autoscale():
    s = rb.build_per_item_attr_section(_metrics(), _params())
    for fig in s.figures:
        hm = fig.data[0]
        assert hm.zmin is None and hm.zmax is None


def test_glossary_has_attr_entries():
    s = rb.build_glossary_section(_params())
    terms = set(s.tables[0]["指標"])
    assert "map_attr@k" in terms
    # ndcg 兩條已退場——glossary 與 report_comparison.html 共用同一份 _GLOSSARY
    assert "ndcg@k" not in terms
    assert "ndcg_attr@k" not in terms


def test_guardrail_section_has_macro_row():
    s = rb.build_guardrail_recall_section(_metrics(), _params())
    table = s.tables[0]
    # Top row is the macro average
    assert list(table.index)[0] == "Macro 平均"
    # Value is the equal-weight per-product average: hit_rate@1 → recall@1 (per-item) column
    assert table.loc["Macro 平均", "recall@1 (per-item)"] == 0.15
    assert table.loc["Macro 平均", "mean_pos"] == 4.0
    # heatmap excludes the macro row
    assert "Macro 平均" not in list(s.figures[0].data[0].y)


def test_guardrail_section_no_macro_when_absent():
    m = _metrics()
    del m["macro_avg"]
    s = rb.build_guardrail_recall_section(m, _params())
    assert "Macro 平均" not in list(s.tables[0].index)


def test_per_item_attr_section_has_macro_rows():
    s = rb.build_per_item_attr_section(_metrics(), _params())
    map_tbl = s.tables[0]
    assert list(map_tbl.index)[0] == "Macro 平均"
    # map_attr@1 per-product average = (0.5 + 0.3) / 2 = 0.4
    assert map_tbl.loc["Macro 平均", "map_attr@1"] == 0.4
    # heatmap excludes the macro row
    assert "Macro 平均" not in list(s.figures[0].data[0].y)


def test_segment_section_has_macro_row():
    m = _metrics()
    m["per_segment"] = {
        "young": {"map@1": 0.6, "ndcg@1": 0.7},
        "old": {"map@1": 0.4, "ndcg@1": 0.5},
    }
    m["macro_avg"]["by_segment"] = {"map@1": 0.5, "ndcg@1": 0.6}
    s = rb.build_segment_section(m, _params())
    assert list(s.tables[0].index)[0] == "Macro 平均"
    assert s.tables[0].loc["Macro 平均", "map@1"] == 0.5


def test_segment_section_no_macro_when_absent():
    m = _metrics()
    m["per_segment"] = {"young": {"map@1": 0.6}}
    # macro_avg has no by_segment key
    s = rb.build_segment_section(m, _params())
    assert "Macro 平均" not in list(s.tables[0].index)


def test_category_section_recall_table_has_macro_row():
    m = _metrics()
    m["category"] = {
        "overall": {"map@1": 0.7},
        "per_item": {
            "fund": {"hit_rate@1": 0.5, "hit_rate@2": 0.6, "mean_pos": 2.0},
            "loan": {"hit_rate@1": 0.3, "hit_rate@2": 0.4, "mean_pos": 4.0},
        },
        "macro_avg": {
            "by_item": {
                "hit_rate@1": 0.4, "hit_rate@2": 0.5, "mean_pos": 3.0,
            },
        },
        "dataset_overview": m["dataset_overview"],
    }
    s = rb.build_category_section(m, _params())
    # tables[1] is the category-level per-item recall@k table
    rec_tbl = s.tables[1]
    assert list(rec_tbl.index)[0] == "Macro 平均"
    assert rec_tbl.loc["Macro 平均", "recall@1 (per-item)"] == 0.4


def test_glossary_has_macro_average_entry():
    s = rb.build_glossary_section(_params())
    terms = list(s.tables[0]["指標"])
    assert "Macro 平均" in terms


def test_baseline_section_renders_popularity_table():
    """purchase_counts -> popularity composition table prepended."""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.4},
        "per_item": {"A": {"hit_rate@1": 0.1}},
        "purchase_counts": {"A": 50, "B": 200, "C": 10},
    }
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "popularity 排名組成" in s.table_titles
    idx = s.table_titles.index("popularity 排名組成")
    tbl = s.tables[idx]
    # Sorted desc by count, with rank starting at 1.
    assert list(tbl.columns) == ["count", "rank"]
    assert list(tbl.index) == ["B", "A", "C"]
    assert list(tbl["count"]) == [200, 50, 10]
    assert list(tbl["rank"]) == [1, 2, 3]


def test_baseline_section_omits_popularity_when_purchase_counts_absent():
    """Backward compat: no purchase_counts -> no popularity table, others stay."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4},
            "per_item": {"A": {"hit_rate@1": 0.1}}}
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "popularity 排名組成" not in s.table_titles


def test_baseline_section_omits_popularity_when_purchase_counts_empty():
    """Empty purchase_counts dict -> no popularity table."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4},
            "per_item": {"A": {"hit_rate@1": 0.1}},
            "purchase_counts": {}}
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "popularity 排名組成" not in s.table_titles


def test_baseline_section_overall_table_has_model_baseline_delta_cols():
    """overall table: rows = metric keys, cols = [Model, Baseline, Delta]."""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.40, "ndcg@1": 0.50},
        "per_item": {"A": {"hit_rate@1": 0.1}},
    }
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    assert "overall metrics" in s.table_titles
    idx = s.table_titles.index("overall metrics")
    tbl = s.tables[idx]
    assert list(tbl.columns) == ["Model", "Baseline", "Delta"]
    # Model fixture has overall["map@1"]=0.5, ndcg@1=0.55 (see _metrics()).
    assert tbl.loc["map@1", "Model"] == 0.5
    assert tbl.loc["map@1", "Baseline"] == 0.40
    assert abs(tbl.loc["map@1", "Delta"] - (0.5 - 0.40)) < 1e-9


def test_baseline_section_overall_table_includes_keys_unique_to_one_side():
    """Keys only in Model OR Baseline still appear, missing side as NaN."""
    m = _metrics()  # has 'precision@1', 'recall@1'
    base = {"overall": {"map@1": 0.4, "extra_key@1": 0.9}}  # no precision/recall
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("overall metrics")
    tbl = s.tables[idx]
    assert "extra_key@1" in tbl.index   # baseline-only key still listed
    assert "precision@1" in tbl.index   # model-only key still listed


def test_baseline_section_has_two_per_item_compare_tables():
    """recall / map_attr each get a M/B/Δ-interleaved table (ndcg 不呈現)。"""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    # Old delta-only title must be gone.
    assert "per-item recall@k delta" not in s.table_titles
    # Two new titles present.
    for title in (
        "per-item recall@k (M/B/Δ)",
        "per-item map_attr@k (M/B/Δ)",
    ):
        assert title in s.table_titles
    assert "per-item ndcg_attr@k (M/B/Δ)" not in s.table_titles


def test_baseline_section_per_item_recall_table_three_cols_per_k():
    """recall table: cols = recall@1 M/B/Δ, recall@2 M/B/Δ (params has guardrail_recall_k=[1,2])."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("per-item recall@k (M/B/Δ)")
    tbl = s.tables[idx]
    assert list(tbl.columns) == [
        "recall@1 M", "recall@1 B", "recall@1 Δ",
        "recall@2 M", "recall@2 B", "recall@2 Δ",
    ]
    # Macro row first.
    assert list(tbl.index)[0] == "Macro 平均"
    # Spot-check A: Model hit_rate@1=0.2, Baseline=0.15, Δ from per_item_delta.
    assert tbl.loc["A", "recall@1 M"] == 0.2
    assert tbl.loc["A", "recall@1 B"] == 0.15
    assert abs(tbl.loc["A", "recall@1 Δ"] - (0.2 - 0.15)) < 1e-9
    # Macro row Δ from macro_a − macro_b.
    assert abs(
        tbl.loc["Macro 平均", "recall@1 Δ"] - (0.15 - 0.115)
    ) < 1e-9


def test_baseline_section_per_item_attr_tables_use_primary_map_k():
    """map_attr / ndcg_attr cols come from primary_map_k = [1, 3, 'all'];
    'all' resolves to n_products (=2 in fixture) for lookup."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("per-item map_attr@k (M/B/Δ)")
    tbl = s.tables[idx]
    assert list(tbl.columns) == [
        "map_attr@1 M", "map_attr@1 B", "map_attr@1 Δ",
        "map_attr@3 M", "map_attr@3 B", "map_attr@3 Δ",
        "map_attr@all M", "map_attr@all B", "map_attr@all Δ",
    ]
    # n_prod=2 means @all → lookup @2. Model A map_attr@2=0.55, Base=0.45.
    assert tbl.loc["A", "map_attr@all M"] == 0.55
    assert tbl.loc["A", "map_attr@all B"] == 0.45


def test_baseline_section_omits_per_item_compare_when_no_baseline_per_item():
    """No baseline per_item -> per-item compare tables skipped (overall stays)."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4}}  # no per_item
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    for title in (
        "per-item recall@k (M/B/Δ)",
        "per-item map_attr@k (M/B/Δ)",
        "per-item ndcg_attr@k (M/B/Δ)",
    ):
        assert title not in s.table_titles


def _metrics_min():
    return {
        "overall": {"map@2": 0.8, "precision@2": 0.5,
                    "ndcg@2": 0.9, "recall@2": 1.0},
        "per_item": {
            "A": {"map_attr@2": 0.75, "ndcg_attr@2": 0.8,
                  "hit_rate@2": 1.0, "mean_pos": 1.5, "n_pos": 2},
            "B": {"map_attr@2": 1.0, "ndcg_attr@2": 1.0,
                  "hit_rate@2": 1.0, "mean_pos": 1.0, "n_pos": 1},
        },
        "macro_avg": {"by_item": {"map_attr@2": 0.875, "ndcg_attr@2": 0.9,
                                  "hit_rate@2": 1.0, "mean_pos": 1.25}},
        "observation_items": [],
        "n_queries": 3,
        "n_excluded_queries": 0,
        "dataset_overview": {"totals": {"n_products": 2}},
    }


_CI_FIXTURE = {
    "enabled": True, "n_boot": 50, "k": None, "seed": 42,
    "metric_params": {"weight_alpha": 0.0, "min_positives": 0,
                      "shrinkage_k": 0.0},
    "per_item": {
        "A": {"ap": 0.74, "ci_low": 0.60, "ci_high": 0.90, "n_pos": 2},
        "B": {"ap": 1.0, "ci_low": 1.0, "ci_high": 1.0, "n_pos": 1},
    },
    "macro": {"ap": 0.87, "ci_low": 0.80, "ci_high": 0.95},
    "sample": {"n_queries_sampled": 3, "n_pos_queries_total": 3},
}


def _params_min():
    return {"evaluation": {"report": {"display": {"primary_map_k": [2]}}}}


def test_per_item_attr_ci_columns_present_when_metric_ci_given():
    from recsys_tfb.evaluation.report_builder import build_per_item_attr_section
    sec = build_per_item_attr_section(
        _metrics_min(), _params_min(), metric_ci=_CI_FIXTURE
    )
    map_tbl = sec.tables[0]
    for col in ["AP(抽樣)", "CI 2.5%", "CI 97.5%", "n_pos(抽樣)"]:
        assert col in map_tbl.columns
    assert map_tbl.loc["A", "CI 2.5%"] == 0.60
    assert map_tbl.loc["Macro 平均", "AP(抽樣)"] == 0.87
    assert "抽樣" in sec.description and "50" in sec.description


def test_per_item_attr_no_ci_columns_when_absent():
    from recsys_tfb.evaluation.report_builder import build_per_item_attr_section
    sec = build_per_item_attr_section(_metrics_min(), _params_min())
    assert "AP(抽樣)" not in sec.tables[0].columns


def test_per_item_attr_observation_list_table():
    from recsys_tfb.evaluation.report_builder import build_per_item_attr_section
    metrics = _metrics_min()
    metrics["observation_items"] = ["B"]
    sec = build_per_item_attr_section(metrics, _params_min())
    assert "觀察名單" in sec.table_titles[-1]
    obs_tbl = sec.tables[-1]
    assert list(obs_tbl.index) == ["B"]
    assert obs_tbl.loc["B", "n_pos"] == 1


def test_primary_map_macro_ci_table():
    from recsys_tfb.evaluation.report_builder import build_primary_map_section
    sec = build_primary_map_section(
        _metrics_min(), _params_min(), metric_ci=_CI_FIXTURE
    )
    assert any("CI" in t for t in sec.table_titles)
    ci_tbl = sec.tables[-1]
    assert ci_tbl.loc["macro per-item mAP", "CI 97.5%"] == 0.95


def test_assemble_report_passes_metric_ci_through():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), metric_ci=_CI_FIXTURE
    )
    assert "CI 2.5%" in html


# gap_vs_global＝gap 減全局參考值 0.3（見 "global"）；residual 已按新公式
# gap_vs_global − clip(gap_vs_global, theory_min, theory_max) 重算：
#   A: gap_vs_global=0.75-0.3=0.45，clip 到帶 [0.693,0.693]→0.693，
#      residual=0.45-0.693=-0.243（|.|≤0.3 → 可解釋，verdict 不變）。
#   B: gap_vs_global=0.9-0.3=0.6，clip 到帶 [0,0]→0，residual=0.6
#      （|.|>0.3 → 不可解釋，verdict 不變）。
# pooled_gap 依 A/B 的 p_mean/y_rate/n_rows 加權合併算出（僅供顯示，
# report_builder 本身不重算）。
# gap_calibrated_vs_global＝gap_calibrated 減全局參考值 reference_calibrated
# =-0.1（opus 審查修正：gap_calibrated 同樣受母體條件化位移，判讀校準層要
# 看相對值而非絕對值）：A: 0.02-(-0.1)=0.12；B: 0.8-(-0.1)=0.9。
_QUAD_FIXTURE = {
    "enabled": True,
    "thresholds": {"auc_threshold": 0.6, "top_k_occupancy": 1},
    "n_queries": 1000, "n_pos_queries": 400,
    "by_item": {
        "A": {"auc": 0.82, "auc_reason": None, "n_pos": 120, "n_neg": 880,
              "n_rows": 1000,
              "disc_status": "好", "quadrant": "健康",
              "ap_sampled": 0.61, "ci_low": 0.55, "ci_high": 0.68,
              "top_share": 0.2, "n_top": 200, "y_rate": 0.12,
              "suppression_count": 30},
        "B": {"auc": 0.51, "auc_reason": None, "n_pos": 20, "n_neg": 980,
              "n_rows": 1000,
              "disc_status": "差", "quadrant": "冷門受害者（判別力差）",
              "ap_sampled": 0.7, "ci_low": 0.5,
              "ci_high": 0.85, "top_share": 0.6, "n_top": 600,
              "y_rate": 0.02, "suppression_count": 480},
    },
    "cross_purchase": {
        "matrix": {"A": {"A": 1.0, "B": 0.3}, "B": {"A": 0.5, "B": 1.0}},
        "n_buyers": {"A": 100, "B": 60},
    },
    "sources": {"metric_ci": True},
    "notes": [],
}


def test_quadrant_section_renders_table_and_matrix():
    from recsys_tfb.evaluation.report_builder import build_quadrant_section
    sec = build_quadrant_section(_QUAD_FIXTURE, _params_min())
    tbl = sec.tables[0]
    assert list(tbl.index) == ["A", "B"]
    assert tbl.loc["B", "quadrant"] == "冷門受害者（判別力差）"
    # 散布圖的縱軸是 gap_vs_global（已隨對帳層退場）→ 整張圖移除
    assert not sec.figures
    assert "gap_vs_global" not in tbl.columns
    assert len(sec.tables) == 2           # 判別力表＋交叉購買矩陣
    assert sec.tables[1].loc["B", "A"] == pytest.approx(0.5)
    assert "判讀" in sec.description
    assert "evaluation-diagnosis" in sec.description


def test_quadrant_section_none_when_disabled_or_absent():
    from recsys_tfb.evaluation.report_builder import build_quadrant_section
    assert build_quadrant_section(None, _params_min()) is None
    assert build_quadrant_section({"enabled": False}, _params_min()) is None
    params_off = {"evaluation": {"report": {"sections": {"quadrant": False}}}}
    assert build_quadrant_section(_QUAD_FIXTURE, params_off) is None


def test_quadrant_section_notes_and_missing_axis():
    from recsys_tfb.evaluation.report_builder import build_quadrant_section
    auc_reason = "單一類別（n_pos=0, n_neg=10）——AUC 未定義"
    fx = dict(
        _QUAD_FIXTURE,
        by_item={"A": dict(_QUAD_FIXTURE["by_item"]["A"],
                           quadrant="無法評估", auc=None,
                           auc_reason=auc_reason)},
        notes=["metric_ci 停用或缺席——AP±CI 欄從缺。"],
    )
    sec = build_quadrant_section(fx, _params_min())
    assert "無法評估" in sec.tables[0]["quadrant"].tolist()
    assert "metric_ci 停用" in sec.description
    # 單類 item 的 auc 為 None 時，人類可讀的原因要進報表（不只在 JSON）。
    assert auc_reason in sec.tables[0]["auc_reason"].tolist()


def test_assemble_report_renders_quadrant():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), quadrant=_QUAD_FIXTURE
    )
    assert "條件判別力" in html


def test_assemble_report_has_no_reconciliation_section():
    """對帳層已退場——report.html 不得再出現該段落。"""
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), quadrant=_QUAD_FIXTURE
    )
    assert "對帳" not in html
    assert "Reconciliation" not in html


_SWEEP_FIXTURE = {
    "enabled": True,
    "map_fit": {"zero": 0.50, "star": 0.58},
    "map_holdout": {"zero": 0.51, "star": 0.56},
    "recovered_gap_holdout": 0.05,
    "interaction_residual_holdout": -0.01,
    "delta_star": {"A": -1.0, "B": 0.0},
    "delta_star_centered": {"A": -0.5, "B": 0.5},
    "per_item": {
        "A": {"delta_star": -1.0, "delta_star_centered": -0.5,
              "loo_contribution_holdout": 0.06},
        "B": {"delta_star": 0.0, "delta_star_centered": 0.5,
              "loo_contribution_holdout": None},
    },
    "params": {"shrink_lambda": 0.1, "holdout_fraction": 0.5,
               "max_rounds": 5,
               "grid": {"lo": -2.0, "hi": 2.0, "step": 0.05}},
    "notes": [],
}


def test_offset_sweep_section_off_by_config():
    from recsys_tfb.evaluation.report_builder import build_offset_sweep_section
    params_off = {
        "evaluation": {"report": {"sections": {"offset_sweep": False}}}
    }
    assert build_offset_sweep_section(_SWEEP_FIXTURE, params_off) is None


def test_offset_sweep_section_none_for_stub_or_missing():
    from recsys_tfb.evaluation.report_builder import build_offset_sweep_section
    assert build_offset_sweep_section(None, _params_min()) is None
    assert build_offset_sweep_section({"enabled": False}, _params_min()) is None


def test_offset_sweep_section_tables_and_waterfall():
    from recsys_tfb.evaluation.report_builder import build_offset_sweep_section
    section = build_offset_sweep_section(_SWEEP_FIXTURE, _params_min())
    assert section is not None
    assert len(section.tables) == 2
    assert "delta_star" in section.tables[1].columns
    assert "delta_star_centered" in section.tables[1].columns
    assert len(section.figures) == 1  # waterfall（有非零 δ*）


def test_offset_sweep_waterfall_skipped_when_all_deltas_zero():
    from recsys_tfb.evaluation.report_builder import build_offset_sweep_section
    payload = dict(
        _SWEEP_FIXTURE,
        delta_star={"A": 0.0, "B": 0.0},
        per_item={
            "A": {"delta_star": 0.0, "loo_contribution_holdout": None},
            "B": {"delta_star": 0.0, "loo_contribution_holdout": None},
        },
    )
    section = build_offset_sweep_section(payload, _params_min())
    assert section is not None
    assert section.figures == []


def test_assemble_report_includes_offset_sweep_section():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), offset_sweep=_SWEEP_FIXTURE
    )
    assert "Offset sweep" in html


_LEDGER_FIXTURE = {
    "enabled": True,
    "n_queries": 2, "n_pos_rows": 3, "n_mis_ordered_pairs": 3,
    "matrix": {"B": {"A": {"pair_count": 2, "dap_sum": 1.0},
                     "C": {"pair_count": 1, "dap_sum": 5 / 6}}},
    "by_suppressor": {"B": {"pair_count": 3, "dap_sum": 11 / 6,
                            "dap_share": 1.0}},
    "by_victim": {"A": {"pair_count": 2, "dap_sum": 1.0,
                        "dap_share": 6 / 11},
                  "C": {"pair_count": 1, "dap_sum": 5 / 6,
                        "dap_share": 5 / 11}},
    "map_current": 7 / 12,
    "substitution": {"B": {"base_rate": 0.0, "base_logit": -27.6,
                           "map_substituted": 1.0,
                           "delta_vs_current": 5 / 12}},
    "by_segment": {"seg": {"X": {"n_pos_rows": 2,
                                 "n_suppressed_pos_rows": 2,
                                 "dap_sum": 4 / 3,
                                 "dap_share": 8 / 11}}},
    "notes": [],
}


def test_pair_ledger_section_renders_heatmap_and_tables():
    from recsys_tfb.evaluation.report_builder import build_pair_ledger_section
    sec = build_pair_ledger_section(_LEDGER_FIXTURE, _params_min())
    assert sec is not None
    assert len(sec.figures) == 1
    assert len(sec.tables) == 3  # 壓制者邊際、substitution、by_segment


def test_pair_ledger_section_no_pairs_skips_figure_keeps_tables():
    from recsys_tfb.evaluation.report_builder import build_pair_ledger_section
    ledger = dict(_LEDGER_FIXTURE, n_mis_ordered_pairs=0, matrix={})
    sec = build_pair_ledger_section(ledger, _params_min())
    assert sec is not None and sec.figures == []


def test_pair_ledger_section_none_when_disabled_or_absent():
    from recsys_tfb.evaluation.report_builder import build_pair_ledger_section
    assert build_pair_ledger_section({"enabled": False}, _params_min()) is None
    assert build_pair_ledger_section(None, _params_min()) is None
    params_off = {
        "evaluation": {"report": {"sections": {"pair_ledger": False}}}
    }
    assert build_pair_ledger_section(_LEDGER_FIXTURE, params_off) is None


def test_pair_ledger_section_notes_appended_to_description():
    from recsys_tfb.evaluation.report_builder import build_pair_ledger_section
    ledger = dict(_LEDGER_FIXTURE, notes=["某注意事項"])
    sec = build_pair_ledger_section(ledger, _params_min())
    assert "某注意事項" in sec.description


def test_assemble_report_includes_pair_ledger_section():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), pair_ledger=_LEDGER_FIXTURE
    )
    assert "壓制帳本" in html


_TRIAGE_FIXTURE = {
    "enabled": True,
    "gain_ledger_present": True,
    "thresholds": {"starve_ratio": 0.25, "weight_cap": 8.0},
    "verdicts": {
        "feat": {
            "verdict": "特徵缺失型",
            "lever": "槓桿5：補特徵（診斷只能縮小範圍，補什麼是領域知識）",
            "starter": None,
            "evidence": {"auc": 0.5, "disc_status": "差",
                        "delta_star_centered": -0.5,
                        "loo_contribution_holdout": 0.0,
                        "context_gain_share": 0.35, "y_rate": 0.05},
            "notes": [],
        },
        "stv": {
            "verdict": "餓死型",
            "lever": "槓桿3：item-aware weight／熱門負類欠採（配 logQ）／HPO 先驗",
            "starter": {"type": "item_weight", "value": 4.47,
                        "unit": "sample_weight 相對倍率（w∝1/√P 加上限，手冊3 Ch8）",
                        "caveat": "起手值，須經快迴路驗證，非定案"},
            "evidence": {"auc": 0.5, "disc_status": "差",
                        "delta_star_centered": 0.3,
                        "loo_contribution_holdout": 0.0,
                        "context_gain_share": 0.01, "y_rate": 0.02},
            "notes": [],
        },
    },
    "summary": {"特徵缺失型": 1, "餓死型": 1},
    "notes": [],
}


def test_triage_section_renders_main_table():
    from recsys_tfb.evaluation.report_builder import build_triage_section
    sec = build_triage_section(_TRIAGE_FIXTURE, _params_min())
    assert sec is not None
    assert sec.figures == []
    assert len(sec.tables) == 1
    tbl = sec.tables[0]
    assert list(tbl.index) == ["feat", "stv"]
    for col in ["判定", "建議槓桿", "起手值", "AUC",
                "δ*_centered", "context_gain_share", "備註"]:
        assert col in tbl.columns
    # 水準軸退場 → 證據欄不再有 gap_vs_global
    assert "gap_vs_global" not in tbl.columns
    assert tbl.loc["feat", "判定"] == "特徵缺失型"
    assert tbl.loc["stv", "判定"] == "餓死型"
    assert "item_weight=4.470" in tbl.loc["stv", "起手值"]


def test_triage_section_none_when_disabled_or_absent():
    from recsys_tfb.evaluation.report_builder import build_triage_section
    assert build_triage_section(None, _params_min()) is None
    assert build_triage_section({"enabled": False}, _params_min()) is None
    params_off = {
        "evaluation": {"report": {"sections": {"triage": False}}}
    }
    assert build_triage_section(_TRIAGE_FIXTURE, params_off) is None


def test_triage_section_description_has_summary_and_gain_ledger_status():
    from recsys_tfb.evaluation.report_builder import build_triage_section
    sec = build_triage_section(_TRIAGE_FIXTURE, _params_min())
    assert "特徵缺失型" in sec.description
    assert "gain_ledger" in sec.description


def test_triage_section_gain_ledger_absent_flagged_in_description():
    from recsys_tfb.evaluation.report_builder import build_triage_section
    ledger_absent = dict(_TRIAGE_FIXTURE, gain_ledger_present=False)
    sec = build_triage_section(ledger_absent, _params_min())
    assert "缺席" in sec.description or "降級" in sec.description


def test_glossary_has_triage_entries():
    from recsys_tfb.evaluation.report_builder import build_glossary_section
    sec = build_glossary_section(_params_min())
    txt = " ".join(str(t.to_dict()) for t in sec.tables)
    assert "triage 總表" in txt
    assert "餓死型" in txt
    assert "起手值" in txt


def test_assemble_report_includes_triage_section():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), triage=_TRIAGE_FIXTURE
    )
    assert "Triage" in html
