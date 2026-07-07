"""Pure-dict tests for report_builder section functions (no Spark)."""

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


def test_primary_map_section_slices_k():
    s = rb.build_primary_map_section(_metrics(), _params())
    # families on the row index, @k slices as columns (one set shared by all)
    assert set(s.tables[0].index) == {"map", "precision", "ndcg", "recall"}
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
    assert len(s.tables) == 2 and len(s.figures) == 2
    map_tbl = s.tables[0]
    assert set(map_tbl.index) == {"Macro 平均", "A", "B"}
    cols = " ".join(map(str, map_tbl.columns))
    assert "map_attr@1" in cols and "map_attr@3" in cols
    ndcg_cols = " ".join(map(str, s.tables[1].columns))
    assert "ndcg_attr@1" in ndcg_cols


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
    assert "ndcg_attr@k" in terms


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
    map_tbl, ndcg_tbl = s.tables[0], s.tables[1]
    assert list(map_tbl.index)[0] == "Macro 平均"
    assert list(ndcg_tbl.index)[0] == "Macro 平均"
    # map_attr@1 per-product average = (0.5 + 0.3) / 2 = 0.4
    assert map_tbl.loc["Macro 平均", "map_attr@1"] == 0.4
    # ndcg_attr@1 per-product average = (0.45 + 0.25) / 2 = 0.35
    assert ndcg_tbl.loc["Macro 平均", "ndcg_attr@1"] == 0.35
    # heatmap excludes the macro row
    assert "Macro 平均" not in list(s.figures[0].data[0].y)
    assert "Macro 平均" not in list(s.figures[1].data[0].y)


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


def test_baseline_section_has_three_per_item_compare_tables():
    """recall / map_attr / ndcg_attr each get a M/B/Δ-interleaved table."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    # Old delta-only title must be gone.
    assert "per-item recall@k delta" not in s.table_titles
    # Three new titles present.
    for title in (
        "per-item recall@k (M/B/Δ)",
        "per-item map_attr@k (M/B/Δ)",
        "per-item ndcg_attr@k (M/B/Δ)",
    ):
        assert title in s.table_titles


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
