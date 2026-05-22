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


def test_baseline_section_has_per_item_recall_delta():
    m = _metrics()
    base = {"overall": {"map@1": 0.4},
            "per_item": {"A": {"hit_rate@1": 0.1}}}
    p = _params()
    p["evaluation"].setdefault("report", {}).setdefault("sections", {})
    s = rb.build_baseline_section(m, base, p)
    assert s is not None
    assert len(s.tables) == 2
    assert "per-item recall@k delta" in s.table_titles


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
    assert s.table_titles == ["overall delta"]
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
    # 頂列為 Macro 平均
    assert list(table.index)[0] == "Macro 平均"
    # 值為各產品等權平均：hit_rate@1 → recall@1 (per-item) 欄
    assert table.loc["Macro 平均", "recall@1 (per-item)"] == 0.15
    assert table.loc["Macro 平均", "mean_pos"] == 4.0
    # heatmap 不含 macro 列
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
    # map_attr@1 各產品平均 = (0.5 + 0.3) / 2 = 0.4
    assert map_tbl.loc["Macro 平均", "map_attr@1"] == 0.4
    # ndcg_attr@1 各產品平均 = (0.45 + 0.25) / 2 = 0.35
    assert ndcg_tbl.loc["Macro 平均", "ndcg_attr@1"] == 0.35
    # heatmap 不含 macro 列
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
    # macro_avg 無 by_segment key
    s = rb.build_segment_section(m, _params())
    assert "Macro 平均" not in list(s.tables[0].index)
