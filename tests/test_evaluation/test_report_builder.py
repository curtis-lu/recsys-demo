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


def _metric_ci():
    return {
        "enabled": True,
        "macro": {"ap": 0.541, "ci_low": 0.520, "ci_high": 0.559},
        "sample": {
            "n_queries_sampled": 10,
            "sampling_description": "未抽樣：全部 10 個有正例的 query 都納入。",
        },
        "per_item": {"A": {"ap": 0.5, "ci_low": 0.45, "ci_high": 0.55,
                           "n_pos": 12}},
    }


def test_headline_section_has_map_card():
    s = rb.build_headline_section(_metrics(), _params())
    txt = " ".join(str(t.to_dict()) for t in s.tables)
    assert "map@1" in txt and "map@all" in txt   # "all" resolves via display
    assert "map@5" not in txt                     # not in display list


def test_overview_section_has_purpose_and_macro_headline():
    s = rb.build_overview_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert s.title == "概覽"
    # 目的句提到排序（這份報表在幹嘛）
    assert "排序" in s.description
    # 關鍵數含 macro per-item mAP（頭號指標）
    joined = " ".join(t.to_string() for t in s.tables)
    assert "macro" in joined.lower()
    # 有「問題 → 看哪一區」導覽表
    assert any("導覽" in tt or "看哪" in tt for tt in s.table_titles)


def test_overview_scale_and_severity_separated():
    # 規模／分母（n_queries 等）與關鍵指標分成不同表，避免分母被讀成嚴重度
    s = rb.build_overview_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert len(s.tables) >= 2
    # overall per-query mAP 明標為「另一種加權」，不宣稱哪個才對
    joined = s.description + " ".join(s.table_titles) + " ".join(
        t.to_string() for t in s.tables
    )
    assert "加權" in joined


def test_overview_no_verdict_vocabulary():
    s = rb.build_overview_section(_metrics(), _params(), metric_ci=_metric_ci())
    text = (s.description + " ".join(s.table_titles)
            + " ".join(t.to_string() for t in s.tables) + " ".join(s.bullets))
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "未達標", "嚴重", "良好"):
        assert bad not in text


def test_core_concept_section_defines_atomic_unit():
    s = rb.build_core_concept_section(_params())
    assert s.title.startswith("核心概念")
    # 有公式（AP@k 定義）
    assert s.formula
    # 用一個具體數字走一遍（bullets 內含數例）
    assert any(any(ch.isdigit() for ch in b) for b in s.bullets)
    # 有「每區＝同一數換切法」的地圖字樣
    joined = s.description + " ".join(s.bullets)
    assert "粒度" in joined or "加總" in joined


def test_core_concept_section_no_verdict_vocabulary():
    s = rb.build_core_concept_section(_params())
    text = s.description + s.formula + " ".join(s.bullets)
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "未達標", "嚴重", "良好"):
        assert bad not in text


def test_dataset_overview_section_tables():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    assert len(s.tables) == 3   # totals / by_snap_date / by_item
    assert s.title


def test_dataset_section_per_item_has_three_cols_with_share():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    by_item = next(
        t for t, tt in zip(s.tables, s.table_titles)
        if "per-item" in tt or "產品" in tt
    )
    cols = " ".join(map(str, by_item.columns))
    assert "正例數" in cols and "正樣本率" in cols and "正例佔比" in cols


def test_dataset_section_share_reconciles():
    # 正例佔比 = n_positives / 總正例，逐列加總 ≈ 1（手算可核對）
    s = rb.build_dataset_overview_section(_metrics(), _params())
    by_item = next(
        t for t in s.tables
        if "正例佔比" in " ".join(map(str, t.columns))
    )
    assert abs(by_item["正例佔比"].astype(float).sum() - 1.0) < 1e-6


def test_dataset_section_flags_phase2_stub():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    assert "後續" in s.description or "per-segment" in s.description.lower()


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


def test_assemble_report_has_no_ndcg_end_to_end():
    """端到端護欄：完整 report.html 整份不得出現 ndcg。fixture 刻意讓
    per_segment 與 baseline 兩條 key-agnostic 路徑都被走到——它們把 metrics
    dict 的 key 直接攤平，是 ndcg 最容易漏出去的地方（metrics_spark 仍算 ndcg，
    只是刻意不呈現）。section 級測試涵蓋不到這種整份洩漏，故獨立一條網子。"""
    m = _metrics()
    m["per_segment"] = {
        "young": {"map@1": 0.6, "ndcg@1": 0.55, "recall@1": 0.3},
        "old": {"map@1": 0.4, "ndcg@1": 0.35, "recall@1": 0.2},
    }
    html = rb.assemble_report(
        m, _params(), baseline_metrics=_baseline_metrics_full()
    )
    assert "ndcg" not in html.lower()


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


def test_assemble_report_has_no_reconciliation_section():
    """對帳層已退場——report.html 不得再出現該段落。"""
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(_metrics_min(), _params_min())
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


