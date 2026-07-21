"""Pure-dict tests for report_builder section functions (no Spark)."""

import pandas as pd

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


def test_core_concept_formula_normalizes_by_R_not_min():
    """AP@k 分母是 R（正例總數）、不是 min(k,R)——這是與 metrics_spark 實作
    （map@K = sum(ap_contrib@K)/total_rel）對齊的硬約束。寫錯會讓讀者拿公式
    手算頭號家族時對不上（map@1 會被誤推成 precision@1 而非 recall@1）。"""
    s = rb.build_core_concept_section(_params())
    body = s.formula + " ".join(s.bullets)
    assert "AP@k = (1 / R)" in s.formula          # 正規化分母＝R
    assert "(1 / min" not in s.formula            # 不得用 min(k,R) 當 AP 分母
    assert "1/min" not in s.formula.replace(" ", "")
    # 提供可手算核對的錨點：map@1 = recall@1
    assert "map@1" in body and "recall@1" in body


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


# ---- Task 5: 衡量指標（合併 primary_map/guardrail/attr/segment/category）----

def test_metrics_section_overall_orientation_locked():
    # families（map/precision/recall）是 overall 表的 row index，非欄（方向鎖）
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert "map" in s.tables[0].index


def test_metrics_section_macro_is_headline():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert any("macro per-item mAP" in tt and "頭號" in tt
               for tt in s.table_titles)


def test_metrics_section_per_item_has_attr_and_recall():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    cols = " ".join(" ".join(map(str, t.columns)) for t in s.tables)
    assert "map_attr@1" in cols
    assert "recall@1 (per-item)" in cols


def test_metrics_section_no_guardrail_verdict():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    text = (s.description + " ".join(s.table_titles)
            + " ".join(t.to_string() for t in s.tables))
    for bad in ("護欄", "pass/fail", "達標", "未達標", "偏高", "偏低",
                "不足", "異常", "嚴重", "良好"):
        assert bad not in text


def test_metrics_section_hides_ndcg():
    m = _metrics()
    m["per_segment"] = {"seg1": {"map@1": 0.5, "ndcg@1": 0.6, "recall@1": 0.3}}
    s = rb.build_metrics_section(m, _params(), metric_ci=_metric_ci())
    assert "ndcg" not in " ".join(t.to_string().lower() for t in s.tables)


def test_metrics_section_detail_tables_collapsed():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert any(s.collapsed_tables)      # 明細收合
    assert not s.collapsed_tables[0]    # overall 頂線可見


def test_metrics_section_has_macro_rows():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    map_tbl = next(t for t in s.tables
                   if "map_attr@1" in " ".join(map(str, t.columns)))
    assert rb._MACRO_LABEL in map_tbl.index


def test_metrics_section_category_present_when_key():
    m = _metrics()
    m["category"] = {
        "overall": {"map@1": 0.4, "map@2": 0.45},
        "per_item": {"fund": {"hit_rate@1": 0.3, "mean_pos": 2.0}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.3, "mean_pos": 2.0}},
        "dataset_overview": {"totals": {"n_products": 2}},
    }
    s = rb.build_metrics_section(m, _params(), metric_ci=_metric_ci())
    assert any("大類" in tt for tt in s.table_titles)


def test_metrics_section_none_when_off():
    p = _params()
    p["evaluation"]["report"]["sections"] = {"primary_map": False}
    assert rb.build_metrics_section(_metrics(), p, metric_ci=_metric_ci()) is None


# ---- Task 6: per-item 細部拆解 ----

def _report_aggregates():
    """手建的 report_aggregates payload（frame_from_json 格式；含 calibration
    以驗證本段刻意不畫它）。"""
    return {
        "columns": {"item": "prod_name", "score": "score",
                    "rank": "rank", "label": "label"},
        "score_histogram": {
            "kind": "long",
            "columns": ["prod_name", "bin_center", "count", "bin_width"],
            "data": [["A", 0.1, 10, 0.2], ["A", 0.3, 10, 0.2],
                     ["B", 0.2, 8, 0.2], ["B", 0.4, 8, 0.2]]},
        "score_box_by_label": {
            "kind": "long",
            "columns": ["prod_name", "label", "q1", "median", "q3",
                        "lowerfence", "upperfence"],
            "data": [["A", 0, 0.1, 0.2, 0.3, 0.05, 0.35],
                     ["A", 1, 0.5, 0.6, 0.7, 0.45, 0.75],
                     ["B", 0, 0.1, 0.15, 0.2, 0.05, 0.25],
                     ["B", 1, 0.4, 0.5, 0.6, 0.35, 0.65]]},
        "rank_counts": {"kind": "matrix", "index": ["A", "B"],
                        "columns": [1, 2], "data": [[30, 10], [10, 30]]},
        "positive_rank_counts": {"kind": "matrix", "index": ["A", "B"],
                                 "columns": [1, 2], "data": [[6, 2], [2, 6]]},
        "positive_rate": {"kind": "matrix", "index": ["A", "B"],
                          "columns": [1, 2], "data": [[0.2, 0.2], [0.2, 0.2]]},
        "calibration": {"kind": "long",
                        "columns": ["prod_name", "bin", "mean_pred", "frac_pos"],
                        "data": [["A", 0, 0.2, 0.15], ["B", 0, 0.3, 0.25]]},
    }


def test_item_share_by_rank_columns_sum_to_one():
    # 欄正規化：每個 rank 欄 ÷ 欄和，每欄加總=1（render 端純算術，G#1）。
    # ★用非對稱矩陣：對稱矩陣下列正規化也會讓欄和=1，測不出「走哪條」，
    # 這裡兩列和不同（40 vs 20），故列正規化會讓欄和≠1（mutation 咬得住）。
    counts = pd.DataFrame([[30, 10], [10, 10]], index=["A", "B"], columns=[1, 2])
    share = rb._item_share_by_rank(counts)
    assert (abs(share.sum(axis=0) - 1.0) < 1e-6).all()


def test_item_detail_drops_calibration():
    # fixture 含 calibration，但本段刻意不畫（排序不是校準）→ 只有 4 張分布圖
    s = rb.build_item_detail_section(_report_aggregates(), _params())
    assert len(s.figures) == 4


def test_item_detail_is_top_level_not_collapsible():
    s = rb.build_item_detail_section(_report_aggregates(), _params())
    assert s.collapsible is False   # 升為頂層，不再整段收合


def test_item_detail_has_item_share_tables():
    s = rb.build_item_detail_section(_report_aggregates(), _params())
    joined = " ".join(s.table_titles)
    assert "item share by rank" in joined
    # share 表逐欄加總=1（手算可核對）
    share_tbl = s.tables[0]
    assert (abs(share_tbl.sum(axis=0) - 1.0) < 1e-6).all()


# ---- Task 9: 完整性檢查 ----

def test_completeness_section_lists_run_facts():
    s = rb.build_completeness_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert s.title == "完整性檢查"
    joined = (s.description + " ".join(s.bullets)
              + " ".join(t.to_string() for t in s.tables))
    assert "k" in joined.lower()               # k_values / metric.k 交代
    assert "query" in joined.lower()           # 規模
    assert "抽樣" in joined or "未抽樣" in joined  # sampling_description 流入


def test_completeness_section_no_verdict_vocabulary():
    s = rb.build_completeness_section(_metrics(), _params(), metric_ci=_metric_ci())
    text = (s.description + " ".join(s.bullets)
            + " ".join(t.to_string() for t in s.tables))
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "未達標", "嚴重", "良好"):
        assert bad not in text


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
    # overall 拆成 mAP/recall/precision 三張 explicit-family 表，天然不含 ndcg。
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    assert "ndcg" not in " ".join(t.to_string().lower() for t in s.tables)
    assert any(tt == "overall mAP@k (M/B/Δ)" for tt in s.table_titles)


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
    assert "概覽" in html                    # 新 spine 第一段


def test_assemble_report_new_spine_order():
    html = rb.assemble_report(
        _metrics(), _params(), baseline_metrics=_baseline_metrics_full(),
        metric_ci=_metric_ci(),
    )
    # 概覽最前、詞彙表殿後、完整性檢查在詞彙表之前
    assert html.index("概覽") < html.index("核心概念")
    assert html.index("完整性檢查") < html.index("詞彙表")
    for title in ("核心概念 — 一個 query 的排序", "基本統計 — 資料集",
                  "衡量指標", "baseline — popularity 對照", "完整性檢查"):
        assert title in html


def test_assemble_report_no_offset_sweep_in_main():
    # offset-sweep 已移出主報表（改由診斷連結導向後繼 score_shift）
    html = rb.assemble_report(
        _metrics_min(), _params_min(), offset_sweep=_SWEEP_FIXTURE
    )
    assert "Offset sweep" not in html
    assert "分流 Offset" not in html


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
    assert any(tt.startswith("by 大類") for tt in s.table_titles)


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
    # 無 per_item、無 purchase_counts → 只剩 overall 三張 family 表
    assert s.table_titles == [
        "overall mAP@k (M/B/Δ)",
        "overall recall@k (M/B/Δ)",
        "overall precision@k (M/B/Δ)",
    ]
    assert len(s.tables) == 3


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


def test_glossary_has_new_structure_terms():
    s = rb.build_glossary_section(_params())
    terms = set(s.tables[0]["指標"])
    assert "正例佔比" in terms
    assert "item share by rank" in terms
    assert "macro per-item mAP" in terms


def _params_lookback():
    p = _params()
    p["evaluation"]["baseline"] = {"lookback_months": 12}
    return p


def test_baseline_shows_lookback_window():
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params_lookback()
    )
    assert "12" in s.description


def test_baseline_overall_three_tables_k_as_columns():
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    fam = [tt for tt in s.table_titles if tt.startswith("overall ")]
    assert len(fam) == 3          # mAP / recall / precision 各一
    idx = s.table_titles.index("overall mAP@k (M/B/Δ)")
    cols = [str(c) for c in s.tables[idx].columns]
    assert "@1" in cols and "@all" in cols   # k 放欄位


def test_baseline_detail_tables_collapsed():
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    idx = s.table_titles.index("overall mAP@k (M/B/Δ)")
    assert s.collapsed_tables[idx] is True     # overall 明細收合


def test_baseline_popularity_avg_per_month_when_lookback():
    m = _metrics()
    base = {"overall": {"map@1": 0.4}, "purchase_counts": {"A": 120, "B": 240}}
    s = rb.build_baseline_section(m, base, _params_lookback())
    pop = s.tables[s.table_titles.index("popularity 排名組成")]
    assert "平均每月" in pop.columns
    assert pop.loc["B", "平均每月"] == 20.0     # 240 / 12


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


def test_baseline_section_overall_map_table_mbdelta_rows_k_cols():
    """新結構：overall mAP 表 rows=[Model,Baseline,Δ]、cols=@k。"""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.40, "ndcg@1": 0.50},
        "per_item": {"A": {"hit_rate@1": 0.1}},
    }
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    idx = s.table_titles.index("overall mAP@k (M/B/Δ)")
    tbl = s.tables[idx]
    assert list(tbl.index) == ["Model", "Baseline", "Δ"]
    # Model fixture has overall["map@1"]=0.5.
    assert tbl.loc["Model", "@1"] == 0.5
    assert tbl.loc["Baseline", "@1"] == 0.40
    assert abs(tbl.loc["Δ", "@1"] - (0.5 - 0.40)) < 1e-9


def test_baseline_section_overall_tables_use_k_superset_columns():
    """overall family 表以 k superset [1,2,3,4,5,all] 放欄位（explicit family，
    不再吃任意 metric key）。"""
    m = _metrics()
    base = {"overall": {"map@1": 0.4}, "per_item": {"A": {"hit_rate@1": 0.1}}}
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("overall mAP@k (M/B/Δ)")
    cols = [str(c) for c in s.tables[idx].columns]
    assert "@1" in cols and "@5" in cols and "@all" in cols


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


# offset-sweep 段已移出主報表（Task 11）；「不在主報表」的護欄見
# test_assemble_report_no_offset_sweep_in_main。build_offset_sweep_section 本身
# 保留、其單元測試（上方四條）續測該函式行為。


