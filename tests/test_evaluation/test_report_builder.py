"""Pure-dict tests for report_builder section functions (no Spark)."""

import pandas as pd
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
            "totals": {"n_rows": 100, "n_entities": 10, "n_items": 2,
                       "n_snap_dates": 1, "n_positives": 20,
                       "positive_rate": 0.2,
                       "avg_positives_per_entity": 2.0},
            "by_snap_date": {"20240331": {"n_rows": 100, "n_positives": 20,
                                          "n_entities": 10,
                                          "positive_rate": 0.2}},
            "by_item": {"A": {"n_rows": 50, "n_positives": 12,
                              "n_entities": 10, "positive_rate": 0.24},
                        "B": {"n_rows": 50, "n_positives": 8,
                              "n_entities": 10, "positive_rate": 0.16}},
            "by_segment": {
                "X": {"n_rows": 60, "n_positives": 14, "n_entities": 6,
                      "positive_rate": 14 / 60, "n_queries": 6,
                      "query_share": 0.6},
                "Y": {"n_rows": 40, "n_positives": 6, "n_entities": 4,
                      "positive_rate": 6 / 40, "n_queries": 4,
                      "query_share": 0.4}}},
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


def _params_with_metric_k(k):
    p = _params()
    p["evaluation"]["metric"] = {"k": k}
    return p


def _metrics_with_n_items(n):
    m = _metrics()
    m["dataset_overview"]["totals"]["n_items"] = n
    return m


def test_overview_ci_note_follows_metric_k():
    """Design H: the CI note is built from metric.k instead of hard-coding
    "same as map_attr@all".

    With metric.k set, point estimate and CI are both truncated at k and the
    matching full-population column is map_attr@k. n_items=5 keeps @3 among
    the metrics section's displayed columns.
    """
    s3 = rb.build_overview_section(
        _metrics_with_n_items(5), _params_with_metric_k(3), metric_ci=_metric_ci()
    )
    assert "截斷在 3" in s3.description
    assert "map_attr@3" in s3.description
    assert "map_attr@all 相同" not in s3.description

    s_null = rb.build_overview_section(
        _metrics_with_n_items(5), _params_with_metric_k(None), metric_ci=_metric_ci()
    )
    assert "不截斷" in s_null.description
    assert "map_attr@all" in s_null.description


def test_metrics_section_ci_point_note_follows_metric_k():
    """Design H: the metrics section's note on which column the CI point
    estimate matches also follows metric.k."""
    s3 = rb.build_metrics_section(
        _metrics_with_n_items(5), _params_with_metric_k(3), metric_ci=_metric_ci()
    )
    assert "截斷在 3" in s3.description
    assert "map_attr@3" in s3.description
    assert "map_attr@all" not in s3.description

    s_null = rb.build_metrics_section(
        _metrics_with_n_items(5), _params_with_metric_k(None), metric_ci=_metric_ci()
    )
    assert "不截斷" in s_null.description
    assert "map_attr@all" in s_null.description


@pytest.mark.parametrize("n_items, k", [(5, 7), (2, 3)])
def test_ci_notes_do_not_name_a_column_the_metrics_section_hides(n_items, k):
    """Design H x bug 8: a metric.k outside the metrics section's displayed
    K columns ([1..5, all] clamped to n_items) must not be pointed at as
    map_attr@k — that column is not on the page. The notes still state the
    truncation, and say the tables have no @k column."""
    m = _metrics_with_n_items(n_items)
    p = _params_with_metric_k(k)
    for section in (
        rb.build_overview_section(m, p, metric_ci=_metric_ci()),
        rb.build_metrics_section(m, p, metric_ci=_metric_ci()),
    ):
        assert f"map_attr@{k}" not in section.description
        assert f"截斷在 {k}" in section.description
        assert f"不顯示 @{k} 欄" in section.description


def test_overview_scale_table_labels_n_queries_as_the_full_total():
    """bug 3 (ADR-0020): n_queries is compute_dataset_overview's "all query
    groups before filtering" count, but the scale table used to label it
    "有正例 query 數" (queries WITH a positive) — self-contradictory next to
    "排除 query 數" on the very same table (you can't simultaneously have a
    million positive queries and exclude 950k of them). The label must say
    "all queries", and the actually-missing number (queries with a positive
    = n_queries - n_excluded_queries) must be printed somewhere."""
    m = _metrics()
    m["n_queries"] = 1_000_000
    m["n_excluded_queries"] = 950_000
    s = rb.build_overview_section(m, _params(), metric_ci=_metric_ci())
    scale = next(t for t in s.tables if "n_excluded_queries" in " ".join(
        str(i) for i in t.index))
    idx = [str(i) for i in scale.index]
    assert "全部 query 數 n_queries" in idx
    assert not any("有正例 query 數" in i for i in idx)
    assert "有正例的 query 數" in idx
    row = dict(zip(idx, scale["value"]))
    assert row["全部 query 數 n_queries"] == 1_000_000
    assert row["有正例的 query 數"] == 50_000  # n_queries - n_excluded_queries


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


def test_core_concept_section_prints_the_readers_own_column_names(
    renamed_schema_params,
):
    """報表這一段是給人看的定義，印的必須是讀者自己的欄名。

    這一段以前自備一份角色預設表；那份預設表跟 ``core/schema`` 的分岔起來不
    會報錯，只會讓同一份報表裡這一段印示例欄名、其他各區印真實欄名（#326）。
    """
    s = rb.build_core_concept_section(renamed_schema_params)
    body = s.description + s.formula + " ".join(s.bullets)
    for name in ("as_of_month", "store_id", "sku"):
        assert name in body
    # 反面：示例欄名一個都不該出現，否則就是又抄了一份預設表。
    for example in ("snap_date", "cust_id", "prod_name"):
        assert example not in body


def test_core_concept_section_reads_the_example_schema_through_get_schema():
    """宣告的就是示例欄名時，這一段也必須經 ``core/schema`` 取，不自備一份。

    這條與上一條合起來才擋得住「複製一份預設表」——只驗改名版的話，一份剛好
    抄對的預設表照樣全綠。#328 之後角色沒有內建預設，所以示例欄名這一半也要
    明寫出來，不能靠 ``{}`` 掉進預設。
    """
    params = _params()
    s = rb.build_core_concept_section(params)
    body = s.description + " ".join(s.bullets)
    from recsys_tfb.core.schema import get_schema

    schema = get_schema(params)
    assert schema["time"] in body
    assert schema["item"] in body


def test_dataset_overview_section_tables():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    assert len(s.tables) == 4   # totals / by_snap_date / by_item / by_segment
    assert s.title


def test_dataset_section_per_segment_real_numbers():
    """per-segment 表顯示真數字（正例數/正樣本率/query 數佔比），無「待補」。

    防退化：欄名或 query_share 讀錯、或退回 placeholder，都該紅。
    """
    s = rb.build_dataset_overview_section(_metrics(), _params())
    seg = next(t for t, tt in zip(s.tables, s.table_titles)
               if "per-segment" in tt)
    cols = list(map(str, seg.columns))
    assert "正例數" in cols and "query 數佔比" in cols
    assert "待補" not in " ".join(map(str, seg.values.ravel()))
    # query 數佔比逐列加總≈1（X=0.6、Y=0.4）
    assert abs(seg["query 數佔比"].astype(float).sum() - 1.0) < 1e-6
    # 第 3 欄不是「正例佔比」（防止被誤改成 per-item 那一軸）
    assert "正例佔比" not in cols


def test_dataset_section_shows_the_unmatched_group_with_its_query_count():
    """ADR-0020 bug 6: queries whose segment the population table could not
    supply are one visible row, with how many queries and what share, and
    the section says they are left out of every macro average."""
    m = _metrics()
    m["dataset_overview"]["by_segment"]["(unmatched)"] = {
        "n_rows": 20, "n_positives": 2, "n_entities": 2,
        "positive_rate": 0.1, "n_queries": 2, "query_share": 2 / 12}
    m["segments"] = {"joined": ["cust_segment_typ"],
                     "sources": {"cust_segment_typ": "sample_pool"},
                     "missing": {}}
    s = rb.build_dataset_overview_section(m, _params())
    seg = next(t for t, tt in zip(s.tables, s.table_titles)
               if "per-segment" in tt)
    assert seg.loc["(unmatched)", "query 數"] == 2
    assert seg.loc["X", "query 數"] == 6
    assert seg.loc["(unmatched)", "query 數佔比"] == pytest.approx(2 / 12)
    notes = " ".join(s.bullets)
    assert "(unmatched)" in notes and "不進 macro" in notes
    assert "cust_segment_typ 取自 sample_pool" in notes


def test_dataset_section_names_the_table_that_lacks_a_segment_column():
    """The population table has no such column: nothing per-segment is
    computed for it, and the report says which table and which column, so a
    misspelt column name is recognisable. Other tables still render."""
    m = _metrics()
    del m["dataset_overview"]["by_segment"]
    m["segments"] = {"joined": [], "sources": {},
                     "missing": {"cust_segment_typ": "inference_population"}}
    s = rb.build_dataset_overview_section(m, _params())
    assert "inference_population 無欄 cust_segment_typ" in " ".join(s.bullets)
    assert [tt for tt in s.table_titles if "per-segment" in tt] == []
    assert len(s.tables) == 3          # totals / by_snap_date / by_item


def test_dataset_section_has_no_segment_notes_without_segments_info():
    """Metrics written before evaluation_segment_columns existed (and the
    comparison report's metrics) carry no ``segments`` block."""
    s = rb.build_dataset_overview_section(_metrics(), _params())
    assert "(unmatched)" not in " ".join(s.bullets)


def test_metrics_section_puts_unmatched_last_and_says_it_is_not_in_macro():
    """The unmatched group is defined where its row appears (presentation
    rule: definitions next to the number), and sits after the real segments
    in the per-segment tables."""
    m = _metrics()
    m["per_segment"] = {
        "(unmatched)": {"map@1": 0.1, "precision@1": 0.1, "recall@1": 0.1},
        "X": {"map@1": 0.6, "precision@1": 0.5, "recall@1": 0.3},
    }
    m["macro_avg"]["by_segment"] = {
        "map@1": 0.6, "precision@1": 0.5, "recall@1": 0.3}
    s = rb.build_metrics_section(m, _params())
    table, title = next((t, tt) for t, tt in zip(s.tables, s.table_titles)
                        if "per-segment map@k" in tt)
    assert list(table.index)[-1] == "(unmatched)"
    assert "(unmatched) 不含在 Macro" in title

    del m["per_segment"]["(unmatched)"]
    s = rb.build_metrics_section(m, _params())
    assert not [tt for tt in s.table_titles if "(unmatched)" in tt]


def test_baseline_per_segment_table_puts_unmatched_last():
    m = _metrics_with_seg_cat()
    b = _baseline_with_seg_cat()
    m["per_segment"]["(unmatched)"] = {"map@1": 0.1, "map@2": 0.1, "map@3": 0.1}
    b["per_segment"]["(unmatched)"] = {"map@1": 0.05, "map@2": 0.05,
                                       "map@3": 0.05}
    s = rb.build_baseline_section(m, b, _params())
    table = next(t for t, tt in zip(s.tables, s.table_titles)
                 if "per-segment mAP@k" in tt)
    assert list(table.index)[-3:] == [
        "(unmatched) · Model", "(unmatched) · Baseline", "(unmatched) · Δ"]


def test_dataset_section_per_segment_has_candidate_col():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    idx = next(i for i, tt in enumerate(s.table_titles) if "per-segment" in tt)
    cols = list(map(str, s.tables[idx].columns))
    assert "候選列數" in cols          # 讓正樣本率可手算核對（正例數÷候選列數）


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


def test_dataset_section_flags_remaining_phase2():
    s = rb.build_dataset_overview_section(_metrics(), _params())
    # per-segment 已補真數字；仍待的是每-query 正例數分佈
    assert "每-query" in s.description or "per-query" in s.description.lower()


# ---- Task 5: 衡量指標（合併 primary_map/guardrail/attr/segment/category）----

def test_metrics_section_overall_orientation_locked():
    # families（map/precision/recall）是 overall 彙總表的 row index，非欄（方向鎖）
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    fam_tbl = next(t for t in s.tables
                   if set(["map", "precision", "recall"]).issubset(set(t.index)))
    assert "map" in fam_tbl.index and "recall" in fam_tbl.index


def test_metrics_section_two_family_blocks_consistent():
    m = _metrics()
    m["per_segment"] = {"seg1": {"map@1": 0.5, "precision@1": 0.4, "recall@1": 0.3}}
    m["category"] = {
        "overall": {"map@1": 0.4, "precision@1": 0.3, "recall@1": 0.2},
        "per_item": {"fund": {"hit_rate@1": 0.3, "map_attr@1": 0.5, "mean_pos": 2.0}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.3, "map_attr@1": 0.5,
                                  "mean_pos": 2.0}},
        "dataset_overview": {"totals": {"n_items": 2}},
    }
    s = rb.build_metrics_section(m, _params(), metric_ci=_metric_ci())
    tt = s.table_titles
    # A 塊 per-segment 拆成 map/precision/recall 三張
    assert sum(1 for x in tt if "per-segment" in x and "map@k" in x) == 1
    assert sum(1 for x in tt if "per-segment" in x and "precision@k" in x) == 1
    assert sum(1 for x in tt if "per-segment" in x and "recall@k" in x) == 1
    # A 塊 大類 overall 有 precision（families 表含 precision 列）
    cat_ov = next(t for t, x in zip(s.tables, tt)
                  if "大類 overall" in x)
    assert "precision" in cat_ov.index
    # B 塊 大類 per-item 補了 map_attr（與 per-item 對稱）
    assert any("大類 per-item" in x and "map_attr@k" in x for x in tt)
    assert any("大類 per-item" in x and "recall@k" in x for x in tt)


def test_metrics_section_macro_is_headline():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    assert any("macro per-item mAP" in tt and "頭號" in tt
               for tt in s.table_titles)


def test_metrics_section_per_item_columns_bare_at_k_family_in_title():
    """B 塊(per-item 歸因)欄名統一裸 @k、family 只在標題——與 A 塊一致。

    防退化：改回冗餘欄名 map_attr@1／recall@1 (per-item)，或把 family
    從標題拿掉，本測試都該轉紅。也順帶驗兩張 per-item 表都在（attr+recall）。
    """
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    by_title = dict(zip(s.table_titles, s.tables))
    map_title = next(t for t in s.table_titles
                     if "per-item 歸因" in t and "map_attr@k" in t
                     and "大類" not in t)
    rec_title = next(t for t in s.table_titles
                     if "per-item 歸因" in t and "recall@k" in t
                     and "大類" not in t)
    map_cols = list(map(str, by_title[map_title].columns))
    rec_cols = list(map(str, by_title[rec_title].columns))
    # 欄名裸 @k（family 在標題、不重複塞進欄名），與 A 塊 _families/_entities 一致
    assert "@1" in map_cols and "@1" in rec_cols
    # 舊冗餘欄名已消除
    assert "map_attr@1" not in map_cols
    assert "recall@1 (per-item)" not in rec_cols
    # recall 表仍保留 mean_pos 額外欄
    assert "mean_pos" in rec_cols


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


def test_metrics_section_per_item_macro_titles_disclose_item_coverage():
    """bug 5 (ADR-0020): per-item macro averages only average items that
    have at least one positive this period — a zero-positive item silently
    vanishes from the denominator (per_item), so the macro shifts across
    months for a reason the report never states. Fix is disclosure, not a
    definition change: the table title states N (items actually averaged)
    out of M (the grain's total item count); the Macro row's own values are
    untouched.
    """
    m = _metrics()  # per_item has exactly {"A", "B"} — 2 items
    m["dataset_overview"]["totals"]["n_items"] = 3  # a 3rd item had 0 positives
    s = rb.build_metrics_section(m, _params(), metric_ci=_metric_ci())
    map_title = next(t for t in s.table_titles
                     if "per-item 歸因" in t and "map_attr@k" in t
                     and "大類" not in t)
    rec_title = next(t for t in s.table_titles
                     if "per-item 歸因" in t and "recall@k" in t
                     and "大類" not in t)
    assert "（參與 macro 的 item 數 2／全部 3）" in map_title
    assert "（參與 macro 的 item 數 2／全部 3）" in rec_title
    # Macro row's own values are unchanged by the disclosure (still the
    # same equal-weight average of the 2 items actually in per_item).
    by_title = dict(zip(s.table_titles, s.tables))
    macro_row = by_title[map_title].loc[rb._MACRO_LABEL]
    assert macro_row["@1"] == m["macro_avg"]["by_item"]["map_attr@1"]


def test_metrics_section_item_coverage_counts_only_items_meeting_min_positives():
    """bug 5: evaluation.metric.min_positives drops items from the macro as
    silently as zero positives do, so N counts per_item entries with
    n_pos >= min_positives, not every per_item key. A (n_pos 100) is in,
    B (n_pos 10 < 50) is out, and the grain has 3 items."""
    m = _metrics()
    m["per_item"]["A"]["n_pos"] = 100
    m["per_item"]["B"]["n_pos"] = 10
    m["dataset_overview"]["totals"]["n_items"] = 3
    p = _params()
    p["evaluation"]["metric"] = {"min_positives": 50}
    s = rb.build_metrics_section(m, p, metric_ci=_metric_ci())
    map_title = next(t for t in s.table_titles
                     if "per-item 歸因" in t and "map_attr@k" in t
                     and "大類" not in t)
    assert "（參與 macro 的 item 數 1／全部 3）" in map_title


def test_macro_coverage_suffixes_follow_each_tables_macro_row_condition():
    """The single-sided suffix appears only for a truthy macro (the Macro row
    test of _per_item_metric_table); the M/B one only when both macros are
    not None (the test of _per_item_metric_compare_table, where {} counts)."""
    per_item = {"A": {"n_pos": 1}}
    p = _params()
    assert rb.macro_coverage_suffix(per_item, 2, p, {}) == ""
    assert rb.macro_coverage_suffix(per_item, 2, p, {"map_attr@1": 0.1}) == (
        "（參與 macro 的 item 數 1／全部 2）"
    )
    assert rb.macro_coverage_suffix_mb(per_item, per_item, 2, p, {}, None) == ""
    assert rb.macro_coverage_suffix_mb(per_item, per_item, 2, p, {}, {}) == (
        "（參與 macro 的 item 數 M 1／B 1／全部 2）"
    )


def test_metrics_section_has_macro_rows():
    s = rb.build_metrics_section(_metrics(), _params(), metric_ci=_metric_ci())
    # 以標題定位 per-item map_attr 表（欄名裸 @k，不能再靠欄名找）
    map_tbl = next(t for t, tt in zip(s.tables, s.table_titles)
                   if "per-item 歸因" in tt and "map_attr@k" in tt
                   and "大類" not in tt)
    assert rb._MACRO_LABEL in map_tbl.index


def test_metrics_section_category_present_when_key():
    m = _metrics()
    m["category"] = {
        "overall": {"map@1": 0.4, "map@2": 0.45},
        "per_item": {"fund": {"hit_rate@1": 0.3, "mean_pos": 2.0}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.3, "mean_pos": 2.0}},
        "dataset_overview": {"totals": {"n_items": 2}},
    }
    s = rb.build_metrics_section(m, _params(), metric_ci=_metric_ci())
    assert any("大類" in tt for tt in s.table_titles)


def test_metrics_section_category_columns_clamp_to_category_count():
    """bug 8 (ADR-0020): only 3 categories -> the 大類 overall family table
    (which uses the literal [1,2,3,4,5,"all"] superset) must not show @4/@5;
    fine-grained tables (n_items=2 in this fixture) are unaffected."""
    m = _metrics()
    m["category"] = {
        "overall": {"map@1": 0.4, "map@2": 0.45, "map@3": 0.5},
        "per_item": {"fund": {"hit_rate@1": 0.3, "mean_pos": 2.0},
                     "exchange": {"hit_rate@1": 0.4, "mean_pos": 1.5},
                     "ccard": {"hit_rate@1": 0.5, "mean_pos": 1.0}},
        "macro_avg": {"by_item": {"hit_rate@1": 0.4, "mean_pos": 1.5}},
        "dataset_overview": {"totals": {"n_items": 3}},
    }
    s = rb.build_metrics_section(m, _params(), metric_ci=_metric_ci())
    cat_overall = next(t for t, tt in zip(s.tables, s.table_titles)
                       if "大類 overall" in tt)
    cols = [str(c) for c in cat_overall.columns]
    assert cols == ["@1", "@2", "@3", "@all"]


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
    # fixture 含 calibration，但本段刻意不畫（排序不是校準）。5 張圖＝score 分布 2
    # ＋rank 矩陣 heatmap 3（含 positive rate），無 calibration 第 6 張。
    s = rb.build_item_detail_section(_report_aggregates(), _params())
    assert len(s.figures) == 5


def test_item_detail_positive_rate_is_figure_not_table():
    # positive rate 改成 heatmap（圖），item-share 仍是數字表；圖群組在前
    s = rb.build_item_detail_section(_report_aggregates(), _params())
    assert len(s.tables) == 2   # 只剩兩張 item-share 數字表
    assert all("item share by rank" in tt for tt in s.table_titles)


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


def test_completeness_facts_label_n_queries_as_the_full_total():
    """bug 3 (ADR-0020): same fix as build_overview_section, mirrored here.
    Only the two n_queries-related labels change — metric_p reading
    (build_completeness_section:1092/1097) is out of scope for this bug."""
    m = _metrics()
    m["n_queries"] = 1_000_000
    m["n_excluded_queries"] = 950_000
    s = rb.build_completeness_section(m, _params(), metric_ci=_metric_ci())
    idx = [str(i) for i in s.tables[0].index]
    assert "全部 query 數 n_queries" in idx
    assert not any("有正例 query 數" in i for i in idx)
    assert "有正例的 query 數" in idx
    row = dict(zip(idx, s.tables[0]["value"]))
    assert row["全部 query 數 n_queries"] == 1_000_000
    assert row["有正例的 query 數"] == 50_000


def test_completeness_section_no_verdict_vocabulary():
    s = rb.build_completeness_section(_metrics(), _params(), metric_ci=_metric_ci())
    text = (s.description + " ".join(s.bullets)
            + " ".join(t.to_string() for t in s.tables))
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "未達標", "嚴重", "良好"):
        assert bad not in text


class TestRenamedSchemaLabels:
    """報表的欄名標籤印使用者自己的欄名，不是示例部署的業務詞（#327）。

    用 ``renamed_schema_params``（三個角色全部改名）而不是本檔的 ``_params()``
    ——後者宣告的正是示例欄名，寫死中文業務詞的程式碼在它底下永遠是綠的，
    這正是這個 fixture 存在的理由。

    斷言的是「印出來的字對不對」這個外部可觀察行為，不是「有沒有呼叫
    ``get_schema``」——後者會在下一次搬動時變成噪音。
    """

    def _renamed(self, renamed_schema_params):
        params = dict(renamed_schema_params)
        params["evaluation"] = _params()["evaluation"]
        return params

    def test_completeness_facts_label_names_the_users_item_column(
        self, renamed_schema_params
    ):
        s = rb.build_completeness_section(
            _metrics(), self._renamed(renamed_schema_params), metric_ci=_metric_ci()
        )
        text = " ".join(t.to_string() for t in s.tables)
        assert "sku 數 n_items" in text
        assert "產品數" not in text

    def test_overview_scale_label_names_the_users_entity_columns(
        self, renamed_schema_params
    ):
        s = rb.build_overview_section(
            _metrics(), self._renamed(renamed_schema_params), metric_ci=_metric_ci()
        )
        text = " ".join(t.to_string() for t in s.tables)
        assert "每 store_id 平均正例數 avg_positives_per_entity" in text
        assert "每客戶平均正例數" not in text

    def test_a_multi_column_entity_is_joined_not_truncated(
        self, two_column_entity_params
    ):
        """entity 是清單，標籤要印完整的清單。

        取第一欄在單欄設定下與正解等價（S4 擋的正是這個讀法），所以這條必須用
        兩欄的 fixture 才問得出來。
        """
        params = dict(two_column_entity_params)
        params["evaluation"] = _params()["evaluation"]
        s = rb.build_overview_section(
            _metrics(), params, metric_ci=_metric_ci()
        )
        text = " ".join(t.to_string() for t in s.tables)
        entity = "×".join(two_column_entity_params["schema"]["columns"]["entity"])
        assert "×" in entity, "fixture 不再是多欄 entity，這條測試已失去意義"
        assert f"每 {entity} 平均正例數" in text


class TestLegacyDatasetOverviewRefused:
    """讀到 #327 之前的 evaluation_results.json 要 raise，不做靜默 fallback。"""

    def _legacy(self, **overrides):
        metrics = _metrics()
        overview = metrics["dataset_overview"]
        overview["totals"].pop("n_items")
        overview["totals"]["n_products"] = 2
        overview.update(overrides)
        return metrics

    def test_a_legacy_totals_key_raises_and_names_the_script(self):
        with pytest.raises(ValueError) as exc:
            rb._dataset_overview(self._legacy())
        message = str(exc.value)
        assert "n_products" in message
        assert rb.MIGRATION_SCRIPT in message

    def test_a_legacy_key_hiding_in_a_cell_is_found_too(self):
        """totals 可能已被手動改對，而 by_* 的 cell 沒有。

        只看 totals 的檢查會放行那種半遷移的檔案，而它的 by_item 表會印出一欄
        ``n_customers``——舊詞彙，零錯誤訊息。
        """
        metrics = _metrics()
        metrics["dataset_overview"]["by_item"]["A"]["n_customers"] = 10
        with pytest.raises(ValueError, match="n_customers"):
            rb._dataset_overview(metrics)

    def test_the_section_builders_refuse_the_same_file(self):
        """護欄擋在存取器上，所以每個讀取端都跟著擋——含比較報表那一條。"""
        from recsys_tfb.evaluation.comparison import report as cmp_report

        legacy = self._legacy()
        for call in (
            lambda: rb.build_overview_section(legacy, _params()),
            lambda: rb.build_dataset_overview_section(legacy, _params()),
            lambda: rb.build_completeness_section(legacy, _params()),
            lambda: cmp_report._n_items(legacy),
        ):
            with pytest.raises(ValueError, match="predates the #327 key rename"):
                call()

    def test_a_bundle_with_no_dataset_overview_is_left_alone(self):
        """baseline 的 slim bundle 本來就沒有 dataset_overview，不得被誤擋。"""
        assert rb._dataset_overview({"overall": {"map@1": 0.5}}) == {}
        assert rb._n_items({"overall": {"map@1": 0.5}}) == 0


class TestResolveDisplayKClampsToItemCount:
    """bug 8 (ADR-0020): the display K list used to be an unclamped
    superset — with only 3 categories, the report still printed @4/@5,
    whose precision denominator is just K (not min(K, n_items)), so those
    columns decline purely because the denominator grew, not because the
    model did anything different. Fix is a filter over the given list
    (never regenerates it — a caller list without "all" doesn't gain one),
    keeping "all" always."""

    def test_drops_k_above_n_items_keeps_all(self):
        assert rb._resolve_display_k([1, 3, 5, "all"], 3) == [1, 3, "all"]

    def test_keeps_everything_when_n_items_covers_it(self):
        assert rb._resolve_display_k([1, 3, 5, "all"], 5) == [1, 3, 5, "all"]

    def test_does_not_add_all_when_caller_list_lacks_it(self):
        assert rb._resolve_display_k([1, 3, 5], 3) == [1, 3]

    def test_skips_the_filter_when_n_items_is_zero_or_unknown(self):
        """n_items<=0 means overview data is missing (e.g. baseline's slim
        per_item bundle) — filtering would collapse every table down to
        just "@all"."""
        assert rb._resolve_display_k([1, 3, 5, "all"], 0) == [1, 3, 5, "all"]


class TestDropMetricKeysAboveItemCount:
    """bug 8 (ADR-0020) for key-agnostic tables: the same K > n_items rule as
    _resolve_display_k, applied to metric keys instead of a display list."""

    def test_drops_only_integer_k_above_the_count(self):
        keys = ["map@3", "precision@4", "recall@5", "mean_pos", "map@all"]
        assert rb.drop_metric_keys_above_item_count(keys, 3) == [
            "map@3", "mean_pos", "map@all",
        ]

    def test_keeps_k_equal_to_the_count(self):
        assert rb.drop_metric_keys_above_item_count(["precision@3"], 3) == [
            "precision@3"
        ]

    def test_skips_the_filter_when_n_items_is_zero_or_unknown(self):
        assert rb.drop_metric_keys_above_item_count(["map@5"], 0) == ["map@5"]


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


def test_baseline_overall_table_hides_ndcg():
    # overall 拆成 mAP/recall/precision 三張 explicit-family 表，天然不含 ndcg。
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    assert "ndcg" not in " ".join(t.to_string().lower() for t in s.tables)
    assert any(tt == "overall mAP@k (M/B/Δ)" for tt in s.table_titles)


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
        "fund": {"n_rows": 10, "n_positives": 3, "n_entities": 5,
                 "positive_rate": 0.3}}}}
    s = rb.build_dataset_overview_section(m, _params())
    idx = next(i for i, tt in enumerate(s.table_titles)
               if tt.startswith("by 大類"))
    assert s.collapsed_tables[idx] is False   # 大類表預設展開，不收合


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


def test_glossary_has_attr_entries():
    s = rb.build_glossary_section(_params())
    terms = set(s.tables[0]["指標"])
    assert "map_attr@k" in terms
    # ndcg 兩條已退場——glossary 與 report_comparison.html 共用同一份 _GLOSSARY
    assert "ndcg@k" not in terms
    assert "ndcg_attr@k" not in terms


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


def _metrics_with_seg_cat():
    """Model metrics carrying per_segment + category overall (for baseline
    by-seg / 大類 comparison)."""
    m = _metrics()
    m["per_segment"] = {
        "X": {"map@1": 0.6, "map@3": 0.62, "map@2": 0.61,
              "recall@1": 0.3, "precision@1": 0.5},
        "Y": {"map@1": 0.4, "map@3": 0.44, "map@2": 0.42,
              "recall@1": 0.2, "precision@1": 0.35},
    }
    m["category"] = {
        "overall": {"map@1": 0.55, "map@3": 0.6, "map@2": 0.58},
        "dataset_overview": {"totals": {"n_items": 3}},
    }
    return m


def _baseline_with_seg_cat():
    base = dict(_baseline_metrics_full())
    base["per_segment"] = {
        "X": {"map@1": 0.5, "map@3": 0.52, "map@2": 0.51,
              "recall@1": 0.25, "precision@1": 0.45},
        "Y": {"map@1": 0.3, "map@3": 0.34, "map@2": 0.32,
              "recall@1": 0.15, "precision@1": 0.3},
    }
    base["category"] = {
        "overall": {"map@1": 0.45, "map@3": 0.5, "map@2": 0.48},
    }
    return base


def test_baseline_per_segment_map_compare_table():
    s = rb.build_baseline_section(
        _metrics_with_seg_cat(), _baseline_with_seg_cat(), _params()
    )
    title = "per-segment mAP@k (M/B/Δ)"
    assert title in s.table_titles
    tbl = s.tables[s.table_titles.index(title)]
    # rows: seg × {Model, Baseline, Δ}, in segment order
    assert list(tbl.index) == [
        "X · Model", "X · Baseline", "X · Δ",
        "Y · Model", "Y · Baseline", "Y · Δ",
    ]
    assert tbl.loc["X · Model", "@1"] == 0.6
    assert tbl.loc["X · Baseline", "@1"] == 0.5
    assert abs(tbl.loc["X · Δ", "@1"] - 0.1) < 1e-9    # 0.6 - 0.5
    assert s.collapsed_tables[s.table_titles.index(title)] is True


def test_baseline_category_overall_map_compare_table():
    s = rb.build_baseline_section(
        _metrics_with_seg_cat(), _baseline_with_seg_cat(), _params()
    )
    title = "大類 overall mAP@k (M/B/Δ)"
    assert title in s.table_titles
    tbl = s.tables[s.table_titles.index(title)]
    assert list(tbl.index) == ["Model", "Baseline", "Δ"]
    assert tbl.loc["Model", "@1"] == 0.55
    assert tbl.loc["Baseline", "@1"] == 0.45
    assert abs(tbl.loc["Δ", "@1"] - 0.10) < 1e-9


def test_baseline_section_absent_when_the_baseline_was_not_computed():
    """``compute_baseline_metrics`` writes a stub, not ``None``, when the
    section is off: the stub has to carry the config fingerprint. The section
    builder must read that stub as "no baseline", not as a baseline whose
    every metric is missing."""
    stub = {"enabled": False,
            "config_fingerprint": {"sha256": "0" * 64, "values": {}}}
    assert rb.build_baseline_section(_metrics(), stub, _params()) is None


def test_baseline_omits_seg_cat_tables_when_absent():
    """Backward compat: baseline without per_segment/category -> no extra
    tables (older artifacts, or model without those slices)."""
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    assert "per-segment mAP@k (M/B/Δ)" not in s.table_titles
    assert "大類 overall mAP@k (M/B/Δ)" not in s.table_titles


def test_baseline_shows_lookback_window():
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params_lookback()
    )
    assert "12" in s.description


def test_baseline_lookback_sentence_prints_the_nodes_actual_default():
    """bug 1 (ADR-0020): the node (compute_baseline_metrics) defaults
    lookback_months to 12 when evaluation.baseline.lookback_months is unset;
    the report used to default to None instead and print no lookback
    sentence at all for that same run — two different beliefs about what was
    actually computed. Both must now read the same shared default (12)."""
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()  # no evaluation.baseline key
    )
    assert "過去 12 個月" in s.description


def test_baseline_lookback_sentence_reflects_a_configured_value():
    """Guardrail: an explicit lookback_months still prints its own value."""
    p = _params_lookback()
    p["evaluation"]["baseline"]["lookback_months"] = 6
    s = rb.build_baseline_section(_metrics(), _baseline_metrics_full(), p)
    assert "過去 6 個月" in s.description


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


def test_baseline_discloses_a_partially_covered_lookback_window():
    """bug 1 (ADR-0020): the empty-window raise only fires when the window
    has no label rows at all. label_table covering 2 of the 12 lookback
    months used to print the plain 12-month sentence and divide the
    per-month average by 12 — 6x too low. monthly_counts holds only months
    with label rows inside the window, so its distinct months are the
    coverage."""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.4},
        "purchase_counts": {"A": 24, "B": 6},
        "monthly_counts": {
            "A": {"2025-11": 12, "2025-12": 12},
            "B": {"2025-12": 6},
        },
    }
    s = rb.build_baseline_section(m, base, _params_lookback())
    assert "實際只涵蓋 2 個月" in s.description
    pop = s.tables[s.table_titles.index("popularity 排名組成")]
    assert pop.loc["A", "平均每月"] == 12.0     # 24 / 2 covered months


def test_baseline_fully_covered_lookback_window_keeps_the_plain_sentence():
    months = [f"2025-{mo:02d}" for mo in range(1, 13)]
    m = _metrics()
    base = {
        "overall": {"map@1": 0.4},
        "purchase_counts": {"A": 24},
        "monthly_counts": {"A": {mo: 2 for mo in months}},
    }
    s = rb.build_baseline_section(m, base, _params_lookback())
    assert "popularity 以過去 12 個月的歷史購買計數重排。" in s.description
    assert "實際只涵蓋" not in s.description
    pop = s.tables[s.table_titles.index("popularity 排名組成")]
    assert pop.loc["A", "平均每月"] == 2.0      # 24 / 12


def test_baseline_section_renders_popularity_table():
    """purchase_counts -> popularity composition table prepended.

    ``平均每月`` is now always present (bug 1, ADR-0020): lookback_months
    resolves via the shared helper's default (12) even when
    evaluation.baseline is unset, so there is no longer an "unknown
    lookback" case that omits this column.
    """
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
    assert list(tbl.columns) == ["count", "平均每月", "rank"]
    assert list(tbl.index) == ["B", "A", "C"]
    assert list(tbl["count"]) == [200, 50, 10]
    assert list(tbl["rank"]) == [1, 2, 3]


def test_baseline_monthly_trend_table():
    """monthly_counts -> 月度趨勢 table: rows=item(總計降序), cols=月份(升序)+合計."""
    m = _metrics()
    base = {
        "overall": {"map@1": 0.4},
        "purchase_counts": {"A": 3, "B": 1},
        "monthly_counts": {
            "A": {"2024-06": 2, "2024-12": 1},
            "B": {"2024-06": 1},
        },
    }
    s = rb.build_baseline_section(m, base, _params())
    assert "popularity 月度趨勢" in s.table_titles
    tbl = s.tables[s.table_titles.index("popularity 月度趨勢")]
    assert list(tbl.columns) == ["2024-06", "2024-12", "合計"]
    assert list(tbl.index) == ["A", "B"]          # A 總計 3 > B 總計 1
    assert list(tbl.loc["A"]) == [2, 1, 3]
    assert list(tbl.loc["B"]) == [1, 0, 1]        # 缺月補 0
    # 合計欄逐 item 對齊 purchase_counts（兩者都是同一批 per-月計數的重排）
    assert tbl.loc["A", "合計"] == base["purchase_counts"]["A"]


def test_baseline_omits_monthly_trend_when_absent():
    """Backward compat: no monthly_counts -> no 月度趨勢 table."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4}, "purchase_counts": {"A": 3}}
    s = rb.build_baseline_section(m, base, _params())
    assert "popularity 月度趨勢" not in s.table_titles


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
    """The overall family tables put the k superset [1,2,3,4,5,all] in columns
    (explicit family, no longer any metric key), but bug 8 (ADR-0020) clamps
    display columns with K > n_items — the fixture has n_items=2, so
    @3/@4/@5 must not appear, leaving [1,2,all]."""
    m = _metrics()
    base = {"overall": {"map@1": 0.4}, "per_item": {"A": {"hit_rate@1": 0.1}}}
    s = rb.build_baseline_section(m, base, _params())
    idx = s.table_titles.index("overall mAP@k (M/B/Δ)")
    cols = [str(c) for c in s.tables[idx].columns]
    assert "@1" in cols and "@2" in cols and "@all" in cols
    assert "@5" not in cols  # bug 8: n_items=2 clamps out K > n_items


def test_baseline_section_has_two_per_item_compare_tables():
    """recall / map_attr each get a M/B/Δ-interleaved table (ndcg is not shown).

    Titles are matched by prefix, not exact equality: bug 5 (ADR-0020)
    appends a "參與 macro 的 item 數 ..." coverage suffix to these titles
    when both sides have a Macro row (as they do here)."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    assert s is not None
    # Old delta-only title must be gone.
    assert not any(t.startswith("per-item recall@k delta") for t in s.table_titles)
    # Two new titles present, each carrying the bug-5 coverage suffix.
    for prefix in (
        "per-item recall@k (M/B/Δ)",
        "per-item map_attr@k (M/B/Δ)",
    ):
        matches = [t for t in s.table_titles if t.startswith(prefix)]
        assert len(matches) == 1, prefix
        assert "參與 macro 的 item 數" in matches[0]
    assert not any(
        t.startswith("per-item ndcg_attr@k (M/B/Δ)") for t in s.table_titles
    )


def _title_starting_with(titles: list[str], prefix: str) -> str:
    return next(t for t in titles if t.startswith(prefix))


def test_baseline_section_per_item_recall_table_three_cols_per_k():
    """The recall / map_attr per-item M/B/Δ tables share k columns =
    primary_map_k=[1,3,all], but bug 8 (ADR-0020) clamps display columns with
    K > n_items — the fixture has n_items=2, so K=3 is dropped, leaving
    [1, all] (@all still resolves to @2)."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    title = _title_starting_with(s.table_titles, "per-item recall@k (M/B/Δ)")
    tbl = s.tables[s.table_titles.index(title)]
    assert list(tbl.columns) == [
        "recall@1 M", "recall@1 B", "recall@1 Δ",
        "recall@all M", "recall@all B", "recall@all Δ",
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
    # bug 5: title discloses both sides' macro item coverage (fixture: A/B on
    # both sides, no n_pos in either -> default min_positives=0 counts both).
    assert "M 2／B 2／全部 2）" in title


def test_baseline_section_per_item_attr_tables_use_primary_map_k():
    """map_attr / ndcg_attr cols come from primary_map_k = [1, 3, 'all'];
    'all' resolves to n_items (=2 in fixture) for lookup. bug 8 (ADR-0020)
    clamps K=3 out (3 > n_items=2), leaving [1, all]."""
    m = _metrics()
    base = _baseline_metrics_full()
    s = rb.build_baseline_section(m, base, _params())
    title = _title_starting_with(s.table_titles, "per-item map_attr@k (M/B/Δ)")
    tbl = s.tables[s.table_titles.index(title)]
    assert list(tbl.columns) == [
        "map_attr@1 M", "map_attr@1 B", "map_attr@1 Δ",
        "map_attr@all M", "map_attr@all B", "map_attr@all Δ",
    ]
    # n_items=2 means @all → lookup @2. Model A map_attr@2=0.55, Base=0.45.
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
        "dataset_overview": {"totals": {"n_items": 2}},
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
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},
        "evaluation": {"report": {"display": {"primary_map_k": [2]}}},
    }


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

