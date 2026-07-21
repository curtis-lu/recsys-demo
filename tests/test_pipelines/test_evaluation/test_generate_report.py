"""generate_report: pure, dict-driven HTML (Plan 1.5 Task 4).

Spark-side toPandas aggregation now lives in ``compute_report_aggregates``;
these tests call it to produce the ``report_aggregates`` payload, then feed
that into ``generate_report`` — exactly the two-step path the pipeline wires.
"""

import inspect

import numpy as np
import pandas as pd

from recsys_tfb.pipelines.evaluation.nodes_spark import (
    compute_report_aggregates,
    generate_report,
)


def _params(diagnostics=False):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"snap_date": "20240331", "report": {
            "sections": {"diagnostics": diagnostics},
            "display": {"primary_map_k": [1], "guardrail_recall_k": [1]},
            "diagnostics": {"include_distributions": diagnostics,
                            "include_calibration": False}}},
    }


def _eval_pred(spark):
    return spark.createDataFrame(
        [("20240331", "c1", "A", 0.9, 1, 1),
         ("20240331", "c1", "B", 0.1, 0, 2),
         ("20240331", "c2", "A", 0.2, 0, 2),
         ("20240331", "c2", "B", 0.8, 1, 1)],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def _metrics():
    return {
        "overall": {"map@1": 0.5},
        "per_item": {"A": {"hit_rate@1": 0.5, "mean_pos": 1.5}},
        "per_segment": {},
        "dataset_overview": {
            "totals": {"n_rows": 4, "n_customers": 2, "n_products": 2,
                       "n_snap_dates": 1, "n_positives": 2,
                       "positive_rate": 0.5,
                       "avg_positives_per_customer": 1.0},
            "by_snap_date": {}, "by_item": {}},
        "n_queries": 2, "n_excluded_queries": 0,
    }


def test_generate_report_html_no_diagnostics(spark):
    params = _params(False)
    aggregates = compute_report_aggregates(_eval_pred(spark), params)
    html = generate_report(_metrics(), params,
                            None, None, None, aggregates, None)
    assert html.startswith("<!DOCTYPE html>")
    assert "概覽" in html
    # diagnostics off → 沒有可收合的診斷 section（<details class="section">）。
    # 注意：明細表級收合 <details class="table-collapse"> 與診斷無關，不在此斷言。
    assert '<details class="section"' not in html


def test_generate_report_with_diagnostics(spark):
    params = _params(True)
    aggregates = compute_report_aggregates(_eval_pred(spark), params)
    html = generate_report(_metrics(), params,
                            None, None, None, aggregates, None)
    # 診斷升為頂層「per-item 細部拆解」段（非收合 section）；其明細數字表用
    # 逐表收合 <details class="table-collapse">。
    assert "per-item 細部拆解" in html
    assert '<details class="table-collapse"' in html


def _params_diag_full():
    p = _params(True)
    diag = p["evaluation"]["report"]["diagnostics"]
    diag["include_distributions"] = True
    diag["include_calibration"] = True
    diag["n_calibration_bins"] = 5
    return p


def _eval_pred_n(spark, n_customers):
    rows = []
    for i in range(n_customers):
        s = (i % 100) / 100.0
        rows.append(("20240331", f"c{i}", "A", s, i % 2, 1 if s > 0.5 else 2))
        rows.append(("20240331", f"c{i}", "B", 1.0 - s, (i + 1) % 2, 2 if s > 0.5 else 1))
    return spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    )


def test_diagnostics_report_size_bounded_by_row_count(spark):
    """The whole point of Spark-side aggregation: diagnostics figures embed
    aggregated values (bins/quartiles/matrices), so report size must not grow
    with the number of evaluation rows. Raw-array embedding would make the
    large report ~100s of KB bigger.

    The aggregation itself now happens in ``compute_report_aggregates``
    (Plan 1.5); this test exercises that node directly, then checks that
    feeding its bounded JSON into the now-pure ``generate_report`` still
    yields a bounded report.
    """
    params = _params_diag_full()
    small_aggregates = compute_report_aggregates(_eval_pred_n(spark, 100), params)
    large_aggregates = compute_report_aggregates(_eval_pred_n(spark, 3000), params)
    small = generate_report(
        _metrics(), params, None, None, None, small_aggregates, None,
    )
    large = generate_report(
        _metrics(), params, None, None, None, large_aggregates, None,
    )
    assert abs(len(large) - len(small)) < 20000


# =====================================================================
# 診斷多頁輸出（Task 2.5）：報表層與 registry 診斷解耦
# =====================================================================

SAMPLING_DESCRIPTION = "分層抽樣：正例 query 全取，負例 query 依 hash 取 40%。"

_DIAG_PARAMS = {
    "schema": {"time": "snap_date", "entity": ["cust_id"],
               "item": "prod_name", "label": "label", "score": "score"},
    "dataset": {
        "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"mass|ccard_ins|0": 0.5},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": 20},
                                 "config_shift": {"enabled": True}}},
}


def _diag_sample_pdf() -> pd.DataFrame:
    """兩個客群 × 兩個 item 的小樣本（形狀取自 test_config_shift.py）。"""
    rng = np.random.default_rng(0)
    rows = []
    for c in range(40):
        segment = "mass" if c % 2 == 0 else "affluent"
        for item in ("ccard_ins", "fund_bond"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c}",
                "prod_name": item, "cust_segment_typ": segment,
                "label": int((item == "ccard_ins" and c % 2 == 0)
                             or (item == "fund_bond" and c % 5 == 0)),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
            })
    return pd.DataFrame(rows)


def _diag_results() -> dict:
    """用**真的** ``config_shift.compute`` 產生 fixture，不手刻 dict。

    手刻的 22 個頂層鍵會跟 ``compute`` 的實際輸出漂移，而漂移的方向剛好是
    「測試綠、real-run 紅」——組裝層讀的每個鍵都必須是計算層真的會給的。
    """
    from recsys_tfb.diagnosis.metric import config_shift

    sample_meta = {"n_queries": 40,
                   "sampling_description": SAMPLING_DESCRIPTION}
    return {"config_shift": config_shift.compute(
        (_diag_sample_pdf(), sample_meta), _DIAG_PARAMS)}


def test_report_builder_has_no_per_diagnosis_builders():
    """報表層不得認識任何 registry 診斷——這是解耦的驗收條件。

    禁用清單動態從 DIAGNOSES 導出：Plan 2-5 每加一項診斷這條自動收緊。
    舊的 offset_sweep builder 不在管轄範圍（它服務的是尚未被取代的既有
    診斷，Plan 5 收尾才清）。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.evaluation import report_builder

    names = [n for n, _ in inspect.getmembers(report_builder, inspect.isfunction)]
    forbidden = [n for n in names
                 if n.startswith("build_") and any(d in n for d in DIAGNOSES)]
    assert not forbidden, f"report_builder 仍認識個別 registry 診斷：{forbidden}"


def test_diagnosis_pages_written(tmp_path):
    from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages

    written = assemble_diagnosis_pages(_diag_results(), {}, out_dir=tmp_path)
    names = sorted(p.name for p in written)
    assert "01-config-shift.html" in names
    assert "index.html" in names
    assert "plotly.min.js" in names


def test_index_intro_states_order_is_not_a_gate():
    """閱讀順序不是硬閘門——五項都會跑、都會呈現，前一項擋不掉後一項。

    沒有這句，編號會被讀成「第 1 項沒事就不必看後面」，而那正是這份索引
    要避免的誤讀。
    """
    from recsys_tfb.evaluation.report_builder import _diagnosis_index_intro

    intro = _diagnosis_index_intro()
    assert "不是硬閘門" in intro


def test_index_intro_has_no_verdict_vocabulary():
    """鐵則 1（不下結論）同樣適用於索引頁的說明文字本身。"""
    from recsys_tfb.evaluation.report_builder import _diagnosis_index_intro

    forbidden = ["建議", "應該", "異常", "不足", "有問題", "健康",
                 "通過", "失敗", "verdict", "severity", "recommend"]
    intro = _diagnosis_index_intro()
    hits = [word for word in forbidden if word in intro]
    assert not hits, f"索引說明出現下結論字眼：{hits}"


def test_unimplemented_diagnoses_produce_no_page(tmp_path):
    """``results`` 只有 config_shift 時，不生出其他頁、也不 raise。

    未實作的診斷在 ``results`` 裡就是缺席，組裝層必須把缺席當成「這頁不
    存在」而不是「這頁是空的」——空頁看起來像「量到了、結果什麼都沒有」。
    """
    from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages

    written = assemble_diagnosis_pages(_diag_results(), {}, out_dir=tmp_path)
    page_names = sorted(p.name for p in written if p.suffix == ".html")
    assert page_names == ["01-config-shift.html", "index.html"]


def test_sampling_description_flows_into_page_scope(tmp_path):
    """``sample_meta["sampling_description"]`` 必須進到該頁的 ScopeNote。

    這條守的是組裝層那一句 ``dataclasses.replace``：模組層級的 ``SCOPE``
    刻意留空 ``sampling``（執行期事實不寫死進常數），填值只發生在這裡。
    """
    from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages

    written = assemble_diagnosis_pages(_diag_results(), {}, out_dir=tmp_path)
    page = next(p for p in written if p.name == "01-config-shift.html")
    html = page.read_text()
    assert SAMPLING_DESCRIPTION in html
    assert "抽樣設計" in html


def test_main_report_links_out_without_duplicating_numbers(tmp_path):
    """主報表只給入口，數字留在專屬頁——複製一份就會有兩個真實來源。"""
    from recsys_tfb.evaluation.report_builder import (
        assemble_diagnosis_pages,
        build_diagnosis_links_section,
    )

    written = assemble_diagnosis_pages(_diag_results(), {}, out_dir=tmp_path)
    section = build_diagnosis_links_section(written, {})
    text = "\n".join(
        [section.title, section.description, *section.table_titles]
        + [t.to_string() for t in section.tables]
    )
    assert "diagnosis/index.html" in text
    for number_word in ("delta", "Δ", "offset", "mAP"):
        assert number_word not in text, f"主報表複製了診斷數字：{number_word}"


def test_no_diagnosis_pages_means_no_links_section():
    """一頁都沒寫出來時不放入口——連出去是 404 的連結比沒有連結更糟。"""
    from recsys_tfb.evaluation.report_builder import (
        build_diagnosis_links_section,
    )

    assert build_diagnosis_links_section([], {}) is None


# =====================================================================
# generate_report 變純函式（Plan 1.5 Task 4）
# =====================================================================


def test_generate_report_takes_no_spark_dataframe():
    """``generate_report`` 是純函式——這是主報表能離線重繪的前提。

    用簽章驗而不是「跑跑看有沒有用到 Spark」：後者在 diagnostics 關閉時會
    假綠（那條路徑本來就不碰 sdf）。

    **不能只用字串比對 ``"SparkDataFrame" in str(annotation)``**：
    ``nodes_spark.py`` 沒有 ``from __future__ import annotations``，所以
    annotation 是活的型別物件，``str()`` 印出的是
    ``"<class 'pyspark.sql.dataframe.DataFrame'>"`` —— 不含字面
    ``"SparkDataFrame"`` 這個 alias 名稱，字串比對永遠命中不到，改壞了也
    測不出來（實測：refactor 前跑這條就已經是綠的）。改用型別本身比對，並
    同時涵蓋兩種可能的 annotation 形式（活物件 或 未來若加了
    postponed-evaluation 字串）。
    """
    import inspect

    from pyspark.sql import DataFrame as SparkDataFrame

    from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report

    def _is_spark_dataframe(annotation) -> bool:
        if annotation is SparkDataFrame:
            return True
        return "pyspark.sql.dataframe.DataFrame" in str(annotation) \
            or "SparkDataFrame" in str(annotation)

    annotations = [
        p.annotation for p in
        inspect.signature(generate_report).parameters.values()
    ]
    hits = [a for a in annotations if _is_spark_dataframe(a)]
    assert not hits, f"generate_report 又收了 Spark 物件：{hits}"


def test_generate_report_body_has_no_spark_actions():
    """額外要求 A：annotation 可以改而行為不變，反之亦然，所以也查函式體。

    掃 ``inspect.getsource`` 找 Spark action/cache 呼叫的字面樣式
    （``.select(``／``.cache(``／``.unpersist(``）——這些是舊實作裡真正碰
    Spark 的呼叫，新的 ``generate_report`` 不應該再出現它們。
    """
    import inspect

    from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report

    source = inspect.getsource(generate_report)
    forbidden = [tok for tok in (".select(", ".cache(", ".unpersist(")
                 if tok in source]
    assert not forbidden, (
        f"generate_report 函式體仍有 Spark action 呼叫：{forbidden}"
    )
