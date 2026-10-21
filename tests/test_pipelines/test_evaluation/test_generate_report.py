"""generate_report: pure, dict-driven HTML (Plan 1.5 Task 4).

Spark-side toPandas aggregation now lives in ``compute_report_aggregates``;
these tests call it to produce the ``report_aggregates`` payload, then feed
that into ``generate_report`` — exactly the two-step path the pipeline wires.
"""

import copy
import json

import pytest

from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
from recsys_tfb.pipelines.evaluation.nodes import (
    compute_report_aggregates,
    generate_report,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    stamp_partition_fingerprint,
)


def _unread_stub(params):
    """A disabled baseline / prediction-quality payload carrying ``params``'
    fingerprint.

    ``generate_report`` checks every landed input's fingerprint, so the old
    ``None`` placeholders no longer pass; this is what the producers write
    when their section is off.
    """
    return {"enabled": False, "config_fingerprint": fingerprint(params)}


def _params(diagnostics=False):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"snap_date": "20240331", "report": {
            "sections": {"per_item_breakdown": diagnostics},
            "display": {"primary_map_k": [1], "guardrail_recall_k": [1]},
            "diagnostics": {"include_distributions": diagnostics}}},
    }


def _eval_pred(spark):
    """The partition as ``prepare_eval_data`` under :func:`_params` writes it,
    settings fingerprint included."""
    return stamp_partition_fingerprint(spark.createDataFrame(
        [("20240331", "c1", "A", 0.9, 1, 1),
         ("20240331", "c1", "B", 0.1, 0, 2),
         ("20240331", "c2", "A", 0.2, 0, 2),
         ("20240331", "c2", "B", 0.8, 1, 1)],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    ), _params(), [])


def _landed_segments(params):
    """``evaluation_segment_columns`` as a run under ``params`` lands it, no
    segment column joined."""
    return {"joined": [], "sources": {}, "missing": {},
            "config_fingerprint": fingerprint(params)}


def _metrics():
    return {
        "overall": {"map@1": 0.5},
        "per_item": {"A": {"hit_rate@1": 0.5, "mean_pos": 1.5}},
        "per_segment": {},
        "dataset_overview": {
            "totals": {"n_rows": 4, "n_entities": 2, "n_items": 2,
                       "n_snap_dates": 1, "n_positives": 2,
                       "positive_rate": 0.5,
                       "avg_positives_per_entity": 1.0},
            "by_snap_date": {}, "by_item": {}},
        "n_queries": 2, "n_excluded_queries": 0,
    }


def _landed_metrics(params):
    """``_metrics()`` as ``compute_metrics`` lands it under ``params``: with
    the fingerprint ``generate_report`` checks (ADR-0018 decision 2)."""
    return {**_metrics(), "config_fingerprint": fingerprint(params)}


def test_generate_report_html_no_diagnostics(spark):
    params = _params(False)
    aggregates = compute_report_aggregates(
        _eval_pred(spark), _landed_segments(params), params)
    html = generate_report(_landed_metrics(params), params,
                            _unread_stub(params),
                            aggregates, _unread_stub(params))
    assert html.startswith("<!DOCTYPE html>")
    assert "概覽" in html
    # diagnostics off → 沒有可收合的診斷 section（<details class="section">）。
    # 注意：明細表級收合 <details class="table-collapse"> 與診斷無關，不在此斷言。
    assert '<details class="section"' not in html


def test_generate_report_with_diagnostics(spark):
    params = _params(True)
    aggregates = compute_report_aggregates(
        _eval_pred(spark), _landed_segments(params), params)
    assert aggregates["config_fingerprint"] == fingerprint(params)
    html = generate_report(_landed_metrics(params), params,
                            _unread_stub(params),
                            aggregates, _unread_stub(params))
    # 診斷升為頂層「per-item 細部拆解」段（非收合 section）；其明細數字表用
    # 逐表收合 <details class="table-collapse">。
    assert "per-item 細部拆解" in html
    assert '<details class="table-collapse"' in html


def test_the_main_report_has_no_diagnosis_section():
    """#532 (ADR-0031 decision 2) removed the diagnosis pages, the report's
    "排序診斷（獨立報表）" link section and the overview row pointing at it.
    ``--post-training`` was the mode that linked to them, so that is the run
    drawn here; the section must not come back under either mode."""
    params = _params_computed_without_baseline()
    params["post_training"] = True
    baseline, aggregates, pq = _landed_inputs(params)
    html = generate_report(_landed_metrics(params), params, baseline,
                           aggregates, pq)
    assert "概覽" in html, "premise: the report was drawn"
    assert "排序診斷" not in html
    assert "diagnosis/" not in html


def _params_diag_full():
    p = _params(True)
    diag = p["evaluation"]["report"]["diagnostics"]
    diag["include_distributions"] = True
    return p


def _eval_pred_n(spark, n_customers):
    rows = []
    for i in range(n_customers):
        s = (i % 100) / 100.0
        rows.append(("20240331", f"c{i}", "A", s, i % 2, 1 if s > 0.5 else 2))
        rows.append(("20240331", f"c{i}", "B", 1.0 - s, (i + 1) % 2, 2 if s > 0.5 else 1))
    return stamp_partition_fingerprint(spark.createDataFrame(
        rows,
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "rank"],
    ), _params(), [])


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
    small_aggregates = compute_report_aggregates(
        _eval_pred_n(spark, 100), _landed_segments(params), params)
    large_aggregates = compute_report_aggregates(
        _eval_pred_n(spark, 3000), _landed_segments(params), params)
    small = generate_report(
        _landed_metrics(params), params, _unread_stub(params),
        small_aggregates, _unread_stub(params),
    )
    large = generate_report(
        _landed_metrics(params), params, _unread_stub(params),
        large_aggregates, _unread_stub(params),
    )
    assert abs(len(large) - len(small)) < 20000


# =====================================================================
# generate_report 變純函式（Plan 1.5 Task 4）
# =====================================================================


def test_generate_report_takes_no_spark_dataframe():
    """``generate_report`` 是純函式——這是 ``--only-node generate_report`` 不重算任何 Spark 計算就能重繪主報表的前提。

    用簽章驗而不是「跑跑看有沒有用到 Spark」：後者在 diagnostics 關閉時會
    假綠（那條路徑本來就不碰 sdf）。

    **不能只用字串比對 ``"SparkDataFrame" in str(annotation)``**：
    ``nodes.py`` 沒有 ``from __future__ import annotations``，所以
    annotation 是活的型別物件，``str()`` 印出的是
    ``"<class 'pyspark.sql.dataframe.DataFrame'>"`` —— 不含字面
    ``"SparkDataFrame"`` 這個 alias 名稱，字串比對永遠命中不到，改壞了也
    測不出來（實測：refactor 前跑這條就已經是綠的）。改用型別本身比對，並
    同時涵蓋兩種可能的 annotation 形式（活物件 或 未來若加了
    postponed-evaluation 字串）。
    """
    import inspect

    from pyspark.sql import DataFrame as SparkDataFrame

    from recsys_tfb.pipelines.evaluation.nodes import generate_report

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

    from recsys_tfb.pipelines.evaluation.nodes import generate_report

    source = inspect.getsource(generate_report)
    forbidden = [tok for tok in (".select(", ".cache(", ".unpersist(")
                 if tok in source]
    assert not forbidden, (
        f"generate_report 函式體仍有 Spark action 呼叫：{forbidden}"
    )


# =====================================================================
# generate_report draws only what the current settings computed
# (#342, ADR-0020 decision 2)
# =====================================================================


def _params_computed_without_baseline():
    params = _params(False)
    params["evaluation"]["report"]["sections"]["baseline"] = False
    return params


def _landed_inputs(params):
    """The three landed inputs besides the metrics, as their producers write
    them under ``params``.

    Every section involved is off, so each producer returns its stub before
    touching a DataFrame and no Spark session is needed.
    """
    from recsys_tfb.pipelines.evaluation.nodes import (
        compute_baseline_metrics,
        compute_prediction_quality,
    )

    return (compute_baseline_metrics(None, None, None, params),
            compute_report_aggregates(None, None, params),
            compute_prediction_quality(None, None, params))


def test_generate_report_refuses_a_computed_setting_flipped_after_the_run():
    """ADR-0020 section 3's demonstration, unit version: computed with
    ``sections.baseline: false``, flipped back to ``true`` and only redrawn,
    it must raise, not return a report quietly missing its baseline section."""
    computed_with = _params_computed_without_baseline()
    baseline, aggregates, pq = _landed_inputs(computed_with)
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["sections"]["baseline"] = True

    with pytest.raises(ValueError) as exc:
        generate_report(_landed_metrics(computed_with), now, baseline,
                        aggregates, pq)

    msg = str(exc.value)
    assert "evaluation.report.sections.baseline" in msg
    # compute_metrics, not compute_baseline_metrics: metrics.json carries the
    # same fingerprint and sorts first, so re-running from the baseline would
    # leave it stale and this raise would repeat (COMPUTED_KEYS).
    assert "--from-node compute_metrics" in msg


def test_generate_report_refuses_metrics_computed_with_other_settings():
    """``evaluation_metrics`` lands as ``metrics.json`` (ADR-0018 decision 2),
    so a redraw after a computed key changed must refuse it by name, not draw
    old metrics under the new settings."""
    computed_with = _params_computed_without_baseline()
    baseline, aggregates, pq = _landed_inputs(computed_with)
    now = copy.deepcopy(computed_with)
    now["evaluation"]["metric"] = {"min_positives": 5}

    with pytest.raises(ValueError) as exc:
        generate_report(_landed_metrics(computed_with), now, baseline,
                        aggregates, pq)

    msg = str(exc.value)
    assert "evaluation_metrics (written by compute_metrics)" in msg
    assert "--from-node compute_metrics" in msg


def test_generate_report_refuses_metrics_without_a_fingerprint():
    """Every other input is current; only the metrics predate fingerprints.
    The single stale artifact names its own producer."""
    params = _params_computed_without_baseline()
    baseline, aggregates, pq = _landed_inputs(params)

    with pytest.raises(ValueError) as exc:
        generate_report(_metrics(), params, baseline, aggregates,
                        pq)

    msg = str(exc.value)
    assert ("evaluation_metrics (written by compute_metrics) has no "
            "config_fingerprint") in msg
    assert "--from-node compute_metrics" in msg


def test_generate_report_redraws_when_only_drawn_settings_changed():
    """A changed drawn key only needs a redraw; a fingerprint that counted
    drawn keys would block every layout tweak."""
    computed_with = _params_computed_without_baseline()
    baseline, aggregates, pq = _landed_inputs(computed_with)
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["display"]["primary_map_k"] = [1, "all"]
    now["evaluation"]["report"]["sections"]["primary_map"] = False

    html = generate_report(_landed_metrics(computed_with), now, baseline,
                           aggregates,
                           pq)

    assert html.startswith("<!DOCTYPE html>")


def test_generate_report_refuses_prediction_quality_switched_on_after_the_run():
    """Computed with the family off (the framework default), switched on and
    only redrawn: the stub on disk says nothing about the rows, so the redraw
    must raise rather than print a report without the section (#381)."""
    computed_with = _params_computed_without_baseline()
    computed_with["evaluation"]["report"]["sections"][
        "prediction_quality"] = False
    baseline, aggregates, pq = _landed_inputs(computed_with)
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["sections"]["prediction_quality"] = True

    with pytest.raises(ValueError) as exc:
        generate_report(_landed_metrics(computed_with), now, baseline,
                        aggregates, pq)

    msg = str(exc.value)
    assert "evaluation.report.sections.prediction_quality" in msg
    assert "--from-node compute_metrics" in msg


def test_generate_report_refuses_a_prediction_quality_json_without_fingerprint():
    params = _params_computed_without_baseline()
    baseline, aggregates, _pq = _landed_inputs(params)

    with pytest.raises(ValueError) as exc:
        generate_report(_landed_metrics(params), params, baseline,
                        aggregates, {"enabled": False})

    assert ("prediction_quality_metrics (written by "
            "compute_prediction_quality) has no config_fingerprint"
            ) in str(exc.value)


def test_generate_report_draws_prediction_quality_from_the_nodes_output(spark):
    """The two-step path the pipeline wires: compute_prediction_quality on
    the partition, its JSON round-tripped, generate_report drawing it."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        compute_prediction_quality,
    )

    params = _params(False)
    params["evaluation"]["report"]["sections"]["prediction_quality"] = True
    params["evaluation"]["prediction_quality"] = {
        "n_bins": 10, "n_display_bins": 5, "top_n": 1}
    eval_pred = stamp_partition_fingerprint(
        _eval_pred(spark).drop("eval_partition_fingerprint"), params, [])
    pq = compute_prediction_quality(
        eval_pred, _landed_segments(params), params)
    pq = json.loads(json.dumps(pq))
    assert pq["enabled"] is True
    assert pq["config_fingerprint"] == fingerprint(params)
    assert pq["overall"]["summary"]["n"] == 4
    aggregates = compute_report_aggregates(
        eval_pred, _landed_segments(params), params)
    html = generate_report(_landed_metrics(params), params,
                           _unread_stub(params),
                           aggregates, pq)
    assert "預測品質" in html
