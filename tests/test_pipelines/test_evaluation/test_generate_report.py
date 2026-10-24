"""generate_report: pure, dict-driven HTML (Plan 1.5 Task 4).

Spark-side toPandas aggregation now lives in ``compute_report_aggregates``;
these tests call it to produce the ``report_aggregates`` payload, then feed
that into ``generate_report`` — exactly the two-step path the pipeline wires.
"""

import copy
import json
import re

import pytest

from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import fingerprint
from recsys_tfb.pipelines.evaluation import create_pipeline
from recsys_tfb.pipelines.evaluation.nodes import (
    compute_report_aggregates,
    compute_row_facts,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    stamp_partition_fingerprint,
)


def _report_node(parameters):
    """``generate_report`` as the pipeline builds it under ``parameters``."""
    pipeline = create_pipeline(
        parameters=parameters,
        post_training=bool(parameters.get("post_training")))
    return next(n for n in pipeline.nodes if n.name == "generate_report")


def generate_report(parameters, **results):
    """Call the main report's node as the Runner does: with exactly the
    results the pipeline wires it to under ``parameters`` (the requirement
    list, #538) — a result a switched-off section alone reads is not passed,
    and passing one here fails, as a missing one does."""
    node = _report_node(parameters)
    assert set(results) == set(node.keyword_inputs) - {"parameters"}, (
        sorted(results), sorted(node.keyword_inputs))
    return node.func(parameters=parameters, **results)


def _params(per_item_breakdown=False):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"snap_date": "20240331", "report": {
            "sections": {"rank_distribution": per_item_breakdown,
                         "popularity_baseline": False},
            "display": {"compare_k": [1]},
            "diagnostics": {"include_distributions": per_item_breakdown}}},
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
        "per_item": {"A": {"recall@1": 0.5, "mean_pos": 1.5}},
        "per_segment": {},
        "population": {"all_k": 2, "macro_items": ["A"],
                       "n_scored_queries_with_positive": 2,
                       "n_scored_queries_without_positive": 0,
                       "segment_columns": []},
    }


def _landed_metrics(params):
    """``_metrics()`` as ``compute_metrics`` lands it under ``params``: with
    the fingerprint ``generate_report`` checks (ADR-0018 decision 2)."""
    return {**_metrics(), "config_fingerprint": fingerprint(params)}


def _landed_row_facts(params):
    """What ``compute_row_facts`` lands for the four-row partition of
    :func:`_eval_pred`, by hand, for the tests that draw no partition; the
    all-positive accounts (#536) by the node's own rule."""
    from recsys_tfb.evaluation.metrics import all_positive_accounts

    counts = {
        "dataset_overview": {
            "totals": {"n_rows": 4, "n_entities": 2, "n_items": 2,
                       "n_snap_dates": 1, "n_positives": 2,
                       "positive_rate": 0.5,
                       "avg_positives_per_entity": 1.0},
            "by_snap_date": {}, "by_item": {}},
        "n_queries": 2, "n_excluded_queries": 0,
        "n_all_positive_queries": 0, "widest_group_rows": 2,
    }
    return {
        **counts, **all_positive_accounts(counts, params),
        "segments": _landed_segments(params),
        "config_fingerprint": fingerprint(params),
    }


def _landed_after_the_metrics(metrics, baseline, params):
    """What ``compare_to_baseline`` and ``resolve_display_ks`` land off
    ``metrics`` and ``baseline`` under ``params`` (#536), by those nodes:
    the report's two inputs that are neither computed on Spark nor
    hand-written. ``baseline`` ``None``: the popularity-baseline section is
    off, there is no comparison, and the cut reads the metrics alone."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        compare_to_baseline,
        resolve_display_ks,
    )

    if baseline is None:
        return {"evaluation_display_ks": resolve_display_ks(metrics, params)}
    comparison = compare_to_baseline(metrics, baseline, params)
    return {"baseline_comparison": comparison,
            "evaluation_display_ks": resolve_display_ks(
                metrics, params, baseline_comparison=comparison)}


def _row_facts_of(eval_pred, params):
    """The row facts of ``eval_pred`` as the real node lands them."""
    return compute_row_facts(eval_pred, _landed_segments(params), params)


def test_generate_report_html_no_diagnostics(spark):
    params = _params(False)
    eval_pred = _eval_pred(spark)
    html = generate_report(
        evaluation_row_facts=_row_facts_of(eval_pred, params),
        evaluation_metrics=_landed_metrics(params),
        **_landed_after_the_metrics(
            _landed_metrics(params), None, params),
        parameters=params,
    )
    assert html.startswith("<!DOCTYPE html>")
    assert "概覽" in html
    # diagnostics off → 沒有診斷 section；也沒有圖，所以沒有收起的東西。
    assert "<details" not in html


def test_generate_report_with_diagnostics(spark):
    params = _params(True)
    eval_pred = _eval_pred(spark)
    aggregates = compute_report_aggregates(
        eval_pred, _landed_segments(params), params)
    assert aggregates["config_fingerprint"] == fingerprint(params)
    html = generate_report(
        evaluation_row_facts=_row_facts_of(eval_pred, params),
        evaluation_metrics=_landed_metrics(params),
        evaluation_report_aggregates=aggregates,
        **_landed_after_the_metrics(
            _landed_metrics(params), None, params),
        parameters=params,
    )
    # 名次分布段畫了：八張圖，每張的數字表收在圖底下，而且每個都關上了（沒關
    # 的 <details> 會吞掉後面的段）；圖以外沒有表。
    section = html[html.index('id="s-rank_distribution"'):]
    section = section[:section.index('<div class="section')]
    assert re.findall(r"<details\b[^>]*>", section) == [
        '<details class="fig-data">'] * 8
    assert section.count("</details>") == 8
    assert "名次佔比 · 各 item" in section
    shown = re.sub(r'<details class="fig-data">.*?</details>', "", section,
                   flags=re.S)
    assert '<div class="tblk">' not in shown
    assert html.count("<details") == html.count("</details>")


def test_the_main_report_has_no_diagnosis_section(spark):
    """#532 (ADR-0031 decision 2) removed the diagnosis pages, the report's
    "排序診斷（獨立報表）" link section, the overview row pointing at it and
    the prediction-quality note comparing its AUC with the "item 能力" page.
    ``--post-training`` was the mode that linked to them, so that is the run
    drawn here, with every section that could name them switched on."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        compute_prediction_quality,
    )

    params = _params(True)
    params["post_training"] = True
    params["evaluation"]["report"]["sections"].update(
        {"prediction_quality": True, "popularity_baseline": False})
    params["evaluation"]["prediction_quality"] = {
        "n_bins": 10, "n_display_bins": 5, "top_n": 1}
    eval_pred = stamp_partition_fingerprint(
        _eval_pred(spark).drop("eval_partition_fingerprint"), params, [])
    pq = compute_prediction_quality(
        eval_pred, _landed_segments(params), params)
    aggregates = compute_report_aggregates(
        eval_pred, _landed_segments(params), params)
    html = generate_report(
        evaluation_row_facts=_row_facts_of(eval_pred, params),
        evaluation_metrics=_landed_metrics(params),
        evaluation_report_aggregates=aggregates,
        prediction_quality_metrics=pq,
        **_landed_after_the_metrics(
            _landed_metrics(params), None, params),
        parameters=params,
    )
    # Premise: the sections that used to name the diagnoses were drawn.
    assert ("概覽" in html and 'id="s-prediction_quality"' in html
            and 'id="s-rank_distribution"' in html)
    assert "排序診斷" not in html
    assert "item 能力" not in html
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
    small_pred = _eval_pred_n(spark, 100)
    large_pred = _eval_pred_n(spark, 3000)
    small_aggregates = compute_report_aggregates(
        small_pred, _landed_segments(params), params)
    large_aggregates = compute_report_aggregates(
        large_pred, _landed_segments(params), params)
    small = generate_report(
        evaluation_row_facts=_row_facts_of(small_pred, params),
        evaluation_metrics=_landed_metrics(params),
        evaluation_report_aggregates=small_aggregates,
        **_landed_after_the_metrics(
            _landed_metrics(params), None, params),
        parameters=params,
    )
    large = generate_report(
        evaluation_row_facts=_row_facts_of(large_pred, params),
        evaluation_metrics=_landed_metrics(params),
        evaluation_report_aggregates=large_aggregates,
        **_landed_after_the_metrics(
            _landed_metrics(params), None, params),
        parameters=params,
    )
    assert abs(len(large) - len(small)) < 20000


# =====================================================================
# generate_report 變純函式（Plan 1.5 Task 4）
# =====================================================================


def test_generate_report_takes_no_spark_dataframe():
    """``generate_report`` 是純函式——這是 ``--only-node generate_report`` 不重算任何 Spark 計算就能重繪主報表的前提。

    Since #538 the node takes ``**results``, so its signature says nothing
    about what it is handed: the wiring does. So every result it can be wired
    to — every section on, both modes — must be a landed JSON catalog entry,
    never a Spark table. (The comparison report's node still names its
    parameters; ``test_generate_comparison_report_takes_no_spark_dataframe``
    checks its annotations — on the type object, not its string: ``nodes.py``
    has no postponed annotations, so ``str()`` of the type never contains the
    ``SparkDataFrame`` alias.)
    """
    from pathlib import Path

    import yaml

    catalog = yaml.safe_load(
        (Path(__file__).parents[3] / "conf/base/catalog.yaml").read_text())
    every_section_on = {"evaluation": {"report": {"sections": {
        "prediction_quality": True}}}}
    for post_training in (False, True):
        node = _report_node({**every_section_on, "post_training": post_training})
        wired = sorted(set(node.keyword_inputs) - {"parameters"})
        assert "evaluation_report_aggregates" in wired, wired
        assert "prediction_quality_metrics" in wired, wired
        not_json = {name: catalog.get(name, {}).get("type") for name in wired
                    if catalog.get(name, {}).get("type") != "JSONDataset"}
        assert not not_json, f"generate_report 又收了不是落地 JSON 的輸入：{not_json}"


def test_generate_report_body_has_no_spark_actions():
    """額外要求 A：annotation 可以改而行為不變，反之亦然，所以也查函式體。

    掃 ``inspect.getsource`` 找 Spark action/cache 呼叫的字面樣式
    （``.select(``／``.cache(``／``.unpersist(``）——這些是舊實作裡真正碰
    Spark 的呼叫，新的 ``generate_report`` 不應該再出現它們。
    """
    import inspect

    generate_report = _report_node({}).func

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
    """Every section that computes something of its own off: the report
    reads the row facts, the metrics and the K cut, nothing else."""
    params = _params(False)
    params["evaluation"]["report"]["sections"]["popularity_baseline"] = False
    return params


def _leftover(params):
    """A result an older run landed under ``params`` for a section that is
    off now — or one that was never computed under today's settings."""
    return {**_landed_metrics(params), "popularity_window": {}}


def test_generate_report_refuses_a_computed_setting_flipped_after_the_run():
    """ADR-0020 section 3's demonstration, unit version: computed with
    ``sections.popularity_baseline: false``, flipped back to ``true`` and only
    redrawn, it must raise, not return a report quietly missing its baseline
    section. Switched on, the report reads the baseline's result again —
    here a file an older run left — and today's settings computed none of
    what it reads."""
    computed_with = _params_computed_without_baseline()
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["sections"]["popularity_baseline"] = True
    leftover = _leftover(computed_with)

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(computed_with),
            evaluation_metrics=_landed_metrics(computed_with),
            baseline_metrics=leftover,
            **_landed_after_the_metrics(
                _landed_metrics(computed_with), leftover, computed_with),
            parameters=now,
        )

    msg = str(exc.value)
    assert "evaluation.report.sections.popularity_baseline" in msg
    # compute_row_facts, not compute_baseline_metrics: row_facts.json and
    # metrics.json carry the same fingerprint and sort first, so re-running
    # from the baseline would leave them stale and this raise would repeat
    # (COMPUTED_KEYS).
    assert "--from-node compute_row_facts" in msg


def test_generate_report_refuses_metrics_computed_with_other_settings():
    """``evaluation_metrics`` lands as ``metrics.json`` (ADR-0018 decision 2),
    so a redraw after a computed key changed must refuse it by name, not draw
    old metrics under the new settings."""
    computed_with = _params_computed_without_baseline()
    now = copy.deepcopy(computed_with)
    now["evaluation"]["metric"] = {"min_positives": 5}

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(computed_with),
            evaluation_metrics=_landed_metrics(computed_with),
            **_landed_after_the_metrics(
                _landed_metrics(computed_with), None, computed_with),
            parameters=now,
        )

    msg = str(exc.value)
    assert "evaluation_metrics (written by compute_metrics)" in msg
    assert "--from-node compute_row_facts" in msg


def test_generate_report_refuses_metrics_without_a_fingerprint():
    """Every other input is current; only the metrics predate fingerprints.
    The single stale artifact names its own producer."""
    params = _params_computed_without_baseline()

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(params),
            evaluation_metrics=_metrics(),
            **_landed_after_the_metrics(
                _landed_metrics(params), None, params),
            parameters=params,
        )

    msg = str(exc.value)
    assert ("evaluation_metrics (written by compute_metrics) has no "
            "config_fingerprint") in msg
    assert "--from-node compute_metrics" in msg


def test_generate_report_redraws_when_only_drawn_settings_changed():
    """A changed drawn key only needs a redraw; a fingerprint that counted
    drawn keys would block every layout tweak. Not ``compare_k``: the
    main report's K are cut before it since #536 (the next test).

    The two sections switched off here were the only readers of the K cut,
    so the redraw does not read it at all (#538): a file left for a section
    that is off is never loaded."""
    computed_with = _params_computed_without_baseline()
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["sections"]["data_stats"] = False
    now["evaluation"]["report"]["sections"]["ranking_metrics"] = False

    html = generate_report(
        evaluation_row_facts=_landed_row_facts(computed_with),
        evaluation_metrics=_landed_metrics(computed_with),
        parameters=now,
    )

    assert html.startswith("<!DOCTYPE html>")


def test_generate_report_refuses_a_display_k_changed_after_the_cut():
    """#536: the K each table shows is cut by ``resolve_display_ks`` and
    landed, so a changed ``compare_k`` drawn with only a redraw would
    print the old columns, exit code 0. The cut's fingerprint carries the
    display key, so it alone is stale and the advice is its own node — not
    ``compute_row_facts``: no computed setting moved."""
    computed_with = _params_computed_without_baseline()
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["display"]["compare_k"] = [1, "all"]

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(computed_with),
            evaluation_metrics=_landed_metrics(computed_with),
            **_landed_after_the_metrics(
                _landed_metrics(computed_with), None, computed_with),
            parameters=now,
        )

    msg = str(exc.value)
    assert ("evaluation_display_ks (written by resolve_display_ks) was "
            "computed with different settings") in msg
    assert "evaluation.report.display.compare_k: [1] -> [1, 'all']" in msg
    assert "--from-node resolve_display_ks" in msg


def test_generate_report_refuses_a_comparison_without_a_fingerprint():
    """The comparison is one of the report's landed inputs (#536) and is
    checked like the others: one written without a fingerprint is the single
    stale artifact, and the advice is its own producer."""
    params = _params_computed_without_baseline()
    params["evaluation"]["report"]["sections"]["popularity_baseline"] = True
    baseline = _leftover(params)
    landed = _landed_after_the_metrics(_landed_metrics(params), baseline,
                                       params)
    del landed["baseline_comparison"]["config_fingerprint"]

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(params),
            evaluation_metrics=_landed_metrics(params),
            baseline_metrics=baseline,
            **landed,
            parameters=params,
        )

    msg = str(exc.value)
    assert ("baseline_comparison (written by compare_to_baseline) has no "
            "config_fingerprint") in msg
    assert "--from-node compare_to_baseline" in msg


def test_generate_report_refuses_prediction_quality_switched_on_after_the_run():
    """Computed with the family off (the framework default), switched on and
    only redrawn: today's settings computed no prediction quality — the file
    the report now reads is an older run's — so the redraw must raise rather
    than print a report drawn off it (#381)."""
    computed_with = _params_computed_without_baseline()
    computed_with["evaluation"]["report"]["sections"][
        "prediction_quality"] = False
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["sections"]["prediction_quality"] = True

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(computed_with),
            evaluation_metrics=_landed_metrics(computed_with),
            prediction_quality_metrics={
                "config_fingerprint": fingerprint(computed_with)},
            **_landed_after_the_metrics(
                _landed_metrics(computed_with), None, computed_with),
            parameters=now,
        )

    msg = str(exc.value)
    assert "evaluation.report.sections.prediction_quality" in msg
    assert "--from-node compute_row_facts" in msg


def test_generate_report_refuses_a_prediction_quality_json_without_fingerprint():
    params = _params_computed_without_baseline()
    params["evaluation"]["report"]["sections"]["prediction_quality"] = True

    with pytest.raises(ValueError) as exc:
        generate_report(
            evaluation_row_facts=_landed_row_facts(params),
            evaluation_metrics=_landed_metrics(params),
            prediction_quality_metrics={"overall": {}},
            **_landed_after_the_metrics(
                _landed_metrics(params), None, params),
            parameters=params,
        )

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
    assert pq["config_fingerprint"] == fingerprint(params)
    assert pq["overall"]["summary"]["n"] == 4
    html = generate_report(
        evaluation_row_facts=_row_facts_of(eval_pred, params),
        evaluation_metrics=_landed_metrics(params),
        prediction_quality_metrics=pq,
        **_landed_after_the_metrics(
            _landed_metrics(params), None, params),
        parameters=params,
    )
    assert "預測品質" in html
    # #536: the hand-written payload the report tests draw from holds the
    # node's keys in both blocks the report reads (the display tables).
    from tests.test_evaluation import test_report_builder
    hand = test_report_builder._pq_payload()
    for block in ("overall", "per_item"):
        assert set(hand[block]) == set(pq[block]), block


class TestTheBaselineIsTheModelsComputation:
    """ADR-0031 decision 8: the baseline goes through the model's metrics,
    macro included. These tests draw the baseline section from what the nodes
    compute. Before #535 the report tests drew it from a hand-written bundle
    holding a ``macro_avg`` the baseline never wrote, so the section's Macro
    row passed every test and appeared in no report.

    The data: two query groups, each holding A, B, C, A the one positive in
    both. The model ranks A first in both. The history makes C the most
    popular, then B, then A, so the baseline ranks A last in both — a tie
    broken by item name would have ranked it first. Each group is its own
    segment and the items map to two categories, so every slice the report
    compares is computed.
    """

    @staticmethod
    def _params():
        return {
            "schema": {"columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank"}},
            "evaluation": {
                "snap_date": "2024-03-31",
                "k_values": [1, 2, 3],
                "segment_columns": ["seg"],
                "item_categories": {"enabled": True, "unmapped": "singleton",
                                    "mapping": {"AB": ["A", "B"]}},
                "baseline": {"lookback_months": 12},
                "report": {
                    "sections": {"rank_distribution": False},
                    "display": {"compare_k": [1]}},
            },
        }

    @classmethod
    def _computed(cls, spark):
        """``(params, row facts, model metrics, baseline metrics)`` as the
        three nodes compute them."""
        from recsys_tfb.pipelines.evaluation.nodes import (
            compute_baseline_metrics,
            compute_metrics,
        )

        params = cls._params()
        params["schema"]["categorical_values"] = {"prod_name": ["A", "B", "C"]}
        frame = stamp_partition_fingerprint(spark.createDataFrame(
            [("2024-03-31", "c1", "A", 0.9, 1, 1, "X"),
             ("2024-03-31", "c1", "B", 0.5, 0, 2, "X"),
             ("2024-03-31", "c1", "C", 0.1, 0, 3, "X"),
             ("2024-03-31", "c2", "A", 0.8, 1, 1, "Y"),
             ("2024-03-31", "c2", "B", 0.3, 0, 2, "Y"),
             ("2024-03-31", "c2", "C", 0.2, 0, 3, "Y")],
            schema=["snap_date", "cust_id", "prod_name", "score", "label",
                    "rank", "seg"],
        ), params, ["seg"])
        # Positives in the lookback window: C 3, B 2, A 1.
        history = spark.createDataFrame(
            [("2023-12-31", f"h{i}", item, int(i < n))
             for i in range(3) for item, n in (("A", 1), ("B", 2), ("C", 3))],
            schema=["snap_date", "cust_id", "prod_name", "label"],
        )
        segments = {**_landed_segments(params), "joined": ["seg"],
                    "sources": {"seg": "sample_pool"}}
        facts = compute_row_facts(frame, segments, params)
        metrics = compute_metrics(frame, facts, segments, params)
        baseline = compute_baseline_metrics(
            frame, facts, history, segments, params)
        return params, facts, metrics, baseline

    def test_the_baseline_section_draws_the_baseline_s_macro(self, spark):
        """Per-item recall@1 of A, the one item with a positive: 1 for the
        model, 0 for the baseline; so is each Macro."""
        import re

        params, facts, metrics, baseline = self._computed(spark)
        html = generate_report(
            evaluation_row_facts=facts,
            evaluation_metrics=metrics,
            baseline_metrics=baseline,
                **_landed_after_the_metrics(
                metrics, baseline, params),
            parameters=params,
        )
        # The ranking-metrics section has a table of the same title (one
        # side): read the popularity-baseline section's.
        section = html[html.index('id="s-popularity_baseline"'):]
        table = section[section.index(
            '<h4 class="tt">per-item recall@K · 各 item</h4>'):]
        table = table[:table.index("</table>")]
        assert '<tr class="pin"><th class="rl">Macro 平均</th>' in table, table
        macro = table[table.index('<th class="rl">Macro 平均</th>'):]
        cells = re.findall(r'data-v="([^"]*)"', macro[:macro.index("</tr>")])
        # recall@1 這次, 對照, 差 (compare_k [1]).
        assert cells == ["1.000000", "0.000000", "1.000000"]

    def _drawn_from(self, params, facts, metrics, baseline, landed):
        """``generate_report`` on the nodes' results, the comparison and the
        cut read back from JSON as ``--only-node generate_report`` reads
        them."""
        return generate_report(
            evaluation_row_facts=facts,
            evaluation_metrics=metrics,
            baseline_metrics=baseline,
                **json.loads(json.dumps(landed)),
            parameters=params,
        )

    def test_a_comparison_without_a_block_the_section_draws_is_refused(
            self, spark):
        """#536 acceptance 4: a comparison landed without the per-segment
        block — by an older compare_to_baseline — must not leave the
        per-segment table out without a word, or stop on a bare KeyError.
        Its fingerprint still matches: no setting changed."""
        params, facts, metrics, baseline = self._computed(spark)
        landed = _landed_after_the_metrics(metrics, baseline, params)
        assert "map@K · 整體與各分群" in self._drawn_from(
            params, facts, metrics, baseline, landed)   # drawn when there
        del landed["baseline_comparison"]["per_segment"]
        with pytest.raises(ValueError) as exc:
            self._drawn_from(params, facts, metrics, baseline, landed)
        assert "baseline_comparison has no per_segment" in str(exc.value)
        assert "--from-node compare_to_baseline" in str(exc.value)

    def test_a_cut_without_a_table_s_k_is_refused(self, spark):
        """The same for the K cut: the metrics have a category grain, the
        cut landed no K for its tables."""
        params, facts, metrics, baseline = self._computed(spark)
        landed = _landed_after_the_metrics(metrics, baseline, params)
        landed["evaluation_display_ks"]["evaluation_metrics"]["category"] = None
        with pytest.raises(ValueError) as exc:
            self._drawn_from(params, facts, metrics, baseline, landed)
        assert ("evaluation_display_ks has no evaluation_metrics.category."
                "metrics_section") in str(exc.value)
        assert "--from-node resolve_display_ks" in str(exc.value)

    @pytest.mark.parametrize("module, fixture", [
        ("tests.test_evaluation.test_report_builder", "_baseline_metrics_full"),
        ("tests.test_evaluation.report_sections.test_popularity_baseline",
         "_baseline")])
    def test_the_hand_written_baseline_holds_only_what_the_node_writes(
            self, spark, module, fixture):
        """The hand-written baselines of the report tests still draw the
        baseline section in the tests that need no Spark. Every key path in
        them must be one ``compute_baseline_metrics`` writes, items,
        segments and months read as any key — the macro
        ``_baseline_metrics_full`` held before #535 was not. Keys only: the
        values there are made up."""
        import importlib

        any_key_below = {"per_item": 1, "per_segment": 1,
                         "per_item_segment": 2, "purchase_counts": 1,
                         "monthly_counts": 2, "window_months_covered": 1}

        def key_paths(bundle, prefix=(), wild=0):
            paths = set()
            for key, value in bundle.items():
                path = prefix + ("*" if wild else key,)
                if isinstance(value, dict) and value:
                    paths |= key_paths(
                        value, path,
                        wild - 1 if wild else any_key_below.get(key, 0))
                else:
                    paths.add(path)
            return paths

        *_, baseline = self._computed(spark)
        hand_written = getattr(importlib.import_module(module), fixture)()
        unwritten = key_paths(hand_written) - key_paths(baseline)
        assert unwritten == set()

    def test_the_hand_written_row_facts_hold_the_keys_the_node_writes(
            self, spark):
        """#536: the report reads the all-positive accounts the row facts
        land, and ``test_report_builder`` draws from hand-written row facts.
        At the item grain they hold exactly the node's keys, bar the two the
        report never reads; at the category grain none it does not write, and
        every account. Keys only: the values there are made up."""
        from recsys_tfb.evaluation.result_readers import (
            ALL_POSITIVE_SHARE_KEY,
            ALL_POSITIVE_WARNS_KEY,
            N_WITH_POSITIVE_KEY,
        )
        from tests.test_evaluation import test_report_builder

        accounts = {N_WITH_POSITIVE_KEY, ALL_POSITIVE_SHARE_KEY,
                    ALL_POSITIVE_WARNS_KEY}
        _, facts, *_ = self._computed(spark)
        hand = test_report_builder._row_facts()
        assert set(hand) == set(facts) - {"segments", "config_fingerprint",
                                          "category"}
        category = test_report_builder._ap_facts(
            category=test_report_builder._ap_category())["category"]
        assert set(category) <= set(facts["category"])
        assert accounts <= set(category)

    def test_the_hand_written_window_holds_the_keys_the_node_writes(
            self, spark):
        """#536: the baseline section reads the window the node lands
        (``popularity_window``), and the report tests draw it from
        ``test_report_builder._windowed``. Both hold the same keys. Keys only:
        the values follow each fixture's own counts."""
        from recsys_tfb.evaluation.result_readers import POPULARITY_WINDOW_KEY
        from tests.test_evaluation import test_report_builder

        *_, baseline = self._computed(spark)
        hand = test_report_builder._baseline_metrics_full()
        assert (set(hand[POPULARITY_WINDOW_KEY])
                == set(baseline[POPULARITY_WINDOW_KEY]))


# =====================================================================
# generate_comparison_report only reads landed results (#536, ADR-0031
# decisions 4 and 5)
# =====================================================================


def _comparison_node(name, compare_only):
    """The node ``name`` as the comparison chain of one compare mode wires
    it (the mode is baked into its pre-check's advice)."""
    from recsys_tfb.pipelines.evaluation import create_pipeline

    pipeline = create_pipeline(
        parameters={},
        compare_source={"kind": "model_version", "model_version": "v0"},
        compare_only=compare_only)
    return next(n for n in pipeline.nodes if n.name == name)


def _compare_params(compare):
    params = _params()
    params["evaluation"]["compare"] = compare
    return params


_EXTERNAL = {
    "kind": "external_hive", "table": "other_project.predictions",
    "label": "ExtX",
    "columns": {"cust_id": "customer_id", "snap_date": "as_of_date",
                "prod_name": "item_code", "score": "pred_score"},
    "prod_mapping": {"ext_a": "A", "ext_b": "B"},
    "unmapped_policy": "fail",
}
_OTHER_MODEL = {"kind": "model_version", "model_version": "v0", "label": "v0"}


def _stamped_for_comparison(result, params):
    """``result`` as a comparison producer lands it under ``params``: the
    fingerprint and the compare source (``compute_compare_metrics`` stamps
    both; ``test_nodes.py::TestComputeCompareMetrics`` pins it)."""
    from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
        compare_source_identity,
    )

    return {**result, "config_fingerprint": fingerprint(params),
            "compare_source": compare_source_identity(params)}


def _comparison_inputs(params, compare_only=False):
    """What ``generate_comparison_report`` reads, landed under ``params``:
    the coverage and A's row facts by hand in their producers' shape, the
    comparison and the K cut by the real nodes, off a bundle holding every
    key the comparison report draws (``tests/test_evaluation/comparison/
    fixtures.bundle``; ``_metrics()`` lacks the ``@all`` keys it refuses to
    draw without, #537) against a weaker one."""
    from tests.test_evaluation.comparison.fixtures import bundle

    comparison = _comparison_node("compare_to_source", compare_only).func(
        _stamped_for_comparison(bundle(0.5), params),
        _stamped_for_comparison(bundle(0.25), params), params)
    coverage = {"kind_a": "model_version", "model_version_a": "(this run)",
                "source_a": "enriched_eval_predictions",
                "kind_b": params["evaluation"]["compare"]["kind"],
                "n_query_group_A_full": 2, "n_query_group_B_full": 2,
                "n_query_group_common": 2, "n_item_A_full": 2,
                "n_item_B_full": 2, "n_item_common": 2,
                "dropped_items_A": [], "dropped_items_B": []}
    return {
        "compare_coverage": _stamped_for_comparison(coverage, params),
        "compare_row_facts_a": _stamped_for_comparison(
            {k: v for k, v in _landed_row_facts(params).items()
             if k not in ("segments", "config_fingerprint")}, params),
        "source_comparison": comparison,
        "comparison_display_ks": _comparison_node(
            "resolve_comparison_display_ks", compare_only).func(
                comparison, params),
    }


def test_generate_comparison_report_takes_no_spark_dataframe():
    """The redraw's premise (ADR-0031 decision 5): nothing it is handed is a
    Spark frame, in either compare mode. Checked on the type object, for
    the reason ``test_generate_report_takes_no_spark_dataframe`` gives."""
    import inspect

    from pyspark.sql import DataFrame as SparkDataFrame

    for compare_only in (False, True):
        node = _comparison_node("generate_comparison_report", compare_only)
        annotations = [p.annotation for p in
                       inspect.signature(node.func).parameters.values()]
        hits = [a for a in annotations if a is SparkDataFrame
                or "pyspark.sql.dataframe.DataFrame" in str(a)
                or "SparkDataFrame" in str(a)]
        assert not hits, (compare_only, hits)


def test_generate_comparison_report_body_has_no_spark_actions():
    """The body too: what the old node ran on every draw (row facts and
    metrics of both sides) must not come back."""
    import inspect

    for compare_only in (False, True):
        source = inspect.getsource(
            _comparison_node("generate_comparison_report", compare_only).func)
        forbidden = [tok for tok in ("compute_all_metrics", "_count_row_facts",
                                     ".select(", ".cache(", ".collect(")
                     if tok in source]
        assert not forbidden, (compare_only, forbidden)


@pytest.mark.parametrize("compare_only", [False, True])
def test_a_redraw_after_prod_mapping_changed_is_refused(compare_only):
    """Unchecked, the old source's numbers would print under the new
    mapping, exit code 0: the fingerprint leaves evaluation.compare out."""
    computed_with = _compare_params(copy.deepcopy(_EXTERNAL))
    landed = _comparison_inputs(computed_with, compare_only)
    now = copy.deepcopy(computed_with)
    now["evaluation"]["compare"]["prod_mapping"]["ext_b"] = "A"

    with pytest.raises(ValueError) as exc:
        _comparison_node("generate_comparison_report", compare_only).func(
            **landed, parameters=now)

    msg = str(exc.value)
    assert "evaluation.compare.prod_mapping.ext_b: 'B' -> 'A'" in msg
    assert "--from-node compute_compare_metrics" in msg


def test_a_redraw_against_another_model_version_is_refused():
    computed_with = _compare_params(dict(_OTHER_MODEL))
    landed = _comparison_inputs(computed_with)
    now = _compare_params({**_OTHER_MODEL, "model_version": "v9"})

    with pytest.raises(ValueError) as exc:
        _comparison_node("generate_comparison_report", False).func(
            **landed, parameters=now)

    msg = str(exc.value)
    assert "evaluation.compare.model_version: 'v0' -> 'v9'" in msg
    assert "--from-node compute_compare_metrics" in msg


def test_a_new_label_redraws_with_the_new_label():
    """``label`` only names the compared source: no re-run, and the report
    prints today's."""
    computed_with = _compare_params(dict(_OTHER_MODEL))
    landed = _comparison_inputs(computed_with)
    now = _compare_params({**_OTHER_MODEL, "label": "last quarter's model"})

    html = _comparison_node("generate_comparison_report", False).func(
        **landed, parameters=now)

    assert "<tr><th>對照</th><td>last quarter's model</td></tr>" in html
    assert "<tr><th>對照</th><td>v0</td></tr>" not in html


def test_compare_only_sends_a_changed_computed_setting_to_a_node_it_has():
    """ADR-0031 decision 15, first row: ``--compare-only`` has neither
    ``prepare_eval_data`` nor ``compute_row_facts``, the nodes
    ``COMPUTED_KEYS`` names, so the advice must be one of its own — and
    ``--compare`` keeps the ``COMPUTED_KEYS`` node, whose slice also
    re-runs the main line the change made stale."""
    import re

    from recsys_tfb.pipelines.evaluation import create_pipeline

    computed_with = _compare_params(dict(_OTHER_MODEL))
    now = copy.deepcopy(computed_with)
    now["evaluation"]["metric"] = {"min_positives": 5}
    for compare_only, expected in ((True, "compute_compare_metrics"),
                                   (False, "compute_row_facts")):
        landed = _comparison_inputs(computed_with, compare_only)
        with pytest.raises(ValueError) as exc:
            _comparison_node("generate_comparison_report", compare_only).func(
                **landed, parameters=now)
        msg = str(exc.value)
        assert "evaluation.metric.min_positives" in msg, compare_only
        advised = re.findall(r"--from-node (\w+)", msg)
        assert advised == [expected], (compare_only, advised)
        names = {n.name for n in create_pipeline(
            parameters={}, compare_source=_OTHER_MODEL,
            compare_only=compare_only).nodes}
        assert expected in names, (compare_only, expected)


@pytest.mark.parametrize("compare_only, key, change, expected", [
    # --compare-only has no main line: the cut's own node is the narrowest.
    (True, "compare_k", [1, 2], "resolve_comparison_display_ks"),
    # --compare: the slice from the cut ends in generate_report, which
    # refuses display_ks.json, stale under compare_k too. The advice is
    # the node before both cuts.
    (False, "compare_k", [1, 2], "resolve_display_ks"),
])
def test_a_changed_display_k_sends_the_comparison_redraw_to_a_slice_that_draws(
        compare_only, key, change, expected):
    """The comparison report's K are cut by ``resolve_comparison_display_ks``
    and landed; its fingerprint carries the display keys it read, so it alone
    is stale. The advice names the node of the run's mode whose slice also
    re-runs everything else a draw needs, without a Spark node."""
    import re
    from pathlib import Path

    import yaml

    from recsys_tfb.pipelines.evaluation import create_pipeline

    computed_with = _compare_params(dict(_OTHER_MODEL))
    landed = _comparison_inputs(computed_with, compare_only)
    now = copy.deepcopy(computed_with)
    now["evaluation"]["report"]["display"][key] = change

    with pytest.raises(ValueError) as exc:
        _comparison_node("generate_comparison_report", compare_only).func(
            **landed, parameters=now)

    msg = str(exc.value)
    assert ("comparison_display_ks (written by resolve_comparison_display_ks)"
            " was computed with different settings") in msg
    assert f"evaluation.report.display.{key}: " in msg
    assert re.findall(r"--from-node (\w+)", msg) == [expected]

    pipeline = create_pipeline(
        parameters={}, compare_source=_OTHER_MODEL, compare_only=compare_only)
    catalog = set(yaml.safe_load(
        (Path(__file__).parents[3] / "conf/base/catalog.yaml").read_text()))
    sliced, _plan = pipeline.slice_from(
        expected, lambda name: name in catalog or name == "parameters")
    ran = {n.name for n in sliced.nodes}
    assert {"resolve_comparison_display_ks",
            "generate_comparison_report"} <= ran
    if not compare_only:
        assert {"resolve_display_ks", "generate_report"} <= ran
    spark_nodes = {"prepare_eval_data", "compute_row_facts", "compute_metrics",
                   "compute_baseline_metrics", "compute_compare_metrics",
                   "compute_prediction_quality", "compute_report_aggregates"}
    assert ran & spark_nodes == set(), sorted(ran)


@pytest.mark.parametrize("reader", ["compare_to_source",
                                    "resolve_comparison_display_ks"])
def test_the_comparison_readers_refuse_results_of_another_source(reader):
    """Each reader checks its inputs before computing, so ``--only-node``
    of it after the source changed cannot stamp today's source on the old
    numbers."""
    computed_with = _compare_params(dict(_OTHER_MODEL))
    now = _compare_params({**_OTHER_MODEL, "model_version": "v9"})
    if reader == "compare_to_source":
        inputs = (_stamped_for_comparison(_metrics(), computed_with),
                  _stamped_for_comparison(_metrics(), computed_with))
    else:
        inputs = (_comparison_inputs(computed_with)["source_comparison"],)

    with pytest.raises(ValueError) as exc:
        _comparison_node(reader, False).func(*inputs, now)

    msg = str(exc.value)
    assert "evaluation.compare.model_version: 'v0' -> 'v9'" in msg
    assert "--from-node compute_compare_metrics" in msg


def test_the_comparison_and_its_cut_land_what_they_were_computed_with():
    """Both carry the fingerprint — the cut's with the display keys it read
    — and the compare source; the cut holds the K columns of every per-K
    table, cut at this run's side's "all" (``_metrics()`` holds two items),
    "all" written as that K (#537)."""
    from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
        compare_source_identity,
    )

    params = _compare_params(dict(_OTHER_MODEL))
    params["evaluation"]["report"]["display"] = {"compare_k": [1, 3, "all"]}
    weaker = copy.deepcopy(_metrics())
    weaker["overall"] = {"map@1": 0.25, "map@3": 0.3}
    weaker["population"] = {**weaker["population"], "all_k": 3}
    comparison = _comparison_node("compare_to_source", False).func(
        _stamped_for_comparison(_metrics(), params),
        _stamped_for_comparison(weaker, params), params)
    cut = _comparison_node("resolve_comparison_display_ks", False).func(
        comparison, params)

    identity = compare_source_identity(params)
    assert comparison["config_fingerprint"] == fingerprint(params)
    assert comparison["compare_source"] == identity
    assert comparison["overall"]["cells"]["map@1"] == {
        "this": 0.5, "reference": 0.25, "diff": 0.25}
    assert cut == {
        "all_k": 2,
        "compare_k": [1, 2],
        "category": None,
        "config_fingerprint": fingerprint(
            params, ("evaluation.report.display.compare_k",)),
        "compare_source": identity,
    }
