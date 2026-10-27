"""Pure-dict tests for the main report: its sections (``report/sections``) and
their assembly (``report/main.py``), no Spark."""

import re

import pandas as pd
import pytest

from recsys_tfb.pipelines.evaluation.report import main as rb
from recsys_tfb.pipelines.evaluation.report.sections import (
    completeness,
    core_concepts,
    data_stats,
    ranking_metrics,
    shared,
)

#: The results ``generate_report`` hands the report, by catalog name.
_RESULT_NAMES = ("evaluation_row_facts", "evaluation_metrics",
                 "baseline_metrics", "evaluation_report_aggregates")

#: The two results the nodes between the metrics and the report land off
#: them (#536): the comparison with the baseline and the K cut.
_LANDED_OFF_THEM = ("baseline_comparison", "evaluation_display_ks")


def _results(**by_name) -> dict:
    """The report's input bundle, fed by name; a result not given is ``None``
    (what a caller passing no baseline / aggregates hands over). The row facts default to ``_row_facts()``: a test that
    changes a row fact (scale, group counts, segments, category) passes its
    own. The comparison and the K cut are left out unless given: they depend
    on the settings, so :func:`_draw` / :func:`_assemble` land them when the
    report is drawn."""
    unknown = set(by_name) - set(_RESULT_NAMES) - set(_LANDED_OFF_THEM)
    assert not unknown, unknown
    out = {name: by_name.get(name) for name in _RESULT_NAMES}
    out.update({name: by_name[name] for name in _LANDED_OFF_THEM
                if name in by_name})
    if "evaluation_row_facts" not in by_name:
        out["evaluation_row_facts"] = _row_facts()
    return out


def _stamped(result: dict, parameters: dict, node: str) -> dict:
    """``result`` as ``node`` lands it under ``parameters``: with its
    fingerprint, which the comparison and the K cut nodes check before
    reading it."""
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
        fingerprint,
    )

    return {**result, "config_fingerprint": fingerprint(parameters, node)}


def _with_landed(results: dict, parameters: dict) -> dict:
    """``results`` plus what ``compare_to_baseline`` and
    ``resolve_display_ks`` land off its metrics under ``parameters`` —
    computed by those nodes, at draw time, so a test that changes the
    metrics or the settings after building ``results`` draws what a run
    with them would. A result the test handed over by name is kept. No
    model metrics, no comparison and no cut (a section test that reads
    neither); no baseline, the stub the comparison node writes for one."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        compare_to_baseline,
        resolve_display_ks,
    )

    out = dict(results)
    facts = out.get("evaluation_row_facts")
    roles = parameters.get("schema", {}).get("columns", {})
    if isinstance(facts, dict) and ("occasion" in roles or "event" in roles):
        # What compute_dataset_overview lands for a declared role; the
        # hand-written facts stand in for it (a declared role without it is
        # refused, shared.refuse_pre_landed_role_totals).
        overview = dict(facts.get("dataset_overview") or {})
        totals = dict(overview.get("totals") or {})
        if "occasion" in roles:
            totals.setdefault("n_time_entity_combos", 3)
        if "event" in roles:
            totals.setdefault("n_query_group_item_combos", 5)
        out["evaluation_row_facts"] = {
            **facts, "dataset_overview": {**overview, "totals": totals}}
    metrics = out.get("evaluation_metrics")
    if "baseline_comparison" not in out:
        baseline = out.get("baseline_metrics")
        if (metrics is not None and isinstance(baseline, dict)
                and baseline.get("enabled") is not False
                and "population" not in baseline):
            # A hand-written baseline without a ledger compares with the one
            # compute_baseline_metrics lands for it: the model's, by
            # construction (it re-scores the model's rows, counted off their
            # row facts — ADR-0031 decision 4, correction).
            baseline = {**baseline, "population": metrics["population"]}
        out["baseline_comparison"] = (
            None if metrics is None or baseline is None else
            compare_to_baseline(
                _stamped(metrics, parameters, "compute_metrics"),
                _stamped(baseline, parameters, "compute_baseline_metrics"),
                parameters))
    if "evaluation_display_ks" not in out and metrics is not None:
        comparison = out["baseline_comparison"]
        out["evaluation_display_ks"] = (
            resolve_display_ks(
                _stamped(metrics, parameters, "compute_metrics"), parameters)
            if comparison is None else
            resolve_display_ks(
                _stamped(metrics, parameters, "compute_metrics"), parameters,
                baseline_comparison=_stamped(
                    comparison, parameters, "compare_to_baseline")))
    return out


def _draw(section, results: dict, parameters: dict):
    """``section.build`` on ``results`` and what the nodes land off them
    (:func:`_with_landed`)."""
    return section.build(_with_landed(results, parameters), parameters)


#: The results only one switched section reads, and its switch.
_SWITCH_OF = {"baseline_metrics": "popularity_baseline",
              "evaluation_report_aggregates": "rank_distribution"}


def _as_the_pipeline_wires(results: dict, parameters: dict) -> dict:
    """``parameters`` with the switch of each result of :data:`_SWITCH_OF`
    set the way the test's ``results`` say, unless the test set it: a result
    left ``None`` is a section this run does not draw — the pipeline neither
    computes nor passes its result (#538) — and a result given is one it
    does."""
    import copy

    params = copy.deepcopy(parameters)
    sections = (params.setdefault("evaluation", {})
                .setdefault("report", {}).setdefault("sections", {}))
    for name, switch in _SWITCH_OF.items():
        sections.setdefault(switch, results.get(name) is not None)
    return params


def _assemble(results: dict, parameters: dict) -> str:
    """``rb.assemble_report`` on ``results`` and what the nodes land off them
    (:func:`_with_landed`), the switches set as the results say
    (:func:`_as_the_pipeline_wires`)."""
    params = _as_the_pipeline_wires(results, parameters)
    return rb.assemble_report(_with_landed(results, params), params)


def _params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"report": {"display": {
            "compare_k": [1, 3, "all"]}}}}


def _metrics():
    # The query-group K grid always holds all_k (2 here) since #537
    # (metrics_spark._resolve_k_grids): map/precision/recall@2 are in it.
    return {
        "overall": {"map@1": 0.5, "map@2": 0.55, "map@3": 0.6, "map@5": 0.65,
                    "map@10": 0.7, "precision@1": 0.4, "precision@2": 0.35,
                    "recall@1": 0.3, "recall@2": 0.45},
        "per_item": {"A": {"recall@1": 0.2, "recall@2": 0.4,
                           "mean_pos": 3.0,
                           "map_attr@1": 0.5, "map_attr@3": 0.6,
                           "map_attr@2": 0.55},
                     "B": {"recall@1": 0.1, "recall@2": 0.3,
                           "mean_pos": 5.0,
                           "map_attr@1": 0.3, "map_attr@3": 0.35,
                           "map_attr@2": 0.32}},
        "macro_avg": {
            "by_item": {
                "recall@1": 0.15, "recall@2": 0.35, "mean_pos": 4.0,
                "map_attr@1": 0.4, "map_attr@2": 0.435, "map_attr@3": 0.475,
            },
        },
        "population": {"all_k": 2, "macro_items": ["A", "B"],
                       "n_scored_queries_with_positive": 10,
                       "n_scored_queries_without_positive": 0,
                       "segment_columns": []},
    }


def _full_ledger(all_k, macro_items, segment_columns=()):
    """A population ledger holding every key ``compute_all_metrics`` writes
    (#536 added ``segment_columns``, which the comparison layer reads); the
    group counts are made up like the rest of each fixture."""
    return {"all_k": all_k, "macro_items": list(macro_items),
            "n_scored_queries_with_positive": 10,
            "n_scored_queries_without_positive": 0,
            "segment_columns": list(segment_columns)}


def _accounted(facts, parameters=None):
    """``facts`` with the all-positive accounts ``compute_row_facts`` lands
    beside the group counts (#536), at each grain, by the node's own rule
    (``metrics.all_positive_accounts``) under ``parameters``: a fixture whose
    counts change must not keep accounts those counts would not give."""
    from recsys_tfb.evaluation.metrics import all_positive_accounts

    out = {**facts, **all_positive_accounts(facts, parameters or {})}
    if facts.get("category"):
        out["category"] = _accounted(facts["category"], parameters)
    return out


def _row_facts():
    """The row facts matching ``_metrics()``: the facts about the rows (scale,
    group counts), kept apart from the metric values."""
    return _accounted({
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
        "n_queries": 10, "n_excluded_queries": 0,
        "n_all_positive_queries": 0, "widest_group_rows": 2,
    })




def _metrics_with_n_items(n):
    m = _metrics()
    m["population"]["all_k"] = n
    return m


def _row_facts_with_n_items(n):
    rf = _row_facts()
    rf["dataset_overview"]["totals"]["n_items"] = n
    return rf


def _row_facts_with_category(overview):
    """``_row_facts()`` plus the category grain's own overview."""
    rf = _row_facts()
    rf["category"] = {"dataset_overview": overview}
    return _accounted(rf)


def _results_with_n_items(n):
    """Metrics and row facts that agree on the item count."""
    return _results(evaluation_row_facts=_row_facts_with_n_items(n),
                    evaluation_metrics=_metrics_with_n_items(n))






# ---- Task 5: 衡量指標（合併 primary_map/guardrail/attr/segment/category）----


def test_a_switch_outside_the_declared_set_is_refused():
    """Pre-check (ADR-0019 decision 6): a switch a section declares must be
    one A34 keeps declared. Without it a section with ``SWITCH = "x"`` reads
    a key no conf declares and silently defaults to on — the
    ``diagnosis_links`` drift. The check is in the one reading of a switch,
    so the requirement list and the report both hit it."""
    from types import SimpleNamespace

    with pytest.raises(ValueError, match="'per_segment' is not in"):
        shared.sections_on((SimpleNamespace(SWITCH="per_segment"),),
                           _params())


def test_every_listed_section_is_read_by_the_report():
    """The third side of A34 (ADR-0019 decision 6). A34 keeps the YAML equal to
    ``EVALUATION_REPORT_SECTIONS`` and ``report_section_on`` keeps the report's names
    inside it, but neither notices a name left in the constant after the
    section that declared it is deleted: a dead switch again, everything
    green. The names here come from the sections' ``SWITCH`` declarations
    (#538), not from the constant: every section of the page-order list,
    and every module of ``report/sections`` that declares one, globbed so a
    section left out of the list is caught too."""
    import importlib
    from pathlib import Path

    from recsys_tfb.core.consistency import EVALUATION_REPORT_SECTIONS

    listed = {s.SWITCH for s in rb.SECTIONS if s.SWITCH is not None}
    declared = set()
    for source in Path(shared.__file__).parent.glob("*.py"):
        module = importlib.import_module(
            f"recsys_tfb.pipelines.evaluation.report.sections.{source.stem}")
        if getattr(module, "SWITCH", None) is not None:
            declared.add(module.SWITCH)
    assert listed == declared == EVALUATION_REPORT_SECTIONS


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


# ---- Task 9: 完整性檢查 ----


class TestLegacyDatasetOverviewRefused:
    """讀到 #327 之前的 dataset_overview 要 raise，不做靜默 fallback。

    The overview now lives in row_facts.json, so the legacy keys go into the
    row facts."""

    def _legacy(self, **overrides):
        row_facts = _row_facts()
        overview = row_facts["dataset_overview"]
        overview["totals"].pop("n_items")
        overview["totals"]["n_products"] = 2
        overview.update(overrides)
        return row_facts

    def test_a_legacy_totals_key_raises_and_names_the_script(self):
        with pytest.raises(ValueError) as exc:
            shared.read_dataset_overview(self._legacy())
        message = str(exc.value)
        assert "n_products" in message
        assert shared.MIGRATION_SCRIPT in message

    def test_the_named_script_exists(self):
        """訊息叫人跑的那支腳本要真的在那個路徑：搬家時只有這條會紅。"""
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[4]
        assert (repo_root / shared.MIGRATION_SCRIPT).is_file(), shared.MIGRATION_SCRIPT

    def test_a_legacy_key_hiding_in_a_cell_is_found_too(self):
        """totals 可能已被手動改對，而 by_* 的 cell 沒有。

        只看 totals 的檢查會放行那種半遷移的檔案，而它的 by_item 表會印出一欄
        ``n_customers``——舊詞彙，零錯誤訊息。
        """
        row_facts = _row_facts()
        row_facts["dataset_overview"]["by_item"]["A"]["n_customers"] = 10
        with pytest.raises(ValueError, match="n_customers"):
            shared.read_dataset_overview(row_facts)

    def test_the_section_builders_refuse_the_same_file(self):
        """護欄擋在存取器上，所以每個讀取端都跟著擋——含比較報表那一條。"""
        from recsys_tfb.pipelines.evaluation.report.comparison import per_item_metrics as cmp_items

        legacy = self._legacy()
        for call in (
            lambda: core_concepts.build(
                _results(evaluation_row_facts=legacy,
                         evaluation_metrics=_metrics()), _params()),
            lambda: data_stats.build(_results(evaluation_row_facts=legacy),
                                     _params()),
            lambda: cmp_items.count_items(legacy),
        ):
            with pytest.raises(ValueError, match="predates the #327 key rename"):
                call()

    def test_a_bundle_with_no_dataset_overview_is_left_alone(self):
        """baseline 的 slim bundle 本來就沒有 dataset_overview，不得被誤擋。"""
        assert shared.read_dataset_overview({"overall": {"map@1": 0.5}}) == {}
        assert shared.count_items({"overall": {"map@1": 0.5}}) == 0


class TestPreRenamePerItemRefused:
    """#534 renamed the per-item ``hit_rate@K`` to ``recall@K``. A metrics file
    written before that would render its per-item recall cells blank, so the
    report refuses it and says how to get a new one."""

    @staticmethod
    def _legacy_per_item(bundle: dict) -> dict:
        for cell in bundle["per_item"].values():
            for key in [k for k in cell if k.startswith("recall@")]:
                cell["hit_rate@" + key.split("@", 1)[1]] = cell.pop(key)
        return bundle

    def test_a_pre_rename_metrics_file_is_refused_with_the_rerun(self):
        legacy = self._legacy_per_item(_metrics())
        with pytest.raises(ValueError) as exc:
            _assemble(_results(evaluation_metrics=legacy), _params())
        message = str(exc.value)
        assert "evaluation_metrics" in message
        assert "hit_rate@" in message
        assert "--from-node compute_row_facts" in message

    def test_the_category_bundle_is_checked_too(self):
        metrics = _metrics()
        metrics["category"] = self._legacy_per_item(_metrics())
        with pytest.raises(ValueError, match="hit_rate@"):
            _assemble(_results(evaluation_metrics=metrics), _params())

    def test_a_pre_rename_baseline_file_is_refused(self):
        legacy = self._legacy_per_item(_baseline_metrics_full())
        with pytest.raises(ValueError, match="baseline_metrics"):
            _assemble(_results(evaluation_metrics=_metrics(),
                                        baseline_metrics=legacy), _params())

    def test_a_query_group_hit_rate_is_not_the_old_format(self):
        """#541 adds a query-group ``hit_rate@K`` to ``overall``: only the
        per-item block tells the old format apart."""
        metrics = _metrics()
        metrics["overall"]["hit_rate@1"] = 0.6
        _assemble(_results(evaluation_metrics=metrics), _params())


class TestPreLedgerRefused:
    """#535: a metrics bundle without the population ledger was written before
    the row facts landed on their own, so its run has no ``row_facts.json``
    either. Refused before any section, naming the node to re-run from.

    The comparison and the K cut are handed over as ``None``: the nodes that
    land them refuse such a bundle themselves (their own tests), and this is
    the report's check, for a directory whose two files are older still."""

    _NOT_LANDED = {"baseline_comparison": None, "evaluation_display_ks": None}

    def test_the_model_s_metrics_without_a_ledger_are_refused(self):
        metrics = _metrics()
        del metrics["population"]
        with pytest.raises(ValueError, match=(
                r"evaluation_metrics predates #535.*"
                r"--from-node compute_row_facts")):
            _assemble(_results(evaluation_metrics=metrics,
                               **self._NOT_LANDED), _params())

    def test_the_baseline_s_metrics_without_a_ledger_are_refused(self):
        baseline = _baseline_metrics_full()
        del baseline["population"]
        with pytest.raises(ValueError, match=r"baseline_metrics predates #535"):
            _assemble(_results(evaluation_metrics=_metrics(),
                               baseline_metrics=baseline,
                               **self._NOT_LANDED), _params())

    def test_no_baseline_with_the_section_off_passes(self):
        """Section off: the pipeline computes no baseline and passes none
        (#538); there is no bundle to check."""
        html = _assemble(_results(evaluation_metrics=_metrics()), _params())
        assert html.startswith("<!DOCTYPE html>")
        assert 'id="s-popularity_baseline"' not in html


class TestPreLandedReadsRefused:
    """#536: the report reads what it used to compute — the row facts'
    all-positive accounts, among others. A result landed before carries
    none of them, and its fingerprint still matches (no setting changed), so
    drawn as is the ledger and the warning would vanish without a word.
    Refused before any section, naming the node to re-run from."""

    @pytest.mark.parametrize("key", [
        "n_groups_with_positive", "all_positive_share", "all_positive_warns"])
    @pytest.mark.parametrize("grain", ["item", "category"])
    def test_row_facts_without_an_account_are_refused(self, grain, key):
        rf = _ap_facts(category=_ap_category())
        del (rf if grain == "item" else rf["category"])[key]
        with pytest.raises(ValueError) as exc:
            _assemble(_results(evaluation_row_facts=rf,
                                        evaluation_metrics=_ap_metrics(True)),
                               _ap_params())
        msg = str(exc.value)
        assert "evaluation_row_facts" in msg and "#536" in msg
        assert repr(key) in msg
        assert "--from-node compute_row_facts" in msg

    def test_a_baseline_without_its_window_is_refused(self):
        base = _baseline_metrics_full()
        del base["popularity_window"]
        with pytest.raises(ValueError) as exc:
            _assemble(_results(evaluation_metrics=_metrics(),
                                        baseline_metrics=base), _params())
        msg = str(exc.value)
        assert "baseline_metrics" in msg and "'popularity_window'" in msg
        assert "--from-node compute_row_facts" in msg




def test_assemble_report_is_html():
    html = _assemble(_results(evaluation_metrics=_metrics()), _params())
    assert html.startswith("<!DOCTYPE html>")
    assert "概覽" in html                    # 新 spine 第一段


def _section_ids(html: str) -> list[str]:
    """The sections' HTML ids, in page order."""
    return re.findall(r'<(?:div|details) class="section" id="([^"]+)"', html)


def test_a_switched_off_section_is_not_on_the_page():
    """Every switch, alone off, takes its own section off the page and no
    other (``shared.sections_on``): the assembler decides, the section reads
    no switch (#538, ADR-0031 principle 3)."""
    from recsys_tfb.core.consistency import EVALUATION_REPORT_SECTIONS

    every = _results(evaluation_metrics=_metrics(),
                     baseline_metrics=_baseline_metrics_full(),
                     evaluation_report_aggregates=_report_aggregates())
    switched = [s for s in rb.SECTIONS if s.SWITCH is not None]
    assert {s.SWITCH for s in switched} == EVALUATION_REPORT_SECTIONS
    for section in switched:
        params = _params()
        params["evaluation"]["report"]["sections"] = {
            **dict.fromkeys(EVALUATION_REPORT_SECTIONS, True),
            section.SWITCH: False}
        ids = _section_ids(_assemble(every, params))
        assert ids == [f"s-{s.SWITCH or s.__name__.rsplit('.', 1)[1]}"
                       for s in rb.SECTIONS if s is not section], ids


def test_assemble_report_spine_order():
    """ADR-0031 decision 16 item 4: 概覽, 核心概念, 資料統計 open the page,
    完整性檢查 closes it, every section of ``SECTIONS`` is drawn, and the
    glossary is gone."""
    html = _assemble(_results(evaluation_metrics=_metrics(),
                              baseline_metrics=_baseline_metrics_full(),
                              evaluation_report_aggregates=_report_aggregates()),
                     _params())
    ids = _section_ids(html)
    assert len(ids) == len(rb.SECTIONS)
    assert ids[:3] == ["s-overview", "s-core_concepts", "s-data_stats"]
    assert ids[-1] == "s-completeness"
    assert "詞彙表" not in html and "Glossary" not in html


def _toc_titles(html: str) -> list[str]:
    """The section titles in page order, read off the table of contents."""
    return re.findall(r'<li><a href="#[^"]+">(.*?)</a>', html)


def test_the_section_list_alone_decides_the_order(monkeypatch):
    """Moving a section is moving its line in ``SECTIONS`` (ADR-0031
    decision 1): swap two entries and the page swaps those two, nothing
    else."""
    results = _results(evaluation_metrics=_metrics(),
                       baseline_metrics=_baseline_metrics_full())
    before = _toc_titles(_assemble(results, _params()))
    i = rb.SECTIONS.index(core_concepts)
    j = rb.SECTIONS.index(completeness)
    swapped = list(rb.SECTIONS)
    swapped[i], swapped[j] = swapped[j], swapped[i]
    monkeypatch.setattr(rb, "SECTIONS", tuple(swapped))
    after = _toc_titles(_assemble(results, _params()))

    a = before.index(core_concepts.TITLE)
    b = before.index(completeness.TITLE)
    expected = list(before)
    expected[a], expected[b] = expected[b], expected[a]
    assert after == expected


def _windowed(base, parameters, *, windows=None):
    """``base`` with the popularity window ``compute_baseline_metrics`` lands
    for it under ``parameters`` (#536), by the node's own rule
    (``baselines.popularity_window``): over the monthly table the section
    prints (``popularity_rate.monthly_positives`` in rate mode,
    ``monthly_counts`` otherwise) and each evaluated date's covered months —
    the rate block's own, ``windows`` for several dates in count mode, or
    one date's window holding the monthly table's months. Recomputed on every
    call, so a test that changes the counts re-lands its window."""
    from recsys_tfb.evaluation.baselines import (
        baseline_score,
        popularity_window,
        resolve_lookback_months,
    )

    rate = base.get("popularity_rate")
    monthly = (rate.get("monthly_positives") if rate
               else base.get("monthly_counts")) or {}
    if rate:
        windows = rate.get("window_months_covered")
    return {**base, "popularity_window": popularity_window(
        monthly, windows, snap_dates=["2026-01-31"],
        lookback_months=resolve_lookback_months(parameters),
        score=baseline_score(parameters))}


def _baseline_metrics_full(parameters=None):
    """Baseline metrics dict mirroring _metrics() per_item / macro shape, with
    the window it lands under ``parameters`` (``_params()`` by default)."""
    return _windowed({
        "overall": {"map@1": 0.4, "map@2": 0.45, "map@3": 0.5,
                    "precision@1": 0.3, "precision@2": 0.25,
                    "recall@1": 0.25, "recall@2": 0.35},
        "per_item": {
            "A": {"recall@1": 0.15, "recall@2": 0.30, "mean_pos": 3.5,
                  "map_attr@1": 0.40, "map_attr@2": 0.45, "map_attr@3": 0.50},
            "B": {"recall@1": 0.08, "recall@2": 0.20, "mean_pos": 5.5,
                  "map_attr@1": 0.25, "map_attr@2": 0.28, "map_attr@3": 0.30}},
        "macro_avg": {"by_item": {
            "recall@1": 0.115, "recall@2": 0.25, "mean_pos": 4.5,
            "map_attr@1": 0.325, "map_attr@2": 0.365, "map_attr@3": 0.40,
        }},
        "population": {"all_k": 2, "macro_items": ["A", "B"],
                       "n_scored_queries_with_positive": 10,
                       "n_scored_queries_without_positive": 0,
                       "segment_columns": []},
    }, parameters or _params())


def test_the_page_title_is_chinese_with_the_english_beside_it():
    html = _assemble(_results(evaluation_metrics=_metrics()), _params())
    assert ('<h1>模型評估報表<span class="en">Model Evaluation Report</span>'
            '</h1>') in html
    assert "<title>模型評估報表</title>" in html


def test_assemble_metadata_has_model_version_and_generated_at():
    p = _params()
    p["model_version"] = "v_test"
    html = _assemble(_results(evaluation_metrics=_metrics()), p)
    assert "<tr><th>模型版本</th><td>v_test</td></tr>" in _metadata_block(html)
    assert re.search(r"<tr><th>產生時間</th><td>\d{4}-\d\d-\d\d "
                     r"\d\d:\d\d:\d\d</td></tr>", _metadata_block(html))


def _metadata_block(html):
    start = html.index('<div class="metadata">')
    return html[start:html.index("</div>", start)]


def _metadata_labels(html):
    return [row.split("</th>")[0]
            for row in _metadata_block(html).split("<tr><th>")[1:]]


def test_metadata_one_date_is_shown_as_configured():
    """A single date as written, no several-date wording anywhere in the
    page. The rows are the demo's three: nothing counted (decision 16 item
    12: the totals are 核心概念's), whatever the all-positive switch says."""
    for drop in (False, True):
        p = _ap_params(drop)
        p["evaluation"]["snap_date"] = "2026-01-31"
        html = _assemble(_ap_results(drop=drop), p)
        assert _metadata_labels(html) == ["模型版本", "評估日期", "產生時間"]
        block = _metadata_block(html)
        assert "<tr><th>評估日期</th><td>2026-01-31</td></tr>" in block
        assert "個日期" not in html


def test_metadata_several_dates_show_the_range():
    """#374: the date row reads as a range with its count; the rows are the
    same three as for one date."""
    p = _params()
    p["evaluation"]["snap_date"] = ["2026-02-28", "2026-01-31", "2026-03-31"]
    html = _assemble(_results(evaluation_metrics=_metrics()), p)
    block = _metadata_block(html)
    assert ("<tr><th>評估日期</th><td>2026-01-31 ~ 2026-03-31（3 個日期）"
            "</td></tr>") in block, block
    assert _metadata_labels(html) == ["模型版本", "評估日期", "產生時間"]




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
        "population": _category_ledger(),
    }
    m["population"]["segment_columns"] = ["segment"]
    return m


def _category_ledger():
    """A category grain's ledger for the seg/cat fixtures: 3 categories."""
    return {"all_k": 3, "macro_items": [],
            "n_scored_queries_with_positive": 10,
            "n_scored_queries_without_positive": 0,
            "segment_columns": ["segment"]}


def _row_facts_with_seg_cat():
    """The row facts matching ``_metrics_with_seg_cat()``: 3 categories."""
    return _row_facts_with_category({"totals": {"n_items": 3}})


def _metrics_min():
    return {
        "overall": {"map@2": 0.8, "precision@2": 0.5, "recall@2": 1.0},
        "per_item": {
            "A": {"map_attr@2": 0.75,
                  "recall@2": 1.0, "mean_pos": 1.5, "n_pos": 2},
            "B": {"map_attr@2": 1.0,
                  "recall@2": 1.0, "mean_pos": 1.0, "n_pos": 1},
        },
        "macro_avg": {"by_item": {"map_attr@2": 0.875,
                                  "recall@2": 1.0, "mean_pos": 1.25}},
        "observation_items": [],
        "population": _full_ledger(2, ["A", "B"]),
    }


def _row_facts_min():
    return _accounted({"dataset_overview": {"totals": {"n_items": 2}},
                       "n_queries": 3, "n_excluded_queries": 0,
                       "n_all_positive_queries": 0, "widest_group_rows": 2})


def _params_min():
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name"}},
        "evaluation": {"report": {"display": {"compare_k": [2]}}},
    }


def test_assemble_report_has_no_reconciliation_section():
    """對帳層已退場——report.html 不得再出現該段落。"""
    html = _assemble(
        _results(evaluation_row_facts=_row_facts_min(),
                 evaluation_metrics=_metrics_min()), _params_min())
    assert "對帳" not in html
    assert "Reconciliation" not in html


# ---------------------------------------------------------------------------
# "all" is looked up at the K the producer recorded (#434)
# ---------------------------------------------------------------------------


def _metrics_with_recorded_all_k():
    """What ``compute_all_metrics`` writes with ``event`` declared: 2 items,
    but the widest query group holds 4 rows, so every ``"all"`` value is
    stored at ``@4`` and the bundle says so. Looking ``"all"`` up at the item
    count asks for ``@2`` instead — a real key, holding the wrong number."""
    m = _metrics_with_seg_cat()
    m["population"]["all_k"] = 4
    m["overall"].update({"map@2": 0.1, "map@4": 0.9, "recall@4": 1.0})
    m["per_segment"]["X"]["map@4"] = 0.8
    m["per_segment"]["Y"]["map@4"] = 0.7
    for item, v in (("A", 0.66), ("B", 0.44)):
        m["per_item"][item].update({"map_attr@4": v, "recall@4": 1.0})
    m["macro_avg"]["by_item"].update({"map_attr@4": 0.55, "recall@4": 1.0})
    return m




# ---- 預測品質（#381，ADR-0024；ADR-0032 起是 --prediction-quality 那一頁）----

def _pq_payload(listed=("A",), n_items=2):
    """A ``prediction_quality_metrics`` payload as ``build_payload`` lands it,
    built from the worked-example bins of
    ``tests/test_evaluation/test_prediction_quality.py``: 4 fine bins over
    [0, 1) (width 0.25), 2 display bins, item A holding every row; the
    population block of a monitoring run."""
    from recsys_tfb.evaluation.prediction_quality import build_payload

    bins = pd.DataFrame({"bin": [0, 1, 3], "n": [4, 2, 2],
                         "n_pos": [0, 1, 2], "score_sum": [0.4, 0.8, 1.8]})
    per_item = bins.assign(prod_name="A")[
        ["prod_name", "bin", "n", "n_pos", "score_sum"]]
    # The areas of the worked example (A); B has no row, so none.
    totals = pd.DataFrame({"prod_name": ["A", "B"][:n_items],
                           "n": [8, 0][:n_items], "n_pos": [3, 0][:n_items],
                           "pr_auc": [11 / 12, None][:n_items],
                           "roc_auc": [14.5 / 15, None][:n_items]})
    aggregated = {
        "lo": 0.0, "hi": 1.0, "width": 0.25, "overall": bins,
        "overall_areas": {"pr_auc": 11 / 12, "roc_auc": 14.5 / 15},
        "item_totals": totals, "listed_items": list(listed),
        "per_item": per_item[per_item["prod_name"].isin(listed)],
    }
    payload = build_payload(
        aggregated, item_col="prod_name", score_col="score",
        label_col="label", weight_col=None, n_bins=4, n_display_bins=2,
        top_n=len(listed))
    return {**payload, "population": {
        "source": "monitoring", "evaluated_dates": ["2026-01-31"],
        "test_zero_positive_group_ratio": None,
        "n_zero_positive_groups": None}}


def _pq_params(post_training=None):
    p = _params()
    if post_training is not None:
        p["post_training"] = post_training
    return p


def test_the_main_report_has_no_prediction_quality_section():
    """ADR-0032 decision 2: the every-candidate-row family is the
    ``--prediction-quality`` mode's page; the main report keeps the ranking
    metrics only, whatever the conf switches on."""
    from recsys_tfb.core.consistency import EVALUATION_REPORT_SECTIONS

    assert "prediction_quality" not in EVALUATION_REPORT_SECTIONS
    assert not [s for s in rb.SECTIONS
                if "prediction_quality" in s.__name__
                or "prediction_quality_metrics" in s.READS]
    html = _assemble(_results(evaluation_metrics=_metrics(),
                              baseline_metrics=_baseline_metrics_full(),
                              evaluation_report_aggregates=_report_aggregates()),
                     _params())
    assert "預測品質" not in html and "s-prediction_quality" not in html


# --- ADR-0025 decision 3: the kept zero-positive groups, where they show -----

def _kept_params(ratio, post_training=True):
    """``ratio`` is the evaluated model's r, as the CLI hands it on."""
    from recsys_tfb.core.consistency import DATASET_TEST_RATIO_KEY

    p = _pq_params(post_training=post_training)
    p[DATASET_TEST_RATIO_KEY] = ratio
    return p


# ---------------------------------------------------------------------------
# #376：全正的 query group——組數帳、佔比警告、開關開著時的加註與改寫
# ---------------------------------------------------------------------------

from recsys_tfb.evaluation.result_readers import ALL_POSITIVE_KEY

#: 開關開著時，不套用它的段落旁印的那一句（票面原文）。用字面值、不 import
#: 報表各段的常數：常數的字被改掉時這裡要紅。
_KEPT = "這段沒有排除全正的 query group"
_SWITCH = "evaluation.query_filter.drop_all_positive_groups"


def _ap_facts(n_queries=1000, n_excluded=50, n_all_positive=50,
              category=None, drop=None):
    """``_row_facts()`` 帶上 #376 的組數（組數在 row facts，不在指標結果）；
    ``n_all_positive=None`` ＝ #376 之前算的 row facts（沒有這個鍵）。預設：
    有正例 950、其中全正 50、無正例 50。每個粒度的帳（#536）照 ``drop`` 這個
    開關算，跟 ``compute_row_facts`` 在那個設定下落地的一樣。"""
    rf = _row_facts()
    rf["n_queries"] = n_queries
    rf["n_excluded_queries"] = n_excluded
    if n_all_positive is not None:
        rf[ALL_POSITIVE_KEY] = n_all_positive
    else:
        del rf[ALL_POSITIVE_KEY]
    if category is not None:
        rf["category"] = category
    return _accounted(rf, _ap_params(drop))


def _ap_metrics(with_category=False):
    """``_metrics()``，需要時帶大類子 bundle 的指標部分（組數在
    ``_ap_category`` 那份 row facts 裡）。"""
    m = _metrics()
    if with_category:
        m["category"] = {
            "overall": {"map@1": 0.4, "map@2": 0.45},
            "per_item": {"fund": {"recall@1": 0.3, "mean_pos": 2.0}},
            "macro_avg": {"by_item": {"recall@1": 0.3, "mean_pos": 2.0}},
            "population": _full_ledger(2, ["fund"]),
        }
    return m


def _ap_category(n_queries=1000, n_excluded=100, n_all_positive=300):
    """大類子 bundle 的 row facts，帶它自己的組數（和細粒度是兩個不同的數）。"""
    cat = {
        "dataset_overview": {"totals": {"n_items": 2}},
        "n_queries": n_queries, "n_excluded_queries": n_excluded,
    }
    if n_all_positive is not None:
        cat[ALL_POSITIVE_KEY] = n_all_positive
    return cat


def _scored(facts, drop):
    """The groups the metrics score off ``facts`` under the switch ``drop``
    (``metrics_spark``'s rule): those holding a positive, less the
    all-positive ones when the switch drops them."""
    with_positive = facts["n_queries"] - facts["n_excluded_queries"]
    return with_positive - (facts.get(ALL_POSITIVE_KEY) or 0 if drop else 0)


def _ap_results(n_queries=1000, n_excluded=50, n_all_positive=50,
                category=None, drop=None, **more):
    """The report's inputs for a #376 test: ``_ap_facts`` row facts with the
    matching ``_ap_metrics`` (a category block on both when ``category``),
    their accounts landed, and the groups the metrics' ledger says were
    scored counted (#536), under the switch ``drop`` — draw them with that
    switch."""
    facts = _ap_facts(n_queries, n_excluded, n_all_positive, category, drop)
    metrics = _ap_metrics(category is not None)
    metrics["population"] = {
        **metrics["population"],
        "n_scored_queries_with_positive": _scored(facts, drop)}
    if category is not None:
        metrics["category"]["population"] = {
            **metrics["category"]["population"],
            "n_scored_queries_with_positive": _scored(category, drop)}
    return _results(evaluation_row_facts=facts, evaluation_metrics=metrics,
                    **more)


def _ap_params(drop=None, *, occasion=False, event=False, post_training=None,
               ratio=None):
    """``drop=None`` ＝ 設定裡沒寫 ``query_filter``（舊設定，等於關）。"""
    p = _params()
    if drop is not None:
        p["evaluation"]["query_filter"] = {"drop_all_positive_groups": drop}
    if occasion:
        p["schema"]["columns"]["occasion"] = "request_id"
    if event:
        p["schema"]["columns"]["event"] = "impression_id"
    if post_training is not None:
        p["post_training"] = post_training
    if ratio is not None:
        from recsys_tfb.core.consistency import DATASET_TEST_RATIO_KEY

        p[DATASET_TEST_RATIO_KEY] = ratio
    return p


def _set_n_items(results, n):
    """Make the row facts and the metrics agree on the item count."""
    results["evaluation_row_facts"]["dataset_overview"]["totals"]["n_items"] = n
    results["evaluation_metrics"]["population"]["all_k"] = n




# ---- 3. 開關開著時的加註（fixture 宣告 occasion、不宣告 event）------------


def test_metrics_section_with_event_declared_has_no_headline_table():
    """宣告 event 時排序指標段也不另印頭號表（概覽的模型選擇指標表見
    sections/test_overview.py）。"""
    p = _ap_params(True, event=True)
    r = _ap_results()
    ms = _draw(ranking_metrics, r, p)
    assert not any(getattr(b, "title", "").startswith("頭號指標")
                   for b in ms.blocks)
    assert "沒有排除全正" not in ms.description


# ---- 4. 開關開著時會變成假話的句子 -----------------------------------------



# ---------------------------------------------------------------------------
# The metric lists and names come from the catalog (ADR-0031 decision 6)
# ---------------------------------------------------------------------------
