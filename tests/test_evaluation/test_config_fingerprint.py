"""``evaluation.config_fingerprint``: which settings make a landed JSON stale.

The expected key lists below are copied from issue #342 / ADR-0020 decision 2,
not derived from ``COMPUTED_KEYS``: a test whose expectation is read from the
module under test agrees with whatever the module says.
"""
import ast
import copy
import datetime
import json
import re
from pathlib import Path

import pytest

from recsys_tfb.evaluation.config_fingerprint import (
    COMPUTED_KEYS,
    LoadedArtifact,
    fingerprint,
    require_computed_with_current_config,
)

#: The closed enumeration as the issue writes it (dotted from the params root).
SPEC_COMPUTED_PATHS = {
    # Not from issue #342: the run mode (--post-training / monitoring),
    # added by F7 (a later code-review round) because it changes which
    # predictions table prepare_eval_data reads and is injected into
    # `parameters` at the top level like model_version/snap_date.
    "post_training",
    "evaluation.snap_date",
    "evaluation.k_values",
    "evaluation.segment_columns",
    "evaluation.segment_sources",
    "evaluation.item_categories",
    "evaluation.baseline",
    "evaluation.metric",
    "evaluation.diagnosis",
    "evaluation.report.diagnostics",
    "evaluation.report.sections.baseline",
    "evaluation.report.sections.diagnostics",
}

#: One leaf per enumerated key, including leaves under the ``.*`` subtrees, so
#: a subtree listed by its root is shown to cover what sits below it.
SPEC_COMPUTED_LEAVES = [
    "post_training",
    "evaluation.snap_date",
    "evaluation.k_values",
    "evaluation.segment_columns",
    "evaluation.segment_sources",
    "evaluation.item_categories.enabled",
    "evaluation.baseline.lookback_months",
    "evaluation.metric.min_positives",
    "evaluation.diagnosis.sample.seed",
    "evaluation.diagnosis.suppression.enabled",
    "evaluation.report.diagnostics.n_calibration_bins",
    "evaluation.report.sections.baseline",
    "evaluation.report.sections.diagnostics",
]

#: "Drawn" keys: they change what the report shows, never what is computed.
SPEC_DRAWN_LEAVES = [
    "evaluation.report.display.primary_map_k",
    "evaluation.report.display.guardrail_recall_k",
    "evaluation.report.sections.primary_map",
    "evaluation.report.sections.per_segment",
]

CONFIG_SHIFT_EXTRA = (
    "dataset.sample_group_keys",
    "dataset.sample_ratio",
    "dataset.sample_ratio_overrides",
    "training.sample_weight_keys",
    "training.sample_weights",
)


def _params() -> dict:
    return {
        "model_version": "mv_test",
        "snap_date": "20260131",
        "post_training": False,
        "dataset": {
            "sample_group_keys": ["segment_a", "label"],
            "sample_ratio": 1.0,
            "sample_ratio_overrides": {"x|1": 0.5},
        },
        "training": {"sample_weight_keys": [], "sample_weights": {}},
        "evaluation": {
            "snap_date": "2026-01-31",
            "k_values": [5, "all"],
            "segment_columns": ["segment_a"],
            "segment_sources": {},
            "item_categories": {"enabled": False},
            "baseline": {"lookback_months": 12},
            "metric": {"weight_alpha": 0.0, "k": None,
                       "min_positives": 0, "shrinkage_k": 0.0},
            "diagnosis": {"ci": {"enabled": True},
                          "sample": {"seed": 42},
                          "suppression": {"enabled": True}},
            "report": {
                "sections": {"primary_map": True, "per_segment": True,
                             "baseline": True, "diagnostics": True},
                "display": {"primary_map_k": [1, 3, 5, "all"],
                            "guardrail_recall_k": [1, 2]},
                "diagnostics": {"include_calibration": True,
                                "n_calibration_bins": 10},
            },
        },
    }


def _set(params: dict, path: str, value) -> dict:
    node = params
    *parents, leaf = path.split(".")
    for seg in parents:
        node = node.setdefault(seg, {})
    node[leaf] = value
    return params


def _delete(params: dict, path: str) -> dict:
    node = params
    *parents, leaf = path.split(".")
    for seg in parents:
        node = node[seg]
    del node[leaf]
    return params


def _changed(params: dict, path: str) -> dict:
    return _set(copy.deepcopy(params), path, {"changed": "value"})


# ---------------------------------------------------------------- fingerprint


def test_enumeration_is_exactly_the_issue_list():
    assert {path for path, _ in COMPUTED_KEYS} == SPEC_COMPUTED_PATHS


def test_same_parameters_same_hash():
    a = fingerprint(_params())
    b = fingerprint(copy.deepcopy(_params()))
    assert a == b
    assert re.fullmatch(r"[0-9a-f]{64}", a["sha256"])


@pytest.mark.parametrize("path", SPEC_COMPUTED_LEAVES)
def test_changing_a_computed_key_changes_the_hash(path):
    base = _params()
    assert fingerprint(_changed(base, path))["sha256"] != \
        fingerprint(base)["sha256"]


@pytest.mark.parametrize("path", SPEC_COMPUTED_LEAVES)
def test_adding_an_absent_computed_key_changes_the_hash(path):
    absent = _delete(_params(), path)
    present = _set(copy.deepcopy(absent), path, 7)
    assert fingerprint(present)["sha256"] != fingerprint(absent)["sha256"]


@pytest.mark.parametrize("path", SPEC_DRAWN_LEAVES)
def test_changing_a_drawn_key_keeps_the_hash(path):
    base = _params()
    assert fingerprint(_changed(base, path)) == fingerprint(base)


def test_evaluation_compare_is_ignored():
    """The CLI injects ``evaluation.compare`` at run time (``__main__.py``)."""
    base = _params()
    with_compare = _set(copy.deepcopy(base), "evaluation.compare",
                        {"kind": "hive", "model_version": "v_b"})
    assert fingerprint(with_compare) == fingerprint(base)


def test_extra_keys_only_affect_the_fingerprint_that_declares_them():
    base = _params()
    moved = _set(copy.deepcopy(base), "dataset.sample_ratio_overrides",
                 {"x|1": 0.25})
    assert fingerprint(moved, CONFIG_SHIFT_EXTRA)["sha256"] != \
        fingerprint(base, CONFIG_SHIFT_EXTRA)["sha256"]
    assert fingerprint(moved) == fingerprint(base)


def test_absent_and_null_are_different_settings():
    """``sections.get("baseline", True)``: a missing key computes the baseline,
    an explicit null does not. One hash for both would let that flip pass."""
    null = _set(_params(), "evaluation.report.sections.baseline", None)
    absent = _delete(_params(), "evaluation.report.sections.baseline")
    assert fingerprint(null)["sha256"] != fingerprint(absent)["sha256"]
    assert "evaluation.report.sections.baseline" not in \
        fingerprint(absent)["values"]
    assert fingerprint(null)["values"][
        "evaluation.report.sections.baseline"] is None


def test_fingerprint_survives_a_json_round_trip():
    """YAML turns an unquoted ``2026-01-31`` into a ``datetime.date``; the
    landed JSON holds a string. Reading it back must still compare equal."""
    params = _set(_params(), "evaluation.snap_date", datetime.date(2026, 1, 31))
    fp = fingerprint(params)
    assert json.loads(json.dumps(fp)) == fp
    require_computed_with_current_config(
        [LoadedArtifact(
            catalog_name="evaluation_metric_ci",
            payload=json.loads(json.dumps(
                {"enabled": False, "config_fingerprint": fp})),
            produced_by="compute_metric_ci")],
        params,
    )


# ------------------------------------------------------- non-string dict keys


def test_int_vs_str_dict_key_changes_the_hash():
    """YAML can write ``{1: 0.5}`` or ``{"1": 0.5}``; every lookup against a
    dict like this (e.g. ``dataset.sample_ratio_overrides``) keys by str, so
    only the second ever matches at runtime. Fingerprinting them the same
    would let a real behaviour change ({1: ...} -> {"1": ...}) report "no
    change" and skip the required re-run."""
    int_keyed = _set(_params(), "evaluation.metric", {1: 0.5})
    str_keyed = _set(_params(), "evaluation.metric", {"1": 0.5})
    assert fingerprint(int_keyed)["sha256"] != fingerprint(str_keyed)["sha256"]


def test_mixed_key_types_does_not_raise():
    """A dict mixing int and str keys is legal YAML; ``json.dumps(...,
    sort_keys=True)`` raises TypeError comparing them directly, crashing the
    node on a legal config."""
    mixed = _set(_params(), "evaluation.metric", {1: 0.5, "a": 0.3})
    fingerprint(mixed)  # must not raise


def test_int_keyed_config_is_stable_and_round_trips():
    """Same int-keyed config fingerprinted twice agrees, and the stored
    ``values`` survive a JSON round trip and still compare equal to a fresh
    fingerprint via ``require_computed_with_current_config`` (no false
    raise)."""
    params = _set(_params(), "evaluation.metric", {1: 0.5})
    fp1 = fingerprint(params)
    fp2 = fingerprint(copy.deepcopy(params))
    assert fp1 == fp2
    stored = json.loads(json.dumps(
        {"enabled": False, "config_fingerprint": fp1}))
    require_computed_with_current_config(
        [LoadedArtifact(catalog_name="evaluation_metric_ci", payload=stored,
                        produced_by="compute_metric_ci")],
        params,
    )


# ------------------------------------------------------- run mode (post_training)


def test_post_training_changes_the_hash():
    """(#342 F7) --post-training and monitoring runs of the same
    (model_version, snap_date) write the same catalog paths but
    prepare_eval_data reads a different predictions table; the fingerprint
    must tell the two populations apart."""
    on = _set(_params(), "post_training", True)
    off = _set(_params(), "post_training", False)
    assert fingerprint(on)["sha256"] != fingerprint(off)["sha256"]


def test_post_training_mismatch_names_the_key_and_rerun_node():
    old = _set(_params(), "post_training", True)
    new = _set(copy.deepcopy(old), "post_training", False)
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_metric_ci",
                            payload=_payload(old),
                            produced_by="compute_metric_ci")],
            new,
        )
    msg = str(exc.value)
    assert "post_training" in msg
    assert "--from-node prepare_eval_data" in msg


# ------------------------------------------------ require_computed_with_...


def _payload(params, extra=()):
    return {"enabled": False, "config_fingerprint": fingerprint(params, extra)}


def test_fresh_artifacts_pass():
    params = _params()
    require_computed_with_current_config(
        [LoadedArtifact(catalog_name="evaluation_metric_ci",
                        payload=_payload(params),
                        produced_by="compute_metric_ci"),
         LoadedArtifact(catalog_name="evaluation_config_shift",
                        payload=_payload(params, CONFIG_SHIFT_EXTRA),
                        produced_by="diagnose_config_shift",
                        extra_keys=CONFIG_SHIFT_EXTRA)],
        params,
    )


def test_changed_key_names_the_leaf_values_and_rerun_node():
    old = _params()
    new = _set(copy.deepcopy(old), "evaluation.metric.min_positives", 5)
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_metric_ci",
                            payload=_payload(old),
                            produced_by="compute_metric_ci")],
            new,
        )
    msg = str(exc.value)
    assert "evaluation_metric_ci" in msg
    assert "compute_metric_ci" in msg
    assert "evaluation.metric.min_positives: 0 -> 5" in msg
    assert "--from-node compute_metrics" in msg


def test_missing_fingerprint_names_the_artifact():
    params = _params()
    for payload in ({"enabled": False}, None, {"config_fingerprint": "x"}):
        with pytest.raises(ValueError) as exc:
            require_computed_with_current_config(
                [LoadedArtifact(catalog_name="evaluation_report_aggregates",
                                payload=payload,
                                produced_by="compute_report_aggregates")],
                params,
            )
        msg = str(exc.value)
        assert "evaluation_report_aggregates" in msg
        assert "has no config_fingerprint" in msg


def test_rule_1_earliest_computed_key_wins_over_message_order():
    """Two stale artifacts; the earliest rerun node among the changed
    computed keys is suggested, not the first artifact's producer."""
    old = _params()
    new = _set(copy.deepcopy(old),
               "evaluation.report.diagnostics.n_calibration_bins", 20)
    new = _set(new, "evaluation.k_values", [1, 5])
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_report_aggregates",
                            payload=_payload(old),
                            produced_by="compute_report_aggregates"),
             LoadedArtifact(catalog_name="evaluation_metric_ci",
                            payload=_payload(old),
                            produced_by="compute_metric_ci")],
            new,
        )
    msg = str(exc.value)
    assert "--from-node compute_metrics" in msg
    assert "--from-node compute_report_aggregates" not in msg
    # collect-all: both artifacts are reported
    assert "evaluation_report_aggregates" in msg
    assert "evaluation_metric_ci" in msg


def test_rule_2_one_artifact_with_only_its_own_key_changed():
    old = _params()
    new = _set(copy.deepcopy(old), "dataset.sample_ratio_overrides",
               {"x|1": 0.25})
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_item_ability",
                            payload=_payload(old),
                            produced_by="diagnose_item_ability"),
             LoadedArtifact(catalog_name="evaluation_config_shift",
                            payload=_payload(old, CONFIG_SHIFT_EXTRA),
                            produced_by="diagnose_config_shift",
                            extra_keys=CONFIG_SHIFT_EXTRA)],
            new,
        )
    msg = str(exc.value)
    assert "dataset.sample_ratio_overrides.x|1: 0.5 -> 0.25" in msg
    assert "--from-node diagnose_config_shift" in msg
    assert "evaluation_item_ability" not in msg


def test_rule_2_one_artifact_without_fingerprint():
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_suppression",
                            payload={"enabled": False},
                            produced_by="diagnose_suppression")],
            _params(),
        )
    assert "--from-node diagnose_suppression" in str(exc.value)


def test_rule_3_several_artifacts_no_computed_key_suggests_full_run():
    old = _params()
    new = _set(copy.deepcopy(old), "dataset.sample_ratio", 0.5)
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_config_shift",
                            payload=_payload(old, CONFIG_SHIFT_EXTRA),
                            produced_by="diagnose_config_shift",
                            extra_keys=CONFIG_SHIFT_EXTRA),
             LoadedArtifact(catalog_name="evaluation_suppression",
                            payload={"enabled": False},
                            produced_by="diagnose_suppression")],
            new,
        )
    msg = str(exc.value)
    assert "--from-node" not in msg
    assert "--only-node" not in msg
    assert "full pipeline" in msg


def test_long_values_are_truncated_in_the_message():
    old = _params()
    new = _set(copy.deepcopy(old), "evaluation.segment_columns",
               [f"col_{i:03d}" for i in range(100)])
    with pytest.raises(ValueError) as exc:
        require_computed_with_current_config(
            [LoadedArtifact(catalog_name="evaluation_metric_ci",
                            payload=_payload(old),
                            produced_by="compute_metric_ci")],
            new,
        )
    msg = str(exc.value)
    assert "col_099" not in msg
    line = next(ln for ln in msg.splitlines()
                if "evaluation.segment_columns:" in ln)
    new_repr = line.split(" -> ", 1)[1]
    assert len(new_repr) <= 80


# --------------------------------------------------------------- dependencies


def test_module_imports_no_project_or_spark_code():
    """Pure stdlib: ``diagnosis/`` depends on nothing here and the node layer
    does the combining (ADR-0020 decision 2, dependency direction)."""
    import recsys_tfb.evaluation.config_fingerprint as mod

    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    bad = sorted(m for m in imported
                 if m.split(".")[0] in {"recsys_tfb", "pyspark"} or m == "")
    assert bad == []
