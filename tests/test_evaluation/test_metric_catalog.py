"""The metric catalog (ADR-0031 decision 6): what each entry carries, and that
its names are the keys the Spark metrics really write."""

import subprocess
import sys

import pytest

from recsys_tfb.evaluation import metric_catalog as mc
from recsys_tfb.evaluation.metric_catalog import ITEM, QUERY_GROUP
from recsys_tfb.evaluation.metrics_spark import (
    compute_all_metrics,
    compute_row_facts,
)


def test_every_entry_has_a_display_name_and_a_definition():
    for metric in mc.METRICS:
        assert metric.display_name.strip(), metric.name
        assert metric.definition.strip(), metric.name


def test_reading_the_catalog_pulls_in_no_spark_and_no_pandas():
    """Reports read the catalog, and a report imports nothing that computes
    (ADR-0031 principle 1), so the catalog holds data only."""
    probe = (
        "import sys\n"
        "import recsys_tfb.evaluation.metric_catalog\n"
        "print(sorted(m for m in ('pyspark', 'pandas') if m in sys.modules))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True,
        check=True,
    ).stdout.strip()
    assert out == "[]"


def _params():
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label", "score": "score",
                "rank": "rank",
            },
            "categorical_values": {"prod_name": ["A", "B", "C"]},
        },
        "evaluation": {
            "k_values": [1, 2, "all"],
            "segment_columns": ["seg"],
            "item_categories": {
                "enabled": True, "unmapped": "singleton",
                "mapping": {"AB": ["A", "B"]},
            },
        },
    }


def _frame(spark):
    rows = [
        ("20240331", "c1", "A", 0.9, 1, "s1"),
        ("20240331", "c1", "B", 0.5, 0, "s1"),
        ("20240331", "c1", "C", 0.1, 1, "s1"),
        ("20240331", "c2", "A", 0.2, 0, "s2"),
        ("20240331", "c2", "B", 0.8, 1, "s2"),
        ("20240331", "c2", "C", 0.4, 0, "s2"),
    ]
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "score", "label",
                      "seg"])


def _blocks(bundle: dict) -> dict[str, list[tuple[str, dict]]]:
    """Every cell of a metrics bundle that holds metric keys, by method.

    A cell is one dict of ``metric key -> value``; ``n_pos`` sits beside the
    item method's metrics as the macro's weight and is not one.
    """
    by_method = {QUERY_GROUP: [], ITEM: []}
    by_method[QUERY_GROUP].append(("overall", bundle["overall"]))
    for seg, cell in (bundle.get("per_segment") or {}).items():
        by_method[QUERY_GROUP].append((f"per_segment[{seg}]", cell))
    for item, cell in bundle["per_item"].items():
        by_method[ITEM].append((f"per_item[{item}]", cell))
    for item, by_seg in (bundle.get("per_item_segment") or {}).items():
        for seg, cell in by_seg.items():
            by_method[ITEM].append((f"per_item_segment[{item}][{seg}]", cell))
    macro = bundle.get("macro_avg") or {}
    for name, method in (("by_segment", QUERY_GROUP), ("by_item", ITEM),
                         ("by_item_segment", ITEM)):
        if name in macro:
            by_method[method].append((f"macro_avg.{name}", macro[name]))
    return by_method


def _assert_keys_are_the_catalog(bundle: dict, where: str):
    for method, cells in _blocks(bundle).items():
        expected = {m.name: m.per_k for m in mc.metrics_of(method)}
        assert cells, (where, method)
        for label, cell in cells:
            found: dict[str, set] = {}
            for key in cell:
                if key == "n_pos":
                    continue
                name, at, k = key.partition("@")
                found.setdefault(name, set()).add(bool(at))
            assert set(found) == set(expected), (where, label, sorted(found))
            for name, per_k in expected.items():
                assert found[name] == {per_k}, (where, label, name)


def test_catalog_names_are_the_keys_the_spark_metrics_write(spark):
    """Every block of the bundle — the one the report reads, and the baseline
    is the same one — holds exactly the catalog's names of its method, each
    with ``@K`` when the entry is per K; the category bundle too.

    The Spark side builds its columns from the catalog, so what this guards
    is a key written past it: a block that adds a metric of its own, or a
    path that stops going through the catalog. Which metrics exist at all is
    the value tests' job (``test_metrics_spark*.py``): drop an entry and
    their keys go missing there, not here."""
    frame, params = _frame(spark), _params()
    facts = compute_row_facts(frame, params, segment_columns=["seg"])
    full = compute_all_metrics(frame, params, facts, segment_columns=["seg"])
    _assert_keys_are_the_catalog(full, "full")
    _assert_keys_are_the_catalog(full["category"], "full.category")


def _toy_metric(name, method, formula):
    return mc.Metric(
        name=name, method=method, formula=formula, per_k=True,
        grains=("overall",), population="toy", display_name=name,
        definition="toy",
    )


def test_a_new_entry_is_computed_with_no_change_to_the_spark_code(
        spark, monkeypatch):
    """Adding a metric is adding an entry (ADR-0031 decision 1): the Spark
    metrics build each entry's formula from the catalog.

    Worked by hand on :func:`_frame` (``"all"`` is 3, the item count). c1
    ranks A(+) B C(+), c2 ranks B(+) C A. ``hits / K`` is precision@K: at K=2
    each group has one positive in its top two, so 0.5. ``hits / n_top`` per
    item reads a shared number no shipped entry reads: of the item's rows in
    the top K, the share that are positives. A is in the top 1 once (c1, a
    positive); C is in the top 2 once (c2, a negative), and in the top 3 twice,
    one of them its positive in c1.
    """
    monkeypatch.setattr(mc, "METRICS", mc.METRICS + (
        _toy_metric("toy_q", QUERY_GROUP, mc.Ratio("hits", "K")),
        _toy_metric("toy_i", ITEM, mc.Ratio("hits", "n_top")),
    ))
    facts = compute_row_facts(_frame(spark), _params(), segment_columns=["seg"])
    full = compute_all_metrics(
        _frame(spark), _params(), facts, segment_columns=["seg"])
    assert full["overall"]["toy_q@1"] == pytest.approx(1.0)
    assert full["overall"]["toy_q@2"] == pytest.approx(0.5)
    assert full["per_item"]["A"]["toy_i@1"] == pytest.approx(1.0)
    assert full["per_item"]["C"]["toy_i@2"] == pytest.approx(0.0)
    assert full["per_item"]["C"]["toy_i@3"] == pytest.approx(0.5)


def test_entry_refuses_a_name_the_catalog_lacks():
    assert mc.entry("map", QUERY_GROUP).name == "map"
    with pytest.raises(KeyError, match="no 'map' in method 'item'"):
        mc.entry("map", ITEM)


# ---- #537: the lists a section shows, and the model-selection marks ----


def test_compare_entry_refuses_a_name_the_catalog_lacks():
    with pytest.raises(KeyError, match="no 'nope' in method 'query_group'"):
        mc.CompareEntry("nope", QUERY_GROUP, "overall")


def test_compare_entry_refuses_a_grain_the_entry_lacks():
    """``map`` is a query-group metric: it has no per-item value."""
    assert mc.CompareEntry("map", QUERY_GROUP, "overall").metric is mc.entry(
        "map", QUERY_GROUP)
    with pytest.raises(ValueError, match="'map' in method 'query_group' is "
                                         "not at grain 'per_item'"):
        mc.CompareEntry("map", QUERY_GROUP, "per_item")


def test_model_selection_marks_and_pending_name_every_registry_metric():
    """Every metric HPO can select by is either marked on its catalog entry
    or listed as not computed yet — once. A metric added to the registry
    turns this red until it is placed in one of the two."""
    from recsys_tfb.evaluation.metric_registry import METRIC_NAMES

    marked = [m.name for m in mc.MODEL_SELECTION_METRICS]
    listed = marked + list(mc.MODEL_SELECTION_PENDING)
    assert len(listed) == len(set(listed)), listed
    assert set(listed) == set(METRIC_NAMES)


def test_model_selection_marks_read_the_entries_their_registry_metric_equals():
    """ADR-0031 decision 16 (the design's 〈指標目錄〉): ``mean_ap`` is ``map``
    over query groups, overall; ``macro_per_item_map`` is ``map_attr`` over
    items, its macro — both at K = all."""
    by_name = {m.name: m for m in mc.MODEL_SELECTION_METRICS}
    assert by_name["mean_ap"].source == mc.CompareEntry(
        "map", QUERY_GROUP, "overall")
    assert by_name["macro_per_item_map"].source == mc.CompareEntry(
        "map_attr", ITEM, "macro")
    assert [m.display_name for m in mc.MODEL_SELECTION_METRICS] == [
        "overall mAP", "macro per-item mAP"]
