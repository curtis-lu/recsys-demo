"""Categories read off a sample_pool column (#379): what ``prepare_eval_data``
lands as ``evaluation_item_categories``, and how its readers use it.

Column mode is ``evaluation.item_categories.column``; it needs
``--post-training`` (A51), so the population here is always sample_pool.
"""

import pandas as pd
import pytest

from recsys_tfb.core.consistency import DataConsistencyError

pytestmark = pytest.mark.spark

JAN, FEB = "2025-01-31", "2025-02-28"


def _params(snap_date=JAN, column="family", **item_categories):
    block = {"enabled": True, "unmapped": "singleton"}
    if column is not None:
        block["column"] = column
    block.update(item_categories)
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
            "categorical_values": {"prod_name": ["A", "B", "C"]},
        },
        "model_version": "mv1",
        "post_training": True,
        "evaluation": {"snap_date": snap_date, "k_values": [1, "all"],
                       "item_categories": block},
    }


#: (cust, item, score, label) per month. c1's positive is B, c2's is A.
_ROWS = [("c1", "A", 0.9, 0), ("c1", "B", 0.5, 1), ("c1", "C", 0.1, 0),
         ("c2", "A", 0.2, 1), ("c2", "B", 0.8, 0), ("c2", "C", 0.3, 0)]


def _predictions(spark, months=(JAN,)):
    """training_eval_predictions: the label rides with the score."""
    return spark.createDataFrame(pd.DataFrame(
        [(c, m, i, s, lab) for m in months for c, i, s, lab in _ROWS],
        columns=["cust_id", "snap_date", "prod_name", "score", "label"]))


def _labels(spark, months=(JAN,)):
    return spark.createDataFrame(pd.DataFrame(
        [(c, m, i, lab) for m in months for c, i, _s, lab in _ROWS],
        columns=["cust_id", "snap_date", "prod_name", "label"]))


def _sample_pool(spark, family_by_row):
    """``family_by_row``: ``{(month, cust, item): family}``; NULL is None."""
    return spark.createDataFrame(
        [(m, c, i, f) for (m, c, i), f in family_by_row.items()],
        "snap_date STRING, cust_id STRING, prod_name STRING, family STRING")


def _grid(months, family_of_item):
    """Every (month, cust, item) of ``_ROWS`` with ``family_of_item[item]``."""
    return {(m, c, i): family_of_item[i]
            for m in months for c, i, _s, _lab in _ROWS}


def _prepare(spark, params, pool, months=(JAN,)):
    from recsys_tfb.pipelines.evaluation.nodes import make_prepare_eval_data_node

    return make_prepare_eval_data_node("sample_pool")(
        _predictions(spark, months), _labels(spark, months), pool, params)


class TestPrepareEvalDataLandsTheTable:
    def test_the_table_is_read_off_the_column_in_the_evaluated_months(self, spark):
        """A train-month row saying A is ``z`` is not read (#379 decision 9):
        only the months this run evaluates."""
        from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
            fingerprint,
        )

        rows = _grid([JAN], {"A": "x", "B": "y", "C": "x"})
        rows[("2024-12-31", "c1", "A")] = "z"
        params = _params()
        _frame, _segments, categories = _prepare(
            spark, params, _sample_pool(spark, rows))
        assert categories == {
            "column": "family",
            "mapping": {"A": "x", "B": "y", "C": "x"},
            "config_fingerprint": fingerprint(params),
        }

    def test_a_hand_mapping_lands_an_empty_table(self, spark):
        """Every mode lands the file; the hand-written mapping stays in the
        conf, where its readers read it."""
        params = _params(column=None, mapping={"xy": ["A", "B"]})
        _frame, _segments, categories = _prepare(
            spark, params, _sample_pool(spark, _grid([JAN], {i: "x" for i in "ABC"})))
        assert categories["column"] is None
        assert categories["mapping"] == {}

    def test_one_item_two_categories_in_one_month_is_refused(self, spark):
        rows = _grid([JAN], {"A": "x", "B": "y", "C": "x"})
        rows[(JAN, "c2", "A")] = "w"
        with pytest.raises(DataConsistencyError) as excinfo:
            _prepare(spark, _params(), _sample_pool(spark, rows))
        message = str(excinfo.value)
        assert "B18" in message
        assert "'A'" in message
        assert f"'w' ({JAN})" in message and f"'x' ({JAN})" in message

    def test_one_item_two_categories_across_months_is_refused(self, spark):
        """The table is joined on the item alone over every evaluated month
        (#379 decision 5), so A in two categories would have its rows counted
        in both."""
        rows = {**_grid([JAN], {"A": "x", "B": "y", "C": "x"}),
                **_grid([FEB], {"A": "w", "B": "y", "C": "x"})}
        with pytest.raises(DataConsistencyError) as excinfo:
            _prepare(spark, _params(snap_date=[JAN, FEB]),
                     _sample_pool(spark, rows), months=(JAN, FEB))
        message = str(excinfo.value)
        assert "'A'" in message
        assert f"'x' ({JAN})" in message and f"'w' ({FEB})" in message
        assert "'B'" not in message and "'C'" not in message

    def test_a_null_is_ignored_where_another_row_names_the_category(self, spark):
        rows = _grid([JAN], {"A": "x", "B": "y", "C": "x"})
        rows[(JAN, "c2", "A")] = None
        _frame, _segments, categories = _prepare(
            spark, _params(), _sample_pool(spark, rows))
        assert categories["mapping"]["A"] == "x"

    def test_an_item_with_only_nulls_is_its_own_category(self, spark):
        rows = _grid([JAN], {"A": "x", "B": "y", "C": None})
        _frame, _segments, categories = _prepare(
            spark, _params(), _sample_pool(spark, rows))
        assert categories["mapping"] == {"A": "x", "B": "y", "C": "C"}

    def test_a_column_sample_pool_lacks_is_refused(self, spark):
        rows = _grid([JAN], {"A": "x", "B": "y", "C": "x"})
        with pytest.raises(ValueError, match="'familly'.*sample_pool"):
            _prepare(spark, _params(column="familly"), _sample_pool(spark, rows))

    def test_the_category_can_be_one_of_the_item_columns(self, spark):
        """#379 decision 11: combining drops the source columns, so the
        category is read next to the combined value, not after it."""
        from pyspark.sql import functions as F

        params = _params(column="campaign")
        params["schema"]["columns"]["item"] = ["campaign", "fmt"]
        params["schema"].pop("categorical_values")
        predictions = (_predictions(spark)
                       .withColumn("item", F.concat("prod_name", F.lit("-v")))
                       .drop("prod_name"))
        labels = (_labels(spark).withColumn("campaign", F.col("prod_name"))
                  .withColumn("fmt", F.lit("v")).drop("prod_name"))
        pool = (_sample_pool(spark, _grid([JAN], {i: None for i in "ABC"}))
                .withColumn("campaign", F.col("prod_name"))
                .withColumn("fmt", F.lit("v")).drop("prod_name", "family"))
        from recsys_tfb.pipelines.evaluation.nodes import make_prepare_eval_data_node

        _frame, _segments, categories = make_prepare_eval_data_node(
            "sample_pool")(predictions, labels, pool, params)
        assert categories["mapping"] == {"A-v": "A", "B-v": "B", "C-v": "C"}


# ---------------------------------------------------------------------------
# The readers
# ---------------------------------------------------------------------------


#: A grouping the conf does not spell: a reader that fell back to it would
#: rank A, B and C as three singletons.
_FAMILY = {"A": "x", "B": "y", "C": "x"}


def _landed(spark, params, family_of_item=_FAMILY, months=(JAN,)):
    """``(partition rows, segment JSON, category JSON)`` one prepare run lands."""
    return _prepare(spark, params,
                    _sample_pool(spark, _grid(months, family_of_item)), months)


class TestReadersRankTheLandedTable:
    def test_compute_metrics(self, spark):
        """c1: x=max(.9,.1) no, y=.5 yes → y at rank 2. c2: x=max(.2,.3)
        yes, y=.8 no → x at rank 2."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = _params()
        frame, segments, categories = _landed(spark, params)
        result = compute_metrics(frame, segments, params, categories)
        assert set(result["category"]["per_item"]) == {"x", "y"}
        assert result["category"]["overall"]["map@1"] == pytest.approx(0.0)
        assert result["category"]["dataset_overview"]["totals"]["n_items"] == 2

    def test_compute_baseline_metrics(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_baseline_metrics

        params = _params()
        params["evaluation"]["baseline"] = {"lookback_months": 12}
        frame, segments, categories = _landed(spark, params)
        result = compute_baseline_metrics(
            frame, _labels(spark, ("2024-12-31",)), segments, params, categories)
        assert set(result["category"]["per_item"]) == {"x", "y"}

    def test_generate_comparison_report_ranks_both_sides_by_it(
        self, spark, monkeypatch,
    ):
        from recsys_tfb.pipelines.evaluation import nodes

        seen = {}

        def capture(metrics_a, metrics_b, *_a, **_k):
            seen["a"], seen["b"] = metrics_a, metrics_b
            return {}

        monkeypatch.setattr(nodes, "build_comparison_result", capture)
        monkeypatch.setattr(nodes, "assemble_comparison_report",
                            lambda *a, **k: "<html/>")
        params = _params()
        frame, segments, categories = _landed(spark, params)
        nodes.generate_comparison_report(
            frame, frame.drop("eval_partition_fingerprint"), {}, segments,
            params, categories)
        assert set(seen["a"]["category"]["per_item"]) == {"x", "y"}
        assert set(seen["b"]["category"]["per_item"]) == {"x", "y"}


class TestAStaleOrMissingTableIsRefused:
    def test_changing_the_column_then_resuming_from_compute_metrics(self, spark):
        """The table was read off ``family``; the conf now says ``family2``.
        A slice starting at compute_metrics reads the old table, so it must
        name prepare_eval_data, where the table is read (#379 decision 4)."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        frame, segments, categories = _landed(spark, _params())
        with pytest.raises(ValueError) as excinfo:
            compute_metrics(frame, segments, _params(column="family2"),
                            categories)
        message = str(excinfo.value)
        assert "evaluation.item_categories.column" in message
        assert "--from-node prepare_eval_data" in message

    def test_compare_only_refuses_a_table_read_off_another_column(self, spark):
        """``--compare-only`` compares no fingerprint with today's settings
        (``post_training`` is inert there), so the landed table's own
        ``column`` is the one thing that says it is stale."""
        from recsys_tfb.pipelines.evaluation import nodes

        frame, segments, categories = _landed(spark, _params())
        with pytest.raises(ValueError) as excinfo:
            nodes.generate_comparison_report(
                frame, frame.drop("eval_partition_fingerprint"), {}, segments,
                _params(column="family2"), categories)
        message = str(excinfo.value)
        assert "read off 'family'" in message
        assert "--from-node prepare_eval_data" in message

    def test_column_mode_without_the_table_names_the_way_out(self, spark):
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = _params()
        frame, segments, _categories = _landed(spark, params)
        with pytest.raises(ValueError, match="--from-node prepare_eval_data"):
            compute_metrics(frame, segments, params, None)

    def test_a_hand_mapping_reads_the_conf_with_no_table_at_all(self, spark):
        """An evaluation directory written before #379 has no
        item_categories.json; the optional catalog entry loads it as None and
        the readers take the mapping from parameters, as before."""
        from recsys_tfb.pipelines.evaluation.nodes import compute_metrics

        params = _params(column=None, mapping={"xy": ["A", "B"]})
        frame, segments, _categories = _landed(spark, params)
        result = compute_metrics(frame, segments, params, None)
        assert set(result["category"]["per_item"]) == {"xy"}


def test_compare_only_with_a_hand_mapping_and_no_json_runs_the_reader(
    spark, tmp_path, monkeypatch,
):
    """#379: ``--compare-only`` in a directory written before #379 (no
    item_categories.json) with a hand mapping. The CLI lets it through
    (``_compare_only_input_errors``); what must also hold is that the node
    reading it runs: the real catalog entry loads the missing file as None,
    and ``generate_comparison_report`` takes the mapping from parameters."""
    from pathlib import Path

    import yaml

    from recsys_tfb.core.catalog import DataCatalog
    from recsys_tfb.pipelines.evaluation import nodes

    entry = dict(yaml.safe_load(
        (Path(__file__).resolve().parents[3] / "conf" / "base" / "catalog.yaml")
        .read_text())["evaluation_item_categories"])
    entry["filepath"] = str(tmp_path / "item_categories.json")
    loaded = DataCatalog({"evaluation_item_categories": entry}).load(
        "evaluation_item_categories")
    assert loaded is None

    seen = {}

    def capture(metrics_a, metrics_b, *_a, **_k):
        seen["a"] = metrics_a
        return {}

    monkeypatch.setattr(nodes, "build_comparison_result", capture)
    monkeypatch.setattr(nodes, "assemble_comparison_report",
                        lambda *a, **k: "<html/>")
    params = _params(column=None, mapping={"xy": ["A", "B"]})
    frame, segments, _categories = _landed(spark, params)
    nodes.generate_comparison_report(
        frame, frame.drop("eval_partition_fingerprint"), {}, segments, params,
        loaded)
    assert set(seen["a"]["category"]["per_item"]) == {"xy"}
