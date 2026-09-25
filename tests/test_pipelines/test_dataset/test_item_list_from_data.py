"""The dataset pipeline with ``schema.categorical_values[<item>]:
from_train_data`` (#379): the item list is counted from the train months'
sample_pool, fixed in the preprocessor, guarded against a silent change under
the same version, and the val/test items it lacks are warned about.
"""

import logging

import pandas as pd
import pytest

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import ITEM_LIST_FROM_TRAIN_DATA
from recsys_tfb.pipelines.dataset.nodes import (
    fit_preprocessor_metadata,
    validate_data_consistency,
)

pytestmark = pytest.mark.spark

TRAIN = ["2024-01-31", "2024-02-29"]
VAL, TEST = "2024-03-31", "2024-04-30"
ENTITIES = [f"C{i:02d}" for i in range(6)]


def _params(item_values=ITEM_LIST_FROM_TRAIN_DATA, **dataset):
    return {
        "random_seed": 42,
        "schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"],
                        "item": "prod_name"},
            "categorical_values": {"prod_name": item_values},
        },
        "dataset": {
            "train_snap_dates": list(TRAIN),
            "val_snap_dates": [VAL],
            "test_snap_dates": [TEST],
            "sample_ratio": 1.0,
            "sample_group_keys": ["prod_name"],
            "sample_ratio_overrides": {},
            **dataset,
        },
    }


def _rows(month, items, label=0):
    return [(pd.Timestamp(month), c, i, label) for c in ENTITIES for i in items]


def _sample_pool(spark, rows):
    return spark.createDataFrame(
        pd.DataFrame(rows, columns=["snap_date", "cust_id", "prod_name", "label"]))


def _feature_table(spark):
    return spark.createDataFrame(pd.DataFrame(
        [(pd.Timestamp(m), c, 1.0) for m in [*TRAIN, VAL, TEST] for c in ENTITIES],
        columns=["snap_date", "cust_id", "total_aum"]))


def _pool_with_a_rare_and_a_late_item(spark):
    """``rare`` is a candidate once in the train months, the sort of item a
    sampling rate drops; ``late`` first appears in val; one train row has no
    item at all."""
    rows = _rows(TRAIN[0], ["b", "a"]) + _rows(TRAIN[1], ["a", "b"])
    rows += [(pd.Timestamp(TRAIN[1]), "C00", "rare", 0),
             (pd.Timestamp(TRAIN[1]), "C01", None, 0)]
    rows += _rows(VAL, ["a", "late"]) + _rows(TEST, ["b", "late"])
    return _sample_pool(spark, rows)


class TestTheListIsCountedFromTheTrainMonths:
    def test_sorted_distinct_train_items_before_sampling(self, spark):
        preprocessor, mappings = fit_preprocessor_metadata(
            _feature_table(spark), _params(), None,
            _pool_with_a_rare_and_a_late_item(spark), None)
        assert preprocessor["category_mappings"]["prod_name"] == ["a", "b", "rare"]
        assert mappings["prod_name"] == ["a", "b", "rare"]

    def test_a_multi_column_item_is_counted_combined(self, spark):
        params = _params()
        params["schema"]["columns"]["item"] = ["campaign", "fmt"]
        params["schema"]["categorical_values"] = {"item": ITEM_LIST_FROM_TRAIN_DATA}
        params["dataset"]["sample_group_keys"] = ["item"]
        pool = spark.createDataFrame(pd.DataFrame(
            [(pd.Timestamp(TRAIN[0]), "C00", "c01", "banner", 0),
             (pd.Timestamp(TRAIN[1]), "C01", "c02", "video", 1),
             (pd.Timestamp(VAL), "C00", "c04", "video", 0)],
            columns=["snap_date", "cust_id", "campaign", "fmt", "label"]))
        preprocessor, _ = fit_preprocessor_metadata(
            _feature_table(spark), params, None, pool, None)
        assert preprocessor["category_mappings"]["item"] == ["c01-banner", "c02-video"]

    def test_a_listed_item_list_is_the_declared_one(self, spark):
        """Unchanged: the declared order, sample_pool not asked."""
        preprocessor, _ = fit_preprocessor_metadata(
            _feature_table(spark), _params(["b", "a"]), None,
            _pool_with_a_rare_and_a_late_item(spark), None)
        assert preprocessor["category_mappings"]["prod_name"] == ["b", "a"]


class TestTheSameVersionCannotChangeItsList:
    """#379: the counted list is not in the conf, so base_dataset_version
    does not move with it; a rerun over changed train-month data would
    overwrite the preprocessor an existing model encodes by."""

    def _existing(self, items):
        return {"feature_columns": ["prod_name"], "categorical_columns": ["prod_name"],
                "category_mappings": {"prod_name": list(items)}, "drop_columns": []}

    def test_a_changed_list_is_refused_naming_what_moved(self, spark):
        """The way on moves the version; deleting the directory is the last
        resort, because the models on it that are not retrained decode items
        by the shifted codes without an error — the thing B19 exists for."""
        with pytest.raises(DataConsistencyError,
                           match=r"not retrained.*silently wrong") as excinfo:
            fit_preprocessor_metadata(
                _feature_table(spark), _params(), None,
                _pool_with_a_rare_and_a_late_item(spark),
                self._existing(["a", "b", "gone"]))
        message = str(excinfo.value)
        assert "['rare']" in message and "['gone']" in message
        assert "schema.categorical_values.prod_name" in message
        assert "dataset.train_snap_dates" in message
        assert message.index("dataset.train_snap_dates") \
            < message.index("data/dataset/")

    def test_the_same_list_passes(self, spark):
        preprocessor, _ = fit_preprocessor_metadata(
            _feature_table(spark), _params(), None,
            _pool_with_a_rare_and_a_late_item(spark),
            self._existing(["a", "b", "rare"]))
        assert preprocessor["category_mappings"]["prod_name"] == ["a", "b", "rare"]

    def test_no_preprocessor_on_disk_passes(self, spark):
        fit_preprocessor_metadata(
            _feature_table(spark), _params(), None,
            _pool_with_a_rare_and_a_late_item(spark), None)


class TestA5RunsAgainstTheCountedList:
    def test_a_typo_in_an_override_key_is_refused(self, spark):
        """With the text the CLI entry raises for a listed list: one
        predicate's message at both sites."""
        from recsys_tfb.core.consistency import override_unknown_item_errors

        params = _params(sample_ratio_overrides={"rar": 0.5})
        with pytest.raises(DataConsistencyError, match="'rar'") as excinfo:
            fit_preprocessor_metadata(
                _feature_table(spark), params, None,
                _pool_with_a_rare_and_a_late_item(spark), None)
        assert str(excinfo.value) == override_unknown_item_errors(
            params, items=["a", "b", "rare"])[0]
        assert str(excinfo.value).startswith("A5: ")

    def test_an_override_on_a_counted_item_passes(self, spark):
        params = _params(sample_ratio_overrides={"rare": 0.5})
        fit_preprocessor_metadata(
            _feature_table(spark), params, None,
            _pool_with_a_rare_and_a_late_item(spark), None)


class TestB1WithACountedList:
    def _labels(self, spark, extra=()):
        rows = [(pd.Timestamp(TRAIN[0]), "C00", "a", 1),
                (pd.Timestamp(VAL), "C01", "late", 1), *extra]
        return spark.createDataFrame(pd.DataFrame(
            rows, columns=["snap_date", "cust_id", "prod_name", "label"]))

    def test_a_val_only_item_is_not_refused(self, spark):
        assert validate_data_consistency(
            _pool_with_a_rare_and_a_late_item(spark), self._labels(spark),
            _feature_table(spark), _params()) is None

    def test_a_label_for_an_item_no_candidate_holds_is_refused(self, spark):
        labels = self._labels(spark, [(pd.Timestamp(TEST), "C02", "ghost", 1)])
        with pytest.raises(DataConsistencyError, match="ghost"):
            validate_data_consistency(
                _pool_with_a_rare_and_a_late_item(spark), labels,
                _feature_table(spark), _params())


#: A month the test keys hold but this run does not build: ``test_keys`` is a
#: persistent table holding every month under the version.
LATER = "2024-05-31"


def _build_node(name, only_test_months=False):
    """The function the pipeline runs under ``name`` — what a run calls, not
    whatever this module happens to import under that name."""
    from recsys_tfb.pipelines.dataset.pipeline import create_pipeline

    return next(n for n in create_pipeline(only_test_months=only_test_months).nodes
                if n.name == name).func


def _new_item_warnings(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING and "new item" in r.getMessage()]


class TestNewItemsAreWarnedWhereTheyAreEncoded:
    """The warning is in the val / test build, off the landed keys table —
    one column, no join — and reads the preprocessor the encoding reads, so
    under ``--only-test-months`` (no fit) it is the one on disk."""

    PRE = {"feature_columns": ["prod_name", "total_aum"],
           "categorical_columns": ["prod_name"],
           "category_mappings": {"prod_name": ["a", "b"]},
           "drop_columns": []}

    def _build(self, spark, split, rows, params, to_process=(TEST,)):
        """Run the split's build node over keys and labels made of ``rows``."""
        from recsys_tfb.pipelines.dataset.month_plans import SnapDatePlan

        labels = _sample_pool(spark, rows)
        keys = labels.select("snap_date", "cust_id", "prod_name")
        if split == "val":
            return _build_node("build_val_model_input")(
                keys, _feature_table(spark), labels, self.PRE, params)
        plan = SnapDatePlan(to_process=[pd.Timestamp(m) for m in to_process],
                            skipped=[])
        return _build_node("build_test_model_input")(
            keys, _feature_table(spark), labels, self.PRE, plan, params)

    @pytest.mark.parametrize("split, month", [("val", VAL), ("test", TEST)])
    def test_a_new_item_is_named_and_kept(self, spark, caplog, split, month):
        with caplog.at_level(logging.WARNING):
            built = self._build(spark, split, _rows(month, ["a", "late"], 1),
                                _params())
        assert {r["prod_name"] for r in built.select("prod_name").distinct().collect()} \
            == {"a", "late"}
        warnings = _new_item_warnings(caplog)
        assert any("['late']" in m for m in warnings), warnings

    def test_a_month_the_build_does_not_process_is_not_scanned(self, spark, caplog):
        """``ghost`` is new too, but only in a month of the keys table this
        run does not build: naming it would warn about rows this run never
        encodes."""
        rows = _rows(TEST, ["a", "late"], 1) + _rows(LATER, ["a", "ghost"], 1)
        with caplog.at_level(logging.WARNING):
            self._build(spark, "test", rows, _params(), to_process=(TEST,))
        warnings = _new_item_warnings(caplog)
        # "late", not "['late']": this test is about "ghost" alone, and the
        # exact list would turn red on the wrong line.
        assert any("late" in m for m in warnings), warnings
        assert not any("ghost" in m for m in warnings), warnings

    def test_no_new_item_no_warning(self, spark, caplog):
        with caplog.at_level(logging.WARNING):
            self._build(spark, "val", _rows(VAL, ["a", "b"], 1), _params())
        assert not _new_item_warnings(caplog)

    def test_a_listed_item_list_has_no_new_items(self, spark, caplog):
        """B1 refuses them upstream; nothing to count here."""
        with caplog.at_level(logging.WARNING):
            self._build(spark, "val", _rows(VAL, ["a", "late"], 1),
                        _params(["a", "b", "late"]))
        assert not _new_item_warnings(caplog)


class TestWiring:
    def test_fit_reads_sample_pool_and_the_preprocessor_on_disk_last(self):
        from recsys_tfb.pipelines.dataset.pipeline import create_pipeline

        fit = next(n for n in create_pipeline().nodes
                   if n.name == "fit_preprocessor_metadata")
        assert fit.inputs == ["feature_table", "parameters",
                              "candidate_feature_table", "sample_pool",
                              "preprocessor_on_disk"]

    def test_the_preprocessor_on_disk_is_an_optional_entry_on_the_same_file(self):
        """A6: a node may not read and write one name. Overwriting is done
        under another entry name — the same file, loaded before the save."""
        from pathlib import Path

        import yaml

        catalog = yaml.safe_load(
            (Path(__file__).parents[3] / "conf/base/catalog.yaml").read_text())
        assert catalog["preprocessor_on_disk"] == {
            "type": "JSONDataset",
            "filepath": catalog["preprocessor"]["filepath"],
            "optional": True,
        }

    @pytest.mark.parametrize("only_test_months", [False, True])
    def test_the_builds_warn_off_the_keys_and_the_preprocessor(self, only_test_months):
        """The warning's inputs are the build's own: the landed keys table and
        the preprocessor. The group drops before the build do not read the
        preprocessor — they are what they were before #379, moved onto the
        keys (ADR-0029 decision 4)."""
        from recsys_tfb.pipelines.dataset.pipeline import create_pipeline

        pipeline = create_pipeline(only_test_months=only_test_months)
        nodes = {n.name: n for n in pipeline.nodes}
        splits = ["test"] if only_test_months else ["val", "test"]
        for split in splits:
            build = nodes[f"build_{split}_model_input"]
            assert build.inputs[0] == f"{split}_keys"
            assert "preprocessor" in build.inputs
            assert "preprocessor" not in nodes[f"filter_{split}_keys"].inputs
            assert nodes[f"filter_{split}_keys"].outputs == [f"{split}_keys"]
        if only_test_months:
            # No fit in this mode, and no node produces the preprocessor: the
            # build loads the one on disk, the list its months are encoded by.
            assert "fit_preprocessor_metadata" not in nodes
            assert not any("preprocessor" in n.outputs for n in pipeline.nodes)
