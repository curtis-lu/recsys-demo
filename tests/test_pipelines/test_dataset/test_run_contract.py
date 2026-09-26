"""What the dataset command asks this package about a run (ADR-0029
decisions 11 and 12), tested without the command.

Where a listing is involved it goes through a real ``HiveTableDataset``; only
the SparkSession under it is faked, answering ``SHOW PARTITIONS`` per physical
table. So what is pinned is the scoping — which partitions count for this
base, this variant, this time column — not how the module happens to call the
dataset.

Pinned through the command instead, in ``tests/test_cli.py``: the dict
:func:`pipeline_inputs` injects (``TestDatasetRegistersTheCandidateTable``,
``TestMonthPlansReachTheCatalog``), because what matters there is the names the
nodes read it back under.
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from recsys_tfb.core.catalog import DataCatalog
from recsys_tfb.core.versioning import compute_feature_table_fingerprint
from recsys_tfb.pipelines.dataset.run_contract import (
    SourceFingerprints,
    TrainVersionLanding,
    _collect_existing_snap_dates,
    month_plans_for_run,
    source_fingerprints,
    train_version_landing,
    unlanded_train_tables,
    versions_for_run,
)

BASE, VARIANT = "b1111111", "v1111111"


def _entry(table):
    """A train table's entry as the command resolves it: both versions
    already substituted into ``partition_filter``."""
    return {
        "type": "HiveTableDataset",
        "database": "ml_recsys",
        "table": table,
        "external": False,
        "columns": "auto",
        "partition_filter": {
            "base_dataset_version": BASE,
            "train_variant_id": VARIANT,
        },
        "partition_cols": [{"name": "snap_date", "type": "STRING"}],
    }


CONFIG = {
    "train_model_input": _entry("recsys_prod_train_model_input"),
    "train_dev_model_input": _entry("recsys_prod_train_dev_model_input"),
}

#: A config with a dev split, and one without: ``train_dev_ratio: 0`` is a
#: supported setting that leaves train_dev empty on purpose.
WITH_DEV = {"dataset": {"train_dev_ratio": 0.2}}
NO_DEV = {"dataset": {"train_dev_ratio": 0}}


def _spec(base=BASE, variant=VARIANT):
    return f"base_dataset_version={base}/train_variant_id={variant}/snap_date=2025-12-31"


def _metastore(**listings):
    """Patch the session so ``SHOW PARTITIONS <db>.<table>`` lists
    ``listings[<table>]``; a table not named has no partitions."""
    spark = MagicMock()

    def sql(query):
        result = MagicMock()
        table = query.split()[2].split(".")[-1]
        result.collect.return_value = [(s,) for s in listings.get(table, [])]
        return result

    spark.sql.side_effect = sql
    return patch(
        "recsys_tfb.utils.spark.get_or_create_spark_session", return_value=spark,
    )


class TestTrainVersionLanding:
    def test_this_variant_has_partitions(self):
        with _metastore(recsys_prod_train_model_input=[_spec()]):
            assert train_version_landing(CONFIG) == TrainVersionLanding(
                base=True, variant=True,
            )

    def test_only_another_variant_of_this_base_has_partitions(self):
        # The #334 state: the sampling settings moved train_variant_id only.
        with _metastore(recsys_prod_train_model_input=[_spec(variant="v2222222")]):
            assert train_version_landing(CONFIG) == TrainVersionLanding(
                base=True, variant=False,
            )

    def test_another_base_does_not_count_for_this_one(self):
        with _metastore(recsys_prod_train_model_input=[_spec(base="b2222222")]):
            assert train_version_landing(CONFIG) == TrainVersionLanding(
                base=False, variant=False,
            )

    def test_train_dev_alone_does_not_count(self):
        # The question is asked of train_model_input, the table #334 empties.
        with _metastore(recsys_prod_train_dev_model_input=[_spec()]):
            assert train_version_landing(CONFIG) == TrainVersionLanding(
                base=False, variant=False,
            )

    def test_an_entry_that_cannot_list_counts_as_not_landed(self, caplog):
        config = {"train_model_input": {
            "type": "ParquetDataset", "filepath": "data/train.parquet",
        }}
        with caplog.at_level(logging.WARNING), _metastore():
            assert train_version_landing(config) == TrainVersionLanding(
                base=False, variant=False,
            )
        assert "train_model_input" in caplog.text

    def test_an_absent_entry_counts_as_not_landed(self):
        with _metastore():
            assert train_version_landing({}) == TrainVersionLanding(
                base=False, variant=False,
            )


class TestUnlandedTrainTables:
    def test_both_tables_under_this_variant_means_built(self):
        with _metastore(
            recsys_prod_train_model_input=[_spec()],
            recsys_prod_train_dev_model_input=[_spec()],
        ):
            assert unlanded_train_tables(CONFIG, WITH_DEV) == []

    def test_names_the_table_that_is_missing(self):
        with _metastore(recsys_prod_train_model_input=[_spec()]):
            assert unlanded_train_tables(CONFIG, WITH_DEV) == ["train_dev_model_input"]

    def test_another_variant_does_not_count(self):
        other = [_spec(variant="v2222222")]
        with _metastore(
            recsys_prod_train_model_input=other,
            recsys_prod_train_dev_model_input=other,
        ):
            assert unlanded_train_tables(CONFIG, WITH_DEV) == [
                "train_model_input", "train_dev_model_input",
            ]

    def test_without_a_dev_split_train_dev_is_not_required(self):
        # An empty frame writes no partition, so a correctly built variant
        # with train_dev_ratio 0 has none in train_dev_model_input — requiring
        # one would leave `latest` unpublishable for that config.
        with _metastore(recsys_prod_train_model_input=[_spec()]):
            assert unlanded_train_tables(CONFIG, NO_DEV) == []

    def test_without_a_dev_split_train_model_input_is_still_required(self):
        with _metastore(recsys_prod_train_dev_model_input=[_spec()]):
            assert unlanded_train_tables(CONFIG, NO_DEV) == ["train_model_input"]


class TestCollectExistingSnapDates:
    def _catalog(self, listings):
        catalog = DataCatalog()
        for name, specs in listings.items():
            dataset = MagicMock()
            dataset.existing_partition_values.return_value = specs
            catalog.add(name, dataset)
        return catalog

    def test_asks_each_dataset_object_for_its_own_partitions(self):
        out = _collect_existing_snap_dates(
            self._catalog({
                "test_keys": [{"as_of": "2026-01-31"}],
                "test_model_input": [
                    {"as_of": "2026-02-28", "prod_name": "fund_stock"},
                ],
            }),
            time_col="as_of",
        )

        # time_col is threaded through, not hardcoded: the framework's time
        # column is configurable via schema.time.
        assert out == {
            "test_keys": ["2026-01-31"],
            "test_model_input": ["2026-02-28"],
        }

    def test_a_dataset_that_cannot_list_partitions_is_rebuilt_in_full(self, caplog):
        catalog = self._catalog({"test_keys": [{"snap_date": "2026-01-31"}]})
        # A ParquetDataset has no existing_partition_values; absent from the
        # result means build_month_plans reads it as "nothing has landed".
        catalog.add("preprocessed_feature_table", SimpleNamespace())

        with caplog.at_level(logging.WARNING):
            out = _collect_existing_snap_dates(catalog, time_col="snap_date")

        # Exact, not "not in": an absent key and a `[]` value are the same
        # answer to build_month_plans but not the same behaviour here, and
        # `test_model_input` (registered nowhere at all) must take the same
        # route rather than raising.
        assert out == {"test_keys": ["2026-01-31"]}
        # Asserted because a silent skip is what makes this dangerous: the run
        # rebuilds a whole artifact and only this line says why.
        assert "preprocessed_feature_table" in caplog.text


# --- Before the run: fingerprints, versions, month plans --------------------

_FEATURES = [("entity_key", "string"), ("t", "date"), ("f1", "double")]
_CANDIDATE_FEATURES = [
    ("entity_key", "string"), ("t", "date"), ("item_key", "string"), ("c1", "int"),
]
_SOURCES = {
    "feature_table": {
        "type": "HiveTableDataset", "database": "src", "table": "features",
    },
}
_WITH_CANDIDATE = {
    **_SOURCES,
    "candidate_feature_table": {
        "type": "HiveTableDataset", "database": "src", "table": "candidate_features",
    },
}


def _source_spark():
    """A session whose ``spark.table`` answers each source table's schema."""
    schemas = {
        "src.features": _FEATURES, "src.candidate_features": _CANDIDATE_FEATURES,
    }

    def table(fqn):
        fields = [
            SimpleNamespace(name=n, dataType=SimpleNamespace(simpleString=lambda t=t: t))
            for n, t in schemas[fqn]
        ]
        return SimpleNamespace(schema=SimpleNamespace(fields=fields))

    spark = MagicMock()
    spark.table.side_effect = table
    return spark


def _schema_params(time="t"):
    return {"schema": {"columns": {
        "time": time, "entity": ["entity_key"], "item": "item_key",
    }}}


class TestSourceFingerprints:
    def test_the_feature_table_fingerprint_covers_every_column_in_order(self):
        """Every column, in the table's order, under Spark's type name: the
        column order becomes the model's feature order, so it is identity."""
        sources = source_fingerprints(_source_spark(), _SOURCES)

        assert sources.feature_table == compute_feature_table_fingerprint(_FEATURES)

    def test_a_declared_candidate_table_is_fingerprinted_from_its_own_schema(self):
        sources = source_fingerprints(_source_spark(), _WITH_CANDIDATE)

        assert sources.candidate_declared
        assert sources.candidate == compute_feature_table_fingerprint(
            _CANDIDATE_FEATURES,
        )

    def test_undeclared_there_is_no_candidate_fingerprint(self):
        sources = source_fingerprints(_source_spark(), _SOURCES)

        assert sources.candidate is None
        assert not sources.candidate_declared


class TestVersionsForRun:
    PARAMS_DATASET = {"dataset": {"sample_ratio": 0.1}}
    SOURCES = SourceFingerprints(feature_table="f0000000", candidate=None)

    def test_the_schema_moves_the_base_version_and_not_the_variant(self):
        a = versions_for_run(_schema_params("t"), self.PARAMS_DATASET, self.SOURCES)
        b = versions_for_run(_schema_params("t2"), self.PARAMS_DATASET, self.SOURCES)

        assert a.base_dataset_version != b.base_dataset_version
        assert a.train_variant_id == b.train_variant_id


class TestMonthPlansForRun:
    def test_the_listing_reads_the_configured_time_column(self):
        """``schema.time`` names the partition column. A deployment whose time
        column is not ``snap_date`` must still see its landed months, or every
        run rebuilds the whole test chain."""
        entry = {
            "type": "HiveTableDataset",
            "database": "ml_recsys",
            "table": "recsys_prod_test_model_input",
            "external": False,
            "columns": "auto",
            "partition_filter": {"base_dataset_version": BASE},
            "partition_cols": [{"name": "as_of", "type": "STRING"}],
        }
        params = {
            **_schema_params("as_of"),
            "dataset": {
                "train_snap_dates": ["2025-12-31"],
                "test_snap_dates": ["2026-01-31", "2026-02-28"],
            },
        }

        with _metastore(recsys_prod_test_model_input=[
            f"base_dataset_version={BASE}/as_of=2026-01-31",
        ]):
            plans = month_plans_for_run(
                {"test_model_input": entry}, params, rebuild=(),
            )

        assert plans["test_model_input"].skipped == [pd.Timestamp("2026-01-31")]
        assert plans["test_model_input"].to_process == [pd.Timestamp("2026-02-28")]
