"""What the dataset command reads off the metastore about train versions
(ADR-0029 decision 12).

The listing goes through a real ``HiveTableDataset``; only the SparkSession
under it is faked, answering ``SHOW PARTITIONS`` per physical table. So what
is pinned is the scoping — which partitions count for this base, and which
for this variant — not how the module happens to call the dataset.
"""

import logging
from unittest.mock import MagicMock, patch

from recsys_tfb.pipelines.dataset.run_contract import (
    TrainVersionLanding,
    train_version_landing,
    unlanded_train_tables,
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
