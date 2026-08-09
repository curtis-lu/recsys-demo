"""Catalog regression: inference output entries reflect the staging gate.

yaml.safe_load on catalog.yaml is an established pattern in this repo (the
${...} placeholders are plain string scalars). Pure-Python, no Spark.
"""

from pathlib import Path

import yaml

# The three tables the inference pipeline writes, in DAG order.
INFERENCE_OUTPUT_TABLES = (
    "unranked_predictions",
    "ranked_staging",
    "ranked_predictions",
)


def _load_catalog():
    # tests/test_core/<this file> -> parents[2] == repo (worktree) root
    root = Path(__file__).resolve().parents[2]
    catalog_path = root / "conf" / "base" / "catalog.yaml"
    return yaml.safe_load(catalog_path.read_text())


def test_ranked_staging_entry_present():
    d = _load_catalog()
    assert "ranked_staging" in d
    assert d["ranked_staging"]["type"] == "HiveTableDataset"
    assert d["ranked_staging"]["table"] == "ranked_staging"


def test_validated_predictions_entry_removed():
    """validate 的 output 現在是 in-DAG MemoryDataset,不再是 Hive 表。"""
    d = _load_catalog()
    assert "validated_predictions" not in d


def test_ranked_predictions_still_declared():
    """production 輸出 + standalone evaluation 讀取入口必須保留宣告。"""
    d = _load_catalog()
    assert "ranked_predictions" in d
    assert d["ranked_predictions"]["table"] == "ranked_predictions"


class TestUnrankedPredictionsRename:
    """`_table` 後綴在本 repo 只用於來源表（feature_table／label_table…）。

    改名連 catalog 條目名與 Hive 表名一起換，所以舊名不得以任何形式殘留：
    留一個舊表名會讓 DROP 遷移只清掉一半。
    """

    def test_score_table_entry_gone(self):
        d = _load_catalog()
        assert "score_table" not in d

    def test_unranked_predictions_declared(self):
        d = _load_catalog()
        assert "unranked_predictions" in d
        assert d["unranked_predictions"]["type"] == "HiveTableDataset"
        assert d["unranked_predictions"]["table"] == "unranked_predictions"

    def test_no_entry_still_points_at_the_old_hive_table(self):
        d = _load_catalog()
        assert not [
            name for name, entry in d.items()
            if isinstance(entry, dict) and entry.get("table") == "score_table"
        ]


class TestInferenceTablesPartitionByModelVersionFilter:
    """`model_version` 是 partition_filter，不是 partition_col（ADR-0010 §5）。

    這是 #187 的核心結構改動，而它有兩個各自獨立的後果，所以分開斷言：
    ``existing_partition_values()`` 只有在有 filter 時才答得出「這次寫了什麼」
    （寫入後的分區回報走 metastore 快照），而續跑的規劃器（票 D）如果照抄
    training 的形狀、又留 `model_version` 在 partition_cols，就會把上個模型
    版本寫的分區算成本次已完成。
    """

    def test_every_inference_output_filters_on_model_version(self):
        d = _load_catalog()
        for name in INFERENCE_OUTPUT_TABLES:
            assert d[name].get("partition_filter") == {
                "model_version": "${model_version}"
            }, name

    def test_model_version_is_not_a_partition_col(self):
        d = _load_catalog()
        for name in INFERENCE_OUTPUT_TABLES:
            part_names = [c["name"] for c in d[name]["partition_cols"]]
            assert "model_version" not in part_names, name

    def test_partition_cols_are_snap_date_then_prod_name(self):
        d = _load_catalog()
        for name in INFERENCE_OUTPUT_TABLES:
            assert [
                (c["name"], c["type"]) for c in d[name]["partition_cols"]
            ] == [("snap_date", "STRING"), ("prod_name", "STRING")], name

    def test_shape_matches_training_eval_predictions(self):
        """三張表修完之後與訓練端的預測表同構——那不是巧合，是同一個問題。"""
        d = _load_catalog()
        reference = d["training_eval_predictions"]
        for name in INFERENCE_OUTPUT_TABLES:
            assert d[name]["partition_filter"] == reference["partition_filter"], name
            assert d[name]["partition_cols"] == reference["partition_cols"], name


class TestNoWritableTableIsLeftOnTheFrameRescanPath:
    """有 partition_cols 但無 partition_filter 的可寫表＝每次寫入重跑整條 lineage。

    ``HiveTableDataset.save()`` 只在有 partition_filter 時走 metastore 快照
    （`io/hive_table_dataset.py` 的 `scoped_to_this_run`）；沒有的話 fallback
    到 `_log_partitions_from_frame()`，為了印一行 log 把整條 lineage 跑第二次
    （ADR-0009 量到 4 個額外 job／17 個額外 task）。#187 之前那三張推論表是
    全 repo 僅存的三筆，這條斷言是防它們再長回來。
    """

    def test_all_writable_partitioned_tables_declare_a_filter(self):
        d = _load_catalog()
        offenders = [
            name for name, entry in d.items()
            if isinstance(entry, dict)
            and entry.get("type") == "HiveTableDataset"
            and not entry.get("read_only")
            and entry.get("partition_cols")
            and not entry.get("partition_filter")
        ]
        assert offenders == []
