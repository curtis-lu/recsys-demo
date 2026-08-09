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

    def test_partition_cols_start_with_snap_date_then_prod_name(self):
        """前兩欄釘死；尾巴由 ``TestEntityBucketStaysInternal`` 分開管。

        這條守的是 model_version 沒有偷偷混回 partition_cols；第三欄
        `entity_bucket` 只有 `unranked_predictions` 有（#188／ADR-0010 §5），
        那件事是另一條斷言。
        """
        d = _load_catalog()
        for name in INFERENCE_OUTPUT_TABLES:
            head = [(c["name"], c["type"]) for c in d[name]["partition_cols"]][:2]
            assert head == [
                ("snap_date", "STRING"), ("prod_name", "STRING")
            ], name

    def test_model_version_filter_matches_training_eval_predictions(self):
        """與訓練端的預測表同構——那不是巧合，是同一個問題被解過第二次。

        只比 `partition_filter`：那是 ADR-0010 §5 主張的那一半。`partition_cols`
        今天恰好也相同，但票 D 會讓 `unranked_predictions` 岔開，所以不拿它當
        同構的判準。
        """
        d = _load_catalog()
        reference = d["training_eval_predictions"]
        for name in INFERENCE_OUTPUT_TABLES:
            assert d[name]["partition_filter"] == reference["partition_filter"], name


class TestEntityBucketStaysInternal:
    """`entity_bucket` 是機制欄，不是對外契約的一部分（ADR-0010 §5）。

    它存在的唯一理由是讓一次 save 恰好碰一個分區（約束 C）。它進 `unranked_predictions`
    是**正確性需求**：少了它，第 2 個 bucket 的 save 會用 dynamic overwrite 把第 1 個
    bucket 剛寫的分區整個換掉，資料少 (B-1)/B 且零錯誤訊息。
    它不進對外表也是硬要求：所有下游讀取路徑都要跟著改，而且不可逆。
    """

    def test_unranked_predictions_partitions_by_entity_bucket(self):
        d = _load_catalog()
        part_names = [c["name"] for c in d["unranked_predictions"]["partition_cols"]]
        assert part_names == ["snap_date", "prod_name", "entity_bucket"]

    def test_the_published_tables_do_not(self):
        d = _load_catalog()
        for name in ("ranked_staging", "ranked_predictions"):
            part_names = [c["name"] for c in d[name]["partition_cols"]]
            assert part_names == ["snap_date", "prod_name"], name

    def test_it_is_not_a_data_column_of_any_published_table(self):
        """釘住的是「一欄都沒有」，不只是分區欄。

        `ranked_staging` 與 `ranked_predictions` 都是列舉式 `columns`，所以把它
        當普通資料欄偷偷帶下去也是可能的失效。
        """
        d = _load_catalog()
        for name in ("ranked_staging", "ranked_predictions"):
            col_names = [c["name"] for c in d[name]["columns"]]
            assert "entity_bucket" not in col_names, name


class TestInferencePopulationFeatures:
    """評分讀的中間表：不含 item 展開、存全集、以 base 版本 scope。"""

    def test_entry_present(self):
        d = _load_catalog()
        assert d["inference_population_features"]["type"] == "HiveTableDataset"
        assert (
            d["inference_population_features"]["table"]
            == "inference_population_features"
        )

    def test_scoped_by_base_dataset_version_not_model_version(self):
        """這是「同一個 base 版本下換模型可以整張重用」的前提。

        它只依賴來源表與 preprocessor.json，與模型無關。改成 model_version 會讓
        每次換模型都要重算整張表，而且沒有任何檢查會紅。
        """
        d = _load_catalog()
        entry = d["inference_population_features"]
        assert entry["partition_filter"] == {
            "base_dataset_version": "${base_dataset_version}"
        }
        assert "model_version" not in entry["partition_filter"]

    def test_columns_are_inferred_not_enumerated(self):
        """存的是 preprocessor.json 特徵欄的全集（扣掉 item）。

        列舉式 `columns` 等於把某一組特徵寫進 catalog，而全集會隨
        base_dataset_version 變。`auto` ＋ append-only schema evolution 是
        `preprocessed_feature_table` 已經在用的形狀。
        """
        d = _load_catalog()
        assert d["inference_population_features"]["columns"] == "auto"

    def test_partitioned_by_time_then_entity_bucket(self):
        d = _load_catalog()
        assert [
            (c["name"], c["type"])
            for c in d["inference_population_features"]["partition_cols"]
        ] == [("snap_date", "STRING"), ("entity_bucket", "STRING")]

    def test_item_is_not_a_partition_column(self):
        """顆粒度是 (time, entity)：沒有 item 展開，所以沒有 item 分區。"""
        d = _load_catalog()
        part_names = [
            c["name"] for c in d["inference_population_features"]["partition_cols"]
        ]
        assert "prod_name" not in part_names


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
