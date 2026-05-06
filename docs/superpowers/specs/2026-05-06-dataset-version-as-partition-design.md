# Dataset Hive Tables: Version as Partition Column

**Status:** Draft
**Date:** 2026-05-06
**Author:** curtis-lu (with Claude)

## Goal

把 dataset pipeline 寫到 Hive 的 11 張 DataFrame artifact 表，從現在的「version 字串塞進 table 名」(`val_model_input_${base_dataset_version}`) 改成「version 字串作為 Hive partition column」(`val_model_input` PARTITIONED BY `(base_dataset_version, ...)`)。

## Motivation

依優先順序：

1. **Hive metastore 乾淨**（主訴求）：避免 `SHOW TABLES` 隨著版本累積而塞爆，11 張邏輯 artifact 永遠就是 11 張表。
2. **同表跨版本 SQL 比對**：`SELECT * FROM val_model_input WHERE base_dataset_version IN ('X', 'Y')` 直接做回歸 / diff，不必跨表 UNION。
3. **Partition pruning**：`base_dataset_version` 放最外層 partition，未指定版本的 query 也能被 Spark 自動 prune。
4. **與 inference 端風格一致**：`score_table` / `ranked_predictions` 已是「單表 + version partition」模式。

## Non-Goals

- 改 inference 端的 `score_table` / `validated_predictions`（已是目標形態）。
- 改 driver-local 非 Hive artifact：`data/dataset/${base_dataset_version}/*.json`、`data/recsys_cache/${base_dataset_version}/.../*.parquet`、`data/models/${model_version}/*` —— 維持 filepath 版本目錄。
- 改 `core/versioning.py` 的 hash 算法。
- 改 CLI surface。
- Production (CDP/YARN) 切換流程 —— dev-cluster 先做完，prod cutover 列為 future work。

## Architecture

### Hive table 佈局

11 張表的命名去掉所有 version suffix；version 與 `snap_date` 一併下放成 partition column。Partition 順序由外到內：先 version（粗），再 `snap_date`（細）。

| Table | PARTITIONED BY |
|---|---|
| `val_keys` | `(base_dataset_version, snap_date)` |
| `test_keys` | `(base_dataset_version, snap_date)` |
| `val_model_input` | `(base_dataset_version, snap_date)` |
| `test_model_input` | `(base_dataset_version, snap_date)` |
| `sample_keys` | `(base_dataset_version, train_variant_id, snap_date)` |
| `train_keys` | `(base_dataset_version, train_variant_id, snap_date)` |
| `train_dev_keys` | `(base_dataset_version, train_variant_id, snap_date)` |
| `train_model_input` | `(base_dataset_version, train_variant_id, snap_date)` |
| `train_dev_model_input` | `(base_dataset_version, train_variant_id, snap_date)` |
| `calibration_keys` | `(base_dataset_version, calibration_variant_id, snap_date)` |
| `calibration_model_input` | `(base_dataset_version, calibration_variant_id, snap_date)` |

備註：`sample_keys` 雖然在現行 catalog 註解放在 base 層，但實際 table 名是 `sample_keys_${base_dataset_version}_${train_variant_id}`，本質就是 train_variant scope，這次一併歸位。

所有 11 張表保持 managed (`external: false`)，這樣 `ALTER TABLE ... DROP PARTITION` 會連帶清掉 data 檔。

### `HiveTableDataset` API 擴充

引入兩種 partition column 概念：

- **static partition**：寫入時整個 partition 只有單一值（per-run 常數）。透過新欄位 `partition_filter` 在 catalog 宣告。
- **dynamic partition**：值由 DataFrame row 決定（如 `snap_date`）。維持原本 `partition_cols` 欄位，但只列「動態的那些」。

Catalog 範例：

```yaml
val_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: val_model_input              # 不再含 ${base_dataset_version}
  external: false
  columns: "auto"
  partition_filter:                   # 新欄位：static, per-run 常數
    base_dataset_version: ${base_dataset_version}
  partition_cols:                     # 既有欄位：dynamic, 由 row 決定
    - {name: snap_date, type: STRING}

train_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: train_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}
```

`HiveTableDataset` 的行為：

- **`CREATE TABLE`**：`PARTITIONED BY` 子句把 `partition_filter` keys 與 `partition_cols` 合併，順序為 `partition_filter` 先（外層）、`partition_cols` 後（內層）。`partition_filter` 的 column type 預設 `STRING`。
- **`load()`**：在底層 SQL 自動加 `WHERE pf_col = 'pf_val'` 條件（每一個 `partition_filter` entry 一條 AND 子句）。回傳 DataFrame **保留** partition column，由 caller 自行決定要不要 drop。
- **`save()`**：產生 `INSERT OVERWRITE TABLE foo PARTITION (base_dataset_version='X', train_variant_id='Y', snap_date)` —— static col 帶字面值、dynamic col 不帶值（走 Spark dynamic partition overwrite，只覆蓋本次 run 對應的 partition 組合）。`HiveTableDataset` 寫入路徑需保證 session 上的 `spark.sql.sources.partitionOverwriteMode=dynamic`（若未設則臨時 set 一次）。
- **`save()` 補欄**：寫入前 caller 不應預先 populate static partition column；若 DataFrame 沒帶，自動補 literal value column；若已存在且值與 `partition_filter` 一致，沿用；若已存在但值不一致，**raise**（防止寫到錯誤的 partition）。
- **未設 `partition_filter` 的相容性**：catalog 條目若不宣告 `partition_filter`（例如 source tables、inference tables），行為與目前 `HiveTableDataset` 完全一致——load 不注 WHERE、save 不補 static col、CREATE TABLE 只用 `partition_cols`。

### Read / write 行為摘要

| 動作 | 觸發條件 | 結果 |
|---|---|---|
| 第一次寫某 table | partition 不存在 | CREATE TABLE + INSERT OVERWRITE |
| 同 version 重跑 | 同 `partition_filter` | INSERT OVERWRITE 該 version partition（同 snap_date 的 row 被覆蓋） |
| 不同 version 重跑 | 不同 `partition_filter` | INSERT OVERWRITE 新 version partition（舊 version partition 不受影響） |
| pipeline 內 load | 一定帶 `partition_filter` | SELECT ... WHERE 注入，只讀該 version partition |
| ad-hoc 跨版本 SQL | 使用者手寫 SQL | 不經 `HiveTableDataset`；schema 兼容性由使用者自行處理 |

## Migration

Dev-cluster 走方案 C（nuke + 重灌）：

1. 新增 `scripts/nuke_ml_recsys.py`：`DROP DATABASE ml_recsys CASCADE`。
2. 重跑 `scripts/setup_hive_dev.py` 建回 source tables（`feature_table` / `label_table` / `sample_pool`）。
3. 跑 dataset → training → inference → evaluation 一輪 e2e，確認新 layout 從零跑得起來。

Production cutover 不在本次 spec 範圍。

## Testing

### 新增測試

`tests/io/test_hive_table_dataset.py`：

- CREATE TABLE 含混合 static + dynamic partition 時 `PARTITIONED BY` 子句順序正確（`partition_filter` 在外、`partition_cols` 在內）。
- `load()` 注入 WHERE 條件正確（單一 / 多個 static partition）。
- `load()` 回傳 DataFrame 保留 partition column。
- `save()` 拼 INSERT OVERWRITE PARTITION 子句，static col 用字面值、dynamic col 不帶值。
- `save()` 在 DataFrame 缺 static partition column 時自動補 literal column。
- 同 version 重跑只覆蓋同 partition；不同 version 重跑不影響舊 partition。

### 既有測試需更新

- `tests/test_pipelines/test_dataset/test_scenarios/`：grep `_${version}` table 名 / asserts 目錄結構的部份改用 partition 描述。
- `tests/core/test_versioning.py`：versioning 邏輯不變；若有測 catalog render 結果含 `${base_dataset_version}` 在 table 名上的 assertion，改成驗 `partition_filter`。

### E2E smoke

dev-cluster 上跑完整 dataset → training → inference → evaluation 一輪，確認：

- Hive metastore 中只有 11 張 dataset 表（加 source 與 inference 共 16 張上下），無 `*_<hash>` 命名。
- 同 base_dataset_version 重跑不會 duplicate；改參數產生新 base_dataset_version 後，老 version 的 partition 仍可讀。
- `SELECT base_dataset_version, COUNT(*) FROM val_model_input GROUP BY base_dataset_version` 跨版本回得了結果。

## Files Changed (高層 list)

新增：
- `scripts/nuke_ml_recsys.py`
- `docs/superpowers/specs/2026-05-06-dataset-version-as-partition-design.md`（本檔）

修改：
- `src/recsys_tfb/io/hive_table_dataset.py` —— 加 `partition_filter` 欄位、CREATE TABLE 合併 partition、load 注 WHERE、save 拼 static+dynamic INSERT OVERWRITE PARTITION 子句、save 自動補 static col 到 DataFrame。
- `conf/base/catalog.yaml` —— 11 張 dataset 表改寫（table 名去 suffix、加 `partition_filter` 與 `partition_cols`）。
- `tests/io/test_hive_table_dataset.py`
- `tests/test_pipelines/test_dataset/test_scenarios/`（凡 grep `_${version}` 命名的地方）
- `tests/core/test_versioning.py`（凡 assert `${base_dataset_version}` 出現在 table 名的部份）

不改：
- `src/recsys_tfb/core/versioning.py`（hash 算法不動）
- CLI / parameters
- driver-local artifact 路徑（preprocessor.json、category_mappings.json、recsys_cache、models）
- `score_table` / `validated_predictions` 的 catalog 條目

## Risks & Trade-offs

- **跨版本 schema drift**：`base_dataset_version` 是 schema-aware hash，schema 一變 hash 就變、partition 自然切開。但若使用者寫 ad-hoc SQL 跨多個 version partition 做 UNION，會踩到 metastore schema 與 partition file schema 之差距（新欄補 null OK，刪欄/改型別有問題）。本 spec 採用「pipeline 內單版本讀，跨版本是 ad-hoc 自己負責」的策略。
- **Drop 舊版改用 `DROP PARTITION`**：清理腳本需從 `DROP TABLE` 改寫成 partition 級操作。本次因 dev nuke + 重灌規避；production retention 策略待 future work。
- **`HiveTableDataset` API 表面變大**：新增 `partition_filter` 概念，需要在文件與測試裡明確說明 static vs dynamic 的差異。

## Future Work（明確列出，本次不做）

- Production (CDP/YARN) cutover plan。
- 長期 schema 演進策略（若 schema 變動頻繁，考慮加 schema migration tooling 或定義 retention policy）。
- `score_table` / `validated_predictions` 的 partition column 順序是否要對齊新 convention（目前是 `snap_date, prod_name, model_version`，version 在內層；考慮顛倒成 `model_version, snap_date, prod_name`）。
