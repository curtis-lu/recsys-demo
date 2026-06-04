# source_etl pipeline

> 把上游原始表整理成框架要讀的三張**來源表**：`feature_table`、`label_table`、`sample_pool`。
> 這是唯一用 **SQL** 跑、不是 DAG node 的階段。

## 用途

`source_etl` 不是單一指令，而是三條獨立的 SQL ETL 流程，各自產出一張來源表：

| 指令 | 產出 | 內容（schema 角色） |
|---|---|---|
| `feature_etl` | `feature_table` | 每個 (time, entity) 的特徵寬表 |
| `label_etl` | `label_table` | 每個 (time, entity, item) 的 label（0/1） |
| `sample_pool_etl` | `sample_pool` | 每個 query group 要納入排名的候選 (time, entity, item) ＋ 分群欄 |

這三張表**由你自定義**——框架只規範 schema 角色欄（time / entity / item / label），特徵 / 分群欄的內容是應用決定。三張表的完整 schema、版本層與下游用途見 [`../data-lineage.html`](../data-lineage.html)。

> **query group**：同一個 (time, entity) 下所有候選 `item` 形成一組；模型分數只在組內比大小、排序指標也每組各算再平均（見 README §0）。`sample_pool` 就是在界定每組的候選範圍；它的「分群欄」供下游 `dataset` 分層抽樣 / 加權使用（見 [`dataset.md`](dataset.md)）。
>
> 本機 dev-cluster 用合成資料時**跳過 source_etl**（合成資料已是 feature / label 粒度，沒有上游表）。以下說明的是生產環境怎麼產這三張表。

## 機制（與 DAG pipeline 不同）

- 每條 ETL ＝ 一個**有序的 `tables` 清單**，逐張：render SQL 範本 → 在 Spark 上 `INSERT OVERWRITE` → 跑品質檢查。
- 對每個 `--target-dates` 指定的 snap_date 各跑一輪（dynamic partition）。
- 由 `SQLRunner`（`src/recsys_tfb/pipelines/source_etl/sql_runner.py`）驅動，**不是** DAG node，所以不在 `data-lineage` 的節點接線裡——它的產物是 lineage 的最上游。

## 輸入 / 輸出

- **輸入**：上游原始 Hive 表（由各 `<table>.sql` 的 `FROM` 決定；非框架管理）。
- **輸出**：`tables` 清單裡每張表 `INSERT OVERWRITE` 到 `${target_db}.<name>`；最後一張通常就是來源表本身。
- **稽核**：每輪寫一筆 summary 到 `${target_db}.etl_audit_log`。

## 每條 ETL 的流程

每條 ETL 的 `tables` 清單可以先做中介表、再彙整成最終來源表。本 repo 的 `feature_etl` 範例（中介表名是銀行示例，你的應用可自訂）：

```
feature_aum → feature_sav → feature_ccard → feature_info → feature_concat → feature_table
```

- **執行順序 ＝ 清單順序**。`depends_on` **只用於文件與順序驗證**（init 時檢查相依表都排在前面），不改變執行順序。
- `label_etl`（…→ `label_table`）、`sample_pool_etl`（→ `sample_pool`）同理，各有自己的 `tables` 清單。

## 關鍵設定（`conf/base/parameters_{feature,label,sample_pool}_etl.yaml`）

每張表一個 entry：

| 欄位 | 意義 |
|---|---|
| `name` | 輸出 Hive 表名（寫到 `${target_db}.<name>`） |
| `sql_file` | 對應 `conf/sql/etl/<…>.sql`（範本；render 時代入 `variables` 與 target_dates） |
| `partition_by` | 分區欄 ＋ 型別（如 `snap_date: DATE`；label 另加 `prod_name: STRING`） |
| `primary_key` | 唯一鍵（供品質檢查；如 feature 是 `[snap_date, cust_id]`、label / sample_pool 是 `[snap_date, cust_id, prod_name]`） |
| `depends_on`（可選） | 相依表，**僅**文件 / 順序驗證 |
| `quality_checks`（可選） | 如 `max_duplicate_key_ratio: 0.0`（PK 不得有重複） |

stage 層級：

- `variables.target_db`：輸出 database。
- `source_checks`：上游表的新鮮度 / schema 檢查，ETL 前先跑（dry-run 時略過）。
- `audit`：稽核表位置（預設 `etl_audit_log`）。
- `dry_run`：是否只 render、不寫表（見下）。

## 重跑語意

- **指定日期**：`--target-dates 2025-01-31,2025-02-28`（逗號分隔多個）；未給則讀 config 的 `target_dates`。
- **dry-run（不寫表）**：`dry_run` 預設在 `--env local` 為 `true`、`--env production` 為 `false`。dry-run 時只 render、**不**起 Spark、**不**寫表、**不**跑 source_checks / audit。可在各 `parameters_*_etl.yaml` 以 `dry_run: false` 覆寫。
  - ⚠️ 本 repo 的 **`sample_pool_etl` 設了 `dry_run: false`**，所以它在 `--env local` 也會**實際寫表**；`feature_etl` / `label_etl` 沒設，故 local 預設 dry-run。要在 local 實寫 feature / label，用 `--env production` 或在該檔設 `dry_run: false`。
- **從某張表續跑**：`--restart-from <table_name>`，跳過它之前的表（接續失敗的長流程）。
- **覆寫語意**：`INSERT OVERWRITE` 對每個 snap_date partition 整個覆寫——重跑同一天 ＝ 覆寫，不是 append。
- **檢查失敗的後果**：`source_checks`（上游）失敗 → 跳過該 snap_date、繼續下一個日期；`quality_checks`（輸出）失敗 → 停掉該 snap_date 剩餘的表、繼續下一個日期；SQL / Spark 執行錯誤（如表不存在 / schema 不符）→ 中止整個 run。三者都記入 `etl_audit_log`。

## 指令

```bash
python -m recsys_tfb feature_etl     --env local --target-dates 2025-01-31
python -m recsys_tfb label_etl       --env local --target-dates 2025-01-31
python -m recsys_tfb sample_pool_etl --env local --target-dates 2025-01-31   # 此 stage local 也會實寫
```

## 接下來

- 三張表的 schema、版本層、下游怎麼消費 → [`../data-lineage.html`](../data-lineage.html)
- 產完之後，下一步常是用輔助工具從 `sample_pool` / `feature_table` 推導抽樣 / 類別設定 → README §2「設定怎麼來」
- 下一個 pipeline → [`dataset.md`](dataset.md)
