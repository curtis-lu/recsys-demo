# source_etl pipeline

> 把上游原始表整理成框架要讀的三張**來源表**：`feature_table`、`label_table`、`sample_pool`。
> 這是唯一用 **SQL** 跑、不是 DAG node 的階段。

## 指令與選項

```bash
# 三條獨立 ETL，各產一張來源表（--target-dates 逗號分隔多個；未給讀 config）
python -m recsys_tfb feature_etl     --env local --target-dates 2025-01-31
python -m recsys_tfb label_etl       --env local --target-dates 2025-01-31
python -m recsys_tfb sample_pool_etl --env local --target-dates 2025-01-31

# 先 preflight 驗上游（唯讀、不寫表；有失敗即 exit 1 並印修復指引）
python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31

# 從某張表續跑（跳過它之前已寫的表；接續失敗的長流程）
python -m recsys_tfb feature_etl --restart-from <table_name> --target-dates 2025-01-31
```

> `source_etl` 非 DAG pipeline、無切片旗標。`--source-check` 不可與 `--restart-from` 併用（檢查不寫表，無從續跑）；dry-run／preflight／覆寫的完整語意見下方「重跑語意」與「preflight 工作流」。

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
> 本機（`--env local`）用合成資料時**跳過 source_etl**（合成資料已是 feature / label 粒度，沒有上游表）。以下說明的是生產環境怎麼產這三張表。

## 機制（與 DAG pipeline 不同）

- 每條 ETL ＝ 一個**有序的 `tables` 清單**，逐張：render SQL 範本 → 在 Spark 上 `INSERT OVERWRITE` → 跑品質檢查。
- 對每個 `--target-dates` 指定的 snap_date 各跑一輪（dynamic partition）。
- 由 `SQLRunner`（`src/recsys_tfb/pipelines/source_etl/sql_runner.py`）驅動，**不是** DAG node，所以不在 `data-lineage` 的節點接線裡——它的產物是 lineage 的最上游。

## 輸入 / 輸出

- **輸入**：上游原始 Hive 表（由各 `<table>.sql` 的 `FROM` 決定；非框架管理）。
- **輸出**：`tables` 清單裡每張表 `INSERT OVERWRITE` 到 `${target_db}.<name>`；最後一張通常就是來源表本身。
- **稽核**：每張表 / 每輪 summary / source-check 失敗都記入 `${target_db}.etl_audit_log`；
  紀錄在 run 期間先 buffer，於 run 結束時**一次** coalesced 寫出（單檔，避免小碎檔）。
  該表**不分區**（`snap_date` 為一般欄位，查詢用 `WHERE snap_date = '...'`）。

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
- `source_checks`：上游表的新鮮度 / schema 檢查，由 `--source-check` preflight 模式執行（見下方指令）。
- `audit`：稽核表位置（預設 `etl_audit_log`）。
- `dry_run`：是否只 render、不寫表（見下）。

### `source_checks` 設定

`source_checks` 是「上游表 → 檢查設定」的 map，由 `--source-check` preflight 模式
執行（見下方指令）。每個 key 是上游表 FQN：

| 欄位 | 型別 | 預設 | 意義 |
|---|---|---|---|
| key（表名） | str | — | 上游表 FQN，如 `feature_store.feat_aum` |
| `partition_key` | str | （必填） | 分區欄名；檢查 `<partition_key>=<snap_date>` 是否存在 |
| `min_row_count` | int | `0` | 該分區最少列數；`0` 表示不檢查列數 |
| `expected_columns` | map{col: type} | `{}` | 期望欄位→型別；缺欄或型別不符即失敗；空表示不檢查 |
| `allow_new_columns` | bool | `true` | `false` 時，上游多出 `expected_columns` 以外的欄位即失敗 |

```yaml
feature_etl:
  source_checks:
    feature_store.feat_aum:
      partition_key: snap_date
      min_row_count: 1000000
      expected_columns:
        cust_id: string
        aum_bal: decimal(18,2)
      allow_new_columns: true
```

## 重跑語意

- **指定日期**：`--target-dates 2025-01-31,2025-02-28`（逗號分隔多個）；未給則讀 config 的 `target_dates`。
- **dry-run（不寫表）**：`dry_run` 預設在 `--env local` 為 `true`、`--env production` 為 `false`。dry-run 時只 render、**不**起 Spark、**不**寫表、**不**記 audit。可在各 `parameters_*_etl.yaml` 以 `dry_run: false` 覆寫。
  - ⚠️ 本 repo 的 **`sample_pool_etl` 設了 `dry_run: false`**，所以它在 `--env local` 也會**實際寫表**；`feature_etl` / `label_etl` 沒設，故 local 預設 dry-run。要在 local 實寫 feature / label，用 `--env production` 或在該檔設 `dry_run: false`。
- **從某張表續跑**：`--restart-from <table_name>`，跳過它之前的表（接續失敗的長流程）。
- **覆寫語意**：`INSERT OVERWRITE` 對每個 snap_date partition 整個覆寫——重跑同一天 ＝ 覆寫，不是 append。
- **檢查 / 執行失敗的後果**：
  - `source_checks`（上游）：只在 `--source-check` preflight 模式跑。會把**全部**
    target_dates × 全部檢查跑完（collect-all），有任一失敗即印出彙整報告（含
    expected vs actual、依檢查類型的修復提示、僅含失敗日期的重跑指令）並以非零碼結束；
    **不寫任何表**。正常 ETL run（不帶 `--source-check`）**不再跑 source_checks**。
  - `quality_checks`（輸出）：任一失敗即 **fail-fast 中止整個 run**、以非零碼結束，並
    印出 `--restart-from <table>` 續跑指引（修復後從失敗那張表續跑、跳過先前已寫的表）。
  - SQL / Spark 執行錯誤（如表不存在 / schema 不符）：中止整個 run。
  - 三者都記入 `etl_audit_log`。

## preflight 工作流（建議）

對來源資料新鮮度沒把握時，**先 preflight 再正式跑**——先唯讀驗上游、通過後再寫表：

```bash
# 1) 先驗上游（唯讀、不寫表；全部跑完有失敗即 exit 1 並印修復指引）
python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31
# 2) 通過後再實際寫表
python -m recsys_tfb feature_etl              --target-dates 2025-01-31
```

> 三條 ETL 的完整指令見開頭「指令與選項」。`sample_pool_etl` 在 `--env local` 也會實寫（設了 `dry_run: false`）；`feature_etl` / `label_etl` 本機預設 dry-run（見「重跑語意」）。`--source-check` 不可與 `--restart-from` 併用。

## 接下來

- 三張表的 schema、版本層、下游怎麼消費 → [`../data-lineage.html`](../data-lineage.html)
- 產完之後，下一步常是用輔助工具從 `sample_pool` / `feature_table` 推導抽樣 / 類別設定 → README §2「設定怎麼來」
- 下一個 pipeline → [`dataset.md`](dataset.md)
