# source ETL pipeline

> 將應用場景的上游資料整理為框架共用的三張來源表：`feature_table`、`label_table` 與 `sample_pool`。
> source ETL 由 SQL 清單依序執行，不使用其他 pipeline 的 DAG node 機制。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 將上游 Hive 表轉換為符合框架 schema 角色與資料粒度的來源表 |
| 執行指令 | `feature_etl`、`label_etl`、`sample_pool_etl` |
| 上游輸入 | 應用自行維護的 Hive tables 與 partitions |
| 主要輸出 | `feature_table`、`label_table`、`sample_pool` |
| 設定檔 | `conf/base/parameters_{feature,label,sample_pool}_etl.yaml` |
| SQL 位置 | `conf/sql/etl/feature/`、`label/`、`sample_pool/` |
| 下游 pipeline | `dataset` |

三條 ETL 是彼此獨立的執行單位：

| ETL | 最終產物 | 預期粒度 | 主要內容 |
|---|---|---|---|
| `feature_etl` | `feature_table` | `time, entity` | 每個對象在各時間切點可供模型使用的特徵 |
| `label_etl` | `label_table` | `time, entity, item` | 目標事件是否發生，通常為 0 或 1 |
| `sample_pool_etl` | `sample_pool` | `time, entity, item` | 要納入建模與排序的候選範圍，以及供分層抽樣使用的欄位 |

`feature_etl` 與 `label_etl` 通常可各自完成；`sample_pool_etl` 需要先確認它在 SQL 中引用的 feature、label 或其他上游產物已經就緒。三張來源表的欄位與下游用途見 [`../data-lineage.html`](../data-lineage.html)。

### Sample pool 需要包含抽樣欄位

dataset 的分層抽樣只讀取 `sample_pool`，不會在抽樣時自動連接 `feature_table`。因此，所有列在 `parameters_dataset.yaml` `sample_group_keys` 中的欄位，都必須在 `sample_pool_etl` 執行時一併產出。

分層欄位可以來自 `feature_table`。若欄位原本位於 `feature_table`，應在 `sample_pool` SQL 中依 `time + entity` 連接，將欄位展開至 `time + entity + item` 粒度：

```sql
SELECT
    p.snap_date,
    p.cust_id,
    p.prod_name,
    f.cust_segment_typ,
    COALESCE(l.label, 0) AS label
FROM candidate_pool p
LEFT JOIN ${target_db}.feature_table f
    ON p.snap_date = f.snap_date
   AND p.cust_id = f.cust_id
LEFT JOIN ${target_db}.label_table l
    ON p.snap_date = l.snap_date
   AND p.cust_id = l.cust_id
   AND p.prod_name = l.prod_name
```

例如 `sample_group_keys: [cust_segment_typ, prod_name, label]` 時，這三個欄位都必須實際存在於 `sample_pool`。其中 `cust_segment_typ` 可由 `feature_table` 取得，`prod_name` 來自候選集合，`label` 則可由 sparse `label_table` left join 後補為 `0`。

連接前應確認 `feature_table` 在 `time + entity` 粒度唯一，否則 join 可能放大 `sample_pool` 筆數。建議為 `sample_pool` 設定完整 `primary_key` 與 `max_duplicate_key_ratio: 0.0`，在 source ETL 邊界阻擋重複候選。

## 2. 執行前準備

正式執行前，建議依序確認：

1. **定義 schema 角色**：先在 `conf/base/parameters.yaml` 設定 `time`、`entity`、`item` 與 `label` 對應的實際欄位名稱。
2. **確認來源資料成熟**：目標日期的上游 partition 必須完成載入；若 label 需要觀察窗，應確認該日期的 ground truth 已成熟。
3. **準備 SQL 與執行順序**：每張中介表各自使用一支 SQL，並依實際相依順序列在 YAML 的 `tables` 中。
4. **宣告輸出契約**：每張表都應設定 `partition_by` 與 `primary_key`，最終三張來源表應符合上表所列粒度。
5. **對齊 item 集合**：`label_table` 與 `sample_pool` 產生的 item，應與 `parameters.yaml` 的 `schema.categorical_values.<item>` 及 `parameters_inference.yaml` 的候選集合一致。
6. **準備抽樣欄位**：所有 `sample_group_keys` 都必須由 `sample_pool_etl` 寫入 `sample_pool`；需要使用 feature 欄位時，先在 SQL 中連接 `feature_table`。
7. **決定執行環境**：確認該環境的 `dry_run` 設定，避免以為已寫表，實際上只 render SQL。

> 本 repo 的本機合成資料流程會直接準備框架需要的來源資料，通常不必執行 source ETL。接入正式應用或真實 Hive 上游時，才需要依本文件建立 SQL 流程。

## 3. 設定方式

### 3.1 檔案配置

每條 ETL 使用一份 YAML 與一組 SQL：

| ETL | YAML | SQL 目錄 |
|---|---|---|
| feature | `conf/base/parameters_feature_etl.yaml` | `conf/sql/etl/feature/` |
| label | `conf/base/parameters_label_etl.yaml` | `conf/sql/etl/label/` |
| sample pool | `conf/base/parameters_sample_pool_etl.yaml` | `conf/sql/etl/sample_pool/` |

YAML 的基本結構如下：

```yaml
feature_etl:
  target_dates: ["2026-01-31"]
  dry_run: false
  rendered_sql_dir: data/rendered_sql

  variables:
    target_db: ml_recsys

  source_checks: {}

  tables:
    - name: feature_table
      sql_file: feature/feature_table.sql
      partition_by:
        snap_date: DATE
      primary_key: [snap_date, cust_id]
      quality_checks:
        min_row_count: 1
        max_duplicate_key_ratio: 0.0

  audit:
    database: "${target_db}"
    table: etl_audit_log
```

### 3.2 Stage 層級設定

| 設定 | 必要性 | 說明 |
|---|---|---|
| `variables.target_db` | 建議必填 | 中介表與最終表寫入的 Hive database，也可在 SQL 中以 `${target_db}` 引用 |
| `target_dates` | 二選一 | 未提供 CLI `--target-dates` 時使用的日期清單 |
| `dry_run` | 選填 | `true` 時只 render SQL，不執行 Hive DDL／DML，也不寫 audit |
| `rendered_sql_dir` | 選填 | 保存最終 SQL；路徑結構為 `<dir>/<run_id>/<target_date>/<table>.sql` |
| `source_checks` | 選填 | 上游 partition、資料量與 schema 的 preflight 設定 |
| `tables` | 必填 | 依執行順序排列的輸出表清單 |
| `audit` | 選填 | audit Hive table 的 database 與 table 名稱 |


### 3.3 Table 層級設定

| 設定 | 必要性 | 說明 |
|---|---|---|
| `name` | 必填 | 輸出 Hive table 名稱，實際寫入 `${target_db}.<name>` |
| `sql_file` | 必填 | 相對於 `conf/sql/etl/` 的 SQL 路徑 |
| `partition_by` | 必填 | 有順序的 `{欄位: Hive 型別}` mapping；不可使用 list |
| `primary_key` | 建議必填 | 輸出資料的唯一鍵，同時作為 schema contract 與重複鍵檢查依據 |
| `depends_on` | 選填 | 文件與順序驗證用途；相依表必須已列在同一份 `tables` 清單的前方 |
| `quality_checks` | 選填 | SQL 寫入後執行的資料品質檢查 |

`tables` 的 list 順序就是實際執行順序。`depends_on` 不會建立 DAG，也不會自動調整順序或檢查其他 ETL 的新鮮度；它只會在初始化時驗證相依表是否已出現在清單前方。

### 3.4 SQL 範本

每支 SQL 應回傳一個 `SELECT`，由框架負責建立 table、schema 對齊及包裝 `INSERT OVERWRITE`。SQL 可使用：

- `${target_date}`：目前處理的日期，由 `--target-dates` 或 `target_dates` 依序帶入。
- `${target_db}`：YAML `variables.target_db`。
- `variables` 中自行增加的其他字串變數。

```sql
SELECT
    snap_date,
    cust_id,
    total_aum
FROM feature_store.feat_aum
WHERE snap_date = '${target_date}'
```

所有 `${...}` 變數都必須能被解析，否則會在執行 SQL 前失敗。SQL 的輸出必須包含 `partition_by` 宣告的所有欄位；框架會依設定型別 cast partition 欄位，並將其放在 projection 最後方。

### 3.5 上游 source checks

`source_checks` 只在使用 `--source-check` 時執行，用於正式寫表前確認上游資料是否可用。

| 設定 | 預設 | 說明 |
|---|---|---|
| table key | 無 | 上游 Hive table FQN，例如 `feature_store.feat_aum` |
| `partition_key` | 必填 | 用來查找目標日期 partition 的欄位 |
| `min_row_count` | `0` | partition 的最低列數；`0` 表示不檢查列數 |
| `expected_columns` | `{}` | 必須存在且型別相符的欄位 |
| `allow_new_columns` | `true` | `false` 時，未列在 `expected_columns` 的額外欄位也會造成失敗 |

```yaml
source_checks:
  feature_store.feat_aum:
    partition_key: snap_date
    min_row_count: 1000000
    expected_columns:
      cust_id: string
      aum_bal: decimal(18,2)
    allow_new_columns: true
```

框架會先檢查 partition 是否存在；若不存在，該 table/date 的 row count 與 schema 檢查會略過。所有 tables 與 dates 都檢查完後才一次回報失敗項目，方便集中修正。

### 3.6 輸出 quality checks

每張表寫入後可執行以下檢查：

| 檢查 | 設定方式 | 說明 |
|---|---|---|
| schema contract | 宣告 `primary_key` 後自動執行 | 確認 primary key 欄位實際存在於輸出表 |
| 最少列數 | `min_row_count` | 目標 `snap_date` partition 至少應有多少列 |
| 重複鍵比例 | `max_duplicate_key_ratio` | 依 `primary_key` 計算重複比例；設定 `0.0` 表示不允許重複 |
| 整體 NULL 比例 | `max_null_ratio` | 計算該 partition 所有資料格的整體 NULL 比例 |

```yaml
quality_checks:
  min_row_count: 1000000
  max_duplicate_key_ratio: 0.0
  max_null_ratio: 0.05
```

零列資料會略過重複鍵與 NULL 比例檢查，因此若空 partition 不可接受，必須同時設定正數的 `min_row_count`。

### 3.7 建表與 schema evolution

- table 不存在時，框架會以 Hive CTAS 建立 partitioned Parquet table。
- table 已存在時，會依既有欄位順序產生 `INSERT OVERWRITE`，避免 positional insert 錯位。
- SQL 新增非 partition 欄位時，框架會先執行 `ALTER TABLE ADD COLUMNS`，再寫入資料。
- SQL 移除既有欄位時會 fail-fast；欄位刪除、重新命名或不相容的型別變更應使用新 table 或版本化重建。

## 4. 使用方式

### 4.1 CLI 選項

三個 ETL 指令共用以下選項：

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇 `conf/<env>` 設定環境 |
| `--target-dates` | YAML `target_dates` | 逗號分隔的日期，例如 `2026-01-31,2026-02-28` |
| `--source-check` | 關閉 | 只執行該 stage 的上游 preflight，不執行 ETL |
| `--restart-from` | 無 | 從指定 table 開始，略過清單中更早的 tables |

`--source-check` 與 `--restart-from` 不能同時使用。source ETL 也不支援 DAG pipeline 的 `--from-node`、`--only-node`、`--list-nodes` 或 CLI `--dry-run`。

### 4.2 建議執行流程

在 YAML 設定好 `source_checks` 後，先對 feature 與 label 上游執行唯讀 preflight：

```bash
python -m recsys_tfb feature_etl --env production --source-check --target-dates 2026-01-31
python -m recsys_tfb label_etl   --env production --source-check --target-dates 2026-01-31
```

確認通過後，先產生 feature 與 label：

```bash
python -m recsys_tfb feature_etl --env production --target-dates 2026-01-31
python -m recsys_tfb label_etl   --env production --target-dates 2026-01-31
```

若 `sample_pool_etl.source_checks` 有設定 feature、label 或其他上游表，可在兩者完成後先執行 preflight，再正式產生 sample pool：

```bash
python -m recsys_tfb sample_pool_etl --env production --source-check --target-dates 2026-01-31
python -m recsys_tfb sample_pool_etl --env production --target-dates 2026-01-31
```

多個日期以逗號分隔，並依輸入順序逐日處理：

```bash
python -m recsys_tfb feature_etl --env production \
  --target-dates 2026-01-31,2026-02-28,2026-03-31
```

省略 `--target-dates` 時，框架會讀取對應 YAML 的 `target_dates`；兩者皆未提供時會直接中止。

### 4.3 檢視 rendered SQL

source ETL 沒有 `--dry-run` CLI 旗標。若只想檢查 SQL，請在對應 YAML 設定：

```yaml
dry_run: true
rendered_sql_dir: data/rendered_sql
```

dry-run 會 render 每張 table、每個日期的完整 SQL，但不查詢或寫入業務 Hive tables，也不寫入 audit。CLI 啟動過程仍可能初始化 Spark session；`--source-check` 則一定會實際查詢 Hive，即使該 stage 設定為 dry-run。

### 4.4 從指定 table 接續

若某張中介表失敗，修正後可從該表重新執行：

```bash
python -m recsys_tfb feature_etl --env production \
  --target-dates 2026-01-31 \
  --restart-from feature_concat
```

`--restart-from` 會略過 `tables` 清單中位於指定 table 之前的步驟。它不能與 `--source-check` 同時使用，也不會驗證被略過的產物是否仍符合目前 SQL 或上游資料。

## 5. 執行流程

正式執行時，每個 target date 依序經過：

| 階段 | 處理內容 | 失敗行為 |
|---|---|---|
| 載入設定 | 解析 YAML、table 順序與 `depends_on` | 設定不合法時，在執行 SQL 前中止 |
| Render SQL | 將 `${target_date}`、`${target_db}` 等變數代入 SQL | 有未解析變數時中止 |
| 探測輸出 schema | 以 `LIMIT 0` 取得 SELECT 欄位與型別 | SQL 或上游 schema 錯誤時中止 |
| 建表或對齊 schema | 首次 CTAS；既有表則 append-only schema evolution | 移除欄位或 partition 欄缺失時中止 |
| 寫入 partition | 以 `INSERT OVERWRITE` 寫入該日期 | Spark／Hive 錯誤時中止 |
| 輸出檢查 | 執行 schema contract 與 `quality_checks` | 任一檢查失敗即中止，不執行後續 tables/dates |
| Audit | 記錄 table 結果與該日期 summary | run 結束時批次寫入 audit table |

一個日期中的 tables 依 YAML 順序執行；多個 target dates 也依輸入順序執行。任一步驟失敗會中止整個 command，因此失敗之前的 tables 或 dates 可能已完成寫入。

## 6. 產物與驗收

| 產物 | 位置 | 驗收重點 |
|---|---|---|
| 中介與最終 Hive tables | `${target_db}.<table>` | 目標 partition 存在，schema 與資料量符合預期 |
| `feature_table` | `${target_db}.feature_table` | 每個 `time, entity` 唯一，特徵欄位完整 |
| `label_table` | `${target_db}.label_table` | 每個 `time, entity, item` 唯一，label 語意與觀察窗正確 |
| `sample_pool` | `${target_db}.sample_pool` | 候選集合完整，與 label／設定中的 item 對齊 |
| rendered SQL | `rendered_sql_dir/<run_id>/<date>/` | 變數、來源表、filter、join 與 partition 寫入符合預期 |
| audit records | `${target_db}.etl_audit_log` | table record 與 `__summary__` 狀態為 `success` |

基本驗收查詢：

```sql
SHOW PARTITIONS ml_recsys.feature_table;

SELECT COUNT(*)
FROM ml_recsys.feature_table
WHERE snap_date = '2026-01-31';

SELECT *
FROM ml_recsys.etl_audit_log
WHERE snap_date = '2026-01-31'
ORDER BY created_at DESC;
```

audit table 不分區，並以 append 方式保存歷次執行紀錄。只有設定 `min_row_count` 時，audit 的 `row_count` 才會取得該檢查算出的實際列數；未設定時即使資料存在，也可能記為 `0`。

## 7. 重跑與恢復

| 情境 | 建議方式 |
|---|---|
| 相同日期需要完整重建 | 直接重跑相同 ETL 與日期；`INSERT OVERWRITE` 會覆寫 partition，不會 append 重複資料 |
| 某張 table 的 SQL 或 quality check 失敗 | 修正後使用錯誤訊息提供的 `--restart-from <table>` |
| 上游資料或較早的 SQL 已變更 | 從第一張受影響的 table 接續，必要時完整重跑 |
| preflight 部分日期失敗 | 修復上游後，只對報告列出的失敗日期重新執行 `--source-check` |
| 多日期 run 在中途失敗 | 先查 audit 確認已完成日期，再只重跑失敗與未執行日期 |
| 新增輸出欄位 | 直接重跑，框架會 append-only 新增非 partition 欄位 |
| 移除、重新命名或不相容地修改欄位 | 建立新 table 或執行版本化重建，不應只使用 `--restart-from` |

source ETL 的輸出不會因 SQL 或來源資料內容改變而自動產生新版本 ID。回補同一日期或修改 SQL 後重跑，可能覆寫同一 partition；資料版本限制見 [`../design-principles.md`](../design-principles.md#3-版本化設計)。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| command 顯示成功但 Hive 沒有新資料 | 該環境啟用了 `dry_run` | 檢查 YAML 的 `dry_run` 與 log 中的 `DRY RUN`；需要寫表時改為 `false` |
| `No target_dates provided` | CLI 與 YAML 都未提供日期 | 加上 `--target-dates`，或設定 stage 的 `target_dates` |
| `No source_checks configured ... nothing to check` | `source_checks` 是空 map | 這不是檢查通過；先為實際上游 table 設定檢查內容 |
| `Source check FAILED ... partition_exists` | 上游 partition 尚未產出或 partition key 設錯 | 以 `SHOW PARTITIONS <table>` 確認日期格式與欄位 |
| `Source check FAILED ... row_count` | 上游載入不完整或門檻設定過高 | 查詢該 partition 實際列數，確認上游完成狀態與合理門檻 |
| `Source check FAILED ... schema_drift` | 缺欄、型別改變或出現不允許的新欄位 | 比對 `DESCRIBE <table>` 與 `expected_columns`，修正上游或更新契約 |
| `Unresolved template variables` | SQL 使用了 YAML `variables` 未定義的 `${...}` | 補上變數或修正 SQL placeholder 名稱 |
| `depends on ... but ... does not appear before it` | `depends_on` 指向不存在或排列在後方的 table | 調整 `tables` list 順序或修正 table 名稱 |
| `Partition columns missing from SELECT output` | SQL 未輸出 `partition_by` 宣告的欄位 | 將 partition 欄位加入 SELECT，並確認命名一致 |
| `Output quality check FAILED` | 列數、重複鍵、NULL 或 primary key schema 不符合契約 | 先查失敗 table/date，再修正 SQL、primary key 或合理門檻，最後依提示接續 |
| `Removing columns ... is not supported` | 新 SQL 移除了既有 Hive table 欄位 | 保留舊欄位並在下游排除，或建立新 table／版本化重建 |
| `restart_from=... not found in tables` | 指定名稱不在該 stage 的 `tables` | 使用 YAML 中完全相同的 `name` |
| `--source-check` 與 `--restart-from` 不能同時使用 | preflight 不寫表，因此沒有接續語意 | 分成兩個 command：先 source check，再正式 ETL 或 restart |

## 9. 限制與注意事項

- `source_checks` 只會在明確使用 `--source-check` 時執行，正式 ETL 不會再次自動執行 preflight。
- `depends_on` 只驗證同一份 stage 設定中的排列順序，不會跨 `feature_etl`、`label_etl` 與 `sample_pool_etl` 排程。
- `max_null_ratio` 是整張 partition 的資料格總體比例，不是逐欄上限；需要欄位級規則時應在 SQL 或額外檢查中明確處理。
- audit 在 run 結束時批次寫入；audit 寫入失敗只會記錄 error log，不會反向將已成功的 ETL 判定為失敗。
- source ETL 不理解特徵洩漏、label 觀察窗或候選資格等業務語意，這些仍需在 SQL review 與資料驗收時確認。

## 10. 相關文件

- 三張來源表的 schema 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- Fail-fast、版本化與可恢復執行的設計背景：[`../design-principles.md`](../design-principles.md)
- 下一個 pipeline：[`dataset.md`](dataset.md)
