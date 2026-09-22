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

`feature_etl` 還可以多產出一張**候選層級特徵表**（選用，最多一張）：一列是一筆候選的特徵，粒度與 `sample_pool` 相同（identity：`time`、`entity`、`item`，宣告了 `occasion`／`event` 時再加上它們），dataset 以 identity 把它接到候選列上。用法見 [`dataset.md`](dataset.md) §3；它的特徵要怎麼算才不會偷看未來，見 [3.8](#38-特徵的時間正確性不偷看未來是-sql-的責任)。

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
| `variables` | 選填 | 提供 SQL 範本 `${...}` 變數的預設值；每個名字可在執行時用 `--var key=value` 覆寫，寫法與三種值的意義見下方說明 |
| `variables.target_db` | 建議必填 | 中介表與最終表寫入的 Hive database，也可在 SQL 中以 `${target_db}` 引用；只能在這裡設定，不能用 `--var` 覆寫，也不能寫成 `~`（原因見下方說明） |
| `target_dates` | 二選一 | 未提供 CLI `--target-dates` 時使用的日期清單 |
| `dry_run` | 選填 | `true` 時只 render SQL，不執行 Hive DDL／DML，也不寫 audit |
| `rendered_sql_dir` | 選填 | 保存最終 SQL；路徑結構為 `<dir>/<run_id>/<target_date>/<table>.sql` |
| `source_checks` | 選填 | 上游 partition、資料量與 schema 的 preflight 設定 |
| `tables` | 必填 | 依執行順序排列的輸出表清單 |
| `audit` | 選填 | audit Hive table 的 database 與 table 名稱 |

`variables` 底下每個名字的值有三種寫法：

- 字串：這次執行沒有用 `--var` 帶同名的值時，就使用它當預設值。這個字串的最終值（不管來自 YAML 還是 `--var`）裡，唯一可以出現的 `${...}` 是 `${target_date}`，例如 `win_start: "add_months('${target_date}', -12)"`；出現其他 `${...}` 就擋下。理由是：變數之間依宣告順序逐一代換，值裡引用別的變數時，換不換得到要看兩者誰排前面，同一份設定換個宣告順序結果就不同；沒換到的 `${...}` 到了 Spark 會被默默換成空字串（Spark 預設開著 `spark.sql.variable.substitute`，對不認得的 `${...}` 不會報錯），跑完只是悄悄少了資料。`${target_date}` 例外，因為它永遠最後才換，結果不受順序影響。其他值請直接寫最終值；要隨日期變的運算式，就引用 `${target_date}`，或直接寫進 SQL 檔。YAML 裡的 `${env.X}` 在載入設定時就換成環境變數的值（見下方），所以不受這條限制，除非環境變數的值本身就含有 `${`。
- `~`（YAML 的 null）：代表這次執行**必須**用 `--var` 帶這個名字的值，沒帶就擋下；用 `--restart-from` 接續、就算用到它的那張表被略過了，也一樣要帶。這樣規則只有一句「寫了 `~` 就一定要帶」，看設定檔就知道會不會被擋，不必逐支 SQL 去查哪張表用到它；接續時照抄上一次的整行指令即可。
- 其他型別（數字、布林……）：會被擋下，改成加引號的字串即可，例如 `my_var: "2025"`。

`target_date` 與 `target_db` 這兩個名字保留給框架自己用，都不能經 `--var` 帶值，但兩者被擋的範圍不一樣：

- `target_date`：每個日期由 `--target-dates`（或 YAML `target_dates`）逐一帶入 SQL。若也能從 `--var` 帶，這個值會被每個日期的實際值蓋掉、等於白帶，所以 `--var target_date=...` 直接擋下，改用 `--target-dates`。`variables` 裡宣告 `target_date`（不管值是什麼）一樣會被擋，理由相同——寫了也不會有任何作用。
- `target_db`：決定表寫入哪個 database，也決定 audit 表位置與輸出檢查讀哪個 database。`--var target_db=...` 會被擋下：只能在這份 YAML 改，這樣「寫到哪裡」一定會留下設定檔的變更紀錄，方便事後追查；而且下游 `dataset` pipeline 讀的是另一份設定 `hive.db`，不會跟著這裡的值變動，若讓它能用 `--var` 覆寫，兩邊會默默不一致。但這條限制只擋 `--var`，**不擋 YAML 裡的正常字串值**——`variables.target_db` 寫成字串（像 §3.1 範例裡的 `target_db: ml_recsys`）是正常且建議的設定。只有寫成 `target_db: ~` 才會被擋，因為 `~` 的意思是「這個值只能從 `--var` 帶」，但 `target_db` 又不准從 `--var` 帶，兩條規則互相衝突，只好直接擋。

以上任何一種情況——`--var` 帶到沒宣告的名字、同一個名字帶了兩次、`--var` 缺少 `=`、`variables` 的值型別不合法、`variables` 本身不是「名字 → 值」的對照、某個 `~` 名字沒有用 `--var` 帶、某個變數的最終值裡有 `${target_date}` 以外的 `${...}`、`--var` 帶了 `target_date` 或 `target_db`、YAML `variables` 宣告了 `target_date`，或 `variables.target_db` 寫成 `~`——都屬於不變量 `A35`，訊息以 `(A35)` 開頭，會在同一次執行裡把所有問題一次列出，並且在 Spark 啟動前完成檢查，見 `src/recsys_tfb/core/consistency.py` 的 invariant legend。

YAML 寫 `my_var: "${env.MY_VAR}"` 這種用環境變數帶值的寫法照樣可用：`${env.X}` 在載入設定時就換掉了，`--var` 在它之後才覆寫，兩者不衝突。這些檢查不受 dry run 影響——不管有沒有開 dry run，都一樣會做。

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

- `${target_date}`：目前處理的日期，由 `--target-dates` 或 `target_dates` 依序帶入；這個名字不能用 `--var` 覆寫。
- `${target_db}`：YAML `variables.target_db`；同樣不能用 `--var` 覆寫，只能改 YAML。
- `variables` 中自行增加的其他字串變數；執行時可用 `--var key=value` 覆寫同名的值，見 [4.1 CLI 選項](#41-cli-選項)。

```sql
SELECT
    snap_date,
    cust_id,
    total_aum
FROM feature_store.feat_aum
WHERE snap_date = '${target_date}'
```

在 Spark 啟動前，框架會把這次要執行的所有表、所有日期的 SQL 都先換一遍：換完只要還剩任何 `${...}`，就會擋下，訊息是 `Unresolved template variables in <檔名>: [...]`。這包含兩種容易誤用的寫法：`${hiveconf:x}`、`${env:X}` 這類 Spark 自己的變數語法，框架不支援；`${env.X}` 只在 YAML 設定檔裡會被換成環境變數，寫在 SQL 檔裡不會。兩者換一遍之後都算沒被解析，所以 SQL 裡不能用。要在框架這一層擋下，是因為留到 Spark 手上，Spark 不會報錯，而是把沒認得的 `${...}` 默默換成空字串（例如 `WHERE x = ''`），整支 SQL 照樣跑完，只是悄悄少了資料。要用環境變數，就在 `variables` 寫 `my_var: "${env.MY_VAR}"`，SQL 裡寫 `${my_var}`。SQL 的輸出必須包含 `partition_by` 宣告的所有欄位；框架會依設定型別 cast partition 欄位，並將其放在 projection 最後方。

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
| primary key 為 NULL | 隨 `max_duplicate_key_ratio` 一起執行 | `primary_key` 任一欄出現 NULL 就 fail，並點名是哪一欄、幾列 |
| 整體 NULL 比例 | `max_null_ratio` | 計算該 partition 所有資料格的整體 NULL 比例 |

```yaml
quality_checks:
  min_row_count: 1000000
  max_duplicate_key_ratio: 0.0
  max_null_ratio: 0.05
```

零列資料會略過重複鍵與 NULL 比例檢查，因此若空 partition 不可接受，必須同時設定正數的 `min_row_count`。

`max_duplicate_key_ratio` 是**整組 primary key 檢查的開關**：只宣告 `primary_key`
不會跑任何值檢查（只跑 schema contract）。重複鍵與 NULL 兩個判定共用同一趟聚合、
各自回報（`check` 欄位分別是 `max_duplicate_key_ratio` 與 `primary_key_not_null`），
所以拿掉這個鍵會同時關掉兩者。dataset pipeline 讀的三張表——`sample_pool`、
`label_table`、`feature_table`——由不變量 A32 在 CLI 進入點確保這個鍵還在，
見 `src/recsys_tfb/core/consistency.py` 的 invariant legend。這道輸出檢查在整個框架的檢查裡屬於哪一層：[pipeline 的檢查](../operations/user-guides/pipeline-checks.md)。

### 3.7 建表與 schema evolution

- table 不存在時，框架會以 Hive CTAS 建立 partitioned Parquet table。
- table 已存在時，會依既有欄位順序產生 `INSERT OVERWRITE`，避免 positional insert 錯位。
- SQL 新增非 partition 欄位時，框架會先執行 `ALTER TABLE ADD COLUMNS`，再寫入資料。
- SQL 移除既有欄位時會 fail-fast；欄位刪除、重新命名或不相容的型別變更應使用新 table 或版本化重建。

### 3.8 特徵的時間正確性：不偷看未來是 SQL 的責任

dataset 接特徵只做**等值 join**：entity 層級特徵表（`feature_table`）以 `time ＋ entity` 接，候選層級特徵表以 identity 接。「這一列候選當下拿得到哪一份資料」這種往回找的邏輯，框架一律不做，也**不檢查**特徵有沒有算到不該看的時間之後（ADR-0022 決定 2、3）。

為什麼不由框架代勞：什麼時候算「當下拿得到」只有部署知道——每日批次幾點才算好、即時特徵算到哪一刻為止。框架替你猜，猜錯時會**靜默**出錯：pipeline 跑得完、指標好看，模型學到的卻是線上根本拿不到的資訊。

#### 兩種特徵，各一條規則

| 特徵 | 一列是 | 規則 |
|---|---|---|
| entity 層級（每日批次、快照） | 某個 entity 在某個時段的狀態 | 取「這個時段開始那一刻**已經算好**」的最後一份，看的是它**完成的時間**，不是它記錄的日期 |
| 候選層級（即時） | 一筆候選當下的情境 | 只算這筆候選發生**之前**的行為，**不含**它發生的那一刻 |

兩條規則都是在問同一件事：**線上真的在這一刻做排序時，手上會有這筆資料嗎？**

#### 範例一：entity 層級，取「時段開始時已經算好」的快照

假設上游有一張每日快照表 `profile_snapshot`：`snapshot_date` 是它記錄的那一天，`available_at` 是它實際算好的時間（批次通常隔天清晨跑完，偶爾晚一天）。時段的第一天是 `snap_date`。

```sql
WITH latest AS (
    SELECT s.entity_id, MAX(s.snapshot_date) AS snapshot_date
    FROM upstream.profile_snapshot s
    WHERE s.available_at <= CAST('${target_date}' AS TIMESTAMP)   -- 時段開始那一刻已經算好
    GROUP BY s.entity_id
)
SELECT
    CAST('${target_date}' AS DATE) AS snap_date,
    s.entity_id,
    s.f1,
    s.f2
FROM upstream.profile_snapshot s
JOIN latest l
  ON s.entity_id     = l.entity_id
 AND s.snapshot_date = l.snapshot_date
```

兩種看起來合理、其實會偷看的寫法：

- **`WHERE s.snapshot_date = '${target_date}'`**：時段第一天那份快照記的是那天**結束時**的狀態，隔天才算好。拿它去排那天一早的候選，就是拿之後才知道的事去排。
- **`WHERE s.snapshot_date = date_sub('${target_date}', 1)`**（固定取前一天）：批次準時的那幾天對，批次晚一天的那天，前一天那份在時段開始時還不存在。只看日期、不看 `available_at` 的寫法，在批次延遲時都會偷看。

`CAST(... AS TIMESTAMP)` 是照 Spark session 時區的午夜解讀；`available_at` 的時區要與它一致，否則邊界會差幾個小時。

上游會重算同一天的快照（同一個 `snapshot_date` 有好幾列、`available_at` 不同）時，上面的 `latest` 只挑了日期，join 回去會拿到那一天的每一列。這時要挑的是「時段開始前已經算好的**最後一版**」：在 `latest` 裡連 `available_at` 一起取（例如先過濾 `available_at <= 時段開始`，再以 `ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY snapshot_date DESC, available_at DESC)` 取第 1 列）。

#### 範例二：候選層級，只算這筆候選之前的行為

假設上游有候選紀錄 `impression_log`（每列一次展示，`event_ts` 是到秒的時間）與行為紀錄 `browse_log`（`entity_id`、`event_ts`）。要算「展示前 30 分鐘瀏覽幾次」：

```sql
SELECT
    i.snap_date,
    i.entity_id,
    i.request_id,          -- identity 的欄一欄都不能少：dataset 用它們接
    i.item_id,
    COUNT(b.event_ts) AS browse_30m
FROM upstream.impression_log i
LEFT JOIN upstream.browse_log b
  ON b.entity_id = i.entity_id
 AND b.event_ts >= i.event_ts - INTERVAL 30 MINUTES
 AND b.event_ts <  i.event_ts            -- 嚴格小於：不含展示那一刻
WHERE i.snap_date = '${target_date}'
GROUP BY i.snap_date, i.entity_id, i.request_id, i.item_id
```

下界（要不要含剛好 30 分鐘前那一秒）是這個特徵自己的定義，與偷看無關；上界才是。最常見的偷看是把上界寫成 `<=`。使用者點了之後常常馬上去瀏覽，那筆瀏覽的時間戳往往與點擊落在同一秒；多算到那一秒，這個特徵就變成「有沒有點」的答案。「這一次之前出現過幾次」這類累計，用 window function 時同理：寫 `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`，不是 `CURRENT ROW`。

輸出的每一列要在 identity 上唯一——同一筆候選有兩列，dataset 接上去就會讓那筆候選變成兩列。所以在它的 `quality_checks` 設 `primary_key`（identity 各欄）與 `max_duplicate_key_ratio: 0.0`（見 [3.6](#36-輸出-quality-checks)）。框架的 A32 不會替這張表檢查這項設定有沒有被刪掉（ADR-0026〈更正 ADR-0022〉）。

#### 寫完之後的自我檢查

1. **每一個時間比較都寫明比的是哪一刻。** 快照比的是「完成時間 ≤ 時段開始」；行為比的是「行為時間 < 候選發生時間」。看到 `=` 比日期、或 `<=` 比到候選那一刻，停下來想。
2. **只用日期欄挑快照的寫法，一律當作可疑。** 除非上游保證快照一定在時段開始前算好——而且那個保證寫在某個地方。
3. **把錯的寫法當變異跑一次。** 把 `<` 改成 `<=`、把 `available_at` 條件拿掉，確認你的檢查（或至少特徵的分布）會變。**錯的寫法算出來跟對的一樣，代表資料根本分不出兩者，你的檢查擋不住它。** 廣告示例的 `examples/ad/check_features.py` 就是這樣做的：照定義用 pandas 重算每一欄，與 SQL 的輸出逐列比對，並列出它擋得住的六種錯寫法（`examples/ad/README.md`〈怎麼跑〉）。
4. **線上評分用同一份定義嗎？** 候選層級特徵在線上由另一個系統計算。兩邊算得不一樣時模型會悄悄變差，框架看不到這件事（ADR-0022〈後果〉）。

完整的實例：`examples/ad/conf/sql/etl/feature/feature_user.sql`（範例一的規則，含批次延遲）與 `feature_realtime.sql`（範例二的規則）。

#### 「怎麼被擺出來」的特徵會讓離線分數虛高

有一類特徵說的不是使用者的偏好，而是舊系統怎麼擺的：這筆候選排在第幾格、這次請求裡這個 item 第幾次出現。第 1 格本來就比第 3 格容易被點，跟 item 好不好無關。

- 訓練時放進去有好處：模型知道「第 3 格點得少是位置害的」，不會怪到 item 頭上。
- 但真正要排序時還不知道會擺第幾格——那正是排序要決定的事。業界的做法是評分時把所有候選都當成同一個位置（例如全部當第 1 格）。
- **框架沒有「評分時固定成一個值」的機制。** evaluation 讀的 test 資料帶著真實的位置，模型可以從它猜出舊系統把誰擺在前面，離線分數會高過線上真正拿得到的。

所以這類欄：不放進特徵表；或者放了，就知道離線分數裡有一部分是在猜位置。同一次請求裡同一個 item 只因版面重複出現兩次時，在來源 SQL 併成一列（label 取「有沒有點過任何一次」），丟掉的只有位置。

## 4. 使用方式

### 4.1 CLI 選項

三個 ETL 指令共用以下選項：

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇 `conf/<env>` 設定環境 |
| `--target-dates` | YAML `target_dates` | 逗號分隔的日期，例如 `2026-01-31,2026-02-28` |
| `--var key=value` | 無 | 覆寫 YAML `variables` 裡同名的值；只在第一個 `=` 切開（值裡可以再有 `=`），`key=` 代表空字串 |
| `--source-check` | 關閉 | 只執行該 stage 的上游 preflight，不執行 ETL |
| `--restart-from` | 無 | 從指定 table 開始，略過清單中更早的 tables |

帶多個變數就重複 `--var`：

```bash
python -m recsys_tfb feature_etl --env production --target-dates 2026-01-31 \
  --var raw_db=raw_lake --var allowed_values="'a','b'"
```

分隔多個變數靠重複 `--var`，不是逗號，所以值裡有逗號（例如上面的 `'a','b'`）也不會被切開；這跟 `--target-dates` 用逗號分隔日期是兩種不同的規則。

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

在 Spark 啟動前，框架會先用跟下表「Render SQL」同一套代換邏輯，把這次要執行的所有表、所有日期的 SQL 都算一遍，檢查 `--var`／`variables` 是否合法（`A35`）、SQL 檔案存不存在、代換完會不會還有殘留的 `${...}`；這一步只是檢查，算出來的結果不會留著給後面用。確認過關、Spark 啟動之後，每個 target date 才依序經過下表列的階段：

| 階段 | 處理內容 | 失敗行為 |
|---|---|---|
| 載入設定 | 解析 YAML、table 順序與 `depends_on` | 設定不合法時，在執行 SQL 前中止 |
| Render SQL | 對這張表、這個日期，重新讀 SQL 檔並代入 `${target_date}`、`${target_db}`、`--var` 覆寫值 | 有未解析的 `${...}` 或 SQL 檔不存在時中止。代換用的是 Spark 啟動前檢查時同一段程式，所以通常只會發生在兩者之間 SQL 檔被改動的情況 |
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

執行開始時，log 會印一行這次生效的所有變數與值，並標出哪些來自 `--var`。這行 log 會印出所有變數的最終值，包括從 `${env.X}` 帶進來的值，所以不要把密碼之類的機密放進 `variables`。audit table 不會多存一份變數值，所以 log 被清掉之後，就查不到那次執行實際用的變數值。

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
| `(A35) ...`（開頭；`--var`／`variables` 設定不合法） | `--var` 帶到沒在 `variables` 宣告的名字、同一個名字帶了兩次、`--var` 缺少 `=`、`variables` 的值不是字串或 `~`、`variables` 本身不是「名字 → 值」的對照、某個 `~` 名字沒有對應的 `--var`、某個變數的最終值裡有 `${target_date}` 以外的 `${...}`、`--var` 帶了 `target_date` 或 `target_db`、YAML `variables` 宣告了 `target_date`，或 `variables.target_db` 寫成 `~` | 這些問題會一次全部列出，且在 Spark 啟動前擋下；照訊息逐條修正 `--var` 參數或 YAML `variables` 後重跑 |
| `Unresolved template variables in <檔名>: [...]` | SQL 換完後還有沒被解析的 `${...}`：可能是 `variables` 沒定義這個名字；也可能是 Spark 自己的變數語法（例如 `${hiveconf:x}`），或只在 YAML 才會被換的 `${env.X}` 寫進了 SQL 檔 | 在 `variables` 補上這個名字的預設值，或用 `--var` 帶值；要用環境變數時，在 `variables` 寫 `my_var: "${env.MY_VAR}"`，SQL 改用 `${my_var}` |
| `No such file or directory: '<路徑>'` | `sql_file` 路徑打錯，或該表的 SQL 檔案還沒建立 | 確認 `conf/sql/etl/...` 路徑與檔名；這項檢查在 Spark 啟動前就會做完，不用等執行到那張表才發現 |
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
- source ETL 不理解特徵洩漏、label 觀察窗或候選資格等業務語意，這些仍需在 SQL review 與資料驗收時確認。特徵的時間正確性怎麼寫、怎麼自我檢查，見 [3.8](#38-特徵的時間正確性不偷看未來是-sql-的責任)。
- `--var` 只提供給 source ETL 系列指令（`feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl`）。其他 DAG pipeline（`dataset`、`training`、`inference`、`evaluation`）沒有這個旗標，它們 SQL／設定裡的 `${...}` 是另一套 catalog 代換機制，語法相似但不是同一件事。

## 10. 相關文件

- 三張來源表的 schema 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- Fail-fast、版本化與可恢復執行的設計背景：[`../design-principles.md`](../design-principles.md)
- 下一個 pipeline：[`dataset.md`](dataset.md)
