# inference pipeline

> 使用指定或已核准的模型，對設定日期中的每個 `(time, entity)` 建立完整候選 item 集合、產生 score 與組內 rank，通過發布前驗證後寫入 production `ranked_predictions`。
> 主要流程為：解析模型與上游版本 → 建立評分母體 → 套用訓練時前處理 → 分批評分 → 組內排名 → staging 驗證 → production 發布。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 產生批次排序結果，供下游行銷、推薦版位或其他資源分配流程使用 |
| 執行指令 | `python -m recsys_tfb inference` |
| 主要輸入 | `inference_population`（評分母體）、`feature_table`（特徵）、版本化 `preprocessor`、版本化 `model` |
| 主要輸出 | `score_table`、`ranked_staging`、production `ranked_predictions` |
| 設定檔 | `conf/base/parameters_inference.yaml` |
| I/O 設定 | `conf/base/catalog.yaml` |
| 上游 pipeline | `source ETL`、`dataset`、`training`、人工 model promotion |
| 下游 pipeline | `evaluation` 的上線後監控模式 |

每筆推論結果代表一個 `(time, entity, item)` 候選，並包含：

| 欄位角色 | 說明 |
|---|---|
| `time` | 本次批次評分的時間切點 |
| `entity` | 接受排序的對象，可由一個或多個欄位組成 |
| `item` | 該對象的候選項目 |
| `score` | 模型輸出分數 |
| `rank` | 同一 `(time, entity)` query group 內依 score 由高到低排列的名次 |
| `model_version` | 產生本筆結果的模型版本 |

inference 預設不使用最新訓練完成的模型，而是解析 `data/models/best` 指向的版本。training 只產生候選模型；使用者完成上線前 evaluation 與人工審核後，需透過 `scripts/promote_model.py` 將核准版本設為 `best`。

也可以用 `--model-version` 明確指定其他版本，但該模式仍會寫入正式 `ranked_predictions`，只是以指定版本作為 partition；它不是僅供預覽的 dry-run。

## 2. 執行前準備

執行 inference 前，建議依序確認：

1. **模型已完成審核**：若使用預設模式，`data/models/best` 必須存在並指向核准版本；若指定 `--model-version`，對應版本目錄必須存在。
2. **模型產物完整**：至少應有 `model.txt`、`model_meta.json` 與 `manifest.json`。校準模型另需 `calibrator.pkl`。
3. **Manifest 能回溯 dataset**：模型 manifest 應包含 `base_dataset_version`、`train_variant_id` 與可選的 `calibration_variant_id`，讓 inference 載入正確的 preprocessor。
4. **評分母體已就緒**：`inference_population` 必須包含每個 `inference.snap_dates` 的母體列。任一日期完全缺少母體時，`build_scoring_dataset` 會立即中止。
5. **母體 grain 唯一**：`inference_population` 對 `time + entity` 唯一，由其 ETL 的 `primary_key` + `quality_checks` 在產出階段保證。`feature_table` 同樣應對 `time + entity` 唯一，否則 enrichment 的 left join 會 fan-out 放大評分母體，最後通常被 completeness 或 duplicate check 阻擋。
6. **候選 item 集合一致**：`inference.products` 必須與 `schema.categorical_values[item]` 為相同集合；CLI 會在啟動時執行雙向一致性檢查。
7. **前處理欄位完整**：評分日期的 `feature_table` 必須提供模型所需欄位。缺欄會在套用 preprocessor 或比對模型 feature names 時中止。
8. **Score 契約正確**：發布閘固定要求 `score` 介於 `[0, 1]`。使用未校準的 learning-to-rank raw score 前，必須確認輸出符合這個契約。
9. **Driver 資源足夠**：模型評分以 `(time, item)` 分批轉成 pandas，但所有批次結果最後仍會在 driver 合併；應依 entity 數、日期數與 item 數預估記憶體。

模型 manifest 缺失或缺少 dataset version 欄位時，CLI 目前會記錄 warning，並回退到 dataset 的 `latest` 版本。
這是舊產物相容機制，不是建議的正式流程；錯誤的 fallback 可能讓模型搭配到不同的 preprocessor。

## 3. 設定方式

### 3.1 評分日期

```yaml
inference:
  snap_dates:
    - "2025-12-31"
    - "2026-01-31"
```

`snap_dates` 決定本次要從 `feature_table` 取出的時間切點。pipeline 支援一次處理多個日期，並對每個日期建立獨立的 Hive partitions。

日期值應使用 ISO `YYYY-MM-DD`，並與 `schema.time` 欄位可轉換成的日期一致。任一設定日期在 `feature_table` 完全不存在時會 fail-fast，不會只發布其他有資料的日期。

修改 `snap_dates` 不會產生新的 `model_version`；它只改變本次要寫入或覆寫的 prediction partitions。

### 3.2 固定候選集合

```yaml
inference:
  products:
    - exchange_usd
    - exchange_fx
    - fund_stock
    - fund_bond
```

框架會先從 `inference_population` 取得每個日期的母體 `(time, entity)`，再與 `products` 做 cross join。因此每個 query group 預設具有完全相同的候選集合：

```text
評分列數 = entity 數 × 日期數 × products 數
```

`products` 必須與下列設定為相同集合，順序可以不同：

```yaml
schema:
  categorical_values:
    prod_name:
      - exchange_usd
      - exchange_fx
      - fund_stock
      - fund_bond
```

這項一致性確保：

- item 已存在於 training 使用的 categorical encoding。
- 模型訓練與正式推論不會使用不同的候選定義。
- validation 可以用 `len(products)` 檢查每個 query group 的候選數量。

每個 entity 是否進入評分母體（entity-level eligibility）由 `inference_population` 的 ETL SQL 決定。但目前不支援每個 entity 擁有**不同候選 item 集合**；若 item 有資格、法遵、庫存或可見性限制，必須在 `build_scoring_dataset` 增加 item-level eligibility 邏輯，不能只依賴模型把不適用 item 排到後面。

### 3.3 Calibration

```yaml
inference:
  use_calibration: true
```

| 模型狀態 | `use_calibration` | 實際 score |
|---|:---:|---|
| 模型包含 calibrator | `true` | 校準後分數 |
| 模型包含 calibrator | `false` | base model 的原始分數 |
| 模型不包含 calibrator | `true` 或 `false` | base model 的原始分數 |

`use_calibration: true` 不會替未校準模型臨時建立 calibrator。模型是否包含 calibration 由 training 產物的 `model_meta.json` 與 `calibrator.pkl` 決定。

若下游只使用組內排序，校準通常不是必要條件；若下游會把 score 解讀為申請機率、點擊機率或期望收益，則應在 training 使用獨立 calibration split。

不論此設定為何，現有 publication gate 都要求 score 位於 `[0, 1]`。部分 ranking objective 的 raw score 不符合此限制，可能需要啟用 calibration 或調整 validation contract。

### 3.4 Schema 與 Spark

inference 會從共用 `parameters.yaml` 讀取 schema：

```yaml
schema:
  columns:
    time: snap_date
    entity: [cust_id]
    item: prod_name
    score: score
    rank: rank
```

`time + entity` 定義 query group；`time + entity + item` 定義 prediction identity。ranking 不會跨日期或跨 entity 進行。

Spark 可在 `parameters_inference.yaml` 覆寫：

```yaml
spark:
  app_name: recsys_tfb-inference
  # spark.sql.shuffle.partitions: 400
```

目前 `conf/base/catalog.yaml` 的 inference tables 使用示例欄位 `cust_id`、`snap_date`、`prod_name`、`score` 與 `rank` 明確宣告 schema。
若修改 schema 角色的實際欄名，也必須同步修改 catalog 欄位與 partition 設定。

### 3.5 推論母體（`inference_population`）

評分母體由獨立的來源表 `inference_population` 提供，定義「每個 `snap_date` 有哪些 `(time, entity)` 該被評分」。它是 inference 端對應 training 端 `sample_pool` 的母體表——把「誰該被推論（membership）」與「他有什麼特徵（`feature_table` enrichment）」分開。

```yaml
# parameters_inference_population_etl.yaml
inference_population_etl:
  tables:
    - name: inference_population
      sql_file: inference_population/inference_population.sql
      partition_by:
        snap_date: DATE
      primary_key: [snap_date, cust_id]   # = (time, entity) grain
      quality_checks:
        max_duplicate_key_ratio: 0.0       # 重複即 ETL fail-loud
```

- **grain**：每個 `(time, entity)` 一列；唯一性由 source ETL 的 `primary_key` + `quality_checks` 在產出階段保證，因此 `build_scoring_dataset` 不需再 `dropDuplicates`。
- **業務邏輯**：哪些 entity 進入母體（在世、未流失、符合行銷資格…）寫在 `inference_population.sql`，由使用者自定義。
- **分群屬性欄**：母體列上可順帶帶 entity-grained 分群欄，供 evaluation 的 `segment_sources` 指向（見 [`evaluation.md`](evaluation.md)）。

`inference_population` 在 `conf/base/catalog.yaml` 以 `HiveTableDataset`、`read_only: true` 宣告，比照 `sample_pool`。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--model-version <id>` | `best` | 指定模型版本；省略時解析 `data/models/best` |
| `--from-node <name>` | 無 | 從指定 node 的拓撲位置執行至 pipeline 結尾 |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開 |
| `--list-nodes` | 關閉 | 列出 node 與從該處接續的補跑成本 |

`--from-node` 與 `--only-node` 互斥；`--list-nodes` 也不可與兩者併用。

`--dry-run` 與 `--list-nodes` 不會執行 nodes 或寫入 Hive，但 CLI 仍會載入設定、初始化 Spark、解析模型版本、讀取 model manifest，並查詢 catalog 產物是否存在。

### 4.2 使用已核准模型

```bash
python -m recsys_tfb inference --env production
```

此指令會：

1. 解析 `data/models/best` 的實際 `model_version`。
2. 透過該模型 manifest 找到正確的 dataset 與 preprocessor。
3. 依 `parameters_inference.yaml` 的日期與 products 產生並發布排序結果。

training 完成後不會自動更新 `best`。人工核准候選版本後，先執行：

```bash
python scripts/promote_model.py <model_version> --dry-run
python scripts/promote_model.py <model_version>
```

promotion 只更新 `best` symlink，不會自動執行 inference，也不會刪除舊模型的 prediction partitions。

### 4.3 指定模型版本

```bash
python -m recsys_tfb inference \
  --env production \
  --model-version <model_version>
```

適合以下情境：

- 在 promotion 前對候選模型進行受控批次測試。
- 重建某個歷史模型版本的 prediction partitions。
- 同一日期保留多個 model versions，供後續 evaluation 比較。

指定版本仍會寫入 `ranked_predictions` production table。下游查詢必須明確使用 `model_version` partition，避免把候選模型結果誤當成目前正式版本。

### 4.4 查看與切片執行

```bash
python -m recsys_tfb inference --list-nodes

python -m recsys_tfb inference \
  --from-node rank_predictions \
  --dry-run
```

常見切片行為：

| 指令 | 實際行為 |
|---|---|
| `--from-node rank_predictions` | 重用 `score_table`，重建 memory-only `scoring_dataset`，重新排名、驗證並發布 |
| `--from-node validate_predictions` | 重用 `ranked_staging`，重建 `scoring_dataset`，重新驗證並發布 |
| `--only-node rank_predictions` | 只重寫 `ranked_staging`，不驗證、不發布 |
| `--only-node validate_predictions` | 重建 `scoring_dataset` 並驗證 staging，但不發布 |
| `--only-node publish_predictions` | 因 `validated_predictions` 是 memory-only，會自動補跑 validation 與 `scoring_dataset`，再發布 |

`score_table` 與 `ranked_staging` 是可持久化接續點；`scoring_dataset`、`X_score` 與 `validated_predictions` 是記憶體中間結果。

切片的 `exists()` 只能確認 Hive table 存在，不能保證本次 model/date partitions 已經產生。實際 node 會再限制在目前 `model_version + snap_dates`；若該範圍沒有資料，仍會在執行時失敗。

## 5. 執行流程

| 階段 | Node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 建立母體 | `build_scoring_dataset` | `inference_population`、`feature_table`、parameters | 篩日期取母體 `(time, entity)`、與 products cross join、left-join 接回 feature columns（標記 `feature_present`） | `scoring_dataset` |
| 前處理 | `apply_preprocessor` | scoring data、`preprocessor` | 套用 training 時的 categorical mappings、欄位順序與 float32 casting | `X_score` |
| 模型評分 | `predict_scores` | `model`、`X_score` | 依模型保存的 feature names 取欄，以 `(time, item)` 分批轉 pandas 並評分 | `score_table` |
| 組內排名 | `rank_predictions` | `score_table` | 限制目前 model/date，依 `(time, entity)` 內 score 降冪產生 rank | `ranked_staging` |
| 發布驗證 | `validate_predictions` | staging、scoring data | 執行六項 sanity checks，任一失敗即拋出 `ValidationError` | `validated_predictions` |
| 正式發布 | `publish_predictions` | validated rows | 將已驗證結果交由 catalog 寫入 production table | `ranked_predictions` |

### 5.1 評分母體

`build_scoring_dataset` 先從 `inference_population` 取出設定日期的母體 `time + entity`，再與完整 products 清單做 cross join，最後依 `time + entity` left join 回 `feature_table` 接上特徵。母體與特徵分離：`inference_population` 定義「誰被評分」（membership），`feature_table` 只負責「他有什麼特徵」（enrichment）。

因此：

- 母體成員資格由 `inference_population` 決定，不再隱式等同於 `feature_table` 的客戶集合。
- 母體 grain 由其 ETL 保證唯一，因此不需 `dropDuplicates`。
- item 本身不需要存在於 `feature_table`。
- 每個 entity 預設會得到全部 products。
- 母體成員若在 `feature_table` 缺特徵，仍保留於評分母體並標記 `feature_present=false`（不中止）。`feature_present` 只存在於 in-memory 的 `scoring_dataset`，**不**寫入 `score_table` / `ranked_predictions` 等 Hive 輸出表；`build_scoring_dataset` 另會 log 每個日期的缺特徵成員數，作為持久的可觀測紀錄。
- `feature_table` 的 `time + entity` 重複列仍會造成 enrichment 的 join fan-out，應在 source ETL 或資料驗收階段先排除。

### 5.2 前處理與模型 feature contract

inference 使用模型 manifest 指向的 base dataset preprocessor，不會重新 fit categorical encoding。未知類別值會依共用 preprocessor 邏輯編碼為 `-1`。

模型評分時會優先使用模型本身保存的 ordered feature names。這讓 training-stage `feature_selection.exclude` 不需重建 dataset，也能確保 inference 只傳入模型實際訓練使用的欄位與順序。

若模型要求的 feature 不在 scoring data 中，pipeline 會明確列出缺少欄位後中止。

### 5.3 Driver-side 分批評分

`predict_scores` 依 distinct `(time, item)` 切分 Spark DataFrame，每一批只將該日期與 item 的 rows 收集到 pandas，再呼叫 `ModelAdapter.predict()`。

identity 與 feature 會在同一次 collect 中取得，以維持每筆分數與 `(time, entity, item)` 的列對齊。所有批次完成後，driver 會合併 pandas 結果並轉回 Spark DataFrame。

`model_version` 會在評分結果中注入，供後續 staging、production 與 evaluation partition 使用。

## 6. 發布驗證與產物

### 6.1 六項 sanity checks

`validate_predictions` 會收集本次執行範圍內的所有失敗，再以單一 `ValidationError` 中止：

| Check | 驗證內容 | 常見失敗原因 |
|---|---|---|
| `row_count_match` | ranked rows 與 scoring rows 總數相同 | 評分遺漏、額外列或中間表範圍錯誤 |
| `score_range` | 所有 score 介於 `[0, 1]` | raw LTR score、模型輸出異常 |
| `no_missing` | identity、score、rank 不可為 NULL | 上游 key／feature 異常或模型輸出缺值 |
| `completeness` | 每個 query group 恰有 `len(products)` 列 | feature join fan-out、候選遺漏 |
| `rank_consistency` | rank 整體範圍為 `1..N`，且沿 rank 增加時 score 不可上升 | rank 被改寫或排序方向錯誤 |
| `no_duplicates` | `time + entity + item` 不可重複 | feature identity 重複、join fan-out |

錯誤物件的 `failures` 會保存每個 check 的名稱與細節，log 也會一次列出所有已發現問題。

### 6.2 Staging／validate／publish

發布順序固定為：

```text
score_table
→ ranked_staging
→ validate_predictions
→ validated_predictions
→ publish_predictions
→ ranked_predictions
```

`ranked_staging` 會在 validation 前先寫入 Hive。驗證失敗時：

- pipeline 立即中止。
- 本次 staging partition 保留，供事後查詢。
- `publish_predictions` 不會執行。
- production `ranked_predictions` 不會寫入本批結果。
- 本次 inference manifest 與 `latest` 不會更新。

`validated_predictions` 只存在於本次 process 記憶體，不是另一張 Hive table。production 的唯一寫入點是 `publish_predictions` 的 `ranked_predictions` catalog output。

### 6.3 主要產物

| 產物 | 儲存方式 | Partition／路徑 | 用途 |
|---|---|---|---|
| `score_table` | Hive managed table | `snap_date, item, model_version` | 可重用的未排名分數 |
| `ranked_staging` | Hive managed table | `snap_date, item, model_version` | 發布前結果與失敗排查 |
| `ranked_predictions` | Hive managed table | `snap_date, item, model_version` | 正式 production 排序結果 |
| `manifest.json` | driver-local JSON | `data/inference/<model_version>/<first_snap_date>/` | 記錄模型、dataset IDs、參數、run ID 與 git commit |
| `parameters_inference.json` | driver-local JSON | 同上 | 保存本次 inference 設定 |
| `latest` | symlink | `data/inference/latest` | 指向最近成功完成的 inference run 目錄 |

Hive tables 採 dynamic partition overwrite，只覆寫本次 DataFrame 實際包含的 `snap_date + item + model_version` partitions，其他模型與日期不受影響。
表格中的 `snap_date` 與 `item` 表示 schema 角色；實際 partition 欄名以 `catalog.yaml` 為準。

### 6.4 驗收重點

執行成功後至少確認：

1. 實際 `model_version` 是預期的 `best` target 或指定版本。
2. 每個設定日期都有 production partitions。
3. 每個 query group 的 rows 數等於 products 數。
4. identity 沒有重複或 NULL。
5. score 全部位於 `[0, 1]`，分布沒有異常集中或全為常數。
6. rank 從 1 開始，並與 score 降冪一致。
7. 各 item 的 rows 數與 entity 母體一致。
8. 抽樣檢視排序結果，確認 eligibility、法遵與基本業務常識。
9. 檢視 `build_scoring_dataset` 的 feature coverage log：每個 snap_date 的缺特徵成員數是否在預期範圍；異常偏高代表 feature ETL 與母體不對齊。

範例查詢：

```sql
SELECT snap_date, model_version,
       COUNT(*) AS rows,
       COUNT(DISTINCT cust_id) AS entities,
       COUNT(DISTINCT prod_name) AS items,
       MIN(score) AS min_score,
       MAX(score) AS max_score,
       MIN(rank) AS min_rank,
       MAX(rank) AS max_rank
FROM ml_recsys.ranked_predictions
WHERE model_version = '<model_version>'
  AND snap_date = '<snap_date>'
GROUP BY snap_date, model_version;
```

實際 database 與欄位名稱以 `conf/base/catalog.yaml` 和 schema 設定為準。

## 7. 版本、重跑與恢復

### 7.1 Inference 沒有獨立版本 hash

inference 不會根據 `parameters_inference.yaml` 產生新的版本 ID。prediction 的邏輯身分由以下欄位決定：

```text
model_version
+ snap_date
+ item
```

`model_version` 已包含上游 dataset IDs 與 model-defining training settings；`snap_dates`、`use_calibration` 與 inference 執行設定不會改變它。

因此同一模型、日期與 item 下：

- 重跑 inference 會覆寫相同 Hive partition。
- 切換 `use_calibration` 也會覆寫同一 partition。
- feature table 同日期資料回補後重跑，仍會覆寫相同 partition。

manifest 保存最後一次成功 run 的 inference parameters，但 Hive partition 本身沒有額外 `inference_version` 可區分上述變化。

### 7.2 設定與重跑矩陣

| 修改內容 | 建議重跑方式 | 原因 |
|---|---|---|
| 新增推論日期 | full inference | 建立新日期 partitions |
| 同日期 feature data 回補 | full inference | 重算該日期所有 score、rank 與 validation |
| `use_calibration` | full inference | score 內容改變，但 partition key 不變 |
| promotion 到新 `best` | full inference | promotion 只更新 symlink，不會自動產生預測 |
| 指定另一個 model version | full inference | 載入不同模型與 preprocessor，寫入新 model partitions |
| 只修改 ranking node | `--from-node rank_predictions` | 可重用目前 model/date 的 `score_table` |
| 只想重新驗證 staging 並發布 | `--from-node validate_predictions` | 重用 staging，重建 scoring 母體後再驗證 |
| 只檢查 staging，不發布 | `--only-node validate_predictions` | validation 成功後即結束 |
| 修改 products | 先依 item 變更流程重建上游，再 full inference | products 必須與 schema item 集合一致 |
| 修改 schema 或 preprocessor | `dataset → training → evaluation → promotion → inference` | 模型與前處理契約改變 |

### 7.3 多日期 manifest

一次設定多個 `snap_dates` 時，Hive 會寫入全部日期，但 driver-local inference 目錄目前只使用清單中的第一個日期：

```text
data/inference/<model_version>/<first_snap_date_without_hyphens>/
```

因此該目錄下的 manifest 代表整次多日期 run，不是只代表路徑名稱中的日期。若後續需要逐日期獨立稽核，應分次執行或擴充 manifest layout。

### 7.4 接續執行的安全邊界

- `score_table` 與 `ranked_staging` 會保留歷史 partitions；rank 與 validation nodes 會先限制目前 `model_version + snap_dates`，避免混入舊批次。
- slicing planner 的 `exists()` 只檢查 table 是否存在，不驗證指定 partition 是否存在或是否由相同參數產生。
- `scoring_dataset` 必須重建，因為 validation 需要用本次 feature table 與 products 定義檢查 row count 和 completeness。
- `--only-node rank_predictions` 不會越過 validation gate，不能視為已發布。
- `--only-node publish_predictions` 仍會自動補跑 validation，不會繞過發布檢查。
- 切片成功後 manifest 會記錄 `resumed_from` 或 `only_node`，但 skipped artifacts 的來源參數仍需由操作者確認。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| `No 'best' symlink found` | 尚未 promotion 就使用預設模式 | 審核模型後執行 `promote_model.py`，或明確指定版本 |
| `Model version directory not found` | `--model-version` 拼錯或 training 未完成 | 檢查 `data/models/<version>/` |
| model manifest warning 並 fallback latest | 舊模型缺 manifest 或 dataset IDs | 補齊 manifest；正式環境不要依賴 fallback |
| `inference_population missing inference.snap_dates` | 母體日期尚未產出、格式錯誤或 source ETL 未完成 | 查 `inference_population` distinct 日期並修正 `snap_dates` |
| `Missing feature columns in scoring dataset` | 新模型需要的欄位未出現在推論日期 feature table | 對齊 feature SQL、dataset preprocessor 與模型版本 |
| `Scoring data is missing feature columns required by the model` | 模型 feature selection／欄位版本與 preprocessor 不一致 | 檢查模型 manifest、base dataset version 與 feature names |
| `No scoring rows found` | 設定日期沒有 entity，或前處理後資料為空 | 查 feature table row count 與日期條件 |
| A4 products mismatch | `inference.products` 與 schema item 清單不一致 | 同步兩處完整 item 集合 |
| `score_range` | raw score 小於 0 或大於 1 | 檢查 objective、calibration 與 `use_calibration` |
| `no_missing` | identity、score 或 rank 出現 NULL | 查 staging 的欄位 NULL count 與上游 feature keys |
| `completeness` | query group 候選數不是 products 數 | 查 feature key 重複、join fan-out 或候選遺漏 |
| `rank_consistency` | rank 範圍或 score 順序異常 | 重新執行 `rank_predictions`，檢查 staging 是否被外部改寫 |
| `no_duplicates` | 同一 identity 重複 | 母體唯一性已由 ETL 保證；優先檢查 `feature_table` enrichment 的 `time + entity` 唯一性是否造成 join fan-out |
| validation 失敗但 staging 有資料 | 正常的 publication gate 行為 | 查 `ranked_staging`；production 未被本批覆寫 |
| 指定候選模型後下游讀錯版本 | `ranked_predictions` 同時保留多個 models | 下游查詢與 evaluation 明確指定 `model_version` |
| `Unknown node` | node 名稱拼錯 | 先以相同環境執行 `--list-nodes` |
| 切片顯示大量 auto-included | 所需輸入是 memory-only 或 partition 不可用 | 依 dry-run 計畫確認補跑成本，必要時 full run |
| Driver OOM | 單一 `(time, item)` entity 過多，或合併後結果過大 | 拆分 snap dates、增加 driver memory，或改寫 distributed scoring |

validation 失敗時，先從 exception 的 checks 清單判斷是模型輸出、候選母體、feature identity 或 ranking 問題，再查相同 model/date 的 `ranked_staging`。

## 9. 限制與注意事項

- 母體成員資格由 `inference_population` 定義；`feature_table` 只提供特徵。缺特徵的母體成員仍會被評分並標記 `feature_present=false`（in-memory + log，不寫入輸出表），不會被自動排除——是否排除由下游決定。
- 目前每個 entity 共用同一份 products 清單，不支援 per-entity eligibility。
- score 必須位於 `[0, 1]`；這對未校準的 ranking objective 是額外限制。
- 模型評分會將每個 `(time, item)` chunk 收集到 driver，且最後在 pandas 合併所有結果，不是完全 distributed inference。
- 所有 distinct `(time, item)` chunks 會先 collect 到 driver；日期與 item 數通常很小，但不適合無界高 cardinality。
- score 相同時沒有額外 tie-break key，Spark `row_number` 對同分 items 的相對名次不保證穩定。
- completeness check 驗證每組候選數量，不會獨立比對每組的實際 item set；目前依賴 scoring cross join 與 duplicate check 共同維持候選正確性。
- rank consistency 會檢查整體 rank 範圍與依 rank 排列的 score 方向，但不是一般用途的任意外部排名驗證器。
- `use_calibration: true` 不會要求模型一定有 calibrator；未校準模型仍回傳原始分數。
- model manifest 缺失時會 fallback dataset latest，可能造成模型與前處理版本錯配。
- inference 沒有獨立 version hash；同 model/date 下修改 calibration、feature data 或程式邏輯會覆寫既有 partitions。
- 多日期 run 只建立一個以第一個日期命名的 driver-local manifest 目錄。
- promotion 只改變 `best` symlink；正式 prediction table 仍保留各 model versions，且不會自動清理。
- production 發布沒有內建業務 eligibility、法遵規則或人工抽查閘；這些仍需在 scoring dataset 與營運流程中明確實作。

## 10. 相關文件

- 模型訓練、feature selection 與 calibration：[`training.md`](training.md)
- 發布後排序指標與監控：[`evaluation.md`](evaluation.md)
- 前處理器、model input 與資料版本：[`dataset.md`](dataset.md)
- 推論使用的 `feature_table` 與母體 `inference_population`：[`source_etl.md`](source_etl.md)
- 各資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- staging／validate／publish 與版本化設計：[`../design-principles.md`](../design-principles.md)
