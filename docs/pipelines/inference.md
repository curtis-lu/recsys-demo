# inference pipeline

> 使用指定或已核准的模型，對設定日期中的每個 `(time, entity)` 建立完整候選 item 集合、產生 score 與組內 rank，通過發布前驗證後寫入 production `ranked_predictions`。
> 主要流程為：解析模型與上游版本 → 落地一張「母體 × 特徵」中間表（不含 item 展開）→ 逐 `(entity 桶, item)` 評分並即刻落地 → 組內排名 → staging 驗證 → production 發布。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 產生批次排序結果，供下游行銷、推薦版位或其他資源分配流程使用 |
| 執行指令 | `python -m recsys_tfb inference` |
| 主要輸入 | `inference_population`（評分母體）、`feature_table`（特徵）、版本化 `preprocessor`、版本化 `model` |
| 主要輸出 | `inference_population_features`（中間）、`unranked_predictions`、`ranked_staging`、production `ranked_predictions` |
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
4. **評分母體已就緒**：`inference_population` 必須包含每個 `inference.snap_dates` 的母體列。任一日期完全缺少母體時，`build_inference_population_features` 會立即中止。
5. **母體 grain 唯一**：`inference_population` 對 `time + entity` 唯一，由其 ETL 的 `primary_key` + `quality_checks` 在產出階段保證。`feature_table` 同樣應對 `time + entity` 唯一，否則 enrichment 的 left join 會 fan-out 放大評分母體，最後通常被 completeness 或 duplicate check 阻擋。
6. **候選 item 集合一致**：`inference.products` 必須與 `schema.categorical_values[item]` 為相同集合；CLI 會在啟動時執行雙向一致性檢查。
7. **前處理欄位完整**：評分日期的 `feature_table` 必須提供模型所需欄位。缺欄會在套用 preprocessor 或比對模型 feature names 時中止。
8. **Score 契約正確**：發布閘固定要求 `score` 介於 `[0, 1]`。使用未校準的 learning-to-rank raw score 前，必須確認輸出符合這個契約。
9. **Driver 資源足夠**：模型評分把母體按 entity 分成 `inference.entity_buckets` 個桶，一次只有**一個桶**的特徵在 driver 上（約 `母體列數 / entity_buckets × 特徵數 × 4 B`），算完就落地、不累積。所以記憶體是設定值的函數而不是母體大小的函數——母體長大時調高桶數即可，不必改程式碼。桶數的健康窗口見 §3.3。

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

每個 entity 是否進入評分母體（entity-level eligibility）由 `inference_population` 的 ETL SQL 決定。但目前不支援每個 entity 擁有**不同候選 item 集合**；若 item 有資格、法遵、庫存或可見性限制，必須在 `build_inference_population_features`（決定母體）或評分節點的內層 item 迴圈（決定每個 entity 配哪些 item）增加 item-level eligibility 邏輯，不能只依賴模型把不適用 item 排到後面。

### 3.3 評分切塊數（`entity_buckets`）

```yaml
inference:
  entity_buckets: 10
```

母體依 `schema.entity` 的確定性 crc32 雜湊分成這麼多桶（`utils/hashing.py` 的 `spark_bucket`）。這個值控制兩件事：

- **driver 峰值。** 評分的外層迴圈是桶、內層是 item；一次只有一個桶的特徵在 driver 上。
- **落地的分區數。** `unranked_predictions` 的分區是 `(snap_date, item, entity_bucket)`，所以本月的分區數 ＝ item 數 × 有資料的桶數。

**健康窗口 5–20**，兩端各由一個約束夾出來：桶太少，單一 chunk 可能吃掉 driver 一半以上；桶太多，每個分區檔遠小於一個 HDFS block。超出窗口只會 WARN，不會阻止。

**但 0 或負數是硬性錯誤**（一致性不變量 A27，在 `inference` 指令啟動 Spark 前擋下）：零個桶等於零個 chunk，跑完會回報成功但一個 entity 都沒評分。1 是合法的——母體小的時候本來就該一桶跑完。整個鍵不寫則吃預設 10。

**改這個值不是零成本。** 它是 `unranked_predictions` 的分區欄，所以調整桶數等於該 `model_version` 的該表要重建——舊桶的分區沒有任何東西會清掉，而它們會繼續貢獻列給排名。`validate_predictions` 的 `partition_completeness` 會擋下這個狀態（把舊桶報成「不屬於任何 chunk 的分區」），但它擋的是發布，不是清理；清理要手動 DROP 那些分區或整張表。

雜湊只吃 entity 欄、不吃時間：排名的 query group 是 `(time, entity)`，桶必須是 group 的函數，否則同一個 entity 的不同 item 會落到不同桶、被獨立評分與驗證。同理，桶的指派在跑之間是穩定的——salt 寫死在程式碼裡而不是設定裡，因為「重新洗牌」會讓已寫的分區全部變成孤兒。

### 3.4 Calibration

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

### 3.5 Schema 與 Spark

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

### 3.6 推論母體（`inference_population`）

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

- **grain**：每個 `(time, entity)` 一列；唯一性由 source ETL 的 `primary_key` + `quality_checks` 在產出階段保證，因此 `build_inference_population_features` 不需再 `dropDuplicates`。
- **業務邏輯**：哪些 entity 進入母體（在世、未流失、符合行銷資格…）寫在 `inference_population.sql`，由使用者自定義。
- **分群屬性欄**：母體列上可順帶帶 entity-grained 分群欄，供 evaluation 的 `segment_sources` 指向（見 [`evaluation.md`](evaluation.md)）。

`inference_population` 在 `conf/base/catalog.yaml` 以 `HiveTableDataset`、`read_only: true` 宣告，比照 `sample_pool`。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--model-version <id>` | `best` | 指定模型版本；省略時解析 `data/models/best` |
| `--rebuild-dates <d1,d2>` | 無 | 強制重算這些日期的所有評分 chunk，即使分區已存在。必須是 `inference.snap_dates` 的子集（A21，在 Spark 啟動前檢查） |
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
| `--from-node predict_and_write_scores` | 重用已落地的 `inference_population_features`，重新評分（分區已存在的 chunk 會被跳過）、排名、驗證並發布 |
| `--from-node rank_predictions` | 重用 `unranked_predictions`，重新排名、驗證並發布。會自動補跑 `predict_and_write_scores`——`score_manifest` 是 memory-only，而排名需要它當拓樸邊；補跑很便宜，因為所有 chunk 的分區都在，它只列一次 metastore 就結束 |
| `--from-node validate_predictions` | 重用 `ranked_staging`，重新驗證並發布；補跑評分節點只為了取得 manifest |
| `--only-node rank_predictions` | 只重寫 `ranked_staging`，不驗證、不發布 |
| `--only-node validate_predictions` | 驗證 staging 但不發布（補跑評分節點取得 manifest） |
| `--only-node publish_predictions` | 因 `validated_predictions` 是 memory-only，會自動補跑 validation（連帶補跑評分節點取得 manifest），再發布 |

`inference_population_features`、`unranked_predictions` 與 `ranked_staging` 是可持久化接續點；`score_manifest` 與 `validated_predictions` 是記憶體中間結果。

**`--only-node apply_preprocessor` 這個接續點在 #188 之後不存在了**（該節點與 `build_scoring_dataset` 合併）。換來的是 `--from-node predict_and_write_scores`：因為中間特徵表落地了，從評分接續不必重跑母體與特徵的 join，而這在改動前做不到（那時它只是一個 lazy plan）。

切片的 `exists()` 只能確認 Hive table 存在，不能保證本次 model/date partitions 已經產生。實際讀取會再限制在目前的 `model_version`（catalog 的 `partition_filter`）與 `snap_dates`（節點內的 `restrict_to_snap_dates`）；若該範圍沒有資料，仍會在執行時失敗。

## 5. 執行流程

五個 node：

| 階段 | Node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 母體 × 特徵 | `build_inference_population_features` | `inference_population`、`feature_table`、`preprocessor`、parameters | 篩日期取母體 `(time, entity)`、left-join 接回 feature columns、分 entity 桶、套用訓練時的 categorical mappings，並把所有數值特徵欄轉成 `dataset.numeric_feature_storage_type` 宣告的型別（與 training 側同一個鍵、同一個 helper）。**不含 item 展開；identity 類別欄不在此編碼** | `inference_population_features`（Hive） |
| 逐 chunk 評分 | `predict_and_write_scores` | `model`、`inference_population_features`、`preprocessor`、parameters | 外層迴圈桶、內層迴圈 item；一桶的特徵讀進 driver 一次，內層就地覆寫 item 那一欄重複使用。每個 `(桶, item)` 算完先跑塊層 sanity checks（§6.1），通過才寫成**恰好一個分區**。分區已存在的 chunk 跳過 | `score_manifest`（記憶體）；資料經 `writes=` 落地到 `unranked_predictions` |
| 組內排名 | `rank_predictions` | `unranked_predictions`、`score_manifest` | 先 `restrict_to_snap_dates` 裁掉歷史月份（模型版本由 catalog 的 `partition_filter` 擋掉），丟掉 `entity_bucket`，再依 `(time, entity)` 內 score 降冪產生 rank | `ranked_staging` |
| 發布驗證 | `validate_predictions` | staging、`score_manifest` | 執行整批層 sanity checks（塊層在評分時已逐 chunk 跑過），任一失敗即拋出 `ValidationError` | `validated_predictions` |
| 正式發布 | `publish_predictions` | validated rows | 將已驗證結果交由 catalog 寫入 production table | `ranked_predictions` |

**評分節點的輸出是 manifest，不是資料。** 資料由節點自己逐分區 `save()` 到 `unranked_predictions`（`Node(writes=[...])`，登記在 `docs/agents/architecture-constraints.md` 的 R1）。`writes=` 刻意不建立拓樸相依邊，所以 manifest 就是那條邊：沒有它，排名可能被排在它要讀的分區存在之前。

### 5.1 中間特徵表：母體 × 特徵，不含 item 展開

`build_inference_population_features` 先從 `inference_population` 取出設定日期的母體 `time + entity`，再依 `time + entity` left join 回 `feature_table` 接上特徵。母體與特徵分離：`inference_population` 定義「誰被評分」（membership），`feature_table` 只負責「他有什麼特徵」（enrichment）。

**這張表的顆粒度是 `(time, entity)`，沒有 item 那一維。** `(entity, item)` 的特徵向量就是該 entity 的特徵加上一個類別純量，展開等於把同一列客戶特徵抄 `len(products)` 遍去配 `len(products)` 個純量——那些副本零資訊。落地不展開的版本讓 `feature_table` 的完整掃描從每月 `len(products)` 次降到 **1 次**（ADR-0010 §4 的成本帳）。

**它存的是 `preprocessor.json` 特徵欄的全集（扣掉 item），不是任何一個模型實際使用的子集。** 子集只在評分的 chunk 內由 `model.feature_names()` 切出來。這是它能跨 `model_version` 重用的前提：表以 `base_dataset_version` 為 `partition_filter`，同一個 base 版本下換模型重跑可以整張重用，換模型的成本只有評分本身。**不要「順手優化」成只存模型要的欄**——那會讓它與 `model_version` 綁死，而且沒有任何檢查會紅。

因此：

- 母體成員資格由 `inference_population` 決定，不再隱式等同於 `feature_table` 的客戶集合。
- 母體 grain 由其 ETL 保證唯一，因此不需 `dropDuplicates`。
- item 本身不需要存在於 `feature_table`。
- 每個 entity 在評分時會得到全部 products——展開發生在 driver 的內層迴圈，不在這張表上。
- 母體成員若在 `feature_table` 缺特徵，仍保留於中間表（特徵欄為 NULL），不中止。缺特徵成員數**每個日期 log 一行**，那是持久的可觀測紀錄；缺特徵的旗標欄本身不落地（它在 #188 之前也沒有落地過，只是活在那個已被合併掉的記憶體中繼上）。
- `feature_table` 的 `time + entity` 重複列仍會造成 enrichment 的 join fan-out，應在 source ETL 或資料驗收階段先排除。
- `snap_date` 在這張表上是**字串**而不是日期：分區目錄名不該取決於 Spark 的型別強制轉換，而續跑的規劃器比對的就是那些目錄名。

### 5.2 前處理與模型 feature contract

inference 使用模型 manifest 指向的 base dataset preprocessor，不會重新 fit categorical encoding。未知類別值會依共用 preprocessor 邏輯編碼為 `-1`，並在 log 留下 `build_inference_population_features: N unknowns in column '…'` 的 WARNING——評分母體不是訓練母體，類別詞彙漂移在這裡是會真的發生的事。（這個聚合現在跑在未展開的中間表上，所以它掃的列數也降了 `len(products)` 倍，而它報的值一模一樣。）

**item 在塊內佔兩個位置。** `schema.item` 同時是 identity 欄與特徵欄，兩者的值形態不同：

| 位置 | 值 | 去向 |
|---|---|---|
| identity 欄 | 原始字串（`exchange_usd`） | 三張推論表的 `prod_name` 分區欄 |
| 特徵欄 | 整數 code（`category_mappings[item]` 的位置） | 餵進模型 |

所以 Spark 側的 `build_inference_population_features` **只編碼非 identity 的類別欄**（item 在那張表上根本不存在），identity 類別欄延後到 driver，由 `pdf_to_X` 對一份 copy 編碼。編碼值取自前處理產物的 `category_mappings`，**不是** `inference.products`：兩者內容由 A4 保證相同，順序不保證，取錯清單會讓所有 item 的分數整組錯位而每一項 sanity check 都照樣通過。論證見 ADR-0010 §4／§6。

**特徵順序與子集的權威是模型，編碼語意的權威是前處理產物。** 模型評分時使用模型本身保存的 ordered feature names（`model.feature_names()`），這讓 training-stage `feature_selection.exclude` 不需重建 dataset。`preprocessor.json` 存的是全集，它不知道有沒有做過特徵選擇，所以兩者的關係是：**模型宣告的欄位必須是產物 `feature_columns` 的保序子序列**，否則直接失敗、不做自動對齊。這一條同時抓到 stale 的產物（模型有的欄產物沒有）與不匹配的模型（順序被打亂）。論證見 ADR-0011 §5。

若模型要求的 feature 不在 scoring data 中，pipeline 會明確列出缺少欄位後中止。

### 5.3 逐 chunk 評分：外層桶、內層 item

`predict_and_write_scores` 的迴圈結構是**外層 `entity_bucket`、內層 item**：

```text
for 每個 (snap_date, entity_bucket):        ← 一次 toPandas()，只讀這一個分區
    for 每個 item:                          ← 就地覆寫 item 那一欄，重複使用同一份特徵
        pdf_to_X → model.predict → save()   ← 恰好一個分區
```

**迴圈順序這一項單獨值 `len(products)` 倍。** 反過來寫（外層 item、內層桶）功能完全正確、分數一模一樣，只是把整個母體讀 `len(products)` 遍——它省的不只是磁碟讀，還有 executor→driver 的 Arrow 序列化搬運。因為輸出無法分辨兩者，這件事由測試的**讀取次數**斷言守著，而不是靠 review 記得。

**每個 `(桶, item)` 算完立刻寫，一次 `save()` 恰好碰一個分區。** 這是硬約束而不是偏好：`HiveTableDataset.save()` 是 `insertInto` ＋ `partitionOverwriteMode=dynamic`，語意是「只動這次 frame 裡出現的分區，但對出現的那些是整個刪掉重建」。所以送進去的 frame 若跨兩個 chunk 的分區，第二次 save 會刪掉第一個 chunk 的列，**零錯誤訊息**。節點在交出 frame 前就地斷言它只含一種分區欄組合（在 pandas 上做，免費），而 `entity_bucket` 進 `unranked_predictions` 的分區欄就是為了讓不同桶的 save 不會互相覆蓋。

item 在 chunk 內佔兩個位置（§5.2 那張表）：identity 欄放原始字串、特徵欄放整數 code。切 X 的是 `io/extract.py` 的 `pdf_to_X`——training 的逐分區預測用的是同一支函式，所以「identity 類別欄延後到 driver 編碼」在兩條 pipeline 上是同一個實作而不是兩份。它切的 view 由模型的宣告當場建出（`{**preprocessor, "feature_columns": model.feature_names()}`），**不是**呼叫 training 那個依當前 config 推導 view 的 `apply_feature_selection`：`model_version` 指向舊模型時，當前 config 的 `feature_selection.exclude` 未必是那個模型訓練時的值。

**續跑。** 節點先問 `unranked_predictions` 哪些分區已經存在（`existing_partition_values()`，純 metastore、零掃描，且由 `partition_filter: model_version` 保證只答本次模型），再算出這次要做哪些 chunk。分區已存在即跳過，`--rebuild-dates` 可以推翻這個判斷。**一個決定少做事的節點必須說出它決定不做什麼**，否則「靜默地漏做」和「正確地跳過」長得一模一樣。規劃邏輯是不依賴 Spark 的純函式（`pipelines/inference/steps/chunk_plans.py`），所以它的測試在毫秒級。

「說出來」分三個層次，**不要把它們當同一件事**（#195）：

| 在哪 | 有什麼 | 誰讀 |
|---|---|---|
| log 的 `[chunks] predict:` 一行 | processed／skipped／rebuilt／surplus 的**計數**（不含清單，理由見 `docs/agents/deliberate-non-goals.md`） | 跑的當下的人 |
| `score_manifest`（節點的第一個 output） | **四份**逐 chunk 清單（processed／skipped／rebuilt／empty）＋ `expected_partitions`／`written_partitions` | `rank_predictions` 與 `validate_predictions`；**memory-only，跑完就沒了** |
| `chunk_report.json`（節點的第二個 output，見 §6.3） | 上面那四份 ＋ **第五份 `chunks_surplus`**（原本只到一行 warning）＋ 摘要 ＋ 寫它的 `run_id`，落在磁碟上 | 事後回來問「那一次到底跳過了哪些」的人 |

同一份資料寫兩次而不是把 `score_manifest` 直接落地，理由在 §7.4 最後一條。

**摘要的計數不是五個互斥桶。** `grid`＝`processed` ＋ `skipped` ＋ `empty`（每個 chunk 恰好落在其中一格）；`of_which_rebuilt` 是**其中**被 `--rebuild-dates` 強制重做的那些，不是第四格，key 名字就是為了擋住「五個數字加起來」這個誤讀；`surplus` 在格點之外。

**空桶，以及「不該是空的空桶」。** 母體比桶數還小時會有桶完全沒有 entity，而 `insertInto` 對空 frame 不會建立分區。這是正常狀態，不是錯誤：manifest 把它們記在 `chunks_empty`，並且從「應該存在的分區」裡扣掉，所以 §6.1 的 `partition_completeness` 不會對小母體誤報。

但「扣掉」開了一個洞，所以節點會先問中間表**哪些桶真的有分區**（每個月一次 metastore 級的 distinct）。一個桶有分區卻讀回零列 → **直接 raise**，因為那代表那些 entity 即將從發布結果裡無聲消失，而沒有任何下游看得到：該 chunk 沒有分區可缺，`completeness` 也只看得到「在場的組」——缺席的 entity 不構成任何組。桶沒有分區才算合法的空桶。所以「這是小母體」是有證據的判定，不是假設。

這條檢查抓不到的殘留是「中間表本身就是舊的」（例如用 `--from-node predict_and_write_scores` 接一張為舊母體建的表）：那時缺席的 entity 連分區都沒有，兩邊一致。那條由續跑的既有殘留承擔——判準是分區存在、不是分區新鮮，`--rebuild-dates` 是覆寫手段。

`model_version` **不由節點注入**：三張輸出表都宣告 `partition_filter: model_version`，值由 catalog 在 `save()` 時從 `parameters["model_version"]` 補上。讀回來時同樣由 catalog 發 `WHERE model_version = '…'` 並把該欄 drop 掉，所以節點看到的 frame 沒有這一欄。

## 6. 發布驗證與產物

### 6.1 驗證分兩層

驗證跑在兩個地方。分界只有一條規則：**一個 chunk 只有一個 item，所以要把同一組的 item 互相比較的檢查在 chunk 內根本問不出來，其餘的都問得出來。** 哪一條在哪一層的唯一真實來源是 `src/recsys_tfb/pipelines/inference/steps/validation.py` 的 `CHUNK_CHECKS` 與 `BATCH_CHECKS`；下面兩張表是它的白話版。兩層都是 collect-all——收集該層所有失敗，再以單一 `ValidationError` 中止，`failures` 帶著每個 check 的名稱與細節。

**塊層**（`validate_scored_chunk`，每個 `(time, 桶, item)` chunk 寫出**前**跑一次；純 pandas、零 Spark action）：

| Check | 驗證內容 | 常見失敗原因 |
|---|---|---|
| `chunk_row_count` | 算出來的列數等於讀進來的 entity 數 | **設定造不出來**——長度不符時 pandas 在建構 `out_pdf` 就先 raise。這是對「建構保持列數不變」的迴歸防護，不是資料檢查 |
| `no_missing` | entity identity、item、time、score 不可為 NULL | 上游 key／feature 異常或模型輸出缺值 |
| `no_duplicates` | `time + entity + item` 不可重複 | 中間表的顆粒度不是 `(time, entity)` |
| `item_values_are_known` | 寫出去的 identity item 值必須落在 `inference.products` 裡 | identity 欄被寫成整數 code（ADR-0010 §6 實跑重現過） |

**整批層**（`validate_predictions`，對 `ranked_staging` 全表跑一次）：

| Check | 驗證內容 | 常見失敗原因 |
|---|---|---|
| `partition_completeness` | 評分節點說「應該存在」的分區集合，與 metastore 說「存在」的分區集合逐一相同 | 連續 save 互相覆蓋（缺分區）；`entity_buckets` 改過留下的舊桶（多分區） |
| `completeness` | 每個 query group 恰有 `len(products)` 列 | feature join fan-out、候選遺漏 |
| `rank_consistency` | rank 整體範圍為 `1..N`，且沿 rank 增加時 score 不可上升 | rank 被改寫或排序方向錯誤 |
| `score_varies_within_group` | 全平手的 query group **佔比**不得超過 `CONSTANT_GROUP_FAILURE_RATIO`（0.5）；未超過則只記 log | 餵給模型的 item 值退化成常數，同一 entity 的所有 item 拿到相同分數 |

**為什麼要分：主要理由是失敗得更早，不是省成本。** 分數算錯若等到整批層才抓，代價是全部 chunk 算完、rank 也跑完——單月數小時的 pipeline 上，這是「十分鐘知道」與「四小時後知道」的差別。省成本是附帶的：塊層的資料本來就在 driver 的 pandas frame 上，而整批層每一條檢查都是對整張 Hive 表的一次掃描。整批層的 Spark action 因此從七次降到**兩次**（一次分組聚合同時回答 `completeness`、`score_varies_within_group` 與 rank 範圍；一次 window pass 看 score 與 rank 的順序），`partition_completeness` 一次都不用——它比的是 manifest 已經帶著的兩份清單。

**`no_missing` 的 entity 那一半讀的是「進來的」frame，不是「寫出去的」frame。** 寫出去的 entity identity 經過 `astype(str)`，NULL 會變成字串 `"None"`——對著輸出檢查等於一條結構上不可能紅的斷言。

**`score_varies_within_group` 補的是資料層那一半。** `require_item_is_a_feature`（`pipelines/dataset/steps/feature_columns.py`）擋的是「設定漏了 item」，擋不住「設定對，但 pipeline 沒把正確的值餵進去」，而逐 chunk 評分把後者變成 driver 裡的一行。三層合起來是：config 層 `require_item_is_a_feature` → 塊層 `item_values_are_known` → 整批層 `score_varies_within_group`。

**它為什麼是比例而不是「有一組就紅」。** 平手不是只有這個 bug 才生得出來：`IsotonicRegression` 擬出來的是帶**平台**的單調函數，同一組的 raw score 全落在同一段平台時，校準後就完全相等——而 `training.calibration.method: isotonic` 是明文支援的設定。合成量測（5 萬列校準資料、正例率約 5%、20 萬 entity × 8 item）：**20 萬組中有 61 組全平手（0.03%），未校準則是 0 組**。所以「有一組就紅」會讓每一次**正確**的 isotonic 執行都擋在發布前——正是乘積形式在小母體上誤報的同一種形狀。

它要抓的故障在四個數量級之外：item 值退化是**程式碼**寫的，套用到每一個 chunk，會讓 **100%** 的組全平手。門檻取一半，是「大多數組」這個說法最粗的邊界。未超過門檻的平手仍然記 warning——那些組的組內排名確實是任意的，沉默會讓它無從查起。

另一個**知情的**誤報空間：一個從不對 item 分裂的模型本來就會讓同一組的分數合法地相同。刻意沒有用「模型的 item feature importance > 0」的啟動斷言把它關掉——真的遇到時，它報的是一個應該有人看的模型品質問題。

**`score_range`（分數介於 `[0, 1]`）已刪除，而且是刪除不是搬家。** 套了校準的路徑上，`[0, 1]` 由校準器的建構方式保證，這條斷言結構上不可能紅；未校準的 ranking objective（A7 允許、`inference.use_calibration: false` 明文支援）輸出的是無界實數，這條斷言是誤報。裝飾品或錯的，沒有第三種情形（ADR-0011 §2）。`tests/test_pipelines/test_inference/test_validation.py::TestScoreRangeIsGone` 釘住它保持刪除狀態。

**`partition_completeness` 取代了 #188 之前的 `row_count_match`**，因為後者的兩邊在新結構下都沒了：它比的是 ranked 列數對 `scoring_dataset` 列數，而未展開的 `inference_population_features` 比 ranked 輸出短 `len(products)` 倍，這個比較在每一次**正確**的執行上都會 fail；而且它讀的那個 frame 每次都要重跑一遍母體與特徵的 join。

改用分區集合而不是列數，是因為列數在續跑之後一定對不上：manifest 的 `n_rows_written` 只累加**這次真的寫出去的**列，被跳過的 chunk 貢獻 0。分區集合則兩種方向都抓得到（少了的、多出來的），而且是純 Python 比對兩份 manifest 已經帶著的清單——零掃描。

（ADR-0011 §3 把這條寫成「分區數 ＝ item 數 × 桶數」。實作改成集合比對，因為母體比桶數小的時候會有空桶、那些桶不會有分區，寫死的乘積會在小母體上誤報——本機端到端就是這種母體。）

### 6.2 Staging／validate／publish

發布順序固定為：

```text
unranked_predictions
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
| `inference_population_features` | Hive managed table | `base_dataset_version / snap_date, entity_bucket` | 母體 × 特徵（不含 item 展開）；同一 base 版本下換模型可整張重用 |
| `unranked_predictions` | Hive managed table | `model_version / snap_date, item, entity_bucket` | 可重用的未排名分數；`entity_bucket` 讓每個 chunk 的 save 落在自己的分區 |
| `ranked_staging` | Hive managed table | `model_version / snap_date, item` | 發布前結果與失敗排查 |
| `ranked_predictions` | Hive managed table | `model_version / snap_date, item` | 正式 production 排序結果 |
| `manifest.json` | driver-local JSON | `data/inference/<model_version>/<first_snap_date>/` | 記錄模型、dataset IDs、參數、run ID 與 git commit；另含 `scoring_chunks` 摘要（計數 ＋ 每月一列，見下一列） |
| `chunk_report.json` | driver-local JSON | 同上 | 這一次評分的**逐 chunk 清單**：processed／skipped／rebuilt／empty／surplus ＋ `expected_partitions`／`written_partitions`，加上摘要（`counts`、`by_snap_date`——欄位語意見 §5.3 末）與寫它的 `run_id`。零下游消費者，存在的理由是事後回答「跳過的是哪些」。體積隨格點線性成長（本機實測 80 chunk ＝ 15 KB，推到 12 月 × 20 桶 × 22 item ≈ 5,280 chunk 約 1 MB 等級），所以只有摘要進 `manifest.json` |
| `parameters_inference.json` | driver-local JSON | 同上 | 保存本次 inference 設定 |
| `latest` | symlink | `data/inference/latest` | 指向最近成功完成的 inference run 目錄 |

三張輸出表的第一層都是 `model_version`（catalog 的 `partition_filter`，與 training 的 `training_eval_predictions` 相同），後面是 `partition_cols`。**`unranked_predictions` 多一個 `entity_bucket`，另外兩張沒有**：那是純計算層的機制欄，`rank_predictions` 讀完就丟，不進對外契約。所有下游讀取路徑都不需要知道它存在。
Hive tables 採 dynamic partition overwrite，只覆寫本次 DataFrame 實際包含的 partitions（`model_version` 由 filter 固定），其他模型與日期不受影響。
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
9. 檢視 `build_inference_population_features` 的 feature coverage log：每個 snap_date 的缺特徵成員數是否在預期範圍；異常偏高代表 feature ETL 與母體不對齊。
10. 檢視 `[chunks] predict:` log 的 processed／skipped／rebuilt／surplus 四個數字。全新的一個月應該是 processed ＝ item 數 × 有資料的桶數、其餘為 0；surplus 非 0 代表有舊桶的分區留在表上（通常是 `entity_buckets` 被改過）。**事後才回來看的話 log 未必還在**——`manifest.json` 的 `scoring_chunks.by_snap_date` 有同樣的數字按月拆開，`chunk_report.json` 有逐 chunk 清單。
11. `ranked_predictions` 的分區目錄**不該**出現 `entity_bucket=`。出現就代表機制欄漏進了對外契約。

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

### 6.5 分區結構遷移（一次性、破壞性）

**適用對象**：在 issue #187 之前跑過 inference、Hive 裡已經有這三張表的環境。全新環境不需要做任何事——表是第一次被建出來的，形狀就是新的。

Issue #187 把三張表的 `model_version` 從 `partition_cols` 提為 `partition_filter`，並把 `score_table` 改名為 `unranked_predictions`。**這不能就地套用**：

- `partition_filter` 的鍵在實體分區順序上排在 `partition_cols` **之前**（`HiveTableDataset._insert_column_order`），所以新的目錄結構是 `model_version=… / snap_date=… / prod_name=…`，舊的是 `snap_date=… / prod_name=… / model_version=…`。
- 建表語句是 `CREATE TABLE IF NOT EXISTS`（`HiveTableDataset._build_create_ddl`），**對已存在的表不做任何事**，不會改分區順序。
- `insertInto` 是**位置對應**，不是名稱對應。

三者疊起來的後果是：舊表照舊以 `snap_date` 為第一個分區欄，而新程式碼送進來的第一欄是 `model_version`。於是 `model_version` 的值被寫進 `snap_date` 分區、`snap_date` 寫進 `prod_name`……三個分區欄都是 `STRING`，**型別檢查不會擋，零錯誤訊息，六項 sanity check 全綠**。

因此遷移只有一條路：**先 DROP 再讓 pipeline 重建**。

```sql
-- 三張都是 managed table，DROP 會一併刪掉資料檔。
DROP TABLE IF EXISTS <db>.score_table;          -- 舊名，改名後不再被任何東西寫入
DROP TABLE IF EXISTS <db>.ranked_staging;
DROP TABLE IF EXISTS <db>.ranked_predictions;
```

本機（`--env local`，內嵌 Derby ＋ `data/local_warehouse/`）：

```bash
export SPARK_CONF_DIR=$PWD/conf/spark-local
bash scripts/local_spark_shell.sh sql -e "
  DROP TABLE IF EXISTS ml_recsys.score_table;
  DROP TABLE IF EXISTS ml_recsys.ranked_staging;
  DROP TABLE IF EXISTS ml_recsys.ranked_predictions;"
```

DROP 之後重跑一次 inference，三張表會以新形狀重建。**這一步會丟掉歷史分區**：`ranked_predictions` 裡其他 `model_version` 的既有結果不會自動搬過來。需要保留的話，先把要留的版本讀出來另存，重建後再依新結構寫回去；或者接受重跑那些版本的成本。

驗收（實體目錄名是唯一看得到錯位的地方）：

```bash
ls data/local_warehouse/ml_recsys.db/ranked_predictions/
# 期望：model_version=<版本>/ ；若看到 snap_date=... 在最外層，就是沒 DROP 就套了新設定
```

`scripts/local_e2e.sh` 末段的 assert 把這條釘住：它要求最外層是 `model_version=` 且值等於這次跑的版本。

#### #188 的第二次遷移：`unranked_predictions` 多一個分區欄

**適用對象**：在 #188 之前跑過 inference 的環境（含只做過 #187 遷移的）。

`unranked_predictions` 的 `partition_cols` 從 `snap_date, prod_name` 變成 `snap_date, prod_name, entity_bucket`。`CREATE TABLE IF NOT EXISTS` 同樣不會改既有表，所以**這張表也必須先 DROP**：

```sql
DROP TABLE IF EXISTS <db>.unranked_predictions;
```

**這一次的失效方向比 #187 溫和：** 送進 `insertInto` 的欄數從 5 變 6，Spark 會直接 raise「欄數不符」而不是靜默寫進錯誤的分區欄。所以忘記 DROP 的後果是一個明確的失敗，不是一張看起來正常的錯表。仍然要做，只是不必擔心它會靜默通過。

`ranked_staging` 與 `ranked_predictions` 的形狀在 #188 沒有改，不需要再 DROP。`inference_population_features` 是新表，第一次跑就會以正確形狀建出來。

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
| 只修改 ranking node | `--from-node rank_predictions` | 可重用目前 model/date 的 `unranked_predictions`；補跑的評分節點會跳過所有既有 chunk |
| 只修改評分邏輯 | `--from-node predict_and_write_scores --rebuild-dates <日期>` | 重用已落地的中間特徵表，但強制重算 chunk。不帶 `--rebuild-dates` 的話所有 chunk 都會被當成已完成而跳過 |
| 只想重新驗證 staging 並發布 | `--from-node validate_predictions` | 重用 staging；補跑評分節點只為了取得 manifest |
| 只檢查 staging，不發布 | `--only-node validate_predictions` | validation 成功後即結束 |
| 上游 feature 回補後要重算某月 | `--rebuild-dates <日期>` | 「分區已存在」不再是一個無法推翻的判斷 |
| 調整 `entity_buckets` | full inference ＋ 先 DROP `unranked_predictions` | 桶數是該表的分區欄；舊桶的分區沒人清，`partition_completeness` 會擋下發布 |
| 修改 products | 先依 item 變更流程重建上游，再 full inference | products 必須與 schema item 集合一致 |
| 修改 schema 或 preprocessor | `dataset → training → evaluation → promotion → inference` | 模型與前處理契約改變 |

### 7.3 多日期 manifest

一次設定多個 `snap_dates` 時，Hive 會寫入全部日期，但 driver-local inference 目錄目前只使用清單中的第一個日期：

```text
data/inference/<model_version>/<first_snap_date_without_hyphens>/
```

因此該目錄下的 manifest 代表整次多日期 run，不是只代表路徑名稱中的日期。若後續需要逐日期獨立稽核，應分次執行或擴充 manifest layout。

### 7.4 接續執行的安全邊界

- `unranked_predictions` 與 `ranked_staging` 會保留歷史 partitions，rank 與 validation nodes 兩層都要擋掉舊批次：`model_version` 由 catalog 的 `partition_filter` 在 load 時就過濾掉，`snap_dates` 由節點內的 `restrict_to_snap_dates` 裁掉。後者拿掉的話，第二個月起會讀回全部歷史月份、重算、把舊月份無聲重新發布。
- slicing planner 的 `exists()` 只檢查 table 是否存在，不驗證指定 partition 是否存在或是否由相同參數產生。
- 評分節點在任何切片裡都會被補跑，因為 `score_manifest` 是 memory-only 而排名與驗證都要它。補跑的成本是**一次 metastore 查詢**：所有 chunk 的分區都在，計畫器把它們全部歸入 skipped，一列都不會重寫。
- 但「跳過」的判準是**分區存在**，不是**分區內容正確**。上游資料換了之後要重算，必須用 `--rebuild-dates` 明說；否則已存在的分區會被當成已完成。
- `--only-node rank_predictions` 不會越過 validation gate，不能視為已發布。
- `--only-node publish_predictions` 仍會自動補跑 validation，不會繞過發布檢查。
- 切片成功後 manifest 會記錄 `resumed_from` 或 `only_node`，但 skipped artifacts 的來源參數仍需由操作者確認。
- **`score_manifest` 刻意不進 `catalog.yaml`，落地的是它的副本 `chunk_report.json`**（#195）。把 `score_manifest` 本身改成落地 dataset，上面第三條就會反轉：`--from-node rank_predictions` 會從磁碟載回**上一次**的 manifest 而不再補跑評分節點，而 `validate_predictions` 是**取值**用它（`expected_partitions`／`written_partitions`），於是驗證會拿另一次 run 的數字去對這一次的表。副本沒有這個問題，因為沒有任何 node 讀它。代價是同一份清單在記憶體與磁碟各有一份，這是刻意付的。
- 這份副本由評分節點自己產出，**不是**另開一個節點：另開的節點會是 `rank_predictions` 的兄弟而不是祖先，於是 `--from-node rank_predictions` 會把它切掉——而那正好是評分節點被補跑、清單最值得留下來的那種 run。
- **`chunk_report.json` 帶 `run_id`，`manifest.json` 只在兩者相符時才引用它。** 版本目錄 `data/inference/<model_version>/<snap_date>/` 是同一個模型同一個月的每一次 run 共用的，而 `--only-node build_inference_population_features`（§8 的既有操作）不會經過評分節點——上一次的 `chunk_report.json` 就還躺在那裡。沒有這道比對的話，這一次的 `manifest.json` 會引用上一次的跳過清單，正好是本功能要消滅的那種混淆。不相符時 CLI 印一行 warning 並略過，檔案本身保留不動（它正確描述的是上一次那個 run）。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| `No 'best' symlink found` | 尚未 promotion 就使用預設模式 | 審核模型後執行 `promote_model.py`，或明確指定版本 |
| `Model version directory not found` | `--model-version` 拼錯或 training 未完成 | 檢查 `data/models/<version>/` |
| model manifest warning 並 fallback latest | 舊模型缺 manifest 或 dataset IDs | 補齊 manifest；正式環境不要依賴 fallback |
| `inference_population missing inference.snap_dates` | 母體日期尚未產出、格式錯誤或 source ETL 未完成 | 查 `inference_population` distinct 日期並修正 `snap_dates` |
| `Missing feature columns in inference population` | 新模型需要的欄位未出現在推論日期 feature table | 對齊 feature SQL、dataset preprocessor 與模型版本 |
| `inference_population_features is missing columns required by the model` | 中間表是在模型需要的欄位存在之前落地的 | 用 `--only-node build_inference_population_features` 重建中間表（它以 `base_dataset_version` 為 scope，重建不影響其他模型） |
| `model.feature_names() is not an order-preserving subsequence` | 模型與 `preprocessor.json` 不匹配：模型有的欄產物沒有（stale 產物），或順序被打亂 | 檢查模型 manifest 指向的 `base_dataset_version` 是否就是訓練時那一個 |
| `a single save must cover exactly one partition` | 評分節點被改成一次送多個 chunk | 這是約束 C 的守門員，不是誤報；把 save 放回內層迴圈 |
| `partition_completeness` | 缺分區＝連續 save 互相覆蓋；多分區＝`entity_buckets` 改過留下舊桶 | 前者查 `unranked_predictions` 的 `partition_cols` 是否還有 `entity_bucket`；後者 DROP 舊桶的分區或整張表重跑 |
| `No scoring rows found` | 設定日期沒有 entity，或前處理後資料為空 | 查 feature table row count 與日期條件 |
| A4 products mismatch | `inference.products` 與 schema item 清單不一致 | 同步兩處完整 item 集合 |
| 訊息帶 `(A27) inference.snap_dates` / `entity_buckets` / `products` | 評分格點 `snap_dates × entity_buckets × products` 有一軸是空的或 0 | 在 `parameters_inference.yaml` 補上該鍵。**訊息會一次列出全部有問題的軸**，所以一輪就能改完；這一關在起 Spark 之前，看到它代表還沒有付任何 cold start |
| `score_range` | raw score 小於 0 或大於 1 | 檢查 objective、calibration 與 `use_calibration` |
| `no_missing` | identity、score 或 rank 出現 NULL | 查 staging 的欄位 NULL count 與上游 feature keys |
| `completeness` | query group 候選數不是 products 數 | 查 feature key 重複、join fan-out 或候選遺漏 |
| `rank_consistency` | rank 範圍或 score 順序異常 | 重新執行 `rank_predictions`，檢查 staging 是否被外部改寫 |
| `no_duplicates` | 同一 identity 重複 | 母體唯一性已由 ETL 保證；優先檢查 `feature_table` enrichment 的 `time + entity` 唯一性是否造成 join fan-out |
| validation 失敗但 staging 有資料 | 正常的 publication gate 行為 | 查 `ranked_staging`；production 未被本批覆寫 |
| 指定候選模型後下游讀錯版本 | `ranked_predictions` 同時保留多個 models | 下游查詢與 evaluation 明確指定 `model_version` |
| `Unknown node` | node 名稱拼錯 | 先以相同環境執行 `--list-nodes` |
| 切片顯示大量 auto-included | 所需輸入是 memory-only 或 partition 不可用 | 依 dry-run 計畫確認補跑成本，必要時 full run |
| Driver OOM | 單一桶的特徵矩陣太大 | **先調高 `inference.entity_buckets`**（這是這個旋鈕存在的理由）；桶數已在健康窗口上界附近時再加 driver memory |
| `--rebuild-dates` 收下了但什麼都沒重算 | 切片把評分節點排除掉了 | log 會印 `[rebuild] WARNING: … had no effect`；改用 `--from-node predict_and_write_scores` 或不帶切片旗標 |

validation 失敗時，先從 exception 的 checks 清單判斷是模型輸出、候選母體、feature identity 或 ranking 問題，再查相同 model/date 的 `ranked_staging`。

## 9. 限制與注意事項

- 母體成員資格由 `inference_population` 定義；`feature_table` 只提供特徵。缺特徵的母體成員仍會被評分（特徵欄為 NULL），只在 log 留下每月的缺特徵成員數，不會被自動排除——是否排除由下游決定。
- 目前每個 entity 共用同一份 products 清單，不支援 per-entity eligibility。
- score 必須位於 `[0, 1]`；這對未校準的 ranking objective 是額外限制。
- 模型評分必須在 driver（生產禁 UDF），所以每個 `(entity 桶, item)` chunk 的特徵會被收集到 pandas，不是完全 distributed inference。與 #188 之前的差別是**不再累積**：算完就落地，driver 上同時只有一個桶。
- **driver 峰值只有下界推算，沒有實測。** `pdf_to_X` 的 `X_df.values` 會把 frame 攤成單一 numpy 陣列，共同 dtype 由所有欄決定。**#283 之後特徵側已經同質**——`cast_numeric_features_to_storage_type` 把所有數值特徵欄（decimal／double／float／整數族／boolean）轉成 `dataset.numeric_feature_storage_type` 宣告的型別，所以共同 dtype 就是宣告值（預設 float32），不再有「一欄 int64 讓整個矩陣翻倍」那條路。仍是下界的理由有兩個：**延後編碼的 identity 類別欄**在 `pdf_to_X` 才成為 `Categorical.codes`，不經過 Spark 側的 cast（實測 float32 ＋ int8／int16 codes 還是 float32，但類別數 >32767 讓 codes 變 int32 時共同型別會回到 float64）；以及實際值取決於生產 `feature_table` 的欄數與 chunk 大小。
- **這道發布閘買到的是「順序」，不是「原子性」。** production 只在整批驗證通過後才被觸碰，但 `publish_predictions` 的寫入同樣是 `insertInto` ＋ dynamic overwrite，跨分區的 commit 不是全有全無。逐 chunk 化把失敗視窗從「整條 run」縮到「最後那一次寫」，那是真實的收益，但它不等於原子發布。
- **跨 chunk 的一致性沒有機制保證。** 一次 run 裡不同 chunk 用的是同一個模型與同一張中間表，但如果中間表在 run 進行中被另一個 process 改寫，前後 chunk 會基於不同的特徵。這個情況今天沒有任何檢查會紅。
- score 相同時沒有額外 tie-break key，Spark `row_number` 對同分 items 的相對名次不保證穩定。
- completeness check 驗證每組候選數量，不會獨立比對每組的實際 item set；目前依賴內層 item 迴圈與 duplicate check 共同維持候選正確性。
- `partition_completeness` 驗的是分區的**存在**，不是分區的**內容**。一個內容錯誤但分區齊全的表照樣通過（那是其他五條檢查的職責）。
- 續跑的「跳過」判準是分區存在，不是分區新鮮。上游回補之後必須用 `--rebuild-dates` 明說。
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
