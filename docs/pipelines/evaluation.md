# evaluation pipeline

> 將指定模型與 `snap_date` 的預測整理成統一評估資料，以 `(time, entity)` 為 query group 計算排序指標，並產生標準報表或模型比較報表。
> 主要流程為：選擇預測來源 → 補入 ground truth、rank 與分群欄位 → 計算排序指標 → popularity baseline → HTML 報表 → 持久化 enriched predictions。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 模型上線前 test 評估、上線後成效監控，以及模型／外部結果比較 |
| 執行指令 | `python -m recsys_tfb evaluation` |
| 主要輸入 | `training_eval_predictions` 或 `ranked_predictions`、`label_table`、可選的 segment 與 compare sources |
| 主要輸出 | `report.html`、可選的 `report_comparison.html`、`enriched_eval_predictions` |
| 設定檔 | `conf/base/parameters_evaluation.yaml` |
| I/O 設定 | `conf/base/catalog.yaml` |
| 上游 pipeline | `training` 或 `inference` |
| 下游用途 | 模型人工審核、上線監控與模型比較 |

evaluation 有兩種評估情境，差別在 Model A 的預測來源：

| 情境 | 指令 | 預測來源 | Ground truth | 使用時機 |
|---|---|---|---|---|
| Training 後評估 | `evaluation --post-training` | `training_eval_predictions` | 沿用 training 當時保存的 test label | 候選模型剛訓練完成，進行上線前審核 |
| 上線後監控 | `evaluation` | inference 發布的 `ranked_predictions` | 依目前 `label_table` 補入 | label 觀察窗成熟後，追蹤正式排序結果 |

兩種情境都使用相同的排序指標與報表。`--post-training` 不是另一套 metric，只是改讀 training 的 held-out test 預測；預設模式則評估已通過 inference sanity check 並正式發布的結果。

evaluation 另有三種執行模式：

| 模式 | 旗標 | 標準報表 | 比較報表 | 更新 enriched data |
|---|---|:---:|:---:|:---:|
| 標準 | 無 compare 旗標 | ✓ |  | ✓ |
| 標準加比較 | `--compare <key>` | ✓ | ✓ | ✓ |
| 只比較 | `--compare-only <key>` |  | ✓ |  |

`--compare-only` 不代表只計算部分指標；它會直接重用先前持久化的 Model A `enriched_eval_predictions`，略過標準評估、baseline、diagnostics 與 `report.html`，只重新載入 Model B、對齊共同母體並產生比較報表。

## 2. 執行前準備

執行 evaluation 前，建議依序確認：

1. **模型版本存在**：evaluation 會讀取 `data/models/<model_version>/manifest.json`，取得該模型使用的 base、train 與可選 calibration dataset IDs。
2. **選對評估情境**：候選模型的 test 評估使用 `--post-training`；已發布批次結果的監控使用預設模式。
3. **候選模型明確指定版本**：省略 `--model-version` 時一律解析 `data/models/best`。尚未 promotion 的新模型必須明確傳入版本，否則可能評估到上一個正式模型。
4. **預測 partition 已存在**：post-training 模式需要對應 `training_eval_predictions`；監控模式需要 inference 已成功發布對應的 `ranked_predictions`。
5. **Ground truth 已成熟**：上線後監控必須等 label 觀察窗結束並完成資料回補。過早執行會將尚未發生或尚未入庫的正例視為負例。
6. **評估日期正確**：`evaluation.snap_date` 必須與預測表中的日期格式和值一致，並使用 ISO `YYYY-MM-DD`。
7. **分群來源可讀取**：每個 `segment_columns` 都必須有對應的 `segment_sources`，且 Hive table、join keys 與 segment 欄位均存在。
8. **比較來源已準備**：使用 `--compare`／`--compare-only` 前，先確認 `compare_sources` key、來源表、model version、item mapping 與日期 coverage。

監控模式會以預測 rows 為母體，依 `time + entity + item` left join `label_table`；沒有 label row 的候選會補成 `label = 0`。
這適用於「label table 只保存正例」的 sparse table，但前提是缺 row 的業務語意確實代表負例，而不是 ground truth 尚未成熟。

post-training 模式會保留 `training_eval_predictions` 已保存的 label，不以後來更新的 `label_table` 覆寫，讓 evaluation 結果與 training 當時的 test 指標保持一致。

## 3. 設定方式

### 3.1 評估日期與 K

```yaml
evaluation:
  snap_date: "2026-01-31"
  k_values: [1, 2, 3, 4, 5, "all"]
```

| 設定 | 說明 |
|---|---|
| `snap_date` | 本次只評估的時間切點，必須使用 `YYYY-MM-DD` |
| `k_values` | 所有 metric 要實際計算的 K 值 superset |
| `"all"` | 在細 item 粒度解析為 distinct item 數；在 category 粒度重新解析為 distinct category 數 |

pipeline 會先依 `model_version` 與 `snap_date` 篩選預測。日期沒有任何資料時會列出該模型實際存在的日期後中止，不會退回整張表計算。

`k_values` 決定 metric computation；`report.display.primary_map_k` 與 `guardrail_recall_k` 只決定報表顯示哪些已計算結果。display 中使用的 K 應包含在 `k_values`，否則報表對應欄位會沒有值。

主要指標包括：

| 層次 | 指標 |
|---|---|
| Overall／per-segment | `map@K`、`ndcg@K`、`precision@K`、`recall@K` |
| Per-item | `hit_rate@K`、`map_attr@K`、`ndcg_attr@K`、`mean_pos` |
| Macro average | 對 item、segment 或 item-segment 等權平均 |

overall 指標先在每個 query group 計算，再對 query 等權平均；per-item attribution 則只在該 item 為正例的 rows 上彙整。兩者回答的問題不同，不應將 `map_attr@K` 誤讀為單一 item 自己的 mAP。

完全沒有正例的 query group 無法定義 AP 與 NDCG，因此會從 metric computation 排除；報表仍會記錄 `n_queries` 與 `n_excluded_queries`。

### 3.2 分群評估

```yaml
evaluation:
  segment_columns:
    - cust_segment_typ

  segment_sources:
    cust_segment_typ:
      table: ml_recsys.sample_pool
      key_columns: [cust_id, snap_date]
      segment_column: cust_segment_typ
```

每個 segment source 會：

1. 讀取指定 Hive table。
2. 選取 `key_columns + segment_column`。
3. 依 `key_columns` 去重，避免 source 比評估資料更細時造成 join fan-out。
4. left join 至 `eval_predictions`。

若輸入資料原本已有同名 segment column，框架會先移除，並以 `segment_sources` 指定的資料為準。來源表不存在或缺少必要欄位時會立即中止。

`segment_columns` 中每一欄都必須由某個 source 的 `segment_column` 提供，否則 CLI 設定一致性檢查會阻擋。
目前 metric pipeline 只會使用清單中第一個實際存在的 segment column 計算 per-segment 與 per-item-segment 指標；若要評估多種分群，應分次調整第一個欄位並執行 evaluation。

segment source 可指向任何 keyed Hive table，不限 `sample_pool`。實務上建議**分群來源跟著該評估情境的母體走**：

| 評估情境 | 預測來源 | 建議 segment source |
|---|---|---|
| 監控模式 | `ranked_predictions`（inference 輸出） | `inference_population`（inference 評分母體） |
| post-training 模式 | `training_eval_predictions` | `sample_pool`（training 母體） |

監控模式指向 `inference_population` 可讓切群定義對齊**實際被評分的客戶**，避免用 training 母體切 inference 結果造成分群定義分歧。`inference_population` 的 grain 為 `(time, entity)`、一 key 一列，`dropDuplicates` 為 no-op、不會 fan-out，只要它帶有分群欄即可直接作為 segment source——evaluation 端只動 `segment_sources` config（程式不變，見 [`inference.md`](inference.md) §3.5）。

### 3.3 產品大類

```yaml
evaluation:
  product_categories:
    enabled: true
    unmapped: singleton
    mapping:
      fund: [fund_stock, fund_bond, fund_mix]
      exchange: [exchange_fx, exchange_usd]
      ccard: [ccard_bill, ccard_cash, ccard_ins]
```

啟用後，框架會在細 item 指標之外，再將每個 query group 的 items 彙整為 category：

- category score = 子 items 的最大 score，等同採用排名最前的子 item。
- category label = 子 items 的最大 label，只要任一子 item 為正例，該 category 即為正例。
- segment 欄位沿用 query group 中的值。
- 同一套 overall、per-item、per-segment 與 macro metrics 會在 category 粒度再計算一次。

mapping 右側的 item 必須存在於 `schema.categorical_values[item]`，未知 item 會 fail-fast。未出現在 mapping 的 item 目前只支援 `unmapped: singleton`，也就是各自成為單獨 category。

同一 item 不應重複出現在多個 categories；目前實作會以後讀到的 mapping 覆蓋先前結果，沒有額外衝突檢查。

### 3.4 Popularity baseline

```yaml
evaluation:
  baseline:
    lookback_months: 12
```

baseline 對每個評估日期 `S`，統計 `label_table` 在 `[S - lookback_months, S)` 期間各 item 的正例數，並以歷史正例數作為所有 entity 共用的 score。它不使用個人特徵，可用來判斷模型是否真正優於「所有人都推薦熱門 item」。

baseline 會在與模型相同的 evaluation rows 上重新排名，計算 overall 與 per-item 指標，再於報表呈現 Model、Baseline 與差異。

若指定回看期間完全沒有 label rows，目前實作會記錄 warning 並退回使用完整 `label_table`。此 fallback 可能包含評估日之後的資料而造成 leakage；看到該 warning 時不應直接採信 baseline，應先補齊歷史資料或修正 lookback。

將 `report.sections.baseline` 設為 `false` 時，pipeline 會直接跳過第二次 baseline metric computation。

### 3.5 報表內容

```yaml
evaluation:
  report:
    sections:
      dataset_overview: true
      primary_map: true
      guardrail_recall: true
      per_item_attr: true
      category: true
      per_segment: true
      diagnostics: true
      baseline: true
    display:
      primary_map_k: [1, 3, 5, "all"]
      guardrail_recall_k: [1, 2, 3, 4, 5]
      recall_colorscale: {low: 0.0, high: 1.0}
    diagnostics:
      include_distributions: true
      include_calibration: true
      n_calibration_bins: 10
```

標準報表固定包含 headline 與 glossary，其餘 sections 可個別開關：

| Section | 主要內容 |
|---|---|
| `dataset_overview` | rows、entities、items、正例數、正例率與各 item 概況 |
| `primary_map` | overall mAP、NDCG、precision、recall |
| `guardrail_recall` | per-item recall 與 mean position |
| `per_item_attr` | 各 item 的 mAP／NDCG attribution |
| `category` | category 粒度 mAP 與 recall |
| `per_segment` | 各 segment 的 query-level 指標 |
| `diagnostics` | score 分布、rank heatmap、正例位置與 calibration curve |
| `baseline` | popularity 組成、Model／Baseline／Delta |

diagnostics 的 row-level aggregation 在 Spark 執行，只將 histogram、quartile、rank matrix 與 calibration bins 等小型結果交給報表層，不會將完整預測資料嵌入 HTML。

calibration curve 只用來觀察 score 與正例率的關係；對 LTR score 或未校準的 binary score，不應因圖形存在就將 score 當成真實機率。

### 3.6 模型與外部結果比較

`compare_sources` 定義 CLI 可使用的比較來源。每個 key 都需要 `kind` 與報表顯示用的 `label`。

#### 同框架 model version

```yaml
evaluation:
  compare_sources:
    previous_model:
      kind: model_version
      label: "Previous production model"
      model_version: "abcdef12"
      source: enriched_eval_predictions
```

`source` 可選：

| Source | 前提 | 適用情境 |
|---|---|---|
| `enriched_eval_predictions` | Model B 已跑過標準 evaluation；預設值 | `--compare-only`，或希望雙方都使用已 enriched 的資料 |
| `ranked_predictions` | Model B 已完成 inference 發布 | 比較正式上線結果 |
| `training_eval_predictions` | Model B 已完成 training | 比較兩個候選模型的 test 預測 |

同框架來源假設 item 名稱已一致，不接受額外 `columns` 或 `prod_mapping`。

#### 外部 Hive 結果

```yaml
evaluation:
  compare_sources:
    external_project:
      kind: external_hive
      label: "External Project"
      table: other_project.predictions
      columns:
        cust_id: customer_id
        snap_date: as_of_date
        prod_name: item_code
        score: prediction_score
      prod_mapping:
        ext_fund_a: fund_stock
        ext_fund_b: fund_bond
      unmapped_policy: fail
```

`columns` 將外部欄位轉成框架使用的 entity、time、item 與 score 欄位。`prod_mapping` 將外部 item 值映射至本框架 item；多個外部 items 可映射到同一個內部 item，此時以最大 score 合併。
目前 `columns` 的映射角色固定使用 `cust_id`、`snap_date`、`prod_name` 與 `score` 四個 key；若專案自訂 schema 欄位角色，外部比較功能仍需配合這組 canonical keys。

外部資料出現 mapping 未涵蓋的 item 時：

| Policy | 行為 |
|---|---|
| `fail` | 立即中止並列出未映射 items，預設且較安全 |
| `drop` | 記錄 warning 後排除未映射 items |

比較前會取得雙方 entity 集合與 item 集合的交集，限制到共同範圍後分別重新排名、重新計算指標，並在報表列出完整與共同 coverage、被排除的 items，以及 Model A、Model B 與 Delta。

比較報表沒有統計顯著性檢定；Delta 只表示共同範圍上的指標差值 `A - B`。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--model-version <id>` | `best` | 指定要評估的模型版本 |
| `--post-training` | 關閉 | 改讀 `training_eval_predictions`；預設讀 `ranked_predictions` |
| `--compare <key>` | 無 | 執行標準評估並額外產生比較報表 |
| `--compare-only <key>` | 無 | 讀取既有 enriched data，只產生比較報表 |
| `--from-node <name>` | 無 | 從指定 node 的拓撲位置開始，並執行其後 nodes |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開 |
| `--list-nodes` | 關閉 | 列出 node 名稱與接續成本 |

`--compare` 與 `--compare-only` 互斥，傳入的 key 必須存在於 `evaluation.compare_sources`。`--from-node` 與 `--only-node` 也互斥；`--list-nodes` 不可與兩者併用。

`--dry-run` 與 `--list-nodes` 不會執行 nodes、寫入 Hive 或報表，但 CLI 仍會載入設定、初始化 Spark、解析 `model_version`、讀取 model manifest 並查詢 catalog 產物是否存在。

### 4.2 Training 後評估

候選模型尚未 promotion 時，應明確指定版本：

```bash
python -m recsys_tfb evaluation \
  --env production \
  --post-training \
  --model-version <candidate_model_version>
```

此模式讀取 training 的 test predictions，並沿用其中保存的 label。它適合模型上線前審核，但評估母體是 dataset pipeline 建立的 test set，不等同正式 inference 的完整上線母體。

省略 `--model-version` 不會自動選擇最新 training 產物，而是使用 `best`；這通常只適合重新檢查目前正式模型的 training test 結果。

### 4.3 上線後監控

```bash
python -m recsys_tfb evaluation \
  --env production \
  --model-version <production_model_version>
```

若要監控目前 `best`，可省略版本：

```bash
python -m recsys_tfb evaluation --env production
```

此模式讀取 inference 正式發布的 `ranked_predictions`，再依目前 `label_table` 補入 ground truth。應在該 `snap_date` 的 label 觀察窗成熟後執行。

### 4.4 標準評估加模型比較

```bash
python -m recsys_tfb evaluation \
  --post-training \
  --model-version <model_a> \
  --compare previous_model
```

這會完整產生：

- Model A 的標準 `report.html`
- Model A 的 `enriched_eval_predictions`
- Model A 與設定來源的 `report_comparison.html`

Model A 的來源由 `--post-training` 決定；Model B 的來源由 `compare_sources.<key>.source` 決定。
比較兩個 training test 結果時，Model B 通常也應設為 `training_eval_predictions`；比較兩個正式推論結果時則使用 `ranked_predictions` 或已完成 evaluation 的 enriched data。

### 4.5 只產生比較報表

```bash
python -m recsys_tfb evaluation \
  --model-version <model_a> \
  --compare-only previous_model
```

適合以下情況：

- Model A 已完成標準 evaluation，只新增或修改 Model B compare source。
- 只想更換比較對象或顯示名稱，不需重做 Model A baseline 與 diagnostics。
- 想使用相同 enriched Model A rows 重新計算共同母體上的 A/B 指標。
- Model B 的預測剛準備完成，但 Model A 的標準報表不需重建。

必要前提：

1. `enriched_eval_predictions` 已存在目前 `model_version + snap_date` partition。
2. 該 partition 的來源情境、label、segment 與資料內容仍符合本次比較需求。
3. compare source 在同一日期有資料。

`--compare-only` 不讀 `training_eval_predictions` 或 `ranked_predictions` 來重建 Model A，因此 `--post-training` 在此模式不會改變 Model A 資料來源，建議不要混用。
若要切換 Model A 的 training／monitoring 情境，必須先用標準模式或 `--compare` 重新建立 enriched partition。

### 4.6 查看 nodes 與部分重跑

```bash
python -m recsys_tfb evaluation --list-nodes

python -m recsys_tfb evaluation \
  --from-node generate_report \
  --dry-run
```

`eval_predictions`、`evaluation_metrics` 與 `baseline_metrics` 都是記憶體中間結果。即使從 `generate_report` 接續，前次完整 run 成功時仍會自動補跑：

- `prepare_eval_data`
- `compute_metrics`
- `compute_baseline_metrics`

因此 evaluation 沒有便宜的「只重新渲染標準報表」接續點。需要只重做模型比較時，應使用持久化 `enriched_eval_predictions` 的 `--compare-only`。

`--from-node` 使用拓撲順序語意，會執行指定 node 與拓撲序中其後的 nodes；`--only-node` 則不執行下游 consumers。只要 pipeline 實際執行，CLI 仍會寫 evaluation manifest 並更新 `data/evaluation/latest`，所以單 node 模式應視為進階除錯工具。

## 5. 執行流程

### 5.1 標準模式

| 階段 | node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 整理資料 | `prepare_eval_data` | 預測、`label_table`、parameters | 篩選模型與日期、補 label、必要時重算 rank、連接 segments | `eval_predictions` |
| 模型指標 | `compute_metrics` | enriched rows | 計算 overall、per-item、per-segment、macro、overview 與可選 category metrics | `evaluation_metrics` |
| Baseline | `compute_baseline_metrics` | enriched rows、歷史 labels | 建立 popularity scores 並計算對照指標 | `baseline_metrics` |
| 持久化 | `persist_eval_predictions` | enriched rows | 透過 catalog 寫入 Hive | `enriched_eval_predictions` |
| 標準報表 | `generate_report` | rows、metrics、baseline | 產生互動式 HTML | `evaluation_report` |

`training_eval_predictions` 不保存 rank，因此 post-training 模式會依 score 在每個 query group 內重算。監控模式的 `ranked_predictions` 已有 rank，`prepare_eval_data` 會保留發布結果中的 rank；metric computation 本身仍會依 score 重新建立內部 position。

### 5.2 `--compare` 模式

標準流程後追加：

| 階段 | node | 處理內容 | 主要輸出 |
|---|---|---|---|
| 載入 Model B | `load_compare_predictions` | 依 compare source 載入、篩日期、轉欄位與 item mapping | `compare_predictions_raw` |
| 對齊母體 | `restrict_to_common` | 取共同 entities 與 items、雙方重新排名、必要時補 Model B label | `eval_predictions_common`、`compare_predictions_common`、coverage |
| 比較報表 | `generate_comparison_report` | 兩側重新計算 metrics 並產生 A/B/Delta | `evaluation_comparison_report` |

### 5.3 `--compare-only` 模式

| 階段 | node | 處理內容 |
|---|---|---|
| 驗證 Model A | `validate_enriched_eval_predictions_present` | 由 catalog 載入 model partition，再確認指定 `snap_date` 有 rows |
| 載入 Model B | `load_compare_predictions` | 載入設定的 comparison source |
| 對齊母體 | `restrict_to_common` | 取共同範圍並重新排名 |
| 比較報表 | `generate_comparison_report` | 產生 `report_comparison.html` |

比較時，若 Model B 本身已有 label，例如來源為 `enriched_eval_predictions` 或含 label 的 `training_eval_predictions`，框架會沿用該 label；若沒有 label，才從目前的 `label_table` left join 並將缺值補為 0。
比較兩側必須確保使用相同 ground truth 定義與資料成熟度。

## 6. 產物與驗收

### 6.1 主要產物

| 產物 | 位置或儲存方式 | 產生模式 |
|---|---|---|
| `report.html` | `data/evaluation/<model_version>/<YYYYMMDD>/report.html` | 標準、`--compare` |
| `report_comparison.html` | `data/evaluation/<model_version>/<YYYYMMDD>/report_comparison.html` | `--compare`、`--compare-only` |
| `manifest.json` | `data/evaluation/<model_version>/<YYYYMMDD>/manifest.json` | 所有實際執行模式 |
| `enriched_eval_predictions` | Hive，以 `model_version` 與 `snap_date` partition | 標準、`--compare` |
| `latest` alias | `data/evaluation/latest` | 指向最近完成的 evaluation 目錄 |

evaluation metrics 目前不會另存成 JSON；聚合結果直接用於產生 HTML。需要機器可讀的長期指標歷史時，應另外建立 metrics sink，而不是從 report HTML 反向解析。

`enriched_eval_predictions` 保存 Model A 的 identity、score、rank、label 與 segment enrichment，供後續比較重用。catalog 使用 `columns: "auto"`；新增 segment 欄位時可做 append-only schema evolution，舊 partitions 缺少的新欄位會是 NULL。

### 6.2 驗收重點

標準評估完成後至少確認：

1. 報表 metadata 的 model version 與 snap date 正確。
2. `n_queries` 大於零，`n_excluded_queries` 比例合理。
3. dataset overview 的 entities、items、rows 與 positives 符合該批次預期。
4. 主要 `map@K`、`ndcg@K` 與 `recall@K` 使用的 K 符合實際展示空間。
5. per-item 與 per-segment 沒有被整體平均掩蓋的明顯退化。
6. popularity baseline 的歷史期間有資料，log 沒有 leakage fallback warning。
7. diagnostics 的 score/rank 分布沒有異常集中、缺產品或不合理 calibration。
8. `enriched_eval_predictions` 的本次 model/date partition 有資料且 key 沒有非預期重複。

比較報表另需確認：

1. Model A、Model B source 與 label 顯示正確。
2. common entity/item coverage 足夠，沒有大量意外被排除的 items。
3. 雙方在共同範圍的 row coverage 與候選集合語意一致。
4. Delta 的方向為 `Model A - Model B`。
5. 外部 mapping 沒有非預期 drop 或多對一合併。

範例查詢：

```sql
SELECT snap_date, COUNT(*) AS rows,
       COUNT(DISTINCT cust_id) AS entities,
       COUNT(DISTINCT prod_name) AS items
FROM ml_recsys.enriched_eval_predictions
WHERE model_version = '<model_version>'
  AND snap_date = '<snap_date>'
GROUP BY snap_date;
```

實際 database、欄位名稱與 partition 設定以 `conf/base/catalog.yaml` 與 schema 設定為準。

## 7. 版本、重跑與恢復

### 7.1 Evaluation 沒有獨立版本 ID

evaluation 不會根據 `parameters_evaluation.yaml` 計算 hash，也沒有 `evaluation_version`。產物身分只由以下兩項決定：

```text
model_version
+ evaluation.snap_date
```

report path 會將 ISO 日期移除 `-`，例如 `2026-01-31` 寫入 `data/evaluation/<model_version>/20260131/`；Hive partition 仍保留實際 schema 中的日期值。

同一個 model version 與日期下修改 K、segments、categories、baseline、report sections 或 compare source，都會覆寫相同報表路徑；標準／`--compare` 模式也會覆寫相同 enriched partition。
manifest 會保存最後一次執行的 evaluation parameters、git commit、run ID、`post_training` 與 slicing metadata。

### 7.2 設定與重跑矩陣

| 修改內容 | 建議執行方式 | 原因 |
|---|---|---|
| `snap_date` | 標準 full run | 使用新的報表目錄與 Hive partition |
| `k_values` | 標準 full run | 需要重新計算所有 metrics |
| `segment_columns`／`segment_sources` | 標準 full run | 需要重新 join 並更新 enriched schema/data |
| `product_categories` | 標準 full run | 標準與比較 metrics 都需重新 collapse |
| `baseline.lookback_months` | 標準 full run | 需要重新讀歷史 labels 與計算 baseline |
| 標準 report sections／display／diagnostics | 標準 full run 或從 `generate_report` 接續 | metrics 是記憶體產物，接續仍會補跑 metric chain |
| `compare_sources` 或比較對象 | `--compare-only` | Model A enriched data 未變時可直接重做比較 |
| 比較報表使用的 K／category 設定 | `--compare-only` | 會在 common rows 上重新計算雙方 metrics |
| Model A 預測來源由 training 改為 monitoring，或反向切換 | 標準 full run | 必須重新建立 enriched Model A partition |
| `label_table` 回補或修正 | 標準 full run | 監控 labels、baseline 與 compare B labels 可能改變 |
| inference 重新發布同 model/date | 標準 full run | version ID 不變，但預測內容可能已覆寫 |
| 只更換比較顯示 label | `--compare-only` | 不需重算標準報表與 baseline |

### 7.3 Training 與 monitoring 共用 enriched partition

同一個 `model_version + snap_date` 的 post-training 與 monitoring evaluation 會寫入相同 `enriched_eval_predictions` partition，也使用相同 report path。最後一次標準／`--compare` run 會覆寫前一次內容。

因此：

- 不要假設 enriched partition 同時保存 training test 與 production monitoring 兩種母體。
- 使用 `--compare-only` 前，先確認最後一次建立 Model A enriched data 的模式。
- 若同一模型同一日期需要長期保留兩種評估情境，現有儲存鍵不足，需另加 scenario partition 或獨立 evaluation version。

### 7.4 部分重跑的安全邊界

- `catalog.exists()` 只能確認產物存在，不能證明內容來自目前 evaluation settings、label snapshot 或預測資料。
- `generate_report` 的上游 rows、metrics 與 baseline 都是 memory-only，因此從該 node 接續仍會補跑完整 metric chain。
- `enriched_eval_predictions` 是唯一為 comparison recovery 持久化的 row-level 中間產物；`--compare-only` 會先驗證指定 model/date partition 非空。
- `--compare-only` 不會更新 enriched partition，也不會重新產生標準 report。
- 位於 slicing 起點之前的資料讀取或驗證可能被跳過；來源資料變更時應 full run。
- slicing 執行會在 manifest 記錄 `resumed_from` 或 `only_node`，但不代表所有報表與 Hive 產物都已同步重建。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| 找不到 model version directory | 版本 ID 錯誤，或 training 未完成 manifest | 檢查 `data/models/<version>/manifest.json` |
| 找不到 `best` | 尚未 promotion，卻省略 `--model-version` | 對候選模型明確傳入版本，或先完成人工 promotion |
| 評估到上一版模型 | `--post-training` 仍省略 `--model-version` | post-training 不會自動選最新模型；指定 candidate ID |
| `No predictions found for evaluation.snap_date` | 日期錯誤、模式用錯、對應 partition 未產生 | 檢查 model、日期、`training_eval_predictions`／`ranked_predictions` |
| 報表正例率異常低 | label 觀察窗未成熟，或 sparse label 的缺 row 不代表負例 | 延後監控、補齊 label，確認資料語意 |
| post-training 與 training 指標不一致 | model/date 不同、K 定義不同，或 report 讀錯版本 | 比對 CLI log、training manifest 與 `k_values` |
| segment source table 無法讀取 | table 名稱、database 或權限錯誤 | 用 Spark/Hive 確認表存在且可讀 |
| segment source missing columns | `key_columns` 或 `segment_column` 拼錯 | 比對來源 schema；每個 source 必須提供全部欄位 |
| per-segment section 沒出現 | `per_segment` section 關閉，或目標欄位不是第一個 active segment | 檢查 `report.sections.per_segment`、enriched schema 與 `segment_columns` 順序 |
| product category unknown product | mapping 引用了未宣告 item | 對齊 `schema.categorical_values[item]` |
| category 結果不符合預期 | item 重複映射或 max-child 語意不適合業務 | 確認每個 item 只屬於一類，重新檢視 category 定義 |
| baseline warning `falling back to full label_table` | lookback window 沒有歷史資料 | 補歷史 labels 或調整期間；不要直接採信可能 leakage 的 baseline |
| `--compare` key 不存在 | CLI key 不在 `compare_sources` | 檢查 YAML key 與錯誤訊息列出的 available keys |
| compare source 沒有該日期資料 | Model B source、model version 或日期不一致 | 查來源 table 的 model/date partitions |
| external item unmapped | `prod_mapping` 未涵蓋外部 item | 補 mapping；確認可接受時才使用 `unmapped_policy: drop` |
| common customers 或 items 為空 | 雙方日期、ID 型別或 item mapping 不一致 | 對齊日期與型別，修正 mapping |
| 比較 coverage 大幅縮水 | 候選母體、客戶母體或外部 mapping 不一致 | 檢查 comparison report coverage 與 dropped items |
| B4 enriched partition missing | 直接使用 `--compare-only`，但 Model A 尚未標準評估 | 先執行標準 evaluation 或 `--compare` |
| compare-only 讀到錯誤情境 | 同 model/date enriched partition 曾被另一種模式覆寫 | 以正確 `--post-training` 狀態重跑標準 evaluation |
| diagnostics 過慢 | 多次 Spark aggregation、items 或 bins 過多 | 關閉不需要的 diagnostics sections 或縮減 K／items |
| `Unknown node` | node 名稱拼錯或 compare mode 的 DAG 不同 | 先用相同模式執行 `--list-nodes` |
| 部分重跑出現大量 auto-included | 所需中間產物為 memory-only | 接受完整 metric 重算，或改用 `--compare-only` |

## 9. 限制與注意事項

- evaluation 沒有獨立版本 hash；同 model/date 的設定變更會覆寫既有報表與 enriched data。
- post-training 與 monitoring 共用同一 enriched partition 與報表路徑，無法同時保留兩種情境。
- 目前 per-segment metrics 只使用 `segment_columns` 中第一個存在的欄位，不會在單次 run 中分別計算多個 segment dimensions。
- comparison 目前以 `schema.entity` 的第一個欄位作為 customer 交集；複合 entity schema 需確認比較語意。
- `external_hive.columns` 的角色 key 目前固定為 `cust_id`、`snap_date`、`prod_name` 與 `score`，尚未完全依 schema 角色動態解析。
- comparison 先取 entity 集合與 item 集合的交集，但不會補齊雙方缺少的 `(entity, item)` rows。若候選 coverage 不對稱，即使 entity/item 集合相同，評估母體仍可能不完全一致。
- Model B 已帶 label 時會沿用來源 label，不會強制以目前 `label_table` 覆寫；跨時間產生的 enriched／training sources 必須確認 ground truth snapshot 一致。
- score 相同時的排名 tie-break 沒有額外穩定鍵，Spark `row_number` 對同分 rows 的相對次序未定義。
- zero-positive query groups 會排除於排序指標，因此報表不代表完整 inference entity 母體。
- popularity baseline 在 lookback 空窗時會 fallback 至完整 label table，可能產生 leakage。
- product category 同 item 重複映射目前不會報錯，後出現的 category 會覆蓋前者。
- 比較報表呈現指標差異但沒有 bootstrap、confidence interval 或顯著性檢定。
- evaluation metrics 沒有獨立 JSON／table sink，無法直接形成長期監控時序。
- HTML 由 driver 組裝；雖然 row-level 計算留在 Spark，過多 items、segments、K 或 Plotly 圖表仍會增加報表大小與 driver 負擔。
- 報表沒有自動 pass/fail threshold。模型是否 promotion 仍需由使用者依整體、per-item、per-segment、baseline 與業務限制人工判斷。

## 10. 相關文件

- 模型訓練與 test predictions：[`training.md`](training.md)
- 正式批次推論與發布閘門：[`inference.md`](inference.md)
- 上游 label 與來源表：[`source_etl.md`](source_etl.md)
- Dataset test 母體與 zero-positive query filtering：[`dataset.md`](dataset.md)
- 指標定義與報表解讀：[`../metrics/metrics.html`](../metrics/metrics.html)
- 診斷產物判讀（metric_ci CI 欄、對帳 Reconciliation）：[`evaluation-diagnosis.md`](evaluation-diagnosis.md)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、恢復與人工卡控設計背景：[`../design-principles.md`](../design-principles.md)
