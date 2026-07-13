# recsys_tfb — 排序問題批次建模框架，以銀行產品推薦為示例

## 0. 這是什麼

`recsys_tfb` 是一套處理**批次排序問題**的建模框架。框架會針對每個查詢群組（query group），預測各候選 `item` 的分數，並依分數由高到低產生排序結果，供下游在有限資源下決定優先處理順序。

本文件以**銀行產品推薦**為例，但框架不限定於此應用。當行銷資源不足以對每位客戶推廣所有產品時，可將問題轉換為排序問題：對每位客戶的候選產品評分與排序，讓 PM 依名次決定優先聯繫哪些客戶、推薦哪些產品。相同設計也可應用於商品、內容、商機或其他候選項目的批次排序。

### 資料模型

框架的基本資料顆粒度為 `time` × `entity` × `item`。其中，一個 query group 由 `time` 與 `entity` 組成，框架會比較該群組內所有候選 `item` 的 `score`，再產生 `rank`。

> `time`、`entity`、`item`、`label`、`score` 與 `rank` 在實際資料表中的欄位名稱，可於 `conf/base/parameters.yaml` 的 `schema` 區塊設定。

| schema 角色 | 意義 | 銀行產品推薦示例 |
|---|---|---|
| `time` | 時間切點 | `snap_date`，快照日 |
| `entity` | 擁有一組候選項目的對象 | `cust_id`，客戶 |
| `item` | query group 內要被排序的候選項目 | `prod_name`，金融產品 |
| `label` | 目標事件是否發生，通常為 0 或 1 | 客戶是否承作該產品 |
| `score` | 模型對候選項目產生的分數 | 產品推薦分數 |
| `rank` | `item` 在 query group 內的名次 | 產品推薦順位 |
| query group | 一次排名與評估的範圍 | 同一快照日的同一位客戶 |

### 輸入與輸出

**輸入** —— 三張由 `source ETL` 維護的 Hive 來源表。下表以 schema 角色表示主鍵；完整欄位與範例資料見 [`docs/data-lineage.html`](docs/data-lineage.html)。

| 來源表 | 內容 | 主鍵（角色） |
|---|---|---|
| `feature_table` | 每位客戶在每個快照日的特徵寬表 | `time, entity` |
| `label_table` | 客戶是否承作某產品的 ground truth（`label` 0/1） | `time, entity, item` |
| `sample_pool` | 要納入建模與排名的候選範圍，並可帶有供分層抽樣使用的欄位 | `time, entity, item` |

> 上列三張是**建模**用的來源表。inference 另用一張 `inference_population` 母體表（同由 `source ETL` 維護，主鍵 `time, entity`），定義每個快照日「哪些 entity 該被推論」——對應 training 端的 `sample_pool`，把「誰被推論（membership）」與「他有什麼特徵（`feature_table` enrichment）」分開；缺特徵的母體成員仍會被評分，並以 `feature_present`（in-memory + log，不寫入輸出表）標記。

**訓練輸出** —— training pipeline 會依資料版本與模型設定產生 `model_version`，並在對應版本目錄中保存模型、最佳參數、最佳迭代次數與訓練診斷等產物。框架也會保存 test set 的預測結果 `training_eval_predictions` 與評估指標，供模型比較及上線前審核；訓練完成不會自動將模型發布為 inference 預設版本。

**推論輸出** —— inference pipeline 會使用指定或已核准的 `model_version` 產生版本化排序結果，最終發布至 Hive 表 `ranked_predictions`。每筆資料包含 `time`、`entity`、`item`、`score`、`rank` 與 `model_version`；排序結果必須先通過 `validate_predictions` 的完整性與排名一致性檢查，才會由 `publish_predictions` 發布。

> `score` 表示模型輸出的排序分數；只有在模型或校準流程具有相應定義時，才可解讀為事件發生機率。

### 限制條件

- **引擎**：PySpark 3.3.2，執行於 Hadoop / HDFS / Hive 環境。
- **三條硬限制**：不可用 Spark UDF、無對外網路、不可安裝額外套件。
- **硬體**：純 CPU，4 核心 / 128GB 記憶體。

---

## 1.主要設計理念

### Kedro 風格：pipeline 與 node 設計

框架將推薦系統建模流程拆分為 `source ETL`、`dataset`、`training`、`evaluation` 與 `inference` 五個 pipeline。
其中 `source ETL` 由 SQL 流程驅動，其餘 pipeline 採用 Kedro-inspired（受 Kedro 啟發）的 DAG 設計。

每個 DAG pipeline 由多個職責單一的 node 組成。node 的輸入與輸出在各 pipeline 的 `pipeline.py` 中明確宣告，框架會根據資料依賴關係決定執行順序；資料的讀寫方式、儲存位置與格式則統一設定於 `catalog.yaml`，使**資料處理邏輯與 I/O 解耦**。

未在 `catalog.yaml` 設定的中間結果會以 `MemoryDataset` 暫存，並在最後一個下游 node 使用完畢後釋放；需要跨次執行重用或支援部分重跑的產物，則透過 catalog 持久化。

### Spark 優先的資料處理

資料清理、特徵處理與資料集建置優先使用 Spark 執行，以支援大規模資料處理。進入模型訓練階段後，框架會將 Hive／HDFS 上的資料快取為 driver-local Parquet，再由對應的 `ModelAdapter` 轉換為演算法適用的可重用格式；例如目前的 LightGBM adapter 會產生 `.bin`。這可減少重複掃描 HDFS、資料轉換及演算法前處理的成本，同時保留擴充其他模型演算法的空間。

### 版本化設計

框架依據 `parameters.yaml`、`parameters_*.yaml` 中會影響產物的設定內容，以及各層上游版本的關聯，計算 8 碼 hash，讓資料集、前處理器、模型與預測結果能互相對齊，也讓不同抽樣或模型實驗可以並存。純執行環境、logging 或監控類設定不會改變產物版本。

- `base_dataset_version`：由資料日期範圍、前處理設定、schema，以及 `feature_table` 的欄位名稱、型別與順序決定；對應前處理器、共用特徵表與 val/test 資料。
- `train_variant_id`：由 train 的抽樣比例、分層 override 與 `train_dev_ratio` 決定；對應 train/train_dev 資料。
- `calibration_variant_id`：啟用機率校準時，由 calibration 的抽樣設定決定。
- `model_version`：由上述資料版本及會影響模型的 training 設定決定，包含演算法參數、HPO、特徵選擇、機率校準與樣本權重等。

相同輸入設定會得到相同版本；只調整 train 抽樣時，不必重建不受抽樣影響的前處理器與 val/test 資料。`latest` 代表最近成功產生的資料版本，`best` 則代表經人工核准、供 inference 預設使用的模型版本。

### 保留人工決策關卡

框架提供 profiling、建議值與一致性檢查，但下列會影響模型語意或上線結果的決策仍由使用者審核：

- **類別欄位判定**：`scripts/suggest_categorical_cols.py` 依資料型別與 cardinality 提出候選欄位，再由使用者確認哪些欄位應採 categorical encoding。
- **下採樣與樣本權重**：`scripts/sampling_overrides_editor.py` 根據各分群的正負樣本分布產生建議與 YAML；使用者再依運算成本、資料不平衡與業務目標決定抽樣比例和權重。
- **模型上線**：training 只產生版本化模型，不會自動發布。使用者檢視評估指標與診斷結果後，透過 `scripts/promote_model.py` 將核准版本設為 `best`，inference 才會預設使用該版本。

人工決策寫入設定後，pipeline 仍會執行 schema、資料一致性與預測結果檢查，避免設定和實際資料不一致，或未通過驗證的結果直接發布。

### Fail-fast

框架將檢查設在高成本處理與資料發布之前，發現不一致時立即中止，避免錯誤產物流入下游：

- **設定一致性**：CLI 啟動時由 `consistency.py` 集中檢查跨 YAML 設定，包含欄位角色衝突、item／產品集合、ranking objective 與 metric、HPO search space、抽樣權重及 evaluation source 等關聯；一次列出所有問題後停止執行。
- **設定與資料一致性**：`dataset` 的第一個 node 會比對設定宣告的 item 集合與 `sample_pool`／`label_table` 實際資料，並檢查 categorical 欄位的實際型別，再進入抽樣與前處理。
- **pipeline 產物品質**：`source ETL` 可先以 preflight 檢查上游 partition、schema 與資料量，寫入後再驗證必要欄位、重複鍵比例、NULL 比例與列數；inference 也必須通過筆數、完整性、分數與排名檢查後才能發布結果。

### 可恢復執行

`source ETL` 支援稽核紀錄及 `--restart-from`；其他 DAG pipeline 可搭配持久化產物，透過 `--from-node` 從指定 node 續跑，或以 `--only-node` 執行單一 node。
training 的 HPO 另有 checkpoint 機制，執行中斷後可沿用既有 Optuna study 與最佳模型，只補跑尚未完成的 trials，降低長流程失敗後的重算成本。

---

## 2.主要功能

### source ETL pipeline

透過 SQL 將上游資料整理成建模用的三張來源表：`feature_table`、`label_table`、`sample_pool`，以及 inference 的評分母體 `inference_population`。

- 建模三張表分別提供 `feature_etl`、`label_etl`、`sample_pool_etl`；inference 母體由 `inference_population_etl` 產出（主鍵 `time, entity`，對應 training 的 `sample_pool`）。
- 使用者可自行定義 SQL 與中介表，框架依 YAML 中的 `tables` 順序逐一執行；`depends_on` 會驗證相依表是否已排在上游。
- SQL 支援 `${target_date}` 等變數，可透過 `--target-dates` 一次處理多個日期。
- 支援 `--source-check` 唯讀 preflight，在寫入前檢查上游 partition 是否存在、資料筆數及 schema 是否符合預期。
- 每張輸出表可設定 primary key 與品質檢查，目前支援最少筆數、重複鍵比例、NULL 比例及必要欄位檢查。
- 支援多欄位 partition，並以 `INSERT OVERWRITE` 覆寫指定日期的 partition，方便安全重跑。
- 流程失敗後可透過 `--restart-from` 從指定中介表繼續執行，略過已完成步驟。
- 支援 dry-run 與 rendered SQL 輸出，可在正式寫入前檢查實際執行的 SQL。
- 首次執行會自動建立 Hive table；新增輸出欄位時可自動執行 schema evolution，移除既有欄位則會阻擋並提示版本化重建。

### dataset pipeline

將 `sample_pool` 依照各資料集的日期範圍與抽樣設定，產出並持久化 `train_keys`、`train_dev_keys`、`val_keys`、`test_keys`，以及啟用機率校準時的 `calibration_keys`。

前處理器只使用 `train_snap_dates` 範圍內的 `feature_table` 建立，再套用至所有資料區間，產出共用的 `preprocessed_feature_table`。最後，各組 `*_keys` 依 `time + entity` 連接特徵，並依 `time + entity + item` 連接 `label_table`，產出 `train_model_input`、`train_dev_model_input`、`val_model_input`、`test_model_input`，以及可選的 `calibration_model_input`，供後續模型訓練、校準與評估使用。

- **資料一致性閘門**：在抽樣與前處理前，先檢查 item 集合是否與設定一致，並防止連續數值欄位被誤設為 categorical。
- **決定性分層抽樣**：可在 `parameters_dataset.yaml` 設定 `sample_group_keys`、預設抽樣比例與各分層 override；抽樣由 identity key、使用場景與 random seed 計算固定 hash，因此相同輸入可重現相同結果。
- **互斥的 train/train_dev 切分**：抽樣後以 entity 為單位進行決定性切分，同一 entity 的所有 item 只會出現在其中一側，避免 train 與 early stopping 資料彼此重疊。
- **互動式抽樣設定**：`scripts/sampling_overrides_editor.py` 會分析各分層的正負樣本分布，產出 `data/profiling/sampling_overrides_editor.html`，供使用者互動式調整下採樣比例，並匯出可寫入設定檔的 YAML。
- **類別欄位建議**：`scripts/suggest_categorical_cols.py` 依欄位型別與 cardinality 產出 categorical 候選清單，再由使用者確認是否納入 encoding。
- **避免前處理資料洩漏**：preprocessor 僅從訓練期間 fit，內容包含 `feature_columns`、`categorical_columns`、category encoding 對照與 `drop_columns`；訓練與推論共用相同 metadata，確保欄位順序與編碼一致。
- **建立完整模型輸入**：`label_table` 未匹配到的候選項目視為負例 (`label = 0`)；數值特徵統一轉為 float32，以降低後續 driver 端訓練的記憶體用量。
- **移除無法評估的 query group**：僅針對 `val_model_input` 與 `test_model_input`，移除同一個 `(time, entity)` 下所有 item 的 label 皆為 0 的群組。這類群組沒有正例，無法衡量正例是否被排到前面，對 mAP、NDCG 等排序指標沒有貢獻；train、train_dev 與 calibration 則保留全部樣本。

### training pipeline

讀取 dataset pipeline 產出的 `train_model_input`、`train_dev_model_input`、`val_model_input`、`test_model_input`，以及啟用機率校準時的 `calibration_model_input`，依指定的資料版本訓練一個供所有 item 共用的模型。流程會先將各 split 快取至 driver，再進行模型格式轉換、超參數搜尋、最終模型產生、可選的機率校準，最後對 test set 產生預測、計算排序指標並輸出模型診斷。

- **可擴充的模型介面**：訓練流程透過 `ModelAdapter` 封裝演算法差異，目前提供 LightGBM adapter，支援 pointwise `binary` 與 learning-to-rank `lambdarank`／`rank_xendcg` objective；不論使用哪種訓練目標，模型仍以 query group 內的排序表現進行評估。
- **Driver-local 訓練快取**：各 split 會由 Hive／HDFS 複製為 driver-local Parquet，再由 adapter 轉換為演算法適用的可重用格式；例如 LightGBM 會建立 `.bin`，避免每次 HPO trial 重複掃描 Hive、轉換資料與分箱。
- **訓練階段特徵選擇**：可透過 `training.feature_selection.exclude` 排除不使用的特徵，不需重建 dataset；HPO、最終訓練、校準、test 預測與 inference 會共用同一份特徵清單，避免訓練與推論欄位不一致。
- **可設定的樣本權重**：可依 item、客群或其他帶入 model input 的欄位組合設定權重，且只作用於 train／train_dev；框架會產生套用報告，列出未匹配的設定，避免權重設定錯誤卻無聲失效。
- **超參數搜尋與資料集職責分離**：Optuna HPO 使用 train 訓練、train_dev 執行 early stopping，並以 val 的排序指標選擇最佳超參數；`hpo_objective` 可選擇整體 query mAP 或各 item 等權重的 macro mAP。
- **HPO 崩潰恢復**：啟用 `hpo_checkpointing` 時會保存 Optuna study 與目前最佳模型，訓練中斷後可只補跑剩餘 trials；若要放棄既有搜尋結果，可使用 `--fresh-hpo` 從頭開始。
- **最終模型與機率校準**：HPO 完成後可直接沿用最佳 trial 模型，或以最佳參數在 train + train_dev 上重新訓練；若下游需要將 score 解讀為機率，可選擇使用獨立 calibration split 執行 sigmoid 或 isotonic calibration。
- **測試評估與模型診斷**：最終模型會對 test set 產生 `training_eval_predictions`，計算整體 mAP 與 per-item mAP attribution，並可輸出特徵統計、feature importance 與 SHAP 診斷（含 per-item 帶方向的特徵 profile、採購者對照與跨 item 偏離度 `item_idiosyncrasy`、象限（TP/FP/FN/TN）per-(item×象限) 聚合 profile 與極值案例 SHAP 圖）；模型、參數、指標與診斷也可記錄至 MLflow。
- **版本化但不自動上線**：模型與其上游 `base_dataset_version`、`train_variant_id`、可選的 `calibration_variant_id` 及有效 training 設定共同決定 `model_version`。training 完成後不會自動供 inference 使用，仍須人工檢視評估結果並透過 `scripts/promote_model.py` 將核准版本設為 `best`。

### evaluation pipeline

將指定 `snap_date` 的模型預測與 ground truth 整理成 `eval_predictions`，以 `(time, entity)` 為 query group 計算排序指標，並產出可互動檢視的 HTML 報表。
evaluation 可用於訓練完成後的 test set 評估，也可在模型上線、label 觀察窗結束後，定期監控 inference 已發布的排序結果。

| 資料角色 | 來源 | 用途 |
|---|---|---|
| 模型預測 | `training_eval_predictions`（`--post-training`）或 `ranked_predictions`（預設） | 分別用於上線前 test set 評估與上線後監控 |
| Ground truth | `label_table`；post-training 模式沿用 `training_eval_predictions` 已保存的 test label | 判定每個 `(time, entity, item)` 是否為正例 |
| 分群欄位 | `segment_sources` 指定的 Hive table（監控模式建議指向 `inference_population`、post-training 指向 `sample_pool`） | 補入客群等欄位，計算 per-segment 指標 |
| Popularity baseline | `label_table` 的歷史觀察窗 | 依 item 歷史正例數建立熱門度基準排序 |
| 比較來源 | 其他 `model_version` 的預測或 `compare_sources` 指定的外部 Hive table | 產生模型間或跨系統比較報表 |

- **兩種評估情境**：`--post-training` 讀取 training pipeline 產生的 `training_eval_predictions`，用於模型上線前的 test set 評估；預設模式讀取 inference 發布的 `ranked_predictions`，用於模型上線後的定期監控。
  監控情境須等該期 label 觀察窗結束並補齊 ground truth 後執行。
- **以排序指標為核心**：不論模型使用 pointwise 或 learning-to-rank objective，評估都以 query group 內的相對排序為準，依 `k_values` 計算 mAP、NDCG、precision 與 recall 等 @K 指標，而非逐筆分類準確率。
  沒有任何正例的 query group 會從指標計算中排除，並在報表中記錄排除數量。
- **多層次指標拆解**：除整體指標外，也會計算 per-item attribution、macro average 及資料集概況，協助辨識整體表現是否由少數熱門 item 主導，而非只看單一平均值。
- **分群評估**：可透過 `segment_columns` 與 `segment_sources` 將客群或其他外部分群欄位連接至評估資料，觀察不同族群的排序品質；來源表不存在、必要欄位缺失或設定未提供對應來源時會 fail-fast。
- **產品大類評估**：可設定 `product_categories`，將細項 item 彙整為產品大類後平行計算同一套指標，同時保留細項與大類兩種視角。
- **Popularity baseline**：依 `baseline.lookback_months` 統計歷史 item 熱門度，建立不使用個人特徵的基準排序，並在報表中呈現模型、baseline 與差異，判斷模型是否真正優於單純推薦熱門項目。
- **模型與外部結果比較**：可透過 `compare_sources` 比較另一個 `model_version` 或外部 Hive 預測表。
  框架會先限制在雙方共同的客戶與 item 範圍後重新排名與評估，並產出 `report_comparison.html`，避免因評估母體不同造成不公平比較。
- **可重用的評估結果**：標準流程會將已連接 label、rank 與 segment 的資料持久化為 `enriched_eval_predictions`；後續可用 `--compare-only` 直接產生比較報表，不需重新執行完整指標與 baseline 計算。
- **Spark 原生計算與診斷報表**：逐筆 join、排名與指標聚合皆在 Spark 執行，只將聚合後的小型結果交給報表層。
  報表可包含分數分布、排名分布、正例位置與 calibration curve 等診斷，避免將完整預測資料收集到 driver。


---

## 3. 快速上手

本節的目標不是解釋每個 node，而是協助第一次使用框架的資料科學家，將一個新的業務問題轉換成可執行、可評估、可發布的排序流程。以下以「銀行 App 理財專區功能排序」為例：針對每位客戶，排序首頁要呈現的理財功能。

### 先確認問題是否適合

開始修改設定前，先確認問題符合以下條件：

- **批次排序**：在固定時間切點產生排序結果，而不是要求即時逐次互動更新。
- **明確的 query group**：每個 `time + entity` 下有多個候選 `item`，且分數只需要在同一組內比較。
- **可觀察的 label**：能定義快照日之後一段固定觀察窗內的正負結果，例如「未來 7 天是否點擊該功能」。
- **時間正確的特徵**：所有特徵在 `time` 當下已經存在，不能使用觀察窗內或更晚才產生的資訊。
- **固定候選集合**：目前 inference 會將每個 `entity` 與 `inference.products` 的完整 item 清單做 cross join，預設每個對象共享同一組候選 item。

> 若不同客戶可見或可使用的功能不同，例如只有完成風險屬性評估的客戶才能看到基金申購，需先在 inference 的 `build_scoring_dataset` 加入 eligibility filter，不能只靠模型把不適用的功能排到後面。

### 定義排序契約

先用一張表把業務問題映射到框架角色；這份契約應在撰寫 SQL 或調整模型前與需求方確認。

| 決策 | App 理財專區功能排序示例 |
|---|---|
| 排序目的 | 決定每位客戶在理財專區首頁看到的功能順序 |
| `time` | `snap_date`，每日或每週產生排序的快照日 |
| `entity` | `cust_id`，被服務的客戶 |
| `item` | `function_code`，例如 `portfolio_overview`、`fund_search`、`market_news`、`recurring_investment` |
| query group | 同一個 `snap_date + cust_id` 下的所有候選功能 |
| `label` | 快照日後 7 天內是否點擊或進入該功能，0 或 1 |
| 模型輸出 | 每位客戶對各功能的 `score`，以及組內的 `rank` |
| 下游動作 | 取 Top N 決定首頁模組順序 |
| 主要指標 | mAP、NDCG 或 Recall@K，依頁面實際可展示的名額選擇 K |

`label` 代表模型真正會優化的行為。若使用「點擊」作為 label，模型學到的是互動傾向，不等同於申購意願、客戶適合度或預期收益；這些目標需要不同的 label、樣本權重或額外業務規則。

### 設定 schema 與候選 item

在 `conf/base/parameters.yaml` 將固定的 schema 角色對應到新題目的實際欄名，並列出合法的 item 集合：

```yaml
schema:
  columns:
    time: snap_date
    entity: [cust_id]
    item: function_code
    label: label
    score: score
    rank: rank
  categorical_values:
    function_code:
      - portfolio_overview
      - fund_search
      - market_news
      - recurring_investment
```

同一份候選清單也要填入 `conf/base/parameters_inference.yaml` 的 `inference.products`。框架會檢查兩處是否一致，避免訓練與推論使用不同的 item 集合。

### 建立三張來源表

修改 `conf/sql/etl/` 下的 SQL，將原始資料整理成框架規範的三張 Hive 表。資料欄位可以依題目擴充，但 identity key 與資料顆粒度必須符合下列契約：

| 來源表 | App 功能排序內容 | 必要顆粒度 |
|---|---|---|
| `feature_table` | 客戶屬性、資產概況、近期 App 行為、各功能歷史使用次數等；只能使用 `snap_date` 當下已知的資訊 | `time, entity` |
| `label_table` | 每位客戶對每個功能在 label 觀察窗內是否發生目標行為 | `time, entity, item` |
| `sample_pool` | 要納入建模的客戶與候選功能，可附帶客群、活躍度等分層抽樣欄位 | `time, entity, item` |

同時調整 `conf/base/parameters_{feature,label,sample_pool}_etl.yaml` 的 SQL 執行順序、partition、primary key 與品質檢查；inference 另需 `parameters_inference_population_etl.yaml` 產出評分母體（主鍵 `time, entity`，對應 `sample_pool`）。如果更換 Hive database、來源表名或下游產物名稱，再修改 `conf/base/catalog.yaml`。

開始大量回補前，建議先選一個日期驗證：

```bash
# 唯讀檢查上游 partition、欄位與資料量
python -m recsys_tfb feature_etl --env production --source-check --target-dates 2026-01-31
python -m recsys_tfb label_etl --env production --source-check --target-dates 2026-01-31

# 檢查通過後再產出三張來源表
python -m recsys_tfb feature_etl --env production --target-dates 2026-01-31
python -m recsys_tfb label_etl --env production --target-dates 2026-01-31
python -m recsys_tfb sample_pool_etl --env production --target-dates 2026-01-31
```

確認單日資料正確後，再將 `--target-dates` 擴充為 train、calibration、val 與 test 所需的所有日期。source ETL 的完整設定與重跑方式見 [`docs/pipelines/source_etl.md`](docs/pipelines/source_etl.md)。

### 設定資料切分與前處理

在 `conf/base/parameters_dataset.yaml` 完成以下設定：

- `train_snap_dates`、`calibration_snap_dates`、`val_snap_dates`、`test_snap_dates` 必須互斥，並依時間先後切分，避免用未來資料評估過去模型。
- `sample_group_keys` 定義分層抽樣維度，例如 `customer_segment + function_code + label`。
- `carry_columns` 列出後續 sample weight 需要使用、但不屬於 identity 的欄位。
- `prepare_model_input.categorical_columns` 列出類別欄，且必須包含 item 欄位 `function_code`。
- `prepare_model_input.drop_columns` 排除 identity、label、觀察窗日期及不應進入模型的欄位。

類別欄與抽樣比例可先由資料產生建議，再由使用者審核：

```bash
python scripts/suggest_categorical_cols.py <database>.feature_table --max-cardinality 30
# 大表可用 --where 裁分區（Spark 下推省 I/O）或 --sample-fraction 抽樣加速；
# 子集會低估 cardinality（低卡判定僅為下界），詳見 docs/pipelines/dataset.md：
python scripts/suggest_categorical_cols.py <database>.feature_table --where "snap_date >= '2026-06-01'" --sample-fraction 0.1
python scripts/sampling_overrides_editor.py profile <database>.sample_pool
```

互動式抽樣工具匯出設定後，可轉成 `parameters_dataset.yaml` 的 `sample_ratio_overrides` 與 `parameters_training.yaml` 的 `sample_weights`：

```bash
python scripts/sampling_overrides_editor.py to-yaml data/profiling/sampling_overrides_export.json
```

### 設定模型與評估方式

在 `conf/base/parameters_training.yaml` 先建立一個容易解讀的 baseline，再逐步增加複雜度：

- 初版可使用 pointwise `binary` objective；若要直接優化組內排序，再比較 `lambdarank` 或 `rank_xendcg`。
- `hpo_objective: mean_ap` 讓每個 query group 等權；`macro_per_item_map` 則讓每個 item 等權，適合避免熱門功能主導調參結果。
- 只有下游需要將 `score` 當機率使用時才啟用 calibration；dataset 的 `enable_calibration` 與 training 的 `calibration.enabled` 必須一起設定。
- 初次 smoke test 可降低 `n_trials` 與 `num_iterations`，確認資料流正確後再恢復正式搜尋規模。

> 目前 inference 的 `validate_predictions` 會檢查 `score` 是否介於 0 與 1；若使用未校準的 ranking objective，需確認模型輸出符合此契約，或同步調整驗證規則。

在 `conf/base/parameters_evaluation.yaml` 設定符合頁面展示空間的 `k_values`。例如首頁只顯示 3 個功能，就應特別關注 mAP@3、NDCG@3 與 Recall@3，並設定重要客群的 `segment_columns`，避免整體指標掩蓋特定客群的退化。

### 執行第一個端到端版本

指令格式為 `python -m recsys_tfb <pipeline> [選項]`。以下假設執行環境已能連線 Spark／Hive；本機環境建置見 [`docs/operations/local-spark-setup.md`](docs/operations/local-spark-setup.md)。

```bash
# 1. 建立版本化資料集
python -m recsys_tfb dataset --env production

# 2. 訓練模型並產生 test set 預測
python -m recsys_tfb training --env production

# 3. 產生上線前 test set 評估報表
python -m recsys_tfb evaluation --env production --post-training

# 4. 人工審核通過後，將指定模型設為 best
python scripts/promote_model.py <model_version> --dry-run
python scripts/promote_model.py <model_version>

# 5. 設定 parameters_inference.yaml 的 snap_dates 後執行批次排序
python -m recsys_tfb inference --env production

# 6. label 觀察窗結束後，評估已發布的推論結果
python -m recsys_tfb evaluation --env production
```

`dataset` 會產生資料版本，`training` 再將資料版本與模型設定組合成 `model_version`。訓練完成不會自動上線；只有人工核准並設為 `best` 的版本，才會成為 inference 的預設模型。

### 驗收第一版

第一次跑完不應只確認 pipeline 顯示成功，至少還要檢查：

- 三張來源表在 identity key 上沒有重複，且各日期、各 item 的資料量符合預期。
- `feature_table` 沒有使用 label 觀察窗內或未來才會產生的欄位。
- 每個 query group 有足夠的候選 item；val／test 中有正例的 query group 數量足以代表真實使用情境。
- train、calibration、val、test 的日期互斥，且 test 保持為最終 held-out 資料。
- 抽樣與 sample weight 沒有讓冷門功能或重要客群消失，未匹配的 weight key 已被檢視。
- test 報表中的模型指標優於 popularity baseline，且 per-item、per-segment 指標沒有明顯退化。
- `ranked_predictions` 每個 query group 都包含完整候選集合，`rank` 從 1 開始且與 `score` 由高到低一致。
- 隨機抽查實際排序，確認結果符合產品資格、法遵限制與基本業務常識。

需要深入調整時，可依序查閱 [`docs/pipelines/source_etl.md`](docs/pipelines/source_etl.md)、[`dataset.md`](docs/pipelines/dataset.md)、[`training.md`](docs/pipelines/training.md)、[`inference.md`](docs/pipelines/inference.md) 與 [`evaluation.md`](docs/pipelines/evaluation.md)。

---

## 4. 設定檢查與常見錯誤

本節用來回答兩個問題：**執行前如何避免設定錯誤，以及指令失敗後應該先檢查哪裡**。模型與排序概念的選擇則放在下一節 FAQ。

### 執行前檢查

第一次接入新題目，或修改 schema、item、特徵、抽樣設定後，建議依序確認：

1. `entity` 是擁有一組候選項目的對象，`item` 才是 query group 內真正被排序的項目。
2. `feature_table` 的主鍵是 `time + entity`；`label_table` 與 `sample_pool` 的主鍵是 `time + entity + item`，三張表都不應有重複鍵。
3. `sample_pool` 包含所有要比較的候選項目，而不是只保留 `label = 1` 的正例。
4. `schema.categorical_values.<item>`、`inference.products` 與 `sample_pool` 使用相同的 item 集合；`label_table` 可以只包含其中一部分，但不可出現未宣告的 item。
5. 特徵只使用 `time` 當下已知的資訊；label 觀察窗尚未結束的日期不能放進 train、val 或 test。
6. train、calibration、val、test 日期彼此不重疊，並依時間先後排列。
7. item 必須列在 `categorical_columns`，且不可同時出現在 `drop_columns` 或 `training.feature_selection.exclude`。
8. 不同 entity 若有不同候選資格，必須在建立 scoring dataset 時過濾，不可只期待模型將不適用的 item 排到最後。

### 最常見的錯誤

| 常見錯誤 | 可能造成的結果 | 避免與修正方式 |
|---|---|---|
| `sample_pool` 只放曾經點擊、申辦或發生事件的 item | 訓練資料幾乎沒有負例，模型學不到同一 query group 內哪些候選應排後面 | `sample_pool` 應表示當時有資格被排序的候選集合，再由 `label_table` 標記哪些候選成為正例 |
| 把「label 資料尚未到齊」當成 `label = 0` | 大量正例被誤標為負例，離線指標與模型方向失真 | 先確認觀察窗已結束、來源 partition 已到齊；只有「確定沒有發生事件」才能視為 0 |
| 特徵使用快照日之後才產生的欄位 | test 指標異常漂亮，但推論時無法取得相同資訊 | feature SQL 必須採 point-in-time join，排除申請結果、觀察窗行為及事後彙總欄位 |
| item 清單只改了一處 | CLI 被一致性檢查擋下，或某些 item 無法訓練、推論 | 同步修改 `schema.categorical_values`、sample pool SQL 與 `inference.products`；label SQL 不可產出未宣告的 item |
| 日期雖未重疊，但 val／test 早於 train，或 label 尚未成熟 | 產生時間穿越或不完整 ground truth | 明確採用 `train → calibration → val → test` 的時間順序，並為每個日期保留完整 label 觀察窗 |
| 連續數值欄誤放入 `categorical_columns`，或同一欄同時 categorical 與 drop | 編碼語意錯誤、前處理失敗，或該欄實際未進入模型 | 類別代碼先轉成 string／int；真正的連續數值欄不需列入 `categorical_columns` |
| 手動填寫 `sample_ratio_overrides` 或 `sample_weights`，但 key 與資料不一致 | 抽樣或權重規則沒有套用，冷門 item／重要客群可能消失 | 使用 `sampling_overrides_editor.py` 產生 key，並檢查 training manifest 中的 unmatched keys |
| 新的 weight 維度沒有放入 `carry_columns` | training 讀不到原始分群欄，weight 靜默落回 1.0 或被設定閘擋下 | 將欄位加入 `parameters_dataset.yaml` 的 `carry_columns`，重跑 dataset |
| 使用 ranking objective，卻沿用 `binary_logloss` 或直接把 score 當機率 | early stopping 指標語意錯誤，或 inference 的 score range 檢查失敗 | ranking objective 搭配 `ndcg`／`map`；需要機率時使用 calibration，並確認 inference 的 score 契約 |
| evaluation 用錯模式或日期 | 報表為空、讀到錯誤資料集，或使用尚未完成的 ground truth | 訓練後 test 評估使用 `--post-training`；上線後監控使用預設模式，並等待該期 label 觀察窗結束 |
| training 完成後直接執行 inference | inference 找不到 `best`，或仍使用上一版模型 | 先審核評估結果，再以 `promote_model.py <model_version>` 人工發布 |

### 指令失敗時先看哪一層

| 發生時間／錯誤類型 | 代表什麼 | 優先檢查 |
|---|---|---|
| CLI 啟動即出現 `Config consistency check failed` | YAML 之間互相矛盾，尚未讀取實際資料 | item 清單、categorical/drop、objective/metric、sample weight、evaluation source |
| `dataset` 第一個 node 出現 `DataConsistencyError` | 設定與 Hive 實際資料不一致 | `sample_pool`／`label_table` 的 item distinct 值，以及 `feature_table` 欄位型別 |
| source ETL 出現 `Source check FAILED` 或 output check 失敗 | 上游 partition／schema 未就緒，或輸出品質不合格 | 失敗日期、必要欄位、row count、重複鍵與 NULL 比例；`source_checks` 只有帶 `--source-check` 時才會執行 |
| inference 出現 `ValidationError` | 排序結果未通過發布條件，因此不會寫入 production table | 筆數、NULL、重複 identity、候選完整性、score 範圍與 rank 順序 |
| 一般 `Node '<name>' failed` | 單一 pipeline node 的執行期錯誤 | 從 log 找第一個失敗 node，再查該 pipeline 文件的輸入、產物與重跑方式 |

錯誤訊息會列出具體設定路徑與不一致的值，應先修正訊息列出的所有問題，再重新執行。加 item、加特徵、改抽樣或訓練目標時，先參考下方重跑矩陣，再查對應 pipeline 文件的「設定方式」與「版本、重跑與恢復」章節。

### 修改後要重跑哪些流程

| 修改內容 | 建議重跑範圍 |
|---|---|
| source SQL、來源 partition 或同日期資料回補 | 重跑受影響日期的 source ETL，再重跑 dataset 與下游；同 schema 的資料回補不一定會改變版本 hash |
| schema、feature 欄位、categorical/drop、`carry_columns` | `dataset → training → evaluation`，核准後再 inference |
| train／calibration 抽樣比例 | `dataset → training → evaluation` |
| objective、HPO、feature selection、sample weight | `training → evaluation`，不需重建 dataset |
| inference 日期 | 只重跑 inference；上線後 evaluation 要等 label 成熟 |
| evaluation 指標、分群或報表設定 | 只重跑 evaluation；已有 `enriched_eval_predictions` 且只做比較時可使用 `--compare-only` |

---

## 5. FAQ

FAQ 只回答框架概念與選項如何取捨；若是設定無法執行、資料不一致或不知道該重跑哪裡，先看上一節。

**Q1. 這跟我做過的「逐產品二元分類」差在哪？我不也是訓一個輸出機率的模型？**

模型可以還是同一種（預設就是 `binary`），差別在**評估**與**你優化的目標**：

- 二元分類問「這位客戶會不會買產品 A」，逐筆看絕對機率、逐筆算 AUC / logloss。
- 這裡問「對這位客戶，所有候選產品該怎麼**排先後**」，評估一律是 per query group 的排序指標 mAP（mAP 怎麼算見 [`docs/metrics/metrics.html`](docs/metrics/metrics.html)）。
- 你還可以把訓練目標從 `binary` 換成 learning-to-rank（`lambdarank` / `rank_xendcg`），讓模型直接優化排序。

排序與分類的數學差異，見手冊 [`gbdt_learning_to_rank.md`](docs/handbooks/gbdt_learning_to_rank.md)。

**Q2. 為什麼資料要切成 train / train_dev / val / calibration / test 五份？各做什麼？**

| split | 設定（`parameters_dataset.yaml`） | 角色 |
|---|---|---|
| `train` | `train_snap_dates` | 建樹的主訓練資料 |
| `train_dev` | 從 `train` 同期按 `train_dev_ratio` 切出 | early-stopping 監控集：單次訓練內決定樹長到第幾棵就停 |
| `val` | `val_snap_dates` | HPO 目標集：跨多次試驗，使用排序指標選擇最佳超參數 |
| `calibration` | `calibration_snap_dates`（可選） | 機率校準的 fit 資料 |
| `test` | `test_snap_dates` | 最終 held-out，產生 `training_eval_predictions` 供上線前評估 |

各 split 應使用不同且時間向前的快照日，例如 train 2025-01～10 → calibration 2025-11 → val 2025-12 → test 2026-01，避免拿未來資料回頭評估。

**Q3. objective 要選 `binary` 還是 `lambdarank`？**

- `binary` 是建議的第一版 baseline：逐筆預測後再排序，流程較容易驗證，而且仍以排序指標評估。
- `lambdarank`／`rank_xendcg` 適合希望訓練目標直接考慮 query group 內相對順序的情境，但需搭配 `ndcg`／`map` metric，並確認 score 是否需要 calibration。

pointwise、pairwise、listwise 的差異見 [`gbdt_learning_to_rank.md`](docs/handbooks/gbdt_learning_to_rank.md)。

**Q4. 排序只看相對名次，為什麼還要做機率校準？**

純排序不需要校準。只有下游要將 `score` 解讀為機率，例如計算期望收益或跨日期比較絕對水準時才需要。啟用時，dataset 的 `enable_calibration` 與 training 的 `training.calibration.enabled` 必須一起設定。

**Q5. 模型訓練好後怎麼上線？**

用 `scripts/promote_model.py` 將通過人工審核的 `model_version` 設為 `best`；training 不會自動發布模型，inference 預設只使用 `best`。可先加 `--dry-run` 查看候選版本而不實際升版。

**Q6. evaluation 的兩個情境怎麼選？**

| 情境 | 指令 | 資料來源 | 使用時機 |
|---|---|---|---|
| 訓練後評估 | `evaluation --post-training` | test set 的 `training_eval_predictions` | 模型剛訓練完成，進行上線前審核 |
| 上線後監控 | `evaluation` | inference 發布的 `ranked_predictions` | label 觀察窗結束後，追蹤正式排序結果 |

---

## 6. 文件與建議閱讀順序

第一次使用建議先閱讀本文件 §0～§3，完成問題定義與第一版流程；遇到設定問題看 §4，需要選擇模型或評估方式看 §5。其餘文件可依任務查閱：

| 需求 | 建議文件 |
|---|---|
| 查看資料流、各表 schema 與範例 | [`data-lineage.html`](docs/data-lineage.html) |
| 深入某一條 pipeline | [`source_etl.md`](docs/pipelines/source_etl.md)、[`dataset.md`](docs/pipelines/dataset.md)、[`training.md`](docs/pipelines/training.md)、[`inference.md`](docs/pipelines/inference.md)、[`evaluation.md`](docs/pipelines/evaluation.md) |
| 加 item、加特徵或判斷重跑範圍 | 本文件 §4「修改後要重跑哪些流程」，以及對應的 pipeline 文件 |
| 理解 mAP、NDCG、per-item 與報表 | [`metrics.html`](docs/metrics/metrics.html) |
| 理解版本化、一致性檢查與其他設計取捨 | [`design-principles.md`](docs/design-principles.md)、[`behavior-diagrams.html`](docs/behavior-diagrams.html) |
| 從分類基礎學到 learning-to-rank | 依序閱讀 [`binary classification`](docs/handbooks/gbdt_binary_classification.md) → [`class imbalance`](docs/handbooks/gbdt_class_imbalance.md) → [`multi-item imbalance`](docs/handbooks/gbdt_multiitem_imbalance.md) → [`learning-to-rank`](docs/handbooks/gbdt_learning_to_rank.md) |
| 本機執行與 pipeline 接續 | [`local-spark-setup.md`](docs/operations/local-spark-setup.md)、[`pipeline-slicing.md`](docs/operations/pipeline-slicing.md)、[`hpo-resume.md`](docs/operations/hpo-resume.md) |
| 排查訓練 OOM（字串特徵欄 → object 矩陣） | [`training-oom-object-matrix.md`](docs/operations/training-oom-object-matrix.md)、[`known-pitfalls.md`](docs/operations/known-pitfalls.md) |

> 公司生產環境的 Spark／Hive 連線已配置好；只有本機開發或排查連線問題時，才需要閱讀 [`spark-connection-architecture.md`](docs/operations/spark-connection-architecture.md) 與 [`worktree-venv-setup.md`](docs/operations/worktree-venv-setup.md)。概念手冊另提供 `*_offline.html`，可在無網路環境直接開啟。
