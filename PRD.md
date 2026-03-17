# 最佳產品推薦排序模型

### 專案目標

- 建立彈性的的machine learning pipeline，讓我可以快速迭代實驗各種特徵組合、抽樣策略、模型架構，與重排策略：
  - 推論目標：客戶對商業銀行多個產品的興趣分數。
  - 推論應用：行銷PM可以利用模型結果優先行銷客戶興趣分數較高的產品。
  - 推論頻率：可以每週批次執行推論，並且不定期手動執行模型訓練。
- 產品清單如下(共22類)：

  | 產品大類  | 大類英文名稱      | 產品中類  | 中類英文名稱         |
  | ----- | ----------- | ----- | -------------- |
  | 信用卡利收 | ccard       | 帳單分期  | ccard_bill     |
  |       |             | 單筆分期  | ccard_ins      |
  |       |             | 預借現金  | ccard_cash     |
  | 信貸    | ploan       | 信貸    | ploan          |
  | 房貸    | mloan       | 新貸    | mloan_new      |
  |       |             | 增貸    | mloan_increase |
  |       |             | 轉貸    | mloan_transfer |
  | 台幣定存  | detime_twd  | 台幣定存  | detime_twd     |
  | 外幣定存  | detime_ntwd | 美金定存  | detime_usd     |
  |       |             | 非美金定存 | detime_fx      |
  | 換匯    | exchange    | 美金    | exchange_fx    |
  |       |             | 非美外幣  | exchange_usd   |
  | 基金    | fund        | 股票型   | fund_stock     |
  |       |             | 債券型   | fund_bond      |
  |       |             | 其他    | fund_mix       |
  | 奈米投   | nmi         | 奈米投   | nmi            |
  | 進階投資  | invest      | 海外股   | overa_stock    |
  |       |             | 海外債   | overa_bond     |
  |       |             | 結構型   | derivative     |
  | 保險    | insur       | 高保障   | insur_high     |
  |       |             | 儲蓄型   | insur_saving   |
  |       |             | 投資型   | insur_invest   |

- 模型架構目前有4種策略：
  - 單一階段
    - 策略1 - 單一模型：將多個產品的分數視為兩分類問題，label為binary，產品名稱則作為feature。
    - 策略2 - 多個OVR(one-versus-rest)模型：每一個產品建立一個兩分類模型，依據個別模型的輸出機率做排序。
  - 兩階段
    - 策略3 - 單層排序：上述策略1 or 策略2 後，疊加單層排序模型(e.g. lambdaranker)，第一階段的輸出作為第二階段的額外輸入使用。
    - 策略4 - 雙層排序：上述策略1 or 策略2 後，疊加兩層排序模型，先排序大類，再排序中類。

### 技術要求

- 套件清單及版本要求
  - python 3.10+（目標 3.10，向前相容 3.11/3.12）
  - pyspark == 3.3.2
  - pyarrow==14.0.1
  - numpy == 1.25.0
  - pandas == 1.5.3
  - scikit-learn == 1.5.0
  - lightgbm == 4.6.0
  - mlflow == 3.1.0
  - optuna == 4.5.0
  - plotly == 5.17.0
  - shap == 0.42.1
  - typer == 0.20.1
  - pytest == 7.3.1
  - ploomber == 0.23.3
  - PyYAML == 6.0
  - joblib == 1.2.0
- 目前是開發環境，將移轉到正式環境，正式環境條件如下：
  - Spark 版本：3.3.2.3.3.7191000.10-1
  - Hive 版本：3.1.3000.7.1.9.1042-1
  - DAG定義工具：Ploomber
  - 儲存：HDFS
  - Table format：Parquet
  - 限制事項：不允許 UDF、不能連網、不能裝額外套件
  - CPU 4 core, RAM 128GB。
- 正式環境依賴相關
  - 正式環境依賴於HIVE資料庫，開發環境將以假資料代替。
  - 假資料的資料規格請檢視@XXXX.md。
- 資料規模：
  - 訓練：行內約1000萬名客戶，共計22類產品，特徵數量預計在500個左右，且希望取12個月的月底資料作為訓練資料。
  - 推論：行內約1000萬名客戶，共計22類產品，特徵數量預計在500個左右。希望每週執行推論時不超過3小時就能推論完畢。

### 核心功能描述

- 預計區分4大pipeline，如附圖(PRD-pipeline-image.png)所示：
  - **SOURCE DATA ETL PIPELINE：**
    - 主要目的：
      - 對來源資料表進行資料轉換，最終會產出一張儲存所有feature的HIVE table，以及一張儲存所有label的HIVE table。
    - 注意事項：
      - 資料轉換過程會有多個SQL file，一個SQL檔案對應一張table。
      - 每張table，原則上都會有partition，一般情況下會是snap_date。
      - SQL的檔案數及執行順序需要可依使用者設定，目前想法是使用者定義在.yaml檔中。
      - 原則上在這階段資料都儲存為HIVE table，並以pyspark實作。
      - 來源資料驗證基本上是資料表新鮮度檢視，若有來源資料表未更新的情況（依該表的資料日、資料鍵值，或是資料筆數判斷）。
      - 一般的資料驗證則會檢視是否有資料重複問題、空值佔比、0值佔比、最大最小值檢視、類別數檢視等。
  - **DATASET BUILDING PIPELINE**
    - 主要目的：
      - 對上一步產出的feature table以及label table，進行模型訓練相關的資料處理過程，包含抽樣、資料集切分、特徵工程處理，最終產出模型訓練及評估需要用到的所有資料集。
    - 注意事項：
      - 抽樣方式會需要彈性依據多個欄位做分層抽樣（例如 snap_date、cust_segment_typ），分層抽樣的 group by 欄位由 YAML 設定檔定義，抽樣比例可以客製化設定。
      - 資料集切分為三組，各組的 snap_dates 互不重疊：
        - **train**：時間內，有抽樣。用於模型訓練。
        - **train-dev**：時間外，有抽樣。用於訓練過程中的驗證（early stopping、超參數搜尋）。
        - **val**：時間外，未抽樣（全量）。用於最終模型評估。
      - 特徵工程的 category_mappings 需另存為 JSON 檔案供檢視，提升可觀測性。
      - 原則上在正式環境，這階段資料都儲存為HIVE table，並以pyspark實作。開發環境則以pandas處理本地Parquet檔案。
      - 部分程式組件以及產物會需要在其他情境複用，例如推論管線中，資料轉換的邏輯需相同且不能造成資料洩漏問題。
  - **TRAINING PIPELINE**
    - 主要目的：
      - 讓我能快速執行各種模型實驗，找出較好的模型。利用超參數搜尋找到最佳超參數組合產出模型。對模型推論結果（可能有推論結果後處理）進行評估，並且比較多種模型，選出最好的模型註冊。當中包含人工以notebook進行的錯誤分析。
    - 注意事項：
      - 需要有實驗紀錄功能，實驗紀錄需包含模型檔、設定檔、特徵清單等資料，也必須包含特徵重要性清單、 模型衡量指標結果、相關圖表等。
      - 推論結果後處理包含：機率校準，或是規則化的重新排序機制。
      - 模型評估使用的模型衡量指標：mAP(mean average precision)、precision@K、recall@K、nDCG、MRR，且依據整體、產品個別、依自定義客群區分。評估的資料來源包含train資料集，與validation資料集（時間外的全量資料）。
  - **INFERENCE PIPELINE**
    - 主要目的：
      - 每週定期執行推論，產出資料表供下游使用。每月初則依據上個月模型的實際結果，計算相關監控指標。
    - 注意事項：
      - 資料產出過程基本上會復用 **SOURCE DATA ETL PIPELINE & DATASET BUILDING PIPELINE** 的部分功能。
      - 推論結果寫回HIVE table，會依snap_date & prod_code 建立partition。
      - 監控指標包含各產品機率值分布是否正常、資料筆數是否正確（例如因為客戶數理論上只會越來越多，所以資料筆數應該要比上一次推論還多）
      - 監控指標一樣需寫HIVE table，雖不用分partition，但若監控指標有異動需保留歷史紀錄。

## 設計原則

- Kedro Design Philosophy
- 設計要求提醒：
  - safe rerun：當執行管線意外中斷，要能夠可以safe rerun。若可行的話，盡量要可跳過已經做過的步驟，從失敗的步驟接續執行。
  - 可觀測性：包含解析後的SQL檔、資料量追蹤、設定檔snapshot、執行日誌、步驟計時、推論結果摘要(分產品機率值的 min / max / mean / std / 四分位數)、Spark 設定記錄等。
  - 單元測試：以小樣本資料測試各模組的各功能是否正常運作。

## 開發規劃指引

- 不要一次要求做完所有功能。建議：
  - **先跑起來** — 最小可用版本（MVP）
  - **逐步加功能** — 一次一個功能點
  - **邊做邊測** — 每加完一個功能就測試確認
  - 模型架構先以策略1優先開發、模型評估先算mAP就好、錯誤分析的template notebook先跳過。

## 開發進度追蹤

### 已完成 ✅

| 功能 | 說明 |
|------|------|
| Kedro-inspired 框架 | Node、Pipeline、Runner、Catalog、ConfigLoader |
| I/O 抽象層 | ParquetDataset（雙後端）、PickleDataset、JSONDataset |
| Dataset Building Pipeline | 分層抽樣、train/train-dev/val 切分、特徵工程、雙後端 |
| Training Pipeline | Optuna 超參搜尋、LightGBM 訓練、mAP 評估、MLflow 追蹤、版本比較 |
| Inference Pipeline | 批次打分、preprocessor 複用、排序、雙後端 |
| 版本管理 | Hash-based dataset/model versioning、manifest JSON、symlink（latest/best） |
| 模型晉升 | `scripts/promote_model.py`（手動觸發） |
| CLI | Typer 入口，支援 --pipeline、--env、--dataset-version |
| Strategy 1 | 單一二分類器 + mAP 評估 |
| 測試 | 完整單元測試覆蓋 |

### 待完成 ⬚

| 功能 | 說明 |
|------|------|
| Source Data ETL Pipeline | SQL 轉換、Hive 整合、資料驗證 |
| 進階評估指標 | precision@K、recall@K、nDCG、MRR |
| 指標切面 | 依整體、產品個別、自定義客群分群 |
| 機率校準 | probability calibration |
| 規則化重新排序 | rule-based reranking |
| 月度監控 | 機率值分佈監控、資料筆數檢查 |
| Safe rerun 檢查點 | 跳過已完成步驟 |
| Strategy 2 | One-vs-Rest 多模型 |
| Strategy 3 | 疊加單層排序（LambdaRank） |
| Strategy 4 | 疊加雙層排序（大類 → 中類） |
| 錯誤分析 notebook | template notebook |
