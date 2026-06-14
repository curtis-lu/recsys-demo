# 主要設計理念

> 本文件是 README §1「主要設計理念」的詳細版，說明各項理念如何落實到 pipeline、設定、資料產物與執行流程。
> 想直接接入新排序問題請先看 README §3；設定檢查與重跑範圍見 README §4 及各 pipeline 文件；完整資料流見 [`data-lineage.html`](data-lineage.html)。

## 1. Kedro 風格：pipeline 與 node 設計

### 以 pipeline 劃分責任

框架將批次排序流程拆成五個具有明確邊界的 pipeline：

| pipeline | 主要責任 | 主要輸出 |
|---|---|---|
| `source ETL` | 將上游資料整理為框架規範的來源表 | `feature_table`、`label_table`、`sample_pool` |
| `dataset` | 抽樣、時間切分、前處理與模型輸入組裝 | `*_keys`、`preprocessor`、`*_model_input` |
| `training` | 快取資料、HPO、模型訓練、校準與 test 預測 | 版本化模型、診斷、`training_eval_predictions` |
| `evaluation` | 連接 ground truth、計算排序指標與產生報表 | 評估結果、HTML 報表 |
| `inference` | 建立評分母體、套用前處理、預測、排名與發布 | `ranked_predictions` |

`source ETL` 以 SQL 清單驅動，因為其主要工作是處理 Hive 上游表與中介表；其餘 pipeline 採用 DAG，因為處理步驟之間可以用具名資料產物表達依賴關係。

pipeline 邊界同時也是責任邊界。例如 dataset 負責建立並版本化前處理器，training 與 inference 只能重用該前處理器，不應各自重新推導類別編碼；training 只產生候選模型，模型發布則由人工 promotion 與 inference 負責。

### node 只描述一段處理責任

每個 DAG pipeline 在 `src/recsys_tfb/pipelines/<name>/pipeline.py` 中，以 `Node` 明確宣告：

- 執行函式
- 具名輸入
- 具名輸出
- node 名稱

`Pipeline` 依輸入與輸出的名稱建立資料依賴，使用拓撲排序決定執行順序。node 不需要知道前一個 node 是誰，也不應依賴函式呼叫順序或全域狀態。

這個設計帶來三個直接結果：

1. **資料流可讀**：從 `pipeline.py` 即可看出每個步驟消費與產生哪些資料。
2. **函式可獨立測試**：node 函式以輸入參數接收資料，回傳輸出，不必在函式內尋找隱藏來源。
3. **執行可切片**：框架可以根據 node 與資料依賴計算部分重跑所需的最小上游集合。

獨立且沒有相依關係的 node 會依 `pipeline.py` 中的宣告順序執行，因此需要優先執行的守門 node，例如 dataset 的 `validate_data_consistency`，會明確放在 node 清單前方。

### 資料處理邏輯與 I/O 解耦

node 使用資料集名稱，例如 `feature_table`、`preprocessor`、`model`，而不直接處理 Hive table 名稱、檔案路徑或儲存格式。實際 I/O 統一由 `conf/base/catalog.yaml` 與 `DataCatalog` 決定。

同一個 node 因此可以在不修改處理邏輯的情況下，更換：

- Hive database 或 table 名稱
- partition 與版本 filter
- JSON、文字、Parquet 或模型 artifact 路徑
- 開發與正式環境的儲存位置

需要跨次執行重用、稽核或支援部分重跑的產物，必須在 catalog 中明確宣告。未宣告的 node 輸出會自動使用 `MemoryDataset`，只適合單次執行內的中間資料；最後一個下游 node 使用完畢後，runner 會釋放該記憶體資料。

這也形成一條重要規則：**是否加入 catalog 不是單純的 I/O 選擇，而是在宣告產物的生命週期。**

### 設定與 schema 也是公開合約

業務參數、欄位角色與環境設定放在 `conf/base/parameters*.yaml`，不埋在 node 函式內。框架只認得 `time`、`entity`、`item`、`label`、`score` 與 `rank` 等 schema 角色，實際欄名由 `parameters.yaml` 決定。

這讓 pipeline 程式碼不需要硬編 `cust_id` 或 `prod_name`。更換應用問題時，主要修改面應是：

- schema 角色對應
- source ETL SQL
- dataset、training、inference 與 evaluation 參數
- 必要的 catalog table 或路徑

如果新需求只能透過在 node 中硬編欄名、路徑或產品清單完成，通常表示設定或 schema 合約尚未被正確擴充。

## 2. Spark 優先的資料處理

### 依運算特性劃分 Spark 與 driver

框架不是所有工作都使用 Spark，也不是所有資料都收集到 driver，而是依運算特性切分：

| 執行位置 | 適合的工作 |
|---|---|
| Spark | 大表篩選、join、抽樣、類別編碼、排名、聚合、資料品質檢查 |
| driver | HPO、模型演算法原生訓練、機率校準、模型 artifact 與診斷 |

source ETL、dataset、inference 的資料組裝，以及 evaluation 的逐筆 join 與指標聚合，優先使用 Spark SQL／DataFrame。這些步驟不使用 Spark UDF，讓 Catalyst、partition pruning、shuffle 與 spill 機制可以正常作用。

模型訓練則在 driver 執行。目前唯一註冊的演算法實作是 LightGBM，但 training pipeline 不直接依賴 LightGBM API，而是透過 `ModelAdapter` 介面呼叫演算法能力。

### 前處理採 fit／transform 分離

`preprocessor` 只使用 `train_snap_dates` 範圍內的 `feature_table` 建立：

- `feature_columns`
- `categorical_columns`
- category encoding 對照
- `drop_columns`

之後再將相同 metadata 套用到 train、calibration、val、test 與 inference。val、test 或未來資料不參與 encoding dictionary 的建立，避免前處理階段的資料洩漏。

前處理器因此不只是 convenience artifact，而是訓練與推論之間的欄位合約。模型、test 預測與 inference 必須使用同一份 feature 順序與類別編碼。

### 大資料先落成 driver-local cache

training 不會在每個 HPO trial 中重新掃描 Hive。各 split 先從 Hive／HDFS 複製為 driver-local Parquet，pipeline 中只傳遞輕量的 `ParquetHandle`。

`ParquetHandle` 保存資料位置，真正需要時才由下游載入。這可避免：

- 在 node 之間長時間保留大型 pandas DataFrame
- 每個 HPO trial 重複執行 Spark／Hive I/O
- 將大型中間資料透過一般 catalog 格式重複序列化

cache 以 `_SUCCESS` marker 判斷是否完整。若路徑存在但 marker 不存在，會視為未完成 cache 並先清除後重建，避免接續使用半成品。

### 演算法專屬格式由 ModelAdapter 管理

`ModelAdapter` 負責訓練、預測、儲存、載入、feature importance、MLflow logging，以及將通用 Parquet 輸入轉換為演算法適用的格式。

以目前的 LightGBM adapter 為例：

- train 與 train_dev 會轉為可重用的 `.bin`
- train_dev 使用 train 作為 reference，確保分箱一致
- ranking objective 會將 query group 一併寫入 binary
- pointwise 與 ranking 使用不同 cache family，避免格式誤用

新增演算法時，應新增並註冊 adapter，而不是在 training pipeline 中加入大量演算法分支。pipeline 保持演算法無關，演算法特有的資料準備與 artifact 格式則留在 adapter。

### 生產限制反映在架構中

目標環境無對外網路、不可安裝額外套件、不可使用 Spark UDF，並以 CPU 執行。因此：

- 大型資料轉換使用既有 Spark SQL／DataFrame 能力
- 模型與部分 cache 使用 driver-local filesystem
- 報表需能離線開啟
- 依賴版本固定於專案環境，不在 pipeline 執行時動態下載

這些限制不是部署附註，而是 Spark／driver 分工、cache 格式與報表輸出方式的設計輸入。

## 3. 版本化設計

### 依失效範圍拆分版本

若所有 dataset 產物共用單一版本，只調整 train 抽樣比例也會迫使前處理器與 val／test 全部重建。框架因此依「哪些設定會使哪些產物失效」拆成多層版本：

| 版本 | 主要輸入 | 對應產物 |
|---|---|---|
| `base_dataset_version` | 非抽樣 dataset 設定、完整 schema、`feature_table` 欄名／型別／順序 | `preprocessor`、共用特徵表、val／test 資料 |
| `train_variant_id` | train 抽樣比例、override、分層 keys、`train_dev_ratio` | train／train_dev 資料 |
| `calibration_variant_id` | calibration 抽樣比例、override、分層 keys | calibration 資料 |
| `model_version` | 上述資料版本與 model-defining training 設定 | 模型、test 預測、inference／evaluation 結果 |

版本以設定內容的 canonical representation 計算 8 碼 SHA-256 hash。相同版本輸入會得到相同版本 ID，不同實驗可以在相同 Hive table 或 artifact root 下並存。

### 只讓真正影響產物的設定翻版

`model_version` 只納入 `training` 區塊中會定義模型的設定。Spark 資源、MLflow、cache 路徑，以及 `verbosity`、`log_period`、`num_threads` 等執行或觀測設定不會改變 model version。

這個規則採取保守預設：未來若在 `training` 區塊新增設定，預設會納入 hash，避免兩個實際不同的模型共用版本；只有確認不影響模型內容的設定才應明確排除。

### manifest 記錄版本關聯

dataset 與 training 完成後會寫入 `manifest.json`，記錄：

- 本次版本與 pipeline
- 實際使用的參數
- 上游 dataset／variant 版本
- 建立時間與 git commit
- 產物清單與部分執行資訊

training 的 manifest 讓 inference 與 evaluation 可以從 `model_version` 反查正確的 dataset 與 preprocessor，不必依賴當下的 `latest`。

`latest` 與 `best` 的語意刻意不同：

- `latest`：最近成功產生的 dataset 或 variant
- `best`：經人工核准、供 inference 預設使用的模型

「最近完成」不等於「適合上線」，因此 training 不會自動更新 `best`。

### 決定性抽樣支援可重現實驗

dataset 使用 identity key、sampling site 與 random seed 計算固定 hash bucket。相同資料與設定會選出相同樣本，不受 Spark partition 排列或重跑次數影響。

不同 sampling site，例如 train 與 calibration，即使共用 seed 也會使用不同 namespace，避免兩個用途意外取得完全相同的抽樣結果。

### 版本 ID 不代表來源資料內容完全相同

目前版本 hash 不包含：

- source table 的資料值
- source ETL SQL 或 Python 程式碼內容
- 同一 partition 被回補後的資料差異

`base_dataset_version` 會包含 `feature_table` 的欄位名稱、型別與順序，但不包含每一列資料。manifest 中的 git commit 可協助追溯程式版本，不能取代來源資料快照。

因此版本 ID 應解讀為「設定與上游版本關係的身分」，不是資料內容的 cryptographic snapshot。同日期資料回補或程式碼修正後重跑時，仍需確認是否會覆寫相同版本產物。

## 4. 保留人工決策關卡

框架會自動整理資料、產生建議、執行一致性檢查與計算評估指標，但不替使用者做會改變模型語意或上線責任的決策。

### 類別欄位由工具建議、使用者確認

`scripts/suggest_categorical_cols.py` 依資料型別與 cardinality 產生候選清單，但不會直接修改設定。數字代碼可能是類別，也可能是真正的連續變數，僅靠 dtype 或 distinct count 無法可靠判定。

使用者確認後，類別欄位寫入 `parameters_dataset.yaml`。設定一致性與 dataset data gate 再檢查 item 是否保留為類別特徵，以及連續 decimal／double／float 欄位是否被誤標。

### 抽樣與權重由 profiling 輔助、業務目標決定

`scripts/sampling_overrides_editor.py` 顯示各分群的正負樣本量與比例，並可匯出 `sample_ratio_overrides` 與 `sample_weights`。

工具可以回答「資料分布如何」，但不能替使用者決定：

- 可接受的訓練成本
- 冷門 item 應保留多少負例
- 哪些客群或 item 需要提高權重
- 整體 query 表現與 item 公平性之間如何取捨

決策結果寫入 YAML 後會參與 train variant 或 model version，讓人工選擇成為可追溯、可重現的模型輸入。

### 模型發布需要人工 promotion

training 會產生版本化模型、test 預測、排序指標與診斷，但不會直接改變 inference 的預設版本。

使用者應檢查：

- 整體與 @K 排序指標
- per-item 與 per-segment 表現
- popularity baseline 比較
- feature importance、SHAP 與其他診斷
- 產品資格、法遵與業務限制

核准後，再以 `scripts/promote_model.py` 將指定 `model_version` 設為 `best`。這將「產生模型」與「承擔發布決策」分成兩個明確步驟。

### 人工決策不會繞過自動檢查

人工確認後的設定仍需通過 config consistency、data consistency 與 inference validation。保留人工決策代表框架不替人做語意判斷，不代表設定可以跳過技術契約。

## 5. Fail-fast

Fail-fast 的目的不是盡快拋出第一個 exception，而是把錯誤放在最便宜、最接近原因的位置攔截，避免高成本處理完成後才發現產物不可用。

### 分層驗證

| 階段 | 驗證內容 | 失敗結果 |
|---|---|---|
| CLI 設定閘 | 跨 YAML 欄位角色、item 集合、objective／metric、HPO、sample weight、evaluation source | pipeline 啟動前中止 |
| source ETL preflight | 上游 partition、row count、必要欄位與型別 | 唯讀檢查失敗，不寫入 ETL 產物 |
| source ETL output checks | 輸出列數、必要欄位、NULL、重複鍵 | 該表寫入後中止後續 ETL |
| dataset data gate | 宣告 item 與實際 item、categorical 欄位型別 | 抽樣與前處理前中止 |
| inference publication gate | 列數、score 範圍、NULL、候選完整性、rank 一致性、重複 identity | 保留 staging，不發布 production 結果 |

設定閘、dataset data gate、source preflight 與 inference validation 會在各自範圍內收集多個問題後一次回報，讓使用者能在一輪修改中處理所有已知錯誤；檢查完成後才以單一 exception 中止流程。

### 一致性規則集中管理

跨設定與資料的核心不變量集中於 `src/recsys_tfb/core/consistency.py`。同一條規則應只有一個 canonical predicate，再由 CLI、dataset 或測試重用，避免不同 pipeline 各自實作而產生訊息或語意漂移。

目前涵蓋的典型不變量包括：

- item 必須保留為模型特徵
- item 宣告、sample pool 與 inference 候選集合一致
- categorical 與 drop／feature selection 不衝突
- ranking objective 搭配合法 metric 與 query group
- sample weight keys 實際存在於 model input
- evaluation segment 與 compare source 設定完整

### 發布前採 staging／validate／publish

inference 不會直接將排名結果寫入 production `ranked_predictions`。流程先產生 `ranked_staging`，接著執行六項 sanity checks，全部通過後才由 `publish_predictions` 回傳並寫入 production table。

驗證失敗時：

- pipeline 中止
- production table 不會寫入本批結果
- staging 產物保留，供事後排查

這個設計將 fail-fast 從「發生錯誤就停」提升為「錯誤結果無法越過發布邊界」。

### 自動檢查仍有邊界

框架無法自動判斷所有業務與時間語意。例如特徵是否包含未來資訊、label 觀察窗是否真正成熟、候選資格是否符合法規，仍需由 source SQL 設計、資料驗收與人工發布流程負責。

README §4 的執行前檢查列出這些目前不能完全由程式驗證的重要事項。

## 6. 可恢復執行

可恢復執行的核心是：將高成本產物持久化，並讓框架能根據依賴關係判斷哪些步驟可以重用、哪些步驟必須補跑。

### source ETL 依表接續

source ETL 按 YAML 中的 `tables` 順序執行，成功與失敗狀態會寫入 audit log。失敗修正後可使用 `--restart-from <table>`，略過清單中更早且已完成的表。

source ETL 使用 `INSERT OVERWRITE` 覆寫指定 partition，因此相同日期可安全重跑，不會以 append 方式累積重複資料。

`--restart-from` 是依執行順序接續，不會重新證明前面產物仍然新鮮；如果上游資料或前段 SQL 已修改，應從受影響的第一張表開始，必要時完整重跑。

### DAG pipeline 依 node 切片

dataset、training、inference 與 evaluation 支援：

- `--from-node`：執行指定 node 與拓撲序中其後的 nodes
- `--only-node`：只執行指定 node
- `--dry-run`：只顯示切片計畫
- `--list-nodes`：列出可用 node 與接續成本

被跳過 node 的輸出若已在 catalog 中持久化且存在，框架會直接載入；若必要輸入不存在，會遞迴將其 producer 自動加入執行計畫。最壞情況會退化成完整重跑，不會在缺少輸入時靜默執行。

部分重跑有兩個重要限制：

1. 位於切片起點之前、沒有輸出的 side-effect guard 不會自動重跑；資料已變更時應執行完整 pipeline。
2. `exists()` 只能確認產物存在，無法證明未版本化產物與目前參數完全一致；版本化路徑與 manifest 才是主要防線。

詳細行為見 [`operations/pipeline-slicing.md`](operations/pipeline-slicing.md)。

### HPO 以 search checkpoint 接續

pipeline 切片只能在完整 node 邊界恢復。若 `tune_hyperparameters` 執行到一半中斷，則由 HPO checkpoint 機制處理。

啟用 `hpo_checkpointing` 時：

- Optuna study 會持久化於 `data/models/_hpo/<search_id>/`
- 每次產生新的最佳 trial 都會更新模型 checkpoint
- 同一份搜尋設定重跑只補足尚未完成的 trial
- 只提高 `n_trials` 可延長同一個 search
- `--fresh-hpo` 可丟棄既有狀態並重新搜尋

`search_id` 使用與 model version 相同的 model-defining 輸入，但排除 `n_trials`。因此改變 search space、資料版本、objective 或其他會改變 trial 意義的設定時，會自動建立新的 search；只改目標 trial 數則沿用既有 search。

詳細行為見 [`operations/hpo-resume.md`](operations/hpo-resume.md)。

### 恢復能力來自持久化邊界

MemoryDataset 無法跨次執行重用；只有 catalog artifact、training cache、HPO checkpoint、manifest 與 source ETL audit 等持久化狀態能支援恢復。

新增昂貴 node 時，應同時評估：

- 它的輸出是否需要加入 catalog
- 哪些下游接續點依賴該輸出
- 缺少產物時是否能合理自動補跑
- 版本或 manifest 是否足以判斷產物身分

因此「可恢復」不是 runner 的單一功能，而是 node 邊界、catalog、版本化與持久化策略共同形成的能力。

## 修改架構時的判斷順序

新增或修改功能時，可依序確認：

1. 這項功能屬於哪一個 pipeline 的責任？
2. 它能否形成輸入／輸出明確、職責單一的 node？
3. I/O、路徑、欄名或閾值是否應移到 catalog／parameters？
4. 產物只在單次 run 使用，還是需要跨次重用、稽核或接續？
5. 這項設定是否會改變 dataset 或模型內容，因而需要納入版本 hash？
6. 錯誤是否可能產生看似成功但語意錯誤的結果？若會，應在哪個最早階段加入一致性檢查？
7. 這是可自動判定的技術契約，還是必須保留給使用者的業務決策？

這些問題比「把程式放在哪個檔案」更重要，因為它們決定新功能是否仍符合框架原本的可讀性、可重現性與可恢復性。

## 延伸閱讀

- 各 pipeline 的節點、設定與指令：[`pipelines/`](pipelines/)
- 設定修改與重跑範圍：README §4，以及各 pipeline 文件的「版本、重跑與恢復」章節
- 資料表與 artifact lineage：[`data-lineage.html`](data-lineage.html)
- 常見設定錯誤與排查：README §4
- 排序指標與評估報表：[`metrics.html`](metrics/metrics.html)
