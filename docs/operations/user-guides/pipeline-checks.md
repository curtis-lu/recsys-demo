# pipeline 的檢查：檢查什麼、在哪個階段、哪些擋不住

這頁給改設定檔、跑 pipeline 的人。讀完應該知道：框架具體檢查哪些東西、各在哪個階段擋下、為什麼在那裡，以及哪幾種錯誤**不會**被擋。

被擋下來時，錯誤訊息會說要改哪裡。想事先知道會查什麼，看下面各層的清單。帶代號的檢查（例如 `A24`），精確定義在 `src/recsys_tfb/core/consistency.py` 開頭的說明。每一節分成「會擋」和「只警告」：只警告的不會停下來，但會印在 log 裡。

這頁用到的幾個詞（除了「分區」，完整定義都在 repo 根目錄的 `CONTEXT.md`）：

- **item 清單**：`schema.categorical_values` 裡 item 欄的值，也就是模型認得的所有 item。
- **query group**：一次排序的範圍，由 time 和 entity 決定。名次只在同一組裡比。
- **identity 欄**：認出一筆候選的欄位組合，也就是 time、entity、item 三個角色對到的欄。
- **前處理器**：dataset 在 train 月份上建出來的特徵清單與類別編號表（產物 `preprocessor`）。training 和 inference 都用同一份。
- **promote**：由人工把某個模型版本設成預設版本。inference、evaluation 沒指定 `--model-version` 時就用它。
- **分區**：Hive 表依某幾欄的值切開存放的一塊，例如一個月一塊。

## 原則：需要看到什麼，就放在看得到它的最早時間點

一個檢查放在哪裡，取決於它要看到什麼才判斷得出對錯：

| 要看到什麼 | 最早什麼時候看得到 |
|---|---|
| 設定檔、指令參數 | 指令一開始，還沒讀任何資料 |
| 資料 | 讀到那份資料的時候 |
| 這次做出來的結果 | 結果做出來之後 |

放得越晚，被擋下之前白跑的就越多。例如 `training.final_model_strategy` 決定超參數搜尋（HPO）結束後怎麼產出最終模型；如果等到用它的那一步才檢查，這個值打錯字，要整個搜尋跑完才會發現。所以它在指令一開始就查（`A25`）。

## 三層

```
python -m recsys_tfb <指令> --env <env>
│
├─ ① 開跑前：看設定檔和指令參數。還沒啟動 Spark。
│
├─ ② 讀資料時：看資料跟設定對不對得上、沿用的舊產物是不是現在的設定做的。
│
└─ ③ 交出去前：看這次做出來的結果完不完整。
```

四個 source ETL 指令（`feature_etl`、`label_etl`、`sample_pool_etl`、`inference_population_etl`）也會過 ①，但之後的檢查時間點不一樣，另外一節說明。

---

## ① 開跑前（Spark 還沒開）

所有指令都會先過這一層。分兩批：

- **所有指令都查**：這一批不分指令，連只有 training 或 evaluation 才用到的設定也在裡面。所以**別的 pipeline 的設定寫錯，也會擋住你現在跑的指令**。
- **只在某個指令查**：少數只有一條 pipeline 會讀的設定，以及只有某些指令才有的旗標。

### 所有指令都查

**環境**
- **`--env <名稱>`**：`conf/<名稱>` 資料夾必須存在（`A30`）。不擋的話，名稱打錯不會報錯，會默默只用 `conf/base` 的設定跑完。

**schema**
- **`schema.columns`**：一定要寫 `time`、`entity`、`item`。`time`、`item`、`label`、`score`、`rank` 有寫就必須是非空字串（後三個沒寫會用預設值）。`entity` 是一個欄名，或非空的欄名清單。這三個角色合起來不能有重複的欄名。
- **`schema.categorical_values`**：格式是「欄名 → 值清單」，清單不能是空的。

**item 清單在各處要一致**
- **`dataset.prepare_model_input.categorical_columns`**：有寫這個鍵，就一定要包含 item 欄（`A2`）；item 欄也一定要在 `schema.categorical_values` 有值清單（`A3`）。
- **`training.feature_selection.exclude`**：不能列 item 欄（`A14`）。item 必須是模型的特徵。
- **`inference.products`**：必須跟 item 清單完全相同，不能多也不能少（`A4`）。
- **`dataset.sample_ratio_overrides`**：鍵是 `dataset.sample_group_keys` 各欄的值用 `|` 串起來的。item 欄在 `sample_group_keys` 裡時，鍵裡 item 那一段必須是清單裡的值（`A5`）。`training.sample_weights` 的鍵也一樣（`A9`）。

**dataset 設定**
- **`dataset.prepare_model_input.drop_columns` 與 `categorical_columns`**：同一個欄名不能兩邊都寫（`A1`）。一個說「這欄不要」，一個說「這欄當類別特徵」；不擋的話會默默以「不要」為準。
- **`dataset.train_split_keys`、`dataset.val_sample_keys`**：有寫的話，必須是 `schema.columns.entity` 裡的欄，不能是空清單（`A29`）。
- **`dataset.numeric_feature_storage_type`** 只能是 `float32` 或 `float64`；**`dataset.numeric_precision_policy`** 只能是 `block` 或 `truncate`。兩個都可以不寫，但不能寫成 `null`（`A31`）。
- **`dataset.train_zero_positive_group_ratio`、`val_zero_positive_group_ratio`、`test_zero_positive_group_ratio`**（沒有正例的 query group 留多少）：有寫就必須是 0 到 1 之間的數字，不能是字串、true／false 或 `null`（`A44`）。它們會進版本 ID，寫錯會讓之後每個指令讀到別的版本路徑。
- **`quality_checks.max_duplicate_key_ratio`**：source ETL 設定裡有 `sample_pool`、`label_table`、`feature_table` 這三張表的話，每一張都要寫，值在 0 到 1 之間、不含 1（`A32`）。拿掉它，那張表的主鍵重複檢查和主鍵空值檢查會一起默默關掉。

**training 設定**
- **`training.algorithm_params.objective` 與 `metric`**：objective 是排序目標（`lambdarank`、`rank_xendcg`）時，metric 有寫就必須是 `ndcg`、`map` 或 `lambdarank`（沒寫預設 `ndcg`），`schema.columns.entity` 也不能是空的（`A7`）。不擋的話，early stopping 看的指標沒有意義。
- **`training.search_space`**：每一項要有不重複的 `name`；`type` 是 `int`、`float`、`categorical` 之一；數值型的 `low` 要小於 `high`、`step` 要是正數；`log: true` 時 `low` 要大於 0，而且不能有 `step`；`categorical` 要有非空的 `choices`（`A8`）。
- **`training.sample_weight_keys` 與 `training.sample_weights`**：`sample_weight_keys` 的每一欄，都要是 train model_input 裡有的欄（identity 欄、label、`dataset.carry_columns`、類別欄）。`sample_weights` 的每個鍵用 `|` 分段，段數要等於 `sample_weight_keys` 的欄數（`A9`）。不擋的話，權重會默默沒套上。
- **`training.hpo_objective`** 只能是 `mean_ap` 或 `macro_per_item_map`；**`training.final_model_strategy`** 只能是 `hpo_best` 或 `refit_on_full`。可以不寫，但不能寫成 `null`（`A25`）。
- **`diagnostics.*`**（`parameters_training.yaml` 最外層）：`diagnostics.shap.background` 只能是 `global` 或 `per_item`；`diagnostics.gain_ledger.enabled`、`diagnostics.shap.quadrant_enabled` 必須是真正的 true／false；`diagnostics.shap.quadrant_top_k_decision`、`quadrant_sample_per_cell`、`quadrant_min_rows` 必須是大於等於 1 的整數（`A20`）。

**evaluation 設定**
- **`evaluation.segment_sources.<名稱>`**：`<名稱>` 必須列在 `evaluation.segment_columns` 裡；`table`、`key_columns`、`segment_column` 三個都要寫；`segment_column` 必須等於 `<名稱>`（`A10`）。
- **`evaluation.compare_sources` 的每一項**（`A11`）：
  - `kind` 是 `model_version` 或 `external_hive`，而且要有 `label`。
  - `model_version` 類：要寫 `model_version`；`source` 有寫的話只能是 `enriched_eval_predictions`、`ranked_predictions`、`training_eval_predictions`；不能寫 `columns`、`prod_mapping`。
  - `external_hive` 類：要寫 `table`、`prod_mapping`，以及 `columns`：time、每一個 entity 欄、item、score 各要對到外部表的一欄。`unmapped_policy` 只能是 `fail` 或 `drop`，沒寫就是 `fail`。
- **evaluation 的數值與開關**（`A15`、`A19`）：
  - `evaluation.metric.weight_alpha` 在 0 到 1 之間；`evaluation.metric.k` 是 `null` 或大於等於 1 的整數；`evaluation.metric.min_positives`、`evaluation.metric.shrinkage_k` 不能是負數。
  - `evaluation.diagnosis.sample.max_queries`、`evaluation.diagnosis.sample.min_pos_queries_per_item`、`evaluation.diagnosis.ci.n_boot` 至少是 1；`evaluation.diagnosis.item_ability.top_n`、`evaluation.diagnosis.suppression.top_examples` 不能是負數。
  - `evaluation.diagnosis.ci.enabled` 和每一個診斷的 `evaluation.diagnosis.<診斷名>.enabled`，必須是真正的 true／false。寫成字串 `"false"` 會被當成打開。
  - `evaluation.segment_columns` 不能用 `stratum`、`inclusion_weight` 這兩個保留名稱。
- **`evaluation.product_categories`**：已經改名成 `evaluation.item_categories`，還寫舊名就擋（`A33`）。

### 只在某個指令查

- **dataset**
  - `dataset.train_snap_dates` 一定要寫，而且不能是空清單（`A23`）。
  - `dataset.train_snap_dates`、`val_snap_dates`、`test_snap_dates` 三個清單不能有同一天（`A24`）。日期按日曆比，`2026-1-31` 和 `2026-01-31` 算同一天。拿同一個月訓練又拿它評估，評估數字會好看得不真實。
- **training**
  - `dataset.test_snap_dates` 一定要寫，而且不能是空清單（`A36`）。training 要在這些月份上預測、算指標、做 SHAP 診斷；不擋的話，要等超參數搜尋整輪跑完才會失敗，錯誤訊息也沒提到這個設定。dataset 指令沒寫或空清單都照樣跑，所以這條只在 training 查。training 的每一種跑法都查，包含 `--from-node`、`--only-node`、`--list-nodes`、`--dry-run`。
  - `dataset.test_snap_dates` 裡，同一天不能寫成只差在有沒有 `-` 的兩種格式，例如 `2026-01-31` 和 `20260131`（`A26`）。不擋的話，那個月的每一列會被算兩次。
  - `catalog.yaml` 的 `training_eval_predictions` 條目，`columns:` 必須包含 `schema.columns.entity` 的每一欄（`A28`）。少寫的欄在寫入時會被默默丟掉。
  - `dataset.test_zero_positive_group_ratio` 大於 0 時，同一個條目的 `columns:` 還必須包含 `zero_positive_group_weight`（`A45`）。少了它，權重在寫入時被默默丟掉，evaluation 會把每個留下的無正例組只算一次、而不是 1／r 次。
- **inference**
  - `inference.snap_dates`、`inference.products` 不能是空清單；`inference.entity_buckets`（把 entity 分成幾桶、一桶一桶評分）有寫就要大於等於 1，不能寫 `null`（`A27`）。
- **evaluation**
  - 帶 `--post-training` 時，`evaluation.snap_date` 一定要寫，而且必須在 `dataset.test_snap_dates` 裡（`A22`）。
  - `evaluation.report.sections` 有寫任何開關的話，開關名稱必須剛好是 `dataset_overview`、`primary_map`、`diagnostics`、`baseline`、`diagnosis_links`、`prediction_quality` 這六個，多一個或少一個都擋（`A34`）。
  - `evaluation.prediction_quality` 的 `n_bins`、`n_display_bins` 是大於等於 1 的整數，而且後者要整除前者；`top_n` 是大於等於 0 的整數；這個區塊裡不能有別的鍵（`A42`）。不擋的話，分箱表最後一格會比其他格窄，而報表不會提；寫 `enabled: true` 也打不開這一段，開關在 `report.sections.prediction_quality`。
  - `evaluation.report.diagnostics` 底下不能再寫 `include_calibration`、`n_calibration_bins`，不論值是什麼（`A43`）。它們設定的 calibration 分箱表 #381 已經移除，留著不會有任何作用；分數分箱改由預測品質指標家族提供。
  - 帶 `--post-training` 又打開 `evaluation.report.sections.prediction_quality` 時，`dataset.test_zero_positive_group_ratio` 必須大於 0（`A46`）。r 為 0（預設）的 test 表已經丟掉所有沒有正例的 query group，把每一列當二元預測的指標會系統性偏高。監控模式不查。
  - `--compare` 和 `--compare-only` 只能給一個；給的名稱必須是 `evaluation.compare_sources` 裡的鍵（`A12`、`A13`）。
- **dataset、training、inference**
  - `--rebuild-dates` 的每個日期要寫成 `YYYY-MM-DD`，而且要在設定的月份清單裡：dataset 和 training 對照 `dataset.test_snap_dates`，inference 對照 `inference.snap_dates`（`A21`）。

### Spark 啟動後才查

- `--model-version`、`--base-dataset-version` 指的版本資料夾存在。沒指定版本時，training 要找得到最新一版 dataset 與它底下最新一版 train 抽樣結果，inference 和 evaluation 要找得到 promote 過的模型。
- `--from-node`／`--only-node` 的步驟名稱存在，而且兩個不能同時給。
- `catalog.yaml` 每個條目的格式，例如要寫 `type`、可寫入的表要寫 `columns`、`external: true` 要寫 `location`。training 是例外：它在開跑前就讀過一次整份 `catalog.yaml`，所以這些格式錯誤在 training 開跑前就會擋下。

---

## ② 讀資料時

這一層擋兩種問題：**資料跟設定對不上**，以及**拿舊的東西當新的**。

### dataset

dataset 有三個**資料閘**：專門檢查、本身不改資料的步驟，分別在最開頭、特徵編碼完、最後一步。除此之外，做事的步驟途中也會順便查幾件事。

**資料閘 1：開頭**（pipeline 的第一步）
- **item 值**：`sample_pool` 在 dataset 設定的月份裡出現的 item 值，必須跟 item 清單完全相同；`label_table` 出現的 item 值必須都在清單裡（`B1`）。這一條要實際讀這兩張表。
- **類別欄的型別**：`dataset.prepare_model_input.categorical_columns` 列的欄，在 `feature_table` 裡不能是 decimal、double、float（`B5`）。decimal 會讓前處理器存檔失敗；double、float 幾乎一定是列錯了。
- **非數字的特徵欄**：`feature_table` 的特徵欄如果是文字、binary、日期、時間戳或複合型別，就必須列進 `categorical_columns` 或 `drop_columns`（`B6`）。不擋的話，training 讀資料時會失敗。
- **帶出欄和特徵欄撞名**：`dataset.carry_columns` 列的欄，如果也是 `feature_table` 裡的特徵欄（沒列在 `drop_columns`），就擋（`B7`）。不擋的話，組 model_input 時 Spark 會報欄名有歧義。
- **特徵欄和權重欄撞名**：`dataset.val_zero_positive_group_ratio` 或 `test_zero_positive_group_ratio` 大於 0 時，`feature_table` 的特徵欄不能叫 `zero_positive_group_weight`（`B12`）。那是 dataset 會加進 val／test 表的權重欄。

後面四條只讀 `feature_table` 的欄位定義，不讀資料。

**資料閘 2：精度閘**（特徵編碼完）
- 會被轉型的特徵欄（整數、decimal、boolean 這類值有固定間距的欄），最大絕對值不能超過 `dataset.numeric_feature_storage_type` 在那個間距下能精確表示的上限（`B8`）。例：整數欄轉 `float32`，上限是 16,777,216；`decimal(18,2)` 轉 `float32`，上限是 131,072。超過的話，兩個不同的值會變成同一個，排序會悄悄改變。
- 只查這一次處理的月份，只讀 parquet 檔尾的統計。`dataset.numeric_precision_policy: truncate` 時改成只警告。

**資料閘 3：粒度閘**（最後一步）
- `train`、`train_dev` 的 model_input 列數，必須等於組它用的 key 表（每一列是一筆抽到的候選）（`B10`）。列數變多，代表 `label_table` 或編碼後的特徵表在 join 鍵上有重複列。val、test 不查。只讀 parquet 檔尾的列數。`dataset.train_zero_positive_group_ratio` 小於 1 時，丟組發生在 key 表落地之前，所以這條比對照樣是「相等」。

**做事途中順便查的**
- 會擋：
  - `feature_table` 必須有 time 與 entity 欄，而且 time 欄要出現設定要處理的每個月份。
  - `dataset.train_dev_ratio` 不是 0，而抽樣切分後 train_dev 變成空的。
- 只警告：
  - `drop_columns` 列的欄在 `feature_table` 找不到（多半是打錯字）。
  - 抽樣切分時，切分用的欄位是空值的列會被丟掉。

### training

- 會擋：
  - **讀訓練資料之前**：特徵欄必須是數字（`B6`），而且儲存型別必須全部等於 `dataset.numeric_feature_storage_type`（`B9`）。例：1,000 個 `float32` 欄裡混了一個 `int64`，整個矩陣會變成 `float64`，2,400 萬列需要的記憶體從 89 GiB 變成 179 GiB。舊版 dataset 建出來的資料不會重過 dataset 的資料閘，這一步是補救。
  - **要預測的月份**：`dataset.test_snap_dates` 的每個月，都必須已經由 dataset 建好。
  - **本機 cache**：樣本權重的長度跟 LightGBM `.bin` cache 的列數對不上，要清掉 cache 目錄重建；磁碟空間不夠。
  - **`training.algorithm`**：必須是有註冊的演算法，目前只有 `lightgbm`。
- 只警告：
  - 上次中途掛掉留下的不完整 cache，自動清掉重抓；LightGBM 的 `.bin` cache 跟目前設定對不上，自動重建。
  - 過濾掉沒有正例的 query group 之後，train 或 train_dev 剩下的太少。
  - 目標是 ranking 類時，印出每個 split「只剩單一種 label 的 query group」佔多少（只是 log，不警告也不擋，判斷交給你）。
  - `training.sample_weights` 一列都沒對到。
  - `--fresh-hpo` 會丟掉已完成的 trial；HPO 的 checkpoint 讀不出來，當作沒有。
  - MLflow 記錄失敗，訓練照樣繼續（`mlflow.strict` 打開時才會擋）。

### inference

- 會擋：
  - **母體**：`inference_population` 必須有 `inference.snap_dates` 的每個月。
  - **特徵欄**：母體接上 `feature_table` 之後，必須有前處理器要的每個特徵欄。
  - **模型與前處理器**：模型的特徵清單，必須是前處理器特徵清單照順序取出的一部分。對不上多半是 `--model-version` 和 dataset 版本搭錯了。
  - **中間表**：`inference_population_features` 必須有模型要的每一欄。缺欄多半是用 `--from-node` 跳過了重建它的那一步。
  - **分區**：某個分區有登記、卻讀回 0 列。
- 只警告：
  - `inference.entity_buckets` 不在建議範圍 5 到 20 之間：太少，一桶可能塞不進 driver 記憶體；太多，每個分區的檔案會小到浪費。
  - 模型資料夾缺 `model_meta.json`。
  - 表裡留著這次格點以外的舊分區（改小 `entity_buckets` 或改過 item 名稱留下的）。評分時只警告，但發布前的檢查會擋，見 ③。

### evaluation

- 會擋：
  - **評估月份**：`evaluation.snap_date` 一定要寫。預測表在這個月必須有資料：帶 `--post-training` 讀 `training_eval_predictions`，不帶讀 `ranked_predictions`。
  - **label 重複**：`label_table` 在這個月，同一組 time、entity、item 不能有兩列。不擋的話，join 之後候選會被複製成多列，名次全錯。
  - **沿用上一輪的結果**：`enriched_eval_predictions`、指標、診斷結果這些已經落地的東西，必須是用現在「會影響計算」的那組設定算的。不是就擋，訊息會告訴你從哪一步重跑。
  - **分群**：`evaluation.segment_sources.<名稱>` 的表必須讀得到，而且有宣告的欄。
  - **baseline**：報表有開 baseline 時，`label_table` 在 `evaluation.baseline.lookback_months` 那段期間必須有資料。
  - **item 大類**：`evaluation.item_categories.enabled` 打開時，`evaluation.item_categories.mapping` 用到的 item 必須在 item 清單裡，`evaluation.item_categories.unmapped` 只能是 `singleton`。
  - **舊格式**：讀到舊格式的 `evaluation_results.json`，訊息會說要跑哪個遷移腳本。
  - **比較（`--compare`、`--compare-only`）**：
    - 帶 `--compare-only` 時，`enriched_eval_predictions` 在這個模型版本、這個月必須有資料，`segment_columns.json` 也必須在。這兩樣都要先跑過一次一般的 evaluation。
    - 比較對象在這個月必須有資料。
    - `unmapped_policy: fail` 時，外部 Hive 表出現的 item 值都必須在 `prod_mapping` 裡。
    - 兩邊至少要有一個共同的 entity 和一個共同的 item。
  - **診斷（`--post-training`）**：
    - config_shift 診斷打開、而且抽樣分層或樣本權重有用到 label 欄時，對應的抽樣比例（`dataset.sample_ratio`、`dataset.sample_ratio_overrides`）與樣本權重（`training.sample_weights`）必須大於 0。
    - 有打開、而且需要原始分數的診斷，預測表必須有 schema 宣告的 score 角色欄（`schema["score"]`，預設 `score`；`item_ability`／`suppression`／`config_shift` 已改讀這欄，不再讀已 deprecated 的 `score_uncalibrated`，#415），而且不能全是空值。
- 只警告：
  - `evaluation.segment_columns` 指到母體表沒有的欄。
  - `unmapped_policy: drop` 時，外部表裡對不上 `prod_mapping` 的 item 被丟掉。
  - 診斷抽樣時，一定要全取的 query 已經超過 `evaluation.diagnosis.sample.max_queries`；或抽樣後，沒被全取的 item 裡，有含正例 query 數低於 `min_pos_queries_per_item` 的。
  - 這個月沒有任何含正例的 query，指標算不出來。

---

## ③ 交出去前

這一層看這次做出來的結果，沒通過就不交給下游。

- **inference**（發布排名之前）
  - 會擋：
    - `inference.snap_dates` × `inference.entity_buckets` × `inference.products` 的每一格都要有分區（沒有任何 entity 的桶除外），也不能多出格點以外的分區。
    - 每個 query group 都要排滿 `inference.products` 的每個 item；名次在 1 到 item 數之間，而且名次越後面，分數不會更高。
    - 每一桶評分結果寫入之前，也會查空值、重複列、列數與 item 值。
    - 同一個 query group 裡所有 item 分數都一樣的 group，超過一半就擋。多半代表 item 餵進模型時變成了常數。
    - 這次一列都沒有評分，而且也沒有沿用任何既有分區。
  - 只警告：分數全一樣的 group 不到一半。
- **evaluation**：算出來的指標必須剛好只有一個月。
- **寫 Hive 表時**（每條 pipeline）：`catalog.yaml` 條目有 `partition_filter` 的，寫進去的資料值必須跟它一致。條目寫 `columns: auto` 而且表已經存在時，同名欄位的型別跟表裡既有的不同就擋，要重建那張表。

---

## 四個 source ETL 指令

它們產出的是其他 pipeline 讀的來源表。除了 ① 之外，檢查分成下面幾類：

- **跑之前**（會擋，Spark 還沒啟動）
  - `--target-dates` 沒給、設定裡也沒有 `target_dates`。
  - `--source-check` 和 `--restart-from` 一起用；`--restart-from` 的表名不存在。
  - 表的設定：`partition_by` 必須是非空的「欄名 → Hive 型別」對照，例如 `snap_date: DATE`；`depends_on` 引用的表必須排在它前面。
  - `--var` 與 YAML `variables` 的檢查（`A35`）：`--var` 帶到沒在 `variables` 宣告的名字、同一個名字帶了兩次、`--var` 缺 `=`、`variables` 的值不是字串或 `~`、`variables` 本身不是「名字 → 值」的對照、某個 `~` 名字沒有對應的 `--var`、某個變數的最終值裡有 `${target_date}` 以外的 `${...}`（變數之間依宣告順序逐一代換，換不換得到看順序，沒換到的到了 Spark 會被默默換成空字串；`${target_date}` 永遠最後換，所以可以用）。`target_date`、`target_db` 這兩個保留名都不能出現在 `--var`（`--var target_date=...`、`--var target_db=...` 都擋）；`target_date` 連寫進 YAML `variables` 也擋，`target_db` 寫成正常字串值不受影響，只有寫成 `target_db: ~` 才擋。這些問題會一次全部列出。
  - 把這次要跑的所有表、所有日期的 SQL 都先換一遍：換完還剩任何 `${...}`（包括 `${hiveconf:x}` 這類 Spark 自己的變數語法，以及只在 YAML 才會被換的 `${env.X}` 寫進 SQL 檔的情況）、SQL 檔案不存在，都在這裡擋下，不用等真正執行到那張表才發現。
- **查上游**（`source_checks`）：**只有帶 `--source-check` 時才跑**，而且那一次只檢查、不寫表。這一次一樣會做上面「跑之前」那些檢查，所以可以用同一行指令（含同一組 `--var`）先加 `--source-check` 預檢一次，通過後拿掉這個旗標正式執行；source check 本身不讀 `variables`，行為不變。
  - 會擋：該月分區不存在；有設 `min_row_count`（大於 0）時，列數不夠；有寫 `expected_columns` 時，實際欄位缺欄或型別不同，`allow_new_columns: false` 時多出新欄也擋。
  - 只警告：這個階段沒設定任何 `source_checks`。
- **執行 SQL 時**（會擋）：`partition_by` 的欄不在 SELECT 輸出裡；表已經存在時，SQL 拿掉了原本就有的欄。
- **寫完才查輸出**（`quality_checks`：列數、主鍵重複或空值、空值比例）：表已經寫進去之後才查。沒通過會中止後面的表，但**不會撤回已經寫進去的那一張**。沒設定 `quality_checks` 的表，不查資料的值。

**dry run 例外**：`dry_run` 沒設定時，`--env local` 預設開啟（目前 `feature_etl`、`label_etl` 就是這樣）。「跑之前」的變數檢查與 SQL 換一遍在 Spark 啟動前就做，不受 `dry_run` 影響，所以就算開了 dry run 也一樣會查。dry run 不會真的執行 SQL，所以「執行 SQL 時」整段不查，「寫完才查輸出」也整段不跑。

---

## 三件擋不住的事

**1. 用 `--from-node`／`--only-node` 跳過的步驟，裡面的檢查不會跑。**
dataset 的三個資料閘都在此列；就算切片自動補跑了前面的步驟，後面的資料閘也不會跟著補跑。執行計畫（`[plan]` 開頭的那幾行）會列出被跳過的步驟。上一輪跑完之後來源表如果變過，這一輪不會發現。① 開跑前的檢查不受影響，每次都會跑。

**2. 已經做好的產物會被直接沿用，框架不保證它是用現在的設定、程式、資料做的。**
框架決定沿用還是重做，看的是產物在不在，以及版本號（evaluation 另外比對設定指紋）。版本號只由一部分設定和 `feature_table` 的欄位結構算出來。所以下面這些情況，舊產物照樣被沿用：

- 改了程式。
- 來源表的某個月被回補過。
- 改了不進版本號的設定，例如 `inference.entity_buckets` 以外的 inference 執行設定：已經寫出的評分分區不會重算，除非帶 `--rebuild-dates`。
- 改了上游的設定，卻沒有重跑上游：training 預設用最新一版 dataset，inference 和 evaluation 預設用 promote 過的模型，都不會發現上游的設定已經不一樣。

什麼時候會出事、怎麼強制重算，見 [`pipeline-slicing.md`](pipeline-slicing.md) 的〈接續前提〉。

**3. 設定「合法」不等於「正確」。**
檢查只能發現設定自相矛盾或值超出範圍。一個合法但填錯的值不會被擋，例如抽樣比例該填 `0.1` 卻填了 `0.01`。
