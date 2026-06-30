# training pipeline

> 讀取 dataset pipeline 產出的各 split `*_model_input`，訓練一個供所有 item 共用的排序模型，並產生版本化模型、test 預測、離線指標與模型診斷。
> 主要流程為：選擇資料版本 → driver-local cache → 特徵選擇與模型格式轉換 → HPO → 最終模型 → 可選機率校準 → test 評估與診斷。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 使用版本化 dataset 訓練、評估並保存候選模型 |
| 執行指令 | `python -m recsys_tfb training` |
| 上游輸入 | `preprocessor`、`train_model_input`、`train_dev_model_input`、`val_model_input`、`test_model_input`，以及可選的 `calibration_model_input` |
| 主要輸出 | `model`、HPO 最佳參數與迭代次數、`training_eval_predictions`、test 指標與模型診斷 |
| 設定檔 | `conf/base/parameters_training.yaml` |
| I/O 設定 | `conf/base/catalog.yaml` |
| 下游 pipeline | `evaluation`、人工 model promotion、`inference` |

training 會訓練一個跨 item 共用的模型，而不是每個 item 各自訓練一個模型。模型輸入的每一列代表一個 `(time, entity, item)` 候選，模型產生 `score` 後，再由 inference 於每個 `(time, entity)` query group 內排序。

各 split 在 training 中的角色如下：

| split | Training 中的用途 | 是否套用 sample weight |
|---|---|:---:|
| `train` | 每個 HPO trial 的模型訓練資料 | ✓ |
| `train_dev` | 每個 trial 的 early stopping validation；`refit_on_full` 時會與 train 合併 | ✓ |
| `val` | 比較不同 HPO trials 的排序指標，選出最佳超參數 |  |
| `calibration` | 啟用時 fit 機率校準器，不參與建樹、early stopping 或 HPO 選模 |  |
| `test` | 最終模型完成後的 held-out 評估與診斷 |  |

`train_dev` 與 `val` 不可互換：前者決定單一 trial 何時停止 boosting，後者決定不同 trials 之間哪一組超參數較好。test 只應在最終模型產生後使用，不應反過來調整超參數。

模型介面透過 `ModelAdapter` 隔離演算法差異；目前專案已註冊並可直接使用的 adapter 為 LightGBM。
LightGBM 的 train/train-dev 會轉成可重用的 `.bin`，但這是目前 adapter 的實作細節，不是所有未來演算法都必須採用的格式。

## 2. 執行前準備

執行 training 前，建議依序確認：

1. **Dataset 已完整完成**：指定的 `base_dataset_version` 與 `train_variant_id` 必須存在，且 train、train-dev、val、test model input 均已產出。
2. **各 split 的資料角色正確**：train、calibration、val、test 日期應互斥並依時間合理安排；test 不可被用於 HPO 或 feature selection 決策。
3. **Calibration 兩端設定一致**：若 `training.calibration.enabled: true`，dataset 必須先以 `enable_calibration: true` 建立 calibration variant 與 `calibration_model_input`。
4. **item 保留為模型特徵**：`schema.item` 必須存在於 preprocessor 的 `feature_columns`，也不可被 `training.feature_selection.exclude` 排除。
5. **Sample weight 欄位可用**：`sample_weight_keys` 中非 identity、label 或 categorical feature 的欄位，必須由 dataset 的 `carry_columns` 帶入 train model input。
6. **Driver-local 空間足夠**：各 split 會從 Hive／HDFS 複製到 `cache.root`，模型、HPO study、診斷與 checkpoint 也會寫入 driver 本機檔案系統。
7. **Driver 記憶體足夠**：模型訓練、部分指標計算及診斷會將資料讀入 driver；應依資料量控制 feature 數、HPO 規模與 SHAP／feature statistics 抽樣上限。

CLI 啟動時會先執行設定一致性檢查，包括 ranking objective 與 metric 是否相容、HPO search space 格式、sample weight key 的欄位與段數、未知 item，以及 feature selection 是否錯誤排除 item。
這些檢查可避免明顯設定錯誤進入長時間訓練，但不能判斷資料是否有 target leakage、日期切分是否符合業務觀察窗，或某個設定是否在統計上合理。

## 3. 設定方式

### 3.1 演算法與訓練目標

```yaml
training:
  algorithm: lightgbm
  algorithm_params:
    objective: binary
    metric: binary_logloss
    verbosity: -1
    log_period: 100
    num_threads: 4
```

| 設定 | 說明 | 版本影響 |
|---|---|---|
| `training.algorithm` | ModelAdapter registry 中的演算法名稱；目前為 `lightgbm` | `model_version`、`search_id` |
| `algorithm_params.objective` | 模型學習目標 | `model_version`、`search_id` |
| `algorithm_params.metric` | train-dev early stopping 使用的演算法原生指標 | `model_version`、`search_id` |
| 其他模型參數 | 未放入 `search_space`、但每個 trial 都固定使用的參數 | `model_version`、`search_id` |
| `verbosity`、`log_period`、`num_threads` | logging 或執行資源設定 | 不影響版本 |

目前 LightGBM 支援的主要訓練範式：

| objective | 範式 | 學習方式 | score 語意 | 適用情境 |
|---|---|---|---|---|
| `binary` | Pointwise | 將每個 `(entity, item)` 視為一筆二元分類樣本 | 原始輸出接近機率，但不保證已校準 | 建立 baseline，或同時重視分類機率 |
| `lambdarank` | Learning to rank | 使用 query group 內 item 的相對順序學習 | 相對排序分數，不是機率 | 主要目標為提升 query 內排序品質 |
| `rank_xendcg` | Learning to rank | 以 ranking objective 直接學習群組內次序 | 相對排序分數，不是機率 | 需要另一種 LightGBM ranking objective 時 |

ranking objective 的 query group 為 `schema.time + schema.entity`。`metric` 必須使用 ranking metric，例如 `ndcg` 或 `map`；若省略，框架會預設為 `ndcg`。
不論模型採用 pointwise 或 learning-to-rank objective，HPO 與最終 test 評估仍以 query group 內的排序指標為準。

### 3.2 HPO 與選模指標

```yaml
training:
  hpo_objective: macro_per_item_map
  n_trials: 20
  num_iterations: 500
  early_stopping_rounds: 50
  search_space:
    - name: learning_rate
      type: float
      low: 0.001
      high: 0.1
      log: true
    - name: num_leaves
      type: int
      low: 4
      high: 64
```

| 設定 | 說明 |
|---|---|
| `hpo_objective` | 使用 val 比較 trials 的框架層排序指標 |
| `n_trials` | 目標完成的 Optuna trial 總數，不是每次重跑都追加的數量 |
| `num_iterations` | 每個 trial 的 boosting 上限 |
| `early_stopping_rounds` | train-dev 指標連續未改善時的停止容忍輪數 |
| `search_space` | Optuna 搜尋參數的有序 ParamSpec 清單 |

`hpo_objective` 目前支援：

| 值 | 選模方式 |
|---|---|
| `mean_ap` | 先計算每個 query group 的 AP，再對 query 等權平均 |
| `macro_per_item_map` | 將 mAP attribution 依 item 彙整後做 macro average，讓各 item 等權參與選模 |

`search_space` 的每個項目必須有唯一的 `name`，且 `type` 為 `int`、`float` 或 `categorical`。數值參數需提供 `low` 與 `high`，可選擇 `step` 或 `log`；類別參數需提供非空的 `choices`。
目前不支援 `when` 條件式空間或字串 expression bounds，傳入時會在 CLI 入口 fail-fast。

`algorithm_params.metric` 與 `hpo_objective` 是不同層次的設定：前者在單一 trial 內搭配 train-dev 做 early stopping，後者使用 val 比較所有 trials。

### 3.3 最終模型策略

```yaml
training:
  final_model_strategy: hpo_best
```

| 值 | 行為 | 取捨 |
|---|---|---|
| `hpo_best` | 直接保存 val 排序指標最佳 trial 所持有的模型 | 成本最低，模型使用 train 訓練並以 train-dev early stopping |
| `refit_on_full` | 以最佳超參數將 train + train-dev 合併重訓，迭代數固定為 `best_iteration`，不再 early stop | 使用更多訓練資料，但最終模型不是 HPO 當下評分的同一個 booster |

`refit_on_full` 只合併 train 與 train-dev，不會將 val、calibration 或 test 加入建模資料。ranking objective 下會保留 query group 邊界，避免合併後不同 query 被錯誤視為同一組。

### 3.4 Training-stage feature selection

```yaml
training:
  feature_selection:
    exclude:
      - low_value_feature
      - duplicated_feature
```

`training.feature_selection.exclude` 會在 training 開始時建立 preprocessor view，從 `feature_columns` 排除指定欄位。
HPO、最終訓練、calibration、test scoring 與診斷都使用同一份 feature view，inference 則依模型保存的 feature names 取欄，避免訓練與推論欄位不一致。

這是模型層的特徵實驗，因此修改後只會更新 `model_version`，不需要重建 dataset。`schema.item` 不可被排除；其他 exclude 名稱也應先確認存在於該 dataset 的 `feature_columns`。
目前不存在的欄位名稱會被忽略，但仍會進入版本 hash，因此可能產生內容相同、ID 不同的 model version。

LightGBM binary cache 會依 objective family 與保留後的 feature list 隔離，避免同一個 train variant 誤用其他 objective 或其他特徵子集建立的 `.bin`。

### 3.5 Sample weights

```yaml
training:
  sample_weight_keys:
    - cust_segment_typ
    - prod_name
  sample_weights:
    "mass|ccard_ins": 2.0
    "affluent|fund_mix": 0.7
```

`sample_weight_keys` 的順序就是 `sample_weights` key 使用 `|` 串接的順序。未列出的組合權重為 `1.0`；大於 `1` 代表提高影響力，小於 `1` 代表降低影響力。

權重只套用於 train 與 train-dev，不套用於 val、calibration、test 或 evaluation。
類別欄位可在設定中使用人類可讀值，runtime 會依 preprocessor 的 category mappings 轉為實際 encoding 後比對；identity、label 與 carry columns 則保留原始值語意。

CLI 會檢查：

- `sample_weight_keys` 是否存在於 model input 可用欄位。
- 每個 weight key 的 `|` 段數是否與 key 欄位數相同。
- 當 item 是 weight key 時，設定是否引用未知 item。

training 另會產生 `sample_weight_report.json`，列出實際 train 資料中完全沒有命中的 `unmatched_keys`。即使設定通過靜態檢查，拼錯客群值、資料期間沒有該組合或 encoding 不一致仍可能出現在此報告。

### 3.6 機率校準

```yaml
training:
  calibration:
    enabled: true
    method: sigmoid
```

校準方法支援 `sigmoid` 與 `isotonic`。啟用時，CLI 會解析 `calibration_variant_id`，pipeline 也會增加 `cache_calibration_model_input` 與 `calibrate_model` nodes。

只有下游需要將 `score` 解讀為機率，例如估算期望收益或比較不同日期的絕對分數水準時，才需要啟用 calibration。
純粹依 query group 內名次進行推薦時，校準通常不是必要步驟；LTR objective 的原始 score 尤其不應直接解讀為機率。

dataset 的 `enable_calibration` 與 training 的 `training.calibration.enabled` 應同步設定。calibration split 只 fit 校準器，不套 sample weight，也不參與 HPO 或最終 test 指標的母體選擇。

### 3.7 Cache、診斷與 MLflow

下列皆是頂層 ops 設定，不會改變 `model_version`：

| 區塊 | 用途 |
|---|---|
| `cache.root` | driver-local Parquet 與演算法格式快取根目錄 |
| `diagnostics.feature_stats` | 控制特徵統計開關、抽樣列數與高 null threshold |
| `diagnostics.feature_importance` | 控制模型原生 split/gain importance |
| `diagnostics.shap` | 控制 SHAP 開關、抽樣量、top K、案例數、計算預算與 per-item 強化（方向、採購者對照、偏離度） |
| `mlflow` | 設定 experiment、tracking URI 與失敗策略 |
| `hpo_checkpointing` | 是否持久化 Optuna study 與最佳模型 checkpoint |
| `spark` | training CLI 初始化 Spark 使用的執行設定 |

#### `diagnostics.shap` 設定詳細說明

`diagnostics.shap` 用來解釋模型在 test split 上「靠什麼把候選 item 排高或排低」。它不改變模型訓練結果，也不影響 `model_version`；調整這個區塊通常是為了控制診斷成本、提高 per-item 覆蓋率，或讓輸出更適合人工審核。

SHAP 診斷主要回答三個問題：

| 問題 | 看哪裡 | 解讀方式 |
|---|---|---|
| 整體模型靠哪些特徵排序？ | `global.top_features`、`summary/shap_summary_global.png` | `mean_abs_shap` 越大代表整體影響越大；`mean_signed_shap` > 0 表示平均把分數往上推，< 0 表示往下壓 |
| 某個 item 是否有自己的驅動特徵？ | `per_item[<item>].top_features`、per-item beeswarm | 對照全域 top features；若方向或排序明顯不同，代表 shared model 對該 item 使用了不同訊號 |
| 實際採購者和全體候選是否被同一組特徵驅動？ | `top_features_positive`、`positive_low_coverage` | 正樣本足夠時，可比較採購者 profile 與全體候選 profile；正樣本不足時先不要過度解讀 |

設定時可先依下列順序調整：

| 目的 | 參數 | 怎麼設定 |
|---|---|---|
| 開關 SHAP | `enabled` | 正式候選模型建議開啟；快速 smoke test 或 driver 資源不足時可暫時關閉 |
| 控制抽樣量 | `sample_rows` | SHAP 最主要的成本來源；資料量大、特徵多或樹多時先降低此值 |
| 避免超出計算預算 | `max_budget` | 以 `sample_rows * n_trees` 估算成本；超過時框架會自動降低有效抽樣列數 |
| 控制每個 item 的最低覆蓋 | `min_rows_per_item` | item 很多或長尾明顯時，可降低以避免抽樣不足；解讀時仍要看 `low_coverage` |
| 控制輸出特徵數 | `top_k` | 影響 JSON 與圖上顯示的特徵數；通常 20～30 足夠人工審核 |
| 控制全域 high/low 案例數 | `n_examples` | 只影響 `shap_diagnostics.json` 中的 example 摘要數，不影響 SHAP profile 計算 |
| 產生 per-item 圖 | `per_item_beeswarm` | item 數少或需要逐 item 審核時開啟；item 很多時可關閉以減少圖片數與執行時間 |
| 比較採購者 profile | `profile_positive` | 推薦保留 `true`；只有不需要 label==1 對照或正樣本極稀疏時才關閉 |
| 設定採購者 profile 門檻 | `positive_min_rows` | 正樣本低於此值時 `top_features_positive` 會是 `null`，避免用太少樣本解讀採購者特徵 |
| 衡量 item 與全域的差異 | `divergence_metric`、`divergence_top_k` | 預設 `jaccard_topk` 適合快速比較 top features 是否重疊；`divergence_top_k` 通常小於或等於 `top_k` |

`shap_diagnostics.json` 的重點欄位如下：

| 欄位 | 說明 |
|---|---|
| `top_features[*].mean_abs_shap` | 該特徵的平均影響幅度 |
| `top_features[*].mean_signed_shap` | 該特徵平均把分數往上或往下推的方向 |
| `top_features_positive` | 只用 label==1 採購者計算的 signed profile；正樣本不足 `positive_min_rows` 時為 `null` |
| `low_coverage` | 該 item 抽樣列數低於 `min_rows_per_item`，相關結論應保守解讀 |
| `positive_low_coverage` | 採購者樣本數低於 `positive_min_rows`，不要用 `top_features_positive` 做決策 |
| `divergence_from_global` | 0～1 浮點數；越高代表此 item 的重要特徵排序越不同於全域 |
| `idiosyncratic_features` | 此 item top-k 中不在全域 top-k 的特徵清單 |
| `item_idiosyncrasy` | 依 `divergence_from_global` 由高到低排序的 item 清單，用來快速找出 shared model 下最「不像全域」的 item |

偏離度高不一定代表模型錯了；它表示該 item 可能依賴更特殊的訊號。若該 item 的離線指標也偏弱，才是評估補特徵、調整 sampling、引入 per-item 策略或兩階段模型的起點。

local Parquet cache 以 dataset IDs 分層，若目錄存在 `_SUCCESS` 便直接重用；若目錄存在但缺少 `_SUCCESS`，框架會視為不完整 cache 並重建。LightGBM `.bin` 會再依 objective family 與 feature selection 子集隔離。

`mlflow.strict: false` 時，MLflow 無法連線或 logging 失敗只會記 warning，不會讓已完成的 training 失敗；設為 `true` 時則會直接中止，適合要求 experiment tracking 必須成功的環境。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--base-dataset-version <id>` | `latest` | 指定 base dataset version |
| `--train-variant <id>` | 該 base 下的 train `latest` | 指定 train variant |
| `--calibration-variant <id>` | 該 base 下的 calibration `latest` | calibration 啟用時指定 calibration variant |
| `--from-node <name>` | 無 | 從指定 node 的拓撲位置開始，並執行其後 nodes |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--fresh-hpo` | 關閉 | 清除目前 `search_id` 的 HPO study 與 checkpoint，從 trial 0 重搜 |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開 |
| `--list-nodes` | 關閉 | 列出 node 名稱與接續成本 |

`--from-node` 與 `--only-node` 互斥；`--list-nodes` 也不能與兩者併用。`--calibration-variant` 只有在 `training.calibration.enabled: true` 時使用。

`--dry-run` 與 `--list-nodes` 不會執行 nodes、寫模型或建立 manifest，但 CLI 仍會載入設定、初始化 Spark、解析 dataset versions、計算 `model_version`／`search_id`，並查詢 catalog 產物是否存在。

### 4.2 完整執行

```bash
python -m recsys_tfb training --env local
```

省略版本旗標時，CLI 會先解析 `data/dataset/latest`，再使用該 base 下的 train `latest`；若 calibration 啟用，也會解析該 base 下的 calibration `latest`。

完整執行適合：

- 第一次訓練某組 dataset 與 training 設定
- 上游 dataset version 改變
- 修改 objective、HPO、sample weights、feature selection、calibration 或 final strategy
- 不確定既有模型產物或 cache 是否完整

### 4.3 指定上游資料版本

```bash
python -m recsys_tfb training \
  --env production \
  --base-dataset-version <base_version> \
  --train-variant <train_variant>
```

啟用 calibration 時可再指定：

```bash
python -m recsys_tfb training \
  --base-dataset-version <base_version> \
  --train-variant <train_variant> \
  --calibration-variant <calibration_variant>
```

固定版本適合重現舊實驗、比較不同 training 設定，或避免 `latest` 在排程期間被其他 dataset run 更新。指定的 base version 不存在時 CLI 會立即中止；variant 也必須存在於該 base 目錄下。

### 4.4 查看 nodes 與執行計畫

```bash
python -m recsys_tfb training --list-nodes

python -m recsys_tfb training \
  --from-node finalize_model \
  --dry-run
```

切片計畫會區分 requested、auto-included、skipped 與 skipped side-effect nodes。
執行前應特別確認 `tune_hyperparameters` 是否被列為 auto-included；若原本預期跳過 HPO，卻因必要產物不存在而被補跑，成本可能大幅增加。

### 4.5 從 final model 接續

```bash
python -m recsys_tfb training \
  --from-node finalize_model
```

`--from-node` 使用拓撲順序語意：執行指定 node，以及拓撲序中位於其後的所有 nodes，不只 dependency descendants。
從 `finalize_model` 接續通常用於已完成 HPO，但需要重做 final model、calibration、test 預測、指標或診斷的情況。

在前一次完整 run 成功且 catalog 產物仍存在時，框架預期直接讀取 `best_params`、`best_iteration` 與 `hpo_best_model`，不重跑 `tune_hyperparameters`。
它仍會自動執行較便宜的 `select_features`、train/train-dev/test cache handle nodes；calibration 啟用時也會執行 calibration cache handle。

若 HPO 的三個必要產物有任何一個不存在，slice planner 會自動補跑其 producer，可能一路回到 `prepare_lgb_train_inputs` 與 `tune_hyperparameters`。是否真的跳過 HPO，應以 `--dry-run` 當次顯示的計畫為準。

### 4.6 只執行單一 node

```bash
python -m recsys_tfb training \
  --only-node calibrate_model
```

`--only-node` 適合除錯或重新產生單一產物；必要輸入不存在時，仍會自動補入最小上游集合，但不會執行該 node 的下游 consumers。

只要 pipeline 實際執行，CLI 仍會寫入該 `model_version` 的 manifest。因此 `--only-node` 應視為進階維運工具：執行後需確認 test 預測、evaluation results 與 diagnostics 是否仍對應目前模型，不應用它建立一個從未完整成功過的新 model version。

### 4.7 HPO 中斷後恢復或重搜

相同 `search_id` 的 training 重跑時，若 `hpo_checkpointing: true`，會自動開啟既有 Optuna study、載入最佳模型 checkpoint，並只執行尚未完成的 trials，不需額外旗標：

```bash
python -m recsys_tfb training \
  --base-dataset-version <base_version> \
  --train-variant <train_variant>
```

若要放棄目前搜尋紀錄並從 trial 0 開始：

```bash
python -m recsys_tfb training \
  --base-dataset-version <base_version> \
  --train-variant <train_variant> \
  --fresh-hpo
```

`--fresh-hpo` 只清除目前計算出的 `search_id`，不會刪除其他模型或其他 search 的 HPO 紀錄。

## 5. 執行流程

calibration nodes 只有在 `training.calibration.enabled: true` 時加入。

| 階段 | node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 特徵選擇 | `select_features` | `preprocessor`、parameters | 套用 training-stage feature exclusion | `preprocessor_view` |
| Local cache | `cache_train_model_input`、`cache_train_dev_model_input`、`cache_val_model_input`、`cache_test_model_input` | 各 split Hive table | 將指定 dataset partitions 複製為 driver-local Parquet | 各 split `ParquetHandle` |
| Calibration cache | `cache_calibration_model_input` | calibration Hive table | 啟用時建立 calibration local cache | calibration `ParquetHandle` |
| 模型格式 | `prepare_lgb_train_inputs` | train/train-dev handles、preprocessor view | 由 adapter 建立可重用訓練格式；LightGBM 為 `.bin` | train/train-dev model handles |
| 權重報告 | `persist_sample_weight_report` | train handle、preprocessor | 比對 weight 設定與實際 train 值 | sample weight report |
| HPO | `tune_hyperparameters` | train/train-dev model handles、val handle | train 訓練、train-dev early stop、val 排序指標選模 | `best_params`、`best_iteration`、`hpo_best_model` |
| 最終模型 | `finalize_model` | HPO 產物、train/train-dev handles | 沿用 HPO best 或在 train + train-dev refit | 未校準模型 |
| 機率校準 | `calibrate_model` | 未校準模型、calibration handle | fit sigmoid 或 isotonic calibrator | 最終 `model` |
| Test 預測 | `predict_and_write_test_predictions` | model、test handle | 逐 `(time, item)` partition 預測並寫入 Hive | `training_eval_predictions`、`predict_manifest` |
| Test 指標 | `compute_test_mAP_spark` | test 預測 | 使用 Spark 計算整體 mAP 與 per-item attribution | `evaluation_results` |
| 特徵統計 | `compute_feature_statistics` | train handle、preprocessor view | 抽樣計算 null、distinct 與數值分布 | `feature_statistics` |
| 模型重要性 | `compute_feature_importance` | model | 計算 split、gain 與 dead features | `feature_importance` |
| SHAP | `compute_shap_diagnostics` | model、test handle | 依 item 分層抽樣後計算全域 beeswarm、per-item 帶方向 SHAP profile（含採購者對照）與偏離度排名 | `shap_diagnostics`、PNG |
| 實驗記錄 | `log_experiment` | model、參數、指標、診斷 | 將實驗寫入 MLflow | 無 |

test 預測會逐 partition 讀取 driver-local Parquet，避免一次將全部 test features 收進記憶體。
寫入 `training_eval_predictions` 的資料包含 entity、`score`、`score_uncalibrated`、label，以及作為 Hive partitions 的 time、item、`model_version`。calibration 關閉時，`score_uncalibrated` 與 `score` 相同。

`compute_test_mAP_spark` 會從 Hive 讀回目前 `model_version` 的預測並計算排序指標。若模型已校準，也會平行計算原始未校準 score 的結果，讓使用者確認 calibration 是否改變排序表現。

## 6. 產物與驗收

### 6.1 主要產物

| 類型 | 產物 | 儲存位置或方式 |
|---|---|---|
| 最終模型 | `model.txt`、`model_meta.json` | `data/models/<model_version>/` |
| HPO 結果 | `best_params.json`、`best_iteration.json` | `data/models/<model_version>/` |
| HPO best model | `hpo/model.txt`、`hpo/model_meta.json` | `data/models/<model_version>/hpo/` |
| Test 指標 | `evaluation_results.json` | `data/models/<model_version>/` |
| 權重診斷 | `sample_weight_report.json` | `data/models/<model_version>/` |
| 模型診斷 | feature statistics、importance、`shap_diagnostics.json` 與 PNG | `data/models/<model_version>/diagnostics/` |
| 執行追溯 | `manifest.json`、`parameters_training.json` | `data/models/<model_version>/` |
| Test 預測 | `training_eval_predictions` | Hive，以 `model_version`、time、item 分區 |
| HPO 恢復狀態 | Optuna journal 與最佳 checkpoint | `data/models/_hpo/<search_id>/` |
| Driver cache | 各 split Parquet 與 LightGBM `.bin` | `cache.root/<base_dataset_version>/...` |
| Experiment tracking | 參數、指標、模型與診斷 | MLflow tracking URI |

SHAP PNG 落於 `diagnostics/summary/` 子目錄：全域 beeswarm 為 `summary/shap_summary_global.png`；`per_item_beeswarm: true` 時每個 item 另有 `summary/per_item/shap_summary__<item>.png`（item 名稱以正規表達式安全化，特殊字元轉底線）。beeswarm 同時呈現 SHAP 幅度與方向；高分、低分與 per-item 高分案例摘要則寫在 `shap_diagnostics.json` 的 `examples` 區塊。

`model_meta.json` 會記錄 adapter 與 calibration metadata，使 inference 載入時能正確還原模型包裝。`hpo_best_model` 放在獨立 `hpo/` 子目錄，避免它的 sidecar 與最終模型互相覆寫。

training 不會建立或更新 `best` model alias。模型必須通過人工審核後，才由 `scripts/promote_model.py` 將指定 `model_version` 設為 inference 預設版本。

### 6.2 驗收重點

執行完成後至少確認：

1. log 中的 `base_dataset_version`、`train_variant_id`、可選的 `calibration_variant_id`、`model_version` 與 `search_id` 符合預期。
2. `manifest.json` 記錄的上游 dataset IDs 與本次指定版本一致，且 artifacts 清單完整。
3. `model.txt`、`model_meta.json`、`best_params.json`、`best_iteration.json` 與 `evaluation_results.json` 均存在。
4. `sample_weight_report.json` 沒有未預期的 `unmatched_keys`。
5. `training_eval_predictions` 的本次 `model_version` partition 有資料，entity、time、item 與 label 範圍合理。
6. `evaluation_results.json` 的 `n_queries` 大於零，`overall_map` 與 per-item attribution 可合理解讀。
7. diagnostics 開啟時，檢查 dead features、高 null／single-value features，以及 SHAP 抽樣覆蓋是否足夠；`item_idiosyncrasy` 中偏離度高的 item 表示共用模型依賴不同特徵組合，是評估 per-item 或兩階段模型的起點；`top_features_positive` 可對照採購者與整體候選的驅動特徵差異。
8. 若啟用 calibration，比較 calibrated 與 uncalibrated 指標，並確認業務下游確實需要機率語意。

範例查詢：

```sql
SELECT snap_date, prod_name, COUNT(*) AS rows
FROM ml_recsys.training_eval_predictions
WHERE model_version = '<model_version>'
GROUP BY snap_date, prod_name
ORDER BY snap_date, prod_name;
```

實際 database、table 名稱與 partition 欄位以 `conf/base/catalog.yaml` 為準。

## 7. 版本、重跑與恢復

### 7.1 `model_version` 的精確計算範圍

`model_version` 是 8 碼 SHA-256 hash，計算內容為：

```text
model-defining training 設定
+ base_dataset_version
+ train_variant_id
+ calibration_variant_id（calibration 啟用時）
```

model-defining training 設定只取 `parameters_training.yaml` 的 `training:` 區塊，並排除 `training.algorithm_params` 下的 `verbosity`、`log_period` 與 `num_threads`。因此：

- `training:` 下既有或未來新增的其他設定，預設都會更新 `model_version`。
- 頂層 `spark`、`mlflow`、`cache`、`diagnostics` 與 `hpo_checkpointing` 不會更新 `model_version`。
- mapping 的 key 排列順序不影響 hash，但 list 的內容與順序會影響，例如 `search_space` 或 feature exclusion 清單重新排序也會翻版。

### 7.2 `model_version` 與 `search_id` 對照

`search_id` 用來識別可恢復的 HPO study。它的計算範圍與 `model_version` 幾乎相同，但刻意排除 `training.n_trials`，讓增加 trials 時可延續同一個搜尋。

| 設定或因素 | `model_version` | `search_id` | 說明 |
|---|:---:|:---:|---|
| `base_dataset_version` | ✓ | ✓ | val、test、preprocessor 或基礎 model input 改變 |
| `train_variant_id` | ✓ | ✓ | train/train-dev 抽樣或切分改變 |
| `calibration_variant_id` | ✓ | ✓ | 僅 calibration 啟用時加入 |
| `training.algorithm` | ✓ | ✓ | 演算法改變 |
| `algorithm_params.objective`、`metric` 與其他模型參數 | ✓ | ✓ | 改變 trial 的模型或評分行為 |
| `algorithm_params.verbosity`、`log_period`、`num_threads` |  |  | 明確排除的 logging／執行設定 |
| `calibration.enabled`、`calibration.method` | ✓ | ✓ | 目前整個 calibration 設定皆位於 hashed `training:` block |
| `sample_weight_keys`、`sample_weights` | ✓ | ✓ | 改變 train/train-dev 權重 |
| `hpo_objective` | ✓ | ✓ | 改變 val 上的 trial 選擇方式 |
| `n_trials` | ✓ |  | 新 model version 可延用相同 HPO study 並補 trials |
| `num_iterations`、`early_stopping_rounds` | ✓ | ✓ | 改變單一 trial 的訓練與停止行為 |
| `final_model_strategy` | ✓ | ✓ | 目前位於 model-defining block，因此兩者都翻新 |
| `feature_selection.exclude` | ✓ | ✓ | 改變模型 feature subset |
| `search_space` | ✓ | ✓ | 改變可搜尋參數或範圍 |
| 頂層 `cache`、`diagnostics`、`mlflow`、`spark`、`hpo_checkpointing` |  |  | 只影響執行、觀測或恢復方式 |
| CLI `--fresh-hpo` |  |  | runtime 動作，只清除目前 search state |

`parameters.yaml` 的 `random_seed` 目前不在 `parameters_training.yaml` 的 hashed payload，因此不會更新 `model_version` 或 `search_id`，但它會影響 Optuna sampler、LightGBM seed 與 final refit。
修改 seed 時應人工視為新模型實驗，避免在同一 `search_id` 下混合不同隨機狀態。

### 7.3 HPO 恢復語意

啟用 `hpo_checkpointing` 時，每個 `search_id` 會在 `data/models/_hpo/<search_id>/` 保存：

| 產物 | 用途 |
|---|---|
| `study_journal.log` | Optuna trials 與狀態 |
| `checkpoint/model.txt` | 目前最佳 trial 的模型 |
| `checkpoint/best_meta.json` | 最佳分數、參數、iteration、trial number 與 search ID |

恢復規則：

1. 相同 `search_id` 重跑會載入已完成 trials 與最佳 checkpoint。
2. `n_trials` 表示目標完成總數；已有 12 個 completed trials 且設定為 20 時，只再跑 8 個。
3. 只增加 `n_trials` 時，`model_version` 會更新，但 `search_id` 不變，因此新版本可沿用原 study。
4. 修改 search space、objective、資料版本、權重等因素時，`search_id` 改變並自動建立新 study。
5. `--fresh-hpo` 會清除目前 search 的 journal 與 checkpoint，再從 trial 0 開始。
6. `hpo_checkpointing: false` 時 study 只存在記憶體，程序中斷後無法續跑。

HPO 恢復要求 `data/models/_hpo` 位於可持久保存的 driver disk。若每次排程取得全新的暫存主機，或該路徑會被清除，checkpoint 機制便無法跨程序生效。

### 7.4 Pipeline slicing 的安全邊界

- catalog 的 `exists()` 只能證明檔案或 partition 存在，不能證明它仍與目前程式碼、來源資料或未納入 hash 的設定一致。
- `--from-node finalize_model` 要跳過 HPO，必須同時存在 `best_params`、`best_iteration` 與 `hpo_best_model`；缺少任一項都可能自動補跑 HPO。
- HPO 跑到一半的恢復由 `search_id` journal/checkpoint 處理；HPO node 已完成後跳到 `finalize_model` 則由 catalog-persisted outputs 處理。兩者是不同層次的恢復機制。
- cache node 的輸出 handle 是記憶體物件，因此接續時會重新執行；底層 local Parquet 有 `_SUCCESS` 時只建立 handle，不會重新從 HDFS 複製。
- 沒有輸出的設定閘或 sink node，在切片起點之前不會自動重跑。資料或設定來源有疑慮時應使用 full run。
- slicing manifest 會記錄 `resumed_from` 或 `only_node`，但 partial run 不代表整個 model version 已重新完成所有驗收步驟。
- 若改了 model-defining 參數後再 `--from-node` 接續，`model_version` 會漂移到一個尚無模型的新版本目錄，切片會把 `finalize_model` 等上游拉回＝重新訓練。CLI 在這種情況會於開跑前印 `[retrain]` 警告（含最接近的既有 `completed` 版本與 diff 提示）但仍照跑；想沿用既有模型請先還原 `training:` 設定。詳見 [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。
- 開跑前 CLI 會先寫一份 `status: running` 的 `manifest.json` stub（崩潰溯源用），成功完成後覆寫為 `status: completed`；用 `--dry-run` / `--list-nodes` 時不寫 stub。

### 7.5 修改設定時要重跑什麼

| 修改內容 | 版本結果 | 建議 |
|---|---|---|
| objective、metric、固定 algorithm params | 新 `model_version` 與 `search_id` | 完整重跑 training |
| HPO search space、選模指標、iteration 或 early stopping | 新 `model_version` 與 `search_id` | 完整重跑 training |
| 只增加 `n_trials` | 新 `model_version`，相同 `search_id` | 完整啟動 training，沿用 study 補足 trials |
| feature selection 或 sample weights | 新 `model_version` 與 `search_id` | 不需重建 dataset；完整重跑 training |
| weight key 新增非既有 model input 欄位 | dataset version 也需更新 | 先加入 `carry_columns` 並重跑 dataset，再 training |
| calibration 開關或方法 | 新 `model_version` 與 `search_id` | 確認 dataset calibration 產物後完整重跑 |
| final model strategy | 新 `model_version` 與 `search_id` | 完整重跑；目前此設定也會建立新的 HPO search |
| diagnostics、MLflow、cache 或 Spark 設定 | 版本不變 | 依變更目的 full run 或從適當 node 接續，避免覆寫同版但語意不同的診斷 |
| 上游 base/train/calibration variant | 新 `model_version` 與 `search_id` | 使用新 IDs 完整重跑 training |
| 全域 `random_seed` | 目前版本與 search ID 不變 | 人工視為新實驗；避免直接延用既有 HPO study |
| training Python 程式碼 | 版本不一定改變 | 程式修正可能覆寫相同 model version，應記錄 git commit 並重新驗收 |

training 版本描述的是模型設定與上游資料身分，不是完整的程式碼或資料內容雜湊。相同 version ID 下重新執行可能覆寫既有模型與 Hive partitions，因此對未納入 hash 的變更必須由使用者管理實驗邊界。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| 找不到 base dataset version | 指定 ID 錯誤，或 dataset 尚未完成 | 檢查 `data/dataset/<base_version>/manifest.json` 與 `latest` |
| 找不到 train/calibration variant | variant 不屬於指定 base，或 calibration dataset 未建立 | 到該 base 目錄確認 `train_variants`／`calibration_variants` |
| ranking objective 搭配 binary metric | `lambdarank`／`rank_xendcg` 仍使用 `binary_logloss` | 改用 `ndcg`、`map`，或省略 metric 使用預設 `ndcg` |
| `training.search_space` 格式錯誤 | 使用舊 dict 格式、重複 name、bound 不合法或用了尚未支援的 `when` | 改為 ParamSpec 有序 list，依錯誤訊息逐項修正 |
| feature selection excludes item | item 被列入 `training.feature_selection.exclude` | 移除 item；item 必須保留為模型特徵 |
| weight key column unavailable | 權重維度未存在於 model input | 將欄位加入 dataset `carry_columns` 或 categorical features，重跑 dataset |
| weight key arity mismatch | `sample_weights` key 的 `|` 段數與 `sample_weight_keys` 不同 | 依欄位順序重建 key，建議使用 sampling editor |
| `sample_weight_report` 出現 unmatched keys | 設定值拼錯、該組合在 train 期間不存在或 encoding 不一致 | 查 train distinct values 與 category mappings，修正權重設定 |
| cache input must be Spark DataFrame | catalog input 不是預期 Hive/Spark dataset，或自行呼叫 node 傳入 pandas | 確認 catalog dataset type 與正常 CLI 執行路徑 |
| partial cache detected | 上次 copyToLocal 中斷，目錄沒有 `_SUCCESS` | 框架會自動清除並重建；若持續發生，檢查 disk、HDFS 權限與 copy 失敗訊息 |
| driver disk space 不足 | local Parquet、`.bin`、模型或 HPO checkpoint 累積 | 檢查 `cache.root` 與 `data/models` 容量，規劃版本與 cache 保留政策 |
| HPO 每次都從頭開始 | `search_id` 已改變、checkpointing 關閉或 driver disk 不持久 | 比對 log 中 search ID，確認 `data/models/_hpo/<search_id>` 存在 |
| HPO 已達 n_trials 但仍重訓一次 | study 有紀錄但最佳 checkpoint 不可讀 | 檢查 checkpoint 完整性；框架會以 study best params 做一次 recovery refit |
| `--from-node finalize_model` 仍補跑 HPO | 三個 HPO catalog outputs 有缺漏 | 先用 `--dry-run` 查看 auto-included，修復或重建缺少的產物 |
| calibration variant 或 input 不存在 | training 開啟 calibration，但 dataset 未建立對應 split | dataset 啟用 calibration 並完整產出後再 training |
| calibration 後排序指標改變很多 | 單調性、資料量或方法不符合預期，或比較母體不同 | 比較 `evaluation_results` 中 calibrated／uncalibrated 結果並檢查 calibration split |
| `n_queries = 0` 或 test 預測為空 | test input 沒資料、版本 partition 錯誤，或沒有可評估正例 query | 查 dataset test model input 與 `training_eval_predictions` partitions |
| SHAP 過慢或記憶體不足 | `sample_rows × n_trees` 太大，或 feature 太多 | 降低 `sample_rows`、`top_k`、`max_budget`，或暫時關閉 SHAP |
| MLflow 失敗但 training 顯示完成 | `mlflow.strict: false` 為 best-effort 模式 | 檢查 warning 與 tracking URI；需要硬性追蹤時設 `strict: true` |
| Unknown algorithm | `training.algorithm` 未在 adapter registry 註冊 | 使用目前支援的 `lightgbm`，或先實作並註冊新的 ModelAdapter |
| 部分重跑後模型、預測與診斷不一致 | `--only-node` 未重跑下游，或 skipped artifact 已過期 | 由較前方 node 接續或執行 full run，重新完成驗收 |

## 9. 限制與注意事項

- 目前實際註冊的演算法 adapter 為 LightGBM；其他演算法需要另外實作 train、predict、save/load、feature importance、MLflow 與 native input preparation。
- 模型訓練是 driver 上的單機 CPU 工作，不是 Spark distributed training；Spark 主要負責上游資料處理、Hive I/O 與 test 指標聚合。
- train、train-dev、val 與 test 的 local Parquet 會占用 driver disk；cache 不會自動依版本數量清理。
- feature statistics、SHAP 與部分模型資料抽取使用 pandas／NumPy，記憶體尖峰取決於 rows、features 與 tree 數。
- HPO study 不支援同一 `search_id` 由多個 training processes 同時寫入；應避免並行啟動相同搜尋。
- HPO resume 可延續 completed trials，但重新建立的 TPE sampler 狀態不保證與完全不中斷的單次執行 bitwise identical。
- `random_seed` 會影響模型與 HPO，但目前不納入 `model_version` 或 `search_id`。
- `num_threads` 被排除於 model version；LightGBM 不保證不同 thread count 下完全 bitwise identical，因此正式環境應固定 core 設定。
- test evaluation 使用 dataset 已排除零正例 query groups 的母體，不代表 inference 的完整 entity 母體。
- calibration 只能改善 score 的機率解讀，不保證提升排序指標；對 LTR score 的機率化也需以獨立資料與業務用途驗證。
- training 成功不代表模型已核准上線。仍需檢查 test 指標、per-item 表現、診斷與業務限制，再人工 promotion。

## 10. 相關文件

- 上游資料切分、前處理與 model input：[`dataset.md`](dataset.md)
- 訓練後 test 評估與模型比較：[`evaluation.md`](evaluation.md)
- 模型上線後的批次排序：[`inference.md`](inference.md)
- 指標定義：[`../metrics/metrics.html`](../metrics/metrics.html)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、恢復與人工卡控設計背景：[`../design-principles.md`](../design-principles.md)
