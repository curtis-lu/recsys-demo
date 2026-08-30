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

CLI 啟動時會先執行設定一致性檢查，包括 ranking objective 與 metric 是否相容、HPO search space 格式、sample weight key 的欄位與段數、未知 item、feature selection 是否錯誤排除 item，以及 `hpo_objective` 與 `final_model_strategy` 是否為合法值（A25——打錯的話原本要等整輪 HPO 跑完才會炸）。另外兩項也在起 Spark 前由 training 指令擋下：`dataset.test_snap_dates` 用兩種拼法指到同一個月（A26），以及 `training_eval_predictions` 這筆 catalog 條目沒有把 `schema.entity` 的每一欄都寫進 `columns:`（A28——Hive 寫入只留宣告過的欄，少宣告的那一欄會被靜默丟掉，寫出來的每一列都變成在指別的東西）。
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
HPO、最終訓練、calibration 與 test scoring 都使用同一份 feature view。**診斷與 inference 不看這份 view，改依模型保存的 feature names 取欄**——模型是唯一記得「這次訓練實際用了哪個子集」的產物，兩邊都因此不受事後改動 `exclude` 影響（見 §5 表後說明）。

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

上例是手填的覆寫；要從實際樣本量**推導**這張表（雙因子地板 `v` ＋ 注意力 `A`，並一併處理 ratio 面下採），用 `scripts/sampling_overrides_editor.py`。概念框架、公式、`w_pos`/`w_neg` 與 key 組法、邊界情況見 [`../operations/user-guides/sampling-overrides-editor.md`](../operations/user-guides/sampling-overrides-editor.md)。

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
| `diagnostics.gain_ledger` | 控制 Gain 帳本開關；跨樹按 item 記帳（id 切點 vs 子樹內 context 切點/Gain），量每個產品分到多少個人化容量。判讀見 `docs/pipelines/evaluation-diagnosis.md` §2（本檔不複述） |
| `diagnostics.shap` | 控制 SHAP 開關、抽樣量、top K、計算預算、per-item 強化（方向、申辦客戶對照、偏離度）與象限診斷（per-(item×象限) profile 與極值案例圖）；`background: global\|per_item` 的條件化背景語意與版本限制見 `docs/pipelines/evaluation-diagnosis.md` §2.5 |
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
| 實際申辦客戶和全體候選是否被同一組特徵驅動？ | `top_features_positive`、`positive_low_coverage` | 正樣本足夠時，可比較申辦客戶 profile 與全體候選 profile；正樣本不足時先不要過度解讀 |

實際執行時，`compute_shap_diagnostics` 不會把整份 test 讀進記憶體：它先只讀 `item`（分區欄）這一欄做分層，並用模型樹數估算 SHAP 成本——若 `sample_rows * n_trees` 超過 `max_budget`，會自動降低有效抽樣列數。接著依 item 分層抽樣（每個 item 至少嘗試抽 `min_rows_per_item`，資料不足的 item 則全取，避免診斷被熱門 item 主導），最後只把抽中的列連同所需的 feature 與 label 欄讀出來。

抽樣完成後，節點把這批 rows 轉成模型輸入矩陣 `X`，用 final model 重新預測 score，並對這批 `X` 計算一次 SHAP values——全域 profile 與 per-item profile 都共用這批 attribution（全域說明整體模型的主要特徵，per-item 說明每個 item 自己的驅動訊號）。申辦客戶（label==1）的 profile 則是獨立針對正樣本另抽一批、另跑一次 SHAP（見 `profile_positive`），與全域抽樣解耦以確保正例覆蓋。單列的代表案例則由象限診斷的 `cases/` 提供（見下方「象限診斷」小節）。

因為採用上述 bounded read（只完整讀分層用的 `item` 欄、特徵只讀抽中的列），driver 記憶體尖峰由抽樣規模決定，而非整份 test 的大小；早期「先把 test parquet 全讀成 pandas 再抽樣」的記憶體疑慮已在記憶體重構後解除。

設定時可先依下列順序調整：

| 目的 | 參數 | 怎麼設定 |
|---|---|---|
| 開關 SHAP | `enabled` | 正式候選模型建議開啟；快速 smoke test 或 driver 資源不足時可暫時關閉 |
| 控制抽樣量 | `sample_rows` | SHAP 最主要的成本來源；資料量大、特徵多或樹多時先降低此值 |
| 避免超出計算預算 | `max_budget` | 以 `sample_rows * n_trees` 估算成本；超過時框架會自動降低有效抽樣列數 |
| 控制每個 item 的最低覆蓋 | `min_rows_per_item` | item 很多或長尾明顯時，可降低以避免抽樣不足；解讀時仍要看 `low_coverage` |
| 控制輸出特徵數 | `top_k` | 影響 JSON 與圖上顯示的特徵數；通常 20～30 足夠人工審核 |
| 控制案例圖特徵數 | `case_top_k` | 單列 case 圖顯示 |SHAP| 最大的前 N 個特徵；預設 15，太擠可再降 |
| 產生 per-item 圖 | `per_item_beeswarm` | item 數少或需要逐 item 審核時開啟；item 很多時可關閉以減少圖片數與執行時間 |
| 比較申辦客戶 profile | `profile_positive` | 推薦保留 `true`；只有不需要 label==1 對照或正樣本極稀疏時才關閉 |
| 設定申辦客戶 profile 門檻 | `positive_min_rows` | 正樣本低於此值時 `top_features_positive` 會是 `null`，避免用太少樣本解讀申辦客戶特徵 |
| 衡量 item 與全域的差異 | `divergence_metric`、`divergence_top_k` | 預設 `jaccard_topk` 適合快速比較 top features 是否重疊；`divergence_top_k` 通常小於或等於 `top_k` |

`shap_diagnostics.json` 的重點欄位如下：

| 欄位 | 說明 |
|---|---|
| `top_features[*].mean_abs_shap` | 該特徵的平均影響幅度 |
| `top_features[*].mean_signed_shap` | 該特徵平均把分數往上或往下推的方向 |
| `top_features_positive` | 只用 label==1 申辦客戶計算的 signed profile；正樣本不足 `positive_min_rows` 時為 `null` |
| `low_coverage` | 該 item 抽樣列數低於 `min_rows_per_item`，相關結論應保守解讀 |
| `positive_low_coverage` | 申辦客戶樣本數低於 `positive_min_rows`，不要用 `top_features_positive` 做決策 |
| `divergence_from_global` | 0～1 浮點數；越高代表此 item 的重要特徵排序越不同於全域 |
| `idiosyncratic_features` | 此 item top-k 中不在全域 top-k 的特徵清單 |
| `item_idiosyncrasy` | 依 `divergence_from_global` 由高到低排序的 item 清單，用來快速找出 shared model 下最「不像全域」的 item |

#### 怎麼讀 `divergence_from_global`（偏離度）

`divergence_from_global` 落在 0～1：**0 = 該 item 的重要特徵排序和全域完全一致；1 = 完全不重疊**。但它**沒有固定門檻**——是相對、比較性的指標，主要用途是把 item 互相排名（`item_idiosyncrasy` 即依它排序），看誰突出，而非比一個絕對數字。什麼算「高」取決於特徵數、`divergence_top_k` 與模型重要性的集中程度。

**中性基準要看 `divergence_metric` 用哪個：**

- `spearman`：全特徵重要性排序的相關係數映射到 `(1 − ρ) / 2`，有天然中性點——**0.5 = 兩排序無關（純隨機）**、0 = 完全一致、1 = 完全相反。想要「0.5 以上才算偏離」的直覺就用它。
- `jaccard_topk`（**預設**）：只比前 `divergence_top_k`（預設 15）大的特徵集合，`1 − 交集 / 聯集`。有兩個陷阱讓「0.5 = 一半不同」的直覺**不成立**：
  1. **非線性**：兩邊各 15 個、共享 m 個時，共享 15→0、12→0.33、**10→0.50**、7→0.70、5→0.80、0→1.0；共享 2/3（10/15）就已是 0.5。
  2. **高維下隨機基準貼近 1**：約 1500 個特徵各取前 15，純隨機的期望交集僅 ≈ 15² / 1500 ≈ 0.15 個 → 隨機 item 的偏離度 ≈ 1。真實 item 遠低於 1，正因模型確實共用一批核心特徵；所以別用「0.7 感覺蠻高」這種絕對讀法。

**預設 `jaccard_topk` 的手感錨點：**

| `divergence_from_global` | 共享前 15 大約 | 解讀 |
|---|---|---|
| 0–0.2 | 13–15 個 | 幾乎同全域，不特殊 |
| 0.3–0.5 | 10–12 個 | 輕微偏離 |
| 0.6–0.8 | 5–8 個 | 驅動特徵明顯不同，值得看 |
| 0.9–1.0 | ≤3 個 | 幾乎全不同 → 強烈特有，或資料太少造成假象 |

更可靠的做法是看 `item_idiosyncrasy` 的**分佈**：排最上面、且和其他 item 有明顯落差的才是真正突出的——**排名與間距比絕對值重要**。

**兩個必記的陷阱：**

- **先看 `low_coverage` / 抽樣列數**：某 item 抽到的列很少 → top-k 不穩 → 偏離度被**假性推高**。高偏離度若伴隨 `low_coverage: true`，先當雜訊。
- **偏離度高不一定代表模型錯**：只表示該 item 依賴更特殊的訊號。要**該 item 的離線指標（如其 mAP）也偏弱**，才是評估補特徵、調整 sampling、引入 per-item 策略或兩階段模型的起點。

#### 象限診斷（top@1 TP/FP/FN/TN）

象限診斷聚焦「模型的 top@1 決策」：對每個 query group（time × entity），以最高分候選為決策。
依 `label` 分四象限——`TP`（排第 1 且採用）、`FP`（排第 1 未採用）、`FN`（未排第 1 但採用）、
`TN`（未排第 1 未採用）。由 `quadrant_enabled` 開關（沿用同一組 `quadrant_*` 設定）。

| 產物 | 內容 | 位置 |
| --- | --- | --- |
| `per_quadrant.json` | 每 (item×象限) 聚合的 signed SHAP profile（平均驅動特徵 + 方向 + `low_coverage`），每格抽樣 `quadrant_sample_per_cell` 列 | `diagnostics/per_quadrant.json` |
| 案例圖 | 每 (item×象限) 全格最高分、最低分各一列的單列 signed SHAP 貢獻橫條圖（`case_top_k` 個特徵，紅=推高分、藍=拉低分） | `diagnostics/cases/<item>/{TP,FP,FN,TN}_{high,low}.png` |
| `cases_manifest.json` | 完整 4-象限稽核表：每 item × 4 象限 × {high,low} 一筆,含 schema 欄位值（time/entity 欄，如 `snap_date`/`cust_id`）與 `rank/score/label` 及 PNG 路徑（相對於 `diagnostics/`，如 `cases/<item>/TP_high.png`）；空格記 `reason=empty`、單行格 low 記 `reason=single_row_same_as_high` | `diagnostics/cases/cases_manifest.json` |

**top@1 本質**：多數 item 從不會被排到第 1，因此其 `TP/FP` 格常為空（manifest 記 `empty`），
`FN/TN` 才飽滿——這本身即是「該 item 幾乎不被列為首選」的重要訊號，而非缺漏。
案例圖用來看「某位客戶在某象限被排高／排低，具體靠哪些特徵」，與 `per_quadrant.json` 的
「平均驅動特徵」互補。SHAP 值在 log-odds（margin）尺上；正值把分數推高、負值拉低。

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
| `--rebuild-dates <d1,d2>` | 無 | 指名重算這些 test 月份的預測（丟掉該月本機 cache ＋ 忽略「已完整」而重新預測）。值須為 `dataset.test_snap_dates` 子集（A21）；上游回補時與 dataset 的同名旗標成對使用 |
| `--from-node <name>` | 無 | 從指定 node 的拓撲位置開始，並執行其後 nodes |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--fresh-hpo` | 關閉 | 清除目前 `search_id` 的 HPO study 與 checkpoint，從 trial 0 重搜 |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開 |
| `--list-nodes` | 關閉 | 列出 node 名稱與接續成本 |

`--from-node` 與 `--only-node` 互斥；`--list-nodes` 也不能與兩者併用。`--calibration-variant` 只有在 `training.calibration.enabled: true` 時使用。`--rebuild-dates` 與切片旗標可以併用——重算某個月的預測本來就走 `--only-node predict_and_write_test_predictions`；只有當切片把該 node 排除、旗標因此無事可做時才會印 `[rebuild] WARNING`。

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

啟用 calibration 時另有一個更後面的接續點：

```bash
python -m recsys_tfb training \
  --from-node calibrate_model
```

只想換 calibration 方法、或重做 calibration 之後的預測與診斷時用它。`finalize_model` 的未校準模型已落地成 `trained_model`，所以**不會**被拉回重跑——`final_model_strategy: refit_on_full` 下那會是一次完整 refit。自動補跑的只有 `select_features` 與 calibration／test 的 cache handle nodes（test handle 是被後面的 `predict_and_write_test_predictions` 需要的，不是 calibration 需要）。這組允許集合釘在 `tests/test_pipelines/test_resume_contracts.py`。

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

接續生效時 log 會印 `HPO resume: N completed trial(s) found; best so far score=... running M more`（`nodes.py::tune_hyperparameters`）；沒看到這行就是沒接上，先查 `search_id` 與 `data/models/_hpo/<search_id>/` 是否存在。

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
| 特徵選擇 | `select_features` | `preprocessor`、parameters | 套用 training-stage feature exclusion；只餵給下方**訓練**模型的 node，診斷 node 不吃（見表後說明） | `preprocessor_view` |
| Local cache | `cache_train_model_input`、`cache_train_dev_model_input`、`cache_val_model_input`、`cache_test_model_input` | 各 split Hive table | 將指定 dataset partitions 複製為 driver-local Parquet | 各 split `ParquetHandle`；`cache_test_model_input` 例外，回傳 `{snap_date: ParquetHandle}` 對應（一月一目錄） |
| Calibration cache | `cache_calibration_model_input` | calibration Hive table | 啟用時建立 calibration local cache | calibration `ParquetHandle` |
| 模型格式 | `prepare_lgb_train_inputs` | train/train-dev handles、preprocessor view | 由 adapter 建立可重用訓練格式；LightGBM 為 `.bin` | train/train-dev model handles |
| 權重報告 | `persist_sample_weight_report` | train handle、preprocessor | 比對 weight 設定與實際 train 值（node 只回傳診斷，`sample_weight_report.json` 由 catalog 寫出） | `sample_weight_report` |
| HPO | `tune_hyperparameters` | train/train-dev model handles、val handle | train 訓練、train-dev early stop、val 排序指標選模 | `best_params`、`best_iteration`、`hpo_best_model` |
| 最終模型 | `finalize_model` | HPO 產物、train/train-dev handles | 沿用 HPO best 或在 train + train-dev refit | 未校準模型 |
| 機率校準 | `calibrate_model` | 未校準模型、calibration handle | fit sigmoid 或 isotonic calibrator | 最終 `model` |
| Test 預測 | `predict_and_write_test_predictions` | model、test handles | 逐月判斷是否需要預測，需要的月份再逐 `(time, item)` partition 預測並寫入 Hive | `training_eval_predictions`、`predict_manifest` |
| Test 指標 | `compute_test_mAP_spark` | test 預測 | 使用 Spark 計算整體 mAP 與 per-item attribution | `evaluation_results` |
| 特徵統計 | `compute_feature_statistics` | train handle、model、`preprocessor` | 抽樣計算 null、distinct 與數值分布 | `feature_statistics` |
| 模型重要性 | `compute_feature_importance` | model | 計算 split、gain 與 dead features | `feature_importance` |
| Gain 帳本 | `compute_gain_ledger` | model、`preprocessor` | 跨樹按 item 記帳（id 切點 vs context 切點的 Gain）；`preprocessor` 只用到 `category_mappings`，把整數切點還原成 item 值 | `gain_ledger` |
| SHAP | `compute_shap_diagnostics` | model、test handle、`preprocessor` | 依 item 分層抽樣後計算全域 beeswarm、per-item 帶方向 SHAP profile（含申辦客戶對照）與偏離度排名 | `shap_diagnostics`、PNG |
| 象限選樣 | `select_shap_population` | training_eval_predictions、test_model_input | top@1 象限 + 每格抽樣（profile）與全格極值（cases） | `shap_population`、`case_rows` |
| 象限 profile | `compute_quadrant_profiles` | model、shap_population、`preprocessor` | per-(item×象限) 聚合 signed profile | `quadrant_profiles`（`per_quadrant.json`） |
| 象限案例 | `compute_quadrant_cases` | model、case_rows、`preprocessor` | 每 (item×象限) 極值案例單列 SHAP 圖 | `cases_manifest`、PNG |
| 實驗記錄 | `log_experiment` | model、參數、指標、診斷 | 將實驗寫入 MLflow | 無 |

**診斷 node 為什麼吃 `preprocessor` 而不是 `preprocessor_view`。** `preprocessor_view` 只活在記憶體裡，沒有 catalog 條目；吃它的 node 一定要連 `select_features` 一起重跑才叫得動。診斷 node 都在模型產出**之後**才跑，所以改問兩個已落地的產物：**用哪些特徵、什麼順序問模型**（`model.feature_names()`），**怎麼編碼問 `preprocessor`**。這五條邊因此消失了（[ADR-0014](../adr/0014-training-modules-split-by-role.md) 決定 7）。

**實際省下的是 HPO。** `compute_feature_statistics` 從前沒有 model 依賴，拓撲序把一個寫進 `data/models/<model_version>/` 的診斷排到「產出模型的 node」之前；`--from-node` 是「跑指定 node 與其後全部」，於是為了重算一份 null rate 的 JSON，`prepare_lgb_train_inputs`、`tune_hyperparameters`、`finalize_model` 全被掃回來。實測 `--from-node compute_feature_statistics` **從 18 個 node 降到 13 個**，不再重跑 HPO。

**還沒省下的**：`predict_manifest` 與兩個 `*_parquet_handle` 都還是 memory-only，所以切片仍會補跑 `predict_and_write_test_predictions`（它會連帶把 `select_features` 拉回來——predict node 是**套用**模型，吃 `preprocessor_view` 對它是正確的）以及兩個 cache node。要再往下砍，卡在 `cache.root` 是相對路徑：診斷若從別的目錄啟動會指到不同地方而且不報錯。確切的接續集合釘在 `tests/test_pipelines/test_resume_contracts.py`。

代價是 `compute_feature_statistics` 多了一個 `model` 輸入——資料層診斷因此綁上模型 artifact。之所以接受：`feature_statistics` 本來就寫在 `data/models/<model_version>/` 底下，替一個不存在的模型算診斷本來就不成立。

**這不是在修一個靜默錯誤。** 改由 config 推導欄集**不會**漂移：`training.feature_selection.exclude` 住在 `training:` block，改它會 bump `model_version` → model 的 catalog 路徑跟著變 → 整條訓練鏈被拉回重跑。ADR-0014 決定 7 對這點有明確的誠實標註。**這是把介接口弄乾淨**，理由是可定址性（catalog 條目）與拓撲位置，不是正確性。（inference 那邊的理由不同、且真的關乎正確性，見 [ADR-0011](../adr/0011-inference-validation-two-layers.md) §5。）

**半成品 cache 現在會擋下來。** cache node 的 `_SUCCESS` marker 是最後才寫的，所以「有目錄、沒 marker」＝複製到一半斷了。同一個 marker 兩邊行為刻意相反：

| 誰 | 拿到沒有 `_SUCCESS` 的目錄 | 為什麼 |
|---|---|---|
| `cache_train_model_input` 等 cache node | **清掉，從 Hive 重建** | 來源還在，重建是對的復原行為 |
| `compute_feature_statistics`、`compute_shap_diagnostics` | **報錯**（`require_complete_cache`） | 它們只拿到 handle、重建不了；讀半成品不會報錯，統計會照算、MLflow 會照記，數字描述的是不知道多大的一部分資料 |

（[ADR-0014](../adr/0014-training-modules-split-by-role.md) 決定 7 的驗收條件，兩條分開測。）

實務差別有兩處，方向相反：`--only-node compute_feature_statistics` 現在需要一個 `model_version` 範圍的輸入（變貴）；`--from-node compute_feature_statistics` 不再掃回 HPO（變便宜，見上）。另外 `--from-node calibrate_model` 會多重建一次 train local cache。

test 預測會逐 partition 讀取 driver-local Parquet，避免一次將全部 test features 收進記憶體。
寫入 `training_eval_predictions` 的資料包含 entity、`score`、`score_uncalibrated`、label，以及作為 Hive partitions 的 time、item、`model_version`。calibration 關閉時，`score_uncalibrated` 與 `score` 相同。

**逐月增量**：predict 會跳過已經預測完整的月份，所以多評估一個月的成本正比於新月份，而不是累積的總月份數。權威的月份清單是 `dataset.test_snap_dates`（cache 只是資料來源）；某月的完成判準是「該月已寫出的 item partition 集合 ＝ 該月 cache 中出現的 distinct item」——寫到一半中斷、或事後新增一個 item，都會讓該月不再完整而被重做。可以跳過是因為 `(model_version, snap_date)` 的預測是不可變產物：`model_version` 已把定義模型的一切雜湊進去，重算必然得到相同結果。「已存在哪些 partition」由 `training_eval_predictions` 這個 catalog dataset 物件回答（`HiveTableDataset.existing_partition_values()`，metastore-only 查詢，套用該表的 `partition_filter` 因此天然限縮在目前 `model_version`）——predict 拿不到 SparkSession，這是唯一的路。

`predict_manifest` 因此帶三份清單：`months_processed`／`months_skipped`／`months_rebuilt`（後者是被 `--rebuild-dates` 強制重做的子集）。同一份 manifest 的 `snap_dates`／`prods`／`n_rows_written` 講的是**這一次寫了什麼**，不是這個 test set 有哪些月——全部月份都被跳過時它們是空的、`0`，這是正確的。指標不受影響：`compute_test_mAP_spark` 是從 Hive 讀回整個 `model_version` 的預測，被跳過的月份的 partition 本來就還在表裡。跳過的判準是「存在」不是「新鮮」，所以上游對舊月份回補之後要用 `--rebuild-dates` 指名重算——它同時丟掉該月的本機 parquet cache 並重新預測；動線見 [adding-an-eval-month.md](../operations/user-guides/adding-an-eval-month.md)。

`compute_test_mAP_spark` 會從 Hive 讀回目前 `model_version` 的預測並計算排序指標。若模型已校準，也會平行計算原始未校準 score 的結果，讓使用者確認 calibration 是否改變排序表現。

## 6. 產物與驗收

### 6.1 主要產物

| 類型 | 產物 | 儲存位置或方式 |
|---|---|---|
| 最終模型 | `model.txt`、`model_meta.json` | `data/models/<model_version>/` |
| HPO 結果 | `best_params.json`、`best_iteration.json` | `data/models/<model_version>/` |
| HPO best model | `hpo/model.txt`、`hpo/model_meta.json` | `data/models/<model_version>/hpo/` |
| 未校準模型（僅 calibration 啟用時） | `trained/model.txt`、`trained/model_meta.json` | `data/models/<model_version>/trained/` |
| Test 指標 | `evaluation_results.json` | `data/models/<model_version>/` |
| 權重診斷 | `sample_weight_report.json` | `data/models/<model_version>/` |
| 模型診斷 | feature statistics、importance、`shap_diagnostics.json`、`per_quadrant.json`、`cases/` PNG 與 `cases_manifest.json` | `data/models/<model_version>/diagnostics/` |
| 執行追溯 | `manifest.json`、`parameters_training.json` | `data/models/<model_version>/` |
| Test 預測 | `training_eval_predictions` | Hive，以 `model_version`、time、item 分區 |
| HPO 恢復狀態 | Optuna journal 與最佳 checkpoint | `data/models/_hpo/<search_id>/` |
| Driver cache | 各 split Parquet 與 LightGBM `.bin` | `cache.root/<base_dataset_version>/...`；test split **一個月一個目錄**（`test_months/<YYYYMMDD>/`，各自 `_SUCCESS`）。test 日期已退出 `base_dataset_version`（ADR-0001），因此**加一個月只複製那一個月**，既有月份 `cache_hit` 原封不動（見 ADR-0003；操作見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)）。代價是同一個 base version 底下**重算**既有月份時 cache 不會失效（只看 `_SUCCESS`、不看新鮮度），須手動刪該月目錄——見 known-pitfalls §15。設定列了某月但來源表沒有 → 該月的複製 glob 零命中即 `FileNotFoundError`（機制自帶的 fail-loud） |
| Experiment tracking | 參數、指標、模型與診斷 | MLflow tracking URI |

SHAP PNG 落於 `diagnostics/summary/` 子目錄：全域 beeswarm 為 `summary/shap_summary_global.png`；`per_item_beeswarm: true` 時每個 item 另有 `summary/per_item/shap_summary__<item>.png`（item 名稱以正規表達式安全化，特殊字元轉底線）。beeswarm 同時呈現 SHAP 幅度與方向。象限案例圖見下方象限診斷小節。

`manifest.json` 的 `artifacts` 清單只列版本目錄**第一層**檔案，**不含 `hpo/`、`trained/` 子目錄**（`hpo/model.txt`、`hpo/model_meta.json`、`trained/model.txt`、`trained/model_meta.json`）——稽核 manifest 時請知悉。`sample_weight_report.json` 在第一層，所以它在清單裡。

`model_meta.json` 會記錄 adapter 與 calibration metadata，使 inference 載入時能正確還原模型包裝。`hpo_best_model` 與（calibration 啟用時的）`trained_model` 各自放在獨立的 `hpo/`／`trained/` 子目錄，避免它們的 sidecar 與最終模型互相覆寫——sidecar 帶著 `calibrated` 旗標，覆寫會決定模型之後被怎麼**載入**。

training 不會建立或更新 `best` model alias。模型必須通過人工審核後，才由 `scripts/promote_model.py` 將指定 `model_version` 設為 inference 預設版本。

### 6.2 驗收重點

執行完成後至少確認：

1. log 中的 `base_dataset_version`、`train_variant_id`、可選的 `calibration_variant_id`、`model_version` 與 `search_id` 符合預期。
2. `manifest.json` 記錄的上游 dataset IDs 與本次指定版本一致，且 artifacts 清單完整。
3. `model.txt`、`model_meta.json`、`best_params.json`、`best_iteration.json` 與 `evaluation_results.json` 均存在。
4. `sample_weight_report.json` 沒有未預期的 `unmatched_keys`。
5. `training_eval_predictions` 的本次 `model_version` partition 有資料，entity、time、item 與 label 範圍合理。
6. `evaluation_results.json` 的 `n_queries` 大於零，`overall_map` 與 per-item attribution 可合理解讀。
7. diagnostics 開啟時，檢查 dead features、高 null／single-value features，以及 SHAP 抽樣覆蓋是否足夠；`item_idiosyncrasy` 中偏離度高的 item 表示共用模型依賴不同特徵組合，是評估 per-item 或兩階段模型的起點；`top_features_positive` 可對照申辦客戶與整體候選的驅動特徵差異。
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
| `base_dataset_version` | ✓ | ✓ | val、preprocessor 或基礎 model input 改變（**test 月份的增減不在其中**——它已退出 base 的 hash payload，見 ADR-0001） |
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

`data/models/_hpo/<search_id>/` 在成功完成後**刻意保留**（很小、可稽核、重跑秒收）。它以 `search_id` 為單位、**跨 `model_version` 共用**，因此刪除某個 `data/models/<model_version>/` 目錄不會連帶清掉它。要清理：單一搜尋用 `--fresh-hpo`（下次該 `search_id` 執行時清）或手動刪該子目錄；全部清空則 `rm -rf data/models/_hpo/`。

### 7.4 Pipeline slicing 的安全邊界

- catalog 的 `exists()` 只能證明檔案或 partition 存在，不能證明它仍與目前程式碼、來源資料或未納入 hash 的設定一致。
- `--from-node finalize_model` 要跳過 HPO，必須同時存在 `best_params`、`best_iteration` 與 `hpo_best_model`；缺少任一項都可能自動補跑 HPO。
- 同理，`--from-node calibrate_model` 要跳過 final model，必須存在 `trained_model`（`trained/model.txt` ＋ `trained/model_meta.json`）；缺了就會把 `finalize_model` 拉回重跑。
- `--from-node calibrate_model` 另外會重建 **train** local cache（`cache_train_model_input`）。原因是 `compute_feature_statistics` 現在吃 `model`，因而排在 `calibrate_model` 之後（先前它沒有 model 依賴，拓撲序可以把它排到模型產出之前），它的 train handle 就被帶進這個切片。成本是一次 Hive→本機複製，不是重訓。
- HPO 跑到一半的恢復由 `search_id` journal/checkpoint 處理；HPO node 已完成後跳到 `finalize_model` 則由 catalog-persisted outputs 處理。兩者是不同層次的恢復機制。
- cache node 的輸出 handle 是記憶體物件，因此接續時會重新執行；底層 local Parquet 有 `_SUCCESS` 時只建立 handle，不會重新從 HDFS 複製。
- 沒有輸出的設定閘或 sink node，在切片起點之前不會自動重跑。資料或設定來源有疑慮時應使用 full run。
- slicing manifest 會記錄 `resumed_from` 或 `only_node`，但 partial run 不代表整個 model version 已重新完成所有驗收步驟。
- 若改了 model-defining 參數後再 `--from-node` 接續，`model_version` 會漂移到一個尚無模型的新版本目錄，切片會把 `finalize_model` 等上游拉回＝重新訓練。CLI 在這種情況會於開跑前印 `[retrain]` 警告（含最接近的既有 `completed` 版本與 diff 提示）但仍照跑；想沿用既有模型請先還原 `training:` 設定。詳見 [`../operations/user-guides/pipeline-slicing.md`](../operations/user-guides/pipeline-slicing.md)。
- `hpo_best_model` **不做 None 防護**：HPO 第一個 trial 必然寫入 best model（score ≥ 0 > 初始 -1.0）；`n_trials=0` 會在 `study.best_params` 就先炸。
- 落地 `hpo_best_model` 後，full run 的 `finalize_model` 也會吃到磁碟 round-trip 的 adapter。行為不變：LightGBM `save_model` 預設截斷至 best_iteration，預測結果一致；`best_iteration` 另以 JSON 落地顯式傳遞。
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
| weight key arity mismatch | `sample_weights` key 的 `|` 段數與 `sample_weight_keys` 不同 | 依欄位順序重建 key，建議使用 [sampling editor](../operations/user-guides/sampling-overrides-editor.md) |
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
- **`prepare_lgb_train_inputs` 的 `to_numpy` 是全流程的 driver 記憶體峰值**，且它的觀測數字會低報——見 §9.1。
- HPO study 不支援同一 `search_id` 由多個 training processes 同時寫入；應避免並行啟動相同搜尋。
- HPO resume 可延續 completed trials，但重新建立的 TPE sampler 狀態不保證與完全不中斷的單次執行 bitwise identical。
- `random_seed` 會影響模型與 HPO，但目前不納入 `model_version` 或 `search_id`。
- `num_threads` 被排除於 model version；LightGBM 不保證不同 thread count 下完全 bitwise identical，因此正式環境應固定 core 設定。
- test evaluation 使用 dataset 已排除零正例 query groups 的母體，不代表 inference 的完整 entity 母體。
- calibration 只能改善 score 的機率解讀，不保證提升排序指標；對 LTR score 的機率化也需以獨立資料與業務用途驗證。
- training 成功不代表模型已核准上線。仍需檢查 test 指標、per-item 表現、診斷與業務限制，再人工 promotion。

### 9.1 `to_numpy` 這一步的峰值與兩個觀測陷阱

**陷阱一：`nbytes` 會低報，觀測性在這裡是瞎的。** `src/recsys_tfb/io/extract.py:321` 的
`log_data_volume(logger, "extract_Xy.X", X)` 問的是 numpy「這張矩陣多大」，而 numpy 對 `object` 矩陣
**只算指標、不算指向的物件**。實測中 object 與 float64 兩種矩陣回報的數字完全一樣（皆 0.49 GiB @ 100k 列），
真實記憶體卻差 4.3 倍——生產規模上它會把 95.9 GiB 報成 22.4 GiB。**不要用這行 log 判斷記憶體是否安全。**

**陷阱二：非數值欄不只是慢和肥，它根本不合法。** LightGBM 拿到 object 矩陣後會嘗試
`np.asarray(mat, dtype=np.float32)`（`lightgbm/basic.py:192` 的 `_np2d_to_np1d`），碰到真的是字串的格子直接
`ValueError: could not convert string to float`。**記憶體不足只是先發生的症狀**，病根是非數值欄進了特徵集
（由 dataset 的 B6 閘擋，見 [`dataset.md` §8.1](dataset.md)）。

**修掉非數值欄是必要條件，不是充分條件。** 2026-07 那次事故的實測：移除文字欄後需求仍是 **54.7 GiB**，
而從 log 框出的機器上限落在 **48.3 GiB 與致死點之間**——很可能還是會死。若仍不足，後續選項（成本由低到高）：

| 做法 | `to_numpy` 時的峰值 | 代價 |
|---|---|---|
| 現況（有非數值欄） | ~142 GiB | — |
| 移除非數值欄 | ~54.7 GiB | 改 config、重建 dataset |
| ＋ 提早釋放 `pdf`、拿掉多餘的 `.copy()` | ~38.7 GiB | 改 `extract_Xy` 的取值順序（`extract.py:258` 的 `.copy()` 是多餘的：pandas 1.5.3 的 `pdf[feature_cols]` 已回傳獨立副本） |
| ＋ 矩陣改用 4 bytes 格子（`to_numpy(dtype=np.float32)`） | ~27.5 GiB | ⚠ int64 欄若有值 > 2²⁴ 會失真，套用前必須驗值域 |
| 讓 LightGBM 直接讀 Arrow，不經過 pandas／numpy | ~18.5 GiB | 需要 `cffi`（**不是** pyarrow 或 lightgbm 的相依套件，生產環境未必有） |
| 一次只讀一小段、邊讀邊餵（`lightgbm.Sequence`） | ~4 GiB | 需自寫約 25 行的 `ParquetSequence`；只需要 numpy |

最後一列的 `~4 GiB` 裡有 2.8 GiB 是 LightGBM 分箱後的資料集本身（要存下來的 `train.bin`），那是跑不掉的下限。

上表的數字是 2026-07 生產事故的實測與外推，**其推導與未證實之處**見
[2026-07 調查紀錄](../notes/2026-07-11-training-oom-investigation.md)。

## 10. 相關文件

- 上游資料切分、前處理與 model input：[`dataset.md`](dataset.md)
- 訓練後 test 評估與模型比較：[`evaluation.md`](evaluation.md)
- 模型上線後的批次排序：[`inference.md`](inference.md)
- 指標定義：[`../metrics/metrics.html`](../metrics/metrics.html)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、恢復與人工卡控設計背景：[`../design-principles.md`](../design-principles.md)
