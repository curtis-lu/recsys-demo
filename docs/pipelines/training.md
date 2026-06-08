# training pipeline

> 用各 split 的 `*_model_input` 訓練**一個共用** LightGBM 模型：cache → 調參 →（校準）→ 寫 test 預測 ＋ 診斷。
> DAG pipeline；節點接線與產物見 [`../data-lineage.html`](../data-lineage.html)。

## 用途

`training` 讀 dataset 產的 `*_model_input`，訓出單一模型（pointwise 或 learning-to-rank），可選做機率校準，並對 test set 評分供 evaluation 情境 1 使用。

```bash
python -m recsys_tfb training --env local
```

> 訓練是 **driver 上的單機 LightGBM**，不靠分散式 cluster——所以模型與快取都駐留 driver 本機檔案系統（見「產物」）。

## 節點流程

| 階段 | 節點 | 做什麼 |
|---|---|---|
| 快取 | `cache_{train,train_dev,val,test[,calibration]}_model_input` | 把各 split 從 Hive `copyToLocal` 成 driver-local parquet handle（cache 不經 catalog，重跑 skip-if-exists） |
| 準備 | `prepare_lgb_train_inputs` | 把 train / train_dev 建成 `lgb.Dataset` binary |
| 權重 | `persist_sample_weight_report` | 產出 sample_weight 套用報告（觀測性） |
| 調參 | `tune_hyperparameters` | Optuna HPO：每個 trial 用 train 訓、train_dev early-stopping、在 **val** 上算排序分數；選分數最佳超參 |
| 訓練 | `finalize_model` | 用最佳超參產出最終 booster |
| 校準 | `calibrate_model`（可選） | 用 calibration split fit 機率校準，包裝成最終 `model` |
| 預測 | `predict_and_write_test_predictions` | 對 test set 評分、chunked 寫 Hive `training_eval_predictions` |
| 評估 | `compute_test_mAP_spark` | 讀回 `training_eval_predictions` 算 test 排序指標 → `evaluation_results.json` |
| 診斷 | `compute_feature_statistics` / `compute_feature_importance` / `compute_shap_diagnostics` | 特徵統計 / 原生 importance / SHAP |
| 記錄 | `log_experiment` | 把模型、超參、指標、診斷記到 MLflow |

> `train_dev` 與 `val` 的角色差別（單次訓練的 early-stopping vs 跨試驗挑超參）見 README §3 Q2。

## 關鍵設定（`conf/base/parameters_training.yaml`）

**訓練目標** `algorithm_params.objective`（你從 binary 過來最關鍵的決策）：

| objective | 範式 | 怎麼學 | `score` 能當機率？ | 何時選 |
|---|---|---|---|---|
| `binary`（預設） | pointwise | 把每個 (entity, item) 當獨立樣本預測 | 校準後可（見下） | 最穩、最接近你熟的分類流程；先從這開始 |
| `lambdarank` / `rank_xendcg` | learning-to-rank | 直接優化 query group **組內排序** | 否（是排序用相對分） | 想讓排序指標更好、且願意處理 LTR 設定 |

> query group ＝ 同一個 (time, entity) 下所有候選 item（見 README §0）。用 LTR 時 `metric` 必須是排序指標（`ndcg` / `map`；留空自動帶 `ndcg`），且 query group（`schema.time + entity`）要有定義，否則被一致性閘擋（README §4）。

其餘設定：

- **HPO** `search_space`：宣告式 ParamSpec 清單（每項 `name` ＋ `type` ∈ {int, float, categorical}…）。HPO 在 **val** 上用哪個排序分數選超參由 `hpo_objective` 設定（如 per-item mAP）；指標定義見 [`../metrics.html`](../metrics.html)。
- **校準** `training.calibration.enabled`（＋ `method`，如 `sigmoid`）：可選。**為什麼要校準**：LTR 的 `score` 是排序用相對分、不是機率；即使 `binary` 目標，LightGBM 原始輸出也未必是校準過的機率。要把 `score` 當機率解讀（算期望值、跨期比較）時才需要（README §3 Q4）。校準還需 dataset 端 `enable_calibration: true` 產出 calibration split。
- **樣本權重** `sample_weight_keys` ＋ `sample_weights`：key 是各維度值用 `|` 串起來；維度欄必須是 train model_input 裡實際有的欄（identity 欄、label、`carry_columns`、類別欄），否則被一致性閘擋。

## Two-stage stacking（`model_structure`）

### 概覽

`training.model_structure` 決定訓練結構：

| 值 | 說明 | 預設 |
|---|---|---|
| `shared` | **現況**：一個共用 LightGBM 模型，pointwise 或 LTR 直接對所有 item 共訓 | ✓ |
| `per_group_plus_rank` | **Two-stage stacking**：Stage-1 per-grouping point-wise 模型 + Stage-2 一個 LTR（lambdarank） | 進階可選 |

切換 `model_structure`、或修改 `stage1`/`stage2` 任一設定，都會 bump `model_version`（版本語意見「版本語意」節）。

### 動機：多 item 冷熱門不平衡

通用原理：一個 shared 模型同時學所有 item，冷門 item 正類筆數少，容易被熱門 item 的統計強度淹沒，難訓。把建模拆成「先各自打分、再學跨群排序」的 stacking，是處理異質群體的標準手段。套到本框架：示例規模（~10M entity × 22 item）下單一 shared model 冷熱門不平衡明顯；`per_group_plus_rank` 提供一條結構性的實驗路徑，評估工具是既有的 `macro_per_item_map`（每 item 等權，正是量冷門 item 有無改善的對的尺規）。

### Config

```yaml
training:
  model_structure: shared          # shared（現況，預設）| per_group_plus_rank
  stage1:                          # 僅 per_group_plus_rank 生效
    grouping: category             # item | category
    objective: binary
    metric: binary_logloss
    n_folds: 5                     # OOF 折數；折鍵 = entity 雜湊互斥
  stage2:
    objective: lambdarank
    metric: ndcg
    # inputs 固定 pointwise（自身分數 + entity 特徵 + grouping id）；跨 item 相對特徵為 future
```

**`product_categories`（頂層單一真實來源）**：training Stage-1 grouping 與 evaluation 大類 collapse 共用同一份 mapping。放頂層（非 `schema.*`）是刻意的：`schema` 影響 `base_dataset_version`，放頂層讓修改 mapping 只 bust `model_version`（且僅在 `grouping: category` 時），不會重建 dataset。

```yaml
product_categories:
  mapping:
    fund: [...]
    exchange: [...]
    ccard: [...]
  unmapped: singleton              # 未列入 mapping 的 item 各自成 singleton 大類
```

**`stage1.grouping`**：

| 值 | 效果 |
|---|---|
| `category` | 每個 `product_categories` 大類一個 point-wise share 模型（推薦預設）；singleton 大類等價 per-item |
| `item` | 每個 `schema.item` 值一個獨立 binary 模型（grouping 的退化極端；adapter 內部一套碼） |

### OOF cross-fitting 資料流

Two-stage 的核心保證：Stage-2 的訓練特徵（Stage-1 預測分數）必須 leakage-clean，不能來自看過同一 entity 的 Stage-1 模型。解法是 out-of-fold（OOF）K 折 cross-fitting，折鍵為 entity 雜湊互斥切。

```
[train 資料]  ── K 折（entity 雜湊互斥切，n_folds 控制）──┐
   每折 k：
     在 train\fold_k 訓各 grouping Stage-1 模型
        └── early-stop valid = train_dev（entity-disjoint，固定）
     對 fold_k 打分
        ▼
OOF 預測（覆蓋整個 train；每列分數都來自「未見過該 entity」的 Stage-1）
        ▼
Stage-1 在整個 train refit（供 Stage-2 valid/HPO 打分 + inference 用）
        ▼
Stage-2（lambdarank，query = entity）訓練：
   特徵 = [自身 grouping 的 OOF 分數, entity 特徵, (可選) grouping id]
   early-stop valid = train_dev（用 refit Stage-1 打分，train_dev entity 對 train 互斥→乾淨）
   HPO 目標 = val（另一 snapshot，用 refit Stage-1 打分）
        ▼
最終 artifact（data/models/<model_version>/）：
   {每 grouping 一個 Stage-1 refit-on-full-train booster}
   + {1 個 Stage-2 booster}
   + grouping 表
   + model_meta.json
```

> **Leakage 不變量（核心保證）**：Stage-2 訓練特徵（train 的 OOF 分數）永不來自看過該 entity 的 Stage-1。

### Split 語義（與單模型路徑一致）

| split | 在 per_group_plus_rank 中的角色 |
|---|---|
| `train` | Stage-1 K 折 fit 的資料池；Stage-1 refit-on-full-train 的訓練集；OOF 預測覆蓋此集合 |
| `train_dev` | Stage-1 每折 early-stop valid；Stage-2 early-stop valid（用 refit Stage-1 打分） |
| `val` | HPO 目標（用 refit Stage-1 打分打分；另一 snapshot，entity 不在 train 內）|
| `test` | held-out，最終評估用 |

### Inference 不動

`CompositeModelAdapter` 對外仍實作 `ModelAdapter` ABC（`predict / save / load / feature_importance / log_to_mlflow`）。`predict(X)` 做的事：每列路由到所屬 grouping 的 refit Stage-1 booster 打分，組 `[分數, entity 特徵]` 過 Stage-2，回最終 scalar。

推論仍是 **pointwise 打分**，故 inference pipeline 的 `predict_scores` 仍按 `(snap_date, item)` chunk 控記憶體、`rank_predictions` / validation / evaluation **全部不動**。

### 節點流程（per_group_plus_rank）

`training/pipeline.py` 依 `model_structure` 分支：

- `shared`（預設）→ 現有 `prepare_lgb_train_inputs → tune_hyperparameters → finalize_model` 不變。
- `per_group_plus_rank` → 以單一 `train_composite_model` node 取代上述三節點鏈：

| node | 做什麼 |
|---|---|
| `train_composite_model` | 編排 OOF K-fold（entity 雜湊切折）→ 各 grouping Stage-1 逐折訓練 + 對留出折打分 → 組 OOF 預測 → Stage-1 refit-on-full-train → Stage-2 lambdarank 訓練；組出 `CompositeModelAdapter`、save 到 version 目錄 |

> **為什麼不走既有 numpy `train()` 介面**：`ModelAdapter.train(X_train, y_train, ...)` 是純 numpy 介面，拿不到 entity id，無法表達「依 entity 互斥切折 + 產 OOF」。因此 composite 的訓練走新節點 `train_composite_model`，不進舊的 tune_hyperparameters → finalize_model 鏈；composite 的 `train` / `prepare_train_inputs` 明確 `raise NotImplementedError`（提示「不該被單模型路徑呼叫」）。

下游節點（`calibrate_model` 之後的預測、診斷、MLflow 記錄）不變，因為 composite 已完整實作 `predict / save / load / feature_importance`。

### 校準（composite 預設關閉）

Lambda ranker 輸出是排序用相對分、非機率，composite 預設不做機率校準（對 per-entity mAP 無影響）。一致性閘（A15）會擋住 `per_group_plus_rank` + `calibration.enabled: true` 的組合。

未來若需在 composite 之上加校準，現有 `CalibratedModelAdapter` 已是「包住任一 `ModelAdapter`」的 wrapper，直接包住 `CompositeModelAdapter` 即可，不需改 composite 內部。

### Diagnostics（v1 限制）

composite 下 SHAP 跳過（N+1 booster，per-submodel SHAP 為 future）；`feature_importance` 落在 Stage-2 booster。屬報表豐富度、非正確性。

### 一致性閘（A15）

`core/consistency.py` 新增 A15 predicate，於 CLI 啟動時一次驗：

- `model_structure` ∈ {`shared`, `per_group_plus_rank`}。
- `per_group_plus_rank` 時：`product_categories.mapping` 必須覆蓋 `schema.categorical_values` 中所有 item 值（或 `unmapped: singleton` 吸收）；`stage2.objective` 必須是 ranking objective；`calibration.enabled` 必須為 `false`。

違反任一條，啟動時即 `ConfigConsistencyError`，訊息可搜尋片段見 §4「設定一致性閘」。

### 未來展望（v1 不做）

- 跨 item 相對特徵（會打破 per-item inference chunking）。
- small-group 最小門檻自動併回 shared。
- 逐子模型完整 HPO。
- composite SHAP 全套診斷。

## 產物（driver-local，除 1 張 Hive）

| 產物 | 位置 / 型別 |
|---|---|
| `model`（model.txt） | `data/models/<model_version>/model.txt`（driver-local；Python `open()` 寫，不認 `hdfs://`） |
| `best_params` | `…/best_params.json` |
| `evaluation_results` | `…/evaluation_results.json`（test mAP；**training 產的**，非 evaluation pipeline） |
| 診斷 ×3 | `…/diagnostics/*.json` |
| `training_eval_predictions` | Hive 表（唯一寫 Hive 的產物；供 evaluation 情境 1 讀回） |

## 版本語意

- `model_version` ＝ hash（**model-defining** 的 training 子集 ＋ `base_dataset_version` ＋ `train_variant_id`〔＋ `calibration_variant_id`〕）。純 logging / threading 的 `algorithm_params` 鍵被排除，改它們不會翻 `model_version`。
- 指定上游：`--base-dataset-version`、`--train-variant`（預設取最新）。
- **上線是人工的**：用 `scripts/promote_model.py` 把某個 `model_version` 設為 `best`（不自動），`inference` 預設用 `best`（README §3 Q5）。

## 接下來

- test 預測怎麼被評估 → [`evaluation.md`](evaluation.md)
- 指標怎麼算 → [`../metrics.html`](../metrics.html)
- 各表 schema / 版本層 → [`../data-lineage.html`](../data-lineage.html)
