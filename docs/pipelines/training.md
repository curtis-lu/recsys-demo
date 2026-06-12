# training pipeline

> 用各 split 的 `*_model_input` 訓練**一個共用** LightGBM 模型：cache → 調參 →（校準）→ 寫 test 預測 ＋ 診斷。
> DAG pipeline；節點接線與產物見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 一般訓練（取 latest base/train 版本）
python -m recsys_tfb training --env local

# 指定上游資料版本
python -m recsys_tfb training --base-dataset-version <base_v> --train-variant <train_v>

# 啟用校準時挑 calibration 版本（需 training.calibration.enabled=true）
python -m recsys_tfb training --calibration-variant <cal_v>

# 改了下游 node、跳過昂貴 HPO 接續（缺料自動補跑上游）
python -m recsys_tfb training --from-node finalize_model

# 只重跑單一 node（如校準）
python -m recsys_tfb training --only-node calibrate_model

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb training --from-node finalize_model --dry-run
python -m recsys_tfb training --list-nodes
```

> 版本旗標省略則取 latest；`--calibration-variant` 僅在 `training.calibration.enabled=true` 時生效。`--from-node` / `--only-node` 互斥；切片機制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途

`training` 讀 dataset 產的 `*_model_input`，訓出單一模型（pointwise 或 learning-to-rank），可選做機率校準，並對 test set 評分供 evaluation 情境 1 使用。

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
- **HPO 崩潰復原** `hpo_checkpointing`（頂層，預設 `true`）：HPO 跑到一半 crash 時，重跑只補跑剩餘 trial、零重訓拿回最佳模型（持久化 Optuna study ＋ 每次刷新最佳就 checkpoint，落 `data/models/_hpo/<search_id>/`）。只改 `n_trials` 可接續／延長；強制重來用 `training --fresh-hpo`。機制、`search_id` 失效規則與清理見 [`../operations/hpo-resume.md`](../operations/hpo-resume.md)。這是 HPO **跑到一半**的接續；整個 `tune_hyperparameters` 跑完後從 `finalize_model` 接續是另一層，見 [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

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
- 指定上游版本：見開頭「指令與選項」（`--base-dataset-version` / `--train-variant` / `--calibration-variant`，預設取最新）。
- **上線是人工的**：用 `scripts/promote_model.py` 把某個 `model_version` 設為 `best`（不自動），`inference` 預設用 `best`（README §3 Q5）。

## 接下來

- test 預測怎麼被評估 → [`evaluation.md`](evaluation.md)
- 指標怎麼算 → [`../metrics.html`](../metrics.html)
- 各表 schema / 版本層 → [`../data-lineage.html`](../data-lineage.html)
