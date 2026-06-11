# inference pipeline

> 用上線模型對評分母體評分、每個 query group 內排名，通過驗證閘後發布到 production 表。
> DAG pipeline；節點接線與每張表的 schema 見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 用上線模型（best）對 parameters_inference.yaml 的 snap_dates 評分、排名、發布
python -m recsys_tfb inference --env local

# 指定模型版本（預設 best symlink）
python -m recsys_tfb inference --model-version <model_version>

# 改了下游 node、從某 node 接續（缺料自動補跑上游）
python -m recsys_tfb inference --from-node rank_predictions

# 只重跑單一 node（如驗證閘）
python -m recsys_tfb inference --only-node validate_predictions

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb inference --from-node predict_scores --dry-run
python -m recsys_tfb inference --list-nodes
```

> 評分母體（`snap_dates` / `products`）在 `parameters_inference.yaml`；`validate_predictions` 的 6 項 sanity check 通過後才 `publish_predictions` 寫 production 表。`--from-node` / `--only-node` 互斥；切片四旗標機制與限制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途

`inference` 載入上線模型（預設 `best`），組出評分母體（每個 (time, entity) 的候選 `item`），用訓練時的前處理編碼後評分，於每個 query group 內依 `score` 排名，最後經驗證閘把已驗證結果發布到 production 表（示例名 `ranked_predictions`）。模型版本決定要回溯哪個 dataset／前處理版本，由模型 manifest 自動對齊。

> 預設讀 `best` 那一版（人工 `scripts/promote_model.py` 升上來的）；`evaluation`（情境 2）之後讀回此 production 表做上線監控。

## 節點流程

| node | 輸入 | 主要功能 | 產出 |
|---|---|---|---|
| `build_scoring_dataset` | `feature_table` | 組出要評分的 (time, entity) × 候選 `item` 母體 | `scoring_dataset` |
| `apply_preprocessor` | `scoring_dataset`、`preprocessor` | 用訓練時的前處理編碼 | `X_score` |
| `predict_scores` | `model`、`X_score` | 模型評分 | `score_table` |
| `rank_predictions` | `score_table` | 每個 query group 內依 `score` 由高到低排名 | `ranked_staging` |
| `validate_predictions` | `ranked_staging`、`scoring_dataset` | 6 項 sanity check（筆數／分數範圍／完整性／排名一致…），失敗即中止整批 | `validated_predictions`（中間態） |
| `publish_predictions` | `validated_predictions` | 驗證通過後才把結果發布到 production 表（**唯一一次 production 寫入**） | `ranked_predictions` |

> **query group** ＝ 同一個 (time, entity) 下所有候選 `item`（見 README §0）；排名在組內進行。`validate_predictions` → `publish_predictions` 是 staging gate：未通過驗證不發布，避免半截／異常結果污染下游。

## 關鍵設定（`conf/base/parameters_inference.yaml`）

- `snap_dates`：要評分的時間切點清單；輸出以 `model_version` ＋ `snap_date` 分區。
- `products`：評分母體納入的 `item` 集合（須與 `schema.categorical_values.<item>` 一致，否則被一致性閘擋，見 README §4）。
- `use_calibration`：是否套用校準後模型輸出。

## 重跑語意

- `--model-version`：指定要用哪一版模型評分（預設 `best` symlink）；該版的 dataset／前處理版本由模型 manifest 回溯對齊。
- `publish_predictions` 對每個 `model_version` ＋ `snap_date` partition 整個覆寫——重跑同一版同一天 ＝ 覆寫，不是 append。
- 切片四旗標（`--from-node` / `--only-node` / `--dry-run` / `--list-nodes`）語意與限制見 [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 接下來

- 各表 schema / 版本層 / 範例 → [`../data-lineage.html`](../data-lineage.html)
- 發布後怎麼監控評估 → [`evaluation.md`](evaluation.md)
- 一致性閘的所有錯誤訊息 → README §4
