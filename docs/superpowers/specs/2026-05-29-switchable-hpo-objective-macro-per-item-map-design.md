# 可切換的 HPO 優化目標：新增 macro per-item mAP

- 日期：2026-05-29
- 分支：`feat/hpo-macro-per-item-map`
- 狀態：設計已核可，待寫實作計畫

## 動機

Training pipeline 的 HPO（`tune_hyperparameters`）目前以 `compute_mean_ap` 為每個
trial 的打分目標：以 `(snap_date, cust_id)` 為 query、query 等權的 mAP。這隱含用
「正例頻率」加權產品——熱門產品的正例 row 多，主導了平均。

需求：讓 HPO 目標**可由 config 切換**，並新增一個與 evaluation pipeline 既有
per-item mAP 對齊的目標 `macro_per_item_map`，讓 22 個產品**等權**，避免冷門產品
被熱門產品淹沒。

## 語意定義

`macro_per_item_map` 重現 `evaluation.metrics_spark` 的
`macro_avg["by_item"]["map_attr@all"]`：

1. query 仍是 `(snap_date, cust_id)`；在每位客戶的 22 產品內依 score 降冪排序。
2. 每個 row 的 `prec_at_pos` = 該位置的累積精確度（`cum_rel / pos`）。
3. `ap_contrib@K` = `prec_at_pos * label * (pos <= K)`；本次 K 固定為 `all`
   （= n_products，完整 mAP，等同不截斷），故正例 row 的 contrib = `prec_at_pos`。
4. per-item `map_attr@all` = 對「該產品為正例的所有 row」取 `ap_contrib` 平均
   （row-equal-weight）。
5. macro 平均 = 對所有出現過的產品等權平均上述 per-item 值。

注意：這是 evaluation 既有定義（attribution），不是「把每個產品當 query 對客戶
排序算 AP」。

## 架構與元件

HPO 打分刻意走單機 numpy primitive（見 `metrics.py` 模組 docstring）：每個 trial
只為一個純量走一趟 Spark 是不可接受的 overhead。因此新目標也以 numpy 實作，與
Spark 版對拍驗證一致，而非在 trial 迴圈內呼叫 Spark。

### (a) `src/recsys_tfb/evaluation/metrics.py` — 新 numpy primitive

```
compute_macro_per_item_map(groups, items, y_true, y_score, k=None) -> float
```

- 與 `compute_ap` / `compute_mean_ap` 同一分層（純 numpy、HPO 專用）。
- 演算法沿用 `compute_mean_ap` 的 `np.lexsort((-y_score, groups))` + per-group
  walk（O(N log N)）：每個 group 算 `prec_at_pos`；對正例 row 收集
  `(item, contrib)`；最後用 `np.unique(items, return_inverse=True)` + `np.bincount`
  向量化做 per-item 平均，再 `np.mean()` over items。
- `k=None` ⇒ 不截斷（= all）。保留 `k` 參數供未來 mAP@K，本次僅用預設。
- tie-break：沿用 lexsort 穩定排序（與 `compute_mean_ap` 一致）。
- 邊界：空輸入 / 全無正例 ⇒ `0.0`。

### (b) `src/recsys_tfb/io/extract.py` — `extract_Xy_with_groups` 加 `with_items`

- 新增 keyword-only 旗標 `with_items: bool = False`。
- `with_items=True` 時於回傳尾端附加 `items`（`pdf[schema["item"]]` 原始產品值，
  per-row，與 X / y / groups 1:1 對齊）。
- 與既有 `with_weights` 獨立；refit 路徑用 `with_weights=True`，不受影響。

### (c) `src/recsys_tfb/pipelines/training/nodes.py` — 選擇器 + 接線

- 新增純函式（可單元測試）：

  ```
  _hpo_score(objective_name, groups, items, y_true, y_score) -> float
  ```

  - `"mean_ap"`            → `compute_mean_ap(groups, y_true, y_score)`
  - `"macro_per_item_map"` → `compute_macro_per_item_map(groups, items, y_true, y_score)`
  - 未知值 → fail-loud `ValueError`（訊息列出允許值）。
- `tune_hyperparameters`：
  - 讀 `training.hpo_objective`，code 預設 `"mean_ap"`（向後相容；key 不存在時不變）。
  - 迴圈開始前就驗證合法值（不合法即 raise，不拖到第一個 trial）。
  - 只有目標需要 items 時才以 `with_items=True` 取 `items_v`，否則維持三元組解包。
  - trial 內改呼叫 `_hpo_score(...)`。
  - `best_state` 的 `"mean_ap"` key 改為中性的 `"score"`；log 文字相應調整。

### (d) `conf/base/parameters_training.yaml`

- `training:` 下新增 `hpo_objective: macro_per_item_map`（生效預設）。
- 註解說明替代值 `mean_ap` 與兩者語意。

## 驗證 / 一致性

- enum 合法性：node 層 fail-loud，**不**進 `consistency.py`。理由：`consistency.py`
  的 A1–A6 範圍是 item-set / column-role 不變量；`hpo_objective` 是 training-only
  的純列舉，不屬該範圍。
- `hpo_objective` 落在被 hash 的 `training:` 區塊（versioning.py 的
  `_model_version_payload` 取 training 區塊 MINUS 純 logging/threading 旋鈕），
  故此次 commit 造成**一次性 model_version bump**（性質同 search_space 遷移）。

## 測試（TDD）

1. `tests/test_evaluation/test_metrics.py`：`compute_macro_per_item_map`
   - 手算案例：2 客戶 × 3 產品，期望 `(1.0 + 1.0 + 2/3)/3 = 8/9`。
   - `k=1` 截斷案例：期望 `(1.0 + 1.0 + 0.0)/3 = 2/3`。
   - 跳過無正例 group；全無正例 ⇒ `0.0`；空輸入 ⇒ `0.0`；items 為字串可運作。
2. 對拍測試（`tests/test_evaluation/test_metrics_spark.py`，重用 spark fixture，
   資料極小）：同一筆資料下 numpy primitive == `compute_all_metrics` 的
   `macro_avg["by_item"]["map_attr@all"]`（用相異 score 避免 tie-order 差異）。
3. `tests/test_io/test_extract.py`：`with_items=True` 回傳 items 對齊、長度一致、值正確。
4. `tests/test_pipelines/test_training/test_nodes.py`：`_hpo_score` 三分支
   （兩有效目標各委派正確、未知值 raise `ValueError`）。

## 不做（YAGNI）

- 不做 mAP@K 的 config（K 固定 all，只保留參數位）。
- 不改 evaluation pipeline、不動 refit 路徑邏輯、不做無關重構。
