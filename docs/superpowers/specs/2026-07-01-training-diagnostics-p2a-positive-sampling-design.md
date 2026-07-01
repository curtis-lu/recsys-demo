# Training Diagnostics P2a — 正例 profile 針對正樣本抽樣 設計

**日期**:2026-07-01
**分支**:`feat/diag-p2a-positive-sampling`(off `main` @ ac281a0,含 #93 的 `data_access`)
**前置**:P1(#92 merged)、記憶體重構(#93 merged)
**後續**:P2b(象限機制:Spark 選樣節點 + per_quadrant 乙 + cases 圖 + manifest)—— 另 PR

---

## 1. 背景與目標

P1 的 per_item 有兩個 profile:`top_features`(全列)與 `top_features_positive`(只用 label==1 的 adopter 列,回答「成功樣本靠什麼特徵被推高」)。目前 `top_features_positive` 是**從 item 分層的全域樣本裡撈殘存的正例**算的;但辦卡率 ~2%,隨機抽 ~120 列只會有 ~2-3 個正例 → 遠低於 `positive_min_rows`(20)→ 大多數 item 的正例 profile 是 `null`(§P1 spec §5.6 早已預告此 flakiness)。

**目標**:把 `top_features_positive` 改成**針對正樣本抽樣**(直接對 label==1、依 item 分層抽),讓成功樣本 profile 的 coverage 穩定;**與全域樣本解耦**(依目的分開抽樣)。純 driver、記憶體安全、不需 rank/Spark。

## 2. 範圍

- **只改** `compute_shap_diagnostics`(`shap_per_item.py`)中 per_item 的 `top_features_positive` / `n_positive` / `positive_low_coverage` 三欄的**來源樣本**,加 `sampling.py` 一個新抽樣函式,加一個 config 鍵。
- **不改**(維持 #93 後的行為,逐位元不變):`global`、per_item 的 `top_features`(全列)、`n_sampled`、`score_*`、`low_coverage`、`divergence_from_global`、`idiosyncratic_features`、`item_idiosyncrasy`、`examples`、beeswarm PNG。這些續用既有 item 分層樣本(sample A)。
- **不在本 PR**:象限 / per_quadrant / cases / Spark 選樣節點(全在 P2b)。

## 3. 設計

### 3.1 新抽樣函式(`sampling.py`)
```python
def _positive_item_sample(item_values, label_values, per_item, seed):
    """只在 label==1 的列中、依 item 分層抽樣（每 item 至多 per_item,不足全取）。
    回傳選中的 positional indices（升序,對齊 dataset 順序）。"""
```
邏輯對齊既有 `_stratified_item_sample`,但候選池限縮為 `label_values == 1` 的位置;`pd.unique` 決定 item 順序、`rng.choice(seed)` 抽樣。

### 3.2 `compute_shap_diagnostics` 改動(decoupled sample B)
在既有 sample A(item 分層 → global / per_item 全列 / divergence / beeswarm,**不動**)之外,新增 **sample B(正例目標)**:
1. `profile_positive=False` 或資料無 `label` 欄 → 完全跳過 sample B(不多跑 SHAP);per_item 正例三欄退回 `null` / `False`(沿用既有語意)。
2. 否則:
   - `all_labels = data_access.read_column(path, label_col)`(全 N × 1,便宜);`item_values`(sample A 已讀,重用)。
   - `pos_idx = _positive_item_sample(item_values, all_labels, positive_sample_per_item, seed=42)`。
   - `pos_pdf = data_access.take_rows(path, pos_idx, columns=take_cols)`(take_cols 同 sample A:feature_cols[+item_col if 不在])。
   - `X_pos = _pdf_to_X(pos_pdf, …)`;`shap_pos = feature_attributions(model, X_pos, feature_cols)`(**第 2 次 SHAP,樣本小**)。
   - 依 `pos_pdf[item_col]` 分群:每 item 若正例數 `>= positive_min_rows` → `top_features_positive = _signed_profile(shap_pos[item_mask], …)`、`positive_low_coverage=False`;否則 `null` / `True`。`n_positive` = 該 item 在 sample B 的正例數。
3. per_item 組裝時,`top_features_positive` / `n_positive` / `positive_low_coverage` 取自 sample B 的結果;其餘欄位仍取自 sample A。

### 3.3 config 新鍵(`diagnostics.shap` top-level,不動 model_version)
```yaml
positive_sample_per_item: 30    # 正例 profile 每 item 目標抽樣數(新)
# positive_min_rows: 20         # 續用:sample B 中正例 < 此 → positive_low_coverage（現在代表「此 item 真的少 adopter」,語意更實）
```

## 4. 記憶體與效能

- sample B 只讀 `label` 全欄(N×1)+ `take_rows` 抽中的正例列(~`positive_sample_per_item × n_items`,小)。**無全表物化**。
- 多一次 SHAP,但 sample B 很小(數百列),成本溫和;對齊使用者「依目的解耦抽樣,除非很耗效能」的原則。
- 觀測性:沿用 `log_data_volume` 記 sample B 的 rows/cols/bytes。

## 5. 不變式

`global` / per_item 全列 / divergence / examples / beeswarm 續用 sample A、邏輯不變 → **這些輸出逐位元不變**(#93 後的既有 24 測試守著)。**唯一改變**:per_item 的正例三欄(來源改 sample B、coverage 變穩)。

## 6. 測試

1. `_positive_item_sample`:只抽到 label==1 的位置;每 item ≤ per_item、不足全取;升序/唯一/determinism(新 `test_diagnostics_sampling.py` 案例)。
2. 行為:某 item 全域樣本裡正例 < positive_min_rows、但針對正樣本抽後 `top_features_positive` **非 null**(coverage 修好);某 item 真的少 adopter → 仍 `positive_low_coverage=True`。
3. `profile_positive=False` / 無 label 欄 → 不跑 sample B(spy 斷言不呼叫第二次 attribution),正例三欄為 null/False。
4. 回歸:`global` / per_item `top_features`(全列)/ divergence / examples 與改動前**逐位元相同**(既有測試維持綠;若某測試斷言舊的 flaky 正例值,更新為新語意,不弱化其他斷言)。
5. 記憶體行為:sample B 讀入列數 ≤ `positive_sample_per_item × n_items`(spy `take_rows` 的 indices 長度)。

## 7. 明確排除

象限 / per_quadrant(乙)/ cases 圖 / manifest / Spark 選樣節點 → **P2b**。global/per_item 全列抽樣、examples、`ParquetHandle` 介面 → 不動。
