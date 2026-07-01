# Training Diagnostics 記憶體重構 — 設計

**日期**:2026-07-01
**分支**:`feat/diag-mem-refactor`(off `main` @ c15514e)
**前置**:P1 per-item SHAP 強化(PR #92,已 merged)

---

## 1. 背景與動機

training diagnostics 的兩個節點目前對 driver 用 pandas/Arrow **全量物化，再抽小樣本**——在 target env(driver 128GB、CPU-only、與 model/SHAP 共用記憶體)的真實規模下是浪費,規模一旦成長會直接 OOM。

實際規模(使用者提供):**train ≈ 數百萬列(train+dev 合計 ~500萬)、test ≈ 50萬列**,特徵 ~1500 欄。

| 節點 | 現況 | driver 峰值(估) |
|---|---|---|
| `compute_feature_statistics`(讀 train) | `pq.read_table(columns=feature_cols)` 全量讀,之後才 `.take(idx)` 抽 `sample_rows`(預設 500k) | 全 train × 1500 ≈ **數十 GB**(例 400萬×1500×8B≈48GB)+ take 複本 → 本 PR 絕對佔用最大處 |
| `compute_shap_diagnostics`(讀 test) | `test_parquet_handle.to_pandas()` 全量讀,之後才分層抽 `sample_rows`(預設 2000) | 全 test × 1500 ≈ **~6GB**,只為 2000 列(~24MB);最誇張的 anti-pattern,規模長大即致命 |

## 2. 目標

消除上述「先全量載入再取小樣本」的 driver 記憶體浪費,**維持 Kedro 風格節點邊界**,且**診斷輸出逐位元不變**(behaviour-preserving 記憶體重構)。

## 3. 關鍵事實(設計據以成立)

所有 `*_model_input` cache 都是 hive 分區的 `…/snap_date=…/prod_name=…/*.parquet`(`pipelines/training/nodes.py` `_populate_cache_from_hive`)。因此 `item`(prod_name)與 `time`(snap_date)是**分區鍵**,可用 `pyarrow.dataset` 從分區樹 + parquet metadata 驅動抽樣,不必物化全部資料列。

`item`(prod_name)同時也在 `preprocessor["feature_columns"]` 內(是模型使用的 identity categorical),`_pdf_to_X` 會對它做 deferred encode。讀取時經 hive 分區重建即可還原此欄。

## 4. 設計:「數 → 算索引 → 只取那些列」

以 `pyarrow.dataset` 取代「全載 → 再抽」。兩節點共用同一套 I/O 原語。

### 4.1 feature_stats(train,均勻抽樣)
1. `n = count_rows(path)`(metadata,零資料掃描)。
2. `n > sample_rows` 時:`idx = np.sort(np.random.RandomState(42).choice(n, sample_rows, replace=False))` — **與現行完全相同的 idx**。
3. `pdf = take_rows(path, idx, columns=feature_cols)`;`n <= sample_rows` 時直接讀全部(已 bounded)。
4. 逐特徵統計邏輯不變。

峰值:數十 GB → `sample_rows × 1500`(~6GB)。

### 4.2 SHAP(test,分層抽樣)
1. `item_values = read_column(path, item_col)` — **只讀 item 分區欄**(N×1,50萬下幾十 MB)。
2. budget guard 依 `n_trees` 算 `eff_sample`(不需資料,順序不變)。
3. `idx = _stratified_item_sample(item_values, eff_sample, min_per_item, seed=42)` — **沿用現有分層抽樣邏輯與 seed**。
4. `sample_pdf = take_rows(path, idx, columns=feature_cols + [label_col])`(item_col 已在 feature_cols 內)。
5. 其後 `_pdf_to_X` → 單次 `feature_attributions` → global / per_item / examples / beeswarm 全部不變。

峰值:全 test × 1500 → `item 欄(N×1)` + `sample × 1500`。

> 為何 50萬 下選「只讀 item 欄」而非「純從 fragment metadata 推 index(連 item 欄都不讀)」:後者可省下 N×1 那一欄,但需重製抽樣器的 RNG/順序、風險高;在 50萬 下 N×1 只是幾十 MB,YAGNI。若日後 test 規模逼近 predict 註解的 220M(item 欄約 7GB,仍 fit),再視需要做 metadata-only 精修。

## 5. 檔案結構與職責(Kedro:I/O ↔ 抽樣策略 ↔ 運算 分離)

- **新增 `diagnostics/data_access.py`** — 唯一碰 `pyarrow.dataset` 的 I/O 層:
  - `count_rows(path) -> int`
  - `read_column(path, col) -> np.ndarray`(供分層抽樣;回傳全 N 列該欄,dataset 順序)
  - `take_rows(path, indices, columns) -> pd.DataFrame`(以**有序 fragment / row-group 迭代**收集 sorted indices,記憶體 bound 在 output + 單一 row-group,不依賴猜測 `pyarrow.dataset.Dataset.take` 內部行為)
- **`diagnostics/sampling.py`** — `_stratified_item_sample` 由吃 `pdf, item_col` 改成吃 `item_values` 陣列(內部 `pd.unique` / `np.where` / `rng.choice` 邏輯與 seed 不變)。
- **`diagnostics/feature_stats.py`** — 改用 `data_access`;移除全量 `pq.read_table`。
- **`diagnostics/shap_per_item.py`** — 改用 `data_access` + 新 `_stratified_item_sample` 簽名;移除 `to_pandas()` 全載。

`ParquetHandle`(#1 god-node,143 edges)介面**不動**——投影/抽樣只在 `data_access` 內做。

## 6. 鐵則:診斷輸出逐位元不變(本 PR 驗收核心)

三處讀取(舊 `to_pandas` / 新 `read_column` / 新 `take_rows`)皆源自**同一 pyarrow fragment 順序**(path-sorted,與 `pq.read_table` 一致);抽樣 idx 皆 `np.sort` 升序,故 `take(idx)` 取得的列與順序 == 舊 `pdf.iloc[idx]`;seed 與選列/選欄邏輯全沿用。因此 `shap_diagnostics` 與 `feature_statistics` 的輸出 dict **byte-for-byte 相同**。現有 24 個測試即最強回歸網。

**Determinism 要求**:`take_rows` / `read_column` 必須以**確定性有序**方式讀取(fragment 依 path 排序、row-group 依序;避免 threading 造成 batch 亂序)。由等價測試(§8.2)把關。

## 7. 觀測性(避免 materialize 為主,log 為輔)

沿用既有 `core.logging.log_step` / `log_data_volume`:每次抽樣**前**記 `n_total`(count_rows)、**後**記 `n_sampled / n_cols / 估算 bytes`。目的:OOM 風險可單從 log 判讀,對齊「抽樣前後 log rows/cols/bytes」原則。優先「不 materialize」,`del` 只作降峰輔助。

## 8. 測試策略(純 python,秒級,不碰 Spark)

1. **現有 24 測試**:回歸網,必須全綠(= 證明輸出不變)。
2. **等價 / 分區 fixture**:新增 hive-partitioned 測資(`pyarrow.dataset.write_dataset(partitioning=["snap_date","prod_name"])`),驗證:
   - prod_name **從分區重建**後 `compute_shap_diagnostics` 正常運作;
   - 輸出與等價 flat fixture 相同(現有 SHAP 測試用 flat parquet,未覆蓋分區路徑,此條補上)。
3. **行為測試(把重構意圖釘進測試)**:
   - SHAP 路徑**不再呼叫** `ParquetHandle.to_pandas()`(spy/monkeypatch 斷言);
   - feature_stats **不再**做全量 `pq.read_table`(讀入列數受 `sample_rows` bound);
   - `n > sample_rows` 時 `len(sample_pdf) <= eff_sample`。
4. **`data_access` 單元測試**:`count_rows` / `read_column`(flat 與 partitioned 皆測)/ `take_rows`(sorted indices、跨 row-group、欄投影)。

## 9. 明確排除(僅風險註記,本 PR 不動)

- **predict 列舉分區**(`predict_and_write_test_predictions` 物化 220M×2):真實 driver 浪費,但屬 predict node → **另開 PR**。
- **`ParquetHandle.to_pandas(columns=)` 介面**:feature_cols≈整表,投影只省幾欄,價值低。
- **`_pdf_to_X` 的 `.copy()`**:微量;copy 為 encode 不汙染來源。
- **sample_weight diagnostics**:已只讀 weight-key 窄欄 + distinct。
- **`refit_on_full` concat**:屬訓練主流程(使用者明示不改),僅記風險:diagnostics 不應新增同級 peak。
- **`examples` 區塊**:非記憶體問題(~32例×1500≈微量),且其正規後繼是 P2 的 `cases/<item>/{TP,FP,FN,TN}` 案例檢視 → 留待 P2 用 `cases/` 取代時一併收掉,本 PR 維持輸出不變。

## 10. 後續(非本 PR)

- **P2**:TP/FP/FN/TN(top@1)× 最高/最低分的單列 SHAP 案例檢視(`cases/<item>/` + manifest),**建於本 PR 的 `data_access` bounded-read 之上**,並在該 PR 收掉 `examples`。
- predict 列舉分區(見 §9)。
