# Training Diagnostics P2b-1 — 象限選樣 + per_quadrant 聚合 profile 設計

**日期**:2026-07-01
**分支**:`feat/diag-p2b-quadrant-cases`(off `main` @ 479399f,含 #92/#93/#94)
**前置**:P1(#92)、記憶體重構(#93)、P2a 正例抽樣(#94)全 merged
**後續**:P2b-2(cases 圖 + manifest + 收 examples)—— 另 PR,建於本 PR 的選樣節點

---

## 1. 背景與目標

P2 的象限診斷(TP/FP/FN/TN)拆兩步交付。**P2b-1 先建骨幹**:用一個 Spark 選樣節點算象限、每 (item×象限) 抽樣,交給 pandas 節點跑 SHAP、產出 **per-(item×象限) 聚合 signed profile**(乙)。**先用較簡單的 JSON 輸出把「Spark 選樣 → SHAP-on-selected」這條新架構驗證起來**,案例圖/manifest 留 P2b-2。

**目標**:`shap_diagnostics.json` 新增 `per_quadrant` 區塊——每 item 對 TP/FP/FN/TN 各一個平均 signed profile(看「模型很有信心卻猜錯(FP)那群平均靠什麼特徵被騙」「漏掉的 adopter(FN)平均長怎樣」)。

## 2. 象限定義(top@1,spec §6.1 已定)

於每列 (snap_date, cust_id, prod_name):先在 (snap_date, cust_id) 內依 score 排 rank。
- **TP** = rank==1 ∧ label==1;**FP** = rank==1 ∧ label==0;**FN** = rank≥2 ∧ label==1;**TN** = rank≥2 ∧ label==0。

## 3. 架構:Spark 選樣節點 + pandas SHAP 節點

### 3.1 `select_shap_population`(新 Spark 節點,全 native,無 UDF)
輸入:`training_eval_predictions`(predict 已寫 Hive:snap,cust,prod,score,label)、`test_model_input`(Spark DF,有特徵)、`parameters`。
1. rank:`Window.partitionBy(time,entity).orderBy(F.col("score").desc())` → `row_number()`。
2. 象限:`rank/label` 以 `when/otherwise` 貼 `quadrant ∈ {TP,FP,FN,TN}`。
3. 每 (item×象限) 抽樣:`Window.partitionBy(item, quadrant).orderBy(F.crc32(F.concat_ws("|", time, entity...)))` → `row_number() <= quadrant_sample_per_cell`(**確定性**、無 UDF;沿用 repo 既有 crc32 sampling 慣例)。
4. join `test_model_input` on (time, entity, item) 取特徵。
5. `toPandas()` 回傳小族群(~`quadrant_sample_per_cell × 4 × n_items` 列 × 特徵 + `item`/`quadrant` 欄)。**heavy(rank/join)全在 executor,driver 只拿小 pandas(≈數十 MB)**。

### 3.2 `compute_quadrant_profiles`(新 pandas 節點,`diagnostics/shap_cases.py`)
輸入:`model`、選樣小 pandas、`preprocessor_view`、`parameters`。
- `X = _pdf_to_X(pdf, …)`;`shap = feature_attributions(model, X, feature_cols)`(單次 SHAP)。
- group by (item, quadrant):`_signed_profile(shap[mask], feature_cols, top_k)` → top signed features;`n_sampled`;`low_coverage`(< `quadrant_min_rows`)。
- 回傳 `{"<item>": {"<quadrant>": {"top_features":[…signed…], "n_sampled":int, "low_coverage":bool}}}`(空格子不出現在 dict)。

### 3.3 接線(`pipelines/training/pipeline.py`)
- 新節點 `select_shap_population`(inputs `["training_eval_predictions","test_model_input","parameters"]` → `shap_population`;in-DAG MemoryDataset,小 pandas)。依賴 predict 已寫 `training_eval_predictions`。
- 新節點 `compute_quadrant_profiles`(inputs `["model","shap_population","preprocessor_view","parameters"]` → `quadrant_profiles`)。
- `quadrant_profiles` 為**獨立 catalog JSONDataset 輸出** → `data/models/${model_version}/diagnostics/per_quadrant.json`(**修正**:shap_diagnostics.json 由 catalog 寫,非 log_experiment,故 per_quadrant 走自己的 catalog JSON 檔最乾淨——`compute_shap_diagnostics` 與寫檔邏輯全不動;`log_experiment` 既有的「上傳整個 diag_dir」自動含進 per_quadrant.json)。`log_experiment` 只加 `quadrant_profiles` 輸入以**保證 DAG 排序**(它在 per_quadrant.json 寫好後才上傳 dir)+ 記一個 scalar(覆蓋的 item×象限格數)。

### 3.4 best-effort(對齊既有診斷哲學)
兩個新節點防禦化:內部 try/except → log 警告 + 回空 dict/略過,**單點失敗不中斷訓練**;`diagnostics.shap.quadrant_enabled=false` 可整段關閉(不建選樣 job)。

## 4. config 新鍵(`diagnostics.shap` top-level,不動 model_version)
```yaml
quadrant_enabled: true          # 關閉則不跑象限選樣/ profile
quadrant_top_k_decision: 1      # 象限「判正」名次界線(top@K)
quadrant_sample_per_cell: 30    # 每 (item×象限) profile 抽樣目標數
quadrant_min_rows: 10           # 某格 < 此 → low_coverage
```

## 5. 範圍

- **本 PR**:選樣節點(僅**抽樣**,不含極值)、per_quadrant profile、接線、config、real-run 驗證。
- **不在本 PR(P2b-2)**:每格 max/min **極值案例**、`cases/<item>/*.png`、`cases_manifest.json`、收掉 `examples`。選樣節點屆時擴充 role=high/low。
- **不動**:`compute_shap_diagnostics`(P1/P2a)、feature_stats、data_access、ParquetHandle。

## 6. 記憶體

象限 rank / 每格抽樣 / join 特徵全在 Spark(executor);driver 只 `toPandas` 小族群 + 一次 SHAP。無 driver 端全表物化。

## 7. 測試

1. **選樣節點(小 Spark fixture,秒~十幾秒級)**:rank@(snap,cust) 正確(含平手穩定序);象限 TP/FP/FN/TN 對應 rank/label;每格抽樣 ≤ `quadrant_sample_per_cell`(某格超量時 cap、determinism 同 seed 同結果);join 後帶到特徵欄 + item/quadrant。
2. **`compute_quadrant_profiles`(純 python)**:group by (item,quadrant) 出 signed profile;某格 < `quadrant_min_rows` → low_coverage;空格不出現;profile_disabled/空族群 → 回 {}。
3. **接線**:`log_experiment` 把 `quadrant_profiles` 併進 `shap_diagnostics.json` 的 `per_quadrant`;`quadrant_enabled=false` → 不建節點/回空。
4. **回歸**:`tests/test_pipelines/test_training/` 既有全綠(P1/P2a diagnostics 不受影響)。

## 8. Real-run 驗證(本 PR 完成定義,使用者要求)

實作 + 單元測試綠後,**在本機 Spark 實跑**:
1. worktree pre-flight(readlink .venv、`local_spark_setup.py --check-isolation`,首次 `local_spark_setup.py` 建 synthetic data)。
2. `export SPARK_CONF_DIR=$PWD/conf/spark-local`;跑 training pipeline(必要時用 `--from-node`/`--only-node` 切到 diagnostics,缺料自動補上游:dataset → train → predict → 象限)。
3. 打開產出的 `data/models/<mv>/diagnostics/shap_diagnostics.json`,**把 `per_quadrant` 真實內容貼給使用者看**:四象限是否有填、每格 signed features 是否合理、low_coverage 分佈是否符合合成資料的稀疏度。
4. 若真實輸出不合理(如象限全空、profile 異常),回頭修再驗——**以實跑結果為準,不只靠測試**。

## 9. 明確排除
cases 圖 / manifest / examples 收斂 → P2b-2。predict 分區列舉、P3 → 另案。
