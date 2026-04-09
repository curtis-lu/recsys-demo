## 1. 參數與 Catalog 設定

- [x] 1.1 新增 `conf/base/parameters_evaluation.yaml`（snap_date、k_values、segment_columns、segment_sources、baseline_type、report 選項）
- [x] 1.2 在 `conf/base/catalog.yaml` 新增 evaluation entries（eval_predictions、evaluation_metrics、evaluation_report），路徑使用 `${model_version}/${snap_date}`
- [x] 1.3 在 `conf/base/catalog.yaml` 新增 baselines entries（baseline_predictions、baseline_metrics），路徑使用 `${snap_date}`
- [x] 1.4 在 `conf/base/catalog.yaml` 新增 label_table entry（供 evaluation/baselines 讀取）— label_table 已存在於 catalog

## 2. Baselines Pipeline

- [x] 2.1 建立 `src/recsys_tfb/pipelines/baselines/` 目錄結構（`__init__.py`、`pipeline.py`）
- [x] 2.2 實作 `nodes_pandas.py`：重用現有 `evaluation/baselines.py` 的 `generate_global_popularity_baseline` / `generate_segment_popularity_baseline`，包裝為 pipeline node 函數（compute_baselines、compute_baseline_metrics）
- [x] 2.3 實作 `nodes_spark.py`：用 Spark SQL 計算 per-product / per-segment popularity rate，cross join 客戶列表產出 baseline predictions，並計算 baseline metrics
- [x] 2.4 實作 `pipeline.py`：定義 baselines pipeline 節點拓撲（compute_baselines → compute_baseline_metrics）

## 3. Evaluation Pipeline — Pandas Backend

- [x] 3.1 建立 `src/recsys_tfb/pipelines/evaluation/` 目錄結構（`__init__.py`、`pipeline.py`）
- [x] 3.2 實作 `nodes_pandas.py` — `prepare_eval_data`：讀取 ranked_predictions，join label_table，可選 join segment sources，輸出 eval_predictions
- [x] 3.3 實作 `nodes_pandas.py` — `compute_metrics`：重用現有 `evaluation/metrics.py` 的 `compute_all_metrics`，輸出 evaluation_metrics dict
- [x] 3.4 實作 `nodes_pandas.py` — `generate_report`：重用現有 report.py、distributions.py、calibration.py、statistics.py，可選讀取 baseline_metrics 做比較，輸出 HTML report

## 4. Evaluation Pipeline — Spark Backend

- [x] 4.1 實作 `nodes_spark.py` — `prepare_eval_data`：Spark DataFrame join ranked_predictions + label_table + segment sources
- [x] 4.2 實作 `nodes_spark.py` — `compute_metrics`：Spark SQL 全程計算排名指標（AP、nDCG、MRR、Precision@K、Recall@K），含 overall、per-product、per-segment、macro/micro average，輸出與 pandas 版格式一致的 dict
- [x] 4.3 實作 `nodes_spark.py` — `generate_report`：從 Spark 聚合結果（已是小表 dict）呼叫現有 report 模組產出 HTML

## 5. Pipeline 定義與註冊

- [x] 5.1 實作 `evaluation/pipeline.py`：定義 evaluation pipeline 節點拓撲（prepare_eval_data → compute_metrics → generate_report），支援 backend 切換
- [x] 5.2 在 `src/recsys_tfb/pipelines/__init__.py` 註冊 `evaluation` 和 `baselines` pipeline
- [x] 5.3 在 `src/recsys_tfb/__main__.py` 新增 evaluation/baselines 的 CLI 分支（版本解析、runtime params、manifest 寫入、symlink 更新）

## 6. 清理與驗證

- [x] 6.1 刪除 `scripts/evaluate_model.py`
- [x] 6.2 撰寫測試：evaluation pipeline pandas backend 端對端測試（用合成資料）
- [x] 6.3 撰寫測試：baselines pipeline pandas backend 端對端測試
- [x] 6.4 撰寫測試：Spark SQL 指標計算與 pandas 版 cross-validation（同一份資料，結果在浮點誤差內一致）
- [ ] 6.5 手動驗證 CLI 執行：`python -m recsys_tfb --pipeline evaluation --env local` 和 `--pipeline baselines --env local`
- [x] 6.6 更新 plan.md 與 CLAUDE.md 反映新增的 pipeline
