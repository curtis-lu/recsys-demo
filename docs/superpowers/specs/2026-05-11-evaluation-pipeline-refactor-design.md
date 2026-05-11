# Evaluation Pipeline Refactor — Persist Test-set Predictions & Split Scenarios

**Date**: 2026-05-11
**Status**: Draft

## 背景與動機

目前 `evaluation` pipeline 的 `prepare_eval_data` 吃 `ranked_predictions`（inference pipeline 寫入 Hive 的全量推論結果），與 `label_table` join 後計算指標。這個設計只覆蓋「推論後監控」一種情境。實務上 evaluation 有兩種互不相同的觸發場景：

1. **訓練後評估**：訓練 pipeline 跑完後針對該次模型做完整指標報告
2. **每月監控**：每月月底 label 入庫後，對上個月已產的推論結果做指標計算

訓練 pipeline 的 `evaluate_model` node 目前已經對 test set 跑了完整 `model.predict`，但結果只保留在記憶體並算出單一 mAP 給 MLflow，**全量逐筆預測結果未持久化**。下游 evaluation 想跑更完整的 per-product / per-segment / calibration 分析時，得重做一次預測。這既浪費也讓「訓練的 mAP」與「evaluation 報告的指標」可能不一致（時間差、preprocessor 差異）。

本 spec 設計三階段重構：
- **Phase 1**：訓練 pipeline 把 test set 預測結果回寫 Hive（新表 `training_eval_predictions`）
- **Phase 2**：Evaluation pipeline 透過 `post_training` flag 切換預測來源（訓練後 vs 監控）
- **Phase 3**：監控場景的時間視窗 / drift 偵測（**本 spec 不展開，僅定義邊界**）

## 設計原則

- **職責邊界清楚**：training 只負責「寫預測 + 算 mAP 給 MLflow」；evaluation 只負責「讀預測 + 完整指標報告」
- **最小侵入既有 training DAG**：拆 node 而非塞邏輯，沿用 MLflow / `evaluation_results.json` 行為
- **重用既有 evaluation 三節點結構**：`prepare_eval_data` / `compute_metrics` / `generate_report` 不動，情境差異由 pipeline factory 的 input wiring 表達
- **對齊既有 Kedro factory pattern**：與 `training/pipeline.py::create_pipeline(enable_calibration)` 同款，新增 `evaluation/pipeline.py::create_pipeline(post_training)`

---

## Phase 1 — Training 寫回測試集預測

### 1.1 Node 拆分（A2）

把現有 `evaluate_model` 拆成三個職責清楚的 node：

```python
# src/recsys_tfb/pipelines/training/nodes.py

def evaluate_model(
    model: ModelAdapter,
    eval_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """讀 test parquet → predict → rank → 回傳 (predictions_pdf, labels_pdf)。

    predictions_pdf columns: identity_columns + [score_col, rank_col]
    labels_pdf       columns: identity_columns + [label_col]

    不再計算 metric、不再 return dict；下游兩個 node 各自消費。
    """
    ...

def write_test_predictions(
    test_predictions_pdf: pd.DataFrame,
    parameters: dict,
) -> None:
    """依 prod_name 批次轉 Spark DataFrame，逐批 insertInto training_eval_predictions。

    Output 為 None，不走 catalog auto-save；node 內直接呼叫 Spark write API。
    """
    ...

def compute_test_mAP(
    test_predictions_pdf: pd.DataFrame,
    test_labels_pdf: pd.DataFrame,
    parameters: dict,
) -> dict:
    """純 ranking metric 計算 → evaluation_results dict（給 log_experiment / MLflow）。"""
    ...
```

`_compute_ranking_metrics` 內現有的 rank 計算搬到 `evaluate_model`，避免下游兩個 node 重做。

### 1.2 寫回實作（依 prod_name 批次）

```python
def write_test_predictions(test_predictions_pdf, parameters):
    schema_cfg = get_schema(parameters)
    item_col = schema_cfg["item"]
    spark = get_or_create_spark_session()
    model_version = parameters["model_version"]
    table_fqn = f"{parameters['hive']['db']}.training_eval_predictions"

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    _ensure_training_eval_predictions_table(spark, table_fqn)  # CREATE TABLE IF NOT EXISTS

    for prod in sorted(test_predictions_pdf[item_col].unique()):
        chunk_pdf = test_predictions_pdf[test_predictions_pdf[item_col] == prod]
        chunk_sdf = (
            spark.createDataFrame(chunk_pdf)
                 .withColumn("model_version", F.lit(model_version))
        )
        chunk_sdf.write.insertInto(table_fqn, overwrite=True)
        logger.info("Wrote prod=%s rows=%d", prod, len(chunk_pdf))
```

**為何依 prod_name 批次**：
- Test set 全量轉 Spark DataFrame 的 peak memory ≈ N_rows × col_size × 2（pandas → Arrow → JVM 短暫多份）。對 prod 數量為 22、單機 driver 128GB 的環境，依 prod 批次把 peak 切成 1/22，並提供 progress log，OOM 時容易判斷哪批爆掉
- Dynamic partition overwrite 保證每批只覆寫該 prod_name 的 partition slice，部分跑失敗可從失敗點繼續

`_ensure_training_eval_predictions_table` 內含 `CREATE TABLE IF NOT EXISTS` DDL，schema 與 catalog 宣告對齊，避免首次跑時表不存在。

### 1.3 Catalog 新增

```yaml
# conf/base/catalog.yaml
training_eval_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: training_eval_predictions
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: rank, type: BIGINT}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
    - {name: model_version, type: STRING}
```

Catalog 條目主要功能：
1. 作為 schema / partition 規格的單一事實來源（給 evaluation 讀取時用）
2. 給 evaluation pipeline 作為輸入 dataset 名稱

Training 端**不**走 catalog auto-save（`Node(write_test_predictions, ..., outputs=None)`），node 直接呼叫 Spark API。

### 1.4 DAG 連線

```python
# src/recsys_tfb/pipelines/training/pipeline.py
nodes.extend([
    Node(evaluate_model,
         inputs=["model", "test_parquet_handle", "preprocessor", "parameters"],
         outputs=["test_predictions_pdf", "test_labels_pdf"]),
    Node(write_test_predictions,
         inputs=["test_predictions_pdf", "parameters"],
         outputs=None),
    Node(compute_test_mAP,
         inputs=["test_predictions_pdf", "test_labels_pdf", "parameters"],
         outputs="evaluation_results"),
    Node(log_experiment,
         inputs=["model", "best_params", "best_iteration",
                 "evaluation_results", "parameters"],
         outputs=None),
])
```

### 1.5 Prerequisite — hive-site.xml symlink

Training pipeline 目前 `SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark`，該目錄**沒有** `hive-site.xml` symlink（CLAUDE.md 明確記載這個 trap）。要寫 Hive 必須先補：

```bash
ln -s ~/dev-cluster/client-template/spark/conf/hive-site.xml \
      ~/dev-cluster/client-template-local/spark/conf/hive-site.xml
```

這是 Phase 1 動工的第一個動作。同時更新 CLAUDE.md「pipeline 與 SPARK_CONF_DIR 對應表」備註，反映 training 現在也會碰 Hive。

### 1.6 MLflow / log_experiment 不變

`log_experiment` node 仍消費 `evaluation_results` dict 寫 mAP 到 MLflow，行為與既有完全一致。`evaluation_results.json` 仍由 catalog 寫到 `data/models/${model_version}/evaluation_results.json`，供 `scripts/promote_model.py` 讀取做 best 選擇。

---

## Phase 2 — Evaluation pipeline 切換情境

### 2.1 Pipeline factory + flag

對齊 training 的 `enable_calibration` 模式：

```python
# src/recsys_tfb/pipelines/evaluation/pipeline.py
def create_pipeline(post_training: bool = False) -> Pipeline:
    predictions_input = (
        "training_eval_predictions" if post_training else "ranked_predictions"
    )
    return Pipeline([
        Node(prepare_eval_data,
             inputs=[predictions_input, "label_table", "parameters"],
             outputs="eval_predictions"),
        Node(compute_metrics,
             inputs=["eval_predictions", "parameters"],
             outputs="evaluation_metrics"),
        Node(generate_report,
             inputs=["eval_predictions", "evaluation_metrics",
                     "parameters", "baseline_metrics"],
             outputs="evaluation_report"),
    ])
```

三個既有 node 完全不動，只切換 input dataset 名稱。

### 2.2 Model version 解析（node 內）

解析邏輯放在 evaluation 包內，**不**放 `__main__.py`。原因：`model_version` 對 evaluation 而言只是「要過濾 Hive 哪個 partition」，不是 catalog template 變數（不需要在 catalog load 前解析）。Spark 對 Hive partition column 的 predicate pushdown 會自動做 partition pruning，效能等價於 catalog 級 partition_filter。

```python
# src/recsys_tfb/pipelines/evaluation/_model_version.py
from pathlib import Path

def resolve_model_version(parameters: dict) -> str:
    """Param 優先，否則讀 data/models/best symlink target。"""
    eval_params = parameters.get("evaluation", {})
    explicit = eval_params.get("model_version")
    if explicit:
        return explicit
    best = Path("data/models/best")
    if not best.is_symlink():
        raise RuntimeError(
            "evaluation.model_version not set and data/models/best symlink missing; "
            "run scripts/promote_model.py first or pass --model-version"
        )
    return best.resolve().name
```

`prepare_eval_data` 在讀進 predictions 後加 filter：

```python
def prepare_eval_data(predictions, label_table, parameters):
    model_version = resolve_model_version(parameters)
    logger.info("Filtering predictions to model_version=%s", model_version)
    predictions = predictions.filter(F.col("model_version") == model_version)
    # ... 既有 label join 邏輯不動
```

### 2.3 CLI

新增 typer flag 到 evaluation 子命令：

```python
# src/recsys_tfb/__main__.py
@app.command()
def evaluation(
    env: str = "production",
    post_training: bool = typer.Option(
        False, "--post-training",
        help="Read predictions from training_eval_predictions (post-training eval)"),
    model_version: Optional[str] = typer.Option(
        None, "--model-version",
        help="Override model_version filter; default resolves data/models/best"),
):
    # 注入 model_version 到 parameters override（若有指定）
    param_overrides = {}
    if model_version:
        param_overrides["evaluation.model_version"] = model_version

    pipeline_kwargs = {"post_training": post_training}
    _execute_pipeline("evaluation", pipeline_kwargs, runtime_params,
                      config, params, env, param_overrides=param_overrides)
```

實際 override 機制依現有 `_execute_pipeline` / `_load_config_and_setup` 怎麼支援 parameter override 而定（實作階段確認）。若不支援，退路：直接修改 `parameters["evaluation"]["model_version"]` 後傳入。

### 2.4 Catalog 新增 `ranked_predictions` 讀取入口

**現況問題**：`evaluation/pipeline.py` 的 `prepare_eval_data` 輸入名為 `ranked_predictions`，但 `conf/base/catalog.yaml` 只有 `validated_predictions`（catalog name）→ Hive 表 `ranked_predictions`（table name）。`core/catalog.py:71` 對未宣告名稱自動 fallback 為空 `MemoryDataset`，導致 evaluation pipeline **無法 standalone 跑**（除非在同 session 接 inference）。

**修正**：新增 catalog entry `ranked_predictions`，與 `validated_predictions` 指向同一張 Hive 表，但職責不同：

```yaml
# 既有：inference 寫入 target
validated_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: ranked_predictions
  external: false
  columns: [...]
  partition_cols: [...]

# 新增：evaluation 讀取 source（與 validated_predictions 同表，分名）
ranked_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: ranked_predictions
  external: false
  columns: [...]      # 同 validated_predictions
  partition_cols: [...]  # 同 validated_predictions
```

雙 entry 一張表的取捨：
- **採用此方案**：零侵入既有 inference pipeline 命名；catalog 兩個 entry 但語意清楚（write vs read）
- 替代方案（暫不採用）：把 catalog `validated_predictions` 直接更名 `ranked_predictions`、inference 中間 node output 改名 `ranked_predictions_unvalidated`。較整潔但要動 inference pipeline 與測試，scope creep

### 2.5 Parameters yaml 補預設

```yaml
# conf/base/parameters_evaluation.yaml
evaluation:
  model_version: null   # null = resolve from data/models/best symlink
  snap_date: "20251231"
  k_values: [5, "all"]
  segment_columns: [cust_segment_typ]
  segment_sources: { ... }   # 既有不動
  baseline: { ... }          # 既有不動
  report: { ... }            # 既有不動
```

### 2.6 使用方式

```bash
# 監控（預設）
python -m recsys_tfb evaluation --env production

# 訓練後評估，讀 best symlink 解析的版本
python -m recsys_tfb evaluation --env production --post-training

# 訓練後評估，明確指定版本（多版本比較情境）
python -m recsys_tfb evaluation --env production --post-training \
    --model-version 20260511_153000
```

---

## Phase 3 — Monitoring 場景（範圍邊界）

**本 spec 不展開實作，只定義邊界。**

### 3.1 這個 spec 內 monitoring 端需要做的事

- 確保 `--post-training=False`（預設）走得通：
  - `prepare_eval_data` 讀 `ranked_predictions`（既有 inference 寫入的 Hive 表）
  - 既有行為不退化
  - `resolve_model_version` 在 monitoring 場景也適用（預設讀 best symlink，對應 production promote 的版本）

### 3.2 Phase 3 另開 spec 範圍

- 時間視窗 / lookback 配置：「上個月推論 vs 本月剛入庫的 label」如何由 parameter 表達（單 `snap_date` vs `[from, to]` 範圍 vs `last_n_months`）
- Drift 偵測 / 跨月趨勢比較
- 例行排程觸發（hooks / 報告分發 / alerting）

---

## 測試策略

### Phase 1

- **Unit test `write_test_predictions`**：mock pandas pdf 與 spark session → 驗證 `spark.createDataFrame` 被以 22 個 prod 呼叫（一次一個）、每次呼叫的 chunk 對應正確 prod_name；驗證 `insertInto` 以 `overwrite=True` 呼叫
- **Unit test `compute_test_mAP`**：對 deterministic 假資料計算 mAP，與既有 `_compute_ranking_metrics` 結果 bit-for-bit 比對（重構不該改數值）
- **Unit test `evaluate_model` 重構**：假 model + 假 parquet handle → 驗證回傳 tuple shape、rank column 正確
- **Integration test**：在 dev-cluster 跑完整 `python -m recsys_tfb training --env production`，驗證
  - `ml_recsys.training_eval_predictions` 表存在
  - 對應 `(snap_date, prod_name, model_version)` partition 都有資料
  - Row count 等於 test set row count
  - `data/models/<version>/evaluation_results.json` 內容與重構前一致

### Phase 2

- **Unit test `resolve_model_version`**：
  - param 顯式給 → 回傳 param 值
  - param 為 None 且 symlink 存在 → 回傳 symlink target name
  - param 為 None 且 symlink 不存在 → raise `RuntimeError`
- **Unit test `prepare_eval_data` model_version filter**：mock Spark DataFrame 含多個 model_version partition，驗證 filter 後只留指定版本
- **Integration test**：
  - `python -m recsys_tfb evaluation --env production --post-training` → 成功讀 `training_eval_predictions`、產 HTML 報告
  - `python -m recsys_tfb evaluation --env production`（預設）→ 成功讀 `ranked_predictions`、產 HTML 報告（驗證現況不退化）
  - 兩個情境的 `evaluation_report.html` artifact 都產出（不檢內容、只檢檔案存在）

### 既有測試影響

- `tests/test_training_pipeline*` 若有 mock `evaluate_model` 回傳 dict，要改成 mock 兩個 node 的回傳（pandas tuple + dict）
- `tests/test_evaluation*` 若有 hard-code `ranked_predictions` 輸入名稱，要參數化或新增 `post_training=True` 情境

---

## Out of scope

明確排除，避免 scope creep：

- 監控場景的 lookback 視窗 / drift 偵測（→ Phase 3 另開 spec）
- 多模型 A/B 並排比較的 UX（現況靠手動跑兩次 evaluation 對照）
- MLflow model registry / promotion 自動化（現況靠 `scripts/promote_model.py`，使用者手動觸發）
- `HiveTableDataset` 抽象的 batched-write 支援（Phase 1 用 node 內直接 `insertInto` 解決，不擴充 catalog 介面）
- Inference pipeline 的 `predict_scores` 也有類似 pandas → spark 全量轉換的 memory pattern，本 spec 不處理

---

## Migration / 部署順序

1. 補 `client-template-local` 的 `hive-site.xml` symlink（Phase 1 prerequisite）
2. 加 catalog `training_eval_predictions` 條目
3. Phase 1 實作：拆 node、加 `write_test_predictions` + `compute_test_mAP`、`_ensure_training_eval_predictions_table` 首次 DDL
4. 跑一次 `python -m recsys_tfb training --env production` 驗證 Hive 表生成
5. 加 catalog `ranked_predictions` 條目（修正 evaluation 無法 standalone 的現況問題）
6. Phase 2 實作：`create_pipeline(post_training)` factory、`resolve_model_version` 解析、`prepare_eval_data` 加 filter、CLI flag
7. 跑 `python -m recsys_tfb evaluation --env production --post-training` 驗證 end-to-end
8. 跑 `python -m recsys_tfb evaluation --env production` 驗證 monitoring standalone 可跑（修正後）
9. 更新 CLAUDE.md SPARK_CONF_DIR 對應表（training 現在也碰 Hive 的事實）

每階段以單獨 PR 提交。Phase 3 後續另起 spec 與 PR。
