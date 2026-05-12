# `build_model_input` / `apply_preprocessor` decimal → double cast

**Date**: 2026-05-12
**Status**: Draft

## 背景

公司環境跑 `tune_hyperparameters` 時 `extract_Xy` 的 `read_parquet` step OOM 被 OS kill。前兩輪 observability ([extract_Xy 子步驟可觀測性](2026-05-12-extract-xy-observability-design.md)、[pre-read metadata](2026-05-12-extract-xy-pre-read-metadata-design.md)) 收到的 metadata log：

```
extract_Xy: parquet metadata
  num_rows=18216258
  num_columns=115
  num_row_groups=200
  total_uncompressed_mb=7911.0
  schema_types={
    'decimal128(31, 2)': 1, 'decimal128(15, 0)': 1, 'int64': 3, 'double': 1,
    'decimal128(29, 0)': 22, 'decimal128(30, 0)': 11,
    'decimal128(38, 6)': 26, 'decimal128(36, 6)': 6,
    ...
  }
```

兇手：**parquet 中 65+ 個 decimal128 欄位**。`pd.read_parquet(engine="pyarrow")` 對 decimal128 預設轉成 Python `decimal.Decimal` 物件，每個值約 50-80 bytes（PyObject header + Decimal 內部），相對於原 16 bytes / `decimal128`，相對於 cast 後 8 bytes / `float64`，膨脹率約 10×：

- decimals as Python objects: ~18M rows × 65 cols × 70 bytes ≈ **80 GB peak** → 64GB RAM OOM
- 全 cast 成 float64: ~18M × 115 × 8 bytes ≈ **16 GB** → 可容納

## 設計

在 Spark 端寫 model_input parquet 之前，把 `feature_columns` 中所有 `DecimalType` 欄位 cast 成 `double`。一次寫死，下游所有 `extract_Xy` / inference / evaluation 自動受益。

### 改動點

兩個寫入路徑都修，因為兩條都會落 parquet 後被 pandas 端讀回：

1. **`build_model_input`** (`src/recsys_tfb/preprocessing/_spark.py:245`) — training path，產 train/val/test/calibration model_input
2. **`apply_preprocessor`** (`src/recsys_tfb/preprocessing/_spark.py:287`) — inference path，產 scoring dataset

### Helper

抽一個共用 helper：

```python
def _cast_feature_decimals_to_double(
    df: DataFrame,
    feature_cols: list[str],
) -> tuple[DataFrame, list[str]]:
    """Cast all DecimalType columns within feature_cols to double.

    Why: pandas pyarrow reads decimal128 as Python decimal.Decimal objects
    (~70 bytes/value vs 8 bytes/float64), causing 10× peak-memory blow-up
    that OOM-kills tune_hyperparameters in our prod cluster.

    Only feature_columns are cast — identity columns and label column are
    left untouched (they should not be decimal to begin with, and we want
    to avoid silently changing primary keys / label dtype).
    """
    decimal_feature_cols = [
        f.name for f in df.schema.fields
        if f.name in set(feature_cols)
        and isinstance(f.dataType, T.DecimalType)
    ]
    for col in decimal_feature_cols:
        df = df.withColumn(col, F.col(col).cast("double"))
    return df, decimal_feature_cols
```

### 呼叫端

`build_model_input` — 在 `select(*output_cols)` 之後、`return result` 之前：

```python
with log_step(logger, "cast_decimals_to_double"):
    result, casted = _cast_feature_decimals_to_double(result, feature_columns)
logger.info(
    "build_model_input: cast %d decimal feature columns to double",
    len(casted),
)
if casted:
    logger.debug("build_model_input: casted columns = %s", casted)
```

`apply_preprocessor` 同樣處理，log message 換成 `apply_preprocessor:`。

### Cast 規則細節

- **只 cast feature_columns 中的 decimal**：`identity_cols` / `label_col` 不動。理由：identity 通常是 string/date（不會是 decimal），label 應是 int；若意外是 decimal 也不該被悄悄 cast，要露出問題。
- **非 decimal 的 feature 欄位不動**：int / double / string 維持原型別。
- **不對 categorical 做特殊處理**：categorical encoding 已在 `_encode_categoricals` 完成；走到這裡的 categorical 已是 int code，本來就不是 decimal。

### 精度影響

`decimal128(38, 6)` → `float64` 約 15-17 位有效十進位數字。

- 業務欄位（交易金額、餘額、比率）日常數值 < 10^12，float64 完全可表達
- LightGBM 內部就吃 float，模型訓練本來就不會用到 decimal128 的 38 位精度
- 若未來有真的需要保留 decimal 精度的欄位，那欄位不該進 `feature_columns`

### Cache 失效（手動）

`/dataset/workspaces/data/recsys_cache/<base_dataset_version>/` 下的舊 parquet caches **還是 decimal 版本**。

cache hash key (`base_dataset_version`) 由 data inputs hash 而來，**不包含 cast 邏輯**，所以新 code 仍會 hit 舊 cache。

**操作**：本 PR merge 後，使用者要手動 `rm -rf` 整個 `<base_dataset_version>/` 讓 pipeline 重建。不自動處理 — 改 hash key 邏輯影響範圍太大（會讓所有歷史 dataset 失效），不值得。

(會在 plan 的最後一個 task 寫一行 release-note 提醒。)

## 不做的事

- 不改 `_encode_categoricals`、`_compute_feature_columns` 等其他 preprocessing 邏輯
- 不自動偵測 cache 失效；使用者手動 `rm`
- 不對 `total_uncompressed_mb` 做 fail-fast / threshold 警告（純記錄）
- 不動 `ParquetHandle.to_pandas`（讀端不變）
- 不處理 `evaluate_model` 中 `eval_pdf = eval_parquet_handle.to_pandas()` 的 OOM 風險 — 本 PR 修完後 eval parquet 也是 float64，那個風險自然消失

## 驗證

### 單元測試

`tests/test_preprocessing/test_spark.py` (或新檔)：

1. **`test_cast_feature_decimals_to_double_casts_only_feature_decimals`**：
   - 構造小 Spark DF，含 identity(string) + label(int) + feature_a(decimal) + feature_b(int) + feature_c(decimal) + non_feature_decimal(decimal)
   - feature_columns = [feature_a, feature_b, feature_c]
   - 呼叫 helper
   - assert feature_a, feature_c 是 DoubleType
   - assert feature_b 仍是 IntegerType
   - assert non_feature_decimal 仍是 DecimalType（不在 feature_cols 中）
   - assert identity 仍是 StringType, label 仍是 IntegerType

2. **`test_cast_feature_decimals_returns_casted_list`**：
   - 同上 DF
   - assert 回傳的 list 是 `["feature_a", "feature_c"]`

3. **`test_cast_feature_decimals_noop_when_no_decimals`**：
   - 全 int/double/string 的 DF
   - assert 回傳 DF 與輸入 schema 相同，list 為空

### 整合測試

`tests/test_preprocessing/test_spark.py` 既有 `test_build_model_input` 系列：

4. **`test_build_model_input_outputs_double_for_decimal_features`**：
   - 構造 feature_table 含 decimal 欄位
   - 跑 `build_model_input`
   - assert 輸出 DF 中 feature_columns 沒有 DecimalType

`apply_preprocessor` 對稱加一個測試。

### 公司環境驗證

merge 後使用者：
1. `rm -rf /dataset/workspaces/data/recsys_cache/6077c62d/` (或當前 base_dataset_version)
2. 重跑 dataset pipeline 重建 caches
3. 跑 training；預期看到 `cast N decimal feature columns to double` log
4. 預期看到 `extract_Xy: parquet metadata ... schema_types={'double': ~70, 'int64': ..., ...}`（decimal 全消失）
5. `read_parquet` step 順利完成不再 OOM
