# `extract_Xy` pre-read parquet metadata observability

**Date**: 2026-05-12
**Status**: Draft

## 背景

前一輪 observability ([extract_Xy 內部子步驟可觀測性](2026-05-12-extract-xy-observability-design.md)) 把 OOM 定位到 `handle.to_pandas()` 的 `read_parquet` step：公司環境跑 `tune_hyperparameters` 時 log 顯示 `Step started: read_parquet` 之後直接 `Killed`。

但缺一個關鍵資訊：**parquet 解壓縮後到底多大**。已知條件：

- on-disk 1.7GB compressed parquet
- 64GB RAM、Spark driver JVM 仍活著
- pyarrow → pandas peak memory ≈ 1.5-2× final，壓縮比可能 10-30×

無法判斷 OOM 是因為「row 數量非預期地大」、「dtype 不必要地寬」、還是「pandas object 字串展開」。`pyarrow.parquet.read_metadata(path)` 是 metadata-only read，毫秒級、零記憶體成本，可以在 `read_parquet` step 之前就把這些事實打出來。

## 設計

`extract_Xy` 入口 log 之後、`with log_step(logger, "read_parquet"):` 之前，多打一行 INFO：

```
extract_Xy: parquet metadata num_rows=N  num_columns=M  num_row_groups=G  total_uncompressed_mb=U.U  schema_types={...}
```

其中：

- `num_rows`：`metadata.num_rows`
- `num_columns`：`metadata.num_columns`
- `num_row_groups`：`metadata.num_row_groups`
- `total_uncompressed_mb`：對所有 row group × 所有 column 累加 `column.total_uncompressed_size`（單位 bytes）→ 除以 `1024**2`
- `schema_types`：把 `metadata.schema` 走一遍把 physical type 計數成 `{"DOUBLE": 110, "INT64": 1, "BYTE_ARRAY": 3, ...}` 之類

「parquet 是個目錄含多個 .parquet」的情況：`pyarrow.parquet.read_metadata(path)` 只吃單檔；directory 要用 `pyarrow.parquet.ParquetDataset(path).fragments` 或 `pyarrow.dataset.dataset(path).count_rows()` + schema。

為了相容兩種情況，實作改用 `pyarrow.dataset`：

```python
import pyarrow.dataset as pads
ds = pads.dataset(handle.path, format="parquet")
n_rows = ds.count_rows()                          # 走 metadata，不讀資料
schema = ds.schema                                 # pyarrow.Schema
# uncompressed size 需要 iterate fragments
total_bytes = 0
n_row_groups = 0
for frag in ds.get_fragments():
    md = frag.metadata
    n_row_groups += md.num_row_groups
    for rg_i in range(md.num_row_groups):
        rg = md.row_group(rg_i)
        for col_i in range(rg.num_columns):
            total_bytes += rg.column(col_i).total_uncompressed_size
```

`pyarrow.dataset` 對於 single-file parquet 和 multi-file parquet directory 都 work，並用 `format="parquet"` 強制指定避免誤判。

### 失敗模式

若 `pyarrow.dataset.dataset()` raise（例如 path 不存在、不是 parquet）：catch + log warning + 繼續 — observability 不該擋住主流程。原本就有的 `read_parquet` step 仍會嘗試並用 pandas 報錯（行為與當前一致）。

## 不做的事

- 不修任何 OOM 真正成因 — 等這層 observability 拿到 metadata 後另開 spec
- 不對 metadata 做任何 thresholding / alert / fail-fast — 純記錄
- 不快取 metadata 結果 — 每次 extract_Xy 都 fresh read（成本可忽略）
- 其他 `to_pandas()` 呼叫點不動（例如 `evaluate_model` 中 `eval_pdf = eval_parquet_handle.to_pandas()` 不在本 PR scope；那個是另一個 OOM 風險點，但目前 kill 發生在這裡）

## 驗證

公司環境再跑一次 `tune_hyperparameters`，預期看到：

```
[YYYY-MM-DD HH:MM:SS] INFO [training:tune_hyperparameters] extract_Xy: parquet metadata num_rows=...  num_columns=...  num_row_groups=...  total_uncompressed_mb=...  schema_types=...
[YYYY-MM-DD HH:MM:SS] INFO [training:tune_hyperparameters] Step started: read_parquet
```

即使後面再被 kill，至少這條 metadata log 已落地。
