# Predict 節點 Partition 枚舉去物化 — 設計

**日期**:2026-07-01
**分支**:`feat/predict-partition-enum`(off `origin/main` @ ac281a0,含 PR #93)
**前置**:Training Diagnostics 記憶體重構(PR #93,已 merged)——本項是該次 driver 記憶體稽核中
明確排除、留待另開 PR 的項目(見該 spec §9「predict 列舉分區」)。

---

## 1. 背景與動機

`predict_and_write_test_predictions`(`src/recsys_tfb/pipelines/training/nodes.py:777-896`)
對 `test_model_input` 逐 `(snap_date, prod_name)` partition 做 predict + Hive write。要先枚舉
出有哪些 distinct partition,現況作法:

```python
ds = pads.dataset(test_parquet_handle.path, format="parquet", partitioning="hive")
partition_table = ds.to_table(columns=[time_col, item_col])   # 見下方問題
partition_pdf = partition_table.to_pandas()
partition_pdf = partition_pdf.drop_duplicates().sort_values([time_col, item_col])
```

`ds.to_table(columns=[time_col, item_col])` 雖然只投影兩個欄,但這兩欄是 **hive partition 欄**——
pyarrow 對 partition 欄仍是「每個 data row 產生一列」(從目錄名回填),所以會在 driver 材料化一個
`(全 row 數 × 2 個短字串)` 的暫時 table/DataFrame,再靠 `drop_duplicates` 收斂成
`n_snap_dates × n_prods`(數十)列。production 規模 `test_model_input` ≈ 220M rows,這是一筆
**本可完全避免**的 driver 峰值——partition 是目錄結構本身已編碼的 metadata,枚舉 distinct
partition 理應是 `O(n_fragments)`、零資料掃描,而不是 `O(n_rows)`。

## 2. 目標

把上述枚舉改成 `O(n_fragments)` 零掃描,**輸出行為完全不變**(對齊 PR #93 的紀律):枚舉出的
`(snap_date, prod_name)` 集合、下游逐 partition 的 filter/predict/save 行為、
`training_eval_predictions` 的輸出內容與 schema,皆與現況位元對位。

## 3. 關鍵事實(已用本機 venv 對 pyarrow 14.0.1 實測驗證,非臆測)

- `pyarrow.dataset.Dataset.get_fragments()` + `pyarrow.dataset.get_partition_keys(fragment.partition_expression)`
  是 pyarrow 14.0.1 的**公開 API**,對 hive-partitioned dataset 逐 fragment 給出
  `{"snap_date": "2025-01-31", "prod_name": "prod_A"}` 這樣的 dict——只讀 fragment/目錄
  metadata,**不掃資料列**。
- 兩者回傳的分區值都是 Python **`str`**,與現況 `str(row[time_col])` 型別一致;用該值組出的
  `pads.field(col) == value` filter,命中的 row 數與現況完全相同(已用 `filter=...` 直接比對驗證)。
  **附帶發現(實作階段 code review 挖出,已收斂)**:上述「回傳 `str`」只在 partition 目錄名
  非純數字時成立——pyarrow 的 hive partition 型別推斷對純數字目錄名(如
  `snap_date=20260701`)會推斷成 `int`,不保證 `str`。已實測確認:**現況**
  `ds.to_table(columns=...).to_pandas()` → `str(row[...])` → `pads.field(...) == snap_date` 這條
  filter,在這種數字型 partition 情境下本就會因 `ArrowNotImplementedError`(型別不符)炸掉——
  是**既有限制,非本次改動造成的退化**。因此 `nodes.py` 的整合(§4.2)刻意保留 `str()` cast
  在 `distinct_partitions` 的回傳值上,讓新碼在任何 partition 值型別下都與舊碼行為(包含這個
  既有失敗模式)逐位元相同,而非趁機修掉這個跟本任務無關的既有 bug。
- pyarrow 的 dataset discovery **已自動排除** `_SUCCESS`、`.DS_Store` 等非 parquet
  檔案/隱藏檔(已建 fixture 驗證,無需自行維護排除清單)。
- 一個 partition 若由多個檔案組成(Spark 多 task 寫入的常態),會產生多個 fragment 但
  partition key 相同——**枚舉必須去重**(已建 fixture 驗證此情境確實發生)。
- 空目錄(零 fragment)不會出錯,`get_fragments()` 回傳空 list——空輸入天然回傳 `[]`,
  不需額外特判。

結論:採用 **pyarrow fragments 方案**(候選 1),放進候選 3 建議的落點——
`src/recsys_tfb/pipelines/training/diagnostics/data_access.py`(PR #93 新增的唯一
`pyarrow.dataset` I/O 層)。放棄候選 2(檔案系統 glob),因為它得自行重做 pyarrow 已內建的
垃圾檔排除與 hive 值解碼,純屬重複造輪子。

## 4. 設計

### 4.1 新函式:`data_access.distinct_partitions`

```python
def distinct_partitions(path: str, columns: list) -> list:
    """Enumerate distinct hive-partition value tuples for `columns`.

    Reads fragment/directory metadata only (`Dataset.get_fragments()` +
    `pyarrow.dataset.get_partition_keys()`) — O(n_fragments), never O(rows).
    Returns tuples in the given column order, deduplicated, sorted ascending.
    """
```

- 重用既有私有 `_dataset(path)` 開檔 helper。
- 對每個 fragment 取 `get_partition_keys(fragment.partition_expression)`,依 `columns` 順序
  投影成 tuple,丟進 `set` 去重,最後 `sorted(...)` 回傳(升序,與現況
  `drop_duplicates().sort_values([time_col, item_col])` 行為對齊,也讓等價測試好比對)。
- 不做防禦性欄位驗證(沿用 `data_access.py` 現有風格):若呼叫端傳入不存在的分區欄名,
  自然拋 `KeyError`,不特別包裝。

### 4.2 `nodes.py` 改動

`predict_and_write_test_predictions` 約 L820-838 的枚舉區塊(含兩個只為觀察舊物化行為而存在的
`log_data_volume` 呼叫)整段替換為:

```python
from recsys_tfb.pipelines.training.diagnostics import data_access as da
...
for snap_date, prod_name in da.distinct_partitions(
    test_parquet_handle.path, [time_col, item_col]
):
    ...  # 迴圈內 filter/predict/save 邏輯完全不動
```

迴圈內部(part_table/part_pdf 的 `log_data_volume`)維持不變——那些量測的是真實逐 partition
資料,不是本次要去掉的枚舉物化。

### 4.3 影響範圍

僅兩個檔案:`data_access.py`(新函式)、`nodes.py`(替換 15 行左右)。不動
`ParquetHandle`、不動 `training_eval_predictions` 的 schema/寫入邏輯、不動
`compute_test_mAP_spark` 或其他下游節點。

## 5. 測試策略

1. **`test_diagnostics_data_access.py`**:針對 `distinct_partitions` 新增測試,涵蓋:
   - 對照 `pq.read_table(path, columns=[...]).to_pandas()[cols].drop_duplicates()` 這條參考
     路徑,斷言回傳集合完全相同(集合等價,不是逐列比對——因為新舊實作的中介型態不同)。
   - 單一 partition 由多個檔案組成時正確去重(不多出重複 tuple)。
   - dataset 根目錄含 `_SUCCESS` 等非資料檔時不受影響。
   - 空 dataset(零 fragment)回傳 `[]`。
   - 回傳為升序排序。
2. **`test_predict_and_write_test_predictions.py`**:現有 3 個測試**維持不改動、必須全綠**——
   這是節點層級的「output unchanged」驗收(斷言對象是 `manifest` 與 `saves` 內容,不涉及枚舉
   內部機制,新舊實作對這些測試而言應無可觀察差異)。
3. 純 pyarrow/檔案系統邏輯,不涉及 Spark,秒級,不需起 local Spark 或 e2e。

## 6. 明確排除(本次不動)

- `extract_Xy` 的全欄讀、`X[perm]`/`X_full` concat 峰值、driver 記憶體稽核報告——另外的項目,
  本次只做 predict partition 枚舉這一項。
- `data_access.py` 現有函式(`count_rows`/`schema_names`/`read_column`/`take_rows`)不動,
  僅新增 `distinct_partitions`。
