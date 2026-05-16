# Training Pipeline 資料量可觀測性 — Design

- 日期：2026-05-16
- Branch：`feat/training-memory-observability`（worktree `.worktrees/training-memory-observability`，← main f1b9894）
- 狀態：已完成 brainstorming，待使用者 review spec

## 1. 背景與問題

Training pipeline 非常耗記憶體（單機 `local[*]` driver，128GB RAM，CPU-only 4 core）。
目前 pipeline 已有良好的「時間」可觀測性（`log_step` 記錄每步 wall-clock），
以及零散且不一致的「資料量」記錄，但無法穩定地從 log 定位記憶體瓶頸：

- 多個高風險物件**完全沒有資料量 log**（見 §6 缺口）。
- 既有 log 風格不一（手寫 format string，難以機器解析、易飄移）。
- 既有 `X_df` 大小用 `memory_usage(deep=False)`，對字串/object 欄**嚴重低估**——
  偏偏寬特徵 frame 正是 OOM 主因，數字會誤導。

> 註：使用者明確排除 process 級別 RSS / peak 量測。本設計只做**資料量**
> 觀測——筆數、特徵數、dtype、以及由 shape×dtype 或 buffer 推導的
> **確定性 in-memory bytes**（非 process 量測）。

## 2. 目標與非目標

**目標**
- 在 training pipeline 所有記憶體高風險物件**實體化點**，輸出一致 schema 的資料量記錄。
- 永久內建、always-on、stdlib-only（core 層零重依賴）、生產安全、零記憶體成本。
- 機器可解析（JSON lines），OOM 後排序單一欄位即可定位瓶頸。

**非目標**
- process 級 RSS / peak / tracemalloc / line profiler（使用者已否決）。
- runtime 開關 / 設定旗標（YAGNI）。
- 改寫 `pd.read_parquet` 讀取路徑以暴露內部 Arrow Table（行為變更、有風險）。
- 為每個 `logger.info` 埋點寫脆弱的 log-assert 測試。

## 3. 方案

採方案 **B：統一 helper**（A=就地補 log；C=擴充 log_step，已否決）。
新增單一 helper，對所有高風險物件輸出統一結構化記錄，補滿缺口並收斂既有零散 log。

## 4. 元件：`log_data_volume` helper

位置：`src/recsys_tfb/core/logging.py`（與 `log_step` 同檔）。

簽章：

```python
def log_data_volume(logger, name, obj, *, deep=True, **fields) -> None
```

**Duck-typing 分派**（讓 `core/logging.py` 不需 import pandas/numpy/pyarrow/lightgbm，
維持 core 層零重依賴）。**分派順序固定**（避免 `nbytes` 在 arrow/numpy 撞型）：

1. `hasattr(obj, "num_data")` → `lgb_dataset`：`rows=num_data()`、`cols=num_feature()`
2. `hasattr(obj, "memory_usage")` → `pandas`：`rows=len`、`cols=shape[1]`、
   `bytes=memory_usage(deep=deep).sum()`、`dtype`（mixed 時記 dtype 直方圖摘要）
3. `hasattr(obj, "num_rows")` 且有 `column_names` → `arrow`（pyarrow Table）：
   `rows=num_rows`、`cols=num_columns`、`bytes=obj.nbytes`（buffer 真實大小，零成本）
4. `hasattr(obj, "nbytes")` → `numpy`：`rows=shape[0]`、`cols=shape[1] if ndim>1`、
   `bytes=nbytes`、`dtype`
5. `str`/`Path` 且存在 → `file`：`bytes=os.path.getsize`
6. 其他 → 不支援，發 WARNING（見 §8）

**統一 schema**（`extra["volume"]`）：

```
{name, kind: pandas|numpy|arrow|lgb_dataset|file,
 rows, cols, bytes, dtype, deep}
```

輸出一行 INFO，message 為人類可讀摘要
（例：`data_volume name=extract_Xy.pdf rows=12,345,678 cols=1523 bytes=18.4GB`），
同時帶 `extra={"event": "data_volume", "volume": {...}}` 供 JSON 解析。

## 5. JSON 整合

`JsonFormatter.format`（core/logging.py:73-75）只 merge 固定白名單 key。
helper 帶 `extra={"event":"data_volume","volume":{...}}`；`event` 已在白名單。
**僅需在白名單 tuple 新增 1 個 key `"volume"`**。
`json.dumps(default=str)` 已能序列化巢狀 dict。
`ConsoleFormatter` 只印 message（人類可讀摘要），不受影響。

## 6. 埋點清單

### 6.1 新增（補 4 缺口 + pyarrow + lgb）

| # | file:line（約） | 物件 | name | deep |
|---|---|---|---|---|
| N1 | io/extract.py:~150 | `extract_Xy` 完整 pdf | `extract_Xy.pdf` | True |
| N2 | io/extract.py:~208 | `extract_Xy_with_groups` 完整 pdf | `extract_Xy_with_groups.pdf` | True |
| N3 | models/lightgbm_adapter.py:~179 | construct 後 `ds_train` | `prepare.ds_train` | — |
| N4 | models/lightgbm_adapter.py:~180/192 | `save_binary` 落地 .bin | `prepare.train.bin` / `prepare.train_dev.bin`（file） | — |
| N5 | models/lightgbm_adapter.py:~142 | cache-hit 既有 .bin | 同 N4 兩個 file | — |
| N6 | models/lightgbm_adapter.py:~191 | construct 後 `ds_dev` | `prepare.ds_dev` | — |
| N7 | pipelines/training/nodes.py:~462 | `X_full` / `y_full`（concat 後峰值） | `finalize.X_full` / `finalize.y_full` | — |
| N8 | pipelines/training/nodes.py:~587/588 | `labels_table`(arrow)→`labels_pdf` | `predict.labels_table` / `predict.labels_pdf` | pdf=True |
| N9 | pipelines/training/nodes.py:~610/611 | `partition_pdf` raw→unique | `predict.partition_pdf` / `predict.partition_pdf_unique` | **False** |
| N10 | pipelines/training/nodes.py:~623/627 | 每 partition `part_table`→`part_pdf` | `predict.part_table[snap/prod]` / `predict.part_pdf[snap/prod]` | pdf=True |

### 6.2 Retrofit（現存改 helper，統一 schema）

| file:line | 現況 | 改法 |
|---|---|---|
| io/extract.py:93 / 227 | `X_df` mem **deep=False**（低估） | → helper，**改 deep=True**（修正字串欄低估） |
| io/extract.py:160 / 252 | `X`/`y` 手寫 nbytes | → helper numpy 分支（保留資訊、統一 schema） |
| io/extract.py:152 / 210 | "parquet loaded rows cols"（僅 shape） | **被 N1/N2 取代並移除**（避免重複/雜訊） |
| pipelines/training/nodes.py:362 | `ds_train/ds_dev`（缺 ds_dev.num_feature） | → helper lgb 分支（補齊 ds_dev 特徵數） |
| io/extract.py:58 `_log_parquet_metadata` | metadata-only 代理 | **維持原樣**（用途不同、已自帶 try/except、非 helper 範疇） |
| pipelines/training/nodes.py:529/498 衍生計數 | n_samples / n | **不動**（domain log，非物件 sizing） |

> 行號為設計時的近似值；實作以實際程式為準。

## 7. 已拍板決策

- **D1**：移除 extract.py:152/210 的 shape-only 行（N1/N2 已含 rows/cols/bytes，重複會吵）。
- **D2**：保留 predict 每 partition 各一行（共 n_snap × n_prod 行）——找肥 partition 的關鍵，
  本就在 per-partition `log_step` 內，一致。
- **D3**：`partition_pdf`（~220M×2 字串）用 **deep=False**，只取筆數
  （deep 會掃 ~4 億 python string，太貴）。

## 8. Error handling

鐵則：**觀測絕不可阻斷或拖垮真正計算**（沿用 `_log_parquet_metadata` 原則）。

- size 計算整段包 `try/except Exception`；失敗降為一行 `WARNING`
  （帶 `name`、`exception_type`、截斷的 `repr(e)`），**不重拋**，呼叫端照常往下。
- duck-typing 全 miss → 不拋例外，記
  `WARNING: log_data_volume unsupported kind name=… type=…`（利於日後發現遺漏型別）。
- file 分支 path 不存在 → `WARNING` 不拋（.bin 尚未寫出等競態）。
- `obj is None` → `WARNING` 略過（呼叫端不需自行 None-check，降低埋點負擔）。
- `deep=True` 對 object 欄是 O(n) 掃描（非複製、無記憶體成本，僅時間成本）；
  超大物件由呼叫端依 D3 傳 `deep=False`。helper 不設全域開關（YAGNI）。

## 9. Testing

新增 `tests/test_core/test_logging.py` 的 `log_data_volume` 單元測試（無 Spark、快）：

- **dispatch 正確性**：pandas / numpy / pyarrow Table / 仿 lgb stub / 檔案 path
  各一例，斷言 `extra["volume"]` 的 `kind`/`rows`/`cols`/`bytes`。
- **分派順序**：同時有 `nbytes` 與 `num_rows` 的 pyarrow Table → 斷言判為 `arrow`
  非 `numpy`（防順序回歸）。
- **deep 行為**：含 object 字串欄 DataFrame，`deep=True` bytes 明顯 > `deep=False`，
  斷言 `volume["deep"]` 旗標正確。
- **error handling**：size 計算丟例外的物件 → 不拋、發一筆 `WARNING`、
  `event` 非 `data_volume`；`None` → `WARNING` 略過；不支援型別 → `WARNING unsupported`。
- **JSON 整合**：經 `JsonFormatter` 格式化一筆 record，斷言 `volume` 巢狀 dict
  完整出現於 JSON line（驗證白名單新增 `"volume"` 生效）。

**回歸保護**：retrofit 動到 `extract.py` 既有 log（X_df 改 deep=True、移除 shape-only 行），
更新既有 `tests/test_io/test_extract.py` 中
`test_extract_xy_logs_size_summaries`、`test_extract_xy_emits_sub_step_events`、
`test_extract_xy_logs_parquet_metadata_before_read` 的斷言，確保 baseline
（調整後）續綠。

**埋點不寫專屬測試**：N1–N10 靠既有 pipeline 測試 + helper 單元測試覆蓋；
不為每個 `logger.info` 寫脆弱 log-assert（YAGNI）。

## 10. 生產限制對齊

- 無額外套件：helper 僅用 stdlib + 既有依賴（pandas/numpy/pyarrow/lightgbm
  皆已在 runtime，且 helper 不直接 import 它們，靠 duck-typing）。
- 無網路、CPU-only：不受影響。
- 零記憶體成本：所有量值來自 shape/dtype/buffer/getsize，無資料複製；
  `deep=True` 僅 O(n) 讀取時間，已由 D3 對超大物件規避。

## 11. Out of scope

- process 級記憶體（RSS/peak/tracemalloc）。
- 改寫 `extract_Xy` 讀取路徑以暴露 `pd.read_parquet` 內部 Arrow Table。
- `compute_test_mAP_spark` 的 Spark 側 row count（會觸發 count，且非 pandas/numpy 範疇）。
- 任何設定旗標 / runtime toggle。
