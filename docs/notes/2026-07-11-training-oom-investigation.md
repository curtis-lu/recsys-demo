# 2026-07 生產 training OOM：`to_numpy` 為什麼吃掉 96 GiB（調查紀錄）

> **這是一次事故的調查過程，不是 runbook。** 保留它是因為結論所依賴的推算與量測都在這裡；
> 要照做的部分已經拆進現役文件（見文末「現役的部分在哪」）。
>
> 調查當下 B6 閘尚未上線，所以下面的推理是「只有 log、沒有錯誤訊息」的處境。
> B6 上線後這條路徑基本消失——錯誤訊息會直接點名兇手欄。
>
> **文中的關鍵事實有一部分未經生產環境證實**，見最後一節。
>
> **識別字註記**：文中的 `_pdf_to_X` 已於 #199（2026-08-31）改名為 `pdf_to_X`。
> 下面的 log 原文與記憶體帳**刻意保留舊名**——那是 2026-07-11 當天真的印出來的字，
> 改掉等於把現場證據改成當天沒發生的樣子。
>
> 同理，文中的 `_encode_categoricals` 已於 #254（2026-09-03）改名為 `encode_categoricals`
> （`_cast_feature_floats_to_float32` → `cast_feature_floats_to_float32` 同批）。**本文一律
> 不改**：文中所附的行號早已腐爛（`preprocessing.py:71`），它記錄的是 2026-07-11 那天的
> 座標，不是今天的。

## 1. 當時看到的樣子

log 停在 `to_numpy`，行程被作業系統殺掉：

```
INFO  extract_Xy: parquet metadata num_rows=4542746 num_columns=665
      num_row_groups=2200 total_uncompressed_mb=2056.1
      schema_types={'string': 9, 'int32': 92, 'float': 401, 'int64': 163}
INFO  Step completed: read_parquet (69.52s)
INFO  data_volume name=extract_Xy.pdf   rows=4,542,746 cols=666 bytes=16.3GB
INFO  Step completed: slice_features (14.09s)
INFO  data_volume name=_pdf_to_X.X_df   rows=4,542,746 cols=663 bytes=16.0GB
INFO  _pdf_to_X: encoded deferred_cats=['prod_name'] count=1
INFO  Step started: to_numpy
train.sh: line 5: 72 Killed   python -m recsys_tfb training
```

`Killed`（SIGKILL）不是 Python 的 `MemoryError`——兇手是 OS 或 cgroup 的 OOM killer，看的是整個行程的 RSS。

`spark.driver.memory` 對這件事毫無幫助：那個設定管 JVM 堆積，而 `prepare_lgb_train_inputs` 整段跑在 Python 行程裡，Spark 此時完全閒置。

## 2. 一句話結論

有幾欄**文字**（字串）混進了特徵欄。整張矩陣從「數字表格」退化成「地址表格」——每一格不再放數字，而是放一張紙條寫著「你要的數字在別處」。三十億張紙條，加上三十億個存在別處的數字。

原理（numpy 一格只能一種大小、`object` dtype 的外殼成本）已抽成手冊素材：
`docs/handbooks/spark-tuning/_drafts/spark-to-pandas-numpy-memory.md`。

## 3. 量測與外推

實測環境 Python 3.10.9 / pandas 1.5.3 / numpy 1.25.0。10 萬列小樣本，欄位型別比例照上面的
`schema_types`（401 float32 + 92 int32 + 162 int64；int64 是 163 − 1，扣掉會被當 identity 丟掉的
`cust_id`），整數值刻意避開 CPython 的小整數快取（−5..256），否則會低估外殼成本：

| | 矩陣格子型別 | 每格真實成本 |
|---|---|---|
| 有 8 欄文字（663 欄） | `object` | **34.2 bytes** |
| 無文字欄（655 欄） | `float64` | **8.0 bytes** |

34.2 bytes/格的組成拆解，可以逐項對上：

```
櫃號（指標）                          8.0
401/663 欄 × 24B（Python 小數物件）  14.5
 92/663 欄 × 32B（Python 整數物件）   4.4
162/663 欄 × 32B（Python 整數物件）   7.8
文字欄本身                             0    ← 字串物件與來源表共用，不重複配置
                                    ─────
                                    34.7    （實測 34.2）
```

生產資料是 4,542,746 列 × 663 欄 = **30.1 億格**。外推：

| | X 常駐記憶體 |
|---|---|
| 現況（地址矩陣） | **95.9 GiB** |
| 移除文字欄後（數字矩陣） | **22.4 GiB** |
| **省下** | **73.5 GiB（4.3 倍）** |

`to_numpy` 那一刻的總需求（三份資料同時活著）：

| | 現況 | 移除文字欄後 |
|---|---|---|
| 從 parquet 讀進來的完整表（`extract_Xy.pdf`） | 16.3 | 16.3 |
| 抽出特徵欄的那份拷貝（`_pdf_to_X.X_df`） | 16.0 | 16.0 |
| 新配置的矩陣 | 95.9 | 22.4 |
| 轉換過程的暫態 | ~13.6 | 0 |
| **合計** | **~142 GiB** | **~54.7 GiB** |

「轉換過程的暫態」＝pandas 拼矩陣時一個型別群組一個型別群組地填，填 `object` 矩陣時最大的那個群組
（401 個 float32 欄）會先整批做成 `401 × 4,542,746 × 8 bytes ≈ 13.6 GiB` 的臨時陣列。填 `float64`
不需要這個中介。

## 4. 文字欄是怎麼混進特徵欄的

`compute_feature_columns`（`src/recsys_tfb/pipelines/dataset/steps/feature_columns.py`）：

```python
non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
```

**凡是不在 `drop_columns`、不在 `identity_columns`、又不是 `label` 的欄，一律變成特徵——不管它是不是文字。**

而 `_encode_categoricals`（`src/recsys_tfb/preprocessing.py:71`）只把 `categorical_columns` 裡**明確列出**的欄轉成整數編碼。

所以一個生產 `feature_table` 有、但既沒宣告 `categorical_columns`、也沒被 `drop_columns` 擋掉的文字欄，會原封不動穿過整條 dataset pipeline。

合成資料（`scripts/generate_synthetic_data.py`）不產生這類欄位，所以**本機永遠不會爆，生產環境必爆**。

### 只用 log 就能推出至少有幾欄

不需要任何資料內容，只用第 1 節那四行：

- parquet 有 **9 個文字欄**（`schema_types` 的 `'string': 9`）
- 抽特徵欄時只丟掉 **3 欄**（`pdf` 666 欄 → `X_df` 663 欄，即 `label` + `snap_date` + `cust_id`）
  - （`pdf` 是 666 欄，比 parquet metadata 的 `num_columns=665` 多 1；多出的一欄來源未確認，可能是 pandas 讀取時還原的 index 欄，不影響下面的減法）
- 延後編碼只處理 **1 欄**（`encoded deferred_cats=['prod_name'] count=1`）

```
9 − 3 − 1 = 至少 5 個原始文字欄，在 to_numpy 執行時仍留在矩陣裡
```

### 舊 cache 的唯讀盤點（B6 上線前的做法）

B6 上線後不需要這段——錯誤訊息會直接列出兇手欄。只在「dataset 是 pre-gate 建的、training 吃 cache
不重建」時才用得上：

```python
import json, pyarrow as pa, pyarrow.parquet as pq

sch = pq.read_schema(
    "data/recsys_cache/<base_dataset_version>/train_variants/<train_variant_id>/train_model_input.parquet")
str_cols = [f.name for f in sch if pa.types.is_string(f.type)]

pp = json.load(open("data/dataset/<base_dataset_version>/preprocessor.json"))
feat, cat = set(pp["feature_columns"]), set(pp["categorical_columns"])

print("兇手（是特徵、卻沒宣告成 categorical）:", sorted((set(str_cols) & feat) - cat))
```

`preprocessor.json` 的位置定義在 `conf/base/catalog.yaml:85`。`prod_name` **不會**出現在結果裡——它有宣告成 categorical，只是延後到 `_pdf_to_X` 才編碼。

## 5. 尚未證實的部分（調查結束時的狀態，未回頭補驗）

- **「有 8 欄文字」是推算值**。第 4 節的減法只給出下界（≥ 5 欄）。確切欄名要在生產環境跑上面那段 snippet（或看 B6 的錯誤訊息）才知道。這是一個有算術支撐的假設，不是事實。
- **95.9 GiB 是外推**。實測是 100,000 列上量到每格 34.2 bytes，再乘 45.4 倍。每格成本與資料量無關，所以外推可靠；但沒有在 30.1 億格的規模上真跑過。
- **13.6 GiB 的轉換暫態**是從 pandas 原始碼推導的，未單獨量測。
- **這台機器的記憶體上限只框出區間**（48.3 GiB 與致死點之間），沒有確切值。可查 `cat /sys/fs/cgroup/memory.max`，或 YARN container 的 `driver.memory + driver.memoryOverhead`。

## 現役的部分在哪

| 要做什麼 | 去哪 |
|---|---|
| B6 點名之後，決定每一欄該 declare 還是 drop | [`dataset.md` §8](../pipelines/dataset.md) |
| 修掉文字欄之後還是 OOM，下一步有哪些選項 | [`training.md` §9.1](../pipelines/training.md) |
| 為什麼 `object` 矩陣這麼貴（通用原理） | `docs/handbooks/spark-tuning/_drafts/spark-to-pandas-numpy-memory.md` |
| 第一分鐘怎麼認出這個坑 | [`known-pitfalls.md` §8](../operations/known-pitfalls.md) |
