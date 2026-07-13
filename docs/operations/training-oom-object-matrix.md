# 訓練 OOM：`to_numpy` 為什麼會吃掉 96 GiB

> 讀者：在生產環境跑 training pipeline、遇到 `prepare_lgb_train_inputs` 被 OOM killer 殺掉的人。
> 不預設 pandas / numpy 的內部知識。
> 範疇：只解釋這一個失敗模式的成因、如何確認、如何修。不涵蓋 Spark 端的記憶體調校。

## 1. 現象

有兩種樣子，取決於你的 dataset 是不是在 B6 閘（見第 6 節）上線後重建的。

### 1a. 秒級 fail-fast（B6 閘上線後，現在的預設）

training 在讀 parquet 資料**之前**就中止，錯誤直接點名兇手欄：

```
DataConsistencyError: train_model_input feature columns include un-encoded
non-numeric type(s) — this OOMs at to_numpy and fails LightGBM's float cast
(1 issue(s)):
- feature column 'cust_segment' is non-numeric and is not declared categorical,
  so it would become an un-encoded object-dtype model feature (OOM at
  _pdf_to_X.to_numpy, then a LightGBM float-cast error). If 'cust_segment' is a
  categorical feature, add it to dataset.prepare_model_input.categorical_columns
  (it is then integer-encoded); if it is not a model feature, add it to
  dataset.prepare_model_input.drop_columns.
```

這是好消息：秒級、指名道姓。直接跳到第 6 節——確認欄名、決定每欄該 declare 還是 drop、修法都在那。**不必再等一次 OOM。**

### 1b. 神秘 OOM（pre-gate 舊 cached dataset，或 backstop 被略過時）

若 dataset 是 B6 閘上線前建好的、且 training 吃 cache（不重建），會走到舊的失敗樣子：log 停在 `to_numpy`，行程被作業系統殺掉：

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

注意是 `Killed`（作業系統的 SIGKILL），不是 Python 的 `MemoryError`。這代表凶手是 OS 或 cgroup 的 OOM killer，看的是整個行程的實際記憶體佔用（RSS）。

也請注意 `spark.driver.memory` 對這件事毫無幫助——那個設定管的是 JVM 的堆積上限，而 `prepare_lgb_train_inputs` 整段跑在 Python 行程裡，Spark 此時完全閒置。

兩種樣子**同一個病根**（下一節），修法也相同。

## 2. 一句話結論

有幾欄**文字**（字串）混進了特徵欄。這讓整張矩陣從「數字表格」退化成「地址表格」——每一格不再直接放數字，而是放一張紙條，寫著「你要的數字在別處」。三十億張紙條，加上三十億個被存放在別處的數字，記憶體就爆了。

## 3. 通用原理：一張矩陣只能有一種格子大小

最後餵給 LightGBM 的那個東西是 `numpy` 矩陣。它有一條鐵律：**整張矩陣只能有一種格子大小**。

這跟試算表不同。試算表可以 A 欄放整數、B 欄放小數、C 欄放文字。numpy 不行——它是一整塊連續的記憶體，「第 n 格在哪裡」是用 `起點 + n × 格子大小` 算出來的。格子不等寬，這個算式就壞了。

所以把試算表（pandas 的 DataFrame）轉成 numpy 矩陣時，必須先決定：**一格要多大，才裝得下所有欄位？**

### 情況 A：全部都是數字

以第 1 節 log 裡的 `schema_types` 為例：

| 欄位型別 | 幾欄 | 一個值本來佔多少 |
|---|---|---|
| 小數（float32） | 401 | 4 bytes |
| 整數（int32） | 92 | 4 bytes |
| 整數（int64） | 163 | 8 bytes |

numpy 要找一個「都裝得下、又不失真」的格子。整數最大要 8 bytes，小數要能表示小數點——最小的共同解是 **8 bytes 的雙精度小數（float64）**。

順帶一提，那 401 欄原本只要 4 bytes 的小數，也會被撐成 8 bytes。這是「一格一種大小」的代價。

### 情況 B：混進了文字欄

現在假設有一欄的值是 `"信用卡"`、`"基金"` 這種文字。

**沒有任何固定寬度的格子能同時裝下 `3.14` 和 `"信用卡"`。** numpy 只剩最後一招：格子裡不放東西本身，改放一個 8 bytes 的**記憶體地址**，指向真正的東西擺在哪裡。這種格子叫 `object`。

用置物櫃比喻：

```
情況 A（數字矩陣）              情況 B（地址矩陣）
┌──────┬──────┬──────┐        ┌──────┬──────┬──────┐
│ 3.14 │ 42   │ 7.0  │        │ #001 │ #002 │ #003 │  ← 每格只是櫃號（8 bytes）
├──────┼──────┼──────┤        ├──────┼──────┼──────┤
│ 2.71 │ 13   │ 9.5  │        │ #004 │ #005 │ #006 │
└──────┴──────┴──────┘        └──────┴──────┴──────┘
   數字就在格子裡                    ↓ 數字全在別的地方
                                置物櫃 #001: [外殼 24B] 3.14
                                置物櫃 #002: [外殼 32B] 42
                                置物櫃 #003: [外殼 24B] 7.0
                                ...（三十億個櫃子，每個都有外殼）
```

**關鍵在「外殼」。** 為了讓一個數字能被地址指到，Python 必須替它做一個獨立的小物件；這個物件除了數字本身，還要背一層 24–32 bytes 的管理開銷（型別是什麼、有幾個地方正在用它）。

於是每一格的成本變成：

```
8 bytes（櫃號）+ 24~32 bytes（櫃子外殼）≈ 34 bytes
```

**一欄文字，就讓另外 662 欄的數字全部付出四倍代價。** 那 662 欄裡的每一個數字，都必須離開原本緊密排好的隊伍，各自搬進一個獨立的櫃子。

## 4. 數字

實測環境 Python 3.10.9 / pandas 1.5.3 / numpy 1.25.0；資料為 100,000 列，欄位型別比例照第 1 節的 `schema_types`（401 float32 + 92 int32 + 162 int64；int64 是 163 − 1，扣掉會被當 identity 丟掉、不進特徵的 `cust_id`），整數值刻意避開 CPython 的小整數快取（−5..256），否則會低估外殼成本：

| | 矩陣格子型別 | 每格真實成本 |
|---|---|---|
| 有 8 欄文字（663 欄） | `object` | **34.2 bytes** |
| 無文字欄（655 欄） | `float64` | **8.0 bytes** |

生產資料是 4,542,746 列 × 663 欄 = **30.1 億格**。外推：

| | X 常駐記憶體 |
|---|---|
| 現況（地址矩陣） | **95.9 GiB** |
| 移除文字欄後（數字矩陣） | **22.4 GiB** |
| **省下** | **73.5 GiB（4.3 倍）** |

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

`to_numpy` 那一刻的總需求（三份資料同時活著）：

| | 現況 | 移除文字欄後 |
|---|---|---|
| 從 parquet 讀進來的完整表（`extract_Xy.pdf`） | 16.3 | 16.3 |
| 抽出特徵欄的那份拷貝（`_pdf_to_X.X_df`） | 16.0 | 16.0 |
| 新配置的矩陣 | 95.9 | 22.4 |
| 轉換過程的暫態（見 §7） | ~13.6 | 0 |
| **合計** | **~142 GiB** | **~54.7 GiB** |

## 5. 文字欄是怎麼混進特徵欄的

`_compute_feature_columns`（`src/recsys_tfb/preprocessing/_spark.py:112`）：

```python
non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
```

**凡是不在 `drop_columns`、不在 `identity_columns`、又不是 `label` 的欄，一律變成特徵——不管它是不是文字。**

而 `_encode_categoricals`（`src/recsys_tfb/preprocessing/_spark.py:85`）只會把 `categorical_columns` 裡**明確列出**的欄轉成整數編碼（`cast("integer")`）。

所以：一個生產 `feature_table` 有、但既沒被宣告成 `categorical_columns`、也沒被 `drop_columns` 擋掉的文字欄，會原封不動穿過整條 dataset pipeline，成為 `feature_columns` 的一員。

合成資料（`scripts/generate_synthetic_data.py`）不產生這類欄位，所以**本機永遠不會爆，生產環境必爆**。

### 從 log 就能推出至少有幾欄

不需要任何資料內容，只用第 1 節那四行：

- parquet 有 **9 個文字欄**（`schema_types` 的 `'string': 9`）
- 抽特徵欄時只丟掉 **3 欄**（`pdf` 666 欄 → `X_df` 663 欄，即 `label` + `snap_date` + `cust_id`）
  - （`pdf` 是 666 欄，比 parquet metadata 的 `num_columns=665` 多 1；多出的一欄來源未確認，可能是 pandas 讀取時還原的 index 欄，不影響下面的減法）
- 延後編碼只處理 **1 欄**（`encoded deferred_cats=['prod_name'] count=1`）

```
9 − 3 − 1 = 至少 5 個原始文字欄，在 to_numpy 執行時仍留在矩陣裡
```

## 6. 怎麼確認（唯讀，不用 Spark，不用重跑 pipeline）

> B6 閘上線後，training backstop 會在讀取前**自動**列出這些欄（就是第 1a 節那則錯誤）。所以你多半不必自己跑下面這段——它的用途已轉為：**拿到欄名後，決定每一欄該 declare 成 categorical 還是 drop**（見第 5 節），以及在還沒重建 dataset、只有舊 cache 的情況下先行盤點。

路徑中的 `522cadb5` 是 log 裡的 `base_dataset_version`，`6cb77dff` 是 `train_variant_id`；`preprocessor.json` 的位置定義在 `conf/base/catalog.yaml:85`。

```python
import json, pyarrow as pa, pyarrow.parquet as pq

sch = pq.read_schema(
    "data/recsys_cache/522cadb5/train_variants/6cb77dff/train_model_input.parquet")
str_cols = [f.name for f in sch if pa.types.is_string(f.type)]

pp = json.load(open("data/dataset/522cadb5/preprocessor.json"))
feat, cat = set(pp["feature_columns"]), set(pp["categorical_columns"])

print("parquet 文字欄:", str_cols)
print("其中屬於特徵的:", sorted(set(str_cols) & feat))
print("兇手（是特徵、卻沒宣告成 categorical）:", sorted((set(str_cols) & feat) - cat))
```

最後一行印出來的欄名就是答案。`prod_name` **不會**出現在裡面——它有宣告成 categorical，只是延後到 `_pdf_to_X` 才編碼。

### 拿到欄名之後怎麼辦

對每一個兇手欄，二選一（依它是不是有用的類別特徵）：

- **是有用的類別特徵**（例：客群別、通路）→ 加進 `dataset.prepare_model_input.categorical_columns`。它會在 Spark 端就被編成整數，仍是模型特徵。
- **不是模型特徵**（例：ID、自由文字）→ 加進 `dataset.prepare_model_input.drop_columns`。

> ⚠ **這會 bump `base_dataset_version`，需要重建整個 dataset**（就是 log 裡那個大 Spark job）——因為這兩個鍵都參與 dataset 版本雜湊。這一步才讓 training 真的跑得起來。閘門本身（B6）只負責讓你**知道是哪幾欄**、並防止未來重建時再犯，不會替你改 config。

`scripts/suggest_categorical_cols.py` 可用來加速這個決定（見 `docs/pipelines/dataset.md`）：它把高 cardinality 字串欄建議進 `drop_columns`，並把 date／timestamp／binary／複合型欄（同屬本節的 object-dtype OOM 兇手，且不做 cardinality 判定、一律列出）放進一個待人工判斷的 review 區塊——這些欄同樣要二選一（`categorical_columns` 或 `drop_columns`），工具不替你決定。

## 7. 兩個容易被忽略的陷阱

### 觀測性在這個情況下是瞎的

`src/recsys_tfb/io/extract.py:321` 會記錄矩陣大小：

```python
log_data_volume(logger, "extract_Xy.X", X)
```

它問 numpy「這張矩陣多大」，而 numpy 的回答**只算櫃號，不算櫃子**。實測中兩種情況回報的數字完全一樣（皆 0.49 GiB @ 100k 列），但真實記憶體差 4.3 倍。

也就是說，即使跑得到這一行，它也會把 95.9 GiB 報成 22.4 GiB。**不要用這個數字判斷記憶體是否安全。**

（本次事故中它根本沒印出來——行程在更早的 `to_numpy` 就被殺了。）

### 就算記憶體無限大，這條路也走不通

LightGBM 拿到地址矩陣後，會試著把每一格轉成數字（`lightgbm/basic.py:192` 的 `_np2d_to_np1d`，對非 float32/float64 的輸入一律 `np.asarray(mat, dtype=np.float32)`）。碰到那幾格真的是 `"信用卡"` 的：

```
ValueError: could not convert string to float
```

**記憶體不足只是先發生的症狀。真正的病是文字欄混進了特徵欄。**

### （補充）§4 表格裡的「轉換過程暫態」是什麼

pandas 在拼出最終矩陣時，是一個型別群組一個型別群組地填。填 `object` 矩陣時，每個群組都要先整批做成櫃號陣列。最大的那個群組（401 個 float32 欄）會產生一個 `401 × 4,542,746 × 8 bytes ≈ 13.6 GiB` 的臨時櫃號陣列。填 `float64` 矩陣則不需要這個中介。

## 8. 修掉文字欄之後，還是可能不夠

從 log 可以框出這台機器的記憶體上限：`slice_features` 那一步的瞬間用量是 `pdf` + **兩份** `X_df` ≈ 48.3 GiB（`src/recsys_tfb/io/extract.py:258` 的 `.copy()` 是多餘的：`pdf[feature_cols]` 在 pandas 1.5.3 已經回傳獨立副本），它**活下來了**；`to_numpy` 才死。

所以上限落在 48.3 GiB 與致死點之間。而移除文字欄後的需求是 **54.7 GiB**——**落在生死線邊緣，很可能還是會死**。

修掉文字欄是必要條件，不是充分條件。若仍不足，後續選項（成本由低到高）：

| 做法 | `to_numpy` 時的峰值 | 代價 |
|---|---|---|
| 現況 | ~142 GiB | — |
| 移除文字欄 | ~54.7 GiB | 改 config |
| ＋ 提早釋放 `pdf`、拿掉多餘的 `.copy()` | ~38.7 GiB | 改 `extract_Xy` 的取值順序 |
| ＋ 矩陣改用 4 bytes 格子（`to_numpy(dtype=np.float32)`） | ~27.5 GiB | ⚠ 163 個 int64 欄若有值 > 2²⁴ 會失真，套用前需驗值域 |
| 讓 LightGBM 直接讀 Arrow，不經過 pandas / numpy | ~18.5 GiB | 需要 `cffi`（**不是** pyarrow 或 lightgbm 的相依套件，生產環境未必有） |
| 一次只讀一小段、邊讀邊餵（`lightgbm.Sequence`） | ~4 GiB | 需自寫約 25 行的 `ParquetSequence`；只需要 numpy |

最後一列的 `~4 GiB` 裡有 2.8 GiB 是 LightGBM 分箱後的資料集本身（也就是要存下來的 `train.bin`）。那是下限，跑不掉。

## 9. 尚未證實的部分

- **「有 8 欄文字」是推算值**。§5 的減法只給出下界（≥ 5 欄）。確切欄名要靠 §6 的 snippet 在生產環境跑出來。在那之前，這是一個有算術支撐的假設，不是事實。
- **95.9 GiB 是外推**。實測是在 100,000 列上量到每格 34.2 bytes，再乘 45.4 倍。每格成本與資料量無關，所以外推可靠；但沒有在 30.1 億格的規模上真跑過。
- **13.6 GiB 的轉換暫態**是從 pandas 原始碼推導的，未單獨量測。
- **這台機器的記憶體上限只框出區間**（48.3 GiB 與致死點之間），沒有確切值。可查 `cat /sys/fs/cgroup/memory.max`，或 YARN container 的 `driver.memory + driver.memoryOverhead`。
