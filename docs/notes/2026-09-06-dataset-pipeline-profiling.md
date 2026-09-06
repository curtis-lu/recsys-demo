# dataset pipeline 全量重建耗時實測（2026-09-06）

量測人：Claude／worktree `.worktrees/perf-profile`（branch `feat/perf-profile`）
原始資料與腳本：`/Users/curtislu/projects/recsys_tfb/.worktrees/perf-profile/data/verification/`
（gitignored，跨 session 保留；重跑方式見 §3）

---

## 1. 結論先講

**輸出框的大小不能動，能動的只有「怎麼算出它」。**

dataset pipeline 產出的是 learning-to-rank 的訓練框：每個 query group `(time, entity)`
配上它的候選 item，一列一個候選。20 萬客戶那輪是 1446 萬列 × 29 欄。這個大小是
**模型類別決定的**，不是實作決定的（推導見 §2）。想縮小它就得換模型類別，那是建模決策。

在「不改變輸出」的前提下，實測到**兩個**真實機會，都屬於「重複做了不必要的工」。
第三條（feature 表被洗五次）看起來像機會，實測後否決——列在表裡是為了讓下一個人不用再試一次：

| # | 機會 | 性質 | 證據強度 |
|---|---|---|---|
| 1 | AQE 分區太粗 → SortMergeJoin 排序 spill 2.29GB | 設定 | **已實測**：31.1s→24.0s、spill 歸零、三次區間零重疊 |
| 2 | `split_train_keys` 用三次洗牌算一個逐列就能算的東西 | 程式（純冗餘） | **已實測**：Exchange 4→0、6.7s→3.6s、輸出指紋一致、區間不重疊 |
| ~~3~~ | ~~同一張 feature 表被 5 個 split 各洗一次~~ | **已否決** | cache／repartition+cache／bucketing 全部實測無效，見 §6.2 |

**測過並否決的三個假設見 §7**——它們佔本次工作的一半，寫在這裡是因為「這條路不通」
跟「這條路通」一樣是結果。

哪個 node 慢：

| | node | 200K 耗時 | 佔比 | 斜率 k（50K→200K） |
|---|---|---|---|---|
| 1 | `build_train_model_input` | 31.1s | 32% | **1.04** |
| 2 | `split_train_keys` | 15.1s | 16% | **1.07** |
| 3 | `select_sample_keys` | 9.6s | 10% | 0.60 |

k 是 `time ∝ rows^k` 的指數。k≈0＝固定開銷（優化無效），k≈1＝線性（資料放大幾倍就慢
幾倍）。**只有前兩名到了 k≈1**；生產母體是百萬級 entity（本機最大測到 20 萬），
資料再往上走，這兩個吃掉的比重只會更大，其他會被稀釋。

---

## 2. 從排序語意推：什麼是必要的工作

**這一節先寫，因為它決定了後面所有數字該怎麼讀。** 不先確定「什麼是必要的」，
就會把模型的運作機制誤判成實作的浪費（本報告第一版就犯了這個錯，見 §10）。

### 2.1 三張來源表的粒度是不同的，而且必須不同

```
sample_pool    (time, entity, item)   ← 候選集：誰在什麼時候有哪些候選
label_table    (time, entity, item)   ← 標籤：哪個候選被申購了
feature_table  (time, entity)         ← 特徵：這個客戶在這個時點長什麼樣
                       ↑
        少一個 item —— 同一個客戶在同一個月，8 個產品看到的特徵完全一樣
```

feature 少一個鍵**不是缺陷，是特徵的定義**。客戶的總資產、年齡、信用卡使用率，
跟你正在為他評分哪個產品無關。

### 2.2 所以 7 倍複製是機制，不是浪費

`build_model_input` 把 feature 用 `(time, entity)` join 上來，於是 18 個特徵欄被複製到
該客戶的每個候選 item 上：

```
preprocessed_feature_table   2,761,863 列 × 25 欄
train_keys                  14,462,040 列 ×  6 欄
train_model_input           14,462,040 列 × 29 欄     ← 特徵欄複製約 7 倍
```

為什麼非這樣不可：一個 flat GBDT 要學會「高資產客戶偏好基金」這種 **entity × item
交互作用**，就必須讓每個 `(entity, item)` 候選各自成為一列，而那一列上要同時看得到
entity 特徵與 item 身分。複製是這件事的實作方式，也是唯一的實作方式——除非換成
per-item 獨立模型，而那條路的代價（正／負遷移翻轉、葉預算競爭）repo 的
`docs/handbooks/gbdt/gbdt_learning_to_rank.md` 已經分析過。**那是建模決策，不是效能決策。**

### 2.3 有沒有「對排序沒貢獻」的列可以砍掉？沒有

排序損失若是 lambdarank，零正例的 query group 產生不出任何 pair，梯度貢獻精確為零，
是可以砍的。但本專案的 `conf/base/parameters_training.yaml:27` 是：

```yaml
objective: binary          # pointwise
```

**pointwise 二元分類用到每一列**，包含零正例群裡的全負例。`pipeline.py` 裡
`filter_groups_with_positives` 只套用在 val／test（那兩者被 mAP／NDCG 評估，
零正例群本來就被排除）而不套用在 train 的註解——

> train / train_dev / calibration are NOT filtered: their losses use every row

——對現行設定是正確的。

> 若哪天把 `objective` 改成 `lambdarank`／`rank_xendcg`（config 第 39 行已備妥註解），
> 這句話就不再成立，train 側的零正例群會變成純浪費。**這是一條「改 A 會讓 B 的宣稱
> 失效」的耦合，值得在切換 objective 時一併檢查。** 我沒有量現行資料裡零正例群佔多少，
> 因為在 `objective: binary` 下這個數字不影響任何決策。

### 2.4 結論

**語意上沒有可壓縮的空間。** 輸出框就是模型類別要求的大小。所以優化只能發生在
「怎麼算出這個框」，而不是「算什麼」。§5 和 §6 都在這條界線之內。

---

## 3. 怎麼量的（可以重跑）

```
環境    本機 local[*]，8 核 / driver 8g，合成資料
情境    全量重建（每輪先 --reset 清空 warehouse + metastore，從零算 13 個月）
規模    5000 / 50000 / 200000 客戶，各跑 3 次取中位數
工具    runner 內建的 per-node load/func/save（logs/**/dataset_*.jsonl）
        ＋ Spark event log（shuffle 讀寫、spill、stage 明細）
```

`data/verification/` 下的腳本：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/perf-profile
bash data/verification/run_profile.sh <label>                       # 跑一輪，收好原始紀錄
.venv/bin/python data/verification/analyze.py runs/<label>          # 單輪 node 表
.venv/bin/python data/verification/summarize.py                     # 跨規模總表
.venv/bin/python data/verification/drilldown.py runs/<label> <node> # 挖 node 內部 stage
.venv/bin/python data/verification/bench_split_train_keys.py        # §6.1 的改寫對照
.venv/bin/python data/verification/bench_feature_cache.py           # §6.2 的 cache / bucketing 對照
.venv/bin/python data/verification/bench_join_order.py              # §7.1 的假設實驗
.venv/bin/python data/verification/bench_copart.py                  # §7.2 的假設實驗
```

改資料規模：`scripts/generate_synthetic_data.py:19` 的 `INITIAL_CUSTOMERS`。

**這輪為了量測改了三個東西，都在 worktree 內，沒進 main，也沒有動任何 `src/`：**

| 改了什麼 | 為什麼 |
|---|---|
| `conf/spark-local/spark-defaults.conf`：driver 4g→8g | 20 萬那輪要吃得下；三種規模必須同一個值，否則大規模多給記憶體會讓時間變快、被誤讀成「資料量影響不大」 |
| 同檔：開 `spark.eventLog` | 要 shuffle／spill 明細。原本刻意關著 |
| `run_profile.sh` 設 `TZ=Asia/Taipei` | 見 §9.1 |

---

## 4. 總表

x 軸用 sample_pool 的列數（pipeline 讀的是列，不是客戶）：

```
      597,864 列  (5000 客戶)     牆鐘中位數  24.4s      三次 20.4 / 24.4 / 24.5
    5,978,936 列  (50000 客戶)    牆鐘中位數  40.9s      三次 40.8 / 40.9 / 50.5
   23,915,848 列  (200000 客戶)   牆鐘中位數 101.4s      三次 100.8 / 101.4 / 122.3
```

| node | 5K | 50K | 200K | k(5K→50K) | k(50K→200K) | 200K shuffle 寫 | 200K spill |
|---|---:|---:|---:|---:|---:|---:|---:|
| `build_train_model_input` | 2.0 | 7.4 | **31.1** | 0.57 | **1.04** | 785.7MB | **2.29GB** |
| `split_train_keys` | 2.4 | 3.4 | **15.1** | 0.15 | **1.07** | 182.2MB | 0 |
| `select_sample_keys` | 2.5 | 4.2 | 9.6 | 0.23 | 0.60 | 0 | 0 |
| `build_train_dev_model_input` | 0.9 | 2.9 | 7.1 | 0.51 | 0.63 | 380.4MB | 0 |
| `build_calibration_model_input` | 0.7 | 2.4 | 5.5 | 0.55 | 0.58 | 373.8MB | 0 |
| `filter_val_model_input` | 0.9 | 2.4 | 5.2 | 0.42 | 0.56 | 348.4MB | 0 |
| `apply_preprocessor_to_features` | 1.6 | 3.2 | 5.0 | 0.31 | 0.34 | 1.4KB | 0 |
| `filter_test_model_input` | 1.0 | 2.7 | 4.8 | 0.45 | 0.41 | 106.1MB | 0 |
| `select_val_keys` | 1.0 | 1.4 | 3.6 | 0.15 | 0.69 | 17.4MB | 0 |
| `validate_data_consistency` | 2.0 | 2.4 | 2.9 | 0.07 | 0.15 | 7.1KB | 0 |
| `select_calibration_keys` | 0.5 | 0.9 | 2.1 | 0.27 | 0.61 | 0 | 0 |
| `select_test_keys` | 0.7 | 0.9 | 2.0 | 0.07 | 0.59 | 12.0MB | 0 |
| `fit_preprocessor_metadata` | 0.9 | 1.1 | 1.7 | 0.07 | 0.31 | 4.3KB | 0 |
| `validate_numeric_precision` | 0.3 | 0.2 | 0.3 | — | — | 0 | 0 |
| `build_val_model_input` | 0.1 | 0.1 | 0.1 | — | — | 0 | 0 |
| `build_test_model_input` | 0.1 | 0.1 | 0.1 | — | — | 0 | 0 |

**固定開銷**（Spark session 啟動＋Layer-1 config gate＋收尾，落在所有 node 之外）：
5K 6.9s、50K 5.3s、200K 5.4s。與資料量無關，生產跑到幾十分鐘時可忽略。

### 讀這張表的兩個陷阱

**`build_val_model_input` / `build_test_model_input` 的 0.1 秒不代表它們免費。**
Spark 是懶的：這兩個 node 只組計畫，真正的計算落在下游 `filter_val_model_input` /
`filter_test_model_input` 的 save 階段。它們的成本要看 `filter_*` 那兩列。

**`validate_data_consistency` 的 2.9 秒全在 `func`、不在 `save`。**
它是 side-effect node（`outputs=None`），時間花在把 distinct 結果 collect 回 driver。
k=0.15 —— 實測支持它 docstring 裡的成本不變量宣稱（ADR-0006：collect 量由 item 基數
決定，不由列數決定）。

---

## 5. 設定層：唯一已實測驗證的機會

`conf/spark-local/spark-defaults.conf` 完全沒設 shuffle 參數
（`conf/base/parameters.yaml:27–31` 有建議值但註解掉），所以走 Spark 預設：
AQE 開啟、`advisoryPartitionSizeInBytes` 64MB。

20 萬客戶那輪，AQE 把 `build_train_model_input` 最重的 shuffle 合併成 **9 個分區**，
每個 task 扛約 57MB 進 SortMergeJoin 的排序緩衝，然後 spill。

**實驗**：只改 `advisoryPartitionSizeInBytes` 為 16m，其餘不動，3 輪。

| | baseline（64MB） | tuned（16MB） |
|---|---|---|
| `build_train_model_input` 秒（三次） | 31.0 / 31.1 / 35.4 | **22.6 / 24.0 / 26.7** |
| spill（三次） | 2.29 / 2.29 / 2.29 GB | **0.00 / 0.00 / 0.00 GB** |
| task 數（中位） | 44 | 85 |

三次區間零重疊，spill 完全歸零。

代價要一起看：另外兩個 build 節點反而略慢（`build_train_dev_model_input` 7.1→8.0s、
`build_calibration_model_input` 5.5→6.3s）——它們本來沒 spill，分區變多只多付排程成本。
整條 pipeline 牆鐘 101.4s→94.4s，但三次區間（100.8–122.3 vs 84.7–104.6）重疊嚴重，
**pipeline 層級的改善不宣稱，node 層級的宣稱**。

> ⚠ **`16m` 這個值不能搬去公司環境。** 本機是 `local[*]` 8 核單一 JVM，公司是多 executor
> 叢集，分區該切多大是不同尺度的問題。能搬的是**診斷方法**（看 spill、看每個 task 扛多少）
> 與**結論形狀**（這個 node 的 shuffle 分區偏粗），不是數值。

---

## 6. 程式層：兩處純冗餘計算

兩者都在「不改變輸出」的界線內——它們算的東西沒有進到結果裡。

### 6.1 `split_train_keys` 用洗牌去算一個逐列就能算出來的東西（已實測）

**這個 node 要保證的是**：同一個 entity 的所有列必須落在 train / train-dev 的同一邊
（train-dev 是 HPO 每個 trial 的 early-stopping 驗證集，entity 跨邊就是洩漏）。

**現況怎麼達成**（`src/recsys_tfb/pipelines/dataset/nodes.py` `split_train_keys`）：

```python
entity_df = sample_keys.select(*split_cols).distinct()                        # ← 洗牌
entity_df = entity_df.withColumn("_bucket", spark_bucket(entity_df, split_cols, seed, site=...))
dev_entities   = entity_df.filter(F.col("_bucket") <  threshold).select(*split_cols)
train_entities = entity_df.filter(F.col("_bucket") >= threshold).select(*split_cols)
train_keys     = sample_keys.join(train_entities, on=split_cols, how="inner")  # ← 洗牌
train_dev_keys = sample_keys.join(dev_entities,   on=split_cols, how="inner")  # ← 洗牌
```

**為什麼那三次洗牌不必要**：`spark_bucket`（`src/recsys_tfb/utils/hashing.py:31`）是

```python
F.crc32(F.concat_ws("|", *parts)) % F.lit(n_buckets)
```

——**純粹逐列的運算式**，輸入只有這一列自己的 `split_cols` 值。而 `sample_keys` 本來就
帶著那幾欄。所以每一列可以直接算出自己的歸屬，不需要先聚出 entity 清單再 join 回來查：

```python
bucket = spark_bucket(sample_keys, split_cols, seed, site="split_train_dev")
train_keys     = sample_keys.filter(bucket >= threshold)
train_dev_keys = sample_keys.filter(bucket <  threshold)
```

而且改寫後「同 entity 同一邊」**由建構保證**（bucket 是 `split_cols` 的函數，同輸入必
同輸出），不再是 distinct + join 湊出來的性質。

**實測**（`bench_split_train_keys.py`，20 萬客戶的 `sample_keys` 1607 萬列，3 輪對調先後）：

| | Exchange 數 | 輸出 | 耗時（三次） | 中位數 |
|---|---|---|---|---|
| 現況 distinct+join | 4 | train 14,462,040／dev 1,614,058 | 8.9 / 6.7 / 6.6 | 6.7s |
| 逐列 filter | **0** | 同上，指紋一致 ✓ | 4.0 / 3.4 / 3.6 | **3.6s** |

**−47%，區間不重疊**（現況最小 6.6s > 改寫最大 4.0s）。

> 這個 6.7→3.6 是**單獨量這一段**的數字（讀已落地的 `sample_keys`、寫普通 parquet），
> 不等於 §4 表中該 node 的 15.1s→8s。node 在 pipeline 裡還要走 catalog 的 Hive 分區寫入
> 與版本路徑、加上 `isEmpty()` 的 action。**縮掉的是洗牌那部分，不是整個 node。**

**動手前要先釐清一條語意差異**（不是效能問題）：`inner join` 會丟掉 `split_cols` 含
NULL 的列（NULL 不等於 NULL），逐列 filter 會保留它們（`concat_ws` 跳過 NULL，照樣算得
出 bucket）。本次實測資料 NULL 為 0 列，所以沒咬到；上游 source_etl 有
`primary_key_not_null`，但那個保證的強度見 §8。**這條要當成規格決定來處理，
不是實作細節。**

> 我先前把這條寫成「加 `.cache()`」。那是治標：cache 讓那三次洗牌少算一次，
> 但那三次洗牌本來就不該存在。

### 6.2 同一張 feature 表被 5 個 split 各洗一次

`preprocessed_feature_table` 被 `build_model_input` 用同一個 `(time, entity)` 鍵
join 五次（train、train_dev、val、test、calibration），每次都重新 shuffle 一遍。
20 萬客戶那輪五個節點的 shuffle 寫入：

```
build_train_model_input          785.7 MB
build_train_dev_model_input      380.4 MB
build_calibration_model_input    373.8 MB
filter_val_model_input           348.4 MB   （val 的 build 是懶的，成本落在這裡）
filter_test_model_input          106.1 MB
                        合計    1994.4 MB
```

直覺的修法是 cache 或 bucketing，讓 feature 側只付一次成本。**兩個都實測了，都沒用。**

**實測**（`bench_feature_cache.py`，五個真實 split 的 keys 各 join 一次並寫出，3 輪輪換先後）：

| | 中位數 | 對 A | 區間 |
|---|---|---|---|
| A 現況（每個 split 重讀表） | 23.1s | — | — |
| B `.cache()` | 27.3s | **+18%（更慢）** | 與 A 重疊 |
| C `.repartition(N, base_key).cache()` | 22.8s | −1% | 與 A 重疊 |

**B 更慢**：把 276 萬列 × 24 欄物化進記憶體，比直接重讀 parquet 貴——parquet 是欄式的，
掃描很快。

**C 在計畫層面確實有效，但效益是零。** 計畫骨架：

```
A 現況                                  C repartition+cache
  SortMergeJoin                           SortMergeJoin
    :- Sort + Exchange(keys)                :- Sort + Exchange(keys)
    +- Sort + Exchange(features)            +- Sort + InMemoryTableScan
              ENSURE_REQUIREMENTS                      +- Exchange(features)
              ← 每次 join 都洗                              REPARTITION_BY_NUM
                                                          ← 在快取裡，只跑一次
```

feature 側的 per-join exchange 真的消失了，時間卻沒動。原因是**瓶頸不在 feature 側**：
keys 側的 exchange ＋ sort 沒省到（train 是 1446 萬列，feature 只有 276 萬），
SortMergeJoin 兩邊的 Sort 也沒省到（快取跳過 exchange，跳不過 sort），寫出更沒省到。

**這連帶否決了 bucketing。** bucket feature 表拿到的就是 C 拿到的東西。要拿到完整效益得
**兩側都 bucket**，但 keys 是每次跑動態產生、且在 `(t,e,i)` 粒度，做不到。
（`src/recsys_tfb/io/` 本來也沒有 bucketing 支援：`grep -rn "bucketBy\|numBuckets"` 零命中。）

> **架構上的附註**：就算 C 有效也拿不到。`runner.py:141` 是
> `inputs = [catalog.load(name) for name in node.inputs]`——每個 node 重新 load，
> 拿到不同的 DataFrame 物件，而 `core/catalog.py` 沒有任何 cache／persist 機制。
> 在 node 裡加一行 `.cache()` 另外四個 node 看不到。要共用得動 catalog／runner。
>
> **規模相依性**：本結論在「keys 比 feature 大約 5 倍」的形狀下成立。生產的比例應該
> 相近（entity 母體放大時兩者同步放大），但我沒有在生產規模驗證過。

### 6.3 `select_sample_keys`：看過，沒問題

9.6 秒，1 個 stage、8 個 task、**完全沒有 shuffle**。掃 2392 萬列的 sample_pool、
寫出 1446 萬列的 keys。時間就是磁碟讀寫。k=0.60。

除非減少寫出的列數（那是抽樣策略的決定，不是效能決定），沒有技術槓桿。
**寫在這裡是為了說明「我看過它、它沒問題」，不是建議動它。**

---

## 7. 測過並否決的三個假設

這一節佔本次工作約一半的時間。負面結果也是結果，寫下來是為了下一個人不用再走一次。

### 7.1 「換 join 順序可以少洗一次大表」→ 實測更慢，作廢

**假設**：兩個 join 的鍵不同（`(t,e,i)` 然後 `(t,e)`），大表被洗兩次。若把 feature join
排前面，`(t,e)` 的分區在邏輯上已經滿足 `(t,e,i)` 的需求（同 entity 同時間的列必在同一
分區），就只要洗一次。

**實測**（`bench_join_order.py`，1446 萬列實際寫出，兩輪）：

| | Exchange 數 | 最好耗時 |
|---|---|---|
| A 現況 label→feature | 4 | **25.2s** |
| B 換順序 feature→label | 4 | 27.6s |

Exchange 數一樣，時間更慢。**原因回頭看很明顯**：順序 B 把 29 欄的寬框帶進第二個 join，
第二次洗牌要搬的位元組更多。四種組合的列數（14,462,040）與 label 總和（1,658,574）完全一致。

### 7.2 「`requireAllClusterKeysForCoPartition=false` 能讓分區被複用」→ 是快取假象

Spark 3.3 這個開關預設 `true`，要求分區鍵與 join 鍵完全相同、不接受子集。
第一版實驗顯示關掉它讓現況順序快 4 秒（25.2s→21.0s）。

**但推理說不該有效**：這個開關放寬的是「分區運算式是所需分群鍵的**子集**」；
現況順序第二個 join 需要 `(t,e)` 分群，而第一個 join 之後的分區是 `(t,e,i)`——是分群鍵的
**超集**，那本來就不合法（同一個 `(t,e)` 會散在不同分區）。

第一版實驗的瑕疵：四格在每輪裡的執行順序固定，`false` 那格永遠排在後面，磁碟快取較熱。

**受控重測**（`bench_copart.py`，每輪對調先後，4 輪）：

```
true（預設）  26.7 / 23.3 / 20.8 / 22.6    中位數 23.0   最小 20.8
false        20.9 / 21.8 / 21.4 / 21.3    中位數 21.4   最小 20.9
                                          ← 區間重疊（20.8 < 21.8）
```

`true` 的 26.7 是整個 session 的第一次寫出，冷快取。**差距落在雜訊內，這個開關無效。**

### 7.3 「零正例的 query group 對排序沒貢獻，可以從 train 砍掉」→ 對現行設定不成立

見 §2.3。這在 `objective: lambdarank` 下成立，在現行的 `objective: binary` 下不成立。

---

## 8. 一個不是效能的發現：輸出粒度沒有被釘住

**與優化無關，但比上面任何一條都嚴重，所以單獨列。**

`build_model_input` 做兩個 LEFT join。兩個右表若在各自的 join 鍵上有重複列，輸出就會
悄悄變成 N 倍大——不報錯、不警告。`nodes.py:831` 的註解**自己點名了這個失敗**：

> its failure mode is a silently N-times-too-large dataset

它用 `require_columns_present` 擋住了其中一個成因（join 鍵漏了 item 欄），但另一個成因
（右表有重複列）沒有任何檢查：

```
怕的結果：資料悄悄變成 N 倍大
  成因 A：join 鍵漏了 item 欄      → require_columns_present 擋住 ✓
  成因 B：label／feature 有重複列  → 沒有任何檢查 ✗
```

而 docstring 的後置條件寫的是**欄位**、不是粒度：

> Post-condition: identity, label and every feature column survive the joins.

### 為什麼「上游有契約」不算保證

三張來源表的 config 都宣告了 `primary_key` ＋ `quality_checks.max_duplicate_key_ratio: 0.0`
（`conf/base/parameters_{label,sample_pool,feature}_etl.yaml`），由 source_etl 的
`OutputChecker` 檢查，不變量 A32 再守著宣告本身。但那不是保證：

| 情況 | A32 擋得住嗎 | 唯一性還在嗎 |
|---|---|---|
| 刪掉 `quality_checks` | ✓ 擋 | — |
| **`primary_key` 和 `quality_checks` 一起刪** | **✗ 放行** | ✗ |
| **設成 `max_duplicate_key_ratio: 0.5`** | **✓ 通過**（[0,1) 合法） | ✗ 允許 50% 重複 |
| 來源表不是這個 repo 的 source_etl 產的 | 檢查從沒跑過 | 不知道 |

第二列是 A32 自己 docstring 寫明的「deliberate residual」。最後一列最關鍵：CLAUDE.md
開宗明義寫「來源表由使用者自定義」——生產的表很可能是別的團隊建的，`OutputChecker`
從未跑過它們。

**所以現況是：使用端的正確性，依賴另一條 pipeline 的 config、在另一個時間點、對一張
可能不是它產出的表、宣告的一個容忍度。** 而 ADR-0006 記載這個形狀已經真實發生過一次
（`feature_table` 有 `primary_key` 卻無 `quality_checks`，半年沒被發現）。

**真正的保證會長這樣**：在使用端斷言粒度本身。

```python
# 兩個 LEFT join 都不該改變列數 —— keys 的粒度就是 model_input 的粒度（ADR-0005）
```

代價是兩個 count。**要不要付這個代價、放在 Layer-2 資料閘還是 node 的後置條件，
是架構決策，不是我該替你決定的。** 這裡只負責指出：現在沒有人在守它。

---

## 9. 量測本身撞到的兩個坑

### 9.1 機器時區 ≠ Spark session 時區 → 月份過濾靜默篩出 0 列

這台 Mac 是 `Asia/Seoul`（UTC+9），`conf/spark-local/spark-defaults.conf` 的
`spark.sql.session.timeZone` 是 `Asia/Taipei`（UTC+8）。

pipeline 各處用 `pd.Timestamp` 比對 Hive 表的 DATE 欄。PySpark 把 `pd.Timestamp` 轉成
TIMESTAMP 字面值時用 Python 端的本地時區，Spark 比對 DATE 欄時用 session 時區——
差一小時，`date IN (timestamp)` 恆不成立。

實測（同一張表、同一個月份）：

```
F.col("snap_date").isin([pd.Timestamp("2025-01-31")])   →      0 列
F.col("snap_date").isin([datetime.date(2025,1,31)])     → 40,000 列
F.col("snap_date").isin(["2025-01-31"])                 → 40,000 列
```

**危險的不是它會壞，是它壞的方式**：不報錯、不警告，靜默回傳空集合，然後在三個 node
之後以「`sample_pool` 從沒產出這 8 個產品」這種完全指錯方向的訊息爆掉。

**解法**：跑本機 pipeline 前 `export TZ=Asia/Taipei`。公司環境伺服器是台北時間，不會遇到。

> 建議收進 `docs/operations/known-pitfalls.md`（符合該檔「改了沒生效／訊息指錯方向」
> 那一類的收錄標準）。我沒有自行加，那是 main 上的檔案。

### 9.2 `pipeline.py` docstring 的 node 數字漂了

`create_pipeline` 的 docstring 寫「the full DAG (13 nodes, or 15 with calibration)」。
實際 `grep -c "Node("` 得 16，`tests/test_pipelines/test_dataset/test_pipeline.py:25/31`
斷言 **14 / 16**。測試是對的，docstring 少算一個。一行的文件修正。

---

## 10. 這份報告不能回答什麼

1. **絕對秒數不能搬到生產。** 本機最大測到 20 萬客戶（sample_pool 2392 萬列），
   生產是百萬級 entity。能搬的是斜率與形狀。
2. **設定的建議值不能搬。** 見 §5 的警告框。
3. **沒有量記憶體峰值。** 只量了 Spark 自報的 spill。想知道「生產會不會 OOM」，
   這份只能說哪裡在 spill，不能說離爆掉還有多遠。
4. **沒有量 `--only-test-months` 與每月增量模式。** 只量了全量重建。
5. **k 值是兩點取對數斜率，不是擬合。** 三個規模各 3 次，樣本很小。
   k=1.04 與 k=1.07 之間的差距不要當真；「這兩個到了線性、其他還沒」才是可信的部分。
6. **§6.2 的 bucketing 只有理論，沒有實測。** 三個機會裡唯一沒有數字背書的一條。

### 方法論的自我更正

本報告第一版把「7 倍特徵複製」列為優化候選。那是錯的框架：它是 flat GBDT 學
entity×item 交互作用的機制，不是實作的浪費。錯誤來源是**從 Spark 執行計畫往上推**
（看到兩個 join 鍵不同 → 猜可以省），而不是**從排序目標往下推**（先問這個模型類別
要求什麼樣的訓練框）。

§7 的三個否決假設全部出自同一個推理方向。現在的 §2 是改過來之後重寫的。

---

## 11. 待拍板的決定（下一個 session 的起點）

**這一節是交接面。** 只讀這份報告就能開工，不需要產生它的那段對話。

量測已經結束，剩下的都是**決定**，而且其中兩條不是效能問題。三條都拍板之後才開票
（`/to-tickets`），一票一個，各自開 PR——三者的風險性質完全不同，綁在同一個 commit 會
讓可逆的那條被不可逆的那條拖住。

### D1 — `split_train_keys` 改寫後，`split_cols` 含 NULL 的列要丟掉還是保留？

- **現況**：`inner join` 會丟掉（NULL 不等於 NULL）。逐列 filter 會保留（`concat_ws`
  跳過 NULL，照樣算得出 bucket）。
- **為什麼是決定不是細節**：現況的「丟掉」看不出是有人決定的，比較像 inner join 的
  副作用。改寫等於順手改掉它，那需要有人明確說「保留才對」或「丟掉才對」。
- **證據**：本次 20 萬客戶資料 NULL 為 0 列，所以兩種寫法實測輸出逐列相同（§6.1）。
  上游 source_etl 有 `primary_key_not_null`，但那個保證的強度見 §8。
- **建議**：這是規格題，寫成 ADR 而不是埋在實作裡。傾向「保留 ＋ 明確報數」——
  靜默丟資料是本 repo 反覆踩過的形狀（§8、§9.1 都是同一類）。信心中等。

### D2 — AQE 的 shuffle 設定要不要進 `conf/base`？

- **實測結論**：`advisoryPartitionSizeInBytes: 16m` 讓 `build_train_model_input`
  31.1s→24.0s、spill 2.29GB→0（§5）。
- **為什麼是決定**：`16m` 是**本機 `local[*]` 8 核**量出來的。公司環境是多 executor
  叢集，該值完全不同。進 `conf/base` 等於把本機數字當成通用預設。
- **選項**：(a) 只進 `conf/spark-local`，生產環境另外量；(b) 進 `conf/base` 但寫清楚
  它是可調的；(c) 不進版控，只寫進文件當作調校起點。
- **建議**：(a)。`conf/base/parameters.yaml:27–31` 已經有一段被註解掉的 shuffle 建議值，
  那段註解的存在本身就說明前人也判斷「不該給死值」。信心中等偏高。

### D3 — 要不要在 `build_model_input` 釘住輸出粒度？

- **不是效能問題**（見 §8），但它決定未來還能不能做假設唯一性的優化。
- **現況**：docstring 的後置條件寫的是**欄位**（"identity, label and every feature column
  survive the joins"），不是粒度。而 `nodes.py:831` 的註解自己點名了怕的失敗
  （"silently N-times-too-large dataset"），只擋了成因 A（join 鍵漏欄），沒擋成因 B
  （右表有重複列）。
- **選項**：(a) node 內加後置條件斷言；(b) 放進 Layer-2 資料閘（`core/consistency.py`
  加 predicate，符合 CLAUDE.md「不得在各 pipeline ad-hoc 散落」的規定）；(c) 不做，
  記進 `docs/agents/deliberate-non-goals.md`。
- **代價**：兩個 count。ADR-0006 對這條 pipeline 的成本不變量有明確立場，**選 (b) 前
  必須先讀它**——它可能已經決定過「不為此付一次掃描」。
- **建議**：先讀 ADR-0006 再決定，不要跳過。若它已經否決過同類檢查，那 (c) 才是對的，
  而且要在 `deliberate-non-goals.md` 補一筆說明「粒度沒被釘住」是已知且刻意的。

### 開票前的檢查

- **本 worktree 有未提交的量測用改動，不可以進 feature 分支**：
  `conf/spark-local/spark-defaults.conf`（driver 8g ＋ eventLog）與
  `scripts/generate_synthetic_data.py`（`INITIAL_CUSTOMERS = 200000`）。
  它們是量測 harness，不是產品改動。
- `src/` 本次一個字都沒動。任何「已經改好了」的印象都是錯的。
- 三個機會裡只有 §5、§6.1 兩條存活；§6.2 與 §7 的四條全部實測否決，**不要重新提案**。
