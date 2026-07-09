# Spark session lifecycle：HPO 之後 predict 撞 stopped SparkContext

- 日期：2026-07-09
- 狀態：設計已核可，待轉實作計劃
- 分支：`feat/spark-session-lifecycle`

## 1. 問題

公司環境跑 training pipeline，長時間 HPO 結束後，`predict_and_write_test_predictions`
寫入 `training_eval_predictions` 時失敗：

```
Fallback: building SparkSession (yaml=conf/local/parameters.yaml, connection settings from SPARK_CONF_DIR)
Active SparkSession already exists; cluster-level configs in spark_configs will be ignored by PySpark.
...
java.lang.IllegalStateException: Cannot call methods on a stopped SparkContext
An error occurred while calling ... defaultParallelism
The currently active SparkContext was created at: (No active SparkContext.)
```

目標：HPO 之後，pipeline 仍能可靠完成 test prediction 寫入與後續 evaluation。

## 2. 根因

錯誤的**位置**在 predict 節點，成因**不在**那裡。分兩層。

### 2.1 Layer 2：救援路徑是空操作（已證實）

那兩行 log 連在一起是自相矛盾的，這就是關鍵證據：先宣告「要重建」，緊接著宣告
「已經有 active session」。

呼叫鏈：

1. `HiveTableDataset.save()` 呼叫 `_get_spark()`（`src/recsys_tfb/io/hive_table_dataset.py:206-209`），
   走無參數的 `get_or_create_spark_session()` → `_fallback_create()`
   （`src/recsys_tfb/utils/spark.py:98`）。
2. `_is_session_alive()`（`utils/spark.py:68`）**正確**判定 session 已死。這是
   `73c45ac`（PR #75）加的 `isStopped()` 探測，它有效。於是印出第一行 log，決定重建。
3. 但「重建」是委派給帶 config 的 mode-1 路徑（`utils/spark.py:146` → `utils/spark.py:58-65`），
   最終呼叫 `SparkSession.builder...getOrCreate()`。
4. PySpark 的 `getOrCreate()` 只在 `SparkSession._instantiatedSession._sc._jsc is None`
   時才真的重建（`pyspark/sql/session.py:264`）。而 `_jsc` 只有在 **Python 端**呼叫
   `SparkContext.stop()` 時才被設為 `None`（`pyspark/context.py:568`）。
   JVM 端自行停止 context 完全不會動到 Python 端狀態。
   `SparkContext.getOrCreate()` 同樣只看 `_active_spark_context is None`（`pyspark/context.py:482`）。
5. 於是 `getOrCreate()` 把**同一個死 session** 原封不動回傳，途中順便印出第二行 warning。
   那行 warning 的真正意思是「這次重建不會發生」。
6. `createDataFrame(pandas_df)` → `sparkContext.defaultParallelism` → Scala 端
   `assertNotStopped()` → `IllegalStateException`。

**PR #75 修的是偵測，不是復原。** 兩者是不同的事：程式碼正確地說出「這個 session 死了」，
然後呼叫了一個不會重建的重建。

本機以 `local[1]` 模擬「只停 JVM 內的 SparkContext」（`spark.sparkContext._jsc.sc().stop()`，
等同 cluster 端殺掉 app），對當前 main 的 `utils/spark.py` 重現，五個檢查點全部成立：

| 檢查點 | 結果 |
|---|---|
| JVM 端停掉後 `_jsc` / `_active_spark_context` / `_instantiatedSession` 全部殘留 | 是 |
| `_is_session_alive()` 正確回報 dead | 是 |
| `get_or_create_spark_session()` 的「重建」回傳同一個死 session | 是 |
| `createDataFrame` 炸在 `oNN.defaultParallelism` + `stopped SparkContext` | 是 |
| 先 Python 端 `.stop()` 清狀態、再重建 → 拿到新 context 且可用 | 是 |

最後一列就是修法。此機制與環境無關（PySpark 3.3.2 為釘版本）。

### 2.2 Layer 1：誰停掉了 context（未證實，但範圍已收斂）

錯誤訊息本身排除了兩個嫌疑：

- 出現 `IllegalStateException` 而非 `Py4JNetworkError`，代表 **driver JVM 還活著、
  py4j gateway 還通**。不是 driver JVM 崩潰或被 OOM killer 砍掉。
- `The currently active SparkContext was created at: (No active SparkContext.)`
  只有在 JVM 端 `SparkContext.activeContext` 被清空時才印得出來，而清空它的唯一途徑
  是有人在 JVM 內呼叫過 `SparkContext.stop()`。

當前 main 的 training pipeline 可達路徑內沒有任何 `.stop()`（`.stop(` 的命中只有
`scripts/local_spark_setup.py:101`、`scripts/suggest_categorical_cols.py:185` 兩個
獨立 dev script，以及測試檔）。所以停掉它的是 Spark 自己的元件。最符合「HPO 期間
driver-local 跑數小時、完全不碰 Spark」這個情境的候選是 `YarnClientSchedulerBackend`
偵測到 YARN application 已結束後主動呼叫 `sc.stop()`。

**application 為何結束（閒置回收 / preemption / AM 失敗 / token 過期）需要叢集端證據，
目前只有應用層 stdout/stderr，無法回溯歸因。** 本設計因此以「context 會死」為前提，
並在這一輪埋入足以在下次失敗時斷定死因類型的 instrumentation。

### 2.3 歷史脈絡（解釋為何這條路徑從未被驗證過）

| commit | 日期 | 內容 |
|---|---|---|
| `fb0d4c4` | 2026-05-14 12:45 | `tune_hyperparameters` 加入 `SparkSession.getActiveSession().stop()`（當時誤判效能問題根因） |
| `03eafb4` | 2026-05-14 12:54 | `_fallback_create` 加入「stopped session 視為無 session、重建」的邏輯，承接上一個 commit |
| `85b28699` | 2026-05-14 16:36 | **移除**那個 `.stop()`，改為 pin `OMP_NUM_THREADS`（真因是 thread oversubscription） |
| `73c45ac` | 2026-06-11 | `_is_session_alive` 從 `_jsc is not None` 硬化為 `isStopped()`（PR #75） |

`fb0d4c4` 的 stop 走的是 **Python 端** `SparkSession.stop()`，它會把 `_jsc` 設為 `None`、
清掉 `_instantiatedSession`，因此 `03eafb4` 的重建**真的會發生**。也就是說，這條重建路徑
從出生到現在，只在「Python 自己停的」情況下有效，**從未處理過 JVM 端自行死亡的情況**。

本設計會在 `tune_hyperparameters` 重新放回一個 stop（見 §4.2）。它與 `fb0d4c4` 的差別
必須寫在程式碼註解裡：當年那個 stop 是誤診效能問題的產物，且它所依賴的重建路徑無法處理
JVM 端死亡；現在這個 stop 是刻意的資源釋放，且 §4.1 讓重建對兩種死法都有效。

### 2.4 順帶發現（不是本次死因，但會讓修復失效）

- `_fallback_create` 讀 `CONF_ENV`（`utils/spark.py:117`），但全 repo 沒有任何地方**寫**
  這個環境變數。`--env` 的 typer 預設值是 `"local"`（`__main__.py:670` 等），本次兩者
  恰好一致所以沒出事；一旦真的下 `--env production`，重建會靜默用錯 env 的設定。
- `_fallback_create` 只讀 base 的 `parameters`，**不讀** `conf/base/parameters_training.yaml:6`
  的 `spark:` 區塊；而 entrypoint 走的是 `_load_spark_config(config, "training")`
  （base + pipeline 合併，`__main__.py:58-75`）。就算重建成功，資源配置也與原本不同。

## 3. 目標與非目標

**目標**

- HPO 之後 predict / evaluation 能可靠完成。
- 消除「Spark session 閒置數小時」這個成因類別。
- 重建失敗時丟出可讀的例外，而非 `IllegalStateException`。
- 下次若仍失敗，能從應用層 log 斷定 context 死亡的時間點與死因類型。

**非目標**

- 不修 Layer 1 的叢集端成因（無權限、無證據）。
- 不改動 inference / evaluation / dataset pipeline——只有 training 有那段 driver-local 長窗口。
- 不引入重試迴圈。重建失敗即 fail-fast。
- 不改動 training pipeline 的 DAG 形狀（見 §4.2）。

## 4. 設計

### 4.1 `utils/spark.py`：把 session lifecycle 收斂成明確的擁有者

現在 `utils/spark.py` 是無狀態的 `getOrCreate` 包裝，真正的狀態藏在 PySpark 的模組級
單例裡——這正是重建變成空操作的原因：它去問 PySpark「有沒有 active session」，
而 PySpark 對 JVM 端的死亡一無所知。

改成薄的 module-level manager，保留現有公開簽名：

- `get_or_create_spark_session(spark_configs=None, enable_hive=False)`
  - **mode-1（`spark_configs` 非 None）**：驗證、**記住**這份 configs 與 `enable_hive`
    作為 canonical 設定，然後建立 session。建立前若存在死 session，先清除（見下）。
  - **mode-2（`spark_configs` 為 None）**：active 且 alive → 直接回傳；否則清除死 session，
    用**記住的** canonical configs 重建。從未 configure 過（scripts / 測試直接呼叫）
    才退回現有的 yaml 讀取路徑。
- `stop_spark_session()`：新增的公開函式。Python 端 `.stop()`，冪等，對已死的 session
  也能安全呼叫。
- `release_spark_session(parameters)`：新增的公開函式。讀 `spark_lifecycle.release_during_hpo`
  開關，log，呼叫 `stop_spark_session()`。回傳是否真的停了（供測試斷言）。
- `_clear_dead_session(session)`：新增的私有函式。對死 session 呼叫 Python 端 `.stop()`，
  使 `_jsc` / `SparkContext._active_spark_context` / `SparkSession._instantiatedSession`
  全部歸零，讓後續的 `builder.getOrCreate()` 真的重建。這是 §2.1 表格最後一列驗證過的動作。

mode-1 順手記住 configs，是因為五個 CLI entry 都必然先以 mode-1 建 session
（`__main__.py:371 / 540 / 709 / 868 / 975`），且回傳值全部丟棄。因此 `__main__.py`
**零改動**，canonical configs 自然就位。

這一步同時修掉 §2.4 的兩個問題：重建不再重讀 yaml 猜 env，而是用 entry 當初實際
用的那份 configs。

`enable_hive` 必須一併記住：`tests/conftest.py:38` 是全 repo 唯一傳 `True` 的地方，
測試中若發生重建而未記住它，Hive support 會靜默消失。真實跑不受影響——local 的 Hive
來自 `conf/spark-local/spark-defaults.conf:8`，是 SparkContext 建立時從 `SPARK_CONF_DIR`
讀的，重建照樣拿得到。

### 4.2 釋放點：`tune_hyperparameters` 函式體的第一行，不新增 DAG 節點

把「不可控的閒置死亡」變成「可控的顯式重啟」。HPO 那數小時本來就白佔 executors。

**這個設計最被低估的好處：重建路徑每次跑都會被走到**，因此不會腐爛成一段沒人驗證過的
緊急路徑。

實作：`tune_hyperparameters` 的第一個語句呼叫 `release_spark_session(parameters)`。
DAG 不動、節點簽名不動。

**為何不新增一個 `release_spark_session` 節點。** 曾經考慮過，但它比較差：

- `Pipeline._topological_sort` 的初始佇列是 `deque(n for n in nodes if in_degree[n] == 0)`
  （`core/pipeline.py:79`），**所有零入度節點依宣告順序全部排在最前面**（該檔
  `pipeline.py:76-78` 的註解自己講明「list position is significant for independent nodes」）。
  一個只吃 `parameters` 的 release 節點入度為 0，它排在 `cache_*` 之前或之後，
  完全取決於宣告位置，且依賴「所有 `cache_*` 節點都是零入度」這個沒人保證的隱含前提。
- 要讓排序穩固，就得讓 release 節點顯式依賴全部的 `*_parquet_handle`。但
  `tune_hyperparameters` 只消費 `train_lgb_handle` / `train_dev_lgb_handle` /
  `val_parquet_handle`（`pipeline.py:103-104`），DAG **並未**強制
  `cache_test_model_input` / `cache_calibration_model_input` 排在 HPO 之前。
  那等於用資料依賴去表達時間約束：**日後任何新增的前置 Spark 節點都必須記得掛進
  release 的 inputs，忘了就靜默失效、無錯誤訊息。**

而 `Runner` 是嚴格循序執行的（`core/runner.py:65` 的 `for node in pipeline.nodes:`）。
所以「在 `tune_hyperparameters` 第一行停掉 session」在構造上就等於「在所有排在它前面的
節點都跑完之後」。不需要拓撲推理，也沒有需要維護的依賴清單。

連帶好處：`tune_hyperparameters` 簽名不變，`--from-node` / `--only-node` 的切片語意
不變，`RESUME_CONTRACTS` 不受影響。

代價與已知缺口，明列如下：

- `prepare_lgb_train_inputs` 與 `persist_sample_weight_report` 也是 driver-local，
  排在 `tune_hyperparameters` 之前。它們執行期間（分鐘級）session 仍活著。
  若日後證據顯示叢集 idle timeout 比這段還短，把呼叫往上搬一個節點即可，是一行的事。
- `--from-node finalize_model` 這類切片會跳過 `tune_hyperparameters`，因此不會 release。
  `finalize_model` 期間 session 保持存活。可接受：那段是分鐘到數十分鐘級，
  且 §4.1 的重建修復仍會接住。
- 副作用藏在 modeling 節點裡。以命名（`release_spark_session`）與一行註解
  （指向 §2.3 的歷史差異）補償；實作本身留在 `utils/spark.py`，節點只呼叫一行。

### 4.3 開關與 config 放置

新增頂層 ops block（**不可**放在 `training:` 底下）：

```yaml
# conf/base/parameters_training.yaml
spark_lifecycle:
  release_during_hpo: true
```

理由：`_model_version_payload`（`core/versioning.py:124-145`）只 hash `params["training"]`，
docstring 明寫頂層 ops block 被結構性排除，且「新增 `training:` 底下的鍵預設會被納入」。
把開關放進 `training:` 會靜默 bump `model_version`、使既有 model 與 cache 失效。

若要對此鍵做型別驗證，必須在 `src/recsys_tfb/core/consistency.py` 加 predicate，
不得在 pipeline 內 ad-hoc 檢查（CLAUDE.md 的 Config consistency gate 規則）。

### 4.4 錯誤處理

新增 `SparkSessionUnavailableError(RuntimeError)`（放在 `utils/spark.py`，與現有
`_fallback_create` 的 `RuntimeError` 同層）。重建失敗時丟出，訊息含：

- 上一個 `applicationId`
- 偵測到 context 死亡的時間戳，與距離上次成功使用 Spark 的秒數
- 建議動作

必須明確處理的兩種失敗：

- **JVM 還活著、context 死了**：可 in-process 重建（本設計的主要情境）。
- **py4j gateway 本身已死（JVM 沒了）**：in-process 無法重建。任何呼叫會拋
  `Py4JNetworkError` 而非 `IllegalStateException`。此時必須明說「需重跑」，不得無限重試。

### 4.5 Layer 1 instrumentation

因為只有應用層 log，這一輪必須埋三個點，且沿用 repo 既有的結構化 log 慣例
（`extra={"event": ...}`，見 `core/runner.py:60-73`）：

1. 每次成功建立 session 後，log `applicationId` 與 `appName`（`event=spark_session_created`）。
2. `release_spark_session` **真的停掉 session 時**，log 時間戳與被停掉的 `applicationId`
   （`event=spark_session_released`）。開關關閉時不發此事件。
3. `_fallback_create` 每次偵測到 dead session 時，log 上一個 `applicationId`、
   偵測時間、距離上次成功使用 Spark 的秒數（`event=spark_context_dead`）。
   需在 manager 內維護模組級的「上次成功回傳 alive session 的時間戳」。

有了這三個點，下次失敗時「idle-timeout 型」與「固定時長 token 過期型」在 log 上就分得開。

## 5. 已驗證的前提

停掉 session 不會讓下游節點拿到綁在死 context 上的 DataFrame：

- `DataCatalog.load()` 每次都真的呼叫 `dataset.load()`，沒有記憶體層快取
  （`core/catalog.py:65-67`）。
- `Runner` 逐節點載入 inputs（`core/runner.py:80-86`），不跨節點快取已 load 的資料。
- HPO 窗口內沒有任何 node 把 Spark DataFrame 存進 `MemoryDataset`：輸出全是
  `ParquetHandle`（本機 parquet）、`preprocessor_view`、lgb handle。
- `select_shap_population` 雖然在 predict 之後再次消費 `test_model_input`
  （`pipeline.py:178-181`），但那是一次全新的 `catalog.load()`，會用重建後的 session。
- 沒有任何長命的 Python 變數指向 entry 建的 session：五個 entry 的回傳值全部丟棄；
  唯一存成變數的是 `__main__.py:552`（`dataset()` 內的 mode-2 呼叫），函式結束即釋放。

HPO 窗口內確實不碰 Spark：`prepare_lgb_train_inputs`（讀本機 `ParquetHandle`）、
`persist_sample_weight_report`（`pyarrow.dataset` 讀本機路徑）、`tune_hyperparameters`
（driver-local optuna + LightGBM）、`finalize_model`、`calibrate_model` 皆為 driver-local。

`Runner` 為嚴格循序執行（`core/runner.py:65`），這是 §4.2 釋放點正確性的基礎。

## 6. 風險與取捨

- **重建 = 在 YARN 上重新提交一個新的 application。** 新 app id、新 executors，需要
  有效的 delegation token，佇列忙時要重新排隊。這不是無痛還原，log 必須明確標示。
  若公司環境對「一次排程產生兩個 app id」有稽核限制，需回頭調整。
- **若 Layer 1 死因與閒置無關**（例如固定時長的 token 過期），§4.2 的釋放不能防，
  但 §4.1 的重建修復仍會接住。
- **在 modeling 節點內操作全域 session 狀態**是本設計刻意接受的取捨，理由與替代方案的
  比較見 §4.2。
- **`tune_hyperparameters` 的既有測試**可能因為多了一個 side effect 而需要調整
  （例如需 monkeypatch `release_spark_session`）；HPO 接續邏輯
  （`pipelines/training/hpo_resume.py`）需確認不受影響。

## 7. 測試策略

單元測試（`tests/test_utils/test_spark.py`，秒級）：

1. 模擬 JVM 端停止（`session.sparkContext._jsc.sc().stop()`），斷言
   `get_or_create_spark_session()` 回傳**新** session，且 `createDataFrame` 可用。
   這是 §2.1 表格的可執行版本。
2. `stop_spark_session()` 對已死 session 冪等，不拋例外。
3. mode-1 呼叫後，mode-2 的重建使用記住的 canonical configs 與 `enable_hive`，
   不重讀 yaml。
4. 從未 configure 過時，mode-2 仍走 yaml fallback（保住 scripts / 測試的既有行為）。
5. `release_spark_session(parameters)` 在 `release_during_hpo: false` 時不停 session
   並回傳 `False`；`true` 時真的停掉並回傳 `True`。
6. 重建失敗（monkeypatch builder 使其拋例外）時包裝成 `SparkSessionUnavailableError`，
   訊息含上一個 `applicationId`；原始例外保留在 `__cause__`。

節點測試（`tests/test_pipelines/test_training/`）：

7. `tune_hyperparameters` 會呼叫 `release_spark_session`（monkeypatch 斷言呼叫一次，
   且發生在任何訓練工作之前）。

整合（本機 `local[*]`，分鐘級）：

8. 跑一次 training pipeline，確認 log 依序出現 `spark_session_created` →
   `spark_session_released` → 重建 → predict 寫入成功。

不需要碰 `tests/test_evaluation` 全量（約 33 分鐘）。改動前先建 baseline，
既有 failing/互擾測試清單見 `docs/operations/known-pitfalls.md §5`。

## 8. 驗收條件

- [ ] 上述 8 項測試全綠，貼最後 10 行輸出。
- [ ] 故意弄壞 `_clear_dead_session` 的清除動作一行，測試 1 轉紅；改回後轉綠
      （證明測試真的覆蓋新路徑）。
- [ ] 本機 `local[*]` 實跑 training pipeline，`training_eval_predictions` 有寫入，
      附產物路徑與 log 片段。
- [ ] `git diff --stat` 不含邊界外檔案。
- [ ] 改過 code 後跑 graphify rebuild。
- [ ] 未處理的事項（Layer 1 叢集端歸因）在交付訊息中明說。

## 9. 附錄：本機重現的核心動作

```python
s1 = get_or_create_spark_session(CFG)      # 正常 session
s1.sparkContext._jsc.sc().stop()           # 只停 JVM 內的 context(模擬 cluster 端殺 app)

_is_session_alive(s1)                      # False —— 偵測有效
s2 = get_or_create_spark_session(CFG)      # 「重建」
s2 is s1                                   # True  —— 重建沒有發生
s2.createDataFrame(pdf)                    # IllegalStateException: stopped SparkContext

s1.stop()                                  # Python 端 stop,清掉 _jsc / 單例
s3 = get_or_create_spark_session(CFG)      # 真的重建
s3 is s1                                   # False
s3.createDataFrame(pdf).count()            # 可用
```
