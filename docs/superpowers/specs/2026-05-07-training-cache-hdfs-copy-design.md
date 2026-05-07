# Training Pipeline：cache 改走 HDFS copy（解 YARN distributed file:// 問題）

- 日期：2026-05-07
- 影響範圍：`pipelines/training/nodes.py`、`utils/hdfs.py`（新檔）、`conf/base/catalog.yaml`、`conf/base/parameters_training.yaml`
- 狀態：Draft（待 user review）
- 接續：`2026-05-02-training-cache-model-input-design.md`（本 spec 修正其在 YARN 環境下的 IO 假設）

## 動機

`2026-05-02-training-cache-model-input-design.md` 的 cache 流程為：

1. cache node 判斷 hit / miss
2. miss → 回傳原 spark df
3. framework 呼叫 `ParquetDataset(backend=spark).save(spark_df)` → `df.write.parquet("file://...")` 寫到 driver-local 路徑

這個設計**隱含假設 driver 跟 executor 共用同一台 host fs**，亦即 `spark.master=local[*]`。

公司 prod 環境是 YARN cluster（driver 在 edge node、executor 在 NodeManager containers），`file://` 對每個 executor 而言是它自己 NodeManager 的本機 fs，不是 driver host 的 fs。Spark 寫 partitioned parquet 是分散式寫入，每個 task 嘗試 mkdir 到 NodeManager 容器看不到的 driver host 路徑，直接 Mkdirs error 失敗：

```
java.io.IOException: Mkdirs failed to create
  file:/dataset/workspaces/data/recsys_cache/.../
  _temporary/0/_temporary/attempt_xxx/snap_date=.../prod_name=fund
  (exists=false, cwd=file:/disk01/yarn/nm/.../container_e63_xxx)
```

且 cache hit 路徑也有同樣問題：`spark.read.parquet("file://...")` 是分散式讀，executor 在自己 NodeManager 上找不到該檔。

需求：讓 cache 在 YARN 上正確運作，不依賴 `spark.master=local[*]`。

### 約束（hard constraints）

1. **不能改 `spark.master`**：公司 prod 對 cluster 模式有運維約束
2. **演算法可插拔（algorithm-agnostic）**：`ModelAdapter.train(X: np.ndarray, y: np.ndarray, ...)` contract 不能動。cache 不能輸出 `lgb.Dataset` 之類的 algorithm-native 形式，否則綁死 LightGBM
3. **`ParquetDataset(backend=pandas)` 即將棄用**：新邏輯不能依附在這個 backend 上
4. **保留 cache 的 between-run 收益**：跨 pipeline run 仍要能 skip Hive scan

## 範圍

### 本次納入

- 新增 `src/recsys_tfb/utils/hdfs.py`：純 HDFS↔driver-local 的 mechanics helper
- 改寫 `src/recsys_tfb/pipelines/training/nodes.py::_cache_or_passthrough`：cache miss 改用 HDFS copy 方式拉資料；cache hit/miss 統一回傳 pandas DataFrame
- `conf/base/catalog.yaml`：刪除四個 `cached_*_model_input` 條目（持久化責任搬到 cache node）
- `conf/base/parameters_training.yaml`：更新註解（`cache.root` 變成 cache 路徑唯一 source of truth）

### 本次不納入

- pandas → Arrow 的 transient 形式優化（記憶體議題；先量資料、有需要再做）
- 從 `lgb.Dataset.save_binary` 衍生的 Layer 2 binary cache（綁定 LightGBM、違反 algorithm-agnostic）
- adapter contract 的 prepare-once / train-many 改造（解決 binning 重做議題；跟 cache layer 解耦）
- 把 `model` / `best_params` / `evaluation_results` 改 HDFS 持久化（YARN cluster mode 才需要；prod 是 client mode 就先不做）
- 抽出 generic `CachedDataset` wrapper 或 `HiveBackedParquetCacheDataset` 新 dataset 類型（YAGNI；目前只一個 caller）

## 設計

### 三層形式（不變的設計骨架）

| 層 | 形式 | 演算法綁定？ | 本次是否改 |
|---|---|---|---|
| 持久化（disk） | parquet on driver-local fs | ❌ | 不變（mechanism 改了，格式沒變） |
| transient（in-memory） | pandas DataFrame | ❌ | 不變（status quo） |
| 演算法 native（per-trial） | `lgb.Dataset` / `xgb.DMatrix` | ✅ | 不變（adapter 內部，cache 不碰） |

cache layer 只負責前兩層；第三層永遠是 adapter 的事。

### Ownership 換手：framework → cache node

舊：cache node 決策 hit/miss，**framework 寫磁碟**（透過 `ParquetDataset.save` 的 spark distributed write）。

新：cache node 同時做決策跟寫磁碟（透過 `utils/hdfs.copy_hdfs_to_local`）；framework 不再有任何磁碟 IO 角色，只在記憶體中介。

```
[Hive table on HDFS]                           ← dataset pipeline 寫的，介面不動
        │
        │ utils.hdfs.copy_hdfs_to_local
        ▼
[driver-local parquet + _SUCCESS marker]       ← cache node 寫
        │
        │ pd.read_parquet
        ▼
[pandas DataFrame, in-memory]                  ← 一次 Runner.run() 內活著
        │
        │ catalog.save → 自動 MemoryDataset
        ▼
[downstream node input]
        │
        │ _extract_Xy → numpy
        ▼
[adapter.train / .predict / ...]
```

刪 catalog entry 的理由：framework 不再做磁碟 IO，所以 catalog.yaml 不需要為 `cached_*` 登記「讀寫合約」。`catalog.py:70-74` 的 `save()` 對 unregistered name 會 auto-create `MemoryDataset`，正好對應「純 in-memory 中介」的角色。

### 模組職責

| 模組 | 職責邊界 |
|---|---|
| `utils/hdfs.py` | 純 HDFS↔driver-local 操作；不認識 Hive、不認識 cache 協定 |
| `_cache_or_passthrough` | 認識 cache 協定（`_SUCCESS` marker、partial 偵測、enable/disable） |
| `_populate_cache_from_hive` | 認識「cache 名稱 → Hive 表名 + outer partition key」對照 |
| `HiveTableDataset` | 不動 |
| `ParquetDataset` | 不動 |
| catalog | 對 `cached_*` 不再有任何登記，全部 auto-MemoryDataset |

未來若有第二、三個 cache 場景（layout 形狀類似），再考慮升級成 `HiveBackedParquetCacheDataset` 或通用 `CachedDataset` wrapper。

### 各模組改動

#### 1. 新檔：`src/recsys_tfb/utils/hdfs.py`

API：

```python
def get_hive_table_location(spark, database: str, table: str) -> str:
    """DESCRIBE FORMATTED 取 Hive 表的 HDFS Location URI。"""

def copy_hdfs_to_local(
    spark, src: str, dst: str, *, glob: bool = False
) -> None:
    """HDFS path / glob 複製到 driver-local。glob=True 把每個匹配項平鋪到 dst/。"""
```

實作走 Spark JVM bridge（`spark._jvm.org.apache.hadoop.fs.FileSystem.copyToLocalFile`），不依賴 host PATH 上有 `hadoop` CLI——避免 dev / prod 環境差異。

#### 2. 改：`src/recsys_tfb/pipelines/training/nodes.py`

新增兩張對照表：

```python
_CACHE_SOURCE_TABLE: dict[str, str] = {
    "val_model_input": "val_model_input",
    "test_model_input": "test_model_input",
    "train_model_input": "train_model_input",
    "train_dev_model_input": "train_dev_model_input",
    "calibration_model_input": "calibration_model_input",
}

_CACHE_OUTER_PARTITIONS: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    "test_model_input": ("base_dataset_version",),
    "train_model_input": ("base_dataset_version", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variant_id"),
    "calibration_model_input": ("base_dataset_version", "calibration_variant_id"),
}
```

新增 helper：

```python
def _populate_cache_from_hive(spark, dataset_name, parameters, local_dst):
    db = parameters["hive"]["db"]
    table = _CACHE_SOURCE_TABLE[dataset_name]
    location = get_hive_table_location(spark, db, table)
    outer = "/".join(
        f"{tok}={parameters[tok]}"
        for tok in _CACHE_OUTER_PARTITIONS[dataset_name]
    )
    src_glob = f"{location.rstrip('/')}/{outer}/snap_date=*"
    copy_hdfs_to_local(spark, src_glob, local_dst, glob=True)
```

改寫 `_cache_or_passthrough`：

| 原行為 | 新行為 |
|---|---|
| `cache.enabled=False` → 回 df 不變 | 不變 |
| 非 Spark df → 警告、回 df 不變 | 不變 |
| partial cache（路徑存在、無 `_SUCCESS`） → rmtree | 不變 |
| miss → 回原 spark df 等 framework 寫 | 改：呼叫 `_populate_cache_from_hive` + `_SUCCESS.touch()` + `pd.read_parquet` 回 pandas |
| hit → 回 `spark.read.parquet("file://...")` | 改：`pd.read_parquet(local_path)` 回 pandas |

#### 3. 改：`conf/base/catalog.yaml`

刪除四個條目：`cached_val_model_input`、`cached_train_model_input`、`cached_train_dev_model_input`、`cached_calibration_model_input`。

#### 4. 改：`conf/base/parameters_training.yaml`

更新 `cache:` 區段註解：去掉「路徑必須與 catalog.yaml 中 `cached_*_model_input.filepath` 一致」這段（catalog 已沒有對應 entry）。新增說明「`cache.root` 是 cache 路徑唯一 source of truth，由 `_resolve_cache_path` 跟 `_populate_cache_from_hive` 共用」。

### 資料流走查

#### 情境 A：cache miss（第一次跑 training）

```
Runner → cache_train_model_input(spark_df, params)
  └→ _cache_or_passthrough：
      └→ _SUCCESS 不存在 → cache miss
          └→ _populate_cache_from_hive：
              └→ get_hive_table_location → "hdfs://nn/.../train_model_input"
              └→ src_glob = "hdfs://.../base_dataset_version=abc/train_variant_id=def/snap_date=*"
              └→ copy_hdfs_to_local(glob=True)：
                  └→ fs.globStatus(src_glob) → [snap_date=A/, snap_date=B/, ...]
                  └→ for each: fs.copyToLocalFile（遞迴複製）
          └→ _SUCCESS.touch()
          └→ return pd.read_parquet(local_path, engine="pyarrow")
              └→ pyarrow 自動辨識 hive partitioning，snap_date / prod_name 還原成欄位

Runner → catalog.save("cached_train_model_input", pandas_df)
  └→ 不在 catalog config → auto-create MemoryDataset → 純記憶體賦值

Runner → tune_hyperparameters → catalog.load → MemoryDataset.load → 同一份 pandas_df
```

#### 情境 B：cache hit（第二次跑 training）

```
Runner → cache_train_model_input(spark_df, params)
  └→ _cache_or_passthrough：
      └→ _SUCCESS 存在 → cache hit
          └→ return pd.read_parquet(local_path)

★ Hive scan 沒觸發！spark_df 是 lazy plan，cache_or_passthrough 直接 short-circuit。
```

#### 情境 C：partial cache 偵測

```
local 路徑存在但 _SUCCESS 不存在（上次 run 被 kill）
  → shutil.rmtree(local_path) 清掉 → 走 cache miss 重新填
```

`_SUCCESS` 是「整份 cache 寫完」的承諾，缺了就丟掉重來。

#### 情境 D：dev cache disabled

```
cache.enabled=False → 第一行 return df 不變
  → MemoryDataset.save(spark_df)
  → 下游 _to_pandas() 觸發 spark_df.toPandas() → Hive scan
```

dev cluster 上跑用的兼容路徑。

## 測試

### 既有 tests

- 不動：`TestResolveCachePath`（7）、`TestIsSparkDataframe`（3）、`TestCacheOrPassthroughDev`（2）、`TestCacheNodes::test_cache_node_dev_passthrough`、`TestParametersWiringRegression::test_cache_node_raises_clear_error_when_versions_missing`
- 改寫（共 9 個）：所有 prod 路徑（hit / miss / partial）、`TestCacheNodes::test_cache_*_passes_dataset_name`（4 個 wrapper）、`TestCacheRunnerIntegration::test_second_run_uses_local_cache`、`TestParametersWiringRegression::test_cache_node_runs_when_versions_present`

### 新增 tests

#### `tests/test_utils/test_hdfs.py`（新檔）

`TestGetHiveTableLocation`：
- `test_parses_location_from_describe_formatted`
- `test_strips_whitespace_in_col_name_and_data_type`
- `test_raises_when_location_row_missing`

`TestCopyHdfsToLocal`：
- `test_non_glob_calls_copyToLocalFile_once`
- `test_glob_iterates_over_globStatus_results`
- `test_glob_raises_when_no_matches`

JVM bridge 統一用 MagicMock 模擬 `spark._jvm.org.apache.hadoop.fs.*` 跟 `spark._jsc.hadoopConfiguration()`。

#### `_populate_cache_from_hive` unit tests（加在 `test_cache_nodes.py`）

`TestPopulateCacheFromHive`：
- `test_train_model_input_constructs_correct_src_glob`
- `test_val_model_input_does_not_include_train_variant`
- `test_calibration_model_input_uses_calibration_variant`

驗證 `_CACHE_OUTER_PARTITIONS` 對照表跟 catalog `partition_filter` 對得上（容易 silently 出錯）。

### 測試分層原則

- `utils/hdfs.py` → 獨立 unit test，邊界 mock spark JVM bridge
- `_populate_cache_from_hive` → unit test 用 mock 接 `utils.hdfs`
- `_cache_or_passthrough` → mock `_populate_cache_from_hive`，專注於 cache 協定（_SUCCESS、partial、enable）
- Runner 整合 → 不 mock hdfs/spark，改用真實 driver-local fixture parquet 模擬「上一次 run 已寫好 cache」

整體：18 個測試動到（9 改寫、9 新增），純 mock-driven、不依賴 spark cluster。

## 風險 / Caveat

1. **Spark JVM bridge mock 複雜度**：`fs.globStatus` / `fs.copyToLocalFile` 的回傳結構需要正確仿造 Java 物件。實作時若 mock 寫得太脆弱，建議考慮用 fakefs（pyfakefs）+ fake hdfs uri 處理 path 部分，純驗證行為。
2. **`pyarrow` 還原 partition column 的 dtype**：HDFS 上 `snap_date` 是 STRING type partition，pyarrow `partitioning="hive"` 還原也是 string；若上游將來改成 date type，要校正。建議在 cache miss 第一次寫完後加一個 schema sanity check（`pd.read_parquet` 結果 vs `spark.table` 直接讀的 schema 是否一致），但這個 check 留在 follow-up，不在本次 patch。
3. **Driver disk 容量**：copyToLocal 整份複製。每個 variant 大小要先量、確認 driver 本機 disk 容得下 + pandas 形式塞得進記憶體。容量不足時 fallback 計畫：partial-load by snap_date（更高層的 streaming pattern；目前不做）。
4. **HADOOP_CONF_DIR 設定**：`fs.globStatus` 透過 `spark._jsc.hadoopConfiguration()` 取得 namenode URL；只要 Spark 提交得進 YARN，這份 config 已正確。dev cluster 跑時需確認 `~/dev-cluster/client-template/spark` 的 hadoop conf 對得到 namenode container。
5. **`_SUCCESS` marker 與並行寫**：本設計假設**單一 driver process 寫該 path**。多個 training pipeline 同時跑同一個 base_dataset_version × train_variant_id 會 race（rmtree partial、互寫 _SUCCESS）。Ploomber DAG 排程下不會有此情況；交互測試或 ad-hoc 開兩個 process 跑同一 variant 才會碰到。本次不處理（改不對外承諾並行安全）。

## 開放問題

1. 公司 prod 是否真的是 YARN client mode？（決定 `model.txt` / `*.json` 是否也要遷 HDFS）這個 question 不影響本次 cache 修法，但會影響後續是否要做 model artifact 的 HDFS 遷移。
2. `tune_hyperparameters` 在 Optuna 20 trials 內每次重新 `lgb.Dataset(X, y)`，binning 重做 20 次。修法在 adapter contract（prepare-once + train-many），不在 cache layer——是 follow-up 優化。
3. `_populate_cache_from_hive` 是否該包到 `utils/hdfs.py` 裡？目前定義為「認識 Hive 表名 + outer partition key」屬於業務知識、留在 `nodes.py`；若未來有第二個 caller 需要相同 hive→local 的 partition-aware 複製，再升級 `utils/hdfs` 提供更高層 API。
