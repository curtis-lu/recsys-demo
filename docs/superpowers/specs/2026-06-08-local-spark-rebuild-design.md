# 本機 Spark 測試環境重建設計（方案 A：純 host `local[*]` + 內嵌 Derby metastore）

- **日期**：2026-06-08
- **狀態**：設計已與使用者確認，待 spec 審閱後轉 writing-plans
- **Branch / worktree**：`feat/local-spark-rebuild` @ `.worktrees/local-spark-rebuild`（從 main `0df0f9c` 分支）
- **範圍**：repo 內的 Spark/Hive 連線相關設定、scripts、docs、CLAUDE.md，以及 `~/.claude/skills/dev-cluster-spark` skill。**不含主專案程式（`src/recsys_tfb/`）**。

---

## 1. 背景與問題

本機測試 pipeline 一直透過 `~/dev-cluster/`——一整套 **分散式** Docker stack：HDFS（namenode + datanode）、Hive Metastore（apache/hive on postgres）、Spark Standalone（master + worker）、spark-history。這些 image 是 x86 的，在 Apple Silicon 上**走 qemu 模擬**執行。

使用者回報的三個痛點，根因都是這個模擬層：

1. **每次用 Spark 都失敗、搞很久**（已數十次）——metastore RPC stuck（skill SOP-1）、datanode HDFS 讀路徑在 host-driver `copyToLocalFile` 下崩潰（`Py4JNetworkError`）、namenode restart 後 394% CPU 卡死。
2. **MacBook 發燙。**
3. **耗電量非常高。**

模擬層對「本機測試程式」**沒有任何必要**：程式需要的只是「能解析 `ml_recsys.*` Hive 表、能讀寫 parquet 的 Spark」，不需要真分散式、不需要真 HDFS。使用者明確表示：**公司環境的 Spark 沒有連線問題，本機這套只為了本機測試**，既有設定可整個廢棄，以本專案程式的本機測試需求為主重建。

## 2. 目標與非目標

**目標**

- 移除 qemu 模擬層（即移除三個痛點的根因）。
- 從程式回推、用最小面覆蓋「本機能跑 ⇒ 公司能跑」所需的**所有正確性 code path**。
- 涵蓋所有 pipeline（dataset / training / inference / evaluation / `*_etl`）與所有會連 Spark 的 scripts（`suggest_categorical_cols`、`sampling_overrides_editor`）。
- 讓**任何使用本專案的人**都能在本機無痛建置並執行測試（文件、skill、CLAUDE.md 一併更新）。

**非目標（使用者明確排除）**

- 生產保真度（HDFS scheme 行為、thrift metastore 特有語意、分散式 shuffle）——公司環境負責，本機不測。
- 規模 / 記憶體 / OOM / `shuffle.partitions` 調參——本機資料小，非本機測試目的。
- 改動主專案程式 `src/recsys_tfb/`。
- 物理拆除 `~/dev-cluster/`（在 repo 外，使用者自行處理；本設計只動 repo 內引用 + skill）。

## 3. 從程式回推：本機測試實際需要的最小面

掃過所有會碰 Spark/Hive 的進入點後，本機只需三件事：

1. `spark.sql.catalogImplementation=hive` + 一個 metastore + 一個 warehouse 目錄 → 讓 `HiveTableDataset` 的 `ml_recsys.*` 能 `CREATE`/`INSERT`/`SHOW PARTITIONS`/`DESCRIBE FORMATTED`。
2. 本機檔案系統能讀寫 parquet（`catalog.yaml` 的檔案類 dataset 用相對 `data/...`）。
3. training cache 能把表資料拉到 driver-local fs（`copy_hdfs_to_local`）。

關鍵盤點結果（決定方案可行性）：

- **`conf/base/catalog.yaml` 的 17 個 `HiveTableDataset` 全是 `external: false`（managed）、零 `location:`、零 `hdfs://`。** managed 表一律落在 `spark.sql.warehouse.dir`，scheme 100% 由 `SPARK_CONF_DIR` 注入，程式裡沒有寫死 HDFS。
- **`get_or_create_spark_session`（`src/recsys_tfb/utils/spark.py`）只吃 `app_name` + tuning**，連線設定（master / warehouse / metastore）全由 `SPARK_CONF_DIR` 提供。
- **`pyspark` pip 套件自帶 `derby-10.14.2.0.jar` + `hive-metastore-2.3.9.jar` + `hive-exec`**：內嵌 Derby metastore 零安裝可用（符合「不裝額外套件」限制）。
- **`scripts/generate_synthetic_data.py` 已存在**（純 pandas、不需 Spark），產 `feature_table` / `label_table` / `sample_pool.parquet`。
- `parameters.yaml` 的 `spark:` 區塊本機只有 `app_name`，其餘 CDP 連線設定（`${vdclient.cdp.*}`）全是註解掉的範本，本機忽略。

**推論：重建本機 Spark 不需要動 `src/`，`conf/base/` 只需一處微調（`cache.root` 絕對→相對，為 worktree 隔離 + 新人可用，見 §5.6），其餘只要新增一份 `SPARK_CONF_DIR` 並改寫 setup/teardown 與文件。**

## 4. 架構（方案 A）

host venv 原生 arm64 JVM 跑 `local[*]`，driver = executor 同一 JVM。

- 儲存 = 本機 fs warehouse（`data/local_warehouse`）。
- metastore = 內嵌 Derby（`data/metastore_db`，首次自動建 schema）。
- **無 Docker / HDFS / qemu / thrift / port-forward。**
- **一份 `SPARK_CONF_DIR` 管所有 pipeline**——取代現在「dataset/inference/evaluation/etl 走 standalone `spark://`、training 走 `local[*]`」的雙路由。

## 5. 元件設計

### 5.1 `conf/spark-local/spark-defaults.conf`（新增，進版控）

```properties
spark.master                       local[*]
spark.submit.deployMode            client

spark.sql.catalogImplementation    hive
spark.sql.warehouse.dir            data/local_warehouse
spark.hadoop.javax.jdo.option.ConnectionURL  jdbc:derby:;databaseName=data/metastore_db;create=true

spark.sql.session.timeZone         Asia/Taipei
spark.serializer                   org.apache.spark.serializer.KryoSerializer
spark.driver.memory                4g

# 讓 host JDK17 直接能跑（driver=executor 同 JVM）；JDK11 下無害
spark.driver.extraJavaOptions      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED
```

設計重點：

- **路徑用相對 repo root**：`data/local_warehouse`、`data/metastore_db` 相對 CWD（= repo/worktree root）解析。專案 SOP 本就要求從 root 跑 CLI，故新人 clone 即可用，無 `/Users/...` 硬路徑。
- **故意不寫** `spark.driver.host` / `spark.driver.port` / `spark.blockManager.port`——避開 skill SOP-3-B 的 `RpcEndpointNotFoundException` 陷阱。
- **`eventLog` 預設關**（不寫 `spark.eventLog.enabled`）——本機不需要 history server。
- **每個 worktree 各自一份 warehouse / metastore_db**（相對路徑 + worktree 各自 CWD），天然隔離，互不污染（完整隔離模型見 §5.6）。
- **不需要 `hive-site.xml`**：內嵌 Derby + warehouse 全用上面的 `spark.hadoop.javax.jdo.*` 與 `spark.sql.warehouse.dir` 設定即可。

### 5.2 `scripts/local_spark_setup.py`（新增，host venv 直接跑）

取代 `setup_hive_dev.py` + `nuke_ml_recsys.py` + `dev_admin.sh` 三者。

- 確保 `generate_synthetic_data.py` 已產 3 個 parquet（或直接呼叫它）。
- 讀 `data/{feature_table,label_table,sample_pool}.parquet`，**`snap_date` cast 成 DATE**（保留現有關鍵修正：合成 parquet 是 timestamp[us]，不轉的話對 `'YYYY-MM-DD'` 字串 filter 會 0 row）。
- `saveAsTable` 寫進 `ml_recsys.*` managed 表（落本機 warehouse）。
- `--reset` flag：`rm -rf data/local_warehouse data/metastore_db` 後重建（瞬間，取代 nuke）。
- **在 host venv 跑**：不經 transient container、不經 `dev_admin.sh`。

### 5.3 統一執行流程

所有 pipeline 與 scripts 同一條路：

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py        # 首次 / --reset
PYTHONPATH=src .venv/bin/python -m recsys_tfb <pipeline> --env local
```

- `--env local`（CLI 預設值），**不再有「`--env production` 指本機」**。
- `suggest_categorical_cols` / `sampling_overrides_editor` 同樣 `export SPARK_CONF_DIR` 後 host venv 直跑——省掉現在 `client-env.sh` + JDK add-opens + `SPARK_CONF_DIR` 覆寫順序那整套儀式。

### 5.4 `scripts/local_e2e.sh`（重寫，取代 `dev_e2e_two_stage.sh`）

精簡的本機端到端 smoke：`local_spark_setup.py --reset` → dataset → training → inference → evaluation，全 `local[*]`。作為「我的 code 能否上公司」的最終確認。沿用現有 e2e 的即時日誌 / summary 風格（符合 subagent 同步審核慣例），但移除所有 docker compose ps / datanode 等待 / Hive nuke via container 的步驟。

### 5.5 退役清單（repo 內）

| 檔案 | 處置 |
|---|---|
| `scripts/dev_admin.sh` | 刪除（不再有 transient container） |
| `scripts/setup_hive_dev.py` | 由 `local_spark_setup.py` 取代 |
| `scripts/nuke_ml_recsys.py` | 由 `local_spark_setup.py --reset` 取代 |
| `scripts/dev_e2e_two_stage.sh` | 由 `local_e2e.sh` 取代 |

### 5.6 Worktree 隔離保證

**原則**：一個 worktree = 完全自足的沙盒；所有本機狀態相對 worktree root（CWD）解析，**無任何東西指向 main**。重建後 setup 快（無 qemu），故「每 worktree 各一份」既正確又負擔得起——舊設計 symlink 到 main 只為省 2–4 分鐘 cold start，該理由已消失。

逐項本機狀態的隔離機制：

| 本機狀態 | 隔離機制 |
|---|---|
| Hive warehouse | `data/local_warehouse` 相對 → worktree |
| Derby metastore | `data/metastore_db` 相對 → worktree |
| 檔案 artifact（models/dataset/eval/inference） | catalog `filepath: data/...` 相對 → worktree |
| mlflow 追蹤 | `${env.MLFLOW_TRACKING_URI\|mlruns}` 相對 → worktree |
| training cache | `cache.root` **絕對→main** 改為相對 `data/recsys_cache` → worktree |
| 合成資料 | `local_spark_setup.py` 每 worktree 重新產 |

**兩項配套改動**：

1. **`cache.root` 絕對→相對**（`conf/base/parameters_training.yaml:161`，`/Users/curtislu/...` → `data/recsys_cache`）。這是唯一要碰 `conf/base` 之處；理由站得住：本機 cache 路徑、純 config 非 src、移除 `/Users/curtislu` 硬路徑正是「新人能用」前提、inference 尚未部署無相容包袱。
2. **廢除「symlink `data/` 子目錄到 main」SOP**：那套正是隔離破口（worktree 寫的 model/dataset 落進 main，CLAUDE.md R3 footgun）。改成**每 worktree 各自一份真 `data/` 樹，由 `local_spark_setup.py` 本機重建**。

**驗證（pre-flight 閘，非靠記得做 SOP）**：`local_spark_setup.py --check-isolation` 在跑 Spark 前 fast assert，任一條不過即 fail-fast：

- CWD == 此 worktree 的 root
- `SPARK_CONF_DIR` == `<此 worktree>/conf/spark-local`
- `data/{local_warehouse,metastore_db,recsys_cache}` 為 worktree 底下**真目錄**（非 symlink-to-main、非絕對-to-main）
- `cache.root` 解析後落在 worktree 內

預設隔離；若**刻意**要用 main 的真 artifact 測，針對該子目錄手動 symlink（opt-in）。

## 6. 文件 / skill / CLAUDE.md 改寫

- **新增 `docs/operations/local-spark-setup.md`**：新人無痛建置的單一權威指南（建 venv → `export SPARK_CONF_DIR` → `local_spark_setup.py` → 跑 pipeline；含 Derby 單行程鎖、「從 root 跑」、cache smoke 注意事項）。
- **CLAUDE.md**：(1) 刪「Local dev-cluster testing」+「Pipeline 與 SPARK_CONF_DIR 的對應」兩節，換成一節薄的「本機 Spark 測試」指向新 doc；(2) 改寫「Worktree / venv」段的 R3 footgun——廢除「symlink `data/` 子目錄到 main」改為每 worktree 真 `data/` 樹 + `--check-isolation`（見 §5.6）。
- **`~/.claude/skills/dev-cluster-spark` → 重寫為 `local-spark`**：
  - 刪 SOP-1（metastore stuck）、SOP-4（HDFS URI）、SOP-5（file:// 需 local[*]）、SOP-6（admin container pattern）。
  - 保留並改寫 SOP-2（pandas ns timestamp parquet：Spark 3.3.2 同版本，本機仍會遇到）、SOP-3-C（`local[*]` 下 `CoarseGrainedScheduler` `RpcEndpointNotFoundException` 良性噪音，本機仍會出現）。
  - 新增：Derby 單行程鎖（同時跑 pipeline + script 碰同一 `metastore_db` 會撞鎖，循序使用即可）、「從 repo root 跑」、training cache 本機 fs→fs 複製的 smoke 確認。
- **`docs/operations/spark-connection-architecture.md`**：大幅瘦身為兩條——「本機 = `conf/spark-local`」、「公司 = 換 `SPARK_CONF_DIR`（`parameters.yaml` 註解的 CDP 範本即模板）」。原 A–F 入口分類 / 5 層配置表收斂。
- **`docs/operations/worktree-venv-setup.md`**：改寫 worktree data 隔離模型（見 §5.6）——廢除「symlink `data/` 子目錄到 main」改為每 worktree 真 `data/` 樹；補 `local_warehouse` / `metastore_db` / `recsys_cache` 為 worktree 本地產物（各自一份、`--reset` 各自清）；補 `--check-isolation` pre-flight 閘。

## 7. 移轉保真度分析（為什麼「本機能跑 ⇒ 公司能跑」成立）

### 7.1 A 忠實覆蓋的 code path（與公司同一條）

| 進入點 | 碰 Spark 的方式 | 為何忠實 |
|---|---|---|
| `dataset` | Spark SQL 抽樣/前處理 + `HiveTableDataset` 讀寫 | DataFrame/SQL 邏輯計畫相同，`local[*]` 只改物理執行不改正確性 |
| `training` | cache `copyToLocal` + LightGBM（driver 單機）+ 寫 eval 表 | cache 變本機 fs→fs；LightGBM 本就 driver 單機 |
| `inference` | Spark 打分 + 寫 Hive managed 表 | `insertInto` + dynamic partition overwrite，本機 parquet 完全支援 |
| `evaluation` | 讀 Hive + Spark metrics + 寫報表/表 | 同上 |
| `*_etl` / `source_etl` | `CREATE DATABASE`（無 LOCATION）+ `spark.sql` 跑 SQL 檔 | 無 LOCATION 建庫在本機 warehouse 直接成立 |
| `suggest_categorical_cols` | `get_or_create_spark_session()` → `spark.table()`/`read.parquet()` | embedded metastore 解析 `ml_recsys.*` |
| `sampling_overrides_editor` | `get_or_create_spark_session({app_name})` → `spark.table()` | 同上 |

`HiveTableDataset` 的 `CREATE ... STORED AS PARQUET` + `insertInto` + `partitionOverwriteMode=dynamic` + `DESCRIBE FORMATTED` 取 Location——全是標準 Hive DDL/DML，pyspark 內建 hive 支援 + `catalogImplementation=hive` 就能跑，**和公司是同一條 code path、同一個 Spark 3.3.2 引擎**。

### 7.2 A 不覆蓋的三個 gap，以及為何不影響移轉

1. **HDFS scheme（file:// vs hdfs://）**：程式對 scheme 設計上中立（managed 表 + `FileSystem.get(conf)`/`copyToLocalFile` 兩種 scheme 都吃）。沒有「只在 HDFS 跑到」的 code，本機不會漏測。殘留風險：未來新寫 code 若手動寫死 `hdfs://`，本機抓不到——靠「scheme 一律走 SPARK_CONF_DIR」既有契約 + code review 擋。
2. **分散式 shuffle / 多 executor**：Spark DataFrame API 正確性與執行模式無關（logical/optimized plan 相同，只差 task 排程）。會差的是規模/記憶體/調參——使用者明示非本機測試目標。
3. **thrift metastore vs 內嵌 Derby**：兩者對外同一套 Hive catalog API、DDL/DML 語意相同。Derby 唯一差別是單行程獨佔；程式用到的操作（CREATE DATABASE/TABLE/INSERT/SELECT/DESCRIBE）Derby 全支援。文件註明循序使用即可。

**結論**：沒有任何「本機測得過、公司卻會掛」的正確性破口被 A 漏掉。

## 8. 唯一要 smoke-test 的點

training cache 的 `copy_hdfs_to_local`（`src/recsys_tfb/utils/hdfs.py`，屬主專案程式、不改）在 A 下變 `file://` 本機複製：`get_hive_table_location` 回傳 `file:<warehouse>/...`，`FileSystem.get(hadoop_conf)`（預設 fs = `file://`，因為本機不設 `fs.defaultFS=hdfs`）+ `copyToLocalFile` 是同 fs 本機複製。理論上必過，但這是唯一 infra-coupled 路徑——實作收尾以一次 `training` pipeline 實跑確認，不靠假設。

## 9. 測試策略

- **pytest 套件不變**（本就 `local[1]`、in-memory DataFrame、無 Hive），重建後驗證仍綠。
- **移轉 smoke**：`scripts/local_e2e.sh` 跑 dataset→training→inference→evaluation 全 local。
- training cache 的本機 fs→fs 複製在 e2e 中一併驗證（§8）。

## 10. 已定決策

- **`~/dev-cluster/` Docker 退場**：本設計只動 repo 內引用 + skill；`~/dev-cluster/` 由使用者自行處理（不在本 spec 動作範圍）。
- **E2E**：重寫精簡 `scripts/local_e2e.sh`（非保留舊 `dev_e2e_two_stage.sh`）。

## 11. 實作檔案異動總表（給實作計畫）

**新增**

- `conf/spark-local/spark-defaults.conf`
- `scripts/local_spark_setup.py`
- `scripts/local_e2e.sh`
- `docs/operations/local-spark-setup.md`

**改寫**

- `CLAUDE.md`（Spark/Hive/dev-cluster 相關節 + Worktree/venv R3 隔離段）
- `docs/operations/spark-connection-architecture.md`
- `docs/operations/worktree-venv-setup.md`
- `~/.claude/skills/dev-cluster-spark/SKILL.md` → `local-spark`
- `conf/base/parameters_training.yaml`（**唯一 conf/base 改動**：`cache.root` 絕對→相對 `data/recsys_cache`，見 §5.6）

**刪除**

- `scripts/dev_admin.sh`
- `scripts/setup_hive_dev.py`
- `scripts/nuke_ml_recsys.py`
- `scripts/dev_e2e_two_stage.sh`

**不動（明確排除）**

- `src/recsys_tfb/`（含 `utils/hdfs.py`、`utils/spark.py`、`io/hive_table_dataset.py`、`pipelines/training/nodes.py`）
- `conf/base/`（catalog / parameters；**除 `cache.root` 相對化外不動**，本機路徑全由 `SPARK_CONF_DIR` 注入）
- `~/dev-cluster/`（使用者自行處理）
