# Spark 連線架構 —— 這個專案怎麼跟 Spark 講話

> 本文是 dev-cluster + recsys_tfb 環境下「程式 ↔ Spark」的單一來源圖（single source of
> truth）。設計來避免每次踩到 conf 不對 / 連不上 / 卡很久時，又要從頭重新摸一次。
> 配套：`~/dev-cluster/README.md`（環境設施）、`dev-cluster-spark` skill（已踩過的 SOP）、
> `CLAUDE.md`（執行最小規範）。

## 0. TL;DR

- 程式有 **6 種** 連 Spark 的入口；每種入口靠 **5 層配置** 不同程度地疊出來。
- 大部分「卡」「炸」是「**入口 ↔ 配置層** mismatch」—— 不是 Spark 壞了。
- 跑前先**對表** §6 cheat-sheet；跑時看 stdout 的 `[N/M]` 進度行，**不要被 stderr stack
  trace 嚇到** —— local mode 有一段 by-design 良性噪音（§5）。

---

## 1. 六種入口

| # | 入口 | 啟動方式 | Driver 在哪 | Executor 在哪 | 典型使用者 |
|---|---|---|---|---|---|
| **A** | Pipeline（standalone） | `python -m recsys_tfb <p> --env production` | host venv | dev-cluster `spark-worker` container | `dataset` / `inference` / `evaluation` / `*_etl` |
| **B** | Pipeline（local[*]） | 同上 + `export SPARK_CONF_DIR=…client-template-local/spark` | host venv | 同 driver JVM（local[*]） | `training` |
| **C** | container 內 Spark CLI | `docker exec devcluster-spark-master /opt/spark/bin/spark-sql --master local[1] -e "..."` | container | container | metastore 健檢、ad-hoc SQL、`SHOW TABLES IN ml_recsys` |
| **D** | 純 stdlib admin script | `scripts/dev_admin.sh scripts/<x>.py` | transient `devcluster/pyspark` container | 同 container（local[2]） | `setup_hive_dev.py`、`nuke_ml_recsys.py` |
| **E** | host-venv script（**會 `import recsys_tfb`**） | `python scripts/<x>.py`（host venv） | host venv | 同 driver JVM（local[*]） | `sampling_overrides_editor.py`、`suggest_categorical_cols.py` |
| **F** | Tests | `pytest tests/...` | pytest 行程 | 同 driver JVM（local[1]） | conftest 的 `spark` fixture，所有 `@pytest.mark.spark` 測試 |

關鍵差異：

- **A vs B**：A 走 standalone master → 真的有 worker container 在 docker 跑 executor；
  B 是 local[*]、worker container 不用，省掉 standalone scheduler init 的 3–5 min。
- **D vs E**：D 跑在 **裸 pyspark container**（無 venv、無 `recsys_tfb` / `typer` / `yaml`）
  → 只能跑 stdlib-only 腳本；E 用 host venv → 可以 `import recsys_tfb`，但完全靠
  host 的 dev-cluster env。

---

## 2. 五層配置

Spark 啟動時把以下五層疊加起來決定怎麼跑：

| 層 | 來源 | 典型內容 | 誰用 |
|---|---|---|---|
| 🅐 **Python config** | `parameters.yaml` `spark:` 區塊 → `get_or_create_spark_session(dict)` → `builder.config(k,v)` | `app_name` / `spark.master` / `driver.host` / `driver.port` / `extraJavaOptions` / `warehouse.dir` / … | Pipeline 入口（A/B）會塞**整包**；script 入口（E）通常**只塞 `app_name`** |
| 🅑 `SPARK_CONF_DIR/spark-defaults.conf` | dev-cluster client-template（`client-template/` 或 `client-template-local/`） | `spark.master` / `warehouse.dir` / `eventLog.dir` | 入口 A/B/E（host 端跑的都吃這個） |
| 🅒 `SPARK_CONF_DIR/hive-site.xml` | dev-cluster | metastore Thrift URI | 入口 A/B/E（任何要連 Hive metastore 的） |
| 🅓 `HADOOP_CONF_DIR/{core,hdfs}-site.xml` | dev-cluster `client-template/hadoop` | `fs.defaultFS=hdfs://...` / namenode 連線參數 | 入口 A/B/E（任何要連 HDFS 的） |
| 🅔 **JVM env vars** | `source ~/dev-cluster/scripts/client-env.sh` 設 | `SPARK_DRIVER_EXTRAJAVAOPTIONS`（**JDK17 add-opens**）/ `PYSPARK_SUBMIT_ARGS` / `JAVA_HOME` / `SPARK_LOCAL_IP` / `HADOOP_CONF_DIR` 本身 | 入口 A/B/E |

### 入口 vs 拼圖對照

| 入口 | 🅐 Python 端傳什麼 | 🅑 / 🅒 / 🅓 / 🅔 env 端要做什麼 |
|---|---|---|
| **A** Pipeline | `parameters.yaml` 整包 spark: 塞 builder | `source client-env.sh` |
| **B** Training | 同 A | `source client-env.sh` + `export SPARK_CONF_DIR=…client-template-local/spark` |
| **C** container CLI | 不適用 | container 自帶 /opt/spark/conf，不靠 host env |
| **D** dev_admin.sh | 不適用（容器裡跑 spark-submit） | dev_admin.sh 自己 bind-mount dev-cluster spark/hadoop conf 進 container |
| **E** host-venv script | **只 `app_name`** | **完全靠 🅑🅒🅓🅔**；任一層缺都炸 |
| **F** Tests | conftest spark fixture 自己 build（`local[1]`） | **不需要**（in-process Spark；不連 dev-cluster） |

> **入口 E 是最脆弱的**：因為 🅐 只給 app_name，所有連線設定（master、metastore、HDFS、
> JDK17 add-opens）都得從 🅑🅒🅓🅔 拿。任何一塊忘了帶，行為都很差 —— 不是 fail-fast、
> 是看似在跑然後 timeout 或拿到空 catalog。

---

## 3. 慢/卡的五大來源

不是「Spark 慢」是「組合起來慢」：

| 種類 | 來自哪 | 量級 | 避法 |
|---|---|---|---|
| ① **Standalone scheduler init** | `client-template/spark` 走 `spark://localhost:7077` + dynamicAllocation | **3–5 分鐘** | 單行程 / tiny collect 改走 `client-template-local`（local[*]） |
| ② **JVM + Hive metastore connect** | 第一次建 SparkSession | 30–90 秒 | 不可避免；把多次 ad-hoc 合一次跑 |
| ③ **Metastore JVM 卡住** | Apple Silicon 跑 amd64 emulation 偶發 stuck | 60s+ sit | `docker restart devcluster-hive-metastore`（dev-cluster-spark skill SOP-1） |
| ④ **多重 Spark timeout 串接** | 預設 `spark.rpc.askTimeout=120s`、`spark.network.timeout=120s`、`spark.driver.maxResultSize=...` | 15s × N segment | 設對 conf 而不是讓它 timeout 串連 |
| ⑤ **測試 × Spark action** | `tests/test_evaluation` 每測試一次 Spark action、conftest session 重用不徹底 | 全集合 **~33 分鐘** | conftest 改 `local[*]` / session 真正重用（CLAUDE.md 已列方向，待量測） |

入口 E 跑 `sampling_overrides_editor.py profile` 的時間預算：

- 走 `client-template/spark`（standalone）：**①+② ≈ 7–10 分鐘**
- 走 `client-template-local`（local[*]）：**只剩 ② ≈ 3–4 分鐘**

---

## 4. 多個來源同時設 `spark.master` 會怎樣

dev-cluster-spark skill SOP-3 有完整變體說明，這裡只摘衝突結果：

| app conf (🅐) 寫的 master | spark-defaults.conf (🅑) 寫的 master | 結果 |
|---|---|---|
| 沒寫 | `spark://localhost:7077` | A 入口正常 |
| 沒寫 | `local[*]` | B/E 入口正常 |
| `yarn` | `spark://...`（dev-cluster） | driver 卡在 YARN ResourceManager discovery（dev-cluster 沒跑 YARN） |
| `local[*]` | `spark://...` + 帶 `driver.host/port` | LocalSchedulerBackend 不註冊 `CoarseGrainedScheduler`，但 client-template 的 driver.host/port 仍生效 → `RpcEndpointNotFoundException` |

**實作守則**：連線參數的 source 統一交給 `SPARK_CONF_DIR`，app conf（parameters.yaml）
不寫 `spark.master` / `driver.host` / `driver.port` / HDFS / Hive metastore / eventLog；
切環境只切 `SPARK_CONF_DIR`。

---

## 5. 良性噪音 vs 真實失敗

local mode（入口 B / E / F）會看到的「假錯誤」trace，**不要被嚇到**：

```
ERROR Inbox: Ignoring error
org.apache.spark.SparkException: Exception thrown in awaitResult
        at org.apache.spark.util.ThreadUtils$.awaitResult(...)
        at org.apache.spark.storage.BlockManagerMasterEndpoint.driverEndpoint$lzycompute(...)
        ...
Caused by: org.apache.spark.rpc.RpcEndpointNotFoundException:
  Cannot find endpoint: spark://CoarseGrainedScheduler@<auto-detected-IP>:<port>
```

**為什麼是良性**：Spark `BlockManagerMasterEndpoint.isExecutorAlive` 不分 master 模式，
永遠用 `RpcUtils.makeDriverRef("CoarseGrainedScheduler", conf, rpcEnv)` 查 cluster-mode
的 endpoint。local mode 下 `LocalSchedulerBackend` 用的是 `LocalEndpoint`、**不**註冊
`CoarseGrainedScheduler`，所以這條查詢必失敗 —— 但 Spark 在 `Inbox.safelyCall` 用 try/catch
吞掉，只 log 一行 `ERROR Inbox: Ignoring error`，主流程繼續。

**判別真實狀態**：

- ✅ stdout 印出腳本自己的成功訊號（如 `[N/M]` 進度行、`Wrote ...`、`Time taken:`）→ **成功**
- ❌ stdout 完全沒有腳本應有的最後一行 + Python `Traceback` 在 stderr 末尾 → 真失敗
- ⚠️ stderr 有大段 stack trace 但 stdout 一路看到 `Wrote ...` → **noise，不是失敗**

**Pipe 進度時不要過早截斷**（自打嘴巴的雷區）：

```bash
# ✗ head -15 會被大段 RPC stack 填滿，看不到後面的進度
python scripts/... 2>&1 | grep -E '...' | head -15

# ✓ 不限筆數，或直接看完整檔案 / 用 tail
python scripts/... 2>&1 | tee /tmp/run.log
grep -E '^\[[0-9]/[0-9]\]|Wrote |^Traceback' /tmp/run.log
```

---

## 6. cheat-sheet：跑前對表

```
要跑的東西                              用哪個入口   執行樣板
====================================================================
Pipeline (dataset/inference/eval/...)     A         source ~/dev-cluster/scripts/client-env.sh
                                                    python -m recsys_tfb <pipeline> --env production

Training pipeline                          B         source ~/dev-cluster/scripts/client-env.sh
                                                    export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
                                                    python -m recsys_tfb training --env production

Metastore 健檢 / ad-hoc SQL                C         docker exec devcluster-spark-master \
                                                      /opt/spark/bin/spark-sql --master local[1] -e "..."

setup_hive_dev / nuke / 純 stdlib 腳本     D         scripts/dev_admin.sh scripts/<x>.py

Import recsys_tfb 的 admin 腳本             E         source ~/dev-cluster/scripts/client-env.sh
(e.g. sampling_overrides_editor.py)                 export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
                                                    PYTHONPATH=<root>/src \
                                                      /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
                                                      scripts/<x>.py <args>

Tests                                      F         PYTHONPATH=<root>/src \
                                                      /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
                                                      -m pytest <paths> -q
```

### 跑前 pre-flight（兩條都過再跑）

```bash
readlink /Users/curtislu/projects/recsys_tfb/.venv 2>/dev/null \
  || echo "(main repo real venv — ok)"
# worktree 必須 → /Users/curtislu/projects/recsys_tfb/.venv

/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # → Python 3.10.9
```

入口 A/B/E 還要：

```bash
echo "SPARK_CONF_DIR=$SPARK_CONF_DIR"        # 必須有值
echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"      # 必須有值
docker ps --format '{{.Names}}' | grep -c devcluster   # 至少 6
grep -E 'namenode|hive-metastore' /etc/hosts | head -1 # 應有 devcluster 行
```

任一條不滿足 → 補完再跑。

---

## 7. 已知坑（指回 SOP）

- **SOP-1**：metastore JVM stuck on Apple Silicon。判別：第一次 `CREATE DATABASE` 5–15s 正常；單一 metastore RPC > 60s 才算病態。
- **SOP-2**：pandas≥1.5 ns timestamp parquet → Spark 3.3.2 拒收。修寫入端。
- **SOP-3 A/B**：app conf vs spark-defaults.conf 的 `spark.master` 衝突。
- **SOP-4**：`Incomplete HDFS URI` —— dev-cluster Hive `LOCATION` 必用 FQ URI（`hdfs://namenode:9000/...`）。
- **SOP-5**：要寫 host fs → 用 `client-template-local`（driver=executor 同 JVM）。
- **SOP-6**：純 stdlib 腳本 → `scripts/dev_admin.sh`（transient pyspark container + local[N]）。
- **本文 §5 良性噪音**：local mode 一定看到 `CoarseGrainedScheduler` RPC lookup 失敗 + `Inbox: Ignoring error`，是 by-design noise，不影響執行。

---

## 8. 還沒納入但已知會踩的（TODO）

- **入口 E 的 fail-fast**：`get_or_create_spark_session` 收到只有 `app_name` 的 dict 時，
  能否偵測 `SPARK_CONF_DIR=UNSET` / `HADOOP_CONF_DIR=UNSET` / JDK17+ 缺 `--add-opens`
  → 直接拋帶 actionable hint 的錯誤，而不是讓 Spark 跑到一半 timeout。
- **`sampling_overrides_editor` 的 usage 範例（見 `docs/change-guide.md` 情境 4、README §2）**：目前是
  bare `python scripts/...`，跟入口 E 的實際需求不符。對齊 §6 cheat-sheet 的 E 樣板。
- **`tests/test_evaluation` ~33min**：CLAUDE.md 已列加速方向，待實測。

---

*Last reviewed: 2026-05-20（本文跟著 dev-cluster-spark skill、CLAUDE.md 一起維護；若三者
不一致以本文跟 skill SOP 為準，CLAUDE.md 提交版若舊應同步更新。）*
