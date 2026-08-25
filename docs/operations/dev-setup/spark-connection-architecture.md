# Spark 連線架構

連線設定（master / warehouse / metastore）**唯一來源是 `SPARK_CONF_DIR`**，程式（`get_or_create_spark_session`）只傳 `app_name` + tuning。切環境＝換 `SPARK_CONF_DIR`，不動 `src/` 不動 `conf/base`。

## 本機測試
`SPARK_CONF_DIR=conf/spark-local` → `local[*]` + 內嵌 Derby + 本機 `data/local_warehouse`。
建置與執行見 [`local-spark-setup.md`](local-spark-setup.md)。所有 pipeline 與 scripts 同一條路（無雙路由）。

## 公司環境
換成公司提供的 `SPARK_CONF_DIR`（YARN/distributed + thrift metastore + HDFS）。
`conf/base/parameters.yaml` 的 `spark:` 區塊裡註解掉的 `${vdclient.cdp.*}` profile 即連線範本；
`resolve_vdclient_placeholders` 在對應 cluster 上解析。本機這些 placeholder 自動忽略。

## 為什麼這樣分層

app conf（`parameters.yaml`）不寫 `spark.master` / `driver.host` / `driver.port` / HDFS /
Hive metastore / eventLog；所有連線參數統一交給 `SPARK_CONF_DIR/spark-defaults.conf`。
這樣切環境只換一個 env var，不動 `src/` 不動 `conf/base`。

背後原理：若 app conf 寫 `spark.master=local[*]` 而 `spark-defaults.conf` 同時設了
`driver.host` / `driver.port`（cluster-mode 用），`LocalSchedulerBackend` 就不會
註冊 `CoarseGrainedScheduler`，但 cluster-mode 的 RPC endpoint 設定仍生效 →
`RpcEndpointNotFoundException`。統一讓 `SPARK_CONF_DIR` 當連線唯一來源即可避開。
