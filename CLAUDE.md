# CLAUDE.md

Claude Code 在此 repo 工作時的最小規範。

## Project Overview

通用的**排序（ranking / learning-to-rank）批次建模框架**。對每個 query group（`time` × `entity`），把候選 `item` 依模型分數排名，供下游依名次分配有限資源。欄位角色（time / entity / item / label）由 `conf/base/parameters.yaml` 的 `schema` 區塊配置；`feature_table` / `sample_pool` / `label_table` 等來源表由使用者自定義。

**商業銀行產品推薦是本 repo 的示例 instantiation**（非框架限定應用）：對每位客戶把候選金融產品排序，供行銷 PM 決定推薦優先順序。

- **Inference**：每週批次推論；示例規模 ~10M entity × 22 item × ~1500 特徵
- **Training**：N 個 snapshot（顯式 `train_snap_dates` list 配置，不一定月底），不定期手動執行
- **Target environment**：PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

## Tech Stack

Python 3.10+ | PySpark 3.3.2 | LightGBM 4.6.0 | scikit-learn 1.5.0 | MLflow 3.1.0 | Optuna 4.5.0 | Ploomber 0.23.3 | pandas 1.5.3 | numpy 1.25.0 | pyarrow 14.0.1 | pytest 7.3.1 | SHAP 0.42.1 | Typer 0.20.1

> 框架是 **Kedro 風格的手刻實作**：`src/recsys_tfb/io/*` 仿 `kedro.io`（`ParquetDataset` / `JSONDataset` / `HiveTableDataset` 等），搭配 `conf/base/{catalog,parameters*}.yaml` 與自製 `DataCatalog` / `Node` / `Pipeline` / `Runner`（`src/recsys_tfb/core/`）。**無 kedro 套件依賴**；上列 `Ploomber` 僅作排程，in-process DAG 由自製 `Runner` 執行。


## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation

## 測試效能（優先讓測試跑快，而不是少跑）

整包 `tests/test_evaluation` 目前約 ~33 分鐘，主因是大量 Spark 測試逐一執行 Spark action（conftest `spark` fixture：`spark.master=local[1]`、`shuffle.partitions=1`；fixture 是 function-scoped，但 `get_or_create_spark_session` 會重用仍存活的 session，並非每測試都重啟）。**正確方向是把測試跑快，不是略過測試。** 待評估的加速手段（採用前先實測，勿臆測；先用 `pytest --durations=20` 找最慢的）：

- conftest `spark` fixture 改 `local[*]` 或調並行度是否有感（小資料下未必，需量測）。
- 全程重用單一 SparkSession（注意 `tune_hyperparameters` 會 `.stop()`，見 conftest 註解需妥善處理）。
- pytest 程序級並行（xdist 類）的取捨：多 JVM 可能反而更慢。
- 減少測試中非必要的 count/collect，用更小固定資料。

測試尚未加速前的**臨時** controller 作法（是權宜、不是解法）：

- 單次改動只跑相關測試檔；驗證優先用 `git diff <base>..<head>`（SHA-based、秒級）＋針對性 grep，不重跑 subagent 已驗過的。
- 可能 >2 分鐘的指令用 background 執行、不阻塞流程（曾因重跑全量空轉整晚）。
- 跨 worktree 驗證用絕對路徑或 `git -C <worktree>`；Bash 指令之間 cwd 會持續（system prompt 明文）但 skill 後可能 reset，相對路徑容易讀到 stale 的 main tree（細節見 §Worktree / venv 踩過的問題 #3）。

## Worktree / venv（完整 SOP：`docs/operations/worktree-venv-setup.md`，務必先讀）

### 已踩過、必須避免再發的問題

1. **`.venv` self-symlink ELOOP**：`.venv` 曾被誤 `git add` 進版控（`0cf79db`），其 symlink 目標指向自己 → 之後任何 checkout / `git worktree add` 都重建這個迴圈，全 `python`/`pytest` 報 `too many levels of symbolic links`。已修（`4e5af3c`：`git rm --cached .venv`、`.gitignore` 同時擋 `.venv` 與 `.venv/`、釘 `.python-version=3.10.9`）。**規則：`.venv` 永不進版控**；`git status`/`git ls-files | grep -x .venv` 一旦出現被追蹤就停下、`git rm --cached .venv`、commit 並**進 main**（否則各分支/worktree 一直繼承）。已追蹤的檔案無視 `.gitignore`。
2. **graphify hook 擋 git checkout/merge（會靜默失敗）**：graphify 的 post-checkout/post-commit hook 會把 **tracked** `graphify-out/GRAPH_REPORT.md` 改髒，使隨後的 `git checkout` / `git merge --ff-only` 被「local changes would be overwritten」擋住；若指令用 `&&`+`set -e`+`>/dev/null` 串接，因 `set -e` 的 AND-list 例外會**靜默失敗、HEAD 沒動**而你以為成功了。**規則：切換分支/合併前先 `git -C <path> checkout -- graphify-out/GRAPH_REPORT.md`；git 串接指令不要吞 stdout/exit code，逐步檢查 HEAD。**
3. **Worktree 內 file path / cd / data symlink 三件踩坑（2026-05-24 連續浪費 ≥3 次 Spark cold start ~2–4min）**：
   - **(R1) 絕對路徑要含 `.worktrees/<name>`**：用 main repo 的絕對路徑 `Edit`/`Write` worktree 的 config —— 改錯邊，worktree 那份沒動、pipeline 讀的還是舊的。徵兆：訓練出來的 `model_version` / best params 跟 baseline 完全相同。
   - **(R2) `cd` 在 Bash tool 之間會持續**（system prompt 明文「The working directory persists between commands」；跟 skill 後 cwd 可能 reset 是兩回事）。`cd <wt>/data && ln -s ...` 後沒 `cd` 回 worktree root，下一個 training 指令 `Path.cwd()/"data"` 看到雙重 `data/data/dataset` → FileNotFoundError after Spark started。**規則**：Bash 指令以 `cd <worktree-root> && ...` 開頭、或全用絕對路徑。
   - **(R3) Worktree 的 `data/` 預設空（只有 `.gitkeep`）**，inference / training / evaluation 寫 model artifact 或讀 dataset 都會 fail。**規則**：第一次進 worktree 跑 pipeline 前 symlink 4 個子目錄到 main（`recsys_cache` 不用，`cache.root` 已是絕對路徑指向 main）：
     ```bash
     cd /Users/curtislu/projects/recsys_tfb/.worktrees/<name>/data
     ln -s /Users/curtislu/projects/recsys_tfb/data/models models
     ln -s /Users/curtislu/projects/recsys_tfb/data/dataset dataset
     ln -s /Users/curtislu/projects/recsys_tfb/data/evaluation evaluation
     ln -s /Users/curtislu/projects/recsys_tfb/data/inference inference
     ```

### Worktree 開發環境啟用 SOP

1. **唯一一個真實 venv**＝`/Users/curtislu/projects/recsys_tfb/.venv`（真實目錄，非 symlink），用 `~/.pyenv/versions/3.10.9/bin/python -m venv` 建（對齊 repo 根 `.python-version=3.10.9`；`pyproject` 要求 `>=3.10,<3.12`，系統 `python3` 是 3.12 **不可用**）＋ `pip install -e ".[dev]"`。各 worktree 的 `.venv` 只是指向它的 **symlink**（`ln -s /Users/curtislu/projects/recsys_tfb/.venv <wt>/.venv`），不建各自獨立 venv。
2. **每次在 worktree 動 python 前先 pre-flight**（Spark pipeline 尤甚，cold start ~2–4min，失敗才發現的成本很高，不要 trust-and-hope）：
   ```bash
   cd /Users/curtislu/projects/recsys_tfb/.worktrees/<name> && pwd          # 在 worktree root
   readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # venv 對齊（Python 3.10.9）
   readlink data/{models,dataset,evaluation,inference}                      # data/ 子目錄已 symlink 到 main
   grep -E "^(objective|metric|snap_date):" conf/base/parameters_*.yaml     # config 真的改在 worktree 那份
   ```
   任一失敗先修再繼續（venv 修復見 `docs/operations/worktree-venv-setup.md`；data symlink / config 路徑問題見上方踩過的問題 #3）。
3. **跑測試/CLI 一律絕對 venv python + `PYTHONPATH=<wt>/src`**：
   `PYTHONPATH=<wt>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
   （裸跑或裸 `.venv/bin/pytest` 會抓到 main 的 `src`＝editable-install target，靜默測/跑錯 code；相對路徑經 symlink 還會 ELOOP）。CLI 同理：`PYTHONPATH=<wt>/src …/.venv/bin/python -m recsys_tfb <pipeline> [--options]`。
4. **跨 worktree git 一律 `git -C <abs-worktree>`**（同 R2：cwd 在 Bash 間持續、skill 後可能 reset，相對路徑容易讀到 stale main tree）。

## Local dev-cluster testing

在本機 dev-cluster 互動測試 pipeline：

- **本機環境**：`~/dev-cluster/`（Docker Spark+HDFS+Hive Metastore），詳見其 README。
- **Hive 來源表 setup**：`scripts/setup_hive_dev.py` 把 `data/{feature_table,label_table,sample_pool}.parquet` 寫成 `ml_recsys.<table>` Hive managed table。**跳過 source_etl**（合成資料已是 feature/label 粒度，沒有上游 `feature_concat`/`label_ccard` 等表）。腳本內**必須把 `snap_date` cast 成 DATE**（合成 parquet 是 timestamp[us]，不轉的話 Spark 對 `'YYYY-MM-DD'` 字串 filter 會 0 row，val/test/calibration 全空）。
- **Ad-hoc / admin PySpark 腳本（setup_hive_dev / nuke_ml_recsys / `SHOW PARTITIONS` 等）**：用 `scripts/dev_admin.sh` wrapper，跑在 transient `devcluster/pyspark` container 內 + `--master local[N]`（README §line 77-91 推薦的 admin pattern）。**不要 host venv**（standalone init 3+ min、`file://<host>` 派給 worker container 找不到）；**也不要 docker exec spark-master**（無 python3）。腳本內 path 寫 `/workspace/...` 不是 host 絕對路徑。詳見 `dev-cluster-spark` skill SOP-6。
  ```bash
  scripts/dev_admin.sh scripts/nuke_ml_recsys.py
  scripts/dev_admin.sh scripts/setup_hive_dev.py
  ```
- **`scripts/` 工具會 `import recsys_tfb` 又讀 Hive 的（如 `sampling_overrides_editor.py`、`suggest_categorical_cols.py`）**：是 host-venv 入口（架構見 [`docs/operations/spark-connection-architecture.md`](docs/operations/spark-connection-architecture.md) §1 入口 E）。**不能**走 `scripts/dev_admin.sh` —— 裸 `devcluster/pyspark` container 沒有 `typer`/`recsys_tfb` 等 venv 套件，import 即 `ModuleNotFoundError`。**必須**：
  ```bash
  source ~/dev-cluster/scripts/client-env.sh                          # 🅔 JDK17 add-opens + HADOOP_CONF_DIR
  export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark    # 🅑 local[*]，省 standalone init 3–5 min
  PYTHONPATH=<root>/src .venv/bin/python scripts/<name>.py <args>
  ```
  順序不能反 —— `client-env.sh` 把 `SPARK_CONF_DIR` 設成 `client-template/spark`，要先 source 再 export 覆寫成 `client-template-local/spark`。**`source client-env.sh` 不是可選**：腳本對 SparkSession 只傳了 `app_name`，所有連線設定全靠環境變數 + spark-defaults.conf。跳過會踩兩種錯：(1) 沒 SPARK_CONF_DIR → 接空 in-memory catalog → `Table or view not found: ml_recsys.<table>`（表其實存在）；(2) JDK17 沒 `--add-opens` → driver 內部反射在 `sun.nio.ch` 上炸。**跑時別被 stderr 嚇到**：local mode 一定會出現一段 `ERROR Inbox: Ignoring error` + `RpcEndpointNotFoundException: CoarseGrainedScheduler@...` —— 是 Spark by-design 良性噪音（`dev-cluster-spark` skill SOP-3-C）。看 stdout 的 `[N/M]` 進度行 / `Wrote ...` 才是真實狀態。
- **/etc/hosts**：host 端讀 Hive 資料前需加 `127.0.0.1 namenode datanode hive-metastore spark-master`，否則 `hdfs://namenode:9000/...` resolve 不到（dev-cluster README §「已知限制」第 3 點）。

### Pipeline 與 SPARK_CONF_DIR 的對應

`--env production` 的 training cache 跟 model artifact (`model.txt` / `calibrator.pkl` / `*.json`) 都駐留在 driver-local fs：cache 由 `_materialize_parquet_handle`（`src/recsys_tfb/pipelines/training/nodes.py`）自己從 HDFS `copyToLocal` 拉下來（不經 catalog `ParquetDataset`、不依賴 `spark.master` 模式；cache node output 是 `ParquetHandle`，由 framework auto-MemoryDataset 在 DAG 中銜接；dev/test 也必須走 `cache.root`，不再有 `enabled=false` 繞行路徑）；artifact 走 Python `open()` 寫不認 `hdfs://` scheme。Pipeline 依下表選對 `SPARK_CONF_DIR`：

| Pipeline | `SPARK_CONF_DIR` | spark.master | 為什麼 |
|---|---|---|---|
| `dataset` / `inference` / `evaluation` / `*_etl` | `~/dev-cluster/client-template/spark`（client-env.sh 預設） | `spark://localhost:7077` | 寫 Hive managed table 走 HDFS，需要 worker container |
| `training` | **`~/dev-cluster/client-template-local/spark`** | `local[*]` | LightGBM 是 driver 單機訓練，distributed cluster 沒幫助；model artifact 駐留 driver-local；cache 由 cache node 自己從 HDFS 拉；evaluate_model 將 test-set 預測寫入 Hive `ml_recsys.training_eval_predictions`（hive-site.xml 已 symlink 進 `client-template-local/spark/`） |

執行：
```bash
source ~/dev-cluster/scripts/client-env.sh                              # 設 HADOOP_CONF_DIR、JDK17 add-opens
# dataset / inference / etc.
.venv/bin/python -m recsys_tfb <pipeline> --env production
# training（必須切 local conf）
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```

走錯 conf 的典型 trap（深入排查見 `dev-cluster-spark` skill）：
- 把 catalog 上 model / best_params / evaluation_results 的 filepath 寫成 `hdfs://` → Python `open()` 在 cwd 建出 literal `./hdfs:/namenode:9000/...` 假目錄
- 早期版本 `client-template-local` 缺 hive-site.xml symlink，會出現 `Table or view not found: ml_recsys.<table>`；現已修正（symlink 至 `~/dev-cluster/client-template-local/hive-site.xml`）

完整入口分類（A/B/C/D/E/F）+ 5 層配置（🅐 Python config / 🅑🅒🅓 conf 檔 / 🅔 env var）對照表見 [`docs/operations/spark-connection-architecture.md`](docs/operations/spark-connection-architecture.md)。跑任何會碰 Spark 的東西**之前**對著 §6 cheat-sheet 走，避免每次重新摸路。

## Config consistency gate

`src/recsys_tfb/core/consistency.py` 是 item-set / column-role 不變量（A1–A13 ＋ 資料閘 B1；各代號意義見該檔**模組 docstring 的 Invariant legend**，程式碼註解中的 `(A1)` 等即指此）的唯一真實來源。`validate_config_consistency(parameters)` 在 CLI entry（`__main__._load_config_and_setup`）執行，collect-all 後一次 raise `ConfigConsistencyError`（`ValueError` 子類），讓使用者在單次修正中解決所有問題。`validate_schema_config`（A3 委派）與 `preprocessing/_spark.py` identity-cat guard（A2 後備）均透過此模組的 predicate，不自行維護重複定義。**新增一致性不變量必須在此新增 predicate，不得在各 pipeline 中以 ad-hoc 方式散落**。Layer-2 資料閘 `validate_data_consistency`（`preprocessing/_spark.py`，dataset pipeline 第一個 side-effect 節點）在跑任何抽樣/前處理前，對 `sample_pool`（與 `resolved_item_values` 雙向集合相等）與 `label_table`（只擋資料端未知 item）做 windowed `distinct(item)` 檢查，raise `DataConsistencyError`；B1 的唯一定義 predicate 是同檔的 `item_coverage_errors`。

## graphify

This project has a graphify knowledge graph at graphify-out/.

**MANDATORY**: For any architecture, refactoring, or codebase exploration task —
read `graphify-out/GRAPH_REPORT.md` BEFORE launching Explore agents or reading raw files.
Do not substitute an Explore agent for this step.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current
