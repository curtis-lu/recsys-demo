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
   - **(R3) Worktree `data/` 隔離**：每個 worktree 用**自己的真 `data/` 樹**，**不 symlink 到 main**
     （`cache.root` 已相對化＝`data/recsys_cache`、warehouse/metastore 也相對）。首次進 worktree 跑
     `PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py` 即重建本機資料；隔離驗證用
     `scripts/local_spark_setup.py --check-isolation`。詳見 `docs/operations/worktree-venv-setup.md`。

### Worktree 開發環境啟用 SOP

1. **唯一一個真實 venv**＝`/Users/curtislu/projects/recsys_tfb/.venv`（真實目錄，非 symlink），用 `~/.pyenv/versions/3.10.9/bin/python -m venv` 建（對齊 repo 根 `.python-version=3.10.9`；`pyproject` 要求 `>=3.10,<3.12`，系統 `python3` 是 3.12 **不可用**）＋ `pip install -e ".[dev]"`。各 worktree 的 `.venv` 只是指向它的 **symlink**（`ln -s /Users/curtislu/projects/recsys_tfb/.venv <wt>/.venv`），不建各自獨立 venv。
2. **每次在 worktree 動 python 前先 pre-flight**（Spark pipeline 尤甚，cold start ~2–4min，失敗才發現的成本很高，不要 trust-and-hope）：
   ```bash
   cd /Users/curtislu/projects/recsys_tfb/.worktrees/<name> && pwd          # 在 worktree root
   readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # venv 對齊（Python 3.10.9）
   PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation  # data/ 隔離閘
   grep -E "^(objective|metric|snap_date):" conf/base/parameters_*.yaml     # config 真的改在 worktree 那份
   ```
   任一失敗先修再繼續（venv 修復見 `docs/operations/worktree-venv-setup.md`；data symlink / config 路徑問題見上方踩過的問題 #3）。
3. **跑測試/CLI 一律絕對 venv python + `PYTHONPATH=<wt>/src`**：
   `PYTHONPATH=<wt>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
   （裸跑或裸 `.venv/bin/pytest` 會抓到 main 的 `src`＝editable-install target，靜默測/跑錯 code；相對路徑經 symlink 還會 ELOOP）。CLI 同理：`PYTHONPATH=<wt>/src …/.venv/bin/python -m recsys_tfb <pipeline> [--options]`。
4. **跨 worktree git 一律 `git -C <abs-worktree>`**（同 R2：cwd 在 Bash 間持續、skill 後可能 reset，相對路徑容易讀到 stale main tree）。

## 本機 Spark 測試

本機測試**不用 Docker/HDFS/Hive container**：Spark 在 venv 跑 `local[*]`、Hive 表 managed
落 `data/local_warehouse`、metastore 是內嵌 Derby（`data/metastore_db`）。連線設定全在
`conf/spark-local/spark-defaults.conf`。完整步驟見
[`docs/operations/local-spark-setup.md`](docs/operations/local-spark-setup.md)。

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py            # 首次 / --reset
PYTHONPATH=src .venv/bin/python -m recsys_tfb <pipeline> --env local    # 所有 pipeline 同一條路
```

- 所有 pipeline（dataset/training/inference/evaluation/`*_etl`）與 scripts
  （`suggest_categorical_cols`、`sampling_overrides_editor`）皆 `export SPARK_CONF_DIR` 後 host venv 直跑。
- `--env local`（預設）；`--env local` 是唯一本機環境識別符，無需切換。
- stderr 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是 local[*] by-design 噪音。
- 端到端 smoke：`bash scripts/local_e2e.sh`。

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
