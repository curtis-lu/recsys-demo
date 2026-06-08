# 本機 Spark 測試環境重建 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用純 host `local[*]` + 內嵌 Derby metastore + 本機 fs warehouse 取代 `~/dev-cluster/` 整套 qemu-emulated Docker stack，讓本機測試所有 pipeline 與 Spark scripts 不再失敗/發燙/耗電，且新人能無痛建置。

**Architecture:** Spark 在 host venv 原生 arm64 JVM 跑 `local[*]`（driver=executor 同 JVM）；Hive 表是 managed、落在 worktree 本地 `data/local_warehouse`，metastore 是 pyspark 自帶的內嵌 Derby（`data/metastore_db`）。連線設定 100% 由新的 `SPARK_CONF_DIR=conf/spark-local` 提供，**不動 `src/`**，`conf/base` 只動一處（`cache.root` 相對化）。所有本機狀態相對 worktree root 解析 → per-worktree 隔離。

**Tech Stack:** PySpark 3.3.2（pip 自帶 `derby-10.14.2.0.jar` + `hive-metastore-2.3.9.jar`）, 內嵌 Derby, host JDK 17（需 `--add-opens`）, Python 3.10.9 venv。

**設計依據：** `docs/superpowers/specs/2026-06-08-local-spark-rebuild-design.md`（方案 A）。

---

## 實作前提（執行者必讀）

- **全程在此 worktree**：`/Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild`（branch `feat/local-spark-rebuild`）。所有 Bash 指令以 `cd <worktree-root> && …` 開頭或用絕對路徑。
- **跑 python/CLI 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python …`（裸跑會抓到 main 的 editable-install src）。
- **本 worktree `data/` 預設只有 `.gitkeep`**——這正是新設計要的（每 worktree 自己重建真 `data/` 樹，**不要** symlink 到 main）。Task 1/2 會自動建出 `data/local_warehouse`、`data/metastore_db`。
- **已知 failing 測試**：`TestPrepareTrainInputsWeight` 兩個測試在 main 即 failing（與本變更無關，見 memory `project_prepare_train_inputs_weight_tests_failing_on_main`）。**不要嘗試修它們**；pytest 驗證只跑相關範圍，別跑全套。
- **local[*] 良性噪音**：跑任何 host Spark 一定會在 stderr 看到 `ERROR Inbox: Ignoring error` + `RpcEndpointNotFoundException: CoarseGrainedScheduler@…`——by-design，不是失敗。看 stdout 的成功訊號（`count:`、`[ok]`、`Pipeline completed`）。
- **commit 時只 `git add` 明列檔案**，避免帶進 `graphify-out/GRAPH_REPORT.md`（post-commit hook 會改它）與 `data/` 產物。

---

## File Structure

**新增**
- `conf/spark-local/spark-defaults.conf` — 本機 Spark 連線唯一設定（local[*] + 內嵌 Derby + 本機 warehouse + JDK17 add-opens）
- `scripts/local_spark_setup.py` — 合成 parquet → `ml_recsys.*` managed 表；`--reset`、`--check-isolation`
- `scripts/local_e2e.sh` — 本機端到端 smoke（setup→dataset→training→inference→evaluation），兼 training cache `file://` 複製驗證
- `docs/operations/local-spark-setup.md` — 新人無痛建置單一權威指南

**修改**
- `conf/base/parameters_training.yaml` — `cache.root` 絕對→相對（唯一 conf/base 改動）
- `CLAUDE.md` — 換掉「Local dev-cluster testing」+「Pipeline 與 SPARK_CONF_DIR 對應」；改寫「Worktree / venv」R3 隔離段
- `docs/operations/spark-connection-architecture.md` — 瘦身為「本機=conf/spark-local」+「公司=換 SPARK_CONF_DIR」
- `docs/operations/worktree-venv-setup.md` — worktree 隔離模型（廢 symlink-to-main、加 `--check-isolation`）
- `~/.claude/skills/dev-cluster-spark/SKILL.md` → 改名/重寫為 `local-spark`

**刪除**
- `scripts/dev_admin.sh`
- `scripts/setup_hive_dev.py`
- `scripts/nuke_ml_recsys.py`
- （`scripts/dev_e2e_two_stage.sh` 不在 main，無需刪）

---

## Task 1: 本機 SPARK_CONF_DIR（conf/spark-local/spark-defaults.conf）

**Files:**
- Create: `conf/spark-local/spark-defaults.conf`

- [ ] **Step 1: 寫 conf 檔**

`conf/spark-local/spark-defaults.conf`：

```properties
# 本機測試用 Spark 設定（方案 A）。純 host local[*] + 內嵌 Derby metastore + 本機 fs warehouse。
# 連線設定唯一來源；切到公司環境＝換 SPARK_CONF_DIR（見 docs/operations/spark-connection-architecture.md）。
# 路徑相對 repo/worktree root（CLI 一律從 root 跑）→ per-worktree 隔離。

spark.master                                        local[*]
spark.submit.deployMode                             client

spark.sql.catalogImplementation                     hive
spark.sql.warehouse.dir                             data/local_warehouse
spark.hadoop.javax.jdo.option.ConnectionURL         jdbc:derby:;databaseName=data/metastore_db;create=true
spark.hadoop.javax.jdo.option.ConnectionDriverName  org.apache.derby.jdbc.EmbeddedDriver

spark.sql.session.timeZone                          Asia/Taipei
spark.serializer                                    org.apache.spark.serializer.KryoSerializer
spark.driver.memory                                 4g

# host JDK17 必要：driver 內部反射打進 sun.nio.ch 等模組。-XX:+IgnoreUnrecognizedVMOptions 讓 JDK11 下也無害。
# 故意不設 driver.host/port/blockManager（避開 RpcEndpointNotFoundException 陷阱）；eventLog 不開。
spark.driver.extraJavaOptions  -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

- [ ] **Step 2: Smoke test — 內嵌 metastore round-trip**

Run（從 worktree root）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
rm -rf data/local_warehouse data/metastore_db
SPARK_CONF_DIR=$PWD/conf/spark-local PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.appName("conf-smoke").getOrCreate()
s.sql("CREATE DATABASE IF NOT EXISTS smoke_db")
s.sql("CREATE TABLE IF NOT EXISTS smoke_db.t (a INT, b STRING) STORED AS PARQUET")
s.sql("INSERT INTO smoke_db.t VALUES (1,'x'),(2,'y')")
print("SMOKE_COUNT:", s.table("smoke_db.t").count())
s.stop()
PY
```

Expected: stdout 含 `SMOKE_COUNT: 2`（中間夾雜的 `RpcEndpointNotFoundException` 噪音忽略）。

- [ ] **Step 3: 驗證 warehouse / metastore 落在 worktree data/**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
ls -d data/local_warehouse/smoke_db.db data/metastore_db && echo ISOLATION_OK
```

Expected: 兩路徑存在 + `ISOLATION_OK`（證明相對路徑解析到 worktree、無 Docker/HDFS）。清掉 smoke：`rm -rf data/local_warehouse data/metastore_db derby.log`。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add conf/spark-local/spark-defaults.conf
git commit -m "feat(local-spark): 新增 conf/spark-local（local[*]+內嵌 Derby+本機 warehouse）"
```

---

## Task 2: scripts/local_spark_setup.py（合成資料 → Hive managed 表 + 隔離閘）

**Files:**
- Create: `scripts/local_spark_setup.py`

- [ ] **Step 1: 寫 setup 腳本**

`scripts/local_spark_setup.py`（取代 `setup_hive_dev.py` + `nuke_ml_recsys.py` + `dev_admin.sh`；在 host venv 直接跑、managed 表、無 HDFS LOCATION）：

```python
"""本機 Spark 測試環境 setup：合成 parquet → ml_recsys.* managed 表（本機 warehouse）。

在 host venv 直接跑（無 Docker、無 transient container）。連線設定全來自 SPARK_CONF_DIR
（conf/spark-local）；warehouse / metastore 落在此 worktree 的 data/ 下，per-worktree 隔離。

用法（從 repo/worktree root）：
    export SPARK_CONF_DIR=$PWD/conf/spark-local
    PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py [--reset] [--check-isolation]
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

DB = "ml_recsys"
DATA = Path("data")
WAREHOUSE = DATA / "local_warehouse"
METASTORE = DATA / "metastore_db"
CACHE = DATA / "recsys_cache"
TABLES = {
    "feature_table": DATA / "feature_table.parquet",
    "label_table": DATA / "label_table.parquet",
    "sample_pool": DATA / "sample_pool.parquet",
}


def check_isolation() -> None:
    """跑 Spark 前 fast assert：本機狀態全在此 worktree 內、無指向 main。任一不過即 exit 1。"""
    root = Path.cwd()
    errors: list[str] = []

    conf = os.environ.get("SPARK_CONF_DIR", "")
    expected_conf = root / "conf" / "spark-local"
    if not conf or os.path.realpath(conf) != os.path.realpath(expected_conf):
        errors.append(f"SPARK_CONF_DIR={conf!r}，應為 {expected_conf}")

    for name, p in {"local_warehouse": WAREHOUSE, "metastore_db": METASTORE, "recsys_cache": CACHE}.items():
        if p.is_symlink():
            errors.append(f"data/{name} 是 symlink（應為 worktree 真目錄）→ {os.readlink(p)}")

    if errors:
        print("[check-isolation] FAIL:")
        for e in errors:
            print("  -", e)
        sys.exit(1)
    print(f"[check-isolation] OK：root={root}；warehouse/metastore/cache 皆 worktree 本地、無指向 main")


def reset() -> None:
    for p in (WAREHOUSE, METASTORE):
        if p.exists():
            shutil.rmtree(p)
            print(f"[reset] removed {p}")


def ensure_synthetic_data() -> None:
    missing = [str(p) for p in TABLES.values() if not p.exists()]
    if missing:
        print(f"[setup] 缺合成 parquet {missing} → 執行 generate_synthetic_data.py")
        subprocess.run([sys.executable, "scripts/generate_synthetic_data.py"], check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="清 warehouse + metastore 後重建")
    ap.add_argument("--check-isolation", action="store_true", help="只做隔離 pre-flight 後結束")
    args = ap.parse_args()

    check_isolation()
    if args.check_isolation:
        return
    if args.reset:
        reset()

    ensure_synthetic_data()

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_date
    from pyspark.sql.types import TimestampType

    spark = SparkSession.builder.appName("local_spark_setup").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    print(f"[ok] database ready: {DB}")
    for table, path in TABLES.items():
        df = spark.read.parquet(path.resolve().as_uri())
        # 合成 parquet 的 snap_date 是 timestamp[us]；不 cast DATE 的話對 'YYYY-MM-DD' 字串
        # filter 會 0 row（val/test/calibration 全空）。
        if "snap_date" in df.columns and isinstance(
            df.schema["snap_date"].dataType, TimestampType
        ):
            df = df.withColumn("snap_date", to_date("snap_date"))
        full = f"{DB}.{table}"
        df.write.mode("overwrite").saveAsTable(full)
        print(f"[ok] {full}: {spark.table(full).count()} rows, columns={df.columns}")
    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 跑 setup，驗證表 + 行數 + snap_date 型別**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py 2>/dev/null
```

Expected: stdout 含 `[check-isolation] OK`、三行 `[ok] ml_recsys.<table>: N rows`（N>0）、`SHOW TABLES` 列出三表。

- [ ] **Step 3: 驗證 snap_date 是 DATE（非 timestamp）**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
SPARK_CONF_DIR=$PWD/conf/spark-local PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY' 2>/dev/null
from pyspark.sql import SparkSession
s = SparkSession.builder.appName("type-check").getOrCreate()
print("SNAP_TYPE:", dict(s.table("ml_recsys.feature_table").dtypes)["snap_date"])
s.stop()
PY
```

Expected: `SNAP_TYPE: date`。

- [ ] **Step 4: 驗證 --reset 與 --check-isolation**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
echo "--- isolation 應 fail（故意拔 SPARK_CONF_DIR）---"
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation; echo "exit=$?"
```

Expected: 第一個 `[check-isolation] OK`；第二個印 FAIL + `exit=1`（證明閘有效）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add scripts/local_spark_setup.py
git commit -m "feat(local-spark): local_spark_setup.py（合成→ml_recsys managed 表 + 隔離閘）"
```

---

## Task 3: cache.root 絕對→相對（conf/base/parameters_training.yaml）

**Files:**
- Modify: `conf/base/parameters_training.yaml:161`

- [ ] **Step 1: 改一行**

把 `conf/base/parameters_training.yaml` 第 161 行：

```yaml
  root: /Users/curtislu/projects/recsys_tfb/data/recsys_cache
```

改為：

```yaml
  root: data/recsys_cache
```

（相對 CWD=worktree root 解析 → per-worktree 隔離 + 移除 `/Users/curtislu` 硬路徑讓新人可用。`_resolve_cache_path` 以 `Path(root)` 讀取，相對值維持相對。）

- [ ] **Step 2: 驗證 config 載入 + 無殘留硬路徑**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
from recsys_tfb.core.config import ConfigLoader
p = ConfigLoader("conf", env="local").get_parameters_by_name("parameters_training")
root = p["cache"]["root"]
assert root == "data/recsys_cache", root
print("CACHE_ROOT_OK:", root)
PY
grep -rn "/Users/curtislu" conf/base/ && echo "STILL_HARDCODED" || echo "NO_HARDCODE_OK"
```

Expected: `CACHE_ROOT_OK: data/recsys_cache` + `NO_HARDCODE_OK`。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add conf/base/parameters_training.yaml
git commit -m "fix(config): cache.root 絕對→相對 data/recsys_cache（worktree 隔離 + 新人可用）"
```

---

## Task 4: scripts/local_e2e.sh（端到端 smoke + cache file:// 複製驗證）

**Files:**
- Create: `scripts/local_e2e.sh`

- [ ] **Step 1: 寫 e2e 腳本**

`scripts/local_e2e.sh`（精簡、無 Docker；setup→四 pipeline 全 local）：

```bash
#!/usr/bin/env bash
# 本機端到端 smoke：local_spark_setup --reset → dataset → training → inference → evaluation，全 local[*]。
# 作為「本機能跑 ⇒ 公司能跑」最終確認，並驗證 training cache 的 file:// 本機複製。
# 用法（從 repo/worktree root）：  bash scripts/local_e2e.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VENV=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
export SPARK_CONF_DIR="$ROOT/conf/spark-local"
export PYTHONPATH="$ROOT/src"

run() { echo; echo "▶ $*"; "$@"; }

run "$VENV" scripts/local_spark_setup.py --reset
for stage in dataset training inference evaluation; do
  run "$VENV" -m recsys_tfb "$stage" --env local
done
echo; echo "✅ local e2e 完成：dataset→training→inference→evaluation 全 local[*]"
```

`chmod +x scripts/local_e2e.sh`。

- [ ] **Step 2: 背景跑 e2e（可能數分鐘），收集結果**

Run（背景，避免阻塞；本機原生 arm64 應遠快於舊 qemu）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
bash scripts/local_e2e.sh > data/e2e.log 2>&1 &
echo "started pid=$!"
```

之後 `tail -n 40 data/e2e.log` 檢查。Expected 最終：`✅ local e2e 完成`，且各 pipeline 有 `Pipeline completed`。

- [ ] **Step 3: 驗證 training cache 是本機複製（§8 唯一 infra smoke）**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
ls -R data/recsys_cache | head -20
grep -iE "copyToLocal|Py4J|copy_hdfs_to_local" data/e2e.log || echo "NO_CACHE_ERROR_OK"
```

Expected: `data/recsys_cache/` 下有 materialized parquet（證明 `copy_hdfs_to_local` 在 `file://` 下成功）、無 Py4J/copyToLocal 錯誤 → `NO_CACHE_ERROR_OK`。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add scripts/local_e2e.sh
git commit -m "feat(local-spark): local_e2e.sh 端到端 smoke（取代 dev_e2e，全 local）"
```

---

## Task 5: 刪除退役 scripts

**Files:**
- Delete: `scripts/dev_admin.sh`, `scripts/setup_hive_dev.py`, `scripts/nuke_ml_recsys.py`

- [ ] **Step 1: 確認無殘留引用**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
grep -rnE "dev_admin\.sh|setup_hive_dev|nuke_ml_recsys" \
  --include='*.py' --include='*.sh' --include='*.md' --include='*.yaml' . \
  | grep -vE '^\./docs/superpowers/(specs|plans)/' || echo "NO_REFS_OK"
```

Expected: `NO_REFS_OK`（spec/plan 內提及不算；若 CLAUDE.md/docs 仍引用，那些會在 Task 6-10 一併改寫——本步只確認沒有**程式**還呼叫它們）。

- [ ] **Step 2: 刪除並 commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git rm scripts/dev_admin.sh scripts/setup_hive_dev.py scripts/nuke_ml_recsys.py
git commit -m "chore(local-spark): 刪除 dev-cluster 退役 scripts（dev_admin/setup_hive_dev/nuke_ml_recsys）"
```

---

## Task 6: docs/operations/local-spark-setup.md（新人指南）

**Files:**
- Create: `docs/operations/local-spark-setup.md`

- [ ] **Step 1: 寫指南**

`docs/operations/local-spark-setup.md`，須含以下章節與**確切內容**（識別字對齊程式碼）：

````markdown
# 本機 Spark 測試環境

本機測試**不需要 Docker / HDFS / Hive Metastore container**。Spark 在你的 venv 裡用 `local[*]`
跑，Hive 表是 managed、落在 `data/local_warehouse`，metastore 是 pyspark 自帶的內嵌 Derby
（`data/metastore_db`）。連線設定全在 `conf/spark-local/spark-defaults.conf`。

> 公司環境的 Spark 連線沒問題；這套**只為本機測試程式**。切到公司環境＝換 `SPARK_CONF_DIR`，
> 見 [`spark-connection-architecture.md`](spark-connection-architecture.md)。

## 一次性建置

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py        # 產合成資料 + 建 ml_recsys.* 表
```

`local_spark_setup.py` 會：合成 parquet（缺則自動跑 `generate_synthetic_data.py`）→ `snap_date`
cast DATE → `saveAsTable` 進 `ml_recsys.{feature_table,label_table,sample_pool}`。

## 跑 pipeline / scripts

```bash
export SPARK_CONF_DIR=$PWD/conf/spark-local      # 每個新 shell 都要
PYTHONPATH=src .venv/bin/python -m recsys_tfb <dataset|training|inference|evaluation> --env local
PYTHONPATH=src .venv/bin/python scripts/suggest_categorical_cols.py <args>
PYTHONPATH=src .venv/bin/python scripts/sampling_overrides_editor.py <args>
```

端到端一鍵：`bash scripts/local_e2e.sh`。

## 重置

```bash
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py --reset   # rm warehouse+metastore 後重建
```

## 注意事項

- **一定從 repo/worktree root 跑**：`conf/spark-local` 的路徑是相對的（`data/local_warehouse` 等）。
- **stderr 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是 local[*] by-design 噪音**，看 stdout 成功訊號即可。
- **內嵌 Derby 單行程獨佔**：別同時跑兩個 Spark 行程碰同一 `data/metastore_db`（會撞鎖）；循序跑即可。
- **隔離**：`local_spark_setup.py --check-isolation` 確認 warehouse/metastore/cache 都在此 worktree、無指向 main。
````

- [ ] **Step 2: 驗證無 stale 名詞**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
grep -inE "dev-cluster|dev_admin|client-env|spark://|hdfs://|namenode|docker" docs/operations/local-spark-setup.md && echo "HAS_STALE" || echo "CLEAN_OK"
```

Expected: `CLEAN_OK`。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add docs/operations/local-spark-setup.md
git commit -m "docs(local-spark): 新增本機建置權威指南 local-spark-setup.md"
```

---

## Task 7: 改寫 docs/operations/spark-connection-architecture.md

**Files:**
- Modify (大幅重寫): `docs/operations/spark-connection-architecture.md`

- [ ] **Step 1: 先讀現況、確認被引用處**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
wc -l docs/operations/spark-connection-architecture.md
grep -rn "spark-connection-architecture" --include='*.md' . | grep -v docs/superpowers
```

- [ ] **Step 2: 重寫為兩條主線**

把整份改寫成下列結構（刪掉 A–F 入口分類 / 5 層配置 / dev-cluster 雙路由表；保留「為什麼連線設定統一交給 `SPARK_CONF_DIR`」的原理）：

````markdown
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
（保留原文對「app conf 不寫 spark.master/driver.host/port，全交 SPARK_CONF_DIR」的說明——
避免 app conf 與 spark-defaults.conf 對 master 不一致導致 RpcEndpointNotFoundException。）
````

- [ ] **Step 3: 驗證無 stale 名詞**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
grep -inE "client-template|dev_admin|spark://localhost:7077|namenode:9000|入口 [A-F]" docs/operations/spark-connection-architecture.md && echo "HAS_STALE" || echo "CLEAN_OK"
```

Expected: `CLEAN_OK`。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add docs/operations/spark-connection-architecture.md
git commit -m "docs(local-spark): 瘦身 spark-connection-architecture（本機 conf/spark-local + 公司換 SPARK_CONF_DIR）"
```

---

## Task 8: 改寫 docs/operations/worktree-venv-setup.md（隔離模型）

**Files:**
- Modify: `docs/operations/worktree-venv-setup.md`

- [ ] **Step 1: 讀現況**

Run: `cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild && grep -nE "symlink|data/|recsys_cache|models|dataset" docs/operations/worktree-venv-setup.md`

- [ ] **Step 2: 改寫 data/ 處理段為「每 worktree 真 data/ 樹」**

把原本「symlink `data/{models,dataset,evaluation,inference}` 到 main、`recsys_cache` 不用因 cache.root 絕對指向 main」的內容，替換為：

````markdown
## Worktree data/ 隔離（重要）

每個 worktree 是**完全自足的沙盒**：所有本機狀態相對 worktree root 解析、**不 symlink 到 main**。
重建後 setup 很快（無 qemu），不需要共用 main 的 artifact。

| 狀態 | 位置（相對 worktree root） |
|---|---|
| Hive warehouse | `data/local_warehouse`（`local_spark_setup.py` 建） |
| 內嵌 Derby metastore | `data/metastore_db` |
| 檔案 artifact | `data/{models,dataset,evaluation,inference}`（pipeline 自動建真目錄） |
| training cache | `data/recsys_cache`（`cache.root` 已相對化） |

首次進 worktree 只需建 venv symlink（見下），**不需要再 symlink data/ 子目錄**；
跑 `local_spark_setup.py` 即重建本機資料。驗證隔離：`local_spark_setup.py --check-isolation`。

> 若**刻意**要拿 main 的真 artifact 測（例如評估 main 訓練好的 model），才針對該子目錄手動 symlink（opt-in）。
````

- [ ] **Step 3: 驗證**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
grep -inE "ln -s .*/data/(models|dataset|evaluation|inference)" docs/operations/worktree-venv-setup.md && echo "STILL_SYMLINK_SOP" || echo "OK_NO_SYMLINK_SOP"
grep -in "check-isolation" docs/operations/worktree-venv-setup.md && echo "HAS_GATE_OK"
```

Expected: `OK_NO_SYMLINK_SOP` + `HAS_GATE_OK`。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add docs/operations/worktree-venv-setup.md
git commit -m "docs(local-spark): worktree 隔離模型（每 worktree 真 data/ 樹 + check-isolation）"
```

---

## Task 9: 改寫 CLAUDE.md（dev-cluster 兩節 + Worktree R3）

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: 換掉「Local dev-cluster testing」+「Pipeline 與 SPARK_CONF_DIR 的對應」兩節**

刪除這兩整節（從 `## Local dev-cluster testing` 到「Config consistency gate」之前），替換為單節：

````markdown
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
- `--env local`（預設）；不再有「`--env production` 指本機」。
- stderr 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是 local[*] by-design 噪音。
- 端到端 smoke：`bash scripts/local_e2e.sh`。
````

- [ ] **Step 2: 改寫 Worktree R3 隔離段**

把「### 已踩過、必須避免再發的問題」第 3 點（R3）裡「symlink 4 個子目錄到 main … `cache.root` 已是絕對路徑指向 main」那塊，替換為：

````markdown
   - **(R3) Worktree `data/` 隔離**：每個 worktree 用**自己的真 `data/` 樹**，**不 symlink 到 main**
     （`cache.root` 已相對化＝`data/recsys_cache`、warehouse/metastore 也相對）。首次進 worktree 跑
     `PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py` 即重建本機資料；隔離驗證用
     `scripts/local_spark_setup.py --check-isolation`。詳見 `docs/operations/worktree-venv-setup.md`。
````

並把「### Worktree 開發環境啟用 SOP」第 2 點 pre-flight 裡 `readlink data/{models,dataset,evaluation,inference}` 那行，改為：

````markdown
   PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation  # data/ 隔離閘
````

- [ ] **Step 3: 驗證 CLAUDE.md 無 stale dev-cluster 操作**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
grep -inE "dev_admin|setup_hive_dev|nuke_ml_recsys|client-env|spark://localhost:7077|--env production|client-template" CLAUDE.md && echo "HAS_STALE" || echo "CLEAN_OK"
```

Expected: `CLEAN_OK`。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
git add CLAUDE.md
git commit -m "docs(local-spark): CLAUDE.md 換本機 Spark 測試節 + Worktree R3 隔離"
```

---

## Task 10: 改寫 skill dev-cluster-spark → local-spark

**Files:**
- Modify/rename: `~/.claude/skills/dev-cluster-spark/SKILL.md` → `~/.claude/skills/local-spark/SKILL.md`

> 注意：在 repo 外（user settings），使用者已同意改。先 `mkdir -p ~/.claude/skills/local-spark` 寫新檔，再 `rm -rf ~/.claude/skills/dev-cluster-spark`。此 Task **不進 git**（skill 不在 repo）。

- [ ] **Step 1: 寫新 skill**

`~/.claude/skills/local-spark/SKILL.md`，frontmatter name 改 `local-spark`，body 只保留本機仍會遇到的 SOP：

````markdown
---
name: local-spark
description: 本機 local[*] Spark 測試環境（內嵌 Derby + 本機 warehouse）的踩坑 SOP。
---

# local-spark

權威建置/執行步驟見 repo `docs/operations/local-spark-setup.md`。本 skill 只記踩過才知道的 SOP。
**已無 Docker/HDFS/Hive container**——舊 dev-cluster 的 metastore-stuck / HDFS-URI / file://-on-worker /
admin-container SOP 全數作廢。

## SOP-A：pandas≥1.5 ns timestamp 寫 parquet，Spark 3.3.2 只到 us
（保留原 dev-cluster-spark SOP-2 全文：`pq.write_table(..., coerce_timestamps='us',
allow_truncated_timestamps=True)`；`local_spark_setup.py` 另以 `to_date` 把 snap_date cast DATE。）

## SOP-B：local[*] 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是良性噪音
（保留原 SOP-3-C 全文：根因 `BlockManagerMasterEndpoint.isExecutorAlive` 在 local mode 查不到
cluster endpoint，被 `Inbox.safelyCall` 吞掉。判別：stdout 有 `[ok]`/`count:`/`Pipeline completed`
就是正常。別用 `head` 過早截斷被 RPC stack 填滿。）

## SOP-C：內嵌 Derby 單行程獨佔
同時兩個 Spark 行程碰同一 `data/metastore_db` → Derby `Another instance ... booted the database`。
循序跑即可；或各 worktree 各自一份（相對路徑天然隔離）。

## SOP-D：一定從 repo/worktree root 跑
`conf/spark-local` 路徑相對（`data/local_warehouse` 等）。不在 root 跑 → warehouse/metastore
建錯地方、`ml_recsys.*` 找不到。跑前 `scripts/local_spark_setup.py --check-isolation` 驗。
````

- [ ] **Step 2: 移除舊 skill 目錄**

```bash
mkdir -p ~/.claude/skills/local-spark
# （Step 1 已寫入 SKILL.md）
test -f ~/.claude/skills/local-spark/SKILL.md && rm -rf ~/.claude/skills/dev-cluster-spark && echo "RENAMED_OK"
ls ~/.claude/skills/ | grep -E "local-spark|dev-cluster-spark"
```

Expected: `RENAMED_OK`，且只剩 `local-spark`、無 `dev-cluster-spark`。

- [ ] **Step 3: 確認 repo 內無殘留指向舊 skill 名**

Run:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/local-spark-rebuild
grep -rinE "dev-cluster-spark" --include='*.md' . | grep -v docs/superpowers && echo "HAS_STALE_REF" || echo "CLEAN_OK"
```

Expected: `CLEAN_OK`（若 CLAUDE.md/docs 仍提 `dev-cluster-spark` skill 名，改成 `local-spark`，併入相關 commit）。

---

## Self-Review（撰寫者已核對）

**Spec coverage：**
- §5.1 conf → Task 1 ✓；§5.2 setup → Task 2 ✓；§5.3 統一流程 → 體現在 Task 4/6/9 ✓；§5.4 local_e2e → Task 4 ✓；§5.5 退役 → Task 5 ✓；§5.6 隔離（含 cache.root + check-isolation + 廢 symlink）→ Task 2/3/8/9 ✓；§6 文件/skill/CLAUDE.md → Task 6/7/8/9/10 ✓；§7 保真度 → 由 Task 4 e2e 實證 ✓；§8 cache smoke → Task 4 Step 3 ✓；§9 測試 → Task 4 + 既有 pytest（不動）✓；§11 異動表逐項對應 ✓。
- 唯一 conf/base 改動（cache.root）= Task 3，與 spec §5.6/§11 一致。

**Placeholder scan：** 程式/設定 step 皆完整內容；docs step 給確切章節文字 + grep 驗收，無 TBD/TODO。

**Type/識別字一致：** `ml_recsys`、`local_warehouse`、`metastore_db`、`recsys_cache`、`--reset`、`--check-isolation`、`conf/spark-local`、`--env local` 跨 task 一致；`local_spark_setup.py` 的 function 名（`check_isolation`/`reset`/`ensure_synthetic_data`）與 e2e/docs 引用一致。

**已知 failing 測試**（`TestPrepareTrainInputsWeight`）不在本計畫範圍，已於前提標註不修。
