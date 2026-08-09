#!/usr/bin/env bash
# 本機端到端 smoke：local_spark_setup --reset → dataset → training → inference，全 local[*]。
# 證明本機 Spark 環境可用：Spark SQL / Hive managed 表讀寫 / partition / parquet，
# 以及 training cache 的 file:// 本機複製（取代舊 qemu HDFS copyToLocal）。
#
# 為什麼要跑到 inference：這是唯一「送進去對、存進去錯」看得到的地方。#185 之前
#   三張推論表的 item 分區目錄叫 prod_name=0…7 而不是產品名，六個 sanity check 全綠、
#   pipeline 回報成功——只有實跑後去看 warehouse 的目錄名才會發現（ADR-0010 §6）。
#   末尾那段 assert 就是把那個失效模式釘在這裡。
#   （本檔曾寫「inference gated on issue #63」；實測早已過期，#185 一併修掉。）
#
# 模型：用 training 剛產出的版本，走既有的 --model-version 選項，**不觸發 promote**
#   （promote 保留給人工）。
# 用法（從 repo/worktree root）：  bash scripts/local_e2e.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VENV=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
export SPARK_CONF_DIR="$ROOT/conf/spark-local"
export PYTHONPATH="$ROOT/src"

run() { echo; echo "▶ $*"; "$@"; }

run "$VENV" scripts/local_spark_setup.py --reset
run "$VENV" -m recsys_tfb dataset  --env local
run "$VENV" -m recsys_tfb training --env local

MODEL_VERSION="$("$VENV" - <<'PY'
import sys
from pathlib import Path

from recsys_tfb.core.versioning import find_latest_completed_model_version

found = find_latest_completed_model_version(Path("data/models"))
if found is None:
    sys.exit("data/models 下沒有 status=completed 的模型；training 這一步沒真的產出模型")
print(found[0])
PY
)"
echo "▶ inference model_version=$MODEL_VERSION"
run "$VENV" -m recsys_tfb inference --env local --model-version "$MODEL_VERSION"

# 分區目錄名必須是產品名。用 conf 的 inference.products 當期望值，不是 hard-code：
# 這條 assert 要在「編碼值取錯清單」時也會紅，而 hard-code 的清單會跟著 conf 一起漂。
echo
echo "▶ assert：三張推論表的 item 分區目錄名是產品名而非整數 code"
"$VENV" - <<'PY'
import sys
from pathlib import Path

import yaml

params = yaml.safe_load(Path("conf/base/parameters_inference.yaml").read_text())
products = set(params["inference"]["products"])
db = Path("data/local_warehouse/ml_recsys.db")

failures = []
for table in ("score_table", "ranked_staging", "ranked_predictions"):
    dirs = sorted(p.name for p in db.glob(f"{table}/snap_date=*/prod_name=*"))
    values = {d.split("=", 1)[1] for d in dirs}
    if not values:
        failures.append(f"{table}: 沒有任何 prod_name 分區目錄")
    elif values != products:
        failures.append(f"{table}: 分區值 {sorted(values)} != inference.products")
    else:
        print(f"  ✓ {table}: {len(values)} 個分區，例：{sorted(values)[0]}")

if failures:
    print("\n".join("  ✗ " + f for f in failures), file=sys.stderr)
    sys.exit(1)
PY

echo
echo "✅ local e2e（環境證明）完成：dataset→training→inference 全 local[*]，"
echo "   含 training cache file:// 複製，以及推論表 item 分區目錄名的實跑斷言。"
