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
