#!/usr/bin/env bash
# 本機端到端 smoke：local_spark_setup --reset → dataset → training，全 local[*]。
# 證明本機 Spark 環境可用：Spark SQL / Hive managed 表讀寫 / partition / parquet，
# 以及 training cache 的 file:// 本機複製（取代舊 qemu HDFS copyToLocal）。
#
# 為什麼到 training 為止：inference/evaluation 的「端到端」驗證目前 gated on issue #63
#   —— training 有 feature selection、inference 沒有 → 模型維度不合（既有落差，
#   非本機環境問題；本機環境已能跑到 inference 讀完 Hive、卡在模型預測層）。
#   #63 對齊後再把 inference/evaluation 納回本 smoke。
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

echo
echo "✅ local e2e（環境證明）完成：dataset→training 全 local[*]，含 training cache file:// 複製。"
echo "ℹ️  inference/evaluation 端到端驗證 gated on issue #63（feature selection 對齊）；本機環境本身已可跑。"
