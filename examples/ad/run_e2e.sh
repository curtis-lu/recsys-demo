#!/usr/bin/env bash
# 廣告情境示例的端到端實跑，全 local[*]：
#   原始表 → source_etl 四條 → dataset → training → inference → evaluation --post-training → digest
#
# 對應銀行示例的 scripts/local_e2e.sh，多了 source_etl 與 evaluation 兩段：銀行示例直接
# 寫出來源表、跳過 source_etl，而這個示例要讓後面幾張票的新路徑（event、多張特徵表、
# item 清單從資料數、預測品質指標）從上游 SQL 一路被走到。
#
# 用法（任何目錄皆可）：
#   bash examples/ad/run_e2e.sh                    # 跑完寫 data/digest.json
#   bash examples/ad/run_e2e.sh --compare          # 另外與 baseline_digest.json 逐項比對，不同就 exit 1
#
# 模型：用 training 剛產出的版本，走 --model-version，不觸發 promote（保留給人工）。
set -euo pipefail

AD_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$AD_DIR/../.." && pwd)"
cd "$AD_DIR"

PY="${RECSYS_PYTHON:-$ROOT/.venv/bin/python}"
if [ ! -x "$PY" ]; then
  echo "找不到可執行的 Python：$PY（設 RECSYS_PYTHON=<python 路徑> 後重跑）" >&2
  exit 1
fi
# 相對路徑版的 spark-defaults.conf：warehouse／metastore 落在目前目錄（examples/ad/data/）
export SPARK_CONF_DIR="$ROOT/conf/spark-local"
# 絕對路徑：在 worktree 跑時要載到該 worktree 的 src，不是 venv editable install 指的 main
export PYTHONPATH="$ROOT/src"
# spark.sql.session.timeZone 是 Asia/Taipei；Python 端時區不同時，pd.Timestamp 與 DATE
# 欄的比對會差一小時而靜默篩出 0 列（docs/notes/2026-09-06-dataset-pipeline-profiling.md §9.1）
export TZ=Asia/Taipei

COMPARE=0
if [ "${1:-}" = "--compare" ]; then
  COMPARE=1
fi

TIMINGS=()
run() {
  local label="$1"
  shift
  echo
  echo "▶ $label"
  local t0=$SECONDS
  "$@"
  local dt=$((SECONDS - t0))
  TIMINGS+=("$(printf '%-28s %4ds' "$label" "$dt")")
}

T_START=$SECONDS
run "setup（原始表）"          "$PY" setup_local.py
run "feature_etl"              "$PY" -m recsys_tfb feature_etl --env local
run "label_etl"                "$PY" -m recsys_tfb label_etl --env local
run "sample_pool_etl"          "$PY" -m recsys_tfb sample_pool_etl --env local
run "inference_population_etl" "$PY" -m recsys_tfb inference_population_etl --env local
run "dataset"                  "$PY" -m recsys_tfb dataset --env local
run "training"                 "$PY" -m recsys_tfb training --env local

MODEL_VERSION="$("$PY" - <<'PY'
import sys
from pathlib import Path

from recsys_tfb.core.versioning import find_latest_completed_model_version

found = find_latest_completed_model_version(Path("data/models"))
if found is None:
    sys.exit("data/models 下沒有 status=completed 的模型；training 這一步沒真的產出模型")
print(found[0])
PY
)"
echo "▶ model_version=$MODEL_VERSION"

run "inference"                "$PY" -m recsys_tfb inference --env local --model-version "$MODEL_VERSION"
run "evaluation --post-training" "$PY" -m recsys_tfb evaluation --env local --post-training --model-version "$MODEL_VERSION"

if [ "$COMPARE" = 1 ]; then
  run "digest（比對基準）"     "$PY" digest.py --model-version "$MODEL_VERSION" --out data/digest.json --compare baseline_digest.json
else
  run "digest"                 "$PY" digest.py --model-version "$MODEL_VERSION" --out data/digest.json
fi

echo
echo "⏱ 各步耗時（含每一步的 Spark 冷啟動）"
printf '  %s\n' "${TIMINGS[@]}"
echo "  總計 $((SECONDS - T_START))s"
echo
echo "✅ 廣告情境示例整條跑完：source_etl → dataset → training → inference → evaluation"
