#!/usr/bin/env bash
# 廣告情境示例的端到端實跑，全 local[*]：
#   原始表 → source_etl 四條（feature_etl 後比對特徵不偷看）→ dataset → training
#   → inference（預期在入口被 A47 擋下）→ evaluation --post-training → digest
#
# 這份 conf 宣告了候選層級特徵表（catalog 的 candidate_feature_table ＝ feature_realtime，
# ADR-0026），所以離線推論不會跑：推論只讀 feature_table，模型卻需要那張表的欄，CLI 在入口
# 以 A47 擋下（不擋的話會在 Spark 起來之後才以 Missing feature columns 失敗）。這裡確認它真的
# 在入口停下、而且訊息說得出原因。
#
# 對應銀行示例的 scripts/local_e2e.sh，多了 source_etl 與 evaluation 兩段：銀行示例直接
# 寫出來源表、跳過 source_etl，而這個示例要讓後面幾張票的新路徑（event、候選層級特徵表、
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
# 框架不檢查特徵有沒有偷看（ADR-0022 決定 3），這個示例自己比：SQL 的值與照定義重算的值逐列相同
run "check 特徵不偷看"          "$PY" check_features.py
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

expect_inference_blocked() {
  local out
  if out="$("$PY" -m recsys_tfb inference --env local --model-version "$MODEL_VERSION" 2>&1)"; then
    echo "$out" | tail -20
    echo "inference 沒有被擋下：宣告了 candidate_feature_table 時應該在入口停下（A47）" >&2
    return 1
  fi
  if ! grep -q "(A47)" <<<"$out"; then
    echo "$out" | tail -20
    echo "inference 失敗了，但不是 A47 擋的" >&2
    return 1
  fi
  echo "  ✓ inference 在入口被 A47 擋下，沒有啟動 Spark 工作"
}
run "inference（預期被 A47 擋下）" expect_inference_blocked

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
echo "✅ 廣告情境示例整條跑完：source_etl → dataset → training → evaluation（inference 如預期被 A47 擋下）"
