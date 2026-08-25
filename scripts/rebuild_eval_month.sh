#!/usr/bin/env bash
# 重算某個既有 test 月份的評估產物（上游回補 feature_table／label_table 之後用）。
#
# 為什麼要有這個腳本：dataset 與 training 現在都只做「還沒做過」的月份，
# 各自有一個同名的 --rebuild-dates 逃生口。**兩邊都要帶同一個月份**——
# 只重算 dataset 而不重算 predict，該月的預測仍是舊模型輸出對舊資料的結果；
# 只重算 predict 而不重算 dataset，讀到的 model_input 還是回補前的。
# 把兩步包成一條指令，打包好的流程就不可能只做一半。
#
# 用法（從 repo 或 worktree root）：
#   bash scripts/rebuild_eval_month.sh 2026-01-31
#   bash scripts/rebuild_eval_month.sh 2026-01-31,2026-02-28 --env local
#
# 月份必須是 dataset.test_snap_dates 的子集，否則在 Spark 起來之前就會被
# 一致性不變量 A21 擋下（兩個 pipeline 共用同一條 predicate）。
#
# 只「新增」一個月份不需要這個腳本——新月份沒有任何既有產物，
# 兩邊都會自動處理它。完整動線見 docs/operations/user-guides/adding-an-eval-month.md。
set -euo pipefail

MONTHS="${1:-}"
if [[ -z "$MONTHS" || "$MONTHS" == -* ]]; then
  echo "用法：bash scripts/rebuild_eval_month.sh <YYYY-MM-DD[,YYYY-MM-DD...]> [--env <env>]" >&2
  exit 2
fi
shift

ENV_NAME="local"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) ENV_NAME="${2:?--env 需要一個值}"; shift 2 ;;
    *) echo "未知的參數：$1" >&2; exit 2 ;;
  esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# worktree 的 .venv 是指向 main 那份唯一真 venv 的 symlink；PYTHONPATH 釘在
# 這棵樹的 src，否則 editable install 會讓指令跑到 main 的程式碼。
PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
export PYTHONPATH="$ROOT/src"
if [[ "$ENV_NAME" == "local" && -z "${SPARK_CONF_DIR:-}" ]]; then
  export SPARK_CONF_DIR="$ROOT/conf/spark-local"
  echo "ℹ️  SPARK_CONF_DIR 未設，指向 $SPARK_CONF_DIR"
fi

run() { echo; echo "▶ $*"; "$@"; }

# 1) dataset：重算該月的 preprocessed_feature_table / test_keys / test_model_input
#    --only-test-months：重算一個既有月份要跑的 node 集，跟新增一個月份完全
#    相同——差別只在 month plan 把哪些月放進 to_process。train/val/calibration
#    不在其中，它們的內容與 test 月份無關。
#
#    這裡有一層自癒因此消失了，記在這裡而不是留給人發現：本腳本原本跑完整
#    dataset，順帶把 train/val/calibration 在當前 variant 底下重建一次，於是會
#    意外修好「同一次編輯既加了 test 月、又改了 sample_ratio」造成的 variant
#    漂移（ADR-0012「這個論證的已知反例」）。**沒有東西補上它。** 仍然接受：
#    那層自癒從來不是這個腳本的職責、沒有任何文件宣稱過，而移除之後的曝險與
#    --only-test-months 主動線完全相同。
run "$PYTHON" -m recsys_tfb dataset --env "$ENV_NAME" \
  --only-test-months --rebuild-dates "$MONTHS"

# 2) training：只跑 predict 切片（不重訓；模型從既有 model_version 讀）。
#    同一個旗標在這裡做兩件事：丟掉該月的本機 parquet cache（命中只看
#    _SUCCESS、不看新鮮度），以及對該月重新預測（否則它的 partition 已完整、
#    會被跳過）。
run "$PYTHON" -m recsys_tfb training --env "$ENV_NAME" \
  --only-node predict_and_write_test_predictions --rebuild-dates "$MONTHS"

cat <<EOF

✅ dataset ＋ predict 已對 $MONTHS 重算完成。
   驗收：上面兩段 log 各有一行
     [months] test branch: processed=$MONTHS ...
     [months] predict: processed=$MONTHS ... rebuilt=$MONTHS

下一步（evaluation 沒有對應的 CLI 旗標，逐月手動）：
   1. conf/base/parameters_evaluation.yaml 把 evaluation.snap_date 指到該月
   2. $PYTHON -m recsys_tfb evaluation --env $ENV_NAME --post-training --model-version <model_version>
EOF
