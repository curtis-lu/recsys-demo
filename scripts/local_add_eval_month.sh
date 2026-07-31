#!/usr/bin/env bash
# 「多看一個月」的動線，包成單一指令（本機 local[*]）。
#
# 四步動線（spec: issue #123）：
#   1. 把新月份加進 dataset.test_snap_dates，並把 evaluation.snap_date 指到它
#      —— 這一步是**你的意圖**，腳本不代你決定，只在開跑前檢查你做了沒有；
#   2. dataset                                    → 產出該月的 test 產物；
#   3. training 的 predict 節點切片（不重訓）      → 對既有模型補出該月 predictions；
#   4. evaluation --post-training --model-version → 產出該月報表。
#
# 為什麼要這支腳本：第 3、4 步的旗標組合每次都得重記（尤其 evaluation 在本機
# 必須同時帶 --post-training 與 --model-version，見 known-pitfalls），而漏掉任何
# 一步的失敗都是**靜默的**——你會拿到一份看起來正常、其實只涵蓋舊月份的報表。
#
# 用法（從 repo/worktree root）：
#   bash scripts/local_add_eval_month.sh              # 目標月份＝設定裡的 evaluation.snap_date
#   bash scripts/local_add_eval_month.sh 2026-02-28   # 明確指定，兩份設定都會被核對
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VENV=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
export SPARK_CONF_DIR="$ROOT/conf/spark-local"
export PYTHONPATH="$ROOT/src"

TARGET="${1:-}"

run() { echo; echo "▶ $*"; "$@"; }

# --- 步驟 1 的檢查：設定真的指向目標月份了嗎 -------------------------------
# 讀的是 ConfigLoader 合併後的設定（跟 CLI 同一條路），不是直接 parse 單一
# base yaml —— 否則有 conf/<env>/ overlay 時會核對到錯的值。
TARGET="$("$VENV" - "$TARGET" <<'PY'
import sys

from recsys_tfb.core.config import ConfigLoader

requested = sys.argv[1]
params = ConfigLoader("conf", env="local").get_parameters()
test_dates = [str(d) for d in (params.get("dataset") or {}).get("test_snap_dates") or []]
eval_date = str((params.get("evaluation") or {}).get("snap_date", ""))
target = requested or eval_date

problems = []
if not target:
    problems.append("  未指定目標月份，且 evaluation.snap_date 也是空的")
if target and target not in test_dates:
    problems.append(
        f"  dataset.test_snap_dates 不含 {target}（目前：{test_dates}）\n"
        f"    → 編輯 conf/base/parameters_dataset.yaml 的 dataset.test_snap_dates"
    )
if target and eval_date != target:
    problems.append(
        f"  evaluation.snap_date 是 {eval_date!r}，不是 {target}\n"
        f"    → 編輯 conf/base/parameters_evaluation.yaml 的 evaluation.snap_date"
    )
if problems:
    sys.stderr.write("設定尚未指向目標月份（動線第 1 步）：\n" + "\n".join(problems) + "\n")
    raise SystemExit(1)

print(target)
PY
)"
# ${...} 一律加大括號：後接全形字元時，裸 $VAR 會把多位元組字元的首位元組
# 吃進變數名，在 set -u 下炸成 "unbound variable"。
echo "目標月份：${TARGET}（dataset.test_snap_dates ✓  evaluation.snap_date ✓）"

# --- 步驟 2：dataset --------------------------------------------------------
run "$VENV" -m recsys_tfb dataset --env local

# --- 步驟 3：training 的 predict 節點切片（不重訓）--------------------------
# --only-node 會自動補跑缺少的**便宜**上游（select_features / cache_test_model_input），
# 既有模型直接從落地讀。若計畫裡出現 [retrain] 警告，代表 model_version 漂移了
# ——那不是本動線該發生的事，看警告訊息裡的 diff 提示。
TRAIN_LOG="$(mktemp -t recsys_add_eval_month_train)"
echo
echo "▶ $VENV -m recsys_tfb training --env local --only-node predict_and_write_test_predictions"
"$VENV" -m recsys_tfb training --env local \
    --only-node predict_and_write_test_predictions 2>&1 | tee "$TRAIN_LOG"

MODEL_VERSION="$(sed -n 's/.*Model version: *\([0-9a-f][0-9a-f]*\).*/\1/p' "$TRAIN_LOG" | head -1)"
if [ -z "$MODEL_VERSION" ]; then
    echo "無法從 training 輸出解析出 model_version（找不到 'Model version:' 行）" >&2
    exit 1
fi
echo
echo "model_version：${MODEL_VERSION}"

# --- 步驟 4：evaluation -----------------------------------------------------
run "$VENV" -m recsys_tfb evaluation --env local \
    --post-training --model-version "$MODEL_VERSION"

SNAP_DIR="${TARGET//-/}"
echo
echo "✅ 完成：${TARGET} 的報表在 data/evaluation/${MODEL_VERSION}/${SNAP_DIR}/（舊月份報表原封不動）"
echo "ℹ️  本機 test cache 的月份分層（目錄名即涵蓋月份）："
find data/recsys_cache -type d -name 'test_windows' -maxdepth 2 -exec ls -1 {} \; 2>/dev/null \
    | sed 's/^/      /' || true
