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

# 兩件事一起斷言，因為兩者都只有實跑看得到、且都不會有錯誤訊息：
#
#   1. item 分區目錄名是產品名而非整數 code（#185／ADR-0010 §6）。
#   2. 實體分區順序是 model_version → snap_date → prod_name（#187／ADR-0010 §5）。
#      `partition_filter` 的鍵排在 `partition_cols` 之前，而 CREATE TABLE IF NOT
#      EXISTS 不改既有表、insertInto 是位置對應——沒 DROP 就套新設定的話，
#      model_version 的值會落進 snap_date 目錄，三個欄都是 STRING，型別檢查擋不住。
#      所以這裡比對的是「最外層目錄叫 model_version= 且值等於這次跑的版本」。
#   3. entity_bucket 只在 unranked_predictions 上，不在對外兩張表上（#188／
#      ADR-0010 §5）。它是為了「一次 save 恰好一個分區」而存在的機制欄；漏進對外
#      契約的話所有下游讀取路徑都要跟著改，而且不可逆。這條也只有看目錄名才知道。
#   4. unranked_predictions 的分區數 = item 數 × 有資料的桶數。合成母體只有幾十個
#      客戶、桶數預設 10，所以有些桶是空的（insertInto 對空 frame 不建分區）——
#      斷言寫成「桶數 ≤ 設定值、且每個有資料的桶都有全部 item」，不是寫死乘積。
#
# 期望值從 conf 與這次的 MODEL_VERSION 讀、不是 hard-code：hard-code 的清單會跟著
# conf 一起漂，而這條 assert 要在「編碼值取錯清單」時仍然會紅。走 ConfigLoader 而
# 不是直接讀 conf/base/*.yaml——pipeline 跑的是 --env local，要比對的就是它實際看到
# 的那份值（今天 conf/local/ 不存在，兩者相同；哪天存在了，直接讀 base 會靜默比錯對象）。
echo
echo "▶ assert：三張推論表的分區結構（item 是產品名、model_version 在最外層、entity_bucket 只在內部表）"
"$VENV" - "$MODEL_VERSION" <<'PY'
import sys
from pathlib import Path

from recsys_tfb.core.config import ConfigLoader

expected_mv = sys.argv[1]
params = ConfigLoader("conf", env="local").get_parameters()
products = set(params["inference"]["products"])
n_buckets = int(params["inference"].get("entity_buckets", 10))
db = Path("data/local_warehouse/ml_recsys.db")

failures = []
for table in ("unranked_predictions", "ranked_staging", "ranked_predictions"):
    leaves = sorted(
        db.glob(f"{table}/model_version=*/snap_date=*/prod_name=*")
    )
    if not leaves:
        failures.append(
            f"{table}: 找不到 model_version=*/snap_date=*/prod_name=* 分區目錄"
            f"（既有表沒 DROP 就套新 catalog 設定會長成這樣）"
        )
        continue
    products_seen = {p.name.split("=", 1)[1] for p in leaves}
    versions_seen = {p.parent.parent.name.split("=", 1)[1] for p in leaves}
    if products_seen != products:
        failures.append(
            f"{table}: prod_name 分區值 {sorted(products_seen)} != inference.products"
        )
    elif versions_seen != {expected_mv}:
        failures.append(
            f"{table}: 最外層 model_version 分區值 {sorted(versions_seen)} "
            f"!= 這次跑的 {expected_mv}（分區欄錯位的徵兆）"
        )
    else:
        print(
            f"  ✓ {table}: model_version={expected_mv}，"
            f"{len(products_seen)} 個 prod_name 分區，例：{sorted(products_seen)[0]}"
        )

# entity_bucket：只該在 unranked_predictions 上
buckets = sorted(db.glob("unranked_predictions/model_version=*/snap_date=*/prod_name=*/entity_bucket=*"))
if not buckets:
    failures.append(
        "unranked_predictions: 找不到 entity_bucket=* 分區目錄。"
        "少了這一欄，後一個桶的 save 會用 dynamic overwrite 刪掉前一個桶的列"
        "（ADR-0010 §3 約束 C），而且不會有錯誤訊息"
    )
else:
    bucket_values = {int(p.name.split("=", 1)[1]) for p in buckets}
    out_of_range = sorted(b for b in bucket_values if not 0 <= b < n_buckets)
    if out_of_range:
        failures.append(
            f"unranked_predictions: entity_bucket 值 {out_of_range} 落在 "
            f"[0, {n_buckets}) 之外（inference.entity_buckets 改過而舊分區沒清？）"
        )
    # 每個有資料的桶都該有全部 item：桶是按 entity 切的，切分與 item 無關
    per_bucket = {}
    for leaf in buckets:
        b = int(leaf.name.split("=", 1)[1])
        per_bucket.setdefault(b, set()).add(leaf.parent.name.split("=", 1)[1])
    ragged = {b: sorted(items) for b, items in per_bucket.items() if items != products}
    if ragged:
        failures.append(
            f"unranked_predictions: 這些桶的 item 分區不完整 {ragged}"
            f"（期望每個桶都有 {len(products)} 個）"
        )
    if not failures:
        print(
            f"  ✓ unranked_predictions: {len(bucket_values)}/{n_buckets} 個桶有資料"
            f"（母體小於桶數時空桶是正常的），每桶 {len(products)} 個 item 分區，"
            f"共 {len(buckets)} 個分區"
        )

for table in ("ranked_staging", "ranked_predictions"):
    leaked = sorted(db.glob(f"{table}/**/entity_bucket=*"))
    if leaked:
        failures.append(
            f"{table}: 出現 entity_bucket 分區（{len(leaked)} 個）。它是純計算層的"
            f"機制欄，進了對外表就等於進了下游契約，而且不可逆（ADR-0010 §5）"
        )
if not any("entity_bucket 分區（" in f for f in failures):
    print("  ✓ ranked_staging／ranked_predictions 都沒有 entity_bucket 分區（機制欄沒漏進對外契約）")

if failures:
    print("\n".join("  ✗ " + f for f in failures), file=sys.stderr)
    sys.exit(1)
PY

echo
echo "✅ local e2e（環境證明）完成：dataset→training→inference 全 local[*]，"
echo "   含 training cache file:// 複製，以及推論表分區結構的實跑斷言。"
