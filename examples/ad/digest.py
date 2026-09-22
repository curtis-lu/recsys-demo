"""廣告情境示例的 digest：每一層產物的版本號、列數與內容指紋。

後面每張票（event 角色、多張特徵表、item 清單從資料數、預測品質指標……）都先在
「還沒打開自己的開關」時跑一次 ``run_e2e.sh --compare``，拿這份跟 baseline_digest.json
比：一樣＝框架改動沒碰壞廣告情境；不一樣時，分層的結果直接指出從哪一層開始變。

分層記錄而不是整條一個雜湊，理由與規劃檔（docs/notes/2026-09-16-event-support-plan.md）
把 event／item 清單／多張特徵表拆成三張票相同：「合在一起，任何一個版本號變了都分不出
是誰造成的」。source_etl 一樣、dataset 不一樣，問題就在 dataset，不必從頭查。

內容指紋的算法：每列對所有欄（依欄名排序）取 xxhash64，整張表加總。與列的順序無關，
也不需要把資料拉回 driver。版本分區欄（base_dataset_version 等）不進指紋——版本號
另外記，這樣「內容一樣、只有版本號變了」與「內容變了」分得開。模型分數四捨五入到
小數第 6 位再進指紋，避免浮點尾數在不同機器上抖動造成假警報。

evaluation 的 JSON 產物全部攤平後進指紋，但**排除 ``config_fingerprint``**：它是設定
的雜湊，框架新增一個「算的」設定鍵就會變（ADR-0024 的預測品質指標就會），指標值卻
一個都沒動。放進來的話，那種改動會讓這裡紅，而「指標值逐值不變」反而驗不出來。

有些欄位同一份程式碼跑兩次本來就不同（例如帶時間戳的欄位）；那些列在
baseline_digest.json 的 ``noisy``，比對時略過並印出來，不當作差異。

資料庫名與表名從 conf 讀（``hive.db``、catalog 各條目的 ``table``），不寫死。

用法（在 examples/ad/ 底下，由 run_e2e.sh 呼叫）：
    python digest.py --model-version <mv> --out data/digest.json [--compare baseline_digest.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

VERSION_COLS = ("base_dataset_version", "train_variant_id", "model_version")
SCORE_COLS = ("score", "score_uncalibrated")
EXCLUDED_JSON_KEYS = ("config_fingerprint",)

# catalog 條目名。candidate_feature_table 指到 feature_etl 寫的 feature_realtime（ADR-0026）。
SOURCE_TABLES = (
    "feature_table", "candidate_feature_table", "label_table", "sample_pool",
    "inference_population",
)
DATASET_TABLES = (
    "train_model_input", "train_dev_model_input",
    "val_model_input", "test_model_input",
)
DATASET_JSON = ("preprocessor.json", "category_mappings.json")


class Tables:
    """catalog 條目名 → 完整 Hive 表名，讀自 conf/（與 pipeline 看到的同一份）。"""

    def __init__(self) -> None:
        from recsys_tfb.core.config import ConfigLoader

        config = ConfigLoader("conf", env="local")
        params = config.get_parameters()
        self._db = params["hive"]["db"]
        self._catalog = config.get_catalog_config()

    def __getitem__(self, entry: str) -> str:
        return f"{self._db}.{self._catalog[entry]['table']}"


def table_fingerprint(spark, table: str, where: str | None = None) -> dict:
    from pyspark.sql import functions as F

    df = spark.table(table)
    if where:
        df = df.where(where)
    cols = sorted(c for c in df.columns if c not in VERSION_COLS)
    exprs = [F.round(F.col(c), 6) if c in SCORE_COLS else F.col(c) for c in cols]
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(*exprs).cast("decimal(38,0)")).alias("hash"),
    ).collect()[0]
    return {"rows": int(row["rows"]), "hash": str(row["hash"]), "columns": cols}


def single_value(spark, table: str, col: str) -> str:
    values = [r[0] for r in spark.table(table).select(col).distinct().collect()]
    if len(values) != 1:
        sys.exit(f"{table}.{col} 有 {len(values)} 個值 {values}：data/ 裡混了別輪的產物，先跑 setup_local.py 清掉")
    return str(values[0])


def json_leaves(obj, prefix: str = "") -> dict:
    """把 JSON 攤平成 路徑 → 值；數字四捨五入到小數第 6 位；略過 EXCLUDED_JSON_KEYS。"""
    out = {}
    if isinstance(obj, dict):
        for k in sorted(obj):
            if k in EXCLUDED_JSON_KEYS:
                continue
            out.update(json_leaves(obj[k], f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(json_leaves(v, f"{prefix}[{i}]"))
    elif isinstance(obj, float):
        out[prefix] = round(obj, 6)
    else:
        out[prefix] = obj
    return out


def file_fingerprint(path: Path) -> dict:
    import hashlib

    leaves = json_leaves(json.loads(path.read_text()))
    blob = json.dumps(leaves, sort_keys=True, ensure_ascii=False).encode()
    return {"leaves": len(leaves), "hash": hashlib.sha256(blob).hexdigest()}


def build(model_version: str) -> dict:
    from pyspark.sql import SparkSession

    tables = Tables()
    spark = SparkSession.builder.appName("ad_example_digest").getOrCreate()
    try:
        versions = {
            "base_dataset_version": single_value(spark, tables["val_model_input"], "base_dataset_version"),
            "train_variant_id": single_value(spark, tables["train_model_input"], "train_variant_id"),
            "model_version": model_version,
        }
        mv_filter = f"model_version = '{model_version}'"
        dataset_dir = Path("data/dataset") / versions["base_dataset_version"]
        eval_dirs = sorted((Path("data/evaluation") / model_version).glob("*/metrics.json"))
        if len(eval_dirs) != 1:
            sys.exit(f"data/evaluation/{model_version}/*/metrics.json 應恰好一份，找到 {len(eval_dirs)} 份")
        eval_dir = eval_dirs[0].parent

        return {
            "versions": versions,
            "source_etl": {t: table_fingerprint(spark, tables[t]) for t in SOURCE_TABLES},
            "dataset": {
                **{t: table_fingerprint(spark, tables[t]) for t in DATASET_TABLES},
                **{f: file_fingerprint(dataset_dir / f) for f in DATASET_JSON},
            },
            "training": {
                "training_eval_predictions": table_fingerprint(
                    spark, tables["training_eval_predictions"], mv_filter),
            },
            # 沒有 inference 層：這份 conf 宣告了候選層級特徵表，離線推論在入口被 A47 擋下
            # （ADR-0026 決定 5），沒有 ranked_predictions 可記。
            # metrics／baseline_metrics／segment_columns／report_aggregates 與 diagnosis/ 底下的診斷。
            # manifest.json 不收：它是執行紀錄（created_at、run_id、git_commit），每次跑都不同，
            # 不是計算結果（2026-09-17 連跑兩次，61 個欄位只有它不同）
            "evaluation": {
                str(p.relative_to(eval_dir)): file_fingerprint(p)
                for p in sorted(eval_dir.rglob("*.json")) if p.name != "manifest.json"
            },
        }
    finally:
        spark.stop()


def flatten(d: dict, prefix: str = "") -> dict:
    out = {}
    for k, v in d.items():
        path = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(v, path))
        else:
            out[path] = v
    return out


def compare(current: dict, baseline_path: Path) -> int:
    baseline = json.loads(baseline_path.read_text())
    noisy = baseline.get("noisy", {})
    want = flatten({k: v for k, v in baseline.items() if k not in ("noisy", "measured")})
    got = flatten(current)

    diffs, skipped = [], []
    for path in sorted(set(want) | set(got)):
        if want.get(path) == got.get(path):
            continue
        reason = next((why for prefix, why in noisy.items() if path.startswith(prefix)), None)
        if reason:
            skipped.append(f"{path}（{reason}）")
        else:
            diffs.append(f"{path}: 基準 {want.get(path)!r} → 這次 {got.get(path)!r}")

    for layer in ("versions", "source_etl", "dataset", "training", "inference", "evaluation"):
        n = sum(1 for d in diffs if d.startswith(f"{layer}."))
        print(f"  {'✓' if n == 0 else '✗'} {layer}{'' if n == 0 else f'：{n} 處不同'}")
    for s in skipped:
        print(f"  ～ 略過已知會抖動的欄位：{s}")
    if diffs:
        print("\n與基準不同：", file=sys.stderr)
        for d in diffs:
            print(f"  {d}", file=sys.stderr)
        return 1
    print(f"與 {baseline_path} 一致")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--model-version", required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--compare", type=Path)
    args = ap.parse_args()

    current = build(args.model_version)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(current, indent=2, ensure_ascii=False) + "\n")
    print(f"digest → {args.out.resolve()}")
    if args.compare:
        sys.exit(compare(current, args.compare))


if __name__ == "__main__":
    main()
