"""L2 檢視：booster 全局 feature importance 的關鍵字彙總（split & gain）。

回答的問題：某一族特徵（例如名稱含 bond / aum 的欄位）在模型全局的
split 次數與 gain 佔比是不是特別少——「訊號有進場但沒被用」的第一層證據。

只需要 model.txt（LightGBM booster 檔），不需要 Spark、不讀資料表。

用法（公司環境照抄，換 model_version 即可）：

  python scripts/feature_gain_summary.py \
      --model-file data/models/<model_version>/model.txt \
      --patterns bond,aum,fund,insur,overa \
      --top 30

輸出三段（皆為可截圖的定寬表）：
  [1] 全特徵 top-N（依 gain 排序，含 gain%/split%/累積 gain%）
  [2] 每個 pattern 的彙總（命中數、gain% 合計、split% 合計、最佳名次、零 split 數）
  [3] 每個 pattern 的逐特徵明細（全列，含全局名次）
"""

from __future__ import annotations

import argparse

import lightgbm as lgb
import pandas as pd


def build_importance_table(booster: lgb.Booster) -> pd.DataFrame:
    names = booster.feature_name()
    gain = booster.feature_importance(importance_type="gain").astype(float)
    split = booster.feature_importance(importance_type="split").astype(float)
    df = pd.DataFrame({"feature": names, "gain": gain, "split": split})
    total_gain = df["gain"].sum() or 1.0
    total_split = df["split"].sum() or 1.0
    df["gain_pct"] = df["gain"] / total_gain * 100
    df["split_pct"] = df["split"] / total_split * 100
    df = df.sort_values("gain", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    df["cum_gain_pct"] = df["gain_pct"].cumsum()
    return df


def pattern_rollup(df: pd.DataFrame, patterns: list[str]) -> pd.DataFrame:
    rows = []
    for p in patterns:
        hit = df[df["feature"].str.contains(p, case=False, regex=False)]
        rows.append({
            "pattern": p,
            "n_features": len(hit),
            "gain_pct_sum": hit["gain_pct"].sum(),
            "split_pct_sum": hit["split_pct"].sum(),
            "best_rank": int(hit["rank"].min()) if len(hit) else None,
            "n_zero_split": int((hit["split"] == 0).sum()),
        })
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model-file", required=True, help="model.txt 路徑")
    ap.add_argument("--patterns", default="bond,aum,fund,insur,overa",
                    help="逗號分隔的特徵名關鍵字（大小寫不敏感、子字串比對）")
    ap.add_argument("--top", type=int, default=30, help="全表顯示前 N 名")
    args = ap.parse_args()

    booster = lgb.Booster(model_file=args.model_file)
    patterns = [p.strip() for p in args.patterns.split(",") if p.strip()]
    df = build_importance_table(booster)

    fmt = {"gain": "{:,.1f}".format, "gain_pct": "{:6.2f}".format,
           "split": "{:,.0f}".format, "split_pct": "{:6.2f}".format,
           "cum_gain_pct": "{:6.2f}".format}
    cols = ["rank", "feature", "gain", "gain_pct", "cum_gain_pct", "split", "split_pct"]

    print(f"model_file = {args.model_file}")
    print(f"n_features = {len(df)}  |  n_trees = {booster.num_trees()}")
    print(f"\n[1] 全特徵 gain top-{args.top}")
    print(df.head(args.top)[cols].to_string(index=False, formatters=fmt))

    print(f"\n[2] pattern 彙總（patterns = {patterns}）")
    roll = pattern_rollup(df, patterns)
    print(roll.to_string(index=False, formatters={
        "gain_pct_sum": "{:6.2f}".format, "split_pct_sum": "{:6.2f}".format}))

    print("\n[3] pattern 逐特徵明細")
    for p in patterns:
        hit = df[df["feature"].str.contains(p, case=False, regex=False)]
        print(f"\n-- pattern: {p}（{len(hit)} 個特徵）")
        if len(hit):
            print(hit[cols].to_string(index=False, formatters=fmt))


if __name__ == "__main__":
    main()
