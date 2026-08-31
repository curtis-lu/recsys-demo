"""L4 檢視：目標 item 正例 query 內，壓制者 vs 目標的 SHAP margin 分解彙總。

回答的問題：在「目標 item（如 fund_bond）為正例」的 query 裡，壓制者
（如 fund_mix）的分數比目標高出來的那段 margin，是由哪些特徵貢獻的——
是 item 身分（prod_name 的先驗位移）還是某些客戶特徵？債券相關特徵在
這些個案上有沒有出力？

作法：對評估月 test_model_input 取「目標 item label=1」的 query，抓同
query 的目標列與壓制者列，各自 ``booster.predict(pred_contrib=True)``
（raw/logit 空間＝score_uncalibrated 同一把尺），逐特徵相減後跨 query
彙總。特徵切片走 ``recsys_tfb.io.extract.pdf_to_X``，與生產 predict
路徑逐位元一致。

用法（公司環境用 --hive-table；需 PYTHONPATH=src）：

  PYTHONPATH=src python scripts/shap_margin_summary.py \
      --model-file data/models/<mv>/model.txt \
      --preprocessor-json data/dataset/<dsv>/preprocessor.json \
      --hive-table <db>.recsys_prod_test_model_input \
      --snap-date 2026-01-31 --target fund_bond \
      --suppressors fund_mix,fund_stock --patterns bond,aum

本機驗證改用 --parquet-path 指向 hive 分割目錄（snap_date=*/prod_name=*
的上一層）。輸出皆為可截圖的定寬表。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import yaml


def load_parameters(conf_dir: str) -> dict:
    """deep-merge conf/base/*.yaml（只為 get_schema/pdf_to_X 所需的 schema）。"""
    merged: dict = {}

    def _merge(a: dict, b: dict) -> dict:
        out = dict(a)
        for k, v in (b or {}).items():
            if isinstance(out.get(k), dict) and isinstance(v, dict):
                out[k] = _merge(out[k], v)
            else:
                out[k] = v
        return out

    for p in sorted(Path(conf_dir).glob("parameters*.yaml")):
        merged = _merge(merged, yaml.safe_load(p.read_text()) or {})
    return merged


def load_rows_parquet(path: str, snap_date: str, items: list[str],
                      columns: list[str], time_col: str, item_col: str) -> pd.DataFrame:
    import pyarrow.dataset as pads
    ds = pads.dataset(path, format="parquet", partitioning="hive")
    flt = (pads.field(time_col) == snap_date) & pads.field(item_col).isin(items)
    avail = [c for c in columns if c in ds.schema.names]
    return ds.to_table(filter=flt, columns=avail).to_pandas()


def load_rows_hive(table: str, snap_date: str, items: list[str],
                   columns: list[str], time_col: str, item_col: str) -> pd.DataFrame:
    from recsys_tfb.utils.spark import get_or_create_spark_session
    spark = get_or_create_spark_session({"app_name": "shap-margin-summary"})
    sdf = spark.table(table)
    avail = [c for c in columns if c in sdf.columns]
    quoted = "','".join(items)
    return (
        sdf.filter(f"cast({time_col} as string) = '{snap_date}'")
           .filter(f"{item_col} in ('{quoted}')")
           .select(avail).toPandas()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model-file", required=True, help="model.txt（raw booster；即 score_uncalibrated）")
    ap.add_argument("--preprocessor-json", required=True)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--hive-table", help="<db>.recsys_prod_test_model_input")
    src.add_argument("--parquet-path", help="test_model_input 的 hive 分割目錄")
    ap.add_argument("--conf-dir", default="conf/base")
    ap.add_argument("--snap-date", required=True)
    ap.add_argument("--target", default="fund_bond")
    ap.add_argument("--suppressors", default="fund_mix,fund_stock")
    ap.add_argument("--patterns", default="bond,aum", help="必列出的特徵名關鍵字")
    ap.add_argument("--top", type=int, default=12)
    args = ap.parse_args()

    import json
    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import pdf_to_X

    parameters = load_parameters(args.conf_dir)
    schema = get_schema(parameters)
    time_col, item_col, label_col = schema["time"], schema["item"], schema["label"]
    cust_col = schema["entity"][0]

    preproc = json.load(open(args.preprocessor_json))
    feat_cols = preproc["feature_columns"]
    suppressors = [s.strip() for s in args.suppressors.split(",") if s.strip()]
    items = [args.target] + suppressors
    need_cols = list(dict.fromkeys(
        feat_cols + [cust_col, label_col, time_col, item_col]))

    if args.parquet_path:
        pdf = load_rows_parquet(args.parquet_path, args.snap_date, items,
                                need_cols, time_col, item_col)
    else:
        pdf = load_rows_hive(args.hive_table, args.snap_date, items,
                             need_cols, time_col, item_col)
    if len(pdf) == 0:
        raise SystemExit(f"snap_date={args.snap_date} items={items} 取不到任何列")

    # 目標 item 正例的 query（cust）集合
    tgt_mask = (pdf[item_col].astype(str) == args.target) & (pdf[label_col] == 1)
    pos_custs = set(pdf.loc[tgt_mask, cust_col].astype(str))
    pdf = pdf[pdf[cust_col].astype(str).isin(pos_custs)].reset_index(drop=True)
    print(f"target={args.target} 正例 query 數 = {len(pos_custs)}；取回列數 = {len(pdf)}")

    booster = lgb.Booster(model_file=args.model_file)
    X = pdf_to_X(pdf, preproc, parameters)
    contrib = booster.predict(X, pred_contrib=True)  # (n, F+1)，末欄 bias
    feat_names = list(booster.feature_name())
    assert contrib.shape[1] == len(feat_names) + 1, "contrib 欄數與特徵數不符"

    cdf = pd.DataFrame(contrib[:, :-1], columns=feat_names)
    cdf["_bias"] = contrib[:, -1]
    cdf["_score"] = contrib.sum(axis=1)
    cdf["_cust"] = pdf[cust_col].astype(str).values
    cdf["_item"] = pdf[item_col].astype(str).values
    cdf = cdf.drop_duplicates(subset=["_cust", "_item"], keep="first")
    by_item = {it: cdf[cdf["_item"] == it].set_index("_cust") for it in items}

    tgt = by_item[args.target]
    patterns = [p.strip() for p in args.patterns.split(",") if p.strip()]

    print(f"\n[0] {args.target} 正例列自身的分數組成（mean |contrib| top-10）")
    mean_abs = tgt[feat_names].abs().mean().sort_values(ascending=False)
    print(mean_abs.head(10).to_frame("mean_abs_contrib")
          .to_string(float_format="{:8.4f}".format))

    for sup in suppressors:
        both = tgt.index.intersection(by_item[sup].index)
        if len(both) == 0:
            print(f"\n[!] {sup}: 無共同 query，略過")
            continue
        t, s = tgt.loc[both], by_item[sup].loc[both]
        margin = s["_score"] - t["_score"]
        delta = s[feat_names].sub(t[feat_names])  # 逐特徵 margin 貢獻
        chk = float((delta.sum(axis=1) + (s["_bias"] - t["_bias"]) - margin).abs().max())
        assert chk < 1e-6, f"分解不閉合：{chk}"

        sup_wins = margin > 0
        print(f"\n===== 壓制者 {sup} vs 目標 {args.target}（{len(both)} 個共同 query）=====")
        print(f"壓制者分數較高的 query 佔比 = {sup_wins.mean():.1%}；"
              f"margin 平均 = {margin.mean():+.3f}（logit）、"
              f"壓制情境內平均 = {margin[sup_wins].mean():+.3f}")

        d_win = delta[sup_wins]
        m_win = float(margin[sup_wins].mean()) or 1.0
        mean_d = d_win.mean().sort_values(ascending=False)
        out = pd.DataFrame({
            "mean_delta": mean_d,
            "margin_share_pct": mean_d / m_win * 100,
        })
        print(f"\n[{sup}-A] 壓制情境（margin>0）margin 的特徵分解 top-{args.top}"
              f"（正=幫壓制者、負=幫目標）")
        print(out.head(args.top).to_string(
            formatters={"mean_delta": "{:+8.4f}".format,
                        "margin_share_pct": "{:7.1f}".format}))
        print(f"\n[{sup}-B] 最幫目標的特徵 bottom-5")
        print(out.tail(5).to_string(
            formatters={"mean_delta": "{:+8.4f}".format,
                        "margin_share_pct": "{:7.1f}".format}))

        hit = [f for f in feat_names
               if any(p.lower() in f.lower() for p in patterns)]
        hit = list(dict.fromkeys(hit + [c for c in ("prod_name",) if c in feat_names]))
        if hit:
            print(f"\n[{sup}-C] 指定 pattern {patterns} ＋ prod_name 的明細")
            print(out.loc[[f for f in hit if f in out.index]].to_string(
                formatters={"mean_delta": "{:+8.4f}".format,
                            "margin_share_pct": "{:7.1f}".format}))


if __name__ == "__main__":
    main()
