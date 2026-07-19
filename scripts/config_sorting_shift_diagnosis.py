"""Manual diagnosis: does sampling/weight config cause ranking shifts?

This is a standalone script for the redesign spike. It intentionally avoids
the old reconciliation gap/residual chain and answers one narrow ranking
question:

    If we subtract the theoretical sampling/weight offset from raw model
    log-odds scores, does macro per-item mAP improve?

Recommended source data is an already enriched evaluation table with one row
per (time, entity, item), label, score_uncalibrated, and every non-item/non-label
column used by dataset.sample_group_keys or training.sample_weight_keys.

Examples:

  PYTHONPATH=src python scripts/config_sorting_shift_diagnosis.py \
      --model-version 20260717_xxx \
      --output data/diagnosis/config_sorting_shift.html
"""

from __future__ import annotations

import argparse
import html
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import to_logit
from recsys_tfb.evaluation.metrics import (
    compute_macro_per_item_map,
    macro_from_per_item,
    positive_row_contributions,
)

ENRICHED_EVAL_ENTRY = "enriched_eval_predictions"
SCORE_COL = "score_uncalibrated"


def _deep_merge(a: dict, b: dict) -> dict:
    out = dict(a)
    for k, v in (b or {}).items():
        if isinstance(out.get(k), dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_parameters(paths: list[str] | None) -> dict:
    if not paths:
        base = Path("conf/base")
        paths = [
            str(base / name)
            for name in [
                "parameters.yaml",
                "parameters_dataset.yaml",
                "parameters_training.yaml",
                "parameters_evaluation.yaml",
            ]
            if (base / name).exists()
        ]
    params: dict[str, Any] = {}
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            params = _deep_merge(params, yaml.safe_load(f) or {})
    return params


def load_catalog(path: str | None) -> dict:
    catalog_path = Path(path or "conf/base/catalog.yaml")
    with open(catalog_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def resolve_enriched_eval_table(catalog: dict, parameters: dict) -> tuple[str, dict]:
    entry = catalog.get(ENRICHED_EVAL_ENTRY)
    if not isinstance(entry, dict):
        raise ValueError(f"Catalog entry {ENRICHED_EVAL_ENTRY!r} not found.")
    if entry.get("type") != "HiveTableDataset":
        raise ValueError(
            f"Catalog entry {ENRICHED_EVAL_ENTRY!r} is {entry.get('type')!r}, "
            "not HiveTableDataset."
        )
    db = str(entry.get("database") or "")
    if db == "${hive.db}":
        db = str((parameters.get("hive", {}) or {}).get("db") or "")
    table = str(entry.get("table") or "")
    if not db or not table:
        raise ValueError(f"Catalog entry {ENRICHED_EVAL_ENTRY!r} lacks database/table.")
    return f"{db}.{table}", entry


def _key_from_values(keys: list[str], values: dict[str, Any]) -> str:
    return "|".join(str(values[k]) for k in keys)


def _offset_for_values(
    values: dict[str, Any],
    *,
    parameters: dict,
    schema: dict,
) -> float:
    label_col = schema["label"]
    ds = parameters.get("dataset", {}) or {}
    tr = parameters.get("training", {}) or {}

    offset = 0.0

    group_keys = list(ds.get("sample_group_keys", []) or [])
    if label_col in group_keys:
        r_default = float(ds.get("sample_ratio", 1.0))
        overrides = ds.get("sample_ratio_overrides", {}) or {}
        pos_vals = dict(values)
        neg_vals = dict(values)
        pos_vals[label_col] = "1"
        neg_vals[label_col] = "0"
        r_pos = float(overrides.get(_key_from_values(group_keys, pos_vals), r_default))
        r_neg = float(overrides.get(_key_from_values(group_keys, neg_vals), r_default))
        if r_pos <= 0.0 or r_neg <= 0.0:
            raise ValueError(
                f"sample ratio must be positive for offset math; got "
                f"r_pos={r_pos}, r_neg={r_neg}"
            )
        offset += math.log(r_pos / r_neg)

    weight_keys = list(tr.get("sample_weight_keys", []) or [])
    if label_col in weight_keys:
        weights = tr.get("sample_weights", {}) or {}
        pos_vals = dict(values)
        neg_vals = dict(values)
        pos_vals[label_col] = "1"
        neg_vals[label_col] = "0"
        w_pos = float(weights.get(_key_from_values(weight_keys, pos_vals), 1.0))
        w_neg = float(weights.get(_key_from_values(weight_keys, neg_vals), 1.0))
        if w_pos <= 0.0 or w_neg <= 0.0:
            raise ValueError(
                f"sample weights must be positive for offset math; got "
                f"w_pos={w_pos}, w_neg={w_neg}"
            )
        offset += math.log(w_pos / w_neg)

    return float(offset)


def offset_context_columns(parameters: dict, schema: dict) -> list[str]:
    item_col = schema["item"]
    label_col = schema["label"]
    cols: list[str] = []
    for key in (
        list((parameters.get("dataset", {}) or {}).get("sample_group_keys", []) or [])
        + list((parameters.get("training", {}) or {}).get("sample_weight_keys", []) or [])
    ):
        if key not in (item_col, label_col) and key not in cols:
            cols.append(key)
    return cols


def required_columns(parameters: dict, schema: dict) -> list[str]:
    return [
        schema["time"],
        *schema["entity"],
        schema["item"],
        schema["label"],
        SCORE_COL,
        schema["score"],
        *offset_context_columns(parameters, schema),
    ]


def _query_key(pdf: pd.DataFrame, query_cols: list[str]) -> pd.Series:
    parts = [pdf[c].astype(str) for c in query_cols]
    out = parts[0]
    for p in parts[1:]:
        out = out.str.cat(p, sep="|")
    return out


def load_enriched_eval_predictions(
    args: argparse.Namespace,
    parameters: dict,
    schema: dict,
) -> tuple[pd.DataFrame, dict]:
    from pyspark.sql import SparkSession, functions as F
    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample

    catalog = load_catalog(args.catalog)
    hive_table, catalog_meta = resolve_enriched_eval_table(catalog, parameters)
    snap_date = args.snap_date or str(
        (parameters.get("evaluation", {}) or {}).get("snap_date") or ""
    )
    model_version = args.model_version or parameters.get("model_version")
    if not snap_date:
        raise ValueError(
            "evaluation.snap_date is missing. Set it in parameters or pass "
            "--snap-date."
        )
    if not model_version:
        raise ValueError(
            "model_version is required for enriched_eval_predictions. Pass "
            "--model-version."
        )

    spark = (
        SparkSession.builder
        .appName("config_sorting_shift_diagnosis")
        .enableHiveSupport()
        .getOrCreate()
    )
    sdf = spark.table(hive_table)

    time_col = schema["time"]
    if time_col not in sdf.columns:
        raise ValueError(f"{hive_table} is missing time column {time_col!r}.")
    if "model_version" not in sdf.columns:
        raise ValueError(f"{hive_table} is missing partition column 'model_version'.")
    sdf = sdf.filter(F.col(time_col).cast("string") == str(snap_date))
    sdf = sdf.filter(F.col("model_version") == str(model_version))

    needed = [c for c in required_columns(parameters, schema) if c in sdf.columns]
    sdf = sdf.select(*list(dict.fromkeys(needed)))

    # Reuse the existing Spark-side query sampler, but make sure it carries the
    # context columns needed by the offset lookup, not just report segments.
    params_for_sample = dict(parameters)
    eval_cfg = dict(params_for_sample.get("evaluation", {}) or {})
    segs = list(eval_cfg.get("segment_columns", []) or [])
    for c in offset_context_columns(parameters, schema):
        if c not in segs:
            segs.append(c)
    eval_cfg["segment_columns"] = segs
    params_for_sample["evaluation"] = eval_cfg

    sample_pdf, sample_meta = draw_diagnosis_sample(sdf, params_for_sample)
    return sample_pdf, {
        "source": hive_table,
        "mode": "hive_table_sample",
        "catalog_entry": ENRICHED_EVAL_ENTRY,
        "catalog": catalog_meta,
        "snap_date": snap_date,
        "model_version": model_version,
        "sample": sample_meta,
    }


def validate_and_prepare(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
) -> tuple[pd.DataFrame, str, list[str]]:
    notes: list[str] = []
    query_cols = [schema["time"], *schema["entity"]]
    base_required = [*query_cols, schema["item"], schema["label"]]

    missing = [c for c in [*base_required, SCORE_COL] if c not in pdf.columns]
    if missing:
        raise ValueError(f"Input data missing required columns: {missing}")

    context_cols = offset_context_columns(parameters, schema)
    missing_context = [c for c in context_cols if c not in pdf.columns]
    if missing_context:
        raise ValueError(
            "Input data missing offset context columns "
            f"{missing_context}. Best source is enriched_eval_predictions, or "
            "join these columns before running the script."
        )

    keep = [*base_required, SCORE_COL, *context_cols]
    out = pdf[keep].copy()
    out[schema["label"]] = out[schema["label"]].astype(int)
    out[schema["item"]] = out[schema["item"]].astype(str)
    return out, SCORE_COL, notes


def build_offset_frame(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
) -> tuple[pd.DataFrame, dict]:
    item_col = schema["item"]
    label_col = schema["label"]
    a_cols = offset_context_columns(parameters, schema)

    items = (
        list((schema.get("categorical_values", {}) or {}).get(item_col, []) or [])
        or sorted(pdf[item_col].astype(str).unique().tolist())
    )
    if a_cols:
        groups_pdf = pdf[a_cols].drop_duplicates().sort_values(a_cols)
        contexts = [tuple(row) for row in groups_pdf.to_numpy()]
    else:
        contexts = [tuple()]

    rows: list[dict[str, Any]] = []
    for ctx in contexts:
        a_values = dict(zip(a_cols, ctx))
        offsets: list[float] = []
        row_offsets: dict[str, float] = {}
        for item in items:
            vals = dict(a_values)
            vals[item_col] = item
            vals[label_col] = "0"
            off = _offset_for_values(vals, parameters=parameters, schema=schema)
            offsets.append(off)
            row_offsets[item] = off
        median = float(np.median(offsets)) if offsets else 0.0
        spread = float(max(offsets) - min(offsets)) if offsets else 0.0
        group_label = "ALL" if not a_cols else " | ".join(str(v) for v in ctx)
        for item in items:
            rows.append({
                "group": group_label,
                **a_values,
                item_col: item,
                "offset": row_offsets[item],
                "offset_centered": row_offsets[item] - median,
                "spread": spread,
            })

    offset_df = pd.DataFrame(rows)
    spread_rows = (
        offset_df[["group", "spread"]]
        .drop_duplicates()
        .sort_values(["spread", "group"], ascending=[False, True])
    )
    meta = {
        "context_columns": a_cols,
        "items": items,
        "n_contexts": int(len(contexts)),
        "max_spread": float(spread_rows["spread"].max()) if len(spread_rows) else 0.0,
        "has_sorting_effect_by_config": bool(
            len(spread_rows) and spread_rows["spread"].max() > 1e-12
        ),
        "spread_by_group": spread_rows.to_dict(orient="records"),
    }
    return offset_df, meta


def row_offsets(pdf: pd.DataFrame, parameters: dict, schema: dict) -> np.ndarray:
    item_col = schema["item"]
    label_col = schema["label"]
    a_cols = offset_context_columns(parameters, schema)
    cache: dict[tuple, float] = {}
    out = np.zeros(len(pdf), dtype=np.float64)
    for i, row in enumerate(pdf[[*a_cols, item_col]].itertuples(index=False, name=None)):
        ctx = tuple(row)
        if ctx not in cache:
            vals = dict(zip([*a_cols, item_col], ctx))
            vals[label_col] = "0"
            cache[ctx] = _offset_for_values(vals, parameters=parameters, schema=schema)
        out[i] = cache[ctx]
    return out


def metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    return {
        "k": None if m.get("k") is None else int(m["k"]),
        "weight_alpha": float(m.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(m.get("min_positives", 0) or 0),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0) or 0.0),
    }


def _bootstrap_macro_values(
    groups: np.ndarray,
    clusters: np.ndarray,
    items: np.ndarray,
    y: np.ndarray,
    score: np.ndarray,
    draws: np.ndarray,
    mp: dict,
) -> np.ndarray:
    contrib, row_idx = positive_row_contributions(groups, y, score, mp["k"])
    if len(contrib) == 0:
        return np.full(draws.shape[0], np.nan)
    item_of = items[row_idx]
    cluster_of = clusters[row_idx]
    uniq_items, item_inv = np.unique(item_of, return_inverse=True)
    n_items = len(uniq_items)
    out = np.full(draws.shape[0], np.nan)
    n_clusters = int(clusters.max()) + 1 if len(clusters) else 0
    for b, draw in enumerate(draws):
        mult = np.bincount(draw, minlength=n_clusters).astype(np.float64)
        w = mult[cluster_of]
        sums = np.bincount(item_inv, weights=contrib * w, minlength=n_items)
        counts = np.bincount(item_inv, weights=w, minlength=n_items)
        present = counts > 0
        if not present.any():
            continue
        vals = np.divide(
            sums,
            counts,
            out=np.full(n_items, np.nan),
            where=present,
        )
        out[b] = macro_from_per_item(
            vals[present],
            counts[present],
            weight_alpha=mp["weight_alpha"],
            min_positives=mp["min_positives"],
            shrinkage_k=mp["shrinkage_k"],
        )
    return out


def run_diagnosis(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
    score_col: str,
    n_boot: int,
    seed: int,
) -> dict:
    query_cols = [schema["time"], *schema["entity"]]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    mp = metric_params(parameters)

    groups = pd.factorize(_query_key(pdf, query_cols))[0]
    clusters = pd.factorize(pdf[entity_cols].astype(str).agg("|".join, axis=1))[0]
    items = pdf[item_col].astype(str).to_numpy()
    y = pdf[label_col].to_numpy(dtype=np.int64)
    z, logit_notes = to_logit(pdf[score_col].to_numpy(dtype=np.float64))
    offs = row_offsets(pdf, parameters, schema)
    z_corrected = z - offs

    baseline = float(compute_macro_per_item_map(groups, items, y, z, **mp))
    corrected = float(compute_macro_per_item_map(groups, items, y, z_corrected, **mp))
    delta = corrected - baseline

    rng = np.random.RandomState(seed)
    n_clusters = int(clusters.max()) + 1 if len(clusters) else 0
    draws = rng.randint(0, n_clusters, size=(n_boot, n_clusters)) if n_clusters else np.empty((0, 0), dtype=int)
    boot_base = _bootstrap_macro_values(groups, clusters, items, y, z, draws, mp)
    boot_corr = _bootstrap_macro_values(groups, clusters, items, y, z_corrected, draws, mp)
    boot_delta = boot_corr - boot_base

    per_item_rows: list[dict[str, Any]] = []
    n_pos_by_item = pd.Series(y, index=pdf.index).groupby(pdf[item_col].astype(str)).sum()
    for item in sorted(pd.unique(pdf[item_col].astype(str))):
        mask = items == item
        z_one = z.copy()
        z_one[mask] = z_one[mask] - offs[mask]
        m_one = float(compute_macro_per_item_map(groups, items, y, z_one, **mp))
        per_item_rows.append({
            "item": item,
            "delta_j": m_one - baseline,
            "map_after_only_this_item": m_one,
            "n_pos": int(n_pos_by_item.get(item, 0)),
            "offset_min": float(np.min(offs[mask])) if mask.any() else None,
            "offset_max": float(np.max(offs[mask])) if mask.any() else None,
        })
    per_item_rows.sort(key=lambda r: r["delta_j"], reverse=True)

    return {
        "metric_params": mp,
        "score_col_used": score_col,
        "logit_notes": logit_notes,
        "n_rows": int(len(pdf)),
        "n_queries": int(len(np.unique(groups))),
        "n_entities": int(n_clusters),
        "n_items": int(len(set(items.tolist()))),
        "n_positive_rows": int(y.sum()),
        "baseline_map": baseline,
        "corrected_map": corrected,
        "delta": delta,
        "delta_ci_low": None if len(boot_delta) == 0 else float(np.nanpercentile(boot_delta, 2.5)),
        "delta_ci_high": None if len(boot_delta) == 0 else float(np.nanpercentile(boot_delta, 97.5)),
        "n_boot": int(n_boot),
        "bootstrap_seed": int(seed),
        "per_item": per_item_rows,
    }


def fmt_num(x: Any, digits: int = 4) -> str:
    if x is None:
        return ""
    try:
        if not np.isfinite(float(x)):
            return ""
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)


def table_html(rows: list[dict], columns: list[tuple[str, str]], limit: int | None = None) -> str:
    shown = rows[:limit] if limit is not None else rows
    head = "".join(f"<th>{html.escape(label)}</th>" for key, label in columns)
    body = []
    for r in shown:
        tds = []
        for key, _ in columns:
            val = r.get(key)
            text = fmt_num(val) if isinstance(val, (int, float, np.floating)) else str(val)
            tds.append(f"<td>{html.escape(text)}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def interpretation(result: dict, spread_meta: dict) -> str:
    if not spread_meta["has_sorting_effect_by_config"]:
        return (
            "配置在目前觀察到的 group/item 上沒有 query 內 item 間 offset 差異；"
            "理論上不會造成排序偏移。"
        )
    lo = result["delta_ci_low"]
    hi = result["delta_ci_high"]
    delta = result["delta"]
    if lo is not None and lo > 0:
        return "扣回理論 offset 後 mAP 顯著上升；有證據顯示配置正在造成有害排序偏移。"
    if hi is not None and hi < 0:
        return "扣回理論 offset 後 mAP 顯著下降；這組配置目前反而有利於 macro mAP，不建議直接扣回。"
    if abs(delta) < 1e-12:
        return "扣回理論 offset 後 mAP 沒有變化；目前沒有配置傷害排序的證據。"
    return "CI 跨 0；目前沒有足夠證據說配置正在穩定傷害或幫助排序。"


def render_html(report: dict) -> str:
    result = report["result"]
    spread = report["offset_spread"]
    item_col = report["schema"]["item"]
    interp = interpretation(result, spread)
    spread_rows = spread["spread_by_group"]
    per_item = result["per_item"]
    matrix_rows = report["offset_matrix_preview"]

    css = """
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;color:#1f2933;background:#fbfcfe}
    h1{font-size:28px;margin-bottom:4px} h2{margin-top:32px;font-size:20px}
    .muted{color:#697386}.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin:20px 0}
    .card{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:14px}
    .big{font-size:24px;font-weight:700}.good{color:#0f766e}.bad{color:#b42318}.warn{color:#9a6700}
    table{border-collapse:collapse;width:100%;background:#fff;margin:12px 0;border:1px solid #d9e2ec}
    th,td{padding:8px 10px;border-bottom:1px solid #edf2f7;text-align:right;font-size:13px}
    th:first-child,td:first-child{text-align:left} th{background:#f4f7fb;color:#344054}
    code{background:#eef2f7;padding:1px 4px;border-radius:4px}.note{background:#fff8e6;border:1px solid #f6d365;border-radius:8px;padding:12px}
    """
    summary_cards = [
        ("baseline mAP", fmt_num(result["baseline_map"])),
        ("mAP after F - offset", fmt_num(result["corrected_map"])),
        ("Delta", fmt_num(result["delta"])),
        ("95% paired CI", f"[{fmt_num(result['delta_ci_low'])}, {fmt_num(result['delta_ci_high'])}]"),
        ("max offset spread", fmt_num(spread["max_spread"])),
        ("rows / queries", f"{result['n_rows']} / {result['n_queries']}"),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='muted'>{html.escape(k)}</div><div class='big'>{html.escape(v)}</div></div>"
        for k, v in summary_cards
    )
    notes = report.get("notes", []) + result.get("logit_notes", [])
    notes_html = "".join(f"<li>{html.escape(n)}</li>" for n in notes)
    source = report["source"]

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Config Sorting Shift Diagnosis</title><style>{css}</style></head>
<body>
<h1>Config Sorting Shift Diagnosis</h1>
<p class="muted">Question: is sampling/weight config causing ranking shifts for macro per-item mAP?</p>
<div class="summary">{cards_html}</div>
<div class="note"><strong>Conclusion.</strong> {html.escape(interp)}</div>

<h2>Input</h2>
<table><tbody>
<tr><th>source</th><td>{html.escape(str(source.get("source")))}</td></tr>
<tr><th>catalog entry</th><td><code>{html.escape(str(source.get("catalog_entry")))}</code></td></tr>
<tr><th>score column</th><td><code>{html.escape(result["score_col_used"])}</code></td></tr>
<tr><th>context columns</th><td><code>{html.escape(", ".join(spread["context_columns"]) or "none")}</code></td></tr>
<tr><th>bootstrap</th><td>{result["n_boot"]} paired entity bootstrap draws, seed={result["bootstrap_seed"]}</td></tr>
</tbody></table>
<p class="muted">Recommended data source: <code>enriched_eval_predictions</code> filtered to one snap_date and model_version. It must contain label, score_uncalibrated, query columns, item column, and all context columns above.</p>
{"<ul>" + notes_html + "</ul>" if notes_html else ""}

<h2>1. Config Spread By Query Context</h2>
<p>Only within-query item differences matter. If every group has spread 0, config cannot change ranking.</p>
{table_html(spread_rows, [("group","group"),("spread","offset spread")])}

<h2>2. mAP Impact</h2>
<p>Compute <code>F = logit(score_uncalibrated)</code>, then compare current mAP with <code>mAP(F - offset)</code>.</p>
<table><tbody>
<tr><th>baseline mAP</th><td>{fmt_num(result["baseline_map"])}</td></tr>
<tr><th>corrected mAP</th><td>{fmt_num(result["corrected_map"])}</td></tr>
<tr><th>Delta</th><td>{fmt_num(result["delta"])}</td></tr>
<tr><th>95% CI</th><td>[{fmt_num(result["delta_ci_low"])}, {fmt_num(result["delta_ci_high"])}]</td></tr>
</tbody></table>

<h2>3. Per-Item Replacement</h2>
<p>Each row subtracts offset only for that item. These values are interventions, not an additive decomposition.</p>
{table_html(per_item, [("item","item"),("delta_j","Delta_j"),("map_after_only_this_item","mAP after only this item"),("n_pos","n_pos"),("offset_min","offset min"),("offset_max","offset max")])}

<h2>4. Offset Matrix Preview</h2>
<p>Centered offset is display-only: offset minus the group median. Delta above uses raw row-level offset.</p>
{table_html(matrix_rows, [("group","group"),(item_col,"item"),("offset","offset"),("offset_centered","centered"),("spread","group spread")], limit=300)}
</body></html>"""


def write_outputs(report: dict, output: str) -> None:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_html(report), encoding="utf-8")
    json_path = out.with_suffix(".json")
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    return str(obj)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--catalog", default="conf/base/catalog.yaml", help="Catalog YAML path.")
    p.add_argument("--params", action="append", help="YAML parameter file. Repeatable. Defaults to conf/base core parameter files.")
    p.add_argument("--snap-date", help="Override evaluation.snap_date.")
    p.add_argument("--model-version", required=True, help="model_version partition to read.")
    p.add_argument("--output", default="data/diagnosis/config_sorting_shift.html")
    p.add_argument("--n-boot", type=int, default=200)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    parameters = load_parameters(args.params)
    schema = get_schema(parameters)
    pdf_raw, source_meta = load_enriched_eval_predictions(args, parameters, schema)
    pdf, score_col, notes = validate_and_prepare(pdf_raw, parameters, schema)
    offset_df, spread_meta = build_offset_frame(pdf, parameters, schema)
    result = run_diagnosis(pdf, parameters, schema, score_col, args.n_boot, args.seed)
    report = {
        "schema": {"item": schema["item"]},
        "source": source_meta,
        "notes": notes,
        "offset_spread": spread_meta,
        "offset_matrix_preview": offset_df.head(1000).to_dict(orient="records"),
        "result": result,
    }
    write_outputs(report, args.output)
    print(f"Wrote {args.output}")
    print(f"Wrote {Path(args.output).with_suffix('.json')}")
    print(f"Delta = {result['delta']:.6f} [{result['delta_ci_low']:.6f}, {result['delta_ci_high']:.6f}]")


if __name__ == "__main__":
    main()
