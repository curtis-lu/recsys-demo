"""Manual diagnosis: can per-item score shifts improve ranking mAP?

This is a standalone script for the redesign spike. It answers one narrow
post-hoc ranking question:

    Without retraining the model, can fixed per-item score shifts improve
    macro per-item mAP on held-out entities?

The script reads the project catalog entry ``enriched_eval_predictions`` so
users may still change the physical Hive table name in ``catalog.yaml``.

Examples:

  PYTHONPATH=src python scripts/per_item_score_shift_diagnosis.py \
      --model-version 20260717_xxx \
      --output data/diagnosis/per_item_score_shift.html
"""

from __future__ import annotations

import argparse
import html
import json
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


def required_columns(schema: dict) -> list[str]:
    return [
        schema["time"],
        *schema["entity"],
        schema["item"],
        schema["label"],
        SCORE_COL,
        schema["score"],
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
        .appName("per_item_score_shift_diagnosis")
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

    needed = [c for c in required_columns(schema) if c in sdf.columns]
    sdf = sdf.select(*list(dict.fromkeys(needed)))

    sample_pdf, sample_meta = draw_diagnosis_sample(sdf, parameters)
    return sample_pdf, {
        "source": hive_table,
        "mode": "hive_table_sample",
        "catalog_entry": ENRICHED_EVAL_ENTRY,
        "catalog": catalog_meta,
        "snap_date": snap_date,
        "model_version": model_version,
        "sample": sample_meta,
    }


def validate_and_prepare(pdf: pd.DataFrame, schema: dict) -> tuple[pd.DataFrame, list[str]]:
    notes: list[str] = []
    query_cols = [schema["time"], *schema["entity"]]
    base_required = [*query_cols, schema["item"], schema["label"]]

    missing = [c for c in [*base_required, SCORE_COL] if c not in pdf.columns]
    if missing:
        raise ValueError(f"Input data missing required columns: {missing}")

    keep = [*base_required, SCORE_COL]
    out = pdf[keep].copy()
    out[schema["label"]] = out[schema["label"]].astype(int)
    out[schema["item"]] = out[schema["item"]].astype(str)
    return out, notes


def metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    return {
        "k": None if m.get("k") is None else int(m["k"]),
        "weight_alpha": float(m.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(m.get("min_positives", 0) or 0),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0) or 0.0),
    }


def split_by_entity(
    pdf: pd.DataFrame,
    schema: dict,
    tune_fraction: float,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    entity_cols = schema["entity"]
    if not 0.0 < tune_fraction < 1.0:
        raise ValueError("--tune-fraction must be between 0 and 1.")
    entity_key = pdf[entity_cols].astype(str).agg("|".join, axis=1)
    hashed = pd.util.hash_pandas_object(entity_key + f"|{seed}", index=False)
    bucket = (hashed.to_numpy(dtype=np.uint64) % np.uint64(10000)).astype(np.int64)
    tune_mask = bucket < int(round(tune_fraction * 10000))
    tune = pdf[tune_mask].copy()
    holdout = pdf[~tune_mask].copy()
    if len(tune) == 0 or len(holdout) == 0:
        raise ValueError(
            "Entity split produced an empty tune or holdout set. Adjust "
            "--tune-fraction or --seed."
        )
    label_col = schema["label"]
    if int(tune[label_col].sum()) == 0 or int(holdout[label_col].sum()) == 0:
        raise ValueError(
            "Entity split produced a tune or holdout set with no positives. "
            "Adjust --tune-fraction or --seed."
        )

    def n_queries(part: pd.DataFrame) -> int:
        return int(part[[schema["time"], *entity_cols]].drop_duplicates().shape[0])

    return tune, holdout, {
        "split_unit": "entity",
        "tune_fraction": float(tune_fraction),
        "seed": int(seed),
        "n_rows_tune": int(len(tune)),
        "n_rows_holdout": int(len(holdout)),
        "n_queries_tune": n_queries(tune),
        "n_queries_holdout": n_queries(holdout),
    }


def cap_queries(
    pdf: pd.DataFrame,
    schema: dict,
    max_queries: int | None,
    seed: int,
) -> tuple[pd.DataFrame, dict]:
    if max_queries is None:
        return pdf, {"applied": False}
    if max_queries <= 0:
        raise ValueError("--search-max-queries must be positive.")

    query_cols = [schema["time"], *schema["entity"]]
    qkey = _query_key(pdf, query_cols)
    n_queries = int(qkey.nunique())
    if n_queries <= max_queries:
        return pdf, {
            "applied": False,
            "n_queries_before": n_queries,
            "max_queries": int(max_queries),
        }

    q_unique = pd.Series(pd.unique(qkey), name="query_key")
    hashed = pd.util.hash_pandas_object(q_unique + f"|{seed}", index=False)
    keep = set(
        q_unique.iloc[np.argsort(hashed.to_numpy(dtype=np.uint64))[:max_queries]]
        .astype(str)
        .tolist()
    )
    out = pdf[qkey.astype(str).isin(keep)].copy()
    return out, {
        "applied": True,
        "n_queries_before": n_queries,
        "n_queries_after": int(out[query_cols].drop_duplicates().shape[0]),
        "rows_before": int(len(pdf)),
        "rows_after": int(len(out)),
        "max_queries": int(max_queries),
        "seed": int(seed),
    }


def arrays_for_metric(
    pdf: pd.DataFrame,
    schema: dict,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[str]]:
    query_cols = [schema["time"], *schema["entity"]]
    groups = pd.factorize(_query_key(pdf, query_cols))[0]
    items = pdf[schema["item"]].astype(str).to_numpy()
    y = pdf[schema["label"]].to_numpy(dtype=np.int64)
    z, logit_notes = to_logit(pdf[SCORE_COL].to_numpy(dtype=np.float64))
    return groups, items, y, z, logit_notes


def entity_clusters(pdf: pd.DataFrame, schema: dict) -> np.ndarray:
    return pd.factorize(pdf[schema["entity"]].astype(str).agg("|".join, axis=1))[0]


def shifted_score(z: np.ndarray, items: np.ndarray, shifts: dict[str, float]) -> np.ndarray:
    shift_arr = np.array([shifts.get(str(item), 0.0) for item in items], dtype=np.float64)
    return z + shift_arr


def n_pos_by_item(pdf: pd.DataFrame, schema: dict) -> dict[str, int]:
    s = pd.Series(
        pdf[schema["label"]].to_numpy(dtype=np.int64),
        index=pdf.index,
    ).groupby(pdf[schema["item"]].astype(str)).sum()
    return {str(k): int(v) for k, v in s.items()}


def optimize_item_shifts(
    groups: np.ndarray,
    items: np.ndarray,
    y: np.ndarray,
    base_score: np.ndarray,
    mp: dict,
    *,
    max_abs_shift: float,
    initial_step: float,
    min_step: float,
    max_sweeps_per_step: int,
    min_pos_for_shift: int,
    candidate_multipliers: tuple[int, ...],
) -> tuple[dict[str, float], dict]:
    uniq_items = sorted(str(x) for x in np.unique(items))
    pos_counts = {
        item: int(y[items == item].sum())
        for item in uniq_items
    }
    shiftable = [
        item for item in uniq_items
        if pos_counts[item] >= min_pos_for_shift
    ]
    shifts = {item: 0.0 for item in uniq_items}
    baseline_map = float(compute_macro_per_item_map(groups, items, y, base_score, **mp))
    current_map = baseline_map
    history: list[dict[str, Any]] = []

    step = float(initial_step)
    tol = 1e-12
    while step >= min_step - 1e-12:
        for sweep in range(max_sweeps_per_step):
            improved = False
            for item in shiftable:
                best_shift = shifts[item]
                best_map = current_map
                for mult in candidate_multipliers:
                    cand = float(np.clip(shifts[item] + mult * step, -max_abs_shift, max_abs_shift))
                    if cand == shifts[item] and mult != 0:
                        continue
                    cand_shifts = dict(shifts)
                    cand_shifts[item] = cand
                    cand_score = shifted_score(base_score, items, cand_shifts)
                    cand_map = float(compute_macro_per_item_map(groups, items, y, cand_score, **mp))
                    if cand_map > best_map + tol:
                        best_map = cand_map
                        best_shift = cand
                if best_shift != shifts[item]:
                    shifts[item] = best_shift
                    current_map = best_map
                    improved = True
            history.append({
                "step": float(step),
                "sweep": int(sweep),
                "map": float(current_map),
                "improved": bool(improved),
            })
            if not improved:
                break
        step = step / 2.0

    final_score = shifted_score(base_score, items, shifts)
    final_map = float(compute_macro_per_item_map(groups, items, y, final_score, **mp))
    meta = {
        "baseline_map": baseline_map,
        "shifted_map": final_map,
        "delta": final_map - baseline_map,
        "n_items": int(len(uniq_items)),
        "n_shiftable_items": int(len(shiftable)),
        "min_pos_for_shift": int(min_pos_for_shift),
        "max_abs_shift": float(max_abs_shift),
        "initial_step": float(initial_step),
        "min_step": float(min_step),
        "max_sweeps_per_step": int(max_sweeps_per_step),
        "candidate_multipliers": list(candidate_multipliers),
        "history": history,
    }
    return shifts, meta


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


def per_item_map_rows(
    items: np.ndarray,
    y: np.ndarray,
    groups: np.ndarray,
    baseline_score: np.ndarray,
    shifted: np.ndarray,
    shifts: dict[str, float],
    mp: dict,
) -> list[dict[str, Any]]:
    def per_item_values(score: np.ndarray) -> tuple[dict[str, float], dict[str, int]]:
        contrib, row_idx = positive_row_contributions(groups, y, score, mp["k"])
        if len(contrib) == 0:
            return {}, {}
        pos_items = items[row_idx].astype(str)
        uniq, inv = np.unique(pos_items, return_inverse=True)
        sums = np.bincount(inv, weights=contrib)
        counts = np.bincount(inv)
        vals = sums / counts
        return (
            {str(item): float(v) for item, v in zip(uniq, vals)},
            {str(item): int(n) for item, n in zip(uniq, counts)},
        )

    base_vals, counts = per_item_values(baseline_score)
    shifted_vals, _ = per_item_values(shifted)
    rows = []
    for item in sorted(set(base_vals) | set(shifted_vals) | set(shifts)):
        base = base_vals.get(item)
        after = shifted_vals.get(item)
        rows.append({
            "item": item,
            "shift": float(shifts.get(item, 0.0)),
            "ap_baseline": base,
            "ap_shifted": after,
            "delta_ap": None if base is None or after is None else after - base,
            "n_pos_holdout": int(counts.get(item, 0)),
        })
    rows.sort(key=lambda r: abs(float(r["shift"])), reverse=True)
    return rows


def run_diagnosis(
    tune: pd.DataFrame,
    holdout: pd.DataFrame,
    search_tune: pd.DataFrame,
    search_meta: dict,
    parameters: dict,
    schema: dict,
    args: argparse.Namespace,
) -> dict:
    mp = metric_params(parameters)
    tune_groups, tune_items, tune_y, tune_z, tune_notes = arrays_for_metric(search_tune, schema)
    hold_groups, hold_items, hold_y, hold_z, hold_notes = arrays_for_metric(holdout, schema)
    full_tune_groups, full_tune_items, full_tune_y, full_tune_z, _ = arrays_for_metric(tune, schema)

    shifts, tune_meta = optimize_item_shifts(
        tune_groups,
        tune_items,
        tune_y,
        tune_z,
        mp,
        max_abs_shift=args.max_abs_shift,
        initial_step=args.initial_step,
        min_step=args.min_step,
        max_sweeps_per_step=args.max_sweeps_per_step,
        min_pos_for_shift=args.min_pos_for_shift,
        candidate_multipliers=tuple(args.candidate_multipliers),
    )
    tune_meta["search_sample"] = search_meta

    full_tune_shifted = shifted_score(full_tune_z, full_tune_items, shifts)
    full_tune_baseline = float(
        compute_macro_per_item_map(full_tune_groups, full_tune_items, full_tune_y, full_tune_z, **mp)
    )
    full_tune_after = float(
        compute_macro_per_item_map(full_tune_groups, full_tune_items, full_tune_y, full_tune_shifted, **mp)
    )
    tune_meta["full_tune_baseline_map"] = full_tune_baseline
    tune_meta["full_tune_shifted_map"] = full_tune_after
    tune_meta["full_tune_delta"] = full_tune_after - full_tune_baseline

    hold_shifted = shifted_score(hold_z, hold_items, shifts)
    hold_baseline = float(compute_macro_per_item_map(hold_groups, hold_items, hold_y, hold_z, **mp))
    hold_after = float(compute_macro_per_item_map(hold_groups, hold_items, hold_y, hold_shifted, **mp))
    hold_delta = hold_after - hold_baseline

    clusters = entity_clusters(holdout, schema)
    rng = np.random.RandomState(args.seed)
    n_clusters = int(clusters.max()) + 1 if len(clusters) else 0
    draws = (
        rng.randint(0, n_clusters, size=(args.n_boot, n_clusters))
        if n_clusters
        else np.empty((0, 0), dtype=int)
    )
    boot_base = _bootstrap_macro_values(hold_groups, clusters, hold_items, hold_y, hold_z, draws, mp)
    boot_after = _bootstrap_macro_values(hold_groups, clusters, hold_items, hold_y, hold_shifted, draws, mp)
    boot_delta = boot_after - boot_base

    per_item = per_item_map_rows(
        hold_items,
        hold_y,
        hold_groups,
        hold_z,
        hold_shifted,
        shifts,
        mp,
    )
    tune_pos = n_pos_by_item(tune, schema)
    for row in per_item:
        row["n_pos_tune"] = int(tune_pos.get(row["item"], 0))
    per_item.sort(key=lambda r: r["shift"])

    return {
        "metric_params": mp,
        "score_col_used": SCORE_COL,
        "logit_notes": list(dict.fromkeys(tune_notes + hold_notes)),
        "search": tune_meta,
        "holdout": {
            "baseline_map": hold_baseline,
            "shifted_map": hold_after,
            "delta": hold_delta,
            "delta_ci_low": None if len(boot_delta) == 0 else float(np.nanpercentile(boot_delta, 2.5)),
            "delta_ci_high": None if len(boot_delta) == 0 else float(np.nanpercentile(boot_delta, 97.5)),
            "n_boot": int(args.n_boot),
            "bootstrap_seed": int(args.seed),
            "n_rows": int(len(holdout)),
            "n_queries": int(len(np.unique(hold_groups))),
            "n_entities": int(n_clusters),
            "n_positive_rows": int(hold_y.sum()),
        },
        "shifts": [{"item": item, "shift": shift} for item, shift in sorted(shifts.items())],
        "per_item_holdout": per_item,
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
    head = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    body = []
    for r in shown:
        tds = []
        for key, _ in columns:
            val = r.get(key)
            text = fmt_num(val) if isinstance(val, (int, float, np.integer, np.floating)) else str(val)
            tds.append(f"<td>{html.escape(text)}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def interpretation(result: dict) -> str:
    hold = result["holdout"]
    lo = hold["delta_ci_low"]
    hi = hold["delta_ci_high"]
    delta = hold["delta"]
    if lo is not None and lo > 0:
        return (
            "On holdout entities, fixed per-item score shifts significantly "
            "improve mAP. This is evidence of item-level ranking bias that "
            "can be corrected post-hoc on the current model scores."
        )
    if hi is not None and hi < 0:
        return (
            "On holdout entities, fixed per-item score shifts significantly "
            "hurt mAP. This post-hoc shift set should not be used."
        )
    if abs(delta) < 1e-12:
        return (
            "Holdout mAP is unchanged; there is no observed room for "
            "fixed per-item score shifts to improve ranking."
        )
    return (
        "The holdout CI crosses 0; the shifts found on tune entities have "
        "not shown stable generalization."
    )


def render_html(report: dict) -> str:
    result = report["result"]
    hold = result["holdout"]
    search = result["search"]
    split = report["split"]
    search_sample = search.get("search_sample", {})
    source = report["source"]
    interp = interpretation(result)
    notes = report.get("notes", []) + result.get("logit_notes", [])
    notes_html = "".join(f"<li>{html.escape(n)}</li>" for n in notes)
    shifts_nonzero = sum(abs(r["shift"]) > 1e-12 for r in result["shifts"])

    css = """
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;color:#1f2933;background:#fbfcfe}
    h1{font-size:28px;margin-bottom:4px} h2{margin-top:32px;font-size:20px}
    .muted{color:#697386}.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin:20px 0}
    .card{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:14px}
    .big{font-size:24px;font-weight:700}
    table{border-collapse:collapse;width:100%;background:#fff;margin:12px 0;border:1px solid #d9e2ec}
    th,td{padding:8px 10px;border-bottom:1px solid #edf2f7;text-align:right;font-size:13px}
    th:first-child,td:first-child{text-align:left} th{background:#f4f7fb;color:#344054}
    code{background:#eef2f7;padding:1px 4px;border-radius:4px}.note{background:#fff8e6;border:1px solid #f6d365;border-radius:8px;padding:12px}
    """
    summary_cards = [
        ("holdout baseline mAP", fmt_num(hold["baseline_map"])),
        ("holdout shifted mAP", fmt_num(hold["shifted_map"])),
        ("holdout Delta", fmt_num(hold["delta"])),
        ("95% paired CI", f"[{fmt_num(hold['delta_ci_low'])}, {fmt_num(hold['delta_ci_high'])}]"),
        ("tune Delta", fmt_num(search["delta"])),
        ("nonzero shifts", f"{shifts_nonzero} / {len(result['shifts'])}"),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='muted'>{html.escape(k)}</div><div class='big'>{html.escape(v)}</div></div>"
        for k, v in summary_cards
    )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Per-Item Score Shift Diagnosis</title><style>{css}</style></head>
<body>
<h1>Per-Item Score Shift Diagnosis</h1>
<p class="muted">Question: without retraining, can fixed per-item score shifts improve macro per-item mAP?</p>
<div class="summary">{cards_html}</div>
<div class="note"><strong>Conclusion.</strong> {html.escape(interp)}</div>

<h2>Input</h2>
<table><tbody>
<tr><th>source</th><td>{html.escape(str(source.get("source")))}</td></tr>
<tr><th>catalog entry</th><td><code>{html.escape(str(source.get("catalog_entry")))}</code></td></tr>
<tr><th>score column</th><td><code>{html.escape(result["score_col_used"])}</code></td></tr>
<tr><th>split</th><td>{fmt_num(split["tune_fraction"], 2)} tune by entity, seed={split["seed"]}</td></tr>
<tr><th>rows tune / holdout</th><td>{split["n_rows_tune"]} / {split["n_rows_holdout"]}</td></tr>
<tr><th>queries tune / holdout</th><td>{split["n_queries_tune"]} / {split["n_queries_holdout"]}</td></tr>
<tr><th>search rows / queries</th><td>{search_sample.get("rows_after", split["n_rows_tune"])} / {search_sample.get("n_queries_after", split["n_queries_tune"])}</td></tr>
<tr><th>bootstrap</th><td>{hold["n_boot"]} paired entity bootstrap draws, seed={hold["bootstrap_seed"]}</td></tr>
</tbody></table>
<p class="muted">This is a post-hoc counterfactual on current model scores, not a retraining estimate. True config/model changes still require retraining and evaluation.</p>
{"<ul>" + notes_html + "</ul>" if notes_html else ""}

<h2>1. Holdout mAP Impact</h2>
<p>Find item shifts on tune entities, then apply the same shifts to holdout entities.</p>
<table><tbody>
<tr><th>baseline mAP</th><td>{fmt_num(hold["baseline_map"])}</td></tr>
<tr><th>shifted mAP</th><td>{fmt_num(hold["shifted_map"])}</td></tr>
<tr><th>Delta</th><td>{fmt_num(hold["delta"])}</td></tr>
<tr><th>95% CI</th><td>[{fmt_num(hold["delta_ci_low"])}, {fmt_num(hold["delta_ci_high"])}]</td></tr>
</tbody></table>

<h2>2. Search Summary</h2>
<p>Coordinate search maximizes tune macro per-item mAP over bounded per-item constants.</p>
<table><tbody>
<tr><th>tune baseline mAP</th><td>{fmt_num(search["baseline_map"])}</td></tr>
<tr><th>tune shifted mAP</th><td>{fmt_num(search["shifted_map"])}</td></tr>
<tr><th>tune Delta</th><td>{fmt_num(search["delta"])}</td></tr>
<tr><th>full tune Delta</th><td>{fmt_num(search["full_tune_delta"])}</td></tr>
<tr><th>shiftable items</th><td>{search["n_shiftable_items"]} / {search["n_items"]}</td></tr>
<tr><th>bounds</th><td>abs shift <= {fmt_num(search["max_abs_shift"])}, min positives = {search["min_pos_for_shift"]}</td></tr>
</tbody></table>

<h2>3. Learned Item Shifts</h2>
<p>Positive shift pushes an item up against other items in the same query; negative shift pushes it down.</p>
{table_html(result["per_item_holdout"], [("item","item"),("shift","shift"),("delta_ap","holdout Delta AP"),("ap_baseline","holdout AP"),("ap_shifted","holdout shifted AP"),("n_pos_tune","n_pos tune"),("n_pos_holdout","n_pos holdout")])}
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
    p.add_argument("--output", default="data/diagnosis/per_item_score_shift.html")
    p.add_argument("--tune-fraction", type=float, default=0.5, help="Entity-level fraction used to learn shifts.")
    p.add_argument("--max-abs-shift", type=float, default=4.0)
    p.add_argument("--initial-step", type=float, default=1.0)
    p.add_argument("--min-step", type=float, default=0.125)
    p.add_argument("--max-sweeps-per-step", type=int, default=3)
    p.add_argument("--candidate-multipliers", type=int, nargs="+", default=[-1, 0, 1], help="Integer step multipliers evaluated for each item.")
    p.add_argument("--min-pos-for-shift", type=int, default=50)
    p.add_argument("--search-max-queries", type=int, default=50000, help="Cap tune queries used to learn item shifts. Holdout evaluation remains full.")
    p.add_argument("--n-boot", type=int, default=200)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    parameters = load_parameters(args.params)
    schema = get_schema(parameters)
    pdf_raw, source_meta = load_enriched_eval_predictions(args, parameters, schema)
    pdf, notes = validate_and_prepare(pdf_raw, schema)
    tune, holdout, split_meta = split_by_entity(pdf, schema, args.tune_fraction, args.seed)
    search_tune, search_meta = cap_queries(tune, schema, args.search_max_queries, args.seed)
    result = run_diagnosis(tune, holdout, search_tune, search_meta, parameters, schema, args)
    report = {
        "schema": {"item": schema["item"]},
        "source": source_meta,
        "split": split_meta,
        "notes": notes,
        "result": result,
    }
    write_outputs(report, args.output)
    hold = result["holdout"]
    print(f"Wrote {args.output}")
    print(f"Wrote {Path(args.output).with_suffix('.json')}")
    ci_low = fmt_num(hold["delta_ci_low"], 6)
    ci_high = fmt_num(hold["delta_ci_high"], 6)
    print(f"Holdout Delta = {hold['delta']:.6f} [{ci_low}, {ci_high}]")


if __name__ == "__main__":
    main()
