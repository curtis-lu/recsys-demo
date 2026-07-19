"""Manual diagnosis: does the model identify buyers for each item?

This is a standalone script for the ranking diagnosis redesign spike. It
answers one narrow ability-layer question:

    For each item, does the model lift that item inside the buyer's own
    query, rather than merely giving active customers higher scores overall?

The main proxy is query-centered within-item AUC:

    relative_score(q, j) = logit(score_uncalibrated(q, j))
                           - mean_k logit(score_uncalibrated(q, k))

Then, for each item, AUC is computed over ``relative_score`` and the item
label. Raw within-item AUC is reported only as a contamination check: when raw
AUC is high but query-centered AUC is low, the raw AUC is likely measuring
query/customer activity level instead of item-specific buyer recognition.

The script reads the project catalog entry ``enriched_eval_predictions`` so
users may still change the physical Hive table name in ``catalog.yaml``.

Examples:

  MPLCONFIGDIR=/tmp/recsys-tfb-mpl PYTHONPATH=src \
  python scripts/item_ability_diagnosis.py \
      --model-version 20260717_xxx \
      --output data/diagnosis/item_ability.html
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import to_logit
from recsys_tfb.evaluation.metrics import (
    macro_from_per_item,
    positive_row_contributions,
)

ENRICHED_EVAL_ENTRY = "enriched_eval_predictions"
SCORE_COL = "score_uncalibrated"
logger = logging.getLogger("item_ability_diagnosis")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )


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
        logger.info("Loading parameters from %s", p)
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
    logger.info(
        "Reading %s for snap_date=%s model_version=%s",
        hive_table,
        snap_date,
        model_version,
    )

    t0 = time.monotonic()
    spark = (
        SparkSession.builder
        .appName("item_ability_diagnosis")
        .enableHiveSupport()
        .getOrCreate()
    )
    logger.info("Spark session ready in %.1fs", time.monotonic() - t0)
    sdf = spark.table(hive_table)

    time_col = schema["time"]
    if time_col not in sdf.columns:
        raise ValueError(f"{hive_table} is missing time column {time_col!r}.")
    if "model_version" not in sdf.columns:
        raise ValueError(f"{hive_table} is missing partition column 'model_version'.")
    sdf = sdf.filter(F.col(time_col).cast("string") == str(snap_date))
    sdf = sdf.filter(F.col("model_version") == str(model_version))

    needed = [c for c in required_columns(schema) if c in sdf.columns]
    logger.info("Selecting columns: %s", needed)
    sdf = sdf.select(*list(dict.fromkeys(needed)))

    params_for_sample = parameters
    if args.sample_max_queries is not None:
        params_for_sample = dict(parameters)
        eval_cfg = dict(params_for_sample.get("evaluation", {}) or {})
        diag_cfg = dict((eval_cfg.get("diagnosis", {}) or {}))
        sample_cfg = dict((diag_cfg.get("sample", {}) or {}))
        sample_cfg["max_queries"] = int(args.sample_max_queries)
        diag_cfg["sample"] = sample_cfg
        eval_cfg["diagnosis"] = diag_cfg
        params_for_sample["evaluation"] = eval_cfg
        logger.info("Overriding Spark diagnosis sample max_queries=%d", args.sample_max_queries)

    logger.info("Drawing diagnosis sample; Spark jobs may run now")
    t0 = time.monotonic()
    sample_pdf, sample_meta = draw_diagnosis_sample(sdf, params_for_sample)
    logger.info(
        "Diagnosis sample loaded to pandas in %.1fs: rows=%d, sampled_queries=%s",
        time.monotonic() - t0,
        len(sample_pdf),
        sample_meta.get("n_queries_sampled"),
    )
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
    logger.info("Validating pandas input: rows=%d, columns=%s", len(pdf), list(pdf.columns))
    out = pdf[keep].copy()
    out[schema["label"]] = out[schema["label"]].astype(int)
    out[schema["item"]] = out[schema["item"]].astype(str)
    logger.info(
        "Prepared pandas frame: rows=%d, positives=%d, items=%d",
        len(out),
        int(out[schema["label"]].sum()),
        int(out[schema["item"]].nunique()),
    )
    return out, notes


def maybe_cap_queries(
    pdf: pd.DataFrame,
    schema: dict,
    max_queries: int | None,
    seed: int,
) -> tuple[pd.DataFrame, dict]:
    if max_queries is None:
        return pdf, {"applied": False}
    if max_queries <= 0:
        raise ValueError("--max-queries must be positive.")

    query_cols = [schema["time"], *schema["entity"]]
    qkey = _query_key(pdf, query_cols)
    n_queries = int(qkey.nunique())
    if n_queries <= max_queries:
        logger.info(
            "Pandas query cap not applied: n_queries=%d <= max_queries=%d",
            n_queries,
            max_queries,
        )
        return pdf, {
            "applied": False,
            "n_queries_before": n_queries,
            "max_queries": int(max_queries),
        }

    logger.info(
        "Applying pandas query cap: n_queries=%d -> max_queries=%d",
        n_queries,
        max_queries,
    )
    q_unique = pd.Series(pd.unique(qkey), name="query_key")
    hashed = pd.util.hash_pandas_object(q_unique + f"|{seed}", index=False)
    keep_queries = set(
        q_unique.iloc[np.argsort(hashed.to_numpy(dtype=np.uint64))[:max_queries]]
        .astype(str)
        .tolist()
    )
    out = pdf[qkey.astype(str).isin(keep_queries)].copy()
    logger.info(
        "Pandas query cap applied: rows=%d -> %d",
        len(pdf),
        len(out),
    )
    return out, {
        "applied": True,
        "n_queries_before": n_queries,
        "n_queries_after": int(out[query_cols].drop_duplicates().shape[0]),
        "rows_before": int(len(pdf)),
        "rows_after": int(len(out)),
        "max_queries": int(max_queries),
        "seed": int(seed),
    }


def metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    return {
        "k": None if m.get("k") is None else int(m["k"]),
        "weight_alpha": float(m.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(m.get("min_positives", 0) or 0),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0) or 0.0),
    }


def weighted_auc(
    score: np.ndarray,
    y: np.ndarray,
    weight: np.ndarray | None = None,
) -> float | None:
    """Weighted binary AUC with 0.5 credit for score ties."""
    if len(score) == 0:
        return None
    s = np.asarray(score, dtype=np.float64)
    yy = np.asarray(y, dtype=np.int64)
    w = (
        np.ones(len(s), dtype=np.float64)
        if weight is None
        else np.asarray(weight, dtype=np.float64)
    )
    keep = w > 0
    s = s[keep]
    yy = yy[keep]
    w = w[keep]
    if len(s) == 0:
        return None

    pos_total = float(w[yy == 1].sum())
    neg_total = float(w[yy == 0].sum())
    if pos_total <= 0.0 or neg_total <= 0.0:
        return None

    order = np.argsort(s, kind="mergesort")
    s = s[order]
    yy = yy[order]
    w = w[order]

    numer = 0.0
    neg_before = 0.0
    start = 0
    while start < len(s):
        end = start + 1
        while end < len(s) and s[end] == s[start]:
            end += 1
        yy_g = yy[start:end]
        w_g = w[start:end]
        pos_w = float(w_g[yy_g == 1].sum())
        neg_w = float(w_g[yy_g == 0].sum())
        numer += pos_w * (neg_before + 0.5 * neg_w)
        neg_before += neg_w
        start = end
    return float(numer / (pos_total * neg_total))


def query_center_scores(groups: np.ndarray, z: np.ndarray) -> np.ndarray:
    sums = np.bincount(groups, weights=z)
    counts = np.bincount(groups)
    return z - (sums / counts)[groups]


def rank_percentiles(groups: np.ndarray, score: np.ndarray) -> np.ndarray:
    """One-based descending rank divided by query size; lower is better."""
    out = np.full(len(score), np.nan, dtype=np.float64)
    if len(score) == 0:
        return out
    order = np.lexsort((-score, groups))
    g_sorted = groups[order]
    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        idx = order[s:e]
        n = e - s
        out[idx] = np.arange(1, n + 1, dtype=np.float64) / float(n)
    return out


def per_item_ap(
    groups: np.ndarray,
    items: np.ndarray,
    y: np.ndarray,
    score: np.ndarray,
    mp: dict,
) -> tuple[dict[str, float], dict[str, int], float]:
    contrib, row_idx = positive_row_contributions(groups, y, score, mp["k"])
    if len(contrib) == 0:
        return {}, {}, 0.0
    pos_items = items[row_idx].astype(str)
    uniq, inv = np.unique(pos_items, return_inverse=True)
    sums = np.bincount(inv, weights=contrib)
    counts = np.bincount(inv)
    vals = sums / counts
    macro = macro_from_per_item(
        vals,
        counts,
        weight_alpha=mp["weight_alpha"],
        min_positives=mp["min_positives"],
        shrinkage_k=mp["shrinkage_k"],
    )
    return (
        {str(item): float(v) for item, v in zip(uniq, vals)},
        {str(item): int(n) for item, n in zip(uniq, counts)},
        0.0 if macro is None else float(macro),
    )


def _bootstrap_item_auc(
    score: np.ndarray,
    y: np.ndarray,
    clusters: np.ndarray,
    draws: np.ndarray,
) -> np.ndarray:
    out = np.full(draws.shape[0], np.nan, dtype=np.float64)
    n_clusters = int(clusters.max()) + 1 if len(clusters) else 0
    for b, draw in enumerate(draws):
        mult = np.bincount(draw, minlength=n_clusters).astype(np.float64)
        auc = weighted_auc(score, y, mult[clusters])
        if auc is not None:
            out[b] = auc
    return out


def analyze_items(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
    *,
    n_boot: int,
    seed: int,
    top_n: int,
) -> dict:
    total_t0 = time.monotonic()
    query_cols = [schema["time"], *schema["entity"]]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    mp = metric_params(parameters)

    logger.info("Factorizing query/entity keys")
    t0 = time.monotonic()
    groups = pd.factorize(_query_key(pdf, query_cols))[0]
    clusters = pd.factorize(pdf[entity_cols].astype(str).agg("|".join, axis=1))[0]
    items = pdf[item_col].astype(str).to_numpy()
    y = pdf[label_col].to_numpy(dtype=np.int64)
    logger.info(
        "Factorized in %.1fs: rows=%d, queries=%d, entities=%d, items=%d, positives=%d",
        time.monotonic() - t0,
        len(pdf),
        int(len(np.unique(groups))),
        int(len(np.unique(clusters))),
        int(len(np.unique(items))),
        int(y.sum()),
    )

    logger.info("Computing logit scores, query-centered scores, rank percentiles, and AP")
    t0 = time.monotonic()
    z, logit_notes = to_logit(pdf[SCORE_COL].to_numpy(dtype=np.float64))
    rel = query_center_scores(groups, z)
    rank_pct = rank_percentiles(groups, z)
    ap_by_item, n_pos_ap, macro_map = per_item_ap(groups, items, y, z, mp)
    logger.info(
        "Base arrays computed in %.1fs: macro_per_item_map=%.6f",
        time.monotonic() - t0,
        macro_map,
    )

    rng = np.random.RandomState(seed)
    n_clusters = int(clusters.max()) + 1 if len(clusters) else 0
    draws = (
        rng.randint(0, n_clusters, size=(n_boot, n_clusters))
        if n_clusters and n_boot > 0
        else np.empty((0, 0), dtype=int)
    )
    logger.info(
        "Bootstrap setup: n_boot=%d, clusters=%d, draws_shape=%s",
        n_boot,
        n_clusters,
        draws.shape,
    )

    rows: list[dict[str, Any]] = []
    unique_items = sorted(str(x) for x in np.unique(items))
    for item_idx, item in enumerate(unique_items, start=1):
        item_t0 = time.monotonic()
        mask = items == item
        yy = y[mask]
        z_i = z[mask]
        rel_i = rel[mask]
        cl_i = clusters[mask]
        pos_mask = yy == 1
        neg_mask = yy == 0
        logger.info(
            "Analyzing item %d/%d: %s rows=%d pos=%d neg=%d",
            item_idx,
            len(unique_items),
            item,
            int(mask.sum()),
            int(pos_mask.sum()),
            int(neg_mask.sum()),
        )

        logger.info("Computing raw AUC for item %d/%d: %s", item_idx, len(unique_items), item)
        raw_auc = weighted_auc(z_i, yy)
        logger.info("Computing query-centered AUC for item %d/%d: %s", item_idx, len(unique_items), item)
        centered_auc = weighted_auc(rel_i, yy)
        if n_boot > 0:
            logger.info(
                "Bootstrapping item %d/%d: %s n_boot=%d",
                item_idx,
                len(unique_items),
                item,
                n_boot,
            )
        boot_centered = _bootstrap_item_auc(rel_i, yy, cl_i, draws)
        boot_raw = _bootstrap_item_auc(z_i, yy, cl_i, draws)

        pos_rel = rel_i[pos_mask]
        neg_rel = rel_i[neg_mask]
        pos_rank = rank_pct[mask][pos_mask]

        mean_pos = float(np.mean(pos_rel)) if len(pos_rel) else None
        mean_neg = float(np.mean(neg_rel)) if len(neg_rel) else None
        centered_ci_low = (
            None if len(boot_centered) == 0 or np.all(np.isnan(boot_centered))
            else float(np.nanpercentile(boot_centered, 2.5))
        )
        centered_ci_high = (
            None if len(boot_centered) == 0 or np.all(np.isnan(boot_centered))
            else float(np.nanpercentile(boot_centered, 97.5))
        )
        raw_ci_low = (
            None if len(boot_raw) == 0 or np.all(np.isnan(boot_raw))
            else float(np.nanpercentile(boot_raw, 2.5))
        )
        raw_ci_high = (
            None if len(boot_raw) == 0 or np.all(np.isnan(boot_raw))
            else float(np.nanpercentile(boot_raw, 97.5))
        )
        gap = (
            None if raw_auc is None or centered_auc is None
            else float(raw_auc - centered_auc)
        )
        rows.append({
            "item": item,
            "ap": ap_by_item.get(item),
            "n_pos": int(pos_mask.sum()),
            "n_neg": int(neg_mask.sum()),
            "query_centered_auc": centered_auc,
            "query_centered_auc_ci_low": centered_ci_low,
            "query_centered_auc_ci_high": centered_ci_high,
            "raw_within_item_auc": raw_auc,
            "raw_within_item_auc_ci_low": raw_ci_low,
            "raw_within_item_auc_ci_high": raw_ci_high,
            "auc_gap_raw_minus_centered": gap,
            "mean_relative_score_pos": mean_pos,
            "mean_relative_score_neg": mean_neg,
            "relative_score_gap": (
                None if mean_pos is None or mean_neg is None
                else float(mean_pos - mean_neg)
            ),
            "median_positive_rank_percentile": (
                None if len(pos_rank) == 0 else float(np.nanmedian(pos_rank))
            ),
            "p25_positive_rank_percentile": (
                None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 25))
            ),
            "p75_positive_rank_percentile": (
                None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 75))
            ),
            "p90_positive_rank_percentile": (
                None if len(pos_rank) == 0 else float(np.nanpercentile(pos_rank, 90))
            ),
            "positive_rank_percentiles": pos_rank.tolist(),
            "n_pos_ap": int(n_pos_ap.get(item, 0)),
        })
        logger.info(
            "Finished item %d/%d: %s in %.1fs centered_auc=%s raw_auc=%s",
            item_idx,
            len(unique_items),
            item,
            time.monotonic() - item_t0,
            fmt_num(centered_auc),
            fmt_num(raw_auc),
        )

    logger.info("Sorting item rows and finalizing report payload")
    rows.sort(key=lambda r: (
        float("inf") if r["ap"] is None else float(r["ap"]),
        -(r["n_pos"]),
        r["item"],
    ))
    logger.info("analyze_items completed in %.1fs", time.monotonic() - total_t0)

    return {
        "metric_params": mp,
        "score_col_used": SCORE_COL,
        "logit_notes": logit_notes,
        "n_rows": int(len(pdf)),
        "n_queries": int(len(np.unique(groups))),
        "n_entities": int(n_clusters),
        "n_items": int(len(rows)),
        "n_positive_rows": int(y.sum()),
        "macro_per_item_map": macro_map,
        "n_boot": int(n_boot),
        "bootstrap_seed": int(seed),
        "top_n": int(top_n),
        "per_item": rows,
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


def _plot_rows(rows: list[dict], xkey: str, ykey: str) -> list[dict]:
    return [
        r for r in rows
        if r.get(xkey) is not None
        and r.get(ykey) is not None
        and np.isfinite(float(r[xkey]))
        and np.isfinite(float(r[ykey]))
    ]


def svg_scatter(
    rows: list[dict],
    xkey: str,
    ykey: str,
    *,
    xlabel: str,
    ylabel: str,
    width: int = 860,
    height: int = 420,
) -> str:
    pts = _plot_rows(rows, xkey, ykey)
    if not pts:
        return "<p class='muted'>No plottable rows.</p>"
    pad_l, pad_r, pad_t, pad_b = 70, 24, 24, 58
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    xs = np.array([float(r[xkey]) for r in pts])
    ys = np.array([float(r[ykey]) for r in pts])
    x0, x1 = min(0.0, float(xs.min())), max(1.0, float(xs.max()))
    y0, y1 = min(0.0, float(ys.min())), max(1.0, float(ys.max()))
    if x0 == x1:
        x1 = x0 + 1.0
    if y0 == y1:
        y1 = y0 + 1.0

    def sx(x: float) -> float:
        return pad_l + (x - x0) / (x1 - x0) * plot_w

    def sy(y: float) -> float:
        return pad_t + (1.0 - (y - y0) / (y1 - y0)) * plot_h

    circles = []
    for r in pts:
        n_pos = max(1.0, float(r.get("n_pos", 1) or 1))
        radius = min(13.0, 3.5 + np.log1p(n_pos) * 1.2)
        label = html.escape(
            f"{r['item']}: {xlabel}={fmt_num(r[xkey])}, "
            f"{ylabel}={fmt_num(r[ykey])}, n_pos={r.get('n_pos')}"
        )
        color = "#0f766e"
        if ykey == "query_centered_auc" and xkey == "raw_within_item_auc":
            if float(r[xkey]) >= 0.7 and float(r[ykey]) <= 0.55:
                color = "#b42318"
        elif ykey == "ap":
            if float(r[ykey]) < 0.4 and float(r[xkey]) < 0.55:
                color = "#b42318"
            elif float(r[ykey]) < 0.4 and float(r[xkey]) >= 0.65:
                color = "#9a6700"
        circles.append(
            f"<circle cx='{sx(float(r[xkey])):.1f}' cy='{sy(float(r[ykey])):.1f}' "
            f"r='{radius:.1f}' fill='{color}' fill-opacity='0.72'>"
            f"<title>{label}</title></circle>"
        )

    grid = []
    for tick in np.linspace(0.0, 1.0, 6):
        if x0 <= tick <= x1:
            x = sx(float(tick))
            grid.append(
                f"<line x1='{x:.1f}' y1='{pad_t}' x2='{x:.1f}' y2='{pad_t + plot_h}' "
                "stroke='#edf2f7'/>"
            )
            grid.append(
                f"<text x='{x:.1f}' y='{height - 32}' text-anchor='middle' "
                f"font-size='11' fill='#697386'>{tick:.1f}</text>"
            )
        if y0 <= tick <= y1:
            y = sy(float(tick))
            grid.append(
                f"<line x1='{pad_l}' y1='{y:.1f}' x2='{pad_l + plot_w}' y2='{y:.1f}' "
                "stroke='#edf2f7'/>"
            )
            grid.append(
                f"<text x='{pad_l - 12}' y='{y + 4:.1f}' text-anchor='end' "
                f"font-size='11' fill='#697386'>{tick:.1f}</text>"
            )

    return (
        f"<svg viewBox='0 0 {width} {height}' width='100%' role='img'>"
        f"<rect x='0' y='0' width='{width}' height='{height}' fill='#fff'/>"
        + "".join(grid)
        + f"<line x1='{pad_l}' y1='{pad_t + plot_h}' x2='{pad_l + plot_w}' y2='{pad_t + plot_h}' stroke='#98a2b3'/>"
        + f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{pad_t + plot_h}' stroke='#98a2b3'/>"
        + "".join(circles)
        + f"<text x='{pad_l + plot_w / 2:.1f}' y='{height - 8}' text-anchor='middle' font-size='13' fill='#344054'>{html.escape(xlabel)}</text>"
        + f"<text transform='translate(18 {pad_t + plot_h / 2:.1f}) rotate(-90)' text-anchor='middle' font-size='13' fill='#344054'>{html.escape(ylabel)}</text>"
        + "</svg>"
    )


def svg_rank_bars(
    rows: list[dict],
    *,
    limit: int,
    width: int = 860,
    row_h: int = 28,
) -> str:
    shown = [r for r in rows if r.get("median_positive_rank_percentile") is not None][:limit]
    if not shown:
        return "<p class='muted'>No positive-rank rows.</p>"
    pad_l, pad_r, pad_t, pad_b = 180, 28, 22, 36
    height = pad_t + pad_b + row_h * len(shown)
    plot_w = width - pad_l - pad_r

    parts = [
        f"<svg viewBox='0 0 {width} {height}' width='100%' role='img'>",
        f"<rect x='0' y='0' width='{width}' height='{height}' fill='#fff'/>",
    ]
    for tick in np.linspace(0.0, 1.0, 6):
        x = pad_l + tick * plot_w
        parts.append(
            f"<line x1='{x:.1f}' y1='{pad_t}' x2='{x:.1f}' y2='{height - pad_b}' stroke='#edf2f7'/>"
        )
        parts.append(
            f"<text x='{x:.1f}' y='{height - 12}' text-anchor='middle' font-size='11' fill='#697386'>{tick:.1f}</text>"
        )
    for i, r in enumerate(shown):
        y = pad_t + i * row_h + row_h / 2
        p25 = float(r["p25_positive_rank_percentile"])
        p75 = float(r["p75_positive_rank_percentile"])
        med = float(r["median_positive_rank_percentile"])
        p90 = float(r["p90_positive_rank_percentile"])
        x25 = pad_l + p25 * plot_w
        x75 = pad_l + p75 * plot_w
        xmed = pad_l + med * plot_w
        x90 = pad_l + p90 * plot_w
        label = html.escape(str(r["item"]))
        parts.append(
            f"<text x='{pad_l - 10}' y='{y + 4:.1f}' text-anchor='end' font-size='12' fill='#344054'>{label}</text>"
        )
        parts.append(
            f"<line x1='{x25:.1f}' y1='{y:.1f}' x2='{x90:.1f}' y2='{y:.1f}' stroke='#98a2b3' stroke-width='2'/>"
        )
        parts.append(
            f"<rect x='{x25:.1f}' y='{y - 7:.1f}' width='{max(1.0, x75 - x25):.1f}' height='14' fill='#d1fadf' stroke='#0f766e'/>"
        )
        parts.append(
            f"<line x1='{xmed:.1f}' y1='{y - 9:.1f}' x2='{xmed:.1f}' y2='{y + 9:.1f}' stroke='#0f766e' stroke-width='2'>"
            f"<title>{label}: median={fmt_num(med)}, p25={fmt_num(p25)}, p75={fmt_num(p75)}, p90={fmt_num(p90)}</title></line>"
        )
    parts.append(
        f"<text x='{pad_l + plot_w / 2:.1f}' y='{height - 1}' text-anchor='middle' font-size='13' fill='#344054'>positive rank percentile, lower is better</text>"
    )
    parts.append("</svg>")
    return "".join(parts)


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


def render_html(report: dict) -> str:
    result = report["result"]
    rows = result["per_item"]
    notes = report.get("notes", []) + result.get("logit_notes", [])
    notes_html = "".join(f"<li>{html.escape(n)}</li>" for n in notes)
    source = report["source"]
    top_n = result["top_n"]

    css = """
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;color:#1f2933;background:#fbfcfe}
    h1{font-size:28px;margin-bottom:4px} h2{margin-top:32px;font-size:20px}
    .muted{color:#697386}.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin:20px 0}
    .card{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:14px}
    .big{font-size:24px;font-weight:700}
    table{border-collapse:collapse;width:100%;background:#fff;margin:12px 0;border:1px solid #d9e2ec}
    th,td{padding:8px 10px;border-bottom:1px solid #edf2f7;text-align:right;font-size:13px;vertical-align:top}
    th:first-child,td:first-child{text-align:left} th{background:#f4f7fb;color:#344054}
    code{background:#eef2f7;padding:1px 4px;border-radius:4px}.note{background:#fff8e6;border:1px solid #f6d365;border-radius:8px;padding:12px}
    .plot{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:10px;margin:12px 0}
    """
    summary_cards = [
        ("macro per-item mAP", fmt_num(result["macro_per_item_map"])),
        ("rows / queries", f"{result['n_rows']} / {result['n_queries']}"),
        ("entities", str(result["n_entities"])),
        ("items", str(result["n_items"])),
        ("positive rows", str(result["n_positive_rows"])),
        ("bootstrap draws", str(result["n_boot"])),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='muted'>{html.escape(k)}</div><div class='big'>{html.escape(v)}</div></div>"
        for k, v in summary_cards
    )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Item Ability Diagnosis</title><style>{css}</style></head>
<body>
<h1>Item Ability Diagnosis</h1>
<p class="muted">Question: does the model lift each item inside the buyer's own query?</p>
<div class="summary">{cards_html}</div>
<div class="note"><strong>Main read.</strong> Use query-centered AUC as the ability proxy. Raw within-item AUC is a contamination check because it can be driven by customers whose whole score row is high.</div>

<h2>Input</h2>
<table><tbody>
<tr><th>source</th><td>{html.escape(str(source.get("source")))}</td></tr>
<tr><th>catalog entry</th><td><code>{html.escape(str(source.get("catalog_entry")))}</code></td></tr>
<tr><th>score column</th><td><code>{html.escape(result["score_col_used"])}</code></td></tr>
<tr><th>bootstrap</th><td>{result["n_boot"]} entity bootstrap draws, seed={result["bootstrap_seed"]}</td></tr>
</tbody></table>
<p class="muted">Relative score is <code>logit(score_uncalibrated) - query mean logit(score_uncalibrated)</code>. This removes the per-query level that ranking never compares across customers.</p>
{"<ul>" + notes_html + "</ul>" if notes_html else ""}

<h2>1. Per-Item Ability Table</h2>
{table_html(rows, [("item","item"),("ap","AP"),("n_pos","n_pos"),("n_neg","n_neg"),("query_centered_auc","centered AUC"),("query_centered_auc_ci_low","AUC CI low"),("query_centered_auc_ci_high","AUC CI high"),("raw_within_item_auc","raw AUC"),("auc_gap_raw_minus_centered","raw-centered"),("relative_score_gap","rel score gap"),("median_positive_rank_percentile","median pos rank pct")])}

<h2>2. AP vs Query-Centered AUC</h2>
<p class="muted">Low AP plus low centered AUC points to an ability problem. Low AP plus high centered AUC points to suppression by other items.</p>
<div class="plot">{svg_scatter(rows, "query_centered_auc", "ap", xlabel="query-centered AUC", ylabel="AP")}</div>

<h2>3. Raw AUC vs Query-Centered AUC</h2>
<p class="muted">Bottom-right/high-raw-low-centered items are the Alice/Bob failure mode: raw AUC is likely measuring query/customer level.</p>
<div class="plot">{svg_scatter(rows, "raw_within_item_auc", "query_centered_auc", xlabel="raw within-item AUC", ylabel="query-centered AUC")}</div>

<h2>4. Positive Rank Distribution</h2>
<p class="muted">Shown for the first {top_n} lowest-AP items. Lower percentile means the positive item appears earlier in its own query.</p>
<div class="plot">{svg_rank_bars(rows, limit=top_n)}</div>
</body></html>"""


def write_outputs(report: dict, output: str) -> None:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing HTML report to %s", out)
    out.write_text(render_html(report), encoding="utf-8")
    json_path = out.with_suffix(".json")
    json_report = dict(report)
    json_report["result"] = dict(report["result"])
    json_report["result"]["per_item"] = [
        {k: v for k, v in row.items() if k != "positive_rank_percentiles"}
        for row in report["result"]["per_item"]
    ]
    logger.info("Writing JSON report to %s", json_path)
    json_path.write_text(
        json.dumps(json_report, ensure_ascii=False, indent=2, default=_json_default),
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
    p.add_argument("--output", default="data/diagnosis/item_ability.html")
    p.add_argument("--n-boot", type=int, default=200)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--top-n", type=int, default=30, help="Number of lowest-AP items shown in rank distribution chart.")
    p.add_argument("--sample-max-queries", type=int, default=None, help="Override Spark-side diagnosis sample max_queries for faster smoke tests.")
    p.add_argument("--max-queries", type=int, default=None, help="Optional pandas-side query cap after loading the diagnosis sample.")
    return p.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    logger.info("Starting item ability diagnosis")
    parameters = load_parameters(args.params)
    schema = get_schema(parameters)
    logger.info(
        "Schema resolved: time=%s entity=%s item=%s label=%s",
        schema["time"],
        schema["entity"],
        schema["item"],
        schema["label"],
    )
    pdf_raw, source_meta = load_enriched_eval_predictions(args, parameters, schema)
    pdf, notes = validate_and_prepare(pdf_raw, schema)
    pdf, cap_meta = maybe_cap_queries(pdf, schema, args.max_queries, args.seed)
    result = analyze_items(
        pdf,
        parameters,
        schema,
        n_boot=args.n_boot,
        seed=args.seed,
        top_n=args.top_n,
    )
    report = {
        "schema": {"item": schema["item"]},
        "source": source_meta,
        "pandas_query_cap": cap_meta,
        "notes": notes,
        "result": result,
    }
    write_outputs(report, args.output)
    print(f"Wrote {args.output}")
    print(f"Wrote {Path(args.output).with_suffix('.json')}")
    print(f"macro per-item mAP = {result['macro_per_item_map']:.6f}")


if __name__ == "__main__":
    main()
