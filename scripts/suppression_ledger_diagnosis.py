"""Manual diagnosis: which negative items suppress positive items?

This is a standalone script for the ranking diagnosis redesign spike. It
answers one narrow ranking-loss question:

    In the same query, which label=0 items are ranked above each label=1 item?

For every positive row ``(q, j)``, the script enumerates negative rows
``(q, k)`` that appear above it under ``logit(score_uncalibrated)``. It then
aggregates a suppression ledger by ``positive_item`` and ``suppressor_item``.

The report intentionally keeps one main diagnostic currency: AP gap attributed
to negative suppressors. For each suppressed positive row, compute its row gap
``1 - current_precision_contribution`` and allocate that gap across the
negative suppressors above it in proportion to their swap severity.

The script reads the project catalog entry ``enriched_eval_predictions`` so
users may still change the physical Hive table name in ``catalog.yaml``.

Examples:

  MPLCONFIGDIR=/tmp/recsys-tfb-mpl PYTHONPATH=src \
  python scripts/suppression_ledger_diagnosis.py \
      --model-version 20260717_xxx \
      --output data/diagnosis/suppression_ledger.html
"""

from __future__ import annotations

import argparse
import heapq
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
logger = logging.getLogger("suppression_ledger_diagnosis")


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


def required_columns(parameters: dict, schema: dict) -> list[str]:
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
        .appName("suppression_ledger_diagnosis")
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

    needed = [c for c in required_columns(parameters, schema) if c in sdf.columns]
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


def validate_and_prepare(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
) -> tuple[pd.DataFrame, list[str]]:
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
    duplicated = out.duplicated([*query_cols, schema["item"]], keep=False)
    if bool(duplicated.any()):
        examples = (
            out.loc[duplicated, [*query_cols, schema["item"]]]
            .drop_duplicates()
            .head(5)
            .to_dict(orient="records")
        )
        raise ValueError(
            "Input data must have at most one row per query/item for suppression "
            f"ledger diagnosis. Duplicate examples: {examples}"
        )
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
        return pdf, {
            "applied": False,
            "n_queries_before": n_queries,
            "max_queries": int(max_queries),
        }

    q_unique = pd.Series(pd.unique(qkey), name="query_key")
    hashed = pd.util.hash_pandas_object(q_unique + f"|{seed}", index=False)
    keep_queries = set(
        q_unique.iloc[np.argsort(hashed.to_numpy(dtype=np.uint64))[:max_queries]]
        .astype(str)
        .tolist()
    )
    out = pdf[qkey.astype(str).isin(keep_queries)].copy()
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


def fmt_num(x: Any, digits: int = 4) -> str:
    if x is None:
        return ""
    try:
        if not np.isfinite(float(x)):
            return ""
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    return float(np.nanpercentile(np.asarray(values, dtype=np.float64), q))


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(np.mean(np.asarray(values, dtype=np.float64)))


def _rank_display(rank_value: float | None, query_size: float | None) -> str | None:
    if rank_value is None or query_size is None:
        return None

    def compact(x: float) -> str:
        return str(int(round(x))) if abs(x - round(x)) < 1e-9 else f"{x:.1f}"

    return f"{compact(float(rank_value))} of {compact(float(query_size))}"


def _suppressor_summary(row: dict | None) -> str:
    if row is None:
        return ""
    return str(row["suppressor_item"])


def _top_targets_summary(rows: list[dict], limit: int = 3) -> str:
    shown = rows[:limit]
    if not shown:
        return ""
    return ", ".join(str(r["positive_item"]) for r in shown)


def analyze_suppression(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
    *,
    top_examples: int,
) -> dict:
    total_t0 = time.monotonic()
    query_cols = [schema["time"], *schema["entity"]]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    mp = metric_params(parameters)

    logger.info("Factorizing query/entity keys")
    groups = pd.factorize(_query_key(pdf, query_cols))[0]
    query_keys = _query_key(pdf, query_cols).astype(str).to_numpy()
    clusters = pd.factorize(pdf[entity_cols].astype(str).agg("|".join, axis=1))[0]
    items = pdf[item_col].astype(str).to_numpy()
    y = pdf[label_col].to_numpy(dtype=np.int64)
    z, logit_notes = to_logit(pdf[SCORE_COL].to_numpy(dtype=np.float64))
    ap_by_item, n_pos_ap, macro_map = per_item_ap(groups, items, y, z, mp)

    logger.info(
        "Enumerating suppression pairs: rows=%d queries=%d entities=%d items=%d positives=%d",
        len(pdf),
        int(len(np.unique(groups))),
        int(len(np.unique(clusters))),
        int(len(np.unique(items))),
        int(y.sum()),
    )

    rank = np.full(len(pdf), -1, dtype=np.int64)
    rank_pct = np.full(len(pdf), np.nan, dtype=np.float64)
    pair_stats: dict[tuple[str, str], dict[str, Any]] = {}
    target_stats: dict[str, dict[str, Any]] = {}
    suppressor_stats: dict[str, dict[str, Any]] = {}
    example_heap: list[tuple[float, float, int, dict[str, Any]]] = []
    example_seq = 0

    for item in sorted(str(x) for x in np.unique(items)):
        item_mask = items == item
        pos_mask = item_mask & (y == 1)
        target_stats[item] = {
            "positive_item": item,
            "ap": ap_by_item.get(item),
            "n_pos": int(pos_mask.sum()),
            "suppressed_positive_rows": 0,
            "suppressed_positive_rate": None,
            "total_suppression_pairs": 0,
            "mean_negatives_above_positive": None,
            "ap_gap": None,
            "allocated_ap_gap": 0.0,
            "median_positive_rank": None,
            "median_query_size": None,
            "median_positive_rank_display": None,
            "median_positive_rank_percentile": None,
            "top_suppressor": "",
            "_positive_ranks": [],
            "_positive_query_sizes": [],
            "_positive_rank_percentiles": [],
            "_negative_above_counts": [],
        }

    sort_idx = np.lexsort((-z, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y[sort_idx].astype(np.float64)
    item_sorted = items[sort_idx]
    z_sorted = z[sort_idx]
    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])

    n_misordered_pairs = 0
    for qi in range(len(boundaries) - 1):
        s, e = boundaries[qi], boundaries[qi + 1]
        local_len = e - s
        orig = sort_idx[s:e]
        rank[orig] = np.arange(1, local_len + 1, dtype=np.int64)
        rank_pct[orig] = rank[orig] / float(local_len)

        yq = y_sorted[s:e]
        if yq.sum() == 0:
            continue

        ranks = np.arange(1, local_len + 1, dtype=np.float64)
        k_eff = float(mp["k"]) if mp["k"] is not None else float(local_len)
        cum = np.cumsum(yq)
        contrib = np.where(ranks <= k_eff, cum / ranks, 0.0)
        pos_recip_prefix = np.cumsum(
            np.where((yq == 1) & (ranks <= k_eff), 1.0 / ranks, 0.0)
        )
        pos_positions = np.flatnonzero(yq == 1)
        neg_positions = np.flatnonzero(yq == 0)

        for b in pos_positions:
            pos_orig = int(sort_idx[s + b])
            positive_item = str(item_sorted[b + s])
            tstat = target_stats[positive_item]
            tstat["_positive_ranks"].append(float(rank[pos_orig]))
            tstat["_positive_query_sizes"].append(float(local_len))
            tstat["_positive_rank_percentiles"].append(float(rank_pct[pos_orig]))

            above = neg_positions[neg_positions < b]
            if len(above) == 0:
                tstat["_negative_above_counts"].append(0)
                continue

            tstat["suppressed_positive_rows"] += 1
            tstat["total_suppression_pairs"] += int(len(above))
            tstat["_negative_above_counts"].append(int(len(above)))
            n_misordered_pairs += int(len(above))

            a_rank = above + 1.0
            new_contrib = np.where(
                a_rank <= k_eff,
                (cum[above] + 1.0) / a_rank,
                0.0,
            )
            intermediate_pos_gain = pos_recip_prefix[b - 1] - pos_recip_prefix[above]
            raw_severity = new_contrib - contrib[b] + intermediate_pos_gain
            raw_total_for_row = float(raw_severity.sum())
            row_ap_gap = max(0.0, 1.0 - float(contrib[b]))
            allocated_gap = (
                raw_severity / raw_total_for_row * row_ap_gap
                if raw_total_for_row > 0.0 and row_ap_gap > 0.0
                else np.zeros_like(raw_severity)
            )

            for a, raw_d, gap_d in zip(above, raw_severity, allocated_gap):
                sup_orig = int(sort_idx[s + a])
                suppressor_item = str(item_sorted[s + a])
                score_margin = float(z_sorted[s + a] - z_sorted[s + b])
                raw_d = float(raw_d)
                gap_d = float(gap_d)

                key = (positive_item, suppressor_item)
                pstat = pair_stats.setdefault(
                    key,
                    {
                        "positive_item": positive_item,
                        "suppressor_item": suppressor_item,
                        "affected_positive_rows": set(),
                        "allocated_ap_gap": 0.0,
                        "_score_margins": [],
                    },
                )
                pstat["affected_positive_rows"].add(pos_orig)
                pstat["allocated_ap_gap"] += gap_d
                pstat["_score_margins"].append(score_margin)

                sstat = suppressor_stats.setdefault(
                    suppressor_item,
                    {
                        "suppressor_item": suppressor_item,
                        "affected_positive_items": set(),
                        "affected_positive_rows": set(),
                        "allocated_ap_gap": 0.0,
                        "_score_margins": [],
                    },
                )
                sstat["affected_positive_items"].add(positive_item)
                sstat["affected_positive_rows"].add(pos_orig)
                sstat["allocated_ap_gap"] += gap_d
                sstat["_score_margins"].append(score_margin)
                tstat["allocated_ap_gap"] += gap_d

                if top_examples > 0:
                    example_seq += 1
                    example = {
                        "query": str(query_keys[pos_orig]),
                        "positive_item": positive_item,
                        "suppressor_item": suppressor_item,
                        "positive_score": float(z_sorted[s + b]),
                        "suppressor_score": float(z_sorted[s + a]),
                        "positive_rank": int(rank[pos_orig]),
                        "suppressor_rank": int(rank[sup_orig]),
                        "score_margin": score_margin,
                        "allocated_ap_gap": gap_d,
                    }
                    heap_key = (gap_d, raw_d, example_seq, example)
                    if len(example_heap) < top_examples:
                        heapq.heappush(example_heap, heap_key)
                    elif heap_key[:3] > example_heap[0][:3]:
                        heapq.heapreplace(example_heap, heap_key)

    pair_rows = []
    total_allocated_gap = float(sum(v["allocated_ap_gap"] for v in pair_stats.values()))
    allocated_gap_by_target = {
        item: float(v["allocated_ap_gap"])
        for item, v in target_stats.items()
    }

    for p in pair_stats.values():
        margins = p["_score_margins"]
        n_pos_target = target_stats[p["positive_item"]]["n_pos"]
        target_allocated_gap = allocated_gap_by_target.get(p["positive_item"], 0.0)
        pair_rows.append({
            "positive_item": p["positive_item"],
            "suppressor_item": p["suppressor_item"],
            "affected_positive_rows": int(len(p["affected_positive_rows"])),
            "affected_positive_rate": (
                None if n_pos_target == 0 else len(p["affected_positive_rows"]) / n_pos_target
            ),
            "mean_score_margin": _mean(margins),
            "median_score_margin": _pct(margins, 50),
            "allocated_ap_gap": float(p["allocated_ap_gap"]),
            "target_ap_gap_share": (
                None if target_allocated_gap <= 0.0 else float(p["allocated_ap_gap"]) / target_allocated_gap
            ),
            "overall_ap_gap_share": (
                None if total_allocated_gap <= 0.0 else float(p["allocated_ap_gap"]) / total_allocated_gap
            ),
        })
    pair_rows.sort(key=lambda r: (-float(r["allocated_ap_gap"]), r["positive_item"], r["suppressor_item"]))

    target_rows = []
    top_suppressors_by_target: list[dict[str, Any]] = []
    for item, t in target_stats.items():
        ranks = t.pop("_positive_ranks")
        query_sizes = t.pop("_positive_query_sizes")
        ranks_list = t.pop("_positive_rank_percentiles")
        neg_above = t.pop("_negative_above_counts")
        suppressed_positive_rate = (
            None if t["n_pos"] == 0 else t["suppressed_positive_rows"] / t["n_pos"]
        )
        mean_neg_above = _mean([float(x) for x in neg_above])
        ap_gap = None if t["ap"] is None else 1.0 - float(t["ap"])
        ap_gap_from_suppressors = (
            None if t["n_pos"] == 0 else float(t["allocated_ap_gap"]) / t["n_pos"]
        )
        unexplained_ap_gap = (
            None if ap_gap is None or ap_gap_from_suppressors is None
            else max(0.0, float(ap_gap) - float(ap_gap_from_suppressors))
        )
        overall_ap_gap_share = (
            None if total_allocated_gap <= 0.0 else float(t["allocated_ap_gap"]) / total_allocated_gap
        )
        median_rank = _pct(ranks, 50)
        median_query_size = _pct(query_sizes, 50)
        median_rank_display = _rank_display(
            median_rank,
            median_query_size,
        )
        median_rank_pct = _pct(ranks_list, 50)
        item_pairs = sorted(
            [p for p in pair_rows if p["positive_item"] == item],
            key=lambda r: -float(r["allocated_ap_gap"]),
        )
        top_suppressor = ""
        if item_pairs:
            top_suppressor = _suppressor_summary(item_pairs[0])
            for rank_i, p in enumerate(item_pairs[:3], start=1):
                top_suppressors_by_target.append({
                    "positive_item": item,
                    "suppressor_rank": rank_i,
                    "suppressor_item": p["suppressor_item"],
                    "target_ap_gap_share": p["target_ap_gap_share"],
                    "overall_ap_gap_share": p["overall_ap_gap_share"],
                    "affected_positive_rows": p["affected_positive_rows"],
                    "affected_positive_rate": p["affected_positive_rate"],
                    "mean_score_margin": p["mean_score_margin"],
                })
        target_rows.append({
            "positive_item": item,
            "ap": t["ap"],
            "ap_gap": ap_gap,
            "n_pos": t["n_pos"],
            "ap_gap_from_suppressors": ap_gap_from_suppressors,
            "unexplained_ap_gap": unexplained_ap_gap,
            "overall_ap_gap_share": overall_ap_gap_share,
            "suppressed_positive_rate": suppressed_positive_rate,
            "mean_negatives_above_positive": mean_neg_above,
            "median_positive_rank_display": median_rank_display,
            "median_positive_rank_percentile": median_rank_pct,
            "top_suppressor": top_suppressor,
        })
    target_rows.sort(key=lambda r: (
        -(0.0 if r["overall_ap_gap_share"] is None else float(r["overall_ap_gap_share"])),
        float("inf") if r["ap"] is None else float(r["ap"]),
        r["positive_item"],
    ))

    suppressor_rows = []
    for s in suppressor_stats.values():
        margins = s["_score_margins"]
        sup_pairs = sorted(
            [p for p in pair_rows if p["suppressor_item"] == s["suppressor_item"]],
            key=lambda r: -float(r["allocated_ap_gap"]),
        )
        suppressor_rows.append({
            "suppressor_item": s["suppressor_item"],
            "affected_positive_items": int(len(s["affected_positive_items"])),
            "affected_positive_rows": int(len(s["affected_positive_rows"])),
            "overall_ap_gap_share": (
                None if total_allocated_gap <= 0.0 else float(s["allocated_ap_gap"]) / total_allocated_gap
            ),
            "mean_score_margin": _mean(margins),
            "top_positive_items": _top_targets_summary(sup_pairs, 3),
        })
    suppressor_rows.sort(key=lambda r: (
        -(0.0 if r["overall_ap_gap_share"] is None else float(r["overall_ap_gap_share"])),
        r["suppressor_item"],
    ))

    examples = [x[3] for x in sorted(example_heap, key=lambda x: (-x[0], -x[1], x[2]))]

    target_gap_share_matrix: dict[str, dict[str, float]] = {}
    affected_rate_matrix: dict[str, dict[str, float]] = {}
    mean_margin_matrix: dict[str, dict[str, float]] = {}
    suppressor_target_gap_share_matrix: dict[str, dict[str, float]] = {}
    suppressor_allocated_gap = {
        str(s["suppressor_item"]): float(s["allocated_ap_gap"])
        for s in suppressor_stats.values()
    }
    for r in pair_rows:
        target_gap_share_matrix.setdefault(r["positive_item"], {})[r["suppressor_item"]] = float(
            r["target_ap_gap_share"] or 0.0
        )
        affected_rate_matrix.setdefault(r["positive_item"], {})[r["suppressor_item"]] = float(
            r["affected_positive_rate"] or 0.0
        )
        mean_margin_matrix.setdefault(r["positive_item"], {})[r["suppressor_item"]] = float(
            r["mean_score_margin"] or 0.0
        )
        sup_total = suppressor_allocated_gap.get(str(r["suppressor_item"]), 0.0)
        suppressor_target_gap_share_matrix.setdefault(r["suppressor_item"], {})[r["positive_item"]] = (
            0.0 if sup_total <= 0.0 else float(r["allocated_ap_gap"]) / sup_total
        )
    n_suppressed_positive_rows = int(sum(r["suppressed_positive_rows"] for r in target_stats.values()))

    logger.info(
        "Suppression ledger completed in %.1fs: pairs=%d allocated_ap_gap=%.6f",
        time.monotonic() - total_t0,
        n_misordered_pairs,
        total_allocated_gap,
    )
    return {
        "metric_params": mp,
        "score_col_used": SCORE_COL,
        "logit_notes": logit_notes,
        "n_rows": int(len(pdf)),
        "n_queries": int(len(boundaries) - 1),
        "n_entities": int(len(np.unique(clusters))),
        "n_items": int(len(np.unique(items))),
        "n_positive_rows": int(y.sum()),
        "n_suppressed_positive_rows": n_suppressed_positive_rows,
        "suppressed_positive_rate": (
            None if int(y.sum()) == 0 else n_suppressed_positive_rows / int(y.sum())
        ),
        "mean_negatives_above_positive": (
            None if int(y.sum()) == 0 else int(n_misordered_pairs) / int(y.sum())
        ),
        "n_misordered_pairs": int(n_misordered_pairs),
        "macro_per_item_map": macro_map,
        "total_ap_gap_allocated_to_suppressors": total_allocated_gap,
        "target_summary": target_rows,
        "top_suppressors_by_target": top_suppressors_by_target,
        "pair_ledger": pair_rows,
        "by_suppressor": suppressor_rows,
        "examples": examples,
        "matrices": {
            "target_gap_share": target_gap_share_matrix,
            "affected_positive_rate": affected_rate_matrix,
            "mean_logit_margin": mean_margin_matrix,
            "suppressor_target_gap_share": suppressor_target_gap_share_matrix,
        },
    }


def table_html(rows: list[dict], columns: list[tuple[str, str]], limit: int | None = None) -> str:
    shown = rows[:limit] if limit is not None else rows
    head = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    body = []
    for r in shown:
        tds = []
        for key, _ in columns:
            val = r.get(key)
            if val is None:
                text = ""
            elif isinstance(val, (int, float, np.integer, np.floating)):
                if key.endswith("_rate") or key.endswith("_share"):
                    text = f"{float(val) * 100:.1f}%"
                else:
                    text = fmt_num(val)
            else:
                text = str(val)
            tds.append(f"<td>{html.escape(text)}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def matrix_html(
    matrix: dict[str, dict[str, float]],
    target_rows: list[dict],
    suppressor_rows: list[dict],
    *,
    value_kind: str,
    limit: int = 30,
) -> str:
    target_order = [str(r["positive_item"]) for r in target_rows]
    victims = [v for v in target_order if v in matrix][:limit]
    suppressors = [
        str(r["suppressor_item"])
        for r in suppressor_rows
        if any(str(r["suppressor_item"]) in matrix.get(v, {}) for v in victims)
    ][:limit]
    if not victims or not suppressors:
        return "<p class='muted'>No suppression matrix to show.</p>"
    max_val = (
        max(abs(matrix.get(v, {}).get(s, 0.0)) for v in victims for s in suppressors)
        or 1.0
    )
    head = "<th>positive item</th>" + "".join(
        f"<th>{html.escape(s)}</th>" for s in suppressors
    )
    rows = []
    for v in victims:
        cells = [f"<td>{html.escape(v)}</td>"]
        for s in suppressors:
            val = float(matrix.get(v, {}).get(s, 0.0))
            alpha = min(1.0, abs(val) / max_val)
            bg = f"rgba(180,35,24,{0.08 + 0.65 * alpha:.3f})" if val > 0 else "#fff"
            text = f"{val * 100:.1f}%" if value_kind == "percent" else fmt_num(val)
            cells.append(
                f"<td style='background:{bg}' title='{html.escape(v)} suppressed by {html.escape(s)}'>{text}</td>"
            )
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def suppressor_distribution_matrix_html(
    matrix: dict[str, dict[str, float]],
    suppressor_rows: list[dict],
    target_rows: list[dict],
    *,
    limit: int = 30,
) -> str:
    suppressors = [
        str(r["suppressor_item"])
        for r in suppressor_rows
        if str(r["suppressor_item"]) in matrix
    ][:limit]
    positive_items = [
        str(r["positive_item"])
        for r in target_rows
        if any(str(r["positive_item"]) in matrix.get(s, {}) for s in suppressors)
    ][:limit]
    if not suppressors or not positive_items:
        return "<p class='muted'>No suppressor distribution matrix to show.</p>"

    max_val = (
        max(matrix.get(s, {}).get(p, 0.0) for s in suppressors for p in positive_items)
        or 1.0
    )
    head = "<th>suppressor item</th>" + "".join(
        f"<th>{html.escape(p)}</th>" for p in positive_items
    )
    rows = []
    for s in suppressors:
        cells = [f"<td>{html.escape(s)}</td>"]
        for p in positive_items:
            val = float(matrix.get(s, {}).get(p, 0.0))
            alpha = min(1.0, val / max_val)
            bg = f"rgba(180,35,24,{0.08 + 0.65 * alpha:.3f})" if val > 0 else "#fff"
            cells.append(
                f"<td style='background:{bg}' title='{html.escape(s)} affects {html.escape(p)}'>{val * 100:.1f}%</td>"
            )
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def percentage_glossary_html() -> str:
    rows = [
        {
            "field": "summary: suppressed positives",
            "numerator": "positive rows with at least one negative item ranked above",
            "denominator": "all positive rows",
        },
        {
            "field": "Target Summary: overall gap share",
            "numerator": "this positive item's AP gap attributed to suppressors",
            "denominator": "all items' AP gap attributed to suppressors",
        },
        {
            "field": "Target Summary: suppressed pos / n_pos",
            "numerator": "this item's positive rows with at least one negative item ranked above",
            "denominator": "this item's positive rows",
        },
        {
            "field": "Matrix: target gap share",
            "numerator": "this suppressor's attributed AP gap for this positive item",
            "denominator": "all suppressor-attributed AP gap for this positive item",
        },
        {
            "field": "Matrix: affected pos / n_pos",
            "numerator": "this item's positive rows affected by this suppressor",
            "denominator": "this item's positive rows",
        },
        {
            "field": "Suppressor Summary: overall gap share",
            "numerator": "this suppressor's attributed AP gap across all positive items",
            "denominator": "all suppressor-attributed AP gap",
        },
        {
            "field": "Suppressor Distribution Matrix cell",
            "numerator": "this suppressor's attributed AP gap for the column positive item",
            "denominator": "this suppressor's attributed AP gap across all positive items",
        },
    ]
    return table_html(rows, [
        ("field", "field"),
        ("numerator", "numerator"),
        ("denominator", "denominator"),
    ])


def render_html(report: dict) -> str:
    result = report["result"]
    source = report["source"]
    notes = report.get("notes", []) + result.get("logit_notes", [])
    notes_html = "".join(f"<li>{html.escape(n)}</li>" for n in notes)
    css = """
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;color:#1f2933;background:#fbfcfe}
    h1{font-size:28px;margin-bottom:4px} h2{margin-top:32px;font-size:20px} h3{margin-top:20px;font-size:16px}
    .muted{color:#697386}.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin:20px 0}
    .card{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:14px}
    .big{font-size:24px;font-weight:700}
    table{border-collapse:collapse;width:100%;background:#fff;margin:12px 0;border:1px solid #d9e2ec}
    th,td{padding:8px 10px;border-bottom:1px solid #edf2f7;text-align:right;font-size:13px;vertical-align:top}
    th:first-child,td:first-child{text-align:left} th{background:#f4f7fb;color:#344054}
    code{background:#eef2f7;padding:1px 4px;border-radius:4px}.note{background:#fff8e6;border:1px solid #f6d365;border-radius:8px;padding:12px}
    """
    summary_cards = [
        ("macro per-item mAP", fmt_num(result["macro_per_item_map"])),
        ("suppressed positives / positives", f"{result['n_suppressed_positive_rows']} / {result['n_positive_rows']} ({float(result['suppressed_positive_rate'] or 0.0) * 100:.1f}%)"),
        ("avg negatives above positive", fmt_num(result["mean_negatives_above_positive"])),
        ("rows / queries", f"{result['n_rows']} / {result['n_queries']}"),
        ("items", str(result["n_items"])),
        ("positive rows", str(result["n_positive_rows"])),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='muted'>{html.escape(k)}</div><div class='big'>{html.escape(v)}</div></div>"
        for k, v in summary_cards
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Suppression Ledger Diagnosis</title><style>{css}</style></head>
<body>
<h1>Suppression Ledger Diagnosis</h1>
<p class="muted">Question: which negative items rank above each positive item inside the same query?</p>
<div class="summary">{cards_html}</div>
<div class="note"><strong>Main read.</strong> Start with Target Item Summary. AP gap says how far an item is from perfect AP; AP gap from suppressors says how much of that gap is explained by label=0 items ranked above its positives. Top suppressors tells you who to inspect first.</div>

<h2>Input</h2>
<table><tbody>
<tr><th>source</th><td>{html.escape(str(source.get("source")))}</td></tr>
<tr><th>catalog entry</th><td><code>{html.escape(str(source.get("catalog_entry")))}</code></td></tr>
<tr><th>score column</th><td><code>{html.escape(result["score_col_used"])}</code></td></tr>
<tr><th>metric k</th><td>{html.escape("none" if result["metric_params"]["k"] is None else str(result["metric_params"]["k"]))}</td></tr>
</tbody></table>
<p class="muted">Scores are <code>logit(score_uncalibrated)</code>. A suppressor is counted only when it is label=0 and ranked above a label=1 row in the same query.</p>
{"<ul>" + notes_html + "</ul>" if notes_html else ""}

<h2>Percentage Glossary</h2>
<p class="muted">Every percentage in this report is a ratio. The table below names the numerator and denominator so the reading frame stays fixed.</p>
{percentage_glossary_html()}

<h2>1. Target Item Summary</h2>
<p class="muted">This is the main table. Use it to decide whether the problem is broad ranking suppression and which item to investigate first.</p>
{table_html(result["target_summary"], [("positive_item","positive item"),("ap","AP"),("ap_gap","AP gap"),("ap_gap_from_suppressors","AP gap from suppressors"),("unexplained_ap_gap","unexplained AP gap"),("overall_ap_gap_share","overall gap share"),("n_pos","n_pos"),("suppressed_positive_rate","suppressed pos / n_pos"),("mean_negatives_above_positive","mean neg above"),("median_positive_rank_display","median pos rank"),("top_suppressor","top suppressor")])}

<h2>2. Suppression Matrices</h2>
<p class="muted">All three matrices use the same row and column order. Rows are positive items, columns are negative suppressor items. Compare the same cell across matrices to separate loss share, frequency, and score strength.</p>
<h3>2a. Target Gap Share Matrix</h3>
<p class="muted">Cell = this suppressor's share of the row item's suppressor-attributed AP gap. Use this as the primary answer to who suppresses whom.</p>
{matrix_html(result["matrices"]["target_gap_share"], result["target_summary"], result["by_suppressor"], value_kind="percent")}

<h3>2b. Affected Positive Rate Matrix</h3>
<p class="muted">Cell = positive rows for the row item affected by this suppressor / positive rows for the row item. Use this to see whether the suppressor is frequent or concentrated in fewer examples.</p>
{matrix_html(result["matrices"]["affected_positive_rate"], result["target_summary"], result["by_suppressor"], value_kind="percent")}

<h3>2c. Mean Logit Margin Matrix</h3>
<p class="muted">Cell = mean logit(score_uncalibrated suppressor) - logit(score_uncalibrated positive) for affected rows. Use this as a score-strength cue, not as the ranking priority.</p>
{matrix_html(result["matrices"]["mean_logit_margin"], result["target_summary"], result["by_suppressor"], value_kind="number")}

<h2>3. Suppressor Perspective</h2>
<p class="muted">This reverses the view: for each suppressor item, where does its attributed AP gap go?</p>
<h3>3a. Suppressor Summary Table</h3>
<p class="muted">Overall gap share ranks suppressors globally. Top positive items names the largest affected targets for quick scanning; use the matrix below for the full distribution.</p>
{table_html(result["by_suppressor"], [("suppressor_item","suppressor item"),("overall_ap_gap_share","overall gap share"),("affected_positive_items","affected items"),("affected_positive_rows","affected pos rows"),("mean_score_margin","mean margin"),("top_positive_items","top positive items")])}

<h3>3b. Suppressor Distribution Matrix</h3>
<p class="muted">Rows are suppressor items. Columns are positive items. Cell = this positive item's share of the row suppressor's attributed AP gap.</p>
{suppressor_distribution_matrix_html(result["matrices"]["suppressor_target_gap_share"], result["by_suppressor"], result["target_summary"])}

<h2>4. Audit Examples</h2>
<p class="muted">Concrete same-query rows for sanity checking. Use these examples to verify the ledger points at real ranking failures; do not use them as aggregate evidence.</p>
{table_html(result["examples"], [("query","query"),("positive_item","positive item"),("suppressor_item","suppressor item"),("positive_rank","positive rank"),("suppressor_rank","suppressor rank"),("positive_score","positive score"),("suppressor_score","suppressor score"),("score_margin","margin")])}
</body></html>"""


def write_outputs(report: dict, output: str) -> None:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Writing HTML report to %s", out)
    out.write_text(render_html(report), encoding="utf-8")
    json_path = out.with_suffix(".json")
    logger.info("Writing JSON report to %s", json_path)
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
    p.add_argument("--output", default="data/diagnosis/suppression_ledger.html")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--top-examples", type=int, default=50)
    p.add_argument("--sample-max-queries", type=int, default=None, help="Override Spark-side diagnosis sample max_queries for faster smoke tests.")
    p.add_argument("--max-queries", type=int, default=None, help="Optional pandas-side query cap after loading the diagnosis sample.")
    return p.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    logger.info("Starting suppression ledger diagnosis")
    parameters = load_parameters(args.params)
    schema = get_schema(parameters)
    pdf_raw, source_meta = load_enriched_eval_predictions(args, parameters, schema)
    pdf, notes = validate_and_prepare(pdf_raw, parameters, schema)
    pdf, cap_meta = maybe_cap_queries(pdf, schema, args.max_queries, args.seed)
    result = analyze_suppression(
        pdf,
        parameters,
        schema,
        top_examples=args.top_examples,
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
    print(
        "suppressed positives = "
        f"{result['n_suppressed_positive_rows']} "
        f"({float(result['suppressed_positive_rate'] or 0.0) * 100:.1f}%), "
        f"mean negatives above positive = {result['mean_negatives_above_positive']:.6f}"
    )


if __name__ == "__main__":
    main()
