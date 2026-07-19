"""Manual diagnosis: did the model spend capacity on per-item personalization?

This standalone script reads the training-side Gain ledger and turns it into a
focused model-capacity report. It answers one narrow model-behavior question:

    Is the model mostly learning item prior, or does each item receive useful
    context-feature capacity to separate buyers from non-buyers?

The main input is:

    data/models/{model_version}/diagnostics/gain_ledger.json

When available, the script also reads:

    data/models/{model_version}/evaluation_results.json
    data/diagnosis/item_ability.json

The ability file is optional. If its model_version matches, query-centered
within-item AUC is shown next to the capacity ledger as context, but the report
does not emit verdicts or prescriptive actions.

Examples:

  PYTHONPATH=src python scripts/model_capacity_diagnosis.py \
      --model-version 20260717_xxx \
      --output data/diagnosis/model_capacity.html
"""

from __future__ import annotations

import argparse
import html
import json
import math
from pathlib import Path
from typing import Any


def load_json(path: Path, *, required: bool) -> dict:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required JSON file not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def parse_lightgbm_total_split_count(path: Path) -> int | None:
    """Return total internal split nodes from a LightGBM text model.

    LightGBM stores ``num_leaves`` per tree; a binary tree with L leaves has
    L - 1 split nodes. This gives true booster split count without importing
    LightGBM.
    """
    if not path.exists():
        return None
    total = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("num_leaves="):
                try:
                    total += int(line.split("=", 1)[1].strip()) - 1
                except ValueError:
                    return None
    return total


def fmt_num(x: Any, digits: int = 4) -> str:
    if x is None:
        return ""
    try:
        v = float(x)
        if not math.isfinite(v):
            return ""
        return f"{v:.{digits}f}"
    except Exception:
        return str(x)


def fmt_pct(x: Any, digits: int = 1) -> str:
    if x is None:
        return ""
    try:
        v = float(x)
        if not math.isfinite(v):
            return ""
        return f"{v * 100:.{digits}f}%"
    except Exception:
        return str(x)


def _model_version_from_ability(payload: dict) -> str | None:
    source = payload.get("source", {}) or {}
    return source.get("model_version")


def ability_by_item(payload: dict, model_version: str, notes: list[str]) -> dict[str, dict]:
    if not payload:
        notes.append("item_ability.json not found; ability columns are omitted.")
        return {}
    seen_mv = _model_version_from_ability(payload)
    if seen_mv and str(seen_mv) != str(model_version):
        notes.append(
            "item_ability.json ignored because model_version does not match: "
            f"{seen_mv} != {model_version}."
        )
        return {}
    rows = ((payload.get("result", {}) or {}).get("per_item", []) or [])
    return {str(r.get("item")): r for r in rows if r.get("item") is not None}


def evaluation_ap_by_item(payload: dict) -> tuple[dict[str, float], float | None]:
    if not payload:
        return {}, None
    block = payload.get("uncalibrated") or payload
    per_item = block.get("per_item_map_attr", {}) or {}
    overall = block.get("overall_map")
    return {str(k): float(v) for k, v in per_item.items()}, (
        None if overall is None else float(overall)
    )


def global_context_feature_rows(
    feature_importance: dict,
    *,
    item_feature: str | None,
    total_gain: float | None,
) -> tuple[list[dict], dict]:
    ranked = feature_importance.get("ranked") or []
    if not isinstance(ranked, list) or not ranked:
        return [], {}
    item_feature = item_feature or ""
    non_item = [r for r in ranked if str(r.get("feature")) != item_feature]
    non_item_gain = sum(float(r.get("gain") or 0.0) for r in non_item)
    non_item_splits = sum(float(r.get("split") or 0.0) for r in non_item)
    rows = []
    for r in non_item:
        gain = float(r.get("gain") or 0.0)
        split = float(r.get("split") or 0.0)
        rows.append({
            "feature": str(r.get("feature")),
            "split": int(split),
            "gain": gain,
            "gain_per_split": None if split <= 0.0 else gain / split,
            "gain_share_total_model": (
                None if total_gain in (None, 0.0) else gain / float(total_gain)
            ),
            "gain_share_non_item_features": (
                None if non_item_gain <= 0.0 else gain / non_item_gain
            ),
            "split_share_non_item_features": (
                None if non_item_splits <= 0.0 else split / non_item_splits
            ),
        })
    rows.sort(key=lambda r: float(r["gain"]), reverse=True)
    return rows, {
        "n_non_item_features": len(rows),
        "non_item_feature_gain_sum": non_item_gain,
        "non_item_feature_split_sum": int(non_item_splits),
        "top3_non_item_feature_gain_share": (
            None
            if non_item_gain <= 0.0
            else sum(float(r["gain"]) for r in rows[:3]) / non_item_gain
        ),
    }


def build_capacity_rows(
    gain_ledger: dict,
    ap_by_item: dict[str, float],
    ability: dict[str, dict],
) -> list[dict]:
    per_item = gain_ledger.get("per_item") or {}
    if not isinstance(per_item, dict) or not per_item:
        return []

    allocated_context_gain_sum = sum(
        float(v.get("context_gain") or 0.0) for v in per_item.values()
    )
    allocated_context_split_sum = sum(
        int(v.get("context_split_count") or 0) for v in per_item.values()
    )
    gain_shares = [
        (float(v.get("context_gain") or 0.0) / allocated_context_gain_sum)
        for v in per_item.values()
        if allocated_context_gain_sum > 0.0
    ]
    split_shares = [
        (int(v.get("context_split_count") or 0) / allocated_context_split_sum)
        for v in per_item.values()
        if allocated_context_split_sum > 0
    ]
    max_share = max(gain_shares) if gain_shares else 0.0
    median_share = sorted(gain_shares)[len(gain_shares) // 2] if gain_shares else 0.0

    rows: list[dict] = []
    for item, led in per_item.items():
        item = str(item)
        context_gain = led.get("context_gain")
        context_splits = int(led.get("context_split_count") or 0)
        gain_share = (
            None
            if allocated_context_gain_sum <= 0.0
            else float(context_gain or 0.0) / allocated_context_gain_sum
        )
        split_share = (
            None
            if allocated_context_split_sum <= 0
            else float(context_splits) / float(allocated_context_split_sum)
        )
        context_gain_per_split = (
            None
            if context_gain is None or context_splits <= 0
            else float(context_gain) / float(context_splits)
        )
        trees = led.get("trees_touched") or []
        ab = ability.get(item, {})
        row = {
            "item": item,
            "ap": ap_by_item.get(item, ab.get("ap")),
            "n_pos": ab.get("n_pos"),
            "query_centered_auc": ab.get("query_centered_auc"),
            "auc_ci": (
                ""
                if ab.get("query_centered_auc_ci_low") is None
                else f"[{fmt_num(ab.get('query_centered_auc_ci_low'))}, "
                f"{fmt_num(ab.get('query_centered_auc_ci_high'))}]"
            ),
            "context_gain_share_allocated": gain_share,
            "context_split_share_allocated": split_share,
            "context_share_vs_max": (
                None if gain_share is None or max_share <= 0.0 else gain_share / max_share
            ),
            "context_share_vs_median": (
                None if gain_share is None or median_share <= 0.0 else gain_share / median_share
            ),
            "context_gain": None if context_gain is None else float(context_gain),
            "context_split_count": context_splits,
            "context_gain_per_split": context_gain_per_split,
            "isolating_split_count": int(led.get("isolating_split_count") or 0),
            "context_gain_isolated": led.get("context_gain_isolated"),
            "first_tree_index": led.get("first_tree_index"),
            "n_trees_touched": len(trees) if isinstance(trees, list) else None,
        }
        rows.append(row)

    by_capacity = sorted(
        [r for r in rows if r.get("context_gain_share_allocated") is not None],
        key=lambda r: float(r["context_gain_share_allocated"]),
        reverse=True,
    )
    for rank, row in enumerate(by_capacity, start=1):
        row["context_capacity_rank"] = rank
    by_ap = sorted(
        [r for r in rows if r.get("ap") is not None],
        key=lambda r: float(r["ap"]),
        reverse=True,
    )
    for rank, row in enumerate(by_ap, start=1):
        row["ap_rank"] = rank
    by_auc = sorted(
        [r for r in rows if r.get("query_centered_auc") is not None],
        key=lambda r: float(r["query_centered_auc"]),
        reverse=True,
    )
    for rank, row in enumerate(by_auc, start=1):
        row["auc_rank"] = rank

    rows.sort(
        key=lambda r: (
            float("inf")
            if r.get("context_capacity_rank") is None
            else int(r["context_capacity_rank"]),
            str(r["item"]),
        )
    )
    return rows


def summarize(
    gain_ledger: dict,
    rows: list[dict],
    overall_map: float | None,
    *,
    total_split_count: int | None,
) -> dict:
    item_id = gain_ledger.get("item_id") or {}
    context = gain_ledger.get("context") or {}
    item_gain = item_id.get("gain_sum")
    context_gain = context.get("gain_sum") if isinstance(context, dict) else None
    item_splits = item_id.get("split_count")
    context_splits = context.get("split_count") if isinstance(context, dict) else None
    accounted_splits = (
        None
        if item_splits is None or context_splits is None
        else int(item_splits) + int(context_splits)
    )
    other_splits = (
        None
        if total_split_count is None or accounted_splits is None
        else int(total_split_count) - int(accounted_splits)
    )
    ratio = (
        None
        if item_gain is None or context_gain in (None, 0)
        else float(item_gain) / float(context_gain)
    )
    gain_share_rows = [r for r in rows if r.get("context_gain_share_allocated") is not None]
    split_share_rows = [r for r in rows if r.get("context_split_share_allocated") is not None]
    gain_shares = sorted(float(r["context_gain_share_allocated"]) for r in gain_share_rows)
    split_shares = sorted(float(r["context_split_share_allocated"]) for r in split_share_rows)
    top_rows = sorted(
        gain_share_rows,
        key=lambda r: float(r["context_gain_share_allocated"]),
        reverse=True,
    )
    top_split_rows = sorted(
        split_share_rows,
        key=lambda r: float(r["context_split_share_allocated"]),
        reverse=True,
    )
    bottom_rows = sorted(
        gain_share_rows,
        key=lambda r: float(r["context_gain_share_allocated"]),
    )
    max_share = gain_shares[-1] if gain_shares else None
    min_share = gain_shares[0] if gain_shares else None
    p25 = gain_shares[len(gain_shares) // 4] if gain_shares else None
    p50 = gain_shares[len(gain_shares) // 2] if gain_shares else None
    p75 = gain_shares[(len(gain_shares) * 3) // 4] if gain_shares else None
    split_p25 = split_shares[len(split_shares) // 4] if split_shares else None
    split_p50 = split_shares[len(split_shares) // 2] if split_shares else None
    split_p75 = split_shares[(len(split_shares) * 3) // 4] if split_shares else None
    allocated_context_gain_sum = sum(float(r.get("context_gain") or 0.0) for r in rows)
    allocated_context_split_sum = sum(int(r.get("context_split_count") or 0) for r in rows)
    top3_gain_share = (
        sum(float(r["context_gain_share_allocated"]) for r in top_rows[:3])
        if top_rows
        else None
    )
    top3_split_share = (
        sum(float(r["context_split_share_allocated"]) for r in top_split_rows[:3])
        if top_split_rows
        else None
    )
    return {
        "overall_map_uncalibrated": overall_map,
        "n_items": len(rows),
        "n_trees": gain_ledger.get("n_trees"),
        "total_gain": gain_ledger.get("total_gain"),
        "item_id_gain_share": item_id.get("gain_share"),
        "item_id_gain_sum": item_gain,
        "item_id_split_count": item_splits,
        "context_gain_share": context.get("gain_share") if isinstance(context, dict) else None,
        "context_gain_sum": context_gain,
        "context_split_count": context_splits,
        "accounted_split_count": accounted_splits,
        "total_split_count": total_split_count,
        "other_split_count": other_splits,
        "item_id_split_share_accounted": (
            None
            if accounted_splits in (None, 0) or item_splits is None
            else float(item_splits) / float(accounted_splits)
        ),
        "context_split_share_accounted": (
            None
            if accounted_splits in (None, 0) or context_splits is None
            else float(context_splits) / float(accounted_splits)
        ),
        "item_id_split_share_total": (
            None
            if total_split_count in (None, 0) or item_splits is None
            else float(item_splits) / float(total_split_count)
        ),
        "context_split_share_total": (
            None
            if total_split_count in (None, 0) or context_splits is None
            else float(context_splits) / float(total_split_count)
        ),
        "other_split_share_total": (
            None
            if total_split_count in (None, 0) or other_splits is None
            else float(other_splits) / float(total_split_count)
        ),
        "unaccounted_gain_share": (
            None
            if item_id.get("gain_share") is None or not isinstance(context, dict) or context.get("gain_share") is None
            else 1.0 - float(item_id["gain_share"]) - float(context["gain_share"])
        ),
        "prior_to_personalization_gain_ratio": ratio,
        "allocated_context_gain_sum": allocated_context_gain_sum,
        "allocated_context_split_sum": allocated_context_split_sum,
        "allocated_to_global_context_gain_ratio": (
            None if context_gain in (None, 0.0) else allocated_context_gain_sum / float(context_gain)
        ),
        "allocated_to_global_context_split_ratio": (
            None
            if context_splits in (None, 0)
            else allocated_context_split_sum / float(context_splits)
        ),
        "context_gain_alloc_share_min": min_share,
        "context_gain_alloc_share_p25": p25,
        "context_gain_alloc_share_p50": p50,
        "context_gain_alloc_share_p75": p75,
        "context_gain_alloc_share_max": max_share,
        "context_split_alloc_share_min": split_shares[0] if split_shares else None,
        "context_split_alloc_share_p25": split_p25,
        "context_split_alloc_share_p50": split_p50,
        "context_split_alloc_share_p75": split_p75,
        "context_split_alloc_share_max": split_shares[-1] if split_shares else None,
        "max_to_min_context_share_ratio": (
            None if min_share in (None, 0.0) or max_share is None else max_share / min_share
        ),
        "top3_context_gain_alloc_share": top3_gain_share,
        "top3_context_split_alloc_share": top3_split_share,
        "top_context_items": [
            {
                "item": r["item"],
                "context_gain_share_allocated": r.get("context_gain_share_allocated"),
                "context_split_share_allocated": r.get("context_split_share_allocated"),
                "ap": r.get("ap"),
                "query_centered_auc": r.get("query_centered_auc"),
            }
            for r in top_rows[:3]
        ],
        "bottom_context_items": [
            {
                "item": r["item"],
                "context_gain_share_allocated": r.get("context_gain_share_allocated"),
                "context_split_share_allocated": r.get("context_split_share_allocated"),
                "ap": r.get("ap"),
                "query_centered_auc": r.get("query_centered_auc"),
            }
            for r in bottom_rows[:3]
        ],
    }


def interpretation(summary: dict) -> str:
    prior = summary.get("item_id_gain_share")
    context = summary.get("context_gain_share")
    max_ratio = summary.get("max_to_min_context_share_ratio")
    top = summary.get("top_context_items") or []
    bottom = summary.get("bottom_context_items") or []
    top_names = ", ".join(str(r["item"]) for r in top) or "none"
    bottom_names = ", ".join(str(r["item"]) for r in bottom) or "none"
    prior_text = fmt_pct(prior)
    context_text = fmt_pct(context)
    ratio_text = fmt_num(max_ratio)
    return (
        f"全模型 split Gain 中，Item Prior 占 {prior_text}，Post-Item Context 占 "
        f"{context_text}；per-item allocated Post-Item Context Gain share 的 max/min 比為 "
        f"{ratio_text}。allocation share 最高的 items 是 {top_names}，最低的是 "
        f"{bottom_names}。"
    )


def table_html(rows: list[dict], columns: list[tuple[str, str]], limit: int | None = None) -> str:
    def is_pct_key(key: str) -> bool:
        return (
            key.endswith("_share")
            or key.endswith("_share_total")
            or key.endswith("_share_accounted")
            or key.endswith("_share_allocated")
            or key.startswith("context_gain_alloc_share")
            or key.startswith("context_split_alloc_share")
            or key
            in {
                "context_share_vs_max",
                "context_share_vs_median",
                "top3_context_gain_alloc_share",
                "top3_context_split_alloc_share",
                "gain_share_total_model",
                "gain_share_non_item_features",
                "split_share_non_item_features",
                "top3_non_item_feature_gain_share",
            }
        )

    shown = rows[:limit] if limit is not None else rows
    head = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    body = []
    for r in shown:
        tds = []
        for key, _ in columns:
            val = r.get(key)
            if is_pct_key(key):
                text = fmt_pct(val)
            elif isinstance(val, int):
                text = str(val)
            elif isinstance(val, float):
                text = fmt_num(val)
            else:
                text = "" if val is None else str(val)
            cls = " class='left'" if key in ("item", "feature") else ""
            tds.append(f"<td{cls}>{html.escape(text)}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def svg_capacity_bars(rows: list[dict], *, width: int = 900, row_h: int = 30) -> str:
    pts = [r for r in rows if r.get("context_gain_share_allocated") is not None]
    pts = sorted(pts, key=lambda r: float(r["context_gain_share_allocated"]), reverse=True)
    if not pts:
        return "<p class='muted'>No capacity rows.</p>"
    pad_l, pad_r, pad_t, pad_b = 180, 90, 18, 34
    height = pad_t + pad_b + row_h * len(pts)
    plot_w = width - pad_l - pad_r
    max_share = max(float(r["context_gain_share_allocated"]) for r in pts) or 1.0
    parts = [
        f"<svg viewBox='0 0 {width} {height}' width='100%' role='img'>",
        f"<rect x='0' y='0' width='{width}' height='{height}' fill='#fff'/>",
    ]
    for tick in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = pad_l + tick * plot_w
        parts.append(
            f"<line x1='{x:.1f}' y1='{pad_t}' x2='{x:.1f}' y2='{height - pad_b}' stroke='#edf2f7'/>"
        )
        parts.append(
            f"<text x='{x:.1f}' y='{height - 12}' text-anchor='middle' font-size='11' fill='#697386'>{fmt_pct(tick * max_share)}</text>"
        )
    for i, r in enumerate(pts):
        y = pad_t + i * row_h + 7
        share = float(r["context_gain_share_allocated"])
        bar_w = share / max_share * plot_w
        color = "#0f766e"
        if r.get("context_capacity_rank") in (1, 2, 3):
            color = "#2563eb"
        elif r.get("context_capacity_rank") and int(r["context_capacity_rank"]) > max(0, len(pts) - 3):
            color = "#9a6700"
        parts.append(
            f"<text x='{pad_l - 10}' y='{y + 12:.1f}' text-anchor='end' font-size='12' fill='#344054'>{html.escape(str(r['item']))}</text>"
        )
        parts.append(
            f"<rect x='{pad_l}' y='{y:.1f}' width='{bar_w:.1f}' height='16' rx='3' fill='{color}' fill-opacity='0.82'>"
            f"<title>{html.escape(str(r['item']))}: allocated Post-Item Context Gain / sum allocated Gain={fmt_pct(share)}, rank={r.get('context_capacity_rank')}</title></rect>"
        )
        parts.append(
            f"<text x='{pad_l + bar_w + 8:.1f}' y='{y + 12:.1f}' font-size='12' fill='#344054'>{fmt_pct(share)}</text>"
        )
    parts.append(
        f"<text x='{pad_l + plot_w / 2:.1f}' y='{height - 1}' text-anchor='middle' font-size='13' fill='#344054'>allocated Post-Item Context Gain / sum allocated Gain</text>"
    )
    parts.append("</svg>")
    return "".join(parts)


def svg_capacity_vs_ability(rows: list[dict], *, width: int = 900, height: int = 430) -> str:
    pts = [
        r for r in rows
        if r.get("context_gain_share_allocated") is not None
        and r.get("query_centered_auc") is not None
    ]
    if not pts:
        return "<p class='muted'>No ability rows available. Run item_ability_diagnosis.py first to enable this plot.</p>"
    pad_l, pad_r, pad_t, pad_b = 74, 24, 24, 58
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    max_share = max(float(r["context_gain_share_allocated"]) for r in pts) or 1.0

    def sx(x: float) -> float:
        return pad_l + x / max_share * plot_w

    def sy(y: float) -> float:
        return pad_t + (1.0 - y) * plot_h

    parts = [
        f"<svg viewBox='0 0 {width} {height}' width='100%' role='img'>",
        f"<rect x='0' y='0' width='{width}' height='{height}' fill='#fff'/>",
    ]
    for tick in (0.0, 0.25, 0.5, 0.75, 1.0):
        x = pad_l + tick * plot_w
        parts.append(f"<line x1='{x:.1f}' y1='{pad_t}' x2='{x:.1f}' y2='{height - pad_b}' stroke='#edf2f7'/>")
        parts.append(f"<text x='{x:.1f}' y='{height - 32}' text-anchor='middle' font-size='11' fill='#697386'>{fmt_pct(tick * max_share)}</text>")
        y = sy(tick)
        parts.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{pad_l + plot_w}' y2='{y:.1f}' stroke='#edf2f7'/>")
        parts.append(f"<text x='{pad_l - 12}' y='{y + 4:.1f}' text-anchor='end' font-size='11' fill='#697386'>{tick:.2f}</text>")
    for r in pts:
        x = sx(float(r["context_gain_share_allocated"]))
        y = sy(float(r["query_centered_auc"]))
        radius = min(14.0, 4.5 + math.log1p(float(r.get("n_pos") or 1)) * 1.15)
        color = "#0f766e"
        if r.get("context_capacity_rank") in (1, 2, 3):
            color = "#2563eb"
        elif r.get("context_capacity_rank") and int(r["context_capacity_rank"]) > max(0, len(pts) - 3):
            color = "#9a6700"
        label = (
            f"{r['item']}: allocated Post-Item Context Gain / sum allocated Gain={fmt_pct(r['context_gain_share_allocated'])}, "
            f"AUC={fmt_num(r['query_centered_auc'])}, capacity_rank={r.get('context_capacity_rank')}"
        )
        parts.append(
            f"<circle cx='{x:.1f}' cy='{y:.1f}' r='{radius:.1f}' fill='{color}' fill-opacity='0.75'>"
            f"<title>{html.escape(label)}</title></circle>"
        )
    parts.append(f"<line x1='{pad_l}' y1='{height - pad_b}' x2='{pad_l + plot_w}' y2='{height - pad_b}' stroke='#98a2b3'/>")
    parts.append(f"<line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height - pad_b}' stroke='#98a2b3'/>")
    parts.append(f"<text x='{pad_l + plot_w / 2:.1f}' y='{height - 8}' text-anchor='middle' font-size='13' fill='#344054'>allocated Post-Item Context Gain / sum allocated Gain</text>")
    parts.append(f"<text transform='translate(18 {pad_t + plot_h / 2:.1f}) rotate(-90)' text-anchor='middle' font-size='13' fill='#344054'>query-centered AUC</text>")
    parts.append("</svg>")
    return "".join(parts)


def render_html(report: dict) -> str:
    summary = report["summary"]
    rows = report["per_item"]
    notes = report.get("notes", [])
    notes_html = "".join(f"<li>{html.escape(n)}</li>" for n in notes)

    css = """
    body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;color:#1f2933;background:#fbfcfe}
    h1{font-size:28px;margin-bottom:4px} h2{margin-top:32px;font-size:20px}
    .muted{color:#697386}.summary{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin:20px 0}
    .card{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:14px}.big{font-size:24px;font-weight:700}
    .note{background:#fff8e6;border:1px solid #f6d365;border-radius:8px;padding:12px}
    .plot{background:#fff;border:1px solid #d9e2ec;border-radius:8px;padding:10px;margin:12px 0}
    table{border-collapse:collapse;width:100%;background:#fff;margin:12px 0;border:1px solid #d9e2ec}
    th,td{padding:8px 10px;border-bottom:1px solid #edf2f7;text-align:right;font-size:13px;vertical-align:top}
    th:first-child,td:first-child{text-align:left} th{background:#f4f7fb;color:#344054}.left{text-align:left}
    code{background:#eef2f7;padding:1px 4px;border-radius:4px}
    """
    cards = [
        ("uncalibrated mAP", fmt_num(summary["overall_map_uncalibrated"])),
        ("Item Prior Gain / Total", fmt_pct(summary["item_id_gain_share"])),
        ("Post-Item Context Gain / Total", fmt_pct(summary["context_gain_share"])),
        ("Pre-Item / Unassigned Gain / Total", fmt_pct(summary["unaccounted_gain_share"])),
        ("Item Prior splits / Total", fmt_pct(summary["item_id_split_share_total"])),
        ("Post-Item Context splits / Total", fmt_pct(summary["context_split_share_total"])),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='muted'>{html.escape(k)}</div><div class='big'>{html.escape(v)}</div></div>"
        for k, v in cards
    )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Model Capacity Diagnosis</title><style>{css}</style></head>
<body>
<h1>Model Capacity Diagnosis</h1>
<p class="muted">Question: how is model split Gain divided among item prior, post-item context, and pre-item / unassigned context?</p>
<div class="summary">{cards_html}</div>
<div class="note"><strong>Ledger snapshot.</strong> {html.escape(interpretation(summary))}</div>

<h2>Input</h2>
<table><tbody>
<tr><th>model_version</th><td><code>{html.escape(str(report["model_version"]))}</code></td></tr>
<tr><th>gain ledger</th><td><code>{html.escape(str(report["paths"]["gain_ledger"]))}</code></td></tr>
<tr><th>model text</th><td><code>{html.escape(str(report["paths"].get("model_txt") or ""))}</code></td></tr>
<tr><th>evaluation results</th><td><code>{html.escape(str(report["paths"].get("evaluation_results") or ""))}</code></td></tr>
<tr><th>item ability</th><td><code>{html.escape(str(report["paths"].get("item_ability") or ""))}</code></td></tr>
<tr><th>report mode</th><td>facts only: no verdicts, thresholds, or actions</td></tr>
</tbody></table>
{"<ul>" + notes_html + "</ul>" if notes_html else ""}

<h2>1. How The Ledger Counts</h2>
<p class="muted">The ledger walks every LightGBM tree from the root while carrying the set of item values still reachable at the current node. It separates product prior from post-item context by whether a split happens on the item column, and whether later context splits happen after at least one item split.</p>
<table><thead><tr><th>term</th><th>meaning</th><th>denominator or counting rule</th></tr></thead><tbody>
<tr><td class="left"><code>Item Prior</code></td><td class="left">A split whose feature is the item column. It routes products and is the closest structural proxy for learning product identity / prior.</td><td class="left">Gain share denominator is total model split Gain. Split count is counted once per item-column split node.</td></tr>
<tr><td class="left"><code>Post-Item Context</code></td><td class="left">A non-item feature split reached after at least one item split on that path. It is the proxy for context rules learned after the model has conditioned on item identity.</td><td class="left">Global count is counted once per split node. Per-item allocation credits the split to every item still reachable at that node.</td></tr>
<tr><td class="left"><code>Pre-Item / Unassigned Context</code></td><td class="left">Remaining split Gain not counted as Item Prior or Post-Item Context.</td><td class="left">This mainly captures non-item splits before any item split on a path, plus any split not assigned to the two ledger buckets above.</td></tr>
<tr><td class="left"><code>allocated Post-Item Context Gain / sum allocated Gain</code></td><td class="left">An item's allocated Post-Item Context Gain divided by the sum of allocated Post-Item Context Gain over all items.</td><td class="left">This is a concentration measure inside the per-item allocation view. It sums to 100% across items, but its denominator is allocated Gain, not global model Gain.</td></tr>
<tr><td class="left"><code>allocated Post-Item Context splits / sum allocated splits</code></td><td class="left">An item's allocated Post-Item Context split count divided by the sum of allocated Post-Item Context split counts over all items.</td><td class="left">Same allocation rule as Gain. A shared split can be credited to multiple reachable items, so allocated split counts can exceed global Post-Item Context split count.</td></tr>
</tbody></table>

<h2>2. Global Capacity Shape</h2>
<p class="muted">This is the whole-model split Gain shape. The three gain-share columns use total model split Gain as the denominator.</p>
{table_html([summary], [("n_trees","n_trees"),("total_gain","total split gain"),("item_id_gain_sum","Item Prior Gain"),("item_id_gain_share","Item Prior Gain / Total"),("context_gain_sum","Post-Item Context Gain"),("context_gain_share","Post-Item Context Gain / Total"),("unaccounted_gain_share","Pre-Item / Unassigned Gain / Total"),("prior_to_personalization_gain_ratio","Item Prior Gain / Post-Item Context Gain")])}

<h2>3. Split Counts</h2>
<p class="muted">When model.txt is available, total split count is the true booster split count: sum(num_leaves - 1) over trees. Pre-Item / Unassigned splits are total splits minus Item Prior splits minus Post-Item Context splits.</p>
{table_html([summary], [("total_split_count","total booster splits"),("item_id_split_count","Item Prior splits"),("context_split_count","Post-Item Context splits"),("other_split_count","Pre-Item / Unassigned splits"),("item_id_split_share_total","Item Prior split / Total"),("context_split_share_total","Post-Item Context split / Total"),("other_split_share_total","Pre-Item / Unassigned split / Total"),("accounted_split_count","Item Prior + Post-Item splits")])}

<h2>4. Allocation Denominators</h2>
<p class="muted">This section shows why allocation numbers must be read separately from global Gain and global split counts. Shared post-item subtree splits are credited to every item still reachable, so allocated totals can be larger than global totals.</p>
{table_html([summary], [("context_gain_sum","global Post-Item Context Gain"),("allocated_context_gain_sum","sum allocated Post-Item Context Gain"),("allocated_to_global_context_gain_ratio","allocated Gain / global Gain"),("context_split_count","global Post-Item Context splits"),("allocated_context_split_sum","sum allocated Post-Item Context splits"),("allocated_to_global_context_split_ratio","allocated splits / global splits")])}

<h2>5. Allocation Concentration</h2>
<p class="muted">These stats are per-item concentration measures. Gain columns use sum allocated Post-Item Context Gain as denominator; split columns use sum allocated Post-Item Context splits as denominator.</p>
{table_html([summary], [("context_gain_alloc_share_min","min Gain alloc share"),("context_gain_alloc_share_p50","median Gain alloc share"),("context_gain_alloc_share_max","max Gain alloc share"),("top3_context_gain_alloc_share","top-3 Gain alloc concentration"),("context_split_alloc_share_min","min split alloc share"),("context_split_alloc_share_p50","median split alloc share"),("context_split_alloc_share_max","max split alloc share"),("top3_context_split_alloc_share","top-3 split alloc concentration"),("max_to_min_context_share_ratio","max/min Gain alloc share")])}

<h2>6. Per-Item Capacity Ledger</h2>
<p class="muted">Each row shows the item's allocated Post-Item Context capacity. The two share columns have allocation denominators, not total-model denominators. AP and centered AUC are reference columns from other reports, not inputs to any verdict here.</p>
{table_html(rows, [("item","item"),("context_capacity_rank","Post-Item Context rank"),("context_gain_share_allocated","allocated Gain / sum allocated Gain"),("context_split_share_allocated","allocated splits / sum allocated splits"),("context_share_vs_max","Gain allocation / max item"),("context_share_vs_median","Gain allocation / median item"),("context_gain","allocated Post-Item Context Gain"),("context_split_count","allocated Post-Item Context splits"),("context_gain_per_split","allocated Gain / split"),("isolating_split_count","Item Prior splits while reachable"),("first_tree_index","first tree"),("n_trees_touched","trees touched"),("ap","AP"),("ap_rank","AP rank"),("n_pos","n_pos"),("query_centered_auc","centered AUC"),("auc_rank","AUC rank"),("auc_ci","AUC CI")])}

<h2>7. Post-Item Context Gain Allocation Ranking</h2>
<div class="plot">{svg_capacity_bars(rows)}</div>

<h2>8. Post-Item Context Gain Allocation vs Ability</h2>
<p class="muted">This is a fact plot only: x is allocated Post-Item Context Gain divided by sum allocated Gain; y is query-centered AUC from item_ability.json.</p>
<div class="plot">{svg_capacity_vs_ability(rows)}</div>
</body></html>"""


def write_outputs(report: dict, output: str) -> None:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_html(report), encoding="utf-8")
    out.with_suffix(".json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def build_report(args: argparse.Namespace) -> dict:
    model_dir = Path(args.model_dir) / str(args.model_version)
    model_txt_path = Path(args.model_txt) if args.model_txt else model_dir / "model.txt"
    gain_path = Path(args.gain_ledger) if args.gain_ledger else model_dir / "diagnostics" / "gain_ledger.json"
    eval_path = Path(args.evaluation_results) if args.evaluation_results else model_dir / "evaluation_results.json"
    ability_path = Path(args.item_ability) if args.item_ability else Path("data/diagnosis/item_ability.json")

    notes: list[str] = []
    gain_ledger = load_json(gain_path, required=True)
    if not gain_ledger.get("enabled", False):
        raise ValueError(f"gain_ledger is disabled in {gain_path}")
    if gain_ledger.get("fallback"):
        raise ValueError(
            "gain_ledger is in fallback mode; per-item capacity ledger is unavailable."
        )
    if not gain_ledger.get("per_item"):
        raise ValueError(f"gain_ledger lacks per_item block: {gain_path}")

    eval_payload = load_json(eval_path, required=False)
    if not eval_payload:
        notes.append(f"evaluation_results.json not found at {eval_path}; AP columns may be blank.")
    total_split_count = parse_lightgbm_total_split_count(model_txt_path)
    if total_split_count is None:
        notes.append(
            f"model.txt not found or not parseable at {model_txt_path}; true total split shares are omitted."
        )
    ap_by_item, overall_map = evaluation_ap_by_item(eval_payload)
    ability_payload = load_json(ability_path, required=False)
    ability = ability_by_item(ability_payload, args.model_version, notes)

    rows = build_capacity_rows(
        gain_ledger,
        ap_by_item,
        ability,
    )
    summary = summarize(
        gain_ledger,
        rows,
        overall_map,
        total_split_count=total_split_count,
    )
    return {
        "model_version": str(args.model_version),
        "paths": {
            "gain_ledger": str(gain_path),
            "model_txt": str(model_txt_path) if total_split_count is not None else None,
            "evaluation_results": str(eval_path) if eval_path.exists() else None,
            "item_ability": str(ability_path) if ability else None,
        },
        "params": {"mode": "facts_only"},
        "notes": notes + list(gain_ledger.get("notes", []) or []),
        "summary": summary,
        "per_item": rows,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--model-version", required=True, help="Model version under data/models.")
    p.add_argument("--model-dir", default="data/models", help="Directory containing model-version folders.")
    p.add_argument("--gain-ledger", help="Override gain_ledger.json path.")
    p.add_argument("--model-txt", help="Optional LightGBM model.txt path for true total split count.")
    p.add_argument("--evaluation-results", help="Override evaluation_results.json path.")
    p.add_argument("--item-ability", help="Optional item_ability.json path.")
    p.add_argument("--output", default="data/diagnosis/model_capacity.html")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report(args)
    write_outputs(report, args.output)
    print(f"Wrote {args.output}")
    print(f"Wrote {Path(args.output).with_suffix('.json')}")
    print(
        "allocated_context_gain_share_range="
        f"{fmt_pct(report['summary']['context_gain_alloc_share_min'])}.."
        f"{fmt_pct(report['summary']['context_gain_alloc_share_max'])} "
        "top3_allocated_context_gain_share="
        f"{fmt_pct(report['summary']['top3_context_gain_alloc_share'])}"
    )


if __name__ == "__main__":
    main()
