"""Report section assembly. One pure function per section; no Spark.

Each builder takes the small aggregated metrics dict (from
metrics_spark.compute_all_metrics) + parameters and returns a ReportSection
(or None when its config toggle is off). assemble_report wires the enabled
sections into the final HTML.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

from recsys_tfb.evaluation.report import ReportSection, generate_html_report


def _resolve_display_k(raw_k: list, n_products: int) -> list:
    """Map mixed int/'all' display k list to concrete column suffixes.

    Returns labels as strings/ints that are used both as dict keys and for
    metric lookups. 'all' resolves to n_products for metric lookup but is
    kept as the label 'all' for display.
    """
    out = []
    for k in raw_k:
        if isinstance(k, str) and k.lower() == "all":
            out.append("all")
        else:
            out.append(int(k))
    return out


def _k_to_lookup(k, n_products: int) -> int | str:
    """Convert display label to metric dict key suffix."""
    if k == "all":
        return n_products
    return k


def _report_cfg(parameters: dict) -> dict:
    return (parameters.get("evaluation", {}) or {}).get("report", {}) or {}


def _section_on(parameters: dict, name: str) -> bool:
    sections = _report_cfg(parameters).get("sections", {}) or {}
    return bool(sections.get(name, True))


def _n_products(metrics: dict) -> int:
    return int(
        metrics.get("dataset_overview", {})
        .get("totals", {})
        .get("n_products", 0)
    )


def build_headline_section(metrics: dict, parameters: dict) -> ReportSection:
    overall = metrics.get("overall", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod
    )
    card = {
        f"map@{k}": overall.get(f"map@{_k_to_lookup(k, n_prod)}")
        for k in ks
    }
    meta = {
        "n_queries": metrics.get("n_queries"),
        "n_excluded_queries": metrics.get("n_excluded_queries"),
    }
    t1 = pd.DataFrame([card]).T
    t1.columns = ["value"]
    t2 = pd.DataFrame([meta]).T
    t2.columns = ["value"]
    return ReportSection(
        title="摘要 Headline",
        description="主指標 mAP@k（細產品 overall）與 run 概況。",
        tables=[t1, t2],
        table_titles=["主指標 mAP@k", "Run 概況"],
    )


def build_dataset_overview_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "dataset_overview"):
        return None
    ov = metrics.get("dataset_overview", {})
    totals = pd.DataFrame([ov.get("totals", {})]).T
    totals.columns = ["value"]
    by_snap = pd.DataFrame(ov.get("by_snap_date", {})).T
    by_item = pd.DataFrame(ov.get("by_item", {})).T
    tables = [totals, by_snap, by_item]
    titles = ["總覽", "by snap_date", "by 產品"]
    cat = metrics.get("category")
    if cat:
        cat_by_item = (cat.get("dataset_overview", {}) or {}).get("by_item", {})
        if cat_by_item:
            tables.append(pd.DataFrame(cat_by_item).T)
            titles.append("by 大類")
    return ReportSection(
        title="資料概況 Dataset Overview",
        description="總覽、依 snap_date、依產品（及大類）的筆數／正樣本數／客戶數。",
        tables=tables,
        table_titles=titles,
    )


def build_primary_map_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "primary_map"):
        return None
    overall = metrics.get("overall", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), _n_products(metrics)
    )
    rows = {}
    n_prod = _n_products(metrics)
    for fam in ("map", "precision", "ndcg", "recall"):
        rows[fam] = {
            f"@{k}": overall.get(f"{fam}@{_k_to_lookup(k, n_prod)}")
            for k in ks
        }
    table = pd.DataFrame(rows).T
    return ReportSection(
        title="主指標 mAP@k（細產品 per-query）",
        description=(
            "overall mAP@k 為主軸；precision/ndcg/recall@k 作脈絡。"
            "K = 產品數時 precision 退化為 base rate、recall 恆為 1。"
        ),
        tables=[table],
        table_titles=["per-query 指標 @k"],
    )


def _per_item_metric_table(
    per_item: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    col_fmt: str,
    extra_cols: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Rows = items; one column per k named ``col_fmt.format(k=k)``, value
    pulled from ``per_item[item][f"{metric_key}@{_k_to_lookup(k, n_prod)}"]``.

    ``extra_cols`` maps an output column name to a flat (non-@k) per_item key,
    e.g. ``{"mean_pos": "mean_pos"}``.
    """
    data = {}
    for item, m in per_item.items():
        row = {
            col_fmt.format(k=k): m.get(f"{metric_key}@{_k_to_lookup(k, n_prod)}")
            for k in ks
        }
        for out_name, src_key in (extra_cols or {}).items():
            row[out_name] = m.get(src_key)
        data[item] = row
    return pd.DataFrame(data).T


def _per_item_heatmap(
    table: pd.DataFrame,
    per_item: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    x_fmt: str,
    title: str,
    zmin: float | None = None,
    zmax: float | None = None,
) -> go.Figure:
    """RdYlGn heatmap; z from ``per_item[item][f"{metric_key}@{lookup(k)}"]``,
    rows ordered by ``table.index``. ``zmin``/``zmax`` left None -> Plotly
    autoscales the colour range.
    """
    z = [
        [per_item.get(it, {}).get(f"{metric_key}@{_k_to_lookup(k, n_prod)}")
         for k in ks]
        for it in table.index
    ]
    fig = go.Figure(
        data=go.Heatmap(
            z=z, x=[x_fmt.format(k=k) for k in ks], y=list(table.index),
            zmin=zmin, zmax=zmax,
            colorscale="RdYlGn", texttemplate="%{z:.3f}",
        )
    )
    fig.update_layout(title=title, yaxis_title="產品")
    return fig


def _per_item_recall_table(per_item: dict, ks: list, n_prod: int) -> pd.DataFrame:
    """Rows = items; recall@k (per-item) cols (renamed from hit_rate@k) + mean_pos."""
    return _per_item_metric_table(
        per_item, ks, n_prod, "hit_rate", "recall@{k} (per-item)",
        extra_cols={"mean_pos": "mean_pos"},
    )


def build_guardrail_recall_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "guardrail_recall"):
        return None
    per_item = metrics.get("per_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    table = _per_item_recall_table(per_item, ks, n_prod)
    cs = disp.get("recall_colorscale", {}) or {}
    fig = _per_item_heatmap(
        table, per_item, ks, n_prod, "hit_rate", "recall@{k}",
        "per-item recall@k 色階",
        zmin=cs.get("low", 0.0), zmax=cs.get("high", 1.0),
    )
    return ReportSection(
        title="護欄 per_item recall@k（細產品）",
        description=(
            "每產品 recall@k（per-item，即 hit_rate@k 正名）＋色階。"
            "純判讀、無 pass/fail 閾值。完整資料統計見「資料概況」。"
        ),
        figures=[fig],
        tables=[table],
        table_titles=["per-item recall@k"],
    )


def build_category_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "category"):
        return None
    cat = metrics.get("category")
    if not cat:
        return None
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_cat = int(cat.get("dataset_overview", {}).get("totals", {})
                .get("n_products", 0))
    map_ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_cat)
    rec_ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_cat)
    overall = cat.get("overall", {})
    map_tbl = pd.DataFrame(
        [{f"map@{k}": overall.get(f"map@{_k_to_lookup(k, n_cat)}")
          for k in map_ks}]
    ).T
    map_tbl.columns = ["value"]
    rec_tbl = _per_item_recall_table(cat.get("per_item", {}), rec_ks, n_cat)
    mapping = (((parameters.get("evaluation", {}) or {})
               .get("product_categories", {}) or {}).get("mapping", {})) or {}
    tables = [map_tbl, rec_tbl]
    table_titles = ["大類 mAP@k", "大類 per-item recall@k"]
    if mapping:
        comp_tbl = pd.DataFrame(
            [{"子產品": ", ".join(v)} for v in mapping.values()],
            index=list(mapping.keys()),
        )
        tables.append(comp_tbl)
        table_titles.append("大類組成")
    return ReportSection(
        title="大類層級 Category",
        description="大類粒度 mAP@k 與 per-item recall@k（大類=子產品最佳 rank）。",
        tables=tables,
        table_titles=table_titles,
    )


def build_segment_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "per_segment"):
        return None
    per_segment = metrics.get("per_segment", {})
    if not per_segment:
        return None
    table = pd.DataFrame(per_segment).T
    return ReportSection(
        title="分群 Per-Segment",
        description="per-query 指標依 segment 切分。",
        tables=[table],
        table_titles=["per-segment 指標"],
    )


def build_diagnostics_section(
    diagnostics_frames: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "diagnostics") or not diagnostics_frames:
        return None
    figs = diagnostics_frames.get("figures", [])
    if not figs:
        return None
    return ReportSection(
        title="診斷 Diagnostics",
        description="score 分布／rank heatmap／calibration（預設收合）。",
        figures=figs,
        collapsible=True,
    )


def build_baseline_section(
    metrics: dict, baseline_metrics: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "baseline") or baseline_metrics is None:
        return None
    from recsys_tfb.evaluation.compare import build_comparison_result

    comp = build_comparison_result(
        metrics, baseline_metrics, "Model", "Baseline"
    )
    delta = pd.DataFrame([comp["overall_delta"]]).T
    delta.columns = ["Delta (Model - Baseline)"]
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    # _k_to_lookup handles a hypothetical "all" in guardrail_recall_k
    # (defaults are numeric, but keep parity with the §3 guardrail).
    rec_ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    pid = comp.get("per_item_delta", {}) or {}
    tables = [delta]
    table_titles = ["overall delta"]
    if pid and (baseline_metrics or {}).get("per_item"):
        rec_rows = {
            item: {
                f"recall@{k} (per-item) Δ":
                    md.get(f"hit_rate@{_k_to_lookup(k, n_prod)}")
                for k in rec_ks
            }
            for item, md in pid.items()
        }
        tables.append(pd.DataFrame(rec_rows).T)
        table_titles.append("per-item recall@k delta")
    return ReportSection(
        title="基準比較 Baseline",
        description="Model vs Baseline：overall mAP@k 與 per-item recall@k delta。",
        tables=tables,
        table_titles=table_titles,
    )


_GLOSSARY = [
    ("mAP@k", "per-query Average Precision@k 對 query 平均；主指標"),
    ("recall@k (per-item)", "P(rank(P)≤k | P 為正)，命中事件等權；護欄"),
    ("precision@k", "per-query 命中數/k；k=產品數時退化為 base rate"),
    ("ndcg@k", "log 折扣排序品質，正規化 [0,1]"),
    ("mean_pos", "產品為正時平均排名位置（越小越好）"),
    ("base rate", "母體正樣本率"),
]


def build_glossary_section(parameters: dict) -> ReportSection:
    tbl = pd.DataFrame(_GLOSSARY, columns=["指標", "語意"])
    return ReportSection(
        title="詞彙表 Glossary",
        description="指標語意，詳見 docs/metrics_concept_map.html。",
        tables=[tbl],
        table_titles=["指標語意"],
    )


def assemble_report(
    metrics: dict,
    parameters: dict,
    baseline_metrics: dict | None = None,
    diagnostics_frames: dict | None = None,
) -> str:
    """Assemble enabled sections (§0–§8) into the final HTML string."""
    candidates = [
        build_headline_section(metrics, parameters),
        build_dataset_overview_section(metrics, parameters),
        build_primary_map_section(metrics, parameters),
        build_guardrail_recall_section(metrics, parameters),
        build_category_section(metrics, parameters),
        build_segment_section(metrics, parameters),
        build_diagnostics_section(diagnostics_frames, parameters),
        build_baseline_section(metrics, baseline_metrics, parameters),
        build_glossary_section(parameters),
    ]
    sections = [s for s in candidates if s is not None]
    eval_params = parameters.get("evaluation", {}) or {}
    metadata = {
        "Model Version": parameters.get("model_version", "unknown"),
        "Snap Date": eval_params.get("snap_date", "unknown"),
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Total Queries": metrics.get("n_queries"),
        "Excluded Queries": metrics.get("n_excluded_queries"),
    }
    return generate_html_report(
        sections, title="Model Evaluation Report", metadata=metadata
    )
