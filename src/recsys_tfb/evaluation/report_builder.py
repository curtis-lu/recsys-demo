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


_MACRO_LABEL = "Macro 平均"


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
    macro_metrics: dict | None = None,
) -> pd.DataFrame:
    """Rows = items; one column per k named ``col_fmt.format(k=k)``, value
    pulled from ``per_item[item][f"{metric_key}@{_k_to_lookup(k, n_prod)}"]``.

    ``extra_cols`` maps an output column name to a flat (non-@k) per_item key,
    e.g. ``{"mean_pos": "mean_pos"}``.

    ``macro_metrics``: when given and non-empty, an equal-weight-average
    metrics dict (same key shape as a per_item value) is prepended as the
    top row labelled ``_MACRO_LABEL``.
    """
    def _row(m: dict) -> dict:
        row = {
            col_fmt.format(k=k): m.get(f"{metric_key}@{_k_to_lookup(k, n_prod)}")
            for k in ks
        }
        for out_name, src_key in (extra_cols or {}).items():
            row[out_name] = m.get(src_key)
        return row

    data: dict = {}
    if macro_metrics:
        data[_MACRO_LABEL] = _row(macro_metrics)
    for item, m in per_item.items():
        data[item] = _row(m)
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


def _per_item_metric_compare_table(
    per_item_a: dict,
    per_item_b: dict,
    per_item_delta: dict,
    ks: list,
    n_prod: int,
    metric_key: str,
    col_base_fmt: str,
    macro_a: dict | None = None,
    macro_b: dict | None = None,
) -> pd.DataFrame:
    """Per-item table with Model/Baseline/Δ interleaved per k.

    Rows = items (Macro 平均 prepended when BOTH macro_a and macro_b are
    given). Columns = ``f"{base} M"``, ``f"{base} B"``, ``f"{base} Δ"`` for
    each ``k``, where ``base = col_base_fmt.format(k=k)``.

    Δ for item rows is read from ``per_item_delta`` (already computed
    upstream by build_comparison_result); Δ for the Macro row is computed
    here as ``macro_a − macro_b`` since macro values aren't part of the
    per-item delta dict.
    """
    def _row(m_a: dict, m_b: dict, m_d: dict | None) -> dict:
        row: dict = {}
        for k in ks:
            lk = _k_to_lookup(k, n_prod)
            key = f"{metric_key}@{lk}"
            base = col_base_fmt.format(k=k)
            a = m_a.get(key)
            b = m_b.get(key)
            if m_d is not None:
                d = m_d.get(key)
            else:
                if a is None and b is None:
                    d = None
                else:
                    d = (a or 0.0) - (b or 0.0)
            row[f"{base} M"] = a
            row[f"{base} B"] = b
            row[f"{base} Δ"] = d
        return row

    data: dict = {}
    if macro_a is not None and macro_b is not None:
        data[_MACRO_LABEL] = _row(macro_a, macro_b, None)
    all_items = list(per_item_a.keys()) + [
        i for i in per_item_b.keys() if i not in per_item_a
    ]
    for item in all_items:
        data[item] = _row(
            per_item_a.get(item, {}),
            per_item_b.get(item, {}),
            per_item_delta.get(item, {}),
        )
    return pd.DataFrame(data).T


def _per_item_recall_table(
    per_item: dict, ks: list, n_prod: int, macro_metrics: dict | None = None
) -> pd.DataFrame:
    """Rows = items; recall@k (per-item) cols (renamed from hit_rate@k) + mean_pos."""
    return _per_item_metric_table(
        per_item, ks, n_prod, "hit_rate", "recall@{k} (per-item)",
        extra_cols={"mean_pos": "mean_pos"}, macro_metrics=macro_metrics,
    )


def build_guardrail_recall_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "guardrail_recall"):
        return None
    per_item = metrics.get("per_item", {})
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    # heatmap uses the table without the macro row; display uses the one with it
    table_plain = _per_item_recall_table(per_item, ks, n_prod)
    cs = disp.get("recall_colorscale", {}) or {}
    fig = _per_item_heatmap(
        table_plain, per_item, ks, n_prod, "hit_rate", "recall@{k}",
        "per-item recall@k 色階",
        zmin=cs.get("low", 0.0), zmax=cs.get("high", 1.0),
    )
    table = _per_item_recall_table(per_item, ks, n_prod, macro_metrics=macro_item)
    return ReportSection(
        title="護欄 per_item recall@k（細產品）",
        description=(
            "每產品 recall@k（per-item，即 hit_rate@k 正名）＋色階。"
            "頂列「Macro 平均」為各產品等權平均。"
            "純判讀、無 pass/fail 閾值。完整資料統計見「資料概況」。"
        ),
        figures=[fig],
        tables=[table],
        table_titles=["per-item recall@k"],
    )


def build_per_item_attr_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "per_item_attr"):
        return None
    per_item = metrics.get("per_item", {})
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod
    )
    # heatmap uses the table without the macro row; display uses the one with it
    map_tbl_plain = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "map_attr@{k}"
    )
    ndcg_tbl_plain = _per_item_metric_table(
        per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}"
    )
    map_fig = _per_item_heatmap(
        map_tbl_plain, per_item, ks, n_prod, "map_attr", "map_attr@{k}",
        "per-item map_attr@k 色階",
    )
    ndcg_fig = _per_item_heatmap(
        ndcg_tbl_plain, per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}",
        "per-item ndcg_attr@k 色階",
    )
    map_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "map_attr@{k}",
        macro_metrics=macro_item,
    )
    ndcg_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "ndcg_attr", "ndcg_attr@{k}",
        macro_metrics=macro_item,
    )
    return ReportSection(
        title="per_item 歸因 Attribution（細產品）",
        description=(
            "每個產品對主指標 mAP@k / nDCG@k 各貢獻多少。算法：對每筆"
            "「(客戶, 產品) 且該產品是這位客戶的正解」的紀錄，先算單筆貢獻 "
            "ap_contrib@k = 該產品排名進前 k 時的累積精度（排越前、前面混入"
            "的非正解越少 → 越高；沒進前 k → 0）。一位客戶的 AP@k = 他所有"
            "正解產品的 ap_contrib@k 加總 ÷ 正解數 total_rel。map_attr@k = "
            "某產品在「它為該客戶正解」的所有客戶上，ap_contrib@k 的平均 → "
            "即這個產品平均替 AP@k 加了多少分。ndcg_attr@k 同理，把單筆貢獻"
            "換成 log 折扣的 ndcg_contrib@k。頂列「Macro 平均」為各產品等權平均。"
        ),
        figures=[map_fig, ndcg_fig],
        tables=[map_tbl, ndcg_tbl],
        table_titles=["per-item map_attr@k", "per-item ndcg_attr@k"],
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
    cat_macro_item = cat.get("macro_avg", {}).get("by_item", {})
    rec_tbl = _per_item_recall_table(
        cat.get("per_item", {}), rec_ks, n_cat, macro_metrics=cat_macro_item
    )
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
        description=(
            "大類粒度 mAP@k 與 per-item recall@k（大類=子產品最佳 rank）。"
            "recall@k 表頂列「Macro 平均」為各大類等權平均。"
        ),
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
    macro_seg = metrics.get("macro_avg", {}).get("by_segment", {})
    rows = (
        {_MACRO_LABEL: macro_seg, **per_segment}
        if macro_seg
        else dict(per_segment)
    )
    table = pd.DataFrame(rows).T
    return ReportSection(
        title="分群 Per-Segment",
        description=(
            "per-query 指標依 segment 切分。"
            "頂列「Macro 平均」為各 segment 等權平均。"
        ),
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
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    rec_ks = _resolve_display_k(
        disp.get("guardrail_recall_k", [1, 2, 3, 4, 5]), n_prod
    )
    attr_ks = _resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod
    )

    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []

    # [1] popularity composition (omitted when missing/empty for back-compat).
    pcounts = (baseline_metrics or {}).get("purchase_counts") or {}
    if pcounts:
        sorted_items = sorted(
            pcounts.items(), key=lambda kv: kv[1], reverse=True
        )
        pop_df = pd.DataFrame(
            {
                "count": [v for _, v in sorted_items],
                "rank": list(range(1, len(sorted_items) + 1)),
            },
            index=[k for k, _ in sorted_items],
        )
        tables.append(pop_df)
        table_titles.append("popularity 排名組成")

    # [2] overall metrics: Model / Baseline / Delta.
    overall_a = comp["result_a"].get("overall", {}) or {}
    overall_b = comp["result_b"].get("overall", {}) or {}
    overall_delta = comp["overall_delta"]
    overall_keys = sorted(set(overall_a) | set(overall_b) | set(overall_delta))
    overall_tbl = pd.DataFrame(
        {
            "Model": [overall_a.get(k) for k in overall_keys],
            "Baseline": [overall_b.get(k) for k in overall_keys],
            "Delta": [overall_delta.get(k) for k in overall_keys],
        },
        index=overall_keys,
    )
    tables.append(overall_tbl)
    table_titles.append("overall metrics")

    # [3] per-item compare tables — only when baseline has per_item.
    per_item_a = comp["result_a"].get("per_item", {}) or {}
    per_item_b = comp["result_b"].get("per_item", {}) or {}
    per_item_delta = comp.get("per_item_delta", {}) or {}
    macro_a = (metrics.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (baseline_metrics.get("macro_avg", {}) or {}).get("by_item")
    if per_item_b:
        for metric_key, col_fmt, ks, title in (
            ("hit_rate", "recall@{k}", rec_ks,
             "per-item recall@k (M/B/Δ)"),
            ("map_attr", "map_attr@{k}", attr_ks,
             "per-item map_attr@k (M/B/Δ)"),
            ("ndcg_attr", "ndcg_attr@{k}", attr_ks,
             "per-item ndcg_attr@k (M/B/Δ)"),
        ):
            tbl = _per_item_metric_compare_table(
                per_item_a, per_item_b, per_item_delta,
                ks, n_prod, metric_key, col_fmt,
                macro_a=macro_a, macro_b=macro_b,
            )
            tables.append(tbl)
            table_titles.append(title)

    return ReportSection(
        title="基準比較 Baseline",
        description=(
            "Model vs Baseline:popularity 排名組成 + overall metrics(M/B/Δ)+ "
            "per-item recall/map_attr/ndcg_attr(M/B/Δ)。"
        ),
        tables=tables,
        table_titles=table_titles,
    )


_GLOSSARY = [
    ("mAP@k", "per-query Average Precision@k 對 query 平均；主指標"),
    ("recall@k (per-item)", "P(rank(P)≤k | P 為正)，命中事件等權；護欄"),
    ("precision@k", "per-query 命中數/k；k=產品數時退化為 base rate"),
    ("ndcg@k", "log 折扣排序品質，正規化 [0,1]"),
    ("map_attr@k",
     "某產品為正解時 ap_contrib@k 的平均；ap_contrib@k = 該產品進前 k 時的"
     "累積精度。客戶該買它、模型排越前 → 值越高。非該產品自己的 mAP@k，"
     "是 mAP@k 拆到單一產品的貢獻"),
    ("ndcg_attr@k",
     "同 map_attr@k，單筆貢獻改用 ndcg_contrib@k（log 折扣排序品質，已用 "
     "iDCG 正規化）。越高越好"),
    ("mean_pos", "產品為正時平均排名位置（越小越好）"),
    ("Macro 平均",
     "對所有產品（或 segment）等權平均；與 query 等權的 overall 不同"),
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
        build_per_item_attr_section(metrics, parameters),
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
