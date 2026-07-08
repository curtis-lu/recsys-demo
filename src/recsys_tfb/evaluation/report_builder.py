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
    metrics: dict, parameters: dict, metric_ci: dict | None = None
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
    tables = [table]
    table_titles = ["per-query 指標 @k"]
    if metric_ci and metric_ci.get("enabled") and metric_ci.get("macro"):
        m = metric_ci["macro"]
        sample_meta = metric_ci.get("sample", {}) or {}
        ci_tbl = pd.DataFrame(
            [{"AP(抽樣)": m.get("ap"), "CI 2.5%": m.get("ci_low"),
              "CI 97.5%": m.get("ci_high"),
              "樣本 query 數": sample_meta.get("n_queries_sampled")}],
            index=["macro per-item mAP"],
        )
        tables.append(ci_tbl)
        table_titles.append("macro per-item mAP 的 CI（抽樣估計）")
    return ReportSection(
        title="主指標 mAP@k（細產品 per-query）",
        description=(
            "overall mAP@k 為主軸；precision/ndcg/recall@k 作脈絡。"
            "K = 產品數時 precision 退化為 base rate、recall 恆為 1。"
        ),
        tables=tables,
        table_titles=table_titles,
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
    metrics: dict, parameters: dict, metric_ci: dict | None = None
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

    description_extra = ""
    if metric_ci and metric_ci.get("enabled"):
        ci_items = metric_ci.get("per_item", {}) or {}
        ci_macro = metric_ci.get("macro") or {}
        sample_meta = metric_ci.get("sample", {}) or {}

        def _ci_val(idx: str, field: str):
            src = ci_macro if idx == _MACRO_LABEL else ci_items.get(idx, {})
            return src.get(field)

        for col, field in (("AP(抽樣)", "ap"), ("CI 2.5%", "ci_low"),
                           ("CI 97.5%", "ci_high"), ("n_pos(抽樣)", "n_pos")):
            map_tbl[col] = [_ci_val(idx, field) for idx in map_tbl.index]
        ci_k_label = metric_ci.get("k") or "all"
        description_extra = (
            f"AP(抽樣)/CI 欄為抽樣估計（{sample_meta.get('n_queries_sampled')} "
            f"個正例 query、bootstrap n_boot={metric_ci.get('n_boot')}，"
            f"cluster=客戶），非全量值；其截斷 k={ci_k_label}，點估計以全量欄 "
            f"map_attr@{ci_k_label} 為準（不要拿去對其他 @k 欄）。"
            f"n_pos(抽樣) 為該 item 進入 CI 估計的正例列數——太小的值代表"
            f"該列 CI 不可靠，判讀時先看這欄。"
        )

    tables = [map_tbl, ndcg_tbl]
    table_titles = ["per-item map_attr@k", "per-item ndcg_attr@k"]
    observation_items = metrics.get("observation_items", []) or []
    if observation_items:
        per_item_all = metrics.get("per_item", {})
        obs_tbl = pd.DataFrame(
            {"n_pos": [per_item_all.get(it, {}).get("n_pos")
                       for it in observation_items]},
            index=observation_items,
        )
        tables.append(obs_tbl)
        table_titles.append("觀察名單（n_pos < min_positives，已移出 macro）")

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
        ) + description_extra,
        figures=[map_fig, ndcg_fig],
        tables=tables,
        table_titles=table_titles,
    )


def build_reconciliation_section(
    reconciliation: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "reconciliation"):
        return None
    if not reconciliation or not reconciliation.get("enabled"):
        return None
    by_item = reconciliation.get("by_item", {}) or {}
    cols = ["theory_min", "theory_max", "gap", "gap_vs_global",
            "gap_calibrated", "gap_calibrated_vs_global",
            "residual", "verdict", "p_mean", "y_rate", "n_rows"]
    tbl = pd.DataFrame(
        {c: [by_item[it].get(c) for it in by_item] for c in cols},
        index=list(by_item),
    )
    score_col = reconciliation.get("score_col_used")
    glob = reconciliation.get("global", {}) or {}
    reference = glob.get("reference", 0.0)
    n_neutral = glob.get("n_neutral_items", 0)
    pooled_gap = glob.get("pooled_gap")
    pooled_clause = (
        f"；n_rows 加權 pooled gap {pooled_gap:.3f} 供交叉檢核"
        if pooled_gap is not None else ""
    )
    desc = (
        "這張表在回答一個問題：模型的機率水準偏移，是不是你自己的抽樣／"
        "加權配置造成的？欄位定義與完整前因後果見 "
        "docs/pipelines/evaluation-diagnosis.md，這裡只給判讀順序：<br>"
        "1. 先掃 verdict 欄。全部「可解釋」＝偏移都是配置的直接後果，"
        "不用動模型；出現「不可解釋」＝有配置以外的事在發生，值得追；"
        "「無法評估」看 reason 欄。<br>"
        "2. theory_min／theory_max＝由配置推得的理論偏移帶（log-odds）。"
        "同一產品在不同客群的抽樣比率不同，所以是「帶」不是單值——"
        "帶越寬 verdict 越寬容（跨 segment 聚合近似，cell 級精確值在 "
        "reconciliation.json）。<br>"
        f"3. verdict 比的是 gap_vs_global（＝gap − 全局參考值 "
        f"{reference:.3f}，取 {n_neutral} 個 config 中性產品的 gap 中位數"
        f"{pooled_clause}），不是絕對 gap——post-training 的評估母體只含"
        "有正例的客戶，所有產品的 gap 會被一致下移，那是母體性質、"
        f"不是任何單一產品的問題。|residual| ≤ "
        f"{reconciliation.get('explained_threshold')} → 可解釋。<br>"
        f"4. 主判欄用 {score_col}（模型原始輸出）。判讀校準層看 "
        "gap_calibrated_vs_global：校準有效時它應遠小於 gap_vs_global、"
        "趨近 0。gap 為 logit(平均) 的近似——產品內分數越分散，偏差越大。"
    )
    if reconciliation.get("fallback"):
        desc += (
            "⚠ 本次執行找不到 score_uncalibrated 欄（monitoring 路徑），"
            "已退回 score——gap 內含校準層效應，判讀時注意。"
        )
    notes = (reconciliation.get("theory", {}) or {}).get("notes") or []
    if notes:
        desc += "／".join(notes)
    return ReportSection(
        title="對帳 Reconciliation（理論偏移 vs 實測校準差距）",
        description=desc,
        tables=[tbl],
        table_titles=["per-item 對帳表"],
    )


def _quadrant_scatter(by_item: dict, thresholds: dict) -> go.Figure | None:
    """象限散布圖：橫軸 AUC（→ 判別力好）、縱軸 gap_vs_global（↑ 水準偏高）。

    樣式鏡射框架手冊 fig2-quadrant-map：淺藍水平帶＝水準大致正確的範圍、
    垂直虛線＝判別力門檻。任一軸缺值的 item 不進圖（表中仍列）。
    """
    pts = {
        it: v for it, v in by_item.items()
        if v.get("auc") is not None and v.get("gap_vs_global") is not None
    }
    if not pts:
        return None
    band = float(thresholds.get("gap_band", 0.35))
    thr = float(thresholds.get("auc_threshold", 0.6))
    fig = go.Figure(
        go.Scatter(
            x=[v["auc"] for v in pts.values()],
            y=[v["gap_vs_global"] for v in pts.values()],
            mode="markers+text",
            text=list(pts),
            textposition="top center",
        )
    )
    fig.add_hrect(y0=-band, y1=band, fillcolor="lightblue", opacity=0.3,
                  line_width=0)
    fig.add_vline(x=thr, line_dash="dash")
    fig.update_layout(
        title="象限地圖：水準（縱）× 條件判別力（橫）",
        xaxis_title="within-item AUC（→ 判別力好）",
        yaxis_title="gap_vs_global（↑ 水準偏高）",
    )
    return fig


def build_quadrant_section(
    quadrant: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "quadrant"):
        return None
    if not quadrant or not quadrant.get("enabled"):
        return None
    by_item = quadrant.get("by_item", {}) or {}
    cols = ["quadrant", "gap_vs_global", "auc", "auc_reason", "ap_sampled",
            "ci_low", "ci_high", "top_share", "y_rate", "suppression_count",
            "n_pos", "n_rows"]
    tbl = pd.DataFrame(
        {c: [by_item[it].get(c) for it in by_item] for c in cols},
        index=list(by_item),
    )
    thresholds = quadrant.get("thresholds", {}) or {}
    fig = _quadrant_scatter(by_item, thresholds)
    tables = [tbl]
    table_titles = ["per-item 象限表"]
    cp = (quadrant.get("cross_purchase", {}) or {}).get("matrix", {}) or {}
    if cp:
        cp_tbl = pd.DataFrame.from_dict(cp, orient="index")
        order = sorted(cp_tbl.index)
        cp_tbl = cp_tbl.reindex(index=order, columns=order)
        tables.append(cp_tbl)
        table_titles.append("交叉購買矩陣 P(買 k｜買 j)（列＝j、欄＝k）")
    desc = (
        "行為層象限：縱軸 gap_vs_global（水準）、橫軸 within-item AUC"
        "（條件判別力）。判讀順序：(1) 先看散布圖每個 item 落在哪個象限；"
        f"(2) 水準帶外（|gap_vs_global| > {thresholds.get('gap_band')}）的 "
        "item 回對帳表查可否由配置解釋；(3) AUC 低於 "
        f"{thresholds.get('auc_threshold')} 的 item 看 suppression_count 與 "
        "top_share 評估傷害；(4) 交叉購買矩陣看高共購 item 之間的壓制是否"
        "實質。完整判讀：docs/pipelines/evaluation-diagnosis.md。"
    )
    notes = quadrant.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    return ReportSection(
        title="象限 Quadrant（水準 × 條件判別力）",
        description=desc,
        figures=[fig] if fig is not None else [],
        tables=tables,
        table_titles=table_titles,
    )


_SWEEP_BLUE = "#1565c0"
_SWEEP_ORANGE = "#e65100"


def _offset_sweep_waterfall(sweep: dict) -> go.Figure | None:
    """分流 waterfall：折外 mAP(0) → 各 item 的 LOO 貢獻 → mAP(δ*)。

    顏色語意沿手冊 fig6-offset-sweep-split：藍＝offset 收復（水準缺口）、
    橘＝負向；mAP(δ*) 與可及上限之間收不回的部分＝條件判別力缺口（上限
    未知，圖上不畫）。原圖為 matplotlib，此處依 spec 修訂以 plotly 重刻。
    """
    mh = sweep.get("map_holdout", {}) or {}
    if mh.get("zero") is None or mh.get("star") is None:
        return None
    per_item = sweep.get("per_item", {}) or {}
    moved = {
        it: v["loo_contribution_holdout"]
        for it, v in per_item.items()
        if v.get("delta_star") and v.get("loo_contribution_holdout") is not None
    }
    if not moved:
        return None
    order = sorted(moved, key=lambda it: -abs(moved[it]))
    x = ["mAP(0) 折外"] + [f"δ*({it})" for it in order]
    y = [mh["zero"]] + [moved[it] for it in order]
    measure = ["absolute"] + ["relative"] * len(order)
    residual = sweep.get("interaction_residual_holdout")
    if residual is not None:
        x.append("交互殘差")
        y.append(residual)
        measure.append("relative")
    x.append("mAP(δ*) 折外")
    y.append(mh["star"])
    measure.append("total")
    fig = go.Figure(go.Waterfall(
        x=x, y=y, measure=measure,
        increasing={"marker": {"color": _SWEEP_BLUE}},
        decreasing={"marker": {"color": _SWEEP_ORANGE}},
        totals={"marker": {"color": "#9e9e9e"}},
    ))
    fig.update_layout(
        title="水準分流：per-item 平移（δ*）可收復的指標缺口（折外）",
        yaxis_title="macro per-item mAP",
        showlegend=False,
    )
    return fig


def build_offset_sweep_section(
    sweep: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "offset_sweep"):
        return None
    if not sweep or not sweep.get("enabled"):
        return None
    mf = sweep.get("map_fit", {}) or {}
    mh = sweep.get("map_holdout", {}) or {}

    def _gap(zero, star):
        return (star - zero) if (zero is not None and star is not None) else None

    summary = pd.DataFrame(
        {
            "mAP(0)": [mf.get("zero"), mh.get("zero")],
            "mAP(δ*)": [mf.get("star"), mh.get("star")],
            "收復量": [_gap(mf.get("zero"), mf.get("star")),
                       _gap(mh.get("zero"), mh.get("star"))],
        },
        index=["折內（fit）", "折外（holdout）"],
    )
    per_item = sweep.get("per_item", {}) or {}
    cols = ["delta_star", "delta_star_centered", "loo_contribution_holdout"]
    tbl = pd.DataFrame(
        {c: [per_item[it].get(c) for it in per_item] for c in cols},
        index=list(per_item),
    )
    fig = _offset_sweep_waterfall(sweep)
    desc = (
        "分流閥：對每個 item 的 logit 分數加常數 δ（不重訓）能收復多少 "
        "macro mAP。判讀順序：(1) 看折外收復量——大＝缺口主要在水準（配置"
        "／再平衡可修）、小＝缺口在條件判別力（必須動訓練）；(2) 看 δ* 大"
        "的 item 是誰，回對帳表查可否由配置解釋；(3) waterfall 看收復量怎"
        "麼分攤到各 item。δ* 單位＝log-odds，與對帳層 offset 同尺度。完整"
        "判讀：docs/pipelines/evaluation-diagnosis.md。"
    )
    notes = sweep.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    return ReportSection(
        title="分流 Offset sweep（水準 vs 條件判別力）",
        description=desc,
        figures=[fig] if fig is not None else [],
        tables=[summary, tbl],
        table_titles=["mAP 收復摘要（折內／折外）",
                      "per-item δ* 與折外 LOO 貢獻"],
    )


def _pair_ledger_heatmap(ledger: dict) -> go.Figure | None:
    matrix = ledger.get("matrix") or {}
    if not matrix:
        return None
    suppressors = sorted(matrix)
    victims = sorted({v for row in matrix.values() for v in row})
    z = [[(matrix.get(s_, {}).get(v) or {}).get("dap_sum")
          for v in victims] for s_ in suppressors]
    counts = [[(matrix.get(s_, {}).get(v) or {}).get("pair_count", 0)
               for v in victims] for s_ in suppressors]
    fig = go.Figure(go.Heatmap(
        z=z, x=victims, y=suppressors, colorscale="Blues",
        customdata=counts,
        hovertemplate=("壓制者 %{y} → 受害者 %{x}<br>"
                       "|ΔAP| 總量 %{z:.4f}<br>"
                       "pair 數 %{customdata}<extra></extra>"),
        colorbar={"title": "|ΔAP| 總量"},
    ))
    fig.update_layout(
        title="壓制帳本：交換名次的指標敏感度 |ΔAP|（λ 會計）",
        xaxis_title="受害者（正例被壓的 item）",
        yaxis_title="壓制者（排上方的負例 item）",
    )
    return fig


def build_pair_ledger_section(
    ledger: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "pair_ledger"):
        return None
    if not ledger or not ledger.get("enabled"):
        return None
    sup = ledger.get("by_suppressor", {}) or {}
    sup_tbl = pd.DataFrame(
        {c: [sup[it].get(c) for it in sup]
         for c in ["pair_count", "dap_sum", "dap_share"]},
        index=list(sup),
    ).sort_values("dap_sum", ascending=False) if sup else pd.DataFrame()
    subst = ledger.get("substitution", {}) or {}
    sub_tbl = pd.DataFrame(
        {c: [subst[it].get(c) for it in subst]
         for c in ["base_rate", "base_logit", "map_substituted",
                   "delta_vs_current"]},
        index=list(subst),
    ).sort_values(
        "delta_vs_current", ascending=False
    ) if subst else pd.DataFrame()
    seg_rows = []
    for col, block in (ledger.get("by_segment", {}) or {}).items():
        for val, st in block.items():
            seg_rows.append({"segment": f"{col}={val}", **st})
    seg_tbl = pd.DataFrame(seg_rows).set_index("segment") if seg_rows \
        else pd.DataFrame()
    fig = _pair_ledger_heatmap(ledger)
    desc = (
        "壓制帳本：誰的負例壓在誰的正例上方、交換名次會讓 query AP 變多少"
        "（|ΔAP|，λ 會計——記帳不訓練）。判讀順序：(1) 看壓制者邊際表，"
        "|ΔAP| 總量大的 item 是主要加害者，回象限表看它是否「水準偏高」；"
        "(2) substitution 表 delta_vs_current 為正＝把該 item 分數換成 "
        "base-rate 常數反而更好（個性化分數是淨傷害）、負＝淨貢獻；"
        "(3) by_segment 看傷害集中在哪群。完整判讀："
        "docs/pipelines/evaluation-diagnosis.md。"
    )
    if ledger.get("n_mis_ordered_pairs", 0) == 0:
        desc += "（本次抽樣無任何排錯 pair——矩陣為空，不畫圖。）"
    notes = ledger.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    tables = [sup_tbl, sub_tbl, seg_tbl]
    table_titles = ["壓制者邊際（|ΔAP| 總量降冪）",
                    "Substitution ablation（淨傷害降冪）",
                    "傷害 × segment"]
    return ReportSection(
        title="壓制帳本 Pair ledger（誰壓了誰、代價多少）",
        description=desc,
        figures=[fig] if fig is not None else [],
        tables=tables,
        table_titles=table_titles,
    )


def _fmt_triage_starter(starter: dict | None) -> str:
    if not starter:
        return "—"
    value = starter.get("value")
    if value is None:
        value_txt = "—"
    elif isinstance(value, (int, float)):
        value_txt = f"{value:.3f}"
    else:
        value_txt = str(value)
    unit = starter.get("unit") or ""
    return f"{starter.get('type')}={value_txt} {unit}".strip()


def build_triage_section(
    triage: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "triage"):
        return None
    if not triage or not triage.get("enabled"):
        return None
    from recsys_tfb.diagnosis.metric.triage import STARTER_CAVEAT

    verdicts = triage.get("verdicts", {}) or {}
    items = sorted(verdicts)
    cols = ["判定", "建議槓桿", "起手值", "AUC", "gap_vs_global",
            "δ*_centered", "context_gain_share", "備註"]
    rows = {}
    for it in items:
        v = verdicts[it] or {}
        ev = v.get("evidence", {}) or {}
        rows[it] = [
            v.get("verdict"),
            v.get("lever"),
            _fmt_triage_starter(v.get("starter")),
            ev.get("auc"),
            ev.get("gap_vs_global"),
            ev.get("delta_star_centered"),
            ev.get("context_gain_share"),
            "；".join(v.get("notes") or []),
        ]
    tbl = pd.DataFrame(
        {c: [rows[it][i] for it in items] for i, c in enumerate(cols)},
        index=items,
    )
    summary = triage.get("summary", {}) or {}
    summary_txt = "、".join(f"{k}×{n}" for k, n in summary.items()) or "無 item"
    gl_present = triage.get("gain_ledger_present")
    gl_txt = (
        "gain_ledger 可用" if gl_present
        else "gain_ledger 缺席或降級——特徵缺失型與餓死型無法區分"
    )
    desc = (
        "跨三層診斷（象限／對帳／分流，＋結構層 gain_ledger）合成的 "
        "per-item 判定總表，省去逐張表交叉核對。判讀順序：(1) 判定欄看落"
        "在哪一型、建議槓桿欄看對應修法；(2) 起手值只是計算出的起點，"
        f"{STARTER_CAVEAT}，套用前務必用快迴路（offline 重算或小流量）驗證"
        "——不是可以直接上線的定案；(3) 證據欄回對應診斷層原表查完整脈絡。"
        f"本次判定分布：{summary_txt}；{gl_txt}。"
        "完整判讀：docs/pipelines/evaluation-diagnosis.md。"
    )
    notes = triage.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    return ReportSection(
        title="Triage 總表（跨診斷判定合成）",
        description=desc,
        tables=[tbl],
        table_titles=["per-item 判定表"],
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
    ("|ΔAP|",
     "交換一對名次讓該 query 的 AP 貢獻總和變多少；λ 會計，query-AP 粒度"),
    ("壓制者／受害者",
     "同 query 排在正例上方的負例 item／被壓的正例 item"),
    ("substitution ablation",
     "把某 item 分數換成 base-rate 常數重算指標；delta 正＝該 item "
     "個性化分數是淨傷害、負＝淨貢獻"),
    ("triage 總表",
     "跨三層診斷（象限／對帳／分流，＋結構層 gain_ledger）合成的 "
     "per-item 判定＋建議槓桿＋起手值總表"),
    ("餓死型",
     "條件判別力差、且結構層 context_gain_share 遠低於同儕——特徵夠但曝光"
     "／訓練樣本不足，非特徵缺失"),
    ("起手值",
     "由診斷數字直接算出的建議修正量（非最終定案）；套用前須經快迴路"
     "（offline 重算或小流量）驗證才能升格"),
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
    metric_ci: dict | None = None,
    reconciliation: dict | None = None,
    quadrant: dict | None = None,
    offset_sweep: dict | None = None,
    pair_ledger: dict | None = None,
    triage: dict | None = None,
) -> str:
    """Assemble every enabled section (the ``candidates`` list below is the
    authoritative order) into the final HTML string."""
    candidates = [
        build_headline_section(metrics, parameters),
        build_dataset_overview_section(metrics, parameters),
        build_primary_map_section(metrics, parameters, metric_ci=metric_ci),
        build_guardrail_recall_section(metrics, parameters),
        build_per_item_attr_section(metrics, parameters, metric_ci=metric_ci),
        build_reconciliation_section(reconciliation, parameters),
        build_quadrant_section(quadrant, parameters),
        build_offset_sweep_section(offset_sweep, parameters),
        build_pair_ledger_section(pair_ledger, parameters),
        build_triage_section(triage, parameters),
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
