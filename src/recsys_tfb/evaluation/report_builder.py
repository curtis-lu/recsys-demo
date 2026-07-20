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

# metrics_spark 仍會算出 ndcg@k / ndcg_attr@k，但兩份報表都刻意不呈現它們。
# 下面幾張表把 metrics dict 的 key 直接攤成欄／列（key-agnostic），所以
# 「不呈現」必須在這裡濾——光是原始碼裡不寫 "ndcg" 字樣擋不住。
_HIDDEN_METRIC_PREFIXES = ("ndcg",)


def _visible_metric_keys(keys) -> list:
    """濾掉刻意不呈現的 metric key，保留原順序。

    契約以**顯示鍵的命名**為準（prefix 比對），不是以「屬於哪個指標家族」
    為準——目前擋掉的是 ``ndcg@k``／``ndcg_attr@k``。若日後 metrics_spark
    把同族但不以 ``ndcg`` 起頭的鍵（例如 ``dcg@k``）曝到攤平表格，需在
    ``_HIDDEN_METRIC_PREFIXES`` 補上，否則會繞過這層過濾。
    """
    return [
        k for k in keys
        if not str(k).startswith(_HIDDEN_METRIC_PREFIXES)
    ]


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
    for fam in ("map", "precision", "recall"):
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
            "overall mAP@k 為主軸；precision/recall@k 作脈絡。"
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
    map_fig = _per_item_heatmap(
        map_tbl_plain, per_item, ks, n_prod, "map_attr", "map_attr@{k}",
        "per-item map_attr@k 色階",
    )
    map_tbl = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "map_attr@{k}",
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

    tables = [map_tbl]
    table_titles = ["per-item map_attr@k"]
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
            "每個產品對主指標 mAP@k 各貢獻多少。算法：對每筆"
            "「(客戶, 產品) 且該產品是這位客戶的正解」的紀錄，先算單筆貢獻 "
            "ap_contrib@k = 該產品排名進前 k 時的累積精度（排越前、前面混入"
            "的非正解越少 → 越高；沒進前 k → 0）。一位客戶的 AP@k = 他所有"
            "正解產品的 ap_contrib@k 加總 ÷ 正解數 total_rel。map_attr@k = "
            "某產品在「它為該客戶正解」的所有客戶上，ap_contrib@k 的平均 → "
            "即這個產品平均替 AP@k 加了多少分。頂列「Macro 平均」為各產品等權平均。"
        ) + description_extra,
        figures=[map_fig],
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
        "的 item 是誰；(3) waterfall 看收復量怎"
        "麼分攤到各 item。δ* 單位＝log-odds。完整"
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
        "|ΔAP| 總量大的 item 是主要加害者；"
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
    rows = {
        seg: {k: (m or {}).get(k)
              for k in _visible_metric_keys(list((m or {}).keys()))}
        for seg, m in rows.items()
    }
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
    overall_keys = _visible_metric_keys(
        sorted(set(overall_a) | set(overall_b) | set(overall_delta))
    )
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
            "per-item recall/map_attr(M/B/Δ)。"
        ),
        tables=tables,
        table_titles=table_titles,
    )


_GLOSSARY = [
    ("mAP@k", "per-query Average Precision@k 對 query 平均；主指標"),
    ("recall@k (per-item)", "P(rank(P)≤k | P 為正)，命中事件等權；護欄"),
    ("precision@k", "per-query 命中數/k；k=產品數時退化為 base rate"),
    ("map_attr@k",
     "某產品為正解時 ap_contrib@k 的平均；ap_contrib@k = 該產品進前 k 時的"
     "累積精度。客戶該買它、模型排越前 → 值越高。非該產品自己的 mAP@k，"
     "是 mAP@k 拆到單一產品的貢獻"),
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
]


def build_glossary_section(parameters: dict) -> ReportSection:
    tbl = pd.DataFrame(_GLOSSARY, columns=["指標", "語意"])
    return ReportSection(
        title="詞彙表 Glossary",
        description="指標語意，詳見 docs/metrics_concept_map.html。",
        tables=[tbl],
        table_titles=["指標語意"],
    )


# =====================================================================
# registry 診斷的多頁輸出
# =====================================================================
#
# **這一段刻意不認識任何單一診斷。** 走的是
# ``diagnosis.metric.contract.DIAGNOSES``：對每個名字 import 模組、讀
# ``TITLE``／``SCOPE``／``render``。因此新增第六項診斷 ＝ 新增一個子套件 ＋ 在
# registry 補一行，本檔零改動。舊的 ``build_offset_sweep_section``／
# ``build_pair_ledger_section`` 不在這個規則的管轄範圍——它們服務的是尚未被
# 取代的既有診斷 node，會在整個 diag-redesign 收尾時一起清掉。
#
# 為什麼數字不複製一份到主報表：主報表只放入口
# （``build_diagnosis_links_section``）。同一個數字出現在兩個地方，就會有兩份
# 各自演化的格式與措辭，而讀者無從得知哪一份是後改的。

#: 索引頁的邏輯架構：五項診斷各回答什麼、各排除什麼，以及編號代表的意思。
#:
#: **這張表是規劃層級的敘述**（五項診斷的分工），不是 registry。哪些項目真的
#: 存在由 ``DIAGNOSES`` 決定，見 :func:`_diagnosis_index_intro` 的狀態欄——
#: 兩者分開，索引頁才不會在後四項尚未落地時假裝它們都在。
_DIAGNOSIS_PLAN = (
    ("config_shift", "配置引入的排序偏移",
     "抽樣比例與 sample weight 有沒有在每個 item 上引入 log-odds 偏移。",
     "偏移為 0 時，排序落差的來源就不在訓練設定這一側。"),
    ("item_ability", "item 辨識力",
     "模型能不能在同一個 query 內分辨誰會買哪一個 item。",
     "把客戶活躍度誤讀成 item 推薦能力。"),
    ("model_capacity", "模型容量分配",
     "gain／split 花在 item 身分，還是花在 context 特徵。",
     "把「學到互動訊號」與「只記住 item prior」分開。"),
    ("suppression", "壓制帳本",
     "哪些 label=0 排在 label=1 之前，造成多少 AP 缺口。",
     "把「模型排錯」與「商品本來就競爭」分開。"),
    ("score_shift", "per-item 分數位移",
     "不重訓、只加 per-item 常數位移，holdout mAP 能不能提升。",
     "把「偏 item 水準」與「偏辨識力／特徵表達」分開。"),
)


def _diagnosis_index_intro() -> str:
    """索引頁的說明片段（raw HTML，``write_pages`` 不 escape）。

    **這段文字就是使用者要的產出本身，不是裝飾**：需求原話是「忠實呈現數據，
    但是用一個清楚好懂的邏輯架構來幫助人類判斷，而不是直接給結論」。這裡寫
    的是那個邏輯架構——每項診斷回答什麼、排除什麼、為什麼是這個順序——讀者
    據此自己判斷，而報表本身一個結論都不下。

    狀態欄從 ``DIAGNOSES`` 動態導出，不寫死：後四項診斷分別在後續計畫落地，
    寫死的話這頁會在它們落地前就宣稱五項都在（而那種錯看不出來，因為字串
    長得很合理）。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    rows = []
    for i, (name, title, answers, rules_out) in enumerate(
        _DIAGNOSIS_PLAN, start=1
    ):
        live = name in DIAGNOSES
        status = "已在 registry" if live else "尚未進 registry"
        rows.append(
            f"<tr><td>{i}</td><td>{title}<br><code>{name}</code></td>"
            f"<td>{answers}</td><td>{rules_out}</td><td>{status}</td></tr>"
        )
    table = (
        "<table><thead><tr>"
        "<th>#</th><th>診斷</th><th>回答什麼</th><th>排除什麼</th>"
        "<th>目前狀態</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )
    return (
        "<p>這裡把排序結果拆成五個彼此不重疊的提問。每一頁只呈現它量到的"
        "數字，並在頁首用「範圍說明」寫出這些數字量的是什麼、算在哪批列上、"
        "看不見什麼。判讀留給讀者。</p>"
        + table
        + "<p><strong>編號的意思</strong>：由「資料與訓練設定造成的」往"
        "「模型學到什麼」再往「排序結果本身」推進。前一層解釋得掉的部分，"
        "後一層就不必重複歸因——這是歸因的優先權，也是預設的閱讀順序。</p>"
        "<p><strong>編號不是硬閘門</strong>：已實作的項目每次都會跑、都會"
        "呈現，前一項的結果不會擋掉後一項；任何一頁都可以單獨打開來讀。</p>"
        "<p>狀態欄標「尚未進 registry」的項目還沒有實作，這次執行不會有"
        "它們的頁面；下方清單列出的就是本次實際寫出的全部頁面。</p>"
    )


def assemble_diagnosis_pages(results: dict, parameters: dict, out_dir) -> list:
    """把每項診斷的結果組成獨立頁面。本函式不認識任何單一診斷。

    Args:
        results: ``{診斷名: compute 的輸出 dict}``。缺席或 ``render`` 回 ``None``
            （例如該項停用）的診斷不會產生頁面——**缺席是「這頁不存在」，不是
            「這頁是空的」**；空頁看起來像「量到了、結果什麼都沒有」。
        out_dir: 頁面輸出目錄（與各診斷 JSON 同一個 ``diagnosis/`` 目錄）。

    Returns:
        實際寫出的檔案路徑（``plotly.min.js`` 最先、各頁、``index.html`` 最後）。
    """
    import dataclasses
    import importlib

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.report import Page
    from recsys_tfb.report.pages import write_pages

    pages = []
    for i, name in enumerate(DIAGNOSES, start=1):
        result = (results or {}).get(name)
        if result is None:
            continue
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        section = mod.render(result, parameters)
        if section is None:
            continue
        slug = f"{i:02d}-{name.replace('_', '-')}"   # 數字前綴＝閱讀順序
        # SCOPE.sampling 在這裡統一填，不是每項診斷自己填：五項共用同一份
        # diagnosis_sample，sampling_description 永遠在同一個位置。讓各診斷
        # 各帶一個 hook 等於同一段 replace 被抄五次。
        scope = dataclasses.replace(
            mod.SCOPE,
            sampling=(result.get("sample_meta", {}) or {}).get(
                "sampling_description", ""),
        )
        pages.append(Page(slug=slug, title=mod.TITLE,
                          scope=scope, sections=(section,)))
    if not pages:
        # 一頁都沒有就完全不落地。否則會留下一個「index.html 說有五項、清單
        # 是空的、外加 3.5MB plotly.min.js」的目錄，看起來像跑過但什麼都沒
        # 量到——那是本重構要避免的誤讀，不是「誠實地呈現沒有資料」。
        return []
    return write_pages(pages, out_dir=out_dir,
                       index_title="排序診斷",
                       index_intro=_diagnosis_index_intro())


def build_diagnosis_links_section(
    diagnosis_pages: list | None,
    parameters: dict,
) -> ReportSection | None:
    """主報表指向診斷頁的入口。**只放連結，不放任何診斷數字。**

    數字複製一份到主報表就會有兩個真實來源；改了其中一邊，讀者無從得知哪一
    份是後改的。一頁都沒寫出來時回 ``None``——指向 404 的入口比沒有入口更糟。

    連結是相對路徑：主報表在 ``…/<snap_date>/report.html``，診斷頁在同層的
    ``diagnosis/``，兩者一起搬移時連結仍有效。
    """
    if not diagnosis_pages or not _section_on(parameters, "diagnosis_links"):
        return None
    n_pages = sum(
        1 for p in diagnosis_pages
        if str(p).endswith(".html") and not str(p).endswith("index.html")
    )
    return ReportSection(
        title="排序診斷（獨立頁面）",
        description=(
            '<a href="diagnosis/index.html">診斷索引 diagnosis/index.html</a>'
            f"　—　本次寫出 {n_pages} 頁。索引頁說明每一項回答什麼、排除"
            "什麼，各頁的數字與範圍說明都留在該頁，這裡不複製一份。"
        ),
    )


def assemble_report(
    metrics: dict,
    parameters: dict,
    baseline_metrics: dict | None = None,
    diagnostics_frames: dict | None = None,
    metric_ci: dict | None = None,
    offset_sweep: dict | None = None,
    pair_ledger: dict | None = None,
    diagnosis_pages: list | None = None,
) -> str:
    """Assemble every enabled section (the ``candidates`` list below is the
    authoritative order) into the final HTML string."""
    candidates = [
        build_headline_section(metrics, parameters),
        build_dataset_overview_section(metrics, parameters),
        build_primary_map_section(metrics, parameters, metric_ci=metric_ci),
        build_guardrail_recall_section(metrics, parameters),
        build_per_item_attr_section(metrics, parameters, metric_ci=metric_ci),
        build_diagnosis_links_section(diagnosis_pages, parameters),
        build_offset_sweep_section(offset_sweep, parameters),
        build_pair_ledger_section(pair_ledger, parameters),
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
