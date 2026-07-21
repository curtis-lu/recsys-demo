"""Report section assembly. One pure function per section; no Spark.

Each builder takes the small aggregated metrics dict (from
metrics_spark.compute_all_metrics) + parameters and returns a ReportSection
(or None when its config toggle is off). assemble_report wires the enabled
sections into the final HTML.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

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


def build_overview_section(
    metrics: dict, parameters: dict, metric_ci: dict | None = None
) -> ReportSection:
    """概覽（定向）：這份報表回答什麼、規模／分母、關鍵數、往哪找。

    presentation §一.1：規模／歸一化分母與嚴重度訊號分開標——分母混進關鍵數
    表會被讀成好壞。頭號指標＝macro per-item mAP（item 等權，＋CI 抽樣估計）；
    overall per-query mAP 並列為「另一種加權」，不宣稱哪個才對（不變量 4）。
    """
    overall = metrics.get("overall", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_prod = _n_products(metrics)
    ks = _resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), n_prod)

    tables: list[pd.DataFrame] = []
    titles: list[str] = []

    # 關鍵指標 1：macro per-item mAP（頭號，含 bootstrap CI）——沿用 metric_ci
    # 的讀法（macro / sample），避免定義漂移。
    ci_note = ""
    if metric_ci and metric_ci.get("enabled") and metric_ci.get("macro"):
        m = metric_ci["macro"]
        sample_meta = metric_ci.get("sample", {}) or {}
        tables.append(pd.DataFrame(
            [{"AP（點估）": m.get("ap"), "CI 2.5%": m.get("ci_low"),
              "CI 97.5%": m.get("ci_high"),
              "CI 用 query 數": sample_meta.get("n_queries_sampled")}],
            index=["macro per-item mAP"],
        ))
        titles.append("頭號指標：macro per-item mAP（item 等權，含 bootstrap CI）")
        n_boot = metric_ci.get("n_boot")
        sd = sample_meta.get("sampling_description", "")
        ci_note = (
            f"　CI 為 cluster bootstrap（cluster＝客戶，B＝{n_boot}）在診斷母體上"
            f"重抽得到；{sd}點估 AP 與衡量指標的全量 macro map_attr@all 相同。"
        )

    # 關鍵指標 2：overall per-query mAP@k（另一種加權，並列不比高下）
    card = {
        f"map@{k}": overall.get(f"map@{_k_to_lookup(k, n_prod)}") for k in ks
    }
    t_overall = pd.DataFrame([card]).T
    t_overall.columns = ["value"]
    tables.append(t_overall)
    titles.append("overall mAP@k（per-query 等權，另一種加權）")

    # 規模／分母（非好壞，明標與關鍵數分開）
    totals = metrics.get("dataset_overview", {}).get("totals", {}) or {}
    scale = {
        "有正例 query 數 n_queries": metrics.get("n_queries"),
        "排除 query 數 n_excluded_queries": metrics.get("n_excluded_queries"),
        "正例列數 n_positives": totals.get("n_positives"),
        "母體正樣本率（÷全體候選列）": totals.get("positive_rate"),
        "每客戶平均正例數 avg_positives_per_customer":
            totals.get("avg_positives_per_customer"),
    }
    t_scale = pd.DataFrame([scale]).T
    t_scale.columns = ["value"]
    tables.append(t_scale)
    titles.append("規模／分母（以下為分母與規模，非好壞）")

    # 導覽：想回答什麼 → 看哪一區（RangeIndex → render 端自動藏流水號）
    nav = pd.DataFrame({
        "想回答的問題": [
            "模型整體排得好不好",
            "哪些 item／segment 排得弱",
            "每個 item 的分數與名次分布長怎樣",
            "跟熱門度（popularity）比如何",
            "想深入各項診斷（排序偏移、item 能力、壓制帳本…）",
            "本次量到什麼、沒量到什麼",
        ],
        "看哪一區": [
            "衡量指標",
            "衡量指標（per-item／per-segment）",
            "per-item 細部拆解",
            "baseline",
            "排序診斷（獨立報表）",
            "完整性檢查",
        ],
    })
    tables.append(nav)
    titles.append("導覽：想回答什麼 → 看哪一區")

    return ReportSection(
        title="概覽",
        description=(
            "這份報表幫你判斷這個模型在 per-query 排序上表現如何、好壞落在哪些 "
            "item／segment、以及相對 popularity baseline 的位置。以下攤開多個粒度"
            "與角度，判斷留給你。頭號指標為 macro per-item mAP（item 等權）；"
            "overall mAP 為 per-query 等權，是另一種加權，並列呈現、不比高下。"
            + ci_note
        ),
        tables=tables,
        table_titles=titles,
    )


def build_core_concept_section(parameters: dict) -> ReportSection:
    """核心概念（地基）：講清一次原子量，後面各區都是它換切法。

    presentation §一.2：定義 ＋ 用一個具體數字走一遍 ＋「下面每區＝它加總到
    什麼粒度」的地圖。不各區重複這條定義（會漂移）。
    """
    cols = ((parameters.get("schema", {}) or {}).get("columns", {}) or {})
    time_col = cols.get("time", "snap_date")
    entity = cols.get("entity", ["cust_id"])
    entity_str = "×".join(entity) if isinstance(entity, list) else str(entity)
    item_col = cols.get("item", "prod_name")
    score_col = cols.get("score", "score")
    label_col = cols.get("label", "label")

    description = (
        f"一個 query＝一組（{time_col} × {entity_str}）。query 內的候選 "
        f"{item_col} 依模型分數 {score_col} 由高到低排名；{label_col}=1 的是"
        f"正例。下面每一個數字都是「這個 per-query 排序結果」加總到不同粒度——"
        "同一個量，換一種切法。"
    )
    formula = (
        "AP@k = (1 / R) · Σ(i=1..k) rel_i · P@i\n"
        "  P@i = 前 i 名中的正例數 / i　（前 i 名的精確率）\n"
        "  rel_i = 第 i 名是正例則 1、否則 0\n"
        "  R = 該 query 的正例總數（total_rel）；分母是 R、不是 min(k, R)——"
        "k < R 時 AP@k 追不到 1（頂多 k 個正例能進前 k、卻除以較大的 R）"
    )
    bullets = [
        f"例：某 query 有 4 個候選 {item_col}、R=2 個正例，排名後正例落在第 1、"
        "第 3 名。",
        "P@1 = 1/1 = 1.0、P@2 = 1/2 = 0.5、P@3 = 2/3 ≈ 0.667。",
        "AP@3 = (1/2)·(1·1.0 + 0·0.5 + 1·0.667) ≈ 0.83；"
        "AP@1 = (1/2)·(1·1.0) = 0.50——分母固定為 R=2，k=1 只納入第 1 名那個"
        "正例的貢獻、仍除以 2。",
        "手算核對點：k=1 時每個 query 的 AP@1 = rel_1 / R，正好等於 recall@1，"
        "所以整份報表的 overall map@1 會等於 recall@1（衡量指標段可對照）。",
        "地圖（下面每區＝這個 per-query AP 加總到不同粒度）："
        "overall＝跨 query 等權平均；per-item＝把 AP 歸因到正例所屬的 "
        f"{item_col} 後 item 等權（macro）；per-segment＝依 segment 分組平均；"
        "per-item 細部拆解＝同一批排名的分數／名次分布側面。",
    ]
    return ReportSection(
        title="核心概念 — 一個 query 的排序",
        description=description,
        formula=formula,
        bullets=bullets,
    )


def build_dataset_overview_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "dataset_overview"):
        return None
    ov = metrics.get("dataset_overview", {})
    totals_d = ov.get("totals", {}) or {}
    totals = pd.DataFrame([totals_d]).T
    totals.columns = ["value"]
    by_snap = pd.DataFrame(ov.get("by_snap_date", {})).T

    # per-item 正例組成：正例數 / 正樣本率 / 正例佔比（＝n_positives÷總正例，
    # render 端純算術、無 Spark）。密集候選下三欄同序，ScopeNote 於 description。
    total_pos = totals_d.get("n_positives") or 0
    by_item_rows: dict = {}
    # per-item 列序統一按字母（與衡量指標、item-share 對齊）
    for item, d in sorted((ov.get("by_item", {}) or {}).items()):
        n_pos = d.get("n_positives")
        by_item_rows[item] = {
            "正例數": n_pos,
            "正樣本率(÷此item候選列)": d.get("positive_rate"),
            "正例佔比": (n_pos / total_pos)
            if (n_pos is not None and total_pos) else None,
        }
    by_item = pd.DataFrame(by_item_rows).T

    tables = [totals, by_snap, by_item]
    titles = ["整體 totals", "各期 by snap_date", "per-item 正例組成"]
    collapsed = [False, True, False]   # 各期單 snap 時＝totals，預設收合
    # per-segment 正例組成：正例數／正樣本率（÷該 segment 候選列）／query 數佔比。
    # 第 3 欄用「query 數佔比」（該 segment 佔多少 query，反映 segment 大小）——
    # segment 分的是 query、per-item 分的是正例，兩者不同軸，故不與 per-item 的
    # 「正例佔比」互換。by_segment 由 compute_dataset_overview 依 active_seg_col
    # 聚合；缺席（舊 artifact 或無 segment 欄）則不呈現此表。
    by_seg = ov.get("by_segment", {}) or {}
    if by_seg:
        seg_rows = {}
        for seg, d in sorted(by_seg.items()):
            seg_rows[seg] = {
                "正例數": d.get("n_positives"),
                "候選列數": d.get("n_rows"),
                "正樣本率(÷此segment候選列)": d.get("positive_rate"),
                "query 數佔比": d.get("query_share"),
            }
        tables.append(pd.DataFrame(seg_rows).T)
        titles.append("per-segment 正例組成")
        collapsed.append(False)
    cat = metrics.get("category")
    if cat:
        cat_by_item = (cat.get("dataset_overview", {}) or {}).get("by_item", {})
        if cat_by_item:
            tables.append(pd.DataFrame(cat_by_item).T)
            titles.append("by 大類（大類粒度，不與整體相加）")
            collapsed.append(False)   # 大類表預設展開（與 totals/by_item/by_segment 一致）

    return ReportSection(
        title="基本統計 — 資料集",
        description=(
            "整體規模與 per-item 的正例組成。per-item 三欄（正例數／正樣本率／"
            "正例佔比）在密集候選集下同序（僅換分母或讀法），非三個獨立軸；候選"
            "覆蓋率因每 item 覆蓋全部 query 恆為 100%，故不列。注意「正樣本率」有"
            "兩種分母：概覽的母體 positive_rate 除以全體候選列（此 run 5,232），"
            "此處 per-item 那欄除以「該 item 自己的候選列數」（此 run 每 item 654），"
            "量級不同。「by 大類」是大類粒度（每客戶每大類一列、label＝該大類任一"
            "子產品為正例、大類分數＝子產品最佳分數），故其正例數 ≤ item 粒度合計、"
            "不與整體 n_positives 相加。per-segment 正例組成：正例數、候選列數、"
            "正樣本率、query 數佔比（該 segment 佔多少 query）。每-query 正例數"
            "分佈為後續階段。"
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
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
    """Rows = items; bare ``@k`` cols (from hit_rate@k) + mean_pos.

    欄名裸 @k、family（recall）在呼叫端的表標題交代——與 A 塊 per-query 表
    及 per-item map_attr 表的欄名慣例一致（family 不重複塞進欄名）。
    """
    return _per_item_metric_table(
        per_item, ks, n_prod, "hit_rate", "@{k}",
        extra_cols={"mean_pos": "mean_pos"}, macro_metrics=macro_metrics,
    )


def _families_by_k_table(overall: dict, ks: list, n_prod: int) -> pd.DataFrame:
    """單一 per-query aggregate → rows=[map, precision, recall]、cols=@k。

    給「單一彙總」用（overall、大類 overall）：只有一個實體，故用指標家族當列。
    explicit family（map/precision/recall）→ 天然不含 ndcg。
    """
    rows = {}
    for fam in ("map", "precision", "recall"):
        rows[fam] = {f"@{k}": overall.get(f"{fam}@{_k_to_lookup(k, n_prod)}")
                     for k in ks}
    return pd.DataFrame(rows).T


def _entities_by_k_table(
    per_entity: dict, macro: dict | None, ks: list, n_prod: int, fam: str
) -> pd.DataFrame:
    """多實體 per-query → rows=實體（Macro 頂列）、cols=@k，單一 metric family。

    給「多實體拆分」用（per-segment）：拆成 map/precision/recall 各一張，實體當列。
    """
    src = {_MACRO_LABEL: macro, **per_entity} if macro else dict(per_entity)
    data = {}
    for ent, m in src.items():
        data[ent] = {f"@{k}": (m or {}).get(f"{fam}@{_k_to_lookup(k, n_prod)}")
                     for k in ks}
    return pd.DataFrame(data).T


def build_metrics_section(
    metrics: dict, parameters: dict, metric_ci: dict | None = None
) -> ReportSection | None:
    """衡量指標：分兩塊、各塊內維度與方向一致。

    A｜per-query 指標（map/precision/recall）——overall、per-segment、大類 overall。
    B｜per-item 歸因（map_attr/recall）——per-item、大類 per-item。precision 是
    per-query 量（整組 top-k 的性質），無法歸因到單一 item，故 B 塊沒有 precision。
    方向規則：單一彙總用指標家族當列；多實體拆分用實體當列。所有表 k 欄統一＝
    [1,2,3,4,5,all]，每張表只一個 metric family（避免寬混表）。全克制、明細收合。
    """
    if not _section_on(parameters, "primary_map"):
        return None
    overall = metrics.get("overall", {})
    # per-item 列序全報表統一按字母（與 per-item 細部拆解的 item-share 表對齊）
    per_item = dict(sorted((metrics.get("per_item", {}) or {}).items()))
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    n_prod = _n_products(metrics)
    ks = _resolve_display_k([1, 2, 3, 4, 5, "all"], n_prod)  # 全表統一 k

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # 頭號指標：macro per-item mAP CI（可見，放最前）
    if metric_ci and metric_ci.get("enabled") and metric_ci.get("macro"):
        m = metric_ci["macro"]
        sm = metric_ci.get("sample", {}) or {}
        _add(
            pd.DataFrame(
                [{"AP（點估）": m.get("ap"), "CI 2.5%": m.get("ci_low"),
                  "CI 97.5%": m.get("ci_high"),
                  "CI 用 query 數": sm.get("n_queries_sampled")}],
                index=["macro per-item mAP"],
            ),
            "頭號指標：macro per-item mAP（item 等權，含 bootstrap CI）",
            False,
        )

    # ===== Block A：per-query 指標（map / precision / recall）=====
    _add(_families_by_k_table(overall, ks, n_prod),
         "A · per-query｜overall（列＝map/precision/recall）", False)
    per_segment = metrics.get("per_segment", {})
    if per_segment:
        macro_seg = metrics.get("macro_avg", {}).get("by_segment", {})
        for fam in ("map", "precision", "recall"):
            _add(_entities_by_k_table(per_segment, macro_seg, ks, n_prod, fam),
                 f"A · per-query｜per-segment {fam}@k（列＝segment）", True)
    cat = metrics.get("category")
    cks = None
    if cat:
        n_cat = int(
            cat.get("dataset_overview", {}).get("totals", {}).get("n_products", 0)
        )
        cks = _resolve_display_k([1, 2, 3, 4, 5, "all"], n_cat)
        _add(_families_by_k_table(cat.get("overall", {}), cks, n_cat),
             "A · per-query｜大類 overall（列＝map/precision/recall）", True)

    # ===== Block B：per-item 歸因（map_attr / recall；無 precision）=====
    b_map = _per_item_metric_table(
        per_item, ks, n_prod, "map_attr", "@{k}", macro_metrics=macro_item,
    )
    if metric_ci and metric_ci.get("enabled"):
        ci_items = metric_ci.get("per_item", {}) or {}
        ci_macro = metric_ci.get("macro") or {}

        def _ci_val(idx, field):
            src = ci_macro if idx == _MACRO_LABEL else ci_items.get(idx, {})
            return src.get(field)

        for col, field in (("CI 2.5%", "ci_low"), ("CI 97.5%", "ci_high"),
                           ("n_pos（CI 用）", "n_pos")):
            b_map[col] = [_ci_val(idx, field) for idx in b_map.index]
    _add(b_map, "B · per-item 歸因｜map_attr@k（列＝item，＋CI 上下界）", True)
    _add(_per_item_recall_table(per_item, ks, n_prod, macro_metrics=macro_item),
         "B · per-item 歸因｜recall@k（列＝item）", True)
    if cat:
        cat_macro_item = cat.get("macro_avg", {}).get("by_item", {})
        cat_pi = dict(sorted((cat.get("per_item", {}) or {}).items()))
        _add(_per_item_metric_table(cat_pi, cks, n_cat, "map_attr",
                                    "@{k}", macro_metrics=cat_macro_item),
             "B · 大類 per-item 歸因｜map_attr@k（列＝大類）", True)
        _add(_per_item_recall_table(cat_pi, cks, n_cat,
                                    macro_metrics=cat_macro_item),
             "B · 大類 per-item 歸因｜recall@k（列＝大類）", True)

    return ReportSection(
        title="衡量指標",
        description=(
            "分兩塊、各塊內維度與方向一致。A｜per-query 指標（map／precision／"
            "recall）——對 overall、per-segment、大類 overall；precision@k 是"
            " per-query 量（整組 top-k 命中幾個），只在這塊。B｜per-item 歸因"
            "（map_attr／recall）——對 per-item、大類 per-item；precision 無法歸因"
            "到單一 item（那是整組 top-k 的性質），故 B 塊沒有 precision。方向規則："
            "單一彙總（overall、大類 overall）用指標家族當列，多實體拆分（per-"
            "segment、per-item、大類 per-item）用實體當列；所有表 k 欄一致＝"
            "[1,2,3,4,5,all]、每張表一個 metric family。頭號指標＝macro per-item "
            "mAP（item 等權，含 bootstrap CI；CI 上下界的點估＝該列 map_attr@all）；"
            "overall per-query mAP 是另一種加權，並列不比高下。手算核對：overall "
            "map@1 = recall@1（見核心概念，AP@k 分母＝R）。K=產品數時 precision "
            "退化為 base rate、recall 恆為 1。CI 僅算到 item 層（大類 per-item 無 "
            "bootstrap CI）。per-item 列序統一按字母。明細表點標題展開。"
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )


def _item_share_by_rank(counts_frame: pd.DataFrame) -> pd.DataFrame:
    """欄正規化：每個 rank 欄 ÷ 欄和 → 各 item 在該 rank 位置的佔比。

    每個 rank 欄加總=1（誰佔據該名次）。全 0 欄（0/0）得 NaN、render 端空白。
    依 §二，正規化後的矩陣用「按欄讀的數字表」呈現，不掛全域色階 heatmap。
    """
    col_sums = counts_frame.sum(axis=0)
    return counts_frame.divide(col_sums, axis=1)


def build_item_detail_section(
    report_aggregates: dict | None, parameters: dict
) -> ReportSection | None:
    """per-item 細部拆解（原診斷區升為頂層）。

    同一批排名的分數／名次分布側面。沿用 score 分布圖與 rank 計數 heatmap；
    新增 item-share-by-rank（欄正規化，數字表，G#1）＋ positive rate by rank
    數字表。依「排序不是校準」，calibration 曲線移到獨立診斷報表、本段不畫
    （即使 payload 有 calibration 鍵）。升為頂層（collapsible=False）。
    """
    if not _section_on(parameters, "diagnostics"):
        return None
    # 與 build_diagnostics_figures 同一個「有沒有 score_histogram 家族」判斷；
    # 只有 calibration 沒有分布家族時，本段不畫。
    if not report_aggregates or "score_histogram" not in report_aggregates:
        return None

    from recsys_tfb.evaluation.diagnostics_spark import frame_from_json
    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )

    cols = report_aggregates["columns"]
    item_col, label_col = cols["item"], cols["label"]
    # 先群組所有圖（score 分布 2 張＋rank 矩陣 heatmap 3 張），再放數字表——
    # 段內視覺一致（section 先 render figures 再 render tables）。positive rate
    # 是有界 [0,1] 的率矩陣，全域色階有意義 → 用 heatmap。
    figs = [
        plot_score_histogram(
            frame_from_json(report_aggregates["score_histogram"]),
            item_col=item_col),
        plot_score_boxplot_by_label(
            frame_from_json(report_aggregates["score_box_by_label"]),
            item_col=item_col, label_col=label_col),
        plot_rank_heatmap(
            frame_from_json(report_aggregates["rank_counts"])),
        plot_positive_rank_heatmap(
            frame_from_json(report_aggregates["positive_rank_counts"])),
        plot_positive_rate_rank_heatmap(
            frame_from_json(report_aggregates["positive_rate"])),
    ]

    # item share by rank：逐欄正規化（每 rank 欄加總=1），刻意用數字表——掛全域
    # 色階 heatmap 會誘導跨欄比色誤讀（§二）。放在所有 heatmap 之後。
    rank_counts = frame_from_json(report_aggregates["rank_counts"])
    pos_rank_counts = frame_from_json(report_aggregates["positive_rank_counts"])
    tables = [
        _item_share_by_rank(rank_counts),
        _item_share_by_rank(pos_rank_counts),
    ]
    titles = [
        "item share by rank（query 數，欄正規化：每 rank 各 item 佔比，欄和=1）",
        "item share by rank（positive query 數，欄正規化）",
    ]
    return ReportSection(
        title="per-item 細部拆解",
        description=(
            "同一批排名的分數與名次分布側面。先看圖（群組在前）：score 分布、"
            "score by label、rank 計數 heatmap、positive rank 計數 heatmap、"
            "positive rate by rank heatmap；再看數字表：item share by rank（欄"
            "正規化，看誰佔據各名次）。item share 刻意用數字表而非 heatmap——它"
            "是逐欄正規化（每欄加總=1），掛全域色階會誘導跨欄比色誤讀，請在同一"
            "欄內比。依「排序不是校準」，校準曲線移到獨立診斷報表、本段不畫。rank "
            "計數的欄和＝總 query 數。明細數字表點標題展開。"
        ),
        figures=figs,
        tables=tables,
        table_titles=titles,
        collapsed_tables=[True, True],
        collapsible=False,
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
    # overall 三表用 k superset（使用者指定，k 放欄位）
    k_super = _resolve_display_k([1, 2, 3, 4, 5, "all"], n_prod)
    lookback = (
        ((parameters.get("evaluation", {}) or {}).get("baseline", {}) or {})
        .get("lookback_months")
    )

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # [1] popularity 排名組成（總計 count + 平均每月）；各月明細/趨勢＝Phase 2。
    pcounts = (baseline_metrics or {}).get("purchase_counts") or {}
    if pcounts:
        sorted_items = sorted(
            pcounts.items(), key=lambda kv: kv[1], reverse=True
        )
        pop_cols = {"count": [v for _, v in sorted_items]}
        if lookback:
            pop_cols["平均每月"] = [
                round(v / lookback, 1) for _, v in sorted_items
            ]
        pop_cols["rank"] = list(range(1, len(sorted_items) + 1))
        _add(
            pd.DataFrame(pop_cols, index=[k for k, _ in sorted_items]),
            "popularity 排名組成", False,
        )

    # [2] overall：mAP / recall / precision 各一張，rows=[Model,Baseline,Δ]、
    #     cols=@k（superset），明細收合。explicit family → 天然不含 ndcg。
    overall_a = comp["result_a"].get("overall", {}) or {}
    overall_b = comp["result_b"].get("overall", {}) or {}
    overall_delta = comp["overall_delta"]
    for fam, label in (("map", "mAP"), ("recall", "recall"),
                       ("precision", "precision")):
        data = {}
        for who, src in (("Model", overall_a), ("Baseline", overall_b),
                         ("Δ", overall_delta)):
            data[who] = {
                f"@{k}": src.get(f"{fam}@{_k_to_lookup(k, n_prod)}")
                for k in k_super
            }
        _add(pd.DataFrame(data).T, f"overall {label}@k (M/B/Δ)", True)

    # [3] per-item compare tables — only when baseline has per_item；明細收合。
    per_item_a = comp["result_a"].get("per_item", {}) or {}
    per_item_b = comp["result_b"].get("per_item", {}) or {}
    per_item_delta = comp.get("per_item_delta", {}) or {}
    macro_a = (metrics.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (baseline_metrics.get("macro_avg", {}) or {}).get("by_item")
    if per_item_b:
        # 兩張 per-item M/B/Δ 用同一組 k（attr_ks＝primary_map_k），彼此一致；
        # 為控寬用縮減集，與衡量指標 per-item 的完整 [1..5,all] 不同（描述封邊）。
        for metric_key, col_fmt, ks, title in (
            ("hit_rate", "recall@{k}", attr_ks, "per-item recall@k (M/B/Δ)"),
            ("map_attr", "map_attr@{k}", attr_ks,
             "per-item map_attr@k (M/B/Δ)"),
        ):
            _add(
                _per_item_metric_compare_table(
                    per_item_a, per_item_b, per_item_delta,
                    ks, n_prod, metric_key, col_fmt,
                    macro_a=macro_a, macro_b=macro_b,
                ),
                title, True,
            )

    lookback_note = (
        f"popularity 以過去 {lookback} 個月的歷史購買計數重排。"
        if lookback else ""
    )
    return ReportSection(
        title="baseline — popularity 對照",
        description=(
            f"Model 相對 popularity baseline 的位置。{lookback_note}popularity "
            "排名組成為各 item 跨月合計（總計＋平均每月）；各月明細與趨勢為後續"
            "階段（需保留逐月計數）。overall 的 mAP／recall／precision 各一張表、"
            "k 放欄位、點標題展開。對照僅做 overall 與 per-item 兩層"
            "（per-segment／大類 vs baseline 從略）；per-item 兩張表 k 欄一致＝"
            "[1,3,5,all]（控寬，與衡量指標 per-item 的完整 [1..5,all] 不同）。"
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )


_GLOSSARY = [
    ("mAP@k", "per-query Average Precision@k 對 query 平均；主指標"),
    ("recall@k (per-item)",
     "P(rank(P)≤k | P 為正)，命中事件等權；map_attr@k 的互補角度"
     "（正例有沒有進 top-k），不下 pass/fail"),
    ("precision@k", "per-query 命中數/k；k=產品數時退化為 base rate"),
    ("map_attr@k",
     "某產品為正解時 ap_contrib@k 的平均（＝mAP@k 拆到單一產品的貢獻，非該"
     "產品自己的 mAP@k）。ap_contrib@k：該正例產品排名 r 若 ≤k 則為 P@r（前 r "
     "名精確率）、否則 0——就是核心概念 AP@k 分子 Σ rel_i·P@i 裡屬於這個產品的"
     "那一項。客戶該買它、模型排越前 → 值越高"),
    ("mean_pos", "產品為正時平均排名位置（越小越好）"),
    ("Macro 平均",
     "對所有產品（或 segment）等權平均；與 query 等權的 overall 不同"),
    ("base rate", "母體正樣本率"),
    ("macro per-item mAP",
     "各 item 的 map_attr 等權平均；本框架頭號指標（item 等權），與 query "
     "等權的 overall mAP 是兩種加權、並列不比高下"),
    ("正例佔比",
     "某 item 的正例數 ÷ 全體正例數；密集候選下與正例數同序（僅換分母／讀法）"),
    ("item share by rank",
     "rank 計數矩陣逐欄正規化——某 rank 位置上各 item 佔的比例（每欄加總=1），"
     "回答「誰佔據該名次」"),
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
# registry 補一行，本檔零改動。（offset_sweep 這項既有診斷仍在計算層產出
# offset_sweep.json，但其主報表呈現段已移除——後繼 score_shift 走
# ``build_diagnosis_links_section`` 連出的獨立診斷報表。）
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
        results: ``{診斷名: compute 的輸出 dict}``。缺席或 ``render`` 回**空
            序列**（例如該項停用）的診斷不會產生頁面——**缺席是「這頁不存在」，
            不是「這頁是空的」**；空頁看起來像「量到了、結果什麼都沒有」。
            ``render`` 回傳的是多個 section（一張圖／一張表各一個 section，
            各自帶標題、公式與重點），整頁的 section 順序即閱讀順序。
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
        sections = mod.render(result, parameters)
        if not sections:
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
                          scope=scope, sections=tuple(sections)))
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
        title="排序診斷（獨立報表）",
        description=(
            '<a href="diagnosis/index.html">診斷索引 diagnosis/index.html</a>'
            f"　—　本次寫出 {n_pages} 頁。索引頁說明每一項回答什麼、排除"
            "什麼，各頁的數字與範圍說明都留在該頁，這裡不複製一份。"
            "（分流分析的後繼 score_shift 為獨立診斷報表，日後於此連出。）"
        ),
    )


def build_completeness_section(
    metrics: dict, parameters: dict, metric_ci: dict | None = None
) -> ReportSection:
    """完整性檢查（殿後）：本次執行的事實 ＋「什麼看似正常其實沒量到」。

    presentation §一.4：交代邊界。只陳述事實，不評級。
    """
    eval_p = parameters.get("evaluation", {}) or {}
    totals = (metrics.get("dataset_overview", {}) or {}).get("totals", {}) or {}
    metric_p = eval_p.get("metric", {}) or {}
    sample_meta = (metric_ci or {}).get("sample", {}) or {}

    mk = metric_p.get("k")
    facts = {
        "k_values": eval_p.get("k_values"),
        "有正例 query 數 n_queries": metrics.get("n_queries"),
        "排除 query 數 n_excluded_queries": metrics.get("n_excluded_queries"),
        "正例列數 n_positives": totals.get("n_positives"),
        "產品數 n_products": totals.get("n_products"),
        "metric.weight_alpha（item 加權指數 α；0＝item 等權）":
            metric_p.get("weight_alpha"),
        "metric.k（AP 截斷 k；無＝不截斷、算全長）":
            ("無（不截斷）" if mk is None else mk),
        "metric.min_positives（觀察名單門檻；0＝不設）":
            metric_p.get("min_positives"),
        "metric.shrinkage_k（向 pooled 平均收縮強度；0＝不收縮）":
            metric_p.get("shrinkage_k"),
        "抽樣描述 sampling_description": sample_meta.get("sampling_description"),
    }
    facts_tbl = pd.DataFrame([facts]).T
    facts_tbl.columns = ["value"]

    bullets = [
        # 刻意不寫出被隱藏指標的名字：整份報表有一條端到端護欄禁止該字串出現
        # （避免值洩漏）；這裡只陳述「算了但不呈現」這件事。
        "部分排序衍生指標有算但刻意不呈現（本框架目標是排序 macro mAP，"
        "非機率校準）。",
        "每-query 正例數分佈本版未算（Phase 2，需新增 per-query 聚合）。",
        "baseline 各月明細與趨勢未落地（Phase 2，需保留逐月計數）。",
        "候選集為密集時每 item 候選覆蓋率恆 100%——per-item 正例佔比與正例數"
        "同序，非獨立軸。",
    ]
    return ReportSection(
        title="完整性檢查",
        description=(
            "本次執行的事實（k、規模、抽樣、metric 參數）與「什麼情況數字看起來"
            "正常、其實沒量到或不完整」。放在最後，交代邊界。"
        ),
        tables=[facts_tbl],
        table_titles=["本次執行事實"],
        bullets=bullets,
    )


def assemble_report(
    metrics: dict,
    parameters: dict,
    baseline_metrics: dict | None = None,
    report_aggregates: dict | None = None,
    metric_ci: dict | None = None,
    offset_sweep: dict | None = None,
    diagnosis_pages: list | None = None,
) -> str:
    """Assemble every enabled section (the ``candidates`` list below is the
    authoritative order) into the final HTML string.

    8 段 spine（目的驅動、由粗到細、克制）：概覽 → 核心概念 → 基本統計 →
    衡量指標 → per-item 細部拆解 → baseline → 排序診斷連結 → 完整性檢查 →
    詞彙表。offset_sweep 不再進主報表（其後繼 score_shift 走診斷連結）；
    ``offset_sweep`` 參數保留僅為簽章相容（未使用）。
    """
    candidates = [
        build_overview_section(metrics, parameters, metric_ci=metric_ci),
        build_core_concept_section(parameters),
        build_dataset_overview_section(metrics, parameters),
        build_metrics_section(metrics, parameters, metric_ci=metric_ci),
        build_item_detail_section(report_aggregates, parameters),
        build_baseline_section(metrics, baseline_metrics, parameters),
        build_diagnosis_links_section(diagnosis_pages, parameters),
        build_completeness_section(metrics, parameters, metric_ci=metric_ci),
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
