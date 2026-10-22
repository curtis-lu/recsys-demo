"""衡量指標 (排序指標 in ADR-0031 decision 16): the ranking metrics, per
query group (overall, per segment, category overall) and per item."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.metric_catalog import (
    ITEM,
    QUERY_GROUP,
    entry,
    metrics_of,
)
from recsys_tfb.evaluation.metrics import (
    ALL_POSITIVE_KEY,
    all_positive_share,
    drop_all_positive_groups,
    format_share,
    n_groups_with_positive,
    resolved_all_k,
)
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_WHY,
    MACRO_LABEL,
    count_items,
    grain_bundles,
    headline_column,
    k_to_lookup,
    kept_ratio_phrase,
    macro_item_coverage,
    metrics_section_ks,
    section_on,
    unmatched_last,
    zero_positive_draw_on_test,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_metrics",)


def macro_coverage_suffix(
    per_item: dict, n_items: int, parameters: dict, macro: dict | None
) -> str:
    """Title suffix disclosing a single-sided per-item macro's coverage (bug 5).

    Returns ``""`` unless ``macro`` is truthy — the same test
    ``_per_item_metric_table`` uses (``if macro_metrics:``) to prepend the
    Macro row. The suffix describes that row's denominator, so it has to
    appear exactly when the row does. ``macro_coverage_suffix_mb`` tests
    ``is not None`` instead, matching its own table; using one helper on the
    other's table puts the suffix on a table without a Macro row (or drops it
    from one that has it) for an empty-dict macro, and nothing raises.
    """
    if not macro:
        return ""
    n = macro_item_coverage(per_item, parameters)
    return f"（參與 macro 的 item 數 {n}／全部 {n_items}）"


def _zero_positive_origin(parameters: dict) -> str:
    """「無正例的組」括號裡接在「不算進衡量指標」後面的那句。

    ``--post-training`` 吃的是 dataset 過濾後的 test 表：r（
    ``dataset.test_zero_positive_group_ratio``）為 0 時無正例的組在 dataset 就
    全丟了，帳上恆為 0；r > 0 時留下的是抽過的一部分。不說的話，讀者會把 0 讀成
    「資料裡沒有這種組」。監控模式沒有 dataset 抽樣，帳上是實際組數，回 ``""``。
    """
    ratio = zero_positive_draw_on_test(parameters)
    if ratio is None:
        return ""
    if ratio > 0:
        return (
            "；dataset 已依 dataset.test_zero_positive_group_ratio 抽過："
            f"{kept_ratio_phrase(ratio)}"
        )
    return (
        "；dataset 已依 dataset.test_zero_positive_group_ratio＝0 丟過，test 表"
        "只剩有正例的組"
    )


def _group_ledger_line(grain: str, bundle: dict, parameters: dict) -> str | None:
    """一個粒度的組數帳（#376）：有正例的組、其中全正、算進衡量指標、無正例的組。

    開關關：全正的組照樣算進衡量指標，「算進衡量指標」＝「有正例」。開關開：
    有正例＝全正（已排除）＋算進衡量指標。無正例的組兩種都不算進衡量指標，
    ``n_excluded_queries`` 也只數它們。帳上不說「算進 mAP」：當初的理由是同一份
    報表連結的診斷頁也印 macro per-item mAP、而且不排除全正的組；診斷頁隨 #532
    拿掉之後，這個措辭照留。

    缺 ``n_queries``／``n_excluded_queries``（更早的 bundle）回 ``None``。只缺
    全正組數的，是 #376 之前算的 ``metrics.json``：開關關著時指紋沒變，照樣會被
    畫出來，所以不報錯，印得出的照印，並說怎麼補。
    """
    n_pos = n_groups_with_positive(bundle)
    if n_pos is None:
        return None
    n_zero = bundle["n_excluded_queries"]
    n_all = bundle.get(ALL_POSITIVE_KEY)
    drop = drop_all_positive_groups(parameters)
    if n_all is None:
        head = (
            f"有正例的組 {n_pos:,}"
            + ("" if drop else "，全部算進衡量指標")
            + "（這份結果沒有全正的組數：它在這個計數加入之前算的，從 "
            "compute_metrics 重跑可得）"
        )
    elif drop:
        head = (
            f"有正例的組 {n_pos:,}，其中全正 {n_all:,} 已排除，"
            f"算進衡量指標的有 {n_pos - n_all:,}"
        )
    else:
        share = all_positive_share(bundle)
        pct = f"{format_share(share)}；" if share is not None else ""
        head = (
            f"有正例的組 {n_pos:,}，其中全正 {n_all:,}（{pct}{ALL_POSITIVE_WHY}）"
            f"照樣算進，算進衡量指標的有 {n_pos:,}"
        )
    return (
        f"組數帳（{grain}）：{head}；無正例的組 {n_zero:,}"
        f"（不算進衡量指標{_zero_positive_origin(parameters)}）。"
    )


def _per_item_metric_table(
    per_item: dict,
    ks: list,
    all_k: int,
    metric_key: str,
    col_fmt: str,
    extra_cols: dict[str, str] | None = None,
    macro_metrics: dict | None = None,
) -> pd.DataFrame:
    """Rows = items; one column per k named ``col_fmt.format(k=k)``, value
    pulled from ``per_item[item][f"{metric_key}@{k_to_lookup(k, all_k)}"]``.

    ``extra_cols`` maps an output column name to a flat (non-@k) per_item key,
    e.g. ``{"mean_pos": "mean_pos"}``.

    ``macro_metrics``: when given and non-empty, an equal-weight-average
    metrics dict (same key shape as a per_item value) is prepended as the
    top row labelled ``MACRO_LABEL``.
    """
    def _row(m: dict) -> dict:
        row = {
            col_fmt.format(k=k): m.get(f"{metric_key}@{k_to_lookup(k, all_k)}")
            for k in ks
        }
        for out_name, src_key in (extra_cols or {}).items():
            row[out_name] = m.get(src_key)
        return row

    data: dict = {}
    if macro_metrics:
        data[MACRO_LABEL] = _row(macro_metrics)
    for item, m in per_item.items():
        data[item] = _row(m)
    return pd.DataFrame(data).T


def _per_item_recall_table(
    per_item: dict, ks: list, all_k: int, macro_metrics: dict | None = None
) -> pd.DataFrame:
    """Rows = items; bare ``@k`` cols (from the per-item recall@k) + mean_pos.

    欄名裸 @k、family（recall）在呼叫端的表標題交代——與 A 塊 per-query 表
    及 per-item map_attr 表的欄名慣例一致（family 不重複塞進欄名）。
    """
    mean_pos = entry("mean_pos", ITEM)
    return _per_item_metric_table(
        per_item, ks, all_k, entry("recall", ITEM).name, "@{k}",
        extra_cols={mean_pos.display_name: mean_pos.key()},
        macro_metrics=macro_metrics,
    )


def _families_by_k_table(overall: dict, ks: list, all_k: int) -> pd.DataFrame:
    """單一 per-query aggregate → rows=指標目錄裡 query group 那種算法的每個
    指標、cols=@k。

    給「單一彙總」用（overall、大類 overall）：只有一個實體，故用指標家族當列。
    """
    # Rows are labelled by the metric's name, as the report always printed
    # them; the catalog's display names take over when ADR-0031 decision 16
    # relabels the section.
    rows = {}
    for fam in _query_group_names():
        rows[fam] = {f"@{k}": overall.get(f"{fam}@{k_to_lookup(k, all_k)}")
                     for k in ks}
    return pd.DataFrame(rows).T


#: Block B's item metrics drawn in an arrangement of their own: map_attr's
#: table, then recall's with mean_pos as an extra column. Every
#: other per-K item entry of the catalog gets a table of its own after them
#: (:func:`_other_item_metrics`).
_ARRANGED_ITEM_METRICS = ("map_attr", "recall", "mean_pos")


def _other_item_metrics() -> list:
    """The per-K item entries block B has no arrangement for, in catalog
    order: each is drawn like map_attr, one table per grain."""
    return [m for m in metrics_of(ITEM)
            if m.per_k and m.name not in _ARRANGED_ITEM_METRICS]


def _query_group_names() -> list[str]:
    """Block A's metrics: every query-group entry of ``metric_catalog``, in
    catalog order — one row of the overall tables, one per-segment table
    each."""
    return [m.name for m in metrics_of(QUERY_GROUP)]


def _entities_by_k_table(
    per_entity: dict, macro: dict | None, ks: list, all_k: int, fam: str
) -> pd.DataFrame:
    """多實體 per-query → rows=實體（Macro 頂列）、cols=@k，單一 metric family。

    給「多實體拆分」用（per-segment）：拆成 map/precision/recall 各一張，實體當列。
    """
    src = {MACRO_LABEL: macro, **per_entity} if macro else dict(per_entity)
    data = {}
    for ent, m in src.items():
        data[ent] = {f"@{k}": (m or {}).get(f"{fam}@{k_to_lookup(k, all_k)}")
                     for k in ks}
    return pd.DataFrame(data).T


def build(
    results: Mapping[str, dict], parameters: dict,
) -> ReportSection | None:
    """衡量指標：分兩塊、各塊內維度與方向一致。

    A｜per-query 指標（map/precision/recall）——overall、per-segment、大類 overall。
    B｜per-item 歸因（map_attr/recall）——per-item、大類 per-item。A 塊的
    precision@k 分母是 k（整組 top-k 的性質），拆不到單一 item 頭上，所以不在 B 塊；
    per-item 的 precision 分母是該 item 排進前 k 名的次數，是另一個指標。
    方向規則：單一彙總用指標家族當列；多實體拆分用實體當列。所有表 k 欄統一＝
    [1,2,3,4,5,all]，每張表只一個 metric family（避免寬混表）。全克制、明細收合。
    """
    if not section_on(parameters, "primary_map"):
        return None
    metrics = results["evaluation_metrics"]
    overall = metrics.get("overall", {})
    # per-item 列序全報表統一按字母（與 per-item 細部拆解的 item-share 表對齊）
    per_item = dict(sorted((metrics.get("per_item", {}) or {}).items()))
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    n_items = count_items(metrics)
    all_k = resolved_all_k(metrics)
    ks = metrics_section_ks(all_k)  # one K list for every table

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # ===== Block A：per-query 指標（map / precision / recall）=====
    families = "/".join(_query_group_names())
    _add(_families_by_k_table(overall, ks, all_k),
         f"A · per-query｜overall（列＝{families}）", False)
    per_segment = unmatched_last(metrics.get("per_segment", {}) or {})
    if per_segment:
        macro_seg = metrics.get("macro_avg", {}).get("by_segment", {})
        # (unmatched) 的定義貼在它出現的表上：列在最後、不在 Macro 列裡。
        unmatched = (f"；{UNMATCHED_SEGMENT} 不含在 Macro"
                     if UNMATCHED_SEGMENT in per_segment else "")
        for fam in _query_group_names():
            _add(_entities_by_k_table(per_segment, macro_seg, ks, all_k, fam),
                 f"A · per-query｜per-segment {fam}@k（列＝segment{unmatched}）",
                 True)
    cat = metrics.get("category")
    cks = None
    if cat:
        n_cat = count_items(cat)
        cat_k = resolved_all_k(cat)
        cks = metrics_section_ks(cat_k)
        _add(_families_by_k_table(cat.get("overall", {}), cks, cat_k),
             f"A · per-query｜大類 overall（列＝{families}）", True)

    # ===== Block B：per-item 歸因（目錄裡 item 那種算法的指標）=====
    map_attr = entry("map_attr", ITEM).name
    attr_label = entry("map_attr", ITEM).display_name
    recall_label = entry("recall", ITEM).display_name
    b_map = _per_item_metric_table(
        per_item, ks, all_k, map_attr, "@{k}", macro_metrics=macro_item,
    )
    # bug 5 (ADR-0020): the macro denominator is "items with a positive this
    # period" (and n_pos >= metric.min_positives); a zero-positive item drops
    # out of per_item silently. The title discloses N of M, only on a table
    # that has a Macro row.
    item_cov = macro_coverage_suffix(per_item, n_items, parameters, macro_item)
    _add(b_map, f"B · per-item 歸因｜{attr_label}@k（列＝item）{item_cov}",
         True)
    _add(_per_item_recall_table(per_item, ks, all_k, macro_metrics=macro_item),
         f"B · per-item 歸因｜{recall_label}@k（列＝item）{item_cov}", True)
    for metric in _other_item_metrics():
        _add(_per_item_metric_table(per_item, ks, all_k, metric.name, "@{k}",
                                    macro_metrics=macro_item),
             f"B · per-item 歸因｜{metric.display_name}@k（列＝item）{item_cov}",
             True)
    if cat:
        cat_macro_item = cat.get("macro_avg", {}).get("by_item", {})
        cat_pi = dict(sorted((cat.get("per_item", {}) or {}).items()))
        cat_item_cov = macro_coverage_suffix(
            cat_pi, n_cat, parameters, cat_macro_item
        )
        _add(_per_item_metric_table(cat_pi, cks, cat_k, map_attr,
                                    "@{k}", macro_metrics=cat_macro_item),
             f"B · 大類 per-item 歸因｜{attr_label}@k（列＝大類）{cat_item_cov}",
             True)
        _add(_per_item_recall_table(cat_pi, cks, cat_k,
                                    macro_metrics=cat_macro_item),
             f"B · 大類 per-item 歸因｜{recall_label}@k（列＝大類）"
             f"{cat_item_cov}", True)
        for metric in _other_item_metrics():
            _add(_per_item_metric_table(cat_pi, cks, cat_k, metric.name,
                                        "@{k}", macro_metrics=cat_macro_item),
                 f"B · 大類 per-item 歸因｜{metric.display_name}@k（列＝大類）"
                 f"{cat_item_cov}", True)

    ledger = [
        line for grain, bundle in grain_bundles(metrics)
        if (line := _group_ledger_line(grain, bundle, parameters))
    ]
    return ReportSection(
        title="衡量指標",
        description=(
            "分兩塊、各塊內維度與方向一致。A｜per-query 指標（map／precision／"
            "recall）——對 overall、per-segment、大類 overall；precision@k 是"
            " per-query 量（整組 top-k 命中幾個），只在這塊。B｜per-item 歸因"
            "（map_attr／recall）——對 per-item、大類 per-item；A 塊的 precision@k"
            " 分母是 k（整組 top-k 的性質），拆不到單一 item 頭上，所以不在這塊。方向規則："
            "單一彙總（overall、大類 overall）用指標家族當列，多實體拆分（per-"
            "segment、per-item、大類 per-item）用實體當列；所有表 k 欄一致＝"
            "[1,2,3,4,5,all]、每張表一個 metric family。頭號指標＝macro per-item "
            f"mAP（item 等權；{headline_column(parameters, all_k, '本段')}）；"
            "overall per-query mAP 是另一種加權，並列不比高下。手算核對：overall "
            "map@1 = recall@1（見核心概念，AP@k 分母＝R）。K=產品數時 precision "
            "退化為 base rate、recall 恆為 1。per-item 列序統一按字母。明細表點標題"
            "展開。"
        ),
        # The query-group ledger (#376), one line per grain, always printed:
        # the reader decides from it whether to turn the switch on.
        bullets=ledger,
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )
