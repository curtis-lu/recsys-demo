"""概覽: what the report answers, its scale, the headline numbers and where
to look for the rest."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metric_catalog import ITEM, QUERY_GROUP, entry
from recsys_tfb.evaluation.metrics import metric_params, resolved_all_k
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    headline_column,
    k_to_lookup,
    prediction_quality_shown,
    query_count_rows,
    read_dataset_overview,
    report_cfg,
    resolve_display_k,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_row_facts", "evaluation_metrics",
         "prediction_quality_metrics")


def _headline_key(parameters: dict, all_k: int) -> str:
    """The ``macro_avg["by_item"]`` key the headline macro per-item mAP is read at.

    ``map_attr@{evaluation.metric.k}``, or ``@all`` (looked up at ``all_k``)
    when ``metric.k`` is unset. ``metrics_spark._resolve_k_grids`` puts that
    K into the per-item grid whether or not ``k_values`` lists it, so the
    key is computed; its value is ``None`` when no item enters the macro
    (e.g. ``metric.min_positives`` above every item's count).
    """
    mk = metric_params(parameters)["k"]
    return entry("map_attr", ITEM).key(
        k_to_lookup("all" if mk is None else mk, all_k))


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """概覽（定向）：這份報表回答什麼、規模／分母、關鍵數、往哪找。

    The navigation table gains the prediction-quality row only when that
    section is on the page (``shared.prediction_quality_shown``), so the row
    never points at a section the report left out. That is why this section
    reads ``prediction_quality_metrics``; ADR-0031 decision 9 drops the
    navigation table, and the read with it.

    presentation §一.1：規模／歸一化分母與嚴重度訊號分開標——分母混進關鍵數
    表會被讀成好壞。頭號指標＝macro per-item mAP（item 等權）；overall
    per-query mAP 並列為「另一種加權」，不宣稱哪個才對（不變量 4）。
    """
    metrics = results["evaluation_metrics"]
    pq_shown = prediction_quality_shown(
        results["prediction_quality_metrics"], parameters)
    overall = metrics.get("overall", {})
    disp = report_cfg(parameters).get("display", {}) or {}
    all_k = resolved_all_k(metrics)
    ks = resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), all_k)

    tables: list[pd.DataFrame] = []
    titles: list[str] = []

    # 關鍵指標 1：macro per-item mAP（頭號）——讀 metrics.json 的 macro 列，與
    # 衡量指標段 B 塊「Macro 平均」同一個數，不另算一份。沒有值（沒有任何
    # item 進 macro）就不畫這張表：一格空白的頭號表看起來像「算了、結果是空」。
    macro_item = (metrics.get("macro_avg") or {}).get("by_item") or {}
    headline = macro_item.get(_headline_key(parameters, all_k))
    if headline is not None:
        tables.append(pd.DataFrame(
            [{"value": headline}], index=["macro per-item mAP"]))
        titles.append("頭號指標：macro per-item mAP（item 等權）")

    # 關鍵指標 2：overall per-query mAP@k（另一種加權，並列不比高下）
    overall_map = entry("map", QUERY_GROUP)
    card = {
        overall_map.key(k): overall.get(overall_map.key(k_to_lookup(k, all_k)))
        for k in ks
    }
    t_overall = pd.DataFrame([card]).T
    t_overall.columns = ["value"]
    tables.append(t_overall)
    titles.append("overall mAP@k（per-query 等權，另一種加權）")

    # 規模／分母（非好壞，明標與關鍵數分開）
    # 「每 X 平均正例數」的 X 印使用者自己的 entity 欄，不寫死「客戶」——entity
    # 是門市時，「每客戶平均」要讀者在腦中翻譯一次才看得懂這個數字在數什麼
    # （#327）。欄名一律走 core.schema.get_schema，理由同核心概念那一段。
    facts = results["evaluation_row_facts"]
    totals = read_dataset_overview(facts).get("totals", {}) or {}
    entity_str = "×".join(get_schema(parameters)["entity"])
    # bug 3 (ADR-0020): n_queries is compute_dataset_overview's count of all
    # queries before filtering, but the old label called it "queries with a
    # positive" — contradicting the excluded-queries row right below (a
    # million queries with a positive cannot coexist with 950k excluded).
    # Fix the label, and add the row the report never gave anywhere:
    # queries with a positive = n_queries - n_excluded_queries. With
    # drop_all_positive_groups on, the rows say which groups each count is
    # (#376, query_count_rows).
    scale = {
        **query_count_rows(facts, parameters),
        "正例列數 n_positives": totals.get("n_positives"),
        "母體正樣本率（÷全體候選列）": totals.get("positive_rate"),
        f"每 {entity_str} 平均正例數 avg_positives_per_entity":
            totals.get("avg_positives_per_entity"),
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
            "本次量到什麼、沒量到什麼",
        ],
        "看哪一區": [
            "衡量指標",
            "衡量指標（per-item／per-segment）",
            "per-item 細部拆解",
            "baseline",
            "完整性檢查",
        ],
    })
    if pq_shown:
        nav.loc[len(nav)] = [
            "把每一列候選當二元預測：門檻切在哪、precision／recall 多少",
            "預測品質",
        ]
    tables.append(nav)
    titles.append("導覽：想回答什麼 → 看哪一區")

    return ReportSection(
        title="概覽",
        description=(
            "這份報表幫你判斷這個模型在 per-query 排序上表現如何、好壞落在哪些 "
            "item／segment、以及相對 popularity baseline 的位置。以下攤開多個粒度"
            "與角度，判斷留給你。頭號指標為 macro per-item mAP（item 等權；"
            f"{headline_column(parameters, all_k, '衡量指標段')}）；overall mAP "
            "為 per-query 等權，是另一種加權，並列呈現、不比高下。"
        ),
        tables=tables,
        table_titles=titles,
    )
