"""完整性檢查: the facts of this run, and what looks measured but is not."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    query_count_rows,
    read_dataset_overview,
)
from recsys_tfb.evaluation.result_readers import metric_params

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("evaluation_row_facts", "evaluation_metrics")


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """完整性檢查（殿後）：本次執行的事實 ＋「什麼看似正常其實沒量到」。

    presentation §一.4：交代邊界。只陳述事實，不評級。
    """
    row_facts = results["evaluation_row_facts"]
    metrics = results["evaluation_metrics"]
    eval_p = parameters.get("evaluation", {}) or {}
    totals = read_dataset_overview(row_facts).get("totals", {}) or {}
    metric_p = metric_params(parameters)

    mk = metric_p["k"]
    # 「item 數」的標籤印使用者自己的 item 欄名，不寫死「產品」（#327；同
    # 概覽段（overview.build）的 entity 標籤）。
    item_col = get_schema(parameters)["item"]
    # bug 3 (ADR-0020): the same n_queries label fix as
    # the overview section (overview.build).
    facts = {
        "k_values": eval_p.get("k_values"),
        # Same rows as the overview's scale table, one derivation (#376).
        **query_count_rows(row_facts, metrics, parameters),
        "正例列數 n_positives": totals.get("n_positives"),
        f"{item_col} 數 n_items": totals.get("n_items"),
        "metric.weight_alpha（item 加權指數 α；0＝item 等權）":
            metric_p.get("weight_alpha"),
        "metric.k（AP 截斷 k；無＝不截斷、算全長）":
            ("無（不截斷）" if mk is None else mk),
        "metric.min_positives（觀察名單門檻；0＝不設）":
            metric_p.get("min_positives"),
        "metric.shrinkage_k（向 pooled 平均收縮強度；0＝不收縮）":
            metric_p.get("shrinkage_k"),
    }
    # 同分列佔比：只有宣告了 event 的部署算得出來，也只有它需要看。這個數字
    # 是「名次由決勝規則決定、不是由分數決定」的列佔多少——決勝規則按 event
    # 由小到大，event 是時間戳時等於早的曝光永遠排前面，而那個方向不是中性的
    # （廣告示例實測約 0.02 mAP，#378）。框架不替使用者選，把數字印出來讓他
    # 自己判斷這件事在他的資料上有多大。沒宣告時這兩列不存在，既有報表不變。
    # 數的是模型分數的同分，所以在模型的指標結果裡，不在列的事實（#535）。
    if metrics.get("n_tied_rows") is not None:
        facts["同分列數 n_tied_rows（名次由 item／event 決勝，非由分數決定）"] = (
            metrics.get("n_tied_rows")
        )
        share = metrics.get("tied_row_share")
        facts["同分列佔比 tied_row_share"] = (
            None if share is None else f"{share:.1%}"
        )
    facts_tbl = pd.DataFrame([facts]).T
    facts_tbl.columns = ["value"]

    bullets = [
        # 刻意不寫出被隱藏指標的名字：整份報表有一條端到端護欄禁止該字串出現
        # （避免值洩漏）；這裡只陳述「算了但不呈現」這件事。
        "部分排序衍生指標有算但刻意不呈現（本框架目標是排序 macro mAP，"
        "非機率校準）。",
        "每-query 正例數分佈本版未算（Phase 2，需新增 per-query 聚合）。",
        "候選集為密集時每 item 候選覆蓋率恆 100%——per-item 正例佔比與正例數"
        "同序，非獨立軸。",
    ]
    return ReportSection(
        title="完整性檢查",
        description=(
            "本次執行的事實（k、規模、metric 參數）與「什麼情況數字看起來"
            "正常、其實沒量到或不完整」。放在最後，交代邊界。"
        ),
        tables=[facts_tbl],
        table_titles=["本次執行事實"],
        bullets=bullets,
    )
