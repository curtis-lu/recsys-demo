"""完整性檢查: what this run measured with which settings, and what looks
measured but is not."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.result_readers import metric_params
from recsys_tfb.report.types import Bullets, Column, Paragraph, Row, Table

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the model's metrics, for the share of tied rows (#535) — the counts are
#: 核心概念's (ADR-0031 decision 16 item 12).
READS = ("evaluation_metrics",)

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "完整性檢查"
TITLE_EN = "Completeness checks"
QUESTION = "這次量了什麼？什麼沒量到？"

#: What each setting in the table means, in the table's order.
_MEANINGS = {
    "k_values": "要算哪些 K",
    "metric.weight_alpha": "item 加權指數 α；0＝item 等權",
    "metric.min_positives": "觀察名單門檻；0＝不設",
    "metric.shrinkage_k": "向 pooled 平均收縮強度；0＝不收縮",
}


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """完整性檢查（殿後）：設定表 ＋「什麼看似正常其實沒量到」。

    presentation §一.4：交代邊界。只陳述事實，不評級。設定照讀者寫的印，
    ``metric.*`` 照 ``result_readers.metric_params`` 解析後的值（沒寫的鍵是它的
    預設），跟計算讀的是同一份。
    """
    metrics = results["evaluation_metrics"]
    eval_p = parameters.get("evaluation", {}) or {}
    metric_p = metric_params(parameters)
    values = {
        "k_values": _list_text(eval_p.get("k_values")),
        "metric.weight_alpha": metric_p["weight_alpha"],
        "metric.min_positives": metric_p["min_positives"],
        "metric.shrinkage_k": metric_p["shrinkage_k"],
    }
    settings = Table(
        "設定", "設定", (Column("值", "text"), Column("意思", "text")),
        tuple(Row(key, (values[key], meaning))
              for key, meaning in _MEANINGS.items()))

    bullets = [
        # 刻意不寫出被隱藏指標的名字：整份報表有一條端到端護欄禁止該字串出現
        # （避免值洩漏）；這裡只陳述「算了但不呈現」這件事。
        "有些從排序結果衍生的指標有算、但刻意不放：本框架的目標是排序，不是"
        "機率校準。",
        "每個 query group 有幾個正例的分布，這版沒有算。",
        "全網格時每個 item 都出現在每個 query group，所以各 item 的候選列一樣"
        "多，正例佔比跟正例數同序。",
    ]
    # 同分列佔比：只有宣告了 event 的部署算得出來，也只有它需要看。這個數字
    # 是「名次由決勝規則決定、不是由分數決定」的列佔多少——決勝規則按 event
    # 由小到大，event 是時間戳時等於早的曝光永遠排前面，而那個方向不是中性的
    # （廣告示例實測約 0.02 mAP，#378）。框架不替使用者選，把數字印出來讓他
    # 自己判斷這件事在他的資料上有多大。沒宣告時沒有這一則。數的是模型分數的
    # 同分，所以在模型的指標結果裡，不在列的事實（#535）。只印佔比：列數是全
    # 資料的計數，總量只在核心概念（決定 16 第 12 項）。
    share = metrics.get("tied_row_share")
    if share is not None:
        bullets.append(
            f"同分的列佔候選列的 {share:.1%}：這些列的名次由 item／event 決勝，"
            "不是由分數決定。")
    return ReportSection(
        title=TITLE,
        description="",
        title_en=TITLE_EN,
        question=QUESTION,
        key="completeness",
        blocks=[
            Paragraph("這次量了什麼，以及什麼情況下數字看起來正常、其實沒量到。",
                      muted=True),
            Bullets(tuple(bullets)),
            settings,
        ],
    )


def _list_text(value) -> str | None:
    """A K list as the reader wrote it, ``[1, 2, 3, 4, 5, all]`` — not
    Python's ``repr`` with its quotes; ``None`` (blank) when unset."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(str(v) for v in value) + "]"
    return str(value)
