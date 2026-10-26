"""母體: which rows the ``--prediction-quality`` page's numbers are on and
which kind of number they are (ADR-0032 decision 5), the page's first
section."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.pipelines.evaluation.report.shell import ReportSection
from recsys_tfb.pipelines.evaluation.report.sections.shared import (
    ALL_POSITIVE_SWITCH,
    kept_ratio_phrase,
)
from recsys_tfb.evaluation.result_readers import (
    POPULATION_SOURCE_KEY,
    POPULATION_TEST_RATIO_KEY,
    POPULATION_ZERO_POSITIVE_GROUPS_KEY,
    POST_TRAINING_POPULATION,
    PREDICTION_QUALITY_POPULATION_KEY,
    drop_all_positive_groups,
)
from recsys_tfb.report.types import Note, Paragraph

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ("prediction_quality_metrics",)

#: No switch turns this section off (``shared.sections_on``).
SWITCH = None

#: The heading, its English and the question the section answers.
TITLE = "母體"
TITLE_EN = "Population"
QUESTION = "這一頁的數字是在哪些列上算的、是哪一種數字？"

#: The reminder ADR-0032 leaves to the reader: no data says whether a
#: label's observation window has closed.
MATURITY_NOTE = (
    "label 觀察窗要成熟才準：評估日期的觀察窗還沒結束時，之後才會出現的正例"
    "在這裡被當成負例，precision、recall 與兩個 AUC 都會偏低。框架沒有資料判斷"
    "觀察窗結束了沒，請自己確認。這份報表的 label 取自寫這份 "
    "enriched_eval_predictions 分區的那次標準 evaluation；label 之後有回補，"
    "要先重跑那條標準 evaluation 再跑這個模式。"
)


def _source(population: Mapping, weight_col: str | None) -> str:
    """The rows, and the kind of number: exact on the inference population,
    a weighted estimate at 0 < r < 1, exact on test at r = 1."""
    if population.get(POPULATION_SOURCE_KEY) != POST_TRAINING_POPULATION:
        return (
            "原料是上線後的 inference 母體：offline inference 對全部候選列的"
            "打分，接上 label（不帶 --post-training 的標準 evaluation 寫的那份"
            " enriched_eval_predictions）。每一列權重 1，數字是這個母體上的"
            "精確值。"
        )
    ratio = population[POPULATION_TEST_RATIO_KEY]
    kept = population[POPULATION_ZERO_POSITIVE_GROUPS_KEY]
    text = (
        "原料是 test 母體：training 對 test 表的打分（帶 --post-training 的"
        "標準 evaluation 寫的那份 enriched_eval_predictions）。"
        f"{kept_ratio_phrase(ratio)}（被評模型的 dataset version 建 test 時的 "
        f"dataset.test_zero_positive_group_ratio），這次的列裡有 {kept} 個"
        "無正例 query group。"
    )
    if ratio < 1:
        return text + (
            f"每列帶權重 1／r＝{1 / ratio:.4g}（{weight_col}），數字是還原到"
            "全部 test 候選列的加權估計。1／r 是設計權重，不是無偏估計：留下的"
            "組少時，加權後的比值型指標不穩。"
        )
    return text + "每列權重 1，數字是 test 母體上的精確值。"


def _against_ranking(parameters: dict) -> str:
    """This page's rows against the main report's ranking metrics' (ADR-0024
    decision 7), without the totals."""
    if drop_all_positive_groups(parameters):
        rule = ("只算有正例、而且不是全正的組"
                f"（{ALL_POSITIVE_SWITCH} 開著）")
    else:
        rule = "只算有正例的組"
    return (
        "這一頁不排除任何 query group，包含沒有正例的；主報表的 map@K、"
        f"precision@K 這類只看組內先後的指標{rule}，所以兩邊的母體不同，"
        "precision 不能互相比。"
    )


def build(
    results: Mapping[str, dict], parameters: dict
) -> ReportSection:
    """Read off the payload's population block
    (``compute_prediction_quality``): which run mode's rows, and under
    ``--post-training`` r and the zero-positive groups kept."""
    payload = results["prediction_quality_metrics"]
    population = payload[PREDICTION_QUALITY_POPULATION_KEY]
    weight_col = payload["columns"].get("weight")
    return ReportSection(
        title=TITLE, description="", title_en=TITLE_EN, question=QUESTION,
        key="population", blocks=[
            Paragraph(_source(population, weight_col)
                      + _against_ranking(parameters)),
            Note(MATURITY_NOTE, warn=True),
        ])
