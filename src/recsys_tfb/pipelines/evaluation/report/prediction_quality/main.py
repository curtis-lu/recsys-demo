"""The ``--prediction-quality`` page (``prediction_quality/<population>/
<scope>/report.html``, ADR-0032 decision 5): its sections in page order
(:data:`SECTIONS`) and the page around them. Each section is a module of this
package; this module only orders them and adds the metadata block."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from recsys_tfb.evaluation.result_readers import (
    POPULATION_DATES_KEY,
    POPULATION_SCOPE_KEY,
    POPULATION_SOURCE_KEY,
    POST_TRAINING_POPULATION,
    PREDICTION_QUALITY_POPULATION_KEY,
    SCOPE_NAME_KEY,
)
from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    population,
    score_bins,
)
from recsys_tfb.pipelines.evaluation.report.shell import generate_html_report
from recsys_tfb.pipelines.evaluation.report.sections.shared import (
    eval_dates_display,
    results_read_by,
    sections_on,
)

#: The page's sections, in page order: which rows first, then the numbers.
SECTIONS = (population, score_bins)

#: Every result the sections read, by catalog name: what
#: ``generate_prediction_quality_report`` must be wired to.
READS = tuple(dict.fromkeys(name for section in SECTIONS
                            for name in section.READS))

#: The page title and its English.
TITLE = "預測品質報表"
TITLE_EN = "Prediction Quality Report"


def assemble_prediction_quality_report(
    results: Mapping[str, dict], parameters: dict,
) -> str:
    """Assemble every section that has something to show, in
    :data:`SECTIONS` order, into the final HTML string. ``results`` holds
    the landed results by catalog name; each section gets the ones it
    declares in ``READS`` and no other."""
    sections = []
    for section in sections_on(SECTIONS, parameters):
        built = section.build(
            results_read_by(section.__name__, section.READS, results),
            parameters)
        if built is not None:
            sections.append(built)
    population = results["prediction_quality_metrics"][
        PREDICTION_QUALITY_POPULATION_KEY]
    metadata = {
        "模型版本": parameters.get("model_version", "unknown"),
        "評估日期": eval_dates_display(population[POPULATION_DATES_KEY]),
        "原料": ("test 母體（--post-training）"
                 if population[POPULATION_SOURCE_KEY] == POST_TRAINING_POPULATION
                 else "上線後的 inference 母體"),
        "範圍": (population[POPULATION_SCOPE_KEY][SCOPE_NAME_KEY]
                 if population[POPULATION_SCOPE_KEY]
                 else "全部候選列（沒有 --scope）"),
        "產生時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return generate_html_report(
        sections, title=TITLE, metadata=metadata, title_en=TITLE_EN)
