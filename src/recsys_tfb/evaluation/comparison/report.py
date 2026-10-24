"""The comparison report (``report_comparison.html``): its sections in page
order (:data:`SECTIONS`) and the page around them. Each section is a module
of this package; this module only orders them and adds the metadata block —
the same arrangement as the main report's ``report_builder``.

The sections only read and draw (ADR-0031 principle 1): every difference is
the comparison layer's, every K list the cut's, and a value missing from
them is refused with the node to re-run from (``shared.landed_value``).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from recsys_tfb.evaluation.comparison import (
    category_metrics,
    overall_metrics,
    overview,
    per_item_metrics,
    segment_metrics,
)
from recsys_tfb.evaluation.report import generate_html_report
from recsys_tfb.evaluation.report_sections.metric_tables import (
    REFERENCE_LABEL,
    THIS_LABEL,
)
from recsys_tfb.evaluation.report_sections.shared import (
    eval_dates_display,
    results_read_by,
)

#: The comparison report's sections, in page order (ADR-0031 decision 16
#: item 4). Moving a section is moving its line; a section reads only the
#: results it declares (``READS``).
SECTIONS = (
    overview,
    overall_metrics,
    per_item_metrics,
    category_metrics,
    segment_metrics,
)

#: Every result the sections read, by catalog name: what
#: ``generate_comparison_report`` must be wired to (``test_pipeline.py``
#: checks the two equal).
READS = tuple(dict.fromkeys(name for section in SECTIONS
                            for name in section.READS))

#: The page title and its English.
TITLE = "模型比較報表"
TITLE_EN = "Model Comparison Report"

#: A metadata cell with nothing to say.
_NONE = "—"


def assemble_comparison_report(
    results: Mapping[str, dict], parameters: dict,
) -> str:
    """Assemble every section that has something to show, in
    :data:`SECTIONS` order, into the final HTML string.

    ``results`` holds the landed results by catalog name
    (``generate_comparison_report`` builds it). Each section gets the ones
    it declares in ``READS`` and no other, so a section reading a result it
    did not declare fails here rather than drifting from its declaration. A
    section returns ``None`` when it has nothing to show.

    The metadata names the two sides as today's parameters do — this run by
    its ``model_version``, the compared source by its ``label`` — so a
    renamed source redraws without a re-run (``label`` is not part of what
    the results were computed against, ``compare_source_identity``).
    """
    sections = []
    for section in SECTIONS:
        built = section.build(
            results_read_by(section.__name__, section.READS, results),
            parameters)
        if built is not None:
            sections.append(built)
    eval_params = parameters.get("evaluation", {}) or {}
    compared = eval_params.get("compare", {}) or {}
    metadata = {
        THIS_LABEL: parameters.get("model_version", _NONE),
        REFERENCE_LABEL: compared.get("label", _NONE),
        "評估日期": eval_dates_display(eval_params.get("snap_date", "unknown")),
        "產生時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return generate_html_report(sections, title=TITLE, metadata=metadata,
                                title_en=TITLE_EN)
