"""Pre-checks on the results a node reads back from disk: each was computed
under today's settings and, for a comparison artifact, against today's
compare source. A landed input that fails one is not used.
"""

from collections.abc import Sequence
from typing import Optional

from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
    COMPARE_SOURCE_KEY,
    compare_source_identity,
)
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    LoadedArtifact,
    require_computed_with_current_config,
    require_same_recorded_values,
)


def require_prepared_with_current_config(
    segment_columns: dict, parameters: dict,
    item_categories: Optional[dict] = None,
) -> None:
    """Pre-check (inputs): the ``evaluation_segment_columns`` this run's
    directory holds — and
    ``evaluation_item_categories``, when given — was landed under today's
    values of the settings ``prepare_eval_data`` applies.

    The readers group by its ``joined`` list. Unchecked, ``--from-node
    compute_metrics`` after a change to ``segment_columns`` segments by the old
    list and writes ``metrics.json`` under the new fingerprint, exit code 0.
    Only that step's settings: a re-run starting after it never rewrites the
    file, so a later step's setting would leave it stale for good.

    It does not vouch for the partitions
    (``restrict_to_current_eval_partitions`` checks each).

    ``item_categories`` is ``None`` when its optional catalog entry finds no
    file; ``item_categories.landed_category_mapping`` refuses that in column
    mode.
    """
    artifacts = [LoadedArtifact(
        catalog_name="evaluation_segment_columns (landed with the "
                     "enriched_eval_predictions partition)",
        payload=segment_columns,
        produced_by="prepare_eval_data",
    )]
    if item_categories is not None:
        artifacts.append(LoadedArtifact(
            catalog_name="evaluation_item_categories",
            payload=item_categories,
            produced_by="prepare_eval_data",
        ))
    require_computed_with_current_config(artifacts, parameters)


def require_row_facts_current(row_facts: dict, parameters: dict) -> None:
    """Pre-check (inputs): the landed row facts were counted under today's
    settings. A slice starting at a metrics node reads them back from disk,
    and facts counted on another population would key every ``@all`` metric
    at the wrong K with no error (``metrics_spark._compute_core``)."""
    require_computed_with_current_config(
        [LoadedArtifact(catalog_name="evaluation_row_facts",
                        payload=row_facts, produced_by="compute_row_facts")],
        parameters)


#: Where a comparison artifact computed against another compare source sends
#: the re-run, in both modes: the first node whose output is side B's numbers.
#: Its inputs are memory-only frames, so its slice pulls back
#: ``load_compare_predictions`` and ``restrict_to_common`` (rewriting the
#: coverage) and, under ``--compare``, no Spark node of the main line.
COMPARISON_RERUN_NODE = "compute_compare_metrics"


def require_comparison_inputs_current(
    artifacts: list[LoadedArtifact], parameters: dict,
    run_nodes: Optional[Sequence[str]],
) -> None:
    """Pre-check (inputs) of every node reading a comparison artifact: each
    was computed under today's settings and against today's compare source
    (``compare_sources.compare_source_identity``; ADR-0031 decision 5).

    The second check is needed: the fingerprint leaves ``evaluation.compare``
    out (``config_fingerprint``), so without it a redraw after switching the
    compared source, or changing its ``prod_mapping``, would print the old
    source's numbers under the new one's label, exit code 0.

    A changed setting is sent to the first of ``run_nodes`` applying it
    (``config_fingerprint.rerun_node``; ADR-0031 decision 15), another source
    to :data:`COMPARISON_RERUN_NODE`.
    """
    require_computed_with_current_config(
        artifacts, parameters, run_nodes=run_nodes)
    require_same_recorded_values(
        artifacts, COMPARE_SOURCE_KEY, compare_source_identity(parameters),
        setting="evaluation.compare", rerun_node=COMPARISON_RERUN_NODE)
