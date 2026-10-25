"""Pre-checks the evaluation nodes run on the results they read back from disk:
each was computed under today's settings and, for a comparison artifact,
against today's compare source. ``nodes.py`` calls them before it uses a
landed input.
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
    directory holds — and ``evaluation_item_categories``, when the reader was
    given one — was landed under today's values of the settings
    ``prepare_eval_data`` applies.

    ``prepare_eval_data`` lands that JSON together with the partitions, and the
    readers group by its ``joined`` list. Unchecked, ``--from-node
    compute_metrics`` after a change to ``segment_columns`` segments by the old
    list and writes ``metrics.json`` under the new fingerprint, which
    ``generate_report`` then accepts, exit code 0. A change to a setting only a
    later step applies leaves it current: the partition is read back from
    Hive, so a re-run starting after ``prepare_eval_data`` could never make it
    current again.

    It does not stand for the partitions themselves: one date's
    partition is also written by runs over other dates, whose JSON lands in
    another directory. Each partition carries its own fingerprint, checked by
    ``restrict_to_current_eval_partitions``.

    ``item_categories`` is ``None`` when its optional catalog entry finds no
    file; that is only a problem in column mode, where
    :func:`item_categories.landed_category_mapping` refuses it.
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
#: the re-run, in both modes. The first node whose output is side B's
#: numbers: its inputs are memory-only frames, so its slice pulls
#: ``load_compare_predictions`` and
#: ``restrict_to_common`` back with it (the coverage is rewritten too), and
#: under ``--compare`` runs no Spark node of the main line — every node of
#: that line sorting after it reads landed JSON only.
#: ``test_pipeline.py::TestTheComparisonReRunsOnItsOwn`` pins both.
COMPARISON_RERUN_NODE = "compute_compare_metrics"


def require_comparison_inputs_current(
    artifacts: list[LoadedArtifact], parameters: dict,
    run_nodes: Optional[Sequence[str]],
) -> None:
    """Pre-check (inputs) of every node reading a comparison artifact: each
    was computed under today's settings, and against today's
    compare source (``compare_sources.compare_source_identity``, recorded
    under ``COMPARE_SOURCE_KEY``; ADR-0031 decision 5).

    The fingerprint leaves ``evaluation.compare`` out on purpose
    (``config_fingerprint``: the CLI injects it per run), so without the
    second check a redraw after switching the compared source, or changing
    its ``prod_mapping``, would print the old source's numbers under the new
    one's label, exit code 0.

    The advice names a node of the run (ADR-0031 decision 15, first row),
    the first of ``run_nodes`` applying a changed setting: under
    ``--compare`` a main-line one when the run has one (the main line is
    stale too, and a slice from the comparison would stop at
    ``generate_report``); under ``--compare-only``, which has no main line,
    a comparison step, and the record of the run that prepared the
    partitions is not compared with today's settings (it is not among
    ``run_nodes``). Another source is
    :data:`COMPARISON_RERUN_NODE` in both.
    """
    require_computed_with_current_config(
        artifacts, parameters, run_nodes=run_nodes)
    require_same_recorded_values(
        artifacts, COMPARE_SOURCE_KEY, compare_source_identity(parameters),
        setting="evaluation.compare", rerun_node=COMPARISON_RERUN_NODE)
