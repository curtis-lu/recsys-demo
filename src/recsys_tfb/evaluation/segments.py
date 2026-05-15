"""External segment-source joining for evaluation.

After the metrics-spark redesign, per-segment metric computation lives inside
``metrics_spark.compute_all_metrics`` (driven by
``parameters.evaluation.segment_columns``). This module keeps only the
external segment-source loader; the per-segment compute / table / plot
helpers were removed along with the pandas metrics path and will be
re-introduced in the evaluation_report rewrite (next phase).
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def load_and_join_segment_sources(
    labels: pd.DataFrame,
    segment_sources: dict,
) -> pd.DataFrame:
    """Load external segment Parquet files and join them to labels.

    Args:
        labels: Labels DataFrame to enrich with external segment columns.
        segment_sources: Dict from parameters_evaluation.yaml, keyed by segment name.
            Each value has: filepath, key_columns, segment_column.

    Returns:
        Labels DataFrame with external segment columns joined (left join).
    """
    for seg_name, source_config in segment_sources.items():
        filepath = Path(source_config["filepath"])
        key_columns = source_config["key_columns"]
        segment_column = source_config["segment_column"]

        if not filepath.exists():
            logger.warning(
                "Segment source '%s' file not found: %s — skipping",
                seg_name,
                filepath,
            )
            continue

        seg_df = pd.read_parquet(filepath)
        labels = labels.merge(
            seg_df[key_columns + [segment_column]],
            on=key_columns,
            how="left",
        )
        logger.info(
            "Joined segment source '%s' (%s) — %d/%d matched",
            seg_name,
            segment_column,
            labels[segment_column].notna().sum(),
            len(labels),
        )

    return labels
