"""Layer-2 data gate: the dataset pipeline's first node.

What belongs in this module: the glue that asks the config and Spark for facts
and hands them to the invariant predicates. Nothing else — no sampling, no
preprocessing, no node that produces an artifact.

What does *not* belong here: the invariants themselves. Every B-series rule —
what it means, why it exists, what it costs — is defined once in
``core/consistency.py`` (see its Invariant legend). Adding one means adding a
predicate there and one term to the sum below; deciding anything in this module
would put a second, drifting copy of the rule next to the real one.

Cost invariant (ADR-0006): the facts gathered here are cheap ones. Column types
come from the metastore — metadata, no rows. The item values come from a
distinct over the configured snap_date windows, so what lands on the driver is
bounded by item cardinality rather than by row count. What this gate does not do
is aggregate over a source table: that would change this node's cost magnitude,
and a check needing one is a data-quality check with a home of its own upstream
in ``source_etl``'s ``quality_checks``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.consistency import (
    DataConsistencyError,
    carry_column_collision_errors,
    categorical_dtype_errors,
    item_coverage_errors,
    nonnumeric_feature_errors,
    resolved_item_values,
    spark_dtype_is_numeric,
)
from recsys_tfb.core.schema import get_schema
from recsys_tfb.pipelines.dataset.nodes_shared import collect_dataset_snap_dates
from recsys_tfb.preprocessing._common import (
    _compute_feature_columns,
    _get_preprocessing_config,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    feature_table: DataFrame,
    parameters: dict,
) -> None:
    """Run the Layer-2 invariants (B1, B5, B6, B7) against the source tables.

    Side-effect only: raises ``DataConsistencyError`` on violation, returns
    ``None`` when everything holds. Each invariant's meaning lives with its
    predicate in ``core/consistency.py``.

    All errors are collected and raised once so a single fix pass clears them.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    time_col = schema["time"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    windows = collect_dataset_snap_dates(parameters)

    def _distinct_items(df: DataFrame) -> set:
        rows = (
            df.filter(F.col(time_col).isin(windows))
            .select(item)
            .distinct()
            .collect()
        )
        return {r[item] for r in rows if r[item] is not None}

    drop_cols, categorical_cols = _get_preprocessing_config(parameters)
    ft_dtypes = dict(feature_table.dtypes)
    feature_cols = _compute_feature_columns(
        list(feature_table.columns),
        identity_cols,
        categorical_cols,
        drop_cols,
        label_col,
    )
    # Only feature_table-sourced columns have a dtype here; identity categoricals
    # (e.g. prod_name) come from schema.categorical_values, are absent from
    # feature_table.dtypes, and are validated by A3.
    feature_kinds = {
        c: ("numeric" if spark_dtype_is_numeric(ft_dtypes[c]) else "nonnumeric")
        for c in feature_cols
        if c in ft_dtypes
    }
    errors = (
        item_coverage_errors(
            item,
            resolved_item_values(parameters),
            _distinct_items(sample_pool),
            _distinct_items(label_table),
        )
        + categorical_dtype_errors(categorical_cols, ft_dtypes)
        + nonnumeric_feature_errors(feature_kinds, set(categorical_cols))
        + carry_column_collision_errors(
            parameters.get("dataset", {}).get("carry_columns") or [],
            # Reuses the dtypes mapping B5/B6 already read — same metastore
            # metadata lookup, so B7 costs no extra call and no scan.
            set(ft_dtypes),
            drop_cols,
            identity_cols,
            label_col,
        )
    )
    if errors:
        raise DataConsistencyError(
            "Data consistency check failed ("
            + str(len(errors))
            + " issue(s)):\n- "
            + "\n- ".join(errors)
        )
