"""The Layer-2 data gate: does the configuration contradict the actual data?

This module owns that whole question for the dataset pipeline — it is the only
place that turns three source tables plus ``parameters`` into a verdict, and it
runs as the pipeline's first node so a contradiction costs one metadata lookup
rather than a full build that crashes somewhere unrecognisable.

Two boundaries hold it in shape:

- **The rules are not here.** Every predicate lives in ``core/consistency.py``,
  which is the single source of truth for the B series (and the A series that
  runs at CLI entry). This module supplies facts and reports the verdict; it
  decides nothing. Adding an invariant means adding a predicate there and one
  term to ``errors`` below.
- **Nothing here scans.** B5/B6/B7 read ``feature_table.dtypes`` — Hive
  metastore metadata, no data touched — and B1 collects distinct item values
  over the configured windows only. ADR-0006 fixes that as the gate's
  definition: it catches *config-vs-data contradictions*, while data quality
  auditing belongs upstream in ``source_etl``'s ``quality_checks``. A rule
  needing a ``groupBy`` over the table does not belong in this module.

Because the node has no outputs, ``--from-node`` slicing skips it (F5 in
``docs/agents/architecture-constraints.md``, registered in R3): resuming a
dataset run mid-pipeline leaves the data layer unchecked.
"""

from pyspark.sql import DataFrame
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

def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    feature_table: DataFrame,
    parameters: dict,
) -> None:
    """Layer-2 data gate (B1 + B5 + B6 + B7). Side-effect only: raises
    ``DataConsistencyError`` on violation, returns ``None`` on success. Wired
    as the first node of the dataset pipeline.

    B1 — item values are checked on sample_pool (set-equality vs declared, both
    directions) and label_table (only data-has-unknown), restricted to the
    configured snap_date windows the pipeline actually uses.

    B5 — a column declared in ``categorical_columns`` must not be a
    continuous-numeric type (decimal/double/float) in feature_table. Read off
    ``feature_table.dtypes`` (metastore metadata, no scan) so the opaque
    "Decimal is not JSON serializable" crash inside fit_preprocessor_metadata
    is caught up-front at the first node instead of after the full distinct pass.

    B6 — a prospective feature column (from ``_compute_feature_columns``) that is
    non-numeric in feature_table and is NOT declared categorical would become an
    un-encoded object-dtype model feature (training OOM at ``_pdf_to_X``). Also
    read off ``feature_table.dtypes`` (no scan) via ``spark_dtype_is_numeric``.

    B7 — a column may be carried or be a model feature, never both. A
    ``dataset.carry_columns`` entry that is also a feature_table column puts a
    copy on each side of the ``build_model_input`` join and Spark raises
    ``Reference 'x' is ambiguous``; either adding it to ``drop_columns`` or
    removing it from ``carry_columns`` resolves that, and they mean different
    things, so the error states both rather than picking. Identity columns and
    the label are exempt (they cannot collide whatever the config says). Reads
    the same ``feature_table.dtypes`` mapping as B5/B6 — no extra lookup, no
    scan (ADR-0004).

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
