"""Convert a ParquetHandle into algorithm-agnostic numpy (X, y) arrays.

Encapsulates deferred categorical encoding (e.g. prod_name) that the dataset
pipeline keeps as raw string values; downstream training code expects fully
numeric numpy arrays.

Moved out of pipelines/training/nodes.py so that ModelAdapter implementations
(e.g. LightGBMAdapter.prepare_train_inputs) can reuse it without circular
imports.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.handles import ParquetHandle

logger = logging.getLogger(__name__)


def _composite_key_series(pdf: pd.DataFrame, weight_keys: list) -> pd.Series:
    """Per-row '|'-joined composite key from ``weight_keys`` columns (str-cast).

    Single source for the lookup-key construction so the weight mapping and the
    zero-match diagnostic agree byte-for-byte. Mirrors the dataset sampler's
    ``sample_ratio_overrides`` key in pipelines/dataset/helpers_spark.py.
    """
    keys = pdf[weight_keys[0]].astype(str)
    for k in weight_keys[1:]:
        keys = keys.str.cat(pdf[k].astype(str), sep="|")
    return keys


def _translate_weight_table(
    sample_weights: dict,
    weight_keys: list,
    category_mappings: dict,
    identity_columns: list,
) -> tuple[dict, dict]:
    """Translate config sample_weights keys into the parquet's encoded space.

    A component whose column is an *encoded feature* (in ``category_mappings``
    and NOT an identity column — identity cats are stored raw in model_input) is
    mapped from its human-readable value to ``str(index)`` in
    ``category_mappings[col]`` (matching ``_encode_categoricals``). Identity /
    label / carry / numeric components pass through unchanged. A key with any
    unknown feature value is dropped (cannot match) and recorded.

    Returns ``(translated, unknown_values)``; ``unknown_values`` maps a weight-key
    column to the sorted config values absent from its mapping.
    """
    identity = set(identity_columns)
    code_of: dict[str, dict[str, str]] = {}
    for col in weight_keys:
        if col in category_mappings and col not in identity:
            code_of[col] = {
                str(cat): str(i) for i, cat in enumerate(category_mappings[col])
            }

    translated: dict[str, float] = {}
    unknown: dict[str, list[str]] = {}
    for key, weight in sample_weights.items():
        parts = str(key).split("|")
        if len(parts) != len(weight_keys):
            # arity is enforced by A9b at the config gate; keep as-is defensively.
            translated[str(key)] = weight
            continue
        out_parts: list[str] = []
        bad = False
        for part, col in zip(parts, weight_keys):
            if col in code_of:
                code = code_of[col].get(part)
                if code is None:
                    unknown.setdefault(col, []).append(part)
                    bad = True
                else:
                    out_parts.append(code)
            else:
                out_parts.append(part)
        if not bad:
            translated["|".join(out_parts)] = weight
    return translated, {c: sorted(set(v)) for c, v in unknown.items()}


def _compute_row_weights(
    pdf: pd.DataFrame,
    weight_keys: list,
    sample_weights: dict,
) -> np.ndarray:
    """Per-row LightGBM sample weight from a composite-key weight table.

    Pure: no Spark, no I/O. Each row's lookup key is its ``weight_keys``
    column values joined with '|' (mirrors the dataset sampler's
    ``sample_ratio_overrides`` key in pipelines/dataset/helpers_spark.py).
    Rows whose key is absent from ``sample_weights`` get weight 1.0
    (sparse-emit: only adjusted groups are written to the table).
    """
    if not sample_weights or not weight_keys:
        return np.ones(len(pdf), dtype=np.float64)
    keys = _composite_key_series(pdf, weight_keys)
    return keys.map(sample_weights).fillna(1.0).to_numpy(dtype=np.float64)


def _row_weights_from_pdf(pdf: pd.DataFrame, parameters: dict) -> np.ndarray:
    """Resolve a per-row weight array from training.sample_weights.

    All-ones when the table is absent/empty or any configured weight-key
    column is missing from pdf (graceful, never raises; consistency gate A9a
    already blocks unavailable columns at CLI entry). Computed from the
    *given* pdf so it stays aligned to the caller's filtering/ordering.

    Emits one observability line per call (train + train_dev each log once) so a
    run's log alone answers "did sample_weight take effect?":
      - INACTIVE — table empty, or a weight-key column is absent from the parquet
        (the graceful all-ones backstop); the message states which.
      - ACTIVE — reports rows_total / rows_adjusted / min·mean·max so a tiny or
        zero effect is visible.
      - A non-empty table that matches **zero** rows is a WARNING with sample
        data keys vs configured keys, since that almost always means the keys
        don't match the parquet values (e.g. int-coded vs string ``prod_name``,
        or a product-name typo) — a failure mode A9 cannot see (it never reads
        the parquet's actual values).
    """
    training = parameters.get("training", {}) or {}
    sw = training.get("sample_weights") or {}
    # The base YAML always supplies sample_weight_keys (default [prod_name]); the
    # [schema.item] fallback only matters for test fixtures that omit the key.
    weight_keys = training.get("sample_weight_keys") or [get_schema(parameters)["item"]]
    n_rows = len(pdf)

    missing = [k for k in weight_keys if k not in pdf.columns]
    if not sw or missing:
        reason = (
            "sample_weights table is empty"
            if not sw
            else f"weight-key column(s) {missing} absent from parquet"
        )
        logger.info(
            "sample_weight INACTIVE — all %d rows weight=1.0 (%s); "
            "weight_keys=%s n_weight_entries=%d",
            n_rows, reason, weight_keys, len(sw),
        )
        return np.ones(n_rows, dtype=np.float64)

    w = _compute_row_weights(pdf, weight_keys, sw)
    n_adjusted = int((w != 1.0).sum())
    if n_adjusted == 0:
        sample_data_keys = (
            _composite_key_series(pdf, weight_keys).drop_duplicates().head(5).tolist()
        )
        logger.warning(
            "sample_weight table matched 0 of %d rows — configured keys likely do "
            "not match the parquet values (check int-coded vs string values / "
            "typos). weight_keys=%s; sample configured keys=%s; sample data keys=%s",
            n_rows, weight_keys, sorted(map(str, sw))[:5], sample_data_keys,
        )
    else:
        logger.info(
            "sample_weight ACTIVE — weight_keys=%s n_weight_entries=%d; "
            "rows_total=%d rows_adjusted=%d (%.2f%%); weight min/mean/max=%.3f/%.4f/%.3f",
            weight_keys, len(sw), n_rows, n_adjusted,
            100.0 * n_adjusted / n_rows if n_rows else 0.0,
            float(w.min()), float(w.mean()), float(w.max()),
        )
    return w


def _log_parquet_metadata(handle: ParquetHandle) -> None:
    """Log parquet shape & uncompressed size before the actual read.

    Uses pyarrow.dataset so a single .parquet file *and* a multi-file
    parquet directory both work. Metadata-only — no row data read, no
    measurable memory cost.

    Observability failures (e.g. path missing) are caught and downgraded
    to WARNING so the probe never blocks the real read. The downstream
    pandas read will then surface the real error itself.
    """
    path = getattr(handle, "path", "<unknown>")
    try:
        import pyarrow.dataset as pads

        ds = pads.dataset(path, format="parquet")
        n_rows = ds.count_rows()
        n_cols = len(ds.schema)
        total_bytes = 0
        n_row_groups = 0
        for frag in ds.get_fragments():
            md = frag.metadata
            n_row_groups += md.num_row_groups
            for rg_i in range(md.num_row_groups):
                rg = md.row_group(rg_i)
                for col_i in range(rg.num_columns):
                    total_bytes += rg.column(col_i).total_uncompressed_size
        type_counts: dict[str, int] = {}
        for t in ds.schema.types:
            key = str(t)
            type_counts[key] = type_counts.get(key, 0) + 1
        logger.info(
            "extract_Xy: parquet metadata num_rows=%d num_columns=%d "
            "num_row_groups=%d total_uncompressed_mb=%.1f schema_types=%s",
            n_rows, n_cols, n_row_groups,
            total_bytes / 1024**2,
            type_counts,
        )
    except Exception as e:
        logger.warning(
            "extract_Xy: parquet metadata probe failed path=%s err=%s",
            path, e,
        )


def _pdf_to_X(
    pdf: pd.DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> np.ndarray:
    """Already-loaded pdf -> X numpy.

    Encapsulates slice_features + encode_categoricals (deferred identity cats)
    + to_numpy. Used by extract_Xy after its parquet read and by
    predict_and_write_test_predictions after a per-partition pyarrow read +
    positive-set filter, so the latter doesn't have to re-read the parquet
    just to reuse the feature-slicing logic.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]

    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    log_data_volume(logger, "_pdf_to_X.X_df", X_df, deep=True)

    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    if deferred_cats:
        with log_step(logger, "encode_categoricals"):
            for col in deferred_cats:
                known = category_mappings[col]
                X_df[col] = pd.Categorical(X_df[col], categories=known).codes
        logger.info(
            "_pdf_to_X: encoded deferred_cats=%s count=%d",
            deferred_cats, len(deferred_cats),
        )

    with log_step(logger, "to_numpy"):
        X = X_df.values
    return X


def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    *,
    with_weights: bool = False,
) -> tuple:
    """Read the parquet at ``handle.path`` and return (X, y) as numpy arrays.

    Categorical identity columns (e.g. prod_name) are int-coded via the
    preprocessor's ``category_mappings``.

    Emits sub-step ``log_step`` events (``read_parquet`` → ``slice_features`` →
    ``encode_categoricals`` (skipped when no deferred cats) → ``to_numpy``) and
    per-step INFO size summaries so OOM-killed runs can be diagnosed from log.
    Step A (read_parquet) lives here; Step B (pdf -> X) is delegated to
    :func:`_pdf_to_X`. A pre-read parquet metadata INFO is emitted before
    ``read_parquet`` so shape/uncompressed-size are visible even if the pandas
    read OOMs.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    logger.info(
        "extract_Xy start path=%s n_feature_cols=%d label=%s identity_cols=%s",
        getattr(handle, "path", "<unknown>"),
        len(feature_cols),
        label_col,
        identity_cols,
    )

    _log_parquet_metadata(handle)

    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    log_data_volume(logger, "extract_Xy.pdf", pdf, deep=True)

    X = _pdf_to_X(pdf, preprocessor_metadata, parameters)
    y = pdf[label_col].values

    log_data_volume(logger, "extract_Xy.X", X)
    log_data_volume(logger, "extract_Xy.y", y)

    if with_weights:
        w = _row_weights_from_pdf(pdf, parameters)
        log_data_volume(logger, "extract_Xy.w", w)
        return X, y, w
    return X, y


def extract_Xy_with_groups(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    *,
    with_weights: bool = False,
    with_items: bool = False,
) -> tuple:
    """Like :func:`extract_Xy` but also returns per-row query-group ids.

    A query group is ``(time, *entity)`` — for the default schema, the
    ``(snap_date, cust_id)`` pair. ``groups`` is an int64 array aligned 1:1
    with rows of X / y; rows in the same group share the same id.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    group_cols = [schema["time"]] + schema["entity"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]

    logger.info(
        "extract_Xy_with_groups start path=%s n_feature_cols=%d label=%s "
        "group_cols=%s",
        getattr(handle, "path", "<unknown>"),
        len(feature_cols),
        label_col,
        group_cols,
    )

    _log_parquet_metadata(handle)

    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    log_data_volume(logger, "extract_Xy_with_groups.pdf", pdf, deep=True)

    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    log_data_volume(logger, "extract_Xy_with_groups.X_df", X_df, deep=True)

    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    if deferred_cats:
        with log_step(logger, "encode_categoricals"):
            for col in deferred_cats:
                known = category_mappings[col]
                X_df[col] = pd.Categorical(X_df[col], categories=known).codes
        logger.info(
            "extract_Xy_with_groups: encoded deferred_cats=%s count=%d",
            deferred_cats, len(deferred_cats),
        )

    with log_step(logger, "to_numpy"):
        X = X_df.values
        y = pdf[label_col].values
        groups = (
            pdf.groupby(group_cols, sort=False).ngroup().to_numpy(dtype=np.int64)
        )
    log_data_volume(logger, "extract_Xy_with_groups.X", X)
    log_data_volume(logger, "extract_Xy_with_groups.y", y)
    log_data_volume(logger, "extract_Xy_with_groups.groups", groups)
    logger.info(
        "extract_Xy_with_groups: n_groups=%d",
        int(groups.max()) + 1 if len(groups) else 0,
    )

    result: list = [X, y, groups]
    if with_weights:
        w = _row_weights_from_pdf(pdf, parameters)
        log_data_volume(logger, "extract_Xy_with_groups.w", w)
        result.append(w)
    if with_items:
        items = pdf[schema["item"]].to_numpy()
        log_data_volume(logger, "extract_Xy_with_groups.items", items)
        result.append(items)
    return tuple(result)
