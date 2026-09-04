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
from pandas.api.types import infer_dtype

from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io import disk_matrix
from recsys_tfb.io.handles import ParquetHandle, open_parquet_dataset

logger = logging.getLogger(__name__)


def _narrow_frame(pdf: pd.DataFrame, cols: list) -> pd.DataFrame:
    """A frame holding only ``cols``, built *without* consolidating ``pdf``.

    Why not the obvious ``pdf[cols]``: list-indexing a DataFrame consolidates
    the source frame's blocks in place — it copies every column of the wide
    frame to answer a question about a few of them. Selecting one column at a
    time goes through the block manager's single-column path, which does not.

    On a 150,000 row x 1,002 column frame built column-at-a-time, so each
    column is its own block (measured 2026-09-03 on macOS arm64, 8 CPU / 16 GB, pandas 1.5.3 /
    numpy 1.25.0): ``pdf[cols]`` takes
    the frame from 1,002 blocks to 3 and costs 1.526s; this helper leaves it
    at 1,002 and costs 0.001s. The callers' timings are in ``_group_ids`` and
    ``_weight_keys_and_codes``.
    """
    return pd.DataFrame({c: pdf[c] for c in cols}, copy=False)


def _group_ids(pdf: pd.DataFrame, group_cols: list) -> np.ndarray:
    """Per-row query-group id, numbered by first appearance.

    Bit-for-bit identical to ``pdf.groupby(group_cols, sort=False).ngroup()``
    (same columns, same ``sort=False``, same row order) — only the frame it
    groups is different. Why not group the whole frame: ``groupby`` on a wide
    frame consolidates it first, which is the entire cost here.

    On 150,000 rows x 1,002 columns (measured 2026-09-03 on macOS arm64, 8 CPU / 16 GB, pandas 1.5.3 /
    numpy 1.25.0):
    1.534s -> 0.008s (201x) on a fragmented frame, and still 0.664s -> 0.022s
    (30x) when the frame was already consolidated by an earlier
    ``pdf[feature_cols]`` — which is the order the callers below actually run
    in, so the win is not just the avoided consolidation.

    ``dropna`` is left at its **default**, i.e. ``True``, because the replaced
    call used the default too and matching it is what makes the ids identical.
    What that default costs, should a group column ever be null: ``ngroup()``
    yields ``NaN`` for those rows (the Series becomes ``float64``), and casting
    that to ``int64`` is undefined — numpy warns ``invalid value encountered in
    cast`` and, on the arm64 build measured above, lands them on ``0``,
    silently merging every null-keyed row into the *first* query group.

    ``schema.time`` cannot get there: rows whose time is null are already gone,
    dropped by the ``isin`` month filter in
    ``pipelines/dataset/steps/scoping.py`` (SQL ``IN`` on NULL is NULL, not
    true), and it is the Hive partition column of every ``*_model_input``
    table. ``schema.entity`` has no such guard — it comes from the user's
    ``sample_pool``, and both joins in ``pipelines/dataset/steps/model_input.py``
    are LEFT joins keyed on it, so a null entity survives into the parquet.
    Fixing that would change the returned ids, so it is not this function's
    call to make. Related: known-pitfalls.md §11.
    """
    return (
        _narrow_frame(pdf, group_cols)
        .groupby(group_cols, sort=False)
        .ngroup()
        .to_numpy(dtype=np.int64)
    )


def composite_key_series(pdf: pd.DataFrame, weight_keys: list) -> pd.Series:
    """Per-row '|'-joined composite key from ``weight_keys`` columns (str-cast).

    Single source for the lookup-key construction so the weight mapping and the
    zero-match diagnostic agree byte-for-byte. Mirrors the dataset sampler's
    ``sample_ratio_overrides`` key in pipelines/dataset/steps/sampling.py.
    """
    keys = pdf[weight_keys[0]].astype(str)
    for k in weight_keys[1:]:
        keys = keys.str.cat(pdf[k].astype(str), sep="|")
    return keys


def weight_key_decode_map(
    weight_keys: list, category_mappings: dict, identity_columns: list,
) -> dict[str, list]:
    """The weight-key columns that hold category *codes*, with their vocabularies.

    A declared categorical that is **not** an identity column is encoded by
    ``preprocessing.encode_categoricals`` into its index in
    ``category_mappings[col]``, so model_input stores a number where the config
    names a category. Identity categoricals are the exception: their encoding is
    deferred to the driver and model_input keeps the raw value (see
    ``preprocessing.encodable_categoricals``), so decoding one would corrupt a
    key that already matches.

    Everything else — the label, carry columns, plain numerics — is stored as
    written and is absent from the map.
    """
    identity = set(identity_columns)
    return {
        col: category_mappings[col]
        for col in weight_keys
        if col in category_mappings and col not in identity
    }


def decode_weight_keys(
    frame: pd.DataFrame, decode_map: dict,
) -> tuple[pd.DataFrame, np.ndarray]:
    """Category codes back to category values; flag the rows that have none.

    Why decode the *data* rather than translate the config table into code
    space: the code's storage type is an implementation detail that has already
    changed under this lookup once. #283 widened
    ``cast_numeric_features_to_storage_type`` to every numeric feature, and an
    encoded categorical *is* a feature column, so its code became ``float32``;
    a key built straight off the column went from ``"0"`` to ``"0.0"`` and
    matched nothing (#297). Decoding restores the value the config was written
    against, which is the only representation neither side can change.

    Returns ``(decoded, undecodable)``. ``undecodable`` is a boolean array over
    the frame's rows: True when a column's value is not a valid index into its
    vocabulary — ``preprocessing.UNKNOWN_CATEGORY_CODE`` (-1, the encoder's
    mark for a value its fit never saw), a NaN, or an out-of-range, infinite or
    non-integral number.

    Such a row is *flagged*, not decoded to a stand-in string. A stand-in would
    be a collision waiting to happen: a vocabulary may contain a category
    literally named ``"-1"`` or ``"nan"``, and then an unknown row and a real
    one would build the same key and take the same weight.

    Acting on the flag is the caller's job, and only the two that decide a
    weight do: :func:`_compute_row_weights` pins those rows to 1.0 by position,
    and ``steps/sample_weights.py::distinct_weight_keys`` drops them from the
    set it reports as present. :func:`_sample_data_keys` deliberately does not
    — its output is a log line, and an undecodable cell showing as ``None``
    there is the reader's cue that the parquet holds a code no vocabulary
    covers.

    The frame is copied only when there is something to decode.
    """
    if not decode_map:
        return frame, np.zeros(len(frame), dtype=bool)
    decoded = frame.copy()
    undecodable = np.zeros(len(frame), dtype=bool)
    for col, categories in decode_map.items():
        # ``.astype("float64")`` before ``to_numpy``: a nullable dtype holding
        # ``pd.NA`` (Int64 is reachable here — see ``dropna=False`` in
        # :func:`_weight_keys_and_codes`) refuses to become a float64 numpy
        # array directly, and the coerce alone does not widen it.
        raw = (
            pd.to_numeric(frame[col], errors="coerce")
            .astype("float64")
            .to_numpy(dtype=np.float64)
        )
        # Clip before the int cast: casting inf or 1e30 to int64 is undefined,
        # and a clipped value fails the `safe == idx` identity below anyway.
        safe = np.clip(np.where(np.isfinite(raw), raw, -1.0), -1.0, float(len(categories)))
        idx = safe.astype(np.int64)
        ok = (safe == idx) & (idx >= 0) & (idx < len(categories))
        undecodable |= ~ok
        out = np.empty(len(frame), dtype=object)
        if len(categories):
            out[ok] = np.asarray(categories, dtype=object)[idx[ok]]
        decoded[col] = out
    return decoded, undecodable


def nameable_weight_entries(
    sample_weights: dict, weight_keys: list, decode_map: dict,
) -> tuple[dict, dict]:
    """Split the configured table into entries a row could name, and the rest.

    A component of a decoded column can only ever be one of that column's
    categories, so an entry naming anything else — a typo, or a value the
    encoder's fit never saw — cannot match a single row. Reporting it is the
    point; the weights would be identical either way.

    Returns ``(nameable, unknown_values)``, where ``unknown_values`` maps a
    weight-key column to the sorted configured values its vocabulary lacks.

    Every returned key is ``str(key)``, matching the string
    :func:`composite_key_series` builds on the data side. YAML is why: an
    unquoted ``sample_weights: {1: 5.0}`` on an int weight key (``label``, a
    year, a flag) parses to the *int* ``1``, and ``Series.map`` on a
    string-keyed Series would miss it — silently, at weight 1.0, which is the
    failure this whole path exists to avoid.
    """
    known = {col: set(map(str, cats)) for col, cats in decode_map.items()}
    nameable: dict = {}
    unknown: dict[str, set] = {}
    for key, weight in sample_weights.items():
        key = str(key)
        parts = key.split("|")
        if len(parts) != len(weight_keys):
            # A9b reports arity at the config gate; here it simply never
            # matches, so keep it and let the zero-match diagnostic speak.
            nameable[key] = weight
            continue
        bad = False
        for part, col in zip(parts, weight_keys):
            if col in known and part not in known[col]:
                unknown.setdefault(col, set()).add(part)
                bad = True
        if not bad:
            nameable[key] = weight
    return nameable, {c: sorted(v) for c, v in unknown.items()}


def decoded_key_series(
    frame: pd.DataFrame, weight_keys: list, decode_map: dict,
) -> tuple[pd.Series, np.ndarray]:
    """Decode, then build one composite key per row of ``frame``.

    Every key the weighting and the diagnostics compare comes through here, so
    "decode first, then join" is stated once rather than five times. Which rows
    of ``frame`` it is handed is what differs between callers — the whole frame,
    its distinct combinations, or one row per group.

    Public because ``pipelines/training/steps/sample_weights.py`` builds the
    same keys off the train parquet, and a report that derived them its own way
    could vouch for a match the weighting never made.
    """
    decoded, undecodable = decode_weight_keys(frame, decode_map)
    return composite_key_series(decoded, weight_keys), undecodable


def _key_column_is_string_faithful(col: pd.Series) -> bool:
    """True when ``str()`` is injective over the values pandas groups together.

    The dedup path below groups rows by *value* and then builds one string key
    per group, so it only reproduces the per-row ``astype(str)`` build when
    equal values always stringify the same. pandas' grouping equality is
    coarser than string equality in three cases measured here (pandas 1.5.3):

      ``0.0`` / ``-0.0``            one group, ``"0.0"`` vs ``"-0.0"``
      ``None`` / ``nan`` in object  one group, ``"None"`` vs ``"nan"``
      ``1`` / ``True`` in object    one group, ``"1"`` vs ``"True"``

    Integer, unsigned, boolean, datetime and timedelta columns cannot hit any
    of them (equal values are equal bit patterns, and each column carries one
    unit and one timezone, so ``NaT`` has a single spelling). Neither can an
    object or ``string`` column whose every element is a ``str`` — the scan
    below includes missing values, so a mixed ``None``/``nan`` column is
    rejected. Everything else — floats, mixed object, categorical — takes the
    exact per-row path instead of a fast-but-different one.

    The scan is cheap enough not to matter: ``infer_dtype`` is C-level and
    measured 0.03s on a 10,000,000-row object column.
    """
    kind = getattr(col.dtype, "kind", "")
    if kind in "iubMm":
        return True
    if kind == "O":
        return infer_dtype(col, skipna=False) == "string"
    return False


def _weight_key_frame(
    pdf: pd.DataFrame, weight_keys: list, decode_map: dict,
) -> pd.DataFrame | None:
    """The weight-key columns as their own frame, or ``None`` to decline.

    ``None`` means resolving the weights per distinct key combination would not
    be provably exact for these columns (see
    :func:`_key_column_is_string_faithful`), so the caller must build the key
    per row instead.

    A column in ``decode_map`` is admitted whatever its dtype, because
    :func:`decode_weight_keys` runs between the grouping and the string build:
    the key is then a function of the *decoded* value, and equal codes decode
    to one category — ``0.0`` and ``-0.0`` are one group and both index 0, and
    every undecodable value is pinned to 1.0 by position rather than by a
    string. That is what makes the dedup path available to the ``float32``
    codes #283 produces; refusing it costs one built string per row. Measured
    2026-09-05 on macOS arm64, 8 CPU / 16 GB, pandas 1.5.3 / numpy 1.25.0:
    one key over 10,000,000 rows 1.898s -> 0.198s (9.6x), three keys
    9.048s -> 0.441s (20.5x).
    """
    small = _narrow_frame(pdf, weight_keys)
    if not all(
        k in decode_map or _key_column_is_string_faithful(small[k])
        for k in weight_keys
    ):
        return None
    return small


def _distinct_weight_keys(
    small: pd.DataFrame, weight_keys: list, decode_map: dict,
) -> pd.Series:
    """One composite key per distinct combination, in first-appearance order.

    ``drop_duplicates`` keeps first occurrences, so this is the cheap half of
    :func:`_weight_keys_and_codes` for the caller that does not need the
    per-row codes.
    """
    keys, _ = decoded_key_series(small.drop_duplicates(), weight_keys, decode_map)
    return keys


def _weight_keys_and_codes(
    small: pd.DataFrame, weight_keys: list, decode_map: dict,
) -> tuple[pd.Series, np.ndarray, np.ndarray]:
    """As :func:`_distinct_weight_keys`, plus each row's index into it and the
    combinations that carry no category at all.

    The point: a weight table is keyed on a handful of distinct combinations,
    so building one ``'|'``-joined string per *row* does N times the work of
    building one per *distinct combination*. Both halves come out of one
    ``GroupBy``, and the key strings are built by the existing
    :func:`composite_key_series` on the few dozen surviving rows — the same
    function the whole-frame build used, unchanged.

    Decoding runs *here*, on those few dozen rows, not on the frame: that is
    what makes reversing the encoding affordable (see :func:`decode_weight_keys`
    for why the data side is the one to move).

    On 10,000,000 rows x 3 weight keys (measured 2026-09-03 on macOS arm64, 8 CPU / 16 GB, pandas 1.5.3 /
    numpy 1.25.0), the
    whole-frame build takes 4.90s and this takes 1.34s (3.7x), and the per-row
    object array of joined strings is never allocated. What is left is two
    passes over the narrow frame, which is what the remaining time is.

    Two details that are not interchangeable with the simpler-looking spelling:

    * ``dropna=False`` — with the default, a missing value gets code ``-1``,
      which silently indexes the *last* lookup entry. Reachable here via a
      nullable dtype (``Int64`` + ``pd.NA``); measured wrong weights without
      it.
    * ``head(1)`` rather than a separate ``small.drop_duplicates()`` — same
      rows either way (both keep first occurrences, and ``ngroup(sort=False)``
      numbers groups in that same order), but taking them off the ``GroupBy``
      that already produced the codes reuses its grouping instead of
      factorizing the frame a second time: measured 0.571s -> 0.459s on
      5,000,000 rows, and it removes the need to assume the two orders agree.
    """
    grouped = small.groupby(weight_keys, sort=False, dropna=False)
    codes = grouped.ngroup().to_numpy(dtype=np.int64)
    keys, undecodable = decoded_key_series(grouped.head(1), weight_keys, decode_map)
    return keys, codes, undecodable


def _sample_data_keys(
    pdf: pd.DataFrame,
    weight_keys: list,
    decode_map: dict | None = None,
    limit: int = 5,
) -> list:
    """First ``limit`` distinct data keys, for the zero-match diagnostic.

    Same keys the whole-frame build produced, from the same
    :func:`composite_key_series`. Still one pass over the rows — deduping them
    is what a pass buys — but it builds one joined string per distinct
    combination rather than one per row, and skips the per-row codes the
    diagnostic has no use for.

    Decoded, so the WARNING names the categories the config was written in
    (``['M', 'F']``) rather than the codes it was stored as (``['0.0', '1.0']``)
    — the reader has to compare the two lists, and that only works when both
    are in the same vocabulary.

    The trailing ``drop_duplicates`` matters when a key *value* itself contains
    ``'|'``: two distinct value combinations can join to one string, and the
    replaced code deduped on the string.
    """
    decode_map = decode_map or {}
    small = _weight_key_frame(pdf, weight_keys, decode_map)
    if small is None:
        keys, _ = decoded_key_series(
            _narrow_frame(pdf, weight_keys), weight_keys, decode_map)
    else:
        keys = _distinct_weight_keys(small, weight_keys, decode_map)
    return keys.drop_duplicates().head(limit).tolist()


def _compute_row_weights(
    pdf: pd.DataFrame,
    weight_keys: list,
    sample_weights: dict,
    decode_map: dict | None = None,
) -> np.ndarray:
    """Per-row LightGBM sample weight from a composite-key weight table.

    Pure: no Spark, no I/O. Each row's lookup key is its ``weight_keys``
    column values joined with '|' (mirrors the dataset sampler's
    ``sample_ratio_overrides`` key in pipelines/dataset/steps/sampling.py).
    Rows whose key is absent from ``sample_weights`` get weight 1.0
    (sparse-emit: only adjusted groups are written to the table).

    ``decode_map`` (from :func:`weight_key_decode_map`) names the key columns
    stored as category codes; they are decoded back to category values before
    the key is built, so the table stays written in the config's own
    vocabulary. A row whose code names no category is pinned to 1.0.

    Resolved per *distinct* key combination and scattered back to rows when
    that is exact (see :func:`_weight_key_frame`), else per row.
    """
    if not sample_weights or not weight_keys:
        return np.ones(len(pdf), dtype=np.float64)
    decode_map = decode_map or {}
    small = _weight_key_frame(pdf, weight_keys, decode_map)
    if small is None:
        keys, undecodable = decoded_key_series(
            _narrow_frame(pdf, weight_keys), weight_keys, decode_map)
        w = keys.map(sample_weights).fillna(1.0).to_numpy(dtype=np.float64)
        w[undecodable] = 1.0
        return w
    uniq_keys, codes, undecodable = _weight_keys_and_codes(
        small, weight_keys, decode_map)
    lookup = uniq_keys.map(sample_weights).fillna(1.0).to_numpy(dtype=np.float64)
    lookup[undecodable] = 1.0
    return lookup[codes]


def _row_weights_from_pdf(
    pdf: pd.DataFrame, parameters: dict, preprocessor_metadata: dict,
) -> np.ndarray:
    """Resolve a per-row weight array from training.sample_weights.

    All-ones when the table is absent/empty or any configured weight-key
    column is missing from pdf (graceful, never raises; consistency gate A9a
    already blocks unavailable columns at CLI entry). Computed from the
    *given* pdf so it stays aligned to the caller's filtering/ordering.

    Decode-aware: weight-key columns that are *encoded features* (present in
    ``preprocessor_metadata["category_mappings"]`` and NOT identity columns)
    are stored as numeric codes in the parquet. Those columns are decoded back
    to their category values before the key is built (see
    :func:`decode_weight_keys`), so callers write human-readable values (e.g.
    ``"hnw"``) in the YAML and the lookup happens in that same vocabulary.
    Entries naming a category the encoder never saw cannot match any row and
    are reported with a WARNING.

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
    weight_keys = _weight_key_columns(parameters)
    n_rows = len(pdf)

    missing = [k for k in weight_keys if k not in pdf.columns]
    if not sw or missing:
        reason = (
            "sample_weights table is empty" if not sw
            else f"weight-key column(s) {missing} absent from parquet"
        )
        logger.info(
            "sample_weight INACTIVE — all %d rows weight=1.0 (%s); "
            "weight_keys=%s n_weight_entries=%d",
            n_rows, reason, weight_keys, len(sw),
        )
        return np.ones(n_rows, dtype=np.float64)

    category_mappings = (preprocessor_metadata or {}).get("category_mappings", {}) or {}
    identity_cols = get_schema(parameters)["identity_columns"]
    decode_map = weight_key_decode_map(weight_keys, category_mappings, identity_cols)
    nameable, unknown = nameable_weight_entries(sw, weight_keys, decode_map)
    if unknown:
        logger.warning(
            "sample_weight: unknown category value(s) %s — those entries cannot "
            "match any row (left at weight 1.0).", unknown,
        )
        # If every entry named something the vocabulary lacks, the warning above
        # is the full diagnosis — skip the redundant 0-match warning below.
        if not nameable:
            return np.ones(n_rows, dtype=np.float64)

    w = _compute_row_weights(pdf, weight_keys, nameable, decode_map)
    n_adjusted = int((w != 1.0).sum())
    if n_adjusted == 0:
        sample_data_keys = _sample_data_keys(pdf, weight_keys, decode_map)
        logger.warning(
            "sample_weight matched 0 of %d rows — weight_keys=%s; sample "
            "configured keys=%s; sample data keys=%s (both decoded)",
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
        ds = open_parquet_dataset(path)
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


def _assert_feature_dtypes_numeric(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> None:
    """B6 training-read backstop — raise before the read if any model feature
    column is a non-numeric parquet type that will NOT be encoded downstream.

    Since #284 this path no longer flattens a frame, so the harm it prevents is
    not "an object matrix gets built and OOMs" but "the matrix cannot be built
    at all": :func:`_stream_matrix` allocates a numeric array and writes each
    batch into it, and a string column has nothing to write there. The
    object-dtype OOM the invariant is named for is still reachable through
    :func:`pdf_to_X`, which inference and the chunked test evaluation use.

    Reads parquet schema only (pyarrow metadata, no data). Deferred identity
    categoricals (e.g. ``prod_name``, encoded per batch during the read) are
    exempt.
    """
    import pyarrow.types as pat

    from recsys_tfb.core.consistency import nonnumeric_feature_errors

    field_type = _feature_field_types(handle, preprocessor_metadata)
    deferred = _deferred_categoricals(preprocessor_metadata, parameters)
    feature_cols = preprocessor_metadata["feature_columns"]

    def _kind(t) -> str:
        if (
            pat.is_integer(t)
            or pat.is_floating(t)
            or pat.is_boolean(t)
            or pat.is_decimal(t)
        ):
            return "numeric"
        return "nonnumeric"

    feature_kinds = {
        c: _kind(field_type[c]) for c in feature_cols if c in field_type
    }
    _raise_if(
        nonnumeric_feature_errors(feature_kinds, deferred),
        "train_model_input feature columns include un-encoded non-numeric "
        "type(s) — an object-dtype matrix the driver cannot hold, then a "
        "LightGBM float-cast error",
    )


#: Bytes one streamed record batch may occupy, before it is folded into the
#: pre-allocated matrix. A *byte* budget rather than a row count because a batch
#: costs ``rows x columns x itemsize``: at 64 MiB this is 16,384 rows of a
#: 1,000-column float32 frame and 327,680 rows of a 50-column one. A hard-coded
#: row count would have to be chosen for the narrow case and would then make the
#: wide case's batch hundreds of MiB — the thing the streaming read exists to
#: avoid.
STREAM_BATCH_BYTES: int = 64 * 1024**2


def stream_batch_rows(
    n_columns: int, itemsize: int, budget: int | None = None,
) -> int:
    """Rows per streamed batch, from a byte budget and the frame's width.

    Always at least one row, so a frame too wide to fit even a single row in the
    budget still makes progress instead of dividing to zero.

    ``budget=None`` reads :data:`STREAM_BATCH_BYTES` **at call time**. Spelling
    the default as the module attribute instead would bind it once at import,
    and a caller that changed the attribute would silently keep the old budget
    — which is exactly how the multi-batch test in ``tests/test_io`` spent its
    first life passing over a single batch.

    ``n_columns`` is priced at ``itemsize`` per column, which is the feature
    columns' width. The handful of aux columns (label, group, item) are not that
    width — a string item column is wider — so a real batch runs a little over
    budget. Left alone: features outnumber aux by two or three orders of
    magnitude in the shape this exists for.
    """
    per_row = max(1, n_columns * itemsize)
    return max(1, (STREAM_BATCH_BYTES if budget is None else budget) // per_row)


def _deferred_categoricals(
    preprocessor_metadata: dict, parameters: dict,
) -> set[str]:
    """Declared categoricals that model_input stores raw, encoded at read time.

    An identity column is kept as its human-readable value in model_input (the
    inference side writes ``prod_name`` into a partition column), so these are
    the feature columns that are legitimately non-numeric on disk — the
    exemption both the B6 and the B9 backstops need, defined once.
    """
    identity = set(get_schema(parameters)["identity_columns"])
    return {
        c for c in preprocessor_metadata["categorical_columns"] if c in identity
    }


def _feature_field_types(
    handle: ParquetHandle, preprocessor_metadata: dict,
) -> dict:
    """Each parquet field's arrow type, read from the schema alone — no data.

    Shared by both training-read backstops (B6 and B9) so "which columns exist,
    and as what" is answered once and the same way. Goes through
    :func:`open_parquet_dataset`, which is what makes hive partition columns
    visible: the training cache stores ``schema.time`` and ``schema.item`` as
    directory names (``<root>/snap_date=.../prod_name=.../*.parquet``, see
    ``populate_cache_from_hive`` in ``pipelines/training/steps/local_cache.py``),
    and a plain ``pyarrow.dataset(path, format="parquet")`` does not discover
    them — it returns a schema missing exactly the group and item columns.
    """
    schema = open_parquet_dataset(handle.path).schema
    return {name: schema.field(name).type for name in schema.names}


def _raise_if(errors: list, headline: str) -> None:
    """Raise one collect-all ``DataConsistencyError``, or return quietly.

    Both backstops report every offending column at once rather than the first,
    so an operator fixing a stale parquet learns the whole story in one run.
    """
    from recsys_tfb.core.consistency import DataConsistencyError

    if errors:
        raise DataConsistencyError(
            f"{headline} ({len(errors)} issue(s)):\n- " + "\n- ".join(errors)
        )


def _arrow_storage_name(arrow_type) -> str:
    """A parquet column type, spelled the way the config declares storage types.

    Only the declarable types get a name of their own; everything else falls
    back to pyarrow's own spelling, which is what the B9 message should quote
    for an ``int64`` or ``decimal128(38, 10)`` column that never should have
    reached model_input.
    """
    import pyarrow.types as pat

    if pat.is_float32(arrow_type):
        return "float32"
    if pat.is_float64(arrow_type):
        return "float64"
    return str(arrow_type)


def matrix_dtype_checked_against_parquet(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> np.dtype:
    """The dtype to allocate the matrix as — and the B9 backstop that earns it.

    Reads the parquet **schema only** (pyarrow metadata, no data), like the B6
    backstop above — the whole point is to fail before a read that cannot fit in
    the driver. Deferred identity categoricals are exempt (they are strings on
    disk by contract).

    The dtype is taken from the file's own types and then required to equal
    ``dataset.numeric_feature_storage_type``, rather than simply trusting the
    declaration: a declaration that does not describe the bytes on disk is the
    failure this gate exists to name, and reading it back out of the parquet is
    what makes the two comparable at all. Since the gate passes only when they
    agree, the returned dtype is both.
    """
    from recsys_tfb.core.consistency import (
        feature_storage_type_errors,
        resolved_numeric_storage,
    )

    declared, _ = resolved_numeric_storage(parameters)
    deferred = _deferred_categoricals(preprocessor_metadata, parameters)
    field_type = _feature_field_types(handle, preprocessor_metadata)
    feature_types = {
        c: _arrow_storage_name(field_type[c])
        for c in preprocessor_metadata["feature_columns"]
        if c in field_type and c not in deferred
    }
    _raise_if(
        feature_storage_type_errors(feature_types, declared),
        "train_model_input feature columns are not stored as the declared "
        "numeric storage type",
    )
    return np.dtype(declared)


def _weight_key_columns(parameters: dict) -> list[str]:
    """The configured sample-weight key columns, defaulting to ``schema.item``.

    One definition so the streaming read knows which columns to keep and
    :func:`_row_weights_from_pdf` resolves the same ones from them.
    """
    training = parameters.get("training", {}) or {}
    return training.get("sample_weight_keys") or [get_schema(parameters)["item"]]


def _stream_matrix(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    aux_columns: list,
    log_prefix: str,
    on_disk_label: str | None = None,
) -> tuple:
    """Read the parquet into a pre-allocated matrix, one pyarrow batch at a time.

    Returns ``(X, aux)`` — the feature matrix, and a pandas frame holding only
    ``aux_columns`` (label, group columns, item, weight keys), row-aligned with
    it.

    ``on_disk_label`` maps the matrix from a scratch file instead of
    allocating it on the heap (:mod:`recsys_tfb.io.disk_matrix`). It changes
    where the bytes live and nothing else — same dtype, same batching,
    byte-identical contents, zero rows included. It is a caller's declaration
    ("I am going to hold this for a long time"), never a size test: a branch
    that only fires on data large enough to hurt is a branch no test ever
    reaches.

    It carries a *name* rather than a flag because that name is what an
    operator reads when the disk is too small to hold the matrix. This
    function's own ``log_prefix`` is the wrong thing to put there — it says
    ``extract_Xy_with_groups``, which names the reader, not the matrix, and
    every caller would report the same string.

    **Why not read the frame and slice it.** The obvious spelling materialises
    the feature data three times over: the frame pandas builds from the parquet,
    the copy ``pdf[feature_cols]`` makes, and the matrix ``.values`` flattens it
    into. One float64 copy of a 24,000,000 x 1,000 model_input is 178.8 GiB on a
    128 GiB driver — so the failure is not "OOM partway through", it is that the
    allocation cannot be served at all. Here the matrix is allocated once, up
    front, at its final dtype, and each batch is written into its own row slice
    and released; peak memory over the read is the matrix plus one batch.

    Only the columns that are actually used are requested — features plus
    ``aux_columns`` — so a model_input carrying columns this model does not want
    costs nothing to read past.

    On 150,000 rows x 1,000 float32 feature columns (measured 2026-09-04 on
    macOS arm64, 8 CPU / 16 GB, pyarrow 14.0.1 / pandas 1.5.3 / numpy 1.25.0;
    median of 5, same process), ``md5`` over the matrix bytes identical either
    way:

      one flat file                    3.472s -> 0.467s   (7.4x)
      hive-partitioned directory       1.533s -> 0.450s   (3.4x)

    **Quote the second row.** The training cache is the partitioned tree
    (``populate_cache_from_hive``); the flat file is a test fixture's shape and
    nothing else. The replaced path is the half that differs — ``pq.read_table``
    is more than twice as fast over the partitioned layout — while this one
    costs the same either way, which is what a pre-allocated matrix filled batch
    by batch should look like.

    Neither row is the reason the change exists. The reason is that at
    24,000,000 x 1,000 the replaced path needs an allocation the driver cannot
    serve at all, and no speed measured at 150,000 rows says anything about
    that.

    Deferred identity categoricals are encoded **per batch** into their matrix
    column. The whole-frame spelling assigned integer codes back into a wide
    frame, which is a column-block rewrite of that frame; here the codes are
    written straight into the slice they belong in, and the raw values are still
    available to ``aux`` for callers that need the original names.
    """
    import pyarrow as pa
    import pyarrow.dataset as pads

    feature_cols = list(preprocessor_metadata["feature_columns"])
    category_mappings = preprocessor_metadata["category_mappings"]
    deferred = _deferred_categoricals(preprocessor_metadata, parameters)

    dtype = matrix_dtype_checked_against_parquet(
        handle, preprocessor_metadata, parameters)

    ds = open_parquet_dataset(handle.path)
    available = set(ds.schema.names)
    # Absent columns are dropped rather than requested: a configured weight-key
    # column that the parquet does not carry is the graceful all-ones case
    # _row_weights_from_pdf already reports, and asking pyarrow for it would
    # turn that into a read error. Duplicates are dropped too — the item column
    # is often also a weight key — because pyarrow reads a repeated name once
    # and the aux frame would then carry two columns of the same name.
    aux_cols: list = []
    for col in aux_columns:
        if col in available and col not in aux_cols:
            aux_cols.append(col)
    feature_set = set(feature_cols)
    read_cols = feature_cols + [c for c in aux_cols if c not in feature_set]
    position = {c: i for i, c in enumerate(read_cols)}

    n_rows = ds.count_rows()
    shape = (n_rows, len(feature_cols))
    X = (
        np.empty(shape, dtype=dtype)
        if on_disk_label is None
        else disk_matrix.open_disk_matrix(shape, dtype, on_disk_label)
    )
    batch_rows = stream_batch_rows(len(read_cols), dtype.itemsize)
    logger.info(
        "%s: streaming read n_rows=%d n_read_columns=%d (of %d in file) "
        "dtype=%s batch_rows=%d matrix_mib=%.1f on_disk=%s",
        log_prefix, n_rows, len(read_cols), len(available), dtype.name,
        batch_rows, X.nbytes / 1024**2, on_disk_label or False,
    )

    aux_schema = pa.schema([ds.schema.field(c) for c in aux_cols])
    aux_batches: list = []
    filled = 0
    with log_step(logger, "read_parquet"):
        for batch in ds.to_batches(columns=read_cols, batch_size=batch_rows):
            n = batch.num_rows
            if not n:
                continue
            stop = filled + n
            for j, col in enumerate(feature_cols):
                column = batch.column(position[col])
                if col in deferred:
                    X[filled:stop, j] = pd.Categorical(
                        column.to_pandas(),
                        categories=category_mappings[col],
                    ).codes
                else:
                    X[filled:stop, j] = column.to_numpy(zero_copy_only=False)
            if aux_cols:
                aux_batches.append(batch.select(aux_cols))
            filled = stop

    if filled != n_rows:
        raise ValueError(
            f"{log_prefix}: streamed {filled} rows into a matrix allocated for "
            f"{n_rows} — the parquet at {getattr(handle, 'path', '<unknown>')} "
            f"changed under the read."
        )

    if deferred & feature_set:
        logger.info(
            "%s: encoded deferred_cats=%s count=%d",
            log_prefix, sorted(deferred & feature_set),
            len(deferred & feature_set),
        )
    aux = pa.Table.from_batches(aux_batches, schema=aux_schema).to_pandas()
    log_data_volume(logger, f"{log_prefix}.aux", aux, deep=True)
    return X, aux


def pdf_to_X(
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
    log_data_volume(logger, "pdf_to_X.X_df", X_df, deep=True)

    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    if deferred_cats:
        with log_step(logger, "encode_categoricals"):
            for col in deferred_cats:
                known = category_mappings[col]
                X_df[col] = pd.Categorical(X_df[col], categories=known).codes
        logger.info(
            "pdf_to_X: encoded deferred_cats=%s count=%d",
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

    The read is streamed into a pre-allocated matrix — see :func:`_stream_matrix`
    for why, and for what the ``read_parquet`` sub-step now covers. Two INFO
    lines land before it: the parquet's shape and uncompressed size, and the
    matrix's own dtype / batch size / footprint, so a driver that dies on the
    allocation still says in the log what it was trying to allocate.
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
    _assert_feature_dtypes_numeric(handle, preprocessor_metadata, parameters)

    aux_cols = [label_col]
    if with_weights:
        aux_cols += _weight_key_columns(parameters)

    X, aux = _stream_matrix(
        handle, preprocessor_metadata, parameters, aux_cols, "extract_Xy",
    )
    y = aux[label_col].values

    log_data_volume(logger, "extract_Xy.X", X)
    log_data_volume(logger, "extract_Xy.y", y)

    if with_weights:
        w = _row_weights_from_pdf(aux, parameters, preprocessor_metadata)
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
    on_disk_label: str | None = None,
) -> tuple:
    """Like :func:`extract_Xy` but also returns per-row query-group ids.

    A query group is ``(time, *entity)`` — for the default schema, the
    ``(snap_date, cust_id)`` pair. ``groups`` is an int64 array aligned 1:1
    with rows of X / y; rows in the same group share the same id.

    ``items`` (``with_items=True``) is the item column's **raw** values, not the
    integer codes the matrix holds for the same column. That is the contract the
    inference side reads: it writes those names into a partition column, so
    handing back codes would silently rename every partition.

    ``on_disk_label`` returns X mapped from a scratch file rather than held
    on the heap, under that name — byte-identical either way, see
    :func:`_stream_matrix`. Only ``tune_hyperparameters`` asks for it: it is
    the one caller that holds a matrix across every fit of the search, so its
    resident memory otherwise grows with the val row count. The ``.bin`` prep,
    the refit and the calibration read each hold theirs for a single fit and
    are left alone, which is also why :func:`extract_Xy` has no such
    parameter — none of its callers would pass it.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    item_col = schema["item"]
    group_cols = [schema["time"]] + schema["entity"]

    logger.info(
        "extract_Xy_with_groups start path=%s n_feature_cols=%d label=%s "
        "group_cols=%s",
        getattr(handle, "path", "<unknown>"),
        len(feature_cols),
        label_col,
        group_cols,
    )

    _log_parquet_metadata(handle)
    _assert_feature_dtypes_numeric(handle, preprocessor_metadata, parameters)

    aux_cols = [label_col] + group_cols
    if with_weights:
        aux_cols += _weight_key_columns(parameters)
    if with_items:
        aux_cols.append(item_col)

    X, aux = _stream_matrix(
        handle, preprocessor_metadata, parameters, aux_cols,
        "extract_Xy_with_groups", on_disk_label=on_disk_label,
    )
    y = aux[label_col].values
    groups = _group_ids(aux, group_cols)

    log_data_volume(logger, "extract_Xy_with_groups.X", X)
    log_data_volume(logger, "extract_Xy_with_groups.y", y)
    log_data_volume(logger, "extract_Xy_with_groups.groups", groups)
    logger.info(
        "extract_Xy_with_groups: n_groups=%d",
        int(groups.max()) + 1 if len(groups) else 0,
    )

    result: list = [X, y, groups]
    if with_weights:
        w = _row_weights_from_pdf(aux, parameters, preprocessor_metadata)
        log_data_volume(logger, "extract_Xy_with_groups.w", w)
        result.append(w)
    if with_items:
        items = aux[item_col].to_numpy()
        log_data_volume(logger, "extract_Xy_with_groups.items", items)
        result.append(items)
    return tuple(result)
