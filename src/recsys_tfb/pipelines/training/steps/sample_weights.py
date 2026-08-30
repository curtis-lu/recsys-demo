"""Configured ``sample_weights`` entries against the rows training actually read.

Nothing here changes a weight — weighting itself happens in ``io/extract.py``.
What lives here is the comparison behind ``persist_sample_weight_report``: did
a configured entry ever find a row?

The comparison runs in the parquet's *encoded* space. A categorical feature is
stored in ``model_input`` as ``str(index)`` into ``category_mappings``, so the
config's ``"mass"`` and the parquet's ``"0"`` are the same key. Both halves of
that translation are imported from ``io/extract.py`` rather than re-derived
here, so the report and the weighting cannot disagree about what a key is — a
re-derivation that drifted would report "everything matched" for weights that
in fact applied to nothing, which is exactly the failure the report exists to
catch.

Reading is deliberately narrow: the weight-key columns only, then distinct. The
train parquet is the largest input this pipeline has and the answer needs no
other column.
"""

from typing import Optional

import pyarrow.dataset as pads

from recsys_tfb.io.extract import _composite_key_series, _translate_weight_table


def distinct_weight_keys(train_handle, weight_keys: list) -> Optional[set]:
    """The composite keys the train parquet actually carries.

    ``None`` — not an empty set — when the parquet has no column for some
    component of the key. That is a different finding from "this key matched no
    row", and the caller is the one that decides what to make of it: an empty
    set would let a caller report a per-entry miss for a key the parquet could
    never have answered.
    """
    ds = pads.dataset(train_handle.path, format="parquet")
    if any(k not in ds.schema.names for k in weight_keys):
        return None
    pdf = ds.to_table(columns=list(weight_keys)).to_pandas().drop_duplicates()
    return set(_composite_key_series(pdf, weight_keys).tolist())


def encoded_key(
    key,
    weight,
    weight_keys: list,
    category_mappings: dict,
    identity_columns: list,
) -> Optional[str]:
    """One config entry as the composite key a parquet row would carry.

    ``None`` when the translation drops the entry, which happens when a
    component names a category absent from ``category_mappings``: no row can
    carry it, so the lookup would silently never fire. ``io/extract.py`` drops
    such an entry from the weight table for the same reason, so ``None`` here
    and "no weight applied" there are the same event.
    """
    translated, _ = _translate_weight_table(
        {key: weight}, weight_keys, category_mappings, identity_columns)
    return next(iter(translated), None)
