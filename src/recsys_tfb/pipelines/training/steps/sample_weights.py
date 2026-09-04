"""Configured ``sample_weights`` entries against the rows training actually read.

Nothing here changes a weight — weighting itself happens in ``io/extract.py``.
What lives here is the comparison behind ``persist_sample_weight_report``: did
a configured entry ever find a row?

The comparison runs in the *config's* vocabulary. A categorical feature is
stored in ``model_input`` as a numeric code into ``category_mappings``, so the
parquet's ``0.0`` and the config's ``"mass"`` are the same key once the code is
decoded. Both halves of that decoding are imported from ``io/extract.py`` rather
than re-derived here, so the report and the weighting cannot disagree about what
a key is — a re-derivation that drifted would report "everything matched" for
weights that in fact applied to nothing, which is exactly the failure the report
exists to catch. (#297 is what that drift looks like from the other side: the
code's *storage type* changed under a key built in encoded space.)

Reading is deliberately narrow: the weight-key columns only, then distinct. The
train parquet is the largest input this pipeline has and the answer needs no
other column.
"""

from typing import Optional

import pyarrow.dataset as pads

from recsys_tfb.io.extract import (
    composite_key_series,
    decode_weight_keys,
    nameable_weight_entries,
)


def distinct_weight_keys(
    train_handle, weight_keys: list, decode_map: dict,
) -> Optional[set]:
    """The composite keys the train parquet actually carries, decoded.

    ``None`` — not an empty set — when the parquet has no column for some
    component of the key. That is a different finding from "this key matched no
    row", and the caller is the one that decides what to make of it: an empty
    set would let a caller report a per-entry miss for a key the parquet could
    never have answered.

    Rows whose code names no category contribute no key: no configured entry
    can name them either, so counting them would vouch for a match that the
    weighting in ``io/extract.py`` pins to 1.0.
    """
    ds = pads.dataset(train_handle.path, format="parquet")
    if any(k not in ds.schema.names for k in weight_keys):
        return None
    pdf = ds.to_table(columns=list(weight_keys)).to_pandas().drop_duplicates()
    decoded, undecodable = decode_weight_keys(pdf, decode_map)
    keys = composite_key_series(decoded, weight_keys)
    return set(keys[~undecodable].tolist())


def nameable_key(key, weight, weight_keys: list, decode_map: dict) -> Optional[str]:
    """One config entry as the composite key a parquet row would carry.

    ``None`` when the entry names a category absent from ``category_mappings``:
    no row can decode to it, so the lookup would silently never fire.
    ``io/extract.py`` drops such an entry from the weight table for the same
    reason, so ``None`` here and "no weight applied" there are the same event.

    Otherwise the key is returned unchanged — the config and the decoded parquet
    are already in one vocabulary, which is the whole point of decoding.
    """
    nameable, _ = nameable_weight_entries({key: weight}, weight_keys, decode_map)
    return next(iter(nameable), None)
