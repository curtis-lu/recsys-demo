"""逐特徵統計（null rate / mean / std / min / max / n_distinct）。"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.io.handles import require_complete_cache
from recsys_tfb.models.feature_view import model_feature_columns

from . import data_access
from ._util import _to_native

logger = logging.getLogger(__name__)


def compute_feature_statistics(
    train_parquet_handle, model, preprocessor: dict, parameters: dict,
) -> dict:
    """Per-feature null_rate / mean,std,min,max (numeric) / n_distinct, plus the
    ``single_value`` and ``high_null`` flags.

    Memory: the row count comes from parquet metadata, then only the sampled
    ``sample_rows`` rows are read (bounded take) instead of loading the whole
    train split and down-sampling afterwards. The sampled indices are unchanged
    (``RandomState(42).choice``), so the output stays bit-for-bit identical.

    Takes ``model`` although this is a *data*-layer diagnosis — the null rate and
    mean of a training feature owe nothing to a booster. The model is here purely
    as the authority on *which* columns to summarize (ADR-0014 decision 7).

    Not because the alternative is unsafe. Re-deriving the column set from
    ``training.feature_selection`` cannot silently drift: that key lives in the
    ``training:`` block, so editing it bumps ``model_version``, the model's
    catalog path moves, and the whole training chain is pulled back. ADR-0014 is
    explicit that this is interface work, not a bug fix. The reason is that
    ``preprocessor_view`` is memory-only, so reading it forces ``select_features``
    into any slice that wants this node — while ``model`` and ``preprocessor``
    both have catalog entries.

    The coupling is accepted because ``feature_statistics`` already lands under
    ``data/models/${model_version}/``: computing it for a model that does not
    exist was never meaningful.

    The edge is not only a cost. It also orders this node after the one that
    produces the model, where it always belonged: without it the topological sort
    put a diagnosis of ``data/models/${model_version}/`` *ahead* of HPO, so
    ``--from-node compute_feature_statistics`` re-ran HPO and the final fit to
    regenerate this JSON (18 nodes, now 13). What it does cost is
    ``--only-node compute_feature_statistics``, which now needs a
    ``model_version``-scoped input rather than only ``base_dataset_version`` ones,
    and ``--from-node calibrate_model``, which picks up this node's train handle.
    Both slices are pinned in ``tests/test_pipelines/test_resume_contracts.py``.
    """
    cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    if not cfg.get("enabled", True):
        return {}
    sample_rows = int(cfg.get("sample_rows", 500000))
    high_null_threshold = float(cfg.get("high_null_threshold", 0.5))
    # Decision — which features get summarized: the model, not
    # apply_feature_selection(preprocessor, parameters). Pick the config and the
    # stats still come out, just over whatever column set the *current* config
    # names; the docstring argues why that is a worse authority than the model
    # even though the version mechanism keeps it from being an outright bug.
    feature_cols = model_feature_columns(model, preprocessor)

    # Pre-check (input) — an interrupted copy reads as a smaller split rather
    # than as an error, so count_rows would return a number and every statistic
    # below would describe an unknown fraction of train (ADR-0014 decision 7).
    # The cache node's opposite behaviour on the same marker — clear and rebuild
    # from Hive — is the right one there and is not touched.
    require_complete_cache(train_parquet_handle)

    path = train_parquet_handle.path
    n = data_access.count_rows(path)
    if n > sample_rows:
        idx = np.sort(np.random.RandomState(42).choice(n, size=sample_rows, replace=False))
        logger.info("feature_statistics: bounded take %d of %d rows", sample_rows, n)
    else:
        idx = np.arange(n, dtype=np.int64)
        logger.info("feature_statistics: reading all %d rows (<= sample_rows)", n)
    pdf = data_access.take_rows(path, idx, columns=feature_cols)
    log_data_volume(logger, "feature_statistics.sample", pdf, deep=True)

    stats: dict = {}
    for col in feature_cols:
        s = pdf[col]
        null_rate = float(s.isna().mean())
        n_distinct = int(s.nunique(dropna=True))
        entry = {
            "null_rate": null_rate,
            "n_distinct": n_distinct,
            "single_value": n_distinct <= 1,
            "high_null": null_rate >= high_null_threshold,
        }
        if pd.api.types.is_numeric_dtype(s):
            entry["mean"] = _to_native(s.mean())
            entry["std"] = _to_native(s.std())
            entry["min"] = _to_native(s.min())
            entry["max"] = _to_native(s.max())
        stats[col] = entry
    logger.info("feature_statistics: %d features summarized", len(stats))
    return stats
