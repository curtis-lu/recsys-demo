"""逐特徵統計（null rate / mean / std / min / max / n_distinct）。"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_data_volume

from .. import data_access
from ._util import _to_native

logger = logging.getLogger(__name__)


def compute_feature_statistics(train_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """逐特徵 null_rate / mean,std,min,max（數值）/ n_distinct + single_value,high_null 旗標。

    記憶體：先由 metadata 取列數，再只讀抽中的 ``sample_rows`` 列（bounded take），
    不再全量讀入 train 後才下採樣。抽樣 idx 與過去相同（``RandomState(42).choice``），
    輸出逐位元不變。
    """
    cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    if not cfg.get("enabled", True):
        return {}
    sample_rows = int(cfg.get("sample_rows", 500000))
    high_null_threshold = float(cfg.get("high_null_threshold", 0.5))
    feature_cols = list(preprocessor["feature_columns"])

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
