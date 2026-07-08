"""Metric-diagnosis 家族共用私有 helper（offset_sweep ＋ pair_ledger）.

抽出動機：``debug_inject_offsets`` 的注入語意是分流層閘門的已知答案來源，
必須在家族內完全一致，不允許兩份複製品各自漂移。scope＝僅分流層家族的
兩個 leaf 節點；metric_ci／reconciliation／quadrant 不受影響。

``_HASH_BUCKETS`` 與 ``utils.hashing.HASH_BUCKETS`` 同值（100_000）——
該模組 top-level import pyspark，而分流層家族的 numpy-leaf 模組
（offset_sweep／pair_ledger／本檔）刻意保持 pyspark-free 以利無 Spark
單元測試，故本地重申而不 import。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

_CLIP_EPS = 1e-12
_HASH_BUCKETS = 100_000


def diag_cfg(parameters: dict) -> dict:
    return ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})


def metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    k = m.get("k")
    return {
        "k": int(k) if k is not None else None,
        "weight_alpha": float(m.get("weight_alpha", 0.0)),
        "min_positives": int(m.get("min_positives", 0)),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0)),
    }


def to_logit(scores: np.ndarray) -> tuple[np.ndarray, list[str]]:
    s = np.asarray(scores, dtype=np.float64)
    if len(s) and (s.min() < 0.0 or s.max() > 1.0):
        return s.copy(), [
            "score 超出 (0,1)——略過 logit 變換，δ 單位為原始分數尺度"
        ]
    z = np.clip(s, _CLIP_EPS, 1.0 - _CLIP_EPS)
    return np.log(z / (1.0 - z)), []


def parse_injection(parameters: dict) -> dict:
    return {
        str(k): float(v)
        for k, v in (diag_cfg(parameters)
                     .get("debug_inject_offsets", {}) or {}).items()
    }


def apply_injection(
    z: np.ndarray, items: np.ndarray, inject: dict,
) -> tuple[np.ndarray, list[str]]:
    if not inject:
        return z, []
    notes = [
        f"debug_inject_offsets 生效（僅分流層節點；基準指標為注入後現狀）："
        f"{inject}"
    ]
    unknown = sorted(set(inject) - set(items.tolist()))
    if unknown:
        notes.append(f"注入鍵不在抽樣 item 中（無作用）：{unknown}")
    return z + pd.Series(items).map(inject).fillna(0.0).to_numpy(), notes
