"""Metric-diagnosis 家族共用私有 helper。

本檔案是 metric-diagnosis 家族（``config_shift``、``item_ability``、
``suppression``）的共用 helper。新增函式前先確認是「兩個以上實例逐字相同」
才抽——見 :func:`query_key`／:func:`sample_arrays`／
:func:`ci_for_corrected_minus_baseline` 各自的 docstring 交代「為什麼這是
真的共用、什麼刻意沒抽」。

``_HASH_BUCKETS`` 與 ``utils.hashing.HASH_BUCKETS`` 同值（100_000）——
該模組 top-level import pyspark，而家族的 numpy-leaf 模組刻意保持
pyspark-free 以利無 Spark 單元測試，故本地重申而不 import。
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from recsys_tfb.diagnosis.metric.uncertainty import paired_bootstrap_delta
from recsys_tfb.evaluation.metrics import macro_from_per_item, positive_row_contributions

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


def query_key(pdf: pd.DataFrame, cols: list[str]) -> pd.Series:
    """把多欄併成 ``a|b|c`` 形式的單一 key。

    ``config_shift._query_key`` 與 ``item_ability._join_key`` 逐字相同
    （僅函式名不同），這是 Task 3.3 逐行比對後**唯一**確認可以無條件合併的
    部分——兩邊都只是「join query id」或「join cluster id」的字串併鍵，語意
    完全一致。呼叫端各自決定要不要 ``pd.factorize``：本函式**不**代做，見
    :func:`sample_arrays` docstring 為什麼 clusters 的 factorize 不能抽到
    這裡。
    """
    parts = [pdf[c].astype(str) for c in cols]
    out = parts[0]
    for p in parts[1:]:
        out = out.str.cat(p, sep="|")
    return out


def sample_arrays(
    pdf: pd.DataFrame, schema: dict,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, Optional[np.ndarray], np.ndarray]:
    """診斷抽樣 → ``(groups, items, y, ht_weights, row_weights)``。

    ``groups``：query id（``time`` × ``entity`` 併鍵後 ``pd.factorize``）。
    ``items``／``y``：schema 對應欄直接投影成陣列，兩邊逐字相同的一行。

    ``ht_weights`` 缺 ``inclusion_weight`` 欄時是 ``None``（走未加權路徑，
    供 ``compute_macro_per_item_map`` 等函式 ``weights=None`` 的語意判斷）；
    ``row_weights`` 是同一組權重的「缺席時填 1」版本，給 n_pos_effective
    這種一定要有數字的地方用。兩個都給是刻意的：mAP 的 weights 參數用 None
    與用全 1 是**位元等價**的兩條路，但混用會讓「有沒有加權」在讀碼時看不出
    來。``config_shift`` 原本各自重算一次「缺席時填 1」（``q_agg`` 的權重欄
    與 ``w_pos_rows``），這裡順便去掉那個內部重複；``item_ability`` 本來就
    只要 ``row_weights`` 這一種，用 ``_`` 丟掉 ``ht_weights`` 即可。

    ⚠ **``clusters`` 刻意不在這個回傳值裡。** ``config_shift`` 要的是未
    factorize 的字串 ``pd.Series``（後面呼叫 ``.nunique()``，且直接把
    ``.to_numpy()`` 交給會自行 ``pd.factorize`` 的
    ``uncertainty.paired_bootstrap_delta``）；``item_ability`` 要的是**已經**
    factorize 過的連續 0-based int 陣列（``iter_stratified_cluster_
    multipliers`` 拿它直接當陣列索引，要求連續編碼，不能是任意 int）。這兩個
    不是同一個東西，硬塞進同一個回傳值只會製造一個沒有人真正需要的中間型別
    ——呼叫端各自用 ``query_key(pdf, schema["entity"])`` 現組，需要
    factorize 的自己再包一層 ``pd.factorize(...)[0]``。
    """
    query_cols = [schema["time"], *schema["entity"]]
    groups = pd.factorize(query_key(pdf, query_cols))[0]
    items = pdf[schema["item"]].astype(str).to_numpy()
    y = pdf[schema["label"]].to_numpy(dtype=np.int64)
    if "inclusion_weight" in pdf.columns:
        w = pdf["inclusion_weight"].to_numpy(dtype=np.float64)
        ht_weights: Optional[np.ndarray] = w
        row_weights = w
    else:
        ht_weights = None
        row_weights = np.ones(len(pdf), dtype=np.float64)
    return groups, items, y, ht_weights, row_weights


def ci_for_corrected_minus_baseline(
    frame: pd.DataFrame,
    metric_kwargs: dict,
    shift,
    *,
    n_boot: int,
    seed: int,
) -> tuple[float, float]:
    """``Δ = corrected − baseline`` 的 [2.5%, 97.5%]。

    名字把方向講完了，所以呼叫端不必記得取負。``paired_bootstrap_delta``
    回的是**反向**的差（``mAP(F) − mAP(F − shift)`` ＝ baseline − corrected），
    取負之後上下界也要對調——這兩步只在這裡做一次，供家族內每個「Δ ＝
    corrected − baseline」定義的診斷共用（目前僅 ``config_shift``；見 Task
    3.3 對 (b) 單一消費者是否值得抽的判斷，寫在 PR 說明／回報裡，不重複貼在
    這裡）。

    ``frame`` 的欄位要求與 ``shift`` 的形狀完全比照
    ``uncertainty.paired_bootstrap_delta``，不在此重複；本函式只包一層符號
    轉換，不改變其餘語意。
    """
    lo, hi = paired_bootstrap_delta(
        frame, metric_kwargs, shift, n_boot=n_boot, seed=seed,
    )
    return -hi, -lo


def per_item_ap(
    groups: np.ndarray,
    items: np.ndarray,
    y: np.ndarray,
    score: np.ndarray,
    mp: dict,
) -> tuple[dict[str, float], dict[str, int], float]:
    """每個 item 的 AP（未加權）＋ macro。原本在 ``item_ability/_compute.py``
    與 ``scripts/item_ability_diagnosis.py``／``scripts/suppression_ledger_
    diagnosis.py`` 各自維護一份逐位元組相同的副本，``suppression`` 是第四個
    消費者，門檻到了（見本檔案模組 docstring：兩個以上實例逐字相同才抽），
    Task 5.1 把它搬到這裡共用。
    """
    contrib, row_idx = positive_row_contributions(groups, y, score, mp["k"])
    if len(contrib) == 0:
        return {}, {}, 0.0
    pos_items = items[row_idx].astype(str)
    uniq, inv = np.unique(pos_items, return_inverse=True)
    sums = np.bincount(inv, weights=contrib)
    counts = np.bincount(inv)
    vals = sums / counts
    macro = macro_from_per_item(
        vals,
        counts,
        weight_alpha=mp["weight_alpha"],
        min_positives=mp["min_positives"],
        shrinkage_k=mp["shrinkage_k"],
    )
    return (
        {str(item): float(v) for item, v in zip(uniq, vals)},
        {str(item): int(n) for item, n in zip(uniq, counts)},
        0.0 if macro is None else float(macro),
    )
