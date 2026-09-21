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

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric.uncertainty import paired_bootstrap_delta
from recsys_tfb.evaluation.metrics import macro_from_per_item, positive_row_contributions

_CLIP_EPS = 1e-12
_HASH_BUCKETS = 100_000


#: 在 driver 上對 query group 內部排名的診斷——也就是宣告 ``event`` 之後算不出
#: 可重現數字的那幾項。判準（spec #426 決定 E）是「這項診斷是否假設同一個
#: query group 內 item 唯一」，逐項對程式碼確認過：
#:
#: * ``suppression``——把「負例排在正例之上」彙總成 ``groupby(["pos_item",
#:   "sup_item"])`` 的 item 對，並算 item 對之間的共買 lift。同一個 item 在一組
#:   裡有多列時會生出 ``(A, A)`` 這種自己壓制自己的對，整張帳本的意思就變了。
#: * ``item_ability``——量同一個 item 內正例列與負例列的 AUC，並在
#:   ``descending_ranks`` 取名次。它自己寫明的盲區「item j 的正例列與負例列分屬
#:   不同 query」在宣告 ``event`` 之後不再成立。
#: * ``config_shift``——Δ 由 ``compute_macro_per_item_map`` 重排後相減得出。
#:
#: 三者都經 ``order_by_score_then_item(..., items)`` 排名，**沒有**接 ``event``
#: 的決勝欄（本票刻意不替診斷加寬，spec #426 決定 E：「診斷不因新角色擴充」），
#: 所以同 item 同分的多列順序由列到達的順序決定——正是 #355 移除掉的那種不可
#: 重現。``model_capacity`` 不在此列：它只讀 booster 的 split gain，完全不碰
#: 診斷抽樣，宣告什麼角色都與它無關。
#:
#: ``"ci"`` 也在裡面，雖然它不在 ``contract.DIAGNOSES`` 裡：它是共用診斷抽樣的
#: 第四個消費者（``evaluation.diagnosis.ci``），而它的 bootstrap 一樣經
#: ``positive_row_contributions`` 在 driver 上取組內名次（``uncertainty.py``）。
#: 頭號 mAP 本身是 Spark 算的、有接 event 決勝欄，所以它是對的；跟著它的 CI 若
#: 用另一套（任意的）同分順序算，兩個數字會併排印在同一行而彼此不對帳。
#:
#: 四項一起跳過還有一個結構上的後果，是刻意的：它們是共用抽樣僅有的消費者，
#: 所以抽樣節點那一關「每個接線的消費者都停用了」會成立，整份抽樣不會被抽——
#: 而那份抽樣的欄位本來就不含 ``event`` 的欄（見 :func:`draw_diagnosis_sample`
#: 的 docstring），拿加寬後的 identity 去對它去重會 ``KeyError``。
_QUERY_RANKING_DIAGNOSES = (
    "config_shift", "item_ability", "suppression", "ci",
)


def schema_skip_reason(parameters: dict, name: str) -> Optional[str]:
    """這項診斷在目前宣告的 schema 下算不算得出來；算不出來就回一句原因。

    回 ``None`` ＝ 照跑。沒宣告任何選用角色時對每一項都回 ``None``，所以既有
    部署的每一項診斷照跑、輸出逐值不變。

    原因字串同時是報表上印的那一句：跳過而不說為什麼，讀者只會看到一頁憑空
    消失，分不出「這版還沒有這項」與「這次刻意沒算」（見 ``contract`` 模組
    docstring 對這兩者的區分）。
    """
    if name not in _QUERY_RANKING_DIAGNOSES:
        return None
    event_cols = get_schema(parameters).get("event", [])
    if not event_cols:
        return None
    return (
        f"宣告了 schema.columns.event（{', '.join(event_cols)}）："
        f"同一個 query group 裡同一個 item 可以有多列，而這項診斷假設 item "
        f"在組內唯一——它在 driver 上的名次只以 item 決勝，同 item 同分的多列"
        f"排序會隨列到達的順序改變。本次跳過。"
    )


def diag_cfg(parameters: dict) -> dict:
    return ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})


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
    編號照 key 排序給（``sort=True``），不照出現順序：抽樣列的順序由 Spark
    決定、換平行度就會變，照出現順序編號的話，依 query 順序列出的東西
    （壓制範例、名次清單）也跟著變（#355）。家族裡自己 factorize
    ``groups``／``clusters`` 的地方同一個理由，都帶 ``sort=True``。
    ``items``／``y``：schema 對應欄直接投影成陣列，兩邊逐字相同的一行。
    ``items`` 保留原始型別、不轉字串：它同時是同分的決勝值，數字 item 要照
    數字大小比才跟 Spark 的名次一致（``utils.ranking``）。要當名字用的地方
    自己 ``str()``。

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
    factorize 的自己再包一層 ``pd.factorize(..., sort=True)[0]``。
    """
    query_cols = schema["query_group_columns"]
    groups = pd.factorize(query_key(pdf, query_cols), sort=True)[0]
    items = pdf[schema["item"]].to_numpy()
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
    """每個 item 的 AP（未加權）＋ macro。

    ``items`` 傳 item 欄的原始值：同分時照它排名（``utils.ranking``），數字
    item 要照數字大小比，所以不能先轉成字串；回傳的 key 才轉字串。

    原本在 ``item_ability/_compute.py``
    與 ``scripts/item_ability_diagnosis.py``／``scripts/suppression_ledger_
    diagnosis.py`` 各自維護一份逐位元組相同的副本，``suppression`` 是第四個
    消費者，門檻到了（見本檔案模組 docstring：兩個以上實例逐字相同才抽），
    Task 5.1 把它搬到這裡共用。
    """
    contrib, row_idx = positive_row_contributions(
        groups, items, y, score, mp["k"]
    )
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
