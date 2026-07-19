"""config_shift 計算層：抽樣比例與 sample weight 在理論上引入的 log-odds 偏移。

回答的問題（只有這一個）：**抽樣設定與 sample weight 在理論上對每個
(context, item) 引入了多少 log-odds 偏移；把它從模型原始分數扣掉之後，macro
per-item mAP 變化多少（Δ）**。

偏移怎麼來的
------------
下採樣（``dataset.sample_ratio_overrides``）與 sample weight
（``training.sample_weights``）都只改變正負例的相對曝光量。在 log-odds 空間
裡，這種改變是一個**加性常數**：

    offset(a, j) = ln(r_pos / r_neg) + ln(w_pos / w_neg)

其中 ``a`` ＝ context（``offset_context_columns`` 推出來的那些欄），``j`` ＝ item。
``F − offset`` 就是「若當初沒有這些設定、模型本來會輸出的分數」的一階近似。

⚠ 這個推導的前提是 **pointwise 機率型 objective**（分數可讀成 log-odds）。
``training.objective`` 允許 ``lambdarank`` 等 pairwise/listwise 設定，此時
``score_uncalibrated`` 是無界的原始分數、不是 log-odds，相減沒有理論基礎。
偵測到分數落在 (0,1) 之外時會在 ``notes`` 標明——但 offset 矩陣與 spread 是
純 config 算術，與 objective 無關，那部分仍然成立。

兩個 spread，為什麼要兩個
-------------------------
``offset_spread``（每個 context group 內 ``max − min``）回答「**config 說了
什麼**」；``query_offset_spread``（在實際樣本上、逐 query 算 ``max − min`` 的
分位數）回答「**真正抵達排序的是什麼**」。兩者在一個前提下相等：

    context 欄在每個 query 內為常數。

當 context 欄是 **entity 級**屬性（客群、分行）時前提成立——一個 query ＝ 一位
客戶在一個時點，整個 query 落在同一個 context group 內，group 間的 offset 差
不會出現在任何一個 query 裡。

**但前提經常不成立。** context ＝ ``sample_group_keys ∪ sample_weight_keys −
{item, label}``，而這些鍵允許取自 ``carry_columns``／``categorical_columns``
——產品層級、產品類別這種 **item 級**屬性完全合法。此時同一個 query 內同時
存在多個 context group，各 group 內部 offset 均勻（``offset_spread`` 全為 0）
卻在 query 內產生真實的名次偏移。實測：``sample_group_keys=["prod_tier",
"label"]``、四個 item 分屬 hi／lo 兩層、層內比例一致 → ``offset_spread =
{'hi': 0.0, 'lo': 0.0}`` 而 ``delta = 0.194``。**只看 offset_spread 會把這種
偏移整個漏掉**，所以 ``query_offset_spread`` 是必要的第二個視角，且它對兩種
context 都成立。context 欄在 query 內非常數時 ``notes`` 會標明。

三個相對於試作腳本（``scripts/config_sorting_shift_diagnosis.py``）的行為修正
--------------------------------------------------------------------------
1. **讀不到 ``score_uncalibrated`` 直接 raise，不退回 ``score``。**
   offset 活在**模型輸出的 log-odds 空間**，校準層是後貼上去的一個單調變換。
   拿校準後的分數去扣理論 offset，是把兩個不同空間的量相減——得到的 Δ 不是
   任何東西的估計值，只是一個看起來像數字的數字。這種錯必須吵，因為它靜默
   時完全看不出來（Δ 照樣會印出一個小數）。
2. **offset 查表零命中的 key 要回報，不靜默當成「無 override」。**
   ``overrides.get(key, default)`` 未命中時安靜地退回預設值，於是 offset 全部
   算成 0、Δ 算成 0。而 Δ ≈ 0 正是本模組宣稱「可以把整個方向排除掉」的訊號
   ——靜默失敗會讓讀者排除掉真正的原因。實測：同一份資料同一份 config，只把
   context 欄的 dtype 從 ``int64`` 換成 ``float64``（＝Spark 整數欄含任一 NULL
   經 ``toPandas()`` 後的**必然**結果），key 從 ``"3|a|0"`` 變成 ``"3.0|a|0"``
   → spread 由 1.609 掉到 0.0、delta 由 −0.200 掉到 0.0、``notes`` 全空。
   零命中**不 raise**（config 裡有一條用不到的 override 是合法的），但必須
   出現在 ``unmatched_override_keys`` 與 ``notes`` 裡。
3. **CI 用既有的 ``uncertainty.paired_bootstrap_delta``，不自己寫重抽迴圈。**
   那份是經過驗收的分層配對版本（層內重抽、``inclusion_weight`` 修正、兩側在
   同一組重抽樣本上相減）。在診斷模組裡再寫一份 bootstrap ＝ 兩份統計方法各自
   漂移，而且第二份不會有人去驗。

不下結論
--------
本模組只輸出數字、分布與對照點。**沒有** severity／verdict／建議動作，也沒有
把連續量切成離散類別的門檻——判斷留給讀者。試作腳本裡的 ``interpretation()``
與 ``has_sorting_effect_by_config``（把 spread 閾值化成一個叫「有沒有排序效果」
的布林）刻意沒有移植過來。
"""
from __future__ import annotations

import logging
import math
from typing import Any, Optional

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import diag_cfg, metric_params, to_logit
from recsys_tfb.diagnosis.metric.uncertainty import paired_bootstrap_delta
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)

#: 唯一可用的分數欄。見模組 docstring 修正 1——不設 fallback 是刻意的。
SCORE_COL = "score_uncalibrated"


def _key_from_values(keys: list[str], values: dict[str, Any]) -> str:
    return "|".join(str(values[k]) for k in keys)


def _offset_for_values(
    values: dict[str, Any],
    *,
    parameters: dict,
    schema: dict,
    matched: Optional[dict[str, set]] = None,
) -> float:
    """單一 (context, item) 的理論 log-odds 偏移 ＝ ln(r_pos/r_neg) + ln(w_pos/w_neg)。

    只有當 label 欄出現在 ``sample_group_keys`` / ``sample_weight_keys`` 裡，
    設定才會動到正負例的相對曝光；否則該項貢獻 0（正負例被同等對待，log-odds
    不變）。

    ``matched`` 是命中觀測用的可變容器（``{"ratio": set(), "weight": set()}``）：
    每次查表命中就把該 key 記進去，呼叫端據此算出「config 宣告了但這批樣本零
    命中」的 key（見模組 docstring 修正 2）。傳 None ＝ 不觀測。
    """
    label_col = schema["label"]
    ds = parameters.get("dataset", {}) or {}
    tr = parameters.get("training", {}) or {}

    offset = 0.0

    group_keys = list(ds.get("sample_group_keys", []) or [])
    if label_col in group_keys:
        r_default = float(ds.get("sample_ratio", 1.0))
        overrides = ds.get("sample_ratio_overrides", {}) or {}
        pos_vals = dict(values)
        neg_vals = dict(values)
        pos_vals[label_col] = "1"
        neg_vals[label_col] = "0"
        key_pos = _key_from_values(group_keys, pos_vals)
        key_neg = _key_from_values(group_keys, neg_vals)
        if matched is not None:
            matched["ratio"].update(k for k in (key_pos, key_neg) if k in overrides)
        r_pos = float(overrides.get(key_pos, r_default))
        r_neg = float(overrides.get(key_neg, r_default))
        for key, val in ((key_pos, r_pos), (key_neg, r_neg)):
            if val <= 0.0:
                raise ValueError(
                    f"offset 算術需要正的抽樣比例，但查到 {val}："
                    f"dataset.sample_ratio_overrides[{key!r}]"
                    f"（該 key 不存在時取 dataset.sample_ratio={r_default}）。"
                    f"比例為 0 或負數時 ln(r_pos/r_neg) 無定義。"
                )
        offset += math.log(r_pos / r_neg)

    weight_keys = list(tr.get("sample_weight_keys", []) or [])
    if label_col in weight_keys:
        weights = tr.get("sample_weights", {}) or {}
        pos_vals = dict(values)
        neg_vals = dict(values)
        pos_vals[label_col] = "1"
        neg_vals[label_col] = "0"
        key_pos = _key_from_values(weight_keys, pos_vals)
        key_neg = _key_from_values(weight_keys, neg_vals)
        if matched is not None:
            matched["weight"].update(k for k in (key_pos, key_neg) if k in weights)
        w_pos = float(weights.get(key_pos, 1.0))
        w_neg = float(weights.get(key_neg, 1.0))
        for key, val in ((key_pos, w_pos), (key_neg, w_neg)):
            if val <= 0.0:
                raise ValueError(
                    f"offset 算術需要正的 sample weight，但查到 {val}："
                    f"training.sample_weights[{key!r}]"
                    f"（該 key 不存在時取 1.0）。"
                    f"權重為 0 或負數時 ln(w_pos/w_neg) 無定義。"
                )
        offset += math.log(w_pos / w_neg)

    return float(offset)


def offset_context_columns(parameters: dict, schema: dict) -> list[str]:
    """offset 除了 item 之外還依哪些欄變動（＝ context 的定義）。

    ``sample_group_keys`` ∪ ``sample_weight_keys``，扣掉 item 與 label。空清單
    代表 offset 只依 item——此時全體列自成單一 context ``"ALL"``。

    ⚠ 這些欄**不保證是 entity 級**（可以是產品層級這種 item 級屬性），所以
    「一個 query 落在單一 context group 內」不是可以依賴的性質。見模組 docstring
    「兩個 spread」。
    """
    item_col = schema["item"]
    label_col = schema["label"]
    cols: list[str] = []
    for key in (
        list((parameters.get("dataset", {}) or {}).get("sample_group_keys", []) or [])
        + list((parameters.get("training", {}) or {}).get("sample_weight_keys", []) or [])
    ):
        if key not in (item_col, label_col) and key not in cols:
            cols.append(key)
    return cols


def unmatched_override_keys(
    parameters: dict, schema: dict, matched: dict[str, set],
) -> list[dict[str, str]]:
    """config 宣告了、但這批樣本一次都沒查到的 override key。

    只列**實際會被查詢**的那個家族：label 欄不在 ``sample_group_keys`` 裡時
    根本不會查 ``sample_ratio_overrides``，把它整份列成「零命中」是誤報。
    """
    label_col = schema["label"]
    ds = parameters.get("dataset", {}) or {}
    tr = parameters.get("training", {}) or {}
    out: list[dict[str, str]] = []
    if label_col in list(ds.get("sample_group_keys", []) or []):
        declared = set((ds.get("sample_ratio_overrides", {}) or {}).keys())
        for key in sorted(declared - matched["ratio"]):
            out.append({"config": "dataset.sample_ratio_overrides", "key": key})
    if label_col in list(tr.get("sample_weight_keys", []) or []):
        declared = set((tr.get("sample_weights", {}) or {}).keys())
        for key in sorted(declared - matched["weight"]):
            out.append({"config": "training.sample_weights", "key": key})
    return out


def build_offset_frame(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
    matched: Optional[dict[str, set]] = None,
) -> tuple[pd.DataFrame, dict]:
    """(context × item) 的 offset 矩陣。回傳 ``(offset_df, meta)``。

    只枚舉**實際觀測到**的 (context, item) 組合，不做笛卡兒積：context 為 item
    級屬性時，「產品層級 hi × 只存在於 lo 的產品」這種格子並不存在於資料裡，
    列出來會虛報一個不存在的偏移，而且會把它算進中位數。

    ``offset_centered``（offset 減該 context 內的中位數）是**呈現用**欄位：
    query 內名次只看相對差，整組平移不影響排序，去中位數只是讓讀者容易比。
    Δ 的計算走 :func:`row_offsets` 的原始逐列 offset，不經過這裡。群內 spread
    由呼叫端在這張表上算（單一真實來源，本函式不另存一份）。
    """
    item_col = schema["item"]
    a_cols = offset_context_columns(parameters, schema)

    observed = (
        pdf[[*a_cols, item_col]]
        .astype({item_col: str})
        .drop_duplicates()
        .sort_values([*a_cols, item_col])
    )
    # 單一 context 欄時傳純量而非長度 1 的 list：pandas 對後者發 FutureWarning
    # （未來版本會改回傳長度 1 的 tuple）。下面統一把 ctx 正規化成 tuple。
    grouped = (
        observed.groupby(a_cols[0] if len(a_cols) == 1 else a_cols, sort=True)
        if a_cols else [((), observed)]
    )

    rows: list[dict[str, Any]] = []
    for ctx, sub in grouped:
        ctx_tuple = ctx if isinstance(ctx, tuple) else (ctx,)
        a_values = dict(zip(a_cols, ctx_tuple))
        item_offsets: dict[str, float] = {}
        for item in sub[item_col].tolist():
            vals = dict(a_values)
            vals[item_col] = item
            # label 欄刻意不在這裡設：_offset_for_values 自己填 "1"/"0" 兩側
            # 各查一次，在這裡先填一個值是死碼（換成任意字串結果不變）。
            item_offsets[item] = _offset_for_values(
                vals, parameters=parameters, schema=schema, matched=matched,
            )
        offsets = list(item_offsets.values())
        median = float(np.median(offsets)) if offsets else 0.0
        group_label = "ALL" if not a_cols else " | ".join(str(v) for v in ctx_tuple)
        for item, off in item_offsets.items():
            rows.append({
                "group": group_label,
                **a_values,
                item_col: item,
                "offset": off,
                "offset_centered": off - median,
            })

    offset_df = pd.DataFrame(rows)
    meta = {"items": sorted({str(r[item_col]) for r in rows})}
    return offset_df, meta


def row_offsets(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
    matched: Optional[dict[str, set]] = None,
) -> np.ndarray:
    """逐列 offset 向量（與 ``pdf`` 等長、逐列對齊）。

    Δ 與其 CI 都用這條向量：offset 依 (context, item) 決定，不是只依 item，所以
    ``paired_bootstrap_delta`` 要走「向量」那條路而不是 ``{item: offset}`` dict。
    """
    item_col = schema["item"]
    a_cols = offset_context_columns(parameters, schema)
    cache: dict[tuple, float] = {}
    out = np.zeros(len(pdf), dtype=np.float64)
    for i, row in enumerate(pdf[[*a_cols, item_col]].itertuples(index=False, name=None)):
        ctx = tuple(row)
        if ctx not in cache:
            vals = dict(zip([*a_cols, item_col], ctx))
            cache[ctx] = _offset_for_values(
                vals, parameters=parameters, schema=schema, matched=matched,
            )
        out[i] = cache[ctx]
    return out


def _validate(pdf: pd.DataFrame, parameters: dict, schema: dict) -> list[str]:
    """必要欄位檢查。回傳 offset 的 context 欄清單。"""
    if SCORE_COL not in pdf.columns:
        raise ValueError(
            f"config_shift 需要 {SCORE_COL!r} 欄，但輸入沒有這一欄。"
            f"這裡刻意不退回 schema.score：理論 offset 活在模型輸出的 log-odds "
            f"空間，校準後的分數是另一個空間的量，兩者相減得到的 Δ 沒有意義。"
        )
    query_cols = [schema["time"], *schema["entity"]]
    required = [*query_cols, schema["item"], schema["label"]]
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"config_shift 輸入缺必要欄位：{missing}")

    context_cols = offset_context_columns(parameters, schema)
    missing_context = [c for c in context_cols if c not in pdf.columns]
    if missing_context:
        raise ValueError(
            f"config_shift 輸入缺 offset context 欄位 {missing_context}。"
            "這些欄由 dataset.sample_group_keys / training.sample_weight_keys "
            "推導，診斷抽樣必須把它們一起帶出來。"
        )
    return context_cols


def _query_key(pdf: pd.DataFrame, query_cols: list[str]) -> pd.Series:
    parts = [pdf[c].astype(str) for c in query_cols]
    out = parts[0]
    for p in parts[1:]:
        out = out.str.cat(p, sep="|")
    return out


def compute(diagnosis_sample: tuple[pd.DataFrame, dict], parameters: dict) -> dict:
    """回傳 JSON-safe dict（會直接被 JSONDataset 寫檔）。

    ``diagnosis_sample`` ＝ ``sample.draw_diagnosis_sample`` 的 ``(sample_pdf,
    sample_meta)``。所有 numpy 純量在出口轉成 python ``float``／``int``。

    **三條 return 路徑（停用／空樣本／完整）的 key set 完全相同**，未計算的值
    留 None／空容器。呼叫端（``render``）因此不必為每個鍵寫存在性判斷——少寫
    的那些判斷正是最少被跑到的路徑上的 KeyError 來源。
    """
    sample_pdf, sample_meta = diagnosis_sample
    schema = get_schema(parameters)
    diag = diag_cfg(parameters)
    cfg = diag.get("config_shift", {}) or {}
    ci_cfg = diag.get("ci", {}) or {}
    mp = metric_params(parameters)
    ci_info = {
        "enabled": bool(ci_cfg.get("enabled", True)),
        "n_boot": int(ci_cfg.get("n_boot", 200)),
        "seed": int((diag.get("sample", {}) or {}).get("seed", 42)),
    }

    out: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled", True)),
        "score_col_used": SCORE_COL,
        "metric_params": mp,
        "context_columns": [],
        "items": [],
        "offset_spread": {},
        "query_offset_spread": {},
        "offset_matrix": {},
        "offset_centered": {},
        "unmatched_override_keys": [],
        "baseline_map": None,
        "corrected_map": None,
        "delta": None,
        "delta_ci_low": None,
        "delta_ci_high": None,
        "ci": ci_info,
        "per_item": [],
        "sample": {},
        "sample_meta": dict(sample_meta or {}),
        "notes": [],
    }
    if not out["enabled"]:
        out["notes"].append("evaluation.diagnosis.config_shift.enabled = false——未計算。")
        return out

    context_cols = _validate(sample_pdf, parameters, schema)
    out["context_columns"] = context_cols

    query_cols = [schema["time"], *schema["entity"]]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]

    # 空樣本必須在 build_offset_frame **之前**擋掉：offset 矩陣是從觀測到的
    # (context, item) 組合枚舉的，空資料 → 零個組合 → offset_df 是一個「連欄位
    # 都沒有」的空 DataFrame，下面的 groupby("group") 會 KeyError。這是良性退化
    # 輸入（抽樣沒抽到東西），不是壞輸入，不該炸。
    if len(sample_pdf) == 0:
        out["notes"].append("診斷抽樣為空——offset 矩陣與 Δ 均未計算。")
        return out

    matched: dict[str, set] = {"ratio": set(), "weight": set()}

    offset_df, offset_meta = build_offset_frame(
        sample_pdf, parameters, schema, matched=matched,
    )
    # 群內 spread：每個 context group 自己算 max − min。這是「config 說了什麼」
    # 那一面；「真正抵達排序的是什麼」由下面的 query_offset_spread 回答。
    out["offset_spread"] = {
        str(g): float(sub["offset"].max() - sub["offset"].min())
        for g, sub in offset_df.groupby("group")
    }
    out["offset_matrix"] = {
        str(g): {str(r[item_col]): float(r["offset"])
                 for _, r in sub.iterrows()}
        for g, sub in offset_df.groupby("group")
    }
    out["offset_centered"] = {
        str(g): {str(r[item_col]): float(r["offset_centered"])
                 for _, r in sub.iterrows()}
        for g, sub in offset_df.groupby("group")
    }
    out["items"] = offset_meta["items"]

    groups = pd.factorize(_query_key(sample_pdf, query_cols))[0]
    clusters = _query_key(sample_pdf, entity_cols)
    items = sample_pdf[item_col].astype(str).to_numpy()
    y = sample_pdf[label_col].to_numpy(dtype=np.int64)
    z, logit_notes = to_logit(sample_pdf[SCORE_COL].to_numpy(dtype=np.float64))
    out["notes"].extend(logit_notes)
    if logit_notes:
        # to_logit 只說「分數超出 (0,1)、單位改成原始分數尺度」。真正要講的是
        # 這讓 Δ 的推導前提失效——offset 是 log-odds 上的加性常數，分數不是
        # log-odds 時相減沒有理論基礎。offset 矩陣與 spread 不受影響。
        out["notes"].append(
            f"Δ 的推導前提是 pointwise 機率型 objective（{SCORE_COL} 可讀成 "
            "log-odds）。偵測到分數落在 (0,1) 之外（例如 training.objective 為 "
            "lambdarank 等 pairwise/listwise 設定），此前提可能不成立。"
            "offset 矩陣與兩個 spread 是純 config 算術，不受此影響。"
        )
    offs = row_offsets(sample_pdf, parameters, schema, matched=matched)

    # 逐 query 的實際 offset 範圍。這一面對 entity 級與 item 級 context 都成立
    # （見模組 docstring「兩個 spread」）。只給分位數，不設門檻、不給布林。
    q_spread = (
        pd.Series(offs).groupby(groups).agg(lambda s: float(s.max() - s.min()))
    ).to_numpy(dtype=np.float64)
    out["query_offset_spread"] = {
        "mean": float(np.mean(q_spread)),
        "p50": float(np.percentile(q_spread, 50)),
        "p90": float(np.percentile(q_spread, 90)),
        "max": float(np.max(q_spread)),
        "n_queries": int(len(q_spread)),
    }

    # context 欄在 query 內非常數 ＝ 兩個 spread 會分歧，讀者必須知道該看哪個。
    if context_cols:
        varying = [
            c for c in context_cols
            if int(sample_pdf.groupby(groups)[c].nunique().max()) > 1
        ]
        if varying:
            out["notes"].append(
                f"context 欄 {varying} 在部分 query 內非常數（item 級屬性）："
                "offset_spread 是同一 context group 內的 max − min，此時它不等於"
                "單一 query 內實際出現的 offset 範圍；逐 query 的實際範圍見 "
                "query_offset_spread。"
            )

    out["unmatched_override_keys"] = unmatched_override_keys(
        parameters, schema, matched,
    )
    if out["unmatched_override_keys"]:
        listed = ", ".join(
            f"{r['config']}[{r['key']!r}]"
            for r in out["unmatched_override_keys"][:10]
        )
        more = len(out["unmatched_override_keys"]) - 10
        out["notes"].append(
            f"有 {len(out['unmatched_override_keys'])} 個 override key 在本次樣本"
            f"零命中：{listed}{f'（另有 {more} 個）' if more > 0 else ''}。"
            "零命中的可能成因：該組合不存在於本次樣本、key 的字串格式與樣本值"
            "不一致（例如整數欄含 NULL 經 toPandas() 後變成 float64，'3' → "
            "'3.0'）、或 key 有錯字。零命中的 key 對 offset 沒有任何作用。"
        )

    # Horvitz–Thompson 修正：點估計與 CI 必須用同一組權重，否則 CI 可能不含點
    # 估計。缺欄（未分層抽樣）時走 weights=None 的原始未加權路徑。
    ht_weights = (
        sample_pdf["inclusion_weight"].to_numpy(dtype=np.float64)
        if "inclusion_weight" in sample_pdf.columns
        else None
    )

    baseline = float(compute_macro_per_item_map(
        groups, items, y, z, weights=ht_weights, **mp
    ))
    corrected = float(compute_macro_per_item_map(
        groups, items, y, z - offs, weights=ht_weights, **mp
    ))
    out["baseline_map"] = baseline
    out["corrected_map"] = corrected
    out["delta"] = float(corrected - baseline)

    if ci_info["enabled"]:
        frame = pd.DataFrame({
            "group": groups,
            "cluster": clusters.to_numpy(),
            "item": items,
            "label": y,
            "score": z,
        })
        for col in ("stratum", "inclusion_weight"):
            if col in sample_pdf.columns:
                frame[col] = sample_pdf[col].to_numpy()
        # paired_bootstrap_delta 回的是 mAP(F) − mAP(F − shift) ＝ baseline −
        # corrected，與本模組的 Δ ＝ corrected − baseline 反號。取負後上下界
        # 對調，才是 Δ 自己的 [2.5%, 97.5%]。
        lo, hi = paired_bootstrap_delta(
            frame, mp, offs,
            n_boot=ci_info["n_boot"], seed=ci_info["seed"],
        )
        out["delta_ci_low"] = float(-hi)
        out["delta_ci_high"] = float(-lo)

    # 逐項替換：一次只扣掉一個 item 的 offset。Σ Δ_j ≠ Δ（名次耦合），這句話由
    # Task 2.3 的 SCOPE 擁有並顯示，不在計算層再存一份。
    pos_mask = y == 1
    w_pos_rows = (
        ht_weights if ht_weights is not None
        else np.ones(len(sample_pdf), dtype=np.float64)
    )
    per_item: list[dict[str, Any]] = []
    for item in sorted(set(items.tolist())):
        mask = items == item
        z_one = z.copy()
        z_one[mask] = z_one[mask] - offs[mask]
        m_one = float(compute_macro_per_item_map(
            groups, items, y, z_one, weights=ht_weights, **mp
        ))
        item_pos = mask & pos_mask
        per_item.append({
            "item": str(item),
            "delta_j": float(m_one - baseline),
            "map_after_only_this_item": m_one,
            # n_pos_raw ＝ 原始列數；n_pos_effective ＝ HT 加權後的有效正例數。
            # mAP 與 min_positives/shrinkage_k/weight_alpha 吃的是**加權**計數
            # （metrics.py 的 weights 路徑），只報 raw 會讓讀者用一個母體去篩
            # 另一個母體算出來的數字。兩個都給，讓讀者自己看。
            "n_pos_raw": int(item_pos.sum()),
            "n_pos_effective": float(w_pos_rows[item_pos].sum()),
            "offset_min": float(np.min(offs[mask])) if mask.any() else None,
            "offset_max": float(np.max(offs[mask])) if mask.any() else None,
        })
    per_item.sort(key=lambda r: r["delta_j"], reverse=True)
    out["per_item"] = per_item

    # 這批列自己的規模。上游抽樣 metadata 原封不動留在 out["sample_meta"]，
    # 兩者刻意分開：sample_meta 講「怎麼抽的」（含 n_queries_sampled 等），
    # sample 講「實際拿到的這批列長什麼樣」。合在一起會出現兩個語意相近的
    # 數字並排，讀者無從判斷該信哪個。
    out["sample"] = {
        "n_rows": int(len(sample_pdf)),
        "n_queries": int(len(np.unique(groups))),
        "n_entities": int(clusters.nunique()),
        "n_items": int(len(set(items.tolist()))),
        "n_positive_rows": int(y.sum()),
        "n_positive_rows_effective": float(w_pos_rows[pos_mask].sum()),
    }

    logger.info(
        "config_shift: %d queries, %d items, baseline=%.6f corrected=%.6f "
        "delta=%.6f, unmatched_override_keys=%d",
        out["sample"]["n_queries"], out["sample"]["n_items"],
        baseline, corrected, out["delta"], len(out["unmatched_override_keys"]),
    )
    return out
