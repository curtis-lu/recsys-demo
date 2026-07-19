"""config_shift 計算層：抽樣比例與 sample weight 在理論上引入的 log-odds 偏移。

回答的問題（只有這一個）：**抽樣設定與 sample weight 在理論上對每個
(客群, item) 引入了多少 log-odds 偏移；把它從模型原始分數扣掉之後，macro
per-item mAP 變化多少（Δ）**。若偏移處處為 0，排序問題就不是訓練設定造成的
——這是一條可以直接把整個方向排除掉的線索，所以它排在診斷順序的第一項。

偏移怎麼來的
------------
下採樣（``dataset.sample_ratio_overrides``）與 sample weight
（``training.sample_weights``）都只改變正負例的相對曝光量。在 log-odds 空間
裡，這種改變是一個**加性常數**：

    offset(a, j) = ln(r_pos / r_neg) + ln(w_pos / w_neg)

其中 ``a`` ＝ 客群（``offset_context_columns`` 推出來的那些欄），``j`` ＝ item。
``F − offset`` 就是「若當初沒有這些設定、模型本來會輸出的分數」的一階近似。

三個相對於試作腳本（``scripts/config_sorting_shift_diagnosis.py``）的行為修正
--------------------------------------------------------------------------
1. **讀不到 ``score_uncalibrated`` 直接 raise，不退回 ``score``。**
   offset 活在**模型輸出的 log-odds 空間**，校準層是後貼上去的一個單調變換。
   拿校準後的分數去扣理論 offset，是把兩個不同空間的量相減——得到的 Δ 不是
   任何東西的估計值，只是一個看起來像數字的數字。這種錯必須吵，因為它靜默
   時完全看不出來（Δ 照樣會印出一個小數）。
2. **``offset_spread`` 依客群分別計算（群內 ``max − min``），不是全域。**
   一個 query ＝ 一位客戶在一個時點 ＝ **單一客群**。query 內的名次只受同一
   query 內各 item 的 offset **相對差**影響；不同客群之間的 offset 差不會出現
   在任何一個 query 裡。拿全域 spread 當指標，會憑空製造出一個根本不影響排序
   的「偏移」數字（例：所有客群的 offset 都群內均勻但彼此相差 ln(1000)，全域
   spread 很大，實際排序影響恰為 0）。
3. **CI 用既有的 ``uncertainty.paired_bootstrap_delta``，不自己寫重抽迴圈。**
   那份是經過驗收的分層配對版本（層內重抽、``inclusion_weight`` 修正、兩側在
   同一組重抽樣本上相減）。在診斷模組裡再寫一份 bootstrap ＝ 兩份統計方法各自
   漂移，而且第二份不會有人去驗。

不下結論
--------
本模組只輸出數字、分布與對照點：offset 矩陣、群內 spread、Δ 與其 CI、逐項替換
的 Δ_j。**沒有** severity／verdict／建議動作，也沒有把連續量切成離散類別的門檻
——判斷留給讀者。試作腳本裡的 ``interpretation()`` 與 ``has_sorting_effect_by_config``
（把 spread 閾值化成一個叫「有沒有排序效果」的布林）刻意沒有移植過來。
"""
from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.diagnosis.metric._common import diag_cfg, metric_params, to_logit
from recsys_tfb.diagnosis.metric.uncertainty import paired_bootstrap_delta
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)

#: 唯一可用的分數欄。見模組 docstring 修正 1——不設 fallback 是刻意的。
SCORE_COL = "score_uncalibrated"

#: Σ Δ_j ≠ Δ。逐項替換是 M 次獨立的介入實驗，不是把 Δ 拆成 M 份。理由：一個
#: item 的分數變動會改變**同一 query 內所有 item 的名次**，各項的效果透過名次
#: 互相耦合，加起來沒有守恆律可言。報表層會原樣顯示這句話。
PER_ITEM_SUM_NOTE = (
    "Σ Δ_j ≠ Δ：名次耦合，這是逐項替換實驗不是分解。"
    "每個 Δ_j 都是「只扣掉這一個 item 的 offset、其餘不動」的獨立實驗結果，"
    "彼此透過同一 query 內的名次相互影響，因此沒有理由相加等於總 Δ。"
)


def _key_from_values(keys: list[str], values: dict[str, Any]) -> str:
    return "|".join(str(values[k]) for k in keys)


def _offset_for_values(
    values: dict[str, Any],
    *,
    parameters: dict,
    schema: dict,
) -> float:
    """單一 (客群, item) 的理論 log-odds 偏移 ＝ ln(r_pos/r_neg) + ln(w_pos/w_neg)。

    只有當 label 欄出現在 ``sample_group_keys`` / ``sample_weight_keys`` 裡，
    設定才會動到正負例的相對曝光；否則該項貢獻 0（正負例被同等對待，log-odds
    不變）。
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
        r_pos = float(overrides.get(_key_from_values(group_keys, pos_vals), r_default))
        r_neg = float(overrides.get(_key_from_values(group_keys, neg_vals), r_default))
        if r_pos <= 0.0 or r_neg <= 0.0:
            raise ValueError(
                f"sample ratio must be positive for offset math; got "
                f"r_pos={r_pos}, r_neg={r_neg}"
            )
        offset += math.log(r_pos / r_neg)

    weight_keys = list(tr.get("sample_weight_keys", []) or [])
    if label_col in weight_keys:
        weights = tr.get("sample_weights", {}) or {}
        pos_vals = dict(values)
        neg_vals = dict(values)
        pos_vals[label_col] = "1"
        neg_vals[label_col] = "0"
        w_pos = float(weights.get(_key_from_values(weight_keys, pos_vals), 1.0))
        w_neg = float(weights.get(_key_from_values(weight_keys, neg_vals), 1.0))
        if w_pos <= 0.0 or w_neg <= 0.0:
            raise ValueError(
                f"sample weights must be positive for offset math; got "
                f"w_pos={w_pos}, w_neg={w_neg}"
            )
        offset += math.log(w_pos / w_neg)

    return float(offset)


def offset_context_columns(parameters: dict, schema: dict) -> list[str]:
    """offset 除了 item 之外還依哪些欄變動（＝「客群」的定義）。

    ``sample_group_keys`` ∪ ``sample_weight_keys``，扣掉 item 與 label。空清單
    代表 offset 只依 item——此時全體列自成單一客群 ``"ALL"``。
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


def build_offset_frame(
    pdf: pd.DataFrame,
    parameters: dict,
    schema: dict,
) -> tuple[pd.DataFrame, dict]:
    """(客群 × item) 的 offset 矩陣。回傳 ``(offset_df, meta)``。

    ``offset_centered``（offset 減群內中位數）與 ``spread`` 是**呈現用**欄位：
    query 內名次只看相對差，所以整組平移不影響排序，去中位數只是讓讀者容易比。
    Δ 的計算走的是 :func:`row_offsets` 的原始逐列 offset，不經過這裡。
    """
    item_col = schema["item"]
    label_col = schema["label"]
    a_cols = offset_context_columns(parameters, schema)

    items = (
        list((schema.get("categorical_values", {}) or {}).get(item_col, []) or [])
        or sorted(pdf[item_col].astype(str).unique().tolist())
    )
    if a_cols:
        groups_pdf = pdf[a_cols].drop_duplicates().sort_values(a_cols)
        contexts = [tuple(row) for row in groups_pdf.to_numpy()]
    else:
        contexts = [tuple()]

    rows: list[dict[str, Any]] = []
    for ctx in contexts:
        a_values = dict(zip(a_cols, ctx))
        offsets: list[float] = []
        item_offsets: dict[str, float] = {}
        for item in items:
            vals = dict(a_values)
            vals[item_col] = item
            vals[label_col] = "0"
            off = _offset_for_values(vals, parameters=parameters, schema=schema)
            offsets.append(off)
            item_offsets[item] = off
        median = float(np.median(offsets)) if offsets else 0.0
        spread = float(max(offsets) - min(offsets)) if offsets else 0.0
        group_label = "ALL" if not a_cols else " | ".join(str(v) for v in ctx)
        for item in items:
            rows.append({
                "group": group_label,
                **a_values,
                item_col: item,
                "offset": item_offsets[item],
                "offset_centered": item_offsets[item] - median,
                "spread": spread,
            })

    offset_df = pd.DataFrame(rows)
    meta = {
        "context_columns": a_cols,
        "items": [str(i) for i in items],
        "n_contexts": int(len(contexts)),
    }
    return offset_df, meta


def row_offsets(pdf: pd.DataFrame, parameters: dict, schema: dict) -> np.ndarray:
    """逐列 offset 向量（與 ``pdf`` 等長、逐列對齊）。

    Δ 與其 CI 都用這條向量：offset 依 (客群, item) 決定，不是只依 item，所以
    ``paired_bootstrap_delta`` 要走「向量」那條路而不是 ``{item: offset}`` dict。
    """
    item_col = schema["item"]
    label_col = schema["label"]
    a_cols = offset_context_columns(parameters, schema)
    cache: dict[tuple, float] = {}
    out = np.zeros(len(pdf), dtype=np.float64)
    for i, row in enumerate(pdf[[*a_cols, item_col]].itertuples(index=False, name=None)):
        ctx = tuple(row)
        if ctx not in cache:
            vals = dict(zip([*a_cols, item_col], ctx))
            vals[label_col] = "0"
            cache[ctx] = _offset_for_values(vals, parameters=parameters, schema=schema)
        out[i] = cache[ctx]
    return out


def _validate(pdf: pd.DataFrame, parameters: dict, schema: dict) -> list[str]:
    """必要欄位檢查。回傳 offset 的客群欄清單。"""
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
            f"config_shift 輸入缺 offset 客群欄位 {missing_context}。"
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
    """
    sample_pdf, sample_meta = diagnosis_sample
    schema = get_schema(parameters)
    diag = diag_cfg(parameters)
    cfg = diag.get("config_shift", {}) or {}
    ci_cfg = diag.get("ci", {}) or {}
    mp = metric_params(parameters)

    out: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled", True)),
        "score_col_used": SCORE_COL,
        "metric_params": mp,
        "context_columns": [],
        "offset_spread": {},
        "offset_matrix": {},
        "offset_centered": {},
        "baseline_map": None,
        "corrected_map": None,
        "delta": None,
        "delta_ci_low": None,
        "delta_ci_high": None,
        "ci": {"enabled": False, "n_boot": 0, "seed": 0},
        "per_item": [],
        "per_item_sum_note": PER_ITEM_SUM_NOTE,
        "sample": dict(sample_meta or {}),
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

    # 空樣本必須在 build_offset_frame **之前**擋掉：item 清單是從資料推的，
    # 空資料 → 零個 context × 零個 item → offset_df 是一個「連欄位都沒有」的
    # 空 DataFrame，下面的 groupby("group") 會 KeyError。這是良性退化輸入
    # （抽樣沒抽到東西），不是壞輸入，不該炸。
    if len(sample_pdf) == 0:
        out["notes"].append("診斷抽樣為空——offset 矩陣與 Δ 均未計算。")
        return out

    offset_df, offset_meta = build_offset_frame(sample_pdf, parameters, schema)
    # 群內 spread（見模組 docstring 修正 2）：每個客群自己算 max − min。跨客群
    # 的 offset 差不會落在同一個 query 裡，所以全域 max − min 不是排序影響的量。
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
    offs = row_offsets(sample_pdf, parameters, schema)

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

    ci_enabled = bool(ci_cfg.get("enabled", True))
    n_boot = int(ci_cfg.get("n_boot", 200))
    seed = int((diag.get("sample", {}) or {}).get("seed", 42))
    out["ci"] = {"enabled": ci_enabled, "n_boot": n_boot, "seed": seed}
    if ci_enabled:
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
            frame, mp, offs, n_boot=n_boot, seed=seed,
        )
        out["delta_ci_low"] = float(-hi)
        out["delta_ci_high"] = float(-lo)

    # 逐項替換：一次只扣掉一個 item 的 offset。Σ Δ_j ≠ Δ，見 PER_ITEM_SUM_NOTE。
    n_pos_by_item = (
        pd.Series(y, index=sample_pdf.index)
        .groupby(sample_pdf[item_col].astype(str)).sum()
    )
    per_item: list[dict[str, Any]] = []
    for item in sorted(set(items.tolist())):
        mask = items == item
        z_one = z.copy()
        z_one[mask] = z_one[mask] - offs[mask]
        m_one = float(compute_macro_per_item_map(
            groups, items, y, z_one, weights=ht_weights, **mp
        ))
        per_item.append({
            "item": str(item),
            "delta_j": float(m_one - baseline),
            "map_after_only_this_item": m_one,
            "n_pos": int(n_pos_by_item.get(item, 0)),
            "offset_min": float(np.min(offs[mask])) if mask.any() else None,
            "offset_max": float(np.max(offs[mask])) if mask.any() else None,
        })
    per_item.sort(key=lambda r: r["delta_j"], reverse=True)
    out["per_item"] = per_item

    # 樣本規模由實際落地的列推導，覆寫 meta 中同名鍵——讀者看到的規模必須跟
    # 產生這些數字的那批列一致。
    out["sample"].update({
        "n_rows": int(len(sample_pdf)),
        "n_queries": int(len(np.unique(groups))),
        "n_entities": int(clusters.nunique()),
        "n_items": int(len(set(items.tolist()))),
        "n_positive_rows": int(y.sum()),
    })

    logger.info(
        "config_shift: %d queries, %d items, baseline=%.6f corrected=%.6f "
        "delta=%.6f",
        out["sample"]["n_queries"], out["sample"]["n_items"],
        baseline, corrected, out["delta"],
    )
    return out
