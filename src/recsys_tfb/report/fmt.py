"""語意化格式器：按「量的語意」決定格式，不按呼叫點決定。

模組宣告的是「這一欄是 log-odds 量」（``fmt_logodds``）、「這一欄是計數」
（``fmt_count``）……而不是「這一欄要 3 位小數」。同一種量在所有報表裡長得
一樣；要改全域顯示慣例（例如 AP 要不要多顯示一位小數），只需要動這一個
檔案，不必去每個報表各自的呼叫點裡找。

反例（本次重構要消滅的）：舊的 6 個一次性診斷腳本各有一份 ``fmt_num``，
其中一份用 ``math.isfinite`` 其餘用 ``np.isfinite``——各自維護的結果是
漂移。本模組統一用 ``math.isfinite`` 判斷有限性，只有這一份實作。

六個函式共同的壞值契約：``None``／``NaN``／``inf``／無法轉 ``float`` 的輸入
一律回空字串 ``""``，不 raise、不回傳字面 ``"nan"``——報表裡的空格比一個
會被誤讀成數字的字串安全。
"""
from __future__ import annotations

import math
from typing import Any


def _to_finite_float(x: Any) -> float | None:
    """轉成有限 float；轉不了或非有限（NaN/inf）一律回 ``None``。"""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return v


def fmt_logodds(x: Any) -> str:
    """log-odds 量（offset、位移 δ）：帶正負號 3 位小數；0 顯示不帶號。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    s = f"{v:+.3f}"
    return "0.000" if s in ("+0.000", "-0.000") else s


def fmt_auc(x: Any) -> str:
    """AUC／份額等 [0,1] 量：3 位小數，不強制正負號。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:.3f}"


def fmt_ap(x: Any) -> str:
    """AP／mAP：4 位小數（有意義差異常在第 3–4 位）。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:.4f}"


def fmt_delta(x: Any) -> str:
    """指標差 Δ：永遠帶正負號、4 位小數（對齊 ``fmt_ap`` 的位數）。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:+.4f}"


def fmt_ratio(x: Any) -> str:
    """倍率（lift、max/min）：2 位小數＋``x`` 後綴。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:.2f}x"


def fmt_count(x: Any) -> str:
    """計數：千分位、無小數。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{round(v):,}"
