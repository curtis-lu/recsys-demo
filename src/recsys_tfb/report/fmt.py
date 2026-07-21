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


def fmt_percent(x: Any) -> str:
    """佔比／比率當百分比讀：[0,1] 量 × 100，1 位小數＋``%``。

    與 ``fmt_auc`` 是同一種底層量（[0,1] 的份額），差別只在**呈現單位**：
    「這個 item 佔全體缺口的 20.8%」比「0.208」直覺，尤其是分子／分母都在
    定義表裡寫清楚的時候。AUC 這種本身就習慣讀小數的量仍用 ``fmt_auc``；
    「share」「rate」這種「一部分佔全部」的量用這個。
    """
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v * 100:.1f}%"


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


def fmt_mean(x: Any) -> str:
    """平均量（每列平均個數等）：2 位小數，**無單位後綴**。

    與 ``fmt_ratio`` 的區別是關鍵：``fmt_ratio`` 的 ``x`` 後綴代表「幾倍」，
    只該用在真正的倍率（lift）。「每個正例列上方平均有幾個負例」這種**平均
    個數**不是倍率，加 ``x`` 會讓讀者誤以為是倍數關係。這裡不加後綴。
    """
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:.2f}"


def fmt_count(x: Any) -> str:
    """計數：千分位、無小數。**只給真正的整數計數**，加權和用 ``fmt_weighted_count``。"""
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{round(v):,}"


def fmt_gain(x: Any) -> str:
    """split gain 量級（大而無界的非負浮點，如 LightGBM 的 split_gain 加總）：
    千分位＋1 位小數。

    與 ``fmt_count`` 的區別是語意而非位數：gain 是連續量、不是計數，保留一位
    小數讓它在視覺上與整數計數分得開（``7,799.0`` vs ``7,799``，同 ``fmt_
    weighted_count`` 的理由）。與 ``fmt_weighted_count`` 格式恰好相同但語意不同
    （一個是 split gain、一個是 inclusion_weight 之和）——本模組按量的語意命名
    格式器，格式偶合不代表可以共用一個名字（同 ``fmt_percent`` vs ``fmt_auc``
    的取捨）。千分位是必要的：gain 常在數萬到數十萬量級，沒有分位很難讀。
    """
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:,.1f}"


def fmt_weighted_count(x: Any) -> str:
    """加權計數（inclusion_weight 之和）：千分位＋1 位小數。

    為什麼不共用 ``fmt_count``：加權和不是整數，捨入到整數會在 ``.5`` 上出事。
    Python 的 ``round`` 是銀行家捨入，於是 61.5 → 62 而 28.5 → 28——同一欄裡兩個
    都是 ``.5`` 的值往相反方向跑，讀者看到的是「這張表的數字不對」。保留一位
    小數就沒有這個問題，而且順帶讓「加權和」與「原始列數」在視覺上分得開
    （``62.0`` vs ``62``）。

    位數選 1 是因為它的用途是給規模感（n_pos_effective 有多少），不是拿來對帳；
    要精確值的人看 JSON。
    """
    v = _to_finite_float(x)
    if v is None:
        return ""
    return f"{v:,.1f}"
