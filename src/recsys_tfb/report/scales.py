"""色階：只編碼資料的大小或正負，不編碼好壞。

單向量（計數、份額）用 ``sequential_scale``——單一色相由淺到深，越深只代表
「數值越大」。有號量（Δ、lift−1、AUC 差）用 ``diverging_scale``——兩端不同
色相、中點是中性色，中點的位置由 ``center``／``lo``／``hi`` 算出，不是寫死
在正中間。

**本模組刻意不提供任何 good/bad 配色**——「這個數字是好是壞」是讀者的判斷，
不是報表的。這是設計約定，**不是程式碼擋得住的事**：誰要在別處畫紅綠燈、
直接寫死一組 good/bad 色碼，這裡的任何機制都攔不住。曾考慮寫一個測試斷言
「某些函式名（例如 ``good_bad_scale``）不存在於本模組」，但那種測試只防得住
「有沒有人在這個檔案裡加同名函式」，防不住它宣稱要防的事（有人在下游用兩個
寫死的 hex 色碼手刻紅綠燈），卻會讓人誤以為防住了——比沒有這個測試更危險。
所以改成這段註解＋code review：新增色階／新報表要引用色碼時，人工檢查有沒有
偷渡好壞語意，而不是指望自動化測試守住這件事。
"""
from __future__ import annotations

# 單一色相（indigo/teal 家族），淺到深。
_SEQ_LIGHT = "#f0fdfa"
_SEQ_MID = "#2dd4bf"
_SEQ_DARK = "#134e4a"

# 兩端不同色相（blue / orange）＋中性色——刻意避開 red/green，避免被讀成
# 「壞/好」而不是單純的「負/正」。
_DIV_LOW = "#2563eb"
_DIV_NEUTRAL = "#e5e5e5"
_DIV_HIGH = "#ea580c"


def sequential_scale() -> list[tuple[float, str]]:
    """單向色階：單一色相由淺到深，位置 0.0 到 1.0，至少 3 個停駐點。"""
    return [
        (0.0, _SEQ_LIGHT),
        (0.5, _SEQ_MID),
        (1.0, _SEQ_DARK),
    ]


def diverging_scale(
    center: float = 0.0,
    lo: float | None = None,
    hi: float | None = None,
) -> list[tuple[float, str]]:
    """發散色階：兩端不同色相、中點中性色，中點位置由 ``center``/``lo``/``hi`` 算出。

    ``lo``／``hi`` 都給定時，把 ``center`` 正規化到 ``[0, 1]`` 的位置
    ``(center - lo) / (hi - lo)``，夾在 ``[0, 1]`` 內；``hi <= lo`` 視為無效
    區間，raise ``ValueError``。未給定 ``lo``/``hi`` 時假設資料對稱於
    ``center``，中點固定在 0.5。

    Raises:
        ValueError: ``hi <= lo``（訊息含兩個值，方便直接看出哪個區間錯了）。
    """
    if lo is not None and hi is not None:
        if hi <= lo:
            raise ValueError(
                f"diverging_scale: hi ({hi!r}) must be greater than lo ({lo!r})"
            )
        mid = (center - lo) / (hi - lo)
        mid = max(0.0, min(1.0, mid))
    else:
        mid = 0.5

    # 中點若被夾到剛好落在 0.0 或 1.0，保留原本三段結構會產生重複位置
    # （違反「位置序列必須遞增」），改成兩段、中性色頂替該端點的顏色。
    if mid <= 0.0:
        return [(0.0, _DIV_NEUTRAL), (1.0, _DIV_HIGH)]
    if mid >= 1.0:
        return [(0.0, _DIV_LOW), (1.0, _DIV_NEUTRAL)]
    return [(0.0, _DIV_LOW), (mid, _DIV_NEUTRAL), (1.0, _DIV_HIGH)]
