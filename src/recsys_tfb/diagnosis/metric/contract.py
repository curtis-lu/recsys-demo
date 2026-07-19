"""診斷契約：報表層與各項診斷之間唯一的約定。

**契約存在的理由**：報表層不需要認識任何單一診斷。``report_builder`` 只走
:data:`DIAGNOSES`，對每個名字 import 模組、讀 :data:`_REQUIRED` 那五個符號，
然後把 ``compute`` 的結果餵給 ``render``。因此新增第六項診斷 ＝ 新增一個子
套件 ＋ 在 :data:`DIAGNOSES` 補一行，``report_builder`` **零改動**。

五個必要符號各自的角色：

* ``NAME``    —— 模組自報的名字，與 :data:`DIAGNOSES` 裡的字串一致。
* ``TITLE``   —— 報表頁標題（繁體中文）。
* ``SCOPE``   —— ``report.types.ScopeNote``：這項診斷量什麼、算在哪批列上、
  **不能**推論什麼。這是「每個數字自帶說明」的執行點，所以它是**必要**符號
  而不是可選的——一項說不出自己看不見什麼的診斷，不該進報表。
* ``compute`` —— 純計算，回傳 JSON-safe dict。不碰呈現。
* ``render``  —— 把 ``compute`` 的 dict 轉成 ``report.types.Page``。不做計算。

刻意**不做**的兩件事，以及理由：

1. **不用 dataclass 包 ``(name, order)``**：order 可由 tuple 順序推導，多一個
   型別只是多一個要學的概念。
2. **不做 ``slug_for()`` 函式**：``f"{i+1:02d}-{name.replace('_','-')}"`` 只有
   一個呼叫點，行內寫掉即可——為單一呼叫點造一個公開函式，讀的人得多跳一層
   才知道檔名長什麼樣。
"""
from __future__ import annotations

import inspect

from recsys_tfb.report.figures import MAX_FIGURE_POINTS

#: 每個診斷模組必須提供的符號。順序即錯誤訊息中的列出順序。
_REQUIRED = ("NAME", "TITLE", "SCOPE", "compute", "render")

#: 兩個函式的參數名與順序。**檢查名字而不只是個數**：``report_builder`` 之後
#: 若改用關鍵字呼叫，一個名字打錯的 ``render(payload, parameters)`` 只有在真的
#: 跑報表時才會炸，而那是 pipeline 尾端最貴的位置。
#:
#: 為什麼要有這條檢查：這個形狀是第一項診斷（Task 2.2）默默立下的，沒有寫在
#: 任何地方。在補上這條之前，後面每一項診斷都可以寫成別的簽章而契約測試照樣
#: 全綠——測試綠但形狀不一致，正是 report_builder「零改動」宣稱破功的方式。
_SIGNATURES = {
    "compute": ("diagnosis_sample", "parameters"),
    "render": ("result", "parameters"),
}

#: registry：順序即閱讀順序，也決定 HTML 檔名的數字前綴。
#: 隨計畫逐步補齊（本計畫只有第一項）。
DIAGNOSES: tuple[str, ...] = ("config_shift",)

__all__ = ["DIAGNOSES", "MAX_FIGURE_POINTS", "check_module"]


def check_module(mod) -> None:
    """檢查一個診斷模組是否滿足契約：五個符號都在，且兩個函式的簽章對得上。

    兩種失敗用兩種例外，因為要做的事不同：缺符號 → ``AttributeError``（去補一個
    符號）；簽章不對 → ``TypeError``（去改參數名）。

    collect-all 只用在缺符號那一段：一次列出**所有**缺的符號，補的人才不必補
    一個、跑一次、再發現還缺下一個。簽章檢查排在後面，因為它得先拿得到函式。
    """
    missing = [symbol for symbol in _REQUIRED if not hasattr(mod, symbol)]
    name = getattr(mod, "__name__", repr(mod))
    if missing:
        raise AttributeError(
            f"diagnosis module {name!r} does not satisfy the diagnosis contract: "
            f"missing {', '.join(missing)}. Every diagnosis must define "
            f"{', '.join(_REQUIRED)} — see recsys_tfb.diagnosis.metric.contract "
            "for what each symbol is for."
        )

    for symbol, expected in _SIGNATURES.items():
        func = getattr(mod, symbol)
        try:
            actual = tuple(inspect.signature(func).parameters)
        except (TypeError, ValueError):  # pragma: no cover - C 實作取不到簽章
            continue
        if actual != expected:
            raise TypeError(
                f"diagnosis module {name!r} does not satisfy the diagnosis "
                f"contract: {symbol} must take exactly ({', '.join(expected)}), "
                f"got ({', '.join(actual) or '無參數'}). report_builder calls "
                "every diagnosis through the same two-argument shape."
            )
