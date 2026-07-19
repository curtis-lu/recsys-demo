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

from recsys_tfb.report.figures import MAX_FIGURE_POINTS

#: 每個診斷模組必須提供的符號。順序即錯誤訊息中的列出順序。
_REQUIRED = ("NAME", "TITLE", "SCOPE", "compute", "render")

#: registry：順序即閱讀順序，也決定 HTML 檔名的數字前綴。
#: 隨計畫逐步補齊（本計畫只有第一項）。
DIAGNOSES: tuple[str, ...] = ("config_shift",)

__all__ = ["DIAGNOSES", "MAX_FIGURE_POINTS", "check_module"]


def check_module(mod) -> None:
    """缺任何必要符號就 raise ``AttributeError``（訊息含缺的符號名）。

    collect-all：一次列出**所有**缺的符號，不是遇到第一個就 raise——補的人
    才不必補一個、跑一次、再發現還缺下一個。
    """
    missing = [symbol for symbol in _REQUIRED if not hasattr(mod, symbol)]
    if not missing:
        return
    name = getattr(mod, "__name__", repr(mod))
    raise AttributeError(
        f"diagnosis module {name!r} does not satisfy the diagnosis contract: "
        f"missing {', '.join(missing)}. Every diagnosis must define "
        f"{', '.join(_REQUIRED)} — see recsys_tfb.diagnosis.metric.contract "
        "for what each symbol is for."
    )
