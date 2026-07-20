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
* ``render``  —— 把 ``compute`` 的 dict 轉成 ``tuple[report.types.ReportSection,
  ...]``。不做計算。停用時回**空 tuple**（不是 ``None``）：組裝層一律以
  「序列為空 ＝ 這頁不存在」判斷，回 ``None`` 與回 ``()`` 兩種寫法並存的話，
  之後新增的診斷有一半會挑錯一種。**回多個 section 而不是一個**是版面定案：
  每張圖／每張表自己一個 section，各自帶 ``title``／``formula``／``bullets``，
  說明才會落在它描述的那張圖旁邊。簽章不變，所以 :data:`_SIGNATURES` 不受影響。

**``DIAGNOSES`` 不是使用者面的開關。** 它宣告的是「這項診斷在程式碼裡存在」
——決定 catalog 鍵、頁面編號、以及 ``render_diagnosis_pages`` 會去找哪些檔案。
使用者要關掉一項診斷，動的是 ``evaluation.diagnosis.<name>.enabled``：那條路
會讓 ``compute`` 寫一份 ``{"enabled": False}`` stub，``render`` 讀到後回空
tuple，於是那一頁不存在。

兩者的差別在**產物**：``enabled: false`` 仍會落地一份 stub JSON（看得出來
「這次刻意沒算」）；從 ``DIAGNOSES`` 移除則連檔案都不會有（看起來像「這個
版本還沒有這項診斷」）。前者是操作，後者是改版本。

刻意**不做**的兩件事，以及理由：

1. **不用 dataclass 包 ``(name, order)``**：order 可由 tuple 順序推導，多一個
   型別只是多一個要學的概念。
2. **不做 ``slug_for()`` 函式**：``f"{i+1:02d}-{name.replace('_','-')}"`` 只有
   一個呼叫點，行內寫掉即可——為單一呼叫點造一個公開函式，讀的人得多跳一層
   才知道檔名長什麼樣。
"""
from __future__ import annotations

import inspect

# **本模組刻意只 import stdlib，不要在這裡加 recsys_tfb 的 import。**
#
# 理由：``core/consistency.py`` 需要 :data:`DIAGNOSES` 來驗每項診斷的 enabled
# 旗標，而 ``validate_config_consistency`` 在**每個** pipeline 的 CLI entry 都會
# 跑（dataset／training／inference／evaluation）。這裡曾經 re-export
# ``report.figures.MAX_FIGURE_POINTS``，那條 import 會連帶把 plotly 拉進來——
# 實測讓 import 本模組從 ~0 變成 374ms，四條 pipeline 每次啟動都白付，而
# 其中三條根本不畫圖。該 re-export 當時沒有任何 production 消費者（只有它
# 自己的測試在斷言它存在），已移除。
#
# 診斷模組要用 ``MAX_FIGURE_POINTS`` 請直接
# ``from recsys_tfb.report.figures import MAX_FIGURE_POINTS``——那裡是實際執行
# ``assert_within_budget`` 的地方，多一層 re-export 只會讓兩個常數有機會漂移。

#: 每個診斷模組必須提供的符號。順序即錯誤訊息中的列出順序。
_REQUIRED = ("NAME", "TITLE", "SCOPE", "compute", "render")

#: ``render`` 的參數名與順序，對所有診斷都固定——``compute`` 的結果 dict 形狀
#: 各異，但 ``render(result, parameters)`` 這個殼不變，``report_builder`` 靠它
#: 才能用同一套呼叫方式對待每一項診斷。**檢查名字而不只是個數**：之後若改用
#: 關鍵字呼叫，一個名字打錯的 ``render(payload, parameters)`` 只有在真的跑報表
#: 時才會炸，而那是 pipeline 尾端最貴的位置。
_SIGNATURES = {
    "render": ("result", "parameters"),
}

#: 一項診斷的 node inputs（也是 ``compute`` 的參數名，見
#: :func:`compute_params_for`）。多數診斷吃共用抽樣，所以有這個預設值；宣告了
#: ``INPUTS`` 的模組（例如讀 ``gain_ledger`` 的 ``model_capacity``）覆寫它。
#:
#: **這是 node inputs 與 compute 簽章的單一真實來源**：``make_diagnosis_node``
#: 與 ``pipeline.py`` 的 registry 迴圈都呼叫 :func:`inputs_for`，不是各自寫一份
#: ``["diagnosis_sample", "parameters"]``。改之前（Plan 2 Task 4.0 之前）兩處
#: 各寫一份、靠人對；現在寫錯 ``compute`` 簽章會被 :func:`check_module` 擋下，
#: 結構上不可能兩邊不一致。
DEFAULT_INPUTS: tuple[str, ...] = ("diagnosis_sample", "parameters")

#: registry：順序即閱讀順序，也決定 HTML 檔名的數字前綴。
#: 隨計畫逐步補齊（Plan 2 加入第二項 ``item_ability``、第三項
#: ``model_capacity``；Plan 3 加入第四項 ``suppression``）。
DIAGNOSES: tuple[str, ...] = (
    "config_shift", "item_ability", "model_capacity", "suppression",
)

__all__ = [
    "DEFAULT_INPUTS", "DIAGNOSES", "check_module", "compute_params_for",
    "inputs_for",
]


def inputs_for(mod) -> tuple[str, ...]:
    """這項診斷的 node inputs（catalog 鍵）。

    模組沒宣告 ``INPUTS`` 就用 :data:`DEFAULT_INPUTS`（共用抽樣）；宣告了就
    原樣採用——不做任何驗證或轉換，打錯字或漏掉 ``parameters`` 由 §3 的兩條
    不變量測試守住，這裡只負責「讀」。
    """
    return tuple(getattr(mod, "INPUTS", DEFAULT_INPUTS))


def compute_params_for(mod) -> tuple[str, ...]:
    """由 :func:`inputs_for` 導出的 ``compute`` 參數名。

    catalog 鍵去掉 ``evaluation_`` 前綴即參數名（``evaluation_item_ability``
    → ``item_ability``）——與 ``generate_report`` 的位置對齊檢查同一套慣例。
    沒有 ``evaluation_`` 前綴的鍵（``diagnosis_sample``／``gain_ledger``／
    ``parameters``）原樣通過。
    """
    prefix = "evaluation_"
    return tuple(
        name[len(prefix):] if name.startswith(prefix) else name
        for name in inputs_for(mod)
    )


def check_module(mod) -> None:
    """檢查一個診斷模組是否滿足契約：五個符號都在，且兩個函式的簽章對得上。

    兩種失敗用兩種例外，因為要做的事不同：缺符號 → ``AttributeError``（去補一個
    符號）；簽章不對 → ``TypeError``（去改參數名）。

    collect-all 只用在缺符號那一段：一次列出**所有**缺的符號，補的人才不必補
    一個、跑一次、再發現還缺下一個。簽章檢查排在後面，因為它得先拿得到函式。

    ``compute`` 的期望簽章不是寫死的常數，是 :func:`compute_params_for` 從
    這個模組自己的 ``INPUTS``（或預設值）導出——宣告了 ``INPUTS`` 卻沒跟著改
    ``compute`` 參數名，這裡就是唯一擋得住的地方。
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

    expected_by_symbol = {"compute": compute_params_for(mod), **_SIGNATURES}
    for symbol, expected in expected_by_symbol.items():
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
