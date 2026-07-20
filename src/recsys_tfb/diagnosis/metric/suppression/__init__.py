"""suppression：同一個 query 裡，哪些負例 item 排在正例 item 前面（壓制帳
本），以及交叉購買 lift（誰跟誰常一起買）。

實作放在私有模組 ``_compute``／``_render``（與 ``config_shift``／
``item_ability`` 同慣例）。**不要**把它們命名成 ``compute.py``／
``render.py``：契約要求本套件匯出叫 ``compute``／``render`` 的**函式**，
同名子模組會被 ``from .compute import compute`` 直接遮蔽掉，``pkg.compute``
從此指向函式而不是模組——而 ``check_module`` 走 ``getattr`` 剛好拿得到那個
函式，所以契約測試**抓不到**這個遮蔽（完整理由見
``config_shift/__init__.py`` 的同一段說明）。

``SCOPE`` 定義在這裡而不是 ``_render`` 裡，理由與其餘診斷相同：它屬於「這項
診斷是什麼」而不是「怎麼畫」。

⚠ **本檔案是 Task 5.2 的中間狀態**：``render`` 尚未實作（Task 5.3 的事），
``SCOPE`` 是暫時版本。``contract.DIAGNOSES`` 這個 task 不加 ``suppression``
（見 Task 5.4／5.3），契約測試不會掃到這個套件，這個中間狀態是刻意的。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.suppression._compute import compute
from recsys_tfb.report import ScopeNote

NAME = "suppression"
TITLE = "壓制帳本與交叉購買（哪些負例排在正例前面、誰跟誰常一起買）"

#: 這項診斷量什麼、算在哪批列上、**不能**推論什麼。
#:
#: 暫時版本（Task 5.3 定案）：``blind_to`` 先放本 task 已知的兩條誠實條款，
#: 不得為空（契約要求）。
SCOPE = ScopeNote(
    measures=(
        "同一個 query 內，label=0 的 item 排在 label=1 的 item 之前的壓制"
        "關係（AP 缺口如何分攤給壓制者），以及 item 對之間的交叉購買 lift"
        "（P(k|j) 除以 k 的基礎購買率）。"
    ),
    population="診斷抽樣：與其餘診斷共用同一份 diagnosis_sample。",
    blind_to=(
        "壓制帳本只看同一 query 內的名次順序，不解釋為什麼模型把兩者排成"
        "這個順序（特徵層面的原因不在本診斷範圍）。",
        "cross_purchase_stats 算在抽樣後的 diagnosis_sample 上，不是"
        "label_table 全量——樣本內共買頻率不是母體共買頻率的無偏估計"
        "（分層抽樣的代價；完整說明見 _compute.py 模組 docstring）。",
    ),
    reference_points=(),
)

__all__ = ["NAME", "TITLE", "SCOPE", "compute"]
