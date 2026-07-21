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
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.suppression._compute import compute
from recsys_tfb.diagnosis.metric.suppression._render import render
from recsys_tfb.report import ScopeNote

NAME = "suppression"
TITLE = "壓制帳本與交叉購買（哪些負例排在正例前面、誰跟誰常一起買）"

#: 這項診斷量什麼、算在哪批列上、**不能**推論什麼。
#:
#: ``blind_to`` 三條逐字對應三條誠實條款（見任務規格與 ``_compute.py``
#: 模組 docstring 的同一組警語）：分攤比例是會計慣例不是因果、交叉購買算在
#: 抽樣（分層）上而非母體、lift=1 只代表樣本內的統計獨立不代表商業無關。
#:
#: ``sampling`` 刻意留空——理由與其餘診斷相同：五項診斷共用同一份
#: ``diagnosis_sample``，執行期才知道的抽樣描述由組裝層統一填入。
SCOPE = ScopeNote(
    measures=(
        "同一個 query 內，label=0 的 item 排在 label=1 的 item 之前的壓制"
        "關係（AP 缺口如何依比例分攤給排在它上面的每個負例，彙總成壓制"
        "帳本），以及 item 對之間的交叉購買 lift（p_k_given_j 相對 k 的"
        "基礎購買率）。壓制事件的計數與 metric k 無關（只要負例排在正例"
        "之上就算）；但 AP 缺口與其分攤是在 top-k 之內衡量的——k 由設定"
        "決定，值印在頁面上。"
    ),
    population=(
        "壓制帳本與 cross_purchase 都算在同一份診斷抽樣（與其餘診斷共用）"
        "上；cross_purchase 的母體是這份抽樣本身，不是 label_table 全量。"
    ),
    blind_to=(
        # 措辭注意：這裡原本寫「依 severity 比例分攤」——`raw_severity` 是
        # 內部變數名，洩到使用者面就變成看不懂的英文；而且 severity 正好是
        # 三條鐵則裡明令不得出現的判定用語，同一個字在頁面上出現會讓讀者
        # 以為系統在評級。改成白話描述那個比例本身是什麼。
        "AP 缺口的分攤比例是一種會計慣例：把某個正例列的缺口，依「各個排"
        "在它上方的負例各自造成多少名次損失」的比例切給它們。這是分帳，"
        "不是因果——它不代表『拿掉這個壓制者就會賺回這麼多』。",
        "共買統計算的是同一份診斷抽樣上、同一批 query 單位的實際標籤共"
        "現，與模型無關；它不解釋模型為什麼這樣排。**當該抽樣有做分層下採"
        "樣時**（規模大時會，本次的實際抽樣範圍見頁首「抽樣設計」），樣本"
        "內的共買頻率不是母體共買頻率的無偏估計。",
        "lift = 1 代表在這份樣本上兩個 item 的購買近似獨立，不代表商業上"
        "無關。",
    ),
    reference_points=(),
)

__all__ = ["NAME", "TITLE", "SCOPE", "compute", "render"]
