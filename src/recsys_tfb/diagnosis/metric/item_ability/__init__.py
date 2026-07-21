"""item_ability：模型能不能在同一個 query 內分辨誰會買哪個 item。

實作放在私有模組 ``_compute``／``_render``（與 ``config_shift`` 同慣例）。
**不要**把它們命名成 ``compute.py``／``render.py``：契約要求本套件匯出叫
``compute``／``render`` 的**函式**，同名子模組會被 ``from .compute import
compute`` 直接遮蔽掉，``pkg.compute`` 從此指向函式而不是模組——而
``check_module`` 走 ``getattr`` 剛好拿得到那個函式，所以契約測試**抓不到**
這個遮蔽（完整理由見 ``config_shift/__init__.py`` 的同一段說明）。

``SCOPE`` 定義在這裡而不是 ``_render`` 裡，理由與 ``config_shift`` 相同：它
屬於「這項診斷是什麼」而不是「怎麼畫」。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.item_ability._compute import compute
from recsys_tfb.diagnosis.metric.item_ability._render import render
from recsys_tfb.report import ScopeNote

NAME = "item_ability"
TITLE = "item 排序能力（raw vs query-centered AUC）"

#: 這項診斷量什麼、算在哪批列上、**不能**推論什麼。
#:
#: ``blind_to`` 前三條逐字對應「這項診斷在回答什麼」的三個誠實條款
#: （見任務規格與 ``_compute.py`` 模組 docstring 的同一組警語）：AUC 是
#: macro mAP 的 proxy 而非分解、母體限定在有正例的 query 因此不能跨模型／
#: 跨資料集比較、AUC 高不保證 mAP 高。
#:
#: ``sampling`` 刻意留空——理由與 ``config_shift.SCOPE`` 相同：五項診斷共用
#: 同一份 ``diagnosis_sample``，執行期才知道的抽樣描述由組裝層統一填入。
SCOPE = ScopeNote(
    measures=(
        "同一個 item 內，正例列與負例列的加權 AUC——分別在原始 logit 分數"
        "（raw_within_item_auc）與扣掉各自 query 平均後的分數"
        "（query_centered_auc）上各算一次。兩者之差 auc_gap_raw_minus_centered"
        "（＝raw − centered）量的是客戶整體分數水準（同一 query 內所有候選的"
        "平均分數）對 raw AUC 的貢獻：正值把 raw 撐高、負值拉低。"
    ),
    population="診斷抽樣：只含有正例的 query（與其餘診斷共用同一份抽樣）。",
    blind_to=(
        "item j 的正例列與負例列分屬不同 query，而 macro mAP 從頭到尾沒做過"
        "跨 query 的分數比較——這個 AUC 是 proxy，不是指標的分解。",
        "母體限定在有正例的 query，所以這個數字不能跟任何外部引用的 AUC 比"
        "較，它會系統性地低於全母體 AUC。",
        "AUC 高不代表 mAP 高：兩者對名次的加權方式不同。",
    ),
    # reference_points 刻意留空：判讀方式寫在它描述的那張圖旁邊（見
    # config_shift.SCOPE 的同一段理由），不放在頁首。
    reference_points=(),
)

__all__ = ["NAME", "TITLE", "SCOPE", "compute", "render"]
