"""model_capacity：模型的 split gain 花在 item 身分上，還是 context 特徵上。

實作放在私有模組 ``_compute``／``_render``（與 ``config_shift``／
``item_ability`` 同慣例）。**不要**把它們命名成 ``compute.py``／``render.py``：
契約要求本套件匯出叫 ``compute``／``render`` 的**函式**，同名子模組會被
``from .compute import compute`` 直接遮蔽掉（完整理由見
``config_shift/__init__.py`` 的同一段說明）。

``SCOPE``／``INPUTS`` 定義在這裡而不是 ``_compute``／``_render`` 裡，理由與
``config_shift``／``item_ability`` 相同：它們屬於「這項診斷是什麼、吃什麼」
而不是「怎麼算」或「怎麼畫」。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.model_capacity._compute import compute
from recsys_tfb.diagnosis.metric.model_capacity._render import render
from recsys_tfb.report import ScopeNote

NAME = "model_capacity"
TITLE = "模型容量分配（Item Prior vs Post-Item Context）"

#: 這項診斷**不吃共用抽樣**——它只讀訓練側的 gain_ledger，外加同一次執行
#: 的 item_ability 結果。node inputs／compute 簽章的單一真實來源見
#: ``diagnosis.metric.contract``；``parameters`` 必須放最後（§3 不變量）。
INPUTS: tuple[str, ...] = ("gain_ledger", "evaluation_item_ability", "parameters")

#: 這項診斷量什麼、算在哪批列上、**不能**推論什麼。
#:
#: ``blind_to`` 前三條逐字對應任務規格的三個誠實條款：gain 是訓練期的量、
#: 不是評測期的貢獻；「未分配」是 item 切點前的切點，不是誤差；這一項不吃
#: 診斷抽樣，樣本規模與其餘四項無關。
SCOPE = ScopeNote(
    measures=(
        "全模型 LightGBM booster 的 split gain，依切點種類分成三份：item-id "
        "切點本身（Item Prior）、item 切點之後的 context 切點（Post-Item "
        "Context，逐 item 分配）、以及兩者之外的殘餘（未分配）。"
    ),
    population="訓練後的 LightGBM booster 全部樹的全部切點——與診斷抽樣、評測資料無關。",
    blind_to=(
        "Gain 是訓練期的分裂增益，不是評測期的貢獻——gain 高不代表在這份"
        "評估資料上排得好。",
        "未分配（Pre-Item）那塊是 item 分裂之前的分裂，無法歸給任何單一"
        "item；它不是誤差。",
        "這一項不碰評測資料，所以它跟其他各項診斷的抽樣規模無關，也不受診斷"
        "抽樣影響。",
    ),
    # reference_points 刻意留空：判讀方式寫在它描述的那張圖旁邊（見
    # config_shift.SCOPE 的同一段理由），不放在頁首。
    reference_points=(),
)

__all__ = ["INPUTS", "NAME", "TITLE", "SCOPE", "compute", "render"]
