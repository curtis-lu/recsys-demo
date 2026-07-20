"""item_ability：模型能不能在同一個 query 內分辨誰會買哪個 item。

實作放在私有模組 ``_compute``／``_render``（與 ``config_shift`` 同慣例）。
**不要**把它們命名成 ``compute.py``／``render.py``：契約要求本套件匯出叫
``compute``／``render`` 的**函式**，同名子模組會被 ``from .compute import
compute`` 直接遮蔽掉，``pkg.compute`` 從此指向函式而不是模組——而
``check_module`` 走 ``getattr`` 剛好拿得到那個函式，所以契約測試**抓不到**
這個遮蔽（完整理由見 ``config_shift/__init__.py`` 的同一段說明）。

本檔目前只 re-export ``compute``——``NAME``／``TITLE``／``SCOPE``／``render``
是下一個 task（呈現層＋接線）的範圍。**本 task 刻意不把 item_ability 加進
``contract.DIAGNOSES``**：契約要求五個符號齊全，少了 ``SCOPE``／``render``
會讓契約測試轉紅。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.item_ability._compute import compute

__all__ = ["compute"]
