"""config_shift：抽樣／sample weight 的理論 log-odds 偏移對排序的影響。

實作放在私有模組 ``_compute``（與 repo 既有的 ``_common.py``／``_spark.py``
同慣例）。**不要**把它命名成 ``compute.py``：契約要求本套件匯出一個叫
``compute`` 的**函式**，同名子模組會被 ``from .compute import compute`` 直接
遮蔽掉，``pkg.compute`` 從此指向函式而不是模組——而 ``check_module`` 走
``getattr`` 剛好拿得到那個函式，所以契約測試**抓不到**這個遮蔽。``render``
之後同理。

本輪（Task 2.2）只有計算層。契約要求的 ``NAME`` / ``TITLE`` / ``SCOPE`` /
``render`` 由 Task 2.3 補上——在那之前
``test_every_registered_diagnosis_satisfies_contract`` 會對本模組 RED，
**這是預期的**，不是缺陷。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.config_shift._compute import compute

__all__ = ["compute"]
