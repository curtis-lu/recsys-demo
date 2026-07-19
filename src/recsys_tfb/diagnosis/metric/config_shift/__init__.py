"""config_shift：抽樣／sample weight 的理論 log-odds 偏移對排序的影響。

本輪（Task 2.2）只有計算層。契約要求的 ``NAME`` / ``TITLE`` / ``SCOPE`` /
``render`` 由 Task 2.3 補上——在那之前
``test_every_registered_diagnosis_satisfies_contract`` 會對本模組 RED，
**這是預期的**，不是缺陷。
"""
from __future__ import annotations

from recsys_tfb.diagnosis.metric.config_shift.compute import compute

__all__ = ["compute"]
