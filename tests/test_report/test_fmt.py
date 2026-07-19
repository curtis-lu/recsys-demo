"""report.fmt：語意化格式器——按量的語意決定位數，不按呼叫點決定。"""

import math

import pytest

from recsys_tfb.report.fmt import (
    fmt_ap,
    fmt_auc,
    fmt_count,
    fmt_delta,
    fmt_logodds,
    fmt_ratio,
)

# 每個函式對「壞值」的共同契約：None/NaN/inf/無法轉 float → 空字串，不 raise。
_BAD_VALUES = [None, float("nan"), float("inf"), float("-inf"), "not-a-number", object()]
_ALL_FMTS = [fmt_logodds, fmt_auc, fmt_ap, fmt_delta, fmt_ratio, fmt_count]


@pytest.mark.parametrize("fn", _ALL_FMTS)
@pytest.mark.parametrize("bad", _BAD_VALUES)
def test_bad_values_return_empty_string(fn, bad):
    assert fn(bad) == ""


class TestFmtLogodds:
    def test_positive_gets_plus_sign_three_decimals(self):
        assert fmt_logodds(6.9078) == "+6.908"

    def test_negative_gets_minus_sign_three_decimals(self):
        assert fmt_logodds(-0.5) == "-0.500"

    def test_zero_has_no_sign(self):
        assert fmt_logodds(0) == "0.000"
        assert fmt_logodds(0.0) == "0.000"

    def test_near_zero_rounds_to_unsigned_zero(self):
        # 避免 "-0.000" 這種帶負號卻視覺為零的產物
        assert fmt_logodds(-0.00001) == "0.000"


class TestFmtAuc:
    def test_three_decimals_no_forced_sign(self):
        assert fmt_auc(0.5) == "0.500"
        assert fmt_auc(1) == "1.000"
        assert fmt_auc(0) == "0.000"


class TestFmtAp:
    def test_four_decimals(self):
        assert fmt_ap(0.12345) == "0.1235"  # 四捨五入到第四位
        assert fmt_ap(1.0) == "1.0000"


class TestFmtDelta:
    def test_always_signed_four_decimals(self):
        assert fmt_delta(0.0123) == "+0.0123"
        assert fmt_delta(-0.0123) == "-0.0123"

    def test_matches_fmt_ap_decimal_places(self):
        assert fmt_delta(0.5)[1:] == fmt_ap(0.5)


class TestFmtRatio:
    def test_two_decimals_with_x_suffix(self):
        assert fmt_ratio(1.5) == "1.50x"
        assert fmt_ratio(1.0) == "1.00x"


class TestFmtCount:
    def test_thousands_separator_no_decimals(self):
        assert fmt_count(4400000) == "4,400,000"
        assert fmt_count(4_400_000.0) == "4,400,000"
        assert fmt_count(0) == "0"


def test_module_uses_math_isfinite_consistently():
    """反例（本次重構要消滅的）：6 個一次性診斷腳本各有一份 fmt_num，其中一份
    用 math.isfinite 其餘用 np.isfinite——各自維護的結果是漂移。這裡只驗證
    行為契約（inf/nan 一律回空字串），不 import 私有實作細節。"""
    for fn in _ALL_FMTS:
        assert fn(math.inf) == ""
        assert fn(math.nan) == ""
