"""report.fmt：語意化格式器——按量的語意決定位數，不按呼叫點決定。"""

import math

import pytest

from recsys_tfb.report.fmt import (
    fmt_ap,
    fmt_auc,
    fmt_count,
    fmt_delta,
    fmt_logodds,
    fmt_mean,
    fmt_percent,
    fmt_ratio,
    fmt_weighted_count,
)

# 每個函式對「壞值」的共同契約：None/NaN/inf/無法轉 float → 空字串，不 raise。
_BAD_VALUES = [None, float("nan"), float("inf"), float("-inf"), "not-a-number", object()]
_ALL_FMTS = [
    fmt_logodds, fmt_auc, fmt_ap, fmt_delta, fmt_ratio, fmt_count,
    fmt_weighted_count, fmt_percent, fmt_mean,
]


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


class TestFmtPercent:
    def test_proportion_becomes_percentage_one_decimal(self):
        assert fmt_percent(0.208) == "20.8%"
        assert fmt_percent(1) == "100.0%"
        assert fmt_percent(0) == "0.0%"

    def test_distinguishes_from_fmt_auc(self):
        # 同一個底層量、不同呈現單位——這正是分開兩個函式的理由
        assert fmt_auc(0.464) == "0.464"
        assert fmt_percent(0.464) == "46.4%"


class TestFmtMean:
    def test_two_decimals_no_suffix(self):
        assert fmt_mean(0.77) == "0.77"
        assert fmt_mean(2.8182) == "2.82"
        assert fmt_mean(0) == "0.00"

    def test_no_x_suffix_unlike_ratio(self):
        # 平均個數不是倍率——不該有 fmt_ratio 的 x 後綴
        assert not fmt_mean(2.82).endswith("x")
        assert fmt_ratio(2.82).endswith("x")


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


class TestFmtWeightedCount:
    """加權計數（HT 權重之和）：不是整數，所以不能借用 ``fmt_count``。"""

    def test_keeps_one_decimal(self):
        assert fmt_weighted_count(61.5) == "61.5"
        assert fmt_weighted_count(28.5) == "28.5"

    def test_thousands_separator_like_fmt_count(self):
        assert fmt_weighted_count(4_400_000.25) == "4,400,000.2"

    def test_whole_numbers_still_show_the_decimal(self):
        """``62`` 與 ``62.0`` 在報表上要長得一樣——不然讀者會以為前者是精確整數。"""
        assert fmt_weighted_count(62) == "62.0"

    def test_does_not_round_two_halves_in_opposite_directions(self):
        """這個函式存在的理由：``fmt_count`` 對 61.5→62、28.5→28（銀行家捨入
        兩個 .5 往不同方向跑），並排在同一欄看起來像資料壞了。加權和保留小數
        就沒有這個問題。"""
        assert fmt_count(61.5) != fmt_count(28.5) + ""  # 前提：兩者都是 .5
        assert fmt_weighted_count(61.5).endswith(".5")
        assert fmt_weighted_count(28.5).endswith(".5")


def test_module_uses_math_isfinite_consistently():
    """反例（本次重構要消滅的）：6 個一次性診斷腳本各有一份 fmt_num，其中一份
    用 math.isfinite 其餘用 np.isfinite——各自維護的結果是漂移。這裡只驗證
    行為契約（inf/nan 一律回空字串），不 import 私有實作細節。"""
    for fn in _ALL_FMTS:
        assert fn(math.inf) == ""
        assert fn(math.nan) == ""
