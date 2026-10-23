"""Tests for scripts/profile_feature_table.py (basic checks, drift and extreme values, correlation).

The judgement rules are tested on hand-built stats (no Spark): each problem type
gets one column that is flagged and one that only just is not. The report is a
single page; its tests read the HTML the way the page's JS does (ids, data-*
attributes). The Spark side gets its own tests for the empty-value cleaning
(NaN, ±Infinity, empty string, all-empty periods) and the batching.
"""

import json
import math

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from scripts.profile_feature_table import (
    DEFAULT_THRESHOLDS,
    PROFILE_FILENAME,
    ProfileConfig,
    _average_ranks,
    actions,
    app,
    check_against_table,
    corr_excluded,
    corr_groups,
    corr_skip_reasons,
    drift_source,
    high_corr_pairs,
    judge,
    judge_column,
    near_const_periods,
    parse_config,
    profile_table,
    psi_by_period,
    spearman_pairs,
    tail_hits,
    upstream_periods_of,
    write_report,
)

TH = dict(DEFAULT_THRESHOLDS)
ROWS = [100, 100, 100]


def _col(null, mins, maxs=None, *, kind="numeric", dtype="bigint"):
    return {
        "name": "x",
        "dtype": dtype,
        "kind": kind,
        "null": list(null),
        "min": list(mins),
        "max": list(mins if maxs is None else maxs),
    }


def _varying(n=3):
    """Mins/maxs that differ inside every period: no 'only one value' anywhere."""
    return [0] * n, [10] * n


# ---------------------------------------------------------------------------
# Judgement rules (no Spark)
# ---------------------------------------------------------------------------


class TestNullFlags:
    def test_every_period_at_threshold_is_null_all(self):
        lo, hi = _varying()
        col = _col([99, 99, 100], lo, hi)
        assert judge_column(col, ROWS, TH) == ["null_all"]

    def test_one_period_just_below_threshold_is_not_null_all(self):
        lo, hi = _varying()
        col = _col([99, 98, 100], lo, hi)
        assert judge_column(col, ROWS, TH) == ["null_some"]

    def test_some_periods_at_threshold_is_null_some(self):
        lo, hi = _varying()
        col = _col([0, 99, 0], lo, hi)
        assert judge_column(col, ROWS, TH) == ["null_some"]

    def test_just_below_threshold_is_not_null_some(self):
        lo, hi = _varying()
        col = _col([0, 98, 0], lo, hi)
        assert judge_column(col, ROWS, TH) == []


class TestOneValueFlags:
    def test_same_single_value_everywhere_is_const_all(self):
        col = _col([0, 0, 0], [5, 5, 5])
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_one_period_with_a_second_value_is_not_const_all(self):
        col = _col([0, 0, 0], [5, 5, 5], [5, 6, 5])
        assert judge_column(col, ROWS, TH) == ["const_some"]

    def test_some_periods_with_one_value_is_const_some(self):
        col = _col([0, 0, 0], [0, 7, 0], [10, 7, 10])
        assert judge_column(col, ROWS, TH) == ["const_some"]

    def test_every_period_varying_is_not_const_some(self):
        lo, hi = _varying()
        assert judge_column(_col([0, 0, 0], lo, hi), ROWS, TH) == []

    def test_one_value_per_period_but_different_values_is_const_each(self):
        col = _col([0, 0, 0], [31.0, 31.5, 32.0], dtype="double")
        assert judge_column(col, ROWS, TH) == ["const_each"]

    def test_one_value_per_period_and_all_equal_is_not_const_each(self):
        col = _col([0, 0, 0], [31.0, 31.0, 31.0], dtype="double")
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_string_columns_compare_their_values(self):
        col = _col([0, 0, 0], ["N", "N", "N"], kind="string", dtype="string")
        assert judge_column(col, ROWS, TH) == ["const_all"]
        col = _col([0, 0, 0], ["A", "B", "A"], kind="string", dtype="string")
        assert judge_column(col, ROWS, TH) == ["const_each"]


def _dist(counts, *, quantiles=None, mins=None, maxs=None, edges=(5,)):
    """有跑分布檢查的數值欄。``counts`` 是每期各組的筆數，最後一組是空值；每期合計就是總筆數。

    ``quantiles`` 是每期（p1, p99），中間 11 個分位數在兩者之間等距；沒給就是每期 p1 = 0、p99 = 10。
    """
    n = len(counts)
    quantiles = quantiles or [(0, 10)] * n
    col = _col([c[-1] for c in counts], mins or [0] * n, maxs or [10] * n)
    col["quantiles"] = [None if q is None else [q[0] + (q[1] - q[0]) * k / 12 for k in range(13)]
                        for q in quantiles]
    col["bins"] = {"edges": list(edges), "counts": [list(c) for c in counts]}
    return col


def _rows(col):
    return [sum(c) for c in col["bins"]["counts"]]


class TestDrift:
    def test_empty_bin_counts_as_0_0001(self):
        # 基準期 50／50／0、本期 100／0／0：第二組本期是 0、空值兩期都是 0，都用 0.0001 代替
        # 0.5 × ln(1 ÷ 0.5) + (0.0001 − 0.5) × ln(0.0001 ÷ 0.5) + 0 ＝ 4.60432
        col = _dist([[50, 50, 0], [100, 0, 0]])
        assert psi_by_period(col) == [0.0, pytest.approx(4.60432, abs=1e-5)]

    def test_psi_at_threshold_is_drift(self):
        # 80／20：0.3 × ln 1.6 + 0.3 × ln 2.5 ＝ 0.416；70／30：0.2 × ln 1.4 + 0.2 × ln(5/3) ＝ 0.169
        drift = _dist([[50, 50, 0], [80, 20, 0]])
        assert judge_column(drift, _rows(drift), TH) == ["drift"]
        mild = _dist([[50, 50, 0], [70, 30, 0]])
        assert judge_column(mild, _rows(mild), TH) == []
        assert judge_column(mild, _rows(mild), {**TH, "psi": 0.169}) == ["drift"]
        assert judge_column(mild, _rows(mild), {**TH, "psi": 0.17}) == []
        exactly = round(psi_by_period(mild)[1], 9)  # 剛好等於門檻也算
        assert judge_column(mild, _rows(mild), {**TH, "psi": exactly}) == ["drift"]

    def test_no_psi_with_one_period_or_an_empty_base_period(self):
        assert psi_by_period(_dist([[50, 50, 0]])) is None
        empty_base = _dist([[0, 0, 100], [80, 20, 0]], quantiles=[None, (0, 10)],
                           mins=[None, 0], maxs=[None, 10])
        assert psi_by_period(empty_base) is None
        assert judge_column(empty_base, _rows(empty_base), TH) == ["null_some"]

    def test_no_psi_in_an_old_profile(self):
        # 只有基本檢查的 profile.json：沒有分組筆數，也沒有分位數
        assert psi_by_period(_col([0, 0], [0, 0], [9, 9])) is None

    def test_source_is_null_when_the_values_alone_did_not_shift(self):
        # 空值從 4% 變 40%，有值的那幾組比例不變：扣掉空值後 PSI 是 0
        nulls = _dist([[48, 48, 4], [30, 30, 40]])
        assert judge_column(nulls, _rows(nulls), TH) == ["drift"]
        assert drift_source(nulls, TH) == "null"
        assert drift_source(_dist([[48, 48, 4], [80, 16, 4]]), TH) == "value"

    def test_excludable_and_one_value_per_period_columns_are_not_drift(self):
        shifted = [[50, 50, 0], [100, 0, 0]]
        const_all = _dist(shifted, mins=[3, 3], maxs=[3, 3])
        const_each = _dist(shifted, mins=[3, 4], maxs=[3, 4])
        null_all = _dist([[1, 0, 99], [0, 1, 99]])
        assert judge_column(const_all, _rows(const_all), TH) == ["const_all"]
        assert judge_column(const_each, _rows(const_each), TH) == ["const_each"]
        assert judge_column(null_all, _rows(null_all), TH) == ["null_all"]


def _tail(*periods):
    """每期（p1, p99, 最小值, 最大值），分組筆數每期一樣（沒有分布偏移）。"""
    col = _dist([[50, 50, 0]] * len(periods), quantiles=[(p[0], p[1]) for p in periods],
                mins=[p[2] for p in periods], maxs=[p[3] for p in periods])
    return judge_column(col, _rows(col), TH), col


class TestTail:
    # 數字取自設計文件附錄一的分布（每期 120 萬筆）
    def test_appendix_errors_are_flagged(self):
        assert _tail((18, 73, 18, 999))[0] == ["tail"]            # 年齡有一筆 999
        assert _tail((0, 40, 0, 100_000))[0] == ["tail"]          # p99 約 40 的次數欄有一筆 100,000
        assert _tail((2153.43, 225147.51, -20_000_000, 3468254.02))[0] == ["tail"]  # 存款正負號弄錯

    def test_lognormal_deposit_is_not_flagged(self):
        # 存款 lognormal σ=1：倍數 14.4 夠了，log 後倍數只有 0.57
        assert _tail((2153.48, 225147.51, 169.17, 3468254.02))[0] == []

    def test_sides_and_reported_period(self):
        flags, col = _tail((18, 73, 18, 95), (2153.43, 225147.51, -20_000_000, 225147.51))
        assert flags == ["tail"]
        assert [(t, side) for t, side, _, _ in tail_hits(col, TH)] == [(1, "min")]

    def test_both_conditions_on_the_same_side(self):
        # p1 1、p99 1,000。最大值 20,000：倍數 19.0、log 後 0.48；最小值 −1,000：倍數 1.0、log 後 1.22
        assert _tail((1, 1000, -1000, 20_000))[0] == []

    def test_both_conditions_in_the_same_period(self):
        # 第一期只有倍數夠（20,000），第二期只有 log 後倍數夠（最小值 −1,000）
        assert _tail((1, 1000, 1, 20_000), (1, 1000, -1000, 1000))[0] == []

    def test_exactly_at_both_thresholds(self):
        # p1 100、p99 1,000、最大值 10,000：倍數剛好 10，log 後 1.003
        flags, col = _tail((100, 1000, 100, 10_000))
        assert flags == ["tail"]
        assert _tail((100, 1000, 100, 9_999))[0] == []
        assert judge_column(col, _rows(col), {**TH, "tail_log_ratio": 1.01}) == []

    def test_p1_equal_to_p99_is_not_judged(self):
        assert _tail((0, 0, 0, 1_000_000))[0] == []


class TestNearConst:
    def test_p1_equal_to_p99_but_not_one_value(self):
        # 第一期 p1 = p99 = 0、最大值 1；第二期有變化；第三期只有一個值（另一種問題，不重複列）
        col = _dist([[50, 50, 0]] * 3, quantiles=[(0, 0), (0, 5), (3, 3)], mins=[0, 0, 3], maxs=[1, 9, 3])
        assert near_const_periods(col) == [0]
        assert near_const_periods(_col([0, 0], [0, 0], [9, 9])) == []  # 舊的 profile.json


class TestNoFloat32Check:
    def test_large_values_are_not_a_problem(self):
        # float32 精度檢查已拿掉：對 LightGBM 幾乎沒影響（設計文件附錄二）
        col = _col([0, 0, 0], [0.5, 0.5, 0.5], [1.5, 272_528_123.98, 1.5], dtype="double")
        assert judge_column(col, ROWS, TH) == []
        assert judge_column(_col([0, 0, 0], [0, 0, 0], [1, 2**24 + 1, 1]), ROWS, TH) == []


class TestOrderAndEdges:
    def test_const_all_drops_null_some(self):
        col = _col([0, 99, 0], [5, 5, 5])
        assert judge_column(col, ROWS, TH) == ["const_all"]

    def test_null_some_and_const_each_coexist(self):
        col = _col([0, 99, 0], [1, 2, 3])
        assert judge_column(col, ROWS, TH) == ["null_some", "const_each"]

    def test_all_empty_period_is_neither_one_value_nor_varying(self):
        # 中間那期全空：不算「只有一個值」，所以另外兩期一樣是同一個值→整欄只有一個值
        col = _col([0, 100, 0], [5, None, 5])
        assert judge_column(col, ROWS, TH) == ["const_all"]
        # 另外兩期都有變化：全空那期不能讓它變成「部分期別只有一個值」
        col = _col([0, 100, 0], [0, None, 0], [9, None, 9])
        assert judge_column(col, ROWS, TH) == ["null_some"]

    def test_single_period_one_value_is_const_all_not_const_each(self):
        col = _col([0], [5])
        assert judge_column(col, [100], TH) == ["const_all"]

    def test_single_period_mostly_empty_is_null_all_not_null_some(self):
        col = _col([99], [0], [9])
        assert judge_column(col, [100], TH) == ["null_all"]

    def test_unchecked_columns_are_not_judged(self):
        profile = {
            "rows": ROWS,
            "columns": [
                {"name": "d", "dtype": "date", "kind": "unchecked"},
                dict(_col([0, 0, 0], [5, 5, 5]), name="c"),
            ],
        }
        assert judge(profile, TH) == {"c": ["const_all"]}


class TestActions:
    """判斷結果到總結裡的四種做法。"""

    def _actions(self, col, rows=ROWS):
        return actions(col, rows, judge_column(col, rows, TH), TH)

    def test_jump_of_at_least_10_points_means_upstream(self):
        # 99% − 89% 剛好 10 個百分點（浮點數相減是 0.0999…，要算成 10）
        lo, hi = _varying()
        assert self._actions(_col([89, 99, 89], lo, hi)) == ["upstream"]

    def test_jump_below_10_points_means_judge(self):
        lo, hi = _varying()
        assert self._actions(_col([90, 99, 90], lo, hi)) == ["judge"]

    def test_const_some_means_upstream_and_const_each_means_judge(self):
        assert self._actions(_col([0, 0, 0], [0, 7, 0], [10, 7, 10])) == ["upstream"]
        assert self._actions(_col([0, 0, 0], [1, 2, 3])) == ["judge"]

    def test_two_actions_are_both_listed(self):
        # 中間那期斷掉（跳幅 96 個百分點），另外兩期各只有一個值、值不同
        assert self._actions(_col([4, 100, 4], [1, None, 2])) == ["upstream", "judge"]

    def test_excludable_columns_only_get_exclude(self):
        assert self._actions(_col([0, 0, 0], [5, 5, 5])) == ["exclude"]
        lo, hi = _varying()
        assert self._actions(_col([99, 100, 99], lo, hi)) == ["exclude"]

    def test_no_problem_no_action(self):
        lo, hi = _varying()
        assert self._actions(_col([0, 0, 0], lo, hi)) == []

    def test_tail_in_a_few_periods_means_upstream_in_most_means_judge(self):
        # 四期裡一期被標（不到一半）：像那一期的資料錯了；剛好兩期被標（一半）：像本來就長尾
        ok, bad = (18, 73, 18, 95), (18, 73, 18, 999)
        flags_once, once = _tail(ok, bad, ok, ok)
        flags_twice, twice = _tail(bad, bad, ok, ok)
        assert actions(once, _rows(once), flags_once, TH) == ["upstream"]
        assert actions(twice, _rows(twice), flags_twice, TH) == ["judge"]

    def test_tail_asks_upstream_about_the_worst_period_only(self):
        # 五期裡第二、四期被標（不到一半），第四期的 99,999 比第二期的 999 離譜
        ok = (18, 73, 18, 95)
        flags, col = _tail(ok, (18, 73, 18, 999), ok, (18, 73, 18, 99_999), ok)
        assert upstream_periods_of(col, _rows(col), flags, TH) == [3]

    def test_drift_already_listed_upstream_is_not_listed_again(self):
        # 中間那期全部填 0（部分期別只有一個值），PSI 也只在那一期達門檻：同一件事
        same = _dist([[50, 50, 0], [100, 0, 0], [50, 50, 0]], mins=[0, 0, 0], maxs=[10, 0, 10])
        assert self._actions(same) == ["upstream"]
        # 之後每一期都變了，不只那一期：兩件事都列
        more = _dist([[50, 50, 0], [100, 0, 0], [100, 0, 0]], mins=[0, 0, 0], maxs=[10, 0, 10])
        assert self._actions(more) == ["upstream", "check"]


# ---------------------------------------------------------------------------
# Correlation (no Spark): Spearman on the latest-period sample, skip reasons, groups
# ---------------------------------------------------------------------------


def _ref_spearman(values, i, j):
    """逐對計算的對照：只取兩欄都有值的筆，在這些筆裡重排名次（pandas 的 Spearman）。"""
    ok = ~np.isnan(values[:, i]) & ~np.isnan(values[:, j])
    return pd.Series(values[ok, i]).corr(pd.Series(values[ok, j]), method="spearman"), int(ok.sum())


class TestSpearman:
    def test_ties_get_the_average_rank_and_empty_values_stay_empty(self):
        got = _average_ranks(np.array([10, 20, 20, np.nan, 5.0]))
        assert np.allclose(got, [2, 3.5, 3.5, np.nan, 1], equal_nan=True)

    def test_equals_spearman_when_nothing_is_empty(self):
        rng = np.random.default_rng(1)
        a = rng.integers(0, 20, 3000).astype(float)  # 很多同值
        values = np.column_stack([a, a * 3 + rng.integers(0, 5, 3000), rng.normal(size=3000), -np.log1p(a)])
        out = spearman_pairs(values, list("abcd"), keep=0)
        ref = pd.DataFrame(values, columns=list("abcd")).corr(method="spearman")
        assert len(out["pairs"]) == 6
        for a_, b_, r, n in out["pairs"]:
            assert r == pytest.approx(ref.loc[a_, b_], abs=1e-6) and n == 3000

    def test_matrix_matches_pair_by_pair_with_empty_values(self):
        # 驗收條件：兩兩的計算用矩陣運算，和逐對計算的結果對照。keep=0 時每一對都會逐對重排，
        # 這裡驗的是重排的名次對不對
        rng = np.random.default_rng(2)
        base = rng.normal(size=(5000, 4))
        values = np.exp(base[:, [0, 0, 1, 1, 2, 3]] + rng.normal(size=(5000, 6)) * 0.3)
        values[rng.random(values.shape) < 0.2] = np.nan
        values[values[:, 1] > np.nanquantile(values[:, 1], 0.7), 1] = np.nan  # 大的值才空
        names = [f"c{i}" for i in range(6)]
        got = {(a, b): (r, n) for a, b, r, n in spearman_pairs(values, names, keep=0)["pairs"]}
        assert len(got) == 15
        for i in range(6):
            for j in range(i + 1, 6):
                ref, n = _ref_spearman(values, i, j)
                assert got[(names[i], names[j])] == (pytest.approx(ref, abs=1e-6), n), (i, j)

    def test_empty_values_that_depend_on_the_value_do_not_hide_a_high_correlation(self):
        # 規格審查的反例：值越大越可能空（有某產品才有餘額）。整欄一起排的名次算出 0.938，
        # 在兩欄都有值的筆裡重排才是 0.965，0.95 的門檻會被翻過去
        rng = np.random.default_rng(0)
        u = rng.random(50_000)
        a = u + rng.normal(size=50_000) * 0.05
        a[rng.random(50_000) < u ** (1 / 3)] = np.nan
        (pair,) = spearman_pairs(np.column_stack([a, u]), ["a", "u"])["pairs"]
        ref, n = _ref_spearman(np.column_stack([a, u]), 0, 1)
        assert pair == ["a", "u", pytest.approx(ref, abs=1e-6), n] and pair[2] > 0.96
        # 只存 ≥ 0.95 時也要留：整欄名次的 0.938 先篩掉的話，精確的 0.965 就沒機會算
        assert len(spearman_pairs(np.column_stack([a, u]), ["a", "u"], keep=0.95)["pairs"]) == 1

    def test_no_empty_values_keep_the_matrix_value(self):
        # 兩欄都沒有空值時整欄的名次就是精確的，不重算，和 pandas 的 Spearman 一樣
        rng = np.random.default_rng(5)
        a = rng.normal(size=3000)
        values = np.column_stack([a, a + rng.normal(size=3000) * 0.3])
        (pair,) = spearman_pairs(values, ["a", "b"])["pairs"]
        assert pair[2] == pytest.approx(_ref_spearman(values, 0, 1)[0], abs=1e-6)

    def test_fewer_than_1000_rows_in_common_gives_no_coefficient(self):
        # a、b 各自有 1,500 筆有值，同時有值的只有 999 或 1,000 筆
        for common, kept in ((999, False), (1000, True)):
            a = np.full(3000, np.nan)
            b = np.full(3000, np.nan)
            a[:1500] = np.arange(1500)
            b[1500 - common:3000 - common] = np.arange(1500)
            out = spearman_pairs(np.column_stack([a, b]), ["a", "b"])
            assert bool(out["pairs"]) is kept, common
            assert out["few_pairs"] == (0 if kept else 1) and out["sparse"] == {}, common

    @pytest.mark.parametrize("first", [True, False])
    def test_no_variation_where_both_have_values_gives_no_coefficient(self, first):
        # flat 只在 other 有值的那些筆裡都是 7，整欄是有變化的；放在第一欄或第二欄都要擋
        flat = np.r_[np.full(2000, 7.0), np.arange(2000.0)]
        other = np.r_[np.arange(2000.0), np.full(2000, np.nan)]
        cols = [flat, other] if first else [other, flat]
        out = spearman_pairs(np.column_stack(cols), ["x", "y"], keep=0)
        assert out["pairs"] == [] and out["flat_pairs"] == 1 and out["flat"] == []

    def test_sparse_and_flat_columns_are_named_but_not_their_partners(self):
        rng = np.random.default_rng(3)
        n = 5000
        good = rng.normal(size=(n, 2))
        sparse = np.full(n, np.nan)
        sparse[:600] = rng.normal(size=600)
        values = np.column_stack([good, sparse, np.full(n, 4.0)])
        out = spearman_pairs(values, ["g1", "g2", "sparse", "const"], keep=0)
        assert out["sparse"] == {"sparse": 600} and out["flat"] == ["const"]
        # 跟 sparse、const 配不成的組，不再算進「兩欄各自都夠」的組數
        assert out["few_pairs"] == 0 and out["flat_pairs"] == 0
        assert [p[:2] for p in out["pairs"]] == [["g1", "g2"]]

    def test_only_pairs_at_or_above_0_5_are_kept_with_their_sign(self):
        rng = np.random.default_rng(4)
        a = rng.normal(size=4000)
        values = np.column_stack([a, -a + rng.normal(size=4000) * 0.05, a + rng.normal(size=4000) * 3])
        out = spearman_pairs(values, ["a", "neg", "weak"])
        assert [(x, y) for x, y, _, _ in out["pairs"]] == [("a", "neg")]
        assert out["pairs"][0][2] < -0.99


def _skip_cols():
    return [
        dict(_col([0, 0, 0], [0, 0, 0], [9, 9, 9]), name="fine"),
        dict(_col([100, 0, 0], [None, 0, 0], [None, 9, 9]), name="empty_before"),  # 只看最新一期
        dict(_col([0, 0, 99], [0, 0, 0], [9, 9, 9]), name="latest_99"),            # 剛好 99%
        dict(_col([0, 0, 98], [0, 0, 0], [9, 9, 9]), name="latest_98"),
        dict(_col([0, 0, 100], [0, 0, None], [9, 9, None]), name="latest_empty"),
        dict(_col([0, 0, 0], [0, 0, 3], [9, 9, 3]), name="latest_one"),
        dict(_col([0, 0, 0], ["a"] * 3, ["z"] * 3, kind="string", dtype="string"), name="s"),
        {"name": "load_dt", "dtype": "date", "kind": "unchecked"},
    ]


class TestCorrSkip:
    def test_run_skips_only_what_needs_no_threshold(self):
        # 最新一期「幾乎都是空值」的照樣算，render 時才依門檻排除，換門檻不用重跑
        assert corr_skip_reasons(_skip_cols(), ROWS) == {"latest_empty": "empty", "latest_one": "const", "s": "string"}

    def test_render_excludes_mostly_empty_columns_by_the_current_threshold(self):
        profile = {"rows": ROWS, "columns": _skip_cols(), "correlation": {
            "skipped": corr_skip_reasons(_skip_cols(), ROWS), "sparse": {"fine": 40}, "flat": ["latest_98"]}}
        base = {"latest_empty": "empty", "latest_one": "const", "s": "string"}
        assert corr_excluded(profile, TH) == {**base, "latest_99": "null", "fine": "sparse", "latest_98": "flat"}
        assert corr_excluded(profile, {**TH, "null_ratio": 0.999}) == {**base, "fine": "sparse", "latest_98": "flat"}
        assert corr_excluded({"columns": []}, TH) == {}  # 沒跑相關性


def _corr_profile_stub(pairs, flags_cols=()):
    """judge 用的最小 profile：每一欄都正常，除了 flags_cols 給的。"""
    names = sorted({x for a, b, _, _ in pairs for x in (a, b)})
    cols = [dict(_col([0, 0, 0], [0, 0, 0], [9, 9, 9]), name=n) for n in names if n not in dict(flags_cols)]
    cols += [dict(c, name=n) for n, c in flags_cols]
    return {"rows": ROWS, "columns": cols, "correlation": {
        "pairs": [list(p) for p in pairs], "skipped": {}, "sparse": {}, "flat": []}}


class TestHighCorr:
    def test_at_threshold_is_high_correlation(self):
        profile = _corr_profile_stub([("a", "b", 0.95, 2000), ("c", "d", -0.95, 2000), ("e", "f", 0.949999, 2000)])
        flags = judge(profile, TH)
        assert [k for k in "abcdef" if "corr" in flags[k]] == list("abcd")

    def test_excludable_columns_are_not_paired(self):
        # 已經可以直接排除的欄位，不再判斷其他問題；它的夥伴也不因為它而被標
        profile = _corr_profile_stub(
            [("dead", "x", 0.99, 2000), ("one", "y", 0.99, 2000)],
            flags_cols=[("dead", _col([99, 99, 100], [0, 0, None], [9, 9, None])),
                        ("one", _col([0, 0, 0], [3, 3, 3]))],
        )
        flags = judge(profile, TH)
        assert flags["dead"] == ["null_all"] and flags["one"] == ["const_all"]
        assert flags["x"] == [] and flags["y"] == []
        assert high_corr_pairs(profile, TH, flags) == []

    def test_old_profile_has_no_pairs(self):
        assert high_corr_pairs({"columns": []}, {"null_ratio": 0.99}, {}) == []

    def test_groups_join_chains_and_follow_the_table_order(self):
        pairs = [("b", "c", 0.96, 1), ("e", "d", 0.97, 1), ("a", "b", 0.99, 1), ("x", "y", 0.98, 1),
                 ("y", "z", 0.98, 1)]
        order = ["x", "y", "z", "a", "b", "c", "d", "e"]
        assert corr_groups(pairs, order) == [["x", "y", "z"], ["a", "b", "c"], ["d", "e"]]

    def test_high_correlation_means_keep_one_per_group(self):
        col = _col([0, 0, 0], [0, 0, 0], [9, 9, 9])
        assert actions(col, ROWS, ["corr"], TH) == ["corr"]
        assert actions(col, ROWS, ["const_each", "corr"], TH) == ["judge", "corr"]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_GOOD = """
table: mydb.feature_table
time_col: snap_date
key_cols: [cust_id]
checks:
  distribution: true
  correlation: false
"""


class TestParseConfig:
    def test_minimal_config_uses_defaults(self):
        cfg, problems = parse_config(_GOOD)
        assert problems == []
        assert cfg.table == "mydb.feature_table"
        assert cfg.exclude_cols == []
        assert cfg.time_start is None and cfg.time_end is None
        assert cfg.where is None
        assert cfg.distribution is True and cfg.correlation is False
        assert cfg.thresholds == DEFAULT_THRESHOLDS

    def test_thresholds_override_only_what_is_written(self):
        cfg, problems = parse_config(_GOOD + "thresholds:\n  null_ratio: 0.9\n")
        assert problems == []
        assert cfg.thresholds == {**DEFAULT_THRESHOLDS, "null_ratio": 0.9}

    def test_unquoted_dates_in_time_range_become_strings(self):
        cfg, problems = parse_config(
            _GOOD + "time_range:\n  start: 2024-01-31\n  end: 2025-02-28\n"
        )
        assert problems == []
        assert (cfg.time_start, cfg.time_end) == ("2024-01-31", "2025-02-28")

    def test_every_problem_is_listed_at_once(self):
        text = """
time_col: snap_date
exlude_cols: [x]
thresholds:
  null_ratio: 1.5
  psi: 0
  corr: 0.4
  cor: 0.9
"""
        _, problems = parse_config(text)
        joined = "\n".join(problems)
        for needle in ("table", "key_cols", "checks", "exlude_cols", "鍵：cor（", "null_ratio", "psi", "corr"):
            assert needle in joined, needle
        assert len(problems) == 8

    def test_checks_must_be_written_as_true_or_false(self):
        base = _GOOD.split("checks:")[0]
        for checks, needle in (
            ("checks:\n  correlation: false\n", "checks.distribution"),
            ("checks:\n  distribution: false\n", "checks.correlation"),
            ("checks:\n  distribution: maybe\n  correlation: false\n", "true 或 false"),
            ("checks:\n  distribution: false\n  correlation: false\n  corr: true\n", "corr"),
        ):
            _, problems = parse_config(base + checks)
            assert len(problems) == 1 and needle in problems[0], (checks, problems)
        cfg, problems = parse_config(base + "checks:\n  distribution: false\n  correlation: true\n")
        assert problems == [] and cfg.distribution is False and cfg.correlation is True

    def test_corr_threshold_is_between_0_5_and_1(self):
        # profile.json 只存絕對值 ≥ 0.5 的配對，再低就畫不出來
        for v, ok in ((0.5, True), (1, True), (0.49, False), (1.01, False), (0, False)):
            cfg, problems = parse_config(_GOOD + f"thresholds:\n  corr: {v}\n")
            if ok:
                assert problems == [] and cfg.thresholds["corr"] == v, v
            else:
                assert len(problems) == 1 and "thresholds.corr 要介於 0.5 到 1" in problems[0], (v, problems)

    def test_drift_and_tail_thresholds_must_be_positive(self):
        text = _GOOD + "thresholds:\n  psi: -1\n  tail_ratio: 0\n  tail_log_ratio: 0.5\n"
        cfg, problems = parse_config(text)
        assert len(problems) == 2
        assert "psi" in problems[0] and "tail_ratio" in problems[1]
        assert cfg.thresholds["tail_log_ratio"] == 0.5

    def test_removed_float32_threshold_is_an_unknown_key(self):
        _, problems = parse_config(_GOOD + "thresholds:\n  precision_max_abs: 100000\n")
        assert len(problems) == 1 and "precision_max_abs" in problems[0]

    def test_spark_settings_are_not_a_config_key(self):
        # 同 scripts/sampling_overrides_editor.py：Spark 設定全由 SPARK_CONF_DIR 決定
        _, problems = parse_config(_GOOD + "spark:\n  spark.executor.memory: 8g\n")
        assert len(problems) == 1 and "spark" in problems[0]

    def test_broken_yaml_is_one_problem(self):
        _, problems = parse_config("table: [unclosed\n")
        assert len(problems) == 1 and "yaml" in problems[0]

    def test_template_parses_and_lists_every_key(self):
        from pathlib import Path

        import yaml

        from scripts import profile_feature_table as pft

        path = Path(pft.__file__).with_name("profile_feature_table.template.yaml")
        text = path.read_text(encoding="utf-8")
        _, problems = parse_config(text)
        assert problems == []
        raw = yaml.safe_load(text)
        assert set(raw) == set(pft._REQUIRED_KEYS) | set(pft._OPTIONAL_KEYS)
        assert set(raw["thresholds"]) == set(DEFAULT_THRESHOLDS)
        assert set(raw["checks"]) == set(pft._CHECK_KEYS) == {"distribution", "correlation"}


class TestCheckAgainstTable:
    def test_missing_table(self):
        cfg, _ = parse_config(_GOOD)
        assert check_against_table(cfg, None) == ["表 mydb.feature_table 不存在"]

    def test_missing_columns_listed_together(self):
        cfg, _ = parse_config(_GOOD + "exclude_cols: [etl_ts, gone]\n")
        problems = check_against_table(cfg, ["SNAP_DATE", "etl_ts", "f1"])
        assert problems == [
            "key_cols 的 cust_id 不在表 mydb.feature_table 裡",
            "exclude_cols 的 gone 不在表 mydb.feature_table 裡",
        ]


# ---------------------------------------------------------------------------
# render: profile.json → judgement → report, no Spark
# ---------------------------------------------------------------------------


def _profile(tmp_path):
    profile = {
        "schema_version": 1,
        "generated_at": "2026-10-15T14:02:00",
        "config_text": _GOOD,
        "config": {
            "table": "mydb.feature_table", "time_col": "snap_date",
            "key_cols": ["cust_id"], "exclude_cols": [],
            "time_range": {"start": None, "end": None}, "where": None,
            "thresholds": dict(DEFAULT_THRESHOLDS),
        },
        "periods": ["2024-01-31", "2024-02-29", "2024-03-31"],
        "rows": ROWS,
        "null_time_rows": 0,
        "elapsed_seconds": 75.0,
        "batch_size": 200,
        "columns": [
            dict(_col([95, 0, 0], [0, 0, 0], [9, 9, 9]), name="half_empty"),
            dict(_col([0, 0, 0], [3, 3, 3]), name="sys_version"),
            {"name": "load_dt", "dtype": "date", "kind": "unchecked"},
        ],
    }
    (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
    return profile


def _section_html(page, sid):
    return page.split(f'<section id="{sid}">')[1].split("</section>")[0]


def _named(name, col):
    col["name"] = name
    return col


def _dist_profile(tmp_path):
    """有跑分布檢查的 profile：每種新問題各一欄，三期、每期 100 筆。"""
    profile = _profile(tmp_path)
    profile["config"]["checks"] = {"distribution": True}
    profile["quantile_accuracy"] = 10000
    for c in profile["columns"]:  # 第一階段那兩欄：沒有分組筆數，不比 PSI
        if c["kind"] != "unchecked":
            c["quantiles"] = [[c["min"][t]] * 12 + [c["max"][t]] for t in range(3)]
            c["bins"] = None
    profile["columns"] += [
        _named("unit_change", _dist([[50, 50, 0], [100, 0, 0], [100, 0, 0]])),      # PSI 4.6
        _named("cc_bal", _dist([[48, 48, 4], [0, 0, 100], [48, 48, 4]],            # 那一期斷掉
                               quantiles=[(0, 10), None, (0, 10)], mins=[0, None, 0], maxs=[10, None, 10])),
        _named("age", _dist([[50, 50, 0]] * 3, quantiles=[(18, 73)] * 3,           # 第二期有一筆 999
                            mins=[18] * 3, maxs=[95, 999, 95])),
        _named("assets", _dist([[50, 50, 0]] * 3, quantiles=[(100862, 5212476)] * 3,  # 每期都被標：長尾
                               mins=[100000] * 3, maxs=[4.97e10] * 3)),
        _named("rare_flag", _dist([[99, 1, 0]] * 3, quantiles=[(0, 0)] * 3, mins=[0] * 3, maxs=[1] * 3)),
        _named("fx_rate", _dist([[50, 50, 0], [100, 0, 0], [100, 0, 0]],                # 每期各只有一個值
                                quantiles=[(1, 1), (2, 2), (3, 3)], mins=[1, 2, 3], maxs=[1, 2, 3])),
    ]
    (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
    return profile


def _names_in(html_part):
    import re

    return set(re.findall(r'data-name="([^"]+)"', html_part))


class TestRender:
    def test_new_thresholds_change_the_judgement(self, tmp_path):
        _profile(tmp_path)
        runner = CliRunner()
        result = runner.invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        page = (tmp_path / "index.html").read_text()
        assert "部分期別幾乎是空值（0 欄）" in page

        cfg = tmp_path / "loose.yaml"
        cfg.write_text(_GOOD + "thresholds:\n  null_ratio: 0.95\n")
        result = runner.invoke(app, ["render", str(tmp_path), "--config", str(cfg)])
        assert result.exit_code == 0, result.output
        page = (tmp_path / "index.html").read_text()
        assert "部分期別幾乎是空值（1 欄）" in page
        assert 'data-name="half_empty"' in _section_html(page, "s-null-some")
        assert "0.95" in _section_html(page, "s-scope")

    def test_old_profile_with_the_removed_float32_threshold_still_renders(self, tmp_path):
        # 上一版存的 profile.json 帶著 precision_max_abs 與 has_decimal
        profile = _profile(tmp_path)
        profile["config"]["thresholds"]["precision_max_abs"] = 100000
        for c in profile["columns"]:
            if c["kind"] != "unchecked":
                c["has_decimal"] = False
        (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
        result = CliRunner().invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "precision_max_abs" not in _section_html((tmp_path / "index.html").read_text(), "s-scope")

    def test_single_page_without_framework_wording(self, tmp_path):
        profile = _profile(tmp_path)
        path = write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        assert path == tmp_path / "index.html"
        assert sorted(p.name for p in tmp_path.iterdir()) == ["index.html", PROFILE_FILENAME]
        page = path.read_text()
        assert "load_dt" in _section_html(page, "s-scope")
        # 使用者要求：報告不跟框架耦合，不提框架的設定或 pipeline
        for word in ("dataset", "training", "feature_selection", "drop_columns", "float32", "LightGBM"):
            assert word not in page, word
        # 設計文件第六節：不用箭頭
        assert "→" not in page

    def test_element_ids_are_unique(self, tmp_path):
        # 重複的 id 讓 getElementById 拿到 section 而不是表格，整段 JS 在載入時就壞掉
        import re
        from collections import Counter

        profile = _profile(tmp_path)
        # 欄名剛好是 data：每列的 id 是 col-<欄名>，不能跟內嵌 JSON 的 id 撞
        profile["columns"].append(dict(_col([0, 0, 0], [0, 0, 0], [9, 9, 9]), name="data"))
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        ids = re.findall(r'\sid="([^"]+)"', (tmp_path / "index.html").read_text())
        assert [i for i, n in Counter(ids).items() if n > 1] == []
        assert {"t-all", "fp-data", "col-data", "col-half_empty"} <= set(ids)

    def test_summary_groups_columns_and_lists_upstream_periods(self, tmp_path):
        profile = _profile(tmp_path)
        profile["columns"] += [
            dict(_col([4, 100, 4], [0, None, 0], [9, None, 9]), name="cc_bal"),     # 跳幅 96：上游
            dict(_col([95, 99, 95], [0, 0, 0], [9, 9, 9]), name="loan_amt"),        # 跳幅 4：看業務意義
        ]
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        summary = _section_html(page, "s-actions")
        exclude, upstream, judge_row = summary.split("<tr>")[2:5]
        assert 'data-copy-text="sys_version"' in exclude
        assert 'data-copy-text="cc_bal"' in upstream and "<b>2024-02</b>" in upstream
        assert "loan_amt" not in upstream
        assert 'data-copy-text="loan_amt"' in judge_row
        # 期別篩選用期別的序號當代號，期別字串裡有空白也不會切壞
        some = _section_html(page, "s-null-some")
        assert 'data-name="cc_bal" data-bad="p1"' in some
        assert '<option value="p1">2024-02</option>' in some

    def test_toc_count_matches_the_variation_filter_at_exactly_1_point(self, tmp_path):
        # 28%、29%、28%：浮點數相減是 0.999… 個百分點，表上顯示 1.0、預設篩選（≥ 1）會列出
        profile = _profile(tmp_path)
        profile["columns"].append(dict(_col([28, 29, 28], [0, 0, 0], [9, 9, 9]), name="wobble"))
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        assert 'data-name="wobble" data-var="1.0"' in _section_html(page, "s-null-var")
        # half_empty（95 個百分點）＋ wobble
        assert "另有 2 欄有變動但沒達門檻" in _section_html(page, "s-overview")

    def test_sparks_drop_the_gap_beyond_75_periods(self, tmp_path):
        # 150px 寬、每根 1px 加 1px 空隙最多放 75 期；再多就要拿掉空隙，否則溢出
        for n, cls in ((75, 'class="spark"'), (76, 'class="spark dense"')):
            profile = _profile(tmp_path)
            profile["periods"] = [f"p{t:03d}" for t in range(n)]
            profile["rows"] = [100] * n
            for c in profile["columns"]:
                if c["kind"] != "unchecked":
                    c.update(null=[0] * n, min=[0] * n, max=[9] * n)
            write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
            assert f'<span {cls} data-mode="null"' in (tmp_path / "index.html").read_text(), n

    def test_copied_names_are_one_per_line(self, tmp_path):
        profile = _profile(tmp_path)
        profile["columns"].append(dict(_col([0, 0, 0], [1, 1, 1]), name="flag_b"))
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        exclude = _section_html((tmp_path / "index.html").read_text(), "s-actions").split("<tr>")[2]
        assert 'data-copy-text="sys_version\nflag_b"' in exclude or 'data-copy-text="flag_b\nsys_version"' in exclude

    def test_table_name_is_escaped(self, tmp_path):
        profile = _profile(tmp_path)
        profile["config"]["table"] = "db.<b>t</b>"
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        assert "<b>t</b>" not in page
        assert "&lt;b&gt;t&lt;/b&gt;" in page

    def test_distribution_sections(self, tmp_path):
        profile = _dist_profile(tmp_path)
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        assert _names_in(_section_html(page, "s-drift-num")) == {"unit_change", "cc_bal"}
        assert _names_in(_section_html(page, "s-tail")) == {"age", "assets"}
        assert _names_in(_section_html(page, "s-const-near")) == {"rare_flag"}
        # 空值造成的偏移排在後面
        drift = _section_html(page, "s-drift-num")
        assert drift.index('data-name="unit_change"') < drift.index('data-name="cc_bal"')
        assert 'data-name="cc_bal" data-drift="p1" data-src="null"' in drift
        # 不比分布的欄位（這裡是每期各只有一個值），展開明細也不放 PSI，免得看到 PSI 很大卻沒被標
        dist = _blob(page)["dist"]
        assert "psi" in dist["unit_change"] and "psi" not in dist["fx_rate"]

    def test_summary_places_drift_and_tail(self, tmp_path):
        profile = _dist_profile(tmp_path)
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        rows = _section_html((tmp_path / "index.html").read_text(), "s-actions").split("<tr>")[2:6]
        upstream, check, judge_row = rows[1], rows[2], rows[3]
        assert "請上游查" in upstream and "確認分布" in check and "看業務意義" in judge_row
        # 極端值只出現在少數幾期：請上游查最離譜的那一期
        assert "<b>2024-02</b>" in upstream and 'data-copy-text="age"' not in check
        assert "age" in upstream.split("<pre>")[1]
        # 那一期斷掉造成的偏移，已經在請上游查列了，不再列一次
        assert "cc_bal" in upstream.split("<pre>")[1] and "cc_bal" not in check
        assert "unit_change" in check.split("<pre>")[1]
        # 每期都被標的長尾：看業務意義
        assert "assets" in judge_row.split("<pre>")[1] and "assets" not in upstream

    def test_new_thresholds_change_drift_and_tail(self, tmp_path):
        _dist_profile(tmp_path)
        runner = CliRunner()
        for thresholds, section, gone in (
            ("psi: 5", "s-drift-num", "unit_change"),             # PSI 4.6
            ("tail_ratio: 20", "s-tail", "age"),                  # 倍數 16.8
            ("tail_log_ratio: 2", "s-tail", "age"),               # log 後倍數 1.92
        ):
            assert runner.invoke(app, ["render", str(tmp_path)]).exit_code == 0
            assert gone in _names_in(_section_html((tmp_path / "index.html").read_text(), section))
            cfg = tmp_path / "th.yaml"
            cfg.write_text(_GOOD + f"thresholds:\n  {thresholds}\n")
            result = runner.invoke(app, ["render", str(tmp_path), "--config", str(cfg)])
            assert result.exit_code == 0, result.output
            assert gone not in _names_in(_section_html((tmp_path / "index.html").read_text(), section)), thresholds

    def test_basic_only_profile_says_not_run(self, tmp_path):
        # 只有基本檢查的舊 profile.json：沒有 checks、分位數、分組筆數，存的門檻也只有 null_ratio
        profile = _profile(tmp_path)
        profile["config"]["thresholds"] = {"null_ratio": 0.99}
        (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
        result = CliRunner().invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        page = (tmp_path / "index.html").read_text()
        for sid in ("s-drift", "s-tail", "s-const-near", "s-corr"):
            assert "這次沒有跑" in _section_html(page, sid), sid
        assert 'href="#s-drift-num"' not in page and "確認分布" not in page

    def test_distribution_page_ids_and_wording(self, tmp_path):
        import re
        from collections import Counter

        profile = _dist_profile(tmp_path)
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        ids = re.findall(r'\sid="([^"]+)"', page)
        assert [i for i, n in Counter(ids).items() if n > 1] == []
        assert {"t-drift-num", "t-tail", "t-const-near", "col-unit_change"} <= set(ids)
        for word in ("dataset", "training", "feature_selection", "drop_columns", "LightGBM", "→"):
            assert word not in page, word

    def test_string_values_cannot_close_the_embedded_json(self, tmp_path):
        profile = _profile(tmp_path)
        profile["columns"] += [
            dict(_col([0, 0, 0], ["</script>"] * 3, ["z"] * 3, kind="string", dtype="string"), name="s"),
            # <!--<script 讓真正的 </script> 失效，後半頁會被吞進 JSON
            dict(_col([0, 0, 0], ["<!--<script"] * 3, ["z"] * 3, kind="string", dtype="string"), name="s2"),
        ]
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        blob = page.split('<script type="application/json" id="fp-data">')[1].split("</script>")[0]
        assert "<" not in blob
        assert _blob(page)["cols"]["s2"][1] == ["<!--<script"] * 3
        # 只有兩個 <script>：內嵌的 JSON 與主程式
        assert page.count("</script>") == 2


def _corr_profile(tmp_path, *, thresholds=None):
    """有跑相關性的 profile：一組三欄的鏈（a 像 b、b 像 c）、一組負相關、各種沒算的原因。"""
    profile = _profile(tmp_path)
    profile["config"]["checks"] = {"distribution": False, "correlation": True}
    if thresholds is not None:
        profile["config"]["thresholds"] = thresholds
    ok = lambda name: dict(_col([0, 0, 0], [0, 0, 0], [9, 9, 9]), name=name)  # noqa: E731
    profile["columns"] += [ok(n) for n in ("dep_a", "dep_b", "dep_c", "login", "days", "loan", "flag")]
    profile["columns"].append(dict(_col([0, 0, 0], ["a"] * 3, ["z"] * 3, kind="string", dtype="string"), name="s"))
    profile["correlation"] = {
        "period_rows": 100, "rows": 100,
        "columns": ["half_empty", "dep_a", "dep_b", "dep_c", "login", "days", "loan", "flag"],
        "skipped": {"sys_version": "const", "s": "string"},
        "pairs": [["dep_a", "dep_b", 0.99, 100], ["dep_b", "dep_c", 0.97, 100], ["dep_a", "dep_c", 0.93, 100],
                  ["dep_a", "login", 0.6, 100], ["login", "days", -0.98, 100]],
        "sparse": {"loan": 40}, "flat": ["flag"], "few_pairs": 2, "flat_pairs": 0,
    }
    (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
    return profile


def _blob(page):
    return json.loads(page.split('<script type="application/json" id="fp-data">')[1].split("</script>")[0])


class TestCorrelationReport:
    def test_correlation_sections(self, tmp_path):
        write_report(_corr_profile(tmp_path), tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        high = _section_html(page, "s-corr-high")
        assert _names_in(high) == {"dep_a", "dep_b", "dep_c", "login", "days"}
        assert "（5 欄，2 組）" in high
        # 大的組在前；組內依表裡的欄位順序
        assert 'data-name="dep_a" data-grp="g0"' in high and 'data-name="days" data-grp="g1"' in high
        assert "-0.980" in high  # 負相關照實寫
        matrix = page.split('<section id="s-corr-matrix">')[1].split('<section id="s-corr-skip">')[0]
        assert matrix.count('class="cm"') == 1  # 兩欄一組的不畫
        cells = matrix.split('data-cm="0">')[1].split("</div>")[0]
        assert cells == ('<i class="d"></i><i class="h"></i><i class="b"></i>'
                         '<i class="h"></i><i class="d"></i><i class="h"></i>'
                         '<i class="b"></i><i class="h"></i><i class="d"></i>')
        skip = _section_html(page, "s-corr-skip")
        for name, why in (("loan", "sparse"), ("flag", "flat"), ("sys_version", "const"), ("s", "string")):
            assert f'data-name="{name}" data-why="{why}"' in skip, name
        assert _names_in(skip) == {"loan", "flag", "sys_version", "s"}
        assert "只有 40 筆有值" in skip and "的 2 對" in skip and "沒有變化的" not in skip
        assert ("2 組，最大的一組 3 欄；另有 4 欄沒算：字串欄位 1、最新一期只有一個值 1、"
                "抽到的筆數裡有值的太少 1、抽到的筆數裡只有一個值 1") in _section_html(page, "s-overview")

    def test_embedded_matrix_data_matches_the_drawing(self, tmp_path):
        write_report(_corr_profile(tmp_path), tmp_path, DEFAULT_THRESHOLDS, None)
        data = _blob((tmp_path / "index.html").read_text())
        assert data["corrKeep"] == 0.5
        (cm,) = data["cm"]
        assert cm["names"] == ["dep_a", "dep_b", "dep_c"]
        assert cm["r"][2] == cm["r"][6] == 0.93 and cm["n"][2] == 100
        assert cm["r"][0] is None  # 對角線

    def test_summary_keeps_one_per_group(self, tmp_path):
        write_report(_corr_profile(tmp_path), tmp_path, DEFAULT_THRESHOLDS, None)
        row = _section_html((tmp_path / "index.html").read_text(), "s-actions").split("<tr>")[-1]
        assert "高度相關的欄位每組留一欄" in row
        assert "<b>第 1 組</b>" in row and 'data-copy-text="dep_a\ndep_b\ndep_c"' in row
        assert 'data-copy-text="login\ndays"' in row

    def test_new_corr_threshold_changes_the_judgement(self, tmp_path):
        _corr_profile(tmp_path)
        runner = CliRunner()
        for corr, names, groups in (
            (None, {"dep_a", "dep_b", "dep_c", "login", "days"}, 2),
            ("0.98", {"dep_a", "dep_b", "login", "days"}, 2),            # 0.97 那組不算了
            ("0.6", {"dep_a", "dep_b", "dep_c", "login", "days"}, 1),    # 0.6 把兩組連成一組
        ):
            args = ["render", str(tmp_path)]
            if corr:
                cfg = tmp_path / "th.yaml"
                cfg.write_text(_GOOD + f"thresholds:\n  corr: {corr}\n")
                args += ["--config", str(cfg)]
            result = runner.invoke(app, args)
            assert result.exit_code == 0, result.output
            high = _section_html((tmp_path / "index.html").read_text(), "s-corr-high")
            assert _names_in(high) == names, corr
            assert f"，{groups} 組）" in high, corr
        cfg.write_text(_GOOD + "thresholds:\n  corr: 0.4\n")
        result = runner.invoke(app, ["render", str(tmp_path), "--config", str(cfg)])
        assert result.exit_code == 1 and "thresholds.corr" in result.output

    def test_profile_without_correlation_says_not_run(self, tmp_path):
        # 第二階段存的 profile.json：沒有 checks.correlation、沒有 correlation，門檻也沒有 corr
        profile = _dist_profile(tmp_path)
        profile["config"]["checks"] = {"distribution": True}
        profile["config"]["thresholds"] = {k: v for k, v in DEFAULT_THRESHOLDS.items() if k != "corr"}
        (tmp_path / PROFILE_FILENAME).write_text(json.dumps(profile))
        result = CliRunner().invoke(app, ["render", str(tmp_path)])
        assert result.exit_code == 0, result.output
        page = (tmp_path / "index.html").read_text()
        corr = _section_html(page, "s-corr")
        assert "這次沒有跑" in corr and "checks.correlation" in corr
        assert 'href="#s-corr-high"' not in page and "每組留一欄" not in page
        assert "cm" not in _blob(page)

    def test_new_null_ratio_moves_mostly_empty_columns_out_of_the_pairs(self, tmp_path):
        # 最新一期 60% 是空值：run 時照樣算；render 的 null_ratio 調到 0.5 時，跟〈空值〉一起變
        profile = _corr_profile(tmp_path)
        profile["columns"] += [dict(_col([0, 0, 60], [0, 0, 0], [9, 9, 9]), name="thin"),
                               dict(_col([0, 0, 0], [0, 0, 0], [9, 9, 9]), name="twin")]
        profile["correlation"]["pairs"].append(["thin", "twin", 0.99, 40])
        for null_ratio, high in ((0.99, True), (0.5, False)):
            write_report(profile, tmp_path, {**DEFAULT_THRESHOLDS, "null_ratio": null_ratio}, None)
            page = (tmp_path / "index.html").read_text()
            assert ("thin" in _names_in(_section_html(page, "s-corr-high"))) is high, null_ratio
            assert ('data-name="thin" data-why="null"' in _section_html(page, "s-corr-skip")) is not high
            assert ("twin" in _names_in(_section_html(page, "s-corr-high"))) is high

    def test_overview_without_skipped_columns(self, tmp_path):
        profile = _corr_profile(tmp_path)
        profile["correlation"].update(skipped={}, sparse={}, flat=[])
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        row = _section_html(page, "s-overview").split('href="#s-corr"')[1].split("</tr>")[0]
        assert "2 組，最大的一組 3 欄</td>" in row and "沒算" not in row and "（）" not in page

    def test_matrix_legend_follows_a_threshold_below_0_8(self, tmp_path):
        write_report(_corr_profile(tmp_path), tmp_path, {**DEFAULT_THRESHOLDS, "corr": 0.6}, None)
        matrix = (tmp_path / "index.html").read_text().split('<section id="s-corr-matrix">')[1].split(
            '<section id="s-corr-skip">')[0]
        assert "≥ 0.6，達門檻" in matrix and "0.5～0.6" in matrix and "0.8～" not in matrix
        assert 'class="b"' not in matrix.split('data-cm="0">')[1]  # 0.93 達門檻，不是 0.8～門檻那一段

    def test_no_numeric_column_to_correlate(self, tmp_path):
        profile = _corr_profile(tmp_path)
        profile["correlation"].update(columns=[], rows=0, pairs=[], sparse={}, flat=[], few_pairs=0)
        write_report(profile, tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        assert "沒有可以算相關係數的數值欄位" in _section_html(page, "s-corr") and "抽 0 筆" not in page

    def test_page_ids_and_wording(self, tmp_path):
        import re
        from collections import Counter

        write_report(_corr_profile(tmp_path), tmp_path, DEFAULT_THRESHOLDS, None)
        page = (tmp_path / "index.html").read_text()
        ids = re.findall(r'\sid="([^"]+)"', page)
        assert [i for i, n in Counter(ids).items() if n > 1] == []
        assert {"s-corr", "s-corr-high", "s-corr-matrix", "s-corr-skip", "t-corr-high", "t-corr-skip"} <= set(ids)
        for word in ("dataset", "training", "feature_selection", "drop_columns", "LightGBM", "→"):
            assert word not in page, word


# ---------------------------------------------------------------------------
# Spark: statistics
# ---------------------------------------------------------------------------


def _cfg(view, **kw):
    base = dict(text=_GOOD, table=view, time_col="t", key_cols=["k"], distribution=True)
    base.update(kw)
    return ProfileConfig(**base)


def _by_name(profile):
    return {c["name"]: c for c in profile["columns"]}


@pytest.mark.spark
class TestProfileTable:
    def test_nan_infinity_and_empty_string_count_as_empty(self, spark):
        rows = [
            ("2024-01", 1, 1.5, "a", True),
            ("2024-01", 2, float("nan"), "", False),
            ("2024-01", 3, float("inf"), None, None),
            ("2024-01", 4, float("-inf"), "a", True),
        ]
        df = spark.createDataFrame(rows, "t string, k int, d double, s string, b boolean")
        df.createOrReplaceTempView("fp_empty_values")
        profile = profile_table(spark, _cfg("fp_empty_values"), batch_size=10)
        cols = _by_name(profile)
        assert profile["periods"] == ["2024-01"] and profile["rows"] == [4]
        assert cols["d"]["null"] == [3]
        assert cols["d"]["min"] == [1.5] and cols["d"]["max"] == [1.5]
        assert cols["s"]["null"] == [2]
        assert cols["s"]["kind"] == "string"
        assert cols["b"]["kind"] == "string"
        assert cols["b"]["min"] == ["false"] and cols["b"]["max"] == ["true"]
        assert "k" not in cols and "t" not in cols

    def test_all_empty_period_and_one_value_column(self, spark):
        rows = [
            ("2024-01", 1, None, 3.0),
            ("2024-01", 2, None, 3.0),
            ("2024-02", 3, 10, 3.0),
            ("2024-02", 4, 20, 3.0),
        ]
        df = spark.createDataFrame(rows, "t string, k int, v bigint, c double")
        df.createOrReplaceTempView("fp_empty_period")
        profile = profile_table(spark, _cfg("fp_empty_period"), batch_size=10)
        cols = _by_name(profile)
        assert cols["v"]["null"] == [2, 0]
        assert cols["v"]["min"] == [None, 10] and cols["v"]["max"] == [None, 20]
        flags = judge(profile, DEFAULT_THRESHOLDS)
        assert flags["v"] == ["null_some"]
        assert flags["c"] == ["const_all"]

    def test_batches_give_the_same_stats(self, spark):
        rows = [("2024-01", i, i, i * 2, i * 0.5, "s") for i in range(5)]
        df = spark.createDataFrame(rows, "t string, k int, a int, b bigint, c double, s string")
        df.createOrReplaceTempView("fp_batches")
        one = profile_table(spark, _cfg("fp_batches"), batch_size=1)
        many = profile_table(spark, _cfg("fp_batches"), batch_size=100)
        assert one["columns"] == many["columns"]

    def test_decimal_and_unchecked_types(self, spark):
        df = spark.sql(
            "SELECT '2024-01' AS t, 1 AS k, CAST(12.50 AS DECIMAL(18,2)) AS dec_frac, "
            "CAST(12 AS DECIMAL(18,2)) AS dec_whole, DATE'2024-01-31' AS load_dt"
        )
        df.createOrReplaceTempView("fp_types")
        cols = _by_name(profile_table(spark, _cfg("fp_types"), batch_size=10))
        assert cols["dec_frac"]["max"] == [12.5]
        assert cols["dec_whole"]["min"] == [12.0]
        assert "has_decimal" not in cols["dec_frac"]  # 只給 float32 檢查用，已拿掉
        assert cols["load_dt"] == {"name": "load_dt", "dtype": "date", "kind": "unchecked"}

    def test_time_range_where_and_null_time_rows(self, spark):
        rows = [
            ("2024-01", 1, 1), ("2024-02", 2, 2), ("2024-03", 3, 3),
            ("2024-03", 4, 99), (None, 5, 5), (None, 6, 6),
        ]
        df = spark.createDataFrame(rows, "t string, k int, v int")
        df.createOrReplaceTempView("fp_filters")
        cfg = _cfg("fp_filters", time_start="2024-02", time_end="2024-03", where="v < 50")
        profile = profile_table(spark, cfg, batch_size=10)
        assert profile["periods"] == ["2024-02", "2024-03"]
        assert profile["rows"] == [1, 1]
        assert profile["null_time_rows"] == 2
        assert math.isclose(profile["config"]["thresholds"]["null_ratio"], 0.99)

    def test_basic_checks_only_has_no_distribution_stats(self, spark):
        spark.createDataFrame([("2024-01", 1, 1)], "t string, k int, v int").createOrReplaceTempView("fp_basic_only")
        profile = profile_table(spark, _cfg("fp_basic_only", distribution=False), batch_size=10)
        assert set(_by_name(profile)["v"]) == {"name", "dtype", "kind", "null", "min", "max"}
        assert profile["config"]["checks"] == {"distribution": False, "correlation": False}
        assert "correlation" not in profile


@pytest.mark.spark
class TestDistributionStats:
    def test_numeric_bins_use_base_period_deciles_with_upper_bound_included(self, spark):
        # 基準期 1～10：十分位數就是 1～9，組別是 (−∞,1]、(1,2]、…、(9,∞)，空值另一組
        rows = [("2024-01", i, float(i)) for i in range(1, 11)]
        rows += [("2024-02", 11, 1.0), ("2024-02", 12, 1.5), ("2024-02", 13, 2.0),
                 ("2024-02", 14, 9.0), ("2024-02", 15, 9.5), ("2024-02", 16, None), ("2024-02", 17, float("nan"))]
        spark.createDataFrame(rows, "t string, k int, v double").createOrReplaceTempView("fp_bins_num")
        col = _by_name(profile_table(spark, _cfg("fp_bins_num"), batch_size=10))["v"]
        assert col["bins"]["edges"] == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        assert col["bins"]["counts"][0] == [1] * 10 + [0]
        assert col["bins"]["counts"][1] == [1, 2, 0, 0, 0, 0, 0, 0, 1, 1, 2]  # NaN 也算空值
        q = col["quantiles"]
        assert (q[0][0], q[0][-1]) == (1.0, 10.0)  # p1、p99
        assert q[1] is not None and col["min"][1] == 1.0

    def test_equal_cut_points_merge(self, spark):
        # 基準期 17 個 0 與 5、6、7：p10～p80 都是 0、p90 是 5，合併後只剩兩個切點
        rows = [("2024-01", i, 0) for i in range(17)] + [("2024-01", 17, 5), ("2024-01", 18, 6), ("2024-01", 19, 7)]
        spark.createDataFrame(rows, "t string, k int, v int").createOrReplaceTempView("fp_bins_dup")
        col = _by_name(profile_table(spark, _cfg("fp_bins_dup"), batch_size=10))["v"]
        assert col["bins"]["edges"] == [0, 5]
        assert col["bins"]["counts"] == [[17, 1, 2, 0]]

    def test_string_bins_keep_the_base_period_top_20(self, spark):
        # 基準期 22 個值：v00 出現 3 次、v01 2 次，其他各 1 次；同樣次數依值排，取前 20 個
        base = [("2024-01", "v00")] * 3 + [("2024-01", "v01")] * 2 + [("2024-01", f"v{i:02d}") for i in range(2, 22)]
        later = [("2024-02", "v00"), ("2024-02", "v21"), ("2024-02", "new"), ("2024-02", ""), ("2024-02", None)]
        rows = [(t, k, v) for k, (t, v) in enumerate(base + later)]
        spark.createDataFrame(rows, "t string, k int, s string").createOrReplaceTempView("fp_bins_str")
        col = _by_name(profile_table(spark, _cfg("fp_bins_str"), batch_size=10))["s"]
        assert col["bins"]["values"] == [f"v{i:02d}" for i in range(20)]
        assert col["bins"]["counts"][0] == [3, 2] + [1] * 18 + [2, 0]
        assert col["bins"]["counts"][1] == [1] + [0] * 19 + [2, 2]  # v21、new 算其他；空字串也算空值
        assert "quantiles" not in col

    def test_empty_base_period_has_no_bins(self, spark):
        rows = [("2024-01", 1, None, None), ("2024-02", 2, 3, "a")]
        spark.createDataFrame(rows, "t string, k int, v int, s string").createOrReplaceTempView("fp_bins_empty")
        cols = _by_name(profile_table(spark, _cfg("fp_bins_empty"), batch_size=10))
        assert cols["v"]["bins"] is None and cols["s"]["bins"] is None
        assert cols["v"]["quantiles"][0] is None

    def test_batches_give_the_same_distribution_stats(self, spark):
        rows = [(f"2024-0{t}", t * 100 + i, i * t, f"s{i % 3}") for t in (1, 2) for i in range(30)]
        spark.createDataFrame(rows, "t string, k int, a int, s string").createOrReplaceTempView("fp_bins_batch")
        one = profile_table(spark, _cfg("fp_bins_batch"), batch_size=1)
        many = profile_table(spark, _cfg("fp_bins_batch"), batch_size=100)
        assert one["columns"] == many["columns"]
        assert set(one["pass_seconds"]) == {"basic", "top_values", "bins"}


@pytest.mark.spark
class TestCorrelationStats:
    def _table(self, spark, view):
        # 最早一期 a、b 反向；最新一期 a、b、c 同向，c 有 NaN、±Infinity；e 最新一期只有一個值
        early = [("2024-01", 10_000 + i, float(i), float(-i), 1.0, "x", float(i)) for i in range(50)]
        inf = float("inf")
        latest = [("2024-02", i, float(i), float(2 * i),
                   {0: float("nan"), 1: inf, 2: -inf}.get(i % 15, float(i)), "y", 5.0) for i in range(1500)]
        spark.createDataFrame(early + latest, "t string, k int, a double, b double, c double, d string, e double") \
            .createOrReplaceTempView(view)

    def test_latest_period_only_with_empty_values_cleaned(self, spark):
        self._table(spark, "fp_corr")
        profile = profile_table(spark, _cfg("fp_corr", distribution=False, correlation=True), batch_size=10)
        corr = profile["correlation"]
        assert corr["rows"] == corr["period_rows"] == 1500  # 不到 50,000 筆就全拿
        assert corr["skipped"] == {"d": "string", "e": "const"}
        assert corr["columns"] == ["a", "b", "c"]
        # 每 15 筆有 3 筆是 NaN 或 ±Infinity：c 剩 1,200 筆。在兩欄都有值的筆裡重排名次，所以剛好是 1
        assert corr["pairs"] == [["a", "b", 1.0, 1500], ["a", "c", 1.0, 1200], ["b", "c", 1.0, 1200]]
        assert set(profile["pass_seconds"]) == {"basic", "correlation"}

    def test_batches_give_the_same_correlation(self, spark):
        self._table(spark, "fp_corr_batch")
        cfg = _cfg("fp_corr_batch", distribution=False, correlation=True)
        one = profile_table(spark, cfg, batch_size=1)["correlation"]
        many = profile_table(spark, cfg, batch_size=100)["correlation"]
        assert one == many

    def test_sample_is_about_50000_rows_and_seeded(self, spark):
        from pyspark.sql import functions as F

        latest = spark.range(120_000).select(
            F.lit("2024-02").alias("t"), F.col("id").alias("k"), F.col("id").cast("double").alias("a"),
            (F.col("id") % 977).cast("double").alias("b"),
        )
        early = spark.range(10).select(F.lit("2024-01").alias("t"), (F.col("id") + 10**6).alias("k"),
                                       F.col("id").cast("double").alias("a"), F.col("id").cast("double").alias("b"))
        latest.unionByName(early).createOrReplaceTempView("fp_corr_sample")
        cfg = _cfg("fp_corr_sample", distribution=False, correlation=True)
        first = profile_table(spark, cfg, batch_size=1)["correlation"]
        second = profile_table(spark, cfg, batch_size=1)["correlation"]
        assert first["period_rows"] == 120_000
        assert 49_000 <= first["rows"] <= 51_000
        assert first == second  # seed 固定：同一張表抽到同一批


@pytest.mark.spark
class TestRunCommand:
    def test_config_problems_listed_at_once_then_exit(self, spark, tmp_path):
        spark.createDataFrame([("2024-01", 1, 1)], "t string, k int, v int") \
            .createOrReplaceTempView("fp_cli_bad")
        cfg = tmp_path / "bad.yaml"
        cfg.write_text(
            "table: fp_cli_bad\ntime_col: t\nexclude_cols: [gone]\n"
            "checks:\n  distribution: false\n  correlation: false\nthresholds:\n  corr: 0.3\n"
        )
        result = CliRunner().invoke(app, ["run", "--config", str(cfg)])
        assert result.exit_code == 1
        for needle in ("key_cols", "gone", "thresholds.corr 要介於"):
            assert needle in result.output, result.output

    def test_run_writes_profile_and_report(self, spark, tmp_path):
        rows = [("2024-01", i, i, None) for i in range(3)] + \
               [("2024-02", i, i, None) for i in range(3)]
        spark.createDataFrame(rows, "t string, k int, v int, dead double") \
            .createOrReplaceTempView("fp_cli_ok")
        out = tmp_path / "out"
        cfg = tmp_path / "ok.yaml"
        cfg.write_text(
            f"table: fp_cli_ok\ntime_col: t\nkey_cols: [k]\noutput_dir: {out}\n"
            "checks:\n  distribution: false\n  correlation: true\n"
        )
        result = CliRunner().invoke(app, ["run", "--config", str(cfg)])
        assert result.exit_code == 0, result.output
        profile = json.loads((out / PROFILE_FILENAME).read_text())
        assert profile["periods"] == ["2024-01", "2024-02"]
        assert profile["correlation"]["skipped"] == {"dead": "empty"}
        assert set(profile["pass_seconds"]) == {"basic", "correlation"}
        page = (out / "index.html").read_text()
        assert 'data-name="dead"' in _section_html(page, "s-null-all")
        assert 'data-name="dead" data-why="empty"' in _section_html(page, "s-corr-skip")
