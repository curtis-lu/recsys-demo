"""report.scales：色階只編碼資料大小/正負，結構上不提供 good/bad 配色。"""

import pytest

from recsys_tfb.report.scales import diverging_scale, sequential_scale


class TestSequentialScale:
    def test_has_at_least_three_stops(self):
        stops = sequential_scale()
        assert len(stops) >= 3

    def test_first_and_last_positions(self):
        stops = sequential_scale()
        assert stops[0][0] == 0.0
        assert stops[-1][0] == 1.0

    def test_positions_strictly_increasing(self):
        stops = sequential_scale()
        positions = [p for p, _ in stops]
        assert positions == sorted(positions)
        assert len(set(positions)) == len(positions)

    def test_single_hue_light_to_dark(self):
        # 單一色相：所有停駐點應共用同一組 hue 家族（此處以「非全部同色」與
        # 「首尾顏色不同」佐證漸層，不做嚴格色彩科學驗證）
        stops = sequential_scale()
        colors = [c for _, c in stops]
        assert len(set(colors)) == len(colors)  # 每個停駐點顏色不同（漸層）


class TestDivergingScale:
    def test_default_midpoint_is_half(self):
        stops = diverging_scale()
        positions = [p for p, _ in stops]
        assert 0.5 in positions

    def test_first_and_last_positions(self):
        stops = diverging_scale()
        assert stops[0][0] == 0.0
        assert stops[-1][0] == 1.0

    def test_positions_strictly_increasing(self):
        stops = diverging_scale()
        positions = [p for p, _ in stops]
        assert positions == sorted(positions)
        assert len(set(positions)) == len(positions)

    def test_unique_midpoint(self):
        stops = diverging_scale()
        positions = [p for p, _ in stops]
        # 中點只有一個（不是頭尾之外有兩個一樣接近 0.5 的停駐點）
        assert positions.count(0.5) == 1

    def test_center_normalized_within_lo_hi_range(self):
        stops = diverging_scale(center=1.0, lo=0.0, hi=3.0)
        positions = [p for p, _ in stops]
        mid = 1.0 / 3.0
        assert any(abs(p - mid) < 1e-9 for p in positions)
        assert positions[0] == 0.0
        assert positions[-1] == 1.0

    def test_neutral_color_at_computed_midpoint(self):
        stops = diverging_scale(center=1.0, lo=0.0, hi=3.0)
        mid = 1.0 / 3.0
        mid_matches = [c for p, c in stops if abs(p - mid) < 1e-9]
        assert len(mid_matches) == 1

    def test_endpoints_use_different_hues(self):
        stops = diverging_scale()
        assert stops[0][1] != stops[-1][1]

    def test_hi_must_be_greater_than_lo(self):
        with pytest.raises(ValueError) as exc:
            diverging_scale(center=0.0, lo=1.0, hi=1.0)
        msg = str(exc.value)
        assert "1.0" in msg

    def test_hi_less_than_lo_raises(self):
        with pytest.raises(ValueError) as exc:
            diverging_scale(center=0.0, lo=2.0, hi=1.0)
        msg = str(exc.value)
        assert "2.0" in msg and "1.0" in msg
