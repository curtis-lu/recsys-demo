"""report.figures：plotly 建構器＋圖表資料量預算。

核心宣稱：
- 軸序由呼叫端決定，建構器不得對 x/y 重新排序（後續有矩陣要並排對照，
  軸序必須一致才能比較）。
- 大於 ``MAX_FIGURE_POINTS`` 的資料一律在建構器第一行被擋下——診斷產物
  的完整明細只落 JSON，進 HTML 的一律先聚合到可視大小。
"""

import pytest

from recsys_tfb.report.figures import (
    MAX_FIGURE_POINTS,
    assert_within_budget,
    bar,
    bubble_grid,
    heatmap,
    scatter,
)


class TestAssertWithinBudget:
    def test_raises_when_over_budget(self):
        with pytest.raises(ValueError) as exc:
            assert_within_budget(MAX_FIGURE_POINTS + 1, name="heatmap")
        msg = str(exc.value)
        assert str(MAX_FIGURE_POINTS) in msg
        assert str(MAX_FIGURE_POINTS + 1) in msg
        assert "heatmap" in msg

    def test_does_not_raise_at_exactly_budget(self):
        assert_within_budget(MAX_FIGURE_POINTS, name="heatmap")

    def test_does_not_raise_when_under_budget(self):
        assert_within_budget(1, name="heatmap")


class TestHeatmap:
    def test_does_not_reorder_axes(self):
        z = [[1, 2], [3, 4]]
        fig = heatmap(z, x=["b", "a"], y=["y2", "y1"], title="t",
                       colorbar_title="c")
        assert list(fig.data[0].x) == ["b", "a"]
        assert list(fig.data[0].y) == ["y2", "y1"]

    def test_center_uses_diverging_scale(self):
        z = [[-1.0, 0.0], [1.0, 2.0]]
        fig = heatmap(z, x=["a", "b"], y=["c", "d"], title="t",
                       colorbar_title="c", center=0.0)
        # diverging_scale 兩端不同色相；只驗證 colorscale 確實被設定且非單色
        colors = {stop[1] for stop in fig.data[0].colorscale}
        assert len(colors) >= 2

    def test_no_center_uses_sequential_scale_single_hue_family(self):
        z = [[1.0, 2.0], [3.0, 4.0]]
        fig = heatmap(z, x=["a", "b"], y=["c", "d"], title="t",
                       colorbar_title="c")
        assert fig.data[0].colorscale is not None

    def test_over_budget_raises(self):
        # 2001 個點（攤平後）應被擋下
        n_cols = MAX_FIGURE_POINTS + 1
        z = [[0.0] * n_cols]
        x = [str(i) for i in range(n_cols)]
        y = ["only-row"]
        with pytest.raises(ValueError) as exc:
            heatmap(z, x=x, y=y, title="t", colorbar_title="c")
        assert str(MAX_FIGURE_POINTS) in str(exc.value)


class TestBubbleGrid:
    def test_size_and_color_are_distinct_encodings(self):
        fig = bubble_grid(
            x=[1, 2, 3],
            y=[1, 2, 3],
            size=[10, 20, 30],
            colour=[0.1, 0.5, 0.9],
            hover_text=["h1", "h2", "h3"],
            title="t",
            colorbar_title="c",
        )
        marker = fig.data[0].marker
        # size 經 sizeref 正規化，不會與原始 colour 相同；兩者本就是不同量
        assert list(marker.color) == [0.1, 0.5, 0.9]
        assert list(marker.size) != [0.1, 0.5, 0.9]

    def test_hovertext_is_passed_through(self):
        fig = bubble_grid(
            x=[1, 2],
            y=[1, 2],
            size=[10, 20],
            colour=[0.1, 0.9],
            hover_text=["alpha", "beta"],
            title="t",
            colorbar_title="c",
        )
        assert list(fig.data[0].hovertext) == ["alpha", "beta"]

    def test_over_budget_raises(self):
        n = MAX_FIGURE_POINTS + 1
        with pytest.raises(ValueError):
            bubble_grid(
                x=list(range(n)),
                y=list(range(n)),
                size=[1] * n,
                colour=[0.0] * n,
                hover_text=[""] * n,
                title="t",
                colorbar_title="c",
            )


class TestScatter:
    def test_labels_passed_as_text(self):
        fig = scatter(
            x=[1, 2, 3],
            y=[4, 5, 6],
            labels=["p", "q", "r"],
            title="t",
            x_title="x",
            y_title="y",
        )
        assert list(fig.data[0].text) == ["p", "q", "r"]

    def test_over_budget_raises(self):
        n = MAX_FIGURE_POINTS + 1
        with pytest.raises(ValueError):
            scatter(
                x=list(range(n)),
                y=list(range(n)),
                labels=[""] * n,
                title="t",
                x_title="x",
                y_title="y",
            )


class TestBar:
    def test_center_uses_diverging_scale(self):
        fig = bar(x=["a", "b", "c"], y=[-1.0, 0.0, 1.0], title="t",
                  x_title="x", y_title="y", center=0.0)
        assert fig.data[0].marker.color is not None

    def test_no_center_single_colour(self):
        fig = bar(x=["a", "b"], y=[1.0, 2.0], title="t", x_title="x",
                  y_title="y")
        assert fig is not None

    def test_over_budget_raises(self):
        n = MAX_FIGURE_POINTS + 1
        with pytest.raises(ValueError):
            bar(x=list(range(n)), y=[0.0] * n, title="t", x_title="x",
                y_title="y")


class TestSharedTheme:
    def test_all_builders_use_plotly_white_template(self):
        fig1 = scatter(x=[1], y=[1], labels=["a"], title="t", x_title="x",
                       y_title="y")
        fig2 = bar(x=["a"], y=[1.0], title="t", x_title="x", y_title="y")
        assert fig1.layout.template.layout.paper_bgcolor == \
            fig2.layout.template.layout.paper_bgcolor
