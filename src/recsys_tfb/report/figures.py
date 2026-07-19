"""中性呈現層的 plotly 建構器：只負責「怎麼畫」，不做任何資料判斷。

每個建構器都是純函式：輸入資料＋顯示參數，輸出 ``go.Figure``。沒有任何一個
建構器會替呼叫端排序、篩選或聚合資料——座標軸順序、顯示的量，全由呼叫端
決定。這點對 ``heatmap`` 特別重要：後續會有兩張矩陣並排對照（例如「舊
config」vs「新 config」），兩張圖的軸序必須完全一致才能逐格比較，若這裡
偷偷排序，對照就毀了。

``assert_within_budget`` 是硬規則的執行點：診斷產物會拆成多份 HTML，完整
明細只落 JSON，進 HTML 的一律先聚合到 ``MAX_FIGURE_POINTS`` 以內。這不是
「建議聚合」，是每個建構器第一行都會擋下超量呼叫。
"""
from __future__ import annotations

from typing import Sequence

import numpy as np
import plotly.graph_objects as go

from recsys_tfb.report.scales import diverging_scale, sequential_scale

MAX_FIGURE_POINTS = 2000

# 共用主題：全部建構器套同一份，避免每個函式各寫一份而慢慢漂移。
_TEMPLATE = "plotly_white"
_FONT_SIZE = 13
_MARGIN = dict(l=60, r=40, t=60, b=50)


def assert_within_budget(n: int, name: str) -> None:
    """``n`` 超過 ``MAX_FIGURE_POINTS`` 時 raise，訊息含門檻／實際值／呼叫者名稱。

    這是硬規則，不是建議：進 HTML 的資料量一律先聚合，完整明細只落 JSON。
    """
    if n > MAX_FIGURE_POINTS:
        raise ValueError(
            f"{name}: {n} points exceeds MAX_FIGURE_POINTS ({MAX_FIGURE_POINTS}); "
            "aggregate before rendering into HTML — full detail belongs in JSON only."
        )


def _finite_bounds(arr: np.ndarray, name: str) -> tuple[float, float]:
    """算 ``arr`` 中有限值的 ``(min, max)``，供 ``diverging_scale`` 的 ``lo``/``hi``。

    空輸入與「全部非有限值（NaN/inf）」都 raise 明確訊息——這兩種是壞資料
    （沒有任何訊號可畫色階），跟「所有有限值恰好相同」（良性退化，見
    ``scales.diverging_scale`` docstring）不是同一件事，不能用同一個標準
    處理。部分 NaN（混著有限值）則忽略 NaN、只用有限值算範圍：那些 NaN
    格在圖上會自然顯示為空白，範圍計算不該被它們拖垮。
    """
    if arr.size == 0:
        raise ValueError(
            f"{name}: cannot build a color scale from empty input (0 values)"
        )
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        raise ValueError(
            f"{name}: all {arr.size} value(s) are NaN/inf — cannot build a "
            "color scale from non-finite input; caller must filter to finite "
            "values before calling"
        )
    return float(finite.min()), float(finite.max())


def _apply_theme(fig: go.Figure, title: str) -> go.Figure:
    fig.update_layout(
        template=_TEMPLATE,
        title=title,
        font=dict(size=_FONT_SIZE),
        margin=_MARGIN,
    )
    return fig


def heatmap(
    z: Sequence[Sequence[float]],
    x: Sequence,
    y: Sequence,
    title: str,
    colorbar_title: str,
    *,
    center: float | None = None,
) -> go.Figure:
    """矩陣熱圖。``x``/``y`` 的順序原樣保留，絕不重新排序。"""
    z_arr = np.asarray(z, dtype=float)
    flat = z_arr.ravel()
    assert_within_budget(flat.size, name="heatmap")

    if center is not None:
        lo, hi = _finite_bounds(flat, name="heatmap")
        colorscale = diverging_scale(center=center, lo=lo, hi=hi)
    else:
        colorscale = sequential_scale()

    fig = go.Figure(
        data=go.Heatmap(
            z=z_arr,
            x=list(x),
            y=list(y),
            colorscale=colorscale,
            colorbar=dict(title=colorbar_title),
        )
    )
    return _apply_theme(fig, title)


def bubble_grid(
    x: Sequence,
    y: Sequence,
    size: Sequence[float],
    colour: Sequence[float],
    hover_text: Sequence[str],
    title: str,
    colorbar_title: str,
    *,
    center: float = 1.0,
) -> go.Figure:
    """氣泡圖：大小＝樣本量，顏色＝關聯強度——刻意用兩個不同的量編碼。"""
    size_arr = np.asarray(size, dtype=float)
    colour_arr = np.asarray(colour, dtype=float)
    assert_within_budget(len(x), name="bubble_grid")

    max_size = float(np.max(size_arr)) if size_arr.size else 0.0
    sizeref = (2.0 * max_size / (40.0 ** 2)) if max_size > 0 else 1.0

    lo, hi = _finite_bounds(colour_arr, name="bubble_grid")
    colorscale = diverging_scale(center=center, lo=lo, hi=hi)

    fig = go.Figure(
        data=go.Scatter(
            x=list(x),
            y=list(y),
            mode="markers",
            hovertext=list(hover_text),
            marker=dict(
                size=size_arr,
                sizeref=sizeref if sizeref > 0 else 1.0,
                sizemode="area",
                color=colour_arr,
                colorscale=colorscale,
                colorbar=dict(title=colorbar_title),
            ),
        )
    )
    return _apply_theme(fig, title)


def scatter(
    x: Sequence[float],
    y: Sequence[float],
    labels: Sequence[str],
    title: str,
    x_title: str,
    y_title: str,
) -> go.Figure:
    """散佈圖，附標籤（hover 同時顯示標籤與兩軸值）。"""
    assert_within_budget(len(x), name="scatter")

    fig = go.Figure(
        data=go.Scatter(
            x=list(x),
            y=list(y),
            mode="markers",
            text=list(labels),
            hovertemplate=(
                "%{text}<br>" + x_title + "=%{x}<br>" + y_title + "=%{y}<extra></extra>"
            ),
        )
    )
    fig.update_layout(xaxis_title=x_title, yaxis_title=y_title)
    return _apply_theme(fig, title)


def bar(
    x: Sequence,
    y: Sequence[float],
    title: str,
    x_title: str,
    y_title: str,
    *,
    center: float | None = None,
) -> go.Figure:
    """長條圖。``center`` 給定時（有號量）用發散色階，否則單色。"""
    y_arr = np.asarray(y, dtype=float)
    assert_within_budget(len(x), name="bar")

    if center is not None:
        lo, hi = _finite_bounds(y_arr, name="bar")
        colorscale = diverging_scale(center=center, lo=lo, hi=hi)
        marker = dict(
            color=y_arr,
            colorscale=colorscale,
        )
    else:
        marker = dict(color=sequential_scale()[1][1])

    fig = go.Figure(data=go.Bar(x=list(x), y=y_arr, marker=marker))
    fig.update_layout(xaxis_title=x_title, yaxis_title=y_title)
    return _apply_theme(fig, title)
