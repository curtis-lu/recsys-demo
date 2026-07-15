"""把 study 渲染成 5 張自足 HTML 圖，每張各自 best-effort，dir 內共用一份 plotly.min.js。"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# (輸出檔名, optuna.visualization 函式名)
_CHARTS = [
    ("optimization_history.html", "plot_optimization_history"),
    ("param_importances.html", "plot_param_importances"),
    ("slice.html", "plot_slice"),
    ("contour.html", "plot_contour"),
    ("parallel_coordinate.html", "plot_parallel_coordinate"),
]


def render_charts(study, out_dir) -> list[str]:
    """Render 5 charts into out_dir。每張 best-effort：任一張失敗（例如完成 trial <2 時
    param_importances raise）只記 warning、跳過該張，不影響其餘。回傳實際寫出的檔名 list。

    用 include_plotlyjs="directory"：dir 內共用一份 plotly.min.js，各 HTML 只剩 KB 級、
    且離線可看（不觸網）。"""
    from optuna import visualization as viz

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for filename, fn_name in _CHARTS:
        try:
            fig = getattr(viz, fn_name)(study)
            fig.write_html(str(out_dir / filename), include_plotlyjs="directory")
            written.append(filename)
        except Exception:
            logger.warning("HPO chart %s skipped", filename, exc_info=True)
    return written
