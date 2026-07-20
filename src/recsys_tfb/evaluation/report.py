"""HTML report generation for evaluation results."""

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.offline

from recsys_tfb.report.pages import _render_section_extras
from recsys_tfb.report.types import ReportSection  # noqa: F401


def _fmt_no_sci(x: float) -> str:
    """Format a float for report tables without scientific notation.

    Integer-valued -> thousands-separated integer (``12,345,678``); otherwise
    fixed 6-decimal with trailing zeros stripped (``0.000034``, ``0.5``).
    Magnitudes below display precision collapse to ``"0"`` and never render as
    ``"-0"``.
    """
    x = float(x)
    if x.is_integer():
        return f"{int(x):,}"
    s = f"{x:,.6f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "-", "") else s


def _fmt_cell(x):
    """Per-cell formatter for report tables. Numbers go through the
    no-scientific-notation rules (big ints also get thousands separators),
    NaN/None render blank, and non-numeric cells (strings, lists) pass through
    unchanged."""
    if x is None or x is pd.NA:
        return ""
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, (int, np.integer)):
        return f"{int(x):,}"
    if isinstance(x, (float, np.floating)):
        if math.isnan(x):
            return ""
        return _fmt_no_sci(float(x))
    return x


def _render_table(table: pd.DataFrame) -> str:
    """Render a DataFrame to HTML with no scientific notation.

    pandas formats a whole float column uniformly, so one extreme value flips
    the entire column to exponential. Formatting every cell up front sidesteps
    that (and keeps object-dtype columns working too). ``applymap`` is used
    because ``DataFrame.map`` does not exist on pandas 1.5.x.
    """
    return table.applymap(_fmt_cell).to_html(index=True)


def generate_html_report(
    sections: list[ReportSection],
    title: str = "Model Evaluation Report",
    metadata: dict | None = None,
) -> str:
    """Generate a self-contained HTML report with embedded Plotly charts.

    Args:
        sections: List of ReportSection objects.
        title: Report title.
        metadata: Optional metadata dict to display at the top.

    Returns:
        Complete HTML string.
    """
    plotly_js = plotly.offline.get_plotlyjs()

    html_parts = [
        "<!DOCTYPE html>",
        "<html><head>",
        '<meta charset="utf-8">',
        f"<title>{title}</title>",
        "<style>",
        "body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 40px; }",
        "h1 { color: #333; }",
        "h2 { color: #555; border-bottom: 1px solid #ddd; padding-bottom: 8px; }",
        "table { border-collapse: collapse; margin: 16px 0; }",
        "th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }",
        "th { background: #f5f5f5; }",
        "tr:nth-child(even) { background: #fafafa; }",
        ".metadata { background: #f0f4f8; padding: 16px; border-radius: 8px; margin-bottom: 24px; }",
        ".section { margin-bottom: 40px; }",
        ".description { color: #666; margin-bottom: 16px; }",
        # 公式用等寬字＋淺底，與敘述區分開；純 Unicode 文字，不引入 MathJax／
        # KaTeX——生產限制是 no network，外部 CDN 一定載不到。
        '.formula { font-family: "SF Mono", Menlo, Consolas, monospace; '
        "background: #f5f7fa; border-left: 3px solid #94a3b8; "
        "padding: 10px 16px; margin: 12px 0 16px; border-radius: 2px; "
        "font-size: 0.95em; color: #1f2937; overflow-x: auto; "
        "white-space: pre-wrap; }",
        ".section-bullets { margin: 12px 0 16px; padding-left: 22px; "
        "line-height: 1.7; }",
        ".section-bullets li { margin: 4px 0; color: #444; }",
        "nav.toc { background: #f5f5f5; padding: 12px 20px; border-radius: 4px; margin-bottom: 24px; }",
        "nav.toc strong { display: block; margin-bottom: 6px; color: #444; }",
        "nav.toc ul { margin: 4px 0 0; padding-left: 20px; }",
        "nav.toc li { margin: 3px 0; }",
        "nav.toc a { color: #0066cc; text-decoration: none; }",
        "nav.toc a:hover { text-decoration: underline; }",
        "details > summary { font-size: 1.5em; color: #555; cursor: pointer; margin: 24px 0 8px; }",
        "#to-top { position: fixed; bottom: 24px; right: 24px; "
        "background: rgba(0, 102, 204, 0.85); color: white; border: none; "
        "border-radius: 50%; width: 44px; height: 44px; font-size: 20px; "
        "cursor: pointer; box-shadow: 0 2px 8px rgba(0,0,0,0.2); "
        "opacity: 0; pointer-events: none; transition: opacity 0.2s; "
        "z-index: 1000; }",
        "#to-top.visible { opacity: 1; pointer-events: auto; }",
        "</style>",
        f"<script>{plotly_js}</script>",
        "</head><body>",
        f"<h1>{title}</h1>",
    ]

    # Metadata
    if metadata:
        html_parts.append('<div class="metadata">')
        html_parts.append("<table>")
        for key, value in metadata.items():
            html_parts.append(f"<tr><th>{key}</th><td>{value}</td></tr>")
        html_parts.append("</table></div>")

    # Navigation (vertical TOC; 9+ sections need a scannable list)
    html_parts.append('<nav class="toc">')
    html_parts.append("<strong>目錄 / Sections</strong>")
    html_parts.append("<ul>")
    for i, section in enumerate(sections):
        section_id = f"section-{i}"
        html_parts.append(
            f'<li><a href="#{section_id}">{section.title}</a></li>'
        )
    html_parts.append("</ul>")
    html_parts.append("</nav>")

    # Sections
    for i, section in enumerate(sections):
        section_id = f"section-{i}"
        if section.collapsible:
            html_parts.append(f'<details class="section" id="{section_id}">')
            html_parts.append(f"<summary>{section.title}</summary>")
        else:
            html_parts.append(f'<div class="section" id="{section_id}">')
            html_parts.append(f"<h2>{section.title}</h2>")
        html_parts.append(f'<p class="description">{section.description}</p>')
        # 與 report/pages.py 共用同一個 helper：兩邊各寫一份的話，之後只改其中
        # 一邊，另一邊會靜默丟掉 formula/bullets 而不報任何錯。
        html_parts.extend(_render_section_extras(section))

        for fig in section.figures:
            html_parts.append(
                fig.to_html(full_html=False, include_plotlyjs=False)
            )

        for ti, table in enumerate(section.tables):
            if ti < len(section.table_titles) and section.table_titles[ti]:
                html_parts.append(f"<h3>{section.table_titles[ti]}</h3>")
            html_parts.append(_render_table(table))

        html_parts.append("</details>" if section.collapsible else "</div>")

    # Floating back-to-top button + JS handlers.
    html_parts.append(
        '<button id="to-top" title="Back to top" aria-label="Back to top">↑</button>'
    )
    html_parts.append("<script>")
    html_parts.append(
        "(function(){"
        "var btn = document.getElementById('to-top');"
        "window.addEventListener('scroll', function(){"
        "btn.classList.toggle('visible', window.scrollY > 300);"
        "});"
        "btn.addEventListener('click', function(){"
        "window.scrollTo({top: 0, behavior: 'smooth'});"
        "});"
        # Anchor jumping into a closed <details> should auto-open it,
        # else the nav link visually 'does nothing' on collapsible sections.
        "function openTargetDetails(){"
        "var hash = location.hash;"
        "if (!hash) return;"
        "var el = document.querySelector(hash);"
        "if (el && el.tagName === 'DETAILS') { el.open = true; }"
        "}"
        "document.querySelectorAll('nav.toc a').forEach(function(a){"
        "a.addEventListener('click', function(){"
        "setTimeout(openTargetDetails, 0);"
        "});"
        "});"
        "window.addEventListener('hashchange', openTargetDetails);"
        "openTargetDetails();"  # also runs on initial load if URL has hash
        "})();"
    )
    html_parts.append("</script>")
    html_parts.append("</body></html>")

    return "\n".join(html_parts)


def save_report(html: str, output_dir: str | Path) -> Path:
    """Write report.html to output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "report.html"
    path.write_text(html, encoding="utf-8")
    return path


def save_metrics_json(metrics: dict, output_dir: str | Path) -> Path:
    """Write metrics.json to output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "metrics.json"
    path.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    return path
