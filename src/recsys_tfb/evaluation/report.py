"""HTML report generation for evaluation results."""

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.offline


@dataclass
class ReportSection:
    """A section in the evaluation report."""

    title: str
    description: str
    figures: list[go.Figure] = field(default_factory=list)
    tables: list[pd.DataFrame] = field(default_factory=list)
    table_titles: list[str] = field(default_factory=list)


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
        "nav { background: #f5f5f5; padding: 12px; border-radius: 4px; margin-bottom: 24px; }",
        "nav a { margin-right: 16px; color: #0066cc; text-decoration: none; }",
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

    # Navigation
    html_parts.append("<nav>")
    for i, section in enumerate(sections):
        section_id = f"section-{i}"
        html_parts.append(f'<a href="#{section_id}">{section.title}</a>')
    html_parts.append("</nav>")

    # Sections
    for i, section in enumerate(sections):
        section_id = f"section-{i}"
        html_parts.append(f'<div class="section" id="{section_id}">')
        html_parts.append(f"<h2>{section.title}</h2>")
        html_parts.append(f'<p class="description">{section.description}</p>')

        # Figures
        for fig in section.figures:
            fig_html = fig.to_html(
                full_html=False, include_plotlyjs=False
            )
            html_parts.append(fig_html)

        # Tables
        for ti, table in enumerate(section.tables):
            if ti < len(section.table_titles) and section.table_titles[ti]:
                html_parts.append(f"<h3>{section.table_titles[ti]}</h3>")
            html_parts.append(table.to_html(classes="metrics-table", index=True))

        html_parts.append("</div>")

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
