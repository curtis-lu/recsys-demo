## ADDED Requirements

### Requirement: HTML report generation
evaluation/report.py SHALL provide `generate_html_report(sections, title, metadata)` that:
1. Accepts a list of ReportSection objects (title, description, figures, tables)
2. Produces a single self-contained HTML string with all Plotly figures and HTML tables
3. Embeds plotly.js inline using `plotly.offline.get_plotlyjs()` (offline-capable)
4. Includes metadata table at top (model version, snap date, timestamp, customer/product counts)

#### Scenario: Self-contained HTML
- **WHEN** HTML report is generated
- **THEN** the HTML file can be opened in a browser without internet connection

#### Scenario: All sections present
- **WHEN** sections include metrics summary, score distribution, rank distribution, calibration, segments
- **THEN** HTML contains all sections with proper headings and navigation

#### Scenario: Interactive Plotly charts
- **WHEN** HTML is opened in browser
- **THEN** all charts support Plotly interactions (zoom, hover, toggle traces)

### Requirement: Report file saving
evaluation/report.py SHALL provide:
- `save_report(html, output_dir) -> Path` — writes report.html
- `save_metrics_json(metrics, output_dir) -> Path` — writes metrics.json

#### Scenario: Directory creation
- **WHEN** output_dir does not exist
- **THEN** directories are created automatically

#### Scenario: JSON roundtrip
- **WHEN** metrics are saved as JSON
- **THEN** reading the JSON back produces identical dict structure

### Requirement: ReportSection dataclass
evaluation/report.py SHALL define a ReportSection dataclass with fields:
- title: str
- description: str
- figures: list[go.Figure]
- tables: list[pd.DataFrame]

#### Scenario: Section with figures and tables
- **WHEN** a ReportSection has 2 figures and 1 table
- **THEN** HTML renders both figures and the table within that section
