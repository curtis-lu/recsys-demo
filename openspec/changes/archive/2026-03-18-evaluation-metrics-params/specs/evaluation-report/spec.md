## MODIFIED Requirements

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
- **WHEN** sections include metrics summary (Overall + Macro Average + Micro Average tables), score distribution, rank distribution, calibration, and segment analysis for each configured segment column
- **THEN** HTML contains all sections with proper headings and navigation

#### Scenario: Metrics Summary with three tables
- **WHEN** the Metrics Summary section is rendered
- **THEN** it contains three tables: Overall (single column), Macro Average (columns per dimension), and Micro Average (columns per dimension)

#### Scenario: Macro/Micro table structure
- **WHEN** Macro Average or Micro Average table is rendered with dimensions by_product and by_cust_segment_typ
- **THEN** table rows are metric names (map, ndcg, ndcg@5, ndcg@N, precision@5, precision@N, recall@5, recall@N, mrr) and columns are dimension names (by_product, by_cust_segment_typ, etc.)
