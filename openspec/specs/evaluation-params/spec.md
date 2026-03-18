### Requirement: Evaluation parameters YAML configuration
The system SHALL support a `conf/base/parameters_evaluation.yaml` configuration file with the following structure:

```yaml
evaluation:
  k_values: [5, "all"]
  segment_columns:
    - cust_segment_typ
  segment_sources:
    <segment_name>:
      filepath: <path_to_parquet>
      key_columns: [cust_id, snap_date]
      segment_column: <column_name>
  holding_combo:
    top_n: 10
```

- `k_values`: list of integers or the string `"all"`. `"all"` SHALL be resolved at runtime to the total number of unique products (N).
- `segment_columns`: list of column names already present in the labels DataFrame.
- `segment_sources`: dict of external segment data sources, each specifying a Parquet file path, join key columns, and the segment column name.
- `holding_combo.top_n`: integer controlling the top-N frequency filtering for holding combo segments.

#### Scenario: Default configuration
- **WHEN** `parameters_evaluation.yaml` contains `k_values: [5, "all"]`
- **THEN** the evaluation computes metrics with K=5 and K=N (where N is the total product count)

#### Scenario: Configuration file not found
- **WHEN** `parameters_evaluation.yaml` does not exist at the expected path
- **THEN** the system uses built-in defaults: `k_values=[5, "all"]`, `segment_columns=["cust_segment_typ"]`, no `segment_sources`

#### Scenario: Multiple segment sources
- **WHEN** `segment_sources` defines two sources (e.g., `holding_combo` and `risk_level`)
- **THEN** both external Parquet files are loaded and joined to labels, and both segment columns are analyzed

### Requirement: CLI parameter override
The CLI `--k-values` flag SHALL override the `k_values` setting from `parameters_evaluation.yaml`. The CLI SHALL accept an optional `--params-file` flag (default: `conf/base/parameters_evaluation.yaml`) to specify the evaluation parameters file path.

#### Scenario: CLI overrides YAML k_values
- **WHEN** YAML sets `k_values: [5, "all"]` and CLI passes `--k-values 3,10`
- **THEN** the evaluation uses `k_values=[3, 10]` (CLI wins)

#### Scenario: Custom params file path
- **WHEN** `--params-file conf/production/parameters_evaluation.yaml` is specified
- **THEN** the system loads evaluation parameters from the specified path
