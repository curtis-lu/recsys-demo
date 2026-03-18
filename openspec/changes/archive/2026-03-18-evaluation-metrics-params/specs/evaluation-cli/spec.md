## MODIFIED Requirements

### Requirement: Analyze subcommand
scripts/evaluate_model.py SHALL provide an `analyze` subcommand (Typer) that:
1. Accepts: model_version (positional, supports hash/latest/best), --snap-date (required), --data-dir (default "data/"), --k-values (optional, overrides YAML), --params-file (default "conf/base/parameters_evaluation.yaml")
2. Loads parameters_evaluation.yaml for k_values, segment_columns, and segment_sources configuration
3. Loads ranked_predictions.parquet, label_table.parquet
4. Loads and joins external segment sources via `load_and_join_segment_sources`
5. Resolves model_version via `core/versioning.resolve_model_version()` for latest/best aliases
6. Runs all analysis dimensions (metrics, distributions, calibration, segments for each configured column)
7. Generates HTML report with Metrics Summary showing Overall, Macro Average, and Micro Average tables
8. Saves report and metrics.json to `data/evaluation/{model_version}/{snap_date}/`
9. Prints summary metrics to terminal

#### Scenario: Analyze with YAML parameters
- **WHEN** `python scripts/evaluate_model.py analyze best --snap-date 2024-03-31` and parameters_evaluation.yaml specifies `k_values: [5, "all"]`
- **THEN** metrics are computed with K=5 and K=N, and report includes @5 and @N metrics

#### Scenario: Analyze with CLI k-values override
- **WHEN** `python scripts/evaluate_model.py analyze best --snap-date 2024-03-31 --k-values 3,10`
- **THEN** CLI k_values [3, 10] override YAML settings

#### Scenario: Analyze with multiple segment dimensions
- **WHEN** YAML configures segment_columns=[cust_segment_typ] and segment_sources has holding_combo
- **THEN** report includes separate Segment Analysis sections for cust_segment_typ and holding_combo

#### Scenario: Missing params file
- **WHEN** parameters_evaluation.yaml does not exist
- **THEN** script uses built-in defaults and continues without error

### Requirement: Compare subcommand
scripts/evaluate_model.py SHALL provide a `compare` subcommand that:
1. Accepts: model_a (positional), model_b (optional positional), --baseline (optional, choices: global_popularity/segment_popularity), --snap-date (required), --data-dir, --k-values (optional, overrides YAML), --params-file
2. If model_b is provided: compare two model versions
3. If --baseline is provided: compare model_a against the specified baseline
4. Exactly one of model_b or --baseline MUST be specified
5. Loads parameters_evaluation.yaml for k_values configuration
6. Generates comparison HTML report at `data/evaluation/compare_{a}_vs_{b}/{snap_date}/`

#### Scenario: Compare uses YAML k_values
- **WHEN** `python scripts/evaluate_model.py compare abc123 --baseline global_popularity --snap-date 2024-03-31` and YAML specifies `k_values: [5, "all"]`
- **THEN** comparison metrics use K=5 and K=N
