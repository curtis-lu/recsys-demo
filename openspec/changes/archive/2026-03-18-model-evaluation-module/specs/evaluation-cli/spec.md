## ADDED Requirements

### Requirement: Analyze subcommand
scripts/evaluate_model.py SHALL provide an `analyze` subcommand (Typer) that:
1. Accepts: model_version (positional, supports hash/latest/best), --snap-date (required), --data-dir (default "data/"), --k-values (default "3,5")
2. Loads ranked_predictions.parquet, label_table.parquet, feature_table.parquet
3. Resolves model_version via `core/versioning.resolve_model_version()` for latest/best aliases
4. Runs all 5 analysis dimensions (metrics, distributions, calibration, segments)
5. Generates HTML report and saves to `data/evaluation/{model_version}/{snap_date}/`
6. Also saves metrics.json to the same directory
7. Prints summary metrics to terminal

#### Scenario: Analyze with explicit version
- **WHEN** `python scripts/evaluate_model.py analyze abc12345 --snap-date 2024-03-31`
- **THEN** loads data from `data/inference/abc12345/20240331/ranked_predictions.parquet` and produces report at `data/evaluation/abc12345/20240331/report.html`

#### Scenario: Analyze with latest alias
- **WHEN** `python scripts/evaluate_model.py analyze latest --snap-date 2024-03-31`
- **THEN** resolves latest symlink to actual version hash and uses that

#### Scenario: Missing inference data
- **WHEN** ranked_predictions.parquet does not exist for given version/snap_date
- **THEN** script exits with clear error message indicating the missing file

### Requirement: Compare subcommand
scripts/evaluate_model.py SHALL provide a `compare` subcommand that:
1. Accepts: model_a (positional), model_b (optional positional), --baseline (optional, choices: global_popularity/segment_popularity), --snap-date (required), --data-dir, --k-values
2. If model_b is provided: compare two model versions
3. If --baseline is provided: compare model_a against the specified baseline
4. Exactly one of model_b or --baseline MUST be specified
5. Generates comparison HTML report at `data/evaluation/compare_{a}_vs_{b}/{snap_date}/`

#### Scenario: Compare two models
- **WHEN** `python scripts/evaluate_model.py compare abc123 def456 --snap-date 2024-03-31`
- **THEN** produces comparison report at `data/evaluation/compare_abc123_vs_def456/20240331/report.html`

#### Scenario: Compare model vs global baseline
- **WHEN** `python scripts/evaluate_model.py compare abc123 --baseline global_popularity --snap-date 2024-03-31`
- **THEN** generates global popularity baseline on-the-fly and compares against model predictions

#### Scenario: Compare model vs segment baseline
- **WHEN** `python scripts/evaluate_model.py compare abc123 --baseline segment_popularity --snap-date 2024-03-31`
- **THEN** generates segment-based baseline and produces comparison report

#### Scenario: Neither model_b nor baseline specified
- **WHEN** only model_a is provided without --baseline
- **THEN** script exits with error indicating that either model_b or --baseline is required

### Requirement: Snap date format handling
The CLI SHALL accept snap_date in format "YYYY-MM-DD" and convert to directory format "YYYYMMDD" for file path resolution.

#### Scenario: Date format conversion
- **WHEN** --snap-date 2024-03-31 is provided
- **THEN** file paths use "20240331" as the directory name
