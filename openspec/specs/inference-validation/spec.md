### Requirement: Validate prediction row count matches scoring dataset
The system SHALL verify that `ranked_predictions` has the same number of rows as `scoring_dataset`.

#### Scenario: Row count matches
- **WHEN** ranked_predictions and scoring_dataset have the same row count
- **THEN** the check passes

#### Scenario: Row count mismatch
- **WHEN** ranked_predictions has a different row count than scoring_dataset
- **THEN** the system raises ValidationError with check name "row_count_match"

### Requirement: Validate score range
The system SHALL verify that all prediction scores are within [0.0, 1.0].

#### Scenario: All scores in range
- **WHEN** all scores are between 0.0 and 1.0 inclusive
- **THEN** the check passes

#### Scenario: Score out of range
- **WHEN** any score is below 0.0 or above 1.0
- **THEN** the system raises ValidationError with check name "score_range"

### Requirement: Validate no missing values
The system SHALL verify that identity columns, score column, and rank column contain no NaN values.

#### Scenario: No NaN values
- **WHEN** all checked columns have no null/NaN values
- **THEN** the check passes

#### Scenario: NaN values found
- **WHEN** any checked column contains NaN
- **THEN** the system raises ValidationError with check name "no_missing"

### Requirement: Validate completeness
The system SHALL verify that each (time, entity) group has exactly N products, where N equals the number of configured products.

#### Scenario: All groups complete
- **WHEN** every group has exactly N products
- **THEN** the check passes

#### Scenario: Incomplete group
- **WHEN** any group has fewer or more than N products
- **THEN** the system raises ValidationError with check name "completeness"

### Requirement: Validate rank consistency
The system SHALL verify that ranks within each group are consecutive integers 1..N and that rank ordering is consistent with score descending order.

#### Scenario: Consistent ranks
- **WHEN** ranks are 1..N and scores decrease monotonically with increasing rank
- **THEN** the check passes

#### Scenario: Non-sequential ranks
- **WHEN** ranks are not consecutive integers 1..N
- **THEN** the system raises ValidationError with check name "rank_consistency"

#### Scenario: Score-rank order mismatch
- **WHEN** a higher rank has a lower score than a lower rank
- **THEN** the system raises ValidationError with check name "rank_consistency"

### Requirement: Validate no duplicates
The system SHALL verify that no duplicate rows exist on identity columns.

#### Scenario: No duplicates
- **WHEN** all identity column combinations are unique
- **THEN** the check passes

#### Scenario: Duplicates found
- **WHEN** duplicate identity column combinations exist
- **THEN** the system raises ValidationError with check name "no_duplicates"

### Requirement: Validation failure behavior
The system SHALL collect all failing checks and raise a single ValidationError containing all failures. The error message SHALL list all failed check names.

#### Scenario: Multiple checks fail simultaneously
- **WHEN** more than one sanity check fails
- **THEN** the system raises one ValidationError with all failure details

### Requirement: Validation pass-through
The system SHALL return the input ranked_predictions unchanged when all checks pass, acting as a pass-through node.

#### Scenario: All checks pass
- **WHEN** all 6 sanity checks pass
- **THEN** the function returns the original ranked_predictions DataFrame
