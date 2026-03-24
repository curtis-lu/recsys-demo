## MODIFIED Requirements

### Requirement: CLI run command
The CLI SHALL read `enable_calibration` from the dataset parameters and pass it to `get_pipeline` when running the dataset pipeline.

#### Scenario: Dataset pipeline with calibration enabled
- **WHEN** `parameters_dataset.yaml` has `enable_calibration: true` and user executes `python -m recsys_tfb --pipeline dataset --env local`
- **THEN** the CLI SHALL pass `enable_calibration=True` to `get_pipeline`, resulting in a pipeline that includes calibration nodes

#### Scenario: Dataset pipeline with calibration disabled
- **WHEN** `parameters_dataset.yaml` has `enable_calibration: false` and user executes `python -m recsys_tfb --pipeline dataset --env local`
- **THEN** the CLI SHALL pass `enable_calibration=False` to `get_pipeline`, resulting in a pipeline without calibration nodes

#### Scenario: Non-dataset pipelines unaffected
- **WHEN** user executes `python -m recsys_tfb --pipeline training --env local`
- **THEN** the CLI SHALL NOT pass `enable_calibration` to `get_pipeline`
