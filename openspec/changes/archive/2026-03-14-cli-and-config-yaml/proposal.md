## Why

The framework skeleton (ConfigLoader, DataCatalog, Node/Pipeline/Runner) is built but there's no way to run a pipeline from the command line, and no config YAML files exist yet. Without a CLI entry point and actual config files, developers can't invoke pipelines via `python -m recsys_tfb run --pipeline <name> --env <env>`, which is required before any pipeline development in Phase 2+.

## What Changes

- Add `__main__.py` with a Typer CLI that wires ConfigLoader + DataCatalog + Runner together
- Add a pipeline registry so the CLI can look up pipelines by name
- Create `conf/base/catalog.yaml` with dataset definitions for the dataset building and training pipelines
- Create `conf/base/parameters.yaml` with global parameters
- Create `conf/local/catalog.yaml` with local dev Parquet path overrides
- Add `typer` as a project dependency

## Capabilities

### New Capabilities
- `cli`: Command-line interface entry point for running pipelines (`python -m recsys_tfb run`)
- `pipeline-registry`: Central registry mapping pipeline names to Pipeline objects

### Modified Capabilities
- `config-loader`: No spec-level changes - existing ConfigLoader API is sufficient

## Impact

- New files: `src/recsys_tfb/__main__.py`, `conf/base/catalog.yaml`, `conf/base/parameters.yaml`, `conf/local/catalog.yaml`
- New dependency: `typer>=0.20.1` in `pyproject.toml`
- Pipeline modules (`src/recsys_tfb/pipelines/*/`) will need to expose a `create_pipeline()` function for the registry
