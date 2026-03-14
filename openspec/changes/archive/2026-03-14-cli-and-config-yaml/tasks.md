## 1. Dependencies & Setup

- [x] 1.1 Add `typer>=0.20.1` to `pyproject.toml` dependencies

## 2. Pipeline Registry

- [x] 2.1 Create pipeline registry in `src/recsys_tfb/pipelines/__init__.py` with `get_pipeline(name)` and `list_pipelines()` functions
- [x] 2.2 Add placeholder `create_pipeline()` in `src/recsys_tfb/pipelines/dataset/__init__.py` (returns empty Pipeline)
- [x] 2.3 Add placeholder `create_pipeline()` in `src/recsys_tfb/pipelines/training/__init__.py` (returns empty Pipeline)
- [x] 2.4 Write tests for pipeline registry (lookup, missing name, list)

## 3. CLI Entry Point

- [x] 3.1 Create `src/recsys_tfb/__main__.py` with Typer `run` command (`--pipeline`, `--env` options)
- [x] 3.2 Wire CLI: ConfigLoader → DataCatalog + parameters injection → pipeline lookup → Runner
- [x] 3.3 Handle errors: unknown pipeline (list available), execution failure (non-zero exit)
- [x] 3.4 Write tests for CLI (successful run, unknown pipeline, help output)

## 4. Config YAML Files

- [x] 4.1 Create `conf/base/catalog.yaml` with placeholder dataset entries for dataset and training pipelines
- [x] 4.2 Create `conf/base/parameters.yaml` with global parameters (random_seed, project_name)
- [x] 4.3 Create `conf/local/catalog.yaml` with local filepath overrides pointing to `data/` directory

## 5. Verification

- [x] 5.1 Run full test suite (`pytest tests/ -v`) and verify all tests pass
- [x] 5.2 Verify `python -m recsys_tfb --help` displays usage information
