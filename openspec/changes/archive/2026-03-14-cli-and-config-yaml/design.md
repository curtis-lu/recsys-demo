## Context

The framework core (ConfigLoader, DataCatalog, Node, Pipeline, Runner) is complete with 38 passing tests. However, there's no CLI entry point to run pipelines, no pipeline registry to look up pipelines by name, and no config YAML files to define datasets and parameters. These are the final pieces of the Phase 1 skeleton before pipeline development begins.

Existing code:
- `ConfigLoader` reads `conf/base/*.yaml` + `conf/{env}/*.yaml` with deep merge
- `DataCatalog` instantiates datasets from a config dict (supports ParquetDataset, PickleDataset)
- `Runner.run(pipeline, catalog)` executes nodes in topological order

## Goals / Non-Goals

**Goals:**
- CLI entry point: `python -m recsys_tfb run --pipeline <name> --env <env>`
- Pipeline registry mapping names → Pipeline factories
- Config YAML files for catalog and parameters (placeholder entries for Phase 2 datasets)
- Wire ConfigLoader → DataCatalog → Runner flow through the CLI

**Non-Goals:**
- Ploomber integration (production orchestration, not dev CLI)
- Actual pipeline node implementations (Phase 2)
- Production catalog entries (HiveDataset, production paths)
- Config validation beyond what ConfigLoader/DataCatalog already do

## Decisions

### 1. CLI framework: Typer

Typer provides declarative CLI definition with type hints, auto-generated help, and is already specified in the PRD (`typer==0.20.1`). Alternatives: argparse (too verbose), click (Typer wraps it with less boilerplate).

### 2. Pipeline registry: dictionary in `pipelines/__init__.py`

A simple `dict[str, Callable[[], Pipeline]]` mapping pipeline names to factory functions. Each pipeline module exposes `create_pipeline() -> Pipeline`. The registry imports lazily to avoid pulling in heavy dependencies. Alternative: decorator-based auto-registration (over-engineered for ~4 pipelines).

### 3. `__main__.py` as thin orchestration layer

The `run` command does: load config → build catalog → look up pipeline → run. All logic stays in existing core classes. `__main__.py` is just glue code, no business logic.

### 4. Config YAML structure

- `conf/base/catalog.yaml`: Dataset definitions with `type` + constructor kwargs. Placeholder entries for dataset/training pipeline inputs/outputs.
- `conf/base/parameters.yaml`: Global parameters (random seed, project name).
- `conf/local/catalog.yaml`: Override `filepath` values to point to `data/` directory for local dev.

### 5. Parameters injection via catalog

Parameters are loaded by ConfigLoader and injected into the catalog as a `MemoryDataset` named `params` (or `parameters`). Nodes access them like any other dataset input. This keeps the Runner interface unchanged.

## Risks / Trade-offs

- [Placeholder catalog entries may be wrong] → Will be updated when Phase 2 pipeline nodes are defined. Entries are intentionally minimal.
- [Typer version compatibility with Python 3.12] → Using `>=0.20.1` to allow compatible versions.
- [Pipeline registry imports could fail if pipeline modules have missing deps] → Factory functions are only called when that pipeline is requested, not at import time.
