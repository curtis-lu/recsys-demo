import json
import logging
import sys
from pathlib import Path
from typing import Optional

import typer

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.logging import RunContext, setup_logging
from recsys_tfb.core.runner import Runner
from recsys_tfb.core.versioning import (
    build_manifest_metadata,
    compute_dataset_version,
    compute_model_version,
    read_manifest,
    resolve_dataset_version,
    resolve_model_version,
    update_symlink,
    write_manifest,
)
from recsys_tfb.pipelines import get_pipeline, list_pipelines

app = typer.Typer(help="recsys_tfb: Product recommendation ranking model CLI")

logger = logging.getLogger(__name__)


def _find_conf_dir() -> Path:
    """Resolve conf/ directory relative to the current working directory."""
    return Path.cwd() / "conf"


def _find_data_dir() -> Path:
    """Resolve data/ directory relative to the current working directory."""
    return Path.cwd() / "data"


def _load_config_and_setup(pipeline: str, env: str) -> tuple[ConfigLoader, dict, str, RunContext]:
    conf_dir = _find_conf_dir()
    config = ConfigLoader(str(conf_dir), env=env)
    params = config.get_parameters()
    backend = params.get("backend", "pandas")

    run_context = RunContext(pipeline=pipeline, env=env, backend=backend)
    setup_logging(params, run_context)
    
    return config, params, backend, run_context


def _execute_pipeline(
    pipeline_name: str,
    pipeline_kwargs: dict,
    runtime_params: dict,
    config: ConfigLoader,
    params: dict,
    env: str
):
    try:
        pipe = get_pipeline(pipeline_name, backend=runtime_params.get("backend", "pandas"), **pipeline_kwargs)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline_name, available)
        raise typer.Exit(code=1)

    source_model_version = runtime_params.pop("source_model_version", None)
    catalog_config = config.get_catalog_config(runtime_params=runtime_params)

    # For inference: when no explicit --model-version is given, model and
    # preprocessor read via "best" symlink while output paths use the actual hash.
    if pipeline_name == "inference" and source_model_version is None:
        mv = runtime_params["model_version"]
        for entry_name in ("model", "preprocessor"):
            if entry_name in catalog_config:
                catalog_config[entry_name]["filepath"] = catalog_config[entry_name][
                    "filepath"
                ].replace(mv, "best")

    catalog = DataCatalog(catalog_config)
    catalog.add("parameters", MemoryDataset(data=params))

    if pipeline_name == "evaluation":
        if not catalog.exists("baseline_metrics"):
            catalog.add("baseline_metrics", MemoryDataset(data=None))
            logger.info("No baseline_metrics found — report will skip baseline comparison")

    logger.info("Running pipeline '%s' (env=%s)", pipeline_name, env)
    try:
        runner = Runner()
        runner.run(pipe, catalog)
    except Exception:
        logger.exception("Pipeline '%s' failed", pipeline_name)
        raise typer.Exit(code=1)


def _write_pipeline_manifest(
    version_dir: Path,
    metadata_kwargs: dict,
    run_id: str,
    extra_metadata: Optional[dict] = None,
    symlink_target: Optional[Path] = None,
    params_name: Optional[str] = None,
    params_dict: Optional[dict] = None
):
    metadata = build_manifest_metadata(**metadata_kwargs)
    metadata["run_id"] = run_id
    if extra_metadata:
        metadata.update(extra_metadata)
    write_manifest(version_dir, metadata)
    if symlink_target:
        update_symlink(version_dir, symlink_target)
    if params_name and params_dict is not None:
        with open(version_dir / f"{params_name}.json", "w") as f:
            json.dump(params_dict, f, indent=2, ensure_ascii=False, default=str)


@app.command(name="source_etl")
def source_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    snap_dates: Optional[str] = typer.Option(
        None, "--snap-dates", help="Comma-separated snap dates for source_etl (e.g. 2024-01-31,2024-02-29)"
    ),
    restart_from: Optional[str] = typer.Option(
        None, "--restart-from", help="Restart source_etl from this table name (skip earlier tables)"
    )
):
    """Run the source_etl pipeline."""
    from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner

    config, params, backend, run_context = _load_config_and_setup("source_etl", env)
    conf_dir = _find_conf_dir()
    
    params_etl = config.get_parameters_by_name("parameters_source_etl")
    etl_config = params_etl.get("source_etl", params_etl)
    sql_dir = conf_dir / "sql" / "etl"
    dry_run = etl_config.get("dry_run", env == "local")

    if snap_dates:
        date_list = [d.strip() for d in snap_dates.split(",")]
    else:
        date_list = etl_config.get("snap_dates", [])
    if not date_list:
        logger.error("No snap_dates provided. Use --snap-dates or set in config.")
        raise typer.Exit(code=1)

    runner = SQLRunner(config=etl_config, sql_dir=sql_dir, dry_run=dry_run)
    try:
        runner.run(snap_dates=date_list, restart_from=restart_from, run_id=run_context.run_id)
    except Exception:
        logger.exception("Source ETL pipeline failed")
        raise typer.Exit(code=1)
    
    logger.info("Pipeline 'source_etl' completed successfully")


@app.command(name="dataset")
def dataset(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    dataset_version: Optional[str] = typer.Option(
        None, "--dataset-version", help="Dataset version to use (default: computed or latest)"
    ),
):
    """Run the dataset pipeline."""
    config, params, backend, run_context = _load_config_and_setup("dataset", env)
    data_dir = _find_data_dir()
    
    try:
        params_dataset = config.get_parameters_by_name("parameters_dataset")
    except KeyError:
        params_dataset = {}
        
    ds_version = compute_dataset_version(params_dataset)
    logger.info("Dataset version: %s", ds_version)
    
    runtime_params = {
        "dataset_version": ds_version,
        "model_version": "best",  # placeholder to avoid unresolved templates
        "snap_date": "__none__",
        "backend": backend,
    }
    
    pipeline_kwargs = {
        "enable_calibration": params_dataset.get("dataset", {}).get("enable_calibration", False)
    }

    _execute_pipeline("dataset", pipeline_kwargs, runtime_params, config, params, env)
    
    # Post run
    version_dir = data_dir / "dataset" / ds_version
    artifacts = [f.name for f in version_dir.iterdir() if f.is_file()] if version_dir.is_dir() else []
    
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": ds_version,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "artifacts": sorted(artifacts),
        },
        run_id=run_context.run_id,
        symlink_target=data_dir / "dataset" / "latest",
        params_name="parameters_dataset",
        params_dict=params_dataset
    )
    logger.info("Pipeline 'dataset' completed successfully")


@app.command(name="training")
def training(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    dataset_version: Optional[str] = typer.Option(
        None, "--dataset-version", help="Dataset version to use (default: computed or latest)"
    ),
):
    """Run the training pipeline."""
    config, params, backend, run_context = _load_config_and_setup("training", env)
    data_dir = _find_data_dir()
    
    dataset_dir = data_dir / "dataset"
    ds_version = resolve_dataset_version(dataset_dir, dataset_version)
    if dataset_version is not None and not (dataset_dir / ds_version).is_dir():
        logger.error("Dataset version directory not found: %s", dataset_dir / ds_version)
        raise typer.Exit(code=1)

    try:
        params_training = config.get_parameters_by_name("parameters_training")
    except KeyError:
        params_training = {}
        
    mv = compute_model_version(params_training, ds_version)
    logger.info("Model version: %s", mv)
    logger.info("Dataset version: %s", ds_version)
    
    runtime_params = {
        "dataset_version": ds_version,
        "model_version": mv,
        "snap_date": "__none__",
        "backend": backend,
    }
    
    pipeline_kwargs = {
        "enable_calibration": params_training.get("training", {}).get("calibration", {}).get("enabled", False)
    }

    _execute_pipeline("training", pipeline_kwargs, runtime_params, config, params, env)
    
    # Post run
    version_dir = data_dir / "models" / mv
    artifacts = [f.name for f in version_dir.iterdir() if f.is_file()] if version_dir.is_dir() else []
    
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": mv,
            "pipeline": "training",
            "parameters": params_training,
            "dataset_version": ds_version,
            "artifacts": sorted(artifacts),
        },
        run_id=run_context.run_id,
        symlink_target=None,
        params_name="parameters_training",
        params_dict=params_training
    )
    logger.info("Pipeline 'training' completed successfully")


@app.command(name="inference")
def inference(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(
        None, "--model-version", help="Model version to use for inference (default: best symlink)"
    ),
):
    """Run the inference pipeline."""
    config, params, backend, run_context = _load_config_and_setup("inference", env)
    data_dir = _find_data_dir()
    
    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    try:
        model_manifest = read_manifest(models_dir / mv)
        ds_version = model_manifest["dataset_version"]
    except (FileNotFoundError, KeyError):
        logger.warning(
            "Model manifest not found or missing dataset_version. "
            "Falling back to dataset latest."
        )
        ds_version = resolve_dataset_version(data_dir / "dataset", None)

    try:
        params_inference = config.get_parameters_by_name("parameters_inference")
    except KeyError:
        params_inference = {}
        
    inf_config = params_inference.get("inference", params_inference)
    snap_dates_list = inf_config.get("snap_dates", [])
    snap_date = snap_dates_list[0].replace("-", "") if snap_dates_list else "unknown"

    logger.info("Model version: %s (%s)", mv, model_version if model_version else "best")
    logger.info("Dataset version: %s", ds_version)
    
    runtime_params = {
        "dataset_version": ds_version,
        "model_version": mv,
        "snap_date": snap_date,
        "backend": backend,
        "source_model_version": model_version, # To indicate if we explicitly requested a model
    }

    _execute_pipeline("inference", {}, runtime_params, config, params, env)
    
    # Post run
    version_dir = data_dir / "inference" / mv / snap_date
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": mv,
            "pipeline": "inference",
            "parameters": params_inference,
            "model_version": mv,
            "dataset_version": ds_version,
        },
        run_id=run_context.run_id,
        symlink_target=data_dir / "inference" / "latest",
        params_name="parameters_inference",
        params_dict=params_inference
    )
    logger.info("Pipeline 'inference' completed successfully")


@app.command(name="evaluation")
def evaluation(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(None, "--model-version", help="Model version to use"),
    dataset_version: Optional[str] = typer.Option(None, "--dataset-version", help="Dataset version fallback"),
):
    """Run the evaluation pipeline."""
    config, params, backend, run_context = _load_config_and_setup("evaluation", env)
    data_dir = _find_data_dir()
    
    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    try:
        model_manifest = read_manifest(models_dir / mv)
        ds_version = model_manifest["dataset_version"]
    except (FileNotFoundError, KeyError):
        logger.warning(
            "Model manifest not found or missing dataset_version. "
            "Falling back to dataset latest."
        )
        ds_version = resolve_dataset_version(data_dir / "dataset", dataset_version)

    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}
        
    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    logger.info("Evaluation — model_version: %s (%s)", mv, model_version if model_version else "best")
    logger.info("Evaluation — snap_date: %s", snap_date)
    
    runtime_params = {
        "dataset_version": ds_version,
        "model_version": mv,
        "snap_date": snap_date,
        "backend": backend,
    }

    _execute_pipeline("evaluation", {}, runtime_params, config, params, env)
    
    # Post run
    version_dir = data_dir / "evaluation" / mv / snap_date
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": mv,
            "pipeline": "evaluation",
            "parameters": params_eval,
            "model_version": mv,
        },
        run_id=run_context.run_id,
        extra_metadata={"snap_date": snap_date},
        symlink_target=data_dir / "evaluation" / "latest"
    )
    logger.info("Pipeline 'evaluation' completed successfully")


@app.command(name="baselines")
def baselines(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
):
    """Run the baselines pipeline."""
    config, params, backend, run_context = _load_config_and_setup("baselines", env)
    data_dir = _find_data_dir()
    
    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}
        
    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    logger.info("Baselines — snap_date: %s", snap_date)
    
    runtime_params = {
        "dataset_version": "__none__",
        "model_version": "__none__",
        "snap_date": snap_date,
        "backend": backend,
    }

    _execute_pipeline("baselines", {}, runtime_params, config, params, env)
    
    # Post run
    version_dir = data_dir / "baselines" / snap_date
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": snap_date,
            "pipeline": "baselines",
            "parameters": params_eval,
        },
        run_id=run_context.run_id,
        extra_metadata={"snap_date": snap_date},
        symlink_target=data_dir / "baselines" / "latest"
    )
    logger.info("Pipeline 'baselines' completed successfully")


if __name__ == "__main__":
    app()
