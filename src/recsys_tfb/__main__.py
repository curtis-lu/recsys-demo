import json
import logging
import sys
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(help="recsys_tfb: Product recommendation ranking model CLI")


def _find_conf_dir() -> Path:
    """Resolve conf/ directory relative to the current working directory."""
    return Path.cwd() / "conf"


def _find_data_dir() -> Path:
    """Resolve data/ directory relative to the current working directory."""
    return Path.cwd() / "data"


@app.callback(invoke_without_command=True)
def run(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help="Pipeline name to run"),
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    dataset_version: Optional[str] = typer.Option(
        None, "--dataset-version", help="Dataset version to use (default: computed or latest)"
    ),
    model_version: Optional[str] = typer.Option(
        None, "--model-version", help="Model version to use for inference (default: best symlink)"
    ),
) -> None:
    """Run a named pipeline with the specified environment config."""
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

    # Load config first to extract backend
    conf_dir = _find_conf_dir()
    data_dir = _find_data_dir()
    config = ConfigLoader(str(conf_dir), env=env)
    params = config.get_parameters()
    backend = params.get("backend", "pandas")

    # Setup structured logging
    run_context = RunContext(pipeline=pipeline, env=env, backend=backend)
    setup_logging(params, run_context)
    logger = logging.getLogger(__name__)

    # Look up pipeline with backend (+ enable_calibration for dataset pipeline)
    pipeline_kwargs = {}
    if pipeline == "dataset":
        try:
            params_dataset = config.get_parameters_by_name("parameters_dataset")
        except KeyError:
            params_dataset = {}
        pipeline_kwargs["enable_calibration"] = params_dataset.get("dataset", {}).get(
            "enable_calibration", False
        )
    try:
        pipe = get_pipeline(pipeline, backend=backend, **pipeline_kwargs)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline, available)
        raise typer.Exit(code=1)

    # --- Version resolution ---
    runtime_params: dict[str, str] = {}

    if pipeline == "dataset":
        # Compute dataset version from parameters_dataset.yaml content
        try:
            params_dataset = config.get_parameters_by_name("parameters_dataset")
        except KeyError:
            params_dataset = {}
        ds_version = compute_dataset_version(params_dataset)
        runtime_params["dataset_version"] = ds_version
        # model_version not needed for dataset pipeline, but set a placeholder
        # to avoid unresolved templates in shared catalog entries
        runtime_params["model_version"] = "best"
        runtime_params["snap_date"] = "__none__"
        logger.info("Dataset version: %s", ds_version)

    elif pipeline == "training":
        # Resolve dataset version (from --dataset-version or latest symlink)
        dataset_dir = data_dir / "dataset"
        ds_version = resolve_dataset_version(dataset_dir, dataset_version)

        # Validate explicitly specified dataset version directory exists
        if dataset_version is not None:
            ds_version_dir = dataset_dir / ds_version
            if not ds_version_dir.is_dir():
                logger.error("Dataset version directory not found: %s", ds_version_dir)
                raise typer.Exit(code=1)

        # Compute model version from training params + dataset version
        try:
            params_training = config.get_parameters_by_name("parameters_training")
        except KeyError:
            params_training = {}
        mv = compute_model_version(params_training, ds_version)

        runtime_params["dataset_version"] = ds_version
        runtime_params["model_version"] = mv
        runtime_params["snap_date"] = "__none__"
        logger.info("Model version: %s", mv)
        logger.info("Dataset version: %s", ds_version)

    elif pipeline == "inference":
        # Resolve model version (from --model-version or best symlink)
        models_dir = data_dir / "models"
        mv = resolve_model_version(models_dir, model_version)

        # Validate explicitly specified model version directory exists
        if model_version is not None:
            mv_dir = models_dir / mv
            if not mv_dir.is_dir():
                logger.error("Model version directory not found: %s", mv_dir)
                raise typer.Exit(code=1)

        # Read dataset_version from model manifest, fallback to latest
        dataset_dir = data_dir / "dataset"
        model_dir = models_dir / mv
        try:
            model_manifest = read_manifest(model_dir)
            ds_version = model_manifest["dataset_version"]
        except (FileNotFoundError, KeyError):
            logger.warning(
                "Model manifest not found or missing dataset_version. "
                "Falling back to dataset latest."
            )
            ds_version = resolve_dataset_version(dataset_dir, dataset_version)

        # Get snap_date from inference parameters
        try:
            params_inference = config.get_parameters_by_name("parameters_inference")
        except KeyError:
            params_inference = {}
        inf_config = params_inference.get("inference", params_inference)
        snap_dates = inf_config.get("snap_dates", [])
        snap_date = snap_dates[0].replace("-", "") if snap_dates else "unknown"

        runtime_params["model_version"] = mv
        runtime_params["dataset_version"] = ds_version
        runtime_params["snap_date"] = snap_date
        source = model_version if model_version else "best"
        logger.info("Model version: %s (%s)", mv, source)
        logger.info("Dataset version: %s", ds_version)

    else:
        # Generic pipeline — provide defaults
        runtime_params["model_version"] = "best"
        runtime_params["dataset_version"] = "latest"
        runtime_params["snap_date"] = "__none__"

    # Build catalog with resolved runtime params
    catalog_config = config.get_catalog_config(runtime_params=runtime_params)

    # For inference: when no explicit --model-version is given, model and
    # preprocessor read via "best" symlink while output paths use the actual hash.
    if pipeline == "inference" and model_version is None:
        for entry_name in ("model", "preprocessor"):
            if entry_name in catalog_config:
                catalog_config[entry_name]["filepath"] = catalog_config[entry_name][
                    "filepath"
                ].replace(mv, "best")

    catalog = DataCatalog(catalog_config)

    # Inject parameters
    catalog.add("parameters", MemoryDataset(data=params))

    # Run
    logger.info("Running pipeline '%s' (env=%s)", pipeline, env)
    try:
        runner = Runner()
        runner.run(pipe, catalog)
    except Exception:
        logger.exception("Pipeline '%s' failed", pipeline)
        raise typer.Exit(code=1)

    # --- Post-run: write manifest and update symlinks ---
    if pipeline == "dataset":
        ds_version = runtime_params["dataset_version"]
        version_dir = data_dir / "dataset" / ds_version
        try:
            params_dataset = config.get_parameters_by_name("parameters_dataset")
        except KeyError:
            params_dataset = {}
        artifacts = [
            f.name for f in version_dir.iterdir() if f.is_file()
        ] if version_dir.is_dir() else []
        metadata = build_manifest_metadata(
            version=ds_version,
            pipeline="dataset",
            parameters=params_dataset,
            artifacts=sorted(artifacts),
        )
        metadata["run_id"] = run_context.run_id
        write_manifest(version_dir, metadata)
        update_symlink(version_dir, data_dir / "dataset" / "latest")
        with open(version_dir / "parameters_dataset.json", "w") as f:
            json.dump(params_dataset, f, indent=2, ensure_ascii=False, default=str)

    elif pipeline == "training":
        mv = runtime_params["model_version"]
        ds_version = runtime_params["dataset_version"]
        version_dir = data_dir / "models" / mv
        try:
            params_training = config.get_parameters_by_name("parameters_training")
        except KeyError:
            params_training = {}
        artifacts = [
            f.name for f in version_dir.iterdir() if f.is_file()
        ] if version_dir.is_dir() else []
        metadata = build_manifest_metadata(
            version=mv,
            pipeline="training",
            parameters=params_training,
            dataset_version=ds_version,
            artifacts=sorted(artifacts),
        )
        metadata["run_id"] = run_context.run_id
        write_manifest(version_dir, metadata)
        with open(version_dir / "parameters_training.json", "w") as f:
            json.dump(params_training, f, indent=2, ensure_ascii=False, default=str)

    elif pipeline == "inference":
        mv = runtime_params["model_version"]
        ds_version = runtime_params["dataset_version"]
        snap_date = runtime_params["snap_date"]
        version_dir = data_dir / "inference" / mv / snap_date
        try:
            params_inference = config.get_parameters_by_name("parameters_inference")
        except KeyError:
            params_inference = {}
        metadata = build_manifest_metadata(
            version=mv,
            pipeline="inference",
            parameters=params_inference,
            model_version=mv,
            dataset_version=ds_version,
        )
        metadata["run_id"] = run_context.run_id
        write_manifest(version_dir, metadata)
        update_symlink(version_dir, data_dir / "inference" / "latest")
        with open(version_dir / "parameters_inference.json", "w") as f:
            json.dump(params_inference, f, indent=2, ensure_ascii=False, default=str)

    logger.info("Pipeline '%s' completed successfully", pipeline)


if __name__ == "__main__":
    app()
