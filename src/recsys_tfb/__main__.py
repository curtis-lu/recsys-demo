import json
import logging
from pathlib import Path
from typing import Optional

import typer

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.logging import RunContext, setup_logging
from recsys_tfb.core.runner import Runner
from recsys_tfb.core.consistency import validate_config_consistency
from recsys_tfb.core.schema import (
    get_schema_for_hash,
    validate_schema_config,
)
from recsys_tfb.core.versioning import (
    build_manifest_metadata,
    compute_base_dataset_version,
    compute_calibration_variant_id,
    compute_feature_table_fingerprint,
    compute_model_version,
    compute_train_variant_id,
    read_manifest,
    resolve_base_dataset_version,
    resolve_model_version,
    resolve_variant_id,
    update_symlink,
    write_manifest,
)
from recsys_tfb.pipelines import get_pipeline, list_pipelines
from recsys_tfb.pipelines.training.nodes import inject_cache_source_tables

app = typer.Typer(help="recsys_tfb: Product recommendation ranking model CLI")

logger = logging.getLogger(__name__)

_NONE_PLACEHOLDER = "__none__"


def _find_conf_dir() -> Path:
    """Resolve conf/ directory relative to the current working directory."""
    return Path.cwd() / "conf"


def _find_data_dir() -> Path:
    """Resolve data/ directory relative to the current working directory."""
    return Path.cwd() / "data"


def _load_spark_config(config: ConfigLoader, pipeline: str) -> dict:
    """Return base + pipeline-specific spark config, merged (pipeline wins),
    with ${vdclient.<name>} placeholders resolved (or dropped if vdclient
    is unavailable)."""
    from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders

    try:
        base_params = config.get_parameters_by_name("parameters")
    except KeyError:
        base_params = {}
    try:
        pipe_params = config.get_parameters_by_name(f"parameters_{pipeline}")
    except KeyError:
        pipe_params = {}
    base_spark = dict(base_params.get("spark", {}))
    pipe_spark = pipe_params.get("spark", {})
    base_spark.update(pipe_spark)
    return resolve_vdclient_placeholders(base_spark)


def _load_config_and_setup(pipeline: str, env: str) -> tuple[ConfigLoader, dict, RunContext]:
    conf_dir = _find_conf_dir()
    try:
        config = ConfigLoader(str(conf_dir), env=env)
        params = config.get_parameters()
    except ValueError as exc:
        logger.error("Config loading failed: %s", exc)
        raise typer.Exit(code=1)

    run_context = RunContext(pipeline=pipeline, env=env)
    setup_logging(params, run_context)

    try:
        validate_schema_config(params)
        validate_config_consistency(params)
    except ValueError as exc:
        logger.error("Config validation failed: %s", exc)
        raise typer.Exit(code=1)

    return config, params, run_context


def _execute_pipeline(
    pipeline_name: str,
    pipeline_kwargs: dict,
    runtime_params: dict,
    config: ConfigLoader,
    params: dict,
    env: str
):
    try:
        pipe = get_pipeline(pipeline_name, **pipeline_kwargs)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline_name, available)
        raise typer.Exit(code=1)

    source_model_version = runtime_params.pop("source_model_version", None)
    substitution_params = {**params, **runtime_params}
    catalog_config = config.get_catalog_config(runtime_params=substitution_params)

    # Auto-inject cache source_tables from catalog config so cache nodes don't
    # need a parallel parameters yaml mapping. Catalog.yaml's HiveTableDataset
    # `table` field is the single source of truth for cache table resolution.
    inject_cache_source_tables(substitution_params, catalog_config)

    # For inference: when no explicit --model-version is given, the model
    # artifact should be read via the "best" symlink; swap the model filepath.
    if pipeline_name == "inference" and source_model_version is None:
        mv = runtime_params["model_version"]
        if "model" in catalog_config:
            catalog_config["model"]["filepath"] = catalog_config["model"][
                "filepath"
            ].replace(mv, "best")

    catalog = DataCatalog(catalog_config)
    catalog.add("parameters", MemoryDataset(data=substitution_params))

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


def _dir_artifacts(d: Path) -> list[str]:
    return sorted(f.name for f in d.iterdir() if f.is_file()) if d.is_dir() else []


def _run_etl(
    stage: str,
    env: str,
    target_dates: Optional[str],
    restart_from: Optional[str],
) -> None:
    """Shared executor for the feature/label/sample_pool ETL sub-commands.

    ``stage`` is one of ``feature_etl``, ``label_etl``, ``sample_pool_etl``
    and is used both as the pipeline name (for logging/config lookup) and as
    the top-level YAML key of its parameters file.
    """
    from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup(stage, env)

    spark_configs = _load_spark_config(config, stage)
    get_or_create_spark_session(spark_configs)

    conf_dir = _find_conf_dir()

    params_etl = config.get_parameters_by_name(f"parameters_{stage}")
    etl_config = params_etl.get(stage, params_etl)
    sql_dir = conf_dir / "sql" / "etl"
    dry_run = etl_config.get("dry_run", env == "local")

    if target_dates:
        date_list = [d.strip() for d in target_dates.split(",")]
    else:
        date_list = etl_config.get("target_dates", [])
    if not date_list:
        logger.error("No target_dates provided. Use --target-dates or set in config.")
        raise typer.Exit(code=1)

    rendered_sql_dir_str = etl_config.get("rendered_sql_dir")
    rendered_sql_dir = Path(rendered_sql_dir_str) if rendered_sql_dir_str else None

    runner = SQLRunner(
        config=etl_config,
        sql_dir=sql_dir,
        dry_run=dry_run,
        rendered_sql_dir=rendered_sql_dir,
    )
    try:
        runner.run(
            target_dates=date_list,
            restart_from=restart_from,
            run_id=run_context.run_id,
        )
    except Exception:
        logger.exception("%s pipeline failed", stage)
        raise typer.Exit(code=1)

    logger.info("Pipeline '%s' completed successfully", stage)


@app.command(name="feature_etl")
def feature_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
):
    """Run the feature ETL pipeline (feature_aum/sav/ccard/info/concat/table)."""
    _run_etl("feature_etl", env, target_dates, restart_from)


@app.command(name="label_etl")
def label_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
):
    """Run the label ETL pipeline (label_ccard/exchange/fund/table)."""
    _run_etl("label_etl", env, target_dates, restart_from)


@app.command(name="sample_pool_etl")
def sample_pool_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
):
    """Run the sample_pool ETL pipeline. Requires feature_etl and label_etl outputs."""
    _run_etl("sample_pool_etl", env, target_dates, restart_from)


@app.command(name="dataset")
def dataset(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
):
    """Run the dataset pipeline (always recomputes versions from parameters)."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("dataset", env)
    get_or_create_spark_session(_load_spark_config(config, "dataset"))
    data_dir = _find_data_dir()

    try:
        params_dataset = config.get_parameters_by_name("parameters_dataset")
    except KeyError:
        params_dataset = {}

    enable_calibration = (
        params_dataset.get("dataset", {}).get("enable_calibration", False)
    )

    spark = get_or_create_spark_session()
    feature_table_cfg = config.get_catalog_config(runtime_params=params)["feature_table"]
    feature_table_fqn = f"{feature_table_cfg['database']}.{feature_table_cfg['table']}"
    feature_table_columns = [
        (f.name, f.dataType.simpleString())
        for f in spark.table(feature_table_fqn).schema.fields
    ]
    feature_table_fp = compute_feature_table_fingerprint(feature_table_columns)

    schema_hash = get_schema_for_hash(params)
    base_v = compute_base_dataset_version(
        params_dataset, schema_hash, feature_table_fingerprint=feature_table_fp,
    )
    train_v = compute_train_variant_id(params_dataset)
    cal_v = (
        compute_calibration_variant_id(params_dataset) if enable_calibration else None
    )

    logger.info("feature_table_fingerprint: %s (%d cols)",
                feature_table_fp, len(feature_table_columns))
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": "best",  # placeholder to avoid unresolved templates
        "snap_date": _NONE_PLACEHOLDER,
    }

    pipeline_kwargs = {"enable_calibration": enable_calibration}

    _execute_pipeline("dataset", pipeline_kwargs, runtime_params, config, params, env)

    # Post run: write three (or two) manifests and update corresponding symlinks.
    base_dir = data_dir / "dataset" / base_v
    _write_pipeline_manifest(
        version_dir=base_dir,
        metadata_kwargs={
            "version": base_v,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "base_dataset_version": base_v,
            "feature_table_fingerprint": feature_table_fp,
            "artifacts": _dir_artifacts(base_dir),
        },
        run_id=run_context.run_id,
        symlink_target=data_dir / "dataset" / "latest",
        params_name="parameters_dataset",
        params_dict=params_dataset,
    )

    train_variant_dir = base_dir / "train_variants" / train_v
    _write_pipeline_manifest(
        version_dir=train_variant_dir,
        metadata_kwargs={
            "version": train_v,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "parent_version": base_v,
            "variant_kind": "train",
            "artifacts": _dir_artifacts(train_variant_dir),
        },
        run_id=run_context.run_id,
        symlink_target=base_dir / "train_variants" / "latest",
    )

    if cal_v is not None:
        cal_variant_dir = base_dir / "calibration_variants" / cal_v
        _write_pipeline_manifest(
            version_dir=cal_variant_dir,
            metadata_kwargs={
                "version": cal_v,
                "pipeline": "dataset",
                "parameters": params_dataset,
                "parent_version": base_v,
                "variant_kind": "calibration",
                "artifacts": _dir_artifacts(cal_variant_dir),
            },
            run_id=run_context.run_id,
            symlink_target=base_dir / "calibration_variants" / "latest",
        )

    logger.info("Pipeline 'dataset' completed successfully")


@app.command(name="training")
def training(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    base_dataset_version: Optional[str] = typer.Option(
        None, "--base-dataset-version",
        help="Base dataset version (default: latest symlink)",
    ),
    train_variant: Optional[str] = typer.Option(
        None, "--train-variant",
        help="Train variant ID (default: latest under base dataset)",
    ),
    calibration_variant: Optional[str] = typer.Option(
        None, "--calibration-variant",
        help="Calibration variant ID (default: latest under base dataset; "
             "only used when training.calibration.enabled=true)",
    ),
):
    """Run the training pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("training", env)
    get_or_create_spark_session(_load_spark_config(config, "training"))
    data_dir = _find_data_dir()

    dataset_dir = data_dir / "dataset"
    base_v = resolve_base_dataset_version(dataset_dir, base_dataset_version)
    base_dir = dataset_dir / base_v
    if base_dataset_version is not None and not base_dir.is_dir():
        logger.error("Base dataset version directory not found: %s", base_dir)
        raise typer.Exit(code=1)

    train_v = resolve_variant_id(base_dir, "train", train_variant)

    try:
        params_training = config.get_parameters_by_name("parameters_training")
    except KeyError:
        params_training = {}

    enable_calibration = (
        params_training.get("training", {}).get("calibration", {}).get("enabled", False)
    )
    cal_v = (
        resolve_variant_id(base_dir, "calibration", calibration_variant)
        if enable_calibration
        else None
    )

    mv = compute_model_version(params_training, base_v, train_v, cal_v)
    logger.info("Model version: %s", mv)
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": _NONE_PLACEHOLDER,
    }

    pipeline_kwargs = {"enable_calibration": enable_calibration}

    _execute_pipeline("training", pipeline_kwargs, runtime_params, config, params, env)

    # Post run
    version_dir = data_dir / "models" / mv
    metadata_kwargs: dict = {
        "version": mv,
        "pipeline": "training",
        "parameters": params_training,
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "artifacts": _dir_artifacts(version_dir),
    }
    if cal_v is not None:
        metadata_kwargs["calibration_variant_id"] = cal_v

    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs=metadata_kwargs,
        run_id=run_context.run_id,
        symlink_target=None,
        params_name="parameters_training",
        params_dict=params_training,
    )
    logger.info("Pipeline 'training' completed successfully")


def _dataset_versions_from_model_manifest(
    model_dir: Path,
    data_dir: Path,
) -> tuple[str, str, str | None]:
    """Return (base_dataset_version, train_variant_id, calibration_variant_id) for a model.

    Reads the model's manifest; falls back to ``latest`` resolutions per layer
    when fields are missing.
    """
    try:
        manifest = read_manifest(model_dir)
    except FileNotFoundError:
        logger.warning(
            "Model manifest not found at %s; falling back to dataset latest.", model_dir
        )
        manifest = {}

    dataset_dir = data_dir / "dataset"
    base_v = manifest.get("base_dataset_version") or resolve_base_dataset_version(
        dataset_dir, None
    )
    base_dir = dataset_dir / base_v
    train_v = manifest.get("train_variant_id") or resolve_variant_id(
        base_dir, "train", None
    )
    cal_v = manifest.get("calibration_variant_id")
    return base_v, train_v, cal_v


@app.command(name="inference")
def inference(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(
        None, "--model-version", help="Model version to use for inference (default: best symlink)"
    ),
):
    """Run the inference pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("inference", env)
    get_or_create_spark_session(_load_spark_config(config, "inference"))
    data_dir = _find_data_dir()

    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    base_v, train_v, cal_v = _dataset_versions_from_model_manifest(
        models_dir / mv, data_dir
    )

    try:
        params_inference = config.get_parameters_by_name("parameters_inference")
    except KeyError:
        params_inference = {}

    inf_config = params_inference.get("inference", params_inference)
    snap_dates_list = inf_config.get("snap_dates", [])
    snap_date = snap_dates_list[0].replace("-", "") if snap_dates_list else "unknown"

    logger.info("Model version: %s (%s)", mv, model_version if model_version else "best")
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": snap_date,
        "source_model_version": model_version,
    }

    _execute_pipeline("inference", {}, runtime_params, config, params, env)

    # Post run
    version_dir = data_dir / "inference" / mv / snap_date
    metadata_kwargs: dict = {
        "version": mv,
        "pipeline": "inference",
        "parameters": params_inference,
        "model_version": mv,
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
    }
    if cal_v is not None:
        metadata_kwargs["calibration_variant_id"] = cal_v

    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs=metadata_kwargs,
        run_id=run_context.run_id,
        symlink_target=data_dir / "inference" / "latest",
        params_name="parameters_inference",
        params_dict=params_inference,
    )
    logger.info("Pipeline 'inference' completed successfully")


@app.command(name="evaluation")
def evaluation(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(None, "--model-version", help="Model version to use"),
    post_training: bool = typer.Option(
        False, "--post-training",
        help="Read predictions from training_eval_predictions (default: ranked_predictions for monitoring)",
    ),
):
    """Run the evaluation pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("evaluation", env)
    get_or_create_spark_session(_load_spark_config(config, "evaluation"))
    data_dir = _find_data_dir()

    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    base_v, train_v, cal_v = _dataset_versions_from_model_manifest(
        models_dir / mv, data_dir
    )

    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}

    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    logger.info(
        "Evaluation — model_version: %s (%s), post_training: %s",
        mv, model_version if model_version else "best", post_training,
    )
    logger.info("Evaluation — snap_date: %s", snap_date)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": snap_date,
    }

    pipeline_kwargs = {"post_training": post_training}
    _execute_pipeline("evaluation", pipeline_kwargs, runtime_params, config, params, env)

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
        extra_metadata={"snap_date": snap_date, "post_training": post_training},
        symlink_target=data_dir / "evaluation" / "latest"
    )
    logger.info("Pipeline 'evaluation' completed successfully")


@app.command(name="baselines")
def baselines(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
):
    """Run the baselines pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("baselines", env)
    get_or_create_spark_session(_load_spark_config(config, "baselines"))
    data_dir = _find_data_dir()

    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}

    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    logger.info("Baselines — snap_date: %s", snap_date)

    runtime_params = {
        "base_dataset_version": _NONE_PLACEHOLDER,
        "train_variant_id": _NONE_PLACEHOLDER,
        "calibration_variant_id": _NONE_PLACEHOLDER,
        "model_version": _NONE_PLACEHOLDER,
        "snap_date": snap_date,
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
