import logging
import sys
from datetime import datetime
from pathlib import Path

import typer

app = typer.Typer(help="recsys_tfb: Product recommendation ranking model CLI")


def _find_conf_dir() -> Path:
    """Resolve conf/ directory relative to the current working directory."""
    return Path.cwd() / "conf"


@app.command()
def run(
    pipeline: str = typer.Option(..., "--pipeline", "-p", help="Pipeline name to run"),
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
) -> None:
    """Run a named pipeline with the specified environment config."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
    from recsys_tfb.core.config import ConfigLoader
    from recsys_tfb.core.runner import Runner
    from recsys_tfb.pipelines import get_pipeline, list_pipelines

    # Load config first to extract backend
    conf_dir = _find_conf_dir()
    config = ConfigLoader(str(conf_dir), env=env)
    params = config.get_parameters()
    backend = params.get("backend", "pandas")

    # Look up pipeline with backend
    try:
        pipe = get_pipeline(pipeline, backend=backend)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline, available)
        raise typer.Exit(code=1)

    # Build catalog — for training, redirect model artifacts to versioned directory
    if pipeline == "training":
        model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info("Training version: %s", model_version)
    else:
        model_version = "best"
    catalog_config = config.get_catalog_config(
        runtime_params={"model_version": model_version}
    )

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

    logger.info("Pipeline '%s' completed successfully", pipeline)


if __name__ == "__main__":
    app()
