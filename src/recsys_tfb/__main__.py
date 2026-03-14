import logging
import sys
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

    # Look up pipeline
    try:
        pipe = get_pipeline(pipeline)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline, available)
        raise typer.Exit(code=1)

    # Load config
    conf_dir = _find_conf_dir()
    config = ConfigLoader(str(conf_dir), env=env)

    # Build catalog
    catalog = DataCatalog(config.get_catalog_config())

    # Inject parameters
    params = config.get_parameters()
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
