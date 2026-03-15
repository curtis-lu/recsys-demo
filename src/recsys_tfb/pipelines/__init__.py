from recsys_tfb.core.pipeline import Pipeline

_REGISTRY: dict[str, str] = {
    "dataset": "recsys_tfb.pipelines.dataset",
    "training": "recsys_tfb.pipelines.training",
    "inference": "recsys_tfb.pipelines.inference",
}


def get_pipeline(name: str) -> Pipeline:
    """Look up a pipeline by name and return it via the module's create_pipeline()."""
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Pipeline '{name}' not found. Available pipelines: {available}"
        )
    import importlib

    module = importlib.import_module(_REGISTRY[name])
    return module.create_pipeline()


def list_pipelines() -> list[str]:
    """Return all registered pipeline names."""
    return sorted(_REGISTRY.keys())
