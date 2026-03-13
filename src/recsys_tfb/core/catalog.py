from recsys_tfb.io.base import AbstractDataset
from recsys_tfb.io.parquet_dataset import ParquetDataset
from recsys_tfb.io.pickle_dataset import PickleDataset


# Registry of known dataset types
_DATASET_REGISTRY: dict[str, type[AbstractDataset]] = {
    "ParquetDataset": ParquetDataset,
    "PickleDataset": PickleDataset,
}


class MemoryDataset(AbstractDataset):
    """In-memory dataset for intermediate pipeline results."""

    def __init__(self, data=None):
        self._data = data

    def load(self):
        return self._data

    def save(self, data) -> None:
        self._data = data

    def exists(self) -> bool:
        return self._data is not None


class DataCatalog:
    """Manage dataset instances, providing unified load/save/exists interface."""

    def __init__(self, config: dict | None = None):
        self._datasets: dict[str, AbstractDataset] = {}
        if config:
            self._init_from_config(config)

    def _init_from_config(self, config: dict) -> None:
        for name, entry in config.items():
            entry = dict(entry)  # copy to avoid mutation
            type_name = entry.pop("type", None)
            if type_name is None:
                raise ValueError(f"Dataset '{name}' missing 'type' field")
            cls = _DATASET_REGISTRY.get(type_name)
            if cls is None:
                raise ValueError(
                    f"Unknown dataset type '{type_name}' for '{name}'. "
                    f"Available: {list(_DATASET_REGISTRY)}"
                )
            self._datasets[name] = cls(**entry)

    def load(self, name: str):
        if name not in self._datasets:
            raise KeyError(f"Dataset '{name}' not found in catalog")
        return self._datasets[name].load()

    def save(self, name: str, data) -> None:
        if name not in self._datasets:
            # Auto-create MemoryDataset for intermediate results
            self._datasets[name] = MemoryDataset()
        self._datasets[name].save(data)

    def exists(self, name: str) -> bool:
        if name not in self._datasets:
            return False
        return self._datasets[name].exists()

    def add(self, name: str, dataset: AbstractDataset) -> None:
        """Register a dataset programmatically."""
        self._datasets[name] = dataset

    def list(self) -> list[str]:
        """Return all registered dataset names."""
        return list(self._datasets.keys())
