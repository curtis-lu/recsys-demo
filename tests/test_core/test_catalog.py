import pytest

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.io.pickle_dataset import PickleDataset


class TestDataCatalog:
    def test_from_config(self, tmp_path):
        config = {
            "my_data": {
                "type": "ParquetDataset",
                "filepath": str(tmp_path / "test.parquet"),
                "backend": "pandas",
            }
        }
        catalog = DataCatalog(config)
        assert "my_data" in catalog.list()

    def test_unknown_type(self):
        config = {"ds": {"type": "UnknownDataset"}}
        with pytest.raises(ValueError, match="Unknown dataset type"):
            DataCatalog(config)

    def test_missing_type(self):
        config = {"ds": {"filepath": "/tmp/x"}}
        with pytest.raises(ValueError, match="missing 'type'"):
            DataCatalog(config)

    def test_load_unregistered(self):
        catalog = DataCatalog()
        with pytest.raises(KeyError, match="not found"):
            catalog.load("nonexistent")

    def test_save_and_load(self, tmp_path):
        filepath = str(tmp_path / "test.pkl")
        catalog = DataCatalog()
        catalog.add("model", PickleDataset(filepath=filepath))
        catalog.save("model", {"weights": [1, 2, 3]})
        loaded = catalog.load("model")
        assert loaded == {"weights": [1, 2, 3]}

    def test_add_dataset(self):
        catalog = DataCatalog()
        ds = MemoryDataset(data=42)
        catalog.add("answer", ds)
        assert catalog.load("answer") == 42

    def test_auto_memory_dataset_on_save(self):
        catalog = DataCatalog()
        catalog.save("intermediate", [1, 2, 3])
        assert catalog.load("intermediate") == [1, 2, 3]

    def test_exists(self, tmp_path):
        catalog = DataCatalog()
        assert catalog.exists("missing") is False
        catalog.save("present", 42)
        assert catalog.exists("present") is True
