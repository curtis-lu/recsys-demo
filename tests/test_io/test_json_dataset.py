import pytest

from recsys_tfb.io.json_dataset import JSONDataset


class TestJSONDataset:
    def test_save_and_load_dict(self, tmp_path):
        filepath = str(tmp_path / "test.json")
        ds = JSONDataset(filepath=filepath)
        data = {"prod_name": ["exchange_fx", "exchange_usd", "fund_stock"]}
        ds.save(data)
        loaded = ds.load()
        assert loaded == data

    def test_save_and_load_nested(self, tmp_path):
        filepath = str(tmp_path / "nested.json")
        ds = JSONDataset(filepath=filepath)
        data = {"mappings": {"prod_name": ["a", "b"]}, "count": 2}
        ds.save(data)
        loaded = ds.load()
        assert loaded == data

    def test_exists(self, tmp_path):
        filepath = str(tmp_path / "test.json")
        ds = JSONDataset(filepath=filepath)
        assert ds.exists() is False
        ds.save({"a": 1})
        assert ds.exists() is True

    def test_creates_parent_directory(self, tmp_path):
        filepath = str(tmp_path / "subdir" / "deep" / "test.json")
        ds = JSONDataset(filepath=filepath)
        ds.save({"key": "value"})
        assert ds.exists() is True
        assert ds.load() == {"key": "value"}

    def test_optional_missing_file_returns_none(self, tmp_path):
        filepath = str(tmp_path / "missing.json")
        ds = JSONDataset(filepath=filepath, optional=True)
        assert ds.exists() is False
        assert ds.load() is None

    def test_default_missing_file_raises(self, tmp_path):
        filepath = str(tmp_path / "missing.json")
        ds = JSONDataset(filepath=filepath)
        with pytest.raises(FileNotFoundError):
            ds.load()

    def test_optional_existing_file_loads_normally(self, tmp_path):
        filepath = str(tmp_path / "present.json")
        ds = JSONDataset(filepath=filepath, optional=True)
        ds.save({"a": 1})
        assert ds.load() == {"a": 1}
