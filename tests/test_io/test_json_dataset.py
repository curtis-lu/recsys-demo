from recsys_tfb.io.json_dataset import JSONDataset


class TestJSONDataset:
    def test_save_and_load_dict(self, tmp_path):
        filepath = str(tmp_path / "test.json")
        ds = JSONDataset(filepath=filepath)
        data = {"prod_name": ["fx", "usd", "stock"]}
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
