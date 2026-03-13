from recsys_tfb.io.pickle_dataset import PickleDataset


class TestPickleDataset:
    def test_save_and_load_dict(self, tmp_path):
        filepath = str(tmp_path / "test.pkl")
        ds = PickleDataset(filepath=filepath)
        data = {"key": "value", "nums": [1, 2, 3]}
        ds.save(data)
        loaded = ds.load()
        assert loaded == data

    def test_save_and_load_object(self, tmp_path):
        filepath = str(tmp_path / "obj.pkl")
        ds = PickleDataset(filepath=filepath)
        data = [1, "two", 3.0, None]
        ds.save(data)
        loaded = ds.load()
        assert loaded == data

    def test_exists(self, tmp_path):
        filepath = str(tmp_path / "test.pkl")
        ds = PickleDataset(filepath=filepath)
        assert ds.exists() is False
        ds.save({"a": 1})
        assert ds.exists() is True
