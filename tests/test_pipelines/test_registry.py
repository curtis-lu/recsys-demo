import pytest

from recsys_tfb.pipelines import get_pipeline, list_pipelines
from recsys_tfb.core.pipeline import Pipeline


class TestPipelineRegistry:
    def test_get_existing_pipeline(self):
        pipe = get_pipeline("dataset")
        assert isinstance(pipe, Pipeline)

    def test_get_training_pipeline(self):
        pipe = get_pipeline("training")
        assert isinstance(pipe, Pipeline)

    def test_get_unknown_pipeline(self):
        with pytest.raises(KeyError, match="not found"):
            get_pipeline("nonexistent")

    def test_error_lists_available(self):
        with pytest.raises(KeyError, match="dataset"):
            get_pipeline("nonexistent")

    def test_list_pipelines(self):
        names = list_pipelines()
        assert "dataset" in names
        assert "training" in names
