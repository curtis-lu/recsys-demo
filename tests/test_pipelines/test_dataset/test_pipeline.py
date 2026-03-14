"""Tests for dataset building pipeline definition."""

from recsys_tfb.pipelines.dataset import create_pipeline


class TestDatasetPipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 5

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "label_table", "parameters"}

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {"X_train", "y_train", "X_val", "y_val", "preprocessor",
                    "sample_keys", "train_keys", "val_keys", "train_set", "val_set"}
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "select_sample_keys" in names
        assert "split_keys" in names
        assert "build_train_dataset" in names
        assert "build_val_dataset" in names
        assert "prepare_model_input" in names
