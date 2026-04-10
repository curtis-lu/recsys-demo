"""Tests for dataset building pipeline definition."""

from recsys_tfb.pipelines.dataset import create_pipeline


class TestDatasetPipeline:
    def test_pipeline_without_calibration(self):
        pipeline = create_pipeline()
        # 4 key nodes + 4 build_dataset + 1 fit_preprocessor + 4 transform = 13
        assert len(pipeline.nodes) == 13

    def test_pipeline_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 13 base + 1 select_calibration_keys + 1 build_calibration_dataset
        # + 1 transform_calibration = 16
        assert len(pipeline.nodes) == 16

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "label_table", "sample_pool", "parameters"}

    def test_pipeline_outputs_without_calibration(self):
        pipeline = create_pipeline()
        expected = {
            "train_model_input", "train_dev_model_input",
            "val_model_input", "test_model_input",
            "preprocessor", "category_mappings",
            "sample_keys", "train_keys", "train_dev_keys", "val_keys", "test_keys",
            "train_set", "train_dev_set", "val_set", "test_set",
        }
        assert pipeline.outputs == expected

    def test_pipeline_outputs_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        expected = {
            "train_model_input", "train_dev_model_input",
            "calibration_model_input",
            "val_model_input", "test_model_input",
            "preprocessor", "category_mappings",
            "sample_keys", "train_keys", "train_dev_keys",
            "calibration_keys", "val_keys", "test_keys",
            "train_set", "train_dev_set", "calibration_set", "val_set", "test_set",
        }
        assert pipeline.outputs == expected

    def test_node_names_without_calibration(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "select_sample_keys" in names
        assert "split_train_keys" in names
        assert "select_val_keys" in names
        assert "select_test_keys" in names
        assert "build_train_dataset" in names
        assert "build_train_dev_dataset" in names
        assert "build_val_dataset" in names
        assert "build_test_dataset" in names
        assert "fit_preprocessor_metadata" in names
        assert "transform_train_to_model_input" in names
        assert "transform_val_to_model_input" in names
        assert "transform_test_to_model_input" in names

    def test_node_names_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert "select_calibration_keys" in names
        assert "build_calibration_dataset" in names
        assert "transform_calibration_to_model_input" in names

    def test_default_parameters(self):
        pipeline = create_pipeline()
        # 4 key nodes + 4 build_dataset + 1 fit_preprocessor + 4 transform = 13
        assert len(pipeline.nodes) == 13
