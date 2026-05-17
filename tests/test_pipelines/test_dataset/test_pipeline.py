"""Tests for dataset building pipeline definition."""

from recsys_tfb.pipelines.dataset import create_pipeline


class TestDatasetPipeline:
    def test_pipeline_without_calibration(self):
        pipeline = create_pipeline()
        # 1 validate + 4 key-selection + 1 fit + 1 apply_features + 4 build_model_input = 11
        assert len(pipeline.nodes) == 11

    def test_pipeline_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 11 base + 1 select_calibration_keys + 1 build_calibration_model_input = 13
        assert len(pipeline.nodes) == 13

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "label_table", "sample_pool", "parameters"}

    def test_pipeline_outputs_without_calibration(self):
        pipeline = create_pipeline()
        expected = {
            "train_model_input", "train_dev_model_input",
            "val_model_input", "test_model_input",
            "preprocessor", "category_mappings",
            "preprocessed_feature_table",
            "sample_keys", "train_keys", "train_dev_keys", "val_keys", "test_keys",
        }
        assert pipeline.outputs == expected

    def test_pipeline_outputs_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        expected = {
            "train_model_input", "train_dev_model_input",
            "calibration_model_input",
            "val_model_input", "test_model_input",
            "preprocessor", "category_mappings",
            "preprocessed_feature_table",
            "sample_keys", "train_keys", "train_dev_keys",
            "calibration_keys", "val_keys", "test_keys",
        }
        assert pipeline.outputs == expected

    def test_node_names_without_calibration(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "validate_data_consistency" in names
        assert "select_sample_keys" in names
        assert "split_train_keys" in names
        assert "select_val_keys" in names
        assert "select_test_keys" in names
        assert "fit_preprocessor_metadata" in names
        assert "apply_preprocessor_to_features" in names
        assert "build_train_model_input" in names
        assert "build_train_dev_model_input" in names
        assert "build_val_model_input" in names
        assert "build_test_model_input" in names

    def test_node_names_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert "select_calibration_keys" in names
        assert "build_calibration_model_input" in names

    def test_default_parameters(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 11

    def test_validate_data_consistency_runs_first(self):
        pipeline = create_pipeline()
        assert pipeline.nodes[0].name == "validate_data_consistency"
        first = pipeline.nodes[0]
        assert sorted(first.inputs) == ["label_table", "parameters", "sample_pool"]
        assert first.outputs == []
    def test_preprocessed_feature_table_feeds_all_splits(self):
        pipeline = create_pipeline(enable_calibration=True)
        build_nodes = [n for n in pipeline.nodes if n.name.startswith("build_") and n.name.endswith("_model_input")]
        for n in build_nodes:
            assert "preprocessed_feature_table" in n.inputs
            assert "preprocessor" in n.inputs
