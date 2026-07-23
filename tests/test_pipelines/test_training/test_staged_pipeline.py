from recsys_tfb.pipelines.training.pipeline import create_pipeline


def _node_names(pipeline):
    return [n.name for n in pipeline.nodes]


class TestStagedPipelineStructure:
    def test_shared_default_structure_unchanged(self):
        names = _node_names(create_pipeline(enable_calibration=False))
        assert "tune_hyperparameters" in names
        assert "train_staged_model" not in names

    def test_staged_replaces_hpo_and_finalize(self):
        names = _node_names(create_pipeline(
            enable_calibration=False, model_structure="staged"))
        assert "train_staged_model" in names
        for absent in ("tune_hyperparameters", "finalize_model",
                       "prepare_lgb_train_inputs", "calibrate_model",
                       "compute_shap_diagnostics", "log_experiment"):
            assert absent not in names, absent

    def test_staged_keeps_predict_and_map(self):
        names = _node_names(create_pipeline(
            enable_calibration=False, model_structure="staged"))
        assert "predict_and_write_test_predictions" in names
        assert "compute_test_mAP_spark" in names

    def test_staged_model_outputs_model_and_report(self):
        p = create_pipeline(enable_calibration=False, model_structure="staged")
        staged_node = next(n for n in p.nodes
                           if n.name == "train_staged_model")
        assert staged_node.outputs == ["model", "stage1_groups_report"]

    def test_staged_with_calibration_raises(self):
        import pytest
        with pytest.raises(ValueError, match="calibration"):
            create_pipeline(enable_calibration=True, model_structure="staged")
