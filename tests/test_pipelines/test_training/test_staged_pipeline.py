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


class TestStagedStage2Pipeline:
    def test_stage2_dag_adds_val_cache_and_stage2_node(self):
        p = create_pipeline(model_structure="staged", stage2_mode="lambdarank")
        names = [n.func.__name__ for n in p.nodes]
        assert "cache_val_model_input" in names
        assert "train_stage2_model" in names
        s1 = next(n for n in p.nodes if n.func.__name__ == "train_staged_model")
        assert s1.outputs == ["stage1_model", "stage1_groups_report"]
        s2 = next(n for n in p.nodes if n.func.__name__ == "train_stage2_model")
        assert s2.outputs == ["model", "stage2_report"]
        assert s2.inputs == [
            "stage1_model", "stage1_groups_report", "train_parquet_handle",
            "train_dev_parquet_handle", "val_parquet_handle",
            "preprocessor_view", "parameters"]

    def test_stage2_none_dag_unchanged(self):
        p = create_pipeline(model_structure="staged", stage2_mode="none")
        names = [n.func.__name__ for n in p.nodes]
        assert "train_stage2_model" not in names
        assert "cache_val_model_input" not in names
        s1 = next(n for n in p.nodes if n.func.__name__ == "train_staged_model")
        assert s1.outputs == ["model", "stage1_groups_report"]

    def test_shared_dag_ignores_stage2_mode(self):
        a = create_pipeline(model_structure="shared")
        b = create_pipeline(model_structure="shared", stage2_mode="lambdarank")
        assert [n.func.__name__ for n in a.nodes] == \
               [n.func.__name__ for n in b.nodes]


class TestStagedDiagnosticsShape:
    def test_none_shape_has_overview_stats_group_runner_and_mlflow(self):
        p = create_pipeline(model_structure="staged", stage2_mode="none")
        names = [n.func.__name__ for n in p.nodes]
        assert "compute_stage1_overview" in names
        assert "compute_feature_statistics" in names
        assert "compute_staged_group_diagnostics" in names
        assert "log_staged_experiment" in names
        assert "compute_shap_diagnostics" not in names
        assert "compute_quadrant_profiles" not in names
        log_node = next(n for n in p.nodes
                        if n.func.__name__ == "log_staged_experiment")
        assert "staged_group_diagnostics" in log_node.inputs

    def test_stage2_shape_reuses_booster_diagnostics_nodes(self):
        p = create_pipeline(model_structure="staged", stage2_mode="lambdarank")
        names = [n.func.__name__ for n in p.nodes]
        for expected in ("compute_stage1_overview", "compute_feature_statistics",
                         "compute_feature_importance", "compute_gain_ledger",
                         "compute_shap_diagnostics", "select_shap_population",
                         "compute_quadrant_profiles", "compute_quadrant_cases",
                         "log_staged_experiment"):
            assert expected in names
        assert "compute_staged_group_diagnostics" not in names
        assert "log_experiment" not in names

    def test_shared_shape_unchanged(self):
        p = create_pipeline()
        names = [n.func.__name__ for n in p.nodes]
        assert "log_experiment" in names and "log_staged_experiment" not in names
