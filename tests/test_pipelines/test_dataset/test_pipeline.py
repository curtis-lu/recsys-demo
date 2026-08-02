"""Tests for dataset building pipeline definition."""

from recsys_tfb.pipelines.dataset import create_pipeline
from recsys_tfb.pipelines.dataset import nodes_spark as nodes


class TestDatasetPipeline:
    def test_pipeline_without_calibration(self):
        pipeline = create_pipeline()
        # 1 validate + 4 key-selection + 1 fit + 1 apply_features + 4 build_model_input
        # + 2 filter (val/test) = 13
        assert len(pipeline.nodes) == 13

    def test_pipeline_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 13 base + 1 select_calibration_keys + 1 build_calibration_model_input = 15
        # (calibration is NOT filtered — keep all rows)
        assert len(pipeline.nodes) == 15

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "label_table", "sample_pool", "parameters"}

    def test_pipeline_outputs_without_calibration(self):
        pipeline = create_pipeline()
        expected = {
            "train_model_input", "train_dev_model_input",
            "val_model_input_unfiltered", "test_model_input_unfiltered",
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
            "val_model_input_unfiltered", "test_model_input_unfiltered",
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
        assert "filter_val_model_input" in names
        assert "filter_test_model_input" in names

    def test_node_names_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert "select_calibration_keys" in names
        assert "build_calibration_model_input" in names

    def test_default_parameters(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 13

    def test_filter_nodes_only_for_val_and_test(self):
        """train / train_dev / calibration go straight to *_model_input;
        only val and test get the group-positive filter."""
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert "filter_val_model_input" in names
        assert "filter_test_model_input" in names
        assert "filter_train_model_input" not in names
        assert "filter_train_dev_model_input" not in names
        assert "filter_calibration_model_input" not in names

    def test_filter_consumes_unfiltered_output(self):
        """Filter node input must be the build node's *_unfiltered output."""
        pipeline = create_pipeline()
        by_name = {n.name: n for n in pipeline.nodes}
        assert "val_model_input_unfiltered" in by_name["filter_val_model_input"].inputs
        assert by_name["filter_val_model_input"].outputs == ["val_model_input"]
        assert "test_model_input_unfiltered" in by_name["filter_test_model_input"].inputs
        assert by_name["filter_test_model_input"].outputs == ["test_model_input"]

    def test_validate_data_consistency_runs_first(self):
        pipeline = create_pipeline()
        assert pipeline.nodes[0].name == "validate_data_consistency"
        first = pipeline.nodes[0]
        assert sorted(first.inputs) == [
            "feature_table", "label_table", "parameters", "sample_pool"
        ]
        assert first.outputs == []

    def test_preprocessed_feature_table_feeds_all_splits(self):
        pipeline = create_pipeline(enable_calibration=True)
        build_nodes = [n for n in pipeline.nodes if n.name.startswith("build_") and n.name.endswith("_model_input")]
        for n in build_nodes:
            assert "preprocessed_feature_table" in n.inputs
            assert "preprocessor" in n.inputs


class TestNodeNameToFunctionBinding:
    """D22 — which function each node name actually runs.

    Node names are plain strings and are what ``--from-node`` / ``--only-node``
    address, what the runner logs, and what ADR-0002's incremental branch is
    reasoned about by name. The existing name tests assert membership, so a name
    attached to the *wrong function* passes them — as does a rename that silently
    moves a slicing entry point. This pins the pairing instead.

    Two pairings here are deliberately not identities and are the ones most
    likely to be "corrected" by mistake:

    - ``select_sample_keys`` runs ``select_train_keys``;
    - ``build_test_model_input`` / ``filter_test_model_input`` run the *test*
      wrappers, not the shared ``build_model_input`` /
      ``filter_groups_with_positives`` the other splits use, because the test
      branch is incremental (ADR-0002).
    """

    BASE_BINDINGS = {
        "validate_data_consistency": nodes.validate_data_consistency,
        "select_sample_keys": nodes.select_train_keys,
        "split_train_keys": nodes.split_train_keys,
        "select_val_keys": nodes.select_val_keys,
        "select_test_keys": nodes.select_test_keys,
        "fit_preprocessor_metadata": nodes.fit_preprocessor_metadata,
        "apply_preprocessor_to_features": nodes.apply_preprocessor_to_features,
        "build_train_model_input": nodes.build_model_input,
        "build_train_dev_model_input": nodes.build_model_input,
        "build_val_model_input": nodes.build_model_input,
        "build_test_model_input": nodes.build_test_model_input,
        "filter_val_model_input": nodes.filter_groups_with_positives,
        "filter_test_model_input": nodes.filter_test_model_input,
    }
    CALIBRATION_BINDINGS = {
        "select_calibration_keys": nodes.select_calibration_keys,
        "build_calibration_model_input": nodes.build_model_input,
    }

    def _bindings(self, pipeline):
        names = [n.name for n in pipeline.nodes]
        # Node names address slicing entry points, so a duplicate would make
        # --only-node ambiguous; assert uniqueness before collapsing to a dict.
        assert len(names) == len(set(names)), f"duplicate node names: {names}"
        return {n.name: n.func for n in pipeline.nodes}

    def test_every_node_name_runs_the_expected_function(self):
        # Equality, not per-key lookups: an added node, a removed one and a
        # renamed one all fail, where `in` checks only ever catch removal.
        assert self._bindings(create_pipeline()) == self.BASE_BINDINGS

    def test_calibration_adds_exactly_two_bound_nodes(self):
        assert self._bindings(create_pipeline(enable_calibration=True)) == {
            **self.BASE_BINDINGS, **self.CALIBRATION_BINDINGS,
        }

    def test_the_four_build_nodes_share_one_function(self):
        """Not a restatement of the table: it is *why* names carry the meaning.

        train / train_dev / val / calibration all run the same
        ``build_model_input``, so the node name is the only thing distinguishing
        them — which is what makes a typo in one a silent topology change rather
        than an import error.
        """
        bindings = self._bindings(create_pipeline(enable_calibration=True))
        shared = {
            name for name, func in bindings.items()
            if func is nodes.build_model_input
        }
        assert shared == {
            "build_train_model_input", "build_train_dev_model_input",
            "build_val_model_input", "build_calibration_model_input",
        }
