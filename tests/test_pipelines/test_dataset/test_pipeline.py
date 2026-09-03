"""Tests for dataset building pipeline definition."""

from pathlib import Path

import pytest
import yaml

from recsys_tfb.pipelines.dataset import create_pipeline
from recsys_tfb.pipelines.dataset import nodes
from recsys_tfb.pipelines.dataset import pipeline as dataset_pipeline
from recsys_tfb.pipelines.dataset.month_plans import (
    INCREMENTAL_DATASETS,
    month_plan_input,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


class TestDatasetPipeline:
    def test_pipeline_without_calibration(self):
        pipeline = create_pipeline()
        # 2 validate (Layer-2 data gate + B8 precision gate) + 4 key-selection
        # + 1 fit + 1 apply_features + 4 build_model_input
        # + 2 filter (val/test) = 14
        assert len(pipeline.nodes) == 14

    def test_pipeline_with_calibration(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 14 base + 1 select_calibration_keys + 1 build_calibration_model_input = 16
        # (calibration is NOT filtered — keep all rows)
        assert len(pipeline.nodes) == 16

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {
            "feature_table", "label_table", "sample_pool", "parameters",
            "preprocessed_feature_table_month_plan",
            "test_keys_month_plan",
            "test_model_input_month_plan",
        }

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
        assert len(pipeline.nodes) == 14

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
    - ``build_test_model_input`` runs the *test* wrapper, not the shared
      ``build_model_input`` the other splits use, because it has to re-scope
      the keys it reads back from a persistent Hive table (ADR-0002);
    - ``filter_test_model_input``, by contrast, runs the *same*
      ``filter_groups_with_positives`` as val — the month scoping is already
      done by then and repeating it would guard nothing (ADR-0007).
    """

    BASE_BINDINGS = {
        "validate_data_consistency": nodes.validate_data_consistency,
        "select_sample_keys": nodes.select_train_keys,
        "split_train_keys": nodes.split_train_keys,
        "select_val_keys": nodes.select_val_keys,
        "select_test_keys": nodes.select_test_keys,
        "fit_preprocessor_metadata": nodes.fit_preprocessor_metadata,
        "apply_preprocessor_to_features": nodes.apply_preprocessor_to_features,
        "validate_numeric_precision": nodes.validate_numeric_precision,
        "build_train_model_input": nodes.build_model_input,
        "build_train_dev_model_input": nodes.build_model_input,
        "build_val_model_input": nodes.build_model_input,
        "build_test_model_input": nodes.build_test_model_input,
        "filter_val_model_input": nodes.filter_groups_with_positives,
        "filter_test_model_input": nodes.filter_groups_with_positives,
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


class TestMonthPlanWiring:
    """#152 — which node follows which month plan, read off the definition.

    Being incremental used to be invisible here: the decision lived inside four
    node bodies, keyed off a magic ``parameters`` entry, so the only way to see
    that ``select_test_keys`` and ``build_test_model_input`` follow *different*
    plans was to read both functions. Now it is one line of ``inputs`` each —
    and mis-wiring one to the other's plan is a same-shaped, still-runnable
    pipeline, so eyeballing the diff is not enough. This is the test that fails.
    """

    #: node name -> the artifact whose plan scopes it. Not derivable from the
    #: node's own output: build_test_model_input writes
    #: ``test_model_input_unfiltered`` but is gated on ``test_model_input``,
    #: the persistent table the pair of nodes ultimately produces.
    EXPECTED_PLAN = {
        "apply_preprocessor_to_features": "preprocessed_feature_table",
        # The B8 gate reads the same plan as the node that writes the table it
        # checks — that shared plan is what makes "the months this run added get
        # checked, and only those" true by construction rather than by comment.
        "validate_numeric_precision": "preprocessed_feature_table",
        "select_test_keys": "test_keys",
        "build_test_model_input": "test_model_input",
    }

    @staticmethod
    def _plan_inputs(node):
        return {i for i in node.inputs if i.endswith("_month_plan")}

    def test_each_incremental_node_follows_its_own_artifacts_plan(self):
        by_name = {n.name: n for n in create_pipeline(enable_calibration=True).nodes}
        for node_name, artifact in self.EXPECTED_PLAN.items():
            assert self._plan_inputs(by_name[node_name]) == {
                month_plan_input(artifact)
            }, f"{node_name} follows the wrong month plan"

    def test_no_other_node_takes_a_month_plan(self):
        # The complement of the table above: a plan handed to a non-incremental
        # node would silently start skipping months of train / val / calibration.
        by_name = {n.name: n for n in create_pipeline(enable_calibration=True).nodes}
        assert {
            name for name, node in by_name.items() if self._plan_inputs(node)
        } == set(self.EXPECTED_PLAN)

    def test_every_declared_plan_is_one_the_cli_injects(self):
        # A typo'd plan name is caught by the runner's input check at run time;
        # this catches it in a second, without Spark.
        pipeline = create_pipeline(enable_calibration=True)
        declared = {i for n in pipeline.nodes for i in self._plan_inputs(n)}
        assert declared <= {month_plan_input(d) for d in INCREMENTAL_DATASETS}

    def test_plans_are_pipeline_inputs_not_node_outputs(self):
        # They enter through the catalog. If some node ever produced one, the
        # runner's "is this input available" check would stop failing loud when
        # the CLI forgets to inject — and a forgotten plan means a silent full
        # rebuild, which is the expensive direction to fail in.
        pipeline = create_pipeline(enable_calibration=True)
        for name in INCREMENTAL_DATASETS:
            assert month_plan_input(name) in pipeline.inputs
            assert month_plan_input(name) not in pipeline.outputs

    def test_the_precision_gate_runs_before_every_model_input_build(self):
        """Declaration order is what orders it, so a test has to hold the order.

        The gate and the ``build_model_input`` nodes share
        ``preprocessed_feature_table`` as their last unmet input, so Kahn queues
        them together and breaks the tie by list position
        (``core/pipeline.py``). Nothing in the DAG forces the gate first — it
        produces no artifact anyone consumes — so moving its entry down the list
        in ``pipeline.py`` would silently let a narrowed value land before the
        check that exists to stop it. That edit passes every other test here.
        """
        names = [n.name for n in create_pipeline(enable_calibration=True).nodes]
        gate = names.index("validate_numeric_precision")
        builds = [i for i, n in enumerate(names) if n.startswith("build_")]
        assert builds, "no build_* nodes found -- the guard would be vacuous"
        assert gate < min(builds), (
            f"precision gate at {gate} runs after a model_input build: {names}"
        )

    def test_the_test_filter_is_the_same_node_function_as_val(self):
        # ADR-0007: the defensive month filter that used to live in
        # filter_test_model_input is gone, so the two filter nodes differ only
        # in which frame they read.
        by_name = {n.name: n for n in create_pipeline().nodes}
        assert (
            by_name["filter_test_model_input"].func
            is by_name["filter_val_model_input"].func
        )
        assert self._plan_inputs(by_name["filter_test_model_input"]) == set()


class TestOnlyTestMonthsMode:
    """``--only-test-months``: the data gate plus the test chain (ADR-0013).

    A **mode** parameter on ``create_pipeline``, not a slice. It decides which
    nodes get built; ``--from-node`` / ``--only-node`` subset whatever was
    built. The two are orthogonal and compose, which is why the node set is
    asserted here (on the pipeline definition) and the composition is asserted
    in ``tests/test_cli.py``.

    Why a listed set rather than one derived from the DAG: ADR-0013. The list
    costs exactly one drift test —
    :meth:`test_the_list_matches_the_dag_derived_test_chain` — which is
    cheaper than the producer-map reasoning a two-name derivation needs.
    """

    EXPECTED = [
        "validate_data_consistency",
        "select_test_keys",
        "apply_preprocessor_to_features",
        "validate_numeric_precision",
        "build_test_model_input",
        "filter_test_model_input",
    ]

    def test_mode_keeps_exactly_the_data_gate_and_test_chain(self):
        # Ordered, not a set: node order is what the runner executes and what
        # `[plan] N of M` counts against, and the gate running first is the
        # whole point of it being in the list.
        pipeline = create_pipeline(only_test_months=True)
        assert [n.name for n in pipeline.nodes] == self.EXPECTED

    def test_calibration_does_not_interact_with_the_mode(self):
        # enable_calibration adds two nodes, neither on the test chain. The
        # factory default is `true` (conf/base/parameters_dataset.yaml), so
        # this is the combination the CLI actually builds.
        pipeline = create_pipeline(enable_calibration=True, only_test_months=True)
        assert [n.name for n in pipeline.nodes] == self.EXPECTED

    def test_default_shape_is_unchanged_by_the_new_parameter(self):
        # Not covered by TestDatasetPipeline's 14/16: those call create_pipeline
        # without the new kwarg, so they would still pass if False were not the
        # default. Spell the default out.
        assert len(create_pipeline(only_test_months=False).nodes) == 14
        assert len(
            create_pipeline(enable_calibration=True, only_test_months=False).nodes
        ) == 16

    def test_the_list_matches_the_dag_derived_test_chain(self):
        """Drift guard: the list == what the DAG says the test chain is.

        Derivation mirrors what a month-aware ``can_load`` reports mid-run
        (ADR-0012): a persisted artifact that is missing *this run's* months
        answers "no", so its producer is upstream of the test chain.

        Both gates are added separately, and that is not a fudge — it is the
        one structural fact this test cannot derive. ``_slice_with_expansion``
        builds its producer map from ``node.outputs``, so a node with
        ``outputs=None`` is never reachable by expansion, no matter what it
        validates. Every zero-output node on the test chain therefore has to be
        named here, and R3 in ``docs/agents/architecture-constraints.md`` is the
        list of the ones that exist.

        Fails when a node is added to the test chain and the list does not
        follow: the mode would then silently skip it.
        """
        catalog_defined = set(
            yaml.safe_load((REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text())
        ) | {"parameters"}

        def can_load(name: str) -> bool:
            if name in INCREMENTAL_DATASETS:
                return False  # this run adds a month these do not have yet
            return name in catalog_defined

        full = create_pipeline(enable_calibration=True)
        sliced, _ = full.slice_only("filter_test_model_input", can_load)
        derived = {n.name for n in sliced.nodes} | {
            "validate_data_consistency", "validate_numeric_precision",
        }

        assert derived == set(dataset_pipeline.ONLY_TEST_MONTHS_NODES)

    def test_a_name_the_pipeline_no_longer_has_fails_loud(self, monkeypatch):
        """A renamed node must raise, not quietly yield a shorter pipeline.

        dataset writing nothing still exits 0 (ADR-0012's opening failure
        mode), so "filter to whatever matches" would turn a rename into a
        silent no-op run that claims success.
        """
        monkeypatch.setattr(
            dataset_pipeline,
            "ONLY_TEST_MONTHS_NODES",
            ("validate_data_consistency", "select_test_keys_renamed"),
        )
        with pytest.raises(ValueError, match="select_test_keys_renamed"):
            create_pipeline(only_test_months=True)
