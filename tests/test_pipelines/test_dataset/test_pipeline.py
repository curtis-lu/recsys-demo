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
    def test_pipeline_node_count(self):
        pipeline = create_pipeline()
        # 3 validate (Layer-2 data gate + B8 precision gate + B10 grain gate)
        # + 4 key-selection + 1 fit + 1 apply_features + 4 build_model_input
        # + 2 filter keys (train/train_dev) + 2 filter model_input (val/test)
        # = 17
        assert len(pipeline.nodes) == 17

    def test_create_pipeline_takes_no_calibration_switch(self):
        """#414 removed the branch. A stale caller passing the old kwarg must
        fail rather than build the 15-node pipeline and look like it worked."""
        with pytest.raises(TypeError):
            create_pipeline(enable_calibration=True)

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {
            "feature_table", "label_table", "sample_pool", "parameters",
            "preprocessed_feature_table_month_plan",
            "test_keys_month_plan",
            "test_model_input_month_plan",
            # Registered by the CLI for every deployment — `None` for the table
            # when none is declared (ADR-0026).
            "candidate_feature_table",
            # The run mode, the one fact the precision gate cannot work out:
            # no month list is injected, each node works out its own months
            # (ADR-0029 decision 2).
            "only_test_months",
            # The preprocessor file already on disk, read by the fit before it
            # overwrites it (#379, B19) — an optional catalog entry.
            "preprocessor_on_disk",
        }

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "train_model_input", "train_dev_model_input",
            "val_model_input_unfiltered", "test_model_input_unfiltered",
            "val_model_input", "test_model_input",
            "preprocessor", "category_mappings",
            "preprocessed_feature_table", "numeric_precision_report",
            "model_input_grain_report",
            "sample_keys", "train_keys", "train_dev_keys", "val_keys", "test_keys",
            "train_keys_unfiltered", "train_dev_keys_unfiltered",
        }
        assert pipeline.outputs == expected

    def test_no_calibration_artifact_is_produced(self):
        """The two Hive tables #414 removed. Named rather than left to the
        equality above, because a reader asking "did the calibration tables
        really stop being written" should find the answer, not infer it."""
        pipeline = create_pipeline()
        assert "calibration_keys" not in pipeline.outputs
        assert "calibration_model_input" not in pipeline.outputs

    def test_node_names(self):
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
        assert "filter_train_keys" in names
        assert "filter_train_dev_keys" in names

    def test_default_parameters(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 17

    def test_train_side_is_drawn_on_the_keys_not_on_model_input(self):
        """ADR-0025 decision 3 for train / train_dev runs on the keys, before
        the builds: B10 pins each train-side model_input's row count to the keys
        table it was built from, so a drop after the build would break the
        pairing on purpose. val / test keep their model_input filter."""
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "filter_train_model_input" not in names
        assert "filter_train_dev_model_input" not in names
        by_name = {n.name: n for n in pipeline.nodes}
        assert by_name["split_train_keys"].outputs == [
            "train_keys_unfiltered", "train_dev_keys_unfiltered",
        ]
        for split in ("train", "train_dev"):
            node = by_name[f"filter_{split}_keys"]
            assert node.inputs == [
                f"{split}_keys_unfiltered", "label_table", "parameters",
            ]
            # The landed keys B10 reads are this node's output, and they are
            # exactly what the build node reads.
            assert node.outputs == [f"{split}_keys"]
            assert f"{split}_keys" in by_name[f"build_{split}_model_input"].inputs

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
            "candidate_feature_table", "feature_table", "label_table",
            "parameters", "sample_pool",
        ]
        assert first.outputs == []

    def test_preprocessed_feature_table_feeds_all_splits(self):
        pipeline = create_pipeline()
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
    - ``build_test_model_input`` runs the *test* wrapper, not the train one,
      because it reads its plan's months and has to re-scope the keys it
      reads back from a persistent Hive table (ADR-0002);
    - ``build_val_model_input`` runs the *val* wrapper: it reads the val
      months, and with the item list counted from the data it warns about
      val's new items off the landed keys (#379), which the train wrapper
      must not do — the train items are the list;
    - ``filter_test_model_input`` runs its *own* function rather than val's,
      although the decisions are the same: the key each reads
      (``dataset.{val,test}_zero_positive_group_ratio``) is the one answer that
      differs, and reading the other split's key would raise nothing. Neither
      repeats the month scoping — that is done by then (ADR-0007);
    - ``filter_train_keys`` and ``filter_train_dev_keys`` share one function:
      one key governs both (ADR-0025 decision 3).
    """

    BASE_BINDINGS = {
        "validate_data_consistency": nodes.validate_data_consistency,
        "select_sample_keys": nodes.select_train_keys,
        "split_train_keys": nodes.split_train_keys,
        "filter_train_keys": nodes.filter_train_keys,
        "filter_train_dev_keys": nodes.filter_train_keys,
        "select_val_keys": nodes.select_val_keys,
        "select_test_keys": nodes.select_test_keys,
        "fit_preprocessor_metadata": nodes.fit_preprocessor_metadata,
        "apply_preprocessor_to_features": nodes.apply_preprocessor_to_features,
        "validate_numeric_precision": nodes.validate_numeric_precision,
        "build_train_model_input": nodes.build_train_model_input,
        "build_train_dev_model_input": nodes.build_train_model_input,
        "build_val_model_input": nodes.build_val_model_input,
        "build_test_model_input": nodes.build_test_model_input,
        "filter_val_model_input": nodes.filter_val_model_input,
        "filter_test_model_input": nodes.filter_test_model_input,
        "validate_model_input_grain": nodes.validate_model_input_grain,
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

    def test_the_two_train_build_nodes_share_one_function(self):
        """Not a restatement of the table: it is *why* names carry the meaning.

        train / train_dev run the same ``build_train_model_input``, so the
        node name is the only thing distinguishing them — which is what makes
        a typo in one a silent topology change rather than an import error.
        val ran it too until #379 gave val its own wrapper (the new-item
        warning); it would now also read the train months.
        """
        bindings = self._bindings(create_pipeline())
        shared = {
            name for name, func in bindings.items()
            if func is nodes.build_train_model_input
        }
        assert shared == {
            "build_train_model_input", "build_train_dev_model_input",
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
        "apply_preprocessor_to_features": {"preprocessed_feature_table"},
        # The B8 gate reads the same plan as the node that writes the table it
        # checks — that shared plan is what makes "the months this run added get
        # checked, and only those" true by construction rather than by comment.
        # It also reads the test build's plan: the candidate-level feature
        # table is checked over the months the builds read, and the test
        # build reads that plan's (ADR-0029 decision 2).
        "validate_numeric_precision": {
            "preprocessed_feature_table", "test_model_input",
        },
        "select_test_keys": {"test_keys"},
        "build_test_model_input": {"test_model_input"},
    }

    @staticmethod
    def _plan_inputs(node):
        return {i for i in node.inputs if i.endswith("_month_plan")}

    def test_each_incremental_node_follows_its_own_artifacts_plan(self):
        by_name = {n.name: n for n in create_pipeline().nodes}
        for node_name, artifacts in self.EXPECTED_PLAN.items():
            assert self._plan_inputs(by_name[node_name]) == {
                month_plan_input(a) for a in artifacts
            }, f"{node_name} follows the wrong month plan"

    def test_no_other_node_takes_a_month_plan(self):
        # The complement of the table above: a plan handed to a non-incremental
        # node would silently start skipping months of train / val.
        by_name = {n.name: n for n in create_pipeline().nodes}
        assert {
            name for name, node in by_name.items() if self._plan_inputs(node)
        } == set(self.EXPECTED_PLAN)

    def test_every_declared_plan_is_one_the_cli_injects(self):
        # A typo'd plan name is caught by the runner's input check at run time;
        # this catches it in a second, without Spark.
        pipeline = create_pipeline()
        declared = {i for n in pipeline.nodes for i in self._plan_inputs(n)}
        assert declared <= {month_plan_input(d) for d in INCREMENTAL_DATASETS}

    def test_plans_are_pipeline_inputs_not_node_outputs(self):
        # They enter through the catalog. If some node ever produced one, the
        # runner's "is this input available" check would stop failing loud when
        # the CLI forgets to inject — and a forgotten plan means a silent full
        # rebuild, which is the expensive direction to fail in.
        pipeline = create_pipeline()
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
        names = [n.name for n in create_pipeline().nodes]
        gate = names.index("validate_numeric_precision")
        builds = [i for i, n in enumerate(names) if n.startswith("build_")]
        assert builds, "no build_* nodes found -- the guard would be vacuous"
        assert gate < min(builds), (
            f"precision gate at {gate} runs after a model_input build: {names}"
        )

    def test_the_test_filter_takes_no_month_plan(self):
        # ADR-0007: the defensive month filter that used to live in
        # filter_test_model_input is gone — its input is already scoped. It
        # has its own function again (ADR-0025: it reads test's ratio key),
        # which is exactly why the absence of a plan input is pinned here
        # rather than inherited from val.
        by_name = {n.name: n for n in create_pipeline().nodes}
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

    def test_default_shape_is_unchanged_by_the_parameter(self):
        # Not covered by TestDatasetPipeline's 17: that calls create_pipeline
        # without the kwarg, so it would still pass if False were not the
        # default. Spell the default out.
        assert len(create_pipeline(only_test_months=False).nodes) == 17

    def test_the_list_matches_the_dag_derived_test_chain(self):
        """Drift guard: the list == what the DAG says the test chain is.

        Derivation mirrors what a month-aware ``can_load`` reports mid-run
        (ADR-0012): a persisted artifact that is missing *this run's* months
        answers "no", so its producer is upstream of the test chain.

        Both gates are added separately, and that is not a fudge — it is the
        one structural fact this test cannot derive. ``_slice_with_expansion``
        walks *backwards* from an artifact somebody needs, so a node nothing
        downstream consumes is unreachable however useful it is:
        ``validate_data_consistency`` because it has no output at all, and
        ``validate_numeric_precision`` because its output
        (``numeric_precision_report``) is a diagnostic with no consumer. Both
        therefore have to be named here.

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

        full = create_pipeline()
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


class TestGrainGateWiring:
    """B10's node, and the one thing its shape costs.

    ``inputs=`` is a list of literals rather than something built at call time,
    because a dynamically built one would drop the node out of the AST audit
    that A1/A5/A6 run on (``test_static_coverage_floor``). Until #414 that cost
    a second, duplicated spelling of the list for the calibration branch; with
    the branch gone there is one list and no duplication to guard.
    """

    def _grain_node(self, pipeline):
        (node,) = [
            n for n in pipeline.nodes if n.name == "validate_model_input_grain"
        ]
        return node

    def test_the_pairing_is_keys_then_its_own_model_input(self):
        # Getting a pair crossed (train_keys against train_dev_model_input) is
        # the failure the ticket named: the gate would be always-false rather
        # than absent. The order is load-bearing because the Runner binds
        # inputs positionally onto the node function's parameters.
        node = self._grain_node(create_pipeline())
        assert node.inputs == [
            "train_keys", "train_model_input",
            "train_dev_keys", "train_dev_model_input",
            "parameters",
        ]
        import inspect

        from recsys_tfb.pipelines.dataset import nodes

        params = list(
            inspect.signature(nodes.validate_model_input_grain).parameters)
        assert params == node.inputs

    def test_it_runs_after_every_build_node(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        gate = names.index("validate_model_input_grain")
        for build in [n for n in names if n.startswith("build_")]:
            assert names.index(build) < gate, build

    def test_it_is_not_a_zero_output_node(self):
        # A7/R3: a zero-output node is silently skipped by slicing (F5) and
        # needs a registered exception. This one carries a report instead.
        assert self._grain_node(create_pipeline()).outputs == [
            "model_input_grain_report"]

    def test_only_test_months_leaves_it_out(self):
        # It gates train / train_dev, neither of which that mode builds. Keeping it would make the mode fail on missing inputs.
        names = [n.name for n in create_pipeline(only_test_months=True).nodes]
        assert "validate_model_input_grain" not in names


class TestCandidateFeatureTableWiring:
    """Where the candidate-level feature table reaches (ADR-0026), and that it
    and the precision gate's two trailing inputs bind to the right parameters.

    They go last on every node that takes them, as optional trailing
    parameters, because the Runner binds ``inputs`` positionally. A node whose
    list put them anywhere else would hand the table to another parameter —
    and the ``=None`` defaults would swallow the arity mismatch instead of
    raising. Which months each build reads of it is not wiring any more: each
    build works it out (``test_month_scoped_reads.py``).
    """

    TABLE = "candidate_feature_table"

    BUILDS = {
        "build_train_model_input", "build_train_dev_model_input",
        "build_val_model_input", "build_test_model_input",
    }

    def _by_name(self):
        return {n.name: n for n in create_pipeline().nodes}

    def test_the_table_reaches_the_gate_the_fit_the_precision_check_and_every_build(self):
        assert {
            name for name, node in self._by_name().items() if self.TABLE in node.inputs
        } == {
            "validate_data_consistency", "fit_preprocessor_metadata",
            "validate_numeric_precision", *self.BUILDS,
        }

    def test_the_precision_gate_binds_the_test_plan_and_the_run_mode_to_their_own_parameters(self):
        """Swapped, the plan would land in ``only_test_months`` — a non-empty
        tuple, so truthy — and every full run would check the test months
        alone, raising nothing."""
        import inspect

        node = self._by_name()["validate_numeric_precision"]
        params = list(inspect.signature(node.func).parameters)
        assert node.inputs.index("test_model_input_month_plan") == params.index(
            "test_month_plan")
        assert node.inputs.index("only_test_months") == params.index(
            "only_test_months")

    def test_the_table_binds_to_the_parameter_of_its_own_name(self):
        import inspect

        for node in create_pipeline().nodes:
            if self.TABLE in node.inputs:
                params = list(inspect.signature(node.func).parameters)
                assert node.inputs.index(self.TABLE) == params.index(self.TABLE), node.name
