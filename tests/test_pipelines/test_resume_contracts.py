"""Resume-point contracts: pin the auto-included set for declared resume nodes.

Node inputs/outputs are descriptive (what a slice WILL re-run); these
contracts are normative (what it SHOULD only re-run). When a future change
adds a memory-only intermediate that degrades a declared resume point, this
test fails loudly — either persist the new dataset in catalog.yaml, or
consciously amend the contract here (visible in PR review).

Pure DAG + catalog-key stub; no Spark, no filesystem state. The stub assumes
every catalog-defined dataset exists — i.e. contracts describe the
"previous full run succeeded" scenario.
"""

from pathlib import Path

import yaml

from recsys_tfb.pipelines import get_pipeline

REPO_ROOT = Path(__file__).resolve().parents[2]


def _catalog_defined() -> set[str]:
    cfg = yaml.safe_load(
        (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
    )
    return set(cfg) | {"parameters"}


# (pipeline, frozen kwargs) -> {resume node -> exact allowed auto-included set}
RESUME_CONTRACTS = {
    ("dataset", ()): {
        # all upstream artifacts (keys tables, feature/label tables) persisted
        "fit_preprocessor_metadata": set(),
        "build_train_model_input": set(),
    },
    # --only-test-months builds a different pipeline, so it gets its own
    # contract: a mode is not a slice, and the resume costs inside it are not
    # the ones above. The pairing that matters is the second line — the
    # unfiltered frame is memory-only, so resuming at the filter re-runs the
    # expensive build. Pinned so that stays a deliberate cost.
    ("dataset", (("only_test_months", True),)): {
        "build_test_model_input": set(),
        "filter_test_model_input": {"build_test_model_input"},
    },
    ("training", ()): {
        # The diagnosis entry point (ADR-0014 decision 7). Before
        # compute_feature_statistics took `model` it had no model dependency at
        # all, so the topological sort placed a diagnosis of
        # data/models/${model_version}/ *ahead* of the node producing the model —
        # and "every node after it" then swept prepare_lgb_train_inputs,
        # tune_hyperparameters and finalize_model back in. 18 nodes re-ran to
        # regenerate a JSON of null rates. The edge moved it after finalize_model
        # and the slice fell to 13.
        #
        # What is left is the two memory-only parquet handles dragging their
        # cache nodes, blocked on cache.root being a relative path (ADR-0014
        # decision 7, second gap). Pinned so the next reduction shows up as a
        # diff.
        #
        # `predict_and_write_test_predictions` and `select_features` used to be
        # in this set too. predict_manifest was memory-only, so a diagnosis
        # resume re-ran the predict node -- which, even with every month
        # skipped, first pulls ~220M rows x 2 string columns into the driver to
        # list its partitions -- and that node dragged select_features with it,
        # because predict *applies* a model and so needs preprocessor_view.
        # Landing predict_manifest (issue #233) removed both.
        "compute_feature_statistics": {
            "cache_train_model_input",
            "cache_test_model_input",
        },
        # A resume point that only exists because predict_manifest lands:
        # recomputing the test metric now re-runs nothing at all. Both inputs
        # are loadable -- the Hive prediction table and the manifest -- so this
        # line is where removing the catalog entry turns red.
        "compute_test_mAP_spark": set(),
        # Same unlock, and the one ADR-0014 decision 7 names: a failed
        # diagnosis is recovered with `--from-node select_shap_population`,
        # and that recovery is only worth recommending while it re-runs
        # nothing. Its three data inputs are all landed datasets, so the
        # manifest was the last thing standing between it and zero.
        "select_shap_population": set(),
        # the "skip HPO, retrain final model" scenario: only cheap
        # view/handle builders may re-run, never tune_hyperparameters
        "finalize_model": {
            "select_features",
            "cache_train_model_input",
            "cache_train_dev_model_input",
            "cache_test_model_input",
        },
    },
    # calibration-enabled training is a real CLI path (training.calibration.enabled);
    # its finalize_model resume additionally rebuilds the calibration handle.
    ("training", (("enable_calibration", True),)): {
        "finalize_model": {
            "select_features",
            "cache_train_model_input",
            "cache_train_dev_model_input",
            "cache_test_model_input",
            "cache_calibration_model_input",
        },
        # The resume point `trained_model`'s catalog entry buys (ADR-0014,
        # "two resume points that got cheaper"): only cheap view/handle
        # builders may re-run. Un-land `trained_model` and finalize_model
        # comes back -- under final_model_strategy: refit_on_full that is a
        # full refit, which is exactly the cost this contract exists to see.
        # cache_test_model_input is here for a different reason than the two
        # above it: the forward slice keeps every node AFTER calibrate_model
        # too, so the test handle is pulled in by predict_and_write_test_
        # predictions downstream, not by anything calibrate_model needs.
        # cache_train_model_input joined that second group when
        # compute_feature_statistics gained its `model` input (ADR-0014
        # decision 7). Before that edge existed the node had no model
        # dependency at all, so the topological sort was free to place a
        # diagnosis of `data/models/${model_version}/` *ahead* of the node that
        # produces the model; it now lands after calibrate_model, where it
        # belongs, and drags its train handle into this slice. Accepted rather
        # than worked around: the ordering is the correct one, and the cost is a
        # Hive-to-local copy on a resume path, not a retrain.
        "calibrate_model": {
            "select_features",
            "cache_calibration_model_input",
            "cache_test_model_input",
            "cache_train_model_input",
        },
    },
    ("inference", ()): {
        # score_manifest is memory-only, so resuming at rank re-runs the
        # scoring node. That is cheap *because* scoring resumes: every chunk's
        # partition already exists, so it lists the metastore once and writes
        # nothing. Training's twin of this, predict_manifest feeding
        # compute_test_mAP_spark, was landed in issue #233 because its predict
        # node is *not* cheap to resume; the inference half is issue #195 and
        # still open, so the two are deliberately different today.
        "rank_predictions": {"predict_and_write_scores"},
        # The resume point the landed intermediate table buys (ADR-0010's
        # "consequences"): scoring reads a persisted feature table, so nothing
        # upstream has to re-run — no population/feature join, no preprocessing.
        "predict_and_write_scores": set(),
    },
    ("evaluation", ()): {
        # eval_predictions/metrics are memory-only: report regeneration
        # re-runs the metric chain. Documented cost, pinned here.
        # render_diagnosis_pages is also memory-only (its output is a list of
        # paths, meaningful only for the run that wrote them) — resuming at
        # generate_report re-renders the pages from the diagnosis JSONs, which
        # is the cheap half-second path, not a Spark job.
        "generate_report": {
            "prepare_eval_data",
            "compute_metrics",
            "compute_baseline_metrics",
            "render_diagnosis_pages",
        },
    },
}


class TestResumeContracts:
    def test_declared_resume_points_hold(self):
        defined = _catalog_defined()
        can_load = lambda name: name in defined
        failures = []
        for (pipeline_name, kwargs_items), contracts in RESUME_CONTRACTS.items():
            pipe = get_pipeline(pipeline_name, **dict(kwargs_items))
            for start, allowed in contracts.items():
                _, plan = pipe.slice_from(start, can_load)
                actual = set(plan.auto_included)
                if actual != allowed:
                    failures.append(
                        f"{pipeline_name}{dict(kwargs_items) or ''}::{start}: "
                        f"auto-included {dict(plan.auto_included)} "
                        f"!= contract {sorted(allowed)}.\n"
                        f"  New memory-only dataset degrading this resume point? "
                        f"Either persist it in conf/base/catalog.yaml or amend "
                        f"RESUME_CONTRACTS with justification."
                    )
        assert not failures, "\n".join(failures)

    def test_training_skip_hpo_requires_persisted_outputs(self):
        # Guard the catalog half of the contract: tune_hyperparameters'
        # three outputs must all be catalog-persisted.
        defined = _catalog_defined()
        for name in ("best_params", "best_iteration", "hpo_best_model"):
            assert name in defined, f"{name} must stay defined in catalog.yaml"

    def test_training_skip_finalize_requires_persisted_trained_model(self):
        # The catalog half of the calibrate_model contract above: without this
        # entry the slice pulls finalize_model back, which under
        # final_model_strategy: refit_on_full is a full refit.
        cfg = yaml.safe_load(
            (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
        )
        assert "trained_model" in cfg
        assert cfg["trained_model"]["type"] == "ModelAdapterDataset"

    def test_predict_manifest_lands_in_the_version_directory(self):
        # The catalog half of the compute_test_mAP_spark contract above, plus
        # where it lands. First level of the model version directory, not a
        # subdirectory: __main__._dir_artifacts lists that level only, so a
        # manifest under diagnostics/ would be missing from the `artifacts`
        # list of manifest.json -- the place someone looks to find out what a
        # run produced (same reasoning as the sample_weight_report entry).
        cfg = yaml.safe_load(
            (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
        )
        assert "predict_manifest" in cfg
        assert cfg["predict_manifest"]["type"] == "JSONDataset"
        assert (
            Path(cfg["predict_manifest"]["filepath"]).parent
            == Path("data/models/${model_version}")
        )

    def test_model_adapter_sidecars_do_not_share_a_directory(self):
        # ModelAdapterDataset writes model_meta.json next to its filepath, and
        # that sidecar carries the `calibrated` flag -- i.e. it decides how the
        # model is later LOADED. Any two of these sharing a directory would
        # overwrite each other's flag, so each needs its own.
        cfg = yaml.safe_load(
            (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
        )
        dirs = {
            name: Path(cfg[name]["filepath"]).parent
            for name in ("model", "hpo_best_model", "trained_model")
        }
        assert len(set(dirs.values())) == len(dirs), dirs

    def test_node_names_unique_within_each_pipeline(self):
        # slice_from/_node_index resolve nodes BY NAME (first match wins);
        # duplicate names would silently slice from the wrong node.
        for (pipeline_name, kwargs_items) in RESUME_CONTRACTS:
            pipe = get_pipeline(pipeline_name, **dict(kwargs_items))
            names = [n.name for n in pipe.nodes]
            assert len(names) == len(set(names)), (
                f"{pipeline_name}: duplicate node names {names}"
            )
