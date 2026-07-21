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
    ("training", ()): {
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
    },
    ("inference", ()): {
        # scoring_dataset is memory-only by design (cheap Spark transform)
        "rank_predictions": {"build_scoring_dataset"},
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

    def test_hpo_model_sidecar_isolated_from_final_model(self):
        # ModelAdapterDataset writes model_meta.json next to its filepath;
        # hpo_best_model must live in its own directory.
        cfg = yaml.safe_load(
            (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
        )
        model_dir = Path(cfg["model"]["filepath"]).parent
        hpo_dir = Path(cfg["hpo_best_model"]["filepath"]).parent
        assert model_dir != hpo_dir

    def test_node_names_unique_within_each_pipeline(self):
        # slice_from/_node_index resolve nodes BY NAME (first match wins);
        # duplicate names would silently slice from the wrong node.
        for (pipeline_name, kwargs_items) in RESUME_CONTRACTS:
            pipe = get_pipeline(pipeline_name, **dict(kwargs_items))
            names = [n.name for n in pipe.nodes]
            assert len(names) == len(set(names)), (
                f"{pipeline_name}: duplicate node names {names}"
            )
