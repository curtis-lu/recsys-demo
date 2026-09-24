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
        # The val / test builds re-run from here and always did: they used to
        # sort *after* build_train_model_input and ride in as "every node after
        # it". Since #429 the train keys come out of filter_train_keys, which
        # becomes ready only after apply_preprocessor_to_features has already
        # queued the val / test builds, so they now sort *before* it and are
        # pulled back by their memory-only outputs instead. Same seven nodes
        # re-run either way; only the column they are reported in moved.
        "build_train_model_input": {
            "build_val_model_input",
            "build_test_model_input",
        },
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
        "compute_test_metrics": set(),
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
    ("inference", ()): {
        # score_manifest is memory-only, so resuming at rank re-runs the
        # scoring node. That is cheap *because* scoring resumes: every chunk's
        # partition already exists, so it lists the metastore once and writes
        # nothing. Training's twin of this, predict_manifest feeding
        # compute_test_metrics, was landed in issue #233 because its predict
        # node is *not* cheap to resume; the two stay deliberately different.
        # Issue #195 asked the adjacent question — where the skip list goes
        # after the process exits — and was answered WITHOUT landing this
        # entry: the scoring node emits a second output, score_chunk_report,
        # which no node reads. So this contract is unchanged on purpose, and
        # landing score_manifest is still the thing not to do
        # (docs/pipelines/inference.md section 7.4).
        "rank_predictions": {"predict_and_write_scores"},
        # The resume point the landed intermediate table buys (ADR-0010's
        # "consequences"): scoring reads a persisted feature table, so nothing
        # upstream has to re-run — no population/feature join, no preprocessing.
        "predict_and_write_scores": set(),
    },
    # Monitoring mode (no post_training): no registry diagnosis is wired
    # (ADR-0018 decision 5), so generate_report's pages input comes from the
    # zero-read no_diagnosis_pages stub — memory-only, and free to re-run.
    ("evaluation", ()): {
        # Redrawing the report re-runs no Spark node (ADR-0018 decision 2):
        # generate_report reads four landed JSONs plus parameters, and its
        # pages list comes from the zero-read stub. Before evaluation_metrics
        # and baseline_metrics had catalog entries this set also held
        # prepare_eval_data, compute_metrics and compute_baseline_metrics — a
        # "redraw" re-joined the predictions and recomputed every metric.
        #
        # Why the cheaper contract is accepted. What the redraw reads back from
        # disk is checked, not trusted: generate_report refuses any landed
        # input whose config_fingerprint disagrees with the current computed
        # settings (ADR-0020 decision 2), so it cannot draw old metrics under
        # new settings. What the fingerprint does not see is data. After a
        # label_table backfill the old contract did recompute the metrics and
        # the baseline, but still reused the landed metric CI and report
        # aggregates, so that report already mixed two data states; the new
        # one reuses all four, so every number comes from one run. Picking up
        # new data takes --from-node prepare_eval_data or a full run
        # (docs/pipelines/evaluation.md section 7.4 says so).
        #
        # Un-land evaluation_metrics or baseline_metrics and this turns red.
        "generate_report": {"no_diagnosis_pages"},
        # The resume point ADR-0018 decision 1 buys: prepare_eval_data writes
        # enriched_eval_predictions, so resuming at the metrics reads the table
        # instead of re-joining the predictions. Before that the set also held
        # prepare_eval_data. What is left is memory-only and cheap: the
        # zero-read stub, and the diagnosis sample compute_metric_ci reads.
        # This stub catalog answers "the table exists"; the evaluated month
        # missing from it is the CLI's month plan's business (test_cli.py).
        "compute_metrics": {"draw_diagnosis_sample_node", "no_diagnosis_pages"},
    },
    ("evaluation", (("post_training", True),)): {
        # Same landing and reasoning as above. render_diagnosis_pages stays
        # memory-only on purpose (its output is a list of paths, meaningful
        # only for the run that wrote them); it re-renders the pages from the
        # landed diagnosis JSONs, the half-second path. When those JSONs are
        # not on disk the slice pulls the diagnosis nodes back as well, which
        # is correct and outside this contract: contracts describe the
        # "previous full run succeeded" scenario.
        "generate_report": {"render_diagnosis_pages"},
        # Same unlock as the monitoring entry: the table is read, not re-joined.
        "compute_metrics": {"draw_diagnosis_sample_node"},
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

    def test_predict_manifest_lands_in_the_version_directory(self):
        # The catalog half of the compute_test_metrics contract above, plus
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
        # that sidecar is what picks the adapter back up on load. Any two of
        # these sharing a directory would overwrite each other's, so each needs
        # its own. `trained_model` was a third entry until #411: it existed only
        # to hold finalize_model's output while calibrate_model wrapped it.
        cfg = yaml.safe_load(
            (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
        )
        dirs = {
            name: Path(cfg[name]["filepath"]).parent
            for name in ("model", "hpo_best_model")
        }
        assert len(set(dirs.values())) == len(dirs), dirs
        assert "trained_model" not in cfg

    def test_node_names_unique_within_each_pipeline(self):
        # slice_from/_node_index resolve nodes BY NAME (first match wins);
        # duplicate names would silently slice from the wrong node.
        for (pipeline_name, kwargs_items) in RESUME_CONTRACTS:
            pipe = get_pipeline(pipeline_name, **dict(kwargs_items))
            names = [n.name for n in pipe.nodes]
            assert len(names) == len(set(names)), (
                f"{pipeline_name}: duplicate node names {names}"
            )
