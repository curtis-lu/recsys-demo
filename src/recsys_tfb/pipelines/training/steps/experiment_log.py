"""What a training run records to MLflow, field by field.

``log_experiment`` keeps the run's lifecycle and the best-effort contract (a
tracking server that is down warns and lets training finish); this module keeps
the field names. The split follows how the two fail. A server being unreachable
is loud and already handled. A renamed field is silent: the run still succeeds,
and whoever was plotting the old name simply stops getting a line.

So every string here is wire format for readers outside this repo. Ten of them
happen to be pinned by ``tests/test_pipelines/test_training/test_nodes.py``;
``map_attr_*``, ``n_queries``, ``n_excluded_queries``, ``n_quadrant_cells`` and
``n_cases_rendered`` are pinned by nothing at all. Treat them all as published
— the test suite is not what makes them stable.

Nothing here decides *whether* to log; the node does. These functions assume
they are inside an active MLflow run.

**The diagnostics artifact upload is deliberately not here.** It stays in
``nodes.py`` beside the run, because ``test_direct_writes_match_registry``
(``tests/test_core/test_architecture_constraints.py``) only scans
``pipelines/**/nodes*.py`` for write calls: an ``mlflow.log_artifacts`` moved
into this module drops out of that registry and is never watched again. This is
the same call ADR-0014 decision 1 made for ``shutil.rmtree``, and it was not
theoretical — moving it here turned that audit red, which is how the rule
proved it was doing something.
"""

import mlflow


def log_run_params(
    best_params: dict,
    algorithm: str,
    final_model_strategy: str,
    best_iteration: int,
) -> None:
    """The configuration that produced this model.

    ``best_iteration`` is a metric rather than a param because MLflow's split
    is inputs vs. outcomes: the round count is what the search produced, not
    what it was handed. Metrics are also the side that plots across runs, which
    is the useful question to ask of it.
    """
    mlflow.log_params(best_params)
    mlflow.log_param("algorithm", algorithm)
    mlflow.log_param("final_model_strategy", final_model_strategy)
    mlflow.log_metric("best_iteration", best_iteration)


def log_evaluation_metrics(evaluation_results: dict) -> None:
    """The ranking scores, with per-item mAP fanned out one metric per item.

    The three headline keys are indexed, not ``.get``: they are required output
    of the evaluation node, so a missing one means a broken upstream rather
    than a metric to quietly skip. The resulting ``KeyError`` is caught by the
    node's best-effort wrapper like any other MLflow failure, which is why
    indexing here does not risk the pipeline.
    """
    mlflow.log_metric("overall_map", evaluation_results["overall_map"])

    for item, attr in evaluation_results.get("per_item_map_attr", {}).items():
        mlflow.log_metric(f"map_attr_{item}", attr)

    mlflow.log_metric("n_queries", evaluation_results["n_queries"])
    mlflow.log_metric("n_excluded_queries", evaluation_results["n_excluded_queries"])


def log_calibration_outcome(evaluation_results: dict) -> None:
    """Whether this model is calibrated, and what calibration changed.

    The presence of an ``uncalibrated`` block is the signal, rather than a
    separate flag: the calibration node is what puts the block there, so there
    is no second source that could disagree with the numbers beside it.
    """
    if "uncalibrated" in evaluation_results:
        mlflow.log_param("calibrated", True)
        mlflow.log_param("calibration_method", evaluation_results["calibration_method"])
        mlflow.log_metric(
            "uncalibrated_overall_map",
            evaluation_results["uncalibrated"]["overall_map"],
        )
    else:
        mlflow.log_param("calibrated", False)


def log_diagnostics_summary(
    feature_statistics: dict,
    feature_importance: dict,
    quadrant_profiles: dict,
    cases_manifest: dict,
) -> None:
    """One scalar per diagnostic, so runs are comparable without opening files.

    Each block is guarded because every diagnostic upstream is best-effort: a
    disabled or failed one arrives empty. An unguarded version would log a zero,
    and a zero reads as "we looked and found none" — the opposite of "we never
    looked".
    """
    if feature_importance:
        mlflow.log_metric("n_dead_features", len(feature_importance.get("dead_features", [])))
    if feature_statistics:
        mlflow.log_metric(
            "n_single_value_features",
            sum(1 for s in feature_statistics.values() if s.get("single_value")),
        )
        mlflow.log_metric(
            "n_high_null_features",
            sum(1 for s in feature_statistics.values() if s.get("high_null")),
        )
    if quadrant_profiles:
        n_cells = sum(len(v) for v in quadrant_profiles.values())
        mlflow.log_metric("n_quadrant_cells", n_cells)
    if cases_manifest:
        n_cases = sum(
            1 for it in cases_manifest.values() for cell in it.values()
            for r in cell.values() if r.get("rendered")
        )
        mlflow.log_metric("n_cases_rendered", n_cases)
