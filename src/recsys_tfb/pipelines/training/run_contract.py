"""What the training command asks this package about a run, before and after it.

ADR-0030 decision 13: the parts of ``__main__.py``'s ``training()`` that only
the training pipeline understands live here, where they can be tested without
the CLI — the shape of ``pipelines/dataset/run_contract.py`` (ADR-0029
decision 11). The command keeps what every command does — load the config,
start Spark, resolve the catalog, write the manifests, run the pipeline, and
log what it decided (including the one-line notices about the mode) — and
asks this module the rest:

- before the run: the config checks only training runs
  (:func:`config_errors`); the versions this run reads and writes
  (:func:`versions_for_run`), and what the dataset version it reads says
  about the test metrics (:func:`dataset_version_verdict`); which DAG to build
  (:func:`pipeline_kwargs`);
- after it: what the version's reports add to ``manifest.json``
  (:func:`manifest_extra`).

The module sits at the package root, not in ``steps/``, because its caller is
outside the pipeline — the criterion in ``docs/agents/pipeline-node-design.md``
rule 8, beside ``cache_sources.py``, which has the same caller.

**What the CLI no longer hands the DAG.** Every value the command puts into
``parameters`` was reviewed against node-design rule 15 when this module was
made. ``search_id`` went: ``tune_hyperparameters`` computes it from the
``training:`` block and the two version IDs, all already in ``parameters``,
and the catalog never reads it. The cache source tables stayed, but only for
training: ``__main__._execute_pipeline`` used to derive them for every
pipeline, though only training's cache nodes read them.

**Verdicts stay in ``core/consistency.py``.** :func:`config_errors` and
:func:`dataset_version_verdict` decide which predicates training runs and hand
each the facts it needs; what counts as an error is the predicate's call.
"""

import json
import logging
from pathlib import Path
from typing import NamedTuple

from recsys_tfb.core.catalog import DataCatalog
from recsys_tfb.core.consistency import (
    BinaryTestMetricsVerdict,
    binary_test_metrics_verdict,
    duplicate_test_month_errors,
    entity_columns_declared_errors,
    hpo_enabled,
    missing_test_month_errors,
    optional_role_columns_declared_errors,
    resolved_zero_positive_group_ratio,
    scoring_param_errors,
    skip_hpo_param_errors,
    training_algorithm_errors,
    zero_positive_group_weight_declared_errors,
)
from recsys_tfb.core.versioning import (
    compute_model_version,
    read_manifest,
    resolve_base_dataset_version,
    resolve_train_variant_id,
)

logger = logging.getLogger(__name__)

#: The catalog entry the declaration checks (A28 / A39 / A45) read: the table
#: ``predict_and_write_test_predictions`` writes.
TEST_PREDICTIONS = "training_eval_predictions"


def config_errors(parameters: dict, catalog_config: dict) -> list[str]:
    """Every config check only training runs, collected in one pass.

    Before the Spark cold start, and long before the nodes that would trip on
    the same mistakes — several of which run only after the whole HPO search
    (the scored months, the columns of the prediction table). None is
    aggregated by ``validate_config_consistency``, which every command runs:
    their harm is training's alone (#158). What each checks is in the legend
    of ``core/consistency.py``:

    - A57 ``training.algorithm``; A58 the skip-HPO keys;
    - A26 / A36 ``dataset.test_snap_dates`` (spelled once, not empty), A53 the
      ``test_metrics`` block judged against it;
    - A28 / A39 / A45 the columns ``training_eval_predictions`` declares.

    The last three are asked of the catalog's dataset object, not of its
    config entry — which columns an artifact keeps is the catalog's
    knowledge — and read once, so an author missing an entity column and an
    event column fixes one ``columns:`` list once. They compare names only, so
    ``catalog_config`` may be resolved without the run's version IDs: those
    fill partition *values*, which none of the three reads. An absent entry is
    not their business: the Runner refuses a pipeline whose outputs it cannot
    resolve, and says so better.
    """
    declared = getattr(
        DataCatalog(catalog_config).get_dataset(TEST_PREDICTIONS),
        "declared_columns",
        None,
    )
    return [
        *training_algorithm_errors(parameters),
        *skip_hpo_param_errors(parameters),
        *missing_test_month_errors(parameters),
        *duplicate_test_month_errors(parameters),
        *scoring_param_errors(parameters),
        *entity_columns_declared_errors(parameters, declared, TEST_PREDICTIONS),
        *optional_role_columns_declared_errors(parameters, declared, TEST_PREDICTIONS),
        *zero_positive_group_weight_declared_errors(
            parameters, declared, TEST_PREDICTIONS),
    ]


class RunVersions(NamedTuple):
    """The versions one training run reads and writes."""

    base_dataset_version: str
    train_variant_id: str
    model_version: str
    #: ``dataset.test_zero_positive_group_ratio`` the base version was built
    #: with, as its manifest records it — which can differ from the config's
    #: (a ratio raised with no dataset rerun, ``latest`` on an older version).
    dataset_test_ratio: float


def versions_for_run(
    dataset_dir: Path,
    params_training: dict,
    base_dataset_version: str | None,
    train_variant: str | None,
) -> RunVersions:
    """Resolve the dataset versions this run reads and the model version it
    writes.

    Training does not recompute the dataset versions from its own config: it
    follows ``latest`` at each layer unless ``--base-dataset-version`` /
    ``--train-variant`` name one. ``model_version`` hashes
    ``params_training`` — the ``parameters_training`` file, not every
    parameter file merged — with both (``core/versioning.py``).

    Raises ``ValueError`` when the base version named on the command
    line has no directory: following ``latest`` cannot get there, so this is
    a typo in the flag.
    """
    base_v = resolve_base_dataset_version(dataset_dir, base_dataset_version)
    base_dir = dataset_dir / base_v
    if base_dataset_version is not None and not base_dir.is_dir():
        raise ValueError(f"Base dataset version directory not found: {base_dir}")
    train_v = resolve_train_variant_id(base_dir, train_variant)
    return RunVersions(
        base_dataset_version=base_v,
        train_variant_id=train_v,
        model_version=compute_model_version(params_training, base_v, train_v),
        dataset_test_ratio=_dataset_version_test_ratio(base_dir),
    )


def _dataset_version_test_ratio(base_dir: Path) -> float:
    """The ``dataset.test_zero_positive_group_ratio`` a dataset version was
    built with, as its manifest records it (the dataset command writes the
    ``parameters_dataset`` it ran with there).

    An absent key reads as the default 0, through the same resolver the
    dataset nodes drew with: a version built before the key existed kept no
    zero-positive test group. So does an absent manifest — every dataset run
    writes one before it starts, so a directory without one was not built by
    it, and nothing says its test kept those groups.
    """
    try:
        manifest = read_manifest(base_dir)
    except FileNotFoundError:
        manifest = {}
    return resolved_zero_positive_group_ratio(
        manifest.get("parameters") or {}, "test")


def dataset_version_verdict(
    parameters: dict, versions: RunVersions,
) -> BinaryTestMetricsVerdict:
    """(A54) The dataset version this run reads, against what test is asked
    to score (ADR-0028 decision 4).

    ``validate_config_consistency`` already held the config to it; this
    catches the config raised after the data was built — a raised test ratio
    with no dataset rerun, ``latest`` on an older version, or
    ``--base-dataset-version`` naming one. Asked here rather than in the
    scoring node, so it stops before the HPO search instead of after it; the
    node reads the same ratio for the objectives it withholds and as the
    runtime backstop.
    """
    return binary_test_metrics_verdict(
        parameters,
        dataset_version=versions.base_dataset_version,
        dataset_test_ratio=versions.dataset_test_ratio,
    )


def pipeline_kwargs(parameters: dict) -> dict:
    """What ``create_pipeline`` builds: the run's mode, from
    ``training.hpo_enabled`` (ADR-0030 decision 11).

    Derived here and handed to ``create_pipeline`` — the way evaluation's
    ``baseline_rate`` is — because the mode decides which nodes exist, and
    only the command builds the DAG.
    """
    return {"hpo_enabled": hpo_enabled(parameters)}


def manifest_extra(version_dir: Path) -> dict:
    """What this version's reports add to ``manifest.json``: the sample weight
    report and the group filter report, each under its own key when its file
    is there.

    The group filter report matters under lambdarank: the training matrix
    then holds fewer rows than the train / train_dev tables (zero-positive
    query groups are dropped), and recording the counts is what lets two runs
    be compared on training-set size without re-deriving where the difference
    came from.
    """
    extra: dict = {}
    for key, name in (
        ("sample_weight", "sample_weight_report.json"),
        ("group_filter", "group_filter_report.json"),
    ):
        report = version_dir / name
        if report.exists():
            with open(report) as f:
                extra[key] = json.load(f)
    return extra
