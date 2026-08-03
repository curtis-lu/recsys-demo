"""Which months each incrementally-built dataset processes on this run.

The three artifacts in :data:`INCREMENTAL_DATASETS` are extended a month at a
time: a run touches only the configured months that have not landed yet, plus
whatever ``--rebuild-dates`` names (ADR-0002). This module owns that whole
decision — which configured months each artifact is entitled to, and how the
landed ones are subtracted — so adding a fourth incremental artifact is one
entry in :data:`_CONFIGURED_SNAP_DATES` plus one input on the pipeline
definition, not a fourth node body that derives its own answer.

The resulting plans reach the nodes through the **catalog**, as named datasets
(``<artifact>_month_plan``), not through ``parameters``: a plan is runtime data
read off the metastore, and declaring it as an ordinary node input is what puts
"this node is incremental, and this is the plan it follows" on the pipeline
definition where a reviewer can see it. See ADR-0007.
"""

import logging

from recsys_tfb.pipelines.dataset.nodes_shared import (
    SnapDatePlan,
    collect_dataset_snap_dates,
    plan_incremental_snap_dates,
)

logger = logging.getLogger(__name__)


def month_plan_input(dataset_name: str) -> str:
    """Catalog name of the month plan that scopes ``dataset_name``."""
    return f"{dataset_name}_month_plan"


def _test_snap_dates(parameters: dict) -> list:
    return (parameters.get("dataset") or {}).get("test_snap_dates", [])


#: The per-artifact answer to "which months does the config ask for", and the
#: only place a new incremental artifact has to be registered. The two test
#: artifacts cover the test months only; ``preprocessed_feature_table`` feeds
#: every split, so it covers the union (train ∪ cal ∪ val ∪ test) — and unlike
#: the other two it is fail-loud about ``train_snap_dates`` being absent.
#:
#: Element type is deliberately loose: config gives ``str``,
#: ``collect_dataset_snap_dates`` gives ``pd.Timestamp``, and
#: :func:`plan_incremental_snap_dates` normalises either.
_CONFIGURED_SNAP_DATES = {
    "preprocessed_feature_table": collect_dataset_snap_dates,
    "test_keys": _test_snap_dates,
    "test_model_input": _test_snap_dates,
}


#: Dataset names whose ``snap_date`` partitions gate incremental processing.
#: Authoritative: the CLI lists partitions for exactly these, builds one plan
#: per entry, and injects them under :func:`month_plan_input` names.
#:
#: Derived, not written out a second time — a name here without a rule above
#: would raise ``KeyError`` mid-build, so the two cannot drift apart.
INCREMENTAL_DATASETS = tuple(_CONFIGURED_SNAP_DATES)


def _fmt(dates) -> str:
    return ",".join(d.strftime("%Y-%m-%d") for d in dates) or "-"


def build_month_plans(
    parameters: dict,
    existing: dict[str, list] | None = None,
    rebuild=(),
) -> dict[str, SnapDatePlan]:
    """One :class:`SnapDatePlan` per incremental artifact.

    Args:
        parameters: the dataset parameters (``dataset.*_snap_dates``).
        existing: ``{dataset name: months already landed}``, from one metastore
            partition listing taken before any node runs. ``None`` means
            "nothing has landed" — the entry point for a pipeline driven
            outside the CLI, which then processes every configured month.
        rebuild: months to reprocess even though they landed (``--rebuild-dates``).
            Applied to every artifact that configures them, so a rebuilt month
            is re-encoded *and* re-keyed rather than half-updated.

    Emits one ``[months]`` line per artifact. This is where ADR-0002's "a
    pipeline that decides to do less work must say what it decided not to do"
    is paid, and saying it here means it is said before any Spark work starts —
    early enough to abort a run whose scope is wrong.
    """
    existing = existing or {}
    rebuild = list(rebuild)
    if rebuild:
        logger.info("[months] rebuild requested: %s", ",".join(str(d) for d in rebuild))

    plans = {}
    for name in INCREMENTAL_DATASETS:
        plan = plan_incremental_snap_dates(
            _CONFIGURED_SNAP_DATES[name](parameters),
            existing.get(name, []),
            rebuild,
        )
        logger.info(
            "[months] dataset=%s processed=%s skipped=%s",
            name, _fmt(plan.to_process), _fmt(plan.skipped),
        )
        plans[name] = plan
    return plans
