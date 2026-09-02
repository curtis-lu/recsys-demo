import json
import logging
from pathlib import Path
from typing import Optional

import typer

from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.logging import RunContext, setup_logging
from recsys_tfb.core.runner import Runner
from recsys_tfb.core.consistency import (
    validate_config_consistency,
    compare_mutual_exclusive_errors,
    compare_source_key_exists,
    date_split_overlap_errors,
    duplicate_test_month_errors,
    entity_columns_declared_errors,
    post_training_snap_date_errors,
    resolved_env_dir,
    resolved_inference_rebuild_dates,
    resolved_rebuild_dates,
    train_snap_dates_errors,
    ConfigConsistencyError,
    REBUILD_SNAP_DATES_KEY,
)
from recsys_tfb.core.schema import (
    get_schema,
    get_schema_for_hash,
    validate_schema_config,
)
from recsys_tfb.core.versioning import (
    build_manifest_metadata,
    compute_base_dataset_version,
    compute_calibration_variant_id,
    compute_feature_table_fingerprint,
    compute_model_version,
    compute_search_id,
    compute_train_variant_id,
    find_latest_completed_model_version,
    read_manifest,
    resolve_base_dataset_version,
    resolve_model_version,
    resolve_variant_id,
    update_symlink,
    write_manifest,
)
from recsys_tfb.pipelines import get_pipeline, list_pipelines
from recsys_tfb.pipelines.dataset.month_plans import (
    INCREMENTAL_DATASETS,
    build_month_plans,
    landed_months,
    month_plan_input,
)
from recsys_tfb.pipelines.dataset.pipeline import ONLY_TEST_MONTHS_NODES
from recsys_tfb.pipelines.training.cache_sources import inject_cache_source_tables

app = typer.Typer(help="recsys_tfb: Product recommendation ranking model CLI")

logger = logging.getLogger(__name__)

_NONE_PLACEHOLDER = "__none__"


def _find_conf_dir() -> Path:
    """Resolve conf/ directory relative to the current working directory."""
    return Path.cwd() / "conf"


def _find_data_dir() -> Path:
    """Resolve data/ directory relative to the current working directory."""
    return Path.cwd() / "data"


def _load_spark_config(config: ConfigLoader, pipeline: str) -> dict:
    """Return base + pipeline-specific spark config, merged (pipeline wins),
    with ${vdclient.<name>} placeholders resolved (or dropped if vdclient
    is unavailable)."""
    from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders

    try:
        base_params = config.get_parameters_by_name("parameters")
    except KeyError:
        base_params = {}
    try:
        pipe_params = config.get_parameters_by_name(f"parameters_{pipeline}")
    except KeyError:
        pipe_params = {}
    base_spark = dict(base_params.get("spark", {}))
    pipe_spark = pipe_params.get("spark", {})
    base_spark.update(pipe_spark)
    return resolve_vdclient_placeholders(base_spark)


def _load_config_and_setup(pipeline: str, env: str) -> tuple[ConfigLoader, dict, RunContext]:
    conf_dir = _find_conf_dir()
    try:
        # A30 before the loader, not after: ConfigLoader treats a missing
        # conf/<env> as an empty overlay, so once it has run there is no
        # evidence left that the requested environment was never read.
        resolved_env_dir(conf_dir, env)
        config = ConfigLoader(str(conf_dir), env=env)
        params = config.get_parameters()
    except ValueError as exc:
        logger.error("Config loading failed: %s", exc)
        raise typer.Exit(code=1)

    run_context = RunContext(pipeline=pipeline, env=env)
    setup_logging(params, run_context)

    try:
        validate_schema_config(params)
        validate_config_consistency(params)
    except ValueError as exc:
        logger.error("Config validation failed: %s", exc)
        raise typer.Exit(code=1)

    return config, params, run_context


def _make_can_load(catalog, month_plans=None):
    """The slice's stopping condition: is this input already satisfied?

    ``catalog.exists`` answers "is the artifact there", which is the whole
    question for an artifact a run either writes or does not. It is the *wrong*
    question for one that is extended a month at a time: the table is there from
    the first run onwards, and what a later run can be missing is **its own
    months**. Asking only the table lets a slice stop one hop short of the
    producer that owns the new month, run to completion, and write nothing
    (ADR-0012).

    ``month_plans`` is ``{dataset name: SnapDatePlan}`` for the artifacts that
    have months — a per-pipeline override in the shape of ``retrain_advice`` /
    ``rebuild_advice``, injected by the one command that has any. Absent (every
    other pipeline, and every dataset with no plan) the answer is ``exists``
    alone.

    The plans are the ones the nodes themselves receive, so the slice and the
    nodes cannot disagree about which months this run covers. The subtraction
    lives here rather than in ``HiveTableDataset.exists()`` because "which
    months does this run want" is computed from config, a metastore listing and
    ``--rebuild-dates`` — a run-level answer the io object would have to ask the
    catalog for, which is the cycle ADR-0012 declines to build.

    Memoized: slice probing re-checks the same names once per node (~6x), and
    each check can be a metastore round-trip.
    """
    plans = month_plans or {}
    memo: dict = {}

    def can_load(name: str) -> bool:
        plan = plans.get(name)
        if plan is not None and plan.to_process:
            return False
        if name not in memo:
            memo[name] = catalog.exists(name)
        return memo[name]

    return can_load


def _slice_pipeline(pipe, can_load, from_node, only_node):
    """Apply --from-node/--only-node slicing. Returns (pipeline, plan|None).

    Raises ValueError on conflicting flags or unknown node names (the
    Pipeline methods list available node names in their message).
    """
    if from_node and only_node:
        raise ValueError("--from-node and --only-node are mutually exclusive")
    if from_node:
        return pipe.slice_from(from_node, can_load)
    if only_node:
        return pipe.slice_only(only_node, can_load)
    return pipe, None


def _format_slice_plan(plan, total: int) -> list[tuple[int, str]]:
    """Render a SlicePlan as ``(log level, [plan]-prefixed line)`` pairs.

    The level rides with the line because only this function knows which
    lines are bookkeeping and which are warnings; a caller would have to
    re-derive that from the text, and prefix-sniffing rots the moment a line
    is reworded. Two kinds of line carry ``WARNING``:

    * **the skipped side-effect nodes** — emitted only when the plan skipped
      any. For the ``dataset`` pipeline that set contains
      ``validate_data_consistency``, the Layer-2 data gate, so the line means
      "data-layer invariants went unchecked this run". At ``info`` it ranked
      below the retrain advisory next to it (issue #157), which is backwards.
      Slicing still skips them — the flag exists to skip upstream work, and
      most resumes follow a crash with unchanged source tables, where
      re-validating is pure cost. The danger is the resume days later against
      changed sources, so the skip is made loud rather than impossible (see
      F5 in ``docs/agents/architecture-constraints.md``).
    * **the resume caveat** — unconditional. It spelled ``WARNING:`` in its
      own text while going out at ``info``.
    """
    def info(text):
        return (logging.INFO, text)

    def warn(text):
        return (logging.WARNING, text)

    lines = [
        info(f"[plan] mode={plan.mode}; requested: {', '.join(plan.requested)}")
    ]
    if plan.auto_included:
        lines.append(info(
            "[plan] auto-included (missing input/write target -> producer re-run):"
        ))
        for name, missing in plan.auto_included.items():
            lines.append(info(f"[plan]   {name}  <- {', '.join(missing)}"))
    if plan.skipped:
        lines.append(info(
            f"[plan] skipped (inputs satisfied from catalog): {', '.join(plan.skipped)}"
        ))
    if plan.skipped_side_effect:
        lines.append(warn(
            "[plan] skipped side-effect nodes (outputs=None, not re-validated): "
            + ", ".join(plan.skipped_side_effect)
        ))
    lines.append(warn(
        "[plan] WARNING: resume assumes the skipped artifacts are still valid. "
        "exists() proves presence, not freshness — version IDs cover config "
        "only, not code changes or backfilled source data."
    ))
    running = len(plan.requested) + len(plan.auto_included)
    lines.append(info(f"[plan] running {running} of {total} nodes"))
    return lines


def _format_node_list(pipe, can_load) -> list[str]:
    """One line per node: name + what a --from-node start there would re-run."""
    lines = ["[nodes] # node  (auto-included when starting here)"]
    for i, node in enumerate(pipe.nodes):
        _, plan = pipe.slice_from(node.name, can_load)
        extra = ", ".join(plan.auto_included) if plan.auto_included else "-"
        lines.append(f"[nodes] {i + 1:>2}  {node.name}  (+ {extra})")
    return lines


def _format_retrain_advisory(model_version, retrain_nodes, latest):
    """Loud WARN lines for an unexpected retrain triggered by a sliced resume.

    ``latest`` is ``(version, created_at)`` of the nearest existing completed
    model, or ``None``.
    """
    lines = [
        f"[retrain] model_version={model_version} — 此版本尚無 finalized 模型"
        "（可能因 parameters 異動而版本漂移）。",
        f"[retrain] 此切片將 auto-include 並重新訓練：{', '.join(retrain_nodes)}",
    ]
    if latest is not None:
        ver, created = latest
        lines.append(f"[retrain] 最接近的既有模型：{ver} (completed, {created})")
        lines.append(
            "[retrain] 想對它重跑？比對你現在的 parameters_training.yaml 與 "
            f"data/models/{ver}/manifest.json 的 parameters（training: 區塊）。"
        )
    lines.append("[retrain] 仍依契約繼續執行（缺料自動補跑）…")
    return lines


def _maybe_warn_retrain(plan, retrain_advice):
    """Return loud-WARN lines when a sliced run will auto-include the model
    producer (``model`` was missing -> finalize/calibrate pulled in), else ``[]``.

    ``retrain_advice`` is ``{"models_dir": Path, "model_version": str}`` (passed
    only by the training command) or ``None``.
    """
    if retrain_advice is None or plan is None:
        return []
    # auto_included maps node -> tuple[str, ...] of missing dataset names; `in`
    # is element membership. Fire iff the missing dataset is exactly `model`,
    # i.e. the model producer (finalize_model, or calibrate_model under
    # calibration) had to be pulled in -> an unexpected retrain.
    if not any("model" in missing for missing in plan.auto_included.values()):
        return []
    latest = find_latest_completed_model_version(retrain_advice["models_dir"])
    # Pass every pulled-in node (not just the model producer) so the advisory
    # shows the full retrain footprint of the model's upstream closure.
    return _format_retrain_advisory(
        retrain_advice["model_version"], list(plan.auto_included), latest
    )


def _resolve_catalog(config: ConfigLoader, params: dict, runtime_params: dict):
    """The substitution params and the catalog config they resolve.

    One function because there is exactly one right answer to "what fills the
    ``${...}`` in catalog.yaml", and getting it wrong is silent: substitution is
    a plain string ``.replace()`` (:func:`recsys_tfb.core.config._apply`), so a
    placeholder nobody supplied survives as its own literal text and raises
    nothing. A dataset built from such a config points at a table named after
    the template, or filters partitions against it and keeps none.

    The practical consequence for callers: ``runtime_params`` must already hold
    every version this run computed before this is called.
    """
    substitution_params = {**params, **runtime_params}
    return substitution_params, config.get_catalog_config(
        runtime_params=substitution_params
    )


def _slice_extra(from_node, only_node):
    """Manifest extra_metadata breadcrumb for sliced runs."""
    if from_node:
        return {"resumed_from": from_node}
    if only_node:
        return {"only_node": only_node}
    return None


def _collect_existing_snap_dates(
    catalog: DataCatalog, time_col: str = "snap_date"
) -> dict[str, list[str]]:
    """Ask the catalog which months each incrementally-built dataset already has.

    Taken once, before any node runs, so every incremental node and the
    manifest agree on what had already landed when this run started (ADR-0002).
    Metadata-only: no data is scanned.

    The question goes to the *dataset object*, not to its config entry: where an
    artifact is stored and how its partitions are listed is the catalog's
    knowledge. The CLI knowing that a ``HiveTableDataset`` has ``database`` and
    ``table`` fields, and how to turn those into a metastore query, is exactly
    the leak ADR-0008 §5 closes. The entry's ``partition_filter`` already scopes
    the answer to this run's ``base_dataset_version``, so the version is not a
    parameter here — see the caller for why that is load-bearing.

    A dataset that cannot list partitions makes every month look not-yet-landed:
    that rebuilds (wasteful) rather than skips (silently stale), which is the
    direction this decision must fail in.

    ``time_col`` comes from ``schema.time`` rather than being hardcoded: the
    partition column is whatever the pipeline writes as its time column, and
    this repo is a configurable ranking framework, not a snap_date-only one.
    """
    existing: dict[str, list[str]] = {}
    for name in INCREMENTAL_DATASETS:
        lister = getattr(catalog.get_dataset(name), "existing_partition_values", None)
        if lister is None:
            logger.warning(
                "[months] %s cannot list its partitions, so its months cannot "
                "be listed and it will be rebuilt in full.", name,
            )
            continue
        existing[name] = landed_months(
            lister(), time_col=time_col, dataset_name=name,
        )
    return existing


def _fmt_months(dates) -> str:
    return ",".join(str(d) for d in dates) or "-"


#: The training nodes ``--rebuild-dates`` acts on: one drops the named month's
#: local cache, the other re-predicts it. A slice keeping either one still does
#: part of what the flag asked for, so the warning fires only when both are
#: gone — that is the case where the flag is accepted and nothing happens.
_REBUILD_TARGET_NODES = (
    "cache_test_model_input",
    "predict_and_write_test_predictions",
)
_REBUILD_PREDICT_NODE = "predict_and_write_test_predictions"

#: The inference side of the same idea: one node reads the flag, and it is the
#: node that decides which scoring chunks to redo.
_INFERENCE_REBUILD_TARGET_NODES = ("predict_and_write_scores",)
_INFERENCE_REBUILD_PREDICT_NODE = "predict_and_write_scores"


def _maybe_warn_rebuild_sliced_away(pipe, rebuild_advice) -> list[str]:
    """WARN lines when ``--rebuild-dates`` was passed but this run does not
    include ``predict_node`` — the one node that turns the flag into new
    predictions — else ``[]``.

    Combining ``--rebuild-dates`` with slicing is the *normal* path here — the
    documented way to re-predict without retraining is
    ``--only-node predict_and_write_test_predictions`` — so the combination
    itself is not worth a warning (unlike the dataset side, where slicing leaves
    part of the test chain stale). What is worth one is a slice that drops
    ``predict_node``: then the flag is accepted, the run succeeds, and no month
    is re-predicted.

    **The condition is ``predict_node``, not "any of ``targets``".** The flag
    drives two nodes on the training side, but only one of them produces what
    the operator asked for: dropping the stale local cache is a means,
    re-predicting is the end. An earlier spelling asked "is any target here",
    which for a *forward* slice was the same question — ``predict_manifest``
    was memory-only, so every forward slice that kept the cache node kept the
    predict node too. Landing ``predict_manifest`` (issue #233) ends that:
    ``--from-node compute_feature_statistics --rebuild-dates ...`` now keeps
    the cache node and drops predict, so the named months drop and rebuild
    their local cache and are never re-predicted. (The narrower version of the
    same hole predates it: ``--only-node cache_test_model_input`` was always
    exactly that run.)

    Which nodes those are is per-pipeline, and the caller must say: naming the
    wrong pipeline's nodes in this message would send the operator to a node
    their pipeline does not have. A caller that names none stays silent — that
    is how the dataset command, whose ``--rebuild-dates`` drives the whole test
    chain rather than one node, opts out of this half.

    ``targets`` and ``predict_node`` are a **pair**, both caller-supplied. A
    default for either one is the same footgun in a smaller disguise: naming
    targets while inheriting training's ``predict_node`` prints an
    ``--only-node`` command for a node the operator's pipeline has not got.
    So the second lookup is a subscript, not a ``.get`` — a caller that names
    one and forgets the other fails in its own test rather than in an
    operator's terminal.
    """
    if not rebuild_advice or not rebuild_advice.get("rebuild"):
        return []
    targets = rebuild_advice.get("targets")
    if not targets:
        return []
    predict_node = rebuild_advice["predict_node"]
    node_names = {node.name for node in pipe.nodes}
    if predict_node in node_names:
        return []
    months = _fmt_months(rebuild_advice["rebuild"])
    # Which of the other driven nodes DID survive, so the message describes the
    # run the operator is about to get rather than a generic "nothing happened".
    kept = [name for name in targets if name != predict_node and name in node_names]
    if kept:
        head = (
            f"[rebuild] WARNING: --rebuild-dates {months} is only half applied — "
            f"this slice runs {', '.join(kept)} but not {predict_node}, so those "
            f"months rebuild their local cache and are never re-predicted."
        )
    else:
        head = (
            f"[rebuild] WARNING: --rebuild-dates {months} had no effect — this "
            f"slice includes none of the nodes it drives ({', '.join(targets)})."
        )
    return [
        head,
        "[rebuild] 要重算既有月份的預測，請跑 "
        f"--only-node {predict_node}（不帶切片旗標的完整 run 也可以）。",
    ]


def _format_only_test_months_plan(enable_calibration: bool) -> list[str]:
    """``[plan]`` lines naming what ``--only-test-months`` left out.

    Diffed against the full pipeline rather than restated from a second list:
    the mode is defined by ``ONLY_TEST_MONTHS_NODES``, and a message carrying
    its own copy could disagree with what actually runs — which is the one
    thing this message exists to prevent, since a dataset run that does less
    than the operator expected still exits 0.

    Names the nodes rather than counting them: "10 left out" tells an operator
    nothing they can check, and checking is the point at the moment they are
    deciding whether this run really is "just adding an eval month".
    """
    full = [
        node.name
        for node in get_pipeline(
            "dataset", enable_calibration=enable_calibration
        ).nodes
    ]
    kept = set(ONLY_TEST_MONTHS_NODES)
    left_out = [name for name in full if name not in kept]
    return [
        f"[plan] only-test-months: {len(full) - len(left_out)} of the dataset "
        f"pipeline's {len(full)} nodes; the {len(left_out)} left out rebuild "
        f"artifacts a new test month cannot change.",
        f"[plan] left out: {', '.join(left_out)}",
    ]


def _maybe_warn_rebuild_partial_chain(
    kept: int, total: int, rebuild_advice
) -> list[str]:
    """WARN lines when ``--rebuild-dates`` ran against a run that actually
    dropped nodes, else ``[]``.

    The condition is ``kept < total`` — did this run exclude anything — not
    "was a slicing flag passed". Those differ, and the old spelling made a
    false claim in the gap: ``--from-node <the first node>`` selects every
    node, yet still printed "the upstream this run did not select will not be
    refreshed" and told the operator to re-run without the flag, which produces
    bit-identical output (ADR-0013 consequences).

    ``total`` is the node count of whatever ``create_pipeline`` built, so a
    **mode** never trips this: a short pipeline is the whole of what the run
    is, not a part of it left out. Only slicing can make ``kept`` smaller.

    ``chain`` names what the operator would be re-running and the caller must
    supply it, because callers mean different things by it: dataset means the
    test chain, inference the scoring chain. Same rule as
    :func:`_maybe_warn_rebuild_sliced_away`'s target nodes — a message
    describing the wrong pipeline sends the operator looking for something
    their run does not have.
    """
    if not rebuild_advice or not rebuild_advice.get("rebuild"):
        return []
    chain = rebuild_advice.get("chain")
    if not chain or kept >= total:
        return []
    return _format_rebuild_slice_warning(rebuild_advice["rebuild"], chain=chain)


def _format_rebuild_slice_warning(
    rebuild: list[str], chain: str = "test 鏈"
) -> list[str]:
    """The WARN lines themselves; see :func:`_maybe_warn_rebuild_partial_chain`
    for when they fire.

    The two flags are orthogonal by design (slicing picks nodes, rebuild picks
    months) and combining them is a supported expert path — but only part of
    the chain gets recomputed, and the untouched upstream partitions stay stale
    without complaint. Say so rather than let it pass silently.
    """
    return [
        "[rebuild] WARNING: --rebuild-dates 與切片旗標（--from-node/--only-node）併用；",
        "[rebuild] 本次只重算被選中的 node，未被選中的上游 partition "
        f"（月份 {_fmt_months(rebuild)}）不會刷新（exists() ≠ fresh）。",
        f"[rebuild] 要整條 {chain} 都重算，請不帶切片旗標再跑一次。",
    ]


def _execute_pipeline(
    pipeline_name: str,
    pipeline_kwargs: dict,
    runtime_params: dict,
    config: ConfigLoader,
    params: dict,
    env: str,
    *,
    from_node: Optional[str] = None,
    only_node: Optional[str] = None,
    dry_run: bool = False,
    list_nodes: bool = False,
    retrain_advice: Optional[dict] = None,
    rebuild_advice: Optional[dict] = None,
    extra_datasets: Optional[dict] = None,
    month_plans: Optional[dict] = None,
) -> bool:
    """Run the pipeline; returns False when nothing was executed
    (--dry-run / --list-nodes early exits) so callers skip post-run
    manifest writing.

    ``extra_datasets`` is ``{catalog name: value}`` for run-scoped values a node
    declares as an ordinary input — the dataset month plans. Same mechanism
    ``parameters`` itself uses, so a node cannot tell them apart, and the
    runner's input check turns a forgotten one into a startup error rather than
    a silent full rebuild.

    ``month_plans`` is ``{dataset name: SnapDatePlan}`` — the same plans, keyed
    by the artifact they scope rather than by their catalog name, for
    :func:`_make_can_load`. Two parameters for one set of plans because the two
    consumers ask different questions of them: a node asks "which months do I
    process", the slice asks "is this artifact complete for this run". Only the
    dataset command has incremental artifacts, so for every other pipeline this
    is ``None`` and slicing behaves exactly as it did before.
    """
    try:
        pipe = get_pipeline(pipeline_name, **pipeline_kwargs)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline_name, available)
        raise typer.Exit(code=1)

    source_model_version = runtime_params.pop("source_model_version", None)
    substitution_params, catalog_config = _resolve_catalog(
        config, params, runtime_params
    )

    # Auto-inject cache source_tables from catalog config so cache nodes don't
    # need a parallel parameters yaml mapping. Catalog.yaml's HiveTableDataset
    # `table` field is the single source of truth for cache table resolution.
    inject_cache_source_tables(substitution_params, catalog_config)

    # For inference: when no explicit --model-version is given, the model
    # artifact should be read via the "best" symlink; swap the model filepath.
    if pipeline_name == "inference" and source_model_version is None:
        mv = runtime_params["model_version"]
        if "model" in catalog_config:
            catalog_config["model"]["filepath"] = catalog_config["model"][
                "filepath"
            ].replace(mv, "best")

    catalog = DataCatalog(catalog_config)
    catalog.add("parameters", MemoryDataset(data=substitution_params))
    for name, value in (extra_datasets or {}).items():
        catalog.add(name, MemoryDataset(data=value))

    _can_load = _make_can_load(catalog, month_plans)

    if list_nodes:
        if from_node or only_node:
            logger.error("--list-nodes cannot be combined with --from-node/--only-node")
            raise typer.Exit(code=1)
        for line in _format_node_list(pipe, _can_load):
            logger.info(line)
        return False

    total = len(pipe.nodes)
    try:
        pipe, plan = _slice_pipeline(pipe, _can_load, from_node, only_node)
    except ValueError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1)

    if plan is not None:
        for level, line in _format_slice_plan(plan, total):
            logger.log(level, line)
    for line in _maybe_warn_retrain(plan, retrain_advice):
        logger.warning(line)
    for line in _maybe_warn_rebuild_sliced_away(pipe, rebuild_advice):
        logger.warning(line)
    for line in _maybe_warn_rebuild_partial_chain(
        len(pipe.nodes), total, rebuild_advice
    ):
        logger.warning(line)
    if dry_run:
        if plan is None:
            logger.info(
                "[plan] no slicing flags: full run of %d nodes "
                "(use --list-nodes to inspect resume costs)", total,
            )
        logger.info("[plan] dry-run: nothing executed, nothing written")
        return False

    logger.info("Running pipeline '%s' (env=%s)", pipeline_name, env)
    try:
        runner = Runner()
        runner.run(pipe, catalog)
    except Exception:
        logger.exception("Pipeline '%s' failed", pipeline_name)
        raise typer.Exit(code=1)
    return True


def _write_manifest_stub(version_dir: Path, metadata_kwargs: dict, run_id: str):
    """Pre-run provenance stub: write manifest.json with status=running so a
    crash before the post-run write still records which parameters defined this
    version. Skip-if-present (never clobber an existing manifest); writes no
    `latest` symlink and no params sidecar (the stub already embeds parameters).
    """
    if (version_dir / "manifest.json").exists():
        return
    metadata = build_manifest_metadata(**metadata_kwargs, status="running")
    metadata["run_id"] = run_id
    write_manifest(version_dir, metadata)


def _write_pipeline_manifest(
    version_dir: Path,
    metadata_kwargs: dict,
    run_id: str,
    extra_metadata: Optional[dict] = None,
    symlink_target: Optional[Path] = None,
    params_name: Optional[str] = None,
    params_dict: Optional[dict] = None
):
    metadata = build_manifest_metadata(**metadata_kwargs, status="completed")
    metadata["run_id"] = run_id
    if extra_metadata is not None:
        metadata.update(extra_metadata)
    write_manifest(version_dir, metadata)
    if symlink_target:
        update_symlink(version_dir, symlink_target)
    if params_name and params_dict is not None:
        with open(version_dir / f"{params_name}.json", "w") as f:
            json.dump(params_dict, f, indent=2, ensure_ascii=False, default=str)


def _dir_artifacts(d: Path) -> list[str]:
    return sorted(f.name for f in d.iterdir() if f.is_file()) if d.is_dir() else []


def _sample_weight_extra(version_dir: Path) -> Optional[dict]:
    """Read sample_weight_report.json (if present) into manifest extra_metadata."""
    report = version_dir / "sample_weight_report.json"
    if not report.exists():
        return None
    with open(report) as f:
        return {"sample_weight": json.load(f)}


def _run_etl(
    stage: str,
    env: str,
    target_dates: Optional[str],
    restart_from: Optional[str],
    source_check_only: bool = False,
) -> None:
    """Shared executor for the feature/label/sample_pool ETL sub-commands.

    ``stage`` is one of ``feature_etl``, ``label_etl``, ``sample_pool_etl``
    and is used both as the pipeline name (for logging/config lookup) and as
    the top-level YAML key of its parameters file.
    """
    from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner, SourceCheckError
    from recsys_tfb.utils.spark import get_or_create_spark_session

    # restart-from 對純檢查無意義 → 先報錯（不必起 Spark）
    if source_check_only and restart_from:
        logger.error("--source-check 與 --restart-from 不能同時使用（檢查不寫表，無從續跑）。")
        raise typer.Exit(code=1)

    config, params, run_context = _load_config_and_setup(stage, env)

    spark_configs = _load_spark_config(config, stage)
    get_or_create_spark_session(spark_configs)

    conf_dir = _find_conf_dir()

    params_etl = config.get_parameters_by_name(f"parameters_{stage}")
    etl_config = params_etl.get(stage, params_etl)
    sql_dir = conf_dir / "sql" / "etl"
    dry_run = etl_config.get("dry_run", env == "local")

    if target_dates:
        date_list = [d.strip() for d in target_dates.split(",")]
    else:
        date_list = etl_config.get("target_dates", [])
    if not date_list:
        logger.error("No target_dates provided. Use --target-dates or set in config.")
        raise typer.Exit(code=1)

    rendered_sql_dir_str = etl_config.get("rendered_sql_dir")
    rendered_sql_dir = Path(rendered_sql_dir_str) if rendered_sql_dir_str else None

    runner = SQLRunner(
        config=etl_config,
        sql_dir=sql_dir,
        dry_run=False if source_check_only else dry_run,  # 檢查唯讀、必須實查 Hive
        rendered_sql_dir=rendered_sql_dir,
        stage=stage,
    )

    if source_check_only:
        try:
            runner.run_source_checks(target_dates=date_list, run_id=run_context.run_id)
        except SourceCheckError as exc:
            logger.error("%s", exc)
            raise typer.Exit(code=1)
        logger.info("Source check completed: %s", stage)
        return

    try:
        runner.run(
            target_dates=date_list,
            restart_from=restart_from,
            run_id=run_context.run_id,
        )
    except Exception:
        logger.exception("%s pipeline failed", stage)
        raise typer.Exit(code=1)

    logger.info("Pipeline '%s' completed successfully", stage)


@app.command(name="feature_etl")
def feature_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
    source_check: bool = typer.Option(
        False, "--source-check",
        help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
             "全部跑完後有任一失敗即以非零碼結束。",
    ),
):
    """Run the feature ETL pipeline (feature_aum/sav/ccard/info/concat/table)."""
    _run_etl("feature_etl", env, target_dates, restart_from, source_check_only=source_check)


@app.command(name="label_etl")
def label_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
    source_check: bool = typer.Option(
        False, "--source-check",
        help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
             "全部跑完後有任一失敗即以非零碼結束。",
    ),
):
    """Run the label ETL pipeline (label_ccard/exchange/fund/table)."""
    _run_etl("label_etl", env, target_dates, restart_from, source_check_only=source_check)


@app.command(name="sample_pool_etl")
def sample_pool_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
    source_check: bool = typer.Option(
        False, "--source-check",
        help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
             "全部跑完後有任一失敗即以非零碼結束。",
    ),
):
    """Run the sample_pool ETL pipeline. Requires feature_etl and label_etl outputs."""
    _run_etl("sample_pool_etl", env, target_dates, restart_from, source_check_only=source_check)


@app.command(name="inference_population_etl")
def inference_population_etl(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    target_dates: Optional[str] = typer.Option(
        None,
        "--target-dates",
        help="Comma-separated target dates, e.g. 2024-01-31,2024-02-29",
    ),
    restart_from: Optional[str] = typer.Option(
        None,
        "--restart-from",
        help="Restart from this table name (skip earlier tables in the list)",
    ),
    source_check: bool = typer.Option(
        False, "--source-check",
        help="只跑該 stage 的上游 source_checks（preflight），不執行 ETL／不寫表；"
             "全部跑完後有任一失敗即以非零碼結束。",
    ),
):
    """Run the inference population ETL pipeline (inference_population)."""
    _run_etl(
        "inference_population_etl", env, target_dates, restart_from,
        source_check_only=source_check,
    )


@app.command(name="dataset")
def dataset(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    rebuild_dates: Optional[str] = typer.Option(
        None, "--rebuild-dates",
        help="Comma-separated snap_dates to recompute even though their "
             "partitions already exist (use after an upstream backfill of an "
             "old month). Must be a subset of dataset.test_snap_dates.",
    ),
    only_test_months: bool = typer.Option(
        False, "--only-test-months",
        help="宣告「這次只加評估月份」：只跑資料閘與 test 鏈。train／val／"
             "calibration 的產物不重算——多一個 test 月不會改變它們的內容。"
             "與 --from-node／--only-node 正交，可併用。上游缺料時當場報錯。",
    ),
    from_node: Optional[str] = typer.Option(
        None, "--from-node",
        help="Start from this node (topological position); missing upstream "
             "artifacts are auto re-run",
    ),
    only_node: Optional[str] = typer.Option(
        None, "--only-node",
        help="Run a single node (plus minimal upstream re-runs for missing inputs)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the slice execution plan and exit"
    ),
    list_nodes: bool = typer.Option(
        False, "--list-nodes",
        help="List pipeline nodes with their resume cost and exit",
    ),
):
    """Run the dataset pipeline (always recomputes versions from parameters)."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("dataset", env)

    # (A24) The four dataset snap_date splits must be disjoint. Deliberately
    # not aggregated by validate_config_consistency: that gate runs inside
    # _load_config_and_setup, i.e. at the entry of EVERY command, while only
    # this pipeline reads these keys — and #158 measured what a dataset-only
    # predicate does there (9 unrelated tests blocked). Same reason A21/A22
    # hang off their own command.
    # (A23) train_snap_dates required / a list / non-empty. Same reason A24
    # hangs off this command rather than the aggregator: only this pipeline
    # reads the key, and aggregating it rejects a valid feature_etl config
    # (#158 measured 9 unrelated tests blocked). Before A24 because "the key is
    # missing" should be reported ahead of "the splits overlap" — the latter
    # reads an absent list as empty and finds nothing to say.
    train_months_errors = train_snap_dates_errors(params)
    if train_months_errors:
        for line in train_months_errors:
            logger.error(line)
        raise typer.Exit(code=1)

    split_errors = date_split_overlap_errors(params)
    if split_errors:
        for line in split_errors:
            logger.error(line)
        raise typer.Exit(code=1)

    # (A21) --rebuild-dates ⊆ dataset.test_snap_dates. Checked before Spark
    # starts: a typo here would otherwise cost a cold start before failing.
    try:
        rebuild = resolved_rebuild_dates(
            params,
            [d.strip() for d in rebuild_dates.split(",")] if rebuild_dates else None,
        )
    except ValueError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1)

    get_or_create_spark_session(_load_spark_config(config, "dataset"))
    data_dir = _find_data_dir()

    try:
        params_dataset = config.get_parameters_by_name("parameters_dataset")
    except KeyError:
        params_dataset = {}

    enable_calibration = (
        params_dataset.get("dataset", {}).get("enable_calibration", False)
    )

    spark = get_or_create_spark_session()
    # Version-free on purpose: the source tables are the only entries readable
    # before the versions below exist, because they carry no ${...} placeholder.
    # Nothing version-scoped may be built from this config — see below.
    source_catalog_config = config.get_catalog_config(runtime_params=params)
    feature_table_cfg = source_catalog_config["feature_table"]
    feature_table_fqn = f"{feature_table_cfg['database']}.{feature_table_cfg['table']}"
    feature_table_columns = [
        (f.name, f.dataType.simpleString())
        for f in spark.table(feature_table_fqn).schema.fields
    ]
    feature_table_fp = compute_feature_table_fingerprint(feature_table_columns)

    schema_hash = get_schema_for_hash(params)
    base_v = compute_base_dataset_version(
        params_dataset, schema_hash, feature_table_fingerprint=feature_table_fp,
    )
    train_v = compute_train_variant_id(params_dataset)
    cal_v = (
        compute_calibration_variant_id(params_dataset) if enable_calibration else None
    )

    logger.info("feature_table_fingerprint: %s (%d cols)",
                feature_table_fp, len(feature_table_columns))
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": "best",  # placeholder to avoid unresolved templates
        "snap_date": _NONE_PLACEHOLDER,
        # A user-supplied setting, unlike the listing below: it stays in
        # parameters, where the training pipeline reads the same key.
        REBUILD_SNAP_DATES_KEY: rebuild,
    }

    # Incremental plans (ADR-0002 / ADR-0007): one metastore listing, one plan
    # per incremental artifact, decided here — before any Spark work — so the
    # nodes, this log and the manifest cannot disagree about which months this
    # run covered. The nodes receive them through the catalog (below), not
    # through `parameters`.
    #
    # Built here rather than earlier because _resolve_catalog needs base_v to
    # already be in runtime_params, and that ordering is load-bearing rather
    # than tidy: a catalog resolved without it would compare every partition
    # against the literal `${base_dataset_version}`, keep none, and answer
    # "nothing has landed" for every month — a full rebuild, silently, with no
    # failing test and no config diff.
    _, listing_catalog_config = _resolve_catalog(config, params, runtime_params)
    existing_snap_dates = _collect_existing_snap_dates(
        DataCatalog(listing_catalog_config),
        time_col=get_schema(params)["time"],
    )
    month_plans = build_month_plans(
        params, existing=existing_snap_dates, rebuild=rebuild,
    )

    pipeline_kwargs = {
        "enable_calibration": enable_calibration,
        "only_test_months": only_test_months,
    }
    if only_test_months:
        for line in _format_only_test_months_plan(enable_calibration):
            logger.info(line)

    # Pre-run crash-safe provenance stubs (skip-if-present, no `latest` symlink);
    # the post-run writes below upgrade these to status=completed + artifacts.
    if not dry_run and not list_nodes:
        stub_base_dir = data_dir / "dataset" / base_v
        _write_manifest_stub(stub_base_dir, {
            "version": base_v, "pipeline": "dataset", "parameters": params_dataset,
            "base_dataset_version": base_v,
            # feature_table_fingerprint on base only; variants inherit via parent_version.
            "feature_table_fingerprint": feature_table_fp,
        }, run_context.run_id)
        _write_manifest_stub(stub_base_dir / "train_variants" / train_v, {
            "version": train_v, "pipeline": "dataset", "parameters": params_dataset,
            "parent_version": base_v, "variant_kind": "train",
        }, run_context.run_id)
        if cal_v is not None:
            _write_manifest_stub(stub_base_dir / "calibration_variants" / cal_v, {
                "version": cal_v, "pipeline": "dataset", "parameters": params_dataset,
                "parent_version": base_v, "variant_kind": "calibration",
            }, run_context.run_id)

    executed = _execute_pipeline(
        "dataset", pipeline_kwargs, runtime_params, config, params, env,
        from_node=from_node, only_node=only_node,
        dry_run=dry_run, list_nodes=list_nodes,
        # A loop over the plans, not three hand-written lines: registering a
        # fourth incremental artifact in month_plans.py is enough, and the
        # injection follows.
        extra_datasets={
            month_plan_input(name): plan for name, plan in month_plans.items()
        },
        # The same plans again, keyed by artifact: a slice has to stop at
        # "complete for this run", and for these three that is a month
        # question, not an exists() question (ADR-0012).
        month_plans=month_plans,
        # No `targets`: --rebuild-dates here drives the whole test chain, not
        # one node, so the "sliced away entirely" half does not apply and would
        # have to name training's nodes to say anything at all.
        rebuild_advice={"rebuild": rebuild, "chain": "test 鏈"},
    )
    if not executed:
        return

    # Post run: write three (or two) manifests and update corresponding symlinks.
    base_dir = data_dir / "dataset" / base_v
    _write_pipeline_manifest(
        version_dir=base_dir,
        metadata_kwargs={
            "version": base_v,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "base_dataset_version": base_v,
            "feature_table_fingerprint": feature_table_fp,
            "artifacts": _dir_artifacts(base_dir),
        },
        run_id=run_context.run_id,
        extra_metadata={
            **(_slice_extra(from_node, only_node) or {}),
            # A pipeline that decides to do less work has to record what it
            # decided not to do — otherwise ADR-0002's "exists() ≠ fresh" is
            # invisible after the fact.
            "test_snap_dates_plan": {
                "processed": [
                    d.strftime("%Y-%m-%d")
                    for d in month_plans["test_model_input"].to_process
                ],
                "skipped": [
                    d.strftime("%Y-%m-%d")
                    for d in month_plans["test_model_input"].skipped
                ],
                "rebuild_requested": list(rebuild),
            },
        },
        symlink_target=data_dir / "dataset" / "latest",
        params_name="parameters_dataset",
        params_dict=params_dataset,
    )

    train_variant_dir = base_dir / "train_variants" / train_v
    _write_pipeline_manifest(
        version_dir=train_variant_dir,
        metadata_kwargs={
            "version": train_v,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "parent_version": base_v,
            "variant_kind": "train",
            "artifacts": _dir_artifacts(train_variant_dir),
        },
        run_id=run_context.run_id,
        symlink_target=base_dir / "train_variants" / "latest",
    )

    if cal_v is not None:
        cal_variant_dir = base_dir / "calibration_variants" / cal_v
        _write_pipeline_manifest(
            version_dir=cal_variant_dir,
            metadata_kwargs={
                "version": cal_v,
                "pipeline": "dataset",
                "parameters": params_dataset,
                "parent_version": base_v,
                "variant_kind": "calibration",
                "artifacts": _dir_artifacts(cal_variant_dir),
            },
            run_id=run_context.run_id,
            symlink_target=base_dir / "calibration_variants" / "latest",
        )

    logger.info("Pipeline 'dataset' completed successfully")


@app.command(name="training")
def training(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    base_dataset_version: Optional[str] = typer.Option(
        None, "--base-dataset-version",
        help="Base dataset version (default: latest symlink)",
    ),
    train_variant: Optional[str] = typer.Option(
        None, "--train-variant",
        help="Train variant ID (default: latest under base dataset)",
    ),
    calibration_variant: Optional[str] = typer.Option(
        None, "--calibration-variant",
        help="Calibration variant ID (default: latest under base dataset; "
             "only used when training.calibration.enabled=true)",
    ),
    rebuild_dates: Optional[str] = typer.Option(
        None, "--rebuild-dates",
        help="Comma-separated snap_dates to re-predict even though their "
             "predictions are already complete (use after an upstream backfill "
             "of an old month, together with the same flag on dataset). Also "
             "drops those months' local parquet cache. Must be a subset of "
             "dataset.test_snap_dates.",
    ),
    from_node: Optional[str] = typer.Option(
        None, "--from-node",
        help="Start from this node (topological position); missing upstream "
             "artifacts are auto re-run",
    ),
    only_node: Optional[str] = typer.Option(
        None, "--only-node",
        help="Run a single node (plus minimal upstream re-runs for missing inputs)",
    ),
    fresh_hpo: bool = typer.Option(
        False, "--fresh-hpo",
        help="丟棄此 search_id 已累積的 HPO study/checkpoint，從 trial 0 重新搜尋",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the slice execution plan and exit"
    ),
    list_nodes: bool = typer.Option(
        False, "--list-nodes",
        help="List pipeline nodes with their resume cost and exit",
    ),
):
    """Run the training pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("training", env)

    # (A26) dataset.test_snap_dates must not spell one month two ways. Wired
    # here rather than aggregated by validate_config_consistency for A24's
    # reason: that gate runs at the entry of EVERY command, while the harm is
    # training-only — the dataset pipeline normalises its months through
    # pd.Timestamp into a set (month_plans.plan_incremental_snap_dates), so two
    # spellings collapse there, whereas the training cache keys on the YYYYMMDD
    # directory name and would count that month's rows twice. Before A21 so
    # "this month is named twice" is reported ahead of anything about the flag.
    month_spelling_errors = duplicate_test_month_errors(params)
    if month_spelling_errors:
        for line in month_spelling_errors:
            logger.error(line)
        raise typer.Exit(code=1)

    # (A21) --rebuild-dates ⊆ dataset.test_snap_dates — the same predicate the
    # dataset command uses, so the two halves of a backfill cannot disagree
    # about which months are nameable. Checked before Spark starts: a typo here
    # would otherwise cost a cold start before failing.
    try:
        rebuild = resolved_rebuild_dates(
            params,
            [d.strip() for d in rebuild_dates.split(",")] if rebuild_dates else None,
        )
    except ValueError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1)

    # (A28) training_eval_predictions must declare every schema.entity column.
    # Asked of the dataset object, not of its config entry, for the reason
    # _collect_existing_snap_dates gives: which columns an artifact keeps is
    # the catalog's knowledge, not the CLI's.
    #
    # Before the cold start below, and long before the node that writes those
    # columns — that node runs after HPO, train_model and calibrate_model, so
    # the same check inside it would report a one-word catalog typo only after
    # the whole search had been paid for. This compares two lists of names and
    # touches no data.
    #
    # runtime_params is deliberately empty: substitution fills partition
    # *values* (${model_version}), while this reads only column *names* —
    # declared columns, partition_filter keys, partition_cols names — none of
    # which a substitution touches. Waiting for the versions would buy nothing
    # and cost exactly the cold start this is placed above to avoid.
    #
    # An absent entry (get_dataset -> None) is deliberately not this gate's
    # business: "declared the wrong columns" and "not in the catalog at all"
    # need different fixes, and the runner already refuses to build a pipeline
    # whose outputs it cannot resolve. Reporting it here would only move that
    # message somewhere it explains less.
    _, gate_catalog_config = _resolve_catalog(config, params, {})
    declaration_errors = entity_columns_declared_errors(
        params,
        getattr(
            DataCatalog(gate_catalog_config).get_dataset(
                "training_eval_predictions"
            ),
            "declared_columns",
            None,
        ),
        "training_eval_predictions",
    )
    if declaration_errors:
        for line in declaration_errors:
            logger.error(line)
        raise typer.Exit(code=1)

    get_or_create_spark_session(_load_spark_config(config, "training"))
    data_dir = _find_data_dir()

    dataset_dir = data_dir / "dataset"
    base_v = resolve_base_dataset_version(dataset_dir, base_dataset_version)
    base_dir = dataset_dir / base_v
    if base_dataset_version is not None and not base_dir.is_dir():
        logger.error("Base dataset version directory not found: %s", base_dir)
        raise typer.Exit(code=1)

    train_v = resolve_variant_id(base_dir, "train", train_variant)

    try:
        params_training = config.get_parameters_by_name("parameters_training")
    except KeyError:
        params_training = {}

    enable_calibration = (
        params_training.get("training", {}).get("calibration", {}).get("enabled", False)
    )
    cal_v = (
        resolve_variant_id(base_dir, "calibration", calibration_variant)
        if enable_calibration
        else None
    )

    mv = compute_model_version(params_training, base_v, train_v, cal_v)
    sid = compute_search_id(params_training, base_v, train_v, cal_v)
    logger.info("Model version: %s", mv)
    logger.info("search_id: %s", sid)
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "search_id": sid,
        "_fresh_hpo": fresh_hpo,
        "snap_date": _NONE_PLACEHOLDER,
        # Read by cache_test_model_input (drop the stale month) and by
        # predict_and_write_test_predictions (re-predict it).
        REBUILD_SNAP_DATES_KEY: rebuild,
    }

    pipeline_kwargs = {"enable_calibration": enable_calibration}

    # Pre-run crash-safe provenance stub (skip-if-present, no symlink); the
    # post-run write below upgrades it to status=completed + artifacts.
    if not dry_run and not list_nodes:
        stub_kwargs = {
            "version": mv,
            "pipeline": "training",
            "parameters": params_training,
            "base_dataset_version": base_v,
            "train_variant_id": train_v,
        }
        # Omit when None: the manifest uses no _NONE_PLACEHOLDER sentinel (unlike
        # runtime_params, whose placeholder is for the Spark substitution layer).
        if cal_v is not None:
            stub_kwargs["calibration_variant_id"] = cal_v
        _write_manifest_stub(data_dir / "models" / mv, stub_kwargs, run_context.run_id)

    executed = _execute_pipeline(
        "training", pipeline_kwargs, runtime_params, config, params, env,
        from_node=from_node, only_node=only_node,
        dry_run=dry_run, list_nodes=list_nodes,
        # Always passed; _maybe_warn_retrain is a no-op under --list-nodes
        # (it returns early before plan exists) and only fires on sliced runs.
        retrain_advice={"models_dir": data_dir / "models", "model_version": mv},
        # No `chain`: training's --rebuild-dates drives two specific nodes, so
        # "the slice dropped them entirely" is the failure worth naming, not
        # "part of a chain is stale".
        rebuild_advice={
            "rebuild": rebuild,
            "targets": _REBUILD_TARGET_NODES,
            "predict_node": _REBUILD_PREDICT_NODE,
        },
    )
    if not executed:
        return

    # Post run
    version_dir = data_dir / "models" / mv
    metadata_kwargs: dict = {
        "version": mv,
        "pipeline": "training",
        "parameters": params_training,
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "artifacts": _dir_artifacts(version_dir),
    }
    if cal_v is not None:
        metadata_kwargs["calibration_variant_id"] = cal_v

    extra = _sample_weight_extra(version_dir) or {}
    slice_extra = _slice_extra(from_node, only_node)
    if slice_extra:
        extra.update(slice_extra)
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs=metadata_kwargs,
        run_id=run_context.run_id,
        extra_metadata=extra or None,
        symlink_target=None,
        params_name="parameters_training",
        params_dict=params_training,
    )
    logger.info("Pipeline 'training' completed successfully")


def _dataset_versions_from_model_manifest(
    model_dir: Path,
    data_dir: Path,
) -> tuple[str, str, str | None]:
    """Return (base_dataset_version, train_variant_id, calibration_variant_id) for a model.

    Reads the model's manifest; falls back to ``latest`` resolutions per layer
    when fields are missing.
    """
    try:
        manifest = read_manifest(model_dir)
    except FileNotFoundError:
        logger.warning(
            "Model manifest not found at %s; falling back to dataset latest.", model_dir
        )
        manifest = {}

    dataset_dir = data_dir / "dataset"
    base_v = manifest.get("base_dataset_version") or resolve_base_dataset_version(
        dataset_dir, None
    )
    base_dir = dataset_dir / base_v
    train_v = manifest.get("train_variant_id") or resolve_variant_id(
        base_dir, "train", None
    )
    cal_v = manifest.get("calibration_variant_id")
    return base_v, train_v, cal_v


@app.command(name="inference")
def inference(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(
        None, "--model-version", help="Model version to use for inference (default: best symlink)"
    ),
    rebuild_dates: Optional[str] = typer.Option(
        None, "--rebuild-dates",
        help="Comma-separated snap_dates to re-score even though their "
             "unranked_predictions partitions already exist (use after an "
             "upstream backfill). Must be a subset of inference.snap_dates.",
    ),
    from_node: Optional[str] = typer.Option(
        None, "--from-node",
        help="Start from this node (topological position); missing upstream "
             "artifacts are auto re-run",
    ),
    only_node: Optional[str] = typer.Option(
        None, "--only-node",
        help="Run a single node (plus minimal upstream re-runs for missing inputs)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the slice execution plan and exit"
    ),
    list_nodes: bool = typer.Option(
        False, "--list-nodes",
        help="List pipeline nodes with their resume cost and exit",
    ),
):
    """Run the inference pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("inference", env)

    # (A21) --rebuild-dates ⊆ inference.snap_dates. Checked before Spark starts:
    # a typo here would otherwise cost a cold start before failing.
    try:
        rebuild = resolved_inference_rebuild_dates(
            params,
            [d.strip() for d in rebuild_dates.split(",")] if rebuild_dates else None,
        )
    except ValueError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1)

    get_or_create_spark_session(_load_spark_config(config, "inference"))
    data_dir = _find_data_dir()

    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    base_v, train_v, cal_v = _dataset_versions_from_model_manifest(
        models_dir / mv, data_dir
    )

    try:
        params_inference = config.get_parameters_by_name("parameters_inference")
    except KeyError:
        params_inference = {}

    inf_config = params_inference.get("inference", params_inference)
    snap_dates_list = inf_config.get("snap_dates", [])
    snap_date = snap_dates_list[0].replace("-", "") if snap_dates_list else "unknown"

    logger.info("Model version: %s (%s)", mv, model_version if model_version else "best")
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": snap_date,
        "source_model_version": model_version,
        # Read by predict_and_write_scores: a month named here has all its
        # scoring chunks re-scored even though their partitions exist.
        REBUILD_SNAP_DATES_KEY: rebuild,
    }

    executed = _execute_pipeline(
        "inference", {}, runtime_params, config, params, env,
        from_node=from_node, only_node=only_node,
        dry_run=dry_run, list_nodes=list_nodes,
        # Both halves apply here: the flag drives one node (so it can be sliced
        # away entirely) and that node sits on a chain (so a slice can leave
        # part of it stale).
        rebuild_advice={
            "rebuild": rebuild,
            "targets": _INFERENCE_REBUILD_TARGET_NODES,
            "predict_node": _INFERENCE_REBUILD_PREDICT_NODE,
            "chain": "評分鏈",
        },
    )
    if not executed:
        return

    # Post run
    version_dir = data_dir / "inference" / mv / snap_date
    metadata_kwargs: dict = {
        "version": mv,
        "pipeline": "inference",
        "parameters": params_inference,
        "model_version": mv,
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
    }
    if cal_v is not None:
        metadata_kwargs["calibration_variant_id"] = cal_v

    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs=metadata_kwargs,
        run_id=run_context.run_id,
        extra_metadata=_slice_extra(from_node, only_node),
        symlink_target=data_dir / "inference" / "latest",
        params_name="parameters_inference",
        params_dict=params_inference,
    )
    logger.info("Pipeline 'inference' completed successfully")


@app.command(name="evaluation")
def evaluation(
    env: str = typer.Option("local", "--env", "-e", help="Config environment"),
    model_version: Optional[str] = typer.Option(None, "--model-version", help="Model version to use"),
    post_training: bool = typer.Option(
        False, "--post-training",
        help="Read predictions from training_eval_predictions (default: ranked_predictions for monitoring)",
    ),
    compare: Optional[str] = typer.Option(
        None, "--compare",
        help="Compare-source key from evaluation.compare_sources (produces report_comparison.html alongside report.html)",
    ),
    compare_only: Optional[str] = typer.Option(
        None, "--compare-only",
        help="Like --compare, but skip prepare/compute/baseline/report and read eval_predictions from Hive (only produces report_comparison.html)",
    ),
    from_node: Optional[str] = typer.Option(
        None, "--from-node",
        help="Start from this node (topological position); missing upstream "
             "artifacts are auto re-run",
    ),
    only_node: Optional[str] = typer.Option(
        None, "--only-node",
        help="Run a single node (plus minimal upstream re-runs for missing inputs)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the slice execution plan and exit"
    ),
    list_nodes: bool = typer.Option(
        False, "--list-nodes",
        help="List pipeline nodes with their resume cost and exit",
    ),
):
    """Run the evaluation pipeline."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, run_context = _load_config_and_setup("evaluation", env)

    # (A22) --post-training evaluates one configured test month. Wired here,
    # not in validate_config_consistency: that runs at CLI entry and cannot see
    # this flag, and monitoring mode (no flag) reads inference output whose
    # month need not be a test month. Checked before Spark starts, like A21.
    # No --compare-only carve-out: that mode re-reads enriched_eval_predictions,
    # which is filtered by model_version alone and so accumulates months across
    # config edits exactly like training_eval_predictions — the same exposure.
    # And --post-training is inert on that path (create_pipeline returns the
    # compare-only branch before it is read), so anyone this blocks gets an
    # identical run by dropping the flag.
    snap_date_errs = post_training_snap_date_errors(params, post_training=post_training)
    if snap_date_errs:
        logger.error("\n".join(snap_date_errs))
        raise typer.Exit(code=1)

    get_or_create_spark_session(_load_spark_config(config, "evaluation"))
    data_dir = _find_data_dir()

    models_dir = data_dir / "models"
    mv = resolve_model_version(models_dir, model_version)
    if model_version is not None and not (models_dir / mv).is_dir():
        logger.error("Model version directory not found: %s", models_dir / mv)
        raise typer.Exit(code=1)

    base_v, train_v, cal_v = _dataset_versions_from_model_manifest(
        models_dir / mv, data_dir
    )

    try:
        params_eval = config.get_parameters_by_name("parameters_evaluation")
    except KeyError:
        params_eval = {}

    eval_config = params_eval.get("evaluation", params_eval)
    snap_date = str(eval_config.get("snap_date", "unknown")).replace("-", "")

    # A13: mutual-exclusive
    errs = compare_mutual_exclusive_errors(compare, compare_only)
    if errs:
        raise ConfigConsistencyError("\n".join(errs))

    # A12: resolve key → source dict (also handles None gracefully)
    compare_key = compare or compare_only
    compare_source_dict = compare_source_key_exists(params_eval, compare_key)
    if compare_source_dict is not None:
        # Stage into the merged `params` dict that pipeline nodes actually read.
        # NOTE: do NOT only mutate `params_eval` — it works today by dict-reference
        # sharing in _deep_merge, but breaks silently if `evaluation:` ever appears
        # in another parameters_*.yaml.
        params.setdefault("evaluation", {})["compare"] = compare_source_dict

    logger.info(
        "Evaluation — model_version: %s (%s), post_training: %s, compare: %s%s",
        mv, model_version if model_version else "best", post_training,
        compare_key or "none",
        " (compare-only)" if compare_only else "",
    )
    logger.info("Evaluation — snap_date: %s", snap_date)

    runtime_params = {
        "base_dataset_version": base_v,
        "train_variant_id": train_v,
        "calibration_variant_id": cal_v if cal_v is not None else _NONE_PLACEHOLDER,
        "model_version": mv,
        "snap_date": snap_date,
    }

    pipeline_kwargs = {
        "post_training": post_training,
        "compare_source": compare_source_dict,
        "compare_only": bool(compare_only),
    }
    executed = _execute_pipeline(
        "evaluation", pipeline_kwargs, runtime_params, config, params, env,
        from_node=from_node, only_node=only_node,
        dry_run=dry_run, list_nodes=list_nodes,
    )
    if not executed:
        return

    # Post run
    version_dir = data_dir / "evaluation" / mv / snap_date
    extra = {"snap_date": snap_date, "post_training": post_training}
    slice_extra = _slice_extra(from_node, only_node)
    if slice_extra:
        extra.update(slice_extra)
    _write_pipeline_manifest(
        version_dir=version_dir,
        metadata_kwargs={
            "version": mv,
            "pipeline": "evaluation",
            "parameters": params_eval,
            "model_version": mv,
        },
        run_id=run_context.run_id,
        extra_metadata=extra,
        symlink_target=data_dir / "evaluation" / "latest"
    )
    logger.info("Pipeline 'evaluation' completed successfully")


if __name__ == "__main__":
    app()
