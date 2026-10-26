"""Which settings an evaluation JSON was computed with, and whether they still hold.

Slicing (``--only-node`` / ``--from-node``) stops at "the file exists", and an
evaluation JSON's path only carries ``model_version`` and ``snap_date``. Change
a setting, re-run only ``generate_report``, and a JSON computed under the old
setting is read into the report with exit code 0 (ADR-0020, bug 2).

Two kinds of settings (ADR-0020 decision 2)
===========================================
* **Applied**: they change a Spark computation, the sample, or a landed
  result. Each step that reads one declares it in :data:`APPLIED_SETTINGS`.
* **Drawn**: every other key — ``evaluation.report.display.*`` except
  ``compare_k``, and the section switches ``evaluation.report.sections.*``
  except ``popularity_baseline`` at ``resolve_display_ks``. They only change
  what the report shows, so ``--only-node generate_report`` is enough.

A section switch decides whether a computation is in the pipeline, not what
it computes, so no result records it (ADR-0031 principle 5) — except
``popularity_baseline`` at ``resolve_display_ks``: with it off, that node is
not wired the baseline comparison and lands no baseline block.

Each result records only what it read (ADR-0031 principle 5)
============================================================
A landed JSON's ``config_fingerprint`` is :func:`fingerprint`'s
``{"node", "applied", "inputs"}``:

* ``applied``: the values of the settings ``node`` declares in
  :data:`APPLIED_SETTINGS`, as they were when it ran;
* ``inputs``: for every node upstream of it whose landed JSON it took, the
  values that node applied — each input's own ``applied`` and ``inputs``,
  merged.

Not ``applied`` alone: a result carries numbers its inputs computed, and
one that applies nothing (``baseline_comparison``) would otherwise pass after
``k_values`` changed.

:func:`require_computed_with_current_config` compares the two parts
separately and says which one moved. ``applied`` is always compared with
today's settings. An ``inputs`` record is too, except one of a step not in
``run_nodes``: no re-run of this run could make it current (under
``--compare-only``, ``prepare_eval_data``'s).

Where a stale result sends the re-run
=====================================
To the first node of the run, in its topological order, that applies a
changed setting or produced a result with no fingerprint
(:func:`rerun_node`). ``--from-node`` re-runs it and every node after it in
that order, which holds every other node applying that setting and
everything downstream of them. A reader given the run's order
(``run_nodes``) advises within it: which nodes a run has follows the mode and
the section switches, and under ``--compare`` with the main report's
sections off the main K cut is not in it, so the comparison's cut is where a
changed ``compare_k`` must go. A reader not given it falls back on
:data:`APPLIED_SETTINGS` order, which is right only for main-line steps.

Settings outside ``evaluation`` and the ones left out
=====================================================
``schema`` and ``dataset.test_zero_positive_group_ratio`` are declared like
any other. ``hive.db`` and ``_compare_source_tables`` only decide which table
a ``model_version`` compare source is read off, so they reach the comparison
results through the source's identity
(``compare_sources.compare_source_identity``), not through a declaration.
``evaluation.compare`` is left out on purpose: the CLI injects it per run, and
the comparison results record their source under their own key instead
(:func:`require_same_recorded_values`). ``model_version`` and the run's dates
are in every result's path.

Why the fingerprint lives inside the JSON, not in its path
==========================================================
A hash in the path would retire stale files by itself, but every config change
would grow another directory tree, and every directory reader would have to
learn to pick one. Inside the file, paths stay put and readers pay one check.

Why stdlib only
===============
The module itself imports nothing from the project. That does not make it
Spark-free to import: it sits in ``pipelines/evaluation/steps/``, so importing
it runs the package's ``__init__``, which loads ``pipeline.py`` and with it
pyspark. S3 forbids a module outside this pipeline to import it.

What the fingerprint compares
=============================
Declared values, not effective ones. A key that is absent is left out of
the recorded values; it is not recorded as ``None``, because
``sections.get("popularity_baseline", True)`` treats a missing key and an
explicit ``null`` in opposite ways. The comparison walks the declared paths
as well as the recorded ones, so a key set since is a change too. The flip
side: writing a default out explicitly (``k_values: [5, "all"]`` where it
was omitted) counts as a change. That errs towards raising, and the message
names the key, so it costs one re-run, never a wrong report.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, NamedTuple, Optional, Sequence

__all__ = [
    "APPLIED_SETTINGS", "COMPARISON_CHAIN", "LoadedArtifact",
    "PARTITION_FINGERPRINT_COLUMN", "PARTITION_FINGERPRINT_KEYS", "fingerprint",
    "normalised", "partition_fingerprint", "recorded_partition_fingerprint",
    "require_computed_with_current_config", "require_same_recorded_values",
    "rerun_node",
]

#: The settings each fingerprinted step applies: ``node -> dotted paths from
#: the parameters root``. Listed in topological order: the main-line steps'
#: order is the advice of the readers that are not given the run's (see the
#: module docstring). A step reading part of a block declares the block when
#: another part would change its result too.
#:
#: ``schema`` is declared whole by every step that resolves a column role
#: (``core/schema.py::get_schema``), not role by role: a role change is rare and
#: redefines the query groups, the ranks or the item list, and one reader missed
#: in a role-by-role trace reopens the hole.
APPLIED_SETTINGS: dict[str, tuple[str, ...]] = {
    # post_training is the --post-training / monitoring run mode, injected at
    # the top level by the CLI like model_version, not a YAML key. It is not
    # read in the node body: create_pipeline wires a different predictions
    # and population table by it. Unlike model_version it is in no path, so
    # a monitoring redraw of a post-training directory must be refused here.
    #
    # The category column is read here, its block's other keys are not:
    # switching `enabled` on while `column` is already written is caught later
    # by compute_row_facts, loudly.
    "prepare_eval_data": (
        "schema", "post_training", "evaluation.snap_date",
        "evaluation.segment_columns", "evaluation.segment_sources",
        "evaluation.item_categories.column",
    ),
    "compute_row_facts": (
        "schema", "evaluation.snap_date", "evaluation.item_categories",
        "evaluation.query_filter",
    ),
    # post_training together with dataset.test_zero_positive_group_ratio
    # decides the row weights.
    "compute_prediction_quality": (
        "schema", "post_training", "dataset.test_zero_positive_group_ratio",
        "evaluation.snap_date", "evaluation.prediction_quality",
    ),
    "compute_report_aggregates": (
        "schema", "evaluation.snap_date", "evaluation.report.diagnostics",
    ),
    "restrict_to_common": ("schema", "evaluation.snap_date"),
    "compute_metrics": (
        "schema", "evaluation.snap_date", "evaluation.item_categories",
        "evaluation.query_filter", "evaluation.k_values", "evaluation.metric",
    ),
    "compute_baseline_metrics": (
        "schema", "evaluation.snap_date", "evaluation.item_categories",
        "evaluation.query_filter", "evaluation.k_values", "evaluation.metric",
        "evaluation.baseline",
    ),
    "compute_compare_metrics": (
        "schema", "evaluation.snap_date", "evaluation.item_categories",
        "evaluation.query_filter", "evaluation.k_values", "evaluation.metric",
    ),
    "compare_to_baseline": (),
    "compare_to_source": (),
    # The switch: see the module docstring.
    "resolve_display_ks": (
        "evaluation.k_values", "evaluation.report.display.compare_k",
        "evaluation.report.sections.popularity_baseline",
    ),
    "resolve_comparison_display_ks": (
        "evaluation.k_values", "evaluation.report.display.compare_k",
    ),
}

#: The fingerprinted steps of the comparison chain in topological order, the
#: only ones ``--compare-only`` runs; the readers skip a record by
#: ``run_nodes``, not by this. That mode reads partitions an earlier run wrote,
#: so the ``prepare_eval_data`` record its results inherit (through the
#: directory's ``evaluation_segment_columns``) is compared with nothing: it is
#: what the partitions were written under, and today's run mode and segment
#: settings say nothing about them. The price: after a standard run rewrites the
#: directory under other settings, ``--compare-only --only-node
#: generate_comparison_report`` draws the comparison computed before it, exit
#: code 0 (``docs/pipelines/evaluation.md`` §9). ``schema`` is in that record
#: too, so partitions written under other roles are not refused; the chain's own
#: steps declare ``schema``, which only catches a role changed since they ran.
COMPARISON_CHAIN: tuple[str, ...] = (
    "restrict_to_common", "compute_compare_metrics", "compare_to_source",
    "resolve_comparison_display_ks",
)

#: What one date's ``enriched_eval_predictions`` partition holds depends on:
#: ``prepare_eval_data``'s settings without ``evaluation.snap_date`` and
#: ``evaluation.item_categories.column``.
#:
#: Why ``evaluation.snap_date`` is left out: a partition holds one date, and
#: its rows depend on the run mode, the column roles and the segment
#: settings, not on which other dates the same run evaluated. Kept in, two runs
#: with identical settings and different date ranges would refuse each other's
#: partitions.
#:
#: Why the category column is left out: it decides the category table, which
#: lands in the run's directory, not a value in the rows. Kept in, changing it
#: would refuse every month's partition, including the months this run does not
#: rewrite.
_NOT_IN_THE_ROWS = ("evaluation.snap_date", "evaluation.item_categories.column")
PARTITION_FINGERPRINT_KEYS: tuple[str, ...] = tuple(
    path for path in APPLIED_SETTINGS["prepare_eval_data"]
    if path not in _NOT_IN_THE_ROWS
)

#: The framework's own column on every ``enriched_eval_predictions`` row:
#: :func:`partition_fingerprint` of the run that wrote the row's partition
#: (these settings plus the segment columns that run actually joined).
#:
#: Why the partition carries it, and the ``evaluation_segment_columns`` JSON's
#: fingerprint does not speak for the partition: that JSON sits in the run's
#: directory (``<model_version>/<dates label>/``), and one date's partition is
#: written by runs with different directories, so a range directory's JSON can
#: say settings A while one of its dates holds B's rows. Only the rows can say
#: who wrote them last.
PARTITION_FINGERPRINT_COLUMN = "eval_partition_fingerprint"

_ABSENT = object()
_MAX_REPR = 80


class LoadedArtifact(NamedTuple):
    """One landed evaluation artifact to check against today's settings.

    A plain positional tuple would let ``catalog_name`` and ``produced_by`` —
    both ``str`` — swap without a type error, silently corrupting the
    advice (``docs/operations/known-pitfalls.md`` §12 positional trap).
    Construct this with keyword arguments.
    """
    catalog_name: str
    payload: Any
    #: The node that lands it, whose :data:`APPLIED_SETTINGS` its ``applied``
    #: part is compared on.
    produced_by: str


def _tag_non_str_keys(value: Any) -> Any:
    """Recursively rewrite dict keys so every key reaching ``json.dumps`` is a
    ``str``.

    YAML can produce non-``str`` dict keys (e.g.
    ``dataset.sample_ratio_overrides: {1: 0.5}``). ``json.dumps(...,
    sort_keys=True)`` sorts by the *original* key objects, so a dict mixing an
    ``int`` and a ``str`` key raises ``TypeError`` on a legal config; and
    ``json.dumps({1: 0.5})`` and ``json.dumps({"1": 0.5})`` serialise alike,
    giving identical fingerprints to two configs that behave differently.

    A ``str`` key is kept as-is. A non-``str`` key ``k`` is rewritten to
    ``f"<{type(k).__name__}>{k!r}"`` (``int`` key ``1`` -> ``"<int>1"``); config
    keys do not start with such a tag, so a real ``str`` key and a tagged
    non-``str`` key never collide. Lists/tuples recurse into elements.
    """
    if isinstance(value, dict):
        return {
            (k if isinstance(k, str) else f"<{type(k).__name__}>{k!r}"):
                _tag_non_str_keys(v)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_tag_non_str_keys(v) for v in value]
    return value


def fingerprint(
    parameters: dict, node: str, inputs: Iterable[Optional[dict]] = (),
) -> dict:
    """``{"node", "applied", "inputs"}`` for a result ``node`` lands from
    ``inputs``, the landed JSON payloads it took (``None`` for an optional
    entry that was absent).

    Values, not only a hash, so a mismatch can say which key moved and from
    what. They are normalised (:func:`normalised`) so that what a landed JSON
    holds after being read back compares equal to what is computed now, e.g. a
    YAML ``datetime.date`` against its string.

    Pass every landed JSON the result is computed from, checked or not: a
    reader that only takes ``evaluation_segment_columns`` for the partition
    check still read partitions ``prepare_eval_data`` wrote. An input
    without a fingerprint in this shape contributes nothing: the caller
    must refuse one first; this does not.

    A ``node`` missing from :data:`APPLIED_SETTINGS` raises ``KeyError``: a
    new fingerprinted step must declare what it applies.
    """
    upstream: dict[str, dict] = {}
    for payload in inputs:
        stored = _stored(payload)
        if stored is None:
            continue
        upstream.update(stored["inputs"])
        upstream[stored["node"]] = stored["applied"]
    return {
        "node": node,
        "applied": _declared_values(parameters, APPLIED_SETTINGS[node]),
        "inputs": upstream,
    }


#: Where :func:`partition_fingerprint` hashes the segment columns actually
#: joined, next to the settings' dotted paths. Named after the landed artifact
#: field it mirrors, which no config path can spell.
_JOINED = "evaluation_segment_columns.joined"


def partition_fingerprint(parameters: dict, joined: Sequence[str]) -> str:
    """The sha256 stored on every row of a partition ``prepare_eval_data``
    writes: :data:`PARTITION_FINGERPRINT_KEYS` plus ``joined``, the segment
    columns that run actually joined.

    Why ``joined`` is in it: the settings do not say what the rows hold. A
    segment column the population table lacks is skipped, not raised, so two
    runs with identical settings write different rows when the population
    changed between them, and a reader whose own JSON says the column was
    joined would put those rows in the unmatched segment, exit code 0. Same
    settings and the same population still give the same hash on every date,
    whichever run wrote it.

    Same normalisation as :func:`fingerprint`. Only the hash is stored: it
    repeats on every row, and a mismatch is fixed the same way whichever part
    moved (``--from-node prepare_eval_data``).
    """
    return _sha256({**_declared_values(parameters, PARTITION_FINGERPRINT_KEYS),
                    _JOINED: list(joined)})


def recorded_partition_fingerprint(payload: Any) -> Optional[str]:
    """The :func:`partition_fingerprint` the run that landed ``payload`` (an
    ``evaluation_segment_columns`` JSON) stamped on its partitions; ``None`` if
    ``payload`` has no ``config_fingerprint`` in :func:`fingerprint`'s shape
    or no ``joined`` list.

    Taken from the recorded ``applied`` values plus the recorded ``joined``.
    For readers that must compare a partition with the run that wrote a
    directory rather than with today's parameters (``--compare-only``, where
    ``post_training`` is inert).
    """
    stored = _stored(payload)
    if stored is None or not isinstance(payload.get("joined"), list):
        return None
    return _sha256({**{path: value for path, value in stored["applied"].items()
                       if path in PARTITION_FINGERPRINT_KEYS},
                    _JOINED: list(payload["joined"])})


def normalised(value: Any) -> Any:
    """``value`` as a landed JSON holds it once read back: non-``str`` dict
    keys tagged (:func:`_tag_non_str_keys`), then a JSON round trip
    (``default=str``). What a node records in its artifact goes through this,
    so the value read back compares equal to today's normalised one."""
    return json.loads(json.dumps(
        _tag_non_str_keys(value), sort_keys=True, ensure_ascii=False,
        default=str))


def _declared_values(parameters: dict, paths: Iterable[str]) -> dict[str, Any]:
    """Each present path's value, :func:`normalised`."""
    values: dict[str, Any] = {}
    for path in paths:
        found, value = _lookup(parameters, path)
        if found:
            values[path] = normalised(value)
    return values


def _sha256(values: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        values, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _stored(payload: Any) -> Optional[dict]:
    """``payload``'s ``config_fingerprint`` when it has :func:`fingerprint`'s
    shape, else ``None`` (no fingerprint, or one of another shape)."""
    stored = payload.get("config_fingerprint") if isinstance(payload, dict) else None
    if not (isinstance(stored, dict) and isinstance(stored.get("node"), str)
            and isinstance(stored.get("applied"), dict)
            and isinstance(stored.get("inputs"), dict)
            and all(isinstance(v, dict) for v in stored["inputs"].values())):
        return None
    return stored


def rerun_node(
    changed: Iterable[str], *, run_nodes: Optional[Sequence[str]] = None,
) -> Optional[str]:
    """The node ``--from-node`` should name when ``changed`` moved: the first
    of ``run_nodes`` — the run's nodes in topological order, by default
    :data:`APPLIED_SETTINGS` order — that applies one of them. ``None`` when
    none does.

    ``changed`` holds changed setting paths, at any depth, and node names
    (a stale result with no fingerprint names its producer). A step applies a
    path when one of its declared paths contains it or lies inside it: a
    changed ``evaluation.item_categories.column`` is ``prepare_eval_data``'s,
    a changed ``evaluation.item_categories.mapping.x`` is not.
    """
    changed = list(changed)
    for node in (APPLIED_SETTINGS if run_nodes is None else run_nodes):
        paths = APPLIED_SETTINGS.get(node, ())
        if any(c == node or any(_overlap(c, p) for p in paths) for c in changed):
            return node
    return None


def _overlap(a: str, b: str) -> bool:
    return a == b or a.startswith(b + ".") or b.startswith(a + ".")


def require_computed_with_current_config(
    artifacts: Iterable[LoadedArtifact],
    parameters: dict,
    *,
    run_nodes: Optional[Sequence[str]] = None,
) -> None:
    """Pre-check (inputs): every artifact was computed with today's settings —
    the ones its own step applied, and the ones each of its inputs did.

    A pre-check, not a postcondition: a mismatch means the inputs on disk are
    older than the config, not that the calling node is wrong.

    Collect-all: every stale artifact is listed in one raise, each moved
    setting under the part it moved in (its own step's, or an input's). One
    re-run is advised for all of them, :func:`rerun_node` of every changed path
    and every producer of an artifact with no fingerprint; when none applies,
    the full pipeline.

    ``run_nodes``: the run's nodes in topological order, when the reader
    knows them. The advice then names one of them, and a record an artifact
    inherited from a step not among them is not compared (the module
    docstring says why). ``None``: every fingerprinted step, in
    :data:`APPLIED_SETTINGS` order.
    """
    problems: list[str] = []
    changed: list[str] = []
    # Pre-check (runtime backstop): left empty, every inherited record would
    # be skipped and a stale input would pass.
    if run_nodes is not None and not run_nodes:
        raise RuntimeError(
            "require_computed_with_current_config was given an empty run "
            "order: create_pipeline did not fill it.")
    in_run = APPLIED_SETTINGS if run_nodes is None else set(run_nodes)

    for artifact in artifacts:
        name = f"{artifact.catalog_name} (written by {artifact.produced_by})"
        stored = _stored(artifact.payload)
        if stored is None:
            problems.append(
                f"  - {name} has no config_fingerprint in the shape this "
                "version writes: written by an older version, or not by this "
                "pipeline."
            )
            changed.append(artifact.produced_by)
            continue
        parts = [(f"settings {artifact.produced_by} applied",
                  _changed_paths(stored["applied"], parameters,
                                 APPLIED_SETTINGS.get(artifact.produced_by, ())))]
        for node, recorded in stored["inputs"].items():
            if node not in in_run:
                continue
            parts.append((f"settings its input from {node} was computed with",
                          _changed_paths(recorded, parameters,
                                         APPLIED_SETTINGS.get(node, ()))))
        lines: list[str] = []
        for label, leaves in parts:
            if not leaves:
                continue
            lines.append(f"      {label}:")
            lines += [f"        {leaf}: {_short(old)} -> {_short(new)}"
                      for leaf, old, new in leaves]
            changed += [leaf for leaf, _, _ in leaves]
        if lines:
            problems.append(f"  - {name} is stale:\n" + "\n".join(lines))

    if not problems:
        return

    node = rerun_node(changed, run_nodes=run_nodes)
    advice = (
        f"Re-run with --from-node {node}, which recomputes every artifact "
        "above." if node is not None else
        "Re-run the full pipeline (no slicing flags): no node of this run is "
        "known to recompute all of the artifacts above."
    )
    raise ValueError(
        "Evaluation artifacts on disk do not match today's settings; drawing "
        "from them would mix old and new settings. What each step applies is "
        "declared in pipelines/evaluation/steps/config_fingerprint.py "
        "(APPLIED_SETTINGS).\n"
        + "\n".join(problems) + "\n" + advice
    )


def _changed_paths(
    recorded: dict, parameters: dict, declared: Sequence[str],
) -> list[tuple[str, Any, Any]]:
    """The changed leaves of ``recorded`` against today's values, over the
    recorded paths and the ``declared`` ones (a path set since it was
    recorded is a change too). A path nested under another walked one
    (``evaluation.item_categories.column`` under ``evaluation.item_categories``)
    reaches the same leaf twice; it is listed once."""
    current = _declared_values(
        parameters, (*declared, *(p for p in recorded if p not in declared)))
    leaves: list[tuple[str, Any, Any]] = []
    shown: set[str] = set()
    for path in (*declared, *(p for p in recorded if p not in declared)):
        for leaf in _changed_leaves(path, recorded.get(path, _ABSENT),
                                    current.get(path, _ABSENT)):
            if leaf[0] not in shown:
                shown.add(leaf[0])
                leaves.append(leaf)
    return leaves


def require_same_recorded_values(
    artifacts: Iterable[LoadedArtifact],
    key: str,
    current: dict,
    *,
    setting: str,
    rerun_node: str,
) -> None:
    """Pre-check (inputs): every artifact recorded ``current`` under ``key``.

    For values a fingerprint deliberately leaves out but some artifacts still
    depend on: the compare source (``evaluation.compare``, ADR-0031 decision
    5) is injected per run by the CLI, so :data:`APPLIED_SETTINGS` leaves it
    out (see the module docstring), yet every comparison artifact holds one
    source's numbers. Its writer records the value (``current``, already
    :func:`normalised`) under ``key``; this compares what was recorded.

    Why the re-run is the caller's ``rerun_node`` and not :func:`rerun_node`:
    a changed value makes every artifact that records it stale at once, so
    the advice is the first node whose output depends on it, a node no
    declaration names.

    Collect-all, like :func:`require_computed_with_current_config`; an
    artifact recording nothing under ``key`` is stale too. ``setting`` names
    the value in the message and prefixes each changed leaf.
    """
    problems: list[str] = []
    for artifact in artifacts:
        recorded = artifact.payload.get(key, _ABSENT) \
            if isinstance(artifact.payload, dict) else _ABSENT
        if recorded is _ABSENT:
            problems.append(
                f"  - {artifact.catalog_name} (written by "
                f"{artifact.produced_by}) records no {key}: written before "
                "it was recorded, or not by this pipeline."
            )
            continue
        leaves = _changed_leaves(setting, recorded, current)
        if leaves:
            problems.append(
                f"  - {artifact.catalog_name} (written by "
                f"{artifact.produced_by}) was computed with another {setting}:\n"
                + "\n".join(f"      {leaf}: {_short(old)} -> {_short(new)}"
                            for leaf, old, new in leaves)
            )
    if problems:
        raise ValueError(
            f"Evaluation artifacts on disk were computed with another "
            f"{setting} than today's; drawing from them would mix the two:\n"
            + "\n".join(problems) + "\n"
            f"Re-run with --from-node {rerun_node}, which recomputes every "
            "artifact above."
        )


def _lookup(parameters: dict, path: str) -> tuple[bool, Any]:
    cursor: Any = parameters
    for seg in path.split("."):
        if not isinstance(cursor, dict) or seg not in cursor:
            return False, None
        cursor = cursor[seg]
    return True, cursor


def _canonical(value: Any) -> str:
    """Compare by JSON text: ``1``, ``1.0`` and ``true`` are equal in Python
    but serialise differently, and the comparison must tell them apart."""
    if value is _ABSENT:
        return "\0absent"
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def _changed_leaves(path: str, old: Any, new: Any) -> list[tuple[str, Any, Any]]:
    """The deepest paths whose values differ, so the message names
    ``evaluation.metric.min_positives`` rather than the whole ``metric`` block.
    """
    if _canonical(old) == _canonical(new):
        return []
    old_dict_or_absent = isinstance(old, dict) or old is _ABSENT
    new_dict_or_absent = isinstance(new, dict) or new is _ABSENT
    if (isinstance(old, dict) or isinstance(new, dict)) \
            and old_dict_or_absent and new_dict_or_absent:
        o = old if isinstance(old, dict) else {}
        n = new if isinstance(new, dict) else {}
        leaves: list[tuple[str, Any, Any]] = []
        for key in (*o, *(k for k in n if k not in o)):
            leaves.extend(_changed_leaves(
                f"{path}.{key}", o.get(key, _ABSENT), n.get(key, _ABSENT)))
        if leaves:
            return leaves
    return [(path, old, new)]


def _short(value: Any) -> str:
    if value is _ABSENT:
        return "<absent>"
    text = repr(value)
    return text if len(text) <= _MAX_REPR else text[:_MAX_REPR - 3] + "..."
