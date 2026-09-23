"""Which settings an evaluation JSON was computed with, and whether they still hold.

Why this exists
===============
Slicing (``--only-node`` / ``--from-node``) stops at "the file exists", and an
evaluation JSON's path only carries ``model_version`` and ``snap_date``. Change
a setting, re-run only ``generate_report``, and a JSON computed under the old
setting is read into the report with exit code 0 (ADR-0020, bug 2).

Two kinds of settings (ADR-0020 decision 2)
===========================================
* **Computed**: they change a Spark computation or the sample, or decide
  whether something is computed at all. Changing one means re-running from
  the node :data:`COMPUTED_KEYS` names for it (normally its first reader).
* **Drawn**: ``evaluation.report.display.*`` and every
  ``evaluation.report.sections.*`` key except ``baseline``,
  ``diagnostics`` and ``prediction_quality``. They only change what the
  report shows, so ``--only-node generate_report`` is enough and takes
  seconds.

Only computed settings go into the fingerprint. Fingerprinting drawn ones too
would make every cosmetic change raise, which is the "re-run everything on any
change" the user rejected. ``sections.baseline``, ``sections.diagnostics``
and ``sections.prediction_quality`` are computed, not drawn: ``baseline:
false`` makes ``compute_baseline_metrics`` return a stub, and ``diagnostics:
false`` / ``prediction_quality: false`` do the same for
``compute_report_aggregates`` / ``compute_prediction_quality``; treated as
drawn, flipping them back and re-drawing yields a report with a section
silently missing.

One row of :data:`COMPUTED_KEYS`, ``post_training``, is not a YAML config
setting at all: it is the ``--post-training`` / monitoring run mode, injected
into ``parameters`` at the top level by the CLI exactly like ``model_version``
and ``snap_date`` are (never nested under ``evaluation``). Unlike those two,
which are already in every artifact's path, the mode is not in the path, yet it
changes which predictions table ``prepare_eval_data`` reads, so two runs of
the same ``(model_version, snap_date)`` in different modes must not have their
artifacts read interchangeably. Node-level unit tests that build ``parameters``
by hand and never set this key see it absent both when a node writes its
fingerprint and when another node later checks it, so they stay unaffected;
only a real mode switch between two runs makes it present on one side and
absent (or the other value) on the other.

Why a closed list, not a hash of the whole ``evaluation`` subtree
=================================================================
* ``evaluation.compare`` is injected into that subtree by the CLI at run time
  (``__main__.py``). Hashing the subtree would make a ``--compare`` run and a
  plain run disagree on every artifact: raises everywhere, all false.
* Some computations read keys outside the subtree (``config_shift`` reads
  ``dataset.*`` and ``training.*``). Each registry diagnosis declares those
  itself (``EXTRA_CONFIG_KEYS``, read through
  ``diagnosis.metric.contract.extra_config_keys_for``), and the caller passes
  them in as ``extra_keys``. A diagnosis JSON's fingerprint is this list plus
  what that diagnosis declares.

Why the fingerprint lives inside the JSON, not in its path
==========================================================
A hash in the path (``<mv>/<snap>/<hash>/...``) would retire stale files by
itself, but every config change would grow another directory tree, and every
directory reader (``--compare-only``, ``scripts/render_diagnosis.py``, the
manifest's artifact list) would have to learn to pick one. Inside the file,
paths stay put and readers pay one check.

Why stdlib only, and no ``diagnosis`` import
============================================
Dependency direction: diagnoses declare their extra keys in their own package,
and the combining happens in ``pipelines/evaluation/`` nodes
(``make_diagnosis_node`` when writing, ``render_diagnosis_pages`` when
checking), the same direction ``pipeline.py`` already uses for
``contract.inputs_for``. The module itself imports nothing from the project;
``tests/test_pipelines/test_evaluation/test_config_fingerprint.py`` pins that by
scanning its imports. That no longer makes it Spark-free to import: since it
moved into ``pipelines/evaluation/steps/`` (#365), importing it runs the
package's ``__init__``, which loads ``pipeline.py`` and with it pyspark. Nothing
outside this pipeline imports it today, and S3 would forbid it.

What the fingerprint compares
=============================
Declared values, not effective ones. A key that is absent is left out of
``values``; it is not recorded as ``None``, because ``sections.get("baseline",
True)`` treats a missing key and an explicit ``null`` in opposite ways. The
flip side: writing a default out explicitly (``k_values: [5, "all"]`` where it
was omitted) counts as a change. That errs towards raising, and the message
names the key, so it costs one re-run, never a wrong report.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, NamedTuple, Optional, Sequence

__all__ = [
    "COMPUTED_KEYS", "LoadedArtifact", "PARTITION_CONTENT_KEYS",
    "PARTITION_FINGERPRINT_COLUMN", "PARTITION_FINGERPRINT_KEYS", "fingerprint",
    "partition_fingerprint", "recorded_partition_fingerprint",
    "require_computed_with_current_config",
]

#: Computed settings: ``(dotted path from the parameters root, node to re-run
#: from when it changes)``. Rows are ordered by that node's topological
#: position (non-decreasing), so the first changed row names the earliest node.
#:
#: The node is **the earlier, in topological order, of the key's first reader
#: and the first node that writes a fingerprinted artifact**
#: (``compute_metrics``), not simply the first reader. Every fingerprinted
#: artifact hashes every row here, so a change makes all of them stale, and
#: ``--from-node`` must re-run all of their producers. The ``baseline`` and
#: ``report.*`` rows are where the two differ: first read by
#: ``compute_baseline_metrics`` / ``compute_report_aggregates``, which both
#: sort after ``compute_metrics`` in both modes. Since ``evaluation_metrics``
#: and ``baseline_metrics`` land on disk (ADR-0018 decision 2), a slice no
#: longer pulls their producers back on its own, so advising the first reader
#: would leave ``metrics.json``'s old fingerprint in place and the next report
#: would raise with the same advice, a loop with no exit. The price is one
#: extra ``compute_metrics`` when only a baseline or report-aggregate setting
#: changed.
#:
#: ``tests/test_pipelines/test_evaluation/test_pipeline.py::
#: TestFingerprintRerunNodes`` pins it in both modes: a row whose node sorts
#: after a fingerprinted producer turns that test red.
#:
#: ``prepare_eval_data`` also writes a fingerprinted artifact
#: (``evaluation_segment_columns``, landed with the ``enriched_eval_predictions``
#: partition), yet the rule above does not move every row to it. That artifact
#: is compared on :data:`PARTITION_CONTENT_KEYS` only, the rows already pointing
#: at ``prepare_eval_data``, so a change to any other row never makes it stale
#: and ``--from-node compute_metrics`` stays enough for those (see that
#: constant for why this is not the same loop).
#:
#: The first row, ``post_training``, is not a user config key: it is the
#: ``--post-training`` / monitoring run mode, a CLI flag injected into
#: ``parameters`` at the top level the same way ``model_version`` and
#: ``snap_date`` are (``__main__.py`` ``evaluation()``'s ``runtime_params``).
#: It belongs in this enumeration because it changes a Spark computation the
#: same way any other row here does: ``prepare_eval_data`` reads a different
#: predictions table (``training_eval_predictions`` vs ``ranked_predictions``)
#: depending on it. Without it, a ``--post-training`` run followed by a
#: monitoring ``--only-node generate_report`` on the same ``(model_version,
#: snap_date)`` writes/reads the same catalog paths, the fingerprints of the
#: unrelated keys match, and the report mixes metric CI / report aggregates
#: computed on one population with metrics computed on the other, exit code 0.
#: Unit tests that build ``parameters`` without a ``post_training`` key see it
#: absent on both the write side (``fingerprint()`` in the producing node) and
#: the check side (``require_computed_with_current_config``), so those stay
#: green; only a real mode switch between two runs makes it present-then-absent
#: or True-then-False.
COMPUTED_KEYS: tuple[tuple[str, str], ...] = (
    ("post_training", "prepare_eval_data"),
    ("evaluation.snap_date", "prepare_eval_data"),
    # prepare_eval_data resolves which segment columns to join and from where
    # (ADR-0020 bug 6), and the joined columns are stored in the partition.
    ("evaluation.segment_columns", "prepare_eval_data"),
    ("evaluation.segment_sources", "prepare_eval_data"),
    # prepare_eval_data reads the category table off this sample_pool column
    # and lands it (#379 decision 4). Its own row although the whole block
    # has one at compute_metrics below: changing the column moves both, and
    # the earlier row decides the advice. At compute_metrics alone, the
    # re-run would rank by the old table. A change to the rest of the block
    # (enabled, mapping) still re-runs from compute_metrics only.
    ("evaluation.item_categories.column", "prepare_eval_data"),
    ("evaluation.diagnosis", "draw_diagnosis_sample_node"),
    ("evaluation.k_values", "compute_metrics"),
    ("evaluation.item_categories", "compute_metrics"),
    ("evaluation.metric", "compute_metrics"),
    # First read by evaluation/metrics.py::drop_all_positive_groups, called
    # from compute_metrics (fine grain and category grain) and
    # compute_baseline_metrics (#376). Listed at compute_metrics, the first
    # fingerprinted producer, for the same reason as the baseline/report rows
    # below: flipping it changes which query groups are dropped before any
    # ranking metric is computed, so both a stale evaluation_metrics.json and
    # a stale baseline_metrics.json must be caught, and both sort no earlier
    # than compute_metrics. Not on prepare_eval_data: the filter runs after
    # the Hive read, on the already-joined rows, not on the join itself.
    ("evaluation.query_filter", "compute_metrics"),
    # First read by compute_baseline_metrics / compute_report_aggregates;
    # listed at compute_metrics, the first fingerprinted producer, for the
    # reason in the comment above.
    ("evaluation.baseline", "compute_metrics"),
    ("evaluation.report.sections.baseline", "compute_metrics"),
    ("evaluation.report.diagnostics", "compute_metrics"),
    ("evaluation.report.sections.diagnostics", "compute_metrics"),
    # First read by compute_prediction_quality, which sorts after
    # compute_metrics in both modes; same reason (ADR-0024).
    ("evaluation.prediction_quality", "compute_metrics"),
    ("evaluation.report.sections.prediction_quality", "compute_metrics"),
)

#: The computed settings that decide what ``prepare_eval_data`` writes: the
#: ``enriched_eval_predictions`` partition and the ``evaluation_segment_columns``
#: and ``evaluation_item_categories`` JSONs landed with it. They are the
#: :data:`COMPUTED_KEYS` rows re-run from ``prepare_eval_data``.
#:
#: Why that artifact is compared on these rows only (ADR-0020 bug 6, the #352
#: correction): the partition is read back from Hive, so a slice starting after
#: ``prepare_eval_data`` does not rewrite it. Compared on every row, changing
#: ``evaluation.k_values`` would make it stale, the advice would be
#: ``--from-node compute_metrics`` (that row's node), the re-run would leave it
#: as it was, and the next run would raise the same way: a loop with no exit.
#: Moving every row to ``prepare_eval_data`` instead would end the loop by
#: re-joining the predictions on any setting change. Neither is needed: no
#: other row changes what ``prepare_eval_data`` computes.
#:
#: The JSON's comparison speaks for the segment list only. The partition
#: itself carries :data:`PARTITION_FINGERPRINT_COLUMN` since #374 (see there).
PARTITION_CONTENT_KEYS: tuple[str, ...] = tuple(
    path for path, node in COMPUTED_KEYS if node == "prepare_eval_data"
)

#: What one date's ``enriched_eval_predictions`` partition holds depends on:
#: :data:`PARTITION_CONTENT_KEYS` without ``evaluation.snap_date`` and
#: ``evaluation.item_categories.column``.
#:
#: Why ``evaluation.snap_date`` is left out: a partition holds one date, and
#: its rows depend on the run mode and the segment settings, not on which other
#: dates the same run evaluated. Kept in, a single-date run of March and a
#: January–March run with identical settings would refuse each other's March,
#: though the rows are the same.
#:
#: Why the category column is left out, for the same reason (#379): the
#: column decides the category table, which lands in the run's directory,
#: not a single value in the rows. Kept in, changing it would refuse every
#: month's partition, including the months this run does not rewrite.
_NOT_IN_THE_ROWS = ("evaluation.snap_date", "evaluation.item_categories.column")
PARTITION_FINGERPRINT_KEYS: tuple[str, ...] = tuple(
    path for path in PARTITION_CONTENT_KEYS if path not in _NOT_IN_THE_ROWS
)

#: The framework's own column on every ``enriched_eval_predictions`` row:
#: :func:`partition_fingerprint` of the run that wrote the row's partition
#: (these settings plus the segment columns that run actually joined).
#:
#: Why the partition carries it, and the ``evaluation_segment_columns`` JSON's
#: fingerprint no longer speaks for the partition (#374): that JSON sits in the
#: run's directory (``<model_version>/<dates label>/``), and one date's
#: partition is now written by runs with different directories — March alone
#: writes ``20260331/``, January–March writes ``20260131-20260331/``. After
#: January–March (settings A), March alone (settings B), then a resume of
#: January–March from ``compute_metrics`` (A), the range directory's JSON still
#: says A while March holds B's rows. Only the rows can say who wrote them last.
PARTITION_FINGERPRINT_COLUMN = "eval_partition_fingerprint"

_KEY_ORDER = {path: i for i, (path, _) in enumerate(COMPUTED_KEYS)}
_ABSENT = object()
_MAX_REPR = 80


class LoadedArtifact(NamedTuple):
    """One landed evaluation artifact to check against today's settings.

    A plain positional 4-tuple let ``catalog_name`` and ``produced_by`` —
    both ``str`` — swap without a type error, silently corrupting only the
    advice text (``docs/operations/known-pitfalls.md`` §12 positional trap).
    Construct this with keyword arguments; that makes such a swap visible at
    the call site instead of a silent value error inside the message.
    """
    catalog_name: str
    payload: Any
    produced_by: str
    extra_keys: Sequence[str] = ()
    #: ``None`` compares the whole fingerprint. A tuple of paths compares only
    #: those values: for an artifact that a change to the other rows cannot
    #: make stale, see :data:`PARTITION_CONTENT_KEYS`.
    compared_keys: Optional[Sequence[str]] = None


def _tag_non_str_keys(value: Any) -> Any:
    """Recursively rewrite dict keys so every key reaching ``json.dumps`` is a
    ``str``.

    Why this is needed: YAML can produce non-``str`` dict keys (e.g.
    ``dataset.sample_ratio_overrides: {1: 0.5}``), and two problems follow.
    First, ``json.dumps(..., sort_keys=True)`` sorts by the *original* key
    objects before stringifying them, so a dict mixing an ``int`` and a
    ``str`` key raises ``TypeError`` (``'<' not supported between instances
    of 'str' and 'int'``) — crashing the node on a legal config. Second, JSON
    itself has no non-string keys, so ``json.dumps({1: 0.5})`` and
    ``json.dumps({"1": 0.5})`` both serialise to ``{"1": 0.5}`` — identical
    fingerprints for two configs that behave differently, because every
    lookup against these dicts (e.g. ``config_shift``'s ``_key_from_values``)
    keys by ``str`` and ``{1: 0.5}`` therefore never matches.

    A ``str`` key is kept as-is so stored ``values`` stay readable for the
    common case. A non-``str`` key ``k`` is rewritten to
    ``f"<{type(k).__name__}>{k!r}"`` (e.g. ``int`` key ``1`` -> ``"<int>1"``):
    the ``<...>`` tag cannot be produced by any plain ``str`` key on the same
    dict without itself starting with a ``<type>`` prefix, which config keys
    do not, so a real ``str`` key and a tagged non-``str`` key never collide
    with each other. Lists/tuples recurse into elements so a non-``str`` key
    nested inside one is caught too.
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


def fingerprint(parameters: dict, extra_keys: Sequence[str] = ()) -> dict:
    """``{"sha256": ..., "values": {path: value}}`` over the computed settings.

    ``values`` is kept next to the hash so a mismatch can say which key moved
    and from what, instead of only "something changed". Values are normalised
    through a JSON round trip (``default=str``) so that what a landed JSON
    holds after being read back compares equal to what is computed now, e.g. a
    YAML ``datetime.date`` against its string. Dict keys are tagged first via
    :func:`_tag_non_str_keys` (see there for why).
    """
    values = _declared_values(
        parameters, (*(p for p, _ in COMPUTED_KEYS), *extra_keys))
    return {"sha256": _sha256(values), "values": values}


#: Where :func:`partition_fingerprint` hashes the segment columns actually
#: joined, next to the settings' dotted paths. Named after the landed artifact
#: field it mirrors (``evaluation_segment_columns``' ``joined``), which no
#: config path can spell.
_JOINED = "evaluation_segment_columns.joined"


def partition_fingerprint(parameters: dict, joined: Sequence[str]) -> str:
    """The sha256 stored on every row of a partition ``prepare_eval_data``
    writes: :data:`PARTITION_FINGERPRINT_KEYS` plus ``joined``, the segment
    columns that run actually joined.

    Why ``joined`` is in it: the settings do not say what the rows hold. A
    segment column the population table lacks is skipped, not raised, so two
    runs with identical settings write different rows when the population
    changed between them. January–February joined ``tier``; the population
    then lost it and February was re-run alone (``tier`` NULL in the
    partition, that run's JSON ``joined: []``); resuming January–February, its
    own JSON still says ``joined: [tier]`` and every setting matches, so all of
    February would fall into the unmatched segment, exit code 0. With the
    joined list hashed in, a reader expecting ``[tier]`` refuses February.
    Same settings and the same population still give the same hash on every
    date, whichever run wrote it.

    Same normalisation as :func:`fingerprint`. Only the hash is stored: it
    repeats on every row, and a mismatch is fixed the same way whichever part
    moved (``--from-node prepare_eval_data``).
    """
    return _sha256({**_declared_values(parameters, PARTITION_FINGERPRINT_KEYS),
                    _JOINED: list(joined)})


def recorded_partition_fingerprint(payload: Any) -> Optional[str]:
    """The :func:`partition_fingerprint` the run that landed ``payload`` (an
    ``evaluation_segment_columns`` JSON) stamped on its partitions; ``None`` if
    ``payload`` has no ``config_fingerprint`` or no ``joined`` list.

    Taken from the recorded values, which :func:`fingerprint` normalised the
    same way :func:`partition_fingerprint` normalises live parameters, plus the
    recorded ``joined``. For readers that must compare a partition with the
    run that wrote a directory rather than with today's parameters
    (``--compare-only``, where ``post_training`` is inert).
    """
    stored = payload.get("config_fingerprint") if isinstance(payload, dict) else None
    if not (isinstance(stored, dict) and isinstance(stored.get("values"), dict)
            and isinstance(payload.get("joined"), list)):
        return None
    return _sha256({**{path: value for path, value in stored["values"].items()
                       if path in PARTITION_FINGERPRINT_KEYS},
                    _JOINED: list(payload["joined"])})


def _declared_values(parameters: dict, paths: Iterable[str]) -> dict[str, Any]:
    """Each present path's value, normalised through a JSON round trip."""
    values: dict[str, Any] = {}
    for path in paths:
        found, value = _lookup(parameters, path)
        if found:
            values[path] = json.loads(json.dumps(
                _tag_non_str_keys(value), sort_keys=True, ensure_ascii=False,
                default=str))
    return values


def _sha256(values: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        values, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def require_computed_with_current_config(
    artifacts: Iterable[LoadedArtifact],
    parameters: dict,
) -> None:
    """Pre-check (inputs): every artifact was computed with today's settings.

    ``artifacts`` holds :class:`LoadedArtifact` entries — construct each with
    keyword arguments (see that class for why). A pre-check, not a
    postcondition: a mismatch means the inputs on disk are older than the
    config, not that the calling node is wrong.

    Collect-all: every stale artifact is listed in one raise, so whoever fixes
    it does not re-run, hit the next one, and re-run again.

    The suggested re-run, in order:

    1. A key from :data:`COMPUTED_KEYS` changed: ``--from-node`` the node of
       the first changed row. That node precedes every fingerprinted producer
       (see :data:`COMPUTED_KEYS`), so it recomputes all of them.
    2. Otherwise exactly one artifact is stale (only its own declared keys
       moved, or it has no fingerprint): ``--from-node`` its producer.
    3. Otherwise no single node is known to cover them all: the full pipeline.
    """
    current_by_extra: dict[tuple[str, ...], dict] = {}
    problems: list[str] = []
    stale_producers: list[str] = []
    first_computed: int | None = None

    for artifact in artifacts:
        extra = tuple(artifact.extra_keys)
        if extra not in current_by_extra:
            current_by_extra[extra] = fingerprint(parameters, extra)
        current = current_by_extra[extra]

        stored = artifact.payload.get("config_fingerprint") \
            if isinstance(artifact.payload, dict) else None
        if not (isinstance(stored, dict) and "sha256" in stored
                and isinstance(stored.get("values"), dict)):
            problems.append(
                f"  - {artifact.catalog_name} (written by "
                f"{artifact.produced_by}) has no config_fingerprint: written "
                "before fingerprints existed, or not by this pipeline."
            )
            stale_producers.append(artifact.produced_by)
            continue
        if artifact.compared_keys is None:
            if stored["sha256"] == current["sha256"]:
                continue
            old_values, new_values = stored["values"], current["values"]
        else:
            # The stored sha256 covers every row, so only values can be
            # compared on a subset.
            keys = set(artifact.compared_keys)
            old_values = {p: v for p, v in stored["values"].items() if p in keys}
            new_values = {p: v for p, v in current["values"].items() if p in keys}
            if _canonical(old_values) == _canonical(new_values):
                continue
        paths = [*new_values, *(p for p in old_values if p not in new_values)]
        # _KEY_ORDER only covers COMPUTED_KEYS; an extra key (not in it) ties
        # at len(_KEY_ORDER) and keeps its relative position via sort
        # stability — `paths` is already in COMPUTED_KEYS-then-extra_keys
        # order because that is the order `fingerprint()` walks to build
        # `values`. A dict .get() here (O(1)) replaces the previous
        # `order.index(p)` (O(n) per comparison inside the sort).
        paths = sorted(paths, key=lambda p: _KEY_ORDER.get(p, len(_KEY_ORDER)))
        lines = []
        for path in paths:
            leaves = _changed_leaves(path, old_values.get(path, _ABSENT),
                                     new_values.get(path, _ABSENT))
            if leaves and path in _KEY_ORDER:
                idx = _KEY_ORDER[path]
                first_computed = idx if first_computed is None \
                    else min(first_computed, idx)
            lines.extend(
                f"      {leaf}: {_short(old)} -> {_short(new)}"
                for leaf, old, new in leaves
            )
        if not lines:
            lines.append("      (hash differs but no value does: the "
                         "fingerprint was written by another hashing version)")
        problems.append(
            f"  - {artifact.catalog_name} (written by {artifact.produced_by}) "
            "was computed with different settings:\n" + "\n".join(lines)
        )
        stale_producers.append(artifact.produced_by)

    if not problems:
        return

    if first_computed is not None:
        advice = (f"Re-run with --from-node {COMPUTED_KEYS[first_computed][1]}, "
                  "which recomputes every artifact above.")
    elif len(stale_producers) == 1:
        advice = f"Re-run with --from-node {stale_producers[0]}."
    else:
        advice = ("Re-run the full pipeline (no slicing flags): no single "
                  "node is known to recompute all of the artifacts above.")
    raise ValueError(
        "Evaluation artifacts on disk do not match the current computed "
        "settings (evaluation.config_fingerprint.COMPUTED_KEYS); drawing a "
        "report from them would mix old and new settings:\n"
        + "\n".join(problems) + "\n" + advice
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
    but hash differently, and a diff must never miss what the hash saw."""
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
