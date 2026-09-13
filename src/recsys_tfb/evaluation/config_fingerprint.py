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
  ``evaluation.report.sections.*`` key except ``baseline`` and
  ``diagnostics``. They only change what the report shows, so
  ``--only-node generate_report`` is enough and takes seconds.

Only computed settings go into the fingerprint. Fingerprinting drawn ones too
would make every cosmetic change raise, which is the "re-run everything on any
change" the user rejected. ``sections.baseline`` and ``sections.diagnostics``
are computed, not drawn: ``baseline: false`` makes ``compute_baseline_metrics``
return a stub and ``diagnostics: false`` does the same for
``compute_report_aggregates``; treated as drawn, flipping them back and
re-drawing yields a report with a section silently missing.

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
``contract.inputs_for``. Importing nothing from the project also keeps this
usable from the pure ``generate_report`` path and from offline tools without
dragging Spark in. ``tests/test_evaluation/test_config_fingerprint.py`` scans
the imports.

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
from typing import Any, Iterable, Sequence

__all__ = [
    "COMPUTED_KEYS", "fingerprint", "require_computed_with_current_config",
]

#: Computed settings: ``(dotted path from the parameters root, node to re-run
#: from when it changes)``. Rows are ordered by that node's topological
#: position (non-decreasing), so the first changed row names the earliest node.
#:
#: The node is **the earlier, in topological order, of the key's first reader
#: and the first node that writes a fingerprinted artifact**
#: (``compute_baseline_metrics``), not simply the first reader. Every
#: fingerprinted artifact hashes every row here, so a change makes all of them
#: stale, and ``--from-node`` must re-run all of their producers. The
#: ``report.diagnostics`` rows are where the two differ: first read by
#: ``compute_report_aggregates``, which sorts after ``compute_baseline_metrics``
#: in both modes. Today the slice pulls the baseline back anyway
#: (``baseline_metrics`` is memory-only); once #339 lands it on disk, a
#: ``--from-node compute_report_aggregates`` would leave the baseline's old
#: fingerprint in place and the next report would raise with the same advice,
#: a loop with no exit.
#:
#: ``tests/test_pipelines/test_evaluation/test_pipeline.py::
#: TestFingerprintRerunNodes`` pins it in both modes: a row whose node sorts
#: after a fingerprinted producer turns that test red.
COMPUTED_KEYS: tuple[tuple[str, str], ...] = (
    ("evaluation.snap_date", "prepare_eval_data"),
    # Read first today by draw_diagnosis_sample_node (diagnosis/metric/
    # sample.py). Listed at prepare_eval_data anyway: eval_predictions is
    # memory-only, so a re-run from either node re-runs prepare_eval_data, and
    # ADR-0020 bug 6 moves the segment-column resolution into it.
    ("evaluation.segment_columns", "prepare_eval_data"),
    ("evaluation.segment_sources", "prepare_eval_data"),
    ("evaluation.diagnosis", "draw_diagnosis_sample_node"),
    ("evaluation.k_values", "compute_metrics"),
    ("evaluation.item_categories", "compute_metrics"),
    ("evaluation.metric", "compute_metrics"),
    ("evaluation.baseline", "compute_baseline_metrics"),
    ("evaluation.report.sections.baseline", "compute_baseline_metrics"),
    # First read by compute_report_aggregates; listed at the baseline, the
    # first fingerprinted producer, for the reason in the comment above.
    ("evaluation.report.diagnostics", "compute_baseline_metrics"),
    ("evaluation.report.sections.diagnostics", "compute_baseline_metrics"),
)

_RERUN_NODE = dict(COMPUTED_KEYS)
_KEY_ORDER = {path: i for i, (path, _) in enumerate(COMPUTED_KEYS)}
_ABSENT = object()
_MAX_REPR = 80


def fingerprint(parameters: dict, extra_keys: Sequence[str] = ()) -> dict:
    """``{"sha256": ..., "values": {path: value}}`` over the computed settings.

    ``values`` is kept next to the hash so a mismatch can say which key moved
    and from what, instead of only "something changed". Values are normalised
    through a JSON round trip (``default=str``) so that what a landed JSON
    holds after being read back compares equal to what is computed now, e.g. a
    YAML ``datetime.date`` against its string.
    """
    values: dict[str, Any] = {}
    for path in (*(p for p, _ in COMPUTED_KEYS), *extra_keys):
        found, value = _lookup(parameters, path)
        if found:
            values[path] = json.loads(json.dumps(
                value, sort_keys=True, ensure_ascii=False, default=str))
    digest = hashlib.sha256(json.dumps(
        values, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
    return {"sha256": digest, "values": values}


def require_computed_with_current_config(
    artifacts: Iterable[tuple[str, Any, str, Sequence[str]]],
    parameters: dict,
) -> None:
    """Pre-check (inputs): every artifact was computed with today's settings.

    ``artifacts`` holds ``(catalog name, payload, producing node, extra_keys)``.
    A pre-check, not a postcondition: a mismatch means the inputs on disk are
    older than the config, not that the calling node is wrong.

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

    for catalog_name, payload, producer, extra_keys in artifacts:
        extra = tuple(extra_keys)
        if extra not in current_by_extra:
            current_by_extra[extra] = fingerprint(parameters, extra)
        current = current_by_extra[extra]

        stored = payload.get("config_fingerprint") \
            if isinstance(payload, dict) else None
        if not (isinstance(stored, dict) and "sha256" in stored
                and isinstance(stored.get("values"), dict)):
            problems.append(
                f"  - {catalog_name} (written by {producer}) has no "
                "config_fingerprint: written before fingerprints existed, or "
                "not by this pipeline."
            )
            stale_producers.append(producer)
            continue
        if stored["sha256"] == current["sha256"]:
            continue

        old_values, new_values = stored["values"], current["values"]
        paths = [*new_values, *(p for p in old_values if p not in new_values)]
        order = [*(p for p, _ in COMPUTED_KEYS), *extra]
        paths = sorted(paths, key=lambda p: order.index(p)
                       if p in order else len(order))
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
            f"  - {catalog_name} (written by {producer}) was computed with "
            "different settings:\n" + "\n".join(lines)
        )
        stale_producers.append(producer)

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
    node: Any = parameters
    for seg in path.split("."):
        if not isinstance(node, dict) or seg not in node:
            return False, None
        node = node[seg]
    return True, node


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
    old_ok = isinstance(old, dict) or old is _ABSENT
    new_ok = isinstance(new, dict) or new is _ABSENT
    if (isinstance(old, dict) or isinstance(new, dict)) and old_ok and new_ok:
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
