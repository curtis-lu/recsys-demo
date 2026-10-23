"""The comparison layer (ADR-0031 decisions 4 and 5): two metrics results —
``metrics_spark.compute_all_metrics`` bundles — joined by name and grain, each
cell holding both values and their difference, plus what the two were
computed on. No Spark: it reads two landed dicts and returns one.

Why one layer: the differences used to be taken in several places — the main
report's baseline section asked ``compare.build_comparison_result`` for the
overall and per-item ones and took the per-segment and category ones itself,
``per_item_metric_compare_table`` took the Macro row's and the ``"all"``
pairing's, the comparison node took its own. Each took "A − B" its own way;
here it is taken once and lands, and a report only reads it (principle 1).

Every K the results hold is compared — every key of ``evaluation.k_values``'
grid — none cut for display: the K a table shows is cut after this layer
(``steps/display_ks``), so changing it re-runs that cut only (decision 16).

Shape (``this`` is the result compared, ``reference`` the one it is compared
against; the key constants are in ``evaluation.result_readers``)::

    {
      # the item grain, then the same under "category":
      "overall":       BLOCK,   # cells keyed by metric key
      "per_item":      BLOCK,   # cells keyed by item, then by metric key
      "macro_by_item": BLOCK,   # macro_avg.by_item; cells keyed by metric key
      "per_segment":   BLOCK,   # cells keyed by segment, then by metric key
      "population": {           # the four annotations (decision 15)
          "all_k" | "macro_items" | "scored_queries" | "segment_columns":
              {"this": v, "reference": v, "same": bool},
      },
      "category": {same as above, without "category"} | None,
    }
    BLOCK = {"entries": {"this": int | None, "reference": int | None},
             "cells": {key: CELL, ...}}
    CELL  = {"this": value | None, "reference": value | None,
             "diff": this - reference | None}

* **Cells pair keys by name**, in ``this``' order and then the keys only
  ``reference`` has. ``diff`` is ``this − reference`` only where both sides
  hold a number; a key one side lacks gets ``None`` there and no ``diff``
  (ADR-0020 bug 4: reading the missing side as 0 printed the other side's
  value as the difference, and a 0 reads as "the control scored 0").
* **``name@all``** pairs ``this``' ``name@{its all_k}`` with ``reference``'s
  ``name@{its all_k}``: the two ``"all"`` can resolve to different K (with
  ``event`` rows, the widest query group, #434), and then no key pairs them
  by name. The int keys still pair by name, so ``map@12`` beside ``map@all``
  is "truncated at 12 on both sides" even where 12 is one side's ``all``.
  A table reads ``name@{label}`` for every label it shows, ``"all"``
  included, and never resolves ``"all"`` itself.
* **``entries``** is how many entries each side's block held — keys of
  ``overall`` / ``macro_by_item``, items of ``per_item``, segments of
  ``per_segment`` — and ``None`` where that side has no such block (a
  ``macro_avg`` without ``by_item``, the empty bundle's). A report decides
  from it which tables and rows to draw; the cells alone cannot say whether
  a blank side was empty or absent.
* **``population``** records each side's ledger (``metrics.POPULATION_KEY``)
  for the four cases decision 4 names where a join by name is silently
  wrong: ``"all"`` resolved to different K, different items in the macro,
  different query groups scored, segments on one side only. ``same`` says
  whether the two agree; a report marks the ones that do not. The
  popularity baseline agrees on all four by construction (it re-scores the
  model's rows, counted off their row facts); they land all the same.
* ``category`` is compared only when both sides have that grain.

Not compared: ``per_item_segment``, ``macro_avg.by_segment`` /
``by_item_segment`` and the shared numbers — no report draws a difference of
them. Add a block here when one does.
"""

from __future__ import annotations

from typing import Any, Optional

from recsys_tfb.evaluation.result_readers import (
    ALL_K_KEY,
    ALL_LABEL,
    CELLS,
    DIFF,
    ENTRIES,
    MACRO_ITEMS_KEY,
    N_SCORED_WITH_POSITIVE_KEY,
    N_SCORED_WITHOUT_POSITIVE_KEY,
    POPULATION_KEY,
    REFERENCE,
    SAME,
    SCORED_QUERIES_KEY,
    SEGMENT_COLUMNS_KEY,
    THIS,
)

#: The population ledger keys this layer reads off each side, at each grain.
LEDGER_KEYS = (ALL_K_KEY, MACRO_ITEMS_KEY, N_SCORED_WITH_POSITIVE_KEY,
               N_SCORED_WITHOUT_POSITIVE_KEY, SEGMENT_COLUMNS_KEY)


def ledger_keys_missing(bundle: dict) -> list[str]:
    """The ledger keys (:data:`LEDGER_KEYS`) ``bundle`` lacks, at each grain
    it holds, as ``"<grain>: <key>"``; empty when it carries them all.

    For the calling node's pre-check: a bundle landed before a key existed
    passes the fingerprint check (no setting changed), and comparing it would
    stop on a bare ``KeyError`` deep in here.
    """
    missing = []
    grains = [("item grain", bundle)]
    if bundle.get("category"):
        grains.append(("category grain", bundle["category"]))
    for grain, grain_bundle in grains:
        ledger = grain_bundle.get(POPULATION_KEY)
        ledger = ledger if isinstance(ledger, dict) else {}
        missing += [f"{grain}: {POPULATION_KEY}.{key}"
                    for key in LEDGER_KEYS if key not in ledger]
    return missing


def compare_metric_results(this: dict, reference: dict) -> dict:
    """``this`` against ``reference``, both ``compute_all_metrics`` bundles
    carrying the full ledger (:func:`ledger_keys_missing` empty): the shape
    in the module docstring.

    Which results the two are, and whether the comparison is drawn at all,
    are the caller's: this function joins whatever it is given.
    """
    out = _compare_grain(this, reference)
    cat_this, cat_reference = this.get("category"), reference.get("category")
    out["category"] = (_compare_grain(cat_this, cat_reference)
                       if cat_this and cat_reference else None)
    return out


def _compare_grain(this: dict, reference: dict) -> dict:
    all_this = this[POPULATION_KEY][ALL_K_KEY]
    all_reference = reference[POPULATION_KEY][ALL_K_KEY]

    def block(read, nested: bool) -> dict:
        return _block(read(this), read(reference), all_this, all_reference,
                      nested=nested)

    return {
        "overall": block(lambda b: b.get("overall"), nested=False),
        "per_item": block(lambda b: b.get("per_item"), nested=True),
        "macro_by_item": block(
            lambda b: (b.get("macro_avg") or {}).get("by_item"), nested=False),
        "per_segment": block(lambda b: b.get("per_segment"), nested=True),
        POPULATION_KEY: _annotations(this[POPULATION_KEY],
                                     reference[POPULATION_KEY]),
    }


def _block(a: Optional[dict], b: Optional[dict], all_a: int, all_b: int,
           *, nested: bool) -> dict:
    entries = {THIS: None if a is None else len(a),
               REFERENCE: None if b is None else len(b)}
    a, b = a or {}, b or {}
    if nested:
        cells = {name: _cells(a.get(name) or {}, b.get(name) or {},
                              all_a, all_b)
                 for name in _in_order(a, b)}
    else:
        cells = _cells(a, b, all_a, all_b)
    return {ENTRIES: entries, CELLS: cells}


def _cells(a: dict, b: dict, all_a: int, all_b: int) -> dict:
    cells = {key: _cell(a.get(key), b.get(key)) for key in _in_order(a, b)}
    for name in _in_order(_names_at(a, all_a), _names_at(b, all_b)):
        cells[f"{name}@{ALL_LABEL}"] = _cell(a.get(f"{name}@{all_a}"),
                                             b.get(f"{name}@{all_b}"))
    return cells


def _names_at(metrics: dict, k: int) -> dict:
    """The metric names keyed at ``@{k}`` in ``metrics``, in key order (a
    dict, for :func:`_in_order`)."""
    names = {}
    for key in metrics:
        name, sep, suffix = str(key).rpartition("@")
        if sep and suffix.isdigit() and int(suffix) == k:
            names[name] = None
    return names


def _in_order(a: dict, b: dict) -> list:
    """``a``'s keys, then the ones only ``b`` has."""
    return list(dict.fromkeys([*a, *b]))


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _cell(this: Any, reference: Any) -> dict:
    return {THIS: this, REFERENCE: reference,
            DIFF: (this - reference
                   if _is_number(this) and _is_number(reference) else None)}


def _annotations(this: dict, reference: dict) -> dict:
    def scored(ledger: dict) -> dict:
        return {N_SCORED_WITH_POSITIVE_KEY: ledger[N_SCORED_WITH_POSITIVE_KEY],
                N_SCORED_WITHOUT_POSITIVE_KEY:
                    ledger[N_SCORED_WITHOUT_POSITIVE_KEY]}

    out = {}
    for name, read in ((ALL_K_KEY, lambda p: p[ALL_K_KEY]),
                       (MACRO_ITEMS_KEY, lambda p: p[MACRO_ITEMS_KEY]),
                       (SCORED_QUERIES_KEY, scored),
                       (SEGMENT_COLUMNS_KEY, lambda p: p[SEGMENT_COLUMNS_KEY])):
        a, b = read(this), read(reference)
        out[name] = {THIS: a, REFERENCE: b, SAME: a == b}
    return out
