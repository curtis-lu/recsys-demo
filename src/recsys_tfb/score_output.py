"""Writing a scored frame: the guards and the assembly both scoring pipelines use.

Training writes test predictions (``predict_and_write_test_predictions``) and
inference writes scores (``predict_and_write_scores``), and until #484 each
built its output frame its own way: inference checked every chunk before the
write and training checked nothing. This module holds the part of that work
that is the same on both sides and knows nothing about the model (ADR-0030
decision 5):

* one ``save()``, one partition (:func:`require_single_partition`);
* the per-chunk post-conditions — the row count, the nulls, the duplicates
  (:func:`scored_chunk_failures`, :func:`require_scored_chunk`);
* laying the output frame out (:class:`ScoredFrameLayout`).

**The decisions stay in each node.** The two pipelines ask the same questions
and answer several of them differently, so this module takes the answer as an
argument and never a flag (``docs/agents/pipeline-node-design.md`` rule 5):

===========================  ==========================  ==========================
question                     training                    inference
===========================  ==========================  ==========================
what identifies a row        ``identity_columns``, with  base key + item; optional
                             the optional roles — under  roles are ignored
                             ``occasion``/``event`` one  (ADR-0025 decision 1)
                             (time, entity, item) may
                             hold several rows
optional-role columns        carried, never stringified  not written
                             (``event`` may be a
                             timestamp)
item value-domain check      not needed: the item comes  needed: the item is a loop
                             from a data partition       variable
columns carried, partitions  label, zero-positive group  ``entity_bucket``;
                             weight; (time, item)        (time, item, bucket)
resume planning              by month, config is the     by chunk
                             authority
===========================  ==========================  ==========================

The duplicate check is the sharpest case: inference's identity handed to
training would report every legitimate multi-impression row of a deployment
that declares ``event``. So what is shared is "given these identity columns,
are there duplicates", and the identity is the caller's.

**Why this is not a module under ``pipelines/inference/``, imported by
training.** No pipeline imports another. The two are peers that change on
their own schedules, and training depending on inference's internals means an
inference refactor can break training. Shared code moves below both, which is
what ``preprocessing.py``, ``models/feature_selection.py`` and
``models/feature_view.py`` did before it (ADR-0008).

Imports pandas, numpy and the standard library only — no project module and no
pyspark — so its checks are tested in milliseconds (``tests/test_score_output.py``
pins the import list).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import pandas as pd

#: Deprecated, and equal to the score by construction: nothing rescales a
#: model's output any more (#411). It stays in the frame so the managed
#: prediction tables keep their column count, which a position-based write
#: depends on; #412 takes it away.
DEPRECATED_UNCALIBRATED_SCORE_COL = "score_uncalibrated"


def require_single_partition(pdf: pd.DataFrame, partition_cols: list[str]) -> None:
    """One ``save()``, one partition — constraint C in its testable form.

    Post-condition on the frame a scoring loop just built.

    ``HiveTableDataset.save()`` is ``insertInto`` under
    ``partitionOverwriteMode=dynamic``, whose semantics are "touch only the
    partitions present in this frame, but *replace* those wholesale". Hand it a
    frame spanning two chunks' partitions and the second save deletes the first
    chunk's rows — 90% of the data gone with no error message
    (ADR-0010 section 3, constraint C).

    Free to check here and nowhere else: the frame is a pandas frame that was
    just built in the driver, so counting its distinct partition values touches
    memory rather than launching a Spark job. The same assertion inside
    ``save()`` would have to act on a lazy plan, which is the second full
    lineage execution ADR-0009 removed.

    Deliberately **not** a before/after diff of
    ``existing_partition_values()``: re-publishing an existing partition makes
    that diff empty by construction, so it is blind to exactly the successive
    overwrite this guards (``io/hive_table_dataset.py`` records the same trap).
    """
    combos = pdf[partition_cols].drop_duplicates()
    if len(combos) != 1:
        raise ValueError(
            f"a single save must cover exactly one partition, got "
            f"{len(combos)} distinct {partition_cols} combination(s): "
            f"{combos.to_dict('records')}. Dynamic-partition overwrite "
            "replaces whole partitions, so successive saves would delete each "
            "other's rows without an error."
        )


def scored_chunk_failures(
    out_pdf: pd.DataFrame,
    source_pdf: pd.DataFrame,
    *,
    entity_cols: Sequence[str],
    identity_cols: Sequence[str],
    not_null_cols: Sequence[str],
) -> list[dict]:
    """The per-chunk post-conditions that hold for any scored frame.

    Returns zero to three ``{"check", "detail"}`` dicts, in the order
    ``chunk_row_count``, ``no_missing``, ``no_duplicates``; the caller adds its
    own checks and raises. Pure pandas over two frames already in the driver,
    so the cost is invisible next to the prediction that produced ``out_pdf``.

    ``chunk_row_count``: the frame about to be saved has one row per row it was
    scored from. It is a guard on the frame's construction staying
    row-preserving rather than a check on the data — pandas already refuses
    arrays of different lengths when the frame is built.

    ``no_missing`` reads the entity columns off ``source_pdf`` and
    ``not_null_cols`` off ``out_pdf``. The source is not a convenience: the
    entity reaches the output through ``astype(str)``, which turns a null into
    the *string* ``"None"``, so a null check on the output's entity can never
    fire — the decorative-check shape ADR-0011 exists to remove.
    ``not_null_cols`` are the output columns the caller itself fills (the
    score, the partition values); a column it only carries from the source is
    the source's contract, not this check's.

    ``no_duplicates`` looks for repeated ``identity_cols`` in the output. The
    identity is the caller's decision (see the module docstring): the same rows
    are duplicates under one pipeline's identity and legitimate under the
    other's.
    """
    failures: list[dict] = []

    n_in, n_out = len(source_pdf), len(out_pdf)
    if n_out != n_in:
        failures.append({
            "check": "chunk_row_count",
            "detail": f"scored {n_out} rows from {n_in} entities",
        })

    null_counts: dict[str, int] = {}
    for col in entity_cols:
        n_null = int(source_pdf[col].isna().sum())
        if n_null:
            null_counts[col] = n_null
    for col in not_null_cols:
        n_null = int(pd.isna(out_pdf[col]).sum())
        if n_null:
            null_counts[col] = n_null
    if null_counts:
        failures.append({
            "check": "no_missing",
            "detail": f"NaN values found: {null_counts}",
        })

    n_dupes = int(out_pdf.duplicated(subset=identity_cols).sum())
    if n_dupes:
        failures.append({
            "check": "no_duplicates",
            "detail": f"{n_dupes} duplicate rows on {identity_cols}",
        })

    return failures


class ScoredChunkError(Exception):
    """A scored chunk failed its post-conditions; ``failures`` names each one."""

    def __init__(self, failures: list[dict]):
        self.failures = failures
        msg = (
            f"{len(failures)} sanity check(s) failed: "
            + ", ".join(f["check"] for f in failures)
        )
        super().__init__(msg)


def require_scored_chunk(
    out_pdf: pd.DataFrame,
    source_pdf: pd.DataFrame,
    *,
    entity_cols: Sequence[str],
    identity_cols: Sequence[str],
    not_null_cols: Sequence[str],
) -> None:
    """Raise :class:`ScoredChunkError` unless :func:`scored_chunk_failures` is empty.

    Post-condition, for a caller with no checks of its own to add. One that has
    some (inference's item value domain) calls :func:`scored_chunk_failures`
    and raises its own error with every failure in it.
    """
    failures = scored_chunk_failures(
        out_pdf, source_pdf, entity_cols=entity_cols,
        identity_cols=identity_cols, not_null_cols=not_null_cols,
    )
    if failures:
        raise ScoredChunkError(failures)


def _unique(columns) -> list[str]:
    seen: list[str] = []
    for col in columns:
        if col not in seen:
            seen.append(col)
    return seen


@dataclass(frozen=True)
class ScoredFrameLayout:
    """Which columns a scored frame holds, and how each one is filled.

    ``entity_cols`` are written through ``astype(str)``: the ranking side
    compares the entity as a string, and every entity column is written, not
    just the first — a scored row's identity is the whole tuple.

    ``score_col`` is the schema's name for the score (``schema.score``), never
    a literal: a deployment that renames it would otherwise get a table whose
    score sits in an undeclared column.

    ``carried_cols`` are copied from the source **as they are**, not
    stringified: ``event`` may be a timestamp, and the tie-break compares it by
    its own type (``utils/ranking.py``), so ``"10" < "2"`` would reorder ranks.

    ``null_cols`` are columns the write target declares and this run has no
    value for. They are written NULL on every row, because a Hive save selects
    every declared column and a frame without one fails there.

    Column order in the frame does not reach the table —
    ``HiveTableDataset.save`` selects the declared columns in the table's own
    order — but it is fixed anyway (entity, score, the deprecated score,
    carried, null, partitions) so a frame reads the same every run.
    """

    entity_cols: tuple[str, ...]
    score_col: str
    carried_cols: tuple[str, ...] = ()
    null_cols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Tuples, so a layout built from config lists is still hashable and
        # cannot be edited through the list it was built from.
        for name in ("entity_cols", "carried_cols", "null_cols"):
            object.__setattr__(self, name, tuple(getattr(self, name)))

    def source_columns(self) -> list[str]:
        """The source columns :meth:`build` reads: entity, then carried, once each."""
        return _unique(self.entity_cols + self.carried_cols)

    def build(
        self,
        source: pd.DataFrame,
        scores: np.ndarray,
        partition: Mapping[str, object],
    ) -> pd.DataFrame:
        """The frame to hand to ``save()``: one row per ``source`` row.

        ``partition`` maps each partition column to this chunk's value, written
        as a constant. Values are taken positionally (``.values``), so a
        filtered source keeps its rows aligned with ``scores`` whatever its
        index says.
        """
        n_rows = len(source)
        return pd.DataFrame({
            **{c: source[c].astype(str).values for c in self.entity_cols},
            self.score_col: scores,
            DEPRECATED_UNCALIBRATED_SCORE_COL: scores,
            **{c: source[c].values for c in self.carried_cols},
            **{c: [None] * n_rows for c in self.null_cols},
            **dict(partition),
        })
