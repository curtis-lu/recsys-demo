"""Which months of the test prediction table are scored, and which are left.

``training_eval_predictions`` holds every month ever predicted for a
``model_version``: its catalog entry prunes by ``model_version`` only, and
``dataset.test_snap_dates`` is in no version ID (ADR-0001), so months pile up
across config edits. Scoring whatever the table holds let a version's score
change with the table, and let two versions be ranked on different months
with nothing saying so (ADR-0028 background three). ``compute_test_metrics``
now scores the scored months alone (``core/consistency.py::
scoring_snap_dates``); these are the two mechanisms that takes.

**The decisions are not here.** Which months, what a missing month means and
what the log says are written at their call sites in ``compute_test_metrics``
(``docs/agents/pipeline-node-design.md`` rules 4 and 9).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional
from urllib.parse import unquote

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def restrict_to_scored_months(
    frame: DataFrame, time_col: str, months: list[str],
) -> DataFrame:
    """``frame`` kept to the rows whose time value is one of ``months``.

    **Compared as text**, the comparison evaluation's ``--post-training``
    makes on the same table (``pipelines/evaluation/steps/snap_date_scope.py``,
    which cannot be imported from here, S3). The partition value is the test
    data's own time text (predict writes ``str()`` of the cached rows'
    column), so a month matches only when spelled as the data spells it. A53
    holds each scored month to ``test_snap_dates``' spelling, which the
    shipped data follows; a month the data spells otherwise matches no row,
    and ``compute_test_metrics`` then stops, naming the months the table
    holds. Not ``dataset/steps/scoping.restrict_to_months``, which compares
    calendar days (``to_date`` on both sides): a different rule under a
    similar name, and picking the wrong one raises nothing.

    ``cast("string")`` on a STRING partition column is a no-op, and one date
    compares with ``=``, several with ``IN``: the forms ``snap_date_scope``
    checked Spark 3.3.2 prunes partitions with.
    """
    column = F.col(time_col).cast("string")
    if len(months) == 1:
        return frame.filter(column == months[0])
    return frame.filter(column.isin(months))


def partition_months(frame: DataFrame, time_col: str) -> Optional[list[str]]:
    """The time values ``frame``'s files are partitioned under, sorted; ``None``
    when that cannot be told from its files.

    Read off ``DataFrame.inputFiles()``: the file listing Spark plans the scan
    with, already pruned to this ``model_version`` by the catalog's filter.
    No Spark job runs and no row is read (checked 2026-09-24 on Spark 3.3.2
    against the local Hive table: 0 jobs), which is the point — the months
    this lists are the ones the node must not read. ``SHOW PARTITIONS`` would
    need the table's name, and the node only gets the frame.

    ``None`` for a frame not read from files partitioned by ``time_col``: an
    in-memory frame, or a table whose partitions are not keyed by it (R7 of
    ``docs/agents/architecture-constraints.md`` has no check that they are).
    The directory key is matched case-insensitively, as
    ``HiveTableDataset.existing_partition_values`` reads the metastore's, and
    the value is URL-unescaped for the same reason given there.
    """
    prefix = f"{time_col}=".lower()
    found = {
        unquote(segment[len(prefix):])
        for path in frame.inputFiles()
        for segment in path.split("/")
        if segment.lower().startswith(prefix)
    }
    return sorted(found) if found else None
