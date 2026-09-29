"""Which (month, item) partitions the driver-local test cache holds.

Read off the partition directory names — ``<root>/<time>=.../<item>=.../`` —
and never off the rows. The replaced listing projected the two partition
columns and de-duplicated them, and pyarrow fills a projected partition column
once per *data row*: about 220 million rows at production scale to learn a few
hundred pairs (ADR-0030 decision 12.2). Every fragment of a hive-partitioned
dataset already carries its partition values as an expression, so the answer
costs a directory walk.

Kept out of ``predict_months.py``: that module imports nothing from the
project and no pyarrow, and its purity is pinned by an AST test. This one has
to touch pyarrow's dataset API.

**What the directory names cannot say** is whether a partition holds any rows:
a directory whose only data file is empty is listed. The cache does not decide
which directories exist: ``populate_cache_from_hive`` (``steps/local_cache.py``)
copies the Hive table's partition directories byte for byte
(``utils.hdfs.copy_hdfs_to_local``, Hadoop's ``copyToLocalFile``), so the
cache holds whatever the table's writer left. That writer is dataset's
``HiveTableDataset.save``, a dynamic-partition ``insertInto``, which creates a
partition directory only for a value some row has — so on a table this
pipeline wrote, the two answers agree. A row-less partition directory put
there some other way is listed, reads back no rows, and stops the node before
that partition is written (``require_single_partition`` finds no partition
value in an empty frame): a loud failure, not a skipped partition.

**A Hive NULL partition is refused.** A row whose time or item is NULL is
written under ``__HIVE_DEFAULT_PARTITION__``, which pyarrow reads back as
``None``. The replaced row read listed it as the string ``'None'``, whose
filter matched no row, so those rows were dropped without a word and the
month never counted as written. Here it raises, naming the directory.
"""

from __future__ import annotations

import pyarrow.dataset as pads


def partitions_from_directory_names(
    dataset, time_col: str, item_col: str,
) -> list[tuple]:
    """Every ``(time value, item value)`` pair, once, sorted.

    ``dataset`` is what ``io.handles.open_parquet_dataset`` returns — one
    root's dataset or the union of several, one per cached month. The values
    have the types hive partitioning inferred for them, the same values a row
    read of those columns would have returned; callers that need strings
    convert.

    Raises ``ValueError`` on a fragment that is not partitioned by both
    columns. A flat file carries them as data instead, and a directory-name
    listing of it would come back empty — predict would then write nothing and
    call the month done.

    Raises ``ValueError`` on a fragment in a Hive NULL partition (see the
    module docstring): there is no value to score those rows under.
    """
    pairs = set()
    for fragment in dataset.get_fragments():
        keys = pads.get_partition_keys(fragment.partition_expression)
        if time_col not in keys or item_col not in keys:
            raise ValueError(
                f"test cache file {fragment.path} is not partitioned by "
                f"({time_col!r}, {item_col!r}); its partition values are "
                f"{keys}. The cache is written by populate_cache_from_hive as "
                f"<root>/{time_col}=.../{item_col}=.../*.parquet — rebuild it "
                "with --rebuild-dates for that month."
            )
        null_keys = [c for c in (time_col, item_col) if keys[c] is None]
        if null_keys:
            raise ValueError(
                f"test cache file {fragment.path} is in a Hive NULL partition: "
                f"({time_col}={keys[time_col]!r}, {item_col}={keys[item_col]!r}), "
                f"the directory named __HIVE_DEFAULT_PARTITION__ for "
                f"{' and '.join(null_keys)}. Those are test_model_input rows "
                f"whose {' and '.join(null_keys)} is NULL, and there is no "
                "value to score them under. Look for them in dataset's "
                "test_model_input — dataset is what wrote them."
            )
        pairs.add((keys[time_col], keys[item_col]))
    return sorted(pairs)
