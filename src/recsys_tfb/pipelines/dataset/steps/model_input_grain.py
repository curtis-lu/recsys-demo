"""What the B10 model-input grain gate reports.

The rule lives in ``core/consistency.py`` (``model_input_grain_errors`` and the
scope check before it, ``model_input_grain_scope_errors``); the pairing — which
keys table goes with which model_input, under which scope, over which months —
in ``validate_model_input_grain``. The row counts come from
``steps/footer_facts.py``, shared with B8. What is left is this module: the
report's shape, one entry per split, each logged as it is made.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

import pandas as pd

from recsys_tfb.core.consistency import SplitRowCounts

logger = logging.getLogger(__name__)

#: Why a run with no test month has no test pair. Written into the report and
#: the log, so "was test checked" has an answer in the artifact itself.
TEST_NOT_WRITTEN = (
    "not written this run: test_model_input_month_plan.to_process is empty, "
    "so this run built no test month and there is no test pair to compare. "
    "Not a failure — the months already on disk were written by earlier runs."
)


def grain_report_entry(
    label: str,
    keys: tuple[int, int],
    model_input: tuple[int, int],
) -> dict:
    """One split's entry in the B10 report, ``(rows, files)`` for each side,
    logged as it is made — one line and one set of fields for every split."""
    logger.info(
        "Model input grain gate: %s keys=%d row(s) in %d file(s), "
        "model_input=%d row(s) in %d file(s)",
        label, *keys, *model_input,
    )
    return {
        "keys_rows": keys[0], "keys_files": keys[1],
        "model_input_rows": model_input[0], "model_input_files": model_input[1],
    }


def split_row_counts_by_month(
    keys: Mapping[pd.Timestamp, tuple[int, int]],
    model_input: Mapping[pd.Timestamp, tuple[int, int]],
) -> dict[str, SplitRowCounts]:
    """``{"YYYY-MM-DD": SplitRowCounts}`` — each month its own pair, the shape
    ``model_input_grain_errors`` takes for a split written a month at a time.

    Both mappings are ``footer_rows_by_month``'s, over the same months.
    """
    return {
        month.strftime("%Y-%m-%d"): SplitRowCounts(
            keys[month][0], model_input[month][0])
        for month in sorted(keys)
    }


def monthly_grain_report_entry(
    split: str,
    keys: Mapping[pd.Timestamp, tuple[int, int]],
    model_input: Mapping[pd.Timestamp, tuple[int, int]],
) -> dict:
    """The entry of a split written a month at a time: summed over the months,
    so it reads like every other split's, plus each month's pair under
    ``months`` — the pairs the gate actually compared."""
    months = sorted(keys)
    entry = grain_report_entry(
        f"{split} ({len(months)} month(s) this run)",
        _summed(keys), _summed(model_input),
    )
    entry["months"] = {
        month: {
            "keys_rows": counts.keys_rows,
            "model_input_rows": counts.model_input_rows,
        }
        for month, counts in split_row_counts_by_month(keys, model_input).items()
    }
    return entry


def grain_report(
    *,
    base_version: str,
    train_variant_id: str,
    splits: dict[str, dict],
    not_checked: dict[str, str],
) -> dict:
    """The persisted report (catalog entry ``model_input_grain_report``)."""
    return {
        # The version and variant this report describes. The catalog keys its
        # file on base_dataset_version alone (as numeric_precision_report does),
        # so two runs of different train variants overwrite one file; carrying
        # the variant inside is what stops a reader attributing one variant's
        # counts to another.
        "base_dataset_version": base_version,
        "train_variant_id": train_variant_id,
        "splits": splits,
        # Named in the artifact, not only in the log: a reader who pulls the
        # report to ask "was my dataset checked" must not have to infer from
        # an absent key that a split was left out, or why.
        "not_checked": not_checked,
    }


def _summed(by_month: Mapping[pd.Timestamp, tuple[int, int]]) -> tuple[int, int]:
    return tuple(map(sum, zip(*by_month.values())))
