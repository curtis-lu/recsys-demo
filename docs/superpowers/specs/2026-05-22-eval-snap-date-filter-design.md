# Evaluation `snap_date` Filtering — Design

**Date:** 2026-05-22
**Status:** Approved

## Problem

`evaluation.snap_date` in `conf/base/parameters_evaluation.yaml` is documented as
"the snap date for evaluation", but the evaluation pipeline never uses it to
scope the data. `prepare_eval_data` (`pipelines/evaluation/nodes_spark.py`)
reads the entire `ranked_predictions` Hive table, filters only by
`model_version`, then derives the label join key from *whatever* snap_dates
happen to be present in those predictions. A `ranked_predictions` table that
accumulates multiple weekly inference runs is therefore evaluated in bulk,
mixing snapshots.

The original intent of `evaluation.snap_date` was to pick **one** inference
snapshot to evaluate. This design makes the pipeline honour that intent.

Today `evaluation.snap_date` has only two real effects, neither of which
filters data:
- Output path label — `data/evaluation/{model_version}/{snap_date}/report.html`
  (`__main__.py:613`, normalised via `.replace("-", "")`).
- Report metadata header "Snap Date" (`report_builder.py:444`).

## Scope

In scope:
- `prepare_eval_data` filters the predictions DataFrame to the single
  configured `evaluation.snap_date`.
- Applies to **both** pipeline modes (monitoring → `ranked_predictions`,
  `--post-training` → `training_eval_predictions`). They share this one node,
  so a single filter covers both.
- Fail-loud error handling when the date is unset or matches no rows.

Out of scope:
- Multi-date / date-range evaluation — `evaluation.snap_date` stays a single
  scalar.
- Catalog-level partition pruning — see Approach B in "Alternatives".
- The `baselines` pipeline's separate `snap_date` string-comparison fragility
  (tracked elsewhere; this design does not touch `baselines/nodes_spark.py`).

## Prerequisite

`evaluation.snap_date` must be an ISO date string (`YYYY-MM-DD`, e.g.
`"2025-12-31"`). The `snap_date` partition column on both `ranked_predictions`
and `training_eval_predictions` is declared `STRING` in `catalog.yaml`; the
filter compares `F.col(time_col).cast("string")` against the config value, so
the config value must be the canonical `YYYY-MM-DD` form. The yaml value and
its comment are updated as part of this change.

## Design

### Component

Single node modified: `prepare_eval_data` in
`src/recsys_tfb/pipelines/evaluation/nodes_spark.py`. No new files, no pipeline
wiring change, no catalog change.

### Data flow

The new filter is inserted **after** the existing `model_version` filter
(`nodes_spark.py:58`) and **before** the `pred_snap_dates` derivation
(`nodes_spark.py:61`):

1. Read `snap_date` from `eval_params`. If absent/empty, raise `ValueError`
   ("evaluation.snap_date not configured").
2. `predictions_at_snap = ranked_predictions.filter(`
   `F.col(time_col).cast("string") == snap_date)`.
   `.cast("string")` mirrors the defensive comparison in
   `baselines/nodes_spark.py`: a no-op for the `STRING` partition column,
   still correct if the column is ever `DATE`.
3. If `predictions_at_snap.isEmpty()` (Spark 3.3 `DataFrame.isEmpty()`):
   collect the distinct `time_col` values from the pre-filter (post
   `model_version`) DataFrame and raise `ValueError` naming both the requested
   `snap_date` and the snap_dates actually present.
4. Otherwise continue with `ranked_predictions = predictions_at_snap`.

`pred_snap_dates` (`nodes_spark.py:61`) then naturally collapses to the single
configured date and the label join scopes with it — no downstream change.

### Error handling

| Condition | Behaviour |
|---|---|
| `evaluation.snap_date` unset/empty | `ValueError`: "evaluation.snap_date not configured" |
| Configured but no matching rows | `ValueError`: requested date + list of available snap_dates |

Both fail loud — never silently produce an empty report, and (unlike the
`baselines` pipeline) never fall back to the full table.

### Caveat — `--post-training` mode

In `--post-training` mode the predictions source is
`training_eval_predictions`, the training run's test-set predictions. The
snap_date filter applies there too. When running `--post-training`,
`evaluation.snap_date` must be set to a snap_date the training test split
actually covers, otherwise the filter empties the DataFrame and step 3 raises.
This is intended fail-loud behaviour; it will be noted in the
`parameters_evaluation.yaml` comment.

## Alternatives considered

- **Approach B — catalog `partition_filter`.** `snap_date` is a partition
  column, so adding it to `partition_filter` would prune partitions at read
  time. Rejected: the `__main__.py` template global `snap_date` is already
  `.replace("-", "")`-normalised to `YYYYMMDD`, which mismatches the
  `YYYY-MM-DD` partition value; it would need a second un-normalised template
  global, and both `ranked_predictions` and `training_eval_predictions`
  catalog entries would need editing. Catalyst can still push the in-node
  equality filter down to partition pruning, so Approach A loses little.
- **Approach C — filter in `__main__.py`.** Not possible: predictions are a
  catalog dataset, not materialised in the CLI entry function.

## Testing

New Spark tests in `tests/test_evaluation/` (using the existing conftest
`spark` fixture):

1. **Scoping** — build a `ranked_predictions` DataFrame spanning two
   snap_dates; set `evaluation.snap_date` to one of them; assert the
   `prepare_eval_data` output contains only that snap_date.
2. **Empty match raises** — set `evaluation.snap_date` to a date absent from
   the predictions; assert `prepare_eval_data` raises `ValueError` and the
   message lists the available snap_dates.
3. **Unset raises** — omit `evaluation.snap_date`; assert `prepare_eval_data`
   raises `ValueError`.
