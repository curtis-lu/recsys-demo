"""Tests for predict_and_write_test_predictions — batched per-partition predict+write."""

from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pandas as pd


def _make_test_parquet(tmp_path: Path) -> Path:
    """Build a small partitioned parquet at ``tmp_path/test.parquet``.

    Layout: snap_date=*/prod_name=*/*.parquet (Hive-style, matches what the
    dataset pipeline produces after this PR's catalog change).

    test_model_input is pre-filtered upstream by the dataset pipeline's
    filter_test_keys node — every (snap_date, cust_id) group present
    here has at least one positive label across some prod_name.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        # snap=2025-01-31: c1 positive on prod_A, c2 positive on prod_B
        ("c1", "2025-01-31", "prod_A", 1.0, 1),
        ("c1", "2025-01-31", "prod_B", 1.1, 0),
        ("c2", "2025-01-31", "prod_A", 2.0, 0),
        ("c2", "2025-01-31", "prod_B", 2.1, 1),
        # snap=2025-02-28: c4 positive on prod_A
        ("c4", "2025-02-28", "prod_A", 4.0, 1),
        ("c4", "2025-02-28", "prod_B", 4.1, 0),
    ]
    df = pd.DataFrame(rows, columns=["cust_id", "snap_date", "prod_name", "feat_a", "label"])
    table = pa.Table.from_pandas(df, preserve_index=False)
    root = tmp_path / "test.parquet"
    pq.write_to_dataset(
        table, root_path=str(root), partition_cols=["snap_date", "prod_name"]
    )
    return root


def _make_prep_meta() -> dict:
    return {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }


def _mock_model(predict, feature_names=("feat_a",)) -> MagicMock:
    """A mock adapter whose scoring entry is the real default.

    The node scores through ``ModelAdapter.score`` and reads the columns
    ``ModelAdapter.scoring_columns`` names. A bare ``MagicMock`` would answer
    both with a ``MagicMock``, so the node would never slice a feature: both
    are wired to the ABC's own implementations, which call this mock's
    ``predict`` and ``feature_names`` — the two things each test scripts.
    """
    from functools import partial

    from recsys_tfb.models.base import ModelAdapter

    model = MagicMock()
    model.predict.side_effect = predict
    model.feature_names.return_value = (
        None if feature_names is None else list(feature_names))
    model.score.side_effect = partial(ModelAdapter.score, model)
    model.scoring_columns.side_effect = partial(ModelAdapter.scoring_columns, model)
    return model


def _make_parameters() -> dict:
    return {
        "model_version": "v_test_001",
        "hive": {"db": "ml_recsys"},
        # The months the fixtures below hold. Not optional: predict takes
        # dataset.test_snap_dates as the authority on which months exist and
        # predicts nothing without it — there is deliberately no "fall back to
        # whatever the cache holds" path to lean on.
        "dataset": {"test_snap_dates": ["2025-01-31", "2025-02-28"]},
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
    }


def _write_ds(existing=()) -> MagicMock:
    """Mock of the ``training_eval_predictions`` catalog dataset.

    ``existing``: ``(snap_date, prod_name)`` pairs already written for this
    model_version — what ``HiveTableDataset.existing_partition_values()``
    reports. Supplying it explicitly is the point of the seam: predict decides
    what to skip from this list alone, so every test states the on-disk
    starting position rather than inheriting a default.
    """
    ds = MagicMock()
    ds.save.side_effect = lambda df: ds.saved.append(df)
    ds.saved = []
    ds.existing_partition_values.return_value = [
        {"snap_date": snap, "prod_name": prod} for snap, prod in existing
    ]
    return ds


def _saved_partitions(write_ds) -> set:
    return {
        (str(df["snap_date"].iloc[0]), str(df["prod_name"].iloc[0]))
        for df in write_ds.saved
    }


def test_predict_and_write_emits_one_save_per_partition(tmp_path):
    """One save() call per (snap_date, prod_name) partition; every row in
    the input parquet appears in some save (no row-level filtering at this
    layer — upstream filter_test_keys already dropped negative-only
    groups before this function runs).
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # Mock model: predict returns increasing scores
    model = _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5)
    model.__class__.__name__ = "LightGBMAdapter"

    # Mock HiveTableDataset handle — capture every save() call
    saves: list[pd.DataFrame] = []

    def capture_save(df):
        # Production code passes a pandas DataFrame to HiveTableDataset.save()
        # (the dataset's _to_spark converts internally); tests assert on it directly.
        saves.append(df)

    write_ds = MagicMock()
    write_ds.save.side_effect = capture_save
    # Nothing written yet, so every configured month is incomplete and gets
    # predicted — the pre-incremental behaviour this test was written against.
    write_ds.existing_partition_values.return_value = []

    manifest = predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    # Expect 4 partitions: (2025-01-31, prod_A), (2025-01-31, prod_B),
    #                     (2025-02-28, prod_A), (2025-02-28, prod_B)
    assert write_ds.save.call_count == 4

    all_written = pd.concat(saves, ignore_index=True)

    # 2025-01-31 has c1 and c2 (both customers carry one positive each)
    snap_jan = all_written[all_written["snap_date"] == "2025-01-31"]
    assert set(snap_jan["cust_id"]) == {"c1", "c2"}

    # 2025-02-28 has only c4
    snap_feb = all_written[all_written["snap_date"] == "2025-02-28"]
    assert set(snap_feb["cust_id"]) == {"c4"}

    # Every input row is written through (no row-level filtering here).
    assert len(all_written) == 6

    # Manifest reports the right shape
    assert set(manifest["snap_dates"]) == {"2025-01-31", "2025-02-28"}
    assert set(manifest["items"]) == {"prod_A", "prod_B"}
    assert manifest["model_version"] == "v_test_001"
    assert manifest["n_rows_written"] == len(all_written)


def test_predict_and_write_score_uncalibrated_equals_score(tmp_path):
    """``score_uncalibrated`` equals ``score`` row-for-row, always.

    The column is deprecated (#412) and kept only so the managed table keeps
    its column count. Nothing rescales a model's output any more (#411), so
    there is no longer a branch that could put a different number there — and
    the node must not reach for one, e.g. by calling a ``predict_uncalibrated``
    that a mock would happily answer.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    model = _mock_model(lambda X: np.array([0.42] * len(X)))
    model.__class__.__name__ = "LightGBMAdapter"

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.save.side_effect = lambda df: saves.append(df)
    write_ds.existing_partition_values.return_value = []  # nothing written yet

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    for df in saves:
        assert (df["score"] == df["score_uncalibrated"]).all()
        assert (df["score"] == 0.42).all()
    # The model is asked for one score per partition and nothing else.
    assert model.predict.call_count == len(saves)
    model.predict_uncalibrated.assert_not_called()


def test_predict_covers_every_month_when_given_a_per_month_mapping(tmp_path):
    """The cache node now hands predict ``{snap_date: ParquetHandle}`` — one
    root per month. Every other test in this module passes a bare handle (still
    supported), so without this one the mapping shape predict actually receives
    in production would never be exercised: a union that dropped a root, or
    partition filtering that broke across roots, would stay green.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import predict_and_write_test_predictions

    def _month_root(snap_date: str) -> str:
        df = pd.DataFrame(
            {
                "cust_id": ["c1", "c2"],
                "snap_date": [snap_date] * 2,
                "prod_name": ["prod_A", "prod_B"],
                "feat_a": [1.0, 2.0],
                "label": [1, 0],
            }
        )
        root = tmp_path / snap_date.replace("-", "") / "test_model_input.parquet"
        pq.write_to_dataset(
            pa.Table.from_pandas(df, preserve_index=False),
            root_path=str(root),
            partition_cols=["snap_date", "prod_name"],
        )
        return str(root)

    handles = {
        "2025-01-31": ParquetHandle(_month_root("2025-01-31")),
        "2025-02-28": ParquetHandle(_month_root("2025-02-28")),
    }

    model = _mock_model(lambda X: np.full(len(X), 0.5))

    saves: list = []
    write_ds = MagicMock()
    write_ds.save.side_effect = lambda df: saves.append(df)
    write_ds.existing_partition_values.return_value = []  # nothing written yet

    manifest = predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handles,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    # 2 months x 2 products, each written as its own partition
    assert write_ds.save.call_count == 4
    assert sorted(manifest["snap_dates"]) == ["2025-01-31", "2025-02-28"]
    written = pd.concat(saves, ignore_index=True)
    assert len(written) == 4
    assert sorted(set(written["snap_date"].astype(str))) == ["2025-01-31", "2025-02-28"]


# ---------------------------------------------------------------------------
# Per-month incremental predict (issue #130)
#
# Every failure mode here is silent: skipping too much, skipping too little and
# predicting a month nobody asked for all produce a green run and a plausible
# report. So each test states the on-disk starting position explicitly and
# asserts on what the manifest *says*, not merely on which saves happened —
# "no save for January" is satisfied just as well by "never knew January
# existed", which is the exact bug this feature could introduce.
# ---------------------------------------------------------------------------


def _month_handle(tmp_path, snap_date: str, items=("prod_A", "prod_B")):
    """One month's cache root, hive-partitioned exactly like the real cache."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from recsys_tfb.io.handles import ParquetHandle

    df = pd.DataFrame(
        {
            "cust_id": [f"c{i}" for i in range(len(items))],
            "snap_date": [snap_date] * len(items),
            "prod_name": list(items),
            "feat_a": [float(i) for i in range(len(items))],
            "label": [1] + [0] * (len(items) - 1),
        }
    )
    root = tmp_path / snap_date.replace("-", "") / "test_model_input.parquet"
    pq.write_to_dataset(
        pa.Table.from_pandas(df, preserve_index=False),
        root_path=str(root),
        partition_cols=["snap_date", "prod_name"],
    )
    return ParquetHandle(str(root))


def _model() -> MagicMock:
    model = _mock_model(lambda X: np.full(len(X), 0.5))
    return model


def _params_with_test_dates(test_snap_dates, rebuild=None) -> dict:
    """Merged parameters as the CLI hands them to nodes.

    ``dataset.test_snap_dates`` is the authoritative month list; ``rebuild``
    lands under the same runtime key the ``--rebuild-dates`` flag writes.
    """
    from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY

    params = _make_parameters()
    params["dataset"]["test_snap_dates"] = list(test_snap_dates)
    if rebuild is not None:
        params[REBUILD_SNAP_DATES_KEY] = list(rebuild)
    return params


def _manifest_in_this_format(params) -> dict:
    """What the last completed run left behind, had it run this code: every
    configured month recorded in this code's prediction format."""
    from recsys_tfb.core.versioning import TRAINING_PREDICTION_FORMAT_VERSION
    from recsys_tfb.pipelines.training.steps.predict_months import (
        PREDICTION_FORMATS_FIELD,
    )

    return {PREDICTION_FORMATS_FIELD: {
        month: TRAINING_PREDICTION_FORMAT_VERSION
        for month in params["dataset"]["test_snap_dates"]
    }}


_IN_THIS_FORMAT = object()


def _predict(handles, params, write_ds, on_disk=_IN_THIS_FORMAT):
    """``on_disk`` is the manifest the node reads back. By default one from a
    run of this code, so the month tests below exercise the partition side
    alone; the prediction-format tests pass their own."""
    from recsys_tfb.pipelines.training.nodes import predict_and_write_test_predictions

    if on_disk is _IN_THIS_FORMAT:
        on_disk = _manifest_in_this_format(params)
    return predict_and_write_test_predictions(
        model=_model(),
        test_parquet_handle=handles,
        preprocessor_metadata=_make_prep_meta(),
        parameters=params,
        predict_manifest_on_disk=on_disk,
        training_eval_predictions=write_ds,
    )


def test_a_brand_new_month_is_predicted_and_the_finished_one_is_skipped(tmp_path):
    """Degenerate input 1+2: a month with no predictions runs; a month whose
    written partitions already match the cache is skipped.
    """
    handles = {
        "2025-01-31": _month_handle(tmp_path, "2025-01-31"),
        "2025-02-28": _month_handle(tmp_path, "2025-02-28"),
    }
    write_ds = _write_ds(
        existing=[("2025-01-31", "prod_A"), ("2025-01-31", "prod_B")]
    )

    manifest = _predict(handles, _params_with_test_dates(handles), write_ds)

    assert manifest["months_processed"] == ["2025-02-28"]
    assert manifest["months_skipped"] == ["2025-01-31"]
    assert manifest["months_rebuilt"] == []
    assert _saved_partitions(write_ds) == {
        ("2025-02-28", "prod_A"), ("2025-02-28", "prod_B"),
    }


def test_every_month_complete_writes_nothing_at_all(tmp_path):
    """The all-skipped case: re-running predict with no config change must
    cost zero saves and still name both months as skipped.
    """
    handles = {
        "2025-01-31": _month_handle(tmp_path, "2025-01-31"),
        "2025-02-28": _month_handle(tmp_path, "2025-02-28"),
    }
    write_ds = _write_ds(
        existing=[
            (month, prod)
            for month in ("2025-01-31", "2025-02-28")
            for prod in ("prod_A", "prod_B")
        ]
    )

    manifest = _predict(handles, _params_with_test_dates(handles), write_ds)

    assert manifest["months_processed"] == []
    assert manifest["months_skipped"] == ["2025-01-31", "2025-02-28"]
    assert write_ds.save.call_count == 0


def test_a_half_written_month_is_finished_off(tmp_path):
    """Degenerate input 3: predict died after one item's partition. "Any
    partition exists" would call that month done and leave prod_B missing
    forever; the item-set criterion re-runs it.
    """
    handles = {"2025-01-31": _month_handle(tmp_path, "2025-01-31")}
    write_ds = _write_ds(existing=[("2025-01-31", "prod_A")])

    manifest = _predict(handles, _params_with_test_dates(handles), write_ds)

    assert manifest["months_processed"] == ["2025-01-31"]
    assert manifest["months_skipped"] == []
    assert ("2025-01-31", "prod_B") in _saved_partitions(write_ds)


def test_an_old_month_that_gained_an_item_is_recomputed(tmp_path):
    """Degenerate input 4: the month was complete until a new item entered the
    catalogue. Its predictions no longer cover every item, so it is no longer
    complete — even though nothing about that month's own run went wrong.

    Shares a code path with the half-written case above (both are "written is a
    proper subset of cached"), so no mutation kills one and spares the other.
    It is kept because it states the second situation that path exists for; do
    not read it as an independent guard.
    """
    handles = {
        "2025-01-31": _month_handle(
            tmp_path, "2025-01-31", items=("prod_A", "prod_B", "prod_C")
        ),
    }
    write_ds = _write_ds(
        existing=[("2025-01-31", "prod_A"), ("2025-01-31", "prod_B")]
    )

    manifest = _predict(handles, _params_with_test_dates(handles), write_ds)

    assert manifest["months_processed"] == ["2025-01-31"]
    assert ("2025-01-31", "prod_C") in _saved_partitions(write_ds)


def test_rebuild_flag_forces_a_complete_month_and_says_so(tmp_path):
    """The escape hatch: after an upstream backfill the month's partitions are
    complete but stale, so completeness cannot be the last word. The forced
    month is reported separately from the ones that merely had work left.
    """
    handles = {
        "2025-01-31": _month_handle(tmp_path, "2025-01-31"),
        "2025-02-28": _month_handle(tmp_path, "2025-02-28"),
    }
    write_ds = _write_ds(
        existing=[
            (month, prod)
            for month in ("2025-01-31", "2025-02-28")
            for prod in ("prod_A", "prod_B")
        ]
    )

    manifest = _predict(
        handles,
        _params_with_test_dates(handles, rebuild=["2025-01-31"]),
        write_ds,
    )

    assert manifest["months_processed"] == ["2025-01-31"]
    assert manifest["months_rebuilt"] == ["2025-01-31"]
    assert manifest["months_skipped"] == ["2025-02-28"]
    assert _saved_partitions(write_ds) == {
        ("2025-01-31", "prod_A"), ("2025-01-31", "prod_B"),
    }


def test_configured_months_are_authoritative_not_whatever_the_cache_holds(tmp_path):
    """A month left over in the cache after being dropped from
    ``test_snap_dates`` must not be predicted. Driving the loop off the cache
    would silently resurrect it — with no written partitions it even looks
    like honest work.
    """
    handles = {
        "2025-01-31": _month_handle(tmp_path, "2025-01-31"),
        "2025-02-28": _month_handle(tmp_path, "2025-02-28"),  # dropped from config
    }
    write_ds = _write_ds()

    manifest = _predict(
        handles, _params_with_test_dates(["2025-01-31"]), write_ds
    )

    assert manifest["months_processed"] == ["2025-01-31"]
    assert manifest["months_skipped"] == []
    assert {snap for snap, _ in _saved_partitions(write_ds)} == {"2025-01-31"}


def test_a_configured_month_missing_from_the_cache_fails_loud(tmp_path):
    """Configured but not cached means dataset never produced it. Treating an
    empty month as "complete" would skip it forever and hand evaluation an
    empty report, so it raises instead.
    """
    import pytest

    handles = {"2025-01-31": _month_handle(tmp_path, "2025-01-31")}
    params = _params_with_test_dates(["2025-01-31", "2025-02-28"])

    # Match on wording only this rule produces: the month alone also appears in
    # the duplicate-spelling error raised a few lines away in the cache node.
    with pytest.raises(ValueError, match="no rows in the test cache"):
        _predict(handles, params, _write_ds())


# ---------------------------------------------------------------------------
# The prediction format version (ADR-0030 decision 9): a code change that
# alters the predictions but not the model re-predicts every month under the
# same model_version, and nothing retrains. Every test starts from both months
# complete, so "processed" can only come from the format.
# ---------------------------------------------------------------------------

_BOTH_MONTHS = ("2025-01-31", "2025-02-28")


def _every_partition_written():
    return _write_ds(existing=[
        (month, prod) for month in _BOTH_MONTHS for prod in ("prod_A", "prod_B")
    ])


def _both_months_complete(tmp_path):
    """Call once per ``tmp_path``: the handles append to the parquet there."""
    handles = {month: _month_handle(tmp_path, month) for month in _BOTH_MONTHS}
    return handles, _every_partition_written()


def _bump_prediction_format(monkeypatch) -> int:
    """This code is one prediction format past the one the manifest recorded."""
    from recsys_tfb.pipelines.training import nodes

    bumped = nodes.TRAINING_PREDICTION_FORMAT_VERSION + 1
    monkeypatch.setattr(nodes, "TRAINING_PREDICTION_FORMAT_VERSION", bumped)
    return bumped


def test_every_month_recorded_in_another_format_is_rewritten(
    tmp_path, monkeypatch,
):
    """The version's whole job: complete months, but written by code that
    predicted differently. Not reported as rebuilt — nobody named them."""
    from recsys_tfb.pipelines.training.steps.predict_months import (
        PREDICTION_FORMATS_FIELD,
    )

    handles, write_ds = _both_months_complete(tmp_path)
    params = _params_with_test_dates(handles)
    on_disk = _manifest_in_this_format(params)
    bumped = _bump_prediction_format(monkeypatch)

    manifest = _predict(handles, params, write_ds, on_disk=on_disk)

    assert manifest["months_processed"] == list(_BOTH_MONTHS)
    assert manifest["months_skipped"] == []
    assert manifest["months_rebuilt"] == []
    assert _saved_partitions(write_ds) == {
        (month, prod) for month in _BOTH_MONTHS for prod in ("prod_A", "prod_B")
    }
    assert manifest[PREDICTION_FORMATS_FIELD] == {m: bumped for m in _BOTH_MONTHS}


def test_the_same_recorded_format_still_skips(tmp_path, monkeypatch):
    """Equal is equal whatever the number: a bumped code reading a manifest
    its own kind wrote skips as before."""
    from recsys_tfb.pipelines.training.steps.predict_months import (
        PREDICTION_FORMATS_FIELD,
    )

    handles, write_ds = _both_months_complete(tmp_path)
    bumped = _bump_prediction_format(monkeypatch)
    on_disk = {PREDICTION_FORMATS_FIELD: {m: bumped for m in _BOTH_MONTHS}}

    manifest = _predict(
        handles, _params_with_test_dates(handles), write_ds, on_disk=on_disk,
    )

    assert manifest["months_skipped"] == list(_BOTH_MONTHS)
    assert write_ds.save.call_count == 0


def test_no_previous_manifest_rewrites_every_complete_month(tmp_path, caplog):
    """No predict run has completed for this model_version, yet partitions are
    there — a first run that died partway. Nothing vouches for their format,
    so they are re-predicted: wasteful, never silently stale."""
    import logging

    handles, write_ds = _both_months_complete(tmp_path)

    with caplog.at_level(logging.WARNING):
        manifest = _predict(
            handles, _params_with_test_dates(handles), write_ds, on_disk=None,
        )

    assert manifest["months_processed"] == list(_BOTH_MONTHS)
    assert write_ds.save.call_count == 4
    assert "<no record>" in caplog.text


def test_a_manifest_from_before_the_field_rewrites_every_month(tmp_path):
    """The first run after the upgrade that added the field (the ticket's
    "manifest 裡還沒有這個欄位"): the old manifest names the months but not
    their format."""
    handles, write_ds = _both_months_complete(tmp_path)
    legacy = {
        "model_version": "v_test_001",
        "months_processed": [],
        "months_skipped": list(_BOTH_MONTHS),
        "months_rebuilt": [],
    }

    manifest = _predict(
        handles, _params_with_test_dates(handles), write_ds, on_disk=legacy,
    )

    assert manifest["months_processed"] == list(_BOTH_MONTHS)
    assert manifest["months_skipped"] == []


def test_a_month_the_last_run_did_not_configure_is_rewritten(tmp_path):
    """February was dropped from the config when the last run completed, and
    is configured again. Its partitions are complete, but no record says in
    which format — a format bump in between would otherwise go unseen."""
    from recsys_tfb.core.versioning import TRAINING_PREDICTION_FORMAT_VERSION
    from recsys_tfb.pipelines.training.steps.predict_months import (
        PREDICTION_FORMATS_FIELD,
    )

    handles, write_ds = _both_months_complete(tmp_path)
    on_disk = {PREDICTION_FORMATS_FIELD: {
        "2025-01-31": TRAINING_PREDICTION_FORMAT_VERSION,
    }}

    manifest = _predict(
        handles, _params_with_test_dates(handles), write_ds, on_disk=on_disk,
    )

    assert manifest["months_processed"] == ["2025-02-28"]
    assert manifest["months_skipped"] == ["2025-01-31"]


def test_what_one_run_records_the_next_one_reads(tmp_path):
    """Writer and reader agree: the manifest a run lands, loaded back through
    the catalog's JSON, makes the next run skip every month it wrote — and
    the month it skipped, which is in this format too."""
    from recsys_tfb.io.json_dataset import JSONDataset

    handles, _ = _both_months_complete(tmp_path)
    params = _params_with_test_dates(handles)
    first = _predict(
        handles, params,
        _write_ds(existing=[("2025-01-31", "prod_A"), ("2025-01-31", "prod_B")]),
    )
    assert first["months_processed"] == ["2025-02-28"]

    landed = JSONDataset(str(tmp_path / "predict_manifest.json"))
    landed.save(first)
    write_ds = _every_partition_written()

    second = _predict(handles, params, write_ds, on_disk=landed.load())

    assert second["months_skipped"] == list(_BOTH_MONTHS)
    assert write_ds.save.call_count == 0


# ---------------------------------------------------------------------------
# Two-column entity — the framework promises `schema.entity` is a list, and
# this is what makes that promise cost something if it stops being true.
#
# That the write target actually declares those columns is A28's job, checked
# at CLI entry (`tests/test_cli.py::TestEntityColumnsDeclaredA28`), not this
# node's: a Hive save keeps only declared columns, and finding that out here
# would mean finding it out after the whole HPO search had run.
#
# Note the nesting: `get_schema` reads `parameters["schema"]["columns"]`, so a
# schema block written one level up (as `_make_parameters` does) is inert and
# silently falls back to the single-entity defaults. Both tests below would
# pass for the wrong reason if written that way.
# ---------------------------------------------------------------------------


def _two_entity_params(declared_entity=("cust_id", "acct_id")) -> dict:
    params = _make_parameters()
    params["schema"] = {
        "columns": {
            "time": "snap_date",
            "entity": list(declared_entity),
            "item": "prod_name",
            "label": "label",
        }
    }
    params["dataset"]["test_snap_dates"] = ["2025-01-31"]
    return params


def _two_entity_handle(tmp_path):
    """One month of cache carrying two entity columns."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from recsys_tfb.io.handles import ParquetHandle

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c1", "c2", "c2"],
            "acct_id": ["a1", "a1", "a2", "a2"],
            "snap_date": ["2025-01-31"] * 4,
            "prod_name": ["prod_A", "prod_B", "prod_A", "prod_B"],
            "feat_a": [1.0, 1.1, 2.0, 2.1],
            "label": [1, 0, 0, 1],
        }
    )
    root = tmp_path / "two_entity" / "test_model_input.parquet"
    pq.write_to_dataset(
        pa.Table.from_pandas(df, preserve_index=False),
        root_path=str(root),
        partition_cols=["snap_date", "prod_name"],
    )
    return ParquetHandle(str(root))


def test_both_entity_columns_are_written_when_the_catalog_declares_both(tmp_path):
    """`schema.entity` with two columns writes two entity columns.

    The failure this guards against is not an exception — it is a frame that
    looks fine and identifies the wrong thing, because only the first entity
    column survived.
    """
    write_ds = _write_ds()

    manifest = _predict(
        _two_entity_handle(tmp_path), _two_entity_params(), write_ds
    )

    written = pd.concat(write_ds.saved, ignore_index=True)
    assert set(written.columns) == {
        "cust_id", "acct_id", "score", "score_uncalibrated", "label",
        "snap_date", "prod_name",
    }
    # Values, not just presence: a column of the right name filled from the
    # wrong source would pass a presence-only assertion.
    assert set(zip(written["cust_id"], written["acct_id"])) == {
        ("c1", "a1"), ("c2", "a2"),
    }
    assert manifest["n_rows_written"] == 4


def test_the_manifest_survives_the_catalog_round_trip(tmp_path):
    """What the node returns has to still be readable after the run ends.

    ``predict_manifest`` has a catalog entry (issue #233) so the three month
    lists can be answered afterwards, and that entry is a ``JSONDataset`` whose
    ``json.dump`` carries no ``default=`` fallback. So a manifest value that is
    a ``set`` or a ``Path`` — both natural things to reach for when adding a
    field here — raises ``TypeError`` at the very end of a run that has already
    spent hours. This asserts on the reloaded copy, not the returned one: an
    assertion on the in-memory dict would pass for exactly the values that
    cannot be written.
    """
    from recsys_tfb.io.json_dataset import JSONDataset

    handles = {
        "2025-01-31": _month_handle(tmp_path, "2025-01-31"),
        "2025-02-28": _month_handle(tmp_path, "2025-02-28"),
    }
    write_ds = _write_ds(
        existing=[("2025-01-31", "prod_A"), ("2025-01-31", "prod_B")]
    )

    manifest = _predict(handles, _params_with_test_dates(handles), write_ds)

    ds = JSONDataset(str(tmp_path / "models" / "v_test_001" / "predict_manifest.json"))
    ds.save(manifest)
    reloaded = ds.load()

    assert reloaded["months_processed"] == ["2025-02-28"]
    assert reloaded["months_skipped"] == ["2025-01-31"]
    assert reloaded["months_rebuilt"] == []
    # The lists are the point, but a manifest that lost the rest of itself in
    # the round trip is still a broken manifest.
    assert reloaded == manifest


def test_data_volume_names_are_fixed_and_identity_travels_as_fields(tmp_path, caplog):
    """``volume["name"]`` must be a constant, not one name per (month, item).

    Same defect as the ``log_step`` event name #225 fixed two lines above these
    calls: a name built from the data gives the log aggregator
    ``n_months * n_items`` distinct buckets, none of which can be summed or
    compared across runs. The identity travels in ``**fields``, which
    ``log_data_volume`` merges into the ``volume`` dict.
    """
    import logging

    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    model = _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5)
    model.__class__.__name__ = "LightGBMAdapter"

    with caplog.at_level(logging.INFO, logger="recsys_tfb.pipelines.training.nodes"):
        predict_and_write_test_predictions(
            model=model,
            test_parquet_handle=handle,
            preprocessor_metadata=_make_prep_meta(),
            parameters=_make_parameters(),
            predict_manifest_on_disk=None,
            training_eval_predictions=_write_ds(),
        )

    volumes = [
        r.volume for r in caplog.records
        if getattr(r, "event", None) == "data_volume"
    ]
    assert volumes, "predict emitted no data_volume records at all"

    names = {v["name"] for v in volumes}
    assert not [n for n in names if "[" in n], (
        "data_volume names must be fixed strings, not one per (month, item); "
        f"got {sorted(names)}"
    )
    assert {"predict.part_table", "predict.part_pdf"} <= names

    # The identity that used to be baked into the name is still recorded — as
    # fields, keyed on the schema roles (`time_value`, `item_name`), not on
    # this repo's demo column names (`snap_date`, `prod_name`).
    per_partition = [
        v for v in volumes
        if v["name"] in ("predict.part_table", "predict.part_pdf")
    ]
    identities = {
        (v["name"], v.get("time_value"), v.get("item_name"))
        for v in per_partition
    }
    assert identities == {
        (name, snap, prod)
        for name in ("predict.part_table", "predict.part_pdf")
        for snap, prod in [
            ("2025-01-31", "prod_A"), ("2025-01-31", "prod_B"),
            ("2025-02-28", "prod_A"), ("2025-02-28", "prod_B"),
        ]
    }


# ---------------------------------------------------------------------------
# The written frame carries the optional roles' columns (#378)
# ---------------------------------------------------------------------------


def _make_event_test_parquet(tmp_path: Path) -> Path:
    """Same shape, but one query group holds an item twice.

    (c1, 2025-01-31, prod_A) appears as two impressions with different labels —
    exactly what declaring `event` is for, and what the old grain could not
    represent.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        ("c1", "2025-01-31", "prod_A", "i1", 1.0, 1),
        ("c1", "2025-01-31", "prod_A", "i2", 1.5, 0),
        ("c1", "2025-01-31", "prod_B", "i3", 1.1, 0),
    ]
    df = pd.DataFrame(
        rows,
        columns=["cust_id", "snap_date", "prod_name", "imp_id", "feat_a", "label"],
    )
    root = tmp_path / "test_event.parquet"
    pq.write_to_dataset(
        pa.Table.from_pandas(df, preserve_index=False),
        root_path=str(root), partition_cols=["snap_date", "prod_name"],
    )
    return root


def _make_event_parameters() -> dict:
    params = _make_parameters()
    params["schema"] = {
        "columns": {**params["schema"]["columns"], "event": "imp_id"}
    }
    params["dataset"] = {"test_snap_dates": ["2025-01-31"]}
    return params


def test_the_written_frame_carries_the_event_column(tmp_path):
    """The prediction frame is a hardcoded column list — there is no "carry
    everything else" path — so the `event` columns have to be added to it by
    name. Without that the published table holds several rows per item that
    nothing can tell apart, and evaluation's duplicate check raises on a table
    that was correct when it was written.

    A39 is the gate that stops a catalog entry dropping the column; this is
    the other half, that the frame had it in the first place.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    handle = ParquetHandle(path=str(_make_event_test_parquet(tmp_path)))
    model = _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5)
    model.__class__.__name__ = "LightGBMAdapter"
    write_ds = _write_ds()

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_event_parameters(),
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    written = pd.concat(write_ds.saved, ignore_index=True)
    assert "imp_id" in written.columns
    # The two impressions of prod_A stay two distinguishable rows.
    prod_a = written[written["prod_name"] == "prod_A"]
    assert len(prod_a) == 2
    assert set(prod_a["imp_id"]) == {"i1", "i2"}


def test_no_event_column_is_added_when_the_role_is_undeclared(tmp_path):
    """The compatibility half: the frame every existing deployment writes must
    be unchanged, column for column."""
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    handle = ParquetHandle(path=str(_make_test_parquet(tmp_path)))
    model = _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5)
    model.__class__.__name__ = "LightGBMAdapter"
    write_ds = _write_ds()

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    written = pd.concat(write_ds.saved, ignore_index=True)
    assert list(written.columns) == [
        "cust_id", "score", "score_uncalibrated", "label", "snap_date",
        "prod_name",
    ]


# ---------------------------------------------------------------------------
# The written frame carries the zero-positive group weight (#429)
# ---------------------------------------------------------------------------


def _make_weighted_test_parquet(tmp_path: Path, weights) -> Path:
    """Two query groups: c1 holds a positive (weight 1), c2 is a kept
    zero-positive group (weight ``weights``, a scalar or ``None``)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL

    df = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31"] * 4,
        "prod_name": ["prod_A", "prod_B"] * 2,
        "feat_a": [1.0, 1.1, 1.2, 1.3],
        "label": [1, 0, 0, 0],
        ZERO_POSITIVE_GROUP_WEIGHT_COL: [1.0, 1.0, weights, weights],
    })
    root = tmp_path / "test_weighted.parquet"
    pq.write_to_dataset(
        pa.Table.from_pandas(df, preserve_index=False),
        root_path=str(root), partition_cols=["snap_date", "prod_name"],
    )
    return root


def _predict_weighted(tmp_path, dataset, weights=4.0, declared=None):
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    params = _make_parameters()
    params["dataset"] = {"test_snap_dates": ["2025-01-31"], **dataset}
    model = _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5)
    model.__class__.__name__ = "LightGBMAdapter"
    write_ds = _write_ds()
    write_ds.declared_columns = declared
    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=ParquetHandle(
            path=str(_make_weighted_test_parquet(tmp_path, weights))),
        preprocessor_metadata=_make_prep_meta(),
        parameters=params,
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )
    return pd.concat(write_ds.saved, ignore_index=True)


def test_the_written_frame_carries_the_zero_positive_group_weight(tmp_path):
    """The frame is a hardcoded column list, so the weight has to be added by
    name; without it evaluation counts each kept zero-positive group once
    instead of 1/r times. A45 is the other half — that the catalog keeps it."""
    from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL

    written = _predict_weighted(
        tmp_path, {"test_zero_positive_group_ratio": 0.25})
    by_cust = written.groupby("cust_id")[ZERO_POSITIVE_GROUP_WEIGHT_COL].agg(set)
    assert by_cust["c1"] == {1.0}
    assert by_cust["c2"] == {4.0}


def test_no_weight_column_is_written_at_the_default_test_ratio(tmp_path):
    """Decided by the config, not by the column: the test table is
    `columns: "auto"`, so a partition written under ratio 0 still has the
    column (NULL) once any run added it. The frame every existing deployment
    writes stays unchanged."""
    written = _predict_weighted(tmp_path, {}, weights=None)
    assert list(written.columns) == [
        "cust_id", "score", "score_uncalibrated", "label", "snap_date",
        "prod_name",
    ]


def test_a_declared_weight_column_is_written_null_at_the_default_ratio(tmp_path):
    """A catalog that declares the column keeps working when test's ratio goes
    back to 0: a Hive save selects every declared column, so a frame without it
    would fail there. NULL, not 1.0 — no design weight applies to those rows,
    and evaluation reads a NULL weight as "these predictions were not written
    under a positive ratio" instead of mistaking them for weighted ones.
    Values in the cached parquet (left over from an earlier run on the
    `columns: "auto"` test table) are not read."""
    from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL

    written = _predict_weighted(
        tmp_path, {}, weights=4.0,
        declared=["cust_id", "score", "score_uncalibrated", "label",
                  ZERO_POSITIVE_GROUP_WEIGHT_COL])
    assert ZERO_POSITIVE_GROUP_WEIGHT_COL in written.columns
    assert written[ZERO_POSITIVE_GROUP_WEIGHT_COL].isna().all()


def test_the_weighted_path_reads_the_weight_column_and_nothing_unused(
    tmp_path, monkeypatch,
):
    """The per-partition read is narrowed to what the frame and the model need;
    under a positive ratio the weight column is one of them (ADR-0030 decision
    12.2 — a fixed list in the node would have missed it)."""
    from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    params = _make_parameters()
    params["dataset"] = {
        "test_snap_dates": ["2025-01-31"], "test_zero_positive_group_ratio": 0.25,
    }
    reads = _record_partition_reads(monkeypatch)
    write_ds = _write_ds()
    predict_and_write_test_predictions(
        model=_model_that_reads_no_column(),
        test_parquet_handle=ParquetHandle(
            path=str(_make_weighted_test_parquet(tmp_path, 4.0))),
        preprocessor_metadata=_make_prep_meta(),
        parameters=params,
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    # One read per partition, each asking for exactly these columns: fewer
    # would lose one the frame or the model needs, more would read what
    # nothing uses.
    assert len(reads) == 2, reads
    assert all(
        cols is not None and sorted(cols) == sorted(
            ["cust_id", "label", ZERO_POSITIVE_GROUP_WEIGHT_COL, "feat_a"])
        for cols in reads
    ), reads
    written = pd.concat(write_ds.saved, ignore_index=True)
    assert written.groupby("cust_id")[ZERO_POSITIVE_GROUP_WEIGHT_COL].agg(set).to_dict() == {
        "c1": {1.0}, "c2": {4.0},
    }


# ---------------------------------------------------------------------------
# Scored through the adapter, laid out and checked like inference (#484)
# ---------------------------------------------------------------------------


def _record_partition_reads(monkeypatch) -> list:
    """Record the ``columns=`` of every per-partition read the node makes.

    The node opens the cache through ``open_parquet_dataset``; this wraps what
    it returns so ``to_table`` reports the columns it was asked for, and
    everything else (the partition listing's ``get_fragments``) passes through.
    It observes the read itself, not the table handed to the model: a node
    that read every column and then sliced before scoring would pass a check
    on the model's input. A read through any other method is not recorded, so
    a test asserting one record per partition fails on it rather than passing.
    """
    import recsys_tfb.pipelines.training.nodes as nodes_mod

    real_open = nodes_mod.open_parquet_dataset
    reads: list = []

    class _RecordingDataset:
        def __init__(self, ds):
            self._ds = ds

        def __getattr__(self, name):
            return getattr(self._ds, name)

        def to_table(self, *args, **kwargs):
            reads.append(kwargs.get("columns"))
            return self._ds.to_table(*args, **kwargs)

    monkeypatch.setattr(
        nodes_mod, "open_parquet_dataset",
        lambda paths: _RecordingDataset(real_open(paths)))
    return reads


def _model_that_reads_no_column() -> MagicMock:
    """An adapter that names ``feat_a`` as its scoring column but scores
    without looking at the table — so a read that missed ``feat_a`` reaches
    the assertion on the read instead of failing inside the model."""
    model = _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5)
    model.score.side_effect = lambda table, *args: np.full(len(table), 0.5)
    return model


def _run(tmp_path, parquet, params, model=None, write_ds=None):
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    write_ds = _write_ds() if write_ds is None else write_ds
    manifest = predict_and_write_test_predictions(
        model=model or _mock_model(lambda X: np.arange(len(X)).astype(float) + 0.5),
        test_parquet_handle=ParquetHandle(path=str(parquet)),
        preprocessor_metadata=_make_prep_meta(),
        parameters=params,
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )
    return manifest, write_ds


def test_the_score_column_is_named_by_the_schema(tmp_path):
    """Written under ``schema.score``, not a literal ``"score"``: a deployment
    that renames it declares the new name in the catalog, and a frame still
    spelling the old one would put the scores in an undeclared column."""
    params = _make_parameters()
    params["schema"]["columns"]["score"] = "pred"

    _, write_ds = _run(tmp_path, _make_test_parquet(tmp_path), params)

    saved = pd.concat(write_ds.saved, ignore_index=True)
    assert "pred" in saved.columns
    assert "score" not in saved.columns
    assert (saved["pred"] == saved["score_uncalibrated"]).all()


def test_only_the_columns_the_frame_and_the_model_need_are_read(
    tmp_path, monkeypatch,
):
    """A test table carries a column neither the output frame nor the model
    reads; it is not read (the replaced read took every column), and every
    column they do read is."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = pd.DataFrame({
        "cust_id": ["c1", "c1"], "snap_date": ["2025-01-31"] * 2,
        "prod_name": ["prod_A", "prod_B"], "feat_a": [1.0, 2.0],
        "not_a_feature": [9.0, 9.0], "label": [1, 0],
    })
    root = tmp_path / "wide.parquet"
    pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index=False),
                        root_path=str(root), partition_cols=["snap_date", "prod_name"])
    params = _make_parameters()
    params["dataset"] = {"test_snap_dates": ["2025-01-31"]}
    reads = _record_partition_reads(monkeypatch)

    _run(tmp_path, root, params, model=_model_that_reads_no_column())

    assert len(reads) == 2, reads
    assert all(
        cols is not None and sorted(cols) == ["cust_id", "feat_a", "label"]
        for cols in reads
    ), reads


def test_rows_repeating_the_identity_stop_the_write(tmp_path):
    """The same ``(time, entity, item)`` twice with no optional role declared
    is a duplicate, caught before the partition is saved. With ``event``
    declared the same rows are legitimate
    (``test_the_written_frame_carries_the_event_column``)."""
    import pytest

    from recsys_tfb.score_output import ScoredChunkError

    params = _make_parameters()
    params["dataset"] = {"test_snap_dates": ["2025-01-31"]}
    write_ds = _write_ds()

    with pytest.raises(ScoredChunkError, match="no_duplicates"):
        _run(tmp_path, _make_event_test_parquet(tmp_path), params, write_ds=write_ds)
    assert write_ds.save.call_count == 0


def test_a_null_score_stops_the_write(tmp_path):
    import pytest

    from recsys_tfb.score_output import ScoredChunkError

    params = _make_parameters()
    write_ds = _write_ds()
    with pytest.raises(ScoredChunkError, match="no_missing") as exc_info:
        _run(tmp_path, _make_test_parquet(tmp_path), params,
             model=_mock_model(lambda X: np.full(len(X), np.nan)), write_ds=write_ds)
    assert "'score': " in exc_info.value.failures[0]["detail"]
    assert write_ds.save.call_count == 0


def test_the_partition_listing_reads_no_data_file(tmp_path):
    """A skipped month is never read, and listing what the cache holds does
    not read it either: one of January's data files is garbage here, and the
    run still plans both months and writes February.

    January's first file is left intact because opening a dataset reads one
    footer to learn the schema; that is the whole cost of opening it, and not
    the per-row read this test is about.
    """
    handles = {
        "2025-01-31": _month_handle(tmp_path, "2025-01-31"),
        "2025-02-28": _month_handle(tmp_path, "2025-02-28"),
    }
    garbled = list(
        (Path(handles["2025-01-31"].path) / "snap_date=2025-01-31"
         / "prod_name=prod_B").rglob("*.parquet"))
    assert len(garbled) == 1
    for f in garbled:
        f.write_bytes(b"not a parquet file")
    write_ds = _write_ds(
        existing=[("2025-01-31", "prod_A"), ("2025-01-31", "prod_B")]
    )

    manifest = _predict(handles, _params_with_test_dates(handles), write_ds)

    assert manifest["months_skipped"] == ["2025-01-31"]
    assert manifest["months_processed"] == ["2025-02-28"]
    assert _saved_partitions(write_ds) == {
        ("2025-02-28", "prod_A"), ("2025-02-28", "prod_B"),
    }


def test_an_item_that_is_a_feature_is_read_from_the_directory_name(tmp_path):
    """The item is a deferred categorical feature, and in the cache it exists
    only as a directory name — ``prod_name=.../`` — never as a column in a data
    file. The per-partition read still has to ask for it: pyarrow fills a
    partition column in only when the read names it, and the model scores
    from its code."""
    import pyarrow.parquet as pq

    root = _make_test_parquet(tmp_path)
    files = list(Path(root).rglob("*.parquet"))
    assert files and all(
        "prod_name" not in pq.read_schema(f).names for f in files)

    seen_X: list = []

    def predict(X):
        seen_X.append(np.asarray(X))
        return np.arange(len(X)).astype(float) + 0.5

    params = _make_parameters()
    params["dataset"] = {"test_snap_dates": ["2025-01-31"]}
    prep = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["prod_A", "prod_B"]},
    }
    write_ds = _write_ds()
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    predict_and_write_test_predictions(
        model=_mock_model(predict, feature_names=("feat_a", "prod_name")),
        test_parquet_handle=ParquetHandle(path=str(root)),
        preprocessor_metadata=prep,
        parameters=params,
        predict_manifest_on_disk=None,
        training_eval_predictions=write_ds,
    )

    # Partitions are scored in (time, item) order: prod_A then prod_B, each
    # item's rows carrying its own code in the item column of the matrix.
    assert [X[:, 1].tolist() for X in seen_X] == [[0.0, 0.0], [1.0, 1.0]]
    assert _saved_partitions(write_ds) == {
        ("2025-01-31", "prod_A"), ("2025-01-31", "prod_B"),
    }


def test_a_null_entity_stops_the_write(tmp_path):
    """A NULL entity cannot be seen in what is written — ``str`` makes it the
    string ``"None"`` — so it is checked on the rows read, and only a wrong
    test_model_input puts one there."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pytest

    from recsys_tfb.score_output import ScoredChunkError

    df = pd.DataFrame({
        "cust_id": ["c1", None, "c1", "c2"],
        "snap_date": ["2025-01-31"] * 4,
        "prod_name": ["prod_A", "prod_A", "prod_B", "prod_B"],
        "feat_a": [1.0, 2.0, 3.0, 4.0],
        "label": [1, 0, 0, 1],
    })
    root = tmp_path / "null_entity.parquet"
    pq.write_to_dataset(pa.Table.from_pandas(df, preserve_index=False),
                        root_path=str(root), partition_cols=["snap_date", "prod_name"])
    params = _make_parameters()
    params["dataset"] = {"test_snap_dates": ["2025-01-31"]}
    write_ds = _write_ds()

    with pytest.raises(ScoredChunkError, match="no_missing") as exc_info:
        _run(tmp_path, root, params, write_ds=write_ds)

    assert "'cust_id': 1" in exc_info.value.failures[0]["detail"]
    # prod_A is the first partition scored, so nothing was written.
    assert write_ds.save.call_count == 0
