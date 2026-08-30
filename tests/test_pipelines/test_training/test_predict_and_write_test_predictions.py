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
    filter_test_model_input node — every (snap_date, cust_id) group present
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
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "label",
            "score": "score",
            "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        },
    }


#: A single-entity declaration in the shape ``HiveTableDataset.declared_columns``
#: reports it (data columns + partition_filter key + partition_cols). It is
#: modelled on the ``training_eval_predictions`` entry in
#: ``conf/base/catalog.yaml`` but is deliberately NOT pinned to it: what these
#: tests are about is the entity columns, and a stand-in that has to be updated
#: whenever an unrelated column is added to the catalog would fail for reasons
#: none of them are testing.
_DECLARED_COLUMNS = (
    "cust_id", "score", "score_uncalibrated", "label",
    "model_version", "snap_date", "prod_name",
)


def _write_ds(existing=(), declared=None) -> MagicMock:
    """Mock of the ``training_eval_predictions`` catalog dataset.

    ``existing``: ``(snap_date, prod_name)`` pairs already written for this
    model_version — what ``HiveTableDataset.existing_partition_values()``
    reports. Supplying it explicitly is the point of the seam: predict decides
    what to skip from this list alone, so every test states the on-disk
    starting position rather than inheriting a default.

    ``declared``: the column names the catalog entry declares — what
    ``HiveTableDataset.declared_columns`` reports. Defaults to the real
    ``training_eval_predictions`` declaration.
    """
    declared = _DECLARED_COLUMNS if declared is None else declared
    ds = MagicMock()
    ds.save.side_effect = lambda df: ds.saved.append(df)
    ds.saved = []
    ds.declared_columns = list(declared)
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
    layer — upstream filter_test_model_input already dropped negative-only
    groups before this function runs).
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # Mock model: predict returns increasing scores; not calibrated
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    # Not a CalibratedModelAdapter (isinstance check fails -> raw == score)
    model.__class__.__name__ = "LightGBMAdapter"

    # Mock HiveTableDataset handle — capture every save() call
    saves: list[pd.DataFrame] = []

    def capture_save(df):
        # Production code passes a pandas DataFrame to HiveTableDataset.save()
        # (the dataset's _to_spark converts internally); tests assert on it directly.
        saves.append(df)

    write_ds = MagicMock()
    write_ds.declared_columns = list(_DECLARED_COLUMNS)
    write_ds.save.side_effect = capture_save
    # Nothing written yet, so every configured month is incomplete and gets
    # predicted — the pre-incremental behaviour this test was written against.
    write_ds.existing_partition_values.return_value = []

    manifest = predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
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
    assert set(manifest["prods"]) == {"prod_A", "prod_B"}
    assert manifest["model_version"] == "v_test_001"
    assert manifest["n_rows_written"] == len(all_written)


def test_predict_and_write_score_uncalibrated_equals_score_when_not_calibrated(tmp_path):
    """When the model is not a CalibratedModelAdapter, score_uncalibrated
    must equal score row-for-row in every written partition.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    model = MagicMock()
    model.predict.side_effect = lambda X: np.array([0.42] * len(X))
    model.__class__.__name__ = "LightGBMAdapter"

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.declared_columns = list(_DECLARED_COLUMNS)
    write_ds.save.side_effect = lambda df: saves.append(df)
    write_ds.existing_partition_values.return_value = []  # nothing written yet

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    for df in saves:
        assert (df["score"] == df["score_uncalibrated"]).all()


def test_predict_and_write_calibrated_branch_calls_predict_uncalibrated(tmp_path):
    """When the model IS a CalibratedModelAdapter, predict_uncalibrated
    is called to populate score_uncalibrated separately from score.
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # spec=CalibratedModelAdapter makes isinstance check pass
    model = MagicMock(spec=CalibratedModelAdapter)
    model.predict.side_effect = lambda X: np.array([0.9] * len(X))
    model.predict_uncalibrated.side_effect = lambda X: np.array([0.1] * len(X))

    saves: list[pd.DataFrame] = []
    write_ds = MagicMock()
    write_ds.declared_columns = list(_DECLARED_COLUMNS)
    write_ds.save.side_effect = lambda df: saves.append(df)
    write_ds.existing_partition_values.return_value = []  # nothing written yet

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
        training_eval_predictions=write_ds,
    )

    # predict_uncalibrated must have been called once per partition
    assert model.predict_uncalibrated.call_count == 4

    for df in saves:
        assert (df["score"] == 0.9).all()
        assert (df["score_uncalibrated"] == 0.1).all()


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

    model = MagicMock()
    model.predict.side_effect = lambda X: np.full(len(X), 0.5)

    saves: list = []
    write_ds = MagicMock()
    write_ds.declared_columns = list(_DECLARED_COLUMNS)
    write_ds.save.side_effect = lambda df: saves.append(df)
    write_ds.existing_partition_values.return_value = []  # nothing written yet

    manifest = predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handles,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
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
    model = MagicMock()
    model.predict.side_effect = lambda X: np.full(len(X), 0.5)
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


def _predict(handles, params, write_ds):
    from recsys_tfb.pipelines.training.nodes import predict_and_write_test_predictions

    return predict_and_write_test_predictions(
        model=_model(),
        test_parquet_handle=handles,
        preprocessor_metadata=_make_prep_meta(),
        parameters=params,
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
# Two-column entity — the framework promises `schema.entity` is a list, and
# these are what make that promise cost something if it stops being true.
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
    write_ds = _write_ds(
        declared=(
            "cust_id", "acct_id", "score", "score_uncalibrated", "label",
            "model_version", "snap_date", "prod_name",
        )
    )

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


def test_an_entity_column_the_catalog_never_declared_raises(tmp_path):
    """Pre-check: catalog declares one entity column, schema names two.

    Without this the run succeeds and `acct_id` is dropped by
    `HiveTableDataset.save`'s `df.select(*declared)` — a whole identity column
    missing from the published table, with nothing in the log to say so.
    """
    import pytest

    # The real single-entity declaration, unchanged, against a two-column schema.
    write_ds = _write_ds()

    with pytest.raises(ValueError, match="does not declare entity column"):
        _predict(_two_entity_handle(tmp_path), _two_entity_params(), write_ds)

    assert write_ds.save.call_count == 0
