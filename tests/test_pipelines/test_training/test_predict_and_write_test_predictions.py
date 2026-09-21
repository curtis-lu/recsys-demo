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
    layer — upstream filter_test_model_input already dropped negative-only
    groups before this function runs).
    """
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import (
        predict_and_write_test_predictions,
    )

    parquet_path = _make_test_parquet(tmp_path)
    handle = ParquetHandle(path=str(parquet_path))

    # Mock model: predict returns increasing scores
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
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

    model = MagicMock()
    model.predict.side_effect = lambda X: np.array([0.42] * len(X))
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

    model = MagicMock()
    model.predict.side_effect = lambda X: np.full(len(X), 0.5)

    saves: list = []
    write_ds = MagicMock()
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

    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    model.__class__.__name__ = "LightGBMAdapter"

    with caplog.at_level(logging.INFO, logger="recsys_tfb.pipelines.training.nodes"):
        predict_and_write_test_predictions(
            model=model,
            test_parquet_handle=handle,
            preprocessor_metadata=_make_prep_meta(),
            parameters=_make_parameters(),
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
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    model.__class__.__name__ = "LightGBMAdapter"
    write_ds = _write_ds()

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_event_parameters(),
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
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    model.__class__.__name__ = "LightGBMAdapter"
    write_ds = _write_ds()

    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=handle,
        preprocessor_metadata=_make_prep_meta(),
        parameters=_make_parameters(),
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
    model = MagicMock()
    model.predict.side_effect = lambda X: np.arange(len(X)).astype(float) + 0.5
    model.__class__.__name__ = "LightGBMAdapter"
    write_ds = _write_ds()
    write_ds.declared_columns = declared
    predict_and_write_test_predictions(
        model=model,
        test_parquet_handle=ParquetHandle(
            path=str(_make_weighted_test_parquet(tmp_path, weights))),
        preprocessor_metadata=_make_prep_meta(),
        parameters=params,
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
