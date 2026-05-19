"""Tests for io.extract.extract_Xy."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


def _make_handle(tmp_path: Path, df: pd.DataFrame):
    from recsys_tfb.io.handles import ParquetHandle

    parquet_dir = tmp_path / "input.parquet"
    df.to_parquet(parquet_dir, engine="pyarrow")
    return ParquetHandle(path=str(parquet_dir))


def test_extract_xy_returns_numpy_arrays(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 3),
            "prod_name": ["fund", "ccard", "fund"],
            "feat_a": [1.0, 2.0, 3.0],
            "feat_b": [0.1, 0.2, 0.3],
            "label": [0, 1, 0],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    X, y = extract_Xy(handle, prep_meta, parameters)

    assert X.shape == (3, 3)
    assert list(y) == [0, 1, 0]
    # prod_name is int-coded: fund=0, ccard=1, fund=0
    assert list(X[:, 2]) == [0, 1, 0]


# ---------------------------------------------------------------------------
# Observability — sub-step log_step events and size summary INFO logs
# ---------------------------------------------------------------------------


def _make_prep_meta_with_cat():
    return {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }


def _make_parameters_with_cat():
    return {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }


def _make_df_with_cat():
    return pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 3),
            "prod_name": ["fund", "ccard", "fund"],
            "feat_a": [1.0, 2.0, 3.0],
            "feat_b": [0.1, 0.2, 0.3],
            "label": [0, 1, 0],
        }
    )


def test_extract_xy_emits_sub_step_events(tmp_path: Path, caplog) -> None:
    from recsys_tfb.io.extract import extract_Xy

    handle = _make_handle(tmp_path, _make_df_with_cat())

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, _make_prep_meta_with_cat(), _make_parameters_with_cat())

    started = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_started"
    }
    completed = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_completed"
    }
    expected = {"read_parquet", "slice_features", "encode_categoricals", "to_numpy"}
    assert started == expected
    assert completed == expected


def test_extract_xy_logs_size_summaries(tmp_path: Path, caplog) -> None:
    from recsys_tfb.io.extract import extract_Xy

    handle = _make_handle(tmp_path, _make_df_with_cat())

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, _make_prep_meta_with_cat(), _make_parameters_with_cat())

    vol = {
        r.volume["name"]: r.volume
        for r in caplog.records
        if getattr(r, "event", None) == "data_volume"
    }
    messages = [r.getMessage() for r in caplog.records]
    # Entry summary（保留既有 domain log）
    assert any(
        "extract_Xy start" in m and "n_feature_cols=3" in m and "label=label" in m
        for m in messages
    )
    # N1: full pdf sized via helper (deep=True)
    assert vol["extract_Xy.pdf"]["kind"] == "pandas"
    assert vol["extract_Xy.pdf"]["rows"] == 3
    assert vol["extract_Xy.pdf"]["deep"] is True
    # retrofit: X_df via helper, deep=True (was deep=False)
    assert vol["_pdf_to_X.X_df"]["rows"] == 3
    assert vol["_pdf_to_X.X_df"]["cols"] == 3
    assert vol["_pdf_to_X.X_df"]["deep"] is True
    # encode_categoricals summary（保留既有 domain log）
    assert any(
        "deferred_cats=" in m and "prod_name" in m and "count=1" in m for m in messages
    )
    # retrofit: X / y via helper numpy branch
    assert vol["extract_Xy.X"]["kind"] == "numpy"
    assert vol["extract_Xy.X"]["rows"] == 3
    assert vol["extract_Xy.X"]["cols"] == 3
    assert vol["extract_Xy.y"]["kind"] == "numpy"
    assert vol["extract_Xy.y"]["rows"] == 3
    # D1: shape-only "parquet loaded" line removed
    assert not any("parquet loaded" in m for m in messages)


def test_extract_xy_skips_encode_step_when_no_deferred_cats(
    tmp_path: Path, caplog
) -> None:
    from recsys_tfb.io.extract import extract_Xy

    # No string identity column in the input → deferred_cats empty
    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date"],
        }
    }

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, prep_meta, parameters)

    started = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_started"
    }
    # Other sub-steps still emit
    assert "read_parquet" in started
    assert "slice_features" in started
    assert "to_numpy" in started
    # Encode step is SKIPPED entirely
    assert "encode_categoricals" not in started
    # And there is no encode summary INFO line
    messages = [r.getMessage() for r in caplog.records]
    assert not any("deferred_cats=" in m for m in messages)


# ---------------------------------------------------------------------------
# Pre-read parquet metadata observability
# ---------------------------------------------------------------------------


def test_extract_xy_logs_parquet_metadata_before_read(
    tmp_path: Path, caplog
) -> None:
    from recsys_tfb.io.extract import extract_Xy

    handle = _make_handle(tmp_path, _make_df_with_cat())

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, _make_prep_meta_with_cat(), _make_parameters_with_cat())

    messages = [r.getMessage() for r in caplog.records]
    metadata_logs = [m for m in messages if "parquet metadata" in m]
    assert len(metadata_logs) == 1
    m = metadata_logs[0]
    # _make_df_with_cat → 6 cols: cust_id, snap_date, prod_name, feat_a, feat_b, label
    assert "num_rows=3" in m
    assert "num_columns=6" in m
    assert "num_row_groups=" in m
    assert "total_uncompressed_mb=" in m
    assert "schema_types=" in m

    # Metadata log MUST come BEFORE the read_parquet step_started event,
    # otherwise the whole feature (visible even when read_parquet OOMs) breaks.
    records = caplog.records
    metadata_idx = next(
        i for i, r in enumerate(records) if "parquet metadata" in r.getMessage()
    )
    read_parquet_started_idx = next(
        i
        for i, r in enumerate(records)
        if getattr(r, "event", None) == "step_started"
        and getattr(r, "step", None) == "read_parquet"
    )
    assert metadata_idx < read_parquet_started_idx


# ---------------------------------------------------------------------------
# extract_Xy_with_groups — tune_hyperparameters helper
# ---------------------------------------------------------------------------


def _make_grouped_df():
    """Six rows across three customers; c1 + c2 have positives, c3 does not."""
    return pd.DataFrame(
        {
            "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "prod_name": ["fund", "ccard", "fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "label": [1, 0, 0, 1, 0, 0],
        }
    )


def _make_grouped_prep_meta():
    return {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }


def test_extract_xy_with_groups_returns_groups(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy_with_groups

    handle = _make_handle(tmp_path, _make_grouped_df())

    X, y, groups = extract_Xy_with_groups(
        handle, _make_grouped_prep_meta(), {}
    )

    assert X.shape == (6, 2)
    assert list(y) == [1, 0, 0, 1, 0, 0]
    assert groups.dtype == np.int64
    assert len(groups) == 6
    # Same (cust_id, snap_date) → same group id
    assert groups[0] == groups[1]  # c1
    assert groups[2] == groups[3]  # c2
    assert groups[4] == groups[5]  # c3
    # Three distinct groups for three distinct customers
    assert len(set(groups.tolist())) == 3


def test_extract_xy_with_groups_filters_no_positive_customers(
    tmp_path: Path,
) -> None:
    from recsys_tfb.io.extract import extract_Xy_with_groups

    handle = _make_handle(tmp_path, _make_grouped_df())

    X, y, groups = extract_Xy_with_groups(
        handle,
        _make_grouped_prep_meta(),
        {},
        filter_groups_with_positives=True,
    )

    # c3 dropped → only c1 (rows 0,1) and c2 (rows 2,3) remain
    assert X.shape == (4, 2)
    assert list(y) == [1, 0, 0, 1]
    assert len(groups) == 4
    assert len(set(groups.tolist())) == 2
    # Row-level alignment preserved within each group
    assert groups[0] == groups[1]
    assert groups[2] == groups[3]
    assert groups[0] != groups[2]


def test_extract_xy_with_groups_filter_all_dropped(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy_with_groups

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 0],
        }
    )
    handle = _make_handle(tmp_path, df)

    X, y, groups = extract_Xy_with_groups(
        handle,
        _make_grouped_prep_meta(),
        {},
        filter_groups_with_positives=True,
    )

    assert X.shape == (0, 2)
    assert len(y) == 0
    assert len(groups) == 0
    assert groups.dtype == np.int64


def test_extract_xy_metadata_probe_failure_logs_warning_but_does_not_block(
    tmp_path: Path, caplog
) -> None:
    """When the metadata probe raises (e.g. bogus path), log WARNING and
    let extract_Xy proceed; the downstream pandas read will fail loudly on
    its own — we don't want observability to mask or replace that error."""
    from recsys_tfb.io.extract import extract_Xy
    from recsys_tfb.io.handles import ParquetHandle

    bogus = ParquetHandle(path=str(tmp_path / "does_not_exist.parquet"))

    with caplog.at_level(logging.WARNING, logger="recsys_tfb.io.extract"):
        with pytest.raises(Exception):
            extract_Xy(
                bogus, _make_prep_meta_with_cat(), _make_parameters_with_cat()
            )

    warning_messages = [
        r.getMessage() for r in caplog.records if r.levelname == "WARNING"
    ]
    assert any(
        "parquet metadata probe failed" in m for m in warning_messages
    )


def test_pdf_to_X_returns_numpy_with_categoricals_encoded() -> None:
    """_pdf_to_X turns an already-loaded pdf into X numpy, applying the
    same slice_features + encode_categoricals + to_numpy logic that
    extract_Xy uses after its read_parquet step.
    """
    from recsys_tfb.io.extract import _pdf_to_X

    pdf = pd.DataFrame({
        "cust_id": ["c1", "c2", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 3),
        "prod_name": ["fund", "ccard", "fund"],
        "feat_a": [1.0, 2.0, 3.0],
        "feat_b": [0.1, 0.2, 0.3],
        "label": [0, 1, 0],
    })
    prep_meta = {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    X = _pdf_to_X(pdf, prep_meta, parameters)

    assert X.shape == (3, 3)
    # prod_name int-coded: fund=0, ccard=1, fund=0
    assert list(X[:, 2]) == [0, 1, 0]
    # numeric features pass through
    assert list(X[:, 0]) == [1.0, 2.0, 3.0]
    assert list(X[:, 1]) == [0.1, 0.2, 0.3]


def test_pdf_to_X_skips_encode_when_no_deferred_cats() -> None:
    """When no categorical_columns overlap with identity_columns, the
    encode_categoricals step is skipped (mirrors extract_Xy behavior).
    """
    from recsys_tfb.io.extract import _pdf_to_X

    pdf = pd.DataFrame({
        "cust_id": ["c1", "c2"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 2),
        "feat_a": [1.0, 2.0],
        "label": [0, 1],
    })
    prep_meta = {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date"],
        }
    }

    X = _pdf_to_X(pdf, prep_meta, parameters)

    assert X.shape == (2, 1)
    assert list(X[:, 0]) == [1.0, 2.0]


from recsys_tfb.io.extract import _compute_row_weights


class TestComputeRowWeights:
    def test_known_pairs_get_weight_unknown_get_one(self):
        seg = pd.Series(["mass", "hnw", "mass", "aff"])
        prod = pd.Series(["a", "a", "b", "a"])
        w = _compute_row_weights(seg, prod, {"mass|a": 3.0, "hnw|a": 2.0})
        assert isinstance(w, np.ndarray)
        np.testing.assert_array_equal(w, np.array([3.0, 2.0, 1.0, 1.0]))

    def test_empty_weights_all_ones(self):
        w = _compute_row_weights(pd.Series(["m", "h"]), pd.Series(["a", "b"]), {})
        np.testing.assert_array_equal(w, np.array([1.0, 1.0]))

    def test_dtype_is_float64(self):
        w = _compute_row_weights(pd.Series(["m"]), pd.Series(["a"]), {"m|a": 2.0})
        assert w.dtype == np.float64


from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.io.extract import extract_Xy, extract_Xy_with_groups


def _wparams(weights):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"sample_weights": weights},
    }


def _wprep():
    return {"feature_columns": ["prod_name", "f1"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
            "drop_columns": []}


def _wparquet(tmp_path):
    pdf = pd.DataFrame({
        "snap_date": ["2025-01-31"] * 4,
        "cust_id": [1, 1, 2, 2],
        "prod_name": ["a", "b", "a", "b"],
        "cust_segment_typ": ["mass", "mass", "hnw", "hnw"],
        "label": [1, 0, 1, 0],
        "f1": [0.1, 0.2, 0.3, 0.4]})
    p = tmp_path / "mi.parquet"
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


class TestExtractWithWeights:
    def test_extract_Xy_default_is_two_tuple(self, tmp_path):
        out = extract_Xy(_wparquet(tmp_path), _wprep(), _wparams({}))
        assert len(out) == 2  # back-compat: existing callers unaffected

    def test_extract_Xy_with_weights_appends_aligned_w(self, tmp_path):
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({"mass|a": 5.0}), with_weights=True)
        assert X.shape == (4, 2)
        # rows: mass|a, mass|b, hnw|a, hnw|b
        np.testing.assert_array_equal(w, np.array([5.0, 1.0, 1.0, 1.0]))

    def test_extract_Xy_with_weights_no_table_all_ones(self, tmp_path):
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({}), with_weights=True)
        np.testing.assert_array_equal(w, np.ones(4))

    def test_extract_Xy_with_groups_default_is_three_tuple(self, tmp_path):
        out = extract_Xy_with_groups(_wparquet(tmp_path), _wprep(), _wparams({}))
        assert len(out) == 3  # back-compat

    def test_extract_Xy_with_groups_with_weights_appends_w(self, tmp_path):
        X, y, g, w = extract_Xy_with_groups(
            _wparquet(tmp_path), _wprep(), _wparams({"hnw|a": 4.0}),
            with_weights=True)
        assert len(g) == 4
        # rows: mass|a, mass|b, hnw|a, hnw|b
        np.testing.assert_array_equal(w, np.array([1.0, 1.0, 4.0, 1.0]))
