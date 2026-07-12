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
    # Entry summary (preserve existing domain log)
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
    # encode_categoricals summary (preserve existing domain log)
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


def test_extract_xy_with_groups_with_items_returns_item_ids(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy_with_groups

    handle = _make_handle(tmp_path, _make_grouped_df())

    X, y, groups, items = extract_Xy_with_groups(
        handle, _make_grouped_prep_meta(), {}, with_items=True
    )

    assert X.shape == (6, 2)
    assert len(items) == 6
    # items are the raw prod_name values, row-aligned with X / y / groups
    assert list(items) == ["fund", "ccard", "fund", "ccard", "fund", "ccard"]


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
    def _pdf(self):
        return pd.DataFrame({
            "cust_segment_typ": ["mass", "hnw", "mass", "aff"],
            "prod_name": ["a", "a", "b", "a"],
            "label": [1, 0, 1, 0],
        })

    def test_single_key_prod_name_only(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {"a": 3.0})
        assert isinstance(w, np.ndarray)
        np.testing.assert_array_equal(w, np.array([3.0, 3.0, 1.0, 3.0]))

    def test_multi_key_segment_prod(self):
        w = _compute_row_weights(
            self._pdf(), ["cust_segment_typ", "prod_name"],
            {"mass|a": 3.0, "hnw|a": 2.0})
        np.testing.assert_array_equal(w, np.array([3.0, 2.0, 1.0, 1.0]))

    def test_three_key_segment_prod_label(self):
        w = _compute_row_weights(
            self._pdf(), ["cust_segment_typ", "prod_name", "label"],
            {"mass|a|1": 5.0})
        np.testing.assert_array_equal(w, np.array([5.0, 1.0, 1.0, 1.0]))

    def test_down_weight_below_one(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {"a": 0.5})
        np.testing.assert_array_equal(w, np.array([0.5, 0.5, 1.0, 0.5]))

    def test_empty_weights_all_ones(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {})
        np.testing.assert_array_equal(w, np.ones(4))

    def test_empty_keys_all_ones(self):
        w = _compute_row_weights(self._pdf(), [], {"a": 3.0})
        np.testing.assert_array_equal(w, np.ones(4))

    def test_dtype_is_float64(self):
        w = _compute_row_weights(self._pdf(), ["prod_name"], {"a": 2.0})
        assert w.dtype == np.float64


from recsys_tfb.io.extract import _row_weights_from_pdf


class TestRowWeightsObservability:
    """The per-call log line is the only runtime signal of whether
    sample_weight took effect (see _row_weights_from_pdf docstring)."""

    def _pdf(self):
        return pd.DataFrame({"prod_name": ["a", "a", "b", "c"], "label": [1, 0, 1, 0]})

    def _params(self, weights, weight_keys=("prod_name",)):
        return {
            "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                                   "item": "prod_name", "label": "label"}},
            "training": {"sample_weights": weights,
                         "sample_weight_keys": list(weight_keys)},
        }

    def test_logs_inactive_for_empty_table(self, caplog):
        with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
            w = _row_weights_from_pdf(self._pdf(), self._params({}), {})
        np.testing.assert_array_equal(w, np.ones(4))
        msg = "\n".join(r.getMessage() for r in caplog.records)
        assert "sample_weight INACTIVE" in msg and "table is empty" in msg

    def test_logs_inactive_when_key_column_absent(self, caplog):
        with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
            w = _row_weights_from_pdf(
                self._pdf(), self._params({"x": 5.0}, weight_keys=("not_a_col",)), {})
        np.testing.assert_array_equal(w, np.ones(4))
        msg = "\n".join(r.getMessage() for r in caplog.records)
        assert "sample_weight INACTIVE" in msg and "absent from parquet" in msg

    def test_logs_active_with_distribution(self, caplog):
        with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
            w = _row_weights_from_pdf(self._pdf(), self._params({"a": 2.0}), {})
        np.testing.assert_array_equal(w, np.array([2.0, 2.0, 1.0, 1.0]))
        msg = "\n".join(r.getMessage() for r in caplog.records)
        assert "sample_weight ACTIVE" in msg
        assert "rows_adjusted=2" in msg          # the two 'a' rows
        assert "weight min/mean/max=1.000" in msg

    def test_warns_when_table_matches_zero_rows(self, caplog):
        # 'zzz' is not a product in the data -> table matches nothing
        with caplog.at_level(logging.WARNING, logger="recsys_tfb.io.extract"):
            w = _row_weights_from_pdf(self._pdf(), self._params({"zzz": 2.0}), {})
        np.testing.assert_array_equal(w, np.ones(4))
        warns = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert any("matched 0 of 4 rows" in m for m in warns)
        # the diagnostic surfaces the real data keys so a mismatch is obvious
        assert any("sample data keys (encoded)=" in m for m in warns)


from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.io.extract import extract_Xy, extract_Xy_with_groups


def _wparams(weights, weight_keys=None):
    training = {"sample_weights": weights}
    if weight_keys is not None:
        training["sample_weight_keys"] = weight_keys
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": training,
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
                             _wparams({"mass|a": 5.0}, weight_keys=["cust_segment_typ", "prod_name"]), with_weights=True)
        assert X.shape == (4, 2)
        # rows: mass|a, mass|b, hnw|a, hnw|b
        np.testing.assert_array_equal(w, np.array([5.0, 1.0, 1.0, 1.0]))

    def test_extract_Xy_default_key_is_prod_name(self, tmp_path):
        # no sample_weight_keys -> defaults to schema.item (prod_name)
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({"a": 7.0}), with_weights=True)
        # rows: prod a, b, a, b
        np.testing.assert_array_equal(w, np.array([7.0, 1.0, 7.0, 1.0]))

    def test_extract_Xy_three_key_segment_prod_label(self, tmp_path):
        X, y, w = extract_Xy(
            _wparquet(tmp_path), _wprep(),
            _wparams({"mass|a|1": 9.0},
                     weight_keys=["cust_segment_typ", "prod_name", "label"]),
            with_weights=True)
        # rows: mass|a|1, mass|b|0, hnw|a|1, hnw|b|0
        np.testing.assert_array_equal(w, np.array([9.0, 1.0, 1.0, 1.0]))

    def test_extract_Xy_missing_key_column_all_ones(self, tmp_path):
        # configured key column not in parquet -> graceful all-ones backstop
        X, y, w = extract_Xy(
            _wparquet(tmp_path), _wprep(),
            _wparams({"x": 5.0}, weight_keys=["not_a_real_column"]),
            with_weights=True)
        np.testing.assert_array_equal(w, np.ones(4))

    def test_extract_Xy_with_weights_no_table_all_ones(self, tmp_path):
        X, y, w = extract_Xy(_wparquet(tmp_path), _wprep(),
                             _wparams({}), with_weights=True)
        np.testing.assert_array_equal(w, np.ones(4))

    def test_extract_Xy_with_groups_default_is_three_tuple(self, tmp_path):
        out = extract_Xy_with_groups(_wparquet(tmp_path), _wprep(), _wparams({}))
        assert len(out) == 3  # back-compat

    def test_extract_Xy_with_groups_with_weights_appends_w(self, tmp_path):
        X, y, g, w = extract_Xy_with_groups(
            _wparquet(tmp_path), _wprep(), _wparams({"hnw|a": 4.0}, weight_keys=["cust_segment_typ", "prod_name"]),
            with_weights=True)
        assert len(g) == 4
        # rows: mass|a, mass|b, hnw|a, hnw|b
        np.testing.assert_array_equal(w, np.array([1.0, 1.0, 4.0, 1.0]))


from recsys_tfb.io.extract import _translate_weight_table


class TestTranslateWeightTable:
    # category_mappings: code = list index. seg "mass"->0, "hnw"->1, "aff"->2.
    CM = {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}
    ID = ["snap_date", "cust_id", "prod_name"]

    def test_feature_component_translated_to_code(self):
        t, unk = _translate_weight_table(
            {"hnw": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {"1": 2.0} and unk == {}

    def test_identity_component_passthrough(self):
        t, unk = _translate_weight_table(
            {"ccard_ins": 3.0}, ["prod_name"], self.CM, self.ID)
        assert t == {"ccard_ins": 3.0} and unk == {}

    def test_mixed_composite_feature_plus_identity(self):
        t, unk = _translate_weight_table(
            {"mass|ccard_ins": 2.0}, ["cust_segment_typ_2a", "prod_name"],
            self.CM, self.ID)
        assert t == {"0|ccard_ins": 2.0} and unk == {}

    def test_unknown_feature_value_dropped_and_recorded(self):
        t, unk = _translate_weight_table(
            {"afflunet": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {} and unk == {"cust_segment_typ_2a": ["afflunet"]}

    def test_arity_mismatch_passthrough(self):
        t, unk = _translate_weight_table(
            {"mass|x|y": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {"mass|x|y": 2.0} and unk == {}

    def test_partial_bad_composite_dropped_correctly(self):
        # First component unknown, second is identity — key must be dropped
        # entirely (no partial code leaks into the translated table).
        t, unk = _translate_weight_table(
            {"bad_seg|ccard_ins": 2.0, "mass|fund": 3.0},
            ["cust_segment_typ_2a", "prod_name"], self.CM, self.ID)
        assert t == {"0|fund": 3.0}
        assert unk == {"cust_segment_typ_2a": ["bad_seg"]}


class TestRowWeightsEncodeAware:
    # cust_segment_typ_2a is an encoded feature: pdf stores int codes.
    def _pdf(self):
        return pd.DataFrame({
            "cust_segment_typ_2a": [0, 1, 0, 2],  # codes for mass/hnw/mass/aff
            "prod_name": ["a", "a", "b", "a"],
            "label": [1, 0, 1, 0],
        })

    def _params(self, weights, keys):
        return {
            "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                                   "item": "prod_name", "label": "label"}},
            "training": {"sample_weights": weights, "sample_weight_keys": keys},
        }

    def _prep(self):
        # identity cats stay raw; feature cat carries a code mapping.
        return {"category_mappings": {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}}

    def test_feature_key_translated_and_applied(self):
        from recsys_tfb.io.extract import _row_weights_from_pdf
        w = _row_weights_from_pdf(
            self._pdf(), self._params({"hnw": 5.0}, ["cust_segment_typ_2a"]),
            self._prep())
        np.testing.assert_array_equal(w, np.array([1.0, 5.0, 1.0, 1.0]))

    def test_composite_feature_plus_identity(self):
        from recsys_tfb.io.extract import _row_weights_from_pdf
        w = _row_weights_from_pdf(
            self._pdf(),
            self._params({"mass|a": 2.0}, ["cust_segment_typ_2a", "prod_name"]),
            self._prep())
        np.testing.assert_array_equal(w, np.array([2.0, 1.0, 1.0, 1.0]))

    def test_unknown_feature_value_warns_and_all_ones(self, caplog):
        from recsys_tfb.io.extract import _row_weights_from_pdf
        with caplog.at_level(logging.WARNING, logger="recsys_tfb.io.extract"):
            w = _row_weights_from_pdf(
                self._pdf(),
                self._params({"afflunet": 2.0}, ["cust_segment_typ_2a"]),
                self._prep())
        np.testing.assert_array_equal(w, np.ones(4))
        warns = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert any("unknown category value" in m for m in warns)
        # all keys unknown -> the unknown-value warning is the full diagnosis;
        # the redundant 0-match warning must NOT also fire.
        assert not any("matched 0 of" in m for m in warns)


# ---------------------------------------------------------------------------
# B6 training-read backstop — fail fast on un-encoded non-numeric feature cols
# ---------------------------------------------------------------------------


def _b6_df(with_string: bool) -> pd.DataFrame:
    cols = {
        "cust_id": ["c1", "c2", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 3),
        "prod_name": ["fund", "ccard", "fund"],  # deferred identity cat (legit string)
        "f_num": [1.0, 2.0, 3.0],
        "flag_bool": [True, False, True],  # boolean feature — numeric, must NOT be flagged
        "label": [0, 1, 0],
    }
    if with_string:
        cols["rogue_str"] = ["x", "y", "z"]  # string feature, NOT declared categorical
    return pd.DataFrame(cols)


def _b6_meta(with_string: bool) -> dict:
    feats = ["f_num", "flag_bool", "prod_name"] + (["rogue_str"] if with_string else [])
    return {
        "feature_columns": feats,
        "categorical_columns": ["prod_name"],  # rogue_str is not here
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }


_B6_PARAMS = {
    "schema": {"label": "label", "identity_columns": ["cust_id", "snap_date", "prod_name"]}
}


class TestExtractXyB6Backstop:
    def test_string_feature_fails_fast(self, tmp_path: Path) -> None:
        from recsys_tfb.core.consistency import DataConsistencyError
        from recsys_tfb.io.extract import extract_Xy

        handle = _make_handle(tmp_path, _b6_df(True))
        with pytest.raises(DataConsistencyError, match="rogue_str"):
            extract_Xy(handle, _b6_meta(True), _B6_PARAMS)

    def test_clean_parquet_proceeds(self, tmp_path: Path) -> None:
        from recsys_tfb.io.extract import extract_Xy

        handle = _make_handle(tmp_path, _b6_df(False))
        X, y = extract_Xy(handle, _b6_meta(False), _B6_PARAMS)
        assert X.shape[0] == 3
        assert list(y) == [0, 1, 0]
