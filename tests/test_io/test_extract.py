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
            "feat_a": np.array([1.0, 2.0, 3.0], dtype=np.float32),
            "feat_b": np.array([0.1, 0.2, 0.3], dtype=np.float32),
            "label": [0, 1, 0],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }
    parameters = {"schema": {"columns": {"label": "label"}}}

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
    return {"schema": {"columns": {"label": "label"}}}


def _make_df_with_cat():
    return pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 3),
            "prod_name": ["fund", "ccard", "fund"],
            "feat_a": np.array([1.0, 2.0, 3.0], dtype=np.float32),
            "feat_b": np.array([0.1, 0.2, 0.3], dtype=np.float32),
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
    # #284 — the streamed read builds the matrix *as* it reads, so there is no
    # frame to slice and no matrix to flatten: read_parquet is the whole build.
    expected = {"read_parquet"}
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
    # #284 — no whole-frame read and no X_df copy exist to size any more. What
    # is left in pandas is the narrow aux frame (label only here).
    assert "extract_Xy.pdf" not in vol
    assert "pdf_to_X.X_df" not in vol
    assert vol["extract_Xy.aux"]["kind"] == "pandas"
    assert vol["extract_Xy.aux"]["rows"] == 3
    assert vol["extract_Xy.aux"]["cols"] == 1
    assert vol["extract_Xy.aux"]["deep"] is True
    # The matrix the read is about to allocate is announced before it happens,
    # so a driver killed by that allocation still says how big it was.
    assert any(
        "streaming read" in m and "dtype=float32" in m and "batch_rows=" in m
        for m in messages
    )
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
            "feat_a": np.array([1.0, 2.0], dtype=np.float32),
            "label": [0, 1],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a"],
        "categorical_columns": [],
        "category_mappings": {},
    }
    parameters = {"schema": {"columns": {"label": "label"}}}

    with caplog.at_level(logging.INFO, logger="recsys_tfb.io.extract"):
        extract_Xy(handle, prep_meta, parameters)

    started = {
        r.step
        for r in caplog.records
        if getattr(r, "event", None) == "step_started"
    }
    # The read still emits
    assert "read_parquet" in started
    # No encode step and no encode summary INFO line
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
            "feat_a": np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float32),
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
    """pdf_to_X turns an already-loaded pdf into X numpy, applying the
    same slice_features + encode_categoricals + to_numpy logic that
    extract_Xy uses after its read_parquet step.
    """
    from recsys_tfb.io.extract import pdf_to_X

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
    parameters = {"schema": {"columns": {"label": "label"}}}

    X = pdf_to_X(pdf, prep_meta, parameters)

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
    from recsys_tfb.io.extract import pdf_to_X

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
    parameters = {"schema": {"columns": {"label": "label"}}}

    X = pdf_to_X(pdf, prep_meta, parameters)

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
        "f1": np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)})
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


from recsys_tfb.io.extract import translate_weight_table


class TestTranslateWeightTable:
    # category_mappings: code = list index. seg "mass"->0, "hnw"->1, "aff"->2.
    CM = {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}
    ID = ["snap_date", "cust_id", "prod_name"]

    def test_feature_component_translated_to_code(self):
        t, unk = translate_weight_table(
            {"hnw": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {"1": 2.0} and unk == {}

    def test_identity_component_passthrough(self):
        t, unk = translate_weight_table(
            {"ccard_ins": 3.0}, ["prod_name"], self.CM, self.ID)
        assert t == {"ccard_ins": 3.0} and unk == {}

    def test_mixed_composite_feature_plus_identity(self):
        t, unk = translate_weight_table(
            {"mass|ccard_ins": 2.0}, ["cust_segment_typ_2a", "prod_name"],
            self.CM, self.ID)
        assert t == {"0|ccard_ins": 2.0} and unk == {}

    def test_unknown_feature_value_dropped_and_recorded(self):
        t, unk = translate_weight_table(
            {"afflunet": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {} and unk == {"cust_segment_typ_2a": ["afflunet"]}

    def test_arity_mismatch_passthrough(self):
        t, unk = translate_weight_table(
            {"mass|x|y": 2.0}, ["cust_segment_typ_2a"], self.CM, self.ID)
        assert t == {"mass|x|y": 2.0} and unk == {}

    def test_partial_bad_composite_dropped_correctly(self):
        # First component unknown, second is identity — key must be dropped
        # entirely (no partial code leaks into the translated table).
        t, unk = translate_weight_table(
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
        "f_num": np.array([1.0, 2.0, 3.0], dtype=np.float32),
        # Cast by build_model_input like every other numeric feature (#283), so
        # it is float32 on disk here. That B6 would admit it *as a boolean* is
        # pinned at the predicate level in test_consistency.py
        # (TestB6AdmissionIsBackedByTheCast); an actual boolean column in
        # model_input is what B9 rejects — see TestExtractXyB9Backstop.
        "flag_bool": np.array([1.0, 0.0, 1.0], dtype=np.float32),
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
    "schema": {"columns": {"label": "label"}}
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


# ---------------------------------------------------------------------------
# Hot path (#280) — narrow-frame group ids and weight keys
#
# Both rewrites replace work done over the *whole* frame with work done over a
# frame holding only the few columns involved.  They must stay bit-for-bit
# identical to what they replaced, so every test below is a parity test against
# an inlined copy of the pre-#280 implementation, plus the structural property
# that makes them fast: the source frame's block count must not change.
# ---------------------------------------------------------------------------


def _fragmented_pdf(n_rows=40, n_feats=30, seed=0):
    """Frame with one block per column.

    Column-at-a-time assignment is what makes a frame fragmented; a whole-frame
    ``groupby`` / ``pdf[list]`` consolidates it, copying every column.  The
    block-count assertions below are vacuous unless the fixture starts
    fragmented, so ``test_whole_frame_groupby_would_consolidate`` guards that.
    """
    rng = np.random.default_rng(seed)
    pdf = pd.DataFrame({"snap_date": rng.choice(["2025-01-31", "2025-02-28"], n_rows)})
    pdf["cust_id"] = rng.integers(0, 7, n_rows)
    for i in range(n_feats):
        pdf[f"f{i}"] = rng.random(n_rows)
    return pdf


class TestGroupIdsNarrowFrame:
    GROUP_COLS = ["snap_date", "cust_id"]

    def test_bit_identical_to_whole_frame_groupby(self):
        from recsys_tfb.io.extract import _group_ids

        pdf = _fragmented_pdf()
        expected = (
            pdf.copy()
            .groupby(self.GROUP_COLS, sort=False)
            .ngroup()
            .to_numpy(dtype=np.int64)
        )
        assert np.array_equal(_group_ids(pdf, self.GROUP_COLS), expected)

    def test_source_frame_is_not_consolidated(self):
        from recsys_tfb.io.extract import _group_ids

        pdf = _fragmented_pdf()
        before = pdf._mgr.nblocks
        _group_ids(pdf, self.GROUP_COLS)
        assert pdf._mgr.nblocks == before

    def test_whole_frame_groupby_would_consolidate(self):
        """Keeps the assertion above honest: the property it checks is one the
        replaced implementation genuinely violated."""
        pdf = _fragmented_pdf()
        before = pdf._mgr.nblocks
        pdf.groupby(self.GROUP_COLS, sort=False).ngroup()
        assert pdf._mgr.nblocks < before

    def test_ids_number_groups_by_first_appearance(self):
        from recsys_tfb.io.extract import _group_ids

        pdf = pd.DataFrame({"t": ["b", "a", "b", "c"], "e": [1, 1, 1, 2]})
        assert list(_group_ids(pdf, ["t", "e"])) == [0, 1, 0, 2]

    def test_single_group_column(self):
        from recsys_tfb.io.extract import _group_ids

        pdf = _fragmented_pdf()
        expected = (
            pdf.copy().groupby(["cust_id"], sort=False).ngroup().to_numpy(np.int64)
        )
        assert np.array_equal(_group_ids(pdf, ["cust_id"]), expected)

    def test_dtype_is_int64(self):
        from recsys_tfb.io.extract import _group_ids

        assert _group_ids(_fragmented_pdf(), self.GROUP_COLS).dtype == np.int64


def _whole_frame_row_weights(pdf, weight_keys, sample_weights):
    """The pre-#280 implementation, kept verbatim as the parity oracle."""
    keys = pdf[weight_keys[0]].astype(str)
    for k in weight_keys[1:]:
        keys = keys.str.cat(pdf[k].astype(str), sep="|")
    return keys.map(sample_weights).fillna(1.0).to_numpy(dtype=np.float64)


_WEIGHT_PARITY_CASES = {
    # --- cases the dedup fast path handles ---
    "str_and_int_keys": (
        pd.DataFrame({"seg": ["mass", "hnw", "mass", "aff"], "n": [1, 1, 2, 1]}),
        ["seg", "n"],
        {"mass|1": 3.0, "hnw|1": 0.5},
    ),
    "many_rows_few_distinct": (
        pd.DataFrame({"p": ["a", "b", "c"] * 50}),
        ["p"],
        {"a": 2.0, "c": 0.25},
    ),
    "bool_key": (
        pd.DataFrame({"flag": np.array([True, False, True, False])}),
        ["flag"],
        {"True": 4.0},
    ),
    # pd.NA on the fast path: only nullable dtypes can carry a missing value
    # there, and this is the case that fails if the groupby drops it.
    "nullable_int_with_na": (
        pd.DataFrame({"n": pd.array([1, None, 2, 1], dtype="Int64")}),
        ["n"],
        {"1": 5.0, "<NA>": 9.0},
    ),
    # --- cases where value-equality is coarser than string-equality, so the
    # fast path must decline and fall back to the exact per-row build ---
    "float_signed_zero": (
        pd.DataFrame({"x": np.array([0.0, -0.0, 0.0, -0.0])}),
        ["x"],
        {"0.0": 2.0, "-0.0": 9.0},
    ),
    "object_none_and_nan": (
        pd.DataFrame({"p": np.array(["p", None, np.nan, "p"], dtype=object)}),
        ["p"],
        {"None": 2.0, "nan": 9.0},
    ),
    "object_int_and_bool": (
        pd.DataFrame({"v": np.array([1, True, 0, False], dtype=object)}),
        ["v"],
        {"1": 2.0, "True": 9.0},
    ),
    "float_with_nan": (
        pd.DataFrame({"x": np.array([1.0, np.nan, 2.0, 1.0])}),
        ["x"],
        {"1.0": 3.0, "nan": 7.0},
    ),
    # --- edges ---
    "single_row": (pd.DataFrame({"p": ["a"]}), ["p"], {"a": 2.0}),
    "empty_frame": (pd.DataFrame({"p": pd.Series([], dtype=object)}), ["p"], {"a": 2.0}),
    "no_key_matches": (pd.DataFrame({"p": ["a", "b"]}), ["p"], {"zzz": 2.0}),
    "datetime_key_with_nat": (
        pd.DataFrame({"d": pd.to_datetime(["2025-01-31", None, "2025-02-28", None])}),
        ["d"],
        {"2025-01-31": 2.0, "NaT": 8.0},
    ),
    "string_dtype_key": (
        pd.DataFrame({"p": pd.array(["a", "b", "a"], dtype="string")}),
        ["p"],
        {"a": 3.0},
    ),
    # two distinct value combinations that join to one string: the weight is
    # still whatever that single string maps to, for both rows.
    "separator_inside_value": (
        pd.DataFrame({"a": ["x|y", "x", "q"], "b": ["z", "y|z", "r"]}),
        ["a", "b"],
        {"x|y|z": 6.0},
    ),
}


class TestRowWeightsParity:
    @pytest.mark.parametrize("case", sorted(_WEIGHT_PARITY_CASES))
    def test_matches_whole_frame_implementation(self, case):
        from recsys_tfb.io.extract import _compute_row_weights

        pdf, keys, table = _WEIGHT_PARITY_CASES[case]
        got = _compute_row_weights(pdf, keys, table)
        want = _whole_frame_row_weights(pdf, keys, table)
        np.testing.assert_array_equal(got, want)
        assert got.dtype == np.float64

    def test_source_frame_is_not_consolidated(self):
        from recsys_tfb.io.extract import _compute_row_weights

        pdf = _fragmented_pdf()
        pdf["prod_name"] = ["a", "b"] * (len(pdf) // 2)
        before = pdf._mgr.nblocks
        _compute_row_weights(pdf, ["prod_name"], {"a": 2.0})
        assert pdf._mgr.nblocks == before


class TestWeightKeyFastPathGuard:
    """The fast path groups rows by *value* and builds one string key per
    group, so it is only exact when equal values always stringify the same."""

    @pytest.mark.parametrize(
        "col",
        [
            pd.Series(["a", "b", "a"]),
            pd.Series([1, 2, 3]),
            pd.Series([True, False]),
            pd.Series(pd.array([1, None], dtype="Int64")),
            pd.Series(["a", "b"], dtype="string"),
            pd.Series(pd.to_datetime(["2025-01-31", "2025-02-28", None])),
            pd.Series(pd.to_timedelta([1, 2], unit="D")),
        ],
    )
    def test_taken_for_faithful_columns(self, col):
        from recsys_tfb.io.extract import _weight_key_frame

        assert _weight_key_frame(pd.DataFrame({"k": col}), ["k"]) is not None

    @pytest.mark.parametrize(
        "col",
        [
            pd.Series([1.0, 2.0]),                                  # -0.0 vs 0.0
            pd.Series(np.array(["a", None], dtype=object)),         # None vs nan
            pd.Series(np.array([1, True], dtype=object)),           # 1 vs True
            pd.Series(["a", "b"]).astype("category"),               # not scanned
        ],
    )
    def test_declined_for_unfaithful_columns(self, col):
        from recsys_tfb.io.extract import _weight_key_frame

        assert _weight_key_frame(pd.DataFrame({"k": col}), ["k"]) is None

    def test_declines_when_any_key_column_is_unfaithful(self):
        from recsys_tfb.io.extract import _weight_key_frame

        pdf = pd.DataFrame({"ok": ["a", "b"], "bad": [1.0, 2.0]})
        assert _weight_key_frame(pdf, ["ok", "bad"]) is None


class TestZeroMatchDiagnosticKeys:
    """The zero-match WARNING must still name the same first five distinct data
    keys, now derived from the deduped frame instead of a per-row string."""

    def _pdf(self):
        return pd.DataFrame({"p": ["e", "a", "b", "a", "c", "d", "e", "f"]})

    def test_same_five_keys_as_whole_frame_build(self):
        from recsys_tfb.io.extract import _sample_data_keys, composite_key_series

        pdf = self._pdf()
        want = composite_key_series(pdf, ["p"]).drop_duplicates().head(5).tolist()
        assert _sample_data_keys(pdf, ["p"]) == want
        assert want == ["e", "a", "b", "c", "d"]

    def test_dedups_value_combinations_that_join_to_one_string(self):
        """A key value containing '|' makes two distinct rows share a string;
        the replaced code deduped on the string, so this one must too."""
        from recsys_tfb.io.extract import _sample_data_keys, composite_key_series

        pdf = pd.DataFrame({"a": ["x|y", "x", "q"], "b": ["z", "y|z", "r"]})
        want = composite_key_series(pdf, ["a", "b"]).drop_duplicates().head(5).tolist()
        assert want == ["x|y|z", "q|r"]
        assert _sample_data_keys(pdf, ["a", "b"]) == want

    def test_source_frame_is_not_consolidated(self):
        from recsys_tfb.io.extract import _sample_data_keys

        pdf = _fragmented_pdf()
        pdf["prod_name"] = ["a", "b"] * (len(pdf) // 2)
        before = pdf._mgr.nblocks
        _sample_data_keys(pdf, ["prod_name"])
        assert pdf._mgr.nblocks == before

    def test_falls_back_for_unfaithful_column(self):
        from recsys_tfb.io.extract import _sample_data_keys, composite_key_series

        pdf = pd.DataFrame({"x": np.array([1.0, np.nan, 2.0, 1.0])})
        want = composite_key_series(pdf, ["x"]).drop_duplicates().head(5).tolist()
        assert _sample_data_keys(pdf, ["x"]) == want


# ---------------------------------------------------------------------------
# Streamed read (#284) — one pre-allocated matrix, byte-budgeted batches, B9
#
# The old path materialised the feature data three times (the frame pandas
# built, the ``pdf[feature_cols]`` copy, the matrix ``.values`` flattened it
# into). The replacement must produce the *same* matrix, so the first test
# below is a parity test against an inlined copy of that implementation; the
# rest pin the properties that make the new one fit in the driver.
# ---------------------------------------------------------------------------


def _pre284_X(pdf: pd.DataFrame, prep_meta: dict, parameters: dict) -> np.ndarray:
    """The pre-#284 matrix build, inlined: slice, encode, flatten."""
    from recsys_tfb.core.schema import get_schema

    feature_cols = prep_meta["feature_columns"]
    identity_cols = get_schema(parameters)["identity_columns"]
    X_df = pdf[feature_cols].copy()
    deferred = [
        c for c in prep_meta["categorical_columns"]
        if c in identity_cols and c in X_df.columns
    ]
    for col in deferred:
        X_df[col] = pd.Categorical(
            X_df[col], categories=prep_meta["category_mappings"][col]
        ).codes
    return X_df.values


def _wide_df(n_rows: int = 37, n_feats: int = 6, seed: int = 0) -> pd.DataFrame:
    """A model_input-shaped frame: homogeneous float32 features + identity cols."""
    rng = np.random.default_rng(seed)
    cols = {
        "snap_date": pd.to_datetime(["2025-01-31"] * n_rows),
        "cust_id": [f"c{i // 2}" for i in range(n_rows)],
        "prod_name": [["fund", "ccard"][i % 2] for i in range(n_rows)],
        "label": (rng.random(n_rows) < 0.3).astype(np.int64),
    }
    for j in range(n_feats):
        cols[f"f{j}"] = rng.random(n_rows).astype(np.float32)
    return pd.DataFrame(cols)


def _wide_meta(n_feats: int = 6) -> dict:
    return {
        "feature_columns": [f"f{j}" for j in range(n_feats)] + ["prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }


_WIDE_PARAMS = {
    "schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"],
        "item": "prod_name", "label": "label"}}
}


class TestStreamedMatrixParity:
    def test_bit_identical_to_the_pre_284_build(self, tmp_path: Path) -> None:
        from recsys_tfb.io.extract import extract_Xy

        df = _wide_df()
        X, y = extract_Xy(_make_handle(tmp_path, df), _wide_meta(), _WIDE_PARAMS)
        expected = _pre284_X(df, _wide_meta(), _WIDE_PARAMS)

        assert X.dtype == expected.dtype
        assert X.tobytes() == expected.tobytes()
        np.testing.assert_array_equal(y, df["label"].to_numpy())

    def test_bit_identical_across_many_batches(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """A budget of a few rows per batch must land the same bytes.

        The failure this shape has that a single-batch read cannot: a batch
        written at the wrong row offset. ``n_batches`` is asserted rather than
        assumed — shrinking the budget proved to be a no-op once already (the
        default argument had frozen it at import), and the test passed anyway
        because one batch is trivially in the right place.
        """
        import recsys_tfb.io.extract as extract_mod

        df = _wide_df(n_rows=101)
        handle = _make_handle(tmp_path, df)
        one_shot, _ = extract_mod.extract_Xy(handle, _wide_meta(), _WIDE_PARAMS)

        # 7 columns x 4 B = 28 B/row -> 3 rows per batch, so ~34 batches.
        monkeypatch.setattr(extract_mod, "STREAM_BATCH_BYTES", 100)
        calls = _spy_on_reads(monkeypatch)
        many, _ = extract_mod.extract_Xy(handle, _wide_meta(), _WIDE_PARAMS)

        assert calls[0]["batch_size"] == 3
        assert calls[0]["n_batches"] > 30
        assert many.tobytes() == one_shot.tobytes()

    def test_deferred_cat_is_coded_in_X_but_raw_in_items(
        self, tmp_path: Path
    ) -> None:
        """prod_name is a feature *and* the item column, and they disagree.

        The matrix needs its integer code; the caller writing partition names
        needs the original string. Encoding per batch must not cost the raw
        values — the inference side reads ``items`` to name its partitions.
        """
        from recsys_tfb.io.extract import extract_Xy_with_groups

        df = _wide_df(n_rows=8)
        X, y, g, items = extract_Xy_with_groups(
            _make_handle(tmp_path, df), _wide_meta(), _WIDE_PARAMS,
            with_items=True,
        )
        assert list(items) == list(df["prod_name"])
        # last feature column is prod_name: fund=0, ccard=1
        np.testing.assert_array_equal(
            X[:, -1], np.array([0.0, 1.0] * 4, dtype=X.dtype)
        )


class TestStreamBatchRows:
    def test_rows_come_from_the_byte_budget_and_the_width(self) -> None:
        from recsys_tfb.io.extract import stream_batch_rows

        budget = 64 * 1024**2
        assert stream_batch_rows(1000, 4, budget) == budget // 4000
        assert stream_batch_rows(50, 4, budget) == budget // 200

    def test_twenty_times_the_columns_is_a_twentieth_of_the_rows(self) -> None:
        """The property a hard-coded row count cannot have."""
        from recsys_tfb.io.extract import stream_batch_rows

        wide = stream_batch_rows(1000, 4)
        narrow = stream_batch_rows(50, 4)
        # Not exactly 20x: the budget floor-divides, so the wide case loses a
        # partial row. Inverse proportionality is the property, not the integer.
        assert 19.99 < narrow / wide < 20.01

    def test_itemsize_halves_the_rows(self) -> None:
        from recsys_tfb.io.extract import stream_batch_rows

        assert stream_batch_rows(100, 8) == stream_batch_rows(100, 4) // 2

    def test_never_zero_however_wide(self) -> None:
        from recsys_tfb.io.extract import stream_batch_rows

        assert stream_batch_rows(1_000_000, 8, budget=16) == 1


class _SpyDataset:
    """Proxy that records the read requests made through it."""

    def __init__(self, ds, calls):
        self._ds = ds
        self._calls = calls

    def __getattr__(self, name):
        return getattr(self._ds, name)

    def to_batches(self, **kwargs):
        record = dict(kwargs, n_batches=0)
        self._calls.append(record)

        def counted():
            for batch in self._ds.to_batches(**kwargs):
                record["n_batches"] += 1
                yield batch

        return counted()


def _spy_on_reads(monkeypatch) -> list:
    import pyarrow.dataset as pads

    calls: list = []
    real = pads.dataset
    monkeypatch.setattr(
        pads, "dataset", lambda *a, **k: _SpyDataset(real(*a, **k), calls)
    )
    return calls


class TestStreamedReadRequestsOnlyWhatItUses:
    def test_unused_columns_are_never_requested(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        from recsys_tfb.io.extract import extract_Xy_with_groups

        df = _wide_df(n_rows=10)
        df["ignored_a"] = np.arange(10, dtype=np.int64)
        df["ignored_b"] = ["x"] * 10
        calls = _spy_on_reads(monkeypatch)

        extract_Xy_with_groups(
            _make_handle(tmp_path, df), _wide_meta(), _WIDE_PARAMS
        )

        assert len(calls) == 1
        requested = set(calls[0]["columns"])
        assert requested == {
            "f0", "f1", "f2", "f3", "f4", "f5", "prod_name",  # features
            "label", "snap_date", "cust_id",                  # aux
        }
        assert "ignored_a" not in requested
        assert "ignored_b" not in requested

    def test_weight_keys_are_requested_only_when_asked_for(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        from recsys_tfb.io.extract import extract_Xy

        df = _wide_df(n_rows=10)
        df["cust_segment_typ"] = ["mass"] * 10
        params = {
            **_WIDE_PARAMS,
            "training": {
                "sample_weights": {"mass": 2.0},
                "sample_weight_keys": ["cust_segment_typ"],
            },
        }
        handle = _make_handle(tmp_path, df)

        calls = _spy_on_reads(monkeypatch)
        extract_Xy(handle, _wide_meta(), params)
        assert "cust_segment_typ" not in set(calls[0]["columns"])

        calls.clear()
        _, _, w = extract_Xy(handle, _wide_meta(), params, with_weights=True)
        assert "cust_segment_typ" in set(calls[0]["columns"])
        np.testing.assert_array_equal(w, np.full(10, 2.0))

    def test_batch_size_tracks_the_column_count(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        from recsys_tfb.io.extract import extract_Xy

        calls = _spy_on_reads(monkeypatch)
        for n_feats in (2, 20):
            sub_dir = tmp_path / f"n{n_feats}"
            sub_dir.mkdir()
            extract_Xy(
                _make_handle(sub_dir, _wide_df(n_feats=n_feats)),
                _wide_meta(n_feats),
                _WIDE_PARAMS,
            )
        narrow, wide = (c["batch_size"] for c in calls)
        assert narrow > wide


class TestStreamedMatrixDtype:
    def test_dtype_is_the_declared_storage_type(self, tmp_path: Path) -> None:
        from recsys_tfb.io.extract import extract_Xy

        X, _ = extract_Xy(
            _make_handle(tmp_path, _wide_df()), _wide_meta(), _WIDE_PARAMS
        )
        assert X.dtype == np.dtype("float32")

    def test_declared_float64_gives_a_float64_matrix(self, tmp_path: Path) -> None:
        from recsys_tfb.io.extract import extract_Xy

        df = _wide_df()
        for c in [c for c in df.columns if c.startswith("f")]:
            df[c] = df[c].astype(np.float64)
        params = {
            **_WIDE_PARAMS,
            "dataset": {"numeric_feature_storage_type": "float64"},
        }
        X, _ = extract_Xy(_make_handle(tmp_path, df), _wide_meta(), params)
        assert X.dtype == np.dtype("float64")

    def test_dtype_survives_wider_non_feature_columns(
        self, tmp_path: Path
    ) -> None:
        """int64 label, string item, bool carry — none of them widen the matrix.

        The pre-#284 build flattened a frame, so pandas picked one common dtype
        across whatever happened to be in it. This one allocates from the
        declaration, so only the *feature* columns can speak to it at all.
        """
        from recsys_tfb.io.extract import extract_Xy_with_groups

        df = _wide_df()
        df["label"] = df["label"].astype(np.int64)
        df["some_flag"] = np.tile([True, False], len(df) // 2 + 1)[: len(df)]
        X, y, _ = extract_Xy_with_groups(
            _make_handle(tmp_path, df), _wide_meta(), _WIDE_PARAMS
        )
        assert X.dtype == np.dtype("float32")
        assert y.dtype == np.dtype("int64")


class TestExtractXyB9Backstop:
    """A feature column that is not the declared storage type stops the read.

    Every case here must fail *before* any data is read — that is the whole
    point at production scale, where the matrix the read would allocate does not
    fit in the driver at all.
    """

    def _handle_with(self, tmp_path: Path, mutate) -> "ParquetHandle":
        df = _wide_df()
        mutate(df)
        return _make_handle(tmp_path, df)

    @pytest.mark.parametrize(
        "mutate, needle",
        [
            (lambda df: df.__setitem__("f2", df["f2"].astype(np.int64)), "int64"),
            (lambda df: df.__setitem__("f2", df["f2"].astype(np.float64)),
             "float64"),
            (lambda df: df.__setitem__("f2", df["f2"] > 0.5), "bool"),
        ],
        ids=["heterogeneous_int64", "homogeneous_but_float64", "boolean"],
    )
    def test_wrong_storage_type_raises(
        self, tmp_path: Path, monkeypatch, mutate, needle
    ) -> None:
        from recsys_tfb.core.consistency import DataConsistencyError
        from recsys_tfb.io.extract import extract_Xy

        handle = self._handle_with(tmp_path, mutate)
        calls = _spy_on_reads(monkeypatch)

        with pytest.raises(DataConsistencyError) as excinfo:
            extract_Xy(handle, _wide_meta(), _WIDE_PARAMS)

        message = str(excinfo.value)
        assert "B9" in message and "'f2'" in message and needle in message
        # ... and nothing was read to find that out.
        assert calls == []

    def test_every_wrong_column_is_named_at_once(self, tmp_path: Path) -> None:
        from recsys_tfb.core.consistency import DataConsistencyError
        from recsys_tfb.io.extract import extract_Xy

        def mutate(df):
            df["f1"] = df["f1"].astype(np.float64)
            df["f3"] = df["f3"].astype(np.int64)

        with pytest.raises(DataConsistencyError) as excinfo:
            extract_Xy(self._handle_with(tmp_path, mutate), _wide_meta(),
                       _WIDE_PARAMS)
        message = str(excinfo.value)
        assert "'f1'" in message and "'f3'" in message
        assert "2 issue(s)" in message

    def test_deferred_identity_categorical_is_exempt(
        self, tmp_path: Path
    ) -> None:
        """prod_name is a string feature column by contract, not a violation."""
        from recsys_tfb.io.extract import extract_Xy

        X, _ = extract_Xy(
            _make_handle(tmp_path, _wide_df()), _wide_meta(), _WIDE_PARAMS
        )
        assert X.shape == (37, 7)


# ---------------------------------------------------------------------------
# The training cache is a hive-partitioned DIRECTORY, not one flat file
#
# ``populate_cache_from_hive`` writes ``<root>/snap_date=.../prod_name=.../*.parquet``
# (its docstring says so), so ``schema.time`` and ``schema.item`` live in
# directory names, not in the files. Every other fixture in this module writes
# one flat file via ``df.to_parquet(path)`` — which is why a reader that opens
# the root without hive partitioning passes the whole suite and then cannot find
# ``prod_name`` in production.
# ---------------------------------------------------------------------------


def _make_partitioned_handle(tmp_path: Path, df: pd.DataFrame, parts: list):
    from recsys_tfb.io.handles import ParquetHandle

    root = tmp_path / "cache_root"
    df.to_parquet(root, partition_cols=parts)
    return ParquetHandle(path=str(root))


class TestHivePartitionedCacheRoot:
    """snap_date / prod_name come back even though they are directory names."""

    def _df(self):
        return pd.DataFrame({
            "snap_date": ["2025-01-31"] * 6,
            "prod_name": ["fund", "ccard", "savings"] * 2,
            "cust_id": ["c1", "c1", "c1", "c2", "c2", "c2"],
            "f0": np.arange(6, dtype=np.float32),
            "f1": np.arange(6, dtype=np.float32) * 2,
            "label": [1, 0, 0, 0, 1, 0],
        })

    def _meta(self):
        return {
            "feature_columns": ["f0", "f1", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
        }

    def test_group_and_item_columns_survive_the_partitioning(
        self, tmp_path: Path
    ) -> None:
        from recsys_tfb.io.extract import extract_Xy_with_groups

        handle = _make_partitioned_handle(
            tmp_path, self._df(), ["snap_date", "prod_name"])

        X, y, groups, items = extract_Xy_with_groups(
            handle, self._meta(), _WIDE_PARAMS, with_items=True,
        )

        assert X.shape == (6, 3)
        assert X.dtype == np.dtype("float32")
        # two customers, one month -> two query groups
        assert len(set(groups.tolist())) == 2
        # the item column is a directory name, and still comes back as a name
        assert sorted(set(items)) == ["ccard", "fund", "savings"]
        # ...and its matrix column is the code, not the string
        assert set(X[:, 2].tolist()) == {0.0, 1.0, 2.0}

    def test_matches_the_flat_file_read_of_the_same_rows(
        self, tmp_path: Path
    ) -> None:
        """Partitioning is a storage layout, not a change of content.

        Row *order* differs (a partitioned read comes back grouped by partition),
        so this compares the rows as a set of (feature row, label, item) tuples.
        """
        from recsys_tfb.io.extract import extract_Xy_with_groups

        df = self._df()
        flat_dir = tmp_path / "flat"
        flat_dir.mkdir()
        flat = _make_handle(flat_dir, df)
        part = _make_partitioned_handle(
            tmp_path, df, ["snap_date", "prod_name"])

        def rows(handle):
            X, y, g, items = extract_Xy_with_groups(
                handle, self._meta(), _WIDE_PARAMS, with_items=True)
            return sorted(
                (tuple(X[i]), int(y[i]), str(items[i])) for i in range(len(y))
            )

        assert rows(part) == rows(flat)

# ---------------------------------------------------------------------------
# Disk-backed matrix (#285) — the same bytes, mapped from a file
#
# HPO holds the val matrix for the whole search (37-89 GiB in production), so
# it reads with ``on_disk_label=...``. These pin that the parameter changes
# *where the bytes live* and nothing else: same values, same dtype, same
# batching. The label is what an operator reads if the disk is too small, so
# production passes ``"hpo_val_matrix"``; these pass ``"unit"``.
#
# The scratch root is redirected the same way ``STREAM_BATCH_BYTES`` is — by
# patching the module attribute the allocator reads at call time — so the API
# does not grow a parameter that only tests would ever pass.
# ---------------------------------------------------------------------------


@pytest.fixture
def scratch(tmp_path: Path, monkeypatch) -> Path:
    from recsys_tfb.io import disk_matrix

    root = tmp_path / "scratch"
    monkeypatch.setattr(disk_matrix, "SCRATCH_ROOT", root)
    return root


class TestOnDiskMatrix:
    def test_bit_identical_to_the_in_memory_read(
        self, tmp_path: Path, scratch: Path
    ) -> None:
        from recsys_tfb.io.extract import extract_Xy_with_groups

        df = _wide_df(n_rows=53)
        handle = _make_handle(tmp_path, df)
        in_memory, y_m, g_m = extract_Xy_with_groups(
            handle, _wide_meta(), _WIDE_PARAMS)
        on_disk, y_d, g_d = extract_Xy_with_groups(
            handle, _wide_meta(), _WIDE_PARAMS, on_disk_label="unit")

        assert on_disk.dtype == in_memory.dtype
        assert np.array_equal(on_disk, in_memory)
        assert on_disk.tobytes() == in_memory.tobytes()
        np.testing.assert_array_equal(y_d, y_m)
        np.testing.assert_array_equal(g_d, g_m)

    def test_the_matrix_is_mapped_not_heap_allocated(
        self, tmp_path: Path, scratch: Path
    ) -> None:
        """Without this, every other assertion here passes on a plain array."""
        from recsys_tfb.io.extract import extract_Xy_with_groups

        X, _, _ = extract_Xy_with_groups(
            _make_handle(tmp_path, _wide_df(n_rows=9)), _wide_meta(),
            _WIDE_PARAMS, on_disk_label="unit")

        assert isinstance(X, np.memmap)

    def test_still_bit_identical_across_many_batches(
        self, tmp_path: Path, scratch: Path, monkeypatch
    ) -> None:
        """Writing a batch at the wrong row offset is the failure a mapped
        matrix can have that a single-batch read cannot show."""
        import recsys_tfb.io.extract as extract_mod

        df = _wide_df(n_rows=101)
        handle = _make_handle(tmp_path, df)
        one_shot, _, _ = extract_mod.extract_Xy_with_groups(
            handle, _wide_meta(), _WIDE_PARAMS)

        monkeypatch.setattr(extract_mod, "STREAM_BATCH_BYTES", 100)
        calls = _spy_on_reads(monkeypatch)
        many, _, _ = extract_mod.extract_Xy_with_groups(
            handle, _wide_meta(), _WIDE_PARAMS, on_disk_label="unit")

        assert calls[0]["n_batches"] > 30
        assert many.tobytes() == one_shot.tobytes()

    def test_off_by_default_so_no_caller_spills_by_accident(
        self, tmp_path: Path, scratch: Path
    ) -> None:
        """Only HPO holds a matrix across many fits; the ``.bin`` prep, the
        refit and the calibration read each hold theirs for one."""
        from recsys_tfb.io.extract import extract_Xy_with_groups

        X, _, _ = extract_Xy_with_groups(
            _make_handle(tmp_path, _wide_df(n_rows=9)), _wide_meta(), _WIDE_PARAMS)

        assert not isinstance(X, np.memmap)
        assert not scratch.exists()

    def test_no_scratch_file_survives_the_read(
        self, tmp_path: Path, scratch: Path
    ) -> None:
        from recsys_tfb.io.extract import extract_Xy_with_groups

        X, _, _ = extract_Xy_with_groups(
            _make_handle(tmp_path, _wide_df(n_rows=9)), _wide_meta(),
            _WIDE_PARAMS, on_disk_label="unit")

        assert list(scratch.rglob("*")) == []
        assert X.shape == (9, 7)
