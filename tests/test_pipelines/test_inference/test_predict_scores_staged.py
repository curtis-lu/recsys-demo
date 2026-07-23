import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.pipelines.inference.nodes_spark import (
    _predict_chunk_staged,
    _raise_if_all_rows_skipped,
)


def _tiny_lgb(seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(60) < 0.4).astype(int)
    X = np.column_stack([rng.normal(loc=y, size=60), rng.normal(size=60)])
    a = LightGBMAdapter()
    a.train(X, y, None, None, {"objective": "binary", "verbosity": -1,
                               "num_threads": 1, "num_leaves": 4,
                               "num_iterations": 8,
                               "early_stopping_rounds": 0})
    return a


def _staged():
    m = StagedModelAdapter()
    m.add_group("A", _tiny_lgb(0), meta={})
    m.set_partition_keys(["seg"])
    return m


class TestPredictChunkStaged:
    def test_known_groups_all_scored(self):
        pdf = pd.DataFrame({"f1": [0.1, 0.2], "f2": [0.0, 0.1],
                            "seg": ["A", "A"]})
        X = pdf[["f1", "f2"]].values
        scores, keep, missing = _predict_chunk_staged(_staged(), X, pdf)
        assert keep.all() and len(scores) == 2 and missing == {}

    def test_missing_group_rows_dropped_and_counted(self):
        pdf = pd.DataFrame({"f1": [0.1, 0.2, 0.3], "f2": [0.0] * 3,
                            "seg": ["A", "ZZ", "ZZ"]})
        X = pdf[["f1", "f2"]].values
        scores, keep, missing = _predict_chunk_staged(_staged(), X, pdf)
        assert keep.tolist() == [True, False, False]
        assert missing == {"ZZ": 2}
        assert np.isfinite(scores[keep]).all()

    def test_missing_partition_key_column_fails_fast(self):
        pdf = pd.DataFrame({"f1": [0.1], "f2": [0.0]})  # 無 seg 欄
        X = pdf[["f1", "f2"]].values
        with pytest.raises(KeyError, match="seg"):
            _predict_chunk_staged(_staged(), X, pdf)


class TestRaiseIfAllRowsSkipped:
    """predict_scores' guard: concat over all-skipped chunks must fail loud,
    not silently produce a zero-row score table (mean() would be NaN)."""

    def test_all_rows_skipped_raises(self):
        # Only group "A" is known to the model; every scoring row is "ZZ".
        pdf = pd.DataFrame({"f1": [0.1, 0.2, 0.3], "f2": [0.0] * 3,
                            "seg": ["ZZ", "ZZ", "ZZ"]})
        X = pdf[["f1", "f2"]].values
        scores, keep, missing = _predict_chunk_staged(_staged(), X, pdf)
        assert not keep.any()
        identity_pdf = pdf[["seg"]].copy()
        identity_pdf["score"] = scores
        result_pdf = identity_pdf.iloc[keep.nonzero()[0]].reset_index(drop=True)
        assert result_pdf.empty

        with pytest.raises(ValueError, match="all rows skipped"):
            _raise_if_all_rows_skipped(result_pdf, missing)

    def test_some_rows_scored_does_not_raise(self):
        # Mixed: one row routes to known group "A", two miss — must NOT raise.
        pdf = pd.DataFrame({"f1": [0.1, 0.2, 0.3], "f2": [0.0] * 3,
                            "seg": ["A", "ZZ", "ZZ"]})
        X = pdf[["f1", "f2"]].values
        scores, keep, missing = _predict_chunk_staged(_staged(), X, pdf)
        identity_pdf = pdf[["seg"]].copy()
        identity_pdf["score"] = scores
        result_pdf = identity_pdf.iloc[keep.nonzero()[0]].reset_index(drop=True)
        assert not result_pdf.empty

        _raise_if_all_rows_skipped(result_pdf, missing)  # must not raise
