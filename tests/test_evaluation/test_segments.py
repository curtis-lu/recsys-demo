"""Tests for evaluation.segments — external segment-source loader only.

The per-segment compute / table / plot helpers moved to
``metrics_spark.compute_all_metrics`` (driven by parameters.evaluation.
segment_columns); this file now only covers
``load_and_join_segment_sources``.
"""

import numpy as np
import pandas as pd

from recsys_tfb.evaluation.segments import load_and_join_segment_sources


def _make_labels(n_customers=30, segments=None, seed=42):
    rng = np.random.RandomState(seed)
    if segments is None:
        segments = ["mass", "affluent", "hnw"]

    snap_date = "20240331"
    rows = []
    for i in range(n_customers):
        seg = segments[i % len(segments)]
        rows.append({
            "snap_date": snap_date,
            "cust_id": f"C{i:04d}",
            "prod_name": "exchange_fx",
            "label": int(rng.rand() > 0.6),
            "cust_segment_typ": seg,
        })
    return pd.DataFrame(rows)


class TestLoadAndJoinSegmentSources:
    def test_join_single_source(self, tmp_path):
        labels = _make_labels()
        seg_df = labels[["cust_id", "snap_date"]].drop_duplicates().copy()
        seg_df["holding_combo"] = [f"combo_{i % 3}" for i in range(len(seg_df))]
        seg_path = tmp_path / "holding_combo.parquet"
        seg_df.to_parquet(seg_path)

        segment_sources = {
            "holding_combo": {
                "filepath": str(seg_path),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "holding_combo",
            }
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        assert "holding_combo" in result.columns
        assert result["holding_combo"].notna().all()

    def test_missing_file_skipped(self, tmp_path):
        labels = _make_labels()
        segment_sources = {
            "missing_seg": {
                "filepath": str(tmp_path / "nonexistent.parquet"),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "missing_col",
            }
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        # Should return labels unchanged (no new column)
        assert "missing_col" not in result.columns

    def test_partial_join_coverage(self, tmp_path):
        labels = _make_labels(n_customers=30)
        unique = labels[["cust_id", "snap_date"]].drop_duplicates()
        half = unique.iloc[:15].copy()
        half["risk_level"] = "high"
        seg_path = tmp_path / "risk.parquet"
        half.to_parquet(seg_path)

        segment_sources = {
            "risk": {
                "filepath": str(seg_path),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "risk_level",
            }
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        assert "risk_level" in result.columns
        assert result["risk_level"].notna().sum() > 0
        assert result["risk_level"].isna().sum() > 0

    def test_multiple_sources(self, tmp_path):
        labels = _make_labels()
        unique = labels[["cust_id", "snap_date"]].drop_duplicates()

        s1 = unique.copy()
        s1["holding_combo"] = "combo_a"
        s1.to_parquet(tmp_path / "combo.parquet")

        s2 = unique.copy()
        s2["risk_level"] = "medium"
        s2.to_parquet(tmp_path / "risk.parquet")

        segment_sources = {
            "holding_combo": {
                "filepath": str(tmp_path / "combo.parquet"),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "holding_combo",
            },
            "risk_level": {
                "filepath": str(tmp_path / "risk.parquet"),
                "key_columns": ["cust_id", "snap_date"],
                "segment_column": "risk_level",
            },
        }
        result = load_and_join_segment_sources(labels, segment_sources)
        assert "holding_combo" in result.columns
        assert "risk_level" in result.columns
