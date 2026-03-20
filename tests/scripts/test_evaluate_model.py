"""Integration tests for scripts/evaluate_model.py CLI."""

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

# Import from scripts directory
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))
from evaluate_model import _load_eval_params, _parse_k_values_str, _run_analysis, app

runner = CliRunner()


@pytest.fixture
def test_data_dir():
    """Create a temporary data directory with synthetic predictions and labels."""
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir)
        rng = np.random.RandomState(42)
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        segments = ["mass", "affluent"]
        snap_date = "20240331"
        model_version = "abc12345"
        n_customers = 20

        # Create label_table
        label_rows = []
        for i in range(n_customers):
            for prod in products:
                label_rows.append({
                    "snap_date": snap_date,
                    "cust_id": f"C{i:04d}",
                    "prod_name": prod,
                    "label": int(rng.rand() > 0.6),
                    "cust_segment_typ": segments[i % 2],
                })
        labels = pd.DataFrame(label_rows)
        labels.to_parquet(data_dir / "label_table.parquet", index=False)

        # Create ranked_predictions
        pred_rows = []
        for i in range(n_customers):
            scores = rng.rand(len(products))
            for j, prod in enumerate(products):
                pred_rows.append({
                    "snap_date": snap_date,
                    "cust_id": f"C{i:04d}",
                    "prod_name": prod,
                    "score": scores[j],
                    "rank": 0,
                })
        preds = pd.DataFrame(pred_rows)
        preds["rank"] = preds.groupby(["snap_date", "cust_id"])["score"].rank(
            method="first", ascending=False
        ).astype(int)

        inference_dir = data_dir / "inference" / model_version / snap_date
        inference_dir.mkdir(parents=True)
        preds.to_parquet(inference_dir / "ranked_predictions.parquet", index=False)

        yield data_dir, model_version, snap_date


class TestAnalyze:
    def test_basic_analyze(self, test_data_dir):
        data_dir, model_version, snap_date = test_data_dir
        result = runner.invoke(
            app,
            ["analyze", model_version, "--snap-date", snap_date, "--data-dir", str(data_dir)],
        )
        assert result.exit_code == 0
        assert "map@" in result.output

        # Check output files exist
        eval_dir = data_dir / "evaluation" / model_version / snap_date
        assert (eval_dir / "report.html").exists()
        assert (eval_dir / "metrics.json").exists()

    def test_date_format_conversion(self, test_data_dir):
        data_dir, model_version, _ = test_data_dir
        result = runner.invoke(
            app,
            ["analyze", model_version, "--snap-date", "2024-03-31", "--data-dir", str(data_dir)],
        )
        assert result.exit_code == 0

    def test_missing_predictions(self, test_data_dir):
        data_dir, _, _ = test_data_dir
        result = runner.invoke(
            app,
            ["analyze", "nonexist", "--snap-date", "20240331", "--data-dir", str(data_dir)],
        )
        assert result.exit_code != 0

    def test_metrics_json_roundtrip(self, test_data_dir):
        data_dir, model_version, snap_date = test_data_dir
        runner.invoke(
            app,
            ["analyze", model_version, "--snap-date", snap_date, "--data-dir", str(data_dir)],
        )
        metrics_path = data_dir / "evaluation" / model_version / snap_date / "metrics.json"
        metrics = json.loads(metrics_path.read_text())
        assert "overall" in metrics
        assert any(k.startswith("map@") for k in metrics["overall"])

    def test_k_values_cli_override(self, test_data_dir):
        data_dir, model_version, snap_date = test_data_dir
        result = runner.invoke(
            app,
            [
                "analyze", model_version,
                "--snap-date", snap_date,
                "--data-dir", str(data_dir),
                "--k-values", "3,10",
            ],
        )
        assert result.exit_code == 0
        metrics_path = data_dir / "evaluation" / model_version / snap_date / "metrics.json"
        metrics = json.loads(metrics_path.read_text())
        assert "precision@3" in metrics["overall"]
        assert "precision@10" in metrics["overall"]


class TestCompare:
    def test_compare_vs_global_baseline(self, test_data_dir):
        data_dir, model_version, snap_date = test_data_dir
        result = runner.invoke(
            app,
            [
                "compare", model_version,
                "--baseline", "global_popularity",
                "--snap-date", snap_date,
                "--data-dir", str(data_dir),
            ],
        )
        assert result.exit_code == 0
        assert "map@" in result.output

    def test_compare_vs_segment_baseline(self, test_data_dir):
        data_dir, model_version, snap_date = test_data_dir
        result = runner.invoke(
            app,
            [
                "compare", model_version,
                "--baseline", "segment_popularity",
                "--snap-date", snap_date,
                "--data-dir", str(data_dir),
            ],
        )
        assert result.exit_code == 0

    def test_compare_no_model_b_no_baseline(self, test_data_dir):
        data_dir, model_version, snap_date = test_data_dir
        result = runner.invoke(
            app,
            ["compare", model_version, "--snap-date", snap_date, "--data-dir", str(data_dir)],
        )
        assert result.exit_code != 0


class TestLoadEvalParams:
    def test_load_from_yaml(self, tmp_path):
        params = {
            "evaluation": {
                "k_values": [3, "all"],
                "segment_columns": ["risk_level"],
                "segment_sources": {
                    "combo": {
                        "filepath": "data/combo.parquet",
                        "key_columns": ["cust_id", "snap_date"],
                        "segment_column": "holding_combo",
                    }
                },
            }
        }
        params_file = tmp_path / "params.yaml"
        with open(params_file, "w") as f:
            yaml.dump(params, f)

        result = _load_eval_params(str(params_file))
        assert result["k_values"] == [3, "all"]
        assert result["segment_columns"] == ["risk_level"]
        assert "combo" in result["segment_sources"]

    def test_missing_file_uses_defaults(self, tmp_path):
        result = _load_eval_params(str(tmp_path / "nonexistent.yaml"))
        assert result["k_values"] == [5, "all"]
        assert result["segment_columns"] == ["cust_segment_typ"]
        assert result["segment_sources"] == {}


class TestParseKValuesStr:
    def test_integers_only(self):
        assert _parse_k_values_str("3,5,10") == [3, 5, 10]

    def test_with_all(self):
        assert _parse_k_values_str("5,all") == [5, "all"]

    def test_single_value(self):
        assert _parse_k_values_str("3") == [3]


class TestRunAnalysisMetricsSummary:
    def test_metrics_summary_has_three_tables(self):
        """Metrics Summary section should contain Overall + Macro + Micro tables."""
        rng = np.random.RandomState(42)
        products = ["exchange_fx", "fund_bond", "fund_stock"]
        snap_date = "20240331"
        n_customers = 20

        pred_rows = []
        label_rows = []
        for i in range(n_customers):
            scores = rng.rand(len(products))
            seg = "mass" if i % 2 == 0 else "affluent"
            for j, prod in enumerate(products):
                pred_rows.append({
                    "snap_date": snap_date,
                    "cust_id": f"C{i:04d}",
                    "prod_name": prod,
                    "score": scores[j],
                    "rank": 0,
                })
                label_rows.append({
                    "snap_date": snap_date,
                    "cust_id": f"C{i:04d}",
                    "prod_name": prod,
                    "label": int(rng.rand() > 0.6),
                    "cust_segment_typ": seg,
                })

        preds = pd.DataFrame(pred_rows)
        preds["rank"] = preds.groupby(["snap_date", "cust_id"])["score"].rank(
            method="first", ascending=False
        ).astype(int)
        labels = pd.DataFrame(label_rows)

        metrics, sections = _run_analysis(
            preds, labels, k_values=[5, "all"],
            segment_columns=["cust_segment_typ"],
        )

        # Find Metrics Summary section
        summary = [s for s in sections if s.title == "Metrics Summary"]
        assert len(summary) == 1
        tables = summary[0].tables
        # Should have 3 tables: Overall, Macro Average, Micro Average
        assert len(tables) == 3

        # First table: Overall (single column)
        assert "Overall" in tables[0].columns

        # Second table: Macro Average (has dimension columns)
        macro_df = tables[1]
        assert "by_product" in macro_df.columns

        # Third table: Micro Average
        micro_df = tables[2]
        assert "by_product" in micro_df.columns

    def test_segment_analysis_sections(self):
        """Each segment column should get its own Segment Analysis section."""
        rng = np.random.RandomState(42)
        products = ["exchange_fx", "fund_bond"]
        snap_date = "20240331"
        n_customers = 10

        pred_rows = []
        label_rows = []
        for i in range(n_customers):
            scores = rng.rand(len(products))
            for j, prod in enumerate(products):
                pred_rows.append({
                    "snap_date": snap_date,
                    "cust_id": f"C{i:04d}",
                    "prod_name": prod,
                    "score": scores[j],
                    "rank": 0,
                })
                label_rows.append({
                    "snap_date": snap_date,
                    "cust_id": f"C{i:04d}",
                    "prod_name": prod,
                    "label": int(rng.rand() > 0.6),
                    "cust_segment_typ": "mass" if i % 2 == 0 else "affluent",
                    "holding_combo": f"combo_{i % 3}",
                })

        preds = pd.DataFrame(pred_rows)
        preds["rank"] = preds.groupby(["snap_date", "cust_id"])["score"].rank(
            method="first", ascending=False
        ).astype(int)
        labels = pd.DataFrame(label_rows)

        _, sections = _run_analysis(
            preds, labels, k_values=[3],
            segment_columns=["cust_segment_typ", "holding_combo"],
        )

        seg_sections = [s for s in sections if s.title.startswith("Segment Analysis")]
        assert len(seg_sections) == 2
        titles = [s.title for s in seg_sections]
        assert "Segment Analysis: Cust Segment Typ" in titles
        assert "Segment Analysis: Holding Combo" in titles
        # Segment sections now use tables instead of figures
        for seg_sec in seg_sections:
            assert len(seg_sec.tables) == 2
            assert seg_sec.table_titles == ["Ranking Metrics", "Dataset Statistics"]
