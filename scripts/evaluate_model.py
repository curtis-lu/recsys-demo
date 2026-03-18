"""CLI for model evaluation — analyze and compare."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import typer
import yaml

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from recsys_tfb.core.versioning import resolve_model_version
from recsys_tfb.evaluation.baselines import (
    generate_global_popularity_baseline,
    generate_segment_popularity_baseline,
)
from recsys_tfb.evaluation.calibration import plot_calibration_curves
from recsys_tfb.evaluation.compare import (
    build_comparison_result,
    plot_comparison_metrics,
    plot_comparison_score_distributions,
)
from recsys_tfb.evaluation.distributions import (
    plot_rank_heatmap,
    plot_score_distributions,
)
from recsys_tfb.evaluation.metrics import compute_all_metrics
from recsys_tfb.evaluation.report import (
    ReportSection,
    generate_html_report,
    save_metrics_json,
    save_report,
)
from recsys_tfb.evaluation.segments import (
    compute_segment_metrics,
    load_and_join_segment_sources,
    plot_segment_charts,
)

app = typer.Typer(help="Model evaluation CLI")

# Default evaluation parameters (used when YAML not found)
_DEFAULT_EVAL_PARAMS = {
    "k_values": [5, "all"],
    "segment_columns": ["cust_segment_typ"],
    "segment_sources": {},
}


def _load_eval_params(params_file: str) -> dict:
    """Load evaluation parameters from YAML file, falling back to defaults."""
    path = Path(params_file)
    if not path.exists():
        return dict(_DEFAULT_EVAL_PARAMS)

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    eval_config = raw.get("evaluation", {})
    return {
        "k_values": eval_config.get("k_values", _DEFAULT_EVAL_PARAMS["k_values"]),
        "segment_columns": eval_config.get(
            "segment_columns", _DEFAULT_EVAL_PARAMS["segment_columns"]
        ),
        "segment_sources": eval_config.get("segment_sources", {}),
    }


def _parse_k_values_str(k_values_str: str) -> list[Union[int, str]]:
    """Parse comma-separated k_values string. Supports 'all' as a value."""
    result = []
    for part in k_values_str.split(","):
        part = part.strip()
        if part == "all":
            result.append("all")
        else:
            result.append(int(part))
    return result


def _parse_snap_date(snap_date: str) -> str:
    """Convert YYYY-MM-DD to YYYYMMDD directory format."""
    if "-" in snap_date:
        return snap_date.replace("-", "")
    return snap_date


def _resolve_version(version: str, data_dir: Path) -> str:
    """Resolve model version aliases (latest/best) to actual hash."""
    models_dir = data_dir / "models"
    if version == "latest":
        latest = models_dir / "latest"
        if latest.is_symlink():
            return latest.resolve().name
        raise FileNotFoundError(f"No 'latest' symlink in {models_dir}")
    elif version == "best":
        return resolve_model_version(models_dir, None)
    return version


def _load_predictions(data_dir: Path, model_version: str, snap_date_dir: str) -> pd.DataFrame:
    """Load ranked_predictions.parquet."""
    path = data_dir / "inference" / model_version / snap_date_dir / "ranked_predictions.parquet"
    if not path.exists():
        typer.echo(f"Error: ranked_predictions not found at {path}", err=True)
        raise typer.Exit(1)
    return pd.read_parquet(path)


def _load_labels(data_dir: Path) -> pd.DataFrame:
    """Load label_table.parquet."""
    path = data_dir / "label_table.parquet"
    if not path.exists():
        typer.echo(f"Error: label_table not found at {path}", err=True)
        raise typer.Exit(1)
    return pd.read_parquet(path)


def _run_analysis(
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    k_values: list[Union[int, str]],
    segment_columns: list[str] | None = None,
    title_prefix: str = "",
) -> tuple[dict, list[ReportSection]]:
    """Run all analysis dimensions and return metrics + report sections."""
    if segment_columns is None:
        segment_columns = []

    # 1. Metrics
    metrics = compute_all_metrics(predictions, labels, k_values=k_values)

    sections = []

    # --- Metrics Summary: Overall + Macro Average + Micro Average ---
    summary_tables = []

    # Overall
    overall_df = pd.DataFrame([metrics["overall"]]).T
    overall_df.columns = ["Overall"]
    summary_tables.append(overall_df)

    # Macro Average
    if metrics["macro_avg"]:
        macro_df = pd.DataFrame(metrics["macro_avg"])
        summary_tables.append(macro_df)

    # Micro Average
    if metrics["micro_avg"]:
        micro_df = pd.DataFrame(metrics["micro_avg"])
        summary_tables.append(micro_df)

    sections.append(
        ReportSection(
            title="Metrics Summary",
            description=(
                "Overall ranking metrics, macro average (unweighted mean across dimensions), "
                "and micro average (query-count-weighted mean)."
            ),
            tables=summary_tables,
        )
    )

    # Per-product metrics table
    if metrics["per_product"]:
        prod_df = pd.DataFrame(metrics["per_product"]).T
        sections.append(
            ReportSection(
                title="Per-Product Metrics",
                description="Metrics broken down by product.",
                tables=[prod_df],
            )
        )

    # 2. Score distributions
    dist_figs = plot_score_distributions(predictions, title_prefix=title_prefix)
    sections.append(
        ReportSection(
            title="Score Distributions",
            description="Histogram and boxplot of prediction scores per product.",
            figures=dist_figs,
        )
    )

    # 3. Rank distribution
    rank_fig = plot_rank_heatmap(predictions, title_prefix=title_prefix)
    sections.append(
        ReportSection(
            title="Rank Distribution",
            description="Heatmap showing how often each product appears at each rank position.",
            figures=[rank_fig],
        )
    )

    # 4. Calibration
    cal_fig = plot_calibration_curves(predictions, labels, title_prefix=title_prefix)
    sections.append(
        ReportSection(
            title="Calibration Curves",
            description="Predicted probability vs actual positive rate per product.",
            figures=[cal_fig],
        )
    )

    # 5. Segment analysis — unified loop over all configured segment columns
    for seg_col in segment_columns:
        if seg_col not in labels.columns:
            continue

        seg_metrics = compute_segment_metrics(
            predictions, labels, segment_column=seg_col, k_values=k_values
        )
        seg_figs = plot_segment_charts(seg_metrics, title_prefix=title_prefix)

        display_name = seg_col.replace("_", " ").title()
        sections.append(
            ReportSection(
                title=f"Segment Analysis: {display_name}",
                description=f"Metrics by {seg_col}.",
                figures=seg_figs,
            )
        )

    return metrics, sections


@app.command()
def analyze(
    model_version: str = typer.Argument(help="Model version hash, 'latest', or 'best'"),
    snap_date: str = typer.Option(..., "--snap-date", help="Snap date (YYYY-MM-DD or YYYYMMDD)"),
    data_dir: str = typer.Option("data/", "--data-dir", help="Base data directory"),
    k_values: Optional[str] = typer.Option(None, "--k-values", help="Comma-separated K values (overrides YAML)"),
    params_file: str = typer.Option(
        "conf/base/parameters_evaluation.yaml",
        "--params-file",
        help="Path to evaluation parameters YAML",
    ),
):
    """Analyze a single model version."""
    data_path = Path(data_dir)
    snap_date_dir = _parse_snap_date(snap_date)

    # Load evaluation parameters
    eval_params = _load_eval_params(params_file)

    # CLI --k-values overrides YAML
    if k_values is not None:
        k_list = _parse_k_values_str(k_values)
    else:
        k_list = eval_params["k_values"]

    # Resolve version
    resolved_version = _resolve_version(model_version, data_path)
    typer.echo(f"Model version: {resolved_version}")

    # Load data
    predictions = _load_predictions(data_path, resolved_version, snap_date_dir)
    labels = _load_labels(data_path)

    # Load and join external segment sources
    segment_sources = eval_params.get("segment_sources", {})
    if segment_sources:
        labels = load_and_join_segment_sources(labels, segment_sources)

    # Build full segment column list
    segment_columns = list(eval_params.get("segment_columns", []))
    for source_config in segment_sources.values():
        seg_col = source_config["segment_column"]
        if seg_col not in segment_columns:
            segment_columns.append(seg_col)

    # Run analysis
    metrics, sections = _run_analysis(
        predictions, labels, k_list, segment_columns=segment_columns
    )

    # Generate report
    metadata = {
        "Model Version": resolved_version,
        "Snap Date": snap_date,
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Total Queries": metrics["n_queries"],
        "Excluded Queries": metrics["n_excluded_queries"],
    }
    html = generate_html_report(sections, title=f"Evaluation: {resolved_version}", metadata=metadata)

    # Save
    output_dir = data_path / "evaluation" / resolved_version / snap_date_dir
    report_path = save_report(html, output_dir)
    metrics_path = save_metrics_json(metrics, output_dir)

    # Print summary
    typer.echo(f"\nOverall Metrics:")
    for k, v in metrics["overall"].items():
        typer.echo(f"  {k}: {v:.4f}")
    typer.echo(f"\nReport: {report_path}")
    typer.echo(f"Metrics: {metrics_path}")


@app.command()
def compare(
    model_a: str = typer.Argument(help="Model A version hash, 'latest', or 'best'"),
    model_b: Optional[str] = typer.Argument(None, help="Model B version (optional if --baseline used)"),
    snap_date: str = typer.Option(..., "--snap-date", help="Snap date (YYYY-MM-DD or YYYYMMDD)"),
    baseline: Optional[str] = typer.Option(None, "--baseline", help="Baseline type: global_popularity or segment_popularity"),
    data_dir: str = typer.Option("data/", "--data-dir", help="Base data directory"),
    k_values: Optional[str] = typer.Option(None, "--k-values", help="Comma-separated K values (overrides YAML)"),
    params_file: str = typer.Option(
        "conf/base/parameters_evaluation.yaml",
        "--params-file",
        help="Path to evaluation parameters YAML",
    ),
):
    """Compare two models or a model vs baseline."""
    if model_b is None and baseline is None:
        typer.echo("Error: either model_b or --baseline is required.", err=True)
        raise typer.Exit(1)
    if model_b is not None and baseline is not None:
        typer.echo("Error: specify either model_b or --baseline, not both.", err=True)
        raise typer.Exit(1)

    data_path = Path(data_dir)
    snap_date_dir = _parse_snap_date(snap_date)

    # Load evaluation parameters
    eval_params = _load_eval_params(params_file)

    # CLI --k-values overrides YAML
    if k_values is not None:
        k_list = _parse_k_values_str(k_values)
    else:
        k_list = eval_params["k_values"]

    # Resolve version A
    resolved_a = _resolve_version(model_a, data_path)
    typer.echo(f"Model A: {resolved_a}")

    # Load data for A
    predictions_a = _load_predictions(data_path, resolved_a, snap_date_dir)
    labels = _load_labels(data_path)

    # Determine B
    if model_b is not None:
        resolved_b = _resolve_version(model_b, data_path)
        typer.echo(f"Model B: {resolved_b}")
        predictions_b = _load_predictions(data_path, resolved_b, snap_date_dir)
        label_b = resolved_b
    else:
        # Generate baseline
        customer_ids = predictions_a["cust_id"].unique().tolist()
        products = sorted(predictions_a["prod_code"].unique().tolist())

        if baseline == "global_popularity":
            typer.echo("Baseline: Global Popularity")
            predictions_b = generate_global_popularity_baseline(
                labels, snap_date_dir, customer_ids, products
            )
            label_b = "global_popularity"
        elif baseline == "segment_popularity":
            typer.echo("Baseline: Segment Popularity")
            predictions_b = generate_segment_popularity_baseline(
                labels, snap_date_dir, customer_ids, products=products
            )
            label_b = "segment_popularity"
        else:
            typer.echo(f"Error: unknown baseline type: {baseline}", err=True)
            raise typer.Exit(1)

    # Compute metrics
    metrics_a = compute_all_metrics(predictions_a, labels, k_values=k_list)
    metrics_b = compute_all_metrics(predictions_b, labels, k_values=k_list)

    # Build comparison
    comparison = build_comparison_result(metrics_a, metrics_b, resolved_a, label_b)

    # Visualizations
    comp_figs = plot_comparison_metrics(comparison)
    dist_figs = plot_comparison_score_distributions(
        predictions_a, predictions_b, resolved_a, label_b
    )

    # Build report sections
    delta_df = pd.DataFrame([comparison["overall_delta"]]).T
    delta_df.columns = ["Delta (A - B)"]

    overall_a_df = pd.DataFrame([metrics_a["overall"]]).T
    overall_a_df.columns = [resolved_a]
    overall_b_df = pd.DataFrame([metrics_b["overall"]]).T
    overall_b_df.columns = [label_b]
    summary_df = pd.concat([overall_a_df, overall_b_df, delta_df], axis=1)

    sections = [
        ReportSection(
            title="Comparison Summary",
            description=f"Overall metric comparison: {resolved_a} vs {label_b}",
            tables=[summary_df],
        ),
        ReportSection(
            title="Per-Product Comparison",
            description="Side-by-side metric comparison per product.",
            figures=comp_figs,
        ),
        ReportSection(
            title="Score Distribution Comparison",
            description="Score distribution overlay and boxplot comparison.",
            figures=dist_figs,
        ),
    ]

    metadata = {
        "Model A": resolved_a,
        "Model B": label_b,
        "Snap Date": snap_date,
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    html = generate_html_report(
        sections,
        title=f"Comparison: {resolved_a} vs {label_b}",
        metadata=metadata,
    )

    output_dir = data_path / "evaluation" / f"compare_{resolved_a}_vs_{label_b}" / snap_date_dir
    report_path = save_report(html, output_dir)
    metrics_json = {
        "model_a": resolved_a,
        "model_b": label_b,
        "metrics_a": metrics_a,
        "metrics_b": metrics_b,
        "overall_delta": comparison["overall_delta"],
    }
    metrics_path = save_metrics_json(metrics_json, output_dir)

    # Print summary
    typer.echo(f"\nComparison Summary ({resolved_a} vs {label_b}):")
    for k, v in comparison["overall_delta"].items():
        sign = "+" if v > 0 else ""
        typer.echo(f"  {k}: {sign}{v:.4f}")
    typer.echo(f"\nReport: {report_path}")
    typer.echo(f"Metrics: {metrics_path}")


if __name__ == "__main__":
    app()
