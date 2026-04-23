"""Suggest categorical columns from a dataset.

Given a parquet file or Hive table, infer which columns are likely categorical
and write the result as a YAML snippet ready to copy into
conf/base/parameters_dataset.yaml.

Local (pandas backend):
    python scripts/suggest_categorical_cols.py data/feature_table.parquet
    python scripts/suggest_categorical_cols.py data/feature_table.parquet -k 30

Production (Spark backend, Hive table):
    python scripts/suggest_categorical_cols.py edw.cust_profile --backend spark

Production (Spark backend, HDFS parquet path):
    python scripts/suggest_categorical_cols.py /user/hive/.../customer --backend spark

The output YAML is written to data/profiling/<stem>_categorical.yaml
(directory auto-created). The stem is derived from Path(input).stem for
parquet inputs and from the raw table name for Hive inputs.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

PROFILING_DIR = Path("data/profiling")

app = typer.Typer(
    help="Suggest categorical columns for parameters_dataset.yaml",
    add_completion=False,
)


def suggest_categorical_columns_pandas(
    df: "pd.DataFrame",
    max_numerical_cardinality: int = 20,
) -> tuple[list[str], list[tuple[str, int]]]:
    """Infer categorical columns from a pandas DataFrame.

    Rules:
      - string / object / bool / pd.CategoricalDtype -> always categorical
      - numeric with nunique(dropna=True) <= max_numerical_cardinality
        -> implicit categorical

    Returns:
        (categorical_columns, implicit_numeric_info)
        - categorical_columns: inferred column names, preserving original order
        - implicit_numeric_info: list of (column, nunique) tuples for numeric
          columns classified as categorical due to low cardinality
    """
    import pandas as pd

    categorical: list[str] = []
    implicit: list[tuple[str, int]] = []

    for col in df.columns:
        dtype = df[col].dtype
        if (
            isinstance(dtype, pd.CategoricalDtype)
            or pd.api.types.is_bool_dtype(dtype)
            or pd.api.types.is_string_dtype(dtype)
            or dtype == object
        ):
            categorical.append(col)
        elif pd.api.types.is_numeric_dtype(dtype):
            nunique = int(df[col].nunique(dropna=True))
            if nunique <= max_numerical_cardinality:
                categorical.append(col)
                implicit.append((col, nunique))

    return categorical, implicit


def suggest_categorical_columns_spark(
    df: "SparkDataFrame",
    max_numerical_cardinality: int = 20,
) -> tuple[list[str], list[tuple[str, int]], int]:
    """Infer categorical columns from a Spark DataFrame.

    String and boolean columns are classified directly from the schema.
    Numeric columns are evaluated via a single aggregation that also
    computes count(*), so the caller needs no additional Spark action
    to obtain the row count.

    Returns:
        (categorical_columns, implicit_numeric_info, n_rows)
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, NumericType, StringType

    string_bool_cols: list[str] = []
    numeric_cols: list[str] = []

    for field in df.schema.fields:
        dt = field.dataType
        if isinstance(dt, (StringType, BooleanType)):
            string_bool_cols.append(field.name)
        elif isinstance(dt, NumericType):
            numeric_cols.append(field.name)
        # Other types (timestamp, array, struct, ...) are ignored.

    implicit: list[tuple[str, int]] = []
    numeric_categorical: set[str] = set()

    # Always include count(*) to avoid a separate sdf.count() action in the caller.
    agg_exprs = [F.count("*").alias("__n_rows__")] + [
        F.approx_count_distinct(F.col(c), rsd=0.05).alias(c)
        for c in numeric_cols
    ]
    row = df.agg(*agg_exprs).collect()[0]
    n_rows = int(row["__n_rows__"])

    for col in numeric_cols:
        n_distinct = int(row[col])
        if n_distinct <= max_numerical_cardinality:
            numeric_categorical.add(col)
            implicit.append((col, n_distinct))

    categorical: list[str] = []
    string_bool_set = set(string_bool_cols)
    for field in df.schema.fields:
        if field.name in string_bool_set or field.name in numeric_categorical:
            categorical.append(field.name)

    return categorical, implicit, n_rows


def format_yaml_output(categorical: list[str]) -> str:
    """Format categorical columns as a flat YAML snippet.

    Example output:
        categorical_columns:
          - "col_a"
          - "col_b"
    """
    lines = ["categorical_columns:"]
    for col in categorical:
        lines.append(f'  - "{col}"')
    return "\n".join(lines) + "\n"


def _load_pandas(source: str) -> tuple["pd.DataFrame", str]:
    import pandas as pd

    path = Path(source)
    if not path.exists():
        typer.echo(f"Error: parquet path not found: {source}", err=True)
        raise typer.Exit(code=1)
    df = pd.read_parquet(path)
    return df, path.stem


def _load_spark(
    source: str, spark: "SparkSession"
) -> tuple["SparkDataFrame", str]:
    path = Path(source)
    if path.exists():
        df = spark.read.parquet(str(path))
        return df, path.stem
    # Fall through to Hive table
    try:
        df = spark.table(source)
    except Exception as exc:
        typer.echo(
            f"Error: could not read input '{source}'. "
            f"Tried as filesystem path (not found) and as Hive table "
            f"(failed: {exc}).",
            err=True,
        )
        raise typer.Exit(code=1) from exc
    return df, source


def _write_output(stem: str, content: str) -> Path:
    PROFILING_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROFILING_DIR / f"{stem}_categorical.yaml"
    output_path.write_text(content)
    return output_path


def _print_summary(
    source: str,
    backend: str,
    max_cardinality: int,
    n_rows: int,
    n_cols: int,
    categorical: list[str],
    implicit: list[tuple[str, int]],
    output_path: Path,
) -> None:
    typer.echo(
        f"Scanned {n_rows:,} rows × {n_cols} columns from {source}", err=True
    )
    typer.echo(
        f"Backend: {backend} | max_numerical_cardinality: {max_cardinality}",
        err=True,
    )
    typer.echo("", err=True)
    typer.echo(
        f"Found {len(categorical)} candidate categorical columns.", err=True
    )
    if implicit:
        typer.echo("", err=True)
        typer.echo(
            "Numeric columns inferred as implicit categoricals "
            "(low cardinality):",
            err=True,
        )
        for col, n in implicit:
            typer.echo(f"  - {col} (nunique={n})", err=True)
    typer.echo("", err=True)
    typer.echo(f"Written to: {output_path}", err=True)


@app.command()
def main(
    source: str = typer.Argument(
        ...,
        help="Parquet path or Hive table name (e.g. data/x.parquet or edw.cust)",
    ),
    max_cardinality: int = typer.Option(
        20,
        "--max-cardinality",
        "-k",
        help="Numeric columns with nunique <= this are considered implicit categoricals",
    ),
    backend: str = typer.Option(
        "pandas",
        "--backend",
        "-b",
        help="Backend: 'pandas' (parquet path only) or 'spark' (parquet path or Hive table)",
    ),
) -> None:
    """Suggest categorical columns from a dataset and write a YAML snippet."""
    if backend == "pandas":
        df, stem = _load_pandas(source)
        categorical, implicit = suggest_categorical_columns_pandas(
            df, max_cardinality
        )
        n_rows = len(df)
        n_cols = len(df.columns)
    elif backend == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("suggest_categorical_cols")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            sdf, stem = _load_spark(source, spark)
            categorical, implicit, n_rows = suggest_categorical_columns_spark(
                sdf, max_cardinality
            )
            n_cols = len(sdf.schema.fields)
        finally:
            spark.stop()
    else:
        typer.echo(
            f"Error: unknown backend '{backend}'. Use 'pandas' or 'spark'.",
            err=True,
        )
        raise typer.Exit(code=1)

    yaml_content = format_yaml_output(categorical)
    output_path = _write_output(stem, yaml_content)
    _print_summary(
        source=source,
        backend=backend,
        max_cardinality=max_cardinality,
        n_rows=n_rows,
        n_cols=n_cols,
        categorical=categorical,
        implicit=implicit,
        output_path=output_path,
    )


if __name__ == "__main__":
    app()
