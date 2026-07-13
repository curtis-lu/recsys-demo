"""Suggest categorical columns from a dataset.

Given a Hive table or HDFS parquet path, infer which columns are likely
categorical and write the result as a YAML snippet ready to copy into
conf/base/parameters_dataset.yaml.

Usage:
    python scripts/suggest_categorical_cols.py edw.cust_profile
    python scripts/suggest_categorical_cols.py /user/hive/.../customer
    # scan only recent partitions (Spark prunes when --where hits a partition col):
    python scripts/suggest_categorical_cols.py edw.cust --where "snap_date >= '2026-06-01'"
    # random row sample to speed the per-column cardinality pass:
    python scripts/suggest_categorical_cols.py edw.cust --sample-fraction 0.1

The output YAML is written to data/profiling/<stem>_categorical.yaml
(directory auto-created). The stem is derived from Path(input).stem for
parquet inputs and from the raw table name for Hive inputs.

Completeness (no column silently ignored): EVERY column lands in one bucket —
low-card → ``categorical_columns``; high-card string → ``drop_columns`` (an
un-encoded string feature becomes an object-dtype model feature and OOMs
training, consistency invariant B6); high-card numeric → kept a numeric
feature; and date/timestamp/binary/complex → a COMMENTED review block (same B6
risk, but the script cannot decide categorical-vs-drop, so a human must). The
terminal summary enumerates the same columns as the YAML plus a reconciliation
line. Review every block before copying.

Speed (``--where`` / ``--sample-fraction``): both scan only a SUBSET, which
UNDER-estimates cardinality — a low-card verdict on a subset is a lower bound
(a column may be higher-cardinality in the full data). The summary states the
scan scope and the YAML carries a "verify before trusting" warning. ``--where``
is the I/O lever (Spark prunes partitions); ``--sample-fraction`` only trims the
per-column HLL pass, not parquet I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

PROFILING_DIR = Path("data/profiling")

# Fixed seed so --sample-fraction gives the SAME subset (hence the same
# suggestion) when the tool is re-run on unchanged data.
_SAMPLE_SEED = 42

app = typer.Typer(
    help="Suggest categorical columns for parameters_dataset.yaml",
    add_completion=False,
)


@dataclass(frozen=True)
class ColumnScan:
    """Full classification of every source column (completeness, req 3a).

    Every schema column lands in EXACTLY ONE of these buckets — no column is
    silently ignored:

    * ``categorical`` — low-card string/bool OR low-card numeric (schema order).
    * ``drop_suggestions`` — high-card string ``(col, approx_nunique)``.
    * ``numeric_features`` — high-card numeric, kept as a numeric feature (not
      emitted to YAML; tracked so completeness reconciles).
    * ``review`` — date/timestamp/binary/complex ``(col, spark_type)``:
      non-numeric AND not string-encodable here, so a B6 object-dtype OOM risk
      if left as an un-encoded feature. The script cannot decide — a human must
      declare each categorical (→ integer-encoded) or drop it.

    ``implicit`` is the low-card *numeric* subset of ``categorical`` (kept
    separately only for the summary display).
    """

    categorical: list[str]
    drop_suggestions: list[tuple[str, int]]
    implicit: list[tuple[str, int]]
    numeric_features: list[str]
    review: list[tuple[str, str]]
    n_rows: int


@dataclass(frozen=True)
class SubsetInfo:
    """Provenance of a --where / --sample-fraction subset scan.

    A subset UNDER-estimates cardinality, so a low-card verdict on a subset is a
    lower bound. Rendered into the summary (always, as scan scope) and, when a
    subset is active, into the YAML as a "verify before trusting" warning.
    """

    where: str | None = None
    fraction: float | None = None

    @property
    def is_subset(self) -> bool:
        return bool(self.where) or self.fraction is not None

    def describe(self) -> str:
        parts: list[str] = []
        if self.where:
            parts.append(f"where={self.where!r}")
        if self.fraction is not None:
            parts.append(f"sample-fraction={self.fraction}")
        return "; ".join(parts) if parts else "full table (no subset)"


def suggest_categorical_columns_spark(
    df: "SparkDataFrame",
    max_numerical_cardinality: int = 20,
    max_string_cardinality: int = 50,
) -> ColumnScan:
    """Classify EVERY source column into exactly one completeness bucket.

    Three-way partition by Spark type, so no column is silently ignored:

    * numeric — nunique <= ``max_numerical_cardinality`` → implicit categorical;
      else a numeric feature (``numeric_features``).
    * string/boolean — nunique <= ``max_string_cardinality`` → categorical;
      else ``drop_suggestions`` (an un-encoded high-card string becomes an
      object-dtype model feature → training OOM, consistency B6).
    * everything else (date/timestamp/binary/complex) → ``review``: non-numeric
      and not string-encodable here, so the same B6 OOM risk if kept as an
      un-encoded feature. NOT cardinality-counted (``approx_count_distinct`` on
      complex types errors); surfaced with its Spark type for a human decision.

    Cardinality for numeric + string/bool columns is computed in ONE aggregation
    (no extra scan). Returns a :class:`ColumnScan`; see its docstring for the
    per-bucket contract and the completeness invariant.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, NumericType, StringType

    string_bool_cols: list[str] = []
    numeric_cols: list[str] = []
    review: list[tuple[str, str]] = []

    for field in df.schema.fields:
        dt = field.dataType
        if isinstance(dt, (StringType, BooleanType)):
            string_bool_cols.append(field.name)
        elif isinstance(dt, NumericType):
            numeric_cols.append(field.name)
        else:
            review.append((field.name, dt.simpleString()))
    review.sort()

    counted_cols = numeric_cols + string_bool_cols
    agg_exprs = [F.count("*").alias("__n_rows__")] + [
        F.approx_count_distinct(F.col(c), rsd=0.05).alias(c)
        for c in counted_cols
    ]
    row = df.agg(*agg_exprs).collect()[0]
    n_rows = int(row["__n_rows__"])

    implicit: list[tuple[str, int]] = []
    numeric_categorical: set[str] = set()
    numeric_features: list[str] = []
    for col in numeric_cols:
        n_distinct = int(row[col])
        if n_distinct <= max_numerical_cardinality:
            numeric_categorical.add(col)
            implicit.append((col, n_distinct))
        else:
            numeric_features.append(col)

    string_categorical: set[str] = set()
    drop_suggestions: list[tuple[str, int]] = []
    for col in string_bool_cols:
        n_distinct = int(row[col])
        if n_distinct <= max_string_cardinality:
            string_categorical.add(col)
        else:
            drop_suggestions.append((col, n_distinct))
    drop_suggestions.sort()

    categorical: list[str] = []
    for field in df.schema.fields:
        if field.name in string_categorical or field.name in numeric_categorical:
            categorical.append(field.name)

    return ColumnScan(
        categorical=categorical,
        drop_suggestions=drop_suggestions,
        implicit=implicit,
        numeric_features=numeric_features,
        review=review,
        n_rows=n_rows,
    )


def format_yaml_output(
    categorical: list[str],
    drop_suggestions: list[tuple[str, int]] | None = None,
    review: list[tuple[str, str]] | None = None,
    subset: "SubsetInfo | None" = None,
) -> str:
    """Format categorical + drop + review columns as a YAML snippet.

    ``review`` (date/timestamp/binary/complex) is emitted as a COMMENTED block:
    each is a B6 OOM risk if kept un-encoded, but the script cannot decide
    categorical-vs-drop, so it is surfaced as comments only — the YAML still
    parses to just ``categorical_columns`` + ``drop_columns``, forcing a
    conscious human move of each review column into one of the two real blocks.

    Example output:
        categorical_columns:
          - "col_a"
        drop_columns:
          - "raw_id"   # nunique=4200 high-card string — review: declare categorical or drop
        # --- review: non-numeric columns (B6 OOM risk ...) ---
        #   - "event_date"   # type=date — un-encoded → object-dtype OOM; ...
    """
    lines = ["categorical_columns:"]
    if subset is not None and subset.is_subset:
        lines.append(
            f"  # ⚠ cardinality measured on a SUBSET ({subset.describe()}); a"
        )
        lines.append(
            "  # low-card verdict here is a LOWER BOUND — a column may be"
        )
        lines.append(
            "  # higher-cardinality in the full data. Verify before trusting."
        )
    for col in categorical:
        lines.append(f'  - "{col}"')
    lines.append("drop_columns:")
    if drop_suggestions:
        for col, n in drop_suggestions:
            lines.append(
                f'  - "{col}"   # nunique={n} high-card string — review: '
                f"declare categorical or drop (script cannot decide)"
            )
    else:
        lines.append("  # （無高 cardinality 字串欄；此清單供人工確認）")
    if review:
        lines.append(
            "# --- review: non-numeric columns (B6 OOM risk if kept "
            "un-encoded — move each to categorical_columns OR drop_columns) ---"
        )
        for col, typ in review:
            lines.append(
                f'#   - "{col}"   # type={typ} — un-encoded → object-dtype OOM; '
                f"declare categorical (encoded) or drop"
            )
    return "\n".join(lines) + "\n"


def _apply_subset(
    df: "SparkDataFrame",
    where: str | None = None,
    fraction: float | None = None,
) -> "SparkDataFrame":
    """Restrict the scan to a subset before cardinality is measured.

    * ``where`` — a Spark SQL predicate applied with ``DataFrame.where``. When
      it references a partition column, Spark PRUNES the non-matching partition
      directories (real I/O saving); otherwise it is a plain row filter (still
      correct, no I/O saving). This is req 1's partition pruning — Spark's own
      pushdown does the work, so nothing here enumerates partitions.
    * ``fraction`` — a random Bernoulli row sample (``DataFrame.sample``) with a
      fixed seed for reproducibility. Applied AFTER ``where`` so the two
      compose: prune to partitions, then sample within.

    Both are optional; with neither, ``df`` is returned unchanged. NOTE: a
    subset UNDER-estimates cardinality, so a low-card verdict on a subset is a
    lower bound — see the summary/YAML provenance note the caller emits.
    """
    if where:
        df = df.where(where)
    if fraction is not None:
        if not 0.0 < fraction <= 1.0:
            raise ValueError(
                f"--sample-fraction must be in (0, 1]; got {fraction}"
            )
        df = df.sample(fraction=fraction, seed=_SAMPLE_SEED)
    return df


def _load_spark(
    source: str, spark: "SparkSession"
) -> tuple["SparkDataFrame", str]:
    path = Path(source)
    if path.exists():
        df = spark.read.parquet(str(path))
        return df, path.stem
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


def _render_summary_lines(
    scan: ColumnScan,
    source: str,
    max_cardinality: int,
    n_cols: int,
    output_path: Path,
    subset: "SubsetInfo | None" = None,
) -> list[str]:
    """Build the human summary as a list of lines (pure; ``_print_summary``
    echoes them).

    Enumerates categorical + drop + review BY NAME so the terminal shows the
    SAME actionable column set the YAML file does — the regression guard for the
    drift where string categoricals were only counted here, not named. The
    "Column accounting" line reconciles every column against ``n_cols`` (the
    independent schema count), proving nothing was silently dropped. Always
    prints a "Scan scope" provenance line (full vs subset).
    """
    implicit_nunique = dict(scan.implicit)
    scope = subset.describe() if subset is not None else "full table (no subset)"
    lines = [
        f"Scanned {scan.n_rows:,} rows × {n_cols} columns from {source}",
        f"Scan scope: {scope}",
        f"max_numerical_cardinality: {max_cardinality}",
        "",
        f"Column accounting: {n_cols} columns = "
        f"{len(scan.categorical)} categorical + "
        f"{len(scan.numeric_features)} numeric-feature + "
        f"{len(scan.drop_suggestions)} drop-suggested + "
        f"{len(scan.review)} review",
        "",
        f"Candidate categorical columns ({len(scan.categorical)}):",
    ]
    for col in scan.categorical:
        if col in implicit_nunique:
            lines.append(f"  - {col} (numeric, nunique={implicit_nunique[col]})")
        else:
            lines.append(f"  - {col}")
    if scan.drop_suggestions:
        lines.append("")
        lines.append(
            "High-cardinality string columns → drop_columns "
            "(un-encoded string feature → object-dtype OOM, consistency B6):"
        )
        for col, n in scan.drop_suggestions:
            lines.append(f"  - {col} (nunique={n})")
    if scan.review:
        lines.append("")
        lines.append(
            "Non-numeric columns needing review (date/timestamp/binary/complex "
            "→ B6 risk; declare categorical or drop):"
        )
        for col, typ in scan.review:
            lines.append(f"  - {col} (type={typ})")
    lines.append("")
    lines.append(f"Written to: {output_path}")
    return lines


def _print_summary(
    scan: ColumnScan,
    source: str,
    max_cardinality: int,
    n_cols: int,
    output_path: Path,
    subset: "SubsetInfo | None" = None,
) -> None:
    for line in _render_summary_lines(
        scan, source, max_cardinality, n_cols, output_path, subset
    ):
        typer.echo(line, err=True)


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
    max_string_cardinality: int = typer.Option(
        50,
        "--max-string-cardinality",
        help="String/bool columns with nunique > this are suggested into drop_columns (B6 footgun)",
    ),
    where: str = typer.Option(
        None,
        "--where",
        help=(
            "Spark SQL predicate to prune the scan, e.g. "
            "\"snap_date >= '2026-06-01'\". Referencing a partition column "
            "prunes partitions (real I/O saving); otherwise a plain row filter."
        ),
    ),
    sample_fraction: float = typer.Option(
        None,
        "--sample-fraction",
        help=(
            "Random row-sample fraction in (0, 1]. Speeds the per-column "
            "cardinality (HLL) pass; does NOT reduce parquet I/O — use --where "
            "for that. A subset under-estimates cardinality (lower bound)."
        ),
    ),
) -> None:
    """Suggest categorical columns from a dataset and write a YAML snippet."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    # Fail fast on a bad fraction BEFORE the 2–4 min Spark cold start.
    if sample_fraction is not None and not 0.0 < sample_fraction <= 1.0:
        raise typer.BadParameter("--sample-fraction must be in (0, 1]")

    subset = SubsetInfo(where=where, fraction=sample_fraction)
    spark = get_or_create_spark_session()
    try:
        sdf, stem = _load_spark(source, spark)
        sdf = _apply_subset(sdf, where=where, fraction=sample_fraction)
        scan = suggest_categorical_columns_spark(
            sdf, max_cardinality, max_string_cardinality
        )
        n_cols = len(sdf.schema.fields)
    finally:
        spark.stop()

    yaml_content = format_yaml_output(
        scan.categorical, scan.drop_suggestions, scan.review, subset
    )
    output_path = _write_output(stem, yaml_content)
    _print_summary(
        scan=scan,
        source=source,
        max_cardinality=max_cardinality,
        n_cols=n_cols,
        output_path=output_path,
        subset=subset,
    )


if __name__ == "__main__":
    app()
