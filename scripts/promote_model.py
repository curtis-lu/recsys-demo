"""Promote a versioned model to best/ for inference use.

Usage:
    python scripts/promote_model.py a1b2c3d4                  # specific version (hash)
    python scripts/promote_model.py 20260316_153000           # specific version (timestamp)
    python scripts/promote_model.py --env <env>                # auto-select the recommended version
    python scripts/promote_model.py --env <env> --dry-run      # list versions without promoting
    python scripts/promote_model.py --models-dir /custom/path  # custom directory

Picking a version ranks them under the configuration as it is **now**
(ADR-0028 decision 3), read from ``conf/`` under the working directory with
``--env`` the way the pipelines read it, and held to the same checks first:

* the ruler is the selection metric — ``test_metrics.selection_metric``, or
  the effective HPO objective when it is not set;
* the exam is the scored months — ``test_metrics.snap_date``, or every
  ``dataset.test_snap_dates`` month when it is not set.

A version takes part only when its ``evaluation_results.json`` holds a value
for that metric and records exactly those months, compared as calendar days.
``Recommended`` and the auto-selection pick among those alone; the rest are
listed with why they were left out and how to bring them back. Why the
versions' own selection metrics do not matter: each file keeps a value for
every metric its run scored, and only the current one is read. Every metric
in the registry is a higher-is-better average precision.

A version named on the command line is promoted as before: nothing is picked,
so the configuration is not read.
"""

import argparse
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Mapping, NamedTuple, Optional

import pandas as pd

from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.consistency import (
    duplicate_test_month_errors,
    missing_test_month_errors,
    resolved_env_dir,
    scoring_param_errors,
    scoring_snap_dates,
    validate_config_consistency,
)
from recsys_tfb.core.schema import validate_schema_config
from recsys_tfb.evaluation.metric_registry import selection_metric

REQUIRED_ARTIFACTS = [
    "model.txt",
    "best_params.json",
    "evaluation_results.json",
]

_VERSION_TIMESTAMP_RE = re.compile(r"^\d{8}_\d{6}$")
_VERSION_HASH_RE = re.compile(r"^[0-9a-f]{8}$")

#: Where a version left out of the ranking is told to look.
RESCORING_GUIDE = "docs/operations/user-guides/rescoring-an-old-version.md"
EVAL_MONTH_GUIDE = "docs/operations/user-guides/adding-an-eval-month.md"

# Why a version is not ranked. One version can have more than one.
NO_SCORED_MONTHS = "no_scored_months"
OTHER_SCORED_MONTHS = "other_scored_months"
NO_VALUE = "no_value"


class Reason(NamedTuple):
    """Why one version is left out of the ranking, and how to bring it back."""

    code: str
    says: str
    fix: str


class Ranked(NamedTuple):
    version: str
    value: float


class NotRanked(NamedTuple):
    version: str
    reasons: tuple


class Ranking(NamedTuple):
    """Every version under one ruler and one exam: ``ranked`` best first,
    ``not_ranked`` in version order."""

    metric: str
    months: list
    ranked: list
    not_ranked: list


def _is_version_dir(name: str) -> bool:
    """Check if a directory name matches a known version format."""
    return bool(_VERSION_TIMESTAMP_RE.match(name) or _VERSION_HASH_RE.match(name))


def _read_versions(models_dir: Path) -> dict:
    """{version: its evaluation_results.json}, for every version directory
    that has one."""
    versions = {}
    for d in sorted(models_dir.iterdir()):
        if not d.is_dir() or d.is_symlink() or not _is_version_dir(d.name):
            continue
        eval_path = d / "evaluation_results.json"
        if not eval_path.exists():
            continue
        with open(eval_path) as f:
            versions[d.name] = json.load(f)
    return versions


def _day(value) -> str:
    """One date as ``YYYY-MM-DD``, so ``20260131`` and ``2026-01-31`` compare
    equal; text that is not a date is kept as written."""
    try:
        return pd.Timestamp(str(value).strip()).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(value).strip()


def _days(values) -> set:
    return {_day(v) for v in values}


def _reasons(results: Mapping, metric: str, months: list) -> tuple:
    """Why a version with these results is not ranked; empty when it is."""
    recorded = results.get("snap_dates")
    if recorded is None:
        # Written before ADR-0028: which months went into it is unknown, and
        # nothing else in the file is worth reporting — it has no per-metric
        # values either.
        return (Reason(
            NO_SCORED_MONTHS,
            "its evaluation_results.json records no scored months (written "
            "before ADR-0028), so which months it was scored on is unknown",
            f"re-score it on the current scored months: {RESCORING_GUIDE}",
        ),)

    reasons = []
    if _days(recorded) != _days(months):
        reasons.append(Reason(
            OTHER_SCORED_MONTHS,
            f"scored on {list(recorded)}, not on the current scored months "
            f"{months}",
            f"re-score it on the current scored months ({RESCORING_GUIDE}); "
            f"if they moved only because a month was added to "
            f"dataset.test_snap_dates, write the old months into "
            f"test_metrics.snap_date instead ({EVAL_MONTH_GUIDE})",
        ))
    if metric not in (results.get("metrics") or {}):
        withheld = (results.get("metrics_not_computed") or {}).get(metric)
        if withheld is not None:
            reasons.append(Reason(
                NO_VALUE,
                f"no value for {metric}: {withheld}",
                "re-scoring cannot give it one while the reason above holds "
                f"for its dataset version ({RESCORING_GUIDE})",
            ))
        else:
            scored = sorted(results.get("metrics") or {})
            reasons.append(Reason(
                NO_VALUE,
                f"no value for {metric} (it has {scored})",
                f"re-score it with test_metrics.selection_metric: {metric} "
                f"written out ({RESCORING_GUIDE})",
            ))
    return tuple(reasons)


def rank_versions(models_dir: Path, parameters: Mapping) -> Ranking:
    """Rank every version under the current selection metric, on the current
    scored months (ADR-0028 decision 3)."""
    metric = selection_metric(parameters)
    months = scoring_snap_dates(parameters)
    ranked, not_ranked = [], []
    for version, results in _read_versions(models_dir).items():
        reasons = _reasons(results, metric, months)
        if reasons:
            not_ranked.append(NotRanked(version, reasons))
        else:
            ranked.append(Ranked(version, float(results["metrics"][metric])))
    ranked.sort(key=lambda r: (-r.value, r.version))
    return Ranking(metric, months, ranked, not_ranked)


def get_current_best_version(models_dir: Path) -> Optional[str]:
    """Detect the currently promoted best version."""
    best_dir = models_dir / "best"
    if not best_dir.exists():
        return None
    if best_dir.is_symlink():
        return best_dir.resolve().name
    # Old format: best is a directory, match by mAP
    best_eval = best_dir / "evaluation_results.json"
    if not best_eval.exists():
        return None
    with open(best_eval) as f:
        best_results = json.load(f)
    best_map = best_results.get("overall_map")
    for version, results in _read_versions(models_dir).items():
        if results.get("overall_map") == best_map:
            return version
    return None


def validate_version(version_dir: Path) -> list[str]:
    """Return list of missing artifacts."""
    return [a for a in REQUIRED_ARTIFACTS if not (version_dir / a).exists()]


def promote(models_dir: Path, version: str) -> dict:
    """Create a symlink best/ -> version/. Returns summary dict."""
    version_dir = models_dir / version
    if not version_dir.is_dir():
        print(f"Error: version directory not found: {version_dir}", file=sys.stderr)
        sys.exit(1)

    missing = validate_version(version_dir)
    if missing:
        print(f"Error: incomplete artifacts in {version}. Missing: {missing}", file=sys.stderr)
        sys.exit(1)

    best_dir = models_dir / "best"
    # Remove existing best (symlink or directory)
    if best_dir.is_symlink():
        best_dir.unlink()
    elif best_dir.is_dir():
        shutil.rmtree(best_dir)

    # Create symlink
    best_dir.symlink_to(version_dir.resolve())

    # Read evaluation for summary
    with open(version_dir / "evaluation_results.json") as f:
        eval_results = json.load(f)

    summary = {
        "promoted_version": version,
        "overall_map": eval_results.get("overall_map"),
        "per_item_map_attr": eval_results.get("per_item_map_attr", {}),
        "target_path": str(best_dir),
    }
    return summary


def read_parameters(conf_dir: Path, env: str) -> dict:
    """The configuration as the pipelines load it for ``env``, held to the
    checks that decide the ruler and the exam. Raises ``ValueError``.

    The same checks as a pipeline command's entry (``__main__.
    _load_config_and_setup``: A30, the schema, and every check each command
    runs — A54 among them, the selection metric test cannot score), then
    the ones the training command adds on the months and the
    ``test_metrics`` block (A26, A36, A53): an invalid block would give
    promote a ruler or an exam training never scores on. The training
    command's other entry checks are about running it — catalog columns,
    ``--rebuild-dates``, the dataset version it reads — and are not repeated.
    """
    resolved_env_dir(conf_dir, env)
    params = ConfigLoader(str(conf_dir), env=env).get_parameters()
    validate_schema_config(params)
    validate_config_consistency(params)
    errors = (
        missing_test_month_errors(params) + duplicate_test_month_errors(params)
        + scoring_param_errors(params)
    )
    if errors:
        raise ValueError("\n".join(errors))
    return params


def _metric_source(parameters: Mapping) -> str:
    if (parameters.get("test_metrics") or {}).get("selection_metric") is not None:
        return "test_metrics.selection_metric"
    if "hpo_objective" in (parameters.get("training") or {}):
        return "test_metrics.selection_metric not set: follows training.hpo_objective"
    return ("test_metrics.selection_metric and training.hpo_objective not set: "
            "the HPO objective's default")


def _months_source(parameters: Mapping) -> str:
    if (parameters.get("test_metrics") or {}).get("snap_date") is not None:
        return "test_metrics.snap_date"
    return "test_metrics.snap_date not set: every dataset.test_snap_dates month"


def print_version_table(models_dir: Path, parameters: Mapping) -> Ranking:
    """Print every version — the ranked ones, then the ones left out with
    why and how to fix it — and the recommendation. Returns the ranking."""
    ranking = rank_versions(models_dir, parameters)
    current_best = get_current_best_version(models_dir)

    def marker(version):
        return "  (current best)" if version == current_best else ""

    print("=== Model Version Comparison ===")
    print(f"Selection metric: {ranking.metric}  ({_metric_source(parameters)})")
    print(f"Scored months:    {ranking.months}  ({_months_source(parameters)})")

    print(f"\nRanked ({len(ranking.ranked)}):")
    for r in ranking.ranked:
        print(f"  {r.version}  {ranking.metric}={r.value:.4f}{marker(r.version)}")

    if ranking.not_ranked:
        print(f"\nNot ranked ({len(ranking.not_ranked)}):")
        for entry in ranking.not_ranked:
            print(f"  {entry.version}{marker(entry.version)}")
            for reason in entry.reasons:
                print(f"    - {reason.says}")
                print(f"      fix: {reason.fix}")

    if ranking.ranked:
        top = ranking.ranked[0]
        print(f"\nRecommended: {top.version} ({ranking.metric}={top.value:.4f})")
    else:
        print(f"\nRecommended: none — no version has a {ranking.metric} value "
              f"scored on exactly {ranking.months}; see why above.")
    return ranking


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Promote a model version to best/")
    parser.add_argument("version", nargs="?", default=None, help="Version ID (auto-select if omitted)")
    parser.add_argument("--models-dir", default="data/models", help="Models directory")
    parser.add_argument("--dry-run", action="store_true", help="List versions without promoting")
    parser.add_argument(
        "--env", "-e", default="local",
        help="Config environment, as for the pipelines; read to rank versions "
             "(not when a version is named)")
    args = parser.parse_args(argv)

    models_dir = Path(args.models_dir)
    if not models_dir.is_dir():
        print(f"Error: models directory not found: {models_dir}", file=sys.stderr)
        sys.exit(1)

    version = args.version
    if args.dry_run or version is None:
        try:
            parameters = read_parameters(Path.cwd() / "conf", args.env)
        except ValueError as exc:
            print(f"Error: config validation failed, no version picked:\n{exc}",
                  file=sys.stderr)
            sys.exit(1)
        ranking = print_version_table(models_dir, parameters)
        if args.dry_run:
            return
        if not ranking.ranked:
            print("Error: no version can be ranked, so none is auto-selected",
                  file=sys.stderr)
            sys.exit(1)
        version = ranking.ranked[0].version
        print(f"\nAuto-selected: {version}")

    summary = promote(models_dir, version)

    print(f"\nPromoted: {summary['promoted_version']}")
    print(f"  mAP: {summary['overall_map']:.4f}")
    for item, attr in sorted(summary["per_item_map_attr"].items()):
        print(f"  {item}: {attr:.4f}")


if __name__ == "__main__":
    main()
