"""Run the evaluation pipeline for a "the outputs did not change" check.

One local evaluation that draws every section both reports have:
``--post-training`` with the prediction-quality section on, and ``--compare``
against the model itself, so ``report_comparison.html`` is produced too. Each
run's outputs are copied to ``data/verification/eval_parity/<label>/`` and
compared with ``compare_eval_runs.py``. The workflow, in a worktree::

    # on the unchanged code
    eval_parity.py prepare                 # dataset + training for the run
    eval_parity.py run base1
    eval_parity.py run base2
    compare_eval_runs.py <verif>/base1 <verif>/base2   # noise floor: all SAME
    # change the code, then
    eval_parity.py run after
    compare_eval_runs.py <verif>/base1 <verif>/after

Those settings need three conf entries the shipped conf does not have; they
are written to ``conf/local/`` for the length of each command and removed
after it, whatever happens. A ``conf/local/`` already holding files is
refused: the overlay would mix with them, and removing it would delete them.

* ``dataset.test_zero_positive_group_ratio`` above 0 — A46 refuses the
  prediction-quality section under ``--post-training`` without it;
* the zero-positive group weight column on ``training_eval_predictions`` —
  A45, built from the base catalog entry so a column added there is kept;
* the section switch and the self-comparison source.

Each run records ``SOURCE``: a digest of every file under ``src/``, the commit,
and uncommitted changes under ``src/``. The digest is taken before and after
the run, and a run during which the code changed is refused — a comparison
is only evidence for the code that produced it.

Run from the repo or worktree root (the CLI finds ``conf/`` and ``data/``
from the current directory), with the repo's interpreter:
  cd <wt> && /Users/.../.venv/bin/python <wt>/scripts/local/eval_parity.py run base1
Spark cold start included, ``prepare`` and each ``run`` take a few minutes
(2026-10-22, 8-core macOS laptop, synthetic data: about one minute each).
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import yaml

#: The ``compare_sources`` key of the self-comparison.
COMPARE_KEY = "parity_self"
#: Where each run's copy goes, under the root.
VERIFICATION_DIR = Path("data/verification/eval_parity")
_HEADER = "# Written by scripts/local/eval_parity.py for one command; removed after it.\n"


def overlay_files(base_conf: Path, model_version: str | None) -> dict[str, str]:
    """``{file name: YAML}`` for ``conf/local/``. ``model_version`` is the
    model the comparison reads; ``None`` (before training) leaves it out."""
    catalog = yaml.safe_load((Path(base_conf) / "catalog.yaml").read_text())
    columns = list(catalog["training_eval_predictions"]["columns"])
    if not any(c["name"] == "zero_positive_group_weight" for c in columns):
        columns.append({"name": "zero_positive_group_weight", "type": "DOUBLE"})
    evaluation: dict = {"report": {"sections": {"prediction_quality": True}}}
    if model_version is not None:
        evaluation["compare_sources"] = {COMPARE_KEY: {
            "kind": "model_version",
            "label": "self (training_eval_predictions)",
            "model_version": model_version,
            "source": "training_eval_predictions",
        }}
    files = {
        "catalog.yaml": {"training_eval_predictions": {"columns": columns}},
        "parameters_dataset.yaml": {
            "dataset": {"test_zero_positive_group_ratio": 0.5}},
        "parameters_evaluation.yaml": {"evaluation": evaluation},
    }
    return {name: _HEADER + yaml.safe_dump(body, sort_keys=False,
                                           allow_unicode=True)
            for name, body in files.items()}


@contextmanager
def overlay(conf_local: Path, files: dict[str, str]):
    """Write ``files`` into ``conf_local`` for the block, then remove them."""
    conf_local = Path(conf_local)
    present = sorted(p.name for p in conf_local.iterdir() if p.name != ".gitkeep")
    if present:
        raise RuntimeError(
            f"{conf_local} already holds {present}; the overlay would mix with "
            "them and removing it would delete them. Move them aside first.")
    written = []
    try:
        for name, text in files.items():
            (conf_local / name).write_text(text)
            written.append(conf_local / name)
        yield
    finally:
        for path in written:
            path.unlink(missing_ok=True)


def src_digest(src: Path) -> str:
    """sha256 over every file under ``src`` (path and content), sorted."""
    h = hashlib.sha256()
    for path in sorted(p for p in Path(src).rglob("*")
                       if p.is_file() and "__pycache__" not in p.parts):
        h.update(path.relative_to(src).as_posix().encode())
        h.update(path.read_bytes())
    return h.hexdigest()


def _cli(root: Path, *args: str) -> None:
    env = {**os.environ, "SPARK_CONF_DIR": str(root / "conf" / "spark-local"),
           "PYTHONPATH": str(root / "src")}
    print(f"▶ {' '.join(args)}", flush=True)
    subprocess.run([sys.executable, *args], cwd=root, env=env, check=True)


def _latest_model(root: Path) -> str:
    sys.path.insert(0, str(root / "src"))
    from recsys_tfb.core.versioning import find_latest_completed_model_version

    found = find_latest_completed_model_version(root / "data" / "models")
    if found is None:
        raise SystemExit("no completed model under data/models: run "
                         "`eval_parity.py prepare` first")
    return found[0]


def prepare(root: Path) -> None:
    with overlay(root / "conf" / "local", overlay_files(root / "conf" / "base", None)):
        _cli(root, str(root / "scripts" / "local" / "local_spark_setup.py"), "--reset")
        _cli(root, "-m", "recsys_tfb", "dataset", "--env", "local")
        _cli(root, "-m", "recsys_tfb", "training", "--env", "local")
    print(f"model_version={_latest_model(root)}")


def run(root: Path, label: str) -> Path:
    model_version = _latest_model(root)
    before = src_digest(root / "src")
    files = overlay_files(root / "conf" / "base", model_version)
    with overlay(root / "conf" / "local", files):
        _cli(root, "-m", "recsys_tfb", "evaluation", "--env", "local",
             "--post-training", "--model-version", model_version,
             "--compare", COMPARE_KEY)
    if src_digest(root / "src") != before:
        raise SystemExit("src/ changed during the run; its outputs belong to "
                         "neither version. Run it again.")
    out = root / VERIFICATION_DIR / label
    if out.exists():
        shutil.rmtree(out)
    shutil.copytree(root / "data" / "evaluation" / model_version, out)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True,
                          capture_output=True, text=True).stdout.strip()
    status = subprocess.run(["git", "status", "--short", "--", "src"], cwd=root,
                            check=True, capture_output=True, text=True).stdout
    (out / "SOURCE").write_text(
        f"src digest {before[:16]} · HEAD {head[:8]}\n{status}")
    print(f"copied to {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("label")
    args = parser.parse_args(argv)
    root = Path.cwd()
    if args.command == "prepare":
        prepare(root)
    else:
        run(root, args.label)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
