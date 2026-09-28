"""Load order between ``io``, ``models`` and ``core`` (ADR-0030 decisions 3, 15).

Every check here runs in a fresh interpreter: inside the pytest process all
three packages are long loaded, so an import that only works in one order,
or a registry that is only full because some earlier test filled it, passes
there regardless.
"""

import subprocess
import sys
import textwrap

import pytest


def _run(code: str) -> subprocess.CompletedProcess:
    # tests/conftest.py has already put this tree's src on PYTHONPATH, so the
    # child loads the code under test and not the editable install's.
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True, text=True,
    )


@pytest.mark.parametrize("entry", [
    "recsys_tfb.io",
    "recsys_tfb.models",
    "recsys_tfb.models.lightgbm_adapter",
    "recsys_tfb.io.model_adapter_dataset",
])
def test_each_entry_point_imports_cold(entry):
    """Each one alone, first thing in a new process. Before decision 15 all
    four loaded too, but only because of the order ``io`` and ``models``
    happened to initialise in; the adapter's function-level imports are what
    still keeps the last one loading (see the top of lightgbm_adapter.py)."""
    result = _run(f"import {entry}")
    assert result.returncode == 0, result.stderr


def test_io_no_longer_loads_models():
    """The cycle itself: ``io/__init__`` used to re-export
    ``ModelAdapterDataset``, which imports ``models``, whose LightGBM adapter
    imports ``io`` — each package needed the other to finish loading."""
    result = _run("""
        import sys
        import recsys_tfb.io
        print(sorted(m for m in sys.modules if m.startswith("recsys_tfb.models")))
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"


def test_the_pre_run_check_sees_lightgbm_registered():
    """Only the check's own module is imported, never ``models``: the check
    has to fill the registry itself. LightGBM registers when
    ``models/__init__`` imports its adapter module; were that import dropped,
    ten test files that import the adapter directly would stay green while
    every real run rejected ``algorithm: lightgbm``."""
    result = _run("""
        from recsys_tfb.core.consistency import training_algorithm_errors
        print(training_algorithm_errors({"training": {"algorithm": "lightgbm"}}))
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]"
