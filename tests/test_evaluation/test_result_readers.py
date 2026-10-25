"""``evaluation.result_readers``: the module the report modules read the
landed results through."""


# ---------------------------------------------------------------------------
# The report reads the landed results through a module that computes nothing
# (#536, ADR-0031 principle 1)
# ---------------------------------------------------------------------------


def test_the_reports_reader_module_loads_without_spark_or_a_computation_module():
    """``evaluation.result_readers`` is what the report modules import to read
    the landed results. It must load with pyspark blocked and must not pull in
    a module that computes those results: the report importing one is how it
    used to compute instead of read. A subprocess, because in this session
    those modules are already loaded."""
    import subprocess
    import sys

    computing = (
        "recsys_tfb.evaluation.metrics",
        "recsys_tfb.evaluation.metrics_spark",
        "recsys_tfb.evaluation.baselines",
        "recsys_tfb.evaluation.prediction_quality",
        "recsys_tfb.pipelines.evaluation.steps.report_aggregates",
    )
    code = (
        "import sys\n"
        "sys.modules['pyspark'] = None\n"
        "import recsys_tfb.evaluation.result_readers\n"
        f"loaded = [m for m in {computing!r}\n"
        "          if m in sys.modules]\n"
        "assert not loaded, loaded\n"
    )
    result = subprocess.run([sys.executable, "-c", code],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-2000:]
