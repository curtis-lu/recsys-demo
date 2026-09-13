"""What a segment value is reported under, and the name reserved for "no value".

Stdlib only, on purpose. The metric layer (``evaluation/metrics_spark.py``) and
the report builder (``evaluation/report_builder.py``, which runs no Spark) both
key segments this way. ``evaluation/segments.py``, where the Spark joins live,
is set to move into ``pipelines/evaluation/steps/`` under ADR-0019, and modules
outside that pipeline may not import a ``steps/`` module (architecture
constraint S3), so the shared name cannot live there.
"""

#: The group a query lands in when its segment column exists but holds NULL
#: for it: the population table (or an override table) has no value for that
#: key. Reported and counted like any group, never averaged into a macro: one
#: made-up group weighted like a real one would drag the average (ADR-0020
#: bug 6). Parenthesised so no plausible real segment value spells it.
UNMATCHED_SEGMENT = "(unmatched)"


def segment_key(value) -> str:
    """The dict key a segment value is reported under.

    NULL becomes :data:`UNMATCHED_SEGMENT`; anything else is stringified. A
    real value that already spells the sentinel raises: it would merge into
    the unmatched group and silently leave every macro.
    """
    if value is None:
        return UNMATCHED_SEGMENT
    key = value if isinstance(value, str) else str(value)
    if key == UNMATCHED_SEGMENT:
        raise ValueError(
            f"segment value {key!r} is the name evaluation reserves for "
            f"queries with no segment value; it would be merged with them and "
            f"left out of every macro average. Rename that value upstream."
        )
    return key
