"""Tests for the inference publish node (staging -> production promotion).

publish_predictions is the single production write in the staging->validate->
publish gate: rank writes ranked_staging, validate runs sanity checks on it
(raising before publish on failure), and only on success does publish promote
the validated DataFrame to the production ranked_predictions table. The node
itself is a pure pass-through; the production write is the catalog save of its
ranked_predictions output. These are pure-Python tests — no Spark needed.
"""

from recsys_tfb.pipelines.inference.nodes_spark import publish_predictions


def test_publish_returns_input_unchanged():
    """Must return the validated DataFrame untouched so the catalog save of
    ranked_predictions writes exactly the validated rows."""
    sentinel = object()
    result = publish_predictions(sentinel, {"model_version": "abc12345"})
    assert result is sentinel


def test_publish_tolerates_missing_model_version():
    """Audit logging of model_version is best-effort; an absent key must not
    raise."""
    sentinel = object()
    result = publish_predictions(sentinel, {})
    assert result is sentinel
