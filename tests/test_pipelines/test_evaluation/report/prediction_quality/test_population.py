"""母體, the ``--prediction-quality`` page's first section
(``report/prediction_quality/population``, ADR-0032 decision 5): which rows,
which kind of number, r and the zero-positive groups kept under
``--post-training``, and the label-maturity reminder — read off the payload's
population block."""
from __future__ import annotations

from recsys_tfb.pipelines.evaluation.report.prediction_quality import (
    population,
)
from recsys_tfb.report.types import Note, Paragraph
from tests.test_pipelines.test_evaluation.report.sections import landed


def _draw(spark, **settings):
    results, parameters = landed.results(spark, **settings)
    return population.build(results, parameters)


def _text(section) -> str:
    return " ".join(b.text for b in section.blocks
                    if isinstance(b, (Paragraph, Note)))


def test_monitoring_is_the_inference_population_exactly(spark):
    s = _draw(spark)
    text = _text(s)
    assert "上線後的 inference 母體" in text and "精確值" in text
    assert "加權估計" not in text and "r＝" not in text
    assert (s.key, s.title) == ("population", "母體")


def test_post_training_below_1_is_a_weighted_estimate_with_the_kept_groups(
        spark):
    text = _text(_draw(spark, post_training=True, ratio=landed.KEPT_RATIO))
    assert "test 母體" in text and "加權估計" in text
    assert "r＝0.3" in text and "1／r＝3.333" in text
    assert "zero_positive_group_weight" in text
    # The one group without a positive (C3) is the kept one.
    assert "有 1 個無正例 query group" in text


def test_post_training_at_1_is_exact_on_test(spark):
    text = _text(_draw(spark, post_training=True, ratio=1.0))
    assert "test 母體上的精確值" in text
    assert "加權估計" not in text


def test_the_maturity_reminder_is_always_there(spark):
    for settings in ({}, {"post_training": True, "ratio": landed.KEPT_RATIO}):
        (note,) = [b for b in _draw(spark, **settings).blocks
                   if isinstance(b, Note)]
        assert note.warn and "觀察窗" in note.text, settings
        # Where the label came from, and what to do when it was backfilled.
        assert "那次標準 evaluation" in note.text and "重跑" in note.text, settings


def test_the_ranking_metrics_population_is_said_apart(spark):
    off = _text(_draw(spark, drop=False))
    on = _text(_draw(spark, drop=True))
    assert "只算有正例的組" in off and "不是全正的組" not in off
    assert ("只算有正例、而且不是全正的組（evaluation.query_filter."
            "drop_all_positive_groups 開著）") in on


def _scoped(spark, columns):
    """The monitoring payload as a run under ``--scope campaign_a`` lands
    it."""
    results, parameters = landed.results(spark)
    results["prediction_quality_metrics"]["population"]["scope"] = {
        "name": "campaign_a", "table": "mkt.campaign_a", "columns": columns,
        "n_rows_in_scope": 1234, "n_scope_rows_not_scored": 56}
    return results, parameters


def test_a_scope_is_named_with_its_rows_and_the_ones_not_scored(spark):
    """ADR-0032 decision 7: the page opens with the scope's name, the rows
    it kept and its rows no evaluated row carries."""
    text = _text(population.build(*_scoped(
        spark, {"snap_date": "d", "cust_id": "c", "prod_name": "p"})))
    assert "campaign_a" in text and "mkt.campaign_a" in text
    assert "1,234 列" in text and "56 列沒被評分" in text
    assert "entity × item" in text


def test_an_entity_scope_says_it_keeps_every_item(spark):
    text = _text(population.build(*_scoped(
        spark, {"snap_date": "d", "cust_id": "c"})))
    assert "只限 entity" in text and "entity × item" not in text


def test_the_unscoped_page_says_nothing_of_a_scope(spark):
    assert "範圍" not in _text(_draw(spark))
