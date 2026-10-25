"""完整性檢查 (``report/sections/completeness``, #537): the settings this
run was evaluated with, and what looks measured but is not. The counts it
used to repeat are 核心概念's (ADR-0031 decision 16 item 12), and
``evaluation.metric.k`` is gone from the settings."""
from __future__ import annotations

from recsys_tfb.pipelines.evaluation.report.sections import completeness
from recsys_tfb.report.types import Bullets, Paragraph, Table
from tests.test_pipelines.test_evaluation.report import test_main as rbt


def _build(metrics=None, params=None):
    return completeness.build(
        rbt._results(evaluation_metrics=metrics or rbt._metrics()),
        params or rbt._params())


def _settings(section) -> Table:
    (table,) = [b for b in section.blocks if isinstance(b, Table)]
    assert table.title == "設定"
    return table


def _bullets(section) -> tuple[str, ...]:
    (bullets,) = [b for b in section.blocks if isinstance(b, Bullets)]
    return bullets.items


def _params(**metric):
    p = rbt._params()
    p["evaluation"]["k_values"] = [1, 2, 3, 4, 5, "all"]
    p["evaluation"]["metric"] = metric
    return p


def test_the_settings_as_configured_and_no_metric_k():
    """The values are what the reader wrote, through ``metric_params`` (the
    same reader the computation uses); ``metric.k`` (gone, #537) is not a
    row."""
    s = _build(params=_params(weight_alpha=0.5, min_positives=3,
                              shrinkage_k=10))
    table = _settings(s)
    assert table.row_header == "設定"
    assert [(c.label, c.kind) for c in table.columns] == [
        ("值", "text"), ("意思", "text")]
    assert [(r.label, r.cells) for r in table.rows] == [
        ("k_values", ("[1, 2, 3, 4, 5, all]", "要算哪些 K")),
        ("metric.weight_alpha", (0.5, "item 加權指數 α；0＝item 等權")),
        ("metric.min_positives", (3, "觀察名單門檻；0＝不設")),
        ("metric.shrinkage_k", (10.0, "向 pooled 平均收縮強度；0＝不收縮")),
    ]
    assert not [r for r in table.rows if r.label == "metric.k"]


def test_unwritten_metric_settings_are_their_resolved_defaults():
    rows = {r.label: r.cells[0] for r in _settings(_build(params=_params())).rows}
    assert (rows["metric.weight_alpha"], rows["metric.min_positives"],
            rows["metric.shrinkage_k"]) == (0.0, 0, 0.0)


def test_the_bullets_say_what_is_not_measured_and_no_count():
    s = _build()
    assert _bullets(s) == (
        "有些從排序結果衍生的指標有算、但刻意不放：本框架的目標是排序，不是"
        "機率校準。",
        "每個 query group 有幾個正例的分布，這版沒有算。",
        "全網格時每個 item 都出現在每個 query group，所以各 item 的候選列一樣"
        "多，正例佔比跟正例數同序。",
    )
    (description,) = [b.text for b in s.blocks if isinstance(b, Paragraph)]
    assert description == "這次量了什麼，以及什麼情況下數字看起來正常、其實沒量到。"
    assert completeness.READS == ("evaluation_metrics",)
    # The settings table's rows are pinned by the test above; the counts the
    # old fact table carried are in none of the text either.
    text = " ".join([description, *_bullets(s)])
    for count in ("query 數", "n_queries", "正例列數", "n_items", "item 數"):
        assert count not in text, count
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "嚴重", "良好"):
        assert bad not in text, bad


def test_the_tied_rows_share_off_the_model_s_metrics_without_the_count():
    """#535: the ties are of the model's scores, so they land with its
    metrics (only with ``event`` declared). The share is said; the row count
    is a total, 核心概念's."""
    metrics = {**rbt._metrics(), "n_tied_rows": 123, "tied_row_share": 0.07}
    bullets = _bullets(_build(metrics))
    assert bullets[-1] == ("同分的列佔候選列的 7.0%：這些列的名次由 item／event "
                           "決勝，不是由分數決定。")
    assert "123" not in " ".join(bullets)
    assert len(_bullets(_build())) == 3
