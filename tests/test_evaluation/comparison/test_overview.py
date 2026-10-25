"""概覽 of the comparison report (``evaluation/comparison/overview.py``,
#537): the description, 「跟誰比」, 「涵蓋」, 「拿掉的 item」 and 「能不能比」.
Inputs: ``fixtures.inputs`` (the real comparison and cut nodes; the
coverage in ``restrict_to_common``'s shape)."""
from __future__ import annotations

import pytest

from recsys_tfb.evaluation.comparison import overview
from recsys_tfb.report.types import Paragraph
from tests.test_evaluation.comparison import fixtures as fx


def _section(p, this=None, reference=None, **kw):
    results = fx.inputs(this or fx.bundle(0.6), reference or fx.bundle(0.5),
                        p, **kw)
    return fx.read(overview, results, p)


def _cells(t) -> dict:
    return {r.label: r.cells for r in t.rows}


def test_whom_against_an_external_table():
    """Every key that decides what the compared side holds, from its landed
    ``compare_source``; this side's kind, model_version and source from the
    coverage; nothing else, a blank as —. ``label`` is not a row."""
    t = fx.table(_section(fx.params(fx.EXTERNAL)), "跟誰比")
    assert t.row_header == "項目"
    assert [c.label for c in t.columns] == ["這次", "對照"]
    assert _cells(t) == {
        "kind": ("model_version", "external_hive"),
        "model_version": (fx.THIS_MODEL_VERSION, "—"),
        "table": ("—", "other_team.scores"),
        "source": ("enriched_eval_predictions", "—"),
        "columns": ("—", "{acct: acct_key, as_of: dt, branch: br, "
                         "prod_name: item_code, score: pred_score}"),
        "prod_mapping": ("—", "{ext_a: item_A, ext_b: item_B, ext_c: item_C}"),
        "unmapped_policy": ("—", "drop"),
    }


def test_whom_against_another_model_version():
    t = fx.table(_section(fx.params(fx.OTHER_MODEL)), "跟誰比")
    assert _cells(t) == {
        "kind": ("model_version", "model_version"),
        "model_version": (fx.THIS_MODEL_VERSION, "20260131_v6"),
        # The table B was read off is part of its identity.
        "table": ("—", "ranked_predictions"),
        "source": ("enriched_eval_predictions", "ranked_predictions"),
        "columns": ("—", "—"),
        "prod_mapping": ("—", "—"),
        "unmapped_policy": ("—", "—"),
    }


def test_coverage_prints_the_landed_counts_and_no_na():
    """Both sides' totals and the common ones, read off the coverage — the
    old table printed this side's table as "n/a" (a key no node lands)."""
    p = fx.params()
    sec = _section(p, cov=fx.coverage(p, n_query_group_A_full=5000,
                                      n_item_B_full=7))
    t = fx.table(sec, "涵蓋")
    assert [(c.label, c.kind) for c in t.columns] == [
        ("這次", "count"), ("對照", "count"), ("兩邊共同", "count")]
    assert _cells(t) == {"query group 數": (5000, 4608, 4600),
                         "item 數": (4, 7, 3)}
    every_cell = [c for table in fx.tables(sec) for r in table.rows
                  for c in r.cells]
    assert every_cell and not any("n/a" in str(c) for c in every_cell)


def test_a_coverage_without_this_side_s_source_is_refused():
    """A coverage landed before #537 has no ``source_a``: refused with the
    node whose slice lands it again, not printed as a blank."""
    p = fx.params()
    cov = fx.coverage(p)
    del cov["source_a"]
    with pytest.raises(ValueError) as exc:
        _section(p, cov=cov)
    msg = str(exc.value)
    assert "compare_coverage has no source_a" in msg
    assert "--from-node compute_compare_metrics" in msg


def test_dropped_items_both_sides():
    p = fx.params()
    t = fx.table(_section(p, cov=fx.coverage(
        p, dropped_items_A=["item_D", "item_E"], dropped_items_B=[])),
        "拿掉的 item")
    assert _cells(t) == {"數量": (2, 0), "是哪些": ("item_D、item_E", "—")}


def test_the_comparability_table_is_the_shared_one():
    t = fx.table(_section(fx.params()), "能不能比")
    assert [r.label for r in t.rows] == [
        "全部＝@幾", "進 Macro 的 item", "算進指標的 query group", "分群欄"]
    assert [r.cells[2] for r in t.rows] == ["相同"] * 4


def _description(sec) -> str:
    first = sec.blocks[0]
    assert isinstance(first, Paragraph) and first.muted
    return first.text


@pytest.mark.parametrize("occasion, unit", [
    (None, "一個 query group＝一組 as_of × acct × branch。"),
    ("req_id", "一個 query group＝一組 as_of × acct × branch × req_id。"),
])
def test_the_query_group_is_named_in_the_schema_s_columns(occasion, unit):
    assert unit in _description(_section(fx.params(occasion=occasion)))


def test_all_is_the_landed_k():
    """"@N 就是全部": N is what this side's "all" resolved to, off the
    comparison; "共 M 個" the common items, off the coverage. With event
    rows the two differ, and the sentence says what "all" then is — this
    side's (the reference's is @8 here)."""
    p = fx.params()
    plain = _description(_section(p))
    assert "item 取兩邊的交集，共 3 個，所以表頭的 @3 就是全部。" in plain
    event = _description(_section(p, this=fx.bundle(0.6, all_k=7),
                                  reference=fx.bundle(0.5, all_k=8)))
    assert ("共 3 個；表頭的 @7 就是全部：一個 query group 裡最多的候選列數。"
            in event)


def test_the_all_positive_switch_is_said_only_when_on():
    """#376: on, both sides dropped their all-positive groups, each on its
    own rows; off, the section does not mention them."""
    on = _section(fx.params(drop_all_positive=True))
    texts = [b.text for b in on.blocks if isinstance(b, Paragraph)]
    assert any("evaluation.query_filter.drop_all_positive_groups 開著" in t
               and "各自依自己的列判定" in t and "一邊排除、一邊保留" in t
               for t in texts), texts
    for switch in (None, False):
        off = _section(fx.params(drop_all_positive=switch))
        assert not any("全正" in b.text for b in off.blocks
                       if isinstance(b, Paragraph))


def test_the_section_declares_its_heading():
    sec = _section(fx.params())
    assert (sec.title, sec.title_en, sec.key) == ("概覽", "Overview", "overview")
    assert sec.question == overview.QUESTION and sec.question.endswith("？")
