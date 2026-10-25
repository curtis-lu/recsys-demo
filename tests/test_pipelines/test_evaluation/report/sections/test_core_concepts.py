"""核心概念 (``report/sections/core_concepts``, #537): the query group and
AP defined once, and the page's totals — the one place the whole data's
counts are printed (ADR-0031 decision 16 item 12) — with the all-positive
warning under them (moved from the page's metadata).

Most cases draw ``report/test_main``'s row facts and bundles, which carry
the keys ``compute_row_facts`` / ``compute_all_metrics`` write; the last one
draws the section off a real computation (event rows and categories).
"""
from __future__ import annotations

import re

import pytest

from recsys_tfb.pipelines.evaluation.report.main import SECTIONS
from recsys_tfb.pipelines.evaluation.report.sections import core_concepts
from recsys_tfb.report.pages import render_report_table
from recsys_tfb.report.types import (
    Bullets,
    Cell,
    Formula,
    Note,
    Paragraph,
    Table,
)
from tests.test_pipelines.test_evaluation.report import test_main as rbt

_SWITCH = "evaluation.query_filter.drop_all_positive_groups"


def _build(results, params):
    return core_concepts.build(results, params)


def _totals(section) -> Table:
    tables = [b for b in section.blocks if isinstance(b, Table)]
    assert [t.title for t in tables] == ["總量"]
    return tables[0]


def _rows(section) -> list[tuple]:
    """``(label, value)`` of each row; a ``Cell`` is unwrapped to its value."""
    return [(r.label, r.cells[0].value if isinstance(r.cells[0], Cell)
             else r.cells[0]) for r in _totals(section).rows]


def _bullets(section) -> tuple[str, ...]:
    (bullets,) = [b for b in section.blocks if isinstance(b, Bullets)]
    return bullets.items


def _paragraphs(section) -> list[str]:
    return [b.text for b in section.blocks if isinstance(b, Paragraph)]


def _text(section) -> str:
    parts = []
    for b in section.blocks:
        if isinstance(b, (Paragraph, Formula, Note)):
            parts.append(b.text)
        elif isinstance(b, Bullets):
            parts += b.items
        elif isinstance(b, Table):
            parts += [r.label for r in b.rows] + [n.text for n in b.notes]
    return " ".join(parts)


def _warnings(section) -> list[str]:
    return [n.text for n in _totals(section).notes if n.warn]


# ---- the totals -----------------------------------------------------------


def test_the_totals_are_the_row_facts_as_landed():
    """Every count distinct, so a row reading the wrong key shows: 1,000
    groups, 930 holding a positive (30 of them all-positive), 70 without.
    "全部＝@幾" is the metrics' recorded K, written with the @ (item 7)."""
    r = rbt._ap_results(n_queries=1000, n_excluded=70, n_all_positive=30)
    s = _build(r, rbt._ap_params(False))
    assert _rows(s) == [
        ("全部 query group", 1000),
        ("有正例的 query group", 930),
        ("　其中全正的", 30),
        ("無正例的 query group", 70),
        ("候選列", 100),
        ("正例數", 20),
        ("entity 數", 10),
        ("item 數", 2),
        ("snap_date 數", 1),
        ("全部＝@幾", "@2"),
    ]
    assert [(c.label, c.kind) for c in _totals(s).columns] == [("值", "count")]


def test_the_category_grain_adds_its_all_positive_groups_and_its_count():
    r = rbt._ap_results(n_queries=1000, n_excluded=70, n_all_positive=30,
                        category=rbt._ap_category(n_all_positive=300))
    rbt._set_n_items(r, 5)   # 5 items fold into the category's 2
    rows = dict(_rows(_build(r, rbt._ap_params(False))))
    assert rows["大類粒度裡全正的 query group"] == 300
    assert rows["大類數"] == 2
    assert rows["item 數"] == 5
    labels = [label for label, _ in _rows(_build(r, rbt._ap_params(False)))]
    assert labels.index("無正例的 query group") < labels.index(
        "大類粒度裡全正的 query group") < labels.index("候選列")
    assert labels.index("item 數") < labels.index("大類數") < labels.index(
        "snap_date 數")
    # No category grain, no category rows.
    plain = dict(_rows(_build(rbt._ap_results(), rbt._ap_params(False))))
    assert "大類數" not in plain and "大類粒度裡全正的 query group" not in plain


def test_with_the_switch_on_the_totals_say_which_groups_were_left_out():
    """#376: switched on, the all-positive groups are left out of the
    ranking metrics, and the groups those scored are read off the metrics'
    ledger (#536) — here 930 − 30, the rule the fixture keeps."""
    r = rbt._ap_results(n_queries=1000, n_excluded=70, n_all_positive=30,
                        category=rbt._ap_category(n_all_positive=300),
                        drop=True)
    on = dict(_rows(_build(r, rbt._ap_params(True))))
    assert on["　其中全正的（已排除）"] == 30
    assert on["　其中算進指標的"] == 900
    assert on["大類粒度裡全正的 query group（已排除）"] == 300
    assert "　其中全正的" not in on
    off = dict(_rows(_build(rbt._ap_results(
        n_queries=1000, n_excluded=70, n_all_positive=30), rbt._ap_params(False))))
    assert not [k for k in off if "已排除" in k or "算進指標" in k]


def test_the_section_reads_the_row_facts_and_the_metrics_it_declares():
    assert core_concepts.READS == ("evaluation_row_facts", "evaluation_metrics")
    assert core_concepts in SECTIONS


# ---- what "all" is -------------------------------------------------------


def test_all_is_the_recorded_k_and_the_sentence_says_so():
    """Undeclared: "all" is the item count, said with the landed number.
    With ``event`` rows it is the widest group (4 against 2 items), the
    table row says so too, and a paragraph explains why."""
    r = rbt._ap_results()
    plain = _build(r, rbt._ap_params(False))
    assert _bullets(plain)[-1] == (
        "表頭寫實際的 K。本次每個 query group 最多 2 個候選，所以 @2 就是全部、"
        "不截斷。")

    metrics = rbt._metrics_with_recorded_all_k()
    del metrics["category"]
    landed = rbt._results(evaluation_metrics=metrics)
    landed["evaluation_row_facts"]["dataset_overview"]["totals"][
        "n_query_group_item_combos"] = 4
    ev = _build(landed, rbt._ap_params(False, event=True))
    assert _bullets(ev)[-1] == (
        "表頭寫實際的 K。宣告了 event，全部＝一個 query group 裡最多的候選列數，"
        "本次是 @4。")
    rows = dict(_rows(ev))
    assert (rows["全部＝@幾"], rows["item 數"]) == ("@4", 2)
    assert any(p.startswith("宣告了 event：") for p in _paragraphs(ev))


def test_the_category_bound_is_said_with_its_own_count():
    r = rbt._ap_results(category=rbt._ap_category())
    rbt._set_n_items(r, 5)
    s = _build(r, rbt._ap_params(False))
    assert _bullets(s)[-1].endswith(
        "所以 @5 就是全部、不截斷；大類只有 2 類，大類的表最多到 @2。")
    assert "大類＝把 item 併成大類後重排再算" in _bullets(s)[4]
    assert "大類" not in _bullets(_build(rbt._ap_results(),
                                         rbt._ap_params(False)))[4]


# ---- the query group and the optional roles -------------------------------


@pytest.mark.parametrize("occasion, event", [
    (False, False), (True, False), (False, True), (True, True)])
def test_occasion_and_event_change_only_this_section_s_text(occasion, event):
    """Decision 16 item 14: with ``occasion`` the query group gains its
    column and a paragraph says what that means; with ``event`` a paragraph
    says "all" is the widest group's rows."""
    r = rbt._ap_results()
    totals = r["evaluation_row_facts"]["dataset_overview"]["totals"]
    if occasion:
        totals["n_time_entity_combos"] = 3
    if event:
        totals["n_query_group_item_combos"] = 5
    s = _build(r, rbt._ap_params(False, occasion=occasion, event=event))
    unit = "snap_date × cust_id" + (" × request_id" if occasion else "")
    assert _paragraphs(s)[0].startswith(f"一個 query group＝一組 {unit}。")
    later = _paragraphs(s)[1:]
    assert any(p.startswith("宣告了 occasion：") for p in later) == occasion
    assert any(p.startswith("宣告了 event：") for p in later) == event


def test_the_reader_s_own_column_names_and_no_example_spelling(
    renamed_schema_params,
):
    """#326: the definition prints the reader's own columns, read through
    ``core.schema.get_schema``; the example deployment's spellings never
    appear. The time label of the totals is the reader's time column too."""
    params = {**renamed_schema_params, "evaluation": {}}
    s = _build(rbt._ap_results(), params)
    body = " ".join(_paragraphs(s) + list(_bullets(s)))
    for name in ("as_of_month", "store_id", "sku"):
        assert name in body, name
    for example in ("snap_date", "cust_id", "prod_name"):
        assert example not in body, example
    assert "as_of_month 數" in dict(_rows(s))
    # The roles' own words, never the bank example's business vocabulary.
    for word in ("客戶", "產品"):
        assert word not in _text(s)


def test_a_multi_column_entity_is_joined_not_truncated(two_column_entity_params):
    params = {**two_column_entity_params, "evaluation": {}}
    entity = "×".join(two_column_entity_params["schema"]["columns"]["entity"])
    assert "×" in entity, "the fixture lost its second entity column"
    s = _build(rbt._ap_results(), params)
    assert f"一組 snap_date × {entity}。" in _paragraphs(s)[0]


# ---- AP, worked once -----------------------------------------------------


def test_the_formula_divides_by_r_and_the_hand_check_is_map_at_1():
    """AP@k's denominator is R, not min(k, R) — what ``metrics_spark``
    computes (``map@K = sum(ap_contrib@K) / total_rel``); so map@1 equals
    recall@1, the check a reader can make by hand."""
    s = _build(rbt._ap_results(), rbt._ap_params(False))
    (formula,) = [b.text for b in s.blocks if isinstance(b, Formula)]
    assert "AP@k = (1 / R)" in formula
    assert "(1 / min" not in formula and "1/min" not in formula.replace(" ", "")
    bullets = _bullets(s)
    assert bullets[0].startswith("例：某 query 有 4 個候選 prod_name、R=2 個正例")
    assert "map@1 會等於 recall@1" in bullets[3]
    for bad in ("偏高", "偏低", "不足", "異常", "達標", "嚴重", "良好"):
        assert bad not in _text(s), bad


def test_which_groups_the_ranking_metrics_count_follows_the_switch():
    off = _bullets(_build(rbt._ap_results(), rbt._ap_params(False)))[5]
    assert "只算有正例的 query group；" in off
    on = _bullets(_build(rbt._ap_results(drop=True), rbt._ap_params(True)))[5]
    assert f"只算有正例、而且不是全正的 query group（{_SWITCH} 開著）" in on


# ---- the all-positive warning ---------------------------------------------


def test_the_warning_sits_under_the_totals_with_the_numbers_and_the_switch():
    r = rbt._ap_results(n_queries=1000, n_excluded=0, n_all_positive=101)
    s = _build(r, rbt._ap_params(False))
    (text,) = _warnings(s)
    assert text.startswith("全正 query group（item 粒度）：")
    assert "10.1%" in text and "（101／1,000 組）" in text
    assert "超過 10%" in text
    assert f"設 {_SWITCH}: true" in text
    # Group-level metrics only: the per-item ones still see which rows reach
    # the top K, and "AP is 1" holds only untruncated.
    assert "組層級指標" in text and "照樣算進" in text
    for wrong in ("每組的分數都一樣", "算進 mAP", "AP 都是 1", "滿分", "偏高",
                  "嚴重", "建議", "應該"):
        assert wrong not in text, wrong
    # Drawn as a ⚠ line right under the table, ⚠ added by the component.
    html = render_report_table(_totals(s))
    assert html.endswith(f'<p class="warn-note">⚠ {text}</p>')


def test_the_warning_names_no_other_section():
    """It used to point at 「衡量指標」 and the baseline; a section may point
    back to 核心概念 only, and this is 核心概念."""
    r = rbt._ap_results(n_queries=1000, n_excluded=0, n_all_positive=500,
                        category=rbt._ap_category(n_all_positive=800))
    (item, category) = _warnings(_build(r, rbt._ap_params(False)))
    for text in (item, category):
        for title in [m.TITLE for m in SECTIONS] + ["衡量指標", "baseline",
                                                    "段"]:
            assert title not in text, (title, text)


def test_a_share_just_above_ten_percent_never_prints_as_ten_point_zero():
    r = rbt._ap_results(n_queries=10000, n_excluded=0, n_all_positive=1004)
    (text,) = _warnings(_build(r, rbt._ap_params(False)))
    assert "10.0%" not in text and "10.04%" in text


@pytest.mark.parametrize("n_all_positive, drop", [(100, False), (500, True)])
def test_no_warning_at_ten_percent_or_with_the_switch_on(n_all_positive, drop):
    r = rbt._ap_results(n_queries=1000, n_excluded=0,
                        n_all_positive=n_all_positive, drop=drop,
                        category=rbt._ap_category(n_all_positive=800))
    s = _build(r, rbt._ap_params(drop))
    if not drop:   # the category grain's 800 of 900 still warns
        assert [t.split("：")[0] for t in _warnings(s)] == [
            "全正 query group（大類粒度）"]
    else:
        assert _warnings(s) == []


def test_the_warning_is_judged_per_grain():
    # item 50 / 950 = 5.3 %, category 300 / 900 = 33.3 %: the category only.
    r = rbt._ap_results(category=rbt._ap_category(n_all_positive=300))
    (text,) = _warnings(_build(r, rbt._ap_params(False)))
    assert text.startswith("全正 query group（大類粒度）：") and "33.3%" in text
    # item 190 / 950 = 20 %, category 45 / 900 = 5 %: the item grain only.
    r = rbt._ap_results(n_all_positive=190,
                        category=rbt._ap_category(n_all_positive=45))
    (text,) = _warnings(_build(r, rbt._ap_params(False)))
    assert text.startswith("全正 query group（item 粒度）：")


def test_the_page_prints_the_warning_once_inside_this_section_not_the_metadata():
    r = rbt._ap_results(n_queries=1000, n_excluded=0, n_all_positive=101)
    html = rbt._assemble(r, rbt._ap_params(False))
    assert html.count("全正 query group（item 粒度）") == 1
    core = html.index('id="s-core_concepts"')
    nxt = html.index('<div class="section', core + 1)
    assert core < html.index("全正 query group（item 粒度）") < nxt
    meta = html[html.index('<div class="metadata">'):html.index("</nav>")]
    assert "全正" not in meta


# ---- off a real computation -----------------------------------------------


def test_the_totals_off_a_real_computation_with_event_rows_and_categories(spark):
    """``_wide_event_df``: c1 shown A three times and B once, clicked B; c2
    no click; c3 shown C once, clicked it. Three groups, two holding a
    positive, c3 all-positive (its one row is positive); at the category grain
    c1 collapses to one AB row, positive, so two all-positive there. Six
    rows, two positives, three entities, three items, two categories, one
    date; "all" is the widest group, 4 rows (#434). Worked out by hand."""
    from tests.test_evaluation.test_metrics_spark_overview import (
        _both,
        _event_params_with_categories,
        _wide_event_df,
    )

    params = _event_params_with_categories()
    facts, metrics = _both(_wide_event_df(spark), params)
    s = _build({"evaluation_row_facts": facts, "evaluation_metrics": metrics},
               params)
    assert _rows(s) == [
        ("全部 query group", 3),
        ("有正例的 query group", 2),
        ("　其中全正的", 1),
        ("無正例的 query group", 1),
        ("大類粒度裡全正的 query group", 2),
        ("(query group, prod_name) 的組合", 4),
        ("候選列", 6),
        ("正例數", 2),
        ("entity 數", 3),
        ("item 數", 3),
        ("大類數", 2),
        ("snap_date 數", 1),
        ("全部＝@幾", "@4"),
    ]
    assert re.search(r"本次是 @4；大類只有 2 類，大類的表最多到 @2。$",
                     _bullets(s)[-1])
    # 1 of 2 at the item grain, 2 of 2 at the category grain: both warn.
    assert [t.split("：")[0] for t in _warnings(s)] == [
        "全正 query group（item 粒度）", "全正 query group（大類粒度）"]


def test_the_all_cell_is_text_so_the_copy_reads_the_at_sign():
    """The value column is a count; the 全部＝@幾 cell is drawn as the text,
    no ``data-v``, while the counts above it keep theirs."""
    r = rbt._ap_results(n_queries=1000, n_excluded=70, n_all_positive=30)
    html = render_report_table(_totals(_build(r, rbt._ap_params(False))))
    assert "<td>@2</td>" in html
    assert 'data-v="1000"' in html


def test_the_totals_with_occasion_count_time_entity_combinations(spark):
    """``_occasion_df`` off a real computation: five rows, three occasions,
    two time x entity combinations (c1 twice-requested). The row sits after
    the groups and before the candidate rows; no event, no
    (query group, item) row."""
    from tests.test_evaluation.test_metrics_spark_overview import (
        _both,
        _occasion_df,
        _occasion_params,
    )

    params = _occasion_params()
    facts, metrics = _both(_occasion_df(spark), params)
    rows = _rows(_build(
        {"evaluation_row_facts": facts, "evaluation_metrics": metrics},
        params))
    labels = [label for label, _ in rows]
    assert dict(rows)["snap_date × cust_id 的組合"] == 2
    assert dict(rows)["全部 query group"] == 3
    assert labels.index("無正例的 query group") < labels.index(
        "snap_date × cust_id 的組合") < labels.index("候選列")
    assert not [label for label in labels if label.startswith("(query group")]


def test_both_roles_print_occasion_before_event():
    r = rbt._ap_results()
    totals = r["evaluation_row_facts"]["dataset_overview"]["totals"]
    totals["n_time_entity_combos"] = 7
    totals["n_query_group_item_combos"] = 9
    params = rbt._ap_params(False)
    params["schema"]["columns"].update(occasion="req_id", event="imp_id")
    labels = [label for label, _ in _rows(_build(r, params))]
    assert labels[labels.index("候選列") - 2:labels.index("候選列")] == [
        "snap_date × cust_id 的組合", "(query group, prod_name) 的組合"]
    assert dict(_rows(_build(r, params)))["(query group, prod_name) 的組合"] == 9


@pytest.mark.parametrize("role, key", [
    ("occasion", "n_time_entity_combos"),
    ("event", "n_query_group_item_combos"),
])
def test_a_declared_role_without_its_landed_count_is_refused(role, key):
    """Landed before the count existed: the fingerprint passes, the row would
    just vanish. Refused naming the key and the node to re-run; a role not
    declared asks for nothing."""
    r = rbt._ap_results()
    params = rbt._ap_params(False)
    _build(r, params)                      # undeclared: passes
    params["schema"]["columns"][role] = "x_col"
    with pytest.raises(ValueError, match=rf"{key}.*compute_row_facts"):
        _build(r, params)
