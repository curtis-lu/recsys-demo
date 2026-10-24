"""report.types.Table drawn by report.pages (#537): the number formats, the
two-row header, pinned rows, the ⚠ marks, tall tables and escaping.

The expected strings come from ADR-0031 decision 16's rules (the design's
〈版面規則〉), not from the renderer: counts ``4,800``; ratios four decimals;
differences signed, the minus U+2212, zero ``0.0000``; ``data-v`` six
decimals with a plain ``-``.
"""
from __future__ import annotations

from html.parser import HTMLParser

import pytest

from recsys_tfb.report.pages import render_block, render_report_table
from recsys_tfb.report.types import Cell, Column, Note, Row, Table

MINUS = "−"


class _Cells(HTMLParser):
    """Every ``th``/``td`` of a rendered table: its attributes, its text
    without the ⚠ marks, and whether it carries one."""

    def __init__(self):
        super().__init__()
        self.cells: list[dict] = []
        self._cur = None
        self._in_sup = False

    def handle_starttag(self, tag, attrs):
        if tag in ("th", "td"):
            self._cur = {"tag": tag, "attrs": dict(attrs), "text": "",
                         "warn": False}
            self.cells.append(self._cur)
        elif tag == "sup" and dict(attrs).get("class") == "warn":
            self._in_sup = True
            if self._cur is not None:
                self._cur["warn"] = True

    def handle_endtag(self, tag):
        if tag in ("th", "td"):
            self._cur = None
        elif tag == "sup":
            self._in_sup = False

    def handle_data(self, data):
        if self._cur is not None and not self._in_sup:
            self._cur["text"] += data


def _cells(html: str) -> list[dict]:
    parser = _Cells()
    parser.feed(html)
    return parser.cells


def _tds(html: str) -> list[dict]:
    return [c for c in _cells(html) if c["tag"] == "td"]


def _one_column(kind: str, values) -> str:
    return render_report_table(Table(
        "t", "列", (Column("v", kind),),
        tuple(Row(f"r{i}", (v,)) for i, v in enumerate(values))))


def test_count_cells_are_integers_with_thousands_separators():
    tds = _tds(_one_column("count", [4800, 1234567, 7, 12.0]))
    assert [c["text"] for c in tds] == ["4,800", "1,234,567", "7", "12"]
    assert [c["attrs"]["data-v"] for c in tds] == ["4800", "1234567", "7", "12"]
    assert all(c["attrs"]["class"] == "num" for c in tds)


def test_ratio_cells_have_four_decimals_and_six_in_data_v():
    tds = _tds(_one_column("ratio", [0.795343, 0.5, 1.0]))
    assert [c["text"] for c in tds] == ["0.7953", "0.5000", "1.0000"]
    assert [c["attrs"]["data-v"] for c in tds] == [
        "0.795343", "0.500000", "1.000000"]


def test_diff_cells_carry_a_sign_and_a_true_minus():
    tds = _tds(_one_column("diff", [0.035282, -0.020468]))
    assert [c["text"] for c in tds] == ["+0.0353", f"{MINUS}0.0205"]
    assert all(c["attrs"]["class"] == "num dl" for c in tds)


def test_a_diff_that_rounds_to_zero_is_written_unsigned():
    tds = _tds(_one_column("diff", [0.0, -0.00001, 0.00004]))
    assert [c["text"] for c in tds] == ["0.0000", "0.0000", "0.0000"]


def test_data_v_has_a_plain_minus_and_never_minus_zero():
    tds = _tds(_one_column("diff", [-0.020468, -1e-9, -0.00001]))
    assert [c["attrs"]["data-v"] for c in tds] == [
        "-0.020468", "0.000000", "-0.000010"]


def test_a_missing_value_is_a_blank_cell():
    tds = _tds(_one_column("ratio", [None, float("nan")]))
    assert [(c["text"], c["attrs"]["data-v"]) for c in tds] == [("", ""),
                                                                ("", "")]


def _compared() -> Table:
    columns = (
        Column("這次", "ratio", group="@1"), Column("對照", "ratio", group="@1"),
        Column("差", "diff", group="@1"),
        Column("這次", "ratio", group="@3"), Column("對照", "ratio", group="@3"),
        Column("差", "diff", group="@3"),
    )
    return Table("map@K · 整體", "分群", columns,
                 (Row("整體", (0.5, 0.4, 0.1, 0.6, 0.55, 0.05), pinned=True),
                  Row("seg_1", (0.4, 0.5, -0.1, None, None, None))))


def test_two_row_header_merges_adjacent_groups_and_flattens_data_full():
    html = render_report_table(_compared())
    ths = [c for c in _cells(html) if c["tag"] == "th" and "rl" not in
           c["attrs"].get("class", "")]
    groups = [(c["text"], c["attrs"].get("colspan")) for c in ths
              if "grp" in c["attrs"].get("class", "")]
    assert groups == [("@1", "3"), ("@3", "3")]
    full = [c["attrs"]["data-full"] for c in ths if "data-full" in c["attrs"]]
    assert full == ["分群", "@1 這次", "@1 對照", "@1 差",
                    "@3 這次", "@3 對照", "@3 差"]
    # the row header spans both header rows
    assert ths[0]["attrs"].get("rowspan") == "2"


def test_pinned_row_is_marked_pin():
    html = render_report_table(_compared())
    assert '<tr class="pin"><th class="rl">整體</th>' in html
    assert '<tr><th class="rl">seg_1</th>' in html


def test_warn_mark_stays_out_of_data_full_and_data_v():
    table = Table("t", "item", (Column("@1", "ratio", warn=True),),
                  (Row("Macro 平均", (Cell(0.5, warn=True),), warn=True),))
    cells = _cells(render_report_table(table))
    header = next(c for c in cells if c["tag"] == "th"
                  and c["attrs"].get("class") == "srt" and c["text"] == "@1")
    label = next(c for c in cells if c["attrs"].get("class") == "rl")
    td = next(c for c in cells if c["tag"] == "td")
    assert header["warn"] and label["warn"] and td["warn"]   # ⚠ is drawn
    assert all("⚠" not in c["attrs"].get("data-full", "") for c in cells)
    assert td["attrs"]["data-v"] == "0.500000"


def test_a_column_group_all_warned_carries_the_mark_on_its_group_header():
    columns = tuple(Column(lbl, "ratio", group="@5", warn=True)
                    for lbl in ("這次", "對照")) + (
        Column("差", "diff", group="@5", warn=True),)
    cells = _cells(render_report_table(
        Table("t", "item", columns, (Row("A", (0.1, 0.2, -0.1)),))))
    group = next(c for c in cells if "grp" in c["attrs"].get("class", ""))
    leaves = [c for c in cells if c["attrs"].get("data-full", "").startswith("@5")]
    assert group["warn"] and not any(c["warn"] for c in leaves)


@pytest.mark.parametrize("n, tall", [(15, False), (16, True)])
def test_more_than_15_rows_is_a_tall_table(n, tall):
    table = Table("t", "item", (Column("v", "count"),),
                  tuple(Row(f"i{i}", (i,)) for i in range(n)))
    html = render_report_table(table)
    assert ('<div class="tw tall">' in html) is tall
    assert ('<div class="tw">' in html) is not tall


def test_strings_are_escaped():
    table = Table("<b>表</b>", "<i>列</i>",
                  (Column("<u>欄</u>", "text", group="<g>"),),
                  (Row("<b>item</b>", ("<script>x</script>",)),),
                  notes=(Note("<b>說明</b>", warn=True),))
    html = render_report_table(table)
    for raw in ("<b>", "<i>", "<u>", "<g>", "<script>"):
        assert raw not in html, raw
    assert "&lt;b&gt;表&lt;/b&gt;" in html
    assert '<th class="rl">&lt;b&gt;item&lt;/b&gt;</th>' in html
    assert "<td>&lt;script&gt;x&lt;/script&gt;</td>" in html
    assert '<p class="warn-note">⚠ &lt;b&gt;說明&lt;/b&gt;</p>' in html


def test_a_warn_text_cell_reads_warn_different_and_copies_without_the_mark():
    html = render_block(Table("能不能比", "檢查", (Column("結果", "text"),),
                              (Row("全部＝@幾", (Cell("不同", warn=True),)),)))
    assert '<td><span class="warn"><sup class="warn">⚠</sup> 不同</span></td>' in html
    td = _tds(html)[0]
    assert td["text"].strip() == "不同" and "data-v" not in td["attrs"]


def test_a_cell_kind_overrides_the_column_kind_for_that_one_cell():
    """A count column whose one cell is the text ``@6``: the others stay
    counts (thousands separator, ``data-v``); the text cell is its words, no
    ``data-v``, and a text cell with ``warn`` still reads warn-first."""
    html = render_block(Table("總量", "項目", (Column("值", "count"),), (
        Row("候選列", (4800,)),
        Row("全部＝@幾", (Cell("@6", kind="text"),)),
        Row("比例", (Cell(0.5, kind="ratio"),)),
    )))
    count, text, ratio = _tds(html)
    assert (count["text"], count["attrs"]["data-v"]) == ("4,800", "4800")
    assert text["text"].strip() == "@6" and "data-v" not in text["attrs"]
    assert "class" not in text["attrs"]
    assert (ratio["text"], ratio["attrs"]["data-v"]) == (
        "0.5000", "0.500000")


def test_a_cell_kind_that_is_not_a_column_kind_raises():
    with pytest.raises(ValueError, match="Cell.kind"):
        Cell(1, kind="percent")


@pytest.mark.parametrize("build, message", [
    (lambda: Table("t", "r", (Column("a", "ratio"),), (Row("x", (1, 2)),)),
     "有 2 格，欄有 1 個"),
    (lambda: Table("t", "r", (Column("a", "ratio"),),
                   (Row("x", (1,)), Row("整體", (1,), pinned=True))),
     "固定列 '整體' 排在一般列後面"),
    (lambda: Table("t", "r", (Column("a", "ratio", group="@1"),
                              Column("b", "ratio")), ()),
     "沒有上層欄頭，卻排在有上層欄頭的欄後面"),
], ids=["cell-count", "pinned-after-free", "ungrouped-after-grouped"])
def test_a_table_of_the_wrong_shape_is_refused_when_built(build, message):
    with pytest.raises(ValueError, match=message):
        build()
