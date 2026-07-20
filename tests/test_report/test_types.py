"""report.types：中性呈現層的三個型別（ReportSection 搬移＋ScopeNote 契約＋Page）。"""

import pytest

from recsys_tfb.report.types import Page, ReportSection, ScopeNote


def test_scope_note_blind_to_empty_raises():
    with pytest.raises(ValueError) as exc:
        ScopeNote(measures="m", population="p", blind_to=())
    assert "blind_to" in str(exc.value)


def test_scope_note_blind_to_string_raises_type_error():
    # 字串是 Iterable[str]，若不擋會被 pages._render_scope_note 逐字元
    # 拆成一堆單字元 <li>，靜默壞掉而不噴錯——這是最容易誤用的地方。
    with pytest.raises(TypeError) as exc:
        ScopeNote(measures="m", population="p", blind_to="不能推論因果")
    msg = str(exc.value)
    assert "blind_to" in msg
    assert "tuple" in msg or "list" in msg


def test_scope_note_reference_points_string_raises_type_error():
    with pytest.raises(TypeError) as exc:
        ScopeNote(
            measures="m", population="p", blind_to=("x",),
            reference_points="baseline model",
        )
    msg = str(exc.value)
    assert "reference_points" in msg
    assert "tuple" in msg or "list" in msg


def test_scope_note_normal_construction_and_sampling_default():
    note = ScopeNote(
        measures="每 item 的排序品質",
        population="有正例的 query",
        blind_to=("item 之外的因素",),
    )
    assert note.sampling == ""
    assert note.reference_points == ()
    assert note.blind_to == ("item 之外的因素",)


def test_scope_note_sampling_can_be_set():
    note = ScopeNote(
        measures="m",
        population="p",
        blind_to=("x",),
        reference_points=("baseline",),
        sampling="分層抽樣：...",
    )
    assert note.sampling == "分層抽樣：..."
    assert note.reference_points == ("baseline",)


def test_page_construction():
    section = ReportSection(title="T", description="d")
    note = ScopeNote(measures="m", population="p", blind_to=("x",))
    page = Page(slug="01-config-shift", title="Config Shift", scope=note,
                sections=(section,))
    assert page.slug == "01-config-shift"
    assert page.sections == (section,)
    assert page.scope is note


def test_page_scope_can_be_none():
    section = ReportSection(title="T", description="d")
    page = Page(slug="s", title="T", scope=None, sections=(section,))
    assert page.scope is None


def test_old_import_path_still_works_and_is_same_class():
    from recsys_tfb.evaluation.report import ReportSection as Old

    assert Old is ReportSection


def test_formula_and_bullets_are_optional_with_empty_defaults():
    """新欄位必須可選——``report_builder`` 有 13 個既有的 ``build_*_section``
    只傳 title/description，加必填欄位會讓它們全部 TypeError。
    """
    section = ReportSection(title="T", description="d")
    assert section.formula == ""
    assert section.bullets == []


def test_bullets_default_is_not_shared_between_instances():
    """``bullets`` 用 ``field(default_factory=list)`` 而不是可變預設值。

    共用同一個 list 的話，一個 section append 會污染所有其他 section——
    那是沒有錯誤訊息的 bug。
    """
    a = ReportSection(title="A", description="d")
    b = ReportSection(title="B", description="d")
    a.bullets.append("只屬於 A")
    assert b.bullets == []
