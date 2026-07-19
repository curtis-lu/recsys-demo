"""report.types：中性呈現層的三個型別（ReportSection 搬移＋ScopeNote 契約＋Page）。"""

import pytest

from recsys_tfb.report.types import Page, ReportSection, ScopeNote


def test_scope_note_blind_to_empty_raises():
    with pytest.raises(ValueError) as exc:
        ScopeNote(measures="m", population="p", blind_to=())
    assert "blind_to" in str(exc.value)


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
