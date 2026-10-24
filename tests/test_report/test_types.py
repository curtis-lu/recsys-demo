"""report.types：``ReportSection``（從 ``evaluation.report`` 搬來）。診斷頁用的
``ScopeNote``／``Page`` 隨 #532 拿掉。"""

from recsys_tfb.report.types import ReportSection


def test_old_import_path_still_works_and_is_same_class():
    from recsys_tfb.evaluation.report import ReportSection as Old

    assert Old is ReportSection


def test_optional_fields_default_empty():
    """只傳 title/description 就能建；其餘欄位可選。"""
    section = ReportSection(title="T", description="d")
    assert (section.title_en, section.question, section.key) == ("", "", "")
    assert section.blocks == []


def test_blocks_default_is_not_shared_between_instances():
    """``blocks`` 用 ``field(default_factory=list)`` 而不是可變預設值——共用同
    一個 list 的話，一個 section append 會污染所有其他 section。"""
    a = ReportSection(title="A", description="d")
    b = ReportSection(title="B", description="d")
    a.blocks.append("只屬於 A")
    assert b.blocks == []
