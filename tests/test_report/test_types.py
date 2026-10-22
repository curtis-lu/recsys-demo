"""report.types：``ReportSection``（從 ``evaluation.report`` 搬來）。診斷頁用的
``ScopeNote``／``Page`` 隨 #532 拿掉。"""

from recsys_tfb.report.types import ReportSection


def test_old_import_path_still_works_and_is_same_class():
    from recsys_tfb.evaluation.report import ReportSection as Old

    assert Old is ReportSection


def test_formula_and_bullets_are_optional_with_empty_defaults():
    """新欄位必須可選——``report_sections`` 的各段 ``build``
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
