"""module_map and ast_compare: reading a module's shape, and proving a move
moved code without changing it."""

import textwrap

from scripts.dev.ast_compare import compare
from scripts.dev.module_map import module_map


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text))
    return path


def test_module_map_lists_who_uses_each_definition(tmp_path):
    mod = _write(tmp_path / "pkg" / "mod.py", '''
        LIMIT = 3

        def helper(x):
            return x + LIMIT

        def section(x):
            return helper(x) * 2
    ''')
    _write(tmp_path / "pkg" / "user.py", '''
        from pkg.mod import section
    ''')
    _write(tmp_path / "pkg" / "other.py", '''
        from pkg import mod as m

        m.helper(1)
    ''')
    entries = {e.name: e for e in module_map(mod, root=tmp_path)}
    assert entries["helper"].used_by == ["section"]
    assert entries["LIMIT"].used_by == ["helper"]
    assert entries["section"].uses == ["helper"]
    assert entries["section"].imported_by == ["pkg/user.py"]
    assert entries["helper"].imported_by == ["pkg/other.py"]
    assert (entries["section"].first_line, entries["section"].last_line) == (7, 8)


_OLD = '''
    def _helper(x):
        """Add one."""
        return x + 1

    def section(x):
        return _helper(x) * 2
'''


def test_a_function_moved_verbatim_is_the_same(tmp_path):
    old = _write(tmp_path / "old.py", _OLD)
    new_a = _write(tmp_path / "new" / "a.py", '''
        def _helper(x):
            """Add one."""
            return x + 1
    ''')
    new_b = _write(tmp_path / "new" / "b.py", '''
        from a import _helper

        def section(x):
            return _helper(x) * 2
    ''')
    result = dict(compare([old], [new_a, new_b]))
    assert result == {"_helper": "SAME", "section": "SAME"}


def test_a_changed_body_is_reported(tmp_path):
    old = _write(tmp_path / "old.py", _OLD)
    new = _write(tmp_path / "new.py", _OLD.replace("x + 1", "x + 2"))
    assert dict(compare([old], [new]))["_helper"] == "CHANGED"


def test_a_rename_is_the_same_only_when_declared(tmp_path):
    old = _write(tmp_path / "old.py", _OLD)
    new = _write(tmp_path / "new.py", _OLD.replace("_helper", "helper"))
    plain = dict(compare([old], [new]))
    assert plain["_helper"] == "OLD-ONLY" and plain["helper"] == "NEW-ONLY"
    assert plain["section"] == "CHANGED"
    renamed = dict(compare([old], [new], renames={"_helper": "helper"}))
    assert renamed == {"helper": "SAME", "section": "SAME"}


def test_docstrings_count_unless_told_otherwise(tmp_path):
    old = _write(tmp_path / "old.py", _OLD)
    new = _write(tmp_path / "new.py", _OLD.replace("Add one.", "Add 1."))
    assert dict(compare([old], [new]))["_helper"] == "CHANGED"
    assert dict(compare([old], [new], ignore_docstrings=True))["_helper"] \
        == "SAME"
