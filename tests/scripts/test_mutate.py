"""mutate: break one line, run the tests, put the original text back."""

import pytest

from scripts.dev.mutate import mutate

_MODULE = "def double(x):\n    return x * 2\n"
_TEST = (
    "from double import double\n\n"
    "def test_double():\n"
    "    assert double(3) == 6\n"
)


@pytest.fixture
def project(tmp_path):
    (tmp_path / "double.py").write_text(_MODULE)
    (tmp_path / "test_double.py").write_text(_TEST)
    return tmp_path


def test_a_mutation_the_test_catches_is_reported_red(project):
    result = mutate(project / "double.py", "x * 2", "x * 3",
                    [str(project / "test_double.py")], cwd=project)
    assert result.caught
    assert any("assert 9 == 6" in line for line in result.failure_lines)


def test_a_mutation_the_test_misses_is_reported_green(project):
    result = mutate(project / "double.py", "x * 2", "x + x",
                    [str(project / "test_double.py")], cwd=project)
    assert not result.caught


def test_the_file_is_restored_from_memory_whatever_happens(project):
    """Restored from the text read before the mutation, not from git: a git
    checkout would also wipe uncommitted work in the same file."""
    (project / "double.py").write_text(_MODULE + "# uncommitted\n")
    mutate(project / "double.py", "x * 2", "x * 3",
           [str(project / "test_double.py")], cwd=project)
    assert (project / "double.py").read_text() == _MODULE + "# uncommitted\n"


def test_an_ambiguous_mutation_is_refused_and_nothing_is_written(project):
    (project / "double.py").write_text(_MODULE + "# x * 2\n")
    with pytest.raises(ValueError, match="occurs 2 times"):
        mutate(project / "double.py", "x * 2", "x * 3",
               [str(project / "test_double.py")], cwd=project)
    assert (project / "double.py").read_text() == _MODULE + "# x * 2\n"
