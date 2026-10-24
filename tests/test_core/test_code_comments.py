"""Machine check for docs/agents/code-comments.md: issue numbers in src/ prose.

An issue number in a comment or docstring (``#483``) is usually history: it
says which change produced the code, which is what the commit and the PR are
for. The rule is "write no new ones"; the existing ones go away as the code
around them is edited.

So the existing ones are listed, file by file, in ``issue_refs_in_src.txt``,
and this fails in both directions: a ref that is not on the list is a new one;
a listed ref that is gone means the list should lose that line too. Comparing
per file rather than a total is what stops "removed three, added one" from
passing as a net cleanup.

Not counted: a pointer to a decision recorded on an issue, written as
``#426 decision D`` -- that is a citation, like ``ADR-0025 decision 1``.

What it sees: ``#`` comments and string statements (docstrings and bare
strings after an assignment) under ``src/recsys_tfb``. What it does not see:
``tests/`` and ``scripts/``; strings that reach a user (log messages, report
text); and other spellings of an issue (``GH-483``, an issue URL,
``issue 483``). Whether the rest of a comment follows the document is left to
review.
"""

import ast
import io
import re
import tokenize
from collections import Counter
from pathlib import Path

import recsys_tfb

SRC = Path(__file__).resolve().parents[2] / "src" / "recsys_tfb"
BASELINE = Path(__file__).with_name("issue_refs_in_src.txt")

ISSUE_REF = re.compile(r"(?<![\w&])#\d{2,4}\b")
DECISION_POINTER = re.compile(r"(?<![\w&])#\d{2,4},?\s+decisions?\s+[A-Z0-9]+\b")


def _prose(src: str) -> list[str]:
    """Text of every comment and string statement in ``src``."""
    out = [
        tok.string
        for tok in tokenize.generate_tokens(io.StringIO(src).readline)
        if tok.type == tokenize.COMMENT
    ]
    for node in ast.walk(ast.parse(src)):
        body = getattr(node, "body", None)
        if not isinstance(body, list):  # Lambda / IfExp: body is an expression
            continue
        for stmt in body:
            if (
                isinstance(stmt, ast.Expr)
                and isinstance(stmt.value, ast.Constant)
                and isinstance(stmt.value.value, str)
            ):
                out.append(stmt.value.value)
    return out


def _issue_refs() -> Counter:
    """(path relative to the repo) + " " + ref, one per occurrence."""
    hits = Counter()
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC.parents[1]).as_posix()
        for text in _prose(path.read_text()):
            for ref in ISSUE_REF.findall(DECISION_POINTER.sub("", text)):
                hits[f"{rel} {ref}"] += 1
    return hits


def _baseline() -> Counter:
    return Counter(
        line.strip() for line in BASELINE.read_text().splitlines() if line.strip()
    )


def test_audits_the_tree_it_was_shipped_with():
    """Same guard as test_architecture_constraints: scan this tree, not another."""
    assert SRC == Path(recsys_tfb.__file__).parent


def test_decision_pointer_is_not_counted_but_a_bare_ref_is():
    text = "tie-break is the item alone (spec #426 decision D); added in #483."
    assert ISSUE_REF.findall(DECISION_POINTER.sub("", text)) == ["#483"]


def test_no_new_issue_numbers_in_src_prose():
    new = _issue_refs() - _baseline()
    assert not new, (
        "issue numbers in src/ comments or docstrings that are not in "
        f"{BASELINE.name}:\n  " + "\n  ".join(sorted(new.elements())) + "\n"
        "Put the issue number in the commit or PR; in the comment, say what the "
        "code must keep doing (docs/agents/code-comments.md, 'do not write "
        "history'). A pointer to a decision recorded on an issue is allowed when "
        "written as '#NNN decision X'. If you only moved existing text to another "
        "file, move its line in the list too."
    )


def test_removed_issue_numbers_leave_the_list():
    gone = _baseline() - _issue_refs()
    assert not gone, (
        f"these lines of {BASELINE.name} no longer match anything in src/ -- "
        "delete them so the numbers cannot come back:\n  "
        + "\n  ".join(sorted(gone.elements()))
    )
