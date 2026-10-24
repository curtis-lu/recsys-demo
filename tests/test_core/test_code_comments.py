"""Machine check for docs/agents/code-comments.md: issue numbers in src/ prose.

An issue number in a comment or docstring (``#483``) is history: it says which
change produced the code, which is what the commit and the PR are for. The rule
is "write no new ones"; the existing ones go away as the code around them is
edited. So this pins the total and fails in both directions -- more means a new
one was written, fewer means the ceiling should come down with the cleanup.

What it sees: ``#`` comments and string statements (module / class / function
docstrings and bare strings after an assignment). What it does not see: string
literals that reach a user (log messages, report text) -- those are output, not
prose about the code. And it only counts the number; whether the rest of a
comment follows the document is left to review.
"""

import ast
import io
import re
import tokenize
from pathlib import Path

import recsys_tfb

SRC = Path(__file__).resolve().parents[2] / "src" / "recsys_tfb"

ISSUE_REF = re.compile(r"(?<![\w&])#\d{2,4}\b")

#: Issue numbers in src/ prose on the commit that introduced this test. Lower it
#: when you remove some; never raise it.
ISSUE_REF_CEILING = 560


def _prose(src: str) -> list[tuple[int, str]]:
    """(line, text) of every comment and string statement in ``src``."""
    out = [
        (tok.start[0], tok.string)
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
                out.append((stmt.lineno, stmt.value.value))
    return out


def _issue_refs() -> list[str]:
    hits = []
    for path in sorted(SRC.rglob("*.py")):
        for line, text in _prose(path.read_text()):
            for ref in ISSUE_REF.findall(text):
                hits.append(f"{path.relative_to(SRC.parents[1])}:{line} {ref}")
    return hits


def test_audits_the_tree_it_was_shipped_with():
    """Same guard as test_architecture_constraints: scan this tree, not another."""
    assert SRC == Path(recsys_tfb.__file__).parent


def test_no_new_issue_numbers_in_src_prose():
    hits = _issue_refs()
    assert len(hits) <= ISSUE_REF_CEILING, (
        f"{len(hits)} issue numbers in src/ comments and docstrings, ceiling "
        f"{ISSUE_REF_CEILING}: a new one was written. Put the issue number in "
        f"the commit or PR; in the comment, say what the code must keep doing "
        f"(docs/agents/code-comments.md, rule 2). To find it: "
        f"git diff main -- src | grep -E '^\\+.*#[0-9]{{2,4}}'"
    )
    assert len(hits) == ISSUE_REF_CEILING, (
        f"{len(hits)} issue numbers left, ceiling {ISSUE_REF_CEILING}: some were "
        f"removed. Lower ISSUE_REF_CEILING in {Path(__file__).name} to "
        f"{len(hits)} so they cannot come back."
    )
