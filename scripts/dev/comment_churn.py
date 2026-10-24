"""How much of the edit history in src/ is comments and docstrings.

Read-only: it only runs ``git log`` / ``git show`` against the repo it lives
in.

    python scripts/dev/comment_churn.py [REV] [--since YYYY-MM-DD]

"Prose" is every ``#`` comment plus every string statement (docstrings and bare
strings after an assignment). A modified file counts as **prose-only** when its
AST, with those string statements removed, is identical before and after --
comments never reach the AST, so only the code is compared.
"""

import argparse
import ast
import collections
import difflib
import io
import re
import subprocess
import tokenize
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def git(*args):
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True)


def show(rev, path):
    r = git("show", f"{rev}:{path}")
    return r.stdout if r.returncode == 0 else None


def _string_stmts(tree):
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(body, list):
            for stmt in body:
                if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)
                        and isinstance(stmt.value.value, str)):
                    yield node, stmt


def code_only(src):
    """ast.dump with every string statement removed; None if it does not parse."""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    for node, stmt in list(_string_stmts(tree)):
        node.body = [s for s in node.body if s is not stmt] or [ast.Pass()]
    return ast.dump(tree)


def prose_lines(src):
    """1-based line numbers that are comment or string-statement lines."""
    try:
        lines = {t.start[0] for t in tokenize.generate_tokens(io.StringIO(src).readline)
                 if t.type == tokenize.COMMENT}
        tree = ast.parse(src)
    except (SyntaxError, tokenize.TokenError):
        return None
    for _, stmt in _string_stmts(tree):
        lines.update(range(stmt.lineno, stmt.end_lineno + 1))
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("rev", nargs="?", default="HEAD")
    ap.add_argument("--since", default="2026-08-01")
    a = ap.parse_args()

    by_month = collections.defaultdict(collections.Counter)
    changed = collections.Counter()
    prose_only_commits = {}
    log = git("log", "--no-merges", "--format=%H|%ad|%s", "--date=short", a.rev).stdout
    for entry in log.splitlines():
        sha, date, subject = entry.split("|", 2)
        names = git("diff-tree", "--no-commit-id", "-r", "--name-only", f"{sha}^", sha).stdout
        for path in names.split():
            if not (path.startswith("src/") and path.endswith(".py")):
                continue
            old, new = show(f"{sha}^", path), show(sha, path)
            if old is None or new is None:
                continue
            co, cn = code_only(old), code_only(new)
            kind = "prose_only" if co is not None and co == cn else "code"
            by_month[date[:7]][kind] += 1
            if date < a.since:
                continue
            if kind == "prose_only":
                prose_only_commits[sha] = subject
            po, pn = prose_lines(old), prose_lines(new)
            if po is None or pn is None:
                continue
            ol, nl = old.splitlines(), new.splitlines()
            ops = difflib.SequenceMatcher(None, ol, nl, autojunk=False).get_opcodes()
            for tag, i1, i2, j1, j2 in ops:
                if tag == "equal":
                    continue
                for i in range(i1, i2):
                    if ol[i].strip():
                        changed["prose" if i + 1 in po else "code"] += 1
                for j in range(j1, j2):
                    if nl[j].strip():
                        changed["prose" if j + 1 in pn else "code"] += 1

    print("Modified src/ .py files whose code did not change, by month:")
    for month in sorted(by_month):
        c = by_month[month]
        total = sum(c.values())
        print(f"  {month}  {c['prose_only']:4d}/{total:4d}  {100 * c['prose_only'] / total:3.0f}%")

    total = sum(changed.values())
    print(f"\nSince {a.since}, changed non-blank lines in modified src/ files: "
          f"{changed['prose']}/{total} prose ({100 * changed['prose'] / total:.0f}%)")
    review = [s for s in prose_only_commits.values() if re.search(r"審查|review|收尾", s)]
    print(f"Commits since {a.since} with at least one prose-only src/ file: "
          f"{len(prose_only_commits)}; subject mentions 審查/review/收尾: {len(review)}")

    def stock_at(rev):
        stock = collections.Counter()
        for path in git("ls-tree", "-r", "--name-only", rev, "src/").stdout.split():
            src = show(rev, path) if path.endswith(".py") else None
            pl = prose_lines(src) if src is not None else None
            if pl is None:
                continue
            nonblank = {i for i, line in enumerate(src.splitlines(), 1) if line.strip()}
            stock["prose"] += len(nonblank & pl)
            stock["all"] += len(nonblank)
        return stock

    start = git("rev-list", "-1", "--first-parent", f"--before={a.since}", a.rev).stdout.strip()
    print()
    for label, rev in ((f"{a.since} ({start[:8]})", start), (a.rev, a.rev)):
        s = stock_at(rev)
        print(f"At {label}: src/ non-blank lines that are prose: "
              f"{s['prose']}/{s['all']} ({100 * s['prose'] / s['all']:.0f}%)")

    long_docs = 0
    docs = collections.Counter()
    for path in git("ls-tree", "-r", "--name-only", a.rev, "src/").stdout.split():
        if not path.endswith(".py"):
            continue
        src = show(a.rev, path)
        if prose_lines(src) is None:
            continue
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                doc = ast.get_docstring(node, clean=False)
                body = node.body[1:] if doc else node.body
                if doc:
                    docs["all"] += 1
                    docs["over_10"] += doc.count("\n") + 1 > 10
                if not doc or not body:
                    continue
                doc_lines = doc.count("\n") + 1
                body_lines = body[-1].end_lineno - body[0].lineno + 1
                if doc_lines >= 15 and doc_lines >= 3 * body_lines:
                    long_docs += 1
    print(f"Function docstrings longer than 10 lines: {docs['over_10']}/{docs['all']}")
    print(f"Functions whose docstring is >= 15 lines and >= 3x the body: {long_docs}")


if __name__ == "__main__":
    main()
