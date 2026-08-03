"""Machine checks for docs/agents/architecture-constraints.md.

Each test here corresponds to one numbered constraint (A1-A7) or exception
registry (R1-R3) in that document. When a test fails, the fix is either to
change the code back, or to update the document AND get the exception
registered — never to loosen the test quietly.

Registries assert on *names*, not line numbers, so ordinary edits above a
registered site do not break them; adding or removing a site does.
"""

import ast
import re
from pathlib import Path

import recsys_tfb

SRC = Path(recsys_tfb.__file__).parent
PIPELINES = SRC / "pipelines"


def _node_calls():
    """Yield (path, ast.Call) for every ``Node(...)`` construction."""
    for path in sorted(PIPELINES.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for call in ast.walk(tree):
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name == "Node":
                yield path, call


def _literal_names(arg):
    """String literals in a Node inputs=/outputs= argument, or None if dynamic."""
    if arg is None:
        return []
    if isinstance(arg, ast.Constant):
        return [arg.value] if isinstance(arg.value, str) else None
    if isinstance(arg, (ast.List, ast.Tuple)):
        names = []
        for elt in arg.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                names.append(elt.value)
            else:
                return None  # dynamic element -> cannot judge this node
        return names
    return None


def _kwargs(call):
    return {kw.arg: kw.value for kw in call.keywords}


def _globals_by_file(directory):
    """{filename: {declared global names}} for one source directory."""
    found = {}
    for path in sorted(directory.rglob("*.py")):
        tree = ast.parse(path.read_text())
        names = set()
        for stmt in ast.walk(tree):
            if isinstance(stmt, ast.Global):
                names.update(stmt.names)
        if names:
            found[path.name] = names
    return found


class TestA1NodesDoNotDoIO:
    """A1: I/O belongs to the catalog; the only escape hatch is the @ prefix."""

    def test_at_prefix_usage_matches_registry(self):
        found = set()
        for path, call in _node_calls():
            for name in _literal_names(_kwargs(call).get("inputs")) or []:
                if name.startswith("@"):
                    found.add((path.parent.name, name))

        assert found == {("training", "@training_eval_predictions")}, (
            "@ handle usage changed. Registered in R1 of "
            "docs/agents/architecture-constraints.md; adding one needs sign-off."
        )


class TestA2NoMutableGlobalStateInNodes:
    """A2: pipeline nodes must not depend on mutable global state."""

    def test_no_global_declarations_in_pipelines(self):
        offenders = _globals_by_file(PIPELINES)
        assert offenders == {}, f"global state leaked into pipeline code: {offenders}"


class TestA3NoPrint:
    """A3: structured logging only -- print() bypasses RunContext."""

    PRINT_CALL = re.compile(r"(?<![\w.])print\s*\(")

    def test_no_print_calls_in_src(self):
        offenders = [
            f"{path.relative_to(SRC)}:{i}"
            for path in sorted(SRC.rglob("*.py"))
            for i, line in enumerate(path.read_text().splitlines(), 1)
            if self.PRINT_CALL.search(line)
        ]
        assert offenders == [], f"use logging.getLogger(__name__): {offenders}"


class TestA4NotebookIsolation:
    """A4: notebooks may import src/, never the reverse."""

    IMPORT_NOTEBOOKS = re.compile(r"^\s*(from|import)\s+notebooks\b", re.MULTILINE)

    def test_src_does_not_import_notebooks(self):
        offenders = [
            str(path.relative_to(SRC))
            for path in sorted(SRC.rglob("*.py"))
            if self.IMPORT_NOTEBOOKS.search(path.read_text())
        ]
        assert offenders == [], f"production code importing notebooks: {offenders}"


class TestA5NodeHasInputOrOutput:
    """A5: core/node.py does not validate this (F4); this test does."""

    def test_every_node_has_an_input_or_an_output(self):
        offenders = []
        for path, call in _node_calls():
            kw = _kwargs(call)
            ins = _literal_names(kw.get("inputs"))
            outs = _literal_names(kw.get("outputs"))
            if ins is None or outs is None:
                continue  # dynamic; cannot judge statically
            if not ins and not outs:
                offenders.append(f"{path.relative_to(SRC)}:{call.lineno}")
        assert offenders == [], f"nodes with neither input nor output: {offenders}"


class TestA6NoInputOutputNameCollision:
    """A6: same name on both sides makes the load/execute/save order ambiguous."""

    def test_no_node_reuses_an_input_name_as_output(self):
        offenders = []
        for path, call in _node_calls():
            kw = _kwargs(call)
            ins = _literal_names(kw.get("inputs"))
            outs = _literal_names(kw.get("outputs"))
            if ins is None or outs is None:
                continue
            stripped = {n[1:] if n.startswith("@") else n for n in ins}
            overlap = stripped & set(outs)
            if overlap:
                offenders.append(f"{path.relative_to(SRC)}:{call.lineno} {sorted(overlap)}")
        assert offenders == [], f"input name reused as output: {offenders}"


class TestA7ZeroOutputNodesRegistered:
    """A7: slicing silently skips these (F5), so each one needs a decision."""

    def test_zero_output_nodes_match_registry(self):
        found = set()
        for path, call in _node_calls():
            kw = _kwargs(call)
            outs = kw.get("outputs")
            is_none = isinstance(outs, ast.Constant) and outs.value is None
            if "outputs" not in kw or is_none:
                func = call.args[0] if call.args else kw.get("func")
                fname = func.id if isinstance(func, ast.Name) else "<dynamic>"
                found.add((path.parent.name, fname))

        assert found == {
            ("dataset", "validate_data_consistency"),
            ("training", "log_experiment"),
        }, (
            "zero-output side-effect nodes changed. Registered in R3 of "
            "docs/agents/architecture-constraints.md; slicing skips these."
        )


class TestR2FrameworkGlobalsRegistry:
    """R2: the framework layer may hold process-level singletons; nodes may not."""

    def test_framework_globals_match_registry(self):
        found = {**_globals_by_file(SRC / "core"), **_globals_by_file(SRC / "utils")}
        assert found == {
            "logging.py": {"_current_context"},
            "spark.py": {
                "_canonical_configs",
                "_canonical_enable_hive",
                "_last_app_id",
                "_last_alive_ts",
            },
        }, f"framework global state changed: {found}"
