"""Machine checks for docs/agents/architecture-constraints.md.

Each test corresponds to one numbered constraint (A1-A7) or exception registry
(R1-R4) in that document. When a test fails, the fix is either to change the
code back, or to update the document AND get the exception registered -- never
to loosen the test quietly.

What these tests can and cannot see, stated plainly so the document does not
overclaim:

* Registries compare **Counters** of (directory, name), so adding a second site
  with an already-registered name is caught. They do not pin line numbers, so
  ordinary edits above a registered site do not break them.
* ``_literal_names`` returns None for a dynamically-built inputs/outputs
  argument; those nodes are skipped by A5/A6. ``test_static_coverage_floor``
  pins how many nodes that is, so the blind spot cannot silently grow.
* The A1 I/O scan only sees **direct** calls (``open``, ``mkdir``, ...). A node
  that writes through a project helper is invisible to it -- ``tune_hyper-
  parameters`` is exactly that case and is registered in R4 by hand.
"""

import ast
import re
from collections import Counter
from pathlib import Path

import recsys_tfb

SRC = Path(__file__).resolve().parents[2] / "src" / "recsys_tfb"
PIPELINES = SRC / "pipelines"

# Direct filesystem / artifact writes the A1 scan can see.
WRITE_CALLS = {"open", "mkdir", "makedirs", "write_text", "write_bytes",
               "savefig", "to_parquet", "to_csv", "to_json", "rmtree"}
WRITE_ATTRS = {"log_artifact", "log_artifacts", "dump", "write_manifest"}


def test_audits_the_tree_it_was_shipped_with():
    """Guard against auditing a different worktree and reporting green.

    Running pytest from another repo root makes ``recsys_tfb`` resolve to that
    root's src (editable install / rootdir-relative pythonpath) while this file
    still lives here. Without this assertion every other test would scan the
    wrong tree and pass.
    """
    imported = Path(recsys_tfb.__file__).parent
    assert SRC == imported, (
        f"this test file lives under {SRC.parents[1]} but recsys_tfb imported "
        f"from {imported.parents[1]}. Run pytest from the repo root that owns "
        f"this file, or set PYTHONPATH to its src/."
    )


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
    """String literals in a Node inputs=/outputs= argument.

    Returns [] for an absent argument and for an explicit ``None`` (the way
    this repo spells a zero-output node) -- both mean "no names here", which is
    exactly what A5/A6 need to judge. Returns None only when the argument is
    built dynamically and genuinely cannot be read statically.
    """
    if arg is None:
        return []
    if isinstance(arg, ast.Constant):
        if arg.value is None:
            return []
        return [arg.value] if isinstance(arg.value, str) else None
    if isinstance(arg, (ast.List, ast.Tuple)):
        names = []
        for elt in arg.elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                names.append(elt.value)
            else:
                return None
        return names
    return None


def _kwargs(call):
    return {kw.arg: kw.value for kw in call.keywords}


def _node_func_name(call):
    func = call.args[0] if call.args else _kwargs(call).get("func")
    return func.id if isinstance(func, ast.Name) else "<dynamic>"


def _judgeable_nodes():
    for path, call in _node_calls():
        kw = _kwargs(call)
        ins = _literal_names(kw.get("inputs"))
        outs = _literal_names(kw.get("outputs"))
        if ins is None or outs is None:
            continue
        yield path, call, ins, outs


def _global_stmts(directory):
    """{filename: (statement count, frozenset of names)}."""
    found = {}
    for path in sorted(directory.rglob("*.py")):
        tree = ast.parse(path.read_text())
        stmts = [s for s in ast.walk(tree) if isinstance(s, ast.Global)]
        if stmts:
            names = frozenset(n for s in stmts for n in s.names)
            found[path.name] = (len(stmts), names)
    return found


def test_static_coverage_floor():
    """A5/A6 skip dynamically-built nodes. Pin how many, so it cannot grow."""
    total = sum(1 for _ in _node_calls())
    judgeable = sum(1 for _ in _judgeable_nodes())
    assert (total, judgeable) == (59, 55), (
        f"Node coverage changed: {judgeable}/{total} statically judgeable. "
        "If this dropped, A5/A6 now have a bigger blind spot -- check why."
    )


class TestA1NodeIO:
    """A1: data-flow artifacts go through the catalog; side artifacts are registered."""

    def test_at_handle_usage_matches_registry(self):
        found = Counter()
        for path, call in _node_calls():
            for name in _literal_names(_kwargs(call).get("inputs")) or []:
                if name.startswith("@"):
                    found[(path.parent.name, name)] += 1

        assert found == Counter({("training", "@training_eval_predictions"): 1}), (
            "@ handle usage changed. Registered in R1 of "
            "docs/agents/architecture-constraints.md; adding one needs sign-off."
        )

    def test_node_modules_do_not_touch_the_catalog(self):
        """AST, not text: ``DataCatalog`` appears in prose in these modules."""
        offenders = []
        for path in sorted(PIPELINES.rglob("nodes*.py")):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Name) and node.id == "DataCatalog":
                    offenders.append(f"{path.relative_to(SRC)}:{node.lineno} DataCatalog")
                elif (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in {"load", "save"}
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "catalog"
                ):
                    offenders.append(
                        f"{path.relative_to(SRC)}:{node.lineno} catalog.{node.func.attr}()")
        assert offenders == [], f"node modules reaching for the catalog: {offenders}"

    def test_direct_writes_match_registry(self):
        found = Counter()
        for path in sorted(PIPELINES.rglob("nodes*.py")):
            tree = ast.parse(path.read_text())
            owner = {}
            for fn in tree.body:
                if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    for ln in range(fn.lineno, fn.end_lineno + 1):
                        owner[ln] = fn.name
            for call in ast.walk(tree):
                if not isinstance(call, ast.Call):
                    continue
                f = call.func
                name = f.id if isinstance(f, ast.Name) else getattr(f, "attr", None)
                if name in WRITE_CALLS or name in WRITE_ATTRS:
                    found[(path.parent.name, owner.get(call.lineno, "<module>"))] += 1

        assert set(found) == {
            ("training", "persist_sample_weight_report"),
            ("training", "log_experiment"),
            ("training", "_materialize_parquet_handle"),
        }, (
            "a pipeline function gained direct filesystem I/O. Registered in R4 "
            "of docs/agents/architecture-constraints.md; adding one needs sign-off."
        )


class TestA2NoMutableGlobalStateInNodes:
    """A2: pipeline nodes must not depend on mutable global state."""

    def test_no_global_declarations_in_pipelines(self):
        offenders = _global_stmts(PIPELINES)
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
        offenders = [
            f"{path.relative_to(SRC)}:{call.lineno}"
            for path, call, ins, outs in _judgeable_nodes()
            if not ins and not outs
        ]
        assert offenders == [], f"nodes with neither input nor output: {offenders}"


class TestA6NoInputOutputNameCollision:
    """A6: same name on both sides makes the load/execute/save order ambiguous."""

    def test_no_node_reuses_an_input_name_as_output(self):
        offenders = []
        for path, call, ins, outs in _judgeable_nodes():
            stripped = {n[1:] if n.startswith("@") else n for n in ins}
            overlap = stripped & set(outs)
            if overlap:
                offenders.append(
                    f"{path.relative_to(SRC)}:{call.lineno} {sorted(overlap)}")
        assert offenders == [], f"input name reused as output: {offenders}"


class TestA7ZeroOutputNodesRegistered:
    """A7: slicing silently skips these (F5), so each one needs a decision."""

    def test_zero_output_nodes_match_registry(self):
        found = Counter()
        for path, call in _node_calls():
            kw = _kwargs(call)
            outs = kw.get("outputs")
            is_none = isinstance(outs, ast.Constant) and outs.value is None
            if "outputs" not in kw or is_none:
                found[(path.parent.name, _node_func_name(call))] += 1

        assert found == Counter({
            ("dataset", "validate_data_consistency"): 1,
            ("training", "log_experiment"): 1,
        }), (
            "zero-output side-effect nodes changed. Registered in R3 of "
            "docs/agents/architecture-constraints.md; slicing skips these."
        )


class TestR2FrameworkGlobalsRegistry:
    """R2: the framework layer may hold process-level singletons; nodes may not."""

    def test_framework_globals_match_registry(self):
        found = {**_global_stmts(SRC / "core"), **_global_stmts(SRC / "utils")}
        assert found == {
            "logging.py": (1, frozenset({"_current_context"})),
            "spark.py": (4, frozenset({
                "_canonical_configs",
                "_canonical_enable_hive",
                "_last_app_id",
                "_last_alive_ts",
            })),
        }, f"framework global state changed: {found}"
