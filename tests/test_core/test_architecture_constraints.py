"""Machine checks for docs/agents/architecture-constraints.md.

Each test corresponds to one numbered constraint (A1-A7, S1-S2) or exception
registry (R1-R4) in that document. When a test fails, the fix is either to
change the code back, or to update the document AND get the exception
registered -- never to loosen the test quietly.

What these tests can and cannot see, stated plainly so the document does not
overclaim:

* Registries compare **Counters** of (directory, name), so adding a second site
  with an already-registered name is caught. They do not pin line numbers, so
  ordinary edits above a registered site do not break them.
* ``_literal_names`` returns None for a dynamically-built inputs/outputs/writes
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
        writes = _literal_names(kw.get("writes"))
        if ins is None or outs is None or writes is None:
            continue
        yield path, call, ins, outs, writes


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

    def test_writes_declarations_match_registry(self):
        found = Counter()
        for path, call in _node_calls():
            for name in _literal_names(_kwargs(call).get("writes")) or []:
                found[(path.parent.name, name)] += 1

        assert found == Counter({("training", "training_eval_predictions"): 1}), (
            "Node(writes=...) usage changed. Registered in R1 of "
            "docs/agents/architecture-constraints.md; adding one needs sign-off."
        )

    def test_no_node_still_spells_a_write_target_with_an_at_prefix(self):
        """The ``@`` sigil was replaced by ``writes=`` (issue #186).

        A leftover ``"@x"`` in ``inputs`` is no longer a handle -- the Runner
        reads it as an ordinary dataset name and the pipeline fails at
        validation. Catch it here, where the message names the file, rather
        than at run time.
        """
        offenders = [
            f"{path.relative_to(SRC)}:{call.lineno} {name}"
            for path, call in _node_calls()
            for name in _literal_names(_kwargs(call).get("inputs")) or []
            if name.startswith("@")
        ]
        assert offenders == [], (
            f"'@' handle sigil is gone; use Node(writes=[...]): {offenders}"
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

    def test_every_node_has_an_input_an_output_or_a_write_target(self):
        offenders = [
            f"{path.relative_to(SRC)}:{call.lineno}"
            for path, call, ins, outs, writes in _judgeable_nodes()
            if not ins and not outs and not writes
        ]
        assert offenders == [], (
            f"nodes with no input, no output and no write target: {offenders}"
        )


class TestA6NoInputOutputNameCollision:
    """A6: same name on both sides makes the load/execute/save order ambiguous."""

    def test_no_node_reuses_an_input_or_write_name_as_output(self):
        offenders = []
        for path, call, ins, outs, writes in _judgeable_nodes():
            overlap = (set(ins) | set(writes)) & set(outs)
            if overlap:
                offenders.append(
                    f"{path.relative_to(SRC)}:{call.lineno} {sorted(overlap)}")
        assert offenders == [], f"input/write name reused as output: {offenders}"


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


DATASET = PIPELINES / "dataset"


def _function_def_names(path):
    """Names ``def``-ed at any depth in ``path`` -- imports deliberately excluded."""
    tree = ast.parse(path.read_text())
    return {
        n.name
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _imported_roots(path):
    """{root package: [lineno]} for every import in ``path``, nested ones included.

    ``ast.walk`` rather than ``tree.body``: a deferred ``import pyspark`` inside
    a function body is the exact form S2 has to catch, and it is the form that
    would otherwise look like "this module has no Spark dependency".

    A relative import is recorded under a leading-dot key (``".scoping"``) --
    skipping it would make the one import form this scan cannot resolve also the
    one it never mentions.
    """
    tree = ast.parse(path.read_text())
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots = [a.name.split(".")[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            roots = ["." * node.level + module.split(".")[0] if node.level
                     else module.split(".")[0]] if module or node.level else []
        else:
            continue
        for root in roots:
            found.setdefault(root, []).append(node.lineno)
    return found


def _dataset_module_targets(path, package_root):
    """Files inside the dataset package that ``path`` imports.

    Each import form is resolved against the base it was written against:
    a relative name from the importer's own directory (one level up per extra
    dot), an absolute-within-package name from ``package_root``.

    Three things a first-segment-only scan gets wrong once a subpackage exists.
    All three fail the same way -- a path that does not exist reads as "reaches
    nothing", so S2 passes on a module that does reach Spark (ADR-0008 §4):

    * **every dotted segment is kept**: ``...dataset.steps.scoping`` is
      ``steps/scoping.py``, not ``steps.py``;
    * **the imported names are candidate modules too**: ``from .steps import
      scoping`` puts the module in ``node.names``, not in ``node.module``;
    * **the package itself is a valid module value**: ``from
      recsys_tfb.pipelines.dataset import month_plans`` has no trailing dot to
      match on.

    Both candidates are emitted for every ``from X import y`` because only the
    filesystem can say whether ``y`` is a module inside package ``X`` or a name
    inside module ``X``. Candidates that do not exist are skipped by the caller,
    so over-emitting costs nothing and under-emitting is a false green.
    """
    prefix = "recsys_tfb.pipelines.dataset."
    pairs = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            pairs += [(package_root, a.name[len(prefix):]) for a in node.names
                      if a.name.startswith(prefix)]
            continue
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            base, inner = path.parents[node.level - 1], node.module or ""
        elif node.module == prefix.rstrip("."):
            base, inner = package_root, ""
        elif node.module and node.module.startswith(prefix):
            base, inner = package_root, node.module[len(prefix):]
        else:
            continue
        if inner:
            pairs.append((base, inner))
        pairs += [(base, f"{inner}.{a.name}" if inner else a.name)
                  for a in node.names]
    return [base.joinpath(*name.split(".")).with_suffix(".py")
            for base, name in pairs]


def _spark_reachable_from(path, package_root=None, _seen=None):
    """First chain of dataset-package imports from ``path`` that reaches pyspark.

    Returns e.g. ``["month_plans.py", "scoping.py"]``, or None. Only modules
    under ``pipelines/dataset/`` are followed -- the point is the purity of one
    module, not a whole-tree dependency audit.

    This hop is what makes S2 mean anything. A direct-import scan reads
    ``from recsys_tfb.pipelines.dataset.steps.scoping import months_filter_as_date``
    as an import of ``recsys_tfb`` and says nothing, while the module it just
    pulled in is the Spark-typed one -- and absolute-within-package is the import
    form this repo actually writes.

    ``package_root`` is where an absolute-within-package name resolves from; it
    defaults to the real dataset package and is overridden only by the tmp-tree
    test that pins the subpackage hop.
    """
    package_root = DATASET if package_root is None else package_root
    _seen = _seen if _seen is not None else set()
    if path in _seen or not path.exists():
        return None
    _seen.add(path)

    for root in _imported_roots(path):
        if root.lstrip(".") == "pyspark":
            return [path.name]

    for target in _dataset_module_targets(path, package_root):
        chain = _spark_reachable_from(target, package_root, _seen)
        if chain:
            return [path.name] + chain
    return None


class TestS1DatasetNodesAreDefinedInNodesModule:
    """S1: every dataset node is ``def``-ed in ``pipelines/dataset/nodes.py``.

    Defined-here, not imported-here, and that difference is the whole point:
    ``nodes.py`` gaining one ``from .steps.sampling import some_step`` line would
    satisfy "the pipeline imports it from nodes.py" while the function body
    still lived elsewhere -- which is the shape ADR-0008 exists to remove.
    """

    def test_every_registered_node_is_defined_in_nodes_py(self):
        defined = _function_def_names(DATASET / "nodes.py")
        registered = [
            (call.lineno, _node_func_name(call))
            for path, call in _node_calls()
            if path == DATASET / "pipeline.py"
        ]
        assert registered, "no Node(...) found in pipelines/dataset/pipeline.py"
        offenders = [
            f"pipelines/dataset/pipeline.py:{lineno} {name}"
            for lineno, name in registered
            if name not in defined
        ]
        assert offenders == [], (
            "dataset nodes not defined in pipelines/dataset/nodes.py "
            f"(S1): {offenders}"
        )


class TestS2MonthPlansStaysPure:
    """S2: ``pipelines/dataset/month_plans.py`` must not import pyspark.

    Load-bearing, not stylistic: it is the only reason ``steps/scoping.py`` is a
    separate file, and the condition under which that module's tests run without
    paying this repo's 2-4 minute Spark cold start (ADR-0008 section 4).

    Two checks, because neither alone covers the property. The direct scan sees a
    **deferred** ``import pyspark`` inside a function body. The reachability
    check sees a **transitive** one: importing a Spark-typed sibling makes
    month_plans Spark-typed too, and that import names ``recsys_tfb``, not
    ``pyspark``, so the direct scan reads it as innocent.

    Deliberately not asserted: that ``pyspark`` stays out of ``sys.modules``.
    That would be the property one wants, and it is unreachable for reasons that
    have nothing to do with this module -- ``pipelines/__init__.py`` -> ``core``
    -> ``io`` -> ``models`` -> ``mlflow`` ends at ``mlflow/types/schema.py``'s own
    ``import pyspark``. What S2 buys is a **structural** boundary: month_plans
    stays free of Spark *types*, so its tests never need a SparkSession -- and
    the session, not the import, is the 2-4 minutes.
    """

    def test_month_plans_does_not_import_pyspark(self):
        found = _imported_roots(DATASET / "month_plans.py")
        assert "pyspark" not in found, (
            "month_plans.py imported pyspark at line(s) "
            f"{found.get('pyspark')} (S2). Spark-typed work belongs in "
            "steps/scoping.py; see docs/agents/architecture-constraints.md."
        )

    def test_month_plans_reaches_no_spark_typed_sibling(self):
        chain = _spark_reachable_from(DATASET / "month_plans.py")
        assert chain is None, (
            f"month_plans.py reaches pyspark via {' -> '.join(chain or [])} (S2). "
            "It imported a Spark-typed sibling, so its purity is gone even "
            "though no pyspark import appears in the file itself."
        )

    def test_reachability_crosses_into_a_subpackage(self, tmp_path):
        """The hop must enter ``steps/``, not look for a ``steps.py``.

        Without this, the check above is decoration: resolving only the first
        dotted segment points ``...dataset.steps.scoping`` at ``dataset/steps.py``,
        a file that does not exist, so the recursion reports "nothing found" and
        the assertion passes on a module that *does* reach Spark. The failure
        direction is exactly the one S2 exists to catch, and no other test in this
        file would go red for it (ADR-0008 section 4).

        Four import forms are pinned, because they fail for two different
        reasons. ``...steps.scoping`` hides the module in a dotted
        ``node.module``; ``from .steps import scoping`` hides it in
        ``node.names`` instead, and that form only becomes writable once a
        subpackage exists -- so this move is what created it.
        """
        (tmp_path / "steps").mkdir()
        (tmp_path / "steps" / "scoping.py").write_text(
            "from pyspark.sql import functions as F\n"
        )
        cases = {
            "abs_module.py":
                "from recsys_tfb.pipelines.dataset.steps.scoping import "
                "months_filter_as_date\n",
            "rel_module.py":
                "from .steps.scoping import months_filter_as_date\n",
            "abs_package.py":
                "from recsys_tfb.pipelines.dataset.steps import scoping\n",
            "rel_package.py":
                "from .steps import scoping\n",
        }
        for name, source in cases.items():
            (tmp_path / name).write_text(source)

        for name in cases:
            assert _spark_reachable_from(
                tmp_path / name, package_root=tmp_path,
            ) == [name, "scoping.py"], f"{name}: the hop did not enter steps/"


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
