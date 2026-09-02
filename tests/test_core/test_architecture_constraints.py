"""Machine checks for docs/agents/architecture-constraints.md.

Each test corresponds to one numbered constraint (A1-A7, S1-S4) or exception
registry (R1-R5) in that document. When a test fails, the fix is either to
change the code back, or to update the document AND get the exception
registered -- never to loosen the test quietly.

What these tests can and cannot see, stated plainly so the document does not
overclaim:

* Registries compare **Counters** of (directory, name), so adding a second site
  with an already-registered name is caught. They do not pin line numbers, so
  ordinary edits above a registered site do not break them.
* ``_literal_names`` returns None for a dynamically-built inputs/outputs/writes
  argument, so this file's A5/A6 tests skip those nodes.
  ``test_static_coverage_floor`` pins how many, so *this* scan cannot silently
  get weaker. It is no longer the only line: since #157 ``Node.__init__``
  raises on both, and the constructor sees the evaluated names -- including
  the dynamic ones. What the AST scan still buys is a verdict without
  constructing anything, reported at a file and line.
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
TESTS = Path(__file__).resolve().parents[1]
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
    """This file's A5/A6 tests skip dynamically-built nodes. Pin how many.

    ``Node.__init__`` covers those four (#157), so this floor guards the
    static scan's own reach rather than the constraints' total coverage.
    """
    total = sum(1 for _ in _node_calls())
    judgeable = sum(1 for _ in _judgeable_nodes())
    assert (total, judgeable) == (58, 54), (
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

        assert found == Counter({
            ("training", "training_eval_predictions"): 1,
            ("inference", "unranked_predictions"): 1,
        }), (
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

        # persist_sample_weight_report left this set when the catalog took
        # over its write (conf/base/catalog.yaml::sample_weight_report) --
        # shrinking a registry, which is the direction that needs no new
        # exception. Putting the write back in the node puts it back here.
        #
        # The five cache nodes replaced _materialize_parquet_handle, the helper
        # that used to hold their shutil.rmtree call along with all four of
        # their cache decisions (ADR-0014 decision 1, approved 2026-08-30).
        # Five entries where there was one is the honest count: each of them
        # really does delete a directory. The deletes stayed in nodes.py rather
        # than moving to steps/local_cache.py precisely so this scan -- which
        # reads pipelines/**/nodes*.py and nothing else -- keeps seeing them.
        assert set(found) == {
            ("training", "log_experiment"),
            ("training", "cache_train_model_input"),
            ("training", "cache_train_dev_model_input"),
            ("training", "cache_val_model_input"),
            ("training", "cache_test_model_input"),
            ("training", "cache_calibration_model_input"),
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
    """A5, checked statically. ``Node.__init__`` raises on it too (#157).

    Kept as a second line: it reads the source rather than running it, so it
    names the offending file and line even for a node no test constructs.
    """

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
    """A6: same name on both sides makes the load/execute/save order ambiguous.

    Also raised by ``Node.__init__`` (#157); see A5 above for why both stay.
    """

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


def _module_paths_imported(path, root):
    """Every module path a file imports, spelled absolutely.

    Two things this does that a plain read of ``node.module`` does not:

    * **Relative imports are resolved** against the file's own package, so
      ``from ..pipelines.training import steps`` written from elsewhere in
      ``src/`` is not invisible.
    * **Both halves of a ``from X import Y`` are yielded** (``X`` and ``X.Y``),
      because a subpackage hides on either side depending on spelling:
      ``from ...training.steps import hpo_resume`` names it in ``node.module``,
      ``from ...training import steps`` names it in ``node.names``.
    """
    package = ("recsys_tfb",) + path.relative_to(root).parts[:-1]
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = list(package[: len(package) - node.level + 1])
            else:
                base = []
            prefix = base + (node.module.split(".") if node.module else [])
            if not prefix:
                continue
            yield ".".join(prefix)
            for alias in node.names:
                yield ".".join(prefix + [alias.name])


def _steps_imports_from_outside(root):
    """``file: module`` for every import of a pipeline's ``steps/`` made from
    outside that pipeline. Empty means every ``steps/`` module is internal."""
    offenders = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        for module in _module_paths_imported(path, root):
            parts = module.split(".")
            if parts[:2] != ["recsys_tfb", "pipelines"] or parts[3:4] != ["steps"]:
                continue
            if rel.parts[:2] != ("pipelines", parts[2]):
                offenders.append(f"{rel}: {module}")
    return sorted(set(offenders))


class TestS3StepsPackagesStayInternal:
    """S3: nothing in ``src/`` outside ``pipelines/<name>/`` may import that
    pipeline's ``steps/``.

    This is the mechanical half of rule 8 in ``pipeline-node-design.md``: root
    level vs ``steps/`` is decided by whether every **src-side** caller sits
    inside the pipeline. The rule's whole payoff is that one directory listing
    separates a pipeline's outward contract from its internals, and that payoff
    is gone the moment an outside module reaches past the root into ``steps/`` --
    at which point the listing lies, and it lies silently.

    Scope and blind spot, stated so the document does not overclaim:

    * All three pipelines, not just training. ``cache_sources.py`` was moved out
      of ``nodes.py`` to the training root under this same rule (issue #234), and
      pinning only training would leave the two pipelines that got there first
      (``dataset`` since #176, ``inference`` since #197) unguarded.
    * **Tests are not scanned, on purpose.** Rule 8 says test imports move
      nothing, because what the criterion protects is the production caller
      graph. ``tests/test_pipelines/test_training/test_hpo_resume.py`` importing
      ``steps.hpo_resume`` directly is correct and must stay legal.
    * It sees imports, not attribute access: a module that imports the pipeline
      package and then reads ``training.steps.hpo_resume`` off it walks past this
      scan. Nothing in the tree does that today, and the import is the form worth
      pinning because it is the one that gets written by accident.
    """

    def test_no_src_module_outside_a_pipeline_imports_its_steps(self):
        offenders = _steps_imports_from_outside(SRC)
        assert offenders == [], (
            "a module outside the pipeline imported its steps/ package (S3): "
            f"{offenders}. Either the step belongs at the pipeline root as an "
            "outward contract (as cache_sources.py does), or the caller should "
            "not be reaching into another pipeline's internals; see "
            "docs/agents/architecture-constraints.md."
        )

    def test_the_scan_sees_every_spelling_of_the_import(self, tmp_path):
        """Without this, the check above is decoration.

        Each spelling hides the subpackage somewhere different, and each of the
        resolver's three parts is load-bearing for a different one:

        ======================  =========================  ====================
        spelling                where ``steps`` hides      needs
        ======================  =========================  ====================
        ``abs_module.py``       ``node.module``            --
        ``abs_package.py``      ``node.module``            --
        ``plain_import.py``     ``alias.name``             ``ast.Import`` arm
        ``abs_names.py``        ``alias.name``             ``node.names`` half
        ``rel_package.py``      ``alias.name``, relative   both of the above
        ======================  =========================  ====================

        So a scan that reads only ``node.module`` of ``ImportFrom`` passes on
        three of the five, and each of the last three fails for its own reason
        -- which is why ``abs_names.py`` is here even though ``rel_package.py``
        would already catch a resolver missing both parts at once.

        The first three are **new with this structure**: until ``hpo_resume``
        moved into ``steps/`` (issue #234) it was at the package root, so no
        spelling could name it under ``steps``. The last two name the package
        rather than the module and were always writable -- ``steps/`` already
        held ``hpo_scoring`` and ``local_cache``.
        """
        inside = tmp_path / "pipelines" / "training"
        inside.mkdir(parents=True)
        (inside / "nodes.py").write_text(
            "from recsys_tfb.pipelines.training.steps import hpo_resume\n"
        )
        cases = {
            "abs_module.py":
                "from recsys_tfb.pipelines.training.steps.hpo_resume import "
                "open_study\n",
            "abs_package.py":
                "from recsys_tfb.pipelines.training.steps import hpo_resume\n",
            "plain_import.py":
                "import recsys_tfb.pipelines.training.steps.hpo_resume\n",
            "abs_names.py":
                "from recsys_tfb.pipelines.training import steps\n",
            "rel_package.py":
                "from .pipelines.training import steps\n",
        }
        for name, source in cases.items():
            (tmp_path / name).write_text(source)

        found = _steps_imports_from_outside(tmp_path)
        for name in cases:
            assert any(line.startswith(f"{name}: ") for line in found), (
                f"{name}: this spelling escaped the scan"
            )
        assert not any(line.startswith("pipelines/") for line in found), (
            f"the pipeline's own module was reported as an outsider: {found}"
        )

    def test_steps_packages_re_export_nothing(self):
        """``steps/__init__.py`` holds no imports and no ``__all__``.

        The import line in ``nodes.py`` is what says which concern a step came
        from, and a re-export erases that: ``from .steps import build_trial_params``
        would compile fine and tell the reader nothing. Docstring-only is the
        shape all three packages have; this keeps it that way.
        """
        offenders = {}
        for init in sorted(PIPELINES.glob("*/steps/__init__.py")):
            tree = ast.parse(init.read_text())
            names = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names += [f"import {a.name}" for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    where = "." * node.level + (node.module or "")
                    names += [f"from {where} import {a.name}" for a in node.names]
                elif isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "__all__"
                    for t in node.targets
                ):
                    names.append("__all__")
            if names:
                offenders[str(init.relative_to(SRC))] = sorted(names)
        assert offenders == {}, (
            f"steps/__init__.py re-exports something (S3): {offenders}. "
            "Node modules import each step module by name so the import line "
            "names the concern."
        )


#: Trees S4 scans, ``label -> root``. The label prefixes every reported
#: path, so an offender reads as a repo-relative location and the exception
#: registry can key on one unambiguous string.
#:
#: ``tests`` is in here and ``scripts`` is not, and neither is arbitrary.
#: A test that takes the first entity column stops testing what its own
#: docstring says it tests -- a false green, which is the failure this file
#: exists to prevent. (Note this is the opposite call from S3, which
#: deliberately does not scan tests; S3 guards where a module *lives*, and
#: a test import moves nothing.) ``scripts/`` holds hand-run demo tools
#: that no production path imports -- see R5.
ENTITY_SCAN_ROOTS = {"src/recsys_tfb": SRC, "tests": TESTS}

ENTITY_KEY = "entity"

# Calls whose return value is a list of entity columns, so indexing the result
# is the same mistake as indexing ``schema["entity"]`` itself.
# ``get_entity_grouping`` (core/schema.py) returns the declared split/sample
# unit and falls back to the whole entity, so it is exactly as much a list as
# the schema key it defaults to.
ENTITY_LIST_CALLS = {"get_entity_grouping"}

# (repo-relative module path, enclosing function) pairs allowed to take the
# first entity column, e.g. ("src/recsys_tfb/pipelines/dataset/nodes.py",
# "split_train_keys"). Empty, and it stays empty unless the user signs one
# off -- R5 in docs/agents/architecture-constraints.md.
ENTITY_FIRST_COLUMN_EXCEPTIONS = frozenset()


def _innermost_function_by_line(tree):
    """``{lineno: enclosing function name}``, innermost def winning.

    Widest-first so the narrowest span is written last: a nested helper's line
    must report the helper, not the function it happens to sit inside, or the
    message sends the reader to the wrong place.
    """
    owner = {}
    funcs = [
        n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    for fn in sorted(funcs, key=lambda f: f.end_lineno - f.lineno, reverse=True):
        for line in range(fn.lineno, fn.end_lineno + 1):
            owner[line] = fn.name
    return owner


def _entity_list_source(node):
    """A short label if ``node`` evaluates to the entity columns, else None.

    What is on the left of ``["entity"]`` is deliberately not inspected:
    ``schema["entity"]``, ``get_schema(parameters)["entity"]`` and
    ``params["schema"]["columns"]["entity"]`` are one expression as far as S4
    is concerned, and pinning any particular spelling would just move the
    blind spot around.
    """
    if isinstance(node, ast.Subscript):
        key = node.slice
        if isinstance(key, ast.Constant) and key.value == ENTITY_KEY:
            return f'...["{ENTITY_KEY}"]'
        return None
    if isinstance(node, ast.Call):
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        return f"{name}(...)" if name in ENTITY_LIST_CALLS else None
    return None


def _first_element_spelling(index):
    """``"[0]"`` / ``"[:1]"`` if this subscript takes element 0, else None.

    ``[:1]`` is here because it is the same read spelled as a list -- the one
    near-miss that would otherwise pass while meaning "the first column".
    ``type(...) is int`` keeps ``x[False]`` from counting: ``False == 0``.
    """
    if isinstance(index, ast.Constant) and type(index.value) is int and index.value == 0:
        return "[0]"
    if isinstance(index, ast.Slice) and index.step is None:
        starts_at_zero = index.lower is None or (
            isinstance(index.lower, ast.Constant) and index.lower.value == 0
        )
        stops_at_one = (
            isinstance(index.upper, ast.Constant) and index.upper.value == 1
        )
        if starts_at_zero and stops_at_one:
            return "[:1]"
    return None


def _bindings(targets, value):
    """``(Name target, bound expression)`` pairs for one assignment.

    Tuple targets are paired positionally, so
    ``entity_cols, item_col = schema["entity"], schema["item"]`` binds only the
    left name. That form is not hypothetical here: the line directly above the
    known ``scripts/shap_margin_summary.py`` offender is
    ``time_col, item_col, label_col = schema["time"], schema["item"], ...``, so
    it is demonstrably how this repo unpacks a schema. A starred target
    (``first, *rest = ...``) is skipped -- positions stop lining up, and that
    spelling is listed as a blind spot rather than guessed at.
    """
    pairs = []
    for target in targets:
        if isinstance(target, ast.Name):
            pairs.append((target, value))
        elif isinstance(target, (ast.Tuple, ast.List)) and isinstance(
            value, (ast.Tuple, ast.List)
        ):
            if len(target.elts) != len(value.elts):
                continue
            if any(isinstance(t, ast.Starred) for t in target.elts):
                continue
            pairs += [
                (t, v) for t, v in zip(target.elts, value.elts)
                if isinstance(t, ast.Name)
            ]
    return pairs


def _entity_list_names(tree):
    """Names bound to the entity columns anywhere in ``tree``.

    Transitive (``b = a`` inherits) and flow-insensitive (a name is judged once
    per module, not per branch). Both are deliberate: the shape being caught is

        entity_cols = schema["entity"]
        ...
        cust_col = entity_cols[0]

    which is how two of the four original sites were written, and following it
    across an intervening rename costs one fixed-point loop. The cost of
    flow-insensitivity is a name rebound to something else later reading as
    entity columns -- a false positive, which is the direction that gets
    noticed rather than the direction that ships a wrong number.
    """
    names = set()
    while True:
        before = len(names)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                targets, value = [node.target], node.value
            else:
                continue
            for target, bound in _bindings(targets, value):
                if _entity_list_source(bound) is not None or (
                    isinstance(bound, ast.Name) and bound.id in names
                ):
                    names.add(target.id)
        if len(names) == before:
            return sorted(names)


def _entity_first_column_offenders(root, exceptions=None, label=""):
    """``path:line in func(): expr`` for every take-the-first-entity-column.

    ``root`` is scanned recursively; paths are reported relative to it, with
    ``label`` prefixed. The real run passes the labels in
    ``ENTITY_SCAN_ROOTS`` so an offender reads as a repo-relative path; the
    tmp-tree tests pass none and assert on bare filenames.
    """
    exceptions = ENTITY_FIRST_COLUMN_EXCEPTIONS if exceptions is None else exceptions
    offenders = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        aliases = _entity_list_names(tree)
        owner = _innermost_function_by_line(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Subscript):
                continue
            spelling = _first_element_spelling(node.slice)
            if spelling is None:
                continue
            source = _entity_list_source(node.value)
            if source is None:
                if not (isinstance(node.value, ast.Name) and node.value.id in aliases):
                    continue
                source = node.value.id
            rel = str(path.relative_to(root))
            rel = f"{label}/{rel}" if label else rel
            func = owner.get(node.lineno, "<module>")
            if (rel, func) in exceptions:
                continue
            where = func if func == "<module>" else f"{func}()"
            offenders.append(f"{rel}:{node.lineno} in {where}: {source}{spelling}")
    return offenders


class TestS4NoFirstEntityColumn:
    """S4: no module may take the first column of ``schema.entity``.

    ``schema.entity`` is a list of columns that *together* identify the owner
    of one ranking request. Reading column 0 as "the identity" and the rest as
    trimmings is indistinguishable from the correct reading while the schema
    declares one column -- which is why four sites did it, all green, for as
    long as this repo only ever shipped single-column entities (issues #263,
    #264). This scan is what makes the fifth occurrence fail at test time
    instead of shipping a wrong number.

    It goes on the ``S`` prefix rather than becoming ``A8`` on purpose: the
    document's own rule is that new structural constraints take ``S`` so the
    collision surface with ``core/consistency.py``'s A-series stops growing.
    """

    def test_no_module_takes_the_first_entity_column(self):
        offenders = [
            line
            for label, root in ENTITY_SCAN_ROOTS.items()
            for line in _entity_first_column_offenders(root, label=label)
        ]
        assert offenders == [], (
            "a module took the first column of schema.entity (S4): "
            f"{offenders}. An entity is every column in schema.entity taken "
            "together; group on the whole list (see evaluation/metrics_spark.py"
            "::rank_within_query), or -- if the unit really is coarser -- let "
            "the user declare it (core/schema.py::get_entity_grouping). "
            "Exceptions are registered in R5 of "
            "docs/agents/architecture-constraints.md and need sign-off."
        )

    def test_the_scan_roots_are_real_and_pinned(self):
        """Both trees are clean, so dropping one leaves the scan above green.

        That is the same false green this class was written to stop: a fence
        that scans nothing reports no violations. Pin the roots themselves, and
        pin that each one really holds modules -- a root pointed at a path that
        does not exist would also scan nothing, silently.
        """
        assert set(ENTITY_SCAN_ROOTS) == {"src/recsys_tfb", "tests"}, (
            "S4's scan roots changed. tests/ is in scope on purpose: a test "
            "that takes the first entity column stops testing what its "
            "docstring claims. Removing a root needs the same sign-off as "
            "registering an exception (R5)."
        )
        for label, root in ENTITY_SCAN_ROOTS.items():
            found = sum(1 for _ in root.rglob("*.py"))
            assert found > 50, f"{label} -> {root} holds {found} .py files"

    def test_the_scan_sees_every_spelling(self, tmp_path):
        """Without this, the check above is decoration.

        Each case hides the first column somewhere different, and each one is
        writable today:

        ====================  ============================================
        case                  what it needs from the scan
        ====================  ============================================
        ``direct.py``         subscript-of-subscript, no variable between
        ``alias.py``          the binding pass (both original dataset sites)
        ``alias_chain.py``    the binding pass reaching a **fixed point**
        ``annotated.py``      ``AnnAssign``, not just ``Assign``
        ``tuple_target.py``   tuple targets paired positionally
        ``call.py``           ``get_entity_grouping`` returns entity columns
        ``sliced.py``         ``[:1]`` takes the first column too
        ``nested.py``         reported against the innermost def
        ``pkg/sub/deep.py``   ``rglob``, not ``glob``
        ====================  ============================================

        ``alias_chain.py`` writes the consumer **above** the producer on
        purpose. ``ast.walk`` visits a module's statements in source order, so
        the readable ordering (``a = ...`` then ``b = a``) is satisfied by a
        single pass and pins nothing; this ordering binds ``a`` in pass one and
        ``b`` only in pass two. Verified: cutting the loop to one pass turns
        this case red and nothing else.

        ``pkg/sub/deep.py`` is the only case not written flat into
        ``tmp_path``. Verified: ``rglob`` -> ``glob`` leaves every other case
        green while the real tree stops being scanned below its top level --
        which would silently exempt all four historical sites.
        """
        cases = {
            "direct.py": 'def f(schema):\n    return schema["entity"][0]\n',
            "alias.py": (
                'def f(schema):\n'
                '    entity_cols = schema["entity"]\n'
                '    return entity_cols[0]\n'
            ),
            "alias_chain.py": (
                'def f():\n'
                '    return b[0]\n'
                'b = a\n'
                'a = SCHEMA["entity"]\n'
            ),
            "annotated.py": (
                'def f(schema):\n'
                '    cols: list[str] = schema["entity"]\n'
                '    return cols[0]\n'
            ),
            "tuple_target.py": (
                'def f(schema):\n'
                '    entity_cols, item_col = schema["entity"], schema["item"]\n'
                '    return entity_cols[0]\n'
            ),
            "call.py": (
                'def f(parameters):\n'
                '    cols = get_entity_grouping(parameters, "train_split_keys")\n'
                '    return cols[0]\n'
            ),
            "sliced.py": 'def f(schema):\n    return schema["entity"][:1]\n',
            "nested.py": (
                'def outer(schema):\n'
                '    def inner():\n'
                '        return schema["entity"][0]\n'
                '    return inner\n'
            ),
            "pkg/sub/deep.py": 'def f(schema):\n    return schema["entity"][0]\n',
        }
        for name, source in cases.items():
            target = tmp_path / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source)

        found = _entity_first_column_offenders(tmp_path, exceptions=frozenset())
        for name in cases:
            assert any(line.startswith(f"{name}:") for line in found), (
                f"{name}: this spelling escaped the scan -- {found}"
            )
        assert any(line.startswith("nested.py:3 in inner():") for line in found), (
            f"the offender was not reported against the innermost def: {found}"
        )

    def test_the_report_names_a_location_not_just_a_count(self, tmp_path):
        """Red-but-mute is a half-fix: the next person still has to go find it.

        The whole rendered line is pinned by equality, so dropping any part of
        it -- the function, the expression, the line number -- fails here.
        Verified: shrinking the message to ``f"{rel}:{node.lineno}"`` turns
        this red. An offender outside any ``def`` is included because it is the
        one case where the function slot is not a function name.
        """
        (tmp_path / "comparison_nodes.py").write_text(
            'def restrict_to_common(schema):\n'
            '    entity_cols = schema["entity"]\n'
            '    cust_col = entity_cols[0]\n'
            '    return cust_col\n'
        )
        (tmp_path / "at_module_level.py").write_text('COL = SCHEMA["entity"][0]\n')

        assert _entity_first_column_offenders(tmp_path, exceptions=frozenset()) == [
            'at_module_level.py:1 in <module>: ...["entity"][0]',
            "comparison_nodes.py:3 in restrict_to_common(): entity_cols[0]",
        ]
        # ...and with the label the real run passes, so an offender reads as a
        # repo-relative path. Two roots are scanned; a bare "pipelines/..."
        # would not say which tree it came from, and the exception registry
        # keys on this exact string.
        assert _entity_first_column_offenders(
            tmp_path, exceptions=frozenset(), label="tests",
        ) == [
            'tests/at_module_level.py:1 in <module>: ...["entity"][0]',
            "tests/comparison_nodes.py:3 in restrict_to_common(): entity_cols[0]",
        ]

    def test_the_scan_leaves_honest_entity_use_alone(self, tmp_path):
        """False positives here cost real work, so pin the shapes that are fine.

        Every line below appears in ``src`` today (or is one edit away) and
        must stay legal: spreading the whole list, indexing something that is
        not the entity list, and reading a *row's* first field.
        """
        (tmp_path / "clean.py").write_text(
            'def f(schema, df, keys):\n'
            '    group_cols = [schema["time"]] + schema["entity"]\n'
            '    entity_cols = schema["entity"]\n'
            '    both = [schema["time"], *entity_cols]\n'
            '    item = schema["item"][0]\n'
            '    rows = {r[0] for r in df.select(schema["item"]).collect()}\n'
            '    return group_cols, both, item, rows, keys[0]\n'
        )
        found = _entity_first_column_offenders(tmp_path, exceptions=frozenset())
        assert found == [], f"honest entity use was flagged: {found}"

    def test_a_registered_exception_silences_exactly_one_site(self, tmp_path):
        """The registry is empty, so nothing else proves it is wired up.

        An empty registry that silently ignores its entries would look exactly
        like today's green. Two sites, one registered: one must survive.
        """
        (tmp_path / "two.py").write_text(
            'def allowed(schema):\n'
            '    return schema["entity"][0]\n'
            'def forbidden(schema):\n'
            '    return schema["entity"][0]\n'
        )
        found = _entity_first_column_offenders(
            tmp_path,
            exceptions=frozenset({("src/recsys_tfb/two.py", "allowed")}),
            label="src/recsys_tfb",
        )
        assert len(found) == 1 and "forbidden" in found[0], (
            f"the exception registry did not filter exactly one site: {found}"
        )

    def test_the_registry_is_empty(self):
        """S4 ships with zero exceptions; growing it needs the user's sign-off."""
        assert ENTITY_FIRST_COLUMN_EXCEPTIONS == frozenset(), (
            "S4 gained an exception. It must be registered in R5 of "
            "docs/agents/architecture-constraints.md with sign-off first."
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
