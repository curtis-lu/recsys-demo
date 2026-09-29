import inspect
from collections.abc import Mapping


class Node:
    """Wraps a function with named inputs and outputs for pipeline execution.

    ``inputs`` is a list, bound to the function **by position**, or a
    ``{parameter name: dataset name}`` dict, bound **by name** — Kedro's dict
    inputs, same meaning (ADR-0030 decision 8). By name is what lets a node
    take a new input anywhere in its signature: by position a new optional
    input can only go last, and a slip there is swallowed by its trailing
    ``=None``. Either way ``self.inputs`` is the list of dataset names —
    topological sort, slicing and the Runner's catalog check all read that —
    and the dict is kept beside it in ``self.keyword_inputs`` (``None`` for a
    list).

    ``writes`` names the datasets this node saves to **itself**, rather than
    returning data for the Runner to save. Those datasets are handed to the
    function as catalog dataset objects (see ``core/runner.py``), bound **by
    keyword** — so the function's parameter name must equal the dataset name.
    Declaring it here rather than inside the function body is the point: a
    reader of the pipeline definition can see which nodes have write side
    effects. Kedro spells the same idea ``confirms``.

    Construction validates A5 and A6 (see
    ``docs/agents/architecture-constraints.md``). Both were previously left
    to the AST audit in ``tests/test_core/test_architecture_constraints.py``,
    which only speaks when the suite runs; raising here means a malformed
    node fails at the line that writes it. The audit test stays as a second
    line — it also checks the ``pipeline.py``-level registries (A7), which no
    constructor can see. Kedro raises both at construction too
    (``kedro/pipeline/node.py``).

    A dict of inputs is also checked against the function's signature, as
    Kedro does: bound by name, a misspelt or unwired parameter would
    otherwise surface only when the Runner calls the node — for a sink at
    the end of a training run, hours in. A list is not held to it.
    """

    def __init__(self, func, inputs=None, outputs=None, name=None, writes=None):
        self.func = func
        self.name = name or func.__name__
        if isinstance(inputs, Mapping):
            self.keyword_inputs = dict(inputs)
            self.inputs = list(inputs.values())
        else:
            self.keyword_inputs = None
            self.inputs = self._normalize(inputs, "inputs")
        self.outputs = self._normalize(outputs, "outputs")
        self.writes = self._normalize(writes, "writes")
        self._validate()

    def _normalize(self, value, argument):
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, Mapping):
            # ``list(dict)`` would quietly keep the keys — the way a dict
            # passed as ``inputs`` used to be read before it meant anything.
            raise TypeError(
                f"Node '{self.name}': only `inputs` takes a {{parameter: "
                f"dataset}} dict; `{argument}` takes a name or a list of names."
            )
        return list(value)

    def _validate(self):
        """Raise on A5 / A6 violations. Runs on the normalized lists.

        Normalized is the point: ``outputs=None`` and ``outputs=[]`` are the
        same emptiness, and this repo spells a zero-output side-effect node
        with the former. The audit test's first cut read ``outputs=None`` as
        "cannot be judged" and skipped those nodes, which made A5 a no-op;
        checking the attribute rather than the argument cannot repeat that.
        """
        if not self.inputs and not self.outputs and not self.writes:
            raise ValueError(
                f"Node '{self.name}' has no inputs, no outputs and no write "
                f"targets — it needs at least one to have any effect (A5). "
                f"A side-effect node keeps its inputs and sets outputs=None."
            )
        collisions = sorted(
            (set(self.inputs) | set(self.writes)) & set(self.outputs)
        )
        if collisions:
            raise ValueError(
                f"Node '{self.name}' uses {collisions} as both an "
                f"input/write target and an output (A6). The Runner loads "
                f"every input, executes, then saves every output, so naming "
                f"the same dataset on both sides has no defined meaning — "
                f"use a separate catalog entry, or declare only the write."
            )
        if self.keyword_inputs is not None:
            self._check_keyword_inputs()

    def _check_keyword_inputs(self):
        """Raise unless the dict of inputs plus the write targets bind to
        ``func`` by name — the same call the Runner will make."""
        twice = sorted(set(self.keyword_inputs) & set(self.writes))
        if twice:
            raise TypeError(
                f"Node '{self.name}': {twice} is both a key of `inputs` and a "
                f"write target; the Runner passes a write target under its own "
                f"name, so that parameter would be passed twice."
            )
        try:
            inspect.signature(self.func).bind(
                **dict.fromkeys([*self.keyword_inputs, *self.writes]))
        except TypeError as exc:
            raise TypeError(
                f"Node '{self.name}': inputs {list(self.keyword_inputs)} (by "
                f"name) and writes {self.writes} do not fit the signature of "
                f"{getattr(self.func, '__name__', self.func)!s}: {exc}"
            ) from None

    def __repr__(self):
        base = f"Node({self.name}, {self.inputs} -> {self.outputs}"
        if self.writes:
            base += f", writes={self.writes}"
        return base + ")"
