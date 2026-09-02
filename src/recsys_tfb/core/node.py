class Node:
    """Wraps a function with named inputs and outputs for pipeline execution.

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
    """

    def __init__(self, func, inputs=None, outputs=None, name=None, writes=None):
        self.func = func
        self.inputs = self._normalize(inputs)
        self.outputs = self._normalize(outputs)
        self.writes = self._normalize(writes)
        self.name = name or func.__name__
        self._validate()

    @staticmethod
    def _normalize(value):
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
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

    def __repr__(self):
        base = f"Node({self.name}, {self.inputs} -> {self.outputs}"
        if self.writes:
            base += f", writes={self.writes}"
        return base + ")"
