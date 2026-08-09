class Node:
    """Wraps a function with named inputs and outputs for pipeline execution.

    ``writes`` names the datasets this node saves to **itself**, rather than
    returning data for the Runner to save. Those datasets are handed to the
    function as catalog dataset objects (see ``core/runner.py``), bound **by
    keyword** — so the function's parameter name must equal the dataset name.
    Declaring it here rather than inside the function body is the point: a
    reader of the pipeline definition can see which nodes have write side
    effects. Kedro spells the same idea ``confirms``.
    """

    def __init__(self, func, inputs=None, outputs=None, name=None, writes=None):
        self.func = func
        self.inputs = self._normalize(inputs)
        self.outputs = self._normalize(outputs)
        self.writes = self._normalize(writes)
        self.name = name or func.__name__

    @staticmethod
    def _normalize(value):
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    def __repr__(self):
        base = f"Node({self.name}, {self.inputs} -> {self.outputs}"
        if self.writes:
            base += f", writes={self.writes}"
        return base + ")"
