class Node:
    """Wraps a function with named inputs and outputs for pipeline execution."""

    def __init__(self, func, inputs=None, outputs=None, name=None):
        self.func = func
        self.inputs = self._normalize(inputs)
        self.outputs = self._normalize(outputs)
        self.name = name or func.__name__

    @staticmethod
    def _normalize(value):
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)

    def __repr__(self):
        return f"Node({self.name}, {self.inputs} -> {self.outputs})"
