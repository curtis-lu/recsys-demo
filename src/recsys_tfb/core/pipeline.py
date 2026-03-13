from collections import defaultdict, deque

from recsys_tfb.core.node import Node


class Pipeline:
    """A collection of Nodes with dependency-based execution ordering."""

    def __init__(self, nodes: list[Node]):
        self._nodes = list(nodes)
        self._sorted = self._topological_sort(self._nodes)

    @property
    def nodes(self) -> list[Node]:
        """Return nodes in topologically sorted order."""
        return list(self._sorted)

    @property
    def inputs(self) -> set[str]:
        """Dataset names required but not produced by any node."""
        all_outputs = set()
        for node in self._nodes:
            all_outputs.update(node.outputs)
        all_inputs = set()
        for node in self._nodes:
            all_inputs.update(node.inputs)
        return all_inputs - all_outputs

    @property
    def outputs(self) -> set[str]:
        """Dataset names produced by nodes."""
        result = set()
        for node in self._nodes:
            result.update(node.outputs)
        return result

    @staticmethod
    def _topological_sort(nodes: list[Node]) -> list[Node]:
        """Kahn's algorithm for topological sorting."""
        # Map output name -> node that produces it
        output_to_node: dict[str, Node] = {}
        for node in nodes:
            for out in node.outputs:
                output_to_node[out] = node

        # Build adjacency: producer -> consumers
        in_degree: dict[Node, int] = {node: 0 for node in nodes}
        dependents: dict[Node, list[Node]] = defaultdict(list)

        for node in nodes:
            for inp in node.inputs:
                producer = output_to_node.get(inp)
                if producer is not None and producer is not node:
                    in_degree[node] += 1
                    dependents[producer].append(node)

        queue = deque(n for n in nodes if in_degree[n] == 0)
        sorted_nodes = []

        while queue:
            node = queue.popleft()
            sorted_nodes.append(node)
            for dependent in dependents[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_nodes) != len(nodes):
            raise ValueError("Pipeline has circular dependencies")

        return sorted_nodes

    def only_nodes_with_outputs(self, output_names: list[str]) -> "Pipeline":
        """Return a new Pipeline with only nodes needed to produce the given outputs."""
        output_names_set = set(output_names)
        output_to_node: dict[str, Node] = {}
        for node in self._nodes:
            for out in node.outputs:
                output_to_node[out] = node

        # BFS backward from target outputs to find all needed nodes
        needed = set()
        queue = deque()
        for name in output_names_set:
            node = output_to_node.get(name)
            if node is not None and node not in needed:
                needed.add(node)
                queue.append(node)

        while queue:
            node = queue.popleft()
            for inp in node.inputs:
                producer = output_to_node.get(inp)
                if producer is not None and producer not in needed:
                    needed.add(producer)
                    queue.append(producer)

        # Preserve original ordering
        filtered = [n for n in self._nodes if n in needed]
        return Pipeline(filtered)
