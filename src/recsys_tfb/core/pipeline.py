from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable

from recsys_tfb.core.node import Node


@dataclass(frozen=True)
class SlicePlan:
    """Execution plan produced by Pipeline.slice_from / slice_only.

    Pure description — printing and assertions only, no runtime behavior.
    ``auto_included`` records, per pulled-in node, the missing dataset(s)
    that triggered inclusion (first trigger only when one node feeds the
    same producer twice).
    """

    mode: str                                  # "from" | "only"
    requested: tuple[str, ...]                 # node names explicitly selected
    auto_included: dict[str, tuple[str, ...]] = field(default_factory=dict)
    skipped: tuple[str, ...] = ()              # nodes with outputs, not run
    skipped_side_effect: tuple[str, ...] = ()  # zero-output nodes, not run


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

        # Iterates `nodes` in declaration order, so independent zero-in-degree nodes
        # are queued (and thus executed) in the order they appear in the nodes list —
        # list position is significant for independent nodes (e.g. a guard node placed first).
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

    def slice_from(
        self, start_node: str, can_load: Callable[[str], bool]
    ) -> tuple["Pipeline", SlicePlan]:
        """Forward slice: start_node and everything after it (topological
        position semantics), plus the minimal upstream closure for inputs
        that ``can_load`` reports unavailable.

        Counterpart of ``only_nodes_with_outputs`` (which cuts downstream
        and is catalog-agnostic); this cuts upstream and consults the
        catalog through ``can_load``.
        """
        idx = self._node_index(start_node)
        return self._slice_with_expansion("from", self._sorted[idx:], can_load)

    def slice_only(
        self, node_name: str, can_load: Callable[[str], bool]
    ) -> tuple["Pipeline", SlicePlan]:
        """Slice down to a single node plus its minimal upstream closure."""
        idx = self._node_index(node_name)
        return self._slice_with_expansion("only", [self._sorted[idx]], can_load)

    def _node_index(self, name: str) -> int:
        for i, node in enumerate(self._sorted):
            if node.name == name:
                return i
        available = ", ".join(n.name for n in self._sorted)
        raise ValueError(
            f"Unknown node '{name}'. Available nodes (topological order): {available}"
        )

    def _slice_with_expansion(
        self, mode: str, requested: list[Node], can_load: Callable[[str], bool]
    ) -> tuple["Pipeline", SlicePlan]:
        producer: dict[str, Node] = {}
        for node in self._sorted:
            for out in node.outputs:
                producer[out] = node

        keep = set(requested)
        auto: dict[str, list[str]] = {}
        queue = deque(requested)
        while queue:
            node = queue.popleft()
            for inp in node.inputs:
                name = inp[1:] if inp.startswith("@") else inp
                p = producer.get(name)
                if p is None or p in keep:
                    continue
                if not can_load(name):
                    keep.add(p)
                    auto.setdefault(p.name, []).append(name)
                    queue.append(p)

        kept_nodes = [n for n in self._sorted if n in keep]
        plan = SlicePlan(
            mode=mode,
            requested=tuple(n.name for n in requested),
            auto_included={k: tuple(v) for k, v in auto.items()},
            skipped=tuple(
                n.name for n in self._sorted if n not in keep and n.outputs
            ),
            skipped_side_effect=tuple(
                n.name for n in self._sorted if n not in keep and not n.outputs
            ),
        )
        return Pipeline(kept_nodes), plan
