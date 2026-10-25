"""What each landed result of the evaluation pipeline holds, declared beside
the node that writes it (ADR-0031 principle 2, supplement 2).

A *block* is a top-level key of a landed result (one JSON dict). A node's
module declares a :class:`Produces` per result it lands, directly above the
node; ``tests/test_pipelines/test_evaluation/test_result_blocks.py`` checks the
node writes exactly those blocks, and ``pipeline.result_readers_table`` joins
the declarations with the wiring and the report sections' ``READS`` to list
who reads each one.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import ModuleType


@dataclass(frozen=True)
class Block:
    """One top-level key of a landed result.

    ``definition`` says what the block is, never which report reads it.
    ``grain`` is the unit one entry of it describes. ``when`` is ``None`` for
    a block written on every run, else the condition under which the key is
    written at all; a key that is always there but may hold ``None`` is not
    conditional, its definition says when it is ``None``.
    """

    name: str
    definition: str
    grain: str
    when: str | None = None


@dataclass(frozen=True)
class Produces:
    """The blocks the node lands as the catalog entry ``result``."""

    result: str
    blocks: tuple[Block, ...]


def declared_results(module: ModuleType) -> dict[str, Produces]:
    """Every :class:`Produces` among ``module``'s top-level names, by result.

    Raises when two declare the same result: a result has one producer.
    """
    found: dict[str, Produces] = {}
    for value in vars(module).values():
        if not isinstance(value, Produces):
            continue
        if value.result in found:
            raise ValueError(
                f"{value.result!r} is declared twice in {module.__name__}")
        found[value.result] = value
    return found
