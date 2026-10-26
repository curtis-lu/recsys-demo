"""The types the evaluation pipeline declares its landed results with
(ADR-0031 principle 2, supplement 2); the declarations are in
``result_catalog``.

A *block* is a top-level key of a landed result (one JSON dict).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


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


def by_result(declarations: Iterable[Produces]) -> dict[str, Produces]:
    """``declarations`` keyed by result, in their order.

    Raises when two declare the same result: a result has one producer.
    """
    found: dict[str, Produces] = {}
    for produces in declarations:
        if produces.result in found:
            raise ValueError(f"{produces.result!r} is declared twice")
        found[produces.result] = produces
    return found
