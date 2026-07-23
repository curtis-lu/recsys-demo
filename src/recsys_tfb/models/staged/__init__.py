"""Staged (two-stage) modeling: stage-1 per-partition models.

Design spec: docs/superpowers/specs/2026-07-23-staged-modeling-design.md
"""

from recsys_tfb.models.staged.adapter import (  # noqa: F401
    StagedMissingGroupError,
    StagedModelAdapter,
)
