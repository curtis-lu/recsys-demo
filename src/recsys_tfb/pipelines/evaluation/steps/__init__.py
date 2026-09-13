"""Mechanisms the evaluation pipeline's nodes call; nothing outside the pipeline
imports them (S3 in ``docs/agents/architecture-constraints.md``).

Import from the module that holds a step, not from this package: the module name
says which concern the step belongs to.
"""
