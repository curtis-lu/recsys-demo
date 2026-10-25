"""The evaluation pipeline's reports: they read the landed results and assemble
the pages, computing nothing; nothing outside the pipeline imports them (S3 and
S9 in ``docs/agents/architecture-constraints.md``).

Import from the module that holds a part, not from this package: the module name
says which part of which report it is.
"""
