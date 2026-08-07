"""Mechanism modules the dataset nodes call: one concern per module.

Placement criterion (ADR-0008 section 2): a module belongs here when its only
caller is ``nodes.py``. A module with a consumer outside ``pipelines/dataset/``
stays at the package root, where the directory listing shows it as part of this
pipeline's outward contract -- ``month_plans.py`` is the one such module today
(``__main__.py`` builds the month plans before the pipeline starts and injects
them through the catalog, ADR-0007).

Nothing is re-exported here on purpose: ``nodes.py`` imports each module by
name, so the import line says which concern a step came from.
"""
