"""Single source of truth for config / data consistency invariants.

Every invariant is defined ONCE here as a pure predicate. Layer-1 config-static
validation, Layer-2 preprocessing guards, and the test_product_consistency lint
all call these predicates — no duplicated definitions, no message drift.

All errors subclass ValueError so existing ``except ValueError`` call sites
(__main__._load_config_and_setup) and existing tests keep working unchanged.
"""

from __future__ import annotations


class ConsistencyError(ValueError):
    """Base for all consistency failures (subclasses ValueError by design)."""


class ConfigConsistencyError(ConsistencyError):
    """Config self-contradiction detectable without data (Layer 1)."""


class DataConsistencyError(ConsistencyError):
    """Config disagrees with the actual data (Layer 2)."""
