"""Shared product grouping resolver (single source of truth).

`product_categories` lives at the top level of the merged parameters (sibling
to `schema`) so it is visible to BOTH the training Stage-1 grouping and the
evaluation category collapse, and so it does NOT bust base_dataset_version
(which hashes only `schema`). The category mapping logic here is the single
definition; evaluation's `_build_category_mapping` and the composite trainer
both call it.
"""
from __future__ import annotations

from recsys_tfb.core.schema import get_schema


def resolve_category_mapping(parameters: dict) -> dict[str, str]:
    """Return {item_value: category}. Products absent from every list become
    their own singleton category. Fail-loud on an unknown product or an
    unsupported `unmapped` policy. Reads top-level `product_categories`.
    """
    pc = parameters.get("product_categories", {}) or {}
    schema = get_schema(parameters)
    item_col = schema["item"]
    known = list((schema.get("categorical_values", {}) or {}).get(item_col, []))
    known_set = set(known)

    mapping: dict[str, str] = {}
    for category, prods in (pc.get("mapping", {}) or {}).items():
        for prod in prods:
            if prod not in known_set:
                raise ValueError(
                    f"product_categories.mapping references unknown product "
                    f"'{prod}' (not in schema.categorical_values['{item_col}'])"
                )
            mapping[prod] = category

    unmapped = pc.get("unmapped", "singleton")
    if unmapped != "singleton":
        raise ValueError(
            f"product_categories.unmapped='{unmapped}' unsupported; "
            f"only 'singleton' is implemented"
        )
    for prod in known:
        mapping.setdefault(prod, prod)
    return mapping


def resolve_groups(parameters: dict, grouping: str) -> dict[str, str]:
    """Return {item_value: group_name} for a Stage-1 grouping.

    grouping == 'item'     -> identity (each item is its own group).
    grouping == 'category' -> resolve_category_mapping.
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    known = list((schema.get("categorical_values", {}) or {}).get(item_col, []))
    if grouping == "item":
        return {p: p for p in known}
    if grouping == "category":
        return resolve_category_mapping(parameters)
    raise ValueError(
        f"stage1.grouping={grouping!r} invalid; must be 'item' or 'category'"
    )
