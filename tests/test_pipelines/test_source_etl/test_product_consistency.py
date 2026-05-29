"""Lint test: prod_name must stay consistent across yaml configs and ETL SQL.

Six places that hard-code product names must agree:
  1. conf/base/parameters.yaml             schema.categorical_values.prod_name
  2. conf/base/parameters_inference.yaml   inference.products
  3. conf/sql/etl/sample_pool/sample_pool.sql  ``prod`` CTE
  4. conf/sql/etl/label/label_ccard.sql        ``candidate_prod`` CTE
  5. conf/sql/etl/label/label_fund.sql         ``candidate_prod`` CTE
  6. conf/sql/etl/label/label_exchange.sql     ``candidate_prod`` CTE

(4) ∪ (5) ∪ (6) must equal (1) == (2) == (3), and (4)/(5)/(6) must be pairwise
disjoint (no product belongs to more than one 大類).

This is a lint, not a fix — the duplication is acknowledged tech debt. Until a
single source of truth is introduced (Hive dim_product or config-driven SQL
renderer), this test prevents silent drift between the six locations.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
CONF = REPO_ROOT / "conf"


def _extract_cte_body(sql: str, cte_name: str) -> str:
    """Return the paren-balanced body of ``<cte_name> AS ( ... )``."""
    pattern = rf"\b{re.escape(cte_name)}\s+as\s*\("
    m = re.search(pattern, sql, re.IGNORECASE)
    if not m:
        raise AssertionError(f"CTE {cte_name!r} not found")
    start = m.end()
    depth = 1
    for i in range(start, len(sql)):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
    raise AssertionError(f"Unbalanced parens around CTE {cte_name!r}")


def _extract_prod_literals(sql_path: Path, cte_name: str) -> set[str]:
    body = _extract_cte_body(sql_path.read_text(encoding="utf-8"), cte_name)
    return set(re.findall(r"'([a-z_]+)'\s+(?:AS\s+)?prod_name", body, re.IGNORECASE))


def _yaml_load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _diff_msg(name_a: str, set_a: set, name_b: str, set_b: set) -> str:
    only_a = sorted(set_a - set_b)
    only_b = sorted(set_b - set_a)
    return (
        f"prod set mismatch:\n"
        f"  in {name_a} only: {only_a}\n"
        f"  in {name_b} only: {only_b}"
    )


@pytest.fixture(scope="module")
def yaml_prods() -> set[str]:
    params = _yaml_load(CONF / "base" / "parameters.yaml")
    return set(params["schema"]["categorical_values"]["prod_name"])


@pytest.fixture(scope="module")
def inference_prods() -> set[str]:
    params = _yaml_load(CONF / "base" / "parameters_inference.yaml")
    return set(params["inference"]["products"])


@pytest.fixture(scope="module")
def sample_pool_prods() -> set[str]:
    return _extract_prod_literals(
        CONF / "sql" / "etl" / "sample_pool" / "sample_pool.sql", "prod"
    )


@pytest.fixture(scope="module")
def ccard_prods() -> set[str]:
    return _extract_prod_literals(
        CONF / "sql" / "etl" / "label" / "label_ccard.sql", "candidate_prod"
    )


@pytest.fixture(scope="module")
def fund_prods() -> set[str]:
    return _extract_prod_literals(
        CONF / "sql" / "etl" / "label" / "label_fund.sql", "candidate_prod"
    )


@pytest.fixture(scope="module")
def exchange_prods() -> set[str]:
    return _extract_prod_literals(
        CONF / "sql" / "etl" / "label" / "label_exchange.sql", "candidate_prod"
    )


def test_label_categories_nonempty(ccard_prods, fund_prods, exchange_prods):
    """Parser sanity: each candidate_prod CTE parses to a non-empty set."""
    assert ccard_prods, "label_ccard.sql candidate_prod CTE parsed empty"
    assert fund_prods, "label_fund.sql candidate_prod CTE parsed empty"
    assert exchange_prods, "label_exchange.sql candidate_prod CTE parsed empty"


def test_label_categories_disjoint(ccard_prods, fund_prods, exchange_prods):
    """No product may belong to more than one product category."""
    overlaps = {
        "ccard ∩ fund": sorted(ccard_prods & fund_prods),
        "ccard ∩ exchange": sorted(ccard_prods & exchange_prods),
        "fund ∩ exchange": sorted(fund_prods & exchange_prods),
    }
    bad = {k: v for k, v in overlaps.items() if v}
    assert not bad, f"Label category overlaps: {bad}"


def test_yaml_categorical_matches_inference(yaml_prods, inference_prods):
    assert yaml_prods == inference_prods, _diff_msg(
        "parameters.yaml schema.categorical_values.prod_name", yaml_prods,
        "parameters_inference.yaml inference.products", inference_prods,
    )


def test_yaml_categorical_matches_sample_pool(yaml_prods, sample_pool_prods):
    assert yaml_prods == sample_pool_prods, _diff_msg(
        "parameters.yaml schema.categorical_values.prod_name", yaml_prods,
        "sample_pool.sql prod CTE", sample_pool_prods,
    )


def test_yaml_categorical_matches_label_union(
    yaml_prods, ccard_prods, fund_prods, exchange_prods
):
    label_union = ccard_prods | fund_prods | exchange_prods
    assert yaml_prods == label_union, _diff_msg(
        "parameters.yaml schema.categorical_values.prod_name", yaml_prods,
        "label_ccard ∪ label_fund ∪ label_exchange candidate_prod", label_union,
    )


def test_lint_uses_consistency_predicate_for_config_side():
    """The yaml/config arm of the lint must derive from the single predicate,
    not re-parse parameters.yaml independently (prevents definition drift)."""
    import inspect
    from recsys_tfb.core import consistency

    src = inspect.getsource(consistency.resolved_item_values)
    assert "categorical_values" in src  # predicate is the canonical reader

    params = yaml.safe_load((REPO_ROOT / "conf/base/parameters.yaml").read_text())
    declared = sorted(params["schema"]["categorical_values"]["prod_name"])

    # PRODUCTS is a flat single-level list literal in the generator; this regex
    # assumes that (no nested brackets). Fine today; revisit if it ever nests.
    gen = (REPO_ROOT / "scripts/generate_synthetic_data.py").read_text()
    m = re.search(r"PRODUCTS\s*=\s*\[(.*?)\]", gen, re.S)
    syn = sorted(re.findall(r'"([a-z_]+)"', m.group(1)))
    assert syn == declared, f"synthetic PRODUCTS {syn} != declared {declared}"
