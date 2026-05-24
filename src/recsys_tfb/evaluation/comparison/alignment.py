"""Common-universe alignment for 2-way model comparison.

Pure-function module: given two prediction DataFrames, return the
intersection of customer IDs and (mapped) product names as Python sets.
"""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.core.consistency import DataConsistencyError


def common_universe(
    a: SparkDataFrame,
    b: SparkDataFrame,
    cust_col: str,
    prod_col: str,
) -> tuple[set, set]:
    """Return `(common_cust, common_prod)` as Python sets.

    Raises ``DataConsistencyError`` (B3) when either intersection is empty —
    caller will surface this as ``fail loud``.
    """
    a_cust = {r[0] for r in a.select(cust_col).distinct().collect()}
    b_cust = {r[0] for r in b.select(cust_col).distinct().collect()}
    common_cust = a_cust & b_cust
    if not common_cust:
        raise DataConsistencyError(
            f"(B3) compare common_cust is empty — A has {len(a_cust)} cust, "
            f"B has {len(b_cust)} cust, intersection = 0. Check snap_date "
            "alignment and cust_id type."
        )

    a_prod = {r[0] for r in a.select(prod_col).distinct().collect()}
    b_prod = {r[0] for r in b.select(prod_col).distinct().collect()}
    common_prod = a_prod & b_prod
    if not common_prod:
        raise DataConsistencyError(
            f"(B3) compare common_prod is empty — A has {len(a_prod)} prods, "
            f"B has {len(b_prod)} prods (after mapping), intersection = 0. "
            "Check prod_mapping config."
        )

    return common_cust, common_prod
