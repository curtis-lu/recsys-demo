"""Dataset statistics for evaluation reports."""

import pandas as pd


def compute_product_statistics(labels: pd.DataFrame) -> pd.DataFrame:
    """Per-product statistics at customer granularity.

    Returns DataFrame indexed by prod_name with columns:
        positive_customers, negative_customers, total_customers,
        positive_rate, avg_positive_products_per_customer.
    """
    # Collapse to customer-product level: 1 if any label=1 for that pair
    cust_prod = (
        labels.groupby(["prod_name", "cust_id"])["label"]
        .max()
        .reset_index()
    )
    stats = cust_prod.groupby("prod_name").agg(
        positive_customers=("label", "sum"),
        total_customers=("label", "count"),
    )
    stats["negative_customers"] = stats["total_customers"] - stats["positive_customers"]
    stats["positive_rate"] = stats["positive_customers"] / stats["total_customers"]

    # avg_positive_products_per_customer: global value
    pos_per_cust = labels[labels["label"] == 1].groupby("cust_id").size()
    avg_pos = pos_per_cust.mean() if len(pos_per_cust) > 0 else 0.0
    stats["avg_positive_products_per_customer"] = avg_pos

    return stats[
        [
            "positive_customers",
            "negative_customers",
            "total_customers",
            "positive_rate",
            "avg_positive_products_per_customer",
        ]
    ]


def compute_segment_statistics(
    labels: pd.DataFrame, segment_column: str = "cust_segment_typ"
) -> pd.DataFrame:
    """Per-segment statistics at customer granularity.

    Returns DataFrame indexed by segment value with columns:
        positive_customers, negative_customers, total_customers,
        positive_rate, avg_positive_products_per_customer.
    """
    if segment_column not in labels.columns:
        return pd.DataFrame(
            columns=[
                "positive_customers",
                "negative_customers",
                "total_customers",
                "positive_rate",
                "avg_positive_products_per_customer",
            ]
        )

    # Collapse to customer-segment level: 1 if any label=1 for that pair
    cust_seg = (
        labels.groupby([segment_column, "cust_id"])["label"]
        .max()
        .reset_index()
    )
    seg_stats = cust_seg.groupby(segment_column).agg(
        positive_customers=("label", "sum"),
        total_customers=("label", "count"),
    )
    seg_stats["negative_customers"] = seg_stats["total_customers"] - seg_stats["positive_customers"]
    seg_stats["positive_rate"] = seg_stats["positive_customers"] / seg_stats["total_customers"]

    # avg_positive_products_per_customer: per segment
    pos_labels = labels[labels["label"] == 1]
    if len(pos_labels) > 0:
        pos_per_cust_seg = (
            pos_labels.groupby([segment_column, "cust_id"])
            .size()
            .reset_index(name="pos_count")
        )
        avg_by_seg = pos_per_cust_seg.groupby(segment_column)["pos_count"].mean()
        seg_stats["avg_positive_products_per_customer"] = avg_by_seg
    else:
        seg_stats["avg_positive_products_per_customer"] = 0.0

    seg_stats["avg_positive_products_per_customer"] = seg_stats[
        "avg_positive_products_per_customer"
    ].fillna(0.0)

    return seg_stats[
        [
            "positive_customers",
            "negative_customers",
            "total_customers",
            "positive_rate",
            "avg_positive_products_per_customer",
        ]
    ]
