"""擴展合成資料產生器，支援多 snap_dates、額外欄位、額外產品。"""

import numpy as np
import pandas as pd

# --- 常數 ---

BASE_SNAP_DATES = [
    "2024-01-31",
    "2024-02-29",
    "2024-03-31",
    "2024-04-30",
    "2024-05-31",
    "2024-06-30",
]

BASE_PRODUCTS = ["fx", "usd", "stock", "bond", "mix"]

EXTENDED_PRODUCTS = ["fx", "usd", "stock", "bond", "mix", "ploan", "mloan"]

SEGMENTS = ["mass", "affluent", "hnw"]

NUM_CUSTOMERS = 200

POSITIVE_LABEL_RATE = 0.10


def generate_feature_table(
    rng: np.random.Generator,
    snap_dates: list[str] | None = None,
    num_customers: int = NUM_CUSTOMERS,
    extra_columns: bool = False,
) -> pd.DataFrame:
    """產生合成特徵資料表。

    Args:
        rng: numpy 隨機數產生器（確保可重複性）。
        snap_dates: 快照日期清單，預設為 BASE_SNAP_DATES。
        num_customers: 客戶數量。
        extra_columns: 若為 True，額外產生 txn_count_l1m 和 avg_txn_amt_l1m。
    """
    if snap_dates is None:
        snap_dates = BASE_SNAP_DATES

    rows = []
    for snap_date in snap_dates:
        cust_ids = [f"C{i:06d}" for i in range(1, num_customers + 1)]
        total_aum = rng.exponential(scale=500_000, size=num_customers)
        fund_aum = total_aum * rng.uniform(0.0, 0.5, size=num_customers)
        in_amt = rng.exponential(scale=50_000, size=num_customers)
        out_amt = rng.exponential(scale=40_000, size=num_customers)

        if extra_columns:
            txn_count = rng.poisson(lam=15, size=num_customers)
            avg_txn_amt = rng.exponential(scale=10_000, size=num_customers)

        for i, cid in enumerate(cust_ids):
            ta = total_aum[i]
            row = {
                "snap_date": pd.Timestamp(snap_date),
                "cust_id": cid,
                "total_aum": round(ta, 2),
                "fund_aum": round(fund_aum[i], 2),
                "in_amt_sum_l1m": round(in_amt[i], 2),
                "out_amt_sum_l1m": round(out_amt[i], 2),
                "in_amt_ratio_l1m": round(in_amt[i] / ta, 6) if ta > 0 else 0.0,
                "out_amt_ratio_l1m": round(out_amt[i] / ta, 6) if ta > 0 else 0.0,
            }
            if extra_columns:
                row["txn_count_l1m"] = int(txn_count[i])
                row["avg_txn_amt_l1m"] = round(avg_txn_amt[i], 2)
            rows.append(row)

    df = pd.DataFrame(rows)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    return df


def generate_label_table(
    rng: np.random.Generator,
    snap_dates: list[str] | None = None,
    num_customers: int = NUM_CUSTOMERS,
    products: list[str] | None = None,
) -> pd.DataFrame:
    """產生合成標籤資料表。

    Args:
        rng: numpy 隨機數產生器。
        snap_dates: 快照日期清單，預設為 BASE_SNAP_DATES。
        num_customers: 客戶數量。
        products: 產品清單，預設為 BASE_PRODUCTS。
    """
    if snap_dates is None:
        snap_dates = BASE_SNAP_DATES
    if products is None:
        products = BASE_PRODUCTS

    rows = []
    for snap_date in snap_dates:
        snap_dt = pd.Timestamp(snap_date)
        apply_start = snap_dt + pd.Timedelta(days=1)
        apply_end = snap_dt + pd.Timedelta(days=30)
        cust_ids = [f"C{i:06d}" for i in range(1, num_customers + 1)]

        for i, cid in enumerate(cust_ids):
            segment = SEGMENTS[i % len(SEGMENTS)]
            for prod in products:
                label = int(rng.random() < POSITIVE_LABEL_RATE)
                rows.append(
                    {
                        "snap_date": snap_dt,
                        "cust_id": cid,
                        "cust_segment_typ": segment,
                        "apply_start_date": apply_start,
                        "apply_end_date": apply_end,
                        "label": label,
                        "prod_name": prod,
                    }
                )

    df = pd.DataFrame(rows)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    df["apply_start_date"] = pd.to_datetime(df["apply_start_date"])
    df["apply_end_date"] = pd.to_datetime(df["apply_end_date"])
    return df
