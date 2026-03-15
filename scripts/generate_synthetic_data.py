"""Generate synthetic feature_table and label_table Parquet files for local dev."""

import numpy as np
import pandas as pd

RANDOM_SEED = 42
NUM_CUSTOMERS = 200
SNAP_DATES = ["2024-01-31", "2024-02-29", "2024-03-31"]
PRODUCTS = ["fx", "usd", "stock", "bond", "mix"]
SEGMENTS = ["mass", "affluent", "hnw"]
POSITIVE_LABEL_RATE = 0.10


def generate_feature_table(rng: np.random.Generator) -> pd.DataFrame:
    """Generate synthetic feature table matching feature_concat.sql output."""
    rows = []
    for snap_date in SNAP_DATES:
        cust_ids = [f"C{i:06d}" for i in range(1, NUM_CUSTOMERS + 1)]
        total_aum = rng.exponential(scale=500_000, size=NUM_CUSTOMERS)
        fund_aum = total_aum * rng.uniform(0.0, 0.5, size=NUM_CUSTOMERS)
        in_amt = rng.exponential(scale=50_000, size=NUM_CUSTOMERS)
        out_amt = rng.exponential(scale=40_000, size=NUM_CUSTOMERS)

        for i, cid in enumerate(cust_ids):
            ta = total_aum[i]
            rows.append(
                {
                    "snap_date": pd.Timestamp(snap_date),
                    "cust_id": cid,
                    "total_aum": round(ta, 2),
                    "fund_aum": round(fund_aum[i], 2),
                    "in_amt_sum_l1m": round(in_amt[i], 2),
                    "out_amt_sum_l1m": round(out_amt[i], 2),
                    "in_amt_ratio_l1m": round(in_amt[i] / ta, 6) if ta > 0 else 0.0,
                    "out_amt_ratio_l1m": round(out_amt[i] / ta, 6) if ta > 0 else 0.0,
                }
            )

    df = pd.DataFrame(rows)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    return df


def generate_label_table(rng: np.random.Generator) -> pd.DataFrame:
    """Generate synthetic label table matching label SQL output (cross join pattern)."""
    rows = []
    for snap_date in SNAP_DATES:
        snap_dt = pd.Timestamp(snap_date)
        apply_start = snap_dt + pd.Timedelta(days=1)
        apply_end = snap_dt + pd.Timedelta(days=30)
        cust_ids = [f"C{i:06d}" for i in range(1, NUM_CUSTOMERS + 1)]

        for i, cid in enumerate(cust_ids):
            segment = SEGMENTS[i % len(SEGMENTS)]
            for prod in PRODUCTS:
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


def main():
    rng = np.random.default_rng(RANDOM_SEED)

    feature_table = generate_feature_table(rng)
    label_table = generate_label_table(rng)

    feature_table.to_parquet("data/feature_table.parquet", index=False)
    label_table.to_parquet("data/label_table.parquet", index=False)

    print(f"feature_table: {feature_table.shape}, columns: {list(feature_table.columns)}")
    print(f"label_table:   {label_table.shape}, columns: {list(label_table.columns)}")
    print(f"label=1 rate:  {label_table['label'].mean():.2%}")
    print(f"snap_dates:    {sorted(feature_table['snap_date'].unique())}")
    print(f"products:      {sorted(label_table['prod_name'].unique())}")


if __name__ == "__main__":
    main()
