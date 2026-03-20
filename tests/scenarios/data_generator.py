"""擴展合成資料產生器，支援多 snap_dates、額外欄位、8 類產品。"""

import numpy as np
import pandas as pd

# --- 常數 ---

BASE_SNAP_DATES = [
    "2025-01-31",
    "2025-02-28",
    "2025-03-31",
    "2025-04-30",
    "2025-05-31",
    "2025-06-30",
]

BASE_PRODUCTS = [
    "exchange_usd", "exchange_fx", "ccard_ins", "fund_stock",
    "ccard_bill", "fund_bond", "ccard_cash", "fund_mix",
]

EXTENDED_PRODUCTS = BASE_PRODUCTS + ["ploan", "mloan"]

SEGMENTS = ["mass", "affluent", "hnw"]

NUM_CUSTOMERS = 200

POSITIVE_LABEL_RATE = 0.10


def generate_feature_table(
    rng: np.random.Generator,
    snap_dates: list[str] | None = None,
    num_customers: int = NUM_CUSTOMERS,
) -> pd.DataFrame:
    """產生合成特徵資料表（22 欄位）。

    Args:
        rng: numpy 隨機數產生器（確保可重複性）。
        snap_dates: 快照日期清單，預設為 BASE_SNAP_DATES。
        num_customers: 客戶數量。
    """
    if snap_dates is None:
        snap_dates = BASE_SNAP_DATES

    rows = []
    for snap_date in snap_dates:
        cust_ids = [f"C{i:06d}" for i in range(1, num_customers + 1)]
        n = num_customers
        total_aum = rng.exponential(scale=500_000, size=n)
        fund_aum = total_aum * rng.uniform(0.0, 0.5, size=n)
        in_amt = rng.exponential(scale=50_000, size=n)
        out_amt = rng.exponential(scale=40_000, size=n)
        safe_aum = np.where(total_aum > 0, total_aum, 1.0)

        ccard_txn_cnt = rng.poisson(lam=12, size=n)
        ccard_txn_amt = rng.exponential(scale=20_000, size=n)
        ccard_revolving = rng.binomial(1, 0.15, size=n)
        ccard_overseas = rng.binomial(1, 0.2, size=n) * rng.exponential(15_000, size=n)
        ccard_installment = rng.binomial(1, 0.2, size=n) * rng.exponential(30_000, size=n)
        ccard_limit = rng.uniform(50_000, 500_000, size=n)
        ccard_util = np.clip(ccard_txn_amt / np.where(ccard_limit > 0, ccard_limit, 1.0), 0, 1)
        ccard_active = rng.poisson(lam=2, size=n)

        age = np.clip(rng.normal(42, 12, size=n), 20, 80).astype(int)
        gender = rng.choice(["M", "F"], size=n)
        tenure = np.clip(rng.exponential(48, size=n), 1, 360).astype(int)
        income_level = rng.choice([1, 2, 3, 4, 5], size=n)
        risk_attr = rng.choice(["C1", "C2", "C3", "C4", "C5"], size=n)
        education = rng.choice(["high_school", "bachelor", "master", "phd"], size=n)
        marital = rng.choice(["single", "married", "divorced"], size=n)
        channel = rng.choice(["branch", "digital", "both"], size=n)

        for i, cid in enumerate(cust_ids):
            ta = total_aum[i]
            rows.append({
                "snap_date": pd.Timestamp(snap_date),
                "cust_id": cid,
                "total_aum": round(ta, 2),
                "fund_aum": round(fund_aum[i], 2),
                "in_amt_sum_l1m": round(in_amt[i], 2),
                "out_amt_sum_l1m": round(out_amt[i], 2),
                "in_amt_ratio_l1m": round(in_amt[i] / safe_aum[i], 6),
                "out_amt_ratio_l1m": round(out_amt[i] / safe_aum[i], 6),
                "ccard_txn_cnt_l1m": int(ccard_txn_cnt[i]),
                "ccard_txn_amt_l1m": round(ccard_txn_amt[i], 2),
                "ccard_revolving_flag": int(ccard_revolving[i]),
                "ccard_overseas_amt_l1m": round(ccard_overseas[i], 2),
                "ccard_installment_amt_l1m": round(ccard_installment[i], 2),
                "ccard_limit": round(ccard_limit[i], 2),
                "ccard_util_ratio": round(ccard_util[i], 6),
                "ccard_active_cnt": int(ccard_active[i]),
                "age": int(age[i]),
                "gender": gender[i],
                "tenure_months": int(tenure[i]),
                "income_level": int(income_level[i]),
                "risk_attr": risk_attr[i],
                "education_level": education[i],
                "marital_status": marital[i],
                "channel_preference": channel[i],
            })

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
