"""Generate synthetic feature_table and label_table Parquet files for local dev.

Produces ~15K feature rows (14 months × ~1000-1150 customers × 22 columns)
and ~123K label rows (14 months × customers × 8 products).
"""

import numpy as np
import pandas as pd
from scipy.special import expit, logit

RANDOM_SEED = 42
INITIAL_CUSTOMERS = 1000
MONTHLY_GROWTH_RATE = 0.01

SNAP_DATES = [
    "2025-01-31", "2025-02-28", "2025-03-31", "2025-04-30",
    "2025-05-31", "2025-06-30", "2025-07-31", "2025-08-31",
    "2025-09-30", "2025-10-31", "2025-11-30", "2025-12-31",
    "2026-01-31", "2026-02-28",
]

PRODUCTS = [
    "exchange_usd", "exchange_fx", "ccard_ins", "fund_stock",
    "ccard_bill", "fund_bond", "ccard_cash", "fund_mix",
]

LABEL_RATES = {
    "exchange_usd": 0.15, "exchange_fx": 0.10,
    "ccard_ins": 0.08, "fund_stock": 0.06,
    "ccard_bill": 0.05, "fund_bond": 0.04,
    "ccard_cash": 0.03, "fund_mix": 0.02,
}

SEGMENT_WEIGHTS = {"mass": 0.60, "affluent": 0.30, "hnw": 0.10}

SEGMENTS = list(SEGMENT_WEIGHTS.keys())
SEGMENT_PROBS = list(SEGMENT_WEIGHTS.values())


def _assign_customer_demographics(
    rng: np.random.Generator, cust_ids: list[str], segments: np.ndarray
) -> pd.DataFrame:
    """Generate fixed demographics for each customer."""
    n = len(cust_ids)
    seg_arr = segments

    # Age: segment-dependent normal
    age_params = {"mass": (35, 12), "affluent": (45, 10), "hnw": (55, 8)}
    age = np.zeros(n)
    for seg, (mu, sigma) in age_params.items():
        mask = seg_arr == seg
        age[mask] = rng.normal(mu, sigma, size=mask.sum())
    age = np.clip(age, 20, 80).astype(int)

    # Gender
    gender = rng.choice(["M", "F"], size=n)

    # Tenure months: segment-dependent exponential
    tenure_scales = {"mass": 24, "affluent": 60, "hnw": 96}
    tenure = np.zeros(n)
    for seg, scale in tenure_scales.items():
        mask = seg_arr == seg
        tenure[mask] = rng.exponential(scale, size=mask.sum())
    tenure = np.clip(tenure, 1, 360).astype(int)

    # Income level (1-5): segment-weighted
    income_weights = {
        "mass": [0.35, 0.35, 0.20, 0.08, 0.02],
        "affluent": [0.05, 0.15, 0.35, 0.30, 0.15],
        "hnw": [0.02, 0.05, 0.13, 0.30, 0.50],
    }
    income = np.zeros(n, dtype=int)
    for seg, w in income_weights.items():
        mask = seg_arr == seg
        income[mask] = rng.choice([1, 2, 3, 4, 5], size=mask.sum(), p=w)

    # Risk attribute (C1-C5)
    risk_weights = {
        "mass": [0.30, 0.30, 0.25, 0.10, 0.05],
        "affluent": [0.10, 0.20, 0.30, 0.25, 0.15],
        "hnw": [0.05, 0.10, 0.20, 0.30, 0.35],
    }
    risk = np.empty(n, dtype=object)
    for seg, w in risk_weights.items():
        mask = seg_arr == seg
        risk[mask] = rng.choice(["C1", "C2", "C3", "C4", "C5"], size=mask.sum(), p=w)

    # Education level
    edu_weights = {
        "mass": [0.40, 0.40, 0.15, 0.05],
        "affluent": [0.15, 0.40, 0.30, 0.15],
        "hnw": [0.05, 0.25, 0.40, 0.30],
    }
    education = np.empty(n, dtype=object)
    for seg, w in edu_weights.items():
        mask = seg_arr == seg
        education[mask] = rng.choice(
            ["high_school", "bachelor", "master", "phd"], size=mask.sum(), p=w
        )

    # Marital status (age-shifted)
    married_prob = np.clip((age - 20) / 40, 0.1, 0.8)
    marital = np.where(
        rng.random(n) < married_prob,
        "married",
        np.where(rng.random(n) < 0.15, "divorced", "single"),
    )

    # Channel preference (age-shifted: younger → digital)
    digital_prob = np.clip((60 - age) / 50, 0.1, 0.8)
    channel = np.where(
        rng.random(n) < digital_prob,
        "digital",
        np.where(rng.random(n) < 0.4, "both", "branch"),
    )

    return pd.DataFrame({
        "cust_id": cust_ids,
        "segment": seg_arr,
        "age": age,
        "gender": gender,
        "tenure_months_base": tenure,
        "income_level": income,
        "risk_attr": risk,
        "education_level": education,
        "marital_status": marital,
        "channel_preference": channel,
    })


def _generate_financial_features(
    rng: np.random.Generator, n: int, segments: np.ndarray
) -> dict[str, np.ndarray]:
    """Generate monthly financial features conditioned on segment."""
    # AUM
    aum_scales = {"mass": 200_000, "affluent": 1_000_000, "hnw": 5_000_000}
    total_aum = np.zeros(n)
    for seg, scale in aum_scales.items():
        mask = segments == seg
        total_aum[mask] = rng.exponential(scale, size=mask.sum())

    fund_ratio_caps = {"mass": 0.3, "affluent": 0.4, "hnw": 0.5}
    fund_aum = np.zeros(n)
    for seg, cap in fund_ratio_caps.items():
        mask = segments == seg
        fund_aum[mask] = total_aum[mask] * rng.uniform(0, cap, size=mask.sum())

    # Savings flow
    sav_scales = {"mass": 30_000, "affluent": 100_000, "hnw": 500_000}
    in_amt = np.zeros(n)
    out_amt = np.zeros(n)
    for seg, scale in sav_scales.items():
        mask = segments == seg
        in_amt[mask] = rng.exponential(scale, size=mask.sum())
        out_amt[mask] = rng.exponential(scale * 0.8, size=mask.sum())

    safe_aum = np.where(total_aum > 0, total_aum, 1.0)
    in_ratio = in_amt / safe_aum
    out_ratio = out_amt / safe_aum

    # Credit card
    txn_cnt_lambdas = {"mass": 8, "affluent": 15, "hnw": 25}
    ccard_txn_cnt = np.zeros(n, dtype=int)
    for seg, lam in txn_cnt_lambdas.items():
        mask = segments == seg
        ccard_txn_cnt[mask] = rng.poisson(lam, size=mask.sum())

    ccard_txn_amt = rng.exponential(20_000, size=n) * (1 + ccard_txn_cnt / 10)

    revolving_probs = {"mass": 0.25, "affluent": 0.10, "hnw": 0.05}
    ccard_revolving = np.zeros(n, dtype=int)
    for seg, p in revolving_probs.items():
        mask = segments == seg
        ccard_revolving[mask] = rng.binomial(1, p, size=mask.sum())

    overseas_probs = {"mass": 0.10, "affluent": 0.30, "hnw": 0.50}
    ccard_overseas = np.zeros(n)
    for seg, p in overseas_probs.items():
        mask = segments == seg
        has_overseas = rng.binomial(1, p, size=mask.sum())
        ccard_overseas[mask] = has_overseas * rng.exponential(15_000, size=mask.sum())

    installment_prob = 0.20
    ccard_installment = (
        rng.binomial(1, installment_prob, size=n)
        * rng.exponential(30_000, size=n)
    )

    limit_ranges = {"mass": (50_000, 200_000), "affluent": (200_000, 1_000_000), "hnw": (500_000, 3_000_000)}
    ccard_limit = np.zeros(n)
    for seg, (lo, hi) in limit_ranges.items():
        mask = segments == seg
        ccard_limit[mask] = rng.uniform(lo, hi, size=mask.sum())

    ccard_util = np.clip(ccard_txn_amt / np.where(ccard_limit > 0, ccard_limit, 1.0), 0, 1)

    active_lambdas = {"mass": 1, "affluent": 2, "hnw": 3}
    ccard_active = np.zeros(n, dtype=int)
    for seg, lam in active_lambdas.items():
        mask = segments == seg
        ccard_active[mask] = rng.poisson(lam, size=mask.sum())
    ccard_active = np.clip(ccard_active, 0, 10)

    return {
        "total_aum": np.round(total_aum, 2),
        "fund_aum": np.round(fund_aum, 2),
        "in_amt_sum_l1m": np.round(in_amt, 2),
        "out_amt_sum_l1m": np.round(out_amt, 2),
        "in_amt_ratio_l1m": np.round(in_ratio, 6),
        "out_amt_ratio_l1m": np.round(out_ratio, 6),
        "ccard_txn_cnt_l1m": ccard_txn_cnt,
        "ccard_txn_amt_l1m": np.round(ccard_txn_amt, 2),
        "ccard_revolving_flag": ccard_revolving,
        "ccard_overseas_amt_l1m": np.round(ccard_overseas, 2),
        "ccard_installment_amt_l1m": np.round(ccard_installment, 2),
        "ccard_limit": np.round(ccard_limit, 2),
        "ccard_util_ratio": np.round(ccard_util, 6),
        "ccard_active_cnt": ccard_active,
    }


def _compute_label_prob(base_rate: float, prod: str, features: dict) -> np.ndarray:
    """Compute adjusted label probability using logistic shift."""
    n = len(features["total_aum"])
    base_logit = logit(base_rate)
    score = np.zeros(n)

    safe_aum = np.where(features["total_aum"] > 0, features["total_aum"], 1.0)

    if prod.startswith("fund_"):
        fund_ratio = features["fund_aum"] / safe_aum
        score += 2.0 * (fund_ratio - 0.15)
    elif prod.startswith("exchange_"):
        score += 1.5 * (features["in_amt_ratio_l1m"] - 0.1)
        score += 1.0 * (features["out_amt_ratio_l1m"] - 0.1)
    elif prod.startswith("ccard_"):
        txn_centered = (features["ccard_txn_cnt_l1m"] - 10) / 10
        score += 1.5 * txn_centered
        score += 1.0 * (features["ccard_util_ratio"] - 0.3)

    return expit(base_logit + score)


def generate_feature_table(rng: np.random.Generator) -> pd.DataFrame:
    """Generate synthetic feature table with 14 months, growing customers, 22 features."""
    # Pre-generate all potential customers
    max_customers = int(INITIAL_CUSTOMERS * (1 + MONTHLY_GROWTH_RATE) ** (len(SNAP_DATES) - 1)) + 10
    all_cust_ids = [f"C{i:06d}" for i in range(1, max_customers + 1)]
    all_segments = rng.choice(SEGMENTS, size=max_customers, p=SEGMENT_PROBS)

    demo_df = _assign_customer_demographics(rng, all_cust_ids, all_segments)

    rows_list = []
    for month_idx, snap_date in enumerate(SNAP_DATES):
        n_cust = int(INITIAL_CUSTOMERS * (1 + MONTHLY_GROWTH_RATE) ** month_idx)
        segments = all_segments[:n_cust]

        fin = _generate_financial_features(rng, n_cust, segments)
        demo = demo_df.iloc[:n_cust]

        snap_df = pd.DataFrame({
            "snap_date": pd.Timestamp(snap_date),
            "cust_id": all_cust_ids[:n_cust],
            **fin,
            "age": demo["age"].values,
            "gender": demo["gender"].values,
            "tenure_months": (demo["tenure_months_base"].values + month_idx),
            "income_level": demo["income_level"].values,
            "risk_attr": demo["risk_attr"].values,
            "education_level": demo["education_level"].values,
            "marital_status": demo["marital_status"].values,
            "channel_preference": demo["channel_preference"].values,
        })
        rows_list.append(snap_df)

    df = pd.concat(rows_list, ignore_index=True)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    return df


def generate_label_table(
    rng: np.random.Generator, feature_table: pd.DataFrame
) -> pd.DataFrame:
    """Generate label table with feature-conditioned probabilities."""
    # Build segment lookup from first snap
    first_snap = feature_table[feature_table["snap_date"] == feature_table["snap_date"].min()]
    max_cust_id = feature_table["cust_id"].nunique()
    all_cust_ids = sorted(feature_table["cust_id"].unique())

    # Pre-compute segments: use customer index modulo (consistent with generation)
    max_customers = int(INITIAL_CUSTOMERS * (1 + MONTHLY_GROWTH_RATE) ** (len(SNAP_DATES) - 1)) + 10
    all_segments_rng = np.random.default_rng(RANDOM_SEED)
    all_segments = all_segments_rng.choice(SEGMENTS, size=max_customers, p=SEGMENT_PROBS)

    rows_list = []
    for snap_date in SNAP_DATES:
        snap_dt = pd.Timestamp(snap_date)
        apply_start = snap_dt + pd.Timedelta(days=1)
        apply_end = snap_dt + pd.Timedelta(days=30)

        snap_features = feature_table[feature_table["snap_date"] == snap_dt]
        n_cust = len(snap_features)
        cust_ids = snap_features["cust_id"].values
        segments = all_segments[:n_cust]

        feat_dict = {
            col: snap_features[col].values
            for col in [
                "total_aum", "fund_aum", "in_amt_ratio_l1m",
                "out_amt_ratio_l1m", "ccard_txn_cnt_l1m", "ccard_util_ratio",
            ]
        }

        for prod in PRODUCTS:
            base_rate = LABEL_RATES[prod]
            probs = _compute_label_prob(base_rate, prod, feat_dict)
            labels = (rng.random(n_cust) < probs).astype(int)

            prod_df = pd.DataFrame({
                "snap_date": snap_dt,
                "cust_id": cust_ids,
                "cust_segment_typ": segments,
                "apply_start_date": apply_start,
                "apply_end_date": apply_end,
                "label": labels,
                "prod_name": prod,
            })
            rows_list.append(prod_df)

    df = pd.concat(rows_list, ignore_index=True)
    df["snap_date"] = pd.to_datetime(df["snap_date"])
    df["apply_start_date"] = pd.to_datetime(df["apply_start_date"])
    df["apply_end_date"] = pd.to_datetime(df["apply_end_date"])
    return df


def generate_sample_pool(
    feature_table: pd.DataFrame, label_table: pd.DataFrame
) -> pd.DataFrame:
    """Generate sample pool table at customer-month-product granularity.

    Each customer-month is cross-joined with all products, then LEFT JOINed
    with label_table (for label) and feature_table (for tenure_months,
    channel_preference) to match the SQL schema:
    (snap_date, cust_id, cust_segment_typ, prod_name, label, tenure_months, channel_preference)
    """
    # Use the same segment assignment as feature_table generation
    rng_seg = np.random.default_rng(RANDOM_SEED)
    max_customers = int(INITIAL_CUSTOMERS * (1 + MONTHLY_GROWTH_RATE) ** (len(SNAP_DATES) - 1)) + 10
    all_segments = rng_seg.choice(SEGMENTS, size=max_customers, p=SEGMENT_PROBS)

    rows_list = []
    for snap_date in SNAP_DATES:
        snap_dt = pd.Timestamp(snap_date)
        snap_features = feature_table[feature_table["snap_date"] == snap_dt]
        n_cust = len(snap_features)
        cust_ids = snap_features["cust_id"].values
        segments = all_segments[:n_cust]

        # Cross join each customer with all products
        for prod in PRODUCTS:
            pool_df = pd.DataFrame({
                "snap_date": snap_dt,
                "cust_id": cust_ids,
                "cust_segment_typ": segments,
                "prod_name": prod,
            })
            rows_list.append(pool_df)

    df = pd.concat(rows_list, ignore_index=True)
    df["snap_date"] = pd.to_datetime(df["snap_date"])

    # LEFT JOIN label_table for label column
    label_cols = label_table[["snap_date", "cust_id", "prod_name", "label"]].copy()
    df = df.merge(label_cols, on=["snap_date", "cust_id", "prod_name"], how="left")
    df["label"] = df["label"].fillna(0).astype(int)

    # LEFT JOIN feature_table for tenure_months, channel_preference
    feat_cols = feature_table[["snap_date", "cust_id", "tenure_months", "channel_preference"]].copy()
    df = df.merge(feat_cols, on=["snap_date", "cust_id"], how="left")

    return df


def main():
    rng = np.random.default_rng(RANDOM_SEED)

    feature_table = generate_feature_table(rng)
    label_table = generate_label_table(rng, feature_table)
    sample_pool = generate_sample_pool(feature_table, label_table)

    feature_table.to_parquet("data/feature_table.parquet", index=False)
    label_table.to_parquet("data/label_table.parquet", index=False)
    sample_pool.to_parquet("data/sample_pool.parquet", index=False)

    # Summary
    print("=" * 60)
    print("Synthetic Data Generation Summary")
    print("=" * 60)

    print(f"\nFeature table: {feature_table.shape}")
    print(f"  Columns: {list(feature_table.columns)}")
    print(f"  Snap dates: {sorted(feature_table['snap_date'].dt.strftime('%Y-%m-%d').unique())}")
    print(f"  Customers per month:")
    for sd in SNAP_DATES:
        n = len(feature_table[feature_table["snap_date"] == pd.Timestamp(sd)])
        print(f"    {sd}: {n}")

    print(f"\nLabel table: {label_table.shape}")
    print(f"  Products: {sorted(label_table['prod_name'].unique())}")
    print(f"  Label rates per product:")
    for prod in PRODUCTS:
        rate = label_table[label_table["prod_name"] == prod]["label"].mean()
        target = LABEL_RATES[prod]
        print(f"    {prod}: {rate:.3f} (target: {target:.3f})")

    print(f"\nSample pool: {sample_pool.shape}")
    print(f"  Columns: {list(sample_pool.columns)}")
    print(f"  Segments: {sample_pool['cust_segment_typ'].value_counts().to_dict()}")

    print(f"\nFeature statistics:")
    numeric_cols = feature_table.select_dtypes(include="number").columns
    print(feature_table[numeric_cols].describe().round(2).to_string())


if __name__ == "__main__":
    main()
