"""情境 4：新增產品（ploan, mloan）。

驗證在 label_table 新增 ploan 和 mloan 後，
category_mappings 和推論結果的正確性。
"""

import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.scenarios.conftest import (
    SCENARIOS_OUTPUT_DIR,
    generate_report,
    promote_model,
    run_pipeline,
    setup_workdir,
)
from tests.scenarios.data_generator import (
    BASE_SNAP_DATES,
    EXTENDED_PRODUCTS,
    NUM_CUSTOMERS,
    generate_feature_table,
    generate_label_table,
)

SCENARIO_NAME = "scenario_4"
EXPECTED_NUM_PRODUCTS = len(EXTENDED_PRODUCTS)  # 10


@pytest.fixture(scope="module")
def work_dir():
    """建立情境 4 工作目錄並執行全 pipeline。"""
    rng = np.random.default_rng(42)
    feature_table = generate_feature_table(rng, snap_dates=BASE_SNAP_DATES)
    label_table = generate_label_table(
        rng, snap_dates=BASE_SNAP_DATES, products=EXTENDED_PRODUCTS,
    )

    config_overrides = {
        "parameters_dataset": {
            "dataset": {
                "sample_ratio": 1.0,
                "sample_group_keys": ["snap_date"],
                "train_dev_snap_dates": ["2025-04-30"],
                "val_snap_dates": ["2025-05-31"],
                "prepare_model_input": {
                    "drop_columns": [
                        "snap_date", "cust_id", "label",
                        "apply_start_date", "apply_end_date", "cust_segment_typ",
                    ],
                    "categorical_columns": [
                        "prod_name", "gender", "risk_attr",
                        "education_level", "marital_status", "channel_preference",
                    ],
                },
            },
        },
        "parameters_training": {
            "training": {
                "n_trials": 3,
                "num_iterations": 100,
                "early_stopping_rounds": 20,
            },
        },
        "parameters_inference": {
            "inference": {
                "snap_dates": ["2025-05-31"],
                "products": sorted(EXTENDED_PRODUCTS),
            },
        },
    }

    wdir = setup_workdir(SCENARIO_NAME, feature_table, label_table, config_overrides)

    run_pipeline(wdir, "dataset", SCENARIO_NAME)
    run_pipeline(wdir, "training", SCENARIO_NAME)
    promote_model(wdir)
    run_pipeline(wdir, "inference", SCENARIO_NAME)

    return wdir


def _find_dataset_version_dir(work_dir: Path) -> Path:
    dataset_dir = work_dir / "data" / "dataset"
    version_dirs = [
        d for d in dataset_dir.iterdir()
        if d.is_dir() and not d.is_symlink()
    ]
    assert len(version_dirs) == 1
    return version_dirs[0]


@pytest.fixture(scope="module")
def dataset_version_dir(work_dir):
    return _find_dataset_version_dir(work_dir)


@pytest.fixture(scope="module")
def ranked_predictions(work_dir):
    rp_files = list((work_dir / "data" / "inference").rglob("ranked_predictions.parquet"))
    assert len(rp_files) == 1
    return pd.read_parquet(rp_files[0])


def test_category_mappings_include_new_products(dataset_version_dir):
    """category_mappings 應包含 ploan 和 mloan。"""
    cm_path = dataset_version_dir / "category_mappings.json"
    with open(cm_path) as f:
        cm = json.load(f)
    prod_categories = cm["prod_name"]
    assert "ploan" in prod_categories, f"缺少 ploan: {prod_categories}"
    assert "mloan" in prod_categories, f"缺少 mloan: {prod_categories}"
    assert len(prod_categories) == EXPECTED_NUM_PRODUCTS


def test_train_set_has_all_products(dataset_version_dir):
    """train_set 的 prod_name 唯一值應為 10 個。"""
    train_set = pd.read_parquet(dataset_version_dir / "train_set.parquet")
    unique_products = train_set["prod_name"].nunique()
    assert unique_products == EXPECTED_NUM_PRODUCTS, (
        f"預期 {EXPECTED_NUM_PRODUCTS} 產品，實際: {unique_products}"
    )


def test_products_per_customer(ranked_predictions):
    """每位客戶應有 10 個產品排名。"""
    prods_per_cust = ranked_predictions.groupby("cust_id")["prod_name"].nunique()
    assert (prods_per_cust == EXPECTED_NUM_PRODUCTS).all(), (
        f"部分客戶產品數不為 {EXPECTED_NUM_PRODUCTS}: {prods_per_cust.value_counts().to_dict()}"
    )


def test_rank_continuous(ranked_predictions):
    """每位客戶的排名應為 1~10 連續整數。"""
    expected_ranks = list(range(1, EXPECTED_NUM_PRODUCTS + 1))
    for cust_id, group in ranked_predictions.groupby("cust_id"):
        ranks = sorted(group["rank"].tolist())
        assert ranks == expected_ranks, f"客戶 {cust_id} 排名不連續: {ranks}"


def test_unique_prod_names(ranked_predictions):
    """ranked_predictions 的唯一 prod_name 應為 10 個。"""
    unique_prods = ranked_predictions["prod_name"].nunique()
    assert unique_prods == EXPECTED_NUM_PRODUCTS, (
        f"預期 {EXPECTED_NUM_PRODUCTS} 唯一產品，實際: {unique_prods}"
    )


def test_generate_report(work_dir):
    """產生驗證報告。"""
    output_path = SCENARIOS_OUTPUT_DIR / SCENARIO_NAME / "report.txt"
    report = generate_report(SCENARIO_NAME, work_dir, output_path)
    assert "推論 Pipeline" in report
    assert output_path.exists()
