"""情境 1：推論新一週的資料。

驗證以新的 snap_date（2025-06-30）執行 inference pipeline 時，
產出 ranked_predictions 的正確性。
"""

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
    BASE_PRODUCTS,
    BASE_SNAP_DATES,
    NUM_CUSTOMERS,
    generate_feature_table,
    generate_label_table,
)

SCENARIO_NAME = "scenario_1"


@pytest.fixture(scope="module")
def work_dir():
    """建立情境 1 工作目錄並執行全 pipeline。"""
    rng = np.random.default_rng(42)
    feature_table = generate_feature_table(rng, snap_dates=BASE_SNAP_DATES)
    label_table = generate_label_table(rng, snap_dates=BASE_SNAP_DATES)

    config_overrides = {
        "parameters_dataset": {
            "dataset": {
                "train_snap_date_start": "2025-01-31",
                "train_snap_date_end": "2025-04-30",
                "sample_ratio": 1.0,
                "sample_group_keys": ["cust_segment_typ", "prod_name"],
                "sample_ratio_overrides": {},
                "train_dev_ratio": 0.2,
                "enable_calibration": False,
                "calibration_snap_dates": [],
                "calibration_sample_ratio": 1.0,
                "val_snap_dates": ["2025-05-31"],
                "val_sample_ratio": 1.0,
                "test_snap_dates": ["2025-06-30"],
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
                "snap_dates": ["2025-06-30"],
                "products": sorted(BASE_PRODUCTS),
            },
        },
    }

    wdir = setup_workdir(SCENARIO_NAME, feature_table, label_table, config_overrides)

    # 執行 dataset → training → promote → inference
    run_pipeline(wdir, "dataset", SCENARIO_NAME)
    run_pipeline(wdir, "training", SCENARIO_NAME)
    promote_model(wdir)
    run_pipeline(wdir, "inference", SCENARIO_NAME)

    return wdir


@pytest.fixture(scope="module")
def ranked_predictions(work_dir):
    """讀取推論結果。"""
    inference_dir = work_dir / "data" / "inference"
    # 找到 model_version/snap_date 目錄
    rp_files = list(inference_dir.rglob("ranked_predictions.parquet"))
    assert len(rp_files) == 1, f"預期 1 個 ranked_predictions，找到 {len(rp_files)}"
    return pd.read_parquet(rp_files[0])


def test_snap_date_is_new(ranked_predictions):
    """推論結果的 snap_date 應為 2025-06-30。"""
    snap_dates = ranked_predictions["snap_date"].unique()
    assert len(snap_dates) == 1
    assert pd.Timestamp(snap_dates[0]) == pd.Timestamp("2025-06-30")


def test_products_per_customer(ranked_predictions):
    """每位客戶應有 8 個產品排名。"""
    n_products = len(BASE_PRODUCTS)
    prods_per_cust = ranked_predictions.groupby("cust_id")["prod_name"].nunique()
    assert (prods_per_cust == n_products).all(), (
        f"部分客戶產品數不為 {n_products}: {prods_per_cust.value_counts().to_dict()}"
    )


def test_rank_continuous(ranked_predictions):
    """每位客戶的排名應為 1~8 連續整數。"""
    n_products = len(BASE_PRODUCTS)
    expected_ranks = list(range(1, n_products + 1))
    for cust_id, group in ranked_predictions.groupby("cust_id"):
        ranks = sorted(group["rank"].tolist())
        assert ranks == expected_ranks, f"客戶 {cust_id} 排名不連續: {ranks}"


def test_customer_count(ranked_predictions):
    """唯一客戶數應等於 feature_table 在 2025-06-30 的客戶數。"""
    assert ranked_predictions["cust_id"].nunique() == NUM_CUSTOMERS


def test_output_path_uses_model_hash(work_dir):
    """推論產出路徑應使用實際 model hash，而非 'best'。"""
    inference_dir = work_dir / "data" / "inference"
    subdirs = [d for d in inference_dir.iterdir() if d.is_dir() and not d.is_symlink()]
    assert len(subdirs) >= 1
    for d in subdirs:
        assert d.name != "best", "推論路徑不應直接使用 'best'"
        # model hash 為 8 位 hex
        assert len(d.name) == 8, f"目錄名稱不像 model hash: {d.name}"


def test_generate_report(work_dir):
    """產生驗證報告。"""
    output_path = SCENARIOS_OUTPUT_DIR / SCENARIO_NAME / "report.txt"
    report = generate_report(SCENARIO_NAME, work_dir, output_path)
    assert "推論 Pipeline" in report
    assert output_path.exists()
