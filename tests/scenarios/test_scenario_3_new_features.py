"""情境 3：驗證新增特徵欄位能被 pipeline 正確處理。

驗證 feature_table 包含信用卡特徵欄位（ccard_txn_cnt_l1m 等）後，
dataset → training → inference 全 pipeline 能正確處理。
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

SCENARIO_NAME = "scenario_3"
NEW_FEATURE_COLUMNS = ["ccard_txn_cnt_l1m", "ccard_txn_amt_l1m"]


@pytest.fixture(scope="module")
def work_dir():
    """建立情境 3 工作目錄並執行全 pipeline。"""
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
                "snap_dates": ["2025-05-31"],
                "products": sorted(BASE_PRODUCTS),
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


def test_x_train_has_new_columns(dataset_version_dir):
    """X_train 應包含信用卡特徵欄位。"""
    X_train = pd.read_parquet(dataset_version_dir / "X_train.parquet")
    for col in NEW_FEATURE_COLUMNS:
        assert col in X_train.columns, f"X_train 缺少欄位: {col}"


def test_preprocessor_includes_new_columns(dataset_version_dir):
    """preprocessor 的 feature_columns 應包含信用卡特徵欄位。"""
    with open(dataset_version_dir / "preprocessor.pkl", "rb") as f:
        preprocessor = pickle.load(f)
    for col in NEW_FEATURE_COLUMNS:
        assert col in preprocessor["feature_columns"], (
            f"preprocessor.feature_columns 缺少: {col}"
        )


def test_scoring_dataset_has_new_columns(work_dir):
    """inference 的 scoring_dataset 應包含信用卡特徵欄位。"""
    sd_files = list((work_dir / "data" / "inference").rglob("scoring_dataset.parquet"))
    assert len(sd_files) >= 1
    sd = pd.read_parquet(sd_files[0])
    for col in NEW_FEATURE_COLUMNS:
        assert col in sd.columns, f"scoring_dataset 缺少欄位: {col}"


def test_ranked_predictions_correct(work_dir):
    """ranked_predictions 行數應正確。"""
    rp_files = list((work_dir / "data" / "inference").rglob("ranked_predictions.parquet"))
    assert len(rp_files) == 1
    rp = pd.read_parquet(rp_files[0])
    assert rp["cust_id"].nunique() == NUM_CUSTOMERS
    assert rp["prod_name"].nunique() == len(BASE_PRODUCTS)


def test_generate_report(work_dir):
    """產生驗證報告。"""
    output_path = SCENARIOS_OUTPUT_DIR / SCENARIO_NAME / "report.txt"
    report = generate_report(SCENARIO_NAME, work_dir, output_path)
    assert "推論 Pipeline" in report
    assert output_path.exists()
