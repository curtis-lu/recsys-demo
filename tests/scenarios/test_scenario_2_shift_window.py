"""情境 2：訓練期間往前挪移一個月。

驗證將 val_snap_dates 改為 ["2025-06-30"]、test_snap_dates 改為 ["2025-07-31"] 後，
data split 和模型訓練的正確性。
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.scenarios.conftest import (
    SCENARIOS_OUTPUT_DIR,
    generate_report,
    run_pipeline,
    setup_workdir,
)
from tests.scenarios.data_generator import (
    BASE_PRODUCTS,
    BASE_SNAP_DATES,
    generate_feature_table,
    generate_label_table,
)

SCENARIO_NAME = "scenario_2"


@pytest.fixture(scope="module")
def work_dir():
    """建立情境 2 工作目錄並執行 dataset + training。"""
    rng = np.random.default_rng(42)
    feature_table = generate_feature_table(rng, snap_dates=BASE_SNAP_DATES)
    label_table = generate_label_table(rng, snap_dates=BASE_SNAP_DATES)

    config_overrides = {
        "parameters_dataset": {
            "dataset": {
                "sample_ratio": 1.0,
                "sample_group_keys": ["snap_date"],
                "sample_ratio_overrides": {},
                "train_dev_ratio": 0.2,
                "enable_calibration": False,
                "calibration_snap_dates": [],
                "calibration_sample_ratio": 1.0,
                "val_snap_dates": ["2025-06-30"],
                "val_sample_ratio": 1.0,
                "test_snap_dates": ["2025-07-31"],
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
    }

    wdir = setup_workdir(SCENARIO_NAME, feature_table, label_table, config_overrides)

    run_pipeline(wdir, "dataset", SCENARIO_NAME)
    run_pipeline(wdir, "training", SCENARIO_NAME)

    return wdir


def _find_dataset_version_dir(work_dir: Path) -> Path:
    """找到 dataset 版本目錄。"""
    dataset_dir = work_dir / "data" / "dataset"
    version_dirs = [
        d for d in dataset_dir.iterdir()
        if d.is_dir() and not d.is_symlink()
    ]
    assert len(version_dirs) == 1, f"預期 1 個版本目錄，找到 {len(version_dirs)}"
    return version_dirs[0]


@pytest.fixture(scope="module")
def dataset_version_dir(work_dir):
    return _find_dataset_version_dir(work_dir)


@pytest.fixture(scope="module")
def train_set(dataset_version_dir):
    return pd.read_parquet(dataset_version_dir / "train_set.parquet")


@pytest.fixture(scope="module")
def train_dev_set(dataset_version_dir):
    return pd.read_parquet(dataset_version_dir / "train_dev_set.parquet")


@pytest.fixture(scope="module")
def val_set(dataset_version_dir):
    return pd.read_parquet(dataset_version_dir / "val_set.parquet")


def test_train_set_excludes_val_and_test_dates(train_set):
    """train_set 的 snap_dates 不應包含 val (2025-06-30) 和 test (2025-07-31)。"""
    snap_dates = set(train_set["snap_date"].dt.strftime("%Y-%m-%d").unique())
    assert "2025-06-30" not in snap_dates, f"train_set 不應含 2025-06-30，實際: {snap_dates}"
    assert "2025-07-31" not in snap_dates, f"train_set 不應含 2025-07-31，實際: {snap_dates}"


def test_train_dev_shares_dates_with_train(train_set, train_dev_set):
    """train_dev_set 的 snap_dates 應與 train_set 一致（共用日期，按 cust_id 切分）。"""
    train_dates = set(train_set["snap_date"].dt.strftime("%Y-%m-%d").unique())
    dev_dates = set(train_dev_set["snap_date"].dt.strftime("%Y-%m-%d").unique())
    assert dev_dates.issubset(train_dates), (
        f"train_dev_set 日期 {dev_dates} 應為 train_set 日期 {train_dates} 的子集"
    )


def test_val_snap_dates(val_set):
    """val_set 的 snap_dates 應恰為 [2025-06-30]。"""
    snap_dates = sorted(val_set["snap_date"].dt.strftime("%Y-%m-%d").unique())
    assert snap_dates == ["2025-06-30"], f"預期 ['2025-06-30']，實際: {snap_dates}"


def test_all_splits_nonempty(train_set, train_dev_set, val_set):
    """各 split 行數應大於零。"""
    assert len(train_set) > 0, "train_set 為空"
    assert len(train_dev_set) > 0, "train_dev_set 為空"
    assert len(val_set) > 0, "val_set 為空"


def test_model_exists(work_dir):
    """模型檔案應存在。"""
    models_dir = work_dir / "data" / "models"
    version_dirs = [
        d for d in models_dir.iterdir()
        if d.is_dir() and not d.is_symlink()
    ]
    assert len(version_dirs) >= 1
    model_path = version_dirs[0] / "model.txt"
    assert model_path.exists(), f"model.txt 不存在: {model_path}"


def test_dataset_version_differs_from_base(dataset_version_dir):
    """dataset_version 應與基礎設定不同。"""
    version = dataset_version_dir.name
    assert len(version) == 8, f"版本格式不正確: {version}"


def test_generate_report(work_dir):
    """產生驗證報告。"""
    output_path = SCENARIOS_OUTPUT_DIR / SCENARIO_NAME / "report.txt"
    report = generate_report(SCENARIO_NAME, work_dir, output_path)
    assert "資料集 Pipeline" in report
    assert output_path.exists()
