"""情境測試共用 fixtures 與 helpers。"""

import json
import pickle
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONF_BASE_DIR = PROJECT_ROOT / "conf" / "base"
SCENARIOS_OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def setup_workdir(
    scenario_name: str,
    feature_table: pd.DataFrame,
    label_table: pd.DataFrame,
    config_overrides: dict[str, dict],
) -> Path:
    """為情境建立隔離的工作目錄。

    Args:
        scenario_name: 情境名稱（如 "scenario_1"）。
        feature_table: 情境專用特徵資料。
        label_table: 情境專用標籤資料。
        config_overrides: 設定覆蓋，格式為 {yaml_stem: content_dict}，
            例如 {"parameters_inference": {"inference": {"snap_dates": ["2024-04-30"]}}}

    Returns:
        工作目錄路徑。
    """
    work_dir = SCENARIOS_OUTPUT_DIR / scenario_name
    if work_dir.exists():
        shutil.rmtree(work_dir)

    # 建立目錄結構
    data_dir = work_dir / "data"
    conf_base = work_dir / "conf" / "base"
    conf_env = work_dir / "conf" / scenario_name
    data_dir.mkdir(parents=True)
    conf_base.mkdir(parents=True)
    conf_env.mkdir(parents=True)

    # 複製 conf/base/ YAML 檔案
    for yaml_file in CONF_BASE_DIR.glob("*.yaml"):
        shutil.copy2(yaml_file, conf_base / yaml_file.name)

    # 寫入情境覆蓋設定
    for stem, content in config_overrides.items():
        with open(conf_env / f"{stem}.yaml", "w") as f:
            yaml.dump(content, f, default_flow_style=False, allow_unicode=True)

    # 寫入情境資料
    feature_table.to_parquet(data_dir / "feature_table.parquet", index=False)
    label_table.to_parquet(data_dir / "label_table.parquet", index=False)

    return work_dir


def run_pipeline(work_dir: Path, pipeline_name: str, env_name: str) -> None:
    """在指定工作目錄下用 subprocess 執行 pipeline CLI。

    Args:
        work_dir: 工作目錄路徑。
        pipeline_name: pipeline 名稱（dataset/training/inference）。
        env_name: 環境名稱（對應 conf/{env_name}/ 目錄）。

    Raises:
        subprocess.CalledProcessError: pipeline 執行失敗時。
    """
    result = subprocess.run(
        [
            sys.executable, "-m", "recsys_tfb",
            "--pipeline", pipeline_name,
            "--env", env_name,
        ],
        cwd=work_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"=== STDOUT ===\n{result.stdout}")
        print(f"=== STDERR ===\n{result.stderr}")
        result.check_returncode()


def promote_model(work_dir: Path) -> None:
    """在情境工作目錄下執行 model promote。

    Args:
        work_dir: 工作目錄路徑。

    Raises:
        subprocess.CalledProcessError: promote 失敗時。
    """
    models_dir = work_dir / "data" / "models"
    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "promote_model.py"),
            "--models-dir", str(models_dir),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"=== STDOUT ===\n{result.stdout}")
        print(f"=== STDERR ===\n{result.stderr}")
        result.check_returncode()


def generate_report(scenario_name: str, work_dir: Path, output_path: Path | None = None) -> str:
    """讀取 pipeline 產出，產生繁體中文驗證報告。

    Args:
        scenario_name: 情境名稱。
        work_dir: 工作目錄路徑。
        output_path: 報告輸出路徑，None 則只回傳字串。

    Returns:
        報告內容字串。
    """
    data_dir = work_dir / "data"
    lines = []
    lines.append("=" * 60)
    lines.append(f"情境測試驗證報告：{scenario_name}")
    lines.append(f"執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 60)

    # --- 資料集 Pipeline ---
    dataset_dir = data_dir / "dataset"
    if dataset_dir.exists():
        lines.append("")
        lines.append("--- 資料集 Pipeline ---")
        # 找到版本目錄（排除 symlinks）
        version_dirs = [
            d for d in dataset_dir.iterdir()
            if d.is_dir() and not d.is_symlink()
        ]
        for vdir in sorted(version_dirs):
            lines.append(f"  Dataset 版本: {vdir.name}")

            for split_name in ["train_set", "train_dev_set", "val_set"]:
                pq_path = vdir / f"{split_name}.parquet"
                if pq_path.exists():
                    df = pd.read_parquet(pq_path)
                    snap_dates = sorted(df["snap_date"].dt.strftime("%Y-%m-%d").unique())
                    lines.append(
                        f"  {split_name}: 行數={len(df)}, 欄位數={len(df.columns)}, "
                        f"snap_dates={snap_dates}"
                    )

            for x_name in ["X_train", "X_val"]:
                pq_path = vdir / f"{x_name}.parquet"
                if pq_path.exists():
                    df = pd.read_parquet(pq_path)
                    lines.append(f"  {x_name}: 形狀={df.shape}, 欄位={list(df.columns)}")

            # preprocessor
            prep_path = vdir / "preprocessor.pkl"
            if prep_path.exists():
                with open(prep_path, "rb") as f:
                    preprocessor = pickle.load(f)
                lines.append(
                    f"  preprocessor: feature_columns={len(preprocessor['feature_columns'])}, "
                    f"categorical_columns={preprocessor['categorical_columns']}"
                )
                lines.append(f"  feature_columns: {preprocessor['feature_columns']}")

            # category_mappings
            cm_path = vdir / "category_mappings.json"
            if cm_path.exists():
                with open(cm_path) as f:
                    cm = json.load(f)
                lines.append(f"  category_mappings: {cm}")

    # --- 訓練 Pipeline ---
    models_dir = data_dir / "models"
    if models_dir.exists():
        lines.append("")
        lines.append("--- 訓練 Pipeline ---")
        version_dirs = [
            d for d in models_dir.iterdir()
            if d.is_dir() and not d.is_symlink()
        ]
        for vdir in sorted(version_dirs):
            lines.append(f"  Model 版本: {vdir.name}")

            bp_path = vdir / "best_params.json"
            if bp_path.exists():
                with open(bp_path) as f:
                    bp = json.load(f)
                lines.append(f"  best_params: {bp}")

            er_path = vdir / "evaluation_results.json"
            if er_path.exists():
                with open(er_path) as f:
                    er = json.load(f)
                lines.append(f"  overall_map: {er.get('overall_map', 'N/A')}")
                lines.append(f"  per_product_ap: {er.get('per_product_ap', {})}")

    # --- 推論 Pipeline ---
    inference_dir = data_dir / "inference"
    if inference_dir.exists():
        lines.append("")
        lines.append("--- 推論 Pipeline ---")
        for mv_dir in sorted(inference_dir.iterdir()):
            if not mv_dir.is_dir() or mv_dir.is_symlink():
                continue
            for sd_dir in sorted(mv_dir.iterdir()):
                if not sd_dir.is_dir():
                    continue
                lines.append(f"  推論路徑: {sd_dir.relative_to(data_dir)}")

                rp_path = sd_dir / "ranked_predictions.parquet"
                if rp_path.exists():
                    df = pd.read_parquet(rp_path)
                    lines.append(f"  ranked_predictions: 行數={len(df)}")
                    lines.append(f"  唯一客戶數: {df['cust_id'].nunique()}")
                    lines.append(f"  唯一產品數: {df['prod_code'].nunique()}")
                    if "snap_date" in df.columns:
                        snap_dates = sorted(df["snap_date"].astype(str).unique())
                        lines.append(f"  snap_dates: {snap_dates}")
                    lines.append(
                        f"  分數範圍: [{df['score'].min():.6f}, {df['score'].max():.6f}]"
                    )
                    lines.append(
                        f"  排名範圍: [{int(df['rank'].min())}, {int(df['rank'].max())}]"
                    )

                    # 每位客戶的產品數
                    prods_per_cust = df.groupby("cust_id")["prod_code"].nunique()
                    lines.append(
                        f"  每位客戶產品數: min={prods_per_cust.min()}, max={prods_per_cust.max()}"
                    )

                    # 前 10 筆樣本
                    lines.append("")
                    lines.append("  前 10 筆樣本:")
                    lines.append(f"  {'cust_id':<12} {'prod_code':<12} {'score':<12} {'rank'}")
                    for _, row in df.head(10).iterrows():
                        lines.append(
                            f"  {row['cust_id']:<12} {row['prod_code']:<12} "
                            f"{row['score']:<12.6f} {int(row['rank'])}"
                        )

                # scoring_dataset
                sd_path = sd_dir / "scoring_dataset.parquet"
                if sd_path.exists():
                    sd_df = pd.read_parquet(sd_path)
                    lines.append(f"  scoring_dataset: 形狀={sd_df.shape}, 欄位={list(sd_df.columns)}")

    lines.append("")
    lines.append("=" * 60)

    report = "\n".join(lines)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)

    print(report)
    return report
