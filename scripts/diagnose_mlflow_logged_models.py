"""診斷 training pipeline `log_experiment` 的 404 logged-models 錯誤。

用途：確認「MLflow client 3.x 的 first-class Logged Model API
(`/api/2.0/mlflow/logged-models`) 在你的 tracking server 上是否存在」。
若 server 是 MLflow < 3.0，這條路由不存在 → `mlflow.<flavor>.log_model()`
會收到 HTML 的 404，正是 training pipeline 報的錯。

執行（用 repo 的 venv，並帶上你跑 training 時相同的 MLFLOW_TRACKING_URI）：

    MLFLOW_TRACKING_URI=<你的 server> \
        PYTHONPATH=src .venv/bin/python scripts/diagnose_mlflow_logged_models.py

不帶 MLFLOW_TRACKING_URI 時 fallback 到本機 `mlruns`（file store，
不會 404，可當對照組驗證 pipeline 程式碼本身沒問題）。
"""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import mlflow


def _resolve_tracking_uri() -> str:
    # 對齊 parameters_training.yaml: tracking_uri: "${env.MLFLOW_TRACKING_URI|mlruns}"
    return os.environ.get("MLFLOW_TRACKING_URI", "mlruns")


def _probe_http(tracking_uri: str) -> None:
    """直接打 server 的版本與 logged-models endpoint，定位 404 來源。"""
    base = tracking_uri.rstrip("/")

    # 1) server 版本（/version 在多數版本都有；沒有就忽略）
    try:
        with urlopen(f"{base}/version", timeout=10) as resp:
            print(f"[server] /version -> {resp.read().decode().strip()}")
    except (HTTPError, URLError) as exc:
        print(f"[server] /version 取不到（{exc}）— 不一定代表異常")

    # 2) 舊 API：search experiments（3.0 之前就存在）
    old_api = f"{base}/api/2.0/mlflow/experiments/search"
    try:
        req = old_api + "?max_results=1"
        with urlopen(req, timeout=10) as resp:
            print(f"[server] OLD  /experiments/search -> HTTP {resp.status} (存在)")
    except HTTPError as exc:
        print(f"[server] OLD  /experiments/search -> HTTP {exc.code}")
    except URLError as exc:
        print(f"[server] OLD  /experiments/search 連不上：{exc}")

    # 3) 新 API：logged-models（MLflow 3.0 才有）— 這就是報錯的 endpoint
    new_api = f"{base}/api/2.0/mlflow/logged-models"
    try:
        with urlopen(new_api, timeout=10) as resp:
            print(f"[server] NEW  /logged-models -> HTTP {resp.status} (存在)")
    except HTTPError as exc:
        if exc.code == 404:
            body = exc.read(200).decode(errors="replace")
            is_html = body.lstrip().lower().startswith("<!doctype")
            print(
                f"[server] NEW  /logged-models -> HTTP 404"
                f"{'（HTML，路由不存在 → server < 3.0）' if is_html else ''}"
            )
            print("  >>> 這就是 training pipeline 報的 404 根因：")
            print("  >>> client 3.x 在 log_model() 時要建 LoggedModel，但 server 沒這條路由。")
        else:
            # 405 之類代表路由存在、只是不收 GET → server 其實支援
            print(f"[server] NEW  /logged-models -> HTTP {exc.code}（路由可能存在）")
    except URLError as exc:
        print(f"[server] NEW  /logged-models 連不上：{exc}")


def _reproduce_log_model(tracking_uri: str) -> None:
    """最小重現：log 一個極小的 lightgbm 模型，看是否在 log_model 炸。"""
    import lightgbm as lgb
    import numpy as np

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("diag_logged_models")

    X = np.random.rand(64, 3)
    y = (X[:, 0] > 0.5).astype(int)
    booster = lgb.train({"objective": "binary", "verbose": -1}, lgb.Dataset(X, y), num_boost_round=2)

    with mlflow.start_run():
        mlflow.log_metric("smoke", 1.0)  # 舊 API，應成功
        print("[repro] log_metric OK（舊 API 正常）")
        mlflow.lightgbm.log_model(booster, artifact_path="model")  # 新 API，可能 404
        print("[repro] log_model OK（server 支援 logged-models）")


def main() -> None:
    tracking_uri = _resolve_tracking_uri()
    print(f"client MLflow 版本 : {mlflow.__version__}")
    print(f"tracking_uri       : {tracking_uri}")
    print("-" * 60)

    if tracking_uri.startswith("http://") or tracking_uri.startswith("https://"):
        _probe_http(tracking_uri)
    else:
        print(f"[info] tracking_uri 非 http(s)（file store）— 不會發 HTTP，故不會出現此 404。")
    print("-" * 60)

    try:
        _reproduce_log_model(tracking_uri)
    except Exception as exc:  # noqa: BLE001 — 診斷腳本要把例外完整印出
        print(f"[repro] log_model 失敗：{type(exc).__name__}: {exc}")
        if "logged-models" in str(exc):
            print("  >>> 確認：與 training pipeline 同一個 404，root cause = server 缺 logged-models 路由。")


if __name__ == "__main__":
    main()
