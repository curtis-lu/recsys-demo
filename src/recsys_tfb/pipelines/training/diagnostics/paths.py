"""診斷產物路徑解析。"""

import re
from pathlib import Path


def diagnostics_dir(parameters: dict) -> Path:
    """Resolve（並建立）診斷產物 dir，對齊 catalog 的
    data/models/${model_version}/diagnostics/ 慣例。"""
    mv = parameters["model_version"]
    d = Path("data") / "models" / str(mv) / "diagnostics"
    d.mkdir(parents=True, exist_ok=True)
    return d


def summary_dir(parameters: dict) -> Path:
    d = diagnostics_dir(parameters) / "summary"
    d.mkdir(parents=True, exist_ok=True)
    return d


def per_item_summary_dir(parameters: dict) -> Path:
    d = summary_dir(parameters) / "per_item"
    d.mkdir(parents=True, exist_ok=True)
    return d


def safe_name(s) -> str:
    """檔名安全化（item 值可能含空白/斜線）。"""
    return re.sub(r"[^0-9A-Za-z._-]+", "_", str(s))
