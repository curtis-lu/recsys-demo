"""診斷產物路徑解析。

Only the diagnostics directory itself is resolved here: ``log_experiment``
uploads it whole, and HPO's search diagnostics write under it. The figure
subdirectories (``summary/``, ``cases/``) belong to their catalog entries
(``shap_summary_figures``, ``case_figures``), which write them (ADR-0030
decision 7).
"""

import re
from pathlib import Path


def diagnostics_dir(parameters: dict) -> Path:
    """Resolve（並建立）診斷產物 dir，對齊 catalog 的
    data/models/${model_version}/diagnostics/ 慣例。"""
    mv = parameters["model_version"]
    d = Path("data") / "models" / str(mv) / "diagnostics"
    d.mkdir(parents=True, exist_ok=True)
    return d


def safe_name(s: object) -> str:
    """檔名安全化（item 值可能含空白/斜線）。"""
    return re.sub(r"[^0-9A-Za-z._-]+", "_", str(s))
