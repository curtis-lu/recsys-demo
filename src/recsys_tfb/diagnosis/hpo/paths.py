"""HPO 搜尋診斷產物路徑。"""

from pathlib import Path

from recsys_tfb.diagnosis.model.paths import diagnostics_dir


def hpo_dir(parameters: dict) -> Path:
    """Resolve（並建立）diagnostics/hpo/ —— HPO 搜尋診斷產物。"""
    d = diagnostics_dir(parameters) / "hpo"
    d.mkdir(parents=True, exist_ok=True)
    return d
