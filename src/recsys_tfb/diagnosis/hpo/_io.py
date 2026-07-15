"""HPO 診斷 artifact 的原子 JSON 寫入（temp file + os.replace）。"""

import json
import os
import tempfile
from pathlib import Path


def atomic_write_json(path, data: dict) -> None:
    """把 data 以 JSON 原子寫入 path：先寫同目錄 temp 檔再 os.replace。

    覆寫是 idempotent 的；中途崩潰只會留下舊檔或新檔，不會有截斷檔。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(path))
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
