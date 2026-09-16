"""``tests/conftest.py`` 讓子行程載到「這棵樹」的 ``src``。

只有在 worktree（或任何不是 editable install 指向的那份 checkout）裡、且指令
**不帶** ``PYTHONPATH=`` 前綴時，這兩個測試才守得住東西：在 main repo 裡 editable
install 本來就指向同一份 ``src``，拿掉 conftest 那行它們照樣綠。
"""
import subprocess
import sys
from pathlib import Path

import pytest

# 期望值取自本檔位置，不取自被測環境：tests/test_conftest.py → <tree>/src
_TREE_SRC = Path(__file__).resolve().parents[1] / "src"


def test_child_python_loads_this_tree_src():
    result = subprocess.run(
        [sys.executable, "-c", "import recsys_tfb; print(recsys_tfb.__file__)"],
        capture_output=True, text=True, check=True,
    )
    loaded = Path(result.stdout.strip()).resolve()
    assert loaded.is_relative_to(_TREE_SRC), (
        f"子行程載到 {loaded}，不是這棵樹的 {_TREE_SRC}"
    )


@pytest.mark.spark
def test_spark_worker_loads_this_tree_src(spark):
    def worker_import_path(_):  # 巢狀函式：cloudpickle 以值傳送，worker 端才自己 import
        import recsys_tfb
        return recsys_tfb.__file__

    loaded = Path(
        spark.sparkContext.parallelize([0], 1).map(worker_import_path).collect()[0]
    ).resolve()
    assert loaded.is_relative_to(_TREE_SRC), (
        f"Spark worker 載到 {loaded}，不是這棵樹的 {_TREE_SRC}"
    )
