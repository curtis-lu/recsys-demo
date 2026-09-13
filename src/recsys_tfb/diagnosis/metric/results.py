"""診斷的落地產物：按檔名讀回來。

呼叫端是離線工具 ``scripts/render_diagnosis.py``。pipeline 內的
``render_diagnosis_pages`` 自 #342 起不再用它：那個 node 畫的是自己的 inputs
（catalog 讀回的診斷結果），按檔名讀會把這次沒算的舊 JSON 也畫進去
（ADR-0020 bug 9）。兩條路徑對「診斷結果叫什麼、放在哪」仍是同一個答案：
這裡讀的檔名就是 catalog 的 ``filepath``，由
``test_every_registry_diagnosis_has_a_catalog_entry`` 守著。

**本模組刻意不 import pyspark，也不 import 任何會把 pyspark 拉進來的東西。**
離線重繪的全部價值在於「不需要 Spark、兩秒跑完」，import 鏈上多一個 pyspark
就把冷啟動拉回數秒。有測試用 AST 掃這件事
（``tests/scripts/test_render_diagnosis.py``）。
"""
from __future__ import annotations

import json
from pathlib import Path


def load_results(input_dir) -> tuple[dict, list[str], list[str]]:
    """依 ``DIAGNOSES`` 的順序讀 ``<input-dir>/<name>.json``。

    Returns:
        ``(results, missing, unknown)``——``results`` 直接餵給
        ``assemble_diagnosis_pages``；``missing`` 是 registry 有、目錄裡沒有的
        診斷名；``unknown`` 是目錄裡有、registry 沒有的 JSON 檔名。後兩者方向
        相反、目的相同：不讓「沒處理」看起來像「沒問題」。

    這裡用 ``contract.DIAGNOSES``（模組屬性）而不是 ``from … import
    DIAGNOSES``：組裝層也是在呼叫當下讀同一個屬性，兩邊看到的 registry 才保證
    是同一份；測試 monkeypatch ``contract.DIAGNOSES`` 時也才有效。
    """
    from recsys_tfb.diagnosis.metric import contract

    input_dir = Path(input_dir)
    results: dict = {}
    missing: list[str] = []
    # 目錄裡有、但不在 registry 的 JSON。與 missing 是相反方向的同一件事：
    # 不要讓「沒處理」看起來像「沒問題」。使用者拷回來的是整個 diagnosis/
    # 目錄，過渡期裡面還有 metric_ci.json
    # 這個尚未進 registry 的既有診斷——拷了幾份只看到 1 頁而畫面一片安靜，
    # 讀起來像工具壞了。
    unknown = sorted(
        p.stem for p in input_dir.glob("*.json")
        if p.stem not in contract.DIAGNOSES
    )
    for name in contract.DIAGNOSES:
        path = input_dir / f"{name}.json"
        if not path.exists():
            missing.append(name)
            continue
        results[name] = json.loads(path.read_text(encoding="utf-8"))
    return results, missing, unknown
