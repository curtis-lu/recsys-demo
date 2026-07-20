"""Tests for scripts/render_diagnosis.py.

離線重繪工具：把公司環境拷回來的 ``<診斷名>.json`` 在本機重畫成 HTML，
讓「調版面 → 看結果」的迴圈不必每次重跑公司環境的 evaluation。

fixture 的 result dict **用真的 ``config_shift.compute`` 產生**（重用
``tests.test_diagnosis.test_metric.test_config_shift`` 的樣本與 params），
不手刻：手刻的 dict 會跟真實 compute 輸出漂移，而這個工具的全部價值就是
「重繪出來的東西跟公司環境真的會看到的東西一樣」。
"""

import json
import re
import sys
from pathlib import Path

import pytest

from recsys_tfb.diagnosis.metric import contract
from recsys_tfb.diagnosis.metric.config_shift._compute import compute
from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages
from scripts.render_diagnosis import main
from tests.test_diagnosis.test_metric.test_config_shift import PARAMS, _sample

#: plotly 每次 ``fig.to_html`` 都給 graph div 一個新的 uuid4，兩次渲染同一份
#: 資料必然差在這些 id 上（實測：兩次 assemble 的頁面長度相同、diff 只有 9 個
#: div id）。比對前正規化掉，否則這條測試恆紅、守不到任何東西。
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)

#: 抽樣描述帶進頁面是 ``assemble_diagnosis_pages`` 的 ``SCOPE.sampling``
#: replace 那一步的可觀察結果。字串刻意獨特，不會被版型或 CSS 意外滿足。
_SAMPLING_DESC = "未抽樣：全部 40 個有正例的 query 都納入。"

#: registry 裡沒有、但使用者可能沒拷回來的診斷名。刻意選一個不會出現在
#: 任何版型字串裡的名字，stderr 斷言才不會被別的輸出意外滿足。
_NOT_COPIED = "zz_not_copied_back"


def _real_result() -> dict:
    """真跑一次 ``compute``，再 JSON round-trip——落地的就是這個形狀。"""
    result = compute((_sample(), {"n_queries": 40,
                                  "sampling_description": _SAMPLING_DESC}),
                     PARAMS)
    return json.loads(json.dumps(result))


def _write_input(dir_: Path, results: dict) -> Path:
    dir_.mkdir(parents=True, exist_ok=True)
    for name, result in results.items():
        (dir_ / f"{name}.json").write_text(json.dumps(result),
                                           encoding="utf-8")
    return dir_


def _write_params(path: Path) -> Path:
    import yaml

    path.write_text(yaml.safe_dump(PARAMS), encoding="utf-8")
    return path


def _normalised_pages(dir_: Path) -> dict[str, str]:
    """目錄下的頁面內容，去掉 plotly.min.js（固定第三方檔）與隨機 div id。"""
    return {
        p.name: _UUID_RE.sub("UUID", p.read_text(encoding="utf-8"))
        for p in sorted(dir_.iterdir())
        if p.suffix == ".html"
    }


def test_renders_html_from_json_without_spark(tmp_path, monkeypatch):
    """重繪不得需要 Spark——這是離線迴圈成立的前提。

    使用者可能在自己的筆電上調版面，那裡沒有 Spark，也不該為了重畫一張圖
    裝一套 Spark。``sys.modules["pyspark"] = None`` 讓任何 ``import pyspark``
    當場 ImportError。
    """
    monkeypatch.setitem(sys.modules, "pyspark", None)
    src = _write_input(tmp_path / "in", {"config_shift": _real_result()})
    out = tmp_path / "out"

    written = main(["--input-dir", str(src), "--output-dir", str(out),
                    "--params", str(_write_params(tmp_path / "params.yaml"))])

    assert (out / "01-config-shift.html").exists()
    assert (out / "index.html").exists()
    assert set(written) == set(out.iterdir())


def test_sampling_description_reaches_the_page(tmp_path):
    """``SCOPE.sampling`` 要從 result 的 sample_meta 填進去。

    這是 ``assemble_diagnosis_pages`` 第 4 步的可觀察結果；沒有這條，script
    可以完全忽略 sample_meta 而其他測試照樣全綠（頁面仍然畫得出來，只是
    範圍說明少了「這些數字算在哪批列上」那一行）。
    """
    src = _write_input(tmp_path / "in", {"config_shift": _real_result()})
    out = tmp_path / "out"

    main(["--input-dir", str(src), "--output-dir", str(out)])

    assert _SAMPLING_DESC in (out / "01-config-shift.html").read_text(
        encoding="utf-8")


def test_skips_missing_diagnoses_without_failing(tmp_path, monkeypatch):
    """只拷了部分 JSON 回來也要能用。

    公司環境可能只跑了部分診斷，使用者也可能只拷一部分回來。缺一項就整個
    炸掉的話，這支工具在最常見的使用情境下不能用。
    """
    monkeypatch.setattr(
        contract, "DIAGNOSES", ("config_shift", _NOT_COPIED))
    src = _write_input(tmp_path / "in", {"config_shift": _real_result()})
    out = tmp_path / "out"

    written = main(["--input-dir", str(src), "--output-dir", str(out)])

    assert (out / "01-config-shift.html").exists()
    assert not (out / f"02-{_NOT_COPIED.replace('_', '-')}.html").exists()
    assert written


def test_reports_skipped_diagnoses_on_stderr(tmp_path, capsys, monkeypatch):
    """跳過要看得見——靜靜少一頁，使用者會以為那項診斷沒問題。

    同時斷言「有拷回來的那項**不**出現在跳過清單」：否則一個把所有名字都
    印到 stderr 的實作也會通過，而那等於沒有回報。
    """
    monkeypatch.setattr(
        contract, "DIAGNOSES", ("config_shift", _NOT_COPIED))
    src = _write_input(tmp_path / "in", {"config_shift": _real_result()})

    main(["--input-dir", str(src), "--output-dir", str(tmp_path / "out")])

    err = capsys.readouterr().err
    assert _NOT_COPIED in err
    assert "config_shift" not in err


def test_output_matches_pipeline_generated_pages(tmp_path):
    """重繪結果必須與 pipeline 產出的頁面一致。

    這條是這個工具的核心宣稱：使用者拿本機重繪的頁面給回饋時，講的必須是
    公司環境真的會看到的東西。做法：對同一份 result，分別呼叫
    ``report_builder.assemble_diagnosis_pages``（＝ pipeline 走的那條路）
    與 ``main()``，比對兩邊 HTML 內容相同。

    這也是「script 不得重複實作 assemble_diagnosis_pages」那條約束的守衛：
    在 script 裡抄一份組頁邏輯，兩份就會漂移，而漂移的症狀正是這兩堆檔案
    開始不一樣。
    """
    result = _real_result()
    src = _write_input(tmp_path / "in", {"config_shift": result})
    params_path = _write_params(tmp_path / "params.yaml")

    expected_dir = tmp_path / "expected"
    assemble_diagnosis_pages({"config_shift": result}, PARAMS, expected_dir)

    actual_dir = tmp_path / "actual"
    main(["--input-dir", str(src), "--output-dir", str(actual_dir),
          "--params", str(params_path)])

    expected = _normalised_pages(expected_dir)
    actual = _normalised_pages(actual_dir)
    assert sorted(actual) == sorted(expected)
    assert actual == expected


def test_missing_params_file_falls_back_to_empty_dict(tmp_path):
    """讀不到 params 不擋流程——重繪的價值在於隨手可用。

    多數 ``render`` 用不到 parameters（config_shift 的 ``render`` 完全沒用
    到），為了一個多半用不上的參數而讓整個重繪失敗不划算。
    """
    src = _write_input(tmp_path / "in", {"config_shift": _real_result()})
    out = tmp_path / "out"

    written = main(["--input-dir", str(src), "--output-dir", str(out),
                    "--params", str(tmp_path / "no-such-file.yaml")])

    assert (out / "01-config-shift.html").exists()
    assert written


def test_no_json_at_all_writes_nothing(tmp_path):
    """一份都沒拷回來時不落地半個目錄。

    ``assemble_diagnosis_pages`` 對「一頁都沒有」回空 list（不建目錄、不寫
    3.5MB 的 plotly.min.js）；script 直接呼叫它，這個行為要跟著繼承而不是
    被繞過。
    """
    src = tmp_path / "in"
    src.mkdir()
    out = tmp_path / "out"

    written = main(["--input-dir", str(src), "--output-dir", str(out)])

    assert written == []
    assert not (out / "index.html").exists()


def test_script_declares_no_pyspark_import():
    """script 自己的 import 區不得把 Spark 拉進來。

    為什麼不能只靠上面那條 monkeypatch：script 的 module-level import 在
    **收集測試時**就跑完了，比 ``monkeypatch.setitem`` 早——真的寫了
    ``import pyspark`` 在檔頭，那條測試反而看不到。這裡用 AST 掃 import
    敘述（不是掃全文），docstring 才能自由說明這條約束。
    """
    import ast

    source = (Path(__file__).resolve().parents[2]
              / "scripts" / "render_diagnosis.py").read_text(encoding="utf-8")
    imported = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert not [m for m in imported if m.split(".")[0] == "pyspark"]


def test_reports_json_files_that_are_not_in_the_registry(tmp_path, capsys):
    """目錄裡有、但不在 registry 的 JSON 也要講出來。

    使用者拷回來的是整個 diagnosis/ 目錄，過渡期裡面還有 metric_ci.json／
    offset_sweep.json／pair_ledger.json 這些尚未進 registry 的既有診斷。拷了
    4 份卻只看到 1 頁、畫面一片安靜，讀起來像工具壞了。
    """
    src, out = tmp_path / "in", tmp_path / "out"
    _write_input(src, {"config_shift": _real_result()})
    (src / "pair_ledger.json").write_text("{}", encoding="utf-8")
    (src / "metric_ci.json").write_text("{}", encoding="utf-8")

    main(["--input-dir", str(src), "--output-dir", str(out)])

    err = capsys.readouterr().err
    assert "不在 registry" in err
    assert "pair_ledger" in err and "metric_ci" in err
