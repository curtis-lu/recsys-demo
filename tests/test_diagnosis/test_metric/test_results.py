"""``diagnosis.metric.results`` —— 診斷落地產物的 loader。

這份 loader 同時服務 pipeline（``render_diagnosis_pages``）與離線工具
（``scripts/render_diagnosis.py``），所以它的回傳契約 ``(results, missing,
unknown)`` 兩邊都依賴，改動要同時看兩個呼叫端。
"""
import json

from recsys_tfb.diagnosis.metric import contract, results


def _write(tmp_path, name, payload):
    (tmp_path / f"{name}.json").write_text(
        json.dumps(payload), encoding="utf-8")


def test_reads_each_registry_diagnosis_by_filename(tmp_path):
    _write(tmp_path, "config_shift", {"delta": 0.25})
    out, missing, unknown = results.load_results(tmp_path)
    assert out == {"config_shift": {"delta": 0.25}}
    assert missing == []
    assert unknown == []


def test_absent_file_is_reported_as_missing_not_raised(tmp_path):
    out, missing, unknown = results.load_results(tmp_path)
    assert out == {}
    assert missing == ["config_shift"]
    assert unknown == []


def test_json_outside_the_registry_is_reported_as_unknown(tmp_path):
    _write(tmp_path, "config_shift", {"delta": 0.25})
    _write(tmp_path, "offset_sweep", {"per_item": {}})
    out, missing, unknown = results.load_results(tmp_path)
    assert list(out) == ["config_shift"]
    assert unknown == ["offset_sweep"]


def test_registry_is_read_at_call_time_not_import_time(tmp_path, monkeypatch):
    """必須用 ``contract.DIAGNOSES``（模組屬性）而不是 ``from … import``。

    組裝層也是在呼叫當下讀同一個屬性，兩邊看到的 registry 才保證是同一份。
    這條同時是 ``scripts/render_diagnosis.py`` 既有兩條 monkeypatch 測試能
    成立的前提——那兩條在搬移後改成走這個模組，這裡先把前提釘住。
    """
    _write(tmp_path, "config_shift", {"delta": 0.25})
    monkeypatch.setattr(contract, "DIAGNOSES", ("config_shift", "not_copied"))
    out, missing, unknown = results.load_results(tmp_path)
    assert missing == ["not_copied"]


def test_results_follow_registry_order(tmp_path, monkeypatch):
    """``results`` 的鍵順序＝registry 順序，不是檔案系統順序。

    ``assemble_diagnosis_pages`` 用 ``enumerate(DIAGNOSES, 1)`` 決定頁面的
    數字前綴，所以順序錯不會有人發現——頁面照樣產出，只是編號亂掉。
    """
    monkeypatch.setattr(contract, "DIAGNOSES", ("b_diag", "a_diag"))
    _write(tmp_path, "a_diag", {"x": 1})
    _write(tmp_path, "b_diag", {"x": 2})
    out, _, _ = results.load_results(tmp_path)
    assert list(out) == ["b_diag", "a_diag"]
