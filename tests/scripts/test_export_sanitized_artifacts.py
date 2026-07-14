"""Tests for scripts/export_sanitized_artifacts.py.

去識別化匯出 script：把 model/evaluation 產出物整包複製到輸出夾，
遮罩台灣身分證格式的 cust_id（[A-Z]\\d{9}），供出關給 AI agent 診斷。
"""

import json
from datetime import datetime, timezone

import pytest

from scripts.export_sanitized_artifacts import (
    DEFAULT_ID_PATTERN,
    KNOWN_BINARY_EXTS,
    compile_patterns,
    export,
    is_binary_ext,
    main,
    mask_value,
    scan_text,
    scrub_text,
)

FAKE_ID = "A123456789"  # 台灣身分證格式：1 英文 + 9 數字
FAKE_ID_2 = "B987654321"
OLD_ARC = "AB12345678"  # 舊式居留證：2 英文 + 8 數字
FIXED_NOW = datetime(2026, 7, 14, 3, 0, 0, tzinfo=timezone.utc)


# ---------- 單元：遮罩 ----------

def test_mask_value_default_full_mask():
    # 預設 mask_keep=0：全遮，等長星號，不洩漏縣市/性別
    assert mask_value(FAKE_ID) == "**********"


def test_mask_value_keep_two():
    assert mask_value(FAKE_ID, mask_keep=2) == "A1********"


def test_mask_value_preserves_length_and_char():
    out = mask_value(FAKE_ID, mask_keep=0, mask_char="#")
    assert out == "##########"
    assert len(out) == len(FAKE_ID)


# ---------- 單元：pattern ----------

def test_default_pattern_is_taiwan_national_id():
    pats = compile_patterns([DEFAULT_ID_PATTERN])
    assert scan_text(FAKE_ID, pats)  # 1 英文 + 9 數字 命中
    # 合成 cust_id C000001（C + 6 數字）不該命中
    assert not scan_text("C000001", pats)
    # 8 數字不命中
    assert not scan_text("A12345678", pats)


def test_is_binary_ext():
    assert is_binary_ext("x/shap_summary.png")
    assert is_binary_ext("x/calibrator.PKL")  # 大小寫不敏感
    assert not is_binary_ext("x/manifest.json")
    assert not is_binary_ext("x/report.html")
    assert ".png" in KNOWN_BINARY_EXTS and ".pkl" in KNOWN_BINARY_EXTS


# ---------- 單元：scrub / scan ----------

def test_scrub_text_masks_id_value():
    text = f'{{"owner": "{FAKE_ID}", "amount": 100}}'
    out, n = scrub_text(text, compile_patterns([DEFAULT_ID_PATTERN]))
    assert n == 1
    assert FAKE_ID not in out
    assert "**********" in out


def test_scrub_text_preserves_field_name():
    # 設定裡的欄名字串 "cust_id" 不是身分證值，不該被動到
    text = '{"key_columns": ["cust_id", "snap_date"]}'
    out, n = scrub_text(text, compile_patterns([DEFAULT_ID_PATTERN]))
    assert n == 0
    assert out == text


def test_scrub_text_multiple_patterns():
    text = f"nid={FAKE_ID} arc={OLD_ARC}"
    pats = compile_patterns([DEFAULT_ID_PATTERN, r"\b[A-Z]{2}[0-9]{8}\b"])
    out, n = scrub_text(text, pats)
    assert n == 2
    assert FAKE_ID not in out and OLD_ARC not in out


def test_scan_text_reports_line_numbers():
    text = f"line1\nhas {FAKE_ID} here\nline3\n{FAKE_ID_2}"
    hits = scan_text(text, compile_patterns([DEFAULT_ID_PATTERN]))
    lines = sorted(ln for ln, _ in hits)
    assert lines == [2, 4]


# ---------- fixture：造一個假 data 樹 ----------

def _build_data_root(tmp_path, snap="20260131", extra_files=None):
    v = "A1B2C3D4"
    data = tmp_path / "data"
    mdir = data / "models" / v
    ddir = mdir / "diagnostics"
    ddir.mkdir(parents=True)
    # 含 id 值 + 欄名字串
    (mdir / "manifest.json").write_text(
        json.dumps({"version": v, "sample_owner": FAKE_ID, "note": "cust_id is a column"}),
        encoding="utf-8",
    )
    (mdir / "model.txt").write_text(f"tree model\nleaf owner {FAKE_ID_2}\n", encoding="utf-8")
    # 純聚合、無 id
    (ddir / "shap_diagnostics.json").write_text(json.dumps({"global": {"top": ["a", "b"]}}), encoding="utf-8")
    # 二進位圖
    png_bytes = b"\x89PNG\r\n\x1a\n" + bytes(range(64))
    (ddir / "shap_summary.png").write_bytes(png_bytes)

    edir = data / "evaluation" / v / snap
    edir.mkdir(parents=True)
    (edir / "manifest.json").write_text(
        json.dumps({"segment_sources": {"seg": {"key_columns": ["cust_id", "snap_date"]}}}),
        encoding="utf-8",
    )
    (edir / "report.html").write_text(
        f"<table><tr><td>owner</td><td>{FAKE_ID}</td></tr></table>", encoding="utf-8"
    )
    if extra_files:
        for rel, content in extra_files.items():
            p = data / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                p.write_bytes(content)
            else:
                p.write_text(content, encoding="utf-8")
    return data, v, png_bytes


# ---------- 整合：export ----------

def test_export_copies_and_masks(tmp_path):
    data, v, png_bytes = _build_data_root(tmp_path)
    out = tmp_path / "export"
    report = export(v, data_root=data, out_root=out, now=FIXED_NOW)

    outv = out / v
    # 檔案都在
    assert (outv / "models" / v / "manifest.json").exists()
    assert (outv / "models" / v / "diagnostics" / "shap_summary.png").exists()
    assert (outv / "evaluation" / v / "20260131" / "report.html").exists()

    # id 值被遮
    man = (outv / "models" / v / "manifest.json").read_text(encoding="utf-8")
    assert FAKE_ID not in man and "**********" in man
    # 欄名保留
    ev = (outv / "evaluation" / v / "20260131" / "manifest.json").read_text(encoding="utf-8")
    assert '"cust_id"' in ev
    # html 表格內嵌 id 也被遮
    html = (outv / "evaluation" / v / "20260131" / "report.html").read_text(encoding="utf-8")
    assert FAKE_ID not in html
    # png byte-for-byte 原樣複製
    assert (outv / "models" / v / "diagnostics" / "shap_summary.png").read_bytes() == png_bytes

    # 報告
    assert report["total_masked_hits"] >= 3  # manifest + model.txt + report.html
    assert report["residual"] == []
    assert report["dry_run"] is False
    rep_file = json.loads((outv / "SANITIZATION_REPORT.json").read_text(encoding="utf-8"))
    assert rep_file["model_version"] == v
    # 每檔命中數存在
    hits_by = {f["path"]: f["masked_hits"] for f in rep_file["files"]}
    assert any(h > 0 for h in hits_by.values())

    # 來源未被修改
    src_man = (data / "models" / v / "manifest.json").read_text(encoding="utf-8")
    assert FAKE_ID in src_man


def test_export_dry_run_writes_nothing(tmp_path):
    data, v, _ = _build_data_root(tmp_path)
    out = tmp_path / "export"
    report = export(v, data_root=data, out_root=out, dry_run=True, now=FIXED_NOW)
    assert report["dry_run"] is True
    assert report["total_masked_hits"] >= 3
    assert not out.exists()  # 完全不寫


def test_export_eval_snap_filter(tmp_path):
    data, v, _ = _build_data_root(tmp_path, snap="20260131")
    # 再加第二個 snap
    edir2 = data / "evaluation" / v / "20260228"
    edir2.mkdir(parents=True)
    (edir2 / "manifest.json").write_text('{"snap": "20260228"}', encoding="utf-8")
    out = tmp_path / "export"
    export(v, data_root=data, out_root=out, eval_snap="20260131", now=FIXED_NOW)
    outv = out / v / "evaluation" / v
    assert (outv / "20260131").exists()
    assert not (outv / "20260228").exists()  # 只匯出指定 snap


def test_export_multiple_patterns_including_arc(tmp_path):
    data, v, _ = _build_data_root(
        tmp_path, extra_files={f"models/{'A1B2C3D4'}/foreign.txt": f"arc {OLD_ARC}\n"}
    )
    out = tmp_path / "export"
    export(
        v,
        data_root=data,
        out_root=out,
        patterns=[DEFAULT_ID_PATTERN, r"\b[A-Z]{2}[0-9]{8}\b"],
        now=FIXED_NOW,
    )
    txt = (out / v / "models" / v / "foreign.txt").read_text(encoding="utf-8")
    assert OLD_ARC not in txt


# ---------- 兜底掃描：漏網 id 要被抓到並 fail-closed ----------

def test_backstop_catches_id_in_binary_and_fails_closed(tmp_path):
    # 一個 .pkl（已知二進位）內含明文 id：不會被 scrub，但兜底掃描要抓到
    data, v, _ = _build_data_root(
        tmp_path, extra_files={f"models/{'A1B2C3D4'}/leak.pkl": b"blob " + FAKE_ID.encode() + b" end"}
    )
    out = tmp_path / "export"
    report = export(v, data_root=data, out_root=out, now=FIXED_NOW)
    assert report["residual"], "兜底掃描應抓到 .pkl 內的明文 id"
    assert any("leak.pkl" in r["path"] for r in report["residual"])
    # fail-closed：預設不寫產出物（只留報告），避免把漏網的一包送出關
    assert not (out / v / "models" / v / "leak.pkl").exists()
    assert (out / v / "SANITIZATION_REPORT.json").exists()


def test_report_file_does_not_embed_raw_id(tmp_path):
    # fail-closed 下報告是唯一產物，其內容不得夾帶原始 id（residual match 要遮）
    data, v, _ = _build_data_root(
        tmp_path, extra_files={f"models/{'A1B2C3D4'}/leak.pkl": b"id " + FAKE_ID.encode()}
    )
    out = tmp_path / "export"
    report = export(v, data_root=data, out_root=out, now=FIXED_NOW)
    assert report["residual"]  # 記憶體回傳仍保留原值供 stderr 排查
    blob = (out / v / "SANITIZATION_REPORT.json").read_text(encoding="utf-8")
    assert FAKE_ID not in blob  # 但寫進檔案的報告不得有原始 id
    persisted = json.loads(blob)
    assert persisted["residual"], "報告仍應記錄有殘留（只是遮掉值）"


def test_main_exit_2_on_residual(tmp_path):
    data, v, _ = _build_data_root(
        tmp_path, extra_files={f"models/{'A1B2C3D4'}/leak.pkl": b"id " + FAKE_ID.encode()}
    )
    out = tmp_path / "export"
    rc = main(["--model-version", v, "--data-root", str(data), "--out", str(out)])
    assert rc == 2


def test_main_exit_0_on_clean(tmp_path):
    data, v, _ = _build_data_root(tmp_path)
    out = tmp_path / "export"
    rc = main(["--model-version", v, "--data-root", str(data), "--out", str(out)])
    assert rc == 0
    assert (out / v / "SANITIZATION_REPORT.json").exists()


def test_skips_os_junk_files(tmp_path):
    # .DS_Store 這類 OS 噪音不該進傳輸包
    data, v, _ = _build_data_root(
        tmp_path, extra_files={f"models/{'A1B2C3D4'}/.DS_Store": b"\x00\x01junk"}
    )
    out = tmp_path / "export"
    report = export(v, data_root=data, out_root=out, now=FIXED_NOW)
    assert not (out / v / "models" / v / ".DS_Store").exists()
    assert all(".DS_Store" not in f["path"] for f in report["files"])


def test_undecodable_unknown_ext_copied_as_binary(tmp_path):
    # 未知副檔名但含非法 UTF-8 bytes → 退回當二進位原樣複製，不 crash
    bad = b"\xff\xfe\x00\x01 not decodable"
    data, v, _ = _build_data_root(tmp_path, extra_files={f"models/{'A1B2C3D4'}/weird.dat": bad})
    out = tmp_path / "export"
    report = export(v, data_root=data, out_root=out, now=FIXED_NOW)
    assert (out / v / "models" / v / "weird.dat").read_bytes() == bad
    kinds = {f["path"].split("/")[-1]: f for f in report["files"]}
    assert kinds["weird.dat"]["kind"] == "binary"
