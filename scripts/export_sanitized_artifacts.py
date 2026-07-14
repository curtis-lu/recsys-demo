#!/usr/bin/env python3
"""去識別化匯出：把 model/evaluation 產出物複製一份、遮罩台灣身分證格式的 cust_id。

用途：資料要從公司 VDI 開發環境經傳輸平台搬到本機、交給 AI agent 做模型優化診斷，
出關前必須把個資（客戶識別碼＝台灣身分證字號 ``[A-Z]\\d{9}``）遮罩掉。

設計（見 docs/superpowers/specs/2026-07-14-sanitized-artifact-export-design.md）：
- Blocklist：先全複製、再刷除已知個資值。
- 只依賴 Python 標準庫，可在鎖死的 VDI 直接跑；no network、no extra packages；來源唯讀。
- 二進位副檔名原樣複製；其餘（含未知副檔名）一律當文字刷（fail-safe）。
- 兜底掃描：對「即將輸出」的全部內容重掃 id；有殘留預設 fail-closed（不寫產出、exit≠0）。

用法：
    python scripts/export_sanitized_artifacts.py --model-version 6059dcef \\
        [--eval-snap 20260131] [--out data/export_sanitized] \\
        [--id-pattern 'C\\d{6}'] [--mask-keep 2] [--dry-run] [--force]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# 台灣身分證：第 1 碼英文字母 + 9 碼數字。字母洩漏戶籍縣市、首位數字洩漏性別，
# 故預設 --mask-keep 0（全遮）。舊式居留證為 2 英文 + 8 數字，不符此樣式，
# 客群含外籍人士時需另用 --id-pattern '[A-Z]{2}[0-9]{8}' 補上。
DEFAULT_ID_PATTERN = r"\b[A-Z][0-9]{9}\b"

# 已知二進位副檔名：原樣複製、不刷（無法 regex）。其餘副檔名一律當文字處理。
KNOWN_BINARY_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp",
    ".pkl", ".pickle", ".parquet", ".bin", ".npy", ".npz",
    ".zip", ".gz", ".tar", ".pdf", ".xlsx", ".xls",
}

_TEXT_EXTS_DOC = "非上列副檔名（含未知）一律當文字刷除，UTF-8 無法解碼者退回二進位。"

# 作業系統噪音檔：不納入匯出（不該進傳輸包）。
SKIP_NAMES = {".DS_Store", "Thumbs.db"}


def compile_patterns(pattern_strs):
    """把 regex 字串清單編成 pattern 物件清單。"""
    return [re.compile(p) for p in pattern_strs]


def mask_value(value, mask_keep=0, mask_char="*"):
    """保留前 ``mask_keep`` 碼，其餘等長替換成 ``mask_char``。預設全遮。"""
    keep = max(0, mask_keep)
    if keep >= len(value):
        return value
    return value[:keep] + mask_char * (len(value) - keep)


def scrub_text(text, patterns, mask_keep=0, mask_char="*"):
    """對 text 套用所有 pattern，命中的值遮罩掉。回傳 (刷後文字, 命中總數)。"""
    total = 0

    def _repl(m):
        return mask_value(m.group(), mask_keep, mask_char)

    for pat in patterns:
        text, n = pat.subn(_repl, text)
        total += n
    return text, total


def scan_text(text, patterns):
    """逐行掃描，回傳 [(行號, 命中字串), ...]（兜底用）。"""
    hits = []
    for i, line in enumerate(text.splitlines(), start=1):
        for pat in patterns:
            for m in pat.finditer(line):
                hits.append((i, m.group()))
    return hits


def is_binary_ext(path):
    """副檔名是否屬已知二進位（大小寫不敏感）。"""
    return Path(path).suffix.lower() in KNOWN_BINARY_EXTS


def _gather_sources(data_root, version, eval_snap):
    """收集要匯出的來源檔（models/<v> 全部 + evaluation/<v>[/<snap>]）。"""
    def _files(root):
        return sorted(p for p in root.rglob("*") if p.is_file() and p.name not in SKIP_NAMES)

    sources = []
    models_dir = data_root / "models" / version
    if models_dir.is_dir():
        sources += _files(models_dir)
    eval_dir = data_root / "evaluation" / version
    if eval_dir.is_dir():
        base = eval_dir / eval_snap if eval_snap else eval_dir
        if base.is_dir():
            sources += _files(base)
    return sources, models_dir, eval_dir


def export(
    version,
    data_root,
    out_root,
    eval_snap=None,
    patterns=None,
    mask_keep=0,
    mask_char="*",
    dry_run=False,
    force=False,
    now=None,
):
    """匯出去識別化產出物。回傳稽核報告 dict。

    - 先全複製、對文字檔遮罩 id 值（二進位原樣）。
    - 兜底掃描「即將輸出」的內容找殘留 id。
    - 有殘留且非 force → **不寫產出物**（只寫報告），fail-closed。
    - dry_run → 完全不寫任何檔。
    """
    data_root = Path(data_root)
    out_root = Path(out_root)
    now = now or datetime.now(timezone.utc)
    pattern_strs = list(patterns) if patterns else [DEFAULT_ID_PATTERN]
    compiled = compile_patterns(pattern_strs)

    sources, models_dir, eval_dir = _gather_sources(data_root, version, eval_snap)

    file_reports = []
    outputs = []  # [(relpath: Path, out_bytes: bytes)]
    total_hits = 0

    for src in sources:
        rel = src.relative_to(data_root)
        raw = src.read_bytes()
        hits = 0
        note = None
        if is_binary_ext(src):
            out_bytes, kind = raw, "binary"
            note = "copied unscanned (binary ext)"
        else:
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                out_bytes, kind = raw, "binary"
                note = "could not decode as UTF-8; copied unscanned — verify manually"
            else:
                scrubbed, hits = scrub_text(text, compiled, mask_keep, mask_char)
                out_bytes, kind = scrubbed.encode("utf-8"), "text"
        total_hits += hits
        outputs.append((rel, out_bytes))
        fr = {"path": rel.as_posix(), "kind": kind, "masked_hits": hits}
        if note:
            fr["note"] = note
        file_reports.append(fr)

    # 兜底掃描：獨立於刷除分類，對每個「即將輸出」的內容重掃（best-effort 解碼）。
    residual = []
    for rel, out_bytes in outputs:
        text = out_bytes.decode("utf-8", errors="ignore")
        for ln, match in scan_text(text, compiled):
            residual.append({"path": rel.as_posix(), "line": ln, "match": match})

    artifacts_written = False
    if not dry_run:
        out_version_dir = out_root / version
        out_version_dir.mkdir(parents=True, exist_ok=True)
        if force or not residual:
            for rel, out_bytes in outputs:
                dest = out_version_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(out_bytes)
            artifacts_written = True

    report = {
        "model_version": version,
        "source": {"models": str(models_dir), "evaluation": str(eval_dir)},
        "output_root": str(out_root),
        "eval_snap": eval_snap,
        "generated_at": now.isoformat(),
        "id_patterns": pattern_strs,
        "mask": {"keep": mask_keep, "char": mask_char},
        "files": file_reports,
        "total_files": len(outputs),
        "total_masked_hits": total_hits,
        "residual": residual,
        "artifacts_written": artifacts_written,
        "dry_run": dry_run,
    }

    if not dry_run:
        (out_root / version / "SANITIZATION_REPORT.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="去識別化匯出 model/evaluation 產出物（遮罩台灣身分證 cust_id）。",
        epilog=_TEXT_EXTS_DOC,
    )
    ap.add_argument("--model-version", required=True, help="要匯出的 model_version（data/models/<v>）")
    ap.add_argument("--data-root", default=Path("data"), type=Path, help="data 根目錄（預設 data）")
    ap.add_argument("--out", dest="out_root", default=Path("data/export_sanitized"), type=Path,
                    help="輸出根目錄（預設 data/export_sanitized）")
    ap.add_argument("--eval-snap", default=None, help="只匯出指定 evaluation snap_date；不給則全收")
    ap.add_argument("--id-pattern", dest="id_patterns", action="append", default=None,
                    help=r"cust_id 值的 regex，可重複；預設台灣身分證 \b[A-Z][0-9]{9}\b")
    ap.add_argument("--mask-keep", type=int, default=0, help="保留前幾碼（預設 0=全遮）")
    ap.add_argument("--mask-char", default="*", help="遮罩字元（預設 *）")
    ap.add_argument("--dry-run", action="store_true", help="只掃描與報告、不寫輸出")
    ap.add_argument("--force", action="store_true",
                    help="即使兜底掃描發現殘留 id 仍寫出（不建議）")
    args = ap.parse_args(argv)

    report = export(
        args.model_version,
        data_root=args.data_root,
        out_root=args.out_root,
        eval_snap=args.eval_snap,
        patterns=args.id_patterns,
        mask_keep=args.mask_keep,
        mask_char=args.mask_char,
        dry_run=args.dry_run,
        force=args.force,
    )

    print(
        f"[export] version={report['model_version']} files={report['total_files']} "
        f"masked={report['total_masked_hits']} residual={len(report['residual'])} "
        f"dry_run={report['dry_run']}"
    )

    if report["total_files"] == 0:
        print(
            f"[export] ERROR: 在 {args.data_root} 下找不到 version={args.model_version} 的產出物；"
            f"請確認版本與 --data-root",
            file=sys.stderr,
        )
        return 3

    if report["residual"]:
        header = (
            "[export] WARNING: 殘留 id 但 --force 已寫出："
            if args.force
            else "[export] RESIDUAL id 殘留，已 fail-closed（未寫出產出物，只留報告）："
        )
        print(header, file=sys.stderr)
        for r in report["residual"][:50]:
            print(f"  {r['path']}:{r['line']}  {r['match']}", file=sys.stderr)
        if len(report["residual"]) > 50:
            print(f"  ...（另有 {len(report['residual']) - 50} 筆）", file=sys.stderr)
        return 2

    print(f"[export] OK → {args.out_root / report['model_version']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
