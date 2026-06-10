# logs/ 分子資料夾(pipeline / 月份 兩層)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 pipeline 執行 log 從扁平 `logs/{pipeline}_{run_id}.jsonl` 改為兩層 `logs/{pipeline}/{YYYY-MM}/{pipeline}_{run_id}.jsonl`,並提供一次性 migration script 把現有舊檔歸位。

**Architecture:** 在 `setup_logging()` 組裝 FileHandler 路徑時,於 `logging.file.path`(根目錄)底下多推導 `<pipeline>/<月份>` 兩層;月份由 `run_id` 前 8 碼(YYYYMMDD)解析,非標準 run_id 則 fallback 當下月份。檔名與 JSON 內容格式完全不變,config schema 不動。另加獨立 Typer script `scripts/migrate_logs_layout.py`,把可測純函式(`plan_moves` / `apply_moves`)與薄 CLI 分離,只掃頂層 `*.jsonl` 以保證可重跑。

**Tech Stack:** Python 3.10、Typer 0.20.1、pytest 7.3.1。純檔案系統操作,無 Spark、無 config 變更。

**環境提醒(worktree SOP):** 所有指令在 worktree root `/Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout` 執行;測試一律用絕對 venv python:
`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`。git 用 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout ...`。

---

## File Structure

- **Modify** `src/recsys_tfb/core/logging.py` — 新增私有 helper `_month_from_run_id()`、新增 `import re`、改 `setup_logging()` 的 file handler 路徑組裝(約 line 146–155)。
- **Modify** `tests/test_core/test_logging.py` — 更新 `test_creates_handlers` 斷言、新增非標準 run_id fallback 測試。
- **Create** `scripts/migrate_logs_layout.py` — 一次性 migration CLI(`plan_moves` / `apply_moves` 純函式 + Typer `main`)。
- **Create** `tests/scripts/test_migrate_logs_layout.py` — migration 純函式測試(以 `tmp_path` 建假 log 檔)。

---

## Task 1: setup_logging 落地路徑改兩層 `<pipeline>/<月份>`

**Files:**
- Modify: `src/recsys_tfb/core/logging.py`(top imports + `setup_logging` line ~146–155)
- Test: `tests/test_core/test_logging.py`(`TestSetupLogging`,line ~99–119)

- [ ] **Step 1: 改寫/新增失敗測試**

在 `tests/test_core/test_logging.py` 把現有 `test_creates_handlers` 的最後兩行(line 118–119)由

```python
        # Verify file was created
        log_file = tmp_path / "logs" / "dataset_20260322_120000_aabbcc.jsonl"
        assert log_file.exists()
```

改為斷言新的兩層路徑:

```python
        # Verify file was created under <pipeline>/<YYYY-MM>/
        log_file = (
            tmp_path / "logs" / "dataset" / "2026-03"
            / "dataset_20260322_120000_aabbcc.jsonl"
        )
        assert log_file.exists()
```

並在 `test_creates_handlers` 後面、`test_file_disabled` 前面新增非標準 run_id fallback 測試(放進 `TestSetupLogging` class):

```python
    def test_creates_handlers_nonstandard_run_id_falls_back_to_current_month(
        self, tmp_path
    ):
        from datetime import datetime, timezone

        ctx = RunContext(pipeline="dataset", run_id="custom-run-id")
        config = {
            "logging": {
                "file": {"enabled": True, "path": str(tmp_path / "logs")},
            }
        }
        setup_logging(config, ctx)
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        log_file = (
            tmp_path / "logs" / "dataset" / month / "dataset_custom-run-id.jsonl"
        )
        assert log_file.exists()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_logging.py::TestSetupLogging -q`
Expected: FAIL — `test_creates_handlers` 與新測試皆找不到巢狀路徑的檔案(舊 code 仍寫扁平 `logs/dataset_...jsonl`)。

- [ ] **Step 3: 實作 — 新增 helper 並改路徑組裝**

在 `src/recsys_tfb/core/logging.py` 的 import 區(現有 `import os` 附近)新增:

```python
import re
```

在 `generate_run_id()` 之後(`_current_context` 宣告之前)新增 helper:

```python
_RUN_ID_DATE_RE = re.compile(r"^(\d{4})(\d{2})\d{2}_")


def _month_from_run_id(run_id: str) -> str:
    """Return ``'YYYY-MM'`` parsed from a run_id beginning with ``YYYYMMDD_``.

    Falls back to the current UTC month when ``run_id`` does not start with a
    standard 8-digit date (e.g. a caller-supplied custom run_id).
    """
    match = _RUN_ID_DATE_RE.match(run_id or "")
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return datetime.now(timezone.utc).strftime("%Y-%m")
```

把 `setup_logging()` 內現有的 file handler 區塊(現為):

```python
    if file_enabled:
        log_dir = Path(file_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{context.pipeline}_{context.run_id}.jsonl"
        file_handler = logging.FileHandler(
            log_dir / filename, encoding="utf-8"
        )
```

改為:

```python
    if file_enabled:
        month = _month_from_run_id(context.run_id)
        pipeline_dir = context.pipeline or "_unknown"
        log_dir = Path(file_path) / pipeline_dir / month
        log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{context.pipeline}_{context.run_id}.jsonl"
        file_handler = logging.FileHandler(
            log_dir / filename, encoding="utf-8"
        )
```

(其餘 `file_handler.setFormatter(...)` / `setLevel` / `addHandler` 不動。)

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_logging.py -q`
Expected: PASS(原 22 + 新增 1 = 23 passed)。

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout add src/recsys_tfb/core/logging.py tests/test_core/test_logging.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout commit -m "feat(logging): log 落地改 logs/<pipeline>/<YYYY-MM>/ 兩層"
```

---

## Task 2: 一次性 migration script `scripts/migrate_logs_layout.py`

**Files:**
- Create: `scripts/migrate_logs_layout.py`
- Test: `tests/scripts/test_migrate_logs_layout.py`

- [ ] **Step 1: 寫失敗測試**

建立 `tests/scripts/test_migrate_logs_layout.py`:

```python
"""Tests for the one-time logs/ layout migration script."""
from pathlib import Path

from scripts.migrate_logs_layout import Move, apply_moves, plan_moves


def _touch(p: Path, content: str = "{}\n") -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def test_plan_moves_standard(tmp_path):
    logs = tmp_path / "logs"
    _touch(logs / "dataset_20260322_120000_aabbcc.jsonl")
    moves, skipped = plan_moves(logs)
    assert skipped == []
    assert len(moves) == 1
    assert moves[0].dst == (
        logs / "dataset" / "2026-03" / "dataset_20260322_120000_aabbcc.jsonl"
    )


def test_plan_moves_pipeline_name_with_underscore(tmp_path):
    logs = tmp_path / "logs"
    _touch(logs / "source_etl_20260401_010101_ffeedd.jsonl")
    moves, skipped = plan_moves(logs)
    assert skipped == []
    assert moves[0].dst == (
        logs / "source_etl" / "2026-04" / "source_etl_20260401_010101_ffeedd.jsonl"
    )


def test_plan_moves_skips_unmatched(tmp_path):
    logs = tmp_path / "logs"
    _touch(logs / "notes.txt")        # not *.jsonl -> not globbed at all
    _touch(logs / "random.jsonl")     # *.jsonl but does not match naming
    moves, skipped = plan_moves(logs)
    assert moves == []
    assert [p.name for p, _ in skipped] == ["random.jsonl"]


def test_plan_moves_skips_existing_destination(tmp_path):
    logs = tmp_path / "logs"
    _touch(logs / "dataset_20260322_120000_aabbcc.jsonl")
    _touch(logs / "dataset" / "2026-03" / "dataset_20260322_120000_aabbcc.jsonl")
    moves, skipped = plan_moves(logs)
    assert moves == []
    assert skipped[0][1] == "destination-exists"


def test_apply_moves_relocates_files(tmp_path):
    logs = tmp_path / "logs"
    src = logs / "training_20260510_080000_001122.jsonl"
    _touch(src, '{"event":"x"}\n')
    moves, _ = plan_moves(logs)
    apply_moves(moves)
    dst = logs / "training" / "2026-05" / "training_20260510_080000_001122.jsonl"
    assert dst.exists()
    assert dst.read_text() == '{"event":"x"}\n'
    assert not src.exists()


def test_rerun_after_apply_is_idempotent(tmp_path):
    logs = tmp_path / "logs"
    _touch(logs / "inference_20260601_000000_abcabc.jsonl")
    moves, _ = plan_moves(logs)
    apply_moves(moves)
    # second pass: no top-level *.jsonl remain
    moves2, skipped2 = plan_moves(logs)
    assert moves2 == []
    assert skipped2 == []
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_migrate_logs_layout.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.migrate_logs_layout'`(script 尚未建立)。

- [ ] **Step 3: 實作 migration script**

建立 `scripts/migrate_logs_layout.py`:

```python
"""One-time migration: reorganize flat logs/ into logs/<pipeline>/<YYYY-MM>/.

Old layout: ``logs/{pipeline}_{run_id}.jsonl``  (run_id = ``YYYYMMDD_HHMMSS_{6hex}``)
New layout: ``logs/<pipeline>/<YYYY-MM>/{pipeline}_{run_id}.jsonl``

Defaults to a dry run that prints the planned moves; pass ``--apply`` to perform
them. Only top-level ``*.jsonl`` files are considered, so re-running after a
successful migration is a no-op (idempotent).

Run from the repo root::

    PYTHONPATH=src .venv/bin/python scripts/migrate_logs_layout.py            # dry run
    PYTHONPATH=src .venv/bin/python scripts/migrate_logs_layout.py --apply    # move
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple

import typer

app = typer.Typer(add_completion=False)

# Old flat name: {pipeline}_{YYYYMMDD}_{HHMMSS}_{6hex}.jsonl
# pipeline is greedy (.+) so names containing '_' (e.g. source_etl) split correctly.
_LOG_NAME_RE = re.compile(
    r"^(?P<pipeline>.+)_(?P<date>\d{8})_\d{6}_[0-9a-f]{6}\.jsonl$"
)


class Move(NamedTuple):
    src: Path
    dst: Path


def plan_moves(
    logs_dir: Path,
) -> tuple[list[Move], list[tuple[Path, str]]]:
    """Compute moves for top-level ``*.jsonl`` files in ``logs_dir``.

    Returns ``(moves, skipped)`` where ``moves`` are ``Move(src, dst)`` for files
    matching the old flat naming, and ``skipped`` are ``(path, reason)`` for files
    left in place (``"unmatched-name"`` or ``"destination-exists"``).
    """
    moves: list[Move] = []
    skipped: list[tuple[Path, str]] = []
    for path in sorted(logs_dir.glob("*.jsonl")):
        if not path.is_file():
            continue
        match = _LOG_NAME_RE.match(path.name)
        if match is None:
            skipped.append((path, "unmatched-name"))
            continue
        pipeline = match.group("pipeline")
        date = match.group("date")
        month = f"{date[:4]}-{date[4:6]}"
        dst = logs_dir / pipeline / month / path.name
        if dst.exists():
            skipped.append((path, "destination-exists"))
            continue
        moves.append(Move(src=path, dst=dst))
    return moves, skipped


def apply_moves(moves: list[Move]) -> None:
    """Execute planned moves, creating parent directories as needed."""
    for src, dst in moves:
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)


@app.command()
def main(
    logs_dir: Path = typer.Option(
        Path("logs"), "--logs-dir", help="Root logs directory to migrate."
    ),
    apply: bool = typer.Option(
        False, "--apply", help="Actually move files (default: dry run)."
    ),
) -> None:
    """Migrate flat logs/ into logs/<pipeline>/<YYYY-MM>/ layout."""
    if not logs_dir.exists():
        typer.echo(f"logs dir not found: {logs_dir}")
        raise typer.Exit(code=1)

    moves, skipped = plan_moves(logs_dir)
    verb = "MOVE" if apply else "PLAN"
    for src, dst in moves:
        typer.echo(f"{verb} {src.name} -> {dst.relative_to(logs_dir)}")
    for path, reason in skipped:
        typer.echo(f"SKIP {path.name} ({reason})")
    typer.echo(f"--- {len(moves)} to move, {len(skipped)} skipped ---")

    if apply:
        apply_moves(moves)
        typer.echo("Applied.")
    else:
        typer.echo("Dry run. Re-run with --apply to move.")


if __name__ == "__main__":
    app()
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_migrate_logs_layout.py -q`
Expected: PASS(6 passed)。

- [ ] **Step 5: 手動 smoke dry-run(確認 CLI 可跑)**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/migrate_logs_layout.py --help
```
Expected: 印出 usage,含 `--logs-dir` 與 `--apply` 選項,exit 0。

- [ ] **Step 6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout add scripts/migrate_logs_layout.py tests/scripts/test_migrate_logs_layout.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout commit -m "feat(scripts): 加 migrate_logs_layout 把舊扁平 log 歸位到新結構"
```

---

## Task 3: 全套測試回歸 + 跑 migration 把現有舊檔歸位

**Files:** 無新增(驗證 + 一次性執行)

- [ ] **Step 1: 跑兩個改動到的測試檔確認綠燈**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_logging.py tests/scripts/test_migrate_logs_layout.py -q`
Expected: PASS(23 + 6 = 29 passed)。

- [ ] **Step 2: 對 main repo 的真實 logs/ 先 dry-run 預覽**

> 注意:真實舊 log 在 **main repo** 的 `logs/`(worktree 自己的 `logs/` 是空的)。對 main tree dry-run 預覽:

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
  /Users/curtislu/projects/recsys_tfb/.worktrees/logs-subfolder-layout/scripts/migrate_logs_layout.py \
  --logs-dir /Users/curtislu/projects/recsys_tfb/logs
```
Expected: 印出 PLAN 清單 + 彙總。**先給使用者看計畫,確認後**再 `--apply`(此步驟需使用者點頭,不要自動 apply)。

- [ ] **Step 3:(待使用者確認後)apply 搬移**

Run:同上指令加 `--apply`。
Expected: 印出 MOVE 清單 + `Applied.`;`logs/` 頂層不再有 `*.jsonl`,舊檔已歸位到 `logs/<pipeline>/<YYYY-MM>/`。

---

## Self-Review

**Spec coverage:**
- setup_logging 兩層路徑(spec §1)→ Task 1。
- `_month_from_run_id` helper + 非標準 run_id fallback(spec §1)→ Task 1 Step 3 + 測試 Step 1。
- migration script、dry-run 預設、`--apply`、只掃頂層、含底線 pipeline、unmatched 保留、destination-exists 跳過、idempotent(spec §2)→ Task 2。
- 測試:更新 `test_creates_handlers` + 新 `tests/scripts/test_migrate_logs_layout.py`(spec §3)→ Task 1、Task 2。
- config / 下游不動(spec 非目標)→ 計畫未觸碰,符合。

**Placeholder scan:** 無 TBD/TODO;每個 code step 皆含完整程式碼與確切指令。

**Type consistency:** `Move` NamedTuple(`src` / `dst`)在 script 定義並於測試一致使用(`moves[0].dst`);`plan_moves` 回傳 `(list[Move], list[tuple[Path, str]])` 在測試解構一致;`apply_moves(moves)` 接 `list[Move]` 一致。`_month_from_run_id` / `_RUN_ID_DATE_RE` 命名前後一致。
