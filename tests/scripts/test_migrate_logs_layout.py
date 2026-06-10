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
