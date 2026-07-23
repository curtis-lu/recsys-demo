"""Stage-1 training-time data gates (spec §9 items 9-11; PR-A subset).

Fail-fast with a collect-all error listing EVERY failing group — mirrors the
Layer-1 consistency convention. Thresholds are config (gates.*); set them
loose to effectively disable.
"""

import numpy as np
import pandas as pd


class StagedGateError(Exception):
    """Raised when any stage-1 group fails a data gate."""


def _per_group_stats(labels: pd.Series, y: np.ndarray) -> dict:
    df = pd.DataFrame({"g": labels.to_numpy(), "y": np.asarray(y)})
    agg = df.groupby("g")["y"].agg(["size", "sum"])
    return {
        g: (int(row["size"]), int(row["sum"]))
        for g, row in agg.iterrows()
    }


def check_stage1_gates(
    train: tuple, train_dev: tuple, gates: dict,
) -> dict:
    """Validate per-group trainability; returns train-split stats on success.

    ``train`` / ``train_dev``: (labels: pd.Series, y: np.ndarray) 對。
    Gates: max_groups / min_rows / min_positives / min_negatives —
    min_* 同時套用到 train 與 train_dev 兩個 split（train_dev 是 early-stop
    與 HPO 評分子集，缺類同樣致命）。
    """
    tr_labels, tr_y = train
    dev_labels, dev_y = train_dev
    tr_stats = _per_group_stats(tr_labels, tr_y)
    dev_stats = _per_group_stats(dev_labels, dev_y)

    errors: list[str] = []
    max_groups = int(gates.get("max_groups", 200))
    if len(tr_stats) > max_groups:
        errors.append(
            f"gates.max_groups exceeded: {len(tr_stats)} groups > "
            f"{max_groups} — check partition_keys for a runaway composite"
        )

    min_rows = int(gates.get("min_rows", 0))
    min_pos = int(gates.get("min_positives", 0))
    min_neg = int(gates.get("min_negatives", 0))

    for split_name, stats in (("train", tr_stats), ("train_dev", dev_stats)):
        for g, (n, n_pos) in sorted(stats.items()):
            n_neg = n - n_pos
            problems = []
            if n < min_rows:
                problems.append(f"rows={n}<{min_rows}")
            if n_pos < min_pos:
                problems.append(f"positives={n_pos}<{min_pos}")
            if n_neg < min_neg:
                problems.append(f"negatives={n_neg}<{min_neg}")
            if problems:
                errors.append(
                    f"group {g!r} fails in {split_name}: " + ", ".join(problems)
                )

    orphans = sorted(set(dev_stats) - set(tr_stats))
    if orphans:
        errors.append(
            f"group(s) present only in train_dev (no training data): "
            + ", ".join(repr(g) for g in orphans)
        )

    if errors:
        raise StagedGateError(
            f"stage-1 data gates failed ({len(errors)} issue(s)):\n- "
            + "\n- ".join(errors)
        )
    return tr_stats
