"""train_staged_model node: stage-1 per-group training orchestration.

Reads the SAME train/train_dev parquet handles as the shared path (spec D9),
slices per-group subsets in memory, runs data gates (fail-fast), then trains
each group (sequential trials inside a group; groups run on a size-aware
thread pool — LightGBM releases the GIL during training). Determinism does
not depend on scheduling: every group's seed derives from (random_seed,
group_key) alone.

Group-level checkpointing (2026-07-24 修訂，先例＝`_hpo/`): each group is
persisted to ``<wip_root>/<slug>/`` (model.txt + meta.json + _SUCCESS) right
after it finishes training. A re-run skips any group whose checkpoint has a
_SUCCESS marker and loads it back instead — safe because model_version
already folds in config + dataset version, and per-group training is
deterministic (D7 seed contract), so "skip" and "retrain" produce byte-
identical results. D7 boundary unchanged: an interrupted per-group HPO
search restarts that whole group's search; the checkpoint granularity is
"group finished", not "trial finished". The wip directory is never
auto-cleaned (small footprint, keyed by model_version; safe to delete by
hand — see plan §修訂 2026-07-24).
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.extract import _pdf_to_X, _row_weights_from_pdf
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.gates import check_stage1_gates
from recsys_tfb.models.staged.partition import group_labels, group_slug
from recsys_tfb.models.staged.train_stage1 import train_one_group

logger = logging.getLogger(__name__)


def _label_col(parameters: dict) -> str:
    return get_schema(parameters)["label"]


def _group_arrays(
    pdf: pd.DataFrame, labels: pd.Series, key: str,
    preprocessor_view: dict, parameters: dict,
):
    """Slice one group's rows out of an already-loaded pdf -> (X, y, w)."""
    sub = pdf.loc[(labels == key).to_numpy()]
    X = _pdf_to_X(sub, preprocessor_view, parameters)
    y = sub[_label_col(parameters)].values
    w = _row_weights_from_pdf(sub, parameters, preprocessor_view)
    return X, y, w


def _wip_dir(parameters: dict, wip_root) -> Path:
    if wip_root is not None:
        return Path(wip_root)
    # model_version 由 runtime_params 併入 parameters（同 catalog 模板機制）。
    # 若執行時發現 parameters 無此鍵，停下回報，不要用 fallback 硬跑。
    mv = parameters.get("model_version", "adhoc")
    return Path("data/models/_staged_wip") / str(mv)


def _load_checkpoint(gdir: Path, key: str):
    if not (gdir / "_SUCCESS").exists():
        return None
    meta = json.loads((gdir / "meta.json").read_text())
    adapter = LightGBMAdapter()
    adapter.load(str(gdir / "model.txt"))
    return key, adapter, meta


def _write_checkpoint(gdir: Path, result) -> None:
    gdir.mkdir(parents=True, exist_ok=True)
    result.adapter.save(str(gdir / "model.txt"))
    (gdir / "meta.json").write_text(json.dumps({
        "best_params": result.best_params, "score": result.score,
        "metric": result.metric, "n_rows": result.n_rows,
        "n_pos": result.n_pos,
        "train_seconds": round(result.train_seconds, 3),
    }, indent=2, ensure_ascii=False))
    (gdir / "_SUCCESS").touch()


def train_staged_model(
    train_parquet_handle,
    train_dev_parquet_handle,
    preprocessor_view: dict,
    parameters: dict,
    wip_root=None,
) -> "tuple[StagedModelAdapter, dict]":
    training = parameters["training"]
    stage1 = training["staged"]["stage1"]
    partition_keys = list(stage1["partition_keys"])
    base_seed = int(parameters.get("random_seed", 42))
    wip = _wip_dir(parameters, wip_root)

    pdf_tr = train_parquet_handle.to_pandas()
    pdf_dev = train_dev_parquet_handle.to_pandas()
    labels_tr = group_labels(pdf_tr, partition_keys)
    labels_dev = group_labels(pdf_dev, partition_keys)
    label_col = _label_col(parameters)

    tr_stats = check_stage1_gates(
        (labels_tr, pdf_tr[label_col].values),
        (labels_dev, pdf_dev[label_col].values),
        stage1.get("gates") or {},
    )
    group_keys = sorted(tr_stats, key=lambda g: -tr_stats[g][0])  # 大群先跑
    logger.info(
        "train_staged_model: %d group(s) by %s, sizes %s",
        len(group_keys), partition_keys,
        {g: tr_stats[g][0] for g in group_keys},
    )

    algorithm_params = {
        **(training.get("algorithm_params") or {}),
        "num_iterations": training.get("num_iterations", 500),
        "early_stopping_rounds": training.get("early_stopping_rounds", 50),
    }
    cat_idx = LightGBMAdapter._categorical_indices(preprocessor_view)

    def _train(key: str):
        X_tr, y_tr, w_tr = _group_arrays(
            pdf_tr, labels_tr, key, preprocessor_view, parameters)
        X_dev, y_dev, w_dev = _group_arrays(
            pdf_dev, labels_dev, key, preprocessor_view, parameters)
        return train_one_group(
            key, X_tr, y_tr, w_tr, X_dev, y_dev, w_dev,
            dict(algorithm_params), dict(stage1.get("params") or {}),
            dict(stage1.get("hpo") or {}), cat_idx, base_seed,
        )

    # 群級 checkpoint：有 _SUCCESS 的群直接載回（確定性保證「載回＝重算同結果」；
    # model_version 涵蓋 config＋dataset 版本，路徑即失效機制）。D7 邊界不變：
    # 群內 search 中斷＝該群整段重搜。
    completed: dict = {}
    todo: list = []
    for k in group_keys:
        loaded = _load_checkpoint(wip / group_slug(k), k)
        if loaded is not None:
            completed[k] = loaded
        else:
            todo.append(k)
    if completed:
        logger.info(
            "train_staged_model: %d/%d group(s) restored from checkpoint %s",
            len(completed), len(group_keys), wip,
        )

    def _train_and_checkpoint(key: str):
        r = _train(key)
        _write_checkpoint(wip / group_slug(key), r)
        return r

    max_workers = max(1, int(stage1.get("max_workers", 1)))
    if max_workers == 1:
        results = [_train_and_checkpoint(k) for k in todo]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(_train_and_checkpoint, todo))

    model = StagedModelAdapter()
    report_groups: dict = {}
    for r in results:
        meta = {"best_params": r.best_params, "score": r.score,
                "metric": r.metric, "n_rows": r.n_rows, "n_pos": r.n_pos,
                "train_seconds": round(r.train_seconds, 3)}
        model.add_group(r.group_key, r.adapter, meta=meta)
        report_groups[r.group_key] = meta
        logger.info(
            "stage1 group %r: rows=%d pos=%d %s=%.5f best_params=%s (%.1fs)",
            r.group_key, r.n_rows, r.n_pos, r.metric, r.score,
            r.best_params, r.train_seconds,
        )
    for key, (k, adapter, meta) in completed.items():
        model.add_group(k, adapter, meta=meta)
        report_groups[k] = meta
    model.set_partition_keys(partition_keys)
    report = {"partition_keys": partition_keys, "groups": report_groups}
    return model, report
