# HPO 崩潰復原 + 接續搜尋 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 `tune_hyperparameters` 在 HPO 進行中 crash 後，重跑只補跑剩餘 trial、零重訓直接拿回最佳模型。

**Architecture:** Optuna `JournalStorage`（接續 trial 歷史）＋ 每次刷新最佳就原子 checkpoint 模型/meta，兩者落在 `data/models/_hpo/<search_id>/`。`search_id` = `model_version` 的 model-defining payload 去掉 `n_trials`，故「改 n_trials」可接續/延長、「改搜尋定義」自動開新 study。自動接續 + 顯著 log，`--fresh-hpo` 強制重來。只改 `tune_hyperparameters` 內部，節點輸出與下游不變。

**Tech Stack:** Python 3.10, Optuna 4.5.0（`optuna.storages.journal`）, LightGBM 4.6.0, pytest 7.3.1。

### 執行環境（worktree）

所有指令在此 worktree 進行。先設好：

```bash
export WT=/Users/curtislu/projects/recsys_tfb/.worktrees/hpo-resume
export PYBIN=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
cd "$WT"
# 測試一律：PYTHONPATH=$WT/src $PYBIN -m pytest <paths> -q
# 跨 worktree git 一律：git -C "$WT" ...
```

參考設計：`docs/superpowers/specs/2026-06-11-hpo-resume-checkpoint-design.md`。

### 檔案結構

- **新增** `src/recsys_tfb/pipelines/training/hpo_resume.py` — study 生命週期 + checkpoint 讀寫（隔離機制、聚焦單測）。
- **修改** `src/recsys_tfb/core/versioning.py` — 新增 `compute_search_id`（hash 單一真實來源）。
- **修改** `src/recsys_tfb/pipelines/training/nodes.py` — 重接 `tune_hyperparameters` 使用上述機制。
- **修改** `src/recsys_tfb/__main__.py` — `training` 加 `--fresh-hpo`、注入 `search_id` / `_fresh_hpo`。
- **修改** `conf/base/parameters_training.yaml` — 頂層 `hpo_checkpointing: true`（不動 `training:`，不 churn model_version）。
- **新增** `docs/operations/hpo-resume.md` — `_hpo/` 目錄存在與清理說明。
- **測試**：`tests/test_core/test_versioning.py`（改）、`tests/test_pipelines/test_training/test_hpo_resume.py`（新）、`tests/test_pipelines/test_training/test_nodes.py`（改）、`tests/test_cli.py`（改）。

---

## Task 1: `compute_search_id`（versioning）

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py`（於 `compute_model_version` 之後，約 line 172）
- Test: `tests/test_core/test_versioning.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_core/test_versioning.py` 的 import 區把 `compute_search_id` 加入既有
`from recsys_tfb.core.versioning import (...)`，並在檔尾新增：

```python
def _tp() -> dict:
    """Minimal model-defining training params for search_id tests."""
    return {
        "training": {
            "algorithm": "lightgbm",
            "algorithm_params": {
                "objective": "binary", "metric": "binary_logloss", "verbosity": -1,
            },
            "n_trials": 20,
            "num_iterations": 500,
            "early_stopping_rounds": 50,
            "search_space": [
                {"name": "learning_rate", "type": "float", "low": 0.01, "high": 0.3},
            ],
        },
    }


class TestComputeSearchId:
    def test_returns_8_char_hex(self):
        sid = compute_search_id(_tp(), "baseV", "trainV")
        assert len(sid) == 8 and all(c in "0123456789abcdef" for c in sid)

    def test_deterministic(self):
        assert compute_search_id(_tp(), "b", "t") == compute_search_id(_tp(), "b", "t")

    def test_n_trials_does_not_affect(self):
        p1 = _tp()
        p2 = _tp(); p2["training"]["n_trials"] = 30
        assert compute_search_id(p1, "b", "t") == compute_search_id(p2, "b", "t")

    def test_num_iterations_affects(self):
        p1 = _tp()
        p2 = _tp(); p2["training"]["num_iterations"] = 1000
        assert compute_search_id(p1, "b", "t") != compute_search_id(p2, "b", "t")

    def test_early_stopping_affects(self):
        p1 = _tp()
        p2 = _tp(); p2["training"]["early_stopping_rounds"] = 100
        assert compute_search_id(p1, "b", "t") != compute_search_id(p2, "b", "t")

    def test_search_space_affects(self):
        p1 = _tp()
        p2 = _tp(); p2["training"]["search_space"][0]["high"] = 0.5
        assert compute_search_id(p1, "b", "t") != compute_search_id(p2, "b", "t")

    def test_base_dataset_version_affects(self):
        assert compute_search_id(_tp(), "b1", "t") != compute_search_id(_tp(), "b2", "t")

    def test_verbosity_does_not_affect(self):
        p1 = _tp()
        p2 = _tp(); p2["training"]["algorithm_params"]["verbosity"] = 1
        assert compute_search_id(p1, "b", "t") == compute_search_id(p2, "b", "t")

    def test_calibration_variant_affects_when_present(self):
        a = compute_search_id(_tp(), "b", "t", "cal1")
        b = compute_search_id(_tp(), "b", "t", "cal2")
        assert a != b
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_core/test_versioning.py::TestComputeSearchId -q`
Expected: FAIL（`ImportError: cannot import name 'compute_search_id'`）

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/core/versioning.py` 的 `compute_model_version` 之後新增：

```python
def compute_search_id(
    params: dict,
    base_dataset_version: str = "",
    train_variant_id: str = "",
    calibration_variant_id: str | None = None,
) -> str:
    """HPO 搜尋身分：與 model_version 相同的 model-defining 輸入，唯一拿掉 n_trials。

    Keys the resumable Optuna study + best-model checkpoint. 只改 trial 數量
    (n_trials) → search_id 不變 → 可接續/延長；改任何會改變一個 trial 的
    (params -> score) 意義者（search_space / hpo_objective / num_iterations /
    early_stopping_rounds / algorithm_params / 資料 / variant 身分）→ search_id
    變 → 自動開新 study。
    """
    payload = _model_version_payload(params)  # deep-copies; safe to mutate
    training = payload.get("training")
    if isinstance(training, dict):
        training.pop("n_trials", None)
    canonical = yaml.dump(payload, sort_keys=True, default_flow_style=False)
    parts = ["search_id|", canonical, base_dataset_version, train_variant_id]
    if calibration_variant_id is not None:
        parts.append(calibration_variant_id)
    return hashlib.sha256("".join(parts).encode()).hexdigest()[:8]
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_core/test_versioning.py::TestComputeSearchId -q`
Expected: PASS（9 passed）

- [ ] **Step 5: Commit**

```bash
git -C "$WT" add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git -C "$WT" commit -m "feat(versioning): compute_search_id（model_version payload 去掉 n_trials）"
```

---

## Task 2: hpo_resume — study 生命週期

**Files:**
- Create: `src/recsys_tfb/pipelines/training/hpo_resume.py`
- Test: `tests/test_pipelines/test_training/test_hpo_resume.py`

- [ ] **Step 1: 寫失敗測試**

Create `tests/test_pipelines/test_training/test_hpo_resume.py`:

```python
"""Tests for hpo_resume: persistent study lifecycle + checkpoint."""

import optuna

from recsys_tfb.pipelines.training import hpo_resume


def _obj(trial):
    return trial.suggest_float("x", 0.0, 1.0)


class TestStudyLifecycle:
    def test_open_study_creates_journal_and_counts(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        study = hpo_resume.open_study(sd, "sid", seed=42)
        study.optimize(_obj, n_trials=1)
        assert (sd / hpo_resume.JOURNAL).exists()
        assert hpo_resume.count_completed(study) == 1

    def test_reload_sees_prior_trials(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        s1 = hpo_resume.open_study(sd, "sid", seed=42)
        s1.optimize(_obj, n_trials=2)
        del s1  # simulate crash: drop the in-memory study
        s2 = hpo_resume.open_study(sd, "sid", seed=42)
        assert hpo_resume.count_completed(s2) == 2

    def test_clear_study_dir(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        hpo_resume.open_study(sd, "sid", seed=42).optimize(_obj, n_trials=1)
        assert sd.exists()
        hpo_resume.clear_study_dir(sd)
        assert not sd.exists()
        hpo_resume.clear_study_dir(sd)  # no error on missing

    def test_hpo_study_dir_path(self):
        from pathlib import Path
        assert hpo_resume.hpo_study_dir("abc") == Path("data") / "models" / "_hpo" / "abc"
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_training/test_hpo_resume.py -q`
Expected: FAIL（`ModuleNotFoundError: ...hpo_resume`）

- [ ] **Step 3: 實作**

Create `src/recsys_tfb/pipelines/training/hpo_resume.py`:

```python
"""Persistent HPO study + best-model checkpoint for crash-resumable tuning.

Keyed by ``search_id`` (recsys_tfb.core.versioning.compute_search_id) so a
crashed HPO run resumes only the remaining trials, and bumping
``training.n_trials`` extends the same search. Storage + checkpoint live under
``data/models/_hpo/<search_id>/`` (driver-local; same persistence guarantee as
the model_version artifacts).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import optuna

logger = logging.getLogger(__name__)

JOURNAL = "study_journal.log"
CHECKPOINT_MODEL = "model.txt"
CHECKPOINT_META = "best_meta.json"


def hpo_study_dir(search_id: str) -> Path:
    """data/models/_hpo/<search_id>/ (relative; mirrors diagnostics_dir 慣例)."""
    return Path("data") / "models" / "_hpo" / str(search_id)


def open_study(study_dir: Path, search_id: str, seed: int) -> optuna.Study:
    """Open (or create) the persistent maximize study for this search_id."""
    study_dir.mkdir(parents=True, exist_ok=True)
    backend = optuna.storages.journal.JournalFileBackend(str(study_dir / JOURNAL))
    storage = optuna.storages.journal.JournalStorage(backend)
    return optuna.create_study(
        storage=storage,
        study_name=search_id,
        load_if_exists=True,
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed),
    )


def count_completed(study: optuna.Study) -> int:
    """Number of COMPLETE trials already recorded."""
    return sum(
        1 for t in study.get_trials(deepcopy=False)
        if t.state == optuna.trial.TrialState.COMPLETE
    )


def clear_study_dir(study_dir: Path) -> None:
    """Remove the study_dir subtree (--fresh-hpo). No-op if absent."""
    if study_dir.exists():
        shutil.rmtree(study_dir, ignore_errors=True)
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_training/test_hpo_resume.py -q`
Expected: PASS（4 passed）

- [ ] **Step 5: Commit**

```bash
git -C "$WT" add src/recsys_tfb/pipelines/training/hpo_resume.py tests/test_pipelines/test_training/test_hpo_resume.py
git -C "$WT" commit -m "feat(hpo_resume): 持久化 study 生命週期（open/count/clear）"
```

---

## Task 3: hpo_resume — checkpoint 讀寫

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/hpo_resume.py`
- Test: `tests/test_pipelines/test_training/test_hpo_resume.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_pipelines/test_training/test_hpo_resume.py` 檔尾新增：

```python
def _tiny_adapter():
    import numpy as np
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    rng = np.random.RandomState(0)
    X = rng.rand(40, 3)
    y = (rng.rand(40) < 0.3).astype(float)
    a = LightGBMAdapter()
    a.train(
        X_train=X, y_train=y, X_val=X, y_val=y,
        params={"objective": "binary", "verbosity": -1, "num_iterations": 5},
    )
    return a


class TestCheckpoint:
    def test_round_trip(self, tmp_path):
        import numpy as np
        sd = tmp_path / "_hpo" / "sid"
        hpo_resume.write_checkpoint(
            sd, _tiny_adapter(), score=0.5, best_iteration=3,
            best_params={"learning_rate": 0.1}, trial_number=2, search_id="sid",
        )
        loaded = hpo_resume.load_checkpoint(sd, "lightgbm")
        assert loaded is not None
        assert loaded["score"] == 0.5
        assert loaded["iteration"] == 3          # from meta, not reloaded booster
        assert loaded["params"] == {"learning_rate": 0.1}
        assert loaded["trial_number"] == 2
        preds = loaded["model"].predict(np.random.RandomState(1).rand(5, 3))
        assert preds.shape == (5,)

    def test_load_missing_returns_none(self, tmp_path):
        assert hpo_resume.load_checkpoint(tmp_path / "nope", "lightgbm") is None

    def test_no_temp_files_left(self, tmp_path):
        sd = tmp_path / "_hpo" / "sid"
        hpo_resume.write_checkpoint(
            sd, _tiny_adapter(), score=0.1, best_iteration=1,
            best_params={}, trial_number=0, search_id="sid",
        )
        leftovers = list((sd / "checkpoint").glob("*.tmp"))
        assert leftovers == []
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_training/test_hpo_resume.py::TestCheckpoint -q`
Expected: FAIL（`AttributeError: module ... has no attribute 'write_checkpoint'`）

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/pipelines/training/hpo_resume.py` 檔尾新增：

```python
def write_checkpoint(
    study_dir: Path,
    adapter,
    *,
    score: float,
    best_iteration: int,
    best_params: dict,
    trial_number: int,
    search_id: str,
) -> None:
    """Atomically persist current best adapter + meta under study_dir/checkpoint/."""
    ckpt = study_dir / "checkpoint"
    ckpt.mkdir(parents=True, exist_ok=True)

    tmp_model = ckpt / (CHECKPOINT_MODEL + ".tmp")
    adapter.save(str(tmp_model))
    os.replace(tmp_model, ckpt / CHECKPOINT_MODEL)

    meta = {
        "score": float(score),
        "best_iteration": int(best_iteration),
        "best_params": best_params,
        "trial_number": int(trial_number),
        "search_id": search_id,
    }
    fd, tmp_meta = tempfile.mkstemp(dir=str(ckpt), suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False, default=str)
    os.replace(tmp_meta, ckpt / CHECKPOINT_META)


def load_checkpoint(study_dir: Path, algorithm: str) -> Optional[dict]:
    """Load best-so-far checkpoint; None if absent/unreadable.

    Returns {score, iteration, params, trial_number, model(ModelAdapter)}.
    ``iteration`` 取自 meta（重載的 LightGBM booster 不保證保留 best_iteration）。
    """
    from recsys_tfb.models.base import get_adapter

    ckpt = study_dir / "checkpoint"
    meta_path = ckpt / CHECKPOINT_META
    model_path = ckpt / CHECKPOINT_MODEL
    if not (meta_path.exists() and model_path.exists()):
        return None
    try:
        with open(meta_path) as f:
            meta = json.load(f)
        adapter = get_adapter(algorithm)
        adapter.load(str(model_path))
    except Exception:
        logger.warning("HPO checkpoint unreadable at %s; ignoring", ckpt, exc_info=True)
        return None
    return {
        "score": float(meta["score"]),
        "iteration": int(meta["best_iteration"]),
        "params": meta.get("best_params", {}),
        "trial_number": int(meta.get("trial_number", -1)),
        "model": adapter,
    }
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_training/test_hpo_resume.py -q`
Expected: PASS（7 passed）

- [ ] **Step 5: Commit**

```bash
git -C "$WT" add src/recsys_tfb/pipelines/training/hpo_resume.py tests/test_pipelines/test_training/test_hpo_resume.py
git -C "$WT" commit -m "feat(hpo_resume): 原子 checkpoint 讀寫（iteration 以 meta 為準）"
```

---

## Task 4: 重接 `tune_hyperparameters`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`（新增 `_resolve_search_id`；改 `tune_hyperparameters` body）
- Test: `tests/test_pipelines/test_training/test_nodes.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_pipelines/test_training/test_nodes.py`：
(1) 確認頂部已 `import logging` 與 `from pathlib import Path`（若無 `Path` 則加 `from pathlib import Path`）。
(2) 在 `# ---- Fixtures ----` 區塊下方新增 autouse cwd 隔離 fixture：

```python
@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """tune_hyperparameters 會寫 ./data/models/_hpo/<sid>/；隔離到各測試自己的 tmp cwd。"""
    monkeypatch.chdir(tmp_path)
```

(3) 把既有 `test_reproducible` 改成走純記憶體路徑（測 sampler 決定論本意）：

```python
    def test_reproducible(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        params = {**training_parameters, "hpo_checkpointing": False}
        p1, i1, _ = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params)
        p2, i2, _ = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params)
        assert p1 == p2
        assert i1 == i2
```

(4) 在 `class TestTuneHyperparameters` 末尾新增復原測試：

```python
    def test_resume_only_runs_remaining(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training import hpo_resume
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]

        def run(n):
            p = {
                **training_parameters, "search_id": "resumesid",
                "training": {**training_parameters["training"], "n_trials": n},
            }
            return tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, p
            )

        run(2)
        sd = hpo_resume.hpo_study_dir("resumesid")
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "resumesid", 42)) == 2
        run(4)  # n_trials 不在 search_id 內 → 同一 study → 只補 2 個
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "resumesid", 42)) == 4

    def test_fresh_hpo_clears_and_logs_discard(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        from recsys_tfb.pipelines.training import hpo_resume
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        base = {
            **training_parameters, "search_id": "freshsid",
            "training": {**training_parameters["training"], "n_trials": 2},
        }
        tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, base)
        sd = hpo_resume.hpo_study_dir("freshsid")
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "freshsid", 42)) == 2

        with caplog.at_level(logging.WARNING):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata,
                {**base, "_fresh_hpo": True},
            )
        assert any(
            "--fresh-hpo" in r.getMessage() and "discarding 2" in r.getMessage()
            for r in caplog.records
        )
        # cleared then re-ran 2 → still 2 (not accumulated to 4)
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "freshsid", 42)) == 2

    def test_checkpointing_disabled_writes_no_files(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        p = {
            **training_parameters, "search_id": "nocp", "hpo_checkpointing": False,
            "training": {**training_parameters["training"], "n_trials": 2},
        }
        _, _, bm = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, p
        )
        assert bm is not None
        assert not (Path("data") / "models" / "_hpo" / "nocp").exists()
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters -q`
Expected: FAIL（`test_resume_only_runs_remaining` 等：節點尚未持久化，第二次 run 會重跑全部而非補跑）

- [ ] **Step 3: 實作 — 新增 `_resolve_search_id`**

在 `src/recsys_tfb/pipelines/training/nodes.py` 的 `HPO_OBJECTIVES = (...)` 之前新增：

```python
def _resolve_search_id(parameters: dict) -> str:
    """HPO search_id：production 由 __main__ 注入；單測/直呼則就地計算。"""
    sid = parameters.get("search_id")
    if sid:
        return str(sid)
    from recsys_tfb.core.versioning import compute_search_id

    cvi = parameters.get("calibration_variant_id")
    if not isinstance(cvi, str) or cvi.startswith("__"):  # "__none__" placeholder
        cvi = None
    return compute_search_id(
        parameters,
        str(parameters.get("base_dataset_version", "")),
        str(parameters.get("train_variant_id", "")),
        cvi,
    )
```

- [ ] **Step 4: 實作 — 改 `tune_hyperparameters` body**

把 `tune_hyperparameters` 內**從 `best_state: dict = {"score": -1.0, ...}` 那行（約 line 432）到函式結尾 `return best_params, best_iteration, best_model`（約 line 512）整段**替換為：

```python
    from recsys_tfb.pipelines.training import hpo_resume

    checkpointing = parameters.get("hpo_checkpointing", True)
    search_id = _resolve_search_id(parameters)
    study_dir = None  # set in the checkpointing branch; referenced by objective

    best_state: dict = {"score": -1.0, "model": None, "iteration": 0, "params": {}}

    def objective(trial: optuna.Trial) -> float:
        trial_idx = trial.number
        trial_params = build_trial_params(trial, search_space)

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        logger.info(
            "tune_hyperparameters: trial=%d/%d start params=%s",
            trial_idx, n_trials, trial_params,
        )
        t0 = time.monotonic()

        adapter = get_adapter(algorithm)
        construct_params = {"feature_pre_filter": False}
        with log_step(logger, "prepare_datasets"):
            ds_train = train_lgb_handle.load(params=construct_params).construct()
            ds_dev = train_dev_lgb_handle.load(
                reference=ds_train, params=construct_params
            ).construct()
        log_data_volume(logger, "tune.ds_train", ds_train)
        log_data_volume(logger, "tune.ds_dev", ds_dev)

        with log_step(logger, "train"):
            adapter.train(
                X_train=None, y_train=None, X_val=None, y_val=None,
                params=params,
                train_dataset=ds_train, val_dataset=ds_dev,
            )

        with log_step(logger, "predict"):
            y_pred = adapter.predict(X_v)

        with log_step(logger, "score"):
            score = _hpo_score(hpo_objective, groups_v, items_v, y_v, y_pred)

        if score > best_state["score"]:
            best_state["score"] = score
            best_state["model"] = adapter
            best_state["iteration"] = adapter.booster.best_iteration
            best_state["params"] = trial_params
            if checkpointing and study_dir is not None:
                hpo_resume.write_checkpoint(
                    study_dir, adapter,
                    score=score, best_iteration=adapter.booster.best_iteration,
                    best_params=trial_params, trial_number=trial_idx,
                    search_id=search_id,
                )

        duration = time.monotonic() - t0
        logger.info(
            "tune_hyperparameters: trial=%d/%d completed score=%.4f "
            "best_iteration=%d duration=%.1fs best_so_far=%.4f",
            trial_idx, n_trials, score,
            adapter.booster.best_iteration, duration, best_state["score"],
        )

        return score

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    if checkpointing:
        study_dir = hpo_resume.hpo_study_dir(search_id)
        if parameters.get("_fresh_hpo", False):
            n_prev, prev_best = 0, float("nan")
            if study_dir.exists():
                try:
                    _tmp = hpo_resume.open_study(study_dir, search_id, seed)
                    n_prev = hpo_resume.count_completed(_tmp)
                    prev_best = _tmp.best_value if n_prev else float("nan")
                except Exception:  # pragma: no cover - defensive
                    pass
            logger.warning(
                "--fresh-hpo: clearing %s (discarding %d completed trial(s), prev best=%.4f)",
                study_dir, n_prev, prev_best,
            )
            hpo_resume.clear_study_dir(study_dir)

        study = hpo_resume.open_study(study_dir, search_id, seed)
        done = hpo_resume.count_completed(study)
        ckpt = hpo_resume.load_checkpoint(study_dir, algorithm)
        if ckpt is not None:
            best_state.update(
                score=ckpt["score"], model=ckpt["model"],
                iteration=ckpt["iteration"], params=ckpt["params"],
            )
            logger.info(
                "HPO resume: %d completed trial(s) found; best so far score=%.4f "
                "(trial #%d); running %d more (target=%d)",
                done, ckpt["score"], ckpt["trial_number"],
                max(0, n_trials - done), n_trials,
            )
        remaining = max(0, n_trials - done)
    else:
        study = optuna.create_study(
            direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed)
        )
        remaining = n_trials

    if remaining > 0:
        with log_step(logger, "optuna_optimize"):
            study.optimize(objective, n_trials=remaining)
    else:
        logger.info("HPO target already met (done>=%d); skipping optimize", n_trials)

    # last-resort: study has trials but no usable checkpoint model — refit best_params once.
    if best_state["model"] is None:
        logger.warning(
            "No usable best model from memory/checkpoint; "
            "refitting study.best_params once (last-resort recovery)"
        )
        study.enqueue_trial(study.best_params)
        with log_step(logger, "last_resort_refit"):
            study.optimize(objective, n_trials=1)

    best_params = best_state["params"] or study.best_params
    best_model = best_state["model"]
    best_iteration = best_state["iteration"]
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, best_state["score"], best_iteration, best_params,
    )
    return best_params, best_iteration, best_model
```

> 注意：`build_trial_params` 已在函式上方 `from ... import build_trial_params` 匯入（既有），保持不變。

- [ ] **Step 5: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_training/test_nodes.py -q`
Expected: PASS（含既有 + 4 新測試全綠）

- [ ] **Step 6: Commit**

```bash
git -C "$WT" add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git -C "$WT" commit -m "feat(training): tune_hyperparameters 持久化 study + checkpoint 自動接續"
```

---

## Task 5: `__main__` 注入 search_id + `--fresh-hpo`

**Files:**
- Modify: `src/recsys_tfb/__main__.py`（`training` 指令）
- Test: `tests/test_cli.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_cli.py` 檔尾新增：

```python
class TestFreshHpoFlag:
    def test_training_help_advertises_fresh_hpo(self):
        result = runner.invoke(app, ["training", "--help"])
        assert result.exit_code == 0
        assert "--fresh-hpo" in result.output
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_cli.py::TestFreshHpoFlag -q`
Expected: FAIL（`--fresh-hpo` 不在 help 輸出）

- [ ] **Step 3: 實作**

(1) 在 `src/recsys_tfb/__main__.py` 匯入 `compute_search_id`：找到既有
`from recsys_tfb.core.versioning import (...)`（含 `compute_model_version`），把 `compute_search_id` 加入。

(2) 在 `def training(...)` 參數列的 `only_node` 之後、`dry_run` 之前新增選項：

```python
    fresh_hpo: bool = typer.Option(
        False, "--fresh-hpo",
        help="丟棄此 search_id 已累積的 HPO study/checkpoint，從 trial 0 重新搜尋",
    ),
```

(3) 在 `mv = compute_model_version(params_training, base_v, train_v, cal_v)` 之後新增：

```python
    sid = compute_search_id(params_training, base_v, train_v, cal_v)
    logger.info("search_id: %s", sid)
```

(4) 在 `runtime_params = { ... "snap_date": _NONE_PLACEHOLDER, }` 字典內補兩個鍵：

```python
        "search_id": sid,
        "_fresh_hpo": fresh_hpo,
```

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_cli.py::TestFreshHpoFlag -q`
Expected: PASS（1 passed）

- [ ] **Step 5: Commit**

```bash
git -C "$WT" add src/recsys_tfb/__main__.py tests/test_cli.py
git -C "$WT" commit -m "feat(cli): training 注入 search_id + --fresh-hpo flag"
```

---

## Task 6: config 預設鍵 + 文件

**Files:**
- Modify: `conf/base/parameters_training.yaml`
- Create: `docs/operations/hpo-resume.md`
- Test: `tests/test_cli.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_cli.py` 的 `TestFreshHpoFlag` 之後新增：

```python
class TestHpoCheckpointingConfig:
    def test_parameters_training_declares_hpo_checkpointing_true(self):
        import yaml as _yaml
        with open("conf/base/parameters_training.yaml") as f:
            cfg = _yaml.safe_load(f)
        assert cfg.get("hpo_checkpointing") is True
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_cli.py::TestHpoCheckpointingConfig -q`
Expected: FAIL（鍵不存在 → `None is True` 失敗）

- [ ] **Step 3: 實作 — config**

在 `conf/base/parameters_training.yaml` 頂層（與 `mlflow:` / `cache:` 同層，不在 `training:` 內 —— 放 `training:` 會 churn model_version）新增：

```yaml
# HPO 崩潰復原開關（ops 旗標，刻意放頂層不影響 model_version）。
# true（預設）：tune_hyperparameters 持久化 Optuna study + 每次刷新最佳 checkpoint
# 模型到 data/models/_hpo/<search_id>/，crash 後重跑只補跑剩餘 trial。
# false：退回純記憶體、不落地（debug 用）。詳見 docs/operations/hpo-resume.md。
hpo_checkpointing: true
```

- [ ] **Step 4: 實作 — 文件**

Create `docs/operations/hpo-resume.md`:

```markdown
# HPO 崩潰復原 + 接續搜尋

`training` pipeline 的 `tune_hyperparameters` 預設持久化 HPO 狀態，crash 後重跑只補跑剩餘
trial、零重訓直接拿回最佳模型。

## 機制

- Optuna study 用 `JournalStorage` 落地 `data/models/_hpo/<search_id>/study_journal.log`；
  每次刷新最佳就原子寫 `checkpoint/model.txt` + `checkpoint/best_meta.json`。
- `search_id` = `model_version` 的 model-defining 輸入去掉 `n_trials`。故：
  - 改 `search_space` / `hpo_objective` / `num_iterations` / `early_stopping_rounds` / 資料身分
    → search_id 變 → 自動開新 study。
  - 只改 `n_trials` → search_id 不變 → 接續（同值補跑）或延長（調高補跑差額）。
- config 的 `n_trials` 當「目標總數」：接續只跑 `max(0, n_trials − 已完成 trial 數)`。

## 操作

- **自動接續**：crash 後用同一份 config 重跑 `training` 即可，log 會印
  `HPO resume: N completed trial(s) found ... running M more`。
- **延長搜尋**：把 `training.n_trials` 調高再跑（最佳模型落新的 model_version 目錄，但共用
  同一 search_id study）。
- **強制重來**：`python -m recsys_tfb training --fresh-hpo` —— 清除當前 search_id 的
  `_hpo/<search_id>/`、從 trial 0 重搜（log 明列丟棄幾個已完成 trial）。
- **關閉持久化**：頂層 `hpo_checkpointing: false`（純記憶體、不落地）。

## 清理

`data/models/_hpo/<search_id>/` 成功後刻意保留（很小、可稽核、重跑秒收）。它**跨 model_version
共用**、不隨任一 model_version 目錄刪除而連帶清。要清：

- 單一搜尋：`--fresh-hpo`（下次該 search_id 執行時清）或手動刪該子目錄。
- 全部：`rm -rf data/models/_hpo/`。

## 限制

- 接續依賴 `data/models/` 在兩次執行間持續存在（與既有 `--from-node finalize_model` 同一保證）；
  driver 本機碟若被清空則無法接續。
- resumed run 非單次不中斷執行的位元級複製：已完成 trial 完全重用，但 TPE sampler 接續時以同
  seed 重建、RNG 不還原，故剩餘 trial 取樣參數可能與不中斷版不同（結果仍有效）。
- 單行程假設：同一 search_id 不應同時兩個訓練在跑。
```

- [ ] **Step 5: 跑測試確認通過**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_cli.py::TestHpoCheckpointingConfig -q`
Expected: PASS（1 passed）

- [ ] **Step 6: Commit**

```bash
git -C "$WT" add conf/base/parameters_training.yaml docs/operations/hpo-resume.md tests/test_cli.py
git -C "$WT" commit -m "docs(hpo): hpo_checkpointing 預設鍵 + _hpo 目錄/清理說明"
```

---

## Task 7: 整合驗證 + graphify 同步

**Files:** 無（驗證與圖譜維護）

- [ ] **Step 1: 跑全部相關測試**

Run:
```bash
PYTHONPATH=$WT/src $PYBIN -m pytest \
  tests/test_core/test_versioning.py \
  tests/test_pipelines/test_training/test_hpo_resume.py \
  tests/test_pipelines/test_training/test_nodes.py \
  tests/test_cli.py -q
```
Expected: 全綠。（`test_cli.py` 中需 Spark/config 的既有測試若在本機慢，可先只跑前三個檔 + `tests/test_cli.py::TestFreshHpoFlag tests/test_cli.py::TestHpoCheckpointingConfig`。）

- [ ] **Step 2: 確認既有 resume contracts 未被破壞**

Run: `PYTHONPATH=$WT/src $PYBIN -m pytest tests/test_pipelines/test_resume_contracts.py -q`
Expected: PASS（節點輸出契約不變，`--from-node finalize_model` 仍可接續）

- [ ] **Step 3: graphify 圖譜重建（CLAUDE.md 規範）**

Run:
```bash
cd "$WT" && $PYBIN -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: 印出 `Rebuilt: ... nodes ... edges`。

- [ ] **Step 4: Commit 圖譜（若有變動）**

```bash
git -C "$WT" add graphify-out/ 2>/dev/null
git -C "$WT" diff --cached --quiet || git -C "$WT" commit -m "chore(graphify): rebuild after HPO resume"
```

---

## Self-Review（撰寫後檢查，已內嵌修正）

**Spec 覆蓋**：§3 架構/輸出不變→Task 4 +Task 7 Step 2；§4 檔案佈局→Task 2/3；§5 backend→Task 2；
§6 search_id（含 num_iterations/early_stopping 保留）→Task 1；§7 控制流（remaining/last-resort）→Task 4；
§8 checkpoint→Task 3；§9 CLI/config（--fresh-hpo、hpo_checkpointing）→Task 5/6；§9.1 fresh-hpo 行為→Task 4 測試；
§10 向後相容/決定論→Task 4（test_reproducible 走 in-memory）+ 文件；§11 失敗邊界（last-resort/done≥target）→Task 4；
§12 清理生命週期→Task 6 文件；§13 測試→各 Task；§14 環境→Task 7。

**Placeholder 掃描**：無 TBD/TODO；每段含實際程式碼與指令。

**型別一致性**：`compute_search_id`（versioning）↔ `_resolve_search_id`（nodes）↔ `__main__` 注入 `search_id`
三者一致；`load_checkpoint` 回傳鍵 `{score, iteration, params, trial_number, model}` ↔ Task 4
`best_state.update(score=, model=, iteration=, params=)` 一致；`hpo_study_dir` / `open_study` /
`count_completed` / `clear_study_dir` / `write_checkpoint` / `load_checkpoint` 命名跨 Task 2/3/4 一致。
