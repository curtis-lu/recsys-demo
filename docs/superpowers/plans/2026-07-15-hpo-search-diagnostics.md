# HPO Search Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 每次 HPO 後自動寫出 Optuna 搜尋過程的稽核基底（`hpo_trials.json`）＋摘要（`hpo_summary.json`，答「要不要再繼續 trial」「search range 要不要調」）＋ 5 張互動 HTML 圖，全程 best-effort，對 resume 契約隱形。

**Architecture:** 新 package `src/recsys_tfb/diagnosis/hpo/`（collect / summary / render / write），由 `tune_hyperparameters` 尾端一段 best-effort `try/except` 呼叫，產物寫進既有 `diagnostics_dir/hpo/`、由終端 `log_experiment` 的 `mlflow.log_artifacts` 自動撿走。不新增 DAG node、不新增 catalog output、不改 `tune_hyperparameters` 的 outputs。

**Tech Stack:** Python 3.10.9、Optuna 4.5.0（`optuna.visualization` plotly backend、`optuna.importance` fANOVA）、plotly 5.17.0、scikit-learn 1.5.0（fANOVA 用；皆已 pinned，無新依賴）。

**Spec:** `docs/superpowers/specs/2026-07-15-hpo-search-diagnostics-design.md`

**環境鐵則（每個 test/CLI 指令都照此，勿裸跑）:**
```
WT=/Users/curtislu/projects/recsys_tfb/.worktrees/hpo-search-diag
PY="PYTHONPATH=$WT/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python"
# 例：$PY -m pytest <paths> -q   ；所有指令以 `cd $WT &&` 開頭或用絕對路徑
```

**驗證過的事實（寫測試斷言的依據，勿再假設）:**
- `write_html` 預設把 ~3.6MB plotly.js inline 進**每個**檔（5 張=18MB）→ 一律 `include_plotlyjs="directory"`：dir 內共用一份 `plotly.min.js`（~3.6MB），各 HTML ~8–10KB，離線可看。
- 只有 1 個搜尋參數時 `plot_contour`/`plot_parallel_coordinate` **不 raise**（畫退化圖）。真正會 raise 的退化＝**完成 trial <2 時 `plot_param_importances`**（內部 `get_param_importances` raise `ValueError`）。
- `get_param_importances`：1 trial → raise；2 trial/1 param → 回 `{'x': 1.0}`。故 `compute_importances` 對 raise 與空 dict 都回 `None`。

---

### Task 1: package 骨架 + 路徑 + 原子寫檔

**Files:**
- Create: `src/recsys_tfb/diagnosis/hpo/__init__.py`
- Create: `src/recsys_tfb/diagnosis/hpo/paths.py`
- Create: `src/recsys_tfb/diagnosis/hpo/_io.py`
- Create: `tests/test_diagnosis/test_hpo/__init__.py`
- Test: `tests/test_diagnosis/test_hpo/test_paths_io.py`

- [ ] **Step 1: 建空 `__init__.py`（暫不 import write，避免循環/未定義）**

`src/recsys_tfb/diagnosis/hpo/__init__.py`:
```python
"""HPO 搜尋診斷：稽核 JSON + 摘要 + 自動圖（best-effort）。入口在 write.py（Task 6 補 export）。"""
```

建空的 `tests/test_diagnosis/test_hpo/__init__.py`（0 bytes）。

- [ ] **Step 2: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_paths_io.py`:
```python
import json

from recsys_tfb.diagnosis.hpo._io import atomic_write_json
from recsys_tfb.diagnosis.hpo.paths import hpo_dir


def test_hpo_dir_under_diagnostics(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = hpo_dir({"model_version": "mv1"})
    assert d.as_posix() == "data/models/mv1/diagnostics/hpo"
    assert d.exists()


def test_atomic_write_json_roundtrip_and_overwrite(tmp_path):
    p = tmp_path / "sub" / "x.json"
    atomic_write_json(p, {"a": 1, "中": "文"})
    assert json.loads(p.read_text()) == {"a": 1, "中": "文"}
    atomic_write_json(p, {"a": 2})  # idempotent overwrite
    assert json.loads(p.read_text()) == {"a": 2}
    assert list((tmp_path / "sub").glob("*.tmp")) == []  # no leftover temp
```

- [ ] **Step 3: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_paths_io.py -q`
Expected: FAIL（`ModuleNotFoundError: recsys_tfb.diagnosis.hpo._io` / `.paths`）

- [ ] **Step 4: 實作 paths + _io**

`src/recsys_tfb/diagnosis/hpo/paths.py`:
```python
"""HPO 搜尋診斷產物路徑。"""

from pathlib import Path

from recsys_tfb.diagnosis.model.paths import diagnostics_dir


def hpo_dir(parameters: dict) -> Path:
    """Resolve（並建立）diagnostics/hpo/ —— HPO 搜尋診斷產物。"""
    d = diagnostics_dir(parameters) / "hpo"
    d.mkdir(parents=True, exist_ok=True)
    return d
```

`src/recsys_tfb/diagnosis/hpo/_io.py`:
```python
"""HPO 診斷 artifact 的原子 JSON 寫入（temp file + os.replace）。"""

import json
import os
import tempfile
from pathlib import Path


def atomic_write_json(path, data: dict) -> None:
    """把 data 以 JSON 原子寫入 path：先寫同目錄 temp 檔再 os.replace。

    覆寫是 idempotent 的；中途崩潰只會留下舊檔或新檔，不會有截斷檔。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(path))
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
```

- [ ] **Step 5: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_paths_io.py -q`
Expected: PASS（2 passed）

- [ ] **Step 6: Commit**

```bash
cd $WT && git add src/recsys_tfb/diagnosis/hpo tests/test_diagnosis/test_hpo && \
  git commit -m "feat(hpo-diag): package skeleton — hpo_dir + atomic_write_json"
```

---

### Task 2: `collect_trials` — 稽核基底 payload

**Files:**
- Create: `src/recsys_tfb/diagnosis/hpo/collect.py`
- Test: `tests/test_diagnosis/test_hpo/test_collect.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_collect.py`:
```python
import optuna

from recsys_tfb.diagnosis.hpo.collect import collect_trials


def _tiny_study(n=4):
    s = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    s.optimize(lambda t: t.suggest_float("x", 0.0, 1.0), n_trials=n)
    return s


def test_collect_trials_schema():
    study = _tiny_study(4)
    ss = [{"name": "x", "type": "float", "low": 0.0, "high": 1.0}]
    payload = collect_trials(
        study, ss, model_version="mv1", search_id="sid",
        hpo_objective="mean_ap", seed=1, n_trials_target=10,
        best_iteration=42, generated_at="2026-07-15T00:00:00",
    )
    assert payload["schema_version"] == 1
    m = payload["meta"]
    assert m["model_version"] == "mv1"
    assert m["search_id"] == "sid"
    assert m["direction"] == "maximize"
    assert m["sampler"] == "TPESampler"
    assert m["n_completed"] == 4
    assert m["n_trials_target"] == 10
    assert m["search_space"] == ss
    assert m["generated_at"] == "2026-07-15T00:00:00"
    assert len(payload["trials"]) == 4
    row = payload["trials"][0]
    assert set(row) == {"number", "value", "state", "params", "duration_s"}
    assert row["state"] == "COMPLETE"
    assert payload["best"]["best_iteration"] == 42
    assert payload["best"]["value"] == study.best_value


def test_collect_trials_no_completed():
    study = optuna.create_study(direction="maximize")  # 0 trials
    payload = collect_trials(
        study, [], model_version="m", search_id="s", hpo_objective="mean_ap",
        seed=1, n_trials_target=5, best_iteration=0, generated_at="t",
    )
    assert payload["meta"]["n_completed"] == 0
    assert payload["best"] is None
    assert payload["trials"] == []
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_collect.py -q`
Expected: FAIL（`ModuleNotFoundError: ...collect`）

- [ ] **Step 3: 實作 collect**

`src/recsys_tfb/diagnosis/hpo/collect.py`:
```python
"""從 Optuna Study 抽出自足的 trial 稽核基底（hpo_trials.json payload）。"""

from __future__ import annotations

import datetime as _dt

import optuna

SCHEMA_VERSION = 1


def _trial_row(t: optuna.trial.FrozenTrial) -> dict:
    return {
        "number": t.number,
        "value": t.value,
        "state": t.state.name,
        "params": dict(t.params),
        "duration_s": t.duration.total_seconds() if t.duration is not None else None,
    }


def collect_trials(
    study: optuna.Study,
    search_space: list,
    *,
    model_version: str,
    search_id: str,
    hpo_objective: str,
    seed: int,
    n_trials_target: int,
    best_iteration: int,
    generated_at: str | None = None,
) -> dict:
    """Build the self-contained hpo_trials.json payload from a live study."""
    completed = [
        t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE
    ]
    if generated_at is None:
        generated_at = _dt.datetime.now().isoformat(timespec="seconds")
    try:
        best = study.best_trial
        best_block = {
            "number": best.number,
            "value": best.value,
            "params": dict(best.params),
            "best_iteration": best_iteration,
        }
    except ValueError:  # no completed trials
        best_block = None
    return {
        "schema_version": SCHEMA_VERSION,
        "meta": {
            "model_version": str(model_version),
            "search_id": str(search_id),
            "hpo_objective": hpo_objective,
            "direction": study.direction.name.lower(),
            "sampler": type(study.sampler).__name__,
            "seed": seed,
            "n_trials_target": n_trials_target,
            "n_completed": len(completed),
            "search_space": search_space,
            "generated_at": generated_at,
        },
        "trials": [_trial_row(t) for t in study.trials],
        "best": best_block,
    }
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_collect.py -q`
Expected: PASS（2 passed）

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/diagnosis/hpo/collect.py tests/test_diagnosis/test_hpo/test_collect.py && \
  git commit -m "feat(hpo-diag): collect_trials audit payload from study"
```

---

### Task 3: `compute_convergence` + `compute_boundary`（純函式，答 Q1/Q2）

**Files:**
- Create: `src/recsys_tfb/diagnosis/hpo/summary.py`
- Test: `tests/test_diagnosis/test_hpo/test_summary.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_summary.py`:
```python
from recsys_tfb.diagnosis.hpo.summary import compute_boundary, compute_convergence


def _t(number, value, state="COMPLETE"):
    return {"number": number, "value": value, "state": state, "params": {}, "duration_s": 1.0}


def test_convergence_plateau_triggers():
    trials = [_t(i, 0.30) for i in range(5)]  # best at #0, never improves
    r = compute_convergence(trials, patience=3)
    assert r["plateau"] is True
    assert r["best_trial_number"] == 0
    assert r["trials_since_improvement"] == 4


def test_convergence_not_plateau():
    trials = [_t(0, 0.30), _t(1, 0.31), _t(2, 0.35)]  # best last
    r = compute_convergence(trials, patience=3)
    assert r["plateau"] is False
    assert r["best_trial_number"] == 2
    assert r["trials_since_improvement"] == 0


def test_convergence_ignores_incomplete():
    trials = [_t(0, 0.30), _t(1, None, state="FAIL"), _t(2, 0.40)]
    r = compute_convergence(trials, patience=1)
    assert r["n_completed"] == 2
    assert r["best_value"] == 0.40


def test_convergence_empty():
    r = compute_convergence([], patience=3)
    assert r["n_completed"] == 0
    assert r["plateau"] is False
    assert r["best_value"] is None


def test_boundary_widen_high_linear_int():
    ss = [{"name": "num_leaves", "type": "int", "low": 20, "high": 100}]
    r = compute_boundary({"num_leaves": 99}, ss, hi_thresh=0.98, lo_thresh=0.02)
    b = r["num_leaves"]
    assert b["suggestion"] == "widen_high"
    assert b["at_high"] is True


def test_boundary_widen_low_log_scale():
    ss = [{"name": "lr", "type": "float", "low": 1e-3, "high": 1e-1, "log": True}]
    r = compute_boundary({"lr": 1.05e-3}, ss, hi_thresh=0.98, lo_thresh=0.02)
    b = r["lr"]
    assert b["suggestion"] == "widen_low"
    assert b["scale"] == "log"


def test_boundary_ok_middle():
    ss = [{"name": "num_leaves", "type": "int", "low": 20, "high": 100}]
    r = compute_boundary({"num_leaves": 60}, ss, hi_thresh=0.98, lo_thresh=0.02)
    assert r["num_leaves"]["suggestion"] == "ok"


def test_boundary_categorical_no_suggestion():
    ss = [{"name": "bt", "type": "categorical", "choices": ["gbdt", "dart"]}]
    r = compute_boundary({"bt": "dart"}, ss, hi_thresh=0.98, lo_thresh=0.02)
    assert r["bt"]["suggestion"] == "n/a"
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_summary.py -q`
Expected: FAIL（`ImportError: cannot import name 'compute_convergence'`）

- [ ] **Step 3: 實作 summary（convergence + boundary）**

`src/recsys_tfb/diagnosis/hpo/summary.py`:
```python
"""從 trials + search_space 算 convergence / boundary / importances 摘要。"""

from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


def compute_convergence(trials: list[dict], *, patience: int) -> dict:
    """答「需不需要再繼續 trial」。只看 COMPLETE 且 value 非 None 的 trial：
    找最後一次刷新最佳（maximize）的 trial，算距今幾個完成 trial 未再進步；
    plateau = 未進步數 >= patience。"""
    completed = [
        t for t in trials
        if t.get("state") == "COMPLETE" and t.get("value") is not None
    ]
    if not completed:
        return {
            "best_value": None, "best_trial_number": None, "n_completed": 0,
            "last_improvement_trial": None, "trials_since_improvement": None,
            "plateau": False, "note": "尚無完成的 trial。",
        }
    best_val = None
    best_num = None
    last_improve_idx = 0
    for idx, t in enumerate(completed):
        if best_val is None or t["value"] > best_val:
            best_val, best_num, last_improve_idx = t["value"], t["number"], idx
    since = len(completed) - 1 - last_improve_idx
    plateau = since >= patience
    note = (
        f"近 {since} 個完成的 trial 未再刷新最佳；已達 plateau 提示閾值"
        f"（patience={patience}），可考慮停止。"
        if plateau else
        f"最佳在第 {best_num} 號 trial；距今 {since} 個未進步，未達 plateau"
        f"閾值（patience={patience}），可能還有空間。"
    )
    return {
        "best_value": best_val, "best_trial_number": best_num,
        "n_completed": len(completed), "last_improvement_trial": best_num,
        "trials_since_improvement": since, "plateau": plateau, "note": note,
    }


def _rel_position(value, low, high, log: bool) -> Optional[float]:
    try:
        lo, hi, v = float(low), float(high), float(value)
    except (TypeError, ValueError):
        return None
    if log:
        if lo <= 0 or hi <= 0 or v <= 0 or hi == lo:
            return None
        return (math.log(v) - math.log(lo)) / (math.log(hi) - math.log(lo))
    if hi == lo:
        return None
    return (v - lo) / (hi - lo)


def compute_boundary(
    best_params: dict, search_space: list, *, hi_thresh: float, lo_thresh: float
) -> dict:
    """答「search range 要不要調」。對每個數值型搜尋參數，看最佳值離 search_space 邊界多近。"""
    best_params = best_params or {}
    out: dict = {}
    for spec in search_space:
        name = spec["name"]
        ptype = spec.get("type")
        if name not in best_params:
            continue
        if ptype not in ("int", "float"):
            out[name] = {"type": ptype, "suggestion": "n/a",
                         "note": "categorical，不做邊界建議。"}
            continue
        value = best_params[name]
        low, high = spec.get("low"), spec.get("high")
        log = bool(spec.get("log", False))
        rel = _rel_position(value, low, high, log)
        scale = "log" if log else ptype
        if rel is None:
            out[name] = {"best_value": value, "low": low, "high": high,
                         "scale": scale, "rel_position": None,
                         "at_low": False, "at_high": False, "suggestion": "ok",
                         "note": "無法計算相對位置（範圍退化），略過建議。"}
            continue
        at_high = rel >= hi_thresh
        at_low = rel <= lo_thresh
        if at_high:
            suggestion = "widen_high"
            note = f"最佳值貼近上界（相對位置 {rel:.3f}），建議放寬上界。"
        elif at_low:
            suggestion = "widen_low"
            note = f"最佳值貼近下界（相對位置 {rel:.3f}），建議放寬下界。"
        else:
            suggestion = "ok"
            note = f"最佳值在範圍內（相對位置 {rel:.3f}），範圍看似足夠。"
        out[name] = {"best_value": value, "low": low, "high": high,
                     "scale": scale, "rel_position": rel,
                     "at_low": at_low, "at_high": at_high,
                     "suggestion": suggestion, "note": note}
    return out
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_summary.py -q`
Expected: PASS（8 passed）

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/diagnosis/hpo/summary.py tests/test_diagnosis/test_hpo/test_summary.py && \
  git commit -m "feat(hpo-diag): convergence + boundary summary (answers Q1/Q2)"
```

---

### Task 4: `compute_importances`（best-effort）+ `build_summary`

**Files:**
- Modify: `src/recsys_tfb/diagnosis/hpo/summary.py`（append）
- Test: `tests/test_diagnosis/test_hpo/test_summary_importances.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_summary_importances.py`:
```python
import optuna

from recsys_tfb.diagnosis.hpo.collect import collect_trials
from recsys_tfb.diagnosis.hpo.summary import build_summary, compute_importances


def test_importances_real_study():
    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    study.optimize(
        lambda t: t.suggest_float("x", 0, 1) + 0.5 * t.suggest_float("y", 0, 1),
        n_trials=12,
    )
    imp = compute_importances(study)
    assert imp is not None
    assert set(imp) <= {"x", "y"}
    assert all(isinstance(v, float) for v in imp.values())


def test_importances_degenerate_returns_none():
    study = optuna.create_study(direction="maximize")
    study.optimize(lambda t: t.suggest_float("x", 0, 1), n_trials=1)  # 1 trial → raise → None
    assert compute_importances(study) is None


def test_build_summary_shape():
    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    study.optimize(lambda t: t.suggest_int("num_leaves", 20, 100), n_trials=6)
    ss = [{"name": "num_leaves", "type": "int", "low": 20, "high": 100}]
    payload = collect_trials(
        study, ss, model_version="m", search_id="s", hpo_objective="mean_ap",
        seed=1, n_trials_target=6, best_iteration=0, generated_at="t",
    )
    s = build_summary(study, payload, patience=3, hi_thresh=0.98, lo_thresh=0.02)
    assert set(s) == {"convergence", "boundary", "importances"}
    assert "num_leaves" in s["boundary"]
    assert s["convergence"]["n_completed"] == 6
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_summary_importances.py -q`
Expected: FAIL（`ImportError: cannot import name 'compute_importances'`）

- [ ] **Step 3: append 到 summary.py**

在 `src/recsys_tfb/diagnosis/hpo/summary.py` 檔尾追加：
```python
def compute_importances(study) -> Optional[dict]:
    """Best-effort fANOVA param importances。任何失敗（trial/參數過少、objective
    近常數、backend 缺）都回 None（空 dict 也視為 None）。"""
    try:
        import optuna

        imp = optuna.importance.get_param_importances(study)
        return {k: float(v) for k, v in imp.items()} or None
    except Exception:  # pragma: no cover - 由退化 study 測試觸發
        logger.warning("HPO param importance unavailable; skipping", exc_info=True)
        return None


def build_summary(
    study, trials_payload: dict, *, patience: int, hi_thresh: float, lo_thresh: float
) -> dict:
    """彙整 convergence（純 trials）+ boundary（純 best_params）+ importances（study, best-effort）。"""
    trials = trials_payload["trials"]
    best = trials_payload.get("best") or {}
    best_params = best.get("params", {})
    search_space = trials_payload["meta"]["search_space"]
    return {
        "convergence": compute_convergence(trials, patience=patience),
        "boundary": compute_boundary(
            best_params, search_space, hi_thresh=hi_thresh, lo_thresh=lo_thresh
        ),
        "importances": compute_importances(study),
    }
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_summary_importances.py -q`
Expected: PASS（3 passed）

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/diagnosis/hpo/summary.py tests/test_diagnosis/test_hpo/test_summary_importances.py && \
  git commit -m "feat(hpo-diag): best-effort fANOVA importances + build_summary"
```

---

### Task 5: `render_charts` — 5 張圖，每張 best-effort，共用 plotly.min.js

**Files:**
- Create: `src/recsys_tfb/diagnosis/hpo/render.py`
- Test: `tests/test_diagnosis/test_hpo/test_render.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_render.py`:
```python
import optuna

from recsys_tfb.diagnosis.hpo.render import render_charts


def _study(nparams=2, n=8):
    s = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )

    def obj(t):
        v = t.suggest_float("x", 0, 1)
        if nparams >= 2:
            v += 0.5 * t.suggest_float("y", 0, 1)
        return v

    s.optimize(obj, n_trials=n)
    return s


def test_render_all_five_with_shared_js(tmp_path):
    written = render_charts(_study(nparams=2, n=8), tmp_path)
    assert set(written) == {
        "optimization_history.html", "param_importances.html", "slice.html",
        "contour.html", "parallel_coordinate.html",
    }
    for f in written:
        assert (tmp_path / f).stat().st_size > 0
    # directory 模式：共用一份 plotly.min.js，各 HTML 才會是 KB 級
    assert (tmp_path / "plotly.min.js").exists()
    assert (tmp_path / "optimization_history.html").stat().st_size < 100_000


def test_render_degenerate_skips_importances(tmp_path):
    # 完成 trial <2 → plot_param_importances raise（其餘 4 張照畫）
    written = render_charts(_study(nparams=2, n=1), tmp_path)
    assert "param_importances.html" not in written
    assert "optimization_history.html" in written
    assert "slice.html" in written
    assert len(written) == 4
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_render.py -q`
Expected: FAIL（`ModuleNotFoundError: ...render`）

- [ ] **Step 3: 實作 render**

`src/recsys_tfb/diagnosis/hpo/render.py`:
```python
"""把 study 渲染成 5 張自足 HTML 圖，每張各自 best-effort，dir 內共用一份 plotly.min.js。"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# (輸出檔名, optuna.visualization 函式名)
_CHARTS = [
    ("optimization_history.html", "plot_optimization_history"),
    ("param_importances.html", "plot_param_importances"),
    ("slice.html", "plot_slice"),
    ("contour.html", "plot_contour"),
    ("parallel_coordinate.html", "plot_parallel_coordinate"),
]


def render_charts(study, out_dir) -> list[str]:
    """Render 5 charts into out_dir。每張 best-effort：任一張失敗（例如完成 trial <2 時
    param_importances raise）只記 warning、跳過該張，不影響其餘。回傳實際寫出的檔名 list。

    用 include_plotlyjs="directory"：dir 內共用一份 plotly.min.js，各 HTML 只剩 KB 級、
    且離線可看（不觸網）。"""
    from optuna import visualization as viz

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for filename, fn_name in _CHARTS:
        try:
            fig = getattr(viz, fn_name)(study)
            fig.write_html(str(out_dir / filename), include_plotlyjs="directory")
            written.append(filename)
        except Exception:
            logger.warning("HPO chart %s skipped", filename, exc_info=True)
    return written
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_render.py -q`
Expected: PASS（2 passed）

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/diagnosis/hpo/render.py tests/test_diagnosis/test_hpo/test_render.py && \
  git commit -m "feat(hpo-diag): render 5 charts (per-chart best-effort, shared plotly.min.js)"
```

---

### Task 6: `write_hpo_diagnostics` 入口（enabled gate + 編排）

**Files:**
- Create: `src/recsys_tfb/diagnosis/hpo/write.py`
- Modify: `src/recsys_tfb/diagnosis/hpo/__init__.py`（補 export）
- Test: `tests/test_diagnosis/test_hpo/test_write.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_write.py`:
```python
import json

import optuna
import pytest

import recsys_tfb.diagnosis.hpo.write as W
from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics


def _study(n=8):
    s = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=1)
    )
    s.optimize(
        lambda t: t.suggest_int("num_leaves", 20, 100) / 100
        + 0.3 * t.suggest_float("lr", 1e-3, 1e-1, log=True),
        n_trials=n,
    )
    return s


_SS = [
    {"name": "num_leaves", "type": "int", "low": 20, "high": 100},
    {"name": "lr", "type": "float", "low": 1e-3, "high": 1e-1, "log": True},
]


def _call(study, params):
    write_hpo_diagnostics(
        study, _SS, params, search_id="sid", hpo_objective="mean_ap",
        seed=1, n_trials_target=10, best_iteration=7,
    )


def test_write_end_to_end(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _call(_study(8), {"model_version": "mvE"})
    hpo = tmp_path / "data/models/mvE/diagnostics/hpo"
    assert json.loads((hpo / "hpo_trials.json").read_text())["schema_version"] == 1
    summary = json.loads((hpo / "hpo_summary.json").read_text())
    assert set(summary) == {"convergence", "boundary", "importances"}
    for f in ("optimization_history.html", "param_importances.html", "slice.html",
              "contour.html", "parallel_coordinate.html", "plotly.min.js"):
        assert (hpo / f).exists()


def test_write_disabled_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _call(_study(4), {"model_version": "mvD",
                      "diagnostics": {"hpo_search": {"enabled": False}}})
    assert not (tmp_path / "data/models/mvD/diagnostics/hpo").exists()


def test_write_json_survives_render_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError("render boom")

    monkeypatch.setattr(W, "render_charts", _boom)
    with pytest.raises(RuntimeError):
        _call(_study(4), {"model_version": "mvR"})
    hpo = tmp_path / "data/models/mvR/diagnostics/hpo"
    assert (hpo / "hpo_trials.json").exists()   # JSON 在 render 前已落地
    assert (hpo / "hpo_summary.json").exists()
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_write.py -q`
Expected: FAIL（`ModuleNotFoundError: ...write` / `ImportError: write_hpo_diagnostics`）

- [ ] **Step 3: 實作 write.py 並補 __init__ export**

`src/recsys_tfb/diagnosis/hpo/write.py`:
```python
"""HPO 搜尋診斷入口：稽核 JSON + 摘要 + 5 張自動圖（寫進 diagnostics/hpo/）。"""

from __future__ import annotations

import logging

from recsys_tfb.diagnosis.hpo._io import atomic_write_json
from recsys_tfb.diagnosis.hpo.collect import collect_trials
from recsys_tfb.diagnosis.hpo.paths import hpo_dir
from recsys_tfb.diagnosis.hpo.render import render_charts
from recsys_tfb.diagnosis.hpo.summary import build_summary

logger = logging.getLogger(__name__)


def write_hpo_diagnostics(
    study, search_space, parameters, *,
    search_id, hpo_objective, seed, n_trials_target, best_iteration,
):
    """為（完成或 resume 的）study 寫 HPO 搜尋診斷。

    受 diagnostics.hpo_search.enabled 控制（預設 True）。先原子寫 hpo_trials.json +
    hpo_summary.json，再 render 5 張圖。設計為由 tune_hyperparameters 尾端 best-effort 呼叫。
    """
    cfg = (parameters.get("diagnostics") or {}).get("hpo_search") or {}
    if not cfg.get("enabled", True):
        logger.info("diagnostics.hpo_search.enabled=false; skip HPO diagnostics")
        return
    patience = int(cfg.get("patience", 10))
    hi = float(cfg.get("boundary_hi", 0.98))
    lo = float(cfg.get("boundary_lo", 0.02))

    out = hpo_dir(parameters)
    payload = collect_trials(
        study, search_space,
        model_version=parameters["model_version"], search_id=search_id,
        hpo_objective=hpo_objective, seed=seed,
        n_trials_target=n_trials_target, best_iteration=best_iteration,
    )
    atomic_write_json(out / "hpo_trials.json", payload)
    summary = build_summary(study, payload, patience=patience, hi_thresh=hi, lo_thresh=lo)
    atomic_write_json(out / "hpo_summary.json", summary)
    written = render_charts(study, out)
    logger.info("HPO diagnostics written to %s (%d charts)", out, len(written))
```

覆寫 `src/recsys_tfb/diagnosis/hpo/__init__.py`:
```python
"""HPO 搜尋診斷：稽核 JSON + 摘要 + 自動圖（best-effort）。"""

from recsys_tfb.diagnosis.hpo.write import write_hpo_diagnostics

__all__ = ["write_hpo_diagnostics"]
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_write.py -q`
Expected: PASS（3 passed）

- [ ] **Step 5: 跑整包確認無回歸**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/ -q`
Expected: PASS（全部 passed）

- [ ] **Step 6: Commit**

```bash
cd $WT && git add src/recsys_tfb/diagnosis/hpo/write.py src/recsys_tfb/diagnosis/hpo/__init__.py tests/test_diagnosis/test_hpo/test_write.py && \
  git commit -m "feat(hpo-diag): write_hpo_diagnostics entry (enabled gate + orchestration)"
```

---

### Task 7: config —— `diagnostics.hpo_search` block

**Files:**
- Modify: `conf/base/parameters_training.yaml`（`diagnostics:` block 內、`shap:` 之後）
- Test: `tests/test_diagnosis/test_hpo/test_config.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_diagnosis/test_hpo/test_config.py`:
```python
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]


def test_hpo_search_config_block_present():
    cfg = yaml.safe_load((REPO / "conf/base/parameters_training.yaml").read_text())
    hs = cfg["diagnostics"]["hpo_search"]
    assert hs["enabled"] is True
    assert hs["patience"] == 10
    assert hs["boundary_hi"] == 0.98
    assert hs["boundary_lo"] == 0.02
```

- [ ] **Step 2: 跑測試確認 FAIL**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_config.py -q`
Expected: FAIL（`KeyError: 'hpo_search'`）

- [ ] **Step 3: 在 parameters_training.yaml 的 `diagnostics:` block 內、`shap:` 子區塊之後加入**

（縮排 2 空格，與 `feature_stats:` / `shap:` 同層）：
```yaml
  # HPO 搜尋診斷（放此非 training:，故不進 model_version hash）。每次 HPO 後 best-effort
  # 寫 diagnostics/hpo/：hpo_trials.json（稽核）+ hpo_summary.json（收斂/邊界/重要性）+ 5 張圖。
  # patience/boundary_* 是啟發式提示、非保證，判讀見 spec §7。
  hpo_search:
    enabled: true        # false → 整包跳過（連稽核 JSON 都不寫）
    patience: 10         # 連續 N 個完成 trial 未進步 → plateau 提示（答「要不要再繼續 trial」）
    boundary_hi: 0.98    # 最佳值相對位置 >= 此 → 貼上界，建議放寬（答「search range 要不要調」）
    boundary_lo: 0.02    # 相對位置 <= 此 → 貼下界，建議放寬
```

- [ ] **Step 4: 跑測試確認 PASS**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/test_config.py -q`
Expected: PASS（1 passed）

- [ ] **Step 5: Commit**

```bash
cd $WT && git add conf/base/parameters_training.yaml tests/test_diagnosis/test_hpo/test_config.py && \
  git commit -m "feat(hpo-diag): diagnostics.hpo_search config block (not model_version-hashed)"
```

---

### Task 8: 整合進 `tune_hyperparameters` 尾端 + 回歸驗證 + real-run

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`（`tune_hyperparameters` 的 `return` 前，約 :600）

- [ ] **Step 1: 加入 best-effort 呼叫**

在 `src/recsys_tfb/pipelines/training/nodes.py` 把：
```python
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, best_state["score"], best_iteration, best_params,
    )
    return best_params, best_iteration, best_model
```
改為（在 `return` 前插入 try/except；`study` / `search_space` / `search_id` / `hpo_objective` / `seed` / `n_trials` / `best_iteration` 此處皆在 scope 內）：
```python
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, best_state["score"], best_iteration, best_params,
    )

    # HPO 搜尋診斷：best-effort 側輸出，衍生自本地 study。失敗只 warning、絕不影響
    # 回傳（診斷 bug 不得逼你重跑 HPO）。不新增 DAG node、不改本函式 outputs → 對
    # RESUME_CONTRACTS 隱形。產物寫進 diagnostics_dir/hpo/，由 log_experiment 的
    # log_artifacts 撿走。見 docs/superpowers/specs/2026-07-15-hpo-search-diagnostics-design.md
    try:
        from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics

        write_hpo_diagnostics(
            study, search_space, parameters,
            search_id=search_id, hpo_objective=hpo_objective, seed=seed,
            n_trials_target=n_trials, best_iteration=best_iteration,
        )
    except Exception:  # pragma: no cover - best-effort guard
        logger.warning("HPO diagnostics failed; training continues", exc_info=True)

    return best_params, best_iteration, best_model
```

- [ ] **Step 2: resume 契約回歸（證明零 DAG 影響，對應要求 2）**

Run: `cd $WT && $PY -m pytest tests/test_pipelines/test_resume_contracts.py -q`
Expected: PASS（原封不動全綠——本改動不新增 node/output，`slice_from("finalize_model")` 不變）

- [ ] **Step 3: 全新 package + 相關 training 測試回歸**

Run: `cd $WT && $PY -m pytest tests/test_diagnosis/test_hpo/ tests/test_pipelines/ -q`
Expected: PASS

- [ ] **Step 4: graphify rebuild（改過 src，讓圖保持最新）**

Run:
```bash
cd $WT && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: 印出 "Rebuilt: ... nodes"；`git -C $WT checkout -- graphify-out/GRAPH_REPORT.md`（若被 hook 弄髒）

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/pipelines/training/nodes.py && \
  git commit -m "feat(hpo-diag): call write_hpo_diagnostics at tune_hyperparameters tail (best-effort, resume-invisible)"
```

- [ ] **Step 6: real-run 端到端驗證（本機 local Spark；background 執行）**

先建本機 dataset（若尚無），再跑 training 產出一個 model_version：
```bash
cd $WT && export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```
（詳細本機步驟見 `docs/operations/local-spark-setup.md`；>2 分鐘指令一律 background。）

驗收：找出剛產的 model_version 目錄，確認 7 個檔 + plotly.min.js：
```bash
cd $WT && ls -la data/models/*/diagnostics/hpo/
# 預期：hpo_trials.json hpo_summary.json + 5 張 *.html + plotly.min.js
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "import json,glob; p=sorted(glob.glob('data/models/*/diagnostics/hpo/hpo_summary.json'))[-1]; s=json.load(open(p)); print('convergence:', s['convergence']['plateau'], s['convergence']['note']); print('boundary keys:', list(s['boundary'])); print('importances:', s['importances'])"
```
Expected: `hpo_summary.json` 的 convergence 有 plateau 判定與 note、boundary 每個數值參數一筆、importances 有值或 null。

- [ ] **Step 7: real-run 的 best-effort mutation 檢查（證明尾端 try/except 真的兜底）**

暫時在 `write_hpo_diagnostics` 第一行插入 `raise RuntimeError("mutation")`，重跑一次 `training --env local`，確認 **training 仍成功完成**（log 出現 "HPO diagnostics failed; training continues" 且退出碼 0），再**還原**該行。這證明尾端 try/except 覆蓋（拿掉 try/except，training 應轉紅）。

---

## Self-Review

**Spec 覆蓋度**（逐節對照）：
- §4 架構/整合點 → Task 1（package）、Task 8（尾端呼叫）✓
- §5 resume 安全性 → Task 8 Step 2（resume 契約回歸）、Step 7（mutation）✓
- §6 hpo_trials.json schema → Task 2 ✓
- §7 hpo_summary（convergence/boundary/importances）→ Task 3、4 ✓
- §8 5 張圖 + directory 模式 + per-chart best-effort → Task 5 ✓
- §9 config（diagnostics.hpo_search）→ Task 7 ✓
- §11 風險（best-effort、3.6MB 共用 JS）→ Task 5/6/8 測試涵蓋 ✓
- §12 測試計畫 → 各 Task 的 TDD + Task 8 resume 回歸 ✓

**型別/命名一致性**：`write_hpo_diagnostics` 簽名（Task 6 定義 = Task 8 呼叫）一致；`collect_trials` / `build_summary` / `render_charts` 跨 Task 命名一致；config 鍵 `patience`/`boundary_hi`/`boundary_lo`（Task 7 yaml = Task 6 `.get()` 讀取）一致。

**Placeholder 掃描**：無 TBD/TODO；每個 code step 附完整程式碼與可執行指令＋預期輸出。

**已知殘留**（非 placeholder，spec §13 開放項）：有序 categorical 邊界建議策略暫不做（categorical 一律 `suggestion="n/a"`）。
