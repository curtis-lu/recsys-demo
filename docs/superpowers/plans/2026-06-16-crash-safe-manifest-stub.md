# Crash-safe Provenance Manifest Stub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write each training/dataset version directory's `manifest.json` *before* the pipeline runs (status=`running`, full `parameters`) so a mid-run crash still records which params produced that version, and warn loudly when a sliced training resume would silently retrain a different model.

**Architecture:** Two-phase manifest. Phase 1 (pre-run): `_write_manifest_stub` writes a skip-if-present `running` stub with all early-knowable fields — no symlink, no sidecar. Phase 2 (post-run, success-only): existing `_write_pipeline_manifest` now stamps `status="completed"` and enriches with artifacts/sidecar/symlink. Plus a training-only advisory: when a `--from-node`/`--only-node` slice auto-includes the `model` producer, log a loud WARN (computed model_version, nodes to be retrained, nearest existing completed version, diff hint) and proceed.

**Tech Stack:** Python 3.10, Typer CLI (`src/recsys_tfb/__main__.py`), hand-rolled versioning (`src/recsys_tfb/core/versioning.py`), pytest. Pure-function / `tmp_path` tests only — no Spark.

---

## Conventions

- **Worktree root (`$WT`):** `/Users/curtislu/projects/recsys_tfb/.worktrees/crash-safe-manifest-stub`
- **Python (`$PY`):** `/Users/curtislu/projects/recsys_tfb/.venv/bin/python` (absolute — never bare `.venv/bin/python`, which ELOOPs through the relative symlink)
- **Run tests from `$WT`** with: `cd $WT && PYTHONPATH=src $PY -m pytest <paths> -q`
- All tasks here touch **only** fast unit tests (no Spark). Each commit auto-rebuilds the graphify graph via the post-commit hook — no manual graphify step needed.
- `git status` should stay clean except your edits; if `graphify-out/GRAPH_REPORT.md` shows dirty before a commit, that's the hook — it's untracked now and harmless.

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `src/recsys_tfb/core/versioning.py` | version IDs + manifest schema | add `status` to `build_manifest_metadata`; add pure `find_latest_completed_model_version` |
| `src/recsys_tfb/__main__.py` | CLI commands + manifest writing + slicing | add `_write_manifest_stub`, `_format_retrain_advisory`, `_maybe_warn_retrain`; stamp `completed` in `_write_pipeline_manifest`; wire stub + advisory into `training`/`dataset`/`_execute_pipeline` |
| `tests/test_core/test_versioning.py` | versioning unit tests | add status + find-latest tests |
| `tests/test_cli.py` | `__main__` helper unit tests | add stub + advisory tests |

---

### Task 1: `status` field in `build_manifest_metadata`

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py:317-361` (`build_manifest_metadata`)
- Test: `tests/test_core/test_versioning.py` (class `TestBuildManifestMetadata`, after line ~563)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_core/test_versioning.py` inside `class TestBuildManifestMetadata`:

```python
    def test_status_included_when_passed(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="training",
            parameters={"lr": 0.01},
            status="running",
        )
        assert meta["status"] == "running"

    def test_status_omitted_when_none(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="training",
            parameters={"lr": 0.01},
        )
        assert "status" not in meta
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_core/test_versioning.py::TestBuildManifestMetadata -q`
Expected: `test_status_included_when_passed` FAILS with `TypeError: build_manifest_metadata() got an unexpected keyword argument 'status'`.

- [ ] **Step 3: Add the `status` parameter**

In `src/recsys_tfb/core/versioning.py`, change the signature of `build_manifest_metadata` to insert `status` after `parameters`:

```python
def build_manifest_metadata(
    *,
    version: str,
    pipeline: str,
    parameters: dict,
    status: str | None = None,
    base_dataset_version: str | None = None,
    train_variant_id: str | None = None,
    calibration_variant_id: str | None = None,
    model_version: str | None = None,
    parent_version: str | None = None,
    variant_kind: str | None = None,
    feature_table_fingerprint: str | None = None,
    artifacts: list[str] | None = None,
) -> dict:
```

Then, immediately after the `metadata: dict = { ... }` literal (after the `"parameters": parameters,` line, before `if base_dataset_version is not None:`), add:

```python
    if status is not None:
        metadata["status"] = status
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_core/test_versioning.py::TestBuildManifestMetadata -q`
Expected: PASS (all tests in the class, including the pre-existing ones).

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git commit -m "feat(versioning): build_manifest_metadata 支援 status 欄位"
```

---

### Task 2: `find_latest_completed_model_version` helper

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py` (add function after `build_manifest_metadata`, near line 362)
- Test: `tests/test_core/test_versioning.py` (new class at end of file)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_core/test_versioning.py`:

```python
class TestFindLatestCompletedModelVersion:
    def _write(self, d, manifest):
        import json
        d.mkdir(parents=True, exist_ok=True)
        with open(d / "manifest.json", "w") as f:
            json.dump(manifest, f)

    def test_picks_most_recent_completed(self, tmp_path):
        from recsys_tfb.core.versioning import find_latest_completed_model_version
        self._write(tmp_path / "old11111", {
            "version": "old11111", "status": "completed",
            "created_at": "2026-06-01T00:00:00+00:00"})
        self._write(tmp_path / "new22222", {
            "version": "new22222", "status": "completed",
            "created_at": "2026-06-10T00:00:00+00:00"})
        assert find_latest_completed_model_version(tmp_path) == (
            "new22222", "2026-06-10T00:00:00+00:00")

    def test_skips_running_and_broken(self, tmp_path):
        from recsys_tfb.core.versioning import find_latest_completed_model_version
        self._write(tmp_path / "crashed1", {
            "version": "crashed1", "status": "running",
            "created_at": "2026-06-20T00:00:00+00:00"})
        (tmp_path / "garbage1").mkdir()
        (tmp_path / "garbage1" / "manifest.json").write_text("{not json")
        self._write(tmp_path / "good3333", {
            "version": "good3333", "status": "completed",
            "created_at": "2026-06-05T00:00:00+00:00"})
        assert find_latest_completed_model_version(tmp_path) == (
            "good3333", "2026-06-05T00:00:00+00:00")

    def test_legacy_missing_status_counts_as_completed(self, tmp_path):
        from recsys_tfb.core.versioning import find_latest_completed_model_version
        self._write(tmp_path / "legacy11", {
            "version": "legacy11", "created_at": "2026-06-07T00:00:00+00:00"})
        assert find_latest_completed_model_version(tmp_path) == (
            "legacy11", "2026-06-07T00:00:00+00:00")

    def test_none_when_no_models(self, tmp_path):
        from recsys_tfb.core.versioning import find_latest_completed_model_version
        assert find_latest_completed_model_version(tmp_path) is None
        assert find_latest_completed_model_version(tmp_path / "missing") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_core/test_versioning.py::TestFindLatestCompletedModelVersion -q`
Expected: FAIL with `ImportError: cannot import name 'find_latest_completed_model_version'`.

- [ ] **Step 3: Implement the function**

In `src/recsys_tfb/core/versioning.py`, add after `build_manifest_metadata` (after line ~361). `json` and `Path` are already imported at the top of the module:

```python
def find_latest_completed_model_version(models_dir: Path) -> tuple[str, str] | None:
    """Return ``(version, created_at)`` of the most recently created model whose
    manifest ``status`` is ``"completed"`` (or legacy: no ``status`` field).

    Skips running/failed and unreadable manifests, and the ``best`` symlink.
    ``created_at`` is an ISO-8601 string, so lexicographic max == newest.
    Returns ``None`` when nothing qualifies or ``models_dir`` does not exist.
    """
    if not models_dir.is_dir():
        return None
    best: tuple[str, str] | None = None  # (created_at, version)
    for child in models_dir.iterdir():
        if not child.is_dir() or child.is_symlink():
            continue
        manifest_path = child / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            with open(manifest_path) as f:
                m = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if m.get("status", "completed") != "completed":
            continue
        created = m.get("created_at", "")
        version = m.get("version", child.name)
        if best is None or created > best[0]:
            best = (created, version)
    if best is None:
        return None
    return (best[1], best[0])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_core/test_versioning.py::TestFindLatestCompletedModelVersion -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git commit -m "feat(versioning): find_latest_completed_model_version 掃 manifest 取最近 completed"
```

---

### Task 3: `_write_manifest_stub` helper

**Files:**
- Modify: `src/recsys_tfb/__main__.py` (add helper right before `_write_pipeline_manifest`, ~line 251; add import in the versioning import block at line 22-36)
- Test: `tests/test_cli.py` (new test, after the `test_sample_weight_extra_absent_returns_none` block ~line 445)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cli.py` (after line ~445):

```python
def test_write_manifest_stub_writes_running(tmp_path):
    import json
    from recsys_tfb.__main__ import _write_manifest_stub
    vdir = tmp_path / "ab12cd34"
    _write_manifest_stub(
        vdir,
        {"version": "ab12cd34", "pipeline": "training",
         "parameters": {"training": {"lr": 0.01}},
         "base_dataset_version": "base1234", "train_variant_id": "trv12345"},
        run_id="run-xyz",
    )
    with open(vdir / "manifest.json") as f:
        m = json.load(f)
    assert m["status"] == "running"
    assert m["run_id"] == "run-xyz"
    assert m["parameters"] == {"training": {"lr": 0.01}}
    assert not (vdir / "latest").exists()           # no symlink
    assert not (vdir / "parameters_training.json").exists()  # no sidecar


def test_write_manifest_stub_skips_if_present(tmp_path):
    import json
    from recsys_tfb.__main__ import _write_manifest_stub
    vdir = tmp_path / "ab12cd34"
    vdir.mkdir()
    (vdir / "manifest.json").write_text(json.dumps(
        {"version": "ab12cd34", "status": "completed", "sentinel": True}))
    _write_manifest_stub(
        vdir,
        {"version": "ab12cd34", "pipeline": "training", "parameters": {}},
        run_id="run-new",
    )
    with open(vdir / "manifest.json") as f:
        m = json.load(f)
    assert m == {"version": "ab12cd34", "status": "completed", "sentinel": True}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py::test_write_manifest_stub_writes_running tests/test_cli.py::test_write_manifest_stub_skips_if_present -q`
Expected: FAIL with `ImportError: cannot import name '_write_manifest_stub'`.

- [ ] **Step 3: Implement the helper**

In `src/recsys_tfb/__main__.py`, add this function immediately before `def _write_pipeline_manifest(` (line ~251):

```python
def _write_manifest_stub(version_dir, metadata_kwargs, run_id):
    """Pre-run provenance stub: write manifest.json with status=running so a
    crash before the post-run write still records which parameters defined this
    version. Skip-if-present (never clobber an existing manifest); writes no
    `latest` symlink and no params sidecar (the stub already embeds parameters).
    """
    if (version_dir / "manifest.json").exists():
        return
    metadata = build_manifest_metadata(**metadata_kwargs, status="running")
    metadata["run_id"] = run_id
    write_manifest(version_dir, metadata)
```

(`build_manifest_metadata` and `write_manifest` are already imported.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py::test_write_manifest_stub_writes_running tests/test_cli.py::test_write_manifest_stub_skips_if_present -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): _write_manifest_stub 寫 pre-run running stub（skip-if-present、無 symlink）"
```

---

### Task 4: `_write_pipeline_manifest` stamps `status="completed"`

**Files:**
- Modify: `src/recsys_tfb/__main__.py:260` (inside `_write_pipeline_manifest`)
- Test: `tests/test_cli.py` (new test)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cli.py`:

```python
def test_write_pipeline_manifest_stamps_completed(tmp_path):
    import json
    from recsys_tfb.__main__ import _write_pipeline_manifest
    vdir = tmp_path / "ab12cd34"
    _write_pipeline_manifest(
        version_dir=vdir,
        metadata_kwargs={"version": "ab12cd34", "pipeline": "training",
                         "parameters": {"lr": 0.01}, "artifacts": ["model"]},
        run_id="run-1",
    )
    with open(vdir / "manifest.json") as f:
        m = json.load(f)
    assert m["status"] == "completed"
    assert m["artifacts"] == ["model"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py::test_write_pipeline_manifest_stamps_completed -q`
Expected: FAIL with `KeyError: 'status'`.

- [ ] **Step 3: Stamp completed**

In `src/recsys_tfb/__main__.py`, change line 260 inside `_write_pipeline_manifest`:

```python
    metadata = build_manifest_metadata(**metadata_kwargs, status="completed")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py::test_write_pipeline_manifest_stamps_completed -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): post-run manifest 蓋 status=completed"
```

---

### Task 5: `_format_retrain_advisory` message builder

**Files:**
- Modify: `src/recsys_tfb/__main__.py` (add helper after `_format_node_list`, ~line 149)
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cli.py`:

```python
def test_format_retrain_advisory_with_latest():
    from recsys_tfb.__main__ import _format_retrain_advisory
    lines = _format_retrain_advisory(
        "ab12cd34", ["finalize_model", "tune_hyperparameters"],
        ("old11111", "2026-06-01T00:00:00+00:00"))
    text = "\n".join(lines)
    assert "ab12cd34" in text
    assert "finalize_model" in text and "tune_hyperparameters" in text
    assert "old11111" in text
    assert "data/models/old11111/manifest.json" in text

def test_format_retrain_advisory_without_latest():
    from recsys_tfb.__main__ import _format_retrain_advisory
    lines = _format_retrain_advisory("ab12cd34", ["finalize_model"], None)
    text = "\n".join(lines)
    assert "ab12cd34" in text
    assert "finalize_model" in text
    assert "manifest.json" not in text  # no nearest-version section
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py::test_format_retrain_advisory_with_latest tests/test_cli.py::test_format_retrain_advisory_without_latest -q`
Expected: FAIL with `ImportError: cannot import name '_format_retrain_advisory'`.

- [ ] **Step 3: Implement the formatter**

In `src/recsys_tfb/__main__.py`, add after `_format_node_list` (after line ~149):

```python
def _format_retrain_advisory(model_version, retrain_nodes, latest):
    """Loud WARN lines for an unexpected retrain triggered by a sliced resume.

    ``latest`` is ``(version, created_at)`` of the nearest existing completed
    model, or ``None``.
    """
    lines = [
        f"[retrain] model_version={model_version} — 無既有 finalized 模型。",
        f"[retrain] 此切片將 auto-include 並重新訓練：{', '.join(retrain_nodes)}",
    ]
    if latest is not None:
        ver, created = latest
        lines.append(f"[retrain] 最接近的既有模型：{ver} (completed, {created})")
        lines.append(
            "[retrain] 想對它重跑？比對你現在的 parameters_training.yaml 與 "
            f"data/models/{ver}/manifest.json 的 parameters（training: 區塊）。"
        )
    lines.append("[retrain] 仍依契約繼續執行（缺料自動補跑）…")
    return lines
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py::test_format_retrain_advisory_with_latest tests/test_cli.py::test_format_retrain_advisory_without_latest -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): _format_retrain_advisory 組重訓警告訊息"
```

---

### Task 6: `_maybe_warn_retrain` trigger + orchestration

**Files:**
- Modify: `src/recsys_tfb/__main__.py` (add helper after `_format_retrain_advisory`; add `find_latest_completed_model_version` to the versioning import block at lines 22-36)
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cli.py` (this reuses `SlicePlan` and the manifest-writing pattern):

```python
def _plan_with_auto(auto):
    from recsys_tfb.core.pipeline import SlicePlan
    return SlicePlan(mode="from", requested=("predict_and_write_test_predictions",),
                     auto_included=auto)

def test_maybe_warn_retrain_fires_when_model_pulled_in(tmp_path):
    import json
    from recsys_tfb.__main__ import _maybe_warn_retrain
    (tmp_path / "old11111").mkdir()
    (tmp_path / "old11111" / "manifest.json").write_text(json.dumps(
        {"version": "old11111", "status": "completed",
         "created_at": "2026-06-01T00:00:00+00:00"}))
    plan = _plan_with_auto({"finalize_model": ("model", "best_params")})
    lines = _maybe_warn_retrain(
        plan, {"models_dir": tmp_path, "model_version": "ab12cd34"})
    text = "\n".join(lines)
    assert "ab12cd34" in text and "finalize_model" in text and "old11111" in text

def test_maybe_warn_retrain_silent_when_model_present(tmp_path):
    from recsys_tfb.__main__ import _maybe_warn_retrain
    plan = _plan_with_auto({"cache_val_model_input": ("val_model_input",)})
    assert _maybe_warn_retrain(
        plan, {"models_dir": tmp_path, "model_version": "ab12cd34"}) == []

def test_maybe_warn_retrain_silent_without_advice():
    from recsys_tfb.__main__ import _maybe_warn_retrain
    plan = _plan_with_auto({"finalize_model": ("model",)})
    assert _maybe_warn_retrain(plan, None) == []

def test_maybe_warn_retrain_silent_when_plan_none():
    from recsys_tfb.__main__ import _maybe_warn_retrain
    assert _maybe_warn_retrain(None, {"models_dir": ".", "model_version": "x"}) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py -k maybe_warn_retrain -q`
Expected: FAIL with `ImportError: cannot import name '_maybe_warn_retrain'`.

- [ ] **Step 3: Add the import and implement the helper**

In `src/recsys_tfb/__main__.py`, add `find_latest_completed_model_version,` to the versioning import block (alphabetically, after `compute_train_variant_id,`):

```python
from recsys_tfb.core.versioning import (
    build_manifest_metadata,
    compute_base_dataset_version,
    compute_calibration_variant_id,
    compute_feature_table_fingerprint,
    compute_model_version,
    compute_search_id,
    compute_train_variant_id,
    find_latest_completed_model_version,
    read_manifest,
    resolve_base_dataset_version,
    resolve_model_version,
    resolve_variant_id,
    update_symlink,
    write_manifest,
)
```

Then add this function right after `_format_retrain_advisory`:

```python
def _maybe_warn_retrain(plan, retrain_advice):
    """Return loud-WARN lines when a sliced run will auto-include the model
    producer (``model`` was missing -> finalize/calibrate pulled in), else ``[]``.

    ``retrain_advice`` is ``{"models_dir": Path, "model_version": str}`` (passed
    only by the training command) or ``None``.
    """
    if retrain_advice is None or plan is None:
        return []
    if not any("model" in missing for missing in plan.auto_included.values()):
        return []
    latest = find_latest_completed_model_version(retrain_advice["models_dir"])
    return _format_retrain_advisory(
        retrain_advice["model_version"], list(plan.auto_included), latest
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py -k maybe_warn_retrain -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cd $WT && git add src/recsys_tfb/__main__.py tests/test_cli.py
git commit -m "feat(cli): _maybe_warn_retrain 偵測 sliced resume 觸發的非預期重訓"
```

---

### Task 7: Wire stub + advisory into `_execute_pipeline`, `training`, `dataset`

**Files:**
- Modify: `src/recsys_tfb/__main__.py` — `_execute_pipeline` (161-248), `training` command (after line ~670, before line 674), `dataset` command (after line ~521, before line 525)

This task is thin glue over the unit-tested helpers; verification is the full fast suite plus an optional dry-run smoke (Task 8).

- [ ] **Step 1: Add `retrain_advice` param to `_execute_pipeline` and emit the advisory**

In `src/recsys_tfb/__main__.py`, add a parameter to `_execute_pipeline` (after `list_nodes: bool = False,` at line ~172):

```python
    retrain_advice: Optional[dict] = None,
```

Then, in the body, locate the slice-plan logging block (lines ~229-231):

```python
    if plan is not None:
        for line in _format_slice_plan(plan, total):
            logger.info(line)
```

Immediately **after** that block and **before** `if dry_run:` (line 232), insert:

```python
    for line in _maybe_warn_retrain(plan, retrain_advice):
        logger.warning(line)
```

(Placing it before the `dry_run` early-return means `--dry-run` also surfaces the warning.)

- [ ] **Step 2: Pass `retrain_advice` from the `training` command**

In the `training` command, the `_execute_pipeline(...)` call is at lines ~674-678. Add the `retrain_advice` kwarg:

```python
    executed = _execute_pipeline(
        "training", pipeline_kwargs, runtime_params, config, params, env,
        from_node=from_node, only_node=only_node,
        dry_run=dry_run, list_nodes=list_nodes,
        retrain_advice={"models_dir": data_dir / "models", "model_version": mv},
    )
```

- [ ] **Step 3: Write the training pre-run stub**

In the `training` command, immediately **before** the `executed = _execute_pipeline(...)` call (after the `pipeline_kwargs = {"enable_calibration": enable_calibration}` line ~672), insert:

```python
    if not dry_run and not list_nodes:
        stub_kwargs = {
            "version": mv,
            "pipeline": "training",
            "parameters": params_training,
            "base_dataset_version": base_v,
            "train_variant_id": train_v,
        }
        if cal_v is not None:
            stub_kwargs["calibration_variant_id"] = cal_v
        _write_manifest_stub(data_dir / "models" / mv, stub_kwargs, run_context.run_id)
```

- [ ] **Step 4: Write the dataset pre-run stubs**

In the `dataset` command, immediately **before** the `executed = _execute_pipeline(...)` call (after the `pipeline_kwargs = {"enable_calibration": enable_calibration}` line ~523), insert:

```python
    if not dry_run and not list_nodes:
        stub_base_dir = data_dir / "dataset" / base_v
        _write_manifest_stub(stub_base_dir, {
            "version": base_v, "pipeline": "dataset", "parameters": params_dataset,
            "base_dataset_version": base_v,
            "feature_table_fingerprint": feature_table_fp,
        }, run_context.run_id)
        _write_manifest_stub(stub_base_dir / "train_variants" / train_v, {
            "version": train_v, "pipeline": "dataset", "parameters": params_dataset,
            "parent_version": base_v, "variant_kind": "train",
        }, run_context.run_id)
        if cal_v is not None:
            _write_manifest_stub(stub_base_dir / "calibration_variants" / cal_v, {
                "version": cal_v, "pipeline": "dataset", "parameters": params_dataset,
                "parent_version": base_v, "variant_kind": "calibration",
            }, run_context.run_id)
```

- [ ] **Step 5: Run the full fast unit suites to confirm no regression**

Run: `cd $WT && PYTHONPATH=src $PY -m pytest tests/test_cli.py tests/test_core/test_versioning.py -q`
Expected: PASS (all, including the pre-existing slicing/manifest tests — proves the new params/imports didn't break existing wiring).

- [ ] **Step 6: Commit**

```bash
cd $WT && git add src/recsys_tfb/__main__.py
git commit -m "feat(cli): training/dataset 寫 pre-run manifest stub + training 重訓 advisory"
```

---

### Task 8 (optional): Local dry-run smoke

Confirms the guard (no stub on `--dry-run`) and the advisory wiring against a real Spark session. Spark cold start is ~2–4 min, so run in the **background** and don't block. Requires the worktree's local data to exist (`scripts/local_spark_setup.py`); skip if not set up.

- [ ] **Step 1: Pre-flight (per CLAUDE.md worktree SOP)**

```bash
cd $WT && readlink .venv && $PY -V
PYTHONPATH=src $PY scripts/local_spark_setup.py --check-isolation
```
Expected: venv `Python 3.10.9`; isolation check exits 0. If data isn't built, run `PYTHONPATH=src $PY scripts/local_spark_setup.py` first.

- [ ] **Step 2: Dry-run the training pipeline (background)**

```bash
cd $WT && export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src $PY -m recsys_tfb training --from-node predict_and_write_test_predictions --dry-run --env local
```
Expected in logs: the `[plan]` slice plan prints; if the local model for the computed `model_version` is absent, the `[retrain] ...` WARN lines appear; and **no** `data/models/<mv>/manifest.json` is created by the dry-run (guard holds).

- [ ] **Step 3: No commit** (read-only verification).

---

## Self-Review

**1. Spec coverage** (against `2026-06-16-crash-safe-manifest-stub-design.md`):
- §3 Q1 stub / Q2 skip-if-present → Task 3. ✓
- §3 Q3 status running/completed → Tasks 1, 3, 4. ✓
- §3 Q4 training + dataset stub, no early symlink → Task 7 (stubs call `_write_manifest_stub`, which never writes a symlink). ✓
- §3 Q5 / §5 Tier 1 advisory (training-only, WARN, nearest version + diff hint, slice-plan trigger not status) → Tasks 2, 5, 6, 7. ✓
- §5 Tier 0 ("--dry-run 與正常 run 都看得到「將(重)訓」") → Task 7 Step 1 places the advisory before the `dry_run` early-return, so dry-run surfaces it; the generic `[plan]` listing already names auto-included nodes. ✓
- §8 容錯 (broken manifest skipped; None → omit nearest section) → Task 2 `test_skips_running_and_broken`, Task 5 `test_format_retrain_advisory_without_latest`. ✓
- §9 testing list → Tasks 1-6 tests. ✓
- §2 non-goal: post-run still overwrites (unchanged), no hard-fail/`--allow-retrain`, no promote guard → none added. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows the exact command and expected result. ✓

**3. Type consistency:** `_write_manifest_stub(version_dir, metadata_kwargs, run_id)`, `find_latest_completed_model_version(models_dir) -> (version, created_at)|None`, `_format_retrain_advisory(model_version, retrain_nodes, latest)`, `_maybe_warn_retrain(plan, retrain_advice{models_dir,model_version})` — names/signatures match across Tasks 2/3/5/6/7. `latest` is always `(version, created_at)` or `None` everywhere. ✓

---

## Execution Handoff

Implement with **superpowers:subagent-driven-development** (recommended) or **superpowers:executing-plans**. Tasks 1-7 are pure/`tmp_path` and fast; Task 8 is an optional background Spark smoke.
