# Staged Modeling PR-A（Stage-1 引擎）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 實作 `training.model_structure: staged` 的 Stage-1 引擎：依 partition_keys 分群、每群獨立訓練＋確定性 per-group HPO、`stage2=none` 端到端可跑（含 eval fail-fast／inference skip+WARN 分流與原子性 bundle）。

**Architecture:** 新增 `models/staged/` 子套件（分群、閘門、單群訓練、StagedModelAdapter）＋ training pipeline 的 staged 分支（單一 `train_staged_model` node 取代 prepare/tune/finalize/calibrate）；兩個 predict 節點加顯式 routed 分支。shared 路徑零行為變更（含 model_version hash 穩定性守衛）。

**Tech Stack:** Python 3.10.9、LightGBM 4.6.0、Optuna 4.5.0（in-memory study）、pandas 1.5.3、numpy；無新增依賴。

**Spec:** `docs/superpowers/specs/2026-07-23-staged-modeling-design.md`（D1–D15；本計畫實作其 PR-A 範圍）

---

## 與 spec 的兩處已核實偏差（執行者照本計畫做即可，偏差已回報使用者）

1. **spec §7「inference/evaluation 呼叫點不動」不成立**：兩個 predict 節點只把 numpy `X` 餵進 `model.predict(X)`，carry 欄位不在 `X` 內、無法 per-row 路由。修正＝兩個節點各加一個顯式 routed 分支（Task 9/10），DAG／catalog／`rank_predictions` 不動。
2. **spec §4「版本化零修改」補一個守衛**：`_model_version_payload` 在 `model_structure` 為 shared（或未設）時剔除 `model_structure`/`staged` 鍵，否則加新預設鍵會 orphan 所有既有 shared model_version（Task 1）。

## 執行前提（每個 task 開始前確認）

- Worktree root：`/Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling`（下稱 `<WT>`；所有絕對路徑都在其下）。
- 測試指令一律：
  ```bash
  cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
  PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <測試路徑> -q
  ```
  （裸 pytest 會抓到 main 的 src；見 CLAUDE.md Worktree 鐵則 5。）
- 非 Spark 測試加 `-m 'not spark'` 可避開 Spark session 啟動。
- main 有既知 failing/互擾測試：`docs/operations/known-pitfalls.md` §5；Task 0 先建 baseline，之後只比對增量。

## File Structure

```
src/recsys_tfb/models/staged/__init__.py     # re-export；匯入 adapter 觸發 registry 註冊
src/recsys_tfb/models/staged/_metrics.py     # binary_auc / binary_logloss（numpy，無 sklearn）
src/recsys_tfb/models/staged/partition.py    # 分群標籤 / slug / 確定性種子 / routing keys
src/recsys_tfb/models/staged/gates.py        # 訓練時資料閘（collect-all）
src/recsys_tfb/models/staged/train_stage1.py # 單群訓練＋per-group HPO（in-memory Optuna）
src/recsys_tfb/models/staged/adapter.py      # StagedModelAdapter（predict_routed / 原子 save / 驗證 load）
src/recsys_tfb/pipelines/training/staged.py  # train_staged_model node（編排＋跨群平行）
修改：
src/recsys_tfb/core/consistency.py           # A21 predicates ＋ docstring legend
src/recsys_tfb/core/versioning.py            # _model_version_payload shared 剔除守衛
conf/base/parameters_training.yaml           # model_structure + staged 預設區塊
src/recsys_tfb/models/__init__.py            # import staged adapter（registry side effect）
src/recsys_tfb/pipelines/training/pipeline.py # create_pipeline(model_structure=...) 分支
src/recsys_tfb/__main__.py                   # training() 讀 model_structure → pipeline_kwargs
src/recsys_tfb/pipelines/training/nodes.py   # predict_and_write_test_predictions routed 分支（raise）
src/recsys_tfb/pipelines/inference/nodes_spark.py # predict_scores routed 分支（skip+WARN+report）
src/recsys_tfb/pipelines/inference/pipeline.py    # predict_scores 增加第二輸出
conf/base/catalog.yaml                        # staged_missing_groups_report entry
測試：
tests/test_models/test_staged/test_metrics.py
tests/test_models/test_staged/test_partition.py
tests/test_models/test_staged/test_gates.py
tests/test_models/test_staged/test_train_stage1.py
tests/test_models/test_staged/test_adapter.py
tests/test_core/test_consistency.py（追加 TestStagedConfigA21）
tests/test_core/test_versioning.py（追加 shared payload 穩定性）
tests/test_pipelines/test_training/test_staged_pipeline.py
tests/test_pipelines/test_training/test_staged_node.py
tests/test_pipelines/test_training/test_predict_routed.py
tests/test_pipelines/test_inference/test_predict_scores_staged.py
```

共用測試 fixture 約定（多個測試檔重複使用時「重複貼」，不建 conftest helper——各檔自足）：

```python
import numpy as np
import pandas as pd

def make_group_pdf(n_per_group=60, groups=("A", "B"), seed=0, label_rate=0.3):
    """兩群、可分性弱的合成 pdf：欄位 f1,f2（特徵）、seg（分群鍵）、label。"""
    rng = np.random.default_rng(seed)
    frames = []
    for gi, g in enumerate(groups):
        n = n_per_group
        y = (rng.random(n) < label_rate).astype(int)
        f1 = rng.normal(loc=y * (0.5 + gi), scale=1.0, size=n)
        f2 = rng.normal(size=n)
        frames.append(pd.DataFrame(
            {"f1": f1, "f2": f2, "seg": g, "label": y}))
    return pd.concat(frames, ignore_index=True)

STAGE1_CFG = {
    "partition_keys": ["seg"],
    "objective": "binary",
    "hpo": {"n_trials": 0, "metric": "auc", "search_space": []},
    "params": {},
    "gates": {"max_groups": 50, "min_rows": 10,
              "min_positives": 3, "min_negatives": 3},
    "max_workers": 1,
}

BASE_ALGO_PARAMS = {
    "objective": "binary", "metric": "binary_logloss", "verbosity": -1,
    "num_threads": 1, "num_leaves": 7, "learning_rate": 0.2,
    "num_iterations": 30, "early_stopping_rounds": 10,
}
```

---

### Task 0: 基線與 pre-flight

**Files:** 無（只跑指令）

- [ ] **Step 1: worktree pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
```
Expected: pwd 含 `.worktrees/staged-modeling`；readlink 指向 main root `.venv`；`Python 3.10.9`。

- [ ] **Step 2: 建 baseline（背景跑，記錄輸出）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py tests/test_core/test_versioning.py \
  tests/test_pipelines/test_training/test_pipeline.py \
  tests/test_models/test_adapter.py -q 2>&1 | tail -5
```
Expected: 記下 pass/fail 數（main 既知 fail 見 known-pitfalls §5——`test_adapter.py::TestPrepareTrainInputsWeight` 兩個 weight-baking 測試歷史上曾 fail，若 fail 記入 baseline、不歸因給本 PR）。

- [ ] **Step 3: 無 commit（無檔案變更）**

---

### Task 1: config 預設 ＋ versioning 守衛 ＋ consistency A21

**Files:**
- Modify: `conf/base/parameters_training.yaml`（training 區塊尾端）
- Modify: `src/recsys_tfb/core/versioning.py:124`（`_model_version_payload`）
- Modify: `src/recsys_tfb/core/consistency.py`（docstring legend＋新 predicates＋`validate_config_consistency`）
- Test: `tests/test_core/test_versioning.py`、`tests/test_core/test_consistency.py`

- [ ] **Step 1: 寫 versioning 穩定性 failing test**

在 `tests/test_core/test_versioning.py` 追加：

```python
class TestStagedModelVersionStability:
    def _params(self, extra_training=None):
        training = {"algorithm": "lightgbm",
                    "algorithm_params": {"objective": "binary"}}
        if extra_training:
            training.update(extra_training)
        return {"training": training}

    def test_shared_payload_unchanged_by_staged_defaults(self):
        from recsys_tfb.core.versioning import _model_version_payload
        base = _model_version_payload(self._params())
        with_defaults = _model_version_payload(self._params({
            "model_structure": "shared",
            "staged": {"stage1": {"partition_keys": ["prod_name"]},
                       "stage2": {"mode": "none"}},
        }))
        assert base == with_defaults  # shared 時 staged 鍵不得進 hash payload

    def test_staged_payload_includes_staged_block(self):
        from recsys_tfb.core.versioning import _model_version_payload
        p = _model_version_payload(self._params({
            "model_structure": "staged",
            "staged": {"stage1": {"partition_keys": ["prod_name"]},
                       "stage2": {"mode": "none"}},
        }))
        assert p["training"]["model_structure"] == "staged"
        assert "staged" in p["training"]
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_versioning.py::TestStagedModelVersionStability -q
```
Expected: FAIL，`test_shared_payload_unchanged_by_staged_defaults` 的 `assert base == with_defaults` 失敗（payload 多了 model_structure/staged 鍵）。**若失敗訊息與此不同（例如 import error），停下回報，不要自行繼續。**

- [ ] **Step 3: 實作 `_model_version_payload` 守衛**

`src/recsys_tfb/core/versioning.py`，在 `_model_version_payload`（:124 起）現有邏輯**之後、return 之前**插入（以現檔實際變數名為準——payload 深拷貝變數若名為 `payload`／`params_copy` 就沿用它）：

```python
    # Staged-mode keys fold into the hash ONLY when the structure is staged.
    # Popping them for shared keeps every pre-existing shared model_version
    # byte-identical across this feature's rollout (pure-additive upgrade).
    training_block = payload.get("training", {})
    if training_block.get("model_structure", "shared") == "shared":
        training_block.pop("model_structure", None)
        training_block.pop("staged", None)
```

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2 指令。Expected: 2 passed。

- [ ] **Step 5: 寫 consistency A21 failing tests**

在 `tests/test_core/test_consistency.py` 追加（`_make_parameters` 為該檔既有 helper，沿用其現有簽名；若鍵名不同以現檔為準）：

```python
class TestStagedConfigA21:
    def _staged_params(self, **stage1_over):
        p = _make_parameters()  # 既有 helper：回傳合法 shared 全套 params
        stage1 = {"partition_keys": ["prod_name"], "objective": "binary",
                  "hpo": {"n_trials": 0, "metric": "auc", "search_space": []},
                  "params": {}, "gates": {"max_groups": 200, "min_rows": 200,
                                          "min_positives": 20, "min_negatives": 20},
                  "max_workers": 1}
        stage1.update(stage1_over)
        p["training"]["model_structure"] = "staged"
        p["training"]["staged"] = {"stage1": stage1, "stage2": {"mode": "none"}}
        p["training"]["calibration"] = {"enabled": False}
        return p

    def test_valid_staged_config_passes(self):
        validate_config_consistency(self._staged_params())

    def test_unknown_model_structure_rejected(self):
        p = self._staged_params()
        p["training"]["model_structure"] = "composite"
        with pytest.raises(ConfigConsistencyError, match="model_structure"):
            validate_config_consistency(p)

    def test_partition_key_label_rejected(self):
        p = self._staged_params(partition_keys=["label"])
        with pytest.raises(ConfigConsistencyError, match="partition_keys.*label"):
            validate_config_consistency(p)

    def test_partition_key_not_in_allowlist_rejected(self):
        p = self._staged_params(partition_keys=["no_such_col"])
        with pytest.raises(ConfigConsistencyError, match="no_such_col"):
            validate_config_consistency(p)

    def test_carry_column_partition_key_allowed(self):
        p = self._staged_params(partition_keys=["seg_col"])
        p["dataset"]["carry_columns"] = ["seg_col"]
        validate_config_consistency(p)

    def test_calibration_must_be_disabled(self):
        p = self._staged_params()
        p["training"]["calibration"] = {"enabled": True}
        with pytest.raises(ConfigConsistencyError, match="calibration"):
            validate_config_consistency(p)

    def test_stage2_mode_only_none_in_pr_a(self):
        p = self._staged_params()
        p["training"]["staged"]["stage2"]["mode"] = "lambdarank"
        with pytest.raises(ConfigConsistencyError, match="stage2"):
            validate_config_consistency(p)

    def test_stage1_objective_only_binary(self):
        p = self._staged_params(objective="lambdarank")
        with pytest.raises(ConfigConsistencyError, match="objective"):
            validate_config_consistency(p)

    def test_hpo_metric_allowlist(self):
        p = self._staged_params(
            hpo={"n_trials": 5, "metric": "rmse", "search_space": []})
        with pytest.raises(ConfigConsistencyError, match="metric"):
            validate_config_consistency(p)

    def test_negative_n_trials_rejected(self):
        p = self._staged_params(
            hpo={"n_trials": -1, "metric": "auc", "search_space": []})
        with pytest.raises(ConfigConsistencyError, match="n_trials"):
            validate_config_consistency(p)

    def test_shared_ignores_staged_block(self):
        p = self._staged_params()
        p["training"]["model_structure"] = "shared"
        p["training"]["staged"]["stage1"]["partition_keys"] = ["label"]
        validate_config_consistency(p)  # shared 時 staged 區塊不驗
```

注意 `match` 護欄（judgment-rubrics §2 假綠型 2）：每個 `match` pattern 先自問「現有 A1–A20 有沒有誰對同一份 config 也會 raise 且訊息含同字串」——`partition_keys`/`stage2`/`model_structure` 均為新詞，安全；`calibration`/`objective`/`metric`/`n_trials` 是舊詞，訊息裡**必須**帶 `A21` 前綴且 match 寫成上表的複合 pattern 驗證通過後，再手動確認 raise 訊息確實來自 A21（把新 predicate 註解掉重跑該測試應轉 FAIL）。

- [ ] **Step 6: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py::TestStagedConfigA21 -q
```
Expected: 多數 FAIL（valid config 可能誤過、reject 類全 FAIL 因 predicate 不存在——`DID NOT RAISE`）。

- [ ] **Step 7: 實作 A21 predicates**

`src/recsys_tfb/core/consistency.py`：

(a) docstring legend（:12–156 區段，A20 之後）追加：

```
A21  staged model-structure config (Layer-1).  model_structure ∈ {shared,
     staged}; when staged: stage1.objective == "binary"; stage2.mode == "none"
     (binary/lambdarank arrive with PR-B); calibration.enabled must be false;
     hpo.metric ∈ {auc, logloss}; hpo.n_trials >= 0; partition_keys non-empty,
     each ∈ {schema.item} ∪ dataset.carry_columns (mirrors A9a availability)
     and NOT a label/score/rank/time/entity role column (label-partitioning is
     target leakage; time/entity partitioning cannot route at inference).
     Alignment/comparability advisories are WARN-level (logged, not raised):
     partition_keys != sample_group_keys; stage2 none with item in keys.
```

(b) 新 predicate 函式（放在 `weight_key_columns_unavailable`（:430）附近，同風格）：

```python
def staged_config_errors(parameters: dict) -> list[str]:
    """A21: staged model-structure config invariants (structure subset)."""
    training = parameters.get("training", {}) or {}
    structure = training.get("model_structure", "shared")
    errors: list[str] = []
    if structure not in ("shared", "staged"):
        errors.append(
            f"A21: training.model_structure must be 'shared' or 'staged', "
            f"got {structure!r}"
        )
        return errors
    if structure == "shared":
        return errors  # staged 區塊在 shared 下不驗（版本化守衛同一語意）
    staged = training.get("staged") or {}
    stage1 = staged.get("stage1") or {}
    stage2 = staged.get("stage2") or {}
    if stage1.get("objective", "binary") != "binary":
        errors.append(
            "A21: staged.stage1.objective only accepts 'binary' "
            f"(got {stage1.get('objective')!r}; reserved key for future use)"
        )
    if stage2.get("mode", "none") != "none":
        errors.append(
            "A21: staged.stage2.mode only accepts 'none' in this release "
            f"(got {stage2.get('mode')!r}; binary/lambdarank arrive with the "
            "Stage-2 PR)"
        )
    if (training.get("calibration") or {}).get("enabled", False):
        errors.append(
            "A21: training.calibration.enabled must be false when "
            "model_structure is staged (lambdarank/staged scores are not "
            "calibrated probabilities; calibration is slated for removal)"
        )
    hpo = stage1.get("hpo") or {}
    metric = hpo.get("metric", "auc")
    if metric not in ("auc", "logloss"):
        errors.append(
            f"A21: staged.stage1.hpo.metric must be 'auc' or 'logloss', "
            f"got {metric!r}"
        )
    n_trials = hpo.get("n_trials", 0)
    if not isinstance(n_trials, int) or isinstance(n_trials, bool) or n_trials < 0:
        errors.append(
            f"A21: staged.stage1.hpo.n_trials must be a non-negative int, "
            f"got {n_trials!r}"
        )
    return errors


def staged_partition_key_errors(parameters: dict) -> list[str]:
    """A21: partition_keys allowlist (mirrors A9a availability semantics)."""
    training = parameters.get("training", {}) or {}
    if training.get("model_structure", "shared") != "staged":
        return []
    keys = (((training.get("staged") or {}).get("stage1") or {})
            .get("partition_keys") or [])
    if not keys:
        return ["A21: staged.stage1.partition_keys must be a non-empty list"]
    schema = get_schema(parameters)
    dataset_cfg = parameters.get("dataset", {}) or {}
    forbidden = {
        schema["label"]: "label", schema["score"]: "score",
        schema["rank"]: "rank", schema["time"]: "time",
        **{c: "entity" for c in schema["entity"]},
    }
    allowed = {schema["item"]} | set(dataset_cfg.get("carry_columns") or [])
    errors: list[str] = []
    for k in keys:
        if k in forbidden:
            errors.append(
                f"A21: staged.stage1.partition_keys contains {k!r} which is "
                f"the {forbidden[k]} role column — partitioning by "
                f"{forbidden[k]} is forbidden (label => target leakage; "
                "time/entity => unroutable at inference)"
            )
        elif k not in allowed:
            errors.append(
                f"A21: staged.stage1.partition_keys column {k!r} is not "
                "available in model_input — allowed: the item column "
                f"({schema['item']!r}) or dataset.carry_columns. Add it to "
                "dataset.carry_columns and re-run the dataset pipeline."
            )
    return errors


def staged_alignment_warnings(parameters: dict) -> list[str]:
    """A21 WARN-level advisories (returned as strings; caller logs, not raises)."""
    training = parameters.get("training", {}) or {}
    if training.get("model_structure", "shared") != "staged":
        return []
    staged = training.get("staged") or {}
    stage1 = staged.get("stage1") or {}
    keys = stage1.get("partition_keys") or []
    warnings: list[str] = []
    group_keys = parameters.get("dataset", {}).get("sample_group_keys") or []
    if group_keys and list(keys) != list(group_keys):
        warnings.append(
            f"A21-WARN: staged.stage1.partition_keys {keys} != "
            f"dataset.sample_group_keys {group_keys} — per-group sampling "
            "ratios may be non-uniform inside a stage-1 partition"
        )
    schema = get_schema(parameters)
    if (staged.get("stage2") or {}).get("mode", "none") == "none" \
            and schema["item"] in keys:
        warnings.append(
            "A21-WARN: stage2=none with an item-bearing partition key — "
            "cross-model scores are ranked within a query without a fusing "
            "stage; differing per-group sampling ratios bias comparability "
            "(experimental-comparison mode; see spec §2.3)"
        )
    return warnings
```

（`get_schema` 該模組既有 import；若無，比照 `weight_key_columns_unavailable` 的取得方式。）

(c) `validate_config_consistency`（:696）在既有 predicates 之後、raise 之前追加：

```python
    errors.extend(staged_config_errors(parameters))
    errors.extend(staged_partition_key_errors(parameters))
    for w in staged_alignment_warnings(parameters):
        logger.warning(w)
```

- [ ] **Step 8: 跑測試確認 pass ＋ mutation check**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py -q 2>&1 | tail -3
```
Expected: 全綠（含既有測試）。Mutation check：把 `validate_config_consistency` 內 `errors.extend(staged_partition_key_errors(parameters))` 這一行註解掉 → `test_partition_key_label_rejected` 與 `test_partition_key_not_in_allowlist_rejected` 必須轉紅；改回。

- [ ] **Step 9: 加 config 預設值**

`conf/base/parameters_training.yaml` 的 `training:` 區塊尾端（`feature_selection` 之後、與其同縮排層）追加：

```yaml
  # --- Staged modeling（兩階段建模；spec: docs/superpowers/specs/2026-07-23-staged-modeling-design.md）---
  # shared（現況單一模型）| staged（Stage-1 分群模型；stage2 本期僅 none）。
  # shared 時 staged 區塊完全忽略（不進 model_version hash，A21 不驗其內容）。
  model_structure: shared
  staged:
    stage1:
      partition_keys: [prod_name]   # 允許：schema.item 或 dataset.carry_columns 欄位（A21）
      objective: binary             # 預留鍵，本期只收 binary
      hpo:
        n_trials: 0                 # 0 = 不搜，直接用 params；每群獨立、確定性種子、無 resume
        metric: auc                 # auc | logloss（每群 train_dev 子集上評分）
        search_space: []            # 同 training.search_space 的 ParamSpec 格式
      params: {}                    # 覆蓋 algorithm_params 的每群基底參數
      gates:                        # 訓練時資料閘（fail-fast、collect-all；spec §9）
        max_groups: 200
        min_rows: 200
        min_positives: 20
        min_negatives: 20
      max_workers: 1                # 跨群平行度（群內 trial 一律序列）
    stage2:
      mode: none                    # 本期僅 none；binary | lambdarank 隨 PR-B
```

- [ ] **Step 10: 驗證 config 載入後 shared 全套仍過（回歸）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py tests/test_core/test_versioning.py -q 2>&1 | tail -3
```
Expected: 全綠（對照 Task 0 baseline）。

- [ ] **Step 11: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add conf/base/parameters_training.yaml src/recsys_tfb/core/consistency.py \
  src/recsys_tfb/core/versioning.py tests/test_core/test_consistency.py \
  tests/test_core/test_versioning.py && \
git commit -m "feat(staged): A21 config predicates + staged defaults + model_version shared-stability guard"
```

---

### Task 2: `_metrics.py`（numpy AUC / logloss）

**Files:**
- Create: `src/recsys_tfb/models/staged/__init__.py`（先建空檔）
- Create: `src/recsys_tfb/models/staged/_metrics.py`
- Test: `tests/test_models/test_staged/test_metrics.py`（＋空 `tests/test_models/test_staged/__init__.py`）

- [ ] **Step 1: 寫 failing test**

```python
import numpy as np
import pytest

from recsys_tfb.models.staged._metrics import binary_auc, binary_logloss


class TestBinaryAuc:
    def test_perfect_separation_is_one(self):
        y = np.array([0, 0, 1, 1])
        s = np.array([0.1, 0.2, 0.8, 0.9])
        assert binary_auc(y, s) == 1.0

    def test_reversed_is_zero(self):
        y = np.array([0, 0, 1, 1])
        s = np.array([0.9, 0.8, 0.2, 0.1])
        assert binary_auc(y, s) == 0.0

    def test_ties_average_rank(self):
        # 一正一負同分：AUC = 0.5（平手貢獻 0.5）
        y = np.array([0, 1])
        s = np.array([0.5, 0.5])
        assert binary_auc(y, s) == 0.5

    def test_single_class_returns_nan(self):
        assert np.isnan(binary_auc(np.array([1, 1]), np.array([0.2, 0.8])))

    def test_matches_bruteforce_pair_count(self):
        rng = np.random.default_rng(7)
        y = (rng.random(200) < 0.3).astype(int)
        s = rng.random(200)
        pos, neg = s[y == 1], s[y == 0]
        wins = (pos[:, None] > neg[None, :]).sum()
        ties = (pos[:, None] == neg[None, :]).sum()
        expected = (wins + 0.5 * ties) / (len(pos) * len(neg))
        assert binary_auc(y, s) == pytest.approx(expected, abs=1e-12)


class TestBinaryLogloss:
    def test_perfect_prediction_near_zero(self):
        y = np.array([0, 1])
        s = np.array([1e-9, 1 - 1e-9])
        assert binary_logloss(y, s) < 1e-6

    def test_uniform_prediction_is_log2(self):
        y = np.array([0, 1, 0, 1])
        s = np.full(4, 0.5)
        assert binary_logloss(y, s) == pytest.approx(np.log(2))

    def test_clips_extreme_scores(self):
        y = np.array([1])
        s = np.array([0.0])  # 未 clip 會是 inf
        assert np.isfinite(binary_logloss(y, s))
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models/test_staged/test_metrics.py -q
```
Expected: FAIL，`ModuleNotFoundError: No module named 'recsys_tfb.models.staged'`。

- [ ] **Step 3: 實作**

`src/recsys_tfb/models/staged/__init__.py`：

```python
"""Staged (two-stage) modeling: stage-1 per-partition models.

Design spec: docs/superpowers/specs/2026-07-23-staged-modeling-design.md
"""
```

`src/recsys_tfb/models/staged/_metrics.py`：

```python
"""Binary metrics on numpy arrays for stage-1 per-group HPO scoring.

No sklearn dependency (production: no additional packages). AUC is the
rank-based Mann-Whitney estimator with average ranks for ties — exact for
the pairwise definition, O(n log n).
"""

import numpy as np

_EPS = 1e-15


def _average_ranks(x: np.ndarray) -> np.ndarray:
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), dtype=np.float64)
    ranks[order] = np.arange(1, len(x) + 1, dtype=np.float64)
    _, inverse, counts = np.unique(x, return_inverse=True, return_counts=True)
    sums = np.bincount(inverse, weights=ranks)
    return (sums / counts)[inverse]


def binary_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """AUC via average ranks; NaN when only one class is present."""
    y = np.asarray(y_true).astype(bool)
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    ranks = _average_ranks(np.asarray(y_score, dtype=np.float64))
    return float(
        (ranks[y].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    )


def binary_logloss(y_true: np.ndarray, y_score: np.ndarray) -> float:
    """Mean binary cross-entropy; scores clipped to (eps, 1-eps)."""
    y = np.asarray(y_true, dtype=np.float64)
    p = np.clip(np.asarray(y_score, dtype=np.float64), _EPS, 1.0 - _EPS)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))
```

（同時建立空的 `tests/test_models/test_staged/__init__.py`。）

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2 指令。Expected: 8 passed。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/models/staged/ tests/test_models/test_staged/ && \
git commit -m "feat(staged): numpy binary_auc/binary_logloss for stage-1 HPO scoring"
```

---

### Task 3: `partition.py`（分群標籤 / slug / 種子 / routing keys）

**Files:**
- Create: `src/recsys_tfb/models/staged/partition.py`
- Test: `tests/test_models/test_staged/test_partition.py`

- [ ] **Step 1: 寫 failing test**

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.partition import (
    group_labels, group_seed, group_slug, routing_keys,
)


class TestGroupLabels:
    def test_single_key(self):
        pdf = pd.DataFrame({"seg": ["a", "b", "a"]})
        labels = group_labels(pdf, ["seg"])
        assert labels.tolist() == ["a", "b", "a"]

    def test_composite_key_pipe_joined(self):
        pdf = pd.DataFrame({"seg": ["a"], "prod": ["x"]})
        assert group_labels(pdf, ["seg", "prod"]).tolist() == ["a|x"]

    def test_missing_column_raises(self):
        with pytest.raises(KeyError):
            group_labels(pd.DataFrame({"seg": ["a"]}), ["nope"])


class TestRoutingKeys:
    def test_same_as_group_labels_as_numpy(self):
        pdf = pd.DataFrame({"seg": ["a", "b"]})
        keys = routing_keys(pdf, ["seg"])
        assert isinstance(keys, np.ndarray)
        assert keys.tolist() == ["a", "b"]


class TestGroupSlug:
    def test_safe_chars_kept_and_suffix_stable(self):
        assert group_slug("fund_stock") == group_slug("fund_stock")
        assert group_slug("fund_stock") != group_slug("fund_bond")

    def test_unsafe_chars_sanitized_but_distinct(self):
        # 消毒後字面相同的兩個 key 仍須因 crc 後綴而不同
        a, b = group_slug("a/b"), group_slug("a|b")
        assert "/" not in a and "|" not in b
        assert a != b


class TestGroupSeed:
    def test_deterministic_and_distinct(self):
        assert group_seed(42, "a") == group_seed(42, "a")
        assert group_seed(42, "a") != group_seed(42, "b")
        assert group_seed(41, "a") != group_seed(42, "a")

    def test_in_valid_range(self):
        s = group_seed(42, "any-key")
        assert 0 <= s < 2**31 - 1
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models/test_staged/test_partition.py -q
```
Expected: FAIL，`ModuleNotFoundError`（partition 不存在）。

- [ ] **Step 3: 實作**

`src/recsys_tfb/models/staged/partition.py`：

```python
"""Stage-1 partitioning: composite group labels, slugs, deterministic seeds.

Group labels reuse the '|'-joined composite-key convention from
io/extract._composite_key_series (same convention as sample_ratio_overrides
keys), so a partition key that equals sample_group_keys produces identical
group identities across sampling and staged training.
"""

import re
import zlib

import numpy as np
import pandas as pd

_SLUG_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def group_labels(pdf: pd.DataFrame, partition_keys: list) -> pd.Series:
    """Per-row group label ('|'-joined partition key values, as str)."""
    missing = [k for k in partition_keys if k not in pdf.columns]
    if missing:
        raise KeyError(
            f"partition key column(s) {missing} not in dataframe columns"
        )
    # lazy import 避免 io↔models 循環（同 lightgbm_adapter 的作法）
    from recsys_tfb.io.extract import _composite_key_series

    return _composite_key_series(pdf, list(partition_keys))


def routing_keys(pdf: pd.DataFrame, partition_keys: list) -> np.ndarray:
    """group_labels as a numpy object array (predict-side routing)."""
    return group_labels(pdf, partition_keys).to_numpy(dtype=object)


def group_slug(group_key: str) -> str:
    """Filesystem-safe, collision-safe directory name for one group."""
    sanitized = _SLUG_UNSAFE.sub("_", group_key)[:40]
    crc = zlib.crc32(group_key.encode("utf-8")) & 0xFFFFFFFF
    return f"{sanitized}_{crc:08x}"


def group_seed(base_seed: int, group_key: str) -> int:
    """Deterministic per-group sampler seed (spec §3.1: derived, distinct)."""
    crc = zlib.crc32(group_key.encode("utf-8")) & 0xFFFFFFFF
    return (int(base_seed) * 1_000_003 + crc) % (2**31 - 1)
```

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2。Expected: 9 passed。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/models/staged/partition.py tests/test_models/test_staged/test_partition.py && \
git commit -m "feat(staged): partition labels/slug/deterministic seed/routing keys"
```

---

### Task 4: `gates.py`（訓練時資料閘）

**Files:**
- Create: `src/recsys_tfb/models/staged/gates.py`
- Test: `tests/test_models/test_staged/test_gates.py`

- [ ] **Step 1: 寫 failing test**

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.gates import StagedGateError, check_stage1_gates

GATES = {"max_groups": 3, "min_rows": 4, "min_positives": 1, "min_negatives": 1}


def _split(labels, y):
    return pd.Series(labels), np.array(y)


class TestCheckStage1Gates:
    def test_healthy_groups_pass(self):
        tr = _split(["a"] * 4 + ["b"] * 4, [0, 1, 0, 1, 0, 1, 0, 1])
        dev = _split(["a", "a", "b", "b"], [0, 1, 0, 1])
        check_stage1_gates(tr, dev, GATES)  # 不 raise

    def test_too_many_groups_fails(self):
        tr = _split(["a", "b", "c", "d"], [0, 1, 0, 1])
        dev = tr
        with pytest.raises(StagedGateError, match="max_groups"):
            check_stage1_gates(tr, dev, GATES)

    def test_group_missing_positives_in_dev_fails(self):
        tr = _split(["a"] * 4, [0, 1, 0, 1])
        dev = _split(["a", "a"], [0, 0])  # dev 無正例
        with pytest.raises(StagedGateError, match="positives"):
            check_stage1_gates(tr, dev, GATES)

    def test_collect_all_reports_every_bad_group(self):
        tr = _split(["a"] * 4 + ["b"] * 2, [0, 0, 0, 0, 1, 1])
        # a: 無正例；b: 列數不足＋無負例 → 錯誤訊息須同時含 a 與 b
        dev = _split(["a", "b"], [0, 1])
        with pytest.raises(StagedGateError) as exc:
            check_stage1_gates(tr, dev, GATES)
        assert "'a'" in str(exc.value) and "'b'" in str(exc.value)

    def test_group_only_in_dev_fails(self):
        # dev 出現 train 沒有的群：無模型可訓，必須擋
        tr = _split(["a"] * 4, [0, 1, 0, 1])
        dev = _split(["a", "z"], [0, 1])
        with pytest.raises(StagedGateError, match="'z'"):
            check_stage1_gates(tr, dev, GATES)
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models/test_staged/test_gates.py -q
```
Expected: FAIL，`ModuleNotFoundError`（gates 不存在）。

- [ ] **Step 3: 實作**

`src/recsys_tfb/models/staged/gates.py`：

```python
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
```

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2。Expected: 5 passed。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/models/staged/gates.py tests/test_models/test_staged/test_gates.py && \
git commit -m "feat(staged): stage-1 training-time data gates (collect-all fail-fast)"
```

---

### Task 5: `train_stage1.py`（單群訓練＋確定性 per-group HPO）

**Files:**
- Create: `src/recsys_tfb/models/staged/train_stage1.py`
- Test: `tests/test_models/test_staged/test_train_stage1.py`

- [ ] **Step 1: 寫 failing test**

```python
import numpy as np
import pytest

from recsys_tfb.models.staged.train_stage1 import GroupResult, train_one_group

ALGO = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
        "num_threads": 1, "num_leaves": 7, "learning_rate": 0.2,
        "num_iterations": 30, "early_stopping_rounds": 10}

SPACE = [{"name": "num_leaves", "type": "int", "low": 3, "high": 15}]


def _data(seed=0, n=200):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.3).astype(int)
    X = np.column_stack([rng.normal(loc=y, scale=1.0, size=n), rng.normal(size=n)])
    w = np.ones(n)
    return X, y, w


class TestTrainOneGroupFixedParams:
    def test_returns_result_with_booster_and_meta(self):
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=80)
        r = train_one_group(
            group_key="a", X_tr=X, y_tr=y, w_tr=w, X_dev=Xd, y_dev=yd, w_dev=wd,
            algorithm_params=dict(ALGO), stage1_params={}, hpo_cfg={"n_trials": 0},
            categorical_indices=None, base_seed=42,
        )
        assert isinstance(r, GroupResult)
        assert r.group_key == "a"
        preds = r.adapter.predict(Xd)
        assert preds.shape == (len(Xd),)
        assert r.n_rows == len(X) and r.n_pos == int(y.sum())
        assert np.isfinite(r.score)

    def test_weights_reach_lgb_dataset(self):
        # 權重全 2.0 與全 1.0 對 logloss 訓練等價（均勻縮放），但把單一正例
        # 權重放大 1000 倍應顯著改變該點附近的預測 → 用可觀察行為驗權重有進去
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=80)
        boosted = w.copy()
        pos_idx = int(np.argmax(y))
        boosted[pos_idx] = 1000.0
        r_plain = train_one_group("a", X, y, w, Xd, yd, wd, dict(ALGO), {},
                                  {"n_trials": 0}, None, 42)
        r_boost = train_one_group("a", X, y, boosted, Xd, yd, wd, dict(ALGO), {},
                                  {"n_trials": 0}, None, 42)
        p_plain = r_plain.adapter.predict(X[pos_idx:pos_idx + 1])[0]
        p_boost = r_boost.adapter.predict(X[pos_idx:pos_idx + 1])[0]
        assert p_boost > p_plain  # 放大該正例權重 → 該點預測機率上升


class TestTrainOneGroupHpo:
    def _run(self, base_seed=42, group_key="a"):
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=120)
        return train_one_group(
            group_key, X, y, w, Xd, yd, wd, dict(ALGO), {},
            {"n_trials": 4, "metric": "auc", "search_space": list(SPACE)},
            None, base_seed,
        )

    def test_deterministic_same_seed_same_best_params(self):
        assert self._run().best_params == self._run().best_params

    def test_different_group_key_different_trajectory(self):
        # 種子由 group_key 派生：不同群的 trial 序列應不同
        # （比較各 trial 的採樣值序列，不比 best——best 可能巧合相同）
        r_a, r_b = self._run(group_key="a"), self._run(group_key="b")
        assert r_a.trial_values != r_b.trial_values

    def test_metric_logloss_direction(self):
        X, y, w = _data()
        Xd, yd, wd = _data(seed=1, n=120)
        r = train_one_group(
            "a", X, y, w, Xd, yd, wd, dict(ALGO), {},
            {"n_trials": 3, "metric": "logloss", "search_space": list(SPACE)},
            None, 42,
        )
        assert np.isfinite(r.score)  # score 記錄原始 metric（logloss 越小越好）

    def test_hpo_best_params_flow_into_final_adapter(self):
        r = self._run()
        assert set(r.best_params) == {"num_leaves"}
        assert 3 <= r.best_params["num_leaves"] <= 15
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models/test_staged/test_train_stage1.py -q
```
Expected: FAIL，`ModuleNotFoundError`（train_stage1 不存在）。

- [ ] **Step 3: 實作**

`src/recsys_tfb/models/staged/train_stage1.py`：

```python
"""Train one stage-1 group model, optionally with per-group HPO.

Determinism contract (spec §3.1): sampler seed derives from
(random_seed, group_key); trials run SEQUENTIALLY inside a group
(parallelism only across groups); in-memory Optuna study — no resume,
interruption restarts the whole search.
"""

import logging
import time
from dataclasses import dataclass, field

import lightgbm as lgb
import numpy as np
import optuna

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged._metrics import binary_auc, binary_logloss
from recsys_tfb.models.staged.partition import group_seed

logger = logging.getLogger(__name__)

# 靜音 per-trial INFO（每群 n_trials 條，N 群會刷爆 log）
optuna.logging.set_verbosity(optuna.logging.WARNING)


@dataclass
class GroupResult:
    group_key: str
    adapter: LightGBMAdapter
    best_params: dict
    score: float           # 該群 train_dev 上的原始 metric（auc 高好 / logloss 低好）
    metric: str
    n_rows: int
    n_pos: int
    train_seconds: float
    trial_values: list = field(default_factory=list)


def _fit_adapter(X_tr, y_tr, w_tr, X_dev, y_dev, params, categorical_indices):
    train_ds = lgb.Dataset(
        X_tr, label=y_tr, weight=w_tr,
        categorical_feature=categorical_indices, free_raw_data=False,
    )
    dev_ds = lgb.Dataset(
        X_dev, label=y_dev, reference=train_ds, free_raw_data=False,
    )
    adapter = LightGBMAdapter()
    adapter.train(
        X_tr, y_tr, X_dev, y_dev, dict(params),
        train_dataset=train_ds, val_dataset=dev_ds,
    )
    return adapter


def _score(metric: str, y_dev, preds) -> float:
    if metric == "logloss":
        return binary_logloss(y_dev, preds)
    return binary_auc(y_dev, preds)


def train_one_group(
    group_key: str,
    X_tr: np.ndarray, y_tr: np.ndarray, w_tr: np.ndarray,
    X_dev: np.ndarray, y_dev: np.ndarray, w_dev: np.ndarray,
    algorithm_params: dict,
    stage1_params: dict,
    hpo_cfg: dict,
    categorical_indices,
    base_seed: int,
) -> GroupResult:
    """Fixed-params train (n_trials=0) or sequential in-memory HPO then refit."""
    t0 = time.monotonic()
    metric = hpo_cfg.get("metric", "auc")
    n_trials = int(hpo_cfg.get("n_trials", 0))
    base_params = {**algorithm_params, **stage1_params,
                   "objective": "binary",
                   "seed": group_seed(base_seed, group_key)}

    best_params: dict = {}
    trial_values: list = []
    if n_trials > 0:
        # lazy import：search_space 機制屬 training pipeline，僅 HPO 用到
        from recsys_tfb.pipelines.training.search_space import build_trial_params

        search_space = hpo_cfg.get("search_space") or []
        sign = -1.0 if metric == "logloss" else 1.0

        def objective(trial):
            trial_params = build_trial_params(trial, search_space)
            trial_values.append(dict(trial_params))
            adapter = _fit_adapter(
                X_tr, y_tr, w_tr, X_dev, y_dev,
                {**base_params, **trial_params}, categorical_indices,
            )
            return sign * _score(metric, y_dev, adapter.predict(X_dev))

        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(
                seed=group_seed(base_seed, group_key)),
        )
        study.optimize(objective, n_trials=n_trials, n_jobs=1)  # 群內序列
        best_params = dict(study.best_params)

    adapter = _fit_adapter(
        X_tr, y_tr, w_tr, X_dev, y_dev,
        {**base_params, **best_params}, categorical_indices,
    )
    score = _score(metric, y_dev, adapter.predict(X_dev))
    return GroupResult(
        group_key=group_key, adapter=adapter, best_params=best_params,
        score=float(score), metric=metric,
        n_rows=int(len(y_tr)), n_pos=int(np.asarray(y_tr).sum()),
        train_seconds=time.monotonic() - t0, trial_values=trial_values,
    )
```

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2。Expected: 7 passed（LightGBM 訓練 7 群小資料，約十餘秒）。

- [ ] **Step 5: Mutation check（判準：因果鏈上不可省的一步）**

把 `TPESampler(seed=group_seed(base_seed, group_key))` 改成 `TPESampler()`（拿掉種子）→ `test_deterministic_same_seed_same_best_params` 應轉紅（可能需跑 2 次確認非巧合）；改回。再把 `weight=w_tr` 拿掉 → `test_weights_reach_lgb_dataset` 應轉紅；改回。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/models/staged/train_stage1.py tests/test_models/test_staged/test_train_stage1.py && \
git commit -m "feat(staged): per-group train + deterministic in-memory HPO (sequential trials)"
```

---

### Task 6: `adapter.py`（StagedModelAdapter：routed predict／原子 save／驗證 load）

**Files:**
- Create: `src/recsys_tfb/models/staged/adapter.py`
- Modify: `src/recsys_tfb/models/__init__.py`（追加 `from recsys_tfb.models.staged import adapter as _staged_adapter  # noqa: F401` 觸發註冊）
- Modify: `src/recsys_tfb/models/staged/__init__.py`（re-export）
- Test: `tests/test_models/test_staged/test_adapter.py`

- [ ] **Step 1: 寫 failing test**

```python
import json
import numpy as np
import pytest

from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
from recsys_tfb.models.base import get_adapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.adapter import (
    StagedMissingGroupError, StagedModelAdapter,
)


def _tiny_adapter(seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(80) < 0.4).astype(int)
    X = np.column_stack([rng.normal(loc=y, size=80), rng.normal(size=80)])
    a = LightGBMAdapter()
    a.train(X, y, None, None,
            {"objective": "binary", "verbosity": -1, "num_threads": 1,
             "num_leaves": 4, "num_iterations": 10,
             "early_stopping_rounds": 0})
    return a


def _staged(groups=("a", "b")):
    m = StagedModelAdapter()
    for i, g in enumerate(groups):
        m.add_group(g, _tiny_adapter(seed=i),
                    meta={"best_params": {}, "score": 0.5, "metric": "auc",
                          "n_rows": 80, "n_pos": 30, "train_seconds": 0.1})
    m.set_partition_keys(["seg"])
    return m


class TestPredictRouted:
    def test_routes_rows_to_own_group_model(self):
        m = _staged()
        X = np.random.default_rng(1).normal(size=(6, 2))
        keys = np.array(["a", "b", "a", "b", "a", "b"], dtype=object)
        scores, mask = m.predict_routed(X, keys, on_missing="raise")
        assert mask.all() and scores.shape == (6,)
        only_a, _ = m.predict_routed(X, np.array(["a"] * 6, dtype=object),
                                     on_missing="raise")
        # 同列不同群模型分數應不同（兩個模型不同 seed 訓練）
        assert not np.allclose(scores, only_a)

    def test_missing_group_raise_lists_counts(self):
        m = _staged()
        X = np.zeros((3, 2))
        keys = np.array(["a", "zz", "zz"], dtype=object)
        with pytest.raises(StagedMissingGroupError, match="'zz'.*2"):
            m.predict_routed(X, keys, on_missing="raise")

    def test_missing_group_skip_returns_mask_and_stats(self):
        m = _staged()
        X = np.zeros((3, 2))
        keys = np.array(["a", "zz", "zz"], dtype=object)
        scores, mask = m.predict_routed(X, keys, on_missing="skip")
        assert mask.tolist() == [True, False, False]
        assert np.isnan(scores[~mask]).all()
        assert m.last_missing_stats == {"zz": 2}

    def test_plain_predict_raises_guidance(self):
        with pytest.raises(NotImplementedError, match="predict_routed"):
            _staged().predict(np.zeros((1, 2)))


class TestSaveLoadBundle(object):
    def test_roundtrip_via_model_adapter_dataset(self, tmp_path):
        m = _staged()
        filepath = tmp_path / "v1" / "model.txt"
        ds = ModelAdapterDataset(filepath=str(filepath))
        ds.save(m)
        meta = json.loads((tmp_path / "v1" / "model_meta.json").read_text())
        assert meta["algorithm"] == "staged"
        loaded = ds.load()
        assert isinstance(loaded, StagedModelAdapter)
        X = np.random.default_rng(2).normal(size=(4, 2))
        keys = np.array(["a", "b", "a", "b"], dtype=object)
        s1, _ = m.predict_routed(X, keys, on_missing="raise")
        s2, _ = loaded.predict_routed(X, keys, on_missing="raise")
        np.testing.assert_allclose(s1, s2)

    def test_save_leaves_no_tmp_dir(self, tmp_path):
        filepath = tmp_path / "v1" / "model.txt"
        ModelAdapterDataset(filepath=str(filepath)).save(_staged())
        leftovers = [p for p in (tmp_path / "v1").iterdir()
                     if p.name.startswith("stage1") and p.name != "stage1"]
        assert leftovers == []

    def test_load_detects_missing_group_file(self, tmp_path):
        filepath = tmp_path / "v1" / "model.txt"
        ds = ModelAdapterDataset(filepath=str(filepath))
        ds.save(_staged())
        victim = next((tmp_path / "v1" / "stage1").glob("*.txt"))
        victim.unlink()
        with pytest.raises(ValueError, match="bundle"):
            ds.load()

    def test_load_detects_bundle_id_mismatch(self, tmp_path):
        # 模擬混血 bundle：index 是舊 run 的、stage1/ 是新 run 的
        filepath = tmp_path / "v1" / "model.txt"
        ds = ModelAdapterDataset(filepath=str(filepath))
        ds.save(_staged())
        stale_index = filepath.read_text()
        ds.save(_staged(groups=("a", "b")))  # 第二次 save（新 bundle_id）
        filepath.write_text(stale_index)     # index 換回舊的
        with pytest.raises(ValueError, match="bundle"):
            ds.load()


class TestRegistry:
    def test_staged_registered(self):
        assert get_adapter("staged") is not None
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models/test_staged/test_adapter.py -q
```
Expected: FAIL，`ModuleNotFoundError`（adapter 不存在）。

- [ ] **Step 3: 實作**

`src/recsys_tfb/models/staged/adapter.py`：

```python
"""StagedModelAdapter: N stage-1 boosters behind the ModelAdapter contract.

Bundle layout under the model_version dir (filepath = <dir>/model.txt):
    model.txt          groups index JSON — written LAST (= bundle commit mark)
    stage1/<slug>.txt  one LightGBM booster per group
    stage1/.bundle_id  uuid; must equal index["bundle_id"] at load

Atomicity (spec §4, three cheap moves): stage1 written to a tmp dir then
os.replace()'d into place; the index (model.txt) written last; load verifies
bundle_id + file set and fails fast on any mix.
"""

import json
import logging
import shutil
import uuid
from pathlib import Path

import numpy as np

from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.partition import group_slug

logger = logging.getLogger(__name__)

_INDEX_VERSION = 1


class StagedMissingGroupError(Exception):
    """Scoring rows reference partition groups with no trained model."""


class StagedModelAdapter(ModelAdapter):
    def __init__(self) -> None:
        self._groups: dict[str, LightGBMAdapter] = {}
        self._group_meta: dict[str, dict] = {}
        self._partition_keys: list[str] = []
        self.last_missing_stats: dict[str, int] = {}

    # ---- assembly（train_staged_model 編排用） ----
    def add_group(self, group_key: str, adapter: LightGBMAdapter,
                  meta: dict) -> None:
        self._groups[group_key] = adapter
        self._group_meta[group_key] = dict(meta)

    def set_partition_keys(self, partition_keys: list) -> None:
        self._partition_keys = list(partition_keys)

    @property
    def partition_keys(self) -> list[str]:
        return list(self._partition_keys)

    @property
    def group_keys(self) -> list[str]:
        return sorted(self._groups)

    # ---- predict ----
    def predict(self, X: np.ndarray) -> np.ndarray:
        raise NotImplementedError(
            "StagedModelAdapter cannot route from features alone; call "
            "predict_routed(X, keys, on_missing=...) with per-row partition "
            "key values (see pipelines' staged branches)."
        )

    def predict_routed(
        self, X: np.ndarray, keys: np.ndarray, on_missing: str = "raise",
    ) -> tuple[np.ndarray, np.ndarray]:
        """Route rows to their group's booster.

        Returns (scores, valid_mask); missing-group rows get NaN score and
        False mask. on_missing: "raise" (evaluation path) | "skip"
        (inference path; stats in self.last_missing_stats).
        """
        if on_missing not in ("raise", "skip"):
            raise ValueError(f"on_missing must be raise|skip, got {on_missing!r}")
        keys = np.asarray(keys, dtype=object)
        if len(keys) != len(X):
            raise ValueError(
                f"keys length {len(keys)} != X rows {len(X)}")
        scores = np.full(len(X), np.nan, dtype=np.float64)
        mask = np.zeros(len(X), dtype=bool)
        missing: dict[str, int] = {}
        for key in np.unique(keys):
            idx = keys == key
            adapter = self._groups.get(key)
            if adapter is None:
                missing[str(key)] = int(idx.sum())
                continue
            scores[idx] = adapter.predict(X[idx])
            mask[idx] = True
        self.last_missing_stats = missing
        if missing and on_missing == "raise":
            detail = ", ".join(
                f"{k!r}: {n} row(s)" for k, n in sorted(missing.items()))
            raise StagedMissingGroupError(
                f"{len(missing)} partition group(s) have no trained model "
                f"({detail}) — evaluation data should share the training "
                "sample_pool build; a gap here signals drift or a wrong "
                "model_version"
            )
        if missing:
            logger.warning(
                "staged predict: skipped %d group(s) / %d row(s) with no "
                "model: %s",
                len(missing), sum(missing.values()), sorted(missing),
            )
        return scores, mask

    # ---- persistence ----
    def save(self, filepath: str) -> None:
        if not self._groups:
            raise RuntimeError("No stage-1 groups to save.")
        index_path = Path(filepath)
        version_dir = index_path.parent
        version_dir.mkdir(parents=True, exist_ok=True)
        bundle_id = uuid.uuid4().hex
        tmp_dir = version_dir / f"stage1.tmp-{bundle_id}"
        tmp_dir.mkdir()
        slugs: dict[str, str] = {}
        for key, adapter in self._groups.items():
            slug = group_slug(key)
            slugs[key] = slug
            adapter.save(str(tmp_dir / f"{slug}.txt"))
        (tmp_dir / ".bundle_id").write_text(bundle_id)
        final_dir = version_dir / "stage1"
        if final_dir.exists():
            shutil.rmtree(final_dir)          # 舊（可能殘缺的）bundle 清掉
        tmp_dir.replace(final_dir)            # 原子發布
        index = {
            "index_version": _INDEX_VERSION,
            "bundle_id": bundle_id,
            "partition_keys": self._partition_keys,
            "groups": {
                key: {"slug": slugs[key], **self._group_meta.get(key, {})}
                for key in sorted(self._groups)
            },
        }
        tmp_index = version_dir / f"model.txt.tmp-{bundle_id}"
        tmp_index.write_text(json.dumps(index, indent=2, ensure_ascii=False))
        tmp_index.replace(index_path)         # index 最後寫＝bundle commit
        logger.info(
            "staged bundle saved: %d group(s), bundle_id=%s, dir=%s",
            len(self._groups), bundle_id, version_dir,
        )

    def load(self, filepath: str) -> None:
        index_path = Path(filepath)
        index = json.loads(index_path.read_text())
        stage1_dir = index_path.parent / "stage1"
        problems: list[str] = []
        id_file = stage1_dir / ".bundle_id"
        if not stage1_dir.is_dir():
            problems.append("stage1/ directory missing")
        elif not id_file.exists():
            problems.append("stage1/.bundle_id missing")
        elif id_file.read_text().strip() != index.get("bundle_id"):
            problems.append(
                "bundle_id mismatch between index and stage1/ (mixed bundle)")
        groups = index.get("groups", {})
        for key, meta in groups.items():
            if not (stage1_dir / f"{meta['slug']}.txt").exists():
                problems.append(f"model file missing for group {key!r}")
        if problems:
            raise ValueError(
                "staged bundle failed integrity check: " + "; ".join(problems)
            )
        self._groups = {}
        self._group_meta = {}
        for key, meta in groups.items():
            adapter = LightGBMAdapter()
            adapter.load(str(stage1_dir / f"{meta['slug']}.txt"))
            self._groups[key] = adapter
            self._group_meta[key] = {
                k: v for k, v in meta.items() if k != "slug"}
        self._partition_keys = list(index.get("partition_keys", []))

    # ---- 其餘 ModelAdapter 契約 ----
    def train(self, X_train, y_train, X_val, y_val, params: dict) -> None:
        raise NotImplementedError(
            "staged training is orchestrated by the train_staged_model node, "
            "not the adapter (needs per-row partition keys)."
        )

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        raise NotImplementedError(
            "per-group diagnostics arrive with the diagnostics PR (PR-C)."
        )

    def log_to_mlflow(self) -> None:
        logger.info(
            "staged adapter: mlflow model logging deferred to PR-C "
            "(%d group(s))", len(self._groups),
        )

    def prepare_train_inputs(self, *args, **kwargs):
        raise NotImplementedError(
            "staged mode does not use the shared lgb .bin prepare layer."
        )


ADAPTER_REGISTRY["staged"] = StagedModelAdapter
```

`src/recsys_tfb/models/__init__.py`：在既有 import 之後追加一行：

```python
from recsys_tfb.models.staged import adapter as _staged_adapter  # noqa: F401  (registry side effect)
```

`src/recsys_tfb/models/staged/__init__.py` 改為：

```python
"""Staged (two-stage) modeling: stage-1 per-partition models.

Design spec: docs/superpowers/specs/2026-07-23-staged-modeling-design.md
"""

from recsys_tfb.models.staged.adapter import (  # noqa: F401
    StagedMissingGroupError,
    StagedModelAdapter,
)
```

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2。Expected: 10 passed。另跑既有 adapter 測試確認 registry 改動無回歸：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models -q -m 'not spark' 2>&1 | tail -3
```
Expected: 與 Task 0 baseline 一致＋新增測試綠。

- [ ] **Step 5: Mutation check**

把 `load` 裡 bundle_id 比對那行改成恆真（`if False:`）→ `test_load_detects_bundle_id_mismatch` 轉紅；改回。把 `tmp_index.replace(index_path)` 改成直接 `index_path.write_text(...)`＋刪除 tmp 檔邏輯 → `test_save_leaves_no_tmp_dir` 仍綠（此 mutation 驗的是原子性，測試抓不到 mid-write crash——記錄為已知測試邊界，不追加測試）。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/models/staged/ src/recsys_tfb/models/__init__.py \
  tests/test_models/test_staged/test_adapter.py && \
git commit -m "feat(staged): StagedModelAdapter — routed predict, atomic bundle save, verified load"
```

---

### Task 7: `train_staged_model` node（編排＋跨群平行）

**Files:**
- Create: `src/recsys_tfb/pipelines/training/staged.py`
- Test: `tests/test_pipelines/test_training/test_staged_node.py`

- [ ] **Step 1: 寫 failing test**

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.gates import StagedGateError
from recsys_tfb.pipelines.training.staged import train_staged_model


def _write_parquet(tmp_path, name, pdf):
    p = tmp_path / f"{name}.parquet"
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


def _pdf(n_per_group=60, groups=("A", "B"), seed=0):
    rng = np.random.default_rng(seed)
    frames = []
    for gi, g in enumerate(groups):
        y = (rng.random(n_per_group) < 0.3).astype(int)
        frames.append(pd.DataFrame({
            "snap_date": "2026-01-01", "cust_id": np.arange(n_per_group),
            "prod_name": "p1",
            "f1": rng.normal(loc=y, size=n_per_group),
            "f2": rng.normal(size=n_per_group),
            "seg": g, "label": y,
        }))
    return pd.concat(frames, ignore_index=True)


def _parameters(**stage1_over):
    stage1 = {"partition_keys": ["seg"], "objective": "binary",
              "hpo": {"n_trials": 0, "metric": "auc", "search_space": []},
              "params": {}, "gates": {"max_groups": 10, "min_rows": 10,
                                      "min_positives": 3, "min_negatives": 3},
              "max_workers": 2}
    stage1.update(stage1_over)
    return {
        "random_seed": 42,
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"},
                   "categorical_values": {"prod_name": ["p1"]}},
        "dataset": {"carry_columns": ["seg"]},
        "training": {
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": "binary",
                                 "metric": "binary_logloss", "verbosity": -1,
                                 "num_threads": 1, "num_leaves": 5,
                                 "learning_rate": 0.2},
            "num_iterations": 20, "early_stopping_rounds": 5,
            "model_structure": "staged",
            "staged": {"stage1": stage1, "stage2": {"mode": "none"}},
        },
    }


PREPROC = {"feature_columns": ["f1", "f2"], "categorical_columns": [],
           "category_mappings": {}}


class TestTrainStagedModel:
    def test_returns_adapter_with_one_model_per_group(self, tmp_path):
        tr = _write_parquet(tmp_path, "train", _pdf(seed=0))
        dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=30, seed=1))
        model = train_staged_model(tr, dev, PREPROC, _parameters())
        assert isinstance(model, StagedModelAdapter)
        assert model.group_keys == ["A", "B"]
        assert model.partition_keys == ["seg"]

    def test_gate_failure_propagates(self, tmp_path):
        pdf = _pdf(seed=0)
        pdf.loc[pdf["seg"] == "B", "label"] = 0  # B 群無正例
        tr = _write_parquet(tmp_path, "train", pdf)
        dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=30, seed=1))
        with pytest.raises(StagedGateError, match="'B'"):
            train_staged_model(tr, dev, PREPROC, _parameters())

    def test_parallel_equals_sequential(self, tmp_path):
        tr = _write_parquet(tmp_path, "train",
                            _pdf(groups=("A", "B", "C"), seed=0))
        dev = _write_parquet(tmp_path, "dev",
                             _pdf(n_per_group=30, groups=("A", "B", "C"), seed=1))
        m_seq = train_staged_model(tr, dev, PREPROC,
                                   _parameters(max_workers=1))
        m_par = train_staged_model(tr, dev, PREPROC,
                                   _parameters(max_workers=3))
        X = np.random.default_rng(3).normal(size=(6, 2))
        keys = np.array(["A", "B", "C"] * 2, dtype=object)
        s1, _ = m_seq.predict_routed(X, keys, on_missing="raise")
        s2, _ = m_par.predict_routed(X, keys, on_missing="raise")
        np.testing.assert_allclose(s1, s2)  # 平行度不得影響結果（確定性）
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_staged_node.py -q
```
Expected: FAIL，`ImportError: cannot import name 'train_staged_model'`。

- [ ] **Step 3: 實作**

`src/recsys_tfb/pipelines/training/staged.py`：

```python
"""train_staged_model node: stage-1 per-group training orchestration.

Reads the SAME train/train_dev parquet handles as the shared path (spec D9),
slices per-group subsets in memory, runs data gates (fail-fast), then trains
each group (sequential trials inside a group; groups run on a size-aware
thread pool — LightGBM releases the GIL during training). Determinism does
not depend on scheduling: every group's seed derives from (random_seed,
group_key) alone.
"""

import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from recsys_tfb.io.extract import _pdf_to_X, _row_weights_from_pdf
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.gates import check_stage1_gates
from recsys_tfb.models.staged.partition import group_labels
from recsys_tfb.models.staged.train_stage1 import train_one_group

logger = logging.getLogger(__name__)


def _group_arrays(pdf: pd.DataFrame, labels: pd.Series, key: str,
                  preprocessor_metadata: dict, parameters: dict):
    sub = pdf.loc[(labels == key).to_numpy()]
    X = _pdf_to_X(sub, preprocessor_metadata, parameters)
    y = sub[parameters["schema"]["columns"]["label"]].values \
        if "columns" in parameters.get("schema", {}) \
        else sub[_label_col(parameters)].values
    w = _row_weights_from_pdf(sub, parameters, preprocessor_metadata)
    return X, y, w


def _label_col(parameters: dict) -> str:
    from recsys_tfb.core.schema import get_schema

    return get_schema(parameters)["label"]


def train_staged_model(
    train_parquet_handle,
    train_dev_parquet_handle,
    preprocessor_view: dict,
    parameters: dict,
) -> StagedModelAdapter:
    training = parameters["training"]
    stage1 = training["staged"]["stage1"]
    partition_keys = list(stage1["partition_keys"])
    base_seed = int(parameters.get("random_seed", 42))

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

    max_workers = max(1, int(stage1.get("max_workers", 1)))
    if max_workers == 1:
        results = [_train(k) for k in group_keys]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            results = list(pool.map(_train, group_keys))

    model = StagedModelAdapter()
    for r in results:
        model.add_group(r.group_key, r.adapter, meta={
            "best_params": r.best_params, "score": r.score,
            "metric": r.metric, "n_rows": r.n_rows, "n_pos": r.n_pos,
            "train_seconds": round(r.train_seconds, 3),
        })
        logger.info(
            "stage1 group %r: rows=%d pos=%d %s=%.5f best_params=%s (%.1fs)",
            r.group_key, r.n_rows, r.n_pos, r.metric, r.score,
            r.best_params, r.train_seconds,
        )
    model.set_partition_keys(partition_keys)
    return model
```

（實作時把 `_group_arrays` 的 label 取值統一走 `_label_col`——上面測試的 parameters 直接餵 `schema.columns` 結構，與 `get_schema` 消費一致；若 `get_schema` 需要完整 schema 區塊，以現檔 `core/schema.py:23` 的實際契約為準微調測試 fixture，**不得**改 `get_schema`。）

- [ ] **Step 4: 跑測試確認 pass**

同 Step 2。Expected: 3 passed。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/pipelines/training/staged.py \
  tests/test_pipelines/test_training/test_staged_node.py && \
git commit -m "feat(staged): train_staged_model node — gates, size-aware thread pool, deterministic assembly"
```

---

### Task 8: pipeline 分支 ＋ `__main__` 接線

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py:32`（`create_pipeline`）
- Modify: `src/recsys_tfb/__main__.py`（training() 於 :723–:754 一帶）
- Test: `tests/test_pipelines/test_training/test_staged_pipeline.py`

- [ ] **Step 1: 寫 failing test**

```python
from recsys_tfb.pipelines.training.pipeline import create_pipeline


def _node_names(pipeline):
    return [n.name for n in pipeline.nodes]


class TestStagedPipelineStructure:
    def test_shared_default_structure_unchanged(self):
        names = _node_names(create_pipeline(enable_calibration=False))
        assert "tune_hyperparameters" in names
        assert "train_staged_model" not in names

    def test_staged_replaces_hpo_and_finalize(self):
        names = _node_names(create_pipeline(
            enable_calibration=False, model_structure="staged"))
        assert "train_staged_model" in names
        for absent in ("tune_hyperparameters", "finalize_model",
                       "prepare_lgb_train_inputs", "calibrate_model",
                       "compute_shap_diagnostics", "log_experiment"):
            assert absent not in names, absent

    def test_staged_keeps_predict_and_map(self):
        names = _node_names(create_pipeline(
            enable_calibration=False, model_structure="staged"))
        assert "predict_and_write_test_predictions" in names
        assert "compute_test_mAP_spark" in names

    def test_staged_model_output_is_model(self):
        p = create_pipeline(enable_calibration=False, model_structure="staged")
        staged_node = next(n for n in p.nodes
                           if n.name == "train_staged_model")
        assert staged_node.outputs == ["model"]

    def test_staged_with_calibration_raises(self):
        import pytest
        with pytest.raises(ValueError, match="calibration"):
            create_pipeline(enable_calibration=True, model_structure="staged")
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_staged_pipeline.py -q
```
Expected: FAIL，`TypeError: create_pipeline() got an unexpected keyword argument 'model_structure'`。

- [ ] **Step 3: 實作 pipeline 分支**

`src/recsys_tfb/pipelines/training/pipeline.py`：`create_pipeline` 簽名改為
`def create_pipeline(enable_calibration: bool = False, model_structure: str = "shared") -> Pipeline:`，
函式開頭插入：

```python
    if model_structure == "staged":
        if enable_calibration:
            raise ValueError(
                "staged model_structure requires calibration disabled "
                "(A21 blocks this at CLI entry; direct callers get the "
                "same contract here)"
            )
        return _create_staged_pipeline()
```

同檔新增（import `train_staged_model` 於檔頭 `from recsys_tfb.pipelines.training.staged import train_staged_model`；node 定義**逐字複製**既有 shared 路徑的對應 Node 宣告——`select_features`、`cache_train_model_input`、`cache_train_dev_model_input`、`cache_test_model_input`、`persist_sample_weight_report`、`predict_and_write_test_predictions`、`compute_test_mAP_spark` 各自的 `Node(...)` 區塊，inputs/outputs 一字不改）：

```python
def _create_staged_pipeline() -> Pipeline:
    """Staged (stage2=none) training DAG — PR-A scope.

    Shared-path nodes reused verbatim: select_features, cache_{train,
    train_dev,test}_model_input, persist_sample_weight_report,
    predict_and_write_test_predictions, compute_test_mAP_spark.
    Excluded (PR-B/PR-C): prepare_lgb_train_inputs, tune_hyperparameters,
    finalize_model, calibrate_model, all diagnostics nodes, log_experiment
    (its inputs depend on diagnostics outputs).
    """
    return Pipeline([
        # …（逐字複製上述共用 Node 宣告）…
        Node(
            train_staged_model,
            inputs=[
                "train_parquet_handle", "train_dev_parquet_handle",
                "preprocessor_view", "parameters",
            ],
            outputs=["model"],
            name="train_staged_model",
        ),
        # …predict_and_write_test_predictions / compute_test_mAP_spark 複製區塊…
    ])
```

（節點順序照 shared 路徑；`cache_val_model_input` 不進 staged——val 只被 tune_hyperparameters 用，stage2=none 用不到。Node 建構子的參數形狀以現檔為準。）

- [ ] **Step 4: 實作 `__main__` 接線**

`src/recsys_tfb/__main__.py` `training()`：在 `enable_calibration` 讀取（:728）之後加：

```python
    model_structure = params_training.get("training", {}).get(
        "model_structure", "shared")
```

`pipeline_kwargs`（:754）改為：

```python
    pipeline_kwargs = {
        "enable_calibration": enable_calibration,
        "model_structure": model_structure,
    }
```

（`get_pipeline("training", **kwargs)` 直接轉發 kwargs，`create_pipeline` 新參數有預設值，其他 pipeline 不受影響；A21 在 :92 已擋 staged＋calibration 組合。）

- [ ] **Step 5: 跑測試確認 pass ＋ 回歸**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_staged_pipeline.py \
  tests/test_pipelines/test_training/test_pipeline.py -q 2>&1 | tail -3
```
Expected: 全綠（shared 結構測試不受影響）。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/pipelines/training/pipeline.py src/recsys_tfb/__main__.py \
  tests/test_pipelines/test_training/test_staged_pipeline.py && \
git commit -m "feat(staged): training pipeline staged branch + CLI model_structure threading"
```

---

### Task 9: eval 路徑 routed 分支（fail-fast）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:807`（`predict_and_write_test_predictions` 的 predict 呼叫段，:899–:901 一帶）
- Test: `tests/test_pipelines/test_training/test_predict_routed.py`

- [ ] **Step 1: 寫 failing test**

測試聚焦新抽出的 helper `_predict_for_partition`（把「staged 走 routed(raise)、shared 走 predict」的分支抽成可單測的純函式；避免整組 node 的 Spark/pyarrow 佈置）：

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.adapter import (
    StagedMissingGroupError, StagedModelAdapter,
)
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.pipelines.training.nodes import _predict_for_partition


def _tiny_lgb(seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(60) < 0.4).astype(int)
    X = np.column_stack([rng.normal(loc=y, size=60), rng.normal(size=60)])
    a = LightGBMAdapter()
    a.train(X, y, None, None, {"objective": "binary", "verbosity": -1,
                               "num_threads": 1, "num_leaves": 4,
                               "num_iterations": 8,
                               "early_stopping_rounds": 0})
    return a


PARAMS = {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                                 "item": "prod_name", "label": "label",
                                 "score": "score", "rank": "rank"},
                     "categorical_values": {"prod_name": ["p1"]}},
          "training": {"model_structure": "staged",
                       "staged": {"stage1": {"partition_keys": ["seg"]},
                                  "stage2": {"mode": "none"}}}}


class TestPredictForPartition:
    def _staged(self):
        m = StagedModelAdapter()
        m.add_group("A", _tiny_lgb(0), meta={})
        m.set_partition_keys(["seg"])
        return m

    def test_shared_adapter_uses_plain_predict(self):
        pdf = pd.DataFrame({"f1": [0.1], "f2": [0.2], "seg": ["A"]})
        X = pdf[["f1", "f2"]].values
        scores = _predict_for_partition(_tiny_lgb(), X, pdf, {})
        assert scores.shape == (1,)

    def test_staged_adapter_routes_by_partition_keys(self):
        pdf = pd.DataFrame({"f1": [0.1, 0.3], "f2": [0.2, 0.1],
                            "seg": ["A", "A"]})
        X = pdf[["f1", "f2"]].values
        scores = _predict_for_partition(self._staged(), X, pdf, PARAMS)
        assert scores.shape == (2,) and np.isfinite(scores).all()

    def test_staged_missing_group_raises(self):
        pdf = pd.DataFrame({"f1": [0.1], "f2": [0.2], "seg": ["ZZ"]})
        X = pdf[["f1", "f2"]].values
        with pytest.raises(StagedMissingGroupError, match="'ZZ'"):
            _predict_for_partition(self._staged(), X, pdf, PARAMS)
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_predict_routed.py -q
```
Expected: FAIL，`ImportError: cannot import name '_predict_for_partition'`。

- [ ] **Step 3: 實作**

`src/recsys_tfb/pipelines/training/nodes.py`：在 `predict_and_write_test_predictions` 附近新增 helper，並把 node 內的 `preds = model.predict(X)`（:899–:901 一帶）替換為 `preds = _predict_for_partition(model, X, pdf, parameters)`：

```python
def _predict_for_partition(model, X, pdf, parameters):
    """Shared: plain predict. Staged: route by per-row partition keys.

    Evaluation path uses on_missing="raise" (spec D11 分流): the test split
    shares the training sample_pool build, so an unseen group here signals
    drift or a wrong model_version — never silently drop scored rows.
    """
    # lazy import 避免 pipelines→models.staged 在非 staged 部署的載入成本
    from recsys_tfb.models.staged.adapter import StagedModelAdapter
    from recsys_tfb.models.staged.partition import routing_keys

    if isinstance(model, StagedModelAdapter):
        keys = routing_keys(pdf, model.partition_keys)
        scores, _mask = model.predict_routed(X, keys, on_missing="raise")
        return scores
    return model.predict(X)
```

（routing 鍵以 **adapter 自帶的 partition_keys** 為準（load 後可得），不再讀 parameters——避免「載入的模型」與「當前 config」不一致時路由錯欄位。）

- [ ] **Step 4: 跑測試確認 pass ＋ 既有 predict 測試回歸**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_predict_routed.py \
  tests/test_pipelines/test_training/test_predict_and_write_test_predictions.py \
  -q 2>&1 | tail -3
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/pipelines/training/nodes.py \
  tests/test_pipelines/test_training/test_predict_routed.py && \
git commit -m "feat(staged): eval predict path routes staged adapter with fail-fast missing groups"
```

---

### Task 10: inference 路徑 routed 分支（skip＋WARN＋結構化 report）

**Files:**
- Modify: `src/recsys_tfb/pipelines/inference/nodes_spark.py:138`（`predict_scores`）
- Modify: `src/recsys_tfb/pipelines/inference/pipeline.py`（predict_scores outputs）
- Modify: `conf/base/catalog.yaml`（新 entry）
- Test: `tests/test_pipelines/test_inference/test_predict_scores_staged.py`

- [ ] **Step 1: 寫 failing test**

同 Task 9 的策略：測新 helper `_predict_chunk_staged`（skip 分流＋統計聚合的純函式部分），不佈置 Spark：

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.pipelines.inference.nodes_spark import _predict_chunk_staged


def _tiny_lgb(seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(60) < 0.4).astype(int)
    X = np.column_stack([rng.normal(loc=y, size=60), rng.normal(size=60)])
    a = LightGBMAdapter()
    a.train(X, y, None, None, {"objective": "binary", "verbosity": -1,
                               "num_threads": 1, "num_leaves": 4,
                               "num_iterations": 8,
                               "early_stopping_rounds": 0})
    return a


def _staged():
    m = StagedModelAdapter()
    m.add_group("A", _tiny_lgb(0), meta={})
    m.set_partition_keys(["seg"])
    return m


class TestPredictChunkStaged:
    def test_known_groups_all_scored(self):
        pdf = pd.DataFrame({"f1": [0.1, 0.2], "f2": [0.0, 0.1],
                            "seg": ["A", "A"]})
        X = pdf[["f1", "f2"]].values
        scores, keep, missing = _predict_chunk_staged(_staged(), X, pdf)
        assert keep.all() and len(scores) == 2 and missing == {}

    def test_missing_group_rows_dropped_and_counted(self):
        pdf = pd.DataFrame({"f1": [0.1, 0.2, 0.3], "f2": [0.0] * 3,
                            "seg": ["A", "ZZ", "ZZ"]})
        X = pdf[["f1", "f2"]].values
        scores, keep, missing = _predict_chunk_staged(_staged(), X, pdf)
        assert keep.tolist() == [True, False, False]
        assert missing == {"ZZ": 2}
        assert np.isfinite(scores[keep]).all()

    def test_missing_partition_key_column_fails_fast(self):
        pdf = pd.DataFrame({"f1": [0.1], "f2": [0.0]})  # 無 seg 欄
        X = pdf[["f1", "f2"]].values
        with pytest.raises(KeyError, match="seg"):
            _predict_chunk_staged(_staged(), X, pdf)
```

- [ ] **Step 2: 跑測試確認 fail**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_inference/test_predict_scores_staged.py -q
```
Expected: FAIL，`ImportError: cannot import name '_predict_chunk_staged'`。

- [ ] **Step 3: 實作**

(a) `src/recsys_tfb/pipelines/inference/nodes_spark.py` 新增 helper：

```python
def _predict_chunk_staged(model, X, features_pdf):
    """Staged inference chunk: route, skip missing groups, count them.

    Returns (scores_for_kept_rows, keep_mask, missing_stats). Inference path
    uses on_missing="skip" (spec D11 分流): new partition values are a
    natural event for inference_population — drop, WARN, and report.
    """
    from recsys_tfb.models.staged.partition import routing_keys

    keys = routing_keys(features_pdf, model.partition_keys)
    scores, keep = model.predict_routed(X, keys, on_missing="skip")
    return scores[keep], keep, dict(model.last_missing_stats)
```

(b) `predict_scores`（:138）改動——chunk 迴圈內 `model.predict(...)` 呼叫處加分支（以現檔實際變數名為準；`features_pdf` 是該 chunk 的 pandas frame）：

```python
        from recsys_tfb.models.staged.adapter import StagedModelAdapter

        if isinstance(model, StagedModelAdapter):
            chunk_scores, keep_mask, chunk_missing = _predict_chunk_staged(
                model, X_chunk, features_pdf)
            features_pdf = features_pdf.loc[keep_mask.nonzero()[0]] \
                if not keep_mask.all() else features_pdf
            for g, n in chunk_missing.items():
                missing_stats[g] = missing_stats.get(g, 0) + n
            scores = chunk_scores
        else:
            scores = model.predict(X_chunk)
```

迴圈前初始化 `missing_stats: dict = {}`、`total_rows = 0`（累加每 chunk 列數）；迴圈後：

```python
    report = {
        "model_structure": "staged"
        if isinstance(model, StagedModelAdapter) else "shared",
        "missing_groups": missing_stats,
        "rows_skipped": int(sum(missing_stats.values())),
        "rows_total": int(total_rows),
    }
    if missing_stats:
        logger.warning(
            "predict_scores: %d group(s) had no stage-1 model — skipped %d/%d "
            "row(s): %s — the candidate universe SHRANK for affected "
            "entities; retrain to cover new groups",
            len(missing_stats), report["rows_skipped"], report["rows_total"],
            dict(sorted(missing_stats.items())),
        )
    return predictions_df, report
```

（`predictions_df` 為現函式原本的回傳值名，以現檔為準。）

(c) `src/recsys_tfb/pipelines/inference/pipeline.py`：`predict_scores` 的 Node `outputs` 從單一輸出改為 `[<原輸出名>, "staged_missing_groups_report"]`（原輸出名以現檔為準）。

(d) `conf/base/catalog.yaml` 追加（仿既有 JSONDataset entry 的縮排與鍵風格）：

```yaml
staged_missing_groups_report:
  type: JSONDataset
  filepath: data/inference/${model_version}/missing_groups.json
```

- [ ] **Step 4: 跑測試確認 pass ＋ inference 既有測試回歸**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_inference -q -m 'not spark' 2>&1 | tail -3
```
Expected: 新測試 3 passed；既有非 Spark inference 測試與 Task 0 baseline 一致。**注意**：`predict_scores` 改了輸出簽名，若既有測試直接呼叫它會壞——同步把該測試的解包改成兩值（行為不變的機械修正，並在 commit message 記明）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git add src/recsys_tfb/pipelines/inference/ conf/base/catalog.yaml \
  tests/test_pipelines/test_inference/test_predict_scores_staged.py && \
git commit -m "feat(staged): inference skip+WARN routing with structured missing-groups report"
```

---

### Task 11: 本機 e2e（staged none）＋ shared 零回歸 ＋ 效率量測

**Files:** 無新檔（跑指令與記錄）

- [ ] **Step 1: pre-flight（照 CLAUDE.md 指令塊逐行）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
grep -n "model_structure:" conf/base/parameters_training.yaml
```
Expected: 隔離閘 PASS；grep 印出 worktree 的 `model_structure: shared`。

- [ ] **Step 2: shared 零回歸（背景跑 dataset＋training）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```
（>2 分鐘 → `run_in_background`。）Expected: shared training 完跑、`data/models/<mv>/manifest.json` status=completed。記下 shared 的 model_version（此即「加了 staged 預設鍵後 shared hash 不變」的實跑對照——與 main 版本比對需相同 conf，僅在 config 未動時有意義；主要證據仍是 Task 1 的 payload 測試）。

- [ ] **Step 3: staged(none) e2e**

改 `conf/base/parameters_training.yaml`：`model_structure: staged`、`staged.stage1.hpo.n_trials: 3`、`search_space` 填一條（`- {name: num_leaves, type: int, low: 7, high: 31}`）、`gates` 按合成資料規模調鬆（`min_rows: 50, min_positives: 5, min_negatives: 5`）。

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
grep -n "model_structure:" conf/base/parameters_training.yaml && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```
（背景跑。）Expected：
- log 出現 `train_staged_model: N group(s)`（合成資料 prod_name 8 產品 → 8 群）與每群 `stage1 group ...` 摘要行；
- `data/models/<新mv>/stage1/` 含 8 個 `*.txt`＋`.bundle_id`；`model.txt` 是 groups index JSON；
- `predict_and_write_test_predictions` 正常完成（routed，無 missing）；
- `compute_test_mAP_spark` 產出 evaluation_results；
- 新 model_version ≠ Step 2 的 shared model_version。

- [ ] **Step 4: evaluation --post-training 相容 smoke**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training
```
（背景跑。）Expected: 讀 `training_eval_predictions` 產出 report，無錯——staged 對 eval 側是純預測值，理論相容在此實證。

- [ ] **Step 5: 效率量測（單群百萬列，spec §8 開放項）**

寫入 scratchpad（非 repo）一個量測腳本並跑：

```bash
cat > /private/tmp/claude-501/-Users-curtislu-projects-recsys-tfb/f26eda09-0422-409e-90f0-790598c443e5/scratchpad/bench_stage1.py <<'EOF'
import time, numpy as np, sys
sys.path.insert(0, "/Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling/src")
from recsys_tfb.models.staged.train_stage1 import train_one_group
rng = np.random.default_rng(0)
n = 1_000_000
y = (rng.random(n) < 0.05).astype(int)
X = rng.normal(size=(n, 40)).astype(np.float32)
X[:, 0] += y * 0.3
w = np.ones(n)
algo = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
        "num_threads": 4, "num_leaves": 63, "learning_rate": 0.1,
        "num_iterations": 200, "early_stopping_rounds": 30}
nd = 200_000
t0 = time.monotonic()
r = train_one_group("bench", X, y, w, X[:nd], y[:nd], w[:nd],
                    algo, {}, {"n_trials": 0, "metric": "auc"}, None, 42)
print(f"1M rows x 40 feats fixed-params: {time.monotonic()-t0:.1f}s auc={r.score:.4f}")
EOF
/Users/curtislu/projects/recsys_tfb/.venv/bin/python /private/tmp/claude-501/-Users-curtislu-projects-recsys-tfb/f26eda09-0422-409e-90f0-790598c443e5/scratchpad/bench_stage1.py
```
（背景跑。）Expected: 印出單群 1M×40 的訓練秒數（供 n_trials 預算與 max_workers 建議寫進 PR 描述；量測值本身無過/不過門檻）。

- [ ] **Step 6: 還原 config、graphify rebuild、收尾 commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
git checkout -- conf/base/parameters_training.yaml && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))" && \
git status --short
```
Expected: config 還原為 `model_structure: shared`；graphify 圖已更新；工作樹只剩 graphify-out 產物（按 repo 慣例處理）。

- [ ] **Step 7: 針對性測試總回歸（非全量）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_models tests/test_core \
  tests/test_pipelines/test_training tests/test_pipelines/test_inference \
  -q -m 'not spark' 2>&1 | tail -5
```
（背景跑。）Expected: 相對 Task 0 baseline 零新增 fail。

---

## 驗收清單（對照 spec §10 PR-A 行）

- [ ] shared 路徑 baseline 零 diff：Task 1 payload 測試＋Task 8 結構測試＋Task 11 Step 7 回歸。
- [ ] staged(none) 本機 e2e：Task 11 Step 3（訓練＋bundle＋test 預測＋mAP）＋ Step 4（eval 相容）。
- [ ] 確定性：Task 5 同種子同結果測試＋Task 7 平行度不變性測試。
- [ ] consistency allowlist＋資料閘：Task 1＋Task 4（含 mutation check 記錄）。
- [ ] 原子性三件：Task 6（tmp-dir rename、index 最後寫、load 驗證——含混血 bundle 測試）。
- [ ] 未見群分流：Task 9（eval raise）＋Task 10（inference skip＋WARN＋JSON report）。
- [ ] 效率量測：Task 11 Step 5 數字記入 PR 描述。

## 刻意不做（PR-A 邊界；spec §11 與 PR 切分）

- Stage-2（OOF、binary/lambdarank、Stage-2 HPO）→ PR-B。
- Stage-1 總覽表、per-group 診斷、log_experiment/MLflow 接回 → PR-C。
- 文件（training.md 等）→ PR-D。
- 分群鍵當 Stage-2 categorical 的編碼路徑 → PR-B（spec §12）。

## Self-Review（已執行）

1. **Spec 覆蓋**：PR-A 範圍逐項對到 Task（見驗收清單）；spec §9 predicate 5（calibration）與 2（objective）在 Task 1；資料閘 9–11 在 Task 4（fold 支援度屬 stage2≠none，PR-B）。
2. **Placeholder 掃描**：Task 8 的 `_create_staged_pipeline` 含「逐字複製既有 Node 宣告」指示——非 placeholder，是 DRY 上的刻意選擇（Node 宣告 40 行、以現檔為準複製比在計畫裡抄一份更不易漂移）；其餘步驟皆附完整程式碼與指令。
3. **型別/命名一致性**：`train_one_group` 簽名（Task 5 定義）與 Task 7 呼叫一致；`predict_routed(X, keys, on_missing)`（Task 6）與 Task 9/10 呼叫一致；`GroupResult.trial_values` 在 Task 5 測試與實作皆存在。
4. 已知風險標註：Task 7 的 `get_schema` fixture 契約、Task 10 的 `predict_scores` 既有測試解包——執行者遇到與預期不同的失敗訊息時停下回報。
