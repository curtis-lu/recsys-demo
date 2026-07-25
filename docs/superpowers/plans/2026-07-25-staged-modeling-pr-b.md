# Staged Modeling PR-B（Stage-2＋OOF）實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 PR-A 的 Stage-1 引擎上補齊 spec §10 PR-B：entity-hash K 折 OOF cross-fitting、stage2 binary／lambdarank 訓練、Stage-2 HPO 接現行 persistent-study 機制（resume／--fresh-hpo／搜尋診斷）、A21 放寬 `stage2.mode`、OOF 資料閘（spec §9 item 11）。

**Architecture:** OOF 與 Stage-2 全部收在新 node `train_stage2_model`（讀同一組 parquet handle，driver-local pandas/numpy）；Stage-2 特徵矩陣＝`[原始特徵 X | OOF stage-1 分數 | 分群 code]`（尾端追加，Stage-1 categorical 索引不動）；Stage-2 booster 掛進 `StagedModelAdapter`（`predict_routed` 內部合成，**兩個 predict 呼叫點零修改**）；HPO 重用 `hpo_resume`／`search_space`／`_hpo_score`／`write_hpo_diagnostics` **模組**而非 `tune_hyperparameters` 函式（shared 路徑零 regression）。

**Tech Stack:** Python 3.10.9、LightGBM 4.6.0、Optuna 4.5.0、pandas 1.5.3、numpy；無新依賴。

**Spec：** `docs/superpowers/specs/2026-07-23-staged-modeling-design.md`（§2.2 步驟 5、§3.2、§9 item 11、§10 PR-B 列）。
**基座：** branch `feat/staged-stage2`@6b5e2e2（＝PR #117 HEAD；#117 開著未 merge，本 PR 之後 base 設 `feat/staged-modeling`、待 #117 merge 後 retarget main）。

---

## 設計定案（寫碼前讀完；與 spec 的對應／偏離都在這裡）

**核實依據（2026-07-25 fact-check，檔案:行號皆已驗）：**
- carry 欄**不會**自動成為特徵：`feature_columns` 由 `preprocessing/_spark.py:112-133` 產生（feature_table 欄＋identity categoricals），sample_pool carry 欄不在其中、也沒有 `category_mappings`（`preprocessing/_spark.py:210-212`）。→ 分群鍵當 Stage-2 categorical 特徵**不走 preprocessor**，改在組表時自算 group code（PR#68 參考 `composite_train.py` 的 `stage2_matrix` 同路）。
- `extract_Xy_with_groups`（`io/extract.py:385-463`）回傳 per-row query-group id（`groupby([time]+entity).ngroup()`）；lambdarank 的 run-length counts 用 `core/group_utils.py:to_contiguous_groups(group_ids) -> (perm, counts)`。
- `tune_hyperparameters`（`nodes.py:389-616`）是 shared 專屬（ranking mAP objective、吃 lgb.Dataset handle）；persistent-study 機制在 `hpo_resume.py`（`hpo_study_dir/open_study/count_completed/clear_study_dir/write_checkpoint/load_checkpoint`）；HPO 搜尋診斷在函式體內 best-effort 呼叫 `write_hpo_diagnostics`（`nodes.py:605-614`）。
- OOF 參考＝`git show origin/feat/two-stage-stacking:src/recsys_tfb/models/composite_train.py`：`zlib.crc32(f"{site}|{seed}|{entity}") % n_folds`、`oof_is_leakage_clean`、fold 內 fit 以 train_dev early stop。

**D-B1（HPO 重用形狀）：** 不動 `tune_hyperparameters`。新寫 `tune_stage2`（`pipelines/training/staged_stage2.py`），重用 `hpo_resume` 全套＋`search_space.build_trial_params`＋`nodes._hpo_score`＋`write_hpo_diagnostics`。Stage-2 HPO 讀**既有 flat `training.*` 鍵**（`n_trials`／`search_space`／`hpo_objective`／`algorithm_params`／`num_iterations`／`early_stopping_rounds`）——spec §2.1「stage2 的超參／HPO 設定比照現行 shared 模式的既有鍵結構」的字面實現。好處：`compute_search_id`（去 n_trials）、`--fresh-hpo`、resume 文件語意全部原樣成立；`search_id` 只涵蓋 Stage-2 搜尋（spec §3.2）。staged config 在 model payload 內 → search_id 天然與任何 shared 設定不同，`_hpo/` 目錄無碰撞。
**D-B2（objective 來源）：** `stage2.mode` 是 objective 的唯一真實來源（binary→`binary`、lambdarank→`lambdarank`），**靜默覆蓋** `algorithm_params.objective`——與 Stage-1 既有行為對稱（`train_one_group` 已 `{**algorithm_params, "objective": "binary"}`）。lambdarank 時若 `staged.stage2.params` 未給 metric，強制 `metric="ndcg"`（`binary_logloss` 對 ranking 無效，early stopping 會靜默失義；同 `default_metric_for_objective` 的理由）。
**D-B3（Stage-2 矩陣）：** `X2 = [X | s1 | gcode]` 尾端追加 → Stage-1 categorical 索引原位有效；categorical＝原索引＋`[n_base+1]`（gcode 欄）。**不採** composite 參考的「刪 item 欄」（spec D4：全部原始特徵）。group code 契約＝`sorted(group_keys)` 的名次——**推導不落地**：排序確定、bundle 完整性檢查保證 save/load 群集合一致，train 與 inference 必然同編碼。
**D-B4（OOF fit 參數）：** 每群 fold fit 用該群 Stage-1 HPO 的 `best_params`（來自 `stage1_groups_report`）＋同一 `group_seed`，train_dev early stop（spec §2.2 步驟 5「各群以最佳參數做 K 折 OOF fit」）。參考實作的「fold 無正例 → fallback 群均值」**不採**——spec §9 item 11 的 gate fail-fast 取代它。
**D-B5（split 衛生）：** Stage-2 early stop **與** HPO trial 評分都用 val（spec §2.2／§3.1 明文；接受同 Stage-1 train_dev 的輕度選擇偏差）；test 兩階段不碰。trial 評分＝`_hpo_score`（`mean_ap`／`macro_per_item_map`，全局 ranking 指標，spec §3.2）。
**D-B6（OOF checkpoint，spec 外新增）：** 沿 PR-A 群級 checkpoint 方向（使用者 2026-07-24 指示「考慮中斷重來的成本」）：每群 OOF 分數向量落 `<wip>/<slug>/oof/`（scores.npy＋meta.json＋_SUCCESS），keyed by model_version；rerun 驗 n_rows/n_folds/seed 相符才還原。
**D-B7（entity 雜湊鍵）：** 多欄 entity schema 用 `_composite_key_series(pdf, schema["entity"])` 字串再 hash（參考實作只 hash `entity[0]`，多欄時會失去互斥保證）。fold site 字串＝`"staged_oof"`（與參考的 `"composite_oof"` 刻意不同，無跨相容意圖）。
**刻意不做（PR-B 範圍外）：** stage2 的 finalize 策略（refit_on_full）——用最佳 trial booster 直接上；staged DAG 的 pipeline slicing 接續契約（`stage1_model` 是 memory dataset）；diagnostics／log_experiment（PR-C）；公司環境驗證。

## File Structure

| 檔案 | 動作 | 職責 |
|---|---|---|
| `src/recsys_tfb/models/staged/oof.py` | Create | fold 分配（crc32）＋leakage guard（純 numpy） |
| `src/recsys_tfb/models/staged/gates.py` | Modify | 追加 `check_oof_gates`（spec §9 item 11） |
| `src/recsys_tfb/models/staged/stage2.py` | Create | group code、`stage2_matrix`、categorical 索引、`fit_stage2` 單次訓練 |
| `src/recsys_tfb/models/staged/adapter.py` | Modify | stage-2 booster：`set_stage2`／predict 合成／save/load 原子性與完整性 |
| `src/recsys_tfb/pipelines/training/staged_stage2.py` | Create | OOF 編排＋checkpoint、`tune_stage2` persistent HPO、`train_stage2_model` node |
| `src/recsys_tfb/core/consistency.py` | Modify | A21 放寬 stage2.mode＋`oof_folds` predicate（:507-512 一帶） |
| `src/recsys_tfb/pipelines/training/pipeline.py` | Modify | `_create_staged_pipeline(stage2_mode)` 分支接線 |
| `src/recsys_tfb/__main__.py` | Modify | 傳 `stage2_mode` pipeline kwarg |
| `conf/base/parameters_training.yaml` | Modify | stage2 預設塊補 `oof_folds`／`params` |
| `conf/base/catalog.yaml` | Modify | `stage2_report` JSONDataset |
| `tests/test_models/test_staged/test_oof.py` 等 | Create/Modify | 各 task 對應測試 |

## 執行前提（每個 subagent prompt 都要帶）

- Worktree：`/Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling`（branch `feat/staged-stage2`）。Edit/Write 絕對路徑必含 `.worktrees/staged-modeling`。
- 測試指令形：`cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling && PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <file> -x -q`
- main 既知 fail（**不得歸因本 PR、不得修**）：`tests/test_models/test_adapter.py::TestPrepareTrainInputsWeight` ×2、`tests/test_pipelines/test_inference/test_pipeline.py::test_pipeline_inputs`。
- TDD：先 RED（預期失敗訊息與計畫不符 → 停下回報）、GREEN、每 task 過後做 mutation check（弄壞因果鏈上不可省的一步，對應測試須轉紅）。
- 註解／訊息全繁體中文或英文，不出現簡體字。

---

### Task 1: A21 放寬 stage2.mode ＋ oof_folds predicate

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py:507-512`（`staged_config_errors` 內）
- Test: `tests/test_core/test_consistency.py`（`TestStagedConfigA21` 追加）

- [ ] **Step 1: 寫失敗測試**（加進既有 `TestStagedConfigA21` class；沿用該 class 既有的 staged 參數 fixture helper——先讀該 class 現況再插入，保持同一構造慣例）

```python
    # --- PR-B: stage2.mode 放寬 + oof_folds ---
    def test_stage2_binary_and_lambdarank_accepted(self):
        for mode in ("binary", "lambdarank"):
            params = self._staged_params()
            params["training"]["staged"]["stage2"] = {"mode": mode, "oof_folds": 5}
            assert staged_config_errors(params) == []

    def test_stage2_unknown_mode_rejected(self):
        params = self._staged_params()
        params["training"]["staged"]["stage2"] = {"mode": "rank_xendcg"}
        errs = staged_config_errors(params)
        assert any("stage2.mode" in e and "rank_xendcg" in e for e in errs)

    def test_oof_folds_must_be_int_ge_2_when_stage2_active(self):
        for bad in (1, 0, -3, True, "5", 2.0):
            params = self._staged_params()
            params["training"]["staged"]["stage2"] = {"mode": "binary",
                                                      "oof_folds": bad}
            errs = staged_config_errors(params)
            assert any("oof_folds" in e for e in errs), f"missed {bad!r}"

    def test_oof_folds_ignored_when_stage2_none(self):
        params = self._staged_params()
        params["training"]["staged"]["stage2"] = {"mode": "none", "oof_folds": 0}
        assert staged_config_errors(params) == []
```

（`_staged_params` 若不存在，取該 class 既有測試建構 staged 合法 config 的方式抽成 helper；不得改動既有測試的斷言。）

- [ ] **Step 2: 跑測試確認 RED**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_core/test_consistency.py -k "stage2_binary or unknown_mode or oof_folds" -q`
預期 RED 訊息：`test_stage2_binary_and_lambdarank_accepted` 失敗於 `assert [...] == []`，錯誤清單含現行文案 `"staged.stage2.mode only accepts 'none' in this release"`。**實際訊息不同 → 停下回報。**

- [ ] **Step 3: 實作**（以下整段**取代** consistency.py:507-512 的 `if stage2.get("mode", "none") != "none":` 區塊）

```python
    mode = stage2.get("mode", "none")
    if mode not in ("none", "binary", "lambdarank"):
        errors.append(
            f"A21: staged.stage2.mode must be 'none', 'binary' or "
            f"'lambdarank', got {mode!r}"
        )
    elif mode != "none":
        oof_folds = stage2.get("oof_folds", 5)
        if (not isinstance(oof_folds, int) or isinstance(oof_folds, bool)
                or oof_folds < 2):
            errors.append(
                f"A21: staged.stage2.oof_folds must be an int >= 2 when "
                f"stage2.mode != 'none', got {oof_folds!r}"
            )
```

同時：grep consistency.py 模組 docstring 的 Invariant legend——A21 條目若寫死「stage2 only none」，更新為「stage2.mode ∈ {none, binary, lambdarank}（PR-B 起）＋ oof_folds >= 2」。

- [ ] **Step 4: 跑測試確認 GREEN**（同 Step 2 指令＋整檔 `pytest tests/test_core/test_consistency.py -q` 全綠）
- [ ] **Step 5: mutation check**：把 `oof_folds < 2` 改成 `oof_folds < 0` → `test_oof_folds_must_be_int_ge_2_when_stage2_active` 轉紅（`bad=1` 漏抓）；改回。
- [ ] **Step 6: Commit** `git add -A && git commit -m "feat(staged): A21 放寬 stage2.mode 收 binary/lambdarank＋oof_folds>=2 predicate"`

---

### Task 2: OOF 折分配與 leakage guard（`models/staged/oof.py`）

**Files:**
- Create: `src/recsys_tfb/models/staged/oof.py`
- Test: `tests/test_models/test_staged/test_oof.py`

- [ ] **Step 1: 寫失敗測試**

```python
import numpy as np
import pytest

from recsys_tfb.models.staged.oof import assign_folds, oof_is_leakage_clean


class TestAssignFolds:
    def test_deterministic_and_in_range(self):
        ids = np.array([f"c{i}" for i in range(500)], dtype=object)
        a = assign_folds(ids, n_folds=5, seed=42)
        b = assign_folds(ids, n_folds=5, seed=42)
        np.testing.assert_array_equal(a, b)
        assert a.dtype == np.int64
        assert set(np.unique(a)) <= set(range(5))

    def test_entity_disjoint(self):
        # 同一 entity 的多列必落同折
        ids = np.array(["e1", "e2", "e1", "e3", "e2", "e1"], dtype=object)
        f = assign_folds(ids, n_folds=4, seed=7)
        assert f[0] == f[2] == f[5]
        assert f[1] == f[4]

    def test_seed_changes_assignment(self):
        ids = np.array([f"c{i}" for i in range(200)], dtype=object)
        assert not np.array_equal(assign_folds(ids, 5, seed=1),
                                  assign_folds(ids, 5, seed=2))

    def test_reasonably_balanced(self):
        ids = np.array([f"cust_{i}" for i in range(2000)], dtype=object)
        f = assign_folds(ids, n_folds=5, seed=42)
        counts = np.bincount(f, minlength=5)
        assert counts.min() > 0.5 * (2000 / 5)  # crc32 均勻性的寬鬆下界


class TestLeakageClean:
    def test_clean(self):
        folds = np.array([0, 1, 2, 0, 1])
        assert oof_is_leakage_clean(folds, folds.copy())

    def test_dirty_one_row(self):
        folds = np.array([0, 1, 2, 0, 1])
        producing = folds.copy()
        producing[3] = 1  # 這列被非自己折的模型評分
        assert not oof_is_leakage_clean(folds, producing)

    def test_length_mismatch_is_dirty(self):
        assert not oof_is_leakage_clean(np.array([0, 1]), np.array([0]))
```

- [ ] **Step 2: RED** — Run: `pytest tests/test_models/test_staged/test_oof.py -q`；預期 `ModuleNotFoundError: No module named 'recsys_tfb.models.staged.oof'`。
- [ ] **Step 3: 實作** `src/recsys_tfb/models/staged/oof.py`

```python
"""Stage-2 OOF cross-fitting: entity-hash folds + leakage guard (spec D5).

Folds are entity-disjoint via zlib.crc32 (IEEE-802.3, the same polynomial
family as Spark's F.crc32 used by the dataset split), keyed on the row's
entity identity string — callers with a multi-column entity schema pass the
'|'-joined composite string. Fold site "staged_oof" is deliberately distinct
from PR #68's reference ("composite_oof"): assignments are internal to this
design, no cross-compatibility intended.
"""

import zlib

import numpy as np

_FOLD_SITE = "staged_oof"


def assign_folds(entity_keys: np.ndarray, n_folds: int, seed: int) -> np.ndarray:
    """Deterministic, entity-disjoint fold index in [0, n_folds) per row.

    Hash computed once per distinct entity then broadcast —
    len(unique) << len(rows) at our scale (spec D12).
    """
    keys = np.asarray(entity_keys)
    uniq, inv = np.unique(keys, return_inverse=True)
    fold_of = np.array(
        [zlib.crc32(f"{_FOLD_SITE}|{seed}|{e}".encode()) % int(n_folds)
         for e in uniq],
        dtype=np.int64,
    )
    return fold_of[inv]


def oof_is_leakage_clean(folds: np.ndarray, producing_fold: np.ndarray) -> bool:
    """True iff every row was scored by its OWN fold's held-out booster
    (which trained on all OTHER folds)."""
    folds = np.asarray(folds)
    producing_fold = np.asarray(producing_fold)
    return bool(len(folds) == len(producing_fold)
                and np.all(producing_fold == folds))
```

- [ ] **Step 4: GREEN**（同 Step 2 指令）
- [ ] **Step 5: mutation check**：`% int(n_folds)` 改 `% (int(n_folds) + 1)` → in_range 測試轉紅；改回。
- [ ] **Step 6: Commit** `feat(staged): OOF entity-hash 折分配（crc32、fold site staged_oof）＋ leakage guard`

---

### Task 3: OOF 資料閘（spec §9 item 11）

**Files:**
- Modify: `src/recsys_tfb/models/staged/gates.py`（檔尾追加）
- Test: `tests/test_models/test_staged/test_gates.py`（追加 class）

- [ ] **Step 1: 寫失敗測試**

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.gates import StagedGateError, check_oof_gates


class TestCheckOofGates:
    def _mk(self, groups, y, folds):
        return pd.Series(groups), np.array(y), np.array(folds)

    def test_pass_when_every_fit_set_trainable(self):
        labels, y, folds = self._mk(
            ["A"] * 6, [1, 0, 1, 0, 1, 0], [0, 0, 1, 1, 2, 2])
        check_oof_gates(labels, y, folds, n_folds=3)  # 不 raise

    def test_fail_when_fit_set_loses_all_positives(self):
        # A 群唯一正例在 fold 0 → 評 fold 1/2 時 fit set 無正例... 錯：
        # 評 fold 0 時 fit set（fold1+2）無正例才是失敗點
        labels, y, folds = self._mk(
            ["A"] * 6, [1, 0, 0, 0, 0, 0], [0, 0, 1, 1, 2, 2])
        with pytest.raises(StagedGateError, match="no positives"):
            check_oof_gates(labels, y, folds, n_folds=3)

    def test_empty_heldout_fold_is_skipped(self):
        # fold 2 沒有 A 群的列 → 不需要 fold-2 模型，不算失敗
        labels, y, folds = self._mk(
            ["A"] * 4, [1, 0, 1, 0], [0, 0, 1, 1])
        check_oof_gates(labels, y, folds, n_folds=3)  # 不 raise

    def test_collect_all_lists_every_failure(self):
        labels, y, folds = self._mk(
            ["A"] * 4 + ["B"] * 4,
            [1, 0, 0, 0] + [0, 1, 1, 1],
            [0, 0, 1, 1] + [0, 0, 1, 1])
        with pytest.raises(StagedGateError) as ei:
            check_oof_gates(labels, y, folds, n_folds=2)
        msg = str(ei.value)
        assert "'A'" in msg and "'B'" in msg  # 兩群的失敗都列出
```

（`test_fail_when_fit_set_loses_all_positives` 的資料：A 群正例只有 index 0（fold 0）。評 fold 0 的 fit set＝fold 1+2 的列＝全負 → `no positives`。）

- [ ] **Step 2: RED** — 預期 `ImportError: cannot import name 'check_oof_gates'`。
- [ ] **Step 3: 實作**（gates.py 檔尾追加；docstring 首行後同步在模組 docstring 的「PR-A subset」字樣改為「PR-A/PR-B subset」）

```python
def check_oof_gates(labels, y, folds, n_folds: int) -> None:
    """Spec §9 item 11: every (group × held-out fold) must leave a trainable
    fit set — >= 1 positive AND >= 1 negative among that group's rows in the
    OTHER folds. Collect-all then raise ``StagedGateError``. A fold holding
    no rows of a group needs no booster for it and is skipped.
    """
    g = labels.to_numpy() if hasattr(labels, "to_numpy") else np.asarray(labels)
    y = np.asarray(y)
    folds = np.asarray(folds)
    errors: list[str] = []
    for key in np.unique(g):
        g_mask = g == key
        for k in range(int(n_folds)):
            held = g_mask & (folds == k)
            if not held.any():
                continue
            fit = g_mask & (folds != k)
            n_pos = int(y[fit].sum())
            n_neg = int(fit.sum()) - n_pos
            problems = []
            if n_pos < 1:
                problems.append("no positives")
            if n_neg < 1:
                problems.append("no negatives")
            if problems:
                errors.append(
                    f"group {key!r} OOF fold {k}: fit set has "
                    + ", ".join(problems)
                )
    if errors:
        raise StagedGateError(
            f"stage-2 OOF data gates failed ({len(errors)} issue(s)):\n- "
            + "\n- ".join(errors)
        )
```

- [ ] **Step 4: GREEN** — `pytest tests/test_models/test_staged/test_gates.py -q` 全檔綠。
- [ ] **Step 5: mutation check**：刪掉 `if not held.any(): continue` → `test_empty_heldout_fold_is_skipped` 仍綠？會——fold 2 的 fit set 有正負例。正確 mutation：把 `folds != k` 改成 `folds == k` → `test_pass_when_every_fit_set_trainable` 或 `test_fail...` 轉紅（fit/held 顛倒）。驗證後改回。
- [ ] **Step 6: Commit** `feat(staged): OOF 資料閘——每群每折 fit set 須可訓練（spec §9 item 11）`

---

### Task 4: Stage-2 特徵組裝與單次訓練（`models/staged/stage2.py`）

**Files:**
- Create: `src/recsys_tfb/models/staged/stage2.py`
- Test: `tests/test_models/test_staged/test_stage2.py`

- [ ] **Step 1: 寫失敗測試**

```python
import numpy as np
import pytest

from recsys_tfb.models.staged.stage2 import (
    encode_group_codes, fit_stage2, group_code_lookup,
    stage2_categorical_indices, stage2_matrix,
)


class TestGroupCodes:
    def test_lookup_is_sorted_rank(self):
        assert group_code_lookup(["b", "a", "c"]) == {"a": 0, "b": 1, "c": 2}

    def test_encode_maps_and_casts_float(self):
        codes = encode_group_codes(
            np.array(["b", "a", "b"], dtype=object), {"a": 0, "b": 1})
        np.testing.assert_array_equal(codes, [1.0, 0.0, 1.0])
        assert codes.dtype == np.float64

    def test_encode_unknown_key_raises(self):
        with pytest.raises(KeyError):
            encode_group_codes(np.array(["zz"], dtype=object), {"a": 0})


class TestStage2Matrix:
    def test_layout_x_then_s1_then_gcode(self):
        X = np.arange(6, dtype=float).reshape(3, 2)
        m = stage2_matrix(X, [0.1, 0.2, 0.3], [1.0, 0.0, 1.0])
        assert m.shape == (3, 4)
        np.testing.assert_array_equal(m[:, :2], X)
        np.testing.assert_allclose(m[:, 2], [0.1, 0.2, 0.3])  # s1 在 n_base
        np.testing.assert_array_equal(m[:, 3], [1.0, 0.0, 1.0])  # gcode 最後

    def test_categorical_indices_append_gcode(self):
        assert stage2_categorical_indices([0, 3], n_base_features=5) == [0, 3, 6]


def _toy(mode, n=240, seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.3).astype(int)
    X2 = np.column_stack([rng.normal(loc=y), rng.normal(size=n),
                          rng.random(n), rng.integers(0, 3, n).astype(float)])
    qg = np.repeat(np.arange(n // 4), 4)  # 每 query 4 列
    return X2, y, qg


PARAMS = {"objective": None, "verbosity": -1, "num_threads": 1,
          "num_leaves": 5, "learning_rate": 0.2,
          "num_iterations": 20, "early_stopping_rounds": 5}


class TestFitStage2:
    def test_binary_trains_and_predicts_finite(self):
        X2, y, qg = _toy("binary")
        params = {**PARAMS, "objective": "binary", "metric": "binary_logloss"}
        adapter = fit_stage2("binary", X2, y, None, qg, X2, y, qg,
                             params, [3])
        preds = adapter.predict(X2)
        assert np.isfinite(preds).all() and len(preds) == len(y)

    def test_lambdarank_trains_with_query_groups(self):
        X2, y, qg = _toy("lambdarank")
        params = {**PARAMS, "objective": "lambdarank", "metric": "ndcg"}
        adapter = fit_stage2("lambdarank", X2, y, None, qg, X2, y, qg,
                             params, [3])
        preds = adapter.predict(X2)
        assert np.isfinite(preds).all() and len(preds) == len(y)

    def test_lambdarank_weight_perm_aligned(self, monkeypatch):
        # 結構性驗證：ranking 分支的 weight 必須跟著 perm 重排（shared prepare
        # 層同款契約）。spy lgb.Dataset 抓 weight 與 label 的對應。
        import lightgbm as lgb
        captured = {}
        real_dataset = lgb.Dataset

        def spy(data, label=None, weight=None, group=None, **kw):
            if group is not None:  # 只抓 train ds
                captured["label"] = np.asarray(label)
                captured["weight"] = None if weight is None else np.asarray(weight)
            return real_dataset(data, label=label, weight=weight,
                                group=group, **kw)

        monkeypatch.setattr(
            "recsys_tfb.models.staged.stage2.lgb.Dataset", spy)
        X2, y, qg = _toy("lambdarank")
        w = y * 10.0 + 1.0  # weight 與 label 完全相關 → 可驗對齊
        params = {**PARAMS, "objective": "lambdarank", "metric": "ndcg"}
        fit_stage2("lambdarank", X2, y, w, qg, X2, y, qg, params, [3])
        np.testing.assert_allclose(
            captured["weight"], captured["label"] * 10.0 + 1.0)

    def test_unknown_mode_raises(self):
        X2, y, qg = _toy("binary")
        with pytest.raises(ValueError, match="binary|lambdarank"):
            fit_stage2("rank_xendcg", X2, y, None, qg, X2, y, qg, PARAMS, [])
```

- [ ] **Step 2: RED** — 預期 `ModuleNotFoundError: No module named 'recsys_tfb.models.staged.stage2'`。
- [ ] **Step 3: 實作** `src/recsys_tfb/models/staged/stage2.py`

```python
"""Stage-2 feature assembly + single-fit helper (spec D4).

Stage-2 matrix layout: ``[original features X | stage-1 score | group code]``.
Appending at the END keeps stage-1's categorical feature indices valid; the
group-code column is itself declared categorical.

Group code contract: code = rank of the group key in ``sorted(group_keys)``.
Derived, not persisted — sorted order is deterministic and the bundle
integrity check guarantees train/load see the same key set, so training and
inference always encode identically.
"""

import lightgbm as lgb
import numpy as np

from recsys_tfb.core.group_utils import to_contiguous_groups
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


def group_code_lookup(group_keys) -> dict:
    return {k: i for i, k in enumerate(sorted(group_keys))}


def encode_group_codes(keys: np.ndarray, lookup: dict) -> np.ndarray:
    """Map routing keys -> float codes. KeyError on an unknown key is
    deliberate: callers route/skip missing groups BEFORE encoding."""
    return np.array([lookup[k] for k in keys], dtype=np.float64)


def stage2_matrix(X, s1_scores, gcodes) -> np.ndarray:
    return np.column_stack([
        np.asarray(X, dtype=np.float64),
        np.asarray(s1_scores, dtype=np.float64),
        np.asarray(gcodes, dtype=np.float64),
    ])


def stage2_categorical_indices(base_cat_idx, n_base_features: int) -> list:
    """Stage-1 categorical indices stay valid; add the gcode column.
    Column order: [0..n_base-1]=X, n_base=s1 score, n_base+1=gcode."""
    return list(base_cat_idx) + [int(n_base_features) + 1]


def fit_stage2(
    mode: str,
    X2_tr, y_tr, w_tr, qgroups_tr,
    X2_val, y_val, qgroups_val,
    params: dict, categorical_indices,
) -> LightGBMAdapter:
    """One stage-2 fit, early-stopped on the provided validation set.

    ``qgroups_*``: per-row query-group ids (``extract_Xy_with_groups``
    convention); only consumed for lambdarank. Weights ride per-row and are
    perm-aligned for the ranking branch (mirrors the shared prepare layer).
    """
    if mode == "lambdarank":
        perm, counts = to_contiguous_groups(np.asarray(qgroups_tr))
        train_ds = lgb.Dataset(
            X2_tr[perm], label=np.asarray(y_tr)[perm],
            weight=None if w_tr is None else np.asarray(w_tr)[perm],
            group=counts, categorical_feature=list(categorical_indices),
            free_raw_data=False,
        )
        permv, countsv = to_contiguous_groups(np.asarray(qgroups_val))
        val_ds = lgb.Dataset(
            X2_val[permv], label=np.asarray(y_val)[permv], group=countsv,
            reference=train_ds, free_raw_data=False,
        )
    elif mode == "binary":
        train_ds = lgb.Dataset(
            X2_tr, label=y_tr, weight=w_tr,
            categorical_feature=list(categorical_indices),
            free_raw_data=False,
        )
        val_ds = lgb.Dataset(
            X2_val, label=y_val, reference=train_ds, free_raw_data=False,
        )
    else:
        raise ValueError(
            f"stage2 mode must be 'binary' or 'lambdarank', got {mode!r}")
    adapter = LightGBMAdapter()
    adapter.train(
        None, None, None, None, dict(params),
        train_dataset=train_ds, val_dataset=val_ds,
    )
    return adapter
```

- [ ] **Step 4: GREEN**
- [ ] **Step 5: mutation check**：`fit_stage2` ranking 分支把 `weight=... [perm]` 的 `[perm]` 拿掉 → `test_lambdarank_weight_perm_aligned` 須轉紅（weight 沒重排、與 label 對不上）。若仍綠＝spy 沒抓對，先修測試。改回。
- [ ] **Step 6: Commit** `feat(staged): stage-2 特徵組裝（[X|s1|gcode] 尾端追加）＋單次訓練 helper`

---

### Task 5: Adapter stage-2 支援（predict 合成＋save/load 原子性）

**Files:**
- Modify: `src/recsys_tfb/models/staged/adapter.py`
- Test: `tests/test_models/test_staged/test_adapter.py`（追加 class）

- [ ] **Step 1: 寫失敗測試**（沿用該檔既有的 fake/小型 LightGBM adapter 建構慣例——先讀檔，用同款 helper；下面的 `_FakeStage2` 自足）

```python
class _FakeStage2:
    """記錄輸入矩陣的假 stage-2 adapter（save/load 走真 LightGBM 的測試另計）。"""
    def __init__(self):
        self.seen = None
    def predict(self, X2):
        self.seen = np.asarray(X2)
        return np.full(len(X2), 7.0)


class TestStage2Composition:
    def test_predict_routed_feeds_stage2_matrix(self, two_group_adapter):
        # two_group_adapter: 既有 helper 建的 A/B 兩群 adapter（真 booster）
        model = two_group_adapter
        fake = _FakeStage2()
        model.set_stage2(fake, {"mode": "binary", "oof_folds": 3})
        X = np.random.default_rng(0).normal(size=(6, 2))
        keys = np.array(["A", "B", "A", "B", "A", "B"], dtype=object)
        scores, mask = model.predict_routed(X, keys, on_missing="raise")
        assert mask.all()
        np.testing.assert_array_equal(scores, 7.0)      # 全走 stage-2
        assert fake.seen.shape == (6, 4)                 # X(2)+s1+gcode
        np.testing.assert_array_equal(
            fake.seen[:, 3], [0, 1, 0, 1, 0, 1])        # gcode=sorted rank
        assert np.isfinite(fake.seen[:, 2]).all()        # s1 分數已填入

    def test_skip_mode_missing_rows_stay_nan(self, two_group_adapter):
        model = two_group_adapter
        model.set_stage2(_FakeStage2(), {"mode": "binary"})
        X = np.zeros((3, 2))
        keys = np.array(["A", "ZZ", "B"], dtype=object)
        scores, mask = model.predict_routed(X, keys, on_missing="skip")
        assert not mask[1] and np.isnan(scores[1])
        assert mask[0] and mask[2] and (scores[[0, 2]] == 7.0).all()

    def test_stage2_mode_property(self, two_group_adapter):
        assert two_group_adapter.stage2_mode == "none"
        two_group_adapter.set_stage2(_FakeStage2(), {"mode": "lambdarank"})
        assert two_group_adapter.stage2_mode == "lambdarank"


class TestStage2Persistence:
    def test_save_load_roundtrip_with_stage2(self, tmp_path, two_group_adapter,
                                             real_stage2_adapter):
        # real_stage2_adapter: 以 4 欄 X2 訓練的真 LightGBMAdapter（helper 建）
        model = two_group_adapter
        model.set_stage2(real_stage2_adapter, {"mode": "binary",
                                               "oof_folds": 3})
        fp = tmp_path / "mv" / "model.txt"
        model.save(str(fp))
        assert (tmp_path / "mv" / "stage2" / "model.txt").exists()
        assert (tmp_path / "mv" / "stage2" / ".bundle_id").exists()
        loaded = StagedModelAdapter()
        loaded.load(str(fp))
        assert loaded.stage2_mode == "binary"
        X = np.random.default_rng(1).normal(size=(4, 2))
        keys = np.array(["A", "B", "A", "B"], dtype=object)
        s0, _ = model.predict_routed(X, keys)
        s1, _ = loaded.predict_routed(X, keys)
        np.testing.assert_allclose(s0, s1)

    def test_load_fails_on_stage2_bundle_id_mismatch(self, tmp_path,
                                                     two_group_adapter,
                                                     real_stage2_adapter):
        model = two_group_adapter
        model.set_stage2(real_stage2_adapter, {"mode": "binary"})
        fp = tmp_path / "mv" / "model.txt"
        model.save(str(fp))
        (tmp_path / "mv" / "stage2" / ".bundle_id").write_text("tampered")
        with pytest.raises(ValueError, match="stage2"):
            StagedModelAdapter().load(str(fp))

    def test_save_without_stage2_removes_stale_dir(self, tmp_path,
                                                   two_group_adapter,
                                                   real_stage2_adapter):
        model = two_group_adapter
        model.set_stage2(real_stage2_adapter, {"mode": "binary"})
        fp = tmp_path / "mv" / "model.txt"
        model.save(str(fp))
        fresh = StagedModelAdapter()          # 無 stage-2 的新 bundle
        for k in model.group_keys:
            fresh.add_group(k, model._groups[k], meta={})
        fresh.set_partition_keys(model.partition_keys)
        fresh.save(str(fp))
        assert not (tmp_path / "mv" / "stage2").exists()  # 殘留清掉
        loaded = StagedModelAdapter()
        loaded.load(str(fp))                  # 不因 index 無 stage2 而炸
        assert loaded.stage2_mode == "none"
```

（`two_group_adapter`／`real_stage2_adapter` fixtures：照該測試檔既有建 adapter 的方式抽 fixture；`real_stage2_adapter` 用 `fit_stage2("binary", ...)` 或直接小 `lgb.train` 4 欄資料建。）

- [ ] **Step 2: RED** — 預期 `AttributeError: 'StagedModelAdapter' object has no attribute 'set_stage2'`。
- [ ] **Step 3: 實作**（adapter.py 四處修改）

(a) `__init__` 追加：

```python
        self._stage2 = None
        self._stage2_meta: dict = {}
```

(b) `set_partition_keys` 之後追加：

```python
    def set_stage2(self, adapter, meta: dict) -> None:
        """Attach the stage-2 booster (train_stage2_model 編排用)."""
        self._stage2 = adapter
        self._stage2_meta = dict(meta)

    @property
    def stage2_mode(self) -> str:
        return (self._stage2_meta.get("mode", "none")
                if self._stage2 is not None else "none")
```

(c) `predict_routed` 的 `return scores, mask` 之前插入（top import 加 `from recsys_tfb.models.staged.stage2 import encode_group_codes, group_code_lookup, stage2_matrix`；模組 docstring 的 bundle layout 補 `stage2/model.txt`＋`stage2/.bundle_id` 兩行）：

```python
        if self._stage2 is not None:
            valid = np.flatnonzero(mask)
            if valid.size:
                lookup = group_code_lookup(self._groups)
                gcodes = encode_group_codes(keys[valid], lookup)
                X2 = stage2_matrix(X[valid], scores[valid], gcodes)
                scores[valid] = self._stage2.predict(X2)
```

(d) `save()`：`tmp_dir.replace(final_dir)` 之後、組 `index` dict 之前插入：

```python
        stage2_dir = version_dir / "stage2"
        if self._stage2 is not None:
            tmp2 = version_dir / f"stage2.tmp-{bundle_id}"
            tmp2.mkdir()
            self._stage2.save(str(tmp2 / "model.txt"))
            (tmp2 / ".bundle_id").write_text(bundle_id)
            if stage2_dir.exists():
                shutil.rmtree(stage2_dir)
            tmp2.replace(stage2_dir)
        elif stage2_dir.exists():
            shutil.rmtree(stage2_dir)  # 前一輪 bundle 殘留，不得與新 index 誤配
```

`index` dict 追加一鍵：`"stage2": dict(self._stage2_meta) if self._stage2 is not None else None,`

(e) `load()`：group 檔案迴圈之後、`if problems:` 之前插入完整性檢查；成功載入段（`self._partition_keys = ...` 之前）插入還原：

```python
        stage2_meta = index.get("stage2")
        stage2_dir = index_path.parent / "stage2"
        if stage2_meta is not None:
            id2 = stage2_dir / ".bundle_id"
            if not stage2_dir.is_dir():
                problems.append("stage2/ directory missing")
            elif not id2.exists():
                problems.append("stage2/.bundle_id missing")
            elif id2.read_text().strip() != index.get("bundle_id"):
                problems.append(
                    "bundle_id mismatch between index and stage2/ "
                    "(mixed bundle)")
            elif not (stage2_dir / "model.txt").exists():
                problems.append("stage2 model file missing")
```

```python
        if stage2_meta is not None:
            s2 = LightGBMAdapter()
            s2.load(str(stage2_dir / "model.txt"))
            self._stage2 = s2
            self._stage2_meta = dict(stage2_meta)
        else:
            self._stage2 = None
            self._stage2_meta = {}
```

- [ ] **Step 4: GREEN** — `pytest tests/test_models/test_staged/test_adapter.py -q` 整檔（既有測試不得轉紅）。
- [ ] **Step 5: mutation check**：(c) 段把 `scores[valid] = self._stage2.predict(X2)` 註掉 → `test_predict_routed_feeds_stage2_matrix` 轉紅（分數仍是 stage-1 值非 7.0）。改回。
- [ ] **Step 6: Commit** `feat(staged): adapter 掛 stage-2 booster——predict 合成＋bundle stage2/ 原子寫入與完整性檢查`

---

### Task 6: OOF 編排＋群級 checkpoint（`staged_stage2.py` 前半）

**Files:**
- Create: `src/recsys_tfb/pipelines/training/staged_stage2.py`（本 task 先放 `_group_oof`／checkpoint helpers；node 與 tune 在 Task 7/8）
- Test: `tests/test_pipelines/test_training/test_staged_stage2_oof.py`

- [ ] **Step 1: 寫失敗測試**

```python
import numpy as np
import pytest

from recsys_tfb.pipelines.training.staged_stage2 import (
    _group_oof, _load_oof_checkpoint, _write_oof_checkpoint,
)

PARAMS = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
          "num_threads": 1, "num_leaves": 5, "learning_rate": 0.2,
          "num_iterations": 15, "early_stopping_rounds": 5}


def _toy_group(n=120, seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.35).astype(int)
    X = np.column_stack([rng.normal(loc=y), rng.normal(size=n)])
    w = np.ones(n)
    folds = np.arange(n) % 3  # 每折都有正負例（機率上；seed 固定已驗）
    return X, y, w, folds


class TestGroupOof:
    def test_every_row_scored_and_deterministic(self):
        X, y, w, folds = _toy_group()
        a = _group_oof("A", X, y, w, folds, X[:30], y[:30], PARAMS, [], 3)
        b = _group_oof("A", X, y, w, folds, X[:30], y[:30], PARAMS, [], 3)
        assert np.isfinite(a).all() and len(a) == len(y)
        np.testing.assert_allclose(a, b)

    def test_oof_scores_differ_from_full_fit(self):
        # 結構性驗證「走了哪條路」：OOF 分數不得等於全量 fit 的自評分數
        # （若 fold 遮罩沒生效，兩者位元相同）
        from recsys_tfb.models.staged.train_stage1 import _fit_adapter
        X, y, w, folds = _toy_group()
        oof = _group_oof("A", X, y, w, folds, X[:30], y[:30], PARAMS, [], 3)
        full = _fit_adapter(X, y, w, X[:30], y[:30], dict(PARAMS), [])
        assert not np.allclose(oof, full.predict(X))

    def test_leakage_guard_raises_on_uncovered_rows(self):
        X, y, w, folds = _toy_group()
        with pytest.raises(RuntimeError, match="OOF"):
            # n_folds=2 但 folds 含 2 → fold-2 的列沒人評 → guard 炸
            _group_oof("A", X, y, w, folds, X[:30], y[:30], PARAMS, [], 2)


class TestOofCheckpoint:
    def test_roundtrip(self, tmp_path):
        scores = np.array([0.1, 0.9, 0.5])
        _write_oof_checkpoint(tmp_path / "oof", scores, 3, 5, 42)
        got = _load_oof_checkpoint(tmp_path / "oof", 3, 5, 42)
        np.testing.assert_allclose(got, scores)

    def test_absent_or_mismatched_returns_none(self, tmp_path):
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 5, 42) is None
        _write_oof_checkpoint(tmp_path / "oof", np.zeros(3), 3, 5, 42)
        assert _load_oof_checkpoint(tmp_path / "oof", 4, 5, 42) is None
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 4, 42) is None
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 5, 43) is None

    def test_no_success_marker_returns_none(self, tmp_path):
        _write_oof_checkpoint(tmp_path / "oof", np.zeros(3), 3, 5, 42)
        (tmp_path / "oof" / "_SUCCESS").unlink()
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 5, 42) is None
```

- [ ] **Step 2: RED** — 預期 `ModuleNotFoundError: No module named 'recsys_tfb.pipelines.training.staged_stage2'`。
- [ ] **Step 3: 實作**（模組頭＋本 task 函式；node/tune 的 import 留到 Task 7/8 再補）

```python
"""train_stage2_model node: OOF stage-1 scores -> stage-2 model (PR-B).

Flow (spec §2.2 step 5): OOF gates (per group x fold trainability) ->
per-group K-fold OOF fits with that group's stage-1 best_params (train_dev
early stop) -> stage-2 matrix [X | oof_s1 | gcode] -> stage-2 fit/HPO,
early-stopped AND trial-scored on val (spec §2.2/§3.1; test untouched).

HPO reuses the shared path's persistent-study machinery by MODULE, not by
function: hpo_resume (journal study / resume / checkpoint), search_space,
nodes._hpo_score and write_hpo_diagnostics — tune_hyperparameters itself is
untouched (shared zero-regression). Stage-2 HPO reads the SAME flat
training.* keys as shared mode (spec §2.1), so compute_search_id's
"minus n_trials" semantics, --fresh-hpo and resume docs hold verbatim and
search_id covers only the stage-2 search (spec §3.2).

OOF checkpointing (PR-A 群級 checkpoint 同款，中斷成本考量): per-group OOF
score vectors under <wip_root>/<slug>/oof/ (scores.npy + meta.json +
_SUCCESS), keyed by model_version; restored only when n_rows/n_folds/seed
all match.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import optuna

from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.extract import (
    _composite_key_series, _pdf_to_X, _row_weights_from_pdf,
)
from recsys_tfb.models.staged.gates import check_oof_gates
from recsys_tfb.models.staged.oof import assign_folds, oof_is_leakage_clean
from recsys_tfb.models.staged.partition import (
    group_labels, group_seed, group_slug, routing_keys,
)
from recsys_tfb.models.staged.stage2 import (
    encode_group_codes, fit_stage2, group_code_lookup,
    stage2_categorical_indices, stage2_matrix,
)
from recsys_tfb.models.staged.train_stage1 import _fit_adapter
from recsys_tfb.pipelines.training.staged import _wip_dir
from recsys_tfb.utils.spark import release_spark_session

logger = logging.getLogger(__name__)


def _group_oof(
    key, X_g, y_g, w_g, folds_g, X_dev_g, y_dev_g,
    params: dict, cat_idx, n_folds: int,
) -> np.ndarray:
    """OOF scores for ONE group's rows (group row order): each row scored by
    the booster that excluded its fold; leakage- and coverage-checked."""
    oof = np.full(len(y_g), np.nan, dtype=np.float64)
    producing = np.full(len(y_g), -1, dtype=np.int64)
    for k in range(int(n_folds)):
        pred_mask = folds_g == k
        if not pred_mask.any():
            continue
        fit_mask = ~pred_mask
        adapter = _fit_adapter(
            X_g[fit_mask], y_g[fit_mask], w_g[fit_mask],
            X_dev_g, y_dev_g, dict(params), cat_idx,
        )
        oof[pred_mask] = adapter.predict(X_g[pred_mask])
        producing[pred_mask] = k
    if np.isnan(oof).any() or not oof_is_leakage_clean(folds_g, producing):
        raise RuntimeError(
            f"OOF integrity failed in group {key!r}: unscored rows or a row "
            "scored in-fold — this is a bug, not a data issue")
    return oof


def _load_oof_checkpoint(odir: Path, n_rows: int, n_folds: int, seed: int):
    if not (odir / "_SUCCESS").exists():
        return None
    meta = json.loads((odir / "meta.json").read_text())
    if (meta.get("n_rows"), meta.get("n_folds"), meta.get("seed")) != \
            (int(n_rows), int(n_folds), int(seed)):
        return None  # 形狀/設定不符 → 視同無 checkpoint（同 model_version 不應發生）
    return np.load(odir / "scores.npy")


def _write_oof_checkpoint(odir: Path, scores, n_rows, n_folds, seed) -> None:
    odir.mkdir(parents=True, exist_ok=True)
    np.save(odir / "scores.npy", np.asarray(scores))
    (odir / "meta.json").write_text(json.dumps(
        {"n_rows": int(n_rows), "n_folds": int(n_folds), "seed": int(seed)}))
    (odir / "_SUCCESS").touch()
```

（注意：`test_leakage_guard_raises_on_uncovered_rows` 走的是 `np.isnan(oof).any()` 分支——fold 2 的列 `pred_mask` 永不為真。）

- [ ] **Step 4: GREEN**
- [ ] **Step 5: mutation check**：`fit_mask = ~pred_mask` 改成 `fit_mask = pred_mask` → `test_oof_scores_differ_from_full_fit` 或 determinism 測試轉紅（in-fold 自評、或單類別 fit 炸）。改回。
- [ ] **Step 6: Commit** `feat(staged): 每群 OOF cross-fit（best_params＋train_dev early stop）＋ OOF 群級 checkpoint`

---

### Task 7: `tune_stage2` persistent-study HPO 迴圈

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/staged_stage2.py`（追加 `tune_stage2`）
- Test: `tests/test_pipelines/test_training/test_tune_stage2.py`

- [ ] **Step 1: 寫失敗測試**

```python
import numpy as np
import pytest

from recsys_tfb.pipelines.training.staged_stage2 import tune_stage2

BASE = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
        "num_threads": 1, "num_leaves": 5, "learning_rate": 0.2,
        "num_iterations": 15, "early_stopping_rounds": 5}


def _toy(n=200, seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.3).astype(int)
    X2 = np.column_stack([rng.normal(loc=y), rng.normal(size=n),
                          rng.random(n), (np.arange(n) % 2).astype(float)])
    qg = np.repeat(np.arange(n // 4), 4)
    items = np.array(["p1", "p2"] * (n // 2), dtype=object)
    return X2, y, qg, items


def _params(n_trials, tmp_path, **over):
    p = {
        "random_seed": 42,
        "search_id": "s2test01",           # 繞過 compute_search_id
        "hpo_checkpointing": True,
        "training": {
            "algorithm": "lightgbm",
            "n_trials": n_trials,
            "hpo_objective": "mean_ap",
            "search_space": [
                {"name": "num_leaves", "type": "int", "low": 4, "high": 8},
            ],
        },
    }
    p.update(over)
    return p


class TestTuneStage2:
    def test_n_trials_zero_single_fit_no_study(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # hpo_study_dir 是相對路徑
        X2, y, qg, items = _toy()
        best, adapter, meta = tune_stage2(
            "binary", dict(BASE), [3], X2, y, None, qg,
            X2, y, qg, items, _params(0, tmp_path))
        assert best == {} and meta["n_trials"] == 0
        assert np.isfinite(adapter.predict(X2)).all()
        assert not (tmp_path / "data").exists()  # 零 trial 不落 study

    def test_hpo_runs_and_persists_study(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        best, adapter, meta = tune_stage2(
            "binary", dict(BASE), [3], X2, y, None, qg,
            X2, y, qg, items, _params(2, tmp_path))
        study_dir = tmp_path / "data" / "models" / "_hpo" / "s2test01"
        assert (study_dir / "study_journal.log").exists()
        assert (study_dir / "checkpoint" / "model.txt").exists()
        assert "num_leaves" in best and meta["score"] > -1.0

    def test_resume_runs_only_remaining_trials(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, qg, items, _params(2, tmp_path))
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        calls = {"n": 0}
        real = mod.fit_stage2
        def spy(*a, **kw):
            calls["n"] += 1
            return real(*a, **kw)
        monkeypatch.setattr(mod, "fit_stage2", spy)
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, qg, items, _params(3, tmp_path))
        assert calls["n"] == 1  # 已完成 2，目標 3 → 只補 1 trial

    def test_fresh_hpo_clears_study(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, qg, items, _params(2, tmp_path))
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        calls = {"n": 0}
        real = mod.fit_stage2
        def spy(*a, **kw):
            calls["n"] += 1
            return real(*a, **kw)
        monkeypatch.setattr(mod, "fit_stage2", spy)
        tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                    X2, y, qg, items,
                    _params(2, tmp_path, _fresh_hpo=True))
        assert calls["n"] == 2  # 清掉重搜

    def test_unknown_hpo_objective_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        X2, y, qg, items = _toy()
        p = _params(1, tmp_path)
        p["training"]["hpo_objective"] = "auc"
        with pytest.raises(ValueError, match="hpo_objective"):
            tune_stage2("binary", dict(BASE), [3], X2, y, None, qg,
                        X2, y, qg, items, p)
```

- [ ] **Step 2: RED** — 預期 `ImportError: cannot import name 'tune_stage2'`。
- [ ] **Step 3: 實作**（staged_stage2.py 追加）

```python
def tune_stage2(
    mode: str, base_params: dict, cat_idx2,
    X2_tr, y_tr, w_tr, qg_tr,
    X2_val, y_val, qg_val, items_val,
    parameters: dict,
):
    """Persistent-study HPO for stage-2, or a single fit when n_trials==0.

    Returns (best_params, adapter, hpo_meta). Study/resume/--fresh-hpo/
    checkpoint semantics mirror tune_hyperparameters via hpo_resume; the
    loop itself is stage-2-specific (in-memory matrices, binary/lambdarank
    objective, val-scored trials with the shared ranking _hpo_score).
    """
    from recsys_tfb.pipelines.training import hpo_resume
    from recsys_tfb.pipelines.training.nodes import (
        HPO_OBJECTIVES, _hpo_score, _resolve_search_id,
    )
    from recsys_tfb.pipelines.training.search_space import build_trial_params

    training = parameters["training"]
    n_trials = int(training.get("n_trials", 0))
    if n_trials <= 0:
        adapter = fit_stage2(mode, X2_tr, y_tr, w_tr, qg_tr,
                             X2_val, y_val, qg_val, base_params, cat_idx2)
        return {}, adapter, {"n_trials": 0}

    hpo_objective = training.get("hpo_objective", "mean_ap")
    if hpo_objective not in HPO_OBJECTIVES:
        raise ValueError(
            f"unknown training.hpo_objective {hpo_objective!r}; "
            f"allowed: {', '.join(HPO_OBJECTIVES)}")
    search_space = training.get("search_space") or []
    seed = int(parameters.get("random_seed", 42))
    checkpointing = parameters.get("hpo_checkpointing", True)
    search_id = _resolve_search_id(parameters)
    study_dir = None
    best_state = {"score": -1.0, "model": None, "params": {}, "iteration": 0}

    def objective(trial):
        trial_params = build_trial_params(trial, search_space)
        adapter = fit_stage2(
            mode, X2_tr, y_tr, w_tr, qg_tr, X2_val, y_val, qg_val,
            {**base_params, **trial_params}, cat_idx2)
        score = _hpo_score(hpo_objective, qg_val, items_val, y_val,
                           adapter.predict(X2_val))
        if score > best_state["score"]:
            best_state.update(
                score=score, model=adapter, params=trial_params,
                iteration=adapter.booster.best_iteration)
            if checkpointing and study_dir is not None:
                hpo_resume.write_checkpoint(
                    study_dir, adapter, score=score,
                    best_iteration=adapter.booster.best_iteration,
                    best_params=trial_params, trial_number=trial.number,
                    search_id=search_id)
        logger.info("tune_stage2: trial=%d score=%.4f best=%.4f",
                    trial.number, score, best_state["score"])
        return score

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    if checkpointing:
        study_dir = hpo_resume.hpo_study_dir(search_id)
        if parameters.get("_fresh_hpo", False):
            logger.warning("--fresh-hpo: clearing %s", study_dir)
            hpo_resume.clear_study_dir(study_dir)
        study = hpo_resume.open_study(study_dir, search_id, seed)
        done = hpo_resume.count_completed(study)
        ckpt = hpo_resume.load_checkpoint(
            study_dir, training.get("algorithm", "lightgbm"))
        if ckpt is not None:
            best_state.update(score=ckpt["score"], model=ckpt["model"],
                              params=ckpt["params"],
                              iteration=ckpt["iteration"])
            logger.info(
                "stage-2 HPO resume: %d completed, best=%.4f; running %d "
                "more (target=%d)", done, ckpt["score"],
                max(0, n_trials - done), n_trials)
        remaining = max(0, n_trials - done)
    else:
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=seed))
        remaining = n_trials
    if remaining > 0:
        study.optimize(objective, n_trials=remaining)
    if best_state["model"] is None:
        # study 有 trial 但無可用 checkpoint 模型 → 補跑一次（shared 同款保底）
        study.enqueue_trial(study.best_params)
        study.optimize(objective, n_trials=1)

    # HPO 搜尋診斷：best-effort，失敗不影響回傳（同 nodes.py:605-614 慣例）
    try:
        from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics
        write_hpo_diagnostics(
            study, search_space, parameters, search_id=search_id,
            hpo_objective=hpo_objective, seed=seed,
            n_trials_target=n_trials,
            best_iteration=best_state["iteration"])
    except Exception:
        logger.warning("stage-2 HPO diagnostics failed; training continues",
                       exc_info=True)

    meta = {"n_trials": n_trials, "search_id": search_id,
            "hpo_objective": hpo_objective,
            "score": float(best_state["score"]),
            "best_iteration": int(best_state["iteration"])}
    return dict(best_state["params"]), best_state["model"], meta
```

- [ ] **Step 4: GREEN** — `pytest tests/test_pipelines/test_training/test_tune_stage2.py -q`
- [ ] **Step 5: mutation check**：把 `remaining = max(0, n_trials - done)` 改成 `remaining = n_trials` → `test_resume_runs_only_remaining_trials` 轉紅（spy 數到 3 非 1）。改回。
- [ ] **Step 6: Commit** `feat(staged): tune_stage2——重用 hpo_resume persistent study/resume/--fresh-hpo/搜尋診斷`

---

### Task 8: `train_stage2_model` node 整合

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/staged_stage2.py`（追加 node）
- Test: `tests/test_pipelines/test_training/test_staged_stage2_node.py`

- [ ] **Step 1: 寫失敗測試**（fixture 沿用 `test_staged_node.py` 的 `_write_parquet`／`_pdf`／`_parameters`／`PREPROC` 形狀——複製後改造，`_pdf` 增加多 entity 列與 val；stage2 config 換 lambdarank）

```python
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.gates import StagedGateError
from recsys_tfb.pipelines.training.staged import train_staged_model
from recsys_tfb.pipelines.training.staged_stage2 import train_stage2_model


def _write_parquet(tmp_path, name, pdf):
    p = tmp_path / f"{name}.parquet"
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


def _pdf(n_per_group=80, groups=("A", "B"), seed=0):
    rng = np.random.default_rng(seed)
    frames = []
    for g in groups:
        y = (rng.random(n_per_group) < 0.3).astype(int)
        frames.append(pd.DataFrame({
            "snap_date": "2026-01-01",
            "cust_id": np.arange(n_per_group) % 20,  # 每 query 多列、entity 重複
            "prod_name": "p1",
            "f1": rng.normal(loc=y, size=n_per_group),
            "f2": rng.normal(size=n_per_group),
            "seg": g, "label": y,
        }))
    return pd.concat(frames, ignore_index=True)


def _parameters(mode="lambdarank", n_trials=0, oof_folds=3):
    return {
        "random_seed": 42,
        "search_id": "s2node01",
        "model_version": "mvtest",
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
            "num_iterations": 15, "early_stopping_rounds": 5,
            "n_trials": n_trials,
            "hpo_objective": "mean_ap",
            "search_space": [
                {"name": "num_leaves", "type": "int", "low": 4, "high": 8}],
            "model_structure": "staged",
            "staged": {
                "stage1": {"partition_keys": ["seg"], "objective": "binary",
                           "hpo": {"n_trials": 0, "metric": "auc",
                                   "search_space": []},
                           "params": {},
                           "gates": {"max_groups": 10, "min_rows": 10,
                                     "min_positives": 3, "min_negatives": 3},
                           "max_workers": 1},
                "stage2": {"mode": mode, "oof_folds": oof_folds,
                           "params": {}},
            },
        },
    }


PREPROC = {"feature_columns": ["f1", "f2"], "categorical_columns": [],
           "category_mappings": {}}


def _handles(tmp_path):
    tr = _write_parquet(tmp_path, "train", _pdf(seed=0))
    dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=40, seed=1))
    val = _write_parquet(tmp_path, "val", _pdf(n_per_group=40, seed=2))
    return tr, dev, val


def _stage1(tmp_path, params):
    tr, dev, _ = _handles(tmp_path)
    return train_staged_model(tr, dev, PREPROC, params,
                              wip_root=tmp_path / "wip")


class TestTrainStage2Model:
    def test_attaches_stage2_and_reports(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        model, report = train_stage2_model(
            m1, rep1, tr, dev, val, PREPROC, params,
            wip_root=tmp_path / "wip")
        assert model is m1 and model.stage2_mode == "lambdarank"
        assert report["mode"] == "lambdarank"
        assert report["oof_folds"] == 3
        assert report["oof_rows"] == 160
        X = np.random.default_rng(3).normal(size=(4, 2))
        keys = np.array(["A", "B", "A", "B"], dtype=object)
        scores, mask = model.predict_routed(X, keys)
        assert mask.all() and np.isfinite(scores).all()

    def test_binary_mode_also_works(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters(mode="binary")
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        model, report = train_stage2_model(
            m1, rep1, tr, dev, val, PREPROC, params,
            wip_root=tmp_path / "wip")
        assert model.stage2_mode == "binary"

    def test_oof_checkpoint_restored_on_rerun(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                           wip_root=tmp_path / "wip")
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        def boom(*a, **kw):
            raise AssertionError("OOF 應全部從 checkpoint 還原，不得重算")
        monkeypatch.setattr(mod, "_group_oof", boom)
        m1b, rep1b = _stage1(tmp_path, params)  # stage-1 也走自己的 checkpoint
        model, _ = train_stage2_model(m1b, rep1b, tr, dev, val, PREPROC,
                                      params, wip_root=tmp_path / "wip")
        assert model.stage2_mode == "lambdarank"

    def test_oof_gate_failure_fails_fast(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        pdf = _pdf(seed=0)
        # B 群正例集中到單一 entity → 該 entity 所在折被留出時 fit set 無正例
        b = pdf["seg"] == "B"
        pdf.loc[b, "label"] = 0
        pdf.loc[b & (pdf["cust_id"] == 0), "label"] = 1
        tr = _write_parquet(tmp_path, "train2", pdf)
        _, dev, val = _handles(tmp_path)
        with pytest.raises(StagedGateError, match="OOF"):
            train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                               wip_root=tmp_path / "wip2")

    def test_val_missing_group_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, _ = _handles(tmp_path)
        val_pdf = _pdf(n_per_group=40, seed=2)
        val_pdf["seg"] = "ZZ"  # val 出現未訓練群 → eval 語意 fail-fast
        val = _write_parquet(tmp_path, "valzz", val_pdf)
        from recsys_tfb.models.staged.adapter import StagedMissingGroupError
        with pytest.raises(StagedMissingGroupError):
            train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                               wip_root=tmp_path / "wip")
```

（`test_attaches_stage2_and_reports` 的 `oof_rows == 160`＝2 群×80 列。`release_spark_session` 在測試環境無 Spark 時必須是 no-op——先確認 `utils/spark.py` 該函式對「無 active session」安全，若不安全，node 改成 `parameters.get("_skip_spark_release")` 可跳過並在測試 params 設之；以實際讀碼為準，測試不得 mock 掉整個 node。）

- [ ] **Step 2: RED** — 預期 `ImportError: cannot import name 'train_stage2_model'`。
- [ ] **Step 3: 實作**（staged_stage2.py 追加）

```python
def train_stage2_model(
    stage1_model,
    stage1_groups_report: dict,
    train_parquet_handle,
    train_dev_parquet_handle,
    val_parquet_handle,
    preprocessor_view: dict,
    parameters: dict,
    wip_root=None,
):
    """Attach a stage-2 booster to the stage-1 adapter -> (model, report)."""
    # HPO 可能數小時；比照 tune_hyperparameters 首行主動釋放 Spark，
    # 由 predict 節點依 canonical configs 重建（nodes.py:408-419 同一理由）。
    release_spark_session(parameters)

    training = parameters["training"]
    staged_cfg = training["staged"]
    stage2_cfg = staged_cfg["stage2"]
    stage1_cfg = staged_cfg["stage1"]
    mode = stage2_cfg["mode"]
    n_folds = int(stage2_cfg.get("oof_folds", 5))
    partition_keys = stage1_model.partition_keys
    seed = int(parameters.get("random_seed", 42))
    schema = get_schema(parameters)
    label_col = schema["label"]
    wip = _wip_dir(parameters, wip_root)

    pdf_tr = train_parquet_handle.to_pandas()
    pdf_dev = train_dev_parquet_handle.to_pandas()
    labels_tr = group_labels(pdf_tr, partition_keys)
    labels_dev = group_labels(pdf_dev, partition_keys)
    y_tr_full = pdf_tr[label_col].values
    entity_tr = _composite_key_series(
        pdf_tr, list(schema["entity"])).to_numpy(dtype=object)
    folds = assign_folds(entity_tr, n_folds, seed)
    check_oof_gates(labels_tr, y_tr_full, folds, n_folds)

    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    cat_idx = LightGBMAdapter._categorical_indices(preprocessor_view)
    algorithm_params = {
        **(training.get("algorithm_params") or {}),
        "num_iterations": training.get("num_iterations", 500),
        "early_stopping_rounds": training.get("early_stopping_rounds", 50),
    }
    best_by_group = {
        k: (v.get("best_params") or {})
        for k, v in (stage1_groups_report.get("groups") or {}).items()
    }
    group_keys = sorted(stage1_model.group_keys)
    tr_masks = {k: (labels_tr == k).to_numpy() for k in group_keys}
    dev_masks = {k: (labels_dev == k).to_numpy() for k in group_keys}

    def _one_group(key):
        odir = wip / group_slug(key) / "oof"
        g_mask = tr_masks[key]
        n_rows = int(g_mask.sum())
        cached = _load_oof_checkpoint(odir, n_rows, n_folds, seed)
        if cached is not None:
            return key, cached
        sub = pdf_tr.loc[g_mask]
        X_g = _pdf_to_X(sub, preprocessor_view, parameters)
        y_g = sub[label_col].values
        w_g = _row_weights_from_pdf(sub, parameters, preprocessor_view)
        sub_dev = pdf_dev.loc[dev_masks[key]]
        X_dev_g = _pdf_to_X(sub_dev, preprocessor_view, parameters)
        y_dev_g = sub_dev[label_col].values
        params = {**algorithm_params, **(stage1_cfg.get("params") or {}),
                  **best_by_group.get(key, {}),
                  "objective": "binary", "seed": group_seed(seed, key)}
        scores = _group_oof(key, X_g, y_g, w_g, folds[g_mask],
                            X_dev_g, y_dev_g, params, cat_idx, n_folds)
        _write_oof_checkpoint(odir, scores, n_rows, n_folds, seed)
        return key, scores

    max_workers = max(1, int(stage1_cfg.get("max_workers", 1)))
    logger.info("train_stage2_model: OOF %d group(s) x %d fold(s), "
                "max_workers=%d", len(group_keys), n_folds, max_workers)
    if max_workers == 1:
        pairs = [_one_group(k) for k in group_keys]
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            pairs = list(pool.map(_one_group, group_keys))
    oof = np.full(len(pdf_tr), np.nan, dtype=np.float64)
    for key, scores in pairs:
        oof[tr_masks[key]] = scores

    # ---- stage-2 訓練矩陣（[X | oof_s1 | gcode]，spec D4）----
    X_tr = _pdf_to_X(pdf_tr, preprocessor_view, parameters)
    w_tr = _row_weights_from_pdf(pdf_tr, parameters, preprocessor_view)
    lookup = group_code_lookup(group_keys)
    g_tr = encode_group_codes(routing_keys(pdf_tr, partition_keys), lookup)
    X2_tr = stage2_matrix(X_tr, oof, g_tr)
    n_base = X_tr.shape[1]
    del X_tr  # X2 已複製，釋放一份全量矩陣（spec §8 記憶體注意）
    qcols = [schema["time"], *schema["entity"]]
    qg_tr = pdf_tr.groupby(qcols, sort=False).ngroup().to_numpy(np.int64)

    pdf_val = val_parquet_handle.to_pandas()
    X_v = _pdf_to_X(pdf_val, preprocessor_view, parameters)
    rk_val = routing_keys(pdf_val, partition_keys)
    # val 缺群＝異常（評估語意，spec D11 分流）→ on_missing="raise"
    s1_v, _mask = stage1_model.predict_routed(X_v, rk_val, on_missing="raise")
    X2_v = stage2_matrix(X_v, s1_v, encode_group_codes(rk_val, lookup))
    del X_v
    y_v = pdf_val[label_col].values
    qg_v = pdf_val.groupby(qcols, sort=False).ngroup().to_numpy(np.int64)
    items_v = pdf_val[schema["item"]].to_numpy()

    from recsys_tfb.core.group_utils import default_metric_for_objective
    objective_name = "binary" if mode == "binary" else "lambdarank"
    stage2_params = dict(stage2_cfg.get("params") or {})
    base2 = {**algorithm_params, **stage2_params,
             "objective": objective_name, "seed": seed}
    # stage2.mode 是 objective 的唯一真實來源（與 stage-1 覆蓋行為對稱）；
    # ranking 下沿用 binary metric 會讓 early stopping 靜默失義 → 補 ndcg。
    base2["metric"] = default_metric_for_objective(
        objective_name, stage2_params.get("metric"))
    if not base2["metric"]:
        base2["metric"] = algorithm_params.get("metric")

    cat_idx2 = stage2_categorical_indices(cat_idx, n_base)
    best_params2, s2_adapter, hpo_meta = tune_stage2(
        mode, base2, cat_idx2, X2_tr, y_tr_full, w_tr, qg_tr,
        X2_v, y_v, qg_v, items_v, parameters)

    stage2_meta = {"mode": mode, "oof_folds": n_folds,
                   "best_params": best_params2, **hpo_meta}
    stage1_model.set_stage2(s2_adapter, stage2_meta)
    report = {"mode": mode, "oof_folds": n_folds,
              "oof_rows": int(len(oof)),
              "n_groups": len(group_keys),
              "best_params": best_params2, **hpo_meta}
    logger.info("train_stage2_model: mode=%s folds=%d best_params=%s",
                mode, n_folds, best_params2)
    return stage1_model, report
```

（metric 邏輯展開：lambdarank＋stage2.params 無 metric → `"ndcg"`；binary → `stage2.params.metric` 或退回 `algorithm_params.metric`。`default_metric_for_objective` 只補 ranking 的 unset 案例，binary 回傳原值可能為 None——上面兩行處理了 fallback。）

- [ ] **Step 4: GREEN** — `pytest tests/test_pipelines/test_training/test_staged_stage2_node.py -q`（lambdarank／binary／checkpoint／gate／missing-val 五案全過）
- [ ] **Step 5: mutation check**：把 `X2_tr = stage2_matrix(X_tr, oof, g_tr)` 的 `oof` 換成 `np.zeros(len(oof))` → `test_oof_checkpoint_restored_on_rerun` **不會**抓到（它只驗還原路徑）；正確 mutation 是拿掉 `s1_v` 改餵 zeros → `test_attaches_stage2_and_reports` 的 predict 不受影響……結論：**本 task 的行為級 mutation 由 Task 10 的 e2e 對照兜底**（stage-2 特徵重要度在 PR-C 才可見）；此處只驗 `check_oof_gates` 呼叫：註掉該行 → `test_oof_gate_failure_fails_fast` 轉紅。改回。
- [ ] **Step 6: Commit** `feat(staged): train_stage2_model node——OOF 編排＋stage-2 HPO＋adapter 掛載`

---

### Task 9: Pipeline／CLI／catalog／config 接線

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`（`create_pipeline` 簽名＋`_create_staged_pipeline`）
- Modify: `src/recsys_tfb/__main__.py`（training 子指令傳 `stage2_mode`；先 `grep -n "model_structure" src/recsys_tfb/__main__.py` 定位）
- Modify: `conf/base/parameters_training.yaml:151-152`（stage2 塊）
- Modify: `conf/base/catalog.yaml`（照抄 `stage1_groups_report` entry 改名）
- Test: `tests/test_pipelines/test_training/test_staged_pipeline.py`（追加）

- [ ] **Step 1: 寫失敗測試**（追加到既有 test_staged_pipeline.py；沿用其 import 慣例）

```python
class TestStagedStage2Pipeline:
    def test_stage2_dag_adds_val_cache_and_stage2_node(self):
        p = create_pipeline(model_structure="staged", stage2_mode="lambdarank")
        names = [n.func.__name__ for n in p.nodes]
        assert "cache_val_model_input" in names
        assert "train_stage2_model" in names
        s1 = next(n for n in p.nodes if n.func.__name__ == "train_staged_model")
        assert s1.outputs == ["stage1_model", "stage1_groups_report"]
        s2 = next(n for n in p.nodes if n.func.__name__ == "train_stage2_model")
        assert s2.outputs == ["model", "stage2_report"]
        assert s2.inputs == [
            "stage1_model", "stage1_groups_report", "train_parquet_handle",
            "train_dev_parquet_handle", "val_parquet_handle",
            "preprocessor_view", "parameters"]

    def test_stage2_none_dag_unchanged(self):
        p = create_pipeline(model_structure="staged", stage2_mode="none")
        names = [n.func.__name__ for n in p.nodes]
        assert "train_stage2_model" not in names
        assert "cache_val_model_input" not in names
        s1 = next(n for n in p.nodes if n.func.__name__ == "train_staged_model")
        assert s1.outputs == ["model", "stage1_groups_report"]

    def test_shared_dag_ignores_stage2_mode(self):
        a = create_pipeline(model_structure="shared")
        b = create_pipeline(model_structure="shared", stage2_mode="lambdarank")
        assert [n.func.__name__ for n in a.nodes] == \
               [n.func.__name__ for n in b.nodes]
```

（`Node` 物件的 `inputs`/`outputs`/`func` 屬性名以 `core/node.py` 實際為準——寫測試前先 read；若屬性是 `_inputs` 等私名，改用該檔既有測試的存取方式。）

- [ ] **Step 2: RED** — 預期 `TypeError: create_pipeline() got an unexpected keyword argument 'stage2_mode'`。
- [ ] **Step 3: 實作**

(a) `pipeline.py`：`create_pipeline(enable_calibration=False, model_structure="shared", stage2_mode="none")`，staged 分支傳給 `_create_staged_pipeline(stage2_mode)`。`_create_staged_pipeline` 改造：

```python
def _create_staged_pipeline(stage2_mode: str = "none") -> Pipeline:
    """Staged training DAG.

    stage2=none（PR-A 形狀）: train_staged_model 直接產出 "model"。
    stage2 in {binary, lambdarank}（PR-B）: train_staged_model 產出
    "stage1_model"，train_stage2_model 做 OOF＋stage-2 後產出 "model"；
    cache_val_model_input 拉回 DAG（val＝stage-2 early stop＋HPO 評分集，
    spec §2.2/§3.1）。predict/mAP 節點兩種形狀共用（吃 "model"）。
    Excluded (PR-C): diagnostics nodes、log_experiment、calibrate。
    """
    with_stage2 = stage2_mode != "none"
    nodes = [
        Node(select_features,
             inputs=["preprocessor", "parameters"],
             outputs="preprocessor_view"),
        Node(cache_train_model_input,
             inputs=["train_model_input", "parameters"],
             outputs="train_parquet_handle"),
        Node(cache_train_dev_model_input,
             inputs=["train_dev_model_input", "parameters"],
             outputs="train_dev_parquet_handle"),
    ]
    if with_stage2:
        nodes.append(Node(cache_val_model_input,
                          inputs=["val_model_input", "parameters"],
                          outputs="val_parquet_handle"))
    nodes.extend([
        Node(cache_test_model_input,
             inputs=["test_model_input", "parameters"],
             outputs="test_parquet_handle"),
        Node(persist_sample_weight_report,
             inputs=["train_parquet_handle", "preprocessor_view",
                     "parameters"],
             outputs="sample_weight_report"),
        Node(train_staged_model,
             inputs=["train_parquet_handle", "train_dev_parquet_handle",
                     "preprocessor_view", "parameters"],
             outputs=(["stage1_model", "stage1_groups_report"] if with_stage2
                      else ["model", "stage1_groups_report"]),
             name="train_staged_model"),
    ])
    if with_stage2:
        nodes.append(Node(
            train_stage2_model,
            inputs=["stage1_model", "stage1_groups_report",
                    "train_parquet_handle", "train_dev_parquet_handle",
                    "val_parquet_handle", "preprocessor_view", "parameters"],
            outputs=["model", "stage2_report"],
            name="train_stage2_model"))
    nodes.extend([
        Node(predict_and_write_test_predictions,
             inputs=["model", "test_parquet_handle", "preprocessor_view",
                     "parameters", "@training_eval_predictions"],
             outputs="predict_manifest"),
        Node(compute_test_mAP_spark,
             inputs=["training_eval_predictions", "predict_manifest",
                     "parameters"],
             outputs="evaluation_results"),
    ])
    return Pipeline(nodes)
```

（import：`from recsys_tfb.pipelines.training.staged_stage2 import train_stage2_model`＋既有 `cache_val_model_input` import（shared DAG 已用，同檔可得）。現行 `_create_staged_pipeline` docstring 的 excluded 清單同步更新——`tune/finalize` 字樣改為「stage-2 走 staged_stage2.tune_stage2」。）

(b) `__main__.py`：`model_structure` 讀取處旁補：

```python
    staged_cfg = (training_params.get("staged") or {}) \
        if model_structure == "staged" else {}
    stage2_mode = (staged_cfg.get("stage2") or {}).get("mode", "none")
```

並在 `pipeline_kwargs` 加 `"stage2_mode": stage2_mode`。同時 grep `search_id` 在 `__main__.py` 的注入路徑，確認 staged 分支也會算（training 子指令共用一段就不用動；若 shared-only 條件包住，放寬到 staged＋stage2!=none 也注入）。

(c) `parameters_training.yaml` stage2 塊改為：

```yaml
    stage2:
      mode: none                    # none | binary | lambdarank（PR-B 起）
      oof_folds: 5                  # stage2!=none 時生效；entity-hash 互斥 K 折（>=2）
      params: {}                    # 覆蓋 algorithm_params 的 stage-2 基底參數（objective 由 mode 決定）
```

(d) `catalog.yaml`：照抄 `stage1_groups_report` entry，改鍵名 `stage2_report`、路徑 `data/models/${model_version}/stage2.json`。

- [ ] **Step 4: GREEN** — `pytest tests/test_pipelines/test_training/test_staged_pipeline.py tests/test_pipelines/test_training/test_staged_node.py -q`（既有 PR-A DAG 測試不得轉紅）。
- [ ] **Step 5: mutation check**：`_create_staged_pipeline` 把 `with_stage2` 分支的 `outputs=["stage1_model", ...]` 改回 `["model", ...]` → `test_stage2_dag_adds_val_cache_and_stage2_node` 轉紅。改回。
- [ ] **Step 6: Commit** `feat(staged): stage2 DAG 接線（cache_val 回歸＋train_stage2_model）＋CLI stage2_mode＋catalog/config`

---

### Task 10: e2e 實跑驗證（controller 親自執行，不派 implementer）

**驗收對應 spec §10 PR-B 列：OOF leakage-clean 測試（Task 2/6 已蓋）＋ staged(lambdarank) 本機 e2e ＋ HPO resume 實測。**

- [ ] **Step 1: pre-flight**（CLAUDE.md §Worktree 指令塊照抄，grep 鍵換 `mode:`／`oof_folds:`）
- [ ] **Step 2: config 切換**：`parameters_training.yaml` 設 `model_structure: staged`、`stage2.mode: lambdarank`、`oof_folds: 3`、`training.n_trials: 2`、`search_space` 給一條 `num_leaves`（int 4–16）、`calibration.enabled: false`（A21）。
- [ ] **Step 3: 訓練 real-run**（background）：`SPARK_CONF_DIR=$PWD/conf/spark-local PYTHONPATH=src .venv/bin/python -m recsys_tfb training --env local`。驗：新 model_version 目錄含 `model.txt`（index 有 `"stage2"` 鍵）＋`stage1/`＋`stage2/model.txt`＋`stage2.json`；log 有 `tune_stage2: trial=` 與 mAP 數字；`data/models/_hpo/<search_id>/study_journal.log` 存在。
- [ ] **Step 4: checkpoint rerun**：同指令重跑 → log 應見 stage-1 `restored from checkpoint` 且 OOF 不重算（總時長顯著縮短）；model_version 不變。
- [ ] **Step 5: HPO resume 實測**：`n_trials` 2→4（search_id 不變＝去 n_trials 語意）重跑 → log 應見 `stage-2 HPO resume: 2 completed... running 2 more`。
- [ ] **Step 6: binary mode smoke**：`stage2.mode: binary` 跑一次 training，成功即可（產物鍵 `"mode": "binary"`）。
- [ ] **Step 7: evaluation 相容**：`python -m recsys_tfb evaluation --env local --post-training --model-version <新mv>`（不 promote——promote 是人工保留步驟）。
- [ ] **Step 8: 效率抽測**（spec §8 開放項）：記下 Step 3 的 OOF 段耗時與峰值記憶體觀察（`log_data_volume` 或 `/usr/bin/time -l`），寫進 PR 描述；不達標不擋 merge，是量測記錄。
- [ ] **Step 9: config 還原**：`model_structure: shared`、`stage2.mode: none`、`oof_folds: 5`、`n_trials`／`search_space`／`calibration.enabled: true` 全數還原；`git diff conf/` 確認乾淨。
- [ ] **Step 10: 回歸**（background）：全量 `pytest tests/ -q --deselect ...`（沿 PR-A 收尾同款；baseline＝684 passed／3 known fails：`test_adapter.py::TestPrepareTrainInputsWeight` ×2、`test_inference/test_pipeline.py::test_pipeline_inputs`）。新 fail 數必須為 0。
- [ ] **Step 11: graphify rebuild**：`.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`
- [ ] **Step 12: Commit＋push＋開 PR**：base 設 `feat/staged-modeling`（#117 未 merge；待其 merge 後 retarget main——PR 描述註明）。

---

## Self-Review 記錄（寫完計畫後自查）

- **Spec 覆蓋**：§10 PR-B 三項（OOF 編排＝Task 2/3/6/8；stage2 binary/lambdarank＝Task 4/8；HPO 接現行機制＝Task 7）；§9 item 11＝Task 3；A21 放寬＝Task 1；驗收三件（leakage 測試／lambdarank e2e／resume 實測）＝Task 2/6/10。§12 待驗證項「分群鍵當 Stage-2 categorical 的編碼路徑」已核實並定案（D-B3，group code 自算）。
- **型別/簽名一致性**：`fit_stage2(mode, X2_tr, y_tr, w_tr, qg_tr, X2_val, y_val, qg_val, params, cat_idx)` 在 Task 4 定義、Task 7/8 呼叫同序；`tune_stage2(mode, base_params, cat_idx2, X2_tr, y_tr, w_tr, qg_tr, X2_val, y_val, qg_val, items_val, parameters)` Task 7 定義、Task 8 呼叫同序；`_group_oof(key, X_g, y_g, w_g, folds_g, X_dev_g, y_dev_g, params, cat_idx, n_folds)` Task 6 定義、Task 8 呼叫同序。
- **已知風險（實作時遇到即停下回報）**：(1) `release_spark_session` 在無 Spark 測試環境的行為（Task 8 註記）；(2) `Node` 物件屬性名（Task 9 註記）；(3) `__main__.py` search_id 注入是否覆蓋 staged 分支（Task 9 (b)）；(4) `_composite_key_series` 對數值 entity 欄的字串化與 PR-A routing 慣例一致（同一函式，風險低）。
