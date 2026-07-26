# Staged Modeling PR-C（診斷）實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 training 側診斷接回 staged DAG——Stage-1 總覽表、stage2 存在時 booster 診斷掛 Stage-2（含 s1 特徵重要度視角）、stage2=none 時每群核心診斷、MLflow log_experiment 對應版——並以 real-run 驗證兩種 mode 的產物與 eval 側相容。

**Architecture:** 三支柱：(1) fit 端補真特徵名（根修 `Column_N` 問題，診斷才有意義）；(2) 診斷函式以 duck-typing（`getattr(model, "stage2_mode", "none")`）分派——shared 路徑 byte-identical；stage2 存在時靠 adapter 新增的 `booster` property 與 `stage2_matrix_for` 讓既有節點直接可用；(3) stage2=none 走獨立 per-group runner node，重用既有 compute 函式、產物落 `diagnostics/groups/<slug>/`。

**Tech Stack:** LightGBM 4.6.0、shap、numpy、既有 diagnosis/model 模組、MLflow 3.1.0。

**Branch / worktree:** `feat/staged-diagnostics` @ `/Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling`（stacked base = feat/staged-stage2@0973fe7；PR base 先設 feat/staged-stage2）。

---

## 執行環境鐵則（每個 task 都適用，照抄不省）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling
# 測試一律：
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <測試檔> -x -q
```

- Edit/Write 絕對路徑必含 `.worktrees/staged-modeling`（R1）。
- TDD：每步先 RED 再 GREEN；**實際 RED 訊息與計畫寫的預期不同 → 停下回報，不要自行繼續**。
- 每個 GREEN 後做 mutation check：弄壞計畫指定的那一行 → 對應測試須轉紅 → 改回。若指定 mutation 弄壞後仍全綠 → 停下回報（計畫的 mutation 選點錯了，需要換 load-bearing 的點），不要宣稱完成。
- 中文輸出全部繁體。

## 設計決策（D-C1～D-C8）

| # | 決策 | 理由 |
|---|---|---|
| D-C1 | **fit 端補 feature_name（根修）**：`train_stage1._fit_adapter` 與 `stage2.fit_stage2` 的 `lgb.Dataset` 補 `feature_name=`。不做「診斷側 wrapper 補名」 | booster 自報 `Column_N` 會讓 gain ledger 的 `split_feature == item_col` 零命中（靜默空帳）、importance 鍵全錯。名字進 booster 一次修好所有下游。#117/#118 未 merge、inference 未部署 → 無舊 bundle 相容包袱；real-run 驗證需重訓（本機 ~30s） |
| D-C2 | OOF fold fits（`staged_stage2._group_oof` 內的 `_fit_adapter` 呼叫）**不**帶 feature_name | fold booster 是拋棄式（只出 OOF 預測、不落 bundle、不被診斷），加了只是 diff 噪音 |
| D-C3 | staged 分派靠 duck-typing：`getattr(model, "stage2_mode", "none")`（stage2 判定）與 `hasattr(model, "predict_routed")`（staged 判定） | `LightGBMAdapter` 兩者皆不命中 → shared 路徑逐位元不變（可測宣稱）。diagnosis 不需 import staged 型別做 isinstance |
| D-C4 | stage2 存在：**重用既有節點**（`compute_feature_importance`／`compute_gain_ledger` 靠 adapter 新增 `booster` property 與 `feature_importance` delegate 直接可用；SHAP 三函式在「組 X」處插入 `resolve_attribution_inputs` 分支） | 不複製 orchestrator（per-item profile／divergence／正例 profile／象限全部原樣受益），維護單點 |
| D-C5 | stage2=none：**獨立 runner node** `compute_staged_group_diagnostics`，每群產「核心四件」＝feature stats／importance／gain ledger／SHAP summary，落 `diagnostics/groups/<slug>/`，manifest 進 catalog。**象限系列不做 per-group 版**（stage2=none 時整段不跑） | 使用者 2026-07-26 裁決（成本：per-group 象限 PNG 在公司規模會爆量）。⚠ 偏離 spec §6「每群完整」——已記錄於本計畫「偏離清單」 |
| D-C6 | Stage-1 總覽表＝新診斷節點 `compute_stage1_overview`，只重排 `stage1_groups_report`（＋`stage2_report` 摘要）既有事實，輸出 `diagnostics/stage1_overview.json`；**不做 HTML** | 使用者 2026-07-26 裁決：JSON＋MLflow artifact；training 側無 HTML 報表慣例。呈現守則：只呈現資料不下結論（diagnosis-report-presentation.md） |
| D-C7 | MLflow＝新節點 `log_staged_experiment`：**單一 run**；per-group 細節以 stage1_overview artifact 上傳；stage2 存在時 `log_to_mlflow` 記 stage-2 booster，none 時 INFO 說明模型在 bundle | 使用者 2026-07-26 裁決（不開 nested runs）。Runner 是 `node.func(*inputs)`（core/runner.py:92）→ 變長尾參 `*diag_deps` 吃 ordering-only 依賴，兩種 DAG 形狀共用一個函式 |
| D-C8 | 全域 `compute_feature_statistics` 節點兩種 staged 形狀都接回（model-free 的資料品質視角）；none 模式另在 runner 內出 per-group 版（抽出共用 `_stats_from_pdf`） | 該節點不 touch model（feature_stats.py:16），零成本重用 |

**Stage-2 特徵名定案（D-C4 附帶）**：`stage1_score`、`partition_gcode`（附加在 X 尾端的兩欄，順序同 `stage2_matrix`）。若使用者特徵撞名，lgb 會因重複特徵名 fail-loud——可接受（改 config 特徵名即可），不加防護。

## 偏離 spec 清單（審查者請對照）

1. spec §6「stage2=none 每群**完整** training 側診斷」→ 縮為核心四件、象限系列不跑（使用者 2026-07-26 裁決，動機＝公司規模 PNG 爆量）。
2. spec §6 未提的全域 feature_statistics 在兩種 staged 形狀保留（D-C8，零成本）。
3. OOF fold fits 不帶 feature_name（D-C2）。

## 檔案地圖

- Modify: `src/recsys_tfb/models/staged/train_stage1.py`（Task 1）
- Modify: `src/recsys_tfb/pipelines/training/staged.py`（Task 1 穿線）
- Modify: `src/recsys_tfb/models/staged/stage2.py`（Task 2）
- Modify: `src/recsys_tfb/pipelines/training/staged_stage2.py`（Task 2 穿線）
- Modify: `src/recsys_tfb/models/staged/adapter.py`（Task 3）
- Create: `src/recsys_tfb/diagnosis/model/staged.py`（Task 4、7）
- Modify: `src/recsys_tfb/diagnosis/model/shap_per_item.py`、`shap_cases.py`（Task 5）
- Modify: `src/recsys_tfb/diagnosis/model/feature_stats.py`（Task 6）
- Modify: `src/recsys_tfb/diagnosis/model/paths.py`（Task 7）
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`（Task 8）
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`、`conf/base/catalog.yaml`（Task 9）
- Tests: `tests/test_models/test_staged/`、`tests/test_diagnosis/`（既有目錄慣例先 `ls` 對齊）、`tests/test_pipelines/test_training/`

---

### Task 1: Stage-1 fit 真特徵名

**Files:**
- Modify: `src/recsys_tfb/models/staged/train_stage1.py:40-47`（`_fit_adapter`）、`:60-115`（`train_one_group`）
- Modify: `src/recsys_tfb/pipelines/training/staged.py:127-139`（`_train` 閉包穿線）
- Test: `tests/test_models/test_staged/test_train_stage1.py`（既有檔，追加）

- [ ] **Step 1: 失敗測試**

```python
class TestFeatureNames:
    def test_booster_reports_real_feature_names(self):
        X_tr, y_tr, X_dev, y_dev = _toy_arrays()   # 沿用檔內既有 fixture helper；無則仿鄰近測試造 60×3
        names = ["f_alpha", "f_beta", "f_gamma"]
        adapter = _fit_adapter(X_tr, y_tr, None, X_dev, y_dev,
                               {"objective": "binary", "verbosity": -1,
                                "num_iterations": 5, "early_stopping_rounds": 0},
                               None, feature_names=names)
        assert adapter.booster.feature_name() == names

    def test_feature_names_omitted_keeps_old_behavior(self):
        X_tr, y_tr, X_dev, y_dev = _toy_arrays()
        adapter = _fit_adapter(X_tr, y_tr, None, X_dev, y_dev,
                               {"objective": "binary", "verbosity": -1,
                                "num_iterations": 5, "early_stopping_rounds": 0},
                               None)
        assert adapter.booster.feature_name()[0].startswith("Column_")
```

- [ ] **Step 2: RED**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_models/test_staged/test_train_stage1.py -k FeatureNames -x -q`
預期 RED：`TypeError: _fit_adapter() got an unexpected keyword argument 'feature_names'`

- [ ] **Step 3: 實作**

`_fit_adapter` 改為（train_ds 加 kwargs；dev_ds 靠 `reference=` 繼承，不必重複）：

```python
def _fit_adapter(X_tr, y_tr, w_tr, X_dev, y_dev, params, categorical_indices,
                 feature_names=None):
    ds_kwargs = {}
    if feature_names is not None:
        ds_kwargs["feature_name"] = list(feature_names)
    train_ds = lgb.Dataset(
        X_tr, label=y_tr, weight=w_tr,
        categorical_feature=categorical_indices, free_raw_data=False,
        **ds_kwargs,
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
```

`train_one_group` 簽名尾端加 `feature_names=None`（在 `base_seed` 之後），兩處 `_fit_adapter(...)` 呼叫（trial objective 內 :92 與 full refit :106）尾端都補 `feature_names=feature_names`。

`staged.py` `_train` 閉包內的 `train_one_group(...)` 呼叫尾端補 `feature_names=list(preprocessor_view["feature_columns"])`。

- [ ] **Step 4: GREEN**

Run: 同 Step 2，預期 2 passed。再跑既有整檔確認無回歸：
`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_models/test_staged/test_train_stage1.py tests/test_pipelines/test_training/test_staged_node.py -q`（第二檔名以 `ls tests/test_pipelines/test_training/` 實際為準；staged node 測試檔一起跑）

- [ ] **Step 5: mutation check**

把 `ds_kwargs["feature_name"] = list(feature_names)` 註掉 → `test_booster_reports_real_feature_names` 須紅（拿到 `Column_0`）→ 改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(staged): stage-1 boosters carry real feature names (diagnostics prerequisite)"
```

---

### Task 2: Stage-2 特徵名（STAGE2_EXTRA_FEATURES ＋ fit_stage2 穿線）

**Files:**
- Modify: `src/recsys_tfb/models/staged/stage2.py`
- Modify: `src/recsys_tfb/pipelines/training/staged_stage2.py`（`tune_stage2` 簽名＋兩處 `fit_stage2` 呼叫 :126/:150；`train_stage2_model` 穿線 :363-368 一帶）
- Test: `tests/test_models/test_staged/test_stage2.py`、`tests/test_pipelines/test_training/test_tune_stage2.py`（追加）

- [ ] **Step 1: 失敗測試（test_stage2.py 追加）**

```python
from recsys_tfb.models.staged.stage2 import STAGE2_EXTRA_FEATURES, stage2_feature_names

class TestStage2FeatureNames:
    def test_names_are_base_plus_extras_in_matrix_order(self):
        assert stage2_feature_names(["a", "b"]) == ["a", "b", "stage1_score", "partition_gcode"]
        assert list(STAGE2_EXTRA_FEATURES) == ["stage1_score", "partition_gcode"]

    def test_fit_stage2_booster_reports_assembled_names(self):
        X2_tr, y, w, qg = _toy_stage2_inputs()   # 仿檔內既有 binary fixture；X2 3 欄（1 base + s1 + gcode）
        names = stage2_feature_names(["base_f"])
        adapter = fit_stage2("binary", X2_tr, y, None, qg, X2_tr, y, None, qg,
                             {"objective": "binary", "verbosity": -1,
                              "num_iterations": 5, "early_stopping_rounds": 0},
                             stage2_categorical_indices(None, 1),
                             feature_names=names)
        assert adapter.booster.feature_name() == names
```

- [ ] **Step 2: RED**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_models/test_staged/test_stage2.py -k FeatureNames -x -q`
預期 RED：`ImportError: cannot import name 'STAGE2_EXTRA_FEATURES'`

- [ ] **Step 3: 實作**

stage2.py 在 `group_code_lookup` 前加：

```python
STAGE2_EXTRA_FEATURES = ("stage1_score", "partition_gcode")


def stage2_feature_names(base_feature_cols) -> list:
    """[X | s1 | gcode] 對應的特徵名（與 stage2_matrix 欄序一致）。
    使用者特徵撞名時 lgb 以重複特徵名 fail-loud，屬 config 錯誤不防護。"""
    return list(base_feature_cols) + list(STAGE2_EXTRA_FEATURES)
```

`fit_stage2` 簽名尾端加 `feature_names=None`；兩個分支的 **train_ds** `lgb.Dataset(...)` 各補 `**({"feature_name": list(feature_names)} if feature_names is not None else {})`（可先組 `ds_kwargs` dict 同 Task 1 寫法；val_ds 靠 reference 繼承）。

`staged_stage2.py`：`tune_stage2` 簽名在 `parameters` 前加 `feature_names2=None` **不可行**（既有測試以位置傳參）→ 改為**尾端 keyword-only**：`def tune_stage2(..., parameters, *, feature_names2=None)`；內部兩處 `fit_stage2(...)`（:126、:150）尾端補 `feature_names=feature_names2`。`train_stage2_model` 呼叫 `tune_stage2(...)`（:365-368）補 `feature_names2=stage2_feature_names(list(preprocessor_view["feature_columns"]))`（import 自 `recsys_tfb.models.staged.stage2`；該模組已被本檔 import，見 :43 一帶）。

- [ ] **Step 4: GREEN ＋ 回歸**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_models/test_staged/test_stage2.py tests/test_pipelines/test_training/test_tune_stage2.py tests/test_pipelines/test_training/test_staged_stage2_node.py -q`
預期全綠（既有 tune_stage2 測試不帶新參數 → 走 None 分支不變）。

- [ ] **Step 5: mutation check**

把 `train_stage2_model` 穿線的 `feature_names2=...` 拿掉 → 需有一個 node 級測試轉紅。若既有 node 測試沒斷言特徵名 → **在 `test_staged_stage2_node.py` 追加**：

```python
def test_stage2_booster_carries_assembled_feature_names(...):  # 沿用該檔既有 e2e fixture
    model, report = train_stage2_model(...)
    names = model._stage2.booster.feature_name()
    assert names[-2:] == ["stage1_score", "partition_gcode"]
    assert names[:-2] == list(preprocessor_view["feature_columns"])
```

再做 mutation：拿掉穿線 → 此測試紅（`Column_N`）→ 改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(staged): stage-2 booster carries [X|stage1_score|partition_gcode] feature names"
```

---

### Task 3: Adapter 診斷能力（booster property／importance delegate／stage2_matrix_for／log_to_mlflow）

**Files:**
- Modify: `src/recsys_tfb/models/staged/adapter.py`
- Test: `tests/test_models/test_staged/test_adapter.py`（追加 class）

- [ ] **Step 1: 失敗測試**

```python
class TestDiagnosticsSurface:
    # fixture 沿用檔內既有 _FakeStage2 / 已組好群的 adapter helper

    def test_booster_property_delegates_to_stage2(self, staged_with_stage2):
        assert staged_with_stage2.booster is staged_with_stage2._stage2.booster

    def test_booster_property_raises_when_no_stage2(self, staged_no_stage2):
        with pytest.raises(NotImplementedError, match="per-group"):
            _ = staged_no_stage2.booster

    def test_feature_importance_delegates_to_stage2(self, staged_with_stage2):
        # _FakeStage2 若無 feature_importance，補一個回固定 dict 的 stub
        assert staged_with_stage2.feature_importance("gain") == {"f": 1.0}

    def test_feature_importance_raises_when_no_stage2(self, staged_no_stage2):
        with pytest.raises(NotImplementedError, match="per-group"):
            staged_no_stage2.feature_importance("gain")

    def test_stage2_matrix_for_matches_predict_routed_compose(self, staged_with_stage2):
        X, keys = _routing_fixture()          # 檔內既有 compose 測試同款
        X2 = staged_with_stage2.stage2_matrix_for(X, keys)
        assert X2.shape == (len(X), X.shape[1] + 2)
        # 尾兩欄 = stage-1 分數、gcode：與逐群手算一致（沿用既有 compose 測試的期望值）
        np.testing.assert_allclose(X2[:, -2], _expected_s1_scores(...))

    def test_stage2_matrix_for_raises_on_missing_group(self, staged_with_stage2):
        with pytest.raises(StagedMissingGroupError):
            staged_with_stage2.stage2_matrix_for(X_unknown, keys_unknown)
```

- [ ] **Step 2: RED**

Run: `... -m pytest tests/test_models/test_staged/test_adapter.py -k DiagnosticsSurface -x -q`
預期 RED：第一個測試 `AttributeError`／`NotImplementedError`（現行 `feature_importance` raise 訊息是 "per-group diagnostics arrive with the diagnostics PR (PR-C)."，`booster` 屬性不存在）。

- [ ] **Step 3: 實作**

1. **抽取 `_stage1_scores`**：把 `predict_routed` 中「迴圈逐群評分＋missing 統計＋raise/skip」段（adapter.py:107-138，從 `keys = np.asarray...` 到 skip 的 `logger.warning` 為止）原樣搬進：

```python
def _stage1_scores(self, X, keys, on_missing):
    """逐群 stage-1 評分（predict_routed 的前半；搬移不改行為）。"""
    ...  # 原 :107-138 內容原樣
    return scores, mask
```

`predict_routed` 改為：

```python
def predict_routed(self, X, keys, on_missing="raise"):
    """<docstring 原樣保留>"""
    if on_missing not in ("raise", "skip"):
        raise ValueError(f"on_missing must be raise|skip, got {on_missing!r}")
    scores, mask = self._stage1_scores(X, keys, on_missing)
    if self._stage2 is not None:
        ...  # 原 compose 塊原樣
    return scores, mask
```

（`on_missing` 驗證留在 `predict_routed`、`keys` 長度檢查隨搬移進 `_stage1_scores`——外部行為不變。）

2. **`stage2_matrix_for`**（放 compose 塊之後、persistence 之前）：

```python
def stage2_matrix_for(self, X: np.ndarray, keys: np.ndarray) -> np.ndarray:
    """診斷用：全列 [X | stage-1 分數 | gcode]（missing group 一律 raise）。
    與 predict_routed 的 compose 塊同一套 stage2.py helper，欄序同 stage2_matrix。"""
    from recsys_tfb.models.staged.stage2 import (
        encode_group_codes, group_code_lookup, stage2_matrix,
    )
    if self._stage2 is None:
        raise NotImplementedError("stage2_matrix_for requires a stage-2 model")
    scores, _ = self._stage1_scores(X, np.asarray(keys, dtype=object), "raise")
    lookup = group_code_lookup(self._groups)
    gcodes = encode_group_codes(np.asarray(keys, dtype=object), lookup)
    return stage2_matrix(X, scores, gcodes)
```

3. **`booster` property**＋**`feature_importance`**（取代 :262-265 的 raise）：

```python
@property
def booster(self):
    """attribution._resolve_booster 契約：stage2 存在＝診斷掛 Stage-2 booster。"""
    if self._stage2 is None:
        raise NotImplementedError(
            "staged(stage2=none) has no single booster; per-group "
            "diagnostics iterate the group adapters instead")
    return self._stage2.booster

def feature_importance(self, kind: str = "split") -> dict:
    if self._stage2 is None:
        raise NotImplementedError(
            "staged(stage2=none) importance is per-group; see "
            "compute_staged_group_diagnostics")
    return self._stage2.feature_importance(kind)
```

4. **`log_to_mlflow`**（取代 :267-271 的 no-op）：

```python
def log_to_mlflow(self) -> None:
    if self._stage2 is not None:
        self._stage2.log_to_mlflow()
        return
    logger.info(
        "staged(stage2=none): per-group boosters live in the model bundle; "
        "no single MLflow model is logged")
```

- [ ] **Step 4: GREEN ＋ 回歸**

Run: `... -m pytest tests/test_models/test_staged/test_adapter.py -q`
預期全綠（既有 compose／persistence 測試守住 `_stage1_scores` 搬移不改行為）。

- [ ] **Step 5: mutation check**

`_stage1_scores` 搬移的 raise 分支：把 `on_missing == "raise"` 條件改成 `False` → 既有 missing-group raise 測試須紅 → 改回。`stage2_matrix_for`：把 `stage2_matrix(X, scores, gcodes)` 的 `scores` 換成 `np.zeros_like(scores)` → `test_stage2_matrix_for_matches_predict_routed_compose` 須紅 → 改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(staged): adapter diagnostics surface (booster/importance delegate, stage2_matrix_for, mlflow)"
```

---

### Task 4: diagnosis/model/staged.py 基礎 ＋ Stage-1 總覽表

**Files:**
- Create: `src/recsys_tfb/diagnosis/model/staged.py`
- Modify: `conf/base/catalog.yaml`（`gain_ledger` 條目後加 `stage1_overview`）
- Test: `tests/test_diagnosis/test_staged_overview.py`（新檔；目錄名以 `ls tests/` 對齊既有 diagnosis 測試位置——若既有慣例是 `tests/test_diagnosis/` 就用它，否則跟著 `compute_gain_ledger` 測試檔所在目錄放）

- [ ] **Step 1: 失敗測試**

```python
import numpy as np

from recsys_tfb.diagnosis.model.staged import (
    compute_stage1_overview, has_stage2, is_staged, model_scores,
    resolve_attribution_inputs,
)


class _SharedLike:
    def predict(self, X):
        return np.full(len(X), 0.5)


class TestDispatchHelpers:
    def test_shared_adapter_is_not_staged(self):
        m = _SharedLike()
        assert not is_staged(m) and not has_stage2(m)

    def test_passthrough_for_shared(self):
        m = _SharedLike()
        X = np.zeros((3, 2))
        X_eff, cols = resolve_attribution_inputs(m, None, X, ["a", "b"])
        assert X_eff is X and cols == ["a", "b"]
        assert np.allclose(model_scores(m, None, X), 0.5)


class TestStage1Overview:
    REPORT = {"partition_keys": ["prod_name"],
              "groups": {"b": {"n_rows": 10, "n_pos": 3, "score": 0.7,
                               "metric": "auc", "train_seconds": 1.5,
                               "best_params": {"num_leaves": 7}},
                         "a": {"n_rows": 20, "n_pos": 0, "score": 0.6,
                               "metric": "auc", "train_seconds": 0.5,
                               "best_params": {}}}}

    def test_rows_sorted_and_totals_add_up(self):
        out = compute_stage1_overview(self.REPORT, {"model_version": "t"})
        assert [r["group"] for r in out["groups"]] == ["a", "b"]
        assert out["n_groups"] == 2 and out["total_rows"] == 30
        assert out["total_positives"] == 3
        assert out["groups"][1]["pos_rate"] == 0.3
        assert "stage2" not in out

    def test_stage2_summary_attached_when_present(self):
        s2 = {"mode": "lambdarank", "oof_folds": 5, "oof_rows": 100,
              "n_groups": 2, "best_params": {"num_leaves": 9}, "extra": "x"}
        out = compute_stage1_overview(self.REPORT, {"model_version": "t"}, s2)
        assert out["stage2"]["mode"] == "lambdarank"
        assert "extra" not in out["stage2"]
```

- [ ] **Step 2: RED**

Run: `... -m pytest <新測試檔> -x -q`
預期 RED：`ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.model.staged'`

- [ ] **Step 3: 實作 `src/recsys_tfb/diagnosis/model/staged.py`**

```python
"""staged 模型診斷：分派 helper、Stage-1 總覽表、per-group runner（Task 7）。

分派契約（D-C3）：duck-typing，不 import staged 型別做 isinstance——
``LightGBMAdapter`` 沒有 ``predict_routed``／``stage2_mode``，shared 路徑
逐位元不變。
"""

import logging

import numpy as np

logger = logging.getLogger(__name__)


def is_staged(model) -> bool:
    return hasattr(model, "predict_routed")


def has_stage2(model) -> bool:
    return getattr(model, "stage2_mode", "none") != "none"


def _routing_keys_for(model, pdf) -> np.ndarray:
    from recsys_tfb.models.staged.partition import routing_keys

    return routing_keys(pdf, model.partition_keys)


def resolve_attribution_inputs(model, pdf, X, feature_cols):
    """SHAP 系函式的輸入分派：stage2 存在 → ([X|s1|gcode], 名單+尾兩欄)；
    其他模型原樣通過。``pdf`` 需帶 partition key 欄（model_input cache 契約，
    spec §5：partition_keys ⊆ identity ∪ carry_columns）。"""
    if not has_stage2(model):
        return X, list(feature_cols)
    from recsys_tfb.models.staged.stage2 import stage2_feature_names

    keys = _routing_keys_for(model, pdf)
    return model.stage2_matrix_for(X, keys), stage2_feature_names(feature_cols)


def model_scores(model, pdf, X) -> np.ndarray:
    """模型分數（診斷抽樣列）：staged 走 routed（eval 資料缺群＝異常，raise）。"""
    if not is_staged(model):
        return model.predict(X)
    keys = _routing_keys_for(model, pdf)
    return model.predict_routed(X, keys, on_missing="raise")[0]


_STAGE2_SUMMARY_KEYS = ("mode", "oof_folds", "oof_rows", "n_groups", "best_params")


def compute_stage1_overview(stage1_groups_report, parameters: dict,
                            stage2_report=None) -> dict:
    """Stage-1 總覽表（spec §6）：每群一列＋彙總。只重排 stage1_groups.json
    既有事實不重算；只呈現資料不下結論（diagnosis-report-presentation.md）。
    ``stage2_report`` 走 trailing-default（pipeline.py log_experiment 同慣例）。"""
    groups = stage1_groups_report.get("groups", {})
    rows = []
    for key in sorted(groups):
        g = groups[key]
        n_rows, n_pos = int(g["n_rows"]), int(g["n_pos"])
        rows.append({
            "group": key,
            "n_rows": n_rows,
            "n_pos": n_pos,
            "pos_rate": (n_pos / n_rows) if n_rows else None,
            "metric": g.get("metric"),
            "score": g.get("score"),
            "train_seconds": g.get("train_seconds"),
            "best_params": g.get("best_params", {}),
        })
    out = {
        "partition_keys": stage1_groups_report.get("partition_keys"),
        "n_groups": len(rows),
        "total_rows": int(sum(r["n_rows"] for r in rows)),
        "total_positives": int(sum(r["n_pos"] for r in rows)),
        "total_train_seconds": float(sum(r["train_seconds"] or 0.0 for r in rows)),
        "groups": rows,
    }
    if stage2_report:
        out["stage2"] = {k: stage2_report[k] for k in _STAGE2_SUMMARY_KEYS
                         if k in stage2_report}
    logger.info("stage1 overview: %d group(s), stage2=%s",
                len(rows), bool(stage2_report))
    return out
```

catalog.yaml（`gain_ledger` 條目後）：

```yaml
stage1_overview:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/stage1_overview.json
```

- [ ] **Step 4: GREEN**（同 Step 2 指令，預期全綠）

- [ ] **Step 5: mutation check**

`total_positives` 的 `sum(r["n_pos"] ...)` 改成 `0` → `test_rows_sorted_and_totals_add_up` 須紅 → 改回。`_STAGE2_SUMMARY_KEYS` 過濾拿掉（直接 `dict(stage2_report)`）→ `test_stage2_summary_attached_when_present` 的 `"extra" not in` 須紅 → 改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(diagnosis): staged dispatch helpers + stage1 overview artifact"
```

---

### Task 5: SHAP 三函式 staged 分支（stage2 存在時掛 Stage-2）

**Files:**
- Modify: `src/recsys_tfb/diagnosis/model/shap_per_item.py`
- Modify: `src/recsys_tfb/diagnosis/model/shap_cases.py`
- Test: `tests/test_diagnosis/test_staged_shap_dispatch.py`（新檔，跟 Task 4 同目錄）

**行為不變宣稱：shared 路徑（`LightGBMAdapter`）逐位元不變**——分派 helper 對非 staged 模型是恆等通過；本 task 結束後既有 shap／quadrant 測試必須原樣全綠。

- [ ] **Step 1: 失敗測試**

用假 staged 模型驗「診斷真的餵了 X2＋擴充名單」，不用真 booster（結構性斷言，非數值等價——兩路在小資料上可能數學等價）：

```python
import numpy as np
import pandas as pd

from recsys_tfb.diagnosis.model.staged import resolve_attribution_inputs


class _FakeStaged:
    stage2_mode = "binary"
    partition_keys = ["grp"]

    def predict_routed(self, X, keys, on_missing="raise"):
        return np.arange(len(X), dtype=float), np.ones(len(X), bool)

    def stage2_matrix_for(self, X, keys):
        s1 = np.arange(len(X), dtype=float)
        g = np.zeros(len(X))
        return np.column_stack([np.asarray(X, float), s1, g])


class TestResolveForStaged:
    def test_matrix_gets_two_extra_columns_and_names(self):
        pdf = pd.DataFrame({"grp": ["a", "a"], "f1": [0.0, 1.0]})
        X = pdf[["f1"]].to_numpy(float)
        X_eff, cols = resolve_attribution_inputs(_FakeStaged(), pdf, X, ["f1"])
        assert X_eff.shape == (2, 3)
        assert cols == ["f1", "stage1_score", "partition_gcode"]
        assert np.allclose(X_eff[:, 1], [0.0, 1.0])   # s1 欄真的來自 stage2_matrix_for


class TestShapNodeUsesResolvedInputs:
    def test_take_cols_include_partition_keys(self, tmp_path, monkeypatch):
        # 對 compute_shap_diagnostics 做 spy：feature_attributions 收到的
        # X 第二維 = len(feature_cols)+2（結構性證據：走了 staged 分支）。
        # fixture：仿 tests 既有 shap 測試造小 parquet（含 grp 欄）＋ stub
        # feature_attributions／attribution_budget_units（monkeypatch 模組屬性），
        # model=_FakeStaged 增 booster stub。斷言 spy 記錄的 shape[1] == n_base+2。
        ...
```

（第二個測試的 fixture 較長：照 `tests/` 內既有 `compute_shap_diagnostics` 測試檔的 parquet fixture 抄結構，加一個 `grp` 欄並 monkeypatch `shap_per_item.feature_attributions` 為記錄 shape 的 stub。關鍵斷言只有兩條：`spy_shapes[0][1] == n_base + 2`、傳入的 `feature_cols` 尾兩項是 `stage1_score/partition_gcode`。）

- [ ] **Step 2: RED**

預期 RED：第二個測試 spy 收到 `shape[1] == n_base`（未走 staged 分支）；第一個測試通過（Task 4 已實作 helper——它是本 task 的前提回歸鎖，留著）。

- [ ] **Step 3: 實作**

`shap_per_item.py`：

1. import 區加：`from .staged import model_scores, resolve_attribution_inputs`（**放函式內 lazy import 亦可**——staged.py 內對 models 的 import 已是 lazy，無循環風險；頂層 import 即可）。
2. `compute_shap_diagnostics` 的 take_cols 段（:153-157）之後補 partition key 欄：

```python
    for col in (getattr(model, "partition_keys", None) or []):
        if col in names and col not in take_cols:
            take_cols.append(col)
```

3. `X = _pdf_to_X(...)`／`scores = model.predict(X)`（:163-164）改為（保留 base X 供評分，SHAP 用 resolved 版）：

```python
    X_base = _pdf_to_X(sample_pdf, preprocessor, parameters)
    scores = model_scores(model, sample_pdf, X_base)
    X, feature_cols = resolve_attribution_inputs(model, sample_pdf, X_base, feature_cols)
```

（`model_scores` 對 staged 用 `predict_routed`，吃 **base X**；`resolve_attribution_inputs` 產出 SHAP 用的 X2。shared 路徑：`model_scores` ≡ `model.predict(X_base)`、resolve ≡ 恆等——與原始碼字面等價。）此後函式內所有 `X`／`feature_cols` 均為 resolved 版本，下游（`_signed_profile`、plots、divergence）零改動。

4. `_positive_profiles`（:68-100）同步：簽名尾端加 `model_obj_pdf_cols=None` **不必**——它自建 `pos_pdf`，直接在 `X_pos = _pdf_to_X(...)`（:87）之後加：

```python
    X_pos, feature_cols = resolve_attribution_inputs(model, pos_pdf, X_pos, feature_cols)
```

並確認 `take_cols` 由呼叫端傳入（已含 partition keys，經第 2 點）。

`shap_cases.py`：兩個函式的 `X = _pdf_to_X(pdf, preprocessor, parameters)`（:43、:152）之後各加一行：

```python
        X, feature_cols = resolve_attribution_inputs(model, pdf, X, feature_cols)
```

（population／case_rows 由 `select_shap_population` join `test_model_input` 產生，天然帶 partition key 欄——spec §5 保證 partition_keys ⊆ model_input 欄。）

- [ ] **Step 4: GREEN ＋ 回歸（shared byte-identical 宣稱）**

Run: 新測試檔＋既有 shap／cases／quadrant 測試檔全部（以 `grep -rl "compute_shap_diagnostics\|compute_quadrant" tests/ | sort` 列出實際檔名逐一跑）。預期全綠。

- [ ] **Step 5: mutation check**

把 `compute_shap_diagnostics` 內 `resolve_attribution_inputs` 那行刪掉 → spy 測試須紅（shape 少 2 欄）→ 改回。把 take_cols 補 partition keys 的迴圈刪掉 → spy 測試須紅（`routing_keys` KeyError）→ 改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(diagnosis): SHAP family dispatches staged stage-2 matrix via resolve_attribution_inputs"
```

---

### Task 6: feature_stats 抽出 `_stats_from_pdf`（機械重構）

**Files:**
- Modify: `src/recsys_tfb/diagnosis/model/feature_stats.py`
- Test: 既有 feature_stats 測試檔（跑之，不新增——行為不變）

- [ ] **Step 1: baseline**

Run: `grep -rl "compute_feature_statistics" tests/` 找出測試檔，先跑一次記錄 baseline（全綠）。

- [ ] **Step 2: 重構**

把 :41-57 的統計迴圈抽成模組級函式，`compute_feature_statistics` 改呼叫它：

```python
def _stats_from_pdf(pdf, feature_cols, high_null_threshold: float) -> dict:
    """統計核心（per-group runner 共用；輸出與抽出前逐位元相同）。"""
    stats: dict = {}
    for col in feature_cols:
        s = pdf[col]
        null_rate = float(s.isna().mean())
        n_distinct = int(s.nunique(dropna=True))
        entry = {
            "null_rate": null_rate,
            "n_distinct": n_distinct,
            "single_value": n_distinct <= 1,
            "high_null": null_rate >= high_null_threshold,
        }
        if pd.api.types.is_numeric_dtype(s):
            entry["mean"] = _to_native(s.mean())
            entry["std"] = _to_native(s.std())
            entry["min"] = _to_native(s.min())
            entry["max"] = _to_native(s.max())
        stats[col] = entry
    return stats
```

`compute_feature_statistics` 尾段改為：

```python
    stats = _stats_from_pdf(pdf, feature_cols, high_null_threshold)
    logger.info("feature_statistics: %d features summarized", len(stats))
    return stats
```

- [ ] **Step 3: 驗證＝baseline 完全一致**

Run: Step 1 同款指令，結果須與 baseline 相同。

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "refactor(diagnosis): extract _stats_from_pdf for per-group reuse (behavior-preserving)"
```

---

### Task 7: stage2=none per-group runner ＋ paths helper

**Files:**
- Modify: `src/recsys_tfb/diagnosis/model/paths.py`（加 `staged_group_dir`）
- Modify: `src/recsys_tfb/diagnosis/model/staged.py`（加 runner）
- Modify: `conf/base/catalog.yaml`（加 `staged_group_diagnostics`）
- Test: `tests/test_diagnosis/test_staged_group_diagnostics.py`（新檔）

- [ ] **Step 1: 失敗測試**

fixture：真 LightGBM per-group 小模型（每群 60×3、5 棵樹，仿 `test_train_stage1` fixture）組進 `StagedModelAdapter`（無 stage2）；train/test 各寫一個含 `grp` 欄＋特徵欄＋item/label 欄的小 parquet（仿既有 shap 測試 fixture）；`parameters` 含 `model_version`、schema、`training.staged.stage1.partition_keys: ["grp"]`、diagnostics 全 enabled、`shap.sample_rows: 50`。

```python
class TestStagedGroupDiagnostics:
    def test_artifacts_written_per_group_and_manifest_indexes_them(self, staged_fixture, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)  # diagnostics_dir 是相對 data/ 路徑
        manifest = compute_staged_group_diagnostics(
            model, train_handle, test_handle, preprocessor_view, parameters)
        assert set(manifest["groups"]) == {slug_a, slug_b}   # group_slug(key)
        for slug, entry in manifest["groups"].items():
            d = tmp_path / "data" / "models" / "tmv" / "diagnostics" / "groups" / slug
            assert (d / "feature_importance.json").exists()
            assert (d / "gain_ledger.json").exists()
            assert (d / "feature_statistics.json").exists()
            assert (d / "shap_top_features.json").exists()
            assert entry["error"] is None
            assert entry["n_train_rows"] > 0 and entry["n_shap_sampled"] > 0
        # importance 鍵 = 真特徵名（Task 1 的果實）
        imp = json.loads((d / "feature_importance.json").read_text())
        assert set(map(str, imp["ranked"][0].keys())) if False else True  # 依實際 schema 斷言：
        # compute_feature_importance 回 {"ranked":[{"feature":..,"split":..,"gain":..},...],...}
        # → 斷言 ranked 內 feature 名 ⊆ preprocessor feature_columns
    def test_one_bad_group_is_isolated_not_fatal(self, staged_fixture, monkeypatch):
        # 把其中一群的 adapter 換成 predict/booster 會炸的 stub →
        # 該群 entry["error"] 非 None、其他群照常產出
        ...
    def test_missing_partition_column_fails_loud(self, staged_fixture):
        # test parquet 沒有 partition key 欄 → raise（schema 問題不 best-effort）
        with pytest.raises(KeyError):
            compute_staged_group_diagnostics(...)
```

（`compute_feature_importance` 的實際回傳鍵在 importance.py:8-22——實作前先 read-back 該函式，斷言照真實鍵寫，**不憑本計畫記憶**。）

- [ ] **Step 2: RED**

預期 RED：`ImportError: cannot import name 'compute_staged_group_diagnostics'`

- [ ] **Step 3: 實作**

paths.py 加：

```python
def staged_group_dir(parameters: dict, slug: str) -> Path:
    """Resolve（並建立）diagnostics/groups/<slug>/ —— stage2=none 每群診斷產物。"""
    d = diagnostics_dir(parameters) / "groups" / slug
    d.mkdir(parents=True, exist_ok=True)
    return d
```

staged.py 加 runner（import 區補 `import json`, `import time`, `from recsys_tfb.core.logging import log_step`）：

```python
def compute_staged_group_diagnostics(model, train_parquet_handle,
                                     test_parquet_handle, preprocessor: dict,
                                     parameters: dict) -> dict:
    """stage2=none：每群核心四件（feature stats／importance／gain ledger／
    SHAP summary），落 diagnostics/groups/<slug>/，回傳 manifest。

    範圍裁決（2026-07-26，偏離 spec §6「完整」）：象限系列不做 per-group 版。
    單群失敗隔離記 error 不中斷（比照 quadrant best-effort 慣例）；但 partition
    key 欄缺失是 schema 問題 → fail-loud。
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap as shap_pkg

    from recsys_tfb.io.extract import _pdf_to_X
    from recsys_tfb.models.staged.partition import group_slug, routing_keys

    from . import data_access
    from .attribution import feature_attributions
    from .feature_stats import _stats_from_pdf
    from .gain_ledger import compute_gain_ledger
    from .importance import compute_feature_importance
    from .paths import staged_group_dir
    from .shap_per_item import _signed_profile

    shap_cfg = parameters.get("diagnostics", {}).get("shap", {})
    fs_cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    top_k = int(shap_cfg.get("top_k", 30))
    shap_rows = int(shap_cfg.get("sample_rows", 2000))
    fs_rows = int(fs_cfg.get("sample_rows", 500000))
    high_null = float(fs_cfg.get("high_null_threshold", 0.5))
    feature_cols = list(preprocessor["feature_columns"])
    pkeys = list(model.partition_keys)

    def _keys_of(path):
        # 只讀 partition key 欄；routing_keys 走與訓練同一 str-join 慣例
        import pandas as pd
        cols = {c: data_access.read_column(path, c) for c in pkeys}
        return routing_keys(pd.DataFrame(cols), pkeys)

    train_path = train_parquet_handle.path
    test_path = test_parquet_handle.path
    keys_tr = _keys_of(train_path)
    keys_te = _keys_of(test_path)

    manifest: dict = {"partition_keys": pkeys, "groups": {}}
    with log_step(logger, "staged_group_diagnostics"):
        for key in model.group_keys:
            slug = group_slug(key)
            t0 = time.monotonic()
            entry = {"group": key, "error": None}
            try:
                adapter = model._groups[key]
                gdir = staged_group_dir(parameters, slug)
                # 1) importance（真特徵名，Task 1）
                imp = compute_feature_importance(adapter, parameters)
                (gdir / "feature_importance.json").write_text(
                    json.dumps(imp, ensure_ascii=False, indent=1))
                # 2) gain ledger（函式只回 dict、不落檔 → 自己寫）
                ledger = compute_gain_ledger(adapter, preprocessor, parameters)
                (gdir / "gain_ledger.json").write_text(
                    json.dumps(ledger, ensure_ascii=False, indent=1))
                # 3) per-group train feature stats
                tr_idx = np.flatnonzero(keys_tr == key)
                entry["n_train_rows"] = int(tr_idx.size)
                if tr_idx.size > fs_rows:
                    tr_idx = np.sort(np.random.RandomState(42).choice(
                        tr_idx, size=fs_rows, replace=False))
                pdf_tr = data_access.take_rows(train_path, tr_idx, columns=feature_cols)
                stats = _stats_from_pdf(pdf_tr, feature_cols, high_null)
                (gdir / "feature_statistics.json").write_text(
                    json.dumps(stats, ensure_ascii=False, indent=1))
                # 4) SHAP summary（test 側、該群列；每群上限 shap.sample_rows）
                te_idx = np.flatnonzero(keys_te == key)
                if te_idx.size > shap_rows:
                    te_idx = np.sort(np.random.RandomState(42).choice(
                        te_idx, size=shap_rows, replace=False))
                entry["n_shap_sampled"] = int(te_idx.size)
                if te_idx.size:
                    pdf_te = data_access.take_rows(test_path, te_idx,
                                                   columns=feature_cols)
                    X_g = _pdf_to_X(pdf_te, preprocessor, parameters)
                    sv = feature_attributions(adapter, X_g, feature_cols)
                    prof, _ = _signed_profile(sv, feature_cols, top_k)
                    (gdir / "shap_top_features.json").write_text(
                        json.dumps({"top_features": prof}, ensure_ascii=False,
                                   indent=1))
                    try:  # 圖 best-effort（比照 shap_per_item 慣例）
                        plt.figure()
                        try:
                            shap_pkg.summary_plot(sv, features=X_g,
                                                  feature_names=feature_cols,
                                                  show=False)
                            plt.tight_layout()
                            plt.savefig(gdir / "shap_summary.png", dpi=100)
                        finally:
                            plt.close()
                    except Exception as e:
                        logger.warning("group %s beeswarm failed: %s", slug, e)
            except Exception as e:  # 單群隔離：一群壞不拖垮其他群
                logger.warning("staged group diagnostics failed for %r: %s",
                               key, e)
                entry["error"] = f"{type(e).__name__}: {e}"
            entry["seconds"] = round(time.monotonic() - t0, 2)
            # 心跳：公司規模群多，逐群留時間戳（PR-B observability 同理由）
            logger.info("staged diagnostics group=%r slug=%s (%.1fs) error=%s",
                        key, slug, entry["seconds"], entry["error"])
            manifest["groups"][slug] = entry
    return manifest
```

注意：`_keys_of` 缺 partition 欄時 `read_column` 直接炸（KeyError/ArrowInvalid）——在群迴圈**之外**，符合 fail-loud 要求。`test_missing_partition_column_fails_loud` 的預期例外型別以實跑為準（pyarrow 可能拋非 KeyError——RED 時看到實際型別後把測試斷言改成該型別並回報）。

catalog.yaml（`stage1_overview` 後）：

```yaml
staged_group_diagnostics:
  type: JSONDataset
  filepath: data/models/${model_version}/diagnostics/staged_groups_manifest.json
```

- [ ] **Step 4: GREEN**（新測試檔全綠）

- [ ] **Step 5: mutation check**

`fit set` 類的假綠風險在這裡是「群過濾」：把 `np.flatnonzero(keys_te == key)` 改成 `np.arange(len(keys_te))` → `test_artifacts_written_per_group_and_manifest_indexes_them` 的 `n_shap_sampled` 斷言若擋不住（兩群列數可能相同）→ **fixture 兩群列數必須刻意不同**（例 60 vs 80），斷言 `entry["n_train_rows"]` 等於各群實際列數。確認弄壞後轉紅再改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(diagnosis): stage2=none per-group core diagnostics runner + manifest"
```

---

### Task 8: `log_staged_experiment`（MLflow 單一 run）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`（`log_experiment` 之後）
- Test: `tests/test_pipelines/test_training/` 內放 log_experiment 測試的同一檔（`grep -rl "log_experiment" tests/` 對齊）

- [ ] **Step 1: 失敗測試**

比照既有 log_experiment 測試的 mock 手法（monkeypatch mlflow）：

```python
class TestLogStagedExperiment:
    def test_single_run_params_metrics_artifacts(self, monkeypatch, tmp_path):
        calls = _install_mlflow_spy(monkeypatch)   # 比照既有測試的 spy helper
        overview = {"partition_keys": ["grp"], "n_groups": 2,
                    "stage2": {"mode": "binary", "best_params": {"num_leaves": 7}}}
        log_staged_experiment(_model_stub(), {"groups": {}},
                              {"overall_map": 0.5, "n_queries": 10,
                               "n_excluded_queries": 0, "per_item_map_attr": {"p": 0.1}},
                              overview, _params(tmp_path))
        assert calls.params["model_structure"] == "staged"
        assert calls.params["stage2_mode"] == "binary"
        assert calls.params["num_leaves"] == 7          # stage2 best_params 攤平
        assert calls.metrics["overall_map"] == 0.5
        assert calls.metrics["map_attr_p"] == 0.1
        assert calls.model_logged                        # model.log_to_mlflow 被呼叫
        assert calls.artifacts_dir.endswith("diagnostics")

    def test_stage2_absent_logs_mode_none(self, ...):
        # overview 無 "stage2" 鍵 → stage2_mode="none"、不 log best_params
        ...

    def test_best_effort_swallow_when_not_strict(self, ...):
        # mlflow.set_tracking_uri 炸 → 不 raise（strict=False 預設），log warning
        ...
```

- [ ] **Step 2: RED**：`ImportError: cannot import name 'log_staged_experiment'`

- [ ] **Step 3: 實作（nodes.py，`log_experiment` 函式後）**

```python
def log_staged_experiment(
    model: ModelAdapter,
    stage1_groups_report: dict,
    evaluation_results: dict,
    stage1_overview: dict,
    parameters: dict,
    *diag_deps,
) -> None:
    """staged 版 MLflow logging：單一 run（2026-07-26 使用者裁決）。

    per-group 細節不進 params（會爆 UI）——隨 diagnostics/ artifacts 上傳
    （stage1_overview.json 就是總覽表）。``*diag_deps`` 是 ordering-only 依賴
    （Runner 位置展開，core/runner.py:92）：保證 catalog 已把診斷 JSON 寫進
    diagnostics/ 後才 log_artifacts，值本身不使用。stage2 資訊讀
    ``stage1_overview["stage2"]``（兩種 DAG 形狀共用本函式的關鍵）。
    """
    from recsys_tfb.diagnosis.model import diagnostics_dir
    mlflow_params = parameters.get("mlflow", {})
    tracking_uri = mlflow_params.get("tracking_uri", "mlruns")
    experiment_name = mlflow_params.get("experiment_name", "recsys_tfb")
    strict = mlflow_params.get("strict", False)
    training_cfg = parameters.get("training", {})
    staged_cfg = (training_cfg.get("staged") or {})
    stage2_info = stage1_overview.get("stage2") or {}

    try:
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        with log_step(logger, "mlflow_log_staged"):
            with mlflow.start_run():
                mlflow.log_param("model_structure", "staged")
                mlflow.log_param("algorithm",
                                 training_cfg.get("algorithm", "lightgbm"))
                mlflow.log_param("partition_keys", ",".join(
                    stage1_overview.get("partition_keys") or []))
                mlflow.log_param("n_groups", stage1_overview.get("n_groups"))
                mlflow.log_param("stage2_mode",
                                 stage2_info.get("mode", "none"))
                if stage2_info.get("best_params"):
                    mlflow.log_params(stage2_info["best_params"])
                mlflow.log_metric("overall_map",
                                  evaluation_results["overall_map"])
                for item, attr in evaluation_results.get(
                        "per_item_map_attr", {}).items():
                    mlflow.log_metric(f"map_attr_{item}", attr)
                mlflow.log_metric("n_queries", evaluation_results["n_queries"])
                mlflow.log_metric("n_excluded_queries",
                                  evaluation_results["n_excluded_queries"])
                model.log_to_mlflow()
                diag_dir = diagnostics_dir(parameters)
                mlflow.log_artifacts(str(diag_dir))
        logger.info("staged experiment logged to MLflow (%s)", experiment_name)
    except Exception as e:
        if strict:
            raise
        logger.warning("staged MLflow logging skipped (best-effort): %s", e)
    _ = staged_cfg  # partition_keys 已由 overview 提供；保留讀取點供日後擴充
```

（最後一行若 lint 嫌棄就把 `staged_cfg` 讀取整段拿掉——以 repo lint 實況為準。`diagnostics_dir` 的 import 路徑以 `log_experiment` 現行寫法為準：nodes.py 內是 `from recsys_tfb.diagnosis.model import diagnostics_dir`——先 read-back 確認再照抄。）

- [ ] **Step 4: GREEN ＋ 既有 log_experiment 測試回歸**

- [ ] **Step 5: mutation check**

把 `mlflow.log_artifacts(str(diag_dir))` 刪掉 → `test_single_run_params_metrics_artifacts` 的 artifacts 斷言須紅 → 改回。把 `model.log_to_mlflow()` 刪掉 → `calls.model_logged` 斷言須紅 → 改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(training): log_staged_experiment — single-run MLflow logging for staged"
```

---

### Task 9: DAG 接回（兩種 staged 形狀）

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`（`_create_staged_pipeline`）
- Test: `tests/test_pipelines/test_training/test_staged_pipeline.py`（追加 shape 斷言）

- [ ] **Step 1: 失敗測試（shape 斷言，追加）**

```python
class TestStagedDiagnosticsShape:
    def test_none_shape_has_overview_stats_group_runner_and_mlflow(self):
        p = create_pipeline(model_structure="staged", stage2_mode="none")
        names = [n.func.__name__ for n in p.nodes]
        assert "compute_stage1_overview" in names
        assert "compute_feature_statistics" in names
        assert "compute_staged_group_diagnostics" in names
        assert "log_staged_experiment" in names
        assert "compute_shap_diagnostics" not in names          # 象限/booster 診斷不在 none
        assert "compute_quadrant_profiles" not in names

    def test_stage2_shape_reuses_booster_diagnostics_nodes(self):
        p = create_pipeline(model_structure="staged", stage2_mode="lambdarank")
        names = [n.func.__name__ for n in p.nodes]
        for expected in ("compute_stage1_overview", "compute_feature_statistics",
                         "compute_feature_importance", "compute_gain_ledger",
                         "compute_shap_diagnostics", "select_shap_population",
                         "compute_quadrant_profiles", "compute_quadrant_cases",
                         "log_staged_experiment"):
            assert expected in names
        assert "compute_staged_group_diagnostics" not in names
        assert "log_experiment" not in names                     # shared 版不進 staged

    def test_shared_shape_unchanged(self):
        p = create_pipeline()
        names = [n.func.__name__ for n in p.nodes]
        assert "log_experiment" in names and "log_staged_experiment" not in names
```

- [ ] **Step 2: RED**：`compute_stage1_overview not in names`（AssertionError）

- [ ] **Step 3: 實作**

pipeline.py import 區加：

```python
from recsys_tfb.diagnosis.model.staged import (
    compute_stage1_overview,
    compute_staged_group_diagnostics,
)
from recsys_tfb.pipelines.training.nodes import log_staged_experiment  # 併入既有 nodes import 清單
```

`_create_staged_pipeline` 的 mAP 節點（`compute_test_mAP_spark`）之後、`return` 之前追加（docstring 的「Excluded (PR-C)」段同步改寫為現況）：

```python
    # ---- PR-C 診斷（兩形狀共通）----
    nodes.append(
        Node(
            compute_feature_statistics,
            inputs=["train_parquet_handle", "preprocessor_view", "parameters"],
            outputs="feature_statistics",
        ),
    )
    nodes.append(
        Node(
            compute_stage1_overview,
            # stage2_report 走 trailing-default（log_experiment 同慣例）
            inputs=(["stage1_groups_report", "parameters", "stage2_report"]
                    if with_stage2 else ["stage1_groups_report", "parameters"]),
            outputs="stage1_overview",
        ),
    )
    if with_stage2:
        nodes.extend([
            Node(
                compute_feature_importance,
                inputs=["model", "parameters"],
                outputs="feature_importance",
            ),
            Node(
                compute_gain_ledger,
                inputs=["model", "preprocessor_view", "parameters"],
                outputs="gain_ledger",
            ),
            Node(
                compute_shap_diagnostics,
                inputs=["model", "test_parquet_handle", "preprocessor_view",
                        "parameters"],
                outputs="shap_diagnostics",
            ),
            Node(
                select_shap_population,
                # predict_manifest：ordering-only（shared DAG 同註解，:186-190）
                inputs=["training_eval_predictions", "test_model_input",
                        "parameters", "predict_manifest"],
                outputs=["shap_population", "case_rows"],
            ),
            Node(
                compute_quadrant_profiles,
                inputs=["model", "shap_population", "preprocessor_view",
                        "parameters"],
                outputs="quadrant_profiles",
            ),
            Node(
                compute_quadrant_cases,
                inputs=["model", "case_rows", "preprocessor_view", "parameters"],
                outputs="cases_manifest",
            ),
            Node(
                log_staged_experiment,
                # 第 6 位起=*diag_deps（ordering-only：保證 catalog 先落檔）
                inputs=["model", "stage1_groups_report", "evaluation_results",
                        "stage1_overview", "parameters", "feature_statistics",
                        "feature_importance", "gain_ledger", "shap_diagnostics",
                        "quadrant_profiles", "cases_manifest"],
                outputs=None,
            ),
        ])
    else:
        nodes.extend([
            Node(
                compute_staged_group_diagnostics,
                inputs=["model", "train_parquet_handle", "test_parquet_handle",
                        "preprocessor_view", "parameters"],
                outputs="staged_group_diagnostics",
            ),
            Node(
                log_staged_experiment,
                inputs=["model", "stage1_groups_report", "evaluation_results",
                        "stage1_overview", "parameters", "feature_statistics",
                        "staged_group_diagnostics"],
                outputs=None,
            ),
        ])
```

**注意**：`log_staged_experiment` 簽名的第 1-5 位是 (model, stage1_groups_report, evaluation_results, stage1_overview, parameters)，第 6 位起全部進 `*diag_deps`——上面兩形狀的 inputs 前五項順序必須逐字一致。

- [ ] **Step 4: GREEN ＋ 全 pipeline shape 回歸**

Run: `... -m pytest tests/test_pipelines/test_training/test_staged_pipeline.py tests/test_pipelines/test_training/test_pipeline.py -q`（第二檔以實際存在為準；shared shape 測試必須原樣綠）

- [ ] **Step 5: mutation check**

把 none 形狀的 `staged_group_diagnostics` ordering 輸入從 log 節點拿掉 → `test_none_shape...` 若擋不住（它只看 node 名單）→ 追加斷言：

```python
        log_node = next(n for n in p.nodes if n.func.__name__ == "log_staged_experiment")
        assert "staged_group_diagnostics" in log_node.inputs
```

確認轉紅再改回。

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(training): wire diagnostics + mlflow back into staged DAG (both shapes)"
```

---

### Task 10: real-run 驗證（controller 親自跑，不派 agent）

前置：pre-flight 指令塊（CLAUDE.md §Worktree）照抄執行。兩種 mode 都要**重訓**（Task 1/2 改了 booster 內容；舊 mv 的 booster 是 `Column_N` 名）。

- [ ] **Step 1: staged(stage2=none) e2e**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/staged-modeling
export SPARK_CONF_DIR=$PWD/conf/spark-local
# conf/local/parameters.yaml 設 model_structure: staged / stage2.mode: none（比照 PR-A e2e 設定）
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```

驗收（逐項 read-back，路徑代入實際 mv）：
- `data/models/<mv>/diagnostics/stage1_overview.json`：`n_groups` 與 `stage1_groups.json` 群數一致、`total_rows`＝各群加總、無 `stage2` 鍵。
- `diagnostics/groups/<slug>/` 每群四件齊；任一群 `feature_importance.json` 的 feature 名 ⊆ preprocessor feature_columns（**不是** `Column_N`）。
- `diagnostics/groups/<slug>/gain_ledger.json` 的 item 帳**非空**（feature_name 根修的實證；若空＝Task 1 沒生效，停下回報）。
- `staged_groups_manifest.json` 各群 `error: null`。
- log 出現 `staged diagnostics group=...` 逐群心跳與 `mlflow_log_staged`（或 best-effort warning）。
- 全域 `diagnostics/feature_statistics.json` 存在。

- [ ] **Step 2: staged(lambdarank) e2e**

```bash
# conf/local 改 stage2.mode: lambdarank（比照 PR-B e2e 設定）
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```

驗收：
- `diagnostics/feature_importance.json` 的鍵含 `stage1_score` 與 `partition_gcode`（s1 重要度視角——spec §6 的「新視角」由此免費取得）。
- `diagnostics/gain_ledger.json` item 帳非空；`shap_diagnostics.json` 的 global top_features 內出現 `stage1_score`（不強制排名，只驗名字在特徵空間裡）。
- `diagnostics/summary/shap_summary_global.png`、`per_item/`、`cases/`、`per_quadrant.json` 齊。
- `stage1_overview.json` 含 `stage2.mode: lambdarank` 與 `best_params`。
- 無 `diagnostics/groups/`（none 專屬）。

- [ ] **Step 3: eval 側相容實跑**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training
```

驗收：跑完無 error；report 產出；`model_capacity` 診斷讀到 staged 的 `gain_ledger.json`（stage2 形狀）不炸；stage2=none 的 mv 跑一次確認 `gain_ledger` catalog load 回 None 走正常路徑（catalog.yaml:252-254 註解宣稱的實證）。

- [ ] **Step 4: shared 回歸 baseline**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_models/test_staged tests/test_pipelines/test_training tests/test_diagnosis -q 2>&1 | tail -15
```

（diagnosis 測試目錄名以實際為準。）對照 main 既知 fail 清單（known-pitfalls.md §5）歸因，PR-C 造成的新 fail 必須為 0。

- [ ] **Step 5: Commit（如有 conf 調整殘留先還原）＋記錄 e2e 證據到 PR body 草稿**

---

### Task 11: 收尾

- [ ] graphify rebuild：`.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`
- [ ] `git push -u origin feat/staged-diagnostics`；開 PR（base=`feat/staged-stage2`，title `feat: staged modeling PR-C — diagnostics`），body 含：偏離清單（本計畫§偏離）、e2e 證據、兩輪審查結論。
- [ ] memory `project_two_stage_stacking.md` 更新 PR-C 狀態。

## Self-Review 記錄（writing-plans 要求）

- Spec 覆蓋：§6 表三情境──stage2 存在（Task 3/5/9）✔；stage2=none（Task 7，範圍經使用者裁決縮小，已記偏離）✔；shared 零變動（D-C3＋Task 5/9 回歸斷言）✔。Stage-1 總覽表（Task 4）✔。eval 相容實跑（Task 10）✔。HPO 搜尋診斷：Stage-2 沿用現行（PR-B 已接 `write_hpo_diagnostics`，本 PR 零改動）✔。
- 型別/識別字一致性：`stage2_feature_names`（Task 2 定義、Task 5/7 使用）、`stage2_matrix_for`（Task 3 定義、Task 5 經 resolve 使用）、`log_staged_experiment` 前五位參數順序（Task 8 定義、Task 9 wiring）已互相核對。
- 已知不確定（實作時 read-back，不憑計畫記憶）：`compute_feature_importance` 回傳 dict 的實際鍵名（importance.py:8-22）；diagnosis 測試目錄實名；`_keys_of` 缺欄的實際例外型別。
