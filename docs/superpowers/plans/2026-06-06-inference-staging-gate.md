# Inference staging → validate → publish gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 inference pipeline 會寫資料的 `validate_predictions` 改成真正的發布閘門:`rank_predictions` 先寫 `ranked_staging`,`validate_predictions` 讀它跑 sanity check 失敗即 raise,通過後才由新節點 `publish_predictions` 寫 production `ranked_predictions`。

**Architecture:** DAG 線性化為 `score_table → ranked_staging → validated_predictions(中間態) → ranked_predictions`。消除「先發布後驗證」與 self-overwrite;`validated_predictions` 不再是 Hive 表(降為 auto-MemoryDataset);新增 `ranked_staging` managed Hive 表。evaluation 程式碼不動(介面不變)。

**Tech Stack:** PySpark 3.3.2、手刻 Kedro 風格 DataCatalog/Node/Pipeline/Runner、pytest。

**Spec:** `docs/superpowers/specs/2026-06-06-inference-staging-gate-design.md`

---

## 環境前置(已完成,執行前確認)

- worktree:`/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate`(branch `feat/inference-staging-gate`)
- venv symlink → `/Users/curtislu/projects/recsys_tfb/.venv`(Python 3.10.9),pre-flight 已過。
- **不需** data/ symlink:本計畫所有測試為 pure-Python 或用 `spark` fixture 的合成 in-memory 資料,不讀 `data/`。
- 所有測試/CLI 一律:`PYTHONPATH=<wt>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest ...`(裸跑會抓到 main 的 src)。
- commit 為 feature branch 本地 commit;**push 由使用者人工觸發**(spec 約定)。graphify post-commit hook 會自動重建 code graph,不需手動 rebuild。

簡寫(下文命令直接展開):
- `WT` = `/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate`
- `PY` = `/Users/curtislu/projects/recsys_tfb/.venv/bin/python`

---

## File Structure

| 檔案 | 動作 | 責任 |
|---|---|---|
| `tests/test_pipelines/test_inference/test_publish.py` | Create | `publish_predictions` 純 pass-through 的單元測試 |
| `src/recsys_tfb/pipelines/inference/nodes_spark.py` | Modify | 新增 `publish_predictions` 節點函式 |
| `tests/test_pipelines/test_inference/test_pipeline.py` | Modify | inference pipeline 結構/接線/拓樸順序斷言 |
| `src/recsys_tfb/pipelines/inference/pipeline.py` | Modify | 重新接線:rank→staging、validate 讀 staging、新增 publish |
| `tests/test_core/test_catalog_inference_entries.py` | Create | catalog 出現 `ranked_staging`、移除 `validated_predictions`、保留 `ranked_predictions` |
| `conf/base/catalog.yaml` | Modify | 新增 `ranked_staging`、移除 `validated_predictions`、更新 `ranked_predictions` 註解 |
| `README.md` | Modify | inference node 表補 validate/publish + staging;catalog 清單加 `ranked_staging` |
| `docs/design-principles.md` | Modify | §9 footgun 段更新 + 補 staging gate 設計原則 |
| `docs/pipelines/evaluation.md` | Modify | 監控讀的是「已發布的已驗證結果」 |
| `docs/data-lineage.html` | Modify | inference lineage 行 + 導覽 + `ranked_predictions` 卡片改寫為 gate |
| `docs/diagrams/pipeline-overview.svg` | Assess | 若畫了 node 串接才補 staging,否則不動 |

---

## Task 1: `publish_predictions` 節點

**Files:**
- Create: `tests/test_pipelines/test_inference/test_publish.py`
- Modify: `src/recsys_tfb/pipelines/inference/nodes_spark.py`(檔尾,接在 `validate_predictions` 之後)

- [ ] **Step 1: 寫 failing test**

Create `tests/test_pipelines/test_inference/test_publish.py`:

```python
"""Tests for the inference publish node (staging -> production promotion).

publish_predictions is the single production write in the staging->validate->
publish gate: rank writes ranked_staging, validate runs sanity checks on it
(raising before publish on failure), and only on success does publish promote
the validated DataFrame to the production ranked_predictions table. The node
itself is a pure pass-through; the production write is the catalog save of its
ranked_predictions output. These are pure-Python tests — no Spark needed.
"""

from recsys_tfb.pipelines.inference.nodes_spark import publish_predictions


def test_publish_returns_input_unchanged():
    """Must return the validated DataFrame untouched so the catalog save of
    ranked_predictions writes exactly the validated rows."""
    sentinel = object()
    result = publish_predictions(sentinel, {"model_version": "abc12345"})
    assert result is sentinel


def test_publish_tolerates_missing_model_version():
    """Audit logging of model_version is best-effort; an absent key must not
    raise."""
    sentinel = object()
    result = publish_predictions(sentinel, {})
    assert result is sentinel
```

- [ ] **Step 2: 跑測試,確認 RED**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_pipelines/test_inference/test_publish.py -q
```
Expected: FAIL — collection error `ImportError: cannot import name 'publish_predictions' from 'recsys_tfb.pipelines.inference.nodes_spark'`(函式還沒寫)。

- [ ] **Step 3: 寫最小實作**

在 `src/recsys_tfb/pipelines/inference/nodes_spark.py` **檔尾**(`validate_predictions` 的 `return ranked_predictions` 之後)新增。`DataFrame` 與 `logger` 已在檔案頂部 import,不需新增 import:

```python


def publish_predictions(
    validated_predictions: DataFrame,
    parameters: dict,
) -> DataFrame:
    """Promote validated predictions to the production ``ranked_predictions`` table.

    Reached only after ``validate_predictions`` passes (the DAG edge runs through
    ``validated_predictions``), so a failed sanity check aborts the run before
    anything reaches production. This is the single production write: the
    pre-validation copy lives in ``ranked_staging`` and is left in place for
    post-mortem when validation fails. The write itself is the catalog save of
    this node's ``ranked_predictions`` output.
    """
    model_version = parameters.get("model_version")
    logger.info(
        "Publishing validated predictions to production ranked_predictions "
        "(model_version=%s)",
        model_version,
    )
    return validated_predictions
```

- [ ] **Step 4: 跑測試,確認 GREEN**

Run(同 Step 2 命令)。Expected: PASS(2 passed)。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
git add tests/test_pipelines/test_inference/test_publish.py \
        src/recsys_tfb/pipelines/inference/nodes_spark.py && \
git commit -m "feat(inference): add publish_predictions node (pass-through production write)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: 重新接線 inference pipeline(staging→validate→publish)

**Files:**
- Modify: `tests/test_pipelines/test_inference/test_pipeline.py`(整檔取代)
- Modify: `src/recsys_tfb/pipelines/inference/pipeline.py`(整檔取代)

- [ ] **Step 1: 改 failing test**

整檔取代 `tests/test_pipelines/test_inference/test_pipeline.py`:

```python
"""Tests for inference pipeline definition."""

from recsys_tfb.pipelines.inference import create_pipeline


class TestInferencePipeline:
    def test_pipeline_has_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 6

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        assert pipeline.inputs == {"feature_table", "parameters", "preprocessor", "model"}

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "scoring_dataset", "X_score", "score_table",
            "ranked_staging", "validated_predictions", "ranked_predictions",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "build_scoring_dataset" in names
        assert "apply_preprocessor" in names
        assert "predict_scores" in names
        assert "rank_predictions" in names
        assert "validate_predictions" in names
        assert "publish_predictions" in names

    def test_staging_validate_publish_chain(self):
        """rank 寫 staging、validate 讀 staging、publish 寫 production —— 證明
        production ranked_predictions 在驗證閘門的下游。"""
        pipeline = create_pipeline()
        by_output = {out: n for n in pipeline.nodes for out in n.outputs}
        # rank_predictions: score_table -> ranked_staging
        assert by_output["ranked_staging"].name == "rank_predictions"
        assert "score_table" in by_output["ranked_staging"].inputs
        # validate_predictions: ranked_staging -> validated_predictions
        assert by_output["validated_predictions"].name == "validate_predictions"
        assert "ranked_staging" in by_output["validated_predictions"].inputs
        # publish_predictions: validated_predictions -> ranked_predictions
        assert by_output["ranked_predictions"].name == "publish_predictions"
        assert "validated_predictions" in by_output["ranked_predictions"].inputs

    def test_publish_runs_after_validate(self):
        """拓樸順序保證 production 寫入發生在驗證閘門之後。"""
        pipeline = create_pipeline()
        order = [n.name for n in pipeline.nodes]
        assert order.index("rank_predictions") < order.index("validate_predictions")
        assert order.index("validate_predictions") < order.index("publish_predictions")
```

- [ ] **Step 2: 跑測試,確認 RED**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_pipelines/test_inference/test_pipeline.py -q
```
Expected: FAIL — `test_pipeline_has_nodes`(6≠5)、`test_pipeline_outputs`(舊 set 無 ranked_staging)、`test_node_names`(無 publish_predictions)、`test_staging_validate_publish_chain`(`KeyError: 'ranked_staging'`)、`test_publish_runs_after_validate`(`ValueError: 'publish_predictions' is not in list`)。

- [ ] **Step 3: 重新接線 pipeline**

整檔取代 `src/recsys_tfb/pipelines/inference/pipeline.py`:

```python
"""Inference pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline() -> Pipeline:
    from recsys_tfb.pipelines.inference.nodes_spark import (
        apply_preprocessor,
        build_scoring_dataset,
        predict_scores,
        publish_predictions,
        rank_predictions,
        validate_predictions,
    )

    return Pipeline(
        [
            Node(
                build_scoring_dataset,
                inputs=["feature_table", "parameters"],
                outputs="scoring_dataset",
            ),
            Node(
                apply_preprocessor,
                inputs=["scoring_dataset", "preprocessor", "parameters"],
                outputs="X_score",
            ),
            Node(
                predict_scores,
                inputs=["model", "X_score", "scoring_dataset", "parameters"],
                outputs="score_table",
            ),
            Node(
                rank_predictions,
                inputs=["score_table", "parameters"],
                outputs="ranked_staging",
            ),
            Node(
                validate_predictions,
                inputs=["ranked_staging", "scoring_dataset", "parameters"],
                outputs="validated_predictions",
            ),
            Node(
                publish_predictions,
                inputs=["validated_predictions", "parameters"],
                outputs="ranked_predictions",
            ),
        ]
    )
```

- [ ] **Step 4: 跑測試,確認 GREEN(含 validate 防退步護欄)**

Run(structure):
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_pipelines/test_inference/test_pipeline.py -q
```
Expected: PASS(6 passed)。

Run(validate node 行為沒被改壞 —— Spark 測試,小資料):
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_pipelines/test_inference/test_validation.py -q
```
Expected: PASS(validate signature/body 未動,positional 呼叫仍有效)。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
git add tests/test_pipelines/test_inference/test_pipeline.py \
        src/recsys_tfb/pipelines/inference/pipeline.py && \
git commit -m "feat(inference): wire staging->validate->publish gate

rank now writes ranked_staging; validate reads staging and raises before
publish on failure; publish promotes to production ranked_predictions only on
success. No more self-overwrite or publish-before-validate.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: catalog.yaml(新增 staging、移除 validated_predictions)

**Files:**
- Create: `tests/test_core/test_catalog_inference_entries.py`
- Modify: `conf/base/catalog.yaml`

- [ ] **Step 1: 寫 failing test**

Create `tests/test_core/test_catalog_inference_entries.py`:

```python
"""Catalog regression: inference output entries reflect the staging gate.

yaml.safe_load on catalog.yaml is an established pattern in this repo (the
${...} placeholders are plain string scalars). Pure-Python, no Spark.
"""

from pathlib import Path

import yaml


def _load_catalog():
    # tests/test_core/<this file> -> parents[2] == repo (worktree) root
    root = Path(__file__).resolve().parents[2]
    catalog_path = root / "conf" / "base" / "catalog.yaml"
    return yaml.safe_load(catalog_path.read_text())


def test_ranked_staging_entry_present():
    d = _load_catalog()
    assert "ranked_staging" in d
    assert d["ranked_staging"]["type"] == "HiveTableDataset"
    assert d["ranked_staging"]["table"] == "ranked_staging"


def test_validated_predictions_entry_removed():
    """validate 的 output 現在是 in-DAG MemoryDataset,不再是 Hive 表。"""
    d = _load_catalog()
    assert "validated_predictions" not in d


def test_ranked_predictions_still_declared():
    """production 輸出 + standalone evaluation 讀取入口必須保留宣告。"""
    d = _load_catalog()
    assert "ranked_predictions" in d
    assert d["ranked_predictions"]["table"] == "ranked_predictions"
```

- [ ] **Step 2: 跑測試,確認 RED**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_core/test_catalog_inference_entries.py -q
```
Expected: FAIL — `test_ranked_staging_entry_present`(`assert 'ranked_staging' in d` → 還沒有)、`test_validated_predictions_entry_removed`(`validated_predictions` 仍在 → 失敗)。`test_ranked_predictions_still_declared` 此時已 PASS。

- [ ] **Step 3: 改 catalog.yaml**

在 `conf/base/catalog.yaml` 找到 `validated_predictions:` 區塊與其後「Evaluation 讀取端入口」註解(在 `score_table` 與 `ranked_predictions` 之間),用 Edit 取代。

old_string:
```yaml
validated_predictions:
  type: HiveTableDataset
  database: ${hive.db}
  table: ranked_predictions
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: rank, type: BIGINT}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
    - {name: model_version, type: STRING}

# --- Inference pipeline（續）— Evaluation 讀取端入口 ---
# 與 validated_predictions 對應到同一張 Hive table；獨立宣告一份，是為了
# evaluation pipeline 可以在同 session 沒跑 inference 的情況下 standalone 執行。
# core/catalog.py:71 對未知名稱會自動 fallback 成 MemoryDataset，會讓
# standalone evaluation 讀取「靜默地壞掉」── 此 entry 就是為了避免這狀況。
```

new_string:
```yaml
# --- Inference pipeline — 發布前 staging（managed Hive table）---
# rank_predictions 先把排名寫進 staging；validate_predictions 讀此表跑 sanity
# checks（raise 即整批中止）。驗證通過後才由 publish_predictions 寫入 production
# 的 ranked_predictions。驗證失敗時 production 完全不被觸碰，本表保留失敗批次供排查。
ranked_staging:
  type: HiveTableDataset
  database: ${hive.db}
  table: ranked_staging
  external: false
  columns:
    - {name: cust_id, type: STRING}
    - {name: score, type: DOUBLE}
    - {name: rank, type: BIGINT}
  partition_cols:
    - {name: snap_date, type: STRING}
    - {name: prod_name, type: STRING}
    - {name: model_version, type: STRING}

# --- Inference pipeline（續）— production 輸出 + Evaluation 讀取端入口 ---
# 由 publish_predictions 寫入（唯一一次 production 寫入，且只在 validate 通過後執行）。
# evaluation pipeline 也讀這張表；獨立宣告確保它在同 session 沒跑 inference 時也能
# standalone 讀，避免 core/catalog.py:71 對未知名稱 fallback 成 MemoryDataset 而
# 「靜默地壞掉」。
```

(`ranked_predictions:` 區塊本身不動。)

- [ ] **Step 4: 跑測試,確認 GREEN**

Run(同 Step 2 命令)。Expected: PASS(3 passed)。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
git add conf/base/catalog.yaml tests/test_core/test_catalog_inference_entries.py && \
git commit -m "feat(catalog): add ranked_staging, drop validated_predictions alias

ranked_staging is the pre-validation managed Hive table; validated_predictions
becomes an in-DAG MemoryDataset (validate->publish edge). ranked_predictions
stays declared as the production output + standalone evaluation read entry.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: 文件(markdown:README / design-principles / evaluation.md)

純文件,無 failing test(TDD 例外)。逐檔 Edit。

**Files:** Modify `README.md`、`docs/design-principles.md`、`docs/pipelines/evaluation.md`

- [ ] **Step 1: README.md — inference node 表**

Edit `README.md`。old_string:
```
| `predict_scores` | `model`、`X_score` | 模型評分 | `score_table` |
| `rank_predictions` | `score_table` | 每個 query group 內依 score 排名 | `ranked_predictions` |
```
new_string:
```
| `predict_scores` | `model`、`X_score` | 模型評分 | `score_table` |
| `rank_predictions` | `score_table` | 每個 query group 內依 score 排名 | `ranked_staging` |
| `validate_predictions` | `ranked_staging`、`scoring_dataset` | 6 項 sanity check（筆數/分數範圍/完整性/排名一致…），失敗即中止整批 | `validated_predictions`（中間態） |
| `publish_predictions` | `validated_predictions` | 驗證通過後才把結果發布到 production 表（唯一一次 production 寫入） | `ranked_predictions` |
```

- [ ] **Step 2: README.md — inference 輸出敘述**

Edit `README.md`。old_string:
```
**輸出** —— 一張 Hive 表，示例名為 `ranked_predictions`。每個 query group 內，`item` 依 `score` 由高到低排出 `rank`：
```
new_string:
```
**輸出** —— 一張 Hive 表，示例名為 `ranked_predictions`（rank 後須先通過 `validate_predictions` 的 sanity check，才由 `publish_predictions` 發布）。每個 query group 內，`item` 依 `score` 由高到低排出 `rank`：
```

- [ ] **Step 3: README.md — inference catalog 清單**

Edit `README.md`。old_string:
```
| `inference` | <ul><li><code>conf/base/parameters_inference.yaml</code>：<code>snap_dates</code></li><li><code>catalog.yaml</code>：<code>score_table</code>、<code>ranked_predictions</code></li></ul> |
```
new_string:
```
| `inference` | <ul><li><code>conf/base/parameters_inference.yaml</code>：<code>snap_dates</code></li><li><code>catalog.yaml</code>：<code>score_table</code>、<code>ranked_staging</code>、<code>ranked_predictions</code></li></ul> |
```

- [ ] **Step 4: docs/design-principles.md — §9 兩個 bullet 改寫 + 補 gate 原則**

Edit `docs/design-principles.md`。old_string:
```
- 未註冊的中間產物 `save` 時**自動建 `MemoryDataset`**（只活在單次 run）。
- ⚠️ footgun：若某個本該持久化的表沒在 catalog 註冊，會悄悄 fallback 成 MemoryDataset → standalone 讀取靜默壞掉。所以 `ranked_predictions` 在 catalog 額外宣告了讀取端 entry（見 [`data-lineage.html`](data-lineage.html)）。
```
new_string:
```
- 未註冊的中間產物 `save` 時**自動建 `MemoryDataset`**（只活在單次 run）—— inference 的 `validated_predictions`（validate 與 publish 之間的閘門中間態）就是這樣的產物。
- ⚠️ footgun：若某個本該持久化的表沒在 catalog 註冊，會悄悄 fallback 成 MemoryDataset → standalone 讀取靜默壞掉。所以 production 的 `ranked_predictions`（由 `publish_predictions` 寫入、evaluation 也讀它）一定要顯式宣告（見 [`data-lineage.html`](data-lineage.html)）。
- inference 的驗證採 **staging→validate→publish 閘門**：`rank_predictions` 先寫 `ranked_staging`，`validate_predictions` 讀它跑 sanity check 失敗即 raise，**通過後才由 `publish_predictions` 寫 production `ranked_predictions`**。重點是閘門 gate 的是「發布」而非只是「中止 run」—— 驗證失敗時 production 完全不被寫入，壞批次留在 `ranked_staging` 供排查。
```

- [ ] **Step 5: docs/pipelines/evaluation.md — 監控來源註記**

Edit `docs/pipelines/evaluation.md`。old_string:
```
| 上線後監控 | `evaluation`（預設） | `ranked_predictions`（inference 產） | 模型上線後定期追蹤排名品質 |
```
new_string:
```
| 上線後監控 | `evaluation`（預設） | `ranked_predictions`（inference 發布的已驗證結果） | 模型上線後定期追蹤排名品質 |
```

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
git add README.md docs/design-principles.md docs/pipelines/evaluation.md && \
git commit -m "docs: reflect inference staging->validate->publish gate (md)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: 文件(diagram:data-lineage.html + pipeline-overview.svg)

**Files:** Modify `docs/data-lineage.html`;Assess `docs/diagrams/pipeline-overview.svg`

- [ ] **Step 1: 先 Read `docs/data-lineage.html` 確認下列字串仍存在(行號可能位移,字串唯一)**

Read 檔案,grep 確認 5 段待改字串都在(以下 Edit 以唯一字串比對,不依賴行號)。

- [ ] **Step 2: data-lineage.html — inference lineage 行(原 L197)**

Edit。old_string:
```
score_table → rank_predictions → ranked_predictions（= validated_predictions 同一張表）
```
new_string:
```
score_table → rank_predictions → ranked_staging → validate_predictions → publish_predictions → ranked_predictions
```

- [ ] **Step 3: data-lineage.html — 導覽連結(原 L252)**

Edit。old_string:
```
  <a href="#ranked_predictions">ranked_predictions / validated_predictions</a>
```
new_string:
```
  <a href="#ranked_predictions">ranked_staging / ranked_predictions</a>
```

- [ ] **Step 4: data-lineage.html — 卡片標題副標(原 L518)**

Edit。old_string:
```
    <span style="font-size:11px;color:#59636e">（= validated_predictions，同一張 Hive 表）</span></h4>
```
new_string:
```
    <span style="font-size:11px;color:#59636e">（rank 後的 production 表；發布前 staging = ranked_staging）</span></h4>
```

- [ ] **Step 5: data-lineage.html — 卡片「產生」行(原 L519)**

Edit。old_string:
```
  <div class="meta"><b>產生</b>：inference · rank_predictions（每個 query group 內依 score 排名）→ validate_predictions　|
```
new_string:
```
  <div class="meta"><b>產生</b>：inference · rank_predictions → ranked_staging → validate_predictions（sanity checks）→ publish_predictions　|
```

- [ ] **Step 6: data-lineage.html — 卡片 note(原 L522)**

Edit。old_string:
```
  <p class="note">catalog 有兩個 entry 指向同一張表 <code>ranked_predictions</code>：<code>validated_predictions</code>（inference 寫入端，validate_predictions 的輸出）與 <code>ranked_predictions</code>（evaluation 讀取端，讓 evaluation 不跑 inference 也能 standalone 讀，避免 MemoryDataset fallback 靜默壞掉）。</p>
```
new_string:
```
  <p class="note">production 由 <code>publish_predictions</code> 寫入，且只在 <code>validate_predictions</code> 通過後才執行：<code>rank_predictions</code> 先寫 <code>ranked_staging</code>，validate 讀它跑 sanity check，失敗即中止、production 不被寫（失敗批次留在 staging 供排查）。<code>ranked_predictions</code> 顯式宣告讓 evaluation 不跑 inference 也能 standalone 讀，避免 MemoryDataset fallback 靜默壞掉。</p>
```

- [ ] **Step 7: pipeline-overview.svg — 評估是否需改**

Read `docs/diagrams/pipeline-overview.svg`(搜 `ranked_predictions` / `rank_predictions` / `validate`)。判斷:
- 若 inference 區塊**只**有輸出 label `ranked_predictions` —— 不動(production 輸出名稱不變,仍正確)。
- 若有畫出 `validate_predictions` 之類的 node box —— 把該 node 的文字改成反映 `ranked_staging → validate → publish`(在不破壞 SVG 座標/版面下,最小幅度替換 `<text>` 內容)。

記錄判斷結果於 commit message。

- [ ] **Step 8: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
git add docs/data-lineage.html docs/diagrams/pipeline-overview.svg && \
git commit -m "docs: reflect inference staging gate in data-lineage diagram

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```
(若 pipeline-overview.svg 判定不需改,`git add` 只加 `docs/data-lineage.html`。)

---

## Task 6: 收尾驗證

**Files:** 無(只跑檢查)

- [ ] **Step 1: 確認沒有殘留把 `validated_predictions` 當 Hive/catalog 名稱的地方**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
grep -rn "validated_predictions" src/ tests/ conf/
```
Expected: 只剩 `src/recsys_tfb/pipelines/inference/pipeline.py`(validate 的 output 字串)與 `tests/test_pipelines/test_inference/test_pipeline.py`(outputs set + chain 斷言)。**不應**再有 catalog/Hive 宣告或其他測試對它的斷言。若出現別處引用(如 evaluation 測試),逐一檢視是否需同步。

- [ ] **Step 2: 跑本次相關測試一次,全綠**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_pipelines/test_inference/ \
  /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate/tests/test_core/test_catalog_inference_entries.py -q
```
Expected: 全部 PASS。

- [ ] **Step 3: 確認 graph 已隨 commit hook 更新**

每次 commit 的 graphify post-commit hook 已自動重建 `graphify-out/`(log 會印 `Rebuilt: N nodes`)。不需手動 rebuild。若要保險,確認最後一次 commit 的 hook log 有出現重建訊息即可。

- [ ] **Step 4: 給使用者看 diff,等待 push 指示**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/inference-staging-gate && \
git log --oneline main..HEAD && echo "---" && git diff --stat main..HEAD
```
push / 合併由使用者人工觸發。

---

## Self-Review(已執行)

- **Spec coverage**:資料流(T2)、catalog 三項(T3)、publish 節點(T1)、gate 語義(由 T2 拓樸順序測試 + catalog 保證)、evaluation 不改(T6 grep 驗證無誤傷)、docs scope 全部對應(T4/T5)、排除歷史 superpowers/plans+specs(未列入 Files)。✓
- **Placeholder scan**:無 TBD/TODO;唯一條件式是 T5 Step7 SVG「讀後判斷」,已給兩種明確結果與動作,非佔位。✓
- **Type/名稱一致**:`ranked_staging` / `validated_predictions` / `ranked_predictions` / `publish_predictions` 全文一致;node `.name` / `.inputs` / `.outputs` 與 `core/pipeline.py`、既有 `test_pipeline.py` 用法一致;`publish_predictions(validated_predictions, parameters)` 簽章在 T1 定義、T2 接線一致。✓
