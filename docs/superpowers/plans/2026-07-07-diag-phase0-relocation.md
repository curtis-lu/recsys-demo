# Phase 0：診斷域 Kedro 式歸位 — 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把訓練側診斷邏輯與跨 pipeline 共用件平移到新的 `src/recsys_tfb/diagnosis/` 領域 library，行為完全不變，以「測試 baseline 一致＋診斷產物逐檔 diff 一致＋diff --stat 稽核」三重閘門收尾。

**Architecture:** 純結構平移（git mv ＋ import 路徑改寫），零邏輯變更。`pipelines/training/diagnostics/` 整包 → `diagnosis/model/`；`pipelines/training/diagnostics_spark.py` → `diagnosis/model/population_spark.py`；`pipelines/dataset/_hashing.py` → `utils/hashing.py`；`diagnosis/metric/` 建空殼（Phase 1–5 之家）。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §3 Phase 0。

**Tech Stack:** git mv、BSD sed（macOS）、pytest、本機 local Spark（`--env local`）。

**Scope note（給規劃讀者）：** spec 共六階段；本計畫只涵蓋 Phase 0。Phase 1–5 各自在前一個使用者閘門通過後另寫計畫——現在寫會在 Phase 1 落地後全部過時。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && ...` 開頭（`cd` 在指令間會殘留，別依賴它）。Edit/Write 的絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`。裸跑會抓到 main 的 src，靜默測錯 code。
3. **可能超過 2 分鐘的指令（訓練、整包 Spark 測試）一律背景執行**，完成再讀輸出。
4. **測試判準是「與 baseline 一致」不是「絕對全綠」**：main 有既知 failing／組合互擾測試（`docs/operations/known-pitfalls.md` §5）。Task 1 會記下 baseline，之後每次比對它。
5. 本計畫**禁止任何邏輯行變更**。做到一半發現「不改邏輯過不了」→ 停下回報，不要硬修。

---

### Task 1：pre-flight ＋ 平移前基準（產物快照與測試 baseline）

**Files:** 無程式碼變更；產出 `/tmp/phase0_diag_before/`、`/tmp/phase0_test_baseline.txt`、`/tmp/phase0_mv.txt`。

- [ ] **Step 1: worktree pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
```
Expected: pwd 印出 worktree 路徑；readlink 印出 `/Users/curtislu/projects/recsys_tfb/.venv`；Python 3.10.9。

- [ ] **Step 2: 本機 Spark 環境（首次建立合成資料與 warehouse）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```
Expected: setup 完成；`--check-isolation` 全過（任一失敗先修再繼續，見 `docs/operations/local-spark-setup.md`）。

- [ ] **Step 3: 跑一次 dataset ＋ training 產生平移前產物**（>2 分鐘，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
```
Expected: 兩條 pipeline 成功結束；`data/models/` 下出現新的 model_version 目錄（含 `model.txt` 與 `diagnostics/`）。

- [ ] **Step 4: 記下 model_version、快照診斷產物**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
MV=$(ls -t data/models | grep -v '^_hpo$' | grep -v '^best$' | head -1) && echo "$MV" | tee /tmp/phase0_mv.txt && \
cp -r "data/models/$MV/diagnostics" /tmp/phase0_diag_before && ls /tmp/phase0_diag_before
```
Expected: 印出 model_version；`/tmp/phase0_diag_before` 含 `feature_statistics.json`、`feature_importance.json`、`shap_diagnostics.json`、`per_quadrant.json`、`summary/`、`cases/`（含 `cases_manifest.json`）。

- [ ] **Step 5: 測試 baseline**（Spark 測試在內，>2 分鐘，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training tests/test_pipelines/test_dataset -q 2>&1 | tail -30 | tee /tmp/phase0_test_baseline.txt
```
Expected: 輸出最後 30 行存檔（pass/fail 統計行在內）。若有 fail，照 known-pitfalls §5 判斷是否既知；記進 baseline 即可，**不要修**。

---

### Task 2：建 `diagnosis` 套件骨架

**Files:**
- Create: `src/recsys_tfb/diagnosis/__init__.py`
- Create: `src/recsys_tfb/diagnosis/metric/__init__.py`

- [ ] **Step 1: 寫兩個 `__init__.py`**

`src/recsys_tfb/diagnosis/__init__.py`：

```python
"""診斷域 library：排序模型的「現象 → 成因 → 槓桿」診斷。

- ``diagnosis.model``  — 模型結構層診斷（SHAP、importance、feature stats、
  象限選樣與案例；Phase 5 的 gain ledger）。訓練 pipeline 的薄 node 呼叫它。
- ``diagnosis.metric`` — 指標層診斷（Phase 1–5 陸續進駐：抽樣、CI、對帳、
  判別力、象限、offset sweep、成對帳本、triage）。評估 pipeline 的薄 node 呼叫它。

依賴方向（單向，違反即錯，見 spec §1 不變量 4）：
``pipelines/* → diagnosis → evaluation(僅 numpy 原語 metrics.py) / io / utils``；
本套件不得 import 任何 ``pipelines/*``。
框架方法論見 docs/ranking-diagnosis-framework.md。
"""
```

`src/recsys_tfb/diagnosis/metric/__init__.py`：

```python
"""指標層診斷（Phase 1–5 進駐；目前為空殼，見 spec §3）。"""
```

- [ ] **Step 2: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis && git commit -m "feat(diagnosis): 建診斷域套件骨架（model/ 由下個 commit 平移進駐）"
```

---

### Task 3：平移訓練側診斷（套件 ＋ population_spark）與全部 import 改寫

**Files:**
- Move: `src/recsys_tfb/pipelines/training/diagnostics/`（10 檔）→ `src/recsys_tfb/diagnosis/model/`
- Move+Rename: `src/recsys_tfb/pipelines/training/diagnostics_spark.py` → `src/recsys_tfb/diagnosis/model/population_spark.py`
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py:5-12`、`src/recsys_tfb/pipelines/training/nodes.py:86,912`
- Move: `tests/test_pipelines/test_training/` 的 6 個診斷測試檔 → `tests/test_diagnosis/test_model/`
- Modify: `tests/test_pipelines/test_training/test_nodes.py`（monkeypatch 字串 ×2 ＋ import ×1）

- [ ] **Step 1: git mv 套件與檔案**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git mv src/recsys_tfb/pipelines/training/diagnostics src/recsys_tfb/diagnosis/model && \
git mv src/recsys_tfb/pipelines/training/diagnostics_spark.py src/recsys_tfb/diagnosis/model/population_spark.py && \
ls src/recsys_tfb/diagnosis/model/
```
Expected: `model/` 下有 `__init__.py, _util.py, attribution.py, data_access.py, feature_stats.py, importance.py, paths.py, population_spark.py, sampling.py, shap_cases.py, shap_per_item.py`。（套件內部全是 relative import，隨包搬移不需改。）

- [ ] **Step 2: 確認 population_spark.py 沒有 reach-back import**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && grep -n "^from \|^import \|    from \|    import " src/recsys_tfb/diagnosis/model/population_spark.py | grep "recsys_tfb"
```
Expected: 命中裡**沒有** `recsys_tfb.pipelines.` 開頭的 import（core/utils/evaluation 都可）。若有 → 停下回報，這代表 spec 的平移前提不成立。

- [ ] **Step 3: 一次性 sed 改寫 src 與 tests 的 import 路徑**（`_spark` pattern 必須先替換，否則會被套件 pattern 切壞）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
grep -rl "recsys_tfb\.pipelines\.training\.diagnostics" src tests | xargs sed -i '' \
  -e 's/recsys_tfb\.pipelines\.training\.diagnostics_spark/recsys_tfb.diagnosis.model.population_spark/g' \
  -e 's/recsys_tfb\.pipelines\.training\.diagnostics/recsys_tfb.diagnosis.model/g' && \
grep -rn "pipelines\.training\.diagnostics" src tests
```
Expected: 最後一個 grep **零命中**。這一步同時處理了 `pipeline.py:5-12`、`nodes.py:86,912` 的 import 與 `test_nodes.py:1117,1145` 的 monkeypatch 字串。

- [ ] **Step 4: 測試檔鏡像平移**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
mkdir -p tests/test_diagnosis/test_model && \
printf '' > tests/test_diagnosis/__init__.py && printf '' > tests/test_diagnosis/test_model/__init__.py && \
git mv tests/test_pipelines/test_training/test_attribution.py tests/test_diagnosis/test_model/ && \
git mv tests/test_pipelines/test_training/test_diagnostics.py tests/test_diagnosis/test_model/ && \
git mv tests/test_pipelines/test_training/test_diagnostics_data_access.py tests/test_diagnosis/test_model/ && \
git mv tests/test_pipelines/test_training/test_diagnostics_sampling.py tests/test_diagnosis/test_model/ && \
git mv tests/test_pipelines/test_training/test_shap_cases.py tests/test_diagnosis/test_model/ && \
git mv tests/test_pipelines/test_training/test_diagnostics_spark.py tests/test_diagnosis/test_model/test_population_spark.py && \
git add tests/test_diagnosis && ls tests/test_diagnosis/test_model/
```
Expected: 6 個測試檔＋`__init__.py` 就位；`test_pipelines/test_training/` 剩下的檔案不含 `test_diagnostics*`、`test_attribution`、`test_shap_cases`。

- [ ] **Step 5: 跑受影響測試**（Spark 測試在內，>2 分鐘，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_training -q 2>&1 | tail -30
```
Expected: 結果與 `/tmp/phase0_test_baseline.txt` 一致（同樣的 pass 數；既知 fail 同集合）。任何**新** fail → 先查 import 殘漏（回 Step 3 的 grep），不是改邏輯。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && git add -A && \
git commit -m "refactor(diagnosis): 平移訓練側診斷至 diagnosis/model（行為不變，僅搬移+import 改寫）"
```

---

### Task 4：平移 hashing 工具到 utils

**Files:**
- Move+Rename: `src/recsys_tfb/pipelines/dataset/_hashing.py` → `src/recsys_tfb/utils/hashing.py`
- Modify: `src/recsys_tfb/pipelines/dataset/helpers_spark.py:10`、`src/recsys_tfb/pipelines/dataset/nodes_spark.py:11`
- Move: `tests/test_pipelines/test_dataset/test_hashing.py` → `tests/test_utils/test_hashing.py`

- [ ] **Step 1: git mv ＋ sed 改寫三個 importer**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git mv src/recsys_tfb/pipelines/dataset/_hashing.py src/recsys_tfb/utils/hashing.py && \
git mv tests/test_pipelines/test_dataset/test_hashing.py tests/test_utils/test_hashing.py && \
grep -rl "recsys_tfb\.pipelines\.dataset\._hashing" src tests | xargs sed -i '' \
  's/recsys_tfb\.pipelines\.dataset\._hashing/recsys_tfb.utils.hashing/g' && \
grep -rn "pipelines\.dataset\._hashing" src tests
```
Expected: 最後的 grep **零命中**。

- [ ] **Step 2: 跑受影響測試**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_hashing.py tests/test_pipelines/test_dataset -q 2>&1 | tail -20
```
Expected: 與 baseline 對應部分一致。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && git add -A && \
git commit -m "refactor(utils): _hashing 平移為 utils/hashing（dataset 私有件升級為共用件，diagnosis 後續取用）"
```

---

### Task 5：斷鏈總檢 ＋ graphify ＋ 全套受影響測試對照 baseline

**Files:** 無新變更；驗證性任務。

- [ ] **Step 1: 全 repo 舊路徑零命中稽核**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
grep -rn "pipelines\.training\.diagnostics\|pipelines/training/diagnostics\|pipelines\.dataset\._hashing" src tests conf scripts 2>/dev/null; echo "exit=$?"
```
Expected: 只印 `exit=1`（零命中）。（docs/ 的敘述性引用——含 spec 與本計畫自身——不在稽核範圍，它們記錄的是平移前的事實。）

- [ ] **Step 2: graphify 重建（repo 規範：改 code 後必跑）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: rebuild 完成訊息。

- [ ] **Step 3: 受影響測試全套重跑對照 baseline**（>2 分鐘，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_training tests/test_pipelines/test_dataset tests/test_utils -q 2>&1 | tail -30
```
Expected: pass/fail 集合與 `/tmp/phase0_test_baseline.txt` 一致（test_utils 多出的 hashing 測試除外——它從 dataset 搬來，數量守恆）。

---

### Task 6：真實執行閘門（使用者驗收物）

**Files:** 無程式碼變更；產出閘門證據給使用者。

- [ ] **Step 1: 切片重跑診斷節點**（只重跑診斷、吃同一個已訓 model artifact——避免整條重訓的位元重現變因，理由見 spec §3 Phase 0 驗收 2）（>2 分鐘，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local --from-node compute_feature_statistics
```
Expected: 成功結束；使用的 model_version 與 `/tmp/phase0_mv.txt` 相同（日誌可見）。

- [ ] **Step 2: 診斷 JSON 逐檔 diff**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && MV=$(cat /tmp/phase0_mv.txt) && \
cd "data/models/$MV/diagnostics" && find . -name "*.json" | sort | while read f; do diff "$f" "/tmp/phase0_diag_before/$f" > /dev/null || echo "DIFF: $f"; done; echo "diff-scan done"
```
Expected: 只印 `diff-scan done`，沒有任何 `DIFF:` 行。

- [ ] **Step 3: diff --stat 稽核（對 spec 最後一個 commit 之後的變更）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && git diff --stat 5b341d7..HEAD && git log --oneline 5b341d7..HEAD
```
Expected: 變更清單只有 (a) 兩個新 `__init__.py`、(b) rename 條目、(c) import 行改寫的小幅 +/-；沒有任何其他邏輯檔案。

- [ ] **Step 4: 整理閘門報告給使用者**

彙整三項證據（測試 baseline 對照輸出、JSON diff 掃描輸出、diff --stat 全文）成一段摘要，交使用者檢視。**使用者確認通過後**，才開 Phase 1 的計畫。

---

## Self-review 紀錄（計畫作者自查）

- spec Phase 0 覆蓋：套件平移（Task 3）、population_spark 改名（Task 3）、hashing 平移含 dataset 測試檔（Task 4）、metric/ 空殼（Task 2）、三重閘門（Task 1 baseline ＋ Task 5/6）——逐項對上。
- 零佔位符：每步有完整指令與預期輸出；sed pattern 的先後順序陷阱（`_spark` 先換）已顯式寫出。
- 型別/識別字一致性：import 目標 `recsys_tfb.diagnosis.model` 與 Task 2 的套件骨架一致；monkeypatch 字串靠同一個 sed pattern 一併改寫（已驗 pattern 覆蓋 `test_nodes.py:1117,1145` 的字串形式）。
