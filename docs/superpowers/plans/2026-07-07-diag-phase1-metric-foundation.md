# Phase 1：指標基座（參數化指標家族＋per-item CI）— 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把主指標（macro per-item mAP）參數化成家族（`weight_alpha` / `min_positives` / `shrinkage_k`，預設值全部等價現行為），並用 cust_id-cluster bootstrap 給 per-item AP 與 macro 補信賴區間；報表增列 CI 欄與觀察名單。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §2＋§3 Phase 1。

**Architecture:** 指標參數化留在 `evaluation/`（metrics 是評估本體）：numpy 原語 `metrics.py` 加共用組合器 `macro_from_per_item` 與貢獻原語 `positive_row_contributions`；Spark 側 `aggregate_per_item` 增列 `n_pos`（前置缺口，additive）、`macro_average` 接參數。CI 與診斷抽樣進 `diagnosis/metric/`（`sample.py` 兩趟抽樣、`uncertainty.py` cluster bootstrap）。評估 pipeline 加一個薄 node `compute_metric_ci`，產物走 catalog（`diagnosis/metric_ci.json`）。新 config 附 consistency A15。

**Tech Stack:** numpy（driver 端 bootstrap）、PySpark 3.3.2（無 UDF）、pytest、本機 local Spark（`--env local`）。

**Scope note：** 本計畫只涵蓋 Phase 1。Phase 2–5 在本階段使用者閘門通過後另寫。HPO objective 不動（`training/nodes.py::_hpo_score` 呼叫 `compute_macro_per_item_map(groups, items, y_true, y_score)`，新參數全部有預設值，該處零改動）。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && ...` 開頭；Edit/Write 絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`。裸跑會抓到 main 的 src，靜默測錯 code。
3. **可能超過 2 分鐘的指令（真跑 pipeline、整包 Spark 測試）一律背景執行**，完成再讀輸出。單檔測試（本計畫各 task 的 TDD 迴圈）幾十秒內，前景跑即可。
4. **測試判準是「與 baseline 一致」不是「絕對全綠」**（`docs/operations/known-pitfalls.md` §5）。Task 1 記 baseline，之後比對它。
5. **生產不變量**：no Spark UDF、no new packages；bootstrap 等逐列計算一律 driver 端 numpy、跑在有上限的抽樣上。
6. **不得動 `compute_model_version` 的任何輸入**：本階段所有新 config 都在 `evaluation.*`（本來就不進 model_version）。訓練不重跑，沿用既有 `data/models/` 下的已訓模型。
7. 依賴方向：`pipelines/* → diagnosis → core / evaluation(僅 numpy 原語 metrics.py) / io / utils`。`diagnosis/metric/` 可以 import `evaluation/metrics.py` 與 `utils/hashing.py`，**不得** import `evaluation/metrics_spark.py` 或任何 `pipelines/*`。

## 設計定案（所有 task 共用的語意，實作時不要再各自發明）

以下決策在 spec 未逐字釘死處做了收斂，實作照此執行；發現做不到再停下回報：

- **`n_pos` 是保留鍵**：`aggregate_per_item` 的每個 per-item dict 增列 `n_pos`（該 item 的正例列數，int）。`macro_average` 把 `n_pos` 當權重來源，**不**把它平均進輸出（輸出鍵集合與現行完全相同）。
- **參數作用順序**（numpy 與 Spark 兩側同一套，共用 `macro_from_per_item`）：
  1. `min_positives`：`n_pos < min_positives` 的 item 移出平均（另列觀察名單）；
  2. `shrinkage_k`：對留下的每個 item，值向「留下 item 的 pooled（n_pos 加權）平均」收縮：`v' = (n·v + k·v̄_pooled) / (n + k)`；
  3. `weight_alpha`：權重 `w_j ∝ n_j^α` 歸一化後加權平均（α=0＝等權＝現行）。
- **收縮目標**＝pooled row-weighted mean（Σ n_j·v_j / Σ n_j，只算留下的 item）。對 map_attr 這等於「所有正例列貢獻的平均」，是自然的 empirical-Bayes prior。可驗性質：`k=0` 不變；`k→∞` 全部收到 pooled 平均。
- **fail loud，不靜默退回等權**：`macro_average` 收到非預設參數、但任何 inner dict 缺 `n_pos` → raise `ValueError`。預設參數（0/0/0）時行為與現行逐位元相同（含 inner dict 沒有 `n_pos` 的舊型輸入）。
- **參數只作用於 item 粒度**：`_compute_core` 把 `evaluation.metric` 參數傳給 `by_item` 與 `by_item_segment` 的 `macro_average`；`by_segment`（per-query 粒度、無 n_pos 概念）維持等權，不接參數。
- **`evaluation.metric.k`**＝診斷側（CI bootstrap）計算 AP 家族時的截斷 k；`null`＝不截斷（full mAP）。報表主體仍照 `k_values` 全列，此鍵不影響 Spark 報表指標。
- **`observation_items`**：`_compute_core` 回傳 dict 新增 additive 頂層鍵（`min_positives=0` 時＝`[]`），值＝`n_pos < min_positives` 的 item 名排序清單。category 粒度的子 dict 也會有自己的一份（`_compute_core` 被 category pass 重用，自動帶到）。
- **CI 是抽樣估計**：metric_ci.json 與報表都必須標示樣本規模（正例 query 數）與 n_boot，不得讓抽樣估計冒充全量。
- **cluster bootstrap 的 cluster＝entity（cust_id）**，不含 time——同一客戶跨期整批重抽（spec §3 Phase 1）。關鍵簡化：整 cluster 重抽不改變任何 query 內的排序，所以每列正例貢獻只算一次，replicate 只是帶乘數的重新聚合。
- **抽樣單位＝query（time × entity），只取有正例的 query**；被抽中的 query 取其**全部候選列**（含負例列，排序需要）。保底機制照 spec：正例 query 數 < 保底的 item 整批全取；其餘 query 用 CRC32 hash-ratio 補滿 `max_queries`。中型 item 抽後仍可能低於保底——不硬補，metadata 誠實回報＋log WARN。
- **既有測試會被本計畫「合法」改到的只有四處**：(1) `tests/test_pipelines/test_evaluation/test_pipeline.py` 鎖 pipeline 結構的斷言——default/post_training 五個 node → 六個、compare-source 模式八個 → 九個（同一份基礎 nodes 清單的必然連帶；Task 8，後者為執行時裁決追認）；(2) `tests/test_evaluation/test_metrics_spark.py::test_aggregate_per_item_emits_attribution_keys_not_precision_recall` 的精確鍵集合斷言——其意圖是「不得有 precision/recall 鍵」，`n_pos` 為 spec 要求的 additive 鍵，加入預期集合（Task 2，執行時裁決）；(3) 同檔 `test_compute_all_metrics_returns_expected_keys` 的頂層鍵 exact-set 斷言——`observation_items` 為 additive 鍵，加入預期集合（Task 4，執行時裁決追認）。`tests/.../TestComputeMacroPerItemMap` 與 `test_macro_average_*` 既有案例**不得改**，必須原樣全綠。

## 執行模式（controller 注意）

使用者明示 subagent 注意 token 成本：機械步驟（跑指令、baseline、真跑閘門）controller 直跑不派 agent；Task 2–9 派 sonnet implementer（每個 prompt 附本計畫對應 task 全文＋「設計定案」節）；spec/quality 審查合併為單一 sonnet reviewer per task；opus 只在 Task 10 收尾做一次總審。

---

### Task 1：pre-flight ＋ baseline（測試子集＋改動前評估報表快照）

**Files:** 無程式碼變更；產出 `/tmp/phase1_test_baseline.txt`、`/tmp/phase1_report_before.html`、`/tmp/phase1_mv.txt`。

- [ ] **Step 1: worktree pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```
Expected: pwd＝worktree 路徑；readlink＝`/Users/curtislu/projects/recsys_tfb/.venv`；Python 3.10.9；isolation 全過。

- [ ] **Step 2: 確認已訓模型仍在（Phase 0 產物；若 `data/models/` 是空的，先照 Phase 0 計畫 Task 1 Step 3 重跑 dataset＋training）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
MV=$(ls -t data/models | head -1) && echo "$MV" | tee /tmp/phase1_mv.txt && \
ls data/models/$MV/model.txt data/models/$MV/diagnostics/
```
Expected: 印出 model_version（Phase 0 時為 `6059dcef` 一類的短雜湊）；`model.txt` 與 `diagnostics/` 存在。

- [ ] **Step 3: 改動前真跑一次 evaluation，快照報表**（>2 分鐘可能性高，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version $(cat /tmp/phase1_mv.txt) && \
MV=$(cat /tmp/phase1_mv.txt) && SNAP=$(grep -A2 '^evaluation:' conf/base/parameters_evaluation.yaml | grep snap_date | sed 's/.*"\(.*\)".*/\1/' | tr -d '-') && \
cp "data/evaluation/$MV/$SNAP/report.html" /tmp/phase1_report_before.html && echo "SNAP=$SNAP" && ls -la /tmp/phase1_report_before.html
```
Expected: evaluation 成功；`SNAP=20260131`；快照檔存在（這是 Task 10 回歸比對的基準）。

- [ ] **Step 4: 相關測試 baseline**（Spark 測試在內，>2 分鐘，背景執行；**不要跑整包 tests/test_evaluation**，全量約 33 分鐘）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py tests/test_evaluation/test_metrics_spark.py \
  tests/test_evaluation/test_metrics_spark_orchestrator.py tests/test_evaluation/test_metrics_spark_slim.py \
  tests/test_evaluation/test_metrics_spark_category.py tests/test_evaluation/test_report_builder.py \
  tests/test_evaluation/test_parameters_evaluation_yaml.py tests/test_core/test_consistency.py \
  tests/test_pipelines/test_evaluation tests/test_diagnosis \
  -q 2>&1 | tail -15 | tee /tmp/phase1_test_baseline.txt
```
Expected: 最後 15 行存檔（pass/fail 統計）。若有 fail，照 known-pitfalls §5 判斷是否既知，記進 baseline，**不要修**。

---

### Task 2：Spark `aggregate_per_item` 增列 `n_pos`（前置缺口）＋ `macro_average` 排除保留鍵

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`（`aggregate_per_item:447`、`macro_average:508`）
- Test: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: 寫失敗測試**（加在 `test_aggregate_per_item_filters_label_zero_rows` 之後、`# macro_average` 註解區之前）

```python
def test_aggregate_per_item_emits_n_pos(spark):
    """n_pos = 該 item 的正例列數（weight_alpha/min_positives/shrinkage_k 的 P_j 來源）。"""
    enriched = _enriched(spark)
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [3])
    # _two_customer_raw：A 正例 1 列（C0）、B 1 列（C1）、C 1 列（C0）
    assert per_item["A"]["n_pos"] == 1
    assert per_item["B"]["n_pos"] == 1
    assert per_item["C"]["n_pos"] == 1
    assert isinstance(per_item["A"]["n_pos"], int)


def test_macro_average_excludes_n_pos_from_output(spark=None):
    per_dim = {
        "A": {"map_attr@3": 0.75, "n_pos": 2},
        "B": {"map_attr@3": 1.0, "n_pos": 1},
    }
    avg = ms.macro_average(per_dim)
    assert avg == {"map_attr@3": pytest.approx(0.875)}
    assert "n_pos" not in avg
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics_spark.py -q -k "n_pos" 2>&1 | tail -5
```
Expected: 2 failed（KeyError `n_pos`；macro 輸出含 `n_pos`）。

- [ ] **Step 3: 實作**

`aggregate_per_item`：aggs 加一項、collect 迴圈補 int 鍵（`_per_item_metric_cols` **不改**——它描述的是 float metric 欄）：

```python
    aggs = [
        F.mean(F.col("pos").cast("double")).alias("mean_pos"),
        F.count(F.lit(1)).alias("n_pos"),
    ]
```

collect 迴圈尾端（`out[key] = {...}` 之後）：

```python
        out[key] = {c: float(r[c]) for c in metric_cols}
        out[key]["n_pos"] = int(r["n_pos"])
```

`macro_average`：先只做保留鍵排除（參數在 Task 4 接）：

```python
_N_POS_KEY = "n_pos"


def macro_average(per_dim: dict[str, dict[str, float]]) -> dict[str, float]:
    """Equal-dim-key weight mean of inner metric dicts.

    ``n_pos`` is a reserved weight-source key (see aggregate_per_item) — it is
    never averaged into the output. Empty input → empty dict. Missing keys in
    some inner dicts are handled per-metric.
    """
    if not per_dim:
        return {}
    accum: dict[str, list[float]] = {}
    for metrics in per_dim.values():
        for k, v in metrics.items():
            if k == _N_POS_KEY:
                continue
            accum.setdefault(k, []).append(float(v))
    return {k: sum(v) / len(v) for k, v in accum.items()}
```

同時更新 `aggregate_per_item` docstring 的輸出清單（`mean_pos` 之後加一行 `n_pos = count of P-positive rows (weight source for macro_average)`）。

- [ ] **Step 4: 跑測試確認通過（含既有案例不倒）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics_spark.py -q 2>&1 | tail -5
```
Expected: 全綠（既有 `test_macro_average_basic` 等不需改動）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py && \
git commit -m "feat(metrics-spark): aggregate_per_item 增列 n_pos；macro_average 保留鍵排除"
```

---

### Task 3：numpy 指標家族（`macro_from_per_item` ＋ `positive_row_contributions` ＋ `compute_macro_per_item_map` 參數）

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics.py`
- Test: `tests/test_evaluation/test_metrics.py`

- [ ] **Step 1: 寫失敗測試**（檔尾新增；import 區補 `macro_from_per_item`）

```python
class TestMacroFromPerItem:
    # 兩個 item：A 值 0.75、n=2；B 值 1.0、n=1。pooled = (2*0.75+1*1.0)/3 = 5/6
    VALUES = np.array([0.75, 1.0])
    N_POS = np.array([2, 1])

    def test_defaults_equal_plain_mean(self):
        assert macro_from_per_item(self.VALUES, self.N_POS) == pytest.approx(0.875)

    def test_weight_alpha_one_weights_by_n_pos(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, weight_alpha=1.0)
        assert r == pytest.approx(5 / 6)

    def test_min_positives_drops_cold_item(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, min_positives=2)
        assert r == pytest.approx(0.75)

    def test_min_positives_all_excluded_returns_none(self):
        assert macro_from_per_item(self.VALUES, self.N_POS, min_positives=3) is None

    def test_shrinkage_known_value(self):
        # pooled=5/6；A'=(2*0.75+5/6)/3=7/9；B'=(1.0+5/6)/2=11/12；mean=61/72
        r = macro_from_per_item(self.VALUES, self.N_POS, shrinkage_k=1.0)
        assert r == pytest.approx(61 / 72)

    def test_shrinkage_large_k_approaches_pooled(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, shrinkage_k=1e9)
        assert r == pytest.approx(5 / 6, abs=1e-6)


class TestComputeMacroPerItemMapParams:
    # 3 queries、2 items。A 正例 2 列（contrib 1.0、0.5 → AP 0.75）、B 1 列（1.0）
    GROUPS = np.array([0, 0, 1, 1, 2, 2])
    ITEMS = np.array(["A", "B", "A", "B", "A", "B"])
    Y = np.array([1, 0, 1, 0, 0, 1])
    SCORE = np.array([0.9, 0.1, 0.1, 0.9, 0.1, 0.9])

    def test_defaults_unchanged(self):
        r = compute_macro_per_item_map(self.GROUPS, self.ITEMS, self.Y, self.SCORE)
        assert r == pytest.approx(0.875)

    def test_weight_alpha(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weight_alpha=1.0
        )
        assert r == pytest.approx(5 / 6)

    def test_min_positives(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, min_positives=2
        )
        assert r == pytest.approx(0.75)

    def test_min_positives_all_excluded_returns_zero(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, min_positives=3
        )
        assert r == 0.0

    def test_shrinkage(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, shrinkage_k=1.0
        )
        assert r == pytest.approx(61 / 72)
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py -q 2>&1 | tail -5
```
Expected: ImportError（`macro_from_per_item` 不存在）。

- [ ] **Step 3: 實作**（`metrics.py`，`compute_mean_ap` 之後插入兩個新函式；`compute_macro_per_item_map` 改為呼叫它們）

```python
def positive_row_contributions(
    groups: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    k: Optional[int] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Per-positive-row AP contribution + original-order row indices.

    contrib[i] is the within-query cumulative precision of positive row
    row_idx[i] (zeroed when its rank exceeds ``k``). Queries with no
    positive rows contribute nothing. Shared by
    :func:`compute_macro_per_item_map` and the diagnosis bootstrap
    (``diagnosis.metric.uncertainty``) — cluster resampling never changes
    within-query ranking, so contributions are computed exactly once.
    """
    if len(groups) == 0:
        return np.array([], dtype=np.float64), np.array([], dtype=np.int64)

    sort_idx = np.lexsort((-y_score, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y_true[sort_idx].astype(np.float64, copy=False)

    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])

    contribs: list[np.ndarray] = []
    row_idx: list[np.ndarray] = []
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        y = y_sorted[s:e]
        if y.sum() == 0:
            continue
        positions = np.arange(1, len(y) + 1, dtype=np.float64)
        prec = np.cumsum(y) / positions
        if k is not None:
            prec = prec * (positions <= k)
        pos_mask = y == 1
        contribs.append(prec[pos_mask])
        row_idx.append(sort_idx[s:e][pos_mask])

    if not contribs:
        return np.array([], dtype=np.float64), np.array([], dtype=np.int64)
    return np.concatenate(contribs), np.concatenate(row_idx)


def macro_from_per_item(
    values: np.ndarray,
    n_pos: np.ndarray,
    weight_alpha: float = 0.0,
    min_positives: int = 0,
    shrinkage_k: float = 0.0,
) -> Optional[float]:
    """Parameterized macro combine over per-item values (spec §3 Phase 1).

    Order of operations: (1) drop items with ``n_pos < min_positives``;
    (2) shrink each surviving value toward the pooled (n_pos-weighted)
    mean of the survivors with factor ``n/(n+k)``; (3) weight items
    ``∝ n_pos**weight_alpha`` (alpha=0 → equal weight). Defaults reproduce
    the plain equal-weight mean bit-for-bit. Returns None when every item
    is excluded (caller picks the fallback).
    """
    keep = n_pos >= min_positives
    if not keep.any():
        return None
    v = values[keep].astype(np.float64, copy=True)
    n = n_pos[keep].astype(np.float64)
    if shrinkage_k > 0:
        pooled = float(np.dot(v, n) / n.sum())
        v = (n * v + shrinkage_k * pooled) / (n + shrinkage_k)
    w = n ** weight_alpha
    w = w / w.sum()
    return float(np.dot(w, v))
```

`compute_macro_per_item_map` 簽名擴充並改用上面兩個函式（**per-group 迴圈邏輯不重複寫**）：

```python
def compute_macro_per_item_map(
    groups: np.ndarray,
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    k: Optional[int] = None,
    weight_alpha: float = 0.0,
    min_positives: int = 0,
    shrinkage_k: float = 0.0,
) -> float:
```

函式主體（docstring 保留、補一段參數說明：預設 0/0/0 ＝ 現行等權 macro；語意見 `macro_from_per_item`）：

```python
    contrib_all, row_idx = positive_row_contributions(groups, y_true, y_score, k)
    if len(contrib_all) == 0:
        return 0.0

    items_all = items[row_idx]
    _, inv = np.unique(items_all, return_inverse=True)
    sums = np.bincount(inv, weights=contrib_all)
    counts = np.bincount(inv)
    per_item = sums / counts
    macro = macro_from_per_item(
        per_item, counts, weight_alpha, min_positives, shrinkage_k
    )
    return 0.0 if macro is None else macro
```

- [ ] **Step 4: 跑測試確認通過（重點：既有 `TestComputeMacroPerItemMap` 五個案例原樣全綠）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py -q 2>&1 | tail -5
```
Expected: 全綠、0 個既有測試被修改。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/evaluation/metrics.py tests/test_evaluation/test_metrics.py && \
git commit -m "feat(metrics): 參數化 macro per-item mAP（weight_alpha/min_positives/shrinkage_k，預設等價現行）"
```

---

### Task 4：Spark `macro_average` 接參數＋`_compute_core` 讀 `evaluation.metric`＋`observation_items`＋雙實作 parity

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py`（`macro_average`、`_compute_core:538`）
- Test: `tests/test_evaluation/test_metrics_spark.py`

- [ ] **Step 1: 寫失敗測試**

`_make_parameters`（`tests/test_evaluation/test_metrics_spark.py:60`）加 `metric` kwarg：

```python
def _make_parameters(k_values=(3,), segment_columns=(), metric=None):
    params = {
        "schema": {...原樣...},
        "evaluation": {
            "k_values": list(k_values),
            "segment_columns": list(segment_columns),
        },
    }
    if metric is not None:
        params["evaluation"]["metric"] = dict(metric)
    return params
```

新 fixture 與測試（加在 parity 測試 `test_macro_per_item_map_numpy_matches_spark` 附近）：

```python
def _three_customer_raw(spark):
    """A 正例 2 個 query（contrib 1.0、0.5 → AP 0.75, n_pos=2）、B 1 個（1.0, n_pos=1）。"""
    return spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.1, 0),
            ("20240331", "C1", "A", 0.1, 1),
            ("20240331", "C1", "B", 0.9, 0),
            ("20240331", "C2", "A", 0.1, 0),
            ("20240331", "C2", "B", 0.9, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def test_macro_average_weighted_by_n_pos():
    per_dim = {
        "A": {"map_attr@2": 0.75, "n_pos": 2},
        "B": {"map_attr@2": 1.0, "n_pos": 1},
    }
    assert ms.macro_average(per_dim, weight_alpha=1.0) == {
        "map_attr@2": pytest.approx(5 / 6)
    }
    assert ms.macro_average(per_dim, min_positives=2) == {
        "map_attr@2": pytest.approx(0.75)
    }
    assert ms.macro_average(per_dim, shrinkage_k=1.0) == {
        "map_attr@2": pytest.approx(61 / 72)
    }
    assert ms.macro_average(per_dim, min_positives=5) == {}


def test_macro_average_missing_n_pos_fails_loud():
    per_dim = {"A": {"map_attr@2": 0.75}, "B": {"map_attr@2": 1.0}}
    with pytest.raises(ValueError, match="n_pos"):
        ms.macro_average(per_dim, weight_alpha=1.0)


def test_compute_all_metrics_observation_items_and_param_macro(spark):
    df = _three_customer_raw(spark)
    # 預設參數：additive 鍵存在且為空、macro 不變
    base = ms.compute_all_metrics(df, _make_parameters(k_values=[2]))
    assert base["observation_items"] == []
    assert base["macro_avg"]["by_item"]["map_attr@2"] == pytest.approx(0.875)
    # min_positives=2：B 進觀察名單、macro 只剩 A
    params = _make_parameters(
        k_values=[2],
        metric={"weight_alpha": 0.0, "min_positives": 2, "shrinkage_k": 0},
    )
    result = ms.compute_all_metrics(df, params)
    assert result["observation_items"] == ["B"]
    assert result["macro_avg"]["by_item"]["map_attr@2"] == pytest.approx(0.75)


def test_param_macro_numpy_matches_spark(spark):
    """參數化後 numpy／Spark 兩實作同輸入同結果（spec Phase 1 parity 要求）。"""
    import numpy as np

    from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

    df = _three_customer_raw(spark)
    metric = {"weight_alpha": 1.0, "min_positives": 0, "shrinkage_k": 1.0}
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[2], metric=metric))
    spark_macro = result["macro_avg"]["by_item"]["map_attr@2"]

    rows = df.collect()
    group_ids = {("20240331", f"C{i}"): i for i in range(3)}
    groups = np.array([group_ids[(r["snap_date"], r["cust_id"])] for r in rows])
    items = np.array([r["prod_name"] for r in rows])
    y = np.array([r["label"] for r in rows])
    score = np.array([r["score"] for r in rows], dtype=np.float64)

    numpy_macro = compute_macro_per_item_map(
        groups, items, y, score,
        weight_alpha=1.0, min_positives=0, shrinkage_k=1.0,
    )
    assert numpy_macro == pytest.approx(spark_macro, rel=1e-12)
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics_spark.py -q -k "weighted or fails_loud or observation or param_macro" 2>&1 | tail -6
```
Expected: 4 failed（TypeError：macro_average 不收 kwargs）。

- [ ] **Step 3: 實作**

`macro_average` 換成參數版（沿用 Task 2 的保留鍵排除；數學走 `macro_from_per_item`）：

```python
def macro_average(
    per_dim: dict[str, dict[str, float]],
    *,
    weight_alpha: float = 0.0,
    min_positives: int = 0,
    shrinkage_k: float = 0.0,
) -> dict[str, float]:
    """Parameterized macro over dim keys (defaults = equal weight, 現行為).

    ``n_pos`` is the reserved weight-source key — never averaged into the
    output. Non-default params require every inner dict to carry ``n_pos``;
    otherwise raises ValueError (fail loud, no silent equal-weight fallback).
    Per-metric combine goes through ``metrics.macro_from_per_item``; a metric
    whose items are all excluded by ``min_positives`` is omitted.

    Default params take the ORIGINAL sum/len code path — bit-identical to the
    pre-parameterization behavior (the real-run regression gate compares
    report values verbatim; ``np.dot`` with uniform weights can differ from
    ``sum/len`` in the last ulp).
    """
    if not per_dim:
        return {}
    params_active = (
        weight_alpha != 0.0 or min_positives > 0 or shrinkage_k > 0
    )
    if not params_active:
        accum: dict[str, list[float]] = {}
        for metrics in per_dim.values():
            for k, v in metrics.items():
                if k == _N_POS_KEY:
                    continue
                accum.setdefault(k, []).append(float(v))
        return {k: sum(v) / len(v) for k, v in accum.items()}

    missing = [k for k, m in per_dim.items() if _N_POS_KEY not in m]
    if missing:
        raise ValueError(
            f"macro_average: weight_alpha/min_positives/shrinkage_k need "
            f"'n_pos' in every per-item dict; missing for {missing}. "
            f"Upstream must be aggregate_per_item (which emits n_pos)."
        )

    pairs_by_metric: dict[str, list[tuple[float, float]]] = {}
    for metrics in per_dim.values():
        n = float(metrics[_N_POS_KEY])
        for k, v in metrics.items():
            if k == _N_POS_KEY:
                continue
            pairs_by_metric.setdefault(k, []).append((float(v), n))
    out: dict[str, float] = {}
    for k, pairs in pairs_by_metric.items():
        values = np.array([p[0] for p in pairs])
        n_pos = np.array([p[1] for p in pairs])
        combined = macro_from_per_item(
            values, n_pos, weight_alpha, min_positives, shrinkage_k
        )
        if combined is not None:
            out[k] = combined
    return out
```

（Task 2 的過渡版 `macro_average` 就是上面「default 路徑」那段——Task 4 只是在它前後加參數分支，預設路徑一行不動。）

（`import numpy as np` 移到模組頂部既有 import 區；`from recsys_tfb.evaluation.metrics import macro_from_per_item` 亦然——同套件內 import，無邊界問題。）

`_compute_core`：讀 config、傳參數、算 `observation_items`。在 `eval_params = ...` 區塊之後加：

```python
    metric_cfg = eval_params.get("metric", {}) or {}
    metric_params = {
        "weight_alpha": float(metric_cfg.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(metric_cfg.get("min_positives", 0) or 0),
        "shrinkage_k": float(metric_cfg.get("shrinkage_k", 0) or 0.0),
    }
```

macro 區塊改為（`by_segment` 維持無參數——per-query 粒度無 n_pos，參數是 item 加權語意）：

```python
            macro_avg: dict = {"by_item": macro_average(per_item, **metric_params)}
            if per_segment:
                macro_avg["by_segment"] = macro_average(per_segment)
            if per_item_segment:
                macro_avg["by_item_segment"] = macro_average(
                    per_item_segment, **metric_params
                )

            observation_items = sorted(
                it for it, m in per_item.items()
                if m.get("n_pos", 0) < metric_params["min_positives"]
            ) if metric_params["min_positives"] > 0 else []
```

return dict 加一鍵 `"observation_items": observation_items,`（`"macro_avg"` 之後）。空結果路徑（`n_queries_with_pos == 0` 的 `_EMPTY_RESULT` return）也補 `"observation_items": []`——直接在 `_EMPTY_RESULT` dict（`:529`）加 `"observation_items": []`。`compute_all_metrics` docstring 的 Returns 區塊補 `observation_items` 一行（額外說明：additive、預設空）。

- [ ] **Step 4: 跑測試確認通過**（本檔全量＋orchestrator/slim/category 三檔確認 additive 鍵不打壞既有斷言）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics_spark.py tests/test_evaluation/test_metrics_spark_orchestrator.py \
  tests/test_evaluation/test_metrics_spark_slim.py tests/test_evaluation/test_metrics_spark_category.py \
  -q 2>&1 | tail -5
```
Expected: 全綠（與 baseline 一致）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/evaluation/metrics_spark.py tests/test_evaluation/test_metrics_spark.py && \
git commit -m "feat(metrics-spark): macro_average 參數化＋_compute_core 接 evaluation.metric＋observation_items"
```

---

### Task 5：config（`evaluation.metric`／`evaluation.diagnosis`）＋ consistency A15

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`
- Modify: `src/recsys_tfb/core/consistency.py`（docstring legend＋新 predicate＋`validate_config_consistency:476` 串接）
- Test: `tests/test_core/test_consistency.py`、`tests/test_evaluation/test_parameters_evaluation_yaml.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_core/test_consistency.py` 檔尾新增（該檔既有測試直接呼叫 predicate，照同款式）：

```python
class TestDiagnosisMetricParamsA15:
    def _params(self, metric=None, sample=None, ci=None):
        ev = {}
        if metric is not None:
            ev["metric"] = metric
        diag = {}
        if sample is not None:
            diag["sample"] = sample
        if ci is not None:
            diag["ci"] = ci
        if diag:
            ev["diagnosis"] = diag
        return {"evaluation": ev}

    def test_absent_blocks_are_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        assert diagnosis_metric_param_errors({}) == []
        assert diagnosis_metric_param_errors(self._params()) == []

    def test_valid_defaults_are_clean(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = self._params(
            metric={"weight_alpha": 0.0, "k": None, "min_positives": 0,
                    "shrinkage_k": 0},
            sample={"max_queries": 200000, "min_pos_queries_per_item": 50,
                    "seed": 42},
            ci={"enabled": True, "n_boot": 200},
        )
        assert diagnosis_metric_param_errors(p) == []

    def test_each_bad_value_reports(self):
        from recsys_tfb.core.consistency import diagnosis_metric_param_errors
        p = self._params(
            metric={"weight_alpha": 1.5, "k": 0, "min_positives": -1,
                    "shrinkage_k": -0.1},
            sample={"max_queries": 0, "min_pos_queries_per_item": 0},
            ci={"n_boot": 0},
        )
        errors = diagnosis_metric_param_errors(p)
        assert len(errors) == 7
        joined = "\n".join(errors)
        for token in ["weight_alpha", "metric.k", "min_positives",
                      "shrinkage_k", "max_queries",
                      "min_pos_queries_per_item", "n_boot"]:
            assert token in joined

    def test_wired_into_validate(self):
        import pytest as _pytest
        from recsys_tfb.core.consistency import (
            ConfigConsistencyError,
            validate_config_consistency,
        )
        p = self._params(metric={"weight_alpha": 2.0})
        with _pytest.raises(ConfigConsistencyError, match="weight_alpha"):
            validate_config_consistency(p)
```

（注意：`validate_config_consistency` 需要能吃「幾乎空的 parameters」——既有 predicate 對缺 schema 的行為以現檔為準；若 `_params` 最小 dict 讓其他 predicate 先炸，改成從該測試檔既有的合法 params fixture 複製一份再疊加 `evaluation` 區塊，維持測試意圖不變。）

`tests/test_evaluation/test_parameters_evaluation_yaml.py` 檔尾新增：

```python
def test_metric_and_diagnosis_blocks():
    ev = _load()
    assert ev["metric"] == {
        "weight_alpha": 0.0, "k": None, "min_positives": 0, "shrinkage_k": 0,
    }
    diag = ev["diagnosis"]
    assert diag["sample"] == {
        "max_queries": 200000, "min_pos_queries_per_item": 50, "seed": 42,
    }
    assert diag["ci"] == {"enabled": True, "n_boot": 200}
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py tests/test_evaluation/test_parameters_evaluation_yaml.py -q 2>&1 | tail -5
```
Expected: 新增測試 failed（ImportError／KeyError `metric`），既有全綠。

- [ ] **Step 3: 實作 config**（`conf/base/parameters_evaluation.yaml`，插在 `compare_sources` 區塊之前）

```yaml
  # 主指標參數化家族（框架診斷項目 4；A15）。預設值全部等價現行為：
  # weight_alpha=0 ＝ item 等權 macro；min_positives=0 ＝ 不設觀察名單門檻；
  # shrinkage_k=0 ＝ 不收縮。作用順序：min_positives 先排除 → shrinkage 向
  # pooled（n_pos 加權）平均收縮 → weight_alpha 加權（w_j ∝ n_pos^α）。
  # 只作用於 item 粒度（by_item / by_item_segment）；by_segment 維持等權。
  # k = 診斷側（CI）計算 AP 的截斷 k；null ＝ 不截斷（full mAP）。
  metric:
    weight_alpha: 0.0
    k: null
    min_positives: 0
    shrinkage_k: 0

  # 評估側診斷（Phase 1 起；spec §2 共用抽樣底座 + §3 Phase 1 CI）。
  diagnosis:
    # 診斷抽樣：單位＝query（snap_date × cust_id）、只取有正例的 query；
    # 兩趟設計——正例 query 數低於保底的 item 整批全取，其餘 CRC32
    # hash-ratio 抽到補滿 max_queries。中型 item 抽後仍可能低於保底：
    # 不硬補，metadata 誠實回報（報表會標示樣本規模）。
    sample:
      max_queries: 200000
      min_pos_queries_per_item: 50
      seed: 42
    # per-item AP 與 macro 的 cluster bootstrap CI（cluster＝cust_id，
    # 同一客戶跨期整批重抽）。driver 端 numpy、跑在上面的抽樣上。
    ci:
      enabled: true
      n_boot: 200
```

- [ ] **Step 4: 實作 A15 predicate**（`consistency.py`，`segment_columns_without_source` 之後）

```python
def diagnosis_metric_param_errors(parameters: dict) -> list[str]:
    """evaluation.metric / evaluation.diagnosis parameter domains (A15).

    Absent blocks are fine (all keys have behavior-preserving defaults);
    present values must be in-domain, else the metric family silently
    degenerates (e.g. alpha>1 over-concentrates on hot items) or the
    bootstrap is undefined (n_boot<1).
    """
    errors: list[str] = []
    ev = parameters.get("evaluation", {}) or {}
    metric = ev.get("metric", {}) or {}
    diag = ev.get("diagnosis", {}) or {}
    sample = diag.get("sample", {}) or {}
    ci = diag.get("ci", {}) or {}

    alpha = metric.get("weight_alpha", 0.0)
    if not (_is_number(alpha) and 0.0 <= float(alpha) <= 1.0):
        errors.append(
            f"evaluation.metric.weight_alpha={alpha!r} must be a number in "
            f"[0, 1] (0 = equal-weight macro, 1 = positive-count weighting)."
        )
    k = metric.get("k", None)
    if k is not None and not (
        isinstance(k, int) and not isinstance(k, bool) and k >= 1
    ):
        errors.append(
            f"evaluation.metric.k={k!r} must be null (no truncation) or an "
            f"int >= 1."
        )
    mp = metric.get("min_positives", 0)
    if not (isinstance(mp, int) and not isinstance(mp, bool) and mp >= 0):
        errors.append(
            f"evaluation.metric.min_positives={mp!r} must be an int >= 0."
        )
    sk = metric.get("shrinkage_k", 0)
    if not (_is_number(sk) and float(sk) >= 0.0):
        errors.append(
            f"evaluation.metric.shrinkage_k={sk!r} must be a number >= 0."
        )

    for key, val, floor in (
        ("evaluation.diagnosis.sample.max_queries",
         sample.get("max_queries", 200000), 1),
        ("evaluation.diagnosis.sample.min_pos_queries_per_item",
         sample.get("min_pos_queries_per_item", 50), 1),
        ("evaluation.diagnosis.ci.n_boot", ci.get("n_boot", 200), 1),
    ):
        if not (isinstance(val, int) and not isinstance(val, bool)
                and val >= floor):
            errors.append(f"{key}={val!r} must be an int >= {floor}.")
    return errors
```

`validate_config_consistency` 串接（`segment_columns_without_source` 區塊之後、`if errors:` 之前）：

```python
    errors.extend(diagnosis_metric_param_errors(parameters))
```

docstring legend 在 A14 條目之後補：

```
* A15 — ``evaluation.metric`` / ``evaluation.diagnosis`` parameter domains:
  ``weight_alpha`` ∈ [0,1]; ``k`` null or int ≥ 1; ``min_positives`` ≥ 0;
  ``shrinkage_k`` ≥ 0; ``diagnosis.sample.max_queries`` ≥ 1;
  ``diagnosis.sample.min_pos_queries_per_item`` ≥ 1;
  ``diagnosis.ci.n_boot`` ≥ 1. Predicate: ``diagnosis_metric_param_errors``.
```

- [ ] **Step 5: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py tests/test_evaluation/test_parameters_evaluation_yaml.py -q 2>&1 | tail -5
```
Expected: 全綠。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add conf/base/parameters_evaluation.yaml src/recsys_tfb/core/consistency.py \
  tests/test_core/test_consistency.py tests/test_evaluation/test_parameters_evaluation_yaml.py && \
git commit -m "feat(config): evaluation.metric/diagnosis 區塊＋consistency A15"
```

---

### Task 6：`diagnosis/metric/sample.py` — 兩趟診斷抽樣

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/sample.py`
- Create: `tests/test_diagnosis/test_metric/__init__.py`（空檔）
- Test: `tests/test_diagnosis/test_metric/test_sample.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""draw_diagnosis_sample：兩趟設計（小 item 全取＋hash-ratio 補滿）、正例 query only、決定性。"""

import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample


def _params(max_queries=3, floor=2, seed=42):
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {
            "diagnosis": {
                "sample": {
                    "max_queries": max_queries,
                    "min_pos_queries_per_item": floor,
                    "seed": seed,
                },
            },
        },
    }


def _fixture(spark):
    """hot：4 個正例 query（H1..H4）；cold：1 個（C1）；N1 無正例（必須被排除）。
    每個 query 兩個候選列（hot、cold 各一）。"""
    rows = []
    for cust in ["H1", "H2", "H3", "H4"]:
        rows.append(("20240331", cust, "hot", 0.9, 1))
        rows.append(("20240331", cust, "cold", 0.1, 0))
    rows.append(("20240331", "C1", "hot", 0.9, 0))
    rows.append(("20240331", "C1", "cold", 0.1, 1))
    rows.append(("20240331", "N1", "hot", 0.9, 0))
    rows.append(("20240331", "N1", "cold", 0.1, 0))
    return spark.createDataFrame(
        rows, schema=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


def test_cold_item_queries_taken_in_full_and_no_positive_free_queries(spark):
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params())
    custs = set(pdf["cust_id"])
    assert "C1" in custs            # cold 正例 query 數 1 < 保底 2 → 全取
    assert "N1" not in custs        # 無正例 query 不進樣本
    assert meta["take_all_items"] == ["cold"]
    assert meta["n_pos_queries_total"] == 5
    assert 1 <= meta["n_queries_sampled"] <= 5


def test_sampled_queries_keep_all_candidate_rows(spark):
    pdf, _ = draw_diagnosis_sample(_fixture(spark), _params())
    sizes = pdf.groupby(["snap_date", "cust_id"]).size()
    assert (sizes == 2).all()       # 被抽中的 query 帶完整候選列（含負例）


def test_deterministic_given_seed(spark):
    df = _fixture(spark)
    pdf1, meta1 = draw_diagnosis_sample(df, _params())
    pdf2, meta2 = draw_diagnosis_sample(df, _params())
    key = ["snap_date", "cust_id", "prod_name"]
    pd.testing.assert_frame_equal(
        pdf1.sort_values(key).reset_index(drop=True),
        pdf2.sort_values(key).reset_index(drop=True),
    )
    assert meta1 == meta2


def test_metadata_shape(spark):
    _, meta = draw_diagnosis_sample(_fixture(spark), _params())
    for k in ["n_pos_queries_total", "n_queries_sampled", "take_all_items",
              "per_item_pos_queries_sampled", "max_queries",
              "min_pos_queries_per_item", "seed", "sample_ratio"]:
        assert k in meta
    assert meta["per_item_pos_queries_sampled"]["cold"] == 1
    assert meta["seed"] == 42


def test_take_all_when_everything_is_small(spark):
    # 保底拉到 10 > 所有 item 的正例 query 數 → 全部 take-all、全量進樣本
    pdf, meta = draw_diagnosis_sample(_fixture(spark), _params(floor=10))
    assert sorted(meta["take_all_items"]) == ["cold", "hot"]
    assert meta["n_queries_sampled"] == 5
    assert set(pdf["cust_id"]) == {"H1", "H2", "H3", "H4", "C1"}
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -q 2>&1 | tail -5
```
Expected: ModuleNotFoundError（`diagnosis.metric.sample` 不存在）。

- [ ] **Step 3: 實作**（`src/recsys_tfb/diagnosis/metric/sample.py`，完整檔案）

```python
"""Bounded driver-side diagnosis sample（spec §2 共用底座）.

抽樣單位＝query（time × entity），只取有正例的 query（指標只由它們定義）。
兩趟設計：pass 1 count 每 item 的正例 query 數；正例 query 少於保底
``min_pos_queries_per_item`` 的 item 整批全取（take-all），其餘 query 用
CRC32 hash-ratio（``utils.hashing``）抽到補滿 ``max_queries``。被抽中的
query 帶回全部候選列（含負例，排序需要），``toPandas()`` 落到 driver 供
numpy 迭代計算（bootstrap／offset sweep／成對帳本）重複使用。

誠實限制：中型 item 經 hash-ratio 抽樣後仍可能低於保底——不硬補，
metadata 回報實際覆蓋＋log WARN；報表必須標示樣本規模，不得讓抽樣估計
冒充全量。
"""
from __future__ import annotations

import logging

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema
from recsys_tfb.utils.hashing import ratio_to_threshold, spark_bucket

logger = logging.getLogger(__name__)

_SITE = "diagnosis_sample"


def draw_diagnosis_sample(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> tuple[pd.DataFrame, dict]:
    """兩趟診斷抽樣。回傳 (sample_pdf, metadata)。

    sample_pdf 欄位：query cols（time + entity）、item、label、score、
    （存在時）score_uncalibrated。metadata 見模組 docstring。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]
    query_cols = [time_col] + entity_cols

    cfg = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("sample", {}) or {}
    )
    max_queries = int(cfg.get("max_queries", 200000))
    floor = int(cfg.get("min_pos_queries_per_item", 50))
    seed = int(cfg.get("seed", 42))

    keep_cols = [
        c
        for c in [*query_cols, item_col, label_col, score_col,
                  "score_uncalibrated"]
        if c in eval_predictions.columns
    ]
    df = eval_predictions.select(*keep_cols)

    # ---- pass 1：正例 query 全集＋per-item 正例 query 數 ----
    pos_rows = df.filter(F.col(label_col) == 1)
    pos_queries = pos_rows.select(*query_cols).distinct()
    n_pos_total = pos_queries.count()

    item_counts = {
        str(r[item_col]): int(r["cnt"])
        for r in pos_rows.select(*query_cols, item_col)
        .distinct()
        .groupBy(item_col)
        .agg(F.count(F.lit(1)).alias("cnt"))
        .collect()
    }
    take_all_items = sorted(
        it for it, c in item_counts.items() if c < floor
    )

    # ---- pass 2：take-all ∪ hash-ratio ----
    if take_all_items:
        must = (
            pos_rows.filter(F.col(item_col).isin(take_all_items))
            .select(*query_cols)
            .distinct()
        )
        n_must = must.count()
        others = pos_queries.join(must, on=query_cols, how="left_anti")
    else:
        must = None
        n_must = 0
        others = pos_queries
    n_others = n_pos_total - n_must

    budget = max_queries - n_must
    if budget <= 0:
        logger.warning(
            "diagnosis sample: take-all queries (%d) already exceed "
            "max_queries=%d — sample is take-all only",
            n_must, max_queries,
        )
        ratio = 0.0
        sampled = must
    elif n_others == 0:
        ratio = 0.0
        sampled = must if must is not None else pos_queries.limit(0)
    else:
        ratio = min(1.0, budget / n_others)
        threshold = ratio_to_threshold(ratio)
        picked = others.filter(
            spark_bucket(others, query_cols, seed, _SITE) < threshold
        )
        sampled = picked if must is None else picked.unionByName(must)

    sample_pdf = df.join(sampled, on=query_cols, how="inner").toPandas()

    # ---- metadata（報表據此標示「抽樣估計＋樣本規模」）----
    n_sampled = int(
        sample_pdf[query_cols].drop_duplicates().shape[0]
    ) if len(sample_pdf) else 0
    pos_sampled = sample_pdf[sample_pdf[label_col] == 1]
    per_item_sampled = {
        str(k): int(v)
        for k, v in pos_sampled.drop_duplicates([*query_cols, item_col])
        .groupby(item_col)
        .size()
        .items()
    }
    below = {
        it: per_item_sampled.get(it, 0)
        for it in item_counts
        if it not in take_all_items and per_item_sampled.get(it, 0) < floor
    }
    if below:
        logger.warning(
            "diagnosis sample: items below per-item floor after hash "
            "sampling (not topped up by design): %s", below,
        )
    meta = {
        "n_pos_queries_total": int(n_pos_total),
        "n_queries_sampled": n_sampled,
        "sample_ratio": float(ratio),
        "take_all_items": take_all_items,
        "per_item_pos_queries_sampled": per_item_sampled,
        "items_below_floor_after_sampling": below,
        "max_queries": max_queries,
        "min_pos_queries_per_item": floor,
        "seed": seed,
    }
    return sample_pdf, meta
```

- [ ] **Step 4: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_sample.py -q 2>&1 | tail -5
```
Expected: 5 passed。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis/metric/sample.py tests/test_diagnosis/test_metric/ && \
git commit -m "feat(diagnosis): 兩趟診斷抽樣 draw_diagnosis_sample（小 item 全取＋CRC32 ratio）"
```

---

### Task 7：`diagnosis/metric/uncertainty.py` — cust_id-cluster bootstrap CI

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/uncertainty.py`
- Test: `tests/test_diagnosis/test_metric/test_uncertainty.py`

- [ ] **Step 1: 寫失敗測試**

```python
"""bootstrap_per_item_ci：cluster bootstrap 的決定性、退化案例、覆蓋性質。"""

import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci


def _params(n_boot=50, k=None, metric_extra=None, seed=42):
    metric = {"weight_alpha": 0.0, "k": k, "min_positives": 0, "shrinkage_k": 0}
    if metric_extra:
        metric.update(metric_extra)
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {
            "metric": metric,
            "diagnosis": {
                "sample": {"max_queries": 1000,
                           "min_pos_queries_per_item": 1, "seed": seed},
                "ci": {"enabled": True, "n_boot": n_boot},
            },
        },
    }


def _pdf(rows):
    return pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


THREE_CUST = [
    # A：C0 rank1（contrib 1.0）、C1 rank2（0.5）→ AP 0.75, n_pos=2
    # B：C2 rank1（1.0）→ AP 1.0, n_pos=1；等權 macro = 0.875
    ("20240331", "C0", "A", 0.9, 1),
    ("20240331", "C0", "B", 0.1, 0),
    ("20240331", "C1", "A", 0.1, 1),
    ("20240331", "C1", "B", 0.9, 0),
    ("20240331", "C2", "A", 0.1, 0),
    ("20240331", "C2", "B", 0.9, 1),
]


def test_point_estimates_match_metric_family():
    out = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    assert out["per_item"]["A"]["ap"] == pytest.approx(0.75)
    assert out["per_item"]["A"]["n_pos"] == 2
    assert out["per_item"]["B"]["ap"] == pytest.approx(1.0)
    assert out["macro"]["ap"] == pytest.approx(0.875)
    assert out["n_boot"] == 50


def test_ci_brackets_point_and_is_deterministic():
    out1 = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    out2 = bootstrap_per_item_ci(_pdf(THREE_CUST), _params())
    assert out1 == out2
    m = out1["macro"]
    assert m["ci_low"] <= m["ap"] <= m["ci_high"]
    a = out1["per_item"]["A"]
    assert a["ci_low"] <= a["ap"] <= a["ci_high"]


def test_single_cluster_degenerates_to_zero_width():
    rows = [
        ("20240331", "C0", "A", 0.9, 1),
        ("20240331", "C0", "B", 0.1, 0),
    ]
    out = bootstrap_per_item_ci(_pdf(rows), _params())
    a = out["per_item"]["A"]
    assert a["ap"] == a["ci_low"] == a["ci_high"] == pytest.approx(1.0)


def test_k_truncation_zeroes_deep_positive():
    rows = [
        ("20240331", "C0", "A", 0.9, 0),
        ("20240331", "C0", "B", 0.1, 1),   # rank 2；k=1 → contrib 0
    ]
    out = bootstrap_per_item_ci(_pdf(rows), _params(k=1))
    assert out["per_item"]["B"]["ap"] == pytest.approx(0.0)
    assert out["k"] == 1


def test_metric_params_flow_into_macro():
    out = bootstrap_per_item_ci(
        _pdf(THREE_CUST), _params(metric_extra={"weight_alpha": 1.0})
    )
    assert out["macro"]["ap"] == pytest.approx(5 / 6)
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_uncertainty.py -q 2>&1 | tail -5
```
Expected: ModuleNotFoundError。

- [ ] **Step 3: 實作**（`src/recsys_tfb/diagnosis/metric/uncertainty.py`，完整檔案）

```python
"""per-item AP 與 macro 的 cluster bootstrap CI（spec §3 Phase 1）.

cluster＝entity（cust_id）：同一客戶跨期整批重抽。關鍵簡化：重抽整個
cluster 不改變任何 query 內的排序，所以每列正例貢獻
（``evaluation.metrics.positive_row_contributions``）只算一次；每個
replicate 只是帶 cluster 乘數的重新聚合（bincount with weights），
n_boot=200 在 driver 端 numpy 上是毫秒級。

CI＝percentile bootstrap（2.5 / 97.5）。某 item 在部分 replicate 中可能
沒有正例列（該客戶群沒被抽到）——以 NaN 略過（nanpercentile）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import (
    macro_from_per_item,
    positive_row_contributions,
)


def bootstrap_per_item_ci(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    """在診斷抽樣上估 per-item AP 與 macro 的 CI。回傳可直接 JSON 序列化的 dict。

    點估計與每個 replicate 都套 ``evaluation.metric`` 的參數家族
    （weight_alpha / min_positives / shrinkage_k；k＝截斷）。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    eval_params = parameters.get("evaluation", {}) or {}
    metric_cfg = eval_params.get("metric", {}) or {}
    k = metric_cfg.get("k", None)
    metric_params = {
        "weight_alpha": float(metric_cfg.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(metric_cfg.get("min_positives", 0) or 0),
        "shrinkage_k": float(metric_cfg.get("shrinkage_k", 0) or 0.0),
    }
    diag_cfg = eval_params.get("diagnosis", {}) or {}
    n_boot = int((diag_cfg.get("ci", {}) or {}).get("n_boot", 200))
    seed = int((diag_cfg.get("sample", {}) or {}).get("seed", 42))

    # query id（time × entity）與 cluster id（entity only）
    query_key = (
        sample_pdf[time_col].astype(str)
        + "|"
        + sample_pdf[entity_cols].astype(str).agg("|".join, axis=1)
    )
    groups = pd.factorize(query_key)[0]
    cluster_key = sample_pdf[entity_cols].astype(str).agg("|".join, axis=1)
    clusters = pd.factorize(cluster_key)[0]

    y = sample_pdf[label_col].to_numpy()
    score = sample_pdf[score_col].to_numpy(dtype=np.float64)
    items = sample_pdf[item_col].astype(str).to_numpy()

    contrib, row_idx = positive_row_contributions(groups, y, score, k)
    if len(contrib) == 0:
        return {
            "enabled": True, "k": k, "n_boot": n_boot, "seed": seed,
            "metric_params": metric_params,
            "per_item": {}, "macro": None,
        }

    item_of = items[row_idx]
    cluster_of = clusters[row_idx]
    uniq_items, item_inv = np.unique(item_of, return_inverse=True)
    n_items = len(uniq_items)
    n_clusters = int(clusters.max()) + 1

    # ---- 點估計 ----
    sums = np.bincount(item_inv, weights=contrib, minlength=n_items)
    counts = np.bincount(item_inv, minlength=n_items).astype(np.float64)
    point = sums / counts
    macro_point = macro_from_per_item(point, counts, **metric_params)

    # ---- bootstrap：重抽 cluster、帶乘數重新聚合 ----
    rng = np.random.RandomState(seed)
    boot_items = np.full((n_boot, n_items), np.nan)
    boot_macro = np.full(n_boot, np.nan)
    for b in range(n_boot):
        draw = rng.randint(0, n_clusters, n_clusters)
        mult = np.bincount(draw, minlength=n_clusters).astype(np.float64)
        w = mult[cluster_of]
        s = np.bincount(item_inv, weights=contrib * w, minlength=n_items)
        c = np.bincount(item_inv, weights=w, minlength=n_items)
        present = c > 0
        vals = np.divide(s, c, out=np.full(n_items, np.nan), where=present)
        boot_items[b] = vals
        m = macro_from_per_item(
            vals[present], c[present], **metric_params
        )
        if m is not None:
            boot_macro[b] = m

    lo = np.nanpercentile(boot_items, 2.5, axis=0)
    hi = np.nanpercentile(boot_items, 97.5, axis=0)

    per_item = {
        str(uniq_items[j]): {
            "ap": float(point[j]),
            "ci_low": float(lo[j]),
            "ci_high": float(hi[j]),
            "n_pos": int(counts[j]),
        }
        for j in range(n_items)
    }
    macro = None
    if macro_point is not None and not np.all(np.isnan(boot_macro)):
        macro = {
            "ap": float(macro_point),
            "ci_low": float(np.nanpercentile(boot_macro, 2.5)),
            "ci_high": float(np.nanpercentile(boot_macro, 97.5)),
        }
    return {
        "enabled": True, "k": k, "n_boot": n_boot, "seed": seed,
        "metric_params": metric_params,
        "per_item": per_item, "macro": macro,
    }
```

- [ ] **Step 4: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/ -q 2>&1 | tail -5
```
Expected: 全綠（含 Task 6 的 sample 測試）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis/metric/uncertainty.py tests/test_diagnosis/test_metric/test_uncertainty.py && \
git commit -m "feat(diagnosis): cust_id-cluster bootstrap CI（bootstrap_per_item_ci）"
```

---

### Task 8：評估 pipeline 加 `compute_metric_ci` 節點＋catalog 產物

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `conf/base/catalog.yaml`
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`、`tests/test_pipelines/test_evaluation/test_pipeline.py`

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipelines/test_evaluation/test_nodes_spark.py` 檔尾新增（沿用該檔既有 spark fixture 慣例；params 構法同 Task 7 `_params` 但含完整 evaluation 區塊）：

```python
def test_compute_metric_ci_disabled_returns_stub(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_metric_ci
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {"diagnosis": {"ci": {"enabled": False}}},
    }
    assert compute_metric_ci(None, params) == {"enabled": False}


def test_compute_metric_ci_end_to_end_small(spark):
    from recsys_tfb.pipelines.evaluation.nodes_spark import compute_metric_ci
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.1, 0),
            ("20240331", "C1", "A", 0.1, 1),
            ("20240331", "C1", "B", 0.9, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"}},
        "evaluation": {
            "metric": {"weight_alpha": 0.0, "k": None,
                       "min_positives": 0, "shrinkage_k": 0},
            "diagnosis": {
                "sample": {"max_queries": 100,
                           "min_pos_queries_per_item": 1, "seed": 42},
                "ci": {"enabled": True, "n_boot": 20},
            },
        },
    }
    out = compute_metric_ci(df, params)
    assert out["enabled"] is True
    assert "A" in out["per_item"] and "macro" in out and "sample" in out
    assert out["sample"]["n_queries_sampled"] == 2
```

`tests/test_pipelines/test_evaluation/test_pipeline.py`：把兩處 `test_pipeline_has_five_nodes` 的 5 改 6；`test_node_names` 的名單加 `"compute_metric_ci"`（在 `"generate_report"` 之前）；outputs 斷言加 `"evaluation_metric_ci"`（該檔實際斷言式樣以現檔為準，維持斷言意圖）。

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```
Expected: 新增測試 ImportError；pipeline 結構測試（改 6 之後）failed。

- [ ] **Step 3: 實作**

`nodes_spark.py` 檔尾新增（薄 node，領域邏輯全在 diagnosis）：

```python
def compute_metric_ci(
    eval_predictions: Optional[SparkDataFrame],
    parameters: dict,
) -> dict:
    """診斷抽樣＋cluster bootstrap CI（spec §3 Phase 1）。

    薄 node：抽樣與 bootstrap 都在 ``diagnosis.metric``。停用時回傳 stub
    （catalog 仍寫出 ``{"enabled": false}``，產物路徑恆存在、下游可判別）。
    輸出含 ``sample`` metadata——CI 是抽樣估計，報表必須標示樣本規模。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    ci_cfg = ((eval_params.get("diagnosis", {}) or {}).get("ci", {}) or {})
    if not ci_cfg.get("enabled", True):
        logger.info("metric CI disabled — writing stub")
        return {"enabled": False}

    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample
    from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci

    sample_pdf, sample_meta = draw_diagnosis_sample(eval_predictions, parameters)
    out = bootstrap_per_item_ci(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "metric CI computed on %d sampled queries (n_boot=%d)",
        sample_meta["n_queries_sampled"], out["n_boot"],
    )
    return out
```

`pipeline.py`：import 補 `compute_metric_ci`；default 路徑 nodes 清單在 `compute_baseline_metrics` 之後插入，並讓 `generate_report` 多吃一個輸入：

```python
        Node(
            compute_metric_ci,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_metric_ci",
        ),
        Node(
            generate_report,
            inputs=["eval_predictions", "evaluation_metrics",
                    "parameters", "baseline_metrics", "evaluation_metric_ci"],
            outputs="evaluation_report",
        ),
```

`generate_report` 簽名加尾參（本 task 只收不render，render 在 Task 9）：

```python
def generate_report(
    eval_predictions: SparkDataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict] = None,
    metric_ci: Optional[dict] = None,
) -> str:
```

`conf/base/catalog.yaml`：`evaluation_report` 條目之前插入：

```yaml
# --- Evaluation pipeline — 診斷產物（Phase 1 起；抽樣估計，含 sample metadata）---
evaluation_metric_ci:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/metric_ci.json
```

`--compare-only` 短 pipeline 不加此節點（維持現狀）。

- [ ] **Step 4: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/pipelines/evaluation/ conf/base/catalog.yaml tests/test_pipelines/test_evaluation/ && \
git commit -m "feat(evaluation): compute_metric_ci 節點＋metric_ci.json catalog 產物"
```

---

### Task 9：報表 CI 欄＋觀察名單

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（`build_primary_map_section:115`、`build_per_item_attr_section:312`、`assemble_report:571`）
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（`generate_report` 把 metric_ci 傳給 assemble_report）
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫失敗測試**（檔尾新增；自帶最小 fixture，不依賴該檔既有 helper）

```python
def _metrics_min():
    return {
        "overall": {"map@2": 0.8, "precision@2": 0.5,
                    "ndcg@2": 0.9, "recall@2": 1.0},
        "per_item": {
            "A": {"map_attr@2": 0.75, "ndcg_attr@2": 0.8,
                  "hit_rate@2": 1.0, "mean_pos": 1.5, "n_pos": 2},
            "B": {"map_attr@2": 1.0, "ndcg_attr@2": 1.0,
                  "hit_rate@2": 1.0, "mean_pos": 1.0, "n_pos": 1},
        },
        "macro_avg": {"by_item": {"map_attr@2": 0.875, "ndcg_attr@2": 0.9,
                                  "hit_rate@2": 1.0, "mean_pos": 1.25}},
        "observation_items": [],
        "n_queries": 3,
        "n_excluded_queries": 0,
        "dataset_overview": {"totals": {"n_products": 2}},
    }


_CI_FIXTURE = {
    "enabled": True, "n_boot": 50, "k": None, "seed": 42,
    "metric_params": {"weight_alpha": 0.0, "min_positives": 0,
                      "shrinkage_k": 0.0},
    "per_item": {
        "A": {"ap": 0.74, "ci_low": 0.60, "ci_high": 0.90, "n_pos": 2},
        "B": {"ap": 1.0, "ci_low": 1.0, "ci_high": 1.0, "n_pos": 1},
    },
    "macro": {"ap": 0.87, "ci_low": 0.80, "ci_high": 0.95},
    "sample": {"n_queries_sampled": 3, "n_pos_queries_total": 3},
}


def _params_min():
    return {"evaluation": {"report": {"display": {"primary_map_k": [2]}}}}


def test_per_item_attr_ci_columns_present_when_metric_ci_given():
    from recsys_tfb.evaluation.report_builder import build_per_item_attr_section
    sec = build_per_item_attr_section(
        _metrics_min(), _params_min(), metric_ci=_CI_FIXTURE
    )
    map_tbl = sec.tables[0]
    for col in ["AP(抽樣)", "CI 2.5%", "CI 97.5%"]:
        assert col in map_tbl.columns
    assert map_tbl.loc["A", "CI 2.5%"] == 0.60
    assert map_tbl.loc["Macro 平均", "AP(抽樣)"] == 0.87
    assert "抽樣" in sec.description and "50" in sec.description


def test_per_item_attr_no_ci_columns_when_absent():
    from recsys_tfb.evaluation.report_builder import build_per_item_attr_section
    sec = build_per_item_attr_section(_metrics_min(), _params_min())
    assert "AP(抽樣)" not in sec.tables[0].columns


def test_per_item_attr_observation_list_table():
    from recsys_tfb.evaluation.report_builder import build_per_item_attr_section
    metrics = _metrics_min()
    metrics["observation_items"] = ["B"]
    sec = build_per_item_attr_section(metrics, _params_min())
    assert "觀察名單" in sec.table_titles[-1]
    obs_tbl = sec.tables[-1]
    assert list(obs_tbl.index) == ["B"]
    assert obs_tbl.loc["B", "n_pos"] == 1


def test_primary_map_macro_ci_table():
    from recsys_tfb.evaluation.report_builder import build_primary_map_section
    sec = build_primary_map_section(
        _metrics_min(), _params_min(), metric_ci=_CI_FIXTURE
    )
    assert any("macro" in t.lower() and "CI" in t for t in sec.table_titles)
    ci_tbl = sec.tables[-1]
    assert ci_tbl.loc["macro per-item mAP", "CI 97.5%"] == 0.95


def test_assemble_report_passes_metric_ci_through():
    from recsys_tfb.evaluation.report_builder import assemble_report
    html = assemble_report(
        _metrics_min(), _params_min(), metric_ci=_CI_FIXTURE
    )
    assert "CI 2.5%" in html
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -q 2>&1 | tail -5
```
Expected: 5 個新測試 failed（TypeError：不收 `metric_ci` kwarg）。

- [ ] **Step 3: 實作**

`build_per_item_attr_section(metrics, parameters, metric_ci=None)`：在 `map_tbl = _per_item_metric_table(...)`（含 macro row 的那份）之後、`return` 之前加：

```python
    description_extra = ""
    if metric_ci and metric_ci.get("enabled"):
        ci_items = metric_ci.get("per_item", {}) or {}
        ci_macro = metric_ci.get("macro") or {}
        sample_meta = metric_ci.get("sample", {}) or {}

        def _ci_val(idx: str, field: str):
            src = ci_macro if idx == _MACRO_LABEL else ci_items.get(idx, {})
            return src.get(field)

        for col, field in (("AP(抽樣)", "ap"), ("CI 2.5%", "ci_low"),
                           ("CI 97.5%", "ci_high")):
            map_tbl[col] = [_ci_val(idx, field) for idx in map_tbl.index]
        description_extra = (
            f"AP(抽樣)/CI 欄為抽樣估計（{sample_meta.get('n_queries_sampled')} "
            f"個正例 query、bootstrap n_boot={metric_ci.get('n_boot')}，"
            f"cluster=客戶），非全量值；點估計以全量欄 map_attr 為準。"
        )

    tables = [map_tbl, ndcg_tbl]
    table_titles = ["per-item map_attr@k", "per-item ndcg_attr@k"]
    observation_items = metrics.get("observation_items", []) or []
    if observation_items:
        per_item = metrics.get("per_item", {})
        obs_tbl = pd.DataFrame(
            {"n_pos": [per_item.get(it, {}).get("n_pos") for it in observation_items]},
            index=observation_items,
        )
        tables.append(obs_tbl)
        table_titles.append("觀察名單（n_pos < min_positives，已移出 macro）")
```

`return ReportSection(...)` 改用上面的 `tables` / `table_titles`，`description` 尾端串上 `description_extra`（空字串時無感）。

`build_primary_map_section(metrics, parameters, metric_ci=None)`：`return` 之前加：

```python
    tables = [table]
    table_titles = ["per-query 指標 @k"]
    if metric_ci and metric_ci.get("enabled") and metric_ci.get("macro"):
        m = metric_ci["macro"]
        sample_meta = metric_ci.get("sample", {}) or {}
        ci_tbl = pd.DataFrame(
            [{"AP(抽樣)": m.get("ap"), "CI 2.5%": m.get("ci_low"),
              "CI 97.5%": m.get("ci_high"),
              "樣本 query 數": sample_meta.get("n_queries_sampled")}],
            index=["macro per-item mAP"],
        )
        tables.append(ci_tbl)
        table_titles.append("macro per-item mAP 的 CI（抽樣估計）")
```

`assemble_report` 簽名加 `metric_ci: dict | None = None`，並把兩個 builder 呼叫改為傳 `metric_ci=metric_ci`。

`nodes_spark.py::generate_report` 尾端 `assemble_report(...)` 呼叫加 `metric_ci=metric_ci`。

- [ ] **Step 4: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/evaluation/report_builder.py src/recsys_tfb/pipelines/evaluation/nodes_spark.py \
  tests/test_evaluation/test_report_builder.py && \
git commit -m "feat(report): per-item AP CI 欄＋macro CI 表＋min_positives 觀察名單"
```

---

### Task 10：收尾閘門（真跑）＋graphify＋總審

**Files:** 無新程式碼；閘門期間會暫改 `conf/base/parameters_evaluation.yaml`（結束時還原）。

- [ ] **Step 1: 相關測試 vs baseline**（>2 分鐘，背景執行；跑 Task 1 Step 4 同一組檔案）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py tests/test_evaluation/test_metrics_spark.py \
  tests/test_evaluation/test_metrics_spark_orchestrator.py tests/test_evaluation/test_metrics_spark_slim.py \
  tests/test_evaluation/test_metrics_spark_category.py tests/test_evaluation/test_report_builder.py \
  tests/test_evaluation/test_parameters_evaluation_yaml.py tests/test_core/test_consistency.py \
  tests/test_pipelines/test_evaluation tests/test_diagnosis \
  -q 2>&1 | tail -15 | tee /tmp/phase1_test_after.txt && diff /tmp/phase1_test_baseline.txt /tmp/phase1_test_after.txt || true
```
Expected: pass 數增加（新測試）、fail 集合與 baseline 完全相同（理想都是 0）。

- [ ] **Step 2: 真跑 evaluation（改動後）**（>2 分鐘，背景執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version $(cat /tmp/phase1_mv.txt)
```
Expected: 成功；node 順序中出現 `compute_metric_ci`。

- [ ] **Step 3: 檢視產物（spec 驗收 3）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && MV=$(cat /tmp/phase1_mv.txt) && \
ls data/evaluation/$MV/20260131/diagnosis/metric_ci.json && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
import json
ci = json.load(open('data/evaluation/$MV/20260131/diagnosis/metric_ci.json'))
assert ci['enabled'] and ci['sample']['n_queries_sampled'] >= 1, ci.get('sample')
assert ci['macro'] and ci['macro']['ci_low'] <= ci['macro']['ap'] <= ci['macro']['ci_high']
print('items:', {k: v['n_pos'] for k, v in ci['per_item'].items()})
print('macro:', ci['macro']); print('sample:', ci['sample'])
" && grep -c "CI 2.5%" data/evaluation/$MV/20260131/report.html
```
Expected: JSON 含 per_item/macro/sample metadata；report.html 含 CI 欄（grep 計數 ≥ 1）。

- [ ] **Step 4: 已知答案 (a) — 預設參數回歸不變**（比對改動前快照：舊欄位值必須逐字相同，新 CI 欄只能是追加）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && MV=$(cat /tmp/phase1_mv.txt) && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
import re

def macro_rows(path):
    html = open(path, encoding='utf-8').read()
    rows = re.findall(r'<th>Macro 平均</th>(.*?)</tr>', html, re.S)
    return [re.findall(r'<td>([^<]*)</td>', r) for r in rows]

before = macro_rows('/tmp/phase1_report_before.html')
after = macro_rows('data/evaluation/$MV/20260131/report.html')
assert len(before) == len(after), (len(before), len(after))
for i, (b, a) in enumerate(zip(before, after)):
    assert a[:len(b)] == b, f'row {i}: {b} vs {a[:len(b)]}'
print(f'OK: {len(before)} 條 Macro 平均列，舊欄位值全部相同（新欄只有追加）')
"
```
Expected: `OK: ... 全部相同`。任何不同＝預設參數改變了行為，**停下回報**，不要調數字硬過。

- [ ] **Step 5: 已知答案 (b) — min_positives 把最冷 item 移入觀察名單**

先從 metric_ci.json 找最冷 item 與門檻（最冷 item 若在 take_all_items 內，樣本 n_pos＝全量 n_pos，門檻取其 n_pos＋1；並確認第二冷 item 的 n_pos 嚴格大於門檻）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && MV=$(cat /tmp/phase1_mv.txt) && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
import json
ci = json.load(open('data/evaluation/$MV/20260131/diagnosis/metric_ci.json'))
np = sorted((v['n_pos'], k) for k, v in ci['per_item'].items())
coldest_n, coldest = np[0]; second_n = np[1][0]
thr = coldest_n + 1
assert second_n >= thr, f'第二冷 {np[1]} 與最冷 {np[0]} 太近，改用 thr={second_n} 以下'
print(f'coldest={coldest} n_pos={coldest_n} → min_positives={thr}')
"
```

把印出的門檻寫進 config、依 CLAUDE.md pre-flight 驗證讀到新值、重跑 evaluation（背景）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
sed -i '' 's/^    min_positives: 0$/    min_positives: <上一步印出的門檻>/' conf/base/parameters_evaluation.yaml && \
grep -n "min_positives:" conf/base/parameters_evaluation.yaml
# 確認印出的是 worktree 的新值後：
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb evaluation --env local --post-training --model-version $(cat /tmp/phase1_mv.txt)
```

驗證觀察名單與 macro 排除，然後**還原 config**：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && MV=$(cat /tmp/phase1_mv.txt) && \
grep -o "觀察名單[^<]*" data/evaluation/$MV/20260131/report.html | head -2 && \
grep -c "<th><最冷 item 名></th>" data/evaluation/$MV/20260131/report.html && \
git checkout conf/base/parameters_evaluation.yaml && git status --short
```
Expected: report.html 出現「觀察名單」表、最冷 item 在其中；還原後 `git status` 乾淨。（macro 值變動可由 Step 4 的 macro_rows 函式對 Step 4 的 after 檔比對確認不同——該 item 已不在等權平均內。）

- [ ] **Step 6: graphify rebuild ＋收尾 commit（若有未提交殘留）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))" && \
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework status --short
```
Expected: graphify 重建完成；working tree 乾淨（或只剩 graphify-out 的未 tracked 輸出）。

- [ ] **Step 7: opus 總審**（controller 派 fresh-context reviewer：只給 spec §2＋§3 Phase 1、本計畫、`git diff <Task1 前的 HEAD>..HEAD`、測試輸出與真跑產物路徑；要求列至少 3 個具體問題或逐項列出檢查面向；verdict PASS/FAIL）

- [ ] **Step 8: 回報使用者閘門**：附 metric_ci.json 摘要、report.html 路徑（CI 欄＋觀察名單截圖或段落）、回歸比對結果、測試 vs baseline。**等使用者檢視通過才進 Phase 2。**

---

## Self-review（計畫作者已核）

- Spec 覆蓋：§3 Phase 1 五個條目——metrics.py 參數家族（Task 3）、metrics_spark n_pos 前置缺口＋參數＋parity（Task 2/4）、uncertainty.py＋sample.py（Task 6/7）、report CI 欄＋metric_ci.json catalog（Task 8/9）、config＋A15（Task 5）；驗收 1–4 ——真跑（Task 1 已有模型免重訓＋Task 10 Step 2）、檢視（Step 3）、回歸不變（Step 4）、觀察名單注入（Step 5）。§2 抽樣底座全數落在 Task 6。
- 佔位符：Task 8 pipeline 結構測試與 Task 5 consistency 最小 params 兩處刻意寫「以現檔既有式樣為準」——這是對既有測試檔的適配指示，不是內容缺口；其餘步驟皆含完整程式碼與預期輸出。
- 型別／識別字一致性：`macro_from_per_item` / `positive_row_contributions` / `_N_POS_KEY` / `observation_items` / `evaluation_metric_ci` / `compute_metric_ci` 在各 task 間逐字一致；CI dict 鍵（`ap`/`ci_low`/`ci_high`/`n_pos`/`macro`/`sample`）在 Task 7/8/9 fixture 間一致。
