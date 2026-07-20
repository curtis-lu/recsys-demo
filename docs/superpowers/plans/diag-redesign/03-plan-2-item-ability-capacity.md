# Plan 2：item_ability 與 model_capacity（診斷重構 3/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 實作第二、三項診斷：模型能不能在 query 內分辨 item（含 raw vs query-centered AUC 對照），以及模型的 gain/split 花在 item 身分還是 context 特徵上。

**Architecture:** 兩項合成一份計畫，因為 `model_capacity` 吃 `item_ability` 的輸出畫 capacity vs ability 散點。`item_ability` 含 sort-once bootstrap 最佳化；`model_capacity` 只讀 `gain_ledger.json`，不碰評測資料。同時刪掉 `discrimination.py`（同一統計量的 Spark 版，用的是校準後分數）。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 1 已完成並 merge（契約與樣板已定案、且你已對樣板形狀給過回饋）。

> ✅ **接線層重構（Plan 1.5）已完成**（`02b-plan-1.5-wiring.md`，7 個 task，`6e2138d`）。本檔的 Phase 3／4 已於 2026-07-20 依重構後的形狀改寫，**以本檔現在的內容為準**；下面三條是與原稿的差異，讀過舊版的人要注意：
>
> 1. **不再手寫診斷 Node**——Node 由 `contract.DIAGNOSES` 經 `make_diagnosis_node` 導出。新增一項診斷只動三處：`DIAGNOSES` 一行、`catalog.yaml` 一條 JSONDataset entry、子套件本身。**`pipeline.py` 與 `generate_report` 都不必動。**
> 2. **不再往 `generate_report` 的 inputs 加位置元素**——診斷頁由 `render_diagnosis_pages` 按**檔名**讀 JSON 產生。
> 3. 檔名是 `_compute.py`／`_render.py`（**前綴底線**），不是原稿寫的 `compute.py`／`render.py`。理由：`from .compute import compute` 會把子模組名重綁成函式，而 `contract.check_module` 走 `getattr` 剛好拿到函式、**抓不到這個遮蔽**。
>
> `_render.py` 的版面形狀（多 section、每節 formula ＋ ≤3 則 ≤160 字 bullets ＋ 自己的圖）**照抄 `config_shift`**——那部分使用者已驗收通過。

## 本計畫的三個新增任務（原稿沒有，2026-07-20 補）

| Task | 為什麼在這裡 |
|---|---|
| **3.3** `_common.py` 抽取 ＋ CI 方向自帶名字 ＋ `q_agg` 權重常數檢查 | README「已裁決延後」表裡標記 **Plan 2 開工時** 的三項。延後理由是「一個實例看不出哪些是共通的」——`item_ability` 上線後就有第二個實例，條件成立 |
| **4.0** `contract.INPUTS` 擴充 | `model_capacity` **不吃 `diagnosis_sample`**，它吃 `gain_ledger` ＋ `evaluation_item_ability`。現行 `make_diagnosis_node` 把 inputs 寫死成 `["diagnosis_sample", "parameters"]`，容不下它 |
| **5** real-run 驗證 | Plan 1.5 的教訓：`report_aggregates.json` 的 int 欄名只有走過真的 `JSONDataset` 寫檔再讀回才驗得到，單元測試碰不到那條路徑 |

**共用脈絡（開工前必讀，本檔不複述）：** `docs/superpowers/plans/diag-redesign/00-shared-context.md`
——五項診斷的邏輯架構與閱讀順序、檔案結構、持久化邊界（§2.7）、共同統計限制（§3.6）、診斷契約（§4）。

---

## 三條鐵則（每份計畫都重貼，不得省）

這次重構的驗收標準跟一般功能不同，**寫錯方向比寫錯程式更貴**。三條鐵則：

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。診斷輸出的是數字、分布、對照點、範圍說明。判斷留給讀者。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別（舊 `quadrant.auc_threshold` 就是被這條判死的）。顏色只編碼資料本身的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，寫出它量的是什麼、算在哪批列上、**不能推論什麼**。`blind_to` 為空即契約違反，有測試擋。

**為什麼**：使用者的原話是「我沒有要把人類的思考與判斷外包給你，我要你做的是忠實呈現數據，但是用一個清楚好懂的邏輯架構來幫助人類判斷，而不是直接給結論」。既有的 `triage.py` 正是被否決的那種東西——它已經實作了「per-item 判定＋槓桿建議」，所以它必須死，不是因為寫得不好。

---


---

## 環境前置

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # 應為 Python 3.10.9
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```

**測試一律用絕對 venv python ＋ `PYTHONPATH=src`**，裸跑會抓到 main 的 src 而靜默測錯 code：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <path> -v
```

**先建立 baseline**（main 上有既知 failing／互擾測試，清單見 `docs/operations/known-pitfalls.md` §5）：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  -q 2>&1 | tail -20 > /tmp/baseline.txt
```

### 本機 real-run（第一次要先建環境）

worktree 的 `data/` 樹是空的（鐵則 R3：每個 worktree 用自己的真 `data/`，不 symlink 到 main）。第一次要建完整鏈：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
V=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
PYTHONPATH=src $V scripts/local_spark_setup.py --reset
PYTHONPATH=src $V -m recsys_tfb dataset  --env local
PYTHONPATH=src $V -m recsys_tfb training --env local      # 印出 model_version，記下來
PYTHONPATH=src $V -m recsys_tfb evaluation --env local --post-training --model-version <mv>
```

**兩個旗標都是必要的，少一個就跑不動**（2026-07-19 實測）：

- `--post-training`：預設模式讀 inference 產出的 `ranked_predictions`，而本機 inference 撞既有 issue #63（`scripts/local_e2e.sh:6-9` 明寫本機 e2e 只收斂到 training）。加了它改讀 training 自己產出的 `training_eval_predictions`（見 `pipelines/evaluation/pipeline.py:74` 的三元式）。
- `--model-version <mv>`：不指定會解析成 `data/models/best` symlink，而那個 symlink 要 promote 才有。**promote 是使用者保留的人工步驟，實作者不得自行執行**（CLAUDE.md 不變量）。用上一步 training 印出的 model_version 代入。


---


---

## Phase 3：`item_ability`

### Task 3.1: 計算層（含 sort-once bootstrap 最佳化）

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/uncertainty.py`（抽出 `iter_stratified_cluster_multipliers`）
- Create: `src/recsys_tfb/diagnosis/metric/item_ability/{__init__.py,_compute.py}`
- Delete: `src/recsys_tfb/diagnosis/metric/discrimination.py`
- Delete: `tests/test_diagnosis/test_metric/test_discrimination.py`
- Test: `tests/test_diagnosis/test_metric/test_uncertainty_draws.py`、`tests/test_diagnosis/test_metric/test_item_ability.py`

> **原稿的一個缺陷，已修正。** 原稿把簽章寫成 `weighted_auc_presorted(labels, weights)`（兩參數），同時要求 `test_weighted_auc_handles_ties_with_midrank` 對 `[1.0, 1.0]` 這組**同分**輸入回 0.5。兩者不相容：只給 label 與 weight，函式無從知道哪些列同分——`[1, 0]` 在「全部相異」的假設下 AUC ＝ 1.0，不是 0.5。**同分邊界必須當成參數傳進去。** 下面的簽章是三參數版本。

#### Step 1a：先抽出重抽骨架（`uncertainty.py`）

`item_ability` 的 AUC CI 是本 repo 第二個需要「分層 cluster 重抽」的地方（第一個是 `paired_bootstrap_delta`）。**不准寫第二份重抽迴圈**——把既有那份的抽籤骨架抽出來共用。

新函式（放 `uncertainty.py`，緊接 `_row_offsets` 之後）：

```python
def iter_stratified_cluster_multipliers(
    clusters: np.ndarray,
    strata: np.ndarray,
    n_boot: int,
    seed: int,
) -> Iterator[np.ndarray]:
    """逐個 replicate 產生「每列的重抽乘數」向量（長度＝len(clusters)）。

    重抽單位 ＝ ``(stratum, cluster)``，層內獨立重抽且各層單位數維持原值——
    理由與取捨完整寫在 :func:`paired_bootstrap_delta` 的 docstring，這裡不複述。

    **為什麼是 generator 而不是回一個 (n_boot, n_rows) 矩陣**：公司規模是
    ≈25 萬 query × 22 item ≈ 5.5M 列，200 次重抽的矩陣 ＝ 8.8GB。generator
    讓記憶體停在 O(n_rows)，而且 RNG 呼叫序列與原本的逐 replicate 迴圈完全
    一致（抽籤與統計量計算本來就不交錯），所以既有呼叫端的輸出逐位元不變。

    呼叫端自己乘上 ``inclusion_weight``：本函式只管抽籤，不管 HT 權重——
    兩者混在一起的話，「權重錯了」與「抽籤錯了」在輸出上分不出來。
    """
```

`paired_bootstrap_delta` 改用它（**行為必須逐位元不變**），`item_ability` 也用它。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_uncertainty_draws.py
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.uncertainty import (
    iter_stratified_cluster_multipliers, paired_bootstrap_delta,
)


def _frame(n=60):
    rng = np.random.default_rng(0)
    return pd.DataFrame({
        "group": np.repeat(np.arange(n // 2), 2),
        "cluster": np.repeat([f"e{i}" for i in range(n // 2)], 2),
        "item": np.tile(["a", "b"], n // 2),
        "label": rng.integers(0, 2, n),
        "score": rng.random(n),
        "stratum": np.repeat(["take_all", "hash_ratio"], n // 2),
        "inclusion_weight": np.repeat([1.0, 4.0], n // 2),
    })


def test_multipliers_stay_within_stratum():
    """層內重抽：某一層的乘數總和必須恆等於該層的單位數。

    跨層一起抽的話這個總和會隨機漂移——那正是 paired_bootstrap_delta
    docstring 第 2 點要避免的事。
    """
    f = _frame()
    clusters = pd.factorize(f["cluster"])[0]
    strata = f["stratum"].to_numpy()
    n_rows_per_stratum = {s: int((strata == s).sum()) for s in set(strata)}
    for mult in iter_stratified_cluster_multipliers(clusters, strata, 20, 7):
        for s, n_rows in n_rows_per_stratum.items():
            sel = strata == s
            # 每列一個乘數，同層的乘數總和 ＝ 該層列數（每個單位被抽到的
            # 次數總和 ＝ 單位數，而每個單位在此 fixture 各對應 2 列）
            assert mult[sel].sum() == pytest.approx(n_rows)


def test_refactor_leaves_paired_bootstrap_bit_identical():
    """抽骨架不准改數字。基準值由重構**前**的實作在同一 fixture 上跑出來，
    抄進這裡當黃金值——不是事後拿新實作的輸出回填。
    """
    f = _frame()
    mp = {"k": None, "weight_alpha": 0.0, "min_positives": 0, "shrinkage_k": 0.0}
    lo, hi = paired_bootstrap_delta(f, mp, {"a": 0.3}, n_boot=50, seed=42)
    assert (lo, hi) == (GOLDEN_LO, GOLDEN_HI)  # ← 實作者填入重構前實測值
```

```python
# tests/test_diagnosis/test_metric/test_item_ability.py
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.item_ability._compute import (
    compute, presort_by_score, weighted_auc_presorted,
)


def test_weighted_auc_matches_hand_computed_value():
    # 分數 [3,1,2]，label [1,0,1]：唯一的負例分數 1 排最後
    # → 兩個正例都贏過它 → AUC = 1.0
    score = np.array([3.0, 1.0, 2.0])
    order, tie_starts = presort_by_score(score)
    labels = np.array([1, 0, 1])[order]
    weights = np.ones(3)
    assert weighted_auc_presorted(labels, weights, tie_starts) == pytest.approx(1.0)


def test_weighted_auc_handles_ties_with_half_credit():
    """同分給 0.5 分。tie_starts 是**必要**參數——只給 label 與 weight
    無從分辨「同分」與「正例贏」，兩者的 AUC 分別是 0.5 與 1.0。
    """
    score = np.array([1.0, 1.0])
    order, tie_starts = presort_by_score(score)
    labels = np.array([1, 0])[order]
    assert weighted_auc_presorted(labels, np.ones(2), tie_starts) == pytest.approx(0.5)


def test_ties_and_distinct_scores_differ():
    """反向釘住上一條：把同分拆開，同一組 label 的 AUC 必須改變。
    少了這條，一個忽略 tie_starts 的實作也能讓上面兩條同時綠
    （只要它剛好回 0.5 …… 不會，但斷言之間互相印證比較穩）。
    """
    labels = np.array([1, 0])
    tied_order, tied_starts = presort_by_score(np.array([1.0, 1.0]))
    dist_order, dist_starts = presort_by_score(np.array([2.0, 1.0]))
    tied = weighted_auc_presorted(labels[tied_order], np.ones(2), tied_starts)
    dist = weighted_auc_presorted(labels[dist_order], np.ones(2), dist_starts)
    assert tied != dist


def test_bootstrap_sorts_once_per_item_regardless_of_n_boot(monkeypatch):
    """效能契約：排序次數 ＝ item 數，與 n_boot 無關。

    腳本原版每次 weighted_auc 呼叫都重排（N_items × (n_boot+2) 次排序）。

    這裡數的是本模組自己的 presort_by_score，不是 np.argsort——np.argsort
    連 pandas 內部的排序都會數進來，得到的數字既不穩定也指不出是誰在排。
    斷言用「等於 item 數」而不只是「與 n_boot 無關」：後者對一個
    「每個 item 排兩次」的實作照樣成立。
    """
    import recsys_tfb.diagnosis.metric.item_ability._compute as m

    calls = {"n": 0}
    real = m.presort_by_score
    monkeypatch.setattr(
        m, "presort_by_score",
        lambda *a, **k: (calls.__setitem__("n", calls["n"] + 1), real(*a, **k))[1],
    )

    sample = _sample()
    n_items = sample["prod_name"].nunique()
    compute((sample, {"n_queries": 40}), _params(n_boot=5))
    few = calls["n"]
    calls["n"] = 0
    compute((sample, {"n_queries": 40}), _params(n_boot=200))
    many = calls["n"]
    assert few == many, f"排序次數隨 n_boot 增長：{few} → {many}"
    # raw 與 query-centered 各排一次 → 每個 item 兩次
    assert many == 2 * n_items, f"預期 {2 * n_items} 次排序，實得 {many}"


def test_reports_both_raw_and_query_centered_auc():
    out = compute((_sample(), {"n_queries": 40}), _params())
    item = out["per_item"][0]
    assert "raw_within_item_auc" in item
    assert "query_centered_auc" in item
    assert "auc_gap_raw_minus_centered" in item


def test_auc_gap_is_raw_minus_centered_not_absolute():
    """方向釘死。取絕對值或反號都不會讓任何數值測試轉紅——散點圖偏離
    對角線的**方向**是這項診斷的全部意義，反了就讀反了。
    """
    out = compute((_sample(), {"n_queries": 40}), _params())
    for r in out["per_item"]:
        if r["raw_within_item_auc"] is None or r["query_centered_auc"] is None:
            continue
        assert r["auc_gap_raw_minus_centered"] == pytest.approx(
            r["raw_within_item_auc"] - r["query_centered_auc"]
        )


def test_inclusion_weight_changes_the_auc():
    """HT 權重必須真的餵進 AUC。把某一層的權重從 1 改成 8，AUC 應該改變；
    若實作忘了乘權重，兩次結果會完全相同。
    """
    base = _sample()
    base["stratum"] = "take_all"
    base["inclusion_weight"] = 1.0
    heavy = base.copy()
    heavy.loc[heavy["cust_id"] < "c20", "inclusion_weight"] = 8.0
    a = compute((base, {"n_queries": 40}), _params(n_boot=0))
    b = compute((heavy, {"n_queries": 40}), _params(n_boot=0))
    assert a["per_item"][0]["raw_within_item_auc"] != \
        b["per_item"][0]["raw_within_item_auc"]


def test_requires_uncalibrated_score():
    sample = _sample().drop(columns=["score_uncalibrated"])
    with pytest.raises(ValueError, match="score_uncalibrated"):
        compute((sample, {"n_queries": 40}), _params())


def test_disabled_returns_stub_with_same_key_set():
    """三條 return 路徑（停用／空樣本／完整）key set 必須相同——
    照抄 config_shift 的契約（見 config_shift/_compute.py::compute docstring）。
    """
    full = compute((_sample(), {"n_queries": 40}), _params())
    p = _params()
    p["evaluation"]["diagnosis"]["item_ability"]["enabled"] = False
    stub = compute((_sample(), {"n_queries": 40}), p)
    empty = compute((_sample().iloc[0:0], {"n_queries": 0}), _params())
    assert set(stub) == set(full) == set(empty)
    assert stub["enabled"] is False


def _params(n_boot=20):
    return {
        "schema": {"time": "snap_date", "entity": ["cust_id"],
                   "item": "prod_name", "label": "label", "score": "score"},
        "evaluation": {"diagnosis": {"ci": {"enabled": True, "n_boot": n_boot},
                                     "sample": {"seed": 42},
                                     "item_ability": {"enabled": True, "top_n": 30}}},
    }


def _sample():
    rng = np.random.default_rng(1)
    rows = []
    for c in range(40):
        for item in ("ccard_ins", "fund_bond"):
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c:02d}",
                "prod_name": item,
                "label": int(rng.random() < 0.3),
                "score_uncalibrated": float(rng.uniform(0.05, 0.95)),
                "score": 0.5,
                "stratum": "take_all",
                "inclusion_weight": 1.0,
            })
    return pd.DataFrame(rows)
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_item_ability.py \
  tests/test_diagnosis/test_metric/test_uncertainty_draws.py -v
```
Expected:
- `test_item_ability.py` 全部 FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.item_ability'`
- `test_uncertainty_draws.py` FAIL — `ImportError: cannot import name 'iter_stratified_cluster_multipliers'`

**實際訊息與此不同 → 停下回報，不要自行繼續。**

- [ ] **Step 3: 實作**

從 `scripts/item_ability_diagnosis.py` 移植：

| 來源 | 目的地 | 改動 |
|---|---|---|
| `query_center_scores`（`:362-365`） | `_compute.py` | 原樣 |
| `per_item_ap`（`:388-414`） | `_compute.py` | 原樣（與 `suppression_ledger_diagnosis.py:313-339` **逐位元組相同**，Plan 3 抽到 `_common.py` 共用，本 Phase 先放這裡） |
| `rank_percentiles`（`:368-385`） | `_compute.py` | 原樣 |
| `weighted_auc`（`:313-359`） | `_compute.py::weighted_auc_presorted` ＋ `presort_by_score` | **拆成兩個**：`presort_by_score(score) -> (order, tie_starts)` 做排序與同分分組（`tie_starts` ＝ 每個同分組的起始索引，含結尾哨兵）；`weighted_auc_presorted(labels, weights, tie_starts)` 只做那個 while 迴圈的線性掃。**內部不得再排序。** |
| `_bootstrap_item_auc`（`:417-430`） | `_compute.py` | 改用 Step 1a 的 `iter_stratified_cluster_multipliers`；每個 item 的 `(order, tie_starts)` 在迴圈**外**算一次，迴圈內只把重抽乘數乘上 `inclusion_weight` 再重排到 `order` |
| `analyze_items`（`:604-618`） | `_compute.py::compute` | 簽章改成 `compute(diagnosis_sample, parameters)`，`diagnosis_sample` ＝ `(sample_pdf, sample_meta)` tuple |
| load／HTML／CSS 相關 | **不移植** | pipeline 提供輸入，`report/` 負責呈現 |

**照抄 `config_shift/_compute.py` 的四項契約**（那份已通過使用者驗收）：
1. `SCORE_COL = "score_uncalibrated"`，**不設 fallback**，缺欄直接 raise 並在訊息裡說明為什麼不退回 `schema.score`。
2. `FIELD_NOTES`：每個非顯然欄位一句話定義，跟著 JSON 走。
3. 三條 return 路徑（停用／空樣本／完整）**key set 完全相同**，未計算的留 `None`／空容器。
4. 貴的區段用 `log_step` 包起來、per-item 迴圈逐項印進度（公司規模下這段會安靜跑很久）。

`__init__.py` 照抄 `config_shift/__init__.py` 的形狀（re-export `NAME`／`TITLE`／`SCOPE`／`compute`／`render`）。本 task 只有 `compute`，`SCOPE`／`render` 在 Task 3.2——所以本 task **先不要**把 `item_ability` 加進 `DIAGNOSES`，否則契約測試會紅。

同時 `git rm src/recsys_tfb/diagnosis/metric/discrimination.py tests/test_diagnosis/test_metric/test_discrimination.py`——它是同一統計量的 Spark 版，且用的是**校準後**的 `score` 欄，與本套設計的 `score_uncalibrated` 不一致。刪之前先 `grep -rn "discrimination" src/ tests/ conf/` 確認沒有殘留呼叫點。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric -q 2>&1 | tail -10
```
Expected: 全綠（含既有的 `test_uncertainty.py` —— 抽骨架不准改既有數字）。

- [ ] **Step 5: mutation check（三個，逐一驗證再改回）**

| # | 弄壞哪裡 | 預期轉紅 | 為什麼下在這裡 |
|---|---|---|---|
| 1 | `weighted_auc_presorted` 忽略 `tie_starts`（每列自成一組） | `test_weighted_auc_handles_ties_with_half_credit` ＋ `test_ties_and_distinct_scores_differ` | 這是新簽章存在的唯一理由 |
| 2 | `_bootstrap_item_auc` 內把 `presort_by_score` 移進重抽迴圈 | `test_bootstrap_sorts_once_per_item_regardless_of_n_boot` | 這是本 task 效能宣稱的因果鏈上唯一不可省的一步 |
| 3 | 算 AUC 時不乘 `inclusion_weight`（只用重抽乘數） | `test_inclusion_weight_changes_the_auc` | HT 權重漏乘不會讓任何其他測試轉紅，數字照樣印得出來 |

**任一 mutation 全綠 → 停下回報**，那代表對應的測試沒有覆蓋到新路徑。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): item_ability 計算層（sort-once bootstrap，discrimination.py 退場）"
```

### Task 3.2: 呈現層、`SCOPE`、pipeline 接線

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/item_ability/_render.py`
- Modify: `src/recsys_tfb/diagnosis/metric/item_ability/__init__.py`（補 `SCOPE`／`render` 的 re-export）
- Modify: `src/recsys_tfb/diagnosis/metric/contract.py`（`DIAGNOSES` 加一行）
- Modify: `conf/base/catalog.yaml`（加 `evaluation_item_ability`）、`conf/base/parameters_evaluation.yaml`（加 `enabled` ＋ `top_n`）
- Test: `tests/test_diagnosis/test_metric/test_item_ability_render.py`

> **`nodes_spark.py` 與 `pipeline.py` 不在清單裡，這是 Plan 1.5 的交付成果。** Node 由 `DIAGNOSES` 導出、診斷頁按檔名讀，所以新增一項診斷只動三處。**如果你發現非動 `pipeline.py` 不可，那是訊號不是例外——停下回報。**

- [ ] **Step 1: 寫失敗測試**

比照 `test_config_shift_render.py` 的六條（section 型別、停用回 None、SCOPE 有 blind_to、禁判定字眼、契約檢查），另加：

```python
def test_scope_states_auc_is_not_metric_native():
    """這條是誠實條款：AUC 不是 macro mAP 的分解，必須寫在 blind_to。"""
    from recsys_tfb.diagnosis.metric import item_ability
    joined = " ".join(item_ability.SCOPE.blind_to)
    assert "不同 query" in joined
    assert "proxy" in joined or "代理" in joined


def test_scope_warns_auc_not_comparable_externally():
    from recsys_tfb.diagnosis.metric import item_ability
    joined = " ".join(item_ability.SCOPE.blind_to) + \
        item_ability.SCOPE.population
    assert "有正例" in joined
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_item_ability_render.py -v`
Expected: FAIL — 模組缺 `render`／`SCOPE`

- [ ] **Step 3: 實作**

呈現內容：
1. **raw vs centered AUC 散點**：`scatter(x=raw, y=centered, labels=item)`，加 y=x 對角參考線。**這張圖是本項的核心**——偏離對角線的距離就是「客戶活躍度」被誤計入的量。
2. **per-item AUC 條圖含 CI 誤差線**，`fmt_auc`。
3. **AUC 差條圖**：`bar(y=auc_gap_raw_minus_centered, center=0.0)`，發散色階。
4. **正例名次百分位分布**：最低 AP 的前 N 個 item（`top_n` 預設 30）的名次分布條圖。
5. **對照點文字**：隨機打散 = 0.500；「只用 item 全域購買率排序」的 baseline **實跑數值**（不是假設值）。

`SCOPE.blind_to` 必含（逐字寫進程式碼）：
- 「item j 的正例列與負例列分屬**不同 query**，而 macro mAP 從頭到尾沒做過跨 query 的分數比較——這個 AUC 是 proxy，不是指標的分解。」
- 「母體限定在有正例的 query，所以這個數字**不能跟任何外部引用的 AUC 比較**，它會系統性地低於全母體 AUC。」
- 「AUC 高不代表 mAP 高：兩者對名次的加權方式不同。」

**接線只有三處**（Plan 1.5 之後的形狀）：

1. `contract.py` 的 `DIAGNOSES` 加 `"item_ability"`（接在 `"config_shift"` 之後——順序即閱讀順序、也決定 HTML 檔名前綴 `02-item-ability.html`），`test_contract.py` 的 `EXPECTED_ORDER` 同步加。
2. `conf/base/catalog.yaml` 加：
   ```yaml
   evaluation_item_ability:
     type: JSONDataset
     filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/item_ability.json
   ```
   漏了會**靜默**不落地（catalog 自動建 MemoryDataset）→ 有 `test_every_registry_diagnosis_has_a_catalog_entry` 擋。
3. `conf/base/parameters_evaluation.yaml` 的 `evaluation.diagnosis` 底下加 `item_ability: {enabled: true, top_n: 30}`。

**`core/consistency.py` 要不要動？** 先跑 `grep -n "DIAGNOSES" src/recsys_tfb/core/consistency.py` 確認它是走 registry 迴圈還是逐項硬寫。走迴圈 → 零改動；逐項硬寫 → 那是 Plan 1.5 漏掉的一處，補進迴圈並在回報裡明說。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation tests/test_core/test_consistency.py \
  tests/scripts/test_render_diagnosis.py -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: mutation check（兩個）**

| # | 弄壞哪裡 | 預期轉紅 |
|---|---|---|
| 1 | `catalog.yaml` 的 `evaluation_item_ability` 整段註解掉 | `test_every_registry_diagnosis_has_a_catalog_entry` |
| 2 | `SCOPE.blind_to` 拿掉「不同 query」那一條 | `test_scope_states_auc_is_not_metric_native` |

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): item_ability 呈現層＋接線（raw vs centered AUC 對照）"
```

---

### Task 3.3: 清掉三項延後案（`_common.py` 抽取 ＋ CI 方向自帶名字 ＋ `q_agg` 權重假設）

**現在做的理由**：這三項在 README「已裁決延後」表裡都標記 **Plan 2 開工時**，延後理由一致——「一個實例看不出哪些是共通的、哪些是 `config_shift` 特有的」。Task 3.1／3.2 上線後有了第二個實例，條件成立。

**這是純重構 task：外部可觀察行為必須完全不變。** 任何一步發現要「順便」改行為 → 停下回報。

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/_common.py`
- Modify: `src/recsys_tfb/diagnosis/metric/config_shift/_compute.py`、`item_ability/_compute.py`
- Test: `tests/test_diagnosis/test_metric/test_common.py`

- [ ] **Step 1: 先記 baseline**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis -q 2>&1 | tail -5 > /tmp/t33-baseline.txt
```

- [ ] **Step 2: 三項各自的規格**

**(a) `_common.py` 抽取——只抽兩個實例都有、且逐字相同的部分。**

對照 `config_shift/_compute.py:421-426`（`_query_key`）、`:544-548`（groups／clusters／items／y／z）、`:564-568`（`ht_weights`）與 `item_ability/_compute.py` 的對應段落，**逐行比對**再決定抽什麼。候選：

```python
def query_key(pdf: pd.DataFrame, cols: list[str]) -> pd.Series:
    """把多欄併成 ``a|b|c`` 形式的單一 key。"""

def sample_arrays(sample_pdf, schema) -> SampleArrays:
    """診斷抽樣 → (groups, clusters, items, y, ht_weights, row_weights)。

    ``ht_weights`` 缺 ``inclusion_weight`` 欄時是 None（走未加權路徑）；
    ``row_weights`` 是同一組權重的「缺席時填 1」版本，給 n_pos_effective 這種
    一定要有數字的地方用。兩個都給是刻意的：mAP 的 weights 參數用 None 與用
    全 1 是**位元等價**的兩條路，但混用會讓「有沒有加權」在讀碼時看不出來。
    """
```

⚠ **不要為了讓兩邊長得一樣而改任一邊的語意。** 例如 `config_shift` 的 `clusters` 是 `pd.Series`（後面呼叫 `.nunique()`），`uncertainty.bootstrap_per_item_ci` 的 `clusters` 是 `factorize` 後的 int 陣列——這兩個不是同一個東西，硬合併會製造一個沒有人真正需要的中間型別。**只抽真的相同的那些行；不同的留在原地並在 `_common.py` 的 docstring 寫明為什麼沒抽。**

**(b) CI 方向自帶名字。**

現況：`paired_bootstrap_delta` 回的是 `mAP(F) − mAP(F − shift)`，而各診斷的 Δ ＝ `corrected − baseline`，**反號**。`config_shift/_compute.py:709-710` 靠一句註解 ＋ 手動 `-hi`／`-lo` 對調撐著。符號寫反不會讓任何數值測試轉紅（大小完全正確、只有正負相反）。

加在 `_common.py`：

```python
def ci_for_corrected_minus_baseline(
    frame: pd.DataFrame, metric_kwargs: dict, shift, *, n_boot: int, seed: int,
) -> tuple[float, float]:
    """``Δ = corrected − baseline`` 的 [2.5%, 97.5%]。

    名字把方向講完了，所以呼叫端不必記得取負。``paired_bootstrap_delta``
    回的是**反向**的差（``mAP(F) − mAP(F − shift)`` ＝ baseline − corrected），
    取負之後上下界也要對調——這兩步只在這裡做一次。

    為什麼值得一個包裝函式：符號寫反時，delta 與 CI 的**數值全部正確**、
    只有正負相反，沒有任何數值斷言會轉紅。Plan 3–5 還有三項診斷要重複這一步，
    而寫的人不會是照抄的人。
    """
```

`config_shift` 改呼叫它（**數值不變**），並加一條結構性測試：

```python
def test_ci_brackets_the_point_estimate():
    """CI 必須包住點估計。符號寫反時這條會紅——那是唯一抓得到反號的形狀，
    因為 delta 與 CI 的絕對值在反號後全部不變。
    """
    out = compute(...)
    assert out["delta_ci_low"] <= out["delta"] <= out["delta_ci_high"]
```

**(c) `q_agg` 的權重常數假設。**

`config_shift/_compute.py:576-581` 的 `weight=("w", "max")` 假設 `inclusion_weight` 在同一 query 內為常數（對 `draw_diagnosis_sample` 的產出成立——權重由 stratum 決定、stratum 是 query 級屬性），但**沒有測試釘住**。上游若讓同一 query 的列帶不同權重，`max` 會靜默選一個。

修法：`max` 改成先驗證再取值——同一 query 內權重非常數時，**不 raise**（診斷不該因為上游變動就整條 pipeline 死掉），而是取 `max` 並在 `notes` 加一條說明有幾個 query 違反假設。加測試：故意造一個權重在 query 內非常數的樣本，斷言 `notes` 裡出現該訊息（**斷言系統說了什麼，不是斷言副作用沒發生**——見 README 假綠形態 8）。

- [ ] **Step 3: 跑測試，與 baseline 逐字對照**

Run: 同 Step 1 的指令。Expected: 與 `/tmp/t33-baseline.txt` **完全一致**（除了新增的測試數）。貼兩次輸出對照。

- [ ] **Step 4: mutation check**

| # | 弄壞哪裡 | 預期轉紅 |
|---|---|---|
| 1 | `ci_for_corrected_minus_baseline` 拿掉取負（直接回 `paired_bootstrap_delta` 的結果） | `test_ci_brackets_the_point_estimate` |
| 2 | `q_agg` 的非常數檢查整段拿掉 | 新增的 notes 斷言 |

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "refactor(diagnosis): 抽 _common 共用層、CI 方向自帶名字、q_agg 權重假設補檢查"
```

---

## Phase 4：`model_capacity`

### Task 4.0: `contract.INPUTS`——讓一項診斷可以宣告自己吃什麼

**這個 task 存在的理由（先讀完再動手）。**

前四項診斷都吃共用抽樣，所以 `make_diagnosis_node` 把 node inputs 寫死成 `["diagnosis_sample", "parameters"]`（`nodes_spark.py:436-480`）、`contract._SIGNATURES["compute"]` 寫死成 `("diagnosis_sample", "parameters")`。

**`model_capacity` 不吃抽樣。** 它讀訓練側的 `gain_ledger`（`catalog.yaml:236-242`，已標 `optional: true`，那條 entry 的註解就是為了等這一刻寫的）＋ `evaluation_item_ability` 的結果。硬塞進現行形狀有三條路，兩條是錯的：

| 做法 | 為什麼不行 |
|---|---|
| 讓每項診斷都收 `(diagnosis_sample, gain_ledger, item_ability, parameters)`，各自忽略用不到的 | 這正是 Plan 1.5 剛殺掉的東西——寬簽章 ＋ 位置綁定 ＝ 靜默錯位。而且會讓 `item_ability` 變成所有診斷的上游 |
| `model_capacity.compute` 自己按路徑讀 `gain_ledger.json` | 繞過 catalog：`--env` 覆寫失效、`optional` 的缺席處理要重寫一份、路徑邏輯在評估側複製一份訓練側的知識 |
| **每項診斷宣告自己的 `INPUTS`** ✅ | 見下 |

**設計**：`INPUTS` 成為 node inputs 與 `compute` 簽章的**單一真實來源**，契約測試證明兩者對齊。這比現況更嚴——現在兩者各寫一份、靠人對；改完之後結構上不可能不一致。

```python
# contract.py
#: 一項診斷的 node inputs。多數診斷吃共用抽樣，所以有預設值；宣告了
#: ``INPUTS`` 的模組覆寫它。
DEFAULT_INPUTS: tuple[str, ...] = ("diagnosis_sample", "parameters")

def inputs_for(mod) -> tuple[str, ...]:
    """這項診斷的 node inputs（catalog 鍵）。"""

def compute_params_for(mod) -> tuple[str, ...]:
    """由 :func:`inputs_for` 導出的 ``compute`` 參數名。

    catalog 鍵去掉 ``evaluation_`` 前綴即參數名（``evaluation_item_ability``
    → ``item_ability``）——與 ``generate_report`` 的位置對齊檢查同一套慣例
    （見 SYNC 清單 §5 的驗證腳本）。
    """
```

`check_module` 的 `compute` 簽章檢查改成比對 `compute_params_for(mod)`，`render` 維持 `("result", "parameters")` 不變。

**兩條必須成立的不變量**（各要一條測試）：
- `INPUTS` 的**最後一個**元素必須是 `"parameters"`——`make_diagnosis_node` 靠它拿 `enabled` 旗標。
- `INPUTS` 裡除 `"parameters"`／`"diagnosis_sample"` 外的每個名字，都必須在 `catalog.yaml` 裡有 entry（否則 runner 會拿到 MemoryDataset 的 None，而 node 照樣跑得完）。

#### 順帶修掉一個靜默的浪費（2026-07-20 查證，原稿沒寫）

`nodes_spark.py:36-54` 的 `_registry_diagnosis_enabled` 目前把 **`DIAGNOSES` 裡的每一項**都算成共用抽樣的消費者：

```python
return any(bool((diag.get(name, {}) or {}).get("enabled", True)) for name in DIAGNOSES)
```

`model_capacity` 進 registry 之後這句就不再成立——它**不吃 `diagnosis_sample`**。後果：使用者只開 `model_capacity`、關掉其他四項時，`draw_diagnosis_sample_node` 仍會跑一次完整抽樣（公司規模 ≈25 萬 query × 22 item 的 `toPandas()` 收到 driver），而那份樣本沒有任何人讀。

**這不會有任何測試轉紅、也不會有錯誤訊息**——pipeline 只是安靜地慢。正是「多一個手工維護的真實來源」的典型後果。

`INPUTS` 讓它可以用導出的：

```python
def _registry_diagnosis_enabled(parameters: dict) -> bool:
    """有任一**吃共用抽樣**的 registry 診斷啟用嗎。

    判準是 ``"diagnosis_sample" in inputs_for(mod)``，不是「有沒有在
    DIAGNOSES 裡」——``model_capacity`` 只讀 gain_ledger，把它算進來會讓
    「只開 model_capacity」白抽一次全量樣本，而且完全沒有徵兆。
    """
```

必要測試（**斷言系統做了什麼，不是斷言副作用沒發生**）：

```python
def test_sample_not_drawn_when_only_non_sample_diagnoses_enabled():
    """只開 model_capacity 時不得抽樣。

    斷言落在「回傳 None ＋ log 說了 skipping」而不是只看有沒有呼叫 Spark：
    後者被「正確跳過」與「根本沒走到這段」同時滿足。
    """
```

`make_diagnosis_node(name)` 改寫：

```python
def _run(*node_inputs):
    if len(node_inputs) != len(declared):
        raise TypeError(...)          # 剛好個數，少給即爆——不用 *args 吞掉
    parameters = node_inputs[-1]
    ...
    # diagnosis_sample 的 None 守衛只在它真的在 INPUTS 裡時才適用
```

`pipeline.py` 的 registry 迴圈把 `inputs=["diagnosis_sample", "parameters"]` 換成 `inputs=list(inputs_for(import_module(...)))`。

- [ ] **Step 1: 寫失敗測試**（`tests/test_diagnosis/test_metric/test_contract.py` 追加）

```python
def test_default_inputs_apply_when_module_is_silent():
    from recsys_tfb.diagnosis.metric import config_shift, contract
    assert contract.inputs_for(config_shift) == ("diagnosis_sample", "parameters")


def test_declared_inputs_override_the_default():
    mod = types.SimpleNamespace(INPUTS=("gain_ledger", "parameters"))
    from recsys_tfb.diagnosis.metric import contract
    assert contract.inputs_for(mod) == ("gain_ledger", "parameters")


def test_compute_params_strip_the_evaluation_prefix():
    from recsys_tfb.diagnosis.metric import contract
    mod = types.SimpleNamespace(
        INPUTS=("gain_ledger", "evaluation_item_ability", "parameters"))
    assert contract.compute_params_for(mod) == (
        "gain_ledger", "item_ability", "parameters")


def test_signature_mismatch_against_declared_inputs_raises():
    """契約的實際價值：宣告 INPUTS 卻寫錯 compute 簽章 → TypeError。
    match 用 'gain_ledger' 而不是 'contract'——後者會被別條規則的訊息滿足。
    """
    from recsys_tfb.diagnosis.metric import contract
    mod = types.SimpleNamespace(
        NAME="x", TITLE="X", SCOPE=object(),
        INPUTS=("gain_ledger", "parameters"),
        compute=lambda diagnosis_sample, parameters: {},   # ← 沒跟著改
        render=lambda result, parameters: (),
    )
    with pytest.raises(TypeError, match="gain_ledger"):
        contract.check_module(mod)


def test_every_diagnosis_node_input_has_a_catalog_entry():
    """INPUTS 打錯字時 runner 給 MemoryDataset 的 None，node 照樣跑完。"""


def test_parameters_must_be_the_last_input():
    """make_diagnosis_node 靠位置 -1 拿 parameters。"""
```

- [ ] **Step 2: 跑測試確認失敗**

Expected: `AttributeError: module 'recsys_tfb.diagnosis.metric.contract' has no attribute 'inputs_for'`。**實際訊息不同 → 停下回報。**

- [ ] **Step 3–4: 實作 ＋ 全綠**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation -q 2>&1 | tail -10
```
**`config_shift` 與 `item_ability` 的 node inputs 必須完全不變**（它們沒宣告 `INPUTS`，走預設）。用 `--dry-run --list-nodes` 對照改動前後的 node 清單，逐字相同。

- [ ] **Step 5: mutation check**

| # | 弄壞哪裡 | 預期轉紅 |
|---|---|---|
| 1 | `compute_params_for` 不剝 `evaluation_` 前綴 | `test_compute_params_strip_the_evaluation_prefix` ＋ Task 4.1 的契約測試 |
| 2 | `_run` 的個數檢查拿掉、改回 `*args` 直接轉呼叫 | 需要一條「少給一個 input → TypeError」的測試；沒有的話**先補**再繼續 |

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(diagnosis): contract.INPUTS——node inputs 與 compute 簽章的單一真實來源"
```

---

### Task 4.1: 計算層

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/model_capacity/{__init__.py,_compute.py}`
- Test: `tests/test_diagnosis/test_metric/test_model_capacity.py`

宣告 `INPUTS = ("gain_ledger", "evaluation_item_ability", "parameters")`，因此 `compute` 的簽章是 `compute(gain_ledger, item_ability, parameters)`——由 Task 4.0 的契約測試強制。

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_diagnosis/test_metric/test_model_capacity.py
import pytest

from recsys_tfb.diagnosis.metric.model_capacity.compute import compute

LEDGER = {
    "total_gain": 100.0,
    "item_id_gain": 60.0,
    "post_item_context_gain": 30.0,
    "per_item": {"ccard_ins": {"context_gain": 20.0},
                 "fund_bond": {"context_gain": 10.0}},
}
PARAMS = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": True}}}}


def test_gain_shares_sum_to_one():
    out = compute(LEDGER, None, PARAMS)
    s = (out["summary"]["item_id_gain_share"]
         + out["summary"]["context_gain_share"]
         + out["summary"]["unaccounted_gain_share"])
    assert s == pytest.approx(1.0, abs=1e-9)


def test_unaccounted_is_residual_not_assumed_zero():
    out = compute(LEDGER, None, PARAMS)
    assert out["summary"]["unaccounted_gain_share"] == pytest.approx(0.10)


def test_degrades_when_gain_ledger_absent():
    out = compute(None, None, PARAMS)
    assert out["enabled"] is True and out["available"] is False
    assert "gain_ledger" in out["reason"]


def test_joins_item_ability_when_model_version_matches():
    ability = {"per_item": [{"item": "ccard_ins", "query_centered_auc": 0.62}]}
    out = compute(LEDGER, ability, PARAMS)
    row = next(r for r in out["per_item"] if r["item"] == "ccard_ins")
    assert row["query_centered_auc"] == pytest.approx(0.62)


def test_missing_ability_leaves_auc_null_without_raising():
    out = compute(LEDGER, None, PARAMS)
    assert all(r.get("query_centered_auc") is None for r in out["per_item"])


def test_disabled_item_ability_stub_is_treated_as_absent():
    """``item_ability.enabled = false`` 時上游落地的是 ``{"enabled": False}``
    stub，不是 None。這條路徑與「檔案不存在」不同，但結果必須一樣——
    少了它，stub 會走進 ``.get("per_item", [])`` 拿到空 list 而**靜默**產出
    一張沒有任何點的散點圖。
    """
    out = compute(LEDGER, {"enabled": False}, PARAMS)
    assert all(r.get("query_centered_auc") is None for r in out["per_item"])
    assert any("item_ability" in n for n in out["notes"])
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_model_capacity.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 實作**

從 `scripts/model_capacity_diagnosis.py` 移植 `summarize`（`:280-436`）為 `compute(gain_ledger, item_ability, parameters)`（檔名 `_compute.py`，前綴底線）。

**關鍵改動**：腳本從檔案路徑讀 `item_ability.json` 並比對 `model_version`（`:97-109`、`:707`）；在 pipeline 裡改成**明確的 node input**，不再讀檔、不再需要版本比對——DAG 保證兩者同一次執行。

`parse_lightgbm_total_split_count`（`:48-65`，手動文字解析 model.txt）**不移植**：split 數應該從 `gain_ledger.json` 取，若 ledger 沒有這個欄位，在 `diagnosis/model/gain_ledger.py` 補上，不要在評估側重新解析模型檔。

> 為什麼：評估側解析訓練產出的 model.txt 是跨層讀內部格式，違反 `diagnosis/__init__.py:1-12` 宣告的依賴方向。

`gain_ledger` 缺席時回 `{"enabled": True, "available": False, "reason": "訓練側未產出 gain_ledger.json（catalog optional）"}`。

- [ ] **Step 4: 跑測試確認通過**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_model_capacity.py -v`
Expected: PASS（5 passed）

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): model_capacity 計算層（gain 三分＋item_ability 併入，不再讀 model.txt）"
```

### Task 4.2: 呈現層與接線

- [ ] **Step 1: 寫失敗測試**

比照前例六條，另加：

```python
def test_capacity_vs_ability_scatter_present_when_ability_given():
    from recsys_tfb.diagnosis.metric import model_capacity
    section = model_capacity.render(RESULT_WITH_ABILITY, {})
    assert len(section.figures) >= 2, "必須含 gain 分配條圖與 capacity vs ability 散點"


def test_unavailable_result_renders_reason_not_blank():
    from recsys_tfb.diagnosis.metric import model_capacity
    section = model_capacity.render(
        {"enabled": True, "available": False, "reason": "訓練側未產出 gain_ledger.json"}, {})
    assert section is not None
    assert "gain_ledger" in section.description
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_model_capacity_render.py -v`
Expected: FAIL — 缺 `render`

- [ ] **Step 3: 實作**

呈現內容：
1. **Gain 三分堆疊條圖**：Item Prior／Post-Item Context／未分配。
2. **per-item context gain 分配條圖**（排序後）。
3. **capacity vs ability 散點**：x = 該 item 分到的 context gain 份額、y = 該 item 的 query-centered AUC，`labels=item`。`item_ability` 缺席時略過此圖並在文字說明原因。

`SCOPE.blind_to` 必含：
- 「Gain 是**訓練期**的分裂增益，不是評測期的貢獻——gain 高不代表在這份評估資料上排得好。」
- 「未分配（Pre-Item）那塊是 item 分裂**之前**的分裂，無法歸給任何單一 item；它不是誤差。」
- 「這一項不碰評測資料，所以它跟其他四項的樣本規模無關，也不受診斷抽樣影響。」

**接線**（照 Task 3.2 的三處，加上 `INPUTS`）：

1. `contract.py` 的 `DIAGNOSES` 加 `"model_capacity"`（第三順位），`test_contract.py` 的 `EXPECTED_ORDER` 同步加。
2. `catalog.yaml` 加 `evaluation_model_capacity` → `.../diagnosis/model_capacity.json`。
3. `parameters_evaluation.yaml` 加 `model_capacity: {enabled: true}`。
4. 模組宣告 `INPUTS = ("gain_ledger", "evaluation_item_ability", "parameters")`。**`pipeline.py` 仍然不必動**——Task 4.0 讓 registry 迴圈自己讀 `INPUTS`。

`gain_ledger` 是**跨 pipeline** 的 optional 產物（訓練側寫 `data/models/${model_version}/diagnostics/gain_ledger.json`，`optional: true` ⇒ 檔案不存在時 `load()` 回 `None` 而非 raise）。所以 evaluation 單獨跑時它可能是 `None`，這是**正常路徑**不是錯誤——Task 4.1 的 `test_degrades_when_gain_ledger_absent` 守著。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation \
  tests/test_core/test_consistency.py tests/scripts/test_render_diagnosis.py \
  -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 5: 檢查 node 拓撲順序（實跑取得，不要用宣告順序推）**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
for i, n in enumerate(create_pipeline().nodes):
    print(i, n.name, n.inputs)
"
```
必須看到 `diagnose_model_capacity` 排在 `diagnose_item_ability` **之後**（它吃後者的輸出）。順序錯 → 拓撲排序沒吃到那條邊，回頭查 `INPUTS` 是否真的進了 `node.inputs`。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(diagnosis): model_capacity 呈現層＋接線（capacity vs ability 散點）"
```

---

## Task 5: real-run 驗證與收尾

**為什麼一定要 real-run**：Plan 1.5 的教訓——`report_aggregates.json` 的 int rank 欄名只有走過真的 `JSONDataset` 寫檔再讀回才驗得到，單元測試完全碰不到那條路徑（`list(matrix.columns)` 變成字串時圖照畫，只有軸標籤不同）。本 Plan 有兩個同類風險：`gain_ledger` 的 optional 缺席路徑、`INPUTS` 導出的 node 接線。

- [ ] **Step 1: 跑 evaluation**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
  -m recsys_tfb evaluation --env local --post-training --model-version 6059dcef
```
（背景執行；本機環境已建好，`model_version` 見 README。）

- [ ] **Step 2: 逐項確認**

| 檢查 | 期望 |
|---|---|
| node 數與順序 | 14 個（12 ＋ 兩項新診斷），`diagnose_model_capacity` 在 `diagnose_item_ability` 之後 |
| `diagnosis/02-item-ability.html` | 存在，含 raw vs centered 散點與 y=x 對角線 |
| `diagnosis/03-model-capacity.html` | 存在。`gain_ledger` 缺席時顯示**原因文字**而非空白頁 |
| `diagnosis/index.html` | 三項都列出、編號連續 |
| `diagnosis/item_ability.json` | 通過**嚴格** JSON 解析（`json.loads`，無 `NaN` 字面值） |
| `report.html` | 與 Task 3.1 之前**逐位元相同**（本 Plan 不動主報表） |
| 離線重繪 | `scripts/render_diagnosis.py --input-dir <diagnosis 目錄> --output-dir /tmp/rd` 兩秒內產出三頁 |

- [ ] **Step 3: 更新文件**

- `README.md` 的進度表（Plan 2 ✅）、假綠形態清單（本 Plan 新發現的）、「已裁決延後」表（劃掉 Task 3.3 清掉的三項）。
- 公司環境同步清單：新增一份 `SYNC-6e2138d-to-<head>.md`，格式照 `SYNC-4bfaeb8-to-6e2138d.md`（**零刪除零改名**除外——本 Plan 刪了 `discrimination.py`，必須在清單裡明確列出「要刪的檔」，那是手動同步最容易出事的一類）。

---


---

## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境 evaluation，拷回 `diagnosis/` 目錄，看：

1. **`02-item-ability.html` 的 raw vs centered AUC 散點**——偏離對角線的量是否符合你對「客戶活躍度混入」的預期？
2. **`03-model-capacity.html` 的 gain 三分**——item prior 佔多少？未分配那塊多大？
3. **capacity vs ability 散點**——兩個診斷的數字並排看，有沒有讀出東西？沒有的話這張圖可能該換。
4. **`gain_ledger` 有沒有缺席**（catalog `optional: true`）。缺席時頁面應顯示原因而非空白。

**看完給回饋之後**：這兩項的 `blind_to` 寫得對不對特別重要——within-item AUC 不是指標原生的量，若說明不夠清楚，讀者會拿它當 mAP 的分解來用。
