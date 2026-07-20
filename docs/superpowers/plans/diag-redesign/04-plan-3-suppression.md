# Plan 3：suppression 壓制帳本與交叉購買（診斷重構 4/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 實作第四項診斷：哪些 label=0 的 item 常排在 label=1 之前、造成多少 AP 缺口，以及這些組合在資料上本來就多常一起買。

**Architecture:** 移植壓制帳本並把最內層的純 Python 逐 pair 迴圈向量化；併入舊 `cross_purchase.py` 的能力，但改成泡泡格圖（顏色＝lift、大小＝共買客戶數），與壓制矩陣同軸序以便並排對照。同時刪掉 `pair_ledger.py` 與 `cross_purchase.py`。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2（僅抽樣階段）、pandas 1.5.3、numpy、plotly、Optuna 4.5.0、pytest。

**前置：** Plan 2 已完成並 merge。

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

## Phase 5：`suppression`（含交叉購買）

> **本節於 2026-07-20 深夜依實際程式碼重寫。** 原稿寫於 Plan 2 開工之前，有六處識別字或設計已與現況不符；每一處都在下面的「原稿修正紀錄」列出，連同「憑什麼判定原稿錯了」。**這是 Plan 2 記下的教訓的直接應用：驗收條件裡的字串一律先查證再寫。**

### 原稿修正紀錄（讀計畫的人先看這段，才知道哪裡跟舊版不一樣）

| # | 原稿寫的 | 實際狀況（證據） | 改法 |
|---|---|---|---|
| 1 | `item_ability/compute.py` | 實際檔名是 `_compute.py`；`item_ability/__init__.py:3-11` 明寫**不得**命名為 `compute.py`（同名子模組會被 `from .compute import compute` 遮蔽，而 `check_module` 走 `getattr` 剛好拿得到函式，契約測試**抓不到**這個遮蔽） | 全節改用 `_compute.py`／`_render.py` |
| 2 | `test_contract.py` 的 `EXPECTED_ORDER` 同步加 | 無此符號。實際是 `tests/test_diagnosis/test_metric/test_contract.py:21-22` 的 `test_registry_is_exactly_the_planned_diagnoses`，裡面是行內 tuple | 改為「更新該測試的行內 tuple」 |
| 3 | A19 改驗 `{enabled, top_examples}` | `consistency.py:589-599`（A15）**已經**對 `DIAGNOSES` 裡的每個 name 驗 `enabled` 是不是真 bool。`suppression` 一進 registry，`enabled` 自動被 A15 蓋住 | A19 只驗 `top_examples`，legend 註明 `enabled` 屬 A15，**避免同一個鍵吐兩條錯誤訊息** |
| 4 | `_allocate_gap` 的效能測試斷言原始碼含 `np.add.at` 或 `np.bincount` | 這是**檢查實作長相而不是行為**的代理指標。真正該向量化的聚合用 pandas `groupby` 表達更清楚，寫成 `np.add.at` 反而更難讀——測試會逼實作往壞的方向走 | 換成兩條真的測到東西的：**慢版參考實作等價比對**（正確性）＋**規模計時**（效能）。理由詳見 Task 5.2 |
| 5 | `cross_purchase_stats(...)` 沿用舊 `cross_purchase.py` 的母體 | 舊版吃的是 **Spark `label_table` 全量**（`cross_purchase.py:21-23` 簽章是 `SparkDataFrame`）；新的 `suppression` 吃的是 `diagnosis_sample`（pandas 抽樣） | **母體改了，這是本 Plan 唯一的語意變更**，必須寫進 `SCOPE.blind_to`。理由詳見 Task 5.2 |
| 6 | 刪除 `pair_ledger`／`cross_purchase` 併在 Task 5.2 | 退場牽動 **11 個檔**（src 4、conf 2、tests 5），與新功能混在一個 commit 會讓公司環境的 `git diff` 無法分開審 | 獨立成 **Task 5.4**，純刪除、單獨 commit |

另有兩處不是錯、但原稿沒講到，補在下面：**圖形點數預算**（Task 5.3）與 **`_common.py` 模組 docstring 的殘留引用**（Task 5.4）。

---

### Task 5.1: 把共用的 `per_item_ap` 抽進 `_common.py`

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/_common.py`
- Modify: `src/recsys_tfb/diagnosis/metric/item_ability/_compute.py`（刪掉本地定義，改 import）
- Test: `tests/test_diagnosis/test_metric/test_common.py`

**為什麼現在抽**：這個函式目前有三份逐位元組相同的副本——`item_ability/_compute.py:164`、`scripts/item_ability_diagnosis.py:388`、`scripts/suppression_ledger_diagnosis.py:313`（後兩者已用 `diff` 驗證零差異）。`suppression` 是第四個消費者。**兩個實例才抽**是本家族的既有判準（見 `_common.py` 模組 docstring），這裡已經有兩個 `src/` 內消費者，門檻到了。

**相依方向沒問題**：`per_item_ap` 只依賴 `positive_row_contributions`／`macro_from_per_item`，兩者都在 `src/recsys_tfb/evaluation/metrics.py:98,189`（不在 diagnosis 套件內），所以 `_common.py` import 它們不會造成 `_common → item_ability` 的反向相依。

- [ ] **Step 1: 寫失敗測試**

追加到 `tests/test_diagnosis/test_metric/test_common.py`：

```python
def test_per_item_ap_available_from_common():
    from recsys_tfb.diagnosis.metric._common import per_item_ap
    assert callable(per_item_ap)


def test_item_ability_reuses_the_shared_per_item_ap():
    """釘住「同一個函式物件」而不是「兩邊算出來一樣」。

    後者對一份被複製貼上的副本照樣成立——而複製品會漂移，這正是本 task
    要消滅的東西。
    """
    import recsys_tfb.diagnosis.metric._common as common
    import recsys_tfb.diagnosis.metric.item_ability._compute as ia
    assert ia.per_item_ap is common.per_item_ap
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_common.py -k per_item_ap -v`

Expected RED（**實際訊息與此不同就停下回報，不要自行繼續**）：
```
ImportError: cannot import name 'per_item_ap' from 'recsys_tfb.diagnosis.metric._common'
```

- [ ] **Step 3: 實作**

把 `item_ability/_compute.py:164-195` 的 `per_item_ap` **整段搬進** `_common.py`（連 docstring 一起，但把「Plan 3 才抽到 `_common.py` 共用，本 Phase 先各自放一份」那句改寫成現況）。`item_ability/_compute.py` 改成從 `_common` import。

`_common.py` 需要新增的 import：`from recsys_tfb.evaluation.metrics import positive_row_contributions, macro_from_per_item`（確認 `item_ability/_compute.py` 原本怎麼寫就怎麼抄，不要自己換寫法）。

- [ ] **Step 4: 跑測試確認通過**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_common.py \
  tests/test_diagnosis/test_metric/test_item_ability.py -q 2>&1 | tail -5
```
Expected: 全綠。

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor(diagnosis): per_item_ap 抽進 _common（第四個消費者出現前先去重）"
```

---

### Task 5.2: `suppression` 計算層（向量化分攤 ＋ lift 交叉購買）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/suppression/__init__.py`（契約五符號）
- Create: `src/recsys_tfb/diagnosis/metric/suppression/_compute.py`
- Test: `tests/test_diagnosis/test_metric/test_suppression.py`

**不動**：`pair_ledger.py`／`cross_purchase.py` 這個 task 一律不碰（Task 5.4 才刪）。**不接線**：`DIAGNOSES` 這個 task 不加（Task 5.3 才加）——`__init__.py` 先建好但 registry 還沒它，契約測試不會掃到它，這是刻意的分段。

#### 移植來源與必做的改動

來源：`scripts/suppression_ledger_diagnosis.py:388-757` 的 `analyze_suppression`。輸出鍵沿用它的 `:727-757`（那份 return dict 是唯一真實來源，**不要照本節的散文重寫鍵名**）。

**改動 1（效能，本 task 的重點）**：`:485-574` 是雙層 Python 迴圈——外層走每個正例列 `b`，內層 `:519` 的 `for a, raw_d, gap_d in zip(above, raw_severity, allocated_gap)` 走每個「排在它上面的負例」。內層的迭代次數 ＝ **成對數**（公司規模估算：25 萬 query × 每 query 約 1–2 個正例 × 平均約 10 個負例排在上面 ≈ 250 萬–500 萬次），這是這項診斷在腳本原版最慢的地方。

**要求：內層迴圈必須消失；外層（逐正例）迴圈可以留。**

為什麼可以留：外層次數 ＝ 正例列數（約 25–50 萬），與 query 迴圈同一個量級，而 query 迴圈 `:464` 本來就在那裡。拿掉內層是 10–20 倍的量級差，拿掉外層是另一個更難的問題（要用 `np.repeat` ＋ ragged-range 建成對索引），**不在本 task 範圍**——但要量出來（見 Step 6），數字難看再另案處理。

建議作法（**這是建議不是規定**，只要通過 Step 5 的等價比對與 Step 6 的計時就算數）：外層每輪把 `above`／`raw_severity`／`allocated_gap`／`score_margin`／正例列號／兩邊的 item 編碼**以陣列形式** append 到 list，迴圈結束後 `np.concatenate` 一次，組成一張扁平的成對表，剩下的統計全部用 pandas `groupby` 一次算完：

- `allocated_ap_gap` 每組（受害 item × 壓制者 item）：`groupby([...])["gap"].sum()`
- `affected_positive_rows`：同一組的**相異正例列數** → `groupby([...])["pos_row"].nunique()`
- `mean_score_margin`／`median_score_margin`：`groupby([...])["margin"].agg(["mean", "median"])`
- per-suppressor 彙總：同一張表改 `groupby("suppressor")`
- `examples` top-K：`nlargest(top_examples, ["allocated_ap_gap", "raw_severity"])`——**取代原版的 `heapq`**（`:557-574`）。heap 是為了逐筆串流才需要的，有整表在手就不必了。

⚠ **記憶體**：成對表的列數 ＝ 上面估的 250 萬–500 萬，乘以 6 欄 float64/int64 ≈ **120–240 MB**。這在 driver 上可以接受，但**必須在 log 印出實際列數**，公司環境跑完才知道估得準不準。用 `logger.info` 印 `n_pairs` 與 `n_positive_rows`。

**改動 2（`cross_purchase_stats`，新函式）**：對每組 `(j, k)` 輸出 `n_joint`（同時買 j 與 k 的 entity 數）、`n_j`、`n_k`、`p_k_given_j = n_joint / n_j`、`lift = p_k_given_j / (n_k / n_entities)`。

**為什麼是 lift 不是裸條件機率**：熱門 item k 對**任何** j 的 `P(k|j)` 都高，只給條件機率的話矩陣會退化成「熱門那幾行整片亮」——那張圖等於在畫 item 的熱門度，不是在畫關聯。lift 把 k 自己的基礎購買率除掉，`lift = 1` ＝ 在這份樣本上兩者近似獨立。

⚠ **母體變了，這是本 Plan 唯一的語意變更（原稿修正 #5）**：舊的 `cross_purchase_matrix` 吃 Spark 的 **`label_table` 全量**；新的 `cross_purchase_stats` 吃 **`diagnosis_sample`**（pandas，抽樣後的）。

- **為什麼改**：兩張圖並排對照是這一項診斷的全部價值（「模型讓 k 壓制 j」對上「買 j 的人本來就常買 k」）。兩張圖算在不同母體上，讀者做的每一次對照都夾帶一個沒被說出口的假設。同源才對得起「並排」這個版面決定。
- **代價**：共買比例會受抽樣影響，且抽樣是**分層**的（`stratum`／`inclusion_weight`），所以樣本內的共買頻率不是母體共買頻率的無偏估計。
- **因此必須寫進 `SCOPE.blind_to`**（Task 5.3），不得只在 docstring 提一句。

`compute` 的輸出另加 `axis_order`：**出現在成對表裡的 item 名稱排序後的清單**。壓制矩陣與交叉購買資料共用同一組順序——兩張圖同軸序才能對照著看。

#### Step 1: 寫失敗測試

`tests/test_diagnosis/test_metric/test_suppression.py`。fixture 形狀**照 `tests/test_diagnosis/test_metric/test_item_ability.py:129-152` 的 `_params`／`_sample` 抄**（那是目前唯一經 real-run 驗證過的形狀，欄位為 `snap_date`／`cust_id`／`prod_name`／`label`／`score_uncalibrated`／`score`／`stratum`／`inclusion_weight`）。

> **Plan 2 最貴的假綠就是 fixture 形狀憑計畫稿捏造**（`model_capacity` 的 ledger fixture 寫扁平鍵、生產端只產巢狀，29 條測試全綠而 production 路徑零覆蓋）。所以：**fixture 的形狀以 `test_item_ability.py` 的實體為準，本計畫稿的描述是待驗證假設。**

```python
import time

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.suppression._compute import (
    compute, cross_purchase_stats,
)


def _params(top_examples=50):
    return {
        "schema": {"time": "snap_date", "entity": ["cust_id"],
                   "item": "prod_name", "label": "label", "score": "score"},
        "evaluation": {"diagnosis": {
            "sample": {"seed": 42},
            "suppression": {"enabled": True, "top_examples": top_examples},
        }},
    }


def _row(cust, item, label, score):
    return {"snap_date": "2026-01-31", "cust_id": cust, "prod_name": item,
            "label": label, "score_uncalibrated": score, "score": 0.5,
            "stratum": "take_all", "inclusion_weight": 1.0}


def test_counts_negatives_ranked_above_each_positive():
    # 一個 query：A=0.9(label 0) 排在 B=0.5(label 1) 之前 → B 被 A 壓制一次
    sample = pd.DataFrame([_row("c1", "A", 0, 0.9), _row("c1", "B", 1, 0.5)])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_suppressed_positive_rows"] == 1
    assert out["mean_negatives_above_positive"] == pytest.approx(1.0)


def test_no_suppression_when_positive_ranks_first():
    """反向釘住上一條。少了它，一個「把每個正例都算成被壓制一次」的
    實作也能讓上面那條綠。"""
    sample = pd.DataFrame([_row("c1", "A", 0, 0.5), _row("c1", "B", 1, 0.9)])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_suppressed_positive_rows"] == 0
    assert out["n_misordered_pairs"] == 0


def test_pair_ledger_attributes_gap_to_the_suppressor():
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    pair = next(p for p in out["pair_ledger"]
                if p["positive_item"] == "B" and p["suppressor_item"] == "A")
    assert pair["allocated_ap_gap"] > 0


def test_allocated_gap_sums_to_the_row_level_ap_gap():
    """會計恆等式：分攤是把單列的 AP 缺口切開，切完要等於原本那塊。

    這是「分攤是會計慣例」這個宣稱的可檢驗形式；比單看某一格 > 0 強得多
    ——後者對一個把比例算錯的實作照樣綠。
    """
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    total_pairs = sum(p["allocated_ap_gap"] for p in out["pair_ledger"])
    assert out["total_ap_gap_allocated_to_suppressors"] == pytest.approx(
        total_pairs)
    per_target = sum(
        (t["ap_gap_from_suppressors"] or 0.0) * t["n_pos"]
        for t in out["target_summary"]
    )
    assert per_target == pytest.approx(total_pairs, rel=1e-9)


def test_axis_order_is_sorted_and_shared():
    out = compute((_two_query_sample(), {"n_queries": 2}), _params())
    assert out["axis_order"] == sorted(out["axis_order"])
    assert set(out["axis_order"]) >= {"A", "B"}


def test_cross_purchase_reports_lift_not_only_conditional_probability():
    """熱門 item 對任何 j 的 P(k|j) 都高——只給條件機率會退化成
    『熱門那行整片亮』，那張圖畫的是熱門度不是關聯。"""
    stats = cross_purchase_stats(_cross_sample(), _params()["schema"])
    row = next(r for r in stats if r["item_j"] == "B" and r["item_k"] == "A")
    assert {"lift", "n_joint", "n_j", "n_k", "p_k_given_j"} <= set(row)


def test_cross_purchase_lift_is_about_one_for_independent_items():
    stats = cross_purchase_stats(_independent_sample(), _params()["schema"])
    row = next(r for r in stats if r["item_j"] == "X" and r["item_k"] == "Y")
    assert row["lift"] == pytest.approx(1.0, abs=0.15)


def test_cross_purchase_lift_exceeds_one_for_items_bought_together():
    """反向釘住上一條：構造真的相關的一對，lift 必須明顯 > 1。
    只驗『獨立時 ≈ 1』的話，一個恆回 1.0 的實作也會綠。"""
    stats = cross_purchase_stats(_coupled_sample(), _params()["schema"])
    row = next(r for r in stats if r["item_j"] == "P" and r["item_k"] == "Q")
    assert row["lift"] > 1.5


def test_empty_sample_returns_stub_without_raising():
    """良性退化輸入：沒有任何正例列。不得 raise，也不得回一個
    看起來像『算過了而且是零』的結果——n_positive_rows 必須是 0。"""
    sample = pd.DataFrame([_row("c1", "A", 0, 0.9), _row("c1", "B", 0, 0.5)])
    out = compute((sample, {"n_queries": 1}), _params())
    assert out["n_positive_rows"] == 0
    assert out["pair_ledger"] == []
    assert out["axis_order"] == []
```

**三個 fixture 自行構造，要求如下**（構造方式由實作者決定，但必須滿足這些性質，且每個 fixture 寫一句 docstring 說明它為什麼滿足）：

- `_two_query_sample()`：兩個 query，至少含 item `A`／`B`，且 `B` 是正例、`A` 是排在它上面的負例，讓 `(B, A)` 這一格有非零分攤。
- `_cross_sample()`：`A` 是熱門 item（多數 entity 都買），`B` 是小眾。用來驗 `lift` 有把 `A` 的基礎購買率除掉。
- `_independent_sample()`：`X` 與 `Y` 的購買在 entity 上獨立——**用 `np.random.default_rng(seed)` 各自獨立抽，entity 數要夠大**（建議 ≥ 400，否則有限樣本誤差會超過 `abs=0.15` 而 flaky）。
- `_coupled_sample()`：`P` 與 `Q` 幾乎總是一起買（例：買 `P` 的 entity 有 90% 也買 `Q`，不買 `P` 的只有 10% 買 `Q`）。

#### Step 2: 跑測試確認失敗

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_suppression.py -v`

Expected RED（**實際訊息與此不同就停下回報**）：
```
ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.suppression'
```

#### Step 3: 實作

見上面「移植來源與必做的改動」。`__init__.py` 照 `item_ability/__init__.py` 的樣板寫（五個符號 ＋ `__all__`），`SCOPE` 的內容 Task 5.3 才定案，這一步先放一個**暫時**版本，`blind_to` 至少一條（不得為空，契約要求）。

#### Step 4: 跑測試確認通過

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_suppression.py -v`
Expected: 10 passed。

#### Step 5: 慢版參考實作等價比對（**取代原稿的 `np.add.at` 原始碼檢查**）

**為什麼換掉原稿那條**：原稿要求 `assert "np.add.at" in inspect.getsource(m._allocate_gap)`。那條測的是「實作長什麼樣」不是「實作做了什麼」——它會在兩個方向同時失靈：(a) 一份寫了 `np.add.at` 但**算錯**的實作照樣綠；(b) 一份用 pandas `groupby` 正確且同樣向量化的實作會**紅**，逼實作者為了過測試改寫成更難讀的形式。向量化散射累加最真實的風險是**算錯**（索引對錯、重複鍵被覆蓋而不是累加），所以要測的是數值等價。

在測試檔中直接寫一份**照抄腳本原版雙層迴圈語意**的慢版參考實作 `_reference_pair_ledger(sample, params)`（就是 `scripts/suppression_ledger_diagnosis.py:485-574` 那段的 Python 迴圈版，只需要算出 `pair_ledger` 的 `allocated_ap_gap`／`affected_positive_rows`／`mean_score_margin` 三個鍵），然後：

```python
def test_vectorised_allocation_matches_the_slow_reference():
    """向量化散射累加最容易錯在『重複鍵被覆蓋而不是累加』——同一個
    (受害 item, 壓制者 item) 會在很多不同 query 反覆出現，那正是這個
    bug 會顯現的地方。所以 fixture 必須是多 query、多重複 pair 的。
    """
    sample = _many_query_sample()      # ≥ 30 query，item 數 ≥ 5，pair 大量重複
    fast = compute((sample, {"n_queries": 30}), _params())["pair_ledger"]
    slow = _reference_pair_ledger(sample, _params())
    assert len(fast) == len(slow)
    fast_by_key = {(r["positive_item"], r["suppressor_item"]): r for r in fast}
    for key, ref in slow.items():
        got = fast_by_key[key]
        assert got["allocated_ap_gap"] == pytest.approx(ref["allocated_ap_gap"])
        assert got["affected_positive_rows"] == ref["affected_positive_rows"]
        assert got["mean_score_margin"] == pytest.approx(ref["mean_score_margin"])
```

#### Step 6: 規模計時（效能契約的可量形式）

```python
def test_scales_to_a_realistic_pair_count():
    """效能契約。腳本原版的內層逐 pair 迴圈在這個規模要數十秒；
    向量化後應在數秒內。門檻設得很鬆（30s）是刻意的——這條要抓的是
    『退回逐 pair 迴圈』這種量級差，不是機器快慢。
    """
    sample = _scale_sample(n_queries=3000, n_items=20)   # ≈ 6 萬列
    t0 = time.monotonic()
    out = compute((sample, {"n_queries": 3000}), _params())
    elapsed = time.monotonic() - t0
    assert out["n_misordered_pairs"] > 100_000, "fixture 沒有製造出足夠的成對數"
    assert elapsed < 30.0, f"耗時 {elapsed:.1f}s"
```

**回報時必須附上實測秒數與 `n_misordered_pairs`**——這個數字是 Plan 3 公司環境檢視點第 4 條的本機對照組。

#### Step 7: mutation check

**下在因果鏈上唯一不可省的那一步**（Plan 2 教訓：mutation 下在最顯眼的幾行常常是假綠）。這裡是分攤比例的**分母**：把 `_compute.py` 裡 `raw_severity / raw_total_for_row * row_ap_gap` 的 `raw_total_for_row` 改成常數 `1.0`。

- Expected: `test_allocated_gap_sums_to_the_row_level_ap_gap` **轉紅**（會計恆等式被破壞）。
- 若只有 `test_pair_ledger_attributes_gap_to_the_suppressor` 紅而恆等式那條仍綠 → 恆等式測試沒測到東西，回報。
- 改回後全綠。

**第二個 mutation**：把 `cross_purchase_stats` 的 `lift` 改成直接回 `p_k_given_j`（拿掉除以基礎率那一步）。
- Expected: `test_cross_purchase_lift_is_about_one_for_independent_items` 轉紅。
- 改回後全綠。

回報時寫出**兩個 mutation 各自弄壞了哪一行、哪些測試轉紅**。

#### Step 8: Commit

```bash
git add -A
git commit -m "feat(diagnosis): suppression 計算層（向量化分攤＋lift 交叉購買，同軸序）"
```

---

### Task 5.3: `suppression` 呈現層 ＋ 接線

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/suppression/_render.py`
- Modify: `src/recsys_tfb/diagnosis/metric/suppression/__init__.py`（`SCOPE` 定案）
- Modify: `src/recsys_tfb/diagnosis/metric/contract.py`（`DIAGNOSES` 加 `"suppression"`，第四順位）
- Modify: `tests/test_diagnosis/test_metric/test_contract.py:22`（行內 tuple 加一項）
- Modify: `conf/base/catalog.yaml`（`evaluation_suppression` JSONDataset entry）
- Modify: `conf/base/parameters_evaluation.yaml`（`evaluation.diagnosis.suppression`）
- Modify: `src/recsys_tfb/core/consistency.py`（A19 改寫，見下）
- Modify: `tests/test_core/test_consistency.py:888-919`
- Test: `tests/test_diagnosis/test_metric/test_suppression_render.py`

**接線只有三處**（Plan 1.5 建立、Plan 2 兩次實測成立）：`DIAGNOSES` 一行、`catalog.yaml` 一條 JSONDataset entry、子套件本身。`pipeline.py`／`nodes_spark.py`／`generate_report` **都不動**。`suppression` 吃共用抽樣，所以**不需要**宣告 `INPUTS`（`DEFAULT_INPUTS` 就是 `("diagnosis_sample", "parameters")`）。

#### 呈現內容

1. **壓制矩陣熱圖**：列 ＝ 受害 item，欄 ＝ 壓制者 item，值 ＝ `matrices.target_gap_share`。用 `figures.heatmap(...)` **不給 `center`**（單向大小，走 `sequential_scale()`）。
2. **交叉購買泡泡格圖**：**同軸序**。用既有的 `figures.bubble_grid(x, y, size, colour, hover_text, title, colorbar_title, center=1.0)`（`src/recsys_tfb/report/figures.py:107-145`，已存在，不要另寫）——`size=n_joint`、`colour=lift`、`center=1.0`。hover 給 `n_joint`／`n_j`／`n_k`／`p_k_given_j`／`lift` 五個數。
3. **兩張圖各自一個 section**，第二個的 `description` 寫一句「怎麼對照著看」——**只描述兩張圖各是什麼，不寫「若 X 則代表 Y」**（鐵則 1）。
4. **具體案例表**：`result["examples"]`（top `top_examples`，預設 50），含 query、正例 item、壓制者、兩者 logit 與差值、名次。
5. **per-suppressor 彙總條圖**：`result["by_suppressor"]` 的 `overall_ap_gap_share`。

#### ⚠ 圖形點數預算（原稿沒講，會在 item 數多的部署炸掉）

`figures.MAX_FIGURE_POINTS = 2000`，而 `heatmap` 檢查的是 `z.size`、`bubble_grid` 檢查的是 `len(x)`——兩張圖都是 **|items|²**。22 個 item ＝ 484，沒事；**45 個 item 就超過 2000，`assert_within_budget` 會直接 raise**，而這個框架的 item 集合由使用者自定義，沒有 22 這個保證。

**作法**：`axis_order` 在 render 端截到前 `N` 個 item，`N = floor(sqrt(MAX_FIGURE_POINTS))`（＝44）。挑哪 44 個：**依 `overall_ap_gap_share` 由大到小**（即在成對表裡分攤到最多缺口的 item）。被截掉時**在該 section 的 `description` 寫出來**：「item 共 M 個，此圖只畫分攤缺口最大的 N 個（圖形點數上限 2000）」。

**不得**用 config 門檻決定截多少——那會撞鐵則 2。這裡的 N 是**繪圖引擎的硬上限推導出來的**，不是可調參數，所以不進 config。

#### `SCOPE.blind_to` 必含（逐條，不得改寫成更軟的說法）

- 「AP 缺口的分攤比例是**會計慣例**（依 severity 比例分攤到每個排在正例上方的負例），不是因果——它不代表『拿掉這個壓制者就會賺回這麼多』。」
- 「共買統計算的是**同一份診斷抽樣**上、同一批 entity 的實際標籤共現，與模型無關；它不解釋模型為什麼這樣排。**而且抽樣是分層的，樣本內的共買頻率不是母體共買頻率的無偏估計**。」
- 「lift = 1 代表在這份樣本上兩個 item 的購買近似獨立，不代表商業上無關。」

#### A19 改寫（原稿修正 #3）

- `consistency.py:691-704` 的 `pair_ledger_param_errors` 改名 `suppression_param_errors`，內容改成**只驗 `evaluation.diagnosis.suppression.top_examples`**：缺席時預設 50；有給就必須是 `int`（`bool` 不算，`isinstance(x, bool)` 要先擋）且 `>= 0`。
- **`enabled` 不在 A19 驗** —— `consistency.py:589-599`（A15）已經對 `DIAGNOSES` 裡每個 name 驗過，`suppression` 一進 registry 就自動涵蓋。兩邊都驗會讓同一個壞值吐兩條訊息。
- legend（`consistency.py:109-110`）改寫成：`A19 — evaluation.diagnosis.suppression.top_examples must be a non-negative int (enabled is covered by A15). Predicate: suppression_param_errors.`
- `consistency.py:857` 的呼叫改名。
- `tests/test_core/test_consistency.py:888-919` 整段改寫（`_params(pair_ledger=...)` → `_params(suppression=...)`，`match="pair_ledger"` → `match="suppression"`）。

> ⚠ **`match=` 的選字**（Plan 2 假綠形態 #2）：`match="suppression"` 要確認**沒有別條 predicate** 會對同一份 config raise 且訊息含 "suppression"。寫完先跑一次「把 `suppression_param_errors` 整個函式回 `[]`」的 mutation，確認那條測試轉紅。

> **已知落差，本 task 不修**：`evaluation.diagnosis.item_ability.top_n` 目前**沒有**任何 consistency 驗證（查證：`grep -rn "top_n" src/recsys_tfb/core/consistency.py` 零命中）。`top_examples` 有驗而 `top_n` 沒有是不一致的，但補 `top_n` 屬於 Plan 2 的範圍、不屬於這裡。**寫進 Plan 5 的收尾清單**，不要順手做掉。

#### Step 1: 寫失敗測試

`tests/test_diagnosis/test_metric/test_suppression_render.py`。比照 `tests/test_diagnosis/test_metric/test_item_ability_render.py` 的既有形狀（**先讀它**，不要自創）。除了各 section 的基本斷言外，必須有這三條：

```python
def test_two_matrices_share_axis_order():
    """兩張圖同軸序是『並排對照』這個版面決定的唯一技術前提。
    軸序漂掉不會 raise、不會有數值測試轉紅，圖看起來也正常——
    只是讀者對照出來的每一個結論都是錯的。
    """
    sections = suppression.render(_result(), {})
    supp = _figure_of_type(sections, "heatmap")
    bubble = _figure_of_type(sections, "scatter")
    assert list(supp.x) == sorted(set(bubble.x))
    assert list(supp.y) == sorted(set(bubble.y))


def test_bubble_grid_encodes_two_different_quantities():
    """大小 ＝ 共買客戶數、顏色 ＝ lift。兩者編同一個量的話這張圖
    只剩一個維度，而『樣本量小的格子顏色不可信』就看不出來了。
    """
    bubble = _figure_of_type(suppression.render(_result(), {}), "scatter")
    assert bubble.marker.size is not None
    assert bubble.marker.color is not None
    assert list(bubble.marker.size) != list(bubble.marker.color)


def test_axis_is_capped_and_says_so_when_items_exceed_the_point_budget():
    """點數預算：item 數超過 44 時必須截斷**並在 description 說明**。

    只斷言『沒有炸掉』會同時被『正確截斷』與『根本沒畫這張圖』滿足
    （Plan 2 假綠形態：「不存在」斷言）——所以要斷言系統**說了什麼**。
    """
    sections = suppression.render(_result_with_items(60), {})
    supp = _figure_of_type(sections, "heatmap")
    assert len(supp.x) == 44
    text = " ".join(s.description for s in sections)
    assert "60" in text and "44" in text
```

#### Step 2: 跑測試確認失敗

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_diagnosis/test_metric/test_suppression_render.py -v`
Expected RED: `ImportError` / `AttributeError: module ... has no attribute 'render'`。

#### Step 3: 實作 → Step 4: 跑測試

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric tests/test_core/test_consistency.py \
  tests/test_pipelines/test_evaluation -q 2>&1 | tail -10
```
Expected: 全綠。**測試範圍就是這三組，不要再往外開**（Plan 2 教訓：測試指令開太寬，665 條跑了很多次）。

#### Step 5: Commit

```bash
git add -A
git commit -m "feat(diagnosis): suppression 呈現層＋接線（壓制矩陣 vs 共買泡泡格圖同軸序；A19 改軌）"
```

---

### Task 5.4: `pair_ledger` ／ `cross_purchase` 退場（純刪除，獨立 commit）

**為什麼獨立成一個 task**（原稿修正 #6）：這動 11 個檔，且**全部是刪除**。公司環境是手動同步的，刪除是最容易漏、漏了又不會報錯的一類（`SYNC-*.md` 的第 0 節就是為此而生）。跟新功能混在同一個 commit，`git diff` 讀起來會分不清「這行沒了是因為被取代，還是因為被搬走」。

**前提**：Task 5.3 已完成且全綠——`suppression` 必須先真的能用，才拆舊的。

**要刪 / 要改的完整清單**（`grep -rn "pair_ledger\|cross_purchase" src conf tests scripts` 於 2026-07-20 查得，`scripts/suppression_ledger_diagnosis.py` 是參考實作、不在本 task 範圍）：

| 檔案 | 動作 |
|---|---|
| `src/recsys_tfb/diagnosis/metric/pair_ledger.py` | `git rm` |
| `src/recsys_tfb/diagnosis/metric/cross_purchase.py` | `git rm` |
| `tests/test_diagnosis/test_metric/test_pair_ledger.py` | `git rm` |
| `tests/test_diagnosis/test_metric/test_cross_purchase.py` | `git rm` |
| `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:22,32,40,225,237,420-448,648,669` | 刪 `compute_pair_ledger`；`_diagnosis_enabled` 的回傳從三元組變二元組（**呼叫端一起改**）；docstring 裡的枚舉更新 |
| `src/recsys_tfb/pipelines/evaluation/pipeline.py:32,122-124,159` | 刪 import、Node、`generate_report` inputs 裡的 `evaluation_pair_ledger` |
| `src/recsys_tfb/evaluation/report_builder.py:520-545,546-587,873,1035,1048` | 刪 `_pair_ledger_heatmap`／`build_pair_ledger_section`／`assemble_report` 的 `pair_ledger` 參數與呼叫；`:873` 的註解更新 |
| `src/recsys_tfb/core/consistency.py` | Task 5.3 已處理（A19 改軌），本 task 確認無殘留 |
| `src/recsys_tfb/diagnosis/metric/_common.py:6,17` | **模組 docstring 的家族清單**仍列 `pair_ledger`（兩處），改成現況 |
| `src/recsys_tfb/diagnosis/metric/results.py:38` | 註解列舉過渡期檔案，移除 `pair_ledger.json` |
| `conf/base/catalog.yaml:254-256` | 刪 `evaluation_pair_ledger` entry |
| `conf/base/parameters_evaluation.yaml:62,131-137,163` | 刪 `report.sections.pair_ledger`、`diagnosis.pair_ledger` 整段；**`:163` 的 `debug_inject_offsets` 註解寫「只影響分流層節點（offset_sweep＋pair_ledger）」要改成只剩 `offset_sweep`** |
| `tests/test_evaluation/test_report_builder.py:683-719` | 刪五條 `pair_ledger` 測試 ＋ `_LEDGER_FIXTURE` 若無其他消費者 |
| `tests/test_pipelines/test_evaluation/test_nodes_spark.py:616-626,643,681,703,755-761,789,863,917` | 刪 `compute_pair_ledger` 測試；其餘是 params fixture 裡的 `"pair_ledger": {"enabled": False}`，一併移除 |
| `tests/test_pipelines/test_evaluation/test_pipeline.py:28,45,65,83,104` | 節點名／catalog 鍵的預期清單移除 `compute_pair_ledger`／`evaluation_pair_ledger`，**加入 `diagnose_suppression`／`evaluation_suppression`** |
| `tests/scripts/test_render_diagnosis.py:241-253` | 該測試用 `pair_ledger.json` 當「registry 外的檔案」樣本。`pair_ledger.json` 不再產出，但這條測的是「registry 外的檔案會被忽略並列名」——**改用 `metric_ci.json`／`offset_sweep.json` 兩個仍存在的即可**，不要刪測試 |
| `tests/test_pipelines/test_evaluation/test_generate_report.py:175` | 註解提到舊 builder，更新 |

- [ ] **Step 1: 先跑一次基準，記下目前的 node 清單**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
for n in create_pipeline().nodes: print(n.name)
" > /Users/curtislu/.claude/jobs/05cdb8ff/tmp/nodes_before.txt
cat /Users/curtislu/.claude/jobs/05cdb8ff/tmp/nodes_before.txt
```

- [ ] **Step 2: 照上表逐檔刪除／修改**

- [ ] **Step 3: 驗證 node 清單的差異恰好是預期的兩項**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
for n in create_pipeline().nodes: print(n.name)
" > /Users/curtislu/.claude/jobs/05cdb8ff/tmp/nodes_after.txt
diff /Users/curtislu/.claude/jobs/05cdb8ff/tmp/nodes_before.txt \
     /Users/curtislu/.claude/jobs/05cdb8ff/tmp/nodes_after.txt
```
Expected：**恰好** `- compute_pair_ledger` 與 `+ diagnose_suppression`（`diagnose_suppression` 在 Task 5.3 就已加入，所以 before 檔裡本來就有它——那麼這裡的差異應該**只有** `- compute_pair_ledger` 一行）。實際結果與此不同就停下回報。

- [ ] **Step 4: 零殘留檢查**

```bash
grep -rn "pair_ledger\|cross_purchase" src conf tests | grep -v __pycache__
```
Expected: **零命中**。（`scripts/suppression_ledger_diagnosis.py` 不在掃描範圍內，那是參考實作，Plan 5 才決定去留。）

- [ ] **Step 5: 全套相關測試**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis tests/test_pipelines/test_evaluation \
  tests/test_core/test_consistency.py tests/test_evaluation/test_report_builder.py \
  tests/scripts/test_render_diagnosis.py -q 2>&1 | tail -10
```
Expected: 全綠。

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(diagnosis): pair_ledger 與 cross_purchase 退場（能力已併入 suppression）"
```

---

### Task 5.5: real-run 驗證（controller 執行，不派 agent）

Plan 2 的同一套流程。`--post-training --model-version 6059dcef`。要確認：

1. node 數從 14 變 **14**（`+diagnose_suppression`、`−compute_pair_ledger`），總時間記下來。
2. `diagnosis/04-suppression.html` 產出，`index.html` 編號 01–04 連續。
3. `diagnosis/suppression.json` 過**嚴格** JSON 解析（無 `NaN` 字面值）。
4. `report.html` 去 plotly UUID 與時間戳後的差異，**應該恰好是「少一個 pair_ledger 區塊」**——多出別的差異就是誤傷。
5. 離線重繪（`scripts/render_diagnosis.py`）能產出四頁。
6. **記下 `n_misordered_pairs` 與 suppression node 的秒數**，對照 Task 5.2 Step 6 的合成規模。

## 公司環境檢視點（本 Plan 的交付驗收）

跑一次公司環境 evaluation，拷回 `diagnosis/` 目錄，看：

1. **兩張矩陣並排讀不讀得出東西**——「模型讓 k 壓制 j」與「買 j 的人也常買 k」對照起來，是不是真的能區分「模型排錯」與「商品本來就競爭」？這是本項設計的全部價值所在，讀不出來就要改。
2. **泡泡大小的辨識度**——公司規模下 item 數與共買數的量級，泡泡會不會擠成一團或差距大到看不見小的？
3. **具體案例表**是否足夠具體到能讓你去查那一筆。
4. **執行時間**——這一項在腳本原版是最慢的，向量化後的實際秒數要記下來。

**看完給回饋之後**：若泡泡格圖在公司規模下不好讀，備案是兩張對齊的矩陣（一張 lift、一張共買數）——精確但要來回看。這個取捨要你看過真實資料才決定得了。
