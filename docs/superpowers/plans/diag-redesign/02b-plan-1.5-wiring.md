# Plan 1.5：接線層重構（診斷重構 1.5/6）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `generate_report` 這個同時做三件事的 node 拆成三個各司其職的 node，讓「顯示哪些內容」完全由 config 決定、不再由 node 接線的參數順序決定。

**Architecture:** 現況 `generate_report` 同時是 Spark 作業（6 次全掃聚合）、診斷頁產生器（寫另一個目錄樹）、主報表組裝器（純函式），三者被綁在 pipeline 的最後一個 node 上。拆成 `compute_report_aggregates`（Spark → JSON）、`render_diagnosis_pages`（診斷 JSON → 多頁 HTML）、`generate_report`（純組裝）。診斷結果的「名字 → 資料」對應改由**檔名**決定，與既有的離線重繪工具共用同一個 loader。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2、pandas 1.5.3。無新套件（生產限制：no UDFs、no network、no additional packages）。

---

## 開工前必讀

1. `00-shared-context.md` —— 五項診斷的邏輯架構、持久化邊界、診斷契約。
2. `README.md` 的「Task 2.8 使用者回饋」兩則 —— 這份計畫的來源。
3. 本檔的「§0 設計脈絡」—— 為什麼是拆節點而不是修綁定機制。**這一節解釋了三個乍看多餘的設計選擇，跳過會在 Task 4 想改回去。**

## §0 設計脈絡（讀懂再動手）

### 為什麼會演變成這樣

不是有人設計成這樣，是逐層長出來的：

1. 最初 `generate_report` 只吃幾個 metrics dict 做組裝。
2. PR#80 為了讓 `report.html` 不要太大，把診斷圖改成 Spark 端聚合——於是 `eval_predictions: SparkDataFrame` 進了簽章，6 個聚合寫在 node 內部。**那次改的目標是圖的大小，不是節點邊界。**
3. 本次重構要出五頁獨立診斷頁。「產出 HTML」直覺上屬於報表節點，於是掛在 `generate_report` 上，五份診斷結果接在既有參數後面（`*registry_diagnoses`）。

每一步單看都合理，合起來是：**pipeline 的最後一個 node 同時是最貴的 node、失敗成本最高的 node、以及參數最脆弱的 node。**

### 2026-07-20 公司環境故障的真正成因

`node.inputs` 少了兩個元素 → 位置 6 的 `evaluation_config_shift` 綁進 `offset_sweep` 參數 → `build_offset_sweep_section` 拿到 `per_item` 是 list 而非 dict → `TypeError`。

**盤點結論（4 條 pipeline、52 個不重複 node）：具備這種「少給 input 不會立刻爆」形狀的只有 3 個** —— `generate_report`（`req=3 all=7 +*args`）、`log_experiment`（training，`req=8 all=10`）、`select_shap_population`（training，`req=3 all=4`）。其餘 49 個是剛好個數，少給即 `TypeError`、爆在該 node 上。

**因此不動 `core/runner.py` 的位置綁定。** 為 3 個 node 去改 59 個呼叫點共用的執行核心，收益／風險比不成立；而其中 1 個正是本計畫要重寫的對象。

> 曾經考慮過、已否決：在 `Node.__init__` 加「inputs 個數要對得上簽章」的檢查。**擋不住這次的失敗**——`generate_report` 有 4 個帶預設值的參數再加 varargs，6/7/8/9 個 inputs 全在合法範圍內。個數對，位置錯。

### 拆完之後的資料流

```
eval_predictions ──→ compute_report_aggregates ──→ evaluation_report_aggregates (JSON)
                                                                    │
diagnosis_sample ──→ diagnose_<name> ×N ──→ evaluation_<name> (JSON) │
                              │                                      │
                              └──→ render_diagnosis_pages ──→ evaluation_diagnosis_pages
                                        （寫 diagnosis/*.html）      │
                                                                     ↓
     evaluation_metrics, baseline_metrics, metric_ci, offset_sweep, pair_ledger
                                        └──────────→ generate_report ──→ report.html
```

**`generate_report` 的簽章裡從此不出現任何診斷。** 加第六項診斷只動 `render_diagnosis_pages` 那一行導出的 inputs。

### 三個乍看多餘的設計選擇，以及理由

**(1) `render_diagnosis_pages` 宣告了五個它不讀值的 inputs。**

它按**檔名**讀 `diagnosis/<name>.json`（診斷結果早已由 catalog 落地）。inputs 裡的五項診斷只當 DAG 的 happens-before 邊：`Pipeline._slice_with_expansion`（`core/pipeline.py:154-189`）走 `node.inputs` 往上拉依賴，拿掉的話 `--only-node render_diagnosis_pages` 不會把診斷節點拉進來。

**為什麼值得付這個怪味道的代價**：Plan 2–5 每加一項診斷都要在 `catalog.yaml` 補一條 JSONDataset entry。忘了補的話——

- 若按位置對應：catalog 自動建 MemoryDataset，值在記憶體裡傳到位，**頁面正常產出**，但磁碟上沒有那份 JSON，離線重繪少一頁。靜默。
- 按檔名對應：讀不到檔 → 進 `missing` → 印出來、那頁不產出。當場可見。

這是**會重複四次**的動作，所以把它的失敗模式從靜默換成可見，值得。

**(2) `load_results` 從 `scripts/` 搬進 `src/`。**

`scripts/render_diagnosis.py::load_results` 已經在按檔名讀了，還回報 `missing`（registry 有、檔案沒有）與 `unknown`（檔案有、registry 沒有）。搬進 `src/` 之後 pipeline 與離線工具**共用同一份**——兩條路徑不可能對「診斷結果長什麼樣、放在哪」產生分歧。方向也對：repo 慣例是 `src/` 不依賴 `scripts/`，反向依賴才是正確的。

**(3) 聚合結果用 `to_dict("split")` 落地，不用 `to_json`。**

6 個聚合回的是 pandas DataFrame，兩種形狀：3 個長格式（RangeIndex，index 無意義）、3 個矩陣（index=item、columns=rank，**index 有意義，不能丟**）。`to_dict("split")` 對兩者都給 `{index, columns, data}`，重建就是 `pd.DataFrame(data, index=..., columns=...)`。

實測掉的三個假設（**不要照直覺重寫**）：

| 直覺 | 實測結果 |
|---|---|
| numpy 值要手動轉成 Python 型別 | **不用**，pandas 的 `to_dict("split")` 已轉好（實測 `int64` → `int`、object index → `str`） |
| 矩陣的 int 欄名（rank 1..N）會在 JSON 裡壞掉 | 不會，`columns` 是 list 不是 dict key，round-trip 後 `.equals()` 為 True |
| NaN 無害 | **有害**。`JSONDataset.save`（`io/json_dataset.py:20-23`）用預設 `allow_nan=True`，NaN 會寫成 `NaN` 字面值——**非合法 JSON**。Python 讀得回來，別的工具不行 |

第三點在 Task 2 有一條專門的驗收條件擋住。

## 範圍

**做：**

- 拆出 `compute_report_aggregates`、`render_diagnosis_pages` 兩個 node；`generate_report` 瘦身成純函式。
- 診斷 node 由 `contract.DIAGNOSES` 導出，不再每項手寫。
- 每項診斷對使用者只有一個開關（`evaluation.diagnosis.<name>.enabled`），並把 `DIAGNOSES` 與它的分工寫進文件。
- 加一條守衛：`DIAGNOSES` 的每個名字都必須有對應的 `catalog.yaml` entry。

**不做（刻意，不是遺漏）：**

| 項目 | 為什麼不做 |
|---|---|
| 改 `core/runner.py` 的位置綁定 | 見 §0：52 個 node 只有 3 個有危險形狀，radius 不成比例 |
| 修 `log_experiment`（`inputs=10, req=8`）與 `select_shap_population`（`inputs=4, req=3`） | training 側，不在本計畫範圍。**已知但不動**，記在本檔備查 |
| 讓 `scripts/render_diagnosis.py` 也重繪主報表 | 本計畫交付的是「`generate_report` 變純函式」這個**前提**。真要做時它是獨立一件事，且使用者尚未要求 |
| 移除舊三項診斷（ci／offset_sweep／pair_ledger） | 它們在 Plan 4／5 才退場。本計畫只搬動接線，不動任何診斷的存廢 |
| `report.sections.*` 的開關整併 | 過渡期產物，Plan 5 收尾時自然消失（見 README 的盤點表 #3） |

## 三條鐵則（每份計畫都重貼一次）

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別。顏色只編碼資料的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，`blind_to` 為空即契約違反，有測試擋。

本計畫不新增任何呈現內容，三條鐵則在此的作用是**約束你不要順手改文案**：搬動接線時看到的任何 section 文字，原樣搬移。

## Pre-flight（每次開工，照抄執行）

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V   # 期望：Python 3.10.9
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```

期望最後一行：`[check-isolation] OK：root=/Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign；...`

**測試一律用絕對路徑的 venv python ＋ `PYTHONPATH=src`**，裸跑 `pytest` 會抓到 main 的 `src`、靜默測錯 code：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <path> -v
```

## File Structure

| 檔案 | 動作 | 職責 |
|---|---|---|
| `src/recsys_tfb/diagnosis/metric/results.py` | **新增** | 按檔名讀 `diagnosis/<name>.json`；回 `(results, missing, unknown)`。pipeline 與離線工具的唯一 loader |
| `src/recsys_tfb/evaluation/diagnostics_spark.py` | 修改 | 加 `frame_to_json` / `frame_from_json` / `aggregate_report_diagnostics`；既有 6 個聚合函式不動 |
| `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | 修改 | 加 `compute_report_aggregates`、`render_diagnosis_pages`、`make_diagnosis_node`；`generate_report` 瘦身；刪 `diagnose_config_shift` |
| `src/recsys_tfb/pipelines/evaluation/pipeline.py` | 修改 | 診斷 node 由 `DIAGNOSES` 導出；接上兩個新 node |
| `scripts/render_diagnosis.py` | 修改 | `load_results` 改成從 `src/` import（保留同名 re-export，既有呼叫端不動） |
| `conf/base/catalog.yaml` | 修改 | 加 `evaluation_report_aggregates` |
| `tests/test_diagnosis/test_metric/test_results.py` | **新增** | loader 的測試（從 `tests/scripts/test_render_diagnosis.py` 搬移相關案例） |
| `tests/test_evaluation/test_diagnostics_spark.py` | 修改 | 加序列化 round-trip 與嚴格 JSON 的測試 |
| `tests/test_pipelines/test_evaluation/test_pipeline.py` | 修改 | node 清單／接線斷言更新 |
| `tests/test_pipelines/test_evaluation/test_nodes_spark.py` | 修改 | 新 node 的測試；`diagnose_config_shift` 的測試改成走工廠 |
| `tests/test_pipelines/test_evaluation/test_generate_report.py` | 修改 | 簽章變動 |

**每個 Task 結束都要 commit。** commit message 用 `refactor(evaluation): ...` 或 `test(evaluation): ...`。

---

（Task 1 起見下一節）

## Task 1: `load_results` 搬進 `src/`

搬移的理由見 §0 (2)。**這是純搬移，不改行為。**

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/results.py`
- Create: `tests/test_diagnosis/test_metric/test_results.py`
- Modify: `scripts/render_diagnosis.py:81-113`（刪 `load_results` 本體，改 import）
- Modify: `tests/scripts/test_render_diagnosis.py:205-223`（AST 守衛擴及新模組）

- [ ] **Step 1: 寫失敗的測試**

`tests/test_diagnosis/test_metric/test_results.py`：

```python
"""``diagnosis.metric.results`` —— 診斷落地產物的 loader。

這份 loader 同時服務 pipeline（``render_diagnosis_pages``）與離線工具
（``scripts/render_diagnosis.py``），所以它的回傳契約 ``(results, missing,
unknown)`` 兩邊都依賴，改動要同時看兩個呼叫端。
"""
import json

from recsys_tfb.diagnosis.metric import contract, results


def _write(tmp_path, name, payload):
    (tmp_path / f"{name}.json").write_text(
        json.dumps(payload), encoding="utf-8")


def test_reads_each_registry_diagnosis_by_filename(tmp_path):
    _write(tmp_path, "config_shift", {"delta": 0.25})
    out, missing, unknown = results.load_results(tmp_path)
    assert out == {"config_shift": {"delta": 0.25}}
    assert missing == []
    assert unknown == []


def test_absent_file_is_reported_as_missing_not_raised(tmp_path):
    out, missing, unknown = results.load_results(tmp_path)
    assert out == {}
    assert missing == ["config_shift"]
    assert unknown == []


def test_json_outside_the_registry_is_reported_as_unknown(tmp_path):
    _write(tmp_path, "config_shift", {"delta": 0.25})
    _write(tmp_path, "offset_sweep", {"per_item": {}})
    out, missing, unknown = results.load_results(tmp_path)
    assert list(out) == ["config_shift"]
    assert unknown == ["offset_sweep"]


def test_registry_is_read_at_call_time_not_import_time(tmp_path, monkeypatch):
    """必須用 ``contract.DIAGNOSES``（模組屬性）而不是 ``from … import``。

    組裝層也是在呼叫當下讀同一個屬性，兩邊看到的 registry 才保證是同一份。
    這條同時是 ``scripts/render_diagnosis.py`` 既有兩條 monkeypatch 測試能
    成立的前提——那兩條在搬移後改成走這個模組，這裡先把前提釘住。
    """
    _write(tmp_path, "config_shift", {"delta": 0.25})
    monkeypatch.setattr(contract, "DIAGNOSES", ("config_shift", "not_copied"))
    out, missing, unknown = results.load_results(tmp_path)
    assert missing == ["not_copied"]


def test_results_follow_registry_order(tmp_path, monkeypatch):
    """``results`` 的鍵順序＝registry 順序，不是檔案系統順序。

    ``assemble_diagnosis_pages`` 用 ``enumerate(DIAGNOSES, 1)`` 決定頁面的
    數字前綴，所以順序錯不會有人發現——頁面照樣產出，只是編號亂掉。
    """
    monkeypatch.setattr(contract, "DIAGNOSES", ("b_diag", "a_diag"))
    _write(tmp_path, "a_diag", {"x": 1})
    _write(tmp_path, "b_diag", {"x": 2})
    out, _, _ = results.load_results(tmp_path)
    assert list(out) == ["b_diag", "a_diag"]
```

- [ ] **Step 2: 跑測試確認轉紅**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_results.py -v
```

預期：`ImportError: cannot import name 'results' from 'recsys_tfb.diagnosis.metric'`。
**實際訊息與此不同 → 停下回報，不要自行繼續。**

- [ ] **Step 3: 建立新模組**

`src/recsys_tfb/diagnosis/metric/results.py`：

```python
"""診斷的落地產物：按檔名讀回來。

pipeline 內的 ``render_diagnosis_pages`` 與離線工具
``scripts/render_diagnosis.py`` 共用這一份 loader —— 兩條路徑對「診斷結果叫
什麼、放在哪」不可能產生分歧。

**本模組刻意不 import pyspark，也不 import 任何會把 pyspark 拉進來的東西。**
離線重繪的全部價值在於「不需要 Spark、兩秒跑完」，import 鏈上多一個 pyspark
就把冷啟動拉回數秒。有測試用 AST 掃這件事
（``tests/scripts/test_render_diagnosis.py``）。
"""
from __future__ import annotations

import json
from pathlib import Path


def load_results(input_dir) -> tuple[dict, list[str], list[str]]:
    """依 ``DIAGNOSES`` 的順序讀 ``<input-dir>/<name>.json``。

    Returns:
        ``(results, missing, unknown)``——``results`` 直接餵給
        ``assemble_diagnosis_pages``；``missing`` 是 registry 有、目錄裡沒有的
        診斷名；``unknown`` 是目錄裡有、registry 沒有的 JSON 檔名。後兩者方向
        相反、目的相同：不讓「沒處理」看起來像「沒問題」。

    這裡用 ``contract.DIAGNOSES``（模組屬性）而不是 ``from … import
    DIAGNOSES``：組裝層也是在呼叫當下讀同一個屬性，兩邊看到的 registry 才保證
    是同一份；測試 monkeypatch ``contract.DIAGNOSES`` 時也才有效。
    """
    from recsys_tfb.diagnosis.metric import contract

    input_dir = Path(input_dir)
    results: dict = {}
    missing: list[str] = []
    # 目錄裡有、但不在 registry 的 JSON。與 missing 是相反方向的同一件事：
    # 不要讓「沒處理」看起來像「沒問題」。使用者拷回來的是整個 diagnosis/
    # 目錄，過渡期裡面還有 metric_ci.json／offset_sweep.json／pair_ledger.json
    # 這些尚未進 registry 的既有診斷——拷了 4 份只看到 1 頁而畫面一片安靜，
    # 讀起來像工具壞了。
    unknown = sorted(
        p.stem for p in input_dir.glob("*.json")
        if p.stem not in contract.DIAGNOSES
    )
    for name in contract.DIAGNOSES:
        path = input_dir / f"{name}.json"
        if not path.exists():
            missing.append(name)
            continue
        results[name] = json.loads(path.read_text(encoding="utf-8"))
    return results, missing, unknown
```

- [ ] **Step 4: 跑測試確認轉綠**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_results.py -v
```
預期：5 passed。

- [ ] **Step 5: script 改成 import，刪掉本體**

`scripts/render_diagnosis.py`：刪除 `:81-113` 的 `def load_results(...)` 整段，在檔頭 import 區加：

```python
from recsys_tfb.diagnosis.metric.results import load_results
```

**保留這個名字在 script 的命名空間**（`:138` 的呼叫端與既有測試都靠它），不要改成 `results.load_results(...)`。

- [ ] **Step 6: 擴充 AST 守衛到新模組**

`tests/scripts/test_render_diagnosis.py:205-223`。**這一步不能省**：`load_results` 搬走之後，原本的守衛只掃 script 自己的 import 區，而真正可能把 pyspark 拉進來的邏輯已經在 `src/` 了——守衛會**繼續全綠但守不到東西**（Agent 盤點指出的靜默失效）。

把該測試改成掃兩個檔案：

```python
def test_no_pyspark_import_in_the_offline_render_path():
    """離線重繪路徑上的**每個**檔案都不得把 Spark 拉進來。

    為什麼不能只靠 sys.modules 那條 monkeypatch：module-level import 在
    **收集測試時**就跑完了，比 ``monkeypatch.setitem`` 早——真的寫了
    ``import pyspark`` 在檔頭，那條測試反而看不到。

    為什麼掃兩個檔案而不只是 script：``load_results`` 已經搬進 ``src/``
    （Plan 1.5 Task 1）。只掃 script 的話，這條會繼續全綠但守備範圍是空的
    ——真正會長出 Spark 相依的是 src 那邊。
    """
    import ast

    repo = Path(__file__).resolve().parents[2]
    targets = [
        repo / "scripts" / "render_diagnosis.py",
        repo / "src" / "recsys_tfb" / "diagnosis" / "metric" / "results.py",
    ]
    offenders = {}
    for path in targets:
        imported = set()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                imported.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
        bad = [m for m in imported if m.split(".")[0] == "pyspark"]
        if bad:
            offenders[path.name] = bad
    assert not offenders, offenders
```

- [ ] **Step 7: 跑既有測試確認搬移沒改行為**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_render_diagnosis.py tests/test_diagnosis/test_metric/ -v
```
預期：全綠。**特別確認 `test_skips_missing_diagnoses_without_failing` 與
`test_reports_skipped_diagnoses_on_stderr` 仍綠**——它們 monkeypatch
`contract.DIAGNOSES`，只有在新模組用「模組屬性存取」時才會通過。

- [ ] **Step 8: mutation check（證明守衛真的有守到）**

把 `results.py` 的 `from recsys_tfb.diagnosis.metric import contract` 搬到模組頂層並改成 `from recsys_tfb.diagnosis.metric.contract import DIAGNOSES`（同時把函式內的 `contract.DIAGNOSES` 改成 `DIAGNOSES`），重跑 Step 7。

預期：`test_registry_is_read_at_call_time_not_import_time` 與 script 那兩條 monkeypatch 測試**轉紅**。確認後改回。

在回報中寫出你改了哪一行、哪幾條轉紅。**若全綠 → 停下回報**：代表這個契約沒有被任何測試守住。

- [ ] **Step 9: Commit**

```bash
git add src/recsys_tfb/diagnosis/metric/results.py \
        tests/test_diagnosis/test_metric/test_results.py \
        scripts/render_diagnosis.py tests/scripts/test_render_diagnosis.py
git commit -m "refactor(diagnosis): load_results 搬進 src，pipeline 與離線工具共用"
```

---

## Task 2: 聚合結果的 JSON 序列化

**Files:**
- Modify: `src/recsys_tfb/evaluation/diagnostics_spark.py`（加 3 個函式，既有 6 個聚合不動）
- Modify: `tests/test_evaluation/test_diagnostics_spark.py`

- [ ] **Step 1: 寫失敗的測試**

加到 `tests/test_evaluation/test_diagnostics_spark.py` 尾端：

```python
class TestFrameJson:
    """聚合小 frame 的落地格式。

    兩種形狀分開處理的理由：長格式的 index 是無意義的 RangeIndex，矩陣的
    index 是 item 名稱（heatmap 的 y 軸標籤，丟了圖就沒有標籤）。
    """

    def test_matrix_round_trip_preserves_index_and_int_columns(self):
        import json

        from recsys_tfb.evaluation.diagnostics_spark import (
            frame_from_json, frame_to_json,
        )
        mat = pd.DataFrame([[1, 2], [3, 4]],
                           index=["insur", "loan"], columns=[1, 2])
        payload = frame_to_json(mat, "matrix")
        back = frame_from_json(json.loads(json.dumps(payload)))
        assert back.equals(mat)
        # 欄名必須留在 int：_heatmap_from_matrix 用 list(matrix.columns) 當
        # rank 值渲染成 "Rank 1"。變成字串 "1" 不會有任何測試轉紅，圖也照畫。
        assert list(back.columns) == [1, 2]
        assert list(back.index) == ["insur", "loan"]

    def test_long_round_trip_drops_the_meaningless_index(self):
        import json

        from recsys_tfb.evaluation.diagnostics_spark import (
            frame_from_json, frame_to_json,
        )
        long = pd.DataFrame({"prod_name": ["a", "b"], "count": [1, 2]})
        payload = frame_to_json(long, "long")
        assert "index" not in payload
        back = frame_from_json(json.loads(json.dumps(payload)))
        assert back.equals(long)
        assert isinstance(back.index, pd.RangeIndex)

    def test_kind_is_declared_not_inferred(self):
        """``kind`` 必須明寫。

        從 index 型別推斷會在「item 名稱剛好是 0,1,2」時猜錯——那時矩陣的
        index 看起來就是 RangeIndex，會被當成長格式而把 y 軸標籤丟掉，而且
        不會有任何測試轉紅。
        """
        import pytest

        from recsys_tfb.evaluation.diagnostics_spark import frame_to_json
        with pytest.raises(ValueError, match="kind"):
            frame_to_json(pd.DataFrame({"a": [1]}), "records")

    def test_empty_frame_round_trips(self):
        """退化輸入：空 frame。

        ``score_histogram_counts`` 在輸入為空時就是回
        ``pd.DataFrame(columns=cols)``（diagnostics_spark.py:39），所以這不是
        假想的邊界。
        """
        import json

        from recsys_tfb.evaluation.diagnostics_spark import (
            frame_from_json, frame_to_json,
        )
        empty = pd.DataFrame(columns=["prod_name", "count"])
        back = frame_from_json(
            json.loads(json.dumps(frame_to_json(empty, "long"))))
        assert list(back.columns) == ["prod_name", "count"]
        assert len(back) == 0

    def test_nan_becomes_null_so_the_file_stays_strict_json(self):
        """NaN 必須換成 None。

        ``JSONDataset.save``（io/json_dataset.py:20-23）用預設
        ``allow_nan=True``，NaN 會寫成 ``NaN`` 這個**非合法 JSON** 的字面值。
        Python 的 ``json.loads`` 讀得回來，別的工具不行——而這些檔案的用途
        就是被拷到別的環境去讀。

        用 ``parse_constant`` 驗而不是掃字串：``"NaN" in text`` 會被
        item 名稱裡剛好有 NaN 三個字母的情況誤判。
        """
        import json

        import numpy as np

        from recsys_tfb.evaluation.diagnostics_spark import frame_to_json
        df = pd.DataFrame({"a": [1.0, np.nan]})
        text = json.dumps(frame_to_json(df, "long"))

        def _boom(const):
            raise AssertionError(f"非合法 JSON 常數：{const}")

        json.loads(text, parse_constant=_boom)   # 有 NaN/Infinity 就 raise
```

- [ ] **Step 2: 跑測試確認轉紅**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_diagnostics_spark.py -k TestFrameJson -v
```
預期：`ImportError: cannot import name 'frame_to_json'`。
**實際訊息與此不同 → 停下回報。**

- [ ] **Step 3: 實作**

加到 `src/recsys_tfb/evaluation/diagnostics_spark.py` 尾端：

```python
#: :func:`frame_to_json` 支援的兩種形狀。
_LONG = "long"
_MATRIX = "matrix"


def _no_nan(value):
    """NaN → None。理由見 :func:`frame_to_json` 的 docstring。"""
    return None if isinstance(value, float) and math.isnan(value) else value


def frame_to_json(df: pd.DataFrame, kind: str) -> dict:
    """把聚合出來的小 frame 轉成可以落地的 dict。

    ``kind`` 明寫而不是從 index 型別推斷：長格式的 index 是無意義的
    ``RangeIndex``（丟掉），矩陣的 index 是 item 名稱（丟了 heatmap 就沒有
    y 軸標籤）。推斷會在「item 剛好是 0, 1, 2」時猜錯，而那不會有任何測試轉紅。

    ``to_dict("split")`` **已經**把 numpy 純量轉成 Python 原生型別（實測
    ``int64`` → ``int``、object index → ``str``），所以這裡不逐格轉型。但它
    **不處理 NaN**：``JSONDataset`` 用預設 ``allow_nan=True``，NaN 會寫成
    ``NaN`` 這個非合法 JSON 的字面值，Python 讀得回來、別的工具不行——而這些
    檔案的用途正是被拷到別的環境去讀。所以在這裡換成 ``None``。
    """
    if kind not in (_LONG, _MATRIX):
        raise ValueError(
            f"kind must be {_LONG!r} or {_MATRIX!r}, got {kind!r}"
        )
    split = df.to_dict("split")
    out = {
        "kind": kind,
        "columns": list(split["columns"]),
        "data": [[_no_nan(v) for v in row] for row in split["data"]],
    }
    if kind == _MATRIX:
        out["index"] = list(split["index"])
    return out


def frame_from_json(payload: dict) -> pd.DataFrame:
    """:func:`frame_to_json` 的反向。回傳的 frame 直接餵給繪圖函式。"""
    df = pd.DataFrame(payload["data"], columns=payload["columns"])
    if payload.get("kind") == _MATRIX:
        df.index = payload["index"]
    return df
```

檔頭 import 區加 `import math`（緊接在 `from __future__` 之後的 stdlib 區）。

- [ ] **Step 4: 跑測試確認轉綠**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_diagnostics_spark.py -v
```
預期：既有測試全綠 ＋ 新的 5 條綠。

- [ ] **Step 5: mutation check**

把 `frame_to_json` 裡的 `if kind == _MATRIX: out["index"] = ...` 整段刪掉，重跑。

預期：`test_matrix_round_trip_preserves_index_and_int_columns` 轉紅。

**為什麼 mutation 下在這一行**：這次改動的因果鏈上，「矩陣要保留 index」是唯一
不可省的一步——長格式那條路本來就不需要 index，型別檢查那條 raise 是獨立的。
下在 `_no_nan` 上則會被 `test_nan_becomes_null...` 抓到，是另一條鏈。

確認後改回，回報中寫出轉紅的測試名稱。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/evaluation/diagnostics_spark.py \
        tests/test_evaluation/test_diagnostics_spark.py
git commit -m "feat(evaluation): 聚合小 frame 的 JSON 落地格式（long / matrix 兩型）"
```

---

## Task 3: 拆出 `render_diagnosis_pages`，`generate_report` 不再收診斷

這是本計畫的**主要交付**：做完之後 `generate_report` 的簽章裡不再出現任何診斷。

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（加 `render_diagnosis_pages`；`generate_report:506-609` 改簽章與尾段）
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py:120-136`
- Modify: `tests/test_pipelines/test_evaluation/test_pipeline.py`（`:22-31`、`:33-43`、`:49-59`、`:66-75`、`:81-92`、`:125-203`）
- Modify: `tests/test_pipelines/test_evaluation/test_generate_report.py:49-59`
- Modify: `tests/test_pipelines/test_resume_contracts.py:62-70`

- [ ] **Step 1: 寫失敗的測試（接線）**

`tests/test_pipelines/test_evaluation/test_pipeline.py`：把 `TestGenerateReportNodeWiring` 的 class docstring 與 `:192-203` 那兩段 varargs 斷言，換成下面這一份。**docstring 一起換**——舊 docstring 描述的機制（varargs 收診斷）在這個 task 之後不再存在，留著會讓下一個人以為程式碼壞了。

```python
class TestGenerateReportNodeWiring:
    """core/runner.py binds Node inputs to the wrapped function purely by
    position (``node.func(*inputs)`` — no keyword matching, see
    src/recsys_tfb/core/runner.py). generate_report's parameters are all
    dict-typed, so if the Node's ``inputs=[...]`` list in pipeline.py drifts
    out of sync with the signature's parameter order, one dict silently lands
    in the wrong parameter — Python raises no TypeError and the corresponding
    report section just goes missing. This test pins that ordering.

    Catalog keys and parameter names aren't spelled identically: some carry an
    "evaluation_" prefix the parameter names drop (evaluation_metric_ci ->
    metric_ci). So the checkable property is: each catalog key equals its
    parameter name, optionally after stripping a leading "evaluation_",
    position-for-position.

    **The signature must have no varargs and no defaults** (Plan 1.5). Both
    properties are what makes a stale pipeline.py fail loudly: with 4 optional
    params plus ``*registry_diagnoses``, 6/7/8/9 inputs were all legal, which
    is exactly how the 2026-07-20 production TypeError happened — the count
    was fine, the positions were not. Diagnosis results no longer appear here
    at all; they are ``render_diagnosis_pages``' business.
    """

    def test_inputs_positionally_match_signature(self):
        pipeline = create_pipeline()
        node = next(n for n in pipeline.nodes if n.name == "generate_report")
        params = inspect.signature(node.func).parameters

        assert not any(
            p.kind is inspect.Parameter.VAR_POSITIONAL for p in params.values()
        ), (
            "generate_report grew varargs again — that reopens the "
            "'count is legal, positions are wrong' failure mode."
        )
        assert not [
            name for name, p in params.items()
            if p.default is not inspect.Parameter.empty
        ], (
            "generate_report grew a defaulted parameter — a stale inputs list "
            "would then bind silently instead of raising TypeError."
        )
        assert len(node.inputs) == len(params), (
            f"generate_report takes {list(params)} but the Node wires "
            f"{node.inputs} — positional binding would misalign."
        )
        for position, (catalog_key, param_name) in enumerate(
            zip(node.inputs, params)
        ):
            stripped = catalog_key[len("evaluation_"):] \
                if catalog_key.startswith("evaluation_") else catalog_key
            assert catalog_key == param_name or stripped == param_name, (
                f"position {position}: catalog key {catalog_key!r} would "
                f"positionally bind to parameter {param_name!r} — inputs "
                f"list and function signature are out of sync."
            )

    def test_no_diagnosis_input_reaches_generate_report(self):
        """加第六項診斷不得再動到 generate_report。

        這條是本次重構的宣稱本身。用 DIAGNOSES 動態導出，Plan 2-5 每加一項
        自動收緊。
        """
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        pipeline = create_pipeline()
        node = next(n for n in pipeline.nodes if n.name == "generate_report")
        leaked = [
            i for i in node.inputs
            if any(i == f"evaluation_{name}" for name in DIAGNOSES)
        ]
        assert leaked == [], (
            f"diagnosis results {leaked} are wired into generate_report "
            "again — they belong to render_diagnosis_pages."
        )


class TestRenderDiagnosisPagesNodeWiring:
    """診斷產物接到 ``render_diagnosis_pages``，而且只接到它。

    這個 node 的 ``*_dag_deps`` **刻意不讀值**——結果按檔名讀（見
    ``diagnosis.metric.results.load_results``）。inputs 存在的理由是
    ``Pipeline._slice_with_expansion`` 走 ``node.inputs`` 往上拉依賴：拿掉的話
    ``--only-node render_diagnosis_pages`` 不會把診斷節點拉進來，會拿舊 JSON
    安靜地重繪。所以這條測試釘的不是資料流，是**切片行為**。
    """

    def test_every_registry_diagnosis_is_wired_as_a_dependency(self):
        from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

        pipeline = create_pipeline()
        node = next(
            n for n in pipeline.nodes if n.name == "render_diagnosis_pages"
        )
        assert node.inputs == [
            "parameters", *(f"evaluation_{name}" for name in DIAGNOSES)
        ]

    def test_slicing_only_this_node_pulls_in_the_diagnosis_nodes(self):
        """真正驗切片行為，而不只是驗 inputs 字串。

        ``can_load`` 全回 False ＝ 什麼都還沒落地，切片必須把整條上游拉回來。
        """
        pipeline = create_pipeline()
        sliced, _plan = pipeline.slice_only(
            "render_diagnosis_pages", lambda name: False
        )
        names = [n.name for n in sliced.nodes]
        assert "diagnose_config_shift" in names
        assert "draw_diagnosis_sample_node" in names
```

同時更新四處清單斷言：

- `:22-31` 與 `:66-75` 的 `expected` 集合，加 `"evaluation_diagnosis_pages"`。
- `:33-43`、`:49-59` 的 `names`，在 `"diagnose_config_shift"` 與 `"generate_report"` 之間插入 `"render_diagnosis_pages"`。
- `:81-92`（compare 模式）同上，插在 `"diagnose_config_shift"` 與 `"generate_comparison_report"` 之間。

**注意 node 順序是 topological sort 的結果，不是宣告順序**（`core/pipeline.py:56-93`）。上面三個插入位置是推導出來的，**Step 4 要實跑確認**。

- [ ] **Step 2: 跑測試確認轉紅**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_pipeline.py -v
```

預期：`StopIteration`（找不到名為 `render_diagnosis_pages` 的 node）以及 node 名單斷言 fail。
**實際訊息與此不同 → 停下回報。**

- [ ] **Step 3: 實作**

`nodes_spark.py`，在 `_diagnosis_pages_dir` 之後、`generate_report` 之前加：

```python
def render_diagnosis_pages(parameters: dict, *_dag_deps) -> list[str]:
    """把已落地的診斷 JSON 組成多頁 HTML，回傳寫出的檔案路徑。

    ``*_dag_deps`` **刻意不讀值**。它們是 ``evaluation_<name>`` 那些診斷產物，
    在這裡只當 DAG 的 happens-before 邊：``Pipeline._slice_with_expansion``
    走 ``node.inputs`` 往上拉依賴，拿掉的話 ``--only-node
    render_diagnosis_pages`` 不會把診斷節點拉進來，會拿上一次執行留下的 JSON
    安靜地重繪。

    結果本身按**檔名**讀（``diagnosis/<name>.json``），與離線工具
    ``scripts/render_diagnosis.py`` 共用 ``diagnosis.metric.results.load_results``。
    為什麼不用位置對應：Plan 2-5 每加一項診斷都要補一條 ``catalog.yaml``
    entry；忘了補的話位置對應會安靜地走記憶體——頁面正常產出、磁碟上卻沒有那
    份 JSON，離線重繪少一頁而沒有任何訊息。按檔名讀則當場進 ``missing``。

    **刻意不吞例外**：寫頁失敗直接紅，比「報表產出了、但少了診斷入口」好認
    ——後者要比對兩次執行的 HTML 才看得出來。
    """
    from recsys_tfb.diagnosis.metric.results import load_results
    from recsys_tfb.evaluation.report_builder import assemble_diagnosis_pages

    out_dir = _diagnosis_pages_dir(parameters)
    results, missing, unknown = load_results(out_dir)
    if missing:
        logger.info(
            "diagnosis results not on disk, no page for: %s",
            ", ".join(missing),
        )
    if unknown:
        logger.info(
            "JSON files outside the diagnosis registry, ignored: %s",
            ", ".join(unknown),
        )
    pages = assemble_diagnosis_pages(results, parameters, out_dir)
    logger.info(
        "diagnosis pages written to %s (%d files from %d results)",
        out_dir, len(pages), len(results),
    )
    return [str(p) for p in pages]
```

`generate_report` 改簽章（**移除 `*registry_diagnoses` 與所有預設值**）：

```python
def generate_report(
    eval_predictions: SparkDataFrame,
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict],
    metric_ci: Optional[dict],
    offset_sweep: Optional[dict],
    pair_ledger: Optional[dict],
    diagnosis_pages: Optional[list],
) -> str:
```

刪掉 `:582-599` 那段（`from ... import DIAGNOSES` 到 `logger.info("diagnosis pages written...")`），`assemble_report` 的 `diagnosis_pages=` 直接傳參數進來的值。docstring 第二段（講 `registry_diagnoses` 的那段）整段刪除，換成一句：

```
    診斷頁由 ``render_diagnosis_pages`` 產生（Plan 1.5 拆出），這裡只收它回傳
    的路徑清單、放一個連結進主報表。
```

`pipeline.py`：把 `:120-136` 換成

```python
        Node(
            diagnose_config_shift,
            inputs=["diagnosis_sample", "parameters"],
            outputs="evaluation_config_shift",
        ),
        # inputs 裡的診斷產物**只當依賴宣告**，node 本身按檔名讀（見
        # nodes_spark.render_diagnosis_pages 的 docstring）。列出它們是為了讓
        # --only-node 的切片擴張能往上拉到診斷節點。
        Node(
            render_diagnosis_pages,
            inputs=["parameters",
                    *(f"evaluation_{name}" for name in DIAGNOSES)],
            outputs="evaluation_diagnosis_pages",
        ),
        Node(
            generate_report,
            inputs=["eval_predictions", "evaluation_metrics",
                    "parameters", "baseline_metrics", "evaluation_metric_ci",
                    "evaluation_offset_sweep", "evaluation_pair_ledger",
                    "evaluation_diagnosis_pages"],
            outputs="evaluation_report",
        ),
```

import 區加 `render_diagnosis_pages`。

- [ ] **Step 4: 實跑確認 node 順序，再跑測試**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
for label, kw in [('default', {}), ('compare', {'compare_source': {'model_version': 'x'}})]:
    print(label, [n.name for n in create_pipeline(**kw).nodes])
"
```

預期 default（11 項）：`prepare_eval_data, draw_diagnosis_sample_node, compute_metrics, compute_baseline_metrics, persist_eval_predictions, compute_metric_ci, compute_offset_sweep, compute_pair_ledger, diagnose_config_shift, render_diagnosis_pages, generate_report`

**實際順序與此不同 → 以實際為準改測試，並在回報中說明差在哪。** 不要反過來改 `pipeline.py` 的宣告順序去迎合預期——宣告順序只影響同層節點的排隊次序，硬調它會讓下一個人以為順序有語意。

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_pipeline.py -v
```

- [ ] **Step 5: 修 `test_generate_report.py` 的三處呼叫**

`:49`、`:56`、`:88`、`:91` 目前是 `generate_report(sdf, metrics, params(...), None)`。簽章變成 8 個必填參數，全部補齊：

```python
generate_report(sdf, metrics, params(False), None, None, None, None, None)
```

**不要給預設值來免掉這件事**——「必填」正是這次要換來的性質。

- [ ] **Step 6: 修 `RESUME_CONTRACTS`**

`tests/test_pipelines/test_resume_contracts.py:62-70`。`generate_report` 現在多一個 memory-only 的輸入 `evaluation_diagnosis_pages`，從它接續會多自動補跑 `render_diagnosis_pages`：

```python
    ("evaluation", ()): {
        # eval_predictions/metrics are memory-only: report regeneration
        # re-runs the metric chain. Documented cost, pinned here.
        # render_diagnosis_pages is also memory-only (its output is a list of
        # paths, meaningful only for the run that wrote them) — resuming at
        # generate_report re-renders the pages from the diagnosis JSONs, which
        # is the cheap half-second path, not a Spark job.
        "generate_report": {
            "prepare_eval_data",
            "compute_metrics",
            "compute_baseline_metrics",
            "render_diagnosis_pages",
        },
    },
```

跑 `PYTHONPATH=src … -m pytest tests/test_pipelines/test_resume_contracts.py -v`。
**若實際集合與此不同 → 以實際為準，並在回報中說明多／少了什麼、為什麼。**

- [ ] **Step 7: mutation check**

把 `pipeline.py` 裡 `render_diagnosis_pages` 的 inputs 改成只剩 `["parameters"]`（拿掉診斷依賴），重跑：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_pipeline.py -v
```

預期：`test_every_registry_diagnosis_is_wired_as_a_dependency` **與**
`test_slicing_only_this_node_pulls_in_the_diagnosis_nodes` 兩條都轉紅。

**為什麼要兩條都紅**：只有前者紅代表我們只驗了字串、沒驗切片真的會壞——而
切片行為才是這些 inputs 存在的唯一理由。**若第二條沒紅 → 停下回報**，那表示
切片測試寫得不對，改測試而不是改實作。

確認後改回。

- [ ] **Step 8: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/ tests/test_pipelines/
git commit -m "refactor(evaluation): 拆出 render_diagnosis_pages，generate_report 不再收診斷"
```

---

## Task 4: 拆出 `compute_report_aggregates`，`generate_report` 變純函式

**Files:**
- Modify: `src/recsys_tfb/evaluation/diagnostics_spark.py`（加 `aggregate_report_diagnostics`）
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（加 `build_diagnostics_figures`）
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `conf/base/catalog.yaml`
- Modify: 對應測試

- [ ] **Step 1: 寫失敗的測試**

`tests/test_pipelines/test_evaluation/test_generate_report.py` 加：

```python
def test_generate_report_takes_no_spark_dataframe():
    """``generate_report`` 是純函式——這是主報表能離線重繪的前提。

    用簽章驗而不是「跑跑看有沒有用到 Spark」：後者在 diagnostics 關閉時會
    假綠（那條路徑本來就不碰 sdf）。
    """
    import inspect

    from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report

    annotations = [
        p.annotation for p in
        inspect.signature(generate_report).parameters.values()
    ]
    assert not [a for a in annotations if "SparkDataFrame" in str(a)], (
        f"generate_report 又收了 Spark 物件：{annotations}"
    )
```

`tests/test_evaluation/test_diagnostics_spark.py` 加（需要 Spark fixture，沿用該檔既有的 `_sdf` 慣例）：

檔頭 import 區加 `import json`，並把 `aggregate_report_diagnostics` 加進既有的
`from recsys_tfb.evaluation.diagnostics_spark import (...)`。

```python
def _report_sdf(spark):
    """一份足夠讓六個家族都非空的最小輸入。

    ``calibration_bins`` 會跳過「列數 < n_bins」或「沒有正例」的 item
    （diagnostics_spark.py:210-212），所以每個 item 要有 >= n_bins 列且至少
    一個正例——否則 calibration 那格是空 frame，測試看起來過了、其實什麼都
    沒量到。這裡用 n_calibration_bins=2 壓低門檻。
    """
    rows = []
    for i, item in enumerate(["insur", "loan"]):
        for k in range(4):
            rows.append((item, 0.1 + 0.2 * k, k + 1, 1 if k == i else 0))
    return _sdf(spark, rows, ["prod_name", "score", "rank", "label"])


class TestAggregateReportDiagnostics:
    def test_returns_json_safe_payload_for_every_enabled_family(self, spark):
        """六個聚合各自成為 payload 的一個鍵，且整包可嚴格序列化。

        用 ``parse_constant`` 驗嚴格性而不是掃字串：``"NaN" in text`` 會被
        item 名稱裡剛好有那三個字母的情況誤判。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
        )
        assert set(out) == {
            "columns", "score_histogram", "score_box_by_label",
            "rank_counts", "positive_rank_counts", "positive_rate",
            "calibration",
        }
        # 每個家族都要有實際資料——全空也會通過上面的鍵集合斷言。
        for key in set(out) - {"columns"}:
            assert out[key]["data"], f"{key} 是空的，這份 fixture 量不到它"

        def _boom(const):
            raise AssertionError(f"非合法 JSON 常數：{const}")

        json.loads(json.dumps(out), parse_constant=_boom)

    def test_matrix_families_keep_index_and_int_rank_columns(self, spark):
        """三個矩陣家族必須帶 index，且 rank 欄名留在 int。

        heatmap 用 ``list(matrix.index)`` 當 y 軸標籤、``list(matrix.columns)``
        當 rank 值（``distributions.py:98-105``）。丟了 index 圖照畫、只是沒有
        item 標籤；rank 變成字串 "1" 也照畫——兩種都不會有測試轉紅。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
        )
        for key in ("rank_counts", "positive_rank_counts", "positive_rate"):
            assert out[key]["kind"] == "matrix", key
            assert out[key]["index"] == ["insur", "loan"], key
            assert all(isinstance(c, int) for c in out[key]["columns"]), key

    def test_columns_are_carried_so_the_payload_renders_standalone(self, spark):
        """欄名要跟著 payload 走，不能在重繪時從 parameters 再推一次。

        離線重繪拷回來的是 JSON，不保證同一份 parameters 也拷了、更不保證那
        份 parameters 的 schema 與產生這份 JSON 時相同。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
        )
        assert out["columns"] == {
            "item": "prod_name", "score": "score",
            "rank": "rank", "label": "label",
        }

    def test_disabled_families_are_absent_not_empty(self, spark):
        """關掉的家族是「不存在」不是「空的」。

        空的看起來像「量到了、結果什麼都沒有」，那是這次重構要避免的誤讀
        （與 ``assemble_diagnosis_pages`` 對空頁的處理同一個立場）。

        兩個方向都測：只測其中一個的話，把兩個旗標接反了照樣有一條會綠。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label",
            include_calibration=False,
        )
        assert "calibration" not in out
        assert "rank_counts" in out

        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
            include_distributions=False,
        )
        assert set(out) == {"columns", "calibration"}
```

- [ ] **Step 2: 跑測試確認轉紅**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_diagnostics_spark.py -k TestAggregateReportDiagnostics \
  tests/test_pipelines/test_evaluation/test_generate_report.py -v
```
預期：`ImportError: cannot import name 'aggregate_report_diagnostics'` 與純函式那條 fail。

- [ ] **Step 3: 實作聚合層**

`diagnostics_spark.py` 加：

```python
def aggregate_report_diagnostics(
    sdf: SparkDataFrame,
    item_col: str,
    score_col: str,
    rank_col: str,
    label_col: str,
    include_distributions: bool = True,
    include_calibration: bool = True,
    n_calibration_bins: int = 10,
) -> dict:
    """報表診斷區需要的全部聚合，一次算完並轉成可落地的 dict。

    ``sdf`` 必須由呼叫端先投影並 ``cache()``：這裡每個家族各是一次 action，
    不 cache 就是 6 次全掃。

    關掉的家族**不放進 payload**（不是放空的）：空的看起來像「量到了、結果
    什麼都沒有」，那是這次重構要避免的誤讀。

    ``columns`` 跟著 payload 走，好讓這份 JSON 拷到別的環境也能單獨重繪——
    重繪端不保證拿得到同一份 ``parameters``。
    """
    out: dict = {
        "columns": {
            "item": item_col, "score": score_col,
            "rank": rank_col, "label": label_col,
        },
    }
    if include_distributions:
        out["score_histogram"] = frame_to_json(
            score_histogram_counts(sdf, item_col, score_col), _LONG)
        out["score_box_by_label"] = frame_to_json(
            score_box_stats_by_label(sdf, item_col, score_col, label_col),
            _LONG)
        out["rank_counts"] = frame_to_json(
            rank_count_matrix(sdf, item_col, rank_col), _MATRIX)
        out["positive_rank_counts"] = frame_to_json(
            positive_rank_count_matrix(sdf, item_col, rank_col, label_col),
            _MATRIX)
        out["positive_rate"] = frame_to_json(
            positive_rate_matrix(sdf, item_col, rank_col, label_col), _MATRIX)
    if include_calibration:
        out["calibration"] = frame_to_json(
            calibration_bins(sdf, item_col, score_col, label_col,
                             n_bins=n_calibration_bins), _LONG)
    return out
```

- [ ] **Step 4: 實作繪圖層（放在 Spark-free 那一側）**

`report_builder.py` 加。**放這裡而不是 `nodes_spark.py` 是刻意的**：`nodes_spark` import pyspark，把繪圖留在那裡等於主報表永遠離線重繪不了。

```python
def build_diagnostics_figures(report_aggregates: dict | None) -> list:
    """把 ``aggregate_report_diagnostics`` 的 payload 還原成圖。

    純函式、不碰 Spark——這是主報表能離線重繪的那一半。缺席的家族直接跳過
    （見該函式：關掉的家族不放進 payload）。
    """
    from recsys_tfb.evaluation.calibration import plot_calibration_curves
    from recsys_tfb.evaluation.diagnostics_spark import frame_from_json
    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )

    if not report_aggregates or not report_aggregates.get("enabled"):
        return []
    cols = report_aggregates["columns"]
    item_col, label_col = cols["item"], cols["label"]
    figs = []
    if "score_histogram" in report_aggregates:
        figs.append(plot_score_histogram(
            frame_from_json(report_aggregates["score_histogram"]),
            item_col=item_col))
        figs.append(plot_score_boxplot_by_label(
            frame_from_json(report_aggregates["score_box_by_label"]),
            item_col=item_col, label_col=label_col))
        figs.append(plot_rank_heatmap(
            frame_from_json(report_aggregates["rank_counts"])))
        figs.append(plot_positive_rank_heatmap(
            frame_from_json(report_aggregates["positive_rank_counts"])))
        figs.append(plot_positive_rate_rank_heatmap(
            frame_from_json(report_aggregates["positive_rate"])))
    if "calibration" in report_aggregates:
        figs.append(plot_calibration_curves(
            frame_from_json(report_aggregates["calibration"]),
            item_col=item_col))
    return figs
```

- [ ] **Step 5: 實作 node 與接線**

`nodes_spark.py` 加：

```python
def compute_report_aggregates(
    eval_predictions: SparkDataFrame,
    parameters: dict,
) -> dict:
    """主報表診斷區的 Spark 聚合，落地成 JSON。

    從 ``generate_report`` 拆出來（Plan 1.5）。理由不只是效能：它讓
    ``generate_report`` 變成純函式，主報表因此能離線重繪；也把這 6 次全掃的
    失敗點從 pipeline 的**最後一個 node** 往上游移。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    report_cfg = eval_params.get("report", {}) or {}
    sections_cfg = report_cfg.get("sections", {}) or {}
    diag_cfg = report_cfg.get("diagnostics", {}) or {}
    if not sections_cfg.get("diagnostics", True):
        logger.info("report diagnostics section disabled — writing stub")
        return {"enabled": False}

    schema = get_schema(parameters)
    item_col, score_col = schema["item"], schema["score"]
    rank_col, label_col = schema["rank"], schema["label"]
    needed = list(dict.fromkeys([item_col, score_col, rank_col, label_col]))
    # 每個家族各是一次 action，不 cache 就是 6 次全掃。
    sdf = eval_predictions.select(*needed).cache()
    try:
        out = aggregate_report_diagnostics(
            sdf, item_col=item_col, score_col=score_col,
            rank_col=rank_col, label_col=label_col,
            include_distributions=diag_cfg.get("include_distributions", True),
            include_calibration=diag_cfg.get("include_calibration", True),
            n_calibration_bins=diag_cfg.get("n_calibration_bins", 10),
        )
    finally:
        # 原本的寫法在例外時不會 unpersist。行為上這是純改善：輸出不變。
        sdf.unpersist()
    out["enabled"] = True
    logger.info("report aggregates computed: %s", sorted(out))
    return out
```

`generate_report` 的**最終簽章**（本計畫到此為止，之後不再變）：

```python
def generate_report(
    evaluation_metrics: dict,
    parameters: dict,
    baseline_metrics: Optional[dict],
    metric_ci: Optional[dict],
    offset_sweep: Optional[dict],
    pair_ledger: Optional[dict],
    report_aggregates: Optional[dict],
    diagnosis_pages: Optional[list],
) -> str:
```

**參數順序不是隨意的**：`TestGenerateReportNodeWiring` 要求「catalog 鍵剝掉
`evaluation_` 前綴後等於參數名，逐位對齊」。上面這個順序對應的 inputs 是

```python
        Node(
            generate_report,
            inputs=["evaluation_metrics", "parameters", "baseline_metrics",
                    "evaluation_metric_ci", "evaluation_offset_sweep",
                    "evaluation_pair_ledger", "evaluation_report_aggregates",
                    "evaluation_diagnosis_pages"],
            outputs="evaluation_report",
        ),
```

八個鍵逐位剝前綴後正好等於八個參數名。**改順序就要兩邊一起改**，只改一邊那條
測試會紅——那是它存在的目的。

`Optional[...]` 但**不給預設值**：型別上可以是 `None`，位置上不可省。少接一個
input 就是 `TypeError: missing 1 required positional argument`，而不是靜默錯位。

函式體 `:542-580` 整段（`diagnostics_frames = None` 到
`diagnostics_frames = {"figures": figs}`）換成：

```python
    diagnostics_frames = None
    figures = build_diagnostics_figures(report_aggregates)
    if figures:
        diagnostics_frames = {"figures": figures}
```

檔頭的 6 個繪圖函式 import（`:11`、`:20-26`）與 `diagnostics_spark` 的聚合 import 都移除——它們的呼叫點都不在這個檔案了。

`pipeline.py`：在 `compute_baseline_metrics` 之後加

```python
        Node(
            compute_report_aggregates,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_report_aggregates",
        ),
```

`generate_report` 的 inputs 第一個從 `"eval_predictions"` 換成 `"evaluation_report_aggregates"`，並移到 `parameters` 之後以對齊新簽章順序。

`conf/base/catalog.yaml`，在 `evaluation_config_shift` 那一段之後加：

```yaml
# 主報表診斷區的 Spark 聚合（Plan 1.5 從 generate_report 拆出）。
# 落地的理由不只是快取：generate_report 因此變成純函式，主報表可以離線重繪。
evaluation_report_aggregates:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/report_aggregates.json
```

- [ ] **Step 6: 更新測試並跑**

`test_pipeline.py`：outputs 集合加 `"evaluation_report_aggregates"`；三處 node 名單插入 `"compute_report_aggregates"`。`test_generate_report.py` 的四處呼叫改成傳 aggregates dict 而非 sdf。

實跑確認順序（同 Task 3 Step 4 的指令），預期 default 12 項、`compute_report_aggregates` 落在 `compute_baseline_metrics` 與 `persist_eval_predictions` 之間。**與實際不同以實際為準。**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_pipelines/test_evaluation/ \
  tests/test_pipelines/test_resume_contracts.py -v
```

- [ ] **Step 7: 驗證「圖沒有變」**

這是重構，**圖必須逐點相同**。跑一次對照：

把這條加進 `tests/test_evaluation/test_diagnostics_spark.py`：

```python
def test_serialisation_round_trip_leaves_every_figure_identical(spark):
    """重構前後的圖必須逐點相同。

    比對 ``fig.to_json()`` 而不是眼睛看：序列化來回若把 int rank 變成字串、
    或把 item index 丟掉，圖**照樣畫得出來**，只是軸標籤變了——沒有任何既有
    測試會轉紅。這是本 task 唯一會靜默出錯的地方。
    """
    from recsys_tfb.evaluation.calibration import plot_calibration_curves
    from recsys_tfb.evaluation.diagnostics_spark import (
        aggregate_report_diagnostics,
    )
    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )
    from recsys_tfb.evaluation.report_builder import build_diagnostics_figures

    sdf = _report_sdf(spark)
    i, s, r, lab = "prod_name", "score", "rank", "label"

    # 舊路徑：Spark 聚合的 frame 直接進繪圖函式（重構前 nodes_spark.py:553-578）
    before = [
        plot_score_histogram(score_histogram_counts(sdf, i, s), item_col=i),
        plot_score_boxplot_by_label(
            score_box_stats_by_label(sdf, i, s, lab), item_col=i,
            label_col=lab),
        plot_rank_heatmap(rank_count_matrix(sdf, i, r)),
        plot_positive_rank_heatmap(
            positive_rank_count_matrix(sdf, i, r, lab)),
        plot_positive_rate_rank_heatmap(positive_rate_matrix(sdf, i, r, lab)),
        plot_calibration_curves(
            calibration_bins(sdf, i, s, lab, n_bins=2), item_col=i),
    ]

    # 新路徑：Spark 聚合 → JSON → 繪圖函式
    payload = aggregate_report_diagnostics(
        sdf, item_col=i, score_col=s, rank_col=r, label_col=lab,
        n_calibration_bins=2,
    )
    payload["enabled"] = True
    after = build_diagnostics_figures(payload)

    assert len(after) == len(before)
    for old, new in zip(before, after):
        assert new.to_json() == old.to_json()
```

跑它：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_diagnostics_spark.py \
  -k test_serialisation_round_trip -v
```

**不相等就停下回報。** 序列化來回改變了數值或標籤是這個 task 唯一會靜默出錯的
地方，而且不會有其他測試發現它。

- [ ] **Step 8: Commit**

```bash
git add src/ conf/base/catalog.yaml tests/
git commit -m "refactor(evaluation): 6 個 Spark 聚合拆成獨立 node，generate_report 變純函式"
```

---

## Task 5: 診斷 node 由 `DIAGNOSES` 導出

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:450-488`（`diagnose_config_shift` → `make_diagnosis_node`）
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`
- Modify: `tests/test_pipelines/test_evaluation/test_nodes_spark.py:855-893`

- [ ] **Step 1: 寫失敗的測試**

改寫 `test_nodes_spark.py:855-893` 三條，並新增兩條：

```python
def test_generated_node_writes_stub_when_disabled():
    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    params = {"evaluation": {"diagnosis": {"config_shift": {"enabled": False}}}}
    assert node_fn(None, params) == {"enabled": False}


def test_generated_node_raises_when_enabled_but_sample_none():
    import pytest as _pytest

    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    node_fn = make_diagnosis_node("config_shift")
    with _pytest.raises(ValueError, match="draw_diagnosis_sample_node"):
        node_fn(None, {})


def test_generated_node_delegates_to_the_named_module(monkeypatch):
    """轉呼叫的是**以名字查到的**模組，不是寫死的 config_shift。

    工廠若把模組名寫死，registry 只有一項時**每一條測試都會照樣綠**——
    Plan 2 加第二項才會爆，而症狀是「第二項診斷的頁面印出第一項的數字」，
    每頁看起來都很正常。所以這裡注入一個假模組，用它有沒有被呼叫到來證明
    查表這件事真的發生了。
    """
    import sys
    import types

    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    called = {}
    fake = types.ModuleType("recsys_tfb.diagnosis.metric.fake_diag")

    def _compute(diagnosis_sample, parameters):
        called["sample"] = diagnosis_sample
        return {"marker": "from_fake"}

    fake.compute = _compute
    monkeypatch.setitem(
        sys.modules, "recsys_tfb.diagnosis.metric.fake_diag", fake)

    node_fn = make_diagnosis_node("fake_diag")
    sample = ("pdf-sentinel", {"sampling_description": "x"})
    out = node_fn(sample, {})

    assert out == {"marker": "from_fake"}
    # compute 拿到的是整個 tuple，不是解包後的 sample_pdf——契約在
    # contract._SIGNATURES 釘住，抄形狀時最容易改壞的就是這裡。
    assert called["sample"] is sample


def test_每項診斷的_node_名字互不相同():
    """``Node.name`` 預設取 ``func.__name__``（core/node.py:8）。

    工廠不設 ``__name__`` 的話五個 node 全叫 ``_run``：``--only-node`` 指不到
    任何一個，log 也分不出誰是誰。而 pipeline 照樣跑得完——這是靜默的。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.pipelines.evaluation.nodes_spark import make_diagnosis_node

    names = [make_diagnosis_node(n).__name__ for n in DIAGNOSES]
    assert names == [f"diagnose_{n}" for n in DIAGNOSES]
    assert len(set(names)) == len(names)
```

- [ ] **Step 2: 跑測試確認轉紅**

預期：`ImportError: cannot import name 'make_diagnosis_node'`。

- [ ] **Step 3: 實作**

`nodes_spark.py`：把 `diagnose_config_shift`（`:450-488`）整個換成

```python
def make_diagnosis_node(name: str):
    """為 registry 裡的一項診斷造一個薄 node 函式。

    Plan 2-5 的五項診斷 node 長得一模一樣：讀 ``enabled``、停用寫 stub、樣本是
    ``None`` 就 fail-loud、否則轉呼叫模組的 ``compute``。手寫五份的問題不是
    行數，是那五份會各自漂移——尤其「停用時回什麼」與「樣本 None 時 raise 還
    是靜默」這兩件事，寫錯了 pipeline 照樣跑得完。

    registry 診斷的 ``compute`` 吃的是整個 ``diagnosis_sample`` tuple
    （``(sample_pdf, sample_meta)``），不是解包後的 ``sample_pdf``——契約在
    ``diagnosis.metric.contract._SIGNATURES`` 釘住。

    ``__name__`` 明設：``Node.name`` 預設取 ``func.__name__``
    （``core/node.py:8``），不設的話五個 node 同名，``--only-node`` 指不到、
    log 分不出誰是誰，而 pipeline 照樣跑得完。
    """
    def _run(diagnosis_sample: Optional[tuple], parameters: dict) -> dict:
        cfg = (((parameters.get("evaluation", {}) or {})
                .get("diagnosis", {}) or {}).get(name, {}) or {})
        if not cfg.get("enabled", True):
            logger.info("%s disabled — writing stub", name)
            return {"enabled": False}
        if diagnosis_sample is None:
            raise ValueError(
                f"diagnose_{name}: diagnosis_sample is None while "
                f"evaluation.diagnosis.{name}.enabled is true — "
                "draw_diagnosis_sample_node gate out of sync with the "
                "consumer flag"
            )
        import importlib

        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        out = mod.compute(diagnosis_sample, parameters)
        # 純量鍵通用地印出來，不為每項診斷各寫一句摘要：那樣 Plan 2-5 每加
        # 一項就要多一段格式化字串，而它們沒有任何測試守著格式。
        scalars = {
            k: v for k, v in out.items()
            if isinstance(v, (int, float, str, bool))
        }
        logger.info("%s computed: %s", name, scalars)
        return out

    _run.__name__ = f"diagnose_{name}"
    _run.__qualname__ = f"diagnose_{name}"
    return _run
```

`pipeline.py`：`diagnose_config_shift` 的那個手寫 Node 換成

```python
        # 五項診斷的 Node 全部由 registry 導出。手寫的話 Plan 2-5 會產生四份
        # 只差模組名的複製品，而它們會各自漂移（見 make_diagnosis_node）。
        *[
            Node(
                make_diagnosis_node(name),
                inputs=["diagnosis_sample", "parameters"],
                outputs=f"evaluation_{name}",
            )
            for name in DIAGNOSES
        ],
```

import 區把 `diagnose_config_shift` 換成 `make_diagnosis_node`。

- [ ] **Step 4: 跑測試**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ -v
```

`test_pipeline.py:292-301`（釘 `diagnose_config_shift` 在 `draw_diagnosis_sample_node` 之後）應該**繼續綠**——`__name__` 設對了 node 名字就不變。**若它轉紅，第一懷疑是 `__name__` 沒設**，不要去改那條測試。

- [ ] **Step 5: mutation check**

拿掉 `_run.__name__ = f"diagnose_{name}"` 那一行，重跑 Step 4。

預期：`test_每項診斷的_node_名字互不相同` 與 `test_pipeline.py` 的 node 名單斷言都轉紅。

**為什麼下在這一行**：這是工廠化這條因果鏈上唯一不可省的一步。目前 registry
只有一項，「名字互不相同」那條在單項時恆真——所以它靠的是**與期望名字相等**
的斷言而不是去重，去重要等 Plan 2 才有鑑別力。回報時說明這一點。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/ tests/test_pipelines/test_evaluation/
git commit -m "refactor(evaluation): 診斷 Node 由 contract.DIAGNOSES 導出"
```

---

## Task 6: 單一開關 ＋ catalog entry 守衛

回應 Task 2.8 回饋 1 的第 1 條：使用者要關一項診斷時該動哪裡，目前沒有任何
地方寫。

**Files:**
- Modify: `tests/test_diagnosis/test_metric/test_contract.py`
- Modify: `src/recsys_tfb/diagnosis/metric/contract.py`（docstring）
- Modify: `conf/base/parameters_evaluation.yaml`（註解）
- Modify: `docs/pipelines/evaluation.md`

- [ ] **Step 1: 寫失敗的測試**

`tests/test_diagnosis/test_metric/test_contract.py` 加：

```python
def test_every_registry_diagnosis_has_a_catalog_entry():
    """registry 有的診斷，``catalog.yaml`` 必須有對應的 JSONDataset。

    漏掉的話 catalog 會自動建一個 MemoryDataset：pipeline 跑得完、頁面也產得
    出來，但**磁碟上沒有那份 JSON**——離線重繪少一頁，而且沒有任何訊息。
    Plan 2-5 每加一項診斷都要補一條 entry，所以這個動作會重複四次。

    連 ``type`` 一起驗：只驗 key 存在的話，寫成 MemoryDataset 照樣通過，而那
    正是要擋的東西。
    """
    from pathlib import Path

    import yaml

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    catalog_path = (Path(__file__).resolve().parents[3]
                    / "conf" / "base" / "catalog.yaml")
    catalog = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    for name in DIAGNOSES:
        key = f"evaluation_{name}"
        assert key in catalog, (
            f"{key} 不在 catalog.yaml——診斷結果不會落地，離線重繪看不到它"
        )
        assert catalog[key]["type"] == "JSONDataset", (
            f"{key} 的 type 是 {catalog[key]['type']}，"
            "非 JSONDataset 就不會有磁碟產物"
        )
        assert catalog[key]["filepath"].endswith(f"diagnosis/{name}.json"), (
            f"{key} 的 filepath 不是 diagnosis/{name}.json——"
            "render_diagnosis_pages 按檔名讀，路徑不對就讀不到"
        )
```

- [ ] **Step 2: 跑測試**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_contract.py -v
```

**這條預期一開始就是綠的**（`config_shift` 的 entry 已存在）。所以要用 mutation
證明它有效：把 `catalog.yaml` 的 `evaluation_config_shift` 暫時改名成
`evaluation_config_shift_x`，確認轉紅，再改回。**沒轉紅就停下回報。**

- [ ] **Step 3: 把兩個開關的分工寫進 `contract.py` 的 docstring**

在模組 docstring 的 `DIAGNOSES` 說明處加一段：

```
**``DIAGNOSES`` 不是使用者面的開關。** 它宣告的是「這項診斷在程式碼裡存在」
——決定 catalog 鍵、頁面編號、以及 ``render_diagnosis_pages`` 會去找哪些檔案。
使用者要關掉一項診斷，動的是 ``evaluation.diagnosis.<name>.enabled``：那條路
會讓 ``compute`` 寫一份 ``{"enabled": False}`` stub，``render`` 讀到後回空
tuple，於是那一頁不存在。

兩者的差別在**產物**：``enabled: false`` 仍會落地一份 stub JSON（看得出來
「這次刻意沒算」）；從 ``DIAGNOSES`` 移除則連檔案都不會有（看起來像「這個
版本還沒有這項診斷」）。前者是操作，後者是改版本。
```

- [ ] **Step 4: `parameters_evaluation.yaml` 的註解**

在 `evaluation.diagnosis` 段開頭加：

```yaml
  # 每項診斷只有這一個開關。關掉 => 仍會落地一份 {"enabled": false} 的 stub
  # JSON、但不產生頁面。想讓一項診斷「完全不存在」是改 code
  # （diagnosis.metric.contract.DIAGNOSES），不是改這裡。
  diagnosis:
```

- [ ] **Step 5: `docs/pipelines/evaluation.md`**

該檔 `:363`、`:387`、`:489`、`:510` 提到 `generate_report` 的 node 名稱與接線
（含一段可執行的 sanity-check 片段）。逐處對齊本計畫後的實際形狀：node 從 10
個變成 12 個、`generate_report` 不再吃 `eval_predictions`。

**逐字核對，不要憑記憶寫**：

```bash
grep -n "generate_report\|diagnose_config_shift\|eval_predictions" docs/pipelines/evaluation.md
```

- [ ] **Step 6: `known-pitfalls.md:101-111`**

該段的 sanity-check 片段引用了舊 node 名單。同步，並在段末加一行說明本計畫改
了什麼、日期 2026-07-20。

- [ ] **Step 7: Commit**

```bash
git add tests/ src/recsys_tfb/diagnosis/metric/contract.py \
        conf/base/parameters_evaluation.yaml docs/
git commit -m "docs(evaluation): 診斷只有一個使用者開關；catalog entry 加守衛"
```

---

## Task 7: 端到端 real-run ＋ 收尾

**測試綠不代表接線對** —— 本計畫動的全部是接線，而接線的錯只有真跑才看得到。

- [ ] **Step 1: 全套相關測試**

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/ tests/test_evaluation/ tests/test_report/ \
  tests/test_pipelines/ tests/scripts/ -v 2>&1 | tail -20
```

**背景執行**（可能 >2 分鐘）。貼最後 20 行。與 main 的既有 fail 對照
（known-pitfalls.md §5），不要把既有 fail 算成自己的。

- [ ] **Step 2: real-run**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb \
  evaluation --env local --post-training --model-version 6059dcef
```

**兩個旗標缺一不可**：`--post-training`（issue #63）＋ `--model-version`
（promote 是人工步驟，不得自行執行）。背景執行。

- [ ] **Step 3: 驗產物**

```bash
D=data/evaluation/6059dcef/20260131
ls -la $D/report.html $D/report_aggregates.json $D/diagnosis/
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
import json, pathlib
p = pathlib.Path("data/evaluation/6059dcef/20260131/report_aggregates.json")
def boom(c): raise AssertionError(f"非合法 JSON 常數：{c}")
d = json.loads(p.read_text(), parse_constant=boom)   # 嚴格解析
print("keys:", sorted(d))
print("columns:", d["columns"])
print("rank_counts index 前 3:", d["rank_counts"]["index"][:3])
print("rank_counts columns 前 3:", d["rank_counts"]["columns"][:3])
PY
```

驗收：
- `report_aggregates.json` 存在且**通過嚴格 JSON 解析**（無 `NaN`）。
- `rank_counts` 的 `columns` 是 **int**（`[1, 2, 3]`），不是字串。
- `diagnosis/01-config-shift.html` 仍是 7 節、尺在第 2 節且可摺疊。

- [ ] **Step 4: 比對重構前後的主報表**

重構是行為不變的改動，**主報表的診斷圖必須看起來一樣**。與重構前的
`report.html` 對照（若已被覆寫，用 `git stash` 回到 Task 3 之前重跑一次取
baseline）。

驗收：兩份 HTML 的診斷區圖表數量相同、y 軸 item 標籤相同、rank 軸標籤相同。
**不同就停下回報。**

- [ ] **Step 5: 切片行為驗證**

這是 `render_diagnosis_pages` 那些「不讀值的 inputs」存在的唯一理由，要真跑：

```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb \
  evaluation --env local --post-training --model-version 6059dcef \
  --only-node render_diagnosis_pages --dry-run
```

驗收：印出的執行計畫**包含** `diagnose_config_shift`（因為診斷 JSON 若可載入
就不會被拉進來——所以先確認 `--dry-run` 的 `can_load` 判定，並在回報中說明實
際看到什麼）。

- [ ] **Step 6: graphify rebuild**

```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 7: 更新計畫索引**

`docs/superpowers/plans/diag-redesign/README.md`：
- 進度表的 `1.5 接線層重構` 改成 ✅，附 commit 範圍。
- 「下一步」段改寫成指向 Plan 2。
- 把本計畫 §0 的兩個發現搬進 README 的踩坑段（Plan 2-5 會重複遇到）：
  1. **新增診斷 = 三個地方**：`DIAGNOSES` 一行、`catalog.yaml` 一條 entry、
     子套件本身。前兩者有測試守（Task 6），第三者有契約測試守。
  2. **node 順序是 topological sort 的結果**，新增 node 後的名單要實跑取得，
     不要用宣告順序推。

- [ ] **Step 8: Commit ＋ push**

```bash
git add -A
git commit -m "docs(plan): Plan 1.5 完成，更新進度與 Plan 2-5 的接線注意事項"
git push origin feat/diag-config-shift
```

---

## 交付後由使用者檢視什麼

1. `data/evaluation/6059dcef/20260131/report.html` —— 診斷區與重構前逐圖相同。
2. `data/evaluation/6059dcef/20260131/report_aggregates.json` —— 新產物，主報表
   離線重繪的來源。
3. `PYTHONPATH=src … -m recsys_tfb evaluation … --list-nodes` —— 12 個 node，
   職責從名字看得出來。
4. `src/recsys_tfb/pipelines/evaluation/pipeline.py` —— **診斷相關的接線只剩兩
   行導出式**，加第六項診斷不必動 `generate_report`。

## 已知但本計畫不動（備查）

- `log_experiment`（training，`inputs=10 / req=8`）與 `select_shap_population`
  （training，`inputs=4 / req=3`）具備與舊 `generate_report` 相同的「少給 input
  不會立刻爆」形狀。**沒有已知的實際故障**，且不在本計畫的範圍。要處理的話是
  獨立一件事，修法照本計畫 §0 的結論：讓簽章變成剛好個數，而不是動 core。
