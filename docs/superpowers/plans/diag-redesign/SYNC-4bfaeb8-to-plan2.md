> # ⛔ 這份已作廢，不要照著做
>
> **請改用 `SYNC-4bfaeb8-to-plan3.md`。**
>
> 作廢原因（2026-07-21）：Plan 3 之後有兩處與本檔**直接矛盾**，照著做會出錯——
>
> 1. **要刪的檔從 2 個變成 6 個**（多了 `pair_ledger.py`／`cross_purchase.py` 及其測試）。
> 2. 本檔第 2 節寫 `conf/` 的改動「**全部是純新增，沒動任何既有行**」——
>    Plan 3 刪掉了 `pair_ledger` 的 catalog entry 與 config 段落，現在
>    `conf/` 是 **+51/−17**。照本檔做會留下指向不存在節點的 config。
>
> 保留本檔僅供對照當時的狀態，不是待辦。

---

# 公司環境手動同步：`4bfaeb8` → Plan 2 完成點（`b008588`）

這一份取代 `SYNC-4bfaeb8-to-6e2138d.md`——**`4bfaeb8..HEAD` 的 diff 同時涵蓋 Plan 1.5 與 Plan 2**，所以只要同步一次，不必先做到 `6e2138d` 再補增量。

**前提已確認**：`4bfaeb8` 是 HEAD 的祖先，且中間**零 merge commit**（全線性），所以 `git diff 4bfaeb8..HEAD` 就是你要的東西。

```bash
git merge-base --is-ancestor 4bfaeb8 HEAD && echo "祖先 OK"
git log --oneline --merges 4bfaeb8..HEAD        # 應為空
```

---

## ⚠ 第 0 節：有兩個檔案要**刪除**（前幾份同步清單沒有這一節）

```
D  src/recsys_tfb/diagnosis/metric/discrimination.py
D  tests/test_diagnosis/test_metric/test_discrimination.py
```

**前幾份都能寫「零刪除、零改名」——這一份不行，這是唯一的行為性差異，也是手動同步最容易漏的一類。**

不刪的後果不是報錯，而是：`discrimination.py` 會繼續存在，而它算的是**同一個統計量的校準後分數版本**。哪天有人 import 到它，數字會跟新的 `item_ability` 不一致，而且看不出為什麼——因為兩者都「有道理」。

取得刪除清單的指令（往後每次同步都該跑）：

```bash
git diff --name-status --find-renames 4bfaeb8..HEAD -- src/ conf/ scripts/ tests/ | grep -E '^[DR]'
```

## 第 1 節：可整檔覆蓋（純程式碼）

```bash
git diff --stat 4bfaeb8..HEAD -- src/ scripts/
```

22 個檔、+2583/−367。`src/` 與 `scripts/` 無環境專屬內容，直接用新版覆蓋。

其中**新增的整個子套件**（公司環境目前沒有這些目錄）：

```
src/recsys_tfb/diagnosis/metric/item_ability/{__init__.py,_compute.py,_render.py}
src/recsys_tfb/diagnosis/metric/model_capacity/{__init__.py,_compute.py,_render.py}
src/recsys_tfb/diagnosis/metric/results.py
```

## 第 2 節：必須手動插入，**不可覆蓋**（`conf/` 帶你的表名）

```bash
git diff 4bfaeb8..HEAD -- conf/
```

兩個檔、共 22 行，**全部是純新增，沒動任何既有行**：

- `conf/base/catalog.yaml`：三條 `JSONDataset` entry（`evaluation_report_aggregates`、`evaluation_item_ability`、`evaluation_model_capacity`）。
- `conf/base/parameters_evaluation.yaml`：`evaluation.diagnosis` 底下三個開關（`config_shift`／`item_ability`／`model_capacity` 的 `enabled`，`item_ability` 另有 `top_n`）。

漏掉 catalog 那幾條的後果：pipeline 仍跑得完，但 JSON **不落地**（catalog 自動建 MemoryDataset），離線重繪拿不到資料。本機有測試擋（`test_every_registry_diagnosis_has_a_catalog_entry`），公司環境靠第 3 節的腳本擋。

## 第 3 節：同步完成後的驗證（**跑 pipeline 之前**）

秒級完成。pipeline 撞到問題要等 Spark cold start 2–4 分鐘，先跑這個便宜得多。

```bash
PYTHONPATH=src python - <<'PY'
import inspect, pathlib, yaml
from recsys_tfb.diagnosis.metric import contract
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline

# (1) registry 應為三項，順序即頁面編號
print("DIAGNOSES:", contract.DIAGNOSES)
assert contract.DIAGNOSES == ("config_shift", "item_ability", "model_capacity"), "!! registry 不對"

# (2) 每項診斷的 INPUTS 與 compute 簽章必須對齊（contract.INPUTS 機制）
import importlib
for name in contract.DIAGNOSES:
    mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
    contract.check_module(mod)                      # 不符會 raise
    print(f"  {name:16s} INPUTS={contract.inputs_for(mod)}")

# (3) node 數與拓撲順序（model_capacity 必須在 item_ability 之後）
names = [n.name for n in create_pipeline().nodes]
print("node 數:", len(names), "（本機 real-run 是 14）")
i_ab, i_mc = names.index("diagnose_item_ability"), names.index("diagnose_model_capacity")
assert i_mc > i_ab, "!! model_capacity 沒排在 item_ability 之後 → INPUTS 沒進 node.inputs"
print(f"  diagnose_item_ability={i_ab}  diagnose_model_capacity={i_mc}  OK")

# (4) catalog entry 三條都在
cat = yaml.safe_load(pathlib.Path("conf/base/catalog.yaml").read_text())
for key in ("evaluation_report_aggregates", "evaluation_item_ability",
            "evaluation_model_capacity"):
    print(f"  catalog {key}:", "OK" if key in cat else "!! 忘了插入第 2 節那條")

# (5) 刪除的檔案確實不存在
import importlib.util
gone = importlib.util.find_spec("recsys_tfb.diagnosis.metric.discrimination")
print("discrimination.py 已移除:", "OK" if gone is None else "!! 第 0 節沒做，舊檔還在")
PY
```

**全部 OK 才跑 pipeline。** 任何一項 `!!` 都代表某個檔沒拷到或拷錯版本。

## 第 4 節：跑起來之後看什麼

```bash
PYTHONPATH=src python -m recsys_tfb evaluation <你的既有旗標>
```

本機 real-run 實測：654 queries、14 節點、49.6 秒。新產物：

| 路徑 | 是什麼 |
|---|---|
| `diagnosis/02-item-ability.html` | **新**。raw vs query-centered AUC 對照散點（含 y=x 對角線） |
| `diagnosis/03-model-capacity.html` | **新**。gain 三分 ＋ capacity vs ability 散點 |
| `diagnosis/{item_ability,model_capacity}.json` | 上面兩頁的資料來源，可拷回本機用 `scripts/render_diagnosis.py` 2 秒重繪 |
| `report_aggregates.json` | Plan 1.5 的產物（主報表診斷區的 Spark 聚合） |

log 裡值得確認的兩行：

```
render_diagnosis_pages ... JSON files outside the diagnosis registry, ignored: metric_ci, offset_sweep, pair_ledger
generate_report        ... Node generate_report completed in 0.0Xs
```

### 特別留意：`model_capacity` 會走哪一條路

`gain_ledger` 是**跨 pipeline 的 optional 產物**（訓練側寫 `data/models/<mv>/diagnostics/gain_ledger.json`）。它有三態，三種在頁面上長得不一樣：

| 情況 | 頁面 |
|---|---|
| 完整 ledger | gain 三分圖 ＋ per-item 條圖 ＋ 散點（本機實測：item prior 48.5%／context 45.7%／未分配 5.8%） |
| **粗帳本降級**（`fallback: true`，訓練側 preprocessor 缺 item 欄的 category mapping） | 只有 item-id 那一塊；`context_gain_share` 與 `unaccounted` 是 **`None` 而不是 0**，頁面會寫出降級原因 |
| 檔案不存在／訓練側關閉 | 頁面顯示原因，不是空白 |

**公司環境很可能走第二條**（取決於 preprocessor 有沒有 item 欄的 category mapping）。看到只有一塊 gain 不要以為壞了——先看頁面上的降級說明。

## 第 5 節：若要回退

刪除的兩個檔從 `4bfaeb8` 取回：

```bash
git show 4bfaeb8:src/recsys_tfb/diagnosis/metric/discrimination.py > <目標路徑>
```

其餘＝把第 1 節的檔案換回舊版、拿掉 `conf/` 那 22 行。已落地的 `item_ability.json`／`model_capacity.json`／`report_aggregates.json` 留著無害（沒有人會讀它們）。
