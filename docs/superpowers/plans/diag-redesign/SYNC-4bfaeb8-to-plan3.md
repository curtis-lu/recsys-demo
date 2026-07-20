# 公司環境手動同步：`4bfaeb8` → Plan 3 完成點（`29952e5`）

**這一份取代 `SYNC-4bfaeb8-to-plan2.md`**——`4bfaeb8..HEAD` 的 diff 同時涵蓋 Plan 1.5、Plan 2 與 Plan 3，只要同步一次。

**前提已確認**（2026-07-21 實跑）：

```bash
git merge-base --is-ancestor 4bfaeb8 HEAD && echo "祖先 OK"
git log --oneline --merges 4bfaeb8..HEAD        # 應為空（實測：0 個）
```

`4bfaeb8` 仍是 HEAD 的祖先、中間零 merge commit（全線性），所以 `git diff 4bfaeb8..HEAD` 就是你要的東西。

---

## ⚠ 第 0 節：**六個檔案要刪除**（比上一份多了四個）

```
D  src/recsys_tfb/diagnosis/metric/discrimination.py
D  src/recsys_tfb/diagnosis/metric/pair_ledger.py
D  src/recsys_tfb/diagnosis/metric/cross_purchase.py
D  tests/test_diagnosis/test_metric/test_discrimination.py
D  tests/test_diagnosis/test_metric/test_pair_ledger.py
D  tests/test_diagnosis/test_metric/test_cross_purchase.py
```

**漏刪不會報錯**，這是它危險的地方。三個舊模組各自算的東西，都已被新診斷取代且**答案不同**：

| 舊檔 | 被誰取代 | 為什麼數字會不一樣 |
|---|---|---|
| `discrimination.py` | `item_ability` | 舊的算**校準後**分數，新的算 `score_uncalibrated`（log-odds 空間） |
| `pair_ledger.py` | `suppression` | 同一個概念，但新版分攤邏輯向量化並補了會計恆等式；輸出鍵不同 |
| `cross_purchase.py` | `suppression.cross_purchase_stats` | **母體從 `label_table` 全量改成診斷抽樣**，且改報 lift 而非裸條件機率 |

留著舊檔，哪天有人 import 到它，會得到一個「看起來合理但跟報表對不起來」的數字，而且查不出為什麼——因為兩者都有道理。

每次同步都跑一次這個取得刪除清單：

```bash
git diff --name-status --find-renames 4bfaeb8..HEAD -- src conf scripts tests | grep -E '^[DR]'
```

## 第 1 節：可整檔覆蓋（純程式碼）

```bash
git diff --stat 4bfaeb8..HEAD -- src scripts
```

26 個檔、+3746/−822。`src/` 與 `scripts/` 無環境專屬內容，直接用新版覆蓋。

**新增的整個子套件**（公司環境目前沒有這些目錄）：

```
src/recsys_tfb/diagnosis/metric/item_ability/{__init__.py,_compute.py,_render.py}
src/recsys_tfb/diagnosis/metric/model_capacity/{__init__.py,_compute.py,_render.py}
src/recsys_tfb/diagnosis/metric/suppression/{__init__.py,_compute.py,_render.py}
src/recsys_tfb/diagnosis/metric/results.py
```

## 第 2 節：`conf/` —— ⚠ **這次不再是純新增**

上一份寫「兩個檔、22 行，全部是純新增，沒動任何既有行」。**這一份不能照抄**：Plan 3 刪掉了 `pair_ledger` 的 config，所以 `conf/` 現在是 **+51/−17**。

```bash
git diff 4bfaeb8..HEAD -- conf
```

### 2a. `conf/base/catalog.yaml`

**新增四條 `JSONDataset` entry**：

```yaml
evaluation_item_ability:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/item_ability.json

evaluation_model_capacity:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/model_capacity.json

evaluation_suppression:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/suppression.json

evaluation_report_aggregates:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/report_aggregates.json
```

**刪除一條**：

```yaml
evaluation_pair_ledger:        # ← 整段刪掉
  type: JSONDataset
  filepath: .../diagnosis/pair_ledger.json
```

另有 `gain_ledger` 上方註解的文字更新（純註解，不影響行為，可略）。

**漏掉新增那幾條的後果**：pipeline 仍跑得完，但 JSON **不落地**（catalog 自動建 MemoryDataset），離線重繪拿不到資料。本機有測試擋（`test_every_registry_diagnosis_has_a_catalog_entry`），公司環境靠第 3 節的腳本擋。

### 2b. `conf/base/parameters_evaluation.yaml`

**新增三個診斷開關**（`evaluation.diagnosis` 底下）：

```yaml
    item_ability:
      enabled: true
      top_n: 30

    model_capacity:
      enabled: true

    suppression:
      enabled: true
      top_examples: 50
```

**刪除**：
- `evaluation.report.sections.pair_ledger: true` 那一行
- `evaluation.diagnosis.pair_ledger:` 整段（含它上方 5 行註解）
- `debug_inject_offsets` 上方註解裡的「（offset_sweep＋pair_ledger）」要改成「（offset_sweep）」

## 第 3 節：同步完成後的驗證（**跑 pipeline 之前**）

秒級完成。pipeline 撞到問題要等 Spark cold start 2–4 分鐘，先跑這個便宜得多。

```bash
PYTHONPATH=src python - <<'PY'
import importlib, importlib.util, pathlib, yaml
from recsys_tfb.diagnosis.metric import contract
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline

# (1) registry 應為四項，順序即頁面編號
print("DIAGNOSES:", contract.DIAGNOSES)
assert contract.DIAGNOSES == ("config_shift", "item_ability",
                              "model_capacity", "suppression"), "!! registry 不對"

# (2) 每項診斷的 INPUTS 與 compute 簽章必須對齊（contract.INPUTS 機制）
for name in contract.DIAGNOSES:
    mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
    contract.check_module(mod)                      # 不符會 raise
    print(f"  {name:16s} INPUTS={contract.inputs_for(mod)}")

# (3) node 數與拓撲：model_capacity 必須在 item_ability 之後
names = [n.name for n in create_pipeline().nodes]
print("node 數:", len(names), "（本機 real-run 是 14）")
assert "diagnose_suppression" in names, "!! suppression node 沒生出來"
assert "compute_pair_ledger" not in names, "!! 第 0 節沒做完，舊 node 還在"
i_ab, i_mc = names.index("diagnose_item_ability"), names.index("diagnose_model_capacity")
assert i_mc > i_ab, "!! model_capacity 沒排在 item_ability 之後 → INPUTS 沒進 node.inputs"

# (4) catalog entry 四條都在、舊的那條已移除
cat = yaml.safe_load(pathlib.Path("conf/base/catalog.yaml").read_text())
for key in ("evaluation_report_aggregates", "evaluation_item_ability",
            "evaluation_model_capacity", "evaluation_suppression"):
    print(f"  catalog {key}:", "OK" if key in cat else "!! 忘了插入第 2a 節那條")
assert "evaluation_pair_ledger" not in cat, "!! 第 2a 節的刪除沒做"

# (5) 刪除的三個模組確實不存在
for gone in ("discrimination", "pair_ledger", "cross_purchase"):
    spec = importlib.util.find_spec(f"recsys_tfb.diagnosis.metric.{gone}")
    print(f"  {gone}.py 已移除:", "OK" if spec is None else "!! 第 0 節沒做，舊檔還在")
PY
```

**全部 OK 才跑 pipeline。** 任何一項 `!!` 都代表某個檔沒拷到或拷錯版本。

## 第 4 節：跑起來之後看什麼

```bash
PYTHONPATH=src python -m recsys_tfb evaluation <你的既有旗標>
```

本機 real-run 實測（2026-07-21，654 queries）：**14 節點、50.4 秒**。新產物：

| 路徑 | 是什麼 |
|---|---|
| `diagnosis/02-item-ability.html` | raw vs query-centered AUC 對照散點（含 y=x 對角線） |
| `diagnosis/03-model-capacity.html` | gain 三分 ＋ capacity vs ability 散點 |
| `diagnosis/04-suppression.html` | **新**。壓制矩陣熱圖 ＋ 共買 lift 泡泡格圖（同軸序）＋ 案例表 ＋ per-suppressor 條圖 |
| `diagnosis/suppression.json` | 上頁的資料來源，可拷回本機 2.6 秒重繪 |
| `report_aggregates.json` | Plan 1.5 的產物（主報表診斷區的 Spark 聚合） |

log 裡值得確認的兩行：

```
diagnose_suppression   ... suppression: n_pairs=... n_positive_rows=... n_misordered_pairs=...
render_diagnosis_pages ... JSON files outside the diagnosis registry, ignored: metric_ci, offset_sweep
```

第二行如果**還列出 `pair_ledger`**，那只是舊執行留下的 JSON 檔還躺在目錄裡（無害，不會產生頁面）。想清乾淨就手動刪 `diagnosis/pair_ledger.json`。

### 主報表 `report.html` 會少一個區塊

本機比對（正規化掉 plotly UUID 與時間戳之後）確認差異**恰好**是：

- 少了「壓制帳本 Pair ledger」整個 h2 區塊（含 h3：壓制者邊際、傷害 × segment、Substitution ablation）
- 目錄後續編號往前遞補（section-8 以後各減一）
- 「本次寫出 3 頁」→「本次寫出 4 頁」

**零新增內容。** 若你在公司環境看到其他差異，那是誤傷，回報。

### `suppression` 的成本

本機 654 queries 下 `diagnose_suppression` 是 **0.04 秒**（1273 個成對）。公司規模（≈25 萬 query）的成對數估計 250 萬–500 萬，記憶體約 120–240 MB 在 driver 上。**這是估的，不是量的**——請把 log 裡那行 `n_pairs=` 的實際數字記下來回報，它是判斷要不要進一步優化的唯一依據。

## 第 5 節：若要回退

刪除的六個檔從 `4bfaeb8` 取回：

```bash
for f in src/recsys_tfb/diagnosis/metric/{discrimination,pair_ledger,cross_purchase}.py; do
  git show "4bfaeb8:$f" > "$f"
done
```

其餘＝把第 1 節的檔案換回舊版、`conf/` 的新增拿掉、刪除的加回。已落地的 `item_ability.json`／`model_capacity.json`／`suppression.json`／`report_aggregates.json` 留著無害（沒有人會讀它們）。
