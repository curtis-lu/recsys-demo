# 公司環境手動同步清單：`4bfaeb8` → `6e2138d`

公司環境無法連 GitHub、且 `conf/` 帶有環境專屬設定（表名等），所以整份覆蓋不可行。
本清單把改動分成三類，**只有第 2 類需要手動逐行處理**。

**好消息先講：這個範圍內沒有任何檔案被刪除或改名。** 手動同步最常見的事故
（舊檔留在原地繼續被 import）不會發生。

---

## 1. 可整檔覆蓋（純程式碼，無環境專屬內容）

這 11 個檔直接用新版覆蓋即可。

**新增（公司環境目前沒有這個檔）：**

```
src/recsys_tfb/diagnosis/metric/results.py
```

**修改：**

```
scripts/render_diagnosis.py
src/recsys_tfb/diagnosis/metric/config_shift/_compute.py
src/recsys_tfb/diagnosis/metric/config_shift/_render.py
src/recsys_tfb/diagnosis/metric/contract.py
src/recsys_tfb/evaluation/diagnostics_spark.py
src/recsys_tfb/evaluation/report_builder.py
src/recsys_tfb/pipelines/evaluation/nodes_spark.py
src/recsys_tfb/pipelines/evaluation/pipeline.py
src/recsys_tfb/report/pages.py
```

> `nodes_spark.py` 與 `pipeline.py` 是這次改動最大的兩個檔（接線層重構）。
> **兩個必須一起換**——`pipeline.py` 引用的 `render_diagnosis_pages` /
> `compute_report_aggregates` / `make_diagnosis_node` 都是 `nodes_spark.py` 新增的，
> 只換一邊會直接 `ImportError`（這是好事，不會靜默）。

## 2. 必須手動插入，**不可覆蓋**

### `conf/base/catalog.yaml` —— 純新增，6 行

在既有的 `evaluation_config_shift:` 那一段**之後**、`evaluation_report:` 之前插入：

```yaml
# 主報表診斷區的 Spark 聚合（Plan 1.5 從 generate_report 拆出）。
# 落地的理由不只是快取：generate_report 因此變成純函式，主報表可以離線重繪。
evaluation_report_aggregates:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/report_aggregates.json
```

**沒動到任何既有行**，所以你的表名、資料庫名都不會被影響。
漏了這一段的話：pipeline 仍會跑完，但 `report_aggregates.json` 不落地（catalog
自動建 MemoryDataset），離線重繪拿不到資料。本機有測試擋
（`test_every_registry_diagnosis_has_a_catalog_entry`），但那條只驗診斷、不驗這一條。

## 3. 可選（只有註解，不影響行為）

```
conf/base/parameters_evaluation.yaml   # 3 行說明「每項診斷只有一個開關」
```

## 4. 測試檔（若公司環境會跑測試）

```
tests/test_diagnosis/test_metric/test_results.py          （新增）
tests/scripts/test_render_diagnosis.py
tests/test_diagnosis/test_metric/test_config_shift.py
tests/test_diagnosis/test_metric/test_config_shift_render.py
tests/test_diagnosis/test_metric/test_contract.py
tests/test_evaluation/test_diagnostics_spark.py
tests/test_pipelines/test_evaluation/test_generate_report.py
tests/test_pipelines/test_evaluation/test_nodes_spark.py
tests/test_pipelines/test_evaluation/test_pipeline.py
tests/test_pipelines/test_resume_contracts.py
tests/test_report/test_pages.py
```

---

## 5. 同步完成後的驗證（**跑 pipeline 之前**）

拷完檔案先跑這一段。它在秒級完成，而 pipeline 撞到問題要等 Spark cold start。

```bash
PYTHONPATH=src python - <<'PY'
import inspect
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline
from recsys_tfb.pipelines.evaluation.nodes_spark import generate_report

# (1) node 清單：應為 12 個，且含三個新／改的 node
names = [n.name for n in create_pipeline().nodes]
print("node 數:", len(names))
for expect in ("compute_report_aggregates", "render_diagnosis_pages",
               "diagnose_config_shift"):
    print(f"  {expect:28s}", "OK" if expect in names else "!! 缺少")

# (2) generate_report 必須是「剛好個數、無 varargs、無預設值、無 Spark」
ps = inspect.signature(generate_report).parameters
print("generate_report 參數數:", len(ps), "（應為 8）")
print("  varargs:", [n for n,p in ps.items() if p.kind is p.VAR_POSITIONAL] or "無")
print("  有預設值:", [n for n,p in ps.items() if p.default is not p.empty] or "無")
print("  Spark 標註:", [n for n,p in ps.items() if "pyspark" in str(p.annotation)] or "無")

# (3) 位置綁定逐位對齊（known-pitfalls §12）
node = next(n for n in create_pipeline().nodes if n.name == "generate_report")
for i, (key, param) in enumerate(zip(node.inputs, ps)):
    stripped = key[len("evaluation_"):] if key.startswith("evaluation_") else key
    mark = "OK" if (key == param or stripped == param) else "!! 錯位"
    print(f"  {i}: {key:34s} -> {param:20s} {mark}")

# (4) catalog entry 是否補上了
import yaml, pathlib
cat = yaml.safe_load(pathlib.Path("conf/base/catalog.yaml").read_text())
print("catalog evaluation_report_aggregates:",
      "OK" if "evaluation_report_aggregates" in cat else "!! 忘了插入第 2 節那段")
PY
```

**全部 OK 才跑 pipeline。** 任何一項 `!!` 都代表某個檔沒拷到或拷錯版本。

## 6. 跑起來之後看什麼

```bash
PYTHONPATH=src python -m recsys_tfb evaluation <你的既有旗標>
```

新產物（本機 real-run 實測：654 queries、12 節點 49 秒）：

| 路徑 | 是什麼 |
|---|---|
| `data/evaluation/<mv>/<snap>/report_aggregates.json` | **新**。主報表診斷區的 6 個 Spark 聚合結果 |
| `data/evaluation/<mv>/<snap>/report.html` | 主報表，內容應與同步前**一致** |
| `data/evaluation/<mv>/<snap>/diagnosis/01-config-shift.html` | 診斷頁，多了「offset 的尺」那一節 |

log 裡值得確認的兩行：

```
render_diagnosis_pages ... JSON files outside the diagnosis registry, ignored: metric_ci, offset_sweep, pair_ledger
generate_report        ... Node generate_report completed in 0.1xs
```

第一行代表按檔名讀的邏輯正常運作（三份舊診斷不在 registry，被正確忽略、不是錯誤）。
第二行代表 Spark 聚合真的搬走了——同步前它是最貴的 node 之一。

## 7. 若要回退

本次改動**沒有刪除任何檔案**，所以回退＝把第 1 節那 10 個修改的檔案換回舊版、
刪掉 `results.py`、拿掉 catalog 那 6 行。`report_aggregates.json` 留著無害
（沒有人會讀它）。
