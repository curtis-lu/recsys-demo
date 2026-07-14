# Evaluation pipeline：可觀測性 + shared diagnosis sample（設計 spec）

- 日期：2026-07-14
- 分支：`feat/eval-observability`
- 狀態：設計核可，待寫 implementation plan

## 1. 動機

使用者在 evaluation pipeline 觀察到三個低效點，經 grilling 逐一對到實際 code：

1. **`draw_diagnosis_sample` 被重複呼叫 3 次**：`compute_metric_ci`
   (`nodes_spark.py:289`)、`compute_offset_sweep` (`:386`)、`compute_pair_ledger`
   (`:427`) 各自呼叫。同 seed → 內容完全相同，但各跑一整趟 Spark 抽樣
   （2× `count` + `collect` + `toPandas`）。程式碼註解自己已承認「各自重抽、
   非共享快取，每次呼叫都是一趟 Spark 掃描」（`:368-369, :409-410`）。
2. **多個 node 讀 in-memory `eval_predictions`、`persist_eval_predictions`
   落在最後** → `--from-node <metric>` 無法跳過 `prepare_eval_data`。
3. **可善用 `.cache()`**，但需衡量記憶體風險。

## 2. 已釘死的決策（grilling 產出）

| 決策 | 結論 |
|---|---|
| 主軸 | **先觀測、量出熱點，再決定 #2/#3**（本專案「先建最便宜的驗證迴路」原則） |
| 觀測深度 | 輕量：wall-time（runner 已有）＋**免費**資料量；不加 SparkListener |
| 免費 size 定義 | pandas 走 `log_data_volume`；領域計數（`n_queries`/`n_pos_total`）直接 log；**不對 lazy Spark DataFrame `.count()`**（那會觸發 join） |
| 輸出形式 | 沿用既有 structured log（`core/logging.py` 的 `data_volume`/`step`/`node_completed` events；`JsonFormatter` 已支援 `volume`/`step` 欄位） |
| 改哪一層 | **節點呼叫點 + 既有 `core/logging.py` helper，`core/runner.py` 不動**（＝training/inference/preprocessing/model-diagnosis 全 repo 既有慣例） |
| 開關 | 一律開（免費項零 overhead） |
| 本 PR 範圍 | **觀測 ＋ #1 shared diagnosis sample** |
| 延後（第二個 PR，等量測） | #3 `.cache()`、#2 早落地/`--from-node` 跳過 prepare、Spark 感知 job/scan 歸因 |

### 現況運作邏輯（供實作者對齊，已查證）

- `eval_predictions` 在 catalog **無宣告** → `catalog.save` 自動建 MemoryDataset
  （`catalog.py:70-74`），存的是 **lazy Spark DF 物件**。
- 下游各 node `catalog.load("eval_predictions")` 回傳**同一個 lazy DF 物件、傳
  參照**（不碰 Hive）。但因 lazy 且未 cache，每個 consumer 觸發 action 都**重跑
  整條 join 計畫**。這是本 PR #1 針對的重複運算的上游背景（本 PR 不改 cache）。
- `enriched_eval_predictions` 是宣告的 HiveTableDataset（`catalog.yaml:309`），由
  最後的 `persist_eval_predictions` 寫入，default run 無下游讀它（terminal，供
  後續 `--compare-only`）。**本 PR 不動它。**

## 3. 範圍

**做**：

- 元件一：把 `draw_diagnosis_sample` 抽成一個共用 node，三個消費者改讀其輸出。
- 元件二：在新 node（與必要處）加**免費**資料量／sub-step 觀測，沿用
  `core/logging.py` 的 `log_data_volume` / `log_step`。

**不做（明確延後）**：

- `eval_predictions` 的 `.cache()`（#3）。
- `eval_predictions` 早落地 / 下游改讀 persisted 表 / `--from-node` 跳過 prepare（#2）。
- SparkListener / job・scan・input-bytes 歸因觀測。
- 不改 `core/runner.py`、不改 `enriched_eval_predictions` catalog 條目。

## 4. 元件一：shared diagnosis sample node

### 4.1 新 node

於 `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` 新增：

```
draw_diagnosis_sample_node(eval_predictions, parameters) -> diagnosis_sample
```

行為契約：

- 計算 `any_enabled` = `ci.enabled OR offset_sweep.enabled OR pair_ledger.enabled`
  ——三個 flag 用**與消費者完全相同的 key 與預設值**讀取（現況三者皆
  `cfg.get("enabled", True)`）。為避免 gate 邏輯與消費者漂移，把「三個 flag 的解析」
  收在單一 helper（例如 `_sample_consumer_flags(parameters) -> tuple[bool,bool,bool]`），
  新 node 與（可選）消費者共用同一解析。
- `any_enabled` 為 False → log 一則「all sample consumers disabled — skipping
  diagnosis sample」，回傳 `None`（not-drawn sentinel）。
- `any_enabled` 為 True → `sample_pdf, sample_meta =
  draw_diagnosis_sample(eval_predictions, parameters)`，回傳 `(sample_pdf,
  sample_meta)`。

輸出 `diagnosis_sample`：**memory-only intermediate**，catalog 無宣告（自動
MemoryDataset）。內容為 `(sample_pdf, sample_meta)` 或 `None`。

### 4.2 消費者改動

`compute_metric_ci` / `compute_offset_sweep` / `compute_pair_ledger` 三者：

- 簽名首參由 `eval_predictions` 改為 `diagnosis_sample`。
- 自身 flag disabled → 回傳既有 stub（不變）。
- 自身 flag enabled → `sample_pdf, sample_meta = diagnosis_sample`
  （由 4.1 的 OR-gate 保證此時 `diagnosis_sample` 非 `None`），其餘邏輯不變
  （bootstrap / sweep / ledger、`out["sample"] = sample_meta`）。
- 防禦性 invariant：自身 flag enabled 但 `diagnosis_sample is None` → raise
  清楚錯誤（不該發生；若發生代表 gate 與消費者解析漂移）。
- 移除各自對 `draw_diagnosis_sample` 的 import 與呼叫。

`compute_quadrant` **不動**：它吃 `evaluation_metric_ci`（dict 輸出）與
`eval_predictions`/`label_table`，本來就不呼叫 `draw_diagnosis_sample`。

### 4.3 pipeline.py 接線

於 `create_pipeline`（default 與 `--compare` 模式共用的 `nodes` 清單）：

- 新增 `Node(draw_diagnosis_sample_node, inputs=["eval_predictions",
  "parameters"], outputs="diagnosis_sample")`。
- 將三個消費者 Node 的 inputs 由 `["eval_predictions", "parameters"]` 改為
  `["diagnosis_sample", "parameters"]`。
- 其餘 Node、`--compare-only` 短 pipeline 皆不動（後者不含這三個 node）。

拓撲：`prepare_eval_data → eval_predictions → draw_diagnosis_sample_node →
diagnosis_sample → {compute_metric_ci, compute_offset_sweep,
compute_pair_ledger}`；`compute_metric_ci → evaluation_metric_ci →
compute_quadrant` 鏈保持。

### 4.4 行為保存保證

- 全部 enabled：現況 3 次抽樣 → 改後 1 次；因同 seed，`sample_pdf` 與
  `sample_meta` **內容 byte-identical**，三消費者輸出（含 `out["sample"]`）不變。
- 僅部分 enabled：抽樣次數與現況相同（現況也只有 enabled 者抽）。
- 全部 disabled：現況 0 次抽樣 → 改後 gate 回 `None`、**0 次**，三者皆回 stub。

## 5. 元件二：可觀測性（免費 size + sub-step 計時）

一律沿用 `core/logging.py` 既有 helper，**不改 runner、不新增 helper**。模板：
`diagnosis/model/shap_per_item.py:161`（`log_data_volume`）、`:88`（`log_step`）。

**MUST**：

- 在 `draw_diagnosis_sample_node` 內、抽樣完成後：
  `log_data_volume(logger, "diagnosis.sample_pdf", sample_pdf, deep=True)`
  → 免費吐 rows/cols/bytes/dtype（pandas 分支）。
- 用 `log_step` 包住抽樣本身（node 層），讓抽樣 wall-time 可歸因。

**SHOULD**（皆為 driver 端 wall-time，零額外 Spark action）：

- 在 `diagnosis/metric/sample.py::draw_diagnosis_sample` 內用 `log_step` 分段：
  `pass1_count` / `pass2_sample` / `to_pandas`，直接回答「抽樣時間花在哪」。
- 在三個 driver 端計算（`bootstrap_per_item_ci` / `sweep` / `pair_ledger`）外層
  各包一個 `log_step`，區分「抽樣 vs 下游 numpy 計算」的時間。

**MUST NOT**：

- 不對 `eval_predictions` 或任何 Spark DataFrame 呼叫 `.count()`/`log_data_volume`
  以外會觸發 action 的取值（`log_data_volume` 對 Spark DF 天生落 else 分支跳過，
  `logging.py:275-281`，符合此限制）。
- 不動 `core/runner.py`。

既有領域計數 log（`compute_metrics` 的 `n_queries`、`compute_baseline_metrics`
的 product 數、`compute_metric_ci` 的 sampled queries）維持不變。

## 6. DAG / 接續契約影響

- `RESUME_CONTRACTS`（`tests/test_pipelines/test_resume_contracts.py:62`）僅釘
  `("evaluation", ())` 的 `generate_report` 接續點，auto-included =
  `{prepare_eval_data, compute_metrics, compute_baseline_metrics}`。
- `generate_report` 的 inputs 為已落地的診斷 dict（`evaluation_metric_ci` 等，
  JSON 落地 → 可 load → 生產者不被拉回），**不含** `diagnosis_sample`。故新增
  memory-only `diagnosis_sample` **不改變此契約**。
- 驗收：`test_resume_contracts` 保持綠燈（預期無需改動契約）。若實測發現契約變動，
  停下回報，不得逕自放寬契約。

## 7. 測試（TDD）

測試落點：`tests/test_pipelines/test_evaluation/test_nodes_spark.py`（node 行為）、
`tests/test_pipelines/test_evaluation/test_pipeline.py`（DAG 接線）、
`tests/test_diagnosis/test_metric/test_sample.py`（若動 sample.py 的 log_step）。

**改動前先建 baseline**：跑 `tests/test_pipelines/test_evaluation/` +
`tests/test_diagnosis/test_metric/` 記綠燈基準（含 main 既有 fail 歸屬，見
known-pitfalls.md §5）。

RED 測試（先寫、應為紅）：

1. **證明優化（核心）**：spy `draw_diagnosis_sample`，跑三診斷節點所在的 pipeline
   切片（三者皆 enabled）→ 斷言 `draw_diagnosis_sample.call_count == 1`。
   現況會是 3 → RED。
   - **Mutation check**：把任一消費者改回自己 import+呼叫 `draw_diagnosis_sample`
     → `call_count == 2` → 該測試須轉紅（證明測到「共用」這條因果，而非裝飾）。
     mutation 下在「消費者不再自抽、改用共用輸出」這條唯一不可省的因果鏈上。
2. **行為保存**：小 fixture 下，三診斷輸出（`evaluation_metric_ci` /
   `evaluation_offset_sweep` / `evaluation_pair_ledger`，含 `out["sample"]`）
   在「共用 node」與「直接呼叫 `draw_diagnosis_sample`」兩路徑下 byte-identical。
3. **gating**：
   - 三者全 disabled → `draw_diagnosis_sample.call_count == 0`，三消費者皆回 stub。
   - 僅一者 enabled → `call_count == 1`。
4. **觀測**：caplog 斷言出現 `event=data_volume`、`volume.name=diagnosis.sample_pdf`
   且帶 `rows`/`bytes`（pandas 分支）。
   - 注意：`draw_diagnosis_sample` **內部本就合法呼叫 Spark `.count()`**
     （`sample.py:70,91`），所以「全程無 count」是錯的斷言。要驗的是**觀測程式碼
     本身不新增 Spark action**：即 `log_data_volume` 收到的 obj 是 pandas
     `sample_pdf`（走 pandas 分支、免費），而非 `eval_predictions`（Spark；即使誤傳
     也會被 helper 於 `logging.py:275-281` 跳過而不 count）。可 spy `log_data_volume`
     斷言其 `obj` 為 `pandas.DataFrame`，或斷言加觀測前後 `draw_diagnosis_sample`
     內既有的 count 次數不變。
5. **DAG 接線**：`create_pipeline()` 產出的 node 清單含 `draw_diagnosis_sample_node`，
   且三消費者 Node 的 inputs 為 `["diagnosis_sample", "parameters"]`；
   `--compare-only` 短 pipeline 不含它。

GREEN 後：`test_resume_contracts` 綠、上述 baseline 測試全綠。

## 8. 涉及檔案

- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（新 node + 三消費者簽名/內文）
- `src/recsys_tfb/pipelines/evaluation/pipeline.py`（接線）
- `src/recsys_tfb/diagnosis/metric/sample.py`（SHOULD：log_step 分段）
- 測試：`tests/test_pipelines/test_evaluation/test_nodes_spark.py`、
  `.../test_pipeline.py`、（視情況）`tests/test_diagnosis/test_metric/test_sample.py`
- 不動：`core/runner.py`、`core/logging.py`、`conf/base/catalog.yaml`

## 9. 驗收條件

- [ ] 三診斷節點皆 enabled 時，`draw_diagnosis_sample` 每次 evaluation run 只被呼叫
      1 次（RED#1 綠 + mutation 驗證）。
- [ ] 三診斷輸出行為 byte-identical（RED#2 綠）。
- [ ] gating：全 disabled → 0 抽樣；部分 enabled → 抽樣次數等於現況（RED#3 綠）。
- [ ] `data_volume name=diagnosis.sample_pdf` 事件出現、且不觸發 Spark DF count
      （RED#4 綠）。
- [ ] `create_pipeline` 接線正確（RED#5 綠）。
- [ ] `test_resume_contracts` 綠（契約未變）。
- [ ] evaluation + diagnosis.metric 相關測試相對 baseline 全綠。
- [ ] `git diff --stat` 僅涵蓋第 8 節列出的檔案，未越界。
