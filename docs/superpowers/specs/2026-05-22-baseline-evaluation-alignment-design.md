# Baseline 對齊 Evaluation Pipeline — 設計文件

- 日期：2026-05-22
- 分支：`feat/baseline-eval-alignment`
- 狀態：設計已核可，待寫實作計畫

## 1. 背景與問題

現況的 baseline 是一條獨立 pipeline（`src/recsys_tfb/pipelines/baselines/`），由
CLI `python -m recsys_tfb baselines` 觸發，行為與 evaluation pipeline 脫鉤：

1. **snap_date 不對齊** — baseline 讀 `parameters_evaluation.yaml` 的
   `evaluation.snap_date`（單一 scalar）；但 evaluation pipeline 的
   `prepare_eval_data` 根本不讀這個值，而是從 predictions 表（post-training 下是
   `training_eval_predictions`）的實際 snap_date(s) 推導，且可為多個。
2. **分數語意** — baseline 用 `mean(label)`（正樣本率）當分數。需求是改用
   **產品申購數**（正樣本筆數）排序。
3. **客戶清單不對齊** — baseline 取「snap_date 當期 label 的 distinct cust_id」；
   evaluation 實際評估的是 predictions 表的客戶集合。兩者不同。
4. **產品清單不對齊** — baseline 在 runtime 用 `label_table.select(prod_name).distinct()`
   推導產品；training / evaluation 的權威來源是
   `conf/base/parameters.yaml` 的 `schema.categorical_values.prod_name`。
5. **接線斷裂** — `baseline_metrics` 從未被寫進任何 catalog dataset，evaluation
   pipeline 取不到它（`__main__.py:129-132` 直接塞 `None`），baseline 報告段永遠被略過。

亦有一份 `src/recsys_tfb/evaluation/baselines.py`（pandas 版產生器），`src/` 內無任何
呼叫端，僅測試引用 — 實為 dead code。

## 2. 目標

讓 baseline 成為 evaluation 報告中一個**結構上必然對齊**的比較基準：與模型 metrics
共用完全相同的 snap_date(s)、客戶集合、產品集合、labels；分數改為產品申購數。

### 範圍內

- 把 baseline 計算併入 evaluation pipeline。
- baseline 分數 = 產品在「eval snap_date 之前歷史窗口」的申購數（`sum(label)`）。
- 同時涵蓋 `--post-training` 與預設監控（monitoring）兩種模式。
- baseline_metrics 只計算報告實際用到的 `overall` + `per_item`。
- 移除獨立 `baselines` pipeline、CLI 指令、dead pandas code。
- 修好 `baseline_metrics` 的接線。

### 範圍外（不做）

- **metrics dict 的 skip-if-exists 快取**。已討論並刻意延後：屬 evaluation pipeline
  的通用加速（同時涵蓋 `evaluation_metrics` 與 `baseline_metrics`），不應綁進本 PR。
  依 repo「先實測、勿臆測」原則，先乾淨實作本設計，於 dev-cluster 量測一次
  evaluation 跑時，若 dev loop 確實過慢，再開獨立 follow-up。
- `segment_popularity` baseline — 移除，不保留。
- baseline 與其他段落（primary mAP、guardrail recall 等）的內嵌比較 — 不做。
  baseline 比較僅出現在專屬的「基準比較 Baseline」段（維持現狀）。

## 3. 核心設計

### 3.1 關鍵洞見：對 `eval_predictions` 逐列換分數

evaluation pipeline 的 `prepare_eval_data` 已產生 `eval_predictions` —— 它已內含
**模式正確**的 snap_date(s)、客戶集合、產品列、labels、segment 欄位（post-training
與 monitoring 的差異已在上游 `predictions_input` 分支處理完畢）。

baseline 定義為：**取 `eval_predictions` 的每一列，把模型 `score` 換成該產品的歷史
申購數，其餘不動**，再算 metrics。

因為是逐列改寫同一份資料：

- 需求 1（snap_date 對齊）：snap_date 直接繼承自 `eval_predictions`。
- 需求 3（客戶清單對齊）：客戶集合直接繼承自 `eval_predictions`
  （post-training → `training_eval_predictions` 客戶；monitoring → `ranked_predictions`
  客戶 = 全體客戶）。
- 需求 4（產品清單）：baseline 不再自行推導產品清單。`eval_predictions` 的產品集合
  即模型被評估的產品集合，而 config 一致性閘門 B1（`item_coverage_errors`）已保證
  `label_table` 的 item 集合 == `resolved_item_values` ==
  `schema.categorical_values.prod_name`。故產品清單**遞移地**對齊 config，無需也
  不應另做 cross-join（憑空造出無 label、無模型對應的列會破壞 delta 的公平性）。
- 需求 2（申購數）：唯一改動就是 `score` 欄。

「對齊」因此成為**資料流的性質**，而非需要人工維護的東西。

### 3.2 申購數計算

對 `eval_predictions` 中每個 distinct snap_date `S`：

1. 歷史窗口 = `label_table` 中 `time_col` 落在 `[S − lookback_months 個月, S)` 的列。
2. 申購數 = 該窗口內 `sum(label)`，依 `prod_name` 分組（label 為 0/1 INT，sum 即正
   樣本筆數 = 申購數）。
3. 無洩漏：窗口嚴格小於 `S`。
4. **Fallback**：若某 `S` 的歷史窗口無任何資料，退回用 `label_table` 全量計算，並
   `logger.warning` 提示可能有 leakage（沿用現有 baseline 的行為）。

`label_table` 的 catalog entry 無 `partition_filter`（為完整表），故 node 看得到歷史。

### 3.3 baseline frame 組裝

1. 由 `eval_predictions` 取得 distinct snap_date 清單。
2. 算出每 `(snap_date, prod_name)` 的申購數（小表，約 snap_date 數 × 8 列）。
3. `eval_predictions` 去掉舊 `score` 欄（若有 `rank`、`model_version` 等模型專屬欄
   一併去除），broadcast-join 申購數，作為新的 `score` 欄。
4. 保留 identity 欄、`label`、`item` —— 下游 slim metrics 需要它們。

**score 欄即直接填申購數**（已定案）。理由：下游 metrics 只看 `score` 誘導的「順序」、
不看數值大小，故計數值可直接當 score；baseline frame 只進 §3.4 的 slim metrics、不進
diagnostics（calibration / score 分布圖才會把 score 當機率解讀），不會誤用。

**Tie-break（已知取捨）**：申購數相同的產品，其 query 內排名由 Spark `row_number` 的
tie-break 決定（未定義、可能不可重現）。視為可接受 —— baseline 僅為比較基準，且
`metrics_spark` 對模型 metrics 本來就是同一套 tie-break 行為。

所有客戶對同一產品的申購數相同 ⇒ baseline 對每位客戶給出相同的產品排序，等同 global
popularity baseline。

### 3.4 baseline metrics 計算（slim：只算 overall + per_item）

報告中只有「基準比較 Baseline」一段用到 `baseline_metrics`，且經 `build_comparison_result`
只取兩個 key：`overall`（mAP@k 等）與 `per_item`（每產品 `hit_rate@k`）。其餘
（`per_segment`、`per_item_segment`、`macro_avg`、`category`、`dataset_overview`、
`n_queries`）報告完全沒用到。

因此 baseline **不呼叫** `compute_all_metrics`（那會多跑 category 折疊整趟
`_compute_core` + segment 切片）。改在 `metrics_spark.py` 新增一個 slim 公用函式，
組合既有的分層建構單元，只算 overall + per_item：

```
rank_within_query        # Layer 1：score desc 排出 pos
add_query_total_rel      #          每 query 的 total_rel
filter(total_rel > 0)    #          排除無正樣本的 query
add_row_contributions    #          per-row 貢獻欄
compute_per_query_metrics(..., carry_cols=[])   # Layer 2：不 carry segment
aggregate_overall        # Layer 3：overall
aggregate_per_item       #          per_item（[item] 粒度）
→ {"overall": ..., "per_item": ...}
```

跳過 `aggregate_per_segment`、`[item, seg]` 粒度的 `aggregate_per_item`、
`macro_average`、category collapse、`compute_dataset_overview`。

回傳 dict 形如 `{"overall": {...}, "per_item": {...}}`，與 `build_comparison_result`
的 `result_b.get("overall"/"per_item")` 相容。空資料情形（無正樣本 query）回傳
`{"overall": {}, "per_item": {}}`，`build_comparison_result` 的 `.get` 預設值能容忍。

### 3.5 section gating（成本控制）

若 `report.sections.baseline` 為 false，`compute_baseline_metrics` 直接回傳 `None`，
略過 §3.2–§3.4 的整段 Spark 計算。`build_baseline_section` 已能處理 `None`（回傳
`None`、不渲染該段）。

## 4. 元件與檔案異動

### 4.1 新增 / 改寫

| 檔案 | 異動 |
|---|---|
| `src/recsys_tfb/evaluation/metrics_spark.py` | 新增 slim 公用函式（暫名 `compute_overall_per_item(eval_predictions, parameters) -> dict`），組合既有 Layer-1/2/3 單元，只算 `overall` + `per_item`（§3.4）。Layer-1 序列若與 `_compute_core` 重複，可抽共用 helper（由實作計畫決定）。 |
| `src/recsys_tfb/evaluation/baselines.py` | **改寫**：移除 pandas 版產生器，改放 Spark 版純邏輯 — 計算 `(snap_date, prod_name)` 申購數、組裝 §3.3 的 baseline frame。函式保持 pure、可獨立測試。 |
| `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` | 新增 node `compute_baseline_metrics(eval_predictions, label_table, parameters) -> dict | None`，串接 §3.3 baseline frame 組裝 + §3.4 slim metrics。 |
| `src/recsys_tfb/pipelines/evaluation/pipeline.py` | 新增一個 Node（見 §5）。 |

### 4.2 移除

| 目標 | 原因 |
|---|---|
| `src/recsys_tfb/pipelines/baselines/`（整個目錄） | 併入 evaluation pipeline 後不再需要。 |
| `pipelines/__init__.py` 的 `"baselines"` 註冊項 | 同上。 |
| `__main__.py` 的 `baselines` CLI 指令 | baseline 只作為 evaluation 報告中的比較欄存在，無獨立執行語意。 |
| `__main__.py:129-132`（evaluation 時注入 `baseline_metrics=None` 的特例） | node 現在必定產出 `baseline_metrics`（值或 `None`）。 |
| `evaluation/baselines.py` 舊 pandas 產生器 | dead code（`src/` 無呼叫端）；以 §4.1 的 Spark 邏輯取代。 |
| 對應測試 | `tests/` 中針對舊 `baselines` pipeline 與舊 `evaluation.baselines` 的測試移除；改寫為新 node / 新邏輯的測試。 |

### 4.3 設定檔異動

`conf/base/parameters_evaluation.yaml` 的 `evaluation.baseline` 區塊收斂為：

```yaml
baseline:
  lookback_months: 12
```

移除 `type`（只剩 global）與 `segment_column`（segment baseline 已棄）。
`report.sections.baseline: true` 維持不變。

## 5. Pipeline 接線

evaluation pipeline 變為四個 node：

```
prepare_eval_data(predictions_input, label_table, parameters)
    -> eval_predictions

compute_metrics(eval_predictions, parameters)
    -> evaluation_metrics

compute_baseline_metrics(eval_predictions, label_table, parameters)   # 新增
    -> baseline_metrics

generate_report(eval_predictions, evaluation_metrics, parameters, baseline_metrics)
    -> evaluation_report
```

`generate_report` 的簽章已含 `baseline_metrics` 參數，無需改動；只是其來源從「`__main__`
注入的 `None`」變成「pipeline 內 node 的真實輸出」。`baseline_metrics` 為 pipeline 內
的 MemoryDataset edge，不落地、不需新增 catalog entry。

## 6. 資料流與模式

| 模式 | `eval_predictions` 來源 | baseline 客戶集合 | baseline snap_date |
|---|---|---|---|
| `--post-training` | `training_eval_predictions` ⋈ `label_table` | `training_eval_predictions` 的客戶 | predictions 表的 distinct snap_date(s) |
| 預設（monitoring） | `ranked_predictions` ⋈ `label_table` | `ranked_predictions` 的客戶（全體） | predictions 表的 distinct snap_date(s) |

兩模式皆「自動」支援：baseline node 只吃 `eval_predictions`，模式差異已在上游解決。

labels 的來源亦自動一致：post-training 下 `eval_predictions` 帶的是
`training_eval_predictions` 的 `label`（`prepare_eval_data` 已 drop 掉 `label_table`
側的 label 以免 ambiguous）；monitoring 下則為 `label_table` 的 label。baseline 與
模型 metrics 因此永遠用同一份 labels。

## 7. 錯誤處理

- **某 snap_date 無歷史窗口資料**：退回全量 `label_table` 計算 + `logger.warning`
  （§3.2 fallback）。
- **`report.sections.baseline` 關閉**：node 回傳 `None`，跳過整段 baseline 計算（§3.5）。
- **`eval_predictions` 無正樣本 query**：slim metrics 回傳 `{"overall": {}, "per_item": {}}`，
  `build_comparison_result` 的 `.get` 預設值可容忍（§3.4）。

## 8. 測試

- `evaluation/baselines.py` 純邏輯：申購數彙總、歷史窗口邊界（嚴格 `< S`）、
  fallback（無歷史→全量+warn）、`lookback_months` 邊界、多 snap_date 各自套用各自窗口。
- `metrics_spark` slim 函式：回傳只含 `overall` + `per_item` 兩 key；數值與
  `compute_all_metrics` 對同一輸入的對應 key 一致；無正樣本 query 時回傳空 dict。
- `compute_baseline_metrics` node：
  - baseline frame 的客戶／產品／snap_date／label 集合與輸入 `eval_predictions` 完全一致。
  - 所有客戶對同一產品得到相同分數（global popularity 性質）。
  - `report.sections.baseline` 關閉時回傳 `None`。
- evaluation pipeline 整合測試：post-training 與 monitoring 兩情境下，報告含
  「基準比較 Baseline」段、overall 與 per-item recall delta 計算正確。
- 沿用既有測試效能規範：單次改動只跑相關測試檔，可能逾 2 分鐘的指令以 background 執行。

## 9. 已知取捨

- baseline 對每次 `evaluation` 執行多加一段 Spark 計算，但**僅** overall + per_item
  的 core（無 category 折疊、無 segment 切片），成本遠小於「第二趟完整
  `compute_all_metrics`」；申購數彙總本身（8 個 group）可忽略。快取列為範圍外的後續
  工作（§2）。
- baseline 與模型 metrics 必須由同一份程式碼、同一次執行算出，delta 才有意義 ——
  併入同一 pipeline 正好結構性保證了這點（這也是不採「獨立 pipeline + 持久化」方案的
  主因：後者只快取了 baseline 一半且不安全）。
- 申購數相同產品的 tie-break 未定義（§3.3）；視為可接受。
- `score` 欄存放原始申購數，欄名語意（「興趣機率」）與內容名實不符；因 baseline frame
  僅進 slim metrics、不進 diagnostics，無實際誤用風險（§3.3）。
