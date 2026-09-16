---
status: accepted
date: 2026-09-16
---

# 預測品質自成一個指標家族：獨立 node 與 JSON、分箱近似、算在零正例 query 被濾掉之前

## 背景

第二種使用情境（線上廣告推薦的離線訓練與評估，見 ADR-0021）的使用者**主要不看排序指標**，而是把它當單純的二元預測問題：照預測分數分箱，看 precision、recall、F1、PR 曲線面積，以及每一箱的「預測平均 vs 實際發生率」。

框架現況（2026-09-16 核對）：

- 主指標家族是排序取向的：`map@K`、`precision@K`、`recall@K` 都先在每個 query group 算、再對 query 平均（`evaluation/metrics_spark.py`）。**整體的 AUC、logloss、PR-AUC 一個都沒有。**
- 分箱的資料**已經在算**：`evaluation/diagnostics_spark.py::calibration_bins`，每個 item 一組點（`prob_pred` vs `prob_true`），開關與 bin 數在 `conf/base/parameters_evaluation.yaml` 的 `evaluation.report.diagnostics`（`include_calibration`、`n_calibration_bins`，預設 10）。
- 但**沒有任何地方畫它**：主報表刻意不畫（`evaluation/report_builder.py`：「依『排序不是校準』，calibration 曲線移到獨立診斷報表、本段不畫」），而四份 registry 診斷（`config_shift`／`item_ability`／`model_capacity`／`suppression`）也沒有一份在讀那個鍵。資料落在 JSON 裡沒有讀者。

所以缺的主要是「家族本身」與呈現層，不是重算。

## 決定

1. **自成一個指標家族**：設定 `evaluation.prediction_quality`（**預設關**），獨立 node、獨立 JSON 產物、主報表新增一段。形狀照 `baseline` 家族抄：自己的 node（`compute_baseline_metrics`）→ 自己的產物（`baseline_metrics`，關掉時寫 `{"enabled": false}` stub）→ 自己的段落開關（`report.sections.baseline`）→ `pipelines/evaluation/steps/config_fingerprint.py::COMPUTED_KEYS` 登記兩筆。
2. **不併進 `metrics_spark::_compute_core`。** 兩個理由：計算位置不同（決定 3），而且「指標家族參數化」是 ADR-0019〈刻意沒做的事〉留給另一輪的跨三模組重構。獨立家族可以宣稱「主指標零行為改動」，驗收便宜得多。
3. **算在 `total_rel > 0` 的 filter 之前。** 主指標會先把「沒有任何正例的 query group」丟掉（`metrics_spark::_compute_core`），對排序指標是對的——mAP 對零正例的 group 無定義。但二元指標必須看到**全部**曝光：接在 filter 之後，precision 只在「有人點過」的請求上算，數字會系統性偏高。
4. **分箱近似，不做精確排序。** 一次 `groupBy(item, bin)` 聚合出每箱的列數與正例數（細 bin，預設 1000），其餘全部在 driver 上由累加得到：任何門檻的 TP／FP／FN、PR 曲線、`pr_auc`、ROC-AUC、F1 最佳門檻。**同一趟聚合供兩張圖**——細 bin 每 100 箱併成 1 箱就是分箱圖要的 10 格。代價是門檻的解析度等於 bin 寬。
5. **曲線面積叫 `pr_auc`，不叫 `average_precision`。** repo 裡 AP 已經是排序指標的名字（`map@K`、`ap_contrib@K`：一個 query 內被點的排多前面），而這裡的 AP 是 PR 曲線下的面積、跨所有列。同名放在同一份 JSON 裡會被讀錯。
6. **粒度是「整體 ＋ 每個 item」**，分群（segment）先不做。per-item 的成本只是 `groupBy` 多一欄，而「哪個 item 估得太高」是會拿來做決定的東西。
7. **報表要寫明 F1 最佳門檻是在這份資料上挑出來的**，換月份會變，不能直接搬去線上當設定值。

## 考慮過、沒選的做法

- **精確的 average precision（照 sklearn）**：要對全量列依分數排序。這個情境的量級是一年十幾億列（見 `docs/notes/2026-09-16-event-support-plan.md` 的量級估計），排序的成本換到的只是門檻解析度。
- **做成第五項 registry 診斷**：`diagnosis/metric/contract.py` 的擴充點很便宜（加一個子套件 ＋ `DIAGNOSES` 補一行，`report_builder` 零改動），但那一層吃的是**抽樣**（`diagnosis/metric/sample.py::draw_diagnosis_sample`，預設上限 25 萬 query）。使用者主要看的指標不能建在抽樣上。
- **用「組內排第一名」當「預測為正」**：那其實就是現有的 `precision@1`／`recall@1`，不需要新家族。放棄是因為使用者要的是「跨所有曝光、照分數門檻」的那把尺，不是每個請求取第一名。

## 後果

- **這個家族不需要校準。** precision、recall、F1、`pr_auc`、ROC-AUC 都只吃分數的**大小順序**。需要 training 的 calibration（`training.calibration.enabled`）才成立的只有兩件事：把分箱圖讀成「說 3% 就真的 3%」，以及把門檻跨模型或跨月份沿用。報表要把這條界線寫出來，否則會有人拿未校準的分數去訂線上門檻。
- **與既有 `calibration_bins` 重疊。** 本家族的分箱表是它的超集（bin 數可設、含每箱列數與正例數）。要不要把 `calibration_bins` 收掉、或讓它改讀本家族的輸出，留給實作決定；本 ADR 不裁，但**不接受兩份分箱同時存在而語意不同**。
- **新增報表段落要動四處**：`core/consistency.py::EVALUATION_REPORT_SECTIONS`、`conf/base/parameters_evaluation.yaml` 的 `report.sections`、`report_builder` 的讀取點、以及「常數裡每個名字都要有人讀」的來源掃描測試。不變量 A34 是雙向的，漏掉一邊會被擋下來（ADR-0019 決定 6）。
- **設定鍵一定要進 `COMPUTED_KEYS`**（`evaluation.prediction_quality` 與 `evaluation.report.sections.prediction_quality`）。沒登記的話，改了設定、已落地的指紋不動，`--only-node generate_report` 會沿用舊數字而且退出碼 0——ADR-0020 決定二要消滅的正是這個形態。
- **兩種模式都跑**（post-training 與監控），因為兩邊都有 label。宣告 `event` 的部署在監控模式被擋在入口（ADR-0021 決定 5），與本家族無關。
- 名字（`prediction_quality`、`pr_auc`）與 `CONTEXT.md` 的詞彙一起維護；新詞依 `docs/agents/domain.md` 的規則標「尚未實作」。
