---
status: accepted
date: 2026-09-16
---

# 預測品質自成一個指標家族：獨立 node 與 JSON、分箱近似、算在零正例 query 被濾掉之前

## 背景

第二種使用情境（線上廣告推薦的離線訓練與評估，見 ADR-0021）的使用者**主要不看排序指標**，而是把它當單純的二元預測問題：照預測分數分箱，看 precision、recall、F1、PR 曲線面積，以及每一箱的「預測平均 vs 實際發生率」。

框架現況（2026-09-16 核對）：

- 主指標家族是排序取向的：`map@K`、`precision@K`、`recall@K` 都先在每個 query group 算、再對 query 平均（`evaluation/metrics_spark.py`）。**整體的 AUC、logloss、PR-AUC 一個都沒有。**
- 分箱的資料**已經在算**：`evaluation/diagnostics_spark.py::calibration_bins`，每個 item 一組點（`prob_pred = avg(score)`、`prob_true = avg(label)`），在 `[0, 1]` 上等寬切，**而且會跳過「列數少於 bin 數」或「沒有正例」的 item**。開關與 bin 數在 `conf/base/parameters_evaluation.yaml` 的 `evaluation.report.diagnostics`（`include_calibration`、`n_calibration_bins`，預設 10）。
- 但**沒有任何地方畫它**：寫入端只有 `diagnostics_spark.py`；四份 registry 診斷（`config_shift`／`item_ability`／`model_capacity`／`suppression`）grep `calibration` 零命中；主報表明說不畫（`evaluation/report_builder.py`：「依『排序不是校準』，calibration 曲線移到獨立診斷報表、本段不畫」——那句自稱移去了「獨立診斷報表」，但全樹沒有那個報表）。資料落在 JSON 裡沒有讀者。
- registry 診斷那一層吃**抽樣**，而且抽樣**只取有正例的 query**（`diagnosis/metric/sample.py` 模組 docstring）。所以就算把上限拉高，二元指標建在那一層的母體也還是錯的。

## 決定

1. **自成一個指標家族**：設定 `evaluation.prediction_quality`，獨立 node、獨立 JSON 產物、主報表新增一段。形狀照 `baseline` 家族抄：node（`pipelines/evaluation/nodes.py::compute_baseline_metrics`）→ 產物（`baseline_metrics`，關掉時寫 `{"enabled": false}` stub）→ 段落開關（`report.sections.baseline`）→ `pipelines/evaluation/steps/config_fingerprint.py::COMPUTED_KEYS` 兩筆。
   **框架的 `conf/base` 預設關；廣告情境的示例 conf 預設開。** 預設關是為了不動現有部署，但如果連示例 conf 都不開，`scripts/local_e2e.sh` 走不到這條路，這個家族就永遠沒有實跑證據。
2. **不併進 `metrics_spark::_compute_core`。** 兩個理由：計算位置不同（決定 3），而且「指標家族參數化」是 ADR-0019〈刻意沒做的事〉留給另一輪的跨三模組重構。獨立家族可以宣稱「主指標零行為改動」，驗收便宜得多。
   將來真的做參數化時，要併的是「算」那一半（bin 聚合），JSON 與報表段落的邊界不動。
3. **算在 `total_rel > 0` 的 filter 之前。** 主指標會先把「沒有任何正例的 query group」整組丟掉（`metrics_spark::_compute_core`；同樣的過濾在 baseline 路徑 `compute_overall_per_item` 也有一份），對排序指標是對的——mAP 對零正例的 group 無定義。但二元指標必須看到**全部**曝光：接在 filter 之後，precision 只在「有人點過」的請求上算，數字會系統性偏高。
4. **分箱近似，不做精確排序。** 一次聚合，三個量：

   ```
   groupBy(item, bin).agg(count(*), sum(label), sum(score))
   ```

   `sum(score)` 不可省——分箱圖的「預測平均」就是它除以 count（`calibration_bins` 算的 `prob_pred` 即 `avg(score)`）。

   **bin 的定義：全域、等寬，範圍取本次資料的 `min(score)`／`max(score)`，不寫死 `[0, 1]`。**
   - 等寬（不是分位數）是為了保住兩個性質：per-item 的箱相加就是整體、細箱併成粗箱就是分箱圖（預設 1000 箱，每 100 箱併成 1 箱得到 10 格）。分位數箱會讓不同 item 的「第 3 箱」不是同一段分數，這兩個性質都會**靜默**失效。
   - 範圍取 min/max 而不是 `[0, 1]`，因為廣告的點擊率常在 0.1%–1%，分數會擠在低端；`[0, 1]` 等寬會讓幾乎所有列落進前幾箱——既有的 `calibration_bins` 就有這個毛病。取 min/max 只要在同一次掃描前多一個聚合。

   **只有 bin 邊界上的門檻算得出來，不得內插出箱內的門檻。** 從這張表能推出什麼：

   | 量 | 判定 | 誤差來源 |
   |---|---|---|
   | bin 邊界的 TP／FP／FN／TN | 精確 | 無 |
   | 該門檻的 precision／recall／F1 | 精確 | 無（精確計數之比） |
   | 每箱的預測平均與實際發生率（分箱圖） | 精確 | 無（要 `sum(score)`） |
   | PR 曲線 | 只有邊界上的點精確 | 箱內無資訊 |
   | `pr_auc` | 近似 | ①箱內正負例不可分；②**PR 空間的內插規則本身與 sklearn 的階梯式定義不同，即使箱細到單列也不會收斂到同一個數** |
   | ROC-AUC | 近似，語意乾淨 | 箱內視為同分（tie 各半），等於「分箱後分數」的精確 AUC，通常低於真值 |
   | F1 最佳門檻 | 只能在邊界的格點上取 argmax | 真正的最佳門檻可能落在箱內 |

5. **命名。** 曲線面積叫 `pr_auc`，不叫 `average_precision`：repo 裡 AP 已經是排序指標的名字（`map@K`、`ap_contrib@K`——一個 query 內被點的排多前面），同名放在同一份 JSON 會被讀錯。
   ROC-AUC 同樣有撞名問題：`diagnosis/metric/item_ability` 已經有 `raw_within_item_auc`／`query_centered_auc`，它的 `SCOPE` 明寫母體是「只含有正例的 query 的抽樣」、**不可與任何外部 AUC 比較**。本家族的 `roc_auc` 母體是全部曝光，所以報表段落必須把母體寫在數字旁邊，兩個 AUC 不得相減或並排比較。
6. **粒度是「整體 ＋ 每個 item」**，分群（segment）先不做。per-item 要有**預算旋鈕**（照 `evaluation.diagnosis.item_ability` 的 `top_n` 先例）：這個情境的 item 是廣告組合，基數可能上千（示例部署的 22 個是商銀的事實，不是廣告的），`item × 1000 箱` 回 driver 的量要有上限。
   **這不推翻 `docs/agents/deliberate-non-goals.md` 的〈per-item 指標沒有 precision〉**（那條標「永久」）。兩者是不同的量：那條講的是 `@K` 家族——`precision@K` 的分母是 K，是 per-query 的概念，攤不到單一 item 頭上（`metrics_spark.py::aggregate_per_item` 的 NOTE 就是這件事）。本家族 per-item 的 precision 分母是「該 item 在門檻以上的曝光數」，跟 K 無關。
7. **報表必須寫出三件事**，缺一個就會被讀錯：
   - F1 最佳門檻是在這份資料上挑的，**而且解析度等於 bin 寬**（要把 bin 寬印出來）；不能直接搬去線上當設定值。
   - 本段的母體是**全部曝光**，主指標段的母體是**有正例的 query group**（被排除的數量在主指標的 `n_excluded_queries`）。兩段的 precision 不可互相比較。
   - `pr_auc` 是分箱近似、內插規則與 sklearn 不同，**不可與外部算的 AP 對帳**。

## 考慮過、沒選的做法

- **精確的 average precision（照 sklearn）**：要對全量列依分數排序。換到的不只是門檻解析度，還有內插規則這個定義差——但反過來說，精確版才是能跟外部對帳的那個數。放棄的理由是成本：若登入客戶是百萬級、每人每天數次曝光，一年約十幾億列（**未量**，見 `docs/notes/2026-09-16-event-support-plan.md`〈還沒解決的風險〉），全量排序的代價換到的主要是「可對帳」這一項，而決定 7 已經改成把「不可對帳」寫進報表。
- **做成第五項 registry 診斷**：`diagnosis/metric/contract.py` 的擴充點很便宜（加一個子套件 ＋ `DIAGNOSES` 補一行，`report_builder` 零改動），但那一層吃抽樣，而且**只抽有正例的 query**。使用者主要看的指標不能建在那個母體上。
- **用「組內排第一名」當「預測為正」**：那其實就是現有的 `precision@1`／`recall@1`，不需要新家族。放棄是因為使用者要的是「跨所有曝光、照分數門檻」的那把尺。

## 後果

- **這個家族不需要校準。** precision、recall、F1、`pr_auc`、`roc_auc` 都只吃分數的**大小順序**。需要 training 的 calibration 才成立的只有兩件事：把分箱圖讀成「說 3% 就真的 3%」，以及把門檻跨模型或跨月份沿用。
- **三張分箱表要收成一張。** 既有的 `calibration_bins`、本家族的分箱表、以及規劃裡 P7c 的「分數十等份的點擊率／申辦率表」是同一種東西的三個版本。本家族的表是 `calibration_bins` 的超集（前提是聚合含 `sum(score)`，見決定 4）。**不接受兩份分箱同時存在而語意不同**：先落地的那張定形狀，後做的併進去；`calibration_bins` 要不要收掉由實作決定。
- **新增報表段落要動四處**：`core/consistency.py::EVALUATION_REPORT_SECTIONS`、`conf/base/parameters_evaluation.yaml` 的 `report.sections`、`report_builder` 的讀取點、以及「常數裡每個名字都要有人讀」的來源掃描測試。不變量 A34 是雙向的，漏掉一邊會被擋下來（ADR-0019 決定 6）。
- **設定鍵一定要進 `COMPUTED_KEYS`**（`evaluation.prediction_quality` 與 `evaluation.report.sections.prediction_quality`）。沒登記的話，改了設定、已落地的指紋不動，`--only-node generate_report` 會沿用舊數字而且退出碼 0——ADR-0020 決定二要消滅的正是這個形態。
  **代價要先講**：`config_fingerprint.py::fingerprint` 把 `COMPUTED_KEYS` 裡每個找得到的鍵都算進 sha256，而 A34 逼 conf 一定要宣告新段落，所以登記之後**每一份帶指紋的產物（`evaluation_metrics`、`baseline_metrics`、報表彙整、`metric_ci`、各診斷）指紋都會變、舊產物要重算**。這是預期行為，不是 regression；驗收時不要為了讓 diff 乾淨而把登記拿掉。
- **低正例率是這個設計最脆弱的地方。** 點擊率 0.1%–1% 時分數擠在低端，決定 4 的「範圍取 min/max」是為此；但 F1 最佳門檻仍可能落在單一箱內，所以決定 7 要求把 bin 寬印在旁邊。真的不夠用時的下一步是「分位數箱」，代價是失去 per-item 相加與併箱兩個性質——那要另開一張 ADR，不要在實作時悄悄換掉。
- **兩種模式都跑**（post-training 與監控），兩邊都有 label。宣告 `event` 的部署在監控模式被擋在入口（ADR-0021 決定 5），與本家族無關。
- 名字（`prediction_quality`、`pr_auc`、`roc_auc`）與 `CONTEXT.md` 的詞彙一起維護；新詞依 `docs/agents/domain.md` 的規則標「尚未實作」。
