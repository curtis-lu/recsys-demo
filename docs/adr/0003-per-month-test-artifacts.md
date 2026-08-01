---
status: accepted
date: 2026-08-01
---

# test 產物以單一 snap_date 為鍵，而非月份集合

`test_model_input` 的 driver-local cache 原本是**整個 test split 一份**，於是「多評估一個
月份」必須整批重抄——包含完全沒變的舊月份。改為**一個 test 月份一個目錄**
（`test_months/<YYYYMMDD>/`），各自持有自己的 `_SUCCESS`：加一個月＝多一個目錄，既有月份
原封不動。

一併確立一條貫穿全域的規則：

> **依賴 test 資料的產物，身分是 `(model_version, snap_date)`；不依賴的，身分是 `model_version`。**

「同一個 `model_version` 在不同月份有不同的診斷產物」不是不一致，是正確的——就像 evaluation
報表本來就落在 `data/evaluation/<model_version>/<YYYYMMDD>/`。**不為此新增任何設定鍵**：月份
的唯一來源就是 `snap_date` 本身。

## 為什麼不是「視窗模型」

實作過一版視窗模型：cache 目錄帶一段排序後、底線串接的**字面月份清單**
（`test_windows/20260131_20260228/`），使「只改 `dataset.test_snap_dates`」也必然換路徑、必然
重新複製。它能正確地讓 cache 失效，但把「有哪些月」壓成一個複合鍵，於是加一個月必須讓整個
鍵失效、整批重抄。

**查證過沒有任何情境是視窗模型比較安全的**：

| 情境 | 視窗模型 | per-month |
|---|---|---|
| 加月份 | 整份重抄（正確但浪費） | 只抄新月 |
| 移除月份 | 新目錄、重抄 | 舊目錄變孤兒、不被讀 |
| 舊月資料回補 | **一樣 stale**（視窗名沒變） | 一樣 stale（靠重算旗標） |

第三列是關鍵：兩者一樣壞，都得靠強制重算旗標（[ADR-0002](0002-preprocessed-feature-table-incremental.md)
的 `--rebuild-dates`）。per-month 完全涵蓋視窗模型要解的問題，並把複製成本從 ∝N 變成 ∝1。

視窗模型還有一個 per-month 沒有的失敗模式：**目錄名可能撒謊**。複製層抓的是來源表底下的所有
`snap_date`，而目錄名來自設定，兩者從不比對。設定加了月份但還沒跑 dataset 時，會建出一個名為
`20260131_20260228`、實際只有 1 月的目錄並蓋上 `_SUCCESS`，之後永遠命中它。per-month 下每個
目錄只宣稱它自己那一個月，這整類問題不存在。

## 這讓一道 fail-loud 變成免費的

逐月複製之後，某個月的 glob 是精確路徑而非萬用字元。`copy_hdfs_to_local` 在 glob 零命中時本來
就 `raise FileNotFoundError`（`utils/hdfs.py`），所以「設定列了某月、但來源表還沒有那個月」
——典型原因是加了月份卻忘了先跑 dataset——**自動變成明確錯誤**。

視窗模型下這需要另外寫一段覆蓋檢查（比對複製到的月份 ⊇ 設定月份），而那段檢查有偽陽性風險：
`filter_groups_with_positives` 會丟掉 label 總和為零的群組，某個月若整月零正樣本就不會有
partition。per-month 不必做這個取捨。

## 這條 ADR 沒有解決的事

`compute_shap_diagnostics` 讀整份 test、只按 item 分層，**完全沒有 `snap_date` 概念**。本次改動
維持它今日的語意不變（讀所有設定月份的聯集），所以在 ADR-0001 讓 test 月份退出 `model_version`
之後，同一個 `model_version` 的 SHAP 產物會隨月份累積而變——既不是單月、也不是穩定的模型屬性。

依上面那條身分規則，它應該搬到 evaluation pipeline 並以 `(model_version, snap_date)` 為鍵。這是
一次 pipeline 邊界重構（牽涉 MLflow 語意、evaluation 的模式分支、以及 SHAP 需要的 driver-local
parquet 該由誰提供），**刻意不併入本次改動**。前期調查見
`docs/superpowers/specs/2026-07-31-per-month-test-artifacts-design.md`。

在那之前的緩解：包裝好的動線只跑 predict 節點切片，不會觸發診斷；**不建議重跑 full training**。

## 順序約束

本改動必須早於 [ADR-0001](0001-test-dates-out-of-dataset-version-identity.md) 的實作。若 test 日期
先退出版本身分而 cache 仍是整批一份：加月份不再翻 `base_dataset_version` → cache 路徑不變 →
`_SUCCESS` 還在 → cache 命中 → 新月份永遠不會被複製進來，**而且不報錯**。先架護欄，再拆牆。
