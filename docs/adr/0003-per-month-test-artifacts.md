---
status: accepted
date: 2026-08-01
---

# test 產物以單一 snap_date 為鍵，而非月份集合

`test_model_input` 的 driver-local cache 原本是**整個 test split 一份**：一個目錄、一個
`_SUCCESS`，裡面裝所有設定月份。於是 cache 一旦失效，「多評估一個月份」就得整批重抄——包含
完全沒變的舊月份。

**改為一個 test 月份一個目錄**（`test_months/<YYYYMMDD>/`，見 `pipelines/training/nodes.py`
的 `_CACHE_PATH_LAYOUT`），各自持有自己的 `_SUCCESS`。加一個月＝多一個目錄，既有月份原封不動，
複製成本從 ∝N 變成 ∝1。

## 由此確立的身分規則

這不只是 cache 的排版問題，它背後是一條貫穿全域的規則：

> **依賴 test 資料的產物，身分是 `(model_version, snap_date)`；不依賴的，身分是 `model_version`。**

所以「同一個 `model_version` 在不同月份有不同的診斷產物」不是不一致，是正確的——evaluation
報表本來就落在 `data/evaluation/<model_version>/<YYYYMMDD>/`，這條規則只是把 cache 拉齊到同一
個形狀。**不為此新增任何設定鍵**：月份的唯一來源就是 `snap_date` 本身。

## 為什麼不是「視窗模型」

實作過一版視窗模型：cache 目錄帶一段排序後、底線串接的**字面月份清單**
（`test_windows/20260131_20260228/`），使「只改 `dataset.test_snap_dates`」也必然換路徑、必然
重新複製。它能正確地讓 cache 失效，但代價是把「有哪些月」壓成**一個複合鍵**——鍵裡任一個月份
變動，整個鍵就失效，於是加一個月仍然得整批重抄。

**查證過沒有任何情境是視窗模型比較安全的**：

| 情境 | 視窗模型 | per-month |
|---|---|---|
| 加月份 | 整份重抄（正確但浪費） | 只抄新月 |
| 移除月份 | 新目錄、重抄 | 舊目錄變孤兒、不被讀 |
| 舊月資料回補 | **一樣 stale**（視窗名沒變） | 一樣 stale（靠重算旗標） |

第三列是關鍵：那是視窗模型唯一可能勝出的情境，而兩者一樣壞——都得靠強制重算旗標
（[ADR-0002](0002-preprocessed-feature-table-incremental.md) 的 `--rebuild-dates`）。連這一列都
追不回來，per-month 就完全涵蓋了視窗模型要解的問題。

### 視窗模型獨有的失敗模式：目錄名可能撒謊

複製層抓的是來源表底下的所有 `snap_date`，而目錄名來自設定，**兩者從不比對**。設定加了月份、
但還沒跑 dataset 時，會建出一個名為 `20260131_20260228`、實際只有 1 月的目錄並蓋上 `_SUCCESS`，
之後永遠命中它。

per-month 下每個目錄只宣稱它自己那一個月，這整類問題不存在。

## 原本的 spec 為何排除它，那兩條理由為何不成立

issue #123 的原 spec 把 per-snap_date 粒度的 cache 列為 Out of Scope，理由兩條，現在都不成立：

- **「會多出半完成的月份等中間狀態」**——當時設想的是「單一大目錄內做 per-month 標記」。改成
  一月一目錄、各自 `_SUCCESS` 之後，半完成的月份反而比原設計更容易辨識，既有的 partial-cache
  recovery 直接適用。
- **「省下的只是一次本機複製」**——低估了，當時沒把 predict 也 ∝N 算進去。

#123 已於 2026-07-31 據此修訂。

## 這讓一道 fail-loud 變成免費的

逐月複製之後，某個月的 glob 是精確路徑而非萬用字元。`copy_hdfs_to_local`（`utils/hdfs.py`）在
`glob=True` 而零命中時本來就 `raise FileNotFoundError`，所以「設定列了某月、但來源表還沒有那個
月」——典型原因是加了月份卻忘了先跑 dataset——**自動變成明確錯誤**。

視窗模型下這需要另外寫一段覆蓋檢查（比對複製到的月份 ⊇ 設定月份），而那段檢查有偽陽性風險：
`filter_groups_with_positives` 會丟掉 label 總和為零的 query group，某個月若整月零正樣本就不會
有 partition，覆蓋檢查會把它誤報成缺資料。per-month 不必做這個取捨。

## 這條 ADR 沒有解決的事

### SHAP 診斷仍以 `model_version` 為鍵

`compute_shap_diagnostics`（`diagnosis/model/shap_per_item.py`）住在 training pipeline，吃的是
`test_parquet_handle`——**所有設定月份的聯集**，只按 item 分層，程式裡沒有任何 `snap_date`
概念；產物落在 `data/models/<model_version>/diagnostics/shap_diagnostics.json`，鍵只有
`model_version`。本次改動刻意維持它今日的語意不變。

問題出在 [ADR-0001](0001-test-dates-out-of-dataset-version-identity.md) 之後：加一個 test 月份
不再翻 `model_version`。

1. 設定多列一個月份 → `model_version` 不變 → 產物路徑不變。
2. 但 `test_parquet_handle` 的內容變了（多了那個月的列）。
3. 重跑 full training → 同一個檔案被覆寫成「涵蓋更多月」的另一份 SHAP。

於是同一個 `model_version` 的 SHAP 會隨「當下設定了哪些月」而變：既不是某一個月的診斷
（是聯集），也不是模型的穩定屬性（會被之後加的月份改寫）——正好違反上面那條身分規則。

**正解是把它搬到 evaluation pipeline、改以 `(model_version, snap_date)` 為鍵**，但那是一次
pipeline 邊界重構，牽涉三件本次範圍外的事：MLflow 的記錄語意、evaluation 的模式分支、SHAP
需要的 driver-local parquet 該由誰提供。範圍界定見 issue #128 的 Out of Scope；前期調查
（接縫分堆、`--from-node` 為何走不通、四項成本盤點）見
`docs/superpowers/specs/2026-07-31-per-month-test-artifacts-design.md`。

**在那之前的緩解**：加月份的動線只跑 `--only-node predict_and_write_test_predictions`
（`docs/operations/adding-an-eval-month.md` §3，實測 3 of 21 nodes），不會執行到這個診斷節點，
既有 SHAP 不會被覆寫。**所以不建議為了加月份重跑 full training**——那就會踩到上面第 3 步。

## 順序約束：本改動必須早於 ADR-0001 的實作

若 test 日期先退出版本身分、而 cache 仍是整批一份，會得到一個**不報錯**的靜默 bug：加月份不再
翻 `base_dataset_version` → cache 路徑不變 → `_SUCCESS` 還在 → cache 命中 → 新月份永遠不會被
複製進來。

先架護欄，再拆牆。
