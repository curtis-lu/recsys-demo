# training pipeline 第二輪整理前的盤點（2026-09-27）

[ADR-0030](../adr/0030-training-second-pass-honest-adapter-shared-scoring.md) 的證據。這是**當天的快照**：行號核對於 main @ `77adca4e`，之後會過期，不隨程式碼更新。

**證據等級**（逐條標示，不得把推論當量測）：

- **讀 code**：只讀程式碼與設定推斷出的結論，沒有實際執行。
- **程式註解裡的數字**：程式作者自己寫在註解裡的量級（例如 37–89 GiB、約 2.2 億列），不是這份盤點量的。
- **本機合成資料量測（日期）**：在本機 `local[*]`、合成資料上實際跑過並記錄下來的數字，附量測日期與環境。
- **公司環境事故紀錄**：公司生產環境事故現場留下的 log 數字，非受控量測，事後無法覆核。

未特別標示等級的表格，等級寫在該表所屬小節的開頭說明或表內「現況」/「證據等級」欄。

**路徑簡寫**（第一節沿用）：`nodes.py`、`pipeline.py`、`cache_sources.py` 在 `src/recsys_tfb/pipelines/training/`；`steps/x.py` ＝ 該目錄的 `steps/`；`adapter.py` ＝ `src/recsys_tfb/models/lightgbm_adapter.py`；`base.py` ＝ `src/recsys_tfb/models/base.py`；`extract.py` ＝ `src/recsys_tfb/io/extract.py`；`handles.py` ＝ `src/recsys_tfb/io/handles.py`；`diag/x.py` ＝ `src/recsys_tfb/diagnosis/model/x.py`；`inf/nodes.py` ＝ `src/recsys_tfb/pipelines/inference/nodes.py`；`main` ＝ `src/recsys_tfb/__main__.py`；`catalog.yaml` ＝ `conf/base/catalog.yaml`。「規則 N」＝ `docs/agents/pipeline-node-design.md` 的規則 N。第二、三節多用 repo 相對路徑；第四節多用絕對路徑 `/Users/curtislu/projects/recsys_tfb/...`——沿用盤點時的寫法，未統一。

**面向代號**（第一節沿用）：①node ②重複 ③效率 ④擴充性 ⑤維護性 ⑥慣例落差。

---

## 一、程式形狀盤點

只讀程式、沒有量測；除表內特別標註「程式註解裡的數字」外，全部等級為「讀 code」。

### 1. 結論

1. 第一輪（ADR-0014）把 `nodes.py` 的 node body 整理得不錯（42 個 `# Decision`），但**決策沉在 node 以外的三個地方**：`adapter.py` 的 `prepare_train_inputs`（311 行、`prepare_lgb_train_inputs` 只是 27 行轉手）、`diagnosis/model/` 的 7 個 node（1 個 node 223 行只有 1 個 `# Decision`）、`main` 的 `training()`（260 行 training 專屬邏輯）。
2. **LightGBM 綁定散在約 13 個模組**，`ModelAdapter` 不是接縫：呼叫端傳 `lgb.Dataset`、讀 `.booster`，而 ABC 的簽章兩者都沒有。
3. 效率上最值得先驗的是四件讀 code 就看得到的：HPO 在確認「還有沒有 trial 要跑」之前就先讀 val（註解寫 37–89 GiB）；predict 每次把所有 test 列的兩個分區欄拉進 driver；`.bin` 建置與 refit 的矩陣複本讓峰值約 2 倍；`select_shap_population` 讀預測表與 `test_model_input` 沒有月份篩選。
4. dataset 第二輪帶進來、training 還沒有的慣例：開跑前契約模組（`run_contract.py`）、讀取寫明月份（規則 14）、產物格式版本常數（規則 18 的 training 版）、`collect_all_message` 統一訊息格式。
5. #418 三件：三件都還是原樣。

### 2. 依「影響 × 信心」排序的發現

影響（H／M／L）× 信心（H／M／L）。前 10 條每條一句「改了會好在哪」。表內「可能的方向」欄是盤點時記下的一句話，不是本筆記的建議。

| # | 面向 | 發現 | 改了會好在哪（一句） | 影響×信心 | 位置 | 可能的方向 |
|---|---|---|---|---|---|---|
| 1 | ②⑥③ | 測試端的四個消費者各讀不同月份範圍：`compute_test_metrics` 讀計分月份、SHAP 讀設定的 test 月份、`select_shap_population` 讀**預測表與 `test_model_input` 的全部月份**（無月份篩選） | 象限診斷的成本不再跟著預測表的歷史長，而且四個測試端產物描述的是同一批月份 | H×H | `diag/population_spark.py:52-54, 79-80, 99-100`；`catalog.yaml:79-89, 391-403`；對照 `nodes.py:1471-1478`、`diag/shap_per_item.py:142-151` | 在 node 內寫月份篩選（規則 14），範圍與 `compute_test_metrics` 同一套 |
| 2 | ①⑤ | `prepare_lgb_train_inputs` 是 27 行轉手 node，決策全在 `adapter.py` 311 行的 `prepare_train_inputs`：objective 快取分段、feature selection 子路徑、兩段過期檢查、lambdarank 丟組、group 排序、權重不進 `.bin`——規則 3 的反例形狀 | 打開 node 就讀得到「`.bin` 快取怎麼判定能不能用、訓練矩陣丟了什麼」，#418 項目 1 在結構上解掉，也拆掉 `io`↔`models` 的循環 import | H×H | `nodes.py:562-588`；`adapter.py:257-567`（`350-385` 兩段 inline 檢查）；`adapter.py:22-25`（循環 import 註解） | `.bin` 建置的決策上浮到 node，機制進 `steps/`，adapter 只保留「把陣列建成原生資料集」 |
| 3 | ④ | `ModelAdapter` 不是接縫：ABC 的 `train` 沒有 `train_dataset`／`val_dataset`，但兩個呼叫端都傳；`prepare_train_inputs` 回傳型別寫死 `LgbDatasetHandle`；`best_iteration` 與 `booster` 不在 ABC、呼叫端直接讀 | 加第二個演算法時，要改的從約 13 個模組縮到 adapter 本身加少數幾處 | H×H | `base.py:20-27, 61-75`；`steps/hpo_scoring.py:360-378, 393, 398, 408`；`nodes.py:1035-1039`；完整清單見第 6 節 | 讓 ABC 說出 training 真的在用的操作（建資料集、帶早停訓練、取最佳迭代數） |
| 4 | ③ | `tune_hyperparameters` 先把 val 串流成磁碟映射矩陣（程式註解：生產 37–89 GiB），**之後**才看 study 還剩幾個 trial；study 已完成（`remaining == 0`）且有 checkpoint 時，這次讀取完全沒用到 | 已完成的搜尋重跑時不再白讀、白寫一份 37–89 GiB 的 val | M×H | 讀取 `nodes.py:720-733`，判斷 `nodes.py:774-813`，量級註解 `nodes.py:697-706` | 先開 study、讀 checkpoint，確定要跑 trial 才讀 val（binary objective 的 val 前置檢查要跟著搬） |
| 5 | ③ | predict 為了列出 `(月份, item)`，把**所有**快取 test 列的兩個分區欄物化成 pandas（程式註解：約 2.2 億列），每次都跑，全部月份都跳過時也一樣；每個分區的讀取也沒有 `columns=` 投影 | 列分區不再讀任何一列資料，每個分區只讀模型要的欄 | M×H | `nodes.py:1126-1145`（註解 `1129-1133`）；`nodes.py:1239-1242` | 從 pyarrow fragment 的分區運算式取 `(月份, item)`；讀取投影到特徵＋entity＋label＋選用角色欄 |
| 6 | ③ | `_stream_matrix` 刻意讓峰值＝矩陣＋一批，但緊接著：ranking 分支的 `drop_zero_positive_groups`（`np.asarray(a)[mask]`）與 `X_tr[perm_tr]` 各複製一份整個矩陣；`refit_on_full` 分支有 `X_tr`＋`X_dv`＋`np.concatenate`＋`X_full[perm]`。峰值約 2 倍 | 最大的兩個矩陣（train、train＋train_dev）峰值記憶體約減半；`refit_on_full` 在生產規模可能從跑不動變成跑得動（未量） | H×M | `adapter.py:452-472, 479-500`；`core/group_utils.py:157`；`nodes.py:959-1004`；`steps/refit.py:43-45`；對照 `extract.py:844-851` | 在串流時就依 group 排序寫入、或就地套 mask／permutation |
| 7 | ⑥④ | training 沒有規則 18 的對應物：`model_version`／`search_id` 只雜湊設定，沒有程式格式版本。training 程式改了模型或 trial 分數、設定沒動時：predict 照「同一個 `model_version` 的月份不可變」跳過舊月份（新舊模型的預測混在同一個版本下），HPO 續跑把舊程式的 trial 與 checkpoint 接著用 | 程式改動改變模型時，舊月份的預測與舊 study 不會靜默混進新結果 | H×M | `core/versioning.py:252-317`（無常數）vs `:157`（dataset 有）；假設寫在 `steps/predict_months.py:3-8` | 在 `model_version`／`search_id` 的雜湊加一個 training 產物格式版本常數，規則照規則 18 |
| 8 | ⑥⑤ | training 沒有 `run_contract.py`：`main` 的 `training()` 260 行裝了 A26/A36/A53、A21、A28/A39/A45、A54、兩層版本解析、`search_id`、注入的 runtime 參數、manifest extra。`cache_sources.inject_cache_source_tables` 則在通用的 `_execute_pipeline` 裡對**每條** pipeline 都跑 | training 開跑前要什麼、跑完寫什麼可以不經 CLI 直接測，而且跟 dataset 同形 | M×H | `main:1466-1725`；`main:767`；`main:930-951` | 照 ADR-0029 決定 11，training 根層一個契約模組；`inject_cache_source_tables` 只在 training 呼叫 |
| 9 | ② | parquet→X 有兩套：train／val 走 `_stream_matrix`（B6、B9 型別閘＋串流），test 預測、inference、SHAP×4、`scripts/shap_margin_summary.py` 走 `pdf_to_X`（沒有型別閘、`pdf[feature_cols].copy()`）。延後編碼的類別欄寫了兩份 | 同一份特徵在訓練與預測走同一條編碼路徑，#418 項目 2 只要改一處 | M×H | `extract.py:933-941` vs `:987-994`；`:984`；型別閘只在 `:1049`、`:890` | 讓 `pdf_to_X` 與 `_stream_matrix` 共用「一批列 → 矩陣列」的機制 |
| 10 | ⑤ | 三個隱性順序依賴：（a） `log_experiment` 10 個輸入、後兩個靠位置加預設 `None`；（b） `gain_ledger.json` 與 `diagnostics/hpo/` 會被上傳 MLflow，只因為 Kahn 排序剛好先跑它們，沒有 DAG 邊；（c） `predict_manifest` 只當排序用（`writes=` 不產生邊） | MLflow 收到的產物由 DAG 保證完整，不靠宣告順序 | M×H | （a） `pipeline.py:209-225`、`nodes.py:1326-1337`；（b） `pipeline.py:168-172`、`catalog.yaml:286-288`、`nodes.py:1398-1400`、`core/pipeline.py:82-85`；（c） `pipeline.py:141, 184-192` | 讓上傳的每個產物都是 `log_experiment` 的輸入，或上傳改成只傳宣告過的清單 |
| 11 | ②⑥ | training 的 test 預測與 inference 的評分是同一件事、兩份機制：輸出 frame 組裝各一份；特徵清單 training 用設定導出的 `preprocessor_view`、inference 用模型自己的 `model_feature_view`；inference 有逐塊驗證（`validate_scored_chunk`、`require_single_partition`），training 沒有 | — | M×M | `nodes.py:1260-1300` vs `inf/nodes.py:334, 453-491` | 共用 frame 組裝機制；不對稱處在 docstring 寫理由（規則 16） |
| 12 | ②⑥ | `select_shap_population` 自己寫排名 window，tie-break 只有 item、沒有 event 欄；其他地方都用 `utils/ranking.rank_by_score_then_item`。宣告 `event` 時象限的 top-1 可能與 evaluation 的排名不同 | — | M×H（只在宣告 event 時） | `diag/population_spark.py:52-54`；`utils/ranking.py:21-23, 77-102` | 改用 `rank_by_score_then_item` |
| 13 | ⑤⑥ | 7 個 diagnosis node 的 `def` 仍在 `diagnosis/model/`（#418 項目 3：使用者 2026-09-20 已拍板搬回）；該目錄 docstring／註解多為中文（違反體例）；私有名跨模組 import 3 處（規則 12）；`compute_shap_diagnostics` 223 行只有 1 個 `# Decision`、用註解分隔線（規則 9） | — | M×H | 見第 3 節 #14–20；`diag/gain_ledger.py:29`、`diag/shap_cases.py:13`、`diag/shap_per_item.py:17` | #418 項目 3 的待裁決 （a）（b）（c） |
| 14 | ② | 小機制重複 10 處，逐條見第 4 節 D5–D14 | — | M×H | 見第 4 節 | 各收成一處 |
| 15 | ③ | 診斷抽樣靠整份循序掃描：`take_rows` 以 `use_threads=False` 掃完所有特徵欄直到最後一個抽中的索引。`compute_feature_statistics` 掃 train 一次，`compute_shap_diagnostics` 掃 test 兩次（全域樣本＋正例樣本），外加 item、label 欄各讀一次整欄 | — | M×M | `diag/data_access.py:54-89`；`diag/feature_stats.py:76-83`；`diag/shap_per_item.py:82-86, 163-177` | 兩次 SHAP 抽樣合成一次掃描；或按 fragment 跳讀 |
| 16 | ④ | 加一種新診斷要動 5–7 處（node、`pipeline.py`、兩份 catalog、`diagnostics.*` 設定、`log_experiment` 位置參數、`experiment_log` 摘要），而且落在 `diagnostics/` 的檔案會不會被上傳，靠 #10（b） 的排序巧合 | — | M×H | 見第 6 節 6.4 | 與 #10 一起處理 |
| 17 | ⑤ | docstring 與程式對不上 9 處（見第 7 節 7.4），含模組開頭的「21 個 node 中 14 個」（實為 20 中 13）與「五個 cache node」（實為四個） | — | L×H | 第 7 節 7.4 | 收尾一次清 |
| 18 | ③ | `compute_test_metrics` 的 `frame` 在 `count_query_groups_by_time` 與每個 metric pass 之間沒有 persist，每個 action 各讀一次 Hive | — | L×M | `nodes.py:1477-1478, 1530-1560` | 量過再決定（scored months 已篩過，可能不值得） |

### 3. 逐 node 盤點（20 個）

`pipeline.py` 建 20 個 `Node(`（`grep -c "Node(" pipeline.py` ＝ 20），13 個 `def` 在 `nodes.py`，7 個在 `diagnosis/model/`。行數用 AST（`end_lineno - lineno + 1`）量；「Decision」＝ body 裡 `# Decision` 的數量。

| # | node | 位置 | 行數 | Decision | 讀 | 寫 | 違反／問題 |
|---|---|---|---|---|---|---|---|
| 1 | `select_features` | `nodes.py:535` | 25 | 0 | `preprocessor`、`parameters` | `preprocessor_view`（memory） | 無明顯違例。A9c 的 runtime backstop 已標前置檢查 |
| 2 | `cache_train_model_input` | `nodes.py:305` | 43 | 2 | `train_model_input`（只為拿 SparkSession）、`parameters` | `train_parquet_handle`（memory）＋本機 parquet | 形狀照規則 4、5；區段註解「Five nodes」過期（`nodes.py:294`） |
| 3 | `cache_train_dev_model_input` | `nodes.py:350` | 39 | 2 | 同上 | `train_dev_parquet_handle` | 同上 |
| 4 | `cache_val_model_input` | `nodes.py:391` | 37 | 2 | 同上 | `val_parquet_handle` | 註解「longest-lived of the five」過期（`nodes.py:414`） |
| 5 | `cache_test_model_input` | `nodes.py:430` | 103 | 5 | 同上＋`--rebuild-dates` | `test_parquet_handle`（dict）＋每月一個目錄 | 規則 5：月份去重的機制與 `steps/predict_months.configured_months` 重複（`nodes.py:463-477` vs `predict_months.py:70-85`），rebuild 集合與 predict 各算一次（`nodes.py:464-466` vs `1192-1194`）；docstring「only one of the five」過期（`:441`） |
| 6 | `prepare_lgb_train_inputs` | `nodes.py:562` | 27 | 0 | train／dev handle、`preprocessor_view`、`parameters` | `train_lgb_handle`、`train_dev_lgb_handle`（memory）＋`.bin` 快取與 sidecar | **規則 3、4、2**：轉手到 `adapter.py:257` 的 311 行（見第 2 節發現 #2）；規則 5：自己組快取路徑（`:577-580`），與 `steps/local_cache._CACHE_PATH_LAYOUT` 重複，而且 `cache.root` 沒設時這裡 `KeyError`、那裡預設 `/tmp/recsys_cache`（`local_cache.py:124-125`） |
| 7 | `persist_group_filter_report` | `nodes.py:159` | 64 | 1 | `train_lgb_handle`、`parameters` | `group_filter_report.json` | 放在「# Helpers」區段標題下（`nodes.py:144-146`），但它是 node |
| 8 | `persist_sample_weight_report` | `nodes.py:225` | 63 | 2 | `train_parquet_handle`、`preprocessor_view`、`parameters` | `sample_weight_report.json` | 規則 5：weight key 自己重算（`:252`），註解自己說「必須跟 `io/extract.py` 同一組」，而那裡已有 `weight_key_columns`（`extract.py:746-753`）；decode map 也重算一次（`:259-262` vs `extract.py:482-484`）。同樣在「# Helpers」標題下 |
| 9 | `tune_hyperparameters` | `nodes.py:611` | 291 | 6 | train／dev lgb handle、`val_parquet_handle`、`preprocessor_view`、`parameters` | `best_params`、`best_iteration`、`hpo_best_model`；副作用：`data/models/_hpo/<search_id>/`、`diagnostics/hpo/`、val 暫存矩陣、**釋放 SparkSession** | 效率見第 2 節發現 #4；study 生命週期（fresh／resume／checkpoint，約 40 行，`:774-813`）是機制卻寫在 node body；`--fresh-hpo` 區塊 `except Exception: pass`（`:789-790`）；metric 預設與 `finalize_model` 重複（`:680-687`）；docstring「val 是 pandas DataFrame」過期（`:620-622`） |
| 10 | `finalize_model` | `nodes.py:904` | 142（7 參數） | 4 | train／dev handle、`hpo_best_model`、`best_params`、`best_iteration`、`preprocessor_view`、`parameters` | `model` | ranking／非 ranking 兩分支各自讀兩份 parquet；規則 5：類別欄索引（`:955-957`）與 `adapter._categorical_indices` 重複，參數 dict（`:1024-1031`）與 `TrialScorer`（`hpo_scoring.py:335-342`）重複；記憶體見第 2 節發現 #6；綁 LightGBM 見第 6 節 6.1 |
| 11 | `predict_and_write_test_predictions` | `nodes.py:1048` | 276 | 9 | `model`、`test_parquet_handle`、`preprocessor_view`、`parameters`；`writes=training_eval_predictions` | `training_eval_predictions`（Hive，逐分區）、`predict_manifest` | 效率見第 2 節發現 #5；規則 5／16 見第 2 節發現 #11；rebuild 集合重算（`:1192-1194`） |
| 12 | `compute_test_metrics` | `nodes.py:1416` | 172 | 5 | `training_eval_predictions`（Spark）、`predict_manifest`（只當排序）、`parameters` | `evaluation_results` | 三個 `raise` 已標種類；`frame` 未 persist（第 2 節發現 #18） |
| 13 | `log_experiment` | `nodes.py:1326` | 88（10 參數） | 3 | `model`、`best_params`、`best_iteration`、`evaluation_results`、4 份診斷、`parameters` | MLflow run＋上傳整個 `diagnostics/` | 第 2 節發現 #10；`gain_ledger`、`group_filter_report`、`sample_weight_report` 都沒進 MLflow 的摘要 |
| 14 | `compute_feature_statistics` | `diag/feature_stats.py:18` | 87 | 1 | `train_parquet_handle`、`model`、`preprocessor`、`parameters` | `feature_statistics.json` | 規則 8（已登記例外，#418 已拍板搬）；模組 docstring 中文；效率見第 2 節發現 #15 |
| 15 | `compute_feature_importance` | `diag/importance.py:8` | 15 | 0 | `model`、`parameters` | `feature_importance.json` | 規則 8；docstring 中文；`kind="split"/"gain"` 是 LightGBM 字彙 |
| 16 | `compute_gain_ledger` | `diag/gain_ledger.py:343` | 30（核心 `_ledger_from_trees` 194 行） | 0 | `model`、`preprocessor`、`parameters` | `gain_ledger.json`（落在 `diagnostics/`） | 規則 8；規則 12：import 私有 `_resolve_booster`（`:29`）；讀 `booster.trees_to_dataframe()` 與 LightGBM 類別切點格式；跟 `log_experiment` 沒有邊（第 2 節發現 #10b） |
| 17 | `compute_shap_diagnostics` | `diag/shap_per_item.py:104` | 223 | 1 | `model`、`test_parquet_handle`、`preprocessor`、`parameters` | `shap_diagnostics.json`＋PNG（自己 `savefig`） | 規則 8；規則 2、4：預算閘、`per_item` 背景降級、正例抽樣都是決策但沒有 `# Decision`；規則 9：用 `# ---- 全域 ----` 分隔線；中文；PNG 是 A1 盲區的直接寫檔；效率見第 2 節發現 #15 |
| 18 | `select_shap_population` | `diag/population_spark.py:12` | 112 | 0 | `training_eval_predictions`、`test_model_input`（兩張 Hive）、`parameters`、`predict_manifest`（只當排序） | `shap_population`、`case_rows`（memory） | 規則 8；**規則 14**（第 2 節發現 #1）；規則 5／16（第 2 節發現 #12）；規則 1 反方向（一個 node 兩個輸出給兩個下游，ADR-0014 決定 3 已論證保留）；pyspark 在函式內 import；全部例外吞掉只 warn |
| 19 | `compute_quadrant_profiles` | `diag/shap_cases.py:20` | 57 | 1 | `model`、`shap_population`、`preprocessor`、`parameters` | `per_quadrant.json` | 規則 8；規則 12：import 私有 `_signed_profile`（`:13`） |
| 20 | `compute_quadrant_cases` | `diag/shap_cases.py:132` | 103 | 1 | `model`、`case_rows`、`preprocessor`、`parameters` | `cases_manifest.json`＋PNG＋`mkdir` | 規則 8；規則 12；規則 9：body 內巢狀函式 `_gkey`（`:190-191`）；PNG 是 A1 盲區 |

`# Decision` 計數（`grep -rh '# Decision' src/recsys_tfb/pipelines/<name>/ | wc -l`）：dataset 73、training 42、evaluation 30、inference 24；`diagnosis/model/` 4。

### 4. 重複（面向②）

| # | 重複的東西 | 位置 | 選錯／漂移的後果 |
|---|---|---|---|
| D1 | parquet 列 → 特徵矩陣兩套（`_stream_matrix` vs `pdf_to_X`），延後編碼的類別欄各寫一份；`pdf_to_X` 沒有 B6／B9 型別閘 | `extract.py:933-941` vs `:987-994`；型別閘只在 `:1049`、`:890` | 兩條路徑對同一欄的編碼或 dtype 若漂移，訓練與預測看到的特徵不同，不報錯 |
| D2 | 排名 window 自寫（無 event tie-break） | `diag/population_spark.py:52-54` vs `utils/ranking.py:77-102` | 宣告 event 時象限指派與 evaluation 排名不一致 |
| D3 | 預測輸出 frame 組裝＋逐分區寫入，training 與 inference 各一份 | `nodes.py:1263-1300` vs `inf/nodes.py:461-491` | inference 有逐塊驗證，training 沒有 |
| D4 | 特徵清單的權威：training 預測用設定導出的 view，inference 與全部診斷用模型 | `nodes.py:1260`（`pipeline.py:127-128`）vs `inf/nodes.py:334`、`diag/*` | training 內不會靜默錯（exclude 改了會換 `model_version`），但同一件事兩個答案，規則 16 要求寫理由 |
| D5 | metric 預設與訓練參數 dict：tune 與 finalize 各算一次；trial 與 refit 各組一次 | `nodes.py:680-687` vs `:942-948`；`steps/hpo_scoring.py:335-342` vs `nodes.py:1024-1031` | refit 的參數與搜尋時不同 → 報告的超參數不是實際訓練用的 |
| D6 | `feature_pre_filter: False` 字面值 5 處 | `hpo_scoring.py:338, 351`；`refit.py:90`；`adapter.py:432`；`nodes.py:1027` | `refit.py:72-77` 自己寫了：漂移會讓 refit 丟掉搜尋時可切的特徵，不報錯 |
| D7 | 類別欄索引 | `nodes.py:955-957` vs `adapter.py:247-255` | — |
| D8 | weight key 與 decode map | `nodes.py:252, 259-262` vs `extract.py:746-753, 482-484` | 報告與實際加權用不同的 key，「沒對到」的報告失真（該 node 的註解自己說了） |
| D9 | 月份去重、rebuild 集合 | `nodes.py:463-477` vs `predict_months.py:70-85`；`nodes.py:464-466` vs `:1192-1194` | — |
| D10 | 快取路徑組法 | `nodes.py:577-580` vs `local_cache.py:54-62, 124-125` | 預設值不同：`cache.root` 缺席時一邊 `KeyError`、一邊 `/tmp/recsys_cache` |
| D11 | `"_SUCCESS"` 字面值 3 處 | `local_cache.py:158`、`handles.py:83`、`adapter.py:341` | `local_cache.py:147-157` 自己記了這個漂移風險 |
| D12 | `training.algorithm` 預設 `"lightgbm"` 4 處＋載入時的 fallback | `nodes.py:574, 671, 941, 1359`；`io/model_adapter_dataset.py:42-47` | — |
| D13 | `data/models/...` 路徑寫死，與 catalog 的 `data/models/${model_version}` 各一份 | `steps/hpo_resume.py:30`；`diag/paths.py:11` | catalog 改路徑時這兩處不跟 |
| D14 | collect-all 訊息格式兩套實作 | `extract.py:675-686`（`_raise_if`）vs `core/consistency.py:1052-1060`（`collect_all_message`） | ADR-0029 決定 7 在 dataset 統一過，training 讀取路徑沒有 |

### 5. 效率（面向③）——全部「讀 code」，沒有量

| # | 發現 | 位置 | 證據等級 |
|---|---|---|---|
| E1 | HPO 先讀 val 再看要不要跑 trial（第 2 節發現 #4） | `nodes.py:720-733, 774-813` | 讀 code；37–89 GiB 取自 `nodes.py:697-706` 的註解 |
| E2 | predict 列分區時把所有 test 列的兩欄拉進 driver（第 2 節發現 #5） | `nodes.py:1126-1145` | 讀 code；約 2.2 億列取自 `nodes.py:1129-1133` 的註解 |
| E3 | predict 每個分區讀全部欄，沒有投影 | `nodes.py:1239-1242` | 讀 code |
| E4 | `.bin` 建置 ranking 分支與 `refit_on_full` 峰值約 2 倍矩陣（第 2 節發現 #6） | `adapter.py:452-500`；`nodes.py:959-1004`；`refit.py:43-45` | 讀 code；生產矩陣多大這份盤點沒查 |
| E5 | `select_shap_population` 對全部月份做排名與兩次 join（第 2 節發現 #1） | `diag/population_spark.py:52-100` | 讀 code |
| E6 | 診斷抽樣整份循序掃描：train 1 次、test 2 次（第 2 節發現 #15） | `diag/data_access.py:54-89` | 讀 code |
| E7 | test 資料在一次 training 裡被讀：predict（分區欄全量＋處理月份全欄）、SHAP（item 欄、label 欄各全量＋兩次循序掃描）、`select_shap_population`（從 Hive 再讀一次 `test_model_input`，不是本機快取） | 同上各處 | 讀 code |
| E8 | 每次 `extract_Xy*` 開 parquet dataset 4–5 次（metadata 探測逐 row group 走、B6、B9 各一次、串流、zero-positive 權重欄檢查）；metadata only，fragment 多時目錄探索重複 | `extract.py:529, 671（被 B6、B9 各呼叫一次）, 893, 1161` | 讀 code；影響可能很小 |
| E9 | `compute_test_metrics` 的 frame 每個 action 各讀一次 Hive（第 2 節發現 #18） | `nodes.py:1530-1560` | 讀 code |
| E10 | `persist_sample_weight_report` 每次（含 `.bin` 快取命中）整欄讀 train 的 weight key 欄再去重 | `steps/sample_weights.py:44-47` | 讀 code；欄窄，可能不值得 |

**沒發現的**：Spark cache／persist 沒釋放——training 範圍內唯一的 `persist` 在 `diag/population_spark.py:68-69`，有 `finally: unpersist()`（`:104-116`）。

### 6. 擴充性（面向④）

#### 6.1 綁死 LightGBM 的位置

**怎麼搜的**（讓人判斷有沒有漏）：

1. 關鍵字 grep，範圍 `pipelines/training`、`models`、`io`、`diagnosis/model`、`diagnosis/hpo`、`__main__.py`、`core`、`evaluation`、`pipelines/inference`、`pipelines/evaluation`、`utils`：
   `grep -rnE "lgb\.|import lightgbm|lightgbm as lgb|mlflow\.lightgbm|Booster|\.booster\b|_booster|save_binary|trees_to_dataframe|num_trees\(|best_iteration|feature_pre_filter|num_iterations|early_stopping_rounds|LgbDatasetHandle|lgb_handle|\.construct\(\)|set_weight|num_data\(|TreeExplainer|decision_type|lambdarank|rank_xendcg|\"lightgbm\"|'lightgbm'"`
   → 199 行、21 個檔。
2. 語意假設 grep（adapter 以外）：`GBDT|log-odds|kind="(split|gain)"|"model.txt"|categorical_feature|feature_name=|.feature_importance(|free_raw_data` → 8 處。
3. 範圍內每個檔都逐行讀過；`conf/base/parameters_training.yaml` 只看了 `algorithm` 那行。
4. **沒掃**：`scripts/`、`tests/`、`docs/`、`examples/ad/conf/`。

| 模組 | 綁在哪 | 位置 |
|---|---|---|
| `models/base.py` | ABC 的 `prepare_train_inputs` 回傳 `LgbDatasetHandle`；`train` 簽章沒有原生資料集參數，但呼叫端都傳；`feature_importance(kind in {"split","gain"})` 是 LightGBM 字彙 | `:9, 20-27, 51-53, 61-75` |
| `models/lightgbm_adapter.py` | 整份（預期中）；但 `prepare_train_inputs` 還裝了 pipeline 決策（見第 2 節發現 #2） | `:257-567` |
| `pipelines/training/nodes.py` | 預設 `"lightgbm"`；`prepare_lgb_train_inputs` 與 `lgb/` 路徑；`*_lgb_handle.sample_weights`；類別欄索引；`refit.build_dataset`；LightGBM 參數鍵（`feature_pre_filter`、`num_iterations`、`early_stopping_rounds`）；`adapter.train(X_train=None, …, train_dataset=)`；`default_metric_for_objective` | `:562-588, 574, 671, 683-687, 828-831, 941, 944-948, 955-957, 1001-1004, 1021-1039, 1359` |
| `steps/hpo_scoring.py` | trial 參數鍵；`handle.load()` 回 `lgb.Dataset`、`set_weight`、`construct()`、`num_data()`；`train_dataset=`／`val_dataset=`；`adapter.booster.best_iteration`；「GBDT 逐列預測所以分批位元相同」的假設 | `:167-172, 202-225, 335-342, 351-369, 374-378, 393, 398, 408` |
| `steps/refit.py` | `import lightgbm as lgb`；`lgb.Dataset(...)` | `:17, 62-92` |
| `steps/hpo_resume.py` | checkpoint 檔名 `model.txt`（LightGBM 文字格式） | `:24` |
| `steps/search_space.py` | 參數名必須是原生 LightGBM／XGBoost 參數（docstring） | `:18-20` |
| `pipeline.py` | catalog 名 `train_lgb_handle`、`train_dev_lgb_handle` | `:80, 87, 104` |
| `io/handles.py` | `.bin` 的兩個 sidecar 常數；`LgbDatasetHandle.load()` 直接建 `lgb.Dataset` | `:128-187, 190-302` |
| `io/__init__.py` | re-export `LgbDatasetHandle` | `:6` |
| `io/model_adapter_dataset.py` | 沒有 `model_meta.json` 時退回 `"lightgbm"` | `:42-47` |
| `core/group_utils.py` | `RANKING_OBJECTIVES` 是 LightGBM 的 objective 名；`objective_cache_key` 的 `lgb/` 分段；預設 metric `"ndcg"` | `:18, 26-56, 83, 190-204` |
| `core/consistency.py` | A7 的 `RANKING_METRICS` 是 LightGBM metric 名 | `:1851-1885` |
| `core/logging.py` | `log_data_volume` 依 `num_data()`／`num_feature()` 辨認 lgb Dataset（duck typing） | `:318` |
| `diag/attribution.py` | `model.booster`＋`shap.TreeExplainer`＋`num_trees()`（已刻意收成一個接縫） | `:10-48` |
| `diag/gain_ledger.py` | `booster.trees_to_dataframe()`、LightGBM 類別切點格式（`"2\|\|3"`、`decision_type == "=="`） | `:29, 97-115, 190-198, 361-363` |
| `diag/importance.py` | `kind="split"`、`kind="gain"` | `:13-14` |
| `diag/shap_cases.py` | 圖的 x 軸標 `signed SHAP (log-odds)` | `:101` |
| `conf/base/parameters_training.yaml` | `algorithm: lightgbm`（註解寫「只有 lightgbm 已註冊」） | `:25` |

**沒綁的**：inference 只用 `model.predict` 與 `feature_names()`，`models/feature_view.py` 經 `getattr` 取 `feature_names`，都與演算法無關。

#### 6.2 加一個非 LightGBM 演算法要動幾處

至少 13 個模組：新 adapter 與註冊（1）、`base.py` 的 ABC（2）、`io/handles.py` 的 handle（3）、`steps/hpo_scoring.py`（4）、`steps/refit.py`（5）、`nodes.py` 的 `prepare_lgb_train_inputs`／`finalize_model`（6）、`pipeline.py` 的 catalog 名（7）、`core/group_utils.py`（8）、`core/consistency.py` A7（9）、`steps/search_space.py`（10，至少 docstring）、`steps/hpo_resume.py` 檔名（11）、`diag/gain_ledger.py`／`importance.py`／`attribution.py`（12）、`io/model_adapter_dataset.py` fallback（13）。另外 `training.algorithm` 目前沒有 A 系列 predicate，未註冊的名字要等 cache node 複製完才在 `get_adapter` 報錯——這件 #387 已開票。

#### 6.3 加一個新 objective

LightGBM 內的新 objective：ranking 類要改 `core/group_utils.py:18`（`RANKING_OBJECTIVES`），並決定 `objective_drops_zero_positive_groups`（`:83`）與預設 metric（`:190-204`）；其他地方都經 `is_ranking_objective` 分派。非 ranking 類全部共用 `lgb/binary/` 快取分段（`group_utils.py:46-49` 刻意如此）。約 1–3 處。
新的 **HPO 評分 objective**（`training.hpo_objective`）是另一件事：`evaluation/metric_registry.py` 加一列＋`metrics_spark` 的 pass，training 端不用改。

#### 6.4 加一種新診斷

5–7 處：node 函式、`pipeline.py`、`catalog.yaml`（與 `examples/ad` 的 catalog）、`diagnostics.<name>` 設定、`log_experiment` 的輸入（只能加在最後、預設 `None`，位置綁定）、`steps/experiment_log.log_diagnostics_summary`，可能還有 `RESUME_CONTRACTS`。**陷阱**：檔案落在 `diagnostics/` 而沒接成 `log_experiment` 的輸入時，會不會被上傳取決於 Kahn 排序（第 2 節發現 #10b）。

#### 6.5 `ModelAdapter` 夠不夠當接縫

**不夠**，四個理由：

1. ABC 的 `train(X_train, y_train, X_val, y_val, params)` 與實際呼叫不符：兩個呼叫端都傳 `X_*=None` 加 `train_dataset=`／`val_dataset=`（`hpo_scoring.py:374-378`、`nodes.py:1035-1039`），這兩個參數只有 `LightGBMAdapter.train` 有（`adapter.py:169-171`）。
2. `prepare_train_inputs` 讓 adapter 讀 parquet、決定快取格式與丟組政策，adapter 因此依賴 `io/`，而 `io/model_adapter_dataset.py` 又依賴 `models/`——這就是 `adapter.py` 12 處函式內 lazy import 的原因（`:22-25`）。
3. 呼叫端繞過 ABC 讀 `.booster`（`hpo_scoring.py:393`、`diag/attribution.py:10-17`、`diag/gain_ledger.py:361`）。
4. HPO 與 refit 在 adapter 以外自己組 LightGBM 參數與 `lgb.Dataset`（`hpo_scoring.py:335-369`、`refit.py:83-92`），所以 adapter 不是唯一懂 LightGBM 的地方。

### 7. 維護性（面向⑤）

#### 7.1 超過 80 行的函式（AST 量）

| 行數 | 函式 | 位置 |
|---|---|---|
| 311 | `LightGBMAdapter.prepare_train_inputs` | `adapter.py:257` |
| 291 | `tune_hyperparameters` | `nodes.py:611` |
| 276 | `predict_and_write_test_predictions` | `nodes.py:1048` |
| 223 | `compute_shap_diagnostics` | `diag/shap_per_item.py:104` |
| 198 | `create_pipeline` | `pipeline.py:31`（多半是註解） |
| 194 | `_ledger_from_trees` | `diag/gain_ledger.py:118` |
| 172 | `compute_test_metrics` | `nodes.py:1416` |
| 146 | `_stream_matrix` | `extract.py:816` |
| 142 | `finalize_model` | `nodes.py:904` |
| 135 | `extract_Xy_with_groups` | `extract.py:1072` |
| 112 | `select_shap_population` | `diag/population_spark.py:12` |
| 103 | `compute_quadrant_cases` | `diag/shap_cases.py:132` |
| 103 | `cache_test_model_input` | `nodes.py:430` |
| 93 | `populate_cache_from_hive` | `steps/local_cache.py:243` |
| 88 | `log_experiment` | `nodes.py:1326` |
| 87 | `compute_feature_statistics` | `diag/feature_stats.py:18` |
| 81 | `_row_weights_from_pdf`、`TrialScorer.__call__` | `extract.py:433`、`steps/hpo_scoring.py:331` |

另外 `main` 的 `training()` 260 行（`main:1466-1725`）。規則 2 允許 node 長，這張表本身不是違例；要看的是第 3 節的「Decision」欄：`compute_shap_diagnostics`（223 行／1）、`select_shap_population`（112 行／0）、`prepare_train_inputs`（311 行，不是 node、決策沒有標）。

#### 7.2 參數超過 6 個

`TrialScorer.__init__` 21 個 keyword（`hpo_scoring.py:268`）、`_positive_profiles` 13（`diag/shap_per_item.py:71`）、`log_experiment` 10（node）、`extract_Xy_with_groups` 9（6 個布林旗標，`extract.py:1072`）、`LightGBMAdapter.train` 8、`_render_case` 8、`finalize_model` 7（node）、`_hpo_score` 7、`write_checkpoint` 7。

#### 7.3 隱性順序依賴與暗道

- 第 2 節發現 #10 的三件（位置綁定的選用輸入、靠排序上傳的檔、只當排序用的 manifest）。
- `tune_hyperparameters` 第一行釋放 SparkSession，靠「Runner 是循序的」這個事實（`nodes.py:645-663`，已寫明）。
- `parameters` 被當成暗道：`_cache_source_tables`、`_cache_partitions`（`cache_sources.py:70-72`）、`_fresh_hpo`、`search_id`（`main:1653-1665`）；`_resolve_search_id` 為了測試在正式碼裡留了一條自己算的分支（`nodes.py:595-608`）。
- `LightGBMAdapter.train` 會 `pop` 呼叫端的 `params`（`adapter.py:176-181`）；目前兩個呼叫端每次都新建 dict，所以沒出事。

#### 7.4 docstring／註解與程式對不上

| 位置 | 寫的 | 實際 |
|---|---|---|
| `nodes.py:1, 23-24` | 21 個 node 中 14 個在這裡 | 20 個中 13 個（calibration 移除後沒改） |
| `nodes.py:294, 414, 441`；`steps/local_cache.py:5, 10, 208` | 五個 cache node | 四個（#413 移除 `cache_calibration_model_input`） |
| `nodes.py:620-622` | val 讀成 pandas DataFrame、函式結束就釋放 | val 是磁碟映射矩陣（`:697-706`） |
| `extract.py:971-975` | `pdf_to_X` 被 `extract_Xy` 用，predict 會先做 positive-set 篩選 | `extract_Xy` 改走 `_stream_matrix`（#284）；丟組已移到 dataset |
| `extract.py:7-9` | 搬出來是為了讓 adapter 沒有循環 import | adapter 仍因循環 import 有 12 處 lazy import（`adapter.py:22-25`） |
| `adapter.py:427-431` | 「pre-cache training path（trial 時由 `lgb.train` 建 Dataset）」 | 那條路徑已不存在 |
| `utils/ranking.py:21-23` | `population_spark.py` 的排名「用同一個順序」 | 沒有 event tie-break，宣告 event 時順序不同 |
| `diag/__init__.py:1-3` | 維持與舊 `diagnostics.py` 相容；匯出 node | 只匯出 7 個中的 5 個，`pipeline.py` 另外直接從子模組 import 兩個（`pipeline.py:12-13`） |
| `nodes.py:144-146` | 「Helpers」區段 | 底下兩個函式是 node |

### 8. 慣例落差（面向⑥）

| # | dataset 第二輪／inference 有、training 沒有 | training 現況 | 位置 |
|---|---|---|---|
| C1 | 開跑前契約模組（ADR-0029 決定 11，`pipelines/dataset/run_contract.py`） | 只有 `cache_sources.py` 管快取來源表；其餘都在 `main` 的 `training()`；`inject_cache_source_tables` 在通用 `_execute_pipeline` 對每條 pipeline 都跑 | `main:1466-1725, 767` |
| C2 | 規則 14：讀來源表或增量產物時在程式裡寫明月份，並有 `test_month_scoped_reads.py` 釘住 | `select_shap_population` 沒篩；training 沒有對應的讀取點測試 | 第 2 節發現 #1 |
| C3 | 規則 16：各 split 同一個機制，不對稱要寫理由 | train／train_dev 走 `.bin`、val 走磁碟映射、test 走 `pdf_to_X`；特徵權威 training 預測與 inference 不同；象限排名與 evaluation 不同。部分有理由，但沒有在 docstring 用規則 16 的檢查寫 | 第 2 節發現 #9、#11、#12 |
| C4 | 規則 17：資料閘訊息由 `core/consistency.collect_all_message` 產出 | B6、B9 在 `io/extract.py` 用自己的 `_raise_if` | D14 |
| C5 | 規則 18：產物格式版本常數 | `model_version`／`search_id` 沒有；`.bin` 快取格式改變靠一段段 inline 的過期檢查（#418 項目 1 的根源） | 第 2 節發現 #7 |
| C6 | inference 的逐塊驗證（ADR-0011 §3） | test 預測寫入前沒有驗證 | 第 2 節發現 #11 |
| C7 | 體例：程式碼註解與 docstring 一律英文 | `diagnosis/model/` 12 個檔中 11 個含中文 docstring 或註解（只有 `data_access.py` 全英文）；`core/versioning.py:303-309`（`compute_search_id`）中文；`main:1495` CLI help 中文 | — |

### 9. #418 三件現況

| 項目 | 結論 | 證據 |
|---|---|---|
| 1 | **仍是 inline `if`**。`stale = False` 之後，zero-positive 標記檢查（`GROUP_FILTER_COUNTS_NAME` 不在就 warn 並設 `stale`）與 `_weight_keys_cache_gap` 的呼叫＋`if weight_gap:` 兩段都直接寫在 `prepare_train_inputs` 中段 | `adapter.py:350`（`stale = False`）、`:351-368`、`:370-385`；helper 定義 `adapter.py:89-124` |
| 2 | **仍是 `pdf[feature_cols].copy()`**，沒有改用 `_narrow_frame` | `extract.py:984`；`_narrow_frame` 在 `extract.py:28-43`。呼叫點現在是 6 個 src＋1 個 script：`nodes.py:1260`、`inf/nodes.py:454`、`diag/shap_cases.py:54, 180`、`diag/shap_per_item.py:88, 182`、`scripts/shap_margin_summary.py:126` |
| 3 | **仍在 `diagnosis/model/`**，7 個都沒搬 | `diag/feature_stats.py:18`、`importance.py:8`、`gain_ledger.py:343`、`shap_per_item.py:104`、`population_spark.py:12`、`shap_cases.py:20`、`shap_cases.py:132`；`pipeline.py:5-13` 從那裡 import |

另：#418 項目 1 內文的行號（`lightgbm_adapter.py:239, 333-351, 353-356`）已過期，現在是上表的行號；項目 3 說「全 repo 唯一的一筆例外」也過期，`pipeline-node-design.md`〈已登記的例外〉現在有 3 筆。

### 10. 已知、已登記，不列入排序

避免重新「發現」：checkpoint 兩檔非原子（ADR-0014 刻意不做第 3 條、`architecture-constraints.md` F3）；`cache.root` 是相對路徑（ADR-0014 第 8 條；`hpo_resume.py:30`、`diag/paths.py:11` 的 `data/...` 也是同一個問題）；rank 落地與 tie-break 統一（ADR-0014 第 2 條、#450、#402）；`select_shap_population` persist 的峰值（#238）；`score_uncalibrated`（#412）；`training.algorithm` 與其他設定擋太晚（#387，其中 training 的「版本資料夾不存在」那一列現在已在 Spark 前擋，`main:1606-1611` 早於 `:1639`）；`populate_cache_from_hive` 的間接寫入不在 R4（ADR-0014 G2 已裁決）。

---

## 二、效能量測

每個數字都附日期、環境、資料量與出處；「現況」欄標「仍可信」或「已過期」，過期一律附理由。來源三份文件（沿用其縮寫）：`doc1` ＝ `docs/notes/2026-09-07-training-pipeline-profiling.md`；`doc2` ＝ `docs/notes/2026-07-11-training-oom-investigation.md`（公司環境 OOM 事故調查）；`doc3` ＝ `docs/agents/pipeline-performance-work.md`（無獨立 training 量測，僅引用 doc1）。

### 1. doc1（2026-09-07 本機合成資料量測）共同量測背景

- **量測日期**：2026-09-07；**環境**：worktree `feat/perf-training`、本機 `local[*]`、LightGBM 釘 `OMP_NUM_THREADS=4`（對齊生產 driver 4 核）、`TZ=Asia/Taipei`、合成資料；**資料量**：列數掃描（1，000/4，000/16，000 entity ×24 欄）與欄數掃描（1，000 entity ×24/128/665 欄，665＝生產實際欄數）兩條獨立軸。

| Node/階段 | 數字（1k/4k/16k entity 或 24/128/665 欄） | 生產外推 | 出處 | 現況 |
|---|---|---|---|---|
| `tune_hyperparameters`（整個 node） | 16.9s／193.1s／210.5s；k列=0.88、k欄=0.76 | **2.43 小時** | `doc1:175` | **已過期（結構性存疑）**：`ee323c1e`（#429，09-21）改零正例留存規則→進 `lgb.train` 列數可能已變；`3c7644a5`（#318，09-09）改樣本權重機制（`.bin` 不再烤權重，改 trial 時 `set_weight()`）；`5eebe802`/`33ffc498`/`cae57f80`（#430，09-22）新增兩個 HPO 目標改變 score 步驟。doc1 §1「98% 在 `lgb.train`」的質性結構可能仍站得住，秒數不可信 |
| `compute_test_mAP_spark` | 20.5s／38.4s／22.7s；k列=0.12、k欄≈0 | 50 秒 | `doc1:176` | **已過期，node 已不存在**：`f9839f98`（#452，09-24）改名重構為 `compute_test_metrics`（3 次 Spark action、不需 K），且 `b0d89b8c`（#413，09-20）已拿掉校準器，舊版「偵測校準後重算」分支不再存在 |
| `predict_and_write_test_predictions` | 5.6s／8.7s／6.9s；k列=0.16、k欄=0.06 | 13 秒 | `doc1:177` | **部分過期**：函式仍在（`nodes.py:1048`），但 `b7bc8a8a`（09-21）多寫一欄 `event`、`543e5654`（09-21）改同分決勝規則；量級小、質性結論大致仍可信 |
| `select_shap_population` | 1.9s／4.3s／5.4s；k列=0.29、k欄=0.31 | 18 秒 | `doc1:178` | **已過期**：`population_spark.py` 被 `f9839f98`（09-24）與 `11ac379e`（09-20）動過 |
| `cache_train_model_input` | 2.6s／3.7s／4.8s；k列=0.12、k欄=0.18 | 8 秒 | `doc1:179` | **仍可信**：`nodes.py:305-349` 自 09-07 起零 commit 命中（跨環境拓撲差異仍適用，見 doc1 §9.4） |
| `compute_shap_diagnostics` | 3.1s／4.5s／4.1s；k列=0.14、k欄=0.09 | 7 秒 | `doc1:180` | **仍可信**：`shap_per_item.py` 零 commit 命中 |
| `compute_quadrant_cases` | 3.2s／3.9s／3.8s；k列=0.07、k欄=0.05 | 5 秒 | `doc1:181` | **幾乎仍可信**：`shap_cases.py` 僅被 `11ac379e` 機械性動過（schema 取值重構，非邏輯改動） |
| `log_experiment` | 3.1s／3.2s／3.3s；k列=0.01、k欄=0.02 | 3 秒 | `doc1:182` | **已過期（邏輯變了，量級仍可信）**：`b0d89b8c`（#413） 拿掉校準器後，原呼叫的 `log_calibration_outcome` 已不存在 |
| 其餘 13 個 node（合計） | 全部 <1s | 合計數秒 | `doc1:183` | 未逐一查證（doc1 未列出這 13 個 node 的名字） |

**doc1 §4.3 `tune_hyperparameters` 內部拆解**（×20 次呼叫）：

| 子步驟 | 數字 | 生產外推 | 佔比 | 出處 | 現況 |
|---|---|---|---|---|---|
| `train`（`lgb.train`） | 15.29s／186.52s／208.00s；k列=0.91、k欄=0.78 | 2.43 小時 | 99.2% | `doc1:197` | **已過期同上**（進入的資料列數/權重機制已變） |
| `prepare_datasets` | 0.03s／0.33s／0.70s；k列=0.87、k欄=0.94 | 25 秒 | 0.28% | `doc1:198` | **已過期**：`3c7644a5`（#318）改成每 trial 現算權重再 `set_weight()`，量測後新增的工作 |
| `predict` | 0.15s／3.74s／0.20s；k列=1.17、k欄=0.09 | 25 秒 | 0.28% | `doc1:199` | **已過期**：`hpo_scoring.py` 被同分決勝規則與新 HPO 目標改動 |
| `score`／mAP | 0.04s／0.91s／0.06s；k列=1.13、k欄=0.12 | 6 秒 | 0.07% | `doc1:200` | **已過期**：新增兩個 HPO 目標直接改變此步算法 |
| `extract_features`／`read_parquet` | <0.1s（各規模） | <1 秒 | — | `doc1:201` | 量級極小，未特別查證 |

**doc1 §4.4 每輪 boosting 成本**（跨規模唯一可比較的量，腳本消融）：1k entity ×24 欄 73,337×23 列 7,205 輪 2.68ms/輪；4k entity 291,030×23 8,093 輪 6.39ms/輪；16k entity 1,158,730×23 7,932 輪 25.97ms/輪；1k entity ×128 欄 73,337×127 6,832 輪 7.22ms/輪；1k entity ×665 欄 73,337×664 6,125 輪 39.69ms/輪（出處 `doc1:216-220`）。現況：**已過期同上**（LightGBM 每輪內部行為未變，但基準列數與權重機制前提已變）。

**doc1 §4.9 兩維相乘外推**：驗證點 4,000 entity ×665 欄 預測 731s（12.2 分）；生產 4,542,746×663 預測 **8,204s（2.28 小時）**（出處 `doc1:237-238`）。現況：**doc1 自己已標註這個驗證點從未實際執行**（doc1 §4.10, `:244-256`），非因程式碼變動而過期——從一開始就是未驗證的外推假設，疊加上方 `tune_hyperparameters` 過期理由，可信度雙重降低。

**doc1 §5.1 `lgb.train` 內部消融**（腳本 `ablate_lgb_train.py`，73,337×663、100 輪、4 執行緒，皆對照 base）：

| 消融項 | 數字 | 出處 | 現況 |
|---|---|---|---|
| `leaves_8`（num_leaves 32→8） | 中位 2.74s，−38.2% | `doc1:272` | 仍可信（`search_space` num_leaves 範圍 4–64 未變），但疊加列數前提風險 |
| `ff_1.0`（feature_fraction 0.8→1.0） | 中位 5.23s，+17.8% | `doc1:273` | 仍可信（search_space 範圍 0.6–1.0 未變） |
| `no_bagging`（bagging_freq 1→0） | 中位 4.61s，+4.0% | `doc1:274` | 仍可信 |
| `max_bin_63`（255→63） | 中位 3.88s，−12.5% | `doc1:275` | 仍可信（`max_bin` 不在 conf，diff 確認未動） |
| `no_valid`（拿掉 valid_sets） | 中位 4.25s，−4.3% | `doc1:276` | 仍可信 |
| `col_wise`（force_col_wise） | 中位 4.22s，−5.0%（doc1 自標為量法假象，見 §7.6） | `doc1:277` | 仍可信，doc1 已自我修正 |
| `row_wise`（force_row_wise） | 中位 5.57s，+25.5%（標記「地雷，別設」） | `doc1:278` | 仍可信 |

**doc1 §5.2 執行緒擴展性**（腳本 `bench_thread_scaling.py`，291,030×663、50 輪）：1 核 18.48s（1.00×，100%）；2 核 10.88s（1.70×，85%）；4 核 6.82s（2.71×，68%）；8 核 5.93s（3.12×，39%）；Amdahl 擬合 4 核→16 核 1.35–1.75×（→1.39–1.79 小時）（出處 `doc1:294-307`）。現況：**部分過期**——列數基準可能因 `ee323c1e` 飄移；doc1 自己標註 §8.2「機器上有別的 session 在跑，量到 74% 漂移」，本身帶雜訊警示；硬體擴展性質性結論（4 核後平行效率崩潰）與程式碼變動無關，機率仍可信。

**doc1 §6.1/§6.2**：`compute_metrics_uncalibrated`（校準判斷後整包重算）佔 `compute_test_mAP_spark` 42–45%，生產外推約 21 秒／0.24%（出處 `doc1:323-358`）。現況：**已過期，機制已不存在**——校準器已拿掉（#413/#414），該 node 已改名重構為 `compute_test_metrics`（#452），「偵測校準後重算」分支已無此邏輯可跳過。`no_valid` 消融的 train_dev 評估數字（−4.3%，4/4 輪一致，出處 `doc1:360-367`）與 §5.1 同一數字，判定同：仍可信但 doc1 自承未驗證評估頻率對 early stopping 選模型的影響。

### 2. doc2（2026-07-11 公司環境 OOM 事故調查）

**證據等級**：公司環境事故紀錄（非受控量測，事後無法覆核）。

| Node/階段 | 數字 | 環境 | 資料量 | 出處 | 現況 |
|---|---|---|---|---|---|
| `read_parquet`（`extract_Xy`） | 69.52 秒 | 公司生產環境，事故現場 log | 4,542,746 列×665 欄 | `doc2:28` | **仍可信但無法覆核**——doc2 自標為「唯一的生產側錨點，來自事故現場」，性質是歷史記錄非可重現基準 |
| `slice_features`（`pdf_to_X`） | 14.09 秒 | 同上 | 同上→663 欄 | `doc2:30` | 同上，歷史記錄性質 |
| `to_numpy` 每格記憶體（含文字欄，object dtype） | 34.2 bytes/格（實測 10 萬列樣本）；外推生產 **95.9 GiB** | 本機小樣本量測外推生產列數（Python 3.10.9/pandas 1.5.3/numpy 1.25.0） | 10 萬列樣本；外推 4,542,746×663 | `doc2:56, 71-77` | **已過期（前提被部分修正，根因未變）**：`compute_feature_columns` 判定未宣告文字欄為特徵的邏輯字面未變，但攔截層 B6（`78d3e689`，同日）已加固為 loud error；且 `42547679`（#283，09-04） 把數值特徵欄收斂到 `numeric_feature_storage_type`（預設 float32），2026-07-11 測到的 34.2 bytes/格是基於「當時型別未收斂（401 float+92 int32+162 int64 混雜）」算出，今日型別分布已不同，數字不能直接沿用 |
| `to_numpy` 每格記憶體（移除文字欄後，float64 假設） | 8.0 bytes/格；外推 **22.4 GiB** | 同上 | 同上 | `doc2:57, 76` | **已過期**：`#283` 後數值特徵欄預設收斂為 float32（4 bytes），非 float64（8 bytes）；理論上限應接近此數字一半（~4.0 bytes/格、~11.2 GiB），除非鍵被明確設為 float64 |
| `to_numpy` 總記憶體需求（含文字欄） | 合計 **~142 GiB**（讀入 16.3+特徵拷貝 16.0+新矩陣 95.9+轉換暫態 13.6） | 外推生產規模 | 同上 | `doc2:79-87` | **已過期**，理由同 95.9GiB 項 |
| `to_numpy` 總記憶體需求（移除文字欄後） | 合計 **~54.7 GiB** | 外推生產規模 | 同上 | `doc2:79-87` | **已過期**，理由同 22.4GiB 項（float64 假設不再是預設） |

### 3. doc3（`docs/agents/pipeline-performance-work.md`）

無獨立 training 量測，僅引用 doc1 兩處作為規則示例：num_leaves/feature_fraction/bagging_fraction 消融（規則 12 示例，`doc3:131-134`，引 doc1 §5.1，判定同上「仍可信」）；4 核→8 核只快 15%（規則 10 示例，`doc3:104-105`，引 doc1 §5.2，判定同上「部分過期」）。

### 4. ADR-0014「決定 3」既知效能問題

未重新查證，直接沿用 ADR-0014 本文既有記錄：`select_shap_population` 的 `labeled` DataFrame 未 persist、rank 被算兩次（2026-08-29 grep 零命中 `.cache()`/`.persist()`）；`predict_and_write_test_predictions` docstring 現況確認「~220M rows × 2 short strings」規模字樣仍在，與 ADR-0014 引用的「2.2 億列」一致；G7 閘門仍是「未量」狀態（見第三節 3.1／3.5 的 G7、#238）。

---

## 三、未結事項

查證日期 2026-09-27，`gh issue`／`ADR` 狀態以查證當時為準。

### 1. ADR-0014 閘門表（G1–G7）現況

| 閘門 | 內容 | 現況 | 證據 |
|---|---|---|---|
| G1 | architecture-constraints.md 直接寫檔登記 3→6、R4 表格 3→2 需使用者簽核 | 已完成（隨 calibration 移除又變 6→5） | ADR-0014 本文〈決定 1〉2026-09-20 修訂：`test_direct_writes_match_registry` 現釘 `log_experiment` ＋ 4 個 cache node |
| G2 | `_populate_cache_from_hive` 要不要補進 R4 表格 | **未做**（獨立判斷，需先問使用者，尚無跡象已問） | `docs/agents/architecture-constraints.md:205` 只記錄函式已搬家，未記錄是否補進 R4 語意範圍 |
| G3 | 架構稽核 glob 放寬到 `pipelines/**/*.py`（#163） | 票已關閉，但**對 training 這塊完全沒動** | `gh issue view 163` → CLOSED；留言明寫「本票列的其餘 7 個假陰性（`comparison_nodes.py` ＋ `diagnosis/model/` 六個模組）一個都沒動」 |
| G4 | `_pdf_to_X` 改名（#199） | **已做**（2026-08-31） | ADR-0014 本文修訂記錄 |
| G5 | rank 落地＋tie-break 統一 → 另一份 ADR（原文指名 ADR-0015） | **未做，且指名的 ADR-0015 實際內容是另一件事**（比較報表母體大小改用 query group 計數，與 rank/tie-break 無關）——這件事沒有被任何後續 ADR 接手。**注意**：#402（OPEN）「同分時可自訂 item 優先順序」是相關但範圍更窄的另一張票，不等於本項；且 blocked on #355/#378 | `docs/adr/0015-compare-population-counted-in-query-groups.md` 全文與 rank/tie-break 無關；`conf/base/catalog.yaml` 的 `training_eval_predictions` 至今無 `rank` 欄；`gh issue view 402` |
| G6 | calibration 去留 | 已關閉（2026-09-20，#411 決定移除，#413/#414/#415/#417 執行） | ADR-0014 本文修訂記錄；`gh issue view 413/415/417` 全數 CLOSED |
| G7 | `select_shap_population` 的 `.persist()` 在生產資料量下峰值記憶體/磁碟未實測 | **未做，已開票追蹤（#238 OPEN）**——`.persist(StorageLevel.MEMORY_AND_DISK)` + `finally: unpersist()` 已落地，但 StorageLevel 選擇純推理、無實測數字佐證 | `src/recsys_tfb/diagnosis/model/population_spark.py:65-114`；`gh issue view 238` → `{"state":"OPEN","title":"量 select_shap_population 的 persist 峰值記憶體／磁碟（ADR-0014 閘門 G7）"}` |

### 2. ADR-0014「刻意不做」8 件事——現況

1. **diagnosis 獨立成一條 pipeline** — 未做（刻意，待另一場裁決）。證據：`src/recsys_tfb/pipelines/` 只有 dataset/evaluation/inference/source_etl/training，無獨立 diagnosis pipeline；7 個 diagnosis node 的 `def` 仍在 `diagnosis/model/` 下。
2. **rank 落地＋tie-break 統一** — 未做。見上表 G5（承諾轉交的 ADR-0015 實際未接手此題）。平手實際頻率仍未量過。
3. **HPO 平行執行** — 未做（刻意，無人要求）。`grep -rn "n_jobs" src/recsys_tfb/pipelines/training/` 零命中；checkpoint 現況（`write_checkpoint` 兩個獨立檔案各自 `os.replace`，無跨檔鎖）仍不支援多行程安全共用。
4. **`_pdf_to_X` 改名（#199）** — 已完成（2026-08-31，含 9 個模組＋別名清理）。
5. **`_REQUIRED_COLUMNS`（#220）** — 已完成（`gh issue view 220` → `stateReason: COMPLETED`）。
6. **calibration 去留** — 已決定並執行移除（#411/#413/#414/#415）。
7. **診斷失敗該不該停 pipeline** — 未做（刻意，另開一題）。`select_shap_population` 仍是 best-effort try/except、只 warn。
8. **`cache.root` 相對路徑** — 未做。`conf/base/parameters_training.yaml:232` 仍是 `root: data/recsys_cache`；`steps/local_cache.py:125` 的 `root = Path(cache_cfg.get("root", ...))` 全函式無 `.resolve()`。

### 3. ADR-0014「這份沒有回答的事」8 條——現況

1. PR 怎麼切／順序 — 交給 to-tickets/triage，不追蹤現況。
2. node 數量沒有實跑 `--list-nodes` 對照 — 未見後續驗證記錄。
3. 「一個 helper 承載幾個決策」語意判定 — 不適用「已做/未做」二分。
4. 決定 1 連鎖效應沒有實測搬檔 — 已隨後續 PR（#230/#232 等）落地，見〈既有登記的一個缺口〉2026-08-30 記錄，此項已透過後續實作間接驗證。
5. training.md 只讀部分章節可能有衝突 — 交叉核對後**無衝突**（training.md 是 ADR-0014/0028 落地後的現況文件，多處明確引用兩份 ADR）。
6. 象限診斷模組是否死碼 — 已有證據澄清，非未結事項。
7. `select_shap_population` 平手頻率未量 — **仍未量**，與 G5/項目 2 是同一件事。
8. persist 生產資料量記憶體/磁碟未量 — **仍未量**，即 G7。

### 4. ADR-0028（test 指標由設定決定）——已實作，非僅裁決

**確認已落地**（非僅「accepted」裁決）：`compute_test_metrics` 已取代 `compute_test_mAP_spark`（`src/recsys_tfb/pipelines/training/nodes.py:1416`）；`test_metrics:` 設定鍵已在 `conf/base/parameters_training.yaml:190`；`src/recsys_tfb/evaluation/metric_registry.py` 已存在。

ADR-0028 本文〈後果〉段落自己列的「沒有解決的事」（逐條查證）：

| 項目 | 現況 | 證據 |
|---|---|---|
| 「每個 time 值各算一個分數」（promote 該看最新一個月、各月平均、還是各月都要贏）未決 | **仍未做** | `grep -n "warn_about_surplus_partitions\|by_snap_date" scripts/promote_model.py` 無相關的 per-time-值分數邏輯 |
| 考卷只認 time 值、不認母體（不同 `base_dataset_version` 但同月份可能是不同考卷，promote 看不出來） | **仍未做** | 同上，ADR-0028 文中原話，未見後續補丁 |
| 表裡已不在 item 清單上的 item，預測分區仍被算進去，維持只警告 | **仍是只警告** | `src/recsys_tfb/pipelines/training/nodes.py:135,1202` 呼叫 `warn_about_surplus_partitions`，函式名本身即「warn」，非阻擋 |
| evaluation 不讀 training 算好的結果（決定 5） | 維持現狀（刻意），非缺口 | ADR-0028 決定 5 本文 |

### 5. 已關閉 issues（#222/#235/#199/#313/#451/#413）留下的未結事項

查證方式：`gh issue view <n> --repo curtis-lu/recsys-demo --json body,comments,title,state` 讀 body 與留言，再對照 main 程式碼。

| # | 來源 | 內容 | 現況 | 證據 |
|---|---|---|---|---|
| C-1 | #222 body Out of Scope；#418 | diagnosis 要不要獨立成 pipeline；7 個 diagnosis node 該不該搬回 `training/nodes.py` | **決定已翻轉、實作未動**：#418 記載 2026-09-20 拍板「diagnosis 不獨立成 pipeline，7 個 node 要搬回 `training/nodes.py`」——**這與 ADR-0014 決定 6（node 留在 `diagnosis/model/`）方向相反**，但尚無 PR，且 （a）搬法 （b）例外登記 （c）與「大幅簡化診斷」的先後三件事仍待裁決 | `grep -n "^def compute_feature_statistics\|^def compute_feature_importance\|^def compute_gain_ledger\|^def compute_shap_diagnostics\|^def select_shap_population\|^def compute_quadrant_profiles\|^def compute_quadrant_cases" src/recsys_tfb/pipelines/training/nodes.py` 零命中；7 個 def 仍在 `diagnosis/model/`；`docs/agents/pipeline-node-design.md:744` 例外登記仍寫舊理由（「未來要搬去 evaluation」），與新決定不符 |
| C-2 | #222 body Out of Scope | rank 落地＋tie-break 統一（ADR-0014 原指 ADR-0015 承接） | **未做**（與 G5 同一件事） | ADR-0015 實際主題不同；`gh issue list --search "tie-break OR rank 落地"` 找到 #402（OPEN）但範圍窄（僅同分自訂優先序），非同一件事，且 blocked on #355/#378 |
| C-3 | #222 body Out of Scope | HPO 平行執行 | 未做，無人排期 | `gh issue list --search "HPO 平行"` 查無開票 |
| C-4 | #222 body Out of Scope | `cache.root` 改絕對路徑 | 未做 | 見上表「刻意不做」第 8 條 |
| C-5 | #222 Further Notes（G7） | `select_shap_population` persist 峰值記憶體/磁碟未實測 | **未做，已開票 #238（OPEN）** | `gh issue view 238` → OPEN；程式已寫 `StorageLevel.MEMORY_AND_DISK`（`population_spark.py:69`）但無實測數字 |
| C-6 | #222 Further Notes（ADR-0014〈刻意不做〉第 7 條） | 診斷失敗該不該停 pipeline（`select_shap_population` best-effort try/except） | 未決、未做，且**無獨立票追蹤** | `gh issue list --search "診斷失敗 停 pipeline"` 查無票 |
| C-7 | #235 留言（2026-08-30，非 AC 正文） | `persist_sample_weight_report` 建議改名為 `compute_sample_weight_report`（函式已只 `return diag`，函式名「persist」已名不符實） | **未做，且無獨立票** | `nodes.py:225` 仍是 `def persist_sample_weight_report`；`pipeline.py:23,94`／`steps/sample_weights.py:4` 同樣用舊名；`gh issue list --search "sample_weight_report rename"` 查無票 |
| C-8 | #235 AC 正文 + #222 留言 | `_composite_key_series`／`_translate_weight_table` 去底線 | **已做** | `composite_key_series` 已改名（`io/extract.py:87`）；`translate_weight_table` 後因 #297（CLOSED）被 `weight_key_decode_map` 等取代 |
| C-9 | #313 body Out of Scope 第 2 項 | LTR（ranking objective）下 `sample_weights` 地板公式未定義（現行雙因子公式為 pointwise/binary 設計） | **未做，僅記錄成「已知缺口」，無推導** | `docs/operations/user-guides/sampling-overrides-editor.md` 只補了適用範圍說明，無新公式；查無獨立追蹤票 |
| C-10 | #313 收尾留言（2026-09-08，附帶發現） | sample_weight 整體乘常數（只變尺度）會讓 lambdarank macro per-item AP 從 0.2723 大幅跳到 0.3311，8 條假設排除後仍未查出原因 | **未解決，未開票追蹤根因**（僅記錄在 notes，確認「對已交付內容無影響」但異常本身沒解開） | `docs/notes/2026-09-08-lambdarank-weight-scale-anomaly.md` §5/§6 仍在檔案中；`gh issue list --search "weight-scale-anomaly"` 只命中 #313 本身 |
| C-11 | #451 body Out of Scope（多項，均標註見 ADR-0028〈沒有解決的事〉） | （a） 每月各自算分數 （b） 排序類逐月小計 （c） promote 檢查母體是否相同 （d） 對指定版本重算分數的獨立腳本 | **全部未做** | 與第 4 節表格同一組項目；`gh issue list` 除 #452/#453/#454（已關閉、屬 #451 本體交付）外查無延伸票 |
| C-12 | #413/#411 鏈（body 提及） | 四張表的 `score_uncalibrated` 欄位（恆等於 `score`）正式拿掉 | **未做，已開票 #412（OPEN）** | `gh issue view 412` body 列出開工前待確認兩件事（下游是否已改讀 `score`；Spark 3.3 對 Hive v1 表 `ALTER TABLE DROP COLUMN` 需本機實測），顯示尚未開始 |
| C-13 | #413 留言（2026-09-20） | `docs/diagrams/*.mmd` 與 `data-lineage.html` 仍畫著已移除的 calibration | **已做** | `gh issue view 415`／`417` → 皆 CLOSED，由 #415/#417 承接完成 |
| C-14 | #413 留言 | `core/consistency.py` 的 `RETIRED_CALIBRATION_KEYS`／A37 擋殘留 calibration config 鍵 | **已做** | #414（CLOSED）把 `RETIRED_CALIBRATION_KEYS` 從 2 個鍵擴到 6 個 |

**補充：查證過程中碰到的相關票（2026-09-27 的狀態，供交叉核對，避免重複發現）**：

- **#238**（OPEN）＝ C-5／G7。
- **#412**（OPEN）＝ C-12／score_uncalibrated。
- **#418**（OPEN，"Training pipeline 程式碼審查：待改清單"）：除 C-1 外另兩項獨立小項未做——（1） `LightGBMAdapter.prepare_train_inputs` 的快取相容性檢查（zero-positive group filter 過期＋sample weight key 過期）仍是兩段各自 inline `if`，未抽成 checker 清單（`grep -n "_CACHE_STALENESS_CHECKS" src/recsys_tfb/models/lightgbm_adapter.py` 零命中）；（2） `pdf_to_X`（`io/extract.py:984`）挑 feature 欄仍用 `pdf[feature_cols].copy()`，未換成同檔已證明快 1000 倍的 `_narrow_frame`。該票 item 4 已由 #451 解決。
- **#402**（OPEN）：同分自訂 item 優先順序，範圍窄於 C-2，blocked on #355/#378。

**已核對為「已解決、非未決缺口」，不需再處理**：G2（`_populate_cache_from_hive` 補進 R4 已裁決不補）、#317／#318（CLOSED）、#240／#254（CLOSED，多欄 entity 與 preprocessing 底線名）。

### 6. 其他未結事項（非任一 ADR 閘門或既有票號）

**`predict_manifest.json` 無 `run_id`** — 未做，且**尚未開票**（文件承諾「另一張票」但查無對應 issue）。切片跳過 predict node 時，`predict_manifest.json` 是舊 run 寫的，與同層 `manifest.json` 的新 run_id 對不上。證據：`docs/pipelines/training.md:533`；`gh issue list --search "run_id"` 只命中已關閉、無關的 #195（inference）。

### 7. staged-modeling 對 training 的接縫需求清單（12 條）

**spec 狀態判定：仍有效**（非被推翻）。分支 `feat/staged-modeling`（及 `feat/staged-stage2`/`feat/staged-diagnostics`/`feat/staged-docs`）曾完整實作此 spec 成 4 個 stacked PR（issue #117-120），2026-09-02 全部關閉（非 merged），理由「設計預計棄用重寫」，但關閉留言明確指定：「設計 spec 仍是唯一真實來源：`docs/superpowers/specs/2026-07-23-staged-modeling-design.md`（在 `feat/staged-modeling`）。重寫時沿用，別憑摘要。」即：**被推翻的是那一輪實作，spec 本身仍是未來重寫的依據**。main 上完全沒有任何 staged-modeling 程式碼。

讀取方式：`git -C /Users/curtislu/projects/recsys_tfb show feat/staged-modeling:docs/superpowers/specs/2026-07-23-staged-modeling-design.md`

| # | 接縫需求 | spec 章節 | 現況 | 證據 |
|---|---|---|---|---|
| 1 | 多模型／分群各自訓練＋各自 HPO | §0、§2.2 步驟2-4、D2/D7/D12 | **完全沒有** | `pipelines/training/pipeline.py:32-36` 明文寫死「no second model-producing node and no conditional branch」；`get_adapter()` 全 repo 僅 4 處呼叫，每次皆單例 |
| 2 | Stage-1 分數當 Stage-2 特徵（OOF K-fold stacking） | §2.2 步驟5、D4/D5、§3.2 | **完全沒有** | `models/base.py:19-29` `ModelAdapter.train()` 簽名無「另一模型分數當額外欄位」概念；無 OOF cross-fitting 機制 |
| 3 | 可替換的 `CompositeModelAdapter`（分群路由＋疊 Stage-2） | §7、D1 | **介面層部分可插拔、實作完全沒有** | `ADAPTER_REGISTRY`/`get_adapter()` 理論上可插拔，但 `models/lightgbm_adapter.py:224-232` save/load 是單一 filepath，`io/model_adapter_dataset.py:40-47` 假設一個 model_version=一個模型檔 |
| 4 | Config 驅動的 pipeline 分支（`model_structure: shared|staged` 切 DAG 形狀） | §10（D14）、D1 | **框架層完全沒有分支原語** | `core/pipeline.py` 的 `Pipeline` 建構子只吃靜態 `list[Node]`（Kahn topological sort），無 conditional/branch；`core/node.py:20-24` 無 predicate 欄位；`__main__.py` 無 `model_structure`/`--model-structure` |
| 5 | Consistency allowlist predicate（`partition_keys` 過欄位白名單，同型 sample_weight_keys） | §9 項1-6、D2 | **同型 pattern 已存在可套用，staged 專屬 predicate 沒有** | `sample_weight_keys` 等價 predicate 在 `core/consistency.py:1999-2070`；`grep -rn "model_structure\|staged" core/consistency.py` 零命中 |
| 6 | 訓練時資料閘（per-group 列數/正負例門檻，fail-fast） | §9 項9-11 | **完全沒有**（因無分群概念） | `pipelines/training/nodes.py` 全 1587 行無 per-group gate |
| 7 | 分群鍵掛進 `dataset.carry_columns` 觸發重建 | §4、§5、D9 | **已支援**（唯一現成機制可直接重用） | `core/consistency.py:62,646,754-759,2002-2023,3569-3570` |
| 8 | `model_version` 雜湊零修改自動納入新 `training.staged.*` 鍵 | §4、D1 | **已支援**（設計上本來如此） | `core/versioning.py:252-273` docstring：「新鍵預設被納入雜湊，safe over-invalidation」 |
| 9 | Atomic 多檔 bundle 儲存/載入（一個 model_version 目錄裝多個模型檔＋index） | §4、§7、D1 | **完全沒有** | `LightGBMAdapter.save/load` 操作單一 filepath；無目錄式 bundle/index 校驗 |
| 10 | 獨立於現行 HPO 的 per-group in-memory、無 resume、確定性種子 Optuna study | §3.1、D7/D12 | **完全沒有** | 現行 `hpo_resume.py`／`hpo_scoring.py` 皆為單模型、可 resume 設計，無多副本平行變體 |
| 11 | 診斷層 dispatch（SHAP/importance 同時服務單一 booster 或 staged adapter） | §6 | **完全沒有** | `grep -rn "resolve_attribution_inputs" src/` 零命中；`pipeline.py:158-177` 診斷節點群組假設單一 adapter 形狀 |
| 12 | 未見分群值的分流路由（inference: skip+WARN；evaluation: fail-fast） | §7、D11 | **完全沒有** | `pipelines/inference/nodes.py:237` `predict_and_write_scores` 對整個 chunk 統一套用同一 adapter，無 per-row 分群成員檢查 |

補充：spec §6 對應的 `diagnosis/model/staged.py`（已關閉分支曾新增 214 行）在 main 上不存在——這是第 11 條接縫在輸出檔案結構層面的具體體現。

### 8. diagnosis／HPO 設計 spec 的範圍外／延後事項

以下項目來自 `docs/superpowers/specs/2026-06-28-training-diagnostics-part-a-design.md` §12「範圍外/延後」與 `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §6「明確不做（v1 邊界）」，不在 ADR-0014／ADR-0028 之列，但與 training pipeline 直接相關：

| 項目 | 來源章節 | 現況 | 證據 |
|---|---|---|---|
| two-stage composite adapter（分階段模型的 SHAP 歸因支援） | part-a-design §12 | **未做**，唯一擴充點是 stub | `src/recsys_tfb/diagnosis/model/attribution.py:1-17`：docstring 明寫「今天走 LightGBM booster 的 TreeExplainer；這是日後支援 composite（two-stage）模型的唯一改點」，`_resolve_booster` 對非 booster 模型直接 raise |
| shaprx 整合 | part-a-design §12 | **已擱置**，非「延後」——2026-07-07 修訂已「移除 shaprx 前提」 | `docs/superpowers/plans/2026-07-07-diag-framework-HANDOFF.md:38`：「shaprx 擱置、HPO objective 不動」；`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md:3` |
| 特徵漂移 PSI/KS | part-a-design §12 | **未做**，另案，查無對應 issue/ADR | `grep -rn "PSI\|drift" src/recsys_tfb/diagnosis/` 命中的 3 處皆為「config drift」語意，非特徵分布漂移 |
| 學習曲線＋gap（`learning_curve.py`，§7.2） | part-a-design §7.2 | **未做** | 全 repo 無 `learning_curve.py`；`conf/base/parameters_training.yaml` 的 `diagnostics:` 區塊無 `learning_curve` 鍵（只有 `hpo_search`，對應另一份已完成的 spec `2026-07-15-hpo-search-diagnostics-design.md`） |
| loss-SHAP、cohort 偵測、處方自動化 | diagnosis-pipeline-integration-design §6 項2 | **未做，去留未定** | 該文件原話：「不在本案；要不要做、以什麼形式做皆未定，等本案落地取得真實讀數後再評估」 |
| influence／訓練資料歸因 | 同上 §6 項3 | **未做** | `grep -rn "influence" src/recsys_tfb/diagnosis/` 零命中 |
| HPO objective 參數化對齊（§1 不變量 5） | 同上 §6 項4 | **已由 ADR-0028 承接並實作**（非仍未決） | ADR-0028 決定 2「指標登記在一張表」＋ `src/recsys_tfb/evaluation/metric_registry.py` |
| triage 總表（Phase 5 原設計） | 同上 Phase 5 | **已退場，非未做**——違反模組邊界規則被整層移除 | `src/recsys_tfb/diagnosis/__init__.py:5-6`：「`triage`（per-item 判定＋建議槓桿）與 `quadrant`（AUC 門檻切象限）就是因為違反它而整層退場」 |
| 公司環境驗證（每階段驗收都在本機合成資料） | 同上 §6 項5 | **未做**（部署前另驗項） | 該文件原話 |
| 指標權重定案 | 同上 §6 項6 | **未做**（待真實資料讀數後裁決） | 該文件原話 |

---

## 四、跨 pipeline 共用的現況

查證日期 2026-09-27，main @ `77adca4e`。只讀調查，沒有改任何 repo 檔案、沒有跑 pipeline 或 pytest；唯一執行過的是 import 冒煙（`python -c "import …"`）與一支沒有進 repo 的一次性 AST 掃描腳本（做法見本節第 4 小節）。repo 根目錄＝`/Users/curtislu/projects/recsys_tfb`。

### 1. 各 pipeline import 了哪些頂層套件

指令（數的是 import **陳述行數**，含函式體內的延遲 import）：

```bash
cd /Users/curtislu/projects/recsys_tfb && for p in dataset training inference evaluation source_etl; do echo "=== pipelines/$p ->"; grep -rhE --include='*.py' "^\s*(from|import) recsys_tfb\." src/recsys_tfb/pipelines/$p | grep -oE "recsys_tfb\.[a-z_]+(\.[a-z_]+)?" | sed 's/^recsys_tfb\.//' | awk '{split($0,a,"."); t=a[1]; if(t=="pipelines") t=$0; print t}' | sort | uniq -c | sort -rn; done
```

結果（自己那條 pipeline 的 import 不列）：

| 使用端 ＼ 被 import 的套件 | core | io | models | evaluation | diagnosis | utils | preprocessing | report |
|---|---|---|---|---|---|---|---|---|
| `pipelines/dataset` | 13 | – | – | – | – | 3 | 1 | – |
| `pipelines/training` | 10 | 4 | 4 | 3 | 5 | 3 | 1 | – |
| `pipelines/inference` | 7 | 1 | 2 | – | – | 2 | 1 | – |
| `pipelines/evaluation` | 12 | – | – | 9 | 5 | 4 | 1 | – |
| `pipelines/source_etl` | 2 | – | – | – | – | 1 | – | – |

逐行清單：

```bash
cd /Users/curtislu/projects/recsys_tfb && for p in training inference evaluation dataset; do echo "=== pipelines/$p:"; grep -rnE --include='*.py' "^\s*(from|import) recsys_tfb\.(models|io|evaluation|diagnosis|utils|preprocessing|report)" src/recsys_tfb/pipelines/$p; done
```

重點：training 是依賴最廣的那條（七個頂層套件），inference 只碰 `core`／`io`／`models`／`preprocessing`／`utils`。

### 2. pipeline 之間互相 import：0 條

```bash
cd /Users/curtislu/projects/recsys_tfb && for p in dataset training inference evaluation source_etl; do echo "=== pipelines/$p importing other pipelines:"; grep -rnE --include='*.py' "^\s*(from|import) recsys_tfb\.pipelines" src/recsys_tfb/pipelines/$p | grep -vE "recsys_tfb\.pipelines\.$p(\.|\s|$)"; done
```

五條全部零命中。反方向（`pipelines/` 以外的 src 模組 import `pipelines.*`）只有 CLI：

```bash
cd /Users/curtislu/projects/recsys_tfb && grep -rnE --include='*.py' "^\s*(from|import) recsys_tfb\.pipelines" src/recsys_tfb | grep -v "^src/recsys_tfb/pipelines/"
```

→ `src/recsys_tfb/__main__.py:71、72、78、79、87、981`（`get_pipeline`、dataset 的 `month_plans`／`run_contract`／`ONLY_TEST_MONTHS_NODES`、training 的 `cache_sources`、source_etl 的 `sql_runner`）。`scripts/` 對 `recsys_tfb.pipelines` 零命中。

測試裡有 2 處跨 pipeline import（`tests/test_pipelines/test_training/test_pipeline.py`、`tests/test_pipelines/test_evaluation/test_popularity_rate.py` → `pipelines.dataset`）；依規則 8「測試不算」。

### 3. 函式庫層彼此之間

```bash
cd /Users/curtislu/projects/recsys_tfb && for d in core io models evaluation diagnosis utils report; do echo "=== $d ->"; grep -rhE --include='*.py' "^\s*(from|import) recsys_tfb\." src/recsys_tfb/$d | grep -oE "recsys_tfb\.[a-z_]+(\.[a-z_]+)?" | sed 's/^recsys_tfb\.//' | awk -v self=$d '{split($0,a,"."); t=a[1]; if(t!=self) print t}' | sort | uniq -c | sort -rn; done
```

| 從 → 到（行數） | 內容 |
|---|---|
| core → io 7、evaluation 4、diagnosis 1 | io 七行全在 `core/catalog.py:1-7`（模組層）；evaluation／diagnosis 五行全是 `core/consistency.py` 的函式內 import（:2220、:2370、:2652、:2726、:5820） |
| io → core 6、utils 3、models 2 | `io/model_adapter_dataset.py:9` 模組層 import `models.base` |
| models → io 6、core 3 | `models/lightgbm_adapter.py:12` 模組層 import `io.handles`；`io.extract` 是函式內（:108、:417、:436、:508） |
| evaluation → core 8、report 4、diagnosis 4、utils 2 | diagnosis 四行全在 `evaluation/report_builder.py` 函式內（:1949、:1999、:2150-2151） |
| diagnosis → core 19、report 16、io 6、evaluation 5、utils 3、models 3 | models 三行＝`models.feature_view`（shap_cases、shap_per_item、feature_stats） |
| utils → core 3 | `utils/item_columns.py:25-26`、`utils/spark.py:389` |
| report → （無） | |

### 4. 循環 import

GRAPH_REPORT 是在 `2e99c573` 建的（`graphify-out/GRAPH_REPORT.md` 開頭的 Graph Freshness），比盤點當天的 HEAD `77adca4e` 舊，所以循環另外用一支一次性的 AST 腳本重算（沒有進 repo）。做法：走過 `src/recsys_tfb/**/*.py` 的每個 import，把邊分成模組層、函式內、`TYPE_CHECKING` 三種，並模擬「import `a.b.c` 會先執行 `a/__init__`」，再找有向圖裡的環。要重現，照這段描述重寫即可。

| 層級 | 環 | 成因 | 今天會不會炸 |
|---|---|---|---|
| 模組層、只算 import 當下的邊 | `io`(`__init__`) → `io.model_adapter_dataset` → `models`(`__init__`) → `models.lightgbm_adapter` → `io.handles` → `io`(`__init__`) | `io/__init__.py` 轉出口 `ModelAdapterDataset`；`models/__init__.py` 轉出口 `LightGBMAdapter` | 不會：四個入口各自冷啟動 import 都成功（`recsys_tfb.models.lightgbm_adapter`、`recsys_tfb.io`、`recsys_tfb.models`、`recsys_tfb.io.model_adapter_dataset`）。但它能不能載入，取決於套件初始化的順序 |
| 模組層、只算 import 當下的邊 | `diagnosis.model`(`__init__`) ↔ `feature_stats`／`shap_cases`／`shap_per_item` | **GRAPH_REPORT 列的就是這一個**（:789）。`from . import data_access` 會回頭碰套件本身，而 `__init__` 又轉出口那幾個 node 函式 | 不會（`shap_per_item`、`shap_cases` 冷啟動 import 成功） |
| 套件層、只算 import 當下的邊 | {core, io, models} | `core/catalog.py` → io；io ↔ models | 同上 |
| 套件層、含函式內與 `TYPE_CHECKING` | {core, diagnosis, evaluation, io, models, utils} 六個套件全在同一個環 | 靠函式內 import 撐開（`core/consistency.py:2217-2219` 的註解明寫這是分層宣稱） | 不會，但分層只靠慣例維持 |

不在任何環裡的：所有 `pipelines.*`、`preprocessing`、`report`。**也就是說，pipeline 層今天是乾淨的樹葉，環全部在函式庫層。**

### 5. repo 對依賴方向與模組邊界的既有宣稱

| # | 宣稱 | 出處 | 當初的理由 | 機械檢查 |
|---|---|---|---|---|
| 1 | 函式庫層（`models/`、`utils/`、`core/`）對 `pipelines/` 零 import；`feature_selection` 因此放 `models/`，不放 `pipelines/training/` | `/Users/curtislu/projects/recsys_tfb/docs/adr/0008-dataset-modules-split-by-role.md:105-109` | `models/lightgbm_adapter.py` 也在用它，放進 pipeline 會製造 `models → pipelines` 的反向依賴 | 無 |
| 2 | 「產物跨 pipeline ≠ 程式碼跨 pipeline」；真正共用的 61 行留在用領域命名的單一模組 `preprocessing.py`，不放 `utils/` | 同檔 :321-329；職責定義 :111-118 | 保留整包會繼續裝只有一個消費者的程式碼；放 `utils/` 會讓「未知類別 → 哨兵值」這個 ML 決策沉進通用工具層 | 無 |
| 3 | diagnosis 依賴方向單向：`pipelines/* → diagnosis → core / evaluation（僅 metrics.py）/ io / utils`；diagnosis 不得 import `pipelines/*` | `/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/diagnosis/__init__.py:13-15`；`/Users/curtislu/projects/recsys_tfb/docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md:24` | 一個診斷域同時服務兩條 pipeline，需要一個函式庫層的家 | 無（寫著「違反即錯」，但沒有測試） |
| 4 | **pipeline 之間不互相 import；「跨 pipeline 內部 import 是禁手」；跨 pipeline 靠 catalog 產物銜接** | spec 同檔 :17、:24 | 初版「沿兩側擴充」會把一個診斷域拆成兩種架構尺度（函式庫層，以及別條 pipeline 的內部套件），評估側也永遠無法重用訓練側的象限語意 | 無（S3 只擋 `steps/`） |
| 5 | S3：pipeline 以外的 src 模組不得 import 該 pipeline 的 `steps/`；`steps/__init__.py` 不得轉出口 | `/Users/curtislu/projects/recsys_tfb/docs/agents/architecture-constraints.md:405-411`、:430；測試 `/Users/curtislu/projects/recsys_tfb/tests/test_core/test_architecture_constraints.py:617-667`、:726-752 | 讀一次目錄列表就分得出對外契約與內部步驟 | **有** |
| 6 | 規則 8：根層還是 `steps/`，看 src 側呼叫端是否全在本 pipeline 內；有外部消費者的放根層 `<contract>.py` | `/Users/curtislu/projects/recsys_tfb/docs/agents/pipeline-node-design.md:270-298`（判準 :283，契約模組實例 :290） | 同上 | 部分（S3 只擋一個方向，:294-297） |
| 7 | 規則 8 管不到 `src/recsys_tfb/evaluation/` 這「第三個位置」；`evaluation/` 的定義是「有 pipeline 以外的讀者、或被它們的依賴拉住」的共用庫 | `/Users/curtislu/projects/recsys_tfb/docs/adr/0019-evaluation-modules-split-by-role.md:59`、:63-79、:104 | 避免 `evaluation/` 繼續是共用庫與私有機制的混合層（:121） | 無 |
| 8 | 規則 5：決策各寫一份，機制才共用 | `pipeline-node-design.md:159-165`、:221-232 | 包成帶旗標的 helper 會把決策搬離 node，選錯（例如抽列還是抽 entity）不會報錯 | 無 |
| 9 | 規則 12：模組用 concern 命名，不用「helper」「shared」「common」 | `pipeline-node-design.md:376` | 名字指向不存在的東西；刻意不加命名守衛（ADR-0008:331-334） | 無 |
| 10 | `model_feature_columns` 一旦有 inference 以外的消費者就要換家，落腳 `models/` | `/Users/curtislu/projects/recsys_tfb/docs/adr/0014-training-modules-split-by-role.md:344`；`/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/models/feature_view.py:11-16` | 規則 8；它是 `feature_selection.py` 的另一半 | 無 |
| 11 | `io/extract.py` 從 `pipelines/training/nodes.py` 搬出來，是為了讓 adapter 重用而不循環 import | `/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/io/extract.py:7-9` | 避免 `models → pipelines` | 無 |
| 12 | `core/` 對上層沒有 import 當下的依賴（要用就在函式內延遲 import） | `/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/core/consistency.py:2217-2219`、:2367-2369、:3317-3318、:342；ADR-0019:217 | diagnosis、evaluation 在 core 之上；`report_builder` 會拖進 pandas 與 plotly | 無 |
| 13 | ranking 放 `utils/` 而不放 `evaluation/`，免得開出第一條 inference → evaluation 的邊 | `/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/utils/ranking.py:56-62` | 一個 window 不值得一條新的依賴邊 | 無 |
| 14 | `evaluation/config_fingerprint` 不 import `diagnosis/`；兩者在 `pipelines/evaluation` 的 node 裡組合 | `/Users/curtislu/projects/recsys_tfb/docs/adr/0020-evaluation-bug-round-intended-behaviours.md:49` | 維持單向依賴 | 無 |
| 15 | training 的 7 個診斷 node 不搬進 `pipelines/training/nodes.py` | ADR-0014:301-325；`pipeline-node-design.md:744` | 會生出 7 個薄殼；未來可能搬去 evaluation 或獨立成一條 pipeline（ADR-0014:532-550）；追 bug 要多跳一次檔 | ——（**這項決定已被推翻**，見上方第三節 5 之 C-1） |
| 16 | training／inference「編排各寫一份、機制只有一份」是 `prod_name` 被寫成整數 code 那個 bug 的來源；沒有任何機制防止再分岔 | `/Users/curtislu/projects/recsys_tfb/docs/adr/0010-inference-chunked-scoring-shape.md:286-295` | ——（這條是支持共用「編排」的證據） | 無 |
| 17 | 把 training 的續跑規劃器形狀照抄到 inference，會漏掉 `model_version` 篩選而撞在一起（collision） | ADR-0010:158-168 | 兩張表「範圍由誰保證」的來源不同 | 無 |

> 註：第 16 條的 `prod_name` 是 ADR-0010 記錄的一起歷史事故裡的具體欄名，照 ADR 原文引用，不是框架的欄位角色。

**這些慣例為什麼會長成這樣（歸納）**：歷次的做法都一樣：**某段程式碼一出現第二條 pipeline 的消費者，就往下搬到一個用領域命名的頂層位置，從來不讓 pipeline 互相 import。** 實例有五個：`preprocessing.py`（ADR-0008）、`models/feature_selection.py`（ADR-0008）、`utils/hashing.py`（從 `pipelines/dataset/_hashing.py` 搬出來，spec 2026-07-06 :33、:59）、`models/feature_view.py`（ADR-0014 決定 7）、整個 `diagnosis/`（spec 2026-07-06）。

理由有三層：（1）函式庫反過來依賴 pipeline，會讓函式庫變成某條 pipeline 的附庸（宣稱 #1）；（2）pipeline 彼此對等、各自演進，互相 import 會讓一條 pipeline 的內部重構打壞另一條（宣稱 #4）；（3）目錄列表要說真話（宣稱 #5、#6）。

**宣稱跟實況已經對不上的地方**：

- **diagnosis 的方向宣稱漏了兩條邊。** 宣稱 #3 只列 `core / evaluation(metrics.py) / io / utils`，實況還 import `models`（3 行）與 `report`（16 行）。反方向，`evaluation/report_builder.py` 也 import `diagnosis`（函式內 4 行），所以「單向」在套件層並不成立。
- **ADR-0019:68 說 training 用 `metrics_spark` 的 `compute_all_metrics`**，但現在 training 只 import `count_query_groups_by_time`（`/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/pipelines/training/nodes.py:94`）；`compute_all_metrics` 的呼叫端只剩 `pipelines/evaluation/nodes.py`。另外 `evaluation/metric_registry.py` 在 pipeline 端的讀者**只有 training**，`pipelines/evaluation` 沒有 import 它。
- **「頂層套件之間的方向」沒有任何一條進了 `architecture-constraints.md`。** 書面的方向宣稱散在 5 處（ADR-0008、ADR-0020、spec 2026-07-06、`diagnosis/__init__.py`、`consistency.py` 的註解），全部沒有機械檢查。

### 6. 「跨 pipeline 共用」本身的壞處：看起來一樣、其實該分開的

1. **identity 不同，重複列檢查就不同。** training 把選用角色欄寫進輸出（`training/nodes.py:1270-1281`）；inference 刻意丟掉它們（`identity.py:15` 起的 docstring、ADR-0025 決定 1）。`validate_scored_chunk` 的 `no_duplicates` 用的是 inference 的定義（`validation.py:206`）。宣告了選用角色欄時，training 一個 (time, entity, item) 本來就有多列，照原樣共用會誤報重複。
2. **選用角色欄的型別。** entity 欄兩邊都轉 `str`，但 training 對選用角色欄刻意**不轉**（`training/nodes.py:1278-1280`：該欄可能是時間戳記，tie-break 要照它自己的型別比）。共用的組裝器若一律轉字串，`"10" < "2"` 會讓名次悄悄重排。
3. **`item_values_are_known` 只對 inference 有意義。** 它守的是 inference「從迴圈變數寫 item」那一次指派（`validation.py:144-150`）。training 的 item 來自資料分區，照搬過去是一條永遠綠的裝飾性檢查。
4. **組合模型遇到沒見過的分群值，政策刻意相反**：test 預測 fail-fast，inference 跳過並 WARN（staged spec D11 與 §7，`feat/staged-modeling@613cff8d:docs/superpowers/specs/2026-07-23-staged-modeling-design.md:34`、:194-201）。能共用的只有機制（比對分群值集合、統計缺了哪些群），政策留在 node。
5. **要帶的欄與分區配置**：label 與零正例組權重，對上 `entity_bucket`；(time, item) 對上 (time, item, bucket)。
6. **續跑規劃**：training 以月為單位、config 是權威（`steps/predict_months.py`），inference 以 chunk 為單位（`steps/chunk_plans.py`）。ADR-0010:158-168 已經證明照抄形狀會撞在一起。
7. **分數欄名**：training 寫死 `"score"`（:1282），inference 用 `schema["score"]`（:466）。共用組裝會逼出一個答案；目前相安無事，只是因為 catalog 的兩張表都宣告了 `score`（`conf/base/catalog.yaml:398`、:533）。
8. 讀取路徑（parquet cache 對 Hive 分塊）：已排除在共用範圍外，這裡照列只為完整。

**還有一個結構性代價：** `model_version` 的雜湊不含程式碼版本（`/Users/curtislu/projects/recsys_tfb/src/recsys_tfb/core/versioning.py:276-295`）。training「已經寫過就跳過」之所以安全，前提是同一個 model_version 的預測不會變（`training/nodes.py:1187-1191`）。若因 inference 需求改評分程式碼，會改變 training 的輸出：舊月份被跳過、新月份用新程式碼，靜默混版。今天 training 自己改程式碼也有同樣風險，這件事本身不隨共用與否而消失，但要在 spec 裡決定操作者怎麼知道該帶 `--rebuild-dates`（dataset 那邊的對應物是規則 18 的 `DATASET_ARTIFACT_FORMAT_VERSION`，model 這邊沒有）。

---

## 五、不確定與未查事項

### 1. 程式形狀盤點（第一節）

- **沒有任何量測**：第一節第 5 節全部是讀 code；37–89 GiB、2.2 億列是程式註解裡的數字。發現 #6 的「約 2 倍峰值」是從程式推的，生產的 train 矩陣實際多大沒查。
- 發現 #7（缺格式版本）的實際發生頻率沒查：要看歷史上有多少 training 程式改動改變了模型或 trial 分數而設定沒動（例如 #315、#318、#355 那一類）。
- `docs/pipelines/training.md` 與 `docs/operations/` 沒讀；只核對了程式內的 docstring。`docs/operations/user-guides/adding-an-eval-month.md:119` 說「`--from-node` 會把超參數搜尋拉進來」。照 `core/pipeline.py:133-145` 的 `slice_from`（排序位置之後的 node，加上缺料時才拉回的上游）推，HPO 排在 predict 之前，只有 `best_params` 等產物缺檔時才會被拉回，所以那句看起來不對；沒實跑 `--dry-run` 證實，不列入發現。
- `scripts/`、`tests/`、`examples/ad/` 沒掃 LightGBM 綁定。
- 各 node 的測試覆蓋沒有逐一看。

### 2. 未結事項與效能量測（第二、三節）

1. **G2**（`_populate_cache_from_hive` 要不要補進 R4 表格）：查了 `docs/agents/architecture-constraints.md`，只確認函式已搬家到 `steps/local_cache.py`，查不到是否已問過使用者、是否已有裁決。
2. **`predict_manifest.json` 加 run_id**：`docs/pipelines/training.md:533` 承諾「另一張票」，但 `gh issue list --search "run_id"` 查無對應 issue，可能是口頭承諾尚未落票，也可能票名用了查不到的關鍵字。
3. **診斷失敗該不該停 pipeline（第三節 3.5 C-6）**：查了 issue 搜尋與 ADR 原文，查不到任何獨立追蹤票或後續決定，只能確認「原地不動」。
4. **G7/G5 的「未做」判定**：基於文件與 grep，不排除有未寫進文件的口頭決定；已用 C-5(#238)/C-2(#402) 部分交叉驗證，但無法排除還有其他未書面化的決定。
5. **doc1 §4.3 其餘 13 個未逐一列名的 node**：doc1 本身未寫出這 13 個 node 的名字，無法逐一對照 git log 判斷是否過期，只能整體標記「未逐一查證」。
6. **doc2 §9.3 對 doc1 的引用行號**：今日行號已飄移，`nodes.py:1141` 這個舊行號已不對應原內容，改以 docstring 文字比對確認規模數字一致，但精確行號需要時要重新 grep。

### 3. 跨 pipeline 共用（第四節）

1. **最關鍵的分析靠一份還沒 merge 的 spec。** staged modeling spec 在 `feat/staged-modeling`（`613cff8d`）；第一輪 PR 已關閉、預計重寫，但關閉留言說 spec 本身仍然有效。「分群鍵不保證在特徵裡」這一點是從 `compute_feature_columns` 的邏輯（特徵只從 `feature_table` 推）與 spec D2／§7 推出來的，沒有實跑。
2. **組合模型的 predict 介面究竟收矩陣還是收 frame**，spec 寫「對外實作既有 `ModelAdapter` 介面（predict/save/load）」，沒有定案。這會直接決定建矩陣放在接縫的哪一側，需要在 spec 裡拍板。
3. **`diagnosis` 是否仍打算獨立成 pipeline**（ADR-0014 刻意不做第 1 條）：待裁決，需要問使用者。
4. **training 寫死 `"score"` 對上 inference 用 `schema["score"]`**：是讀程式碼看到的。若把 `schema.columns.score` 改成別的名字，training 會不會寫錯欄，沒有實跑驗證。
5. **io↔models 那個套件初始化環**：四個入口冷啟動 import 都成功，但對順序的敏感度沒有壓力測試。
6. **GRAPH_REPORT 過期**（建於 `2e99c573`），循環是用另一支自寫的 AST 腳本重算的。GRAPH_REPORT 列的那一個有重現；腳本另外抓到兩個它沒列的（io↔models、套件層）。腳本本身沒有經過獨立審查。
7. **沒有量任何耗時或記憶體。** 把建矩陣移到 adapter 後面，在結構上不應該改變每塊的成本，但沒有量測佐證。
8. **寫出模組的命名與落點是品味題**，信心低，不在本筆記範圍。
