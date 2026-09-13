---
status: accepted
date: 2026-09-13
---

# evaluation 的效能：在生產者端物化、把固定次數的 job 收斂、監控模式不跑 registry 診斷

> **要知道「怎麼判斷一件事該不該優化」，讀 [`pipeline-performance-work.md`](../agents/pipeline-performance-work.md)；要知道 evaluation 這次為什麼選這幾刀，讀這份。**
>
> - **判準不在這裡。** 效能工作的推理紀律在 `pipeline-performance-work.md`（本份稱「perf 規則 N」），node 的形狀在 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)（「node 規則 N」），流程在 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)（「flow 規則 N」）。三份各自從 1 編號，所以本份每次引用都帶前綴。本份只記「把那三份套到 evaluation 時，量到了什麼、選了哪一邊、為什麼不是另一邊」。
> - **結構重整不在這裡。** 模組怎麼分層是 [ADR-0019](0019-evaluation-modules-split-by-role.md) 的事。兩份分開，因為它們在不同時間變成事實：本份的決定做完就不動，ADR-0019 會在 bug 修正那一輪被反覆修正。
> - **bug 清單不在這裡。** 稽核與複核的逐條記錄在 [`docs/notes/2026-09-09-evaluation-audit.md`](../notes/2026-09-09-evaluation-audit.md)（與本份同一批進 main）。本份只引用其中改變了決定的幾條。
> - **行號會腐爛**，所以本份盡量只給檔名與函式名。少數必要的行號標了核對日期（2026-09-13，main @ `6898179`）。
> - **這份被 fresh-context 審查過兩輪**（引證核對、決定健全性），改掉的地方都寫進了對應決定的正文，沒有另留「更正」節——因為它還沒進 main。進 main 之後的偏離照 flow 規則 2 回頭改。

---

## 這份在解什麼問題

evaluation pipeline 拿模型排好的候選名單跟真實答案對一遍，出一份 HTML 報表。它的成本在 2026-09-09 至 09-12 用 `local[*]`（單一 JVM、driver 4g、PySpark 3.3.2、`--post-training` 模式）量過一輪，資料量從 654 個 query group 掃到 654,000 個。**秒數不能搬去生產**（單機與叢集是不同尺度），但「次數」可以，因為它們在資料量差 1,000 倍、正樣本率差 5 倍、換成監控模式時**一次都沒動**：

| 量到的事實 | 數字 | 為什麼是結構性的 |
|---|---|---|
| `prepare_eval_data` 產出的 join 鏈被下游各自重掃 | **57 個 SQL execution** 各自從來源表掃一次 | `eval_predictions` 只是一份 Spark 計畫，沒有 cache、沒有落地；Runner 對每個下游 `catalog.load()` 到的是同一份計畫 |
| `compute_dataset_overview` 固定發出 | **148 個 job／421 個 stage** | 它每個統計量各自一次 action（每次呼叫 8–9 個，被叫兩次），每個 action 一個或多個 job，數量與資料無關；executor 時間卻超線性成長（資料 ×10、時間 ×11.8），654,000 個 query group 時佔總成本 41.6% |
| `compute_all_metrics` 算了沒人讀的 NDCG | 每個 K 各算一次 iDCG、收兩次 | 報表層用 `_HIDDEN_METRIC_PREFIXES` 濾掉，`evaluation_results.json`、promote、MLflow 都不讀它 |

**否定結果也要留下**（perf 規則 9）：正樣本率 10% → 50%，總時間幾乎不動（±20%，非單調＝雜訊）。只有五個診斷 node 隨正例數線性成長，加起來佔 1.6–7.7%。所以「示例資料正例密度 100% 會讓排名失真」這個顧慮**已被推翻**，下一輪量測不必再掃這條軸。

**沒有生產環境的數字。** 生產無法量測，所以本份的目標數字全是次數，不是秒數（perf 規則 11 要求有目標；perf 規則 8 說秒數不可跨環境搬）：

| 目標 | 現在 | 做完之後 |
|---|---|---|
| 同一條 join 鏈被掃幾次 | 57 | 1 次 join ＋ N 次讀物化表，N 用同一把尺量出來 |
| `compute_dataset_overview` 發幾個 job | 148 | ≤ 20 |
| NDCG 相關的 action | 每個 K 兩次 | 0 |

量尺是 Spark event log 離線解析（jobGroup 標籤歸因、重掃偵測）。量測台架已刪，重建方式見〈驗收〉；上面三個「現在」的數字來自已刪的台架，**repo 裡無法重驗**，重建台架後第一件事是重量一次基線。

---

## 決定 1：`prepare_eval_data` 直接寫進 Hive 表，每個消費者自己限縮到評估月份

### 現在的形狀

```
ranked_predictions ─┐
label_table ────────┼─► prepare_eval_data ─► eval_predictions（記憶體，只是一份計畫）
                                                   │
                     ┌─────────┬─────────┬─────────┼─────────┬─────────┐
                     ▼         ▼         ▼         ▼         ▼         ▼
                  metrics  baseline  aggregates  sample   compare   persist_eval_predictions
                  （每個下游各自把 join 從頭再算一次）                  └─► Hive: enriched_eval_predictions
```

那張 Hive 表**已經存在**：`enriched_eval_predictions`（`conf/base/catalog.yaml`，`HiveTableDataset`、`partition_filter: model_version`、`partition_cols: snap_date`、`columns: "auto"`），內容就是 `prepare_eval_data` 的輸出，只是由**最後一個** node `persist_eval_predictions`（`comparison_nodes.py`，純 pass-through）寫。catalog 側的讀者只有 `--compare-only` 模式；另外 `evaluation/comparison/sources.py` 的 `MODEL_VERSION_SOURCES` 把它當 `kind: model_version` 比較來源的預設表，直接 `spark.table()` 讀、自帶 model_version 與 snap_date 的 filter——那條路不受本決定影響。

### 改成

```
ranked_predictions ─┐
label_table ────────┼─► prepare_eval_data ──► Hive: enriched_eval_predictions
                                              （這個 model_version 的所有月份；join 只算這一次）
                                                   │  catalog 原樣讀回（partition_filter 只剪 model_version）
        ┌─────────────────┬────────────────────────┼────────────────────┐
        ▼                 ▼                        ▼                    ▼
 compute_metrics   compute_baseline_metrics   draw_diagnosis_sample_node   restrict_to_common ...
 # Decision — 只評這一個月                          （五個消費者各自一行，機制共用）
 df = restrict_to_eval_snap_date(df, parameters)
```

三個變化：

1. **`prepare_eval_data` 的 `outputs` 改成 `enriched_eval_predictions`**。Runner 對它 `catalog.save()` 時 `HiveTableDataset.save` 就把 join 算完 `insertInto` 分區；之後每個消費者 `catalog.load()` 到的是 `spark.sql(SELECT … WHERE model_version = …)` 的新計畫——讀表，不再重做 join。`DataCatalog` 對已 save 的 Hive dataset 不留記憶體副本（`core/catalog.py`），這條鏈審查時逐段追過並實跑確認。
2. **`persist_eval_predictions` 刪除**。它存在的唯一理由是「把記憶體裡的 frame 接到 catalog 的 auto-save 邊」，而現在生產者自己接。
3. **每個消費者 node 的第一步是「只留這個月」**。機制放在一個新的 `steps/` 模組（暫名 `pipelines/evaluation/steps/snap_date_scope.py::restrict_to_eval_snap_date(df, parameters)`），**簽章吃 `parameters`、在 step 內讀 `evaluation.snap_date`**，不是每個消費者各讀一次——理由見下面 S6 那段。比較用 `F.col(time_col).cast("string") == snap_date`，沿用 `--compare-only` 現有 B4 檢查的寫法：字串分區欄轉字串是空操作，審查時用 physical plan 實跑確認分區裁剪成立（`PartitionFilters: [(snap_date = …)]`、`DataFilters: []`）。**不用** dataset 那個 `to_date()` 的形式，因為對字串分區欄做 `to_date` 會不會讓 Spark 放棄分區裁剪，沒有量過。

### 為什麼是「每個消費者自己篩」，不是多一個 node

這是本份改過一次立場的地方，理由要寫清楚。第一版設計多了一個 `keep_snap_date` node：讀表、篩月、輸出記憶體裡的 `eval_predictions`，五個消費者不動。它被否決，理由是 repo 自己的規則，不是偏好：

- **node 規則 1**：node 邊界要落在「撈得出來看」的產物上。那個 node 的輸出沒有 catalog 條目、沒有 log、沒有測試單獨讀它——正是該合併掉的邊界。
- **node 規則 5**：決策重複寫兩份，機制才共用。dataset pipeline 就是這樣做的：`select_train_keys`、`select_calibration_keys`、`build_test_model_input` 各自呼叫一次 `restrict_to_months`（`pipelines/dataset/steps/scoping.py`），catalog 對來源表什麼都不設。「要哪幾個月」是每個 node 的一個決定，不是 catalog 或某個中介 node 的事。

**代價一：五個 node 各多一行，而且忘了篩不會有錯誤訊息。** 以後有人加第六個消費者忘了篩，會**安靜地把所有月份算進去**（`enriched_eval_predictions` 累積這個 model_version 評估過的每個月）。兩層護欄：

- **機械的**：一條測試（放 `tests/test_pipelines/test_evaluation/`），用 AST 掃 node 定義所在的模組——凡是在 `pipeline.py` 被接上 `enriched_eval_predictions` 這個 input 的 node 函式，函式體內必須有一個對 `restrict_to_eval_snap_date` 的呼叫。這才是擋得住「第六個消費者」的東西。測試要從 `pipeline.py` 的 import 反查模組，**不要寫死檔名**：本決定落地時檔還叫 `nodes_spark.py`，改名成 `nodes.py` 是 ADR-0019 那張票的事。
- **後置條件**：`compute_metrics` 已經在 `compute_dataset_overview` 裡算 `n_snap_dates`（在篩之後算），加一句「必須等於 1，否則 raise」。它擋的不是忘篩（忘篩的 node 不是它），是**分區為空**（篩完 0 列 → `n_snap_dates` 是 0）。依 node 規則 11，docstring 標明是**後置條件**。

**代價二：S6 登記表。** `architecture-constraints.md` 的 S6 掃的是與 `snap_date` 完全相等的字串字面值；`evaluation.snap_date` 是設定鍵（ADR-0017 決定一保留的 time 語彙），讀它的地方要登記在 `tests/test_core/test_architecture_constraints.py` 的 `LITERAL_COLUMN_EXCEPTIONS`，而那張表「加一筆要使用者簽核」。dataset 的 `restrict_to_months` 類比在這條軸上斷掉——dataset 讀的是 `train_snap_dates`，S6 不管。所以 step 的簽章吃 `parameters`、只在 `steps/snap_date_scope.py` 讀那個鍵一次：**新登記 1 筆**（那個 step），不是 5 筆。加上 ADR-0019 改檔名會讓既有 4 筆的路徑作廢要重指，**登記表的變更要在實作前拿給使用者簽**。

### 為什麼不是把月份塞進 catalog 的 `partition_filter`

`HiveTableDataset.load` 會把 `partition_filter` 的欄位從讀回的 frame 裡**丟掉**（理由寫在 `io/hive_table_dataset.py`：常數欄留著會讓兩張版本化表的 join 撞名）。`snap_date` 是 query group `(time, entity)` 的一半，丟了下游全壞。要走這條路得在 `io/` 加一個「這些欄讀回來留著」的開關——動框架層，換來的只是省五行。否決。

### 為什麼不是 `.cache()`

記憶體快取不落地，`--compare-only` 還是得有一張表；生產 entity 母體是百萬級、每個配 22 個候選，一個月的 frame 不是能安心留在 executor 記憶體裡的量；而且 node 規則 7 說「貴的落地」——這條 join 鏈的重算成本正是整條 pipeline 最大的一塊。

### 同一張表、兩種模式、欄位是聯集——這是決定 5 必須先落地的原因

`columns: "auto"` 的意思是表的 schema 由第一次寫入推得、之後只增不減（`_evolve_schema`：新欄 `ALTER TABLE ADD COLUMNS`、缺欄補 NULL）。兩種模式寫進同一張表，欄位集合不同：`--post-training` 讀 `training_eval_predictions`，帶 `score_uncalibrated` 與 `label`；監控模式讀 `ranked_predictions`，帶 `rank`、不帶 `score_uncalibrated`。今天這件事只影響 `--compare-only`（唯一的讀者）；決定 1 之後**每個消費者**讀回來的 frame 都帶著另一個模式寫過的欄，值全 NULL。審查時實跑確認：先跑 post-training 再跑監控，監控的消費者拿到 `score_uncalibrated` 這一欄、`IS NULL` 逐列成立。

後果落在需要「欄位存在」當守衛的地方。各 registry 診斷（`diagnosis/metric/*/_compute.py`）的守衛是 `if SCORE_COL not in pdf.columns: raise`——欄位存在但全 NULL 就不會 raise，接著在 NULL 上算 logit，得到一堆 NaN 而不是錯誤。所以：

- **決定 5 的第 1 件（監控模式不組 registry 診斷 node）是決定 1 的前置條件**，順序上不准落在它後面。
- **守衛本身也要補強**：改成「欄位存在**且**在抽樣裡不是全 NULL」（抽樣已經是 driver 端的 pandas frame，`pdf[SCORE_COL].notna().any()` 零成本）。理由跟原守衛一樣：靜默算錯比炸掉糟。
- `draw_diagnosis_sample`（`diagnosis/metric/sample.py`）「`score_uncalibrated` 存在就帶進抽樣」的邏輯在監控模式下會帶進一欄 NULL；抽樣的其他讀者（metric CI）不讀它，無害，但要在該處寫一行為什麼。
- 同型的「欄位存在就用」還有一處：`compute_dataset_overview` 與 per-segment 聚合挑 `active_seg_col` 的方式是「`evaluation.segment_columns` 裡第一個出現在 frame 欄位裡的」。（2026-09-13 更正：本段原本寫「config 側被 A10 擋住，不需要守衛」，那是在 segment 來源還寫死 `sample_pool` 時成立的。[ADR-0020](0020-evaluation-bug-round-intended-behaviours.md) bug 6 讓 segment 跟著各模式的母體表走，監控母體可能沒有某個 segment 欄，於是 post-training 那次 join 進來的欄在監控模式的 frame 裡會是全 NULL——「這次有哪些 segment 欄可用」因此改由母體表的 metadata 決定，消費者不再看 frame 的欄位。修法在 ADR-0020。）
- `docs/pipelines/evaluation.md` §7.3「post-training 與 monitoring 共用同一個分區」那段要加上「而且共用同一個 schema，另一模式的欄位讀回來是 NULL」。要不要分成兩張表，見〈沒有解決的事〉。

### 切片的 `can_load` 要看分區，不看表

今天 `eval_predictions` 是 memory-only，任何切片起點都一定補跑 `prepare_eval_data`。決定 1 之後它是 Hive 表，而 `HiveTableDataset.exists()` 是**表級**的（`_table_exists`）：表從第一次跑之後永遠存在，`--from-node compute_metrics` 從此永不補跑 `prepare_eval_data`。這正是 ADR-0012 記錄、`__main__.py::_make_can_load` docstring 逐字寫下的陷阱（「asking only the table lets a slice stop one hop short of the producer that owns the new month」），解法也是現成的：`month_plans` 讓 `can_load` 問「這個月的分區在不在」。今天只有 dataset 指令建 `month_plans`，evaluation 的 `_execute_pipeline` 呼叫不傳。

決定：**evaluation 也傳**。形狀要對：`_make_can_load` 讀的是 `plan.to_process`，值必須是 `SnapDatePlan`（`pipelines/dataset/month_plans.py`，dataset 的根層契約模組，`__main__.py` 本來就 import 它），傳一個 list 會在第一次切片探測時 `AttributeError`。所以 evaluation 的 `_execute_pipeline` 要建 `{"enriched_eval_predictions": plan_incremental_snap_dates(configured=[evaluation.snap_date], existing=<這個 model_version 已落地的月份，來自該 dataset 的 existing_partition_values()>)}`——`to_process` 只在分區缺席時非空，切片才把 `prepare_eval_data` 拉回來；分區存在時 `to_process` 是空的、不拉。（第一版寫成直接傳 `[snap_date]`，審查指出那樣 `to_process` 恆非空、`prepare_eval_data` 永遠被拉回來，跟宣稱相反。）

**這條只解「分區不存在」**。「分區存在但是用舊設定（例如改了 `segment_sources`）寫的」屬於稽核 bug 2 的同一個病（`exists()` 不驗新鮮度），本份不解，但 `docs/pipelines/evaluation.md` §7.4 那條「`catalog.exists()` 只能確認產物存在」要從「只影響 JSON 產物」升級成「也影響 row-level 資料」。

### 名字

改完之後 DAG 裡只剩一個名字：`enriched_eval_predictions`＝那張 Hive 表＝這個 model_version 評估過的所有月份。「這一個月」不再是 DAG 上的資料集，是每個 node 內部的一個區域變數。**原本的 `eval_predictions` 這個 DAG 名字由本決定收掉**：`pipeline.py` 裡不再出現；函式參數名可以保留（Runner 是位置綁定，參數名不必等於資料集名）；`tests/`、`docs/` 裡指涉「DAG 上的 `eval_predictions`」的地方跟著改，是本份實作 PR 的收尾項。

### 可重現性沒有變

使用者逐個 `snap_date` 跑，過去跑過的月份要能重新產出。這在改動前後都成立：表以 `snap_date` 分區、寫入走 `spark.sql.sources.partitionOverwriteMode=dynamic`，重跑某個月只覆寫 `(model_version, 那個月)` 那一格。**表名、分區佈局都不變，所以不需要遷移 script。**

### `--compare-only` 的 B4 檢查改成零輸出的閘門

現在 `validate_enriched_eval_predictions_present` 讀表、篩月、非空才放行、**輸出**篩過的 frame 給 `restrict_to_common`。改成：它只做「非空才放行」，零輸出，名字不改；`restrict_to_common` 跟其他消費者一樣自己篩。零輸出的閘門 node 是 [ADR-0013](0013-pipeline-modes-and-slicing-are-separate.md) 登記過的形狀（`validate_data_consistency`），由模式清單點名進來、切片永遠拉不回來——對 `--compare-only` 這條沒有切片用例的短 pipeline，這是可接受的。

---

## 決定 2：`evaluation_metrics` 與 `baseline_metrics` 落地成 JSON，接續合約跟著改寫

`generate_report` 是純函式（六個必填輸入、無 Spark），node docstring 與 `conf/base/catalog.yaml` 的註解都說「主報表可以離線重繪」。但它的三份輸入裡 `evaluation_metrics`、`baseline_metrics`、`evaluation_diagnosis_pages` 沒有 catalog 條目，所以 `--only-node generate_report` 會把 `prepare_eval_data`、`compute_metrics`、`compute_baseline_metrics` 全部拉回來重跑。

**複核糾正了稽核的一半**：這個成本不是被忽略的，是**登記過的**——`tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS` 用四個節點名釘住它，註解寫「Documented cost, pinned here」。真正壞掉的只有那句「可以離線重繪」，它跟同一個 repo 的登記合約互相矛盾。

決定：**還是落地**，同時改寫合約。

- 新增兩條 `JSONDataset`：`evaluation_metrics` → `data/evaluation/${model_version}/${snap_date}/metrics.json`、`baseline_metrics` → 同目錄 `baseline_metrics.json`。
- **關掉時的表示法統一成 stub**：`compute_baseline_metrics` 在 `report.sections.baseline` 關閉時現在回 `None`；改成回 `{"enabled": false}`，跟同一棵 `data/evaluation/${model_version}/${snap_date}/` 產物樹裡的 `diagnosis/metric_ci.json`、`report_aggregates.json` 一致，`generate_report` 把 stub 當缺席處理。否則機器可讀的指標歷史裡會混著 `null` 與 stub 兩種「關掉」。
- `RESUME_CONTRACTS` 要**分模式**改，因為決定 5 之後兩種模式在 `generate_report` 上游的形狀不同：
  - `("evaluation", ())` 是監控模式（沒有 `post_training` 參數）：`generate_report` 只補跑 `no_diagnosis_pages`（零成本的 stub，見決定 5）。
  - 新增一筆 `("evaluation", (("post_training", True),))`：`generate_report` 只補跑 `render_diagnosis_pages`（它輸出的是路徑清單，只對寫它的那次執行有意義，重繪半秒，留 memory-only 是對的）。審查時用契約測試的 stub 實跑確認：各診斷 JSON 在磁碟時，補跑集合從四個變一個；**診斷 JSON 不在磁碟時切片會連各診斷 node 一起拉回來**，那是正確行為，不是這條合約管的。
  - 本份第一版只寫了前者、而且寫的是 `render_diagnosis_pages`，審查指出它跟決定 5 自相矛盾。PR 說明要寫「為什麼接受合約變便宜」——因為決定 1 之後 `prepare_eval_data` 已經不在補跑集合裡，這筆合約本來就要重釘。
- 那句錯的註解（`catalog.yaml` 的 `evaluation_report_aggregates` 條目上方、`nodes_spark.py` 的 `compute_report_aggregates` docstring）改成成立的版本。

**為什麼不是只改那句註解**：兩個理由都不是「重繪便宜」本身。一是驗證：後面每一張 PR 的「行為沒變」都要比產物，指標 dict 落成檔之後那個比對是 `diff` 一行，不落地就得比 HTML。二是它順便解掉「指標只活在 HTML 裡、沒有機器可讀的長期歷史」。

**必須跟 bug 2 綁在一起**：診斷 JSON 的路徑只用 `${model_version}` 與 `${snap_date}` 當 key，不含任何 config 指紋，所以改了 `evaluation.metric.*` 再切片重繪會得到新舊混合的報表、退出碼 0（稽核 bug 2，複核實跑重現）。落地兩份新 JSON 等於把這個形態多鋪到兩個產物上。所以順序是：bug 那一輪先決定 bug 2 怎麼修（指紋、或切片時 fail loud、或別的），新條目照同一套規則落地。**本決定不替 bug 2 選修法。**

---

## 決定 3：`compute_dataset_overview` 的固定 job 數收斂到 ≤ 20

現在（`evaluation/metrics_spark.py::compute_dataset_overview`）：`count()`、三個 `select(…).distinct().count()`、一個 `agg(sum).collect()`、`total_queries` 再一次 `distinct().count()`，然後每個分群欄各一次 `groupBy(…).agg(…).collect()`；整個函式被叫兩次（細粒度一次、產品大類一次）。每個統計量一次 action，每次 action 從物化表（決定 1 之後）或 join 鏈（現在）掃一遍。

改成：**總量一次 `agg`**，**每個分群欄一次 `groupBy`** 不變。兩次呼叫不合併（大類那次吃的是 `collapse_to_categories` 之後的另一個 frame）。

**寫法有一個陷阱，審查抓到、實跑定案**：`F.countDistinct(col)` **忽略 NULL**，而現在的 `select(col).distinct().count()` 把 NULL 算成一個值。同一個函式裡兩種寫法已經並存（總量那幾個用 `select(…).distinct().count()`、算 NULL；`_group` 內用 `F.countDistinct`、不算 NULL），直接合併會改值。定案的寫法是 `F.countDistinct(F.struct(*cols))`：struct 裡含 NULL 仍是非 NULL 的 struct，所以計法跟 `distinct().count()` 逐位元相同。用含 NULL 的六列資料在 `local[1]` 實跑：單欄、雙欄、含 NULL 列全部與現在的寫法相等，而裸 `countDistinct` 少算一個。

三條驗收，缺一不可：

1. **`dataset_overview` 與 `report_aggregates` 的 JSON 逐字相同**。這些是精確計數，**不准換成近似算法**（`approx_count_distinct` 之類），也不准改 dict 的鍵名與巢狀。逐字比對必須涵蓋含 NULL 的情況——測試資料要有 NULL 的 entity、item、time。
2. **job 數用 event log 量**：148 → ≤ 20。基線在決定 1 落地之後、本決定之前重量一次（台架重建後的第一件事），前後在同一份 code base 上比。
3. **牆鐘不得變差**：一個 `agg` 裡多個 `countDistinct` 會展開成 `Expand`（列數乘上 distinct 群數），job 少了不代表快了。受控實驗：三次以上、每輪對調順序、區間不重疊才算沒變慢。**job 數是這條的目標，牆鐘是它的護欄。**

順帶：`compute_report_aggregates` 早已對它的六個家族做了一次 `.cache()`（「每個家族各是一次 action，不 cache 就是 6 次全掃」），這條不動。

---

## 決定 4：NDCG 整個停算，連 training 一起省

NDCG 在 `evaluation/metrics_spark.py` 有三個產生點：`add_row_contributions`（每列的 `ndcg_contrib@K`）、`compute_per_query_metrics`（聚成 `ndcg@K`）、`aggregate_per_item`（`ndcg_attr@K`），外加 `_PER_QUERY_KINDS` 這個清單。`compute_all_metrics` 只是組裝，**照字面「在那一層拿掉」只會丟鍵不省算**——要動的是三個產生點。之後 `report_builder.py` 用 `_HIDDEN_METRIC_PREFIXES` 把它們濾掉。複核追了三個可能的讀者，全部不讀：`training/nodes.py` 寫 `evaluation_results.json` 時手挑 `overall_map`／`per_item_map_attr`／`n_queries`／`n_excluded_queries`；`scripts/promote_model.py` 只讀前兩個；MLflow 的 `training/steps/experiment_log.py` 零命中；`scripts/migrate_evaluation_results_keys.py` 零命中。

而且浪費不只在 evaluation：`training/nodes.py` 的測試集評分也走同一個 `compute_all_metrics`，校準開啟時算兩次，然後整批丟掉。

決定：**在三個產生點拿掉 NDCG**，不做「指標家族參數化」。入口是共用的，training 不用改一行就一起省。

要一起收掉的：

- `_HIDDEN_METRIC_PREFIXES` 與 `_visible_metric_keys`（`report_builder.py`）——沒有東西要藏了，留著是指向不存在的東西（node 規則 6 的反面）。`evaluation/comparison/report.py` 也 import `_visible_metric_keys` 用在兩處，那兩處一起拿掉；這也是 ADR-0019 決定 5 說「要搬的只剩三個函式」的原因。
- 測試（審查逐檔數過，本份第一版只列了一條）：
  - `tests/test_evaluation/test_metrics_spark.py`——直接斷言 `ndcg_contrib@K` 數值、`set(overall.keys()) == {"map@3","ndcg@3",…}` 這類等值比對，**會真的紅**，改斷言。
  - `tests/test_evaluation/test_report_builder.py`——`test_assemble_report_has_no_ndcg_end_to_end`、`test_metrics_section_hides_ndcg`、`test_baseline_overall_table_hides_ndcg` 三條變**恆真**（`test-false-green` 的假綠形態），退休；`test_drops_ndcg_keys` 與兩條 `_visible_metric_keys` 斷言隨函式刪除而失效，一併退休。
  - `tests/test_evaluation/test_comparison_report.py`——三條 `test_*_hides_ndcg` 同樣恆真，退休；fixture 裡的 `ndcg@3` 拿掉。
  - `tests/test_evaluation/test_compare.py`——fixture 帶 ndcg 鍵，拿掉。
  - **別留一條永遠綠的護欄。**
- `docs/pipelines/evaluation.md` §3.5 `primary_map`／`per_item_attr` 兩列提到 NDCG 的文字。

**別碰的**：`conf/base/parameters_training.yaml` 與 `models/lightgbm_adapter.py` 裡的 `ndcg` 是 LightGBM 自己的 early-stopping 指標，跟這裡的評估指標是兩件事。

**為什麼不做參數化**（稽核 J 的建議）：跨三個模組、十幾處寫死清單，而且 `report_builder` 的表格版面跟家族是耦合的（`for fam in ("map", "precision", "recall")` 決定「一個家族一張表」）——改版面是另一個等級的改動。停算是可以單獨驗收的一半（HTML 逐位元不變），參數化是另一輪的事。（2026-09-13 更正：本句原本寫「改版面就撞報表凍結」；報表凍結已依 [ADR-0020](0020-evaluation-bug-round-intended-behaviours.md) 解除，理由只剩前半。）

---

## 決定 5：監控模式不跑 registry 診斷；inference 照樣多寫原始分數

### 現象

registry 診斷（`diagnosis/metric/contract.py::DIAGNOSES`，現行 `config_shift`、`item_ability`、`model_capacity`、`suppression`，以 registry 為準）裡，除了 `model_capacity` 之外都要 `score_uncalibrated` 這一欄，也就是校準前的原始分數。它們的 `_compute.py` 明寫「讀不到就 raise，不退回 `score`」，理由正確：這些診斷在模型輸出的 log-odds 空間做減法與 logit，校準是事後貼上去的單調變換，拿校準後的分數去算會得到一個看起來像數字、實際上什麼都不是的東西，而且不會報錯。

這一欄**只有 training 會寫**（`training/nodes.py` 的 predict node 同時寫 `score` 與 `score_uncalibrated`，寫進 `training_eval_predictions`）。inference 只寫 `score`，而且預設 `inference.use_calibration: true`，原始分數就丟了。於是預設的監控模式（讀 `ranked_predictions`）跑到第一個診斷 node 就炸——**生產也一樣**，不只是示例環境。

### 決定

兩件事分開做：

1. **監控模式不組出 registry 診斷 node。** 依 [ADR-0013](0013-pipeline-modes-and-slicing-are-separate.md)，「跑哪些 node」由 `create_pipeline` 的模式參數決定、明寫清單，不是 config 開關。`create_pipeline(post_training=False)` 不加入 `make_diagnosis_node(...)` 那一組，也**不加入 `render_diagnosis_pages`**。**保留** `draw_diagnosis_sample_node` → `compute_metric_ci`（指標的 bootstrap 信賴區間，監控正需要「這個月的數字可不可信」）與 `compute_report_aggregates`（主報表的 item 細節區，只看分數分佈，不需要原始分數）。

   `generate_report` 的六個位置輸入**不變**（`core/runner.py` 是位置綁定，少接一個 Runner 在開跑前就 raise「requires input … not produced by any prior node」，審查實跑確認）。所以監控模式要有人產出 `evaluation_diagnosis_pages`＝空清單。本份第一版寫「讓 `render_diagnosis_pages` 在該模式下 inputs 只剩 `parameters`、讀不到就回空清單」，**審查證明那是錯的**：它按檔名從磁碟讀（`load_results(out_dir)`，目錄由 model_version ＋ snap_date 算出），回空清單的條件是「目錄裡沒有 JSON」而不是「這次沒算」；而且 inputs 只剩 `parameters` 讓它 in-degree 為 0、被拓撲排序排到第二位——它自己的 docstring 逐字警告過這件事。同一個 `(model_version, snap_date)` 先跑過 `--post-training` 再跑監控，監控報表會長出指向 post-training 那批診斷頁的入口，退出碼 0。

   定案：監控模式用一個**零磁碟讀取**的 stub node `no_diagnosis_pages(parameters) -> []`，輸出 `evaluation_diagnosis_pages`。它不讀任何東西，所以拓撲位置無所謂。這是 ADR-0013「模式決定形狀」底下的一個明列 node，不是 workaround；node docstring 要寫它為什麼存在（`generate_report` 位置綁定、`render_diagnosis_pages` 按檔名讀磁碟）。**不**用「`generate_report` 加一個 `diagnosis_pages=None` 預設值」——`known-pitfalls.md` §12 記過，尾端預設值會吞掉 arity 錯誤，`generate_report` 的六個必填正是那次修出來的。

   **連帶**：`--compare`（`create_pipeline(post_training=False, compare_source=…)`）的三個 compare node 是在 `post_training` 判斷之外加的，所以監控模式的 `--compare` 也不跑 registry 診斷。`docs/pipelines/evaluation.md` §4.3、§4.4 要寫明「監控模式（含 `--compare`）不含 registry 診斷，要診斷用 `--post-training`」。

2. **inference 也寫 `score_uncalibrated`。** `predict_and_write_scores`（`inference/nodes.py`）多寫一欄原始 booster 輸出（校準關閉時等於 `score`，跟 training 的寫法一致）；`unranked_predictions`、`ranked_staging`、`ranked_predictions` 三個 catalog 條目的 `columns` 各加一欄——`HiveTableDataset.save` 結尾的 `df.select(*declared)` 會**靜默丟掉**未宣告的欄（`io/hive_table_dataset.py` 的 `declared_columns` docstring），漏一個條目那一欄就不見、沒有錯誤。不變量 A28 幫不上忙：它只管 `schema.entity` 各欄、只接在 training 指令上。inference 尚未部署，不需要遷移。

為什麼第 2 件還要做，既然第 1 件之後監控模式不跑診斷了：原始分數是模型的事實，丟掉之後任何事後分析都拿不回來；多一欄 DOUBLE 的成本跟它未來的用途（例如 `--compare` 外部來源要對齊分數空間）比起來可以忽略。

### 守衛補強（見決定 1〈同一張表、兩種模式〉）

需要原始分數的那幾項診斷「讀不到就 raise」的規則**不動**，但「讀不到」的定義從「欄位不存在」改成「欄位不存在，或在抽樣裡全 NULL」。原因是決定 1 之後同一張表的另一模式會把這一欄以 NULL 送回來。

### 這條的實作歸 bug 那一輪

它是稽核清單的第 13 條（見 notes），修法由本份定，實作順序見〈順序〉。

---

## 考慮過但否決的選項

| 選項 | 為什麼否決 |
|---|---|
| 多一個 `keep_snap_date` node，五個消費者不動 | node 規則 1（輸出撈不出來看）與規則 5（dataset 慣例是各 node 自己篩）。S6 登記筆數兩案相同（各 1 筆）。見決定 1 |
| `snap_date` 進 catalog 的 `partition_filter`，`io/` 加「讀回來留著」開關 | 動框架層換五行；`load` 丟掉 filter 欄的規則有它的理由 |
| `eval_predictions.cache()` | 不落地、記憶體量級不可控、`--compare-only` 還是要一張表 |
| 新開一個 driver 端 parquet 條目，Hive 表照舊在最後寫 | 一張表兩個名字；Hive 表已存在且已有讀者 |
| 只改「可離線重繪」那句註解，不落地 metrics JSON | 驗證尺與指標歷史兩個收益都拿不到。見決定 2 |
| 指標家族參數化（稽核 J-2） | 跨三模組的重構，另一輪；停算是可單獨驗收的一半。見決定 4（報表凍結已解除，ADR-0020） |
| 監控模式用 config 開關關掉需要原始分數的診斷 | ADR-0013：模式決定形狀，不是 config。而且開關忘了關就是原問題 |
| 監控模式保留 `render_diagnosis_pages`、只給它 `parameters` | 按檔名讀磁碟、in-degree 0 排到最前，會撿到上一次 post-training 的 JSON。見決定 5 |
| `generate_report` 給 `diagnosis_pages` 一個預設值 | 尾端預設值吞 arity 錯誤，`known-pitfalls.md` §12 |
| 診斷讀不到原始分數時退回 `score` | 靜默算錯。見決定 5 |

---

## 順序

依 flow 規則 3（行為改動先做完，純結構搬移放最後）：

```
Phase 0   bug 修正（notes 裡的清單，另開一場討論逐條定行為）
            必含：bug 13（本份決定 5 的兩件事——模式清單、inference 多一欄）
                  bug 15（rank 在 ranked_predictions 是 BIGINT、prepare_eval_data 補的是 INT，
                          同一張表換模式寫會被 _evolve_schema 的 type conflict 擋下；見〈驗收〉）
Phase 1   本份其餘決定，建議的落地順序：
            決定 2（metrics JSON）  ← 先做，之後每張 PR 的驗證變成 diff 兩個 JSON
            決定 1（物化 ＋ month_plans ＋ AST 護欄）← 決定 5 第 1 件已在 Phase 0 落地
            決定 3 ＋ 決定 4（job 收斂、NDCG 停算）
            ADR-0019 決定 6（report.sections 刪死鍵 ＋ 雙向不變量）——它改 conf/，所以在這裡而不在 Phase 2
Phase 2   ADR-0019 的結構搬移，一張票
```

PR 怎麼切交給 `/to-spec`、`/to-ticket`，**偏粗不偏細**：切點只看「這一半有沒有比測試綠更強的證據」（flow 規則 4），不看行數。

---

## 驗收

決定 1 改了 DAG，`pipeline.py` 的 diff 不會是空的，所以不能只靠「diff 為空」免實跑（flow 規則 8 的免跑條件不成立）。證據三樣：

1. **Hive 表該分區的 digest 前後一致**——`enriched_eval_predictions` 的 `(model_version, snap_date)` 分區，逐列雜湊。
2. **所有 JSON 產物一致**——先把 main 自己跑兩次、記下本來就會不同的檔案集合（noise floor，flow 規則 9：timestamp、繪圖細節），再拿分支的差異去減。決定 2 之後指標 JSON 也在這個集合裡。
3. **重掃次數與 job 數**——重建最小量測台架（只要 event log 解析那一支），放在 worktree 的 `data/verification/`（gitignored）。台架設計要點：不改 `src/`，jobGroup 標籤在執行期包上去（`pipeline.py` 的 import 是延後到 `create_pipeline()` 裡做的，所以包得住）；parser 先用兩個人工 jobGroup 各跑固定數量 action 校準，parser 數出來的 job 數必須跟已知答案一樣，**零結果要先證明抓得到東西**。

**兩種模式都要跑**，因為 `prepare_eval_data` 對兩者走不同分支（`--post-training` 讀 `training_eval_predictions`：有 `label`、無 `rank`；監控模式讀 `ranked_predictions`：有 `rank`、無 `label`）。但書兩條：

- 監控模式在示例環境要先把 `evaluation.snap_date` 對到 inference 產出的月份（e2e 只產 `2025-12-31`，預設是 `2026-01-31`），且決定 5 做完之後才跑得完整條。
- **同一個 model_version 換模式寫同一張表，今天就會炸**：`ranked_predictions.rank` 宣告 BIGINT，`prepare_eval_data` 在 post-training 補的 `rank` 來自 `row_number()` 是 INT，`_evolve_schema` 對同名不同型直接 raise（「Schema evolution never casts」）。這是既有問題（今天由最後一個 node 觸發，決定 1 之後提前到第一個 node），列為 bug 15 進 Phase 0（修法：`prepare_eval_data` 把補出來的 `rank` cast 成跟 `ranked_predictions` 宣告一致的型別）。bug 15 修完之前，兩種模式要用不同 model_version 或先 drop table。

「A 比 B 快」的宣稱一律不做（每格只跑一次、±20% 當雜訊）。要比較就受控實驗：三次以上、每輪對調順序、看區間是否重疊。

---

## 後果

- **接續點變便宜，也變得會撒謊一次**：`--from-node compute_metrics` 讀表不重 join；`--only-node generate_report` 在 `--post-training` 只補跑 `render_diagnosis_pages`、在監控模式只補跑 `no_diagnosis_pages`。代價是 `can_load` 對 row-level 資料也只驗存在不驗新鮮度（見決定 1〈切片的 `can_load`〉）。`RESUME_CONTRACTS` 要同步改；`docs/pipelines/evaluation.md` §4.6、§7.4 要改。
- **DAG 少一個 node（`persist_eval_predictions`）、監控模式多一個 stub（`no_diagnosis_pages`）、少各 registry 診斷與 `render_diagnosis_pages`**；`tests/test_pipelines/test_evaluation/test_pipeline.py` 釘的 node 數要跟著改；`docs/pipelines/evaluation.md` §5.1 的 node 表（刪 `persist_eval_predictions` 那列、加模式差異）與 `docs/diagrams/evaluation-pipeline.mmd`／`.html` 要重畫。
- **多一條 AST 測試、一條後置條件、一筆 S6 登記**（登記要使用者簽）。
- **監控模式的報表少了診斷入口**；§4.3、§4.4 寫明。
- **inference 三張表多一欄**；`docs/pipelines/inference.md` 的欄位表要加。
- **catalog 多兩條 JSON、一句錯話被改掉、`baseline_metrics` 關閉時的表示法從 `null` 變 stub**。
- **NDCG 從指標輸出消失**；§3.5 兩列改字；四個測試檔改斷言或退休護欄。
- **量測方法可以搬去生產**（jobGroup 標籤＋event log 離線解析在叢集上同樣適用），數字不行。

---

## 這條 ADR 沒有解決的事

- **post-training 與 monitoring 共用同一個 `(model_version, snap_date)` 分區、同一個 schema**（`docs/pipelines/evaluation.md` §7.3 ＋ 決定 1〈同一張表、兩種模式〉）。最後一次跑的覆寫前一次；另一模式的欄位讀回來是 NULL。決定 1 沒有把它變好也沒變壞。要分開有兩條路：加 scenario 分區（同表、同 schema，NULL 欄問題不解）或兩張表（`--compare-only` 與 `MODEL_VERSION_SOURCES` 都要知道讀哪張）。那是另一個決定。
- **`--compare` 模式的比較數字沒有 JSON**（稽核 F）：`generate_comparison_report` 自己算兩次全量指標再組 HTML。決定 2 的形狀可以直接套過去，但本輪不做——它沒有效能證據，也沒有讀者要那份數字。
- **主報表的診斷區與 registry 是兩套機制**（稽核 B）：不在本輪。2026-09-13 更正——原寫「撞報表凍結」，凍結已依 [ADR-0020](0020-evaluation-bug-round-intended-behaviours.md) 解除；仍不做的理由是它搬一整塊報表、屬結構重整，要做另開一票。
- **`compute_dataset_overview` 之後排名第二的 `aggregate_overall`**（654,000 個 query group 時 1,299 秒 executor 時間、次線性成長）：這輪沒有它的假設，先做完決定 1 再量一次。
- **training 那 7 個 diagnosis node 要不要搬來 evaluation**：跟 `overall_map` 跨月合併是同一個接縫，`deliberate-non-goals.md` 明寫要另開一輪，本份不預留位置。

---

## 出處

- 成本剖析的原始數據與台架設計：2026-09-09～09-12 的量測紀錄（artifact，未進 repo；結構性結論已抄進本份〈這份在解什麼問題〉）。
- 稽核與複核：[`docs/notes/2026-09-09-evaluation-audit.md`](../notes/2026-09-09-evaluation-audit.md)。
- 決定 1 的第一版（多一個 node）與否決它的討論、決定 5 第一版（`render_diagnosis_pages` 留在監控模式）與否決它的審查：2026-09-13，結論已寫進各決定正文。
- `countDistinct(struct(…))` 對 NULL 的計法：2026-09-13 在 `local[1]` 用六列含 NULL 資料實跑比對（單欄、雙欄各一次，與 `distinct().count()` 逐格相等）。
