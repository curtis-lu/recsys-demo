# 模式與切片：這次少跑一些 node 的兩個機制

> **旗標的基本用法**（`--from-node` / `--only-node` / `--dry-run` / `--list-nodes`）與各 pipeline 的實際 node 名，在**該 pipeline 文件的 §4「使用方式」**——五份都有。
>
> 本文講的是那上面一層：**兩個機制的差別**、dataset 為什麼多一道判準、以及版本化擋得住／擋不住哪些接續風險。
>
> 設計 spec：`docs/superpowers/specs/2026-06-10-pipeline-node-slicing-design.md`

> 兩個小細節，各 pipeline 文件都沒寫：`--dry-run` 不執行、不寫任何 pipeline 產物與 manifest，但 **run log 照常寫**；切片是**節點邊界**接續——整個 `tune_hyperparameters` 跑完並落地後，才能用 `--from-node finalize_model` 跳過它，HPO **跑到一半** crash 的接續（只補跑剩餘 trial）是另一層機制，見 [`training.md` §7.3](../../pipelines/training.md)。

## 先分清楚：模式不是切片

「這次少跑一些 node」在本 repo 有**兩個**機制。它們正交、可以併用，而且回答的是不同的問題（[ADR-0013](../../adr/0013-pipeline-modes-and-slicing-are-separate.md)）：

| | **模式** | **切片** |
|---|---|---|
| 旗標 | `--only-test-months`（dataset）、`--compare-only`（evaluation）…… | `--from-node` / `--only-node` |
| 問題 | 這次要跑**哪一條動線** | 這次要**從哪裡接續** |
| 誰決定 node 集 | `create_pipeline(**kwargs)`，**明確列出**在 pipeline 定義旁 | 起點 ＋ DAG 反推 |
| 缺料時 | 照常 `catalog.load()`，缺了當場 raise | `can_load` 判斷後自動補跑上游 |
| 零輸出 node | 在清單裡就會跑 | **一定**被跳過 |

併用的順序是「模式先組出短 pipeline，切片再對它取子集」，所以 `[plan] running N of M nodes` 的 **M 是模式的 node 數**，不是完整 pipeline 的。

**選哪一個**：想加一個評估月份 → 用模式（見 [adding-an-eval-month.md](adding-an-eval-month.md) 步驟 2），它把資料閘明確列在清單裡；想從某個 node 手動接續 → 用切片，但要知道零輸出的資料閘一定被跳過。

## 自動擴張補跑：dataset 為什麼多一道判準

一般判準是 `catalog.exists()`：被跳過 node 的輸出若已落地就直接讀，否則遞迴把生產者拉回必跑集合，最壞退化成 full run（各 pipeline 文件的 §4 都有寫）。

**dataset 的三個增量產物多問一件事**：`preprocessed_feature_table`、`test_keys`、
`test_model_input` 是逐月延伸的，表從第一次跑完就一直在，所以「存在」對它們永遠是真，
會缺的是**這次要的月份**。它們的判準因此是「這次的月份計畫還有沒有要處理的月」——
`[months]` 那幾行印的就是這個計畫（[ADR-0012](../../adr/0012-month-aware-slicing-not-per-artifact-skip.md)）。

具體長相：加了一個 `test_snap_dates` 月份之後 `--only-node filter_test_model_input`，
auto-included 會列出三個——`build_test_model_input`（一般判準：中間產物
`test_model_input_unfiltered` 是 memory-only），以及月份判準拉回來的
`select_test_keys` 與 `apply_preprocessor_to_features`。
`fit_preprocessor_metadata` 不在其中（`preprocessor` 是落地的 JSON，沒有月份）。

其餘三個 pipeline 沒有增量產物，判準仍是 `exists()` 一個。

## 接續前提：版本擋得住什麼、擋不住什麼

接續的共通前提（`exists()` 不驗證新鮮度、side-effect 守門 node 不重跑、training 改參數會漂移到新
`model_version` 並印 `[retrain]` 警告）寫在 [`design-principles.md`](../../design-principles.md)
與 [`training.md` §7.4](../../pipelines/training.md)。這裡補一件最容易誤會的事：

**版本化是靠 partition column，不是靠表名。** `catalog.yaml` 的 22 個 `HiveTableDataset` 裡，
**18 個 pipeline 產物全部帶版本 `partition_filter`**（`base_dataset_version`／`train_variant_id`／
`model_version`）——包括表名看起來完全沒有版本的 `recsys_prod_train_keys`。config 變了 → 版本變了 →
filter 指向一個不存在的 partition → `exists()` 自然為假。**這一層防呆是有效的。**

沒有版本 filter 的只有 4 張（`feature_table`、`label_table`、`sample_pool`、`inference_population`），
那是 source_etl 維護的唯讀來源表——切片不會跳過產生它們的 node，所以它們不是切片的風險來源。
（它們**內容被改**時的風險是另一回事，見下面第 2 點。）

**真正擋不住的是另外兩件事**：

1. **版本 hash 涵蓋 config，不涵蓋 code。** 改了 Python 但沒改 config → 版本不變 → `exists()` 為真 →
   讀到的是舊 code 產出的東西。
   **所以改了 code 之後不要用 `--from-node` 接續**——切片會把上游的舊產物直接讀進來。
   跑 full run 也不一定夠：dataset 的既有月份要 `--rebuild-dates` 才會重算；driver-local parquet 與
   LightGBM `.bin` cache 的判準同樣只有「`_SUCCESS` 在不在」
   （`pipelines/training/nodes.py::_materialize_parquet_handle`），其中 test 月份可由 `--rebuild-dates`
   帶到，**train／val／calibration 那幾份只能手動 `rm -rf <cache.root>/<base_dataset_version>/...`**。
   `data/models/<model_version>/` 反而不用刪——full run 會覆寫它。
   相關：[`training.md` §7.5](../../pipelines/training.md) 最後一列、[`known-pitfalls.md` §17](../known-pitfalls.md)。
2. **同一個 snap_date 的來源資料被回補。** `base_dataset_version` 含 `feature_table` 的欄位名稱、型別與
   順序，但**不含每一列的值**——[`design-principles.md`](../../design-principles.md)「版本 ID 不代表來源資料
   內容完全相同」明列排除三項，其中就有「同一 partition 被回補後的資料差異」。所以上游重跑了某個月的
   `feature_table`，版本不會動；而月份判準只問「這個月的 partition 在不在」
   （`pipelines/dataset/month_plans.py::plan_incremental_snap_dates`），在就跳過。
   **逃生口是 `--rebuild-dates`**（該函式的 docstring 自稱 "the escape hatch for upstream backfill"）。
   注意**新增**一個月份不受影響——新月份的 partition 不存在，必然被處理；出事的只有「既有月份、內容變了」。
   完整症狀與重算指令見 [`known-pitfalls.md` §15](../known-pitfalls.md)。

> 切片計畫會**無條件**印一行警語（`src/recsys_tfb/__main__.py::_format_slice_plan`）：
> `resume assumes the skipped artifacts are still valid. exists() proves presence, not freshness — version IDs cover config only, not code changes or backfilled source data.`
> 它每次都印，看到它不代表這次有問題；它提醒的就是上面那兩個洞。
>
> （2026-08-25 更正：這行原本的括號寫 `overwrite-style Hive tables are not version-stamped`，
> 與 catalog 現況不符——18/18 的 pipeline 產物都是 version-stamped。本文件曾照抄那句錯話，已一併修正。）

## 相關

- **要改 pipeline 結構**（新增 node、決定產物落不落地）→ [`pipeline-node-design.md` G7](../../agents/pipeline-node-design.md)：接續點是會被新增 node 默默破壞的契約，`RESUME_CONTRACTS` 釘住它。
- **`hpo_best_model` 為什麼落地、落在 `hpo/` 子目錄** → [`training.md`](../../pipelines/training.md) §6.1 與 §7.4。
