---
status: accepted
date: 2026-08-29
---

# training pipeline 依角色切模組：node 講 ML 決策，機制進 `steps/`

> **要照著做，讀 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)；要知道 training 為什麼這樣切，讀這份。**
>
> - **判準不在這裡。** 13 條形狀判準的唯一真實來源是 `pipeline-node-design.md`，流程判準是
>   [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)。本份只記錄「把那兩份套到 training 時，
>   遇到哪些判準給不出答案的岔路，以及各自選了哪一邊」。
> - **現況盤點不在這裡。** 16 條落差的逐條清單在
>   [`docs/notes/2026-08-25-training-pipeline-gap-survey.md`](../notes/2026-08-25-training-pipeline-gap-survey.md)。
>   本份只引用它的結論，不重抄。⚠ 該盤點有數處已被本份的查證修正，衝突時以本份為準。
> - **行號會腐爛**，所以本份盡量只給檔名與函式名。少數必要的行號都標了核對日期（2026-08-29）。

---

## 這份在解什麼問題

`src/recsys_tfb/pipelines/training/` 還沒依形狀判準整理過（evaluation 同樣還沒，但不在本次範圍）。三個數字說明問題：

```
                      # Decision   Pre-check   require_*
  dataset                  21          10          5
  inference                22           5          5
  training                  0           0          0     ← 這裡
  evaluation                0           0          0
```

實測指令（**印出的是逐檔計數，要自己加總**）：

```bash
grep -rc '# Decision' src/recsys_tfb/pipelines/<name>/
```

**核心問題不是「node 太長」，是「node 裡沒有故事」。** 決策全部沉在 helper 裡，讀 node body 看不出這條 pipeline 對資料做了什麼決定。最重的一處是 `nodes.py` 的 `_materialize_parquet_handle`：75 行裝 4 個決策，上面掛 5 個 cache node、其中 4 個只有 3–5 行——正是 `pipeline-node-design.md` 規則 3 明寫的失效形態。

現況規模（2026-08-29 核對）：

| 檔案 | 行數 |
|---|---|
| `nodes.py` | 1443（14 個 node ＋ 10 個 helper） |
| `pipeline.py` | 217（21 個 `Node(...)` 建構，其中 2 個只在 calibration 開啟時加入） |
| `search_space.py` | 82（在根層，但只有 `nodes.py` 呼叫） |
| `hpo_resume.py` | 119（同上） |

另有 7 個 node 的 `def` 在 `src/recsys_tfb/diagnosis/model/` 底下（該目錄 1316 行），見〈決定 6〉。

---

## 重構後長什麼樣

```
src/recsys_tfb/pipelines/training/
  __init__.py            re-export create_pipeline
  pipeline.py            接線：DAG 拓撲，以及「為什麼這樣接」的註解
  nodes.py               node 函式 ＋ shutil.rmtree 呼叫（見決定 1）
  cache_sources.py       ← 新：對外契約，__main__.py 在 pipeline 開跑前呼叫
  steps/
    local_cache.py       driver-local parquet cache 的路徑、複製與 HDFS 拉取機制
    predict_months.py    「這一次要 predict 哪些月」— 零 pyspark 純模組
    search_space.py      宣告式 HPO 搜尋空間 → Optuna 取樣（從根層搬入）
    hpo_resume.py        study 生命週期、checkpoint、search_id 身分（從根層搬入）
    hpo_scoring.py       一個 trial 的分數怎麼算（含 TrialScorer，見決定 4）
    refit.py             train ＋ train_dev 併成一份 refit 用的 Dataset
    sample_weights.py    sample_weight 設定與實際訓練列的比對
    experiment_log.py    往 MLflow 記什麼
```

**根層 vs `steps/` 的判準**（規則 8）：src 側呼叫端是否全在本 pipeline 內。

- `cache_sources.py` 放根層，因為 `__main__.py` 在 pipeline 開跑前就要把 cache 來源表注進 `parameters`。與 `pipelines/dataset/month_plans.py` 同型（[ADR-0007](0007-month-plans-travel-through-the-catalog.md)）。
- `search_space.py` 與 `hpo_resume.py` 從根層搬進 `steps/`：src 側呼叫端**只有** `nodes.py`。`core/consistency.py` 的 A8 是自己重寫一份 schema 檢查，並未 import `search_space`。規則 8 明說「測試不算」，所以測試 import 不是留在根層的理由。

`steps/__init__.py` **不 re-export 任何東西**。`nodes.py` 逐模組 import，import 那一行就說出這個步驟來自哪個 concern。

**`steps/local_cache.py` 收兩個東西**（盤點只列了一個）：`_resolve_cache_path` 那一族路徑機制，**以及 `_populate_cache_from_hive`**（`nodes.py:168`，把 Hive 分區複製到 driver 本機）。後者見〈既有登記的一個缺口〉。

---

# 實作前的閘門

**這份的 status 是 accepted，指的是「形狀已裁決」，不是「可以直接開工做完」。** 下面每一條在被滿足之前，對應的部分不得落地。照著本份實作的人先看這張表。

| # | 閘門 | 卡住什麼 | 怎麼解 |
|---|---|---|---|
| G1 | **要使用者簽核**：`architecture-constraints.md` 的直接寫檔登記 3 → 6（決定 1），以及 R4 表格 3 → 2（決定 2） | 決定 1、決定 2 | 問使用者。`architecture-constraints.md` 節三規定例外登記增刪必須先問 |
| G2 | **要使用者裁決**：`_populate_cache_from_hive` 要不要補進 R4 表格 | 只影響登記，不擋程式碼 | 見〈既有登記的一個缺口〉 |
| G3 | **等別的票**：把稽核 glob 放寬到 `pipelines/**/*.py`（issue #163 一帶） | 不擋本次；但它做完之後，決定 1 應重新檢討 | 那張票獨立進行 |
| G4 | **列入完成定義、但不在本次 PR**：`_pdf_to_X` 改名（issue #199） | 重構不做完 #199 不算完成 | 獨立的最後一張票 |
| G5 | **另一份 ADR**：rank 落地 ＋ tie-break 統一 | 決定 3 只做 `.persist()`，落地留給 ADR-0015 | 見〈刻意不做〉第 2 條 |
| G6 | **懸而未決**：calibration 去留 | 若決定刪除，本份三處要回頭改 | 見〈刻意不做〉第 6 條 |
| G7 | **要實測**：`persist` 在生產資料量下的峰值記憶體／磁碟 | 決定 3 的 StorageLevel 選擇 | 量一次再定 |

---

# 七個有取捨的決定

判準給不出唯一答案的地方。每一節同一個模板：**做什麼／代價／為什麼不選另一邊**。

## 決定 1：`shutil.rmtree` 留在 `nodes.py`，直接寫檔的函式從 3 個變 6 個

**做什麼。** `_materialize_parquet_handle` 那 75 行裡的決策上浮到 5 個 cache node 各自的 body，每個配一段 `# Decision —`。機制（路徑計算、複製、HDFS 拉取）進 `steps/local_cache.py`，**但 `shutil.rmtree` 的呼叫留在 `nodes.py`**。

**代價。** `tests/test_core/test_architecture_constraints.py` 的 `test_direct_writes_match_registry` 目前釘死三個函式：

```
今天（3 個）                          改後（6 個）
persist_sample_weight_report   ──→   （因決定 2 離開這一組）
log_experiment                 ──→   log_experiment
_materialize_parquet_handle    ──→   cache_train_model_input
                                     cache_train_dev_model_input
                                     cache_val_model_input
                                     cache_test_model_input
                                     cache_calibration_model_input
```

`architecture-constraints.md` 的 **R4 登記**要同步更新，**這需要使用者簽核**（該文件節三的規則）。

> ⚠ **「R4 登記」是兩個不同的集合，不要混。** `architecture-constraints.md` 的 R4 **表格**列的是「會寫診斷副產物的 node」（含 `tune_hyperparameters`，它是間接寫入、掃描看不到）；上面那個測試釘的是「**掃描看得到直接寫檔的函式**」。兩組今天各 3 筆但成員不同，該文件的 R4 段落自己有 ⚠ 說明這件事。**本決定動的是測試那一組（3 → 6）**；表格那一組因決定 2 從 3 變 2。

**為什麼不搬進 `steps/`。** 那個稽核測試掃的是 `PIPELINES.rglob("nodes*.py")`：

```
今天    nodes.py::_materialize_parquet_handle  --[rmtree]-->  稽核看得到 ✓

搬 steps/  steps/local_cache.py::clear_cache_dir --[rmtree]-->  稽核掃不到 ✗
           → 測試先轉紅（少一筆），改完登記之後
           → 這個寫檔動作從此無人看管
```

**把 glob 放寬到 `pipelines/**/*.py` 是一個真的選項，但它是一張獨立的票、還沒做**（`architecture-constraints.md` 的 A1〈這個檢查看不到〉：「要不要把 glob 放寬到 `pipelines/**/*.py` 是一張獨立的票（放寬會一併把 `dataset/steps/` 納入，需先確認那邊也乾淨）」；`deliberate-non-goals.md` 節四把 #163 列為「等使用者裁決」）。

**所以這不是「規則禁止」，是「時序上還不能」**：在那張票做完之前，`rmtree` 搬進 `steps/` 就是進死角。等它做完，這個決定可以重新檢討。

**登記變大是誠實的**——5 個 node 真的各自會刪檔。

## 決定 2：`sample_weight_report` 進 catalog，不是變成零輸出 node

**做什麼。** `persist_sample_weight_report` 現在宣告 `outputs="sample_weight_report"`，但那個名字**catalog 沒有條目、零下游消費者**——是個假輸出。改成給它 catalog 條目，node 只 `return diag`，寫檔交給 catalog。

**代價。** R4 **表格**從 3 筆變 2 筆，直接寫檔的**測試集合**也少這一個。登記縮小也要簽核，但方向是好的。catalog 條目要指到 model version 目錄，維持「它出現在 manifest 的 artifacts 清單裡」這件事。

**為什麼不改成 `outputs=None`。** 那條路要把 A7／R3（零輸出 side-effect node）登記從 2 筆擴到 3 筆，而 `deliberate-non-goals.md` 明寫「別為了讓自己的新程式碼合規而擴充登記」。**縮小一個登記比擴大另一個好。**

> ⚠ **不要用「切片會跳過零輸出 node」當理由。** 那個說法對這個 node 不成立：`core/pipeline.py` 的 `slice_from` 是 `self._sorted[idx:]`，**按拓撲位置切**；F5 講的「零輸出 node 拉不回來」是指**自動擴張**（producer map 由 `node.outputs` 建）。而 `sample_weight_report` 零消費者，所以擴張兩種情況下都不會把它拉回來——**有沒有 catalog 條目，切片行為完全一樣**。

真正的理由是規則 1：假輸出撈不出來看。

## 決定 3：`select_shap_population` 不切、不落地，只加 `.persist()`

**做什麼。** 一行：

```python
labeled = ranked.withColumn("quadrant", quadrant).withColumn("_ck", ck).persist()
```

**為什麼——排名現在跑了兩遍。** `diagnosis/model/population_spark.py` 裡的 `labeled`（window rank ＋ 象限標記）被兩條分支各用一次，而該檔**沒有任何 `.cache()` 或 `.persist()`**（2026-08-29 grep 零命中）。Spark 是 lazy 的，兩個 `toPandas()` 是兩個 action，rank 的 shuffle 因此執行兩次。這是本節唯一有證據的缺陷，一行修好。

**為什麼不切成三個 node。** 規則 1 字面上命中（一個 node 出兩樣東西、被兩個不同下游各自消費 → 切開），但規則 1 的**目的**是「出事的時候能不能單獨撈出來看」：

- 切開但中間物 memory-only → 它仍是一份 Spark 計畫，撈不出來，多兩個框而不多一個可觀察的地方。**那正是規則 1 反方向要求合併的形狀。**
- 切開並落地 → 見下。

**為什麼不落地（在這份 ADR 裡）。** 落地本身是對的方向——同一個排名今天算了四次：

```
  training pipeline
    select_shap_population   ×2   ← 沒 persist
    compute_test_mAP_spark   ×1   ← compute_all_metrics 內部
  evaluation pipeline（--post-training，另一次執行）
    prepare_eval_data        ×1   ← rank 欄不存在，讀取時補算（commit b9fff01）
```

而且當初決定不存 rank 的理由**前提已經到期**。出處 `docs/superpowers/specs/2026-05-13-batched-test-eval-spark-design.md:127-133`（commit `fb19982`）：寫入改成逐 `(月份, 產品)` 分區之後，rank 是跨產品的量、單一 chunk 算不出來。三個選項裡選了「不存」，而否決「全部寫完後跑一趟算 rank」的**唯一理由是「為一個沒人讀的欄位把 I/O 加倍」**——今天有三個消費者各自重算，那個前提不成立了。

**但落地牽動 evaluation，而本次盤點沒有涵蓋 evaluation**，且它需要先裁決一件會改變指標數字的事：

- `select_shap_population` 用**確定性** tie-break（`orderBy(score.desc(), item_col)`，註解明寫是為了象限指派可重現）。
- `rank_within_query` 的 docstring 明寫 **tie-break 未定義**（`metrics_spark.py:281`）。
- 而 `evaluation/nodes_spark.py:190` 已經有「**rank 欄存在時就信上游**」的邏輯——所以只要往 `training_eval_predictions` 加一個 `rank` 欄，evaluation 的指標會自動改用 training 的 tie-break，**數字會變且無任何訊息**。

因此「rank 落地 ＋ tie-break 統一」是**另一份 ADR** 的題目，見〈刻意不做〉第 2 條。把它塞進這裡會讓一次指標變化被埋在模組重構的 diff 裡。

**⚠ 一定要配 `unpersist`，這不是可選的。** Spark 的 cache **不會**因為 node 結束而釋放——`core/runner.py` 與 `core/data_catalog.py` 裡 `unpersist` 零命中（2026-08-29 grep），Runner 只釋放 `MemoryDataset`，不碰 Spark DataFrame 的 storage。少了它，那 2.2 億列會佔著 executor 的記憶體／磁碟直到 SparkSession 結束，可能觸發 eviction 或拖慢後面所有 Spark 工作。

所以形狀是 `try/finally`，而且要顯式指定 StorageLevel：

```python
labeled = (...).persist(StorageLevel.MEMORY_AND_DISK)   # 顯式，不靠預設
try:
    ...兩個 toPandas() 分支...
finally:
    labeled.unpersist()
```

`finally` 而不是只在成功路徑釋放：`select_shap_population` 是 best-effort，失敗路徑同樣會離開這個函式。

**代價。** cache 存在 executor 端（不碰 driver），生命週期就是這個 node。**峰值記憶體／磁碟需求沒有用生產資料量量過**——實作時要量一次再定 StorageLevel，見〈這份沒有回答的事〉。買不到「撈得出來看」與接續點——那要等 ADR-0015。

**這一節被推翻過的東西，留著當紀錄**：本 ADR 初稿曾提議「落地成一張叫 `ranked_predictions` 的表、切三個 node、用 `Node(writes=[...])` 保住 best-effort」。三處錯誤：(1) `ranked_predictions` 是 inference 的 production 輸出（`conf/base/catalog.yaml:434`），撞名；(2) 落地不省 shuffle——兩條下游各自的 cell window 不變；(3) `writes=` 的登記理由是「逐 partition 寫、避免 driver 物化整表」（R1 兩筆皆然），而 `labeled` 是 Spark DataFrame、`insertInto` 本來就分散式，借那個機制純粹是為了把 `.save()` 塞回 node 的 try/except，屬於 `deliberate-non-goals.md` 禁止的「為了讓自己的程式碼合規而擴充登記」。

## 決定 4：Optuna 閉包改成 class——**只是可讀性改善，不解決平行執行**

**做什麼。** `tune_hyperparameters` 裡那個 64 行的 `objective` 巢狀閉包，捕捉十個以上的外層變數，還偷改一個外層的 `best_state` dict。改成 `steps/hpo_scoring.py` 裡的一個 class：

```python
class TrialScorer:
    def __init__(self, algorithm_params, seed, X_v, y_v, groups_v, ...):
        ...
        self.best = {"score": -1.0, "model": None, "iteration": 0, "params": {}}

    def __call__(self, trial):        # Optuna 只要求 callable
        ...
        return score
```

`best_state` 從「被閉包偷改的外部 dict」變成實例屬性——狀態的歸屬寫在型別上，讀起來誠實。**收穫僅止於此。**

**⚠ 這裡要修一條錯的登記。** `architecture-constraints.md`（2026-08-29 核對於 :105）寫著：

> 若未來要加平行執行，`tune_hyperparameters` 內嵌的 Optuna 閉包會是第一個擋路的東西——它不可 pickle。

**這句有兩層錯，本 ADR 落地時要一併修掉那條登記**：

1. **pickle 送得過去的是程式碼，不是模型。** `nodes.py` 的 `best_state["model"] = adapter` 存的是訓練好的 LightGBM 物件，躺在 driver 的 Python 記憶體。多行程平行時每個子行程改的是**自己那份** `best_state`，主行程拿到的仍是 `None` → `finalize_model` 沒有模型可用。**真正的擋路點在這裡。**
2. **Optuna 平行化不需要 pickle objective。** `study.optimize(..., n_jobs=N)` 用執行緒（共用記憶體）；多行程做法是各行程自己建 objective、共用一個 storage。而本專案已經在用後者的基礎設施——`hpo_resume.py:36` 的 `JournalStorage` 正是 Optuna 的多行程 backend。

**真要平行的話，方向是「從磁碟讀回贏家」而不是「讓閉包可 pickle」**：`hpo_resume.py:61` 的 `write_checkpoint` 每次刷新最佳成績就 `adapter.save()` 到磁碟。⚠ **但現況的 checkpoint 不能直接拿來多行程用**，見〈刻意不做〉第 3 條。

**為什麼還是做這個改動。** 改動無害、讀起來誠實，而且順手做掉。但**沒有任何票或需求要求平行 HPO**——那條登記是一句沒被驗證過的假設，本 ADR 初稿照抄它當成主要理由，是被它誤導。留著不修，下一個人會重犯。

**界線提醒**：`deliberate-non-goals.md` 規定「HPO 搜尋診斷必須留在 `tune_hyperparameters` 尾端、不得抽成 DAG node」——那條管的是**診斷寫出**，不是這個閉包。拉成 callable 不違反它，但重構時不得順手把診斷升格成 node。

## 決定 5：多欄 entity 改成**真的支援**，不是加守衛擋掉

**做什麼。** `predict_and_write_test_predictions` 目前這樣（`nodes.py:1124-1130`，守衛在前）：

```python
if len(entity_cols) != 1:
    raise ValueError(
        f"predict_and_write_test_predictions expects single entity column; "
        f"got {entity_cols}."
    )
cust_id_col = entity_cols[0]
```

改成比照 `pipelines/inference/nodes.py` 用整個 list（inference 用 `identity_cols`，即 `core/schema.py` 推出來的 `[time] + entity + [item]`），**並拿掉那個 `raise`**。

**為什麼不加「entity 恰好一欄」的 A 系列代號。** 原本考慮加 A27「`schema.entity` 恰好一欄」。查證後撤掉：

- `conf/base/parameters.yaml` 把 `entity` 宣告成 list（`entity: [cust_id]`），框架的設計就是多欄。
- catalog 的欄位由使用者按自己的情境設定，要兩欄就宣告兩欄——**問題不在 catalog**。
- **對照組就在隔壁**：inference 從頭到尾用 `identity_cols`，兩欄照樣寫得出來。

所以那個 `raise` 不是在保護框架限制，是在擋自己只寫得出一欄。加 A27 等於把「沒做完」寫成「不支援」。

**但要補一個守衛，因為拿掉 raise 之後會靜默掉欄。** `io/hive_table_dataset.py:182` 是 `df = df.select(*self._insert_column_order())`——只留 catalog 宣告的欄。`schema.entity` 兩欄但 catalog 只宣告一欄的話，**第二欄被這個 select 默默丟掉**。

（`training_eval_predictions` 明列 `columns:`，走的正是這條路。同檔另有 `columns: "auto"` 的條目——那些從 DataFrame 推 schema，不受此影響。本守衛只針對明列 `columns:` 的條目。）

而 `core/consistency.py` **只讀 parameters YAML、從不讀 catalog**（2026-08-29 全檔 grep `catalog` 零命中），所以 A 系列看不到 catalog 的欄位宣告，這件事擋不到 Layer-1。

因此：

- `io/hive_table_dataset.py` 加一個公開的 `declared_columns` property（形狀比照既有的 `existing_partition_values()`：dataset 物件回答關於自己的問題）。
- 加一個 **A28** 不變量擋這件事。

**為什麼不讓 node 直接讀 `self._columns`。** 那是私有屬性，而「node 讀別人的底線私有名」正是這次要清的東西（見〈刻意不做〉的 #199）。

### 修訂（2026-08-30，實作 #224 時）：守衛從 node 移到一致性層

**本節原本寫的是**「`nodes.py` 加 `require_entity_columns_declared(...)`，docstring 標明是**前置檢查**。node 透過 `Node(writes=...)` 本來就拿得到那個 dataset 物件」。實作時審查指出這個結論不成立，改掉。

**原推論的洞。** 上面「`core/consistency.py` 只讀 parameters、從不讀 catalog」這個**事實仍然成立**，但它只證明了「總閘 `validate_config_consistency` 擋不到」，不等於「只能放 node」。該模組自己的 legend 寫著另一條路：

> Layer 1 invariants that hang off a single command instead of the aggregator, because they need context the aggregator never sees: A12/A13 和 A21（CLI flags）、A22（`--post-training`）、A24/A26（config keys whose harm belongs to one pipeline）

**看不到的東西掛在「指令」上，不掛總閘**——已經有 6 個代號在用這招。而 `__main__.py` 早就在向 catalog 的 dataset 物件問問題（`_collect_existing_snap_dates` 走 `catalog.get_dataset(name).existing_partition_values`），`declared_columns` 正是照那個形狀做的。

**代價是實際的，不只是分類潔癖。** `pipeline.py` 的順序是 HPO → `train_model` → `calibrate_model` → …… → `predict_and_write_test_predictions`。守衛放 node 裡，catalog 少宣告一欄要等**整輪搜尋跑完**才報——那正是決定 3 為了 `final_model_strategy` 立 A25 的同一個理由（本 ADR 自己的 User Story 4）。放在指令上則連 Spark cold start 都還沒付。

**改成怎樣。**

- `core/consistency.py` 加 predicate `entity_columns_declared_errors(parameters, declared_columns, target_name)`，登記為 **A28**。
- 它**仍然不讀 catalog**：呼叫端讀好 `declared_columns` 傳進來，predicate 保持純函式。這才是「consistency.py 從不讀 catalog」與「這條不變量住在一致性層」可以同時成立的原因。
- `__main__.py` 的 `training` 指令在 `get_or_create_spark_session` **之前**呼叫它。`runtime_params` 刻意傳 `{}`：替換填的是分區**值**（`${model_version}`），而這裡只讀欄位**名**，等版本算完只會白付一次 cold start。
- node 端**不留 runtime backstop**，理由同 A24：純 config 判準，沒有資料相依的東西可以再驗一次。

**代號用 A28 不用 A27。** A27 已在 #222 的切票留給 #200；而本節上面那段「考慮過 A27＝entity 恰好一欄、已撤掉」也還在，同一個號碼再指第三件事只會讓引用它的人讀錯。`consistency.py` 的 legend 對 A16/A17/A18 已經立過同一條規矩：**號碼只退役、不回收**。

**同一批發現、處置不同的兩處**：

| 位置 | 處置 |
|---|---|
| `core/consistency.py:1151` 的 `_REQUIRED_COLUMNS = {"cust_id", "snap_date", "prod_name", "score"}` | 動到 A11、跨 evaluation，**開了 issue #220**，不在本次範圍 |
| `evaluation/statistics.py:9,40` 的 `entity_col: str = "cust_id"` 預設值 | `src/` 裡零呼叫端（只有測試在用），不在活路徑上，不處理 |

## 決定 6：7 個 diagnosis node 留在原地，加一段導航 docstring

**做什麼。** 這 7 個 node 的 `def` 在 `src/recsys_tfb/diagnosis/model/` 底下，**不搬**。改為在 `pipelines/training/nodes.py` 的模組 docstring 列出完整清單與理由：

| node | `def` 在哪 |
|---|---|
| `compute_feature_statistics` | `diagnosis/model/feature_stats.py` |
| `compute_feature_importance` | `diagnosis/model/importance.py` |
| `compute_quadrant_profiles` | `diagnosis/model/shap_cases.py` |
| `compute_quadrant_cases` | `diagnosis/model/shap_cases.py` |
| `compute_shap_diagnostics` | `diagnosis/model/shap_per_item.py` |
| `compute_gain_ledger` | `diagnosis/model/gain_ledger.py` |
| `select_shap_population` | `diagnosis/model/population_spark.py` |

這就是 `pipeline-node-design.md`〈已登記的例外〉第三筆（「`pipelines/training/` 的部分 node `def` 在 `recsys_tfb.diagnosis.model` 底下」）的實際內容——原文只說「部分 node」、沒有列舉。

**代價。** 違反規則 8 的「`nodes.py` 是這條 pipeline 的 ML 故事唯一的家」。用 10 行導航 docstring 換掉這個代價：讀者從 `nodes.py` 第一段就找得到路。

**為什麼不搬進 `nodes.py`。** 三個理由：

1. **那 7 個殼正是規則 3 明文要擋的形狀**——轉手 node ＋ 大 helper。要不違反就得把 `diagnosis/model/` 那 1316 行的決策全部上浮，是另一個等級的工程。
2. **未來要搬就白做。** 見〈刻意不做〉第 1 條：diagnosis 可能整組獨立成一條 pipeline。真搬的話這 7 個殼要全刪重寫；留在原地只要改 import。
3. 多一層轉手，追 bug 要多跳一次檔。

**一個不對稱要記著**：這 7 個裡有 6 個吃 `model`，只有 `select_shap_population` 不吃（它的輸入是 `training_eval_predictions` / `test_model_input` / `parameters` / `predict_manifest`）。決定 7 的「用模型當特徵權威」對它不適用。

## 決定 7：diagnosis 介接口——`preprocessor_view` 那條拿掉，`parquet_handle` 那條**不拿**

**背景。** 7 個 diagnosis node 有兩個輸入不在 catalog 裡。盤點原本判定兩個都能「拿掉一條邊」。查證後**只有一個成立**。

### 漏點 1：`preprocessor_view` —— 拿掉（成立）

7 個裡有 5 個吃它。它是 `apply_feature_selection(preprocessor, parameters)` 的結果，memory-only。改用 inference 已經走過的那條路：

```
  問「用哪些特徵、什麼順序？」 → 問 model（model_feature_columns()）
  問「怎麼編碼？」             → 問 preprocessor artifact

  兩個都在 catalog 裡 → preprocessor_view 這條邊消失
```

6 個 node 本來就有材料，只有 `compute_feature_statistics` 要多接一個 `model` 輸入。（`select_shap_population` 不吃 `preprocessor_view`，見決定 6 末。）

**副作用**：`model_feature_columns` 現在住在 `pipelines/inference/steps/`，一旦有本 pipeline 以外的消費者就要換家（規則 8）。落腳處是 `models/`，與 `feature_selection.py` 作伴。

**一個誠實標註**：曾經以為這修掉一個靜默錯誤（config 漂移），查證後發現**版本機制擋得住**——`feature_selection` 住在 `training:` block，改它會 bump `model_version` → model 的 catalog 路徑跟著變 → 整條訓練鏈被拉回重跑。所以**這不是修 bug，是把介接口弄乾淨**。（inference 那邊的理由不同，見 [ADR-0011](0011-inference-validation-two-layers.md) §5。）

### 漏點 2：`train_parquet_handle` / `test_parquet_handle` —— **不拿，改成記錄阻礙**

盤點的提議是「diagnosis 拿 `parameters` 就自己算得出同一個 cache 路徑，不用別人傳」。**這個提議不成立**：

- `conf/base/parameters_training.yaml` 的 `cache.root` 是**相對路徑**（`data/recsys_cache`），而 `nodes.py` 的 `_resolve_cache_path` 直接 `Path(...)`、不 `resolve()`，整個應用是 CWD-relative（`__main__.py` 用 `Path.cwd()`）。
- 今天沒事，是因為兩個 node 在**同一次執行**裡、共用 CWD。**一旦 diagnosis 獨立成另一條 pipeline（正是這條在鋪路的事），兩次執行從不同目錄啟動就會指到不同地方，而且不報錯。**
- 生產環境的 driver 是不是同一台，**repo 裡找不到證據**：`spark.master: yarn` 在 `conf/base/parameters.yaml` 是註解掉的；全 repo 唯一設 deployMode 的是 `conf/spark-local/spark-defaults.conf`（`local[*]` ＋ client，本機用）。cluster mode 下兩次提交幾乎確定不共用 driver-local 快取。

**所以這條改成「記錄下來」**：`cache.root` 是相對路徑，是 diagnosis 切割的真正阻礙之一，切之前要先解決（把它 resolve 成絕對路徑，或給 `ParquetHandle` 一個真的落地點）。見〈刻意不做〉第 8 條。

### `compute_feature_statistics` 會多一個 `model` 輸入——這個耦合要寫下來

它現在只用 `preprocessor_view["feature_columns"]` 決定「算哪些欄的統計」（`diagnosis/model/feature_stats.py`）。**它是資料層診斷**——訓練資料裡這些特徵的 null rate、mean、distinct——本來不需要模型。改成問 model 等於把資料診斷耦合到模型 artifact。

**接續成本的影響，兩種切法不同**：

- **`--from-node compute_feature_statistics` 沒有變。** 切片按拓撲位置切（`self._sorted[idx:]`），它後面 6 個 diagnosis node **本來就吃 `model`**，所以 `model` 早就是這個切片的必要輸入。
- **`--only-node compute_feature_statistics` 變了**：今天只需要 `preprocessor`（`base_dataset_version` 範圍）＋ cache；改後需要 `model`（`model_version` 範圍）。

**為什麼接受這個耦合**：`feature_statistics` 本來就寫在 `data/models/${model_version}/` 底下，替一個不存在的模型算診斷本來就不成立。

**考慮過的替代**：讓它自己 `apply_feature_selection(preprocessor, parameters)`，不碰 model。缺點是 `preprocessor` 是 `base_dataset_version` 範圍、`feature_selection` 是 `training:` 範圍，兩者可以不同步——config 漂移時會**靜默**對錯的欄集算統計。問模型不會。

### 驗收條件（兩件事要分開測，不要合成一條）

盤點寫「`--from-node compute_feature_statistics` 單獨跑得起來」。**那個條件是假綠的**，但**修法不是「要求它失敗」**：

`--from-node compute_feature_statistics` 需要 `train_parquet_handle`（memory-only），所以切片會把 `cache_train_model_input` 拉回來，而那個 node 的決策之一正是「**半成品 cache 清掉重建**」。也就是說：

```
  正常 --from-node 路徑遇到 partial cache
      → cache node 被拉回來 → 清掉 → 從 Hive 重建 → 成功
      ↑ 這是既有的 recovery 行為，是對的，不能為了讓測試轉紅而禁止它
```

所以驗收要**拆成兩條**，測不同的東西：

| 測什麼 | 怎麼測 | 期望 |
|---|---|---|
| **cache node 的 marker/重建行為** | 弄出一個有目錄、無 `_SUCCESS` 的 cache，跑 `cache_train_model_input` | 清掉並從 Hive 重建，成功 |
| **診斷對「已拿到的 handle」的輸入驗證** | 直接把指向半成品目錄的 `ParquetHandle` 傳給 `compute_feature_statistics`（來源不可用，無法重建） | **必須失敗**，不得算出數字 |

第二條才是真正的漏洞：`feature_stats.py` 的 `count_rows(path)` 讀到半成品不會報錯，統計照算、MLflow 照記（`_populate_cache_from_hive` 的 docstring 自己寫了：複製中斷會留下沒有 `_SUCCESS` 的目錄）。

**分界線**：來源可用 → 該重建；來源不可用、或直接拿到 partial handle → 該失敗。

---

# 照判準直接推出的（沒有取捨）

這些不需要裁決，套規則就有答案。列出來是為了讓重構範圍完整。

## 兩個新的 config 檢查：A25 與 A26

`core/consistency.py` 的 A 系列最新是 **A24**。A16／A17／A18 已退場且該檔明文禁止重用編號，所以下一個是 A25。

| 代號 | 檢查什麼 | 為什麼要搬 |
|---|---|---|
| **A25** | training 側 HPO 與 finalize 參數域：`training.hpo_objective`、`training.final_model_strategy` 的合法值 | `final_model_strategy` 打錯目前要等**整輪 HPO 跑完**才炸，一個 typo 賠掉整輪 |
| **A26** | `dataset.test_snap_dates` 不得有兩種拼法指到同一個月 | 純 config，現在留在 `nodes.py` 的 node body 裡 |

形狀比照 A20（training 側 `diagnostics.*` 參數域）——一個代號管一族參數域。A26 比照 A24 的接線方式（只有一條 pipeline 讀的 config 鍵，單獨接線）。

規則 11 明文要求這樣做，所以這**不是新增例外**。

## 五個 `raise` 逐一標種類

規則 11：留在 node 的 `raise` 必須在 docstring 標明是**前置檢查**（檢查輸入，失敗＝上游沒準備好）還是**後置條件**（檢查自己算出來的結果，失敗＝這個 node 的邏輯或設定有問題）。兩種錯誤要找的人不同。

| 位置（2026-08-29） | 檢查什麼 | 種類 | 去向 |
|---|---|---|---|
| `nodes.py:302` | 輸入不是 Spark DataFrame | 前置檢查 | 留 node |
| `nodes.py:324` | `rmtree` 之後 `_SUCCESS` 還在 | **後置條件** | 留 node |
| `nodes.py:404` | `test_snap_dates` 兩種拼法 | 前置檢查 | **搬去 A26** |
| `nodes.py:1000` | 設定的月份在 cache 裡沒有列 | 前置檢查 | 留 node（要看 cache，不是純 config） |
| `nodes.py:1125` | `schema.entity` 不只一欄 | — | **刪掉**（見決定 5；取而代之的 A28 住在一致性層，不在 node） |

另按**規則 12**（不是規則 11）：守衛一律改名 `require_*`，只警告不 raise 的用 `warn_*`。training 現在一個都沒有。

## 21 處函式體內 import 收到模組層

三條 pipeline 的實測對照（2026-08-29）：

```
  dataset/nodes.py     函式體內 import：0
  inference/nodes.py   函式體內 import：1   ← 且模組 docstring 明寫為什麼
  training/nodes.py    函式體內 import：21  ← 唯一的例外
```

而 `inference/nodes.py` 是在**模組層**直接 `from pyspark.sql import DataFrame, Window`——所以「pyspark 太重所以延後 import」不是這個 repo 的慣例。另外 `nodes.py:785` 的 `import numpy as np` 跟模組層第 11 行重複。

全部收到模組層。**唯一保留的是 `_pdf_to_X`**（見〈刻意不做〉；inference 也保留同一個並在模組 docstring 說明）。

理由是規則 8：「import 那一行就說出這個步驟來自哪個 concern」。建了 `steps/` 之後，那幾行 `from .steps.X import ...` 就是 node 的目錄；埋在函式體裡等於沒有目錄。

## `steps/predict_months.py` 的純度用「自己的測試檔」釘

`_plan_predict_months` 與相鄰的 `_written_prediction_partitions`、`_test_month_dir` 現況都不 import pyspark，可做成零 pyspark 純模組——測試就不必開 SparkSession（本機 Spark cold start 2–4 分鐘）。

**但「今天很純」不等於「明天還純」**，要有測試釘住。repo 有兩種掛法：

| 掛法 | 例子 | 代價 |
|---|---|---|
| 進架構稽核的模組純度登記（S2） | `pipelines/dataset/month_plans.py` | S2 明文只管 `pipelines/dataset/`，要改登記範圍 |
| 自己的測試檔 | `pipelines/inference/steps/chunk_plans.py` | 兩個小測試（`test_no_pyspark_import` ＋ `test_no_project_import`），不動登記 |

**用後者。** `chunk_plans.py` 就在 `steps/` 底下、位置同型；而擴充 S2 登記撞上「別為了讓自己的新程式碼合規而擴充登記」。

## 其餘機械條目

- **模組 docstring 改寫**。`nodes.py:1` 現在寫 `"""Pure functions for the training pipeline."""`，但這個檔寫檔、mutate `parameters`、停 SparkSession、跑 MLflow——**沒有一個是 pure function**。比照 `inference/nodes.py` 開頭那段重寫，並併入決定 6 的導航清單。
- **每個具名步驟上方補 `# Decision —`**，說出決策**與選錯的後果**（規則 9）。
- **四個只重述簽章的 docstring 改寫**（規則 13）：`nodes.py:357`／`:362`／`:369`（三個 `"Skip-if-exists local-parquet cache for X."`）與 `:1268`（`"Log training results to MLflow."`）。要說「為什麼不是另一種做法」與「選錯會不會報錯」。
- **中文註解翻英文**（體例：程式碼註解與 docstring 一律英文，`docs/` 一律繁體）。training 是唯一還有中文註解的 pipeline。
  ⚠ **`nodes.py:539-549` 是 SparkContext 死亡的現場記錄，內容不得精簡**，只翻譯。
- **`finalize_model` 的 ranking／非 ranking 兩分支**把 extract → concat → `lgb.Dataset` 幾乎逐字重複。按規則 5：決策各寫各的，concat 與 Dataset 建構的**機制**抽一份進 `steps/refit.py`。
- **`resolve_weight_diagnostics`**（40 行）的呼叫端 node 整段就一個呼叫。「哪些欄構成 weight key」「什麼算 unmatched」上浮到 node body，機制進 `steps/sample_weights.py`。
- **`compute_test_mAP_spark` 補 `log_step`**。它整個沒有計時但裡面有真 Spark action。規則 10 只禁「包 lazy 區塊」、不要求「action 一定要被包」，所以不是違例——但這個 node 的時間去向不可見，而它讀的是全月份的預測表。
- **`log_step` 事件名固定**。`nodes.py:1198` 現在是 `log_step(logger, f"partition_{snap_date}_{prod_name}")`，log 聚合端會炸出 月份數 × 產品數 個不同的 step 名。改成固定名 `predict_partition`，值走結構化欄位：
  - `core/logging.py` 的 `log_step` 加一個 `**fields` 參數（併進 `extra`）。**純加法，既有呼叫端零改動。**
  - `JSON_EXTRA_FIELDS` 白名單加 `"prod_name"`（`"snap_date"` 已經在裡面）。
  - 修在共用層，因為 inference 逐 chunk 也會遇到同一個問題。

---

# 既有登記的一個缺口（不是本次造成的）

`_populate_cache_from_hive`（`nodes.py:168`）把 Hive 分區複製到 driver 本機，經 `recsys_tfb.utils.hdfs` 的 `copy_hdfs_to_local`。**這是間接寫入**——AST 掃描只看得到直接呼叫，所以稽核測試看不到它；而它也**不在 R4 表格上**（表格上的間接寫入只登記了 `tune_hyperparameters`）。

本次重構會把它搬進 `steps/local_cache.py`，位置更明確，但**掃不到這件事不會因此改變**。要不要把它補進 R4 表格是一個獨立的判斷（它寫的是快取、不是診斷副產物，可能不屬於 R4 的語意範圍），**動之前要問使用者**。

---

# 兩個變便宜的接續點

規則 7：產物落不落地，是接續成本的決定。判準＝「是不是某個宣告接續點的必要輸入」×「重算貴不貴」。

## `trained_model` 落地

開 calibration 時 `finalize_model` 的輸出是 `trained_model`，而 **catalog 沒有這個條目**（2026-08-29 `grep trained_model conf/base/catalog.yaml` 零命中）：

```
  finalize_model --→ "trained_model" --→ calibrate_model --→ "model"
                          ↑                                    ↑
                     catalog 無條目                        catalog 有條目

  --from-node calibrate_model
      → 要 trained_model → 沒落地 → 把 finalize_model 拉回重跑
      → final_model_strategy: refit_on_full 下 ＝ 一次完整 refit
```

規則 7 的兩格全中（是接續點的必要輸入 ＋ 重算很貴），所以落地。成本是多存一顆 booster。

## `calibrate_model` 進 `RESUME_CONTRACTS`

`tests/test_pipelines/test_resume_contracts.py` 的 calibration 變體只釘了 `finalize_model`，所以上面那個重跑成本**現在沒有任何測試看得見**。補一條 `"calibrate_model"` 接續點把它釘住。

（本條原本因「calibration 預計移除」而列為不處理。使用者 2026-08-29 決定**去留未定、照重構**，所以放回範圍。）

---

# 刻意不做的八件事

每條都附「什麼時候該做」。

## 1. diagnosis 獨立成一條 pipeline

**方向**（使用者 2026-08-29 提出）：

```
  training pipeline   ... → model → evaluation_results   ← 到此結束
            │
            ↓
  diagnosis pipeline  只吃 training 的產出
            │
            ↓
  evaluation 報表     拆兩半：一半只靠 training，一半靠 diagnosis
```

**難點是 `log_experiment`**：它有 10 個輸入，5 個是 diagnosis 產物。三條路各有代價——留 training 則 diagnosis 不獨立；移 diagnosis 則純訓練跑完 MLflow 什麼都不記；拆成 `log_experiment` ＋ `log_diagnostics`（補記到同一個 MLflow run）要傳 run_id、也要決定 run 什麼時候關。

**為什麼不在這份裁決**：(a) 它會動到 evaluation，而本次盤點只查了 training；(b) `log_experiment` 的 MLflow run 生命週期沒人查過；(c) `deliberate-non-goals.md` 已經把「SHAP／象限診斷搬到 evaluation」列為使用者要另開 grill 的接縫問題。

**什麼時候做**：另一場 grill。決定 7 的漏點 1 已經先把一條邊拿掉；漏點 2（`cache.root`）與第 8 條是還沒解的前提。

## 2. rank 落地 ＋ tie-break 統一 → 另一份 ADR

見決定 3。三個已經查到、寫下來省得下次重查的事實：

1. **當初不存 rank 的理由**：`docs/superpowers/specs/2026-05-13-batched-test-eval-spark-design.md:127-133`。逐 `(月份, 產品)` 分區寫入之後，跨產品的 rank 單一 chunk 算不出來。否決「寫完後跑一趟算 rank」的理由是「為一個**沒人讀**的欄位把 I/O 加倍」——**今天有三個消費者各自重算，這個前提到期了**。
2. **兩種 tie-break 不相容**：`population_spark.py` 用確定性（`orderBy(score.desc(), item_col)`，為了象限可重現）；`metrics_spark.py:281` 的 `rank_within_query` docstring 明寫未定義。
3. **evaluation 會靜默改行為**：`evaluation/nodes_spark.py:190` 是「rank 欄存在時就信上游」。加一個 rank 欄 → evaluation 自動改用 training 的 tie-break → **指標數字會變且無訊息**。

**建議的形狀**：統一成確定性 tie-break（按 `prod_name`），並跑一次前後 mAP 對照貼出來。理由：未定義的 tie-break 既不可重現、也不是有原則的選擇；診斷（挑案例給人看）本來就要求確定性。⚠ **平手實際發生的頻率沒量過**，動手前先用一句 SQL 數一次。

## 3. HPO 平行執行

決定 4 只讓 Optuna 的 scorer 可 pickle，**不啟用平行**，而且**那也不是平行的擋路點**（真正的擋路點是 `best_state` 持有 driver-local 模型）。

**什麼時候做**：有人真的提出需求時。目前沒有任何票或需求要求它。

**⚠ 現況的 checkpoint 不能直接支援多行程，別把〈決定 4〉那句讀成「已經可以了」。** `hpo_resume.py` 的 `write_checkpoint` 把模型與 meta 寫成**兩個獨立檔案、兩次獨立的 `os.replace`**（`adapter.save()` → replace 模型；`mkstemp` → replace meta），中間**沒有跨檔鎖、沒有版本指標**。多行程同時刷新最佳成績時，讀者可能拿到 A worker 的模型配 B worker 的 score／params。

`JournalStorage` 保護的是 **Optuna study 的記錄**，不保護這對檔案。

**啟用平行之前必須先解決其中之一**：單一 writer、檔案鎖、或改成「版本化的 checkpoint 目錄 ＋ 一個原子寫入的 manifest 指向當前贏家」。

## 4. `_pdf_to_X` 改名（issue #199）

`nodes.py` 從 `recsys_tfb.io.extract` import 三個底線私有名。實際擴散範圍三個不一樣：

| 名字 | 誰在用 | 處置 |
|---|---|---|
| `_composite_key_series` | 只有 `training/nodes.py:48` | **本次改名**（去底線），零跨界成本 |
| `_translate_weight_table` | 只有 `training/nodes.py:48` | **本次改名** |
| `_pdf_to_X` | `training/nodes.py:1117`、`inference/nodes.py:276`、`diagnosis/model/shap_per_item.py`（2 處）、`shap_cases.py`（2 處） | **#199**，不在本次 |

**但 #199 列入 training 重構的完成定義**（使用者 2026-08-29）：不做完不算重構完成，只是它是獨立的最後一張票。動它要同時改 5 個模組，塞進重構 PR 會讓 diff 讀不動。

## 5. `_REQUIRED_COLUMNS`（issue #220）

見決定 5。動到 A11、跨 evaluation。

## 6. calibration 的去留

使用者 2026-08-29：**還沒決定要不要刪**。所以這次照常重構 calibration 路徑（`cache_calibration_model_input`、`calibrate_model`，以及 `create_pipeline(enable_calibration=...)` 的兩個分支）。

**如果之後決定刪**，本份有三處要回頭改：決定 1 的 6 個變 5 個、`steps/local_cache.py` 的路徑常數少一筆、兩個接續點那節整節作廢。

## 7. 「診斷失敗該不該停 pipeline」

今天 `select_shap_population` 整段包在 `try/except` 裡，失敗只 warn、訓練繼續（spec §12 的刻意設計）。

**但那個 warning 出現在一個跑了好幾小時的 log 尾端，實務上沒人會看到。** 硬失敗反而顯眼——而且 `model` 有 catalog 條目，`--from-node select_shap_population` 就救得回來，HPO 與 refit 都不用重跑。

**為什麼這次不動**：改掉一個明文的行為契約不該是形狀重構的副作用。值得單獨開一題。（⚠ 當時的但書：`predict_manifest` 是 memory-only，切片擴張會把 `predict_and_write_test_predictions` 拉回來，它就算全月份 skip 也要把 2.2 億列 × 2 個字串欄拉進 driver 算 distinct。**2026-08-30 更新：這個但書已經不成立**——issue #233 把 `predict_manifest` 落地了，`--from-node select_shap_population` 現在不會再拉回 predict node。）

## 8. `cache.root` 的相對路徑

見決定 7 漏點 2。`conf/base/parameters_training.yaml` 的 `cache.root: data/recsys_cache` 是 CWD-relative，`_resolve_cache_path` 不 `resolve()`。同一次執行內沒事，跨執行會靜默指到不同地方。

**什麼時候做**：diagnosis 要獨立成 pipeline 之前，這是必須先解決的前提之一。

---

# 這份沒有回答的事

1. **PR 怎麼切、什麼順序。** 那是 `/to-tickets` 與 `/triage` 的事。本份只回答「為什麼是這個形狀」。切票時請一併讀 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)。
2. **node 數量沒有實跑 `--list-nodes` 對照。** 21 是從 `pipeline.py` 數 `Node(` 出來的（盤點與本份各數一次、結果一致），但與預設 config 下的實際值可能有出入。
3. **「一個 helper 承載幾個決策」是語意判定。** `_materialize_parquet_handle` 那 4 個裡，本份對兩個有保留——「非 Spark 輸入 → TypeError」比較像型別守衛、「`force_refresh` 清不掉硬失敗」偏錯誤處理，真正改變資料的只有「什麼算 cache hit」與「半成品清掉重建」。決定 1 不受這個保留影響（決策不論 2 個還是 4 個都該上浮），但拆出幾個具名步驟會受影響。
4. **決定 1 的連鎖效應是從測試碼推的**（`test_architecture_constraints.py` 的 `set(found)` 比對邏輯），沒有實際搬檔跑一次看它怎麼紅。
5. **`docs/pipelines/training.md`（686 行）只讀了部分章節**，若其他章節另有與判準衝突的敘述，本份沒有涵蓋。
6. **象限診斷不是已退場的那個 `quadrant`——這件事有證據，不必再問。** 退場的是**AUC 門檻切象限**（`diagnosis/__init__.py` 模組 docstring：「``triage``（per-item 判定＋建議槓桿）與 ``quadrant``（AUC 門檻切象限）就是因為違反它而整層退場」；對應的 A17 已 retired）。training 現行的 `compute_quadrant_profiles`／`compute_quadrant_cases` 是 **rank-based、無門檻**（`population_spark.py` 的 `_rank <= top_k_decision`）。而 `deliberate-non-goals.md` 直接點名它們是活的，並警告「列名單只會再一次被讀成『這些是死碼』（2026-08-25 就發生過）」。**盤點文件把這條列為『沒有證據能證實、動之前要問使用者』，那是錯的——證據有三份。**
7. **`select_shap_population` 平手的實際頻率沒量過**，見〈刻意不做〉第 2 條。
8. **決定 3 的 `persist` 在生產資料量下要吃多少 executor 記憶體／磁碟，沒有量過**（閘門 G7）。文件裡的 2.2 億列來自 `nodes.py:1141` 的既有註解，不是為這件事量的。StorageLevel 要等實測再定。

---

## 延伸閱讀

| 想知道 | 讀 |
|---|---|
| 要照著做的判準（13 條形狀規則） | [`pipeline-node-design.md`](../agents/pipeline-node-design.md) |
| 可機械檢查的約束、R1–R4 登記、例外登記規則 | [`architecture-constraints.md`](../agents/architecture-constraints.md) |
| 重整一整條 pipeline 的**流程**（排順序、切 PR、選驗證手段） | [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md) |
| training 的 16 條落差逐條清單與現況行號（⚠ 數處已被本份修正） | [`2026-08-25-training-pipeline-gap-survey.md`](../notes/2026-08-25-training-pipeline-gap-survey.md) |
| dataset 那次同型裁決的完整論證 | [ADR-0008](0008-dataset-modules-split-by-role.md) |
| node 邊界該不該合併的實例論證 | [ADR-0010](0010-inference-chunked-scoring-shape.md) §4 |
| 「模型當特徵權威」那條的原始論證 | [ADR-0011](0011-inference-validation-two-layers.md) §5 |
| 刻意不做的事（動手前掃一眼） | [`deliberate-non-goals.md`](../agents/deliberate-non-goals.md) |
| 程式碼現在長什麼樣 | `graphify-out/GRAPH_REPORT.md` |
