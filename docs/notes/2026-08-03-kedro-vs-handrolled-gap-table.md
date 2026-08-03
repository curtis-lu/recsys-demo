# Kedro 官方 × 手刻版 對照表（階段 2）

> 2026-08-03。輸入：`2026-08-03-kedro-official-design-rationale.md`（Kedro 1.5.0，38 條可判定約束 C-1～C-38）
> ＋ `2026-08-03-handrolled-core-inventory.md`（core/io 現況）＋ `2026-08-03-day1-doc-triage.md`（檔 A 死活判定）。
> 用途：階段 3 grilling 的題目清單。**本表不下結論，只把選擇題排好。**

---

## 零、一個決策解釋了大半張表：**本專案沒有 hooks 機制**

`grep -rni hook src/recsys_tfb/core/ src/recsys_tfb/io/` → **零命中**。這一件事的連鎖後果：

| 後果 | 證據 | 性質 |
|---|---|---|
| C-29～C-33（hook 參數不得有預設值、不得依賴 hook 順序、hook 狀態唯讀…）**整組不適用** | 無 hook 機制 | 不適用 |
| `io/base.py` 把 `load`/`save`/`exists` 攤平成公開 `@abstractmethod`（Kedro 是私有 `_load`/`_save` ＋ 公開 wrapper）**沒有損失** | Kedro 的公開 wrapper 正是掛 hook 與 versioning 的地方；我們沒有要掛的東西 | 刻意偏離（但理由沒寫下來） |
| observability 從「可插拔」變成「強制」 | Kedro 把 observability 列為 hook 的**使用場景**（`docs/extend/hooks/examples.md:235`）＝可以不裝；本專案硬編進 Runner（`core/runner.py` 7 處 `logger.`，每個 node 必記 started/completed/failed） | **正向偏離**——比 Kedro 強 |

→ **階段 3 題目 Q1**：這條要寫成原則嗎？候選陳述：「本框架不提供 hook 機制；橫切關注點（logging、計時、錯誤處理）直接實作在 `core/runner.py`，代價是無法在不改 runner 的情況下加新的橫切行為。」

---

## 一、無意識偏離（最高價值：不是決定，是沒想到）

Kedro 在 **node 建構期就 raise** 的四條驗證，我們的 `core/node.py`（19 行）**零 raise**：

| Kedro 約束 | Kedro 出處 | 我們的現況 | 這條值得採納嗎 |
|---|---|---|---|
| C-5 每個 node 至少要有一個 input 或 output | `kedro/pipeline/node.py:152` | 無檢查 | 我們有零 output 的 side-effect node（`SlicePlan.skipped_side_effect`），語意不同，**需重新界定** |
| C-6 node／tag 名稱只能含英數字 `-_.` | `kedro/pipeline/node.py:165,174` | 無檢查 | 低價值？我們的 node 名來自函式名，天然受限 |
| C-7 同一 node 的 input 名不得與 output 名相同 | `kedro/pipeline/node.py:719` | 無檢查 | **看起來便宜且真能擋錯**，候選採納 |
| C-10 pipeline 宣告 inputs 必須是 free inputs | `kedro/pipeline/pipeline.py:60–74` | 無檢查（我們的 `Pipeline.inputs` 是推導出來的，不是宣告的） | 前提不同，需重新界定 |
| C-12 不得存 `None` 到 dataset | `kedro/io/core.py:303` | 無檢查 | 待查我們是否有 node 回傳 `None` |

已有的：C-9（循環相依）→ `core/pipeline.py:91` `raise ValueError("Pipeline has circular dependencies")` ✓

→ **階段 3 題目 Q2**：這五條逐條決定採納／不採納／重新界定。採納的每條都要能寫成 `core/node.py` 或 `core/pipeline.py` 的建構期 raise，**否則不准寫進原則文件**（寫了又不檢查＝口號）。

---

## 二、正面對撞：`@handle` vs C-4

| | 陳述 | 出處 |
|---|---|---|
| Kedro | 程式化 `catalog.save(...)` **只允許**出現在測試檔或 hosted notebook；產品程式碼一律用 YAML | C-4，`catalog-data/advanced_data_catalog_usage.md:169` |
| Kedro | node 函式體內不得取得或操作 DataCatalog 物件 | C-1，`configure/configuration_basics.md:250` |
| Kedro | node 應為 pure function，「without any observable side effects」 | 官方文件＋glossary（research 檔 :98, :103） |
| 我們 | `@` 前綴讓 node 拿到 catalog dataset handle 自己 `.save()` | `core/runner.py:79-87` |
| 我們 | 全 repo 唯一使用點 | `pipelines/training/pipeline.py:140` → `pipelines/training/nodes.py:1082-1253` |
| 我們的理由 | 分區級 Hive overwrite，避免整表重寫（chunked save） | 只存在於 `core/runner.py:79-81` 的程式碼註解 |

→ **階段 3 題目 Q3**（本次最核心的一題）：三選一——
(a) 承認例外並寫成有條件的規則：「node 不得做 I/O，除非透過 `@` 前綴且理由是 X；每個新增的 `@` 使用點需 ADR」。可機械檢查：`grep -rn '"@' src/recsys_tfb/pipelines/` 命中數必須等於已核准清單。
(b) 消除例外：把 chunked save 改成框架層能力（例如讓 catalog 支援 append/partition 寫入），讓 node 回歸純函式。
(c) 不寫這條原則。

---

## 三、明確不適用（Kedro 的前提我們沒有）

| Kedro 約束 | 前提 | 我們的現況 |
|---|---|---|
| C-16 所有 dataset/node 必須可 pickle（不得 lambda／closure／巢狀函式） | `ParallelRunner` | `core/runner.py` 只有 sequential 一種 |
| C-17 不能並用多行程的 dataset 要設 `_SINGLE_PROCESS = True` | 同上 | 同上 |
| C-33 依賴 dataset/node 層 hook 時不得用 `ParallelRunner` | 同上＋hooks | 兩個前提都沒有 |
| C-19 `conf/local/**` 不得進版控 | Kedro 的 `conf/local` ＝使用者專屬設定 | **我們沒有 `conf/local`**，但有 `conf/spark-local` 且**進版控**（`git ls-files conf/` 確認）——名字撞車、語意相反 |
| C-15 node 級 namespace 不得用來分組執行 | Kedro 的 namespace 機制 | 我們的 `Node` 無 namespace 概念 |
| C-8 dataset 名稱不得含 `.`（保留給 namespace） | 同上 | 前提不存在 |

→ **階段 3 題目 Q4**：不適用的條目**要不要寫進原則文件**？寫的價值是標明「因為我們沒有 X 所以不管 Y」——以後有人加平行執行時，才知道要先補 picklability。不寫的成本是文件變長。
→ **階段 3 題目 Q5**：`conf/spark-local` 的命名撞車要不要處理？改名（動很多路徑）還是寫進原則文件警告？

---

## 四、已對齊（低價值，簡列）

C-3 路徑不散落程式碼（檔 A triage 驗過：`pipelines/*/nodes*.py` 只命中一處參數化路徑）｜C-9 無循環相依（`core/pipeline.py:91`）｜C-13 一 pipeline 一資料夾（五個 pipeline 子目錄）｜C-14 元件解耦、diagnostics 不得 import pipeline 內部（本專案已有此邊界宣稱）｜C-18/C-20 機密與 `data/**` 不進版控（`.gitignore:14`）｜C-24 超參數集中定義（`conf/base/parameters*.yaml`）｜C-35 raw 層不被修改。

---

## 五、我們有、Kedro 沒有（本專案的加法，各需存在理由）

| 模組 | 行數 | Kedro 有無對應 | 待寫的理由 |
|---|---|---|---|
| `core/consistency.py` | **1374** | 無（Kedro 靠 node 契約＋第三方 pandera） | **正確性重心的根本差異**：Kedro 押在 node 契約，我們押在集中式 predicate。這是全表最大的刻意偏離，量體比：consistency 1374 行 vs node 19 行 |
| `core/versioning.py` | 415 | Kedro 有 dataset versioning，但**官方對「不涵蓋什麼」沒有邊界宣告**（research §9.8） | 我們的三層 hash 版本 ID 在解不同問題：Kedro 版本化的是「檔案的哪一版」，我們版本化的是「哪組設定產生的產物」 |
| `core/schema.py` | 189 | 無 | 欄位角色集中定義 |
| `core/safe_eval.py` | 141 | 無 | HPO 宣告式搜尋空間 |
| `core/logging.py` | 303 | Kedro 靠 hooks | 見 §零 |
| `Pipeline.slice_from/slice_only` ＋ `SlicePlan` 自動擴張 | — | Kedro 有 `filter()`，但**無自動擴張上游 producer** | 我們多做的：自動拉回缺少輸入的上游 node，並回報 `auto_included`／`skipped`／`skipped_side_effect` |

→ **階段 3 題目 Q6**：C-37（`filter()` 多條件是交集語意）與 C-38（「只跑缺漏輸出」的三條語意規則）是 Kedro 對切片語意的明文規定。我們的 `_slice_with_expansion` 語意與之不同——要不要對齊，還是明文寫「我們刻意不同，因為 X」？

---

## 六、從檔 A 搶救的 6 條（來源：day1-doc-triage）

依「不搶救就會失去什麼」排序，全部有程式碼證據：

1. **Observability 是一等關注點**——實作最完整卻從未被任何文件寫成設計價值。與 §零 合併處理。
2. **預設純函式、`@` 是唯一具名例外**——與 §二 Q3 合併處理。
3. **測試三層策略**（unit／integration／schema-consistency）——`tests/test_pipelines/test_training/test_nodes.py:184`、`test_pipeline.py:167`、`tests/test_core/test_consistency.py` 都活著，但檔 B 與 CLAUDE.md 都只談測試效能不談分層。
4. **生產程式碼與 notebook 徹底隔離**——`notebooks/` 兩檔、零被 `src/` 引用。
5. **中介產物命名要有業務語意**——`conf/base/catalog.yaml` 68 個名稱零 `tmp`/`_v2`/`_final`。
6. **pipeline node 不得依賴可變全域狀態**——node 層零污染，僅框架層兩處有註解的例外（`utils/spark.py:49,87,161,232`、`core/logging.py:139`）。

---

## 七、退役但不搶救的一條（需要決定怎麼處理）

檔 A `### 3. Build small, composable processing units` ＋ 反模式「giant training functions that do everything」——**現況違反，且集中在最核心的 training pipeline**：

| 函式 | 行數 | 位置 |
|---|---|---|
| `tune_hyperparameters` | **230** | `pipelines/training/nodes.py:520-749` |
| `predict_and_write_test_predictions` | **174** | `pipelines/training/nodes.py:1082-1253` |
| `finalize_model` | **157** | `pipelines/training/nodes.py:750-904` |

（行數已由主對話用 `awk` 獨立數過，非 agent 自述。同檔第四長函式為 `log_experiment` 99 行，落差明顯。）

→ **階段 3 題目 Q7**：三選一——
(a) 不寫這條原則（承認我們不管函式大小）。
(b) 寫成有數值門檻的規則，並明列現有三處為已知例外＋重構 backlog。
(c) 先重構再寫（本次範圍外）。
**注意**：若寫成無條件的「函式要小」，原則文件第一天就是紅的——這正是「寫得出來但檢查不過」的口號型原則，是這次要避免的東西。

---

## 待補（本表尚未涵蓋）

- C-21～C-23（設定載入期的 key 衝突、`globals` 規則）對照本專案 `core/config.py` 的 `_deep_merge`／`_substitute` —— 未查。已知本專案有一個 catalog deep-merge 對 type-discriminator 的 bug（memory 記載，workaround＝base 完整定義）。
- C-25～C-28（dataset versioning 的路徑與版本字串規則）對照 `core/versioning.py` —— 未查。
- research §9.5 提醒：「路徑進 catalog、超參數進 parameters」在 Kedro 官方文件裡**查無明文**，若要寫進原則必須標成本 repo 自訂，不得掛 Kedro 名義。
