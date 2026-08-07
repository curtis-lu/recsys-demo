---
status: accepted
date: 2026-08-05
---

# dataset pipeline 依角色切模組：node 講 ML 決策，機制在具名 helper

dataset 一帶已經改過三次（#152 月份計畫走 catalog、#159 preprocessing import 清理、
#162 Layer-2 資料閘搬進 `pipelines/dataset/data_gate.py`），每一次都是對的，但沒有一次
是朝著一個講得出來的形狀走——所以沒有「做完了」這個狀態。本 ADR 定義那個形狀，以及
判斷它有沒有達成的條件。

## 一、現在的模組名字說了謊

盤點於 `b667ece` 現查（不引用既有文件的統計，這一帶最近三次改動已讓舊數字失效過）：

| 檔案 | 行 | 名字宣稱 | 實際 |
|---|---|---|---|
| `pipelines/dataset/nodes_spark.py` | 289 | node 的實作 | 12 個函式裡 **4 個是純轉手**、1 個近乎轉手（`build_test_model_input` 多一行 scope，[ADR-0007](0007-month-plans-travel-through-the-catalog.md) 論證過），合計 79 行；實體在 `preprocessing/_spark.py` |
| `pipelines/dataset/nodes_shared.py` | 104 | 共用的 node | **零個 node**。4 個成員裡只有 `plan_incremental_snap_dates` 的唯一消費者是 `month_plans.py`，其餘三個各有 1–2 個消費者，全部在 dataset 內 |
| `pipelines/dataset/helpers_spark.py` | 208 | 輔助函式 | `select_keys`（`:86`，98 行）裝著整個抽樣設計，而它的 node `select_train_keys` 只有 12 行 |
| `pipelines/dataset/data_gate.py` | 120 | 資料閘 | 它就是一個 node（`:48`），只是不叫 node |
| `preprocessing/_common.py` ＋ `_spark.py` | 566 | 跨 pipeline 共用的前處理 | 真正被兩條以上 pipeline 用的只有 **61 行（10.8%）** |

`_spark` 後綴是**已廢棄的 pandas／Spark 雙軌制的遺跡**。雙軌只剩 Spark 一條（`conf/base/`
零個 `backend` 鍵；dataset 相關 catalog 條目全是 `HiveTableDataset`／`JSONDataset`），
pandas 只在「必須於 driver 處理」時才出現——而 dataset 一帶零個這種情形，`pd.Timestamp`
的全部用途是把設定字串正規化成日期，從不碰資料。同源的殘留還有
`nodes_spark.py:5`／`:7` 兩個死 import（`numpy`、`Window`，AST 掃描零使用）。

**這些不是命名品味問題**：五個名字裡有三個指向不存在的東西，讀者無法用檔名決定該打開哪一個。

## 二、目標形狀

### 兩條判準

1. **node body ＝ 具名步驟的組合，每個步驟名就是一個 ML 決策。** 打開 node 就讀得完這個
   節點對資料做了什麼決定，不必跳檔。
2. **一個 helper 至多承載一個決策。** 兩個以上必須拆成多個具名步驟，由 node body 依序呼叫。

分界線：**決策 ＝ 會改變模型看到的資料的選擇；機制 ＝ 語意定了之後，怎麼在 Spark 上算出來。**
常數與型別細節（未知類別的哨兵值、float32 的 cast 實作）留在 helper——步驟的**名字**負責
說出決策，**值**不必上浮。

判定程序（機械檢查給不了，靠 code review 執行）：把 helper 的名字換成純機械的名字
（`_encode_via_map_literal`、`_bucket_by_crc32`），node 讀起來若仍講得完整個 ML 故事就過；
讀完會問「這一步到底決定了什麼」就是決策漏進 helper 了。

用現況驗這條線：`select_keys` 一個呼叫裡有四個決策（月份過濾／有效抽樣率的覆寫優先序／
依 identity key 決定去留／輸出欄＝identity＋carry），名字一個都沒說 → 非法。
`_compute_feature_columns`（當時的 `preprocessing/_common.py:30`；今日
`steps/feature_columns.py` 的 `compute_feature_columns`）只有一個決策、名字也說出來了 → 合法，
即使它被兩個 node 呼叫。

### 檔案清單

```
pipelines/dataset/
  __init__.py         re-export create_pipeline（不變）
  pipeline.py         接線（不變）
  nodes.py            11 個 node 函式，含 validate_data_consistency ← ML 故事唯一的家
  month_plans.py      月份計畫（吸收 nodes_shared 的三個成員）← 保持零 Spark
  steps/
    sampling.py         抽樣機制：有效抽樣率解析、bucket 過濾
    scoping.py          把 month plan 套到 frame 上（原 _date_filter）
    feature_columns.py  純欄名推導：preprocessing_config ＋ compute_feature_columns
    model_input.py      join 機制、輸出欄組裝、欄位存在守衛
    categoricals.py     類別詞彙表怎麼 fit、未知值怎麼回報
```

**2026-08-07 修訂（形狀微調，不重開本節的論證）**：五個機制模組移入 `steps/` 子套件，
`nodes.py`／`pipeline.py`／`month_plans.py` 留在根層——這樣根層的 `ls` 本身就分得出
「對外契約」與「內部步驟」，不必先讀六個檔名才知道該打開哪一個。

`month_plans.py` 留在根層的判準是**它有 src 側的外部消費者**：`__main__.py:44` 在 pipeline
開跑前就 import 它算好計畫、再注進 catalog（[ADR-0007](0007-month-plans-travel-through-the-catalog.md)
的設計），所以它是這條 pipeline 的對外契約，不是 node 呼叫的步驟。另外五個的呼叫端只有
`nodes.py`。`categoricals.py` 不在原始清單裡——它是票 4 實作時（`3cab5c7`）新生的第六個模組，
本次補進清單。

S1（node 必須 `def` 在 `nodes.py`）與 S2（`month_plans.py` 零 pyspark）的錨點都沒動，
兩個檔都還在根層；只有 S2 可達性檢查的**模組路徑解析**要支援子套件，見第四節。

消失的四個檔：`nodes_spark.py`、`helpers_spark.py`、`nodes_shared.py`、`data_gate.py`。
dataset 一帶零個 `_spark` 後綴。

`month_plans.py` **必須維持零 pyspark import**：它與 `nodes_shared.py` 現有的 451 行測試
（`test_month_plans.py` 160 ＋ `test_nodes_shared.py` 291）完全不需要 SparkSession，而這個
repo 的 Spark cold start 是 2–4 分鐘。所以 `scoping.py` 不能併進去——它回傳 Spark Column。

`preprocessing/` 解散：61 行真正共用的機制（類別編碼、float32 轉型）留在單一模組
`preprocessing.py`；`apply_preprocessor` 併進 `pipelines/inference/nodes_spark.py:129`
既有的 wrapper；feature-selection 那 38 行進 `models/`（`models/lightgbm_adapter.py:38` 也用它，
放 `pipelines/training/` 會製造 `models/` → `pipelines/` 的反向依賴，而 `models/`／`utils/`／
`core/` 目前對 `pipelines/` 是零 import）；其餘全部是 dataset 專用，進 `pipelines/dataset/`。

**`preprocessor.json` 的四鍵契約也留在 `preprocessing.py`。** 解散之後 writer 進
`pipelines/dataset/nodes.py`、reader 進 `pipelines/inference/nodes_spark.py`，而
`feature_columns`／`categorical_columns`／`category_mappings`／`drop_columns` 這組鍵是**真正
跨 pipeline 的東西**——今天它只靠 writer 與 reader 剛好同檔而可見（`_spark.py:214-219` 寫、
`:411-414` 讀），解散後就沒有定義點了。失效模式是 dataset 側改鍵名或加鍵，inference 側到公司
環境才爆（inference e2e 本機難驗，撞既有 #63），或更糟——`dict` 讀取靜默套用舊語意。
用 TypedDict／dataclass，至少 docstring 化。這不改形狀，只是讓那 61 行的模組多一個名正言順的
職責：它本來就該是「fit 端與 apply 端共享語意」的家。

`__main__.py:217-243` 的 `_collect_existing_snap_dates` 改走 catalog：它現在直接讀 catalog
的**原始設定 dict**（`entry.get("type") != "HiveTableDataset"`、`entry["database"]`、
`entry["table"]`），自己知道 HiveTableDataset 長什麼樣、怎麼列分區。三個增量表都有
`partition_filter: {base_dataset_version: ...}`（`conf/base/catalog.yaml:55`／`:77`／`:134`），
所以 `HiveTableDataset.existing_partition_values()`（`io/hive_table_dataset.py:200`）回傳的
就是已按當前版本篩過的答案。`helpers_spark.py:24` 的 `existing_snap_date_partitions` 刪除
——它是同一件事的第二份實作。`__HIVE_DEFAULT_PARTITION__` 的丟棄守衛不能跟著消失，它是個
決策（丟棄＝該月視為未落地、會被重做），移進 `month_plans.py`。

**這個改動有一個順序約束，照字面實作會靜默全量重建。** 現行 `__main__.py:682` 取得
`catalog_config` 時傳的 `runtime_params=params` **還不含** `base_dataset_version`——它在
`:692` 才算出、`:719` 才進 runtime_params。而 `core/config.py:111-115` 的替換是字串
`.replace()`，**未解析的 placeholder 靜默留成字面 `${base_dataset_version}`、不 raise**。
若拿 `:682` 那份 config 建 dataset 物件去問 `existing_partition_values()`，
`io/hive_table_dataset.py:254-257` 會用那個字面字串逐一比對 partition spec、**丟掉全部**，
回傳 `[]` → 每個月都視為未落地 → 全量重建，而且不報錯。現行實作把 `base_v` 當函式參數
顯式傳進去，正是繞開了這件事。

所以實作必須在 `base_v` 算出之後，用**含 `base_dataset_version` 的 substitution params**
重建（或延後建立）catalog 物件才問它。這個坑不會被票 2 的其他驗收條件抓到——`conf/` diff
是空的、測試是綠的、DAG 沒變，只有 `[months]` log 會顯示 processed=全部而沒有人斷言它。
驗收要另加一條：**對已落地的表實測 `plan.skipped` 非空**。

### 底線前綴的判準

`_` 前綴 ＝ **只有本模組呼叫**。`nodes.py`（或任何跨模組的呼叫端）呼叫得到的一律無底線
——底線在 Python 的意思是「模組外不要用」，而 node body 逐行呼叫它就是模組外在用。這是
第一節那個病（名字指向不存在的東西）在函式名這一層的同一種形態。

**2026-08-07 清掉五個 pre-#170 的殘留**：

| 舊名 | 新名 | 為什麼 |
|---|---|---|
| `_get_preprocessing_config` | `prepare_model_input_config` | **不只去底線**：舊名指向一個不存在的設定鍵。它讀的是 `dataset.prepare_model_input`（`steps/feature_columns.py:31`，區域變數就叫 `pmi_config`），config 裡沒有 `preprocessing` 這個鍵 |
| `_compute_feature_columns` | `compute_feature_columns` | 純去底線（不縮成 `feature_columns`，那會與模組名相撞） |
| `_warn_missing_drop_columns` | `warn_missing_drop_columns` | 純去底線，接上 `warn_unknown_encodings` |
| `_validate_columns` | `require_columns_present` | 接上既有的 `require_*` 家族（`require_months_present`／`require_base_key_columns`／`require_item_is_a_feature`／`require_declared_categoricals`） |
| `_date_filter` | `months_filter_as_date` | 見下段 |

這五個的底線記錄的是**出生年份而不是可見性**：`git log -S` 逐一核過，它們全部誕生於 #170
之前（`55d3ee4`／`bf191eb`／`1d3f740`），而 #170 新生的名字（`require_base_key_columns`、
`model_input_columns`、`keep_rows_drawn_under_ratio`、`restrict_to_months`）一個底線都沒有。
兩個競爭解釋都被現況否證：「底線＝模組私有」對不上（八個底線名裡五個被 `nodes.py` 跨模組
import），「底線＝守衛或實作細節」也對不上（`_validate_columns` 與 `require_base_key_columns`
是同一物種，一個掛底線一個沒有）。留著底線的三個名副其實——`_ratio_lookup_df`、
`_test_snap_dates`、`_fmt` 的唯一呼叫者是自己的模組。

`months_filter_as_date` 的 `as_date` 是承重的，不是裝飾：它與同檔 `restrict_to_months` 的差別
**不是**「要不要過濾月份」，而是有沒有把兩邊正規化成 DATE。來源表的時間欄是真 DATE，正規化
會靜默放寬命中範圍；從 Hive 分區欄讀回來是字串，不正規化則靜默零命中（`steps/scoping.py` 的
docstring 論證過這兩個方向）。名字若丟掉這個區別，兩個函式在呼叫端看起來就可以互換，而選錯
一邊的兩種後果都是靜默的。

**上一段的一處更正（fresh-context 審查抓到）**：「來源表 vs Hive 分區欄」說的是兩個失效方向
為何都靜默，**不是呼叫點的分界線**。`feature_table` 今天同時出現在兩側——`nodes.py:464` 用
不正規化的 `restrict_to_months`、`nodes.py:558` 用正規化的 `months_filter_as_date`，而
`conf/base/catalog.yaml:11-15` 的 `feature_table` 沒有 `partition_cols`，時間欄是真 DATE。
所以正規化與否是**逐呼叫點**的選擇，不是逐表的；那個不一致早於本次改動，而 rename 不改行為
也就沒有動它。要帶 `as_date` 的結論不變：兩個函式真的只差正規化，兩種選錯都不會有錯誤訊息。

## 三、驗證邏輯的歸屬

規則由「它需要看到什麼」決定——這大部分是把 `core/consistency.py` legend 既有的制度講清楚，
不是新規則：

| 需要看到 | 家 |
|---|---|
| 只有 config | Layer-1 A 系列（`core/consistency.py`，CLI entry 執行） |
| 來源表的 metadata 或廉價 distinct | Layer-2 B 系列（資料閘，零掃描，[ADR-0006](0006-data-quality-checks-belong-upstream.md)） |
| node 執行期才存在的中間資料（抽樣後的 keys、join 後的欄位） | 留在 node，且必須在 docstring 標明是**前置檢查**或**後置條件** |

兩個詞不可混用：檢查**輸入**（feature_table 有沒有這些月份、這些欄）是前置檢查；檢查
**自己算出來的結果**（切完的 dev 是不是空的、join 完欄位齊不齊）是後置條件。前者失敗表示
上游沒準備好，後者失敗表示這個 node 的邏輯或設定有問題——兩種錯誤要找的人不同。

legend 已經有「runtime backstop」這個登記過的模式（A2 的 `_spark.py:202` item guard、
A3 的 `:167` identity-cat guard、B6 在 `io/extract.py` 的第二個掛點），所以「node 裡有 raise」
本來就不等於違規——**違規的是沒登記**。

照這條規則重掃，唯一無家可歸的是 `validate_date_splits`（`nodes_shared.py:11`）：它是純 config
判斷（四個 split 的日期互斥），legend 零登記，卻藏在 `select_train_keys` 裡當副作用。
**predicate 移進 `core/consistency.py` 成為 A24**（A23 已由 issue #158 佔用；A16/A17/A18 已退休
且刻意不回填）。

**但 A24 不進 `validate_config_consistency` 這個全域 aggregator，而是接在 `dataset` 指令上**
（`_load_config_and_setup` 之後、Spark 啟動之前）。這條照 issue #158 已裁決的先例：那張票實測
把 dataset-only 的 predicate 收進 aggregator **擋掉 9 個既有測試**，因為 aggregator 在**每個**
CLI 指令的 entry 都跑（`__main__.py:102`），而只有 dataset 讀這些鍵。A21／A22 也是因為
「aggregator 看不到需要的 context」而各自接在指令上。

日期互斥的情況比 #158 溫和——`validate_date_splits` 四個鍵全用 `.get(..., [])`，沒有 dataset
區塊的 config 會得到四個空集合、判定互斥、放行，不會擋掉 `feature_etl`。但**接法要一致**：
同一類「只有一條 pipeline 讀的設定不變量」不該有兩種接法，否則下一個人得逐條查才知道自己
該接哪裡。

`_spark.py` 的三筆（`:152` 缺 train 月份、`:266` 缺 base-key 欄、`:281` 缺 to_process 月份）
維持在 node，補標為**前置檢查**（三者查的都是 `feature_table` 這個輸入）。不前移到 Layer-2
的理由：`:281` 檢查的是**這次 run 的 to_process 月份**，前移等於讓「設定 vs 資料」的閘門
依賴執行計畫。[ADR-0005](0005-model-input-degenerate-state-contracts.md) 的兩筆
（`split_train_keys` 的空 dev、`build_model_input` 的欄位守衛）則是後置條件。

A24 落地時順帶把比對正規化：現行 `validate_date_splits` 用 `str(d)` 比字串
（`nodes_shared.py:15-18`），`"2026-01-31"` 與 `"2026-1-31"` 這種同日不同寫法抓不到。改成
先轉 `pd.Timestamp` 再比。**這是刻意的行為收緊，不是純搬移**——原本放行的設定可能開始
raise，屬於票 1 的預期效果，不進版本 hash。

## 四、可機械檢查的，與不可檢查的

新增兩條結構約束，登記在 `docs/agents/architecture-constraints.md` 節二：

> **S1** — `pipelines/dataset/pipeline.py` 中每個 `Node(...)` 的第一參數，必須是
> `pipelines/dataset/nodes.py` 裡以 `def` **定義**的名稱。檢查：AST 取 `nodes.py` 的
> `FunctionDef` 名稱集合，比對 `pipeline.py` 中所有 `Node(...)` 第一參數。
>
> **S2** — `pipelines/dataset/month_plans.py` 不得 import pyspark。檢查兩條，缺一不可：
> ① AST 掃該模組的 `Import`／`ImportFrom`（含函式體內的延遲 import）；② 沿 dataset 套件內的
> import 遞迴一跳以上，任何路徑都不得抵達 pyspark。條文與失效理由見
> `architecture-constraints.md` 的 S2 節。

S2 守的不是風格，是**檔案切分的承重前提**：第二節說 `month_plans.py` 必須維持零 pyspark，
而那正是 `scoping.py` 不能併進去的唯一理由，也是 451 行免 Spark 測試存活的條件。一句沒有
機制強制的「必須」會漂移——這份 ADR 否決「靠 docstring 說明 ML 決策」用的就是這個論證
（見下方「考慮過但否決的選項」與 ADR-0002:67 那個三天就過時的補釘），同一把尺必須量到自己。
成本趨近零：`tests/test_core/test_architecture_constraints.py` 已有 291 行 AST 稽核基建、
12 個測試跑約 0.5 秒，加一條 import 掃描沒有假陽性面。

**2026-08-07：`steps/` 子套件在 S2 的可達性檢查上開了一個必須同時修掉的洞。** 該檢查沿
`pipelines/dataset/` 的同層 import 遞迴，把模組名解析成「同層目錄 ／ 名字 `.py`」。搬進子套件
之後，`recsys_tfb.pipelines.dataset.steps.scoping` 這種帶點的路徑會被解析成 `dataset/steps.py`
——那個檔不存在，遞迴回傳「查無」，於是**檢查靜默放行**。而失效方向正是它要守的那一個：
`month_plans.py` 哪天 import 了 `steps/` 裡任何 Spark-typed 模組，兩條檢查都不會紅。所以解析
必須按 `.` 展開成子路徑，並補一個「解析器真的跨得進子套件」的測試——否則 S2 的第二條會退化成
結構上不可能紅的裝飾品，而搬檔這個動作本身就是製造它的原因。

**「必須 def 定義」而不是「必須從 nodes.py import」**，因為後者有 re-export 漏洞：
`nodes.py` 寫一行 `from .sampling import select_keys`，`pipeline.py` 照樣「來自 nodes.py」，
檢查全綠，而函式定義在別的檔——正是本次要消滅的形狀。

前綴用 `S`（structure）而不是接續 `A8`，因為 `architecture-constraints.md` 的 A1–A7 與
`core/consistency.py` 的 A1–A22 **已經在撞車**（兩邊都有 A5、A7，意思完全不同）。本次不
重編號，只在該檔加一句說明兩套編號的差別。

**S1 與 issue #163 的關係**：#163 記錄了同一個問題的 repo-wide 版本——A1 稽核用檔名
（`nodes*.py`）當「模組裡有沒有已註冊 node」的代理，實測 8 個假陰性（含 `data_gate.py` 與
更早的 `evaluation/comparison_nodes.py`）＋`nodes_shared.py` 這個零 node 的假陽性。#163 的
未決 Q1 是「代理換成什麼」：runtime import 精確但要 import 整棵 pipeline 樹，AST 追 import 鏈
則要處理 re-export 與別名。**S1 繞開了這題**——它不回推函式定義在哪，而是**要求**定義必須在
`nodes.py`，於是檢查退化成兩行 AST、零 import 追蹤。dataset 這一版做完就是 #163 全域修法的
可行性範本。代價：短期內 A1 的檔名代理與 S1 的 def 比對並存。

**S1 只擋位置，擋不住內容。** 一個 12 行的轉手 node 加一個四決策的 helper 完全滿足 S1
——那正是現況 `select_train_keys` → `select_keys` 的形狀。所以第二節的判定程序必須被
**動 node 的人真的看到**：`architecture-constraints.md` 是 `CLAUDE.md` 路由表規定「新增或
修改 node、catalog 條目之前先讀」的檔案，判準只活在 ADR 裡等於對執行者不可見。在該檔加一行
路由指向本 ADR 第二節。

**第二節的兩條判準寫不成測試**，這點必須誠實標明：「決策 vs 機制」與「一個 helper 至多
一個決策」都是判斷題。硬要機械化只會得到假指標——行數是最常見的那個，而
`architecture-constraints.md` F8 自己就寫過「60 行可以混五種職責，130 行也可能只是一段長而
平的轉換」。唯一的執行手段是上面那個判定程序 ＋ code review。

## 五、這與 kedro 的關係

本框架是手刻的 kedro 風格實作，所以要說清楚哪些是對齊、哪些是本 repo 自訂
（出處：`docs/notes/2026-08-03-kedro-official-design-rationale.md`，基準 kedro 1.5.0）：

- **對齊**：票 2 把儲存知識收回 catalog，就是 C-3（「檔案路徑與儲存邏輯不得散落在
  codebase 各處」）。現況 `__main__.py` 自己知道 HiveTableDataset 有 `database`／`table`
  欄、知道怎麼列分區，正是那句要擋的。
- **本 repo 自訂，不掛 kedro 名義**：第二節的兩條判準。kedro 對 node 大小只有一句
  「small single responsibility functions」（§2.1），而且**明文沒有**行數或複雜度上限
  （§2.4）。gap-table 警告過不得把本 repo 的規則冒掛 kedro，所以這裡標明：判準是我們自己
  的可操作化，kedro 只提供了方向。
- **不受本次影響的既有偏離**：本框架的正確性重心在集中式 predicate（`core/consistency.py`
  1374 行）而非 node 契約（`core/node.py` 19 行），見 `architecture-constraints.md` F7。
  本 ADR **沒有**把重心搬回 node——第三節的歸屬規則反而是強化 predicate 那一側。
- **沒有把 kedro 明文不要求的東西當成要求**：§2.3／§2.4 列出 kedro 對 node 寫檔、log、
  行數都沒有規定。本 ADR 也沒有據此立規則。

## 考慮過但否決的選項

**node 保持薄轉手，靠 docstring 說明 ML 決策。** 現有的轉手 node 就是這個做法的樣板實作
（`nodes_spark.py:237` 用 14 行 docstring 講清楚 [ADR-0002](0002-preprocessed-feature-table-incremental.md)
為何跳過既有月份是安全的），品質
不差。否決的理由不是它做得不好，是**散文描述「邏輯住在哪」會過時而沒有任何機制逼人發現**：
`docs/adr/0002-preprocessed-feature-table-incremental.md:67` 就有一段補釘——原文寫的「差集邏輯
集中在 `nodes_shared.py` 的單一 helper，由四個 node 共用」在三天內被
[ADR-0007](0007-month-plans-travel-through-the-catalog.md) 改掉，散文留在
原地。用可執行的呼叫描述決策，改了 diff 就在同一處。

**以 ML 關注點為主軸切檔**（`sampling.py`／`preprocessor.py`／`model_input.py` 各自含 node ＋
機制）。它的好處是真的：改一個關注點只碰一個檔。否決是因為那樣就沒有「node 的家」這種東西，
「ML 業務邏輯在 node 裡看得出來」會退化成「在每個檔案的前半段」，而 node 散落正是本次要修的
症狀之一。代價明說：角色軸換來的是「改一個關注點要碰兩個檔」。

**`preprocessing/` 保留為「preprocessor 這個產物」的家。** 立論是 `preprocessor.json`
（`conf/base/catalog.yaml:83`）跨 pipeline，所以它的程式該有共用的家。否決的理由是
**產物跨 pipeline 不代表程式碼跨 pipeline**：dataset 決定用什麼資料 fit 它，inference 決定
把它套到什麼上（`pipelines/inference/pipeline.py:26` 把 `preprocessor` 當 catalog input 收下，
`preprocessing/_spark.py:411-414` 四個值全從已落地的產物讀，不碰 `parameters`）。真正被兩邊
共用的只有 61 行編碼機制。保留會讓它繼續裝 400+ 行單一消費者的碼——正是 `nodes_shared.py`
那個病。

**把共用的 61 行放 `utils/`。** 會讓「未知類別 → 哨兵值」這個 ML 決策沉進通用工具層。留在
`preprocessing` 是讓它保有領域名稱，而且 61 行的模組名終於與內容相符。

**加一條「檔名不得含 `_spark`／`helpers`／`shared`」的守衛。** 否決：這些命名的根因是已經
廢棄的雙軌制，那個原因消失了不會再生；而「把 node 放到別的檔」這種**位置**的復發已由 S1
擋住。（**內容**的復發——決策漏進 helper——S1 與檔名守衛都擋不住，那條靠第四節的判定程序，
這是本 ADR 已知的最大殘留風險。）加一條抓不到真問題的檢查，只會製造「測試綠＝沒問題」的錯覺。

**把「一個 helper 至多一個決策」放寬成「兩三個緊密相關的決策可以打包」。** 放寬之後
`select_keys` 現況就合規，這條判準就擋不住它要擋的東西。

## 後果

**版本中性，這是整套改動最重要的性質。** `compute_base_dataset_version` 的 hash payload 是
`{"dataset": 去掉抽樣／覆蓋鍵的 params, "schema": schema}` ＋ 選配的
`feature_table_fingerprint`（`core/versioning.py:112-120`）——純設定，零程式碼輸入；
`model_version` 同理只吃 `training:` 區塊。所以**不翻版本、不重建 dataset、不重訓**。
這讓「行為不變」可以當硬驗收條件：`conf/` 的 `git diff` 為空，就是版本中性最便宜的證明。

**node 函式會變長，這是目標形狀的必然結果不是副作用。** 決策上浮後 `nodes.py` 估計 500 行
左右（估計值，非量測），落在 repo 常態內（`evaluation/nodes_spark.py` 614、
`inference/nodes_spark.py` 401、`training/nodes.py` 1443），但單一 node 函式會明顯超過
`architecture-constraints.md` F8 記錄的「七成在 40 行內」。**不要把它「修好」。**

這句免責有邊界：**變長的授權只涵蓋「具名步驟組合」造成的長，不涵蓋機制內聯造成的長。**
一個 60 行、由 8 個具名步驟組成的 node 是目標形狀；一個 60 行、把 Spark 表達式攤平在
函式體裡的 node 不是，它只是把 helper 貼進來而已。

**純／Spark 的界線從檔案層降到函式層。** 快測迴路不再靠檔名維持，靠「決策是純函式」維持。
**唯一還靠檔案切分維持的是月份計畫的純度，而那條由 S2 釘住。**

**dataset 會暫時與其他 pipeline 不對稱。** `evaluation/nodes_spark.py`、
`inference/nodes_spark.py` 是同一個雙軌制遺跡，但不在本次範圍。看到 dataset 沒有 `_spark`
而別人有時，那不是 dataset 的例外，是別人還沒改。

**切成四張票，順序有理由：**

1. `validate_date_splits` → A24，接在 `dataset` 指令上。**先做**，因為它最小、零依賴，且它
   決定了第三節那條歸屬規則到底怎麼落地——後面三張票都在同一份 legend 與同一個指令入口上
   疊東西，這條先釘好，後面不必回頭改。（初版把它排第一的理由是「唯一會集體弄紅測試」；
   改成接在 dataset 指令上之後那個風險消失了，見第三節。）
2. `__main__.py` 改走 catalog、刪 `existing_snap_date_partitions`。獨立。
3. `preprocessing/` 解散。純機械搬移，不改形狀。
4. dataset 內部重整成上面七檔 ＋ S1／S2。**依賴票 3**（碼要先搬進來才能上浮決策），也是唯一
   需要判斷力的一張。不再細切：票 4 內部是同一個判準的重複套用，切成三張只會多兩輪
   fresh-context 審查、不降低風險。

**每張票的驗收條件**：`conf/` 的 `git diff` 為空；catalog 條目名、`Node(name=...)`、
pipeline DAG 全部不變（`--only-node` 的可定址性是既有介面，見
`docs/operations/pipeline-slicing.md`）；相關測試綠且 main 既有 fail 先建 baseline。
票 4 另加：跑一次本機 dataset pipeline，**各 split 的 row count 與 manifest 的
processed／skipped 月份清單，與重構前的 baseline 逐字相同**。

刻意**不用**「`base_dataset_version` 逐字相同」當驗收：版本 ID 是純 config 的函數，而同一份
驗收已經要求 `conf/` 的 diff 為空——ID 相同是被前一條保證的，把程式碼改壞它照樣相同。
那是一個結構上不可能失敗的斷言，唯一的資訊量是「pipeline 跑完了」。驗收要斷言**內容**。

## 這條 ADR 沒有解決的事

- **`evaluation/` 與 `inference/` 的同源遺跡**：`_spark` 後綴、以及 inference 的 node 不照
  第二節的判準重寫（票 3 只搬 `apply_preprocessor` 的位置，不改它的形狀）。
- **`build_test_model_input` 與 `build_model_input` 是否該合一**：ADR-0007 已論證維持分開
  （test_keys 是持久化 Hive 表、讀回來會拿到全部歷史），本次不重開。
- **`split_train_keys` 的空 dev 守衛與 `build_model_input` 的欄位守衛**：
  [ADR-0005](0005-model-input-degenerate-state-contracts.md) 已論證是
  node 後置條件，本次只補 docstring 標記，不搬家。
- **兩套 A 系列編號的撞車**：`architecture-constraints.md` A1–A7 與 `core/consistency.py`
  A1–A22。本次只加一句說明，不重編號——重編號會讓既有文件的引用全部指錯，理由同
  A16/A17/A18 退休不回填。
- **inference 無法自我檢查「載入的 preprocessor 是否與現行 config 相符」**：`compute_feature_columns`
  搬進 `pipelines/dataset/` 之後，「什麼算特徵」這條規則只在 dataset 裡。這不是本次弄丟的
  能力——inference 現在的把關本來就是 artifact 對 artifact（model 的 `feature_names()` vs
  `X_score` 欄位，`pipelines/inference/nodes_spark.py:151-168`），不是 artifact 對 config。
- **第二節的兩條判準沒有機械檢查**，見第四節。這是知情的缺口，不是疏漏。
- **`recsys_tfb.preprocessing` 那兩個底線函式**（`_cast_feature_floats_to_float32`、
  `_encode_categoricals`）：依第二節的底線判準它們也該去底線——被 dataset 與 inference
  兩條 pipeline import，比 dataset 內那五個更明顯。不在本次範圍，因為它不在
  `pipelines/dataset/` 內，而該模組的改動會同時碰到 inference（本機難驗，撞既有 #63）。
- **`feature_table` 同時被正規化與不正規化的月份過濾**（`nodes.py:464` vs `:558`）：兩個呼叫點
  對同一個 catalog 條目做了不同選擇。這個不一致早於本次改動，rename 不改行為也就沒有動它；
  第二節的命名論證已更正為「正規化與否是逐呼叫點的選擇」，但**哪一邊才對還沒有人判定**。
- **底線判準沒有機械檢查**：可以寫（AST 掃 `steps/` 下被外部 import 的名字有無 `_` 開頭），
  刻意不寫——這次的目的是目錄可讀性，加稽核會讓一個純結構搬移長出新的約束面。復發成本低：
  下一次有人加底線名字給 `nodes.py` 呼叫時，判準就寫在同一節裡。
