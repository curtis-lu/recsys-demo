# 手刻 Kedro 風格框架 — core/io 現況盤點（階段 1b）

> 2026-08-03。用途：階段 2「Kedro 官方原則 × 手刻版現況」對照表的左半邊素材。
> 來源：`graphify-out/GRAPH_REPORT.md`（建於 `28a9b0b7`，落後 HEAD `dd61b48` 23 個 commit，
> 但期間 src 只動 `core/consistency.py`／`pipelines/dataset/nodes_spark.py`／`preprocessing/_spark.py`，
> 結構性檔案未動，架構盤點可用）＋ 直接讀 `src/recsys_tfb/core/`、`src/recsys_tfb/io/`。

## A. 有 Kedro 對應物的部分

| 手刻（行數） | Kedro 對應 | 觀察到的形狀差異 |
|---|---|---|
| `core/catalog.py` `DataCatalog`（91） | `kedro.io.DataCatalog` | 型別解析用模組級 dict `_DATASET_REGISTRY`（catalog.py:10-17），非 Kedro 的全限定路徑動態 import；`MemoryDataset` 定義在 catalog.py:21 內而非 io/ |
| `core/node.py` `Node`（**19**） | `kedro.pipeline.node` | 極薄：只有 `func`／`inputs`／`outputs`／`name`。**無 `tags`、無 `namespace`、無 `confirms`** |
| `core/pipeline.py` `Pipeline`（189） | `kedro.pipeline.Pipeline` | 有 `slice_from`/`slice_only`＋**自動擴張上游 producer**，並以 `SlicePlan`（pipeline.py:9）回報 `auto_included`／`skipped`／`skipped_side_effect`。這是 Kedro 沒有的加法 |
| `core/runner.py` `Runner`（181） | `kedro.runner.SequentialRunner` | 只有 sequential 一種。有 last-consumer 驅逐（runner.py:14 `_build_last_consumer_map`），語意近似 Kedro 的 `_release` |
| `core/config.py` `ConfigLoader`（194） | `kedro.config.OmegaConfigLoader` | 自製 `${env.NAME\|default}` resolver（config.py:12）、`_deep_merge`、`_substitute` 參數插值 |
| `io/base.py` `AbstractDataset`（20） | `kedro.io.AbstractDataset` | **公開的 `load`/`save`/`exists` 直接是 `@abstractmethod`**；Kedro 是私有 `_load`/`_save`/`_exists` ＋ 公開 wrapper（wrapper 承載 logging／versioning）。→ 待查：這是否讓本專案沒有地方掛橫切關注點 |

## B. Kedro 沒有對應物的自製抽象（各自需要存在理由）

| 模組 | 行數 | 角色 |
|---|---|---|
| `core/consistency.py` | **1374** | 不變量 predicate 的單一真實來源（A 系列 config-static／B 系列資料閘） |
| `core/versioning.py` | 415 | 三層 hash 版本 ID |
| `core/logging.py` | 303 | `RunContext` ＋ `JsonFormatter` 結構化日誌 |
| `core/schema.py` | 189 | 欄位角色集中定義（`get_schema()`，god node #3，86 edges） |
| `core/safe_eval.py` | 141 | HPO 宣告式搜尋空間的 `ast` 受限求值 |
| `core/group_utils.py` | 81 | ranking／query-group helpers |
| `io/handles.py`、`io/extract.py` | — | `ParquetHandle`（god node #2，**89 edges**） |

**量體對比值得注意**：`consistency.py` 1374 行 vs `node.py` 19 行。本專案的正確性重心不在 node 契約，而在集中式 predicate——這是最大的「刻意偏離」候選。

## C. 明確的原則逃生口：`@dataset` handle 慣例

`runner.py:79-87`：input 名稱以 `@` 開頭時，交給 node 的是 **catalog dataset handle 本身**（`catalog.get_dataset()`），不是載入後的資料，**讓 node 自己呼叫 `.save()`**。

- 全 repo 只有一處使用：`src/recsys_tfb/pipelines/training/pipeline.py:140`
  （`"@training_eval_predictions",  # catalog handle for chunked save`）
- 這是對 Kedro 核心原則「node 不做 I/O、I/O 由 catalog 負責」的**直接例外**。
- 現況：機制層有註解（runner.py:79-81），**原則層沒有明文**——沒有任何文件說「什麼情況下允許用 `@`」。
- 階段 3 待決：原則要寫成「node 不得做 I/O」還是「node 不得做 I/O，除非透過 `@handle` 且滿足條件 X」。
  後者可機械檢查：`grep -rn '"@' src/recsys_tfb/pipelines/` 每個命中都要有對應理由。

## D. 其他觀察

- **Import cycle 一處**：`diagnosis/model/__init__.py → shap_cases.py → shap_per_item.py → __init__.py`（GRAPH_REPORT.md:587）。
- God nodes 前十名有 `Pipeline`(69)、`Node`(60)，但前三名是 `ReportSection`(103)、`ParquetHandle`(89)、`get_schema()`(86)——診斷報表與 schema 的連結度高於框架抽象本身。

## 待查（等階段 1a research 回來才能判定）

1. Kedro 官方對「node 不做 I/O」的明文理由是什麼？`@handle` 這種逃生口 Kedro 有無對應設計（如 `IncrementalDataset`、`PartitionedDataset` 的 lazy save）？
2. Kedro 的 `AbstractDataset` 為何用私有 `_load`/`_save` ＋ 公開 wrapper？本專案攤平成公開 abstractmethod 損失了什麼？
3. Kedro node 的 `tags`/`namespace` 是為了什麼問題存在？本專案沒有它們是否已用別的方式解決（`SlicePlan` 的自動擴張）？
4. Kedro 的 dataset versioning 明文**不涵蓋**什麼？跟本專案 `versioning.py` 的三層 hash 是不是在解不同的問題？
