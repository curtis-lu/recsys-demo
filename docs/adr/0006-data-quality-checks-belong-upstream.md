---
status: accepted
date: 2026-08-02
---

# 資料品質檢查歸上游 `source_etl`，dataset 閘門維持零掃描

兩個候選檢查，兩個不同的歸屬。未來讀者會問「為什麼 `feature_table` 的 PK 檢查在 ETL 設定裡，
而 B1/B5/B6 在 dataset 閘門裡」——這條 ADR 回答它。

## `feature_table` 的 `(time, entity)` 唯一性 → 補在上游

先講事實：**目前沒有任何地方驗證它。**

| ETL 設定檔 | `primary_key` | `quality_checks: max_duplicate_key_ratio` |
|---|---|---|
| `parameters_sample_pool_etl.yaml` | ✅ | ✅ `0.0`（`:22`） |
| `parameters_label_etl.yaml`（含 `label_table`） | ✅ | ✅ `0.0`（4 張表各一） |
| `parameters_feature_etl.yaml`（**全 6 張含 `feature_table`**） | ✅ | ❌ **沒有 `quality_checks` 區塊** |

而 `pipelines/source_etl/checks.py:343` 的條件是
`if "max_duplicate_key_ratio" in qc and table_config.primary_key`
——**光宣告 `primary_key` 不會跑任何檢查**。

所以 `pipelines/dataset/helpers_spark.py:134-135` 那句「PK 由 `source_etl` 的
`max_duplicate_key_ratio` 保證」對 `sample_pool` 為真、**對 `feature_table` 為假**。
`feature_table` 若有重複 `(snap_date, cust_id)`，`build_model_input` 的 feature join
會靜默把 model_input 列數乘開。

→ **在 `parameters_feature_etl.yaml` 的 6 張表補
`quality_checks: {max_duplicate_key_ratio: 0.0}`**，不在 Layer-2 加閘。

理由：修的是真的洞、用既有機制、在資料還熱的 ETL 時點付一次成本（**每月一次**，而不是
每次 dataset run 一次），而且讓上面那句註解變成真的。

## query group 完整性 → 不設閘，只加測試

「每個 `(time, entity)` 的 distinct item 數 == 宣告的 item 數」在**這個 repo 裡是結構保證**的：

- `conf/sql/etl/sample_pool/sample_pool.sql` 的 `cross_pop` 是字面 cross join
  （`cust_snap LEFT JOIN prod ON 1=1`）
- `select_test_keys` 取全母體、不抽樣（`pipelines/dataset/nodes_spark.py:171`）
- `filter_groups_with_positives` 只丟整組、不丟組內 item
  （`pipelines/dataset/nodes_spark.py:285-299`）

所以「mAP 的分母不可信」這個風險在現行部署下不存在，加閘是為一個結構上不會發生的狀況付
全掃成本。

**現在真的破著的是另一件事**：`filter_groups_with_positives` 的群鍵定義**零覆蓋**——
把群鍵改成含 item（過濾退化成「只留正例列」，會靜默刪光所有負樣本）仍然 16 passed。
這是 code regression 風險，用測試守得住，零 production 成本。

→ 只加測試（D18/D19），重點是鎖住群鍵定義；閘門等真的遇到非 cross-join 的 `sample_pool`
部署再說。

## 為什麼不順手都加成 Layer-2 閘門

`validate_data_consistency` 現有三條全部是**零掃描**：B5/B6 讀 `feature_table.dtypes`
（metastore metadata），B1 只 collect distinct item 值。加一條 `groupBy` 全掃會改變這個
節點的成本量級，而公司規模下的成本未知。

[ADR-0004](0004-carry-drop-columns-intersection.md) 的 B7 刻意也選在零掃描這一類——
**這個閘門的定位是「設定與資料的矛盾」，不是資料品質稽核**。資料品質有它自己的家
（`source_etl` 的 `quality_checks`）。

## 這條 ADR 沒有解決的事

- 框架允許使用者自備 `feature_table`（不經本 repo 的 `source_etl`）。那種部署下上游 PK
  檢查不存在，`feature_table` 的唯一性再度無人保證。若日後真的出現這種部署，再考慮
  Layer-2 補位。
- 非 cross-join 的 `sample_pool` 部署會讓 group 完整性失去結構保證。同上，出現再說。
- 補上 `max_duplicate_key_ratio` 之後，`feature_table` 的重複鍵會在 ETL 階段 raise。
  這個檢查從未在生產跑過，**首次啟用可能揭露既有的資料問題**，不該在沒有人看著的排程裡首跑。
