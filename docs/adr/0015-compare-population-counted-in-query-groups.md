---
status: accepted
date: 2026-09-01
---

# 比較報表的母體大小改成數 query group，與 mAP 同一個分母

模型 A/B 比較報表（`report_comparison.html`）的第一個區塊是 coverage：兩邊各有多大的母體、裁到共同範圍後剩多少。這個數字原本叫 `n_cust`，數的是 `schema.entity` **第一欄**的相異值。

同一份報表接下來每一個 mAP，都是以 **query group**（`schema.time` × `schema.entity` 全部的欄）為單位算的。兩個數字放在同一頁、沒有任何標示說它們不同單位。

## 決定

**coverage 的母體大小改成數 distinct query group**，並把輸出鍵名從寫死「客戶」改成 schema 角色名：

| 舊鍵 | 新鍵 | 數的是什麼 |
|---|---|---|
| `n_cust_A_full` / `n_cust_B_full` | `n_query_group_A_full` / `n_query_group_B_full` | `[time] + entity` 的相異組合數 |
| `n_cust_common` | `n_query_group_common` | 上述兩個集合的交集大小 |
| `n_prod_A_full` / `n_prod_B_full` / `n_prod_common` | `n_item_A_full` / `n_item_B_full` / `n_item_common` | 相異 `item` 數（意義未變，只去掉行業專屬名稱） |
| `dropped_prods_A` / `dropped_prods_B` | `dropped_items_A` / `dropped_items_B` | 被剔除的 item 清單（意義未變） |

實作在 `pipelines/evaluation/comparison_nodes.py::restrict_to_common`，渲染在 `evaluation/comparison/report.py::_build_coverage_section`。

## 為什麼是 query group，不是 entity 組合

`schema.entity` 是一個欄名清單，一筆排序請求的擁有者由這些欄**共同**構成。所以「一個 entity 有幾個」與「有幾筆排序請求」是兩個不同的量：同一個 entity 跨多個 `time` 就是多筆請求。

舉一份具體的資料：`entity` 設兩欄 `[e1, e2]`，資料有 2 個 `time` 值、`e1` 有 3 個相異值、每個 `e1` 底下 `e2` 有 2 個相異值。也就是 6 個 entity、12 個 query group。三種可能的單位差很多：

```
只數 entity 第一欄（e1）        → 3     ← 舊行為
數 entity 全部欄的組合          → 6
數 query group（time × entity） → 12    ← 新行為，也是 mAP 的分母
```

選 query group 的理由只有一個，但夠了：**整份報表都在講 mAP，母體大小必須與 mAP 的分母同單位**，否則讀者會把兩個不同尺度的數字放在一起解讀，而報表沒給他任何線索察覺這件事。

單欄 `entity` 且單一 `time` 的設定下，舊數字與新數字仍會不同（舊的沒有乘上時間維度）。這是刻意的：讓數字對齊指標，比讓數字不動重要。

## 這是既有欄位的定義變更，不是內部重構

同一份報表在這個改動前後跑同一批資料，coverage 欄位的**數字會跳**。這是預期的，本 ADR 就是為了讓半年後看到跳值的人查得到原因。

三件相關的事實：

- **不需要重跑任何 pipeline。** 報表數字不落地成版本化產物，重跑 evaluation 就是新的。
- **版本 ID 一個都不翻。** `core/versioning.py` 只雜湊設定的「值」，不雜湊程式碼；本次改動沒有動 `conf/` 一個字。
- **`n_item` 系列的數字不變**，只有鍵名與顯示名改了。

## 順帶修掉的：排名分組

同一次改動把 `evaluation/comparison/restrict.py::restrict_to_common` 的重排分組從 `[time, entity[0]]` 改成 `[time] + entity`。裁到共同範圍後候選集變小，兩邊都要重排；分組若只取第一欄，同一個第一欄底下的多個 entity 會被併成**一個** query 一起排名，算出來的 mAP 與 `compute_test_mAP_spark` 算的不是同一個量。

這一項與母體大小是同一個根因（把「一組共同構成身分的欄」讀成「第一欄是身分」），但它沒有取捨可談——那裡沒有「刻意用較粗的欄」的正當情境，只有錯，所以不設定可調。它記在這裡，是因為讀者查母體大小為什麼變的時候，多半也在查指標為什麼變。

## 考慮過但否決的選項

- **維持 `n_cust`、另外加一欄 `n_query_group`。** 讀者會看到兩個母體大小欄，仍然得自己判斷哪一個對得上 mAP，等於把問題原封不動交給讀者。而且 `n_cust` 那一欄在多欄 entity 下本來就是錯的（數的是第一欄），保留它等於保留一個沒有正確用途的數字。
- **改成數 entity 組合（不乘時間）。** 比舊行為好，但仍與 mAP 分母差一個時間維度——同一個誤讀風險換個量級再出現一次。
- **把鍵名留著、只改語意。** `n_cust` 裝的已經不是客戶數，名字繼續叫 cust 是在主動誤導；而且這個 repo 是通用排序框架，銀行產品推薦只是示例 instantiation，欄位名不該寫死某個行業的名詞。

## 後果

- 讀舊報表與新報表的同一個欄位會看到不同數字，且欄名也換了；本 ADR 是那個落差的唯一解釋處。
- `n_query_group_common` 用的是「A 的 query group 集合 ∩ B 的」。裁切本身只依 entity 與 item 交集（不依 time），所以在兩邊 `time` 範圍不同的情況下，這個數字會小於實際被評分的 group 數。compare 模式目前只跑單一 `evaluation.snap_date`，兩者等價；未來若 compare 擴到多月份，這裡要重新想。
- 這份 coverage dict 是 `restrict_to_common` → `generate_comparison_report` 之間的內部介面，沒有外部消費者，所以改鍵名不需要相容期。
