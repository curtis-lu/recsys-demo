---
status: accepted
date: 2026-08-08
---

# 寫入後的分區回報改問 metastore，不問 DataFrame

`HiveTableDataset.save()` 寫完之後會印一行「寫了哪些分區」。原本的取得方式是回頭問剛寫出去的
那個 `df`：

```python
df.write.mode(self._write_mode).insertInto(self._qualified_name)
written = df.select(*part_cols).distinct().collect()      # ← 這一行
```

未來讀者會問「為什麼不直接問 `df`？那不是一行的事嗎」——這條 ADR 回答它。

## 那一行的真正成本

`df` 不是剛寫出去的資料，是一份**怎麼算出那份資料的計畫**。`insertInto` 執行了計畫，但沒有把
結果留在 `df` 裡，save 路徑全域也沒有任何 `cache()` / `persist()`。所以第二句會建一個全新的
QueryExecution，從頭再跑一次整條 lineage。

實測（PySpark 3.3.2、AQE on、忠實複製 `build_model_input` 的兩個 LEFT join ＋
`drop_groups_without_positives` 的 Window）：

| | jobs | tasks | wall clock |
|---|---|---|---|
| `insertInto` | 3 | 11 | 9.67s |
| 事後那行 log | **4** | **17** | 3.25s |
| 改列目錄 | 0 | 0 | 0.1ms |

`df.select(part_cols).distinct()` 的實體計畫裡，兩個 `SortMergeJoin`、`Window`、三個
`Scan parquet` 原封不動全在。欄位裁剪只砍到剩 partition 欄，**砍不掉 join**——Spark 3.3
無法證明右表 join key 唯一、join 不改變列數重複度，所以消不掉。

控制組排除了「它是不是佔了寫入的便宜」：同一個 df 在**完全沒寫過**的 session 裡跑同一句是
5.94s（冷）／3.38s（暖），與寫入後那次的 3.25s 同一量級。**沒有 shuffle 重用，它是一次完整
獨立的重跑。**

影響面不只 dataset：這段在 `io/`，`conf/base/catalog.yaml` 有 17 個可寫且帶 `partition_cols`
的條目、**沒有一個覆寫 `write_mode`**（全吃預設 `"overwrite"`），所以條件恆真。其中 12 個屬
dataset pipeline，含最貴的三個 `model_input`。

## 決定：**有 `partition_filter` 的表**，寫入前後各拍一次 metastore 快照

```python
before = {_partition_key(p) for p in self.existing_partition_values()}
df.write.mode(self._write_mode).insertInto(self._qualified_name)
after  = self.existing_partition_values()
new    = [p for p in after if _partition_key(p) not in before]
```

`existing_partition_values()`（同 class，本來就存在、predict node 已在用）走
`SHOW PARTITIONS`：純 metastore，成本與表大小無關。

**「有 `partition_filter`」是這個做法能成立的條件，不是巧合。** 它的過濾條件是
`any(spec.get(k) != v for k, v in self._partition_filter.items())`——空 dict 時
`any([])` 恆為 `False`，**一筆都不濾掉**，回傳的是整張表跨所有 run 累積的全部分區。
那種情況下 before／after 差集分不出「這次覆寫的」與「早就在那、這次沒動的」，`new` 在
重新發布時為空、`partition_count` 也與這次寫入無關。

17 個可寫且帶 `partition_cols` 的條目裡，**14 個有 `partition_filter`**，走上面這條路：

| `partition_filter` 的鍵 | 條目數 |
|---|---|
| `base_dataset_version` | 5 |
| `base_dataset_version` ＋ `train_variant_id` | 5 |
| `base_dataset_version` ＋ `calibration_variant_id` | 2 |
| `model_version` | 2 |

剩下 **3 個沒有**——
`score_table`、`ranked_staging`、`ranked_predictions`，全在 inference／publish 路徑上——
維持原本問 DataFrame 的做法，輸出與改動前逐字相同。理由與後續見
[#179](https://github.com/curtis-lu/recsys-demo/issues/179)。

## 代價：不再逐位精確，而且盲點打在哪要說清楚

以下只談走 metastore 快照的那 14 個條目。

| 節點類型 | 每次寫入的分區 | `new` 是否精確 |
|---|---|---|
| 有 month plan（`preprocessed_feature_table`、`test_keys`、`test_model_input`） | 只有尚未落地的月 | **是** |
| 無 month plan（`train_keys`、`val_keys`、`sample_keys`、`train_model_input`、`calibration_*` 等 9 個） | 全量重建，每次覆寫既有分區 | **恆為空集合** |

**所以 log 兩個數字都印**：`after`（這個版本底下現在有哪些分區）＋ `new`（其中哪些是這次新增
的）。只印 `new` 的話，上表第二列那 9 個節點會固定印「0 new」，讀起來像「什麼都沒寫」——比原
本更糟的假訊號。全量重建的節點由 `after` 承載資訊，增量節點由 `new` 承載。

**真盲點只有一個**：`--rebuild-dates` 重算既有月份時 `new` 為空、`after` 又包含沒動的月，兩
個數字都說不出「這次重算了哪幾個月」。接受它，因為那次要重算哪幾個月是 CLI 旗標的值，入口已
經有 log（見 `docs/operations/adding-an-eval-month.md` 的重算流程）。

## 考慮過但否決的選項

- **由呼叫端傳 `month_plan.to_process`**：成本同樣是零且對月份精確，但要改 `save()` 的介面或
  改走 `@` handle（[ADR-0007](0007-month-plans-travel-through-the-catalog.md)），動的是框架
  邊界，換到的資訊還比較少——`test_model_input` 的 partition 含 `prod_name`，呼叫端不保證知
  道實際落地了哪些值。
- **寫入前 `df.cache()`**：能讓第二句吃快取，但 `model_input` 是寬表，為了一行 log 佔住
  executor 記憶體不划算。
- **直接刪掉那行 log**：成本零，但使用者確認需要從 log 知道寫了哪些分區。

## 什麼會讓這個決定失效

- **Spark 升級後具備 join elimination**（能證明 join 不改變列數而消掉它）：那時原本那句的成本
  會塌成一次 partition-only 掃描，精確性就變得便宜。目前 3.3.2 沒有。
- **`existing_partition_values()` 不再是零掃描**：它現在的保證寫在自己的 docstring 裡；那句話
  若失效，本 ADR 的成本論證跟著失效。
- **出現需要逐位精確 `new` 的下游消費者**：目前那行 log 沒有任何文件或測試依賴（本次改動前
  grep 過），所以它是純資訊性的。一旦有程式讀它，就得重新評估。

## 這條 ADR 沒有解決的事

- **那 3 張沒有 `partition_filter` 的 inference 表**（`score_table`、`ranked_staging`、
  `ranked_predictions`）仍然每次寫入都把整條 lineage 跑第二次。本 ADR 只把它們**排除**在新
  做法之外、保住原行為，沒有修好它們。要修得先決定 inference 的輸出怎麼分區
  （[#179](https://github.com/curtis-lu/recsys-demo/issues/179)），那屬於 inference pipeline
  的重構，不屬於這裡。
- **`--rebuild-dates` 重算既有月份時**，`new` 為空、`after` 又含沒動的月，兩個數字都說不出
  這次重算了哪幾個月。見上一節。
- **哪個 node 的時間花在哪**不是這條 ADR 的題目。它由 Runner 的 `load`／`func`／`save` 分段
  回答（`core/runner.py`），本 ADR 只是讓 `save` 那一段不再包含一趟白做的重算。

## 稽核

`tests/test_io/test_hive_table_dataset.py::TestSaveReportsPartitionsWithoutRecomputing`
把「不得對傳入的 `df` 觸發第二個 action」釘成可測的成本契約——測試用的 DataFrame double 對
`collect` / `count` / `distinct` / `toPandas` 等一律 raise。分區狀態的 stub 依「`insertInto`
有沒有跑過」回答而非依呼叫次序：後者分不出「快照拍在寫入前」與「拍在寫入後」，而那正是這段
邏輯唯一的因果核心。
