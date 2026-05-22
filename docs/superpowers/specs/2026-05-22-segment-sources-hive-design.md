# Design: evaluation segment_sources 改吃 Hive table

- 日期：2026-05-22
- 分支：`feat/segment-sources-hive`
- 狀態：設計已核准,待寫實作計畫

## 背景與問題

evaluation pipeline 產出的 `report.html` 缺少 per-segment(分群)段落。追查鏈如下：

1. `report_builder.build_segment_section` 在 `metrics["per_segment"]` 為空時回傳 `None`,
   該 section 即不進 HTML。
2. `metrics_spark.compute_all_metrics` 只有在偵測到 `active_seg_col`(`segment_columns`
   清單中第一個出現在資料欄位裡的欄)時才會計算 `per_segment`。
3. `segment_columns` 預設為 `[cust_segment_typ]`,但 `cust_segment_typ` 既不在
   `ranked_predictions`,也不該在 `label_table` 裡(production label ETL
   `conf/sql/etl/label/label_{ccard,exchange,fund}.sql` 並不產出此欄)。
4. `segment_sources` 機制本可把外部欄位 join 進來,但預設只配了 `holding_combo`,
   且 `_read_segment_source` 走 `spark.read.parquet()`、讀失敗即**靜默跳過**。

結果：configured 的 segment 欄在資料中無聲缺席 → per_segment 為空 → 報告少一段,
但整份報告看起來「正常」,使用者無從察覺。本設計修正此問題並把
`segment_sources` 從 parquet 改為 Hive table 來源。

## 設計決策

### 1. segment_sources 來源後端：純 Hive table

`segments.py` 的 source seam `_read_segment_source` 改成只讀 Hive table,移除
parquet 路徑：

```python
def _read_segment_source(spark, source_config):
    return spark.table(source_config["table"])
```

配置欄位 `filepath:` → `table:`(fully-qualified,如 `ml_recsys.sample_pool`,
直接傳給 `spark.table()`)。

### 2. segment join 接到 eval_predictions

`prepare_eval_data`(`pipelines/evaluation/nodes_spark.py`)把 `join_segment_sources`
從「join 到 `labels`、在 labels↔predictions join 之前」移到
「`eval_predictions` 建好之後」。修正後流程：

```
predictions 過濾(model_version + snap_date)
  → labels 過濾(收斂到 predictions 的 snap_dates)
  → eval_predictions = ranked_predictions ⋈ labels  (inner, on identity_cols)
  → 補 rank(若缺)
  → eval_predictions = join_segment_sources(eval_predictions, segment_sources)
  → return
```

理由：`label_table` 那側保持最小、不被 segment 欄污染；segment 明確定位成
「最終 eval 表的 enrichment」,而非要硬撐過 inner join 的欄位。

### 3. 防 join fan-out：dropDuplicates(key_columns)

`sample_pool` 是 `(snap_date, cust_id, prod_name)` 粒度,一個客戶在同一 snap_date
有多列(每產品一列),`cust_segment_typ` 都相同。segment join 只用
`key_columns`(如 `[cust_id, snap_date]`)當 key,不去重會把 `eval_predictions`
列數放大 ~22 倍。

`join_segment_sources` 在 join 前：

```python
seg = seg_df.select(key_columns + [segment_column]).dropDuplicates(key_columns)
eval_predictions = eval_predictions.join(seg, on=key_columns, how="left")
```

用 `dropDuplicates(key_columns)`(非 `.distinct()`)——**保證** join 後每個
`(cust_id, snap_date)` 至多一列,無 fan-out,且不需額外 Spark action。對齊
「同一 snap_date 下一個 cust_id 只能有一筆 segment」的不變量。

`how="left"`:沒對到 segment 的客戶該欄為 NULL,`aggregate_per_segment` 的
`groupBy` 會把它們歸成一個 null 分群,可接受。

### 4. 修根因：合成 label_table 不應帶 cust_segment_typ

`scripts/generate_synthetic_data.py` 的 `generate_label_table`(約 line 357)
在 `prod_df` 裡塞了 `"cust_segment_typ": kept_segments`,使合成
`data/label_table.parquet` 多出此欄(production label ETL 並無此欄,該檔 docstring
本就宣稱 label_table「mirrors `label_{ccard,exchange,fund}.sql` semantics」)。

修正：

- 從 `generate_label_table` 的 `prod_df` 移除 `"cust_segment_typ"` 欄。
- 重跑 `generate_synthetic_data.py` 重新產出 `data/label_table.parquet`。
- `generate_sample_pool` 不受影響:它自行另算 `segments`,只從 label_table 取
  `[snap_date, cust_id, prod_name, label]`(已驗證)。`data/feature_table.parquet`、
  `data/sample_pool.parquet` 內容不變。

### 5. 護欄模型

| 情況 | 行為 | 落點 |
|---|---|---|
| `segment_columns` 列了某欄,但無任何 `segment_sources` 條目提供它 | **raise** `ConfigConsistencyError` | `core/consistency.py` 新增 predicate,`validate_config_consistency` 於 CLI entry 執行 |
| `segment_sources` 的 Hive 表讀不到 | **raise**(`spark.table()` 的 `AnalysisException` 往上拋,raise 前補「哪個 segment source / 哪張表」context) | `join_segment_sources` |
| 表讀到了但缺 `key_columns` 或 `segment_column` | **raise** 明確錯誤(不讓 Spark 在 `.select()` 丟模糊錯) | `join_segment_sources` join 前欄位檢查 |
| `segment_column` 已預先存在於 `eval_predictions`(碰撞,如未來有人把該欄加回 label_table) | **silent drop 預存欄 + info log**,以 segment_sources 為權威來源 | `join_segment_sources` join 前 |

第一條 predicate 的內容:**`evaluation.segment_columns` 的每一欄,必須等於某個
`evaluation.segment_sources` 條目的 `segment_column`**。專案規定一致性不變量必須在
`core/consistency.py` 集中定義,不得在各 pipeline ad-hoc 散落。此 check 在跑任何
Spark 之前就擋下「configured segment 欄無來源」——正是本次踩坑的狀況。

碰撞採 silent drop 而非 raise:config 已**明確宣告** `segment_sources` 是該欄的
權威來源,旁邊有個同名欄只是無害雜訊,不是非預期狀態;以 segment_sources 為準
即可,但留一行 info log 記錄。

### 6. 預設配置（`conf/base/parameters_evaluation.yaml`）

```yaml
segment_sources:
  cust_segment_typ:
    table: ml_recsys.sample_pool
    key_columns: [cust_id, snap_date]
    segment_column: cust_segment_typ
  # 外部 Hive table 範例（保留彈性，預設註解掉；無對應表時若留作 active
  # 條目，fail-loud 下會讓 evaluation 直接報錯）：
  # holding_combo:
  #   table: ml_recsys.holding_combo
  #   key_columns: [cust_id, snap_date]
  #   segment_column: holding_combo
```

`ml_recsys.sample_pool` 在 dev(合成 `sample_pool.parquet` 含 `cust_segment_typ`)
與 production(`conf/sql/etl/sample_pool/sample_pool.sql` 產出含此欄)皆可用,
不需新增 ETL。

`segment_columns: [cust_segment_typ]` 維持不動(metric 層偵測用),但其註解
`# Segment columns already present in the labels DataFrame` 已過時,改為
「由 segment_sources join 進來」。

## 影響檔案

- `src/recsys_tfb/evaluation/segments.py` — source seam 改 `spark.table()`;
  `join_segment_sources` 加欄位檢查、碰撞 drop、`dropDuplicates`、fail-loud,移除
  `seg_df is None` 分支。
- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` — `join_segment_sources`
  呼叫點從 labels-join 前移到 `eval_predictions` 建好後。
- `src/recsys_tfb/core/consistency.py` — 新增 `segment_columns ⊆ segment_sources`
  的 predicate,納入 `validate_config_consistency`。
- `conf/base/parameters_evaluation.yaml` — `segment_sources` 改 Hive table 寫法、
  新增 `cust_segment_typ` 條目;更新 `segment_columns` 註解。
- `scripts/generate_synthetic_data.py` — `generate_label_table` 移除
  `cust_segment_typ` 欄。
- `data/label_table.parquet` — 重新產生(不再含 `cust_segment_typ`)。
- `tests/test_evaluation/`、`tests/test_core/`(consistency)、相關 pipeline 測試 —
  既有測試更新 + 新護欄的測試。

## 測試重點

- `segments.join_segment_sources`：Hive 來源讀取、`dropDuplicates` 後無 fan-out、
  缺欄 raise、表不存在 raise、碰撞 drop 後以 segment_sources 為準、left join 的
  NULL 分群。
- `core/consistency.py`：`segment_columns` 有/無對應 `segment_sources` 條目時的
  raise / pass。
- `prepare_eval_data`:segment join 接在 `eval_predictions` 之後,且
  `eval_predictions` 列數不因 segment join 變動。
- `compute_all_metrics` → `report_builder.build_segment_section`:end-to-end
  確認 per-segment 段落在 segment 欄存在時出現。

## 範圍外（後續工作）

- per_segment 目前只支援單一 segment 維度(`compute_all_metrics` 取
  `segment_columns` 第一個出現的欄)。多 segment 維度同時切分需把 `per_segment`
  改成 dict-of-dicts,本次不做。
- `holding_combo` 等外部 segment 表的 source_etl(產表流程)不在本次範圍。
