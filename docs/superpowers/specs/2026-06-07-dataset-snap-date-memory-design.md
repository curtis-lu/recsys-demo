# Dataset pipeline 跨 12 個月 snap_date 的記憶體疑慮 — 設計

- 日期：2026-06-07
- 分支：`feat/dataset-snap-date-memory`
- 狀態：設計已確認，待寫 implementation plan

## 背景與觸發

Dataset pipeline 一次框出 training dataset 約 12 個月的 `snap_date` 範圍（train ∪ cal ∪ val ∪ test 的聯集）。
擔心「12 個月一次抓」對記憶體壓力大，提案改成「by snap_date 批次處理」。

釐清後確認：

- **觸發來源 = 預防性的擔心**（尚未實際觀察到 OOM / executor lost / 大量 spill）。
- **關注環節 = `train`/`train_dev` 產出，以及 `feature_table` 的讀取/編碼**（`apply_preprocessor_to_features`）。

## 關鍵事實：這個架構已經在物理層按 snap_date 分區

「12 個月一次抓」在這套 Spark backend 下**不等於把 12 個月壓進記憶體**，原因有四層（皆已驗證於現行碼）：

1. **全程 lazy + spill**：`apply_preprocessor_to_features`、`build_model_input` 都是 transformation，不在 driver 累積資料；executor 記憶體不足時 Spark spill 到磁碟。
2. **中間產物落磁碟、非 cache 於記憶體**：`preprocessed_feature_table` 在 `conf/base/catalog.yaml` 是 `HiveTableDataset`（`partition_cols: [snap_date]`），編碼後寫回 Hive，下游 5 個 `build_model_input` 各自從 Hive 讀。整個 `src/recsys_tfb/pipelines/dataset/` 與 `preprocessing/_spark.py` **沒有任何 `.cache()` / `.persist()`**（已 grep，零筆）。
3. **輸出依 snap_date dynamic partition 寫出**：每個 `*_model_input` 都 `partition_cols: [snap_date]`，寫入即逐分區落地。
4. **join key 含 `time`**：`base_key = [time] + entity`（`preprocessing/_spark.py`），故 keys⋈label⋈feature 的 join **永不跨 snap_date**——Spark 可逐分區處理。這正是「按 snap_date 切」想要的隔離性，框架已免費提供。

結論：使用者想手動做的「按 snap_date 批次」，**物理執行層已經在做**。手動再切一層在預防性前提下屬 premature optimization，且會破壞 `fit_preprocessor_metadata`「必須看完所有 train 月份才能建一致 category mapping」的前提。

## 真正會在 10M 規模咬人的，不是月份數

決定 peak memory 的是**單一 shuffle partition 大小**與 join 的 shuffle 量，不是 filter 了幾個月。對應旋鈕（目前 `conf/base/parameters.yaml` 的 `spark:` 區塊全是註解的預設）：

- `spark.sql.shuffle.partitions`（預設 200）：220M 列 join 時，分區太少 → 每分區過大 → spill / OOM。**主要旋鈕。**
- AQE（`spark.sql.adaptive.enabled`，Spark 3.3 預設 on）：確認沒被關掉。
- executor / driver memory、broadcast 門檻。

`parameters["spark"]` 的每個 key 會逐一 `builder.config(key, value)`（`utils/spark.py:59-62`）；`spark.sql.shuffle.partitions` 是 runtime SQL config，即使 session 已存在也生效（`utils/spark.py` 的 warning 只針對 cluster-level config），故設在 `parameters.yaml` 的 `spark:` 區塊對 dataset pipeline 有效。

## 決策：方向 A — Spark 調參（文件化）+ 一個小修

不重構 DAG，不加 per-snap_date 迴圈。兩個交付物：

### 交付物 1 — 程式修正：`apply_preprocessor_to_features` 的 multi-`count()` 收斂成單次 aggregation

- **位置**：`src/recsys_tfb/preprocessing/_spark.py`，`apply_preprocessor_to_features`，現行約 359-369 行：

  ```python
  if encode_cols:
      result = _encode_categoricals(result, encode_cols, category_mappings)
      for col in encode_cols:
          n_unknown = result.filter(F.col(col) == -1).count()   # ← N 次全表掃描
          if n_unknown > 0:
              logger.warning("apply_preprocessor_to_features: %d unknowns in column '%s'", n_unknown, col)
  ```

- **問題**：N 個 encode 欄位 = N 次 `.count()` action，每次都重算 filter+encode（`result` lazy 且未 cache），即 N 次全 12 個月 feature_table 掃描，只為了一個診斷警告。

- **改法**：單一 aggregation 一次掃回所有欄位的 unknown 數，再逐欄發相同警告：

  ```python
  if encode_cols:
      result = _encode_categoricals(result, encode_cols, category_mappings)
      counts = result.agg(*[
          F.sum(F.when(F.col(c) == -1, 1).otherwise(0)).alias(c) for c in encode_cols
      ]).collect()[0]
      for c in encode_cols:
          n_unknown = counts[c] or 0
          if n_unknown > 0:
              logger.warning("apply_preprocessor_to_features: %d unknowns in column '%s'", n_unknown, c)
  ```

- **行為不變**：有 unknown 的欄位照樣 per-column WARNING、數字相同；只是 N 次 action → 1 次掃描。
- **不嘗試做到零額外掃描**：診斷的 agg 與最終寫出本來就是兩個 action；要合一需 accumulator / UDF（專案禁 UDF）或 `.cache()`（與本案省記憶體目標相悖）。N→1 是合比例的修正，刻意不過度。

#### TDD

- 現況：`tests/test_preprocessing/test_spark.py` **沒有** `apply_preprocessor_to_features` 的 unknown-warning 測試（只有 `_cast_feature_*` / `filter_groups_with_positives` / `build_model_input` carry）。
- 先加測試（紅）：用一個含「fit 時未見過的 categorical 值」的小 Spark DataFrame，斷言
  - encode 後該欄位該列為 `-1`；
  - 透過 `caplog` 斷言對該欄位發出含正確 count 的 WARNING；
  - 多個 encode 欄位各自發各自的 warning（涵蓋「逐欄」語意）。
- 測試應對「舊 N-count 實作」與「新單-agg 實作」都成立（行為等價），確保是純內部優化。先讓測試在現行碼通過後再改實作、確認仍綠，作為等價性回歸。

### 交付物 2 — 文件化 Spark 記憶體旋鈕與「為什麼月份數不是記憶體問題」

不在共用 base config 硬塞生產級數值（dev/合成資料很小，conftest 用 `shuffle.partitions=1`，硬塞會拖慢測試）。改為：

- **`conf/base/parameters.yaml` 的 `spark:` 區塊**：補註解版指引 + 一行可取消註解的生產建議值。內容涵蓋
  - `spark.sql.shuffle.partitions` 的 sizing rule：目標每個 shuffle partition ~128–256MB → `partitions ≈ shuffle_input_bytes / 128MB`；
  - AQE Spark 3.3 預設 on、勿關；
  - executor / driver memory 指引。
- **`docs/pipelines/dataset.md`**：新增一節（例如「## 規模與記憶體」，置於「重跑語意」與「接下來」之間），把本 spec「四層物理分區 → 月份數不是瓶頸 → 真正旋鈕是 shuffle.partitions / AQE / executor memory」的推論固化為使用者可讀說明，並一句指路逃生口 B。

## 刻意不做（YAGNI）

- 不重構 DAG、不加 in-DAG per-snap_date 迴圈（方向 C）。
- 不硬編生產級 Spark 數值進共用 base config。
- 方向 B（外部 per-snap_date 編排）**只在文件留一句指路，不實作**。

## 逃生口 B（僅文件記載，未來需要時才實作）

若日後在生產 10M 規模**實測**撞到單 job 記憶體天花板：snap_date 清單本就 config 驅動，可用「先一次 `fit_preprocessor_metadata` 並持久化 `preprocessor` / `category_mappings`，再以凍結的 preprocessor 逐月跑 apply+build」的外部編排。關鍵限制：category mapping 必須一次 fit 完所有 train 月份（每月各自 fit 會造成跨月編碼不一致，是錯的），故需「fit 一次 / 持久化 / 逐月套用」的拆分。代價：多一層編排 + 多次 Spark 啟動。本案不實作。

## 影響範圍

- `src/recsys_tfb/preprocessing/_spark.py`（交付物 1）
- `tests/test_preprocessing/test_spark.py`（交付物 1 TDD）
- `conf/base/parameters.yaml`（交付物 2，僅註解 + 可選生產值）
- `docs/pipelines/dataset.md`（交付物 2）

不動：DAG 結構、catalog、`nodes_spark.py`、`nodes_shared.py`、sampling、`model_version` 計算（純內部優化 + 註解/文件，不改任何版本雜湊輸入）。

## 驗證

- `tests/test_preprocessing/test_spark.py` 新增測試通過；既有 dataset pipeline 測試不退化。
- 抽查確認 `parameters.yaml` 註解格式正確、`docs/pipelines/dataset.md` 章節銜接無誤。
