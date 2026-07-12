# 設計：關閉「字串特徵欄靜默 OOM」的縫隙（Phase 0）

- 日期：2026-07-11
- 狀態：設計待審（brainstorming 產出，尚未進 writing-plans）
- 範疇：Phase 0 only。Phase 1 / Phase 2 列為 gated follow-up，不在本 spec 實作。

## 1. 背景與問題

生產環境 training pipeline 在 `prepare_lgb_train_inputs` → `extract_Xy` → `_pdf_to_X`
的 `to_numpy` 步被 OOM killer 殺掉（`train.sh: line 5: 72 Killed`）。

根因不是資料量，是**型別**：有字串欄混進了 `feature_columns`。`numpy` 矩陣只能有一種
格子大小，任何一欄是字串 → 整張矩陣退化成 `object`（每格存指標、指向 heap 上獨立的
Python 物件）。實測每格成本從數值矩陣的 8 bytes 暴增到 34.2 bytes（4.3×），公司規模
（4,542,746 列 × 663 欄 ≈ 30.1 億格）從 22.4 GiB 膨脹到約 95.9 GiB。完整推導與量測見
`docs/operations/training-oom-object-matrix.md`。

**這是資料/schema 問題，不是記憶體問題**：即使記憶體無限大，LightGBM 的
`_np2d_to_np1d`（`basic.py:192`）對 object 矩陣做 `np.asarray(mat, dtype=np.float32)`，
碰到真字串會 `ValueError: could not convert string to float`。OOM 只是先發生的症狀。

### 縫隙怎麼開的

`_compute_feature_columns`（`src/recsys_tfb/preprocessing/_spark.py:112`）：

```python
non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
```

凡是不在 `drop_columns`、不在 `identity_columns`、又不是 `label` 的欄，一律成為特徵
——不管型別。而 `_encode_categoricals`（`_spark.py:85`）只把**明確宣告**在
`categorical_columns` 的欄 cast 成 integer。所以一個生產 `feature_table` 有、卻既沒宣告
categorical、也沒被 drop 的字串欄，會原封不動穿過整條 pipeline 成為特徵。

合成資料不產生這類欄位（見 memory `project_cust_segment_typ_devprod_schema_divergence`），
所以**本機永不爆、生產必爆**。

### 為什麼單一閘門不夠（時機點分析）

現有兩道 consistency 閘的觸發時機（查證 `src/recsys_tfb/__main__.py:92`、
`src/recsys_tfb/pipelines/dataset/pipeline.py:25`）：

| 閘門 | 觸發時機 | 讀什麼 | 擋得下當前 cached run？ |
|---|---|---|---|
| Layer-1 `validate_config_consistency`（A1–A14） | CLI 入口，Spark 前 | 只有 config YAML | ❌ 光看 config 看不出 parquet 裡有字串欄 |
| Layer-2 `validate_data_consistency`（B1+B5） | dataset pipeline 第一個 node | `feature_table.dtypes` | ❌ training 不跑 dataset pipeline；要重建 dataset 才會重跑 |

當前失敗的 run 讀的是**已建好的** cached parquet（log 顯示 `cache_train_model_input`
全是 `cache_hit`），完全不經過 dataset pipeline 的 B5 閘。所以一道 dataset 側閘門只能
「防未來重建時復發」，救不了「現在這個已烤壞的 parquet」。要同時做到「當前 run 有清楚
錯誤、不必等 OOM」與「未來不復發」，需要兩個位置 + 一個源頭防呆。

## 2. 目標 / 非目標

### 目標（Phase 0）

- 當前這次生產 run：把「2–4 分鐘後神秘 OOM」變成「秒級 `DataConsistencyError`，直接列出
  兇手欄名」——這份清單同時是 Phase 1 的輸入。
- 未來重建 dataset：字串欄若沒宣告 categorical、也沒 drop，在 dataset 建構第一個 node
  就 fail-fast，進不了 `feature_columns`。
- 源頭防呆：`suggest_categorical_cols.py` 對高 cardinality 字串欄明確建議進 `drop_columns`，
  讓「不當 categorical 的字串」有唯一去處，不再從清單上靜默消失。
- 相關文件同步，各依其讀者設計撰寫角度。

### 非目標（Phase 0 明確不做）

- **不改任何 config**（不動 `categorical_columns` / `drop_columns`）→ 不 bump
  `base_dataset_version` → **不重建 dataset**。
- **不碰記憶體路徑**（無 `lgb.Sequence`、無 Arrow 直通、無 float32 降精度）。
- **不會讓當前 run 訓練成功**——只讓它快速、清楚地失敗。訓練要成功屬於 Phase 1。
- 不動 handbooks、歷史 plans/specs、design-principles 等未描述此行為的文件。

## 3. 設計總覽

一個純 predicate（登記為不變量 **B6**），三個用途，把同一縫隙關在三個時間點：

```
寫 config 時          建 dataset 時              訓練時
suggest 腳本   ──►    validate_data_consistency  ──►   extract_Xy fail-fast
(建議 drop)          (B6，dataset pipeline 首 node)   (讀 parquet schema，讀資料前)
   源頭防呆              防復發                          救現在
```

belt-and-suspenders：腳本讓 config 一開始就寫對；B6 dataset 閘在建構時擋住手改錯的
config；training fail-fast 擋住已經建壞的 parquet。

## 4. 元件設計

### 4.1 核心 predicate（`src/recsys_tfb/core/consistency.py`，不變量 B6）

坐在 B5（`categorical_dtype_errors`）旁邊，是 B5 的反面。純函式、無 Spark：

```python
def nonnumeric_feature_errors(
    feature_col_dtypes: dict[str, str],   # 僅 feature 欄：colname -> "numeric" | "nonnumeric"
    will_be_encoded: set[str],            # 現在非數值、但下游會轉成數值的欄
) -> list[str]:
    """B6 — a feature column that is non-numeric and will NOT be encoded downstream.
    Such a column forces DataFrame.values into object dtype (OOM) and later
    fails LightGBM's float cast. Returns offending column names (sorted)."""
    return sorted(
        col for col, kind in feature_col_dtypes.items()
        if kind != "numeric" and col not in will_be_encoded
    )
```

兩個呼叫點各自建 `feature_col_dtypes`（自己的 schema 來源）與 `will_be_encoded`
（依 config），predicate 本體只有一份——符合 consistency.py「single source of truth」。

`will_be_encoded` 兩處語意不同（關鍵細節）：
- dataset 側（讀 `feature_table.dtypes`，**編碼前**）：所有宣告的 `categorical_columns`
  都會在建 dataset 時 cast 成 int → `will_be_encoded = set(categorical_columns)`。
- training 側（讀 parquet schema，**編碼後**）：Spark 端 categoricals 在 parquet 裡已是
  int（不會被 predicate 命中）；唯一仍是字串的合法特徵是 deferred identity categorical
  （`prod_name`，留到 `_pdf_to_X` 才編）→ `will_be_encoded = {identity cats in categorical_columns}`。

登記：consistency.py 模組 docstring 的 Invariant legend 加 B6 條目（B4 目前未用；B6 取
與 B5 相鄰的號，避免與 deferred 的 B2/B3 混淆）。

### 4.2 呼叫點 1 — dataset 側閘（防復發）

擴充既有 `validate_data_consistency`（`pipelines/dataset/nodes_spark.py:154` 委派至
`core/consistency.py`）。它已為 B5 讀過 `feature_table.dtypes`，B6 複用同一份讀取，
**不增加任何 Spark action**。B6 與 B1/B5 一起 collect、一次 raise `DataConsistencyError`。
掛點不變（dataset pipeline 第一個 node，`outputs=None`，fail-fast）。

### 4.3 呼叫點 2 — training 側 fail-fast（救現在）

在 `extract_Xy`（`src/recsys_tfb/io/extract.py`）的昂貴 `handle.to_pandas()` **之前**。
`_log_parquet_metadata`（`extract.py:193`）本就用 `pyarrow.dataset` 讀 parquet schema
（metadata only，不讀資料）。新增一個薄函式從該 schema 取 feature 欄型別，餵 B6 predicate，
命中即 raise `DataConsistencyError`，訊息列出兇手欄名。

- 位置：`extract_Xy` 與 `extract_Xy_with_groups` 兩條入口都要（ranking 走後者）。抽一個
  共用 helper（如 `_assert_feature_dtypes(schema, preprocessor_metadata, parameters)`），
  兩處都在 read_parquet 前呼叫，避免重複。
- 語意上這是「runtime backstop」，與 `_spark.py` 對 A2/A3 的 runtime 守衛同款模式
  （consistency.py docstring 已描述該模式）。
- 效果：turns 69s 讀取 + OOM 成讀 schema 後秒級 raise。

### 4.4 呼叫點 3 — `scripts/suggest_categorical_cols.py`（關源頭）

現況（`:52-84`）：字串/布林欄**無條件**進 categorical 建議，不算 cardinality；只有數值欄
有門檻。改法：

- 把字串欄一起放進現有 `approx_count_distinct` 聚合（`:65-68`），單一 action 拿到，
  **不多掃一次**。
- 新增 CLI 選項 `--max-string-cardinality`（預設 **50**；與數值欄的 `--max-cardinality`
  分開，因字串合法 categorical 常有 20+ 值）。
- 路由：字串欄 `n_distinct ≤ 50` → `categorical_columns`（同現況）；`> 50` →
  **新的 `drop_columns:` 建議塊**，每欄附 `# nunique=N` 註解說明為何。
- `format_yaml_output` 同時輸出 `categorical_columns:` 與 `drop_columns:` 兩塊（drop 塊
  無內容時以註解標明「（無高 cardinality 字串欄）」而非省略，讓使用者知道有檢查過）。
- `_print_summary` 增列被導向 drop 的欄與其 cardinality。
- `suggest_categorical_columns_spark` 回傳簽名增第 4 元素（drop 建議清單）。

### 4.5 文件更新（各依讀者設計角度）

| 文件 | 讀者 | 變了什麼 | 撰寫角度 |
|---|---|---|---|
| `docs/operations/training-oom-object-matrix.md` | 生產撞到失敗、手上有一則錯誤的工程師 | 症狀從 `Killed` 變成 `DataConsistencyError` 列出兇手欄；§6 snippet 現由閘門自動跑 | 從「OOM 驗屍」改寫成「你收到這則錯誤 → 成因 → 怎麼辦（宣告/drop → Phase 1）」。物件矩陣原理留作「為什麼」，入口改成讀者實際看到的錯誤。§6 標註「閘門現在會自動列出這些欄，此 snippet 供決定 declare/drop 用」 |
| `docs/pipelines/dataset.md` | 替新 dataset 寫 config 的人 | 多一條規則：字串欄必須 categorical 或 drop，否則 B6 擋下；suggest 腳本也建議 drop | 改 `:38`（腳本現在也建議 drop）＋不變量清單（`~:172`）加 B6 規則。規則式、簡短，服務「第一次就寫對」 |
| `docs/operations/known-pitfalls.md` | 未來開發／Claude session | footgun 現被閘門擋住 | 一條，照既有格式（第一分鐘認出的症狀／根因／規則／驗證），指向 ops 文件 |
| `README.md`（`:461` operations 表） | 瀏覽 operations 文件的人 | 那份 OOM 文件目前無任何入口（孤兒檔） | 一行連結補進表格 |
| `src/recsys_tfb/core/consistency.py` docstring | 改不變量的人 | 新增 B6 legend | ID-referenced，對齊 A/B 既有體例（隨 4.1） |
| `scripts/suggest_categorical_cols.py` docstring + `--help` | 跑 profiling 的 CLI 使用者 | 新 `--max-string-cardinality` + drop 路由 | CLI 用法語氣，Usage/output 段說明路由（隨 4.4） |

不碰：handbooks、歷史 plans/specs、design-principles.md（未描述此行為）。

## 5. 資料流 / 觸發時機彙整

| 用途 | 觸發時機 | 讀什麼 | 對哪種情境生效 |
|---|---|---|---|
| suggest 腳本 | 人工跑 profiling 時 | 來源表全欄 | 產生 config 之前（源頭） |
| B6 dataset 閘 | dataset pipeline 首 node | `feature_table.dtypes`（metastore，不掃） | 重建 dataset（防復發） |
| training fail-fast | `extract_Xy` 讀資料前 | parquet schema（pyarrow metadata，不讀） | 已建好的 cached parquet（救現在） |

## 6. 測試策略（全本機、TDD、走 worktree）

- **predicate（純單元）**：字串特徵欄不在 `will_be_encoded` → 被抓；`prod_name`（deferred
  identity cat，在 `will_be_encoded`）→ 不抓；數值欄 → 不抓；空輸入 → 空清單。兩種
  `will_be_encoded` 語意各一組。
- **dataset 閘**：小型 local Spark `feature_table` 塞一個未宣告的字串欄 →
  `validate_data_consistency` raise 且訊息含該欄名；乾淨表 → 過。與既有 B5 測試同檔同款。
- **training fail-fast**：造一個含字串特徵欄的小 parquet → `extract_Xy` /
  `extract_Xy_with_groups` 在 `to_pandas` 前就 raise；乾淨 parquet → 正常讀。
  **弄壞驗證**：mutation 下在「fail-fast 呼叫本身」（拿掉 `_assert_feature_dtypes` 的
  呼叫，測試該轉紅），而不是下在 predicate 內部某行——避免上次覆盤那種選錯 mutation
  目標的假綠（見 `~/.claude/rules/20-judgment-rubrics.md` §2）。
- **suggest 腳本**：local DataFrame 含高卡字串、低卡字串、高卡數值、低卡數值各一 →
  斷言路由（高卡字串→drop 塊、低卡字串→categorical、高卡數值→留數值特徵、低卡數值→
  categorical）。斷言**數值**（drop 塊確含該欄、cardinality 值正確），不是只斷型別。
  `tests/scripts/test_suggest_categorical_cols.py`。
- **文件**：交付前派 fresh reader subagent 分別以各文件的目標讀者身分通讀
  `training-oom-object-matrix.md` 與 `dataset.md` 改動段，挑「只有 repo、沒這段對話的人
  是否讀得懂並知道下一步」（memory `feedback_analysis_docs_handbook_style`）。

baseline：改動前於 worktree 基準點跑相關測試建 baseline，排除 main 既有 fail（
known-pitfalls.md §5）。改 code 後跑 graphify rebuild。

## 7. Gated follow-up（不在本 spec；列出解鎖前提）

- **Phase 1 — schema 修正**：解鎖前提＝training fail-fast 或 §6 snippet 給出的兇手欄名
  清單 **＋ 每欄的領域判斷**（是有用類別特徵→宣告 categorical；是 ID/自由文字→drop）。
  改 `categorical_columns` / `drop_columns` → bump `base_dataset_version` → 重建 dataset。
  公司規模重建不可本機驗。這一步才讓 training 真的成功。
- **Phase 2 — 記憶體結構解**：解鎖前提＝Phase 1 後 `X` 已是數值矩陣，但峰值（估 ~54.7 GiB）
  仍超過機器上限（log 顯示機器撐過 48.3 GiB、死在更高處，上限落在此區間）。方案：
  `lgb.Sequence` 串流（僅需 numpy，峰值估 ~4 GiB，與列數脫鉤）或 Arrow 直通（需 `cffi`，
  非 pyarrow/lightgbm 相依，生產未必有）。兩者本機實測過機制可行，公司規模未驗。

## 8. 未證實假設 / 開放問題

- **兇手欄名未知**：§1 的減法（9 string − 3 dropped − 1 encoded）只給下界（≥5），且
  下界依「哪些欄是字串」而變，非嚴謹。確切欄名要 training fail-fast 上線後、或 §6 snippet
  在生產跑出來才知道。本 spec 的三個機制都**不依賴**知道確切欄名（predicate 對任意兇手
  欄集合都成立）。
- **`will_be_encoded` 對 boolean 欄的處理**：布林欄在 numpy 塌縮不會變 object（bool→數值
  相容），predicate 應把 boolean 視為 "numeric"。實作時以 pyarrow / Spark 型別判定為準，
  於測試涵蓋一個布林特徵欄確認不被誤抓。
- **B6 vs B4 編號**：legend 現有 B1/B2(deferred)/B3(deferred)/B5，B4 未用。取 B6（與 B5
  相鄰、語意配對）。此為內部 ID，可於審查調整。

## 9. 不變量記錄位置

新不變量 B6 的唯一真實來源＝`src/recsys_tfb/core/consistency.py` 的 predicate ＋ 模組
docstring legend（CLAUDE.md「新增一致性不變量必須在該模組加 predicate，不得 ad-hoc
散落」）。training 側 fail-fast 與 dataset 側閘皆呼叫同一 predicate，不各自定義。
