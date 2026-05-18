# Sampling Overrides Editor + 冷門產品模型層加權 — 設計 spec

- 日期：2026-05-18
- 分支：`feat/sampling-overrides-editor`
- 狀態：設計已與使用者逐段確認，待 spec review

## 1. 問題

`conf/base/parameters_dataset.yaml` 的 `sample_ratio_overrides` 以
`|`-joined `sample_group_keys`（現為 `[cust_segment_typ, prod_name, label]`）
為 key、抽樣比例為 value。item 多（dev 8、prod ~22）、客群分類也可能多類，
人工手寫每個 `segment|product|label` 的 override 比例繁瑣且易錯（打錯 item
分量會被 A5 一致性閘擋下，但 segment 分量打錯會 silently never match）。

需求：一個資料驅動的工具，視覺化呈現各組統計、預填建議值、讓使用者調整後
匯出，再以稀疏 YAML 片段人工貼回 config。延伸需求（使用者「重新思考」）：
冷門產品需要**上採樣機制**——但現有降採樣機制無法上採樣。

## 2. 核心決策（已逐項與使用者確認）

| # | 決策 | 確認 |
|---|---|---|
| D1 | override 消費方式 = **產 YAML 片段、人工貼回**（對齊 `scripts/suggest_categorical_cols.py` 前例；不新增 config 載入路徑/驗證面） | ✓ |
| D2 | 視覺化媒介 = **本機 self-contained HTML 矩陣編輯器**（純 stdlib 產出、免 server、免額外套件），匯出 JSON + 可複製 YAML 片段，**只匯出 ≠ default 的 cell** | ✓ |
| D3 | 維度值與建議比例 = **從真實資料分布算**（profile sample_pool）；item 軸仍以 `schema.categorical_values` 為準 | ✓ |
| D4 | 範圍 = **只做 train、稀疏輸出**；calibration / val overrides 不在本輪 | ✓ |
| D5 | 冷門上採樣機制 = **方案 C**：降採樣留資料層（機制不動）、冷門 boost 改**模型層 LightGBM `sample_weight`**（無重複列洩漏，對 GBDT 統計最乾淨） | ✓ |
| D6 | weight 只作用於 LightGBM **train** 的 `lgb.Dataset(weight=...)`；early-stopping val、calibration、evaluation **一律不加權** | ✓ |
| D7 | model-input 接法 = **carry `cust_segment_typ` 進 model_input parquet、於 `extract_Xy` 依訓練 config 算 weight**（不烤進 parquet，保 dataset cache 跨 weight 變更仍有效，weight 只綁 `model_version`） | ✓ |
| D8 | 冷門 weight 公式採反頻率家族 + 兩安全閥；`median_pos` 母體用 **per-cell（全 (segment,product) grid）中位數** | ✓ |

## 3. 兩個正交機制

| 機制 | 層 | 載體 config | 目的 | versioning |
|---|---|---|---|---|
| 降採樣 | 資料層（現有 `select_keys`，**不改機制**） | `parameters_dataset.yaml: dataset.sample_ratio_overrides`（train、稀疏） | 壓過度代表的負樣本 | 已在 `train_variant_id`（`versioning.py` `TRAIN_SAMPLING_KEYS`） |
| 冷門 boost | 模型層（**新增**，LightGBM `sample_weight`） | `parameters_training.yaml: training.sample_weights`（稀疏） | 不複製列、平滑放大冷門 (segment,product) 梯度 | 自動納入 `model_version`（`_model_version_payload` 對 `training:` 下任何新 key 預設 over-include；**不需改 versioning 程式碼**） |

正交不重複計：weight clamp ≥ 1 只 boost、永不重複砍降採樣已砍的負樣本；
降採樣只動負樣本、不碰正樣本。

## 4. 建議值公式（editor 的起始建議，非固定政策；每格可手動覆寫）

### 4.1 降採樣 ratio（`sample_ratio_overrides`）
每 (segment, product)：正樣本全留；目標 `neg:pos = R`：

```
neg_ratio = clamp(R * n_pos / n_neg, 0, 1)
```

- 旋鈕：`--target-neg-pos`（R，預設 5）
- 已平衡組（`R*n_pos/n_neg >= 1`）→ ratio 1.0 → 不輸出（稀疏）

### 4.2 冷門 weight（`training.sample_weights`）
每 (segment, product)：

```
w = clamp( (median_pos / n_pos) ** alpha, 1.0, W_max )
```

- `n_pos` = 該 (segment,product) 正樣本數（熱度代理；ranking 任務的稀缺問
  題在正樣本不足）
- `median_pos` = **全 (segment,product) grid cell 的 `n_pos` 中位數**
  （per-cell；對右偏分布穩健、客群偏斜納入）
- 旋鈕：`--alpha`（預設 0.5，sqrt 阻尼）、`--w-max`（預設 5.0）
- 下界 1.0 = boost-only（過度代表交降採樣，不重複罰）；上界 W_max = 防極尾
  組劫持目標函數
- `n_pos >= median_pos` → w 1.0 → 不輸出（稀疏；LightGBM 缺項視為 1.0）

兩前提：(a) instance weighting 非複製列 → 無 CV/early-stop 洩漏；
(b) 加權 train 會位移機率 → 故 D6（calibration 不加權）；profile 取在
**train snap_dates 同視窗**，`median_pos` 才不與訓練視窗漂移。

## 5. 架構

### 5.1 工具：`scripts/sampling_overrides_editor.py`
非 production DAG、本機跑、Typer app、純 stdlib 產 HTML（無額外套件）；
沿用 `suggest_categorical_cols.py` 慣例（吃 Hive table 或 parquet path，dev
預設 `ml_recsys.sample_pool` / `data/sample_pool.parquet`，產物寫
`data/profiling/`）。

**子指令 `profile <table>`**
- Spark `groupBy(cust_segment_typ, prod_name, label)` 算 `n_pos`/`n_neg`/
  `pos_rate`，profile 視窗 = train snap_dates
- 計算 4.1 / 4.2 建議值
- 產 self-contained HTML：grid 預填統計欄（n_pos / n_neg / pos_rate）+ 兩
  可編欄（ratio、weight）+ 內嵌 vanilla JS；按鈕「Export JSON」（下載）、
  「Export YAML snippet」（可複製面板，兩段）
- item 軸取自 `core.schema.get_schema` 的 `categorical_values`（A5/A7 依賴的
  單一真實來源）；segment 軸由資料 distinct（config 無宣告）

**子指令 `to-yaml <export.json>`**
- 讀回 JSON → 跑 A5 + 新 A7 predicate（貼回前先擋壞 key，collect-all）→
  正規化 → 印兩段稀疏 YAML（只出 ≠ default 的 cell）：
  - `dataset.sample_ratio_overrides` → 貼 `parameters_dataset.yaml`
  - `training.sample_weights` → 貼 `parameters_training.yaml`

### 5.2 模型層加權資料流接點（風險最高，已核實）
- `pipelines/dataset/nodes_spark.py::build_model_input`：`cust_segment_typ`
  由 drop 改為**非特徵 carry 欄保留**（與 identity 欄同類，不入
  `preprocessor_metadata["feature_columns"]` → 自然不入 X）。`prod_name`
  本就保留。Spark 端無 UDF（weight 不在此算）。
- `io/extract.py::extract_Xy`：新增回傳第三陣列 `w`（與 X/y 1:1）。在 train
  讀 pdf 後、**`_pdf_to_X` 編碼之前**，依
  `params["training"]["sample_weights"]` 對該 pdf 中**原始字串**值
  `(cust_segment_typ, prod_name)` 算每列 weight（缺項 = 1.0）。weight 表
  key 與 config/A7 一致採原始 item/segment 字串，不涉 category_mappings 編
  碼。**不烤進 parquet**（理由見 D7）。需相容既有 2-tuple 呼叫點
  （calibration/eval 路徑取 weight=全 1 或不取第三項）。
- `models/lightgbm_adapter.py::train`：`lgb.Dataset(X_train, label=y_train,
  weight=w_train, free_raw_data=False)`；`val_dataset` **不傳 weight**（D6）。
- 預快取路徑（`lgb.Dataset` 直建於 numpy）與 cache 路徑都要帶到 weight。

### 5.3 一致性不變量 A7（遵 CLAUDE.md 單一真實來源）
`core/consistency.py` 新增 pure predicate `weight_unknown_items`（比照 A5
`override_unknown_items`：只驗 key 的 **item 分量** ∈
`resolved_item_values`；segment 分量無 config 宣告、不驗），註冊進
`validate_config_consistency`（collect-all），模組 docstring 的 Invariant
legend 增列 A7。**不得在 pipeline 散落 ad-hoc 檢查**。

## 6. 錯誤處理
- A7 在 CLI 進入點（`__main__._load_config_and_setup`）fail-loud；與既有
  A1–A5 一次 collect-all raise `ConfigConsistencyError`
- `to-yaml` 對 unknown item collect-all 報錯，貼回前即擋
- HTML 為唯讀本機產物，壞輸入只影響本機、不入 DAG
- `extract_Xy` weight 路徑：unknown (segment,product) → weight 1.0（與稀疏
  語意一致），不 raise

## 7. 測試策略（遵專案測試效能規範：跑快不少跑）
- 純函式（4.1/4.2 公式、稀疏化、JSON↔YAML、`weight_unknown_items`）→
  numpy/dict 單元測試，無 Spark
- `profile` 的 Spark groupBy → 小固定資料單一 Spark 測試（沿 conftest
  `spark` fixture）
- `extract_Xy` 加權路徑、`build_model_input` carry 欄、LightGBM weight 串接
  → 擴充既有測試模式；驗證 val/calibration/eval 路徑 weight 不生效（D6）
- versioning：改 `training.sample_weights` 須 bust `model_version`、不動
  `train_variant_id` 的回歸測試

## 8. 變更檔案清單（預估）
- 新增 `scripts/sampling_overrides_editor.py`
- 新增 HTML/JS 模板（內嵌於 script 或同目錄 template）
- 改 `src/recsys_tfb/core/consistency.py`（A7 predicate + 註冊 + legend）
- 改 `src/recsys_tfb/pipelines/dataset/nodes_spark.py`（carry
  `cust_segment_typ`）
- 改 `src/recsys_tfb/io/extract.py`（`extract_Xy` 回傳/計算 weight）
- 改 `src/recsys_tfb/models/lightgbm_adapter.py`（train 串 `weight=`）
- 改 `conf/base/parameters_training.yaml`（新增 `training.sample_weights:`
  空 dict + 註解）
- 對應 `tests/` 擴充

## 9. 範圍外 / deferred
- calibration_sample_ratio_overrides、val overrides 的 editor 支援（D4）
- 方案 B（ratio>1 複製列上採樣機制）——已評估後不採（GBDT 統計最弱、
  blast radius 大）
- editor 把建議值寫回 config 的自動化（D1：維持人工貼回）
- segment 值的 config 宣告 / A7 對 segment 分量的驗證（config 無宣告來源）
