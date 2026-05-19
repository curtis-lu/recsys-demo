# Sampling Overrides Editor + 冷門產品模型層加權 — 設計 spec

- 日期：2026-05-18
- 分支：`feat/sampling-overrides-editor`
- 狀態：設計已逐段確認；D7→D7'、D9（拆兩 plan）；branch rebase 到 origin/main 後依 PR#22（lambdarank/group plumbing）再修 D10（A7→A8、extract 改 with_weights opt-in）

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
| D7' | model-input 接法（plan 前核實後修正）= **新增 `dataset.carry_columns` 可設定清單，從 `sample_pool` 經 `select_keys` carry 進 train/train_dev model_input parquet**；weight 於訓練讀取時依 `training.sample_weights` 算（不烤進 parquet）。carry 一組寬鬆超集、weight 只取需要子集 → 調 weight 表完全不動 dataset、免重產資料。**原 D7「經 feature_table carry」作廢**：`select_keys` 只回傳 identity_key（`helpers_spark.py:66,90`，`cust_segment_typ` 在回傳前被丟），且合成 dev `feature_table` 無 `cust_segment_typ`（記憶 `project_cust_segment_typ_devprod_schema_divergence` 地雷，dev-cluster 測不到）；`sample_pool` dev+prod 都有且為 identity 粒度 | ✓ |
| D8 | 冷門 weight 公式採反頻率家族 + 兩安全閥；`median_pos` 母體用 **per-cell（全 (segment,product) grid）中位數** | ✓ |
| D10 | rebase 到 origin/main（含 PR#22 `feat/configurable-hpo-search-space`）後的調和：(a) PR#22 已占用不變量 **A7**（`ranking_objective_conflicts`），本案 `weight_unknown_items` 改編號 **A8**；(b) PR#22 把 `prepare_train_inputs`/refit 依 objective 分 **binary／ranking** 兩支，ranking 支用 `extract_Xy_with_groups` 並以 `perm` **重排列**——故 weight 必須與 X/y **同次抽取**且套同一 `perm`，原「單一 `extract_Xyw` sibling」作廢，改為對 `extract_Xy` 與 `extract_Xy_with_groups` 各加 opt-in `with_weights`；(c) refit/trial 的 `adapter.train` 收**預建 `train_dataset`**（weight 在 `lgb.Dataset` 建構時注入），故 **`train()` 簽章不需改** | ✓ |
| D9 | 實作拆**兩個獨立 plan**（writing-plans scope check）：Plan A = 機制（carry_columns + 模型層加權 + A7 + config，可獨立 ship/測，用手寫 `sample_weights`）；Plan B = 工具（`sampling_overrides_editor.py`，獨立 dev script）。Plan A 先做（Plan B 依賴其 `sample_weights` config schape） | ✓ |

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

### 5.2 carry_columns 與模型層加權資料流接點（風險最高，已核實）

**新 config `dataset.carry_columns`**（list，預設 `["cust_segment_typ"]`）：
非 identity、要從 `sample_pool` 帶進 model_input 供訓練讀取的欄位。寬鬆超
集策略——使用者可一次列足（如再加 `channel_preference`），weight 只取需要
子集；改 weight 表不動 dataset。`carry_columns` 不在 `ALL_SAMPLING_KEYS`，
`compute_base_dataset_version`（`versioning.py:80`）天然納入 → 改 carry 集
會 bust `base_dataset_version`（正確：parquet schema 變了），但日常調
weight 不會（**versioning 不需改程式碼**，已核實）。

接點（只走 train 路徑，符合 D6）：
- `pipelines/dataset/helpers_spark.py::select_keys`：回傳由 `identity_key`
  改為 `identity_key + carry_columns`（carry 為 passthrough，**不參與抽樣
  決定論**——`spark_bucket` 仍只 hash identity_key；早回傳路徑與 override
  路徑都要帶 carry）。
- `pipelines/dataset/nodes_spark.py::split_train_keys`：join 後 train /
  train_dev keys 仍帶 carry（join key 是 `cust_col`，carry 隨 row 通過）。
  `select_val_keys` / `select_test_keys` / `select_calibration_keys` **不改**
  （weight train-only；它們不經 `select_keys` 或不需 carry）。
- `preprocessing/_spark.py::build_model_input`：output 由
  `identity + label + feature_columns` 改為 `+ carry_present`，其中
  `carry_present = [c for c in carry_columns if c in keys.columns]`（val/
  test/cal keys 無 carry → 條件式包含，graceful）。carry 不入
  `feature_columns` → `_pdf_to_X` 切片自然不入 X。Spark 端無 UDF。
- `io/extract.py`：對 `extract_Xy` 與 `extract_Xy_with_groups` 各加
  keyword-only **`with_weights: bool = False`**（不改既有正位簽章/回傳元
  數 → 既有呼叫零影響）。`with_weights=True` 時，在該函式**既有 filter/
  slice 之後、與回傳 X/y(/groups) 同一 pdf 列序**，依
  `params["training"]["sample_weights"]` 對 pdf 中**原始字串**
  `(cust_segment_typ, prod_name)` 算 `w` 並 append 進回傳 tuple：
  `extract_Xy → (X,y,w)`、`extract_Xy_with_groups → (X,y,groups,w)`；缺
  `sample_weights` 或無 `cust_segment_typ` 欄 → `w` 全 1（不 raise）。純
  helper `_compute_row_weights(seg, prod, sample_weights)`（numpy）為唯一
  算式來源。
- weight 注入點 = **4 個 `lgb.Dataset(...)` 建構處**，皆 train，ranking
  支須套與 X/y 相同的 `perm`：
  1. `lightgbm_adapter.py::prepare_train_inputs` ranking 支
     （`extract_Xy_with_groups(..., with_weights=True)` →
     `weight=w[perm]`，train+dev）
  2. 同函式 binary 支（`extract_Xy(..., with_weights=True)` →
     `weight=w`，train+dev）
  3. `training/nodes.py` refit ranking 支（`w_full=concat`，
     `ds_full=lgb.Dataset(..., weight=w_full[perm], group=grp)`）
  4. 同處 binary 支（`ds_full=lgb.Dataset(..., weight=w_full)`）
- **不加權處（不傳 `with_weights`，結構上保證）**：val/HPO
  `extract_Xy_with_groups(..., filter_groups_with_positives=True)`
  （`nodes.py`）、calibration `extract_Xy`（`calibrate_model`）。符合 D6。
- **`adapter.train()` 簽章不改**：refit/trial 收預建 `train_dataset=`，
  weight 已於上述 `lgb.Dataset` 建構時烤入；`val_dataset` 永不帶 weight。

### 5.3 一致性不變量 A8（遵 CLAUDE.md 單一真實來源）
PR#22 已占用 A7（`ranking_objective_conflicts`）。`core/consistency.py`
新增 pure predicate `weight_unknown_items`（比照 A5 `override_unknown_items`：
key 格式固定 `"<segment>|<product>"`，只驗 **product 分量(index 1)** ∈
`resolved_item_values`；segment 無 config 宣告、不驗），註冊進
`validate_config_consistency`（在既有 ranking A7 區塊後、`raise` 前），模組
docstring 的 Invariant legend 在 A7 bullet 後增列 **A8**。**不得在 pipeline
散落 ad-hoc 檢查**。

## 6. 錯誤處理
- A8 在 CLI 進入點（`__main__._load_config_and_setup`）fail-loud；與既有
  A1–A7 一次 collect-all raise `ConfigConsistencyError`
- `to-yaml` 對 unknown item collect-all 報錯，貼回前即擋
- HTML 為唯讀本機產物，壞輸入只影響本機、不入 DAG
- `with_weights` 路徑：unknown (segment,product) 或無 carry 欄 → weight 1.0
  （與稀疏語意一致），不 raise

## 7. 測試策略（遵專案測試效能規範：跑快不少跑）
- 純函式（4.1/4.2 公式、稀疏化、JSON↔YAML、`weight_unknown_items`）→
  numpy/dict 單元測試，無 Spark
- `profile` 的 Spark groupBy → 小固定資料單一 Spark 測試（沿 conftest
  `spark` fixture）
- `extract_Xy` 加權路徑、`build_model_input` carry 欄、LightGBM weight 串接
  → 擴充既有測試模式；驗證 val/calibration/eval 路徑 weight 不生效（D6）
- versioning：改 `training.sample_weights` 須 bust `model_version`、不動
  `train_variant_id` 的回歸測試

## 8. 變更檔案清單

**Plan A（機制；行號以 rebase 後 `33eb37d` 為準）：**
- 改 `src/recsys_tfb/core/consistency.py`（**A8** `weight_unknown_items` +
  legend 於 A7 bullet 後 + 註冊於 ranking A7 區塊後）
- 改 `src/recsys_tfb/pipelines/dataset/helpers_spark.py::select_keys`
  （回傳 + carry_columns，兩條回傳路徑）— PR#22 未動此檔
- 改 `src/recsys_tfb/pipelines/dataset/nodes_spark.py::split_train_keys`
  （test-only 守恆；val/test/cal 不改）— PR#22 未動此檔
- 改 `src/recsys_tfb/preprocessing/_spark.py::build_model_input`
  （output 條件式含 carry_present）— PR#22 未動此檔
- 改 `src/recsys_tfb/io/extract.py`（`extract_Xy`/`extract_Xy_with_groups`
  各加 `with_weights` opt-in + 純 helper `_compute_row_weights`）
- 改 `src/recsys_tfb/models/lightgbm_adapter.py::prepare_train_inputs`
  （ranking 支 `weight=w[perm]`、binary 支 `weight=w`，train+dev；
  `train()` 簽章**不改**）
- 改 `src/recsys_tfb/pipelines/training/nodes.py`（refit ranking 支
  `weight=w_full[perm]`、binary 支 `weight=w_full`；val/calibration 不傳
  `with_weights`）
- 改 `conf/base/parameters_dataset.yaml`（新增 `dataset.carry_columns`）
- 改 `conf/base/parameters_training.yaml`（`training.sample_weights: {}`
  + 註解；PR#22 已加 lambdarank 註解，本案加在 `training:` 下同層）
- 對應 `tests/` 擴充（含 versioning 回歸：carry_columns bust
  base_dataset_version、sample_weights bust model_version 不動
  train_variant_id）
- versioning **不需改程式碼**（已核實：`_model_version_payload` 對
  `training:` 下任何 key over-include；`carry_columns` 不在
  `ALL_SAMPLING_KEYS` → 進 `base_dataset_version`）

**Plan B（工具）：**
- 新增 `scripts/sampling_overrides_editor.py`（Typer，`profile` /
  `to-yaml` 子指令；純 stdlib HTML）
- 新增內嵌 HTML/JS 模板
- 對應 `tests/`（純函式公式/稀疏/JSON↔YAML + 單一 Spark profile 測試）

## 9. 範圍外 / deferred
- calibration_sample_ratio_overrides、val overrides 的 editor 支援（D4）
- 方案 B（ratio>1 複製列上採樣機制）——已評估後不採（GBDT 統計最弱、
  blast radius 大）
- editor 把建議值寫回 config 的自動化（D1：維持人工貼回）
- segment 值的 config 宣告 / A7 對 segment 分量的驗證（config 無宣告來源）

## 10. 實作拆解（D9；writing-plans scope check）

兩個獨立子系統，各自可獨立 ship 且可測：

- **Plan A — 機制**（`docs/superpowers/plans/2026-05-18-cold-product-weighting-mechanism.md`）：
  carry_columns 基礎建設 + 模型層加權端到端 + A7 + config。完成後用手寫
  `training.sample_weights` 即可訓出加權模型，獨立可驗。**先做。**
- **Plan B — 工具**（`docs/superpowers/plans/2026-05-18-sampling-overrides-editor-tool.md`）：
  `sampling_overrides_editor.py` profile→HTML→to-yaml。獨立 dev script，
  邏輯上依賴 Plan A 已定的 `sample_weights` config schema（不 import 其
  程式碼）。Plan A 之後做。
