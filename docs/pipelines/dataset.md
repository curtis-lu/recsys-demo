# dataset pipeline

> 將 `feature_table`、`label_table` 與 `sample_pool` 轉換為 train、train-dev、calibration、validation 與 test 所需的模型輸入。
> 主要流程為：資料一致性檢查 → 日期切分與抽樣 → fit 前處理器 → 組裝各 split 的 `*_model_input`。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 建立版本化的資料切分、前處理器與模型輸入 |
| 執行指令 | `python -m recsys_tfb dataset` |
| 上游輸入 | `feature_table`、`label_table`、`sample_pool` |
| 主要輸出 | `preprocessor`、`category_mappings`、`*_keys`、`*_model_input` |
| 設定檔 | `conf/base/parameters_dataset.yaml` |
| I/O 設定 | `conf/base/catalog.yaml` |
| 下游 pipeline | `training` |

各 split 的用途如下：

| split | 資料來源 | 用途 |
|---|---|---|
| `train` | `train_snap_dates` 內抽樣後的大部分 entity | 模型訓練 |
| `train_dev` | 與 train 相同日期，依 `train_dev_ratio` 切出的 entity | 單次模型訓練的 early stopping |
| `calibration` | `calibration_snap_dates`，選用 | fit 機率校準器，不參與模型建樹與 HPO |
| `val` | `val_snap_dates` | HPO 跨 trials 選擇最佳超參數 |
| `test` | `test_snap_dates` | 模型完成後的最終離線評估 |

`train` 與 `train_dev` 共用同一段日期，並以 entity 做互斥切分；calibration、val 與 test 則使用各自的時間區間。

## 2. 執行前準備

執行 dataset 前，建議依序確認：

1. **來源表已就緒**：`feature_table` 與 `sample_pool` 必須涵蓋所有設定日期；`label_table` 可以是只保存正例的 sparse table，但 label 觀察窗必須成熟。
2. **schema 角色正確**：`conf/base/parameters.yaml` 的 `time`、`entity`、`item` 與 `label` 必須對應實際欄位。
3. **item 集合一致**：`sample_pool` 在本次日期範圍內的 item 集合必須與 `schema.categorical_values.<item>` 完全一致；`label_table` 不可產生未宣告 item。
4. **日期切分互斥**：train、calibration、val 與 test 日期不可重疊，並應由使用者依時間先後安排，避免資料洩漏。
5. **類別欄位已人工確認**：可先使用 `scripts/suggest_categorical_cols.py` 產生候選清單，再決定 `categorical_columns`。
6. **抽樣設定已檢視**：可使用 `scripts/sampling_overrides_editor.py` 檢視各分層樣本量並產生 override。
7. **calibration 設定對齊**：若 dataset 啟用 calibration，training 端也應有相應設定；不需要將 score 解讀為機率時通常不必啟用。

> pipeline 只會檢查日期是否重疊，不會判斷 train、val、test 是否依時間正確排序，也無法自動識別特徵或 label 的未來資訊。

## 3. 設定方式

### 3.1 日期與 split

| 設定 | 必要性 | 說明 | 版本影響 |
|---|---|---|---|
| `train_snap_dates` | 必填 | fit preprocessor 與建立 train/train-dev 的日期 | `base_dataset_version` |
| `train_dev_ratio` | 必填 | 從 train 日期內切給 train-dev 的 entity 比例 | `train_variant_id` |
| `enable_calibration` | 選填 | 是否建立 calibration keys 與 model input | `base_dataset_version` |
| `calibration_snap_dates` | 啟用時必填 | calibration 使用的日期 | `base_dataset_version` |
| `val_snap_dates` | 必填 | HPO validation 日期 | `base_dataset_version` |
| `test_snap_dates` | 必填 | 最終 test 日期 | `base_dataset_version` |

```yaml
dataset:
  train_snap_dates:
    - "2025-01-31"
    - "2025-02-28"
  train_dev_ratio: 0.1

  enable_calibration: true
  calibration_snap_dates:
    - "2025-11-30"

  val_snap_dates:
    - "2025-12-31"

  test_snap_dates:
    - "2026-01-31"
```

train、calibration、val、test 日期集合必須互斥。`train_dev_ratio` 不會切日期，而是依第一個 entity 欄位將該 entity 的所有日期與 items 一起分配至 train 或 train-dev，避免同一 entity 同時出現在兩側。

### 3.2 Train 分層抽樣

| 設定 | 預設 | 說明 | 版本影響 |
|---|---|---|---|
| `sample_ratio` | 無 | 未命中 override 時使用的 train 抽樣比例 | `train_variant_id` |
| `sample_group_keys` | `[time]` | 分層維度，順序也決定 override key 的組成方式 | `train_variant_id` ＋ `calibration_variant_id` |
| `sample_ratio_overrides` | `{}` | 各分層的抽樣比例覆寫 | `train_variant_id` |
| `random_seed` | `42` | 位於 `parameters.yaml`，控制決定性抽樣 | 目前未納入 dataset version hash |

多欄位分層會以 `|` 串接成 override key：

```yaml
dataset:
  sample_ratio: 1.0
  sample_group_keys:
    - cust_segment_typ
    - prod_name
    - label
  sample_ratio_overrides:
    "mass|ccard_ins|0": 0.5
    "affluent|ccard_ins|0": 0.9
```

抽樣使用 identity key、sampling site 與 `random_seed` 計算固定 CRC32 bucket。同一份資料與設定重跑會選出相同資料，不受 Spark partition 排列影響。未出現在 `sample_ratio_overrides` 的分層使用 `sample_ratio`。

override key 通常不建議手動輸入；使用 `scripts/sampling_overrides_editor.py` 可減少欄位順序、字串格式或不存在 item 導致規則沒有命中的風險。

#### Sample group key 的欄位來源

所有 `sample_group_keys` 都必須已存在於 `sample_pool`。抽樣 node 只讀取 `sample_pool`，不會為了取得分層欄位再連接 `feature_table`。若要使用客群、風險屬性等 feature 欄位分層，必須先在 `sample_pool_etl` SQL 中依 `time + entity` 連接 `feature_table`，將欄位寫入 `sample_pool`。

同一欄位是否還要保留在其他資料中，取決於它的用途：

| 用途 | 必須存在的位置 | Dataset 設定 |
|---|---|---|
| 只用於分層抽樣 | `sample_pool` | 加入 `sample_group_keys` |
| 同時作為模型特徵 | `sample_pool` 與 `feature_table` | 加入 `sample_group_keys`；類別特徵另加入 `categorical_columns`，連續特徵則不可放入 `drop_columns` |
| 同時作為 sample weight 維度 | `sample_pool` 與 train model input | 加入 `sample_group_keys`；若不是 identity、label 或 categorical feature，另加入 `carry_columns` |

例如 `cust_segment_typ` 只用於控制抽樣比例時，只需存在於 `sample_pool`；若模型也要使用它，則需保留在 `feature_table`，讓前處理與 model input 組裝能取得該欄位。完整的 `sample_pool` SQL 範例見 [`source_etl.md`](source_etl.md#sample-pool-需要包含抽樣欄位)。

### 3.3 Calibration 與 validation 抽樣

| 設定 | 預設 | 說明 | 版本影響 |
|---|---|---|---|
| `calibration_sample_ratio` | `1.0` | calibration 的預設抽樣比例 | `calibration_variant_id` |
| `calibration_sample_ratio_overrides` | `{}` | calibration 的分層比例覆寫 | `calibration_variant_id` |
| `val_sample_ratio` | `1.0` | 依 entity 縮減 val 母體 | `base_dataset_version` |

calibration 與 train 共用 `sample_group_keys`，但使用不同 sampling site，因此即使 seed 相同也不會刻意取得相同 bucket。test 不提供抽樣比例，會保留設定日期內的完整候選母體。

### 3.4 Carry columns

`carry_columns` 用來將 `sample_pool` 中不屬於 identity 的欄位帶入 train、train-dev 與 calibration model input，常見用途是提供 training 的 `sample_weight_keys`。

```yaml
dataset:
  carry_columns:
    - cust_segment_typ
```

注意事項：

- 欄位必須實際存在於 `sample_pool`。
- val 與 test keys 不會攜帶這些欄位。
- sample weights 只套用於 train 與 train-dev；calibration 即使帶有欄位也不加權。
- 修改 `carry_columns` 會改變 model input schema，因此會更新 `base_dataset_version`。

若 training 新增權重維度卻未將該欄位放入 identity、categorical features 或 `carry_columns`，CLI 設定閘會在 pipeline 啟動前阻擋。

### 3.5 前處理設定

```yaml
dataset:
  prepare_model_input:
    categorical_columns:
      - prod_name
      - gender
      - channel_preference
    drop_columns:
      - snap_date
      - cust_id
      - label
      - apply_start_date
      - apply_end_date
```

| 設定 | 說明 | 版本影響 |
|---|---|---|
| `categorical_columns` | 需要建立 category mapping 並轉為 integer encoding 的欄位 | `base_dataset_version` |
| `drop_columns` | 不應進入模型特徵的欄位 | `base_dataset_version` |

設定原則：

- `schema.item` 必須列在 `categorical_columns`，否則模型無法區分 query group 內的 items。
- 同一欄不可同時出現在 `categorical_columns` 與 `drop_columns`。
- 真正的連續數值特徵不需列入任一清單。
- 宣告為 categorical 的 feature 欄位不可是 Decimal、Double 或 Float；數字代碼應先在 source ETL 轉為 string 或 integer。
- 一般 categorical feature 不需設定 `schema.categorical_values`；其 category mapping 會從 `train_snap_dates` 範圍內的 `feature_table` 自動建立。
- identity categorical 若不在 `feature_table`，必須在 `parameters.yaml` 的 `schema.categorical_values` 明確提供完整值域。

preprocessor 只使用 `train_snap_dates` 範圍內的 feature rows fit category mapping，再將同一份 metadata 套用至 train、calibration、val、test 與 inference。未在 train 出現的新類別會編碼為 `-1` 並記錄 warning。

model input 寫出前，Decimal 與 Double 類型的 feature 會轉成 Spark `float`，降低後續 driver 讀取與模型訓練的記憶體成本。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--from-node <name>` | 無 | 從指定 node 與其後的 nodes 開始執行 |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開，不執行 pipeline |
| `--list-nodes` | 關閉 | 列出 node 名稱與從該處接續時的自動補跑成本 |

dataset 不接受版本旗標。每次啟動都會依目前設定、schema 與 `feature_table` schema 重新計算版本；指定既有 dataset 版本是下游 training 的責任。

`--from-node` 與 `--only-node` 互斥；`--list-nodes` 也不能與兩者併用。`--dry-run` 可單獨使用表示 full-run 計畫，也可搭配切片選項檢視部分重跑計畫。

`--dry-run` 與 `--list-nodes` 不會執行 nodes、寫入 pipeline 產物或更新 manifest；但 CLI 仍會載入設定、初始化 Spark、讀取 `feature_table` schema 以計算版本，並查詢 catalog 產物是否存在。

### 4.2 完整執行

```bash
python -m recsys_tfb dataset --env local
```

完整執行會包含最前方的設定與資料一致性檢查，適合以下情況：

- 第一次建立 dataset
- source tables 或資料日期有更新
- 修改 schema、前處理、日期、抽樣或 carry columns
- 不確定既有中間產物是否與目前設定一致

### 4.3 查看 nodes 與執行計畫

```bash
python -m recsys_tfb dataset --list-nodes

python -m recsys_tfb dataset \
  --from-node build_train_model_input \
  --dry-run
```

`--list-nodes` 會列出每個 node，以及從該處執行時可能因缺少輸入而自動補跑的上游 nodes。切片計畫會區分：

- requested：使用者指定且預期執行的 nodes
- auto-included：必要輸入不存在，框架自動補入的 producer nodes
- skipped：輸出可從 catalog 載入，因此略過的 nodes
- skipped side-effect：沒有輸出的守門 node，不會在接續時重新執行

### 4.4 從指定 node 接續

```bash
python -m recsys_tfb dataset \
  --from-node build_train_model_input
```

`--from-node` 使用拓撲順序語意：執行指定 node，以及拓撲序中位於其後的所有 nodes，而不只是該 node 的 dependency descendants。若指定 node 所需的上游資料已在 catalog 中持久化且存在，框架會直接讀取；若不存在，則遞迴補跑 producer，最壞情況退化為完整執行。

dataset 已明確維護的接續契約包括：

| 接續點 | 前次完整 run 成功時的預期行為 |
|---|---|
| `fit_preprocessor_metadata` | 直接讀取持久化來源與 keys，不必補跑前方 key-selection nodes |
| `build_train_model_input` | 直接讀取 `train_keys`、`preprocessed_feature_table`、`preprocessor` 與 `label_table` |

實際是否補跑仍以當次 `--dry-run` 計畫為準。

### 4.5 只執行單一 node

```bash
python -m recsys_tfb dataset \
  --only-node fit_preprocessor_metadata
```

`--only-node` 適合除錯或重新產生單一產物。若必要輸入缺少，框架仍會自動補入最小上游集合；它不會執行指定 node 的下游 consumers。

只要 pipeline 實際執行，CLI 仍會寫入 manifest 並更新 `latest` symlink。因此 `--only-node` 應視為進階維運工具：執行後必須確認該版本的其他必要產物原本已存在且仍然有效，不應用它建立一個從未完整成功過的新版本。

## 5. 執行流程

calibration nodes 只有在 `enable_calibration: true` 時加入。

| 階段 | node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 資料閘 | `validate_data_consistency` | 三張來源表、parameters | 檢查 item coverage 與 categorical feature 型別，收集問題後一次中止 | 無 |
| Train 抽樣 | `select_sample_keys` | `sample_pool` | 依 train 日期、分層比例與 overrides 做決定性抽樣 | `sample_keys` |
| Train 切分 | `split_train_keys` | `sample_keys` | 依 entity 將資料互斥切成 train 與 train-dev | `train_keys`、`train_dev_keys` |
| Val/Test keys | `select_val_keys`、`select_test_keys` | `sample_pool` | 建立 val 與 test identity keys；val 可依 entity 縮減 | `val_keys`、`test_keys` |
| Calibration keys | `select_calibration_keys` | `sample_pool` | 依 calibration 日期與比例抽樣 | `calibration_keys` |
| Fit 前處理器 | `fit_preprocessor_metadata` | `feature_table` | 只使用 train 日期建立 feature 清單與 category mappings | `preprocessor`、`category_mappings` |
| 套用前處理 | `apply_preprocessor_to_features` | `feature_table`、`preprocessor` | 篩選所有 split 日期、編碼 feature categoricals | `preprocessed_feature_table` |
| 組裝輸入 | `build_*_model_input` | keys、feature、label、preprocessor | left join label 與 feature，補齊缺失 label，選取欄位並轉 float32 | 各 split 的 model input |
| 評估母體過濾 | `filter_val_model_input`、`filter_test_model_input` | 未過濾的 val/test input | 移除整組沒有正例的 query groups | `val_model_input`、`test_model_input` |

model input 的組裝規則：

1. keys 與 `label_table` 依 `time + entity + item` left join；沒有 label row 時補為 `0`。
2. 再與 `preprocessed_feature_table` 依 `time + entity` left join。
3. 輸出 identity、label、feature columns，以及 keys 帶入的 carry columns。
4. val/test 才會移除零正例 query groups；train、train-dev 與 calibration 保留所有 rows。

## 6. 產物與驗收

### 6.1 主要產物

| 層級 | 產物 | 儲存方式 |
|---|---|---|
| Base | `preprocessor`、`category_mappings` | `data/dataset/<base_dataset_version>/` |
| Base | `preprocessed_feature_table`、`val_keys`、`test_keys`、`val_model_input`、`test_model_input` | Hive，以 `base_dataset_version` partition |
| Train variant | `sample_keys`、`train_keys`、`train_dev_keys`、`train_model_input`、`train_dev_model_input` | Hive，以 base + `train_variant_id` partition |
| Calibration variant | `calibration_keys`、`calibration_model_input` | Hive，以 base + `calibration_variant_id` partition |
| Metadata | base、train variant、calibration variant 的 `manifest.json` | 對應版本目錄 |
| Alias | 各層的 `latest` symlink | 指向最近完成的版本目錄 |

Hive 的實際 table 名稱與 partition 欄位以 `conf/base/catalog.yaml` 為準。

### 6.2 驗收重點

執行完成後至少確認：

1. log 中顯示的三層 version ID 符合預期。
2. `preprocessor.json` 的 `feature_columns` 包含 item，且欄位順序合理。
3. `category_mappings.json` 包含所有 categorical columns。
4. train 與 train-dev 都有資料，且同一 entity 不會同時出現在兩者。
5. model input 的 identity key 沒有重複，label 僅包含合法值。
6. val/test 每個保留的 query group 至少有一個正例。
7. carry columns 確實存在於 train/train-dev model input。

範例查詢：

```sql
SELECT COUNT(*)
FROM ml_recsys.recsys_prod_train_model_input
WHERE base_dataset_version = '<base_version>'
  AND train_variant_id = '<train_variant>';

SELECT snap_date, cust_id, COUNT(*) AS rows, SUM(label) AS positives
FROM ml_recsys.recsys_prod_val_model_input
WHERE base_dataset_version = '<base_version>'
GROUP BY snap_date, cust_id
HAVING SUM(label) <= 0;
```

第二個查詢應回傳零列。若 schema 的 entity 不只一欄，驗收 query group 時應使用全部 entity 欄位。

## 7. 版本、重跑與恢復

### 7.1 三層 dataset 版本

dataset 每次啟動都會計算以下版本：

| 版本 | 精確計算依據 | 主要產物 |
|---|---|---|
| `base_dataset_version` | `parameters_dataset.yaml` 中除了六個抽樣 keys 以外的所有內容，加上完整 schema 與 `feature_table` schema fingerprint | preprocessor、共用 feature、val/test |
| `train_variant_id` | 只包含 `sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio` | train/train-dev keys 與 inputs |
| `calibration_variant_id` | 只包含 `calibration_sample_ratio`、`calibration_sample_ratio_overrides`、`sample_group_keys` | calibration keys 與 input |

六個會從 base payload 排除的抽樣 keys 是：

```text
sample_ratio
sample_ratio_overrides
sample_group_keys
train_dev_ratio
calibration_sample_ratio
calibration_sample_ratio_overrides
```

除了這六個 keys，`parameters_dataset.yaml` 在 `dataset` 區塊新增的其他設定，預設都會納入 `base_dataset_version`。這是保守策略：新設定若可能改變 dataset 產物，會先讓 base version 翻新，避免不同內容共用版本。

每層使用 canonical YAML 計算 8 碼 SHA-256 hash。mapping 的 key 排列順序不影響 hash，但 list 的內容與順序會影響，例如重新排列 `sample_group_keys`、日期清單或 `categorical_values` 都會產生不同版本。

dataset 本身不接受指定版本的 CLI 旗標；執行時永遠以目前設定重新計算，training 再選擇要使用的既有版本。

### 7.2 設定版本矩陣

下表列出目前 `parameters_dataset.yaml` 的所有設定：

| 設定 | Base | Train variant | Calibration variant | 說明 |
|---|:---:|:---:|:---:|---|
| `train_snap_dates` | ✓ |  |  | 改變 fit preprocessor 與 train 資料時間範圍 |
| `sample_ratio` |  | ✓ |  | 只改變 train 抽樣 |
| `sample_ratio_overrides` |  | ✓ |  | 只改變 train 各分層抽樣 |
| `sample_group_keys` |  | ✓ | ✓ | train 與 calibration 共用分層 key，因此兩個 variant 都翻新 |
| `carry_columns` | ✓ |  |  | 改變 model input schema |
| `train_dev_ratio` |  | ✓ |  | 只改變 train/train-dev entity 切分 |
| `enable_calibration` | ✓ |  |  | 改變 pipeline 結構及是否建立 calibration 產物 |
| `calibration_snap_dates` | ✓ |  |  | 日期範圍屬於 base；不是 calibration 抽樣 variant |
| `calibration_sample_ratio` |  |  | ✓ | 只改變 calibration 抽樣 |
| `calibration_sample_ratio_overrides` |  |  | ✓ | 只改變 calibration 各分層抽樣 |
| `val_snap_dates` | ✓ |  |  | 改變 validation 資料 |
| `val_sample_ratio` | ✓ |  |  | val 屬於 base layer，不屬於 train sampling |
| `test_snap_dates` | ✓ |  |  | 改變 test 資料 |
| `prepare_model_input.drop_columns` | ✓ |  |  | 改變 feature 清單與 model input |
| `prepare_model_input.categorical_columns` | ✓ |  |  | 改變 category mappings、encoding 與 feature 清單 |

特殊情況：

- `enable_calibration: false` 時，CLI 不會計算或建立 `calibration_variant_id`。此時只修改 `calibration_sample_ratio` 或 `calibration_sample_ratio_overrides`，不會改變任何實際產生的 dataset version。
- 即使 `enable_calibration: false`，`calibration_snap_dates` 仍位於 base payload；修改它仍會翻新 `base_dataset_version`。
- `sample_group_keys` 同時進入 train 與 calibration variant；calibration 關閉時只會翻新 train variant。

### 7.3 設定檔外的版本因素

以下內容也會影響 `base_dataset_version`：

| 因素 | 是否翻新 Base | 說明 |
|---|:---:|---|
| `parameters.yaml` 的 `schema.columns` | ✓ | `time`、`entity`、`item`、`label`、`score`、`rank` 都納入 |
| `schema.categorical_values` | ✓ | 值與 list 順序都納入；改變 item 值域或 encoding 順序會翻新 |
| `feature_table` 欄位名稱 | ✓ | 新增或移除欄位都會改變 fingerprint |
| `feature_table` 欄位型別 | ✓ | 例如 `double` 改為 `float` |
| `feature_table` 欄位順序 | ✓ | feature 順序會傳入 preprocessor，因此 fingerprint 對順序敏感 |

以下內容目前**不會**改變任何 dataset version：

| 因素 | 為何不翻新 | 操作注意 |
|---|---|---|
| `parameters.yaml` 的 `random_seed` | 不在三層 hash payload | 會改變 train/train-dev、train sampling、calibration sampling 與 val sampling 結果；修改後應人工視為資料版本變更並完整重建 |
| `project_name`、`hive`、`spark`、`logging` | 不屬於 dataset hash 的 schema payload | 一般只影響執行環境或觀測性 |
| `conf/base/catalog.yaml` | catalog 設定不進 hash | 修改 table/path/partition 時需自行確認是否誤讀或覆寫既有版本 |
| `feature_table` 的資料值 | fingerprint 只看欄名、型別與順序 | 同 schema 的資料回補不會翻版，必須重跑相同版本 partitions |
| `label_table`、`sample_pool` 的資料值或 schema | 目前沒有對兩表計算 fingerprint | 上游回補、候選或 label 改變時需人工完整重跑 |
| source ETL SQL、dataset Python 程式碼 | 程式碼內容不進 hash | 程式修正後可能覆寫同一版本；manifest 的 git commit 只供追溯 |
| `parameters_training.yaml` | training 設定不參與 dataset IDs | 可能改變 `model_version`，但不重建 dataset |

`parameters_dataset.yaml` 以外的任意設定，除上述 schema payload 外，都不會自動影響 dataset version。

### 7.4 修改設定時要重跑什麼

| 修改內容 | 版本結果 | 建議 |
|---|---|---|
| train ratio、override、分層 keys、train-dev ratio | 新 train variant，base version 不變 | 完整執行最安全；熟悉切片者可依執行計畫只重建 train 路徑 |
| calibration ratio 或 override | 新 calibration variant，base/train version 不變 | 完整執行最安全；熟悉切片者可只重建 calibration 路徑 |
| 日期、calibration 開關、categorical/drop、carry columns | 新 base version | 完整執行 dataset |
| schema roles 或 item values | 新 base version | 先確認 source tables，再完整執行 dataset |
| `feature_table` 欄名、型別或順序 | 新 base version | 完整執行 dataset |
| source table 資料值回補，但 schema 不變 | version ID 可能不變 | 完整重跑受影響版本，避免沿用舊 partition |
| 全域 `random_seed` | 目前 version ID 不會自動改變 | 視為抽樣版本變更，清楚記錄並完整重建相關產物 |

三層版本描述的是產物身分與失效範圍，不是自動增量執行器。未使用切片旗標時，dataset 仍會執行完整 DAG，並覆寫相同版本 partitions。

任何 dataset ID 改變後，training 使用該組新版本時，`model_version` 也會隨之改變。`base_dataset_version` 翻新時，即使 `train_variant_id` 的 8 碼字串相同，它也會位於新的 base 目錄／partition 之下，兩者仍是不同的有效資料組合。

### 7.5 部分重跑的安全邊界

- catalog 的 `exists()` 只能確認產物存在，不能證明內容由目前參數或來源資料產生。
- dataset 的主要 Hive 產物具有版本 partitions，可降低設定改變後誤讀舊資料的風險；來源資料值回補與 seed 變更仍需人工判斷。
- `validate_data_consistency` 沒有輸出，若它位於切片起點之前便不會自動重跑。source tables 或 item 資料有變時應執行 full run。
- `val_model_input_unfiltered` 與 `test_model_input_unfiltered` 是記憶體中間結果；若只從 filter node 接續，框架會自動補跑對應 build node。
- 切片執行會在 manifest 記錄 `resumed_from` 或 `only_node`，供後續追溯。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| `Config consistency check failed`，item 不在 categorical columns | item 被 drop 或漏設為類別 feature | 將 item 加回 `categorical_columns`，並從 `drop_columns`／feature exclusion 移除 |
| categorical 與 drop 衝突 | 同一欄位同時出現在兩份清單 | 明確決定該欄要作為 feature 或排除 |
| override references unknown item | override key 中的 item 未宣告或拼錯 | 用 sampling editor 重建 key，並對齊 `schema.categorical_values` |
| weight column unavailable | training 權重維度未進入 model input | 將非 identity 欄位加入 `carry_columns` 後重跑 dataset |
| `Data consistency check failed`，sample_pool item 不一致 | `sample_pool` 缺少宣告 item，或含有未知 item | 檢查本次日期範圍的 distinct item，修正 source ETL 或 schema |
| categorical dtype 為 decimal/double/float | 連續值誤標類別，或代碼欄型別不適合 | 真正連續特徵移出 categorical；代碼欄在 source ETL cast 為 string/int |
| `Date splits overlap` | train/calibration/val/test 使用相同日期 | 重新切分日期，確保集合互斥 |
| `feature_table missing required ... snap_dates` | source ETL 未產出某些日期 | 補跑 feature ETL 或修正日期設定 |
| identity categorical missing declarations | item 等 identity 類別無法從 feature table fit | 在 `schema.categorical_values` 提供完整值域 |
| log 出現 `unknowns in column ...` | 非 train 日期出現 mapping 未見的新類別 | 檢查是否為資料異常；必要時延伸 train mapping 或調整來源清理 |
| 抽樣結果為空或某分層消失 | ratio/override 為 0、key 格式不符或母體太小 | 檢查 profiling、override key 順序與實際分層值 |
| `sample_group_keys` 欄位不存在 | 分層欄位只存在於 `feature_table`，未寫入 `sample_pool` | 在 `sample_pool_etl` SQL 連接來源欄位並重建 `sample_pool` |
| val/test 筆數比 sample pool 少很多 | 零正例 query groups 被預期移除 | 查詢 group 的 label sum；這是排序評估母體設計，不一定是錯誤 |
| `Unknown node ...` | node 名稱拼錯或 pipeline 已變更 | 先執行 `dataset --list-nodes` 取得目前名稱 |
| 切片計畫出現昂貴的 `auto-included` | 必要 artifact 不存在或 catalog 無法載入 | 先確認版本 partition 與檔案；不接受補跑成本時先停止修復 |
| 部分重跑後結果與設定不一致 | skipped artifacts 已過期，或資料閘被跳過 | 使用 full run，並比較 manifest、版本與 source data 更新時間 |
| Spark shuffle 或記憶體壓力過高 | 單一 partition 太大或 join shuffle 過重 | 檢查 `spark.sql.shuffle.partitions`、AQE、資料偏斜與 executor memory |

## 9. 限制與注意事項

- train/train-dev 切分與 val entity sampling 目前只使用 `schema.entity` 的第一個欄位；使用複合 entity 時需確認這符合業務語意。
- 日期只檢查集合互斥，不檢查時間順序與 label 觀察窗。
- `random_seed` 會改變抽樣結果，但目前未納入 dataset 版本 hash。
- 版本 hash 包含 `feature_table` schema fingerprint，不包含 source rows 的資料值或 source ETL SQL。
- `sample_pool` identity 唯一性由 source ETL 品質檢查負責；dataset 不會在抽樣前再次 deduplicate。
- label left join 不到時會視為負例 `0`；必須確定 sparse label table 的語意確實如此。
- feature left join 不到時可能留下 NULL feature，dataset 不會將其視為缺少 entity 的硬錯誤。
- val/test 會排除零正例 query groups，因此產物不代表完整上線母體。
- 多月份資料仍由 Spark lazy execution、shuffle spill 與 Hive partitions 處理；尖峰資源通常取決於單一 shuffle partition 與資料偏斜，而不是月份數本身。

## 10. 相關文件

- 三張來源表的建立方式：[`source_etl.md`](source_etl.md)
- 模型訓練與 dataset version 選擇：[`training.md`](training.md)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、前處理與恢復設計背景：[`../design-principles.md`](../design-principles.md)
