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
5. **類別欄位已人工確認**：可先使用 `scripts/suggest_categorical_cols.py` 依型別與 cardinality 產生候選清單——低 cardinality 欄建議進 `categorical_columns`、高 cardinality 字串欄進 `drop_columns`，其餘型別欄（date／timestamp／binary／複合型）另列一個待人工判斷的 review 區塊；再由你決定各欄歸屬（工具只建議、不改設定。輸出格式與大表加速選項見 §3.5）。
6. **抽樣設定已檢視**：可使用 `scripts/sampling_overrides_editor.py` 檢視各分層樣本量並產生 override。
7. **calibration 設定對齊**：若 dataset 啟用 calibration，training 端也應有相應設定；不需要將 score 解讀為機率時通常不必啟用。

> pipeline 只會檢查日期是否重疊，不會判斷 train、val、test 是否依時間正確排序，也無法自動識別特徵或 label 的未來資訊。

## 3. 設定方式

### 3.1 日期與 split

| 設定 | 必要性 | 說明 | 版本影響 |
|---|---|---|---|
| `train_snap_dates` | 必填 | fit preprocessor 與建立 train/train-dev 的日期 | `base_dataset_version` |
| `train_dev_ratio` | 必填 | 從 train 日期內切給 train-dev 的 entity 比例 | `train_variant_id` |
| `train_split_keys` | 選填 | 切分單位：`schema.entity` 的非空子集，預設完整 entity | `train_variant_id` |
| `enable_calibration` | 選填 | 是否建立 calibration keys 與 model input | `base_dataset_version` |
| `calibration_snap_dates` | 啟用時必填 | calibration 使用的日期 | `base_dataset_version` |
| `val_snap_dates` | 必填 | HPO validation 日期 | `base_dataset_version` |
| `test_snap_dates` | 必填 | 最終 test 日期 | 不影響任何版本（見 7.1） |

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

train、calibration、val、test 日期集合必須互斥（一致性不變量 A24，在 `dataset` 指令啟動 Spark 前檢查；按日比對而非按字面，同一天的不同寫法也算重疊）。日期本身仍須寫成 `YYYY-MM-DD`。`train_dev_ratio` 不會切日期，而是把一個 entity 的所有日期與 items 一起分配至 train 或 train-dev，避免同一 entity 同時出現在兩側。

「一個 entity」指哪些欄由 `train_split_keys` 宣告，**預設是完整的 `schema.entity`**。單欄 entity 下沒有第二種讀法；多欄時若你的洩漏單位比 query group 粗（例如 entity 是 `[cust_id, acct_id]`，而同一客戶的多個帳戶不得跨邊），就填上較粗的那個子集。填了不在 `entity` 裡的欄名會被不變量 A29 在 CLI 進入點擋下。為什麼這個鍵與 `val_sample_keys` 是兩個而不是一個，見 [ADR-0016](../adr/0016-split-unit-declared-by-two-keys.md)。

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

override key 通常不建議手動輸入；使用 `scripts/sampling_overrides_editor.py` 可減少欄位順序、字串格式或不存在 item 導致規則沒有命中的風險。用法、概念與 key 組法見 [`../operations/user-guides/sampling-overrides-editor.md`](../operations/user-guides/sampling-overrides-editor.md)。

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
| `val_sample_ratio` | `1.0` | 依 entity 縮減 val 母體（`conf/base` 目前設 `0.5`；這一欄是**程式碼的 fallback**，不是 conf 的值） | `base_dataset_version` |
| `val_sample_keys` | 完整 `entity` | 抽樣單位：`schema.entity` 的非空子集 | `base_dataset_version` |

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
- val 與 test keys 不會攜帶這些欄位。這不是疏漏：train／train-dev／calibration 走
  抽樣式的 key 選取（會帶 carry），val／test 只取 identity。sample weights 只作用於
  train 側，而 per-segment 評估是在 evaluation 階段另外從 `sample_pool` 取 segment，
  所以 val／test 不需要這些欄位。
- **若同一欄也存在於 `feature_table`，必須同時列入 `prepare_model_input.drop_columns`**
  ——否則 `build_model_input` 的 join 兩側各帶一份同名欄，Spark 會報一句看不出設定
  在哪寫錯的 `Reference 'x' is ambiguous`。反方向的修法（把該欄從 `carry_columns`
  拿掉）同樣合法，差別是前者保 carry 棄特徵、後者保特徵棄 carry。不變量 B7 會在
  dataset 的第一個 node 擋下並同時給出兩種修法（見
  [ADR-0004](../adr/0004-carry-drop-columns-intersection.md)）。identity 欄與 label
  不適用此規則——它們不會被複製第二份。
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
- 字串／非數值欄若要當特徵，**必須**列入 `categorical_columns`（會被 integer-encode）；否則**必須**列入 `drop_columns`。若未處理，該字串欄本會靜默變成 object-dtype 特徵並在訓練時 OOM；此情形現由不變量 B6 攔下（fail-fast）：dataset 建構的第一個 node（`validate_data_consistency`）會擋住，training 讀取時亦有 backstop（被點名之後怎麼決定，見 §8.1）。
  - ⚠ **該欄若同時列在 `carry_columns`，上面兩個選項只有 `drop_columns` 可用。** B6 的錯誤訊息會建議「宣告成 categorical 或 drop」，但對 carry 欄選前者只是把 B6 換成下一個錯誤：欄位留在 `feature_table` 側，`build_model_input` 兩側各帶一份，改撞 `Reference 'x' is ambiguous`。B7 會同時報出來（collect-all），但 B6 那則排在前面，由上往下照做會先繞一圈。判斷方式見 §3.4 的配對規則。
- 真正的連續數值特徵不需列入任一清單。
- 宣告為 categorical 的 feature 欄位不可是 Decimal、Double 或 Float；數字代碼應先在 source ETL 轉為 string 或 integer。
- 一般 categorical feature 不需設定 `schema.categorical_values`；其 category mapping 會從 `train_snap_dates` 範圍內的 `feature_table` 自動建立。
- identity categorical 若不在 `feature_table`，必須在 `parameters.yaml` 的 `schema.categorical_values` 明確提供完整值域。

#### 用 `suggest_categorical_cols.py` 產生候選

工具吃一個 Hive 表或 parquet 路徑，把 YAML 片段寫到 `data/profiling/<stem>_categorical.yaml`（供人工貼回上面的設定，不會自動改 config）。它把**每一個**欄位分類，不靜默漏欄：

- 低 cardinality 欄 → `categorical_columns:`；高 cardinality 字串欄 → `drop_columns:`（附 nunique）；
- date／timestamp／binary／複合型 → 一個**註解式 review 區塊**：這些同屬 object-dtype OOM 兇手（見上一則設定原則與 §8.1），但工具無法判該當 categorical 還是 drop，故只列出、由你把每欄搬進上面兩塊之一；
- 高 cardinality 數值欄留作連續特徵（不列入任一清單）。

terminal 摘要與 YAML 列出同一組欄位，並附一行對帳（例如 `8 columns = 2 categorical + 1 numeric-feature + 1 drop-suggested + 4 review`），可據此確認沒有欄位被漏掉。

**大表加速**（兩者可組合，皆為選用；預設全表掃描）：

- `--where "<Spark SQL 述語>"`：只掃符合述語的資料。述語引用**分區欄**時 Spark 會下推、跳過其他分區目錄（真正省 I/O）；引用非分區欄則只是 row filter。
- `--sample-fraction <比例>`（須 `0 < 比例 ≤ 1`，超出範圍會在起 Spark 前就報錯）：隨機抽樣（固定 seed、可重現）。省的是每欄 cardinality 估算，**不省 parquet I/O**（I/O 槓桿是 `--where`）。

> ⚠ `--where` 與 `--sample-fraction` 都只看**子集**，會**低估** cardinality——子集裡判為低卡的欄只是「至少這麼低」的下界，全表可能更高。因此 summary 會印出本次 scan scope，子集模式的 YAML 也在 `categorical_columns:` 頂加上一段「採用前請複查」的警告註解。（已被建議 `drop` 的高卡欄不受**此低估**影響——子集裡已超過門檻，代表全表也一定超過。）掃分散的多個分區、而非單一連續窗口，可降低「與分區鍵相關的欄」被藏住的風險。

preprocessor 只使用 `train_snap_dates` 範圍內的 feature rows fit category mapping，再將同一份 metadata 套用至 train、calibration、val、test 與 inference。未在 train 出現的新類別會編碼為 `-1` 並記錄 warning。

model input 寫出前，**所有數值 feature 欄**（decimal／double／float／整數族／boolean）都會轉成 `dataset.numeric_feature_storage_type` 宣告的型別（預設 float32），降低後續 driver 讀取與模型訓練的記憶體成本。收斂範圍涵蓋整數與 boolean 的理由：`pdf_to_X` 用 `DataFrame.values` 攤平，pandas 只挑一個共同 dtype，所以一欄沒轉就決定了整個矩陣的型別。

### 3.6 三個欄位清單各自作用在哪張表

`carry_columns`、`drop_columns`、`feature_columns` 常被當成同一件事的三種寫法，其實
**三者作用在不同的來源表、也在不同的 node 生效**。所以同一個欄名同時出現在
`carry_columns` 與 `drop_columns` 不是自相矛盾——當該欄同時存在於 `sample_pool` 與
`feature_table` 時，那是唯一可行的寫法（見 §3.4）。

| 設定鍵 | 作用對象 | 生效處 | 語意 |
|---|---|---|---|
| `prepare_model_input.drop_columns` | **`feature_table`** 的欄 | `compute_feature_columns` | 黑名單：不得成為模型特徵 |
| `carry_columns` | **`sample_pool`** 的欄 | `select_train_keys`／`select_calibration_keys` | 白名單：keys 除 identity 外還要多帶這些欄 |
| `feature_columns` | 推導結果，存進 `preprocessor.json` | `compute_feature_columns` | identity categoricals ＋（`feature_table` 欄 − drop − 非 categorical 的 identity 欄 − label） |

`feature_columns` **不是設定鍵**，沒有地方可以直接寫它；它是前兩者與 schema 推導出來
的結果。想增減特徵就改 `drop_columns` 或 `categorical_columns`。

**`drop_columns` 會物理刪欄，不只是「不當特徵」。** `apply_preprocessor_to_features`
只保留 `base_key ＋ 有出現在 feature_table 的 feature_columns`，所以被擋在
`feature_columns` 之外的欄根本不會寫進 `preprocessed_feature_table`。這正是同時
`carry` 又 `drop` 一個欄能運作的原因：`feature_table` 那一份被刪掉，只剩 keys 帶進來
的那一份，join 時就不會撞名。

各 split 最後拿到哪些欄，是一條推導規則而不是逐 split 的清單：

```
model_input.columns == identity ∪ {label} ∪ feature_columns ∪ (carry_columns ∩ 該 split keys 的欄)
```

train／train-dev／calibration 的 keys 帶 carry，val／test 不帶，所以同一條規則在不同
split 展開出不同的欄位集合（見 §3.4 與
[ADR-0004](../adr/0004-carry-drop-columns-intersection.md)）。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--rebuild-dates <d1,d2>` | 無 | 強制重算指定 test 月份（即使 partition 已存在）；值必須是 `test_snap_dates` 的子集 |
| `--only-test-months` | 關閉 | 宣告「這次只加評估月份」：只跑資料閘與 test 鏈，train／val／calibration 的產物不重算。與 `--from-node`／`--only-node` 正交、可併用；上游缺料時當場報錯。它是**模式**不是切片，差別見 §5.1 |
| `--from-node <name>` | 無 | 從指定 node 與其後的 nodes 開始執行 |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開，不執行 pipeline |
| `--list-nodes` | 關閉 | 列出 node 名稱與從該處接續時的自動補跑成本 |

dataset 不接受版本旗標。每次啟動都會依目前設定、schema 與 `feature_table` schema 重新計算版本；指定既有 dataset 版本是下游 training 的責任。

`--rebuild-dates` 的值不是 `test_snap_dates` 的子集時，在 Spark 啟動之前就報錯退出（一致性不變量 A21）。它與 `--from-node`／`--only-node` **可以併用**（切片選 node、rebuild 選月份，兩者正交），但併用時會印一段 WARN：未被選中的上游 node 不會重算，那些 partition 仍是舊的。用法與時機見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)。

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
- skipped side-effect：沒有輸出的守門 node，不會在接續時重新執行。**這一行走 warning**（不是 info）——這條 pipeline 的第一個 node `validate_data_consistency` 就在裡面，被跳過代表這一輪沒有檢查 Layer-2 資料層不變量

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
| Val/Test keys | `select_val_keys`、`select_test_keys` | `sample_pool`（test 另收 `test_keys_month_plan`） | 建立 val 與 test identity keys；val 可依 entity 縮減。test 只處理計畫中的月份 | `val_keys`、`test_keys` |
| Calibration keys | `select_calibration_keys` | `sample_pool` | 依 calibration 日期與比例抽樣 | `calibration_keys` |
| Fit 前處理器 | `fit_preprocessor_metadata` | `feature_table` | 只使用 train 日期建立 feature 清單與 category mappings | `preprocessor`、`category_mappings` |
| 套用前處理 | `apply_preprocessor_to_features` | `feature_table`、`preprocessor`、`preprocessed_feature_table_month_plan` | 編碼 feature categoricals；只處理計畫中的月份 | `preprocessed_feature_table` |
| 精度閘 | `validate_numeric_precision` | `preprocessed_feature_table`、`preprocessor`、`preprocessed_feature_table_month_plan` | 不變量 B8：讀剛落地那幾個月份的 parquet footer 統計值（零掃描），確認會被 cast 的欄（decimal、整數族與 boolean——有格點的那些）在該欄自己的解析度下撐得過 `numeric_feature_storage_type`；同時產出每欄的 headroom 報告 | `numeric_precision_report` |
| 組裝輸入 | `build_*_model_input` | keys、feature、label、preprocessor（test 另收 `test_model_input_month_plan`） | left join label 與 feature，補齊缺失 label，選取欄位並把所有數值特徵欄轉成 `numeric_feature_storage_type` 宣告的型別（預設 float32） | 各 split 的 model input |
| 評估母體過濾 | `filter_val_model_input`、`filter_test_model_input` | 未過濾的 val/test input | 移除整組沒有正例的 query groups | `val_model_input`、`test_model_input` |

model input 的組裝規則：

1. keys 與 `label_table` 依 `time + entity + item` left join；沒有 label row 時補為 `0`。
2. 再與 `preprocessed_feature_table` 依 `time + entity` left join。
3. 輸出 identity、label、feature columns，以及 keys 帶入的 carry columns。
4. val/test 才會移除零正例 query groups；train、train-dev 與 calibration 保留所有 rows。

#### 兩個 left join 各自的契約

兩個 join 都是 left，而且**列數恆等於 keys 的列數**——keys 的 grain 就是 model input
的 grain。這一點是後續所有列數斷言的地基，改成 inner join 會靜默改變列數，也會讓 mAP
的候選集跟著變。

| join miss | 產生什麼 | 為什麼這是預期行為 |
|---|---|---|
| `label_table` 沒有這筆 | `label` 補 `0` | label table 是稀疏的：只有發生過交易的 entity 才有 row，沒有 row 就是負例 |
| `preprocessed_feature_table` 沒有這筆 | 該列的 feature 欄全為 NULL，**列仍保留** | `sample_pool` 與 `feature_table` 的母體來自不同上游，miss 是結構性的常態；LightGBM 自行處理 missing |

**全 NULL 特徵列是合法輸出，不是 bug。** 看到它不代表資料壞了，代表這個
`(time, entity)` 在 `feature_table` 裡沒有對應 row。目前刻意不加覆蓋率閘門：真實
miss 率只有在生產跑過一次才知道，本機量不到，所以「先量再決定」這一輪無法執行。日後
要量，量測點在 `build_model_input` 產出之後（量實際進了 model input 的東西），而不是
在 `sample_pool` 的 ETL 端取代理值。完整理由與被否決的替代方案見
[ADR-0005](../adr/0005-model-input-degenerate-state-contracts.md)。

### 5.1 test 分支是增量的

`apply_preprocessor_to_features`、`select_test_keys`、`build_test_model_input` 三個 node 只處理**尚未落地**的月份。train／train-dev／val／calibration **不是**增量的：它們一旦被執行就整批重算，把逐位元相同的內容覆寫回同一批 partition。

省掉那次重算的方法是**不執行它們**，不是讓它們變成增量的。`--only-test-months` 就是這樣做的——它是 `create_pipeline` 的**模式**參數（不是切片），只組出資料閘加上 test 鏈，其餘節點根本不進 pipeline；留下哪些節點以 `pipelines/dataset/pipeline.py` 的 `ONLY_TEST_MONTHS_NODES` 為準。反過來說，**增量性與這個旗標無關**：上面三個 node 帶不帶旗標都只處理尚未落地的月份，旗標改的是節點集，不是增量性。模式與切片的分工見 [ADR-0013](../adr/0013-pipeline-modes-and-slicing-are-separate.md)，使用動線見[新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)。

怎麼看出誰是增量的：**pipeline 定義上有 `*_month_plan` input 的就是**。CLI 在任何 Spark 工作開始之前列一次 metastore partition（零掃描）、算出三份計畫（`month_plans.build_month_plans`），以 `<產物名>_month_plan` 這三個名字放進 catalog；節點把它當一般 input 收下。所以：

- 「這次處理／跳過哪些月」在 pipeline 開跑前就以三行 `[months]` log 印出來，範圍設錯可以在花掉時間之前發現；
- 忘記提供計畫不會靜默全量重建——runner 在第一個節點執行前就 raise；
- 每張表吃自己那份計畫（`test_keys` 已寫、`test_model_input` 還沒，是正常狀態）。

`test_model_input` 的過濾節點（`filter_test_model_input`）**沒有**月份範圍檢查，跟 val 用同一個節點函式：它的上游已經 scoped 過了。

之所以安全：每個 `snap_date` partition 的內容只是該月 `feature_table` rows 與 `category_mappings` 的函數，與其他月份無關，而 `category_mappings` 只在 train 月份上 fit。所以跳過既有月份不改變任何 partition 的內容，只改變這次要做多少工。

代價、`--rebuild-dates` 逃生口與完整理由見 [ADR-0002](../adr/0002-preprocessed-feature-table-incremental.md)；計畫為什麼走 catalog 而不是 `parameters`，以及過濾節點為什麼沒有防禦性檢查，見 [ADR-0007](../adr/0007-month-plans-travel-through-the-catalog.md)。

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
| `base_dataset_version` | `parameters_dataset.yaml` 中除了七個抽樣 keys 與 `test_snap_dates` 以外的所有內容，加上完整 schema 與 `feature_table` schema fingerprint | preprocessor、共用 feature、val/test |
| `train_variant_id` | 只包含 `sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio`、`train_split_keys` | train/train-dev keys 與 inputs |
| `calibration_variant_id` | 只包含 `calibration_sample_ratio`、`calibration_sample_ratio_overrides`、`sample_group_keys` | calibration keys 與 input |

會從 base payload 排除的抽樣 keys 有七個：

```text
sample_ratio
sample_ratio_overrides
sample_group_keys
train_dev_ratio
train_split_keys
calibration_sample_ratio
calibration_sample_ratio_overrides
```

`val_sample_keys` **刻意不在這份清單裡**：val 產物只由 `base_dataset_version` 分割，把它排除掉就等於讓 val 的抽樣單位改了卻靜默沿用舊 parquet。推導見 [ADR-0016](../adr/0016-split-unit-declared-by-two-keys.md)。

除了這七個 keys，還有第八個被排除的 key —— `test_snap_dates`：

```text
test_snap_dates
```

它被排除的理由與抽樣 keys 不同。抽樣 keys 是因為「另有一層 variant ID 承接」；`test_snap_dates` 則是因為**它不定義產物身分，只定義資料覆蓋範圍**。test 資料不進任何模型擬合（`val` 驅動 early stopping、`calibration` 決定校準後輸出，兩者都留在 base payload 裡），所以在 `test_snap_dates` 加一個月份時：

- `base_dataset_version` 與 `model_version` 都不變，因此**不需要重訓**；
- 新月份的 test 產物以 dynamic partition 寫入，既有月份的產物與評估報表原封不動（累積語意）；
- 新舊月份的評估報表並存於同一個模型身分之下，可直接比較。

代價是 `parameters_dataset.yaml` 不再是 test 覆蓋範圍的唯一真實來源 —— 同一個 `base_dataset_version` 底下的月份會隨時間累積，**實際有哪些月份要以 Hive partition 為準**（`SHOW PARTITIONS`）；manifest 只記錄**最後一次執行**當下的設定，每次執行覆寫，因此讀不出累積的覆蓋範圍。完整推導與否決過的選項見 [ADR-0001](../adr/0001-test-dates-out-of-dataset-version-identity.md)；操作步驟見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)。

除了上述八個 keys，`parameters_dataset.yaml` 在 `dataset` 區塊新增的其他設定，預設都會納入 `base_dataset_version`。這是保守策略：新設定若可能改變 dataset 產物，會先讓 base version 翻新，避免不同內容共用版本。

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
| `train_split_keys` |  | ✓ |  | 只改變 train/train-dev 的切分單位；val/test 產物完全不動 |
| `enable_calibration` | ✓ |  |  | 改變 pipeline 結構及是否建立 calibration 產物 |
| `calibration_snap_dates` | ✓ |  |  | 日期範圍屬於 base；不是 calibration 抽樣 variant |
| `calibration_sample_ratio` |  |  | ✓ | 只改變 calibration 抽樣 |
| `calibration_sample_ratio_overrides` |  |  | ✓ | 只改變 calibration 各分層抽樣 |
| `val_snap_dates` | ✓ |  |  | 改變 validation 資料 |
| `val_sample_ratio` | ✓ |  |  | val 屬於 base layer，不屬於 train sampling |
| `val_sample_keys` | ✓ |  |  | 同上；不登記進 train sampling，否則 val 會靜默沿用舊資料 |
| `test_snap_dates` |  |  |  | 只改變 test 覆蓋範圍，不改變任何產物身分（見 7.1） |
| `prepare_model_input.drop_columns` | ✓ |  |  | 改變 feature 清單與 model input |
| `prepare_model_input.categorical_columns` | ✓ |  |  | 改變 category mappings、encoding 與 feature 清單 |

特殊情況：

- `enable_calibration: false` 時，CLI 不會計算或建立 `calibration_variant_id`。此時只修改 `calibration_sample_ratio` 或 `calibration_sample_ratio_overrides`，不會改變任何實際產生的 dataset version。
- 即使 `enable_calibration: false`，`calibration_snap_dates` 仍位於 base payload；修改它仍會翻新 `base_dataset_version`。
- `sample_group_keys` 同時進入 train 與 calibration variant；calibration 關閉時只會翻新 train variant。
- `test_snap_dates` 是唯一一個「改了卻不翻新任何版本」的日期設定。改動它之後 dataset 會在**同一個** `base_dataset_version` 底下補上新月份的 partition；既有月份不受影響，也不需要重訓。

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
| train／val／calibration 日期、calibration 開關、categorical/drop、carry columns | 新 base version | 完整執行 dataset |
| 只在 `test_snap_dates` 加一個月份 | 版本全部不變 | 執行 dataset 補上新月份，再跑 predict 與該月份的 evaluation；不重訓。步驟見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md) |
| schema roles 或 item values | 新 base version | 先確認 source tables，再完整執行 dataset |
| `feature_table` 欄名、型別或順序 | 新 base version | 完整執行 dataset |
| source table 資料值回補，但 schema 不變 | version ID 可能不變 | 完整重跑受影響版本，避免沿用舊 partition |
| 全域 `random_seed` | 目前 version ID 不會自動改變 | 視為抽樣版本變更，清楚記錄並完整重建相關產物 |

三層版本描述的是產物身分與失效範圍，不是自動增量執行器。未帶任何**模式**或**切片**旗標時（模式＝`--only-test-months`，切片＝`--from-node`／`--only-node`），dataset 仍會執行完整 DAG，並覆寫相同版本 partitions。

任何 dataset ID 改變後，training 使用該組新版本時，`model_version` 也會隨之改變。`base_dataset_version` 翻新時，即使 `train_variant_id` 的 8 碼字串相同，它也會位於新的 base 目錄／partition 之下，兩者仍是不同的有效資料組合。

### 7.5 部分重跑的安全邊界

- catalog 的 `exists()` 只能確認產物存在，不能證明內容由目前參數或來源資料產生。**test 分支的增量跳過把這件事變成了正常執行路徑的預設行為**：`feature_table` 對某個舊 test 月份回補之後，該月 partition 不會自動更新且不報錯，得用 `--rebuild-dates` 指名重算（[ADR-0002](../adr/0002-preprocessed-feature-table-incremental.md)）。
- dataset 的主要 Hive 產物具有版本 partitions，可降低設定改變後誤讀舊資料的風險；來源資料值回補與 seed 變更仍需人工判斷。
- `validate_data_consistency` 沒有輸出，若它位於切片起點之前便不會自動重跑。source tables 或 item 資料有變時應執行 full run。
- `validate_numeric_precision` 有輸出（`numeric_precision_report`），所以**不會**被當成側效應 node 跳過；但沒有任何 node 消費那份報告，所以它也不會被自動拉回來——切片起點在它之後就不會跑到它。
- `val_model_input_unfiltered` 與 `test_model_input_unfiltered` 是記憶體中間結果；若只從 filter node 接續，框架會自動補跑對應 build node。
- 切片執行會在 manifest 記錄 `resumed_from` 或 `only_node`，供後續追溯。
- 開跑前 CLI 會對 base、train variant、calibration variant 各先寫一份 `status: running` 的 `manifest.json` stub（崩潰溯源用，**不**更新 `latest` symlink，也不覆寫既有 manifest），成功完成後再覆寫為 `status: completed` 並更新 `latest`；`--dry-run` / `--list-nodes` 不寫 stub。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| `Config consistency check failed`，item 不在 categorical columns | item 被 drop 或漏設為類別 feature | 將 item 加回 `categorical_columns`，並從 `drop_columns`／feature exclusion 移除 |
| categorical 與 drop 衝突 | 同一欄位同時出現在兩份清單 | 明確決定該欄要作為 feature 或排除 |
| override references unknown item | override key 中的 item 未宣告或拼錯 | 用 sampling editor 重建 key，並對齊 `schema.categorical_values` |
| weight column unavailable | training 權重維度未進入 model input | 將非 identity 欄位加入 `carry_columns` 後重跑 dataset |
| `Data consistency check failed`，sample_pool item 不一致 | `sample_pool` 缺少宣告 item，或含有未知 item | 檢查本次日期範圍的 distinct item，修正 source ETL 或 schema |
| `DataConsistencyError: ... un-encoded non-numeric type(s)`，讀 parquet 前秒級失敗 | 字串／非數值欄進了 `feature_columns`，既沒宣告 categorical 也沒 drop（不變量 B6） | 錯誤訊息逐欄點名兇手；每欄二選一，見下方 §8.1。改完會 bump `base_dataset_version`、需重建 dataset |
| categorical dtype 為 decimal/double/float | 連續值誤標類別，或代碼欄型別不適合 | 真正連續特徵移出 categorical；代碼欄在 source ETL cast 為 string/int |
| `(A24) dataset.X_snap_dates [...] and dataset.Y_snap_dates [...] name the same calendar day` | train/calibration/val/test 使用相同日期 | 重新切分日期，確保集合互斥。此檢查在 Spark 啟動前執行，**按日比對而非按字面**，所以同一天的不同寫法也抓得到；訊息會分別印出兩邊各自的原始寫法 |
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

### 8.1 B6 點名之後，怎麼決定每一欄

B6 擋下來時，錯誤訊息會**逐欄點名**（`feature column 'cust_segment' is non-numeric and is not declared categorical...`）。對每一個被點名的欄，二選一：

- **是有用的類別特徵**（例：客群別、通路）→ 加進 `dataset.prepare_model_input.categorical_columns`。它會在 Spark 端就被編成整數，仍是模型特徵。
- **不是模型特徵**（例：ID、自由文字）→ 加進 `dataset.prepare_model_input.drop_columns`。

> ⚠ **這會 bump `base_dataset_version`，需要重建整個 dataset**——兩個鍵都參與 dataset 版本雜湊。閘門本身只讓你**知道是哪幾欄**、並防止未來重建時再犯，不會替你改 config。

`scripts/suggest_categorical_cols.py` 可以加速這個決定（用法見 §3）：它把高 cardinality 字串欄建議進 `drop_columns`，並把 date／timestamp／binary／複合型欄放進待人工判斷的 review 區塊——那些同屬 object-dtype OOM 兇手，但工具不替你決定該 categorical 還是 drop。

**不處理會怎樣**：該欄會原封不動穿過整條 dataset pipeline 成為特徵，training 讀取時整張矩陣塌縮成 object dtype（每格 ~34 B vs float64 8 B），在 `to_numpy` 被 OOM killer 殺掉。合成資料不含這類欄位，所以**本機永遠不會重現，生產環境必爆**。事故全貌見 [2026-07 調查紀錄](../notes/2026-07-11-training-oom-investigation.md)。

## 9. 限制與注意事項

- train/train-dev 切分與 val entity sampling 目前只使用 `schema.entity` 的第一個欄位；使用複合 entity 時需確認這符合業務語意。
- 日期只檢查集合互斥，不檢查時間順序與 label 觀察窗。
- `random_seed` 會改變抽樣結果，但目前未納入 dataset 版本 hash。
- 版本 hash 包含 `feature_table` schema fingerprint，不包含 source rows 的資料值或 source ETL SQL。
- `sample_pool` identity 唯一性由 source ETL 品質檢查負責；dataset 不會在抽樣前再次 deduplicate。
- label left join 不到時會視為負例 `0`；必須確定 sparse label table 的語意確實如此。
- feature left join 不到時會留下全 NULL feature 的列，dataset 不會將其視為缺少 entity 的硬錯誤。這是明文契約而非容忍，代價是「特徵缺失」與「特徵值真的是 NULL」在 model input 裡無法區分；契約與量測點見 §5。
- val/test 會排除零正例 query groups，因此產物不代表完整上線母體。
- 多月份資料仍由 Spark lazy execution、shuffle spill 與 Hive partitions 處理；尖峰資源通常取決於單一 shuffle partition 與資料偏斜，而不是月份數本身。

## 10. 相關文件

- 三張來源表的建立方式：[`source_etl.md`](source_etl.md)
- 模型訓練與 dataset version 選擇：[`training.md`](training.md)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、前處理與恢復設計背景：[`../design-principles.md`](../design-principles.md)
