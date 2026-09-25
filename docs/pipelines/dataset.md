# dataset pipeline

> 將 `feature_table`、`label_table` 與 `sample_pool` 轉換為 train、train-dev、validation 與 test 所需的模型輸入。
> 主要流程為：資料一致性檢查 → 日期切分與抽樣 → fit 前處理器 → 組裝各 split 的 `*_model_input`。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 建立版本化的資料切分、前處理器與模型輸入 |
| 執行指令 | `python -m recsys_tfb dataset` |
| 上游輸入 | `feature_table`、`label_table`、`sample_pool`；選用的 `candidate_feature_table`（§3.8） |
| 主要輸出 | `preprocessor`、`*_keys`、`*_model_input` |
| 設定檔 | `conf/base/parameters_dataset.yaml` |
| I/O 設定 | `conf/base/catalog.yaml` |
| 下游 pipeline | `training` |

各 split 的用途如下：

| split | 資料來源 | 用途 |
|---|---|---|
| `train` | `train_snap_dates` 內抽樣後的大部分 entity | 模型訓練 |
| `train_dev` | 與 train 相同日期，依 `train_dev_ratio` 切出的 entity | 單次模型訓練的 early stopping |
| `val` | `val_snap_dates` | HPO 跨 trials 選擇最佳超參數 |
| `test` | `test_snap_dates` | 模型完成後的最終離線評估 |

`train` 與 `train_dev` 共用同一段日期，並以 entity 做互斥切分；val 與 test 則使用各自的時間區間。

## 2. 執行前準備

執行 dataset 前，建議依序確認：

1. **來源表已就緒**：`feature_table` 與 `sample_pool` 必須涵蓋所有設定日期；宣告了候選層級特徵表時，它必須涵蓋本次執行要讀的每個月份（§3.8）；`label_table` 可以是只保存正例的 sparse table，但 label 觀察窗必須成熟。
2. **schema 角色正確**：`conf/base/parameters.yaml` 的 `time`、`entity`、`item` 與 `label` 必須對應實際欄位。
3. **item 集合一致**：item 清單逐一列出時，`sample_pool` 在本次日期範圍內的 item 集合必須與 `schema.categorical_values.<item>` 完全一致；`label_table` 不可產生未宣告 item。item 清單從資料數時（§3.10），`label_table` 的 item 必須在 `sample_pool` 出現過。
4. **日期切分互斥**：train、val 與 test 日期不可重疊，並應由使用者依時間先後安排，避免資料洩漏。
5. **類別欄位已人工確認**：可先使用 `scripts/suggest_categorical_cols.py` 依型別與 cardinality 產生候選清單——低 cardinality 的字串／布林／整數欄建議進 `categorical_columns`、高 cardinality 字串欄進 `drop_columns`，其餘型別欄（date／timestamp／binary／複合型）另列一個待人工判斷的 review 區塊（它們不能當類別欄，只能 drop 或回 source ETL 轉換）；再由你決定各欄歸屬（工具只建議、不改設定。輸出格式與大表加速選項見 §3.5）。
6. **抽樣設定已檢視**：可使用 `scripts/sampling_overrides_editor.py` 檢視各分層樣本量並產生 override。

> pipeline 只會檢查日期是否重疊，不會判斷 train、val、test 是否依時間正確排序，也無法自動識別特徵或 label 的未來資訊。

## 3. 設定方式

### 3.1 日期與 split

| 設定 | 必要性 | 說明 | 版本影響 |
|---|---|---|---|
| `train_snap_dates` | 必填 | fit preprocessor 與建立 train/train-dev 的日期 | `base_dataset_version` |
| `train_dev_ratio` | 必填 | 從 train 日期內切給 train-dev 的 entity 比例 | `train_variant_id` |
| `train_split_keys` | 選填 | 切分單位：`schema.entity` 的非空子集，預設完整 entity | `train_variant_id` |
| `val_snap_dates` | 必填 | HPO validation 日期 | `base_dataset_version` |
| `test_snap_dates` | training 必填（至少一個月，A36）；dataset 沒寫或空清單都照樣跑 | 最終 test 日期 | 不影響任何版本（見 7.1） |

```yaml
dataset:
  train_snap_dates:
    - "2025-01-31"
    - "2025-02-28"
  train_dev_ratio: 0.1

  val_snap_dates:
    - "2025-12-31"

  test_snap_dates:
    - "2026-01-31"
```

四個 `*_snap_dates` 都可以改寫成「起日～迄日」區間，不必逐一列出：

```yaml
dataset:
  train_snap_dates:
    start: "2025-01-31"
    end: "2025-10-31"
    step: month_end      # day | week | month_start | month_end
```

- **含頭含尾，而且起日與迄日都必須落在 `step` 上。** 上例展開成 2025-01-31、2025-02-28……2025-10-31 共 10 個日期。`week` 從起日起每 7 天一個，迄日必須剛好是起日加整數週；`month_start`／`month_end` 的起迄必須是月初／月底。沒落在 step 上時（例如 `month_end` 卻寫 `2025-01-30`）直接報錯、指令退出，**不會**自動挪到最近的日期——挪了就等於悄悄換掉一整段切分。
- **區間與逐一列出是同一份設定。** 設定檔一載入就把區間展開成 `YYYY-MM-DD` 字串清單（`core/date_ranges.py`），之後所有檢查、所有 node、版本雜湊看到的都是清單，所以兩種寫法得到**同一個 `base_dataset_version`**。
- **前提：原本的清單是「加引號、依日期遞增」的寫法**（上面的示例就是）。清單照寫的樣子進雜湊，不會被排序或補引號——那樣做會讓所有既有的版本 ID 一起變。所以如果你原本的清單順序不同、或日期沒加引號（YAML 會讀成日期物件），改寫成區間會翻一次 `base_dataset_version`、重建一次產物；之後就穩定了。
- 區間沒有「最近 N 天」這種相對寫法：迄日若跟著執行日期走，同一份設定明天重跑會得到不同的版本 ID。
- `conf/<env>/` 的覆蓋層可以只蓋區間裡的一個鍵（例如只改 `end`），因為展開發生在覆蓋之後。

train、val、test 日期集合必須互斥（一致性不變量 A24，在 `dataset` 指令啟動 Spark 前檢查；按日比對而非按字面，同一天的不同寫法也算重疊）。日期本身仍須寫成 `YYYY-MM-DD`。`train_dev_ratio` 不會切日期，而是把一個 entity 的所有日期與 items 一起分配至 train 或 train-dev，避免同一 entity 同時出現在兩側。

來源表的 time 欄可以是 DATE、TIMESTAMP，或寫成 `YYYY-MM-DD` 的字串（Hive 的分區欄讀回來就是字串）。dataset 每一處按月份篩選，都先把 time 欄轉成日期、再和設定的日期比，所以三種型別讀到的月份相同（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 3）。TIMESTAMP 帶時分的，算它那一天。字串寫成別的格式（`2025/01/31`）一個月份都對不到，月份存在檢查會報錯。2026-09 之前不是這樣：time 欄是字串（或 TIMESTAMP 帶時分）時，抽樣、fit 詞表、從資料數 item 清單一個月份都對不到而且不報錯，資料閘的 item 檢查則一個 item 都看不到。

「一個 entity」指哪些欄由 `train_split_keys` 宣告，**預設是完整的 `schema.entity`**。單欄 entity 下沒有第二種讀法；多欄時若你的洩漏單位比 query group 粗（例如 entity 是 `[cust_id, acct_id]`，而同一客戶的多個帳戶不得跨邊），就填上較粗的那個子集。填了不在 `entity` 裡的欄名會被不變量 A29 在 CLI 進入點擋下。為什麼這個鍵與 `val_sample_keys` 是兩個而不是一個，見 [ADR-0016](../adr/0016-split-unit-declared-by-two-keys.md)。

**entity 任一欄是 NULL 的列，三個 split 選 key 時都會丟掉，並印一行警告**，寫明是哪個 split 的 keys、每一欄各有幾列 NULL（例如 `val keys: dropped 2 row(s) whose entity is NULL (NULL by column: cust_id=2)`）。這種列不屬於任何 entity，接不到特徵，也接不到 label。
- train 在 `split_train_keys` 查，看的是已抽樣、落地的 `sample_keys`，而且看完整的 `schema.entity`，不只切分單位。
- val、test 在各自選 key 時查，只看自己的月份（test 只看這次處理的月份）；val 在抽樣之前查。
- 只警告、不中止：這項資料品質歸上游 source ETL 的 `primary_key_not_null`（[ADR-0006](../adr/0006-data-quality-checks-belong-upstream.md)）。

2026-09 之前只有 train 會丟並警告，而且只看切分單位：切分單位以外的 entity 欄是 NULL 的列會留在 train。val 抽樣時，抽樣單位（`val_sample_keys`）的欄是 NULL 的列會被默默丟掉，其他 entity 欄是 NULL 的列則留著；留著的這些、val 不抽樣時的、以及 test 的，都留到丟無正例組那一步，`*_zero_positive_group_ratio` 是 0 時在那裡被默默丟掉（它接不到 label，算無正例組），大於 0 時可能留進 model_input（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 5）。

### 3.2 Train 分層抽樣

| 設定 | 預設 | 說明 | 版本影響 |
|---|---|---|---|
| `sample_ratio` | 無 | 未命中 override 時使用的 train 抽樣比例 | `train_variant_id` |
| `sample_group_keys` | `[time]` | 分層維度，順序也決定 override key 的組成方式 | `train_variant_id` |
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

### 3.3 Validation 抽樣

| 設定 | 預設 | 說明 | 版本影響 |
|---|---|---|---|
| `val_sample_ratio` | `1.0` | 依 entity 縮減 val 母體（`conf/base` 目前設 `0.5`；這一欄是**程式碼的 fallback**，不是 conf 的值） | `base_dataset_version` |
| `val_sample_keys` | 完整 `entity` | 抽樣單位：`schema.entity` 的非空子集 | `base_dataset_version` |

test 不提供抽樣比例，會保留設定日期內的完整候選母體。

### 3.4 Carry columns

`carry_columns` 用來將 `sample_pool` 中不屬於 identity 的欄位帶入 train 與 train-dev model input，常見用途是提供 training 的 `sample_weight_keys`。

```yaml
dataset:
  carry_columns:
    - cust_segment_typ
```

注意事項：

- 欄位必須實際存在於 `sample_pool`。
- val 與 test keys 不會攜帶這些欄位。這不是疏漏：train／train-dev 走
  抽樣式的 key 選取（會帶 carry），val／test 只取 identity。sample weights 只作用於
  train 側，而 per-segment 評估是在 evaluation 階段另外從 `sample_pool` 取 segment，
  所以 val／test 不需要這些欄位。
- **若同一欄也存在於 `feature_table`（或候選層級特徵表，§3.8），必須同時列入 `prepare_model_input.drop_columns`**
  ——否則 `build_model_input` 的 join 兩側各帶一份同名欄，Spark 會報一句看不出設定
  在哪寫錯的 `Reference 'x' is ambiguous`。反方向的修法（把該欄從 `carry_columns`
  拿掉）同樣合法，差別是前者保 carry 棄特徵、後者保特徵棄 carry。不變量 B7 會在
  dataset 的第一個 node 擋下並同時給出兩種修法（見
  [ADR-0004](../adr/0004-carry-drop-columns-intersection.md)）。identity 欄與 label
  不適用此規則——它們不會被複製第二份。
- 修改 `carry_columns` 只更新 `train_variant_id`：只有 train 與 train-dev 的 keys 與 model input 帶 carry 欄，base 底下的產物（前處理器、val／test）一欄都不變（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 9）。同一欄若也在 `feature_table`，上一條要求它列進 `drop_columns`，那一個改動會更新 `base_dataset_version`。

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
| `categorical_columns` | 需要建立 category mapping 並轉為 integer encoding 的欄位。沒寫時只有 `schema.item` | `base_dataset_version` |
| `drop_columns` | 不應進入模型特徵的欄位。沒寫時只有 time、entity、label 三個 schema 角色的欄；其他不是特徵的欄（例如上面的 `apply_start_date`）要自己列 | `base_dataset_version` |

設定原則：

- `schema.item` 必須列在 `categorical_columns`，否則模型無法區分 query group 內的 items。
- 同一欄不可同時出現在 `categorical_columns` 與 `drop_columns`。
- 字串欄若要當特徵，**必須**列入 `categorical_columns`（會被 integer-encode）；否則**必須**列入 `drop_columns`。date／timestamp／binary／複合型欄不能當 categorical（見下方型別規則），要當特徵只能先在 source ETL 轉成數值或字串，否則列入 `drop_columns`。若未處理，該字串欄本會靜默變成 object-dtype 特徵並在訓練時 OOM；此情形現由不變量 B6 攔下（fail-fast）：dataset 建構的第一個 node（`validate_data_consistency`）會擋住，training 讀取時亦有 backstop（被點名之後怎麼決定，見 §8.1）。
  - ⚠ **該欄若同時列在 `carry_columns`，上面兩個選項只有 `drop_columns` 可用。** B6 的錯誤訊息會建議「宣告成 categorical 或 drop」，但對 carry 欄選前者只是把 B6 換成下一個錯誤：欄位留在 `feature_table` 側，`build_model_input` 兩側各帶一份，改撞 `Reference 'x' is ambiguous`。B7 會同時報出來（collect-all），但 B6 那則排在前面，由上往下照做會先繞一圈。判斷方式見 §3.4 的配對規則。
- 真正的連續數值特徵不需列入任一清單。
- 宣告為 categorical 的 feature 欄位**只能是字串、整數（tinyint／smallint／int／bigint）或布林**（不變量 B5，白名單：沒列到的型別一律擋下）。其他型別在 source ETL 先處理：
  - decimal／double／float：真正的連續值就留作數值特徵；數字代碼轉成 string 或 integer。
  - date／timestamp：換成數值特徵（例如距快照日的天數）。當類別的話，模型只認得 train 月份出現過的那幾個日期。
  - binary（bytes，不是 0／1 旗標；0／1 旗標是布林或整數欄，可以當類別）：是代碼就用 `hex()` 轉成字串，一個值對一個字串，不丟資訊。
  - 複合型（array／struct／map）：攤平成多個字串／整數／布林欄。
- 一般 categorical feature 不需設定 `schema.categorical_values`；其 category mapping 會從 `train_snap_dates` 範圍內、該欄所在的特徵表（`feature_table` 或候選層級特徵表）自動建立。
- identity categorical 若不在 `feature_table`，必須在 `parameters.yaml` 的 `schema.categorical_values` 明確提供完整值域；item 那一格也可以寫 `from_train_data`，改成從 train 時段的 `sample_pool` 數出來（§3.10）。

#### 用 `suggest_categorical_cols.py` 產生候選

工具吃一個 Hive 表或 parquet 路徑，把 YAML 片段寫到 `data/profiling/<stem>_categorical.yaml`（供人工貼回上面的設定，不會自動改 config）。它把**每一個**欄位分類，不靜默漏欄：

- 低 cardinality 的字串／布林／整數欄 → `categorical_columns:`；高 cardinality 字串欄 → `drop_columns:`（附 nunique）。double／float／decimal 不論幾個值都不建議成類別（B5 不收），留作數值特徵；
- date／timestamp／binary／複合型 → 一個**註解式 review 區塊**：這些同屬 object-dtype OOM 兇手（見上一則設定原則與 §8.1），而且不能當 categorical（B5 會擋）。工具不能替你回上游轉換，所以只列出，每欄旁邊附上該型別的轉換方式；由你決定 drop，或回 source ETL 轉換；
- 高 cardinality 整數欄留作連續特徵（不列入任一清單）。

terminal 摘要與 YAML 列出同一組欄位，並附一行對帳（例如 `8 columns = 2 categorical + 1 numeric-feature + 1 drop-suggested + 4 review`），可據此確認沒有欄位被漏掉。

**大表加速**（兩者可組合，皆為選用；預設全表掃描）：

- `--where "<Spark SQL 述語>"`：只掃符合述語的資料。述語引用**分區欄**時 Spark 會下推、跳過其他分區目錄（真正省 I/O）；引用非分區欄則只是 row filter。
- `--sample-fraction <比例>`（須 `0 < 比例 ≤ 1`，超出範圍會在起 Spark 前就報錯）：隨機抽樣（固定 seed、可重現）。省的是每欄 cardinality 估算，**不省 parquet I/O**（I/O 槓桿是 `--where`）。

> ⚠ `--where` 與 `--sample-fraction` 都只看**子集**，會**低估** cardinality——子集裡判為低卡的欄只是「至少這麼低」的下界，全表可能更高。因此 summary 會印出本次 scan scope，子集模式的 YAML 也在 `categorical_columns:` 頂加上一段「採用前請複查」的警告註解。（已被建議 `drop` 的高卡欄不受**此低估**影響——子集裡已超過門檻，代表全表也一定超過。）掃分散的多個分區、而非單一連續窗口，可降低「與分區鍵相關的欄」被藏住的風險。

preprocessor 只使用 `train_snap_dates` 範圍內的 feature rows fit category mapping，再將同一份 metadata 套用至 train、val、test 與 inference。未在 train 出現的新類別會編碼為 `-1` 並記錄 warning（候選層級特徵表的類別欄一樣編成 `-1`，但不記 warning，見 §9）。

model input 寫出前，**所有數值 feature 欄**（decimal／double／float／整數族／boolean）都會轉成 `dataset.numeric_feature_storage_type` 宣告的型別（預設 float32），降低後續 driver 讀取與模型訓練的記憶體成本。收斂範圍涵蓋整數與 boolean 的理由：`pdf_to_X` 用 `DataFrame.values` 攤平，pandas 只挑一個共同 dtype，所以一欄沒轉就決定了整個矩陣的型別。

### 3.6 三個欄位清單各自作用在哪張表

`carry_columns`、`drop_columns`、`feature_columns` 常被當成同一件事的三種寫法，其實
**三者作用在不同的來源表、也在不同的 node 生效**。所以同一個欄名同時出現在
`carry_columns` 與 `drop_columns` 不是自相矛盾——當該欄同時存在於 `sample_pool` 與
`feature_table` 時，那是唯一可行的寫法（見 §3.4）。

| 設定鍵 | 作用對象 | 生效處 | 語意 |
|---|---|---|---|
| `prepare_model_input.drop_columns` | **兩張特徵表**的欄：`feature_table`，以及宣告了的候選層級特徵表（§3.8） | `compute_feature_columns` | 黑名單：不得成為模型特徵 |
| `carry_columns` | **`sample_pool`** 的欄 | `select_train_keys` | 白名單：keys 除 identity 外還要多帶這些欄 |
| `feature_columns` | 推導結果，存進 `preprocessor.json` | `compute_feature_columns` | identity categoricals ＋（`feature_table` 欄 ＋ 候選層級特徵表的非 identity 欄 − drop − 非 categorical 的 identity 欄 − label） |

`feature_columns` **不是設定鍵**，沒有地方可以直接寫它；它是前兩者與 schema 推導出來
的結果。想增減特徵就改 `drop_columns` 或 `categorical_columns`。

**`drop_columns` 會物理刪欄，不只是「不當特徵」。** `apply_preprocessor_to_features`
只保留 `base_key ＋ 有出現在 feature_table 的 feature_columns`，所以被擋在
`feature_columns` 之外的欄根本不會寫進 `preprocessed_feature_table`。這正是同時
`carry` 又 `drop` 一個欄能運作的原因：`feature_table` 那一份被刪掉，只剩 keys 帶進來
的那一份，join 時就不會撞名。候選層級特徵表不落地，但結果相同：`build_model_input`
從它只選 identity 欄與落在 `feature_columns` 裡的欄（`candidate_frame_columns`），被
drop 的欄同樣不會被帶進 join。

各 split 最後拿到哪些欄，是一條推導規則而不是逐 split 的清單：

```
model_input.columns == identity ∪ {label} ∪ feature_columns ∪ (carry_columns ∩ 該 split keys 的欄)
```

train／train-dev 的 keys 帶 carry，val／test 不帶，所以同一條規則在不同
split 展開出不同的欄位集合（見 §3.4 與
[ADR-0004](../adr/0004-carry-drop-columns-intersection.md)）。val／test 另有一個例外：
`*_zero_positive_group_ratio` 大於 0 時多一欄 `zero_positive_group_weight`（見 §3.7）。

### 3.7 沒有正例的 query group 留多少

| 設定 | 預設 | 說明 | 版本影響 |
|---|---|---|---|
| `train_zero_positive_group_ratio` | `1.0`（全留） | 同時管 train 與 train_dev | `train_variant_id` |
| `val_zero_positive_group_ratio` | `0.0`（全丟） | | `base_dataset_version` |
| `test_zero_positive_group_ratio` | `0.0`（全丟） | | `base_dataset_version` |

規則只有一條：**有正例的 query group 永遠全留；沒有正例的 query group 整組決定去留，留下比例 r。** 三個鍵都是 [0, 1] 的數字（不變量 A44 在 CLI 進入點擋下其他值，連 YAML 的 `null` 與 `true` 也擋）。預設值就是加鍵之前的行為，所以 `conf/base` 只以註解列出它們——寫成實鍵會翻對應的版本 ID，即使值等於預設。決定與理由見 [ADR-0025](../adr/0025-query-group-widened-by-occasion-role.md) 決定 3。

為什麼要留：組內排序指標（mAP 等）算不了沒有正例的組，丟掉沒損失；但把每一列當成二元預測的指標（evaluation 的預測品質指標家族）對正例佔比很敏感，算在丟過的表上會系統性偏高。這類資料又大到不能全留，所以留一個比例、用權重補回去。

**怎麼決定去留。** 對 `query_group_columns` 做決定性雜湊分桶，桶號落在 r 以下的組留下。同 `random_seed`、同設定 → 同一批組。「這一組有沒有正例」看的是 **`label_table` 接上來的 label**，不是 `sample_pool` 自己帶的那欄：那欄是來源 SQL 抄的副本，框架不保證兩者一致，拿它判斷的話，一不一致就會把真正的正例連同整組刪掉。

**在哪一步做。** 四個 split 都在 keys 上做，發生在建表**之前**：train／train_dev 由 `filter_train_keys`／`filter_train_dev_keys` 做，val／test 由 `filter_val_keys`／`filter_test_keys` 做。為了判斷有沒有正例，這一步自己接一次 `label_table`（只接 identity 與 label，只讀該 split 的月份：train 與 train_dev 讀 `train_snap_dates`、val 讀 `val_snap_dates`、test 讀這次要處理的月份），然後只留 keys 的欄；val／test 在 r > 0 時多加權重欄。建表時照舊再接一次 label 與特徵。

- 為什麼不在建表後丟：粒度閘 B10 要求每個 split 的 model_input 列數**等於**它的 keys。建表後才丟組，兩邊列數一定對不上，B10 就分不出「故意丟的」與「右表重複鍵造成的放大」——丟 1,000 列就能蓋住 500 列的放大。先在 keys 上丟，落地的 keys 就是建表的輸入，B10 照樣逐列相等。train 的位置是使用者拍板的（2026-09-21，見 ADR-0025 決定 3 的補記）；val／test 原本在建表之後丟，[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 4 把它們搬到 keys 上，B10 從此四個 split 都查。
- 搬過來之後，val／test 的 model input：r ＝ 0 或 1 時與搬之前逐列相同。0 < r < 1 時，留下的無正例組會換一批（比例一樣，同一份設定重跑得到同一批）：分桶雜湊的對象含 time，time 欄在 keys 上是 `sample_pool` 的型別（框架的 `source_etl` 產出 DATE），搬之前看到的是 Hive 分區欄讀回來的 STRING，兩者轉成字串不一樣。
- 落地的 `val_keys`、`test_keys` 因此是丟過組的；r > 0 時多一欄權重。
- 代價：r < 1 時 label 多接一次（窄表，只有 identity 與 label）；0 < r < 1 時外加一次計數，也在窄表上算。r ＝ 1 時整步不讀 label：train 原樣通過（train 的預設，`train_keys` 與加鍵之前逐列相同），val／test 每列權重 1。val／test 的預設是 0，所以每次都會接一次窄表；換掉的是以前在寬表（帶全部特徵欄）上做的 window。
- train 的先後是：逐列抽樣（`sample_ratio`／`sample_ratio_overrides`）→ train／train_dev 切分 → 本步驟 → 接 label 與特徵。所以本步驟看到的是**逐列抽樣之後還在的列**：「有正例的組全留」的意思是本步驟不再丟它們的任何一列，不是「逐列抽樣不會動它們」。某一組唯一的正例若被逐列抽樣抽掉了，它在本步驟就是無正例的組。要讓小的 query group 保持完整，`sample_ratio` 要設 1 且不設 override。
- train 與 train_dev 用同一個 r，各自對自己的組判定。兩邊以 entity 互斥切開，一個 query group 不會跨兩邊。

**權重欄（只有 val／test）。** r > 0 時表多一欄 `zero_positive_group_weight`（double）：有正例的組的列 ＝ 1，留下來的無正例組的列 ＝ 1／r。train 不產生權重欄：排序只看分數高低，不需要把比例還原回去。

- 這是**設計權重，不是無偏估計**。加權後的比值型指標（precision、pr_auc 這類）會隨組數增加收斂到未抽樣母體的值，但期望值不等於它；r 小、正例率低、留下的組少時，偏差可觀。所以 log 會印出各 split 的 r 與留下來的無正例組數：

  ```text
  val zero-positive query groups: r=0.3, kept <留下的組數> of <無正例組總數> (weight 1/r=3.333 on their rows is a design weight — few kept groups means an unstable estimate); <有正例的組數> group(s) holding a positive, all kept
  ```

  r ＝ 0 印「none kept」，r ＝ 1 印「every group kept (not counted)」；計數只在 0 < r < 1 時做（多一次 Spark action）。evaluation 報表的「基本統計 — 資料集」段在 `--post-training` 且 test 的 r > 0 時，也會印出 test 的 r 與這次評估資料裡的無正例組數。
- 權重欄一路帶到 training 的預測表 `training_eval_predictions`，evaluation 的預測品質指標家族拿它加權，而不是數列數。那張表在 catalog 是**明確列出欄位**的，沒宣告的欄會在寫入時被靜默丟掉，所以 test 的 r > 0 時 catalog 必須宣告 `{name: zero_positive_group_weight, type: DOUBLE}`（不變量 A45 在 training 的 CLI 進入點擋下）。⚠ 明確列出欄位的 Hive 表不會自動加欄：既有的 `training_eval_predictions` 表要先加上這一欄（或換一張新表）再跑 training。
- 宣告了這一欄之後把 test 的 r 改回 0 也可以：training 照樣寫這一欄，值是 NULL（沒有設計權重可言）。evaluation 只在設定的 test r > 0 時才用權重，而且會先確認每一列都真的有權重——欄不存在、或有 NULL，代表被評估的模型（`--model-version` 或 `best`）是在不同的 test r 下建的，和目前的設定對不上，evaluation 會停下並說明，不會拿 NULL 去加權（那會把那些列丟掉）。
- 欄名是框架自己的，**不得**與使用者提供的逐列訓練權重（#425）共用。特徵表裡有同名的特徵欄時，資料閘在 dataset 開頭就擋下（不變量 B12）；切片執行跳過資料閘時，組裝 val／test model input 那一步會因為兩欄同名而報錯（Spark 的 `Reference ... is ambiguous`），不會覆寫。
- 與 `val_sample_ratio` 並用時權重仍然對：那是對 entity 均勻抽，有正例與無正例的組一視同仁，不改變 1 與 1／r 的相對比例。

**train 的 r 與訓練目標。** train 的 r ＝ 0 只適合 `lambdarank`：它在沒有正例的組上梯度恆為零，training 本來就在建訓練資料時替它丟掉這些組（`core/group_utils.py` 的 `objective_drops_zero_positive_groups`），所以 r ＝ 0 只是把同一件事提前到 dataset，訓練表小很多，模型吃到的列與早停的母體都不變。`binary` 與 `rank_xendcg` 會從無正例的組學到東西（後者 repo 有實測，在全是負例的組上照樣長樹），對它們而言 r < 1 是**整組的負例降採樣**：訓練母體的正例佔比會上升，train_dev（early stopping 的驗證集）的母體也跟著變——性質與既有的逐列抽樣（`sample_ratio_overrides` 壓低負例）相同，框架刻意不擋。建議：`lambdarank` ＋ 小的 query group 時設 `train_zero_positive_group_ratio: 0` 且 `sample_ratio: 1`。training 用 ranking 類目標時，建訓練資料會 log 出「只剩單一種 label 的 query group 佔多少」（這種組沒有可比的配對），讓你判斷逐列抽樣有沒有把小組抽壞。

**r > 0 時什麼不變、什麼會變。**

- 不變：**固定整數 K** 的組內排序指標（evaluation 的 Spark 端與 HPO 的 numpy 端都在計算時跳過無正例的組），逐值相同。
- 會變：算在過濾之前的量。item 種數（某些 item 可能只出現在無正例的組）、由它解析出來的 `"all"` 的 K 與報表上依 item 種數裁掉的 K、evaluation `dataset_overview` 的各項總數與比率（列數、正樣本率……）。這是表裡多了那些組的如實反映，**不是 regression**。注意 `dataset_overview` 的數字**沒有加權**：它描述的是 dataset 留下來的這張表，所以正樣本率這類比率會隨 r 改變，既不是 r ＝ 0 時的值、也不是全部曝光的值；要估全部曝光，看預測品質指標家族的加權數字。

**evaluation 那一側。** 開了預測品質指標家族（`evaluation.report.sections.prediction_quality: true`）又跑 `--post-training` 時，test 的 r 必須大於 0（不變量 A46 在 evaluation 的 CLI 進入點擋下）：r ＝ 0 的 test 表已經沒有無正例的組，二元指標會系統性偏高。監控模式不受影響——它把 label 用 LEFT JOIN 接到推論結果上，沒經過 dataset 的篩選。

r 該設多少、生產規模下撐不撐得住，repo 裡的合成資料推不出來（生產的 entity 母體是百萬級），要在接近生產的量上實測。

### 3.8 候選層級特徵表（選用）

`feature_table` 是 **entity 層級特徵表**：一列是某個 entity 在某個時段的特徵，以 base key（`time` ＋ `entity`）接到候選列上，一個部署恰好一張。有些特徵描述的是一筆候選本身，例如「展示前 30 分鐘瀏覽了幾次」：同一個 entity、同一個時段的每一筆候選各有一個值，放不進一個 entity 一列的表。這類特徵放進**候選層級特徵表**：一列是一筆候選的特徵，以 identity（`time`、`entity`、`item`，宣告了 `occasion`／`event` 時再加上它們）接到候選列上。它是選用的，最多一張。

分類看的是 join 的鍵，不是特徵多久算一次：以 identity 接的表都屬於候選層級，不論是不是即時算出來的。其他形狀框架不收，要在來源 SQL 展開成兩類之一：比 base key 粗的表（例如 entity 有兩欄、表裡只有其中一欄）展開到每個 entity、併進 `feature_table`；只以 item 接的表展開到每一列候選、放進候選層級特徵表。它的來源 SQL 怎麼寫才不會偷看未來，見 [`source_etl.md` §3.8](source_etl.md#38-特徵的時間正確性不偷看未來是-sql-的責任)。

**宣告方式**：在 `catalog.yaml` 加一個固定名字的條目 `candidate_feature_table`，指到部署自己的實體表，寫法與 `feature_table` 相同（`conf/base/catalog.yaml` 裡有一段註解掉的範例）：

```yaml
candidate_feature_table:
  type: HiveTableDataset
  database: ${hive.db}
  table: <實體表名>
  read_only: true
```

有這個條目就是宣告，沒有另外的開關。判斷看的是疊上 `--env` 那一層之後的 catalog，所以只寫在某個 `conf/<env>/catalog.yaml` 的條目只對那個環境生效。這張表必須有每一個 identity 欄（不變量 B13，資料閘擋下）。

**它的欄怎麼變成特徵**：

- identity 欄是 join 鍵，不是特徵，也不從它讀類別詞表。`schema.item` 的值域照舊只來自 `schema.categorical_values`：這張表只有被展示過的 item，從它讀會把詞表縮小。
- 其餘的欄與 `feature_table` 的欄走同一套規則：列在 `categorical_columns` 的編成整數，列在 `drop_columns` 的丟掉，label 欄不當特徵，剩下的是數值特徵。型別規則（B5、B6）、carry 撞名（B7）與權重欄撞名（B12，§3.7）兩張表都查。
- 類別欄的詞表只從這張表的 `train_snap_dates` 月份建立，與 `feature_table` 相同。
- `preprocessor.json` 的 `feature_columns` 順序固定是：identity 類別欄、`feature_table` 的欄、候選層級特徵表的欄。沒宣告時順序與原本相同。
- 同一個特徵欄不能兩張表都有（B14）：兩張表接到同一列候選上，同名欄會出現兩次，Spark 報欄名有歧義。在其中一張的來源 SQL 改名；兩份都不是特徵的話，列進 `drop_columns`，它同時作用在兩張表。identity 欄、label 欄與 drop 掉的欄不算重複。
- `drop_columns` 列了兩張表都沒有的欄時，會記一行 warning：`drop_columns not found in any feature table`。發這行的是 `fit_preprocessor_metadata`，因為它是唯一同時看得到兩張表的 node；只看其中一張，分不出「這個欄名另一張表有」與「打錯字」。

**什麼時候讀、讀哪些月份**：候選層級特徵表不先編碼、不落地。`build_*_model_input` 在抽樣之後才讀它，用前處理器 fit 出來的詞表編碼（train 沒見過的值編成 `-1`），再以 identity left join 接上。理由是大小：它跟 `sample_pool` 一樣大，而 keys 是抽樣後的子集；若像 `feature_table` 那樣在抽樣前先編碼、存成 Hive 表，等於把之後會被抽掉的大量負例也多寫一遍（[ADR-0026](../adr/0026-feature-tables-by-join-key.md) 決定 2）。

每個 build 只讀自己 split 的月份：train 與 train-dev 讀 `train_snap_dates`、val 讀 `val_snap_dates`、test 讀還沒落地的月份（`--rebuild-dates` 指名的月份也算；log 的 `[months] dataset=test_model_input processed=…` 那一行）；帶 `--only-test-months` 時只有 test 那一份。`label_table` 與 `preprocessed_feature_table` 也照同一個規則（§5）。identity 含 `time`，所以這個篩選只影響成本，不影響結果。找不到的列、整個月沒資料各會怎樣，見 §5〈三個 left join 各自的契約〉。

**宣告之後，離線推論在 CLI 入口被擋下**（不變量 A47）。推論 pipeline 只讀 `feature_table`，而模型需要這張表的欄，放行的話它會在啟動 Spark 之後才以 `Missing feature columns` 失敗；原因與範圍見 [`inference.md` §3.6](inference.md#36-宣告了候選層級特徵表的部署不能跑離線推論)。training 與 `evaluation --post-training` 不受影響。版本號怎麼跟著變見 §7.3；有哪些事沒有檢查守著見 §9。

### 3.9 item 由多欄組成（選用）

item 由好幾個屬性組成時（例如「活動 × 素材格式」），`schema.columns.item` 直接寫成清單，不必在來源 SQL 自己拼：

```yaml
schema:
  columns:
    item: [campaign_id, creative_format]
  categorical_values:
    item:                 # 鍵是 item，值是拼好的組合
      - c01-banner
      - c01-video
```

框架讀使用者的表時，把清單裡各欄的值依序用 `-` 接起來，放進一欄固定叫 `item` 的欄，然後丟掉原本那幾欄（[ADR-0027](../adr/0027-multi-column-item-combined-on-read.md)）。之後的每一步都只看 `item` 這一欄，跟單欄的部署走同一條路。只寫一欄時（`item: prod_name` 或 `item: [prod_name]`）什麼都不做，欄名與版本號都跟原本一樣。

一條規則切開三種東西：

| 東西 | 寫什麼 | 例子 |
|---|---|---|
| 你給的表：`sample_pool`、`label_table`、候選層級特徵表 | **原欄**，框架負責拼 | `campaign_id`、`creative_format` 兩欄 |
| 評估的外部比較表 | 原欄，**或**它自己的一欄編號（寫成 `item`） | 見下面最後一點 |
| 框架自己寫的表：model_input、預測表、推論結果表 | 只有拼好的 `item` | `item = c01-banner` |
| conf 裡寫到 item 的地方 | **拼好的值**；寫欄名的地方寫 `item` | `categorical_values.item`、`inference.products`、`sample_group_keys` 裡的 `item`、`sample_ratio_overrides` 的鍵、`sample_weight_keys` |

要知道的事：

- **值裡有 `-` 沒關係**（`cmp-01` ＋ `banner` → `cmp-01-banner`）。會出事的只有兩個不同的組合拼出同一個值：`a-b` ＋ `c` 與 `a` ＋ `b-c` 都是 `a-b-c`，兩個 item 會被當成一個。資料閘在本次日期範圍的 `sample_pool` 與 `label_table` 裡找這種情況（不變量 B15），找到就擋下，訊息列出撞在一起的組合。熱門度基準線往回看到 dataset 日期範圍之前的那一段不查。
- **有一格是 null，拼出的 item 就是 null**，跟單欄時 item 是 null 一樣；不會略過那一格去拼（略過的話 `a-b` ＋ null 會變成 `a-b`，撞上真的 `a` ＋ `b`）。數字欄照它的字面拼（`7` → `7-banner`）。
- **你的表裡不能已經有一欄叫 `item`**，也不能缺清單裡的任何一欄（不變量 B16）：資料閘查 `sample_pool`、`label_table`、候選層級特徵表，evaluation 讀這些表時也會再查一次。清單裡的欄也不能同時是別的角色的欄（例如 `item: [user_id, creative_format]`），在 CLI 入口就擋下。
- **同一個原欄在每張表的型別要一樣**（不變量 B17）。每一格都先轉成文字再拼，整數 `7` 與小數 `7.0` 會拼成兩個不同的 item，那些列就接不上了；資料閘比對 `sample_pool`、`label_table`、候選層級特徵表的欄位型別。
- **原欄拼完就丟掉**，所以它們不會變成特徵；想讓活動、格式各自當特徵不在這個功能的範圍內。
- **同分時照拼好的字串比**，不逐欄照數字大小比（`CONTEXT.md` 的 **rank**）。
- **推論結果表只有拼好的 `item`**，不拆回原欄。
- 不想逐一列出所有組合時，`categorical_values.item` 寫 `from_train_data`（§3.10）：框架從 train 時段的 `sample_pool` 數出拼好的組合。
- 評估的外部比較表（`compare kind: external_hive`）是例外，兩種寫法都收：外部表也分欄存時，`columns` 寫原欄，框架拼好再套 `prod_mapping`；外部表只有自己的一欄編號時，直接寫 `item: <那一欄>`，由 `prod_mapping` 把編號翻成拼好的值。兩種都寫會被擋下（見 [`evaluation.md`](evaluation.md) 的比較報表設定）。

### 3.10 item 清單從 train 時段的資料數（選用，#379）

不想在 conf 逐一列出 item 時，item 那一格寫 `from_train_data`：

```yaml
schema:
  categorical_values:
    item: from_train_data      # 取代逐一列出；只有 item 那一格能這樣寫
```

框架在 `fit_preprocessor_metadata` 從 **train 時段、抽樣之前**的 `sample_pool` 數出 distinct 非 NULL 的 item（多欄 item 是拼好之後的值），排序後存進前處理器（`preprocessor.json` 的 `category_mappings`）。不從抽樣後的 model_input 數：抽樣沒有「每個 item 至少留一列」的保底，冷門 item 會隨抽樣設定時有時無；而前處理器是同一個 `base_dataset_version` 底下所有抽樣設定共用的。

**新 item**：val／test 期間才出現、train 時段沒有的 item。

- 列照樣留著、照樣評分，編碼成「未知」（模型沒見過它，當缺值處理）。
- `build_val_model_input`／`build_test_model_input` 印一行警告，列出有哪些新 item（不列列數）。它從已落地的 `val_keys`／`test_keys` 讀 item 那一欄（test 只讀本次要組裝的月份），不從還沒落地的 model input 數——那樣會把整段 join 再跑一次；keys 裡的 item 就是 model input 裡的 item（組裝全是從 keys 出發的 left join）。比對的是編碼用的同一份前處理器；`--only-test-months` 不重跑 fit，讀的就是磁碟上那份，不會拿今天的 train 時段資料重數。
- 資料閘不再拿 `sample_pool` 跟清單比（沒有宣告的清單可比）；`label_table` 的 item 必須在 `sample_pool` 出現過，不然照擋（B1）。

**同一版本下清單不能變（不變量 B19）**：清單不在 conf 裡，所以不會進 `base_dataset_version`。同一個版本重跑、而 train 時段的 `sample_pool` 變了（例如回補），數出的清單就可能跟磁碟上那份不同；照樣覆寫的話，用舊清單訓練的模型推論時編號全部錯位，而且不報錯。所以 `fit_preprocessor_metadata` 在覆寫前先讀磁碟上的舊檔（catalog 條目 `preprocessor_on_disk`，同一個檔、換個條目名讓 node 讀得到自己要覆寫的東西；這個條目由 CLI 從 `preprocessor` 推出，部署的 catalog 不用寫，寫了而路徑不同會在開跑前擋下，A56），不同就擋下，訊息列出多了、少了哪些 item。要照新資料建，**先讓版本號變動**：改 `dataset.train_snap_dates`，或把清單逐一寫進 `schema.categorical_values.<item>`（版本號從此含著這份清單）。逐一列出時要列 `sample_pool` 在 train、val、test 時段出現過的**每一個** item（B1），連 val／test 才出現的新 item 也要列；離線推論要照寫一份相同的 `inference.products`（A4、A27）。只貼上 train 時段數出的那份，遇到新 item 會被 B1 擋下。版本號一變就建一個新目錄，舊模型照樣讀自己那份前處理器。刪掉 `data/dataset/<base_dataset_version>/`（與該版本的 dataset 分區）再重建是最後手段：推論與評估讀的前處理器就是那個目錄裡的檔，模型目錄裡沒有副本，所以這個版本上**沒有重訓的每個模型（包含已 promote 的）都會拿錯位的 item 編號評分，結果是錯的，而且不報錯**——正是 B19 要擋的事。這道檢查在 fit 裡，所以 `--from-node fit_preprocessor_metadata` 也擋得到。

**開跑前查不了的，挪到清單數出來之後查**：`sample_ratio_overrides` 的鍵裡的 item（A5）在 fit 數完清單後查；`training.sample_weights` 的鍵裡的 item（A9c）在 training 讀到前處理器時查（`select_features`）。打錯字一樣擋。

**其他地方怎麼跟著變**：離線推論不寫 `inference.products`（寫了就擋，A52），候選取自前處理器的清單，上線後才出現的新 item 不評分（見 [`inference.md`](inference.md) 3.2 節的已知風險）；evaluation 的手寫大類 mapping 只能寫這個模型認得的 item（見 [`evaluation.md`](evaluation.md) 3.3 節）。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--rebuild-dates <d1,d2>` | 無 | 強制重算指定 test 月份（即使 partition 已存在）；值必須是 `test_snap_dates` 的子集 |
| `--only-test-months` | 關閉 | 宣告「這次只加評估月份」：只跑資料閘、test 鏈與檢查它的精度閘、粒度閘，train／val 的產物不重算。與 `--from-node`／`--only-node` 正交、可併用；上游缺料時當場報錯。它是**模式**不是切片，差別見 §5.1。開跑前先確認目前設定的 train 版本已經有表，沒有就擋下（A55，§7.5） |
| `--from-node <name>` | 無 | 從指定 node 與其後的 nodes 開始執行 |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開，不執行 pipeline |
| `--list-nodes` | 關閉 | 列出 node 名稱與從該處接續時的自動補跑成本 |

dataset 不接受版本旗標。每次啟動都會依目前設定、schema 與 `feature_table` schema（宣告了候選層級特徵表時再加上它的 schema）重新計算版本；指定既有 dataset 版本是下游 training 的責任。

`--rebuild-dates` 的值不是 `test_snap_dates` 的子集時，在 Spark 啟動之前就報錯退出（一致性不變量 A21）。它與 `--from-node`／`--only-node` **可以併用**（切片選 node、rebuild 選月份，兩者正交），但併用時會印一段 WARN：未被選中的上游 node 不會重算，那些 partition 仍是舊的。用法與時機見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)。

`--from-node` 與 `--only-node` 互斥；`--list-nodes` 也不能與兩者併用。`--dry-run` 可單獨使用表示 full-run 計畫，也可搭配切片選項檢視部分重跑計畫。

`--dry-run` 與 `--list-nodes` 不會執行 nodes、寫入 pipeline 產物或更新 manifest；但 CLI 仍會載入設定、初始化 Spark、讀取特徵表的 schema 以計算版本，並查詢 catalog 產物是否存在。

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

只要 pipeline 實際執行，CLI 仍會把 base 的 manifest 寫成 `completed` 並更新 `data/dataset/latest`。train variant 那一層不同：只有 train 的 model input（`train_model_input`，以及 `train_dev_ratio` 不是 0 時的 `train_dev_model_input`）在目前 variant 底下都有表，才寫 `completed`、更新 `train_variants/latest`（§7.5）。因此 `--only-node` 應視為進階維運工具：執行後必須確認該版本的其他必要產物原本已存在且仍然有效，不應用它建立一個從未完整成功過的新版本。

## 5. 執行流程

資料閘、精度閘、粒度閘三個檢查步驟在整個框架的檢查裡屬於哪一層、擋不住什麼：[pipeline 的檢查](../operations/user-guides/pipeline-checks.md)。

| 階段 | node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 資料閘 | `validate_data_consistency` | 三張來源表、parameters、`candidate_feature_table`（沒宣告時是 `None`） | 檢查 item coverage 與 categorical feature 型別；宣告了候選層級特徵表時，另查它有齊 identity 欄（B13）、沒有和 `feature_table` 重複的特徵欄（B14）；item 由多欄組成時，另查各表有齊原欄、沒有已叫 `item` 的欄（B16）、同一個原欄各表型別相同（B17），以及沒有兩個組合拼成同一個值（B15，§3.9）。收集問題後一次中止 | 無 |
| Train 抽樣 | `select_sample_keys` | `sample_pool` | 依 train 日期、分層比例與 overrides 做決定性抽樣 | `sample_keys` |
| Train 切分 | `split_train_keys` | `sample_keys` | 依 entity 將資料互斥切成 train 與 train-dev | `train_keys_unfiltered`、`train_dev_keys_unfiltered`（不落地） |
| Train 整組抽樣 | `filter_train_keys`、`filter_train_dev_keys` | 上一步的 keys、`label_table` | 依 `train_zero_positive_group_ratio` 整組丟掉部分無正例的 query group（label 取自 `label_table`，只讀 train 月份）；預設 r ＝ 1 原樣通過（§3.7） | `train_keys`、`train_dev_keys` |
| Val/Test keys | `select_val_keys`、`select_test_keys` | `sample_pool`（test 另收 `test_keys_month_plan`） | 建立 val 與 test identity keys；val 可依 entity 縮減。test 只處理計畫中的月份 | `val_keys_unfiltered`、`test_keys_unfiltered`（不落地） |
| Val/Test 整組抽樣 | `filter_val_keys`、`filter_test_keys` | 上一步的 keys、`label_table`（test 另收 `test_keys_month_plan`） | 有正例的 query group 全留；無正例的依 `val_`／`test_zero_positive_group_ratio` 整組留下比例 r（預設 0：全丟；label 取自 `label_table`，只讀該 split 的月份），r > 0 時在 keys 上加權重欄，組裝時帶進 model input（§3.7） | `val_keys`、`test_keys` |
| Fit 前處理器 | `fit_preprocessor_metadata` | `feature_table`、`candidate_feature_table`、`sample_pool`、`preprocessor_on_disk` | 只使用 train 日期建立 feature 清單與 category mappings（兩張特徵表都看，§3.8）；`drop_columns` 裡任何一張特徵表都沒有的欄在這裡記 warning。item 清單寫 `from_train_data` 時，從 train 時段的 `sample_pool` 數清單，並與磁碟上同版本的舊檔比對（B19，§3.10）；清單逐一列出時不使用這兩個輸入 | `preprocessor` |
| 套用前處理 | `apply_preprocessor_to_features` | `feature_table`、`preprocessor`、`preprocessed_feature_table_month_plan` | 編碼 feature categoricals；只處理計畫中的月份 | `preprocessed_feature_table` |
| 精度閘 | `validate_numeric_precision` | `preprocessed_feature_table`、`preprocessor`、`preprocessed_feature_table_month_plan`、`candidate_feature_table`、`test_model_input_month_plan`、`only_test_months`（執行模式，CLI 注入） | 不變量 B8：讀剛落地那幾個月份的 parquet footer 統計值（零掃描），確認會被 cast 的欄（decimal、整數族與 boolean——有格點的那些）在該欄自己的解析度下撐得過 `numeric_feature_storage_type`；同時產出每欄的 headroom 報告。宣告了候選層級特徵表時，它不落地、沒有 footer 可讀，改成掃一次本次各 build 要讀的月份聯集（類別欄不在內：它們在 cast 之前已編成詞表索引）。聯集是 `train_snap_dates`、`val_snap_dates`、test 那份計畫要處理的月份；`--only-test-months` 時只有最後一份，同一次掃描也確認這些月份每個都有資料；報告多一段 `candidate_feature_table` | `numeric_precision_report` |
| 組裝輸入 | `build_*_model_input` | keys、feature、label、preprocessor（test 另收 `test_model_input_month_plan`）、`candidate_feature_table` | 先把 `label_table`、`preprocessed_feature_table`、候選層級特徵表篩到自己 split 的月份（train／train-dev 讀 `train_snap_dates`、val 讀 `val_snap_dates`、test 讀計畫要處理的月份），再 left join label 與 feature（宣告了候選層級特徵表時，在這裡才讀它、編碼、接上），補齊缺失 label，選取欄位並把所有數值特徵欄轉成 `numeric_feature_storage_type` 宣告的型別（預設 float32）；val／test 在 item 清單從資料數時警告前處理器清單裡沒有的新 item（§3.10） | `train_model_input`、`train_dev_model_input`、`val_model_input`、`test_model_input` |
| 粒度閘 | `validate_model_input_grain` | 四個 split 的 keys 與 model_input、`test_model_input_month_plan` | 不變量 B10：讀 parquet footer 的列數（零掃描），確認每張 model_input 的列數等於它的 keys 表。train、train_dev、val 比這個版本底下的全部檔案；test 是增量的，只比 test 組裝這次處理的月份（`test_model_input_month_plan` 的 `to_process`），而且逐月比（加總的話，一個月多、一個月少會互相抵銷）：沒有要處理的月份時，報告寫 `not written this run`、不算失敗；某個月兩邊都沒有檔（整月的組都被丟了）算 0 ＝ 0。test 某個月沒過時，那個月已經落地了，直接重跑會跳過它：修好上游之後要用 `--rebuild-dates` 指名重算。擋的是右表（`label_table`／`preprocessed_feature_table`／宣告了的候選層級特徵表）有重複 join 鍵造成的靜默放大；同時產出每個 split 的列數報告（test 另列每個月）。`--only-test-months` 也跑它：train／train_dev／val 沒有重建，只重讀檔尾 | `model_input_grain_report` |

model input 的組裝規則（讀取範圍見列表後）：

1. keys 與 `label_table` 依 identity（`time + entity + item`，宣告了 `occasion`／`event` 時再加上它們）left join；沒有 label row 時補為 `0`。
2. 再與 `preprocessed_feature_table` 依 base key（`time + entity`）left join。
3. 宣告了候選層級特徵表時，再與它依 identity left join：只取本次執行要讀的月份，用前處理器的詞表編碼類別欄（§3.8）。
4. 輸出 identity、label、feature columns，以及 keys 帶入的 carry columns。
5. 沒有正例的 query group 在組裝之前就照三個 `*_zero_positive_group_ratio` 從 keys 丟過了（§3.7）：預設 val/test 全丟、train 與 train-dev 全留。val／test 在 r > 0 時 keys 帶的權重欄，照第 4 條帶進來。

**讀取範圍**：keys 要接的三張表（`label_table`、`preprocessed_feature_table`、宣告了的候選層級特徵表）在 join 之前都先篩到該 split 的月份。每個 join 鍵都含 `time`，別的月份本來就接不上，所以這一步只影響成本、不影響結果；寫在程式裡，是因為沒有別的東西會替它修剪：join 是從 keys 往右 LEFT join，Spark 的 dynamic partition pruning 在這個方向（預設設定下）一個分區都不會跳過，即使執行前的 `explain()` 看起來像有。依 time 分區的表因此只讀這幾個月的分區；沒依 time 分區的來源表仍會整張掃，省下的是進 join 的列（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 1 與〈決定 1–3 實作後的更正〉）。

#### 三個 left join 各自的契約

三個 join 都是 left（沒宣告候選層級特徵表時是前兩個），而且**列數恆等於 keys 的列數**——keys 的 grain 就是 model input
的 grain。這一點是後續所有列數斷言的地基，改成 inner join 會靜默改變列數，也會讓 mAP
的候選集跟著變。

這個恆等式**只有在右表的 join 鍵唯一時才成立**，而那是上游契約、不是這裡保證的事。
`validate_model_input_grain`（不變量 B10）就是實際去核對它的地方，涵蓋四個 split
（test 只看這次處理的月份）。

| join miss | 產生什麼 | 為什麼這是預期行為 |
|---|---|---|
| `label_table` 沒有這筆 | `label` 補 `0` | label table 是稀疏的：只有發生過交易的 entity 才有 row，沒有 row 就是負例 |
| `preprocessed_feature_table` 沒有這筆 | 該列的 feature 欄全為 NULL，**列仍保留** | `sample_pool` 與 `feature_table` 的母體來自不同上游，miss 是結構性的常態；LightGBM 自行處理 missing |
| 候選層級特徵表沒有這筆 | 該列的候選層級特徵欄全為 NULL，**列仍保留** | 與上一列同一個理由：丟列會悄悄改掉候選集合，mAP 的母體跟著變 |

**整個月都沒有資料則是錯誤，不補 NULL**，這點兩張特徵表相同：少了一個月，那個月每一筆候選的特徵全是 NULL，left join 不會報錯，模型照樣訓練得完，沒有人會發現。`feature_table` 的月份在 fit 與 `apply_preprocessor_to_features` 時查；候選層級特徵表不經過後者，所以由 `fit_preprocessor_metadata` 查 `train_snap_dates` 的每個月、`validate_numeric_precision` 查本次執行要讀的每個月。後者與精度無關，`numeric_precision_policy: truncate` 時照樣擋。

**全 NULL 特徵列是合法輸出，不是 bug。** 看到它不代表資料壞了，代表這個
`(time, entity)` 在 `feature_table` 裡沒有對應 row。目前刻意不加覆蓋率閘門：真實
miss 率只有在生產跑過一次才知道，本機量不到，所以「先量再決定」這一輪無法執行。日後
要量，量測點在 `build_model_input` 產出之後（量實際進了 model input 的東西），而不是
在 `sample_pool` 的 ETL 端取代理值。完整理由與被否決的替代方案見
[ADR-0005](../adr/0005-model-input-degenerate-state-contracts.md)。

### 5.1 test 分支是增量的

`apply_preprocessor_to_features`、`select_test_keys`、`filter_test_keys`、`build_test_model_input` 四個 node 只處理**尚未落地**的月份。train／train-dev／val **不是**增量的：它們一旦被執行就整批重算，把逐位元相同的內容覆寫回同一批 partition。

省掉那次重算的方法是**不執行它們**，不是讓它們變成增量的。`--only-test-months` 就是這樣做的——它是 `create_pipeline` 的**模式**參數（不是切片），只組出資料閘、test 鏈與檢查它的精度閘、粒度閘，其餘節點根本不進 pipeline；留下哪些節點以 `pipelines/dataset/pipeline.py` 的 `ONLY_TEST_MONTHS_NODES` 為準。反過來說，**增量性與這個旗標無關**：上面四個 node 帶不帶旗標都只處理尚未落地的月份，旗標改的是節點集，不是增量性。模式與切片的分工見 [ADR-0013](../adr/0013-pipeline-modes-and-slicing-are-separate.md)，使用動線見[新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)。

怎麼看出誰是增量的：**pipeline 定義上有 `*_month_plan` input 的就是**。CLI 在任何 Spark 工作開始之前列一次 metastore partition（零掃描）、算出三份計畫（`month_plans.build_month_plans`），以 `<產物名>_month_plan` 這三個名字放進 catalog；節點把它當一般 input 收下。所以：

- 「這次處理／跳過哪些月」在 pipeline 開跑前就以三行 `[months]` log 印出來，範圍設錯可以在花掉時間之前發現；
- 忘記提供計畫不會靜默全量重建——runner 在第一個節點執行前就 raise；
- 每張表吃自己那份計畫，只有一個例外：`test_keys` 的計畫另外包含 `test_model_input` 這次要組的月份。`test_keys` 帶著丟組的判斷（看 `label_table`），上次跑到一半留下的 keys 若是在 label 回補之前判斷的，拿來接新的 label 會對不上，而且列數照樣相等、B10 看不出來，所以跟著組裝一起重做。

`test_keys` 的整組抽樣節點（`filter_test_keys`）吃 `test_keys` 那份計畫：它讀 `label_table` 的月份，就是這次選 key 的月份。它和 val 的決策相同，但各用自己的節點函式，因為兩者讀的 ratio 鍵與月份不同（讀錯不會報錯）。粒度閘吃的則是 `test_model_input` 那份：它比的是 test 組裝這次寫了哪些月份。

之所以安全：每個 `snap_date` partition 的內容只是該月 `feature_table` rows（宣告了候選層級特徵表時再加上它該月的 rows）與 `category_mappings` 的函數，與其他月份無關，而 `category_mappings` 只在 train 月份上 fit。所以跳過既有月份不改變任何 partition 的內容，只改變這次要做多少工。

代價、`--rebuild-dates` 逃生口與完整理由見 [ADR-0002](../adr/0002-preprocessed-feature-table-incremental.md)；計畫為什麼走 catalog 而不是 `parameters`，見 [ADR-0007](../adr/0007-month-plans-travel-through-the-catalog.md)。

## 6. 產物與驗收

### 6.1 主要產物

| 層級 | 產物 | 儲存方式 |
|---|---|---|
| Base | `preprocessor`（`preprocessor.json`） | `data/dataset/<base_dataset_version>/` |
| Base | `preprocessed_feature_table`、`val_keys`、`test_keys`、`val_model_input`、`test_model_input` | Hive，以 `base_dataset_version` partition |
| Train variant | `sample_keys`、`train_keys`、`train_dev_keys`、`train_model_input`、`train_dev_model_input` | Hive，以 base + `train_variant_id` partition |
| Metadata | base、train variant 的 `manifest.json` | 對應版本目錄 |
| Alias | 各層的 `latest` symlink | base 層指向最近一次成功執行的版本目錄；train variant 層指向最近一次「執行成功、而且 train 的 model input 在它底下都有表」的 variant（§7.5） |

Hive 的實際 table 名稱與 partition 欄位以 `conf/base/catalog.yaml` 為準。

### 6.2 驗收重點

執行完成後至少確認：

1. log 中顯示的三層 version ID 符合預期。
2. `preprocessor.json` 的 `feature_columns` 包含 item，且欄位順序合理。
3. `preprocessor.json` 的 `category_mappings` 包含所有 categorical columns。
4. train 與 train-dev 都有資料，且同一 entity 不會同時出現在兩者。
5. model input 的 identity key 沒有重複，label 僅包含合法值。
6. `*_zero_positive_group_ratio` 維持預設 0 時，val/test 每個保留的 query group 至少有一個正例；設了 r > 0 時，無正例的組的列都帶權重 1／r。
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

`val_zero_positive_group_ratio` 維持預設 0 時，第二個查詢應回傳零列。若 schema 的 entity 不只一欄（或宣告了 `occasion`），驗收 query group 時應使用全部 query group 欄位。

## 7. 版本、重跑與恢復

### 7.1 兩層 dataset 版本

dataset 每次啟動都會計算以下版本：

| 版本 | 精確計算依據 | 主要產物 |
|---|---|---|
| `base_dataset_version` | `parameters_dataset.yaml` 中除了七個只影響 train 的 keys 與 `test_snap_dates` 以外的所有內容，加上完整 schema、`feature_table` schema fingerprint 與 dataset 產物格式版本；宣告了候選層級特徵表時，再加上它的 schema fingerprint（§7.3） | preprocessor、共用 feature、val/test |
| `train_variant_id` | 只包含 `sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio`、`train_split_keys`、`train_zero_positive_group_ratio`、`carry_columns` | train/train-dev keys 與 inputs |

會從 base payload 排除、改進 `train_variant_id` 的 keys 有七個（`core/versioning.py` 的 `TRAIN_SAMPLING_KEYS`）。前六個是 train 的抽樣與切分；`carry_columns` 不抽樣，但它的欄只進 train／train-dev 的表：

```text
sample_ratio
sample_ratio_overrides
sample_group_keys
train_dev_ratio
train_split_keys
train_zero_positive_group_ratio
carry_columns
```

`val_sample_keys`、`val_zero_positive_group_ratio`、`test_zero_positive_group_ratio` **刻意不在這份清單裡**：val／test 產物只由 `base_dataset_version` 分割，把它們排除掉就等於讓 val／test 的抽樣改了卻靜默沿用舊 parquet。推導見 [ADR-0016](../adr/0016-split-unit-declared-by-two-keys.md) 與 [ADR-0025](../adr/0025-query-group-widened-by-occasion-role.md) 決定 3。

除了這七個 keys，還有一個被排除的 key —— `test_snap_dates`：

```text
test_snap_dates
```

它被排除的理由與抽樣 keys 不同。抽樣 keys 是因為「另有一層 variant ID 承接」；`test_snap_dates` 則是因為**它不定義產物身分，只定義資料覆蓋範圍**。test 資料不進任何模型擬合（`val` 驅動 early stopping，所以它留在 base payload 裡），所以在 `test_snap_dates` 加一個月份時：

- `base_dataset_version` 與 `model_version` 都不變，因此**不需要重訓**；
- 新月份的 test 產物以 dynamic partition 寫入，既有月份的產物與評估報表原封不動（累積語意）；
- 新舊月份的評估報表並存於同一個模型身分之下，可直接比較。

代價是 `parameters_dataset.yaml` 不再是 test 覆蓋範圍的唯一真實來源 —— 同一個 `base_dataset_version` 底下的月份會隨時間累積，**實際有哪些月份要以 Hive partition 為準**（`SHOW PARTITIONS`）；manifest 只記錄**最後一次執行**當下的設定，每次執行覆寫，因此讀不出累積的覆蓋範圍。完整推導與否決過的選項見 [ADR-0001](../adr/0001-test-dates-out-of-dataset-version-identity.md)；操作步驟見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)。

除了上面列出的排除 keys，`parameters_dataset.yaml` 在 `dataset` 區塊新增的其他設定，預設都會納入 `base_dataset_version`。這是保守策略：新設定若可能改變 dataset 產物，會先讓 base version 翻新，避免不同內容共用版本。

每層使用 canonical YAML 計算 8 碼 SHA-256 hash。mapping 的 key 排列順序不影響 hash，但 list 的內容與順序會影響，例如重新排列 `sample_group_keys`、日期清單或 `categorical_values` 都會產生不同版本。

dataset 本身不接受指定版本的 CLI 旗標；執行時永遠以目前設定重新計算，training 再選擇要使用的既有版本。

### 7.2 設定版本矩陣

下表列出目前 `parameters_dataset.yaml` 的所有設定：

| 設定 | Base | Train variant | 說明 |
|---|:---:|:---:|---|
| `train_snap_dates` | ✓ |  | 改變 fit preprocessor 與 train 資料時間範圍 |
| `sample_ratio` |  | ✓ | 只改變 train 抽樣 |
| `sample_ratio_overrides` |  | ✓ | 只改變 train 各分層抽樣 |
| `sample_group_keys` |  | ✓ | train 的分層 key |
| `carry_columns` |  | ✓ | 只改變 train/train-dev 的 keys 與 model input 帶哪些欄 |
| `train_dev_ratio` |  | ✓ | 只改變 train/train-dev entity 切分 |
| `train_split_keys` |  | ✓ | 只改變 train/train-dev 的切分單位；val/test 產物完全不動 |
| `train_zero_positive_group_ratio` |  | ✓ | 只改變 train/train-dev 留下多少無正例的 query group（§3.7） |
| `val_snap_dates` | ✓ |  | 改變 validation 資料 |
| `val_sample_ratio` | ✓ |  | val 屬於 base layer，不屬於 train sampling |
| `val_sample_keys` | ✓ |  | 同上；不登記進 train sampling，否則 val 會靜默沿用舊資料 |
| `val_zero_positive_group_ratio` | ✓ |  | 同上（§3.7） |
| `test_zero_positive_group_ratio` | ✓ |  | test 產物也只由 base 分割，所以留在 base（§3.7） |
| `test_snap_dates` |  |  | 只改變 test 覆蓋範圍，不改變任何產物身分（見 7.1） |
| `prepare_model_input.drop_columns` | ✓ |  | 改變 feature 清單與 model input |
| `prepare_model_input.categorical_columns` | ✓ |  | 改變 category mappings、encoding 與 feature 清單 |

特殊情況：

- `test_snap_dates` 是唯一一個「改了卻不翻新任何版本」的日期設定。改動它之後 dataset 會在**同一個** `base_dataset_version` 底下補上新月份的 partition；既有月份不受影響，也不需要重訓。

### 7.3 設定檔外的版本因素

以下內容也會影響 `base_dataset_version`：

| 因素 | 是否翻新 Base | 說明 |
|---|:---:|---|
| `parameters.yaml` 的 `schema.columns` | ✓ | `time`、`entity`、`item`、`label`、`score`、`rank` 都納入 |
| `schema.categorical_values` | ✓ | 值與 list 順序都納入；改變 item 值域或 encoding 順序會翻新。item 那一格寫 `from_train_data` 時納入的是這個字串，數出來的清單**不**納入——所以同一版本下清單變了由 B19 擋（§3.10） |
| `feature_table` 欄位名稱 | ✓ | 新增或移除欄位都會改變 fingerprint |
| `feature_table` 欄位型別 | ✓ | 例如 `double` 改為 `float` |
| `feature_table` 欄位順序 | ✓ | feature 順序會傳入 preprocessor，因此 fingerprint 對順序敏感 |
| 宣告或拿掉 `candidate_feature_table` 條目 | ✓ | 宣告時，它的 schema fingerprint 以自己的 payload 鍵 `candidate_feature_table_fingerprint` 進 hash，也寫進 base 的 `manifest.json`；沒宣告時 payload 裡沒有這個鍵，所以不用這張表的部署，版本號完全不受它影響 |
| 候選層級特徵表的欄位名稱、型別、順序 | 只在宣告時 ✓ | 與 `feature_table` 同一套 fingerprint 規則。用另一個 payload 鍵而不是併進 `feature_table` 那一個：兩張表接的鍵不同，同一組欄放在哪一張，是不同的 dataset |
| dataset 產物格式版本（`core/versioning.py` 的 `DATASET_ARTIFACT_FORMAT_VERSION`） | ✓ | 框架自己的整數，不是設定。程式改了 dataset 落地的內容、設定卻沒動時，框架把它加 1，讓每個部署的 base 都翻一次、在新版本下重建（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 15）。升級到加了 1 的框架之後要重建 dataset、重訓；重訓之前，「只加評估月份」的流程對現役模型用不了（[新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)） |

以下內容目前**不會**改變任何 dataset version：

| 因素 | 為何不翻新 | 操作注意 |
|---|---|---|
| `parameters.yaml` 的 `random_seed` | 不在兩層 hash payload | 會改變 train/train-dev、train sampling 與 val sampling 結果；修改後應人工視為資料版本變更並完整重建 |
| `project_name`、`hive`、`spark`、`logging` | 不屬於 dataset hash 的 schema payload | 一般只影響執行環境或觀測性 |
| `conf/base/catalog.yaml` | catalog 設定不進 hash（唯一的例外是上表的 `candidate_feature_table` 條目有沒有宣告） | 修改 table/path/partition 時需自行確認是否誤讀或覆寫既有版本；把 `feature_table` 或 `candidate_feature_table` 換成指到另一張 schema 相同的實體表，版本號也不變 |
| `feature_table`、候選層級特徵表的資料值 | fingerprint 只看欄名、型別與順序 | 同 schema 的資料回補不會翻版，必須重跑相同版本 partitions（候選層級特徵表的情形見 §7.5） |
| `label_table`、`sample_pool` 的資料值或 schema | 目前沒有對兩表計算 fingerprint | 上游回補、候選或 label 改變時需人工完整重跑 |
| source ETL SQL、dataset Python 程式碼 | 程式碼內容不進 hash | 框架改了 dataset 落地內容時會把 dataset 產物格式版本加 1（上表），這要靠改程式的人記得加；自己改 source ETL SQL 不會翻版，可能覆寫同一版本。manifest 的 git commit 只供追溯 |
| `parameters_training.yaml` | training 設定不參與 dataset IDs | 可能改變 `model_version`，但不重建 dataset |

`parameters_dataset.yaml` 以外的任意設定，除上述 schema payload 外，都不會自動影響 dataset version。

### 7.4 修改設定時要重跑什麼

| 修改內容 | 版本結果 | 建議 |
|---|---|---|
| train ratio、override、分層 keys、train-dev ratio、`train_zero_positive_group_ratio`、carry columns | 新 train variant，base version 不變 | 完整執行最安全；熟悉切片者可依執行計畫只重建 train 路徑 |
| `val_`／`test_zero_positive_group_ratio` | 新 base version | 完整執行 dataset；test 的 r 從 0 改成 > 0 時，先讓 `training_eval_predictions` 宣告權重欄（§3.7） |
| train／val 日期、categorical/drop | 新 base version | 完整執行 dataset |
| 升級到 dataset 產物格式版本加了 1 的框架 | 每個部署都是新 base version | 完整執行 dataset，再重訓（`model_version` 與 HPO 的 `search_id` 都會變）（§7.3） |
| 只在 `test_snap_dates` 加一個月份 | 版本全部不變 | 執行 dataset 補上新月份，再跑 predict 與該月份的 evaluation；不重訓。步驟見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md) |
| schema roles 或 item values | 新 base version | 先確認 source tables，再完整執行 dataset |
| `feature_table` 欄名、型別或順序 | 新 base version | 完整執行 dataset |
| 宣告或拿掉候選層級特徵表，或改它的欄名、型別、順序 | 新 base version | 完整執行 dataset |
| source table 資料值回補，但 schema 不變 | version ID 可能不變 | 完整重跑受影響版本，避免沿用舊 partition |
| 全域 `random_seed` | 目前 version ID 不會自動改變 | 視為抽樣版本變更，清楚記錄並完整重建相關產物 |

三層版本描述的是產物身分與失效範圍，不是自動增量執行器。未帶任何**模式**或**切片**旗標時（模式＝`--only-test-months`，切片＝`--from-node`／`--only-node`），dataset 仍會執行完整 DAG，並覆寫相同版本 partitions。

任何 dataset ID 改變後，training 使用該組新版本時，`model_version` 也會隨之改變。`base_dataset_version` 翻新時，即使 `train_variant_id` 的 8 碼字串相同，它也會位於新的 base 目錄／partition 之下，兩者仍是不同的有效資料組合。

### 7.5 部分重跑的安全邊界

- catalog 的 `exists()` 只能確認產物存在，不能證明內容由目前參數或來源資料產生。**test 分支的增量跳過把這件事變成了正常執行路徑的預設行為**：`feature_table` 對某個舊 test 月份回補之後，該月 partition 不會自動更新且不報錯，得用 `--rebuild-dates` 指名重算（[ADR-0002](../adr/0002-preprocessed-feature-table-incremental.md)）。
- dataset 的主要 Hive 產物具有版本 partitions，可降低設定改變後誤讀舊資料的風險；來源資料值回補與 seed 變更仍需人工判斷。
- `validate_data_consistency` 沒有輸出，若它位於切片起點之前便不會自動重跑。source tables 或 item 資料有變時應執行 full run。
- 候選層級特徵表不落地，train／train_dev／val 的 model input 每次執行都重讀它，所以它的回補在下一次完整執行就生效；已落地的 test 月份照樣被跳過，要用 `--rebuild-dates` 指名重算。
- `validate_numeric_precision` 有輸出（`numeric_precision_report`），所以**不會**被當成側效應 node 跳過；但沒有任何 node 消費那份報告，所以它也不會被自動拉回來——切片起點在它之後就不會跑到它。候選層級特徵表的缺月檢查（§5）也在這個 node 裡：沒跑到它的那一輪，只剩 `fit_preprocessor_metadata`（如果有跑）查 train 月份，其餘缺的月份會變成一整個月的 NULL 特徵。
- `validate_model_input_grain` 同樣有輸出（`model_input_grain_report`），行為與上一條一致：不會被當成側效應 node 跳過，但也沒有下游會把它拉回來。
- `val_keys_unfiltered` 與 `test_keys_unfiltered` 是記憶體中間結果；從 `filter_val_keys`／`filter_test_keys` 接續會補跑對應的 `select_val_keys`／`select_test_keys`（各自只讀自己的月份）。從 `fit_preprocessor_metadata` 接續也會補跑這兩個：四個整組抽樣 node 都排在 fit 之後。四個 split 的 model input 都由 build node 直接落地，所以從任何 build 接續都不會補跑別的 split 的 build。
- `train_keys_unfiltered` 與 `train_dev_keys_unfiltered` 同樣不落地；從 `filter_train_keys`／`filter_train_dev_keys` 接續會補跑 `split_train_keys`（它不 shuffle，代價低）。落地的 `train_keys`／`train_dev_keys` 是整組抽樣之後的 keys，正是 build node 的輸入，所以 B10 的配對不受影響。
- 切片執行會在 manifest 記錄 `resumed_from` 或 `only_node`，供後續追溯。
- 開跑前 CLI 會對 base、train variant 各先寫一份 `status: running` 的 `manifest.json` stub（崩潰溯源用，**不**更新 `latest` symlink，也不覆寫既有 manifest）。成功完成後，base 的 stub 覆寫為 `status: completed` 並更新 `data/dataset/latest`；train variant 的要看下一條。`--dry-run` / `--list-nodes` 不寫 stub。
- train variant 那一層的 `completed` 與 `train_variants/latest` 看**表在不在**，不看這一輪跑了哪些 node（[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 12）。跑完之後 CLI 向 metastore 確認：目前設定的 variant 在 `train_model_input`，以及 `train_dev_ratio` 不是 0 時的 `train_dev_model_input` 底下都有分區，才寫 `completed`、把 `latest` 指過去（`train_dev_ratio: 0` 會故意讓 train_dev 是空的，空表沒有分區，所以這時不查它）；否則兩樣都不動，stub 留在 `running`，並印兩行 `[train_variant]` 警告，寫明 training 會繼續讀 `latest` 指的哪一個。為什麼要這樣：training 讀哪個 variant 只看 `latest`，不會拿自己的設定重算，所以 `latest` 錯了不會報錯。它有兩種錯法，「這輪跑了哪些 node」都判斷錯：
  - 改了抽樣設定之後跑一段不碰 train（或只碰一部分 train）的切片：新 variant 底下沒有 train 的表。`latest` 若照「跑完了」指過去，training 會讀到 0 列；所以它留在舊的那個。
  - 先後用抽樣設定 A、B 各建過一次，再把設定改回 A 跑 `--only-test-months`：這一輪沒跑任何 train build，但 A 的表在。`latest` 會指回 A。
- `--only-test-months` 開跑前另有一道檢查（不變量 A55）：這個模式不建 train 的表，所以目前設定的 variant 在 `train_model_input` 底下沒有分區時，在任何 node 執行、任何 stub 寫入之前就停下來。`--dry-run`、`--list-nodes` 也照樣檢查。訊息分兩種，見 §8。只查 metastore 的 partition 清單，不跑 Spark job。

## 8. 常見錯誤與排查

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| `Config consistency check failed`，item 不在 categorical columns | item 被 drop 或漏設為類別 feature | 將 item 加回 `categorical_columns`，並從 `drop_columns`／feature exclusion 移除 |
| categorical 與 drop 衝突 | 同一欄位同時出現在兩份清單 | 明確決定該欄要作為 feature 或排除 |
| override references unknown item | override key 中的 item 未宣告或拼錯 | 用 sampling editor 重建 key，並對齊 `schema.categorical_values` |
| 訊息帶 `B19:` | item 清單從 train 時段數出來，同一個 `base_dataset_version` 重跑、清單跟磁碟上的前處理器不同 | 要照新資料建：先讓版本號變動——改 `dataset.train_snap_dates`，或把清單逐一寫進 `schema.categorical_values.<item>`（要列 train、val、test 時段出現過的每一個 item，B1），舊模型照樣讀自己那份前處理器。刪 `data/dataset/<base_dataset_version>/` 重建是最後手段：這個版本上沒重訓的模型（含已 promote 的）會拿錯位的編號評分、不報錯；見 §3.10 |
| `(A56) catalog entry 'preprocessor_on_disk' reads '...', but 'preprocessor' writes '...'` | 部署的 catalog 自己寫了 `preprocessor_on_disk`，路徑卻跟 `preprocessor` 不同。這個條目是選用的（第一次跑時檔案還不存在），路徑不對就讀到「沒有」，B19 會以為是第一次跑而不檢查 | 刪掉 `preprocessor_on_disk` 條目：CLI 會從 `preprocessor` 推出它（同一個 filepath、`optional`）。或把它的 `filepath` 改成跟 `preprocessor` 一樣 |
| `Node 'fit_preprocessor_metadata' requires input 'preprocessor_on_disk' which is not in the catalog and not produced by any prior node`（evaluation 是 `Node 'prepare_eval_data' …`） | 部署的 `preprocessor` 條目不是 `JSONDataset`。只有 `JSONDataset` 能在檔案不存在時回傳「沒有」，所以 CLI 只從它推出 `preprocessor_on_disk` | 把 `preprocessor` 改成 `JSONDataset`（框架寫出的就是 JSON） |
| `A5: dataset.sample_ratio_overrides references item value(s) … counted from the train months` | item 清單從資料數時，override 鍵裡的 item 不在數出來的清單裡 | 修正鍵，或確認那個 item 在 train 時段的 `sample_pool` 有出現 |
| weight column unavailable | training 權重維度未進入 model input | 將非 identity 欄位加入 `carry_columns` 後重跑 dataset |
| `Data consistency check failed`，sample_pool item 不一致 | `sample_pool` 缺少宣告 item，或含有未知 item | 檢查本次日期範圍的 distinct item，修正 source ETL 或 schema |
| `DataConsistencyError: ... un-encoded non-numeric type(s)`，讀 parquet 前秒級失敗 | 字串／非數值欄進了 `feature_columns`，既沒宣告 categorical 也沒 drop（不變量 B6） | 錯誤訊息逐欄點名兇手；每欄依型別決定怎麼處理，見下方 §8.1。改完會 bump `base_dataset_version`、需重建 dataset |
| `categorical column '...' is a ... type`（B5） | categorical 欄的型別不是字串／整數／布林：連續值誤標類別，或日期、binary、複合型被設成類別 | 錯誤訊息依型別給解法；整理見 §3.5 的型別規則。不是特徵就 drop。訊息寫 `in candidate_feature_table` 時，是候選層級特徵表的欄 |
| `B13: candidate_feature_table is missing identity column(s) [...]` | 候選層級特徵表缺 identity 欄。最常見的是把一張一個 entity、一個時段一列的表宣告成候選層級 | 在它的來源 SQL 補上缺的欄；那張表若描述的是 entity 而不是一筆候選，它該併進 `feature_table`（§3.8） |
| `B14: column(s) [...] are in both feature_table and candidate_feature_table` | 同一個特徵欄兩張特徵表都有 | 在其中一張的來源 SQL 改名；兩份都不是特徵就列進 `drop_columns`（§3.8） |
| `B15: item combinations (...) of [...] all combine to '...'` | item 由多欄組成，兩個不同的組合用 `-` 拼出同一個值 | 在來源 SQL 改其中一個值，讓組合拼出來不同（§3.9） |
| `B16: <表> is missing item column(s) [...]`／`already has a column named 'item'` | item 由多欄組成，那張表缺某個原欄，或已經有一欄叫 `item` | 補上原欄；已有的 `item` 欄在來源 SQL 改名（§3.9） |
| `B17: item column '...' has different types across tables {...}` | item 由多欄組成，同一個原欄在兩張表型別不同（例如一張 `int`、一張 `double`） | 在各表的來源 SQL 轉成同一個型別（§3.9） |
| `(A24) dataset.X_snap_dates [...] and dataset.Y_snap_dates [...] name the same calendar day` | train/val/test 使用相同日期 | 重新切分日期，確保集合互斥。此檢查在 Spark 啟動前執行，**按日比對而非按字面**，所以同一天的不同寫法也抓得到；訊息會分別印出兩邊各自的原始寫法 |
| `N 個日期區間設定無法展開` | 某個 `{start, end, step}` 區間寫錯：起迄沒落在 step 上、迄日早於起日、`step` 拼錯、少鍵或多鍵 | 訊息逐一點名是哪個檔的哪個鍵、哪一端不對；所有寫錯的區間一次列完。規則見 §3.1 |
| `feature_table missing required ... snap_dates`／`candidate_feature_table missing required ... snap_dates` | source ETL 未產出某些日期 | 補跑 feature ETL 或修正日期設定。後者點名的是候選層級特徵表：`train_snap_dates` 在 fit 前處理器時查，其餘要讀的月份在精度閘查（§5） |
| identity categorical missing declarations | item 等 identity 類別無法從 feature table fit | 在 `schema.categorical_values` 提供完整值域 |
| log 出現 `unknowns in column ...` | 非 train 日期出現 mapping 未見的新類別 | 檢查是否為資料異常；必要時延伸 train mapping 或調整來源清理。只有 `feature_table` 的類別欄會數，候選層級特徵表的不會（§9） |
| 抽樣結果為空或某分層消失 | ratio/override 為 0、key 格式不符或母體太小 | 檢查 profiling、override key 順序與實際分層值 |
| `sample_group_keys` 欄位不存在 | 分層欄位只存在於 `feature_table`，未寫入 `sample_pool` | 在 `sample_pool_etl` SQL 連接來源欄位並重建 `sample_pool` |
| val/test 筆數比 sample pool 少很多 | 零正例 query groups 被預期移除（`*_zero_positive_group_ratio` 預設 0） | 查詢 group 的 label sum；這是排序評估母體設計，不一定是錯誤。要留一部分，見 §3.7 |
| `A44: dataset.*_zero_positive_group_ratio=... is not a ratio` | 值不在 [0, 1]，或寫成字串、布林、`null` | 改成 [0, 1] 的數字，或刪掉那一行用預設值 |
| `(A45) catalog entry 'training_eval_predictions' does not declare 'zero_positive_group_weight'` | test 的 r > 0，但預測表沒宣告權重欄 | 在該 catalog 條目的 `columns:` 加 `{name: zero_positive_group_weight, type: DOUBLE}`；既有表要先加欄（§3.7） |
| `B12: feature column 'zero_positive_group_weight' is a model feature` | 特徵表有一欄與框架的權重欄同名，而 val 或 test 的 r > 0 | 在來源 SQL 改名；不是特徵的話列進 `drop_columns` |
| `Reference 'zero_positive_group_weight' is ambiguous`（在 `build_val_model_input`／`build_test_model_input`） | 同上，但 B12 被跳過（切片執行時資料閘不會跑） | 同上 |
| `(A55) --only-test-months: train_model_input has partitions under base_dataset_version=... but none under train_variant_id=...` | 只影響 train 的設定（`core/versioning.py` 的 `TRAIN_SAMPLING_KEYS`：train 的抽樣設定與 `carry_columns`）改了，這個 train 版本還沒建；這個模式不建 train 的表 | 不帶 `--only-test-months` 跑完整的 dataset，或把那個改動還原（§7.5） |
| `(A55) --only-test-months: train_model_input has no partition under base_dataset_version=...` | 這個 base 版本從沒建過：第一次跑、base 層的設定或特徵表的 schema 改了，或框架升級時把 dataset 產物格式版本加了 1 | 不帶 `--only-test-months` 跑完整的 dataset。base 換了代表要重訓，已經不是「只加評估月份」（[新增一個評估月份](../operations/user-guides/adding-an-eval-month.md) 步驟 2） |
| `[train_variant] 目前設定的 train 版本（train_variant_id=...）還沒建` | 這一輪沒把 train 的 model input 都建在目前 variant 底下（切片跳過了 train build，或只建了 `train_model_input` 與 `train_dev_model_input` 其中一張），所以 `completed` 與 `train_variants/latest` 都沒動 | 要讓 training 用目前設定，跑完整的 dataset；否則 training 會讀警告裡寫的那一個 variant（§7.5） |
| `Unknown node ...` | node 名稱拼錯或 pipeline 已變更 | 先執行 `dataset --list-nodes` 取得目前名稱 |
| 切片計畫出現昂貴的 `auto-included` | 必要 artifact 不存在或 catalog 無法載入 | 先確認版本 partition 與檔案；不接受補跑成本時先停止修復 |
| 部分重跑後結果與設定不一致 | skipped artifacts 已過期，或資料閘被跳過 | 使用 full run，並比較 manifest、版本與 source data 更新時間 |
| Spark shuffle 或記憶體壓力過高 | 單一 partition 太大或 join shuffle 過重 | 檢查 `spark.sql.shuffle.partitions`、AQE、資料偏斜與 executor memory |

### 8.1 B6 點名之後，怎麼決定每一欄

B6 擋下來時，錯誤訊息會**逐欄點名**（`feature column 'cust_segment' is non-numeric and is not declared categorical...`）。對每一個被點名的欄：

- **是有用的類別特徵**（例：客群別、通路）→ 加進 `dataset.prepare_model_input.categorical_columns`。它會在 Spark 端就被編成整數，仍是模型特徵。
- **不是模型特徵**（例：ID、自由文字）→ 加進 `dataset.prepare_model_input.drop_columns`。
- **是 date／timestamp／binary／複合型欄** → 不能加進 `categorical_columns`，會被類別欄的型別檢查擋下。資料閘報出的訊息會直接寫 `It cannot be declared categorical either`；training 讀取時的 backstop 不看型別，訊息仍是通用的「宣告成 categorical 或 drop」，照上面這條處理即可。要當特徵，就在 source ETL 轉換：日期換成數值特徵，binary 用 `hex()` 轉字串，複合型攤平。不要就 drop。

> ⚠ **這會 bump `base_dataset_version`，需要重建整個 dataset**——兩個鍵都參與 dataset 版本雜湊。閘門本身只讓你**知道是哪幾欄**、並防止未來重建時再犯，不會替你改 config。

`scripts/suggest_categorical_cols.py` 可以加速這個決定（用法見 §3）：它把高 cardinality 字串欄建議進 `drop_columns`，並把 date／timestamp／binary／複合型欄放進待人工判斷的 review 區塊——那些同屬 object-dtype OOM 兇手，也不能當 categorical，只能 drop 或回 source ETL 轉換。

**不處理會怎樣**：該欄會原封不動穿過整條 dataset pipeline 成為特徵，training 讀取時整張矩陣塌縮成 object dtype（每格 ~34 B vs float64 8 B），在 `to_numpy` 被 OOM killer 殺掉。合成資料不含這類欄位，所以**本機永遠不會重現，生產環境必爆**。事故全貌見 [2026-07 調查紀錄](../notes/2026-07-11-training-oom-investigation.md)。

## 9. 限制與注意事項

- train/train-dev 切分與 val entity sampling 目前只使用 `schema.entity` 的第一個欄位；使用複合 entity 時需確認這符合業務語意。
- 日期只檢查集合互斥，不檢查時間順序與 label 觀察窗。
- `random_seed` 會改變抽樣結果，但目前未納入 dataset 版本 hash。
- 版本 hash 包含 `feature_table` schema fingerprint（宣告了候選層級特徵表時也包含它的），不包含 source rows 的資料值或 source ETL SQL。
- `sample_pool` 的 identity 唯一性由 source ETL 的 `max_duplicate_key_ratio` 檢查負責；dataset 的三個 split 選 key 時都不去重（val、test 在 2026-09 之前各自 `dropDuplicates`，[ADR-0029](../adr/0029-dataset-second-pass-scoped-reads-symmetric-splits.md) 決定 6）。自己準備 `sample_pool`、不經過 source ETL 時，沒有任何東西檢查：重複的 identity 會變成重複的 keys 與 model_input 列，粒度閘 B10 也看不出來（兩邊一樣重複，列數仍相等）。
- label left join 不到時會視為負例 `0`；必須確定 sparse label table 的語意確實如此。
- feature left join 不到時會留下全 NULL feature 的列，dataset 不會將其視為缺少 entity 的硬錯誤。這是明文契約而非容忍，代價是「特徵缺失」與「特徵值真的是 NULL」在 model input 裡無法區分；契約與量測點見 §5。
- val/test 預設排除零正例 query groups，因此產物不代表完整上線母體；設 `val_`／`test_zero_positive_group_ratio` > 0 會留下一部分並帶上設計權重（§3.7），權重不是無偏估計。
- 宣告了候選層級特徵表時，以下幾件事沒有檢查守著，或要付額外成本（[ADR-0026](../adr/0026-feature-tables-by-join-key.md)〈後果〉）：
  - **重複的 identity 列只在這次組裝的表上抓得到。** B10 四個 split 都查，但 test 只看這次處理的月份；已經落地的 test 月份不會重查。真正擋它的是這張表 source ETL 的 `primary_key` 與 `max_duplicate_key_ratio: 0.0`（[`source_etl.md` §3.6](source_etl.md#36-輸出-quality-checks)），而不變量 A32 不守這項設定：A32 只讀 parameters，拿 `sample_pool`、`label_table`、`feature_table` 這三個固定名字去 ETL 設定裡找表。`candidate_feature_table` 是 catalog 的邏輯名，它在 ETL 設定裡的實體表叫什麼由部署決定，A32 看不到 catalog，也就找不到它。所以這個鍵被刪掉時沒有任何東西會報錯。
  - **類別欄出現 train 沒見過的值時，不會有 warning。** 值照樣編成 `-1`。`feature_table` 的未知值在 `apply_preprocessor_to_features` 數一次；候選層級特徵表在每個 split 組 model input 時才編碼，要數就得每個 split 多一次 Spark 動作。
  - **B8 每次執行多掃一次這張表本次要讀的月份。** 它不落地，沒有框架自己寫的 parquet footer 可讀，框架也不規定部署的表用什麼格式，所以只能掃。每個 split 的組裝只讀自己的月份，B8 則把全部月份掃一次，所以讀這張表的量明顯變多（以廣告示例的月份數算約多 57%）；fit 另外讀 train 月份兩次（月份檢查與詞表）。`numeric_precision_policy: truncate` 只讓超標的值通過，不省這次掃描。
- 多月份資料仍由 Spark lazy execution、shuffle spill 與 Hive partitions 處理；尖峰資源通常取決於單一 shuffle partition 與資料偏斜，而不是月份數本身。

## 10. 相關文件

- 資料一列是一次曝光，要選宣告 `event`、`occasion` 或兩個都宣告（query group 是 entity × 時段，還是一次請求）：[`../operations/user-guides/impression-data-shapes.md`](../operations/user-guides/impression-data-shapes.md)
- 同一個 query group 裡同一個 item 要有多列（一次事件一列）：[`../operations/user-guides/one-row-per-event.md`](../operations/user-guides/one-row-per-event.md)
- 三張來源表的建立方式：[`source_etl.md`](source_etl.md)
- 模型訓練與 dataset version 選擇：[`training.md`](training.md)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、前處理與恢復設計背景：[`../design-principles.md`](../design-principles.md)
