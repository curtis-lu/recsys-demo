---
status: accepted
date: 2026-08-02
---

# `carry_columns` 與 `drop_columns` 的交集是必要設定，不是矛盾

`conf/base/parameters_dataset.yaml` 把 `cust_segment_typ` 同時列進 `carry_columns` 與
`prepare_model_input.drop_columns`。讀起來像「一個欄位被下了兩個相反的指令」，下一個維護者會想
把它「修好」。**它是必要的**，理由是這兩個鍵作用在不同的表。

## 兩個鍵作用在不同的表

| 設定鍵 | 作用對象 | 生效處 | 語意 |
|---|---|---|---|
| `drop_columns` | **`feature_table`** 的欄 | `compute_feature_columns`（`pipelines/dataset/steps/feature_columns.py`） | 黑名單：不得進 `feature_columns` |
| `carry_columns` | **`sample_pool`** 的欄 | `key_output_columns`（`pipelines/dataset/steps/sampling.py`），由 `select_train_keys`／`select_calibration_keys` 呼叫 | 白名單：keys 除 identity 外還要多帶 |
| `feature_columns` | 推導結果，存進 `preprocessor_metadata` | 同上 | identity categoricals ＋（feature_table 欄 − drop − 非 categorical identity − label） |

而 `drop_columns` **會物理刪欄**：`apply_preprocessor_to_features` 選出的欄是 base key ＋ 落在
feature_table 裡的 `feature_columns`（`encoded_frame_columns`，同一個檔），被擋在 `feature_columns`
之外的欄不會進 `preprocessed_feature_table`。

兩件事合起來就是那個交集的來由：某欄若**同時**存在於 `sample_pool`（要 carry）與 `feature_table`，
它必須列進 `drop_columns`，否則 `build_model_input` 的 join 兩側各帶一份同名欄，`select` 撞
ambiguous。實跑真函式的兩情境對照：

```
(1) cust_segment_typ IN drop_columns   [= 現行 conf/base]
  preprocessed_ft cols : ['snap_date','cust_id','tenure_months','gender']   ← 已刪
  model_input cols     : [...,'gender','cust_segment_typ']                  ← 只剩 keys 那份
  rows                 : cust_segment_typ='mass'                            ✅

(2) cust_segment_typ NOT in drop_columns
  preprocessed_ft cols : ['snap_date','cust_id','cust_segment_typ',...]     ← 留著
  AnalysisException: Reference 'cust_segment_typ' is ambiguous              ❌
```

## 決定

1. **交集合法**，語意是「帶進 model_input 但不當特徵」。不加禁止交集的 predicate。
2. **新增 Layer-2 不變量 B7**：`(carry_columns ∩ feature_table.columns) − identity_columns − {label}`
   中任何欄若不在 `drop_columns` → `DataConsistencyError`。單一定義在
   `carry_column_collision_errors`（`core/consistency.py`），掛在 `validate_data_consistency`，
   與 B5/B6 共用同一次 `feature_table.dtypes` 讀取，**零掃描**。（為什麼要再扣掉 identity 與
   label，見下方「實作時的修正」——那是實跑之後才收窄的。）
   **B7 是互斥而非義務**：撞到時「加進 `drop_columns`」與「從 `carry_columns` 拿掉」都合法，
   前者保 carry 棄特徵、後者保特徵棄 carry。閘門不替使用者選——只指名「加進 drop」會讓每個讀到
   的人默默少一個特徵、還為此重建整批 dataset。錯誤訊息兩條都給。
3. `drop_columns` **不改名**（它確實刪欄，`drop` 是準確的），也**不清理**裡面的冗餘項。

加 B7 的理由不是多一層保險，是**當時這條規則沒有任何地方寫下來**：`carry_columns` 的設定註解
只說它給 `training.sample_weights` 用，完全沒提「若這欄也在 feature_table 裡，你必須另外去
`drop_columns` 補一筆」，而踩中的症狀是一句看不懂的 `Reference 'x' is ambiguous`。（實作 B7 時
一併把這條規則補進了設定註解，見 `conf/base/parameters_dataset.yaml` 的 `carry_columns` 段。
註解可以被跳過，閘門不行——兩者不互相取代。）

### 實作時的修正：條件要再扣掉 identity 與 label（2026-08-02，追 code path ＋實跑）

決定 2 原本寫的是「`carry_columns ∩ feature_table.columns` 中任何欄若不在 `drop_columns` →
raise」。實跑真的 `build_model_input` 才發現這個條件**過寬會誤報**：

```
(a) carry=[cust_id]（identity 欄，在 feature_table、未列 drop）
    keys.columns : ['snap_date','cust_id','prod_name']   ← 沒有第二份 cust_id
    build_model_input: OK，無 ambiguity                    ← 但原條件會 raise ❌
(b) carry=[cust_segment_typ]（非 identity，同樣未列 drop）
    keys.columns : [...,'cust_segment_typ']
    AnalysisException: Reference 'cust_segment_typ' is ambiguous  ✅
```

原因：key 選取只把**不在 identity key 裡**的 carry 欄另外附加（`key_output_columns`，
`pipelines/dataset/steps/sampling.py`），base key 又被 join 本身併攏；而 label 與非 categorical
的 identity 欄一律被 `compute_feature_columns` 排除在 `feature_columns` 之外，與 `drop_columns`
無關。對這些欄報錯，等於要求使用者做一次**零行為改變、卻會 bust `base_dataset_version` 的設定
修改**——正是本 ADR 前面反對的那種代價。

### 實作時發現：B7 真正獨佔的只有一格（2026-08-02）

「踩中的症狀是 ambiguous reference」那句對**沒有閘門**的世界成立，但 B6 已經上線，所以 B7 真正
獨佔的是下表第二格：

| 漏掉 drop 的 carry 欄 | 現況（B7 之前） |
|---|---|
| **字串**（如 `cust_segment_typ`） | 它會進 `feature_columns`、是非數值、又沒宣告 categorical → **B6 先擋下**。但 B6 的建議是「宣告成 categorical 或 drop」，選前者的人會保留碰撞、下一步撞 ambiguous |
| **數值 或 已宣告 categorical** | B6 不適用（數值不觸發／已宣告視為會被編碼）→ 直落 `Reference 'x' is ambiguous` |

第一格 B7 **不是取代 B6 的訊息，是多加一則**：閘門是 collect-all 且串接順序為 B1+B5+B6+B7，所以
兩則會同時出現、**B6 那則排在前面**，而它的第一個建議（宣告成 categorical）對 carry 欄不適用。
由上往下照做的人會先繞一圈。若要讓訊息順序符合修法順序，得改閘門的串接次序——那不屬於本 ADR
的決定。

## 為什麼不清那 3 個結構性 no-op 項

`snap_date` / `cust_id` / `label` 被 `compute_feature_columns` 自己的
`(identity − categorical) | {label}` 保證排除，列不列都一樣。但 `dataset.prepare_model_input`
進 `base_dataset_version` 的 hash（`core/versioning.py:112-117` 只剝 `ALL_SAMPLING_KEYS` 與
`COVERAGE_ONLY_KEYS`），**清理一次＝整批重算**，換到的是零行為改變。留著也不是純粹的雜訊：它讓
設定自我說明「這些不是特徵」。

`apply_start_date` / `apply_end_date` **不算冗餘**。它們對這份 `feature_table` 沒作用，但對另一份
部署可能是承重的——「來源表由使用者自定義」是這個框架的前提，設定不該假設 `feature_table` 的
形狀。同理，`warn_missing_drop_columns` 那個 WARNING 是誠實地說「這張表沒有這欄」，不是缺陷的
自白。

## split 之間的 schema 不對稱是推導結果，不是疏漏

train / train_dev / calibration 走抽樣式的 key 選取因此帶 carry；val / test 只 select identity
（`select_val_keys`／`select_test_keys`，`pipelines/dataset/nodes.py`）。這個不對稱與需求對齊：
`sample_weights` 只作用於 train/train_dev，per-segment 評估在 evaluation 階段從 `sample_pool` 取
segment，val/test 不需要 carry。多出來的欄也不會被誤讀——`extract_Xy` 按
`preprocessor_metadata["feature_columns"]` 切欄（`io/extract.py:352`）。

因此測試斷言的是**推導規則**：

> 每個 split 的 `model_input.columns` == identity ∪ {label} ∪ `feature_columns` ∪
> (`carry_columns` ∩ 該 split keys 的欄)

不寫死「train 有、val 沒有」——那會把一個推導結果記成巧合，且改 `carry_columns` 就得改測試。

## 這條 ADR 沒有解決的事

- 生產環境的 `feature_table` 是否真的有 `cust_segment_typ`，**無第一手證據**（repo 的
  `conf/sql/etl/feature/*.sql` 沒有這欄；memory 條目說生產有）。B7 的價值不依賴這個答案，它守的
  是規則本身。
- `core/consistency.py` 的 legend 把 B4 標為 unused。本次沿序號往後取 **B7**，不回填 B4，避免
  未來讀者看到 B4 重新出現而懷疑它被復活。
