# HiveTableDataset append-only schema evolution — 設計

- 日期：2026-06-10
- 狀態：設計已核可，待實作
- Branch：`feat/hive-schema-evolution`

## 背景與問題

feature_table 加欄在 source_etl 端已支援（`sql_runner._build_existing_table_statements`：
`ALTER TABLE ADD COLUMNS` ＋ 按表序對齊的 `INSERT OVERWRITE`，append-only、移除欄位
fail-loud）。dataset CLI 也會即時讀 feature_table schema 算 `feature_table_fingerprint`
進 `base_dataset_version`，版本正確 bust、不會與舊快取碰撞。

斷點在 dataset 產物的物理表：catalog 中所有 dataset 產物
（`preprocessed_feature_table`、`val/test/train/train_dev/calibration_model_input` 等）
是**固定名稱的 Hive 表、`base_dataset_version` 只是分區欄**。而
`HiveTableDataset.save()` 的流程是 `columns: "auto"` 從當次 DataFrame 推 schema →
`CREATE TABLE IF NOT EXISTS`（表已存在＝no-op）→ **位置式** `insertInto`，完全沒有
ALTER 邏輯。feature_table 加欄後再跑 dataset pipeline，新版本的 DataFrame 比物理表
多一欄，positional insert 直接拋 AnalysisException。要繼續只能手動 drop 整批
`recsys_prod_*` 表，舊版本分區陪葬——分區版本化形同虛設。

`enriched_eval_predictions` 的 catalog 註解（segment_sources 改變要先 drop table，
「ALTER TABLE schema evolution 是待辦的 follow-up」）是同一個 gap 的已知記錄。

讓修法安全的關鍵事實：下游讀取的欄位清單**不從物理表推**——training/inference 的
特徵欄位來自版本化的 `preprocessor.json`（`extract.py` 的
`preprocessor_metadata["feature_columns"]`），evaluation 的 segment 欄位來自
parameters 設定。物理表變成「跨版本欄位聯集、缺的補 NULL」不污染任何版本。

## 目標

feature_table append-only 加欄後，dataset pipeline（及其他 `columns: "auto"` 的寫入表）
**不需要人工 drop/重建任何 Hive 表**即可寫入新版本分區，舊版本分區完整保留。

## 非目標（YAGNI）

- 欄位移除偵測/警告（df 窄於表是合法情境，直接 NULL 補欄）
- 自動型別升級/cast
- `partition_cols` 演化
- source_etl 重構共用（政策一致即可，實作各自獨立）
- dataset CLI / manifest / versioning 改動

## 設計決策

### D1. 適用範圍：只對 `columns: "auto"` 的表（已與使用者確認）

`"auto"` 的語意本來就是「schema 跟著 DataFrame 走」，演化是其自然延伸。顯式
`columns:` 宣告的表（`ranked_staging`、`ranked_predictions`、
`training_eval_predictions` 等 production 契約表）＝宣告即契約，維持現狀
fail-loud，行為完全不變。不新增任何 config knob。

實際被涵蓋的表：全部 dataset 產物表（keys / model_input / preprocessed_feature_table）
＋ `enriched_eval_predictions`（恰好補掉 catalog 註解裡的待辦）。

### D2. save() 演化流程（僅 `columns: "auto"` 且表已存在）

1. 讀既有表 schema，取非分區欄（排除 `partition_cols` 與 `partition_filter` keys）。
   欄名比對一律 lowercase（Hive 不分大小寫，與 sql_runner 同）。
2. Diff DataFrame schema vs 表 schema，三種情況：

   | 情況 | 行為 |
   |---|---|
   | df 有、表沒有 | 發 `ALTER TABLE db.t ADD COLUMNS (c1 t1, ...)`，型別取自 df |
   | 表有、df 沒有 | 投影時補 `lit(None).cast(表型別)` |
   | 同名不同型別 | raise `ValueError`，列出衝突欄位與兩邊型別 |

3. insert 投影**按表的欄位序**（既有欄在前、新 ALTER 的欄附加在後，再接
   `partition_filter` keys 與 `partition_cols`）。`self._columns` 以表 schema
   為準，不再以 df 推斷——位置式 `insertInto` 從此不信任「df 剛好同序」。

表不存在時維持現行路徑：df 推 schema → CREATE → insert。

### D3. NULL 補欄不 fail-loud 的理由（與 source_etl 取捨不同，刻意）

source_etl 對來源表的「SELECT 少了既有欄」fail-loud，因為來源表是 in-place
overwrite，缺欄＝資料毀損。這裡的表是**分區版本化**：新 `base_dataset_version`
用 `drop_columns` 砍特徵後，新版本分區本來就不該有該欄，舊版本分區仍保有原值，
補 NULL 是正確語意而非錯誤。

### D4. 型別衝突 fail-loud 的理由

Spark ANSI store assignment 允許 double→int 這類數值窄化（runtime 才可能炸），
靜默寫入等於資料毀損。同名異型一律 raise，使用者要嘛上游不改型別、要嘛
版本化重建該表。

### D5. ALTER SQL 放本檔小 helper，不動 `SQLRenderer`

`SQLRenderer.build_alter_add_columns` 耦合 `TableConfig`（source_etl 專用），
抽共用模組是過度設計。兩處政策一致（append-only）、實作各自獨立，
hive_table_dataset.py 內加一個組 `ALTER TABLE ... ADD COLUMNS` 字串的小函式即可。

## 讀取端（不動）

`load()` 維持現狀（分區裁剪 ＋ drop `partition_filter` 欄）。副作用：某版本分區
讀回時會多出其他版本造成的 NULL 欄。安全，理由見「背景」末段——下游欄位清單
全部由版本化 metadata / parameters 驅動。

## 錯誤處理

- 同名異型 → `ValueError`（D4），訊息含表名、衝突欄位、df 型別 vs 表型別。
- ALTER 本身失敗（權限、語法）→ 讓 Spark 例外原樣上拋，不吞。
- 顯式 `columns:` 表 schema 不符 → 行為不變（現狀的 insertInto 失敗）。

## 測試計畫

單元測試（mock SparkSession，沿用 `tests/test_io/test_hive_table_dataset.py` 形態）：

1. 表已存在、df 多欄 → 發出正確的 `ALTER TABLE ... ADD COLUMNS`，insert 投影
   為「表序＋新欄附加」。
2. 表已存在、df 缺欄 → 投影含 `lit(None).cast(<表型別>)`，不 raise。
3. 同名不同型別 → `ValueError`，訊息含欄名與兩邊型別。
4. 顯式 `columns:` 宣告的表 → 完全不走演化路徑（不查表 schema、不 ALTER）。
5. 表不存在 → 現行 CREATE 路徑不變（回歸）。
6. 欄名大小寫不同視為同欄，不誤判為新欄。

真 Spark 整合測試一條（conftest `spark` fixture）：

- 窄 df 首寫建表 → 加一欄的 df 二寫（不同 `base_dataset_version` 分區）→
  驗證：表 schema 變寬、新分區新欄有值、舊分區讀回該欄為 NULL。
  （同時驗證 managed parquet 表 ALTER 後讀舊檔的行為；source_etl 在
  feature_table 上已踩過同路徑。）

## 順手修的文件債

- `catalog.yaml` 開頭「table 名也帶各自的 version 後綴」stale 註解（實際是
  分區版本化）。
- `enriched_eval_predictions` 的「schema evolution 是待辦 follow-up」註解改為
  已支援、segment_sources 改變不再需要先 drop table。
