# 廣告情境示例

框架的第二個示例部署：**線上廣告推薦的離線訓練與評估**。一份小量合成資料＋一套自成一格的 conf，從上游原始表一路跑到 evaluation。

## 為什麼有這個示例

- **後面幾張票的新路徑要有地方實跑。** `event` 角色（#378）、item 清單從資料數（#379）、多張特徵表（#380）、預測品質指標家族（#381），商銀示例資料一條都走不到；沒有這份資料，那幾張票只能靠單元測試。
- **本機唯一真的跑 source_etl 的地方。** 商銀示例的 `scripts/local_spark_setup.py` 直接把來源表寫進 Hive，跳過 source_etl；這個示例從原始表開始，四條 ETL 都要跑過。第一次實跑就踩到 #390，見〈踩到的框架問題〉。

設計依據：ADR-0021（`time` 維持時段、`event` 是選用角色）、`docs/notes/2026-09-16-event-support-plan.md` 的 P1。

## 資料長什麼樣

```
原始表（Hive: ad_raw）            source_etl（conf/sql/etl/）                 框架的來源表（Hive: ad_recsys）
generate_data.py 產生

impression_log  一次曝光一列 ─┬─ label/label_table.sql ────────────────▶ label_table
  到秒的 event_ts、event_date │                                              │
  campaign_id、creative_format │  sample_pool/sample_pool.sql ◀──────────────┘
  clicked                      │    ＋ user_profile 算 user_segment ──────▶ sample_pool
                               │
user_profile    使用者每週快照 ┼─ feature/feature_user.sql ──┐
slot_dim        版位維度       ┼─ feature/feature_slot.sql ──┼─ feature_user_slot.sql
week_calendar   每週一         ┘                             └─ feature_table.sql ──────▶ feature_table
                                                                                           │
                                  inference_population/inference_population.sql ◀──────────┘
                                                                                ──────▶ inference_population
```

| 角色 | 欄 | 在這個示例裡是什麼 |
|---|---|---|
| `time` | `snap_date` | 一週（每週一）。ADR-0021：`time` 是時段，不是曝光那一秒 |
| `entity` | `[user_id, slot_id]` | 替「某個使用者在某個版位」排一次；400 人 × 3 個版位 |
| `item` | `ad_creative` | `campaign_id` 與 `creative_format` 在 SQL 用 `-` 拼成一欄，4 × 3 ＝ 12 種 |
| `label` | `label` | 這一週在這個版位有沒有點過這個素材 |

- **候選數不固定。** 候選＝那一週真的曝光過的素材，一個 query group 平均兩三個，不是 12 個全配（商銀示例是每位客戶配滿全部產品）。
- **特徵不偷看。** 行為特徵只數 `event_date < snap_date` 的曝光（過去 28 天）；這是來源 SQL 的責任，框架不檢查（ADR-0022）。
- **點擊率刻意偏高（約 14%）。** 母體只有幾百人，照真實的 1% 一週只有個位數正例。這份資料的數字不代表生產。
- **有訊號，但弱。** 年齡層偏好某個活動、裝置偏好某種格式、個人點擊傾向，都寫進了產生器。test 週 202 個有正例的 query group 上，模型 mAP@12 是 0.712，熱門度基準 0.692。一組只有兩三個候選，怎麼排分數都不低，所以兩者差距小；這個示例是拿來驗「路徑跑得通、輸出沒變」，不是拿來比模型好壞。

各週的用途：

| 週（週一） | 用途 |
|---|---|
| 2025-10-27 | 只當特徵的回看窗，不進任何 split |
| 2025-11-03 ～ 2025-12-08（6 週） | train／train_dev（依 `user_id` 切，同一人不跨兩邊） |
| 2025-12-15 | calibration |
| 2025-12-22 | val |
| 2025-12-29 | test；evaluation 評這一週 |
| 2026-01-05 | inference 評分 |

## 怎麼跑

```bash
bash examples/ad/run_e2e.sh             # 整條跑完，寫 examples/ad/data/digest.json
bash examples/ad/run_e2e.sh --compare   # 另外與 baseline_digest.json 逐項比對，不同就 exit 1
```

- 在 worktree 裡跑就寫絕對路徑：`bash /Users/curtislu/projects/recsys_tfb/.worktrees/<name>/examples/ad/run_e2e.sh`。腳本用它所在那棵樹的 `src`，不會跑到 main 的程式碼。
- 每次都從頭來：`setup_local.py` 先刪掉整個 `examples/ad/data/` 再重建。
- 所有本機狀態（warehouse、metastore、模型、報表、`logs/`、`mlruns/`）都落在 `examples/ad/` 底下，已 gitignore，不會碰到 repo 根目錄的 `data/`。原理：CLI 的 `conf/`、`data/` 都相對目前目錄，而 `conf/spark-local/spark-defaults.conf` 的 warehouse 與 metastore 也是相對路徑；`setup_local.py` 起 Spark 後會核對 warehouse 真的在這裡，不在就中止。
- evaluation 只跑 `--post-training`。`evaluation.snap_date` 只有一個值：post-training 要它在 `test_snap_dates` 裡（A22），監控模式要它是 inference 評過分的週，一份 conf 兩邊都滿足不了。
- 不觸發 model promote：inference 與 evaluation 都用 `--model-version` 指定 training 剛產出的版本。

`tests/examples/test_ad_example.py` 守不起 Spark 就驗得到的部分：這份 conf 過得了 CLI 入口的檢查、產生器的 item 等於 conf 逐一列出的清單、原始資料裡真的有同一週同一素材曝光多次的列（`event` 那張票要用）。

## 耗時

整條約 **4 分鐘**。量測：2026-09-17，macOS 8 核、local[*]，機器上另有別的 session 在跑（load average 2.5～5.5）；同一份程式碼連跑兩次，兩次都是 227 秒。

| 步驟 | 秒（第 1 次／第 2 次） |
|---|---|
| setup（產生原始表、寫進 Hive） | 7／8 |
| feature_etl | 28／28 |
| label_etl | 14／15 |
| sample_pool_etl | 14／13 |
| inference_population_etl | 14／14 |
| dataset | 16／16 |
| training（HPO 5 次） | 43／40 |
| inference | 54／56 |
| evaluation --post-training | 28／27 |
| digest | 8／8 |

每一步都是獨立的 CLI 指令，**秒數都含一次 Spark 啟動**。log 裡從建立 `run_id` 到 `SparkSession ready` 約 1 秒；加上 Python 載入，每一步的固定成本是幾秒等級（沒有單獨量）。資料量：原始曝光約 2.1 萬列、`sample_pool` 16,348 列、`feature_table` 12,000 列。這些數字只說明「本機幾分鐘跑得完」，推不出生產成本（見〈沒做的事〉）。

## 基準 digest

`baseline_digest.json` 是後面每張票判斷「廣告情境沒壞」的比較點，由 `digest.py` 產生。它分層記錄，不是整條一個雜湊——哪一層開始不同，問題就從那一層查：

| 層 | 記什麼 |
|---|---|
| `versions` | `base_dataset_version`、`train_variant_id`、`calibration_variant_id`、`model_version` |
| `source_etl` | 四張來源表的列數與內容指紋 |
| `dataset` | 五個 split 的 model_input 列數與內容指紋；`preprocessor.json`、`category_mappings.json` 的內容指紋 |
| `training` | `training_eval_predictions`（分數四捨五入到小數第 6 位） |
| `inference` | `ranked_predictions`（同上） |
| `evaluation` | `metrics.json`、`baseline_metrics.json` 攤平後的指紋（數字四捨五入到小數第 6 位） |

- **內容指紋**：每列對所有欄（依欄名排序）取 `xxhash64` 後整張表加總，與列的順序無關。版本分區欄不進指紋，所以「只有版本號變了」與「內容變了」分得開。
- **沒有會抖動的欄位。** 同一份程式碼連跑兩次，45 個欄位逐一相同（包括模型分數與評估指標），所以 `noisy` 是空的。哪天出現了本來就會變的欄位，把它的路徑前綴與理由寫進 `noisy`，比對時會略過並印出來。

後面的票怎麼用：

1. **還沒打開自己的開關時**，跑 `bash examples/ad/run_e2e.sh --compare`，必須一致——這證明框架改動沒碰壞廣告情境。
2. **打開開關之後**，輸出本來就該變。確認變的是預期的那幾層後，用新的 `data/digest.json` 取代 `baseline_digest.json`，更新裡面的 `measured`，並在 PR 說明寫出哪幾層變了、為什麼。

## 這組 conf 刻意沒打開的東西

這份示例落地時（#373）框架還不支援下面四件事，所以 conf 只用當時就支援的寫法。**每張票自己打開自己的開關，並在同一張票內證明這個示例整條跑綠**——把開關提前寫進來，這張票自己就會紅。

| 功能 | 這份 conf 現在的寫法 | 哪張票打開 |
|---|---|---|
| `event` 角色 | 不宣告；`label_table.sql` 把同一週同一素材的多次曝光聚合成一列（`MAX(clicked)`） | #378 |
| item 清單從資料數 | item 清單（`schema.categorical_values.ad_creative`）逐一列出 12 種；離線推論的候選 `inference.products` 另外照抄一份（A4 要求兩者相同） | #379 |
| 多張特徵表 | 只宣告一張 `feature_table`；使用者、版位兩種粒度已在 `feature_user`、`feature_slot` 分開算好，打開時可以直接各當一張 | #380 |
| 預測品質指標家族 | 不開 | #381 |

## 踩到的框架問題

### 1. time 欄必須叫 `snap_date`

`src/recsys_tfb/pipelines/source_etl/checks.py` 的輸出檢查把 `WHERE snap_date = …` 寫死了，而 A32 強制 `sample_pool`、`label_table`、`feature_table` 都要做這些檢查。time 欄換別的名字，source_etl 就壞。所以這個示例的週也叫 `snap_date`。

### 2. 兩種 SQL 寫法讓第二個日期寫不進去（#390）

source_etl 第一次建表用 Hive CTAS，建出來的欄位標記「可不可以是 NULL」照的是 Spark **最佳化後**的 SQL；之後的日期寫入時，Spark 檢查的卻是**最佳化前**的 SQL。兩邊不一致時，Spark 在寫任何資料之前就拒絕：

```
Cannot write nullable values to non-null column 'snap_date'
```

資料裡其實沒有 NULL。機制、本機實驗與 Spark 原始碼出處都在 #390。這個示例的 SQL 守兩條規則避開它：

1. **`snap_date` 一律取自某張表的欄位，不寫常數。** `to_date('${target_date}')` 在最佳化後變成常數、被存成「不可為 NULL」。沒有日期欄的來源（`slot_dim`、`impression_log` 的週）從 `week_calendar` 取。`DATE '${target_date}'` 本身寫得進去，但這一欄同樣被存成「不可為 NULL」，下游再讀它就撞上規則 2——日期欄幾乎一定會被下游讀，所以不用。
2. **讀上游由 `COALESCE`／`COUNT` 算出來的欄時，外面再包一次 `COALESCE`。** `feature_table.sql` 裡的五個計數欄就是這種情況。

#390 修好之後，這兩條規則與相關 SQL 註解應該一併拿掉，並用 `run_e2e.sh --compare` 證明輸出不變。

公司環境可能不受影響（例如 `spark.sql.storeAssignmentPolicy` 設成 `LEGACY`），怎麼確認見 #390。

## 沒做的事

- **生產規模的資料量估算。** 因為部署層的每日曝光量未知，本機合成資料量推不出生產成本；硬估會變成日後被引用的假數字。
- **監控模式的 evaluation。** 理由見〈怎麼跑〉。
- **修 #390。** 因為它改的是所有部署第一次建表的方式，而且要先查清楚 2026-04 為什麼改成現在的做法，應該單獨審；#373 只避開。
