# 廣告情境示例

框架的第二個示例部署：**線上廣告推薦的離線訓練與評估**。一份小量合成資料＋一套自成一格的 conf，從上游原始表一路跑到 evaluation。離線推論不跑：這份 conf 宣告了候選層級特徵表，推論會在入口被擋下（見〈怎麼跑〉）。

## 為什麼有這個示例

- **後面幾張票的新路徑要有地方實跑。** `occasion` 角色（#428）、item 清單從資料數（#379）、候選層級特徵表（#380）、預測品質指標家族（#381），商銀示例資料一條都走不到；沒有這份資料，那幾張票只能靠單元測試。`event` 角色（#378）已用這份資料跑綠過，現在換成 `occasion` 示範。
- **本機唯一真的跑 source_etl 的地方。** 商銀示例的 `scripts/local_spark_setup.py` 直接把來源表寫進 Hive，跳過 source_etl；這個示例從原始表開始，四條 ETL 都要跑過。第一次實跑就踩到 #390，見〈踩到的框架問題〉。

設計依據：ADR-0021（`time` 維持時段、`event` 是選用角色）、ADR-0025（`occasion` 角色，query group 因它變寬）、ADR-0026（特徵表照接的鍵分兩類）、`docs/notes/2026-09-16-event-support-plan.md` 的 P1。

## 資料長什麼樣

```
原始表（Hive: ad_raw）             source_etl（conf/sql/etl/）                 框架的來源表（Hive: ad_recsys）
generate_data.py 產生

impression_log  一次曝光一列 ──┬─ label/label_table.sql ────────────────▶ label_table
  到秒的 event_ts、event_date  │                                              │
  campaign_id、creative_format │  sample_pool/sample_pool.sql ◀──────────────┘
  clicked                      │    ＋ feature_user 的年資算 user_segment ──▶ sample_pool
                               │
user_profile    使用者每日快照 ┼─ feature/feature_user.sql ──┐
  available_at（隔天清晨）     │                             │
slot_dim        版位維度       ┼─ feature/feature_slot.sql ──┼─ feature_user_slot.sql
week_calendar   每週一         ┘                             └─ feature_table.sql ──────▶ feature_table
                                                                                           │
                                  inference_population/inference_population.sql ◀──────────┘
                                                                                ──────▶ inference_population

impression_log ┐
browse_log     ┼─ feature/feature_realtime.sql ──▶ feature_realtime（一次曝光一列）＝ catalog 的 candidate_feature_table
campaign_dim   ┘
  瀏覽紀錄、活動屬於哪類內容
```

| 角色 | 欄 | 在這個示例裡是什麼 |
|---|---|---|
| `time` | `snap_date` | 一週（每週一）。ADR-0021：`time` 是時段，不是曝光那一秒 |
| `entity` | `[user_id, slot_id]` | 替「某個使用者在某個版位」排一次；400 人 × 3 個版位 |
| `occasion` | `request_id` | 到秒的一次請求（ADR-0025、#428）：同一秒、同一 entity 底下同時展示的若干素材是同一個 query group。平均每次請求 3.96（約 4）個素材 |
| `item` | `ad_creative` | `campaign_id` 與 `creative_format` 在 SQL 用 `-` 拼成一欄，4 × 3 ＝ 12 種 |
| `label` | `label` | 這一次請求裡，這個素材有沒有被點——同一素材在同一週可能出現在好幾次不同的請求裡，各自帶自己的 label |

- **候選數不固定。** 候選＝這一次請求真的展示過的素材，一個 query group（一次請求）平均約 4 個（3.96），不是 12 個全配（商銀示例是每位客戶配滿全部產品）；有些請求只有 1 個候選（佔 4.9%），組內排序沒有意義，mAP 恆為 1。
- **特徵不偷看，而且有檢查。** 這是來源 SQL 的責任，框架不檢查（ADR-0022），所以 `run_e2e.sh` 在 feature_etl 之後跑 `check_features.py`，把 `feature_table` 與 `feature_realtime` 的每一欄特徵、以及 `feature_user` 取的是哪一天的快照（`profile_snap_date`），與照定義用 pandas 重算的值逐列比對。定義三條：
  - 行為特徵只數 `event_date < snap_date` 的曝光（過去 28 天）。
  - 使用者快照取「週一 00:00 已經算好」（`available_at` 不晚於那一刻）的最後一份。快照 D 記 D 當天結束時的狀態，D+1 清晨才算好，批次偶爾晚一天——取週一那份是偷看，只看日期固定取週六那份也會在批次晚的那週偷看。「週一 00:00」是 Spark session 時區的午夜（本示例 Asia/Taipei）。
  - 即時特徵的瀏覽只算曝光前 30 分鐘、不含曝光那一秒。

  資料刻意讓錯的界線看得出差別：點擊後的瀏覽有一半與點擊同一秒，`<` 寫成 `<=` 就算進去了；批次週末常晚一天，固定取前兩天那份就會取錯。
- **點擊率刻意偏高（約 15%）。** 母體只有幾百人，照真實的 1% 一週只有個位數正例。這份資料的數字不代表生產。
- **時間欄寫檔時標上時區。** 產生器的時間是 Asia/Taipei 當地時間；不帶時區寫進 parquet 的話，Spark 3.3 會當成 UTC 讀，晚 8 小時，和 `event_date` 對不上（2026-09-18 實測）。`generate_data.py::write_parquet` 先標上時區再寫。
- **排序指標分得出好壞。** 年齡層偏好某個活動、裝置偏好某種格式、個人點擊傾向，都寫進了產生器。test 週 445 個有正例的 query group（request）上，模型 mAP@12 是 0.748、mAP@1 是 0.497；熱門度基準分別是 0.578、0.282（2026-09-22，含候選層級的三個即時特徵；只有週級特徵時是 0.717、0.457）。候選數與親和力的強度是為此調的：候選只有一兩個時，隨便排的 mAP 也很高，指標分不出好壞。調的時候用不起 Spark 的估算（test 週、有正例的 request、整組 AP，200 次隨機排列取平均）：隨便排 0.557、照過去點擊數排 0.580、照 train 週 (年齡層, 裝置, 版位, item) 分群的點擊率排 0.741（2026-09-21，`generate_data.py` 現值；分群 CTR 用 train／train_dev 六週）。

### 資料涵蓋了什麼、沒涵蓋什麼

後面的票要在這份資料上實跑，先看它走不走得到你要驗的那條路：

| 形狀 | 有沒有 | 誰要用 |
|---|---|---|
| 一次請求展示多個素材（`occasion`） | 有：10,759 個請求，平均 3.96 個素材／請求，分布 `{1: 532, 2: 1052, 3: 2165, 4: 3239, 5: 2158, 6: 1613}`；只有 1 個素材的請求佔 4.9%（那種組排序沒有意義，mAP 恆為 1） | #428 occasion（已用上） |
| 同一週、同一 (使用者, 版位, 素材) 被曝光多次 | 有（分散在不同的請求裡） | #378 event（資料仍撐得住，這份 conf 現在用 occasion 示範） |
| 一個 (使用者, 版位) 一週的曝光次數超過 item 種數 12（`"all"` 不截斷才驗得到） | 有：全部 738 組、test 週 59 組，最多 35 次 | #378 event（資料仍撐得住） |
| 逐筆曝光的即時特徵 | 有：`feature_realtime`，一次曝光一列，鍵是 identity `(snap_date, user_id, slot_id, request_id, ad_creative)`；catalog 的 `candidate_feature_table` 指到它，dataset 以這組鍵接（ADR-0026）。22.9% 的曝光在前 30 分鐘瀏覽過活動同類內容，這些曝光的點擊率 28.4%，其餘 11.1%；同一週同一 (使用者, 版位, 素材) 曝光不只一次的 8,042 組裡，有 3,143 組（39.1%）這個值不是每次都一樣 | #380（已用上） |
| 算即時特徵時偷看的後果 | 有：點擊後 5 分鐘內會瀏覽同類內容（一半就在點擊那一秒），窗口放到曝光那一秒或之後，「有沒有瀏覽」幾乎就是「有沒有點」。`check_features.py` 擋得住（變異檢查見〈怎麼跑〉） | #380 的 as-of 文件與範例 |
| 使用者特徵、版位特徵各自的粒度 | 有：`feature_user`（snap_date, user_id）、`feature_slot`（snap_date, slot_id）。框架不收比 base key 粗的表（ADR-0026），所以兩張在 `feature_table.sql` 併到 (snap_date, user_id, slot_id) | 示範「比 base key 粗的表在 SQL 展開」 |
| 快照日與 `time` 不同（as-of join） | 有：`user_profile` 每天一份，`available_at` 在隔天 05:00～08:00，批次晚一天的日子（平日 10%、週末 50%）再晚 24 小時；每週約 5% 的人在一天中的某個時刻換裝置，點擊看曝光那一刻的裝置。10 週 × 400 人裡，「週一 00:00 拿得到的最後一份」不是週六那份的有 3,200 列（8 週），其中裝置因此不同的 28 列；取「週一那份」會拿到不同裝置的有 86 列。今天的 `feature_user.sql` 以週為單位取後者，這是一個已經被 `check_features.py` 驗過的 as-of 範例（`docs/pipelines/source_etl.md` 3.8 的範例一就是它的一般化）。**逐筆曝光的 as-of 沒有算**（ADR-0022 表格第一列：每次曝光用它當下拿得到的那一份）：要的話，在 `feature_realtime.sql` 裡照曝光那一刻取快照欄，當候選層級特徵。資料已經撐得住——38,557 筆曝光裡（限 feature_etl 的 10 週），照曝光當下取快照與照週一取快照，裝置不同的有 1,448 筆 | 週級的已用上；逐筆的沒有票 |
| 沒見過的 item 屬性組合 | 有：`c04`、`video` 從第一週就有，`c04-video` 從 val 週 2025-12-22 才第一次曝光，train 沒有它。上線後給 4 倍流量（`LATE_ITEM_LAUNCH_BOOST`），test 週有 42 個正例；代價是從 val 週起其他 11 個 item 的曝光占比各降約 10.3%，item 分布在 train 與 val／test 之間本來就不同 | #379 item 清單從資料數；#394 item 宣告成多欄 |
| 低點擊率（分數擠在低端） | **沒有**：點擊率約 15% | #381 預測品質 |

**`c04-video` 對模型來說是一個沒見過的 item 值。** 框架要求 item 那一欄一定是模型特徵（`pipelines/dataset/steps/feature_columns.py::require_item_is_a_feature`），模型看到的是 `ad_creative` 這一個類別值，不是活動、格式兩個屬性，所以 train 沒出現過就不認得。item 宣告成多欄（#394）之後也一樣：那張票省掉的是拼欄與逐一列出組合，模型看的仍是組合。實跑時框架對這件事沒有任何警告（類別編號從 conf 的清單讀，train 有沒有出現不影響；#379 要加的就是這個警告）。test 週這個 item 的 42 個正例上，模型的平均名次 3.6、`map_attr@12` 0.435，熱門度基準是 4.6、0.333。

低點擊率沒做：母體幾百人，1% 一週只剩個位數正例，其他票反而驗不了。#381 也沒有改資料：預測品質指標「分數範圍取本次資料的最小～最大、不寫死 `[0, 1]`」這條路，不論分數擠不擠都會走到，擠在低端時的分箱由單元測試（分數落在 0.001～0.011 的資料，`tests/test_evaluation/test_prediction_quality.py`）證明。

各週的用途：

| 週（週一） | 用途 |
|---|---|
| 2025-10-27 | 只當特徵的回看窗，不進任何 split |
| 2025-11-03 ～ 2025-12-08（6 週） | train／train_dev（依 `user_id` 切，同一人不跨兩邊） |
| 2025-12-15 | 不進任何 split。原本是 calibration 週；#411 移除 calibration 後資料照產、ETL 目標日期不動（這樣升級前後每一層才比得起來），只是沒有 split 讀它 |
| 2025-12-22 | val |
| 2025-12-29 | test；evaluation 評這一週 |
| 2026-01-05 | inference 評分用的週；`inference_population_etl` 照樣產出，但推論本身被擋下（見〈怎麼跑〉） |

## 怎麼跑

```bash
bash examples/ad/run_e2e.sh             # 整條跑完，寫 examples/ad/data/digest.json
bash examples/ad/run_e2e.sh --compare   # 另外與 baseline_digest.json 逐項比對，不同就 exit 1
```

- 在 worktree 裡跑就寫絕對路徑：`bash /Users/curtislu/projects/recsys_tfb/.worktrees/<name>/examples/ad/run_e2e.sh`。腳本用它所在那棵樹的 `src`，不會跑到 main 的程式碼。
- 每次都從頭來：`setup_local.py` 先刪掉整個 `examples/ad/data/` 再重建。
- 所有本機狀態（warehouse、metastore、模型、報表、`logs/`、`mlruns/`）都落在 `examples/ad/` 底下，已 gitignore，不會碰到 repo 根目錄的 `data/`。原理：CLI 的 `conf/`、`data/` 都相對目前目錄，而 `conf/spark-local/spark-defaults.conf` 的 warehouse 與 metastore 也是相對路徑；`setup_local.py` 起 Spark 後會核對 warehouse 真的在這裡，不在就中止。
- evaluation 只跑 `--post-training`。`evaluation.snap_date` 只有一個值：post-training 要它在 `test_snap_dates` 裡（A22），監控模式要它是 inference 評過分的週，一份 conf 兩邊都滿足不了。
- 不觸發 model promote：evaluation 用 `--model-version` 指定 training 剛產出的版本。
- **inference 預期被擋下。** 這份 conf 宣告了候選層級特徵表（catalog 的 `candidate_feature_table`），而離線推論的候選是框架自己產生的 entity × item、沒有一筆被展示過，那張表一列都接不到，所以 CLI 在入口以 A47 擋下（ADR-0026 決定 5）。腳本確認它真的在入口停下、訊息帶 `(A47)`；沒停下或停在別的原因都算失敗。副作用：**宣告 `occasion` 的離線推論在這個示例裡不再實跑**。要實跑，把 catalog 的 `candidate_feature_table` 條目註解掉、從 dataset 重跑——選錯鍵的問題在那裡會直接報錯，不會悄悄算錯（ADR-0026〈考慮過、沒選的做法〉）。
- feature_etl 之後有一段「特徵不偷看」檢查（`check_features.py`），見〈資料長什麼樣〉。它擋得住偷看：把 SQL 改成六種錯的寫法——瀏覽窗口多看到曝光後 5 分鐘、窗口上界 `<` 寫成 `<=`、快照取週一那份、不看 `available_at` 固定取前兩天那份、疲乏次數把這一次也算進去、行為計數多算到週一當天——各自重跑 feature_etl，六次檢查都紅在被改的那一欄（2026-09-18，在資料改成「一次請求多個素材」之前量的：例如 `<=` 那一種有 3,019 列的同類瀏覽次數與定義不同，固定取前兩天那種有 3,200 列取錯快照日。改資料之後沒有重做這六次）。

`tests/examples/test_ad_example.py` 守不起 Spark 就驗得到的部分：這份 conf 過得了 CLI 入口的檢查；產生器的 item 等於 conf 逐一列出的 item 清單，每支拼 item 的 SQL（`label_table.sql`、`feature_realtime.sql`）用的分隔字元都與產生器相同；catalog 條目與根目錄那份一致（框架新增條目時兩邊要一起加；`candidate_feature_table` 是部署自選的條目，不在比對內）；候選層級特徵表的 ETL `primary_key` 就是 identity、而且擋重複（框架的 A32 看不到這張表，由示例自己守）；ETL 的日期涵蓋每個 split；原始資料真的有上表標「有」的形狀。即時特徵與快照的形狀照 `check_features.py` 的同一份定義算，所以「測試說原始資料有這個形狀」和「實跑說 SQL 算對了」講的是同一個定義。

## 耗時

整條約 **3 分多鐘**（194 秒）。量測：2026-09-22 跑一次，macOS 8 核、local[*]，跑完當下的 load average 是 2.2（前 5 分鐘平均 3.8：同一台機器上另有背景工作）。比 2026-09-21 的 247 秒少，主要是離線推論被擋下，省掉那一步的 53～57 秒。

| 步驟 | 秒 |
|---|---|
| setup（產生原始表、寫進 Hive） | 10 |
| feature_etl | 34 |
| check 特徵不偷看 | 10 |
| label_etl | 14 |
| sample_pool_etl | 14 |
| inference_population_etl | 14 |
| dataset | 17 |
| training（HPO 5 次） | 41 |
| inference（在入口被 A47 擋下） | 2 |
| evaluation --post-training | 28 |
| digest | 9 |

每一步都是獨立的 CLI 指令，**秒數都含一次 Spark 啟動**。log 裡從建立 `run_id` 到 `SparkSession ready` 約 1 秒；加上 Python 載入，每一步的固定成本是幾秒等級（沒有單獨量）。資料量：原始曝光 42,555 列、瀏覽 45,725 列、使用者快照 33,600 列；`sample_pool` 38,557 列、`feature_table` 12,000 列、`feature_realtime` 38,557 列、train model_input 19,674 列。這些數字只說明「本機幾分鐘跑得完」，推不出生產成本（見〈沒做的事〉）。

## 基準 digest

`baseline_digest.json` 是後面每張票判斷「廣告情境沒壞」的比較點，由 `digest.py` 產生。它分層記錄，不是整條一個雜湊——哪一層開始不同，問題就從那一層查：

| 層 | 記什麼 |
|---|---|
| `versions` | `base_dataset_version`、`train_variant_id`、`model_version` |
| `source_etl` | 五張來源表的列數與內容指紋：`feature_table`、`candidate_feature_table`（＝`feature_realtime`）、`label_table`、`sample_pool`、`inference_population` |
| `dataset` | 四個 split 的 model_input 列數與內容指紋；`preprocessor.json`、`category_mappings.json` 的內容指紋 |
| `training` | `training_eval_predictions`（分數四捨五入到小數第 6 位） |
| `evaluation` | 評估目錄下每份 JSON 攤平後的指紋（`metrics.json`、`baseline_metrics.json`、`prediction_quality.json`、`report_aggregates.json`、`segment_columns.json`、`diagnosis/` 五份診斷；數字四捨五入到小數第 6 位） |

- **內容指紋**：每列對所有欄（依欄名排序）取 `xxhash64` 後整張表加總，與列的順序無關。版本分區欄不進指紋，所以「只有版本號變了」與「內容變了」分得開。
- **排除的東西**：JSON 裡的 `config_fingerprint`（設定的雜湊，新增一個「算的」設定鍵就會變，指標值卻沒動；放進來的話 #381 那種改動會讓這裡紅，「指標值逐值不變」反而驗不出來）；評估目錄的 `manifest.json`（執行紀錄，含執行時間、run_id、git commit）。
- **沒有會抖動的欄位。** 2026-09-21 以現在的資料連跑兩次，58 個欄位逐一相同（包括模型分數、評估指標與診斷），所以 `noisy` 是空的。哪天出現了本來就會變的欄位，把它的路徑前綴與理由寫進 `noisy`，比對時會略過並印出來。

後面的票怎麼用：

1. **還沒打開自己的開關時**，跑 `bash examples/ad/run_e2e.sh --compare`，必須一致——這證明框架改動沒碰壞廣告情境。
2. **打開開關之後**，輸出本來就該變。先說出「哪幾層應該不變」再跑——例如 #380 接上候選層級特徵表時，`source_etl` 層除了那張表之外四張都該相同、`train_variant_id` 該相同，其餘因為模型多了特徵而改變；預測品質指標（#381）只新增產物，既有的評估 JSON 應該相同。確認只有預期的那幾層變了，再用新的 `data/digest.json` 取代 `baseline_digest.json`，更新裡面的 `measured`，並在 PR 說明寫出哪幾層變了、為什麼。

## 這組 conf 刻意沒打開的東西

這份示例落地時（#373）框架還不支援下面四件事，所以 conf 只用當時就支援的寫法。**每張票自己打開自己的開關，並在同一張票內證明這個示例整條跑綠**——把開關提前寫進來，這張票自己就會紅。

| 功能 | 這份 conf 現在的寫法 | 哪張票打開 |
|---|---|---|
| `event` 角色 | 曾經打開過（#378）：`event: impression_id`，以這份資料跑綠過形狀一（同一 entity × 時段一個 query group，event 只進 identity）。#428 換成 `occasion` 之後不再宣告——想改回去，見 `conf/base/parameters.yaml` 的 schema 註解列出要改哪幾個檔 | #378（已改用 occasion 示範） |
| `occasion` 角色 | **已打開**（#428）：`occasion: request_id`，`label_table.sql` 與 `sample_pool.sql` 都以一次請求為一個 query group，兩張表的 `primary_key` 與 `training_eval_predictions` 的 catalog 欄位都含 `request_id`。#380 接上逐筆的即時特徵之前，同一次請求裡的素材分數只靠週級特徵、容易同分；接上之後同分列是 0（報表〈完整性檢查〉的同分列佔比） | #428（完成） |
| item 清單從資料數 | item 清單（`schema.categorical_values.ad_creative`）逐一列出 12 種，包括 train 沒有的 `c04-video`；離線推論的候選 `inference.products` 另外照抄一份（A4 要求兩者相同） | #379 |
| 候選層級特徵表 | **已打開**（#380）：catalog 的 `candidate_feature_table` 指到 `feature_realtime`，它多帶了 `request_id`、`primary_key` 改成 identity；`impression_id`、`event_ts` 列進 `drop_columns`。模型多了 `browse_30m`、`browse_same_category_30m`、`prior_exposures_this_week` 三個特徵。代價是離線推論被擋下（見〈怎麼跑〉） | #380（完成） |
| 預測品質指標家族 | **已打開**（#381）：`report.sections.prediction_quality: true`，`prediction_quality` 的三個值與框架預設相同。這個示例只跑 `--post-training`；test 表留下一半沒有點擊的請求（下一列），報表這一段的數字用權重加權、代表還原到全部曝光的估計 | #381（完成） |
| 沒有正例的 query group 留多少 | **已打開**（#429）：`val_zero_positive_group_ratio: 0.5`、`test_zero_positive_group_ratio: 0.5`，沒有點擊的請求整個留一半，列上帶權重 `zero_positive_group_weight`（1 或 2），`training_eval_predictions` 的 catalog 欄位宣告了它。train 不設（objective 是 `binary`，r < 1 會變成負例降採樣）。預測品質打開時 test 的 r 必須大於 0（A46），所以這一列與上一列是一起的。0.5 只是示例值 | #429（完成） |

## 踩到的框架問題

### 1. time 欄必須叫 `snap_date`（#390 的第二件）

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
- **item 宣告成多欄。** 使用者要能直接寫 `item: [campaign_id, creative_format]`，不必在 SQL 拼、也不必逐一列出所有組合；模型看的仍是組合。今天的框架 item 恆為一欄，改它會推翻 `CONTEXT.md` 的 **item** 與 #379 原本的 Out of scope，由 #394 處理。資料已經備好：原始表的 `campaign_id`、`creative_format` 是分開的兩欄。
- **修 #390 的第二件（輸出檢查寫死 `snap_date`）。** 理由同上：改的是框架，#373 只沿用 `snap_date` 這個欄名避開。
