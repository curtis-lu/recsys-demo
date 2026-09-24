# evaluation pipeline

> 將指定模型與 `snap_date` 的預測整理成統一評估資料，以 `(time, entity)` 為 query group 計算排序指標，並產生標準報表或模型比較報表。
> 主要流程為：選擇預測來源 → 補入 ground truth、rank 與分群欄位 → 計算排序指標 → popularity baseline →（打開時）預測品質指標 → HTML 報表 → 持久化 enriched predictions。

## 1. Pipeline 總覽

| 項目 | 說明 |
|---|---|
| 主要用途 | 模型上線前 test 評估、上線後成效監控，以及模型／外部結果比較 |
| 執行指令 | `python -m recsys_tfb evaluation` |
| 主要輸入 | `training_eval_predictions` 或 `ranked_predictions`、`label_table`、該模式的母體表（`sample_pool` 或 `inference_population`，分群用）、可選的 segment 覆寫與 compare sources |
| 主要輸出 | `report.html`、可選的 `report_comparison.html`、`enriched_eval_predictions` |
| 設定檔 | `conf/base/parameters_evaluation.yaml` |
| I/O 設定 | `conf/base/catalog.yaml` |
| 上游 pipeline | `training` 或 `inference` |
| 下游用途 | 模型人工審核、上線監控與模型比較 |

evaluation 有兩種評估情境，差別在 Model A 的預測來源：

| 情境 | 指令 | 預測來源 | Ground truth | 使用時機 |
|---|---|---|---|---|
| Training 後評估 | `evaluation --post-training` | `training_eval_predictions` | 沿用 training 當時保存的 test label | 候選模型剛訓練完成，進行上線前審核 |
| 上線後監控 | `evaluation` | inference 發布的 `ranked_predictions` | 依目前 `label_table` 補入 | label 觀察窗成熟後，追蹤正式排序結果 |

兩種情境都使用相同的排序指標與報表。`--post-training` 不是另一套 metric，只是改讀 training 的 held-out test 預測；預設模式則評估已通過 inference sanity check 並正式發布的結果。

evaluation 另有三種執行模式：

| 模式 | 旗標 | 標準報表 | 比較報表 | 更新 enriched data |
|---|---|:---:|:---:|:---:|
| 標準 | 無 compare 旗標 | ✓ |  | ✓ |
| 標準加比較 | `--compare <key>` | ✓ | ✓ | ✓ |
| 只比較 | `--compare-only <key>` |  | ✓ |  |

`--compare-only` 不代表只計算部分指標；它會直接重用先前持久化的 Model A `enriched_eval_predictions`，略過標準評估、baseline、diagnostics 與 `report.html`，只重新載入 Model B、對齊共同母體並產生比較報表。

## 2. 執行前準備

執行 evaluation 前，建議依序確認：

1. **模型版本存在**：evaluation 會讀取 `data/models/<model_version>/manifest.json`，取得該模型使用的 base 與 train dataset IDs。#411 之前的模型 manifest 多帶一個 `calibration_variant_id`，讀取時會略過，不影響評估。
2. **選對評估情境**：候選模型的 test 評估使用 `--post-training`；已發布批次結果的監控使用預設模式。
3. **候選模型明確指定版本**：省略 `--model-version` 時一律解析 `data/models/best`。尚未 promotion 的新模型必須明確傳入版本，否則可能評估到上一個正式模型。
4. **預測 partition 已存在**：post-training 模式需要對應 `training_eval_predictions`；監控模式需要 inference 已成功發布對應的 `ranked_predictions`。
5. **Ground truth 已成熟**：上線後監控必須等 label 觀察窗結束並完成資料回補。過早執行會將尚未發生或尚未入庫的正例視為負例。
6. **評估日期正確**：`evaluation.snap_date` 的每一個日期都必須與預測表中的日期格式和值一致，並使用 ISO `YYYY-MM-DD`（一個或多個日期的寫法見 3.1 節）。
7. **分群欄在母體表上**：`segment_columns` 的欄取自該模式的母體表（`--post-training`＝`sample_pool`、監控＝`inference_population`）。母體表沒有某欄時不中止，只在 log 與報表註明；要從別張表取，就用 `segment_sources` 覆寫，覆寫表讀不到或缺欄會立即中止（見 3.2 節）。
8. **比較來源已準備**：使用 `--compare`／`--compare-only` 前，先確認 `compare_sources` key、來源表、model version、item mapping 與日期 coverage。

監控模式會以預測 rows 為母體，依 `time + entity + item` left join `label_table`；沒有 label row 的候選會補成 `label = 0`。

> ⚠ **已知風險（item 清單從資料數時，#379）**：監控模式的預測來自離線推論，而離線推論只替 item 清單裡的 item 評分——上線後才出現的新 item 沒有預測列。`label_table` 裡新 item 的正例接不上任何預測，在上面那個 left join 裡被丟掉；若它是某組唯一的正例，那一組會變成沒有正例、不進排序指標。所以**監控模式看不出漏掉新 item 的損失**。要看，就每月加一個評估月份跑 `--post-training` 評估（`docs/operations/user-guides/adding-an-eval-month.md`）：那裡的候選來自 `sample_pool`，新 item 照樣被排名、算進整體與 per-item 指標。
`label_table` 在這三欄上必須唯一（兩種模式都檢查，只看評估的那個月）：有重複 key 時 `prepare_eval_data` 直接失敗並印出重複的 key 數，不會替你挑一列。
這適用於「label table 只保存正例」的 sparse table，但前提是缺 row 的業務語意確實代表負例，而不是 ground truth 尚未成熟。

post-training 模式會保留 `training_eval_predictions` 已保存的 label，不以後來更新的 `label_table` 覆寫，讓 evaluation 結果與 training 當時的 test 指標保持一致。

## 3. 設定方式

### 3.1 評估日期與 K

```yaml
evaluation:
  snap_date: "2026-01-31"
  k_values: [1, 2, 3, 4, 5, "all"]
```

| 設定 | 說明 |
|---|---|
| `snap_date` | 本次評估的時間切點：一個日期，或多個日期（三種寫法見表下）。日期一律使用 `YYYY-MM-DD` |
| `k_values` | @K 家族（map@K、precision@K、recall@K、map_attr@K）要實際計算的 K 值 superset。`evaluation.metric.k` 是另一個獨立的軸：主指標 per-item macro 點估與 CI 的截斷深度。它非 null 時，per-item 那一族（map_attr、hit_rate）會多算 `@metric.k`，overall／per-segment 的 @K 家族不受影響，所以不必自己把它列進 `k_values` |
| `"all"` | 在細 item 粒度解析為 distinct item 數；在 category 粒度重新解析為 distinct category 數。宣告 `event` 時，細 item 粒度改取最寬 query group 的列數（一組 30 列、12 個 item 時取 12 會截斷），並把這個數寫進評估指標檔 `metrics.json`（catalog 的 `evaluation_metrics`）的 `all_k`；報表、比較報表與 training 的 test mAP 都照同一個鍵查 `@all`，不自己數 item（#434）。category 粒度不變：聚合之後每組每個大類只剩一列 |

`snap_date` 有三種寫法：

```yaml
# 一、一個日期
snap_date: "2026-01-31"

# 二、多個日期，逐一列出
snap_date: ["2026-01-31", "2026-02-28", "2026-03-31"]

# 三、多個日期，寫成區間（與第二種完全等價）
snap_date:
  start: "2026-01-31"
  end: "2026-03-31"
  step: month_end      # day | week | month_start | month_end
```

區間在載入設定時就展開成清單，之後的程式只看得到清單，所以第三種與第二種算出同一個設定指紋。區間**含頭含尾，而且起迄兩端都必須落在 step 上**：`month_end` 要每月最後一天、`month_start` 要每月第一天、`week` 從 `start` 起每 7 天一個、`day` 每天一個。不在 step 上（例如 `step: month_end` 配 `start: "2026-01-30"`）直接報錯，不會自動挪到最近的日期。

多個日期是**合起來評估**：所有日期的 query group（`time × entity`）放進同一份指標、同一份報表，不是每個日期各出一份。同一個 entity 在兩個日期是兩個 query group。要逐月各看一份，就逐月各跑一次。

pipeline 會先依 `model_version` 與 `snap_date` 篩選預測。**每一個**設定的日期都必須有資料：任一個日期沒有資料時，列出沒有資料的日期與該模型實際存在的日期後中止，不會退回整張表計算，也不會少評估一個月照常跑完。

帶 `--post-training` 時另有一道更前面的把關（一致性不變量 A22）：`evaluation.snap_date` 的每一個日期都必須是 `dataset.test_snap_dates` 的成員，否則在 Spark 起來之前就報錯退出；多個日期時，訊息列出不在其中的那幾個。這條之所以不能只靠上面那個「零列就中止」的檢查：`training_eval_predictions` 累積該 `model_version` **歷來預測過的每一個月**（test 日期不進版本身分，見 [ADR-0001](../adr/0001-test-dates-out-of-dataset-version-identity.md)），所以一個已經從 `test_snap_dates` 移除的月份照樣抓得到 rows，跑出一份看起來完全正常、卻在量目前設定不評估的月份的報表。**monitoring（不帶旗標）模式不受此限**——它讀 inference 產出的 `ranked_predictions`，月份本來就不必是 test 月份；這也是這條檢查由 CLI 帶旗標呼叫、而不是寫成一般 config predicate 的原因（Layer-1 在 CLI entry 執行，看不到旗標）。

`k_values` 決定 metric computation；`report.display.primary_map_k` 與 `guardrail_recall_k` 只決定報表顯示哪些已計算結果，而 `guardrail_recall_k` **只影響比較報表**（`report_comparison.html` 的 per-item recall@k 與大類 per-item recall@k 兩張表；主報表不讀這個鍵，鍵缺席時比較報表用 `[1, 3, 5]`）。display 中使用的 K 應包含在 `k_values`，否則報表對應欄位會沒有值。display 清單在每個粒度會先濾掉大於該粒度 `"all"` 所解析的 K 的整數 K（`"all"` 保留；bug 8, ADR-0020）。沒宣告 `event` 時那個 K 就是 item 數：預設 `primary_map_k: [1, 3, 5, "all"]` 遇到 3 個大類只印 @1、@3、@all——只是過濾不印，計算層照 `k_values` 全集算。宣告了 `event` 時界線是最寬 query group 的列數，因為比 item 數深的名次上還有真的列要排。比較報表裡把算過的鍵整批攤開的 overall／大類 overall 表，套同一條規則，但兩側**各自**以自己的 K 為界：兩側各保留自己的列、在同一個 common universe 裡最寬的組可能不一樣長；某側超過自己 K 的格子留空，Δ 也留空。

主要指標包括：

| 層次 | 指標 |
|---|---|
| Overall／per-segment | `map@K`、`precision@K`、`recall@K` |
| Per-item | `hit_rate@K`、`map_attr@K`、`mean_pos` |
| Macro average | 對 item、segment 或 item-segment 等權平均 |

NDCG 不算，報表與落地的 `metrics.json` 都沒有它（[ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 4）。`training.algorithm_params.metric: ndcg` 是 LightGBM 自己的 early-stopping 指標，跟這裡無關。

overall 指標先在每個 query group 計算，再對 query 等權平均；per-item attribution 則只在該 item 為正例的 rows 上彙整。兩者回答的問題不同，不應將 `map_attr@K` 誤讀為單一 item 自己的 mAP。

完全沒有正例的 query group 無法定義 AP，因此會從 metric computation 排除；報表仍會記錄 `n_queries` 與 `n_excluded_queries`。

### 3.2 分群評估

```yaml
evaluation:
  segment_columns:
    - cust_segment_typ
```

`segment_columns` 只列欄名。欄從哪張表來，由執行模式決定——segment 跟著**被評估的母體**走（[ADR-0020](../adr/0020-evaluation-bug-round-intended-behaviours.md) bug 6）：

| 評估情境 | 預測來源 | segment 取自 | 為什麼 |
|---|---|---|---|
| 監控模式 | `ranked_predictions` | `inference_population` | 被評分的就是它 |
| post-training 模式 | `training_eval_predictions` | `sample_pool` | 測試集從它抽出 |

兩者都以 `(time, entity)` 當 key：母體表先依 key 去重（`sample_pool` 一個 entity 有多列 item），再 left join 到連好 label 的預測列上，不會 fan-out。輸入原本已有同名欄（例如 `label_table` 帶進來的）會先移除，以母體表為準。

**哪些欄 join 得進來，落地成一份檔。** `prepare_eval_data` 只讀母體表的欄位清單（metastore，不掃資料），把結果寫成 `segment_columns.json`（catalog 條目 `evaluation_segment_columns`，`prepare_eval_data` 的第二個 output）：

| 鍵 | 內容 |
|---|---|
| `joined` | 這次實際 join 進來的欄，照 `segment_columns` 的順序 |
| `sources` | 每個 joined 欄取自哪張表（母體表寫 catalog 條目名，覆寫表寫設定的 `table`） |
| `missing` | 母體表沒有、因此跳過的欄 → 那張表 |
| `config_fingerprint` | 算它時的「算的」設定（見 7.2 節）；寫進去供事後查看，沒有 node 比對它 |

分群的 node（`compute_metrics`、`compute_baseline_metrics`、`draw_diagnosis_sample_node`、`generate_comparison_report`）照 `joined` 分群，**不看 frame 的欄位**：`enriched_eval_predictions` 兩種模式共用一張表，另一模式 join 過的欄在這次的列上是全 NULL，照欄位挑會把它挑進來。`--compare-only` 沒有 `prepare_eval_data`，靠寫 enriched partition 那次落地的這份檔知道當時 join 了哪些欄。

**對不到與缺欄是兩件事：**

- 母體表**有**這欄、某些 query 在上面沒有值（或覆寫表對不到那個 key）：這群叫 `(unmatched)`，出現在分群表、印 query 數與佔比，但**不進任何 macro 平均**——一個假客群不該等權拉動平均。真實 segment 值剛好叫 `(unmatched)` 時直接失敗，不會被併進去。
- 母體表**沒有**這欄：這欄本次不算 per-segment，log 印 WARN、報表「基本統計」段印「`<表>` 無欄 `<欄>`」，pipeline 照常跑完。`segment_columns` 的欄名打錯也走這條路（一致性檢查在 Spark 起來前跑，看不到母體表），靠訊息裡的欄名分辨。那句註記只印在「基本統計」段；關掉該段（`report.sections.dataset_overview: false`）時，看 log 的 WARN 或 `segment_columns.json` 的 `missing`。表名：母體表記的是 catalog 條目名（`sample_pool`／`inference_population`），覆寫表記設定的 `table`。

示例環境的 `conf/sql/etl/inference_population/inference_population.sql` 只有 `(snap_date, cust_id)`，所以示例的監控模式會走「缺欄」這條路。要在示例裡看到分群，是那支 SQL 要多帶分群欄，框架不用改。

**覆寫：segment 在另一張表時**

```yaml
evaluation:
  segment_columns:
    - holding_combo
  segment_sources:
    holding_combo:                       # 鍵＝segment_columns 裡的欄名
      table: ml_recsys.holding_combo     # Hive-qualified
      key_columns: [cust_id, snap_date]
      segment_column: holding_combo      # 必須等於上面的鍵
```

有覆寫的欄改讀那張表（依 `key_columns` 去重後 left join），不看母體表。三個欄位缺一個、或 `segment_column` 與鍵不同，CLI 入口的一致性檢查（A10）直接擋下；覆寫表讀不到或缺欄，執行時立即中止——覆寫是明確的設定，跟「母體表剛好沒有這欄」不同。覆寫用欄名查，所以 `segment_sources` 的鍵必須是 `segment_columns` 裡的欄名：鍵不在清單裡的條目（例如舊寫法把鍵取成別名、靠 `segment_column` 對應）A10 同樣擋下——不擋的話它會通過檢查、從不被 join，那一欄悄悄改從母體表取。

目前 metric pipeline 只用 `joined` 的第一欄計算 per-segment 與 per-item-segment 指標；若要評估多種分群，應分次調整欄位順序並執行 evaluation。

### 3.3 item 大類

```yaml
evaluation:
  item_categories:
    enabled: true
    unmapped: singleton
    mapping:
      fund: [fund_stock, fund_bond, fund_mix]
      exchange: [exchange_fx, exchange_usd]
      ccard: [ccard_bill, ccard_cash, ccard_ins]
```

啟用後，框架會在細 item 指標之外，再將每個 query group 的 items 彙整為 category：

- category score = 子 items 的最大 score，等同採用排名最前的子 item。
- category label = 子 items 的最大 label，只要任一子 item 為正例，該 category 即為正例。
- segment 欄位沿用 query group 中的值。
- 同一套 overall、per-item、per-segment 與 macro metrics 會在 category 粒度再計算一次。

mapping 右側的 item 必須是已知的 item，未知 item 會 fail-fast：逐一列出時是 `schema.categorical_values[item]`；item 清單從 train 時段的資料數出來時（#379）是**被評估模型的清單**——`prepare_eval_data` 從這個模型的前處理器讀出、落地在 `item_categories.json` 的 `known_items`（前處理器檔不在時要先補回，否則讀大類的 node 會擋下並說明）。未出現在 mapping 的 item 目前只支援 `unmapped: singleton`，也就是各自成為單獨 category。

**對照表裡找不到大類的 item 自己成一類**（#379）：大類那一輪對 item 是 left join，找不到的取 item 本身當大類，不會被丟掉。這在 item 清單從資料數、val／test 冒出新 item 時才會發生（新 item 不在任何已知清單裡）；不這樣做的話，新 item 的列會悄悄從大類指標消失，一組若只有它是正例，大類那一輪會少算這一組，兩輪數的 query group 就不一樣了。逐一列出時每個 item 都有大類（mapping 或 singleton），結果與以前的 inner join 逐列相同。

同一 item 不應重複出現在多個 categories；目前實作會以後讀到的 mapping 覆蓋先前結果，沒有額外衝突檢查。

#### 大類取自候選的某一欄（`column`，#379，只在 `--post-training`）

不想手寫 mapping 時，改寫 `column`，每個 item 的大類就從 `sample_pool` 那一欄讀：

```yaml
evaluation:
  item_categories:
    enabled: true
    unmapped: singleton   # 這個模式下＝「那一欄全是 NULL 的 item 自成一類」
    column: campaign_id   # sample_pool 的任何一欄；與 mapping 二選一
```

- **讀哪些列**：這次評估的月份（`evaluation.snap_date`）在 `sample_pool` 的列，不看 train 月份。`prepare_eval_data` 讀一次，對照表落地成 `item_categories.json`（catalog 條目 `evaluation_item_categories`，`prepare_eval_data` 的第三個 output）；`compute_metrics`、`compute_baseline_metrics`、`generate_comparison_report` 都照這份排大類。沒設 `column` 時這份檔也照樣落地，`mapping` 是空的，讀者照舊從設定讀手寫的 mapping。
- **這一欄可以是組成 item 的其中一欄**（例：item 宣告成 `[campaign_id, creative_format]` 時的 `campaign_id`）。框架先把這一欄抄出來再拼 item，所以拼完丟掉原欄不影響它。
- **NULL**：同一個 item 在別列有非 NULL 的值，就忽略 NULL；全部都是 NULL，這個 item 自成一類。
- **大類不隨時間變**：同一個 item 在這次評估的列裡（同一個月或跨月）對到兩個以上不同的非 NULL 值，`prepare_eval_data` 就擋下（B18），訊息列出 item、各個值與各自出現的月份。理由：多個評估月份是一起算的，對照表只用 item 連接，一個 item 有兩列的話，它的預測會被複製進兩個大類、重複計數。
- **不支援監控模式**：監控模式的母體是 `inference_population`，沒有 `sample_pool` 的候選欄可讀。沒加 `--post-training` 就設了 `column`（只下 `--compare-only` 也算監控模式），CLI 在 Spark 啟動前就擋下（A51）；`column` 與 `mapping` 同時寫（`mapping: null` 算沒寫）、`column` 不是欄名字串、或 `unmapped` 寫了 `singleton` 以外的值，也一樣擋下。同一份設定因此不能同時給監控模式用手寫 mapping、給 post-training 用 `column`：要兩種都跑，就在不同的 env 覆蓋層各寫一種。
- **改了 `column` 要從 `prepare_eval_data` 重跑**：對照表在那裡讀。從後面的 node 接續時，讀表的 node 會擋下並指示 `--from-node prepare_eval_data`（見 4.6 節）。
- **`--compare-only`**：欄模式下要先有一般的 `--post-training` 評估留下的 `item_categories.json`，沒有就在任何 node 之前擋下。手寫 mapping 不需要這份檔（#379 之前留下的評估目錄沒有它，照常能跑）。
- training 的 test mAP 不算大類（兩種寫法都一樣）：它只讀細 item 的指標。

### 3.4 Popularity baseline

```yaml
evaluation:
  baseline:
    lookback_months: 12
```

baseline 對每個評估日期 `S`，統計 `label_table` 在 `[S - lookback_months, S)` 期間各 item 的正例數，並以歷史正例數作為所有 entity 共用的 score。它不使用個人特徵，可用來判斷模型是否真正優於「所有人都推薦熱門 item」。

baseline 會在與模型相同的 evaluation rows 上重新排名，計算 overall 與 per-item 指標，再於報表呈現 Model、Baseline 與差異。

若指定回看期間完全沒有 label rows，會直接 raise（bug 1, ADR-0020）——不再退回完整 `label_table`。舊行為的退回會把評估日之後的資料算進 baseline，讓 popularity 用答案排名，報表卻照印「以過去 N 個月的歷史購買計數重排」；使用者裁定 baseline 是重要資訊，沒算出來要 raise，不做靜默 fallback。錯誤訊息含視窗範圍與 `label_table` 實際有的月份；解法是補齊 `label_table` 歷史、調整 `evaluation.baseline.lookback_months`，或把 `evaluation.report.sections.baseline` 設 `false` 直接不算這段。

視窗不是全空、只是沒涵蓋滿 `lookback_months` 時（例：設 12 個月，`label_table` 在視窗內只有 2 個月），不會 raise，但報表會揭露：baseline 段那句寫成「以過去 12 個月的歷史購買計數重排（label_table 在這個視窗內實際只涵蓋 2 個月）」，「平均每月」除以實際涵蓋的月數，不是除以 12。

評估多個日期時，每個日期各有一個視窗，排名組成的 count 是所有視窗的合計，所以「平均每月」除以**各視窗月數的加總**：視窗都滿時是「日期數 × `lookback_months`」，某個視窗沒涵蓋滿時那個視窗改算它實際涵蓋的月數（例：兩個日期、各 12 個月，除以 24）。段落那句改寫成「對 N 個評估日期各以該日期之前 12 個月的歷史購買計數重排」並寫出除數。視窗彼此重疊時，同一個月會被每個涵蓋它的視窗各算一次，月度趨勢表的逐月數字是重複計數後的合計，報表上也會寫這一句。

將 `report.sections.baseline` 設為 `false` 時，pipeline 會直接跳過第二次 baseline metric computation。

#### 正例率模式（`score: rate`，#397，只在 `--post-training`）

```yaml
evaluation:
  baseline:
    lookback_months: 12
    score: rate          # 沒寫＝count，就是上面那種
```

上面講的是預設的**正例數**模式。`score: rate` 改用**正例率**排名：

```
正例率 ＝ 回看期間的正例數 ÷ 回看期間當過候選的次數
```

**什麼時候要開。** 候選集合是被展示的子集時（例如廣告：每個 query group 只有當時真的展示的那幾個）。這時正例數會偏向被展示最多的 item：展示 1000 次、點 50 次的 A，會排在展示 100 次、點 20 次的 B 前面。但看到之後，B 被點的機會其實比較高。

每個 entity 的候選都是全部 item、回看期間的正例也都落在候選裡時（例如商銀示例），每個 item 當候選的次數都一樣。這時兩種模式排出的名次相同，不需要開。

**只在 `--post-training` 生效，監控模式一律用正例數。** 通用原則是：熱門度分數要描述**被評估的那些列**。

- `--post-training` 評的是 test 列，它們就是從 `sample_pool` 抽出來的。拿 `sample_pool` 算的正例率，描述的正是這些列。
- 監控模式評的是離線推論的全網格：每個 entity 配上全部 item，沒展示過的配對 label 是 0。在這些列上，一列會不會是正例，要看它被展示過幾次，也要看展示後多常被點——這正是正例數量的東西。這時改用正例率，基準線反而變弱，模型的領先會被灌大。

所以設定是同一份：監控模式遇到 `score: rate` 照舊用正例數、不讀 `sample_pool`，報表 baseline 段會寫出這句原因。將來離線推論若能讀「這期的候選清單」，這條再重新評估。

**分母從 `sample_pool` 數，不看 `label_table` 的列數。** `sample_pool` 的一列就是一次當候選。`label_table` 不行，原因有兩個：

- 它可能只放正例。這時列數＝正例數，每個有正例的 item 正例率都是 1。
- 它可能篩過。商銀示例只收「同群組至少一次正例」的 entity。

分子是這些候選列用 identity 接上 `label_table`（LEFT JOIN，缺的算 0）後的正例數，也就是 label 的加總（跟正例數模式一樣；label 是分級時，它是 label 加總，正例率是每次候選的平均 label）。宣告了 `event` 時，一列算一次候選。回看期間 `label_table` 有重複 identity 會 raise，道理跟 `prepare_eval_data` 那條一樣。

**沒當過候選的 item** 分數是 0，跟其他 0 分的 item 一起照 item 名排。分母很小的 item 照算，不平滑。報表在每個 item 旁印出分母，讓你自己判斷哪些比率不可靠。

**資料不齊。** 規則跟正例數模式相同，只是看的是 `sample_pool` 的候選列，不是 `label_table`：

- 回看期間 `sample_pool` 全空 → raise。就算 `label_table` 有資料也一樣。
- 沒涵蓋滿 → 不 raise。報表寫出「sample_pool 在這個視窗內實際只涵蓋 N 個月」。

**報表。** baseline 段的「popularity 排名組成」改照正例率排，欄位是正例率（4 位有效數字）、當候選次數、正例數。沒有「平均每月」：比率不能按月平均。月度趨勢表印的是同一批正例數的逐月拆分，合計對得上排名組成。`baseline_metrics.json` 多一個 `popularity_rate` 鍵，只在這個模式寫。`purchase_counts`／`monthly_counts` 照舊從 `label_table` 算。

評估多個日期時，每個日期各用自己視窗的正例率排名；排名組成表印的是**各視窗涵蓋的期合在一起、每期只算一次**的正例率。所以它不是任何一個日期實際用的那一個，但分母不會因為視窗重疊而變成好幾倍。

**每期彙總表 `popularity_period_counts`。** 回看 12 個月的 `sample_pool` 再接 `label_table`，在廣告規模可能是十幾億列。所以每個 time 值 × item 的兩個數（候選數、正例數）落地成 Hive 表，每期只算一次：

- **不按模型分。** 換 `model_version`、同一段日期重評，都不重算。分區鍵是 `popularity_source_version`：schema 加上 `sample_pool`／`label_table` 兩個 catalog 條目的雜湊（`core/versioning.py::compute_popularity_source_version`）。換了來源表或 schema，就落到新分區，全部重算。
- **要算哪些期。** 開跑前，CLI 列出 `sample_pool` 在各評估日期回看窗裡實際有的 time 值。這些值不一定是月底，廣告示例是每週一。扣掉已落地的，剩下的交給 `build_popularity_period_counts`。log 會印 `[months] popularity_period_counts to_count=… skipped=…`，`manifest.json` 的 `popularity_period_counts_month_plan` 也記同一件事（`to_count` 是計畫要算的期；切片把那個 node 切掉時，會另外印 `[rebuild] WARNING`）。
- **列 time 值的成本。** 這一步是一個 Spark job，連 `--dry-run`、`--list-nodes` 也會跑：`sample_pool` 按 time 分區時只讀回看窗那些分區的 time 欄；沒分區時要掃整張表的 time 欄。
- **只加總 `sample_pool` 現在還有的期。** 表裡留著算過的每一期；`sample_pool` 刪掉或改日期的期，不會再被加進基準線。
- **已落地的期預設沿用，不偵測來源有沒有變。** 兩種情況要帶 `--rebuild-dates <那幾期>` 重算：
  - `sample_pool` 或 `label_table` 某期補過資料。
  - 某期第一次被算進這張表時，它的 label 還沒成熟（例如搶在 label 觀察窗結束前先跑了一次）。之後 label 自然成熟，這張表不會自己更新。

  每個值都必須落在某個評估日期的回看窗裡，而且 `sample_pool` 在那一期有資料，不然會報錯。沒開 `score: rate`、不是 `--post-training`、或用了 `--compare-only` 時帶這個旗標，也會報錯。切片沒包含 `build_popularity_period_counts`（例如 `--from-node generate_report`）時，旗標不會生效，會印 `[rebuild] WARNING`，要重算請 `--from-node build_popularity_period_counts`。
- **不要同時跑兩個會寫同一期的評估。** 這張表不按模型分，兩個 `model_version` 同時評同一段日期，會同時覆寫同一個分區。寫進去的內容相同，但一邊寫、一邊讀的時候可能讀到換到一半的分區。
- **開關關著時什麼都不多。** 沒有這個 node、不建這張表，監控模式也不讀 `sample_pool`。

**前提。** 框架檢查不到，要由你確認：

- **每一期第一次被算進彙總表時，它的 label 已經成熟。** 彙總表只算回看窗 `[S - lookback_months, S)` 裡的期，都早於評估日期 S；S 的 label 已成熟是既有前提（2. 節）。各期的 label 觀察窗等長時，S 成熟就代表回看期都成熟了。但這張表會把第一次算的結果留下來，所以搶先跑過的話，之後要對那幾期帶 `--rebuild-dates`（見上）。
- **`sample_pool` 按 item 用不同比例抽負例，不影響這裡。** 分母描述的是被評估的列，而 `--post-training` 的 test 列就是從同一份抽過的 `sample_pool` 來的，抽過之後的比例正好是它們的比例。框架自己的 `dataset.sample_ratio_overrides` 只抽 train，也不影響。

### 3.5 報表內容

```yaml
evaluation:
  report:
    sections:
      dataset_overview: true
      primary_map: true
      diagnostics: true
      baseline: true
      diagnosis_links: true
      prediction_quality: false
    display:
      primary_map_k: [1, 3, 5, "all"]
      guardrail_recall_k: [1, 2, 3, 4, 5]
    diagnostics:
      include_distributions: true
```

標準報表固定包含 headline 與 glossary，其餘 sections 可個別開關：

| Section | 主要內容 |
|---|---|
| `dataset_overview` | rows、entities、items、正例數、正例率與各 item 概況 |
| `primary_map` | 衡量指標：overall、per-segment、大類 overall 的 mAP／precision／recall，以及 per-item、大類 per-item 的 recall 與 `map_attr` |
| `diagnostics` | score 分布、rank heatmap 與正例位置 |
| `baseline` | popularity 組成、Model／Baseline／Delta |
| `diagnosis_links` | 主報表指向診斷頁的入口，只放連結、不放數字；只有 `--post-training` 有診斷頁可指 |
| `prediction_quality` | 把每一列候選當二元預測：precision／recall／F1、`pr_auc`、`roc_auc`、分箱表（見 3.7 節）；框架預設 `false`，`false` 時不算、只寫 stub |

`sections` 的鍵必須剛好是上表這六個：多宣告一個沒有程式在讀的鍵，或少宣告一個程式在讀的鍵，evaluation 的 CLI 入口都會擋下，別的指令不受影響（A34；清單在 `core/consistency.py` 的 `EVALUATION_REPORT_SECTIONS`）。per-item、大類、per-segment 的表沒有自己的開關，跟著所在的段落開關。`guardrail_recall`、`per_item_attr`、`category`、`per_segment` 與 `display.recall_colorscale` 以前宣告過、但沒有程式讀，已經刪除（[ADR-0019](../adr/0019-evaluation-modules-split-by-role.md) 決定 6）。舊設定還寫著前四個的話，evaluation 的 CLI 入口會擋下，照錯誤訊息刪掉即可；`display.recall_colorscale` 不在檢查範圍內，留著不會報錯、也沒有作用，一併刪掉。

diagnostics 的 row-level aggregation 在 Spark 執行，只將 histogram、quartile 與 rank matrix 等小型結果交給報表層，不會將完整預測資料嵌入 HTML。

分數分箱（每格的平均分數 vs 實際正例率）不在 `diagnostics` 段，是 `prediction_quality` 段的分箱表（3.7 節）。#381 之前 `report.diagnostics` 另有 `include_calibration`、`n_calibration_bins` 兩個鍵，算一份在 `[0, 1]` 上等寬切、沒有任何讀者的 calibration bins；#381 把三份分箱表收成一份時一併移除。舊設定還寫著這兩個鍵的話，evaluation 的 CLI 入口會擋下（A43，不論值是什麼），照錯誤訊息刪掉即可；別的指令不受影響。

### 3.6 模型與外部結果比較

`compare_sources` 定義 CLI 可使用的比較來源。每個 key 都需要 `kind` 與報表顯示用的 `label`。

#### 同框架 model version

```yaml
evaluation:
  compare_sources:
    previous_model:
      kind: model_version
      label: "Previous production model"
      model_version: "abcdef12"
      source: enriched_eval_predictions
```

`source` 可選：

| Source | 前提 | 適用情境 |
|---|---|---|
| `enriched_eval_predictions` | Model B 已跑過標準 evaluation；預設值 | `--compare-only`，或希望雙方都使用已 enriched 的資料 |
| `ranked_predictions` | Model B 已完成 inference 發布 | 比較正式上線結果 |
| `training_eval_predictions` | Model B 已完成 training | 比較兩個候選模型的 test 預測 |

同框架來源假設 item 名稱已一致，不接受額外 `columns` 或 `prod_mapping`。

#### 外部 Hive 結果

```yaml
evaluation:
  compare_sources:
    external_project:
      kind: external_hive
      label: "External Project"
      table: other_project.predictions
      columns:
        cust_id: customer_id
        snap_date: as_of_date
        prod_name: item_code
        score: prediction_score
      prod_mapping:
        ext_fund_a: fund_stock
        ext_fund_b: fund_bond
      unmapped_policy: fail
```

`columns` 將外部欄位轉成框架使用的 entity、time、item 與 score 欄位。item 由多欄組成時（[`dataset.md` §3.9](dataset.md#39-item-由多欄組成選用)），有兩種寫法，看外部表長什麼樣：

- **外部表也把 item 分成那幾欄存**：寫各原欄的對應（例如 `campaign_id: ext_campaign`、`creative_format: ext_format`）。框架改名之後照同一條規則拼成 `item`，再套 `prod_mapping`，所以 `prod_mapping` 的鍵是拼好的外部值（例如 `ea-banner`）。
- **外部表只有它自己的一欄 item 編號**（例如另一套系統的廣告代碼）：直接寫 `item: <那一欄>`。框架不拼，`prod_mapping` 把那些編號翻成我們拼好的值（例如 `AD-00017: c01-banner`）。

兩種都寫會被 A11 擋下。`prod_mapping` 將外部 item 值映射至本框架 item；多個外部 items 可映射到同一個內部 item，此時以最大 score 合併。
`columns` 的 key 是**框架的 schema 角色欄名**，不是固定字面值：`schema.columns` 裡 `time`、`entity`（是 list，每一欄都要給）、`item`、`score` 解析出來的實際欄名。上例用的 `cust_id`／`snap_date`／`prod_name`／`score` 是預設 schema 的結果；若專案改過 `schema.columns`，這裡就要跟著改成新欄名，否則 A11 會在 CLI 進入點擋下。

外部資料出現 mapping 未涵蓋的 item 時：

| Policy | 行為 |
|---|---|
| `fail` | 立即中止並列出未映射 items，預設且較安全 |
| `drop` | 記錄 warning 後排除未映射 items |

比較前會取得雙方 query group（`time × entity`）集合與 item 集合的交集，限制到共同範圍後分別重新排名、重新計算指標，並在報表列出完整與共同 coverage、被排除的 items，以及 Model A、Model B 與 Delta。交集用的是 query group、不是 entity：評估多個日期時，B 只在 1 月評分過的 entity，A 在 2 月的列不會被留下來比。兩側的時間欄一律轉成文字再比對，所以外部表的時間欄是 DATE、A 側是 STRING 分區時照樣對得上（與載入 B 時篩日期的比法相同）。

比較報表沒有統計顯著性檢定；Delta 只表示共同範圍上的指標差值 `A - B`。

- **Δ 只在兩側都有值時才算**（bug 4，ADR-0020）。某側沒有這個值——例如某個 item 在 B 側沒有正例——那格留空，不當 0。某側 overall 整個是空的（全部 query 零正例）時 Δ 欄整欄空，表格標題寫明是哪一側。
- **coverage 的 common query group 數是裁切後兩側都還在的 group**（bug 14，ADR-0020）。兩側候選對稱時，它就是兩側指標用到的母體；某個 group 在一側只剩對方沒有的 item 時，它只進得了另一側的指標，不算 common。entity 欄為 NULL 的列在裁切時就被丟掉，不計入；舊版會把兩側的 NULL entity 當成同一個而算進去，所以有 NULL key 的資料上這個數字會變小。

### 3.7 預測品質指標家族

```yaml
evaluation:
  prediction_quality:
    n_bins: 1000          # 細箱數
    n_display_bins: 10    # 報表分箱表的格數，必須整除 n_bins
    top_n: 30             # per-item 預算
  report:
    sections:
      prediction_quality: false   # 開關：框架預設關，廣告示例（examples/ad）打開
```

**這一段回答什麼。** 排序指標（mAP、precision@K、recall@K）看的是同一個 query group 裡誰排前面。有些使用者不這樣用模型：他們把每一列候選當成一次「會不會是正例」的預測，照分數切一刀，想知道切在哪裡、precision 與 recall 各是多少、每一段分數實際有多少正例（[ADR-0024](../adr/0024-prediction-quality-metric-family.md)）。這一段給的就是這把尺：precision、recall、F1、`pr_auc`、`roc_auc` 與分箱表，整體＋per-item。

**怎麼算：一張分箱表，其他全部從它推出來。**

1. 取本次評估資料的最小與最大分數，中間切成 `n_bins` 個等寬的細箱。範圍不寫死 `[0, 1]`：正例率在 0.1%–1% 時分數擠在低端，`[0, 1]` 等寬會讓幾乎所有列落進前幾箱。
2. 一次 `groupBy(item, 細箱)`，每格加總列數、正例數、分數和（每一列先乘上權重；今天沒有權重欄，每列權重 1，見下方）。
3. 下面每個數都從這張表推出：
   - **門檻掃描**：門檻取每個細箱的下緣，分數 ≥ 門檻就預測為正，從最高的箱往下累加出 TP、FP，算 precision、recall、F1。
   - **F1 最佳門檻**：掃描裡 F1 最大的那個下緣（同分取較高的門檻）。
   - **`pr_auc`**：把同一箱的列視為同分之後的 average precision＝Σ（該箱正例 ÷ 全部正例）×（該箱下緣的 precision）。
   - **`roc_auc`**：同一箱視為同分、同分算一半的 ROC 面積。
   - **分箱表**：每 `n_bins ÷ n_display_bins` 個細箱併成一格，印出分數範圍、列數、正例數、平均分數（分數和 ÷ 列數）、實際正例率，以及「以這格下緣為門檻」的 precision、recall、F1。

箱是等寬的，不是分位數，為的是保住兩個性質：每個 item 的細箱加起來就是整體的細箱；細箱可以直接併成分箱表的格子。分位數箱會讓不同 item 的「第 3 箱」不是同一段分數，這兩個性質都會靜默失效；要換的話先另開一張 ADR。

**哪些是精確的、哪些是近似的。**

| 量 | 精確嗎 | 原因 |
|---|---|---|
| 細箱下緣的 TP／FP／FN／TN，以及該門檻的 precision／recall／F1 | 精確 | 都是計數 |
| 分箱表的平均分數與實際正例率 | 精確 | 表裡有分數和 |
| 落在箱內的門檻 | 算不出來 | 不內插 |
| F1 最佳門檻 | 只在細箱下緣上取 | 真正的最佳可能落在箱內，所以解析度＝細箱寬 |
| `pr_auc`、`roc_auc` | 「分箱後分數」的精確值 | 箱內的先後已經丟掉，不等於在原始分數上算的值 |

**讀這一段最容易讀錯的三件事**（報表也把它們印在數字旁邊）：

1. **門檻解析度＝細箱寬。** 報表印出細箱寬。F1 最佳門檻是在這份資料上挑的，換一份資料（下個月、線上）分數分布會變，不能直接搬去當線上的設定值。
2. **母體跟主指標段不同。** 本段不排除任何 query group；主指標段只算有正例的 query group（排除的數量在 `n_excluded_queries`）。兩段的 precision 不可互相比較。
   ⚠ `--post-training` 模式下，本段看到的是 dataset 階段過濾過的 test 表（`filter_test_model_input`）：有正例的 query group 全留，沒有正例的只留 `dataset.test_zero_positive_group_ratio`（r）那麼多，每列帶權重 `zero_positive_group_weight`（有正例的組 ＝ 1，留下來的無正例組 ＝ 1／r）。本段的列數、正例數與所有指標都用這個權重加權，代表還原到全部曝光的估計；1／r 是設計權重，不是無偏估計，留下的組少時不穩——報表在母體說明與〈基本統計 — 資料集〉印出 r 與留下來的組數。r 為 0（預設）時 test 表已經沒有無正例的組，二元指標會系統性偏高，所以開了本段又跑 `--post-training` 而 r 為 0 時，evaluation 在 CLI 入口就停下（不變量 A46）。反過來，設定的 r > 0 但被評估的模型是在 r ＝ 0 下建的（預測表沒有權重欄、或權重是 NULL），本段會在計算前停下並說明兩者對不上。監控模式不受影響。設定方式見 [dataset.md §3.7](dataset.md#37-沒有正例的-query-group-留多少)。
3. **`pr_auc` 不是外部工具算的 average precision。** 它是分箱後的值，不能拿來跟 sklearn 在原始分數上算的 average precision 直接比較。另外有兩個精確算法的指標 `pooled_average_precision`、`macro_per_item_average_precision`（HPO 在 val 上用它們挑參數，training 在 test 上也算，見 [training.md §3.7](training.md#37-test-上算哪些指標test_metrics)），定義與本段不同，也不可互相比較。名字刻意不叫 average precision：在這個 repo 裡，AP 是排序指標 `map@K` 的說法。

其他要知道的：

- **per-item 的 precision 不是 `precision@K`。** per-item 的 precision 分母是該 item 在門檻以上的列數；`precision@K` 的分母是 K（每個 query group 的前 K 名），是 per-query 的概念。
- **per-item 的 `roc_auc` 不是「item 能力」診斷的 AUC。** 後者的母體是只含有正例的 query 的診斷抽樣，兩者不可並排比較。
- **分箱表不是校準。** 框架不做校準（#411），分數不保證是機率；平均分數與實際正例率只是並列。本段每個指標都只看分數的大小順序。
- **一個門檻橫跨所有 query group。** 這要求不同 query group 的分數彼此可比。模型用排序類目標（`lambdarank`、`rank_xendcg`）訓練時，分數只為同一個 query group 內的先後而學，跨 group 用同一個門檻切是模型沒有優化過的用法；`binary` 目標沒有這個問題。報表也印這一句。
- **極端值會把細箱撐寬。** 範圍取最小～最大，少數特別高的分數會讓多數列擠進少數幾個細箱，門檻解析度跟著變差。分箱表每一格都印列數，看得出來是不是這樣；要改用分位數箱得先另開 ADR（見上）。
- **per-item 預算。** 只有列數最多的前 `top_n` 個 item 有自己的分箱與指標（item × 細箱要收回 driver）；其餘 item 仍算進整體，報表會寫出一共幾個 item。`top_n: 0` 只看整體。
- **權重欄。** 分箱時每一列先乘上權重。今天預測表沒有權重欄，每列權重 1；#429 會讓 test 表保留一部分沒有正例的 query group、帶一欄 1／r 的權重，那時在 `compute_prediction_quality` 接上。
- **成本。** 打開之後多一次取最小、最大分數的聚合，和一次 `groupBy(item, 細箱)` 的全表 shuffle。收回 driver 的量是「細箱數 ×（1 ＋ `top_n`）＋ item 數」列，與資料列數無關。
- **產物。** `prediction_quality.json`（catalog 名 `prediction_quality_metrics`）：整體與前 `top_n` 個 item 的細箱表、每張表的關鍵數、分數範圍與細箱寬、欄名；帶設定指紋。關掉時是 `{"enabled": false}` stub。
- **合法值**（A42，evaluation 指令入口檢查）：`n_bins`、`n_display_bins` 是 ≥ 1 的整數，而且後者整除前者；`top_n` 是 ≥ 0 的整數；這個區塊不能有別的鍵。開關在 `report.sections.prediction_quality`，在這裡寫 `enabled: true` 不會打開任何東西，所以會被擋下。

## 4. 使用方式

### 4.1 CLI 選項

| 選項 | 預設 | 說明 |
|---|---|---|
| `--env`, `-e` | `local` | 選擇設定環境 |
| `--model-version <id>` | `best` | 指定要評估的模型版本 |
| `--post-training` | 關閉 | 改讀 `training_eval_predictions`；預設讀 `ranked_predictions` |
| `--compare <key>` | 無 | 執行標準評估並額外產生比較報表 |
| `--compare-only <key>` | 無 | 讀取既有 enriched data，只產生比較報表 |
| `--rebuild-dates <dates>` | 無 | 逗號分隔的 time 值：`popularity_period_counts` 已落地仍要重算的期（補過 `sample_pool`／`label_table`，或第一次算時 label 還沒成熟）。只在 `evaluation.baseline.score: rate` 加 `--post-training` 可用，見 3.4 節 |
| `--from-node <name>` | 無 | 從指定 node 的拓撲位置開始，並執行其後 nodes |
| `--only-node <name>` | 無 | 只執行指定 node，以及缺少輸入時必要的上游 nodes |
| `--dry-run` | 關閉 | 顯示切片執行計畫後離開 |
| `--list-nodes` | 關閉 | 列出 node 名稱與接續成本 |

`--compare` 與 `--compare-only` 互斥，傳入的 key 必須存在於 `evaluation.compare_sources`。`--from-node` 與 `--only-node` 也互斥；`--list-nodes` 不可與兩者併用。

`--dry-run` 與 `--list-nodes` 不會執行 nodes、寫入 Hive 或報表，但 CLI 仍會載入設定、初始化 Spark、解析 `model_version`、讀取 model manifest 並查詢 catalog 產物是否存在。

### 4.2 Training 後評估

候選模型尚未 promotion 時，應明確指定版本：

```bash
python -m recsys_tfb evaluation \
  --env production \
  --post-training \
  --model-version <candidate_model_version>
```

此模式讀取 training 的 test predictions，並沿用其中保存的 label。它適合模型上線前審核，但評估母體是 dataset pipeline 建立的 test set，不等同正式 inference 的完整上線母體。

省略 `--model-version` 不會自動選擇最新 training 產物，而是使用 `best`；這通常只適合重新檢查目前正式模型的 training test 結果。

### 4.3 上線後監控

```bash
python -m recsys_tfb evaluation \
  --env production \
  --model-version <production_model_version>
```

若要監控目前 `best`，可省略版本：

```bash
python -m recsys_tfb evaluation --env production
```

此模式讀取 inference 正式發布的 `ranked_predictions`，再依目前 `label_table` 補入 ground truth。應在該 `snap_date` 的 label 觀察窗成熟後執行。

此模式（含加上 `--compare`）**不含 registry 診斷**：不組 `diagnose_*` 各項與 `render_diagnosis_pages`，報表也沒有診斷入口。要看診斷請用 `--post-training`。理由見 [ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 5。指標信賴區間（`compute_metric_ci`）與報表診斷區（`compute_report_aggregates`）兩種模式都有。

### 4.4 標準評估加模型比較

```bash
python -m recsys_tfb evaluation \
  --post-training \
  --model-version <model_a> \
  --compare previous_model
```

這會完整產生：

- Model A 的標準 `report.html`
- Model A 的 `enriched_eval_predictions`
- Model A 與設定來源的 `report_comparison.html`

Model A 的來源由 `--post-training` 決定；Model B 的來源由 `compare_sources.<key>.source` 決定。
比較兩個 training test 結果時，Model B 通常也應設為 `training_eval_predictions`；比較兩個正式推論結果時則使用 `ranked_predictions` 或已完成 evaluation 的 enriched data。

`--compare` 不改變有沒有 registry 診斷：帶 `--post-training` 才有；監控模式加 `--compare` 一樣沒有（見 4.3 節）。

### 4.5 只產生比較報表

```bash
python -m recsys_tfb evaluation \
  --model-version <model_a> \
  --compare-only previous_model
```

適合以下情況：

- Model A 已完成標準 evaluation，只新增或修改 Model B compare source。
- 只想更換比較對象或顯示名稱，不需重做 Model A baseline 與 diagnostics。
- 想使用相同 enriched Model A rows 重新計算共同母體上的 A/B 指標。
- Model B 的預測剛準備完成，但 Model A 的標準報表不需重建。

必要前提：

1. `enriched_eval_predictions` 已存在目前 `model_version` 在每一個評估日期的 partition。
2. 該 partition 的來源情境、label、segment 與資料內容仍符合本次比較需求。
3. compare source 在同一日期有資料。

`--compare-only` 不讀 `training_eval_predictions` 或 `ranked_predictions` 來重建 Model A，因此 `--post-training` 在此模式不會改變 Model A 資料來源，建議不要混用。
若要切換 Model A 的 training／monitoring 情境，必須先用標準模式或 `--compare` 重新建立 enriched partition。

### 4.6 查看 nodes 與部分重跑

```bash
python -m recsys_tfb evaluation --list-nodes

python -m recsys_tfb evaluation \
  --from-node generate_report \
  --dry-run
```

`generate_report` 是純函式，7 個參數全部是別的 node 算好的產物（見 5.1 節的表）。其中 `evaluation_metrics`（`metrics.json`）、`baseline_metrics`（`baseline_metrics.json`）、`evaluation_metric_ci`、`evaluation_report_aggregates`、`prediction_quality_metrics`（`prediction_quality.json`）已落地成 `JSONDataset`（前兩者見 [ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 2），每項 registry 診斷各自的 JSON（如 `evaluation_config_shift`）也是；只有 `evaluation_diagnosis_pages` 是記憶體中間結果。切片的自動擴張只看「輸出是否已落地」（見 [`pipeline-slicing.md`](../operations/user-guides/pipeline-slicing.md) 的「自動擴張補跑」），所以從 `generate_report` 接續，前次完整 run 成功時只會自動補跑產出 `evaluation_diagnosis_pages` 的那個 node，兩種都不跑 Spark：

- `--post-training`：`render_diagnosis_pages`（它的輸出是「這次執行寫出的頁面路徑清單」，語意上不該落地重用；它畫的診斷結果從已落地的 JSON 讀回，不會連 `diagnose_config_shift` 一起被拉回來重跑）
- 監控模式：`no_diagnosis_pages`（不讀任何東西、回空清單，重跑零成本）

兩種模式各一組清單，釘在 `tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS[("evaluation", ())]`（監控）與 `RESUME_CONTRACTS[("evaluation", (("post_training", True),))]` 的 `"generate_report"`，之後改動這幾個 node 的形狀，該測試會紅燈提醒同步文件。

接續時直接讀回的落地 JSON（`evaluation_metrics`、`baseline_metrics`、`evaluation_metric_ci`、`evaluation_report_aggregates`、`prediction_quality_metrics`、各項診斷 JSON）若是在不同的「算的」設定下算出來的，`render_diagnosis_pages` 與 `generate_report` 會在畫之前 raise，訊息列出哪份產物、哪個鍵從什麼值變成什麼值、該 `--from-node` 哪個 node。只改了「畫的」設定則照常重繪。兩類設定的分法見 7.2 節。

因此只改了「畫的」設定時，`--only-node generate_report` 就是便宜的重繪接續點：不重算任何指標（CLI 仍會照 4.1 節所說初始化 Spark）。需要只重做模型比較時，應使用持久化 `enriched_eval_predictions` 的 `--compare-only`。

從指標接續（`--from-node compute_metrics`）也不重做 join：`prepare_eval_data` 把連好 label、rank 與 segment 的列直接寫進 `enriched_eval_predictions`，之後的 node 讀表（[ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 1）。前次完整 run 成功時，只補跑兩個不做 join 的 node：`draw_diagnosis_sample_node`（抽樣只在記憶體）與監控模式的 `no_diagnosis_pages`；清單同樣釘在 `RESUME_CONTRACTS` 的 `"compute_metrics"`。兩個情況會把 `prepare_eval_data` 拉回來或擋下：

- **評估日期的 partition 不在**：這張表從第一次評估之後就一直存在，所以 CLI 不問「表在不在」，問「`evaluation.snap_date` 的每一個日期的 partition 在不在」。任一個不在就把 `prepare_eval_data` 拉回切片，全都在就直接讀。
- **partition 在，但寫的時候設定不同**：兩道比對，擋的是兩件事。
  - `prepare_eval_data` 跟 partition 一起寫的 `segment_columns.json` 帶設定指紋。分群的讀表 node（`compute_metrics`、`compute_baseline_metrics`、`draw_diagnosis_sample_node`）先比對其中五個決定 `prepare_eval_data` 產出的鍵（`post_training`、`evaluation.snap_date`、`evaluation.segment_columns`、`evaluation.segment_sources`、`evaluation.item_categories.column`），不合就 raise，指示 `--from-node prepare_eval_data`。只比這五個鍵的理由：改 `k_values` 這類只影響後段計算的設定，照訊息從 `compute_metrics` 重跑不會重寫 partition，全部都比的話會一直被擋。`compute_metrics`、`compute_baseline_metrics` 讀到的 `item_categories.json`（3.3 節）也照同一組鍵比；欄模式下這份檔不在、或是從別的欄讀的，同樣擋下並指示 `--from-node prepare_eval_data`。
  - 每個 partition 的每一列另外帶 `eval_partition_fingerprint`（#374）：寫它的那次執行的 `post_training`、`segment_columns`、`segment_sources`（不含 `snap_date`），加上那次**實際 join 進去的分群欄**（`segment_columns.json` 的 `joined`）。**每個**讀表 node 讀之前逐日期比對「今天的設定＋這個目錄 JSON 的 `joined`」，不合或是空值就 raise，列出所有有問題的日期，指示 `--from-node prepare_eval_data`。這道擋的是「這個日期後來被另一次執行蓋掉」：同一個日期會被目錄不同的執行寫到（單跑 3 月寫 `20260331/`、1–3 月一起跑寫 `20260131-20260331/`），目錄裡那份 JSON 只代表自己那次寫的東西，看不出分區後來被誰蓋過。詳細的反例與理由見 7.1 節。

`--from-node` 使用拓撲順序語意，會執行指定 node 與拓撲序中其後的 nodes；`--only-node` 則不執行下游 consumers。只要 pipeline 實際執行，CLI 仍會寫 evaluation manifest 並更新 `data/evaluation/latest`，所以單 node 模式應視為進階除錯工具。

## 5. 執行流程

### 5.1 標準模式

Plan 1.5（2026-07-20）把原本擠在 `generate_report` 裡的 Spark 聚合與診斷渲染拆成獨立 node。兩種模式的 node 不同：registry 診斷與 `render_diagnosis_pages` 只在 `--post-training` 組出來，監控模式改組 `no_diagnosis_pages`（[ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 5）。下表是兩者的聯集，只屬於一種模式的列有標明；實際順序以帶同樣旗標的 `--list-nodes` 印出的拓撲序為準：

| 階段 | node | 輸入 | 處理內容 | 主要輸出 |
|---|---|---|---|---|
| 整理資料 | `prepare_eval_data` | 預測、`label_table`、該模式的母體表（`sample_pool`／`inference_population`）、parameters | 篩選模型與日期、檢查 `label_table` 在 identity 上沒有重複 key、補 label、必要時重算 rank、從母體表或覆寫表連接 segments（見 3.2 節）；欄模式下從 `sample_pool` 讀大類對照表（見 3.3 節）。join 只在這裡算一次，結果寫進這個月的 Hive partition | `enriched_eval_predictions`、`evaluation_segment_columns`、`evaluation_item_categories` |
| 抽取診斷樣本 | `draw_diagnosis_sample_node` | `enriched_eval_predictions`、`evaluation_segment_columns`、parameters | 只抽一次、後續診斷 node 共用同一份樣本（見 `evaluation.diagnosis.sample`）。監控模式裡只有 `compute_metric_ci` 用它，所以關掉 `diagnosis.ci` 就不抽 | `diagnosis_sample` |
| 模型指標 | `compute_metrics` | `enriched_eval_predictions`、`evaluation_segment_columns`、`evaluation_item_categories` | 計算 overall、per-item、per-segment、macro、overview 與可選 category metrics；照 `joined` 分群，並把 `joined`／`sources`／`missing` 帶給報表；結果帶設定指紋。後置條件：篩完之後的月份數不等於設定的日期數（某個日期的 partition 空的或沒寫過）就 raise，訊息寫出預期數與實際數 | `evaluation_metrics`（落地 `metrics.json`） |
| Baseline 每期彙總（僅 `baseline.score: rate` 且 `--post-training`） | `build_popularity_period_counts` | `sample_pool`、`label_table`、`popularity_period_counts_month_plan`（CLI 算的期計畫）、parameters | 只算計畫裡還沒落地的期（加上 `--rebuild-dates` 點名的）：每個 time 值 × item 的候選數與正例數。與模型無關（見 3.4 節） | `popularity_period_counts`（Hive） |
| Baseline | `compute_baseline_metrics` | `enriched_eval_predictions`、歷史 labels、`evaluation_segment_columns`、`evaluation_item_categories`；正例率模式再加 `popularity_period_counts`（讀回整張表）與期計畫（只加總計畫裡的期） | 建立 popularity scores（正例數，或正例率模式的正例率）並計算對照指標；`report.sections.baseline: false` 時不算，只回 `{"enabled": false}` stub（帶設定指紋） | `baseline_metrics`（落地 `baseline_metrics.json`） |
| 預測品質 | `compute_prediction_quality` | `enriched_eval_predictions`、`evaluation_segment_columns`（只取 `joined` 確認分區指紋，不分群）、parameters | 把每一列候選當二元預測：一次 `groupBy(item, 細箱)` 的分箱表，加上從它推出的 `pr_auc`、`roc_auc`、F1 最佳門檻（見 3.7 節）；不排除沒有正例的 query group。`report.sections.prediction_quality: false`（框架預設）時不算，只回 `{"enabled": false}` stub（帶設定指紋） | `prediction_quality_metrics`（落地 `prediction_quality.json`） |
| 報表區 Spark 聚合 | `compute_report_aggregates` | `enriched_eval_predictions`、`evaluation_segment_columns`（只取 `joined` 確認分區指紋，不分群）、parameters | 標準報表診斷區要用的 Spark 端聚合（bin 計數／quartile／rank 矩陣），落地後 `generate_report` 才能是純函式 | `evaluation_report_aggregates` |
| 指標信賴區間 | `compute_metric_ci` | `diagnosis_sample`、parameters | per-item AP 與 macro 的 cluster bootstrap CI（cluster＝`cust_id`） | `evaluation_metric_ci` |
| 診斷（registry，現行 4 項；僅 `--post-training`） | `diagnose_config_shift`（其餘 3 項同形狀，由 `make_diagnosis_node` 產生） | `diagnosis_sample`、parameters | 讀 `evaluation.diagnosis.<name>.enabled`（使用者唯一的開關，見該鍵旁的註解與 `diagnosis.metric.contract` docstring）；停用時寫 `{"enabled": false}` stub。輸出（含 stub）另帶 `"diagnosis": <name>` 與設定指紋 `config_fingerprint` | `evaluation_<name>` |
| 診斷頁面組裝（僅 `--post-training`） | `render_diagnosis_pages` | parameters、各項 `evaluation_<name>`（依 `DIAGNOSES` 順序） | 把這次的診斷結果組成獨立分頁 HTML；哪項停用就少哪一頁。畫之前檢查第 i 個結果帶著第 i 項診斷的名字、指紋與目前的「算的」設定一致，不合就 raise（見 7.2 節） | `evaluation_diagnosis_pages` |
| 空的診斷頁清單（僅監控模式） | `no_diagnosis_pages` | parameters（值不讀） | 回空清單、不讀磁碟。`generate_report` 是位置綁定，診斷頁這個輸入必須有人產出；不沿用 `render_diagnosis_pages`，因為它要求每項 registry 診斷各一份帶名字的結果，而監控模式一份都沒算 | `evaluation_diagnosis_pages` |
| 標準報表 | `generate_report` | `evaluation_metrics`、parameters、`baseline_metrics`、`evaluation_metric_ci`、`evaluation_report_aggregates`、`evaluation_diagnosis_pages`、`prediction_quality_metrics`（7 個必填參數，皆無預設值） | 產生互動式 HTML；純函式，不含任何 Spark 物件或 action。畫之前比對 `evaluation_metrics`、`baseline_metrics`、`evaluation_metric_ci`、`evaluation_report_aggregates`、`prediction_quality_metrics` 的設定指紋，不合就 raise | `evaluation_report` |

讀 `enriched_eval_predictions` 的 node，第一步都先篩到 `evaluation.snap_date` 的日期（一個或多個）：表裡是這個 model_version 評估過的**所有月份**，catalog 只替你篩掉別的 model_version。篩的同時逐日期確認分區指紋（見 4.6 節），再把指紋欄丟掉往下用。忘了篩不會報錯，只會把所有月份一起算進去，所以 `tests/test_pipelines/test_evaluation/test_pipeline.py` 有一條 AST 測試：接上這張表、函式裡卻沒呼叫 `restrict_to_current_eval_partitions` 的 node 會讓測試失敗（只呼叫只篩不驗的 `restrict_to_eval_snap_dates` 也算沒做）。讀表的 node 之中，分群的那三個另外先比對 `segment_columns.json` 的指紋（見 4.6 節）。

`diagnose_*` 這幾列由 registry（`diagnosis.metric.contract.DIAGNOSES`）導出：現行 4 項（`config_shift`／`item_ability`／`model_capacity`／`suppression`）各是一個同形狀的 `diagnose_<name>` node（由 `make_diagnosis_node` 產生）；新增或移除診斷時 registry 與此表一起變。

`training_eval_predictions` 不保存 rank，因此 post-training 模式會依 score 在每個 query group 內重算。監控模式的 `ranked_predictions` 已有 rank，`prepare_eval_data` 會保留發布結果中的 rank；metric computation 本身仍會依 score 重新建立內部 position。

重算與 inference 用同一條規則：score 降冪，同分按 item 升冪（`utils/ranking.py`），所以同一批列在兩邊拿到相同名次。補出來的 rank 轉成與 `ranked_predictions` 相同的 BIGINT，`label` 一律轉成與 `training_eval_predictions` 相同的 INT：兩種模式寫同一張 `enriched_eval_predictions`，而這張表的 schema 由第一次寫入決定、之後不轉型，型別不同的那次寫入會直接失敗。

### 5.2 `--compare` 模式

標準流程後追加：

| 階段 | node | 處理內容 | 主要輸出 |
|---|---|---|---|
| 載入 Model B | `load_compare_predictions` | 依 compare source 載入、篩日期（每個設定的日期都要有 rows，缺的日期會列出來並 raise）、轉欄位與 item mapping | `compare_predictions_raw` |
| 對齊母體 | `restrict_to_common` | Model A 先篩到評估日期並確認分區指紋（比今天的設定＋`segment_columns.json` 的 `joined`；為什麼不比那份 JSON 記錄的設定，見 5.3 節），再取共同 query groups 與 items、雙方重新排名、Model B 一律沿用 Model A 的 label | `eval_predictions_common`、`compare_predictions_common`、coverage |
| 比較報表 | `generate_comparison_report` | 兩側重新計算 metrics 並產生 A/B/Delta；只有 Model A 照 `evaluation_segment_columns` 分群，Model B 是另一張預測表、沒有分群欄；兩側的大類都照 `evaluation_item_categories`（欄模式）或手寫 mapping 排 | `evaluation_comparison_report` |

### 5.3 `--compare-only` 模式

| 階段 | node | 處理內容 |
|---|---|---|
| 驗證 Model A | `validate_enriched_eval_predictions_present` | 確認這個 model_version 在 `evaluation.snap_date` 的每一個日期都有 rows、而且分區指紋跟 `segment_columns.json` 記錄的設定與 `joined` 一致；沒有列、指紋不合、指紋是空值三類一次列完再 raise。沒有 output，不傳東西給下一步 |
| 載入 Model B | `load_compare_predictions` | 載入設定的 comparison source；B 是 enriched 表時，丟掉它的分區指紋欄、不驗 |
| 對齊母體 | `restrict_to_common` | 讀 `enriched_eval_predictions`、先篩到評估日期並確認分區指紋（同上，比 `segment_columns.json` 記錄的設定與 `joined`），再取共同範圍並重新排名 |
| 比較報表 | `generate_comparison_report` | 產生 `report_comparison.html` |

這條路沒有 `prepare_eval_data`，它讀的東西都是寫 enriched partition 的那次標準 run 一起寫的：評估日期的 partition、`evaluation_segment_columns`（`segment_columns.json`，`generate_comparison_report` 用它決定分群），以及欄模式下的 `evaluation_item_categories`（`item_categories.json`，大類對照表，見 3.3 節）。兩者會被分開刪掉（清掉 `data/`、DROP 表），缺哪個都會被擋下、說出缺的是哪個：

- **partition 不在、或 `segment_columns.json` 不在**（欄模式下再加上 `item_categories.json`，見 3.3 節）：CLI 在任何 node 執行前就停下，每缺一樣列一行（表名與缺 partition 的所有日期、檔案路徑），最後寫出該先跑的 `python -m recsys_tfb evaluation --model-version …`。這一步只看 partition 清單與檔案在不在，不讀資料。放在 CLI、不只靠下面的閘門，是因為切片會跳過沒有輸出的 node：`--compare-only` 加上 `--from-node` 時閘門不會跑。
- **partition 列得出來但某個日期沒有列**：`validate_enriched_eval_predictions_present` 擋下，訊息寫出表名、沒有列的日期與 model_version。
- **某個日期的 partition 後來被另一次執行蓋掉**（#374）：閘門與 `restrict_to_common` 都擋下，訊息列出日期。**只有 `--compare-only`** 比的是「這個目錄的 `segment_columns.json` 記錄的設定」，不是今天的設定：`--post-training` 在這條路上是啞的，拿今天的值比，照常不帶旗標讀 post-training 寫的分區就會被誤擋。一般 `--compare` 模式的 `restrict_to_common` 跟其他讀表 node 一樣比今天的設定，因為 `--compare X --only-node generate_comparison_report` 這種切片讀的是先前執行寫的分區，那個目錄的 JSON 跟分區一樣記著舊設定，比 JSON 會放行。

三種都是先重跑一次同 model_version、同月份的標準 evaluation。

比較時，Model B 一律沿用 Model A 的 label（依 `time + entity + item` 對上）（bug 7，ADR-0020）。

- **B 自帶的 label 先丟掉。** `enriched_eval_predictions` 與 `training_eval_predictions` 都帶 label，那是 B 落地當時的答案；沿用的話，之後的回補會混進每一個 Δ，看起來像模型差異。
- **也不另外讀 `label_table`。** A 的答案不一定是現在的 `label_table`：`--post-training` 刻意沿用訓練時存下的 label，`--compare-only` 沿用寫 enriched partition 那次存的。抄 A 的，三種模式下兩側都是同一份答案，比較報表也跟同一次 run 的主報表對得上。
- **代價**：B 有、A 沒評過的 `(entity, item)` 沒有答案可抄，一律算 0。只有兩側候選不對稱時才會發生。

## 6. 產物與驗收

### 6.1 主要產物

| 產物 | 位置或儲存方式 | 產生模式 |
|---|---|---|
| `report.html` | `data/evaluation/<model_version>/<YYYYMMDD>/report.html` | 標準、`--compare` |
| `report_comparison.html` | `data/evaluation/<model_version>/<YYYYMMDD>/report_comparison.html` | `--compare`、`--compare-only` |
| `manifest.json` | `data/evaluation/<model_version>/<YYYYMMDD>/manifest.json` | 所有實際執行模式 |
| `metrics.json` | `data/evaluation/<model_version>/<YYYYMMDD>/metrics.json`（`evaluation_metrics`：overall、per-item、per-segment、macro、overview 與 category 指標，帶 `config_fingerprint`） | 標準、`--compare` |
| `baseline_metrics.json` | `data/evaluation/<model_version>/<YYYYMMDD>/baseline_metrics.json`（`baseline_metrics`；`report.sections.baseline: false` 時是 `{"enabled": false}` stub，一樣帶指紋） | 標準、`--compare` |
| `prediction_quality.json` | `data/evaluation/<model_version>/<YYYYMMDD>/prediction_quality.json`（`prediction_quality_metrics`，見 3.7 節；`report.sections.prediction_quality: false` 時是 `{"enabled": false}` stub，一樣帶指紋） | 標準、`--compare` |
| `segment_columns.json` | `data/evaluation/<model_version>/<YYYYMMDD>/segment_columns.json`（`evaluation_segment_columns`，見 3.2 節） | 標準、`--compare` 寫；`--compare-only` 讀 |
| `item_categories.json` | `data/evaluation/<model_version>/<YYYYMMDD>/item_categories.json`（`evaluation_item_categories`，見 3.3 節；沒設 `column` 時 `mapping` 是空的） | 標準、`--compare` 寫；`--compare-only` 讀（缺檔只在欄模式擋） |
| `enriched_eval_predictions` | Hive，以 `model_version` 與 `snap_date` partition；每列帶 `eval_partition_fingerprint`（寫這格的執行的分區內容設定與實際 join 的分群欄，見 7.1 節） | 標準、`--compare` |
| `popularity_period_counts` | Hive，以 `popularity_source_version` 與 `snap_date`（time 值）partition；不含 `model_version`（見 3.4 節） | 只在 `evaluation.baseline.score: rate` 且 `--post-training` |
| `latest` alias | `data/evaluation/latest` | 指向最近完成的 evaluation 目錄 |

上表路徑裡的 `<YYYYMMDD>` 是評估日期去掉 `-`。評估多個日期時，這一段是 `<最早>-<最晚>`，例如 1 到 3 月月底寫入 `data/evaluation/<model_version>/20260131-20260331/`；`enriched_eval_predictions` 仍是每個日期各一個 partition。目錄名只看起迄兩端，後果見 7.1 節。

需要機器可讀的指標時讀 `metrics.json`，不要從 report HTML 反向解析（[ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 2）。它跟其他產物一樣以 `(model_version, snap_date)` 為路徑：同一組重跑會覆寫，兩種模式跑同一組也寫同一個檔、後跑的蓋掉先跑的（與 7.3 節的 enriched partition 同一個限制；讀回時指紋裡的 `post_training` 會擋下兩種模式混用）。

`enriched_eval_predictions` 保存 Model A 的 identity、score、rank、label 與 segment enrichment，供後續比較重用。catalog 使用 `columns: "auto"`；新增 segment 欄位時可做 append-only schema evolution，舊 partitions 缺少的新欄位會是 NULL。

### 6.2 驗收重點

標準評估完成後至少確認：

1. 報表 metadata 的 model version 與 snap date 正確（多個日期時顯示「最早 ~ 最晚（N 個日期）」，N 應等於設定的日期數）。
2. `n_queries` 大於零，`n_excluded_queries` 比例合理。
3. dataset overview 的 entities、items、rows 與 positives 符合該批次預期。
4. 主要 `map@K` 與 `recall@K` 使用的 K 符合實際展示空間。
5. per-item 與 per-segment 沒有被整體平均掩蓋的明顯退化。
6. popularity baseline 段有出現（lookback 視窗查無歷史時整條會 raise，不會帶著用答案排名的 baseline 跑完），段落說明寫的 lookback 月數與 `evaluation.baseline.lookback_months` 一致。
7. diagnostics 的 score/rank 分布沒有異常集中或缺產品。
8. `enriched_eval_predictions` 的本次 model/date partition 有資料且 key 沒有非預期重複。
9. 打開 `prediction_quality` 時：整體分箱表的列數加總等於該段寫的母體列數；`--post-training` 模式下，母體說明有「test 表在 dataset 階段已經丟掉沒有正例的 query group」那一句。

比較報表另需確認：

1. Model A、Model B source 與 label 顯示正確。
2. common query group／item coverage 足夠，沒有大量意外被排除的 items。
3. 雙方在共同範圍的 row coverage 與候選集合語意一致。
4. Delta 的方向為 `Model A - Model B`。
5. 外部 mapping 沒有非預期 drop 或多對一合併。

範例查詢：

```sql
SELECT snap_date, COUNT(*) AS rows,
       COUNT(DISTINCT cust_id) AS entities,
       COUNT(DISTINCT prod_name) AS items
FROM ml_recsys.enriched_eval_predictions
WHERE model_version = '<model_version>'
  AND snap_date = '<snap_date>'
GROUP BY snap_date;
```

實際 database、欄位名稱與 partition 設定以 `conf/base/catalog.yaml` 與 schema 設定為準。

## 7. 版本、重跑與恢復

### 7.1 Evaluation 沒有獨立版本 ID

evaluation 沒有 `evaluation_version`，產物的路徑只由以下兩項決定：

```text
model_version
+ evaluation.snap_date
```

report path 會將 ISO 日期移除 `-`，例如 `2026-01-31` 寫入 `data/evaluation/<model_version>/20260131/`；Hive partition 仍保留實際 schema 中的日期值。

評估多個日期時，目錄名是 `<最早>-<最晚>`，例如 `2026-01-31` 到 `2026-03-31` 寫入 `data/evaluation/<model_version>/20260131-20260331/`。**目錄名只看起迄兩端**：起迄相同、中間日期不同的兩組設定（例如 1、2、3 月與只有 1、3 月）寫到同一個目錄，後跑的覆寫先跑的，跟改了任何其他設定再重跑一樣。落地 JSON 的設定指紋含 `evaluation.snap_date` 的完整清單，所以換了日期組合之後只做部分重跑，讀到舊組合算的 JSON 會被擋下，不會把兩組日期的結果混進同一份報表。

**同一個日期的 partition 會被不同目錄的執行寫到，所以 partition 自帶設定指紋。** `enriched_eval_predictions` 一個日期一個 partition，但寫它的執行可能是單跑那個月、也可能是一段區間；兩者的目錄不同，各自的 `segment_columns.json` 只代表自己那次寫了什麼。反例：

```text
1. 1–3 月（設定 A）              → 1、2、3 月 partition＝A；20260131-20260331/ 的 JSON＝A
2. 單跑 3 月（設定 B）           → 3 月 partition＝B；      20260331/ 的 JSON＝B
3. 接續 1–3 月 --from-node compute_metrics（設定 A）
   → 只看 20260131-20260331/ 的 JSON 會放行，實際讀到 B 的 3 月
```

所以每一列另外帶 `eval_partition_fingerprint`：寫它的那次執行的 `post_training`、`evaluation.segment_columns`、`evaluation.segment_sources`，加上那次**實際 join 進去的分群欄**（`joined`），算出的 sha256（`steps/config_fingerprint.py::partition_fingerprint`）。

- **不含 `evaluation.snap_date`**：一個日期 partition 的內容只取決於模式與分群設定，跟同一次還評估了哪些別的日期無關；含進去的話，設定相同的單月執行與區間執行會互相擋。
- **不含 `evaluation.item_categories.column`**（#379）：同一個理由——它決定的是落在執行目錄裡的大類對照表，不是列上的任何一個值；含進去的話，改了這一欄會連這次沒重寫的月份一起擋。
- **含 `joined`**：母體表缺某個分群欄時 `prepare_eval_data` 只跳過、不報錯，所以設定相同的兩次執行也可能寫出不同的列。例：1–2 月區間（母體有 `tier`，`joined=[tier]`）→ 母體表拿掉 `tier` → 單跑 2 月（2 月分區的 `tier` 變 NULL，那次的 JSON `joined=[]`）→ 接續區間：區間目錄的 JSON 仍是 `joined=[tier]`、設定也全相同，沒有 `joined` 的話 2 月整月會落進對不到的那一段，退出碼 0。設定相同、母體也相同時，單月與區間寫出的指紋照樣相同，互接不受影響。

每個讀表 node 讀之前逐日期比對「今天的設定＋這個目錄 JSON 的 `joined`」（`--compare-only` 例外，設定取 JSON 記錄的，見 5.3 節），上例第 3 步會 raise 並點名 3 月，反方向（先單月、再用別的設定跑區間、再接續單月）一樣。

**成本**：每個讀表 node 多一次小 action（標準執行的 4 個讀表 node 各一次，`--compare`／`--compare-only` 的 `restrict_to_common` 再一次；`--compare-only` 的閘門則是把原本的逐日期空值檢查換成這一次，不另外加）。這次 action 對每個評估日期只讀那個 partition 的一列（每支 `coalesce(1).limit(1)`，全部 union 後 collect 一次），日期數只影響 union 的支數，不整欄掃描。Spark 3.3.2（AQE 開啟）實測：一個日期或三個日期都是 1 個 job。前提是一個 partition 由一次 dynamic overwrite 寫成、整格同一個指紋值，所以一列就代表整格；用別的方式把列塞進某一格會讓這個檢查看不到第二種值。compare 的 `external_hive` 來源是例外：使用者的表不保證依時間分區，所以逐日期用 `isEmpty` 平行檢查（每個日期一個 job），不用單一 task 的讀法。理由與取捨見 [ADR-0020](../adr/0020-evaluation-bug-round-intended-behaviours.md) 文末〈補充（#374）〉。

同一個 model version 與日期下修改 K、segments、categories、baseline、report sections 或 compare source，都會覆寫相同報表路徑；標準／`--compare` 模式也會覆寫相同 enriched partition。
manifest 會保存最後一次執行的 evaluation parameters、git commit、run ID、`post_training` 與 slicing metadata。

設定的指紋不進路徑，寫在每份落地 JSON 裡（`config_fingerprint` 鍵，只涵蓋下一節的「算的」設定）。它不區分產物身分、不保留舊版本，只用來讓讀這些 JSON 的 node 在設定已經變了時 raise，而不是把舊設定算的結果畫進報表。不放進路徑的理由見 [ADR-0020](../adr/0020-evaluation-bug-round-intended-behaviours.md) 決定二。

### 7.2 設定與重跑矩陣

evaluation 的設定分兩類，分法是「改了它，已落地的 JSON 還能不能用」（[ADR-0020](../adr/0020-evaluation-bug-round-intended-behaviours.md) 決定二）：

| 類別 | 鍵 | 變了要做什麼 | 做錯會怎樣 |
|---|---|---|---|
| **算的**：改變 Spark 計算、抽樣，或決定要不要算 | `evaluation.` 底下的 `snap_date`、`segment_columns`、`segment_sources`、`diagnosis.*`、`k_values`、`item_categories.*`、`metric.*`、`query_filter.*`（`drop_all_positive_groups`）、`baseline.*`、`prediction_quality.*`、`report.sections.baseline`、`report.sections.diagnostics`、`report.sections.prediction_quality`、`report.diagnostics.*`。`config_shift` 診斷另外依賴 `dataset.sample_group_keys`、`dataset.sample_ratio`、`dataset.sample_ratio_overrides`、`training.sample_weight_keys`、`training.sample_weights`，這幾個只算進它自己那份 JSON 的指紋 | 從該鍵對應的 node 重跑（`--from-node`），或標準 full run。每個鍵對應哪個 node，錯誤訊息會直接寫出來；對照表住在 `src/recsys_tfb/pipelines/evaluation/steps/config_fingerprint.py` 的 `COMPUTED_KEYS`（例：`metric.*` → `compute_metrics`；`item_categories.column` → `prepare_eval_data`，其餘 `item_categories.*` → `compute_metrics`） | 只做 `--only-node generate_report`，或做了沒涵蓋那個 node 的切片：`render_diagnosis_pages`／`generate_report` 在畫之前 raise，訊息列出哪份產物、哪個鍵從什麼值變成什麼值、該 `--from-node` 哪個 node。不會產出混著新舊設定的報表 |
| **算的**：切換執行模式（`--post-training` ↔ 監控） | 不是 `evaluation.*` 底下的使用者設定，是 CLI 注入 `parameters` 頂層的 run mode（同 `model_version`／`snap_date`），對照表裡的鍵名是 `post_training` | 照錯誤訊息從 `prepare_eval_data` 重跑（`--from-node prepare_eval_data`），或標準 full run | 同一 `(model_version, snap_date)` 先跑過一種模式、再切另一種模式做 `--only-node generate_report`：`prepare_eval_data` 讀的預測表不同（`training_eval_predictions` vs `ranked_predictions`），不擋的話兩種母體的數字會混進同一份報表 |
| **畫的**：只改報表長相 | `evaluation.report.sections` 的其餘鍵（`dataset_overview`、`primary_map`、`diagnosis_links`）、`evaluation.report.display.*` | `--only-node generate_report` | 不會出錯；做 full run 也可以，只是多花時間 |

`query_filter.drop_all_positive_groups`（預設 `false`，#376）打開後，衡量指標（overall／per-item／分群／大類）、熱門度基準線，以及 `--compare`／`--compare-only` 比較報表的兩側都排除「全正的 query group」——組內每一筆候選的 label 都是正例（label 有分級時，每一筆都是同一個正值），不管怎麼排，它的組層級排序指標（mAP／precision／recall）都一樣、排法分不出好壞（CONTEXT.md 的**全正的 query group**）。比較報表的兩側各自依自己的列判定，候選列不對稱的組可能一側排除、一側保留，報表的 coverage 說明會寫出來。不論開或關都會多印全正組數與佔比，佔比超過 10%（寫死在程式，非設定鍵）就警告。**不套用**：頭號 mAP、報表診斷段、各診斷、training 的 test mAP 與 HPO 目標。唯一讀法是 `evaluation/metrics.py::drop_all_positive_groups`；值域檢查是 A49（`core/consistency.py`，[`pipeline-checks.md`](../operations/user-guides/pipeline-checks.md)）。

`report.sections.baseline`、`report.sections.diagnostics` 與 `report.sections.prediction_quality` 雖然在 `sections` 底下，卻是「算的」：設成 `false` 時 `compute_baseline_metrics`／`compute_report_aggregates`／`compute_prediction_quality` 直接不算、只寫 stub。把它們從 `false` 翻回 `true` 只重繪，報表會缺那一段，所以指紋把它們算進去。

判斷依據是 JSON 裡的指紋，不是檔案在不在：

- 在指紋機制之前寫的 JSON 沒有 `config_fingerprint`，第一次接續會被擋下（訊息寫 `has no config_fingerprint`），照訊息重跑一次即可。
- 指紋只看設定，不看資料。`label_table` 回補、預測重發布這類資料變更，要照下表自己決定重跑。

與上表設定無關、但同樣要決定怎麼重跑的情況：

| 修改內容 | 建議執行方式 | 原因 |
|---|---|---|
| 想評估一個尚未產出預測的新月份（`dataset.test_snap_dates` 尚未列入） | 先補資料再評估：dataset → training 的 predict 切片 → 本 pipeline 標準 full run | `model_version` 不變（`test_snap_dates` 不進版本 hash），因此**不重訓**；新舊月份報表並存於同一模型身分之下。步驟見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md) |
| `compare_sources` 或比較對象 | `--compare-only` | Model A enriched data 未變時可直接重做比較 |
| 比較報表使用的 K／category 設定 | `--compare-only` | 會在 common rows 上重新計算雙方 metrics |
| Model A 預測來源由 training 改為 monitoring，或反向切換 | 標準 full run | 必須重新建立 enriched Model A partition |
| `label_table` 回補或修正 | 標準 full run | 監控 labels、baseline 與 compare B labels 可能改變 |
| inference 重新發布同 model/date | 標準 full run | version ID 不變，但預測內容可能已覆寫 |
| 只更換比較顯示 label | `--compare-only` | 不需重算標準報表與 baseline |

### 7.3 Training 與 monitoring 共用 enriched partition

同一個 `model_version + snap_date` 的 post-training 與 monitoring evaluation 會寫入相同 `enriched_eval_predictions` partition，也使用相同 report path。最後一次標準／`--compare` run 會覆寫前一次內容。

因此：

- 不要假設 enriched partition 同時保存 training test 與 production monitoring 兩種母體。
- 兩種模式也共用同一個 schema（聯集）：另一模式寫過的欄，在這次寫入的列上是 NULL。例如先跑 `--post-training`（segment 欄從 `sample_pool` join 進來）再跑監控、而 `inference_population` 沒有那一欄，監控讀回來的列就帶著那個 segment 欄、值全是 NULL。每個讀表的 node 都會拿到這些欄，但不受影響：分群一律照 `evaluation_segment_columns`，不看 frame 有哪些欄（見 3.2 節）。`score_uncalibrated` 兩種模式都有寫（inference 也寫這一欄），只有 inference 開始寫它之前留下的分區才會是 NULL；這一欄已 deprecated、恆等於 `score`，欄位保留只為維持表的欄數（追蹤票 #412）。registry 診斷已改讀 schema 宣告的 score 角色欄（`schema["score"]`），不再讀 `score_uncalibrated`；只在 `--post-training` 組出來是模式本身的設計決定（[ADR-0013](../adr/0013-pipeline-modes-and-slicing-are-separate.md)），不是因為監控模式缺這一欄——監控模式唯一讀抽樣的 `compute_metric_ci` 也不讀它。
- 使用 `--compare-only` 前，先確認最後一次建立 Model A enriched data 的模式。
- 若同一模型同一日期需要長期保留兩種評估情境，現有儲存鍵不足，需另加 scenario partition 或獨立 evaluation version。

指紋機制（§7.2 新增的「切換執行模式」列）只擋得住**同一次評估流程**裡、模式切換之後才做的部分重跑（例如跑完 `--post-training` 再用另一種模式 `--only-node generate_report`）：每份落地產物的指紋都含 `post_training`，不合就 raise，並指示從 `prepare_eval_data` 重跑。`prepare_eval_data` 跟 partition 一起寫的 `segment_columns.json` 也帶指紋，讀表的 node 只比對其中五個決定 `prepare_eval_data` 產出的鍵（`post_training` 是其中之一，見 4.6 節）；partition 自己也帶指紋（含 `post_training`，見 7.1 節），所以「另一種模式的完整執行蓋掉了這個月，再回頭接續前一種模式」會在讀表時被擋下。擋不住的是**不接續、直接把兩種模式各自完整跑完**：後跑的那次會重寫 partition 與報表，前一次的結果就沒了——指紋比對的是設定，不會替你保留被覆寫的內容，仍需照上面幾條手動判斷。

### 7.4 部分重跑的安全邊界

- `catalog.exists()` 只能確認產物存在，不能證明內容來自目前 evaluation settings、label snapshot 或預測資料。設定這一半由指紋補上：落地 JSON 帶 `config_fingerprint`，讀到與目前「算的」設定不合的 JSON 會 raise（見 7.2 節）。label snapshot 與預測資料沒有指紋，資料變了仍要自己決定重跑。
- **這條對 row-level 資料也成立。** `enriched_eval_predictions` 落地之後（[ADR-0018](../adr/0018-evaluation-materialize-at-producer.md) 決定 1），從 `prepare_eval_data` 之後接續會讀表、不重做 join。CLI 只替你問「評估日期的 partition 在不在」（不在就把 `prepare_eval_data` 拉回來），`segment_columns.json` 與 partition 自帶的指紋替你擋「決定 partition 內容的設定變了、或 partition 被另一組設定的執行蓋掉」（見 4.6 節）；partition 在、設定也沒變，但 `label_table` 回補或預測重發布過，讀到的就是舊的列，不會報錯。資料變了要 `--from-node prepare_eval_data` 或 full run。
- `generate_report` 的輸入裡只有 `evaluation_diagnosis_pages` 是 memory-only，指標、baseline、metric CI、report aggregates、預測品質都已落地，因此從該 node 接續只補跑 `render_diagnosis_pages`（監控模式是 `no_diagnosis_pages`），不重算任何指標（見 4.6 節）。已落地的 JSON 讀回時照指紋檢查，不是照單全收；改了 `evaluation.diagnosis.*` 而只做這個接續，會在 `render_diagnosis_pages` 被指紋擋下並提示 `--from-node draw_diagnosis_sample_node`，不會悄悄沿用舊結果。指紋只看設定、不看資料：`label_table` 回補或預測重發布之後只做這個接續，畫出來的是上一次 run 的指標（#351 之前這個接續會順便重算指標與 baseline，但 metric CI 與 report aggregates 本來就沿用舊的）。資料變了要 `--from-node prepare_eval_data` 或 full run。
- `enriched_eval_predictions` 是 evaluation 唯一的 row-level 落地產物：`prepare_eval_data` 寫一次，同一次 run 的其他 node 與之後的 `--compare-only` 都讀它；`--compare-only` 會先驗證指定 model/date partition 非空。
- `--compare-only` 不會更新 enriched partition，也不會重新產生標準 report。
- 位於 slicing 起點之前的資料讀取或驗證可能被跳過；來源資料變更時應 full run。
- slicing 執行會在 manifest 記錄 `resumed_from` 或 `only_node`，但不代表所有報表與 Hive 產物都已同步重建。

## 8. 常見錯誤與排查

檢查分幾層、各在什麼時候擋下、哪些擋不住：[pipeline 的檢查](../operations/user-guides/pipeline-checks.md)。

| 症狀或訊息 | 常見原因 | 檢查與修正 |
|---|---|---|
| 找不到 model version directory | 版本 ID 錯誤，或 training 未完成 manifest | 檢查 `data/models/<version>/manifest.json` |
| 找不到 `best` | 尚未 promotion，卻省略 `--model-version` | 對候選模型明確傳入版本，或先完成人工 promotion |
| 評估到上一版模型 | `--post-training` 仍省略 `--model-version` | post-training 不會自動選最新模型；指定 candidate ID |
| `No predictions found for evaluation.snap_date`（多個日期時是 `No predictions found for N of M evaluation.snap_date dates: [...]`） | 日期錯誤、模式用錯、對應 partition 未產生；多個日期時只有列出的那幾個沒有預測 | 檢查 model、日期、`training_eval_predictions`／`ranked_predictions` |
| `enriched_eval_predictions holds evaluated date(s) not written under ...`，列出 `<日期>: written under other settings` 或 `<日期>: written before partitions carried a settings fingerprint` | 前者：那個日期的 partition 後來被另一組設定（另一種模式、別的分群設定）的執行蓋掉，或那次執行時母體表的分群欄跟現在這個目錄記錄的不同（實際 join 進去的欄不一樣），常見於單月與區間交錯跑再接續；也可能是設定改了之後只做切片（見 7.1 節）。後者：partition 是 #374 之前寫的。`--compare-only` 的閘門會把這兩類和「某日期沒有列」一次列完 | `--from-node prepare_eval_data` 重寫那些日期；`--compare-only` 沒有 `prepare_eval_data`，先用想比的設定重跑那些日期的標準 evaluation |
| `compute_metrics postcondition: N evaluated months ... expected exactly M` | 某個評估日期的 `enriched_eval_predictions` partition 是空的或沒寫過（多半是從 `prepare_eval_data` 之後接續） | 照訊息 `--from-node prepare_eval_data` 重跑 |
| `N duplicated label_table key(s) on [...]` | `label_table` 在該月的 `time + entity + item` 上有重複列 | 在上游去重。evaluation 不替你挑一列：重複的 key 會讓 LEFT JOIN 把候選複製成多列、rank 全錯，而且不會報錯 |
| `Type conflict writing to Hive table '…enriched_eval_predictions' (rank: … ／ label: …)` | 這張表是舊版寫的：當時 `--post-training` 補出來的 `rank` 是 INT（現在是 BIGINT），監控模式的 `label` 沿用 `label_table` 的型別（現在一律 INT） | 表內容可由重跑 evaluation 重建：DROP 這張表，再對需要的月份重跑。同一模式重跑也會撞，不是只有換模式；這張表跨所有 model_version 與月份，DROP 之後到重跑之前 `--compare-only` 找不到 Model A |
| 監控模式的報表沒有診斷入口 | 設計如此：registry 診斷只在 `--post-training` 組出來 | 要診斷改跑 `--post-training`（見 4.3 節） |
| `Evaluation artifacts on disk do not match the current computed settings` | 改了「算的」設定之後只重繪（例如只做 `--only-node generate_report`）；或磁碟上的 JSON 是指紋機制之前寫的（訊息寫 `has no config_fingerprint`） | 照訊息最後一行的 `--from-node` 重跑。只改了「畫的」鍵卻看到它，先對照 7.2 節確認那個鍵的分類 |
| `render_diagnosis_pages: diagnosis input N should be the result of '...'` | `pipeline.py` 裡這個 node 的 inputs 順序與 `DIAGNOSES` 不一致；或讀回的診斷 JSON 是結果帶名字之前寫的（訊息寫 `no 'diagnosis' key`） | 前者修 `pipeline.py`；後者 `--from-node` 該項診斷的 node 重算 |
| `(A22) evaluation.snap_date=... is not a test month`（多個日期時是 `evaluation.snap_date date(s) [...] are not test months`），還沒起 Spark | 帶了 `--post-training`，但該月（或列出的那幾個月）不在 `dataset.test_snap_dates` | 把該月加進 `dataset.test_snap_dates` 並補跑 dataset ＋ predict（見 [新增一個評估月份](../operations/user-guides/adding-an-eval-month.md)），或把 `evaluation.snap_date` 指回已設定的月份 |
| `A34: evaluation.report.sections declares [...]`／`does not declare [...]`，還沒起 Spark | `sections` 多了沒有程式讀的鍵（常見：舊設定還留著 `guardrail_recall`、`per_item_attr`、`category`、`per_segment`），或少了程式讀的鍵（`diagnosis_links`） | 照訊息刪掉或補上；該有哪五個鍵見 3.5 節 |
| 報表正例率異常低 | label 觀察窗未成熟，或 sparse label 的缺 row 不代表負例 | 延後監控、補齊 label，確認資料語意 |
| post-training 與 training 指標不一致 | model/date 不同、K 定義不同，或 report 讀錯版本 | 比對 CLI log、training manifest 與 `k_values` |
| `evaluation.segment_sources.<欄> is missing [...]`／`has segment_column=...`，還沒起 Spark（A10） | 覆寫缺 `table`、`key_columns` 或 `segment_column`，或 `segment_column` 不等於鍵 | 補齊；或刪掉這個覆寫，改從母體表取 |
| `evaluation.segment_sources.<鍵> is not in evaluation.segment_columns`，還沒起 Spark（A10） | 覆寫的鍵不是 `segment_columns` 裡的欄名（常見於舊寫法：鍵取別名、靠 `segment_column` 對應） | 把鍵改成它提供的欄名，並確認該欄列在 `segment_columns`；不需要的話刪掉這個條目 |
| segment source table 無法讀取 | 覆寫的 table 名稱、database 或權限錯誤 | 用 Spark/Hive 確認表存在且可讀 |
| segment source missing columns | 覆寫的 `key_columns` 或 `segment_column` 拼錯 | 比對覆寫表 schema；覆寫必須提供全部欄位 |
| log `population table '...' has no such column`；報表寫「`<表>` 無欄 `<欄>`」 | 該模式的母體表沒有這欄，或 `segment_columns` 欄名打錯 | 核對欄名；確實要分群就讓母體表帶這欄，或用 `segment_sources` 覆寫指到有這欄的表 |
| 分群表出現 `(unmatched)` | 母體表（或覆寫表）對某些 `(time, entity)` 沒有值 | 看它的 query 數與佔比判斷影響；它不進 macro 平均 |
| `segment value '(unmatched)' is the name evaluation reserves` | 真實的 segment 值就叫 `(unmatched)` | 上游改名 |
| per-segment 的表沒出現 | `segment_columns.json` 的 `joined` 是空的（母體表缺欄），或 `report.sections.primary_map` 關閉（per-segment 的表沒有自己的開關） | 看報表「基本統計」段的缺欄註記與 `segment_columns.json` |
| `--compare-only` 讀不到 `segment_columns.json` | enriched partition 是這個機制之前寫的 | 重跑一次標準 evaluation |
| product category unknown product | mapping 引用了未宣告 item | 對齊 `schema.categorical_values[item]` |
| category 結果不符合預期 | item 重複映射或 max-child 語意不適合業務 | 確認每個 item 只屬於一類，重新檢視 category 定義 |
| baseline raise `No label_table history in [...) ... label_table has months: [...]` | lookback window 沒有歷史資料（bug 1，不再是 warning + fallback） | 補歷史 labels、調整 `evaluation.baseline.lookback_months`，或把 `evaluation.report.sections.baseline` 設 `false` |
| `--compare` key 不存在 | CLI key 不在 `compare_sources` | 檢查 YAML key 與錯誤訊息列出的 available keys |
| compare source 沒有該日期資料 | Model B source、model version 或日期不一致 | 查來源 table 的 model/date partitions |
| external item unmapped | `prod_mapping` 未涵蓋外部 item | 補 mapping；確認可接受時才使用 `unmapped_policy: drop` |
| `compare common_query_groups is empty` 或 `compare common_items is empty` | 雙方日期、ID 型別或 item mapping 不一致；共同範圍以 `(time, entity)` 計，同一批 entity 落在不同日期也對不上 | 對齊日期與型別，修正 mapping |
| 比較 coverage 大幅縮水 | 候選母體、客戶母體或外部 mapping 不一致 | 檢查 comparison report coverage 與 dropped items |
| `enriched_eval_predictions has no partition` | 直接使用 `--compare-only`，但 Model A 尚未標準評估 | 先執行標準 evaluation 或 `--compare` |
| compare-only 讀到錯誤情境 | 同 model/date enriched partition 曾被另一種模式覆寫 | 以正確 `--post-training` 狀態重跑標準 evaluation |
| diagnostics 過慢 | 多次 Spark aggregation、items 或 bins 過多 | 關閉不需要的 diagnostics sections 或縮減 K／items |
| `Unknown node` | node 名稱拼錯或 compare mode 的 DAG 不同 | 先用相同模式執行 `--list-nodes` |
| 部分重跑出現大量 auto-included | 所需中間產物為 memory-only（例如 `diagnosis_sample`）、該落地的 JSON 不在磁碟（前一次 run 沒跑完），或這個月的 `enriched_eval_predictions` partition 還沒寫過（會把 `prepare_eval_data` 拉回來，見 4.6 節） | 先用 `--dry-run` 看補跑清單；前次完整 run 成功時，`--only-node generate_report` 只會補一個不跑 Spark 的 node（4.6 節）。要重做比較可改用 `--compare-only` |

## 9. 限制與注意事項

- evaluation 沒有獨立版本 hash；同 model/date 的設定變更會覆寫既有報表與 enriched data。落地 JSON 內的設定指紋只用來擋「接續時讀到舊設定的結果」，不保留舊版本。
- post-training 與 monitoring 共用同一 enriched partition 與報表路徑，無法同時保留兩種情境。
- 目前 per-segment metrics 只使用 `evaluation_segment_columns` 的 `joined` 第一欄，不會在單次 run 中分別計算多個 segment dimensions。
- segment 對母體表的覆蓋率沒有門檻：對不到的 query 只成為 `(unmatched)` 群並印出佔比，不會因為佔比高而失敗。
- comparison 先取 query group（`time × entity`）集合與 item 集合的交集，但不會補齊雙方缺少的 `(entity, item)` rows。若候選 coverage 不對稱，即使 query group／item 集合相同，評估母體仍可能不完全一致。報表 coverage 段的 common query group 數只算裁切後兩側都還在的 group，這種情況下它會小於某一側實際評分的 group 數。
- 比較的兩側都用 Model A 那份 label。`--compare-only` 的 A 是寫 enriched partition 那次標準 run 補的；`label_table` 在那之後回補過，要先重跑標準 evaluation，比較才會用上新答案。
- score 相同時按 item 升冪決定名次（與 inference 同一條規則，`utils/ranking.py`）。名次因此可重現，但同分本身仍代表模型分不出高下。driver 端的診斷與 `metric_ci.json` 也照這條規則排名，而且不依賴診斷抽樣回來的列順序（#355）：同一份抽樣換個 Spark 平行度，這些 JSON 逐位元相同。例外是 `report_aggregates.json` 的分位數——`percentile_approx` 本身可能隨分區怎麼切而變，追蹤於 #366。
- zero-positive query groups 會排除於排序指標，因此報表不代表完整 inference entity 母體。預測品質段（3.7 節）不排除它們，但 `--post-training` 讀的 test 表在 dataset 階段就已經排除了。
- popularity baseline 在 lookback 空窗時會 raise（bug 1），不再 fallback 至完整 label table 產生 leakage；只有 `evaluation.report.sections.baseline: false` 才會整段跳過不算。
- product category 同 item 重複映射目前不會報錯，後出現的 category 會覆蓋前者。
- 比較報表呈現指標差異但沒有 bootstrap、confidence interval 或顯著性檢定。
- evaluation metrics 沒有獨立 JSON／table sink，無法直接形成長期監控時序。
- HTML 由 driver 組裝；雖然 row-level 計算留在 Spark，過多 items、segments、K 或 Plotly 圖表仍會增加報表大小與 driver 負擔。
- 報表沒有自動 pass/fail threshold。模型是否 promotion 仍需由使用者依整體、per-item、per-segment、baseline 與業務限制人工判斷。
- **評估多個日期時，信賴區間可能失準**（issue #389）：診斷頁的分層 bootstrap 會把同一個 entity 在不同日期、落在不同抽樣層的列當成互相獨立的 cluster，CI 偏窄；主報表的 metric CI 在觸發次抽樣時不套用抽樣權重，而多個日期讓 query 數成倍增加、更容易觸發。修正之前，主報表 metadata 在日期列下方會多一句警告；單一日期的報表沒有這一句。
- 評估多個日期時，目錄名只看起迄兩端（見 7.1 節），而且 baseline 的月度趨勢表在視窗重疊時會重複計數（見 3.4 節）。
- `scripts/` 底下的離線診斷腳本（例如 `scripts/item_ability_diagnosis.py`、`scripts/suppression_ledger_diagnosis.py`）只吃單一 `--snap-date`，不支援多個日期或區間。

## 10. 相關文件

- 宣告 `event` 之後 mAP 的語意（按列算）、同分方向的代價、哪幾項診斷會跳過：[`../operations/user-guides/one-row-per-event.md`](../operations/user-guides/one-row-per-event.md)
- 宣告 `occasion` 之後一組是一次請求：小的 query group 讓 mAP 偏高、哪幾項診斷會跳過、離線指標在曝光資料上的三個限制：[`../operations/user-guides/impression-data-shapes.md`](../operations/user-guides/impression-data-shapes.md)
- 模型訓練與 test predictions：[`training.md`](training.md)
- 正式批次推論與發布閘門：[`inference.md`](inference.md)
- 上游 label 與來源表：[`source_etl.md`](source_etl.md)
- Dataset test 母體與 zero-positive query filtering：[`dataset.md`](dataset.md)
- 指標定義與報表解讀：[`../metrics/metrics.html`](../metrics/metrics.html)
- 診斷產物判讀（metric_ci CI 欄、訓練側 Gain 帳本／SHAP）：[`evaluation-diagnosis.md`](evaluation-diagnosis.md)
- 資料表、partition 與完整 lineage：[`../data-lineage.html`](../data-lineage.html)
- 版本化、恢復與人工卡控設計背景：[`../design-principles.md`](../design-principles.md)
