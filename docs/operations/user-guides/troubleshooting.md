# 出事了、改了設定：先看這裡

這份給已經在跑自己題目的人，回答三個問題，各一節：

1. **指令失敗了**：它停在哪一層、先查什麼。
2. **跑成功了，但結果不對**：最常見的資料與設定錯誤。
3. **改了設定**：要重跑哪幾條 pipeline。

每一層檢查的完整清單（擋什麼、什麼時候擋、哪些擋不住）在 [`pipeline-checks.md`](pipeline-checks.md)；本文件只講「看到這個症狀，先往哪裡找」。指令裡的 `dev` 是環境名稱的例子（見 [README](../../../README.md) §5 步驟 0），換成你自己取的名字。

---

## 1. 指令失敗了：先看它停在哪一層

框架把檢查放在越早越好的地方：能只看設定就判斷的，在啟動 Spark 之前就擋；要看資料的，在讀到資料的第一步擋；要看結果的，在寫出去之前擋。所以**停在哪一層，就告訴你問題出在哪一類東西上**。

| 什麼時候停／看到什麼 | 代表什麼 | 先查什麼 |
|---|---|---|
| CLI 一啟動就出現 `Config consistency check failed` | 設定檔之間互相矛盾；還沒讀任何資料 | 訊息會一次列出所有問題，每一條帶設定路徑與不一致的值。常見的是 item 清單兩處不同、同一欄既是類別欄又被丟掉、objective 與 metric 不搭、抽樣比例的 key 裡有不認得的 item、樣本權重要用的欄沒放進 `carry_columns` |
| 訊息裡有 `(A30)` | `--env` 指到一個不存在的 `conf/<env>/` 目錄 | 訊息會印出它找的絕對路徑與 `conf/` 底下現有的目錄。多半是打錯字，或還沒建自己的環境目錄（見 [`using-a-release.md`](using-a-release.md) §5） |
| `dataset` 第一步出現 `DataConsistencyError` | 設定與 Hive 裡的實際資料對不上 | `sample_pool`／`label_table` 裡 item 欄實際出現的值、`feature_table` 各欄的實際型別 |
| source ETL 出現 `Source check FAILED`，或寫完後的輸出檢查失敗 | 上游 partition／schema 還沒到齊，或產出的表品質不合格 | 失敗的日期、必要欄位、列數、重複鍵比例、NULL 比例。上游檢查只有帶 `--source-check` 時才會跑 |
| inference 出現 `ValidationError` | 排序結果沒通過發布條件，**沒有寫進正式結果表** | 列數、NULL、重複列、有沒有不認得的 item、每個 query group 的候選是否完整、`rank` 是否與 `score` 一致、組內分數是否全部相同 |
| 一般的 `Node '<name>' failed` | 某一個 node 執行時出錯 | 從 log 找**第一個**失敗的 node，再查該 pipeline 文件裡那個 node 的輸入與產物 |

錯誤訊息會列出所有它找到的問題。**先把訊息列的全部修完再重跑**，不要修一條跑一次——每次重跑都要付啟動成本。

---

## 2. 跑成功了，但結果不對

下面這些錯誤大多**不會讓指令失敗**。框架照樣算完、照樣產出報表，只是數字的意思已經不是你以為的那樣。

| 常見錯誤 | 會造成什麼 | 怎麼避免或修正 |
|---|---|---|
| 忘了帶 `--env` | `--env` 的預設是 `local`，而 repo 附的 `conf/local/` 是空的。整場只用 `conf/base/` 的示例設定跑完，你的設定一條都沒生效，也沒有任何錯誤 | 每一條指令都明寫 `--env dev`（或你自己取的環境名稱） |
| `sample_pool` 只放發生過事件的 item | 同一個 query group 裡幾乎沒有負例，模型學不到「哪些候選該排後面」 | `sample_pool` 放的是「當時有資格被排序的全部候選」；哪些成為正例由 `label_table` 標記 |
| 把「label 還沒到齊」當成 0 | 大量正例被標成負例，指標和模型方向都失真 | 先確認觀察窗已經結束、來源 partition 已經到齊；只有「確定沒發生」才能當 0 |
| 特徵用到了 `time` 之後才產生的資訊 | test 指標異常漂亮，上線後拿不到同樣的資訊 | 特徵 SQL 要做 point-in-time join（只取 `time` 當下已知的值）。框架不檢查這件事，是來源 SQL 的責任 |
| 同一個日期的來源資料回補後重跑，結果沒變 | 版本只看設定、不看資料內容；那個日期已經做過的東西會被直接沿用 | test 的日期：dataset 與 training 都帶 `--rebuild-dates <日期>` 重跑，兩條都要（指令見 [`adding-an-eval-month.md`](adding-an-eval-month.md#上游回補了要重算某個月份)）。train 或 val 的日期：目前沒有指令能只重算它們（dataset 與 training 的 `--rebuild-dates` 只收 test 的日期） |
| 日期沒有重疊，但 val／test 早於 train，或 label 還沒成熟 | 拿未來的資料評估過去，或 ground truth 不完整 | 照 `train → val → test` 的時間順序切，每個日期都留足 label 觀察窗 |
| 連續數值欄被列進 `categorical_columns` | 每個數值被當成一個獨立類別，編碼意思錯了 | 類別代碼先轉成字串或整數；真正的連續數值欄不要列進去 |
| 手寫 `sample_weights`，key 跟資料對不上 | 那條權重規則沒套用（權重維持 1.0），冷門 item 或重要客群的權重沒有被調到 | 用 `scripts/sampling_overrides_editor.py` 產生 key；training 的 `manifest.json` 會列出沒對上的 key（`sample_weight.unmatched_keys`），要看 |
| 用 ranking objective（`lambdarank`／`rank_xendcg`），卻把 `score` 當機率讀 | ranking objective 的分數是沒有上下界的實數，不在機率的尺度上 | 下游需要機率語意，改用 `binary` objective，並由下游自己驗證校準；框架不提供機率校準 |
| evaluation 用錯模式或日期 | 報表是空的、評錯資料，或用了還不完整的 ground truth | 訓練後評估 test 用 `--post-training --model-version <剛訓練的版本>`（不帶版本會評到 `best`）；上線後監控用預設模式，而且要等那一期的 label 觀察窗結束 |
| training 完直接跑 inference | 從來沒 promote 過：inference 會停下並提示先 promote。**以前 promote 過：它會安靜地繼續用舊模型** | 看完評估報表，用 `scripts/promote_model.py <model_version>` 把新版本設為 `best`（見 [`promoting-a-model.md`](promoting-a-model.md)） |

---

## 3. 改了設定，要重跑哪些

### 先懂一件事：版本分三層

框架替每一層產物算一個版本 ID，ID 由「會影響這層產物的設定」決定。設定沒變，ID 就不變，已經產出的東西直接沿用；設定變了，只有受影響的那幾層會重算。

```
base_dataset_version        前處理器、val、test   ← schema、特徵欄、類別欄／丟掉的欄、
   │                                               train／val 的日期、val 的抽樣
   └─ train_variant_id      train、train_dev      ← train 的抽樣與切分、carry_columns
         │
         └─ model_version   模型、test 的預測     ← 上面兩個版本 ＋ objective、HPO、
                                                    特徵選擇、樣本權重
```

- **`test_snap_dates` 不在任何一層裡。** 它只決定要評估哪些月份，所以多評估一個月份不用重建資料、不用重訓（見 [`adding-an-eval-month.md`](adding-an-eval-month.md)）。
- **版本看的是設定，不是資料內容。** 同一日期的資料回補不會讓版本變，要自己叫它重算（見上一節）。升級框架時，版本可能變、也可能不變，要看那一版改了什麼（見 [`using-a-release.md`](using-a-release.md) §7）。

每個鍵算進哪一層的精確定義在 `src/recsys_tfb/core/versioning.py` 的模組說明。

### 重跑對照表

| 改了什麼 | 翻哪一層 | 要重跑 |
|---|---|---|
| 來源 SQL 改了 `feature_table` 的欄位（名稱、型別、順序） | `base_dataset_version` | source ETL → dataset → training → evaluation，核准後再 inference |
| 同一日期的資料回補（欄位沒變） | 不翻 | 受影響日期的 source ETL →（test 的日期）dataset 與 training 都帶 `--rebuild-dates` → evaluation（之後月份的報表也要重跑，見 [`adding-an-eval-month.md`](adding-an-eval-month.md#上游回補了要重算某個月份)）。train 或 val 的日期目前沒有指令能只重算，見上一節 |
| schema、特徵欄、`categorical_columns`、`drop_columns`、train／val 日期、`val_sample_ratio` | `base_dataset_version` | dataset → training → evaluation，核准後再 inference |
| `sample_ratio`、`sample_ratio_overrides`、`sample_group_keys`、`train_dev_ratio`、`carry_columns` | `train_variant_id` | dataset（只重建 train／train_dev）→ training → evaluation |
| `test_snap_dates`（加一個評估月份） | 不翻 | 照 [`adding-an-eval-month.md`](adding-an-eval-month.md) 的四個步驟 |
| objective、HPO、`feature_selection`、`sample_weights` | `model_version` | training → evaluation，不用重建 dataset |
| inference 的日期 | 不翻 | 先替新日期跑 `feature_etl` 與 `inference_population_etl`，再跑 inference；上線後的 evaluation 要等 label 成熟 |
| evaluation 的指標、分群或報表設定 | 不翻 | 只重跑 evaluation；已經有 `enriched_eval_predictions`、只想產比較報表時，用 `--compare-only <比較對象>` |

一個例外：`carry_columns` 裡的欄如果**也是** `feature_table` 的欄，它必須同時列進 `drop_columns`，而 `drop_columns` 會翻 `base_dataset_version`——這時就是第二列的重跑範圍。

每條 pipeline 的部分重跑（`--from-node`、`--only-node` 這類）見 [`pipeline-slicing.md`](pipeline-slicing.md)。
