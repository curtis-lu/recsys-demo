---
status: accepted
date: 2026-10-26
---

# 全部候選列的指標自成 evaluation 的一個模式：母體標清楚、r 從被評模型讀、可限定範圍

## 背景

**全部候選列的指標**（`CONTEXT.md`）：母體是全部候選列、含沒有正例的 query group 的指標。做法是把每一列當成一次「會不會是正例」的預測，算分箱表、門檻掃描、AUC-ROC、AUC-PR。ADR-0024 把它定為自成一個家族，ADR-0031 之前它是主報表的「預測品質」段，開關是 `evaluation.report.sections.prediction_quality`，框架預設關、廣告示例打開。

這類指標的母體必須含沒有正例的組（[ADR-0024](0024-prediction-quality-metric-family.md) 決定 3、[ADR-0025](0025-query-group-widened-by-occasion-role.md) 決定 3）。放在主報表裡有三個問題：

1. **開了就可能擋掉整份報表。** `--post-training` 讀的 test 表只留 `dataset.test_zero_positive_group_ratio`（下稱 r）比例的無正例組。開關打開又 r＝0（r 的預設）時，入口檢查 A46 擋下整次 evaluation，連不受影響的排序指標都不產。
2. **報表沒寫清楚數字是哪一種。** 讀的人分不出是「r 下的估計」還是「上線後的精確值」。
3. **r 讀錯來源。** 「要不要加權」、A46、報表「資料統計」段印的 r、設定指紋裡的 r，讀的都是今天設定檔的 r，不是被評模型建 dataset 時的 r。權重的值本身取自預測表的權重欄，這部分沒錯。用 `--model-version` 評舊模型時：
   - 今天 r＝0、模型 r＝0.2：A46 誤擋，訊息叫使用者去重建 dataset；繞過 A46 的話，會靜默不加權。
   - 兩邊 r 不同：報表印錯 r。

另一個需求在 #576：被打分的母體不一定是真正要行銷（或曝光）的母體，使用者想只在「符合條件的那部分」裡量 AUC。條件有兩種：只限 entity，以及 entity × item。

（A40、A46、A54 等代號的意思，寫在 `src/recsys_tfb/core/consistency.py` 開頭的模組 docstring。）

## 決定

### 1. 正確的母體是使用者在 `sample_pool` 定義的候選，而且沒有正例的組全留

- **一般部署（候選＝全網格）**：上線後的監控母體就是它，算出來是精確值。`--post-training` 讀的是 r 下的估計。
- **展示紀錄部署（宣告了 `occasion`／`event`，候選＝展示過的列）**：inference 的全網格會把沒展示的 item 補成 label 0，是假的負例，所以沒有上線後這條路。精確值要在建 dataset 時設 test 的 r＝1。A40 不動。

### 2. 搬出主報表，改成 evaluation 的一個模式旗標 `--prediction-quality`

- 主報表只留排序指標；`evaluation.report.sections.prediction_quality` 開關拿掉。
- 新模式跟 `--compare-only` 一樣，是 evaluation pipeline 裡的另一組 node，不開新 pipeline。補 label、指紋、報表外殼都在 evaluation pipeline 內部，別的 pipeline 不能 import（架構約束 S3、S8）。
- 它讀已落地的 `enriched_eval_predictions`，不重接 label，只算全部候選列的指標。
- `evaluation.prediction_quality` 的參數（`n_bins`、`n_display_bins`、`top_n`）照舊歸它用，A42 照舊檢查值域。
- 舊設定還寫著那個開關時，A34（段落開關要跟清單相等）報錯，訊息指向 `--prediction-quality`。這條不能併進 `RENAMED_REPORT_SECTIONS`：那張表的值必須是現存的段名，而新模式不是主報表的段，所以另開一條分支。這條擋舊鍵的檢查屬於過渡期檢查，記進 #524 的退場清單。

### 3. 原料跟 evaluation 同一套習慣

- 不帶 `--post-training`：讀 inference 那份（上線後母體，不加權）。這是主要用法。
- 加 `--post-training`：讀 test 那份。
- `enriched_eval_predictions` 每個模型、每個月份只有一份，由最後一次跑的標準 evaluation 決定是哪一份。新模式用既有的分區指紋（`steps/snap_date_scope.py::restrict_to_current_eval_partitions` 那套）確認「這格是這個模式寫的」；不是就停下，告訴使用者先跑哪一條指令。

### 4. r 從被評模型讀

- 讀法同 training 入口的 A54（`pipelines/training/run_contract.py`）：從被評模型的 dataset version manifest 讀。
- 「要不要加權」、主報表「資料統計」段印的 r、設定指紋裡的 r，全部改用這個值。
- **r＝0**：`--prediction-quality --post-training` 在入口停下並說明。打開方法是設 r＞0、重建 dataset、重訓。這是新的入口檢查 A64；A46 退休（主報表沒有這一段了）。
- **0＜r＜1**：有正例的組每列權重 1，留下的無正例組每列權重 1／r。報表寫「加權估計」與留下的無正例組數。
- **r＝1**：test 母體上的精確值。

### 5. 輸出位置與報表開頭

- 輸出放在 `data/evaluation/<model_version>/<snap_date>/prediction_quality/<post_training 或 monitoring>/<範圍名>/`，內含 JSON 與 `report.html`。不指定範圍時範圍名是 `all`。
- 報表開頭寫：哪份原料、r 與留下的無正例組數（`--post-training` 時）、「label 觀察窗要成熟才準」的提醒。
- 報表本體沿用原本主報表那一段的畫法，搬進新的一頁。
- 這一族的數值只搬位置、不改算法，所以搬前搬後可以逐位比對。

### 6. 互斥

`--prediction-quality` 跟 `--compare`、`--compare-only` 互斥。

### 7. 範圍：`--scope` 只在使用者指定的範圍內算

- **範圍表**是使用者自己的 Hive 表，欄位是 `(time, entity)` 加可有可無的 `item`。不寫 item 就是只限 entity；寫了 item 就是 entity × item。
- **設定**照 `evaluation.compare_sources` 的先例，每個範圍取一個名字，放在 `evaluation.scopes` 底下。每個範圍有 `table` 與 `columns`：`columns` 左邊是 schema 角色解析出的欄名，右邊是範圍表的欄名；只限 entity 時不寫 item 那一格。範圍名同時是輸出資料夾名。
- **指令** `--scope <名字>` 只能跟 `--prediction-quality` 一起用；不給就算全部（輸出在 `all/`）。
- **範圍只套在全部候選列的指標**，主報表的排序指標不受影響。
- **範圍裡沒被評分過的列**（例如那個 entity 不在 inference 母體裡），數出來印在報表開頭，連同範圍名與圈到的列數。
- **item 值由使用者的 SQL 對齊框架的 item**，框架不提供對照表。
- **入口檢查**（代號編號時照 `core/consistency.py` 的規矩加 predicate）：
  - `evaluation.scopes` 的形狀不對、`--scope` 給了沒登記的名字、不帶 `--prediction-quality` 就用 `--scope`，都在入口報錯。
  - 範圍表不存在、或 `columns` 指到的欄不存在，在開始算之前報錯，訊息點名是哪個範圍、哪一欄。

## 考慮過、沒選的做法

- **兩份原料（test、inference）同時保留。** 要做，得給 `enriched_eval_predictions` 加一層模式分區；Hive 建表後不能加分區欄，只能砍表重建，舊模型的 `--compare-only` 要全部重跑。比較來源讀這張表時也繞過 catalog，會一次讀進兩份。輸出資料夾已經分開，結果不會互蓋，這就夠了。
- **新模式支援 `--compare`。** 比較報表的對照側不帶權重，共同母體也被主側縮過，兩邊不可比。
- **label 成熟度的程式檢查。** 觀察窗有沒有結束只有使用者知道，框架沒有資料可以判斷；報表開頭提醒。
- **inference 的「只算不寫正式表」模式。** `inference --model-version <M>` 寫在 M 自己的分區，不會蓋到 `best`，已經能用來評候選模型。
- **範圍也套到排序指標。** 拿掉部分 item 會改變組內名次，問的是另一個問題。
- **item 值的對照表**（像 `compare_sources` 的 `prod_mapping`）。使用者寫範圍表的 SQL 時就能對齊；真的有人需要再加。
- **把新的數字（每個 item 都有 AUC、pooled AP、per-item precision@K）一起做。** 在 #577。這一族只搬位置，數字不變，才能逐位比對。

## 後果

- 舊路徑的 `prediction_quality.json` 不再有人讀。新位置一開始沒有結果，每個模型與月份要先跑一次新模式。
- 使用者看得到的用法：

  ```bash
  # 主要用法：上線後母體
  python -m recsys_tfb evaluation --env dev --model-version <M> --prediction-quality
  # 上線前估計：test 母體
  python -m recsys_tfb evaluation --env dev --model-version <M> --post-training --prediction-quality
  # 限定範圍
  python -m recsys_tfb evaluation --env dev --model-version <M> --prediction-quality --scope <範圍名>
  ```

  （`<M>` 是模型版本，`<範圍名>` 是 `evaluation.scopes` 登記的名字，每次不同。）
- `steps/config_fingerprint.py` 的 `APPLIED_SETTINGS`：新 step 沒登記會 raise，新模式的 step 要一起登記。
- 影響到的程式、設定、catalog、示例與文件，開工時用 `grep -rn prediction_quality` 列全，以那次的結果為準；本份不列清單。
