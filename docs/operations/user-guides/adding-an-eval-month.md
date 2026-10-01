# 新增一個評估月份

想知道**現有的模型**在**新的一個月**上表現如何。

模型不會被重訓，也不會產生新版本。新月份的報表會跟既有月份並排存在同一個模型底下，可以直接互相比較。

promote 挑版本的比較範圍也不會動：步驟 1 先把 training 計分用的月份固定下來，加進來的月份只拿去出報表。要讓 promote 改用新月份比，是另一件事，見〈[要讓 promote 用新月份比](#要讓-promote-用新月份比)〉。

**先確認你屬於哪一種：**

- 這個月份**沒跑過**，你要第一次評估它 → 往下讀。
- 這個月份**跑過了**，但上游補了資料，你要重算 → 跳到〈[上游回補了，要重算某個月份](#上游回補了要重算某個月份)〉。本文的四個步驟幫不了你，因為每一步都會判定這個月「已經做完」而直接跳過。

下面的指令都用 `--env production`（`production` 是環境名稱的例子，見 [README](../../../README.md) §5 步驟 0；換成你自己取的名字）。本機執行請先照 [local-spark-setup.md](../dev-setup/local-spark-setup.md) 設好環境，並把 `--env` 換成 `local`。

## 整件事長什麼樣

```
 步驟 1          步驟 2            步驟 3              步驟 4
 改設定    →     dataset     →     training      →     evaluation
                （造資料）        （舊模型預測）      （出報表）
   │               │                  │                  │
   ▼               ▼                  ▼                  ▼
 固定計分月份、   新月份的           新月份的            report.html
 test_snap_      model_input        預測值
 dates 加一行

 ┌───────────────────────────────────────────────────────────┐
 │  模型全程沒有被重訓 —— model_version 從頭到尾都是同一個   │
 └───────────────────────────────────────────────────────────┘
```

## 動手前先確認

**① 新月份的來源資料齊了。**

特徵表跟母體表沒齊的話，pipeline 會直接報錯，你會知道。

**標籤沒齊則不會報錯**——那些候選會被當成負例，指標安靜地偏低，報表看起來完全正常。標籤的觀察窗有沒有結束，只有你自己知道，框架不會擋你。

**② 抄下目前的三個版本號。** 跑完要拿它們比對。

```bash
ls -1 data/models/          # 目錄名就是 model_version
```

`base_dataset_version` 與 `train_variant_id` 都在上一次 dataset 執行的 log 裡（`base_dataset_version: …` 與 `train_variant_id: …` 兩行相鄰）；前者也可以從 `data/dataset/` 底下的目錄名讀到。

**`train_variant_id` 一定要一起抄。** 只比對 `base_dataset_version` 會漏掉一整類錯誤——原因寫在步驟 2。

**③ 框架升級之後、重訓之前，這套流程用不了。** 升級前後比對 `core/versioning.py` 的兩個值（舊版沒有這個常數的，算 0：第一次出現就等於加了 1）。任一個加了 1，training 算出的 `model_version` 就不再是現役模型的：

- **dataset 產物格式版本**（`DATASET_ARTIFACT_FORMAT_VERSION`）：每個部署的 `base_dataset_version` 都會換掉。新版本還沒建，步驟 2 的 `--only-test-months` 會在開跑前被擋下（A55）。先照 [dataset 手冊 §7.4](../../pipelines/dataset.md#74-修改設定時要重跑什麼) 重建 dataset，再重訓。
- **training 模型格式版本**（`TRAINING_MODEL_FORMAT_VERSION`）：dataset 不受影響，步驟 2 照常，但步驟 3 算出的 `model_version` 底下沒有模型，`--only-node` 會把超參數搜尋與訓練一起拉回來——那是重訓。先重訓。

重訓之後才能對新模型用這套流程。已經部署的推論與評估讀的是模型 manifest 記的版本，不受影響。

同一個檔的 **training 預測格式版本**（`TRAINING_PREDICTION_FORMAT_VERSION`）加了 1 不擋這套流程，也不重訓；只是步驟 3 會把所有月份的預測重寫一次（見步驟 3 的「成功的話」）。

## 步驟 1：把新月份加進設定

**先固定計分月份。** 打開 `conf/base/parameters_training.yaml`，看 `test_metrics.snap_date`：

- **有寫**：不用動，跳到下面加新月份。
- **沒寫**（出貨的設定是註解掉的）：把**現在的** `dataset.test_snap_dates` 原樣抄進去，寫法要一模一樣：

```yaml
test_metrics:
  snap_date: "2026-01-31"   # 現在的 dataset.test_snap_dates，一個字都不改
```

為什麼：training 在 test 上算分數時，只算「計分月份」（`test_metrics.snap_date`）；沒寫就是整份 `dataset.test_snap_dates`。promote 挑版本時，只拿「當初算分數的月份跟現在的計分月份完全一樣」的版本互相比。所以沒寫這一行就加月份，計分月份跟著變成兩個月，**每一個**版本記下的都只有舊月份，全部退出 promote 的排名。寫下這一行，計分月份就停在舊月份，加進來的月份只給 evaluation 用。

**再把新月份加進去。** 編輯 `conf/base/parameters_dataset.yaml`：

```yaml
dataset:
  test_snap_dates:
    - "2026-01-31"
    - "2026-02-28"   # 新增這一行
```

**舊月份要留著**，這是累積清單不是替換清單。把某個月份刪掉只是讓它以後不再被處理，已經產出的資料跟報表不會消失。

`test_metrics.snap_date` 裡的月份也都要留在 `dataset.test_snap_dates` 裡，否則 training 一開始就會擋下（訊息帶 `(A53)`）。

## 步驟 2：產出新月份的資料

```bash
python -m recsys_tfb dataset \
  --env production --only-test-months
```

`--only-test-months` 告訴 pipeline「這次只是加評估月份」，於是它跳過訓練資料那一整段的重算。**只有當你這次除了加月份還改了別的設定時才不要帶它**（改了抽樣、特徵、訓練月份……那些改動需要被跳過的部分重算才會生效）。不確定就不要帶，跑完整的一輪永遠是安全的那一邊。

**成功的話**，log 裡會有這幾行：

```
base_dataset_version: <跟你抄下來的一模一樣>
train_variant_id:     <跟你抄下來的一模一樣>
[plan] only-test-months: 7 of the dataset pipeline's 17 nodes; ...
[months] dataset=test_keys         processed=2026-02-28 skipped=2026-01-31
[months] dataset=test_model_input  processed=2026-02-28 skipped=2026-01-31
```

`processed` 是新月份、`skipped` 是舊月份，就對了。另外還有一行 `dataset=preprocessed_feature_table`，它涵蓋的月份範圍更廣、訓練月份也算在內，月份清單跟前兩行不一樣是正常的。

**開跑前它會先確認一件事**：這個模式不建訓練資料，只能沿用以前建好的那一份。所以它會先向 metastore 問：目前設定算出的 `base_dataset_version`、`train_variant_id` 底下，`train_model_input` 有沒有分區。沒有的話，這一輪如果照樣跑完，步驟 3 讀到的訓練資料會是 0 列，而且不會報錯；所以它在任何 node 執行之前就停下來，訊息帶 `(A55)`，不寫任何資料或 manifest（run log 照常寫）。訊息有兩種：

- **`... has partitions under base_dataset_version=... but none under train_variant_id=...: the train sampling settings changed ...`**：你這次還改到了抽樣設定（`sample_ratio` 那一類）。改抽樣只會讓 `train_variant_id` 變，`base_dataset_version` 不變，所以只看 `base_dataset_version` 會以為沒改東西。二選一：把抽樣改動還原、只留新增的月份，或是不帶旗標跑完整的一輪。
- **`... has no partition under base_dataset_version=..., so this base version has never been built ...`**：這個 `base_dataset_version` 底下從來沒建過訓練資料。原因有三種：第一次跑；你這次不只加月份，還改到了 base 層的設定，或特徵表的 schema 變了；框架升級時把「dataset 產物格式版本」加了 1。後兩種都代表資料版本換了，現有模型對不上它。是你沒打算做的改動，就還原它；確實要換版本，就不帶旗標跑完整的一輪、再重訓——那已經不是「只加一個評估月份」了。

**沒被擋下、但還是要停的一種**：`base_dataset_version` 或 `train_variant_id` 跟你抄下來的不一樣。

代表你改到的設定剛好對到一個以前建過的版本（例如把抽樣設定改回以前用過的值），所以檢查放行了。但現在的模型不是用那個版本訓練的。**停下來**，不要跑步驟 3。把其他改動還原，只留新增的月份。

**跑完之後**，`data/dataset/<base_dataset_version>/train_variants/latest` 通常會指向目前設定的那個 `train_variant_id`，步驟 3 讀的就是它。條件是 train 的 model input 在這個 variant 底下都有表：`train_model_input`，以及 `train_dev_ratio` 不是 0 時的 `train_dev_model_input`。開跑前只查了前一張，所以少了後一張時（例如之前某次切片只建了 `train_model_input`），這一輪照樣跑完，但 log 最後會有兩行 `[train_variant]` 警告，`latest` 不動。看到它就不要跑步驟 3，先不帶旗標跑完整的一輪。

## 步驟 3：用現有模型對新月份產生預測

```bash
python -m recsys_tfb training \
  --env production --only-node predict_and_write_test_predictions
```

**一定要用 `--only-node`。** 換成 `--from-node` 會把下游全部拉進來，包含超參數搜尋——那是重訓，不是預測，會產生一個新的 `model_version`，這份文件的前提就不成立了。

不放心可以先加 `--dry-run` 看它打算跑哪些步驟再決定。

**成功的話**，log 裡會有：

```
[months] predict: processed=2026-02-28 skipped=2026-01-31 rebuilt=-
```

框架升級時把「training 預測格式版本」加了 1 的話（步驟 ③ 最後一段），這一輪會把**每個**月份都重寫：`processed` 列出所有月份、`skipped` 是 `-`，前面還有舊月份各一行警告：

```
[months] predict: 2026-01-31 is complete, but its predictions were recorded in prediction format 1, not 2 (this code's TRAINING_PREDICTION_FORMAT_VERSION); re-predicting it.
```

這是預期的，只會發生這一次：評分程式改了、模型沒變，舊月份的預測要用新程式重寫。模型沒有重訓——「跑完了，怎麼確認真的成功」第 1 件照樣成立。

舊月份的預測換了，用它們算出來的東西不會自己跟著換：training 在 test 上的分數（`evaluation_results.json`，promote 讀它）與舊月份的 evaluation 報表，都還是舊預測算的。要更新分數就跑〈[要讓 promote 用新月份比](#要讓-promote-用新月份比)〉第 2 步那一行（`--only-node compute_test_metrics`）；舊月份的報表要更新，就對那些月份各跑一次步驟 4。

**最常壞的一種**：拋 `FileNotFoundError`，路徑裡帶著新月份。

步驟 2 沒跑成功，來源表裡沒有這個月。回去重跑步驟 2。

## 步驟 4：產出報表

編輯 `conf/base/parameters_evaluation.yaml`，把日期指到新月份：

```yaml
evaluation:
  snap_date: "2026-02-28"
```

```bash
python -m recsys_tfb evaluation \
  --env production --post-training --model-version <你的 model_version>
```

`--post-training` 讓它讀步驟 3 產生的預測。

也可以一次評估多個月，把新舊月份合成一份報表：`snap_date` 寫成清單或 `{start, end, step}` 區間，每個月都要在 `dataset.test_snap_dates` 裡，寫法見 [evaluation.md §3.1](../../pipelines/evaluation.md#31-評估日期與-k)。

**成功的話**，這個檔案會出現：

```
data/evaluation/<model_version>/20260228/report.html
```

**最常壞的一種**：忘了 `--model-version`。

省略它時 evaluation 會去找 `best` 這個標記，那指向的是已經正式核准上線的模型——不見得是你這次要評估的那一個。你會拿到一份看起來正常、但評的是別的模型的報表。

## 要讓 promote 用新月份比

上面四步做完，新月份只進了報表。想讓 promote 改拿新月份（或新舊一起）來比版本，要兩步：

1. 把 `test_metrics.snap_date` 改成你要的月份，例如兩個月都算就寫成清單；或刪掉這一行，回到「整份 `dataset.test_snap_dates`」。
2. 替現在設定的模型重算分數：

```bash
python -m recsys_tfb training \
  --env production --only-node compute_test_metrics
```

**成功的話**，log 裡會有：

```
compute_test_metrics: scoring ['mean_ap', 'macro_per_item_map'] on ['2026-01-31', '2026-02-28'] (selection metric macro_per_item_map, HPO objective macro_per_item_map)
```

`on` 後面是你在第 1 步寫的月份。`data/models/<model_version>/evaluation_results.json` 的 `snap_dates` 也會變成這幾個月。

**要知道的代價**：第 2 步只重算**現在設定**對應的那一個 `model_version`。其他版本記下的還是舊月份，從這一刻起全部退出 promote 的排名，直到各自重算；設定已經改過的舊版本怎麼重算，見 [替舊版本補分數](rescoring-an-old-version.md)。MLflow 裡的數字不會更新（那是 `log_experiment` 寫的），promote 讀的是 `evaluation_results.json`，不受影響。

## 上游回補了，要重算某個月份

上游對一個**已經跑過**的月份補了或修了資料之後，重跑上面的四個步驟沒有用：那個月在 dataset 跟 predict 兩層都已經是「做完」狀態，兩層都會跳過它。

**兩層都要用 `--rebuild-dates` 指名重算，月份寫一樣：**

```bash
python -m recsys_tfb dataset \
  --env production --only-test-months --rebuild-dates 2026-01-31
python -m recsys_tfb training \
  --env production --only-node predict_and_write_test_predictions --rebuild-dates 2026-01-31
# 多個月份用逗號分隔：2026-01-31,2026-02-28
```

只跑第一行，預測還是舊資料算的；只跑第二行，讀到的還是回補前的資料。兩種情況報表數字都不會動。

這兩行的月份必須已經在 `dataset.test_snap_dates` 裡，不在的話會在 Spark 起來之前就報錯退出。

然後回到**步驟 4** 重跑 evaluation，該月的報表才會更新。

**之後月份的報表也要重跑。** 報表裡的熱門度基準線（baseline）會回頭看評估月份**之前** `evaluation.baseline.lookback_months` 個月的資料，所以回補月份之後、在這個範圍內已經出過的報表也過時了，各自重跑一次步驟 4。

設了 `evaluation.baseline.score: rate` 的話還要多一步。rate 把每個月的 label 與 `sample_pool` 數字算一次就存進一張所有模型共用的表，之後沿用、不重算。所以回補之後，每次帶 `--post-training` 跑 evaluation，回看範圍裡有**還沒點名過**的回補月份，就加 `--rebuild-dates <那些月份>`；每個回補月份點名一次就好，不管是哪個模型的 evaluation 點的。評估月份不在自己的回看範圍裡，不要列它自己，列了會被擋下。例：補了 1、2 月，重跑 2 月的報表帶 `--rebuild-dates 2026-01-31`，之後跑 3 月帶 `--rebuild-dates 2026-02-28`（見 [evaluation.md §3.4](../../pipelines/evaluation.md#34-popularity-baseline)）。

## 跑完了，怎麼確認真的成功

四件事都對才算完成：

```bash
# 1. 沒多出新模型 —— 這是「沒有重訓」的證據
ls -1 data/models/
#    跟你動手前抄下來的清單一樣，沒有新目錄

# 2. 新舊報表並存在同一個模型底下
ls -d data/evaluation/<model_version>/*/
# → data/evaluation/<model_version>/20260131/
#   data/evaluation/<model_version>/20260228/    ← 新的
```

3. 步驟 2 印出的 `base_dataset_version` 與 `train_variant_id` 都跟你動手前抄下來的一樣。

4. promote 的比較範圍沒變：

```bash
python scripts/promote_model.py --env production --dry-run
```

`Scored months:` 還是舊月份，`Ranked` 底下的版本跟動手前一樣。畫面每一行的意思見 [promote 一個版本](promoting-a-model.md)。

舊月份的 `report.html` 修改時間應該停在它自己那次執行。被更新了代表你不小心對舊月份也跑了一次 evaluation——內容一樣，無害，但時間戳被蓋掉了。例外是步驟 3 因為預測格式版本重寫了舊月份：那時舊月份的報表本來就該重跑，內容會不同。

## 出錯了怎麼辦

| 你看到什麼 | 怎麼辦 |
|---|---|
| 步驟 2 開跑前就退出，訊息帶 `(A55)` 與 `train sampling settings changed` | 這次還改到了抽樣設定。還原它，或不帶 `--only-test-months` 跑完整的一輪（步驟 2） |
| 步驟 2 開跑前就退出，訊息帶 `(A55)` 與 `never been built` | 這個 `base_dataset_version` 沒建過訓練資料：第一次跑、改到了 base 層設定或特徵表 schema，或框架升級。見步驟 2 |
| 步驟 2 跑完了，但 log 最後有兩行 `[train_variant] 目前設定的 train 版本...還沒建` | 目前的 variant 少了一張 train 表，`latest` 沒動。不要跑步驟 3，先不帶旗標跑完整的一輪（步驟 2 最後一段） |
| `base_dataset_version` 或 `train_variant_id` 跟你抄下來的不一樣，但沒被擋下 | 這次還改到了別的設定，而且剛好對到一個以前建過的版本。把那些改動還原，只留新增的月份 |
| `FileNotFoundError`，路徑帶著新月份 | 步驟 2 沒跑成功。回去重跑步驟 2 |
| `No predictions found for evaluation.snap_date` | 步驟 3 沒跑。回去重跑步驟 3 |
| 步驟 3 把舊月份也重寫了，log 有 `recorded in prediction format ..., not ...` | 框架升級改了評分程式（training 預測格式版本加了 1）。預期的，只有這一次；模型沒重訓（步驟 3） |
| `test month '<月份>' ... has no rows in the test cache` | 這個月在設定裡但 dataset 還沒產出它。先跑步驟 2 |
| 訊息帶 `(A24) ... name the same calendar day` | 這個月份已經在 train／val 其中一組裡了。從不該擁有它的那一組移除 |
| promote `--dry-run` 把每個版本都列在 `Not ranked`，原因是 `scored on [...], not on the current scored months [...]` | 加月份之前沒固定計分月份。把 `test_metrics.snap_date` 寫成原本的月份（步驟 1 開頭） |
| 訊息帶 `(A53) test_metrics.snap_date month(s) ... are not in dataset.test_snap_dates` | 計分月份裡有月份不在 `dataset.test_snap_dates` 裡，或寫法不同。照 `dataset.test_snap_dates` 的寫法抄 |
| 訊息帶 `(A26) ... spells one month more than one way` | 同一個月在 `dataset.test_snap_dates` 裡出現了兩種寫法（例如 `2026-01-31` 與 `20260131`）。只留 `YYYY-MM-DD` 那一種，刪掉其餘 |
| 訊息帶 `(A22) evaluation.snap_date=... is not a test month` | 步驟 4 的日期不在 `dataset.test_snap_dates` 裡。漏做了步驟 1，補做步驟 1–3 |
| dataset 或 training 的訊息帶 `--rebuild-dates`，還沒起 Spark 就退出 | 你要重算的月份不在 `dataset.test_snap_dates` 裡。先把它加進去 |
| evaluation 的訊息帶 `(A21)` 與 `popularity_period_counts`，還沒起 Spark 就退出 | 這次的 evaluation 不重數 baseline（沒設 `baseline.score: rate`、關了 `report.sections.baseline`，或沒帶 `--post-training`），用不到這個旗標，拿掉 |
| evaluation 的訊息帶 `(A21)` 與 `outside every lookback`，還沒起 Spark 就退出 | 點名的月份不在這次評估月份的回看範圍裡（例如評估月份自己），把它從旗標拿掉（〈[上游回補了，要重算某個月份](#上游回補了要重算某個月份)〉） |
| 重算之後數字跟上次逐位相同 | 只重算了其中一層。dataset 與 training 兩行都要帶 `--rebuild-dates`（〈[上游回補了，要重算某個月份](#上游回補了要重算某個月份)〉） |
| `[rebuild] WARNING: ... had no effect` 或 `... is only half applied` | 你選的步驟範圍把預測那一步排除掉了。前者是一步都沒選到、旗標完全無事可做；後者是選到了「丟掉舊 cache」那一步、但沒選到重新預測那一步，所以 cache 重建了、預測沒重做。兩者都改用 `--only-node predict_and_write_test_predictions`。evaluation 也會印 `had no effect`，那是切片沒選到重數 baseline 的那一步，照警告下一行改用 `--from-node build_popularity_period_counts` |

## 相關文件

- [dataset pipeline](../../pipelines/dataset.md) —— 版本號怎麼算出來的、哪些設定會讓它翻號
- [evaluation pipeline](../../pipelines/evaluation.md) —— 報表內容、指標定義、比較模式
- [pipeline 切片](pipeline-slicing.md) —— `--only-node`／`--from-node`／`--dry-run` 的完整用法
- [promote 一個版本](promoting-a-model.md) —— promote 怎麼挑版本、為什麼只比計分月份相同的版本
- [known-pitfalls](../known-pitfalls.md) —— 重算時的資料新鮮度陷阱
