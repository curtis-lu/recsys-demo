# 新增一個評估月份

想知道**現有的模型**在**新的一個月**上表現如何。

模型不會被重訓，也不會產生新版本。新月份的報表會跟既有月份並排存在同一個模型底下，可以直接互相比較。

**先確認你屬於哪一種：**

- 這個月份**沒跑過**，你要第一次評估它 → 往下讀。
- 這個月份**跑過了**，但上游補了資料，你要重算 → 跳到〈[上游回補了，要重算某個月份](#上游回補了要重算某個月份)〉。本文的四個步驟幫不了你，因為每一步都會判定這個月「已經做完」而直接跳過。

下面的指令都用 `--env <你的環境>`。本機執行請先照 [local-spark-setup.md](../dev-setup/local-spark-setup.md) 設好環境，並把 `--env` 換成 `local`。

## 整件事長什麼樣

```
 步驟 1          步驟 2            步驟 3              步驟 4
 改設定    →     dataset     →     training      →     evaluation
                （造資料）        （舊模型預測）      （出報表）
   │               │                  │                  │
   ▼               ▼                  ▼                  ▼
 test_snap_      新月份的           新月份的            report.html
 dates           model_input        預測值
 加一行

 ┌───────────────────────────────────────────────────────────┐
 │  模型全程沒有被重訓 —— model_version 從頭到尾都是同一個   │
 └───────────────────────────────────────────────────────────┘
```

## 動手前先確認

**① 新月份的來源資料齊了。**

特徵表跟母體表沒齊的話，pipeline 會直接報錯，你會知道。

**標籤沒齊則不會報錯**——那些候選會被當成負例，指標安靜地偏低，報表看起來完全正常。標籤的觀察窗有沒有結束，只有你自己知道，框架不會擋你。

**② `dataset.calibration_snap_dates` 不是空清單。**

空清單不代表「不做校準」。它會讓校準集收進母體裡的**每一個**月份——包含你剛加的新月份。那個月的評估因此變成模型「先看過答案」的分數，會虛高。沒有任何檢查會擋你。

**③ 抄下目前的兩個版本號。** 跑完要拿它們比對。

```bash
ls -1 data/models/          # 目錄名就是 model_version
```

`base_dataset_version` 在上一次 dataset 執行的 log 裡（`base_dataset_version: …`），也可以從 `data/dataset/` 底下的目錄名讀到。

## 步驟 1：把新月份加進設定

編輯 `conf/base/parameters_dataset.yaml`：

```yaml
dataset:
  test_snap_dates:
    - "2026-01-31"
    - "2026-02-28"   # 新增這一行
```

**舊月份要留著**，這是累積清單不是替換清單。把某個月份刪掉只是讓它以後不再被處理，已經產出的資料跟報表不會消失。

## 步驟 2：產出新月份的資料

```bash
python -m recsys_tfb dataset \
  --env <你的環境> --only-test-months
```

`--only-test-months` 告訴 pipeline「這次只是加評估月份」，於是它跳過訓練資料那一整段的重算。**只有當你這次除了加月份還改了別的設定時才不要帶它**（改了抽樣、特徵、訓練月份……那些改動需要被跳過的部分重算才會生效）。不確定就不要帶，跑完整的一輪永遠是安全的那一邊。

**成功的話**，log 裡會有這幾行：

```
base_dataset_version: <跟你抄下來的一模一樣>
[plan] only-test-months: 5 of the dataset pipeline's 15 nodes; ...
[months] dataset=test_keys         processed=2026-02-28 skipped=2026-01-31
[months] dataset=test_model_input  processed=2026-02-28 skipped=2026-01-31
```

`processed` 是新月份、`skipped` 是舊月份，就對了。（另外還有一行 `dataset=preprocessed_feature_table`，它涵蓋的月份範圍更廣，訓練月份也算在內，數字跟上面兩行不一樣是正常的。）

**最常壞的一種**：`base_dataset_version` 跟你抄下來的不一樣。

代表你這次不只加月份，還改到了別的設定。**停下來**，不要跑步驟 3——模型會對不上新的資料版本。把其他改動還原，只留新增的月份。

## 步驟 3：用現有模型對新月份產生預測

```bash
python -m recsys_tfb training \
  --env <你的環境> --only-node predict_and_write_test_predictions
```

**一定要用 `--only-node`。** 換成 `--from-node` 會把下游全部拉進來，包含超參數搜尋——那是重訓，不是預測，會產生一個新的 `model_version`，這份文件的前提就不成立了。

不放心可以先加 `--dry-run` 看它打算跑哪些步驟再決定。

**成功的話**，log 裡會有：

```
[months] predict: processed=2026-02-28 skipped=2026-01-31 rebuilt=-
```

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
  --env <你的環境> --post-training --model-version <你的 model_version>
```

`--post-training` 讓它讀步驟 3 產生的預測。

**成功的話**，這個檔案會出現：

```
data/evaluation/<model_version>/20260228/report.html
```

**最常壞的一種**：忘了 `--model-version`。

省略它時 evaluation 會去找 `best` 這個標記，那指向的是已經正式核准上線的模型——不見得是你這次要評估的那一個。你會拿到一份看起來正常、但評的是別的模型的報表。

## 上游回補了，要重算某個月份

上游對一個**已經跑過**的月份補了或修了資料之後，重跑上面的四個步驟沒有用：那個月在 dataset 跟 predict 兩層都已經是「做完」狀態，兩層都會跳過它。

**兩層都要指名重算，只做一層數字不會動。** 一行指令做完兩層：

```bash
bash scripts/rebuild_eval_month.sh 2026-01-31 --env <你的環境>
# 多個月份用逗號分隔：2026-01-31,2026-02-28
```

`--env` **不要省略**：省略時這個腳本預設走 `local`，在其他環境會安靜地跑錯地方。

然後回到**步驟 4** 重跑 evaluation，該月的報表才會更新。

兩件事要知道：

1. **月份必須已經在 `dataset.test_snap_dates` 裡。** 不在的話會在 Spark 起來之前就報錯退出。
2. **只有你指名的月份會被重算**，其他月份不受影響。

## 跑完了，怎麼確認真的成功

三件事都對才算完成：

```bash
# 1. 沒多出新模型 —— 這是「沒有重訓」的證據
ls -1 data/models/
#    跟你動手前抄下來的清單一樣，沒有新目錄

# 2. 新舊報表並存在同一個模型底下
ls -d data/evaluation/<model_version>/*/
# → data/evaluation/<model_version>/20260131/
#   data/evaluation/<model_version>/20260228/    ← 新的
```

3. 步驟 2 印出的 `base_dataset_version` 跟你動手前抄下來的一樣。

舊月份的 `report.html` 修改時間應該停在它自己那次執行。被更新了代表你不小心對舊月份也跑了一次 evaluation——內容一樣，無害，但時間戳被蓋掉了。

## 出錯了怎麼辦

| 你看到什麼 | 怎麼辦 |
|---|---|
| `base_dataset_version` 跟你抄下來的不一樣 | 這次還改到了別的設定。把那些改動還原，只留新增的月份 |
| `FileNotFoundError`，路徑帶著新月份 | 步驟 2 沒跑成功。回去重跑步驟 2 |
| `No predictions found for evaluation.snap_date` | 步驟 3 沒跑。回去重跑步驟 3 |
| `test month '<月份>' ... has no rows in the test cache` | 這個月在設定裡但 dataset 還沒產出它。先跑步驟 2 |
| 訊息帶 `(A24) ... name the same calendar day` | 這個月份已經在 train／val／calibration 其中一組裡了。從不該擁有它的那一組移除 |
| 訊息帶 `(A26) ... spells one month more than one way` | 同一個月在 `dataset.test_snap_dates` 裡出現了兩種寫法（例如 `2026-01-31` 與 `20260131`）。只留 `YYYY-MM-DD` 那一種，刪掉其餘 |
| 訊息帶 `(A22) evaluation.snap_date=... is not a test month` | 步驟 4 的日期不在 `dataset.test_snap_dates` 裡。漏做了步驟 1，補做步驟 1–3 |
| 訊息帶 `--rebuild-dates`，還沒起 Spark 就退出 | 你要重算的月份不在 `dataset.test_snap_dates` 裡。先把它加進去 |
| 重算之後數字跟上次逐位相同 | 只重算了其中一層。改用 `bash scripts/rebuild_eval_month.sh <月份>`，它一次做完兩層 |
| `[rebuild] WARNING: ... had no effect` 或 `... is only half applied` | 你選的步驟範圍把預測那一步排除掉了。前者是一步都沒選到、旗標完全無事可做；後者是選到了「丟掉舊 cache」那一步、但沒選到重新預測那一步，所以 cache 重建了、預測沒重做。兩者都改用 `--only-node predict_and_write_test_predictions` |

## 相關文件

- [dataset pipeline](../../pipelines/dataset.md) —— 版本號怎麼算出來的、哪些設定會讓它翻號
- [evaluation pipeline](../../pipelines/evaluation.md) —— 報表內容、指標定義、比較模式
- [pipeline 切片](pipeline-slicing.md) —— `--only-node`／`--from-node`／`--dry-run` 的完整用法
- [known-pitfalls](../known-pitfalls.md) —— 重算時的資料新鮮度陷阱
