# 新增一個評估月份

想知道模型在**新的一個月**上表現如何時，把該月份加進 `dataset.test_snap_dates`。

`base_dataset_version` 與 `model_version` 都**不會改變**，因此**不需要重訓**：新月份的評估報表與既有月份並存於同一個模型身分底下，可以直接互相比較。理由（為什麼 test 日期可以退出版本身分，而 `val` / `calibration` 不行）見 [ADR-0001](../adr/0001-test-dates-out-of-dataset-version-identity.md)；版本語意見 [dataset.md §7.1](../pipelines/dataset.md)。

## 開始前確認

1. 新月份的 `feature_table` 與 `sample_pool` 已就緒，且 `label_table` 的觀察窗已成熟。**沒有任何閘門會擋下未成熟的 label**（consistency 的資料閘 B1 只驗 item 集合關係，不驗 label 數量），指標會安靜地偏低；完全沒有 label 時該月的正例為零，報表數字沒有意義。
2. 手上有目前的 `model_version`（training 執行時印出，或查 `data/models/<model_version>/manifest.json`）。**動手前先把目前的 `base_dataset_version` 與 `model_version` 抄下來**——驗收要拿它們比對。
3. 你要做的是**新增**月份。若是**重算既有月份**（上游回補、修了 preprocessing），先讀 [known-pitfalls §15](known-pitfalls.md)：本機 parquet cache 對既有月份只看 `_SUCCESS`、不看新鮮度，得先刪掉該月的 cache 目錄。

## 四個步驟

以下指令是本機（`--env local`）形式；其他環境只換 `--env`。所有指令都從 repo 或 worktree root 執行，且 `SPARK_CONF_DIR` 必須在**同一個 shell** 內對步驟 2–4 都有效（每開一個新 shell 就要重設一次，未設會直接失敗）。

### 1. 設定：加上新月份

`conf/base/parameters_dataset.yaml`：

```yaml
dataset:
  test_snap_dates:
    - "2026-01-31"
    - "2026-02-28"   # 新增
```

語意是**累積**：舊月份留著，不是換掉。移除某個月份只是讓它不再被處理，既有產物與報表不會被刪。

### 2. dataset：補上新月份的產物

```bash
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python -m recsys_tfb dataset --env local
```

log 印出的 `base_dataset_version` 應與上次**完全相同**。翻號了就停下來——代表你同時改到了其他設定（對照 [dataset.md §7.2 設定版本矩陣](../pipelines/dataset.md)）。

這一步只處理**尚未落地**的月份，既有月份的 partition 完全不動，所以加第 N 個月的成本正比於「新月份」而不是累積的總月份數。log 會明講它做了什麼、沒做什麼：

```
[months] test branch: processed=2026-02-28 skipped=2026-01-31 rebuild=-
```

各個 node 另有自己的一行（`[months] dataset=test_keys …`）。同一份資訊也寫進
`data/dataset/<base_dataset_version>/manifest.json` 的 `test_snap_dates_plan`，事後追溯不必去翻 Hive partition。

**這裡有一個必須知道的代價**：跳過的判準是「partition 存在」，不是「partition 新鮮」。`feature_table` 對某個舊月份回補資料之後，該月**不會**自動更新，而且不報錯。要重算舊月份請走下一節，理由見 [ADR-0002](../adr/0002-preprocessed-feature-table-incremental.md)。

### 3. predict：用既有模型對新月份產生預測（不重訓）

```bash
PYTHONPATH=src .venv/bin/python -m recsys_tfb training --env local \
  --only-node predict_and_write_test_predictions
```

**用 `--only-node`，不要用 `--from-node`。** `--from-node cache_test_model_input` 會把該 node 之後的所有下游一起拉進來（實測 21 of 21 nodes，含 `tune_hyperparameters`）＝ 重訓。`--only-node` 只跑 predict，自動擴張補上兩個記憶體輸入（`cache_test_model_input`、`select_features`），模型從既有 `model_version` 目錄讀 —— 實測 **3 of 21 nodes**。跑之前可加 `--dry-run` 看計畫確認。

這一步同樣**只做新月份**：已經預測完整的月份會被跳過。log 兩層各講一次——cache 層對每個月份各印一行（既有月份 `cache_hit name=test_model_input path=.../test_months/20260131/...`，新月份 `cache_miss ...` 並實際複製），predict 層印它對每個設定月份的決定：

```
[months] predict: processed=2026-02-28 skipped=2026-01-31 rebuilt=-
```

同一份資訊也在 `predict_manifest` 的 `months_processed` / `months_skipped` / `months_rebuilt` 三份清單裡。

**能跳過的理由**：`(model_version, snap_date)` 的預測是不可變產物——`model_version` 已經把定義模型的一切雜湊進去，同一個模型對同一份該月 model_input 的預測必然逐位元相同，重算不會得到不同的數字。**判準是「該月已寫出的 item partition 集合 ＝ 該月 cache 中出現的 distinct item」**，不是「有 partition 就算數」：後者會把寫到一半就中斷的月份永久當成完成，也看不出該月的 model_input 多了一個 item。

所以 **predict 寫到一半掛掉，重跑會自動補完**。但「上游新增了一個產品」不會自動被看到——判準比的是**這一層 cache 裡有什麼**，而該月的 cache 是回補前複製的，裡面同樣沒有新產品，兩邊一樣就是「完成」。要讓新產品進到舊月份，走下一節的重算動線（dataset 重算該月 → cache 重新複製 → predict 這時才看得到多出來的 item）。

**它的對價**跟步驟 2 一樣：存在 ≠ 新鮮。上游回補舊月份之後要用 `--rebuild-dates` 指名重算（見下一節）。

新月份的 `cache_miss` 之後若拋 `FileNotFoundError`，是步驟 2 沒跑或沒成功——來源表沒有該月份，逐月複製的 glob 零命中。這是機制自帶的 fail-loud，不是 bug。

### 4. evaluation：產出該月的報表

`conf/base/parameters_evaluation.yaml` 把日期指到新月份（evaluation 沒有對應的 CLI 旗標）：

```yaml
evaluation:
  snap_date: "2026-02-28"
```

```bash
PYTHONPATH=src .venv/bin/python -m recsys_tfb evaluation --env local \
  --post-training --model-version <model_version>
```

`--post-training` 讀 training 產出的 `training_eval_predictions`；`--model-version` 是必要的，否則會去解析需要人工 promote 的 `best` symlink（見 [known-pitfalls §9](known-pitfalls.md)）。

## 重算某個既有月份（上游回補時）

上游對某個已經跑過的月份回補或修正了 `feature_table`／`label_table` 之後，只加月份的動線幫不了你——那個月在兩層都已經是「完成」狀態：dataset 的 partition 存在（步驟 2 跳過它），predict 的 item partition 也齊全（步驟 3 跳過它）。**兩層都要指名重算，只做一層數字不會動。**

一條指令做完兩層：

```bash
bash scripts/rebuild_eval_month.sh 2026-01-31            # 多個月份用逗號分隔
bash scripts/rebuild_eval_month.sh 2026-01-31 --env local  # 預設就是 local
```

它就是把同一個 `--rebuild-dates` 轉送給 dataset 與 training 的 predict 切片（想自己下也可以）：

```bash
PYTHONPATH=src .venv/bin/python -m recsys_tfb dataset --env local \
  --rebuild-dates 2026-01-31
PYTHONPATH=src .venv/bin/python -m recsys_tfb training --env local \
  --only-node predict_and_write_test_predictions --rebuild-dates 2026-01-31
```

```
[months] test branch: processed=2026-01-31 skipped=2026-02-28 rebuild=2026-01-31
[months] predict: processed=2026-01-31 skipped=2026-02-28 rebuilt=2026-01-31
```

兩行的最後一欄拼法不同是刻意的：dataset 的 `rebuild=` 是**你要求了什麼**（旗標原值），predict 的 `rebuilt=` 是**它實際重做了哪些月**（本來會被跳過、因旗標而重做的那些）。要一次抓兩行就 grep `[months]`。

四件要知道的事：

1. **值必須是 `dataset.test_snap_dates` 的子集**，兩個 pipeline 共用同一條檢查，否則在 Spark 起來之前就報錯退出（一致性不變量 A21）。這條之所以要 fail loud：pipeline 從來不處理設定沒列的月份，靜默放行等於讓你以為重算過了。
2. **dataset 重算的是整條 test 鏈**（前處理編碼 → test keys → test model input），不是只有最後一段。
3. **training 帶這個旗標時會順手丟掉該月的本機 parquet cache**（log 印 `cache_rebuild name=test_model_input path=...`）再重新複製。cache 命中只看 `_SUCCESS`、不看新鮮度，不丟就會拿回補前的舊資料重算出逐位元相同的數字——見 [known-pitfalls §15](known-pitfalls.md)。**只有被指名的月份會被丟**，其他月份仍是 `cache_hit`。
4. **與切片旗標併用是正常的**（步驟 3 本來就是 `--only-node`）。只有當切片把 `predict_and_write_test_predictions` 排除在外、旗標因此完全沒作用時，training 才會印 `[rebuild] WARNING`。dataset 那側的規則不同（併用一律 WARN，因為未選中的上游 partition 會留在舊狀態）。

重算之後回到步驟 4 重跑 evaluation，該月的報表才會更新。

## 驗收

三件事都成立才算完成：

```bash
# 1. 版本沒變：這次 dataset 印出的 base_dataset_version 與加月份前相同
#    （步驟 2 的 log；training 那步印出的 model_version 同理）

# 2. 沒有多出一個模型
ls data/models/                       # 不應**多出**新的 model_version 目錄
                                      #（既有的其他版本、best symlink、_hpo 本來就會在）

# 3. 報表累積：新舊月份並存於同一個 model_version 底下
ls -d data/evaluation/<model_version>/*/
# → data/evaluation/<model_version>/20260131/
#   data/evaluation/<model_version>/20260228/
```

舊月份的 `report.html` 修改時間應停留在它自己那次執行——被重寫代表你不小心對舊月份也跑了一次 evaluation（覆寫同路徑，內容相同，無害但會蓋掉時間戳）。

> `data/models/<model_version>/manifest.json` 的時間戳**會**被步驟 3 更新（切片執行照樣寫 manifest，並記錄 `only_node`）。那是正常的，不是「模型被動到了」。

Hive 端可另外確認新月份的 partition 已寫入：

```sql
SHOW PARTITIONS ml_recsys.recsys_prod_test_model_input;
SHOW PARTITIONS ml_recsys.training_eval_predictions;   -- 這張沒有 recsys_prod_ 前綴
```

## 常見狀況

| 症狀 | 原因 | 處理 |
|---|---|---|
| dataset 印出的 `base_dataset_version` 翻號了 | 同時改到了其他 dataset 設定 | 對照 [dataset.md §7.2](../pipelines/dataset.md) 找出改到哪個 key；只加月份不會翻號 |
| predict 拋 `FileNotFoundError`，路徑帶著新月份 | 步驟 2 沒跑或沒成功，來源表沒有該月 | 回去跑 dataset，再重跑步驟 3 |
| evaluation 報 `No predictions found for evaluation.snap_date` | 步驟 3 沒跑，該月沒有預測 | 回去跑步驟 3 |
| 該月數字與上次逐位相同（重算既有月份時） | 只重算了其中一層：dataset 跳過既有 partition，或 predict 跳過已完整的月份／命中舊 parquet cache | 用 `bash scripts/rebuild_eval_month.sh <該月>` 一次做完兩層；細節見 [known-pitfalls §15](known-pitfalls.md) |
| dataset log 印 `skipped=<你要重算的月份>`，或 predict 印 `skipped=<該月>` | 沒帶 `--rebuild-dates`；產物存在就會被跳過 | 加上 `--rebuild-dates <該月>` 重跑（兩個 pipeline 都要） |
| `--rebuild-dates` 直接報錯退出、還沒起 Spark | 該月份不在 `test_snap_dates`（不變量 A21） | 先把月份加進 `dataset.test_snap_dates`，或修正旗標的值 |
| dataset 報 `(A24) dataset.X_snap_dates [...] and dataset.Y_snap_dates [...] name the same calendar day`、還沒起 Spark | 步驟 1 加的月份已經在 train／calibration／val 裡了（不變量 A24） | 從不該擁有它的那一組移除。訊息會分別印出兩邊**各自的原始寫法**，因為比對是按日、不是按字面（月份仍必須寫成 `YYYY-MM-DD`，其他寫法會被 A21／A22 擋下）。這條之所以要擋：同一個月同時訓練又評估，該月的指標會靜默變成 in-sample 數字，報表看起來完全正常 |
| evaluation 報 `(A22) evaluation.snap_date=... is not a test month`、還沒起 Spark | 步驟 4 的 `evaluation.snap_date` 指到不在 `test_snap_dates` 的月份（漏了步驟 1） | 補做步驟 1–3，或把 `evaluation.snap_date` 指回已設定的月份。這條之所以要擋：`training_eval_predictions` 累積歷來每一個月，已移除的月份照樣抓得到 rows，會跑出一份看起來正常、卻不是目前設定要評估的報表 |
| training 印 `[rebuild] WARNING: ... had no effect` | 切片把 `predict_and_write_test_predictions` 排除了，旗標無事可做 | 改用 `--only-node predict_and_write_test_predictions`，或不帶切片旗標 |
| predict 拋 `test month '<月份>' ... has no rows in the test cache` | 該月在 `test_snap_dates` 裡但 dataset 沒產出它 | 先跑步驟 2 產出該月。predict 之所以直接報錯而不安靜跳過：跳過會讓你拿到一份空報表 |

## 相關文件

- [ADR-0001：`test_snap_dates` 退出 dataset 版本身分](../adr/0001-test-dates-out-of-dataset-version-identity.md)
- [dataset pipeline](../pipelines/dataset.md) §7 版本、重跑與恢復
- [evaluation pipeline](../pipelines/evaluation.md) §7.2 設定與重跑矩陣
- [pipeline 切片](pipeline-slicing.md)
