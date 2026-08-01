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

log 對每個月份各印一行：既有月份 `cache_hit name=test_model_input path=.../test_months/20260131/...`，新月份 `cache_miss ...` 並實際複製。

> predict 目前會對**所有**設定的月份重算預測（既有月份寫回相同 partition、內容相同）。逐月跳過已完成的月份是後續工作。

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

上游對某個已經跑過的月份回補或修正了 `feature_table`／`label_table` 之後，只加月份的動線幫不了你——那個月的 partition 已經存在，步驟 2 會跳過它。用 `--rebuild-dates` 指名要重算的月份：

```bash
PYTHONPATH=src .venv/bin/python -m recsys_tfb dataset --env local \
  --rebuild-dates 2026-01-31          # 多個月份用逗號分隔
```

```
[months] test branch: processed=2026-01-31 skipped=2026-02-28 rebuild=2026-01-31
```

四件要知道的事：

1. **值必須是 `dataset.test_snap_dates` 的子集**，否則在 Spark 起來之前就報錯退出（一致性不變量 A21）。這條之所以要 fail loud：pipeline 從來不處理設定沒列的月份，靜默放行等於讓你以為重算過了。
2. **重算的是整條 test 鏈**（前處理編碼 → test keys → test model input），不是只有最後一段。
3. **接著要刪該月的本機 cache 再跑 predict**，否則 predict 會命中舊 parquet——見 [known-pitfalls §15](known-pitfalls.md)。這是兩個獨立的快取層，dataset 重算不會通知 training 的 cache。
4. **與 `--from-node`／`--only-node` 併用會印 WARN**。兩者正交（切片選 node、rebuild 選月份）且併用是支援的，但只有被選中的 node 會重算，未選中的上游 partition 仍然是舊的。要整條鏈重算就別帶切片旗標。

重算之後回到步驟 3、4 重跑 predict 與 evaluation，該月的報表才會更新。

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
| 該月數字與上次逐位相同（重算既有月份時） | 兩層各有一個快取：dataset 跳過既有 partition、predict 命中舊 parquet cache | 依序排除：先用 `--rebuild-dates` 重算 dataset，再依 [known-pitfalls §15](known-pitfalls.md) 刪該月 cache 目錄 |
| dataset log 印 `skipped=<你要重算的月份>` | 沒帶 `--rebuild-dates`；partition 存在就會被跳過 | 加上 `--rebuild-dates <該月>` 重跑 |
| `--rebuild-dates` 直接報錯退出、還沒起 Spark | 該月份不在 `test_snap_dates`（不變量 A21） | 先把月份加進 `dataset.test_snap_dates`，或修正旗標的值 |

## 相關文件

- [ADR-0001：`test_snap_dates` 退出 dataset 版本身分](../adr/0001-test-dates-out-of-dataset-version-identity.md)
- [dataset pipeline](../pipelines/dataset.md) §7 版本、重跑與恢復
- [evaluation pipeline](../pipelines/evaluation.md) §7.2 設定與重跑矩陣
- [pipeline 切片](pipeline-slicing.md)
