# 替舊版本補分數

promote 的 `--dry-run` 把某個版本列在 `Not ranked`，你想讓它重新參加排名。

做法是替它在**現在的計分月份**上重算分數。計分月份是 training 在 test 上算分數用的那幾個月（`test_metrics.snap_date`，沒寫就是整份 `dataset.test_snap_dates`）；promote 只拿「在現在的計分月份上算的分數」互相比。模型不會被重訓，也不會產生新版本：用的是那個版本已經訓練好的模型，只補預測、重算 `evaluation_results.json`。

**先確認你屬於哪一種：**

- 這個版本就是**現在的設定**算出來的那一個（沒改過 `training:`、也沒換過 dataset 版本）→ 只要做步驟 3、4，而且不用帶 `--base-dataset-version`、`--train-variant` 這兩個旗標。加了評估月份後想讓 promote 用新月份比，就是這種，見 [新增一個評估月份](adding-an-eval-month.md#要讓-promote-用新月份比)。
- promote 列的原因是 `no value for <指標>: <一段原因>`（冒號後面有 training 記下的原因）→ 先看〈[補不回來的情況](#補不回來的情況)〉。
- 其他情況（`records no scored months`、`scored on [...], not on the current scored months`、`no value for <指標> (it has [...])`）→ 往下讀。

下面的指令都用 `--env <你的環境>`。本機執行請先照 [local-spark-setup.md](../dev-setup/local-spark-setup.md) 設好環境，並把 `--env` 換成 `local`。

## 為什麼這麼麻煩

training 沒有 `--model-version` 這種旗標：`model_version` 是從設定算出來的，由三樣東西決定——`training:` 區塊、`base_dataset_version`、`train_variant_id`。要替一個舊版本重跑 training 的任何一步，就得讓這三樣東西暫時回到它當初的樣子，算出同一個 `model_version`，pipeline 才會去讀那個版本的模型。

所以這份手冊大部分在做一件事：**暫時把設定還原成那個版本的，做完再改回來**。每一步都有一行 log 讓你確認有沒有還原對。

## 整件事長什麼樣

```
 步驟 1            步驟 2              步驟 3           步驟 4          步驟 5
 還原 training:    （需要時）          預測缺的         重算分數   →    設定改回來、
 寫死選版指標  →   補造計分月份  →     月份       →                    latest 指回來
                   的資料
   │                 │                   │                │               │
   ▼                 ▼                   ▼                ▼               ▼
 Model version:    舊 dataset 版本     舊版本在新        evaluation_     promote 的
 <舊版本>          多了新月份          月份的預測        results.json    Ranked 有它

 ┌───────────────────────────────────────────────────────────────────────────┐
 │  模型全程沒有被重訓 —— 每一步的 log 都要印出同一個 Model version：<舊版本> │
 └───────────────────────────────────────────────────────────────────────────┘
```

## 動手前先確認

下面每一項都把要記的東西存進 `rescore-backup/`，最後拿來比對，不必抄在別處。

**① 記下現在的選版指標與計分月份。**

```bash
mkdir -p rescore-backup
python scripts/promote_model.py --env <你的環境> --dry-run | tee rescore-backup/promote-before.txt
```

開頭三行：

```
=== Model Version Comparison ===
Selection metric: macro_per_item_map  (...)
Scored months:    ['2026-01-31', '2026-02-28']  (...)
```

`Selection metric` 是現在的**選版指標**（promote 拿來排名的那個指標），`Scored months` 是現在的計分月份。做完之後這三行要一模一樣，這是「設定有改回來」的證據。

**② 記下舊版本的兩個上游版本號。**

```bash
python -c "import json; m = json.load(open('data/models/<舊版本>/manifest.json')); print(m['base_dataset_version'], m['train_variant_id'])"
```

印出來的兩個值，下面叫 `<它的 dataset 版本>` 與 `<它的 train 抽樣版本>`。

**③ 記下現在 `latest` 指到哪。**

```bash
readlink data/dataset/latest | tee rescore-backup/latest-before.txt
```

步驟 2 會把它改掉，步驟 5 要改回這個值。

**④ 選版指標是二元預測類時，先確認補得回來。**

選版指標是 `pooled_average_precision` 或 `macro_per_item_average_precision` 時，要那個 dataset 版本的 test 留下了沒有正例的 query group：

```bash
python -c "import json; d = json.load(open('data/dataset/<它的 dataset 版本>/manifest.json'))['parameters']['dataset']; print(d.get('test_zero_positive_group_ratio'))"
```

印出 `None` 或 `0`：**補不回來**，停在這裡，原因見〈[補不回來的情況](#補不回來的情況)〉。選版指標是 `mean_ap` 或 `macro_per_item_map` 時不用看。

**⑤ 備份設定與那個版本的 manifest。**

```bash
cp -R conf rescore-backup/conf
cp data/models/<舊版本>/manifest.json rescore-backup/model-manifest.json
```

整個 `conf/` 都備份，步驟 5 一次還原、一次比對。

manifest 也要備份，因為步驟 3、4 會用這次執行的內容覆寫 `data/models/<舊版本>/manifest.json`：`created_at`、`git_commit`、`run_id` 換成這次的，並多一個 `only_node`。模型本身不變，但「這個版本當初是哪一天、用哪個 commit 訓練的」只剩這份備份記得，做完之後別刪它。

**下面的還原一律改 `conf/base/` 的檔案。** 印出來的 `training:`／`dataset:` 是當時 base 與環境層合併後的結果，已經是完整的一段。環境層（`conf/<你的環境>/`）只能蓋過 base 的鍵，刪不掉 base 裡多出來的鍵，所以沒辦法拿它來「整段換」。環境層的 `parameters_training.yaml`／`parameters_dataset.yaml` 若也寫了 `training:`／`dataset:` 底下的鍵，步驟 1、2 期間把那些鍵刪掉；步驟 5 會連同它們一起還原。

## 步驟 1：還原那個版本的 training 設定

印出它當初的 `training:` 區塊（manifest 記下了整段）：

```bash
python -c "import json, yaml; m = json.load(open('data/models/<舊版本>/manifest.json')); print(yaml.safe_dump({'training': m['parameters']['training']}, sort_keys=False, allow_unicode=True))"
```

打開 `conf/base/parameters_training.yaml`，做兩件事：

1. 把整個 `training:` 區塊換成印出來的那一段，**整段換**，不是只改你記得不一樣的幾個鍵。
2. 在 `test_metrics:` 底下把**選版指標寫死**成動手前 ① 記下的那一個：

```yaml
test_metrics:
  selection_metric: macro_per_item_map   # 換成 ① 記下的 Selection metric
```

**為什麼要寫死選版指標**：它沒寫的時候跟著 `training.hpo_objective` 走。你剛還原的 `training:` 帶著那個版本當初的 `hpo_objective`，可能跟現在的不一樣：

- 現在的選版指標是二元預測類時，重算出來的分數就沒有它的值，白做一趟。（`mean_ap`、`macro_per_item_map` 兩個排序類指標每次一律會算，所以選版指標是排序類時不會少值。）
- 更麻煩的是忘了改回來：promote 之後會用舊的選版指標排名，畫面上只有 `Selection metric:` 那一行看得出來。

`test_metrics.snap_date` 跟 `dataset.test_snap_dates` 不要動：計分月份要維持現在的。

**確認還原對了**——先不跑任何東西，只看版本號：

```bash
python -m recsys_tfb training --env <你的環境> \
  --base-dataset-version <它的 dataset 版本> --train-variant <它的 train 抽樣版本> \
  --dry-run
```

**成功的話**，log 裡會有：

```
Model version: <舊版本>
```

兩個版本旗標一定要帶：不帶的話 training 用 `data/dataset/latest`，那是現在設定的 dataset 版本，算出來的 `model_version` 一定不一樣。

**最常壞的一種**：`Model version` 不是 `<舊版本>`。

`training:` 沒有還原到一模一樣。常見原因：只改了部分鍵；或 `conf/<你的環境>/parameters_training.yaml` 還留著 `training:` 底下的鍵，蓋過了 base 的值（動手前 ⑤ 最後一段）。修到印出 `<舊版本>` 為止，**不要往下做**——版本號不同，後面每一步都會對著另一個（可能還不存在的）版本做事。

## 步驟 2：計分月份沒有資料時，補造它

先看那個 dataset 版本建過哪些 test 月份：

```bash
python -c "import json; d = json.load(open('data/dataset/<它的 dataset 版本>/manifest.json'))['parameters']['dataset']; print(d.get('test_snap_dates'))"
```

這是最後一次建這個 dataset 版本時的 `test_snap_dates`。動手前 ① 記下的 `Scored months` 每個月都在裡面 → **跳過這一步**。有月份不在裡面 → 往下做。

印出它當初的 `dataset:` 區塊：

```bash
python -c "import json, yaml; d = json.load(open('data/dataset/<它的 dataset 版本>/manifest.json')); print(yaml.safe_dump({'dataset': d['parameters']['dataset']}, sort_keys=False, allow_unicode=True))"
```

打開 `conf/base/parameters_dataset.yaml`，把整個 `dataset:` 區塊換成印出來的那一段，**但 `test_snap_dates` 換回現在的**（計分月份都要在裡面）：

```yaml
dataset:
  # ……其他鍵照印出來的……
  test_snap_dates:
    - "2026-01-31"
    - "2026-02-28"      # 現在的 test_snap_dates，不是印出來的那份
```

`test_snap_dates` 不參與 `base_dataset_version` 的計算，所以換掉它不會變成另一個 dataset 版本；留著舊的那份，這一步就什麼月份都不會補。

```bash
python -m recsys_tfb dataset --env <你的環境> --only-test-months
```

**成功的話**，log 裡會有：

```
base_dataset_version: <它的 dataset 版本>
train_variant_id:     <它的 train 抽樣版本>
[months] dataset=test_model_input processed=2026-02-28 skipped=2026-01-31
```

`processed` 是補造的月份。

**這一步的副作用**：dataset 跑完會把 `data/dataset/latest` 指到 `<它的 dataset 版本>`。這時候不帶旗標跑 training，會靜悄悄地用舊資料。步驟 5 會把它指回來，**在那之前不要跑別的 training**。

**最常壞的一種**：`base_dataset_version` 不是 `<它的 dataset 版本>`。

`dataset:` 沒有還原到一模一樣；或是 `dataset:` 以外、也算進這個版本號的東西變了——`schema` 設定，或 `feature_table` 的欄位與型別。前者修到一樣為止；後者現在的設定造不出那個 dataset 版本，見〈[補不回來的情況](#補不回來的情況)〉。不管哪一種，這一步已經建了一個你不要的 dataset 版本、而且 `latest` 指著它：做步驟 5 把 `latest` 指回來。

## 步驟 3：預測缺的月份

```bash
python -m recsys_tfb training --env <你的環境> \
  --base-dataset-version <它的 dataset 版本> --train-variant <它的 train 抽樣版本> \
  --only-node predict_and_write_test_predictions
```

**一定要用 `--only-node`。** 換成 `--from-node` 會把超參數搜尋之後的每一步都拉進來。

**成功的話**，log 裡會有：

```
Model version: <舊版本>
[months] predict: processed=2026-02-28 skipped=2026-01-31 rebuilt=-
```

`processed` 是這次預測的月份，`skipped` 是那個版本早就預測過的。每個計分月份都已經預測過時，`processed=-`，這是正常的。

## 步驟 4：重算分數

```bash
python -m recsys_tfb training --env <你的環境> \
  --base-dataset-version <它的 dataset 版本> --train-variant <它的 train 抽樣版本> \
  --only-node compute_test_metrics
```

**成功的話**，log 裡會有：

```
Model version: <舊版本>
compute_test_metrics: scoring ['mean_ap', 'macro_per_item_map'] on ['2026-01-31', '2026-02-28'] (selection metric macro_per_item_map, HPO objective mean_ap)
```

`on` 後面要是動手前 ① 記下的 `Scored months`，`selection metric` 後面要是 ① 記下的 `Selection metric`。`HPO objective` 是那個版本當初的，跟現在不一樣是正常的。

`data/models/<舊版本>/evaluation_results.json` 被覆寫。MLflow 裡的數字不會更新（那是 `log_experiment` 寫的）；promote 讀的是 `evaluation_results.json`，不受影響。

## 步驟 5：把設定改回來

```bash
cp -R rescore-backup/conf/. conf/
diff -r rescore-backup/conf conf
```

`diff -r` 沒有輸出，設定才算改回來了。

**做過步驟 2 的話**，再用現在的設定跑一次 dataset，讓 `latest` 指回現在的版本：

```bash
python -m recsys_tfb dataset --env <你的環境> --only-test-months
readlink data/dataset/latest | diff rescore-backup/latest-before.txt -
```

`--only-test-months` 只跑 test 月份那幾步。現在的 dataset 版本早就建好了，這些月份全部是 `skipped`，什麼都不重算，跑完照樣把 `latest` 指到這個版本。不帶這個旗標也可以，只是訓練資料那一段會整個重算一次。

log 的 `base_dataset_version:` 要是現在的版本，`diff` 沒有輸出。

## 跑完了，怎麼確認真的成功

```bash
python scripts/promote_model.py --env <你的環境> --dry-run | tee rescore-backup/promote-after.txt
head -3 rescore-backup/promote-after.txt | diff <(head -3 rescore-backup/promote-before.txt) -
```

三件事都對才算完成：

1. `<舊版本>` 列在 `Ranked` 底下。
2. 第二個指令沒有輸出：`Selection metric:` 與 `Scored months:` 跟動手前 ① 記下的一模一樣。有輸出代表設定沒改回來，promote 正在用別的選版指標或別的月份排名。
3. 步驟 5 的 `readlink … | diff …` 沒有輸出（沒做步驟 2 就不用看）。

```bash
ls -1 data/models/
```

也不該多出新目錄——多了代表某一步的 `Model version` 不是 `<舊版本>`，你訓練出了一個新版本。

## 補不回來的情況

有三種情況，用現在的程式讓這個版本重新參加排名是做不到的：

- **選版指標是二元預測類，而那個版本的 dataset 在 test 沒留沒有正例的 query group**（動手前 ④ 印出 `None` 或 `0`）。二元預測類的指標把 test 每一列都當成一題，沒留那些 query group，算出來的是另一個母體的數字，training 會拒絕算：寫死了這個選版指標，步驟 1 的 `--dry-run` 就會報錯（訊息帶 `A54:`）。promote 畫面上 `no value for <指標>: ...` 冒號後面的原因，就是 training 記下的這件事。
- **現在的設定造不出那個 dataset 版本**：步驟 2 的 `base_dataset_version` 在 `dataset:` 還原一模一樣之後仍然不同，代表 `schema` 或 `feature_table` 的欄位變過。那個版本缺的月份造不出來。
- **現在的程式不接受那個版本的 `training:`**：步驟 1 的 `--dry-run` 在印出版本號之前就報設定錯誤（例如裡面有已經被移除的鍵）。

這時候的選擇：讓所有版本改用一個這個版本有值的選版指標（把 `test_metrics.selection_metric` 明寫成排序類指標），或把計分月份縮回這個版本算過的月份（`test_metrics.snap_date`）；兩者都會改變**所有**版本的比較方式。要不然就是用現在的設定重新訓練。

## 出錯了怎麼辦

| 你看到什麼 | 怎麼辦 |
|---|---|
| 任何一步的 `Model version` 不是 `<舊版本>` | `training:` 沒還原對，或忘了帶 `--base-dataset-version`／`--train-variant`。回步驟 1 |
| 步驟 2 的 `base_dataset_version` 不是 `<它的 dataset 版本>` | 見步驟 2 的〈最常壞的一種〉。先做步驟 5 把 `latest` 指回來 |
| 步驟 4 報錯 `scored month(s) [...] have no rows in training_eval_predictions` | 那幾個月沒預測。回步驟 3；步驟 3 也說找不到資料，就是步驟 2 沒做 |
| 步驟 3 拋 `FileNotFoundError`（路徑帶著月份），或訊息帶 `has no rows in the test cache` | 那個 dataset 版本沒有這個月。做步驟 2 |
| 步驟 4 的 `selection metric` 不是 ① 記下的 | 步驟 1 沒寫死選版指標。補上後重跑步驟 4 |
| 報錯訊息帶 `A54:` | 選版指標是二元預測類，但那個版本的 test 算不出來。見〈補不回來的情況〉 |
| 做完之後 promote 的 `Selection metric:` 或 `Scored months:` 跟 ① 不一樣 | 設定沒改回來。重做步驟 5 的 `cp` 與 `diff -r` |
| 做完之後不帶旗標的 training 用了舊資料（log 的 `base_dataset_version` 不對） | 做過步驟 2 卻沒把 `latest` 指回來。做步驟 5 的 dataset 那一段 |

## 相關文件

- [promote 一個版本](promoting-a-model.md) —— promote 怎麼挑版本、畫面每一行的意思
- [新增一個評估月份](adding-an-eval-month.md) —— 加月份時怎麼不讓所有版本退出排名
- [training pipeline](../../pipelines/training.md) §3.7 —— `test_metrics` 各鍵、`evaluation_results.json` 記了什麼
- [pipeline 切片](pipeline-slicing.md) —— `--only-node`／`--dry-run` 的完整用法
