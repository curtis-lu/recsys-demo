# promote 一個版本

**promote**＝把某個 `model_version` 設成預設版本：`data/models/best` 這個連結指向它。inference 與 evaluation 沒帶 `--model-version` 時用的就是它。training 從不自己 promote，一定由人執行 `scripts/promote_model.py`。

## 兩種用法

**你已經決定要哪一個版本**：

```bash
python scripts/promote_model.py <model_version>
```

直接把 `best` 指過去。不排名，也不讀設定。

**讓它照規則挑**：先看，再挑。

```bash
python scripts/promote_model.py --env <你的環境> --dry-run   # 只列出來，不動 best
python scripts/promote_model.py --env <你的環境>             # promote 列表上的 Recommended
```

`--env` 跟 pipeline 的一樣，指的是 `conf/<你的環境>/`，要從 repo 根目錄執行。不帶時是 `local`。不帶版本號時也會先印出同一張表，再 promote 表上的 `Recommended`。

## 它怎麼挑

版本之間要比，得用同一把尺、考同一份考卷。這兩樣都取自**執行當下的設定**，不取自各版本自己當初的設定：

- **尺**＝選版指標：`test_metrics.selection_metric`；沒寫就是實際生效的 HPO 目標（有寫 `training.hpo_objective` 就是它，沒寫是程式的預設 `mean_ap`）。
- **考卷**＝計分月份：`test_metrics.snap_date`；沒寫就是整份 `dataset.test_snap_dates`。

每個版本的 `data/models/<model_version>/evaluation_results.json` 記著它當初在哪幾個月份上算分數（`snap_dates`），以及算出來的每個指標（`metrics`）。一個版本要同時滿足兩件事才參加排名：

1. `metrics` 裡有這把尺的值。
2. `snap_dates` 跟考卷**完全一樣**。日期先統一寫法再比，所以 `20260131` 跟 `2026-01-31` 算同一天；但多一個月、少一個月都不算一樣。

分數高的排前面。`Recommended` 與不帶版本號時自動挑的，都只從參加排名的版本裡挑。

為什麼要求月份完全一樣：同一個模型在不同月份上的分數本來就會差，月份不同的兩個分數放在一起排，比的一部分是月份，不是模型。

## 畫面怎麼讀

```
=== Model Version Comparison ===
Selection metric: macro_per_item_map  (test_metrics.selection_metric not set: follows training.hpo_objective)
Scored months:    ['2026-01-31', '2026-02-28']  (test_metrics.snap_date not set: every dataset.test_snap_dates month)

Ranked (1):
  4810d84d  macro_per_item_map=0.5451

Not ranked (1):
  b8e313bd
    - its evaluation_results.json records no scored months (written before ADR-0028), so which months it was scored on is unknown
      fix: re-score it on the current scored months: docs/operations/user-guides/rescoring-an-old-version.md

Recommended: 4810d84d (macro_per_item_map=0.5451)
```

- `Selection metric`、`Scored months`：這次用的尺與考卷；括號裡是它們從哪個設定來的。
- `Ranked`：參加排名的版本，由高到低。
- `Not ranked`：沒參加的版本。每一條 `-` 是一個原因，底下的 `fix:` 是怎麼補。
- `(current best)`：現在 `best` 指著的版本，不管它在哪一區都會標。
- `Recommended: none`：沒有任何版本參加排名。這時不帶版本號的用法會報錯停下，不 promote 任何版本。

## 為什麼有版本沒參加排名

| 畫面上的原因 | 什麼意思 | 怎麼補 |
|---|---|---|
| `records no scored months (written before ADR-0028)` | 這個版本是在 training 開始記下計分月份之前算的分數，不知道它考的是哪幾個月 | [替舊版本補分數](rescoring-an-old-version.md) |
| `scored on [...], not on the current scored months [...]` | 它算分數的月份跟現在的考卷不同 | 同上。如果考卷變了只是因為有人把月份加進 `dataset.test_snap_dates`，改成把原本的月份寫進 `test_metrics.snap_date`，見 [新增一個評估月份](adding-an-eval-month.md) |
| `no value for <指標> (it has [...])` | 它當初沒算這把尺（尺是二元預測類、而它當初的設定沒要這個指標） | [替舊版本補分數](rescoring-an-old-version.md)，步驟 1 會把選版指標寫死 |
| `no value for <指標>: <一段原因>` | training 當初想算、但算不出來，冒號後面是它記下的原因 | 通常補不回來，見 [替舊版本補分數〈補不回來的情況〉](rescoring-an-old-version.md#補不回來的情況) |

同一個版本可以同時列好幾條原因。

## 會讓所有版本一起退出排名的三件事

尺與考卷跟著現在的設定走，所以改設定的當下，每個版本記下的東西可能一起對不上：

- **剛升級到會記計分月份的版本。** 之前算的 `evaluation_results.json` 都沒記月份，全部退出。要讓某個舊版本回來，就替它補分數。
- **把月份加進 `dataset.test_snap_dates`，而 `test_metrics.snap_date` 沒寫。** 考卷跟著多一個月，每個版本記下的都只有舊月份。只想多評估一個月，就先把原本的月份寫進 `test_metrics.snap_date`（[新增一個評估月份](adding-an-eval-month.md) 步驟 1）。
- **改了 `training.hpo_objective`，而 `test_metrics.selection_metric` 沒寫。** 尺跟著換。例如原本的版本都用 `macro_per_item_map` 訓練，某次實驗把 HPO 目標改成二元預測類：尺跟著換成那個二元預測類指標，原本的版本沒有它的值，全部退出，`Recommended` 只剩那次實驗的版本——不管它在排序上是不是比較差。實驗 HPO 目標時，先把 `test_metrics.selection_metric` 寫下來。

## 設定不合法時

讀設定時跑的檢查跟 pipeline 一樣：每個指令入口都跑的那一組，再加上 training 入口對月份與 `test_metrics` 的檢查。任何一條不過，就印出錯誤、結束，不挑任何版本。直接指定版本號的用法不讀設定，所以不受影響。

錯誤訊息開頭的代號是檢查的編號，跟挑版本最相關的幾個：

| 代號 | 擋下的是 |
|---|---|
| `A30` | `--env` 指的 `conf/<環境>/` 目錄不存在 |
| `A36` | `dataset.test_snap_dates` 沒寫或是空的：沒有計分月份 |
| `A26` | `dataset.test_snap_dates` 把同一個月寫成兩種寫法 |
| `A53` | `test_metrics` 區塊寫錯：計分月份是空清單、不在 `dataset.test_snap_dates` 裡、或同一個月兩種寫法；指標名不在登記表；打錯的鍵 |
| `A54` | 選版指標是二元預測類，但設定讓 test 不留沒有正例的 query group，這個指標在 test 上算不出來 |

每個代號的完整定義在 `src/recsys_tfb/core/consistency.py` 開頭的說明。

## 它看不出來的事

考卷只認月份，不認母體。兩個版本若建在不同的 `base_dataset_version` 上（item 清單、`sample_pool`、label 的定義、test 留下多少沒有正例的 query group 不同），同樣的月份也可能是不同的考卷，promote 照樣把它們並排。要比這種版本，先自己確認它們的母體可比。

## 相關文件

- [替舊版本補分數](rescoring-an-old-version.md)
- [新增一個評估月份](adding-an-eval-month.md)
- [training pipeline](../../pipelines/training.md) §3.7 —— `test_metrics` 各鍵、`evaluation_results.json` 記了什麼
- [ADR-0028](../../adr/0028-test-metrics-set-by-config.md) —— 為什麼尺與考卷取自現在的設定
