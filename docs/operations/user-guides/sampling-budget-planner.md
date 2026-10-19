# 用抽樣預算算出抽樣與權重設定

訓練環境塞得下的筆數有上限：抽樣之後，train 與 train_dev 加起來最多只能有這麼多筆。這個上限叫**抽樣預算**。

這頁講兩支腳本怎麼用：給它一個抽樣預算，它算出兩份設定，貼回 conf 就能跑。

- `dataset.sample_ratio_overrides`：每個抽樣格、正負兩類各自的**保留率**。
- `training.sample_weights`（連同 `training.sample_weight_keys`）：對應的**樣本權重**，把抽樣造成的偏差補回來。

詞彙（抽樣預算、抽樣格、分層欄、保留率、目標權重、樣本權重）照 [`CONTEXT.md`](../../../CONTEXT.md) 的「抽樣與權重」。每一步怎麼算，寫在 issue #525 的〈計算規格〉。

**跟 [`sampling_overrides_editor.py`](sampling-overrides-editor.md) 的分工：** 編輯器是讓你一格一格手調、看著數字改的視覺化工具；這兩支腳本是給一個預算、照一套固定的算法一次算完整套。兩者產出的 key 格式相同。

## 整件事長什麼樣

```
                  ① profile_sample_pool.py run              （Spark，掃表一次，分鐘級）
 sample_pool  ───────────────────────────────▶  profile.json
                                                    │
 抽樣預算＋旋鈕  ─────────────────────────────────────┤
                  ② sampling_budget_planner.py plan  │   （純 Python，秒級）
                                                    ▼
                                  sample_ratio_overrides.yaml   → 貼到 parameters_dataset.yaml
                                  sample_weights.yaml           → 貼到 parameters_training.yaml
                                  report.md                     → 看一眼，確認自我檢查全過
                                                    │
                  ③ 貼回 conf  →  ④ dataset  →  training
```

只有 ① 會掃表。改預算或旋鈕只要重跑 ②。

## 動手前先確認

**① `dataset.sample_group_keys` 要含 item 欄和 label 欄。** 工具替每個 item 的正、負兩類各算一個保留率，少了任何一個就無從分起，會直接擋下。

**② 分層欄都要能當權重的 key。** 分層欄就是 `sample_group_keys` 裡 item、label 以外的欄。工具會把 `training.sample_weight_keys` 設成跟 `sample_group_keys` 一樣，因為權重要補回抽樣的偏差，就得跟抽樣分得一樣細。而 train 的 model_input 只帶 identity 欄、label、`dataset.carry_columns` 與宣告過的類別欄，所以分層欄要列在 `carry_columns` 裡（time 這類 identity 欄本來就帶著，不用列）。如果特徵表也有這一欄，還要列進 `dataset.prepare_model_input.drop_columns`，否則 dataset 第一個 node 會擋下：訊息寫著這一欄 `is in dataset.carry_columns and is also a column of feature_table`，因為兩邊各帶一份同名欄沒辦法 join。分層欄不在這些欄裡的話，權重會安靜地維持 1.0，所以工具會先擋下，訊息開頭是 `A9a:`。

**③ `training.algorithm_params.objective` 是 `binary`。** 用權重把抽樣偏差補回來，只在 pointwise loss 下成立。ranking objective 是成對計算的，逐列抽樣也會拆散 query group。不是 `binary` 時工具照樣算，但會警告。

**④ `dataset.train_zero_positive_group_ratio` 不寫，或寫 1。** 它會在抽樣之後再整組丟掉沒有正樣本的 query group，丟掉的部分權重補不回來，預算也就對不上。寫了別的值，工具會警告。

## 步驟 1：數 sample_pool

```bash
PYTHONPATH=src .venv/bin/python scripts/profile_sample_pool.py run ml_recsys.sample_pool --env <env>
```

`ml_recsys.sample_pool` 是表名的例子，換成你的表；也可以給 parquet 路徑。

**設定的讀法跟 pipeline 一樣**：先讀 `conf/base/`，再把 `conf/<env>/` 疊上去。`<env>` 填你跑 pipeline 時給 `--env` 的那個值；不寫就是 `local`，跟 pipeline 一樣。`conf/` 底下沒有這個資料夾（例如打錯字）會直接報錯，不會只用 `conf/base/` 默默跑完。設定是從目前目錄的 `conf/` 讀的，所以要在 repo 根目錄下指令。步驟 2 和編輯器也是這樣讀設定。

**它數的是 dataset pipeline 抽樣時看到的那批列**，所以數出來的筆數就是預算要分配的母體：

- 只數 `dataset.train_snap_dates` 的月份。time 欄存成字串、date 或 timestamp 都數得到。
- item 由多欄組成時，先照 pipeline 的方式合併成一欄。
- entity 是 NULL 的列不數。pipeline 切 train／train_dev 時會丟掉這些列，它們不佔預算。

**產出：** `data/profiling/sample_pool_profile/profile.json`（要換位置用 `--out`）。每一格記一組欄值和正、負筆數，另外記下產生時的條件：依哪些欄分格、label 欄、item 欄、訓練月份、來源表、時間。

分格的欄是 `sample_group_keys` 與 `sample_weight_keys` 的聯集去掉 label，比預算工具需要的細；預算工具讀進來會自己加總到抽樣格。

**可能看到的警告：**

| 警告 | 意思 | 要不要處理 |
|---|---|---|
| `sample_weight_keys column(s) [...] are not in sample_pool` | 權重 key 裡有 sample_pool 沒有的欄（例如只在特徵表的類別欄），這一欄不分格 | 預算工具用不到它，不用處理 |
| `row(s) have a NULL in a key column` | 有列的分格欄或 label 是 NULL。這些列對不上任何保留率的 key，抽樣時用 `dataset.sample_ratio`、訓練時權重 1.0 | 要處理。它們不在預算裡，而且整群全留、權重可能比旁邊的列輕幾百倍。修來源表 |

**什麼時候要重跑：** 換了訓練月份、改了 `sample_group_keys`、或 sample_pool 的資料更新了。只改預算或旋鈕不用重跑。

## 步驟 2：算設定

```bash
PYTHONPATH=src .venv/bin/python scripts/sampling_budget_planner.py plan \
    data/profiling/sample_pool_profile/profile.json --budget 10000000 --env <env>
```

`--budget` 是 train 加 train_dev 的總筆數，必填；`10000000` 是例子。`--env` 給跟步驟 1 一樣的值。

### 旋鈕

| 旋鈕 | 預設 | 管什麼 |
|---|---|---|
| `--budget` | 必填 | 抽樣預算 |
| `--t` | 1/6 | 每個 item 加權後的正樣本率。冷門 item 的正樣本率被抬到這裡，熱門的被壓到這裡 |
| `--alpha` | 0.5 | 冷門 item 拿到多少注意力。0＝每個正樣本一樣重，熱門 item 主導 loss；1＝每個 item 的 loss 一樣大 |
| `--gamma` | 1.0 | 預算在格之間怎麼分。1＝每格留下的筆數跟它的價值成正比；0.5＝跟價值的平方根成正比，小格分到比較多。範圍 (0, 1] |
| `--eta` | 0.2 | 負樣本保底。每格的負樣本至少拿到該 item 平均保留率的這個比例，避免某格負樣本被抽得太少、權重太大。範圍 [0, 1] |
| `--pos-floor` | 0.6 | 正樣本保留率的下限。0＝不設下限。範圍 [0, 1] |
| `--weight-t`、`--weight-alpha` | 同 `--t`、`--alpha` | 只換權重、不換保留率，見〈只想換權重〉 |

輸出位置用 `--out-dir` 換，預設 `data/profiling/sampling_budget_plan/`。

### 產出三個檔

**`sample_ratio_overrides.yaml`**：每個抽樣格、正負兩類都寫出來，不留給 `dataset.sample_ratio` 補。key 照 `sample_group_keys` 的順序用 `|` 串接，label 那一段填 `0`（負）或 `1`（正）。例如 `sample_group_keys` 是 `[seg, prod, label]` 時，一格會寫成兩行：

```yaml
  sample_ratio_overrides:
    "seg_1|prod_A|0": 0.04217
    "seg_1|prod_A|1": 1.00000
```

每個保留率都對應整數個桶。pipeline 把列分進 100000 個 hash 桶，留下桶號小於 `int(保留率 × 100000)` 的列，所以保留率只能是 0.00001 的整數倍；每一類至少一個桶，不會整類消失。少數桶數會寫成六位小數，例如 7 個桶寫成 `0.000075`：寫成 `0.00007` 的話，電腦算 `0.00007 × 100000` 得到 6.999…，截斷成 6 個桶。多加半個桶，pipeline 讀到的桶數才跟工具算的一樣。

**`sample_weights.yaml`**：`sample_weight_keys`（設成跟 `sample_group_keys` 一樣）和 `sample_weights`，key 跟保留率那份逐字相同。

兩份檔的開頭都用註解記下這次的預算、profile 檔和旋鈕值，事後看得出是怎麼算出來的。保留率那份只記會影響保留率的旋鈕。

**`report.md`**：同樣的摘要也會印在終端。內容：

- **λ、k、期望筆數。** λ 是讓期望筆數剛好等於預算的那個門檻；k 是讓抽出來的列平均權重等於 1 的倍數（LightGBM 的 `min_sum_hessian_in_leaf` 這類參數是絕對量，權重的整體大小會影響模型）。期望筆數拆成正、負兩類，並列出跟預算的差，差距來自換成整數個桶。最後是實際抽出筆數的標準差：每一列各自擲一次骰，所以實際抽出的筆數會在期望筆數上下這麼多的範圍內浮動。
- **各 item 一張表：** 正樣本總數 P、留下的正樣本、正樣本保留 %、留下的負樣本、neg:pos、注意力 A、負樣本折數 v。
- **被限制卡住的格數：** 保留率碰到 1、碰到正樣本下限、只剩 1 個桶的各有幾類。
- **自我檢查**，四條都要是 `[PASS]`：同 item 同一類在各分層的保留率 × 權重相等；各 item 加權後的正樣本率等於 `--weight-t`；各 item 的 loss 佔比跟 P 的 (1 − `--weight-alpha`) 次方成正比；平均權重等於 1。檢查讀的是寫進檔的文字，保留率照 pipeline 的方式截斷成整數個桶。任何一條失敗，工具只寫報告、不寫兩份設定檔。

報告可能多出兩種說明：

- `every keep-rate is 1`：預算不小於母體筆數，全部保留。權重仍然照目標權重算。
- `the budget cannot be spent`：沒有正樣本的格只會拿到保底的負樣本，所以留得再多也到不了預算。要多用預算就調高 `--eta`。

### 示例：repo 內建的合成資料

用 `scripts/local/local_spark_setup.py` 產生的示例資料（3 個分層 × 8 個 item，訓練月份共 83,672 筆）、預算 30,000、旋鈕全用預設，終端印出：

```
λ = 0.148271
k = 1.3031
expected rows: 9,206 positive + 20,794 negative = 30,000 (budget 30,000, difference -0.01; the draw's standard deviation ≈ 114 rows)
cell-classes held by a limit: capped at 1 24, positive floor 0, 1-bucket floor 0
self-checks: 4/4 passed
```

這份資料的正樣本只有 9,206 筆，預算塞得下，所以 24 格的正樣本全留（`capped at 1 24`），剩下的 20,794 筆分給負樣本。

## 步驟 3：貼回設定

- `sample_ratio_overrides.yaml` 貼到 `conf/base/parameters_dataset.yaml` 的 `dataset:` 底下，**整段取代**原本的 `sample_ratio_overrides`。檔案內容已經縮排好兩格。
- `sample_weights.yaml` 貼到 `conf/base/parameters_training.yaml` 的 `training:` 底下，取代原本的 `sample_weight_keys` 和 `sample_weights`。

貼在 `conf/base/`，不要貼在 `conf/<env>/`：環境檔只能覆蓋或新增 key，刪不掉 base 裡原有的 override key（issue #477）。原有的 key 如果是現在已經不存在的格，就會殘留下來。

## 步驟 4：跑 dataset 與 training，確認成功

照平常的方式跑 dataset，再跑 training。

**① 筆數對得上報告。** training 的 log 裡，`prepare_train_inputs` 這個 node 印的兩行 `extract_Xy: parquet metadata num_rows=`，一行是 train、一行是 train_dev，加起來跟報告的期望筆數比，差距應該在報告印的標準差的兩三倍以內。拿前面那份示例資料把四個步驟跑完：train 27,384 筆加 train_dev 2,580 筆，共 29,964 筆，比期望少 36 筆。正樣本全留，剛好 9,206 筆；少的 36 筆都是負樣本，大約是報告印的標準差（114 筆）的三分之一。

**② 權重都對上了資料。** 打開 `data/models/<model_version>/sample_weight_report.json`，`unmatched_keys` 應該是空的。唯一的例外是期望筆數不到 1 的 key：那一類太小，抽不到任何列，對不上是正常的。其他 key 對不上，代表 key 的字串跟資料對不起來，回頭檢查分層欄的值。

## 只想換權重，不重跑 dataset

只改 `--weight-t`、`--weight-alpha`，不改其他旋鈕，`sample_ratio_overrides.yaml` 會逐位元組不變。這時只要把新的 `sample_weights.yaml` 貼回去，dataset 不用重跑，直接重跑 training。

這樣做權重仍然不偏：權重只要等於目標權重 ÷ 保留率，保留率沒變，換掉的只是目標權重。拿來比較幾種權重設定很方便。

## 被擋下時

訊息都會寫原因和修法。常見的幾種：

| 訊息開頭 | 原因 | 修法 |
|---|---|---|
| `the label column ... is not in dataset.sample_group_keys`（item 同理） | 見〈動手前先確認〉① | 把那一欄加進 `sample_group_keys` |
| `A9a: stratum column(s) ...` | 分層欄不在 train model_input 裡，權重會失效 | 加進 `dataset.carry_columns`，見〈動手前先確認〉② |
| `item(s) ... have no positive rows` | 某個 item 在訓練月份一筆正樣本都沒有，它的目標權重沒有定義 | 拉長 `train_snap_dates`，或把這個 item 從這幾個月的 sample_pool 拿掉，再重跑步驟 1 |
| `budget ... is below the fewest rows any plan can keep` | 預算連正樣本下限加上每類一個桶都放不下 | 加大 `--budget` 或調低 `--pos-floor` |
| `A5:` 或 `A9c:` | profile 裡有 `schema.categorical_values` 沒宣告的 item | 修 sample_pool，或把 item 加進宣告清單 |
| `the profile was counted by [...]` | profile 沒依現在的某個抽樣欄分格 | 照現在的設定重跑步驟 1 |
| `profile format_version ...` | profile 是別的版本的腳本產生的 | 用同一份程式重跑步驟 1 |
| `--gamma`、`--eta`、`--pos-floor`、`--t` 超出範圍 | 見〈旋鈕〉的範圍 | 改旋鈕 |

**只警告、不擋的：** 換了訓練月份還沒重跑 profile、profile 記到 NULL 列、objective 不是 `binary`、`train_zero_positive_group_ratio` 不是 1。警告同時印在終端和報告裡。

## 相關文件

- [`sampling-overrides-editor.md`](sampling-overrides-editor.md)：手調用的視覺化編輯器，key 格式相同。
- [`CONTEXT.md`](../../../CONTEXT.md)「抽樣與權重」：詞彙。
- [`docs/pipelines/dataset.md`](../../pipelines/dataset.md)：dataset pipeline 怎麼抽樣。
