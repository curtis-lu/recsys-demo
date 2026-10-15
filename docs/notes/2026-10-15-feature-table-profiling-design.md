# 特徵表檢查工具：設計

> 2026-10-15 定案，開發照這份做。報告的樣子看同目錄的 `2026-10-15-feature-table-profiling-mockup.html`：數字是假的，用瀏覽器打開即可（樣板為了預覽從網路載入 plotly；實際報告用本機產生的 `plotly.min.js`，不連網）。

## 總表

| 項目 | 決定 |
|---|---|
| 做什麼 | 檢查一張特徵表在每一期的狀況，標出需要處理的欄位，給建議的處理方式 |
| 看什麼 | 一次看一張 Hive 表，表名由工具使用者指定，不綁 `catalog.yaml` |
| 怎麼跑 | 獨立腳本 `scripts/profile_feature_table.py`，手動執行；設定寫在工具自己的 yaml，用 `--config` 指定 |
| 和框架的關係 | 不讀 `conf/` 底下的任何設定；判斷規則也不沿用框架的函式（例如 float32 精度直接依 float32 本身的值域算） |
| 檢查項目 | 基本檢查（一定跑）、分布偏移與極端值（可選）、相關性（可選） |
| 用多少資料 | 每一期都用全部資料；只有相關性是抽最新一期的 50,000 筆 |
| 門檻 | 寫在 yaml 的 `thresholds`，沒寫就用預設值；報告首頁列出這次用的值 |
| 輸出 | 先存成 `profile.json`，再畫成一個 HTML 資料夾。改了門檻只要重畫，不用重跑 Spark |
| 建議處理方式 | 寫出真的設定鍵與可以直接貼上的欄名清單；字句在開發時 demo 給專案負責人，一起決定 |

## 一、目的

特徵表通常有上百到上千欄，每一期由上游 ETL 寫入。常見的問題有：整欄幾乎都是空值、某一期上游沒進資料、某一期填了預設值、單位在某一期改了、出現不合理的極端值、存成 float32 會改變數值、好幾欄幾乎是同一個東西。這些問題在訓練前就該知道。

框架的 dataset pipeline 有幾道資料閘門（`src/recsys_tfb/core/consistency.py` 的 B 系列），但它們只擋「會讓 pipeline 跑錯」的情況，不會逐欄、逐期整理成報告；資料品質本身的檢查屬於上游（`docs/adr/0006-data-quality-checks-belong-upstream.md`）。這支工具補這一塊：工具使用者給一張表，工具用 Spark 算完每一期、每一欄的統計量，產出一份報告，告訴他哪些欄位要處理、怎麼處理。

**不做的事：**

| 不做 | 理由 |
|---|---|
| 一起看 sample_pool、label_table | 範圍只限特徵表；要看樣本或標籤是另一件事 |
| 判斷欄位對排序有沒有用 | 只看這張表本身的數字，看不出和標籤的關係 |
| 檢查代碼值（例如沒有資料時填 9,999,999） | 不在這支工具的需求範圍；而且代碼一旦占超過 1% 的筆數，p99 本身就是代碼，用「離 p99 多遠」也抓不到 |
| 建議金額欄取 log 或換單位 | 對 LightGBM 沒有幫助，換成萬元再取整數還會變差（附錄二） |
| 讀 parquet footer 的統計量來加速 | 任意一張 Hive 表不保證是 parquet；先用 Spark 量過實際耗時再決定 |
| 改 training 既有的 `compute_feature_statistics`（`src/recsys_tfb/pipelines/training/nodes.py:2023`） | 那是訓練資料的摘要，用途不同，維持原樣 |

## 二、使用方式

這是獨立腳本，不是 `python -m recsys_tfb <pipeline>` 的一條 pipeline，所以有自己的子指令：

```bash
# 算統計量、存 profile.json、畫報告
python scripts/profile_feature_table.py run --config my_profile.yaml

# 只重畫報告：讀既有的 profile.json，不開 Spark。
# 給 --config 時用它的 thresholds 重新判斷；不給時用 profile.json 裡存的 thresholds
python scripts/profile_feature_table.py render <輸出資料夾> [--config my_profile.yaml]
```

範本放在 `scripts/profile_feature_table.template.yaml`，工具使用者複製一份再改。範本要列出每一個鍵，並寫明沒寫時會怎樣：

```yaml
table: mydb.feature_table     # 必填。要檢查的 Hive 表
time_col: snap_date           # 必填。期別欄位
key_cols: [cust_id]           # 必填。主鍵欄位，不檢查
exclude_cols: []              # 沒寫＝不排除。不是特徵、也不是主鍵的欄位，例如寫入時間
time_range:                   # 沒寫＝讀全部期別
  start: "2024-01-31"         # 含這一期
  end: "2025-02-28"           # 含這一期
where: null                   # 沒寫＝不篩選。額外的 Spark SQL 條件，和 time_range 用 AND 合併
checks:                       # 基本檢查一定會跑
  distribution: true          # 必填。分布偏移與極端值
  correlation: true           # 必填。相關性
thresholds:                   # 沒寫的鍵用右邊的預設值
  null_ratio: 0.99
  precision_max_abs: 100000   # 有小數的欄位；整數固定用 16,777,216
  psi: 0.25
  tail_ratio: 10
  tail_log_ratio: 1
  corr: 0.95
spark:                        # 沒寫＝用叢集的 spark-defaults.conf
  spark.executor.memory: 8g
output_dir: null              # 沒寫＝data/profiling/feature_profile/<表名>_<YYYYMMDD_HHMMSS>/
```

- `checks` 兩個鍵設為必填：它們決定要多跑幾趟 Spark，耗時差很多，讓工具使用者每次都明確選。
- `time_range` 的 `start`／`end` 和 `time_col` 直接比較（`time_col` 是 date 時 Spark 會把字串轉成日期）。`time_col` 是 NULL 的列不讀，筆數印在報告首頁。
- `where` 篩的如果不是分區欄位，Spark 會掃全表，會變慢。報告首頁印出 `where` 原文與實際讀到的期別。
- `spark` 區塊合併在 `{"app_name": "feature-profile"}` 之上（工具使用者寫了 `app_name` 就用他的），交給 `get_or_create_spark_session`（`src/recsys_tfb/utils/spark.py:56`）。值只能是字串、整數或布林（同檔 `_validate_values`）。讀 Hive 需要叢集的 `SPARK_CONF_DIR`；本機驗證的設定見 `docs/operations/dev-setup/local-spark-setup.md`。
- `output_dir` 預設的 `<表名>` 不含資料庫名稱（`mydb.feature_table` → `feature_table`）。
- 開跑前先檢查設定，所有問題一次列完再結束：表不存在、`time_col`／`key_cols`／`exclude_cols` 有不存在的欄、必填鍵沒寫、`thresholds` 有不認得的鍵。

## 三、欄位分類

- 扣掉 `time_col`、`key_cols`、`exclude_cols`，剩下的是特徵欄位。
- **數值欄位**：tinyint、smallint、int、bigint、float、double、decimal。
- **字串欄位**：string。boolean 先轉成字串（`cast(x as string)`），當字串欄位處理。
- **未檢查的欄位**：其他型別（date、timestamp、array、map、struct），列在 05 頁的「未檢查的欄位」。
- **空值**＝NULL，加上 float／double 的 NaN 與 ±Infinity，加上字串的空字串。後面所有統計量都先把這些換成 NULL 再算。
- **有小數**＝任何一期有任何一個值的小數部分不是 0。沒有小數的數值欄位，在精度判斷時當整數看（不管宣告的型別）。

## 四、計算方式

所有統計都用 Spark 內建函式，不用 UDF。欄位很多時，一次 `groupBy` 帶上千個聚合式會讓執行計畫過大，所以**欄位分批**，每批跑一次；批次大小開發時在真的表上量（第八節）。不 cache 整張表：每批只讀自己那幾欄。

### 基本檢查（一定跑）

一趟 `groupBy(time_col)`，每一欄算：

| 統計量 | Spark 寫法（x 已先把空值換成 NULL） | 用在 |
|---|---|---|
| 總筆數 | `count(*)` | 空值比例的分母 |
| 空值筆數 | `count(*) − count(x)` | 空值比例 |
| 最小值、最大值 | `min(x)`、`max(x)` | 只有一個值（`min = max`）、精度、極端值 |
| 有沒有小數 | `max(abs(x − floor(x))) > 0`（float／double／decimal 才算） | 精度 |

`min = max` 就表示扣掉空值後只有一個值，不需要 `count(distinct)`。字串用字典序的 `min`／`max`，道理一樣。

### 分布偏移與極端值（`checks.distribution: true` 時才跑）

1. 在基本檢查那一趟順便算 `percentile_approx(x, [0.01, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 0.99])`。p1、p25、p50、p75、p99 給走勢圖和極端值用；十分位數只用最早一期的，當 PSI 的切點。
2. 第二趟算 PSI 的分組筆數：
   - 數值欄位：用最早一期的 9 個十分位數切成 10 組，每組含上界：(−∞, d1]、(d1, d2]、…、(d9, ∞)。相同的切點合併，組數可能變少。空值另成 1 組。用 `CASE WHEN` 算組別，再用 `sum(CASE WHEN 組別 = k THEN 1 END)` 數每一期每一組的筆數。
   - 字串欄位：先在最早一期找出最常見的 20 個值，各成 1 組；其他值合成 1 組；空值 1 組。
3. PSI ＝ Σ（本期占比 − 基準期占比）× ln（本期占比 ÷ 基準期占比）。占比是 0 的組用 0.0001 代替，避免 ln(0)。
4. 基準期＝這次讀到的最早一期。想換基準，就改 `time_range` 或 `where`。
5. 不算 PSI 的情況：只讀到一期；基準期這一欄全是空值；欄位已標成「每一期都幾乎是空值」「整欄只有一個值」「每期各只有一個值」。

### 相關性（`checks.correlation: true` 時才跑）

1. 只取最新一期。抽樣：`sample(fraction = min(1, 55,000 ÷ 該期筆數), seed = 42)` 再 `limit(50,000)`；該期不到 50,000 筆就全拿。拉回 driver。
2. 只算數值欄位。跳過：最新一期空值比例 ≥ `null_ratio` 的欄位、最新一期只有一個值的欄位。跳過的欄位和原因列在 04 頁。
3. Spearman：每一欄先換成排名（同值取平均名次，空值保留），再算兩兩之間的相關係數，每一組只用兩欄都有值的筆數。用 numpy 做：把「是否有值」做成 0／1 矩陣，兩兩的筆數、和、平方和都用矩陣相乘一次算完，不逐對迴圈。兩欄都有值的筆數少於 1,000，或其中一欄在這些筆裡沒有變化，就不給相關係數。
4. 為什麼不用 Spark 的 `Correlation.corr`：它要求整筆都沒有空值；欄位一多，幾乎每一筆都會被丟掉。

### `profile.json`

`render` 只靠這個檔重畫並重新判斷，所以它要存「算出來的統計量」，不存「判斷結果」：

- `schema_version`、產出時間、設定檔原文、實際讀到的期別與每期筆數、執行時間。
- 每一欄：型別、分類（數值／字串／未檢查）、有沒有小數；每一期的總筆數、空值筆數、最小值、最大值，有跑分布時再加分位數、PSI。
- 字串欄位的分組占比（畫各值占比圖用）。
- 相關性：絕對值 ≥ 0.5 的配對與兩欄都有值的筆數（所以 `render` 能把 `corr` 調低到 0.5；更低要重跑）；跳過的欄位與原因。

## 五、問題類型與判斷標準

### 先後順序

1. 標成「每一期都幾乎是空值」或「整欄只有一個值」的欄位，不再判斷其他問題：它們已經可以直接排除。
2. 標成「每期各只有一個值」的欄位，不判斷 float32 精度、分布偏移、極端值。
3. 其他問題可以同時成立，例如同一欄可以既是「部分期別幾乎是空值」又是「分布偏移」。

### 判斷標準

「只有一個值」都是指扣掉空值之後；某一期全是空值時，那一期不算「只有一個值」，也不算「有變化」，直接略過。

| 問題類型 | 判斷標準（預設） | 設定鍵 |
|---|---|---|
| 每一期都幾乎是空值 | 每一期的空值比例都 ≥ 99% | `null_ratio` |
| 部分期別幾乎是空值 | 只有部分期別的空值比例 ≥ 99% | `null_ratio` |
| 整欄只有一個值 | 所有期別合計只有一個值 | — |
| 部分期別只有一個值 | 只有部分期別只有一個值 | — |
| 每期各只有一個值 | 每一期都只有一個值，但各期的值不同（例如每期一個數字的總體指標） | — |
| float32 精度不足 | 有小數，而且絕對值最大 ≥ 100,000；或沒有小數，而且絕對值最大 ≥ 16,777,216 | `precision_max_abs`（只管有小數的；16,777,216 是 2^24，固定） |
| 分布偏移 | 有一期的 PSI ≥ 0.25 | `psi` |
| 極端值 | 有一期、同一側同時符合：倍數 ≥ 10，而且 log 後倍數 ≥ 1 | `tail_ratio`、`tail_log_ratio` |
| 高度相關 | 最新一期的 Spearman 相關係數，絕對值 ≥ 0.95 | `corr` |
| 值幾乎都相同（只供參考） | 有一期 p1 = p99，表示那一期至少約 98% 的值相同。不算問題 | — |

### float32 精度不足

float32 只保留大約 7 位有效數字。100,000 以上，第二位小數開始不準（100,000.37 存成 100,000.3672）；16,777,216 以上，整數的個位數開始被改掉（25,000,001 存成 25,000,000）。

這對 LightGBM 幾乎沒有影響（附錄二）。但要知道：框架的 dataset 有一道精度閘門 B8（`validate_numeric_precision`，`src/recsys_tfb/pipelines/dataset/nodes.py:1009`），`dataset.numeric_precision_policy` 預設是 `block`，整數與 decimal 欄位撐不過 float32 時會讓 dataset 跑失敗；float／double 欄位它不檢查。這支工具的規則刻意不沿用它（不跟框架綁），所以兩邊標出的欄位不完全相同。建議處理方式的字句要提到這件事，例如「上游四捨五入，或把 `numeric_precision_policy` 改成 `truncate` 接受失真」，確切寫法 demo 時定。

### 極端值

最大值、最小值兩側各算一次。兩個條件要在同一期、同一側同時成立才算。報告列出 log 後倍數最大的那一期、那一側。p1 = p99 的那一期不判斷。

```
最小值 ···· p1 ═══════ p99 ····················· 最大值
              └─寬度─┘    └─────── 距離 ───────┘
倍數 ＝ 距離 ÷ 寬度
最大值那一側：（最大值 − p99）÷（p99 − p1）
最小值那一側：（p1 − 最小值）÷（p99 − p1）
log 後倍數：四個數先換成 sign(x) × ln(1 + |x|)，再用同樣的公式
```

為什麼要兩個條件：只看倍數，金額這類天生長尾的欄位幾乎都會被標出來（最大值常是 p99 的幾十倍）；只看 log 後倍數，真正的錯誤也會被一起縮小，門檻得設高，就會漏掉年齡 999 這種錯誤。兩個一起用，「倍數」確認真的離很遠，「log 後倍數」負責排除正常的長尾，所以門檻可以放低到 1。模擬比較見附錄一。

log 不改變大小順序，所以不用另外對資料取 log：照常算 p1、p99、最大值、最小值，最後只對這四個數取 log，不多跑 Spark。

## 六、報告編排

用 `recsys_tfb.report.write_pages`（`src/recsys_tfb/report/pages.py:262`）寫成一個資料夾：`index.html`、五個頁面、共用的 `plotly.min.js`。頁面的 slug 決定檔名與首頁的排列順序（`write_pages` 依 slug 排序），固定為 `01-flags`、`02-basic`、`03-drift`、`04-corr`、`05-all`。首頁最後的頁面清單由 `write_pages` 自動產生，不另做。`index_intro` 是原樣放進 HTML 的，表名、`where` 原文這類字串要先 escape。

`Page.scope` 留空，不放「範圍說明」框：這份報告的目的就是依門檻標出問題並給建議，不適用 `docs/agents/diagnosis-report-presentation.md` 的「不下結論、不設門檻」。

表格是靜態的。圖用 plotly：熱圖可以用 `src/recsys_tfb/report/figures.py` 的 `heatmap`；分位數走勢（帶狀圖加下拉選單）、字串各值占比（堆疊長條）沒有現成的，直接用 plotly 在腳本裡畫。

每一段都要說得出它幫讀者做哪個決定：

| 頁 | 段落 | 幫讀者做什麼 |
|---|---|---|
| 首頁 | 總覽 | 一眼看出有幾欄要處理、幾欄可以直接排除 |
| | 資料範圍 | 確認算的是哪張表、哪幾期、有沒有篩選、每期幾筆、跑了多久 |
| | 判斷標準 | 看這次用的標準；想調時知道改哪個鍵 |
| | 設定檔（收合） | 要重跑一樣的報告時照抄 |
| 01 需要處理的欄位 | 總表 | 每種問題有幾欄、建議怎麼處理 |
| | 每種問題一段 | 欄名清單和依據；可以直接排除的那段附上能直接貼的設定（`training.feature_selection.exclude` 只需重訓；`dataset.prepare_model_input.drop_columns` 要重建 dataset） |
| 02 空值與精度 | 各期空值比例（熱圖） | 看是哪幾期空掉。只畫至少有一期空值比例 ≥ 0.5% 的欄位 |
| | float32 精度 | 每欄最大值存成 float32 後會變成多少。列出所有有小數的欄位，和絕對值最大 ≥ 16,777,216 的整數欄位 |
| | 所有欄位的各期空值比例（收合） | 熱圖背後的完整數字 |
| 03 分布偏移與極端值 | 分布偏移（PSI）熱圖 | 哪些欄位從哪一期開始變。只畫 PSI 最大的 12 欄，顏色到 0.5 為止，滑鼠移上去看原值 |
| | 分位數走勢 | 看怎麼變：整條突然跳好幾倍通常是換了單位，慢慢移動通常是客群在變。選單只放有分布偏移的數值欄位，PSI 最大的排第一 |
| | 字串欄位各值的占比 | 字串欄位的分布怎麼變 |
| | 極端值 | 最離譜的那一期、那個值、p1、p99，判斷是不是資料錯了 |
| | 值幾乎都相同 | 只供參考 |
| 04 相關性 | 高度相關的欄位 | 哪幾組欄位幾乎一樣 |
| | 相關係數矩陣 | 看是不是一整群欄位彼此都很像；只畫出現在上表的欄位 |
| | 未計算的欄位 | 知道哪些欄位沒算相關性、為什麼 |
| 05 所有欄位明細 | 各欄位摘要、各期中位數／最大值／PSI（收合） | 查單一欄位 |
| | 未檢查的欄位 | 知道哪些欄位因為型別不支援而沒有檢查 |

**用字**：用台灣的繁體中文寫法。標題用名詞，不放寫給作者自己的備註（例如「門檻（寫死在程式裡）」），括號只用來放數量或英文原名（「極端值（3 欄）」「分布偏移（PSI）」）；不用箭頭、問句。期別寫「期」，筆數寫「筆」；問題類型用白話句（「每一期都幾乎是空值」），不用自創的「X・Y」標籤。

## 七、實作約束

- 程式放 `scripts/profile_feature_table.py`，和 `scripts/suggest_categorical_cols.py`、`scripts/sampling_overrides_editor.py` 一樣用 typer；測試放 `tests/scripts/test_profile_feature_table.py`，用 `from scripts.profile_feature_table import …`。
- 只能用已宣告的套件：pyspark、pandas、numpy、plotly、typer。scipy 沒有宣告，Spearman 用 numpy 自己排名。
- Spark 不用 UDF。
- `src/recsys_tfb/report/__init__.py:15` 寫著「Nothing under ``scripts/`` imports it.」，這支工具上線後這句變假話，要一起改。
- 寫一份使用說明 `docs/operations/user-guides/feature-table-profiling.md`，同類文件可參考 `docs/operations/user-guides/sampling-overrides-editor.md`。

## 八、開發時待定事項

開發分三個階段：基本檢查、分布偏移與極端值、相關性。每個階段做完都在正式環境用真實特徵表實測一次，下面各項在對應的階段定案。

1. **欄位分批的大小**（基本檢查）：量耗時與 executor 記憶體再定。量的時候寫清楚量了什麼（秒數、記憶體）、在什麼環境。
2. **`percentile_approx` 的精度**（分布偏移與極端值）：同上。
3. **相關性的 driver 記憶體**（相關性）：上千欄（例如 1,500 欄）× 50,000 筆，float32 約 300 MB、float64 約 600 MB，加上排名與遮罩矩陣會再多幾倍。先量；不夠時再討論抽樣筆數要不要變成設定鍵。
4. **相關係數矩陣的欄數上限**（相關性）：高度相關的欄位很多時，矩陣會看不清楚，要定上限與挑選方式。
5. **門檻與建議處理方式的字句**（每個階段各自的問題類型）：demo 給專案負責人，一起決定。極端值的兩個門檻特別需要真實資料。

## 九、驗證方式

1. 每一種問題類型各做一個小 DataFrame 剛好觸發它，和一個剛好不觸發的，測判斷規則；故意弄壞規則的關鍵一行，確認測試會紅。邊界情況（全空的期別、NaN、±Infinity、只讀到一期）各一個測試。
2. 用一份固定的 `profile.json` 測 `render`，不開 Spark；包括換 `thresholds` 後判斷跟著變。
3. 本機用合成資料的特徵表實跑一次 `run`。
4. 每個階段做完，都在正式環境用真實特徵表跑一次，demo 報告，定下第八節裡屬於這個階段的項目。

## 附錄一：極端值規則的模擬

一個月 120 萬筆，11 種正常欄位、6 種壞掉的欄位。腳本在原作者 worktree 的 `data/verification/feature-profile/tail_sim2.py`（`data/` 不進版控）；分布設定如下，可以照著重做。

| 規則 | 正常欄位標錯幾種 | 壞掉的欄位抓到幾種 |
|---|---|---|
| 只看倍數 ≥ 10 | 7 / 11 | 6 / 6 |
| 只看 log 後倍數 ≥ 3 | 1 / 11 | 3 / 6 |
| 倍數 ≥ 10，而且 log 後倍數 ≥ 1（採用） | 2 / 11 | 6 / 6 |

- 正常欄位：存款（lognormal σ=1、σ=2）、刷卡（lognormal σ=1.5、40% 是 0）、資產（Pareto α=1.16）、淨流入（t 分布 df=3，有正有負）、年齡（常態）、次數（Poisson 5）、多 0 的長尾次數（p99 約 40）、稀有次數（Poisson 0.1）、利率（lognormal，中位數 0.01）、使用率（0～1.4）。
- 壞掉的欄位：次數有一筆 100,000（兩種 p99）、存款有一筆 1 兆、存款有 50 筆多乘 1000、年齡有一筆 999、使用率有一筆 50。
- 採用的規則標錯的兩種：資產、利率。只看 log 後 ≥ 3 漏掉的三種：年齡 999、p99 約 40 的次數欄有一筆 100,000、存款有 50 筆多乘 1000。

## 附錄二：金額欄的存法對 LightGBM 的影響

LightGBM 不是直接拿原始數字切，而是先把每個特徵分成最多 `max_bin` 個箱子（LightGBM 預設 255，框架沒有另外設定），每個箱子的筆數差不多，樹只能在箱子邊界切。箱子裝哪幾筆只看大小順序，所以取 log、換單位都不會改變切法；float32 只會合併「第 8 位數之後才不一樣」的值，這種值幾乎不會剛好落在箱子邊界。

60 萬筆、一個金額欄（元、兩位小數、lognormal 中位數約 16 萬），目標是金額的平滑函數加雜訊，同一組 LightGBM 設定，只換這一欄的存法（`data/verification/feature-profile/money_scale_lgb.py`）：

| 存法 | 不同的值有幾個 | 測試 AUC |
|---|---|---|
| float64，元 | 398,409 | 0.72880 |
| float32，元 | 397,944 | 0.72880（預測值完全相同） |
| float32，log(1+元) | 386,108 | 0.72875 |
| float32，元 ÷ 10000 | 397,985 | 0.72877 |
| float32，元，四捨五入到整數 | 290,150 | 0.72876 |
| float32，萬元，四捨五入到整數 | 1,795 | 0.72777 |

另外比了 `max_bin` 與事前依業務意義分箱（`money_bins_lgb.py`）：`max_bin` 從 255 調到 4095，AUC 只多 0.0003；依 1 萬、10 萬、100 萬、1000 萬分成 5 組，不管當數值或 categorical，AUC 都掉 0.009 以上，就算目標剛好在 100 萬有跳躍也一樣。事前分箱（例如 WOE）是邏輯迴歸評分卡的做法，用在 LightGBM 上不如直接給原始數值。
