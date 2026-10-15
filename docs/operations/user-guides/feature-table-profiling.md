# 特徵表檢查工具

`scripts/profile_feature_table.py`：給一張 Hive 特徵表，用 Spark 算每一期、每一欄的統計量，產出一份 HTML 報告，標出要處理的欄位，附建議的處理方式。

> 它**只看、不改**：報告裡可以直接排除的欄位附上能貼進設定檔的清單，貼不貼由你決定。
>
> 設計文件：`docs/notes/2026-10-15-feature-table-profiling-design.md`（規則、門檻、報告編排都以它為準）。

**這一版只有基本檢查**：空值、只有一個值、float32 精度。分布偏移與極端值、相關性在後兩個階段加上；在那之前，設定檔寫了 `checks`、`thresholds.psi` 這類鍵會報錯。

---

## 1. 怎麼跑

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=<Spark 設定目錄>      # 本機用 $PWD/conf/spark-local（見 local-spark-setup.md）

# 1. 複製範本，改成你的表
cp scripts/profile_feature_table.template.yaml my_profile.yaml

# 2. 算統計量、存 profile.json、畫報告
PYTHONPATH=src .venv/bin/python scripts/profile_feature_table.py run --config my_profile.yaml

# 3. 想換門檻：只重畫，不重跑 Spark
PYTHONPATH=src .venv/bin/python scripts/profile_feature_table.py render <輸出資料夾> --config my_profile.yaml
```

- 開 Spark 的方式同 `scripts/sampling_overrides_editor.py`：工具只給 `app_name: feature-profile`，其餘 Spark 設定（master、executor 記憶體、Hive metastore）全部來自 `SPARK_CONF_DIR` 目錄裡的設定檔，工具的設定檔裡沒有 Spark 區塊。要調 Spark 就改那個目錄。
- 跑完會印出報告位置。用瀏覽器打開資料夾裡的 `index.html`；它是單一檔案，不連網，單獨傳給別人也打得開。
- `render` 不給 `--config` 時，用 `profile.json` 裡存的門檻（也就是 `run` 當時的門檻）。`render` 不會改 `profile.json`。

### `--batch-size`

`run` 把特徵欄位分批，每批跑一趟 Spark 聚合、只讀自己那幾欄。預設每批 200 欄。上千欄的表一次聚合會讓執行計畫過大，每批太少又會多掃很多趟。預設值還沒在正式環境量過：第一次在真表上跑時，請記下欄數、期數、每期筆數、耗時，必要時換幾個 `--batch-size` 比較。

## 2. 設定檔

每一個鍵和「沒寫會怎樣」都寫在範本 `scripts/profile_feature_table.template.yaml` 裡，這裡只講容易踩到的：

| 鍵 | 要注意的事 |
|---|---|
| `table`、`time_col`、`key_cols` | 必填。`key_cols` 至少一欄 |
| `exclude_cols` | 不是特徵、也不是主鍵的欄位（例如寫入時間）。不寫的話會被當成特徵檢查 |
| `time_range` | `start`、`end` 都含當期，可以只寫一個。直接和 `time_col` 比較 |
| `where` | 篩的不是分區欄位時，Spark 會掃全表，會變慢 |
| `thresholds` | 只認 `null_ratio`、`precision_max_abs`。沒寫的鍵用預設值 |

**開跑前會先檢查設定**：必填鍵沒寫、不認得的鍵、值的型別不對、表不存在、`time_col`／`key_cols`／`exclude_cols` 有不存在的欄。所有問題一次列完再結束，不會改一個、跑一次、再冒出下一個。只有「連 `table` 都沒寫」時不開 Spark、直接列出其他問題。

## 3. 報告怎麼讀

報告是一個頁面 `index.html`：頂端固定一行資料範圍與全頁的欄名篩選框，左邊是目錄（附各組欄數）。段落照**要做的事**分組，不是照工具用哪條規則抓到：

| 段落 | 用來做什麼 |
|---|---|
| 總覽 | 每一組有幾欄、怎麼做；名詞（空值、只有一個值、float32 最大誤差） |
| 直接排除 | 每一期都幾乎是空值、整欄只有一個值的欄位。附兩段能直接貼的設定：`training.feature_selection.exclude`（只要重訓）與 `dataset.prepare_model_input.drop_columns`（要重建 dataset），一行一個欄名 |
| 回頭查上游 | 先列「要回頭查的期別」：每一期是哪些欄幾乎全空或全部同一個值，可以直接拿去開單。再列各欄位的依據 |
| dataset 會擋下 | 整數或 decimal 欄位超過 float32 的上限，dataset 的精度檢查預設會讓 dataset 跑失敗。列出超過上限幾倍（要縮小多少）、超過的期數 |
| 自行判斷 | 平常就很空、偶爾越過門檻的稀疏欄位（附每期有值的筆數），與每期各只有一個值的欄位 |
| 可以不理 | 只有 double 的 float32 問題：dataset 不檢查 double，對 LightGBM 幾乎沒有影響。預設收合，列出最多差多少 |
| 各期空值比例 | 色格表，一列一欄、一格一期。預設只列變動幅度 ≥ 1 個百分點的欄位、隱藏直接排除的欄位 |
| 全部欄位 | 查單一欄位或一整族欄位。點欄名展開各期的總筆數、空值筆數、最小值、最大值 |
| 資料範圍與判斷標準 | 表、期別、每期筆數、執行時間、這次用的門檻、未檢查的欄位、設定檔原文 |

- **排序**：點表頭；再點一次反過來。表上的 ▲▼ 是目前的排序。
- **篩選**：每張表上方的框比對欄名，空白分隔多個關鍵字（例如 `cc_ l12m`）；頂端的框一次套用到每一張表。
- **複製**：「複製列出的欄名」複製目前篩選後看得到的欄名，用逗號分隔。
- **欄名連結**：任何一段的欄名點了，都會跳到〈全部欄位〉那一列並展開各期數字。
- 報告沒有放大縮小的功能：要看全貌用篩選，要看細節把滑鼠移到格子上或展開欄位。

### 分組規則

「只有一個值」都是扣掉空值之後；某一期全是空值時，那一期直接略過。空值＝NULL、float／double 的 NaN 與 ±Infinity、字串的空字串。

| 問題類型 | 判斷標準（預設） | 歸到 |
|---|---|---|
| 每一期都幾乎是空值 | 每一期的空值比例都 ≥ 99% | 直接排除 |
| 整欄只有一個值 | 所有期別合計只有一個值 | 直接排除 |
| 部分期別幾乎是空值 | 只有部分期別的空值比例 ≥ 99% | 出事那幾期比其他期別高 10 個百分點以上：回頭查上游；不到：自行判斷 |
| 部分期別只有一個值 | 只有部分期別只有一個值 | 回頭查上游 |
| 每期各只有一個值 | 每一期都只有一個值，但各期的值不同 | 自行判斷 |
| float32 精度不足 | 有小數且絕對值最大 ≥ 100,000；或沒有小數且絕對值最大超過 16,777,216。宣告成 float 的欄位本來就是 float32，不判斷 | 整數或 decimal：dataset 會擋下；double：可以不理 |

- 前兩種已經可以直接排除，不再判斷其他問題；「每期各只有一個值」不判斷 float32 精度。其他問題可以同時成立，同一欄會出現在每一個要做的段落裡，表上的「其他問題」欄寫明。
- 10 個百分點只決定分組，寫在程式裡（`UPSTREAM_JUMP_MIN`），不是設定鍵。
- float32 精度：整數欄位的上限和 dataset 的精度閘門（`validate_numeric_precision`）相同，都是超過 16,777,216 就擋。decimal 欄位這支工具用自己的門檻，dataset 依宣告的小數位數另算上限，以 dataset 跑出來的精度報告為準。被擋下時可以請上游改單位，或改 `dataset.numeric_feature_storage_type: float64`，或改 `dataset.numeric_precision_policy: truncate` 接受失真。
- 建議處理方式的字句與門檻，會在正式環境實測後和專案負責人一起定案（設計文件第八節）。

## 4. 輸出的檔案

```
data/profiling/feature_profile/<表名>_<YYYYMMDD_HHMMSS>/   # 或 output_dir 指定的位置
├── profile.json     # 統計量（不含判斷結果），render 只靠它重畫
└── index.html       # 報告，單一檔案，不需要其他檔案
```

`profile.json` 存的是每一欄、每一期的總筆數、空值筆數、最小值、最大值，與有沒有小數；判斷結果每次畫報告時依門檻重算。所以換門檻只要 `render`。`schema_version` 不符時 `render` 會請你重跑 `run`。

舊版畫的資料夾裡還有 `01-flags.html`、`02-basic.html`、`05-all.html`、`plotly.min.js`。對它跑 `render` 只會重寫 `index.html`，舊的四個檔不會更新，可以自己刪掉。
