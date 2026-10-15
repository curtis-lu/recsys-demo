# 特徵表檢查工具

`scripts/profile_feature_table.py`：給一張 Hive 特徵表，用 Spark 算每一期、每一欄的統計量，產出一份 HTML 報告，標出要處理的欄位，附建議的處理方式。

> 它**只看、不改**：報告最後的〈行動建議總結〉把欄名整理成可以複製的清單，要怎麼用由你決定。報告不提框架的設定。
>
> 設計文件：`docs/notes/2026-10-15-feature-table-profiling-design.md`（規則、門檻、報告編排都以它為準）。

**這一版只有基本檢查**：空值、只有一個值。分布偏移與極端值、相關性在後兩個階段加上；在那之前，設定檔寫了 `checks`、`thresholds.psi` 這類鍵會報錯。

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
- 跑完會印出報告位置。報告是單一頁面 `index.html`，不連網，可以單獨帶走。
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
| `thresholds` | 只認 `null_ratio`。沒寫的鍵用預設值。上一版的 `precision_max_abs` 已經拿掉，寫了會報錯，刪掉那一行即可 |

**開跑前會先檢查設定**：必填鍵沒寫、不認得的鍵、值的型別不對、表不存在、`time_col`／`key_cols`／`exclude_cols` 有不存在的欄。所有問題一次列完再結束，不會改一個、跑一次、再冒出下一個。只有「連 `table` 都沒寫」時不開 Spark、直接列出其他問題。

## 3. 報告怎麼讀

報告是一頁，左邊是目錄，最上面一行固定顯示表名、期間、每期筆數。段落依檢視項目排：

| 段落 | 用來做什麼 |
|---|---|
| 總覽 | 每個檢視項目有幾欄有事，點了跳過去 |
| 空值 | 每一期都幾乎是空值、部分期別幾乎是空值；另外列出有變動但沒達門檻的欄位，只供參考 |
| 只有一個值 | 整欄只有一個值、部分期別只有一個值、每期各只有一個值 |
| 全部欄位 | 查單一欄位或一整族欄位；點欄名展開各期的總筆數、空值筆數、最小值、最大值 |
| 行動建議總結 | 把前面的結果整理成三種做法，每種都能複製欄名（一行一個） |
| 資料範圍與判斷標準 | 這次算的是哪張表、哪幾期、用了什麼門檻；型別不支援而沒檢查的欄位；設定檔原文 |

- 每一欄的「各期」是一個小長條圖：一期一根，由左到右是最早到最新，期別再多也一樣寬（超過 150 期時看不清單一期，點欄名展開明細）；滑鼠移上去看那一期的數字。
  - 空值的圖：越高越空，達門檻的是橘色。
  - 〈部分期別只有一個值〉的圖：橘＝那一期只有一個值，灰＝有變化，空白＝整期都是空值。
- 表格點表頭可以排序；上方的篩選框可以篩欄名，頁首的篩選框會套到每一張表。「部分期別幾乎是空值」可以用「出事的期別」篩出某一期斷了哪些欄。

### 五種問題

「只有一個值」都是扣掉空值之後；某一期全是空值時，那一期直接略過。空值＝NULL、float／double 的 NaN 與 ±Infinity、字串的空字串。

| 問題類型 | 判斷標準（預設） |
|---|---|
| 每一期都幾乎是空值 | 每一期的空值比例都 ≥ 99% |
| 部分期別幾乎是空值 | 只有部分期別的空值比例 ≥ 99% |
| 整欄只有一個值 | 所有期別合計只有一個值 |
| 部分期別只有一個值 | 只有部分期別只有一個值 |
| 每期各只有一個值 | 每一期都只有一個值，但各期的值不同 |

- 「每一期都幾乎是空值」「整欄只有一個值」不再判斷其他問題；其他問題可以同時成立。

### 行動建議總結的三種做法

| 做法 | 來自 |
|---|---|
| 可以排除：欄位沒有資訊 | 每一期都幾乎是空值、整欄只有一個值 |
| 請上游查這幾期的資料 | 部分期別幾乎是空值，而且跳幅 ≥ 10 個百分點；部分期別只有一個值 |
| 看業務意義決定要不要留 | 部分期別幾乎是空值，但跳幅 < 10 個百分點；每期各只有一個值 |

- 跳幅＝出事那幾期最低的空值比例 − 其他期別最高的空值比例。跳幅大，是那幾期突然空掉；跳幅小，是本來就很空、偶爾越過門檻。10 個百分點寫在程式裡（`UPSTREAM_JUMP_MIN`），不是設定鍵。
- 「請上游查」那一列會列出每一期斷了哪些欄，每一期各有一個複製按鈕。
- 不檢查 float32 精度：對 LightGBM 幾乎沒有影響，理由見設計文件〈一、目的〉的「不做的事」。

## 4. 輸出的檔案

```
data/profiling/feature_profile/<表名>_<YYYYMMDD_HHMMSS>/   # 或 output_dir 指定的位置
├── profile.json     # 統計量（不含判斷結果），render 只靠它重畫
└── index.html       # 報告
```

用新版 `render` 重畫上一版 `run` 的資料夾也可以：`index.html` 會換成新版，舊版的 `01-flags.html`、`02-basic.html`、`05-all.html`、`plotly.min.js` 不會被刪，也不再被連到，可以自己刪掉。

`profile.json` 存的是每一欄、每一期的總筆數、空值筆數、最小值、最大值；判斷結果每次畫報告時依門檻重算。所以換門檻只要 `render`。`schema_version` 不符時 `render` 會請你重跑 `run`。
