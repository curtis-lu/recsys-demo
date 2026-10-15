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
- 跑完會印出報告位置。用瀏覽器打開資料夾裡的 `index.html`；整個資料夾要一起帶走（頁面共用同一份 `plotly.min.js`，不連網）。
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

| 頁 | 用來做什麼 |
|---|---|
| 首頁 | 有幾欄要處理、幾欄可以直接排除；這次算的是哪張表、哪幾期、每期幾筆、用了什麼門檻；設定檔原文 |
| 01 需要處理的欄位 | 每種問題的欄名清單與依據。可以直接排除的那段附上能直接貼的設定 |
| 02 空值與精度 | 各期空值比例熱圖、每欄最大值存成 float32 後變成多少 |
| 05 所有欄位明細 | 查單一欄位；型別不支援而沒檢查的欄位 |

### 六種問題與建議處理方式

「只有一個值」都是扣掉空值之後；某一期全是空值時，那一期直接略過。空值＝NULL、float／double 的 NaN 與 ±Infinity、字串的空字串。

| 問題類型 | 判斷標準（預設） | 建議處理方式 |
|---|---|---|
| 每一期都幾乎是空值 | 每一期的空值比例都 ≥ 99% | 可以排除 |
| 整欄只有一個值 | 所有期別合計只有一個值 | 可以排除 |
| 部分期別幾乎是空值 | 只有部分期別的空值比例 ≥ 99% | 欄位本身正常，檢查那幾期的上游資料 |
| 部分期別只有一個值 | 只有部分期別只有一個值 | 欄位本身正常，檢查那幾期的上游資料 |
| 每期各只有一個值 | 每一期都只有一個值，但各期的值不同 | 通常是匯率這類總體指標，不一定要排除 |
| float32 精度不足 | 有小數且絕對值最大 ≥ 100,000；或沒有小數且絕對值最大超過 16,777,216。宣告成 float 的欄位本來就是 float32，不判斷 | 對 LightGBM 幾乎沒影響；但整數與 decimal 欄位會被 dataset 的精度檢查擋下 |

- 前兩種已經可以直接排除，不再判斷其他問題；「每期各只有一個值」不判斷 float32 精度。其他問題可以同時成立。
- 可以直接排除的欄位，貼到 `training.feature_selection.exclude` 只需要重訓；要連 dataset 一起拿掉，改加到 `dataset.prepare_model_input.drop_columns`，要重建 dataset。
- float32 精度：這支工具依 float32 本身的值域判斷，刻意不沿用 dataset 的精度閘門（`validate_numeric_precision`，由 `dataset.numeric_precision_policy` 控制，預設 `block`），所以兩邊標出的欄位不完全相同。被閘門擋下時，可以請上游四捨五入，或把 `numeric_precision_policy` 改成 `truncate` 接受失真。
- 建議處理方式的字句與門檻，會在正式環境實測後和專案負責人一起定案（設計文件第八節）。

## 4. 輸出的檔案

```
data/profiling/feature_profile/<表名>_<YYYYMMDD_HHMMSS>/   # 或 output_dir 指定的位置
├── profile.json     # 統計量（不含判斷結果），render 只靠它重畫
├── index.html
├── 01-flags.html
├── 02-basic.html
├── 05-all.html
└── plotly.min.js
```

`profile.json` 存的是每一欄、每一期的總筆數、空值筆數、最小值、最大值，與有沒有小數；判斷結果每次畫報告時依門檻重算。所以換門檻只要 `render`。`schema_version` 不符時 `render` 會請你重跑 `run`。
