---
status: accepted
date: 2026-09-09
---

# 框架語彙的界線：item／entity 改掉、time 保留、對齊靠使用者

這個 repo 是通用的排序框架，欄位角色由 `conf/base/parameters.yaml` 的
`schema.columns` 宣告（`time` / `entity` / `item` / `label` / `score` / `rank`）。
商業銀行產品推薦只是示例 instantiation。

但框架自己的程式碼與落地產物長期在用示例的業務詞：`evaluation_results.json` 的
`dataset_overview` 裡 `by_item`（抽象）與 `n_products` / `n_customers`（業務）並排、
報表印「產品數」「每客戶平均正例數」、config 鍵叫 `evaluation.product_categories`。

#323／#326／#327 這一輪把界線畫清楚。這份 ADR 記的是**三個會被下一個人當成漏改
而順手「修好」的決定**，不是改名清單（清單在 #327 票面）。

## 決定一：`time` 語彙保留 `snap_date`，`item` / `entity` 語彙全部改掉

改掉的：

| 類別 | 舊 | 新 |
|---|---|---|
| config 輸入鍵 | `evaluation.product_categories` | `evaluation.item_categories` |
| 落地產物鍵（零讀取端） | predict manifest 的 `prods` | `items` |
| 落地產物鍵 | `dataset_overview.totals.n_products` | `n_items` |
| 落地產物鍵 | `dataset_overview.totals.n_customers` | `n_entities` |
| 落地產物鍵 | `dataset_overview.totals.avg_positives_per_customer` | `avg_positives_per_entity` |
| 落地產物鍵 | `by_snap_date` / `by_item` / `by_segment` 各 cell 的 `n_customers` | `n_entities` |

predict manifest 那一列**確實會落地**（`data/models/<model_version>/predict_manifest.json`），但它沒有遷移腳本，也不需要：全 repo 只有寫它的那個函式與同函式的一行 log 讀那個鍵，`compute_test_mAP_spark` 與 `select_shap_population` 都只把整份 manifest 當 in-DAG 排序依賴（`pipelines/training/pipeline.py` 的註解、`nodes.py::compute_test_mAP_spark` 的 docstring）。磁碟上的舊 manifest 帶著 `prods`、沒有人會去讀它。

**完全不動的**：`dataset.{train,calibration,val,test}_snap_dates`、
`inference.snap_dates`、`evaluation.snap_date`、指標的 `by_snap_date` 與
`n_snap_dates`、`core/logging` 的觀測欄白名單、`source_etl` 稽核表自己的欄名。

**為什麼不對稱。** 使用者的判斷是：絕大多數部署的 `time` 就是一個 snap date，而
entity 不一定是客戶、item 不一定是產品。改掉 time 語彙要動每一份 conf、每一個
落地鍵、每一條觀測欄白名單，換到的是一個沒有人會踩到的抽象性。

**這條寫下來是因為它看起來像漏改。** 半年後看到 `by_snap_date` 跟 `n_items` 在同一個
dict 裡並排的人，第一個念頭會是「有人只改了一半」。它不是；動它之前先回到這裡。

**保留的代價是誠實的**：稽核護欄 S6（`docs/agents/architecture-constraints.md`）
禁止 `src/` 出現字面 `snap_date` / `cust_id` / `prod_name`，而它的例外登記表 R6 開局
就有 15 筆，其中 14 筆是 time 語彙。那份清單就是這個決定的帳單——要翻案，先看它。

## 決定二：不加 catalog ↔ schema 的一致性不變量

`schema.columns` 管 DataFrame 的欄名；`catalog.yaml` 的 `partition_cols` 管實體儲存
的分區欄名。**兩者靠使用者自行對齊，框架不加不變量去驗它們相等。**

不加的理由是把四種情境攤開之後，只有一種是靜默的，而那一種配置層擋不到：

| 情境 | 會怎樣 |
|---|---|
| 只改 `schema.columns.time` | 寫入時 catalog 宣告的分區欄不在 DataFrame 裡 → 大聲 raise |
| 只改 catalog 的 `partition_cols` | 同上，方向相反 → 大聲 raise |
| 兩邊都改，表是新的 | `CREATE TABLE` 用新欄名建，正確 |
| **兩邊都改對了，但 Hive 表已經存在** | **靜默不生效** |

最後一種：`CREATE TABLE IF NOT EXISTS` 不會去改既有表的 schema，而 `insertInto` 是
**位置對應**不是名稱對應，於是資料照樣寫進去、分區目錄名還是舊的、零錯誤訊息。

而那個情境比的是**conf 對 metastore**——一個配置層的不變量看不到 metastore，本來就
擋不到它。加一條擋不到目標情境的不變量，只會讓下一個人以為這件事已經有人守著。

**取而代之的是操作要求**，登記在 `docs/agents/architecture-constraints.md` 的例外區：
改 `schema.columns.time` 必須同步改 catalog 的 `partition_cols`，**且既有 Hive 表必須
先 DROP 再重建**。

要推翻這個決定需要新證據：出現一個**不必主動改設定也會踩到**的實例。

## 決定三：`time` / `entity` / `item` 的必填檢查擋在 CLI 入口，不擋 `get_schema`

`core/schema.validate_schema_config`（每個指令進入點都跑）在缺這三個角色時 raise；
`get_schema` 的內建預設本輪不動。

**理由是爆炸半徑，不是純度。** 靜態量測（#323）：

| 擋在哪 | 轉紅的測試函式 | 分佈 |
|---|---|---|
| `get_schema` 的內建預設 | 304 個、18 個檔 | 194 個由 11 處檔內共用 provider 涵蓋、110 個各自寫死 inline params |
| `validate_schema_config`（本決定） | 48 個 | 全部集中在 `tests/test_cli.py` 的一個 conf 建構 helper |

**真實執行擋得一樣死**——`src/` 的 71 個 `get_schema` 系列呼叫點，parameters 全部源自
conf 載入，而任何 conf 都得先過 CLI 入口。差別只在測試側要改幾處。

把 304 個機械 diff 跟一個安全性改動綁在同一張 PR，等於讓審查失效。所以測試側 params
的清理、以及把三個角色從內建預設移除，是之後的獨立一張 PR（**#328**）。

**在 #328 落地之前**，`core/schema.py` 的內建預設是 S6 例外登記表 R6 裡唯一一筆
**不是** time 語彙的例外，並且已標註為暫時。它跟著 #328 一起消失。

## 這一輪刻意沒做的事

- **catalog ↔ schema 一致性不變量**（決定二）。
- **time 語彙的任何改名**（決定一）。
- **報表 prose 與詞彙表裡的業務詞。** 只有**表格欄名標籤**解凍改成從 schema 取；
  報表其餘內容調整仍凍結（`docs/agents/deliberate-non-goals.md`）。
- **`evaluation_results.json` 以外的落地產物鍵。** 本輪只處理該檔的 `dataset_overview`。
- **`docs/` 底下歷史 plan / spec 對舊鍵名的引用。** 那些是歷史紀錄，不回頭改。

## 舊檔怎麼辦

`evaluation_results.json` 的舊鍵有一支一次性遷移腳本
（`scripts/migrate_evaluation_results_keys.py`，預設 dry run、`--apply` 才寫、可重複執行）。

**讀取端不做靜默 fallback**：`evaluation/report_builder._dataset_overview` 讀到只有舊鍵
的檔案就 raise，訊息裡指向那支腳本。理由是 fallback 要藏的那個失敗正是值得失敗的
那個——`n_items` 讀成 0 會讓 `_resolve_display_k` 把 `"all"` 解成 `map@0`，每個指標查
不到值，跨版本比較報表整張變空白，讀起來像「這個模型什麼都沒排到」而不是「這個檔太舊」。
