---
status: accepted
date: 2026-09-23
---

# item 宣告成多欄時，讀入就拼成一欄 `item`；identity 不加寬

`schema.columns.item` 可以宣告多欄（例：`[campaign_id, creative_format]`）。框架在讀使用者的表時，把各欄的值用 `-` 拼成一欄，欄名固定叫 `item`；之後的程式照今天「item 是一欄」的方式走。使用者不必再在來源 SQL 自己拼（#394）。

這推翻了兩個舊說法：`CONTEXT.md` 的 **item** 原本寫「恆為一欄；由多個屬性組成時，在來源 SQL 先拼成一欄」，ADR-0022 表格第三列寫「item 恆為一欄，多屬性在來源 SQL 拼起來」。

## 背景：模型本來就只看組合

廣告示例的 item 是「活動 × 素材格式」。今天的做法是在來源 SQL 裡自己拼：`label_table.sql` 與 `feature_realtime.sql` 各寫一次 `concat(campaign_id, '-', creative_format) AS ad_creative`，`sample_pool.sql` 從 `label_table` 取拼好的那一欄。使用者要的是直接寫：

```yaml
schema:
  columns:
    item: [campaign_id, creative_format]
```

問題是「多欄」要多到哪一層。有兩種讀法：

```
讀法一：identity 真的變多欄
  identity = time, entity…, campaign_id, creative_format
  → 每個 join、分組、排名、分區、輸出都要處理「item 是好幾欄」

讀法二：讀入時拼成一欄（本 ADR）
  使用者的表 (campaign_id, creative_format) ──拼──▶ item = "c01-banner"
  identity = time, entity…, item      ← 跟今天一樣
```

模型把 item 當成**一個**類別特徵，詞表是 `schema.categorical_values` 裡的組合清單（`pipelines/dataset/steps/feature_columns.py`）。所以就算選讀法一，要餵給模型時還是得拼成一欄；讀法一多出來的只有「在框架內部到處帶著好幾欄」的成本。

## 決定

1. **讀入時拼成一欄，identity 仍是一欄。** 拼出的欄名固定是 `item`，使用者不取名：item 是角色，不需要替它想名字。`get_schema` 回傳的 `item` 就是 `"item"`，identity、query group、排名、分區都照今天的程式走；組成它的原欄在 `get_schema` 的 `item_source_columns`。
   - **只寫一欄時什麼都不做。** `item: prod_name` 與 `item: [prod_name]` 都不拼、欄名不變，版本 ID 與今天相同（`get_schema_for_hash` 對單欄仍輸出字串）。多欄時輸出宣告的清單，所以跟「一欄真的叫 `item`」的部署分得開。
2. **分隔字元固定 `-`，只在兩個不同組合拼出同一個值時擋。** 值本身含 `-`（例如 `cmp-01`）是常態，不該擋。會出錯的只有撞值：`a-b`＋`c` 與 `a`＋`b-c` 都拼成 `a-b-c`，兩個不同的 item 被當成同一個。這個檢查是 B15，放在 dataset 資料閘，和 B1 一起掃 `sample_pool`／`label_table`（`core/consistency.py`）。
3. **一條規則切開三種東西：**

   ```
   使用者給的表 → 帶原欄，框架拼
     sample_pool、label_table、候選層級特徵表、評估的外部比較表（compare kind: external_hive）

   框架自己寫的表 → 只帶拼好的 item
     model_input、評估預測表、推論結果表……

   conf 裡的值 → 寫拼好的值（c01-banner）
     categorical_values.item、inference.products、抽樣設定鍵、sample_weight_keys 的鍵、大類 mapping
   ```

   拼完之後原欄就丟掉。候選層級特徵表的非鍵欄全部會變成特徵，原欄留著的話，`campaign_id`、`creative_format` 會變成兩個新特徵——這跟今天在 SQL 拼的行為不同，也不是本 ADR 要的（屬性各自當特徵見 #394 的 Out of Scope）。
4. **擋掉會撞名的設定。** 宣告多欄時，使用者的表若已經有一欄叫 `item` 就 raise，否則拼出的欄會蓋掉它。原欄也不能是其他角色的欄（例如 `item: [cust_id, prod]`）：拼完會把原欄丟掉，等於把 entity 的欄丟掉。後者在 `validate_schema_config` 擋，前者在拼欄的地方擋（每張表各擋一次，訊息點名是哪張表；B16）。
   同一個原欄在不同表的型別也要一致（B17，dataset 資料閘查 `sample_pool`、`label_table`、候選層級特徵表）：每一格都先轉成文字再拼，同一個值在一張表是整數 `7`、另一張是小數 `7.0`，會拼成兩個不同的 item。拼之前 Spark 會替 join 轉型、接得上；拼之後就接不上，候選層級特徵整批變成 NULL，而且沒有任何錯誤。
5. **拼欄只有一個函式，每個讀使用者表的入口都呼叫它**（`utils/item_columns.py`）。只用 Spark 內建函式（`concat`、`cast`），不用 UDF。用 `concat` 而不是 `concat_ws`：`concat_ws` 會跳過 null 那一格，`a-b` ＋ null 會拼成 `a-b`，撞上真的 `a` ＋ `b`；`concat` 遇到 null 整個值就是 null，跟單欄時 item 是 null 一樣。漏掉一個用到 item 的入口時，那張表沒有 `item` 欄，接 identity 的 join 會直接報找不到欄——會失敗，不會悄悄算錯。不用到 item 的讀者（例如 evaluation 從 `sample_pool` 取分群欄）拿到的是沒拼過的表，原欄還在；拿原欄當分群欄是把候選層級的屬性當成 entity 層級，這與單欄時拿任何候選層級的欄當分群欄一樣是錯的用法，不是本 ADR 帶進來的。

## 考慮過、沒選的做法

**讓 identity 真的變多欄（讀法一）。** 最「誠實」：輸出可以帶原欄、同分可以逐欄照數字比。但模型仍然只吃一個拼好的類別值，所以每一處都要同時處理「多欄的 identity」與「一欄的特徵」。2026-09-23 盤點：讀法一約要改 `src/` 70–90 處、`scripts/` 44 處；讀法二約 15–25 處。換來的東西（輸出拆回原欄）目前沒有人要：痛點在輸入，今天的輸出本來就是一欄。

**分隔字元可設定。** 只有真的撞值才需要換，這很少見；撞了使用者可以在來源 SQL 改值。真的遇到再加。

**值裡含 `-` 就擋。** 最簡單，但會擋掉大量沒有問題的資料（`cmp-01`、`2025-q4`）。真正會壞的只有撞值，所以只擋撞值。

**在 catalog 那一層拼（包一層 dataset，`load()` 回來就已經拼好）。** 一個地方，新的入口自動涵蓋。沒選：撞值檢查要看拼之前的原欄，包在 dataset 裡就拿不到；而 catalog 條目不知道 parameters，要把 schema 塞進 IO 層。漏掉入口的代價是報錯、不是算錯（決定 5），所以用「每個入口呼叫同一個函式」就夠。

## 後果

- **推論結果表只有拼好的 `item`。** 之後若要拆回原欄，**不能靠 split 字串**：決定 2 允許值本身含 `-`，`cmp-01-banner` 拆不回唯一的原值；得把原欄一路帶到輸出。
- **同分時照拼好的字串比**，不逐欄照數字大小比（`CONTEXT.md` 的 **rank**）。同分規則只為名次可重現，照字串比同樣可重現。
- **conf 仍要列出所有組合**（`categorical_values.item`）。模型的類別編號表、離線推論的全網格（每個 entity × 整份清單）都靠這份清單。改成從資料數是 #379；本 ADR 拼成一欄之後，#379 數的就是 `item` 那一欄，不必知道它是多欄。
- **外部比較表也要帶原欄。** item 宣告成多欄時，`compare kind: external_hive` 的 `columns` 要對應每一個原欄，框架接好之後才套 `prod_mapping`（決定 3）。外部表若只有它自己的一欄 item id，這個部署目前沒辦法設定它。2026-09-23 考慮過「寫原欄就接、直接寫 `item` 就不接」兩種都收，使用者決定照決定 3 只收原欄。
- **撞值檢查只看 dataset 讀到的期間。** 熱門度基準線回看到 dataset 期間之前的那一段不查：撞值要欄值恰好互相吻合，極少見，不為它每次評估多掃一次來源表。
- **`scripts/` 底下直接讀來源表的診斷腳本不拼欄。** 它們在多欄部署上會報找不到 `item` 欄；要用時在腳本讀表之後呼叫同一個函式。例外是抽樣設定工具 `scripts/sampling_overrides_editor.py`：它不是診斷腳本，而是推導 `sample_group_keys`／`sample_ratio_overrides` 的工具（這兩個設定寫的正是 `item` 與拼好的值），所以它讀 `sample_pool` 之後會拼。
- ADR-0025 說「之後任何加寬 identity 的改動（例如 #394 的 item 多欄）都不得調動既有欄位的相對順序」。本 ADR 不加寬 identity，所以這條對 #394 不適用；它對之後真的加寬 identity 的改動仍然成立。
