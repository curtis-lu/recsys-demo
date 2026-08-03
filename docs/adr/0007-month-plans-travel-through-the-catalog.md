---
status: accepted
date: 2026-08-03
---

# 月份計畫走 catalog，不走 `parameters`

[ADR-0002](0002-preprocessed-feature-table-incremental.md) 讓 dataset 只處理「設定列出、但尚未落地」的月份。差集**決定**當時就集中在一個 helper，但**套用**沒有：CLI 開跑時算過一次差集（列 partition、印 log），算完丟掉；四個節點在執行期各自從 `parameters` 的 `_existing_snap_dates` 這個底線開頭的 key 重算一次。

代價不是效能，是**可讀性**：`pipeline.py` 上看不出哪些節點是增量的——那個資訊藏在四個函式的前四行。想知道這次 run 要做多少工，得讀節點內文。

**改為：計畫在 CLI 算一次，以三份具名 dataset（`<artifact>_month_plan`）進 catalog，需要的節點把它宣告成一般 input。**

## 為什麼是 catalog 而不是 `parameters`

`parameters` 是設定；`_existing_snap_dates` 裝的是執行期從 metastore 查出來的資料。**鍵名的底線前綴就是這段設計自己的招供**——它需要一個「使用者不會寫、但節點讀得到」的通道，而 `parameters` 剛好被傳給每個節點。

catalog 本來就是那個通道：`parameters` 自己就是用 `catalog.add(name, MemoryDataset(...))` 進去的。改走 catalog 後：

- **增量性寫在 pipeline 定義上。** 有 `*_month_plan` input 的節點是增量的，沒有的不是——一行 `inputs` 就看得出來，接錯線在 diff 上看得見。
- **「哪個節點吃哪份計畫」變成可審閱的事實。** `select_test_keys` 吃 `test_keys_month_plan`、`build_test_model_input` 吃 `test_model_input_month_plan`——兩份不同的計畫（keys 可能已寫、model input 還沒），過去這件事只有讀完兩個函式才知道。
- **忘記接線會 fail-loud。** runner 在任何節點執行前檢查每個 input 拿不拿得到，缺的直接 raise。走 `parameters` 時，忘記注入的後果是 `.get()` 回 `None` → 靜默全量重建，也就是最貴的那個方向。
- **測試不必再組 magic key。** 節點收一個 `SnapDatePlan`，測試就傳一個。

否決 **一份 `dict[str, SnapDatePlan]`**：那樣節點仍得在函式體內寫死自己的資料集名字，沒有型別擋、呼叫端看不見，測試同樣得知道那個字串。具名輸入把它換成 pipeline 定義上的一行。

「哪張表要哪些月」的規則收斂到 `pipelines/dataset/month_plans.py` 的 `build_month_plans`：test 兩張吃 `dataset.test_snap_dates`，`preprocessed_feature_table` 吃全 split 聯集。新增第四張增量表＝該檔加一個條目。

**ADR-0002 的取捨全部不變**：差集邏輯（`plan_incremental_snap_dates`）一行沒動，`--rebuild-dates` 語意不變，A21 不變，`exists() ≠ fresh` 這筆帳仍然掛在那邊。變的只有「決定在哪裡套用」。ADR-0002 §「差集邏輯集中在單一 helper，由四個 node 共用」那段描述的實作位置到此為止。

### 一個順帶買到的東西

`[months]` log 從執行期每節點一行，變成**任何 Spark 工作開始之前**一次三行。ADR-0002 要求「一個會自己決定少做事的 pipeline，必須把它決定不做的事說出來」——仍然滿足，而且說得夠早，範圍設錯可以在花掉時間之前發現。

## `filter_test_model_input` 的防禦性月份過濾刪除

原本 `filter_test_model_input` 除了丟掉零正例的 query group，還重做一次月份過濾。註解宣稱它守的是「切片時被餵到一份由更早、範圍不同的 run 物化出來的上游」。

**查證結果：那個狀態進不來。** 它與上游 `build_test_model_input` 之間的中間產物 `test_model_input_unfiltered` 不在 catalog 設定裡（runner 自動建的 `MemoryDataset`），跨 process 不存在；`--from-node` / `--only-node` 讀不到它時，會把上游拉回來一起跑（`Pipeline._slice_with_expansion`）。

**最壞情況評估**：即使該狀態真的發生，多出來的月份會用**同一份 `preprocessor`** 重算出正確內容再覆蓋 partition（category mappings 只在 train 月份上 fit、整個 run 共用一份，見 ADR-0002）。代價是白做工，不是資料錯。

否決 **改成 fail-loud 斷言**（「發現計畫外的月份就 raise」）：要多付一次 Spark 掃描才知道有沒有計畫外月份，與 [ADR-0006](0006-data-quality-checks-belong-upstream.md)「dataset 閘門維持零掃描」的取捨不一致。

所以 test 的過濾節點改用與 val **相同**的節點函式，`filter_test_model_input` 這個函式刪除。節點名字保留（`--only-node filter_test_model_input` 仍可定址）。

> **給未來的維護者**：看到「test 的過濾沒有月份範圍檢查、val 也沒有」時，那不是漏掉的。它的上游已經 scoped，這裡再過一次只是重述同一件事；真要防的狀態需要一次全掃才驗得到，而這個 pipeline 的閘門刻意不付那個成本。

## 這條 ADR 沒有解決的事

- **節點簽名是樸素的** `(frame, month_plan, ..., parameters)`，沒有 typed parameter object。後續「config 解析下移到單一接縫」的重構會再碰這些簽名，先蓋的架子會被換掉——這是知情的取捨，不是疏漏。
- `build_test_model_input` 仍是 `build_model_input` 的 scoped 變體而不是同一個函式。過濾避不掉：`test_keys` 是持久化 Hive 表、存著所有月份，下游讀回來會拿到全部歷史。讓五個 split 統一吃計畫、非增量的給一份「全部處理」，會讓 pipeline 定義失去「誰是增量」的訊號，並生出四份假計畫。
