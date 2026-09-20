---
status: accepted
date: 2026-09-16
---

# `time` 維持「時段」；到秒的時間放進選用角色 `event`

> **更正（2026-09-20，ADR-0025）**：決定 1、3、5 沿用；下面三處以 ADR-0025 為準。
> - query group 不再寫死是 `time` ＋ `entity`：另一個選用角色 `occasion` 會把它加寬。`event` 仍然不進 query group。
> - 〈考慮過、沒選的做法〉那一節裡，以「代價要老實說：」開頭的那一段（該段已就地加註）說粒度閘 B10 與 evaluation 的重複檢查在宣告 `event` 後「語意會反轉」——**不對**。語意不變，只是「同一筆候選」的鍵多了欄位；要改的只有錯誤訊息。
> - 決定 4 列的「要逐處帶上 `event` 的地方」：identity 與 query group 的欄位收成 `get_schema` 的欄位之後，label join、抽樣分桶、取鍵去重會自動跟上；要逐處改的只剩同分規則與 `k_values` 的 `"all"`。

設計第二種使用情境（線上廣告推薦的離線訓練與評估）時，一列資料是一次曝光，發生在某一秒。直覺上 `time` 應該放那一秒。本 ADR 記的是：**不這樣做**，以及為什麼。

## 背景：`time` 身上背了好幾件事

在示例部署裡，下面這些事剛好都用同一個日期，所以沒人發現它們是分開的：

| `time` 做的事 | 依據 | 廣告情境需要的時間 |
|---|---|---|
| 分組：query group ＝ `time` ＋ `entity` | `pipelines/dataset/nodes.py::filter_groups_with_positives`、`conf/base/parameters_training.yaml` 的 LTR 分組註解 | 日或週 |
| 資料表分區 | `conf/base/catalog.yaml` 各產物的 `partition_cols` | 日或週（到秒會分出極多分區） |
| 切 split | `dataset.*_snap_dates` | 日期區間 |
| 增量處理與續跑 | month plan（`pipelines/dataset/month_plans.py`） | 日或週 |
| 分辨同一組裡的每一筆、往回算特徵 | 無（示例部署用不到） | 秒 |

前四件要「時段」，只有最後一件要「秒」。而「往回算特徵」已決定放在使用者的來源 SQL（ADR-0022），所以**框架自己用到秒的地方，只剩「分辨每一筆」**。

## 決定

1. **`time` 維持「時段」的意思（日或週）。** 框架只保證到週：放日可以跑，但「time 值很多時會變慢、query group 也可能太小」這件事要寫進 `docs/pipelines/dataset.md`（本輪尚未寫）。多處程式假設 time 值不多，例如 `pipelines/dataset/steps/scoping.py::require_months_present` 對 time 做 `distinct().collect()`，docstring 寫「bounded by the month count (typically 12-52)」。**這個「只保證到週」沒有量過**：唯一的依據是上面那個 docstring，以及 `conf/base/parameters_dataset.yaml` 的日期是逐值列舉（日粒度一年要列 365 個值）——那是設定可用性的問題，不是量測結果。
2. **新增選用欄位角色 `event`**，在同一個 `time`、`entity`、`item` 底下有多筆時分辨每一筆；可以是一欄或多欄，例如事件 ID 或到秒的時間戳。
3. **沒宣告 `event` 時，行為與所有版本號跟現在一模一樣。** `event` 不放進 `core/schema.py` 的 `_ROLE_KEYS`（那個 tuple 裡的每個鍵都會進版本雜湊），宣告了才進雜湊。先例有兩個：同檔的 `ENTITY_GROUPING_KEYS` 刻意不放進 `_ROLE_KEYS`；`core/versioning.py::compute_base_dataset_version` 的 `feature_table_fingerprint` 為 `None` 時不進 payload。

   順帶一條，實作時要一起做：`core/schema.py` 目前**不拒絕未知的 `schema.columns` 鍵**（`get_schema` 只保留 `_ROLE_KEYS` 裡的鍵，`validate_schema_config` 也不檢查多餘的鍵），所以在 `event` 落地之前宣告它，會被靜默丟掉——沒有錯誤訊息、版本號也不動。擋未知鍵和加這個角色要在同一張票。
4. **宣告了 `event` 時，它是 identity 的一部分。** 凡是以 identity 認列的地方都要帶上它，包括：
   - label 的 join；
   - 決定性抽樣的分桶——呼叫端 `pipelines/dataset/steps/sampling.py::keep_rows_drawn_under_ratio` 把 identity 三欄餵給 `utils/hashing.py::spark_bucket`，所以同一組同 item 的多筆會落在同一桶，一起被抽中或一起被丟掉。`spark_bucket` 本身收任意欄清單，**其餘呼叫端（entity 級的 val 抽樣、train／dev 切分、inference 的 entity 分桶、診斷抽樣）必須維持原本的鍵，不得帶上 `event`**——它們刻意是 entity 或 query 粒度；
   - 同分排序——`utils/ranking.py::rank_by_score_then_item` 目前只以 item 決勝，同 item 的多筆名次每次跑可能不同（ADR-0020 修過同一類問題）；
   - `evaluation.k_values` 的 `"all"`——目前解析成全部資料的 distinct item 數（`evaluation/metrics_spark.py::_resolve_k_values`），一組比 item 種類還長時會被悄悄截斷。
5. **離線推論忽略 `event`。** 推論的候選由框架以 entity × item 產生，不會重複，也不讀曝光紀錄。因此 `ranked_predictions` 沒有 `event` 欄。

   連帶的一條：evaluation 的**監控模式**是把 `ranked_predictions` 依 identity 接 `label_table`（ADR-0020 bug 10 在那裡對 identity 做重複檢查）。宣告 `event` 時兩邊的識別欄對不上，所以**監控模式在宣告 `event` 時不支援，在 CLI 入口擋下**，理由與 ADR-0022 決定 4 相同：識別欄對不上時接出來的是錯的答案，不是少的答案。post-training 模式不受影響。

## 考慮過、沒選的做法

**`time` 放到秒，另加一個「時段」角色。** 存下來的資料一樣，但上表前四件事的每個使用處都要從 `time` 改成「時段」；而且要推翻 ADR-0017 決定一（`time` 語彙保留 snap date，理由是「絕大多數部署的 `time` 就是一個 snap date」）。框架只有一件事需要秒，不值得為它重新定義 `time`。

**把曝光層的多筆先在來源 SQL 聚合成一列（例如點擊數當 label），框架完全不動。** 這樣同一個 query group 裡 item 仍然唯一，identity 不必動，框架零改動。放棄的原因是它把即時特徵的用處丟掉：同一個 item 的多次曝光發生在不同時刻，當下的即時特徵不同，聚合成一列就只能挑其中一次的值，而「這一刻該不該推這個 item」正是要模型學的事。

代價要老實說：本決定把 `event` 推進 identity，而 identity 是這個 repo 最深的地基（`core/schema.py` 由它推 `identity_columns`，label join、粒度閘 B10、抽樣分桶、evaluation 的重複檢查都吃它）。其中兩處的語意在宣告 `event` 之後會**反轉**——B10 的「列數恆等於 keys」與 ADR-0020 bug 10 的「label_table 在 identity 上重複就 raise」，原本都把「重複」當成上游壞了，宣告 `event` 後重複是合法狀態。實作時要一併改寫這兩處的訊息與文件，否則使用者會收到一個說謊的錯誤訊息。

> **⚠ 上面這一段「語意會反轉」的說法已被 ADR-0025 更正，不要照它實作。** 這兩處檢查**不放寬、不關掉**：宣告 `event` 後，同一個 item 的多次曝光 `event` 不同，本來就不是「同一筆候選」；連 `event` 都相同的兩列仍然是上游壞了，照樣報錯。變的只有比對的鍵（完整的 identity）與錯誤訊息的文字。

**角色名叫 `row_key`。** 用途直白、沒有撞名，但和 `time`／`entity`／`item` 的名詞風格不一致。選 `event` 的兩個代價與對策：

- 程式裡已有數十處以 `"event"` 當結構化 log 的欄位名（例：`core/runner.py` 的 `extra={"event": "node_started"}`）。兩者不互相干擾，但搜尋時會混在一起——程式裡取這個角色時，變數名用 `event_col`。
- 「事件紀錄」原本容易被拿來指往回算特徵的原始紀錄——那個一律改稱「行為紀錄」（見 `CONTEXT.md`）。
