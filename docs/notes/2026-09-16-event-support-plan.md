# 讓框架容納「線上廣告推薦的離線訓練與評估」：整體規劃

> 2026-09-16。設計討論的結論在 ADR-0021～0024 與 `CONTEXT.md`；本檔只排「先做哪個、PR 怎麼切、每張拿什麼當證據」。流程判準依 `docs/agents/pipeline-refactor-process.md`。P10（預測品質指標家族，ADR-0024）是後來加的，與 P1–P9 沒有相依。

## 總表（先看這裡）

| PR | 做什麼 | 依據 | 相依 | 行為沒變的證據（對現有部署） | 新行為的證據 |
|---|---|---|---|---|---|
| **P0** | ADR-0021～0023、`CONTEXT.md`、`docs/agents/domain.md` 與 `CLAUDE.md` 原則改寫、`feature_concat.sql` 修兩處 | 規則 1：ADR 零程式碼先進 main | — | 無程式碼改動 | SQL：Spark 建臨時視圖實跑一次，確認輸出含 `snap_date`、`SQLRenderer.build_aligned_select` 不 raise；修正前兩者都失敗 |
| **P1** | 合成的廣告情境示例資料＋conf（小量），供後面每張 PR 實跑 | 新路徑要真的被執行過 | P0 | 不動現有示例 | 資料能跑完 source_etl |
| **P2** | 信賴區間重抽單位設定 | ADR-0023 | — | 單欄 entity 的診斷 JSON 與 main 逐值相同（bootstrap 有固定 seed） | 多欄 entity 測試：區間比拆開重抽時寬；多欄又沒宣告時 log 與報表各有一句警告 |
| **P3** | `event` 角色（同票擋未知的 `schema.columns` 鍵） | ADR-0021 | P1 | 沒宣告 `event` 時 `base_dataset_version` 不變（沿用 `test_versioning.py` 寫死舊雜湊值的測試範本）；示例 e2e digest 相同 | 同組同 item 多筆的測試：label join 不放大、抽樣逐筆決定、同分排序可重現、`"all"` 不截斷；宣告未知角色鍵會報錯（現在會被靜默丟掉）；宣告 `event` 時監控模式在入口被擋下 |
| **P4** | item 清單可從資料數 | 共識（見下） | P1 | 宣告清單的設定版本號不變 | 新組合只警告；A4 改拿前處理器的清單比 |
| **P5** | 特徵表可以多張 | ADR-0022 | P1 | 單一 `feature_table` 設定的版本號與 model_input digest 相同 | 多張特徵表組出正確 model_input；離線推論入口擋下 |
| **P6** | 日期設定支援「起日～迄日」 | 共識 | — | 展開成清單後再算雜湊，版本號與逐一列出相同 | 區間與清單寫法得到同一個版本號 |
| **P7a** | 不適用的診斷自動跳過、報表寫原因 | 共識 Q20 | P3 | 示例部署所有診斷照跑、輸出不變 | 宣告 `event` 或候選數不固定時跳過 |
| **P7b** | evaluation 可評估一段日期 | 共識 | — | 單一日期設定輸出不變 | 區間評估 |
| **P7c** | 分數十等份的點擊率／申辦率表、「是否隨機流量」分開算 | 共識 Q9、Q12、Q13 | P1 | 沒設定就不產出這一段 | 合成資料上實跑 |
| **P7d** | 「至少一正一負」組過濾、熱門度基準線改點擊率（皆選用）、大類從資料欄位取 | 共識 Q17 | — | 預設關，輸出不變 | 開啟後的測試 |
| **P8** | training 產出交接包 | 共識 Q15 | P4、P5 | 現有產物不變，只多一個檔 | 交接包內容與 preprocessor 一致 |
| **P9** | 文件：來源 SQL 的時間正確性責任與範例、`time` 只保證到週、@K 指標在短清單上的解讀、**宣告 `event` 後 mAP 的語意**（按列算，同一個 item 的多列互相競爭名次） | ADR-0021、0022、共識 Q23 | P3、P5 | — | fresh 讀者照文件寫出不偷看的 SQL；讀者答得出「同一廣告曝光兩次，mAP 怎麼算」 |
| **P10** | 預測品質指標家族：`prediction_quality` 的 node、JSON 產物、主報表一段（precision／recall／F1／`pr_auc`／`roc_auc`／分箱表，整體＋per-item） | ADR-0024 | **無**（不依賴 P1–P9；要在合成資料上實跑就排在 P1 之後）。⚠ 與 **P7c** 的十等份表是同一種東西，先做的那張定形狀 | 框架預設關；報表與各 JSON 的**指標值**逐值不變；主指標的計算路徑一行未動。⚠ 各產物的 `config_fingerprint` **會變**（新增兩筆 `COMPUTED_KEYS`），舊產物要重算——ADR-0020 決定二的預期行為，不是 regression | 一次 `groupBy(item, bin)` 的聚合含 `count`／`sum(label)`／`sum(score)`；門檻掃描與分箱表由同一張 bin 表推出；零正例 query 也進得了分母；報表印出 bin 寬與兩段母體差 |

> **更正（2026-09-18，改寫 #376 時）**：上表 P7c、P7d 兩列已過時，以票面為準。
> - 出處：Q17 只對得上「大類從資料欄位取」；Q12 對應「隨機流量」；Q9、Q13 對應十等份表。「一正一負」與「熱門度改點擊率」不是這幾題討論出來的，來自同一場討論裡「evaluation 會有的問題」那份清單。
> - 十等份表併進 #381。
> - 熱門度基準線改成「正例率＝正例數 ÷ 當過候選的次數」（不是 `label_table` 的列數），配每期彙總表，拆成 #397。
> - 隨機流量延後（目前部署沒有隨機流量），拆成 #396（pending）。
> - #376 只剩「一正一負」（只套主指標、熱門度基準線、比較報表，不碰診斷）與「大類從資料欄位取」（只支援 `--post-training`）。

## 相依

```
P0 ──┬── P1 ──┬── P3 ── P7a
     │        ├── P4 ──┐
     │        ├── P5 ──┴── P8
     │        └── P7c
     ├── P2
     ├── P6
     ├── P7b
     └── P7d
                      P9（最後，收 P3／P5 的實際行為）
```

P2、P6、P7b、P7d 不依賴 `event` 或多張特徵表，可以和 P1 平行做。

## 為什麼這樣切

- **P0 先進 main**：後面每張票都要引 ADR 當設計權威（`pipeline-refactor-process.md` 規則 1）。實作時發現 ADR 站不住，要回頭在 ADR 補「更正」，不能只寫在 commit message（規則 2）。
- **P1 放最前面**：`event`、多張特徵表、從資料數 item 清單這三條新路徑，示例的銀行資料一條都走不到。沒有合成資料，這些路徑只能靠單元測試，實跑證據拿不到。
  - ⚠ 另一個 worktree `.worktrees/perf-profile` 目前有未 commit 的 `scripts/generate_synthetic_data.py` 改動。P1 開工前先確認要改同一支還是另寫一支，避免衝突。
- **P3、P4、P5 分開**：三張各自有「比測試綠更強」的證據——沒宣告新設定時版本號不變（規則 4）。合在一起，任何一個版本號變了都分不出是誰造成的。
- **P7 拆四張**：四件事互不相依，各自的驗收條件不同；P7a 必須等 P3（跳過的觸發條件是「有沒有宣告 `event`」）。
- **沒有純結構搬移**：這一輪全是行為新增，規則 3（結構搬移放最後）不適用。
- **P0 的 SQL 修了兩處，不是一處**：`pool_cust` 與最終投影原本都沒有 `snap_date`，但 `conf/base/parameters_feature_etl.yaml` 宣告它是分區欄，`pipelines/source_etl/sql_renderer.py::build_aligned_select` 會 raise「Partition columns missing from SELECT output」。第一版只補了 CTE 之間缺的逗號，語法過了、檔案還是跑不起來——這就是「Spark 解析成功」不能當驗收證據的實例。

## 每張 PR 都要做的

- **相容性**：每張都要證明「沒填新設定時，行為與版本號跟 main 一樣」（共識 Q14）。便宜的先跑：`git diff main..HEAD -- conf/base/` 應只有新增的註解狀態鍵；版本號測試；最後才是示例 e2e digest。
- **mutation**：新邏輯弄壞一行，確認測試紅在目標斷言上（`test-false-green` skill）。
- **收尾 grep 兩份清單**：模組路徑＋符號名（規則 10）。
- **審查留一個視角不吃驗收條件**：「假設 ADR 本身寫錯了，錯在哪」（規則 11）。

## 併入各 PR 的小問題（搜尋時發現）

| 問題 | 位置 | 併入 |
|---|---|---|
| 抽樣分桶只吃 identity 三欄，同組同 item 多筆一起被抽中或丟掉 | `utils/hashing.py::spark_bucket` 的呼叫端 | P3 |
| 同分排序只以 item 決勝 | `utils/ranking.py::rank_by_score_then_item` | P3 |
| `k_values` 的 `"all"` 解析成全資料的 distinct item 數 | `evaluation/metrics_spark.py::_resolve_k_values` | P3 |
| SHAP 圖檔名把非英數字元換成 `_`，`a|b` 與 `a/b` 撞名 | `diagnosis/model/paths.py::safe_name` | P7a |
| mlflow 的 per-item 指標名含特殊字元可能不合法（推測，要查證） | `pipelines/training/steps/experiment_log.py` | P8 |
| test 預測按 `(time, item)` 分區逐一跑，item 多時很慢 | `pipelines/training/nodes.py` 的 test 預測 node | 先量再決定（`pipeline-performance-work.md`），不預先併入 |

診斷模組逐一的判決（照用／要改／該關）存在 session 暫存檔，P7a 開工時重新對程式碼確認，不直接沿用。

## 共識裡沒有進 ADR 的決定

這些決定容易回頭改，不符合寫 ADR 的條件，記在這裡：

- **範圍**：本框架目前只支援離線推論；新設定一律選用，沒填時行為與版本號不變。
- **item**：屬性組合在來源 SQL 拼成一欄；item 清單可以從 train 時段的資料數出來，驗證／測試期間的新 item 只警告。
- **離線推論**：只擋多張特徵表；從資料數 item 清單不擋（A4 改拿前處理器存的清單比）；`event` 忽略。
- **evaluation**：
  - precision@K、recall@K、mAP@K **公式不改**。查證結果：三個公式都等於主流工具的預設定義（trec_eval、ranx、torchmetrics 預設），清單短於 K 時的表現是定義本身；只在文件註明。這一條由 Claude 依使用者「照最簡單的做」決定，使用者未逐字確認。
  - 其他：大類從資料欄位取；分數十等份的點擊率／申辦率表；「是否隨機流量」分開算，組內指標只在按客戶隨機時算；不適用的診斷自動跳過。
  - 更正（2026-09-18）：「是否隨機流量」延後到 #396（pending）；「組內指標只在按客戶隨機時算」改成兩個候選做法（只拿整組都隨機的組，或每組只留隨機的列），開工時定。熱門度基準線的新決定見 #397。

## 還沒解決的風險

- 即時特徵在線上與離線是兩份算法，算得不一樣時模型悄悄變差（ADR-0022 後果）。
- 部署若半年才重訓，期間的新 item 在線上都不認得；建議每月加一段評估區間監控。
- 資料量未量：若登入客戶是百萬級、每人每天數次曝光，一年約十幾億列，負例要大量抽樣。P1 的合成資料量不出生產的成本。
- 部署沒有隨機流量之前，離線分數只能參考。

## 沒做的事

- 沒開 issue：等使用者看過本檔再決定要不要用 `/to-tickets` 拆票。
- 沒估每張 PR 的工時。
- 部署層的數字（每天曝光量、每日批次幾點可用）未知，不影響框架設計。
