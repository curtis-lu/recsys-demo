---
status: accepted
date: 2026-09-13
---

# evaluation bug 修正輪：每條 bug「對的行為」是什麼

> **事實不在這裡。** 15 條 bug 各自的機制、失敗情境、複核證據、行號，在 [`docs/notes/2026-09-09-evaluation-audit.md`](../notes/2026-09-09-evaluation-audit.md)；本份只記**修成什麼樣**、**為什麼不是另一種修法**。
>
> - 這是 [ADR-0018](0018-evaluation-materialize-at-producer.md)〈順序〉的 Phase 0。bug 13 與 15 的修法已在 ADR-0018 決定 5 與〈驗收〉定案，這裡只指過去。
> - 判準前綴同 ADR-0018：「node 規則 N」＝`pipeline-node-design.md`，「flow 規則 N」＝`pipeline-refactor-process.md`，「perf 規則 N」＝`pipeline-performance-work.md`。
> - **路徑寫法**：省略 `src/recsys_tfb/` 前綴；`evaluation/x.py` 指頂層共用庫 `src/recsys_tfb/evaluation/`，pipeline 底下的一律寫全（`pipelines/inference/nodes.py`、`pipelines/evaluation/nodes_spark.py`）。
> - PR 怎麼切不在這裡，交給 `/to-spec`、`/to-ticket`，偏粗。
> - 實作偏離本份，照 flow 規則 2 回頭改這裡。
> - 本份被一輪 fresh-context 審查判 FAIL 後改寫（三條決定的層級或範圍寫錯：bug 6 的 A10、bug 2 的分類表與指紋範圍、bug 9 的理由），改掉的地方寫進各決定正文。

---

## 三個橫切的決定

### 一、報表呈現的凍結解除

`deliberate-non-goals.md` 原有一條「別調報表的呈現——除了欄名標籤那一部分」，它自己寫的刪除條件是「使用者給出涵蓋其餘部分的明確反饋、開始那一輪調整時」。使用者 2026-09-13 明示：evaluation 重構期間報表可以改。條件成立，那一條**隨本份刪除**。

後果有兩層：

- 本份底下 bug 1、3、5、6、8、14 與設計 H 的修法會動報表的說明文字、欄位、數字，不再需要逐句解凍。
- ADR-0018／0019 有三處以「撞報表凍結」為由排除的東西（稽核 B：主報表診斷區改成 registry 診斷；D-2：統一 `guardrail_recall_k` 預設值；J-2：指標家族參數化），理由的前半段失效。三者各自還有第二個理由（B 是搬一整塊報表、J-2 是跨三模組的重構，都超出 bug 輪；D-2 主報表那個鍵本來就是死的，統一等於沒事做），所以**本份維持排除，但標明「凍結已解除，要做另議」**。ADR-0018 四處、ADR-0019 四處、ADR-0017 一處已加更正。

### 二、bug 2 的修法：「算的」與「畫的」分開，指紋只認前者、而且是封閉列舉

**現象**：切片的停止條件是「檔案在不在」，evaluation 的 JSON 路徑只含 model_version 與 snap_date。改了設定再 `--only-node generate_report`，磁碟上舊設定算的 JSON 照樣被讀進報表，退出碼 0。

**不能的修法**：設定一變就全部重跑。使用者要的是「重做成本高的計算先多算一些，`generate_report` 可以依呈現需求多次調整，不必連資料都重算」。

**決定**：把會影響 evaluation 產物的設定鍵切成兩類，只對「算的」做指紋。

| 類別 | 鍵 | 變了要做什麼 |
|---|---|---|
| **算的**（改變 Spark 計算、抽樣，或決定要不要算） | `evaluation.snap_date`、`k_values`、`segment_columns`、`segment_sources`、`item_categories.*`、`baseline.*`、`metric.*`、`diagnosis.*`、`report.diagnostics.*`、**`report.sections.baseline`**、**`report.sections.diagnostics`** | 照錯誤訊息點名的 `--from-node` 重跑（對照表在 `evaluation/config_fingerprint.py` 的 `COMPUTED_KEYS`；不一定是產出那份 JSON 的 node） |
| **畫的**（只改報表長相） | `report.sections.*` 的其餘鍵、`report.display.*` | `--only-node generate_report`，幾秒 |

`report.sections` 裡那兩個鍵歸「算的」，因為它們**決定要不要算**：`baseline: false` 讓 `compute_baseline_metrics` 直接回 stub、`diagnostics: false` 讓 `compute_report_aggregates` 回 stub（bug 1 的決定也依賴前者）。把它們當「畫的」，改了只重繪就會得到缺段報表——正是本決定要消滅的形態。

機制：

1. **「算的」是一份封閉列舉，不是「`evaluation` 子樹扣掉畫的」。** 理由兩個：`evaluation.compare` 是 CLI 在執行期注入 `evaluation` 子樹的（`__main__.py`），雜湊整棵子樹會讓 `--compare` 與不帶 `--compare` 的兩次執行指紋不同、全面假陽性；反過來，有些計算依賴 `evaluation` 以外的鍵——`config_shift` 診斷讀 `dataset.sample_group_keys`、`dataset.sample_ratio`、`dataset.sample_ratio_overrides`、`training.sample_weight_keys`、`training.sample_weights`。所以列舉住在一個地方（暫定 `evaluation/config_fingerprint.py`，純 Python），各 registry 診斷在自己的 contract 宣告額外依賴的鍵（形狀比照 `diagnosis/metric/contract.py` 現有的 `INPUTS` 宣告），該診斷 JSON 的指紋＝共用列舉 ＋ 它自己宣告的鍵。每個「算的」鍵變了該從哪個 node 重跑，對照表住在 `evaluation/config_fingerprint.py` 的 `COMPUTED_KEYS`。
2. 每份落地的 evaluation JSON（指標、baseline、metric CI、report aggregates、各診斷、`evaluation_segment_columns`）帶一個 `config_fingerprint` 鍵：列舉鍵的值排序後序列化再 hash。`snap_date` 已在路徑裡但仍納入。**關掉時的 stub 也帶**——ADR-0018 決定 2 已把 `baseline_metrics` 關閉時的回傳從 `None` 改成 `{"enabled": false}`，就是為了讓它裝得下指紋；沒有指紋的 `null` 會讓下面第 3 點與〈三〉的驗收都跑不過。
   依賴方向：`evaluation/config_fingerprint.py` **不 import `diagnosis/`**；各診斷的額外鍵由它們自己的 contract 宣告，組合發生在 `pipelines/evaluation/` 的 node（`make_diagnosis_node` 寫指紋時、`render_diagnosis_pages` 比對時），跟 `pipeline.py` 現在用 `inputs_for(contract)` 取 node inputs 是同一個方向。
3. 讀 JSON 的 node（`generate_report`、`render_diagnosis_pages`）逐份比對指紋與現在的設定；不合就 raise，訊息點名**哪個鍵變了、該從哪個 node 重跑**（例：「`metric.min_positives` 已變，請 `--from-node compute_metrics`」）。標為前置檢查。
4. 「先多算一些」現況已成立，本份只把它寫成規則：`k_values` 是全集、`display.primary_map_k` 只挑要印的；分群對 `segment_columns` 全算、報表挑要印的；大類與細粒度都算。**不准把「畫的」鍵偷渡進計算層**——例如為了省時間只算 `display.primary_map_k` 那幾個 K。
5. 分類表進 `docs/pipelines/evaluation.md` §7「設定與重跑矩陣」，取代現在那張。
6. **後續更新（flow 規則 2）**：同輪 code review（F7）發現 `--post-training`／監控模式的切換也屬於「算的」——`prepare_eval_data` 依它讀不同的預測表（`training_eval_predictions` vs `ranked_predictions`），但它是 CLI 注入的 run mode，不是 `evaluation.*` 底下的使用者設定，原始封閉列舉沒收它，導致同一 `(model_version, snap_date)` 兩種模式互跑時指紋一致、放行混母體的報表。已補進 `COMPUTED_KEYS` 第一列（`post_training` → `prepare_eval_data`）。

**為什麼不是雜湊進路徑**（`data/evaluation/<mv>/<snap>/<雜湊>/…`）：舊檔自然失效是它唯一的好處；代價是每改一次設定多一棵目錄樹，`--compare-only`、`scripts/render_diagnosis.py`、manifest 的 artifacts 清單全部要學會挑目錄。指紋在檔內，路徑不動，讀者只多一個 raise。

**與 ADR-0018 的關係**：決定 2 新落地的 `evaluation_metrics`、`baseline_metrics` 照同一套帶指紋。決定 1 的 `month_plans` 解的是「分區不存在」，本決定解的是「存在但陳舊」，互補不重疊。

### 三、每條 bug 的驗收

- 每條修法的 PR 帶**一條從複核重現腳本改來的測試**，先紅後綠（複核時的腳本：bug 2、3、4、8、9 純 Python，5、6、12 一次 Spark；腳本本身不在 repo，notes 有涵蓋範圍）。
- 修法會改報表數字或欄位的（3、5、6、8、14、H），再拿 noise floor 比一次 HTML（flow 規則 9），差異必須只落在該 bug 宣稱要改的地方。
- 決定二那張表要實證一次：設 `report.sections.baseline: false` 跑一遍、翻回 `true` 只重繪，必須 raise 而不是出缺段報表。

---

## 逐條決定

格式：**現象一句 → 決定 → 為什麼不是另一邊 → 碰到什麼**。

### bug 1：baseline 找不到歷史就偷用當月答案，報表還印一句假話

**決定**：`_lookback_window`（`evaluation/baselines.py`）查無資料時 **raise**，訊息說明視窗範圍與 `label_table` 實際有的月份。只有 `report.sections.baseline: false` 時才不算、不 raise。`build_baseline_section` 那句寫死「以過去 12 個月的歷史購買計數重排」的文字，改成從 `baseline.lookback_months` 組出來。

**為什麼不是退化成 stub**：使用者裁定 baseline 是重要資訊——「除非使用者明確不出該數字，否則沒有填入的話應該要 raise」。stub 會讓「沒算」看起來像「算了但沒東西」。

**碰到什麼**：不要拿資料閘 B2 擋它（B2 管特徵洩漏，這是 baseline 的計分視窗）。標為前置檢查（上游 `label_table` 沒有足夠月份）。

### bug 2：見〈橫切決定二〉。

### bug 3：`n_queries` 在同一份 HTML 裡被標成兩個相反的意思

**決定**：標籤改對——`n_queries` 是「全部 query 數」（`compute_dataset_overview` 的定義）；`build_overview_section` 與 `build_completeness_section` 多印一列「有正例的 query 數 ＝ `n_queries − n_excluded_queries`」。

**為什麼不是改定義**：`n_queries` 的定義跟 docstring 一致，錯的是標籤。

### bug 4：比較報表某側缺這個 key 就當 0，印出捏造的 Δ

**決定**：`_compute_delta`／`_compute_nested_delta`（`evaluation/compare.py`）只在**兩側都有** key 時算 Δ，否則不產生 Δ 鍵；呈現層那格留空。若某側 `overall` 整個是空 dict，Δ 欄整欄空，表格上方加一行「B 側無可比的 query（全部零正例）」。

**為什麼不是保留當 0 加標記**：0 會被讀成「對照組是 0」，標記救不回來。

**碰到什麼**：per-item 那種缺 key 的觸發前提是 bug 7（兩側母體不同），修 7 之後這條主要剩 overall 整側空的情況——仍要修，因為那個情況跟 7 無關。

### bug 5：當月零正例的 item 從 macro 平均的分母消失

**決定**：**揭露，不改定義**。`macro` 那幾張表的表頭或第一列印「參與 macro 的 item 數 N／全部 M」；分母仍是「有正例的 item 數」。

**為什麼不是零正例以 0 計入**：那是改指標定義，不是修 bug；而且會把「沒資料」跟「表現差」混成同一個 0。

**碰到什麼**：做成「零正例就擋」會撞 `deliberate-non-goals.md` 的資料閘 B3 那條；揭露式不撞。

### bug 6：segment 對不到的列變成一個叫 "None" 的客群，等權進平均

這條分兩半：**來源錯**與 **NULL 怎麼算**。

**來源（決定）**：segment 跟著**該模式的母體**走，不再寫死 `sample_pool`。

| 模式 | 母體表 | 為什麼 |
|---|---|---|
| `--post-training` | `sample_pool` | 測試集從它抽的 |
| 監控 | `inference_population` | 被評分的就是它；`sample_pool` 是訓練階段的東西 |

兩者都以 `(entity, time)` 當 key。`segment_columns` 列欄名即可；`segment_sources.<欄>.table` 改成**可選的覆寫**，留給「segment 在另一張外部表」的情況（設定檔註解掉的 `holding_combo` 範例就是）。

**檢查放哪一層（本份第一版寫錯，審查糾正）**：「母體表有沒有這一欄」要開 metastore 看表，而 A 系列 predicate 只收 `parameters`、在 Spark 起來之前跑（`__main__.py` 明寫「Checked before Spark starts」），而且 A 系列看不到 `--post-training` 這個模式。所以：

- 不變量 A10（`segment_columns_without_source`）**縮成純 config 檢查**：每個 `segment_columns` 若有 `segment_sources` 覆寫，覆寫必須完整（table、key_columns、segment_column）。沒有覆寫的欄不再是錯——那代表「從該模式的母體表取」。
- 「母體表有沒有這一欄」是**執行期的 metadata 讀取**（`spark.table(...).columns`，只碰 metastore、不掃資料），**由 `prepare_eval_data` 做一次**——它是唯一知道這次是哪個模式、也是唯一做 segment join 的地方。它把「這次實際 join 進來的 segment 欄」寫成一份小 JSON（新 catalog 條目 `evaluation_segment_columns`，`data/evaluation/${model_version}/${snap_date}/segment_columns.json`，是 `prepare_eval_data` 的第二個 output）。需要分群的消費者（`compute_metrics`、`compute_report_aggregates`、`draw_diagnosis_sample_node`、`restrict_to_common`）讀那份 JSON 決定要對哪些欄分群，**不看 frame 的欄位、也不各自去查母體表**。**不 raise**，缺欄就走下面「母體表沒有這欄」那條路。

  為什麼落地成 JSON 而不是各消費者自己查母體表（本份第二版的寫法，審查指出走不通）：`--compare-only` 那條路沒有 `prepare_eval_data`、`--post-training` 旗標在那條路上是啞的，消費者根本不知道該查哪張母體表；而它讀回來的 enriched 分區裡 segment 欄是 persist 當時 join 的，跟同時寫下的 JSON 一致。落地之後它也符合 node 規則 1（撈得出來看：出事時直接開那份檔）。這個機制目前住在 `evaluation/segments.py`（`join_segment_sources` 所在），ADR-0019 方案 α 落地後才搬進 `pipelines/evaluation/steps/`。

  **一個 collision 要寫明**：A10 縮成純 config 檢查之後，「設定裡把欄名打錯」與「母體表本來就沒有這欄」會走同一條路——都變成「母體表無此欄」。分辨靠訊息：報表那句與 log 的 WARN 都要印出**表名與欄名**（「`ml_recsys.inference_population` 無欄 `cust_segment_typ`」），打錯字的人看得出來。要擋在 config 層得開 Spark，A 系列做不到，這是接受的代價。

**NULL（決定）**，兩種情況分開：

- 母體表**有**這欄、某些列是 NULL（或覆寫表對不到）：這群改名 `(unmatched)`，**不進** macro 平均，分群表印它的 query 數與佔比。
- 母體表**沒有**這欄：per-segment 對這欄不算，報表寫一句「母體表無此欄」。**不 raise**——監控母體是使用者自訂的表，缺一個分群欄不該擋住整份月報。

**「有沒有這欄」要看母體表，不能看 frame 的欄位**：ADR-0018 決定 1 之後 `enriched_eval_predictions` 是兩種模式共用的一張表、schema 是聯集——post-training 那次 join 進來的 `cust_segment_typ`，在監控模式的 frame 裡照樣存在、只是全 NULL。現在 `compute_dataset_overview` 與 per-segment 聚合挑「active segment 欄」的方式是「`segment_columns` 裡第一個出現在 frame 欄位裡的」，那會把全 NULL 的欄挑進來、長出一個假的 `(unmatched)` 桶。所以「這次有哪些 segment 欄可用」由 `prepare_eval_data` 寫進 `evaluation_segment_columns`，消費者拿那份清單，不看 frame。ADR-0018 決定 1 對這一處的說明已同步更正。

**為什麼不是覆蓋率門檻**：門檻值是規格真空；生產 `sample_pool` 對 `inference_population` 的覆蓋率 repo 裡查不到。先讓數字自己說話。

**碰到什麼**：示例 ETL 的 `conf/sql/etl/inference_population/inference_population.sql` 只有 `(snap_date, cust_id)`，沒有 `cust_segment_typ`——示例環境的監控模式會走「母體表無此欄」那條路。要在示例裡看到分群，那支 SQL 要多帶那一欄，那是示例資料的事，不是框架的事。

### bug 7：比較模式 B 側用自己落地時的舊 label，A 側用現在的

**決定**：`restrict_to_common` 對 B 側**一律丟掉自帶的 label、重新 join 現在的 `label_table`**。`evaluation/comparison/restrict.py` 的 docstring「both sides are scored against the same ground truth」因此變成真的。

**為什麼不是記錄來源就好**：「同一份答案」是比較報表存在的前提。多一次 join 的代價，在 ADR-0018 物化之後是讀一張表。

### bug 8：大類只有 3 類，報表照印 @4、@5

**決定**：每個粒度**只顯示 K ≤ 該粒度 item 數**的欄，外加 `@all`——是對 `display.*_k` 清單做**過濾**，不是重新生成清單。預設 `primary_map_k: [1, 3, 5, "all"]` 遇到 3 個大類印 @1、@3、@all；`guardrail_recall_k: [1, 2, 3, 4, 5]` 印 @1、@2、@3。計算層不動（`k_values` 全集照算）。

**為什麼不是 precision 分母改 min(K, n)**：改定義。

### bug 9：`render_diagnosis_pages` 用 varargs 收輸入、按檔名讀磁碟

**決定**：它**直接用傳進來的診斷結果**畫頁（inputs 就是各診斷 JSON 經 catalog 讀回的 dict），不再自己從磁碟讀。簽章保留 `(parameters, *diagnosis_results)`——registry 的長度是動態的，位置綁定下 varargs 躲不掉——但檢查從「個數」升級成**內容**：第 i 個結果必須帶著 `DIAGNOSES[i]` 的名字，`parameters` 必須含 `evaluation` 鍵。接線順序錯了會在第一個對不上的名字炸掉，不是靠「剛好都是 dict」矇過去。

名字從哪來（審查指出本份第一版寫「compute 輸出本來就帶名字」是假的——四個 `_compute.py` 的回傳與 stub 路徑都沒有）：**在 `make_diagnosis_node(name)` 這個工廠裡加**，它是所有診斷 node 唯一的出口，包含 disabled 時的 stub 路徑；每份診斷 JSON 因此多一個 `"diagnosis": name` 鍵。一個地方加一行，四個 `_compute.py` 不動。

**這條修的是什麼、不是什麼**（本份第一版寫「一次修掉兩件事」，審查糾正）：它修的是「接錯不炸」與「畫的不是這次算的」。「撿到上次設定的 JSON」由〈橫切決定二〉的指紋修——這些 dict 是 Runner 從 catalog 讀回來的，`render_diagnosis_pages` 就是決定二說的「讀 JSON 的 node」，指紋比對在它身上。`known-pitfalls.md` §12 反對的是「個數對得上就算修好」，本決定用內容檢查回應它；varargs 本身留下是 registry 形狀的取捨，不是 §12 說的那種修復。

**碰到什麼**：現行「按檔名讀」有一個副作用是防護——忘了給新診斷補 catalog 條目時，`DataCatalog` 會靜默生一個 MemoryDataset、頁照畫、JSON 不落地。改吃 inputs 之後這個防護消失，補一條測試：`DIAGNOSES` 裡每個名字都要有 `evaluation_<name>` 的 catalog 條目。ADR-0018 決定 5 在監控模式用 `no_diagnosis_pages` stub 供空清單，跟本決定互不干擾。

### bug 10：`label_table` 對同一個 key 有兩列，候選集膨脹、rank 全錯、沒人會發現

**決定**：`prepare_eval_data` 在 LEFT JOIN 之前檢查 `label_table` 在 `identity_columns` 上有無重複，有就 raise。**放在 node 裡、標為前置檢查，不進 `core/consistency.py`。** 一次 groupBy，只掃這個月。

**為什麼不是 `dropDuplicates`**：那等於隨便挑一個答案，而且列數會對得上，比原問題更難發現（複核明確否決稽核的這個建議）。

**為什麼不進 `consistency.py`**：`deliberate-non-goals.md` 的「`inference_population` 的唯一性沒有寫進 `consistency.py`」是同一個形狀的決策——使用者自定義來源表的品質保證不進不變量模組。本份沿用那條的邊界，但**不**沿用它「靠 source_etl」的做法：`label_table` 不一定經過 source_etl，所以在使用端斷言（perf 規則 2：把檢查搬到真正依賴它的地方）。

### bug 11：rank 的同分規則沒定義，而且同一條未定義的規則寫了兩次

**現象**：`rank_predictions`（`pipelines/inference/nodes.py`）與 `rank_within_query`（`evaluation/metrics_spark.py`）的 window 都是「依分數降冪 `row_number()`」，語意完全相同——問題不是兩套規則互相矛盾，是同分時的順序**沒定義**，而這條沒定義的規則被寫了兩次，每次執行、每個地方都可能給出不同的名次。

**決定**：同分打破規則統一成「分數相同時按 item 名升冪」，寫在**一個**共用函式裡，兩處都呼叫它。函式落點：**新的 `utils/ranking.py`**，純 Spark 運算式、零專案 import——比照 `utils/hashing.py::spark_bucket`（純 Spark helper 住 `utils/` 的現成先例）。不放 `evaluation/`：repo 目前沒有任何 inference → evaluation 的 import，不為一個 window 開這條邊。`evaluation/metrics_spark.py::rank_within_query` 改成呼叫它。

**為什麼不是 evaluation 直接用上游的 rank**：比較模式裁切後候選集會縮，那時必須重排，「直接用」無法無條件套用。

**碰到什麼**：inference 已落地的 `ranked_predictions` 同分列的 rank 值會變。inference 尚未部署，不需要遷移。

### bug 12：`per_item_segment` 的 key 用底線串接會撞號

**決定**：改成兩層 dict `{item: {segment: …}}`；`aggregate_per_item` 的產出、`report_builder` 的讀取、比較報表跟著改。

**為什麼不是換分隔符**：只是把撞號機率變小，沒消掉。

**碰到什麼**：指標 JSON 是 ADR-0018 決定 2 新落地的，現在改形狀不用遷移。

### bug 13：監控模式跑到診斷就炸

**決定**：ADR-0018 決定 5——監控模式不組 registry 診斷 node；inference 多寫 `score_uncalibrated`；診斷守衛改成「欄位存在且抽樣非全 NULL」。

### bug 14：coverage 數字跟實際保留的母體不是同一個量

**決定**：coverage **從裁切後的 frame 數 query group**（`restrict_to_common` 拿 `common_universe` 裁完的結果來數），單位不變（[ADR-0015](0015-compare-population-counted-in-query-groups.md)：跟 mAP 同分母），算法跟裁切同一套（`left_semi`，不再另外 `intersect`）。順便去掉 node 自己多做的那幾次 `collect`，讓 `common_universe` 把它已算出的 `a_items`／`b_items` 一起回傳。

**為什麼不是只去重、數字不動**：數字現在就是用被同一個 repo 判定為錯誤語意的運算算出來的；變的方向是「跟實際一致」。

**碰到什麼**：`report_comparison.html` 的 coverage 數字會跳（有 NULL 鍵、或兩側月份不同時）。ADR-0015 已經記過一次「數字會跳是預期的」，本份是第二次，理由寫在同一段旁邊。

### bug 15：同一個 model_version 換模式寫同一張表，`rank` INT vs BIGINT 撞 `_evolve_schema`

**決定**：ADR-0018〈驗收〉——`prepare_eval_data` 補出來的 `rank` cast 成與 `ranked_predictions` 宣告一致的型別（BIGINT）。

**實作時補的一條（2026-09-13，#341）**：兩種模式來源不同的欄不只 `rank`，還有 `label`——post-training 讀 `training_eval_predictions`（宣告 INT），監控模式讀使用者自訂的 `label_table`（示例環境的合成資料是 BIGINT）。本機先跑 post-training 再跑監控，`persist_eval_predictions` 撞 `label: DataFrame=bigint vs table=int`，跟 `rank` 同一個形態。所以 `prepare_eval_data` 也把 `label` cast 成 INT，理由同上一句：跟唯一宣告過 `label` 型別、而且會進 evaluation 的表（`training_eval_predictions`）一致。選 INT 不選 BIGINT：post-training 那側不變；label 在這個框架是整數相關度，INT 裝得下。

**一次性的代價（審查指出，原文沒寫）**：`_evolve_schema` 對同名不同型直接 raise。所以本改動之前寫過的 `enriched_eval_predictions`——`--post-training` 寫的 `rank` 是 INT、監控模式寫的 `label` 沿用 `label_table` 的型別——升級後第一次寫入就撞型別，**同一模式重跑也會**，不是只有換模式。解法是 DROP 這張表、再重跑需要的月份。這張表跨所有 model_version 與月份，DROP 之後到重跑之前，`--compare-only` 讀不到任何 Model A。不寫遷移 script，因為表的內容全部可由重跑 evaluation 重建。

### 設計 H（併入本輪）：`evaluation.metric` 有多份讀取程式碼，`metric.k` 只有 CI 那邊讀

**現象**：`metric.k` 設成非 null 時，metric CI 會截斷在 k、主指標的點估不會，而概覽段的註腳宣稱兩者相同。讀 `evaluation.metric` 的程式碼：`src/` 四個讀取點（`metrics_spark.py`、`diagnosis/metric/uncertainty.py`、`diagnosis/metric/_common.py`、`report_builder.py`）**形狀各異**——有的讀 `k` 有的不讀、缺鍵時的 fallback 不同；`scripts/` 五份 `metric_params()` 彼此逐字相同、但跟 `_common.py` 那份在 `or 0.0` 的 fallback 上語意不同。

**決定**：

- `metric_params()` 只留**一份**，放在 `evaluation/metrics.py`——它是純 numpy 的葉模組（import 只有 `logging`、`typing`、`numpy`），`diagnosis/metric/*`、`metrics_spark.py` 與五支 `scripts/` 已經 import 它，`report_builder.py` 加一行 import 也不會有循環。該模組 docstring 現在寫「範圍刻意只放 `tune_hyperparameters` 需要的原語」，一併改。缺鍵時的 fallback 統一成 `conf/base/parameters_evaluation.yaml` 註解裡寫的「等價現行為」的預設值（`weight_alpha=0`、`k=null`、`min_positives=0`、`shrinkage_k=0`）。
- **`metric.k` 與 `k_values` 是兩個獨立的軸**：`k_values` 是 `@K` 家族（map@K、precision@K、recall@K）的 K 網格，全集照算、報表挑要印的；`metric.k` 是主指標家族（per-item macro 的點估**與** CI）的截斷深度，null＝不截斷。`metric.k` 不必在 `k_values` 裡。`metrics_spark.py` 因此也讀 `k`，讓點估與 CI 用同一個截斷。
- `build_overview_section` 的 CI 註腳從 `metric.k` 的值組出來（null 時說「不截斷」，非 null 時說「截斷在 k」），不再寫死「相同」。

**為什麼不是只改註腳**：註腳改對了，多份複製品照樣各自漂移。

**碰到什麼**：ADR-0019 決定 5 不動這條（結構那邊只管搬檔）。A15 只驗 `metric.*` 值域，不驗一致性；不加新不變量，因為讀取點收成一份之後沒有東西可以不一致。

---

## 考慮過但否決的（彙整）

| 選項 | 為什麼否決 |
|---|---|
| 設定一變就整條重跑 | 使用者明示不接受；「畫的」鍵不該碰資料 |
| 指紋雜湊進路徑 | 每改一次設定多一棵目錄樹，所有讀目錄的工具要跟著改 |
| 指紋＝`evaluation` 子樹整棵雜湊 | `evaluation.compare` 是執行期注入，會全面假陽性；診斷還依賴子樹以外的鍵 |
| `report.sections.baseline`／`diagnostics` 當「畫的」 | 它們決定要不要算，改了只重繪會出缺段報表 |
| baseline 視窗空時退化成 stub | 使用者裁定 baseline 是重要資訊，沒算出來要 raise |
| 缺 key 當 0 加標記（bug 4） | 0 會被讀成對照組是 0 |
| 零正例 item 以 0 計入 macro（bug 5） | 改指標定義；混淆「沒資料」與「表現差」 |
| 「母體表有沒有這欄」做成 A 系列不變量（bug 6） | A 系列在 Spark 起來前跑、只看 config、看不到模式 |
| segment 覆蓋率門檻（bug 6） | 門檻是規格真空 |
| 只記錄 label 來源不重 join（bug 7） | 違反比較報表的前提 |
| precision 分母 min(K, n)（bug 8） | 改定義 |
| `render_diagnosis_pages` 只加個數檢查（bug 9） | `parameters` 跟診斷結果都是 dict，個數對得上照樣接錯 |
| `dropDuplicates` label_table（bug 10） | 靜默挑一個答案 |
| 唯一性進 `consistency.py`（bug 10） | 同 `inference_population` 那條刻意例外的邊界 |
| evaluation 直接用上游 rank（bug 11） | 比較模式裁切後必須重排 |
| 共用 tiebreaker 放 `evaluation/`（bug 11） | 會開出第一條 inference → evaluation 的 import 邊 |
| 換分隔符（bug 12） | 沒消掉撞號 |
| coverage 只去重不改算法（bug 14） | 數字現在就錯 |

---

## 這條 ADR 沒有解決的事（凍結解除後可另議）

- **稽核 B**：主報表診斷區改成第 N＋1 項 registry 診斷。凍結不再是理由；剩下的理由是它搬一整塊報表、屬於結構重整，不屬於 bug 輪。要做另開一票。
- **D-2**：`guardrail_recall_k` 主報表那半是死的（ADR-0019 決定 5 刪 `rec_ks`），比較報表用自己的預設。凍結解除後仍沒有理由動它。
- **J-2**：指標家族參數化。跨三模組的重構，另一輪。
- **bug 6 的覆蓋率門檻**：等生產有數字。

---

## 出處

- 事實：[`docs/notes/2026-09-09-evaluation-audit.md`](../notes/2026-09-09-evaluation-audit.md)。
- 決定：2026-09-13 逐條討論，共 19 題；「算的／畫的」分開與「segment 跟著母體走」兩條是使用者在討論中改寫的方向，不是稽核或複核提出的。
- 順序與驗證框架：[ADR-0018](0018-evaluation-materialize-at-producer.md)〈順序〉〈驗收〉。
- 本份第一版的三處層級錯誤（bug 6 的 A10、bug 2 的分類表與指紋範圍、bug 9 的理由）由 2026-09-13 的 fresh-context 審查抓到，已改寫進正文。
