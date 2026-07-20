# Evaluation 診斷重構：計畫索引

把 evaluation pipeline 的診斷層換成五項模組化診斷。**系統忠實呈現資料並說明每個數字的邊界，不下結論、不給處方**——判斷是讀者的工作。

## 讀的順序

1. **`00-shared-context.md`** — 開工前必讀。五項診斷的邏輯架構、檔案結構、持久化邊界、共同統計限制、診斷契約。六份計畫都依賴它，都不複述它。
2. 然後照編號執行下面六份。

## 進度（最後更新 2026-07-20）

| Plan | 狀態 |
|---|---|
| **0 地基** | ✅ **已 merge（PR #109）**。清場＋抽樣加權＋`recsys_tfb/report/` 五個檔全部在 main |
| **1 config_shift** | ✅ **實作完成**，branch `feat/diag-config-shift`。2.1–2.7 全部完成；**2.8 樣板檢查點使用者已通過**（2026-07-20 公司環境實跑成功，回饋見下方三則）；**2.9 offset 換算量表已追加**（`810cce3`，見下方） |
| **1.5 接線層重構** | ✅ **完成**（`f91bdf6`…`5bfbe45`，7 個 task）。計畫＋執行紀錄見 `02b-plan-1.5-wiring.md` |
| **2 item_ability ＋ model_capacity** | ✅ **完成**（`77ae013`…`b008588`，6 個 task）。含 `contract.INPUTS` 機制與三項延後案結清 |
| 3–5 | **可以開工了** |

### Plan 2 交付了什麼（2026-07-20 收工）

`DIAGNOSES` 從一項變三項：`config_shift` → `item_ability` → `model_capacity`。

| 交付 | 內容 |
|---|---|
| `item_ability` | within-item AUC（raw vs query-centered 對照）＋ sort-once bootstrap；`discrimination.py` 退場 |
| `model_capacity` | gain 三分（item prior／post-item context／未分配）＋ capacity vs ability 散點；只讀 `gain_ledger`，不碰評測資料 |
| **`contract.INPUTS`** | 每項診斷宣告自己的 node inputs，成為 **node inputs 與 `compute` 簽章的單一真實來源**（契約測試證明兩者對齊） |
| `uncertainty.iter_stratified_cluster_multipliers` | 分層 cluster 重抽骨架抽出共用（generator，公司規模下記憶體 O(n_rows)；`paired_bootstrap_delta` 逐位元不變） |
| `_common.query_key` / `sample_arrays` | 兩個實例對照後才抽的共用層 |
| `_common.ci_for_corrected_minus_baseline` | CI 方向自帶名字（符號反了不會有任何數值斷言轉紅，靠 `test_ci_brackets_the_point_estimate` 結構性守住） |

**real-run 驗證**（`--post-training --model-version 6059dcef`，654 queries）：14 節點 49.6s；三頁 HTML 產出、`index.html` 編號 01/02/03 連續；兩份新 JSON 過嚴格解析；`report.html` 正規化（去 plotly UUID 與時間戳）後**唯一實質差異是「本次寫出 1 頁 → 3 頁」**；離線重繪 2.5 秒產出三頁。

**`_registry_diagnosis_enabled` 的判準改了**（本 Plan 唯一的既有行為變更）：從「在 `DIAGNOSES` 裡」改成「`INPUTS` 含 `diagnosis_sample`」。`model_capacity` 不吃抽樣，舊判準會讓「只開 `model_capacity`」白抽一次全量樣本（公司規模 ≈25 萬 query × 22 item 的 `toPandas()`），**而且沒有任何徵兆**——不會報錯、不會有測試轉紅，pipeline 只是安靜地慢。

### Plan 1.5 交付了什麼（Plan 2 開工前先看這段）

`generate_report` 原本一個 node 做三件事（Spark 聚合 ／ 產生診斷頁 ／ 組裝主報表），拆成三個：

| node | 職責 | 產物 |
|---|---|---|
| `compute_report_aggregates` | 6 個 Spark 聚合 | `report_aggregates.json` |
| `render_diagnosis_pages` | 按**檔名**讀診斷 JSON → 多頁 HTML | `diagnosis/*.html` |
| `generate_report` | 純組裝（**無 Spark**） | `report.html` |

**對 Plan 2–5 的直接影響**：新增一項診斷要動的地方是**三個**，全部有測試守著——

1. `contract.DIAGNOSES` 加一行；
2. `catalog.yaml` 加一條 `evaluation_<name>` 的 **JSONDataset** entry（漏了會靜默不落地 → `test_every_registry_diagnosis_has_a_catalog_entry` 擋）；
3. 診斷子套件本身（契約測試擋）。

**`pipeline.py` 與 `generate_report` 都不必動**——Node 由 `DIAGNOSES` 導出，`generate_report` 的簽章裡不再出現任何診斷。

**real-run 驗證**（2026-07-20，`--post-training --model-version 6059dcef`，654 queries）：pipeline 12 節點 49.2s；`generate_report` 從最貴的 node 之一變成 **0.10s**；主報表 10 張圖與重構前 **traces/layout 逐位元相同**；`report_aggregates.json` 通過嚴格 JSON 解析（無 `NaN` 字面值）、rank 欄名仍是 int。

**`--only-node render_diagnosis_pages` 的兩種行為**（診斷 inputs 存在的理由）：診斷 JSON 已落地 → 跑 1/12 個 node（便宜重繪）；JSON 不存在 → 跑 4/12（自動往上拉抽樣與診斷）。

## Task 2.9：offset 換算量表（2026-07-20 追加，`810cce3` → `2d2604f`）

2.8 通過後使用者看公司環境產出的 `01-config-shift.html`，追加一則回饋：**只看 log-odds 讀者很難有數學直覺**，要求補一張量表。已完成。

**最終形狀**：第 2 節（緊接 offset 矩陣熱圖）、**可摺疊**、**26 列 × 9 欄**。列＝offset ±{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}；欄＝`offset` ＋ `勝算倍率` ＋ 七個起始機率（0.01%／0.1%／1%／2%／5%／10%／50%）。機率欄**只印結果機率、不印倍率**。

**Plan 2–5 照抄的部分**：
- 「尺」章節放在**它服務的那張圖之後**（不是頁首）。單位需要翻譯時，翻譯緊貼被翻譯的東西。
- 尺是**純數學**：格線與錨點都是模組常數，不吃執行期資料，跨執行可比。本次執行落在尺的哪裡，寫在 bullet 而不是塞進表格——把本次數值混進尺裡，讀者會把某一列讀成基準線（違反「不設門檻」）。
- 負向用 `÷` 不用 `×0.00034`。
- 錨點選**通用**的 base rate，**不要釘在某次實驗的正樣本率**。曾有一版打算存 `score_uncalibrated` 分位數、把欄位釘在該次的 16.7%，被使用者以「這個框架是通用的，我只是套用我目前的案例」駁回。副作用是好的：撤掉後這一節變回純呈現層，公司環境現有 JSON 拷回本機就能重繪。
- **收合之後「列數不是成本、欄寬才是」**：列收在 summary 後面不佔版面，欄一律撐開頁寬。所以格線可以補密（`|offset| ≥ 1` 後每整數一列，查表不必內插），但每格要壓到最短——保留括號裡的倍率會讓總字寬 109 → 172（約 1590px，超過多數螢幕內容寬度而需橫向捲動）。**資訊不是刪掉而是換位置**：勝算倍率自成一欄，機率倍率由前後值直接看得出來。
- **bullet 引用的每個數字都必須在表格裡看得到**。機率欄改成只印結果機率之後，原本 bullet 引用的 `×957` 就懸空了，改用表上看得到的 68.8%（從 0.01%）與 100%（從 50%）講同一件事。
- **測試不要解析格式化字串**：原本那條關鍵測試解析格子裡的 `×2.71`，改格式就失效。改成**從結果機率反算倍率再比對數值**，換格式化方式不會讓它誤紅或假綠。

**踩到的坑（記在這裡是因為 Plan 2–5 會重複遇到）**：
- **表格別 `set_index`**：具名 index 會讓 pandas 產生兩列表頭（第二列只有欄名＋一排空格），本頁其他表格都是單列，看起來像渲染壞掉。留 RangeIndex，`pages._show_index` 會自動隱藏。
- **`ReportSection.collapsible` 原本只有 `evaluation/report.py` 有實作**，`report/pages.py` 設了會被靜默忽略。本次補齊，兩邊 CSS 也對齊了。
- **未閉合的 `<details>` 會吞掉後續所有 section**（游離的 `</div>` 不會隱式關閉它）。只驗開標籤的測試抓不到——實測：把 `</details>` 寫成 `</div>`，22 條測試全綠，而真實產物有 5 節被吞進收合區。**任何產生成對標籤的渲染測試都要驗守恆**，不只驗開標籤。
- **bullet 的動態數值不可用 substring `in` 斷言**：`"÷21" in bullet` 會被 `÷21.9`／`÷210`／`÷21,000` 滿足；而且 lo/hi 對調（範圍上下顛倒）也照樣通過。用整句相等。
- **`min`／`max` 前必須濾掉非有限值**：`nan` 比較恆為 False，結果隨 dict 插入順序而變——NaN 排前面回 `nan`，排後面被靜默吞掉。`_compute` 的 `if val <= 0.0: raise` 守衛擋不住 NaN（`nan <= 0.0` 為 False）。
- **node 順序是拓撲排序的結果，不是宣告順序**（Plan 1.5）。新增 node 之後的名單要**實跑**取得（`[n.name for n in create_pipeline(**kw).nodes]`），不要用宣告順序推。同一個 node 在 default 與 compare 模式可能落在不同位置——`render_diagnosis_pages` 在 default 是倒數第二，在 compare 模式落在 `generate_comparison_report` **之後**。
- **只讀 `parameters` 的 node，in-degree 是 0**（Plan 1.5）。`parameters` 沒有生產者，所以 Kahn 會把這種 node 排到很前面。一個「要在某些 node 之後才有意義」的 node，即使它按檔名／路徑自己讀資料，也**必須把上游產物列進 `inputs`**——否則它會提早執行、讀到上一次執行留下的檔案，而且照樣「成功」。
- **用 annotation 的字串表示驗型別，成敗取決於模組有沒有 `from __future__ import annotations`**（2026-07-20，Plan 1.5 Task 4）。有這行時 annotation 是**原始碼字串**（`"SparkDataFrame"`）；沒有時是**解析後的型別物件**，`str()` 給 `<class 'pyspark.sql.dataframe.DataFrame'>`——**別名不保留**。所以 `assert "SparkDataFrame" not in str(annotation)` 在後者恆真，**改動前就是綠的**。`nodes_spark.py` 正是後者。**判準：驗型別就比對型別物件本身（`p.annotation is SparkDataFrame` 或 `"pyspark" in str(...)`），不要比對你以為它會印出的字串。** 這是 substring 斷言家族的一個變種，特徵是「字串來自你沒有控制權的 repr」。順帶：驗「函式不碰 Spark」時，annotation 與函式體要分開驗——annotation 可以改而行為不變，反之亦然。
- **fixture 用的是計畫稿捏造的 schema，而不是生產端真正產出的形狀**（2026-07-20，Plan 2 Task 4.1，**本系列目前最嚴重的一次假綠**）。`model_capacity` 的 `LEDGER` fixture 寫成扁平鍵（`item_id_gain`／`post_item_context_gain`），但 `diagnosis/model/gain_ledger.py:217-232` 從頭到尾只產巢狀（`item_id: {gain_sum: ...}`／`context: {gain_sum: ...}`）——**兩條 emit 路徑（`compute_gain_ledger` 與 `_coarse_ledger`）都是巢狀，扁平形狀在 production 從未存在**。實作者發現不符後讓讀取「巢狀優先、扁平備援」，runtime 是對的，但**fixture 留著錯的**，於是：把巢狀讀取整段拿掉 → **29 條測試全綠**，而真實 ledger 算出 `item_id_share=None`、`context_share=None`（公司環境整頁空白）。修正＝fixture 換真實 schema ＋ **扁平備援整段刪除**（留著等於「靜默回退到錯答案」的機制），同一 mutation 之後轉紅 3 條。**判準：fixture 的形狀必須從生產端的產出程式碼查證，不得從計畫稿抄；計畫稿裡的 schema 是待驗證的假設，不是事實。** 連帶揭露計畫漏掉的第四條退化路徑（`_coarse_ledger` 的 `context`／`per_item` 為 `None`、`fallback: True`），已補測試——並要求 `context_gain_share` 必須是 `None` 而非 `0.0`，因為 `0` 會被讀成「context 完全沒貢獻」這個錯誤結論。
- **「不存在」斷言同時被「正確跳過」與「根本沒嘗試」滿足**（2026-07-20，Plan 1.5 Task 1 的 mutation 意外揭露）。`tests/scripts/test_render_diagnosis.py:106-121` monkeypatch `DIAGNOSES` 成兩項、只放一項的 JSON，然後斷言 `01-config-shift.html` 存在、`02-<缺的那項>.html` **不**存在。把 registry 改成「import 時凍結」（等於完全看不到 monkeypatch 的第二項）之後，**三條斷言全部照樣通過**——因為「知道有第二項但沒資料所以跳過」與「根本不知道有第二項」產生**完全相同的檔案系統狀態**。真正有鑑別力的是它的兄弟測試 `:124-137`：斷言那個名字**出現在 stderr**。**判準：驗「某件事被正確略過」時，斷言要落在「系統說了什麼」（log／回傳的 `missing` 清單），不能只落在「檔案沒產生」——後者是雙關的。**

## 下一步：Plan 3（`04-plan-3-suppression.md`）

第四項診斷 `suppression` ＋ 交叉購買。開工前先讀本檔的「Plan 2 交付了什麼」與下方「假綠形態」兩段。

**新增一項診斷要動的地方仍然是三處**（Plan 1.5 的宣稱，Plan 2 兩次實測都成立）：`contract.DIAGNOSES` 一行、`catalog.yaml` 一條 JSONDataset entry、診斷子套件本身。**若那項診斷不吃共用抽樣，再加一行模組層級的 `INPUTS`。** `pipeline.py` 與 `nodes_spark.py` 都不必動。

### Plan 2 執行後對「派工方式」的三條教訓（Plan 3 開工前先看）

1. **驗收條件裡的字串一律先查證再寫。** Plan 2 有三次因為「憑印象寫的識別字」讓 agent 做錯或差點做錯：`EXPECTED_ORDER`（repo 裡沒這個符號）、`git stash` 取 baseline（乾淨工作區會失敗、髒工作區有掉工作的風險，正解是 `git archive <sha> src | tar -x -C <暫存目錄>`）、`grep "item_id_gain" 零命中`（該字串同時是輸出鍵名，照做會打壞對外 schema）。**驗收條件的定義是「兩個人檢查會得到同一結論」——寫之前自己先跑一次。**
2. **測試 fixture 必須對照真實產出，不得從計畫稿捏造。** `model_capacity` 的 `LEDGER` fixture 用扁平鍵，而 `gain_ledger.py:217-232` 從來只產巢狀。結果是 **29 條測試全綠、production 路徑零覆蓋**——把巢狀讀取整段拿掉，測試不會有一條轉紅，而公司環境會整頁空白。修正後同一 mutation 轉紅 3 條。
3. **測試指令開太寬是浪費。** 每個 task 都跑五個測試目錄（665 條 / 36 秒）而 agent 會跑很多次；專案 CLAUDE.md 自己寫著「單次改動只跑相關測試檔」。Plan 3 改成「相關測試檔 ＋ 最後一次全量」。另外 task 別切太細——每個新 agent 都要冷啟動重讀同一批樣板檔（`_render.py` 各 400–600 行）。

---

## 附錄：接線層重構（Plan 1.5，已完成）

**計畫與執行紀錄：`02b-plan-1.5-wiring.md`（7 個 task）。以下是當初的問題描述，保留供追溯。**

Task 2.8 的回饋 1 與 2 指向同一組接線缺陷，且**已在公司環境造成一次實際故障**。Plan 2–5 會各新增一項診斷，照現在的形狀做下去＝四份手寫 Node、四次位置平移的機會。所以在 Plan 2 之前先收掉：

1. **診斷 Node 由 `contract.DIAGNOSES` 導出**，不再每項手寫。
2. **`generate_report` 不再收診斷結果**——診斷頁改由獨立的 `render_diagnosis_pages` 產生。
3. **把 `generate_report` 裡的 Spark 聚合抽成獨立 node**，落地 JSON；`generate_report` 簽章不再有 `SparkDataFrame`，變成純函式。

> **兩處更正（2026-07-20，寫 Plan 1.5 時查證）：**
>
> - **原本第 3 條寫「五個 Spark 聚合」，實際是六個。** `include_distributions` 底下 5 個、`include_calibration` 底下還有 `calibration_bins`（`nodes_spark.py:571-578`）。
> - **原本第 1 條被寫成「TypeError 的根治法」，這是錯的。** `pipeline.py:134` 現在**就已經**從 `DIAGNOSES` 導出 inputs；出事是因為公司環境那份 `pipeline.py` 是手動拷貝的舊檔。真正的成因是 `generate_report` 有 4 個帶預設值的參數再加 varargs，**6/7/8/9 個 inputs 全都在合法範圍內**——個數對、位置錯。根治法是讓它變成剛好個數（無 varargs、無預設值），這樣少接一個 input 就是 `TypeError`。實作見 `02b-plan-1.5-wiring.md` Task 3。
> - 連帶結論：**不動 `core/runner.py` 的位置綁定**。盤點 4 條 pipeline、52 個不重複 node，具備「少給 input 不會立刻爆」形狀的只有 3 個（`generate_report`、`log_experiment`、`select_shap_population`），其餘 49 個本來就 fail-loud。為 3 個 node 改 59 個呼叫點共用的執行核心，radius 不成比例。
4. **每項診斷對使用者只有一個開關**：`evaluation.diagnosis.<name>.enabled`；`DIAGNOSES` 降級成「程式碼裡存在」的宣告，並把兩者的差別寫進文件。

1＋2 動同一個簽章，3 讓那個簽章順便擺脫 Spark——**一次做完比分三次便宜**。做完的連帶好處：`scripts/render_diagnosis.py` 的 2 秒重繪範圍可從診斷頁擴大到整份主報表；公司環境手動同步時 `pipeline.py` 不再需要逐項核對。

**這一輪要正式走 `superpowers:writing-plans` 產出計畫**，不要當成順手的重構——它動的是五項診斷共用的接線，出錯的半徑是全部。

**續作接手方式**：讀 `00-shared-context.md` ＋ `02-plan-1-config-shift.md`，從上表標示的下一個 task 開始。worktree 是 `/Users/curtislu/projects/recsys_tfb/.worktrees/diag-redesign`，**本機 Spark 環境已建好**（dataset／training 跑過，model_version `6059dcef`），不必重跑 `local_spark_setup --reset`。

**執行過程中對計畫原稿的修正（照計畫檔逐字實作會撞到，先看這裡）**：

- **Task 2.2 的 `build_offset_frame` 簽章**：計畫的測試寫成單參數 `build_offset_frame(PARAMS)`，但同一個 task 的移植表要求「原樣移植」——原函式是 `(pdf, parameters, schema)` 且回傳 `(offset_df, meta)` tuple。單參數版本推導不出未出現在 `sample_ratio_overrides` 裡的 item，該測試無解。**以移植表為準**。
- **Task 2.2 的 `test_group_internal_spread_not_global` 原稿是假綠**：`_sample()` 只有 `mass` 一個客群，單一 context 下「群內 spread」恆等於「全域 spread」，Step 5 的 mutation 改成全域之後測試照樣過。已改成兩個客群（`mass`／`affluent`）且群間 offset 不同。
- **`paired_bootstrap_delta` 與各診斷的 Δ 反號**：它回的是 `mAP(F) − mAP(F − shift)`，而 `config_shift` 的 `delta = corrected − baseline`。呼叫端要取負並對調上下界。**Plan 2–5 照抄樣板時會踩到同一個坑**，且符號寫反不會有任何數值測試轉紅（大小完全正確、只有正負相反），必須用「CI 與 Δ 同號」這種結構性斷言釘住。
- **診斷子套件的檔名用 `_compute.py`／`_render.py`（前綴底線）**，不是計畫原稿寫的 `compute.py`／`render.py`。理由：`from .compute import compute` 會把子模組名重綁成函式（`pkg.compute is mod` → `False`），而 `contract.check_module` 走 `getattr` 剛好拿到函式、**抓不到這個遮蔽**。與 repo 既有的 `_common.py`／`_spark.py` 命名也一致。
- **「同一客群內 offset 同加常數 → Δ 不變」不是無條件成立的不變量**：前提是「每個 query 完整落在單一 context group 內」，只有 context 欄是 **entity 級**屬性時才成立。context ＝ `sample_group_keys ∪ sample_weight_keys − {item, label}`，而 `parameters_training.yaml:54-55` 明文允許它取自 item 級屬性（產品層級／類別）。實測反例：`offset_spread` 報 `{hi: 0.0, lo: 0.0}` 而 `delta = 0.1875`。**Task 2.3 的 `SCOPE.blind_to` 原稿把它寫成無條件成立，要改成有前提的敘述。**

## Task 2.8 使用者回饋（2026-07-20，公司環境實跑後）

### 回饋 1：診斷的開關散在三個地方，分不清誰管誰

使用者原話：「診斷項目的開關有點混亂，evaluation pipeline 的 `pipeline.py` 中可以定義要跑的 node、`parameters_evaluation.yaml` 中的 `report` 段落、然後又有 `diagnosis` 段落」。

**實際的開關面（盤點，不是印象）**：

| # | 在哪 | 管什麼 | 舊診斷 | registry 診斷 |
|---|---|---|---|---|
| 1 | `pipeline.py` 的 Node 清單 | 這個 node 存不存在 | 手寫 | **也是手寫**（每項一個） |
| 2 | `diagnosis.<name>.enabled` | 算不算（不算就寫 stub） | ✓ | ✓ |
| 3 | `report.sections.<name>` | 在主報表渲不渲染 | ✓ | **無**（走自己的頁） |
| 4 | `contract.DIAGNOSES` | 進不進 registry（決定 catalog 鍵、頁面、`generate_report` 輸入） | 不適用 | ✓（**在 Python 裡，不是 config**） |
| 5 | `_sample_consumer_flags` ＋ `_registry_diagnosis_enabled` | 共用抽樣抽不抽 | 衍生 | 衍生 |
| 6 | CLI `--only-node` / `--from-node` | 這次跑哪些 | 正交 | 正交 |

**哪些是過渡期、哪些是真問題**：

- **過渡期（Plan 5 收尾自然消失）**：#3 只服務舊診斷。五項 registry 診斷全部上線、舊的移除後，`report.sections` 會只剩非診斷的區塊，#3 就不再是「診斷開關」。
- **真問題，且已經咬過一次**：`generate_report` 用**位置**收 7 個具名參數 ＋ varargs。2026-07-20 公司環境實例：`node.inputs` 少了兩個元素 → 位置 6 的 `evaluation_config_shift` 綁進 `offset_sweep` 參數 → `build_offset_sweep_section` 拿到 `per_item` 是 list 而非 dict → `TypeError`。**運氣好才爆；型別相容的話會靜默把 A 診斷的數字印在 B 診斷的標題下。**
  - **更正（2026-07-20）**：這一條原本把 #1（手寫 Node）也算成成因，不成立——`pipeline.py:134` 早就是從 `DIAGNOSES` 導出的。真正讓錯位變得可能的是**簽章裡那 4 個預設值加 varargs**，它讓「個數不對」不再是錯誤。#1 仍值得收（Plan 2–5 會產生四份只差模組名的複製品，而它們會各自漂移），但它是**簡化**，不是這個故障的修法。
- **真問題**：#4 在 Python 裡而 #2 在 YAML 裡，「關掉一項診斷」有兩個位置、語意不同（不進 registry ＝ 完全不存在；`enabled: false` ＝ 存在但寫 stub）。使用者要關一項時該動哪個，目前沒有任何地方寫。

**目標狀態（Plan 2 開工前定案，因為 Plan 2–5 會照抄現在的形狀）**：

1. **每項診斷對使用者只有一個開關**：`evaluation.diagnosis.<name>.enabled`。`contract.DIAGNOSES` 降級成「這個診斷在程式碼裡存在」的宣告，不是使用者面的開關。
2. **Node 由 `DIAGNOSES` 導出**，不再每項手寫——同時消滅 #1 的手寫 Node 與 `generate_report` 的位置綁定脆弱性。
3. **`generate_report` 不再用 N 個位置參數收診斷結果**，改成收單一 dict（或由 runner 具名綁定）。這是上面那個 TypeError 的根治法，不是 workaround。

### 回饋 2：`generate_report` 應該只組裝，不該再做 Spark 計算

使用者原話：「我想把 `generate_report` 這個 node 的負擔降低，我希望跑這個 node 單純就是在組裝之前 node 已經跑過的結果，不要再有多餘的 spark 計算。」

**現況（`nodes_spark.py:542-570`）**：`generate_report` 吃 `eval_predictions: SparkDataFrame`，`.select(...).cache()` 之後跑五個 Spark 聚合——`score_histogram_counts`／`score_box_stats_by_label`／`rank_count_matrix`／`positive_rank_count_matrix`／`positive_rate_matrix`。每個都是一次全掃。

**為什麼這是問題，不只是效能**：

1. **它是最後一個 node，卻是最貴的之一。** 失敗成本最大化：前面全部跑完才死在這裡（2026-07-20 公司環境的 TypeError 正是如此）。
2. **主報表無法離線重繪。** 診斷頁已經做到「JSON 落地 → 純函式 render → 2 秒重繪」，主報表沒有——因為它的圖表要靠 Spark 現算。使用者要調主報表版面就得重跑整條 pipeline。
3. **它違反這次重構的核心邊界。** `compute` 純計算落地 JSON、`render` 純呈現讀 JSON——五項新診斷都遵守，而報表層自己沒有。

**修法（與診斷層同一個模式）**：把那五個聚合抽成獨立 node，輸出落地成 JSON（都是小東西：bin counts、quartiles、rank 矩陣），`generate_report` 改讀那份 JSON、簽章不再有 `SparkDataFrame`。

**連帶效果**：
- `generate_report` 變成純函式 → 可以納入 `scripts/render_diagnosis.py` 的離線重繪範圍，2 秒迴圈從診斷頁擴大到整份報表。
- 失敗點往上游移到真正做事的地方。
- 與回饋 1 的第 3 條（`generate_report` 不再用 N 個位置參數收診斷結果）是同一次改動的兩面，一起做比分兩次做便宜。

### 回饋 3：舊 `offset_sweep` section 的判讀語氣

見下方「過渡期的兩種語氣並存」。使用者可用 `report.sections.offset_sweep: false` 立即關掉，理由獨立成立（該段文字違反鐵則 1），不必等 Plan 4。

## 已裁決延後的重構（不是遺漏，時機到了再做）

> **✅ 標記「Plan 2 開工時」的三項已於 2026-07-20 結清**（`3c026be`）：`_common.py` 樣板抽取、CI 方向自帶名字的包裝、`q_agg` 權重常數假設檢查。下表保留原始條目供追溯。
>
> 抽取的結論值得記一筆：**逐行比對後真正能抽的很少**——只有 `query_key` 與 `sample_arrays` 的三行陣列組裝。`clusters` **刻意不合併**：`config_shift` 要未 factorize 的 `pd.Series`（要呼叫 `.nunique()`、且 `paired_bootstrap_delta` 會自己 factorize），`item_ability` 要已 factorize 的 0-based int 陣列（`iter_stratified_cluster_multipliers` 直接拿它當索引）。兩者名字一樣、語意不同，硬合併會造出一個沒有人真正需要的中間型別。**「抽得少」是正確結論，不是失敗**——這正是當初延後到「有第二個實例」才動手要換到的判斷依據。

兩關審查提出、我判斷**現在做是從單一實例猜共用抽象**，延到有第二個實例時再收：

| 項目 | 何時做 | 為什麼不是現在 |
|---|---|---|
| 把 query key／`ht_weights`／CI frame 組裝那 ~40 行樣板抽到 `_common.py` | **Plan 2 開工時**（有第二個實例可對照） | 一個實例看不出哪些是共通、哪些是 `config_shift` 特有 |
| `_common` 加「CI 方向自帶名字」的包裝，讓不知道反號這回事的人也不會寫錯 | Plan 2，與上一項一起 | 同上；目前靠 config_shift 自己的測試守著，風險是重寫的人不是照抄的人 |
| `contract.check_module` 補簽名檢查（`compute(diagnosis_sample, parameters)`）與三態 key-set 一致性檢查 | **Task 2.3**（`render` 存在之後） | 只有 `compute` 時釘不了完整契約；目前簽名形狀是默默立的，後四項寫錯簽名契約測試照樣綠 |
| `field_notes` 與實際輸出鍵的一致性檢查（新增欄位忘了補說明不會有測試轉紅） | Task 2.3，與上一列一起 | 同屬 contract 強化 |
| `q_agg` 的 `weight=("w","max")` 假設 `inclusion_weight` 在 query 內為常數 | **Plan 2 開工時檢查** | 對 `draw_diagnosis_sample` 的產出成立（權重由 stratum 決定、stratum 是 query 級屬性），但無測試釘住；上游若讓同一 query 的列帶不同權重，`max` 會靜默選一個 |
| 讓診斷提供 result-dependent 的 `blind_to`（例如偵測到 lambdarank 時動態多一條） | 有第二個實例需要時 | 目前 `notes` 已承載這類資訊、`render` 也顯示了；為此加一個每項診斷各一份的 hook，會讓同一段邏輯被抄五次（Task 2.3 的 `scope_for()` 即因此撤回） |
| `display` config 機制（YAML 覆寫表格欄位順序／欄名） | 使用者在公司環境看過真實產出、明確說出要調哪幾樣之後 | 中介層要的性質（JSON 持久化、呈現隨時可改、不必重跑公司環境）**離線重繪 2 秒就已經拿到了**。再疊一層 config：每個旋鈕都是程式碼＋測試＋文件，而且只調得動事先想到的那幾樣；直接改 `_render.py` 再重繪則什麼都能調、成本一樣。理由完整寫在 `scripts/render_diagnosis.py` 的 module docstring，**那是刻意不做、不是漏做** |
| `render_diagnosis.py` 的 `--params` 讀不到時退回空 dict | 某項診斷的 `render` 真的開始讀 `parameters` 時 | 今天無害（`config_shift.render` 完全沒用到 `parameters`，只有簽章要求），但屆時會變成「安靜地用空 config 畫圖」。**現在沒有守衛**，那時要補 |

**過渡期的兩種語氣並存（Plan 2–5 收尾前會一直存在）**：主報表裡舊的 `build_offset_sweep_section`（`report_builder.py:501-502`）寫著「大＝缺口主要在水準（配置／再平衡可修）、小＝缺口在條件判別力（必須動訓練）」——明確的處方，與三條鐵則相反。它服務的是 Plan 4 將由 `score_shift` 取代的舊診斷，且**今天就已在 main 出貨**，所以留著不會讓現況變差，改了也是改一段即將刪除的程式碼。**但 Task 2.8 檢視樣板時要把它排除在外**，否則會看到新舊兩種語氣打架而誤判樣板本身有問題。Plan 4 移除該診斷時一併清掉，Plan 5 全案驗收時複查。

**Task 2.2 已知的弱斷言（不擋交付，但別當成有守住）**：`test_null_context_group_survives_and_is_visible` 對 note 的斷言是 `"prod_tier" in n`，靠當下文案措辭撐著——目前只有結構 note 含欄名所以有鑑別力（mutation 1b 驗證過），但改文案可能讓它退化成假綠。

**一個跨版本比對的注意事項**：`query_offset_spread` 的分位數用 inverse-CDF（不插值），與 `np.percentile` 的線性插值**算出來的數字不同**，即使權重全為 1 也不同。理由是插值會產生一個資料裡不存在的 spread 值，而每個值都該對應「某個 query 實際出現的偏移範圍」。目前尚無任何一次公司環境 real-run，所以沒有舊 JSON 需要對照；日後若要跨版本比 `p50`／`p90`，要知道這件事（`mean`／`max` 不受影響）。

**Plan 0 已落地、後續計畫可直接用的地基**：
- `src/recsys_tfb/report/`：`types`（`ReportSection`／`ScopeNote`／`Page`）、`fmt`（六個語意化格式器）、`scales`、`figures`（含 `MAX_FIGURE_POINTS`）、`pages`（多頁 HTML ＋共用 plotly.js，單頁實測 9.3KB）
- `diagnosis/metric/sample.py`：回傳的 `sample_pdf` 帶 `stratum`／`inclusion_weight`；`meta` 帶 `strata`／`sampling_description`
- `diagnosis/metric/uncertainty.py::paired_bootstrap_delta`：分層配對 cluster bootstrap（**不要再寫第二份 bootstrap**）
- `evaluation/metrics.py`：mAP 原語支援 optional `weights`（不傳時位元等價）＋`align_positive_row_weights`

**公司環境實況（本機測不到，已據此設定）**：有正例的 query 約 22 萬、driver 128GB → `max_queries` 設 250,000 → `ratio == 1.0` → 診斷是**普查**、權重全 1。

## 六份計畫

| # | 檔案 | 一句話 | 交付後你看什麼 |
|---|---|---|---|
| 0 | `01-plan-0-foundation.md` | 清場 ＋ 抽樣加權 ＋ 呈現層。**不含任何新診斷** | 公司環境的 `sample_ratio` 到底是多少 |
| 1 | `02-plan-1-config-shift.md` | 契約 ＋ 第一項診斷 ＋ 離線重繪工具 | **樣板形狀**（後三份照抄它） |
| 2 | `03-plan-2-item-ability-capacity.md` | 第二、三項診斷 | AUC 對照散點、gain 三分 |
| 3 | `04-plan-3-suppression.md` | 第四項診斷 ＋ 交叉購買 | 壓制矩陣與共買圖並排對照 |
| 4 | `05-plan-4-score-shift.md` | 第五項診斷（最貴的一項） | 執行時間、Δ 與 CI |
| 5 | `06-plan-5-wrapup.md` | 改名 ＋ ScopeNote 驗收 ＋ 文件 | 框架文件講不講得通 |

**必須依序執行。** Plan 0 的抽樣加權是五項診斷共同的地基；Plan 1 立下的樣板，Plan 2–4 照抄。

## 五項診斷在回答什麼

順序是歸因優先權，**不是硬閘門**——五項全跑、全呈現。

| # | 診斷 | 回答什麼 | 排除什麼 |
|---|---|---|---|
| 1 | `config_shift` | 抽樣比例與 sample weight 有沒有引入 per-item 的 log-odds 偏移 | 若偏移為 0，排序問題就不是訓練設定造成的 |
| 2 | `item_ability` | 模型能不能在同一個 query 內分辨誰會買哪個 item | 把客戶活躍度誤判成 item 推薦能力 |
| 3 | `model_capacity` | gain／split 花在 item 身分還是 context 特徵 | 「學到互動訊號」與「只記住 item prior」 |
| 4 | `suppression` | 哪些 label=0 排在 label=1 之前、造成多少 AP 缺口 | 「模型排錯」與「商品本來就競爭」 |
| 5 | `score_shift` | 不重訓、只加 per-item 常數位移，holdout mAP 能不能提升 | 問題偏 item 水準，還是偏辨識力／特徵表達 |

## 三條鐵則（每份計畫都重貼一次）

1. **不下結論。** 不得產生 severity、verdict、建議動作、「應該／不足／異常」這類字眼。
2. **不設門檻。** 不得用 config 門檻把連續量切成離散類別。顏色只編碼資料的大小或正負，不編碼好壞。
3. **每個數字自帶說明。** 每項診斷必須宣告 `ScopeNote`，`blind_to` 為空即契約違反，有測試擋。

## 參考素材（在本 branch，非產品程式碼）

`scripts/*_diagnosis.py` 六份 ＋ `tests/scripts/test_*_diagnosis.py` 兩份，是與 codex 討論後的試作實作。各計畫的移植步驟會逐一引用它們的 `檔案:行號`。**功能全部進 `src/` 之後要不要刪，由使用者決定**（見 Plan 5 的全案驗收）。
