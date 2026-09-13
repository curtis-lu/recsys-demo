# Evaluation pipeline 稽核發現與複核結果（15 條 bug ＋ 8 條設計問題）

## 這份是什麼

2026-09-09 對 evaluation pipeline 做過一次唯讀靜態稽核，基準是 `main` @ `5fe8791`。稽核只做靜態追蹤——讀 code、讀 conf、讀 docs、grep，除了一次 `inspect.signature` 對照之外沒有實際執行任何東西，所有「失敗情境」都是從程式碼路徑推出來的，不是觀察到的。

2026-09-13 對同一批發現做了一次複核，基準是 `main` @ `6898179`。複核分兩份：bug 清單逐條複驗（實際執行了其中 6 條的重現腳本）、設計問題 A–J 逐條複核（唯讀，加一次純函式實跑）。兩次複核都沒有修改任何 repo 檔案。

這份檔把兩次的結果合成一份事實記錄，不含任何決策。各條列出的修法候選與 trade-off 只是已知的取捨與限制，沒有任何一條被選定。

**條目數怎麼算的**：2026-09-09 稽核原本是 12 條 bug ＋ 10 條設計問題＝22 條。bug 清單後來加了第 13 條（2026-09-12 撞到的監控模式問題）與第 15 條（2026-09-13 審查 ADR 時發現的 schema 型別衝突）；設計問題 I 已經被 `c88746a` 修掉、從清單移除；設計問題 G 經 2026-09-13 複核改列成 bug 14。所以現在是 **15 條 bug ＋ 8 條設計問題**（A、B、C、D、E、F、H、J），設計清單裡 G 的位置只留一行指向 bug 14。

**兩個基準之間發生過什麼**：`5fe8791`..`6898179` 共 26 個 commit，其中只有 4 個碰過 evaluation 相關路徑（`cab6c74`、`431836b`、`c88746a`、`a5f7801`）。evaluation 相關檔案改了 16 個（`metrics_spark.py` ±91 行、`report_builder.py` ±193 行，`statistics.py`／`calibration.py` 整個刪除），所以稽核當時寫的行號幾乎全部漂了，複核已重新定位過。**這份檔裡的行號：稽核原文那一段是 `5fe8791` 當時的，複核結果那一段是 `6898179` 當時的。**

**每一條都拆成四段**：**稽核原文**（2026-09-09 寫下的機制、失敗情境與位置；措辭與行號都是當時的）、**複核結果**（2026-09-13 的判定、現在的檔案:行號、證據等級）、**稽核寫錯的地方**（稽核原句 ＋ 更正；沒有這種情形的條目就沒有這一段）、**動它之前要知道的事**（撞哪條凍結、與其他條的因果耦合、還沒驗到的部分、修法上的陷阱）。

**幾個名詞**：

- **fan-out**：一對多的 join 把列數放大。左表一列對到右表兩列，結果就變兩列。
- **collision（撞號）**：兩個不同的輸入被映射成同一個結果。單看一個輸入看不出來，只有兩個輸入同時出現時才現形。
- **leakage（洩漏）**：評估時用到了本來不該看得到的資訊，通常是評估當期或之後的答案。
- **M／B／Δ**：報表表格裡並排的三欄。M ＝ Model（這次評估的模型）、B ＝ Baseline（對照組；主報表裡是 popularity baseline，比較報表裡是 B 側那個模型版本）、Δ ＝ 前兩者相減。欄名字面就長這樣，見 `src/recsys_tfb/evaluation/report_builder.py:447-449`（per-item 表的 `row[f"{base} M"]`／`B`／`Δ`）與 `:787-788`（overall 表的 `("Model", …)`／`("Baseline", …)`／`("Δ", …)`）。
- **`A4`／`A10`／`A15`／`B1`–`B10` 這類代號**：config 與資料一致性不變量的編號。A 系列在 CLI 進入點檢查 config，B 系列在 pipeline 執行時檢查資料。**唯一真實來源是 `src/recsys_tfb/core/consistency.py` 模組 docstring 的 invariant legend**——每個代號管什麼逐條寫在那裡，這份檔刻意不複製一份，以免兩邊漂開。
- **CONFIRMED／PARTIAL／REFUTED／STALE**：2026-09-13 複核給每條稽核發現的判定。CONFIRMED ＝ 稽核描述的機制與後果在現在的 main 上完全成立；PARTIAL ＝ 主要機制成立，但稽核描述的某一部分不成立（是哪一部分寫在該條的「稽核寫錯的地方」）；REFUTED ＝ 該項宣稱不成立（這份檔裡只用在 bug 11 的一個子宣稱上，那一整條的判定仍是 PARTIAL）；STALE ＝ 稽核當時成立，但已被後來的 commit 修掉（只有設計問題 I）。另外 **latent** ＝ 機制成立，但現行資料與設定下還沒有觸發過。
- **證據等級**：每條「複核結果」末尾標的取證方式。**實跑**（真的把那段程式跑起來、貼得出輸出）強過 **讀 code 追路徑**（照著程式碼把因果鏈追一遍）與 **讀 code ＋ grep**（靠全樹搜尋佐證「沒有第二個呼叫端」這類否定式宣稱）。標「未驗」的是兩次複核都沒有取證的部分。
- **`A-1`／`A-2`、`B-1`／`B-2`、`D-1`／`D-2`、`H-1`／`H-2`、`J-1`／`J-2` 這類子編號**：同一條裡被拆開的修法或子問題，定義在該條的「動它之前要知道的事」，不是獨立條目。總表用到時一律指得回該條。

**「撞哪條凍結」指的是** `docs/agents/deliberate-non-goals.md` 裡刻意不做的事（底下簡稱 `deliberate-non-goals.md`）。底下一律用該檔的小節標題原文稱呼，不用行號——那份檔會改，行號會漂，標題可以直接拿去 grep。同目錄另外兩份會被引用到的是 `docs/agents/pipeline-node-design.md`（node 的內容判準，底下簡稱 `pipeline-node-design.md`）與 `docs/agents/pipeline-refactor-process.md`（整條 pipeline 的重整流程判準，底下簡稱 `pipeline-refactor-process.md`）。

**2026-09-13 更新**：該檔的「別調報表的呈現——除了欄名標籤那一部分」一條已依 ADR-0020 刪除（使用者明示 evaluation 重構期間報表可改）。底下各條寫「撞報表呈現凍結」「要改得先問使用者」的地方，現在**不再構成障礙**，只保留當作「這條會動到報表」的提示；各條的修法由 ADR-0020 定。

**兩份複核報告、重現腳本（`repro_bug2.py` 等）與腳本的輸出檔（`repro_5_6_12.out`）都不在 repo 裡**，也不在任何 GitHub issue 裡；它們寫在複核當時的暫存目錄，沒有提交。這份檔是那些內容目前唯一留在 repo 裡的形式。底下出現的那些檔名只標示每條複核的涵蓋範圍與需不需要 Spark，不是可以開的路徑。

---

<a id="toc"></a>

## 目錄

**bug 清單**

- [bug 1. popularity baseline 會悄悄拿答案來排名](#bug-1)
- [bug 2. 改了設定重繪，會得到新舊混合的報表，退出碼 0](#bug-2)
- [bug 3. `n_queries` 在同一份 HTML 裡被標成兩個相反的意思](#bug-3)
- [bug 4. 比較報表會印出捏造的 Δ](#bug-4)
- [bug 5. 該月零正例的 item 從三處同時消失](#bug-5)
- [bug 6. segment join 沒對到的列變成一個叫 "None" 的客群，並等權進巨觀平均](#bug-6)
- [bug 7. 比較的兩側可能用不同版本的 ground truth，而 docstring 宣稱相同](#bug-7)
- [bug 8. 產品大類只有 3 類，報表照印 @4 / @5](#bug-8)
- [bug 9. render_diagnosis_pages 仍是 varargs 形狀，接錯了不會炸](#bug-9)
- [bug 10. label_table 的 LEFT JOIN 沒有唯一性保護](#bug-10)
- [bug 11. rank 與 pos 是兩套帳，同分時互不一致](#bug-11)
- [bug 12. per_item_segment 的底線串接 key 會撞號](#bug-12)
- [bug 13. 監控模式跑不完整條 pipeline](#bug-13)
- [bug 14. `restrict_to_common` 與 `comparison/alignment.py` 對「共同母體」用了不同定義](#bug-14)
- [bug 15. 同一個 model_version 換模式跑，第二次會在 `persist_eval_predictions` 炸](#bug-15)

**設計問題清單**（G 已升格成 bug 14，設計清單裡只留一行指過去；I 已被 `c88746a` 修掉，不在清單上）

- [設計 A. 「純函式可離線重繪」這個宣稱，被 catalog 抵銷了](#design-a)
- [設計 B. 「Spark 聚合給報表用」有兩套機制](#design-b)
- [設計 C. 兩個渲染器 ＋ 兩份數字格式器，已經漂移](#design-c)
- [設計 D. 兩個報表組裝器共用私有函式，同一個 config 鍵兩個預設值](#design-d)
- [設計 E. report.sections 的 8 個開關有一半沒人讀，而文件在教使用者用它們](#design-e)
- [設計 F. compare 模式把「算」與「畫」壓在同一個 node](#design-f)
- [設計 H. evaluation.metric 參數有四份讀取程式碼，其中 metric.k 只有一邊讀](#design-h)
- [設計 J. 加一個指標家族要改 6 個檔、10 處寫死清單](#design-j)

**其他**

- [總表](#summary)
- [2026-09-09 稽核「查過、確認沒問題的地方」](#no-issue)
- [兩次複核都沒驗到的事](#not-verified)

---

<a id="summary"></a>

## 總表

「改到報表？」問的是：修完之後、**在正常情境下**（不是這個 bug 被觸發的那一次執行），使用者在 `report.html`／`report_comparison.html`／診斷頁上看得到的內容會不會變。所以「否」的意思是「平常跑出來的報表長得一模一樣」，不代表這個 bug 觸發時看不出差別。

「撞哪條凍結」欄引的是 `deliberate-non-goals.md` 的小節標題原文（過長的用「……」截斷，但開頭逐字相同，可以直接拿去 grep 那個檔）。欄裡出現的 **PR #327** 是 2026-09-09 合併的那個 PR：它把報表兩張表的欄名標籤改成從 `schema` 取使用者的真實欄名，也因此讓「別調報表的呈現」那條凍結**只有欄名標籤那一部分**解凍。

#### 總表：bug

| # | 一句話 | 複核結果 | 改到報表？ | 撞哪條凍結 |
|---|---|---|---|---|
| 1 | popularity baseline 的 lookback 視窗查無資料就退回整張 `label_table` | CONFIRMED（稽核低估） | 是（baseline 段的說明文字） | 「別調報表的呈現……」（`build_baseline_section` 的說明文字仍凍結） |
| 2 | 改了 config 重繪會得到新舊混合的報表，退出碼 0 | CONFIRMED（範圍比稽核更廣） | 否 | 不撞 |
| 3 | `n_queries` 在同一份 HTML 裡被標成兩個相反的意思 | CONFIRMED | 是（欄名標籤） | 不撞——「別調報表的呈現……」的欄名標籤部分已由 PR #327 解凍 |
| 4 | 比較報表把缺值當 0，印出捏造的 Δ | CONFIRMED（觸發前提寫錯） | 視修法而定 | 修在呈現層才撞「別調報表的呈現……」 |
| 5 | 該月零正例的 item 從巨觀平均的分母消失 | PARTIAL（第三處不存在） | 可能（多一個分母數字） | 做成閘會撞「別做資料閘 B2（leakage）／B3（零正樣本）……」 |
| 6 | segment join 沒對到的列變成一個叫 `"None"` 的客群 | PARTIAL（「沒有計數」不成立） | 視修法而定 | 改名／加註撞「別調報表的呈現……」 |
| 7 | 比較的兩側可能用不同版本的 ground truth | CONFIRMED | 視修法而定 | 在 coverage 段加文字才撞「別調報表的呈現……」 |
| 8 | 產品大類只有 3 類，報表照印 @4／@5 | CONFIRMED（含 docstring 錯誤） | 是（印哪些欄） | 「別調報表的呈現……」；docstring 那半不撞 |
| 9 | `render_diagnosis_pages` 仍是 varargs 形狀 | PARTIAL（稽核描述的觸發路徑已有測試擋） | 否 | 不撞 |
| 10 | `label_table` 的 LEFT JOIN 沒有唯一性保護 | CONFIRMED（稽核指的修法方向錯） | 否 | fail-loud 做成 predicate 會撞「`inference_population` 的唯一性沒有寫進 `consistency.py`」 |
| 11 | rank 與 pos 是兩套帳，同分時互不一致 | PARTIAL（「有第三處也在算 rank」不成立） | 可能（診斷區的名次） | 不撞 |
| 12 | `per_item_segment` 的底線串接 key 會撞號 | CONFIRMED | 未驗（JSON 鍵集合會變） | 不撞 |
| 13 | 監控模式在第一個診斷就 raise，跑不完整條 pipeline | CONFIRMED | 是（監控模式才會有那三頁） | 不撞 |
| 14 | `restrict_to_common` 與 `comparison/alignment.py` 對「共同母體」用了不同定義 | CONFIRMED（由設計問題 G 升格） | 統一定義那半會改 coverage 數字 | 改 coverage 數字撞「別調報表的呈現……」 |
| 15 | 同一個 model_version 換模式跑，`rank` 的 INT／BIGINT 衝突讓第二次寫不進 Hive | CONFIRMED（原稽核未列） | 否（修完兩種模式都跑得完） | 不撞 |

#### 總表：設計問題

| # | 一句話 | 複核結果 | 改到報表？ | 撞哪條凍結 |
|---|---|---|---|---|
| A | 「主報表可以離線重繪」這個宣稱與接續合約互相矛盾 | CONFIRMED（但成本已被登記） | 否 | 不撞 |
| B | 「Spark 聚合給報表用」有兩套機制 | CONFIRMED | 搬段落是；只刪 calibration payload 否 | 搬段落撞「別調報表的呈現……」 |
| C | 兩個渲染器 ＋ 兩份數字格式器，已經漂移 | CONFIRMED（含實跑證據） | 部分（診斷頁的 `-0`） | 界線不清：「別調報表的呈現……」沒明講診斷頁算不算 |
| D | 兩個報表組裝器共用私有函式，同一個 config 鍵兩個預設值 | CONFIRMED | 搬函式否／統一預設是 | 統一預設撞「別調報表的呈現……」（會改 `report_comparison.html` 欄數） |
| E | `report.sections` 的 8 個開關有 4 個沒人讀 | CONFIRMED | 否 | 不撞 |
| F | compare 模式把「算」與「畫」壓在同一個 node | CONFIRMED | 否 | 不撞 |
| G | 共同母體算兩次，兩處定義可能對不上 | 已升格成 bug 14 | — | 見 bug 14 |
| H | `evaluation.metric` 有多份讀取程式碼，`metric.k` 只有一邊讀 | CONFIRMED（讀 code 即可定案） | 是（CI 註腳／`map_attr@k` 的值） | H-2 撞「別調報表的呈現……」明列的「`build_overview_section` 的 CI 註腳」 |
| J | 加一個指標家族要改多個檔、多處寫死清單 | CONFIRMED（範圍比稽核大） | 否（HTML 逐位元不變） | J-1 不撞；J-2 改版面撞「別調報表的呈現……」 |

原稽核的設計清單是 A–J 十條。**第 I 條已經不在清單上**：它講的是「`evaluation/statistics.py`（98 行）與 `evaluation/calibration.py`（53 行）在 `src/` 與 `scripts/` 零 import」，這兩個檔在 `c88746a`（2026-09-09）連同專屬測試檔一起刪掉了，複核判定 STALE。稽核當時的事實是對的（`git ls-tree 5fe8791` 確認兩檔當時存在，行數也逐位吻合），只是被後來的 commit 修掉了。

---

## bug 清單（15 條）

<a id="bug-1"></a>
### 1. popularity baseline 會悄悄拿答案來排名

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：lookback 視窗 `[snap_date − 12M, snap_date)` 若查不到任何列，就把 `window` 換成整張 `label_table`——包含評估當月與之後。只發一則 `logger.warning`。

失敗情境：warehouse 只跑過 2026-01-31 一個月的 label ETL，而 `evaluation.snap_date` 就是 2026-01-31。視窗空 → fallback 到全表 → popularity 分數＝評估當月的真實購買數。baseline 是用答案排的，M/B/Δ 三張表會顯示模型輸給 baseline，報表沒有任何一個字說明。月度趨勢圖也會畫出評估月那一格。

為什麼修不掉就會再犯：`baseline_metrics` 的回傳值沒有任何 leakage flag。

位置：`evaluation/baselines.py:35-45` · `pipelines/evaluation/nodes_spark.py:356-363` · `report_builder.py:676-722`

**複核結果**（2026-09-13）

CONFIRMED，而且比稽核寫的更嚴重。

- `src/recsys_tfb/evaluation/baselines.py:35-44` — `_lookback_window` 先 filter `[snap_date − N 月, snap_date)`，`window.limit(1).count() == 0` 就 `window = label_table`（整張表，含評估當月與之後），只發一則 `logger.warning`。
- `src/recsys_tfb/evaluation/baselines.py:79-85` — fallback 之後 `groupBy(item_col).agg(sum(label))`，沒有任何後續的時間過濾，所以計數確實吃到評估當月。`compute_monthly_purchase_counts` 共用同一個 `_lookback_window`，月度趨勢圖同樣受污染。
- 回傳值沒有 leakage flag：`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:361-362` 只塞 `purchase_counts` 與 `monthly_counts`。

證據等級：讀 code 追路徑。沒有實跑——要造出「視窗為空」需要一整套 warehouse 狀態，成本遠高於讀那三個函式。

**稽核寫錯的地方**

稽核原句：「baseline 是用答案排的，M/B/Δ 三張表會顯示模型輸給 baseline，**報表沒有任何一個字說明**。」

更正：報表不是沉默，是主動印出一句與事實相反的話。`src/recsys_tfb/evaluation/report_builder.py:859-866` 會印 `f"popularity 以過去 {lookback} 個月的歷史購買計數重排。"`，其中 `lookback` 取自 `report_builder.py:733-736` 讀 config 的 `12`，與實際用了哪個視窗無關。fallback 發生時這句話仍然照印。

為什麼這個差別重要：「沒說明」讀者還可能自己起疑；「印了一句與事實相反的話」會主動關掉讀者的疑心。這也代表修法不能只動 `baselines.py`——只修資料、留著說謊的文字，等於沒修。

**動它之前要知道的事**

- **修法候選**。(a) fail loud：空視窗直接 raise。不可能誤讀，但第一次上線（warehouse 只有一個月 label）就跑不動 baseline，而 baseline 只是參考段。(b) 把旗標帶進回傳值與報表：`baseline_metrics["lookback_fallback"] = True`，說明文字改成「視窗為空，已退回全表，此 baseline 含當月答案」。保留可跑性，但多一個要維護的 payload 鍵。
- **無論走哪一條，`report_builder.py:860` 那句寫死 `lookback` 的話術都要一起改。**
- **撞凍結**：修法 (b) 會動 `build_baseline_section` 的說明文字，而 `deliberate-non-goals.md` 的「別調報表的呈現——除了欄名標籤那一部分」明列「`build_metrics_section` 與 `build_baseline_section` 的說明文字」仍然凍結。**要改得先問使用者。**
- **不要拿資料閘 B2 來擋這條。** `deliberate-non-goals.md` 的「別做資料閘 B2（leakage）／B3（零正樣本）」講的 B2 是「label-window leakage columns reach features」（定義在 `core/consistency.py` 的 invariant legend），管的是**特徵**洩漏；這裡是 baseline 的計分視窗，不是同一件事。拿那條擋這個 bug 是誤用。

---

<a id="bug-2"></a>
### 2. 改了設定重繪，會得到新舊混合的報表，退出碼 0

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：切片的停止條件是 `can_load` ＝「檔案在不在」。診斷／CI／aggregates 的 JSON 路徑只用 `${model_version}` 與 `${snap_date}` 當 key，不含任何 config 指紋。相對地 `evaluation_metrics`／`baseline_metrics`／`evaluation_diagnosis_pages` 沒有 catalog entry，永遠重算。

失敗情境：改了 `evaluation.metric.min_positives`（或 `k_values`、某項 `diagnosis.*.enabled`），跑 `--only-node generate_report`。metrics 與 baseline 用新 config 重算，metric_ci、report_aggregates、四份診斷 JSON 因為檔案還在而被切掉，`generate_report` 讀回舊 config 的值。退出碼 0。

同型：`--only-node diagnose_model_capacity` 會吃到上一次的 `evaluation_item_ability`。

位置：`__main__.py:152-158` · `io/json_dataset.py:25-26` · `core/pipeline.py:179-187` · `catalog.yaml:322-347`

**複核結果**（2026-09-13）

CONFIRMED，且比稽核描述的更廣。

- 停止條件＝檔案在不在：`src/recsys_tfb/__main__.py:152-159`（`can_load` → `catalog.exists`）、`src/recsys_tfb/core/pipeline.py:183-187`（`if not can_load(name)` 才往上拉生產者）。
- 路徑 key 不含 config 指紋：`conf/base/catalog.yaml:322-347` 全部是 `data/evaluation/${model_version}/${snap_date}/...`。
- **`model_version` 不涵蓋 evaluation config**：`src/recsys_tfb/core/versioning.py:15-19` 的模組 docstring 寫「derived from the *model-defining* subset of **training** params only」。所以改 `evaluation.metric.min_positives`／`k_values`／`diagnosis.*.enabled` 不會移動任何路徑。
- 三個節點沒有 catalog entry → `DataCatalog.exists` 回 False（`src/recsys_tfb/core/catalog.py:76-79`）→ 永遠重算。

證據等級：實跑（無需 Spark）。腳本 `repro_bug2.py`，建真的 `create_pipeline()`、跑真的 `slice_only`。關鍵輸出：

```
recomputed with the NEW config : ['compute_baseline_metrics', 'compute_metrics', 'prepare_eval_data', 'render_diagnosis_pages']
read back from the OLD config  : ['compute_metric_ci', 'compute_report_aggregates', 'diagnose_config_shift', 'diagnose_item_ability', 'diagnose_model_capacity', 'diagnose_suppression', 'draw_diagnosis_sample_node', 'persist_eval_predictions']
```

比稽核多一項：`render_diagnosis_pages` 也會重跑（它的 output 沒有 catalog entry），但它按檔名讀那四份**舊的**診斷 JSON——所以它會拿舊資料重新產生診斷頁，看起來像是新產出。

**稽核寫錯的地方**

稽核原句：「`generate_report` 讀回舊 config 的值。**退出碼 0。**」

更正：不是完全沉默。`src/recsys_tfb/__main__.py:222-226` 每次切片跑都會印一則 WARNING（「exists() proves presence, not freshness」）。稽核只寫「退出碼 0」，讀起來像沒有任何提示。

不過那則 WARNING 自己也不精確——它說「version IDs cover config only」，而對 evaluation 而言**沒有任何 version ID 涵蓋它的 config**。所以更正的方向是：提示存在但內容有誤，不是提示不存在。

**動它之前要知道的事**

- **修法候選**。(a) 把 evaluation config 指紋折進路徑（新增一個 `eval_config_id`，像 `train_variant_id` 那樣）：對得徹底，但每次微調 config 都變成新目錄，跟 `core/versioning.py:24-28` 講的「coverage vs identity」是同一個張力。(b) 在 JSON payload 裡存下產生時的 config 子集，`generate_report` 讀回時比對、不一致就 fail loud：便宜很多、不動版本語意，但每份 JSON 要多一段 metadata。
- **設計問題 A、F 都掛在這條上**。A（把 `evaluation_metrics`／`baseline_metrics` 落地成 JSON）與 F（把比較報表的 metrics 切出來落地）都會新增 catalog 條目，而新條目會**繼承這條的形態**——路徑 key 一樣只有 `${model_version}/${snap_date}`。這兩條不能在這條之前單獨落地，否則是把一個已知的誤導形態多鋪到幾個產物上。
- **`catalog.exists` 是表級的，所以把產物落地到 Hive 不會自動修好這條**。`src/recsys_tfb/io/hive_table_dataset.py:284-286` 的 `exists()` 就是 `_table_exists`——問的是「這張表在不在」，不是「這次執行要的那個月的分區在不在」。框架對這個陷阱有解法，叫 `month_plans`（`src/recsys_tfb/__main__.py:122-135` 的 `_make_can_load` docstring 逐字寫著它，並註明是 ADR-0012 的教訓），但 `src/recsys_tfb/__main__.py:1065` 只有 dataset 指令會 `build_month_plans(...)`、`:1110` 傳進去；evaluation 的 `_execute_pipeline` 呼叫（`:1712-1716`）不傳。所以任何把 evaluation 的中間產物改成 Hive 表的修法，都會把「表從第一次執行起就一直在」這個更弱的停止條件帶進來，`--from-node` 之後會讀到上一次設定下寫的分區——那是同一個病的更嚴重版本（從 JSON 產物升級到 row-level 資料）。
- **改到報表？否**——正常情境（沒有改 config 之後切片重繪）下跑出來的報表逐位元不變。差別只在這個 bug 被觸發的那一次執行：修好之後，`generate_report` 不會再讀回舊 config 的值，`render_diagnosis_pages` 也不會再拿舊的診斷 JSON 重畫成看起來像新產出的頁。
- **撞凍結**：不撞。`deliberate-non-goals.md` 的「別在 `[plan]` 那幾行裡加『這個會（重）訓』的標註」只管重訓標註，不擋這裡的新舊混合提示。

---

<a id="bug-3"></a>
### 3. `n_queries` 在同一份 HTML 裡被標成兩個相反的意思

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：`n_queries` 的定義是 `select(*group_cols).distinct().count()`，也就是全部 query 數（docstring 明說 "before filtering"）。報表兩處標成「有正例 query 數」，第三處標成 "Total Queries"。

失敗情境：1,000,000 個 query，其中 50,000 有正例。報表印出「有正例 query 數 ＝ 1,000,000」「排除 query 數 ＝ 950,000」。正確值 `n_queries − n_excluded_queries` 報表沒有任何地方印出。

位置：`metrics_spark.py:645,718` · `report_builder.py:129` · `:1023` · `:1093`

**複核結果**（2026-09-13）

CONFIRMED。

- 值的定義：`src/recsys_tfb/evaluation/metrics_spark.py:645` `n_queries_total = eval_predictions.select(*group_cols).distinct().count()`（全部 query），`:718` 把它當成 `"n_queries"` 回傳（零正例時的早退分支 `:659` 回傳同一個值）。
- 兩處標成「有正例」：`src/recsys_tfb/evaluation/report_builder.py:197` 與 `:1094`，鍵字面是 `"有正例 query 數 n_queries"`。
- 第三處標成 Total：`src/recsys_tfb/evaluation/report_builder.py:1164` `"Total Queries": metrics.get("n_queries")`。

（稽核給的 `report_builder.py:129/1023/1093` 是舊行號，現為 `:197`／`:1094`／`:1164`。）

證據等級：實跑（無需 Spark）。腳本 `repro_bug3.py`，呼叫真的 `build_overview_section`。關鍵輸出：

```
有正例 query 數 n_queries                     1000000
排除 query 數 n_excluded_queries              950000
```

同一張表自我矛盾：若真有 100 萬個 query 有正例，就不可能同時排除 95 萬。真值 `n_queries − n_excluded_queries = 50,000` 報表任何一處都沒有印出。

**動它之前要知道的事**

- **修法候選**。只有一種形狀（改標籤），兩個變體：把 `report_builder.py:197`／`:1094` 的鍵改成「query 數 n_queries（全部）」；或再加一列印出 `n_queries − n_excluded_queries`。後者多一個數字，也多一份要維護的定義。
- **撞凍結**：不撞，而且這是「別調報表的呈現——除了欄名標籤那一部分」目前唯一明示可動的區域。該條記著 2026-09-09（#327）的部分解凍只動了 `build_overview_section` 與 `build_completeness_section` 兩張表的欄名標籤鍵，而 `report_builder.py:197` 正是 `build_overview_section` 的欄名標籤鍵。
- **`report_builder.py:1164` 的 "Total Queries" 不在解凍區**——那一處的標籤是對的（值就是總數），要動的是另外兩處。

---

<a id="bug-4"></a>
### 4. 比較報表會印出捏造的 Δ

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：delta 計算是 `metrics_a.get(k, 0.0) − metrics_b.get(k, 0.0)`，某側缺這個 key 就當 0.0。呈現層取的是兩側 key 的聯集。

失敗情境（per-item）：`per_item` 的 key 集合是「該側有正例的 item」（見 bug 5）。若某個產品在 A 有正例、在 B 沒有，該列印出 `M = 0.42`、`B = (空白)`、`Δ = +0.42`。

失敗情境（overall）：若 B 側所有 query 都零正例，`overall_b` 是空 dict，overall 表每一個 Δ 都印成 A 的絕對值。

位置：`evaluation/compare.py:39-45` · `:48-54` · `comparison/report.py:121-131` · `:164-168`

**複核結果**（2026-09-13）

CONFIRMED。

- 缺 key 當 0.0：`src/recsys_tfb/evaluation/compare.py:41-44` `metrics_a.get(k, 0.0) - metrics_b.get(k, 0.0)`。
- overall 呈現層取聯集：`src/recsys_tfb/evaluation/comparison/report.py:121-123` `sorted(set(overall_a) | set(overall_b) | set(overall_d))`，然後 `:126-129` 各自 `.get(k)`——B 欄拿到 `None`（空白），Δ 欄拿到 A 的絕對值。
- per-item 呈現層取聯集：`src/recsys_tfb/evaluation/report_builder.py:455-458` `all_items = list(per_item_a.keys()) + [i for i in per_item_b.keys() if i not in per_item_a]`，`:438-449` 印 M／B／Δ 三欄。

證據等級：實跑（無需 Spark）。腳本 `repro_bug4.py`，呼叫真的 `build_comparison_result`。關鍵輸出：

```
overall_b     : {}   overall_delta : {'map@5': 0.5, 'recall@5': 0.8}   delta == A's absolute values? True
```

per-item 情境同樣重現：`fund_mix  A=0.42  B=None  -> delta=0.42`。

**稽核寫錯的地方**

稽核原句：「失敗情境（per-item）：`per_item` 的 key 集合是『該側有正例的 item』（**見 bug 5**）。」

更正：per-item 情境的觸發前提是 **bug 7，不是 bug 5**。`src/recsys_tfb/evaluation/comparison/restrict.py:55-63` 把 A／B 兩側限縮到同一組 (entity × item) 再各自重排（`:55` left-semi 限縮 entity、`:56` inner join 限縮 item、`:59-60` 重排、`:62-63` 對兩側各跑一次）；兩側若用同一份 label，`per_item` 的 key 集合**必然相同**，聯集不會有差、Δ 不會被捏造。唯一能讓兩側 key 集合分岔的是兩側 label 不同，而那正是 bug 7 的機制（`restrict.py:65` 的 `if label_col not in b_common.columns:` 只在 B 沒有 label 欄時才 join 當次執行的 `label_table`）。

為什麼這個差別重要：如果照稽核畫的因果圖先修 bug 5（讓零正例 item 進 key 集），bug 4 的 per-item 情境**一點都不會改善**，因為它從來不是 bug 5 造成的。

**動它之前要知道的事**

- **同一個地雷有第二顆，稽核沒提**：`report_builder.py:443-446`，`m_d is None` 時走 `d = (a or 0.0) - (b or 0.0)`。主報表的 baseline 段（`build_baseline_section`，定義在 `report_builder.py:713`；那兩張 M／B／Δ 表在 `:804-816`，呼叫在 `:810`）用的就是同一個 `_per_item_metric_compare_table`。實務上不會爆——baseline 與 model 共用同一批列與同一組 label（`nodes_spark.py:351` 的 `build_baseline_frame` 只換分數），所以兩側 key 集合恆等。列出來是因為修的時候別只修 `compare.py`。
- **修法候選**。(a) 修在 `compare.py`：`.get(k)` 回 `None` 時 delta 也給 `None`。一處修、兩個出口都受惠，但所有讀 `overall_delta` 的地方都要能吃 `None`。(b) 修在呈現層：`comparison/report.py` 與 `report_builder.py` 各自在任一側缺值時印「—」。不動資料契約，但要改兩個檔，且 `report_builder.py:443-446` 那個 fallback 還是留著。
- **撞凍結**：(b) 動的是表格的值與缺值符號，不是欄名標籤 → 撞「別調報表的呈現」。(a) 只動 `evaluation/compare.py`，不碰呈現 → 不撞。

---

<a id="bug-5"></a>
### 5. 該月零正例的 item 從三處同時消失

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：per-item 聚合先 `filter(label == 1)` 再 `groupBy`，零正例的 item 不會出現在 key 裡。巨觀平均的分母是「有正例的 item 數」而非 item 總數；`observation_items` 從 `per_item` 推導，結構上不可能列出 n_pos = 0。

失敗情境：8 個產品，`fund_mix` 這個月零正例。「macro per-item mAP」變成 7 個 item 的平均，上個月是 8 個。`min_positives = 50` 的觀察名單會列出 n_pos 1～49 的 item，唯獨漏掉最極端的 0。

位置：`metrics_spark.py:502,516,695,706-709` · `report_builder.py:332`

**複核結果**（2026-09-13）

PARTIAL。三處裡兩處成立，第三處不存在。

成立的兩處：

- `src/recsys_tfb/evaluation/metrics_spark.py:502` `rel = enriched.filter(F.col(label_col) == 1)`，`:516` `rows = rel.groupBy(*dim_cols).agg(*aggs).collect()` → 零正例 item 不會出現在 `per_item` 的 key 裡。
- 巨觀平均的分母＝有正例的 item 數：`metrics_spark.py:695` `macro_average(per_item, **metric_params)`，輸入就是上面那個缺項的 dict。

證據等級：實跑（Spark，`local[1]`，一次）。腳本 `repro_bugs_5_6_12.py`，輸出存在 `repro_5_6_12.out`。關鍵輸出：

```
items present in eval_predictions : ['dead_item', 'fund', 'fund_stock']
keys in metrics['per_item']       : ['fund', 'fund_stock']
macro by_item denominator (n items averaged) : 2 of 3 items in the data
```

**稽核寫錯的地方**

稽核原句：「`observation_items` 從 `per_item` 推導，**結構上不可能列出 n_pos = 0**」，位置指 `report_builder.py:332`；失敗情境寫「`min_positives = 50` 的觀察名單會列出 n_pos 1～49 的 item，**唯獨漏掉最極端的 0**」。

更正：前半對，後半錯。

- 前半對：`metrics_spark.py:706-709` 確實從 `per_item.items()` 推導，零正例 item 不在裡面。
- 後半錯：`report_builder.py:332-345` 現在是 per-segment 正例組成表（表格本體在 `:333-345`，`:332` 是它上方的註解），跟 observation 無關。而 `observation_items` 在 `src/` 與 `scripts/` **零消費者**——全樹只有 `metrics_spark.py` 自己產生它。**報表根本沒有「觀察名單」這張表**，所以「唯獨漏掉最極端的 0」在報表上不成立：那張表不存在。
- 另外 `conf/base/parameters_evaluation.yaml:81` 是 `min_positives: 0`，現行設定下 `observation_items` 恆為 `[]`。

**動它之前要知道的事**

- **「該月零正例的 item 要不要進 macro 平均」有兩個都站得住的答案**。(a) 不進（現況）：per-item 歸因指標（`map_attr`／`hit_rate`／`mean_pos`）的定義域就是「該 item 是答案的那些列」，零正例時這些量沒有定義，塞 0 進去等於宣稱「這個 item 表現最差」，那是錯的。(b) 進，但要標出來：分母跨月漂移（8 → 7）確實讓 macro 不可跨月比較，這是真問題；修法是額外回報分母（`macro_avg.by_item` 旁邊加 `n_items_in_macro`），而不是把沒定義的值填 0。
- **撞凍結（兩條，方向相反）**。`deliberate-non-goals.md` 的「per-item 指標沒有 precision」記著的價值觀是「沒有定義的量就不要編出來」——同一個推理正好支持 (a)；要走 (b) 得說清楚新增的是分母揭露、不是補值。另一條「別做資料閘 B2（leakage）／B3（零正樣本）」：若把修法做成「零正例就開閘擋下來」，**直接撞**；做成 (b) 那種揭露式修法則不撞。
- **`observation_items` 是設計問題 E 的同一個形態**（宣告了但沒人讀），不是 bug 5 的一部分。動 bug 5 時順手刪它會把兩件事綁在一起。

---

<a id="bug-6"></a>
### 6. segment join 沒對到的列變成一個叫 "None" 的客群，並等權進巨觀平均

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：segment 是 LEFT JOIN，對不到就 NULL；`groupBy(seg_col)` 產生 NULL 群，key 被 `str()` 成 `"None"`，跟真實 segment 等權平均。沒有過濾、沒有計數、沒有 warning。config 檢查 A10 只驗「segment_columns 有沒有對應的 source」，不驗覆蓋率。

失敗情境：monitoring 模式評估母體來自 `inference_population`（讀 `feature_concat`），segment source 設在 `ml_recsys.sample_pool`（ETL 讀 `dim_all_customer`）。30% 的評估客戶不在 sample_pool → per-segment 表多一列 `None` 佔 30% query。極端：一列都沒對到 → `per_segment = {"None": 跟 overall 完全相同}`。

未確認：生產 `sample_pool` 是否覆蓋 `inference_population` 全部客戶。

位置：`evaluation/segments.py:81` · `metrics_spark.py:449-457,697,234-250` · `core/consistency.py:871`

**複核結果**（2026-09-13）

PARTIAL。機制 CONFIRMED，「沒有計數」那半不成立。

- LEFT JOIN：`src/recsys_tfb/evaluation/segments.py:81` `df.join(seg, on=key_columns, how="left")`。
- NULL 被 `str()` 成 `"None"`：`src/recsys_tfb/evaluation/metrics_spark.py:456-458` `raw_key = r[seg_col]; key = raw_key if isinstance(raw_key, str) else str(raw_key)`。
- 等權進巨觀平均：`metrics_spark.py:697` `macro_avg["by_segment"] = macro_average(per_segment)`（等權，沒有 `n_pos` 加權）。
- A10 只驗有無 source：`src/recsys_tfb/core/consistency.py:888-902` 的 `segment_columns_without_source`，函式體只比對 `segment_column` 集合，不看覆蓋率。

證據等級：實跑（Spark，`local[1]`）。腳本 `repro_bugs_5_6_12.py`。關鍵輸出：

```
per_segment keys : ['None', 'vip']
macro_avg.by_segment (equal weight over the keys above): {'map@1': 0.5}
overall map@1 (query-equal-weight)  : 0.7
extreme (0% join coverage): per_segment = ['None']   per_segment['None'] == overall ? True
```

30% 的未對到群拿走 50% 的 macro 權重（0.5 對上 query 等權的 0.7）；極端情況（0% 覆蓋）`per_segment["None"]` 與 `overall` 逐鍵相同，與稽核所述一致。

**稽核寫錯的地方**

稽核原句：「沒有過濾、**沒有計數**、沒有 warning。」

更正：「沒有過濾」「沒有 warning」對，「沒有計數」錯。`src/recsys_tfb/evaluation/report_builder.py:333-345` 的「per-segment 正例組成」表會把 `"None"` 當成一列印出來，含「候選列數」與「query 數佔比」，而且 `collapsed=False`（預設展開）。資料來自 `metrics_spark.py:237` 同樣的 `str(NULL) -> "None"` 轉換。

所以「30% 的評估客戶沒對到」這件事，報表上**看得到一個 30% 的數字**，缺的是「這一列代表 join miss」這個解釋，以及 macro 權重被污染的警示。真正的問題比稽核描述的窄，修法因此也可以更小。

**動它之前要知道的事**

- **修法候選**。(a) 加覆蓋率閘（新增一條 consistency predicate 或 runtime 檢查，miss ratio 超過門檻就 fail／warn）：門檻值本身是規格真空。(b) 保留但改名並標註：key 改成 `"(unmatched)"`，報表那一列加註「segment source 未涵蓋」——最小侵入。(c) 從 macro 平均裡排除，但在 dataset_overview 保留計數：修掉權重污染，但改變了一個既有指標的值。
- **撞凍結**。(a)：`deliberate-non-goals.md` 的「`inference_population` 的唯一性沒有寫進 `consistency.py`」是**同一個形狀**的決策（資料品質保證留在 source_etl，不進 `consistency.py`）。不是字面撞，但要加 A 系列不變量前應該先問，理由同該條。(b) 動報表文字 → 撞「別調報表的呈現」（各段的 `description` 與說明文字仍凍結）。(c) 不動呈現 → 不撞。
- **稽核的「未確認」還是未確認**：生產 `sample_pool` 是否覆蓋 `inference_population` 全部客戶，兩次複核都沒有查證——那要看生產資料，不在 repo 裡。

---

<a id="bug-7"></a>
### 7. 比較的兩側可能用不同版本的 ground truth，而 docstring 宣稱相同

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：`restrict` 只在「B 側沒有 label 欄」時才去 join `label_table`。三個允許的 B 來源裡，`enriched_eval_predictions`（預設）與 `training_eval_predictions` 兩個都自帶 label，B 用的是它自己落地時存的 label。A 側用這次執行的 `label_table`。

失敗情境：`--compare v_prev` 用預設來源。B 的 label 可能是三個月前持久化的。若 label_table 對該月重跑過，`overall_delta` 混進「label 差異」。coverage 段沒有任何 label 版本或時間戳。

文件也錯：`restrict.py` 模組 docstring 寫「both sides are scored against the same ground truth」。

位置：`comparison/restrict.py:5-7,65-70` · `comparison/sources.py:64-68,76` · `nodes_spark.py:181-183`

**複核結果**（2026-09-13）

CONFIRMED。

- 只有 B 側沒有 label 欄時才 join 當次執行的 `label_table`：`src/recsys_tfb/evaluation/comparison/restrict.py:65-70`，判斷式 `if label_col not in b_common.columns:` 在 `:65`。
- 三個允許來源裡兩個自帶 label：`src/recsys_tfb/evaluation/comparison/sources.py:64-68` `MODEL_VERSION_SOURCES = ("enriched_eval_predictions", "ranked_predictions", "training_eval_predictions")`；同檔 `:58-62` 的註解自己寫明「`enriched_eval_predictions` additionally carries **label** / rank / segment columns; `restrict_to_common` … skips the label LEFT JOIN when already present」。預設來源是 `enriched_eval_predictions`（`sources.py:76`）。
- A 側用當次執行的 `label_table`：`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:181`。
- 文件確實自相矛盾：`restrict.py:5-7` 的模組 docstring 寫 “mirroring `prepare_eval_data`'s convention so both sides are scored against the same ground truth”，而 `sources.py:58-62` 寫的是相反的事實。兩份 docstring 在同一個套件裡直接打架。

證據等級：讀 code 追路徑。沒有實跑——要實跑得先造兩個版本的 `label_table` 與一張持久化的 `enriched_eval_predictions`，成本高於收益；而矛盾在兩份 docstring 的字面上已經可以判定。

**動它之前要知道的事**

- **bug 4 的 per-item 情境掛在這條上**（見 bug 4 的「稽核寫錯的地方」）。修這條會連帶動到 bug 4 的 per-item 觸發面；修 bug 5 不會。
- **修法候選**。(a) 讓行為對齊 docstring：`restrict_to_common` 一律丟掉 B 自帶的 label、重 join 當次執行的 `label_table`。語意最乾淨（比的是模型，不是 label 版本），代價是每次 compare 都多一次 join，且失去「B 當時看到的世界」這個資訊。(b) 讓 docstring 對齊行為，並在 coverage 段印出兩側 label 的來源與時間戳。便宜，保留 B 的歷史快照語意，但讀者必須自己判斷 Δ 裡有多少是 label 差異。
- **這一條決定的是「`--compare` 到底在回答什麼問題」**，屬規格層。
- **撞凍結**：`deliberate-non-goals.md` 沒有對應條目。但修法 (b) 要在 coverage 段加文字 → 撞「別調報表的呈現」。

---

<a id="bug-8"></a>
### 8. 產品大類只有 3 類，報表照印 @4 / @5

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

機制：顯示用的 K 是寫死的 superset `[1,2,3,4,5,"all"]`，precision 的分母永遠是 K，不是 `min(K, n_items)`。

失敗情境：3 個大類。precision 印出 `@3 = 0.33`、`@4 = 0.25`、`@5 = 0.20`、`@all = 0.33`。recall 那一列 @3 ＝ @4 ＝ @5 ＝ @all ＝ 1.0。fine 粒度不受影響。

順帶：模組 docstring 的退化說明寫錯：說 K ≥ n_products 時 precision 收斂到 `total_rel / n_products`，實際是 `total_rel / K`。

位置：`metrics_spark.py:80-92,411,44-47` · `report_builder.py:454,495`

**複核結果**（2026-09-13）

CONFIRMED，連「順帶」的 docstring 錯誤也成立。

- precision 分母永遠是 K：`src/recsys_tfb/evaluation/metrics_spark.py:411` `.withColumn(f"precision@{k}", F.col(f"_hits_{k}") / F.lit(k))`，不是 `min(K, n_items)`。
- 計算側真的會算到 @4／@5：`conf/base/parameters_evaluation.yaml:12` `k_values: [1, 2, 3, 4, 5, "all"]`，`metrics_spark.py:86-92` 的 `_resolve_k_values` 只把 `"all"` 解析成 `n_items`，不裁掉大於 `n_items` 的 K。
- 顯示側寫死 superset：`src/recsys_tfb/evaluation/report_builder.py:566` 與 `:846` `cks = _resolve_display_k([1, 2, 3, 4, 5, "all"], n_cat)`；`_resolve_display_k`（`report_builder.py:26-32`）不做任何 clamp。
- 大類確實是 3 類：`conf/base/parameters_evaluation.yaml:42-45` 的 mapping 有 `fund` / `exchange` / `ccard` 三個 key，8 個產品全部有歸屬（`unmapped: singleton` 不會新增）。

證據等級：實跑（無需 Spark）。腳本 `repro_bug8.py`，用真的 `_resolve_k_values`／`_resolve_display_k`／`_families_by_k_table`。關鍵輸出：

```
            @1   @2        @3    @4   @5      @all
precision  1.0  0.5  0.333333  0.25  0.2  0.333333
```

precision 從 @3 開始純粹因為分母變大而遞減，`@all`（＝@3）跟 `@3` 相同；recall 在 @3／@4／@5／@all 全是 1.0。與稽核描述逐位吻合。

**稽核寫錯的地方**

稽核原句：「模組 docstring 的退化說明寫錯：說 K ≥ n_products 時 precision 收斂到 `total_rel / n_products`，**實際是 `total_rel / K`**。」

更正：稽核的更正本身是對的，但它把整句 docstring 說成全錯，其實是半錯。`metrics_spark.py:42-45` 那句只在 `K == n_items` 時成立，`K > n_items` 時才錯（K=5、n_items=3 時是 `1/5 = 0.2`，不是 `1/3 = 0.333`）。

**動它之前要知道的事**

- **這條要分成兩半看，兩半答案不一樣**。docstring（`metrics_spark.py:42-45`）：純文件、無風險。報表印不印 @4／@5：怎麼修有兩種。
- **修法候選**。(a) 顯示側 clamp：`_resolve_display_k` 裁掉大於 `n_items` 的 K。最小改動，`@all` 仍在。(b) 計算側也 clamp：`_resolve_k_values` 不產生大於 `n_items` 的 K。連帶省掉 Spark 的無用欄位，但會改變 `evaluation_metrics` JSON 的鍵集合，舊 artifact 與新報表對不上。
- **「分母不照教科書」在這個 repo 是已知且刻意的**。`docs/operations/known-pitfalls.md` 已登記「報表的 AP@k 分母是 R，不是 `min(k, R)`——所以 `map@1` 恆等於 `recall@1`」，並訂了規則「寫地基公式一律照 code 追一次，別照領域慣例寫」。動 precision 分母前要先分清楚要的是「改定義」還是「別印沒意義的欄」——這兩件事不一樣。
- **撞凍結**：(a)(b) 都是改「印哪些欄」，屬報表呈現而非欄名標籤 → 撞「別調報表的呈現」，要動得先拿到使用者的明確反饋。docstring 那半不撞。

---

<a id="bug-9"></a>
### 9. render_diagnosis_pages 仍是 varargs 形狀，接錯了不會炸

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

失敗情境：簽章是 `(parameters, *_dag_deps)`，靠 `parameters` 放 inputs 第一格才對得上。若被移走，node 收到某個診斷 dict 當 parameters；`.get()` 不會 KeyError，路徑解析成 `data/evaluation/unknown/unknown/diagnosis`，四項全歸 missing 只發 `logger.info`，node 印「0 files from 0 results」並成功返回。

背景：known-pitfalls §12 說「varargs 消失才是真正的修復」。`generate_report` 已修（6 個必填、無 varargs）。

位置：`pipelines/evaluation/nodes_spark.py:507,499-504` · `pipeline.py:137-142`

**複核結果**（2026-09-13）

PARTIAL。形狀與後果 CONFIRMED，失敗情境的嚴重度稽核寫過重。

- 形狀：`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:507` `def render_diagnosis_pages(parameters: dict, *_dag_deps) -> list[str]:`。
- `Node` 不檢查 arity：`src/recsys_tfb/core/node.py:38-64` 的 `_validate` 只檢查 inputs／outputs 非空與不重疊，不比對函式簽章；`core/runner.py` 是位置展開。
- 後果如稽核所述：路徑解析成 `data/evaluation/unknown/unknown/diagnosis`，四項全 missing 只發 `logger.info`（`nodes_spark.py:540-544`），node 成功返回。
- 稽核的背景引用正確：`docs/operations/known-pitfalls.md:290` 逐字有「varargs 消失才是真正的修復」（該句所屬的 §12 標題在 `:286`），且 `generate_report` 現在確實是 6 個必填、無 varargs。

證據等級：實跑（無需 Spark）。腳本 `repro_bug9.py`。關鍵輸出：

```
mutate inputs to: ['evaluation_config_shift', ..., 'parameters']
would that test catch it? -> True
```

腳本前半也重現了後果本身：`render_diagnosis_pages(fake_diagnosis_dict)` 不 raise、回傳 `[]`、路徑解析成 `data/evaluation/unknown/unknown/diagnosis`。

**稽核寫錯的地方**

稽核原句：「靠 `parameters` 放 inputs 第一格才對得上。**若被移走**，node 收到某個診斷 dict 當 parameters。」

更正：「被移走」這件事有測試擋。`tests/test_pipelines/test_evaluation/test_pipeline.py:231-239` 的 `test_every_registry_diagnosis_is_wired_as_a_dependency` 斷言 `node.inputs == ["parameters", *(f"evaluation_{name}" for name in DIAGNOSES)]`，把 `parameters` 釘在第 0 格；複核在記憶體裡把 inputs 重排後該斷言確實轉紅。

varargs 的**機制**確實還在（`nodes_spark.py:507`），但稽核用來論證嚴重度的那條路徑不是無人看守的。

**動它之前要知道的事**

- **varargs 在這裡是刻意的設計**。`nodes_spark.py:508-520` 的 docstring 花了 12 行說明那些 dep 只當 happens-before 邊、刻意不讀值。把它改成 4 個具名參數會讓「每加一項診斷就要改簽章」回來——那正是 registry 化想消掉的東西。
- **真正不證自明的是另一件事**：node 不該在讀不到任何診斷時宣告成功。那跟 varargs 是兩件事。
- **修法候選**。(a) 把 `parameters` 變成 keyword-only（`def render_diagnosis_pages(*_dag_deps, parameters)`）＋ Runner 支援具名綁定：根治位置綁定，但要動框架。(b) 保留形狀，加一道自我檢查：`if not isinstance(parameters, dict) or "evaluation" not in parameters: raise`，或 `results` 全 missing 時 raise 而非 `logger.info`。一行、不動框架，但擋的是後果不是機制。
- **`docs/operations/known-pitfalls.md:309` 提醒這類測試「守不了手動同步的環境」**——隔離環境仍有風險，但那是該檔 §13 的問題，不是這個 node 獨有的。
- **改到報表？否**——兩種修法都不動 `render_diagnosis_pages` 的輸出，診斷頁的內容與版面一樣。(b) 改變的是「讀不到任何診斷時 pipeline 會不會停」，那是這條 pipeline 跑不跑得完，不是報表長什麼樣。
- **撞凍結**：`deliberate-non-goals.md` 的「別為了讓自己新寫的程式碼合規，就去架構約束的例外登記加一筆」只在修法涉及登記例外時相關。不直接撞。

---

<a id="bug-10"></a>
### 10. label_table 的 LEFT JOIN 沒有唯一性保護

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

失敗情境：`label_table` 是使用者自定義來源表。參考 ETL 用 `cust_pool CROSS JOIN candidate_prod` 保證每 (snap_date, cust_id, prod_name) 一列，但那是參考實作的建構保證，不是框架強制的。若使用者的表對某 key 有兩列，候選集從 8 列變 9 列 → rank 重新編號 → AP／precision 全部改變，`n_rows` 只會多 1。

對照：`segments.py:78-80` 對完全相同的形狀做了 `dropDuplicates(key_columns)`。

位置：`pipelines/evaluation/nodes_spark.py:181` · `comparison/restrict.py:66-70`

**複核結果**（2026-09-13）

CONFIRMED（latent，目前沒踩到）。

- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:181` `eval_predictions = ranked_predictions.join(labels, on=identity_cols, how="left")`——label 側沒有 `dropDuplicates`、沒有 count 檢查。
- 同樣沒有保護的第二處：`src/recsys_tfb/evaluation/comparison/restrict.py:70` 的 B 側 label join。
- 全樹只有 `src/recsys_tfb/evaluation/segments.py:78-80` 做了 `dropDuplicates(key_columns)`。
- consistency 的資料閘（B 系列，`core/consistency.py` 的 invariant legend 裡的 B1／B5–B10）沒有任何一條驗 `label_table` 的 grain（每個 key 幾列）。

證據等級：讀 code 追路徑。沒有實跑——fan-out 的算術後果是 join 的定義，實跑只會證明 Spark 的 LEFT JOIN 語意，沒有新資訊。

**稽核寫錯的地方**

稽核原句：「對照：`segments.py:78-80` 對**完全相同的形狀**做了 `dropDuplicates(key_columns)`。」

更正：兩者形狀不同，稽核把一個更糟的修法寫成了現成範本。

- `segments.py:78-80` 去重的是**維度查找表**（segment 值）。重複列代表 source 的資料品質雜訊，任取一列是合理的預設。
- `nodes_spark.py:181` join 的是 **ground truth**。重複列代表「同一個 key 有兩個答案」。對它做 `dropDuplicates` 會**任意挑一個答案**，而且列數會對得上——比 fan-out 更難發現。

**動它之前要知道的事**

- **修法候選**。(a) `dropDuplicates`（稽核暗示的方向）：理由如上，會把「答案有歧義」變成「靜默選一個」。(b) fail loud：join 前檢查 `label_table.groupBy(identity_cols).count()` 有無大於 1。一次額外 shuffle，但把歧義變成當場紅。(c) 什麼都不做，倚賴上游：把保證留在 source_etl 的 `primary_key` ＋ `quality_checks`。
- **撞凍結**：(b) 若做成 consistency predicate，**直接撞** `deliberate-non-goals.md` 的「`inference_population` 的唯一性沒有寫進 `consistency.py`」。該條明寫「所以別做：把這條唯一性補進 `consistency.py`。CLAUDE.md 要求新的一致性不變量都要進那個模組，**這一條是刻意的例外**」，而它給的理由（靠 source_etl 的 `primary_key` ＋ `quality_checks`）正好就是修法 (c)。`label_table` 與 `inference_population` 都是使用者自定義來源表，是同一個決策的同一個形狀。要做 (b) 必須先問使用者。

---

<a id="bug-11"></a>
### 11. rank 與 pos 是兩套帳，同分時互不一致

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

失敗情境：報表診斷區吃上游存的 `rank`，metric 自己用 `row_number()` 重算 `pos`，兩者的同分打破規則都是 undefined。同分時 item×rank 矩陣跟 `mean_pos` 的名次可能相反。`persist_eval_predictions` 寫進 Hive 的 rank 是第三次獨立求值。

順帶：`eval_predictions` 從來沒有 cache，5 個消費者各自重跑一次 plan。

位置：`nodes_spark.py:574-586,216,199-200` · `metrics_spark.py:283-284` · `pipelines/inference/nodes.py:570-577`

**複核結果**（2026-09-13）

PARTIAL。兩套帳 CONFIRMED，「還有第三處程式碼也在算一次 rank」REFUTED。

兩套帳成立：

- metric 側自己重算 `pos`：`src/recsys_tfb/evaluation/metrics_spark.py:283-284` `Window.partitionBy(*group_cols).orderBy(F.col(score_col).desc())` ＋ `F.row_number()`；`:281` 的 docstring 自承「Tie-breaking among equal scores is undefined」。
- 報表診斷區吃上游存的 `rank`：`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:575-577`（`rank_col = schema["rank"]`，選進 `needed` 後餵給 `aggregate_report_diagnostics`），最終用在 `src/recsys_tfb/evaluation/diagnostics_spark.py:128-166` 的 item×rank 矩陣。
- 監控模式的 `rank` 來自 inference：`src/recsys_tfb/pipelines/inference/nodes.py:576-577`；同檔 `:571-575` 的註解自己寫明「ties broken arbitrarily … `row_number` numbers tied rows 1, 2, 3 in whatever order the shuffle produced」。

順帶的效能宣稱 CONFIRMED：`eval_predictions` 全樹沒有 cache（`nodes_spark.py` 只有 `:578` 對自己的投影做 cache），預設模式下 5 個消費者（`draw_diagnosis_sample_node`／`compute_metrics`／`compute_baseline_metrics`／`compute_report_aggregates`／`persist_eval_predictions`）。

證據等級：讀 code 追路徑。沒有實跑——要重現「同分時兩套帳不一致」得刻意製造同分加上跨 stage 的 shuffle 非決定性，在 `local[1]` 上很可能重現不出來，而重現不出來不等於不存在。

**稽核寫錯的地方**

稽核原句：「`persist_eval_predictions` 寫進 Hive 的 rank 是**第三次獨立求值**」，位置引 `pipelines/inference/nodes.py:570-577`。

更正：`persist_eval_predictions` 是純 pass-through。`src/recsys_tfb/pipelines/evaluation/comparison_nodes.py:116-124` 的函式體只有 `return eval_predictions`，docstring 自述「This function exists solely as the named DAG edge」——它不算 rank。稽核引的 `inference/nodes.py:576-577` 是 inference pipeline 的 `rank_predictions`，那是監控模式裡 rank 的**來源**（第一套帳），不是第三次求值。

正確的計數是 2 套帳，不是 3：監控模式是 inference 算 rank ＋ metric 算 pos；post-training 模式是 `nodes_spark.py:196-200` 補算 rank ＋ metric 算 pos。

**更正要傳播到結論**：算 rank 的程式碼有兩處，不是三處。所以「metric 層不再重算 `pos`」這個修法消掉的是兩處裡的一處，而不是三處裡的兩處。

另外有一個稽核沒講清楚、但真的存在的風險，它跟「幾處程式碼在算 rank」是不同的一件事：`eval_predictions` 沒有 cache，persist 寫出時會把整個 plan 從頭重跑，而**同一段 `row_number()` 被跑第二次時，同分列的名次不保證跟第一次一樣**。那是同一段程式碼被執行多次，不是多了一份程式碼。

**動它之前要知道的事**

- **修法候選**。(a) 統一同分打破規則：兩處都改成 `orderBy(score.desc(), item_col)`（加一個決定性 tiebreaker）。便宜、根治不一致，但會改變既有 `ranked_predictions` 的 rank 值，且 inference 已落地的資料不會自動一致。(b) 單一來源：metric 層不再重算 `pos`，直接用上游的 `rank`。消掉整套帳，但 `restrict_to_common` 之後候選集會縮，那時**必須**重排，所以這條不能無條件套用。
- **cache 那半是效能議題，不是正確性議題**，但它跟「跨次不一致」是同一個根：只要 plan 會被重跑，同分列的名次就沒有跨次保證。
- **撞凍結**：`deliberate-non-goals.md` 的「inference 的 `entity_bucket` 沒有把『讀的桶數』和『寫的桶數』分開」是不同議題。沒有直接撞的條目。

---

<a id="bug-12"></a>
### 12. per_item_segment 的底線串接 key 會撞號

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

失敗情境：key 是 `"_".join(...)`，後寫的覆蓋先寫的。`(item="fund", seg="stock_vip")` 與 `(item="fund_stock", seg="vip")` 都變成 `"fund_stock_vip"`。目前 8 個產品沒有任何一個是另一個的 `_` 前綴。

位置：`metrics_spark.py:521-527,702-704`

**複核結果**（2026-09-13）

CONFIRMED。

- `src/recsys_tfb/evaluation/metrics_spark.py:525` `key = "_".join(str(r[c]) for c in dim_cols)`，`:526-527` `out[key] = {...}`——後寫覆蓋先寫。
- 呼叫點：`metrics_spark.py:691-693` `aggregate_per_item(enriched, [item_col, active_seg_col], ...)`。
- 消費端：`macro_avg["by_item_segment"]`（`metrics_spark.py:700-702`）直接吃這個撞過號的 dict。
- 稽核說「目前 8 個產品沒有任何一個是另一個的 `_` 前綴」成立：`conf/base/parameters_evaluation.yaml:42-45` 的 8 個產品（`fund_stock`／`fund_bond`／`fund_mix`／`exchange_fx`／`exchange_usd`／`ccard_bill`／`ccard_cash`／`ccard_ins`）兩兩不成前綴關係，現行資料下不會踩到。但 item 值由使用者的 `schema.categorical_values` 定義，不是框架保證。

證據等級：實跑（Spark，`local[1]`）。腳本 `repro_bugs_5_6_12.py`，刻意造 `(item="fund", seg="stock_vip")` 與 `(item="fund_stock", seg="vip")`。關鍵輸出：

```
distinct (item, segment) cells with positives = 4  ->  keys actually emitted = 3
```

4 個 cell 收斂成 3 個 key，一個 cell 的數字被靜默覆蓋。

**動它之前要知道的事**

- **修法候選**。(a) 換分隔符：用不可能出現在值裡的字元（例如 `"\x1f"`）。一行改動，但 key 變成不可讀，且已落地的 JSON 鍵集合改變。(b) 改成巢狀 dict：`{item: {segment: metrics}}`。結構上不可能撞號、可讀，但 `macro_average` 與所有消費端都要改成兩層。
- **兩種修法都會改變 `per_item_segment` 的鍵集合**，而這個鍵集合最終會不會出現在報表上，兩次複核都沒有追到底（只確認 `macro_avg["by_item_segment"]` 吃它）。動之前要先確認這一點。
- **撞凍結**：沒有對應條目。不撞。

---

<a id="bug-13"></a>
### 13. 監控模式跑不完整條 pipeline

**稽核原文**（2026-09-12 成本剖析時撞到，不在 2026-09-09 稽核的 12 條之內）

`diagnose_config_shift`、`diagnose_item_ability`、`diagnose_suppression` 都要 `score_uncalibrated` 欄，而 inference 產出的 `ranked_predictions` 沒有這欄 → 第一個診斷 node raise。`training_eval_predictions` 有，所以 `--post-training` 跑得完。

位置：`diagnosis/metric/config_shift/_compute.py`（SCORE_COL）· `item_ability/_compute.py` · `suppression/_compute.py` · `conf/base/catalog.yaml` 的 ranked_predictions columns

**複核結果**（2026-09-13）

CONFIRMED。

- `ranked_predictions` 的 catalog schema 沒有 `score_uncalibrated`：`conf/base/catalog.yaml:510-523` 的 `columns:` 只有 `cust_id` / `score` / `rank`（partition 欄 `snap_date` / `prod_name`）。
- 抽樣層**靜默**丟掉這一欄：`src/recsys_tfb/diagnosis/metric/sample.py:173-177`，`keep_cols` 用 `if c in eval_predictions.columns` 過濾，沒有就不選，不報錯。
- 三個診斷都硬要這一欄且刻意不 fallback：`src/recsys_tfb/diagnosis/metric/config_shift/_compute.py:94` `SCORE_COL = "score_uncalibrated"` ＋ `:400-402` `raise ValueError`；`item_ability/_compute.py:86` ＋ `:260-261`；`suppression/_compute.py:89` ＋ `:149-151`。
- 三者都預設啟用：`conf/base/parameters_evaluation.yaml:119-120`／`:127-128`／`:143-144` 全是 `enabled: true`；`make_diagnosis_node`（`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:455-457`）只有 `enabled: false` 才寫 stub。
- 第一個炸的是 `config_shift`：`src/recsys_tfb/diagnosis/metric/contract.py:85-87` `DIAGNOSES = ("config_shift", "item_ability", "model_capacity", "suppression")`，拓撲順序照這個排。
- `training_eval_predictions` 有這一欄，所以 `--post-training` 跑得完，與稽核一致。

證據等級：讀 code 追路徑。沒有實跑——要實跑得先跑完一整條 inference pipeline 產生 `ranked_predictions`；而三個 `raise` 的條件是純欄位存在性檢查，路徑無分支。

**動它之前要知道的事**

- **修法候選**。(a) inference 也寫出 `score_uncalibrated`：改 `conf/base/catalog.yaml` 的 `ranked_predictions` columns ＋ inference 端產出。三個診斷在監控模式都能跑，代價是 inference 輸出表變寬、要重跑。(b) 監控模式自動停用這三項診斷：在 `make_diagnosis_node` 或 config 層依來源決定 `enabled`。便宜，但「監控模式看不到這三項診斷」變成永久狀態，而它們正是監控最想要的東西。(c) 保持 raise，但把訊息改成可操作的（明講「監控模式請用 `--post-training`，或讓 inference 寫出 `score_uncalibrated`」）。最便宜，只是把失敗變得可理解，沒有真的修好。
- **(a) 的成本取決於一個 repo 外的事實**：inference 是否已在生產環境部署。若尚未部署，改輸出表沒有相容包袱，(a) 比看起來便宜很多。這個前提沒有寫在 repo 裡，動之前要先確認它還成立。
- **落地成 Hive 表之後，這三個 `raise` 會變成不 raise**。這條 bug 的守衛是「欄位在不在」（`config_shift/_compute.py:400-402` 的 `if SCORE_COL not in pdf.columns: raise`，另兩個同型），而 `enriched_eval_predictions` 是 `columns: "auto"`、兩種模式共用同一張表（`conf/base/catalog.yaml:392-402`；`docs/pipelines/evaluation.md:515`）。Hive 寫入層 `src/recsys_tfb/io/hive_table_dataset.py:522-526` 對「表上有、DataFrame 沒有」的欄會補一欄 `F.lit(None)`。所以只要同一個 `(model_version, snap_date)` 先跑過 `--post-training`（表上因此有了 `score_uncalibrated`），之後監控模式再從那張表讀回來時，那一欄**存在但整欄是 NULL**——`diagnosis/metric/sample.py:173-177` 的 `if c in eval_predictions.columns` 會把它收進樣本，三個診斷的存在性檢查全部通過，然後在全 NaN 上算下去。失敗從「當場炸」變成「安靜地算出沒有意義的數字」。今天還沒發生，是因為那些欄只出現在 `persist_eval_predictions` 的區域變數上、不會流回消費者；只要 `eval_predictions` 變成任何消費者都從表讀回的 catalog 條目，這條路就打開了。要改這條 bug 的人必須先確定自己沒有同時把那個窗口打開。
- **同一機制的第二個出口**：`metrics_spark.py:667-671` 的 `active_seg_col` 取的是「`segment_columns` 裡第一個出現在 `enriched.columns` 的」。前一次執行留在表上的 segment 欄以全 NULL 被補回來，就會把它點亮，`per_segment` 因此多一個 key 為 `"None"` 的桶——那正是 bug 6 的形態，但來源完全不同。條件是兩次執行的 `segment_sources` 不一樣。
- **撞凍結**：沒有對應條目。不撞。

---

<a id="bug-14"></a>
### 14. `restrict_to_common` 與 `comparison/alignment.py` 對「共同母體」用了不同定義

這一條原本編在設計清單的 G。2026-09-13 複核發現它的後果不是「算兩次浪費」，而是「印給讀者的 coverage 數字跟實際被保留的母體不是同一個量」，所以搬到 bug 清單。設計清單的 G 留了一行指過來。

**稽核原文**（2026-09-09，原設計問題 G；行號為 `5fe8791` 當時）

`restrict_to_common` 在 pipeline 層做了 2 次 item collect ＋ 2 次 count ＋ 1 次 intersect count，而 `comparison/alignment.py` 內部再 collect 一次同樣的 item 集合、再算一次 `common_items`。node 註解說「no extra Spark work」只對「不再多算第五次」成立。

位置：`comparison_nodes.py:58-71` · `comparison/alignment.py:88-92`

**複核結果**（2026-09-13）

CONFIRMED，而且比稽核寫的重。

算兩次成立：pipeline 層 `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py:58-59` 兩次 item `.collect()`、`:61-65` 兩次 `.count()` ＋ 一次 `intersect().count()`、`:71` `common_items = a_items_full & b_items_full`；純函式層 `src/recsys_tfb/evaluation/comparison/alignment.py:85-87` 再 collect 一次同樣的 item 集合、再算一次 `common_items`。node 註解（`comparison_nodes.py:67-68`）寫「no extra Spark work, and the same value `_restrict` derives internally」——「no extra Spark work」只對「交集本身不用再算一次」成立，對那兩次 `.collect()` 不成立。

定義不同（稽核沒抓到的部分）。下表「用什麼鍵」欄裡的 `[time]` 指 schema 的 `time` 角色欄（在示例設定裡是 `snap_date`），`entity` 是 schema 的 `entity` 角色欄清單；`[time] + entity` 合起來就是一個 query group 的識別鍵：

| 在哪 | 用什麼鍵 | 用什麼運算 |
|---|---|---|
| `comparison_nodes.py:61-65`（給讀者看的 coverage 數字） | `[time] + entity` | `intersect()` |
| `alignment.py:62-71`（真正決定留下哪些列） | 只有 `entity` | `join(..., how="left_semi")` |

而 `alignment.py:64-70` 的註解逐字說明為什麼**不能**用 `intersect`：「A left-semi join, not `intersect`. … they disagree on nulls: `intersect` treats `NULL == NULL` as a match, while the equi-join that `restrict_to_common` then runs drops null keys.」

所以 `coverage_partial["n_query_group_common"]`（`comparison_nodes.py:86`，會印進 `report_comparison.html` 的 coverage 段）是用**被同一個 repo 判定為錯誤語意**的運算、在**不同的鍵**上算出來的，跟實際被保留的母體不是同一個量。

證據等級：讀 code 追路徑。

**稽核寫錯的地方**

稽核原句（標題）：「共同母體算兩次，兩處定義**可能**對不上」。

更正：不是「可能」，是**已經**對不上——鍵不同、運算不同，而且其中一處用的正是另一處 docstring 明說是錯的語意。稽核把一個已成立的事實寫成了風險，也因此把它歸類成設計問題而非會誤導讀者的 bug。

**動它之前要知道的事**

- **修法候選**。去重那半：讓 `common_universe` 回傳它已經算出的東西，node 不再自己 collect——把回傳從 `(common_entities, common_items)` 擴成同時帶回 `a_items` / `b_items`，或另開一個回傳 coverage 的純函式。動 2 個檔（`comparison/alignment.py` ＋ `comparison_nodes.py`）＋ 測試，coverage 數字不變。統一定義那半：讓 coverage 數字改用 left-semi、用同一個鍵。
- **兩半的報表影響不同**。只做去重：coverage 數字不變。順手統一定義：有 null 鍵、或 A／B 覆蓋的月份不同時，`report_comparison.html` 上的數字會變。
- **撞凍結**：統一定義那半會改 `report_comparison.html` 上的值 → 撞「別調報表的呈現」。去重那半不撞。
- **不做統一也至少要留字**：兩處用不同語意這件事目前沒有寫在任何註解裡，下一個讀的人會以為它們一致。
- **沒驗到的**：兩種定義在現行資料上實際差多少，兩次複核都沒有量過——只證明了定義不同，沒有證明目前的數字已經錯了多少。

---

<a id="bug-15"></a>

### 15. 同一個 model_version 換模式跑，第二次會在 `persist_eval_predictions` 炸

**稽核原文**

原稽核未列；2026-09-13 審查 ADR 時發現。

**複核結果**（2026-09-13）

CONFIRMED。`enriched_eval_predictions` 一張表被兩種模式共用，而兩種模式寫進去的 `rank` 欄型別不同，Hive 那一層又明文拒絕自動轉型。

- 一張表兩種模式共用：`conf/base/catalog.yaml:392-402` 的 `enriched_eval_predictions` 是 `columns: "auto"`（不宣告欄位清單，寫什麼就是什麼），`partition_filter` 只有 `model_version`、`partition_cols` 只有 `snap_date`。`docs/pipelines/evaluation.md:515`（§7.3〈Training 與 monitoring 共用 enriched partition〉）明寫「同一個 `model_version + snap_date` 的 post-training 與 monitoring evaluation 會寫入相同 `enriched_eval_predictions` 分區」。
- 監控模式的 `rank` 是 **BIGINT**：它直接來自 `ranked_predictions`，而那張表的 catalog 宣告寫死 `conf/base/catalog.yaml:518` `- {name: rank, type: BIGINT}`。
- `--post-training` 的 `rank` 是 **INT**：`training_eval_predictions` 沒有 rank 欄，所以 `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:192-200` 當場補——`if rank_col not in eval_predictions.columns:` 之後呼叫 `rank_within_query`，那個函式是 `src/recsys_tfb/evaluation/metrics_spark.py:284` 的 `F.row_number()`，Spark 的 `row_number()` 回 INT。
- Hive 寫入層對同名不同型直接 raise：`src/recsys_tfb/io/hive_table_dataset.py:488-502`，把 DataFrame 與表的 schema 逐欄比對，型別不同就 `raise ValueError("Type conflict writing to Hive table … Schema evolution never casts; fix the upstream dtype or rebuild the table.")`。
- 炸點是 pipeline 的最後一個 node：`persist_eval_predictions`（`src/recsys_tfb/pipelines/evaluation/comparison_nodes.py:116-124`）就是那個把 `eval_predictions` 寫進 `enriched_eval_predictions` 的命名 DAG 邊。前面所有 Spark 都算完了才炸。

所以：同一個 `model_version` 先跑一種模式、再跑另一種模式，第二次會在最後一個 node 失敗，訊息是 `rank` 的型別衝突。

證據等級：讀 code 追路徑。沒有實跑——要實跑得先在同一個 `model_version` 上跑完兩種完整模式，成本遠高於比對兩個宣告；而型別衝突的判斷式（`:488-492`）是逐欄字串比對，路徑無分支。

**稽核寫錯的地方**

無。這一條不在 2026-09-09 稽核的清單上。

**動它之前要知道的事**

- **修法候選**。最小的一種：讓 `prepare_eval_data` 補出 `rank` 之後就地 cast 成與 `ranked_predictions` 宣告一致的型別（`nodes_spark.py:192-200` 那個分支尾端加一次 `.cast("bigint")`），兩種模式從此寫出同型的欄。它不動 catalog、不動 Hive 層、不改任何既有分區的內容。代價是把「rank 的型別」這個約束從 catalog 宣告複製到一段 Python，兩邊要一起看。
- **另一半的選擇是：型別要以哪一邊為準**。以 `ranked_predictions` 的 BIGINT 為準，`--post-training` 這條路的 `rank` 就變寬；反過來改 catalog 宣告成 INT，則要確認 inference 端寫出的值也是 INT，而那張表已經有落地資料。這是規格層的選擇，不是實作細節。
- **繞過法（不是修法）**：兩種模式用不同的 `model_version`，或換模式前先 drop 那張表。任何「同一個 model_version 要跑完兩種模式」的驗收計畫都得先寫明用哪一種，否則拿不到證據。
- **跟 `docs/pipelines/evaluation.md` §7.3 的關係**：§7.3 已經記著兩種模式共用同一個 enriched 分區，並提醒「不要假設 enriched partition 同時保存 training test 與 production monitoring 兩種母體」。它講的是**內容**會互相覆蓋；這一條加的是**型別**會讓第二次根本寫不進去。修完之後 §7.3 要補這一句，否則讀者會以為覆蓋是唯一的後果。
- **跟 bug 13 的關係**：bug 13 是監控模式在**第一個診斷**就 raise，這一條是換模式時在**最後一個 node** raise。兩者都由「兩種模式共用一張表、但欄位不一致」造成，但觸發點與修法不同，不要當成同一條處理。
- **沒驗到的**：實際跑兩種模式撞出這個 `ValueError` 的訊息原文沒有取得過；上面的訊息字串是從 `hive_table_dataset.py:498-502` 抄的，不是從 log 抄的。

---

## 設計問題清單（8 條）

<a id="design-a"></a>
### A. 「純函式可離線重繪」這個宣稱，被 catalog 抵銷了

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

`evaluation_metrics`、`baseline_metrics`、`evaluation_diagnosis_pages` 在 catalog.yaml 完全沒有 entry，雖然 node docstring 與 catalog 註解都宣稱主報表能離線重繪。

痛點：`--only-node generate_report` 會把 `prepare_eval_data`、`compute_metrics`、`compute_baseline_metrics` 全部拉回來重跑全量 Spark。

建議形狀：給 `evaluation_metrics`（＋ baseline）各一個 `JSONDataset`。

位置：`nodes_spark.py:559-564` · `catalog.yaml:343-347` · `pipeline.py:97,102,141`

**複核結果**（2026-09-13）

CONFIRMED。

- 三個 node 產物在 catalog 零 entry：`grep -rn "^evaluation_metrics:\|^baseline_metrics:\|^evaluation_diagnosis_pages:" conf/` 回 0 行（對照組 `grep -c "evaluation_report_aggregates" conf/base/catalog.yaml` 回 1，證明 grep 抓得到東西）。產出點在 `src/recsys_tfb/pipelines/evaluation/pipeline.py:97`（`evaluation_metrics`）、`:102`（`baseline_metrics`）、`:141`（`evaluation_diagnosis_pages`）。
- 宣稱在三處：`conf/base/catalog.yaml:343-344`（「落地的理由不只是快取：generate_report 因此變成純函式，主報表可以離線重繪」）、`src/recsys_tfb/pipelines/evaluation/nodes_spark.py:561-563`（同一句話的來源）、`nodes_spark.py:603-607` 的 `generate_report` docstring（「so this function stays pure — no SparkDataFrame in the signature」）。
- 機制在 `src/recsys_tfb/core/pipeline.py:163-187`（`_slice_with_expansion` 沿 `node.inputs` 往上找生產者，`can_load` 為 False 才拉進來）。

證據等級：讀 code ＋ grep。

**稽核寫錯的地方**

稽核原句：把 A 描述成一個被忽略的宣稱衝突，並建議「給 `evaluation_metrics`（＋ baseline）各一個 `JSONDataset`」。

更正：這個成本**已經被登記且被測試釘住**。`tests/test_pipelines/test_resume_contracts.py:146-158` 用四個節點名逐一釘住「resume 在 `generate_report` 會拉回 `prepare_eval_data` / `compute_metrics` / `compute_baseline_metrics` / `render_diagnosis_pages`」，註解寫「Documented cost, pinned here.」——那是 `pipeline-node-design.md` 規則 7（「產物落不落地，是接續成本的決定；誰擋得住＝部分：`RESUME_CONTRACTS`」）指定機制下的登記項。

所以真正壞掉的比稽核寫的窄也更明確：**`conf/base/catalog.yaml:344` 那句「主報表可以離線重繪」與同一個 repo 的登記合約互相矛盾**。函式確實是純的（拿三份 dict 就能離線重繪），但 pipeline 拿不回那三份 dict——沒有任何一份落地。讀者照 catalog 註解去跑 `--only-node generate_report` 會得到一次全量 Spark。

稽核的建議方向也指到了較貴的那一半：修那句話是 2 個檔的事；加 `JSONDataset` 要同步改合約，而且會繼承 bug 2 的形態。

**動它之前要知道的事**

- **這條可以拆成兩件獨立的事**。A-1：改掉 `catalog.yaml:343-344` 與 `nodes_spark.py:561-563` 的「主報表可以離線重繪」，改寫成真正成立的版本（`generate_report` 是純函式；三份輸入目前不落地，接續成本在 `RESUME_CONTRACTS` 有登記）。2 個檔、無跨模組、零行為改動。A-2：真的加 2–3 條 `JSONDataset` entry，要同步改 `tests/test_pipelines/test_resume_contracts.py` 的合約，可能再加一條測試。
- **A-2 必須跟 bug 2 綁在一起**。`conf/base/catalog.yaml:322-347` 的路徑 key 只有 `${model_version}/${snap_date}`、`can_load` 只看檔案在不在——落地之後改 config 重繪會讀到舊值而退出碼 0。單獨落地等於把一個已知的誤導形態多鋪到三個產物上。
- **稽核沒提的替代形狀**：不落地，改成「`--only-node generate_report` 在這條 pipeline 上直接 fail loud，訊息指向 `--from-node`」。A-2 買到便宜重繪但引入陳舊風險；替代案不買重繪，但把「看起來成功其實混了新舊」那個形態消滅掉。
- **撞凍結**：不撞。三份 dict 內容不變、報表 HTML 逐位元不變。

---

<a id="design-b"></a>
### B. 「Spark 聚合給報表用」有兩套機制

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

registry 那套（contract → `make_diagnosis_node` → JSONDataset → `render` 回 ReportSection）有圖點預算、共用色階、範圍說明；`compute_report_aggregates` 那套自己一份 `frame_to_json`、自己一套 plotly，三樣都沒有。

最直接的證據：calibration bins 每跑一次算一次、寫進 JSON，而報表明說不畫、也沒有任何診斷讀它——兩個 config 鍵在付 Spark 的錢買沒有讀者的 payload。

建議形狀：`build_item_detail_section` 做成第五項 registry 診斷，`compute_report_aggregates` 退休。

位置：`nodes_spark.py:555-592` · `diagnostics_spark.py:265-310,305-308` · `report_builder.py:563,566-568`

**複核結果**（2026-09-13）

CONFIRMED。

兩套機制的證據：

- registry 那套：`diagnosis/metric/contract.py` → `nodes_spark.py:409-481`（`make_diagnosis_node`）→ `conf/base/catalog.yaml:327-341`（四條 JSONDataset）→ `report_builder.py:1013-1043`（`assemble_diagnosis_pages`）→ `report/pages.py:232-267`（`write_pages`）。三樣設施都用上：圖點預算（`report/figures.py:22` `MAX_FIGURE_POINTS = 2000`、`:30` `assert_within_budget`）、共用色階（`report/scales.py:33,42`）、範圍說明（`report/types.py` 的 `ScopeNote`，四個診斷的 `__init__.py` 都 import 它）。
- 另一套：`nodes_spark.py:555-592`（`compute_report_aggregates`）→ `evaluation/diagnostics_spark.py:229-254`（自己一份 `frame_to_json`）→ `conf/base/catalog.yaml:345-347` → `report_builder.py:632-704`（`build_item_detail_section`）→ `evaluation/distributions.py` 自己一套 plotly。`distributions.py` 全檔沒有 `assert_within_budget`／`MAX_FIGURE_POINTS`／`sequential_scale`／`diverging_scale`（只有 `:100`、`:111` 兩個手傳的 `colorscale` 字串），也沒有 `ScopeNote`。

calibration 是沒有讀者的 payload，CONFIRMED：全樹 grep 只有一個寫入端 `src/recsys_tfb/evaluation/diagnostics_spark.py:306` `out["calibration"] = frame_to_json(...)`，零讀取端（其餘命中是 dataset／training 的 `training.calibration`，無關）。付錢的地方：`conf/base/parameters_evaluation.yaml:67-68`（`include_calibration: true`、`n_calibration_bins: 10`）→ `nodes_spark.py:584-585` → `diagnostics_spark.py:305-308` → `calibration_bins()`（`:172-216`，一次 groupBy ＋ toPandas）。報表明說不畫：`report_builder.py:638-639`。

證據等級：讀 code ＋ grep。

**稽核寫錯的地方**

稽核原句：「calibration bins 每跑一次算一次、寫進 JSON，而報表明說不畫、**也沒有任何診斷讀它**。」

更正：稽核的結論對，但它引用的那句報表文字本身也是假的。`report_builder.py:638-639` 寫「依『排序不是校準』，calibration 曲線移到獨立診斷報表、本段不畫（即使 payload 有 calibration 鍵）」——而那份獨立診斷報表不存在：`DIAGNOSES` 只有 `config_shift`／`item_ability`／`model_capacity`／`suppression` 四項，`grep -rn calibration src/recsys_tfb/diagnosis/` 零命中。稽核只說「沒有讀者」，沒有發現報表那句話在指向一個不存在的東西。

**動它之前要知道的事**

- **這條可以拆成兩件事，報表影響完全不同**。B-1：刪掉 `calibration_bins` 與它的兩個 config 鍵。4 個檔（`diagnostics_spark.py`、`nodes_spark.py`、`parameters_evaluation.yaml`、`tests/test_evaluation/test_diagnostics_spark.py`），無跨模組，報表現在就不畫它 → 零視覺變動。B-2：統一兩套機制。
- **B-2 會動版面**。把 `build_item_detail_section` 做成第五項 registry 診斷 ＝「per-item 細部拆解」整段從 `report.html` 消失、移到 `diagnosis/05-*.html`。這是版面層級的變動 → 撞「別調報表的呈現——除了欄名標籤那一部分」（各段的 `description` 仍凍結，#327 只解凍了兩張表的欄名標籤鍵）。**必須先問使用者。**
- **稽核的建議形狀有個替代**：不搬段落，只讓 `build_item_detail_section` 改用 `report.figures` 的圖與 `report.scales` 的色階，段落留在主報表原位。稽核案買到「一套機制」的完整性但改版面；替代案只買到共用設施（圖點預算、色階一致）、不動版面、不撞凍結，但 `compute_report_aggregates` 這個第二套 pipeline 機制還在。
- **B-1 若要做，順手把 `report_builder.py:638-639` 那句指向不存在報表的話一起改**——但那是說明文字，會撞凍結。

---

<a id="design-c"></a>
### C. 兩個渲染器 ＋ 兩份數字格式器，已經漂移

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

`evaluation/report.py` 與 `report/pages.py` 各有一份 `_fmt_cell`／`_render_table`；前者把 −0 收成 "0"、後者不會。`evaluation/report.py` 還跨套件 import 私有的 `_render_section_extras`。

建議形狀：renderer 拆成「一個 Page」與「殼」兩層，格式器只留 `report/` 那一份。

位置：`evaluation/report.py:12,33-64` · `report/pages.py:87-116` · `report/types.py:48-52`

**複核結果**（2026-09-13）

CONFIRMED，含實跑證據。

兩份 `_fmt_cell`：`src/recsys_tfb/evaluation/report.py:31-46`、`src/recsys_tfb/report/pages.py:87-98`。兩份 `_render_table`：`evaluation/report.py:49-64`、`report/pages.py:115-116`。跨套件 import 私有函式：`evaluation/report.py:12` `from recsys_tfb.report.pages import _render_section_extras`。

證據等級：實跑（純函式，未留腳本檔）。關鍵輸出：

```
-1e-09  | evaluation/report.py: '0'   | report/pages.py: '-0'
```

成因：`evaluation/report.py:28` 的 `return "0" if s in ("-0","-","") else s`，`report/pages.py:97` 沒有這一步。

**稽核寫錯的地方**

稽核原句：「前者把 −0 收成 "0"、後者不會。……建議形狀：renderer 拆成『一個 Page』與『殼』兩層，格式器只留 `report/` 那一份。」

更正：稽核抓到的 `-0` 是較小的那個差異，漏掉了真正會擋住合併的那一個——兩個渲染器的 **HTML escaping 契約相反**。`report/pages.py:186` 對 `section.description` 做 `_escape()`；`evaluation/report.py:163` 直接 f-string 注入不 escape。而主報表**依賴**這個不對稱：`report_builder.py:1066-1073`（`build_diagnosis_links_section`）把 `<a href="diagnosis/index.html">診斷索引 diagnosis/index.html</a>`（在 `:1069`）塞進 `ReportSection.description`。

為什麼這個差別重要：照稽核的建議形狀合併而沒先處理這個契約，主報表的診斷入口會退化成字面文字，不會有任何錯誤訊息，測試也未必轉紅——現有測試斷言的是 `_render_section_extras` 的 formula 與 bullets，不是 `description` 的 HTML。稽核把一個有語意差異的合併寫成了純去重。

另外，稽核的建議「格式器只留 `report/` 那一份」方向也要反過來：`evaluation/report.py` 那一份多修了 `-0`，是較完整的版本。

第三處（同時影響設計問題 B 與 C）：稽核在 B 與 C 反覆推論 `report/` 套件的邊界——誰依賴它、哪些設施是共用的——但沒發現**那個判斷最明顯的來源自己寫錯了**。`src/recsys_tfb/report/__init__.py:3-8` 的模組 docstring 寫「現況：目前唯一的消費者是 `evaluation/report.py` re-export `ReportSection`……`diagnosis/` 與 `evaluation/report_builder.py` 目前都沒有 import 本套件」。實際上 `evaluation/report_builder.py:1015` 有 `from recsys_tfb.report import Page`，四個診斷的 `__init__.py` 全部 `from recsys_tfb.report import ScopeNote`，四個 `_render.py` 全部 `from recsys_tfb.report.figures import ...`；同檔 `:9-12` 寫的「目標狀態」其實已經達成了。這一條稽核沒抓到，因為它是文件腐爛而不是 code 有問題——但 B 與 C 的建議形狀都建立在那個被寫錯的判斷上。

**動它之前要知道的事**

- **合併之前要先決定 `description` 到底是 raw HTML 還是純文字**。這是設計決策，不是搬移。
- **範圍可以縮**。不做「兩層 renderer」那個大重構，只做兩件小事：(a) `_fmt_cell` / `_render_table` 收到 `report/fmt.py` 或新的 `report/tables.py`，兩邊 import 同一份（以 `evaluation/report.py` 的版本為準）；(b) `_render_section_extras` 從 `report/pages.py` 提成公開名字，消掉那條跨套件私有 import。3 個檔 ＋ 對應測試，無跨 pipeline。對應 `pipeline-node-design.md` 規則 12（底線＝模組私有）與規則 5（決策重複寫兩份，機制才共用）。
- **撞凍結：界線不清，屬於要問的那類**。把 `report/pages.py` 的格式器換成 `evaluation/report.py` 那一份，會讓**診斷頁**的 `-0` 變 `0`——凍結條文講的是「報表的呈現」，診斷頁算不算在內沒有明文。反向（主報表換成 `pages.py` 那份）會讓主報表出現 `-0`，明確變差。純搬 `_render_section_extras` 到公開位置則不動任何輸出。
- **判斷「誰依賴 `report/`」時不要用 `src/recsys_tfb/report/__init__.py:3-8` 的模組 docstring**——它寫的現況與實際相反（詳細對照在上面的「稽核寫錯的地方」第三處）。動 B 或 C 之前順手把它改對，成本一個檔。

---

<a id="design-d"></a>
### D. 兩個報表組裝器共用私有函式，同一個 config 鍵兩個預設值

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

`comparison/report.py` 從 `report_builder` import 五個底線開頭的函式。`guardrail_recall_k` 在主報表預設 `[1,2,3,4,5]`、在比較報表預設 `[1,3,5]`。

建議形狀：那五個函式提到公開的 `evaluation/report_tables.py`。

位置：`comparison/report.py:11-18,153,201` · `report_builder.py:655`

**複核結果**（2026-09-13）

CONFIRMED。

- `src/recsys_tfb/evaluation/comparison/report.py:11-18` import 五個底線開頭的名字：`_per_item_metric_compare_table`、`_resolve_display_k`、`_k_to_lookup`、`_n_items`、`_visible_metric_keys`（另加公開的 `build_glossary_section`）。使用點：`:121, :152, :153, :154, :198, :199, :200, :208`。
- 同鍵兩預設：`report_builder.py:725-727` → `disp.get("guardrail_recall_k", [1, 2, 3, 4, 5])`；`comparison/report.py:153` 與 `:199` → `disp.get("guardrail_recall_k", [1, 3, 5])`。
- 順帶（稽核沒提）：`_k_to_lookup` 在 `comparison/report.py` 全檔只出現 1 次（就是 import 那行），**是死 import**。`.flake8` 的 `per-file-ignores` 只放行 `__init__.py:F401`，所以這不是被刻意放行的；但 `.venv` 裡沒裝 flake8，現在沒有任何東西在跑這個檢查。

證據等級：讀 code ＋ grep 計數。死 import 是用 grep 計數判定的，沒有跑 linter。

**稽核寫錯的地方**

稽核原句：「`guardrail_recall_k` 在主報表預設 `[1,2,3,4,5]`、在比較報表預設 `[1,3,5]`」——讀起來像兩個活著的預設值互相漂移。

更正：實際是「一個死的 ＋ 一個活的」。`report_builder.py:725-727` 是主報表**唯一**讀 `report.display.guardrail_recall_k` 的地方，而它算出來的 `rec_ks` 在整個 `report_builder.py` 只出現那一次（`grep -n rec_ks` 只有 1 行），沒有任何使用點。

推論（稽核沒做）：`evaluation.report.display.guardrail_recall_k` 對 `report.html` **完全沒有作用**。它在 `conf/base/parameters_evaluation.yaml:63` 被宣告、在 `docs/pipelines/evaluation.md` 被寫進使用者文件，實際只影響 `report_comparison.html`。

為什麼這個差別重要：若照直覺把預設值統一成主報表那份 `[1,2,3,4,5]`，等於悄悄改掉比較報表的欄數——動到的是唯一活著的那一邊。稽核把 `rec_ks` 當成設計問題 E 裡的一個附註，沒有把它跟這裡的預設值衝突接起來。

**動它之前要知道的事**

- **兩半的報表影響不同**。D-1（五個函式提成公開的 `evaluation/report_tables.py` ＋ 刪掉 `_k_to_lookup` 死 import）：3 個檔（新檔、`report_builder.py`、`comparison/report.py`）＋ 測試，無跨 pipeline，報表不變。D-2（統一 `guardrail_recall_k` 預設）：活著的那一份是 `[1,3,5]`，統一成 `[1,2,3,4,5]` 會讓 `report_comparison.html` 的 per-item recall 表從 3 欄變 5 欄。
- **D-2 要連設計問題 E 一起問**：這個鍵目前在主報表完全無效，是「該讓它生效」還是「該從主報表的宣告裡拿掉」，跟 E 的四個死鍵是同一個問題。
- **稽核建議形狀的替代**：不新增 `report_tables.py`，改成把這五個放進既有的 `src/recsys_tfb/report/`（該套件的定位是中性呈現層）。稽核案讓 evaluation 的表格邏輯留在 evaluation 套件內，比較貼 `pipeline-node-design.md` 規則 8（模組放哪看 `src/` 側呼叫端在不在本 pipeline 內——兩個呼叫端都在 evaluation 內）；替代案讓 `report/` 成為唯一的呈現層，但會把 metrics-dict 的知識帶進中性層。
- **撞凍結**：D-1 不撞。D-2 改的是使用者看得到的欄數 → 撞「別調報表的呈現」。

---

<a id="design-e"></a>
### E. report.sections 的 8 個開關有一半沒人讀，而文件在教使用者用它們

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

宣告了 8 個，實際被讀的只有 `dataset_overview`、`primary_map`、`diagnostics`、`baseline`。`guardrail_recall`／`per_item_attr`／`category`／`per_segment`／`display.recall_colorscale` 全樹零命中。反向地，程式讀的 `diagnosis_links` 沒有宣告。`report_builder.py:654` 算出來的 `rec_ks` 沒有任何使用點。

`evaluation.md` 的排錯表叫使用者「檢查 `report.sections.per_segment`」。

建議形狀：`_section_on` 對未知鍵 fail loud，或加一條 consistency predicate。

位置：`parameters_evaluation.yaml:52-64` · `report_builder.py:229,447,570,645,654,992`

**複核結果**（2026-09-13）

CONFIRMED。

程式實際讀的 5 個（`src/recsys_tfb/evaluation/report_builder.py`）：`:302` `dataset_overview`、`:520` `primary_map`、`:641` `diagnostics`、`:716` `baseline`、`:1060` `diagnosis_links`。另加 pipeline 端兩個同鍵讀取：`nodes_spark.py:306`（`sections.get("baseline", True)`）、`nodes_spark.py:569`（`sections_cfg.get("diagnostics", True)`）。

yaml 宣告 8 個（`conf/base/parameters_evaluation.yaml:53-60`）：`dataset_overview` / `primary_map` / `guardrail_recall` / `per_item_attr` / `category` / `per_segment` / `diagnostics` / `baseline`。四個沒人讀：`guardrail_recall`、`per_item_attr`（唯一命中是 `tests/test_evaluation/test_parameters_evaluation_yaml.py:27`，斷言的是 yaml 值本身、不是行為）、`category`（只有 `item_categories.enabled` 是活的）、`per_segment`。`display.recall_colorscale` 同樣零命中。

反向：`diagnosis_links` 被讀（`report_builder.py:1060`）但 yaml 沒宣告，CONFIRMED。

**2026-09-13 第二次複核補充**：把這件事講精確——`_section_on` 一共有**五個**呼叫點，不是四個：`report_builder.py:302`（`dataset_overview`）、`:520`（`primary_map`）、`:641`（`diagnostics`）、`:716`（`baseline`）、`:1060`（`diagnosis_links`）。前四個鍵在 `conf/base/parameters_evaluation.yaml:53-60` 有宣告，第五個沒有。而 `_section_on`（`report_builder.py:68-70`）是 `return bool(sections.get(name, True))`——**鍵不在 config 裡就回 `True`**。後果是主報表的診斷入口永遠開著，使用者沒有任何設定可以關掉它，而報表文件也沒寫過這個鍵。這跟四個死鍵是同一個家族、方向相反的漂移：一邊是 config 宣告了程式不問的鍵，另一邊是程式問了 config 沒宣告的鍵。`rec_ks` 死值 CONFIRMED（`grep -n rec_ks` 只有 `:725` 一行）。文件在教使用者用死鍵 CONFIRMED：`docs/pipelines/evaluation.md:545` 的排錯表寫「per-segment section 沒出現 … 檢查 `report.sections.per_segment`」。

證據等級：讀 code ＋ 逐鍵 grep（grep 抓得到東西的對照：同樣的指令對 `primary_map_k` 命中 `report_builder.py:156,729` 等 9 行）。

**稽核寫錯的地方**

稽核原句一：「`guardrail_recall`／`per_item_attr`／`category`／`per_segment`／`display.recall_colorscale` **全樹零命中**。」

更正：對 `per_segment` 而言用詞不精確。`"per_segment"` 這個字串在 `report_builder.py:556,821,822`、`metrics_spark.py:607,713,798` 都有命中——但那是 **metrics dict 的鍵**，跟 `report.sections.per_segment` 這個開關無關。結論（開關沒人讀）成立，理由的措辭會誤導下一個 grep 的人。

稽核原句二（建議形狀）：「`_section_on` 對未知鍵 fail loud，或加一條 consistency predicate。」

更正：前一個方向錯，擋不住它要擋的東西。現況的病是**conf 宣告了程式不問的鍵**；而 `_section_on`（`report_builder.py:68-70`）只在**程式問某個鍵**時才執行，它永遠看不到「conf 裡有一個沒人問的鍵」。「對未知鍵 fail loud」只會抓到反方向的那一種（`diagnosis_links`：程式問了、conf 沒宣告），而那一個目前靠 `sections.get(name, True)` 的預設值正常運作，不是病。

**動它之前要知道的事**

- **修法候選（三個，稽核只提了兩個）**。刪鍵：1 個 conf 檔 ＋ 1 個測試（`test_parameters_evaluation_yaml.py:27`）＋ 1 份文件（`docs/pipelines/evaluation.md:545` 那一列與 §4 的說明）。最小、當下可完成，但沒有機制防止下次再長出死鍵。consistency predicate：進 `src/recsys_tfb/core/consistency.py` 加一條——CLAUDE.md 明文要求新的一致性不變量都要進那個模組，這是 repo 指定的做法。4 個檔（`consistency.py` ＋ 它的 invariant legend ＋ conf ＋ 測試），無跨 pipeline；缺點是程式讀取集合要能被列舉，而目前 `_section_on` 的呼叫點是散的，要先集中成一個常數。`_section_on` fail loud：見上，方向錯。
- **那條 predicate 寫成單向包含會漏掉 `diagnosis_links`**。如果條件訂成「config 宣告的 section 鍵必須全在程式讀取集合內」，`diagnosis_links`（讀了但沒宣告）完全符合——它根本不在被檢查的那一側，會被放行。要同時擋住兩個方向，條件必須是**兩個集合相等**，不是單向包含。這一點決定了修法的形狀，不是實作細節。
- **跟設計問題 D 綁在一起**：`guardrail_recall_k` 目前只對 `report_comparison.html` 有效、對主報表無效，跟這裡的四個死鍵是同一個問題的兩面。
- **撞凍結**：不撞。四個鍵目前都沒作用，刪掉它們、補宣告 `diagnosis_links: true`（`_section_on` 預設就是 True）、改 `evaluation.md` 的排錯列——報表 HTML 逐位元不變。

---

<a id="design-f"></a>
### F. compare 模式把「算」與「畫」壓在同一個 node

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

`generate_comparison_report` 自己跑兩次 `compute_all_metrics` 再組 HTML；標準模式是分開的兩段。指標定義沒有兩份——兩邊都走同一個 `compute_all_metrics`。

真正的問題是產物：catalog 只有 HTML，比較的數字沒有 JSON。`--only-node generate_comparison_report` ＝ 兩次全量 metric。

位置：`comparison_nodes.py:96-113,103-104` · `catalog.yaml:353-355`

**複核結果**（2026-09-13）

CONFIRMED。

- `src/recsys_tfb/pipelines/evaluation/comparison_nodes.py:96-113`：`generate_comparison_report` 在 `:103`、`:104` 各跑一次 `compute_all_metrics`，`:111` 才 `return assemble_comparison_report(...)`。
- 標準模式是分開的：`pipeline.py:94-98`（`compute_metrics`）對上 `:143-150`（`generate_report`）。
- 指標定義沒有兩份：兩邊都走 `metrics_spark.compute_all_metrics`（`comparison_nodes.py:21`、`nodes_spark.py:266`）。
- 產物：`conf/base/catalog.yaml:353-355` 只有 `evaluation_comparison_report`（TextDataset / HTML）。A／B 兩側的 metrics dict 沒有任何 entry（`grep -rn "^comparison_metrics" conf/` → 0）。

證據等級：讀 code ＋ grep。

**動它之前要知道的事**

- **切法**：切成 `compute_comparison_metrics`（產 JSON）＋ `generate_comparison_report`（純函式吃 JSON），HTML 由同一份 `assemble_comparison_report` 產生，內容不變。動 4 個檔：`pipeline.py`、`comparison_nodes.py`、`catalog.yaml`、`tests/test_pipelines/test_evaluation/test_evaluation_compare_pipeline.py`（node 數是被釘住的），無跨 pipeline。
- **切完要在 `RESUME_CONTRACTS` 補一條接續點**，否則跟設計問題 A 一樣是沒登記的成本。這是 `pipeline-node-design.md` 規則 7 指定的機制。
- **落地 JSON 會繼承 bug 2 的陳舊形態**，路徑 key 要含 compare source 的識別，否則換一個 B 來源重跑會讀到上一個來源的 JSON。這一條跟 A-2 一樣，不能在 bug 2 之前單獨落地。
- **撞凍結**：不撞。HTML 由同一份函式產生，內容不變。

---

**G（共同母體算兩次，兩處定義可能對不上）已升格成 bug 14**，不再是設計問題——複核發現兩處不只是「算兩次」，而是用了不同的鍵與不同的運算，印進 `report_comparison.html` coverage 段的數字跟實際被保留的母體不是同一個量，那是會誤導讀者的等級。稽核原文、判定、行號、證據等級與「動它之前要知道的事」全部在 bug 14。A–J 的原編號在這裡保留 G 的位置。

---

<a id="design-h"></a>
### H. evaluation.metric 參數有四份讀取程式碼，其中 metric.k 只有一邊讀

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

四個讀取點：`metrics_spark.py`（不讀 k）、`uncertainty.py`、`_common.py`、`report_builder.py`。`uncertainty.py` 用不到既有的 `_common.metric_params()`，因為 `_common.py` 反過來 import 它。

痛點：把 `metric.k` 設成非 null 時，概覽段會把「不截斷的點估」與「截斷在 k 的 CI」並排，而說明文字宣稱它們相同。A15 只驗值域。

未驗證：從程式碼路徑推的，沒有實跑。最便宜的驗法：在 `local[*]` 環境設 `evaluation.metric.k: 3` 跑一次，比對概覽段的點估與 `map_attr@all`。

位置：`metrics_spark.py:636-641` · `diagnosis/metric/uncertainty.py:42-47` · `_common.py:25,32-41` · `report_builder.py:113-115,1017-1034`

**複核結果**（2026-09-13）

CONFIRMED，而且可以只靠讀 code 定案，不必實跑。完整的因果鏈：

- Spark 主線不讀 `metric.k`：`src/recsys_tfb/evaluation/metrics_spark.py:636-641` 的 `metric_params` 只組 `weight_alpha`／`min_positives`／`shrinkage_k`，沒有 `"k"`。它的截斷來自另一個鍵 `k_values`（`:634` → `:645` `_resolve_k_values`）。`macro_avg["by_item"]`（`:695`）＝ `macro_average(per_item, **metric_params)`，而 `per_item` 的 `map_attr@{n_items}` 走的是 `k_values` 的 `"all"`。
- CI 那條讀 `metric.k` 且真的截斷：`src/recsys_tfb/diagnosis/metric/uncertainty.py:43` `k = metric_cfg.get("k", None)`，`:67` `contrib, row_idx = positive_row_contributions(groups, y, score, k)`。`positive_row_contributions`（`src/recsys_tfb/evaluation/metrics.py:98-107`）的 docstring 明寫「contrib[i] is the within-query cumulative precision of positive row row_idx[i] (zeroed when its rank exceeds ``k``)」。
- 報表把兩者並排並宣稱相同：`src/recsys_tfb/evaluation/report_builder.py:176-178`，`ci_note` 寫「點估 AP 與衡量指標的全量 macro map_attr@all 相同」。其中 `m` ＝ `metric_ci["macro"]`，其 `ap` ＝ `macro_from_per_item(point, counts, **metric_params)`（`uncertainty.py:85`），而 `point` 是 k-截斷過的。所以 `metric.k` 非 null 時，`report_builder.py:178` 那句話為假。
- A15 只驗值域，CONFIRMED：`src/recsys_tfb/core/consistency.py:904-955`（`diagnosis_metric_param_errors`），`:926-932` 只檢查 `k` 是 null 或 int ≥ 1，沒有任何跨讀取點的一致性檢查。
- 四份讀取程式碼 CONFIRMED：`metrics_spark.py:636`、`uncertainty.py:42`、`diagnosis/metric/_common.py:32-41`、`report_builder.py:1085`。循環 import 也 CONFIRMED：`_common.py:20` `from recsys_tfb.diagnosis.metric.uncertainty import paired_bootstrap_delta`，所以 `uncertainty.py` 不能反過來 import `_common.metric_params`。

證據等級：讀 code 追路徑（鏈路完整、每一步有行號）。稽核提的「設 `metric.k: 3` 跑一次比對概覽段」這個最終驗證沒有做。

**稽核寫錯的地方**

稽核原句：「`evaluation.metric` 參數有**四份**讀取程式碼。」

更正：`src/` 裡是四份，但 `scripts/` 裡另有五份逐字相同的 `metric_params()`：`per_item_score_shift_optuna_diagnosis.py:204`、`config_sorting_shift_diagnosis.py:360`、`per_item_score_shift_diagnosis.py:188`、`item_ability_diagnosis.py:303`、`suppression_ledger_diagnosis.py:303`。合計 9 份，不是 4 份。

**動它之前要知道的事**

- **`scripts/per_item_score_shift_*.py` 不得清掉**。`deliberate-non-goals.md` 的「別把 `scripts/per_item_score_shift_*.py` 和它的規劃檔當死碼清掉」明文保護它們（`score_shift` 診斷使用者 2026-07-22 說暫緩，腳本刻意留著當日後評估的依據）。統一讀取程式碼時可以讓它們改用共用函式，不能因為「重複」就刪檔。
- **這條拆兩半，一半要使用者點頭**。H-1（code 半）：把 `metric_params()` 從 `_common.py` 搬到一個 `uncertainty.py` 也能 import 的位置（例如 `evaluation/metric_config.py`——`uncertainty.py` 目前只 import `evaluation.metrics`，不會產生新的循環），四個 `src/` 讀取點全部改用它。5 個檔 ＋ 測試，**跨模組**（evaluation ↔ diagnosis）。H-2（報表半）：改那句 CI 註腳講實話。
- **H-1 本身不會修好 `metric.k` 的語意分歧**。統一讀取之後，`metrics_spark` 仍然可以選擇不用 `k`。要一起決定的是「`metric.k` 到底只作用於診斷側，還是也作用於主線」。
- **撞凍結**：H-2 撞明文列出的凍結項。「別調報表的呈現——除了欄名標籤那一部分」逐字列出仍然凍結的四項，其中一項就是「`build_overview_section` 的 CI 註腳」。**必須先問使用者。**
- **急迫性的前提**：現在預設是 `metric.k: null`（`conf/base/parameters_evaluation.yaml:80`），所以現況下那句註腳是真的；它只在有人把 `k` 設成非 null 時才變假。
- **稽核沒提的替代形狀**：加一條 A 系列 predicate（`core/consistency.py`）——`evaluation.metric.k` 非 null 時 raise，訊息說明「主線 metric 不吃這個鍵，設了會讓概覽段的點估與註腳不一致」。零報表改動、當場可驗收，但把一個現有旋鈕鎖掉；改註腳案保留旋鈕但要解凍。

---

<a id="design-j"></a>
### J. 加一個指標家族要改 6 個檔、10 處寫死清單

**稽核原文**（2026-09-09；行號為 `5fe8791` 當時）

寫死的 metric 名稱清單散在 `metrics_spark.py`（5 處）、`report_builder.py`（5 處）、`comparison/report.py`（2 處）與 `evaluation/metrics.py`。

已經有受害者：NDCG 在 Spark 端每個 K 都算一次 iDCG、收兩次，然後在報表層被 `_HIDDEN_METRIC_PREFIXES` 濾掉，全樹沒有第二個消費者。

位置：`metrics_spark.py:330-349,358,395-414,462-467,504-515` · `report_builder.py:46,415,486,713,814` · `comparison/report.py:160-163,221-224`

**複核結果**（2026-09-13）

CONFIRMED，且範圍比稽核寫的大。

寫死清單（現在的行號）：`src/recsys_tfb/evaluation/metrics_spark.py:344`（`ndcg_contrib@{k}`）、`:358`（`_PER_QUERY_KINDS = ("map", "ndcg", "precision", "recall")`）、`:399,:410`、`:464-467`（`_per_item_metric_cols`）、`:505,:513`；`src/recsys_tfb/evaluation/report_builder.py:47`（`_HIDDEN_METRIC_PREFIXES`）、`:476-477`、`:488`、`:559`、`:572`、`:591`、`:784`、`:805-806`、`:893`；`src/recsys_tfb/evaluation/comparison/report.py:161-162`、`:220-221`；`src/recsys_tfb/evaluation/metrics.py`（numpy 側的 AP 家族）。

NDCG 沒有第二個消費者，逐一查過，CONFIRMED：

- 報表：`report_builder.py:47` `_HIDDEN_METRIC_PREFIXES = ("ndcg",)`、`:50-61` `_visible_metric_keys`；端到端護欄 `tests/test_evaluation/test_report_builder.py:591-604` `test_assemble_report_has_no_ndcg_end_to_end` 斷言 `"ndcg" not in html.lower()`。
- `evaluation_results.json`：不含 ndcg。`pipelines/training/nodes.py:1391-1398` 手挑四個鍵（`overall_map` / `per_item_map_attr` / `n_queries` / `n_excluded_queries`），其餘鍵就地丟棄。
- promote：不讀。`scripts/promote_model.py:39-49`、`:67-75`、`:107-114` 都只用那兩個鍵，排序鍵是 `overall_map`（`:49` 的 `versions.sort(key=lambda v: v["overall_map"], reverse=True)`）。
- MLflow：不讀。`pipelines/training/steps/experiment_log.py:59-65` 同四個鍵。
- 比較報表：濾掉。`comparison/report.py:121,208` 都經過 `_visible_metric_keys`。
- 詞彙表：已退場。`tests/test_evaluation/test_report_builder.py:664-666` 斷言 glossary 不含 `ndcg@k`／`ndcg_attr@k`。

全樹 grep 的其餘 ndcg 命中全部是 LightGBM 訓練期的 `metric: ndcg` / `objective: rank_xendcg`（`core/consistency.py:668,695`、`core/group_utils.py:18,164-174`、`conf/base/parameters_training.yaml:27-28,40-41`），與 `metrics_spark` 的 `ndcg@k` 無關。

證據等級：讀 code ＋ 全樹 grep。

**稽核寫錯的地方**

稽核原句：「已經有受害者：NDCG 在 **Spark 端**每個 K 都算一次 iDCG、收兩次，然後在報表層被 `_HIDDEN_METRIC_PREFIXES` 濾掉。」

更正：稽核低估了範圍——這筆浪費也發生在 **training pipeline**，不只 evaluation。`pipelines/training/nodes.py:69` import `compute_all_metrics`，`:1389` 在 `compute_test_mAP_spark` 裡呼叫一次、`:1409` 在 calibration 開啟時再呼叫一次。每次都會建 `ndcg_contrib@{k}`（`k_values` 預設 6 個值）＋ 每個 k 一次 `aggregate(sequence(1, least(total_rel, k)))` 的 iDCG（`metrics_spark.py:337-349`），然後在 `:1391-1398` 全數丟棄、連序列化都沒有。

**動它之前要知道的事**

- **兩種形狀，範圍差很多**。J-1（刪掉 ndcg，不做指標家族參數化）：動 `metrics_spark.py`（4 處）、`report_builder.py:44-61`（`_HIDDEN_METRIC_PREFIXES` 與 `_visible_metric_keys` 可以整組退休）、以及測試裡真的斷言 ndcg 的那些（`tests/test_evaluation/test_metrics_spark.py`、`test_report_builder.py:306-310,541-554,591-604`）。約 5 個檔，無跨 pipeline，但會改到 training pipeline 的執行成本。J-2（稽核提的參數化）：把指標家族變成 registry，跨 3 個模組、10 處以上寫死清單，而且 `report_builder` 的表格佈局跟家族是耦合的（`:488` `for fam in ("map","precision","recall")` 決定了「一個 family 一張表」的版面）——那是重構等級，且改版面就撞凍結。
- **J-1 會製造一條永遠綠的護欄**。刪掉 ndcg 之後 `test_assemble_report_has_no_ndcg_end_to_end` 變成恆真（假綠）。依 `test-false-green` skill 的判準（那是 `~/.claude/skills/` 底下的 skill，不是 repo 裡的檔案），它應該一起退休，或改成守「`_visible_metric_keys` 這層過濾還在」的其他宣稱。**別留一條永遠綠的護欄。**
- **J-1 有比測試綠更強的證據可拿**：`_HIDDEN_METRIC_PREFIXES` 已經把 ndcg 濾掉，端到端護欄保證 `report.html` 不含 `ndcg`，所以拿掉上游的 ndcg 欄位之後可見鍵集合不變、HTML 逐位元不變；`evaluation_results.json` 也不變（手挑鍵）。這符合 `pipeline-refactor-process.md` 規則 4（切點看「這一半有沒有比測試綠更強的證據」）。
- **撞凍結**：J-1 不撞（HTML 逐位元不變）。J-2 改版面 → 撞「別調報表的呈現」。
- **沒核到的**：稽核寫「6 個檔」，複核只核到 3 個 `src/` 模組 ＋ `evaluation/metrics.py`。稽核的 6 可能把 conf／docs 算進去了，複核沒有去湊那個數字，核的是「寫死清單確實散在多處」這個宣稱本身，它成立。

---

<a id="no-issue"></a>

## 2026-09-09 稽核「查過、確認沒問題的地方」（原樣抄）

這一節是 2026-09-09 稽核列的「不必重查」清單，逐字照抄。兩次複核都沒有重新查證它。

- 四處 join 的 fan-out：segments 先 dropDuplicates；baseline 因 eval 已限縮到單一 snap_date，(time, item) 唯一；comparison 用 left_semi、Python set、groupBy 收斂。
- `collapse_to_categories` 的 inner join 不會靜默丟 item（A4 強制）。
- node inputs 的位置綁定：`generate_report` 6 對 6 且無 varargs；四個 `diagnose_*` 當場核對個數並 raise。
- nDCG 的 iDCG 截斷 `least(total_rel, k)` 正確。
- 零正例 query 的排除整個 query 一起丟。
- post-training 的 label 歧義、Runner 的 MemoryDataset 提前釋放、`--compare-only` 的 B4 閘、外部來源的 prod_mapping：都查過沒問題。

---

<a id="not-verified"></a>

## 兩次複核都沒驗到的事

把兩份複核報告各自的「沒做到／不確定」合併去重。這一節是地雷圖：底下每一項都還沒有證據，不要當成已知事實引用。

**沒有跑過任何完整的 pipeline。** 兩次複核都沒有端到端執行 evaluation 或 inference。實跑的六條 bug（2／3／4／8／9 不需要 Spark，5／6／12 用 `local[1]` Spark 跑了一次）是針對單一函式或單一 node 的重現，不是整條 pipeline 的行為。

**逐條沒有實跑的**：bug 1（要造出空 lookback 視窗需要一整套 warehouse 狀態）、bug 7（要造兩個版本的 `label_table` 與一張持久化的 `enriched_eval_predictions`）、bug 10（fan-out 的算術後果是 join 的定義，實跑沒有新資訊）、bug 11（要刻意製造同分加上跨 stage 的 shuffle 非決定性，在 `local[1]` 上很可能重現不出來——**重現不出來不等於不存在**）、bug 13（要先跑完一整條 inference pipeline 產生 `ranked_predictions`）、bug 14／設計問題 G（只證明了兩處定義不同，沒有量過現行資料上實際差多少）。

**設計問題 H 的最終驗證沒做**：稽核提的「設 `evaluation.metric.k: 3` 跑一次，比對概覽段的點估與 `map_attr@all`」沒有執行。複核改用讀 code 定案，鏈路完整、每一步有行號，結論不依賴實跑，但那條實測仍然是空的。

**測試只跑了一個檔**。bug 複核跑了 `tests/test_pipelines/test_evaluation/test_pipeline.py`（21 passed），其餘測試檔沒跑。設計問題 A–J 的複核**一個測試都沒跑**——那份報告裡所有「測試會不會轉紅」的陳述都是讀測試檔得出的，不是跑出來的。

**bug 12 的報表影響沒追到底**：只確認 `macro_avg["by_item_segment"]` 吃那個撞過號的 dict，沒有追到它最終會不會出現在 HTML 上。

**bug 6 的生產覆蓋率沒查證**：稽核自己標的「未確認：生產 `sample_pool` 是否覆蓋 `inference_population` 全部客戶」仍然未確認——那要看生產環境的資料，不在 repo 裡。

**設計問題 D 的死 import 是 grep 判定的**：`_k_to_lookup` 在 `comparison/report.py` 只有 import 那一行命中，但沒有跑 linter 確認（`.venv` 沒裝 flake8）。

**設計問題 J 的「6 個檔」沒有核到**：複核只核到 3 個 `src/` 模組 ＋ `evaluation/metrics.py`，沒有去湊稽核寫的那個數字。

**兩份複核的分工留了一個縫**：設計問題 A–J 的複核完全沒碰 bug 1–13，bug 複核完全沒碰 A–J。兩份唯一的交會點是「A-2 與 F 的落地 JSON 會繼承 bug 2 的形態」，那是從設計側單向指出的耦合，沒有從 bug 側反向驗證過。
