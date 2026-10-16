---
status: proposed
date: 2026-10-16
---

# evaluation pipeline 第二輪整理的方向：計算與呈現分開、依效率切計算、關掉就不算、不重複算

> **狀態。** 本份是提案，repo 擁有者核准、merge 之後才生效。文中標註分三種：
>
> - **已拍板**：擁有者在討論時已經表態的。
> - **作者建議**：本份作者的提案，核准本份時一併生效。
> - **待決定**：留到重構開工時再逐項拷問（grill）。
>
> **範圍。** 本份只定方向。跟 [ADR-0029](0029-dataset-second-pass-scoped-reads-symmetric-splits.md)、[ADR-0030](0030-training-second-pass-honest-adapter-shared-scoring.md) 不同，本份還沒有盤點整條 pipeline，證據只來自一次具體的改動：#515／PR #520，拿掉信賴區間。「具體要改哪些地方」的決定清單，等盤點之後補進本份。
>
> **相關判準。** 跨 pipeline 的形狀判準在 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)，本份稱「node 規則 N」；流程判準在 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)，本份稱「flow 規則 N」。
>
> **數字是快照。** 文中的行數與計數都是 main @ `11ad407e`（2026-10-16）量的，程式一改就會變；重現指令在附錄二。檔案位置只寫檔名與函式名，不寫行號。
>
> **用詞。**
>
> - 「頭號指標」：主報表最上面那一個 macro per-item mAP。
> - 「計算單元」：一個或幾個 node，一起產出一組結果。
> - 「設定指紋」：見原則 5。

---

## 一、為什麼要整理

#515 要拿掉 evaluation 的信賴區間。這個功能本身只有一個 385 行的模組（`diagnosis/metric/uncertainty.py`），但 PR #520 最後改了 70 個檔，issue 建立到 merge 花了約 2 小時 50 分，還不含建 issue 之前的分析。

時間有一部分跟程式結構無關。PR #520 說明裡列的驗證包括：全量測試、廣告示例的 e2e 比對、兩輪審查，以及 main 中途合了兩個 PR 之後的 rebase 與重驗。

另一部分是結構造成的。70 個檔的分布如下：

| 範圍 | 檔數 | 增／刪行數 | 為什麼要改 |
|---|---|---|---|
| src | 23 | +272／−1124 | 多數是刪除，這部分正常 |
| tests | 23 | +538／−1085 | **新增最多的地方**。其中 146 行是新規則 A61 的測試；其餘多半是因為測試要自己組出報表的每一個輸入，少一個輸入就要改一大片，光是 `metric_ci=` 相關的就約 50 行 |
| docs | 15 | +75／−83 | 多數是刪掉信賴區間的段落、章節重新編號，以及改「數量」的句子 |
| conf、examples | 7 | +16／−38 | 設定鍵與 catalog 條目，這部分正常 |
| `CONTEXT.md`、`README.md` | 2 | +2／−5 | |

一個小功能為什麼會牽動這麼多地方，拆開來有四個問題，分別對應原則 1～4：

1. **報表的入口是固定的幾個位置參數。** 多一個結果或少一個結果，至少要改 6 個地方：catalog、pipeline 的輸入清單、`generate_report` 與 `assemble_report` 的簽章、指紋檢查清單，以及 `report_builder.py` 裡往下傳的每一處；外加每一支組這些參數的測試。
2. **計算照報表的頁切，準備工作重複做。** 信賴區間有三份實作，分別嵌在三個地方。頭號那一份，在一台 8 核 macOS 筆電上用 550 萬列的合成資料量，單獨跑約 45 秒，幾乎都花在準備工作上。這次量測的腳本沒有進 repo。
3. **「關掉了要不要算」散在好幾個地方判斷。** 抽樣 node 會自己去查別人的開關，所以刪信賴區間時，連它也要改。
4. **同一個數字有好幾份算法。** 頭號指標原本是信賴區間的程式順便算出來的。拿掉信賴區間，頭號那張表就跟著不見了，只好改讀 `metrics.json` 裡 Spark 算的同一個值。

---

## 二、目標的樣子

```
  設定開關：使用框架的人關掉哪些指標、診斷
        │
        ▼
  需求清單：這次要哪些結果，每個結果要哪些變體（例如哪幾個 K）
        │                                   ← 只在這裡判斷
        ▼
  計算單元：依效率切，只跑需求清單用得到的
        │  每個結果只由一個單元產出，落地存起來
        ▼
  落地的結果：JSON、表，各自記得「用哪些設定算的」
        │  下游照名字取
        ▼
  報表、診斷頁：只組裝，不計算
```

### 原則總表

| # | 原則 | 現在做到多少 | 誰擋得住 |
|---|---|---|---|
| 1 | 計算歸計算，呈現歸呈現 | 部分：報表讀的是落地的 JSON，但報表層還在算一些數字 | **重構後可加機械檢查**：報表模組不准 import 計算模組（今天加會紅） |
| 2 | 計算單元依效率切，效率要實際量過 | 沒做到：現在是一頁報表一個單元 | 沒人，只能人看 |
| 3 | 需求清單在一處決定，關掉的就不算 | 一半：會省，但判斷散在四處 | **可加測試**：關掉某項之後，組出來的 pipeline 不含它的單元 |
| 4 | 每個結果只算一次、落地，下游照名字取 | 一半：落地做到了，還有重複算 | **部分**：parity 測試只盯得住已知的幾份實作 |
| 5 | 設定改了，只重算有讀到那個設定的單元 | 沒做到 | **可加測試**：改一個只有某單元讀的鍵，只有它的結果算過期 |
| 6 | 每個模組的職責符合框架的角色 | 沒做到：約四分之三的 evaluation 程式放在 pipeline 外面 | **部分**：架構稽核 S3 已有；本份另提一條新稽核，見原則 6 |

---

## 三、各原則

### 原則 1　計算歸計算，呈現歸呈現

**規則。** 計算單元只負責產出結果並落地。報表與診斷頁只讀結果、組裝成畫面，不做任何計算。

**怎麼判斷一段程式算哪一邊。** 問兩件事：這個數字別的地方會不會用到？會不會拿來比較或判斷？兩個都不會，就是呈現，例如格式化、排序後挑前幾名來顯示、決定欄位順序。有一個會，就是計算，要放進計算單元，結果落地。

**現在有兩個缺口。**

第一，報表層還在算數字：

- baseline 那一段（`build_baseline_section`）呼叫 `compare.build_comparison_result` 算差值；
- 預測品質那一段呼叫 `prediction_quality.coarse_bin_table` 與 `threshold_sweep`，重算分箱表與門檻掃描。

這些要搬進計算單元，結果落地。

第二，報表的入口是一串位置參數：

```python
# pipelines/evaluation/pipeline.py
Node(
    generate_report,
    inputs=["evaluation_metrics", "parameters", "baseline_metrics",
            "evaluation_report_aggregates",
            "evaluation_diagnosis_pages",
            "prediction_quality_metrics"],
    outputs="evaluation_report",
)
```

`assemble_report`（`evaluation/report_builder.py`）也是一個結果佔一個參數，再一路往下傳。

**改成。** `generate_report` 用名字接輸入，組成「一包結果」交給報表，每段報表宣告自己要哪幾個名字。框架已經支援照名字接輸入，training 在 ADR-0030 決定 8 用過 `inputs={...}`。

**作者建議：「一包結果」只在 `generate_report` 裡組成一個 dict，不另外落地。** 每個結果本來就各自落地了，另外再落地一份目錄沒有額外好處。

**誰擋得住。** 重構完成後，加一條架構測試：報表模組不准 import 計算模組。今天加會紅，原因除了上面那兩處，`report_builder.py` 還在模組層 import `metrics.py`（例如 `all_positive_share_warns`）與 `baselines.baseline_score`。哪些算「計算模組」、哪些是報表可以用的輕量讀取函式，要在盤點時先定義清楚，這條檢查才寫得出來。

**跟 ADR-0019 的關係。** [ADR-0019](0019-evaluation-modules-split-by-role.md) 決定 5 的結論是「不做兩層渲染器」，那一條講的是 HTML 外殼，本條講的是資料怎麼交給報表，兩者不衝突。

---

### 原則 2　計算單元依效率切，效率要實際量過

**規則。** 這裡的「效率」指時間與記憶體的成本。怎麼把計算分成單元，看兩條：

1. **一起算幾乎沒有成本，就放同一個單元。** 例如同一次 Spark 掃描、同一個 `groupBy`，或已經在 driver 上準備好的同一批陣列。
2. **單獨拿掉能省下可觀的成本，而且使用框架的人會想關，就自己一個單元。**

**現在。** 診斷是「報表一頁對一個計算單元」，每一頁各自準備資料。例如把 query 與 entity 組成分組鍵的 `query_key`，三個診斷模組加上共用 helper，一共呼叫了 8 次，其中 `suppression/_compute.py` 一個檔就呼叫了 5 次。被刪掉的信賴區間也是這個形狀：同樣在 8 核 macOS 筆電、550 萬列合成資料上，頭號那一份重抽 1 次和重抽 200 次都要約 45 秒，表示時間幾乎都花在重抽之前的準備，重抽本身幾乎不花時間。

**補充一：成本要量過再切。** [`pipeline-performance-work.md`](../agents/pipeline-performance-work.md) 記過，dataset 那一輪的六個效能假設，有四個被實測否決。切之前，先量每個計算花多少時間、多少記憶體。

**補充二：可讀性靠宣告，不靠註解。** 依效率切的代價，是很多下游會用到的結果綁在同一個單元裡，讀程式的人不容易看出某段計算在做什麼。但不能用註解寫「這是給誰用的」，因為讀取端一變，註解就過時。改成三件事：

- **計算單元宣告自己產出什麼**：每個結果的名字、一句話定義、粒度。宣告寫在程式裡，並由測試檢查單元真的產出了這些結果。
- **報表與診斷宣告自己讀什麼**：照名字列出要讀的結果。
- **「誰讀了這個結果」由程式從兩邊的宣告算出來**，例如提供一個指令列出「結果 → 產出單元 → 讀取端」。這張對照沒有人手寫，所以不會過時。

名字說「它是什麼」，例如「query 粒度的排名指標」，不說「給哪一段報表用」。一個單元的產出多到一句話講不清楚，就該拆。

**補充三（已拍板）：共用的準備步驟，只做不會改變指標定義的事。** 組鍵、排序、把資料拉到 driver，這類步驟可以共用。「要不要包含沒有正例的 query」會改變指標定義，要由各個指標自己決定：mAP 本來就不計沒有正例的 query；其他指標要不要計，看那個指標的定義。

- **推翻的既有設計。** 現在的共用準備步驟（`make_draw_diagnosis_sample_node`，名字叫「抽樣」）一開始就只留有正例的 query。這是 2026-07-06 診斷整合設計（`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`）定的，理由是「指標只由它們定義」。這個理由對 mAP 成立，對 item_ability 的 ROC AUC 不一定成立。
- **成本。** 要計入沒有正例的 query 的指標，資料量會從約 22 萬個 query 變成全部 query（百萬級 entity × 22 個 item）。現在 550 萬列約佔 1.25GB，driver 有 128GB；全部 query 放不放得下，要先量，放不下就改在 Spark 上算。所以套用這條時，每個指標要一起決定在哪裡算。
- **怎麼套用到 item_ability，跟它要不要留一起決定**，見第四節第 1 步。
- **名字照實改。** 這一步的「抽樣」在正式環境不會觸發：有正例的 query 約 22 萬個，上限 `max_queries` 是 25 萬。實際上它在做的是「準備診斷要用的資料」。

**待決定。** 實際怎麼切，要等盤點量出每個計算的成本。

---

### 原則 3　需求清單在一處決定，關掉的就不算

**規則。** 「使用框架的人的開關 → 這次需要哪些結果 → 要跑哪些計算單元」這條推導只在一個地方做：組 pipeline 的時候。計算單元自己不去查別人的開關。如果某個被關掉的結果，跟其他要算的結果在同一個單元裡、幾乎不花成本，就一起算，報表不顯示就好。

**現在。** 關掉就省，大部分已經做到：關掉的診斷寫空殼，全部關掉時連抽樣都不做。但判斷散在至少四個地方：

- 抽樣 node（`make_draw_diagnosis_sample_node`）會自己查各個診斷開了沒（`_registry_diagnosis_enabled`）；
- 每個診斷 node（`make_diagnosis_node`）查自己的開關；
- 預測品質由報表的段落開關 `evaluation.report.sections.prediction_quality` 決定算不算；
- 監控模式在組 pipeline 時決定不接診斷。

所以這次刪信賴區間，連抽樣 node 也要跟著改。

**已拍板：需求清單寫成一個函式，而且不能變得複雜難維護。** 函式輸入設定，輸出這次要跑的計算單元與變體，`create_pipeline` 照它組；它可以單獨測試。擁有者要求避免複雜難維護，因為計算會常常增減或改動。作者建議的做法：

- 每個計算單元在自己旁邊宣告「哪個開關讓我跑、我產出哪些變體、我讀哪些設定」。最後一項給原則 5 用。
- 需求清單函式只負責把這些宣告收集起來，不另外寫死任何單元的名字。

這樣新增、刪除或修改一個計算時，只要改那個單元自己的宣告，不必到別處同步。

**補充一：需求清單要列到「每個結果要哪些變體」。** #520 的審查抓到一個 bug：頭號指標要的是 `map_attr@{metric.k，沒設才是 all}`，但 Spark 只算 `k_values` 有列的 K；`k_values` 沒列 `"all"`、`metric.k` 又沒設時，頭號那一格是空白。這個 bug 已在 #520 裡修掉（`metrics_spark._resolve_k_grids`，commit `465fa928`），修法是讓計算端一律補算頭號要的 K。教訓是：報表需要什麼，計算端並不知道。如果需求清單把變體也列進去、由計算單元保證算出來，這一類 bug 就不會再發生。

**補充二：報表只讀需求清單裡的結果，空殼可能就不用寫了。** 現在關掉的診斷會寫一個 `{"enabled": false}` 的空殼 JSON，理由有兩個：讓報表的位置參數有東西可收，以及避免報表讀到上一次留下的舊結果。報表改成只讀需求清單裡的結果之後，這兩個理由看起來都不成立。這點列為待決定，盤點時確認。

**跟 ADR-0013 的關係。** [ADR-0013](0013-pipeline-modes-and-slicing-are-separate.md) 的做法是在 `create_pipeline` 裡**明列**要跑哪些 node；本份改成由各單元的宣告**推導**。這是對 ADR-0013 的修訂：核准本份時，在 ADR-0013 的對應段落加一行指向本份。

---

### 原則 4　每個結果只算一次、落地，下游照名字取

**規則。** 每個結果只由一個計算單元產出、落地。報表、診斷頁、比較報表要用，都去讀那一份，不自己再算。

**現在。** 落地已經做到（[ADR-0018](0018-evaluation-materialize-at-producer.md) 決定 2）。還有重複的是 macro per-item mAP，目前有三條算法：

- 主報表：Spark 的 `metrics_spark.macro_average`；
- config_shift 頁：numpy 的 `metrics.compute_macro_per_item_map`；
- item_ability 頁與 suppression 頁：`diagnosis/metric/_common.per_item_ap`，在診斷資料上算。

平常這幾個數字一樣。但有兩種情況會不一樣：開了 `evaluation.query_filter.drop_all_positive_groups`（主報表套用，診斷不套用），或資料量超過 `max_queries`、診斷開始抽樣。這時同一份報表的兩頁會印出兩個不同的 macro per-item mAP，報表讀者看不出哪個才對。

**已拍板：同一個指標，整份報表只能有一個數字。** 作者建議分兩種處理：

- **只當背景數字的**（item_ability 頁、suppression 頁）：直接讀主報表的結果，不自己算。
- **要拿來做前後比較的**（config_shift 頁的「調整前」與「調整後」）：前後兩個數字必須用同一種方法、在同一批資料上算，比較才有意義，所以還是在診斷裡算；但診斷用的資料要跟主報表套同一套篩選規則，這樣「調整前」就會等於主報表的數字。
  - 連帶後果：主報表不抽樣，所以 config_shift 也不能抽樣。現在有正例的 query 約 22 萬個，低於 `max_queries` 的 25 萬，所以目前不會抽樣。但有正例的 query 一超過 `max_queries`，抽樣就會自動恢復，config_shift 會在不報錯的情況下開始在抽樣資料上算。所以要加一道防線：抽樣比例小於 1 時，config_shift 拒算並說明原因。到那時再決定是改在 Spark 上算，還是把 `max_queries` 調高。

**例外一：執行環境逼出來的第二份實作。** HPO 要在記憶體裡反覆算驗證集分數（`training/steps/hpo_scoring.py` → `metric_registry.score_val` → `compute_macro_per_item_map`），不能每次都開 Spark job，所以 numpy 版本有存在的理由。training 的 test 指標走的是 Spark，不在此列。這是 node 規則 5「機制要共用」的例外，所以要有 parity 測試盯住兩份實作，並在第二份的 docstring 寫明它為什麼存在。

**例外二：不是每個中間結果都要落地。** 只落地「下游要讀」或「重算很貴」的結果；很便宜的中間結果留在記憶體裡就好。這是 node 規則 7「產物落不落地，是接續成本的決定」。

---

### 原則 5　設定改了，只重算有讀到那個設定的單元

**現在怎麼運作。** 每份落地的結果（`metrics.json`、各診斷的 JSON）都附一個「設定指紋」，就是「算這份結果時用了哪些設定」的雜湊值。只重畫報表時，程式用現在的設定再算一次指紋，跟結果上記的比對；對不上就拒絕畫，並告訴使用框架的人要從哪個 node 重跑。用意是避免一份報表混到用舊設定算的結果。

**問題在哪。** 每份結果的指紋，都是用同一張設定清單算的（`pipelines/evaluation/steps/config_fingerprint.py` 的 `COMPUTED_KEYS`），不管它有沒有讀那些設定。

**例子。** suppression 診斷頁最後有一張「具體案例」表，列出「某位客戶的某個正例 item，被哪幾個負例 item 排到前面」；設定 `top_examples: 50` 決定最多列幾筆。把它從 50 改成 30，只會影響這一張表。

- 現在：`metrics.json` 根本沒讀這個設定，但它的指紋一樣對不上。報表會要求從抽樣 node 開始重跑，所有指標都要讓 Spark 重算一遍。
- 改成：只有 `suppression.json` 過期，只要重跑 suppression。

**規則。** 每個落地的結果只對「它真的讀過的設定」算指紋。設定改了之後，只有讀過那個設定的結果算過期。每個單元讀哪些設定，用原則 3 的那份宣告來寫。

**附帶的問題。** `COMPUTED_KEYS` 有一列 `("evaluation.diagnosis", "draw_diagnosis_sample_node")`，要求每種模式都要有那個 node，讓使用框架的人可以從它重跑。所以就算在不跑診斷的監控模式，也得留著一個什麼都不做的抽樣 node。

**已拍板：放進重構範圍。**

**作者建議的順序。** 這條會改變「哪些結果算過期」，屬於行為變更。照 flow 規則 3（行為變更先做，純搬移放最後），它排在純搬移之前；又因為要用到原則 3 的宣告，排在原則 3 之後。這條會改動 [ADR-0020](0020-evaluation-bug-round-intended-behaviours.md)「二、bug 2 的修法」定下的指紋機制，是幾條原則裡改動最大的；現況的代價是白白重跑，結果不會錯。

---

### 原則 6　每個模組的職責符合框架的角色

**框架給每一種模組的角色。**

| 模組 | 職責 | 不該做的事 | 出處 |
|---|---|---|---|
| `pipeline.py` | 組裝：這次跑哪些 node、接哪些輸入輸出。原則 3 的需求清單也在這一層 | 計算、讀資料 | node 規則 8；需求清單放這一層對應 ADR-0013 |
| `nodes.py` 的 node | 產出一個或一組落地結果；body 寫出這一步做了哪些決定（`# Decision —`），機制交給 helper | 自己藏機制，例如幾百行的實作細節 | node 規則 2、4（決策寫在 node）、8（機制放哪） |
| pipeline 根層的契約模組 | 本 pipeline 與 CLI 共用的契約，例如 dataset 的 `month_plans.py`、`run_contract.py`，evaluation 的 `compare_tables.py` | 放只有 node 在用的機制 | node 規則 8 |
| `pipelines/<name>/steps/` | 只有這條 pipeline 用的機制 | 被本 pipeline 以外的任何 src 模組 import | node 規則 8、架構稽核 S3 |
| 根層套件（`evaluation/`、`diagnosis/` 等） | 有別的 pipeline 或 `core/` 在用的機制 | 放只有一條 pipeline 在用的東西 | **本份新增**（見下） |

前四列是 repo 既有的規則。第五列是本份新增的：[ADR-0019](0019-evaluation-modules-split-by-role.md) 寫過「node 規則 8 對 `src/recsys_tfb/evaluation/` 沒有明文」，方向上則對齊 S8 與 ADR-0030 決定 14 的 import 方向。

照這張表擺好之後，檔案放在哪裡就說明了它屬於哪一層、給誰用。讀程式的人從 `pipeline.py` 開始，就能一路找到每一個機制。

**現在：dataset 照著做了，evaluation 沒有。**

| | dataset | evaluation |
|---|---|---|
| pipeline 自己目錄內的程式 | 4,095 行 | 3,912 行 |
| 放在 pipeline 外面的相關程式 | 幾乎沒有 | `evaluation/` 6,896 行、`diagnosis/metric/` 5,861 行、`report/` 892 行，合計 13,649 行 |
| `nodes.py` 從哪裡 import | 自己 pipeline 內的模組 8 處（`steps/` 7 處、根層 `month_plans.py` 1 處） | 根層的 `evaluation/` 8 處，多於自己的 `steps/` 6 處 |
| `nodes.py` 裡的私有 helper | 0 個 | 8 個 |
| node 裡標出的 `# Decision` | 68 處 | 30 處 |
| 最長的 node | 222 行 | 399 行（`make_prepare_eval_data_node`） |

要讀懂 evaluation，得在四個套件之間跳來跳去，而且看不出哪些是共用的、哪些只有 evaluation 在用。

**為什麼會這樣。** ADR-0019 照同一套規則搬過一輪，但把 `report_builder.py` 留在根層，理由是有兩支 script 在 import 它：`render_diagnosis.py` 與 `migrate_evaluation_results_keys.py`。根層模組不准 import `steps/`（S3），所以 `report_builder.py` 用到的 `report.py`、`compare.py`、`diagnostics_spark.py`、`distributions.py`、`baselines.py`、`prediction_quality.py`、`segment_keys.py` 也全被拉住，跟著留在根層。

現在 `render_diagnosis.py` 已經刪了，只剩 `scripts/migrations/migrate_evaluation_results_keys.py` 一支，而且它是一次性的遷移工具，處理方式見下面的已拍板。

**哪些真的要留在 pipeline 外面。** 拿掉那支 script 這個理由之後，還有下面這些在 pipeline 外面有讀者：

| 模組 | pipeline 外的讀者 | 該放哪裡 |
|---|---|---|
| `evaluation/metrics.py`、`metrics_spark.py`、`metric_registry.py` | training pipeline | 根層 `evaluation/` |
| `evaluation/segment_keys.py` | `metrics_spark.py` | 根層 `evaluation/` |
| `evaluation/baselines.py` | CLI（`__main__.py`） | evaluation pipeline 根層，當契約模組：S3 不准 CLI import `steps/` |
| `diagnosis/metric/contract.py` | `core/consistency.py` | 根層 `diagnosis/`：S8 不准 `core/` import pipelines |

另外，比較報表 `evaluation/comparison/report.py` 從 `report_builder.py` import 了 7 個名字。`report_builder.py` 搬進 pipeline 之後，它如果留在根層會違反 S8，所以**比較報表必須跟著一起搬**。

**改成（示意，盤點後再定）。**

```
pipelines/evaluation/
  pipeline.py        組裝與需求清單
  nodes.py           每個計算單元一個 node，寫決策
  baselines.py       CLI 也讀的契約模組
  compare_tables.py  既有的契約模組
  steps/             只有 evaluation 用的計算機制，含各診斷的計算
  report/            主報表、比較報表與診斷頁的各段，一段一個檔，只組裝
evaluation/          metrics.py、metrics_spark.py、metric_registry.py、segment_keys.py
diagnosis/metric/    contract.py
```

各診斷現在是計算與畫圖放在同一個子套件，`_compute.py` 與 `_render.py` 並排；照原則 1，計算進 `steps/`，畫圖進 `report/`。根層的 `report/`（頁面外殼）不在本條範圍，盤點時確認它的讀者。

**誰擋得住。**

- S3 已經擋住「pipeline 以外的模組 import `steps/`」。
- 本份提一條新的架構測試：根層套件的模組，至少要有一個別的 pipeline 或 `core/` 的讀者。直接 import 或經由其他根層模組間接使用都算，例如 `segment_keys.py` 經由 `metrics_spark.py`、`metrics.py` 經由 `metric_registry.py`。只有 CLI 讀的，放 pipeline 根層。
- `# Decision` 寫得夠不夠、node 裡有沒有藏機制，只能人看。

**已拍板：遷移 script 保留，但不再 import `report_builder.py`。** 擁有者決定：script 已經不需要，但不刪，保留備查。作者建議的做法：

- 位置不動，仍在 `scripts/migrations/`；這個資料夾本來就是放一次性遷移工具的地方，旁邊還有 `migrate_logs_layout.py`。
- 它從 `report_builder.py` import 的只有兩個常數 `RENAMED_OVERVIEW_KEYS`、`OVERVIEW_CELL_GROUPS`，內容是一份已經凍結的舊鍵名對照表，直接複製進 script，並在開頭註明「已完成的遷移，保留備查」。
- 這會推翻 `tests/scripts/test_migrate_evaluation_results_keys.py::test_the_migrator_and_the_reader_share_one_rename_map`。那支測試要求兩邊是同一個物件，理由是「偵測端與遷移端必須用同一份清單」，script 開頭與 `report_builder.py` 的 `MIGRATION_SCRIPT` 註解也寫了這條理由。改成：測試比對兩份內容相等（測試可以同時 import 兩邊，不受 S3 限制），兩處註解跟著改寫。

**已拍板：舊格式的檢查保留。** `report_builder.py` 讀到改名前的舊鍵（`n_products`、`n_customers`、`avg_positives_per_customer`）時，會拒絕畫報表並提示去跑那支 script。這段檢查留著：沒有它，報表找不到新鍵名，那幾格會默默留白，不會報錯。搬進 pipeline 時跟著一起搬。

**作者建議：報表依段落拆檔。** 擁有者授權作者依易讀性與可維護性判斷，作者判斷要拆。理由：要看某一段報表時，直接開那一段的檔，不必在 2139 行的 `report_builder.py` 裡找；刪一個功能時，只會動到用到它的那幾段。拆法：

- 照報表的大段落拆，一段一個檔，檔名就是段名。段落以 `assemble_report` 列出的順序為準：概覽、核心概念、基本統計、衡量指標、預測品質、per-item 細部拆解、baseline、排序診斷連結、完整性檢查、詞彙表。
- 兩段以上都用到的小工具，例如數字格式與組表格，放進同一層的一個共用檔。比較報表借用的 7 個名字也放這裡。
- 只有一段在用的東西，留在那一段的檔裡，不預先抽出去。

---

## 四、動手順序

1. **先決定要留什麼。** 刪掉的東西就不用再整理。開工時逐項拷問定案，目前已知的候選：
   - **item_ability 的 centered AUC**：回答「模型推這個 item 時，是挑對了人，還是只是挑了活躍的客戶」。這是跨客戶的問題，只有在拿分數替某個 item 挑客戶時才用得到；如果只是替每位客戶把 item 排順序，mAP 已經回答了該回答的問題。留下的話，要一起決定原則 2 補充三怎麼套用到它。
   - **config_shift 的配置偏移、suppression 的壓制矩陣**：都不好懂，要先確認報表讀者看完會做什麼決定。
2. **盤點。** 照 ADR-0030 的做法，把 evaluation 的盤點筆記寫進 `docs/notes/`：每個計算花多少時間、產出什麼、誰在讀、讀哪些設定。原則 2 的切法、原則 3 的宣告、原則 5 的指紋都要靠這份清單。
3. **本份補上決定清單，零程式碼先進 main**（flow 規則 1）。
4. **排實作順序。** 預設照 flow 規則 3：行為變更先做，純搬移放最後。但原則之間有依賴，盤點後在決定清單裡定實際順序：
   - 原則 3、5 的宣告寫在計算單元旁邊，所以原則 2 的切分最好先做，不然切完要重寫宣告。
   - 原則 1 會新增落地結果（從報表搬出來的計算），不算純搬移。
   - 原則 4（含原則 2 補充三）、原則 5 會改變結果或改變「哪些結果算過期」，是行為變更。
   - 原則 6 的搬家與拆檔是純搬移，放最後。

## 五、本份還沒回答的事

- 哪些診斷要留：見第四節第 1 步。
- 原則 2 的實際切法：要等盤點量出成本。
- 原則 3 補充二：空殼能不能拿掉。
- 原則 6 的目標樣子細節，以及根層 `report/` 的讀者。
- 比較報表：它已經被原則 6 綁住、要跟 `report_builder.py` 一起搬，但它自己的內容還沒看過。
- 每條原則實際要改幾個檔、花多久：要等盤點。

---

## 附錄一　一條不限 evaluation 的教訓

**程式數得出來的數量，文字裡不要寫成數字。** #520 改了大約 15 句這類句子，例如「四個消費者」「七個輸入裡五份落地」。

最典型的例子是 node 數：

- 同一個數字寫在兩處：`tests/test_core/test_architecture_constraints.py::test_static_coverage_floor` 的斷言，和 `docs/agents/architecture-constraints.md` 的一句「62 個 node 裡 59 個」。
- #517、#519、#520 各刪了一個 node。測試的斷言每次都有人跟著改，因為不改測試就會紅。
- 文件那一句從 2026-09-30 起就停在 62，#517、#519 都沒改，直到 #520 rebase 時才一次改成 60。

這類數字要嘛不寫，改寫成「去哪裡查」；要嘛只寫在有測試會核對的地方。本份第三節也寫了不少數字，所以標明是 `11ad407e` 的快照，並附上重現指令。

去處：這條跟 evaluation 無關，核准時再決定放進哪一份給 agent 讀的文件，例如併進 node 規則 13（docstring 講為什麼）。

## 附錄二　數字的重現指令

在 repo 根目錄、main @ `11ad407e` 執行：

```bash
# 程式行數
wc -l src/recsys_tfb/evaluation/report_builder.py src/recsys_tfb/pipelines/evaluation/nodes.py
for d in pipelines/dataset pipelines/evaluation evaluation diagnosis/metric report; do
  find src/recsys_tfb/$d -name '*.py' -exec cat {} + | wc -l; done

# # Decision 標記
grep -c "# Decision" src/recsys_tfb/pipelines/dataset/nodes.py src/recsys_tfb/pipelines/evaluation/nodes.py

# query_key 呼叫次數（排除定義與 docstring 裡的提及）
grep -rn "query_key(" src/recsys_tfb/diagnosis --include='*.py' | grep -v "def query_key" | grep -v '``query_key('

# PR #520 的檔案分布
git diff --numstat 11ad407e^1 11ad407e

# nodes.py 裡的私有 helper（頂層、名字以底線開頭的函式）
grep -cE "^def _" src/recsys_tfb/pipelines/dataset/nodes.py src/recsys_tfb/pipelines/evaluation/nodes.py

# nodes.py 的 import 來源
for f in dataset evaluation; do echo "== $f"
  grep -E "^from recsys_tfb\." src/recsys_tfb/pipelines/$f/nodes.py | awk '{print $2}' \
    | awk -F. '{print ($2=="pipelines") ? $2"."$3"."$4 : $2}' | sort | uniq -c; done
```

node 長度是用 Python 的 `ast` 模組解析 `nodes.py` 與 `pipeline.py` 量的：node 函式取 `pipeline.py` 裡 `Node(` 的第一個參數，長度取函式的起訖行。
