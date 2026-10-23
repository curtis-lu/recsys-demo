---
status: accepted
date: 2026-10-16
---

# evaluation pipeline 第二輪整理的方向：計算與呈現分開、依效率切計算、關掉就不算、不重複算

> **狀態。** 第一～五節是 2026-10-16 的提案；2026-10-21 與擁有者逐題拷問之後補上第六節〈決定清單〉，連同前五節一起核准。前五節的標註分三種，保留當歷史：
>
> - **已拍板**：擁有者在討論時已經表態的。
> - **作者建議**：本份作者的提案，核准本份時一併生效。
> - **待決定**：留到重構開工時再逐項拷問（grill）。
>
> 「待決定」的項目都在第六節有了答案。前五節與第六節有出入時，以第六節為準。
>
> **範圍。** 第一～五節只定方向，證據只來自一次具體的改動：#515／PR #520，拿掉信賴區間。第六節的決定是在兩份讀程式的盤點（指標從設定到報表的路徑、兩份報表怎麼組出來）與一次對著程式挑錯的分層審查之後定的；這三份都只讀了程式，**沒有量時間**，量時間排進各張票（決定 13）。跟 [ADR-0029](0029-dataset-second-pass-scoped-reads-symmetric-splits.md)、[ADR-0030](0030-training-second-pass-honest-adapter-shared-scoring.md) 不同，本份沒有另寫一份盤點筆記。
>
> **相關判準。** 跨 pipeline 的形狀判準在 [`pipeline-node-design.md`](../agents/pipeline-node-design.md)，本份稱「node 規則 N」；流程判準在 [`pipeline-refactor-process.md`](../agents/pipeline-refactor-process.md)，本份稱「flow 規則 N」。
>
> **數字是快照。** 第一～五節的行數與計數是 main @ `11ad407e`（2026-10-16）量的，重現指令在附錄二；第六節是 main @ `fe3c30dd`（2026-10-21）。程式一改就會變。第一～五節的檔案位置只寫檔名與函式名，不寫行號；第六節為了讓切票的人直接找得到，附了行號。
>
> **用詞。**
>
> - 「頭號指標」：主報表最上面那一個 macro per-item mAP。
> - 「計算單元」：一個或幾個 node，一起產出一組結果。
> - 「設定指紋」：見原則 5。
> - 「side」：一份被評估的分數，例如這次的模型、熱門度基準線、比較對象（第六節起使用）。
> - 「列的事實」：跟分數無關、描述一批列的數字，例如規模、組數、全正組數（決定 4）。
> - 「母體帳」：一份指標結果算在哪些列上的紀錄（決定 4）。

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

**2026-10-21 更新。** 診斷全部拿掉（決定 2），補充三講的共用準備步驟跟著刪，這一條只剩背景用途。切法見決定 4（四層）與決定 7（指標分三種算法）；量時間見決定 13。

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

**補充二：報表只讀需求清單裡的結果，空殼可能就不用寫了。** 現在關掉的診斷會寫一個 `{"enabled": false}` 的空殼 JSON，理由有兩個：讓報表的位置參數有東西可收，以及避免報表讀到上一次留下的舊結果。報表改成只讀需求清單裡的結果之後，這兩個理由看起來都不成立。這點列為待決定，盤點時確認。（2026-10-21：作者建議預設拿掉，開工時定，見決定 15。）

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

**2026-10-21 更新。** 診斷全部拿掉（決定 2），item_ability、suppression、config_shift 的算法跟著消失。macro per-item mAP 只剩主報表的 Spark 那一份，加上下面例外一的 HPO numpy 版本；上面兩種處理與 config_shift 的防線只剩背景用途。

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

**2026-10-21 更新。** suppression 與「附帶的問題」裡的 `draw_diagnosis_sample_node` 都隨決定 2 拿掉，上面的例子只剩說明用途；規則本身不變。

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

**2026-10-21 更新。** 診斷全部拿掉（決定 2），上面這段講的各診斷子套件不必再搬，`diagnosis/metric/contract.py` 那一列也跟著消失。根層 `report/` 今天的讀者是 evaluation 與 `diagnosis/metric/*`；診斷拿掉後只剩 evaluation，照上表第五列搬進 pipeline，只為診斷頁存在的部分（多頁外殼）跟著拿掉。

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
- **2026-10-21 更新**：排序診斷連結、詞彙表兩段拿掉（決定 3）；各段彼此不互相提到，比較報表也照同一套做，跟主報表共用同一套指標表的程式（決定 9、11）。
- **更正（#533 實作時發現，2026-10-23）**：
  - 比較報表借用的 7 個名字裡有一個是整段（詞彙表 `build_glossary_section`）。它照「一段一個檔」放在自己的檔，比較報表讀那個檔；其餘 6 個放共用檔。詞彙表在決定 3 拿掉之後，這一條跟著消失。
  - 檔名用決定 16 第 4 項定案的新段名（例如衡量指標段的檔叫 `ranking_metrics.py`），不用拆檔當下的段名（擁有者 2026-10-23 決定），#537 改段名時就不必再改檔名。在那之前，檔名跟畫面上的段名對不上；每個檔開頭寫了兩個名字。
  - 本份引用的報表函式搬家後的位置（都在 `evaluation/report_sections/`）：`_headline_key` → `overview.py`；`_headline_column` → `shared.headline_column`；`_macro_item_coverage` → `shared.macro_item_coverage`；`build_baseline_section` → `popularity_baseline.build`；`build_completeness_section` → `completeness.build`；其餘 `build_*_section` 同樣是各段檔的 `build`。行號仍是 `fe3c30dd` 的快照。
- **更正（#533 實作時發現，2026-10-23）：決定 6、7 的指標目錄。**
  - 目錄 `evaluation/metric_catalog.py` 先只收前兩種算法的指標。第三種（把分數當機率，預測品質段的分箱 `pr_auc`／`roc_auc`）由 `evaluation/prediction_quality.py` 用自己的函式算，沒有可讓公式指名的共用數字；等那種算法備齊共用數字時再進目錄。
  - 決定 6 要 query group 的 `recall@K` 與 per-item 的那一個「報表顯示名要分得出來」。#533 要求兩份 HTML 一字不差，所以目錄裡兩者的顯示名都照今天的報表寫 `recall`；分開它們留給改名那一步（per-item 的 `hit_rate@K` 改叫 `recall@K` 時），那一步的驗收本來就有這一條。#534 已分開，見決定 7 的更正；同一則更正也寫了第三種算法的共用數字是什麼。
  - 決定 11 讓各段自己宣告「要比哪些」，所以熱門度基準段與比較報表各留一份「要比的目錄條目」清單；衡量指標段則對目錄裡每個指標都畫表（per-item 那塊既有的兩張表照舊，其餘每個 per-K 的 item 指標各一張）。照決定 11，新指標要出現在比較表裡，仍要改那兩份清單；這跟決定 1「加指標兩份報表的程式不改」衝突，留給驗收那張票定（例如改成在目錄上加「要比」的標記，決定 16 第 1 項的模型選擇指標標記是同一種做法）。

---

## 四、動手順序

> **2026-10-21 更新。** 第 1 步的答案是決定 2、3。第 2 步只做了讀程式的盤點，沒有量時間（見文首〈範圍〉）。第 3 步就是第六節。第 4 步的 PR 順序，擁有者決定另用 `/to-spec`、`/to-tickets` 設計，本份只給約束（決定 13）。

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

2026-10-16 列的這幾件，2026-10-21 都有了去處：

| 當時沒回答的事 | 去處 |
|---|---|
| 哪些診斷要留 | 決定 2：全部拿掉 |
| 原則 2 的實際切法 | 決定 4、7；量時間見決定 13 |
| 原則 3 補充二：空殼能不能拿掉 | 決定 15：作者建議預設拿掉，開工時定 |
| 原則 6 的目標樣子細節，以及根層 `report/` 的讀者 | 原則 6 的 2026-10-21 更新；目錄細節在搬移那張票定 |
| 比較報表的內容 | 決定 5、11 |
| 每條原則實際要改幾個檔、花多久 | 沒有回答：等 `/to-spec`、`/to-tickets` 切票時估 |

---

## 六、決定清單（2026-10-21）

本節是 2026-10-21 與擁有者逐題拷問的結果。行號與計數是 main @ `fe3c30dd` 的快照，程式一改就會變。標「作者建議」的是寫本節時補的、擁有者沒有逐條看過的項目，切票時可以推翻。

### 決定總表

| # | 決定 | 會改變的行為 |
|---|---|---|
| 1 | 驗收標準：改報表只動一段的檔、兩份報表不跑 Spark 計算就能重畫、加指標只在目錄加一筆 | 無（這是驗收的尺） |
| 2 | 拿掉全部診斷頁 | 診斷頁、共用準備步驟、報表的「排序診斷連結」段消失 |
| 3 | 主報表留下哪些段 | 詞彙表段消失 |
| 4 | 四層：資料、指標、比較、報表 | 新增「列的事實」產物；每份指標結果附母體帳 |
| 5 | 比較自己一層，差值只在這裡算 | 比較結果落地；`--compare-only` 之後可以只重畫 |
| 6 | 指標目錄：一個指標一筆 | 無 |
| 7 | 指標分三種算法，每種先備齊共用數字 | per-item 的 `hit_rate@K` 改名 `recall@K`；per-item 那一種改看該 item 的全部列 |
| 8 | baseline 走跟模型同一套指標計算 | baseline 段第一次有 Macro 列 |
| 9 | 報表每段獨立 | 概覽的導覽表、詞彙表段消失 |
| 10 | 報表裡還在做的判斷，搬到計算層 | 無（數字不變，改在哪裡算） |
| 11 | 比較報表只顯示「要比的指標」 | 比較報表的整體表不再印出每一個鍵 |
| 12 | 一起處理與關掉的票 | 無 |
| 13 | 順序的約束 | 無 |
| 14 | 附錄一的教訓併進 node 規則 13 | 無 |
| 15 | 開工時再定的技術細節與預設做法 | 見該決定 |
| 16 | 報表的長相（#531 demo 定案） | 見該決定；改寫決定 3 的段名，以及決定 1、4、7、9、11 的部分內容 |

### 決定 1　驗收標準

這輪要證明兩件事：兩份報表能快速調整；加指標不必大改程式。重構做完，逐列實際做一次，數改了哪些檔。

| 想證明的事 | 怎麼驗 | 及格線 |
|---|---|---|
| 改一段報表 | 在「衡量指標」段的表加一欄 | 只改那一段的檔，加測試 |
| 調段落順序 | 把兩段對調 | 段落清單改一行 |
| 拿掉一段 | 拿掉「預測品質」段 | 刪那段的檔、段落清單刪一行，連同它的開關；它的計算由需求清單自動不跑 |
| 重畫不重算 | 主報表、比較報表都用 `--only-node` 重畫 | 不重算任何指標 |
| 加指標 | 加上下面這批指標，當最後一張票 | 每個指標只在指標目錄加一筆，加測試；兩份報表的程式不改 |

**驗收第一列改寫（2026-10-22，決定 16）。** 「衡量指標」段改叫「排序指標」，而且表的欄是 K；能顯示的 K 由 `evaluation.k_values` 決定，多一個 K 要改設定、重跑 Spark，測不到「只改一段」。所以改成「資料統計段的表加一欄 entity 數」：各期、各 item、各分群的 entity 數今天已經落地在 `metrics.json` 的 `dataset_overview`，決定 4 之後搬進列的事實，加這一欄是純呈現的改動。及格線不變：只改那一段的檔，加測試。

驗收用的指標是擁有者列的這批：precision@K、F1@K、AUC-ROC、AUC-PR，各要整體、per-item、macro；加上 query group 層級的 hit_rate@K（只有整體，沒有 per-item）。今天已經有的（整體的 precision@K、整體的分箱 AUC）照舊。定義的邊界情況（例如某個 item 從沒排進前 K 名時 precision 怎麼算、macro 怎麼處理、per-item 那一種算進哪些 query group）在那張票開工時定。

**拿掉一段要連開關一起拿（作者補充）。** 一段有開關時（例如 `report.sections.prediction_quality`），A34 要求開關清單跟報表讀的段一一對應，那份清單放在 `core/`（`EVALUATION_REPORT_SECTIONS`），因為 `core/` 不能 import 報表層；各份設定檔也宣告了它。所以拿掉一段時，那個開關要從 `core/` 的清單與設定檔一起拿掉，A34 會指出漏了哪裡。這兩處算進及格線。

**重畫不啟動 Spark。** 今天就算只重畫，CLI 也會先初始化 Spark（`docs/pipelines/evaluation.md` 4.6 節）。擁有者偏向重畫時連 Spark 都不啟動。只改 `__main__.py` 做得到就做；要動 `core/` 的執行器或 catalog 才做得到，就算太複雜，改開 issue 記著。

### 決定 2　拿掉全部診斷頁

拿掉 `diagnosis.metric.contract.DIAGNOSES` 列的全部診斷：`config_shift`、`item_ability`、`model_capacity`、`suppression`。只為它們存在的東西一起拿掉：共用準備步驟 `make_draw_diagnosis_sample_node`、`render_diagnosis_pages` 與監控模式的 `no_diagnosis_pages`、報表的「排序診斷連結」段（`build_diagnosis_links_section`）與它的開關 `report.sections.diagnosis_links`（A34 要求開關清單跟 `EVALUATION_REPORT_SECTIONS` 完全相等，兩邊一起拿）、`evaluation.diagnosis.*` 設定鍵，以及一致性檢查裡只檢查這些鍵的部分。A15 同時檢查 `evaluation.metric` 與 `evaluation.diagnosis`（`consistency.py:2302`），只拿掉後者那一半；A15 另外擋 `evaluation.segment_columns` 用到保留名 `stratum`／`inclusion_weight`（`consistency.py:2371-2389`），理由是診斷抽樣會自己加這兩欄，這一塊跟著拿掉。A19 整條只檢查 `evaluation.diagnosis.suppression.top_examples`，整條退休，代號不重編（照 A16／A17／A18 的慣例）。

- **為什麼。** 留下的每一頁都要搬家、拆開計算與畫圖、補宣告；擁有者判斷沒有讀者會看完這幾頁就做決定。這是第四節第 1 步「先決定要留什麼」的答案。
- **連帶消失的問題。** 原則 2 補充三（共用準備步驟只留有正例的 query）、原則 4 的 config_shift 防線，以及 item_ability 的 AUC 沒算進無正例的 query group（它的母體是只含有正例 query 的診斷抽樣）。
- **training 不動。** 四項裡只有 `model_capacity` 讀 training 的產物 `gain_ledger`。training 的 `log_experiment` 也把它當輸入，跟其他診斷檔一起上傳到 MLflow（`pipelines/training/pipeline.py:249`），所以拿掉之後它仍有去處。範圍規則：拿掉之後，若 training 有產物變成沒有任何去處，先問擁有者，不自己刪。
- **舊設定鍵。** 公司的設定檔可能還寫著 `evaluation.diagnosis.*`。照 repo 擋舊設定鍵的慣例在 `core/consistency.py` 擋下，跟既有的 A61（擋 `evaluation.diagnosis.ci`）合成一條，並列進 #524 的退場清單。
- **報表外殼的跳脫。** `evaluation/report.py:109` 的註解說段落說明不跳脫，因為 `build_diagnosis_links_section` 在說明裡放連結（ADR-0019 決定 5）。連結段拿掉後這個理由消失；跳脫的修法併進 #366。
- **詞彙表。** `CONTEXT.md` 的 `roc_auc` 條拿 `raw_within_item_auc`／`query_centered_auc` 當對照，這兩個隨 item_ability 消失；那一句在這張票裡刪，不提前刪（程式還在）。

### 決定 3　主報表留下的段

留：概覽、核心概念、基本統計、衡量指標、預測品質、per-item 細部拆解、baseline、完整性檢查。拿掉：排序診斷連結（決定 2）、詞彙表（決定 9）。

段名後來在決定 16 第 4 項改了（例如「衡量指標」改叫「排序指標」）；本條以下仍用舊名。

- **per-item 細部拆解留著。** 主報表只有它畫出 item 落在第幾名；它的計算已經落地成 `report_aggregates.json`，報表只畫圖。
- **開關改名（作者建議）。** 這一段的開關叫 `report.sections.diagnostics`，診斷拿掉後這個名字會誤導。改名的代價很小：拿掉 `diagnosis_links` 之後，有宣告 `sections` 的設定檔這輪本來就要改，因為 A34 要求 `sections` 的鍵完全等於 `EVALUATION_REPORT_SECTIONS`（`core/consistency.py` 的 A34 說明）。同一條 A34 也會擋下還寫著舊名的設定檔，不必另寫擋舊鍵的檢查。新名切票時定。後來 #532 改名成 `per_item_breakdown`。
- **calibration bins 早就拿掉了。** #363 第 5 項問的那份 calibration bins，#381 已經移除（`diagnostics_spark.py:261-264` 的 docstring）。這一項沒事可做。擋它舊設定鍵的 A43 照舊留著，退場跟著 #524。

### 決定 4　四層：資料、指標、比較、報表

```
資料層（Spark，落地）
  A 的列：enriched_eval_predictions（這次的模型）
  熱門度：同一批列換成熱門度分數（baseline）
  B 的列 → 跟 A 對齊 → A′、B′，以及對齊的涵蓋率
  列的事實（新）：規模、組數、全正組數、分群欄這類跟分數無關的數字
        │
指標層（Spark，一個 side 一份，落地，附母體帳）
  A：全部指標、預測品質、per-item 細部拆解的分布
  baseline、A′、B′：只算「要比的指標」所屬的那幾種算法
        │
比較層（不跑 Spark，落地）
  兩份指標結果照名字與粒度 join，算差值
  (A, baseline) → 主報表的 baseline 段
  (A′, B′)     → 比較報表
        │
報表層（不跑 Spark）
  主報表、比較報表：只讀落地結果、只組裝
```

- **列的事實自己一份。** 今天資料概況與組數帳藏在 `metrics.json`，熱門度視窗的事實藏在 `baseline_metrics.json`，比較的涵蓋率只在記憶體。A 與 baseline 是同一批列，共用一份；A′、B′ 各一份。
- **每份指標結果附母體帳。** 只照名字 join，有四種情況會默默算錯：兩邊的 `all` 解析成不同的 K、進 macro 的 item 不同、算進指標的組不同、B 沒有分群欄（今天 B 用 `segment_columns=[]` 算，`nodes.py:1941`）。所以每份結果要記下：`all` 解析成幾、哪些 item 進了 macro、算進指標的組數。比較層靠它判斷能不能比、要怎麼標註。
- **需求清單分 side。** A 要全部；baseline、A′、B′ 只要「要比的指標」所屬的那幾種算法。預測品質段的分箱表與門檻掃描、per-item 細部拆解的分布只有 A 有。把分數當機率的那一種算法（AUC；今天由 `compute_prediction_quality` 算，開關 `report.sections.prediction_quality` 預設關）只有被某段的「要比的指標」清單列到時，baseline、A′、B′ 才算。同一種算法的指標一起算（原則 2 第 1 條），所以清單只要動到已經算過的算法，就只要重畫。
- **清單要的結果沒算到時要報錯。** 「要比的指標」清單寫在各段的檔裡（決定 11），它決定 baseline、A′、B′ 要算哪幾種算法，所以改它可能要重算，不只是重畫；而清單在程式裡，設定指紋看不到它。所以報表或比較層要讀的指標在落地結果裡找不到時，報錯並指出從哪個 node 重跑，不默默留白。
- **改寫（2026-10-22，決定 16 第 3 項）。** 主報表概覽固定列 pooled AP 與 macro per-item AP，比較報表的預設清單也有，所以 A、A′、B′ 每次都要算這兩個。它們跟 AUC 一樣把每一列當一次二元預測，但在原始分數上算，不是上一條講的分箱版，不受「清單列到才算」限制。
- **更正（#535 實作時發現，2026-10-23）。**
  - **熱門度視窗的事實留在 `baseline_metrics.json`，不進列的事實。** 本條第一點把 `purchase_counts`、`monthly_counts`、`window_months_covered`、`popularity_rate` 算成列的事實。它們是 baseline 分數本身的來源（每個 item 在回看窗裡的正例數或正例率），不是這批列跟分數無關的事實；算進列的事實，等於 baseline 產出自己的列的事實，跟決定 8 衝突。窗的涵蓋月數改由計算層算，是決定 10 的事（#536）。
  - **同分列數跟分數有關，搬到指標結果，不放在列的事實。** `n_tied_rows`／`tied_row_share`（宣告了 optional role 才寫）數的是「組內跟別列同分的列」，數的是這組分數的同分；今天它在資料概況裡（`dataset_overview.totals`），決定 8 不讓報表讀 baseline 的資料概況，理由也正是熱門度分數的同分數字不同。所以它跟著分數走：每份指標結果各記一份（item 與大類粒度，baseline 的是熱門度分數的），主報表的完整性檢查段讀模型那份，畫面不變；列的事實只剩跟分數無關的數字（擁有者 2026-10-23 選定，審查時抓到）。
  - **列的事實是一個 node（`compute_row_facts`），排在兩個指標 node 前面，兩個指標 node 從它讀組數、item 數與最寬組的列數，不再自己數。** 所以它是第一個寫帶指紋產物的 node，`COMPUTED_KEYS` 裡原本指向 `compute_metrics` 的列都改成指向它；設定改了之後，錯誤訊息建議 `--from-node compute_row_facts`（擁有者 2026-10-23 在「放指標前面、組數只算一次」與「跟指標並排、各自再數一次」之間選了前者）。
  - **母體帳的樣子。** 每份指標結果（模型、baseline、比較報表的兩邊；大類粒度各一份）多一個 `population`：`all_k`（`all` 解析成幾，以前只有宣告 `event` 才寫，而且寫在頂層）、`macro_items`（進 `macro_avg.by_item` 的 item）、`n_scored_queries_with_positive`、`n_scored_queries_without_positive`（算進指標的組，今天沒有正例的組一律不算，所以是 0）。列的事實（組數、資料概況、分群欄）從 `metrics.json` 拿掉，報表改讀 `row_facts.json`。
  - **baseline 的母體帳一定跟模型的相同，查不出兩邊不同。** 它的組數與 `all_k` 是從模型那份列的事實推出來的；(A, baseline) 這一對靠構造保證是同一批列（`build_baseline_frame` 是一對一的 left join，分數表的 (time, item) 不重複），不靠查帳。母體帳拿來判斷「能不能比」的是 A′、B′：兩邊各數各的列的事實。帳上也還沒有決定 4 第四種情況（B 沒有分群欄）的欄位，也只記組數、不記是哪些組；比較層落地時（#536）照它需要的補。
  - **比較報表的 A′、B′ 這張先在記憶體裡算列的事實**（`generate_comparison_report` 兩邊各算一次），落地是 #536 比較層的事。

### 決定 5　比較自己一層，差值只在這裡算

- **擁有者的看法。** 比較報表是一份獨立的報表；比較要對兩份指標做 join，是獨立的一層。主報表的 baseline 段也是同一件事（這次的模型對熱門度基準線），所以兩種比較共用這一層。
- **今天差值散在好幾處。** `build_baseline_section` 呼叫 `compare.build_comparison_result`；`per_item_metric_compare_table` 自己算 Macro 列的差值；`generate_comparison_report` 在 node 裡算。收成比較層一份。
- **比較結果落地。** 這樣比較報表也能不跑 Spark 重畫（決定 1）。今天 `generate_comparison_report` 每次都在 node 裡跑兩次 `compute_all_metrics`（`nodes.py:1934-1944`），結果不落地。
- **落地的結果要帶「跟誰比」**：`evaluation.compare_sources.<key>` 底下每個決定 B′ 內容的鍵，也就是 `kind`、model_version 或表名、`source`、`columns`、`prod_mapping`、`unmapped_policy`（`compare_sources.py:58-179`）；`label` 只影響顯示。不然先跟 X 比、再跟 Y 比（或改了 `prod_mapping`）之後只重畫，會讀到舊的數字、印上新的標籤；今天的指紋刻意不含 `evaluation.compare`（`config_fingerprint.py:46-48`），擋不下來。
- **考慮過、沒選：不另立一層。** 分層審查建議，差值在「同時拿得到兩邊數字」的那個單元裡順手算，少一層。沒選，因為它反對分層的主要理由是「照名字 join 會缺資訊」，而這已由母體帳補上（決定 4）；母體帳兩種做法都需要，補上之後分層不比較貴，差值也只有一個地方。

### 決定 6　指標目錄：一個指標一筆

- **每筆寫什麼。** 名字、屬於哪一種算法（決定 7）、公式（由那一種算法備好的共用數字算出）、粒度（整體、per-item、macro、分群、大類）、母體（算在哪些列上）、報表顯示名、一句話定義。
- **一筆＝一個名字在一種算法裡。** 同一個名字可以在兩種算法各有一筆、定義不同。例如 query group 那一種的 `recall@K`（每組的正例裡排進前 K 名的比例，再對組平均），與 item 那一種的 `recall@K`（該 item 的正例裡排進前 K 名的比例）；`precision@K`、`F1@K` 也是。落地結果裡兩者在不同的區塊（`overall` 對 `per_item`），報表顯示名要分得出來。
- **目錄只放資料，不做計算。** 計算、比較層、主報表、比較報表都照名字讀它，取代今天寫死在各處的名單（`metrics_spark.py:729`；`report_builder.py:967`、`1027`、`1603`；`comparison/report.py:246`）。因為它不計算，報表讀它不違反原則 1 預告的「報表模組不准 import 計算模組」。
- **報表上的指標定義從目錄來**（決定 9）。
- **跟 training 的 `metric_registry.py` 這輪分開**，不動 training（決定 2 的範圍規則）。兩邊同一個數字卻不同名字的地方，在目錄裡註明，例如 training 的 `macro_per_item_map` 與 evaluation 的 macro `map_attr@all`。

### 決定 7　指標分三種算法，每種先備齊共用數字

所有指標都先在 query group 內把 item 排序，再算指標；不做「替某個 item 挑 entity 名單」這種算法。

| 算法 | 母體 | 指標 |
|---|---|---|
| 每個 query group 先算、再平均 | 有正例的 query group；全正組照 `evaluation.query_filter` | map@K、precision@K、recall@K、F1@K、hit_rate@K |
| 每個 item 一份，再對 item 平均成 macro | 該 item 的列（算進哪些 query group，驗收那張票定）；「排進前 K 名」就算推薦了一次 | recall@K（今天的 `hit_rate@K`）、precision@K、F1@K、map_attr@K |
| 把分數當機率 | 全部候選列，包含無正例的 query group（ADR-0025）；分箱近似（ADR-0024） | AUC-ROC、AUC-PR：整體＝全部列、per-item＝該 item 的全部列、macro＝對 item 平均 |

表裡今天還沒有的（F1@K、query group 層級的 hit_rate@K、per-item 的 precision@K 與 F1@K、全部 item 的 per-item AUC 與 macro AUC）都是驗收那張票才加。重構只負責讓三種算法的共用數字齊備。

- **`hit_rate@K` 名字取錯了。** 今天它是「該 item 的正例列裡，排進前 K 名的比例」（`metrics_spark.py:858-860`），也就是 per-item recall，報表也標成「recall@k (per-item)」。改名成 per-item 的 `recall@K`。另外新增 query group 層級的 `hit_rate@K`：有正例的 query group 裡，前 K 名至少中一個的比例（驗收那張票加）。改名之後，舊 `metrics.json` 的鍵名不同；報表要認出舊格式並報錯（照 `report_builder.py` 既有的舊鍵檢查做），不能讓那幾格默默留白。判斷舊格式只看 per-item 的區塊裡有沒有 `hit_rate@`，不看整份檔，因為驗收那張票會在 query group 層級加回新的 `hit_rate@K`。
- **改名推翻了程式裡寫明的理由。** `aggregate_per_item` 的 NOTE（`metrics_spark.py:869-872`）說 per-item 刻意不叫 precision／recall，因為 K 的分母是 query group 的概念。照決定 7 的定義，per-item precision 的分母是「這個 item 被推薦了幾次」，那條理由不再成立；改名時一起改寫那段 NOTE。
- **每個 item 那一種要先備齊共用數字。** 今天它只看正例列（`aggregate_per_item`，`metrics_spark.py:878`），算不出 precision 要的「排進前 K 名的次數」。重構時改成看該 item 的列並備好這些次數；之後加指標只是寫公式。算進哪些 query group 留給驗收那張票定，所以共用數字要把「有正例的 query group」與「無正例的 query group」分開記，驗收時選定義不必再改算法。
- **AUC 這輪維持分箱版**（ADR-0024）。補上全部 item 與 macro 是驗收那張票的事；精確版不在這輪。決定 16 第 3 項加的 pooled AP 與 macro per-item AP 是擁有者知情下的例外：它們在原始分數上算，是三種算法之外的第四種，用 training 已有的 Spark 計算，跟分箱 AUC 並存，不是取代。
- **更正（#534 實作時發現，2026-10-23）。**
  - **沒有正例的 query group 不記。** 上面寫「有正例與無正例的 query group 分開記」，但 `--post-training` 評的 test 表預設不留沒有正例的組（`dataset.test_zero_positive_group_ratio` 預設 0）；設了也只是抽樣，留下的列帶權重 1／r（ADR-0025）。只有監控模式有完整的。直接數列數，抽樣時會算錯。擁有者 2026-10-23 決定：每個 item 那一種只看 query group 那種算進的組，在這些組裡看該 item 的全部列，不只正例列。#541 若要把沒有正例的組算進 per-item 指標，要回來改算法，並一起決定權重（決定 16 已把「evaluation 的列帶不帶權重」留給 #541）。
  - **另外兩種算法的共用數字本來就齊，這張票不改它們的程式。** query group 那種的 `ap_sum`、`hits`、`total_rel`、`K` 已經算得出 F1@K（＝2·hits／(K＋total_rel)）與 query group 層級的 hit_rate@K（hits ≥ 1）。把分數當機率那種的共用數字，是 `prediction_quality.aggregate_score_bins` 的分箱表：Spark 端每個 item 都有，只有列數最多的 `top_n` 個收回 driver（ADR-0024 決定 6）。全部 item 的 per-item AUC 要放寬 `top_n` 還是改在 Spark 算，#541 定。數字齊了，不等於「之後加指標只是在目錄寫公式」：目錄今天只有一種公式寫法 `Ratio`（兩個共用數字相除）。#541 要加的指標裡，只有 per-item 的 precision@K（`hits`／`n_top`）寫得成 `Ratio`。F1@K（兩種算法都是）、query group 層級的 hit_rate@K、per-item 與 macro 的 AUC，都要先在目錄加一種公式寫法，並在 `metrics_spark` 寫它的算法（`metric_catalog.py` 那段註解寫明了這條路）。這跟決定 1「加指標只在目錄加一筆」衝突，留給 #541 定。
  - **落在哪。** `metrics.json` 的 `per_item_shared_numbers`，有分群欄時另有 `per_item_segment_shared_numbers`，大類 bundle 也各有一份。不塞進 `per_item` 的格子，因為 macro 與報表會把格子裡的每個鍵當成指標。per-item 的三個指標改由這些數照目錄的公式相除，值跟改名前相同。熱門度基準線這張票只改名、不落地共用數字（決定 8 由 #535 讓它走同一套）。
  - **顯示名。** per-item 那個 recall 的顯示名改成「per-item recall」（決定 16 第 5 項），跟 query group 那個 `recall` 分開。熱門度基準段與比較報表的 per-item 表，標題原本在顯示名前加「per-item」，改名後會出現兩次，所以拿掉這個前綴。`map_attr` 的表標題因此不再帶「per-item」。排序指標段 B 塊的標題寫成「B · per-item 歸因｜per-item recall@k」，前一個「per-item 歸因」是塊名，跟顯示名沒有緊貼著重複，所以這張票沒改。這些標題 #537 照決定 16 重定表名時一起處理。

### 決定 8　baseline 走跟模型同一套指標計算

- **今天的缺口。** baseline 走精簡路徑 `compute_overall_per_item`（`metrics_spark.py:1161`），不產出 `macro_avg`；報表卻照著讀 baseline 的 macro（`report_builder.py:1619`）。所以 baseline 段沒有 Macro 列，頭號指標從沒跟 baseline 比過。測試沒抓到：夾具 `_baseline_metrics_full`（`tests/test_evaluation/test_report_builder.py:936`）放了一份程式從不產出的 `macro_avg`，是假綠。
- **改成跟模型走同一套**（決定 4 的指標層），這個缺口順著補上。這是行為變更：baseline 段第一次出現 Macro 列。
- **baseline 不產出列的事實。** 資料概況屬於列的事實，A 與 baseline 共用一份（決定 4）。報表不讀 baseline 自己的資料概況：熱門度分數同一個 item 都一樣，同分列的數字會跟 A 不同，讀者看不懂。
- **成本沒量。** 這條決定不等量完才算數；量是為了估時間、找出要先處理的貴步驟（原則 2 補充一）。

### 決定 9　報表每段獨立

「一段」是報表一個大標題底下的內容。一段一個檔，段落順序放在一份清單。這是擁有者提的做法；作者原本提的是「由各段宣告自動產生導覽表與詞彙表」，那也是一種耦合。

- **每段在自己的檔裡宣告**：段名、一句話「這段回答什麼問題」、要讀哪些落地結果（照名字）。
- **不手寫導覽表。** 頁面外殼已經照段落標題產生目錄（`evaluation/report.py:88-98`）；每段那一句「回答什麼問題」顯示在目錄的標題旁（作者建議的細節，要改外殼；demo 時確認）。這取代概覽裡手寫的導覽表（`report_builder.py:580-603`），它今天在某段關掉時照樣印出那一列。
- **拿掉詞彙表段。** 定義貼在數字所在的段，[`diagnosis-report-presentation.md`](../agents/diagnosis-report-presentation.md) 第三節本來就這樣要求；指標的定義從指標目錄來。比較報表借用主報表詞彙表、詞對不上的問題一起消失。
- **段落之間不互相提到。** 加一條測試：一段的文字裡不得出現別段的標題，核心概念段除外（見下一條）。
- **只留一個單向依賴。** 核心概念段講地基概念，其他段需要時回指它；它不依賴任何段（同份規則第一節第 2 點）。
- **報表呈現規則放寬兩處（擁有者 2026-10-21 同意）。** 同份規則第一節第 1 點要概覽有「想回答什麼問題→看哪一區」的導覽；「貫穿全篇」那條要一區的數字能追回另一區。兩條都會讓段與段互相提到。定案：導覽改由「不手寫導覽表」那條講的目錄產生；數字只追回核心概念段的總量，不跨段對帳。那份規則已加一段例外，指回這裡。
- **比較報表照同一套。**
- **動手前先給 demo。** 這會改報表的長相，所以先用假資料做一份給擁有者看，點頭才做。
- **demo 定案（2026-10-22）。** 目錄那一句放在標題旁、全資料的總量只印在核心概念一處，見決定 16 第 12、13 項。

### 決定 10　報表裡還在做的判斷，搬到計算層

照原則 1 的判準，下面這些都算計算，搬進計算單元並落地：差值（決定 5）；K 截斷（`resolve_display_k`、`drop_metric_keys_above_all_k`）；全正 query group 的組數帳與警告；熱門度視窗的涵蓋月數；進 macro 的 item 數（`_macro_item_coverage` 是 `metrics_spark.py` 那條規則的第二份）；預測品質的顯示分箱與門檻掃描（`coarse_bin_table`、`threshold_sweep`）。

- **更正（#536 實作時發現，2026-10-23）。各項落在哪。**
  - **全正組帳與警告落在列的事實。** 每個粒度多三個鍵：`n_groups_with_positive`、`all_positive_share`、`all_positive_warns`，由寫列的事實的函式算。`all_positive_warns` 讀一個設定（`evaluation.query_filter.drop_all_positive_groups`，開關打開就不警告）。列的事實的定義是「跟分數無關」，不是「跟設定無關」，所以不衝突；這個設定本來就在 `COMPUTED_KEYS`，改了會重算。
  - **熱門度窗的月數落在 `baseline_metrics.json` 的 `popularity_window` 區塊**：回看月數、分數模式、涵蓋月數、各評估日的涵蓋月數、滿窗月數、平均每月的除數、窗滿不滿。決定 4 更正第一點寫的頂層 `window_months_covered` 搬進這個區塊，而且每種模式都寫（以前 count 模式只在多個評估日時才寫）。
  - **進 macro 的 item 數，報表讀 `population.macro_items` 的長度**（`result_readers.macro_item_count`）。`shared.macro_item_coverage` 刪掉，原則 6 更正裡那行搬家對照因此過時。
  - **顯示分箱與門檻掃描落在 `prediction_quality.json`**：`overall.display_bins`、`overall.threshold_sweep`、`per_item.display_bins`。
  - **差值落在比較層，(A, baseline) 這一對由 `compare_to_baseline` 寫 `baseline_comparison.json`。** 機制在 `pipelines/evaluation/steps/comparison_layer.py`（`compare_metric_results`），不跑 Spark。每個粒度（item；大類另一份）有 `overall`、`per_item`、`macro_by_item`、`per_segment` 四個區塊，每格是「這次、對照、差」，差只在兩邊都有那個鍵時才算（ADR-0020 bug 4）。主報表的熱門度基準段只讀它，不再呼叫 `compare.build_comparison_result`（決定 5 那句「`build_baseline_section` 呼叫……」因此過時），也不再自己算分群與大類的差。
    - **`all` 的配對在比較層做，而且帶標註。** 決定 15 的查證說指標鍵存整數，`all` 存成邏輯名會讓兩邊不同的 K 被當成同一格相減。比較層照舊存整數鍵，另外刻意多一格 `name@all`，把兩邊各自解析出來的 `all` 配成一對（今天 `per_item_metric_compare_table` 就是這樣配的），兩邊不同時，`population.all_k.same` 記成 `false`，由 #537 畫 ⚠。
    - **四項標註落在比較結果的 `population`**：`all_k`、`macro_items`、`scored_queries`、`segment_columns`，各是 `{this, reference, same}`。分群欄原本不在母體帳（決定 4 更正最後一點說留給 #536 補），現在每份指標結果的 `population` 多一個 `segment_columns`。(A, baseline) 四項都是 `same: true`；主報表照舊不畫「能不能比」表（HTML 要一字不差，畫是 #537 的事）。
  - **K 截斷落在 `resolve_display_ks`（不跑 Spark），寫 `display_ks.json`。** 它讀 `evaluation_metrics` 與 `baseline_comparison`，把主報表每張表要印的 K 切好落地；概覽、排序指標、熱門度基準三段照名字讀，不再呼叫 `resolve_display_k`、`metrics_section_ks`。決定 16 說的「K 截斷排在比較之後」，由「K 截斷讀比較結果」保證，`test_pipeline.py` 的 `TestTheKCutComesAfterTheComparison` 守著。它的設定指紋多帶它讀的 `report.display` 鍵，所以改了 `primary_map_k` 之後只做 `--only-node generate_report` 會被擋，訊息指向 `--from-node resolve_display_ks`；`report.display.*` 仍是「畫的」，不重算任何指標（ADR-0020 決定二的表因此加一個步驟）。
  - **報表要的值不在落地結果裡時報錯**（決定 4「清單要的結果沒算到時要報錯」）：報表經 `report_sections/shared.py` 的 `landed_value` 讀比較結果與 K，缺了就 `ValueError` 並指出產出它的 node（對照表 `LANDED_BY`）。
  - **比較報表那一側全部落地**（決定 4 第一點「比較的涵蓋率只在記憶體」、更正最後一點「A′、B′ 先在記憶體裡算列的事實」、決定 5「`generate_comparison_report` 在 node 裡算」都因此過時）。`--compare` 與 `--compare-only` 共用一串：`load_compare_predictions → restrict_to_common → compute_compare_metrics → compare_to_source → resolve_comparison_display_ks → generate_comparison_report`。前三個跑 Spark，後三個不跑。`restrict_to_common` 的涵蓋率改名 `compare_coverage` 並落地；`compute_compare_metrics` 落地 A′、B′ 各自的列的事實與指標（四份 JSON），今天 `generate_comparison_report` 裡那兩次 Spark 計算搬到這裡；`compare_to_source` 用同一個比較層機制比 A′ 與 B′，落地 `source_comparison.json`；`resolve_comparison_display_ks` 落地比較報表每張表要印的 K（per-item 表照舊只看 A′ 的 `all_k`，overall 表兩邊各看各的）。`generate_comparison_report` 只收落地結果，不收 Spark 物件；`--only-node generate_comparison_report` 不補跑任何 node（`RESUME_CONTRACTS` 釘住），CLI 仍會建 Spark session、查分區清單。`resolve_display_k`、`drop_metric_keys_above_all_k` 搬進 `pipelines/evaluation/steps/display_ks.py`，報表那側不再有截斷或算差值的函式。
  - **「跟誰比」記成每份比較產物的 `compare_source` 欄位，不進設定指紋的 `extra_keys`**（決定 5 最後一點）。內容由 `steps/compare_sources.py::compare_source_identity` 產生：`kind: model_version` 記 `kind`、`model_version`、`source`；`kind: external_hive` 記 `kind`、`table`、`columns`、`prod_mapping`、`unmapped_policy`；`label` 不記。沒寫的鍵先補上 loader 自己的預設值再記，所以「寫出預設值」與「不寫」讀到的是同一個 B′，不會誤擋。不放進指紋的理由：換了比較對象，每份比較產物同時過期，要建議的一律是 `--from-node compute_compare_metrics`；指紋的建議規則遇到多份過期只會建議單一 producer（要連環重跑）或整條 pipeline（在 `--compare` 會重跑主線）。只改 `label` 不擋，重畫就印新名字。
  - **決定 15 第 1 列的做法：重跑建議依模式指向存在的 node。** `require_computed_with_current_config` 多一個 `computed_rerun_node` 參數；比較側三個不跑 Spark 的 node 由依模式的 factory 建（`make_compare_to_source_node` 等），`--compare-only` 下 `COMPUTED_KEYS` 變了建議 `compute_compare_metrics`，`--compare` 下照舊建議 `COMPUTED_KEYS` 那一列的 node（不能也送 `compute_compare_metrics`：從它往後的切片含 `generate_report`，主線的產物這時也過期）。`test_pipeline.py` 的 `TestFingerprintRerunNodes` 把 `--compare-only` 也納入檢查。代價：`--compare-only` 只重畫時，帶不帶 `--post-training` 要跟寫那份結果時一樣，否則指紋不合而被擋（寧可多擋，照建議重算後結果正確）。
  - **決定 16 第 15、16 項的值已落地，報表還沒畫。** 四項標註與 `compare_source` 都在 `source_comparison.json`；「能不能比」表與「跟誰比」表由 #537 畫（#536 要求兩份 HTML 一字不差）。比較報表今天的 coverage 段照舊印 `table_a` 為 "n/a"（node 從沒寫過這個鍵），一併留給 #537。
  - **組數帳的減法改讀母體帳。** 報表原本自己算「算進衡量指標的組數＝有正例的 − 全正的」（`query_count_rows`、排序指標段的組數帳），現在讀 `population.n_scored_queries_with_positive`（`shared.scored_with_positive`）。
  - **#536 之前落地的結果要重算。** 它們缺上面這些鍵，設定指紋卻照樣對得上（沒有設定變）。報表看到缺鍵就報錯，指向 `--from-node compute_row_facts`（`refuse_pre_landed_reads`），不讓組數帳與警告默默從頁面消失。
  - **報表裡還留兩個「挑哪句話」的判斷，#536 沒搬。** 預測品質段看落地的組數，判斷它跟排序指標的母體是不是同一批（`n_excluded_queries == 0`，且全正組沒被排除），挑一句說明；熱門度基準段看 `popularity_window` 有幾個評估日，挑「平均每月」的說法。照原則 1 的字面判準（會拿來判斷），兩者都算計算。留下的理由：它們不產出任何印在頁面上或給別處用的數字，只決定印哪一句；要搬，前者得有一個同時讀兩份母體的單元，後者只是多落地一個旗標。代價是熱門度那條跟 node 算除數用的是同一條規則（評估日多於一個），改一邊時要一起改。#537 重畫這兩段時再決定搬不搬。

### 決定 11　比較報表只顯示「要比的指標」

- 比較報表放一份「要比的指標」清單，所有表只顯示清單上的。清單的一項是指標目錄的一筆（名字＋算法＋粒度），因為同一個名字可以在兩種算法各有一筆（決定 6）。預設跟今天一樣是頭號指標加護欄指標：macro 與 per-item 的 `map_attr`（K 用 `report.display.primary_map_k`）、per-item 的 `recall`（K 用 `guardrail_recall_k`）（`comparison/report.py:235-247`）。今天整體表把每個鍵都印出來（`comparison/report.py:172-198`），改成也只列清單上的。預設清單裡沒有 query group 層級的指標，所以照預設，整體表沒有東西可印、就不印；這會改變比較報表的長相，demo 時讓擁有者確認（決定 9）。
- 每段自己宣告要比哪些：主報表 baseline 段一份、比較報表一份。今天兩邊顯示的本來就不同。baseline 段的預設照今天印的四組表（`report_builder.py:1603-1683`）：overall 的 map／recall／precision；per-item 的 recall 與 `map_attr`（K 用 `primary_map_k`），含 Macro 列；per-segment 的 map；大類 overall 的 map。overall、per-segment、大類三組的 K 是寫死的 `[1, 2, 3, 4, 5, "all"]`，不是設定鍵。不要把比較報表的「頭號加護欄」套過去，否則 baseline 段會少掉 overall、per-segment、大類那幾張表。
- 比較報表與主報表共用同一套指標表的程式；比較報表裡每一格是「這次／對照／差值」。
- **理由。** 比較報表的目的是「要不要換模型」，每個數字都要扣回這個目的（`diagnosis-report-presentation.md` 第零節）。加指標快不快跟這題無關，決定它的是表照名字讀目錄（決定 6）。
- **改寫（2026-10-22）。** 預設清單、熱門度基準段（今天的 baseline 段）的 K、格子的寫法，在決定 16 第 2、7、8 項改了；本條跟它衝突的地方以決定 16 為準。

### 決定 12　一起處理與關掉的票

| 票 | 處理 |
|---|---|
| #366、#368、#505、#361 | 併入：碰到的正是要重寫的程式 |
| #450 | 併入：每一項改之前、改之後量時間 |
| #363 | 第 1 項跟 #450 一起量；第 3 項＝決定 5；第 5 項 #381 已做完（決定 3）；第 6 項＝決定 6；第 4 項在診斷拿掉後不存在；第 2 項（兩種模式分開存）等 inference 部署、監控模式真的用上再開新票。整張可關 |
| #446 | 留著（擁有者 2026-10-22 改判，原本寫併入）：它修的是熱門度回看窗的涵蓋揭露與窗的上界，跟這輪「報表好改、指標好加」的目標無關。熱門度的列照決定 4 搬進資料層，不必先修它 |
| #396、#524、#402 | 留著：#396 要等部署有隨機流量，#524 要等公司的設定檔清乾淨，#402 跨三條 pipeline |

關票與在票上留言，等本份的 PR 開出時再跟擁有者確認。

切票時才發現的 evaluation 舊票，照擁有者給的同一個方針：碰到要重寫的程式就併入；前提已經不在就關掉，關之前先問擁有者。

### 決定 13　順序的約束

PR 怎麼切、先後怎麼排，擁有者決定另用 `/to-spec`、`/to-tickets` 設計。下面是作者建議的約束，擁有者只拍板了「順序另外設計」，切票時可以推翻；其中「行為變更先做、純搬移最後」來自 flow 規則 3：

- 拿掉診斷（決定 2）排第一：刪掉的就不用整理。
- 宣告（原則 3、5）寫在計算單元旁邊，所以分層（決定 4）先於需求清單與指紋。
- 行為變更先做；原則 6 的純搬移排在驗收之前的最後（flow 規則 3）。
- 改報表長相（決定 9）之前先給 demo。
- 驗收（決定 1 的「加指標」）是最後一張，在新結構上做。
- 效能類的改動（#450、#363 第 1 項）改之前先量（原則 2 補充一）。本機合成資料只看得出相對大小；正式環境的 entity 是百萬級，driver 側的大小不能用本機數字推。

### 決定 14　附錄一的教訓併進 node 規則 13

併進 [`pipeline-node-design.md`](../agents/pipeline-node-design.md) 規則 13，範圍從 docstring 擴大到 `docs/` 的文字。本份的 PR 一起改。

### 決定 15　開工時再定的技術細節與預設做法

這幾項要對著程式才定得準，到那張票開工時定；下面是預設做法。標「作者建議」的兩列是寫本節時補的，其餘擁有者在拷問時看過。

| 項目 | 哪一步 | 預設做法 |
|---|---|---|
| 需求清單會讓 pipeline 的形狀跟著設定變，但今天 `test_pipeline.py` 要求 `COMPUTED_KEYS` 點名的每個重跑起點在每種模式都存在（`pipeline.py:131-138` 的註解；`config_fingerprint.py:99-121`） | 需求清單 | 指紋的錯誤訊息只指向這次真的存在的 node；那條測試改成檢查這件事 |
| 空殼（原則 3 補充二；作者建議） | 需求清單 | 拿掉：報表只讀需求清單裡的結果，`--only-node generate_report` 不讀清單以外的舊檔 |
| 指標鍵存邏輯名（`map@all`）還是解析後的整數 K | 指標層 | 先查當初為什麼存整數；理由還成立就不改，`all` 解析成幾記在母體帳 |
| 指紋分兩種：輸入資料當時的設定、這一步套用的設定。例如 `--compare-only` 的 A′ 用分區記錄的設定檢查分區，卻用今天的 `k_values` 計算 | 指紋 | 兩種都記、分開比對 |
| 大類算哪一層 | 指標層 | 照今天，當成指標層的一種粒度 |
| 公式在指標目錄裡怎麼表示（作者建議） | 指標層 | 宣告「由哪幾個共用數字怎麼組合」，不放 Spark 物件；報表讀目錄不必 import Spark 計算模組。不得用 UDF |

**「整數 K 還是邏輯名」的查證結果（#535，2026-10-23）：不改，鍵照舊存解析後的整數。**

- **當初沒有寫理由。** 整數鍵是最早 pandas 版的副產物：先把 `"all"` 換成 item 數，再用 `f"recall@{k}"` 組鍵，因為 K 本來就要拿去切前 K 名（`f812f28b` 的 `metrics.py`）。Spark 版沿用（`9b85956a`、`40b89607`），也沒寫理由。#434 加上 `resolved_all_k`，是在補整數鍵造成的洞（宣告 `event` 時讀者自己數 item 會查錯格），不是整數鍵存在的理由。
- **今天有一個理由成立：比較時兩邊的 `all` 可能解析成不同的數。** 決定 4 列的四種「照名字 join 會默默算錯」，第一種就是這個。鍵存整數時，A′ 的 `map@12` 跟 B′ 的 `map@10` 名字不同，不會被當成同一格相減；存成 `map@all` 就會。母體帳記下各自解析成幾，比較層需要時讀它判斷。
- **改的代價也不小。** `k_values: [12, "all"]` 而 item 剛好 12 個時，今天合成一個鍵；改成邏輯名要先定「兩個都存還是只存一個」。決定 16 第 7 項也要表頭寫解析後的數，讀者照樣要整數。

### 決定 16　報表的長相（#531 demo 定案）

2026-10-22 擁有者看了 #531 的假資料 demo（主報表、比較報表），對主報表提了 11 點回饋。改了幾版之後，他對頁面上列的待決題目回「全照建議」。本決定記下定案；跟決定 1、3、4、7、9、11 衝突的地方以本決定為準，那幾條各加了一行指過來。demo 是私人頁面，連結會記在 #531 的留言。

擁有者的總結有兩句：報表的整體一致性很重要，標題、版面、定義、風格都算；表格要能篩選、排序，方便複製。下面各項都是這兩句的展開。

1. **不標頭號指標。** 主報表概覽改列 training 的 HPO 能選的指標，並列、不分主次。今天的概覽只印兩個，而且標「頭號」。下文把這些叫「模型選擇指標」。
   - 名單是 `evaluation/metric_registry.py` 那張表的全部指標（`METRIC_NAMES`；今天四個：`mean_ap`、`macro_per_item_map`、`pooled_average_precision`、`macro_per_item_average_precision`），照擁有者的原話「HPO 指標有哪些選擇就列哪些」。名單寫成指標目錄裡的標記，不在報表裡 import registry（報表模組不准 import 計算模組）；加一條測試比對目錄標記的名單跟 `METRIC_NAMES` 相等，registry 多一個指標時測試會紅。
   - 值用 evaluation 自己算的數，整份報表一個指標只有一個數字（原則 4）：overall mAP 讀 `map@all` 的整體值，macro per-item mAP 讀各 item 那一種的 Macro `map_attr@all`；只有兩個 AP 要新的計算（第 3 項）。今天整體 map 只算 `evaluation.k_values` 列到的 K（`metrics_spark._resolve_k_grids`），Macro `map_attr@all` 是靠 `metric.k` 沒設時補算的；所以需求清單要保證這兩個一定算到「全部」這個 K，不看 `k_values`，也不靠 `metric.k`。這些值只在 `evaluation.metric` 用預設值（`min_positives`、`weight_alpha`、`shrinkage_k`）、而且 `evaluation.query_filter.drop_all_positive_groups` 沒打開時，才等於 training 在 test 上算的同名指標；指標目錄要註明。
2. **比較報表的預設清單**（改寫決定 11）：從「頭號指標加護欄」改成「模型選擇指標（第 1 項），加上護欄（各 item 的 `map_attr` 與 recall），再加上大類的 `map_attr` 與 recall」。所以比較報表的整體段照預設就有一張表；決定 11 寫的「照預設不印整體表」不再成立。
3. **兩個 AP 每次都算**（改寫決定 4、7；推翻 ADR-0024 一個沒選的做法）。
   - evaluation 今天不算 pooled AP 與 macro per-item AP，只有 training 在 test 上算（Spark 計算是 `metrics_spark.compute_pooled_average_precision`、`compute_macro_per_item_average_precision`）。A′、B′ 也要算，因為比較報表的清單有它們。
   - 成本：每次都算要多兩次 Spark 計算，pooled 那次要把全部候選列依分數排一次（分段做，三個 action）。ADR-0024〈考慮過、沒選的做法〉第一條正是因為這個成本沒選精確 AP；擁有者看過「成本沒量」這句，仍選每次都算。#450 一起量，太貴再加開關。
   - 它們不屬於決定 7 的三種算法，是第四種「原始分數上的二元預測」。要一個新的計算單元去呼叫 registry 的 Spark 計算，不是在指標目錄加一筆就好。
   - 指標目錄要註明它們跟預測品質段的分箱 PR-AUC 不同：一個在原始分數上算，一個在分箱後算，數字會差一點。
4. **標題中文為主、盡量短**（改寫決定 3 的段名）。段標題旁用小字放英文。指標名（`map@K`、`recall@K`）與角色名（item、query group、entity）留英文。
   - 主報表的段：概覽、核心概念、資料統計、排序指標、預測品質、名次分布、熱門度基準、完整性檢查。對應今天的基本統計、衡量指標、per-item 細部拆解、baseline 這幾段改了名。
   - 比較報表的段：概覽、整體指標、各 item 指標、大類指標、分群指標。
   - 重要名詞的中英對照只有一份，放在報表外殼。每段第一次出現的詞加虛線，滑過去看英文。
5. **表名。** 表名就是內容；列是切片時，再加「 · 列是什麼」，例如 `map@K · 整體與各分群`、`正例組成 · 各期`。表名不放括號說明。「參與 macro 的 item 數」只在進 macro 的 item 少於全部時才出現，寫在表下一行。各 item 那種 recall 的顯示名是「per-item recall@K」。
6. **表的形狀。**
   - 指標表一個指標一張：列是切片（整體、各分群、各 item），欄是 K。「整體」是分群表的第一列；沒有 K 的指標，列是指標。
   - item 粒度與大類粒度排成同一個骨架：先「整體與各分群」三張，再「各 item（各大類）」兩張。
   - 其他表（總量、比例、關鍵數、分箱、資料統計）一張放多個量。資料統計的四張表用同樣五欄：候選列、候選列佔比、正例數、正例佔比、正例率。
7. **K 寫實際的數**（改寫決定 11）。表頭的 `@all` 寫成它解析成的數，例如 @6；核心概念段說一次「最大的那個就是全部」。同一段的表用同一組 K，所以熱門度基準段全部用 `report.display.primary_map_k`。今天那一段只有各 item 的表用 `primary_map_k`，整體、分群、大類都寫死 `[1, 2, 3, 4, 5, "all"]`。比較報表的各 item 指標段也一樣：`map_attr` 與 recall 都用 `primary_map_k`（今天 recall 用 `report.display.guardrail_recall_k`）。
8. **比較表每個 K 三欄**（改寫決定 11 的「每一格是這次／對照／差值」）：這次、對照、差，差＝這次 − 對照。理由是方便貼進 Excel。
9. **數字格式。** 計數是整數，加千分位；比例與指標四位小數；差帶正負號，零寫 0.0000，不寫 ±（讀者會讀成信賴區間）。
10. **全篇不收合。** 今天有些表收合、有些不收，擁有者嫌不一致。
11. **表格工具。** 全部是頁面裡的原生 JavaScript：不連網，不加套件。
    - 每張表每一欄的欄頭都能點，點了跳出像 Excel 的選單。
    - 選單裡可以排序：由小到大、由大到小、原本順序。
    - 文字欄勾要看的值；數字欄填最小、最大。
    - 篩選中的條件以標籤列在表上方，點一下就拿掉。
    - Macro、整體那一列固定在最上面，不會被篩掉。
    - 每張表右上角有「複製」：複製看得到的列，貼進 Excel 一格一個數；⚠ 不會跟著複製。
    - 表格裡的字不換行，太寬就在表格框裡左右捲。
    - 超過 15 列的表固定高度，捲動時表頭不動。
    - 各 item 的分箱表今天是一個 item 一段往下接；改成一張表，item 是一欄，用欄頭勾要看的 item。
    - 複製取原始值：數字到小數第 6 位，負號是半形 `-`（畫面上的「−」是 U+2212 減號，Excel 讀不成數字）。兩層的表頭攤成一列，例如「@1 這次」。
12. **全資料的總量只印一處**（決定 9 的「數字只追回核心概念段的總量」）。全部 query group、候選列、正例數這類全資料的總量，只在核心概念的總量表印；別段要用就寫「核心概念的總量」。切片的計數照印，例如各期、各 item 的候選列與正例數。今天在概覽、資料統計、排序指標、預測品質、完整性檢查重印的總量都收掉；大類粒度的全正組數也搬進總量表。比較報表沒有核心概念段，兩邊的總量放在概覽的涵蓋表。「能不能比」表的組數不算重印：它比的是兩邊的母體。
13. **目錄**（決定 9，demo 確認）。外殼照段落產生目錄，每段那一句「這段回答什麼問題」放在目錄的標題旁。
14. **occasion 與 event 只改核心概念段**：改 query group 的定義與總量表。
    - 宣告 occasion：總量表多一列「time × entity 的組合」。
    - 宣告 event：總量表多一列「`(query group, item)` 的組合」。「全部」不再是 item 數，是一個 query group 裡最多的候選列數（`metrics.resolved_all_k`）。
    - 不做照 occasion 切的表：occasion 的值像請求 ID，種類太多。
15. **能不能比**（決定 4）。主報表熱門度基準段與比較報表概覽用同一張「能不能比」表，列四件事：全部＝@幾、進 Macro 的 item、算進指標的 query group、分群欄。兩邊不同時，受影響的欄頭或列名標 ⚠，表下一行寫兩邊各是多少；數字照印。對照沒有分群欄時，分群表整張不印，只在「能不能比」表寫明。
16. **「跟誰比」表**（決定 5）：印出決定對照內容的每個鍵，也就是 `kind`、model_version 或表名、`source`、`columns`、`prod_mapping`、`unmapped_policy`。
17. **其他。**
    - 各 item 那種算法這次還沒有 precision：今天的算法只看正例列，數不出「排進前 K 名幾次」。決定 7 由 #541 補上 per-item 的 precision@K 與 F1@K。（#534 之後已數得出「排進前 K 名幾次」，見決定 7 的更正；precision@K 仍由 #541 加。）
    - `mean_pos` 從排序指標搬到名次分布，叫「正例平均名次」。
    - 預測品質的兩張關鍵數表拿掉候選列、正例數、正例率。
    - 分箱表的欄分三組：分數、這一格、門檻以上。
    - 今天基本統計段的說明寫死「此 run 5,232」「此 run 每 item 654」（`report_builder.py:774-775`），不管跑哪份資料都印這兩個數；新版一起修掉。

**影響到哪些票。**

- #537 照本決定做。票上跟本決定衝突、以本決定為準的有：預設清單的內容與「照預設沒有整體表」；「每格是這次／對照／差值」（改成每個 K 三欄）；「baseline 段的 K 照今天」；「在『衡量指標』段的表加一欄」（見決定 1 的改寫）。
- #536 要求兩份 HTML 一字不差，所以它照今天的 K 與格子落地比較結果；#537 改 K 時，只要重跑不跑 Spark 的 K 截斷單元（決定 10）。前提是比較結果落地的是 `evaluation.k_values` 的全部 K，K 截斷排在比較之後；#536 開工時確認。落地的比較結果要帶第 15 項的四項標註。
- #541 的驗收表寫「在『衡量指標』段的表加一欄」與「每個指標只在指標目錄加一筆」。兩個 AP 若排給它，那一項的驗收要改成「新的計算單元」（第 3 項）。
- #530 的使用者故事與決定摘要裡，頭號指標、比較報表照預設不印整體表、baseline 段的 K 這幾句已經過時，以本份為準。
- 兩個 AP 排在哪張票，切票時定（作者建議 #541，跟其他新指標一起）。在那之前，概覽只有排序那兩列。

**留給切票時定。**

- `evaluation.metric.k` 失去主要讀者。今天它決定頭號指標截斷在哪（`report_builder.py` 的 `_headline_key`、`_headline_column`），也印在完整性檢查的設定表（`build_completeness_section`），A15 檢查它的值（`core/consistency.py`）。拿掉頭號之後，計算上只剩 `metrics_spark.py:213` 把它加進各 item 的 K 清單。#537 盤點後決定拿掉或留；拿掉的話，照慣例在 `core/consistency.py` 擋舊鍵。
- `report.display.guardrail_recall_k` 唯一的讀者是 `comparison/report.py`；第 7 項之後沒有讀者。拿掉（照慣例擋舊鍵）或留，#537 定。
- `report.display.primary_map_k` 名字裡的 primary 是「頭號」的意思，現在它只是「顯示用的 K」。改不改名，#537 定。
- 講頭號指標的地方 #537 一起改。已知的有：`report_builder.py`、`conf/base/parameters_evaluation.yaml` 的註解、`docs/pipelines/evaluation.md`、`CONTEXT.md` 的兩個 _Avoid_ 條目（選版指標、test mAP）、`docs/metrics/metrics.html`、`tests/test_evaluation/test_report_builder.py`、`tests/test_pipelines/test_evaluation/test_nodes.py`。動手前用「頭號」與英文 headline 各 grep 一次；headline 也出現在跟頭號指標無關的檔，只改講頭號指標的。
- 名次分布段的開關是 `report.sections.per_item_breakdown`（#532 從 `diagnostics` 改名）。段名改成名次分布之後，它和 `dataset_overview`、`primary_map` 這些開關名要不要跟著段名改，#537 定。
- training 算這兩個 AP 時，每一列都要帶 `ZERO_POSITIVE_GROUP_WEIGHT_COL` 這欄權重（`metrics_spark._binary_prediction_rows`）。evaluation 的列要不要帶、權重怎麼給，#541 開工時定。
- 表格工具的 JavaScript 已經寫在 demo 頁面裡，#537 可以直接拿來改。

### 對既有 ADR 的修訂

- [ADR-0013](0013-pipeline-modes-and-slicing-are-separate.md)：`create_pipeline` 由明列 node 改成由各單元的宣告推導（原則 3）。該份文末加一段修訂指向本份。
- [ADR-0019](0019-evaluation-modules-split-by-role.md)：`report_builder.py` 與被它拉住的模組（含 `baselines.py`）不再留在根層 `evaluation/`（原則 6）。該份文末加一段修訂指向本份。
- [ADR-0020](0020-evaluation-bug-round-intended-behaviours.md)：〈橫切決定二〉所有結果共用一張 `COMPUTED_KEYS` 的指紋，改成每份結果只對它讀過的設定算指紋（原則 5）。該份文末加一段修訂指向本份。
- [ADR-0024](0024-prediction-quality-metric-family.md)：〈考慮過、沒選的做法〉第一條因為全量排序的成本沒選精確的 average precision。決定 16 第 3 項改成每次都算 pooled AP 與 macro per-item AP（擁有者知情，成本由 #450 量）。該份文末加一段修訂指向本份。

---

## 附錄一　一條不限 evaluation 的教訓

**程式數得出來的數量，文字裡不要寫成數字。** #520 改了大約 15 句這類句子，例如「四個消費者」「七個輸入裡五份落地」。

最典型的例子是 node 數：

- 同一個數字寫在兩處：`tests/test_core/test_architecture_constraints.py::test_static_coverage_floor` 的斷言，和 `docs/agents/architecture-constraints.md` 的一句「62 個 node 裡 59 個」。
- #517、#519、#520 各刪了一個 node。測試的斷言每次都有人跟著改，因為不改測試就會紅。
- 文件那一句從 2026-09-30 起就停在 62，#517、#519 都沒改，直到 #520 rebase 時才一次改成 60。

這類數字要嘛不寫，改寫成「去哪裡查」；要嘛只寫在有測試會核對的地方。本份第三節也寫了不少數字，所以標明是 `11ad407e` 的快照，並附上重現指令。

去處：這條跟 evaluation 無關，核准時再決定放進哪一份給 agent 讀的文件，例如併進 node 規則 13（docstring 講為什麼）。2026-10-21：併進 node 規則 13，範圍擴大到 `docs/` 的文字（決定 14）。

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
