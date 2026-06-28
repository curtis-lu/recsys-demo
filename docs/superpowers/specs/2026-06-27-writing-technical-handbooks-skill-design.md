# `writing-technical-handbooks` Skill 設計文件

**狀態**：草案，待使用者審核
**日期**：2026-06-27
**分支**：`feat/handbook-writing-skill`（worktree `.worktrees/handbook-skill`）

**目標**：把《Spark 優化參考手冊》round-1/2/3 累積、已實證有效的「撰寫技術自學手冊」完整方法論，固化成一支可複用、跨專案的個人 skill。

**藍本來源**：`memory/project_handbook_writing_skill.md`（round-1/2/3 萃取）。本 spec 是經 grilling 釐清後、把藍本收斂成可實作的設計。

---

## 1. 背景與動機

過去寫《GBDT 數學推導手冊》與《Spark 優化參考手冊》時，逐輪磨出一套**重複出現、可遷移**的手冊撰寫方法（讀者 persona 建模、每章體例、5 審稿角色、純標點稽核、slugger 錨點驗證、概念地圖式 landing page、分階段回饋修訂…）。這些目前散在多份 plan/spec 文件與一份 `docs/handbooks/handbook-writing-guide.md`，每次寫新手冊都要重新翻找、重新口傳。把它固化成 skill 後，未來任何主題的技術手冊都能一鍵載入這套流程與工具。

## 2. 範圍

**鎖定體裁**：多章節的**技術自學／教學型長文**（手冊、guide）。**域無關**（主題可為 Spark、Postgres、K8s、ML…），但**形式固定為「給人自學的多章節教材」**。

**明確排除**（非本 skill 對象）：RFC、設計文件、API 參考、runbook、單篇 how-to。理由：本 skill 的招牌方法（讀者 persona、每章體例、capstone 貫穿範例、概念地圖 landing page、教學法審稿）都預設「這是教學作品」，放寬會稀釋強度。

**語言**：方法論語言無關；但隨附一份繁體中文慣例檔（見 §5），未來寫英文手冊時抽換該檔即可。

## 3. 設計決議總表（grilling 產物）

| # | 決議 |
|---|---|
| D1 體裁 | 鎖定多章節技術自學手冊（域無關、形式固定）。 |
| D2 與 superpowers 關係 | **邊緣複用、核心母語**：複用 `brainstorming`＋`dispatching-parallel-agents`；規劃／審稿／品質關卡自寫手冊母語，不包 `subagent-driven-development`。執行階段的「skill 撰寫工藝＋測試」遵 `writing-skills`。 |
| D3 互動模式 | **關卡式（gated）**：四個關卡停下給使用者確認，關卡之間 subagent 自動跑並留 live log。 |
| D4 放置 | 個人層 `~/.claude/skills/writing-technical-handbooks/`；**references/ 完全自足**（素材拷貝進去、不指回原 repo）。 |
| D5 品質關卡 | 做成**可執行腳本**＋self-test：`punctuation_audit.py`、`anchor_check.py`。 |
| D6 風格檔 | **分兩檔**：`writing-style.md`（語言無關）＋`language-conventions.md`（繁中專屬；標點集與稽核腳本同源）。 |
| D7 工作流入口 | **兩入口**：從零寫新手冊 ＋ 回饋驅動修訂輪，後段審稿／品質關卡共用。 |
| D8 metadata | 名稱 `writing-technical-handbooks`、手動觸發（`disable-model-invocation: true`）。 |
| D9 Follower | 新增 **optional 第 6 審稿角色 Follower**：只照手冊文字實作、回報脈絡缺口。逐章、被環境閘住（見 §6 gating）。 |
| D10 Persona 引導 | 需求鎖定提供**結構化 persona 引導**：六維具體錨點＋proxy 人物法＋**「skill 使用者不熟主題」的 fallback**（skill 擬前置知識 ladder、使用者只分類）。見 §6.2。 |

**`writing-skills` 對上表的修正（grilling 後段補上）：**

- **M1（修 D8 description）**：description 只寫「Use when…」觸發條件，**嚴禁摘要工作流程**（官方實測：摘要會讓 Claude 照 description 走、跳過內文）。
- **M2（修 D5 模板哲學）**：5 審稿 prompt **不做抽象挖空模板**，改為**各放一個用 Spark 手冊 persona 填好的具體範例**，使用者照著改（「一個出色範例 ＞ 填空模板」）。
- **M3（印證 D4/progressive disclosure）**：SKILL.md 精簡（目標 <500 字），細節下沉 references/。
- **M4（新增 scope）**：採用「skill 也要 TDD」——按比例（見 §8）。

## 4. 與既有 skill 生態的關係

```
需求釐清        →   grill-me / grilling（已完成）＋ brainstorming
規劃 skill 怎麼建  →   writing-plans
執行（寫 skill）   →   writing-skills 規範工藝＋測試（RED-GREEN-REFACTOR）
  └ 內部苦工      →   dispatching-parallel-agents（如批次審稿/機械修訂）
```

本 skill 產出後，它**自身**在被使用時的工作流（§6）會在內部複用 `brainstorming`（手冊需求釐清）與 `dispatching-parallel-agents`（批次審稿、機械式修訂）。對 superpowers 是**軟依賴**：SKILL.md 標注「建議搭配 superpowers；若無，相關步驟退化為純文字指引」。

## 5. Skill 結構

```
~/.claude/skills/writing-technical-handbooks/
├─ SKILL.md                      # 關卡式工作流 + guardrails + 何時叫哪個 subagent/腳本/模板（精簡，<500 字本體）
└─ references/
   ├─ reviewer-prompts/          # 5 必備 + 1 optional 審稿角色，各一個「填好的具體範例」（非抽象模板，M2）
   │   ├─ R1-technical.md          技術正確性：版本釘死、只認官方來源、逐條 ✅/❌(正解+出處)/⚠️
   │   ├─ R2-reader-novice.md      初階讀者 persona（讀懂度）
   │   ├─ R3-reader-advanced.md    進階讀者 persona（讀懂度）
   │   ├─ C-architecture.md        跨全書一致性／能力地圖／里程碑審
   │   ├─ P-pedagogy.md            教學法／Diátaxis 四象限
   │   └─ F-follower.md            （optional）動手實作脈絡充足性探針：只照手冊文字實作、回報卡點與缺口
   ├─ scripts/
   │   ├─ punctuation_audit.py     純標點稽核 + self-test（吃一個 .md 目錄）
   │   └─ anchor_check.py          slugger 錨點驗證 + 相對連結健檢 + self-test
   ├─ checklists/
   │   ├─ chapter-template.md       每章體例：前提→目錄→內文→capstone→取捨→一句話帶走→上下章導覽
   │   ├─ reader-gotchas.md         讀者反覆會卡的點（術語首見沒 gloss、缺前置概念錨點…）
   │   └─ reviewer-checklist.md     定稿前自審清單
   ├─ persona-elicitation.md       讀者 persona 引導：六維具體錨點 + proxy 人物法 + 非專家 fallback + persona spec 模板
   ├─ writing-style.md             語言無關散文原則（簡潔但描述性、宣稱/新名詞要脈絡、抽象主張數字落地、結論誠實、不洩漏鷹架、符號紀律、貫穿範例）
   ├─ language-conventions.md      繁中專屬（專有名詞沒慣用譯法用原文、繁體不簡體、全形標點、稽核標點集 `— ，：。（）；`）
   └─ landing-page-recipe.md       概念地圖式首頁設計 + 反面教訓（過瘦失脈絡、band 標籤塞儲存格很醜）
```

**分工界線**：`writing-style.md`＝「怎麼下筆」（字句層）；`chapter-template.md`＝章的**結構**骨架；`reader-gotchas.md`＝讀者**會卡在哪**。三者不同層、不重疊。

**素材來源**（建 references/ 時從這些萃取，多數在 `feat/spark-tuning-handbook` 分支／`.worktrees/spark-handbook`）：

- 5 審稿 prompt + 寫作慣例 + 每章 Step A–F：`docs/superpowers/plans/2026-06-22-spark-handbook-round2-revision-plan.md`、`…/2026-06-14-spark-tuning-handbook-plan.md`。
- 回饋驅動分階段修訂計畫 + 破折號 fix subagent prompt（機械式 fix subagent 模板）：`docs/superpowers/plans/2026-06-24-spark-handbook-round3-feedback-revision-plan.md`。
- 審稿框架／能力地圖：`docs/superpowers/specs/2026-06-22-spark-handbook-round2-review-design.md`。
- 風格原則（需泛化、去數學味，並補 grilling 新增的三條）：`docs/handbooks/handbook-writing-guide.md`。
- 純標點稽核 / slugger 演算法：藍本 `memory/project_handbook_writing_skill.md` §3。
- 成品 worked example（文字指路、非依賴）：`docs/handbooks/spark-tuning/`。

## 6. 工作流（SKILL.md 本體）

SKILL.md 開頭分流兩入口，後段收斂到共用的審稿＋品質關卡。**四個使用者關卡**（D3）：

**入口一・從零寫新手冊**
1. 需求鎖定（複用 brainstorming）：讀者 persona（建議雙/多軌，引導見 §6.2）、範圍、授權、來源規則（技術只認官方）、審稿角色、分階段流程、交付終點。→ **關卡①：出 spec，使用者確認方向**。
2. 規劃：分章計畫，每章 Step A–F（A 查證→B 寫草稿→C 派審稿→D triage→E 使用者審→F commit）＋每章「須查證重點」。→ **關卡②：使用者過目計畫**。
3. 逐章執行：A–F 循環；批次審稿用 dispatching-parallel-agents。→ **關卡③：每章/階段 triage 結果給使用者拍板**。

**入口二・回饋驅動修訂輪**
1. Intake 回饋 → 盤點（grep 定位每條、找連帶實例）。
2. 出**分階段計畫**（每階段一個 commit）。→ **關卡②'**。
3. 逐階段執行 + 過品質關卡。→ **關卡③'：每階段確認再進下一個**。

**共用後段**：5 必備審稿角色（＋逐章視情況加 optional Follower，見下）→ 三級 triage（真缺陷必補／可加強斟酌／誤讀或刻意取捨不改）→ 品質關卡（§7）→ commit。

**口味校準關卡（兩入口共用）**：風格類大改（破折號砍多狠、index 重構）→ **關卡④：先做一章樣本＋分級選項（剛好/太兇/再狠），確認力道再全書套**；設計大改先討論→確認方向→呈現 mockup→才動手。

### 6.1 Follower（optional 第 6 審稿角色，D9）

抓一類 R1/R2/R3 都漏的缺陷：**手冊技術正確、也讀得懂，卻跳了真正動手者才會撞到的步驟**（未明說的前置條件、沒給的指令、預設已有的檔案/權限/環境）。Follower **只照手冊文字實作**，產出「在哪一步卡住／被迫自己猜／得動用手冊沒給的知識」＝**脈絡充足性探針**。本質是把 `writing-skills` 的「派 subagent 試跑真實任務」套到手冊層（與 §8 同源）。

**何時派（gating rubric，三者皆過才派）**：

1. **程序具體性**：該章有具體可重現的程序/capstone（概念章如「心智模型」「選型決策」無可執行之物 → 不派，R2/R3 已覆蓋）。
2. **環境可達性**：subagent 構得到執行環境（見下三層）。
3. **邊際增益**：相對 R2/R3 讀懂度通讀有額外價值。

**環境三層（成本可預期）**：

| 層 | 環境 | 成本 | 可否派 Follower |
|---|---|---|---|
| Tier 0 | 無環境、純推理/紙上跟做（概念、數學推導） | 近乎免費 | ✅ |
| Tier 1 | 本機沙盒（本機 Spark `local[*]`、Python、本機 DB） | 中（cold start） | ✅ 一部分章 |
| Tier 2 | 真實生產環境（CDP 叢集、CRM 等） | subagent 構不到 | ❌ 標「待真實環境驗」，靠人類讀者 |

**兩條讓它有效的護欄**（否則給假訊號）：

1. **只准用手冊文字**：硬性禁止上網/動用手冊沒給的背景知識（否則它會偷補洞、把缺脈絡蓋掉）；每次伸手到文字之外都記一筆。
2. **環境失敗 vs 脈絡缺口分欄**：因 cold start 抽風而失敗 ≠ 手冊缺脈絡；報告分兩欄，避免 false negative 污染 triage。

### 6.2 讀者 Persona 引導（D10，最高槓桿輸入）

persona 一模糊，下游深度／該 gloss 什麼／capstone 真實度全跟著偏。需求鎖定階段以結構化方式引導，產出注入每個 reviewer prompt（R2/R3/F 尤其依此扮演）。

**六維，每維「具體錨點 ＞ 形容詞」**（不寫「中階」，寫「他現在已能做到的一件具體事」）：

| 維度 | 具體錨點寫法範例 |
|---|---|
| 背景知識（地板/天花板） | 「能寫多表 JOIN、看得懂 `EXPLAIN`，但沒聽過 shuffle」 |
| 閱讀目的 / job-to-be-done | 「讓每週批次別再 OOM」（非「學 Spark」） |
| 深度期待（承重水位） | 「要會調也要懂為什麼，但不需讀原始碼」 |
| 工具 / 操作介面 | 「在 Hue 寫 SQL、偶爾 spark-submit；不寫 Scala」 |
| 環境 / 約束 | 「CDP 7.1.9、禁 UDF、無外網」← **環境約束的正當歸宿**（寫成手冊普世規則才是洩漏，見 §2 修正） |
| 反例（不是寫給誰） | 「不是給已在 prod 跑 Spark 的資料工程師」 |

**引導方式（複用 brainstorming，一次一問）**：

1. 先要一個**真實 proxy 人物**（某同事，或 N 個月前的自己）——真人比抽象 persona 好推理。
2. 就那個真人逐維逼出**具體錨點**（問「他能完成的一個任務是？」而非「他程度如何」）。
3. 問有無**第二讀者**（明顯更進階/初階）→ 有則收 2 個（地板＋stretch）；上限 2–3 個（YAGNI）。

**非專家 fallback（核心手法，處理「skill 使用者自己不熟主題」）**：當使用者答不出「讀者懂不懂 X」（因他自己也不確定 X 是什麼）→ **翻轉負擔**：skill 派研究步驟（官方來源）草擬該主題的**前置知識 ladder**（基礎→進階 candidate 清單）→ 使用者**只對每一階標「會／半懂／不會」**。使用者只需懂「讀者」，不需懂「主題分類學」。

**兩個減壓閥**（避免在 persona 上過度糾結）：proxy 人物法本身降低非專家焦慮；persona **不必一次到位**——R2/R3 讀者通讀＋Follower 會在審稿時抓出「假設程度錯了」（術語 gloss 太晚、漏前置步驟），故 persona 是「夠好就開工、靠 review loop 校準」，非完美主義關卡。

**產出物 persona spec 區塊**（進手冊 spec，注入 reviewer prompt）：

```
## 讀者 Persona
### P1（地板）：<proxy 一句話>
- 已能：<具體任務>／想達成：<job-to-be-done>／深度期待：<水位>
- 工具介面：<…>／環境約束：<…>／不是寫給：<反例>
### P2（stretch，可選）：…
```

## 7. 品質關卡與腳本（D5）

- **純標點稽核**（`punctuation_audit.py`）：證明「只動標點、其他字未改」。`git diff` 取 ±行，各自剝掉標點集後比對 ordered list 是否完全相等；配合 insertions==deletions 對稱、圍欄 ```` ``` ```` 數偶數，三證合一。標點集從 `language-conventions.md` 讀取（不寫死），換語言跟著換。
- **錨點級 slugger 驗證**（`anchor_check.py`）：github-slugger 近似（小寫→保留字母/數字/CJK/空白/-/_→空白轉 `-`、不合併連字號）＋相對連結健檢。內建 self-test：餵已知標題吐已知 slug。
- **連結健檢**：所有相對 `](X.md)` 目標檔存在（併入 `anchor_check.py`）。
- 每輪改完跑上述 + C 一致性複核（章序/footer/導覽表/交叉引用一致）。

兩支腳本**純 stdlib、吃一個 .md 目錄當參數、與主題/語言解耦**，self-test 確保稽核器本身可信。

## 8. 開發方法論：對 skill 本身做 TDD（採用 `writing-skills`，按比例）

本 skill 屬 **technique/reference 型**（非紀律強制型），故按比例採用 RED-GREEN-REFACTOR：

- **RED（baseline）**：派一個**沒有此 skill** 的 subagent，給它一個真實的小型手冊任務（例：「為某主題寫一章草稿並自審」或「對一份既有手冊跑一輪回饋修訂」），記錄它**做不到/做不好**之處（漏 persona、無體例、不會純標點稽核、錨點驗證跳過…）。
- **GREEN**：寫 SKILL.md + references/ 補那些洞；派**有此 skill** 的 subagent 重做同任務，驗證它能正確套流程／找對 reference／指令無洞。
- **REFACTOR**：subagent 出現新的誤用/卡點 → 補明確指引、堵漏洞 → 重測。

測試以「**能否正確應用技術／找對資訊／指令是否有缺口**」為成功準則（非對抗壓力測試）。全程符合「subagent 過程可同步審核」——每個測試 subagent 留 live log。

> **同源對稱**：本節對 **skill** 做的 RED-GREEN-REFACTOR，與 §6.1 的 **Follower** 對 **手冊** 做的「只照文字試跑、回報缺口」是同一個方法論套在不同層級。兩者共享「派受限 subagent 試做真實任務、把卡點當缺陷訊號」的精神。

## 9. 落地與放置

- **交付物**（SKILL.md + references/）寫到 `~/.claude/skills/writing-technical-handbooks/`（個人層、跨專案、不進版控，與既有 `grill-me`/`grilling`/`graphify` 一致）。
- **過程文件**（本 spec + 後續 writing-plans 計畫）進 `feat/handbook-writing-skill` 分支的 `docs/superpowers/`（進版控、留設計脈絡）。
- skill 本身自足，未來要 promote 成 plugin 只是搬資料夾。

## 10. 非目標 / YAGNI

- 不做泛長技術文件（RFC/runbook/API 參考）共用層——等真有需求再抽。
- 不做 plugin 化／marketplace 發佈——v1 個人層。
- 不做 HTML 轉檔流程——那是手冊產出的下游、與本 skill 解耦。
- 不允許自動觸發——v1 手動叫。

## 11. 開放問題 / 風險

- **素材跨分支**：多數素材在 `feat/spark-tuning-handbook`，建 references/ 時需從該 worktree 讀取／複製。
- **軟依賴 superpowers**：缺 `brainstorming`/`dispatching-parallel-agents` 時退化為文字指引，SKILL.md 須標注。
- **風格檔泛化品質**：`handbook-writing-guide.md` 偏數學味，泛化時要避免去掉有用的具體性（保留「抽象主張要數字落地」這類原則，只抽掉數學專屬例子）。
