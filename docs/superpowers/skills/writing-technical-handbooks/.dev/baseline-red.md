# RED Baseline — no-skill 開章場景（Plan Task 0）

開發期記錄，**不 deploy**（`.dev/` 在 Task 13 部署時排除）。這是「失敗測試」：一個沒有本 skill 的 subagent 做一個微型手冊任務時，自然漏掉什麼。漏掉的每一項，就是某個 skill 元件存在的理由。

## 固定任務（GREEN/Task 11 會重用同一題）

> 為一本技術自學手冊寫「開章」（本章前提 ＋ 前兩節草稿）。主題：**用 `EXPLAIN` 讀懂 PostgreSQL 查詢計畫**；讀者：**會寫多表 SQL、但從沒看過查詢計畫的資料分析師**。寫完自我審稿一次、列出弱點。繁體中文。不呼叫 skill、不上網。

派 general-purpose subagent（無 skill、無 WebFetch），agentId `acbac78a307d93399`。

## ⚠️ 重要方法論發現：baseline 被環境污染

baseline subagent **開場白自承**：「this project has a handbook-writing style guide with principles I should follow (concrete numbers, honest conclusions, don't leak scaffolding, general-before-specific). Let me apply those.」——它**從專案的 CLAUDE.md / memory（`MEMORY.md` 注入 system prompt）繼承了手冊撰寫哲學**，所以它的草稿已經套了「通用先於具體」「具體數字」「誠實自審」「不洩漏鷹架」。

**這是真實 baseline 的污染源**：在這個 repo 裡跑的 subagent 不是「乾淨的 naive agent」。**但這反而強化 RED 訊號**：即使是一個**已經懂方法論**的 agent，下列缺口仍然存在——代表這些缺口不是「常識能補」的，必須靠 skill 的明確元件與關卡才補得起來。

> **這本身是 skill 要記的一條 guardrail**：跑 baseline / 對照測試時，要意識到 subagent 會繼承專案 ambient 方法論；真正乾淨的 baseline 需在無此 memory 的環境跑，或明確指示「忽略任何既有手冊指引」。（將寫入 SKILL.md guardrails 或 testing 註記。）

## 觀察到的缺口（≥5，每條附 baseline 佐證 → 對應 skill 元件）

| # | 缺口 | baseline 佐證 | 對應 skill 元件 |
|---|---|---|---|
| G1 | **捏造技術輸出、無來源紀律**：`EXPLAIN` 的 cost/rows/time 全是編的，連輸出**折行格式**都與真實 psql 不符（真實是一長行） | 自審 #1 自承「範例輸出是我『編』的，沒有實機驗證……真實輸出是一長行」 | `reviewer-prompts/R1-technical.md`（只認官方來源、版本釘死）＋ `writing-style.md`（mock 數字標「示意」、誠實性） |
| G2 | **讀者無法真的跟著動手**：整章用 `customers` 表，卻沒給「怎麼建表造 10 萬列假資料」的可複製 snippet | 自審 #8 自承「沒有交代樣本資料怎麼來……一本自學手冊的命脈就在『跟得動』」 | `checklists/reader-gotchas.md`（給可操作的橋）＋ optional `F-follower.md`（動手實作探針會立刻撞到這個缺口） |
| G3 | **章體例不完整**：有「本章前提」，但**無本章目錄、無刻意 capstone 貫穿範例、無取捨、無一句話帶走、無上下章導覽 footer、無 📚 來源 footer** | 草稿只有「本章前提 + 1.1 + 1.2 + 結尾鉤子」；缺上述固定結構件 | `checklists/chapter-template.md`（固定體例骨架） |
| G4 | **無結構化 persona**：從 prompt 推了讀者，但沒有 floor/stretch、job-to-be-done、工具/介面、反例的結構化 persona spec；深度水位靠直覺 | 草稿「本章前提」段是散文式讀者描述，非可注入審稿者的 persona spec | `persona-elicitation.md`（六維錨點 + persona spec 模板） |
| G5 | **只有單次自審，無多角色 triage**：自審雖誠實，但是單一視角；沒有技術/初階讀者/進階讀者/架構/教學法分流，也沒有「真缺陷/可加強/刻意取捨」三級 triage | 交付僅含一段「我的自審」 | 5 審稿角色 `reviewer-prompts/*` ＋ SKILL.md 三級 triage |
| G6 | **跨節一致性張力未解**：1.1 說「聰明規劃器先圈小範圍」，1.2 卻示範老實全表掃，新手會困惑「到底聰不聰明」 | 自審 #4 自承「兩節之間有個沒講破的張力……容易自相矛盾的觀感」 | `reviewer-prompts/C-architecture.md`（跨章/節一致性）＋ capstone 貫穿範例參考點一致 |
| G7 | **無品質關卡意識**：單章草稿看不到錨點/連結/標點的全書級紀律（成書時才致命） | 交付無任何錨點/連結/標點檢查痕跡 | `scripts/anchor_check.py`、`scripts/punctuation_audit.py` ＋ SKILL.md Quality Gates |

## 結論

7 條缺口、條條有佐證，覆蓋 skill 的每一類元件（腳本、persona、體例、審稿角色、guardrails）。GREEN（Task 11）將以**同一題**重跑、逐條核對是否補上。額外收穫：G 的污染現象本身要回寫成 skill 的 testing/guardrail 註記。
