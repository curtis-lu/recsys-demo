---
name: writing-technical-handbooks
description: Use when authoring or revising a multi-chapter technical self-study handbook or guide for a defined reader — starting a new handbook, running a feedback-driven revision round, or reviewing chapters for reader comprehension and technical correctness. Not for RFCs, design docs, API references, or single how-tos.
disable-model-invocation: true
---

# Writing Technical Handbooks

## Overview

固化多章節技術自學手冊的撰寫方法：**讀者 persona 驅動深度、每章固定體例、多角色審稿、可執行品質關卡**。手冊體裁專用（教學型多章節給人自學），主題不限。

## When to use / not

- **用**：從零寫一本技術自學手冊、對既有手冊跑一輪回饋修訂、或審查章節的讀懂度與技術正確性。
- **不用**：RFC、設計文件、API 參考、runbook、單篇 how-to（這些沒有 persona／體例／教學審稿的需求）。

## 軟依賴

複用 `superpowers:brainstorming`（需求釐清）、`superpowers:dispatching-parallel-agents`（批次審稿／機械修訂）。缺則相關步驟退化為文字指引。規劃／審稿／品質關卡是本 skill 自帶，**不**走 subagent-driven-development 的碼導向框。

## 兩個入口

- **從零寫**：需求鎖定（persona，見 `references/persona-elicitation.md`）→ 分章計畫（每章 Step A–F：查證→寫草稿→派審稿→triage→使用者審→commit）→ 逐章寫＋審。
- **回饋修訂輪**：intake 回饋 → 盤點（grep 定位每條＋找連帶實例）→ 分階段計畫（每階段一個 commit）→ 逐階段過品質關卡。

兩入口後段共用：審稿角色 → **三級 triage**（真缺陷必補／可加強斟酌／誤讀或刻意取捨不改）→ 品質關卡 → commit。

## 四個使用者關卡（gated）

① 需求鎖定後出 spec、確認方向　② 計畫過目　③ 每章／階段 triage 拍板　④ 風格大改**先做一章樣本＋分級選項**（剛好／太兇／再狠），設計大改先討論→呈現 mockup→才動手。關卡之間用 subagent 自動跑，每個 subagent 留即時 live log 供同步審核。

## 每章體例

見 `references/checklists/chapter-template.md`：本章前提 → 目錄 → 內文 → capstone 貫穿範例 → 取捨 → 一句話帶走 → 上下章導覽 → 📚 來源 footer。首頁／概念地圖設計見 `references/landing-page-recipe.md`。

## 審稿角色（`references/reviewer-prompts/`）

5 必備 ＋ 1 optional，各是填好的具體範例；把 persona spec 注入後派：

- **R1** 技術（只認官方來源）／ **R2** 初階讀者（讀懂度）／ **R3** 進階讀者（深度）／ **C** 架構一致性 ／ **P** 教學法
- **F** Follower（optional）：只照手冊文字動手實作、回報脈絡缺口；逐章、被環境閘住（gating rubric 見該檔）。

定稿前對照 `references/checklists/reader-gotchas.md`、`references/checklists/reviewer-checklist.md` 自審。

## 品質關卡（`references/scripts/`，每輪改完都跑）

- `python3 anchor_check.py <dir>`：slugger 錨點驗證 ＋ 相對連結健檢（0 斷才算過）。
- `git diff | python3 punctuation_audit.py`：證明「只動標點、沒改字」（剝標點後 ±行相等）。
- 加 C 角色複核章序／footer／導覽／交叉引用一致。

## 寫作風格 / 語言

散文原則見 `references/writing-style.md`（簡潔但描述性、宣稱／新名詞要脈絡、抽象落地、誠實結論、不洩漏鷹架）。繁中專屬慣例（術語沒慣用譯法用原文、標點集）見 `references/language-conventions.md`。

## Guardrails

- **技術只認官方來源**；mock 數字標「示意」；區分「官方逐字 vs 機制合理推論」。
- **不洩漏寫作鷹架**：不對讀者講「本節暫緩／刻意寫短」，不用代號代稱概念。
- **環境約束放 persona，不寫成手冊普世規則**（否則是脈絡洩漏，R1 會抓）。
- **subagent 苦工無 WebFetch 才穩**（含 WebFetch 的長 subagent 會 socket death）；窄範圍 dispatch ＋ 即時 live log。
- **審稿後加料一定補審**（narrow scope 補審即可）。
- **跑 baseline／對照測試要意識到 subagent 會繼承專案 ambient 方法論**（CLAUDE.md／memory）；真乾淨的 baseline 需在無此脈絡處跑、或明令「忽略任何既有手冊指引」。
- 機械重排（章號／檔名）用**單次掃描** `re.sub` ＋ grep 驗證 ＋ C 複核。
