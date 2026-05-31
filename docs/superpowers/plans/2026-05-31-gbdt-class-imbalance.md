# GBDT 類別不平衡手冊 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 撰寫 `gbdt_class_imbalance.md` —— 一份延續主手冊風格、純通用的「GBDT 二元分類類別不平衡」數學影響與處理手冊。

**Architecture:** 兩部式（診斷 Ch1–8 / 處方 Ch9–15），逐章追蹤不平衡如何作用在主手冊已建立的數學量上；單一 markdown 檔，導覽/記號/體例對齊主手冊；HTML 鏡像為定稿後的獨立步驟（本計畫不含）。

**Tech Stack:** Markdown + `$...$`/`$$...$$` 數學（KaTeX-相容）、`<a id>` anchor 導覽。內容規格與已驗算數值見 spec：`docs/superpowers/specs/2026-05-31-gbdt-class-imbalance-design.md`。

**注意（此為散文任務，非程式）：**
- 「測試」= (a) 手算數值複算一致、(b) anchor/nav/目錄連結自洽、(c) 定稿後 reader-subagent 通讀挑卡點。無 pytest。
- 內容不在本計畫重抄；每章「要寫什麼」以 spec 對應章節為準（本計畫只鎖定**結構、順序、anchor/nav 字串、必含公式與數值、commit 點、驗證**）。
- 風格一致性靠單一作者連續完成 → 建議 **inline 執行**（非每章換 subagent，以免語氣分裂）；subagent 留給 QA。
- 全程在 `docs/gbdt-handbook` 分支，無 worktree。

**全域不變量（每章都要守）：**
- 繁體中文；數學一律 `$`/`$$`。
- 記號沿用 spec §4；新記號（$w, \tau, C_{FP}, C_{FN}$）首次出現要定義。
- 無 recsys_tfb 專案引用；無「top-K / 排序」多標的語言。
- 每章結尾 `<nav>` 上一章/下一章；每章標題前 `<a id="chN">`。
- anchor 方案：`top`、`toc`、`ch1`…`ch15`。

---

## Task 1: 文件骨架 + 主手冊回連

**Files:**
- Create: `gbdt_class_imbalance.md`
- Modify: `gbdt_binary_classification.md`（目錄區加一行指向本文）

- [ ] **Step 1: 建立檔案骨架**
  寫入：`<a id="top"></a>` → 標題（暫定《二元分類 GBDT：類別不平衡的數學影響與處理手冊》）→ 引言（spec §2 核心框架：不平衡 ≠ 機率錯，真正咬在幾個具體位置）→ `<a id="toc"></a>` + 目錄（15 章 + 兩部分隔，全部 `[...](#chN)` 連結）→ 一句導覽說明 → 「符號接續」小節（重述 spec §4 記號清單）→ 一句連回主手冊（相對連結 `gbdt_binary_classification.md`）。先放好 `ch1`…`ch15` 全部 `<a id>` 佔位 + 章標題 + 空 `<nav>`，後續任務填內容。

- [ ] **Step 2: 主手冊加回連**
  在 `gbdt_binary_classification.md` 目錄區（附錄 A 那行之後）加一行指向本文，例如：`- [延伸：類別不平衡的數學影響與處理](gbdt_class_imbalance.md)`。

- [ ] **Step 3: 驗證**
  `grep -nE '<a id="(top|toc|ch[0-9]+)"' gbdt_class_imbalance.md` → 應見 top、toc、ch1…ch15 共 17 個 anchor。確認目錄 15 條連結 target 與 anchor 一一對應。

- [ ] **Step 4: Commit**
  ```bash
  git add gbdt_class_imbalance.md gbdt_binary_classification.md
  git commit -m "docs(imbalance): 文件骨架、目錄、符號接續 + 主手冊回連"
  ```

---

## Task 2: 第一部 Ch 1–5（逐機制診斷 + 手算範例）

**Files:** Modify `gbdt_class_imbalance.md`（填 ch1–ch5）

- [ ] **Step 1: Ch 1 起點 $F_0$**（spec 第一部 Ch1）
  必含：$F_0=\log\frac{\bar y}{1-\bar y}$；$p_0=\bar y$；推過 $p=0.5$ 要抬升 $|F_0|$；「全預測成負類」是門檻假象之澄清；數值 $\bar y=1\%\Rightarrow F_0\approx-4.595$。呼應主手冊 Ch3。

- [ ] **Step 2: Ch 2 梯度/海森階級失衡**（spec Ch2）
  必含：$g_i=p_i-y_i,\ h_i=p_i(1-p_i)$；起點處負類 $g=\bar y$、正類 $g=\bar y-1$、兩類 $h=\bar y(1-\bar y)$；$G=\sum g_i$ 拔河、初始 $\sum g_i=0$（呼應 Ch3 一階條件）；$\bar y$ 小 → $H$ 小（預告 Ch3 分母）。呼應主手冊 Ch4/5.3。

- [ ] **Step 3: Ch 3 Gain 與「稀有區飢餓」**（spec Ch3，★ 核心；數值見 spec，須可複算）
  20 樣本（18 負+2 正，$p=0.01$）：$G=-1.80,\ H=0.198$；孤立 2 正左葉 $G_L=-1.98,H_L=0.0198,G_R=0.18,H_R=0.1782$。
  $\text{Gain}(\lambda{=}0)\approx90.9$；$\text{Gain}(\lambda{=}1)\approx0.583$（約 156× 縮減）。點出 `min_child_weight`/`min_sum_hessian_in_leaf`/`min_data_in_leaf` 會直接禁止小葉切點。誠實框架：正則化是對的，但系統性不利少數類；伏筆 Ch11 同範例救回。呼應主手冊 Ch5.3。

- [ ] **Step 4: Ch 4 葉輸出 $\gamma$ 收縮**（spec Ch4）
  必含：$\gamma=-G/(H+\lambda)$ 無偏；小樣本葉 $H$ 小 → $\lambda$ 壓 $\gamma$；用 Ch3 左葉數字示範 $\lambda=0$ vs $\lambda=1$ 的 $\gamma$。呼應主手冊 Ch6。

- [ ] **Step 5: Ch 5 門檻 0.5 錯位**（spec Ch5）
  必含：$p\ge0.5\iff F\ge0\iff$ odds $\ge1{:}1$ vs base odds $\bar y/(1-\bar y)$；一般化 $\tau$ on $p\iff F\ge\operatorname{logit}(\tau)$；預告 Ch10 成本式門檻。呼應主手冊 Ch1.4。

- [ ] **Step 6: 驗證（複算）**
  以計算器/python 複算 Ch3 兩個 Gain（$90.9$、$0.583$）與 Ch4 的 $\gamma$，數字需與 spec 一致。檢查 ch1–ch5 的 `<nav>` 上一章/下一章正確。

- [ ] **Step 7: Commit**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 第一部 Ch1-5 逐機制診斷 + 手算範例"
  ```

---

## Task 3: 第一部 Ch 6–8（光譜 + 指標簡述 + 小結表）

**Files:** Modify `gbdt_class_imbalance.md`（填 ch6–ch8）

- [ ] **Step 1: Ch 6 不平衡程度光譜**（spec Ch6，★ 新增綜合章；含對照表）
  對照表三欄 $\bar y=1\%/0.1\%/0.01\%$：$F_0=-4.595/-6.907/-9.210$；爬升 $\approx4.6/6.9/9.2$；100 萬中正類 $10000/1000/100$；$\text{scale\_pos\_weight}\approx99/999/9999$；後果欄。文字解讀：$|F_0|$ 對數成長、正類絕對數驟減、補償 $w$ 在極端時方差爆炸（伏筆第二部）。

- [ ] **Step 2: Ch 7 評估與早停陷阱（簡述）**（spec Ch7，刻意精簡，一節短講）
  accuracy 灌水（$\bar y=1\%$ 全負類 99%）；ROC-AUC 對 base rate 不敏感；早停看 val log-loss 被多數類主導。點到為止，「該用什麼指標」留第二部。

- [ ] **Step 3: Ch 8 小結對照表**（spec Ch8）
  表欄：｜不平衡咬在哪｜對應數學量｜症狀｜對應處方（連到第二部章節）｜，收束 Ch1–7。

- [ ] **Step 4: 驗證**
  複算 Ch6 三個 $F_0$ 值與 `scale_pos_weight`。檢查 ch6–ch8 nav、Ch8 表內處方連結 target 存在。

- [ ] **Step 5: Commit**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 第一部 Ch6-8 光譜 + 指標簡述 + 小結表"
  ```

---

## Task 4: 第二部 Ch 9–10（問對問題 + 調門檻）

**Files:** Modify `gbdt_class_imbalance.md`（填 ch9–ch10）

- [ ] **Step 1: Ch 9 先問對問題**（spec Ch9）
  (a) 要可信機率值（餵成本/期望計算）vs (b) 要操作點上的好決策（選 $\tau$，只在意排序+切點）。說明此分類如何決定後面哪些招有用、並吸收校準顧慮（(b) 不在意絕對值；(a) 才需 Ch11/12 的機率警語）。

- [ ] **Step 2: Ch 10 調門檻**（spec Ch10，接回 Ch5）
  成本式 $\tau^*=\frac{C_{FP}}{C_{FP}+C_{FN}}$，附簡短推導（比較 $(1-p)C_{FP}$ 與 $p\,C_{FN}$）；無成本時驗證集掃 $\tau$ 最大化 $F_\beta$/達標 precision/recall；強調不改模型、最該先試。

- [ ] **Step 3: 驗證**
  檢查 $C_{FP},C_{FN},\tau$ 首次出現有定義；門檻推導自洽；ch9–ch10 nav。

- [ ] **Step 4: Commit**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 第二部 Ch9-10 問對問題 + 調門檻"
  ```

---

## Task 5: 第二部 Ch 11–12（權重 + 重採樣，深入 ★ 重心）

**Files:** Modify `gbdt_class_imbalance.md`（填 ch11–ch12）

- [ ] **Step 1: Ch 11 類別權重 / `scale_pos_weight`**（spec Ch11，數值須可複算）
  機制：正類 $g_i,h_i\times w$ → $G,H$ 變 → Gain/$\gamma$ 變。重用 Ch3 範例（$w=9$）：正類 $g=-8.91,h=0.0891$；$G=-17.64,H=0.3564$；左葉 $G_L=-17.82,H_L=0.1782$；$\text{Gain}(\lambda{=}1)\approx20.1$（對比 Ch3 的 $0.583$）。$w=n_{neg}/n_{pos}$ 選法。LightGBM `scale_pos_weight`/`is_unbalance`（互斥）/`sample_weight` 差異。與 $\lambda$/`min_sum_hessian` 交互。機率警語一段 + $p_{\text{true}}=\frac{p_w}{p_w+(1-p_w)w}$。

- [ ] **Step 2: Ch 12 重採樣**（spec Ch12，深入）
  欠採：改 $\bar y$→$F_0$ 平移、丟資料代價、prior-shift 還原 $F_{\text{true}}=F_{\text{sampled}}+\log\frac{\bar y_t/(1-\bar y_t)}{\bar y_s/(1-\bar y_s)}$（平衡時 $\bar y_s=0.5$，平移=真實 $F_0$）。過採（複製）：**證明複製=整數權重**（$k$ 份 → $kg_i,kh_i$）；與權重在 `min_data_in_leaf`（算 $k$ 筆）vs `min_sum_hessian`（同）的差異；過擬合風險。SMOTE 在 tree/tabular 的侷限。三法對照表（梯度總和/base rate/變異數/計算成本/何時用）。

- [ ] **Step 3: 驗證（複算）**
  複算 Ch11 加權 Gain $\approx20.1$，確認與 Ch3 共用同一組數字、口徑一致。prior-shift 在 $\bar y_s=0.5$ 退化為真實 $F_0$ 之敘述正確。檢查 ch11–ch12 nav、三法表。

- [ ] **Step 4: Commit**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 第二部 Ch11-12 權重 + 重採樣（深入）"
  ```

---

## Task 6: 第二部 Ch 13–15 + 結尾

**Files:** Modify `gbdt_class_imbalance.md`（填 ch13–ch15 + 結尾）

- [ ] **Step 1: Ch 13 自訂 loss（精簡）**（spec Ch13）
  focal/weighted log-loss 改 $g_i,h_i$ → 改 Gain/$\gamma$；focal 用 $(1-p_t)^\gamma$ 調降易樣本；實作注意：自備一/二階導數、focal 二階可能變號需處理、超參數與校準影響。作為進階選項，篇幅精簡。

- [ ] **Step 2: Ch 14 選招決策流程**（spec Ch14）
  「可信機率值 vs 操作點決策」×「成本是否已知」兩軸，文字版決策樹給「先試什麼、再試什麼」。

- [ ] **Step 3: Ch 15 常被忽略的事實 + 結尾**（spec Ch15）
  只需操作點決策 + 調門檻 + 用對指標 → 原生 GBDT 常不需重採樣/加權且機率還校準；多數「處理不平衡」是拿校準換一個本可用門檻解掉的問題。收尾呼應核心框架 + 一句話總結 + 連回主手冊。

- [ ] **Step 4: 驗證**
  ch13–ch15 nav（Ch15 只有「上一章」）；結尾回連主手冊存在。

- [ ] **Step 5: Commit**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 第二部 Ch13-15 自訂loss/決策流程/收尾"
  ```

---

## Task 7: 全文一致性巡檢

**Files:** Modify `gbdt_class_imbalance.md`（修正發現的問題）

- [ ] **Step 1: 導覽自洽**
  `grep -nE '<a id=|#ch[0-9]+|<nav' gbdt_class_imbalance.md`：確認 17 anchor、目錄 15 連結、每章 nav 上一章/下一章鏈正確（Ch1 只有下一章、Ch15 只有上一章）。

- [ ] **Step 2: 數值/記號終檢**
  複算所有手算數值（Ch3 $90.9$/$0.583$、Ch6 三 $F_0$、Ch11 $20.1$）；確認所有新記號（$w,\tau,C_{FP},C_{FN},\bar y_s,\bar y_t$）首次出現有定義；無 simplified-Chinese 字。

- [ ] **Step 3: 範疇終檢（對照 spec §3/§8 驗收）**
  無 recsys_tfb 引用、無「top-K」語言、無獨立校準章節/Platt/isotonic（只警語+prior-shift）、雙向交叉連結就位。

- [ ] **Step 4: Commit（如有修正）**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 全文一致性巡檢修正"
  ```

---

## Task 8: Reader-subagent QA + 修訂

**Files:** Modify `gbdt_class_imbalance.md`（依回報修訂）

- [ ] **Step 1: 派 reader subagent**
  用 Explore/general-purpose subagent，persona 用 spec §9（工程讀者，會微積分/線代/logistic regression/sigmoid，**已讀完主手冊**、熟悉 $F_0/g_i/h_i$/Gain/$\gamma$/Newton step）。任務：通讀 `gbdt_class_imbalance.md` 逐段標卡關處（哪句/哪式、原因分類、期待補什麼），不客氣、不假裝懂、不猜。

- [ ] **Step 2: 分類回報並修訂**
  把卡點分「真缺陷（補）/ 可加強（斟酌）/ 誤讀（不改或微調）」，逐項處理。

- [ ] **Step 3: Commit**
  ```bash
  git add gbdt_class_imbalance.md
  git commit -m "docs(imbalance): 依 reader QA 回報修訂"
  ```

- [ ] **Step 4: 回報使用者**
  總結完成狀態，並提醒 HTML 鏡像為後續獨立步驟（待使用者決定是否進行）。

---

## Self-Review（對照 spec）

**Spec coverage：** spec 第一部 Ch1–8 → Task 2(Ch1–5)/Task 3(Ch6–8)；第二部 Ch9–15 → Task 4(9–10)/Task 5(11–12)/Task 6(13–15)；開頭區+主手冊回連 → Task 1；交叉連結 → Task 1+Task 6；QA 計畫 §9 → Task 8；驗收 §8 → Task 7。HTML 鏡像 spec §7 明列為後續步驟，不在本計畫（Task 8 Step 4 提醒）。無遺漏。

**Placeholder scan：** 各任務以「必含公式 + 已驗算數值 + anchor/nav 字串」鎖定，內容散文以 spec 對應章節為準（散文任務刻意不重抄全文，避免雙寫）；無 TBD/TODO。

**一致性：** anchor 方案 `top/toc/ch1..ch15` 全任務一致；Ch3 與 Ch11 明確標示共用同一組 20 樣本數字（$0.583\to20.1$）；記號清單跨任務統一。

**HTML 例外說明：** 依 spec §3「不在草稿期維護 HTML」，本計畫只產 markdown；HTML 鏡像為定稿後獨立任務。
