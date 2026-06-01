# 手冊3《多 item 共享模型下的冷熱門不平衡》撰寫實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 寫出手冊3 的 `gbdt_multiitem_imbalance.md` 正文與其 KaTeX 離線 HTML，聚焦「單一共享 GBDT 模型服務多個冷熱門懸殊 item」時，冷熱門差異在訓練數學裡的行為與共享模型框架內的處方。

**Architecture:** 自學向數學手冊，繁體中文，行內 `$...$`／獨立式 `$$...$$`。沿用手冊1/2 的符號與兩部（診斷→處方）結構。內容、符號、貫穿範例數字、文獻錨點全部來自已審定的 spec：`docs/superpowers/specs/2026-06-01-gbdt-multiitem-imbalance-design.md`（**每個寫作任務都先回讀對應 spec 章節**）。離線 HTML 由 `/tmp/build_imbalance_html.py` 改作的 `build_multiitem_html.py` 產生（複用既有 HTML shell + math 保護 + python-markdown）。

**Tech Stack:** Markdown（python-markdown：tables / fenced_code / sane_lists）、KaTeX（已內嵌於既有 offline HTML shell）、worktree venv python（`/Users/curtislu/projects/recsys_tfb/.venv/bin/python`，3.10.9，含 `markdown`）。

**寫作鐵律（每章適用，違反即返工）：** 遵 `docs/handbook-writing-guide.md`。具體數字落地、結論誠實（不誇稱共享模型能根治冷門）、不洩漏鷹架（章名不放備註、不用代號代稱）、一個貫穿範例前後呼應、符號紀律（item 下標 $j$、不一符二義）、**純繁體中文不可出現簡體**。

**工作目錄：** 一律 `/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-multiitem/`（分支 `docs/gbdt-multiitem-imbalance`）。所有路徑用此絕對前綴（worktree path footgun：別寫成 main repo 路徑）。git 用 `git -C <該絕對路徑>`。commit 時 graphify hook 用 `-c core.hooksPath=/dev/null` 關掉。

---

## File Structure

- Create: `/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-multiitem/gbdt_multiitem_imbalance.md` — 手冊正文（單一檔，11 章 + 前言 + TOC + 符號表）。
- Create: `/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-multiitem/build_multiitem_html.py` — HTML build script（**不 commit**，比照手冊1/2 的 `/tmp/build_imbalance_html.py` 慣例）。
- Create (committed): `/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-multiitem/gbdt_multiitem_imbalance_offline.html` — build 產物。
- Reference only（已存在於 worktree，勿改）：`gbdt_binary_classification.md`、`gbdt_class_imbalance.md`、`gbdt_binary_classification_offline.html`（HTML shell 來源）、`docs/handbook-writing-guide.md`、spec 檔。

**章節錨點命名：** `#top`、`#toc`、`#ch1`…`#ch11`。每章結尾放 `[← Ch N](#chN) ｜ [Ch N+2 →](#chN+2)` 樣式 nav（比照手冊2）。

---

## Task 1：骨架（前言 + 目錄 + 符號表）

**Files:** Create `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §1–§4** 取目標、範疇、排除/後續跟進清單、新增符號表。
- [ ] **Step 2：寫檔頭與前言**
  - `<a id="top"></a>` + `# 二元分類 GBDT：多 item 共享模型下的冷熱門不平衡`
  - 開頭一段：接續手冊1（機制）、手冊2（單一目標不平衡），連結用相對路徑 `[主手冊](gbdt_binary_classification.md)`、`[手冊2](gbdt_class_imbalance.md)`。
  - 一句話問題陳述（spec §1.1：一個模型同時服務冷熱門懸殊的 item 時，訓練數學改變了什麼）。
  - 範疇框（spec §1.2）+ 明確排除/後續跟進框（spec §1.3：per-item 模型、LTR、NDCG/mAP-vs-logloss、排序評估、校準絕對值 → 手冊4）。
  - 一句話標明建模佈局假設（完整候選等列數，冷熱門差在正類率；負採樣影響留 Ch10）。
- [ ] **Step 3：寫目錄**（兩部結構，比照手冊2 的 `## 目錄` + 兩個 `<strong>第N部</strong>` + `<ol>`／`<ol start="9">` 風格純 markdown，HTML build 再轉 nav）。第一部 診斷 Ch1–7、第二部 處方 Ch8–11。
- [ ] **Step 4：寫「符號接續」表**（spec §4）：$j$、$n_j$、$P_j$、$\bar y_j$、$\bar y_{\text{global}}$、$F_{0,j}$、$G_j,H_j$；並一句話說明沿用手冊1/2 既有符號（$F,p,\bar y,F_0,g_i,h_i,G,H,\gamma,\text{Gain},\nu,\lambda$）不重定義。
- [ ] **Step 5：驗證**：grep 確認 TOC 內每個 `#chN` 之後在正文都會有對應 `<a id="chN">`（此時正文尚未寫，先確認 TOC 列了 ch1–ch11）；確認無簡體字（`python -c` 掃常見簡體或人工通讀）。
- [ ] **Step 6：commit**（worktree，關 hook）：`docs(handbook): scaffold 手冊3 前言+目錄+符號表`

---

## Task 2：Ch1 從 1 個 item 到 M 個（混合體）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch1。**
- [ ] **Step 2：寫 Ch1**：`<a id="ch1"></a>` + 標題「Ch 1. 從 1 個 item 到 M 個：訓練集變成『混合體』」。
  - 推導 $\bar y_{\text{global}} = \sum_j P_j / \sum_j n_j$ 是各 item base rate 以列數為權重的加權平均；等列數佈局下分子被熱門 $P_j$ 主導。
  - 一段「全域 base rate 反映熱門 item 購買傾向，冷門訊號在全域統計幾乎不可見」。
  - 術語對接：**流行度偏差（popularity bias）/ 長尾（long-tail）**，引 Abdollahpouri et al. 2017。
  - 標明佈局假設（完整候選等列數；負採樣影響 → Ch10）。
- [ ] **Step 3：驗證**：手算式自洽（加權平均定義）；nav 連結正確。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch1 混合體 base rate`

---

## Task 3：Ch2 全域 $F_0$ vs per-item $F_{0,j}$（起點便宜）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch2。**
- [ ] **Step 2：寫 Ch2**：標題「Ch 2. 全域 $F_0$ vs per-item $F_{0,j}$：起點錯位，但這段便宜」。
  - $F_0=\log\frac{\bar y_{\text{global}}}{1-\bar y_{\text{global}}}$ 單一全域常數；$F_{0,j}$ 各 item 不同。冷門從 $F_0$ 出發被系統性高估、熱門被低估。
  - **誠實關鍵**：起點錯位便宜——一刀 item split 就讓各 item 落到自己 base rate（高 Gain，預告 Ch7 範例 ① $\approx5.45$）。**per-item 先驗不是深層問題**，深層問題留 Ch3–5。
  - **文獻錨點**：focal loss（Lin et al. 2017，arXiv:1708.02002）做同一區分——先驗初始化 $\pi$ 穩定訓練但不解決不平衡，真正的解是讓大量易分樣本不再主導梯度。採用「易分樣本／梯度被主導」用語。
  - 與手冊2 Ch1 對照。
- [ ] **Step 3：驗證**：$F_0,F_{0,H},F_{0,C}$ 的具體值若此章先給，須與 Ch7（$-2.066/-1.386/-3.664$）一致；focal 引用正確。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch2 先驗便宜 + focal 錨點`

---

## Task 4：Ch3 跨 item 的梯度質量

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch3。**
- [ ] **Step 2：寫 Ch3**：標題「Ch 3. 跨 item 的梯度質量：熱門 item 擁有大部分『正類訊號』」。
  - $G=\sum_j G_j$；起點處正類出 $g=\bar y_{\text{global}}-1$、負類出 $g=\bar y_{\text{global}}$。正類梯度質量總量 $\approx(\sum_j P_j)|\bar y_{\text{global}}-1|$ 被熱門 $P_j$ 主導。
  - 推論：非 item-specific 的共享客戶特徵切點，Gain 由熱門正類質量決定 → 切點服務熱門買家輪廓，冷門沿用。
  - **條件性 caveat**：熱門主導是**相對**的；共享切點的 Gain 只有在「該特徵真能把熱門正類從熱門負類分開」時才**絕對**夠大，否則樹寧可選 item-isolation 切點。不過度宣稱共享切點總是被選中。
  - 與手冊2 Ch2 對照。
- [ ] **Step 3：驗證**：梯度式與手冊2 一致；caveat 在場。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch3 梯度質量被熱門主導`

---

## Task 5：Ch4 葉預算的競爭（核心洞見）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch4。**
- [ ] **Step 2：寫 Ch4**：標題「Ch 4. 葉預算的競爭：冷門 item 的個人化切點被餓死」。
  - 個人化 = 在已隔出冷門 item 的子區域用客戶特徵切出冷門少數正類。
  - 這是手冊2 Ch3「稀有區飢餓」的多 item 版，但**雙重稀少**：冷門正類絕對筆數少 → 左葉 $G_L,H_L$ 小 → $\lambda$／`min_data_in_leaf`／`min_sum_hessian_in_leaf` 一夾就塌或被禁。
  - **新增維度**：樹全域挑最大 Gain。冷門個人化 Gain（$\approx2.97$）遠輸熱門個人化（$\approx39.6$，差 ~13×），`num_leaves` 有限下葉預算被熱門搶走。明標：這是手冊3 核心洞見。
- [ ] **Step 3：驗證**：2.97 / 39.6 / 13× 與 Ch7 表一致（此章只引用、Ch7 才完整算）。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch4 葉預算競爭(核心洞見)`

---

## Task 6：Ch5 雙重懲罰（+ 對照表）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch5。**
- [ ] **Step 2：寫 Ch5**：標題「Ch 5. 雙重懲罰」。
  - 收束 Ch3–4：冷門同時吃兩記——(1) 跨 item 搶不到葉預算；(2) item 內部仍是手冊2 飢餓（遞迴）。
  - 一張對照表：單一目標不平衡（手冊2）vs 多 item 冷熱門（手冊3）各咬哪個量（$F_0$／$G,H$／Gain／葉預算）。
  - 再引一次 focal loss 的「梯度被主導」框架收束。
- [ ] **Step 3：驗證**：對照表兩欄對齊、無新未定義符號。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch5 雙重懲罰 + 對照表`

---

## Task 7：Ch6 共享葉子的跨 item 負遷移（條件性短節）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch6（注意：刻意寫短、明白 hedge）。**
- [ ] **Step 2：寫 Ch6（短）**：標題「Ch 6. 共享葉子的跨 item 負遷移」。
  - 機制：樹先切客戶特徵 → 葉子混入冷熱門 → $\gamma=-G_{\text{leaf}}/(H_{\text{leaf}}+\lambda)$ 被熱門主導 → 冷門被套熱門輪廓 = **負遷移（negative transfer）**，可正可負。
  - **為何次要（hedge）**：LightGBM 原生類別切分依 $\sum g/\sum h$ 排序後切（Fisher-style），item 是依 Gain 競爭的單一候選欄（22 ≪ ~1000 cardinality）；因 item-isolation Gain 高（Ch2 ①），leaf-wise 常早早切出主要 item → 共處一葉前提常不成立。引 LightGBM Features 文件。
  - 跨 item 分數可比性一句帶過；機率校準絕對值沿手冊2 不展開。
- [ ] **Step 3：驗證**：篇幅明顯短於前幾章；語氣 hedge（不寫「校準全面崩壞」）；「負遷移」術語在場。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch6 跨 item 負遷移(條件性短節)`

---

## Task 8：Ch7 貫穿範例（診斷側，數學最重）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch7（數字已預先驗算、subagent 已二驗）。**
- [ ] **Step 2：寫 Ch7**：標題「Ch 7. 貫穿範例：熱門+冷門兩 item 手算」。完整呈現：
  - 設定：H（$n{=}80,P{=}16,\bar y_H{=}0.20$）、C（$n{=}80,P{=}2,\bar y_C{=}0.025$）、全體 $N{=}160$、18 正、$\bar y_{\text{global}}{=}0.1125$。
  - 起點（$p{=}0.1125$，$h{=}0.09984$）：$F_0{=}{-}2.066$、$F_{0,H}{=}{-}1.386$、$F_{0,C}{=}{-}3.664$；$G_H{=}{-}7,H_H{=}7.987$；$G_C{=}{+}7,H_C{=}7.987$；全體 $G{=}0$（$\sum g_i{=}0$ 一階條件）。
  - 三切點 Gain 對照表（$\lambda{=}0$／$\lambda{=}1$）：① item-isolation $6.13/5.45$；② 熱門個人化（左 $G{-}14.2,H1.597$／右 $G7.2,H6.39$）$\lambda{=}1{:}\,39.6$；③ 冷門個人化（左 $G{-}1.775,H0.1997$／右 $G8.775,H7.788$）$\lambda{=}1{:}\,2.97$。
  - 三段解讀：① 先驗便宜（印證 Ch2）；②≫③ 差 ~13×（印證 Ch4）；③ 左葉僅 2 列 $H_L{=}0.1997$ 一碰 min-child 就被禁（雙重稀少）。
  - 放大到真實量級：玩具 8:1 → 真實 400:1 方向性放大；**繪稿護欄**：示範 400:1 用 $n\sim10^6$（$P_{\text{hot}}40000,P_{\text{cold}}100$），不可硬縮放 $n{=}80$（會 $\bar y{>}1$）。
- [ ] **Step 3：驗證（關鍵）**：用 venv python 獨立重算全部數字，逐一比對：
  ```bash
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
  import math
  yb=18/160; F0=math.log(yb/(1-yb)); h=yb*(1-yb)
  print("F0",round(F0,3), "FH",round(math.log(.2/.8),3), "FC",round(math.log(.025/.975),3), "h",round(h,5))
  gpos=yb-1; gneg=yb
  GH=16*gpos+64*gneg; GC=2*gpos+78*gneg; HH=80*h
  print("GH",round(GH,3),"GC",round(GC,3),"HH",round(HH,3))
  def gain(GL,HL,GR,HR,lam):
      G,H=GL+GR,HL+HR
      return .5*(GL**2/(HL+lam)+GR**2/(HR+lam)-G**2/(H+lam))
  print("① l0",round(gain(GC,HH, -GC,HH,0),2),"l1",round(gain(GC,HH,-GC,HH,1),2))
  print("② l1",round(gain(16*gpos,16*h, 64*gneg,64*h,1),2))
  print("③ l1",round(gain(2*gpos,2*h, 78*gneg,78*h,1),2))
  PY
  ```
  期望：F0 -2.066 / FH -1.386 / FC -3.664 / h 0.09984；GH -7.0 / GC 7.0 / HH 7.987；① 6.13 / 5.45；② 39.6；③ 2.97。任一不符即修正正文數字。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch7 貫穿範例(三切點 Gain 手算)`

---

## Task 9：Ch8 item-aware sample weight（主處方，含加權重算）

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch8。**
- [ ] **Step 2：寫 Ch8**：標題「Ch 8. item-aware sample weight」。
  - 機制：把冷門 item 列的 $g_i,h_i$ 乘 $w_j$（隨 $\bar y_j$ 反向／正比 $1/P_j$）→ 復活 $G_j,H_j$ 與冷門個人化 Gain。
  - **加權重算（沿用 Ch7 數字，比照手冊2 Ch11 的 local 重算，p 不重解）**：取 $w_C{=}8$（使冷門正類質量 $2{\times}8{=}16$ 與熱門拉平）。冷門個人化切點③ 在 $w_C{=}8$ 下：左 $G_L{=}{-}14.2,H_L{=}1.598$；右 $G_R{=}70.2,H_R{=}62.30$；母 $G{=}56,H{=}63.90$；$\text{Gain}(\lambda{=}1)\approx53.6$ —— 從 $2.97$ 跳到 $53.6$，**反超熱門個人化的 $39.6$**，冷門 item 終於搶得到下一刀葉預算。
  - 接手冊2 Ch11：`scale_pos_weight`／class weight 的「按 item」推廣（手冊2 按類別、這裡按 item × 類別）。
  - 兩條誠實提醒：(1) 放大梯度也放大冷門少數正類**雜訊**，權重太猛過擬合；(2) 加權使輸出機率系統性偏高（仍 loss-calibrated／保序，排序無妨；絕對值校準沿手冊2 不展開）。
- [ ] **Step 3：驗證**：venv python 重算 $w_C{=}8$ 的③：
  ```bash
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
  yb=18/160; h=yb*(1-yb); gpos=yb-1; gneg=yb; w=8
  GL=w*2*gpos; HL=w*2*h; GR=w*78*gneg; HR=w*78*h; G=GL+GR; H=HL+HR
  print("GL",round(GL,2),"HL",round(HL,3),"GR",round(GR,2),"HR",round(HR,2))
  print("Gain l1", round(.5*(GL**2/(HL+1)+GR**2/(HR+1)-G**2/(H+1)),1))
  PY
  ```
  期望：GL -14.2 / HL 1.598 / GR 70.2 / HR 62.30；Gain≈53.6。不符即修正。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch8 item-aware 權重(加權重算)`

---

## Task 10：Ch9 全域正則化旋鈕的多 item 困境

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch9。**
- [ ] **Step 2：寫 Ch9**：標題「Ch 9. 全域正則化旋鈕的多 item 困境（$\lambda$ / `min_data_in_leaf` / `min_sum_hessian_in_leaf`）」。
  - 新現象：旋鈕全域、無法 per-item。
  - 兩難：放鬆讓冷門個人化切點存活 → 熱門過擬合；收緊壓住熱門 → 冷門被餓死。
  - 對照手冊2 Ch3（同旋鈕單一目標的飢餓）；點出多 item 讓單一全域設定更左右為難。
- [ ] **Step 3：驗證**：與 Ch4 的 min-child 機制一致、不重複定義符號。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch9 全域正則化旋鈕困境`

---

## Task 11：Ch10 per-item 負採樣 / 列比例再平衡

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch10 + §5 佈局結論。**
- [ ] **Step 2：寫 Ch10**：標題「Ch 10. per-item 負採樣 / 列比例再平衡」。
  - 機制：熱門負類欠採 → 壓低其在 $\sum_j n_j,\sum_j P_j$ 主導 → 共享切點不被綁架；並抬高各 item 有效 $\bar y_j$（縮小 Ch2 先驗錯位）。
  - 代價：動到 $\bar y_j$／全域 base rate → $F_0$、輸出分數偏移，需 **logQ / 抽樣機率修正**（負採樣標準校正；手冊2 Ch12 base-rate 偏移的跨 item 推廣）。
  - 與手冊2 Ch12 對照：這裡是**跨 item 列比例**，非單一 item 正負比例。
- [ ] **Step 3：驗證**：logQ 術語在場、與 §5 一致。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch10 負採樣 + logQ 修正`

---

## Task 12：Ch11 選招流程 + 收尾 + 後續跟進

**Files:** Modify `gbdt_multiitem_imbalance.md`

- [ ] **Step 1：回讀 spec §6 Ch11 + §1.3 排除清單。**
- [ ] **Step 2：寫 Ch11**：標題「Ch 11. 選招流程 + 收尾」。
  - 決策流程：確認 item 是否為特徵 → 先驗便宜先修（Ch2）→ 量冷門個人化是否被餓死 → 依序試 item-aware 權重（Ch8）／調正則化（Ch9）／列比例再平衡（Ch10）。流程用文字/清單，不用密碼式 ASCII。
  - 誠實收尾：共享模型內為**緩解非根治**；冷門 item 絕對正類筆數低到一程度，單一共享模型有上限（引 cold-start popularity-bias，Meehan & Goyal 2025）。
  - **後續跟進**段：明指手冊4 範疇——per-item 獨立模型、LTR（NDCG/mAP 作訓練目標 vs `binary_logloss`）、跨 item 排序與校準。
- [ ] **Step 3：驗證**：收尾不誇稱；後續跟進清單與 spec §1.3 一致。
- [ ] **Step 4：commit**：`docs(handbook): 手冊3 Ch11 選招流程 + 收尾`

---

## Task 13：建離線 HTML

**Files:** Create `build_multiitem_html.py`（不 commit）、`gbdt_multiitem_imbalance_offline.html`（commit）

- [ ] **Step 1：複製 build script**：把 `/tmp/build_imbalance_html.py` 複製為 worktree 的 `build_multiitem_html.py`，改三處：
  - `ROOT = pathlib.Path("/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-multiitem")`（**worktree path，不是 main**）。
  - 來源 md 改 `gbdt_multiitem_imbalance.md`、輸出改 `gbdt_multiitem_imbalance_offline.html`。
  - `<title>` 改「二元分類 GBDT：多 item 共享模型下的冷熱門不平衡」。
  - HTML shell 來源仍讀 `gbdt_binary_classification_offline.html`（含 KaTeX 內嵌，worktree 已有）。
  - 5c 的 `<ol start="9">` 調成第二部起始章號（本手冊第二部從 Ch8 起 → 改 `start="8"` 並對應 `#ch8` 錨點）。
- [ ] **Step 2：執行 build**：
  ```bash
  cd /Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-multiitem && /Users/curtislu/projects/recsys_tfb/.venv/bin/python build_multiitem_html.py
  ```
  期望 stdout：`MATHPLACEHOLDER leftover: 0`、`nav.nav` > 0、`nav.toc: 1`。
- [ ] **Step 3：驗證**：`MATHPLACEHOLDER leftover: 0`（數學保護無殘留）；grep HTML 確認 `katex` CSS/JS 已內嵌、`<ol start="8">` 存在、每個 `#chN` 錨點都在。瀏覽器抽看（可選）。
- [ ] **Step 4：commit**（只加 `.html`，build script 不加）：`docs(handbook): 手冊3 離線 HTML`

---

## Task 14：定稿 reader-subagent 通讀 QA

**Files:** Modify `gbdt_multiitem_imbalance.md`（修 QA 發現）、必要時重 build HTML

- [ ] **Step 1：派 reader-subagent**（**整篇寫完、HTML build 完才跑**），通讀 `gbdt_multiitem_imbalance.md`。使用者指定的 reviewer prompt（一字不差用這個 persona）：

  > 你是一位工程背景的讀者：
  > - 會微積分、線性代數
  > - 知道 logistic regression、sigmoid
  > - 沒系統學過 GBDT、boosting；不熟 Newton's method
  > - 不熟 XGBoost / LightGBM 的內部數學
  >
  > 任務：通讀文件，逐段標出卡關處。不要客氣、不要假裝懂、不要從上下文猜。看不懂就明說：
  > - 哪一句、哪個式子卡住
  > - 卡的原因（符號沒定義 / 推導跳步 / 動機沒講 / 斷言無理 / 上下文不接）
  > - 你期待補什麼才能往下讀
  >
  > 另外四項檢查：(i) 主旨一致性（結論／開場是否與內文證據矛盾、有無過度宣稱）；(ii) 抽象未落地（哪些主張只有形容詞、缺具體數字）；(iii) 鷹架洩漏（標題／內文的自用記號）；(iv) 範疇失衡（邊緣主題佔太多／核心講太淺）。
  >
  > 保留 persona、驗算（重算 Ch7／Ch8 所有數字）、跨章引用檢查（章號／符號／數字跨章一致）。

  **給 subagent 的脈絡（自身冷啟動需知）**：本手冊**假設讀者已讀手冊1（GBDT 機制、Newton step）與手冊2（單一不平衡）**——若卡關處其實是手冊1/2 已教的前提，subagent 仍應標出，但我（觸發者）triage 時判定「屬前提、不在本手冊補」即可；真正要補的是手冊3 自身引入卻沒交代的東西。檔案路徑：worktree 內 `gbdt_multiitem_imbalance.md`、`gbdt_binary_classification.md`、`gbdt_class_imbalance.md`。
- [ ] **Step 2：修正** QA 發現的問題（逐項）。
- [ ] **Step 3：若改動正文 → 重跑 Task 13 Step 2 重建 HTML。**
- [ ] **Step 4：驗證**：QA 無剩餘 blocking 問題；簡繁掃描乾淨。
- [ ] **Step 5：commit**：`docs(handbook): 手冊3 定稿 QA 修正`

---

## Task 15：收束開發分支

- [ ] **Step 1：** 用 superpowers:finishing-a-development-branch。docs-only，無測試套件可跑——以「Ch7/Ch8 數字重算腳本通過 + reader QA 通過」作為驗證取代 `pytest`（明說此替代，勿空跑 ~33 分鐘 Spark 測試）。
- [ ] **Step 2：** 呈現 4 選項給使用者（合併／PR／保留／丟棄），依選擇執行。**不自行 merge/push，等使用者選。**

---

## Self-Review（計畫對 spec 的覆蓋檢查）

- 11 章 + 前言 + TOC + 符號表 + HTML + QA + 收束 = Task 1–15，逐一對上 spec §1–§8。✓
- 無 placeholder：Ch7/Ch8 的數字與驗算腳本均寫死、可執行。✓
- 型別/數字一致：Ch4 引用的 2.97/39.6/13× 與 Ch7 表、Ch8 的 53.6 與其驗算腳本一致；錨點 `#ch1..#ch11` 與 TOC、HTML `<ol start="8">` 一致。✓
- 審查精修全部落章：Ch1 popularity-bias、Ch2 focal、Ch3 條件性、Ch6 負遷移+降級、Ch7 護欄、Ch8 校準提醒、Ch10 logQ。✓
