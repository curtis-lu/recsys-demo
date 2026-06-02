# 手冊4《learning-to-rank vs 二元分類 GBDT，與多 item 冷熱門》撰寫實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 寫出手冊4 的 `gbdt_learning_to_rank.md` 正文與 KaTeX 離線 HTML，講清 learning-to-rank（lambdarank）與 pointwise 二元分類在「梯度 / 樹生成策略 / 葉輸出」的差異，以及 by-customer 排序目標對多 item 冷熱門做了什麼（兩軸分析），並收束到 objective × structure 的 2×2。

**Architecture:** 自學向數學手冊，繁體中文，行內 `$...$`／獨立式 `$$...$$`。接續手冊1/2/3 的符號。兩部（換目標 LTR ＝重頭戲 8 章／換結構 per-item ＝2 章）+ 2×2 收尾。內容、符號、貫穿範例、文獻錨點全部來自已審定 spec：`docs/superpowers/specs/2026-06-02-gbdt-learning-to-rank-design.md`（**每個寫作任務先回讀對應 spec 章節**）。離線 HTML 由複用手冊3 的 build script（改 ROOT/檔名/title/`<ol start>`）產生。

**Tech Stack:** Markdown（python-markdown：tables / fenced_code / sane_lists）、KaTeX（內嵌於既有 offline HTML shell）、worktree venv python（`/Users/curtislu/projects/recsys_tfb/.venv/bin/python`，3.10.9，含 `markdown`）。

**寫作鐵律（每章適用，違反即返工）：** 遵 `docs/handbook-writing-guide.md`。具體數字落地、結論誠實（不誇稱 LTR 根治冷門）、不洩漏鷹架（章名不放備註、不後設旁白）、一個貫穿範例前後呼應、符號紀律（**$\lambda_{ij}$ 排序梯度 vs $\lambda$ 正則化、$\sigma(\cdot)$ sigmoid vs $\sigma_0$ RankNet shape、$g=p-y$ 對照手冊1 殘差 $r=y-p$**）、**純繁體中文不可出現簡體**。

**工作目錄：** 一律 `/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-ltr/`（分支 `docs/gbdt-learning-to-rank`）。所有路徑用此絕對前綴。git 用 `git -C <該絕對路徑>`。commit 時 graphify hook 用 `-c core.hooksPath=/dev/null` 關掉。

---

## 貫穿範例：單一真實來源（CANONICAL，所有章節共用，禁止偷換 guide §4/§12）

**固定設定**（ch5/ch6/ch7 全部引用同一組，不得換值）：
- items 宇宙 $\{H, C, X\}$：$H$ 熱門、$C$ 冷門、$X$ 中性 filler。位置折扣 $1/\log_2(1+\text{pos})$：pos1=1.0、pos2=0.63093、pos3=0.5。
- **Alice（冷門買家）**：買 $C$（$\mathrm{rel}_C{=}1, \mathrm{rel}_H{=}\mathrm{rel}_X{=}0$）。現況分數 $s_H{=}0.5, s_C{=}0.0, s_X{=}-0.5$（模型有 popularity bias，把熱門 $H$ 排最高 → 現況序 $H@1, C@2, X@3$）。
- **Bob（熱門買家）**：買 $H$（$\mathrm{rel}_H{=}1$，其餘 0），同分數 → $H@1$ 已正確。
- 常數：$\sigma_0{=}1$、正則化 $\lambda{=}1$。

**鎖定數字**（已用 venv python 驗算，見各 Task 的 verify heredoc；任一不符即修正正文）：
- Alice NDCG $= 0.63093$；$|\Delta\mathrm{NDCG}_{C,H}| = 0.36907$、$|\Delta\mathrm{NDCG}_{C,X}| = 0.13093$。
- Alice $\lambda_{C,H} = -0.22973$、$\lambda_{C,X} = -0.04943$、$\lambda_C = -0.27916$（負 = 把 $s_C$ 往上推）。
- Alice pointwise 對照 $g = [\,g_H{=}0.62246,\ g_C{=}-0.5,\ g_X{=}0.37754\,]$。
- Alice query 內 $\lambda_H{+}\lambda_C{+}\lambda_X = 0$（恆等式）→ 整張 list 同葉時 $\gamma{=}0$。
- Bob $\lambda_H = -0.27381$。**冷門買家 $|\lambda_C|{=}0.279 \approx$ 熱門買家 $|\lambda_H|{=}0.274$**（per-query 等權；差異是位置/margin 的巧合，非本質）。
- ch5 跨客戶葉子 $\{$Alice 的 $C$ 列, Bob 的 $H$ 列$\}$：binary $\gamma{=}0.59094$（log-odds 增量）vs lambdarank $\gamma{=}0.42453$（排序分數增量）。
- 真實量級（ch6 口語放大，比照手冊3）：$H$ 約 40000 買家、$C$ 約 100 → 軸 b 的 ~400:1 客戶數差。

---

## File Structure

- Create: `.../gbdt_learning_to_rank.md` — 手冊正文（單檔，11 章 + 前言 + TOC + 符號表）。
- Create: `.../build_ltr_html.py` — HTML build script（**不 commit**，比照手冊1/2/3 慣例）。
- Create (committed): `.../gbdt_learning_to_rank_offline.html` — build 產物。
- Modify (Task 16): `.../docs/handbook-writing-guide.md` — 補本次沉澱的原則。
- Reference only（worktree 已存在，勿改）：`gbdt_binary_classification.md`、`gbdt_class_imbalance.md`、`gbdt_multiitem_imbalance.md`、`gbdt_binary_classification_offline.html`（HTML shell 來源）、spec 檔。

**章節錨點：** `#top`、`#toc`、`#ch1`…`#ch11`。每章結尾 `[← Ch N](#chN) ｜ [Ch N+2 →](#chN+2)` nav（比照手冊3）。

---

## Task 1：骨架（前言 + 目錄 + 符號表 + 2×2 框架）

**Files:** Create `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §1–§4。**
- [ ] **Step 2：寫檔頭與前言**
  - `<a id="top"></a>` + `# 從二元分類到排序學習：learning-to-rank vs pointwise GBDT，與多 item 冷熱門`
  - 接續三本：連結用相對路徑 `[手冊1](gbdt_binary_classification.md)`（GBDT 機制）、`[手冊2](gbdt_class_imbalance.md)`（單一不平衡）、`[手冊3](gbdt_multiitem_imbalance.md)`（多 item 共享 pointwise 的冷熱門天花板）。
  - 複習手冊3 天花板一段（冷門病根 = 葉預算競爭 + 絕對稀疏；兩條突破路 = 換目標 / 換結構）。
  - 範疇框（spec §2：in = LTR + per-item；out 一句話劃出去 = 完整校準方法 / lambdarank 工程設定教學 / 其他 listwise 成員）。
  - 前提框：LTR 一律 by-customer；by-item 只在歸因/評估時提；輸出主要用來排序。
  - **2×2 框架圖**（objective × structure 的 markdown 表，spec §3）。
- [ ] **Step 3：寫目錄**（兩部 + 收尾，純 markdown，HTML build 再轉 nav）。第一部 換目標 Ch1–8、第二部 換結構 Ch9–10、收尾 Ch11。
- [ ] **Step 4：寫「符號接續」表**（spec §4 那張）：$s_i{=}F(x_i)$、$q$、$\mathrm{rel}_i$、$\mathrm{DCG}/\mathrm{IDCG}/\mathrm{NDCG}$、$\lambda_{ij}$、$\sigma_0$；明標防一符二義三條（$\lambda_{ij}$ vs $\lambda$、$\sigma$ vs $\sigma_0$、$g$ vs 手冊1 的 $r$）。並說明沿用手冊1–3 既有符號不重定義。
- [ ] **Step 5：驗證**：grep 確認 TOC 列了 `#ch1`–`#ch11`；無簡體字。
- [ ] **Step 6：commit**：`docs(handbook): scaffold 手冊4 前言+目錄+符號表+2×2`

---

## Task 2：Ch1 為什麼 pointwise 不是排序問題的「原生」損失

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.1–§5.2、§7 ch1。**
- [ ] **Step 2：寫 Ch1**：`<a id="ch1"></a>` + 標題「Ch 1. 為什麼 pointwise 不是排序問題的『原生』損失」。
  - 手冊1–3 全程 pointwise：`binary_logloss` 對**每一列**獨立要求「$p_i$ 對不對」。
  - 但推薦的用途與評估只在乎**同一客戶名下 items 的相對次序**（top-K）——目標與用途錯位：兩個模型可有相同 logloss 但排序天差地別，或相同排序但不同 logloss。
  - 鋪陳：要讓「訓練目標」對齊「排序用途」，就得換損失 → 進入 LTR。
  - 埋一句主軸（spec §5.1）：GBDT 建樹只看 $(g,h)$，所以「換損失」具體就是「換 $(g,h)$」，第一部會一路追這條因果。
- [ ] **Step 3：驗證**：無新未定義符號；錯位主張有具體「同 logloss 不同排序」式說明（不只形容詞，guide §3）。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch1 pointwise 非排序原生損失`

---

## Task 3：Ch2 三種取向 taxonomy（pointwise / pairwise / listwise）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.2、§7 ch2。**
- [ ] **Step 2：寫 Ch2**：標題「Ch 2. 三種取向：pointwise / pairwise / listwise」。
  - pointwise：逐列回歸/分類標籤（手冊1–3）。
  - pairwise：同 query 內成對比較（RankNet）。
  - listwise：直接對整張 list 的指標（NDCG/mAP）下手。
  - 定位 **lambdarank 在 pairwise 與 listwise 之間**（pairwise 梯度 × listwise 指標位移加權），這正是 Ch3–4 要推的。
  - 其他 listwise 成員（ListNet / softmax-CE）一句點名、不展開（spec §2 out）。
- [ ] **Step 3：驗證**：三類定義清楚、lambdarank 定位明確；無題外展開。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch2 LTR taxonomy`

---

## Task 4：Ch3 RankNet：pairwise 梯度

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.3、§7 ch3。**
- [ ] **Step 2：寫 Ch3**：標題「Ch 3. RankNet：成對比較的梯度」。
  - 動機先於公式：要比「$i$ 該排在 $j$ 前」，建模 $P_{ij}=\sigma(\sigma_0(s_i-s_j))$（$\sigma_0$ 註明原論文記 $\sigma$、與 sigmoid 區分）。
  - 交叉熵 $C_{ij}\xrightarrow{\bar P=1}\log(1+e^{-\sigma_0(s_i-s_j)})$；微分得 $\dfrac{\partial C_{ij}}{\partial s_i}=-\dfrac{\sigma_0}{1+e^{\sigma_0(s_i-s_j)}}\equiv\lambda_{ij}$。
  - 對照手冊1 的 $g_i=p_i-y_i$：**梯度從「單列 vs 標籤」變成「一對 vs 分數差」**。一筆文件梯度 $\lambda_i=\sum_j\lambda_{ij}$。
- [ ] **Step 3：驗證**：微分步驟可複核；$\sigma_0$ 與 $\sigma(\cdot)$ 不混。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch3 RankNet pairwise 梯度`

---

## Task 5：Ch4 從 RankNet 到 lambdarank（$\lambda_{ij}$ + 對照表，全文中心）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.4、§7 ch4。**
- [ ] **Step 2：寫 Ch4**：標題「Ch 4. 從 RankNet 到 lambdarank：用 $|\Delta\mathrm{NDCG}|$ 加權」。
  - 動機：RankNet 只顧「對」、不顧「位置」——把排名第 1、2 弄反，跟把第 50、51 弄反，代價不該一樣。
  - NDCG/DCG/IDCG 定義（二元相關形式，spec §5.4）。
  - 核心 boxed：$\lambda_{ij}=-\dfrac{\sigma_0}{1+e^{\sigma_0(s_i-s_j)}}|\Delta\mathrm{NDCG}_{ij}|$，$|\Delta\mathrm{NDCG}_{ij}|$ 展開式。
  - 二階導：$\propto\sigma_0^2\rho(1-\rho)|\Delta\mathrm{NDCG}|$，**正但非 $p(1-p)$**。
  - **全文中心：梯度逐量對照表**（spec §5.4 那張：作用單位 / 一階導 / 驅動力 / 在乎什麼 / 位置敏感 / 二階導）。
- [ ] **Step 3：驗證**：$\lambda_{ij}$、$|\Delta\mathrm{NDCG}|$、NDCG 公式與 spec／Burges 2010 一致；對照表六列齊全。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch4 lambdarank λij + 梯度對照表`

---

## Task 6：Ch5 同一套建樹機器、不同 $(g,h)$：切分與葉輸出（對照手冊1）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.5、§7 ch5。**
- [ ] **Step 2：寫 Ch5**：標題「Ch 5. 同一套建樹機器、不同的 $(g,h)$：切分與葉輸出怎麼變」。
  - **核心框架**：手冊1 **Ch 5.3** 的 Gain 公式、**Ch 6.3–6.4** 的葉輸出 $\gamma=-G/(H+\lambda)$ **原封不動**；LightGBM 對所有 objective 共用同段建樹/算葉值碼，唯一變的是餵進的 $(g_i,h_i)$。
  - **切分（樹結構）**：binary 高 Gain 一刀＝把正類從負類分乾淨（$g{=}p{-}y$ 驅動）；lambdarank 高 Gain 一刀＝把同 query 內排錯序的成對修正（$\lambda$ 驅動）→ 同資料選不同切點、長不同形狀樹。用 Alice 三列的兩種梯度向量對照：$g=[0.622,-0.5,0.378]$ vs $\lambda=[\lambda_H{=}+0.230,\lambda_C{=}-0.279,\lambda_X{=}+0.049]$。
  - **葉輸出**：$\gamma=-G/(H+\lambda)$ 在 binary 是 **log-odds 增量**（$\sigma$ 還原校準機率）；在 lambdarank 是**排序分數增量、無機率錨點**。跨客戶葉子 $\{$Alice 的 $C$, Bob 的 $H\}$：binary $\gamma{=}0.591$ vs lambdarank $\gamma{=}0.425$（同公式、不同值、不同意義）。
  - **「為何不是機率」的嚴格理由**（zero-space，spec §5.5）：排序損失只依賴 query 內分數差 → 同 query 分數同加常數損失不變 → query 內 $\sum_i\lambda_i=0$。**戲劇化鉤子**：若一葉剛好裝某客戶整張 list（如 Alice 三列），$\sum\lambda=0\Rightarrow\gamma=0$——模型對絕對水準毫無訊號，只在乎相對序。
  - **二階導語意改變**：`min_sum_hessian_in_leaf` 等旋鈕語意跟著變（lambdarank 的 $h$ 非機率信心）。
- [ ] **Step 3：驗證（python）**：
  ```bash
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
  import math
  sig=lambda x:1/(1+math.exp(-x)); log2=lambda x:math.log(x,2); disc=lambda p:1/log2(1+p)
  sH,sC,sX=0.5,0.0,-0.5; s0=1.0; lam=1.0; IDCG=disc(1)
  dN=lambda pi,pj,ri,rj:abs(2**ri-2**rj)*abs(disc(pi)-disc(pj))/IDCG
  L =lambda si,sj,dn:-s0/(1+math.exp(s0*(si-sj)))*dn
  # Alice 三列 lambda
  lC=L(sC,sH,dN(2,1,1,0))+L(sC,sX,dN(2,3,1,0)); lH=-L(sC,sH,dN(2,1,1,0)); lX=-L(sC,sX,dN(2,3,1,0))
  print("Alice lambda H/C/X:",round(lH,3),round(lC,3),round(lX,3),"sum",round(lH+lC+lX,6))
  print("Alice g H/C/X:",round(sig(sH),5),round(sig(sC)-1,5),round(sig(sX),5))
  # 跨客戶葉子 γ：Alice 的 C 列 + Bob 的 H 列
  hp=lambda si,sj,dn:(lambda r:s0*s0*r*(1-r)*dn)(1/(1+math.exp(s0*(si-sj))))
  gCb,hCb=sig(sC)-1,sig(sC)*(1-sig(sC)); gHb,hHb=sig(sH)-1,sig(sH)*(1-sig(sH))
  gamma_b=-(gCb+gHb)/(hCb+hHb+lam)
  lamC=lC; hessC=hp(sC,sH,dN(2,1,1,0))+hp(sC,sX,dN(2,3,1,0))
  lamH=L(sH,sC,dN(1,2,1,0))+L(sH,sX,dN(1,3,1,0)); hessH=hp(sH,sC,dN(1,2,1,0))+hp(sH,sX,dN(1,3,1,0))
  gamma_l=-(lamC+lamH)/(hessC+hessH+lam)
  print("leaf gamma binary",round(gamma_b,5),"lambda",round(gamma_l,5))
  PY
  ```
  期望：Alice lambda H/C/X = 0.23 / -0.279 / 0.049，sum 0；g = 0.62246 / -0.5 / 0.37754；leaf gamma binary 0.59094 / lambda 0.42453。不符即修正。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch5 生成策略差異(同機器異 g,h)`

---

## Task 7：Ch6 by-customer 分組對冷熱門做了什麼（兩軸，載重論點）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.6、§7 ch6（注意：方向性宣稱要實算佐證，guide §12）。**
- [ ] **Step 2：寫 Ch6**：標題「Ch 6. by-customer 排序對冷熱門做了什麼：兩軸分析」。
  - 把手冊3 的「稀釋」拆兩軸：(a) 全域共池稀釋（pointwise 下冷門 100 正類淹在 1000 萬列裡、且個人化切點 Gain 是「總列 logloss」尺度的零頭，min-child 還禁切）；(b) 出現頻率（冷門只在 ~100 客戶 list 是相關 item）。
  - **移除軸 (a)**——用 Alice vs Bob 實算佐證：冷門買家 Alice 對 $C$ 貢獻 $|\lambda_C|{=}0.279$、熱門買家 Bob 對 $H$ 貢獻 $|\lambda_H|{=}0.274$，**幾乎等大**。關鍵：$C$ 全域稀有**完全沒縮小** Alice 這一筆，因 $\lambda$ 只在她自己 list 內算、且 per-query 以 IDCG 正規化。lambdarank 把 Gain 的「貨幣」從「占總列 logloss 的比例」（冷門是零頭）換成「各客戶 NDCG 改善之和」（每個冷門買家是一個完整單位）。
  - **軸 (b) 仍在**：個人化 $C$ 的切點累積 $\lambda$ 質量 = 對 ~100 客戶求和，仍遠少於 $H$ 的 ~40000；樹按全域 Gain 生長，冷門仍被低估，只是**不再災難性**。且 `min_data_in_leaf` 是列數硬底、objective 無關，LTR 並未拿掉它。
  - **誠實結論**（guide §5）：LTR 打掉「共池稀釋」軸，但「絕對稀疏 / 出現頻率」軸＝手冊3 天花板**沒被變不見**；100 人買的 item 換 LTR 也生不出不存在的訊號。
  - 必然 vs 巧合（guide §12）：$0.279$ 與 $0.274$ 的微小不等是位置/margin 巧合，**本質是等大**；勿宣稱精確相等。
- [ ] **Step 3：驗證（python）**：
  ```bash
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
  import math
  log2=lambda x:math.log(x,2); disc=lambda p:1/log2(1+p)
  sH,sC,sX=0.5,0.0,-0.5; s0=1.0; IDCG=disc(1)
  dN=lambda pi,pj,ri,rj:abs(2**ri-2**rj)*abs(disc(pi)-disc(pj))/IDCG
  L=lambda si,sj,dn:-s0/(1+math.exp(s0*(si-sj)))*dn
  lC=L(sC,sH,dN(2,1,1,0))+L(sC,sX,dN(2,3,1,0))      # Alice 買 C
  lHb=L(sH,sC,dN(1,2,1,0))+L(sH,sX,dN(1,3,1,0))     # Bob 買 H（H 已 @1）
  print("|lambda_C| Alice",round(abs(lC),5),"|lambda_H| Bob",round(abs(lHb),5))
  PY
  ```
  期望：$|\lambda_C|\approx0.27916$、$|\lambda_H|\approx0.27381$（同量級，印證 per-query 等權）。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch6 by-customer 兩軸分析(載重)`

---

## Task 8：Ch7 貫穿範例（Alice 完整手算，數學最重）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §6、§7 ch7。**
- [ ] **Step 2：寫 Ch7**：標題「Ch 7. 貫穿範例：一位冷門買家的 NDCG 與 $\lambda$ 手算」。完整呈現 CANONICAL 設定：
  - Alice 三 items $\{H,C,X\}$、$s=(0.5,0,-0.5)$、買 $C$、現況序 $H@1,C@2,X@3$。
  - 位置折扣表（1.0 / 0.63093 / 0.5）。
  - $\mathrm{DCG}=0.63093$、$\mathrm{IDCG}=1.0$、$\mathrm{NDCG}=0.63093$。
  - 逐對：$|\Delta\mathrm{NDCG}_{C,H}|=0.36907$、$|\Delta\mathrm{NDCG}_{C,X}|=0.13093$；$\lambda_{C,H}=-0.22973$、$\lambda_{C,X}=-0.04943$、$\lambda_C=-0.27916$。
  - **對照 pointwise**：同三列 $g=[0.62246,-0.5,0.37754]$——pointwise 只看「$p$ 對不對」、不看位置；lambdarank 把「$C$ 該排在 $H$ 前」的位置資訊編進梯度。
  - query 內 $\sum\lambda=0$ 的恆等式（標明：必然，源於只依賴分數差；值是這組數字）。
  - 放大到真實量級（口語）：把 1 個 Alice 換成 ~100 個冷門買家、~40000 個 Bob → 軸 b 的客戶數差（接 Ch6）。
- [ ] **Step 3：驗證（python，關鍵）**：
  ```bash
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
  import math
  sig=lambda x:1/(1+math.exp(-x)); log2=lambda x:math.log(x,2); disc=lambda p:1/log2(1+p)
  sH,sC,sX=0.5,0.0,-0.5; s0=1.0; IDCG=disc(1)
  print("disc",round(disc(1),5),round(disc(2),5),round(disc(3),5))
  print("NDCG",round(disc(2)/disc(1),5))
  dN=lambda pi,pj,ri,rj:abs(2**ri-2**rj)*abs(disc(pi)-disc(pj))/IDCG
  L=lambda si,sj,dn:-s0/(1+math.exp(s0*(si-sj)))*dn
  dCH,dCX=dN(2,1,1,0),dN(2,3,1,0)
  lCH,lCX=L(sC,sH,dCH),L(sC,sX,dCX)
  print("dCH",round(dCH,5),"dCX",round(dCX,5))
  print("lCH",round(lCH,5),"lCX",round(lCX,5),"lC",round(lCH+lCX,5))
  print("g",round(sig(sH),5),round(sig(sC)-1,5),round(sig(sX),5))
  PY
  ```
  期望：disc 1.0/0.63093/0.5；NDCG 0.63093；dCH 0.36907 / dCX 0.13093；lCH -0.22973 / lCX -0.04943 / lC -0.27916；g 0.62246/-0.5/0.37754。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch7 貫穿範例(Alice NDCG/λ 手算)`

---

## Task 9：Ch8 mAP 變體 + 換目標的校準後果（輕量）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.7、§7 ch8。**
- [ ] **Step 2：寫 Ch8**：標題「Ch 8. mAP 變體與『輸出不再是機率』的後果」。
  - mAP 一節：把 $|\Delta\mathrm{NDCG}|$ 換成 $|\Delta\mathrm{AP}|$，$\lambda_{ij}$ 框架不變——換指標＝換 $|\Delta|$ 加權（引 Yue et al. 2007）。一句帶過 NDCG vs mAP 的取捨（NDCG 有位置折扣與多級相關，mAP 純二元 precision 導向）。
  - 校準後果（輕量，深層機制已在 Ch5）：LTR 分數無絕對水準（承 Ch5 zero-space）→ 跨 item / 跨客戶比分數、要機率絕對值時要另做校準；同一客戶內排序不受影響（保序）。完整校準方法（Platt/isotonic）一句話劃為獨立主題、不展開（spec §2 out）。
- [ ] **Step 3：驗證**：mAP 與 NDCG 同框架敘述正確；校準段不展開方法、不與 Ch5 重複機制。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch8 mAP 變體 + 校準後果`

---

## Task 10：Ch9 per-item 獨立模型（換結構，葉預算競爭消失）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.8 ch9、§7 ch9。**
- [ ] **Step 2：寫 Ch9**：標題「Ch 9. 換結構：per-item 獨立模型」。
  - 結構：每個 item 各訓一個（pointwise）模型。
  - 手冊3 核心病根——跨 item 葉預算競爭——**因結構直接消失**：冷門模型獨佔自己全部葉預算；$F_0=F_{0,C}$（先驗自動正確，不再有全域起點錯位）。
  - 與第一部對照：LTR 是「同一模型內改梯度讓冷門有競爭力」，per-item 是「拆開讓冷門不必競爭」——兩條不同軸。
- [ ] **Step 3：驗證**：與手冊3 Ch4「葉預算競爭」「Ch2 先驗」引用一致。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch9 per-item 模型(競爭消失)`

---

## Task 11：Ch10 per-item 的三筆代價（含正/負遷移翻轉）

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.8 ch10、§7 ch10、手冊3 Ch6 負遷移。**
- [ ] **Step 2：寫 Ch10**：標題「Ch 10. per-item 的三筆代價」。
  - 代價一 **失去跨 item 正遷移**：共享模型讓冷門能借用熱門學到的特徵結構（冷熱買家相似時受益）——這正是手冊3 Ch6「負遷移」的**另一面**；拆成 per-item 後，正負遷移一起消失，冷門再借不到力。（內嵌一段強連結手冊3 Ch6。）
  - 代價二 **每模型回到手冊2 單目標極端稀疏**：冷門模型只有 ~100 正類、**且無處借力**，手冊2 那套不平衡（飢餓、$F_0$ 太低、min-child）全套上身。
  - 代價三 **$M$ 倍訓練/維運成本**（$M$ 個模型的版本、監控、上線）。
- [ ] **Step 3：驗證**：三筆代價齊全；正/負遷移翻轉與手冊3 Ch6 連結正確（引對章節）。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch10 per-item 三筆代價`

---

## Task 12：Ch11 2×2 合論 + 誠實天花板 + 延後校準

**Files:** Modify `gbdt_learning_to_rank.md`

- [ ] **Step 1：回讀 spec §5.9、§7 ch11。**
- [ ] **Step 2：寫 Ch11**：標題「Ch 11. 合論：objective × structure 的 2×2」。
  - 2×2 四格（pointwise/listwise × 共享/per-item）各自適用情境；**可組合**（per-item LTR 模型）。回扣 Ch1 埋的主軸。
  - 誠實天花板（呼應手冊3 + Ch6）：這些都在重排「誰跟誰競爭、用什麼梯度」，冷門 item 的絕對稀疏是資料層問題，模型形態換不出無中生有的訊號（引 cold-start，Meehan & Pauwels 2025，沿手冊3）。
  - 延後：完整機率校準（→ 可能的手冊5）。
- [ ] **Step 3：驗證**：收尾不誇稱（不寫「換 LTR 就解決冷門」）；2×2 與前文一致；延後清單明確。
- [ ] **Step 4：commit**：`docs(handbook): 手冊4 Ch11 2×2 合論 + 收尾`

---

## Task 13：建離線 HTML

**Files:** Create `build_ltr_html.py`（不 commit）、`gbdt_learning_to_rank_offline.html`（commit）

- [ ] **Step 1：複製改作 build script**：以 worktree 既有的 `build_multiitem_html.py`（若不在 worktree，從手冊3 worktree `.worktrees/gbdt-multiitem/build_multiitem_html.py` 取）為底，存成 `build_ltr_html.py`，改：
  - `ROOT = pathlib.Path("/Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-ltr")`（**worktree path**）。
  - 來源 `gbdt_learning_to_rank.md` → 輸出 `gbdt_learning_to_rank_offline.html`。
  - `<title>` 改手冊4 標題。
  - HTML shell 來源仍讀 `gbdt_binary_classification_offline.html`（KaTeX 內嵌）。
  - `<ol start=...>`：第一部 Ch1–8（預設 start=1）、第二部 `<ol start="9">`（Ch9–10）、收尾 `<ol start="11">`（Ch11）——對應 TOC 三段。
- [ ] **Step 2：執行 build**：
  ```bash
  cd /Users/curtislu/projects/recsys_tfb/.worktrees/gbdt-ltr && /Users/curtislu/projects/recsys_tfb/.venv/bin/python build_ltr_html.py
  ```
  期望 stdout：`MATHPLACEHOLDER leftover: 0`、`nav.nav` > 0、`nav.toc: 1`。
- [ ] **Step 3：驗證**：`MATHPLACEHOLDER leftover: 0`；grep HTML 確認 katex CSS/JS 內嵌、`<ol start="9">`/`<ol start="11">` 存在、每個 `#chN` 錨點都在。
- [ ] **Step 4：commit**（只加 `.html`）：`docs(handbook): 手冊4 離線 HTML`

---

## Task 14：定稿 reader-subagent 通讀 QA

**Files:** Modify `gbdt_learning_to_rank.md`（修 QA 發現）、必要時重 build HTML

- [ ] **Step 1：派 reader-subagent**（整篇寫完、HTML build 完才跑），通讀 `gbdt_learning_to_rank.md`。使用者指定 reviewer persona（一字不差）：

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
  > 另外四項檢查：(i) 主旨一致性（結論／開場是否與內文證據矛盾、有無過度宣稱）；(ii) 抽象未落地（哪些主張只有形容詞、缺具體數字）；(iii) 鷹架洩漏（標題／內文的自用記號、後設旁白）；(iv) 範疇失衡（邊緣主題佔太多／核心講太淺）。
  >
  > 保留 persona、驗算（重算 Ch5/Ch6/Ch7 所有數字）、跨章引用檢查（章號／符號／數字跨章一致）。

  **給 subagent 的脈絡**：本手冊**假設讀者已讀手冊1（GBDT 機制、Newton step、Gain/葉輸出公式）、手冊2（單一不平衡）、手冊3（多 item 共享冷熱門）**——若卡關處其實是前三冊已教的前提，仍標出，但觸發者 triage 時判「屬前提、不在本冊補」即可。檔案：worktree 內 `gbdt_learning_to_rank.md` 及手冊1/2/3。
- [ ] **Step 2：修正** QA 發現（逐項）。
- [ ] **Step 3：若改動正文 → 重跑 Task 13 Step 2 重建 HTML。**
- [ ] **Step 4：驗證**：無剩餘 blocking；簡繁掃描乾淨。
- [ ] **Step 5：commit**：`docs(handbook): 手冊4 reader QA 修正`

---

## Task 15：正確性 + 易讀性雙 subagent 審（比照手冊3）

**Files:** Modify `gbdt_learning_to_rank.md`、必要時重 build

- [ ] **Step 1：派「正確性」subagent**（可上網查文獻）。任務：
  - 驗 spec §5 全部數學：RankNet 微分、$\lambda_{ij}$ 與 $|\Delta\mathrm{NDCG}|$ 公式、二階導近似、zero-space（query 內 $\sum\lambda=0$）論證、Ch6 兩軸方向性（重算 $|\lambda_C|$ vs $|\lambda_H|$、確認「移除軸 a / 保留軸 b」沒過度宣稱）。
  - 查證引用（guide §12）：Burges 2005/2006/2010、Järvelin & Kekäläinen 2002、Yue et al. 2007、Meehan & Pauwels 2025 的作者/年份/出處；$\lambda_{ij}$ 公式與 Burges 2010 一致；NDCG 定義無誤；「軟建議 vs 硬限制」沒寫死。
- [ ] **Step 2：派「易讀性」subagent**：檢查 guide §6/§7/§12（鷹架洩漏、後設旁白、代號代稱、流程可操作、參考點一致、必然 vs 巧合標註、規模壓力測試、旋鈕起手值、處方交互）。
- [ ] **Step 3：triage 三類（真缺陷補 / 可加強斟酌 / 誤讀不改）並修正。**
- [ ] **Step 4：若改動正文 → 重建 HTML。**
- [ ] **Step 5：commit**：`docs(handbook): 手冊4 正確性+易讀性雙審修正`

---

## Task 16：統整原則 → 更新寫作指引 + memory

**Files:** Modify `docs/handbook-writing-guide.md`、memory 檔

- [ ] **Step 1：把本次新沉澱的原則補進 `docs/handbook-writing-guide.md`**（若有；候選：「跨『物件形狀不同』的續作如何接貫穿範例」「方向性直覺被實算否定的案例（Bob λ≠0）」「同一引擎換損失＝換 (g,h) 的講法」——僅在確實是**新**且通用時才加，避免與 §12 重複）。
- [ ] **Step 2：更新 memory** `project_gbdt_handbook_series.md`（手冊4 狀態）與 `MEMORY.md` 索引行。
- [ ] **Step 3：commit**：`docs(handbook): 手冊4 寫作原則沉澱 + memory`（memory 檔在 `~/.claude/...` 不在 repo，分開處理、不入此 commit）。

---

## Task 17：收束開發分支

- [ ] **Step 1：** 用 superpowers:finishing-a-development-branch。docs-only，無測試套件——以「Ch5/6/7 數字重算腳本通過 + 三審通過」替代 `pytest`（明說此替代，勿空跑 ~33 分鐘 Spark 測試）。
- [ ] **Step 2：** 呈現 4 選項（合併／PR／保留／丟棄）。**不自行 merge/push，等使用者選。**

---

## Self-Review（計畫對 spec 的覆蓋檢查）

- **Spec 覆蓋**：spec §5.1→Ch1/Ch5、§5.2→Ch2、§5.3→Ch3、§5.4→Ch4、§5.5→Ch5、§5.6→Ch6、§5.7→Ch8、§5.8→Ch9/Ch10、§5.9→Ch11、§6 貫穿範例→Ch5/6/7、§8 文獻→Ch4/Ch8/Ch15、§9 審稿→Task 14/15。✓
- **無 placeholder**：Ch5/Ch6/Ch7 數字與驗算腳本全寫死、可執行（CANONICAL 區 + 各 Task verify heredoc）。✓
- **數字/型別一致**：Alice $\lambda_C{=}-0.27916$、Bob $\lambda_H{=}-0.27381$、leaf $\gamma$ 0.591/0.425、NDCG 0.63093、$|\Delta\mathrm{NDCG}|$ 0.36907/0.13093 跨 Ch5/6/7 同源；錨點 `#ch1..#ch11` 與 TOC、HTML `<ol start="9">/"11">` 一致；符號 $\lambda_{ij}$/$\lambda$、$\sigma$/$\sigma_0$、$g$/$r$ 全處防混。✓
- **審稿三審**（reader + 正確性 + 易讀性）落在 Task 14/15；載重論點（Ch6 兩軸）特別交正確性 subagent 重算。✓
