# 設計：GBDT 數學自學手冊 4 — learning-to-rank vs 二元分類，與多 item 冷熱門

- **日期**：2026-06-02
- **狀態**：設計中（brainstorming → spec）
- **產出檔**：`gbdt_learning_to_rank.md`（手冊正文）、`gbdt_learning_to_rank_offline.html`（KaTeX 離線版）
- **系列**：接續手冊1《二元分類 GBDT 數學推導手冊》、手冊2《類別不平衡的數學影響與處理》、手冊3《多 item 共享模型下的冷熱門不平衡》。
- **暫定標題**：《從二元分類到排序學習：learning-to-rank vs pointwise GBDT，與多 item 冷熱門》（spec review 時可調）

---

## 1. 背景與動機

手冊3 在「**單一共享 pointwise 模型**」框架內證明了冷門 item 的核心病根：先驗便宜（$O(M)$ 刀 item split 修掉），但個人化切點搶不到 leaf-wise 的**葉預算**競爭，且當冷門 item 的**絕對正類筆數**低到一定程度（保單 100 人 / 1000 萬列），單一共享模型有它的**天花板**。手冊3 結尾明列了兩條可能突破天花板的路，都是「換掉某個東西」：

1. **換目標函數**：把 pointwise 的 `binary_logloss` 換成按客戶分組的排序損失（learning-to-rank，lambdarank），用 NDCG/mAP 當訓練目標。
2. **換模型結構**：把單一共享模型換成 per-item 獨立模型，徹底消除跨 item 競爭。

手冊4 就把這兩條路講清楚，並且回答使用者最在意的核心問題：**learning-to-rank 與二元分類器在「模型生成策略」上到底差在哪——因為目標不同，樹怎麼切、葉子輸出什麼，都跟著變；而這對冷熱門 item 又意味著什麼。**

## 2. 讀者、前提、範疇（guide §1）

**預設讀者**：已讀過手冊1–3（熟 GBDT 的 $F/p/g/h/\text{Gain}/\gamma$ 機制、類別不平衡、多 item 共享模型的冷熱門診斷）；會微積分、線代、logistic regression。

**前提**：
- LTR 一律以 **query = 客戶** 分組（排該客戶名下的 items，對應 top-K 推薦）。**不存在 by-item 當 query 的訓練情境**；by-item 分組只在「把貢獻歸因到某個 item / 做 per-item 評估」時才計算，會在 ch6 一句帶過、不作為訓練目標。
- 沿手冊1–3 立場：輸出主要用來**排序 / 挑高分客戶**，不是拿機率絕對值去運算。

**範疇（in）**：
- 第一部（重頭戲）：pointwise `binary_logloss` vs pairwise/listwise（RankNet → lambdarank）的**梯度差異**，以及「同一套 GBDT 建樹機器、不同 $(g,h)$ → 切分與葉輸出皆變」的**生成策略差異**；by-customer 分組對冷熱門的效果（兩軸分析）。
- 第二部：per-item 獨立模型（換結構），其對葉預算競爭的消除與三筆代價。
- 收尾：objective × structure 的 2×2 合論。

**範疇（out，一句話劃出去，guide §1）**：
- **完整的機率校準**（Platt scaling / isotonic regression 等方法）是重要但獨立的主題，本文只談「LTR 輸出為何不是校準機率」的**機制與後果**（輕量），方法留待可能的手冊5。
- lambdarank 的**完整工程設定教學**（group 構造、`label_gain`、`lambdarank_truncation_level`、`eval_at` 等旋鈕）只在與數學相扣處輕點，不開 config 章節。
- listwise 家族的其他成員（ListNet、softmax cross-entropy / NeuralNDCG 等）只在 taxonomy 點名定位，不逐一展開。

## 3. 核心設計決定（brainstorm 結論）

1. **結構**：兩部制（換目標 / 換結構）+ 2×2 收尾，鏡像手冊3「診斷→處方」的節奏。LTR 是重頭戲（第一部 8 章），per-item 是較短的第二部（2 章）。
2. **LTR 分組**：純 by-customer。
3. **新增 ch5**：LTR vs 原生 GBDT 的生成策略差異，明確回扣手冊1（使用者明確要求）。
4. **貫穿範例**：全新 per-customer 結構（客戶 × items 排序 list），但「冷/熱 item」沿用手冊3 的 H/C 身分與冷熱精神；數字挑乾淨好手算的，**不硬遷就手冊3 舊值**。一例串 ch5–7。
5. **審稿**：定稿派三審——reader-subagent（工程讀者 persona）＋ 正確性 subagent（可查文獻）＋ 易讀性 subagent（比照手冊3）。

## 4. 符號接續與紀律（guide §8）

沿用手冊1–3 全部記號。新增 / 釐清：

| 記號 | 意義 | 防一符二義 |
|---|---|---|
| $s_i = F(x_i)$ | 模型對一列 $(\text{客戶}, \text{item})$ 的**原始分數**（即手冊1 的 $F$） | pointwise 下解讀成 log-odds；LTR 下是**未校準排序分數**——同一個 $F$，不同解讀。這是 ch5 的核心橋樑 |
| $q$ | query（本文 = 一位客戶） | |
| $\mathrm{rel}_i \in \{0,1\}$ | item $i$ 對該客戶的相關性（買=1） | |
| $\mathrm{DCG}@K,\ \mathrm{IDCG},\ \mathrm{NDCG}$ | 折扣累積增益與其正規化 | |
| $\lambda_{ij}$ | lambdarank 對「一對 $(i,j)$」的梯度 | **不可**與手冊1–3 的正則化 $\lambda$ 混；後者維持無下標 $\lambda$ |
| $\sigma_0$ | RankNet 機率模型的 shape 參數 | sigmoid 維持 $\sigma(\cdot)$；$\sigma_0$ 註明「原論文記 $\sigma$」；玩具例取 $\sigma_0 = 1$ |
| $g_i = p_i - y_i$ | pointwise 一階導（沿手冊3） | 手冊1 Ch 4.2 記殘差 $r = y - p = -g$；引用時註明，避免符號衝突 |

## 5. 技術內容與數學錨點

> 這節是給「正確性 subagent」審的設計核心。所有公式在實作時都要用 python 實算驗證（guide §3）。

### 5.1 統一框架：GBDT 只看 $(g, h)$

手冊1 推導的建樹流程，從頭到尾只用到每列的一階導 $g_i$ 與二階導 $h_i$：
- Gain 公式（手冊1 **Ch 5.3**）：$\text{Gain} = \tfrac12\big[\frac{G_L^2}{H_L+\lambda} + \frac{G_R^2}{H_R+\lambda} - \frac{G^2}{H+\lambda}\big]$，其中 $G=\sum g_i,\ H=\sum h_i$。
- 葉輸出（手冊1 **Ch 6.3–6.4**，Newton step）：$\gamma = -G/(H+\lambda)$。

**關鍵洞見**：LightGBM 對所有 objective 共用**同一段**建樹 / 算葉值的程式碼。換 loss，唯一變的是餵進去的 $(g_i, h_i)$。於是「換目標 → 換梯度 → 同公式產生不同的樹與葉值」是一條乾淨的因果鏈，貫穿整個第一部。

### 5.2 pointwise 的 $(g,h)$（recap，手冊1 Ch 4 / Ch 6.2）

$$g_i = p_i - y_i, \qquad h_i = p_i(1-p_i), \qquad p_i = \sigma(s_i)$$

驅動力 = 標籤與預測機率的差；位置無關；逐列獨立。

### 5.3 RankNet：pairwise 梯度（ch3）

對同一 query 內、已知 $i$ 應排在 $j$ 前面（$\mathrm{rel}_i > \mathrm{rel}_j$）的一對，建模「$i$ 排前」機率
$$P_{ij} = \sigma\big(\sigma_0 (s_i - s_j)\big) = \frac{1}{1 + e^{-\sigma_0 (s_i - s_j)}}$$
目標 $\bar P_{ij} = 1$。交叉熵成本
$$C_{ij} = -\bar P_{ij}\log P_{ij} - (1-\bar P_{ij})\log(1-P_{ij}) \;\xrightarrow{\bar P=1}\; \log\!\big(1 + e^{-\sigma_0(s_i - s_j)}\big)$$
對 $s_i$ 微分：
$$\frac{\partial C_{ij}}{\partial s_i} = -\frac{\sigma_0}{1 + e^{\sigma_0(s_i - s_j)}} \equiv \lambda_{ij}, \qquad \frac{\partial C_{ij}}{\partial s_j} = -\lambda_{ij}$$
文件要點：**梯度作用在「一對」上、且只看分數差 $s_i - s_j$**（不看絕對值、不看標籤機率）。一筆文件 $i$ 的總梯度 = 對所有涉及 $i$ 的對求和：$\lambda_i = \sum_{j} \lambda_{ij}$（依誰更相關定正負號）。

### 5.4 從 RankNet 到 lambdarank：用 $|\Delta\mathrm{NDCG}|$ 加權（ch4，全文中心）

NDCG（二元相關）：
$$\mathrm{DCG}@K = \sum_{r=1}^{K} \frac{2^{\mathrm{rel}_r} - 1}{\log_2(r+1)} \xrightarrow{\mathrm{rel}\in\{0,1\}} \sum_{\substack{\text{買的 item}\\\text{排在 top-}K}} \frac{1}{\log_2(\text{rank}+1)}, \qquad \mathrm{NDCG} = \frac{\mathrm{DCG}}{\mathrm{IDCG}}$$

lambdarank 把 RankNet 的每一對乘上「交換 $i,j$ 兩者排名位置會造成的 NDCG 變化量」：
$$\boxed{\;\lambda_{ij} = -\,\frac{\sigma_0}{1 + e^{\sigma_0(s_i - s_j)}}\;\big|\Delta\mathrm{NDCG}_{ij}\big|\;}$$
其中 $|\Delta\mathrm{NDCG}_{ij}| = \dfrac{1}{\mathrm{IDCG}}\,\big|2^{\mathrm{rel}_i} - 2^{\mathrm{rel}_j}\big|\,\Big|\dfrac{1}{\log_2(1+\mathrm{pos}_i)} - \dfrac{1}{\log_2(1+\mathrm{pos}_j)}\Big|$。

二階導（hessian）：對 $s_i$ 再微分，$\propto \sigma_0^2\,\rho_{ij}(1-\rho_{ij})\,|\Delta\mathrm{NDCG}_{ij}|$（$\rho_{ij} = 1/(1+e^{\sigma_0(s_i-s_j)})$）。要點：它是**正的、但不是 $p(1-p)$**，是排序損失的二階近似。

**全文中心：梯度逐量對照表**（ch4 的高潮）

| 面向 | `binary_logloss`（pointwise） | lambdarank（pairwise + listwise） |
|---|---|---|
| 梯度作用單位 | 單列 $(\text{客戶}, \text{item})$ | 同一 query 內的「一對」$(i,j)$ |
| 一階導 | $g_i = p_i - y_i$ | $\lambda_i = \sum_j \lambda_{ij}$ |
| 由什麼驅動 | 標籤 vs 預測機率的差 | 同 query 內相對分數順序 $\times$ 指標位移 $|\Delta\mathrm{NDCG}|$ |
| 在乎什麼 | 每列機率對不對（校準） | 同 query 內的相對次序 |
| 位置敏感 | 否 | 是（$|\Delta\mathrm{NDCG}|$ 含 $\log_2$ 位置折扣） |
| 二階導 $h_i$ | $p_i(1-p_i)$，機率信心 | $\propto \sigma_0^2\rho(1-\rho)|\Delta\mathrm{NDCG}|$，近似、無機率語意 |

### 5.5 生成策略差異：同機器、異 $(g,h)$ → 切分與葉輸出皆變（ch5，對照手冊1）

**切分（樹結構）**：Gain 公式（手冊1 Ch 5.3）不變，但 $G_L, G_R$ 現在彙總的是 $\lambda$ 梯度。
- binary 下高 Gain 的一刀 = 「把正類從負類分乾淨」（$g = p-y$ 驅動）。
- lambdarank 下高 Gain 的一刀 = 「把同 query 內**排錯序的成對**修正」（$\lambda$ 驅動）。
- → 同一筆資料，兩個目標**選出不同切點、長出不同形狀的樹**（ch7 用同一玩具具體演示，guide §3/§4）。

**葉輸出（葉節點）**：$\gamma = -G/(H+\lambda)$（手冊1 Ch 6.3–6.4）不變。
- binary：$\gamma$ 是一個 **log-odds 增量**（可加進 $F$，$\sigma$ 還原成校準機率）。
- lambdarank：$G, H$ 是 $\lambda$ 梯度之和，$\gamma$ 變成一個**排序分數增量、沒有機率錨點**。

**「為何不是機率」的嚴格理由**（不靠語感，guide §12）：排序損失只依賴**同 query 內的分數差** $s_i - s_j$。把一個 query 內所有分數同加常數 $c$，所有差不變 → 損失不變 → query 內 $\sum_i \lambda_i = 0$（梯度的「常數平移方向」是零空間）。所以**絕對水準根本沒被損失約束** → 分數不是機率。這正是 ch7/ch8 校準後果的機制源頭。

### 5.6 by-customer 分組對冷熱門做了什麼：兩軸分析（ch6，載重論點，需 §12 正負號驗證）

把手冊3 對冷門 item 的「稀釋」拆成兩軸：
- **軸 (a) 全域共池稀釋**：pointwise 下，冷門 C 的 ~100 個正類，與全域 ~1000 萬列負類同處**一個 loss 總和**，被數量淹沒（手冊3 Ch3–Ch4）。
- **軸 (b) 出現頻率**：C 只在 ~100 位客戶的 list 裡是「買了的相關 item」（H 在 ~40000 位）。

LTR by-customer 的效果：
- **移除軸 (a)**：損失按 query 分解、且**每位客戶等權正規化**（除以該客戶的 IDCG）。一位「只買了冷門 C」的客戶，貢獻一個**全強度**、把 C 往上推的 $\lambda$——與「只買了熱門 H」的客戶的 $\lambda$ **同等強度**，完全不被那 1000 萬列「C 不在場」的負類稀釋。per-query 正規化中和了「熱門靠更多正類列灌爆全域梯度和」這個 pointwise 機制。
- **軸 (b) 仍在**：把 C 往上推的總梯度質量 = 對 ~100 個 list 求和，仍遠少於 H 的 ~40000；樹仍按全域 Gain 生長，個人化 C 的切點累積的 $\lambda$ 質量仍較小 → 冷門仍被低估，只是**不再災難性**。

**誠實結論（guide §5）**：LTR 打掉「共池稀釋」軸，但「絕對出現頻率 / 稀疏」軸——即手冊3 的天花板——**沒被變不見**。一個只有 100 人買的 item，換成 LTR 也生不出本來不存在的訊號。需用 ch7 玩具具體算出「兩位客戶的 $\lambda$ 同強度（印證移除軸 a）、但冷門客戶數遠少（印證軸 b 仍在）」。

### 5.7 mAP 變體 + 校準後果（ch7 尾 / ch8）

- **mAP 變體**：把 $|\Delta\mathrm{NDCG}|$ 換成 $|\Delta\mathrm{AP}|$（average precision 的位移），$\lambda_{ij}$ 框架不變——LambdaMART 可直接優化 mAP。一節帶過，點出「換指標 = 換 $|\Delta|$ 加權」。
- **校準後果（輕量）**：承 5.5 的零空間論證，LTR 分數無絕對水準 → 跨 item / 跨客戶比分數時，分數可比性與機率絕對值會浮上檯面；要機率得另做校準（方法延後）。在「同一客戶內排序」用途下不影響（保序）。

### 5.8 第二部 — per-item 獨立模型（換結構，ch9–10）

- **ch9 機制**：每個 item 各訓一個（pointwise）模型。手冊3 的核心病根——跨 item 葉預算競爭——**因結構直接消失**：冷門 item 的模型獨佔自己全部的葉預算、$F_0 = F_{0,C}$（先驗自動正確）。
- **ch10 三筆代價**：
  1. **失去跨 item 訊號共享（正遷移）**：這是手冊3 Ch6「負遷移」的另一面——共享模型讓冷門能借用熱門學到的特徵結構（冷熱買家若相似則受益）；拆開後，正負遷移一起消失，冷門再也借不到力。
  2. **每模型回到手冊2 的單目標極端稀疏**：冷門模型只有 ~100 正類、且**無處借力**，手冊2 那套不平衡全套上身。
  3. **$M$ 倍訓練 / 維運成本**（22 個模型的版本、監控、上線）。

### 5.9 收尾 — 2×2 合論（ch11）

objective（pointwise / listwise）× structure（共享 / per-item）的 2×2：四格各自的適用情境，且**可組合**（per-item LTR 模型）。誠實天花板（呼應手冊3）：這些都在重排「誰跟誰競爭、用什麼梯度」，但冷門 item 的絕對稀疏是資料層問題，模型形態換不出無中生有的訊號。延後：完整校準 → 可能的手冊5。

## 6. 貫穿範例設計（guide §3/§4，數字實作時 python 驗證）

**物件**：少數客戶（~4–5 位）× 一組 items（含熱門 H、冷門 C，加 1–2 個 filler 使 list 長度 ≥ 3，NDCG 才有位置可言），每位客戶一個二元相關 list（買=1）；外加一個可切的**客戶特徵**（如年齡組），讓「樹切分」有東西可切。

範例要同時服務三章：
- **ch5**：同一玩具下，比 binary（$g=p-y$）vs lambdarank（$\lambda$）各自選出的最佳切點與葉值——演示「同機器、不同樹/葉」。
- **ch6**：算出「只買 C 的客戶」與「只買 H 的客戶」貢獻的 $\lambda$ **同強度**（移除軸 a），但冷門客戶數少（軸 b 仍在）。
- **ch7**：完整手算某客戶的 $\mathrm{DCG}/\mathrm{NDCG}/\mathrm{IDCG}$、一對的 $|\Delta\mathrm{NDCG}|$、$\lambda_{ij}$（取 $\sigma_0=1$），並對照 pointwise 對同列會給的 $g$。

數字原則：挑能讓 $\log_2$ 折扣、$|\Delta\mathrm{NDCG}|$、$\lambda_{ij}$ 都好手算的小值；中間值能加總回最終值，關鍵步驟標 `✓`。

## 7. 章節大綱（11 章）

**前言**：接續手冊1/2/3、複習手冊3 天花板、2×2 框架、範疇、符號接續（§4 那張表）。

**第一部 — 換目標函數：從 pointwise 到 learning-to-rank**
1. 為什麼 pointwise 不是排序問題的「原生」損失（目標與用途錯位：`binary_logloss` 在乎每列機率，推薦只在乎同一客戶名下的相對次序）。
2. 三種取向 taxonomy：pointwise / pairwise / listwise，定位 lambdarank。
3. RankNet：pairwise 梯度推導（§5.3），對照 $g_i=p_i-y_i$。
4. 從 RankNet 到 lambdarank：$|\Delta\mathrm{NDCG}|$ 加權 → $\lambda_{ij}$，梯度逐量對照表（§5.4，全文中心）。
5. 同一套建樹機器、不同 $(g,h)$：切分與葉輸出怎麼變（§5.5，對照手冊1）。
6. by-customer 分組對冷熱門做了什麼：兩軸分析，連回手冊3 天花板（§5.6）。
7. 貫穿範例：客戶 × item list 手算 NDCG/$\lambda$/梯度，並對照同資料 binary vs lambdarank 的切點與葉值（§6）。
8. mAP 變體 + 換目標的校準後果（輕量，深層機制已在 ch5）（§5.7）。

**第二部 — 換結構：per-item 獨立模型**
9. per-item 獨立模型：跨 item 葉預算競爭直接消失（§5.8 ch9）。
10. 代價三筆：失去正遷移、每模型極端稀疏、$M$ 倍成本（§5.8 ch10）。

**收尾**
11. objective × structure 的 2×2 合論 + 誠實天花板 + 延後校準（§5.9）。

每章結尾「上一章 / 下一章」；目錄 anchor；HTML 版右下角回頂鈕；與手冊1/3 雙向交叉連結。

## 8. 文獻錨點（guide §12：作者/年份查證）

- **RankNet**：Burges, C., Shaked, T., Renshaw, E., et al. (2005). "Learning to Rank using Gradient Descent." *ICML*.
- **LambdaRank**：Burges, C., Ragno, R., Le, Q. (2006). "Learning to Rank with Nonsmooth Cost Functions." *NIPS*.
- **整合 overview（$\lambda_{ij}$ 公式出處）**：Burges, C. J. C. (2010). "From RankNet to LambdaRank to LambdaMART: An Overview." *Microsoft Research Technical Report MSR-TR-2010-82*.
- **NDCG**：Järvelin, K., Kekäläinen, J. (2002). "Cumulated gain-based evaluation of IR techniques." *ACM TOIS*.
- **直接優化 AP**：Yue, Y., Finley, T., Radlinski, F., Joachims, T. (2007). "A support vector method for optimizing average precision." *SIGIR*（mAP 一節的錨點）。
- 沿用手冊3 已引：popularity bias（Abdollahpouri et al., 2017）、cold-start（Meehan & Pauwels, 2025）。
- 實作對照：LightGBM `lambdarank` objective 官方文件（只在輕點工程旋鈕時引）。

正確性 subagent 任務之一：查證上述作者 / 年份 / 出處無誤，且 $\lambda_{ij}$、NDCG、$|\Delta\mathrm{NDCG}|$ 公式與 Burges 2010 一致。

## 9. 風格、體例、審稿（guide 全）

- 繁體中文；行內 `$...$`、展示式 `$$...$$`；關鍵結論 `$$\boxed{...}$$`；動機先於公式。
- 通用原理先於本文案例、分節（§2）。一節一概念。
- **特別自查（§12）**：(i) 貫穿範例參考點一致（明訂「在某分數快照上評估」，換點要明講）；(ii) 標明必然 vs 巧合（如 query 內 $\sum\lambda_i=0$ 是恆等式）；(iii) 規模壓力測試（兩軸結論對 $400{:}1$ 仍成立？）；(iv) 方向性宣稱先算正負號（LTR「幫了冷門」要實算 $\lambda$ 同強度）；(v) 新機制回頭校舊章（LTR 視角下手冊3 的「葉預算競爭」要重述成「軸 b」）；(vi) per-item 與 LTR 兩處方的交互（2×2 可組合）；(vii) 旋鈕給起手值（如 truncation level）；(viii) 引用查證 + 軟建議 vs 硬限制。
- **審稿三審**：reader-subagent（工程讀者 persona，逐段標卡關 + 驗算 + 跨章引用）＋ 正確性 subagent（可查文獻，驗 §5 全部數學與 §8 引用）＋ 易讀性 subagent。依回報分「真缺陷 / 可加強 / 誤讀」處理。

## 10. 工具與產出

- 正文 `gbdt_learning_to_rank.md`。
- 離線 HTML：`build_ltr_html.py`（複用手冊3 `gbdt_*_offline.html` 的 KaTeX-inlined shell；python-markdown extensions: tables / fenced_code / sane_lists）。**比照慣例 `build_*_html.py` 不入版控**，`gbdt_learning_to_rank_offline.html` 入版控。
- 開發環境：worktree `.worktrees/gbdt-ltr`、branch `docs/gbdt-learning-to-rank`（off origin/main）、`.venv` symlink 至 main root（Python 3.10.9）。docs-only，不跑 pipeline，不需 data symlink。

## 11. spec review 待確認的開放問題

1. 標題定稿（§元資料的暫定標題可換）。
2. 第二部是否真的只要 2 章，或要把「正遷移 / 負遷移翻轉」獨立成節以便與手冊3 Ch6 強連結（目前設計為 ch10 的第 1 筆代價，內嵌一段呼應）。
