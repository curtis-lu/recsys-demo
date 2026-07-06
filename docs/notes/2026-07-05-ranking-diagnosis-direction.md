# 方向診斷：per-query 排序 × macro per-item mAP 的模型診斷該怎麼定義

> 2026-07-05 初版，2026-07-06 重寫（修正一處條件機率錯誤、全面改寫行文）。這是一份**方向診斷文件**，不是工具規格。預設讀者：讀過 `docs/handbooks/gbdt/` 四冊手冊的人；本文沿用手冊的記號（$F$、$g$、$h$、Gain、$P_j$＝item $j$ 的正類筆數）。

## 0. 問題脈絡：我們在診斷什麼

**模型與用途。** 一個 LightGBM 二元分類模型（objective = `binary_logloss`，pointwise：每列獨立算 loss），訓練資料是 (query, item) 粒度的列，query = entity × time（例如客戶×月份），item id 是模型的一個類別特徵。推論時，每個 query 內把 ~22 個候選 item 按模型分數排名。量級：每期 ~10M query、熱冷 item 正類數約 400:1（~40000 vs ~100）。

**目標指標：macro per-item mAP。** 分三步定義：

1. 對某個正例 row（客戶 $q$ 這期真的買了 item $j$），看它在**自己 query 內**的名次 $k$，算 precision@$k$ ＝「該 query 排序前 $k$ 名中，正例佔幾分之幾」。
2. item $j$ 的 AP ＝ 對 item $j$ 的**所有正例 row** 把上面的 precision 取平均。
3. macro mAP ＝ 對 22 個 item 的 AP **等權**平均（不管冷熱）。

這個定義有兩個立即的推論，後面會反覆用到：

- **沒有任何正例的 query 完全不影響指標**——指標只在「有人買了東西」的 query 上有定義。所以任何「某 item 在多少比例的 query 裡如何」的論證，都必須在**有正例的 query**（更精確：受影響 item 的正例 query）上條件化，不能用全體 10M query 的邊際比例。
- **每個正例 row 在整個指標裡的權重是 $\frac{1}{22 \times P_j}$**。最冷 item（$P_j \approx 100$）的一個正例權重 $\frac{1}{22\times100}$，最熱（$P_j \approx 40000$）的一個正例權重 $\frac{1}{22\times40000}$——**前者是後者的 400 倍**。這是「macro 等權」的具體含義。

**要診斷的兩個現象。**

- **現象1（item 間尺度壓制）**：某些 item 的分數整體偏高，在每個 query 都佔前排，把**其他 item** 的正例名次往下壓、拉低那些 item 的 AP。受害的是別人；偏高的 item 自己反而得利。
- **現象2（item 內不可分）**：某個 item 自己的正例與負例分數分佈重疊——模型分不出「這個 item 該推給誰」，該 item 自己的 AP 低、拖累 macro。

**最終目的**不是理解模型本身，而是把「現象 → 成因 → 該動哪個訓練槓桿」接起來。可動的槓桿：per-item 負採樣、sample_weight、HPO 搜尋範圍、`lgb.Dataset` 分箱、對特定子母體補特徵。

**可用輸入**：model.txt 為主，可補一份同分布、有標籤的代表性樣本，必要時可重訓並記錄過程。

---

## 1. 用語約定

本文會用到幾個沒有通用中譯、或容易含糊的詞，先講定：

| 本文用語 | 意思 |
|---|---|
| **raw score（分數）** | 模型輸出的 $F$，log-odds 尺度（手冊1 Ch1）。本文說「分數」都指它，不指 sigmoid 後的機率。 |
| **校準（calibration）** | 模型預測的機率和實際發生率一致。例：模型對某群列平均預測 2%，那群列實際有 2% 是正例，就叫校準。 |
| **item 內 AUC** | 只拿 item $j$ 自己的列（跨所有 query 蒐集），算「隨機抽一個正例、一個負例，正例分數較高」的機率。0.5 ＝完全分不出來。它從頭到尾**沒有跟別的 item 比較**。 |
| **分數水準（level）／條件判別力（slope）** | 把 item $j$ 的分數粗分成兩部分：水準＝它跨所有 query 的平均高度；條件判別力＝分數會不會**在對的 query**（真的會買的客戶那裡）升高。現象1 是水準的問題，現象2 是條件判別力的問題。 |
| **人為偏移（artifact）** | 不是模型「學壞」，而是你自己的採樣／加權配置在數學上必然造成的分數偏移。 |
| **替換實驗（ablation）** | 把模型輸出的某個成分換成一個基準值、重算指標，用指標的變化量衡量那個成分的貢獻。 |
| **同一根因（同源）** | 「同源」指現象1 與現象2 出自同一個機制；本文一律寫成「同一根因」。 |
| **正類率（prevalence）** | 某群列裡正例的比例。 |

## 2. 貫穿範例

後面多數論證用同一組人物走一遍（少數地方需要反例時會臨時加角色、當場交代），角色沿用手冊3/4：熱門 item $H$（換匯型）、冷門 item $C$（保單型）、陪襯 item $X$。

- **客戶 Alice**：這期買了 $C$，沒買 $H$、$X$。模型給她的分數：$s_H = 2.0$、$s_C = 1.5$、$s_X = 0.0$，所以她的排序是 $H$@1、$C$@2、$X$@3。她那筆 $C$ 的正例排在第 2 名，precision@2 ＝（前 2 名裡的正例數）/2 ＝ 1/2 ＝ **0.5**。
- **客戶 Bob**：這期 $H$ 和 $C$ **都買了**。同樣的分數排序下，$H$@1 是正例、$C$@2 也是正例，$C$ 那筆的 precision@2 ＝ 2/2 ＝ **1.0**——$H$ 壓在上面，但因為 $H$ 在 Bob 這裡是真的買了，分子也加一，$C$ 毫髮無傷。

Alice 和 Bob 的處境只差一件事：壓在 $C$ 上方的 $H$ 是不是該 query 的正例。這個差別是後面好幾個結論的樞紐。

---

## 3. 總裁決表

先給結論總表，各節展開。整體判斷：**你的框架大體成立，不必推翻**；但有兩個地方要重講，它們會改變你接下來做的每件事——

1. **現象1 是三種情況，不是「bug vs 正常熱門」兩種。** 第三種（而且是最可能的一種）：分數偏移是**你自己的採樣／加權配置的數學後果**，或者「模型其實校準正確，是你的指標本身要求把冷門往上搬」。三種情況的處方完全不同（§5.4）。
2. **歸因的主軸該是「在指標上做替換實驗」，不是「看分數分佈的形狀」。** 分佈重疊、分佈偏高都只是間接症狀，會漏報也會誤報（§5.3、§5.5）。

| # | 待裁決 | 裁決 | 一句話 |
|---|---|---|---|
| 假設1 | 逐棵讀樹能答現象成因？ | **不成立**（逐棵這個粒度）；跨樹按 item 記帳的聚合統計**部分成立** | 單棵樹不可解讀；有用的是「每個 item 分到多少切點與 Gain」的帳本 |
| 假設2 | 只丟 model.txt 夠嗎？ | **不成立** | 兩個現象都是「模型×母體×標籤」三者合起來的性質，model.txt 只有模型 |
| 假設3a | 現象1 經 query 內競爭掉分？ | **成立**，附兩個修正 | per-item 偏移正是平移不變性豁免不掉的那個分量；但觀測方式要改（§5.3） |
| 假設3b | 「偏高」vs「正常熱門」怎麼分？ | 用 **per-item 校準檢定**分，結果分三種情況 | §5.4 |
| 假設3c | 「item 內重疊」＝ per-item mAP 差？ | **只是相關**，兩個方向都推不出對方 | 分數平坦但水準高的 item，自己的 mAP 反而好（§5.5） |
| 同一根因？ | 現象1、2 出自同一機制？ | **部分成立**：同一個上游（冷熱懸殊），兩條不同的中介路徑 | 處方不同，必須分開治（§5.6） |
| 你的拆解 lens | 水準＋條件兩成分 | **成立**（做成指標上的替換實驗，可判定）；當成模型內部真實結構則不行 | 這就是我建議的第一個實驗（§5.6） |
| 指標本身 | 該優化 macro per-item mAP 嗎？ | **不建議原樣當唯一目標**；修形後可當主指標 | 400 倍的 per-row 權重差＋最冷 item 只有 100 個正例的雜訊（§9.3） |

---

## 4. 先把問題拆成三層

「透過理解模型來改模型」其實是三個不同的子問題，需要的證據不同：

| 層 | 問的是 | 需要的證據 |
|---|---|---|
| **結構分析** | 模型這個函數長什麼樣（切點、葉值、容量花在哪個 item） | model.txt 就夠 |
| **行為歸因** | 模型在某個母體上的輸出與錯誤長什麼樣、被什麼驅動 | 有標籤的代表性樣本＋模型 |
| **訓練動態** | 訓練為什麼把函數學成這樣（梯度競爭、葉預算、權重效應） | 訓練資料統計；必要時重訓記錄 |

**現象1、現象2 都是第二層（行為）的觀測**；它們的**成因**在第三層；第一層只是佐證來源。你的提問三層都碰到了，最大的風險是拿第一層的工具（讀樹、importance）去回答第二層的問題——那是粒度錯配（見假設1、2 的裁決）。

**現象1 與現象2 該分開處理嗎？該。** 一個是水準問題、一個是條件判別力問題：觀測方法不同（校準檢定 vs 判別力指標）、混淆因子不同、處方不同（水準可以在推論階段修、不用重訓；條件判別力只能動訓練）。但**報表要合在一起看**：每個 item 同時報水準與判別力，排成 2×2 象限——因為指標是兩者的交互作用。一個「分數平坦但水準高」的 item 自己的 AP 反而好、卻是現象1 的加害者；「平坦且水準低」才是現象2 的典型受害者（§5.5 有數字）。兩個數各自單獨排序，會漏掉這層交互。

---

## 5. 逐條裁決

### 5.1 假設1：「看懂每棵樹怎麼長」——不成立（逐棵這個粒度）

不成立的理由是機制性的，不是「幾百棵讀不完」：

- **單棵樹沒有獨立意義。** 模型是 $F = F_0 + \nu\sum_m f_m$；因為 shrinkage，每棵樹只是「當時殘差方向」的一小步修正。第 173 棵樹的形狀取決於前 172 棵留下什麼殘差——同一個最終 $F$ 可以由完全不同的樹序列組成。讀懂一棵樹，讀到的是訓練路徑的偶然，不是資料的性質。
- **粒度不對。** 「item $A$ 的分數整體偏高」是 $F$ 在整個母體上的平均性質，散在幾百棵樹的葉值加總裡，任何單棵樹上都看不到。

**什麼情況下部分成立**：把樹的結構統計**跨樹聚合、按 item 記帳**，是有用的第三層（訓練動態）證據，而且只需要 model.txt。兩個具體可算的帳：

- **個人化預算帳**：每個 item 被 item-id 切點隔出來之後，它的子樹裡還有多少用 context 特徵（年齡、資產…）的切點、累積多少 Gain。手冊3 Ch4 的預測是：冷門 item 的個人化切點會被葉預算競爭餓死——如果帳本顯示冷門 item 隔出來之後**幾乎沒有後續切點**，就是「先驗有修、個人化沒學」的結構鐵證，直接支撐現象2 的成因。
- **先驗吸收帳**：item-id 的切點出現在第幾棵樹、貢獻多大——驗證手冊3 Ch2「先驗便宜、前幾棵樹就吸收」在你的模型裡是否成立。

失敗條件（逐棵讀反而可行的情況）：樹數個位數、深度極淺、或有 monotone constraints 的玩具模型。你的場景不在此列。

### 5.2 假設2：「只丟 model.txt」——不成立

現象1、2 都是（模型 $F$）×（輸入分佈）×（標籤 $y$）三者合起來的性質，model.txt 只給你第一項：

- **現象2 完全觀測不到**：「正負例分數重疊」需要標籤，model.txt 裡沒有任何標籤。
- **現象1 幾乎觀測不到**：「整體偏高」是相對於「該多高」而言，「該多高」＝該 item 的實際購買率——又需要標籤。

一個容易誤會的中間地帶要說清楚：model.txt 存有每個節點的 cover（訓練時流過的樣本數），TreeSHAP 的 `tree_path_dependent` 模式就是拿它當訓練分佈的代理——所以「只靠 model.txt 也能算一種 SHAP」是真的（§9.2）。但那只是訓練分佈沿樹路徑的邊際計數，沒有標籤、也不代表你要評估的母體。結論：**你必須補那份有標籤的代表性樣本**。這不是 nice-to-have——它是這兩個現象的定義的一部分。你的直覺是對的，這裡只是把理由釘死。

### 5.3 假設3a：現象1 的因果鏈——成立，附兩個修正

**你的推理本體是對的。** 「per-query 同量平移不變」說的是：對**同一個 query 內所有候選**加同一個常數，query 內次序不變。現象1 不是這種平移——它是 **per-item** 的偏移：item $H$ 在**所有 query** 的分數都被抬 $\delta_H$。query 內的次序由分數差 $s(q,H) - s(q,C)$ 決定，per-item 偏移直接改變這個差。用貫穿範例看：把 Alice 的三個分數同加 10（$12.0, 11.5, 10.0$），次序絲毫不動；但若 $H$ 沒被抬高（$s_H = 1.0$ 而不是 $2.0$），Alice 的 $C$ 就從第 2 升到第 1，precision 從 0.5 變 1.0——她那筆對 $C$ 的 AP 貢獻**翻倍**。把分數寫成「query 效應 + item 效應 + 剩餘」：$s(q,j) = c_q + \delta_j + \varepsilon(q,j)$，指標對 $c_q$ 完全免疫、對 $\delta_j$ 與 $\varepsilon$ 全額敏感——per-item 偏移恰好就是平移不變性**豁免不掉**的那個分量。

**修正一：壓在上方的若是「真的買了」的 item，不造成傷害——而且這件事的頻率要在對的母體上量。**

貫穿範例已經演過：同樣是 $H$ 壓在 $C$ 的正例上方，Alice（$H$ 是假正例）的 precision 是 0.5，Bob（$H$ 是真正例）的是 1.0。所以現象1 的實際傷害量是：

$$\text{傷害頻率} \approx P\big(H \text{ 壓在 } C \text{ 的正例上方，且 } H \text{ 在該 query 是負例}\big)$$

關鍵是這個機率要在哪個母體上算。**指標只由「$C$ 有正例」的 query 貢獻（§0），所以相關母體是「$C$ 的買家」，不是全體客戶。** 在全體 10M query 裡 $H$ 的正類率是 $40000/10\text{M} \approx 0.4\%$，但「買了 $C$ 的人裡有多少也買了 $H$」是**交叉購買率**——買家本來就是活躍客群，這個條件機率幾乎必然高於 0.4%，可能高很多。它多高是實證問題，要在有標籤樣本上直接量。方向上：除非交叉購買率非常高，$H$ 壓上來時多半仍是假正例、傷害仍大部分成立——但**別用 0.4% 這種邊際數字**下結論，那是條件化錯了母體。

**修正二：「邊際分佈整體偏高」這個觀測訊號，會抓錯犯人也會放走犯人。**

傷害發生在**具體的 query 內**：某 item 的分數是否在「$C$ 的正例 query 裡」高過 $C$。跨所有 query 混在一起看的分數直方圖（邊際分佈），跟這件事可以脫鉤。造一個具體反例：客群分年輕／退休兩半，$C$ 的買家全在退休群，$C$ 的正例分數 1.5。

- item $H$：年輕群分數 3.0、退休群 1.0 → 邊際平均 **2.0**，直方圖上很顯眼。但在 $C$ 的正例 query（退休群）它只有 1.0 < 1.5，**一次都沒壓到 $C$**。
- item $Y$：年輕群 1.2、退休群 1.6 → 邊際平均 **1.4**，直方圖上不顯眼。但在退休群它是 1.6 > 1.5，**每一筆 $C$ 的正例都被它壓**。

看邊際直方圖，你會去查 $H$、放過 $Y$——抓錯犯人。此外，名次類指標對分數是階梯狀的：$C$ 的正例 1.5、壓它的人 1.6 時，把對方往下修 0.05（變 1.55）名次一格都不動，修 0.2 才翻轉；而對方若是 3.0，同樣修 0.2 什麼都不變。**傷害不與偏移量成比例**，取決於有多少「分數差」落在偏移量以內。

所以正確的觀測單位是 **per-query 的名次事實**，例如：item $j$ 佔據 top-1 的 query 比例 vs 它的正類率；或「item $j$ 以負例身分壓在其他 item 正例上方」的次數統計。邊際分數直方圖只能當初篩。

### 5.4 假設3b：「偏高」vs「正常熱門」——用 per-item 校準檢定分，結果有三種情況

**先講判斷基準從哪來（通用原理）。** `binary_logloss` 是 proper scoring rule——它的最小值落在真實機率上，所以在沒有扭曲的訓練分佈上，模型的分數是在**估購買機率**。這給了「該多高」一個明確的參照：**如果 item $j$ 的預測機率和它實際的購買率一致，它排前面就只是忠實反映「它真的比較多人買」——那是正確的熱門，不是 bug。**

**檢定方法**：在有標籤樣本上按 item 分組，比較「模型平均預測機率」vs「實際購買率」，在 log-odds 尺度上看差距（機率尺度在 0 附近被壓縮，差距不好比；log-odds 尺度上偏移是可加的常數，乾淨）。可再按分數分位或客群分層細看。例：$H$ 實際購買率 0.4%，模型平均預測 2% → odds 差約 5 倍，log-odds 尺度偏高 $\log 5 \approx 1.6$——這 1.6 就是「偏高」的量，可以直接跟 §5.3 的名次翻轉幅度對起來。這種「按組別檢查校準」在文獻裡叫 group-wise calibration，一般化的版本是 multicalibration（Hébert-Johnson et al., 2018）。

**檢定結果分三種情況，處方完全不同：**

| 情況 | 證據長相 | 定性 | 處方 |
|---|---|---|---|
| **(1) 人為偏移** | 校準差距 ≈ 能從配置算出來的量。對 item $j$ 的負類欠採、保留率 $r_j$，未做修正時分數在 log-odds 尺度**必然**偏高 $-\log r_j$（推導見手冊3 Ch10；例：$r_j = 0.1$ → 偏高 2.3，比大多數 query 內的分數差都大）。sample_weight $w_j$ 造成同方向偏移（手冊3 Ch8、手冊2 Ch11 有定性提醒） | 不是模型學壞，是**配置的數學後果** | 推論期把偏移原路減掉：在 log-odds 分數上加回 $\log r_j$——這個修正在文獻裡叫 **logQ 修正**（King & Zeng, 2001；Yi et al., 2019），後文提到 logQ 都指這件事。或者直接修訓練配置 |
| **(2) 正確的熱門** | 校準差距 ≈ 0 | 不是 bug | 若 macro 指標仍不滿意 → 那是指標的要求，見 (3) |
| **(3) 指標要求的再平衡** | 模型完全校準，但 macro 指標還是低 | **指標與「按機率排序」的目標錯位**，不是模型缺陷 | per-item 常數偏移在驗證集上對指標直調（§5.6 的 offset sweep），或 item-aware weight 在訓練端做同一件事 |

情況 (3) 需要多解釋一句，因為它反直覺：**就算模型把每個機率都估得完全準，按機率排序也不是 macro per-item mAP 的最優解。** 按機率排序最優化的是 per-query 的 precision 類期望指標（probability ranking principle；Robertson, 1977。對 AP 的延伸在 query 內獨立假設下成立——這句是我的推論，信心中高）。但 macro 給冷門正例 400 倍的 per-row 權重（§0），要最大化它，最優排序必須把冷門 item 相對機率**故意**往上搬。換句話說：**「修好校準」和「優化 macro」是兩個互斥的方向**，你得選一個當主人。

**對你的專案最重要的一句話：情況 (1) 不是假想。** 你們已經在用 per-item 負採樣與 sample_weight（sampling editor 的雙因子權重），所以「現象1」的第一嫌疑人是自己的配置。這是整條診斷鏈裡最便宜、最確定的一步：把現行 $r_j$、$w_j$ 換算成理論偏移量，跟實測的 per-item 校準差距對帳。帳沒對完之前，觀測到的任何「偏高」都可疑——你可能花數週研究一個一行 offset 就能修掉的東西。

### 5.5 假設3c：「item 內正負重疊」與 per-item mAP——只是相關，兩個方向都推不出對方

**「重疊 ⇒ mAP 差」不成立。** 造一個極端例：item $D$ 的分數是常數 2.5（正負例完全同分佈，item 內 AUC 恰為 0.5——「不可分」的極致）。在 Alice 那種 query（其他人最高 2.0）裡，$D$ 永遠排第 1——$D$ 自己的每個正例 precision@1 ＝ 1，**$D$ 的 AP 接近滿分**；同時它把所有人都壓了一名，是現象1 的加害者。反過來 item $E$ 分數恆為 0.5：一樣 AUC = 0.5，正例永遠墊底，AP 爛。**同樣的「重疊」症狀，指標一好一壞**——差別全在水準。這就是 §4 說「水準×判別力交互」的具體數字。

**「mAP 差 ⇒ 重疊」也不成立。** item $j$ 自己的判別力可以完好（正例 query 裡分數確實升高），但升幅不夠越過競爭者在那些 query 的分數——mAP 照樣差。病不在它自己身上，在別人的水準。

**機制上為什麼只是相關**：per-item mAP 由「item $j$ 在**它的正例 query 裡**的分數 vs **同 query 其他 item** 的分數」決定；item 內 AUC 比的是「item $j$ 在正例 query vs 負例 query」的分數。**指標從頭到尾沒做過後面這種比較**——$j$ 的正例列和負例列分屬不同 query，永遠不會在同一個排序裡相遇。兩者只在「其他 item 的分數對 $j$ 的正例／負例 query 大致同分佈」時才近似掛鉤。

**那該對誰歸因？先對指標，症狀當第二步的機制線索。** 操作順序：

1. 先用 §5.6 的替換實驗，把每個 item 的 mAP 缺口拆成「水準造成」與「條件判別力造成」兩塊——這步直接在指標上做，沒有代理誤差。
2. 確認某 item 的缺口主要來自條件判別力之後，**再**用 item 內 AUC／分佈重疊當「它自己沒學到 context 條件化」的證據，往訓練動態層追（葉預算餓死？特徵缺失？）。

順序反過來的風險：你會去「把 AUC 修好」，但修好 AUC（分數拉開了、水準卻不對）不保證指標回升；反之亦然。

### 5.6 你要驗的拆解 lens：成立——而且它就是第一個該做的實驗

你的提案：把 item $j$ 的分數拆成「item-marginal（跨 context 的平均水準）」＋「context-conditional（隨 context 變動的部分）」，前者測現象1、後者測現象2。裁決分兩層：

**作為指標上的替換實驗：成立，而且可判定。** 關鍵觀察：**水準可以事後操縱，條件判別力不能。**對推論分數加 per-item 常數 $\delta_j$（22 個可調的數）是零訓練成本的干預：它改變 query 內跨 item 的次序，但完全不動任何 item 內部「哪個 query 分數高」的結構。於是有一個乾淨的判定實驗——**offset sweep（per-item 常數偏移搜尋）**：在驗證集上調 $\{\delta_1,\dots,\delta_{22}\}$，讓 macro mAP 最大（維度低，座標下降或網格都可行）。讀法：

- offset 就能收復大部分指標缺口 → 卡住指標的是**水準錯位**（現象1 這型），處方在校準／加權／後處理層，**可能根本不用重訓**。
- offset 收不回的部分 → **只能**來自條件判別力（現象2 這型），處方必須動訓練（權重／資料／特徵）。
- 順帶得到現象1 傷害量的直接度量：$\text{macro mAP}(\delta^*) - \text{macro mAP}(0)$。
- 注意過擬合：冷門 item 的 $\delta_j$ 只由 ~100 個正例的驗證訊號估出，sweep 會擬合驗證集的雜訊——$\delta$ 要向 0 收縮（正則化）或用折外資料挑，並連同 §9.3 的信賴區間一起讀，否則「offset 收復了缺口」可能只是擬合了噪音。

**當成模型內部的真實結構：不行，它只是投影。** GBDT 學到的 $F$ 在 (item, context) 上有交互作用；「平均水準」依賴你選哪個母體來平均；而且指標在乎的是條件部分**相對於同 query 競爭者**的值，不是它孤立的變異量。現象2 的本質恰恰是「item × context 的交互項對冷門 item 缺失」。所以：用這個 lens 組織診斷，可以；把兩個成分當成模型裡真的存在的兩個零件、分頭去做特徵歸因，不行。

**現象1、2 是同一根因嗎（正面判斷）：部分成立——同一個上游，兩條不同的中介，必須分開治。**

- 共同上游是**冷熱懸殊**（$P_j$ 的量級差）。但往下走是兩條路：**水準**走「base rate → per-item 先驗＋採樣/加權扭曲」這條**便宜路**——手冊3 Ch2 證過先驗 $O(M)$ 刀就修好、幾乎保證會被學到；**條件判別力**走「葉預算競爭 → 冷門個人化切點餓死」這條**貴路**（手冊3 Ch4）。同一個 $P_j$，一條路保證通、一條路保證堵。
- 你陳述的同一根因假設（「模型過度依賴 item 身分：給高 intercept、又不隨 context 變動」）**對受害側（冷門 item）大致成立**——手冊3 預測的正是「intercept 有、slope 無」這個輪廓。但**對加害側（熱門 item）不成立**：熱門的高水準多半是正確的 base rate（或人為偏移），它的條件判別力通常學得很好——葉預算都在它那裡。一個機制講不了兩邊。
- **怎麼區分「同一根因」與「巧合並存」**：(a) 對每個 item 算兩個數——水準差（校準差距）與判別力缺口（item 內 AUC 對某個基準的差），各自對 $P_j$ 畫散佈圖。葉預算的故事預測「判別力缺口隨 $P_j$ 變小而單調惡化」；人為偏移的故事預測「水準差能被 $r_j / w_j$ 的配置數學解釋掉」。兩條關係各自成立、殘差互不相關 → 這是「同一上游、兩條獨立中介」，不是單一機制。(b) 干預測試：offset sweep 只動水準；item-aware weight 同時動兩者。若加權後判別力缺口收斂、而水準按手冊3 Ch8 預期的方向移動，兩條因果線就被分開驗證了。

---

## 6. 輔助指標該怎麼算：per-query 平均還是全域 pooled（你直接問的）

同一個 AUC，聚合方式不同，量到的東西不同——選錯聚合，對現象1 就是瞎的：

| 聚合方式 | 具體做法 | 對現象1 | 對現象2 | 用途 |
|---|---|---|---|---|
| **全域 pooled** | 所有列混一起算一個 AUC / AUC-PR | 敏感 | 敏感 | 只當總體健康粗指標。它把兩個現象和正類率結構全混在一起，**不能歸因** |
| **per-query 算完再平均** | 每個 query 內算，再對 query 平均（mean AP、GAUC；GAUC 是業界慣例，見 Zhou et al., 2018） | 敏感（query 內競爭正是現象1 的作用面） | 敏感 | **與部署一致的結果指標族**。注意：你的 macro per-item mAP 用同樣的 query 內名次，但外層是**對 item 等權**、不是對 query 平均——對現象1 敏感的機制相同，聚合單位不同（這正是 400 倍權重的來源） |
| **per-item 跨 query pool** | 只取 item $j$ 的列、跨 query 算 item 內 AUC | **完全不敏感**——它從不跨 item 比較，per-item 常數偏移被整個除掉 | **只**量這個 | 現象2 的專用儀表 |

推論鏈：item 內 AUC 好、但該 item 的 AP 差 → 問題在水準／競爭（現象1 側）；item 內 AUC 差 → 它自己沒學到條件化（現象2 側）。**pooled 與 item 內的落差本身**，就是「跨 item 分數可比性壞掉程度」的量表。

跨 item 互比時的地雷：AUC-PR / AP 的隨機基線約等於正類率（Saito & Rehmsmeier, 2015；Davis & Goadrich, 2006），冷熱 item 的正類率差 100 倍，AP 差可能全是基線差。**跨 item 比判別力，用 item 內 ROC-AUC（基線恆 0.5、與正類率無關），不要用 AUC-PR。**

---

## 7. 主流途徑盤點：你的切角在哪

「透過理解模型行為來優化模型」的既有路線，按「回答什麼問題」分五族：

| 途徑 | 回答 | 代表 | 成熟度 |
|---|---|---|---|
| **子母體發現（slice discovery）／error analysis** | 錯誤集中在**哪個**子母體 | SliceLine（Sagadeeva & Boehm, 2021）、Slice Finder（Chung et al., 2019）、Domino（Eyuboglu et al., 2022） | 主流 |
| **對 loss 做特徵歸因** | 這些錯是**哪些特徵**造成的（單列層級） | TreeSHAP 的 `model_output='log_loss'`（Lundberg et al., 2020）；你規劃中的 shaprx 就在這族 | 確立、但用熟的人少 |
| **訓練資料歸因（influence）** | **哪些訓練樣本**造成這個行為 | influence functions（Koh & Liang, 2017）；GBDT 專用：Sharchilev et al.（2018）、系統評測 Brophy et al.（2023, JMLR；repo `jjbrophy47/tree_influence`）；TracIn（Pruthi et al., 2020）、Data Shapley（Ghorbani & Zou, 2019） | 研究活躍；10M 列的規模下成本高 |
| **data-centric 修復迴路** | 診斷完，動資料／權重 | 加權與再平衡（手冊2/3 的整個第二部）；「最差群體」目標的訓練端對應物 Group DRO（Sagawa et al., 2020）；標籤雜訊用 cleanlab（Northcutt et al., 2021） | 主流實務 |
| **排序指標專屬的退化分析** | rank 指標為何掉、誰造成 | 最薄的一族。既有零件：lambdarank 的 λ 梯度本身就是「指標對每一對分數的敏感度」（Burges, 2010）；LambdaLoss 給了 pairwise 代理損失與指標的界關係（Wang et al., 2018）；SVM-MAP（Yue et al., 2007）；「熱門該不該佔前排」的規範面歸流行度偏差／曝光公平文獻（Abdollahpouri et al., 2017；Singh & Joachims, 2018）；排序模型的 Shapley 歸因有零星研究（RankingSHAP、ShaRP——**需查證**，研究階段） | **偏門** |

**你的定位：偏門但有價值，不是死路。**「以 item 為歸因鍵、query 為排序單位的結構化分析，接到訓練槓桿」這個組合——前四族各覆蓋一角、沒有一族原生表達它；第五族最貼近但最薄。兩條確定的死路別踩：逐棵讀樹（§5.1）；對整個 rank 指標做 per-feature Shapley 分解（§9.5）。一個必須選邊的岔路：保留 macro 指標，就別同時追求「修好校準」——情況 (3) 的再平衡本質上是**故意的失準**（§5.4），兩個目標互斥。

---

## 8. 重造輪子檢查

### 8.1 已有現成件的部分

| 你想做的事 | 現成件 | 備註 |
|---|---|---|
| 單列分數／loss 的特徵歸因 | `shap` 套件、LightGBM 內建 `pred_contrib` | 成熟，別自己寫 TreeSHAP |
| loss 歸因＋錯誤分群＋處方（單列層） | **你自己的 shaprx 規劃**已覆蓋 binary pointwise 這層；本題的訓練 loss 恰好就是 binary logloss，單列層可直接沿用 | 載體未定沒關係，這層不必重想 |
| 子母體自動發現 | sliceline（PyPI 有維護的實作）、cleanlab | 可拿「per-row 指標傷害量」（§8.2 第 3 件）當目標欄餵它 |
| 按組別的指標報表 | Fairlearn `MetricFrame`，或 pandas groupby 幾行 | 不值得造 |
| GBDT 的訓練樣本 influence | `jjbrophy47/tree_influence`（Brophy et al., 2023） | 規模是障礙；當研究工具，不是日常件 |
| per-item 校準檢定 | groupby＋log-odds 差，用定義寫即可 | 不用工具 |
| 專案內已有的 | training diagnostics 已有 SHAP 母體選樣、top@1 名次象限與案例圖（PR#94–#97）；sampling editor 已有雙因子權重基建 | 象限機器與 item-aware weight 的施工面已存在 |

### 8.2 扣掉之後真正剩下的（值得做，但比你想的小）

不是「一套排序版可解釋工具」。剩三件，全在**指標層與對帳層**，不在歸因數學層：

1. **指標上的替換實驗工具**：對你這個自定義指標實作 (a) offset sweep（§5.6）；(b) 成分替換——把 item $j$ 的分數換成校準常數、重算指標，量它對別人的傷害；把競爭者換成基準、量 $j$ 自己的缺口。$M = 22$ 讓「逐一替換」完全可行（22 次指標評估），這是對「rank 指標不可加」的務實替代（§9.5）。沒有任何現成件懂你的指標。
2. **配置對帳器**：把現行 $r_j$、$w_j$ 換算成理論分數偏移，跟實測 per-item 校準差距對帳（§5.4 情況 (1)）。純算術，但沒人會替你寫。
3. **per-row 指標傷害訊號**：借 λ 梯度的定義（不用訓練，只做會計）——對驗證集算每一對「排錯的 (i,j)」的 $|\Delta\text{AP}_{ij}|$，得出「誰壓了誰、代價多少」的成對帳本，再把 per-row 傷害量當目標欄接給子母體發現工具。這是把第五族嫁接到前四族現成工具上的黏合劑。

### 8.3 誠實條款

這三件的「新穎度」都不高——是組合與對帳，不是新方法。真正無法外包的兩段：「特徵缺失 → 該補什麼特徵」（§9.6，本質是領域知識）與指標定案本身（§10 第 1 項，是價值判斷）。如果你期待做出「通用排序診斷框架」級的東西，先降預期：真正只有你能做、也只有你需要做的，來自「指標是自定義的」這件事本身。

---

## 9. 盲點檢查

### 9.1 importance 偏誤

split 次數型 importance 偏向高基數／連續特徵（Strobl et al., 2007 的實證，隨機森林脈絡）；gain importance 有不一致問題——特徵的真實貢獻增加、gain 排名反而可能下降（Lundberg et al., 2018 以此論證改用 SHAP）。對本題的具體影響：**「是什麼讓 item $j$ 偏高／不可分」不要用全域 importance 回答**。你的 item id 是 22 類的低基數類別特徵，跟連續 context 特徵放同一張 importance 表上先天吃虧，會系統性低估 item 身分的角色。替代：在「item = $j$」的子母體上聚合 per-row SHAP，或直接用 §5.1 的按 item 記帳 Gain 帳本。

### 9.2 TreeSHAP 兩個變體 × 你的輸入條件（接假設2）

- **`tree_path_dependent`**：**只需要 model.txt**（用存在模型裡的 cover 統計當背景分佈）。代價：對相關特徵會把貢獻抹到「路徑上出現過」的特徵上，且背景固定是訓練分佈的路徑摘要，不是你要評估的母體。
- **`interventional`**：對模型的實際函數形式忠實（"true to the model"；Chen et al., 2020；因果角度的論證見 Janzing et al., 2020），但**必須提供背景樣本**——正好就是你要補的那份代表性樣本。
- **`model_output='log_loss'`**：歸因對象從分數換成單列 loss，額外需要**標籤**。你在 shaprx 規劃裡選的 interventional + log_loss 組合，用在本題的單列層同樣正確。
- 本題的特殊用法：診斷「item $j$ 為何不可分」時，把背景樣本取成「item = $j$ 的子母體」——你要問的是「在這個 item 內部，哪個 context 特徵沒把正負分開」，用全域背景會混進跨 item 的差異。

### 9.3 目標指標本身：該不該優化 macro per-item mAP（正面回答）

**不建議原樣當唯一優化目標；修形後可當主指標。** 三個彼此獨立的理由：

1. **統計功效。** 最冷 item 的 AP 由 ~100 個正例估出，而每個正例的指標權重是熱門正例的 400 倍（§0）——幾筆雜訊就能搬動整個 macro。不建信賴區間之前，指標的期間差異無法解讀；而且區間要用**以 entity 為單位**的 bootstrap（同一客戶跨期的列高度相關，按列重抽會嚴重低估變異）。
2. **激勵形狀。** macro 等權隱含「每個 item 的商業價值相等」——22 個產品裡最小眾的那個，值不值得跟換匯等權？這是業務判斷不是數學。既然指標是你自選的，就把權重函數當顯式參數：$w_j \propto 1$（macro）到 $w_j \propto P_j$（≈ 傳統 per-query mean AP）之間有整條光譜（$\sqrt{P_j}$、$\log P_j$、業務價值權重）。另外考慮**截斷**：你的 AP 沒截斷，排第 10 的正例照算分，但業務只推前幾名——AP@K 才對齊使用面。
3. **可及性天花板。** 最冷 item 只有 100 個正例是資料層的事實（手冊3 Ch11 的天花板），macro 把這段「誰也治不了的部分」放大進主指標，會把你推向對噪音調參。務實修形：(a) 設最低正類數門檻，低於門檻的 item 移出 macro、單獨列觀察名單；(b) 或對 per-item AP 做收縮（往全體平均縮）再平均。

**指標與槓桿的耦合警告**：item-aware weight／負採樣比例，實作的就是指標隱含的 item 權重。指標沒定案就開始調槓桿，等於對著會漂移的靶射擊——換一次指標，全部重調。

### 9.4 base rate／正類率的比較陷阱

- 跨 item 比「不可分程度」：用 item 內 **ROC**-AUC（基線恆 0.5），不要用 AUC-PR / AP（基線 ≈ 正類率，冷熱差 100 倍，見 §6）。
- 你的指標還有一層 query 端的正類率差：不同 item 的正例落在「query 內共有幾個正例」不同的 query 裡。§5.3 的 Bob 例子顯示，上方的真正例會**抬高** precision 的分子——活躍客群（多正例 query）的 precision 有天然加成。跨 item 比較時這是混淆項；要比純能力，對每個正例報「贏過多少同 query 的競爭者」（成對勝率）比報 precision@rank 乾淨。
- 「偏高」的比較同理：比 log-odds 尺度的校準差距（§5.4），不比分數絕對值——絕對值高可能只是購買率真的高。

### 9.5 rank 指標不可加，attribution 的數學怎麼站住

你的懷疑正確：**SHAP 的可加分解對象是「單一列的分數或 loss」，把它直接當成「某 item 跨 query 的名次型集合量」的分解，數學上不成立**——per-item mAP 對分數是階梯函數（幾乎處處梯度為零）、跨列不可加、且依賴整個 query 的聯合分數。四條正路，各自的成立條件與成本：

| 路線 | 做法 | 成立條件 | 成本 | 對本題的判斷 |
|---|---|---|---|---|
| **(a) 歸因給可加的代理損失** | 對 query 內的成對 logistic loss（RankNet 型）或 λ 加權版做 SHAP | 代理與指標方向要一致——代理跟指標的界關係有 LambdaLoss 框架背書（Wang et al., 2018），界不緊時會歸因錯對象 | 低 | 可用，當單列層工具 |
| **(b) 成對帳本** | 對驗證集的每一對排錯的 $(i,j)$ 記 $\lvert\Delta\text{AP}_{ij}\rvert$——λ 梯度的定義就是這個量（Burges, 2010），只做會計、不訓練 | 指標能寫成「成對交換的敏感度」的和（AP、NDCG 都可以） | 低（每 query 只有 22 個候選） | **首選**：與指標零代理誤差，直接得出「誰壓了誰、代價多少」 |
| **(c) 對集合做 Shapley** | 把「保留哪些 item 的分數、其餘換基準」當合作賽局 | 特徵函數（把誰換成什麼基準）要先定義，有任意性；精確算要 $2^{22}$ 個組合 | 高 | **用 22 次「逐一替換」的替換實驗回答同一個問題**，別做全 Shapley |
| **(d) 訓練資料歸因** | influence 類方法找「哪些訓練列造成這個行為」 | 針對可微的訓練 loss（pointwise logloss：成立）；到 rank 指標還隔一層 | 高（10M 列） | 保留給點狀問題，例如「冷門 item 那幾筆正例是否綁架了模型」 |

### 9.6 歸因 → 行動的斷層（正面判斷）＋ 觀測→槓桿對照表

**這個鴻溝存在、你問對了——但它不是均勻的。** 整條鏈按段判斷：

| 觀測到什麼（左欄都是可量測的證據） | 該動的槓桿 | 這段鏈的強度 | 依據 |
|---|---|---|---|
| per-item 校準差距 ≈ 配置能算出的量（$-\log r_j$、$w_j$ 效應對得上） | 推論期 offset／logQ 修正，或修訓練配置 | **閉式，最強**——有公式，沒有推論鴻溝 | King & Zeng, 2001；手冊3 Ch10 |
| 校準正確，但 macro 指標仍要求把冷門往上搬 | offset sweep 後處理；或 item-aware weight 在訓練端做同一件事 | **強**——直接對指標優化 | §5.4 情況 (3)；流行度偏差的 re-ranking 一族 |
| 判別力缺口隨 $P_j$ 變小而惡化＋Gain 帳本顯示冷門 item 子樹沒有個人化切點 | item-aware weight、熱門負類欠採（配 logQ 修正）；HPO 搜索範圍給先驗（`min_data_in_leaf` 上限別開太高、`num_leaves` 下限別太小） | **方向可靠、幅度不可預測**——手冊3 的 Gain 算術給方向，但改善多少必須實驗；沒有快的驗證迴路這段就斷 | 手冊3 Ch4／Ch8／Ch10 |
| item 有正例、也分到了切點預算，item 內 AUC 仍低；用「item = $j$」當背景的 SHAP 顯示沒有任何 context 特徵在該子母體有區分力 | 補特徵 | **斷層最大**——現有方法（slice、SHAP）只能告訴你「缺訊號、缺在誰身上」（縮小搜索範圍），**沒有任何方法告訴你該造什麼特徵**；這段本質是領域知識 | 子母體發現工具全族都止步於「在哪」 |
| 冷門 item 買家的關鍵特徵，其變異被全域分箱壓進同一個 bin（可在有標籤樣本上量：該子母體的特徵值範圍 vs bin 邊界） | 分箱（`max_bin`／`min_data_in_bin`）調整 | **次要，證據前置**——沒量到左欄的證據前別動 | 直方圖分箱是全域建的，冷門子母體的解析度犧牲是可能但未證的機制 |
| （沒有 per-item 的對應觀測——正則化旋鈕是全域的，冷熱需求互斥，per-item 診斷映射不到 per-item 的 HPO 動作） | HPO 範圍只能當搜索空間的先驗（同上第三列），不能當處方 | **弱** | 手冊3 Ch9 |

一句話總結這張表：**水準問題（現象1 型）的鏈條幾乎是閉式的——對帳、加 offset、或對指標直調；條件判別力問題（現象2 型）的鏈條方向可靠但幅度要靠快驗證迴路量；「該補什麼特徵」是真空段，方法只能縮小搜索範圍。HPO 與分箱是輔助旋鈕，不是處方。**

另一個容易被低估的耦合：**動判別力的槓桿（加權／欠採）會同時搬動水準**——手冊3 Ch8 提醒過加權會讓輸出機率系統性偏高。所以每輪動完訓練槓桿，水準修正（offset）要重做一次。這就是「現象1、2 部分同根因」在操作面的實際代價。

### 9.7 你沒問、但更該問的四件事

1. **訓練目標與指標的錯位要不要一起上桌？** 你列的槓桿全在 pointwise 框架內，但手冊4 已論證：by-query 的 lambdarank 會中和「共池稀釋」那條軸（手冊4 Ch6 的術語：冷門的少數正例在全域 loss 總和裡被千萬列淹沒），而且 λ 的 $|\Delta|$ 權重可以換成 AP 型（Yue et al., 2007）、甚至掛上 per-item 權重去逼近你的 macro 指標。也許你有工程理由不換 objective（要機率、基礎設施），但「不換」應該是**顯式決策**，不是清單漏了。
2. **指標要不要 K 截斷**（§9.3 第 2 點）。
3. **per-item 指標的統計功效**（§9.3 第 1 點）——不建信賴區間就開始迭代，前幾輪的「改善」大概率是噪音。
4. **評估母體的時間結構**：query = entity × time，同一 entity 跨期高度相關；任何重抽或顯著性檢定要以 entity 為群組單位，否則變異被低估、假訊號變多。

---

## 10. 動手做任何工具之前，先想清楚的事（按優先序）

1. **指標定案**：權重函數（macro～micro 光譜取哪點）、K 截斷、最低正類數門檻或收縮、信賴區間協定。**不先想清楚的代價**：槓桿實作的就是指標的隱含權重——指標一換，所有調參作廢；而且冷門 item 的雜訊會被你當訊號追好幾輪。
2. **配置對帳先行**：把現行 $r_j$、$w_j$ 換算成理論偏移，跟實測 per-item 校準差距對上（§5.4 情況 (1)）。**代價**：不做的話，可能花數週「診斷」一個自己配置造成、一行 offset 就能修的東西。這是全鏈裡期望值最高的一步。
3. **建指標層的快驗證迴路**：有標籤代表性樣本＋macro mAP 計算件＋offset sweep，秒級到分鐘級。**代價**：本環境的瓶頸是驗證成本；沒有快迴路，§9.6 第三列「幅度要實驗」的每個假設都得用一次真訓練來驗，迭代空間歸零。
4. **凍結現象的操作化定義**：每個 item 固定報四個數——校準差距（log-odds 尺度）、item 內 ROC-AUC、per-item AP、對他人的傷害帳（替換實驗或成對帳本）——排成 2×2 象限報表。**代價**：定義不凍結，「偏高」「不可分」在每次討論裡漂移；正類率混淆讓跨 item 比較失真。
5. **劃定與 shaprx 的邊界**：單列層的 loss 歸因與處方是 shaprx 的疆域；指標層的替換實驗與成對帳本是本題的新增層。**代價**：不劃界會做出兩個半套的重疊工具。

---

## 參考文獻

- Abdollahpouri, H., Burke, R., Mobasher, B. (2017). Controlling Popularity Bias in Learning-to-Rank Recommendation. RecSys.
- Brophy, J., Hammoudeh, Z., Lowd, D. (2023). Adapting and Evaluating Influence-Estimation Methods for Gradient-Boosted Decision Trees. JMLR.（repo: `jjbrophy47/tree_influence`）
- Burges, C. (2010). From RankNet to LambdaRank to LambdaMART: An Overview. MSR-TR-2010-82.
- Chen, H., Janizek, J., Lundberg, S., Lee, S.-I. (2020). True to the Model or True to the Data? arXiv:2006.16234.
- Chung, Y., Kraska, T., Polyzotis, N., Tae, K.H., Whang, S.E. (2019). Slice Finder: Automated Data Slicing for Model Validation. ICDE.
- Davis, J., Goadrich, M. (2006). The Relationship Between Precision-Recall and ROC Curves. ICML.
- Eyuboglu, S., et al. (2022). Domino: Discovering Systematic Errors with Cross-Modal Embeddings. ICLR.
- Ghorbani, A., Zou, J. (2019). Data Shapley. ICML.
- Hébert-Johnson, Ú., Kim, M., Reingold, O., Rothblum, G. (2018). Multicalibration: Calibration for the (Computationally-Identifiable) Masses. ICML.
- Janzing, D., Minorics, L., Blöbaum, P. (2020). Feature Relevance Quantification in Explainable AI: A Causal Problem. AISTATS.
- King, G., Zeng, L. (2001). Logistic Regression in Rare Events Data. Political Analysis.
- Koh, P.W., Liang, P. (2017). Understanding Black-box Predictions via Influence Functions. ICML.
- Lundberg, S., Erion, G., Lee, S.-I. (2018). Consistent Individualized Feature Attribution for Tree Ensembles. arXiv:1802.03888.
- Lundberg, S., et al. (2020). From Local Explanations to Global Understanding with Explainable AI for Trees. Nature Machine Intelligence.
- Northcutt, C., Jiang, L., Chuang, I. (2021). Confident Learning: Estimating Uncertainty in Dataset Labels. JAIR.（cleanlab）
- Pruthi, G., Liu, F., Kale, S., Sundararajan, M. (2020). Estimating Training Data Influence by Tracing Gradient Descent. NeurIPS.（TracIn）
- Robertson, S.E. (1977). The Probability Ranking Principle in IR. Journal of Documentation.
- Sagadeeva, S., Boehm, M. (2021). SliceLine: Fast, Linear-Algebra-based Slice Finding for ML Model Debugging. SIGMOD.
- Sagawa, S., Koh, P.W., Hashimoto, T., Liang, P. (2020). Distributionally Robust Neural Networks for Group Shifts. ICLR.（Group DRO）
- Saito, T., Rehmsmeier, M. (2015). The Precision-Recall Plot Is More Informative than the ROC Plot When Evaluating Binary Classifiers on Imbalanced Datasets. PLOS ONE.
- Sharchilev, B., Ustinovskiy, Y., Serdyukov, P., de Rijke, M. (2018). Finding Influential Training Samples for Gradient Boosted Decision Trees. ICML.
- Singh, A., Joachims, T. (2018). Fairness of Exposure in Rankings. KDD.
- Strobl, C., Boulesteix, A.-L., Zeileis, A., Hothorn, T. (2007). Bias in Random Forest Variable Importance Measures. BMC Bioinformatics.
- Wang, X., Li, C., Golbandi, N., Bendersky, M., Najork, M. (2018). The LambdaLoss Framework for Ranking Metric Optimization. CIKM.
- Yi, X., et al. (2019). Sampling-Bias-Corrected Neural Modeling for Large Corpus Item Recommendations. RecSys.（logQ 修正）
- Yue, Y., Finley, T., Radlinski, F., Joachims, T. (2007). A Support Vector Method for Optimizing Average Precision. SIGIR.
- Zhou, G., et al. (2018). Deep Interest Network for Click-Through Rate Prediction. KDD.（GAUC 聚合慣例）
- **需查證**：RankingSHAP（listwise 排序模型的特徵歸因，約 2024 arXiv）；ShaRP（Shapley for Rankings，約 2024 arXiv）。僅確認方向存在、細節未核實，引用前請自查。
