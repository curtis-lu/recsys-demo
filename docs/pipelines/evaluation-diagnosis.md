# 評估側診斷產物判讀手冊

report.html 裡的「主指標 CI」「per_item 歸因的 CI 欄」「對帳 Reconciliation」三段，以及 `data/evaluation/<model_version>/<snap_date>/diagnosis/` 下的 `metric_ci.json`、`reconciliation.json`，都由本手冊解釋。報表上的段落描述只是速查；**為什麼要看這些數字、數字背後發生了什麼事**，都在這裡。

方法論根源見 `docs/ranking-diagnosis-framework.md`（診斷框架手冊）；本文件只講「跑完 evaluation 之後，這幾張表怎麼讀」。

## 名詞速查（先掃這張表，後面各節是展開說明）

| 名詞 | 一句話定義 |
|---|---|
| log-odds（logit） | 機率 p 換算成 ln(p/(1−p))：10% → −2.20、50% → 0、90% → +2.20。整份對帳的加減都在這個尺度上做 |
| `gap` | logit(平均預測機率) − logit(實際正類率)，用**模型原始輸出** `score_uncalibrated` 算。正值＝模型高估這個產品、負值＝低估（§3） |
| `gap_calibrated` | 同一條公式，改用**校準後**分數 `score` 算。跟 `gap` 對照＝校準層修掉了多少 |
| 全局參考值 | 「config 中性」產品的 `gap` 中位數。量的是所有產品**共同**承受的評估母體效應（§4） |
| `gap_vs_global` | `gap` − 全局參考值：扣掉共同水準之後，這個產品**自己**偏了多少。verdict 看的是它 |
| `gap_calibrated_vs_global` | `gap_calibrated` − 校準版全局參考值。校準層有效時它應趨近 0 |
| `theory_min` / `theory_max` | 由你的抽樣／加權配置算出的理論偏移範圍（§2）。**配置完全沒動到的產品＝[0, 0]** |
| `residual` | `gap_vs_global` 超出理論帶 [theory_min, theory_max] 的部分；落在帶內＝0 |
| `verdict` | \|residual\| ≤ 門檻（預設 0.3）→ 可解釋；超過 → 不可解釋；`gap` 算不出來 → 無法評估（看 `reason`） |
| config 中性 | 該產品沒出現在任何 `sample_ratio_overrides`、也沒有 label 不對稱的 `sample_weights`——配置沒動它的正負比例 |
| 母體條件化 | 評估母體經過篩選（post-training 模式只含有買過任一產品的客戶），整體購買率被機制性墊高（§4） |
| post-training／monitoring 模式 | 訓練後拿 test 窗預測評估（你平常看的 report）／拿正式批次推論結果評估上線表現 |

---

## 1. 對帳層在回答什麼問題

先講通用原理，再套本專案。

**通用原理**：訓練資料的抽樣與加權，會刻意扭曲模型看到的正負樣本比例。比方說你把某產品的負樣本丟掉一半（保留率 retention r = 0.5），模型看到的世界裡「買的人」比真實世界多了一倍——它學到的購買機率就會整體偏高。這不是 bug，是你自己的配置造成的、**可以事先精確算出來**的效應（統計上叫 logQ 校正，見 GBDT 手冊 3 第 10 章）。

**對帳（reconciliation）的意思**：把「配置理論上會造成多少偏移」和「模型實際偏了多少」放在同一張表上對。

- 兩者對得上 → 模型的機率偏移**可解釋**：它就是你配置的直接後果，不用緊張，也不用動模型。
- 對不上 → 有配置以外的事情在發生（特徵漂移、資料問題、模型病灶），**值得追**。

這是診斷框架「判讀第 1 步」：先排除自己造成的偏移，再談模型有沒有問題。

## 2. 理論偏移怎麼算：一個數字的旅程

以本 repo 合成資料的實際配置為例。`conf/base/parameters_dataset.yaml` 裡有：

```yaml
sample_ratio_overrides:
  "mass|ccard_ins|0": 0.5      # mass 客群、ccard_ins、負樣本(label=0) → 只保留一半
  "affluent|ccard_ins|0": 0.9
  "hnw|ccard_ins|0": 0.8
```

負樣本保留 r=0.5、正樣本全留，等於模型看到的正:負比是真實世界的 1/0.5 = 2 倍。為什麼「比例乘 2」會變成「log-odds 加 ln 2」？勝算（odds）＝正的機率÷負的機率，欠採把負類砍到 r 倍，就是把 odds 除以 r；取對數後，「除以 r」變成「減 ln r」——所以模型在採樣資料上學到的 log-odds 比真實世界**高出 −ln r，而且這個位移量跟原本的機率是多少無關**（不管 p 是 1% 還是 30%，都是同一個常數上移）。這個「常數、跟 p 無關」的性質是整個對帳方法能成立的前提：§4 才能拿不同產品的位移互相比較。代回數字：r=0.5 → 上移 **−ln(0.5) = +0.693**；同理 affluent 的 0.9 → +0.105、hnw 的 0.8 → +0.223。

一般式（`reconciliation.json` 的 `theory.cells` 每一格都是這樣算的）：

```
offset = ln( (r_pos × w_pos) / (r_neg × w_neg) )
```

r＝抽樣保留率、w＝訓練權重（`training.sample_weights`）；正負類同率同權時抵消為 0——所以**全域的** `sample_ratio`（正負一起抽）不會產生偏移，只有「對 label 不對稱」的配置才會。

**為什麼產品層是「帶」不是單一數字**：override 的 key 是三維（客群|產品|label），同一個產品在不同客群的保留率不同。ccard_ins 三個客群的偏移分別是 +0.693 / +0.105 / +0.223，混在一起的產品層效應取決於各客群買家佔比（資料相關、config 算不出來），所以理論值只能給範圍。

**theory_min / theory_max 的確切算法**：把該產品出現在配置裡的每一個 cell（客群×產品×label 組合）各算一個 offset（上面的公式），**theory_min＝這些 offset 的最小值、theory_max＝最大值**。ccard_ins 的三個 cell offset 是 {+0.105, +0.223, +0.693} → `theory_min=0.105`、`theory_max=0.693`。cell 級明細在 `reconciliation.json` 的 `theory.cells`。

**為什麼大部分產品的帶是 [0, 0]**：這些產品沒出現在任何 `sample_ratio_overrides`、也沒有 label 不對稱的 `sample_weights`——配置完全沒動它們的正負比例，理論偏移就是 0，「帶」退化成單點 [0, 0]（＝「這個產品理論上不該偏」）。此時 verdict 的判準等於：`gap_vs_global` 自己要落在 ±門檻內。

這個帶是誠實的近似——**帶越寬，verdict 越寬容**（任何落在帶內的實測值都算可解釋），判讀時記得看一眼帶寬。

## 3. 實測差距怎麼量

每個產品算一個 **gap**：

```
gap = logit(平均預測機率) − logit(實際正類率)
```

白話：模型說這個產品平均有多少機率被買（p̄），實際上有多少人買（ȳ），兩者都換算成 log-odds 再相減。gap = +0.7 的意思是「模型的勝算估計比實際高了一倍」（e^0.7 ≈ 2）。

這個 gap 用的分數欄是 `score_uncalibrated`（模型原始輸出，尚未經過校準層）；為什麼不用校準後的 `score`，見 §5 判讀順序第 4 步。

誠實限制：嚴格說，logQ 位移是加在**每一筆**預測的 log-odds 上的，理想的量法是逐筆取 logit 再平均；但那樣就不能用一次 groupBy 聚合完成（工程取捨）。本文件用的是反過來的「先平均再取 logit」，兩者不相等（Jensen 不等式）——產品內分數越分散，差異越大。量級感：分數集中在 0.05±0.02 時兩種算法幾乎相同；分數從 0.01 散到 0.5 時，差異可到 0.1～0.2 log-odds 級。在本 repo 的判定門檻（0.3 log-odds）下通常是二階效應，但看到「分數分布很寬的產品剛好壓線」時要想起這件事。

## 4. 為什麼 verdict 看 gap_vs_global，不看 gap 本身

這是 Phase 2 真跑時撞到、然後修訂進設計的關鍵一課，用真實數字講。

第一次跑對帳時，**8 個產品的 gap 全部落在 −0.38 ～ −0.65**——包括 7 個完全沒有任何 override 的「配置中性」產品。如果 verdict 直接看絕對 gap，7 個健康產品全部會被判「不可解釋」。

追下去發現原因，先解釋一個詞：**post-training 模式**＝訓練剛結束、直接拿訓練 pipeline 在 test 窗留下的預測（`evaluation --post-training`）來評估；相對的是 **monitoring 模式**（拿正式批次推論的 `ranked_predictions` 評估上線後表現）。你平常訓練完看的 report.html 就是 post-training 模式的產物。

post-training 模式的評估母體**只包含 test 月有買過任何產品的客戶**（`training_eval_predictions` 只寫入這些人；本機合成資料是 654 位）。這群人的購買率被機制性墊高：每位客戶面對 8 個候選產品、且**保證至少買了一件**，所以平均每產品購買率至少 1/8＝12.5% 起跳（本機實測 ȳ 全體平均 19.2%）；而模型是照「全體客戶」（多數人什麼都沒買）校準的，它估的機率天生低於這個被篩選過的母體 → 所有產品的 gap 一起往下掉。這是**評估母體的選擇效應**，跟任何單一產品的校準品質無關。

所以 verdict 改成兩步：

1. 先估「共同水準」：拿配置中性產品（理論偏移＝0 的那些）的 gap 取中位數，當作**全局參考值**（global reference）。這些產品理論上不該偏，它們實際偏多少，量到的就是母體效應本身。本機實測 −0.462。
2. 每個產品看**相對偏離** `gap_vs_global = gap − 全局參考值`，再跟理論帶比。例：ccard_ins 的 gap=−0.133，vs_global = −0.133 − (−0.462) = **+0.329**，恰好落在理論帶 [0.105, 0.693] 內 → residual=0 → 可解釋。

交叉檢核線索（都在報表描述與 JSON 的 `global` 區塊）：`n_neutral_items`＝參考值靠幾個中性產品撐（越少越不穩，少於 3 個會直接退回 0、即原絕對語意）；`pooled_gap`＝另一種算法的全局水準（n_rows 加權合併），兩者量級應相近。**陷阱**：若某個「配置中性」產品其實有真實病灶，它會污染參考值——中位數對單一污染有抵抗力，但中性產品本來就少時要提高警覺。

## 5. 對帳表欄位逐一對照

| 欄位 | 怎麼算（自足定義） | 判讀 |
|---|---|---|
| `theory_min` / `theory_max` | 該產品在配置裡每個 cell 的理論 offset 取 min／max（§2）；配置沒動到的產品＝[0, 0] | 帶寬＝跨客群異質度；越寬 verdict 越寬容 |
| `gap` | logit(`p_mean`) − logit(`y_rate`)，分數用模型原始輸出 `score_uncalibrated` | 含母體效應，**不要**直接拿來判產品好壞 |
| `gap_vs_global` | `gap` − 全局參考值（config 中性產品的 gap 中位數，§4） | verdict 的依據：扣掉共同水準後產品自己偏多少 |
| `gap_calibrated` | 同 `gap` 公式，分數改用校準後的 `score` 欄 | 同樣含母體效應，看下一欄 |
| `gap_calibrated_vs_global` | `gap_calibrated` − 校準版全局參考值 | 校準層有效時，量級應明顯小於 `gap_vs_global`、趨近 0 |
| `residual` | `gap_vs_global` 超出 [`theory_min`, `theory_max`] 的部分；帶內＝0 | \|residual\| ≤ 門檻（config `explained_threshold`，預設 0.3）→ 可解釋 |
| `verdict` | 由 residual 與門檻決定：可解釋／不可解釋；`gap` 算不出來 → 無法評估 | 見下方判讀順序 |
| `reason` | verdict＝無法評估時的原因說明 | 只有無法評估的列才有值 |
| `p_mean` / `y_rate` / `n_rows` | 平均預測機率／實際正類率／評估列數 | n_rows 小的產品，gap 本身就吵 |

上表是逐產品的欄位；`reconciliation.json` 另有一個**全表層級**的 `global` 區塊（不在報表表格裡）：`reference`（全局參考值）、`n_neutral_items`（參考值靠幾個中性產品）、`pooled_gap`（交叉檢核用的另一種全局水準算法）、`reference_calibrated`（校準版參考值）。§4 的可信度檢查就看這裡。

**判讀順序**：

1. 掃 `verdict` 欄。全部「可解釋」→ 模型的機率水準沒有配置之外的異常，結束。
2. 有「不可解釋」→ 看它的 `gap_vs_global` 與理論帶差多遠、往哪個方向，對照該產品近期的配置改動與特徵變化。這是診斷框架後續步驟（offset sweep、行為層象限）的輸入。
3. 有「無法評估」→ 看 `reason`：要嘛該產品實際正類率退化（全買或全沒買，logit 算不出來），要嘛 config 有它的理論偏移但評估資料裡根本沒有它的列（下架？打錯產品名？）。
4. 主判欄用的是 `score_uncalibrated`（模型原始輸出）——因為校準層本身就在修水準，要看模型「原本」偏多少。monitoring 路徑沒有這個欄位時會自動退回 `score` 並在描述標註，此時 gap 內含校準層效應，判讀要打折。

## 6. 實例走讀：一次注入實驗

Phase 2 驗收時做過一次「已知答案注入」，完整走一遍上面的邏輯：

1. 對 `fund_bond` 的三個客群負樣本全部注入保留率 0.5 → 理論帶收斂成單點 [0.693, 0.693]。
2. 重訓重評。`fund_bond` 的絕對 gap 從 −0.648 移到 +0.042——**位移 +0.690，幾乎恰好等於 ln 2**。模型完整吸收了配置造成的偏移，理論公式定量命中。
3. `gap_vs_global` = +0.484，落帶附近（residual −0.209，門檻內）→ 可解釋。
4. `gap_calibrated` = −0.127：校準層把注入的偏移修掉大半——校準在做它該做的事的直接證據。
5. 還原配置重訓，`fund_bond` 理論帶回 [0,0]、對帳表回全綠。

這個實驗同時示範了對帳層的兩種用途：**正向**（配置的效應可預測、可驗證）與**反向**（哪天對帳表有產品不可解釋，你知道那不是配置造成的）。

## 7. metric_ci.json 與 CI 欄（Phase 1 產物）

主指標段與 per_item 歸因段的 CI 欄回答另一個問題：**這些指標數字有多少統計不確定性**。

- CI 是**抽樣估計**：在有正例的 query 上抽樣（上限與保底見 config `evaluation.diagnosis.sample`），對客戶（cust_id）做整簇重抽的 bootstrap。樣本規模與 n_boot 都印在描述裡——樣本小的估計不要當真。
- `n_pos(抽樣)` 欄＝該產品進入 CI 估計的正例列數，太小（個位數）代表該列 CI 不可靠，先看這欄再看區間。
- 觀察名單＝正例數低於 `evaluation.metric.min_positives` 而被移出 macro 平均的產品；它們的指標仍照列，只是不參與等權平均。

## 8. 已知限制一覽

- 產品層理論帶是跨客群聚合近似（§2）；cell 級精確值在 JSON。
- gap 的 logit(平均) 是近似（§3）。
- 全局參考值依賴「配置中性產品夠多、且沒有自己的病灶」（§4）。
- 公司規模（22 產品 × 10M query）的計算成本與 `theory.cells` 條數上限尚未實測（部署前另驗，spec §7）。
- 對帳只看「水準」（level）；「產品內排序品質」是另一軸，由行為層象限（Phase 3）處理。
