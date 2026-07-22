# 診斷產物判讀手冊（CI 欄與 Gain 帳本）

`evaluation` 的診斷層現行有 4 項 registry 診斷（`config_shift`／`item_ability`／`model_capacity`／`suppression`），每一項都已改成**每頁自帶「範圍說明」的 self-documenting 頁面**，統一收在 `diagnosis/index.html`——那些頁面自己交代每個數字怎麼來、抽樣範圍多大、怎麼讀，不需要另一份手冊複述。

本手冊只補這幾項 self-documenting 頁面**沒涵蓋**的兩塊判讀：

1. **`metric_ci.json` 的 CI 欄**（§1）：report.html 主指標段與 per_item 歸因段的信賴區間怎麼來、怎麼讀。產物在 `data/evaluation/<model_version>/<snap_date>/diagnosis/`。
2. **訓練側的 `gain_ledger.json` 與條件化 SHAP 背景**（§2）：不看評估分數、改看模型內部把個人化容量分給了誰；`gain_ledger` 是 `model_capacity` 診斷頁的輸入之一，落在 `data/models/<model_version>/diagnostics/`。

方法論根源見 `docs/ranking-diagnosis-framework.md`；本文只講判讀。

## 名詞速查

讀內文時忘了某個詞，回來查這張表；每個詞的完整說明在括號指的節。

| 名詞 | 一句話定義 |
|---|---|
| `gain_ledger` | Gain 帳本：把每個產品被 id 切點隔出後、子樹內用了多少 context 切點與 Gain 記成帳，量個人化容量夠不夠（§2） |
| `context_gain_share` | 某產品分到的 context Gain 占「全產品 context Gain 總和」的比例；判餓死的主欄，低＝個人化容量少（§2.2） |
| `context_gain_isolated` | isolated 欄：只在「可達集合縮到只剩該產品」時才累積的 context Gain；產品少、樹淺時普遍偏小甚至為 0，是輔助資訊、勿當主判準（§2.4） |
| 餓死型 / 特徵缺失型 | 判別力差的兩種成因：沒分到容量學（餓死，看 `context_gain_share` 低）／有容量但沒有能分開的特徵（特徵缺失）（§2.1/§2.5） |

---

## 1. metric_ci.json 與 CI 欄

主指標段與 per_item 歸因段的 CI 欄回答的是：**指標數字有多少統計不確定性**。

- CI 是**抽樣估計**：在有正例的 query 上抽樣（上限與保底見 config `evaluation.diagnosis.sample`），對客戶（cust_id）整簇重抽做 bootstrap。樣本規模與 n_boot 印在報表描述——樣本小的估計不要當真。
- `n_pos(抽樣)` 欄＝該產品進入 CI 估計的正例列數；個位數代表該列 CI 不可靠，先看這欄再看區間。
- 觀察名單＝正例數低於 `evaluation.metric.min_positives` 而移出 macro 平均的產品；指標照列，只是不進等權平均。

## 2. 結構層（Gain 帳本與條件化 SHAP 背景）

這一節看的不是評估側的分數，而是**模型內部把容量分給了誰**。產物是訓練側的 `gain_ledger.json`（在 `data/models/<model_version>/diagnostics/` 下，跟 SHAP 診斷同一個目錄），report.html 不畫它；它是 `model_capacity` 診斷頁的輸入之一。

### 2.1 在回答什麼問題：切點預算

先講通用原理。梯度提升樹（GBDT）每棵樹是一連串「切點」（split）：在某個特徵的某個閾值把資料分兩邊，每個切點消耗一點模型容量、換回一點訓練損失下降（這個下降量就是這個切點的 **Gain**）。一個排序模型要把某個產品排好，靠的是「在這個產品的候選列裡，用客戶特徵（context 特徵：年齡、資產、往來紀錄……）把會買的跟不會買的分開」——也就是模型得在「這個產品」這個條件下，花切點去學客戶差異。

問題是：模型有沒有真的為每個產品都花了這個功夫？還是某些冷門產品被隔出來之後，子樹裡幾乎沒有後續切點、模型只學到「這個產品整體多熱門」（一個常數水準）就收工了？後者就是框架講的**餓死型**（starved）——訊號可能存在，但模型沒分到容量去學它。Gain 帳本就是來量這件事的：**把每個產品被「產品 id 切點」隔出來之後，子樹裡還用了多少 context 切點、累積多少 Gain**。

本框架的模型第一個特徵就是產品 id（`prod_name`，一個類別特徵），所以樹裡會出現「這個節點往左是 {基金、外幣}、往右是其他」這種**類別切點**。帳本從每棵樹的根往下走，一路記住「走到這裡，還可能是哪些產品」（可達集合），碰到產品 id 切點就把可達集合縮小、並標記「從這裡開始，後面的 context 切點是**在某個產品條件下**的個人化切點」；碰到 context 切點，就把它的 Gain 記給當時可達集合裡的每個產品。

補產指令（不必重訓，對既有模型切一個節點就好）：

```bash
python -m recsys_tfb training --env local --only-node compute_gain_ledger
```

它讀 `data/models/<model_version>/model.txt` 的樹結構，跑約半秒就寫出 `gain_ledger.json`。

### 2.2 輸出鍵逐一判讀

頂層三塊帳：

- `item_id`：產品 id 切點本身的帳。`split_count`（有幾個產品 id 切點）、`gain_sum`（它們的 Gain 總和）、`gain_share`（占全模型 Gain 的比例）、`tree_index_summary`（這些切點分佈在第幾棵樹——`min`=0 表示第一棵樹就開始用產品先驗，這是正常的：模型會先用最強的產品水準訊號）。
- `context`：**已條件化**（conditioned，路徑上已經過至少一個產品 id 切點）的 context 切點全帳，`split_count`／`gain_sum`／`gain_share`。這是「模型花在個人化上的總容量」。
- `per_item`：每個產品一列，七欄。最重要的是 **`context_gain_share`**——這個產品分到的 context Gain 占「所有產品 context Gain 總和」的比例，是判餓死的主欄。其餘：`context_split_count`（該產品的個人化切點數）、`context_gain`（累積 Gain）、`context_gain_isolated`（只在「可達集合只剩這一個產品」時累積的 Gain，見 §2.4 陷阱）、`isolating_split_count`（把這個產品隔出來的產品 id 切點數）、`first_tree_index`（第一次出現在第幾棵樹）、`trees_touched`（這個產品出現過的**所有**樹序號的排序清單——是一個 list，不是計數；要「總共幾棵樹」自己取長度）。

停用（`diagnostics.gain_ledger.enabled: false`）時只寫 `{"enabled": false}`。

### 2.3 示例走讀：冷門基金的餓死結構

真跑一個模型（161 棵樹、8 個產品）的 `per_item`，按 `context_gain_share` 由低到高排：

| 產品 | context_gain_share | context_gain | 個人化切點數 | isolated Gain | first_tree |
|---|---|---|---|---|---|
| fund_mix | 0.0419 | 7,799 | 800 | 765.9 | 0 |
| fund_bond | 0.0466 | 8,658 | 761 | 0.0 | 0 |
| fund_stock | 0.0508 | 9,438 | 701 | 93.8 | 0 |
| ccard_cash | 0.0525 | 9,753 | 761 | 81.4 | 0 |
| ccard_bill | 0.0621 | 11,555 | 706 | 311.7 | 0 |
| ccard_ins | 0.2398 | 44,578 | 787 | 1,832 | 0 |
| exchange_fx | 0.2490 | 46,289 | 801 | 507.1 | 0 |
| exchange_usd | 0.2574 | 47,861 | 829 | 556.2 | 0 |

**三個基金產品包辦倒數前三**：最冷的 `fund_mix`（正例最少）分到的個人化 Gain 占比只有 4.2%，而最熱的 `exchange_usd` 是 25.7%——差 6.1 倍。這不是切點**數**的差別（大家的 `context_split_count` 都在 700–830 之間，差不多），是每個切點**含金量**的差別：熱門產品的個人化切點切下去 Gain 大（訊號強、學得動），冷門基金的切點切下去 Gain 小（要嘛訊號弱、要嘛正例太少估不準）。這就是「先驗有修、個人化沒學」的結構證據——模型知道基金整體多冷（產品 id 切點記在 `item_id` 帳裡），但在基金內部分不太出誰會買。

### 2.4 陷阱

- **不要用 `context_gain_isolated`（isolated 欄）當主判準**。isolated 只在「可達集合縮到只剩一個產品」時才累積，但 8 個產品的樹通常很淺，一個切點的可達集合往往還剩兩三個產品（例如「基金三兄弟」還沒被切開），這種切點的 Gain 記進 `context_gain`（給集合裡每個產品都記）但**不**記進 `context_gain_isolated`。所以 isolated 欄普遍偏小、甚至為 0（上表 `fund_bond` 整支 isolated 是 0），它只是輔助資訊；判餓死一律看 `context_gain_share`。
- **`context_gain_share` 是相對量**，分母是「所有產品的 context_gain 加總」。它跟 `item_id.gain_share`（產品 id 切點占**全模型** Gain 的比例，此模型約 0.49）不是同一個分母、不能直接比——一個在問「個人化容量在產品間怎麼分」，一個在問「水準先驗 vs 個人化在整個模型裡各占多少」。
- **逐棵讀樹不是有效診斷**。單棵樹只反映訓練當下的隨機路徑，同一個模型函數可以由無數種樹序列組成；有意義的是跨全部樹聚合之後的帳，不是某一棵樹長什麼樣。

### 2.5 條件化 SHAP 背景：一個設計上保留、但目前版本做不到的選項

框架的「條件化 SHAP」診斷（方法論見 `ranking-diagnosis-framework.md`）是：對「某個產品的子母體」做 SHAP 歸因、**背景樣本也取自該子母體**（interventional 模式），回答「在這個產品內部，哪個客戶特徵有／沒有把正負例分開」——這是「餓死型 vs 特徵缺失型」的最後一道分辨（餓死是沒學，特徵缺失是根本沒有能分開的特徵）。

設定鍵是 `diagnostics.shap.background`，兩個值（注意此處 `per_item` 是 config 設定值，與 §2.2 `gain_ledger` 的 `per_item` 輸出鍵無關）：

- `global`（預設）：背景用整個訓練分佈的路徑摘要（`tree_path_dependent`），走模型原生的貢獻計算。這是現行行為。
- `per_item`：背景改取該產品的子母體，interventional 模式。

**但在目前釘的版本組合（SHAP 0.42.1 × LightGBM 4.6.0）下，`per_item` 對本框架的模型結構性做不到**。原因很具體：interventional SHAP 需要 SHAP 自己解析每棵樹的結構，而它的樹表示把切點閾值當成一個浮點數存——碰到 LightGBM 的類別切點（閾值長這樣：`"2||3||4"`，表示「類別 2、3、4 往這邊」），轉浮點數直接失敗。實測一個真實模型有 161 棵樹、其中 129 棵含類別切點解析失敗。由於本框架的模型第一個特徵就是產品 id 類別切點，`per_item` 在真模型上必然踩到這個。

實作上的處置：`per_item` 模式會先用幾列資料做一次能力探針，探針失敗就**整段降級回 `global` 行為**，並在 `shap_diagnostics.json` 的 `notes` 留一句說明（「per_item 背景已降級為 global：interventional TreeSHAP 在目前版本組合下無法解析類別切分」），訓練不會因此中斷。所以今天 `per_item` 這個選項是**參數空間保留**：設定看得到、config 檢查會擋非法值，但實際跑出來等同 `global`＋一句 notes。等未來 SHAP 版本支援 LightGBM 類別特徵的 interventional 歸因，這個降級就會自動消失、per-item 背景才真正生效。判讀上：今天的 SHAP 診斷一律當 `global` 背景讀，divergence 等欄位的語意不變。
