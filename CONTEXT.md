# recsys_tfb

通用的排序（learning-to-rank）批次建模框架：替每個 query group 把候選 item 依模型分數排出名次。商業銀行產品推薦只是示例部署，這裡的詞一律是框架的抽象說法。

版本 ID 與不變量代號的精確定義在程式碼的模組 docstring，本檔對它們只寫一句意思；兩邊有出入時以 docstring 為準（分工理由見 `docs/agents/domain.md`）。

## Language

### 欄位角色

**time**:
一次排序所屬的時段，也是資料分組、分區與切分的時間單位。設定鍵與落地產物沿用 `snap_date` 字樣（ADR-0017）。
_Avoid_: 事件時間、曝光時間（到秒的時間是 **event**）

**entity**:
一次排序是替誰排的那個對象，由一欄或多欄組成。
_Avoid_: 客戶、customer、user

**item**:
被排序的東西，恆為一欄；由多個屬性組成時，在來源 SQL 先拼成一欄。
_Avoid_: 產品、廣告、product

**event**（ADR-0021，尚未實作）:
同一個 time、entity、item 底下有多筆時，用來分辨每一筆的選用欄位角色，由一欄或多欄組成，例如事件 ID 或到秒的時間戳。
_Avoid_: row_key、請求鍵、曝光鍵

**label**:
一筆候選的答案，排序好不好以它為準。
_Avoid_: target、y

**score**:
模型給一筆候選的分數，是排出名次的依據。

**rank**:
一筆候選在所屬 query group 內依 score 由高到低的名次，從 1 起算。score 相同時按 item 由小到大排（照 item 本身的值比，數字就照數字大小）；這只讓名次可重現，不代表模型分得出高下。

### 排序的結構

**query group**:
一次排序的範圍，由 time 與 entity 決定；名次只在同一個 query group 內比較。
_Avoid_: session、清單

**候選**:
query group 裡被排出名次的一列；宣告 event 時，同一個 item 可以有多列。
_Avoid_: 推薦項

**identity**:
認出一筆候選的欄位組合：time、entity、item，宣告了 event 時再加上 event。
_Avoid_: 主鍵（來源表設定裡的 `primary_key` 是另一回事）

### 資料

**來源表**:
使用者自己產出、交給 dataset pipeline 的表：`sample_pool`（要排的候選列）、`label_table`（答案）與特徵表。

**特徵表**:
提供特徵的來源表；目前一個部署只有一張，以 time ＋ entity 接到候選列上（多張特徵表、各自宣告 join 欄位：ADR-0022，尚未實作）。

**行為紀錄**:
使用者用來往回計算特徵的原始紀錄，例如點擊、交易；它不是來源表，框架不直接讀。
_Avoid_: 事件紀錄、event log（會和 **event** 混淆）

**split**:
依 time 切出的資料段：`train`、`train_dev`（和 train 同時段、以 entity 互斥切出）、`val`、`test`。
_Avoid_: fold

**日期區間**:
time 設定的另一種寫法：起日、迄日、間隔（`{start, end, step}`），含頭含尾。設定一載入就展開成它代表的 time 值清單，所以與「加引號、依日期遞增」的逐一列出是同一份設定、同一個版本 ID。精確規則：`src/recsys_tfb/core/date_ranges.py` 模組 docstring。
_Avoid_: 時間窗、lookback（那是 baseline 往回看的月數）

**model_input**:
某個 split 組好、可以直接餵給模型的資料列。

**前處理器**:
在 train 時段上 fit 出來的特徵清單與類別編號表（產物 `preprocessor`、`category_mappings`）；training 與推論都重用同一份。

**類別欄**:
值是一組離散標籤的特徵欄；前處理器把每個標籤換成它在類別編號表裡的位置。能用的型別見不變量 B5（`src/recsys_tfb/core/consistency.py`）。
_Avoid_: 把 0／1 旗標叫「binary 欄」（在本 repo，binary 指 bytes 型別；0／1 旗標是布林或整數欄）

**item 清單**:
模型認得的 item 值的集合，是 item 類別編號的來源。
_Avoid_: 產品清單；也不要和 `inference.products`（離線推論的候選清單）混用

**month plan**:
一次執行中，某個增量產物要處理、要跳過哪些 time 值的清單；名字沿用月份字樣，實際單位是 time。

### 版本與不變量（只寫意思，精確定義見 docstring）

**base_dataset_version**:
dataset 產物的版本身分。精確定義：`src/recsys_tfb/core/versioning.py` 模組 docstring。

**train_variant_id**:
train 與 train_dev 抽樣設定的版本身分，位在 base_dataset_version 底下，是唯一的 variant 層（#411 移除了 calibration 那一層，`calibration_variant_id` 已不存在）。精確定義同上。

**model_version**:
模型的版本身分。精確定義同上。

**不變量代號**:
一致性規則的編號：A 系列檢查設定，B 系列檢查資料。精確定義與完整清單：`src/recsys_tfb/core/consistency.py` 模組 docstring 的 Invariant legend。

**資料閘**:
pipeline 裡專門檢查資料、本身不改資料的一步。dataset 有三個：開頭的 `validate_data_consistency`、編碼後的精度閘（B8）、最後的粒度閘（B10）。
_Avoid_: 資料驗證（太籠統，開跑前的設定檢查也算驗證）

### 流程與產物

**離線推論**:
對一整批 entity 的候選一次評分、排出名次並發布結果表的流程（`inference` pipeline）。
_Avoid_: 線上推論、即時評分（那是另一種推論）

**promote**:
由使用者人工把某個 model_version 設為離線推論預設版本的動作。

**交接包**（規劃中，尚未實作）:
training 產出、讓框架以外的評分系統重現特徵順序與類別編號的一份檔案。

### 評估

**post-training 模式**:
evaluation 以 `sample_pool` 當母體的評估模式。

**監控模式**:
evaluation 以 `inference_population` 當母體的評估模式。

**per-item attribution**:
把 query group 層級的指標拆成各 item 的貢獻，只在該 item 為正例的列上彙整；指標名帶 `_attr`（例如 `map_attr@K`），不是單一 item 自己的 mAP。

**大類**:
item 的分組，一個 item 只屬於一個大類（例如三種基金併成「基金」）；evaluation 會在大類粒度上把指標再算一次。
_Avoid_: segment、分群（那是 entity 的切片）

**分群**:
依 entity 的某個屬性把 query group 切片、各片分開算指標；設定鍵沿用 `segment` 字樣。
_Avoid_: 大類（那是 item 的分組）

**macro**:
以 item 為單位先算、再跨 item 平均的指標彙整方式。

**重抽單位**（ADR-0023，尚未實作）:
算信賴區間時一起被重抽的 entity 欄位子集。

**熱門度基準線**:
不看模型、只按每個 item 過去一段時間的正例數（或正例率）排出名次的對照組，用來回答「模型有沒有比只看熱門度好」。
_Avoid_: popularity baseline、購買數基準（正例不一定是購買）

**正例率**:
同一段時間裡，一個 item 的正例數 ÷ 它當過候選的次數。`sample_pool` 的一列就是一次當候選；`label_table` 的列數不是（它可能只收正例，或只收篩過的 entity）。
_Avoid_: 點擊率、申辦率、CTR（那是正例率在各示例裡的名字）

**校準**:
把 score 當成機率來看時，「模型說會發生的比例」與「實際發生的比例」有多接近。與排序好壞無關，兩者可以一好一壞。
_Avoid_: 準確度、準不準（太模糊，會被讀成排序）

**門檻**（ADR-0024，尚未實作）:
把 score 切成「預測為正」與「預測為負」的那個切點。排序本身不需要門檻——只有二元指標要。

**`pr_auc`**（ADR-0024，尚未實作）:
precision-recall 曲線下的面積，母體是全部候選列（依 ADR-0024 由分箱近似，內插規則與 sklearn 不同）。
_Avoid_: average precision、AP（排序的 AP 是 `map@K` 與 `ap_contrib@K`，意思不同）

**`roc_auc`**（ADR-0024，尚未實作）:
ROC 曲線下的面積，母體是全部候選列。
_Avoid_: 跟 `raw_within_item_auc`／`query_centered_auc` 混用（那兩個的母體是「只含有正例的 query」的診斷抽樣，不可與本項相比）
