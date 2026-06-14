# 可行性評估：per-大類模型 ＋ rank model（two-stage stacking）vs 單一 shared model

**狀態**：可行性評估（brainstorming 產出，待使用者審閱後再議要不要走、走哪條路線）
**日期**：2026-06-02
**動機**：冷熱門產品不平衡（使用者選定）
**關係文件**：
- 手冊3《多 item 共享模型下的冷熱門不平衡》設計 `docs/superpowers/specs/2026-06-01-gbdt-multiitem-imbalance-design.md` —— 本文是它「後續跟進（手冊4 範疇）」的**工程落地對應面**。
- 既有不平衡槓桿：`docs/superpowers/plans/2026-05-18-cold-product-weighting-mechanism.md`、`2026-05-31-gbdt-class-imbalance.md`。
- LTR 已落地的部分：`docs/superpowers/plans/2026-05-18-configurable-hpo-phase1-lambdarank-group.md`。

> **一句話結論（先講誠實的）**：技術上可行，而且專案已有的 LTR / 四分割 / `ModelAdapter` 抽象讓「乾淨切換」比想像中便宜。但動機是冷熱門不平衡，而專案**已有三個參數性槓桿在打同一個問題**；two-stage 是結構性大改，其增益**尚未被證明**大過「把現有槓桿調到底」。因此建議把「路線 3 基準線」當成必跑的對照組，再決定是否長期維護 two-stage。

---

## 1. 背景：現況是「單一 shared model」

事實（讀碼確認，非臆測）：

- **一個 LightGBM**，把 `prod_name` 當 categorical 特徵；輸入 = 客戶特徵 + 產品。`ModelAdapter` ABC 是「單一模型」抽象（`src/recsys_tfb/models/base.py`）。
- 產物是**一個目錄** `data/models/${model_version}/`：`model.txt`（booster）＋ `model_meta.json`（sidecar，記 algorithm / calibrated）＋（若校準）`calibrator.pkl`。存讀由 `ModelAdapterDataset`（`src/recsys_tfb/io/model_adapter_dataset.py`）＋ `CalibratedModelAdapter` 負責。
- **LTR 基礎建設已經有了**：`is_ranking_objective` / `to_contiguous_groups` / `extract_Xy_with_groups`（`core/group_utils.py`、`io/extract.py`），A7 一致性檢查擋 ranking objective 配錯 metric。目前用在「單一 shared model 切 `lambdarank`/`rank_xendcg`」，query group =（snap_date, cust_id）。
- **不平衡槓桿已有三個**（都在不改結構的前提下作用）：
  1. `training.hpo_objective: macro_per_item_map`（**現值**）——HPO 評分每產品等權，冷門不被熱門淹沒。
  2. `training.sample_weights`（composite-key，可對單一冷門產品 boost）。
  3. zero-positive downsample editor（PR #57）＋ 手冊3 的方法論。
- **大類表只活在 evaluation**：`conf/base/parameters_evaluation.yaml` 的 `product_categories.mapping`（dev 合成資料：`fund`=3、`exchange`=2、`ccard`=3，其餘 `unmapped: singleton`），只用來把細產品 collapse 成大類跑第二輪 metrics report。**目前完全不是 training 概念。**
- **model_version** 只 hash `training:` block（`core/versioning.py`）；config 一致性閘 A1–A13（`core/consistency.py`）是不變量唯一真實來源。

---

## 2. 候選結構與設計演進

| 候選 | Stage-1 形狀 | 對冷熱門不平衡的對策 | 否決 / 採用 |
|---|---|---|---|
| 全 per-item（22 獨立 binary） | 每產品一個 model | 各產品完全隔離 | **否決**：冷門產品 positives 太少 → 高變異、易過擬合，反而更不穩（手冊3 Ch7「雙重稀少」的工程版） |
| **per-大類 point-wise shared ＋ rank**（採用） | 每個產品大類一個 `binary` shared model（同類產品靠 `prod_name` 特徵區分、**類內 pooling**），上接一個 LTR rank model 做跨類校準/排序 | 類內共享統計強度、類間互不干擾，rank 層補跨類可比性 | **採用**：兼顧「隔離」與「不至於 positives 太少」 |
| 單一 shared model（現況基準線） | 一個 model 管全部 | 靠 §1 的三個參數槓桿 | **保留為對照組** |

### 採用結構的資料流（概念圖）

```
                    ┌─ Stage-1: 大類模型（point-wise, binary） ─┐
 客戶×產品 特徵 ───►│  M_fund(X)   →  s_fund                     │
                    │  M_exchange(X) → s_exchange                │──► 每列得到「該列所屬大類的分數」 s_cat
                    │  M_ccard(X)  →  s_ccard                    │
                    │  M_<singleton>(X) → ...                    │
                    └───────────────────────────────────────────┘
                                      │
                                      ▼
                    ┌─ Stage-2: rank model（lambdarank, query=客戶） ─┐
 每列輸入 = [ s_cat , 客戶層特徵 ] ──►│  R([s_cat, cust_feats]) → 最終分數         │
                    └─────────────────────────────────────────────────┘
                                      │
                                      ▼
                         rank_predictions（現有節點，照分數在 22 產品間排序）
```

關鍵：**Stage-1 每列只取「自己所屬大類模型」的輸出**（路由，不是全部跑），Stage-2 對每列輸出一個 scalar，最終排序仍交給現有 `rank_predictions`。

---

## 3. 兩個橫切的硬問題（不論走哪條路線都要先解）

這兩點決定 blast radius，必須在路線選擇前想清楚。

### (A) Leakage —— Stage-2 的特徵是 Stage-1 的預測

若 Stage-1 在 `train` 上訓練，又用**同一批 `train`** 的預測當 Stage-2 特徵 → 嚴重 leakage（Stage-1 對 train 過度自信，Stage-2 學到的權重在新資料崩掉）。標準解是 out-of-fold（OOF）。

> **好消息**：專案**已有現成的乾淨分割**可直接接 OOF，幾乎零額外複雜度：
> - `train` / `train_dev` / `val` / `calibration` 四個 split 已存在；
> - `finalize_model` 註解明載 **train 與 train_dev 是「customer-disjoint by sampling design」**（同一 query group 不跨兩 split）。
>
> 於是最便宜的 leakage-clean 流程：
> 1. **Stage-1 大類模型** 在 `train` 訓練。
> 2. **Stage-2 rank model** 的特徵 = Stage-1 對 **`train_dev`** 的預測（Stage-1 沒看過）＋ `train_dev` 的 label；早停 / HPO 在 `val`。
> 3.（選配）最終 refit：Stage-1 在 train+train_dev 重訓、Stage-2 用 cross-fit 或保留 val 當 OOF。
>
> 這把 two-stage 最大的理論風險，降級成「重用既有 split 的編排問題」。**這是整個可行性的關鍵 enabler。**

### (B) Inference chunking —— rank model 的輸入要不要跨產品？

現有 inference `predict_scores`（`pipelines/inference/nodes_spark.py`）**按 (snap_date, prod_name) chunk**，每 chunk 算出該產品所有客戶的分數。這個 chunking 是 10M×22 規模下控記憶體的關鍵。

- **若 Stage-2 每列輸入只含「自己大類的分數 ＋ 客戶特徵」**（推薦）：lambdarank 在**推論時是 pointwise 輸出**（每列獨立打分，group 只在訓練時需要）。於是 inference 仍可按產品 chunk：每 chunk 路由到該產品的大類模型 → Stage-2 → scalar。**inference 幾乎不用改 chunking 結構。**
- **若 Stage-2 輸入含「其他產品的分數」**（跨產品相對特徵，如「本產品分數 − 客戶最高分」）：就需要**先把同一客戶 22 列組裝起來**才能算特徵 → 現有 per-產品 chunking 被打破，inference 要改成 per-客戶組裝。blast radius 大很多。

> **建議**：Stage-2 採「每列 = 自身大類分數 ＋ 客戶特徵」的 pointwise-input 設計，**保住 inference chunking**。跨產品相對特徵列為 later 實驗，不進第一版。

---

## 4. 三條落地路線 ＋ 逐檔 change-impact map

### 路線 1 — Composite adapter（推薦）

核心：新增 `CompositeModelAdapter`，**對外仍實作 `ModelAdapter` ABC**（一個 `predict()`、一個 `save()/load()` 到同一個 version 目錄），內部持有 `{大類: Stage-1 booster}` ＋ 一個 Stage-2 rank booster。`get_adapter()` / 訓練流程依 `training.model_structure` 決定建單一 `LightGBMAdapter` 還是 `CompositeModelAdapter`。複雜度（訓練 N 子模型 ＋ OOF ＋ rank）封裝在 adapter / 一個新訓練子節點內。

**要動的檔：**

| 檔案 | 改動 | 規模 |
|---|---|---|
| `src/recsys_tfb/models/composite_adapter.py`（新） | 新 adapter：`train` 編排「N 大類模型（train）→ 對 train_dev 預測 → rank model」；`predict(X)` 路由每列到其大類模型再過 rank；`save/load` 序列化 N+1 booster ＋ 大類表到 version dir | 大（核心） |
| `src/recsys_tfb/models/base.py` ＋ adapter 註冊 | 註冊 `composite`；`get_adapter` 取得 | 小 |
| `src/recsys_tfb/pipelines/training/nodes.py` ＋ `pipeline.py` | `tune_hyperparameters` / `finalize_model` 要能走 composite 分支（或新增 `train_composite_model` 節點）；OOF 需要 `train_dev` handle（已在 DAG 內，現成）；HPO 對 N+1 模型的搜尋策略 | 中–大 |
| `core/versioning.py` | `model_structure` ＋ 大類表納入 `training:` hash payload（否則切結構不 bump model_version → 災難） | 小但**必做** |
| `core/consistency.py` | 新增不變量：大類表覆蓋所有 `schema.item` 值且與 inference.products 對齊（A14-ish）；`model_structure` ∈ 合法集合；composite 下 calibration 語義（見 §6） | 中 |
| `io/model_adapter_dataset.py` | `model_meta.json` 記 `model_structure` 與大類表，load 時還原 composite | 小–中 |
| `models/calibrated_adapter.py` | 決定校準掛在 Stage-1（per 類、機率）還是整體關掉（見 §6） | 中 |
| `pipelines/training/diagnostics.py` | SHAP / importance 目前假設單一 booster；composite 要 per-子模型（或只診斷 Stage-1） | 中 |
| `conf/base/parameters_training.yaml` | 新增 `training.model_structure` ＋（若抽出）大類表 ＋ Stage-1/Stage-2 各自的 objective/search_space | 小 |
| `conf/base/parameters_evaluation.yaml` | 大類表改為「引用單一真實來源」而非自帶 mapping（單一真實來源化） | 小 |
| `pipelines/inference/nodes_spark.py` | 若採 §3(B) pointwise-input → **幾乎不動**（`model.predict` 介面不變）；adapter 內部自行路由 | 小 |
| tests | 新 adapter 單元測試、versioning 測試、consistency 新不變量測試、composite save/load round-trip | 中–大 |

**blast radius**：收斂在「adapter 邊界」內。inference / evaluation / catalog / `rank_predictions` 因為只呼叫 `model.predict()`，**大致不動**。這是推薦它的主因。
**最大坑**：(1) `predict(X)` 要能從 X 還原每列的大類（X 裡 `prod_name` 是編碼後 int，需 adapter 帶 item-column index ＋ 大類表解碼）；(2) save/load 多檔序列化；(3) model_version hash 一旦漏掉大類表 → 不同結構共用版本號。

### 路線 2 — Pipeline 層 fan-out（DAG 顯式分叉）

training pipeline 本身分叉成 N 個大類訓練子 DAG ＋ rank 訓練節點，結構在 Ploomber DAG 上可見；catalog 為每個大類模型開 artifact；inference 也 fan-out。

**要動的檔**：上面全部 ＋ `pipelines/training/pipeline.py` 大改成「資料相依的動態 DAG」（N 隨大類數變）、`conf/base/catalog.yaml` 多 artifact 條目、`pipelines/inference/pipeline.py` ＋ `nodes_spark.py` 多模型載入與組裝、`__main__.py` 的 model 載入路徑。

**blast radius**：最大。和現在「處處假設一個 model」正面衝突（catalog 的 `model:` 單條、`predict_scores(model, ...)` 單一入參、`ModelAdapterDataset` 單目錄）。**不建議作為第一步**——除非未來要對各大類模型獨立排程 / 獨立重訓 / 獨立觀測，那時 fan-out 的透明度才值回票價。

### 路線 3 — 不重構，把現有槓桿推到底（**必跑的基準線**）

維持單一 shared model：

1. `sample_weights` 對冷門大類/產品系統化加權（手冊3 Ch8 的工程版，已支援）。
2. `hpo_objective: macro_per_item_map`（已開）。
3.（選配，小改）**Stage-2-lite**：對單一 shared model 輸出的分數，做一層「per-大類 post-hoc 單調/排序重校準」（每類一個 isotonic / rank transform），不真的開大類模型。這是「rank model」概念的最小代理，幾乎只在 inference 後處理加一個節點。

**要動的檔**：`parameters_training.yaml`（調權重）＋（選配）一個輕量後處理節點 ＋ 其測試。**不動** model_version 結構、adapter、inference 主結構。

**blast radius**：極小、無 leakage 問題。
**侷限**：拿不到「不同大類用不同特徵動態」的潛在增益（若該增益存在）。

---

## 5. 切換設計（你要的「使用者可切換」）

```yaml
training:
  # shared（現況，預設） | per_category_plus_rank（two-stage）
  model_structure: shared
  # 僅 per_category_plus_rank 時生效：
  category_stage:
    objective: binary          # Stage-1 大類模型
    metric: binary_logloss
  rank_stage:
    objective: lambdarank      # Stage-2 rank model
    metric: ndcg
    inputs: [category_score]   # 第一版：自身大類分數＋客戶特徵（保住 inference chunking）
```

- **與 model_version**：`model_structure` ＋ `category_stage` ＋ `rank_stage` ＋ **大類表** 全部必須進 `training:` hash payload（`core/versioning.py`）。否則切換結構不 bump 版本號 → inference 載到結構不符的 artifact，靜默錯誤。
- **與 consistency**：新增不變量——`model_structure` 合法值；per_category 時大類表必須覆蓋 `schema.categorical_values[item]` 全集且與 `inference.products` 對齊（沿用 A4/A6 的 item-set 思路）；rank_stage objective 必為 ranking（重用 A7）。
- **大類表單一真實來源**：把現在 evaluation 的 `product_categories.mapping` 抽到一個 training/eval 共用位置（如 `schema.product_categories` 或頂層），evaluation 與 training 都引用同一份，consistency 閘驗證一致。**這是即使最後不做 two-stage 也值得做的整理**（消除 evaluation 自帶 mapping 的孤島）。

---

## 6. Calibration 互動（容易被忽略的坑）

現況 `CalibratedModelAdapter` 把單一 model 的機率輸出做 isotonic/sigmoid 校準（`calibration.enabled: true, method: sigmoid`）。two-stage 下：

- **Stage-2 rank model（lambdarank）的輸出不是機率**，是排序分數。對它做機率校準語義不對。
- 兩個合理選擇：
  - **(i) 校準掛 Stage-1**：每個大類模型輸出校準過的機率，Stage-2 吃校準後機率當特徵（排序用途下其實非必要，但讓 Stage-2 輸入有一致尺度）。
  - **(ii) 整體關掉校準**：若最終用途純排序（本專案 evaluation 永遠是 per-customer mAP，排序用途），校準對 mAP 無影響，可在 two-stage 模式關閉。
- 需在 consistency 加規則：`model_structure: per_category_plus_rank` 時，`calibration` 的語義要嘛限定掛 Stage-1、要嘛強制關閉，不能沿用「校準整體 model」的舊假設。

---

## 7. Evaluation 衝擊（好消息為主）

- **per-customer mAP 不變**：evaluation 永遠對最終分數做 per-(snap_date, cust_id) mAP（`compute_test_mAP_spark` / `metrics_spark`），只看最終排序，不在乎模型內部幾段。two-stage 只要 `model.predict()` 給得出每列分數，evaluation **完全不用改**。
- **macro_per_item_map**（HPO 與報表都用）正好是評估 two-stage 是否真的幫到冷門的對的尺規——用它對照路線 3 基準線。
- **診斷**（SHAP / native importance / 分數分布）目前綁單一 booster，需改成 per-子模型或限定診斷 Stage-1。屬「報表豐富度」非「正確性」，可後補。

---

## 8. 誠實判斷與建議路徑

1. **技術可行性：是。** 專案的 `ModelAdapter` 抽象、現成四分割（OOF）、已落地 LTR、pointwise 推論特性，讓路線 1 的 blast radius 出乎意料地小。
2. **但增益未證明。** 動機是冷熱門不平衡，而 §1 的三個參數槓桿已在打同一問題；手冊3 的結論也明說共享模型內這些手段是「緩解非根治」，真正上限在「冷門 item 絕對正類筆數」。per-大類 pooling 是否突破這個上限，**得用資料證**，不能先驗假設。
3. **維護成本真實存在**：N+1 模型的 HPO、save/load、版本化、診斷、校準語義，都是長期負擔。

> **建議路徑（階段化、可隨時喊停）：**
> - **階段 0（必做、便宜）**：把大類表抽成 training/eval 單一真實來源 ＋ 進 consistency。即使不做 two-stage 也賺。
> - **階段 1（基準線）**：路線 3 推到底（sample_weights × 大類 ＋ 選配 Stage-2-lite 重校準），用 `macro_per_item_map` 量出冷門產品的天花板。
> - **階段 2（結構實驗）**：若階段 1 證明仍有明顯缺口，才實作路線 1 的 `CompositeModelAdapter`，用 `training.model_structure` 切換，和階段 1 同尺規 A/B。增益顯著且穩定才長期保留。
> - **路線 2 暫不考慮**，除非出現「各大類需獨立排程/重訓/觀測」的營運需求。

---

## 9. 待你拍板的未決問題

1. **rank model 輸入**：第一版鎖「自身大類分數 ＋ 客戶特徵」（保 inference chunking），還是要含跨產品相對特徵（inference 要改 per-客戶組裝）？（§3B）
2. **校準**：two-stage 下掛 Stage-1 機率，還是整體關閉？（§6）
3. **大類粒度**：用現有 evaluation 的 `fund/exchange/ccard + singletons`，還是 production 22 產品另定 canonical 大類表？singleton 大類（只有一個產品）其 Stage-1 模型 = 真的 per-item，要不要設「最小類大小」門檻把太小的類併回 shared？
4. **階段化是否接受**：先跑階段 0+1 基準線，再決定階段 2？還是直接要路線 1 的切換骨架？

---

## 10. 不在本文範疇（YAGNI）

- 各大類模型獨立排程 / 獨立重訓 / 獨立觀測（→ 路線 2，目前無需求）。
- 跨產品相對特徵、deep ranking、產品 embedding（第一版不做）。
- 手冊4 的**數學**內容（NDCG vs logloss 梯度、排序校準理論）——那在 handbook 寫，本文只談工程落地。
