# 設計：可切換的 two-stage stacking（per-grouping Stage-1 ＋ LTR Stage-2）

**狀態**：設計定案（brainstorming 產出，已與使用者逐題對齊四個核心決策，待使用者審閱 spec 後進實作計畫）
**日期**：2026-06-07
**關係文件**：本檔取代並重寫自較早的可行性探索 `docs/superpowers/specs/2026-06-02-per-item-rank-stacking-feasibility.md`（該檔內容不再作為依據；本檔由本次需求重新推導）。

---

## 0. 一句話

讓使用者用一個 config 開關，把「單一 shared 排序模型」切換成 **two-stage stacking**：Stage-1 對每個 grouping（item 自身，或產品大類）訓一個 point-wise 模型，Stage-2 一個 LTR（lambdarank）吃 Stage-1 分數 ＋ 客戶特徵、學跨產品最終排序。複雜度封裝在一個對外仍實作 `ModelAdapter` ABC 的 `CompositeModelAdapter`，**inference / evaluation / catalog / `rank_predictions` 不動**。

## 1. 動機與目標

通用原理（先講通用、再套專案）：

- **單一 shared 多 item 模型的冷熱門不平衡**：一個模型同時學所有 item，冷門 item 的正類絕對筆數少，容易被熱門 item 的統計強度淹沒，難訓。把建模拆成「先各自打分、再學跨群排序」的 stacking，是處理這類異質群體的標準手段之一。
- **避免 stacking 的 leakage**：Stage-2 的特徵是 Stage-1 的預測。若用「同一批訓練資料」的 Stage-1 預測當 Stage-2 特徵，Stage-1 對該批過度自信，Stage-2 學到的權重在新資料崩掉。標準解是 out-of-fold（OOF）/ cross-fitting。

套到本專案：

- 本框架的一大目的是**簡化建模流程、讓使用者彈性實驗各種模型調整與組合**。two-stage 是其中一種更進階的可選模式，預設仍是現況的單一 shared model。
- 示例規模下（~10M entity × 22 item），單一 shared model 的冷熱門不平衡明顯；two-stage 提供一條結構性的實驗路徑。

**成功標準**：

1. 能力面——`training.model_structure` 一鍵切換，end-to-end 跑得起來、artifact 自洽、版本號正確區分結構。
2. 度量面——以既有 `macro_per_item_map`（每 item 等權）對照單模型基準線，量得出 two-stage 對冷門 item 的影響。

## 2. 範圍

**做（v1）**

- `CompositeModelAdapter`（對外仍是 `ModelAdapter`）。
- Stage-1 grouping 抽象：`item`（per-item，每個 `schema.item` 值一個 binary）或 `category`（每大類一個 share 模型）。
- OOF K 折 cross-fitting（折鍵 = entity 雜湊互斥）產生 leakage-clean 的 Stage-2 訓練特徵。
- Stage-2 = lambdarank，**pointwise 輸入**（自身 grouping 分數 ＋ 客戶特徵 ＋ 可選 grouping id）。
- 版本化納入新結構；consistency 新增不變量；`product_categories` 抽成 training/eval 共用單一真實來源。
- 文件更新（README ＋ docs/，見 §13）。

**不做（YAGNI / future）**

- 跨產品相對特徵（會打破 per-product inference chunking，見 §10）。
- small-group 自動併回 shared（與「OOF 餵飽冷門稀疏正類」的動機相衝）。
- 逐子模型完整 HPO；pipeline 層 fan-out（多 catalog artifact、獨立排程）。
- 對 lambda ranker 做機率校準（保留未來最後一層疊加的彈性，見 §9）。
- composite 的 SHAP 全套診斷（先限定 Stage-1 或後補）。

## 3. 四個核心決策（已與使用者對齊）

| # | 決策 | 選定 | 理由 |
|---|---|---|---|
| 1 | leakage 切割 | **OOF K 折 cross-fitting** | 資料效率最足，對冷門 item 稀疏正類最友善（直球打主要動機）；leakage 最乾淨。接受多訓 K 次的編排成本（訓練不定期手動執行，成本可接受）。 |
| 2 | Stage-2 輸入 | **pointwise（自身分數＋客戶特徵）** | lambdarank 推論是 pointwise 打分，故 inference 仍可按 `(snap_date, prod_name)` chunk，**保住 10M×22 的記憶體架構**。 |
| 3 | 整合路線 | **CompositeModelAdapter** | inference/evaluation/catalog/`rank_predictions` 只呼叫 `model.predict()`（`ModelAdapter` 是 god node #2，98 edges），複雜度收斂在 adapter 邊界，blast radius 最小。 |
| 4 | 預設結構 | `shared`（現況），grouping 預設 `category` | 純加值、opt-in，不影響既有使用者。 |

## 4. 架構與資料流

```
[train pool]  ── K 折（依 entity/cust 雜湊互斥切）──┐
   每折 k：在 train\fold_k 訓 Stage-1 各 grouping 模型 → 對 fold_k 打分
        └── early-stop valid = train_dev（customer-disjoint，固定）
   ▼
OOF 預測（覆蓋整個 train，每列分數都來自「沒看過該客戶」的 Stage-1）
   ▼
Stage-2 (lambdarank, query=客戶) 訓練：每列 = [自身 grouping 的 OOF 分數, 客戶特徵, (可選)grouping id]
        ├── early-stop valid = train_dev，用「Stage-1 refit-on-full-train」打分（train_dev 對 train 客戶互斥 → 乾淨）
        └── HPO 目標 = val（另一 snapshot），同樣用 refit Stage-1 打分
   ▼
最終 artifact（同一個 data/models/${model_version}/）：
   {每 grouping 一個 Stage-1 refit-on-full-train booster} + {1 個 Stage-2 booster} + grouping 表 + model_meta.json
```

**推論**：`predict(X)` 每列 → 路由到所屬 grouping 的 refit Stage-1 → 分數 → 組 `[分數, 客戶特徵]` → Stage-2 → 最終 scalar。最終排序仍交給現有 `rank_predictions`。

> **leakage 不變量（核心保證）**：Stage-2 訓練特徵（train 的 OOF 分數）永不來自看過該客戶的 Stage-1。三個 split 的語義與單模型路徑完全一致：`train`＝fit、`train_dev`＝early-stop valid、`val`＝HPO 目標。

## 5. Stage-1 grouping 抽象（per-item 與 per-category 統一）

- `stage1.grouping: item` → 每個 `schema.item` 值一個 binary 模型（= 22 個獨立 binary）。
- `stage1.grouping: category` → 每個產品大類一個 point-wise share 模型（類內 pooling，靠 `prod_name` 等特徵區分同類產品）。
- per-item 只是 grouping 的退化極端，**adapter 內部一套碼**，不分兩套。singleton 大類（只含一個產品）自然等價於該產品的 per-item 模型。
- **`product_categories` 單一真實來源**：現在只活在 `conf/base/parameters_evaluation.yaml` 的 `product_categories.mapping` 抽到 `schema.product_categories`（或等價的頂層共用位置），training Stage-1 grouping 與 evaluation collapse 都引用同一份，consistency 閘驗證一致。**即使最後不長期維護 two-stage，這個整理也賺**（消除 evaluation 自帶 mapping 的孤島）。

## 6. CompositeModelAdapter（對外仍是 ModelAdapter）

對外契約不變（`predict / save / load / feature_importance / log_to_mlflow`），故下游不動：

- **`predict(X) -> np.ndarray`**：純 numpy；每列從 X 還原所屬 grouping（X 內 item 欄是編碼後 int，adapter 需帶 item-column index ＋ grouping 表解碼），路由到對應 refit Stage-1 booster 打分，組 `[分數, 客戶特徵, (可選)grouping id]` 過 Stage-2，回最終 scalar。
- **`save(filepath)` / `load(filepath)`**：序列化 N+1 個 booster（各自 `*.txt`）＋ grouping 表 ＋ `model_meta.json`（記 `model_structure`、grouping、各 stage objective/metric、子模型清單）到**同一個** version 目錄。`io/model_adapter_dataset.py` 的 `ModelAdapterDataset` 依 `model_meta.json` 的 `model_structure` 還原成 composite 或單一 adapter。
- **內部複用**：每個 Stage-1/Stage-2 子 booster 仍透過 `get_adapter('lightgbm')` 走既有 `LightGBMAdapter`——composite 與 algorithm 正交，不重寫 booster 訓練碼。
- **`feature_importance`**：per-子模型（v1 可先只給 Stage-1）。屬報表豐富度、非正確性。

> **訓練介面的誠實限制**：現有 `ModelAdapter.train(X_train, y_train, X_val, y_val, params)` 是 numpy 介面、**拿不到 customer id**，無法表達「依客戶互斥切折 ＋ 產 OOF ＋ 組 Stage-2」。因此 composite 的訓練**不走** numpy `train()`，而是新增一條 composite 訓練路徑（見 §7）。
>
> ABC 怎麼滿足而不自相矛盾：`CompositeModelAdapter` 完整實作下游真正依賴的契約——`predict / save / load / feature_importance / log_to_mlflow`（inference ＋ 持久化）。ABC 中的訓練編排方法（`train` / `prepare_train_inputs`）在 composite 模式**不在執行路徑上**：由 §7 的 composite 訓練節點呼叫 `train_composite`，而 composite 的 `train` / `prepare_train_inputs` 實作為 `raise NotImplementedError`（明示「不該被單模型路徑呼叫」）。如此「對外仍是 `ModelAdapter`」對下游用到的部分為真，同時誠實標出 numpy `train()` 不是 composite 的訓練入口。inference 側（`predict/save/load`）乾淨封裝；training 側必須新增分支——這是本設計唯一 blast radius 較大的地方。

## 7. 訓練編排

`training/pipeline.py` 依 `training.model_structure` 分支：

- `shared`（預設）→ 現有 `build_lgb_datasets → tune_hyperparameters → finalize_model` 不變。
- `per_group_plus_rank` → 走新節點 `train_composite_model`（或 `CompositeModelAdapter.train_composite(train_handle, train_dev_handle, val_handle, preprocessor_metadata, parameters)`），內部：
  1. 依 `entity` 雜湊把 `train` 切 `n_folds` 個互斥折（**複用 `sampling` 既有的 `spark_bucket` / `ratio_to_threshold`**，與 dataset 的 train/train_dev 切法同源、確定性）。
  2. 對每折 k：在 `train\fold_k` 訓各 grouping Stage-1 模型，early-stop valid = `train_dev`，對 `fold_k` 打分。
  3. 組 OOF 預測（覆蓋整個 `train`）。
  4. 在整個 `train` refit 各 grouping Stage-1（inference 與 Stage-2 valid/HPO 打分都用這份）。
  5. 訓 Stage-2（lambdarank，query=entity）：特徵 = OOF 分數 ＋ 客戶特徵；early-stop valid = `train_dev`（用 refit Stage-1 打分）；HPO 目標 = `val`。
  6. 組 `CompositeModelAdapter`、`save` 到 version 目錄。

**HPO 範圍（v1）**：所有 Stage-1 子模型共用一組超參、Stage-2 一組；不做逐子模型獨立搜尋。`n_folds` 預設 5、可 config。

## 8. Config 切換

```yaml
training:
  model_structure: shared          # shared(現況,預設) | per_group_plus_rank
  stage1:                          # 僅 per_group_plus_rank 生效
    grouping: category             # item | category
    objective: binary
    metric: binary_logloss
    n_folds: 5                     # OOF 折數；折鍵 = entity 雜湊互斥
  stage2:
    objective: lambdarank
    metric: ndcg
    inputs: [group_score]          # v1 鎖 pointwise；跨產品相對特徵為 future
```

`schema.product_categories`（新，單一真實來源；evaluation 改為引用）：

```yaml
schema:
  product_categories:
    mapping: { fund: [...], exchange: [...], ccard: [...] }
    unmapped: singleton            # 未列入者各自成 singleton 大類
```

## 9. 版本化 ＋ consistency（必做，否則靜默錯）

- **`core/versioning.py`**：`model_structure` ＋ `stage1` ＋ `stage2` ＋ **grouping/`product_categories` 表** 全進 `training:` hash payload。否則切結構不 bump `model_version` → inference 載到結構不符 artifact，靜默錯誤。
- **`core/consistency.py`**（沿用既有 A 系列風格，新增 predicate，不在 pipeline 內 ad-hoc）：
  - `model_structure` ∈ {`shared`, `per_group_plus_rank`}。
  - `per_group_plus_rank` 時：grouping/`product_categories` 表必須覆蓋全部 `schema.item` 值，且與 `inference.products` 對齊（沿用 A4/A6 item-set 思路）。
  - `stage2.objective` 必為 ranking objective（重用 A7：`is_ranking_objective` ＋ metric 配對）。
  - calibration 語義（見 §10）：composite 下校準限定關閉或限定掛 Stage-1，不沿用「校準整體 model」舊假設。

## 10. Calibration / 未來疊加 hook

- 本版 lambda ranker 輸出是排序分數，**不掛機率校準**（對 per-customer mAP 無影響）。consistency 限定 `per_group_plus_rank` 預設關閉校準。
- **未來最後一層疊加「免費」**：現有 `CalibratedModelAdapter` 本就是「包住任一 `ModelAdapter`」的 wrapper。未來要在 composite 之上再加一層校準/疊加，直接用同一個 wrapper 包 `CompositeModelAdapter` 即可，不需為此改 composite 內部。

## 11. Inference / Evaluation 衝擊：幾乎為零

- **inference**：`pipelines/inference/nodes_spark.py` 的 `predict_scores` 只呼叫 `model.predict()`，且按 `(snap_date, prod_name)` chunk 控記憶體（`nodes_spark.py:71,90-100`）。pointwise 設計下 chunking 不變 → **不動**。
- **evaluation**：永遠對最終分數做 per-`(snap_date, cust_id)` mAP，不管模型內部幾段 → **不動**。`macro_per_item_map` 正好是量「two-stage 有沒有幫到冷門」的對的尺規（對照單模型基準線）。

## 12. Diagnostics

`pipelines/training/diagnostics.py` 目前假設單一 booster（SHAP / native importance / 分數分布）。composite 下改成 per-子模型，或 v1 先限定診斷 Stage-1。屬報表豐富度、非正確性，可後補。

## 13. 逐檔 change-impact map

**程式碼**

| 檔案 | 改動 | 規模 |
|---|---|---|
| `src/recsys_tfb/models/composite_adapter.py`（新） | `CompositeModelAdapter`：`train_composite` 編排 OOF＋N×K 子 fit＋refit＋Stage-2；`predict` 路由；`save/load` N+1 booster ＋ grouping 表 | 大（核心） |
| `src/recsys_tfb/models/base.py` | 不改 ABC；composite 選擇由 pipeline 依 `model_structure` 決定（與 `algorithm` 正交） | 小 |
| `src/recsys_tfb/pipelines/training/pipeline.py` ＋ `nodes.py` | 依 `model_structure` 分支；新增 composite 訓練節點；複用 `spark_bucket` 切折 | 中–大 |
| `src/recsys_tfb/core/versioning.py` | `model_structure`/`stage1`/`stage2`/grouping 表納入 `training:` hash | 小但**必做** |
| `src/recsys_tfb/core/consistency.py` | 新 predicate（§9） | 中 |
| `src/recsys_tfb/core/schema.py` | 新增 `product_categories` 解析（單一真實來源） | 小–中 |
| `src/recsys_tfb/io/model_adapter_dataset.py` | `model_meta.json` 記 `model_structure`＋grouping；load 還原 composite | 小–中 |
| `src/recsys_tfb/models/calibrated_adapter.py` | composite 預設關校準；wrapper 行為不變 | 小 |
| `src/recsys_tfb/pipelines/training/diagnostics.py` | per-子模型或限定 Stage-1 | 中 |
| `conf/base/parameters_training.yaml` | `model_structure` ＋ `stage1`/`stage2` | 小 |
| `conf/base/parameters_evaluation.yaml` | `product_categories` 改引用 `schema.*` | 小 |
| `pipelines/inference/nodes_spark.py` | pointwise 設計 → **不動** | 無 |
| `conf/base/catalog.yaml` | composite 存同一 version 目錄 → `model:` 單條 **不動** | 無 |
| tests | composite 單元測試、OOF leakage 測試、versioning、consistency 新不變量、save/load round-trip、pipeline 分支 | 中–大 |

**文件（本次明確納入範圍）**

| 檔案 | 改動 |
|---|---|
| `README.md` | §0「這是什麼」補一句 two-stage 為進階可選模式；§2「各 pipeline 的 node 全貌」training 補 composite 分支；§4「設定一致性閘」補新不變量；§5「文件地圖」加交叉引用 |
| `docs/pipelines/training.md` | **主要更新**：`model_structure` 切換、OOF cross-fitting 資料流、config schema、版本化、診斷限制 |
| `docs/pipelines/evaluation.md` | `product_categories` 改為單一真實來源（引用 `schema.*`）；註明 `macro_per_item_map` 作為冷門 item 的對照尺規 |
| `docs/design-principles.md` | 以 `CompositeModelAdapter` 為例，說明「adapter 邊界收斂 blast radius」原則 |
| `docs/change-guide.md` | 新增「如何加 model structure / consistency 不變量」的 SOP 指引（呼應 consistency 單一真實來源規範） |
| `docs/handbooks/gbdt_multiitem_imbalance.md`、`gbdt_learning_to_rank.md` | **交叉引用**：two-stage stacking 是本手冊不平衡對策的工程落地之一；**不重寫數學** |

> 文件撰寫沿用既有原則：保持抽象排序框架定位（銀行只是示例）、對齊程式碼真實識別字、補「為什麼」、手冊只交叉引用不重寫數學。

## 14. 測試策略

- **OOF leakage 測試**（最關鍵）：構造小資料，驗證 Stage-2 訓練特徵對應的 Stage-1 預測，其 Stage-1 fold 模型確實沒看過該客戶（折互斥性 ＋ OOF 覆蓋性）。
- `CompositeModelAdapter` save/load round-trip：N+1 booster ＋ grouping 表還原後 `predict` 一致。
- versioning：`model_structure` / grouping 表變動 → `model_version` 改變；`shared` 路徑 hash 不受新欄位影響（backward-compat）。
- consistency：新不變量各自的 fail-loud 測試（沿用既有 A 系列測試風格）。
- pipeline 分支：`shared` 與 `per_group_plus_rank` 各跑得通；複用既有小資料 fixture，遵守測試跑快原則。

## 15. 未決 / future（YAGNI）

- 跨產品相對特徵（需 per-客戶組裝、打破 inference chunking）。
- small-group 最小門檻自動併回 shared。
- 逐子模型完整 HPO、pipeline 層 fan-out（獨立排程/重訓/觀測）。
- composite SHAP 全套；最後一層機率校準（架構已留 hook）。
