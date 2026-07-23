# Staged Modeling（兩階段建模）設計 spec

> 2026-07-23 定案（grilling 對談逐項確認）。取代 PR #68（`per_group_plus_rank`）的規劃：
> main 自該 PR 開出後歷經診斷層重構（Plan 0-3）、eval report 重排、HPO 診斷等大改，
> 且其單一大 PR 的審查負擔過重。本 spec 以 main `42e4083` 為基準重新推導；
> PR #68 分支保留於 remote 供實作細節參考（CompositeModelAdapter、OOF 編排、
> consistency predicate），**不 cherry-pick**。

## 0. 一句話定位

新增 config 可切換的建模模式 `training.model_structure: staged`：
Stage-1 依「sample_pool 任意欄位組合」把訓練資料分群、**每群各自訓練＋各自搜超參**的
point-wise binary 模型；Stage-2 可選 point-wise binary、list-wise lambdarank、或不用
（`none`，直接以 Stage-1 分數排序）。預設仍是 `shared`（現況單一模型），純 opt-in、
shared 路徑零 regression。

## 1. 決策記錄（每條都經使用者確認）

| # | 決策點 | 定案 |
|---|---|---|
| D1 | 重設計動機 | main 演進使 PR #68 脫節＋其 diff 過大，兩者皆是；以現行 main 重推、變更足跡小＋可分段落地為設計目標 |
| D2 | Stage-1 分群 | 以 sample_pool 的**任意欄位組合**當分群鍵（例：`prod_name`、`cust_segment`、`prod_name＋cust_segment`），先以 item 為主要情境；分群是**資料驅動**（distinct 值），無外部 mapping 檔 |
| D3 | stage2=none 定位 | 允許，文件化警告（見 §2.3 分數可比性） |
| D4 | Stage-2 輸入 | Stage-1 分數＋**全部原始特徵**（可用既有 feature selection 縮減）＋分群鍵欄位當 categorical |
| D5 | OOF | 沿用 PR #68：entity 雜湊（crc32）互斥 K 折、OOF 分數餵 Stage-2、serving 用全量 refit；K 可設（缺省 5）；stage2=none 時完全跳過 OOF |
| D6 | Stage-1 objective | 本期固定 binary；config 預留 `stage1.objective`（只收 `binary`），日後擴充不破 schema |
| D7 | HPO | **Stage-1：各群獨立搜超參**（共用同一 search space 定義與 n_trials），**不支援 resume**（in-memory study，中斷即重搜），訓練效率必須極高；**Stage-2：比照現行 shared 機制**（persistent study、崩潰復原 resume、搜尋診斷） |
| D8 | Stage-1 trial 評分 | 各群自己的 validation 子集上的 binary 指標（AUC/logloss） |
| D9 | 抽樣/權重 | 完全沿用既有機制（同一份 dataset 產物、`sample_group_keys`＋overrides 控各群平衡、權重照既有 lookup 繼承）＋**對齊提醒**：分群鍵與 `sample_group_keys` 不一致時 consistency WARN |
| D10 | 診斷 | stage2 存在：booster 類診斷（SHAP/importance/gain ledger）掛 Stage-2、Stage-1 出精簡總覽表；**stage2=none：Stage-1 每群各自做一份完整 training 側診斷**；eval 側診斷不動（驗證相容即可） |
| D11 | 未見分群值 | **跳過＋WARN**：缺模型的列不評分、排除於輸出，大聲統計缺哪些群、影響幾列 |
| D12 | 規模量級 | N（群數）＝十～百；**單群萬～百萬列**（2026-07-23 更正：上看百萬）；設計不得假設單群小 |
| D13 | PR #68 處置 | 關閉 PR（留註連結新 spec）；remote 分支保留當參考 |
| D14 | 交付切法 | 3～4 個**連續** PR（一個 merge 進 main 才開下一個，不疊 stacked PR），每個可獨立 merge、shared 路徑零 regression |
| D15 | 校準 | staged 模式下 calibration 一律關閉（consistency 擋住）；不做 staged 校準支援——校準功能本身預計日後移除，本設計只當 placeholder 邊界 |

## 2. 建模語意

### 2.1 Config 形狀（示意，實作計畫時定稿鍵名）

```yaml
training:
  model_structure: shared          # shared | staged（缺省 shared）
  staged:                          # 只在 staged 時讀取
    stage1:
      partition_keys: [prod_name]  # sample_pool 欄位組合，資料驅動分群
      objective: binary            # 本期只收 binary（預留擴充）
      hpo:
        n_trials: 30               # 每群獨立搜；0 = 不搜、用 params 固定值
        metric: auc                # auc | logloss
        search_space: {...}        # 全群共用同一份定義
      params: {...}                # 基底參數（HPO 之外的固定鍵；n_trials=0 時全用它）
    stage2:
      mode: lambdarank             # binary | lambdarank | none
      oof_folds: 5                 # stage2!=none 時生效
      # stage2 的超參／HPO 設定比照現行 shared 模式的既有鍵結構
```

所有新鍵都在 `training:` 區塊下 → `model_version` 由既有機制自動折入
（`versioning.py` 雜湊整個 training 區塊），**版本化機制零修改**（見 §4）。

### 2.2 訓練流程

1. 吃**同一份** dataset 產物（train/train_dev/val/test model_input）——dataset pipeline 完全不動。
2. 單次 extract 後，依 `partition_keys` 在記憶體切出各群子集（含列權重）。
3. **每群**：以 train 子集訓練、train_dev 子集 early stopping；`n_trials>0` 時先跑
   in-memory Optuna 搜尋（評分＝該群 validation 子集的 AUC/logloss，見 §3.1 的 split 衛生），
   取最佳參數。
4. `stage2=none`：各群以最佳參數全量訓練，收工（無 OOF）。
5. `stage2≠none`：各群以最佳參數做 entity-hash K 折 OOF fit（訓練資料的 Stage-1 分數
   由「不含該 entity 的折模型」產生）＋全量 refit（serving 用）；Stage-2 以
   「原始特徵＋OOF Stage-1 分數＋分群鍵 categorical」訓練，early stop 於 val
   （val/test 的 Stage-1 分數用全量 refit 模型產生——entity 不重疊，無 leakage）。

### 2.3 stage2=none 的分數可比性（文件化警告）

最終排序在每個 query（snap_date × entity）內**跨 item** 比分數（`metrics.py`
`compute_macro_per_item_map`：ranking is within each query）。因此：

- 分群鍵**只含 entity 側欄位**（如 `cust_segment`）：同 query 所有候選落同一模型，無可比性問題。
- 分群鍵**含 item**：同 query 的候選由不同模型評分。各模型都在估 P(y=1|x)，概念上同尺，
  但**各群負樣本下採比例不同時機率估計會系統性歪掉**，跨模型比較有偏。
  此模式定位為實驗對照組——允許使用、評估指標自己說話；文件與（視實作）log WARN 明示此風險。

## 3. HPO

### 3.1 Stage-1：各群獨立、無 resume、講效率

- 每群一個 **in-memory** Optuna study（不落 SQLite）：中斷即整段重搜，換取零 resume
  契約負擔（刻意不碰 RESUME_CONTRACTS）。
- search space 定義全群共用一份；各群獨立搜、各自出最佳參數。
- trial 評分：該群 validation 子集的 binary 指標（缺省 auc）。
- **split 衛生（設計決定，標註給審者）**：Stage-1 HPO 的評分子集用 **train_dev**
  （early stopping 同源，接受選擇偏差輕度樂觀）；**val 保留給 Stage-2 HPO 評分、
  test 兩階段都不得碰**。避免 val 被 Stage-1 選參污染後 Stage-2 又用它選參。
- 每群搜出的最佳參數寫入 manifest／總覽表（§6），保留可稽核性——雖不可 resume，
  但「搜到了什麼」必須落地。

### 3.2 Stage-2：比照現行 shared 機制

- persistent study、`--fresh-hpo`、崩潰復原、HPO 搜尋診斷（PR #106 那套）照用。
- trial 評分沿用全局 macro per-item mAP（現行 `tune_hyperparameters` 路徑）。
- trial 內只重訓 Stage-2（Stage-1 分數已定、當快取特徵）→ 單 trial 成本 ≈ 現行 shared
  模式的一次訓練，成本結構不變。
- `search_id`（model_version 去 n_trials）語意只涵蓋 Stage-2 搜尋；Stage-1 無 search_id。

## 4. 版本化

- **機制零修改**：`compute_model_version` 雜湊 `training:` 區塊（減
  `MODEL_VERSION_IRRELEVANT_PARAMS`），新增的 `model_structure`／`staged.*` 鍵自動折入。
- 分群資料驅動 → 無 PR #68 `product_categories` 式的頂層 mapping 折入問題。
- `base_dataset_version`／`train_variant_id`／dataset pipeline 完全不受影響（§5）。
- Artifact 布局（單一 model_version 目錄＝完整 bundle）：
  ```
  data/models/<model_version>/
    manifest.json                  # 既有兩階段 manifest（stub→completed）照用
    stage1/<group_slug>/model.txt  # group_slug = 分群鍵值的安全編碼
    stage1/groups_index.json       # 分群鍵→slug 對照、各群列數/正例數/最佳參數/分數
    stage2/model.txt               # stage2!=none 時
  ```
  載入端由 adapter 的 save/load 封裝（§7），對 `ModelAdapterDataset` 呈現單一模型介面。
- Stage-1 各群搜參不可重現（無 resume、不進 config）→ 與現行 HPO 相同的既有語意：
  model_version 標識「config 意圖」，實際搜出參數記錄在 manifest／groups_index。

## 5. 抽樣與樣本權重

- staged 模式吃同一份 dataset 產物；Stage-1 各群子集訓練時從記憶體切出。
- 各群正負平衡：靠既有 `sample_group_keys`＋sampling overrides（editor 調），
  **不新增 staged 專屬抽樣機制**。
- 列權重：既有 `sample_weight_keys` lookup 產生的權重跟著列走——各群繼承自己子集的
  權重，Stage-2 繼承全量權重（兩種 mode 都比照現行 shared 行為）。
- **對齊提醒**（consistency WARN，非 error）：`staged.stage1.partition_keys` 與
  `sample_group_keys` 不一致時提醒「各群抽樣比例可能在群內不均勻；且 stage2=none 時
  影響跨群分數可比性（§2.3）」。

## 6. 診斷

| 情境 | training 側 booster 診斷（SHAP／importance／gain ledger） | Stage-1 總覽表 | HPO 搜尋診斷 |
|---|---|---|---|
| staged＋stage2∈{binary,lambdarank} | 掛 **Stage-2 booster**（含「Stage-1 分數特徵有多重要」新視角） | ✔（每群一列：列數/正例數/最佳參數/trial 分數/訓練時間） | Stage-2 study 沿用現行；Stage-1 結果只進總覽表 |
| staged＋stage2=none | **每群各自一份完整** training 側診斷（輸出按 group_slug 分目錄） | ✔ | 無（Stage-1 結果進總覽表） |
| shared（現況） | 現行行為，零變動 | — | 現行行為 |

- evaluation 側診斷（item_ability、model_capacity、suppression、metric CI…）吃預測值，
  理論上不動；驗收時需實跑確認相容（不是宣稱）。
- gain ledger 讀 booster trees（非 model.txt 重生），per-group 版直接對各群 booster 跑。

## 7. 推論與 Adapter

- `CompositeModelAdapter`（名稱暫定）對外實作既有 `ModelAdapter` 介面
  （predict/save/load）：內部做分群路由（依列的分群鍵值選 Stage-1 模型）＋
  Stage-2 疊加。目標：inference／evaluation／catalog／`rank_predictions` **不動**
  （沿 PR #68 已驗證的結論，實作計畫時對 main 現況重新驗證）。
- 推論 chunk 粒度（snap_date, prod_name）與 per-item 分群天然相容；entity 側分群鍵
  需要 scoring dataset 帶該欄位 → 前置檢查：`partition_keys ⊆ scoring dataset 欄位`，
  缺欄 fail-fast（這是 schema 問題，不是資料漂移，不適用跳過）。
- **未見分群值（D11）**：評分前比對 scoring 資料分群值集合 vs 已訓群集合；
  缺模型的列**跳過＋WARN**（不評分、排除於輸出），log 大聲列出缺哪些群、各影響幾列、
  佔比。候選宇宙縮水必須在 log 與（視實作）manifest 可見，不得靜默。
- inference 尚未在公司環境部署（issue #63 背景）→ 無 backward-compat 包袱；
  公司規模 e2e 驗證照既有慣例列為 deferred。

## 8. 效率設計（D7「Stage-1 效率必須極高」＋ D12 規模）

設計基準：N＝十～百群、單群萬～**百萬**列（不得假設群小）。

- 單次 extract 後記憶體切群，避免 N 次重讀。
- 跨群平行：stdlib（`concurrent.futures`）pool，**依群大小排程**——大群（十萬～百萬列）
  序列或低併發跑、小群打包平行；每 worker 的 LightGBM `num_threads` 配額化避免超訂。
  生產限制（no additional packages、CPU-only、no UDF）下只用現有依賴。
- 單群 HPO 總成本 ≈ n_trials 次小模型訓練；n_trials 是使用者的成本旋鈕（`0`＝跳過）。
- ⚠ 開放項（實作計畫時實測定案）：百萬列大群 × 併發下的峰值記憶體；是否需要
  「切群後落地、逐群 lazy 載入」的退讓路徑。**不預先過度設計，先量測。**

## 9. Consistency（`core/consistency.py`，編號依現況 legend 順延，不 ad-hoc 散落）

新增 predicate（error 級，collect-all）：
1. `model_structure ∈ {shared, staged}`；staged 時 `staged` 區塊必備。
2. `stage1.objective == binary`（預留鍵，本期只收 binary）。
3. `stage2.mode ∈ {binary, lambdarank, none}`；`oof_folds >= 2`（stage2≠none 時）。
4. `partition_keys` 非空、皆為 sample_pool 欄位（Layer-1 查 schema config；
   欄位實存性由 Layer-2 資料閘慣例補）。
5. staged 時 calibration 必須關閉（D15）。
6. Stage-1 HPO：`n_trials >= 0`、`metric ∈ {auc, logloss}`。

WARN 級：
7. `partition_keys != sample_group_keys`（D9 對齊提醒）。
8. `stage2.mode == none` 且 partition_keys 含 item 欄位（§2.3 可比性警告）。

## 10. 交付切法（D14：連續 PR，前一個 merge 進 main 才開下一個）

| PR | 內容 | 驗收重點 |
|---|---|---|
| **PR-A：Stage-1 引擎** | 分群切分、per-group 訓練＋per-group HPO（in-memory）、adapter（save/load/predict 路由）、`stage2=none` 端到端可跑、consistency 新 predicate、artifact 布局、未見群跳過＋WARN | shared 路徑 baseline 零 diff；staged(none) 本機 e2e；效率量測（單群百萬列） |
| **PR-B：Stage-2** | OOF K 折編排、stage2 binary／lambdarank、Stage-2 HPO 接現行機制（persistent study/resume/搜尋診斷） | OOF leakage-clean 測試（fold 互斥）；staged(lambdarank) 本機 e2e；HPO resume 實測 |
| **PR-C：診斷** | Stage-1 總覽表、stage2=none 每群完整診斷、stage2 存在時掛 Stage-2、eval 側相容驗證 | 兩種 mode 的診斷產物 real-run 驗證 |
| **PR-D：文件** | training.md 章節、README、design-principles、相關手冊交叉引用 | fresh 讀者驗收（handbook 風格規範） |

每個 PR 在自己的 worktree 週期內完成；PR-A 開工前先關 PR #68（D13）。

## 11. 刻意不做（YAGNI，均已與使用者對齊）

- Stage-1 lambdarank（per-segment 分群理論可行，本期不做；D6 預留鍵）。
- 未見群的 fallback 模型（D11 選了跳過＋WARN；日後有需求再議）。
- staged 專屬抽樣機制（D9）。
- Stage-1 HPO resume／persistent study（D7 明示不做）。
- stage2 存在時的 per-group 完整診斷（D10 只在 none 時做）。
- staged 校準支援（D15；校準功能本身預計移除）。
- Stage-1 各群 n_trials 差異化、共享先驗／warm-start（規模若上千群再議，D12 註記）。

## 12. 實作計畫前的待驗證清單（寫各 PR plan 時逐項核實，不憑本 spec 記憶）

- [ ] main 現行 training pipeline node 清單與 staged 分支點（PR #68 時代是 15-node，已演進）。
- [ ] `extract_Xy`／prepare 層（lgb.Dataset binary 預備層）與 per-group 切分的接口；
      weight baking 與 .bin cache 在 per-group 路徑下的行為（known 坑：sample_weight cache 靜默失效）。
- [ ] `ModelAdapterDataset` 載入路由對 bundle 目錄的相容方式。
- [ ] evaluation `--post-training`／compare pipeline 對 staged 預測的實跑相容。
- [ ] consistency 現行編號 legend（A15 之後排到哪）。
- [ ] Stage-1 併發＋大群的記憶體實測（§8 開放項）。
