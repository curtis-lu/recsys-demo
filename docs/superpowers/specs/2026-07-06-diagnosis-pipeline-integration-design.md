# Spec：診斷框架接入 training / evaluation pipeline

> 2026-07-06 初版；2026-07-07 修訂（納入 Phase 0 診斷域重構、移除 shaprx 前提、loss-SHAP/cohort/處方自動化裁決）。目標：把 `docs/ranking-diagnosis-framework.md`（下稱**框架**）的十個診斷項目落地到本 repo 的 training 與 evaluation pipeline，**分六個階段（Phase 0–5）交付，每階段以本機真實 pipeline 執行的產物為驗收閘門**——使用者檢視產物、確認後才進下一階段。
>
> 本 spec 是設計文件（要蓋什麼、長在哪、怎麼驗收），不是實作計畫（工作項拆解與順序留給 writing-plans 階段）。

## 0. 已定案的範圍決策

| 決策點 | 定案 |
|---|---|
| 範圍 | 十個診斷項目**全部**納入，分六階段（Phase 0 重構＋Phase 1–5 功能） |
| 主指標 | **參數化指標家族**＋暫定預設（原樣 macro ＋ entity-cluster bootstrap CI）；權重定案等真實資料 |
| 模組歸屬 | ~~沿既有兩側擴充~~ → **2026-07-07 修訂**：新建 `src/recsys_tfb/diagnosis/` 診斷域 library（`model/`＝訓練側結構診斷、`metric/`＝評估側指標診斷），既有 `pipelines/training/diagnostics/` 於 Phase 0 行為不變平移進去；兩條 pipeline 都只留薄 node。原「沿兩側擴充」的否決理由是改動面，Phase 0 閘門（測試 baseline＋產物 diff）把改動面關進籠子後不再成立 |
| offset sweep | v1 **只當診斷**（產報表），不做可部署的 predict 後處理 |
| loss-SHAP／cohort 偵測／處方自動化 | **2026-07-07 裁決**：loss-SHAP 不納 v1（SHAP 接縫留 explainer 選項空間）；cohort 自動搜索不納（撞 no-additional-packages），納降級版「傷害×既有 segment 分組報表」（Phase 4）；處方自動化只到「規則表＋起手值」層級（Phase 5 triage），閉環自動化不納 |

被否決的替代架構（記錄理由）：(a) **全部塞進 report_builder**——把計算與呈現攪在一起，違反現有「Spark 聚合 → plot → report 組裝」的分層；(b) **沿既有兩側擴充、不建 library**（本 spec 初版方案）——會把跨兩條 pipeline 的單一診斷域拆在「library 層」與「別條 pipeline 的內部套件」兩種架構尺度上，且評估側永遠無法重用訓練側的象限語意（跨 pipeline 內部 import 是禁手）；2026-07-07 經使用者要求以 Kedro 風格重新評估後，改採 Phase 0 的 `diagnosis/` library 方案。注意新方案**不搬 `evaluation/` 的 metrics/report**（那是評估本體），只收攏診斷域——初版否決「共用模組」時擔心的搬動面，大部分來自誤把 metrics 也算進去。

## 1. 不變量與邊界（違反即錯）

1. **生產限制沿用**：no Spark UDF、no network、no additional packages、CPU-only。所有 Spark 端診斷用內建函式（window / groupBy）；需要逐列迭代的計算（bootstrap、offset sweep、成對帳本）一律在 **driver 端 numpy、跑在有上限的抽樣**上（§2）。
2. **新增 config 鍵必附 consistency predicate**：進 `src/recsys_tfb/core/consistency.py`，新代號從 **A15** 起（現用到 A14），照該檔慣例：predicate 函式＋`validate_config_consistency` 串接＋docstring Invariant legend 補代號。
3. **不動 model_version 的語意**：評估側全部新 config 在 `evaluation.*`（本來就不進 model_version）；訓練側新診斷 config 掛既有頂層 `diagnostics:`（已確立不進 model_version 的前例）。**任何會改變 `compute_model_version` 輸入的鍵都不允許在本案新增。**
4. **模組邊界與依賴方向（單向，違反即錯）**：`pipelines/* → diagnosis → evaluation`（僅 numpy 原語 `evaluation/metrics.py`）`/ io / utils`；`diagnosis` 不得 import 任何 `pipelines/*` 內部；pipeline 之間不互相 import。Phase 5 的 triage 總表需要訓練側產物（gain ledger），**靠 catalog 的 JSON 產物銜接，不靠 import**。
5. **HPO objective 本案不動**：`training.hpo_objective` 維持現狀（`mean_ap` / `macro_per_item_map`，`pipelines/training/nodes.py:361-364`）。把參數化指標接進 HPO 會經 training params 影響 model_version 與搜尋行為，留待指標定案後另案。
6. **SHAP 範圍**：本案的 SHAP 只到「per-item 背景的分數歸因」（Phase 5）；loss-SHAP、cohort 偵測器、處方自動化不在本案（見 §6）。

## 2. 共用底座：診斷抽樣（bounded driver-side sample）

多個診斷項目（CI、offset sweep、成對帳本、替換實驗）需要在同一份資料上做迭代式計算，Spark 端做不划算也不必要。定義一個共用抽樣件：

- **來源**：pipeline 內的 `eval_predictions`（`prepare_eval_data` 節點的輸出、in-DAG dataset，**不是 catalog 表名**；上游依模式為 `training_eval_predictions` 或 `ranked_predictions`）。已含 `snap_date, cust_id, prod_name, score, label`，rank 由 `rank_within_query` 補算（`src/recsys_tfb/evaluation/metrics_spark.py:253`）。
- **抽樣單位＝query（snap_date × cust_id）**，只取**有正例的 query**（指標只由它們定義），分層保證每個 item 至少 `min_pos_queries_per_item` 個正例 query（不足全取）；上限 `max_queries`。**抽樣是兩趟設計**（單一 hash 門檻給不了 per-item 保底）：第一趟 count 每 item 的正例 query 數；正例 query 少於保底的 item 整批全取（take-all），其餘 item 才用 hash-ratio 抽到補滿 `max_queries`。抽樣用既有 CRC32 確定性雜湊工具（`spark_bucket` / `ratio_to_threshold`，現位於 `src/recsys_tfb/pipelines/dataset/_hashing.py`；跨 pipeline 私有檔的問題由 **Phase 0** 平移到 `src/recsys_tfb/utils/hashing.py` 解掉，diagnosis 從 utils 取用）。
- **落到 driver**：`toPandas()` 後轉 numpy，供 `evaluation/metrics.py` 的既有 numpy 原語（`compute_ap:21`、`compute_macro_per_item_map:98`）重複使用。
- **實作位置**：新檔 `src/recsys_tfb/diagnosis/metric/sample.py`，單一函式 `draw_diagnosis_sample(eval_predictions, parameters) -> pandas.DataFrame`＋抽樣 metadata（實際 query 數、每 item 正例覆蓋、seed）。metadata 一律寫進產物 JSON——**報表必須顯示「這是抽樣上的估計」與樣本規模**，不得讓抽樣估計冒充全量。
- **config**：`evaluation.diagnosis.sample: {max_queries: 200000, min_pos_queries_per_item: 50, seed: 42}`。

## 3. 六個階段（Phase 0–5）

每階段的固定結構：**做什麼 → 動哪些檔 → 新 config → 驗收（真實執行）**。驗收一律在本機 local Spark 環境（`--env local`，合成資料），指令照 `docs/operations/local-spark-setup.md`；「已知答案注入」指刻意在 local config 製造一個已知效應、驗證診斷能抓到它——這是每階段閘門的核心。

---

### Phase 0：診斷域的 Kedro 式歸位（行為不變的結構平移）

**動機**：現況兩側不對稱——評估邏輯有「library 層（`src/recsys_tfb/evaluation/`）＋薄 pipeline 層」的乾淨分層，訓練側的診斷邏輯卻整包住在 pipeline 套件內部（`pipelines/training/diagnostics/`）。本案讓「診斷」成為跨兩條 pipeline 的單一領域（triage 合併兩側產物、象限語意兩側共用），繼續分居兩個架構層，就是把一個領域拆在兩種尺度上。依 repo 的 Kedro 式慣例：pipeline 套件只留薄 node 與接線，領域邏輯進共用 library，跨 pipeline 資料一律走 catalog。這個平移同時兌現「單列層歸因留可抽出接縫」的既定方向——日後若要獨立化，`diagnosis/` 就是那個接縫。

**做什麼（全部是行為不變的搬移）**：

```
src/recsys_tfb/diagnosis/
  model/    ← 平移 pipelines/training/diagnostics/ 全部檔案
             （attribution, sampling, shap_per_item, shap_cases, importance,
               feature_stats, data_access, paths, _util）
             ＋ pipelines/training/diagnostics_spark.py（改名 population_spark.py）
  metric/   ← 空殼建立（Phase 1–5 的評估側診斷邏輯之家）
```

- `pipelines/dataset/_hashing.py` → `src/recsys_tfb/utils/hashing.py`（§2 所需；原 import 處 `helpers_spark.py:10`、`nodes_spark.py:11` 同步更新）。
- `pipelines/training/pipeline.py` 與相關 node 只改 import 路徑；node 名、config 鍵、catalog 條目、產物路徑、model_version **全部不變**。
- 測試鏡像目錄同步平移（含 `tests/test_pipelines/test_dataset/test_hashing.py` 的 import 更新——它也 import `_hashing`）。

**驗收（真實執行）**：
1. 平移前在 main 記錄測試 baseline（含既知 failing/互擾清單）；平移後測試結果與 baseline 完全一致。
2. 用 pipeline 切片**只重跑診斷節點**（`python -m recsys_tfb training --env local --from-node compute_feature_statistics`），吃同一個已訓 model artifact：`data/models/${model_version}/diagnostics/*.json` 與平移前逐檔 diff 一致、model_version 不變。（不整條重跑的理由：重訓的位元重現性沒有被 config 鎖死——base config 無顯式 seed/deterministic——整條重跑會把「模型浮點差異」誤報成平移問題；診斷節點本身的隨機性已全部釘 seed，切片重跑是決定性的。）
3. `git diff --stat` 只含搬移與 import 更新，無任何邏輯行變更。

---

### Phase 1：指標基座（框架診斷項目 4；判讀第 0 步）

**做什麼**：把主指標參數化成家族，並給 per-item AP 補信賴區間。沒有這一步，後面所有診斷讀數都無法判定顯著性。

- `src/recsys_tfb/evaluation/metrics.py`：`compute_macro_per_item_map`（`:98`，已支援 `k`）擴充參數 `weight_alpha`（$w_j \propto P_j^{\alpha}$，$\alpha=0$＝現行 macro）、`min_positives`（低於門檻的 item 移出平均、單獨回報）、`shrinkage_k`（per-item AP 向全體平均收縮 $\frac{P_j}{P_j+k}$）。**預設值全部等價於現行為**（`alpha=0, min_positives=0, shrinkage_k=0`），既有測試（`tests/.../TestComputeMacroPerItemMap`）不需改即須全綠。
- `src/recsys_tfb/evaluation/metrics_spark.py`：`aggregate_per_item`（`:447`）／`macro_average`（`:508`）接同一組參數。**前置缺口（審查發現，必修）**：`aggregate_per_item` 目前的 per-item 輸出只有 `{hit_rate@K, map_attr@K, ndcg_attr@K, mean_pos}`，**沒有 per-item 正例數**——而 `weight_alpha` / `min_positives` / `shrinkage_k` 三者都需要 $P_j$。因此本階段第一步是讓 `aggregate_per_item` 增列 `n_pos`（additive、不動既有鍵），`macro_average` 從它取 $P_j$；沒有這一步，Spark 側參數化在數學上做不到、會靜默退回等權。完成後維持 Spark／numpy 兩實作在同輸入上結果一致（既有 parity 測試模式照抄，見 `tests/test_evaluation/` 的 metrics_spark 對照測試）。
- （指標參數化留在 `evaluation/`——metrics 是評估本體、不是診斷；以下 CI 起才進 diagnosis）新檔 `src/recsys_tfb/diagnosis/metric/uncertainty.py`：`bootstrap_per_item_ci(sample_pdf, parameters) -> dict`——在 §2 的診斷抽樣上，以 **cust_id 為 cluster** 的 bootstrap（同一客戶跨期整批重抽），輸出 per-item AP 與 macro 的區間。`n_boot` 預設 200。
- report：`build_per_item_attr_section`（`report_builder.py:312`）與 `build_primary_map_section`（`:115`）增列 CI 欄；新產物 `diagnosis/metric_ci.json`。
- **新 catalog 條目**：`evaluation_diagnosis_dir` 系列 JSON 落在 `data/evaluation/${model_version}/${snap_date}/diagnosis/`（與 `evaluation_report` 同層，`catalog.yaml:236` 慣例）。
- **config**：`evaluation.metric: {weight_alpha: 0.0, k: null, min_positives: 0, shrinkage_k: 0}`；`evaluation.diagnosis.ci: {enabled: true, n_boot: 200}`＋§2 的 sample 區塊。
- **consistency（A15）**：`alpha ∈ [0,1]`、`min_positives ≥ 0`、`shrinkage_k ≥ 0`、`n_boot ≥ 1`、`max_queries ≥ 1`、`min_pos_queries_per_item ≥ 1`。

**驗收（真實執行）**：
1. `python -m recsys_tfb dataset --env local && python -m recsys_tfb training --env local`（既有 e2e 路徑）取得 model_version。
2. `python -m recsys_tfb evaluation --env local --post-training`。
3. 檢視：`report.html` 的 per-item 表每列有 AP±CI；`diagnosis/metric_ci.json` 含抽樣 metadata。
4. 已知答案：(a) 預設參數下 macro 值與改動前 report 完全相同（回歸不變）；(b) 把 `min_positives` 調到高於最冷合成 item 的正類數，重跑 evaluation，該 item 從 macro 移出、出現在「觀察名單」列。

---

### Phase 2：對帳層（框架診斷項目 1、2；判讀第 1 步）

**做什麼**：把「自己的採樣／加權配置理論上會造成的分數偏移」算出來，跟實測 per-item 校準差距對帳——框架裡期望值最高的一步。

- 新檔 `src/recsys_tfb/diagnosis/metric/reconciliation.py`：
  - `theoretical_offsets(parameters) -> dict`：讀 `dataset.sample_ratio` / `dataset.sample_ratio_overrides`（key＝`sample_group_keys` 以 `"|"` 串接，含 `label` 維度，`parameters_dataset.yaml:19-23`）與 `training.sample_weights`（key＝`sample_weight_keys` 串接，`parameters_training.yaml:48-59`）。負類保留率 $r$ → 理論偏移 $-\log r$（手冊3 Ch10）；權重效應同向。**誠實限制**：overrides 是多維 key（如 `cust_segment_typ|prod_name|label`），per-item 單一 offset 是聚合近似——產物按完整 group key 細列、item 層只給摘要，並標註近似性質。
  - `calibration_gap_by_item(sdf, parameters) -> DataFrame`（Spark，無 UDF）：per item 的 $\operatorname{logit}(\overline{p}) - \operatorname{logit}(\bar y_j)$（平均預測機率與實際正類率各自取 logit 後相減）。`score_col` 可設 `score` 或 `score_uncalibrated`——**預設 `score_uncalibrated`**，因為校準層本身就在修 level，對帳要看模型原始輸出；報表兩欄都列。**降級行為**：`score_uncalibrated` 只保證存在於 post-training 路徑（`training_eval_predictions` 有此欄，`training/nodes.py:872-879`）；monitoring 路徑（`ranked_predictions`）若無此欄，自動退回 `score` 並在報表標註，不失敗。
  - 對帳表：per item {理論 offset、實測 gap、殘差、verdict（可解釋／不可解釋，門檻 config）}。
- report 新 section `reconciliation`（進 `report.sections` 清單；組裝函式 `build_reconciliation_section` 加入 `report_builder.py`）；產物 `diagnosis/reconciliation.json`。
- **config**：`evaluation.diagnosis.reconciliation: {enabled: true, score_col: score_uncalibrated, explained_threshold: 0.3}`（殘差絕對值低於門檻＝可解釋；門檻單位是 log-odds）。
- **consistency（A16）**：`score_col ∈ {score, score_uncalibrated}`、`explained_threshold > 0`。

**驗收（真實執行）**：
1. **已知答案注入**：在 local 的 `parameters_dataset.yaml` 對某個合成 item 的負類設 `sample_ratio_overrides` 保留率 0.5。**注意 key 是三維**（`sample_group_keys = [cust_segment_typ, prod_name, label]`）：要讓 item 層的理論值乾淨等於 $-\log 0.5$，必須把該 item 的**全部 segment cell 一起注入**（合成資料共三個 segment：`"mass|<item>|0": 0.5`、`"affluent|<item>|0": 0.5`、`"hnw|<item>|0": 0.5`）；只注入單一 segment 時，item 層實測位移會被未注入的 segment 稀釋（≈ 買家 segment 佔比加權），不能拿 +0.693 當期望值。重跑 `dataset → training → evaluation`。
2. 檢視：對帳表該 item 的理論 offset ≈ $-\log 0.5 = +0.693$（全 segment 注入時），實測 gap 同號且量級接近、verdict＝可解釋；**未注入的 item** verdict 應為可解釋且理論 offset ≈ 0。
3. 還原注入、重跑，確認對帳表回到全綠。

---

### Phase 3：行為層象限（框架診斷項目 3、5、10；判讀第 3 步的量測面）

**做什麼**：補齊 2×2 象限報表需要的另一軸（條件判別力）與傷害觀測。

- 新檔 `src/recsys_tfb/diagnosis/metric/discrimination.py`：`within_item_auc(sdf, parameters) -> DataFrame`——per item 的 ROC-AUC，用 rank-sum（Mann–Whitney U）在 Spark 內算：`Window.partitionBy(prod_name).orderBy(score)` 取 rank，正例 rank 和 → $\mathrm{AUC} = \frac{R^+ - n^+(n^++1)/2}{n^+ n^-}$。無 UDF；**平手處理釘死為 midrank（平均秩）**——rank-sum 公式只在 midrank 下精確，`F.rank`（min-rank）直接代入會系統性偏差。midrank 可由 `F.rank() + (同分數列數 − 1)/2` 組出（window 內建函式即可）。這點不是實作細節：框架的核心診斷對象正是「近常數分數的冷門 item」，它們大量平手，AUC 真值應為 0.5——**numpy parity 測試的 fixture 必須包含一個全平手（常數分數）item**，驗證 Spark 值恰為 0.5。
- 新檔 `src/recsys_tfb/diagnosis/metric/occupancy_spark.py` 兩個聚合（寫法沿 `evaluation/diagnostics_spark.py` 的 `rank_count_matrix:138` 家族慣例，但**歸屬診斷域**——放 evaluation 會讓 quadrant.py 跨邊界 import）：
  - `top_slot_share(sdf, k)`：per item 佔據 top-k 的 query 比例，並列該 item 正類率當對照。
  - `suppression_counts(sdf)`：per item「以**負例**身分排在該 query 內某正例上方」的次數（框架的傷害直接觀測；用 query 內 min positive rank 的 window 實作）。
- 交叉購買率：新檔 `src/recsys_tfb/diagnosis/metric/cross_purchase.py`：`cross_purchase_matrix(label_rows) -> DataFrame`（$P(\text{買 }k \mid \text{買 }j)$，label_table 自 join，per snap_date 聚合）。`label_table` 已是 evaluation pipeline 的既有輸入（`compute_baseline_metrics` 的 in 清單就有它），新 node 照同樣方式接線，無需新資料源。
- 象限組裝：新檔 `src/recsys_tfb/diagnosis/metric/quadrant.py`：合併 per-item {校準 gap（Phase 2）、within-item AUC、AP±CI（Phase 1）、suppression counts} → 象限判定（AUC 門檻與 gap 帶寬 config）→ `diagnosis/quadrant_summary.json` ＋ 散布圖（樣式沿手冊 `docs/diagrams/ranking-diagnosis/fig2-quadrant-map.png`；matplotlib 走既有 `evaluation/distributions.py` 慣例）。
- report 新 section `quadrant`。
- **config**：`evaluation.diagnosis.quadrant: {enabled: true, auc_threshold: 0.6, gap_band: 0.35, top_k_occupancy: 1}`。
- **consistency（A17）**：`0.5 ≤ auc_threshold < 1`、`gap_band > 0`。

**驗收（真實執行）**：
1. 重跑 evaluation（同一 model_version，`--only-node` 切片可用）。
2. 檢視：象限散布圖＋表；`quadrant_summary.json`。
3. 已知答案（方向性）：合成資料的 label 生成規則已知（`scripts/generate_synthetic_data.py` 的 `_compute_label_prob`）——最冷合成 item 應落在「判別力差」半邊；within-item AUC 的 Spark 值與 numpy（sklearn-free 手算）在診斷抽樣上一致（單元測試釘 parity）。

---

### Phase 4：指標層分流（框架診斷項目 6、7；判讀第 2、4 步）

**做什麼**：框架的分流閥——把指標缺口拆成「水準（不必重訓）」與「條件判別力（必須動訓練）」，並記「誰壓了誰」的帳。全部跑在 §2 的診斷抽樣上（driver-side numpy）。

- 新檔 `src/recsys_tfb/diagnosis/metric/offset_sweep.py`：
  - `sweep(sample_pdf, parameters) -> dict`：座標下降（一次動一個 $\delta_j$、一維掃描、輪流迭代到收斂或 `max_rounds`），目標＝Phase 1 的參數化指標；$\delta$ 向 0 收縮（L2 正則 `shrink_lambda`）；`holdout_fraction` 把抽樣切兩半、在折外報告 $\text{mAP}(\delta^*)$，防止「收復缺口」只是擬合驗證雜訊（框架 Ch 3 診斷項目 6 的警告）。
  - 輸出：$\delta^*$ 向量、$\text{mAP}(0)$、$\text{mAP}(\delta^*)$（折內＋折外）、per-item 缺口拆解。
- 新檔 `src/recsys_tfb/diagnosis/metric/pair_ledger.py`：
  - `pair_ledger(sample_pdf, parameters) -> dict`：對每個正例列，列舉同 query 排其上方的 item，記 $|\Delta\text{AP}_{ij}|$（交換名次的指標敏感度，λ 會計）→ 聚合成「壓制者 × 受害者」矩陣。
  - `substitution_ablation(sample_pdf, parameters) -> dict`：逐 item 把分數換成該 item base rate 的 logit 常數、重算指標（$O(M)$ 次），得每 item 的淨貢獻／淨傷害。
  - **傷害 × 分群報表**（cohort 自動偵測的降級版；完整自動搜索因 no-additional-packages 限制不納，v2 再議）：把 per-row 傷害訊號按既有 `evaluation.segment_columns` 維度分組聚合，回答「傷害集中在誰身上」。**邊界注意**：segment 欄位在進 diagnosis 之前就已由 `prepare_eval_data` 的 `join_segment_sources` 併進 eval_predictions（`pipelines/evaluation/nodes_spark.py:166-169`）——diagnosis 只消費欄位、**不 import** `evaluation/segments.py`。產物併入 `pair_ledger.json` 的 `by_segment` 區塊。
- report 新 section `offset_sweep`（含 waterfall 圖，樣式沿 `fig6-offset-sweep-split.png`）；產物 `diagnosis/offset_sweep.json`、`diagnosis/pair_ledger.json`。
- **config**：`evaluation.diagnosis.offset_sweep: {enabled: true, shrink_lambda: 0.1, holdout_fraction: 0.5, max_rounds: 5, grid: {lo: -2.0, hi: 2.0, step: 0.05}}`；`evaluation.diagnosis.pair_ledger: {enabled: true}`。
- **驗證用注入鍵**：`evaluation.diagnosis.debug_inject_offsets: {}`（per-item 常數，在算指標**前**加到抽樣分數上；僅供驗收與測試，預設空、文件標明勿用於正式評估）。
- **consistency（A18）**：`0 < holdout_fraction < 1`、`shrink_lambda ≥ 0`、grid 井然（lo < hi、step > 0）、`debug_inject_offsets` 值為有限實數。

**驗收（真實執行）**：
1. 已知答案注入：`debug_inject_offsets` 對某 item 設 `+1.0`，跑 evaluation——sweep 的 $\delta^*_j$ 應 ≈ $-1.0$（容差配 CI 讀）、$\text{mAP}(\delta^*) > \text{mAP}(0)$（折外）；pair_ledger 應顯示該 item 的壓制次數暴增。
2. 清掉注入重跑：$\delta^*$ 全體接近 0（收縮生效）、折外收復量 ≈ 0（合成資料若無真實水準錯位）。
3. 檢視：分流 waterfall 圖與兩份 JSON。

---

### Phase 5：結構層＋triage 總表（框架診斷項目 8、9；判讀第 3 步定型）

**做什麼**：訓練側的 Gain 帳本與 per-item 背景 SHAP，加上把三層診斷合成單一 triage 判定表。

- 訓練側新檔 `src/recsys_tfb/diagnosis/model/gain_ledger.py`：
  - `compute_gain_ledger(model, parameters) -> dict`：經 `model.booster`（`LightGBMAdapter.booster:345`；SHAP 接縫同源 `attribution.py::_resolve_booster:10`）取 `trees_to_dataframe()`，跨樹按 item 記帳：item 特徵切點的樹序分佈、item 隔離後子樹內 context 切點數與累積 Gain（per item）、全模型 Gain 中 item-id vs context 的占比。
  - **Spike 前置**（進入實作前必須先驗證的技術點）：LightGBM 類別切分在 `trees_to_dataframe` 的 threshold 表示（category set）解析、與「item 隔離子樹」的 parent-child 遍歷可行性。Spike 失敗的退路：降級為「切點按特徵計數＋Gain 占比」的粗帳本（仍可判餓死型，只是不含子樹歸屬）。
  - 新 node `compute_gain_ledger` 加入 training pipeline（in=[`model`,`parameters`] out=`gain_ledger`），catalog 條目 `gain_ledger` → `data/models/${model_version}/diagnostics/gain_ledger.json`（沿 `shap_diagnostics` 慣例，`catalog.yaml:224`）。
- 條件化 SHAP：`diagnosis/model/shap_per_item.py::compute_shap_diagnostics`（平移前 `:90`）增 `diagnostics.shap.background: global | per_item` 選項——`per_item` 時背景樣本取自該 item 子母體（重用 `sampling.py::_stratified_item_sample:7`）。預設 `global`（現行為不變）。同時把 attribution 接縫（`diagnosis/model/attribution.py`）的簽名開放 explainer 選項傳遞——**只留參數空間、不實作 loss 模式**（loss-SHAP 為 v2 候選，見 §6）。
- triage 總表：新檔 `src/recsys_tfb/diagnosis/metric/triage.py`＋evaluation pipeline 新 node `assemble_triage_summary`（in=[`quadrant_summary`、`reconciliation`、`offset_sweep`、`gain_ledger`（catalog JSON，跨側靠產物不靠 import）、`parameters`] out=`triage_summary`）：per item 產出判定 {健康｜水準-配置型｜水準-指標再平衡型｜餓死型｜特徵缺失型} ＋建議槓桿（框架 Ch 5 的映射表寫成 code 常數表）＋**起手值欄**——配置型附 logQ offset 精確值（來自 Phase 2 對帳）、再平衡型附 $\delta^*$（來自 Phase 4）、餓死型附 item weight 起手式（$w_j \propto 1/\sqrt{P_j}$ 加上限，手冊3 Ch8），欄位明標「**起手值，須經快迴路驗證，非定案**」→ report 新 section `triage` ＋ `diagnosis/triage.json`。gain_ledger 缺席（訓練側未跑該 node）時 triage 降級為「無結構層證據」標註，不失敗（best-effort，沿 cases_manifest 慣例）。
- **config**：`diagnostics.gain_ledger: {enabled: true}`；`diagnostics.shap.background: global`；`evaluation.diagnosis.triage: {enabled: true}`。
- **consistency（A19）**：`background ∈ {global, per_item}`。

**驗收（真實執行）**：
1. `python -m recsys_tfb training --env local`（產 `gain_ledger.json`）→ `python -m recsys_tfb evaluation --env local --post-training`。
2. 檢視：`gain_ledger.json`（item-id 切點樹序、per-item 個人化 Gain）；report 的 triage 表——每 item 一列、判定＋建議槓桿＋支撐證據欄。
3. 已知答案（方向性）：最冷合成 item 的 per-item 個人化 Gain 應顯著低於熱門（手冊3 Ch4 的預測在合成資料上重現）；Phase 2 曾注入過採樣偏移的組合重演一次，triage 應把該 item 判成「水準-配置型」而非餓死型。

---

## 4. 新增 config 總表

| 鍵 | 預設 | 讀取者 | Phase |
|---|---|---|---|
| `evaluation.metric.{weight_alpha,k,min_positives,shrinkage_k}` | `0.0 / null / 0 / 0`（＝現行為） | metrics.py、metrics_spark.py | 1 |
| `evaluation.diagnosis.sample.{max_queries,min_pos_queries_per_item,seed}` | `200000 / 50 / 42` | diagnosis/metric/sample.py | 1 |
| `evaluation.diagnosis.ci.{enabled,n_boot}` | `true / 200` | uncertainty.py | 1 |
| `evaluation.diagnosis.reconciliation.{enabled,score_col,explained_threshold}` | `true / score_uncalibrated / 0.3` | reconciliation.py | 2 |
| `evaluation.diagnosis.quadrant.{enabled,auc_threshold,gap_band,top_k_occupancy}` | `true / 0.6 / 0.35 / 1` | quadrant.py | 3 |
| `evaluation.diagnosis.offset_sweep.{enabled,shrink_lambda,holdout_fraction,max_rounds,grid}` | `true / 0.1 / 0.5 / 5 / [-2,2,0.05]` | offset_sweep.py | 4 |
| `evaluation.diagnosis.pair_ledger.enabled` | `true` | pair_ledger.py | 4 |
| `evaluation.diagnosis.debug_inject_offsets` | `{}` | diagnosis/metric/sample.py | 4 |
| `evaluation.diagnosis.triage.enabled` | `true` | triage.py | 5 |
| `diagnostics.gain_ledger.enabled` | `true` | gain_ledger.py | 5 |
| `diagnostics.shap.background` | `global` | shap_per_item.py | 5 |
| `evaluation.report.sections` 增列 | `reconciliation, quadrant, offset_sweep, triage` | report_builder.py | 2–5 |

命名注意：既有 `evaluation.report.diagnostics`（`parameters_evaluation.yaml:65`）是**報表圖表開關**，與新的 `evaluation.diagnosis`（診斷計算配置）是兩件事——保留兩者、在 yaml 註解裡互相指路，不合併（合併會破壞既有 report 測試與使用習慣）。

## 5. 測試策略

- **單元（純 python/numpy，最快迴路）**：metrics 家族參數、bootstrap、offset sweep 收斂與收縮、pair ledger、theoretical_offsets、triage 判定表。每個新模組一個測試檔，照 `tests/test_evaluation/` 既有版型。
- **Spark 整合（local[*] fixture 既有模式）**：within_item_auc 與 numpy 對照 parity、suppression_counts、calibration_gap_by_item、diagnosis_sample 分層保證、每個新 node 的管線接線（照 `TestEvaluationPipelineDefault` 版型）。
- **known-answer 注入測試**：Phase 2 的 $-\log r$ 與 Phase 4 的 `debug_inject_offsets` 各寫成自動化整合測試（不只人工驗收）。
- **回歸鎖**：Phase 1 預設參數下 `compute_all_metrics` 輸出 dict 與現行 shape/值完全一致（沿 `test_backward_compatible_keys_present` 慣例）。
- **baseline 紀律**：動工前先在 main 記錄既有 failing/互擾測試 baseline（`docs/operations/known-pitfalls.md` §5），結果對照 baseline 而非絕對全綠。

## 6. 明確不做（v1 邊界）

1. offset 的**部署**（predict 後處理）——診斷產物裡有 $\delta^*$，要不要部署等真實數字。
2. loss-SHAP、cohort 偵測、處方自動化——不在本案；要不要做、以什麼形式做（repo 內或獨立工具）皆未定，等本案落地取得真實讀數後再評估。
3. influence／訓練資料歸因。
4. HPO objective 的參數化對齊（§1 不變量 5）。
5. 公司環境驗證——每階段驗收都在本機合成資料；公司規模的成本（bootstrap、sweep 的抽樣上限是否夠）標為部署前另驗項。
6. 指標權重定案——參數化保留光譜，定案是拿到真實資料讀數後的使用者決策。

## 7. 風險與 spike 清單

| 風險 | 影響 | 對策 |
|---|---|---|
| Phase 0 平移引入隱性行為變化（import 副作用、路徑常數） | 全案地基不穩 | 行為不變三重閘門（測試 baseline、產物逐檔 diff、diff --stat 稽核）；平移獨立成 commit 不夾邏輯 |
| gain ledger 的樹遍歷／類別切分解析比預期難 | Phase 5 延誤 | 進 Phase 5 前先做 spike；退路＝粗帳本（§3 Phase 5） |
| 診斷抽樣在公司規模下失真（每 item 正例覆蓋不足） | 讀數噪音 | 抽樣 metadata 強制入報表；`min_pos_queries_per_item` 保底＋不足 take-all |
| bootstrap × sweep 的 driver 記憶體 | OOM | `max_queries` 上限＋抽樣後欄位裁剪（5 個核心欄＋配置的 `segment_columns`，Phase 4 的 by_segment 需要）；成本在 Phase 1 驗收時實測記錄 |
| 多維 sampling override 的 per-item 聚合近似誤導對帳 | 誤判「不可解釋」 | 產物按完整 group key 細列；item 層摘要標註近似 |
| report 章節膨脹拖慢載入 | 使用性下降 | 沿 PR#80 的 Spark 端聚合紀律：圖只吃聚合結果；JSON 與 HTML 分離 |

## 8. 分階段驗收總覽（使用者閘門）

| Phase | 真實執行指令（本機） | 你檢視的產物 | 已知答案閘門 |
|---|---|---|---|
| 0 診斷域歸位 | `training --env local` 重跑 | `git diff --stat`、測試結果 vs baseline、diagnostics JSON 逐檔 diff | 產物與平移前完全一致、model_version 不變 |
| 1 指標基座 | dataset → training → `evaluation --env local --post-training` | report per-item CI 欄、`diagnosis/metric_ci.json` | 預設參數輸出與現行完全一致；`min_positives` 調高後觀察名單出現 |
| 2 對帳層 | 注入 `sample_ratio_overrides: 0.5` 後全鏈重跑 | report `reconciliation` 節、`diagnosis/reconciliation.json` | 理論 offset ≈ +0.693 且 verdict 可解釋；還原後全綠 |
| 3 象限 | `evaluation`（可 `--only-node` 切片） | 象限散布圖＋表、`diagnosis/quadrant_summary.json` | 最冷 item 落判別力差半邊；AUC Spark/numpy parity |
| 4 分流 | `debug_inject_offsets: {item: +1.0}` 跑 evaluation | 分流 waterfall、`offset_sweep.json`、`pair_ledger.json`（含 by_segment） | $\delta^*_j \approx -1.0$、折外 mAP 回升；清注入後 $\delta^*\approx 0$；分群表方向與注入一致 |
| 5 結構層＋triage | training（gain_ledger）→ evaluation（triage） | `gain_ledger.json`、report triage 表、`diagnosis/triage.json` | 冷 item 個人化 Gain 顯著低；注入偏移組合被判「水準-配置型」 |

每個閘門：使用者檢視 → 回饋 → 修 → 通過才開下一階段的實作。任何閘門發現設計層問題，回本 spec 修訂（spec 是活文件，修訂附日期）。
