# 診斷框架開發交接檔（/compact 前固化，2026-07-07）

> 給續作 session：讀完本檔＋下列兩份文件，即可直接開工，不需要舊對話。

## 現在進行到哪

- **Phase 0 已完成並通過使用者閘門**。branch `feat/diag-framework`（worktree `.worktrees/diag-framework`）@ `ee9caef`。
- **下一步＝寫 Phase 1（指標基座）的實作計畫**，然後 subagent-driven 執行。

## 唯一真實來源（先讀這兩份）

1. **Spec**：`docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md`——六階段（Phase 0–5）的完整設計，含每階段的檔案清單、config 鍵、consistency 代號（A15 起）、已知前置缺口與驗收閘門。**Phase 1 要做什麼全在 §3 Phase 1**，關鍵接縫的檔案:行號都寫在裡面（行號以 main 的平移前狀態為準；診斷模組已搬家，見下）。
2. **Phase 0 計畫**（當範本用）：`docs/superpowers/plans/2026-07-07-diag-phase0-relocation.md`——後續 phase 計畫照這個規格寫（零佔位符、每步完整指令＋預期輸出、真跑閘門收尾）。

方法論背景（需要時再讀）：`docs/ranking-diagnosis-framework.md`（診斷框架手冊）；`docs/notes/2026-07-05-ranking-diagnosis-direction.md`（方向診斷）。

## Phase 0 之後的 code 現狀（行號級事實）

- 新套件：`src/recsys_tfb/diagnosis/`——`model/`＝原 `pipelines/training/diagnostics/` 全部＋`population_spark.py`（原 `diagnostics_spark.py`）；`metric/`＝空殼（Phase 1–5 之家）。依賴白名單（`diagnosis/__init__.py` docstring）：`pipelines/* → diagnosis → core / evaluation(僅 numpy 原語) / io / utils`。
- `src/recsys_tfb/utils/hashing.py`＝原 `pipelines/dataset/_hashing.py`（`spark_bucket`/`ratio_to_threshold`）。
- 測試新家：`tests/test_diagnosis/test_model/`（6 檔）；`tests/test_utils/test_hashing.py`。
- **注意**：`src/recsys_tfb/evaluation/diagnostics_spark.py` 是**另一個**既有模組（評估側圖表聚合），沒搬、別混淆。

## 本機環境狀態（worktree 內已就緒）

- local Spark 已 setup（合成資料＋warehouse），已訓模型 `model_version=6059dcef`（`data/models/6059dcef/`，含 diagnostics 產物）。
- Phase 0 基準檔在 `/tmp`（重開機會消失，Phase 1 不依賴它們）：`/tmp/phase0_mv.txt`、`/tmp/phase0_diag_before/`、`/tmp/phase0_test_baseline.txt`。
- 測試 baseline 事實：`tests/test_pipelines/test_training + test_dataset`＝209 passed 零 fail；加 `tests/test_diagnosis + tests/test_utils`＝238 passed。

## 已確立的執行協議（使用者定案，勿走回頭路）

1. **一階段一份計畫**；每階段結束＝本機真跑產物＋已知答案注入的閘門，**使用者檢視通過才進下一階段**。
2. **subagent token 成本控制**（使用者明示）：機械步驟 controller 直跑不派 agent；多檔改寫派 sonnet implementer；審查合併成單一 reviewer（sonnet）；opus 只在階段收尾做一次總審。
3. 行為不變類改動的判準＝與 baseline 一致，不是絕對全綠。
4. shaprx 已擱置，任何規劃不得引用它當既有資產或邊界。
5. HPO objective 本案不動；指標參數化預設值必須等價現行為。

## 踩坑教訓（Phase 0 實證）

- sed 連續字串 pattern 抓不到**分離式 import**（`from X import Y as Z`）——每次批次改寫後必須用寬 pattern 複查（已寫進 Task 4 之後的派工範式）。
- 整條重訓的位元重現性未鎖（config 無顯式 seed）——產物比對類閘門用 pipeline 切片（`--from-node`）吃同一 model artifact，不要整條重跑。

## Phase 1 的三個開工提醒（spec 裡都有，這裡只標重點）

1. **先修前置缺口**：`evaluation/metrics_spark.py::aggregate_per_item`（`:447`）要增列 per-item 正例數 `n_pos`（additive），否則 `weight_alpha/min_positives/shrinkage_k` 在 Spark 側數學上做不到、會靜默退回等權。
2. 指標參數化留在 `evaluation/`（評估本體）；CI（`bootstrap_per_item_ci`）與診斷抽樣（兩趟設計）進 `diagnosis/metric/`。
3. consistency 新代號從 **A15** 起；新增法＝predicate 函式＋`validate_config_consistency`（`consistency.py:476`）串接＋docstring legend。
