# Phase 4a：指標層分流（offset sweep）— 實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 框架的分流閥（診斷項目 6）：在診斷抽樣上（driver-side numpy）對每個 item 的 logit 分數加常數 δ、座標下降搜尋讓參數化 macro per-item mAP 最大的 δ*——折外收復量＝「純水準可收復的缺口」，收不回的部分＝條件判別力缺口。落 `diagnosis/offset_sweep.json`、報表新 section（plotly waterfall）與判讀手冊擴充。依 spec `docs/superpowers/specs/2026-07-06-diagnosis-pipeline-integration-design.md` §3 Phase 4（131–151 行）。**pair_ledger 不在本計畫**——使用者已裁決 Phase 4 拆兩個閘門（2026-07-08），pair_ledger＝Phase 4b、另一份計畫。

**Architecture:** 一個新診斷模組 `diagnosis/metric/offset_sweep.py`（`sweep(sample_pdf, parameters) -> dict`：logit 變換→注入→query 層切折→座標下降→折內/折外 mAP＋per-item LOO 拆解）。評估 pipeline 加薄節點 `compute_offset_sweep`（重抽 `draw_diagnosis_sample`，同 seed＝同一份樣本），報表加 `build_offset_sweep_section`（`go.Waterfall`）。config `evaluation.diagnosis.{offset_sweep,debug_inject_offsets}`＋consistency **A18**。

**Tech Stack:** numpy／pandas（driver-side，無 Spark 聚合——與 Phase 2/3 模式不同）、`evaluation/metrics.py` 參數化 numpy 家族當目標函數（依賴白名單允許）、plotly `go.Waterfall`、pytest、本機 local Spark（只有節點入口的抽樣走 Spark）。

**Scope note:** 閘門**只跑 evaluation**（model_version 6059dcef，零重訓）；已知答案＝(a) 單元測試的「唯一最優平台」fixture（δ* 恰＝−1.0）與乾淨資料 δ* 全零、(b) 真跑注入 `debug_inject_offsets: {ccard_ins: 1.0}` → δ*_ccard_ins ≈ −1.0＋折外 mAP(δ*)>mAP(0)＋**其餘三份 diagnosis JSON 位元不變**（注入只影響本節點的負控制）、(c) 清除注入 → offset_sweep.json 與現狀位元一致（全程確定性）。

---

## 執行者必讀（違反會靜默做錯）

1. **一切都在 worktree**：repo root＝`/Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework`，branch `feat/diag-framework`。每個 Bash 指令以 `cd <該路徑> && ...` 開頭；Edit/Write 絕對路徑必含 `.worktrees/diag-framework`。
2. **跑 python 一律**：`PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest|recsys_tfb ...`。
3. **可能超過 2 分鐘的指令（evaluation 真跑）一律背景執行**。evaluation CLI 必帶 `--model-version 6059dcef`（無 `best` symlink；promote 是使用者保留的人工步驟）。
4. **生產不變量**：no Spark UDF、no new packages；`diagnosis/*` 只 import `core / evaluation(僅 numpy 原語 metrics.py) / io / utils`＋pandas/numpy/標準庫；**plotly 不進 diagnosis**（waterfall 在報表側建）。
5. **本階段不動 dataset/training config**：閘門注入只動 `evaluation.diagnosis.debug_inject_offsets`（不影響 model_version），結束時還原。
6. 測試判準＝與 baseline 一致（known-pitfalls §5）；`SPARK_LOCAL_IP=127.0.0.1` 已釘進 conftest，不需手動設。
7. 欄名一律經 `get_schema(parameters)` 取（`from recsys_tfb.core.schema import get_schema`），勿硬編 `prod_name`/`snap_date`。

## 設計定案（所有 task 共用語意，不要各自發明）

- **δ 的尺度＝log-odds**：排序分數（`schema["score"]`，實際決定上線排序的分數）先 clip 到 `[1e-12, 1−1e-12]` 再 logit 變換，δ 加在 logit 上。理由：(a) 與 Phase 2 對帳層 offset、`gap_band` 同尺度，Phase 5 triage 可直接把 δ* 當再平衡起手值；(b) spec 注入閘門「+1.0 → δ*≈−1.0」只有在 log-odds 下才有意義。**score 超出 (0,1) 的防禦**：整欄 min<0 或 max>1 時**略過 logit**、直接在原始分數上平移＋`notes` 註記（強行 clip 會把界外值壓成同值、毀掉排序）。
  - 用 `score` 而非 reconciliation 的 `score_uncalibrated`：對帳層看模型原始輸出是**歸因**；分流層是**干預模擬**——「對現在實際上線的排序做 per-item 平移能收復多少」，所以作用在上線分數。
- **目標函數＝Phase 1 的參數化家族**：`compute_macro_per_item_map(groups, items, y_true, z+off, k, weight_alpha, min_positives, shrinkage_k)`，四個參數逐一從 `evaluation.metric` 讀（`parameters_evaluation.yaml:80-84`）——sweep 的「收復量」必須跟報表主指標同一定義才可判讀。輸出 echo `metric_params`。
- **注入（`debug_inject_offsets`）語意**：在**一切計算之前**加到 logit 分數上（含 mAP(0)——「zero」＝被注入後的現狀，sweep 量的是「從現狀能收復多少」）。**scope 只在 offset_sweep 模組內**：metric_ci／reconciliation／quadrant 完全不受影響（真跑閘門以三份 JSON 位元不變當負控制）。注入非空時 `notes` 註記；注入鍵不在抽樣 item 中也註記（無作用防呆）。
- **折切**：query 層（不是 row 層）。`groups` 用 `sample_pdf.groupby(query_cols, sort=False).ngroup()`（連續碼 0..n−1），`np.random.RandomState(sample.seed)`（重用 `evaluation.diagnosis.sample.seed`，offset_sweep config 不另設 seed——spec 的 config 清單沒有它）permutation 取前 `round(n×holdout_fraction)` 個 query 當 holdout，且 `n_hold ≤ n−1` 保 fit 非空；holdout 空（query 太少）→ 折外指標 None＋notes，不炸。δ 在 fit 折搜尋；mAP(0)/mAP(δ*) 折內折外都報。
- **座標下降**：item 按字典序輪流；每個 item 在 grid 上一維掃描（grid＝`lo + step*arange`，浮點 round 到 10 位；grid 不含精確 0 時插入 0.0——δ=0 必須可達）。**座標選擇目標＝`mAP_fit − shrink_lambda·g²/M`**（M＝抽樣中 unique item 數；等價於全域懲罰 `λ·mean_j(δ_j²)`，除以 M 讓 λ 的意義不隨 item 數變）。**平手偏向 |g| 小**：候選按 |g| 升冪迭代、只有嚴格改善（>1e-12）才換——這保證 (a) 乾淨資料 δ* 恰為 0（不在雜訊平台上亂走）、(b) 注入場景 δ* 取「最優平台靠 0 的那端」。一整輪無任何 δ 變動＝收斂提前停；回報 `n_rounds_run`、`converged`。
  - **數感檢查（λ 預設 0.1 不會擋掉注入收復）**：懲罰在 δ=1、M=8 時＝0.1/8≈0.0125 mAP；+1.0 注入在合成資料上造成的 mAP 損傷遠大於此（單元測試的交錯 fixture 收復增益≈0.089、對應懲罰 0.1·1²/3≈0.033，也過）。同時 0.0125 足以壓掉 654-query 折內的雜訊級增益。若真跑發現量級假設錯，屬執行時裁決：改 λ 預設須在 spec 註記日期與證據。
- **per-item 缺口拆解＝折外 LOO**：`contribution_j = mAP_holdout(δ*) − mAP_holdout(δ* 但 δ_j←0)`，只算 δ*_j≠0 的 item（＝0 者貢獻恆 0）。LOO 不保證加總＝總收復量，差額另報 `interaction_residual_holdout`（waterfall 用它補橋）。
- **輸出 dict（JSON-ready，鍵名即契約，報表與文件都吃它）**：頂層 `enabled / score_col_used / params(echo) / metric_params(echo) / injected_offsets(echo) / items / delta_star{item:float} / n_rounds_run / converged / map_fit{zero,star} / map_holdout{zero,star} / recovered_gap_holdout / per_item{item:{delta_star, loo_contribution_holdout}} / interaction_residual_holdout / n_queries_fit / n_queries_holdout / notes[]`；節點再補 `sample`（抽樣 metadata，沿 `compute_metric_ci` 慣例 `nodes_spark.py:289-291`）。空抽樣 → 各指標 None／空 dict＋notes，不炸。
- **停用 stub／必要輸入**：沿 `compute_quadrant` 模式（`nodes_spark.py:327-358`）——`enabled: false` → `{"enabled": False}` stub；enabled 但 `eval_predictions is None` → `ValueError`。
- **waterfall 走 plotly `go.Waterfall`（非 matplotlib）**：手冊 fig6 原圖是 matplotlib（`docs/diagrams/ranking-diagnosis/make_figures.py:219-245`），spec 已修訂報表圖一律 plotly（證據 `distributions.py:9`）。沿用 fig6 的顏色語意：藍（`#1565c0`）＝收復（increasing）、橘（`#e65100`）＝負向（decreasing）、灰＝絕對值柱。全體 δ*=0（無可收復）→ 不畫圖只留表，description 說明。
- **既有測試會被本計畫「合法」改到的只有**：`tests/test_pipelines/test_evaluation/test_pipeline.py` 結構斷言——default/post_training **8→9** node、compare-source **11→12**、node 名清單加 `compute_offset_sweep`（`compute_quadrant` 之後、`generate_report` 之前）、outputs 加 `evaluation_offset_sweep`、`generate_report` inputs 斷言（若有）加同名（Phase 1–3 同款，**預先授權**）。其他既有測試檔若有 report sections／diagnosis config 鍵的 exact-set 斷言需 additive 更新，同屬預先授權；**任何非 additive 的既有測試改動 → 停下回報**。
- **閘門的已知答案基準數字（Phase 3 真跑 6059dcef）**：`ccard_ins` gap_vs_global ≈ +0.329（Phase 2 起的內建答案 item，正例 query 數充足），故注入 item 選 `ccard_ins`、量 +1.0。容差配 CI 讀：δ*_ccard_ins ∈ **[−1.35, −0.6]**（收縮＋平台靠 0 端＋抽樣雜訊；不是精確 −1.0），其餘 item |δ*| ≤ 0.15，折外 `recovered_gap_holdout > 0`。現狀（無注入）預期 |δ*| 全部 ≤ 0.15（Phase 2 已證合成資料水準都在帶內；實際值記錄下來即可，超出就如實回報討論）。
- **效能（HANDOFF 開工提醒 2 的估算）**：成本 ＝ `rounds × M × |grid| × lexsort(fit 列數)`。本機：≤5×8×81×(~2,600 列 lexsort) ≈ 3,240 次評估，秒級。公司規模上限（22 item、200k query ≈ 4.4M 列）：≤8,910 次 × 每次 ~0.5–1.5s ≈ **1.2–3.7 小時最壞情況**（收斂提前停通常遠少於 max_rounds）。緩解＝調粗 `grid.step`（0.1 → 41 點折半）／降 `max_rounds`／降 `sample.max_queries`。Task 7 實測本機節點耗時、按列數比例外推，寫進手冊已知限制——**不做進一步演算法優化**（v1 取簡單正確）。
- **文件是一等交付物（spec §3 固定結構）**：Task 8 內建，契約見該 task；寫法鐵則（禁用開發詞彙、真跑示例表印進文件、數感節、讀者 agent 驗洩漏）不可省。

## 執行模式（controller 注意）

同 Phase 3＋提速協議（HANDOFF 執行協議 9）：Task 1、7 controller 直跑；**Task 2 派 sonnet implementer A**；**Task 3–5 合批派 sonnet implementer B**（同 setup、內部逐 task TDD＋各自 commit；3–5 都會碰 nodes_spark/report 一帶，不得與 Task 2 並行——但 B 開工時 2 已完）；**Task 6 合併 reviewer（sonnet）背景執行**、與 Task 7 真跑並行，prompt 附 controller 綠燈證據＋明令只讀 diff、只跑新增/變更測試檔；Task 8 文件 writer 的素材包由 controller 從 Task 7 產物先備好；**Task 9 opus 總審背景執行**。所有 implementer prompt **直接內嵌該 task 全文＋執行者必讀＋設計定案**，計畫檔路徑只作查證。

---

### Task 1：pre-flight ＋ baseline（controller 直跑）

**Files:** 無程式碼變更；產出 `/tmp/phase4a_test_baseline.txt`、`/tmp/phase4a_report_before.html`、`/tmp/phase4a_json_before/`。

- [ ] **Step 1: pre-flight**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && pwd && readlink .venv && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation && \
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework status --short
```
Expected: worktree root、Python 3.10.9、isolation OK、working tree 乾淨。

- [ ] **Step 2: 相關測試 baseline（背景）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/ tests/test_core/test_consistency.py \
  tests/test_evaluation/test_report_builder.py \
  tests/test_pipelines/test_evaluation/ \
  -q 2>&1 | tail -5 | tee /tmp/phase4a_test_baseline.txt
```
Expected: 全綠（Phase 3 收尾 297 passed 的同一集合），記下確切數字。

- [ ] **Step 3: 產物快照（改動前基準）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
mkdir -p /tmp/phase4a_json_before && \
cp data/evaluation/6059dcef/20260131/report.html /tmp/phase4a_report_before.html && \
cp data/evaluation/6059dcef/20260131/diagnosis/*.json /tmp/phase4a_json_before/ && \
ls -la /tmp/phase4a_json_before/
```
Expected: metric_ci.json、reconciliation.json、quadrant_summary.json 三份＋report 快照。

---

### Task 2：`offset_sweep.py` 模組＋單元測試（implementer A）

**Files:**
- Create: `src/recsys_tfb/diagnosis/metric/offset_sweep.py`
- Test: `tests/test_diagnosis/test_metric/test_offset_sweep.py`

- [ ] **Step 1: 寫失敗測試（完整檔）**

`tests/test_diagnosis/test_metric/test_offset_sweep.py`（測試用 parameters fixture 的 schema 結構若與 `tests/test_diagnosis/test_metric/test_quadrant.py` 的既有 fixture 不合——例如 `get_schema` 要求額外鍵——以該檔既有寫法為準對齊，屬合法調整，回報即可）：

```python
"""offset_sweep 單元測試。

交錯 fixture 設計（見 _interleaved_pdf）：每個 item 同時有正例與負例、
與其他 item 的列以 0.02 logit 的緊margin 交錯排列——乾淨資料下排序是
唯一最優（任何 item 任何方向的非零平移都會翻掉至少一對、mAP 嚴格變差），
所以 δ* 必須全零。對 A 注入 +1.0 後，完整復原的 δ_A 平台是 (−1.02, −0.98)
——上邊界＝A 正例跌破 B 負例、下邊界＝A 負例仍壓著 C 正例——grid
（step 0.05）落在平台內的點只有 −1.00，故 δ*_A 恰等於 −1.0，不靠容差。
"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.offset_sweep import sweep


def _sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.asarray(z, dtype=np.float64)))


def _params(inject=None, **sweep_overrides):
    cfg = {
        "enabled": True,
        "shrink_lambda": 0.1,
        "holdout_fraction": 0.5,
        "max_rounds": 5,
        "grid": {"lo": -2.0, "hi": 2.0, "step": 0.05},
    }
    cfg.update(sweep_overrides)
    return {
        "schema": {
            "time": "snap_date",
            "entity": ["cust_id"],
            "item": "prod_name",
            "label": "label",
            "score": "score",
            "rank": "rank",
        },
        "evaluation": {
            "metric": {"weight_alpha": 0.0, "k": None,
                       "min_positives": 0, "shrinkage_k": 0},
            "diagnosis": {
                "sample": {"max_queries": 200000,
                           "min_pos_queries_per_item": 50, "seed": 42},
                "offset_sweep": cfg,
                "debug_inject_offsets": dict(inject or {}),
            },
        },
    }


def _interleaved_pdf(n_queries=12):
    """每 query 六列（logit 降冪）：
    A+ 1.00 > B- 0.98 > B+ 0.50 > C- 0.48 > C+ 0.10 > A- 0.08。

    每個 item 都有正例與負例，相鄰對的 margin 都是 0.02——乾淨資料下任何
    item 任何方向的非零平移（|δ| ≥ 0.05 一格）都會翻掉至少一對、mAP 嚴格
    變差，δ* 被釘在 0。對 A 注入 +1.0：A+ 2.00、A- 1.08，正例掉到
    rank 1/4/6；復原平台 δ_A ∈ (-1.02, -0.98)——δ_A > -1.02 保 A+ 在
    B-（0.98）之上、δ_A < -0.98 讓 A-（1.08+δ）回到 C+（0.10）之下。
    """
    rows = []
    for q in range(n_queries):
        cust = f"c{q:03d}"
        rows += [
            ("20260131", cust, "A", 1, _sigmoid(1.00)),
            ("20260131", cust, "B", 0, _sigmoid(0.98)),
            ("20260131", cust, "B", 1, _sigmoid(0.50)),
            ("20260131", cust, "C", 0, _sigmoid(0.48)),
            ("20260131", cust, "C", 1, _sigmoid(0.10)),
            ("20260131", cust, "A", 0, _sigmoid(0.08)),
        ]
    return pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "label", "score"]
    )


class TestKnownAnswerInjection:
    def test_injected_offset_recovered_exactly_on_plateau_fixture(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["delta_star"]["A"] == pytest.approx(-1.0)
        assert out["delta_star"]["B"] == 0.0
        assert out["delta_star"]["C"] == 0.0

    def test_holdout_map_improves_under_injection(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["map_holdout"]["star"] > out["map_holdout"]["zero"]
        assert out["recovered_gap_holdout"] == pytest.approx(
            out["map_holdout"]["star"] - out["map_holdout"]["zero"]
        )

    def test_injection_echoed_and_noted(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["injected_offsets"] == {"A": 1.0}
        assert any("debug_inject_offsets" in n for n in out["notes"])

    def test_unknown_injection_key_noted(self):
        out = sweep(_interleaved_pdf(), _params(inject={"nosuch": 1.0}))
        assert any("nosuch" in n for n in out["notes"])

    def test_loo_contribution_positive_for_recovered_item(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["per_item"]["A"]["loo_contribution_holdout"] > 0
        # δ*=0 的 item 不算 LOO（恆 0）
        assert out["per_item"]["B"]["loo_contribution_holdout"] is None

    def test_interaction_residual_closes_the_bridge(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        loo_sum = sum(
            v["loo_contribution_holdout"] or 0.0
            for v in out["per_item"].values()
        )
        assert out["interaction_residual_holdout"] == pytest.approx(
            out["recovered_gap_holdout"] - loo_sum
        )


class TestCleanData:
    def test_clean_data_all_deltas_zero(self):
        out = sweep(_interleaved_pdf(), _params())
        assert all(v == 0.0 for v in out["delta_star"].values())
        assert out["recovered_gap_holdout"] == pytest.approx(0.0)

    def test_converges_before_max_rounds(self):
        out = sweep(_interleaved_pdf(), _params())
        assert out["converged"] is True
        assert out["n_rounds_run"] <= 2


class TestMechanics:
    def test_deterministic(self):
        a = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        b = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert a == b

    def test_holdout_split_counts(self):
        out = sweep(_interleaved_pdf(n_queries=12), _params())
        assert out["n_queries_fit"] + out["n_queries_holdout"] == 12
        assert out["n_queries_holdout"] == 6  # round(12 * 0.5)

    def test_single_query_leaves_holdout_empty_without_crash(self):
        out = sweep(_interleaved_pdf(n_queries=1), _params())
        assert out["n_queries_holdout"] == 0
        assert out["map_holdout"]["zero"] is None
        assert any("折外" in n or "holdout" in n.lower() for n in out["notes"])

    def test_empty_sample_returns_stub_shape(self):
        out = sweep(_interleaved_pdf().iloc[0:0], _params())
        assert out["delta_star"] == {}
        assert out["notes"]

    def test_score_outside_unit_interval_skips_logit_with_note(self):
        pdf = _interleaved_pdf()
        pdf["score"] = pdf["score"] * 10.0  # 超出 (0,1)
        out = sweep(pdf, _params())
        assert any("logit" in n for n in out["notes"])

    def test_metric_params_and_config_echoed(self):
        out = sweep(_interleaved_pdf(), _params())
        assert out["metric_params"] == {
            "k": None, "weight_alpha": 0.0,
            "min_positives": 0, "shrinkage_k": 0.0,
        }
        assert out["params"]["shrink_lambda"] == 0.1
        assert out["score_col_used"] == "score"

    def test_grid_without_exact_zero_still_reaches_zero(self):
        # lo=-0.07, step 0.05 → grid 無精確 0，實作須插入 0.0
        out = sweep(_interleaved_pdf(), _params(grid={"lo": -0.07, "hi": 0.08,
                                                  "step": 0.05}))
        assert all(v == 0.0 for v in out["delta_star"].values())
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_offset_sweep.py -q 2>&1 | tail -3
```
Expected: FAIL（ModuleNotFoundError: offset_sweep）。

- [ ] **Step 3: 實作模組（完整檔）**

`src/recsys_tfb/diagnosis/metric/offset_sweep.py`：

```python
"""Offset sweep（分流閥，框架診斷項目 6；spec §3 Phase 4）。

在診斷抽樣上（driver-side numpy）對每個 item 的 logit 分數加常數 δ_j，
座標下降搜尋讓參數化 macro per-item mAP 最大的 δ*。判讀語意：折外
mAP(δ*) − mAP(0) ＝「純水準（per-item 平移）可收復的指標缺口」；收不回
的部分＝條件判別力缺口（必須動訓練）。

設計要點（計畫「設計定案」節的落地）：
- δ 單位 log-odds：排序分數先 logit 變換再平移，與對帳層 offset 同尺度。
  整欄超出 (0,1) 時略過 logit（直接平移原始分數）＋ notes 註記。
- holdout：query 層切折（RandomState(sample.seed) permutation），δ 只在
  fit 折搜尋、mAP 兩折分開報告——防「收復缺口」只是擬合驗證雜訊。
- 收縮＋平手偏 0：座標選擇目標 = mAP_fit − shrink_lambda·g²/M，候選按
  |g| 升冪、僅嚴格改善才換——乾淨資料 δ* 恰為 0。
- debug_inject_offsets（僅驗收/測試）：在一切計算之前加到 logit 分數上，
  模擬已知水準錯位；mAP(0) 是注入後的現狀。scope 僅本模組。
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

logger = logging.getLogger(__name__)

_CLIP_EPS = 1e-12
_TIE_EPS = 1e-12


def _diag_cfg(parameters: dict) -> dict:
    return ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})


def _metric_params(parameters: dict) -> dict:
    m = ((parameters.get("evaluation", {}) or {}).get("metric", {}) or {})
    k = m.get("k")
    return {
        "k": int(k) if k is not None else None,
        "weight_alpha": float(m.get("weight_alpha", 0.0)),
        "min_positives": int(m.get("min_positives", 0)),
        "shrinkage_k": float(m.get("shrinkage_k", 0.0)),
    }


def _logit_scores(scores: np.ndarray) -> tuple[np.ndarray, list[str]]:
    s = np.asarray(scores, dtype=np.float64)
    if len(s) and (s.min() < 0.0 or s.max() > 1.0):
        return s.copy(), [
            "score 超出 (0,1)——略過 logit 變換，δ 單位為原始分數尺度"
        ]
    z = np.clip(s, _CLIP_EPS, 1.0 - _CLIP_EPS)
    return np.log(z / (1.0 - z)), []


def _grid(cfg: dict) -> np.ndarray:
    g = cfg.get("grid", {}) or {}
    lo = float(g.get("lo", -2.0))
    hi = float(g.get("hi", 2.0))
    step = float(g.get("step", 0.05))
    n = int(round((hi - lo) / step))
    grid = np.round(lo + step * np.arange(n + 1), 10)
    if not np.any(grid == 0.0):
        grid = np.sort(np.append(grid, 0.0))
    return grid


def _split_queries(
    groups: np.ndarray, holdout_fraction: float, seed: int
) -> tuple[np.ndarray, np.ndarray]:
    """query 層切折。groups 必須是連續碼 0..n-1（groupby().ngroup()）。"""
    n = int(groups.max()) + 1 if len(groups) else 0
    rng = np.random.RandomState(seed)
    perm = rng.permutation(n)
    n_hold = min(int(round(n * holdout_fraction)), n - 1) if n else 0
    hold_flag = np.zeros(n, dtype=bool)
    if n_hold > 0:
        hold_flag[perm[:n_hold]] = True
    hold_mask = hold_flag[groups] if len(groups) else np.zeros(0, dtype=bool)
    return ~hold_mask, hold_mask


def sweep(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    schema = get_schema(parameters)
    query_cols = [schema["time"], *schema["entity"]]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    diag = _diag_cfg(parameters)
    cfg = diag.get("offset_sweep", {}) or {}
    shrink_lambda = float(cfg.get("shrink_lambda", 0.1))
    holdout_fraction = float(cfg.get("holdout_fraction", 0.5))
    max_rounds = int(cfg.get("max_rounds", 5))
    seed = int((diag.get("sample", {}) or {}).get("seed", 42))
    mp = _metric_params(parameters)
    inject = {
        str(k): float(v)
        for k, v in (diag.get("debug_inject_offsets", {}) or {}).items()
    }
    notes: list[str] = []

    out: dict = {
        "enabled": True,
        "score_col_used": score_col,
        "params": {
            "shrink_lambda": shrink_lambda,
            "holdout_fraction": holdout_fraction,
            "max_rounds": max_rounds,
            "grid": dict(cfg.get("grid", {}) or {}),
        },
        "metric_params": mp,
        "injected_offsets": inject,
        "items": [],
        "delta_star": {},
        "per_item": {},
        "n_rounds_run": 0,
        "converged": False,
        "map_fit": {"zero": None, "star": None},
        "map_holdout": {"zero": None, "star": None},
        "recovered_gap_holdout": None,
        "interaction_residual_holdout": None,
        "n_queries_fit": 0,
        "n_queries_holdout": 0,
        "notes": notes,
    }
    if len(sample_pdf) == 0:
        notes.append("診斷抽樣為空——sweep 未執行")
        return out

    groups = sample_pdf.groupby(query_cols, sort=False).ngroup().to_numpy()
    items = sample_pdf[item_col].astype(str).to_numpy()
    y = sample_pdf[label_col].to_numpy()
    z, z_notes = _logit_scores(sample_pdf[score_col].to_numpy())
    notes.extend(z_notes)

    if inject:
        z = z + pd.Series(items).map(inject).fillna(0.0).to_numpy()
        notes.append(
            f"debug_inject_offsets 生效（僅本節點；mAP(0) 為注入後現狀）："
            f"{inject}"
        )
        unknown = sorted(set(inject) - set(items.tolist()))
        if unknown:
            notes.append(f"注入鍵不在抽樣 item 中（無作用）：{unknown}")

    fit_mask, hold_mask = _split_queries(groups, holdout_fraction, seed)
    out["n_queries_fit"] = int(len(np.unique(groups[fit_mask])))
    out["n_queries_holdout"] = int(len(np.unique(groups[hold_mask])))
    if out["n_queries_holdout"] == 0:
        notes.append("holdout 折為空（query 數過少）——折外指標無法報告")

    unique_items = sorted(set(items.tolist()))
    masks = {it: items == it for it in unique_items}
    n_items = len(unique_items)
    grid = _grid(cfg)
    # 候選按 |g| 升冪：平手時偏向 0（kind=stable 保同 |g| 的負值先於正值）
    grid_by_abs = grid[np.argsort(np.abs(grid), kind="stable")]

    def _map_on(mask: np.ndarray, off: np.ndarray):
        if not mask.any():
            return None
        return float(compute_macro_per_item_map(
            groups[mask], items[mask], y[mask], (z + off)[mask], **mp
        ))

    delta = {it: 0.0 for it in unique_items}
    off = np.zeros(len(z), dtype=np.float64)
    converged = False
    n_rounds_run = 0
    for _ in range(max_rounds):
        n_rounds_run += 1
        changed = False
        for it in unique_items:
            base_off = off - delta[it] * masks[it]
            best_g, best_obj = delta[it], -np.inf
            for g in grid_by_abs:
                m_fit = _map_on(fit_mask, base_off + g * masks[it])
                if m_fit is None:
                    continue
                obj = m_fit - shrink_lambda * (g ** 2) / n_items
                if obj > best_obj + _TIE_EPS:
                    best_obj, best_g = obj, float(g)
            if best_g != delta[it]:
                changed = True
                delta[it] = best_g
            off = base_off + delta[it] * masks[it]
        if not changed:
            converged = True
            break
    out["n_rounds_run"] = n_rounds_run
    out["converged"] = converged

    zero_off = np.zeros(len(z), dtype=np.float64)
    out["items"] = unique_items
    out["delta_star"] = dict(delta)
    out["map_fit"] = {
        "zero": _map_on(fit_mask, zero_off),
        "star": _map_on(fit_mask, off),
    }
    out["map_holdout"] = {
        "zero": _map_on(hold_mask, zero_off),
        "star": _map_on(hold_mask, off),
    }
    mh = out["map_holdout"]
    if mh["zero"] is not None and mh["star"] is not None:
        out["recovered_gap_holdout"] = mh["star"] - mh["zero"]

    per_item: dict = {}
    for it in unique_items:
        d = delta[it]
        loo = None
        if d != 0.0 and mh["star"] is not None:
            m_wo = _map_on(hold_mask, off - d * masks[it])
            if m_wo is not None:
                loo = mh["star"] - m_wo
        per_item[it] = {"delta_star": d, "loo_contribution_holdout": loo}
    out["per_item"] = per_item
    if out["recovered_gap_holdout"] is not None:
        loo_sum = sum(
            v["loo_contribution_holdout"] or 0.0 for v in per_item.values()
        )
        out["interaction_residual_holdout"] = (
            out["recovered_gap_holdout"] - loo_sum
        )
    logger.info(
        "offset sweep: %d items, rounds=%d converged=%s, "
        "holdout mAP %s -> %s",
        n_items, n_rounds_run, converged, mh["zero"], mh["star"],
    )
    return out
```

- [ ] **Step 4: 跑測試確認通過**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_offset_sweep.py -q 2>&1 | tail -3
```
Expected: 全 PASS。

- [ ] **Step 5: 突變檢查（證明測試真的覆蓋）**

把 `obj = m_fit - shrink_lambda * (g ** 2) / n_items` 的 `-` 改成 `+`（獎勵大 |δ|）→ 重跑，`TestCleanData::test_clean_data_all_deltas_zero` 必須轉紅；改回後再把 `grid_by_abs` 換成 `grid`（拿掉平手偏 0）→ `test_clean_data_all_deltas_zero` 或 `test_injected_offset_recovered_exactly_on_plateau_fixture` 至少一個轉紅（平台 fixture 對純 mAP 有唯一 argmax，若兩者皆綠則以乾淨資料含雜訊平手的補充 fixture 驗證，回報你的處置）。全部改回、確認全綠。回報你弄壞了哪兩處與各自轉紅的測試名。

- [ ] **Step 6: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
git add src/recsys_tfb/diagnosis/metric/offset_sweep.py \
        tests/test_diagnosis/test_metric/test_offset_sweep.py && \
git commit -m "feat(diagnosis): offset sweep 分流閥（座標下降+收縮+折外報告）"
```

---

### Task 3：config ＋ consistency A18（implementer B，第一段）

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml`（`quadrant:` 區塊之後，約 :124 之後）
- Modify: `src/recsys_tfb/core/consistency.py`（A17 `quadrant_param_errors` :577-608 之後加 predicate；`validate_config_consistency` :711 之後註冊；模組 docstring legend :90-92 之後加 A18 行）
- Test: `tests/test_core/test_consistency.py`（沿既有 A17 測試同款結構加 A18 案例）

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_core/test_consistency.py` 找到 A17（quadrant）測試區塊，鏡射其結構新增 A18 測試（函式級 import 與 helper 沿該檔既有慣例）：

```python
class TestOffsetSweepParamErrors:  # A18
    def test_defaults_pass(self):
        assert offset_sweep_param_errors(_valid_params()) == []

    def test_holdout_fraction_must_be_strictly_inside_unit_interval(self):
        for bad in (0.0, 1.0, -0.1, "0.5"):
            p = _valid_params()
            p["evaluation"]["diagnosis"]["offset_sweep"]["holdout_fraction"] = bad
            assert any("holdout_fraction" in e
                       for e in offset_sweep_param_errors(p))

    def test_shrink_lambda_nonnegative(self):
        p = _valid_params()
        p["evaluation"]["diagnosis"]["offset_sweep"]["shrink_lambda"] = -0.1
        assert any("shrink_lambda" in e for e in offset_sweep_param_errors(p))

    def test_grid_well_formed_and_straddles_zero(self):
        p = _valid_params()
        p["evaluation"]["diagnosis"]["offset_sweep"]["grid"] = {
            "lo": 2.0, "hi": -2.0, "step": 0.05}
        assert any("lo < hi" in e or "lo=" in e
                   for e in offset_sweep_param_errors(p))
        p["evaluation"]["diagnosis"]["offset_sweep"]["grid"] = {
            "lo": 0.5, "hi": 2.0, "step": 0.05}
        assert any("must contain 0" in e for e in offset_sweep_param_errors(p))
        p["evaluation"]["diagnosis"]["offset_sweep"]["grid"] = {
            "lo": -2.0, "hi": 2.0, "step": 0}
        assert any("step" in e for e in offset_sweep_param_errors(p))

    def test_max_rounds_positive_int_not_bool(self):
        for bad in (0, True, 2.5):
            p = _valid_params()
            p["evaluation"]["diagnosis"]["offset_sweep"]["max_rounds"] = bad
            assert any("max_rounds" in e for e in offset_sweep_param_errors(p))

    def test_enabled_must_be_bool(self):
        p = _valid_params()
        p["evaluation"]["diagnosis"]["offset_sweep"]["enabled"] = "false"
        assert any("enabled" in e for e in offset_sweep_param_errors(p))

    def test_inject_values_must_be_finite_numbers(self):
        for bad in (float("nan"), float("inf"), "1.0"):
            p = _valid_params()
            p["evaluation"]["diagnosis"]["debug_inject_offsets"] = {"x": bad}
            assert any("debug_inject_offsets" in e
                       for e in offset_sweep_param_errors(p))

    def test_registered_in_validate_config_consistency(self):
        p = _valid_params()
        p["evaluation"]["diagnosis"]["offset_sweep"]["shrink_lambda"] = -1
        with pytest.raises(ConfigConsistencyError, match="shrink_lambda"):
            validate_config_consistency(p)
```

（`_valid_params()`＝該檔既有的合法 parameters helper；名稱以檔內實際為準。）跑一次確認 FAIL（ImportError）。

- [ ] **Step 2: 加 predicate（`quadrant_param_errors` 之後）**

```python
def offset_sweep_param_errors(parameters: dict) -> list[str]:
    """evaluation.diagnosis.{offset_sweep,debug_inject_offsets} domains (A18)."""
    errors: list[str] = []
    diag = (
        (parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {}
    )
    cfg = diag.get("offset_sweep", {}) or {}

    f = cfg.get("holdout_fraction", 0.5)
    if not (_is_number(f) and 0.0 < float(f) < 1.0):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.holdout_fraction={f!r} must "
            f"be a number in (0, 1) — 0 leaves no holdout fold (out-of-fold "
            f"mAP undefined), 1 leaves nothing to fit on."
        )
    lam = cfg.get("shrink_lambda", 0.1)
    if not (_is_number(lam) and float(lam) >= 0.0):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.shrink_lambda={lam!r} must "
            f"be a number >= 0."
        )
    grid = cfg.get("grid", {}) or {}
    lo = grid.get("lo", -2.0)
    hi = grid.get("hi", 2.0)
    step = grid.get("step", 0.05)
    if not (_is_number(lo) and _is_number(hi) and float(lo) < float(hi)):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.grid lo={lo!r} hi={hi!r} "
            f"must be numbers with lo < hi."
        )
    elif not (float(lo) <= 0.0 <= float(hi)):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.grid [lo={lo!r}, hi={hi!r}] "
            f"must contain 0 — δ=0 (no shift) must stay reachable so the "
            f"sweep can report a clean baseline."
        )
    if not (_is_number(step) and float(step) > 0.0):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.grid.step={step!r} must be "
            f"a number > 0."
        )
    r = cfg.get("max_rounds", 5)
    if not (isinstance(r, int) and not isinstance(r, bool) and r >= 1):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.max_rounds={r!r} must be an "
            f"integer >= 1."
        )
    en = cfg.get("enabled", True)
    if not isinstance(en, bool):
        errors.append(
            f"evaluation.diagnosis.offset_sweep.enabled={en!r} must be a "
            f"bool (true/false without quotes in YAML)."
        )
    inj = diag.get("debug_inject_offsets", {}) or {}
    if not isinstance(inj, dict):
        errors.append(
            f"evaluation.diagnosis.debug_inject_offsets={inj!r} must be a "
            f"mapping item -> finite number (log-odds units)."
        )
    else:
        for key, v in inj.items():
            if not (_is_number(v) and math.isfinite(float(v))):
                errors.append(
                    f"evaluation.diagnosis.debug_inject_offsets[{key!r}]="
                    f"{v!r} must be a finite number (log-odds units)."
                )
    return errors
```

注意：先讀 `_is_number` 的定義——若它已排除 NaN/inf 則拿掉 `math.isfinite`（並調整測試斷言），否則檔首補 `import math`。`validate_config_consistency` 在 `errors.extend(quadrant_param_errors(parameters))`（:711）之後加一行 `errors.extend(offset_sweep_param_errors(parameters))`。模組 docstring 的 Invariant legend（:90-92 A17 行之後）加：`A18 evaluation.diagnosis.offset_sweep / debug_inject_offsets 參數域（分流層）`（格式鏡射 A15–A17 行）。

- [ ] **Step 3: 加 config（`parameters_evaluation.yaml` 的 `quadrant:` 區塊後）**

```yaml
    # 分流層（A18）：offset sweep——在診斷抽樣上（driver 端 numpy）對每個
    # item 的 logit 分數加常數 δ、座標下降搜尋讓參數化 macro mAP 最大的
    # δ*。δ 單位 log-odds（與對帳層 offset 同尺度）。holdout_fraction 切
    # query 折、折外報告防擬合雜訊；shrink_lambda 把 δ 向 0 收縮（座標目
    # 標 = mAP_fit − λ·δ²/M）。成本 ≈ rounds×items×grid點數 次 mAP 評估，
    # 大樣本時先調粗 grid.step。
    offset_sweep:
      enabled: true
      shrink_lambda: 0.1
      holdout_fraction: 0.5
      max_rounds: 5
      grid: {lo: -2.0, hi: 2.0, step: 0.05}
    # 驗收/測試專用：在 offset_sweep 計算前對指定 item 的 logit 分數加常數
    # （例 {ccard_ins: 1.0}）。只影響 offset_sweep 節點；正式評估必須留空。
    debug_inject_offsets: {}
```

- [ ] **Step 4: 跑測試確認通過＋commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_core/test_consistency.py -q 2>&1 | tail -3 && \
git add conf/base/parameters_evaluation.yaml src/recsys_tfb/core/consistency.py \
        tests/test_core/test_consistency.py && \
git commit -m "feat(consistency): A18 offset_sweep/debug_inject_offsets 參數域 + config 預設"
```

---

### Task 4：`compute_offset_sweep` 節點＋catalog＋pipeline 接線（implementer B，第二段）

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`（`compute_quadrant` :327-358 之後加節點；`generate_report` :361-436 簽名與 `assemble_report` 呼叫）
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py`（import :24-32；nodes list :101-113）
- Modify: `conf/base/catalog.yaml`（`evaluation_quadrant` :245-247 之後）
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`（鏡射 `compute_quadrant` 的 stub／ValueError 測試）＋ `tests/test_pipelines/test_evaluation/test_pipeline.py`（結構斷言，**預先授權**的 additive 更新）

- [ ] **Step 1: 寫失敗測試**

在 `test_nodes_spark.py` 找到 `compute_quadrant` 的停用 stub 與 ValueError 測試，鏡射新增：

```python
def test_compute_offset_sweep_disabled_writes_stub():
    params = _params()  # 該檔既有 helper，名稱以檔內實際為準
    params["evaluation"]["diagnosis"]["offset_sweep"] = {"enabled": False}
    assert compute_offset_sweep(None, params) == {"enabled": False}


def test_compute_offset_sweep_requires_eval_predictions_when_enabled():
    params = _params()
    params["evaluation"]["diagnosis"]["offset_sweep"] = {"enabled": True}
    with pytest.raises(ValueError, match="compute_offset_sweep"):
        compute_offset_sweep(None, params)
```

`test_pipeline.py` 結構斷言 additive 更新：default/post_training node 數 **8→9**、compare-source **11→12**、node 名清單在 `compute_quadrant` 後加 `compute_offset_sweep`、outputs 集合加 `evaluation_offset_sweep`、`generate_report` inputs 斷言（若有）加 `evaluation_offset_sweep`。跑一次確認新測試 FAIL、被改的結構測試也 FAIL（還沒接線）。

- [ ] **Step 2: 節點實作（`compute_quadrant` 之後）**

```python
def compute_offset_sweep(
    eval_predictions: Optional[SparkDataFrame],
    parameters: dict,
) -> dict:
    """分流層薄 node（spec §3 Phase 4；框架診斷項目 6）。

    領域邏輯全在 ``diagnosis.metric.offset_sweep``（driver 端 numpy）。
    抽樣與 metric_ci 同一套 ``draw_diagnosis_sample``（同 seed＝同一份
    樣本）。停用時寫 stub。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {})
           .get("offset_sweep", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("offset sweep disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None:
        raise ValueError(
            "compute_offset_sweep: eval_predictions is required when "
            "evaluation.diagnosis.offset_sweep.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.offset_sweep import sweep
    from recsys_tfb.diagnosis.metric.sample import draw_diagnosis_sample

    sample_pdf, sample_meta = draw_diagnosis_sample(
        eval_predictions, parameters
    )
    out = sweep(sample_pdf, parameters)
    out["sample"] = sample_meta
    logger.info(
        "offset sweep computed: %d items, rounds=%d converged=%s, "
        "holdout mAP zero=%s star=%s",
        len(out.get("delta_star", {})), out.get("n_rounds_run"),
        out.get("converged"),
        (out.get("map_holdout") or {}).get("zero"),
        (out.get("map_holdout") or {}).get("star"),
    )
    return out
```

`generate_report`（:361）簽名加尾參 `offset_sweep: Optional[dict] = None,`（`quadrant` 之後），`assemble_report` 呼叫（:429-436）加 `offset_sweep=offset_sweep,`。

- [ ] **Step 3: pipeline 接線＋catalog**

`pipeline.py`：import 區（:24-32）加 `compute_offset_sweep,`（字典序落在 `compute_metrics` 後、`compute_quadrant` 前，照 isort）；nodes list 在 `compute_quadrant` 的 Node（:101-106）之後插入：

```python
        Node(
            compute_offset_sweep,
            inputs=["eval_predictions", "parameters"],
            outputs="evaluation_offset_sweep",
        ),
```

`generate_report` 的 Node inputs（:107-113）在 `"evaluation_quadrant"` 後加 `"evaluation_offset_sweep"`。

`catalog.yaml`（`evaluation_quadrant` 之後）：

```yaml
evaluation_offset_sweep:
  type: JSONDataset
  filepath: data/evaluation/${model_version}/${snap_date}/diagnosis/offset_sweep.json
```

- [ ] **Step 4: 跑測試確認通過＋commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -3 && \
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py \
        src/recsys_tfb/pipelines/evaluation/pipeline.py conf/base/catalog.yaml \
        tests/test_pipelines/test_evaluation/ && \
git commit -m "feat(evaluation): compute_offset_sweep 節點 + catalog + pipeline 接線（8→9 node）"
```

---

### Task 5：報表 section（waterfall）＋ assemble_report 接縫（implementer B，第三段）

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（`build_quadrant_section` :510-554 之後加 `_offset_sweep_waterfall`＋`build_offset_sweep_section`；`assemble_report` :763-788 簽名與 candidates）
- Modify: `conf/base/parameters_evaluation.yaml`（`report.sections` :52-62 加 `offset_sweep: true`，放 `quadrant: true` 之後）
- Test: `tests/test_evaluation/test_report_builder.py`

- [ ] **Step 1: 寫失敗測試**

鏡射該檔 quadrant section 測試的結構新增：

```python
def _sweep_payload():
    return {
        "enabled": True,
        "map_fit": {"zero": 0.50, "star": 0.58},
        "map_holdout": {"zero": 0.51, "star": 0.56},
        "recovered_gap_holdout": 0.05,
        "interaction_residual_holdout": -0.01,
        "delta_star": {"A": -1.0, "B": 0.0},
        "per_item": {
            "A": {"delta_star": -1.0, "loo_contribution_holdout": 0.06},
            "B": {"delta_star": 0.0, "loo_contribution_holdout": None},
        },
        "params": {"shrink_lambda": 0.1, "holdout_fraction": 0.5,
                   "max_rounds": 5,
                   "grid": {"lo": -2.0, "hi": 2.0, "step": 0.05}},
        "notes": [],
    }


def test_offset_sweep_section_off_by_config():
    params = _params()
    params["evaluation"]["report"]["sections"]["offset_sweep"] = False
    assert build_offset_sweep_section(_sweep_payload(), params) is None


def test_offset_sweep_section_none_for_stub_or_missing():
    params = _params()
    assert build_offset_sweep_section(None, params) is None
    assert build_offset_sweep_section({"enabled": False}, params) is None


def test_offset_sweep_section_tables_and_waterfall():
    section = build_offset_sweep_section(_sweep_payload(), _params())
    assert section is not None
    assert len(section.tables) == 2
    assert "delta_star" in section.tables[1].columns
    assert len(section.figures) == 1  # waterfall（有非零 δ*）


def test_offset_sweep_waterfall_skipped_when_all_deltas_zero():
    payload = _sweep_payload()
    payload["delta_star"] = {"A": 0.0, "B": 0.0}
    payload["per_item"] = {
        "A": {"delta_star": 0.0, "loo_contribution_holdout": None},
        "B": {"delta_star": 0.0, "loo_contribution_holdout": None},
    }
    section = build_offset_sweep_section(payload, _params())
    assert section is not None
    assert section.figures == []


def test_assemble_report_includes_offset_sweep_section():
    html = assemble_report(
        _metrics(), _params(), offset_sweep=_sweep_payload()
    )  # _metrics()＝該檔既有 helper，名稱以檔內實際為準
    assert "Offset sweep" in html
```

跑一次確認 FAIL。

- [ ] **Step 2: 實作（`build_quadrant_section` 之後）**

```python
_SWEEP_BLUE = "#1565c0"
_SWEEP_ORANGE = "#e65100"


def _offset_sweep_waterfall(sweep: dict) -> go.Figure | None:
    """分流 waterfall：折外 mAP(0) → 各 item 的 LOO 貢獻 → mAP(δ*)。

    顏色語意沿手冊 fig6-offset-sweep-split：藍＝offset 收復（水準缺口）、
    橘＝負向；mAP(δ*) 與可及上限之間收不回的部分＝條件判別力缺口（上限
    未知，圖上不畫）。原圖為 matplotlib，此處依 spec 修訂以 plotly 重刻。
    """
    mh = sweep.get("map_holdout", {}) or {}
    if mh.get("zero") is None or mh.get("star") is None:
        return None
    per_item = sweep.get("per_item", {}) or {}
    moved = {
        it: v["loo_contribution_holdout"]
        for it, v in per_item.items()
        if v.get("delta_star") and v.get("loo_contribution_holdout") is not None
    }
    if not moved:
        return None
    order = sorted(moved, key=lambda it: -abs(moved[it]))
    x = ["mAP(0) 折外"] + [f"δ*({it})" for it in order]
    y = [mh["zero"]] + [moved[it] for it in order]
    measure = ["absolute"] + ["relative"] * len(order)
    residual = sweep.get("interaction_residual_holdout")
    if residual is not None:
        x.append("交互殘差")
        y.append(residual)
        measure.append("relative")
    x.append("mAP(δ*) 折外")
    y.append(mh["star"])
    measure.append("total")
    fig = go.Figure(go.Waterfall(
        x=x, y=y, measure=measure,
        increasing={"marker": {"color": _SWEEP_BLUE}},
        decreasing={"marker": {"color": _SWEEP_ORANGE}},
        totals={"marker": {"color": "#9e9e9e"}},
    ))
    fig.update_layout(
        title="水準分流：per-item 平移（δ*）可收復的指標缺口（折外）",
        yaxis_title="macro per-item mAP",
        showlegend=False,
    )
    return fig


def build_offset_sweep_section(
    sweep: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "offset_sweep"):
        return None
    if not sweep or not sweep.get("enabled"):
        return None
    mf = sweep.get("map_fit", {}) or {}
    mh = sweep.get("map_holdout", {}) or {}

    def _gap(zero, star):
        return (star - zero) if (zero is not None and star is not None) else None

    summary = pd.DataFrame(
        {
            "mAP(0)": [mf.get("zero"), mh.get("zero")],
            "mAP(δ*)": [mf.get("star"), mh.get("star")],
            "收復量": [_gap(mf.get("zero"), mf.get("star")),
                       _gap(mh.get("zero"), mh.get("star"))],
        },
        index=["折內（fit）", "折外（holdout）"],
    )
    per_item = sweep.get("per_item", {}) or {}
    cols = ["delta_star", "loo_contribution_holdout"]
    tbl = pd.DataFrame(
        {c: [per_item[it].get(c) for it in per_item] for c in cols},
        index=list(per_item),
    )
    fig = _offset_sweep_waterfall(sweep)
    desc = (
        "分流閥：對每個 item 的 logit 分數加常數 δ（不重訓）能收復多少 "
        "macro mAP。判讀順序：(1) 看折外收復量——大＝缺口主要在水準（配置"
        "／再平衡可修）、小＝缺口在條件判別力（必須動訓練）；(2) 看 δ* 大"
        "的 item 是誰，回對帳表查可否由配置解釋；(3) waterfall 看收復量怎"
        "麼分攤到各 item。δ* 單位＝log-odds，與對帳層 offset 同尺度。完整"
        "判讀：docs/pipelines/evaluation-diagnosis.md。"
    )
    notes = sweep.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    return ReportSection(
        title="分流 Offset sweep（水準 vs 條件判別力）",
        description=desc,
        figures=[fig] if fig is not None else [],
        tables=[summary, tbl],
        table_titles=["mAP 收復摘要（折內／折外）",
                      "per-item δ* 與折外 LOO 貢獻"],
    )
```

`assemble_report`（:763）簽名加尾參 `offset_sweep: dict | None = None,`（`quadrant` 之後）；candidates（:774-787）在 `build_quadrant_section(quadrant, parameters),` 之後插入 `build_offset_sweep_section(offset_sweep, parameters),`。`report.sections` config 加 `offset_sweep: true`。

- [ ] **Step 3: 跑測試確認通過＋commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py -q 2>&1 | tail -3 && \
git add src/recsys_tfb/evaluation/report_builder.py \
        conf/base/parameters_evaluation.yaml \
        tests/test_evaluation/test_report_builder.py && \
git commit -m "feat(report): 分流 offset sweep section（plotly waterfall 重刻 fig6）"
```

- [ ] **Step 4: implementer B 收尾——相關全套回歸**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/ tests/test_core/test_consistency.py \
  tests/test_evaluation/test_report_builder.py \
  tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```
Expected: 全綠且數量＝Task 1 baseline ＋ 新增測試數。貼輸出。

---

### Task 6：合併審查（sonnet reviewer，**背景執行**，與 Task 7 並行）

**待審物：** Task 2–5 的 commits（`git log --oneline` 範圍由 controller 填入 prompt）。

**Scope 釘死（提速協議 c）**：prompt 附 controller 的綠燈證據（Task 2 突變檢查回報、Task 5 Step 4 輸出原文），明令：只讀 `git diff <Task1 後 HEAD>..<Task5 後 HEAD>`＋只跑新增/變更的測試檔（`test_offset_sweep.py`、consistency/report/pipeline 測試檔）＋針對性 spot-check；**不重跑全套、不重驗 controller 已附證據的項目**。

**審查要求（照 30-delegation-templates 模板 5）**：逐條檢查——(a) 設計定案的每一條在 code 有對應落地（特別是：平手偏 0 的實作真的按 |g| 升冪＋嚴格改善；懲罰除以 M；注入 scope 只在 offset_sweep；LOO 只算 δ*≠0）；(b) 依賴白名單（offset_sweep.py 不 import plotly/pyspark）；(c) A18 錯誤訊息與實際鍵名逐字對齊；(d) 報表 None-safety（map 缺值、全零 δ*）。列出至少 3 個具體問題（附 檔案:行號 與失敗情境），找不到就逐項列出檢查過程。verdict：PASS / PASS-with-nits / FAIL。

---

### Task 7：真跑閘門三狀態＋效能量測（controller 直跑；Task 6 審查在背景同時進行）

**Files:** 產出 `/tmp/phase4a_state_{A,B,C}/`（各含 offset_sweep.json 拷貝）＋閘門結論。

- [ ] **Step 0: graphify rebuild（code 已改）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 1: 狀態 A——現狀真跑（背景）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/diag-framework && \
export SPARK_CONF_DIR=$PWD/conf/spark-local && \
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb \
  evaluation --env local --model-version 6059dcef 2>&1 | tail -30
```

檢查（每條附證據）：
1. `data/evaluation/6059dcef/20260131/diagnosis/offset_sweep.json` 存在、`enabled: true`、`delta_star` 8 個 item 全部 |δ*| ≤ 0.15（實際值記錄；超出→停下如實回報，不硬過）。
2. `recovered_gap_holdout` ≈ 0（|·| < 0.01 級；記錄實際值）。
3. **負控制**：metric_ci.json／reconciliation.json／quadrant_summary.json 與 `/tmp/phase4a_json_before/` 逐檔 `diff` 零差異。
4. 報表逐字回歸：`difflib` 對 `/tmp/phase4a_report_before.html`——新增 hunks 只能是 offset_sweep section 與 plotly 內嵌；既有 section 文字零變動。
5. **效能**：從 log 取 `compute_offset_sweep` 節點耗時（runner 有 per-node 計時；沒有就以 log 時間戳差估），連同本機抽樣列數記錄，按「設計定案」的公式外推公司規模（22 item×200k query），數字交給 Task 8 素材包。
6. 快照：`mkdir -p /tmp/phase4a_state_A && cp data/evaluation/6059dcef/20260131/diagnosis/offset_sweep.json /tmp/phase4a_state_A/`。

- [ ] **Step 2: 狀態 B——已知答案注入（背景）**

編輯 `conf/base/parameters_evaluation.yaml`：`debug_inject_offsets: {}` → `debug_inject_offsets: {ccard_ins: 1.0}`。重跑同一指令。

檢查：
1. `delta_star["ccard_ins"]` ∈ **[−1.35, −0.6]**；其餘 item |δ*| ≤ 0.15。
2. `map_holdout.star > map_holdout.zero` 且 `recovered_gap_holdout > 0`。
3. `notes` 含注入註記；`injected_offsets` echo `{ccard_ins: 1.0}`。
4. **負控制**：其餘三份 diagnosis JSON 與 `/tmp/phase4a_json_before/` 仍零差異（注入 scope 證明）。
5. 報表 offset_sweep section 帶 ⚠ 注入警示；waterfall 有 ccard_ins 柱。
6. 快照到 `/tmp/phase4a_state_B/`。

- [ ] **Step 3: 狀態 C——還原**

`debug_inject_offsets:` 改回 `{}`，`grep -n "debug_inject_offsets" conf/base/parameters_evaluation.yaml` 確認（不加行首錨定）。重跑，`diff /tmp/phase4a_state_A/offset_sweep.json data/evaluation/6059dcef/20260131/diagnosis/offset_sweep.json` **位元級零差異**（全程確定性：同 seed 抽樣＋同 seed 切折＋確定性座標下降）。`git status --short` 確認 conf 乾淨。

---

### Task 8：判讀手冊擴充（writer＋讀者 agent；素材包由 controller 從 Task 7 備好）

**Files:**
- Modify: `docs/pipelines/evaluation-diagnosis.md`

**結構定案**：新增 **§10 分流層（offset sweep）**，既有 §10 已知限制 **改號為 §11**（已知限制必須維持全檔最後一節）；改號後 `grep -n "§10" docs/pipelines/evaluation-diagnosis.md` 逐處檢查既有內文引用並更新。

**內容契約（每節都要有讀者看得見的數字/表）**：
- 10.1 分流閥在回答什麼：指標缺口＝水準（不必重訓）＋條件判別力（必須動訓練）兩部分；sweep 用「模擬 per-item 平移」量出前者的上限。
- 10.2 δ 的尺度與數感：log-odds、與對帳表 offset 同單位（指向 §2 的尺度節，不重複推導）；錨點——δ*=−1.0 ＝「把該產品的勝算砍到 1/e ≈ 0.37 倍才最優」。
- 10.3 折內／折外：為什麼分兩折——δ 是搜出來的，折內收復量必然偏樂觀；用真跑數字示範折內 ≥ 折外。
- 10.4 收縮與 δ*≈0 的讀法：λ 懲罰讓雜訊級的增益推不動 δ；「δ* 全零」是有意義的結論（水準大致正確），不是沒算出來。
- 10.5 示例表走讀：把真跑（狀態 A）的 mAP 收復摘要與 per-item 表**印進文件**逐欄走讀。
- 10.6 waterfall 讀法：藍柱＝該 item 平移收復的量、交互殘差是什麼、總長＝折外收復量；「圖上沒畫的」＝mAP(δ*) 之上收不回的部分＝判別力缺口。
- §11（原 §10）已知限制追加：座標下降成本公式與大樣本緩解（調粗 grid.step）；LOO 貢獻不保證加總（交互殘差補橋）；分數超出 (0,1) 時 δ 退回原始分數尺度；全部數字是抽樣估計（樣本規模看 sample metadata）。

**寫法鐵則（HANDOFF 協議 6，逐條）**：(a) 禁用開發詞彙（本機/Phase/spec/驗收/真跑/本次/我們的），交付前 grep；(b) 示例產物直接印進文件；(c) 無直覺尺度建數感；(d) 報表 description 已是短判讀順序，手冊不重複它；(e) 交付前派 fresh 讀者 agent，驗證清單含「列出所有指涉你看不到的東西的詞」。

- [ ] Step 1: controller 備素材包（狀態 A 的兩張表 markdown 化＋效能外推數字＋δ* 實際值）
- [ ] Step 2: writer 起草（prompt 內嵌本 task 契約全文＋素材包）
- [ ] Step 3: 讀者 agent 通讀（fresh context，不給作者結論）
- [ ] Step 4: controller 對鐵則 checklist 終審＋grep 禁用詞零命中
- [ ] Step 5: Commit `docs(diagnosis): 判讀手冊 §10 分流層（offset sweep）+ 已知限制改號 §11`

---

### Task 9：opus 總審＋nit 修復（**背景執行**）

**待審物：** Task 2–8 全部 commits（`git diff <Task1 後 HEAD>..HEAD`）＋三狀態閘門證據（controller 附上：狀態 A/B/C 的檢查結果原文、JSON 快照路徑、報表 difflib 結論）。

**審查要求**：閘門證據核驗（數字對得上 JSON 嗎）＋跨檔一致性（config 鍵名／JSON 鍵名／報表欄名／手冊用詞逐字對齊；A18 訊息裡的鍵路徑真實存在）＋手冊是否洩漏開發脈絡。明令不重跑測試與 evaluation（證據已附）；列出至少 3 個具體問題或逐項列出檢查過程；verdict：READY / READY-with-nits / NOT-READY。nits 由 controller 裁決後修復＋commit；行為有變則重跑受影響的驗證（含必要時重做狀態 C 位元比對）。

---

### Task 10：使用者閘門

controller 彙整（全部絕對路徑）：
1. 三狀態閘門結果摘要（δ* 表、折外收復量、負控制、位元還原）。
2. 報表新 section 截圖級描述＋`data/evaluation/6059dcef/20260131/report.html` 路徑。
3. 手冊 §10 新增內容路徑＋讀者 agent 結論。
4. 測試對照（baseline vs 終態數量）。
5. 效能實測與公司規模外推。
6. 「沒做的事」：pair_ledger（Phase 4b）、演算法優化（v1 取簡單正確）、公司環境驗證。

**等使用者確認通過才進 Phase 4b。**

---

## Self-review（writing-plans skill 要求，寫完計畫後 controller 自查）

- Spec 覆蓋：spec §3 Phase 4 的 offset_sweep 相關交付物（sweep 函式、輸出、config、A18、report section、注入鍵、known-answer 自動化測試 :197）逐項有 task 對應；pair_ledger 明確劃出（Phase 4b）。
- 無占位詞：全部 task 附完整程式碼／指令／預期輸出。
- 型別一致：`sweep(sample_pdf, parameters) -> dict`、節點簽名、`build_offset_sweep_section(sweep, parameters)`、JSON 鍵名（delta_star/map_holdout/…）在 Task 2/4/5/8 之間逐字一致。
