# evaluation report.html 三塊移除 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 從 `report.html` 移除對帳 Reconciliation 段落（連帶象限水準軸與 triage 兩個水準型判定）、Score Distribution (Boxplot) 圖、以及兩份報表的所有 NDCG 呈現。

**Architecture:** 五個任務、五個 commit。Task 1→2→3 是一條**必須照序**的拆線鏈：先讓 quadrant 不再吃 reconciliation（T1），再讓 triage 不再吃（T2），最後才刪 reconciliation 本體（T3）——這個順序讓每個 task 結束時測試都是綠的。Task 4（boxplot）與 Task 5（ndcg）與前三者無相依，可任意順序。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2、pandas 1.5.3、plotly、pytest。

**設計依據：** `docs/superpowers/specs/2026-07-17-report-section-removal-design.md`（決策與理由都在裡面，本計畫不複述）。

---

## 每個指令的共用約定（照抄，不要改）

**工作目錄**＝`/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim`（**不是** main）。
每個 Bash 指令以 `cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && ...` 開頭。

跑測試一律用這個形式（裸跑 `pytest` 會抓到 main 的 `src`，靜默測錯 code）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

**Baseline（已建，2026-07-17 @ b0c431e）**：本計畫涉及的 13 個測試檔
`329 passed, 0 failed, 19.21s`。**沒有既知 fail**——所以過程中任何一條紅的都是你造成的，不得歸因於既有問題。

**環境鐵則**：
- Edit/Write 的絕對路徑必含 `.worktrees/report-slim`。改錯邊的徵兆是「改了但輸出完全沒變」。
- 不碰 `src/recsys_tfb/evaluation/metrics_spark.py`（ndcg 計算刻意保留）。
- 不碰 `src/recsys_tfb/core/group_utils.py`、A7 `ranking_objective_conflicts`、`conf/base/parameters_training.yaml`——那是 training 側的 ndcg，與本次無關。`rank_xendcg` 是 objective 名稱含 "ndcg" 的 grep 假陽性。
- 本 session 改完 code 後跑一次 graphify rebuild（見計畫末）。

**遇到下列情況停下回報，不要自行決定**：
- 實際的 RED 失敗訊息與計畫寫的**不同**（代表計畫對機制的描述有錯，繼續做只會寫出守錯失敗模式的測試）。
- 必須改到「不做」清單裡的檔案才做得下去。

---

## Task 1: 象限水準軸退場

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/quadrant.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:379-410`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py:113-118`
- Modify: `src/recsys_tfb/evaluation/report_builder.py:476-556`
- Modify: `src/recsys_tfb/core/consistency.py:91`、`:610-616`
- Modify: `conf/base/parameters_evaluation.yaml:118-127`
- Test: `tests/test_diagnosis/test_metric/test_quadrant.py`
- Test: `tests/test_evaluation/test_report_builder.py:607-680`
- Test: `tests/test_core/test_consistency.py:719-745`
- Test: `tests/test_evaluation/test_parameters_evaluation_yaml.py:64-75`
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py:655-675`
- Test: `tests/test_pipelines/test_evaluation/test_pipeline.py:43-53`

- [ ] **Step 1: 改測試 — `test_quadrant.py` 全檔改寫成單軸**

`_params` 拿掉 `gap_band`、`_recon` helper 整個刪、`test_low_level_side_labels` 整個刪（它只測水準軸）：

```python
"""build_quadrant_summary：條件判別力軸合成＋傷害觀測（best-effort 降級）。"""

import pytest

from recsys_tfb.diagnosis.metric.quadrant import build_quadrant_summary


def _params(auc_threshold=0.6, top_k=1):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
        "evaluation": {
            "diagnosis": {
                "quadrant": {
                    "enabled": True,
                    "auc_threshold": auc_threshold,
                    "top_k_occupancy": top_k,
                },
            },
        },
    }
```

`_eval_df` / `_label_df` / `_ci` 三個 helper **一字不動**（照抄現有的）。三個測試改成：

```python
def test_disc_labels_and_damage_observation(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci=_ci({"A": {"ap": 0.8, "ci_low": 0.6, "ci_high": 0.9,
                             "n_pos": 2}}),
        parameters=_params(),
    )
    a = out["by_item"]["A"]
    # A：AUC=1.0（正例 0.9/0.7 > 負例 0.1）→ 判別力好 → 健康
    assert a["auc"] == pytest.approx(1.0)
    assert a["disc_status"] == "好"
    assert a["quadrant"] == "健康"
    assert a["ap_sampled"] == pytest.approx(0.8)
    assert a["suppression_count"] == 0  # 零壓制補 0
    b = out["by_item"]["B"]
    # B：常數分數 → AUC=0.5（差）→ 冷門受害者
    assert b["auc"] == pytest.approx(0.5)
    assert b["disc_status"] == "差"
    assert b["quadrant"] == "冷門受害者（判別力差）"
    assert b["suppression_count"] == 1
    assert b["ap_sampled"] is None  # metric_ci 沒給 B → None 不炸
    assert out["thresholds"]["auc_threshold"] == pytest.approx(0.6)
    assert out["cross_purchase"]["n_buyers"]["A"] == 2


def test_level_axis_fields_are_gone(spark):
    """水準軸（對帳層產物）已退場——這些鍵不得再出現在輸出裡。"""
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci=None, parameters=_params(),
    )
    a = out["by_item"]["A"]
    for gone in ("gap_vs_global", "level_status", "is_aggressor"):
        assert gone not in a
    assert "gap_band" not in out["thresholds"]
    assert "reconciliation" not in out["sources"]


def test_degrades_when_metric_ci_is_stub(spark):
    out = build_quadrant_summary(
        _eval_df(spark), _label_df(spark),
        metric_ci={"enabled": False},
        parameters=_params(),
    )
    a = out["by_item"]["A"]
    assert a["auc"] == pytest.approx(1.0)  # AUC 軸照算
    assert a["ap_sampled"] is None
    assert out["sources"] == {"metric_ci": False}
    assert len(out["notes"]) == 1
```

- [ ] **Step 2: 跑測試確認 RED**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_quadrant.py -q
```

**預期失敗訊息**：`TypeError: build_quadrant_summary() missing 1 required positional argument: 'reconciliation'`（三個測試都是這條）。

⚠ **實際訊息與此不同 → 停下回報**，不要自行繼續。

- [ ] **Step 3: 改 `quadrant.py`**

module docstring（`:1-8`）換成：

```python
"""象限組裝（框架 Ch2）：條件判別力軸合成＋傷害觀測。

原本的水準軸（``gap_vs_global``，取自對帳層）已隨對帳層一起退場——那組量是
為了回答「絕對水準對不對」，而該問題在純排序（macro per-item mAP）的推導鏈
上不存在。純排序的縱軸替代品（offset sweep 的 δ*_j）尚未定案，故本模組目前
只有條件判別力（within-item AUC）一軸；``quadrant`` 這組識別字沿用，等縱軸
補回來時名實即再度相符。
上游停用（metric_ci stub 或 None）→ 對應欄位 None、notes 註記，不失敗
（best-effort，沿 cases_manifest 慣例）。
"""
```

`_QUADRANT_LABELS`（`:24-31`）塌成單軸、`_level_status`（`:34-41`）整個刪：

```python
_QUADRANT_LABELS = {
    "好": "健康",
    "差": "冷門受害者（判別力差）",
}
```

`build_quadrant_summary`（`:50-140`）：簽章拿掉 `reconciliation`，body 拿掉 `gap_band`／`recon_ok`／`recon_items`／`gvg`／`level`：

```python
def build_quadrant_summary(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    metric_ci: dict | None,
    parameters: dict,
) -> dict:
    """條件判別力軸＋傷害觀測 → per-item 判定（JSON-ready）。"""
    cfg = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("quadrant", {}) or {}
    )
    auc_threshold = float(cfg.get("auc_threshold", 0.6))
    top_k = int(cfg.get("top_k_occupancy", 1))

    auc = within_item_auc(eval_predictions, parameters)
    occupancy = top_slot_share(eval_predictions, parameters, top_k)
    suppression = suppression_counts(eval_predictions, parameters)
    prob, n_buyers = cross_purchase_matrix(label_table, parameters)

    ci_ok = bool(metric_ci and metric_ci.get("enabled"))
    ci_items = (metric_ci.get("per_item", {}) or {}) if ci_ok else {}

    notes: list[str] = []
    if not ci_ok:
        notes.append("metric_ci 停用或缺席——AP±CI 欄從缺。")

    by_item: dict[str, dict] = {}
    for item in sorted(auc):
        a = auc[item]
        disc = _disc_status(a.get("auc"), auc_threshold)
        label = "無法評估" if disc == "無法評估" else _QUADRANT_LABELS[disc]
        ci = ci_items.get(item) or {}
        occ = occupancy["by_item"].get(item) or {}
        by_item[item] = {
            "auc": a.get("auc"),
            "auc_reason": a.get("reason"),
            "n_pos": a["n_pos"],
            "n_neg": a["n_neg"],
            "n_rows": a["n_rows"],
            "disc_status": disc,
            "quadrant": label,
            "ap_sampled": ci.get("ap"),
            "ci_low": ci.get("ci_low"),
            "ci_high": ci.get("ci_high"),
            "top_share": occ.get("top_share"),
            "n_top": occ.get("n_top"),
            "y_rate": occ.get("y_rate"),
            "suppression_count": (
                (suppression["by_item"].get(item) or {})
                .get("suppression_count", 0)
            ),
        }

    return {
        "enabled": True,
        "thresholds": {
            "auc_threshold": auc_threshold,
            "top_k_occupancy": top_k,
        },
        "n_queries": occupancy["n_queries"],
        "n_pos_queries": suppression["n_pos_queries"],
        "by_item": by_item,
        "cross_purchase": {
            "matrix": (
                {j: {k: float(prob.loc[j, k]) for k in prob.columns}
                 for j in prob.index}
                if not prob.empty else {}
            ),
            "n_buyers": (
                {j: int(n_buyers[j]) for j in n_buyers.index}
                if not n_buyers.empty else {}
            ),
        },
        "sources": {"metric_ci": ci_ok},
        "notes": notes,
    }
```

- [ ] **Step 4: 跑測試確認 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_quadrant.py -q
```

預期：`3 passed`。

- [ ] **Step 5: 改 `nodes_spark.py` 的 `compute_quadrant`（`:379-410`）**

簽章拿掉 `reconciliation`、docstring 改字、log 拿掉 `is_aggressor` 計數：

```python
def compute_quadrant(
    eval_predictions: Optional[SparkDataFrame],
    label_table: Optional[SparkDataFrame],
    metric_ci: Optional[dict],
    parameters: dict,
) -> dict:
    """象限層薄 node（框架診斷項目 3/5/10）。

    領域邏輯全在 ``diagnosis.metric.quadrant``。停用時寫 stub；上游診斷
    產物（metric_ci）是停用 stub 時 best-effort 降級不失敗。
    """
    eval_params = parameters.get("evaluation", {}) or {}
    cfg = ((eval_params.get("diagnosis", {}) or {}).get("quadrant", {}) or {})
    if not cfg.get("enabled", True):
        logger.info("quadrant disabled — writing stub")
        return {"enabled": False}
    if eval_predictions is None or label_table is None:
        raise ValueError(
            "compute_quadrant: eval_predictions and label_table are required "
            "when evaluation.diagnosis.quadrant.enabled is true"
        )
    from recsys_tfb.diagnosis.metric.quadrant import build_quadrant_summary
    out = build_quadrant_summary(
        eval_predictions, label_table, metric_ci, parameters
    )
    logger.info("quadrant computed: %d items", len(out["by_item"]))
    return out
```

- [ ] **Step 6: 改 `pipeline.py:113-118` 的 inputs**

```python
        Node(
            compute_quadrant,
            inputs=["eval_predictions", "label_table", "evaluation_metric_ci",
                    "parameters"],
            outputs="evaluation_quadrant",
        ),
```

- [ ] **Step 7: 改 `test_pipeline.py:43-53` 的順序測試**

該測試原本的存在理由是「`evaluation_metric_ci` 與 `evaluation_reconciliation` 都是 dict，交換會 type-check 通過但靜默把水準軸餵錯上游」。少一個 dict 之後那個交換風險消失，但 `evaluation_metric_ci` 與 `parameters` **仍然都是 dict**，交換一樣會靜默餵錯——所以**改寫而非刪除**：

```python
    def test_compute_quadrant_inputs_wired_in_order(self):
        # evaluation_metric_ci and parameters are both dicts, so a swap
        # would type-check but silently feed the CI axis from parameters
        # (or vice versa) — this pins the exact positional wiring so that
        # swap fails loudly.
        pipeline = create_pipeline()
        node = next(n for n in pipeline.nodes if n.name == "compute_quadrant")
        assert node.inputs == [
            "eval_predictions", "label_table", "evaluation_metric_ci",
            "parameters",
        ]
```

（現況在 `tests/test_pipelines/test_evaluation/test_pipeline.py:43-53`，本步驟只改註解與 inputs 清單兩處，`pipeline`／`node` 的取用方式與現況一致。）

- [ ] **Step 8: 改 `consistency.py` 的 A17**

`:91` 的 legend 拿掉 `gap_band` 那句：

```python
* A17 — ``evaluation.diagnosis.quadrant`` parameter domains:
  ``auc_threshold`` ∈ (0.5, 1); ``top_k_occupancy`` integer >= 1.
  Predicate: ``quadrant_param_errors``.
```

`quadrant_param_errors` 裡驗 `gap_band` 的那段（`:610-616`，從 `band = quad.get("gap_band", 0.35)` 到它的 `errors.append(...)` 結束）整段刪。**A17 本身保留**。

- [ ] **Step 9: 改 `conf/base/parameters_evaluation.yaml:118-127`**

```yaml
    # 象限層（A17）：橫軸 within-item AUC（條件判別力，midrank rank-sum）。
    # 原本的縱軸 gap_vs_global（水準，取對帳層產物）已隨對帳層退場；純排序的
    # 替代品（offset sweep 的 δ*_j）未定案，目前只有判別力一軸。
    # auc_threshold 以下＝條件判別力差。top_k_occupancy：top-slot 佔據統計的 k。
    quadrant:
      enabled: true
      auc_threshold: 0.6
      top_k_occupancy: 1
```

- [ ] **Step 10: 改 `report_builder.py` 的象限段落**

`_quadrant_scatter`（`:476-507`）**整個函式刪**（含 docstring）。`build_quadrant_section`（`:510-556`）改成：

```python
def build_quadrant_section(
    quadrant: dict | None, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "quadrant"):
        return None
    if not quadrant or not quadrant.get("enabled"):
        return None
    by_item = quadrant.get("by_item", {}) or {}
    cols = ["quadrant", "auc", "auc_reason", "ap_sampled",
            "ci_low", "ci_high", "top_share", "y_rate", "suppression_count",
            "n_pos", "n_rows"]
    tbl = pd.DataFrame(
        {c: [by_item[it].get(c) for it in by_item] for c in cols},
        index=list(by_item),
    )
    thresholds = quadrant.get("thresholds", {}) or {}
    tables = [tbl]
    table_titles = ["per-item 判別力表"]
    cp = (quadrant.get("cross_purchase", {}) or {}).get("matrix", {}) or {}
    if cp:
        cp_tbl = pd.DataFrame.from_dict(cp, orient="index")
        order = sorted(cp_tbl.index)
        cp_tbl = cp_tbl.reindex(index=order, columns=order)
        tables.append(cp_tbl)
        table_titles.append("交叉購買矩陣 P(買 k｜買 j)（列＝j、欄＝k）")
    desc = (
        "行為層：within-item AUC（條件判別力）＋傷害觀測。判讀順序："
        f"(1) AUC 低於 {thresholds.get('auc_threshold')} 的 item 看 "
        "suppression_count 與 top_share 評估傷害；(2) 交叉購買矩陣看高共購 "
        "item 之間的壓制是否實質。完整判讀："
        "docs/pipelines/evaluation-diagnosis.md。"
    )
    notes = quadrant.get("notes") or []
    if notes:
        desc += "⚠ " + "／".join(notes)
    return ReportSection(
        title="條件判別力 Discrimination（per-item 行為觀測）",
        description=desc,
        tables=tables,
        table_titles=table_titles,
    )
```

⚠ 原本 `return ReportSection(...)` 有 `figures=[fig]` 之類的參數（`:548-556`）——**先讀該段現況**，把 figures 相關參數整個拿掉，其餘照上面。

- [ ] **Step 11: 改剩下的測試**

跑一次找出所有紅的，逐條改：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py \
  tests/test_core/test_consistency.py \
  tests/test_evaluation/test_parameters_evaluation_yaml.py \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py \
  tests/test_pipelines/test_evaluation/test_pipeline.py -q
```

已知要改的（靜態盤點，可能不只這些，以實際紅燈為準）：
- `test_report_builder.py:611`（fixture `thresholds` 拿掉 `gap_band`）、`:617,624`（fixture 拿掉 `gap_vs_global`/`level_status`/`is_aggressor`）、`:632`（`sources` 斷言）、`:667-671`（notes 字串斷言）。
- `test_core/test_consistency.py:728`（fixture 拿掉 `gap_band`）、`:735,740`（A17 錯誤訊息斷言拿掉 `gap_band`）。
- `test_parameters_evaluation_yaml.py:70`（斷言 quadrant 區塊拿掉 `gap_band`）。
- `test_nodes_spark.py:661`（fixture 拿掉 `gap_band`）＋ `compute_quadrant` 呼叫拿掉 `reconciliation` 引數。

- [ ] **Step 12: 跑全部相關測試確認 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_diagnosis/test_metric/ \
  tests/test_pipelines/test_evaluation/ tests/test_core/test_consistency.py -q 2>&1 | tail -5
```

預期：全綠。貼最後 5 行輸出。

- [ ] **Step 13: Mutation check（證明新路徑真的被測到）**

把 `quadrant.py` 的 `_QUADRANT_LABELS` 查表換成固定回傳（`label = "健康"`），跑
`tests/test_diagnosis/test_metric/test_quadrant.py`。

**預期**：`test_disc_labels_and_damage_observation` 轉紅（B 的 `quadrant` 應為
`冷門受害者（判別力差）` 卻得到 `健康`）。確認轉紅後**改回去**。

回報你弄壞了哪一行、以及紅燈訊息原文。若全綠 → 代表測試沒覆蓋到標籤映射，**先補測試再繼續**，不要跳過。

- [ ] **Step 14: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git add -A && git commit -m "refactor(quadrant): 水準軸退場，只留條件判別力一軸

gap_vs_global 唯一來源是即將刪除的對帳層。移除 _level_status／gap_band／
is_aggressor 與象限散布圖（縱軸就是 gap_vs_global，剩一維沒有資訊量）。
A17 predicate 同步拿掉 gap_band 驗證；quadrant 識別字沿用（δ*_j 縱軸未定案）。"
```

---

## Task 2: Triage 水準型判定退場

**Files:**
- Modify: `src/recsys_tfb/diagnosis/metric/triage.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:486-494`
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py:129-134`
- Modify: `src/recsys_tfb/evaluation/report_builder.py:780-800`（`build_triage_section`）
- Modify: `conf/base/parameters_evaluation.yaml:150-153`
- Test: `tests/test_diagnosis/test_metric/test_triage.py`
- Test: `tests/test_evaluation/test_report_builder.py`（triage section 測試）

- [ ] **Step 1: 改測試 — `test_triage.py`**

該檔既有的 helper（已核對過實際名稱）：`_quadrant(by_item, enabled=True)`（`:8`）、
`_base_quadrant_by_item()`（`:12`）、`_recon_entry`（`:27`）、`_base_reconciliation`（`:34`）、
`_sweep_entry(dsc, loo=0.0)`（`:48`）、`_base_offset_sweep()`（`:52`）、`_base_gain_ledger()`（`:65`）。

要做的改動：

1. 檔頭 docstring（`:1-2`）的「合成三層診斷 dict（quadrant/reconciliation/offset_sweep/gain_ledger）」改成「合成診斷 dict（quadrant/offset_sweep/gain_ledger）」。
2. `_recon_entry`（`:27-32`）與 `_base_reconciliation`（`:34-46`）兩個 helper 整個刪。
3. `_base_quadrant_by_item()`（`:12-25`）：**刪掉 `cfg` 與 `reb` 兩個 item**（它們存在的唯一理由就是觸發 `_V_CONFIG` / `_V_REBALANCE`，現已無此判定），其餘三個拿掉 `level_status` / `gap_vs_global` 兩鍵：

```python
def _base_quadrant_by_item():
    return {
        "stv": {"auc": 0.5, "disc_status": "差",
                "auc_reason": None, "y_rate": 0.02},
        "feat": {"auc": 0.5, "disc_status": "差",
                 "auc_reason": None, "y_rate": 0.05},
        "ok": {"auc": 0.8, "disc_status": "好",
               "auc_reason": None, "y_rate": 0.4},
    }
```

⚠ `_base_offset_sweep()`（`:52-63`）與 `_base_gain_ledger()`（`:65-77`）的 fixture 裡若有 `cfg` / `reb` 這兩個 key 的 entry，一併拿掉——先讀該兩個 helper 現況。

4. 所有 `triage(...)` 呼叫拿掉第 2 個引數（`reconciliation`）。
5. 整個刪掉這四個只測水準軸的測試：`test_priority_order_config_over_disc_low`（`:124`）、
   `test_offset_sweep_stub_reb_starter_none`（`:180`）、
   `test_level_unmeasured_not_treated_as_level_off`（`:225`）、
   `test_level_unmeasured_with_config_signal_still_not_config_verdict`（`:238`）。
6. `test_six_verdicts_full_fixture`（`:79`）**改名**成 `test_four_verdicts_full_fixture`，
   拿掉 `cfg` / `reb` 的斷言與 `_V_CONFIG` / `_V_REBALANCE` 相關期望。
7. `test_disc_unmeasured_leaves_note_symmetric_to_level`（`:250`）**改名**成
   `test_disc_unmeasured_leaves_note`——「symmetric_to_level」的對稱對象已不存在。
8. 新增一個守退場的測試：

```python
def test_level_verdicts_and_recon_evidence_fields_are_gone():
    """水準型判定與對帳證據欄已退場——不得再出現在輸出裡。"""
    out = triage(
        _quadrant(_base_quadrant_by_item()),
        _base_offset_sweep(),
        _base_gain_ledger(),
        {},
    )
    assert out["verdicts"], "fixture 應產出至少一個 item 的判定"
    for v in out["verdicts"].values():
        assert v["verdict"] not in ("水準-配置型", "水準-指標再平衡型")
        for gone in ("level_status", "gap_vs_global", "recon_verdict",
                     "theory_min", "theory_max", "residual"):
            assert gone not in v["evidence"]
```

- [ ] **Step 2: 跑測試確認 RED**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_triage.py -q
```

**預期失敗訊息**：`TypeError: triage() missing 1 required positional argument: 'parameters'`（因為少傳一個位置引數，最後一個 `{}` 被吃進 `gain_ledger`）。

⚠ **實際訊息與此不同 → 停下回報**。

- [ ] **Step 3: 改 `triage.py`**

module docstring（`:1-12`）第 3 行的「四個由各自診斷節點產出的 JSON-ready dict（quadrant/reconciliation/offset_sweep/gain_ledger）」改成「三個…（quadrant/offset_sweep/gain_ledger）」。

刪 `_V_CONFIG`（`:22`）、`_V_REBALANCE`（`:23`）兩個常數與 `_LEVERS`（`:30-31`）對應的兩條；刪 `_config_signal`（`:38-44`）、`_config_starter`（`:67-82`）、`_rebalance_starter`（`:85-103`）三個函式。

`triage` 簽章與 body：

```python
def triage(
    quadrant: dict | None,
    offset_sweep: dict | None,
    gain_ledger: dict | None,
    parameters: dict,
) -> dict:
    """跨診斷 dict → per-item 判定表（框架 Ch4 判讀流程）。

    best-effort：quadrant/offset_sweep/gain_ledger 任一缺席、停用或降級都不
    raise，改記 notes 並儘量給出可評估的部分。``parameters`` 保留供未來
    config 覆寫門檻，目前未讀取任何鍵（門檻是起手值，見模組 docstring）。
    """
    top_notes: list[str] = []

    quadrant_ok = bool(quadrant) and quadrant.get("enabled", True) and bool(
        quadrant.get("by_item")
    )
    by_item = quadrant.get("by_item", {}) if quadrant_ok else {}
    if not quadrant_ok:
        top_notes.append(
            "quadrant 缺席、停用或 by_item 為空——無法產生逐 item 判定"
        )

    sweep_by_item = (offset_sweep or {}).get("per_item") or {}
    if not offset_sweep or not offset_sweep.get("per_item"):
        top_notes.append(
            "offset_sweep 缺席或 per_item 為空——健康 item 的 δ* 漂移觀測從缺"
        )

    gl_present = _gain_ledger_usable(gain_ledger)
    gl_per_item = (gain_ledger or {}).get("per_item") or {}
    if not gl_present:
        top_notes.append(
            "gain_ledger 缺席、停用或 fallback——結構層裁決降級為"
            "「無結構層證據」"
        )
    max_share = _max_context_gain_share(gl_per_item) if gl_present else 0.0

    verdicts: dict[str, dict] = {}
    for item in sorted(by_item):
        q = by_item[item] or {}
        sweep_entry = sweep_by_item.get(item)
        gl_entry = gl_per_item.get(item)

        notes: list[str] = []
        auc = q.get("auc")
        auc_reason = q.get("auc_reason")
        disc_status = q.get("disc_status")
        disc_low = disc_status == "差" and auc_reason is None
        # 判別力軸無法評估（AUC 算不出＝quadrant 給「無法評估」，或算得出但
        # 樣本太少帶 auc_reason）時不計入 disc_low＋留 note，免得只看 verdict
        # 的讀者誤以為判別力已查過沒問題（審查修復 2026-07-08）。
        if disc_status == "無法評估" or (disc_status == "差" and auc_reason is not None):
            reason = auc_reason or "樣本不足"
            notes.append(
                f"判別力軸（within-item AUC）無法評估（{reason}）"
                "——判別力側判定略過，未計入 disc_low")

        if disc_low:
            if gl_present:
                share = (gl_entry or {}).get("context_gain_share")
                share = share if share is not None else 0.0
                if share < _STARVE_RATIO * max_share:
                    verdict = _V_STARVED
                else:
                    verdict = _V_FEATURE_MISSING
                    notes.append("特徵缺失型判定待條件化 SHAP 佐證")
            else:
                verdict = _V_NO_STRUCTURAL_EVIDENCE
                notes.append(
                    "gain_ledger 缺席或降級——無法區分餓死型與特徵缺失型"
                )
        else:
            verdict = _V_HEALTHY

        if verdict == _V_HEALTHY and sweep_entry is not None:
            dsc = sweep_entry.get("delta_star_centered")
            loo = sweep_entry.get("loo_contribution_holdout")
            if dsc is not None and loo is not None and abs(dsc) >= 0.3 and loo > 0:
                notes.append(
                    f"健康判定但 δ*_centered={dsc:.2f} 且 holdout LOO 貢獻為正"
                    "——留意早期水準漂移（框架 Ch 4 δ* 觀測）"
                )

        starter = None
        if verdict == _V_STARVED:
            starter = _starved_starter(item, by_item, notes)

        verdicts[item] = {
            "verdict": verdict,
            "lever": _LEVERS[verdict],
            "starter": starter,
            "evidence": {
                "auc": auc,
                "disc_status": disc_status,
                "delta_star_centered": (
                    sweep_entry.get("delta_star_centered") if sweep_entry else None
                ),
                "loo_contribution_holdout": (
                    sweep_entry.get("loo_contribution_holdout")
                    if sweep_entry else None
                ),
                "context_gain_share": (
                    gl_entry.get("context_gain_share") if gl_entry else None
                ),
                "y_rate": q.get("y_rate"),
            },
            "notes": notes,
        }

    summary: dict[str, int] = {}
    for v in verdicts.values():
        summary[v["verdict"]] = summary.get(v["verdict"], 0) + 1

    return {
        "enabled": True,
        "gain_ledger_present": gl_present,
        "thresholds": {"starve_ratio": _STARVE_RATIO, "weight_cap": _WEIGHT_CAP},
        "verdicts": verdicts,
        "summary": summary,
        "notes": top_notes,
    }
```

- [ ] **Step 4: 跑測試確認 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_diagnosis/test_metric/test_triage.py -q
```

- [ ] **Step 5: 改 `nodes_spark.py:486-494`**

```python
def assemble_triage_summary(quadrant: Optional[dict],
                            offset_sweep: Optional[dict],
                            gain_ledger: Optional[dict],
                            parameters: dict) -> dict:
    """Triage 總表 node：純 dict 合成，gain_ledger 缺席 best-effort 降級。"""
    diag = ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
    if not (diag.get("triage", {}) or {}).get("enabled", True):
        return {"enabled": False}
    from recsys_tfb.diagnosis.metric.triage import triage
    return triage(quadrant, offset_sweep, gain_ledger, parameters)
```

- [ ] **Step 6: 改 `pipeline.py:129-134`**

```python
        Node(
            assemble_triage_summary,
            inputs=["evaluation_quadrant", "evaluation_offset_sweep",
                    "gain_ledger", "parameters"],
            outputs="evaluation_triage",
        ),
```

- [ ] **Step 7: 改 `report_builder.py` 的 `build_triage_section`**

`cols` 拿掉 `gap_vs_global`、`rows` 對應的 `ev.get("gap_vs_global")` 那行拿掉：

```python
    cols = ["判定", "建議槓桿", "起手值", "AUC",
            "δ*_centered", "context_gain_share", "備註"]
    rows = {}
    for it in items:
        v = verdicts[it] or {}
        ev = v.get("evidence", {}) or {}
        rows[it] = [
            v.get("verdict"),
            v.get("lever"),
            _fmt_triage_starter(v.get("starter")),
            ev.get("auc"),
            ev.get("delta_star_centered"),
            ev.get("context_gain_share"),
            "；".join(v.get("notes") or []),
        ]
```

`desc` 開頭（`:790`）的「跨三層診斷（象限／對帳／分流，＋結構層 gain_ledger）」改成「跨診斷（判別力／分流，＋結構層 gain_ledger）」。

- [ ] **Step 8: 改 `conf/base/parameters_evaluation.yaml:150-153`**

```yaml
    # 跨診斷合成總表：把 quadrant/offset_sweep/gain_ledger 三個診斷 dict 合成
    # per-item 判定＋建議槓桿＋起手值。gain_ledger 缺席時 best-effort 降級
    # （見 catalog.yaml gain_ledger optional: true）。
    # 判讀見 docs/pipelines/evaluation-diagnosis.md §13。
    triage:
      enabled: true
```

- [ ] **Step 9: 跑相關測試 + 改剩下的紅燈**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_diagnosis/test_metric/ \
  tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```

預期：全綠。貼最後 5 行。

- [ ] **Step 10: Mutation check**

把 `triage.py` 的 `disc_low` 判定反轉（`disc_low = disc_status != "差" and auc_reason is None`），跑 `tests/test_diagnosis/test_metric/test_triage.py`。

**預期**：verdict 斷言轉紅（原本健康的變成餓死型／特徵缺失型，反之亦然）。確認後改回。

回報弄壞哪行與紅燈原文。全綠 → 補測試再繼續。

- [ ] **Step 11: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git add -A && git commit -m "refactor(triage): 水準型判定退場，不再吃 reconciliation

_V_CONFIG／_V_REBALANCE 由 level_off 閘住，而 level_status 只有已退場的水準軸
會產出——兩個判定失去觸發器，連同 _config_signal／_config_starter／
_rebalance_starter 一起移除。已知能力損失：槓桿 1／2 不再有觸發器（offset
sweep 照跑、δ* 照算，只是 triage 不再據此建議槓桿 2）。"
```

---

## Task 3: 刪除 reconciliation 本體

**Files:**
- Delete: `src/recsys_tfb/diagnosis/metric/reconciliation.py`
- Delete: `tests/test_diagnosis/test_metric/test_reconciliation.py`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:351-376`、`generate_report` 簽章與 `assemble_report` 呼叫
- Modify: `src/recsys_tfb/pipelines/evaluation/pipeline.py:32`、`:108-112`、`:135-143`
- Modify: `src/recsys_tfb/evaluation/report_builder.py:415-473`、`:638-639`、`:1010`、`:1037`、`:1051`
- Modify: `src/recsys_tfb/core/consistency.py:87-89`、`:570-596`、`:851`
- Modify: `conf/base/catalog.yaml:246-248`
- Modify: `conf/base/parameters_evaluation.yaml:61`、`:108-116`
- Test: 多檔（見 Step 6）

- [ ] **Step 1: 刪兩個檔**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git rm src/recsys_tfb/diagnosis/metric/reconciliation.py \
       tests/test_diagnosis/test_metric/test_reconciliation.py
```

- [ ] **Step 2: 跑測試確認 RED**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py -q 2>&1 | tail -15
```

**預期失敗訊息**：`ModuleNotFoundError: No module named 'recsys_tfb.diagnosis.metric.reconciliation'`，來自 `compute_reconciliation` 的兩個測試（`test_nodes_spark.py:577-613`）。

⚠ **實際訊息與此不同 → 停下回報**。特別是：若這裡**沒有**任何測試轉紅，代表 `compute_reconciliation` 根本沒被測到，停下回報。

- [ ] **Step 3: 刪 `nodes_spark.py` 的 `compute_reconciliation`（`:351-376`）整個函式**

同時改 `generate_report` 簽章拿掉 `reconciliation: Optional[dict] = None,`（`:503`），與 `assemble_report(...)` 呼叫拿掉 `reconciliation=reconciliation,`（`:573`）。

- [ ] **Step 4: 改 `pipeline.py`**

`:32` 的 import 拿掉 `compute_reconciliation`；`:108-112` 的 Node 整段刪；`:135-143` 的 `generate_report` inputs 拿掉 `"evaluation_reconciliation"`：

```python
        Node(
            generate_report,
            inputs=["eval_predictions", "evaluation_metrics",
                    "parameters", "baseline_metrics", "evaluation_metric_ci",
                    "evaluation_quadrant",
                    "evaluation_offset_sweep", "evaluation_pair_ledger",
                    "evaluation_triage"],
            outputs="evaluation_report",
        ),
```

- [ ] **Step 5: 改 `report_builder.py`**

1. `build_reconciliation_section`（`:415-473`）整個函式刪。
2. `assemble_report` 簽章（`:1037`）拿掉 `reconciliation: dict | None = None,`；candidates（`:1051`）拿掉 `build_reconciliation_section(reconciliation, parameters),`。
3. offset_sweep section 的 desc（`:638-639`）——現況：

   > `"的 item 是誰，回對帳表查可否由配置解釋；(3) waterfall 看收復量怎麼分攤到各 item。δ* 單位＝log-odds，與對帳層 offset 同尺度。完整"`

   改成（拿掉兩處對帳指路，保留 waterfall 與單位說明）：

   > `"的 item 是誰；(3) waterfall 看收復量怎麼分攤到各 item。δ* 單位＝log-odds。完整"`

   ⚠ 先讀 `:630-645` 現況再改，確認上下文銜接通順（前半句 `(2)` 的主詞要還在）。
4. glossary 的 `triage 總表` 條目（`:1009-1011`）：「跨三層診斷（象限／對帳／分流，＋結構層 gain_ledger）合成的 per-item 判定＋建議槓桿＋起手值總表」→「跨診斷（判別力／分流，＋結構層 gain_ledger）合成的 per-item 判定＋建議槓桿＋起手值總表」。

- [ ] **Step 6: 改 `consistency.py`**

1. `:87-89` 的 A16 legend 整條刪（**A17/A18/A19 不重編**，留編號洞——既有文件用編號引用不變量，重編會靜默指錯）。
2. `reconciliation_param_errors`（`:570-596`）整個函式刪。
3. `:851` 的 `errors.extend(reconciliation_param_errors(parameters))` 那行刪。

- [ ] **Step 7: 改 config**

`conf/base/catalog.yaml:246-248` 的 `evaluation_reconciliation` 條目整段刪。

`conf/base/parameters_evaluation.yaml`：`:61` 的 `reconciliation: true`（在 `report.sections` 底下）刪；`:108-116` 的註解區塊＋`reconciliation:` 區塊整段刪。

- [ ] **Step 8: 改測試**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_diagnosis/ \
  tests/test_pipelines/test_evaluation/ tests/test_core/test_consistency.py -q 2>&1 | tail -20
```

已知要刪／改的（以實際紅燈為準）：
- `tests/test_core/test_consistency.py:663-690`（`TestReconciliationParamsA16` 整個 class 刪）、`:703-707`（`test_reconciliation_enabled_string_rejected` 刪）、`:709-717`（`test_bool_values_clean` **改測不刪**——它同時斷言 ci + reconciliation，只拿掉 reconciliation 那半）。
- `tests/test_evaluation/test_report_builder.py:535-563`（`_RECON_FIXTURE` 刪）、`:565-605`（4 個 recon section 測試刪）。
- `tests/test_evaluation/test_parameters_evaluation_yaml.py:52-62`（`test_reconciliation_block` 與 `test_report_sections_include_reconciliation` 刪）。
- `tests/test_pipelines/test_evaluation/test_nodes_spark.py:577-613`（2 個 `compute_reconciliation` 測試刪）。
- `tests/test_pipelines/test_evaluation/test_pipeline.py:12,62`（節點數 12→11）、`:88`（15→14）、`:25,75`（outputs set 拿掉 `evaluation_reconciliation`）、`:37`（node names 拿掉 `compute_reconciliation`）。

⚠ 節點數的數字**以實跑為準**，不要照抄本計畫的 11／14——先跑測試看實際訊息說「expected 12, got N」。

- [ ] **Step 9: 確認 reconciliation 在 src/conf 已零殘留**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
grep -rni "reconcil" src/ conf/ tests/ | grep -v "suggest_categorical_cols"
```

**預期輸出：空**（`scripts/suggest_categorical_cols.py` 的 "reconcile" 是英文動詞誤命中，與本功能無關，故排除）。有殘留 → 逐條處理。

- [ ] **Step 10: 全部相關測試 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_diagnosis/ \
  tests/test_pipelines/test_evaluation/ tests/test_core/test_consistency.py -q 2>&1 | tail -5
```

貼最後 5 行。

- [ ] **Step 11: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git add -A && git commit -m "refactor(eval): 刪除對帳 Reconciliation 整層

模組／節點／catalog／config／A16 predicate／report section 全部移除。
gap／theory_min-max／residual／verdict 這組量是為了回答「絕對水準對不對」，
而該問題在純排序（macro per-item mAP）的推導鏈上不存在。
A16 留編號洞不重編 A17-A19（既有文件以編號引用不變量）。
score_col 這個 config 鍵隨模組消失；欄名 score_uncalibrated 是全域資產，不動。"
```

---

## Task 4: 移除 Score Distribution (Boxplot)

**與 Task 1-3、5 無相依，可任意順序執行。**

**Files:**
- Modify: `src/recsys_tfb/evaluation/distributions.py:63-82`
- Modify: `src/recsys_tfb/evaluation/diagnostics_spark.py:87-97`
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py:540-543` + import
- Test: `tests/test_evaluation/test_distributions.py:50-76`
- Test: `tests/test_evaluation/test_diagnostics_spark.py:56-77`

**保留清單（不要多刪）**：`distributions.py:47-60` `_add_box`、`:85-109`
`plot_score_boxplot_by_label`、`diagnostics_spark.py:21` `_PCTS`、`:62-69`
`_fences`、`:72-84` `_box_stats`、`:100-113` `score_box_stats_by_label`。
`Score Distribution by Label` 仍在用它們。histogram 與三張 rank heatmap 不動。

- [ ] **Step 1: 刪測試**

`tests/test_evaluation/test_distributions.py`：刪 `class TestPlotScoreBoxplot`
（`:50-76`，含它的 `_stats()` fixture）。**保留** `class TestPlotScoreBoxplotByLabel`（`:78-101`）。
`:15-16` 的 import 拿掉 `plot_score_boxplot`（**保留** `plot_score_boxplot_by_label`）。

`tests/test_evaluation/test_diagnostics_spark.py`：刪 `class TestScoreBoxStats`
（`:56-77`）。**保留** `class TestScoreBoxStatsByLabel`（`:79-89`）。
`:13-14` 的 import 拿掉 `score_box_stats`（**保留** `score_box_stats_by_label`）。

- [ ] **Step 2: 跑測試確認仍 GREEN（這步不是 RED）**

刪測試不會讓任何東西轉紅——這是刪除任務，RED 由 Step 4 的殘留檢查代替。

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_distributions.py \
  tests/test_evaluation/test_diagnostics_spark.py -q
```

預期：全綠，且測試數比 baseline 少 4 條（`test_one_box_per_item`、
`test_uses_precomputed_stats_not_raw_points`、`test_quartiles_and_clamped_fences`、
`test_one_row_per_item`）。

- [ ] **Step 3: 刪實作**

`distributions.py:63-82`：`plot_score_boxplot` 整個函式刪。
`diagnostics_spark.py:87-97`：`score_box_stats` 整個函式刪。
`nodes_spark.py:540-543`：

```python
            figs.append(plot_score_boxplot(
                score_box_stats(sdf, item_col, score_col),
                item_col=item_col,
            ))
```
整段刪，並把該檔 import 區的 `plot_score_boxplot` 與 `score_box_stats` 拿掉
（**保留** `plot_score_boxplot_by_label` 與 `score_box_stats_by_label`）。

`distributions.py:1-6` 的 module docstring 提到 "boxplot stats" 仍然成立
（by-label 版還在），**不用改**。

- [ ] **Step 4: 確認零殘留**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
grep -rn "plot_score_boxplot\b\|score_box_stats\b" src/ tests/
```

**預期輸出：空**（`\b` 讓 `plot_score_boxplot_by_label` / `score_box_stats_by_label` 不會命中）。

- [ ] **Step 5: 跑相關測試 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```

貼最後 5 行。

- [ ] **Step 6: 驗證 by-label 圖真的還在（這步是本 task 的核心驗收）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
import inspect
from recsys_tfb.pipelines.evaluation import nodes_spark as ns
src = inspect.getsource(ns.generate_report)
assert 'plot_score_boxplot_by_label' in src, 'by-label 圖被誤刪了'
assert 'plot_score_histogram' in src, 'histogram 被誤刪了'
assert 'plot_score_boxplot(' not in src, 'boxplot 沒刪乾淨'
print('OK: by-label 與 histogram 保留、boxplot 已移除')
"
```

預期輸出：`OK: by-label 與 histogram 保留、boxplot 已移除`。

- [ ] **Step 7: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git add -A && git commit -m "refactor(eval): 移除 Score Distribution (Boxplot) 圖

只刪 plot_score_boxplot 與其 Spark 聚合 score_box_stats。
Score Distribution by Label 與 histogram 保留，故 _add_box／_PCTS／_fences／
_box_stats 這組共用 helper 一併留著。"
```

---

## Task 5: 兩份報表移除 NDCG 呈現

**與 Task 1-4 無相依，可任意順序執行。**

**`src/recsys_tfb/evaluation/metrics_spark.py` 一行都不能改**——ndcg 照算，只是不呈現。若你發現自己想改它，停下回報。

**Files:**
- Modify: `src/recsys_tfb/evaluation/report_builder.py`（多處，見下）
- Modify: `src/recsys_tfb/evaluation/comparison/report.py:104`、`:144`、`:155`、`:192`、`:204`、`:215`
- Test: `tests/test_evaluation/test_report_builder.py`
- Test: `tests/test_evaluation/test_comparison_report.py`

- [ ] **Step 1: 寫失敗測試 — 過濾 helper**

在 `tests/test_evaluation/test_report_builder.py` 新增：

```python
class TestVisibleMetricKeys:
    def test_drops_ndcg_keys(self):
        from recsys_tfb.evaluation.report_builder import _visible_metric_keys
        keys = ["map@3", "ndcg@3", "precision@3", "recall@3", "ndcg@all"]
        assert _visible_metric_keys(keys) == [
            "map@3", "precision@3", "recall@3"
        ]

    def test_preserves_input_order(self):
        from recsys_tfb.evaluation.report_builder import _visible_metric_keys
        assert _visible_metric_keys(["recall@1", "ndcg@1", "map@1"]) == [
            "recall@1", "map@1"
        ]

    def test_does_not_drop_unrelated_keys_that_merely_contain_ndcg(self):
        """只濾「以 prefix 起頭」的 key，不是子字串比對。"""
        from recsys_tfb.evaluation.report_builder import _visible_metric_keys
        assert _visible_metric_keys(["my_ndcg@1"]) == ["my_ndcg@1"]
```

- [ ] **Step 2: 跑測試確認 RED**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py::TestVisibleMetricKeys -q
```

**預期失敗訊息**：`ImportError: cannot import name '_visible_metric_keys' from 'recsys_tfb.evaluation.report_builder'`。

⚠ 實際訊息不同 → 停下回報。

- [ ] **Step 3: 實作 helper**

`report_builder.py` 模組層（放在 `_GLOSSARY` 之前、其他 `_` helper 附近）：

```python
# metrics_spark 仍會算出 ndcg@k / ndcg_attr@k，但兩份報表都刻意不呈現它們。
# 下面幾張表把 metrics dict 的 key 直接攤成欄／列（key-agnostic），所以
# 「不呈現」必須在這裡濾，光是不寫 "ndcg" 字樣擋不住。
_HIDDEN_METRIC_PREFIXES = ("ndcg",)


def _visible_metric_keys(keys) -> list:
    """濾掉刻意不呈現的 metric key，保留原順序。"""
    return [
        k for k in keys
        if not str(k).startswith(_HIDDEN_METRIC_PREFIXES)
    ]
```

- [ ] **Step 4: 跑測試確認 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py::TestVisibleMetricKeys -q
```

預期：`3 passed`。

- [ ] **Step 5: 接上四處 key-agnostic 表格**

**(a) `report_builder.py:865-870`（`build_segment_section`）**——per_segment 的每個
seg 是一個 dict，`ndcg@k` 是它的 key，攤平後變成表格的**欄**：

```python
    rows = (
        {_MACRO_LABEL: macro_seg, **per_segment}
        if macro_seg
        else dict(per_segment)
    )
    rows = {
        seg: {k: (m or {}).get(k)
              for k in _visible_metric_keys(list((m or {}).keys()))}
        for seg, m in rows.items()
    }
    table = pd.DataFrame(rows).T
```

⚠ 這裡**必須經過 `_visible_metric_keys`**，不可以就地寫
`if not k.startswith(_HIDDEN_METRIC_PREFIXES)` 抄捷徑——Step 12 的 mutation
check 是把 helper 換成 identity，抄捷徑的話這處不會轉紅，那個檢查就白做了
（正是它要抓的「helper 寫好了但某處沒接上」）。

**(b) `report_builder.py:940`（`build_baseline_section`）**：

```python
    overall_keys = _visible_metric_keys(
        sorted(set(overall_a) | set(overall_b) | set(overall_delta))
    )
```

**(c) `comparison/report.py:104`（`_build_overall_section`）**：

```python
    keys = _visible_metric_keys(
        sorted(set(overall_a) | set(overall_b) | set(overall_d))
    )
```

**(d) `comparison/report.py:192`（`_build_category_section` 的大類 overall）**：

```python
    keys = _visible_metric_keys(
        sorted(set(overall_a) | set(overall_b) | set(overall_d))
    )
```

`comparison/report.py` 的 import 區（`:16` 附近，已 import `build_glossary_section`）加上 `_visible_metric_keys`。

⚠ **(d) 這處是 controller 自行核對時抓到的，探索 agent 的清單只有 (a)(b)(c)。四處都要接，漏 (d) → 大類 overall 表仍會出現 ndcg。**

- [ ] **Step 6: 移除顯性 ndcg — `report_builder.py`**

1. `:127`：`for fam in ("map", "precision", "ndcg", "recall"):` → `for fam in ("map", "precision", "recall"):`
2. `:149`：`"overall mAP@k 為主軸；precision/ndcg/recall@k 作脈絡。"` → `"overall mAP@k 為主軸；precision/recall@k 作脈絡。"`
3. `:341-343`（`ndcg_tbl_plain`）、`:348-351`（`ndcg_fig`）、`:356-359`（`ndcg_tbl`）三個賦值整段刪。
4. `:384-385`：

```python
    tables = [map_tbl]
    table_titles = ["per-item map_attr@k"]
```

5. `:409`：`figures=[map_fig, ndcg_fig],` → `figures=[map_fig],`
6. `:400`：`"每個產品對主指標 mAP@k / nDCG@k 各貢獻多少。算法：對每筆"` → `"每個產品對主指標 mAP@k 各貢獻多少。算法：對每筆"`
7. `:406-407`：`"即這個產品平均替 AP@k 加了多少分。ndcg_attr@k 同理，把單筆貢獻" "換成 log 折扣的 ndcg_contrib@k。頂列「Macro 平均」為各產品等權平均。"` → `"即這個產品平均替 AP@k 加了多少分。頂列「Macro 平均」為各產品等權平均。"`
8. `:964-965`：baseline 的 `("ndcg_attr", "ndcg_attr@{k}", attr_ks, "per-item ndcg_attr@k (M/B/Δ)"),` 這個 tuple 項刪。
9. `:979`：`"per-item recall/map_attr/ndcg_attr(M/B/Δ)。"` → `"per-item recall/map_attr(M/B/Δ)。"`
10. `:990`：glossary 的 `("ndcg@k", "log 折扣排序品質，正規化 [0,1]"),` 刪。
11. `:995-997`：glossary 的 `("ndcg_attr@k", ...)` 三行整條刪。

- [ ] **Step 7: 移除顯性 ndcg — `comparison/report.py`**

1. `:144`：`("ndcg_attr", "ndcg_attr@{k}", attr_ks, "per-item ndcg_attr@k (M/B/Δ)"),` 刪。
2. `:155`：`description="細產品粒度的 recall / map_attr / ndcg_attr,頂列 Macro 平均。",` → `description="細產品粒度的 recall / map_attr,頂列 Macro 平均。",`
3. `:204`：`("ndcg_attr", "ndcg_attr@{k}", attr_ks, "大類 per-item ndcg_attr@k (M/B/Δ)"),` 刪。
4. `:215`：`description="大類粒度 overall + per-category recall/map_attr/ndcg_attr。只列雙方共通的大類。",` → `description="大類粒度 overall + per-category recall/map_attr。只列雙方共通的大類。",`

- [ ] **Step 8: 改測試**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py \
  tests/test_evaluation/test_comparison_report.py -q 2>&1 | tail -20
```

已知要改的（以實際紅燈為準）：
- `test_report_builder.py:73`：`assert set(s.tables[0].index) == {"map", "precision", "ndcg", "recall"}` → 拿掉 `"ndcg"`。
- `:197-198`：`ndcg_cols` 那兩行刪。
- `:214-219`（`test_glossary_has_attr_entries`）：`assert "ndcg_attr@k" in terms` 那行 **反轉成** `assert "ndcg_attr@k" not in terms`（守退場）。
- `:242-248`（`test_per_item_attr_section_has_macro_rows`）：`map_tbl, ndcg_tbl = s.tables[0], s.tables[1]` → `map_tbl = s.tables[0]`；`:247-248` 的 ndcg 斷言刪。
- `:373-385`（`test_baseline_section_has_three_per_item_compare_tables`）：**改名**成 `test_baseline_section_has_two_per_item_compare_tables`，`:385` 的 `"per-item ndcg_attr@k (M/B/Δ)"` 斷言刪、表格數 3→2。
- `:413-440`（`test_baseline_section_per_item_attr_tables_use_primary_map_k`）：`:440` 同上。
- `test_report_builder.py` 與 `test_comparison_report.py` 的 fixture 裡的 `ndcg@k` / `ndcg_attr@k` 值：**保留不動**——它們正是「metrics 有 ndcg，但報表不呈現」的輸入條件，刪掉就測不到過濾了。

- [ ] **Step 9: 新增守隱性洩漏的測試（四處各一條）**

**⚠ 假綠陷阱（已核對，務必照做）**：`_metrics()`（`test_report_builder.py:17`）
**沒有 `per_segment` 鍵**——segment 測試是各自就地注入的（見 `:254-270`），而且
現有注入的 fixture 長 `{"young": {"map@1": 0.6}}`，**根本沒有 ndcg**。直接拿
`_metrics()` 去測 segment 過濾會假綠（`build_segment_section` 遇到空
`per_segment` 直接回 `None`；就算有，沒 ndcg 也就無從濾起）。所以下面的 segment
測試**必須自己注入含 `ndcg@k` 的 per_segment**。

`_metrics()["overall"]` 與 `_baseline_metrics_full()["overall"]` **都含 `ndcg@1`**
（已核對），baseline 那條可以直接用。該檔以 `from recsys_tfb.evaluation import
report_builder as rb` 匯入，呼叫一律加 `rb.` 前綴。

在 `tests/test_evaluation/test_report_builder.py` 新增：

```python
def test_segment_section_hides_ndcg():
    # per_segment 的每個 seg dict 的 key 會被直接攤成表格的欄——ndcg@k
    # 必須被濾掉。fixture 刻意帶 ndcg@1，否則這條測試會假綠。
    m = _metrics()
    m["per_segment"] = {
        "young": {"map@1": 0.6, "ndcg@1": 0.55, "recall@1": 0.3},
        "old": {"map@1": 0.4, "ndcg@1": 0.35, "recall@1": 0.2},
    }
    s = rb.build_segment_section(m, _params())
    cols = [str(c) for c in s.tables[0].columns]
    assert "map@1" in cols, "非 ndcg 的欄不該被誤濾"
    assert not [c for c in cols if c.startswith("ndcg")]


def test_baseline_overall_table_hides_ndcg():
    # _metrics()["overall"] 與 _baseline_metrics_full()["overall"] 都含
    # ndcg@1；overall 表用 set union 攤成列，ndcg@1 必須被濾掉。
    s = rb.build_baseline_section(
        _metrics(), _baseline_metrics_full(), _params()
    )
    overall = s.tables[s.table_titles.index("overall metrics")]
    idx = [str(i) for i in overall.index]
    assert "map@1" in idx, "非 ndcg 的列不該被誤濾"
    assert not [i for i in idx if i.startswith("ndcg")]
```

在 `tests/test_evaluation/test_comparison_report.py` 新增兩條，分別守
`_build_overall_section`（`comparison/report.py:104`）與 `_build_category_section`
的大類 overall 表（`comparison/report.py:192`）。

⚠ 先讀該檔既有的 fixture 與匯入形式（它的 fixture 在 `:11-21` 附近含 `ndcg@3` /
`ndcg_attr@1` / `ndcg_attr@3`），照它的慣例寫；大類那條需要 fixture 帶
`category`（含 `overall` 有 ndcg）且 `evaluation.product_categories.enabled` 為
true，否則 `_build_category_section` 直接回 `None` → **假綠**。若既有 fixture 湊
不出這個條件，先把條件補上再寫斷言。

- [ ] **Step 10: 跑相關測試 GREEN**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_pipelines/test_evaluation/ -q 2>&1 | tail -5
```

貼最後 5 行。

- [ ] **Step 11: 確認 metrics_spark 沒被動到（紅旗檢查）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git diff --stat HEAD -- src/recsys_tfb/evaluation/metrics_spark.py && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics_spark.py -q 2>&1 | tail -3
```

**預期**：`git diff --stat` 輸出**空**（該檔零改動）；`test_metrics_spark.py` **全綠**（它的 ndcg 斷言仍在，因為計算保留）。**任一條 ndcg 測試轉紅＝你誤刪了計算，停下回報。**

- [ ] **Step 12: Mutation check（本 task 最重要的一步）**

把 `_visible_metric_keys` 改成 identity：

```python
def _visible_metric_keys(keys) -> list:
    return list(keys)
```

跑：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_report_builder.py \
  tests/test_evaluation/test_comparison_report.py -q 2>&1 | tail -8
```

**預期**：Step 9 新增的四條隱性洩漏測試**全部轉紅**（`TestVisibleMetricKeys` 也會紅，那是預期內的）。確認後改回。

⚠ **不要**把 mutation 下在 `_HIDDEN_METRIC_PREFIXES` 的字串上（例如改成 `("zzz",)`）——那只證明常數被讀到，證不了過濾真的**接在四處表格上**。identity mutation 才會暴露「helper 寫好了但某處表格忘了接」。

若四條裡有任何一條**沒轉紅** → 那處表格沒接上 helper，或那條測試的 fixture 根本沒有 ndcg（假綠）。**修好再繼續，不要跳過。**

- [ ] **Step 13: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git add -A && git commit -m "refactor(report): 兩份報表移除 NDCG 呈現（計算保留）

顯性移除主指標表 family、per-item ndcg_attr 表與色階圖、baseline／comparison
的 ndcg_attr 表、glossary 兩條。四處 key-agnostic 表格（per-segment／baseline
overall／comparison overall／comparison 大類 overall）改用 _visible_metric_keys
過濾——那些表把 metrics dict 的 key 直接攤平，程式碼裡沒有 ndcg 字樣。
metrics_spark 零改動：ndcg 照算，只是刻意不呈現。"
```

---

## 收尾

- [ ] **Step 1: 跑一次完整相關測試，與 baseline 對照**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/report-slim/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/ tests/test_diagnosis/ \
  tests/test_pipelines/test_evaluation/ tests/test_core/test_consistency.py -q 2>&1 | tail -5
```

Baseline 是 `329 passed, 0 failed`。本次會少掉被刪的測試（reconciliation 20 條、
boxplot 4 條、水準軸相關數條），加上新增的守退場測試。**failed 必須是 0**。

- [ ] **Step 2: 確認沒動到邊界外的檔案**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
git diff --stat b0c431e..HEAD
```

**檢查**：`metrics_spark.py`、`core/group_utils.py`、`conf/base/parameters_training.yaml`、
`models/lightgbm_adapter.py`、`docs/ranking-diagnosis-framework.md`、
`docs/pipelines/evaluation-diagnosis.md` **都不該出現在清單裡**。出現了 → 回報。

- [ ] **Step 3: graphify rebuild（改過 code 檔後必跑）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/report-slim && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 4: 回報給 controller**

回報格式：
1. 結論先行（≤5 行）。
2. 每個 task 的驗收條件逐條附證據（測試輸出原文、mutation check 的紅燈訊息原文與你弄壞了哪行）。
3. 「沒做到或不確定的事」獨立一段；全部做到就寫「無」。

---

## 不做（範圍外，動到就是錯）

- `src/recsys_tfb/evaluation/metrics_spark.py`——ndcg 計算刻意保留。
- `src/recsys_tfb/core/group_utils.py`、A7 `ranking_objective_conflicts`、
  `conf/base/parameters_training.yaml`、`models/lightgbm_adapter.py`——training 側 ndcg。
- `docs/ranking-diagnosis-framework.md`——使用者正在重新設計的對象，main 上有未 commit 改動。
- `docs/pipelines/evaluation-diagnosis.md` 與其餘文件——文件另開一輪（使用者決策）。
- A17/A18/A19 的不變量代號重編。
- `quadrant` 程式識別字改名。
- `docs/superpowers/plans/` 底下提到 reconciliation 的歷史計畫書。
