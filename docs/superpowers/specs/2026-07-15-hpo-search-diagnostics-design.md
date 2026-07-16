# Spec：HPO 搜尋診斷（Optuna hyperparameter search diagnostics）

> 建立 2026-07-15。經 brainstorming 對話定案；使用者四項硬要求見 §1。

## 1. 目的與情境

`tune_hyperparameters`（`src/recsys_tfb/pipelines/training/nodes.py:389`）目前只回傳
`best_params / best_iteration / best_model`，整個 Optuna study 的 trial 歷史用完即丟——**沒有
任何「搜尋過程」的診斷輸出**。本 spec 補上搜尋過程的輔助資訊。

定位：**偏可稽核為主、不是每次都要看**（使用者明示）。要能回答兩個實務問題：

- (Q1) **需不需要再繼續 trial**（搜尋收斂了沒）。
- (Q2) **超參數 search range 要不要調整**（最佳值有沒有貼到 search_space 邊界）。

**使用者的四項硬要求（本 spec 每個決策都對應回這四項）：**

1. 可安全錯誤重來（診斷的 bug 不得逼你重跑 HPO）。
2. 不重跑成本高的 node（`--from-node finalize_model` 跳過 HPO 的行為不得被破壞）。
3. 能回答 Q1、Q2。
4. 診斷本身不耗過多成本。

## 2. 範圍

**In scope**
- 每次 HPO 後寫入 `diag_dir`：稽核基底 `hpo_trials.json` ＋ 摘要 `hpo_summary.json` ＋ 5 張
  HTML 圖（optimization history / param importances / slice / contour / parallel coordinate），一次寫完。
- 新 package `src/recsys_tfb/diagnosis/hpo/`（對齊既有 `src/recsys_tfb/diagnosis/model/`）。

**Out of scope（YAGNI / deferred）**
- 不改 HPO 搜尋演算法、不改 `hpo_resume` 接續機制。
- **不做按需重繪 script**：使用者權衡後決定「避免多一個元件的複雜度」比 artifact 精簡更重要，故所有圖
  每次自動渲染，不另做 `scripts/hpo_diagnostics.py`（決策 D4）。連帶消除「從 JSON 重建 study 畫圖」的保真度風險。
- 不做 intermediate-value / pruning 圖（本專案 objective 無 per-iteration reporting，畫不出來）。
- 不做 edf / timeline 圖（本輪不需要）。
- 不做跨 model_version 的搜尋比較 dashboard（未來可疊多份 `hpo_trials.json`，現在不做）。

## 3. 使用者已確認的決策

| # | 決策 | 對應要求 |
|---|---|---|
| D1 | 單次寫入：稽核 JSON ＋ 摘要 ＋ 全部圖，HPO 尾端一次寫完（**無按需 script**） | 4 |
| D2 | 寫在 `tune_hyperparameters` 尾端，**不新增 Node、不新增 catalog output** | 1, 2 |
| D3 | 每次自動存 7 個檔：`hpo_trials.json`、`hpo_summary.json`、5 張 HTML 圖 | 3 |
| D4 | 5 張圖全部每次自動存（含 contour / parallel coordinate）；**不做按需 script**（避免多一個元件的複雜度，使用者定案） | 3, 4 |
| D5 | 全程 best-effort（失敗只 warning，絕不 raise） | 1 |
| D6 | 開關 `diagnostics.hpo_search.enabled`（預設 `true`），可整包關；放 `diagnostics:` block（**不進 model_version hash**） | 4 |

## 4. 架構與整合點

**程式落點**（新 package，對齊 `diagnosis/model/` 拆法）：

```
src/recsys_tfb/diagnosis/hpo/
  __init__.py     # write_hpo_diagnostics(...) 對外入口
  collect.py      # 從 optuna Study 抽出 trials → hpo_trials.json 的 dict
  summary.py      # 從 trials + search_space 算 convergence / boundary / importances
  render.py       # optuna.visualization.plot_* → fig.write_html 的 5 張圖（各張各自 best-effort）
```

輸出目錄：`diagnostics_dir(parameters) / "hpo"`（`diagnostics_dir` 定義於
`src/recsys_tfb/diagnosis/model/paths.py:7`，僅從 `parameters` 推導）。

**整合點（唯一）**：在 `tune_hyperparameters` 的 `return` 前（`nodes.py:600` 附近）插入一段
best-effort 呼叫：

```python
try:
    from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics
    write_hpo_diagnostics(
        study, search_space, parameters,
        hpo_objective=hpo_objective, seed=seed,
        best_iteration=best_iteration,
    )
except Exception:
    logger.warning("HPO diagnostics failed; training continues", exc_info=True)
```

- **不改** `tune_hyperparameters` 的簽名與 `outputs`（仍是 `[best_params, best_iteration, hpo_best_model]`）。
- 輸出檔是 side-effect 檔（**非 catalog dataset**），與既有「PNG 由 shap 節點寫出」同一種。
- 既有終端 `log_experiment` 的 `mlflow.log_artifacts(str(diag_dir))`（`nodes.py:996` 附近）
  會自動把整個 `diag_dir`（含 `hpo/`）上傳 MLflow，**不必動 MLflow 接線**。

## 5. 資料流與 resume 安全性（對應要求 1、2）

**為什麼一定寫在函式內、而非新節點**：Optuna `study` 是 `tune_hyperparameters` 的函式局部物件；
它的耐久性來自 Optuna 自己的 `JournalStorage`（`data/models/_hpo/<search_id>/study_journal.log`，
見 `hpo_resume.py:33-39`），**不是**框架的 DataCatalog。寫在函式內：

- **零 DAG／catalog／output 變動** → `tests/test_pipelines/test_resume_contracts.py:37-46` 宣告的
  contract（`slice_from("finalize_model").auto_included == {select_features,
  cache_train_model_input, cache_train_dev_model_input, cache_test_model_input}`）**原封不動**
  → `--from-node finalize_model` 仍然跳過昂貴的 `tune_hyperparameters`。**要求 2 達成。**

**要求 1（可安全錯誤重來）的具體保證：**

- **best-effort**：整段 try/except，診斷失敗只 warning、絕不 raise → 不影響 HPO/training 回傳。
- **位置在 `release_spark_session(parameters)` 之後、`return` 之前** → 純 driver-local，無 Spark job。
- **不以 `remaining > 0` 為條件**：target 已達成（跳過 `study.optimize`）也照寫一次。
- **原子覆寫**（temp file + `os.replace`）、idempotent；診斷衍生自 durable study，永遠可重算 →
  中途死掉 study 完好，下次重算。診斷本身不需要自己的 resume 邏輯。
- **邊角（誠實標註）**：`--from-node finalize_model` 這條 resume 路徑，`tune_hyperparameters`
  不執行 → 診斷不重生。但那時診斷早在「HPO 真跑那次」寫好了（跳過 HPO 的前提就是 HPO 不用重來）。
  一般 `python -m recsys_tfb training` 重跑會重進 `tune_hyperparameters`（`remaining=0` 秒收）重寫。

## 6. 稽核基底 `hpo_trials.json`（schema）

自足（不依賴 Optuna journal 還在，因為 `--fresh-hpo` 會清 journal）。

```jsonc
{
  "schema_version": 1,
  "meta": {
    "model_version": "<mv>",
    "search_id": "<search_id>",
    "hpo_objective": "mean_ap",
    "direction": "maximize",
    "sampler": "TPESampler",
    "seed": 42,
    "n_trials_target": 30,      // config 的 n_trials（目標總數）
    "n_completed": 30,
    "search_space": { /* config search_space 原文快照 */ },
    "generated_at": "<ISO8601>"
  },
  "trials": [
    {"number": 0, "value": 0.31, "state": "COMPLETE", "params": {...}, "duration_s": 42.1}
    // ...
  ],
  "best": {"number": 17, "value": 0.34, "params": {...}, "best_iteration": 213}
}
```

`search_space` 快照是**邊界分析可重算的關鍵**——重算 boundary 靠它，不回頭讀 config。

即使沒有按需 script，這份 JSON 仍保留：它是**可程式化查詢/比對的稽核基底**（HTML 圖是給人看的、
不可 diff），也是「可稽核為主」定位的核心產物。

## 7. 摘要 `hpo_summary.json`（對應要求 3——純文字/JSON 就能答問，不必開圖）

```jsonc
{
  "convergence": {                     // 答 Q1：需不需要再繼續 trial
    "best_value": 0.34,
    "best_trial_number": 17,
    "n_completed": 30,
    "last_improvement_trial": 17,
    "trials_since_improvement": 13,
    "plateau": true,                   // trials_since_improvement >= patience
    "note": "近 13 個 trial 未再刷新最佳；已達 plateau 提示閾值，可考慮停止。"
  },
  "boundary": {                        // 答 Q2：search range 要不要調
    "num_leaves": {
      "best_value": 98, "low": 20, "high": 100, "scale": "int",
      "rel_position": 0.975, "at_low": false, "at_high": true,
      "suggestion": "widen_high",
      "note": "最佳值貼近上界（97.5% 位置），建議放寬上界。"
    }
    // 每個「數值型」搜尋參數一筆
  },
  "importances": {"num_leaves": 0.41, "learning_rate": 0.33, ...} // 或 null（見下）
}
```

**convergence（Q1）**：`trials_since_improvement >= patience` → `plateau=true`（建議可停）；否則
提示「可能還有空間」。**啟發式**，附人話 `note`。

**boundary（Q2）**：對每個**數值型**搜尋參數算 `rel_position ∈ [0,1]`（log-scale 參數在 log
空間算）；`rel_position >= hi_thresh`（預設 0.98）→ `at_high` → `suggestion="widen_high"`；
`<= lo_thresh`（預設 0.02）→ `widen_low`；否則 `ok`。**categorical 參數預設只記「categorical，
不做邊界建議」**（無序 categorical 無邊界概念；有序 categorical 的邊界建議列為開放項 §12）。

**importances（順帶「哪些參數重要」）**：`optuna.importance.get_param_importances` 預設
`FanovaImportanceEvaluator`（吃 `scikit-learn==1.5.0`，已 pinned）。**best-effort**：trial/參數
過少或 objective 近常數時 fANOVA 可能丟例外 → `importances=null` ＋ `note`，不硬失敗、不引新套件。

**閾值**（`patience` / `hi_thresh` / `lo_thresh`）是啟發式，寫成可調 config（位置見 §9），附預設值，
文件明講「這是提示、不是保證」。

## 8. 自動圖（對應要求 3、4）

**每次自動存 5 張**（`optuna.visualization.plot_*` → `fig.write_html`，自足 HTML，已驗
`is_available()=True`、plotly 5.17.0 已 pinned）。使用者定案不做按需 script，故全部每次自動渲染：

| 檔 | 函式 | 答哪一問 |
|---|---|---|
| `optimization_history.html` | `plot_optimization_history` | Q1 收斂 |
| `param_importances.html` | `plot_param_importances` | 哪些參數重要 |
| `slice.html` | `plot_slice` | Q2 最佳值落在範圍哪裡 |
| `contour.html` | `plot_contour` | 參數交互 / 最佳區域 |
| `parallel_coordinate.html` | `plot_parallel_coordinate` | 參數交互 / 最佳區域 |

**每張圖各自 best-effort**（`render.py` 內各包 try/except）：任一張失敗只記 warning、跳過該張，不影響
其餘圖與 JSON。實測的真實退化案例是 **`param_importances` 在完成 trial <2 時**（內部 `get_param_importances`
raise `ValueError`）→ 跳過該張、其餘 4 張照畫。（註：只有 1 個搜尋參數時 contour/parallel **不 raise**，
會畫出退化圖，非失敗。）

**HTML 大小（要求 4，重要）**：`fig.write_html` 預設把整份 plotly.js（~3.6MB）inline 進**每個**檔
（實測 5 張＝18MB）。故一律 `write_html(..., include_plotlyjs="directory")`：整個 `hpo/` dir **共用一份**
`plotly.min.js`（~3.6MB），各 HTML 只剩 ~8–10KB，且**離線可看（不觸網）**。每次 HPO 的圖表淨成本
≈ 一份 3.6MB 共用 JS ＋ 5 個 KB 級 HTML。

**成本（要求 4）**：皆 driver-local，無額外 Spark job / 網路 / 新套件。`contour` 成本隨搜尋參數個數約
O(n²) 成長（兩兩配對子圖），典型 4–8 參數仍是秒級小事；本輪不預先加參數數守衛（YAGNI）。

## 9. 開關與 config（對應要求 4）

放 `diagnostics:` block（**不進 model_version hash**，對齊既有 `diagnostics.shap` 等；`training:`
block 會進 hash，放那會導致「開/關診斷」churn model_version，故不放那）：

```yaml
diagnostics:
  hpo_search:
    enabled: true        # false → 整包跳過（連稽核 JSON 都不寫）
    patience: 10         # 連續 N 個完成 trial 未進步 → plateau 提示
    boundary_hi: 0.98    # 相對位置 >= 此 → 貼上界，建議放寬
    boundary_lo: 0.02    # 相對位置 <= 此 → 貼下界，建議放寬
```

預設值為啟發式，文件明講是提示、非保證。`write_hpo_diagnostics` 以 `.get()` 讀取、缺鍵用上述預設。

## 10. 約束（專案不變量）

- **No UDF / no network / CPU-only / no additional packages**：全部 driver-local，用
  pandas + plotly + optuna + scikit-learn（**皆已 pinned**，無新依賴）。
- 文件與 `note` 全繁體中文；`boundary` / summary 的鍵名對齊 config 真實識別字。

## 11. 殘餘風險與已知取捨（誠實標註）

- **fANOVA importance 在退化 study**（參數少 / trial 少 / objective 近常數）可能丟例外或無意義 →
  best-effort 降級為 `null`，不保證每次都有 importances。
- **plateau / boundary 是啟發式提示**，非收斂/範圍的數學保證；文件明講、`note` 用「建議/可考慮」語氣。
- **`--from-node finalize_model` 不重生診斷**（§5 邊角）——可接受。
- **`contour` 的 HTML 隨搜尋參數個數 O(n²) 膨脹**；本輪不加參數數守衛，靠 per-chart best-effort
  兜底（§8）。典型 4–8 參數無虞。
- **每次 HPO 產出一份 ~3.6MB 的共用 `plotly.min.js`**（`include_plotlyjs="directory"`；比 inline
  的 18MB 省 5×、且離線可看）——這是互動 plotly 圖的成本地板，隨 model_version 進 MLflow。
- **`hpo_checkpointing: false`**（純記憶體 study）時，crash 則無 resume 也無診斷——**既有行為，不變**。

## 12. 測試計畫（TDD）

- **unit / collect**：對 fake study 抽 trials → `hpo_trials.json` schema 欄位正確、`best` 對齊。
- **unit / summary**：對已知 trials 算 convergence（plateau 觸發/未觸發兩例）、boundary
  （`widen_high` / `widen_low` / `ok` 各一例，含 log-scale）、importances best-effort 降級（退化 study → `null`）。
- **unit / render**：5 張圖 `write_html(include_plotlyjs="directory")` 產出檔存在且非空、且共用
  一份 `plotly.min.js`；**完成 trial <2 的 study → `param_importances` 跳過並記 warning，其餘 4 張
  仍產出**（per-chart best-effort 的實測退化案例）。
- **integration**：`tune_hyperparameters` 尾端呼叫；**注入 render 例外 → 確認記 warning 且回傳正常**
  （mutation：拿掉 try/except，該測試應轉紅——證明 best-effort 真的覆蓋）。
- **resume 契約**：跑 `tests/test_pipelines/test_resume_contracts.py`，應**原封不動全綠**
  （證明零 DAG 影響，對應要求 2）。

## 13. 待確認 / 開放項

1. **有序 categorical 參數**的邊界建議策略（本 spec 預設不建議；若之後要有序 categorical 也判邊界再加）。

（原開放項「config 鍵位置」已定案：`diagnostics.hpo_search`，見 §9——放不進 hash 的 `diagnostics:` block。）
