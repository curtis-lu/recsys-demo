# Training Diagnostics 強化(Part A)設計

- 日期:2026-06-28
- 分支:`feat/training-diag-part-a`
- 範圍:**training pipeline 的 diagnostic 強化**,分三階段(P1 → P2 → P3)交付。
- 不在本設計:獨立的「錯誤分析互動工具」(原 Part B)——**刻意延後**,見 §3。

---

## 1. 背景與目標

本 repo 是通用的 **learning-to-rank 批次建模框架**(query group = `time × entity`,對候選 `item` 排名;銀行產品推薦為示例)。training pipeline 目前的診斷(`pipelines/training/diagnostics.py`)有三塊:`compute_feature_statistics`、`compute_feature_importance`、`compute_shap_diagnostics`。

使用者回饋的不足:

1. **SHAP 資訊量不足**:per-item 區塊只有 `top_features`(mean|SHAP|,**無方向**),且**不跨 item 比較**。
2. **SHAP 案例檔名看不出選擇邏輯**:`waterfall_high_{rank}.png` 依「全域最高分」挑,既非失敗案例,檔名也看不出「為何是這幾筆、哪個 item」。
3. **缺 Optuna(HPO)診斷**:參數重要度 / 優化歷史 / trials 表全無。
4. **缺學習動態(generalization gap)**:LightGBM 早停的 train/train-dev 損失曲線**從未被記下**(adapter 無 `record_evaluation` callback),最終模型也沒算過 train 側指標。

**主軸**:強化「**模型對個別 item 的預測能力**」的可見度,讓診斷能**指出模型優化方向**;且**模型結構無關**(single shared model 與未來 two-stage 皆適用)。

---

## 2. 主軸:per-item 預測能力的四個面向

| 面向 | 對單一 item 的問題 | 看到問題 → 優化方向 |
|---|---|---|
| A 排得準嗎 | 真實 adopter 有沒有排到前面 | (已在 **evaluation**,不重做) |
| B 靠什麼排 | 靠哪些特徵、往上推或往下壓;跟別人多不同 | 驅動因子偏離 → two-stage / 補特徵 |
| C 錯在哪 | 排錯(TP/FP/FN/TN)的真實案例長怎樣 | 某特徵不當壓低 → 特徵工程 |
| D 能不能信 | 樣本/覆蓋率夠嗎 | 樣本太少 → 結論先存疑 |

本設計強化 **B、C、D**(模型內部,evaluation 拿不到),A 留在 evaluation。

---

## 3. 分工:training vs evaluation vs(延後的)錯誤分析工具

- **evaluation pipeline**:成效會計,只吃 predictions+labels(model-blind)。`--post-training` 會讀 training 產出的 `training_eval_predictions`(`pipelines/evaluation/pipeline.py:70`)。per-item 的 `hit_rate@K / map_attr@K / ndcg_attr@K / mean_pos`、score/rank 分布、calibration 曲線、run-over-run 比較都在這。**本設計不動 evaluation 的職責**。
- **training diagnostic(本設計)**:需要模型內部(SHAP / booster / HPO study)的面向。
- **錯誤分析互動工具(延後,原 Part B)**:獨立於兩條 pipeline、使用者高度介入、用來深掘大量錯誤案例並輔助決策。**本輪不做**;本設計的案例圖是「每次訓練自動產出的少量代表案例」,與該工具互補(廣度 vs 互動深度)。

---

## 4. 架構決定:重構成 `diagnostics/` 子套件(方案 B)

現有 `diagnostics.py`(~223 行)已做 3 件事,本設計再加 5 個關注點。直接擴充會變大雜燴且分階段易衝突。改為子套件,一個關注點一檔:

```
src/recsys_tfb/pipelines/training/diagnostics/
  __init__.py        # re-export 既有公開函式,維持 import 相容(pipeline.py / 既有測試不改)
  paths.py           # diagnostics_dir + 子資料夾 helper
  attribution.py     # feature_attributions 接縫(two-stage 無關的唯一擴充點)
  sampling.py        # 抽樣:P1 沿用 item 分層;P2 擴為 query-level + 象限分層
  feature_stats.py   # 既有 compute_feature_statistics 搬入(行為不變)
  importance.py      # 既有 compute_feature_importance 搬入(行為不變)
  shap_per_item.py   # P1:signed / profile_positive / divergence / beeswarm
  shap_cases.py      # P2:象限 aggregate profile + 案例圖
  hpo.py             # P3:Optuna 診斷
  learning_curve.py  # P3:train/train-dev 曲線 + gap
```

`__init__.py` re-export 後,既有 `from recsys_tfb.pipelines.training import diagnostics as diag; diag.compute_feature_importance(...)` 照常可用 → **零破壞**(以 back-compat 測試守住)。

### 共用原則

- **best-effort**:每個診斷節點防禦化(try/except → log + 落部分產物),單點失敗(某張 plotly、某 item 的 beeswarm)**不中斷訓練**;保留 `strict` 旗標供 CI。對齊既有 `log_experiment` 哲學。
- **不動 `model_version`**:所有新 config 留在 `diagnostics:` top-level(`compute_model_version` 只雜湊 `training:` block)。
- **two-stage 無關**:所有 SHAP 取值經 `attribution.feature_attributions(model, X, names)`,內部今天走 `model.booster`,是日後支援 composite 模型的唯一改點。

---

## 5. P1 — per-item SHAP 資訊量(實作細度)

**動到**:新增 `attribution.py`、`shap_per_item.py`、`paths.py`;搬入 `feature_stats.py`/`importance.py`;改寫主節點(原 `compute_shap_diagnostics`)。

### 5.1 attribution.py

```python
def feature_attributions(model, X, feature_names) -> np.ndarray:
    booster = getattr(model, "booster", None)
    if booster is None:
        raise TypeError(
            f"{type(model).__name__} 無 booster;SHAP 歸因不支援"
            "(請在此 seam 擴充 composite 模型)"
        )
    sv = np.asarray(shap.TreeExplainer(booster).shap_values(X))
    if sv.ndim == 3:        # 部分版本回 [classes, n, feat]
        sv = sv[-1]
    return sv[:, : len(feature_names)]   # 去掉可能的 bias 欄

def attribution_budget_units(model) -> int:   # budget guard 用,不直接摸 booster
    b = getattr(model, "booster", None)
    return b.num_trees() if b is not None else 1
```

### 5.2 per-item 輸出 schema(`shap_diagnostics.json` 擴充)

global 區塊已有 `mean_signed_shap`,不動。per_item 擴充:

```jsonc
per_item["<item>"] = {
  "top_features":          [{"feature","mean_abs_shap","mean_signed_shap"}],  // 全列(加方向)
  "top_features_positive": [...] | null,            // 只 label==1(新)
  "divergence_from_global": 0.0,                     // 0=跟全域一致, 1=完全不同(新)
  "idiosyncratic_features": ["..."],                 // 在本 item top-k 但不在全域 top-k(新)
  "n_sampled","n_positive","score_min","score_max","score_mean","low_coverage",  // 不變
  "positive_low_coverage": false                     // 正樣本 < positive_min_rows(新)
}
item_idiosyncrasy = [ {"item","divergence_from_global","idiosyncratic_features"} ... ]  // 依偏離度排序(新)
```

- **divergence**:top-k Jaccard 距離 `1 − |I∩G| / |I∪G|`;全域基準取 micro(全 pool `mean|SHAP|` 排序)。`divergence_metric` 可切 `spearman`(全向量 rank 相關)。
- **方向**:`mean_signed_shap` 為該特徵在該 item 子集的 signed SHAP 均值(>0 往上推、<0 往下壓)。

### 5.3 圖

- `summary/shap_summary_global.png`(全域 beeswarm)
- `summary/per_item/shap_summary__<item>.png`(每 item beeswarm;beeswarm 才同時看出重要性與方向)
- **移除** `waterfall_high_{rank}.png`。

### 5.4 config 新鍵(`diagnostics.shap`)

```yaml
profile_positive: true
positive_min_rows: 20          # 低於此 → positive_low_coverage
divergence_metric: jaccard_topk   # ∈ {jaccard_topk, spearman}
divergence_top_k: 15
per_item_beeswarm: true
```

### 5.5 測試點(純 pandas/shap,快)

signed 正負號正確、profile_positive 分流 + low_coverage、divergence(相同→0 / 不相交→1)、idiosyncrasy 排序、beeswarm 路徑、attribution(shape + 無 booster raise)、back-compat(`diag.compute_feature_importance` 仍可 import)。更新既有 `test_shap_*`(summary PNG 改路徑)。

### 5.6 階段取捨(誠實註記)

P1 的 `profile_positive` 跑在現有 label-agnostic 抽樣上,稀疏正樣本(~2%)下許多 item 會 `positive_low_coverage=true`。**P2 換象限抽樣後 coverage 才穩**——這是刻意的階段順序。

---

## 6. P2 — 象限案例與 profile(設計細度)

### 6.1 TP/FP/FN/TN 的定義(排序模型)

排序模型不輸出 0/1,需先定「判正」界線。**採 top@K, K=1**(每客戶只有排第 1 的 item 算「判正」),理由:忠於「依名次配資源」、FN 自動接上「名次後悔」、不依賴 calibration、不被 2% 稀疏度打爆(分數門檻法在 2% base rate 下會讓 TP/FP 幾乎全空)。

於每列 (客戶×item):TP=rank1∧adopted、FP=rank1∧¬adopted、FN=adopted∧rank≥2、TN=¬adopted∧rank≥2。`quadrant_top_k_decision` 本設計採 **1**(使用者決定)。此為 rank-based cutoff,與 evaluation `rank@K` 指標**同一類**,但 **K 各自獨立**——案例象限固定 K=1,不需等於 eval 的 `@K` 值。

### 6.2 抽樣(query-level + 象限分層)

- 算象限需 rank-within-(time,entity) → 需完整客戶 group,**抽樣單位由「列」改「客戶」**。
- **規模做法**:新增 Spark 選樣節點 `select_shap_population(training_eval_predictions, parameters)`——用既有預測(已含 score)算 rank/象限,(item×象限)分層抽 `quadrant_sample_per_cell` 列、並每格標記「高分/低分」兩筆 example;輸出小 keyset(snap,cust,item,quadrant,role)。**pandas SHAP 節點只 load 這些 key 的特徵,仍只跑一次 SHAP**(最貴的全表評分已由既有節點完成)。本機/測試走 pandas 直算 rank。

### 6.3 產物

- per-(item×象限) aggregate signed profile(`shap_diagnostics.json` 新增 `per_quadrant` 區塊;每格 top signed features + `low_coverage`)。
- 案例圖:每 item 8 張 `cases/<item>/{TP,FP,FN,TN}_{high,low}.png`(每象限分數最高、最低各一)。
- `cases/cases_manifest.json`:每張的 (hashed cust, rank, score, quadrant, 選擇理由) 與空格子記錄。

### 6.4 config 新鍵

```yaml
quadrant_top_k_decision: 1
quadrant_sample_per_cell: 30
case_examples_per_quadrant: 2   # 高分 + 低分
```

### 6.5 邊界

某象限只 1 筆 → 高低同筆出 1 張;象限空 → 略過並記 manifest(沿用 low_coverage 精神);案例列強制納入 SHAP 抽樣(force-include),不額外多跑 SHAP。

### 6.6 測試點

rank@1 象限(平手/組邊界)、每格高低分選樣 + force-include + 空/單筆、per-quadrant low_coverage、manifest、Spark 選樣節點(小 fixture)。

---

## 7. P3 — 訓練過程診斷(設計細度)

### 7.1 Optuna 診斷(`hpo.py`)

`compute_hpo_diagnostics(parameters)` 節點:以 `search_id` 經 `hpo_resume.open_study` 載回**已持久化的 study**(JournalStorage,`data/models/_hpo/<search_id>/`)→ 產出:

- `optuna.importance.get_param_importances`(JSON)
- plotly HTML(`optimization_history` / `param_importances` / `parallel_coordinate`;因環境無 kaleido,用 `write_html`)
- `trials.csv`(`study.trials_dataframe()`)

降級:無持久化 study(`hpo_checkpointing: false`)或 `n_trials < 2` → **skip + log**;plotly 不可用 → 退 matplotlib backend 或 skip。

### 7.2 學習曲線 + gap(`learning_curve.py` + adapter)

- `LightGBMAdapter.train` 增 `lgb.record_evaluation`;valid_sets 補 train 本身 → 同時得 **train 與 train-dev** 兩條曲線(術語對齊:早停 eval set 是 `train_dev`,專案的 `val` 另指 HPO 評分 split)。
- 輸出 `learning_curve/train_dev_curve.png` + `gap_summary.json`(best_iteration 處 train/train-dev 指標、gap、區間標籤 underfit/overfit/balanced)。
- **未決子決策(P3 開工時拍板)**:曲線怎麼取——
  - **(i)** 用 best_params 做一次「專用診斷 refit」(乾淨、與 `final_model_strategy` 無關,代價 +1 次 fit)。**傾向此案**,config gate。
  - **(ii)** 在 HPO best trial 當下擷取 `evals_result`(不多 fit,但要多接線)。
- 取捨:`final_model_strategy=refit_on_full` 無 holdout → 該策略下只有 train 曲線、無 gap;以 (i) 的專用 refit 可一致取得曲線。

### 7.3 config 新鍵

```yaml
diagnostics:
  hpo:
    enabled: true
    plots: [optimization_history, param_importances, parallel_coordinate]
  learning_curve:
    enabled: true        # 用 best_params 記 train & train-dev 曲線
```

### 7.4 測試點

tiny study fixture(reload → importances/history/trials;無 study 走 skip)、learning_curve(小 train → evals_result 抓到 → gap/區間)。

---

## 8. 輸出資料夾佈局

```
data/models/<model_version>/diagnostics/
  summary/   shap_summary_global.png   per_item/shap_summary__<item>.png      # P1
  cases/     <item>/{TP,FP,FN,TN}_{high,low}.png   cases_manifest.json        # P2
  hpo/       optimization_history.html  param_importances.html  trials.csv    # P3
  learning_curve/  train_dev_curve.png  gap_summary.json                      # P3
  shap_diagnostics.json   feature_statistics.json   feature_importance.json
```

`log_experiment` 既有行為(上傳整個 `diagnostics_dir`)不變。

---

## 9. config 變更彙整(皆 `diagnostics:` top-level,不動 model_version)

| 階段 | 區塊 | 新鍵 |
|---|---|---|
| P1 | `shap` | `profile_positive`, `positive_min_rows`, `divergence_metric`, `divergence_top_k`, `per_item_beeswarm` |
| P2 | `shap` | `quadrant_top_k_decision`, `quadrant_sample_per_cell`, `case_examples_per_quadrant` |
| P3 | `hpo` / `learning_curve` | `hpo.enabled`, `hpo.plots`, `learning_curve.enabled` |

---

## 10. 測試策略(總則)

- 純計算函式(profiles / divergence / quadrant / gap)優先單元測、快;SHAP/optuna 重路徑用極小 fixture。
- 診斷節點防禦化:失敗 log 並落部分產物,不中斷訓練(`strict` 旗標給 CI)。
- 每階段獨立可出貨、可單獨測、皆不 bump model_version。
- 既有 `tests/test_pipelines/test_training/test_diagnostics.py` 沿用並擴充;back-compat 以 `__init__` re-export 守住。

---

## 11. 文件更新(納入各階段 DoD)

| 階段 | 文件 | 調整 |
|---|---|---|
| P1 | `docs/pipelines/training.md` | per-item signed/方向、divergence+idiosyncrasy、beeswarm、輸出資料夾佈局、新 config 鍵 |
| P1 | `docs/design-principles.md` | 若有診斷原則陳述,補 per-item 預測能力 / two-stage 無關接縫定位 |
| P1 | `README.md` | 維持抽象;至多一句點到「per-item SHAP」,不堆細節 |
| P2 | `docs/pipelines/training.md` | top@1 象限定義、query-level 象限抽樣、per-(item×象限) profile、案例圖+manifest;說明 case 的 top@1 與 eval `rank@K` 同類、K 各自獨立 |
| P2 | `docs/pipelines/evaluation.md` | 一句交叉引用:training 案例 top@1 與 eval `rank@K` 同類(K 各自獨立)、各自角色(eval 算成效、training 看歸因) |
| P3 | `docs/pipelines/training.md` | Optuna 診斷、train/train-dev 曲線 + gap/區間;`refit_on_full` 無 holdout 取捨 |
| P3 | `docs/operations/hpo-resume.md` | 交叉引用:診斷 reload study 的依賴(checkpointing on);plotly→HTML / 無 kaleido / 生產需有 plotly 的 caveat |

原則:文件跟功能同階段落地;README 不為實作細節變厚,細節落 `docs/pipelines/training.md`。

---

## 12. 範圍外 / 延後

- **獨立錯誤分析互動工具**(原 Part B):延後。
- **two-stage composite adapter**:不在本輪(尚未進 main);`attribution.py` 已留唯一擴充點。
- **shaprx 整合**:延後(C 面向日後可 delegate)。
- **特徵漂移 PSI/KS**:另案。

---

## 13. 階段化交付與驗收

- **P1**:per-item signed/positive/divergence/idiosyncrasy + beeswarm + attribution 接縫 + 子套件重構 + P1 文件。獨立 PR。
- **P2**:象限抽樣 + per-quadrant profile + 案例圖 + manifest + P2 文件。獨立 PR。
- **P3**:Optuna 診斷 + 學習曲線/gap(先拍板 7.2 的 (i)/(ii))+ P3 文件。獨立 PR。

每階段:TDD、只跑相關測試檔、不 bump model_version、文件同階段落地;改 code 後重建 graphify graph。
