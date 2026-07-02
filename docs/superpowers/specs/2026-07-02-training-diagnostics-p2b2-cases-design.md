# Training Diagnostics P2b-2: 象限 cases 案例圖 + manifest + 收 examples

> Spec。承接 P2b-1(#96,象限聚合 profile `per_quadrant.json`)。走 superpowers 全流程
> (brainstorming → 本 spec → writing-plans → subagent-driven-development → real-run 驗證 → finishing)。

## 1. 目標(一句話)

為每個 (item × top@1 象限) 的**全格最高分、最低分案例**各產一張「單列 signed SHAP 貢獻橫條圖」,
配一份完整的 4-象限稽核 `cases_manifest.json`;同時移除 P1 遺留、已無消費者的 `examples` 區塊;
文件(README、training.md)同步補齊象限診斷家族並清掉 examples。

## 2. 背景與動機

- P2b-1 已能回答「每個 (item×象限) **平均**靠哪些特徵」(聚合 signed profile)。P2b-2 補「**單一代表案例**
  靠哪些特徵」——PM/分析者要看具體某位客戶在某象限被排高/排低的驅動特徵。
- top@1 象限定義(沿用 P2b-1):以每個 query group(time × entity)的最高分候選為「決策」。
  `TP`=rank1∧label1、`FP`=rank1∧label0、`FN`=rank≥2∧label1、`TN`=rank≥2∧label0。
- **依目的解耦抽樣**(使用者核心原則):聚合 profile 用 crc32 每格 ~30 列(足夠平均);case 案例只取
  **全格真正極值**(最高、最低分各一列)。兩者是不同用途 → 兩份 frame、兩個 pandas 節點。
- **收 examples**:`compute_shap_diagnostics` 內 `examples = {high, low, per_item_high}`(每筆全特徵 SHAP dict)
  自 P1 移除 waterfall 圖後已無任何程式消費者,卻仍寫入 `shap_diagnostics.json` 並留在文件中。cases/ 正式取代它。

## 3. 設計決策(brainstorming 已定,不重議)

| 決策 | 選定 | 理由 |
|---|---|---|
| 節點結構 | **兩個乾淨節點,依目的解耦** | `select_shap_population` 加第二輸出 `case_rows`;新 pandas 節點 `compute_quadrant_cases`;`compute_quadrant_profiles` 一行不動(P2b-1 產出零回歸風險) |
| SHAP 次數 | cases 自己一次小 SHAP(僅 over 幾十列極值) | 與 P2a/既有多次 pass 一致;成本可忽略;換得節點單一職責 |
| manifest 完整度 | **完整 4-象限稽核表**(記空格、單行格) | 空格本身是 top@1 行為訊號(此 item 從未被排第 1),記錄比靜默消失有用 |
| cust 假名化 | **不 hash**,放原始值 | 公司環境已做資料隔離;省 helper 與 config,PM 可直接對到客戶 |
| 圖型 | **signed 橫條圖**(非 waterfall) | 與 P2b-1 聚合 profile 同一套 signed-SHAP 語彙與單位;純 matplotlib、零 SHAP 繪圖 API 版本脆弱性;避開 `shap.plots.waterfall` 的 Explanation/base_value 耦合(P1 已因此移除 waterfall) |
| 每格取樣數 | 固定 **1 高 + 1 低**(不設 N 旋鈕) | YAGNI;日後要 top-N 再加 |

## 4. 架構與資料流

```
predict → training_eval_predictions ─┐
                                     ├─▶ select_shap_population (Spark, 擴為 2 輸出)
test_model_input ────────────────────┘        ├─▶ shap_population  (profile 抽樣, 不變) ─▶ compute_quadrant_profiles ─▶ per_quadrant.json (P2b-1)
                                               └─▶ case_rows        (全格極值列, 新)     ─▶ compute_quadrant_cases    ─┬▶ cases/<item>/*.png (側效)
                                                                                                                       └▶ cases_manifest.json (新 catalog 輸出)
log_experiment 末加 cases_manifest 輸入(in-DAG 排序 edge,保證上傳 diag dir 前寫好)
```

- rank + 象限標記在 Spark **只算一次**,派生兩份 frame(profile 抽樣 vs 全格極值)。
- `compute_quadrant_profiles`、`compute_shap_diagnostics`(除移除 examples 外)、P2b-1 的 `per_quadrant.json` 產出**不變**。

## 5. 元件規格

### 5.1 `select_shap_population`(擴充,`pipelines/training/diagnostics_spark.py`)

現況:回傳單一 `shap_population`(每格 crc32 抽 ~30 列 profile 樣本)。改為**回傳 `(shap_population, case_rows)` tuple**(pipeline 端對應兩個 outputs)。

`case_rows` 建構(沿用既有已計算的 `labeled`,含 `_rank`/`quadrant`/`_ck`):

- **high** 列:`Window.partitionBy(item_col, "quadrant").orderBy(F.col("score").desc(), F.col("_ck").asc())`,取 `row_number()==1`。
- **low** 列:同 partition,`orderBy(F.col("score").asc(), F.col("_ck").desc())`,取 `row_number()==1`。
- **不對稱 tiebreak**(high 用 `_ck ASC`、low 用 `_ck DESC`)確保:同分格 high/low 抓到**不同**列;唯有真正單行格(該格僅 1 列)才 `high` 與 `low` 落在同一列(供 pandas 端偵測)。
- 兩者各加 `F.lit("high")` / `F.lit("low")` 為 `role` 欄後 `unionByName`。
- select 欄位:`group_cols(time + entity)`、`item_col`、`quadrant`、`role`、`_rank`(as `rank`)、`score`、`label_col`。
- `join` `test_model_input`(`on=group_cols + [item_col], how="inner"`)補特徵欄 → `toPandas()`。
- **best-effort**:整段包在 `try/except` 內;`quadrant_enabled=false` → 回 `(None, None)`;失敗 → log + 回 `(None, None)`。
- 無 UDF(僅 Window / row_number / unionByName / join)。

### 5.2 `compute_quadrant_cases(model, case_rows, preprocessor, parameters) → dict`(`diagnostics/shap_cases.py`,新函式)

與既有 `compute_quadrant_profiles` 同檔(象限診斷家族),但**不同節點、不同一次 SHAP**。

1. `cfg = parameters["diagnostics"]["shap"]`;`quadrant_enabled=false` 或 `case_rows` 為 None/空 → 回 `{}`(空 manifest)。
2. `X = _pdf_to_X(case_rows, preprocessor, parameters)`;**一次** `feature_attributions(model, X, feature_cols)` 得每列 shap。
3. 逐列畫 **signed 橫條圖**(見 5.3),存 `cases/<safe_item>/{quadrant}_{role}.png`;**per-chart try/except 隔離**(單張失敗只 log、不中斷)。
4. 建 manifest(見 §6)並回傳 dict → catalog `cases_manifest`。
5. 全函式包 best-effort try/except:未預期錯誤 → log + 回目前累積的 manifest(或 `{}`),不中斷訓練。

### 5.3 單列 signed 橫條圖

- 取該列 shap 值 `|φ|` 最大的前 `case_top_k`(預設 15)個特徵。
- 水平長條,依 shap 值排序;**正貢獻紅色(推高分)、負貢獻藍色(拉低分)**,長條標數值。
- 標題:`f"{item} · {quadrant} · {role} · score={score:.3f} · rank={rank} · label={label}"`。
- `matplotlib.use("Agg")`;畫完 `plt.close()`(對齊 `compute_shap_diagnostics` 既有繪圖慣例)。

### 5.4 `paths.py` 加 helper

```python
def cases_dir(parameters: dict) -> Path:
    """Resolve（並建立）diagnostics/cases/ —— 每 (item×象限) 極值案例圖 + manifest。"""
    d = diagnostics_dir(parameters) / "cases"
    d.mkdir(parents=True, exist_ok=True)
    return d
```

per-item 子目錄 `cases/<safe_item>/` 在寫圖時以 `safe_name(item)` 建立。

## 6. `cases_manifest.json` 結構(完整 4-象限稽核表)

對 `case_rows` 出現過的每個 `item` × 4 象限 × `{high, low}`:

```json
{
  "ccard": {
    "TP": {
      "high": {"rendered": true, "png": "cases/ccard/TP_high.png",
               "snap_date": "20260101", "cust": "C0001", "rank": 1, "score": 0.912, "label": 1},
      "low":  {"rendered": false, "reason": "single_row_same_as_high"}
    },
    "FP": {
      "high": {"rendered": false, "reason": "empty"},
      "low":  {"rendered": false, "reason": "empty"}
    },
    "FN": { "high": {"rendered": true, "png": "...", ...}, "low": {"rendered": true, "png": "...", ...} },
    "TN": { "high": {"rendered": true, "png": "...", ...}, "low": {"rendered": true, "png": "...", ...} }
  }
}
```

- **item universe** = `case_rows` 中出現過的 item(自足、不需額外輸入;實務上每 item 至少有 TN 會出現 ≈ 涵蓋全部)。
- **空格**(該 item×象限無列):`{"rendered": false, "reason": "empty"}`,不產圖。
- **單行格**(該格僅 1 列,`high` 與 `low` 為同一實體列 —— 以 group-key 相等判定):畫 `high`,`low` 記 `{"rendered": false, "reason": "single_row_same_as_high"}`,不產重複檔。
- `cust` 放原始值(不 hash);多個 entity 欄則以其值組合表示。

## 7. 接線 / config / catalog

- **config**(`conf/base/parameters_training.yaml` 的 `diagnostics.shap`,top-level → 不動 `model_version`):
  - **新增** `case_top_k: 15`。
  - cases 沿用既有 `quadrant_enabled` 開關;固定每格 1 高 + 1 低。
  - **移除** `n_examples`(examples 收掉後成死碼)。
- **catalog**(`conf/base/catalog.yaml`):新增
  `cases_manifest: {type: JSONDataset, filepath: data/models/${model_version}/diagnostics/cases/cases_manifest.json}`。
- **pipeline**(`pipelines/training/pipeline.py`):
  - `select_shap_population` outputs 改 `["shap_population", "case_rows"]`。
  - 新增 `Node(compute_quadrant_cases, inputs=["model", "case_rows", "preprocessor_view", "parameters"], outputs="cases_manifest")`。
  - `log_experiment` inputs 末加 `"cases_manifest"`。
- **nodes.py(唯一一處 nodes.py 改動)**:`log_experiment` 簽名末加 `cases_manifest: dict = None`,
  並記一個 scalar `mlflow.log_metric("n_cases_rendered", <rendered 圖總數>)`(同 P2b-1 加 `quadrant_profiles` /
  `n_quadrant_cells` 手法)。placement 在最末(default 參數不可置於非 default 前)。
- **`diagnostics/__init__.py`**:export `compute_quadrant_cases`。

## 8. 收掉 `examples`

- `diagnostics/shap_per_item.py::compute_shap_diagnostics`:移除 `_example()`、`hi`/`lo`/`per_item_high` 區塊,
  以及回傳 dict 的 `"examples"` key 與不再使用的 `n_examples` 讀取。回傳改為
  `{"global": ..., "per_item": ..., "item_idiosyncrasy": ...}`。
- 既有測試 `tests/test_pipelines/test_training/test_diagnostics.py`(或對應檔)中斷言
  `{"high","low"} <= set(out["examples"])` 的測試 → 改為斷言 `"examples" not in out`(或移除該斷言,改測保留的鍵)。

## 9. 文件(本次異動一併更新)

> 發現:P2b-1 的 `per_quadrant.json` / 象限聚合 profile **從未寫入文件**,且 README/training.md 仍把 `examples` 當現役。
> 本次一併補齊象限診斷家族(profiles + cases)並清掉 examples。

- **`docs/pipelines/training.md`**:
  - 移除 `examples` 描述:L418「高/低/per-item 高分案例…`examples` 區塊」、L234 `n_examples` 列、L208 config 摘要中的「案例數」。
  - **新增象限診斷小節**:說明 top@1 象限定義、`per_quadrant.json`(P2b-1 聚合 profile)、`cases/<item>/{TP,FP,FN,TN}_{high,low}.png` + `cases_manifest.json`(P2b-2),以及 top@1 本質下多數 item 的 TP/FP 為空是正常訊號。
  - config 表(L206–235 區)加 `case_top_k`;產物/輸出表(L411、catalog 輸出)加 cases_manifest 與 cases/ PNG;§節點表(L392 區)加 `select_shap_population` 第二輸出、`compute_quadrant_profiles`、`compute_quadrant_cases`。
- **`README.md`**(L144 診斷句):補上「象限(TP/FP/FN/TN)聚合 profile 與極值案例 SHAP 圖」。
- 若 `docs/design-principles.md` 提及診斷產物清單,順帶對齊(次要,發現才改)。

## 10. 測試策略(TDD)

- **Spark 測 `select_shap_population` 的 `case_rows`**(`test_diagnostics_spark.py` 擴充):
  - 每 (item×象限) 取到全格 max/min score 列;`role` 標記正確。
  - 不對稱 tiebreak 決定性:同分多列時 high/low 落不同列且可重現。
  - 單行格:`high` 與 `low` group-key 相同。
  - join 補齊特徵欄;`quadrant_enabled=false` → `(None, None)`。
  - `shap_population`(第一輸出)行為與 P2b-1 一致(回歸保護)。
- **純 python 測 `compute_quadrant_cases`**(`test_shap_cases.py` 擴充):
  - manifest 完整格(每 item × 4 象限 × 2 role);空格 `reason=empty`;單行格 `reason=single_row_same_as_high`。
  - `case_rows=None`/空 / `quadrant_enabled=false` → `{}`。
  - PNG 檔實際產出(寫 tmp model_version 目錄,斷言檔案存在)。
  - best-effort:單列繪圖 raise 時其餘仍完成、函式不 raise。
- **examples 移除**:更新 `test_diagnostics.py` 相關斷言。
- **config regression**:`parameters_training.yaml` 有 `case_top_k`、無 `n_examples`。
- **pipeline 結構測**(`test_pipeline.py`):node count(cases 節點 +1)、`select_shap_population` 兩輸出、
  `pipeline.outputs` 集合含 `case_rows` + `cases_manifest`、E2E skip/stub 補新節點。`test_resume_contracts.py`
  依 `predict_manifest` edge 不受影響(確認)。

## 11. Real-run 驗證(使用者明確要求,完成定義的一部分)

單元測試綠後,本機 Spark 實跑:
```
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py          # 首次建合成資料
PYTHONPATH=src .venv/bin/python -m recsys_tfb dataset --env local
PYTHONPATH=src .venv/bin/python -m recsys_tfb training --env local
```
檢視 `data/models/<mv>/diagnostics/cases/` 的真實 PNG 與 `cases_manifest.json`,以 SendUserFile 傳給使用者驗;
不合理則回頭修(對齊 P2b-1 real-run 流程)。

## 12. 邊界 / best-effort 總表

| 情境 | 行為 |
|---|---|
| `quadrant_enabled=false` | `select_shap_population`→`(None,None)`;`compute_quadrant_cases`→`{}` |
| Spark 選樣失敗 | `case_rows=None`;下游回 `{}` |
| SHAP / 單張圖失敗 | 只 log,其餘照常;不中斷訓練 |
| 空象限格 | manifest 記 `reason=empty`,不產圖 |
| 單行格 | 畫 high;low 記 `reason=single_row_same_as_high` |

## 13. 非目標(本 PR 不做)

- top-N(N>1)極值案例 / 可調每格取樣數。
- 獨立 `case_charts_enabled` 開關(沿用 `quadrant_enabled`)。
- P3(Optuna 診斷、train/train-dev 學習曲線)。
- 觸碰 #95(predict 分區列舉,parked)。
