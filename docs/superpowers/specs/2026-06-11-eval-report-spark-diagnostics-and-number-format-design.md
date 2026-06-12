# Eval report：診斷區 Spark 聚合 ＋ 表格數字去科學記號

- 日期：2026-06-11
- 分支：`feat/eval-report-spark-diag`
- 狀態：設計已與使用者確認，待寫 implementation plan

## 背景與問題

evaluation pipeline 產出的 `report.html`（以及 `--compare` 的 `report_comparison.html`）體積過大，且表格內充斥科學記號（`1.234e-05`），難以閱讀。

### 根因（已用實證確認）

1. **plotly.js 固定地板 ≈ 3.43 MB**：`evaluation/report.py:39,72` 用 `plotly.offline.get_plotlyjs()` 把整包 plotly.js inline。實測 3,596,753 bytes。此為自包含 HTML 的必要成本，**不在本次處理範圍**（production 無網路，CDN 不可行）。

2. **診斷圖 embed 原始陣列、體積隨資料量線性成長**：`go.Histogram` / `go.Box` 不存「分箱／四分位結果」，而是把**原始輸入陣列**序列化進 HTML，分箱在瀏覽器端做。實測單張圖：1,000 點 = 26 KB；1,000,000 點 = 18.4 MB。`distributions.py` 對 score 畫 histogram＋boxplot＋by-label boxplot，等於把全量 score 塞進 HTML 約三次。預設 `evaluation.report.diagnostics.sample_rows: null`（不抽樣，`conf/base/parameters_evaluation.yaml:69`）→ production ~10M entity × 22 item ≈ 220M 列時為 GB 級，且同一份全量 `toPandas`（`nodes_spark.py:270`）也是 driver OOM 風險。對照：聚合後的 heatmap（22×22）僅 16.6 KB，與列數無關。

3. **pandas `to_html` 整欄科學記號**：`report.py:116` 的 `table.to_html(index=True)` 未給 `float_format`。pandas 對**整個 float 欄位**套統一格式，只要欄內有一個極端值（大 count 或極小 rate）整欄就翻指數。實測 `[1.2e8, 12345.678, 0.5, …]` 連 `0.5` 都被渲染成 `5.000000e-01`。`report_comparison.html` 走同一個 renderer（`comparison/report.py:44` → `generate_html_report`），故一處修正涵蓋兩份報表。

## 目標 / 非目標

**目標**
- 診斷區所有圖在 Spark 端聚合，HTML 體積與 driver 記憶體都與資料列數脫鉤。
- `report.html` 與 `report_comparison.html` 表格不再出現科學記號。
- 不改任何資料產出流程（不碰 `eval_predictions` 表、`compute_all_metrics`、catalog、`metrics.json`、`model_version`）。
- 遵守 production 限制：**No UDFs**、無新套件、無網路。

**非目標**
- 不處理 plotly.js 3.43 MB 地板（無網路環境只能 inline）。
- 不改 metrics 計算或 §0–§8 指標表的內容（只改它們的數字「顯示格式」）。
- calibration 不追求與 sklearn bit-exact，只複刻其 uniform-strategy 的語意。

## 設計

兩條邏輯獨立的工作流，置於同一 spec / 同一分支，可拆成兩個 commit。

### 工作流 B：表格數字去科學記號（小、先做）

單點修改 `report.py:116`：
```python
table.to_html(index=True, float_format=_fmt_no_sci, na_rep="")
```
新增純函式 `_fmt_no_sci(x: float) -> str`（置於 `report.py`）：
- 整數值（`float(x).is_integer()`）→ 千分位無小數：`12,345,678`。
- 非整數 → 固定小數 6 位、去尾零與尾點：`f"{x:,.6f}".rstrip("0").rstrip(".")` → `0.000034`、`0.123457`。
- 極小值退化為 `"0"`，並修掉 `-0`（結果為 `"-0"`/`"-"`/`""` 時回 `"0"`）。
- NaN 由 `na_rep=""` 處理（pandas 不會把 NaN 餵進 `float_format`），不需在函式內處理。

範圍：`float_format` 只作用於 float 欄位；int 欄（counts）本就無指數問題；object 欄（glossary、大類組成、dropped prods 等純字串）無數字、不受影響。heatmap 圖內文字本就是 `%{z:.3f}` / `%{text:.1%}`，無指數問題。

### 工作流 A：診斷區改 Spark 聚合（範圍＝全部）

#### 新增 `src/recsys_tfb/evaluation/diagnostics_spark.py`（純聚合，回傳小 pandas DataFrame，無繪圖、無 UDF）

輸入皆為 `eval_predictions`（已含 identity / `score` / `rank` / `label`，後者 fillna 0，見 `nodes_spark.py:114-133`）。

1. `score_histogram_counts(sdf, item_col, score_col, nbins) -> pd.DataFrame`
   - 一次 agg 取全域 `min(score)=m`、`max(score)=M`；`width=(M-m)/nbins`，若 `width==0`（全同值）退化為單一 bin。
   - bin index：`least(nbins-1, greatest(0, floor((score-m)/width)))`（用 `F.floor`/`F.least`/`F.greatest`，無 UDF）。
   - `groupBy(item, bin).count()`。
   - 回傳欄位：`[item, bin, count]` ＋ 由 `m,width` 在 pandas 端算出 `bin_center = m + (bin+0.5)*width`。小表（item × nbins）。

2. `score_box_stats(sdf, item_col, score_col) -> pd.DataFrame`
   - `groupBy(item).agg(F.percentile_approx(score, [0.0,0.25,0.5,0.75,1.0], accuracy))`。
   - collect 後在 pandas 端拆出 `dmin,q1,median,q3,dmax`，算 Tukey fence：`IQR=q3-q1`、`lowerfence=max(dmin, q1-1.5*IQR)`、`upperfence=min(dmax, q3+1.5*IQR)`。
   - 回傳：`[item, q1, median, q3, lowerfence, upperfence]`。

3. `score_box_stats_by_label(sdf, item_col, score_col, label_col) -> pd.DataFrame`
   - 同上但 `groupBy(item, label)`。回傳：`[item, label, q1, median, q3, lowerfence, upperfence]`。

4. `rank_count_matrix(sdf, item_col, rank_col) -> pd.DataFrame`
   - `groupBy(item, rank).count()` → pandas pivot 成 `item × rank` 矩陣（缺格補 0）。

5. `positive_rank_count_matrix(sdf, item_col, rank_col, label_col) -> pd.DataFrame`
   - `filter(label==1).groupBy(item, rank).count()` → pivot。

6. `positive_rate_matrix(sdf, item_col, rank_col, label_col) -> pd.DataFrame`
   - `groupBy(item, rank).agg(count()=total, sum(label)=pos)` → `rate=pos/total`（total 0 → 0）→ pivot。

7. `calibration_bins(sdf, item_col, score_col, label_col, n_bins) -> pd.DataFrame`
   - 複刻 sklearn `calibration_curve(strategy="uniform")` 語意：在 `[0,1]` 均勻分箱。`bin = least(n_bins-1, greatest(0, floor(clip(score,0,1)*n_bins)))`。
   - `groupBy(item, bin).agg(avg(score)=prob_pred, avg(label)=prob_true, count=n)`，drop 空 bin（自然不存在）。
   - skip 規則複刻原碼：某 item 總樣本 `< n_bins` 或正樣本數為 0 → 不出該 item。
   - 回傳：`[item, bin, prob_pred, prob_true]`（已過濾）。
   - 刻意小幅分歧並記錄：Spark 版對超出 `[0,1]` 的 score 做 clip 後分箱（較 sklearn robust），不追求 bit-exact；display-only 可接受。

#### 改 `distributions.py` / `calibration.py`：繪圖函式改吃聚合小 frame

- 直方圖：`go.Histogram` → 每 item 一條 `go.Bar`（共用 bin，x=`bin_center`，y=`count`，`width` 用 bin 寬），`barmode="overlay"`。
- boxplot：`go.Box(q1=, median=, q3=, lowerfence=, upperfence=)`（預算統計量；**不畫離群點**）。by-label 版兩條 trace（Positive/Negative）依 item 分組。
- 三個 rank heatmap、calibration：改吃對應的聚合 frame，繪圖邏輯（Heatmap / Scatter）不變。
- 數值軸（histogram 的 count 軸、boxplot 的 score 軸）設 `tickformat`（如 `","` 或 `"~r"`）避免 plotly 在大 count 軸標出指數。

#### 改 `nodes_spark.py`（`generate_report` 診斷區，現 263-313 行）

- 拿掉 `sample_rows` 分支與全量 `toPandas`。
- 對 `eval_predictions` 投影到需要欄位後 `.cache()`（多趟聚合掃描），呼叫各 `diagnostics_spark.*` 得小 frame，餵繪圖函式組 `figs`。
- 移除 `conf/base/parameters_evaluation.yaml:69` 的 `sample_rows`（全聚合後無 row-level pandas，dead config）；保留 `include_distributions` / `include_calibration` / `n_calibration_bins` 等 toggle。

### 三個已確認的預設
1. 直方圖改全域共用 bin 邊界（原本每 item 各自 auto-bin；overlay 比較更正確，且分箱須先在 Spark 定邊界）。
2. boxplot 不畫離群點（預算 quartile 的取捨；display-only 可接受）。
3. 移除 `sample_rows` config。

## 資料流：不變 vs 變
- **不變**：`eval_predictions` 表、`compute_all_metrics`、catalog / Hive / parquet、`metrics.json`、`model_version`。純報表層改動，不碰資料產出流程。
- **變**：診斷區由「全量 toPandas → Plotly 端分箱」改為「Spark 端聚合 → 小 frame → 預算好的圖」；表格數字格式由 pandas 預設改為 `_fmt_no_sci`。

## 模組邊界（isolation）
- `diagnostics_spark.py`：只依賴 Spark DataFrame，回傳小 pandas frame；可用小 `spark` fixture 對手算值單測，不需 plotly。
- `distributions.py` / `calibration.py`：只依賴小 pandas frame → `go.Figure`；可用極小 frame 快速單測，不需 Spark。
- `nodes_spark.py`：只負責 cache + 串接，薄。

## 測試策略（TDD）
- **B（格式器）**：對含「大 count ＋ 極小 rate ＋ NaN」的 DataFrame 斷言 `to_html` 輸出**不含** `e+`/`e-`、NaN 渲染為空字串、千分位與去尾零正確；`_fmt_no_sci` 對 `-0`/極小值的退化行為。
- **A 聚合函式**：小 Spark fixture（手構 score/rank/label）→ 斷言 bin 計數、quartile（含 fence）、rate 矩陣、calibration bin 等於手算值；驗證 `width==0`、空 bin、skip 規則等邊界。
- **A 繪圖函式**：極小聚合 frame → 斷言 trace 結構正確，且**每個 trace 的 data 長度有界**（= nbins 或 5 個 quartile 點），不隨列數成長 —— 此為修復核心斷言。
- **回歸**：用中等列數（如 5 萬）Spark DataFrame 跑完整診斷組裝，斷言產出的圖不含「長度隨列數成長」的 trace（體積有界）。

## 風險 / 取捨
- 直方圖共用 bin、boxplot 去離群點：與舊版視覺略有差異，但語意更一致，且為去除 row-scaling 的必要條件。已與使用者確認。
- calibration 與 sklearn 非 bit-exact：display-only，已記錄分歧。
- 多趟 Spark 聚合掃描：以 `.cache()` 緩解；本就優於舊版「全量 collect 到 driver」。
- 測試效能：`tests/test_evaluation` 整包約 ~33 分鐘；本次只跑相關測試檔，必要時 background 執行，不阻塞（依 CLAUDE.md 測試守則）。

## 不在範圍
- plotly.js 3.43 MB 地板。
- metrics / §0–§8 指標數值本身。
- inference 對齊、其他既有 follow-up。
