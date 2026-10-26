"""Every JSON result the evaluation pipeline lands and the blocks it holds
(ADR-0031 principle 2, supplement 2), keyed by result in :data:`RESULTS`.

Declaring a block writes nothing: each node builds its result itself, and
``tests/test_pipelines/test_evaluation/test_result_blocks.py`` keeps the two
equal.
"""

from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARNS_KEY,
    DISPLAY_ALL_K,
    DISPLAY_COMPARE_K,
    N_WITH_POSITIVE_KEY,
    POPULARITY_WINDOW_KEY,
    POPULATION_KEY,
    WIDEST_GROUP_KEY,
)
from recsys_tfb.pipelines.evaluation.steps.compare_sources import COMPARE_SOURCE_KEY
from recsys_tfb.pipelines.evaluation.steps.result_blocks import (
    Block,
    Produces,
    by_result,
)

_FINGERPRINT_BLOCK = Block(
    "config_fingerprint",
    "寫出這份結果時用的設定，連同它讀的上游結果的指紋。",
    "整次執行一份")
_COMPARE_SOURCE_BLOCK = Block(
    COMPARE_SOURCE_KEY,
    "這份結果是對哪個比較來源算的：該來源中決定 B 側內容的設定。",
    "整次執行一份")
_DISTRIBUTIONS_WHEN = "evaluation.report.diagnostics.include_distributions 為真（預設）"
_CATEGORY_WHEN = "有 item 大類對照時（手寫 mapping 或 column 模式）"


_PRODUCES_SEGMENT_COLUMNS = Produces("evaluation_segment_columns", (
    Block("joined", "這次實際接進預測列的分群欄，依 evaluation.segment_columns 的順序。",
          "整次執行一份"),
    Block("sources", "每個接進來的分群欄取自哪一張表。", "每個分群欄一格"),
    Block("missing", "母體表沒有、因此略過的分群欄，對應到缺這欄的表。",
          "每個缺的分群欄一格"),
    _FINGERPRINT_BLOCK,
))

_PRODUCES_ITEM_CATEGORIES = Produces("evaluation_item_categories", (
    Block("column", "item 大類取自的欄名；沒設（用手寫 mapping）時為 None。",
          "整次執行一份"),
    Block("mapping", "item 到大類的對照；沒用 column 模式時為空。", "每個 item 一格"),
    Block("known_items",
          "被評估模型的 preprocessor 所列的 item 清單，手寫大類 mapping 要拿它核對（右側 item 須在清單內）；沒有該檔時為 None。",
          "整次執行一份",
          when="item 清單從資料數出來時（item_list_counted_from_data）"),
    _FINGERPRINT_BLOCK,
))

#: The facts every row-facts result holds, whatever scored the rows.
_ROW_FACT_BLOCKS = (
    Block("dataset_overview",
          "資料集總覽：列數、實體數、query 數等總計，以及依月份、item（有分群欄時再依分群）的拆分。",
          "整體一格，加每月、每個 item（、每個分群值）各一格"),
    Block("n_queries", "query group 的總數，不論有無正例。", "整體一格"),
    Block("n_excluded_queries", "沒有任何正例的 query group 數。", "整體一格"),
    Block(ALL_POSITIVE_KEY, "所有列都是正例的 query group 數（只算有正例的那些）。",
          "整體一格"),
    Block(WIDEST_GROUP_KEY, "最大的 query group 所含的列數；frame 是事件列時，\"all\" 解析成它。",
          "整體一格"),
    Block(N_WITH_POSITIVE_KEY, "至少有一個正例的 query group 數。", "整體一格"),
    Block(ALL_POSITIVE_SHARE_KEY, "全正例 query group 占有正例 query group 的比例；沒有正例組時為 None。",
          "整體一格"),
    Block(ALL_POSITIVE_WARNS_KEY, "全正例比例是否高到要警告（只在沒開 drop_all_positive_groups 時可能為真）。",
          "整體一格"),
    Block("category", "item 合併成大類之後的同一組列的事實（鍵同上，不再含 category）。",
          "整體一格，形狀同上層", when=_CATEGORY_WHEN),
)

_PRODUCES_ROW_FACTS = Produces("evaluation_row_facts", (
    *_ROW_FACT_BLOCKS,
    Block("segments", "prepare_eval_data 接進來的分群欄、各自的來源表、缺欄的表（evaluation_segment_columns 的複本）。",
          "整次執行一份"),
    _FINGERPRINT_BLOCK,
))

#: The bundle ``compute_all_metrics`` returns, shared by the model's metrics,
#: the baseline's and both sides of a comparison. The metric cells inside
#: ``overall``, ``per_item`` ... are checked against the metric catalog
#: (tests/test_evaluation/test_metric_catalog.py).
_METRIC_BLOCKS = (
    Block("overall", "整體的指標格子，名稱與定義見指標目錄（metric_catalog）。", "整體一格"),
    Block("per_segment", "依分群值切開的指標格子；沒有分群欄時為空 dict。", "每個分群值一格"),
    Block("per_item", "每個 item 的指標格子（含正例數 n_pos）。", "每個 item 一格"),
    Block("per_item_segment", "每個分群值 × 每個 item 的指標格子；沒有分群欄時為空 dict。",
          "每個分群值 × 每個 item 一格"),
    Block("macro_avg",
          "對等權單位平均的指標：by_item 對 item（依 min_positives 篩）；有分群時另有 "
          "by_segment（對分群值，不經 min_positives）與 by_item_segment（對每個 item × "
          "分群格，依 min_positives 篩）；兩者都不含未配對的分群。",
          "by_item 一格；有分群時另加 by_segment、by_item_segment 各一格"),
    Block("observation_items", "正例數低於 min_positives、不進 macro 的 item。", "每個 item 一格"),
    Block(POPULATION_KEY,
          "這份指標是在哪些 query group 上算的：\"all\" 解析成的 K、進 macro 的 item、有／沒有正例而被計分的 query 數、分群欄。",
          "整次執行一份"),
    Block("per_item_shared_numbers", "item 層指標共用的底層數字（各 K 的命中數等），其他格子由它組出來。",
          "每個 item 一格"),
    Block("per_item_segment_shared_numbers", "分群 × item 層指標共用的底層數字；沒有分群欄時為空。",
          "每個分群值 × 每個 item 一格"),
    Block("n_tied_rows", "分數相同、名次由 tie-break 決定的列數。", "整體一格",
          when="schema 宣告了 occasion 或 event 角色"),
    Block("tied_row_share", "分數相同的列占全部列的比例。", "整體一格",
          when="schema 宣告了 occasion 或 event 角色"),
    Block("category", "item 合併成大類之後的指標包（形狀同上層，不再含 category）。",
          "整體一格，形狀同上層", when=_CATEGORY_WHEN),
)

_PRODUCES_EVALUATION_METRICS = Produces("evaluation_metrics", (
    *_METRIC_BLOCKS, _FINGERPRINT_BLOCK))

_PRODUCES_BASELINE_METRICS = Produces("baseline_metrics", (
    *_METRIC_BLOCKS,
    Block("purchase_counts", "各 item 在回溯視窗內的歷史購買次數（label_table 數出），跨評估月份加總（兩種計分模式都是 label_table 的）。",
          "每個 item 一格"),
    Block("monthly_counts", "同上的購買次數，依日曆月份拆開。", "每個 item × 每個月份一格"),
    Block("popularity_rate",
          "rate 模式的正例率帳：rate（整個視窗加總的正例數 ÷ 候選數）、candidates、"
          "positives、monthly_positives（正例數依日曆月份）、window_months_covered（各評估日視窗內有候選列的月數）。",
          "每個 item 一格，monthly_positives 再加每個月份",
          when="baseline 以 rate 計分（--post-training 且 evaluation.baseline.score: rate）"),
    Block(POPULARITY_WINDOW_KEY,
          "熱門度回溯視窗的帳：lookback_months 與 score（計分時用的設定）、months_covered（有資料的月數）、"
          "window_months_covered（各評估日涵蓋的月數）、full_window_months、per_month_divisor（月均的分母）、"
          "window_partial（視窗是否不完整）。",
          "整次執行一份"),
    _FINGERPRINT_BLOCK,
))

_PRODUCES_PREDICTION_QUALITY = Produces("prediction_quality_metrics", (
    Block("columns", "算的時候用的 item、分數、標籤、權重欄名。", "整次執行一份"),
    Block("bins", "分數分箱的設定：細箱數、顯示箱數、下界、上界、箱寬。", "整次執行一份"),
    Block("overall", "全部評估列當作二元預測的結果：摘要（pr_auc、roc_auc、最佳 F1 的箱界）、細箱表、顯示箱表、門檻掃描。",
          "整體一格，加每個分數區間"),
    Block("per_item",
          "列數最多的 top_n 個 item 的結果：top_n、n_items（全部 item 數）、listed（列出的 item）、"
          "totals（每個 item 的列數與正例數）、summary（每個列出 item 的摘要）、"
          "bins（細箱表）、display_bins（顯示箱表）。",
          "每個列出的 item 一格，加每個分數區間"),
    _FINGERPRINT_BLOCK,
))

_PRODUCES_REPORT_AGGREGATES = Produces("evaluation_report_aggregates", (
    Block("columns", "算的時候用的 item、分數、名次、標籤欄名。", "整次執行一份"),
    Block("score_histogram", "每個 item 的分數直方圖計數。", "每個 item × 每個分數區間",
          when=_DISTRIBUTIONS_WHEN),
    Block("score_box_by_label", "每個 item、每個標籤值的分數箱形圖統計。", "每個 item × 每個標籤值",
          when=_DISTRIBUTIONS_WHEN),
    Block("rank_counts", "每個 item 排在每個名次的列數。", "每個 item × 每個名次",
          when=_DISTRIBUTIONS_WHEN),
    Block("positive_rank_counts", "每個 item 的正例排在每個名次的列數。", "每個 item × 每個名次",
          when=_DISTRIBUTIONS_WHEN),
    Block("positive_rate", "每個 item 在每個名次的正例率。", "每個 item × 每個名次",
          when=_DISTRIBUTIONS_WHEN),
    _FINGERPRINT_BLOCK,
))

#: The cell-by-cell comparison ``compare_metric_results`` returns, shared by
#: the baseline comparison and the source comparison.
_COMPARISON_BLOCKS = (
    Block("overall", "兩邊整體指標格子的並排：各自的值與差。", "整體的每個指標格子一格"),
    Block("per_item", "兩邊每個 item 指標格子的並排。", "每個 item × 每個指標格子"),
    Block("macro_by_item", "兩邊對 item 等權平均的指標並排。", "每個指標格子一格"),
    Block("per_segment", "兩邊每個分群值指標格子的並排。", "每個分群值 × 每個指標格子"),
    Block(POPULATION_KEY, "兩邊各自的母體註記（\"all\" 的 K、進 macro 的 item、被計分的 query 數、分群欄）並排。",
          "整次執行一份"),
    Block("category", "item 大類層的同一組並排；任一邊沒有大類層時為 None。",
          "整體一格，形狀同上層"),
)

_PRODUCES_BASELINE_COMPARISON = Produces("baseline_comparison", (
    *_COMPARISON_BLOCKS, _FINGERPRINT_BLOCK))

_PRODUCES_DISPLAY_KS = Produces("evaluation_display_ks", (
    Block("evaluation_metrics",
          "模型指標的 K 欄位清單，各粒度截在該粒度的 all_k（含大類層）。",
          "整次執行一份"),
    Block("baseline_comparison",
          "baseline 對照的 K 欄位清單，截在 compare_k 與該粒度的 all_k；沒有 baseline 對照時為 None。",
          "整次執行一份"),
    _FINGERPRINT_BLOCK,
))

_PRODUCES_COMPARE_COVERAGE = Produces("compare_coverage", (
    Block("kind_a", "A 側（本次執行）的來源種類。", "整次執行一份"),
    Block("model_version_a", "A 側的 model_version。", "整次執行一份"),
    Block("source_a", "A 側讀的表。", "整次執行一份"),
    Block("kind_b", "B 側（比較來源）的種類。", "整次執行一份"),
    Block("model_version_b", "B 側的 model_version；不適用時為 \"n/a\"。", "整次執行一份"),
    Block("table_b", "B 側讀的表；不適用時為 \"n/a\"。", "整次執行一份"),
    Block("n_query_group_A_full", "A 側限縮前的 query group 數。", "整體一格"),
    Block("n_query_group_B_full", "B 側限縮前的 query group 數。", "整體一格"),
    Block("n_query_group_common", "兩側限縮後都還在的 query group 數。", "整體一格"),
    Block("n_item_A_full", "A 側限縮前的 item 數。", "整體一格"),
    Block("n_item_B_full", "B 側限縮前的 item 數。", "整體一格"),
    Block("n_item_common", "兩側共有的 item 數。", "整體一格"),
    Block("dropped_items_A", "只有 A 側有、因而被排除的 item。", "每個被排除的 item 一格"),
    Block("dropped_items_B", "只有 B 側有、因而被排除的 item。", "每個被排除的 item 一格"),
    _FINGERPRINT_BLOCK,
    _COMPARE_SOURCE_BLOCK,
))

#: The row facts of one side of a comparison: the run's own facts without
#: the segment copy, plus the compare source.
_COMPARE_ROW_FACT_BLOCKS = (*_ROW_FACT_BLOCKS, _FINGERPRINT_BLOCK,
                            _COMPARE_SOURCE_BLOCK)
_COMPARE_METRIC_BLOCKS = (*_METRIC_BLOCKS, _FINGERPRINT_BLOCK,
                          _COMPARE_SOURCE_BLOCK)

_PRODUCES_COMPARE_ROW_FACTS_A = Produces("compare_row_facts_a", _COMPARE_ROW_FACT_BLOCKS)
_PRODUCES_COMPARE_ROW_FACTS_B = Produces("compare_row_facts_b", _COMPARE_ROW_FACT_BLOCKS)
_PRODUCES_COMPARE_METRICS_A = Produces("compare_metrics_a", _COMPARE_METRIC_BLOCKS)
_PRODUCES_COMPARE_METRICS_B = Produces("compare_metrics_b", _COMPARE_METRIC_BLOCKS)

_PRODUCES_SOURCE_COMPARISON = Produces("source_comparison", (
    *_COMPARISON_BLOCKS, _FINGERPRINT_BLOCK, _COMPARE_SOURCE_BLOCK))

_PRODUCES_COMPARISON_DISPLAY_KS = Produces("comparison_display_ks", (
    Block(DISPLAY_ALL_K, "本次執行這一側的 \"all\" 解析成的 K。", "整次執行一份"),
    Block(DISPLAY_COMPARE_K, "compare_k 設定截在 all_k 的 K 欄位清單。", "整次執行一份"),
    Block("category", "大類層的同兩項；來源比較沒有大類層時為 None。", "整次執行一份"),
    _FINGERPRINT_BLOCK,
    _COMPARE_SOURCE_BLOCK,
))


RESULTS = by_result((
    _PRODUCES_SEGMENT_COLUMNS,
    _PRODUCES_ITEM_CATEGORIES,
    _PRODUCES_ROW_FACTS,
    _PRODUCES_EVALUATION_METRICS,
    _PRODUCES_BASELINE_METRICS,
    _PRODUCES_PREDICTION_QUALITY,
    _PRODUCES_REPORT_AGGREGATES,
    _PRODUCES_BASELINE_COMPARISON,
    _PRODUCES_DISPLAY_KS,
    _PRODUCES_COMPARE_COVERAGE,
    _PRODUCES_COMPARE_ROW_FACTS_A,
    _PRODUCES_COMPARE_ROW_FACTS_B,
    _PRODUCES_COMPARE_METRICS_A,
    _PRODUCES_COMPARE_METRICS_B,
    _PRODUCES_SOURCE_COMPARISON,
    _PRODUCES_COMPARISON_DISPLAY_KS,
))
