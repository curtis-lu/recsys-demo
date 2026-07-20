"""Tests for evaluation.diagnostics_spark — Spark-side aggregation that
replaces row-level toPandas for the diagnostics figures. Each function reduces
the (potentially huge) eval_predictions DataFrame to a small pandas frame whose
size does not grow with the number of rows."""

import json

import pandas as pd

from recsys_tfb.evaluation.diagnostics_spark import (
    aggregate_report_diagnostics,
    calibration_bins,
    positive_rank_count_matrix,
    positive_rate_matrix,
    rank_count_matrix,
    score_box_stats_by_label,
    score_histogram_counts,
)


def _sdf(spark, rows, cols):
    return spark.createDataFrame([tuple(r) for r in rows], list(cols))


class TestScoreHistogramCounts:
    def test_shared_global_bins_and_counts(self, spark):
        # Global min=0.0, max=1.0, nbins=2 -> width=0.5
        # bin(v) = min(1, floor(v/0.5)); 1.0 clamps into the top bin.
        rows = [
            ("A", 0.0), ("A", 0.4), ("A", 0.6), ("A", 1.0),
            ("B", 0.5),
        ]
        sdf = _sdf(spark, rows, ["item", "score"])
        out = score_histogram_counts(sdf, "item", "score", nbins=2)

        # bin centers for width 0.5 over [0,1]: 0.25 and 0.75
        centers = sorted(out["bin_center"].round(6).unique())
        assert centers == [0.25, 0.75]

        def cnt(item, center):
            m = out[(out["item"] == item) & (out["bin_center"].round(6) == center)]
            return int(m["count"].sum())

        assert cnt("A", 0.25) == 2   # 0.0, 0.4
        assert cnt("A", 0.75) == 2   # 0.6, 1.0
        assert cnt("B", 0.75) == 1   # 0.5 -> floor(1.0)=1 -> top bin
        assert out["bin_width"].round(6).unique().tolist() == [0.5]

    def test_constant_score_single_bin(self, spark):
        rows = [("A", 3.0), ("A", 3.0), ("A", 3.0)]
        sdf = _sdf(spark, rows, ["item", "score"])
        out = score_histogram_counts(sdf, "item", "score", nbins=4)
        # All identical -> one bin holding everything.
        assert int(out["count"].sum()) == 3
        assert len(out) == 1


class TestScoreBoxStatsByLabel:
    def test_one_row_per_item_label(self, spark):
        rows = [
            ("A", 1.0, 1), ("A", 2.0, 1), ("A", 5.0, 0), ("A", 6.0, 0),
            ("B", 3.0, 1), ("B", 4.0, 0),
        ]
        sdf = _sdf(spark, rows, ["item", "score", "label"])
        out = score_box_stats_by_label(sdf, "item", "score", "label")
        pairs = {(r["item"], int(r["label"])) for _, r in out.iterrows()}
        assert pairs == {("A", 1), ("A", 0), ("B", 1), ("B", 0)}


class TestRankCountMatrix:
    def test_counts_per_item_rank(self, spark):
        # 2 items, 2 customers; ranks 1..2 within each query.
        rows = [
            ("c1", "A", 1), ("c1", "B", 2),
            ("c2", "A", 2), ("c2", "B", 1),
        ]
        sdf = _sdf(spark, rows, ["cust", "item", "rank"])
        mat = rank_count_matrix(sdf, "item", "rank")
        # index = sorted items, columns = ranks 1..n_items
        assert list(mat.index) == ["A", "B"]
        assert list(mat.columns) == [1, 2]
        assert mat.loc["A", 1] == 1
        assert mat.loc["A", 2] == 1
        assert mat.loc["B", 1] == 1
        assert mat.loc["B", 2] == 1
        # Each item appears in every query exactly once -> row sums == n_queries
        assert mat.loc["A"].sum() == 2


class TestPositiveRankCountMatrix:
    def test_only_positive_rows_counted(self, spark):
        rows = [
            ("A", 1, 1), ("A", 2, 0),
            ("B", 1, 0), ("B", 2, 1), ("B", 1, 1),
        ]
        sdf = _sdf(spark, rows, ["item", "rank", "label"])
        mat = positive_rank_count_matrix(sdf, "item", "rank", "label")
        assert mat.loc["A", 1] == 1
        assert mat.loc["A", 2] == 0   # label 0, excluded
        assert mat.loc["B", 1] == 1
        assert mat.loc["B", 2] == 1
        # total counted == number of positive rows
        assert mat.values.sum() == 3


class TestPositiveRateMatrix:
    def test_rate_is_positive_over_total(self, spark):
        # Two items so the rank axis spans 1..2 (ranks 1..n_items convention).
        rows = [
            ("A", 1, 1), ("A", 1, 0), ("A", 1, 1),   # A rank1: 2/3
            ("A", 2, 0),                              # A rank2: 0/1
            ("B", 1, 1), ("B", 2, 0),                 # B present -> n_items=2
        ]
        sdf = _sdf(spark, rows, ["item", "rank", "label"])
        mat = positive_rate_matrix(sdf, "item", "rank", "label")
        assert abs(mat.loc["A", 1] - 2 / 3) < 1e-9
        assert mat.loc["A", 2] == 0.0
        assert mat.loc["B", 1] == 1.0
        assert ((mat.values >= 0.0) & (mat.values <= 1.0)).all()


class TestCalibrationBins:
    def test_bins_and_means(self, spark):
        # n_bins=5 over [0,1]; bin = min(4, floor(score*5)).
        rows = [
            ("A", 0.05, 0), ("A", 0.15, 1),   # bin0=0.05/lab0, bin0=0.15/lab1
            ("A", 0.45, 1), ("A", 0.55, 1),
            ("A", 0.95, 1),
        ]
        sdf = _sdf(spark, rows, ["item", "score", "label"])
        out = calibration_bins(sdf, "item", "score", "label", n_bins=5)
        # 0.05 and 0.15 both land in bin 0 -> prob_pred=0.10, prob_true=0.5
        b0 = out[(out["item"] == "A") & (out["bin"] == 0)].iloc[0]
        assert abs(b0["prob_pred"] - 0.10) < 1e-9
        assert abs(b0["prob_true"] - 0.5) < 1e-9
        # all prob values within [0,1]
        assert ((out["prob_true"] >= 0) & (out["prob_true"] <= 1)).all()

    def test_skip_item_with_too_few_rows_or_no_positives(self, spark):
        rows = (
            [("A", 0.1 * i, 1 if i % 2 else 0) for i in range(1, 7)]  # 6 rows, has pos
            + [("B", 0.2, 1), ("B", 0.4, 0)]                          # 2 rows < n_bins
            + [("C", 0.1 * i, 0) for i in range(1, 7)]                # 6 rows, no pos
        )
        sdf = _sdf(spark, rows, ["item", "score", "label"])
        out = calibration_bins(sdf, "item", "score", "label", n_bins=5)
        items = set(out["item"])
        assert "A" in items
        assert "B" not in items   # too few rows
        assert "C" not in items   # no positives


class TestFrameJson:
    """聚合小 frame 的落地格式。

    兩種形狀分開處理的理由：長格式的 index 是無意義的 RangeIndex，矩陣的
    index 是 item 名稱（heatmap 的 y 軸標籤，丟了圖就沒有標籤）。
    """

    def test_matrix_round_trip_preserves_index_and_int_columns(self):
        import json

        from recsys_tfb.evaluation.diagnostics_spark import (
            frame_from_json, frame_to_json,
        )
        mat = pd.DataFrame([[1, 2], [3, 4]],
                           index=["insur", "loan"], columns=[1, 2])
        payload = frame_to_json(mat, "matrix")
        back = frame_from_json(json.loads(json.dumps(payload)))
        assert back.equals(mat)
        # 欄名必須留在 int：_heatmap_from_matrix 用 list(matrix.columns) 當
        # rank 值渲染成 "Rank 1"。變成字串 "1" 不會有任何測試轉紅，圖也照畫。
        assert list(back.columns) == [1, 2]
        assert list(back.index) == ["insur", "loan"]

    def test_long_round_trip_drops_the_meaningless_index(self):
        import json

        from recsys_tfb.evaluation.diagnostics_spark import (
            frame_from_json, frame_to_json,
        )
        long = pd.DataFrame({"prod_name": ["a", "b"], "count": [1, 2]})
        payload = frame_to_json(long, "long")
        assert "index" not in payload
        back = frame_from_json(json.loads(json.dumps(payload)))
        assert back.equals(long)
        assert isinstance(back.index, pd.RangeIndex)

    def test_kind_is_declared_not_inferred(self):
        """``kind`` 必須明寫。

        從 index 型別推斷會在「item 名稱剛好是 0,1,2」時猜錯——那時矩陣的
        index 看起來就是 RangeIndex，會被當成長格式而把 y 軸標籤丟掉，而且
        不會有任何測試轉紅。
        """
        import pytest

        from recsys_tfb.evaluation.diagnostics_spark import frame_to_json
        with pytest.raises(ValueError, match="kind"):
            frame_to_json(pd.DataFrame({"a": [1]}), "records")

    def test_empty_frame_round_trips(self):
        """退化輸入：空 frame。

        ``score_histogram_counts`` 在輸入為空時就是回
        ``pd.DataFrame(columns=cols)``（diagnostics_spark.py:39），所以這不是
        假想的邊界。
        """
        import json

        from recsys_tfb.evaluation.diagnostics_spark import (
            frame_from_json, frame_to_json,
        )
        empty = pd.DataFrame(columns=["prod_name", "count"])
        back = frame_from_json(
            json.loads(json.dumps(frame_to_json(empty, "long"))))
        assert list(back.columns) == ["prod_name", "count"]
        assert len(back) == 0

    def test_nan_becomes_null_so_the_file_stays_strict_json(self):
        """NaN 必須換成 None。

        ``JSONDataset.save``（io/json_dataset.py:20-23）用預設
        ``allow_nan=True``，NaN 會寫成 ``NaN`` 這個**非合法 JSON** 的字面值。
        Python 的 ``json.loads`` 讀得回來，別的工具不行——而這些檔案的用途
        就是被拷到別的環境去讀。

        用 ``parse_constant`` 驗而不是掃字串：``"NaN" in text`` 會被
        item 名稱裡剛好有 NaN 三個字母的情況誤判。
        """
        import json

        import numpy as np

        from recsys_tfb.evaluation.diagnostics_spark import frame_to_json
        df = pd.DataFrame({"a": [1.0, np.nan]})
        text = json.dumps(frame_to_json(df, "long"))

        def _boom(const):
            raise AssertionError(f"非合法 JSON 常數：{const}")

        json.loads(text, parse_constant=_boom)   # 有 NaN/Infinity 就 raise


def _report_sdf(spark):
    """一份足夠讓六個家族都非空的最小輸入。

    ``calibration_bins`` 會跳過「列數 < n_bins」或「沒有正例」的 item
    （diagnostics_spark.py:210-212），所以每個 item 要有 >= n_bins 列且至少
    一個正例——否則 calibration 那格是空 frame，測試看起來過了、其實什麼都
    沒量到。這裡用 n_calibration_bins=2 壓低門檻。
    """
    rows = []
    for i, item in enumerate(["insur", "loan"]):
        for k in range(4):
            rows.append((item, 0.1 + 0.2 * k, k + 1, 1 if k == i else 0))
    return _sdf(spark, rows, ["prod_name", "score", "rank", "label"])


class TestAggregateReportDiagnostics:
    def test_returns_json_safe_payload_for_every_enabled_family(self, spark):
        """六個聚合各自成為 payload 的一個鍵，且整包可嚴格序列化。

        用 ``parse_constant`` 驗嚴格性而不是掃字串：``"NaN" in text`` 會被
        item 名稱裡剛好有那三個字母的情況誤判。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
        )
        assert set(out) == {
            "columns", "score_histogram", "score_box_by_label",
            "rank_counts", "positive_rank_counts", "positive_rate",
            "calibration",
        }
        # 每個家族都要有實際資料。長格式驗「有列」就夠；**矩陣家族不行**
        # ——`_to_matrix`（diagnostics_spark.py:116-122）對「有列但全被濾掉」
        # 的輸入會回一個 items×ranks 的**全零**frame，`data` 是
        # [[0,0],[0,0]] 恆為 truthy。只驗 truthy 等於沒驗到「量到東西」。
        for key in ("score_histogram", "score_box_by_label", "calibration"):
            assert out[key]["data"], f"{key} 是空的，這份 fixture 量不到它"
        for key in ("rank_counts", "positive_rank_counts", "positive_rate"):
            total = sum(abs(v) for row in out[key]["data"] for v in row)
            assert total > 0, f"{key} 是全零矩陣，這份 fixture 量不到它"

        def _boom(const):
            raise AssertionError(f"非合法 JSON 常數：{const}")

        json.loads(json.dumps(out), parse_constant=_boom)

    def test_matrix_families_keep_index_and_int_rank_columns(self, spark):
        """三個矩陣家族必須帶 index，且 rank 欄名留在 int。

        heatmap 用 ``list(matrix.index)`` 當 y 軸標籤、``list(matrix.columns)``
        當 rank 值（``distributions.py:98-105``）。丟了 index 圖照畫、只是沒有
        item 標籤；rank 變成字串 "1" 也照畫——兩種都不會有測試轉紅。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
        )
        for key in ("rank_counts", "positive_rank_counts", "positive_rate"):
            assert out[key]["kind"] == "matrix", key
            assert out[key]["index"] == ["insur", "loan"], key
            assert all(isinstance(c, int) for c in out[key]["columns"]), key

    def test_columns_are_carried_so_the_payload_renders_standalone(self, spark):
        """欄名要跟著 payload 走，不能在重繪時從 parameters 再推一次。

        離線重繪拷回來的是 JSON，不保證同一份 parameters 也拷了、更不保證那
        份 parameters 的 schema 與產生這份 JSON 時相同。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
        )
        assert out["columns"] == {
            "item": "prod_name", "score": "score",
            "rank": "rank", "label": "label",
        }

    def test_disabled_families_are_absent_not_empty(self, spark):
        """關掉的家族是「不存在」不是「空的」。

        空的看起來像「量到了、結果什麼都沒有」，那是這次重構要避免的誤讀
        （與 ``assemble_diagnosis_pages`` 對空頁的處理同一個立場）。

        兩個方向都測：只測其中一個的話，把兩個旗標接反了照樣有一條會綠。
        """
        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label",
            include_calibration=False,
        )
        assert "calibration" not in out
        assert "rank_counts" in out

        out = aggregate_report_diagnostics(
            _report_sdf(spark), item_col="prod_name", score_col="score",
            rank_col="rank", label_col="label", n_calibration_bins=2,
            include_distributions=False,
        )
        assert set(out) == {"columns", "calibration"}


def test_serialisation_round_trip_leaves_every_figure_identical(spark):
    """重構前後的圖必須逐點相同。

    比對 ``fig.to_json()`` 而不是眼睛看：序列化來回若把 int rank 變成字串、
    或把 item index 丟掉，圖**照樣畫得出來**，只是軸標籤變了——沒有任何既有
    測試會轉紅。這是本 task 唯一會靜默出錯的地方。
    """
    from recsys_tfb.evaluation.calibration import plot_calibration_curves
    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )
    from recsys_tfb.evaluation.report_builder import build_diagnostics_figures

    sdf = _report_sdf(spark)
    i, s, r, lab = "prod_name", "score", "rank", "label"

    # 舊路徑：Spark 聚合的 frame 直接進繪圖函式（重構前 nodes_spark.py:553-578）
    before = [
        plot_score_histogram(score_histogram_counts(sdf, i, s), item_col=i),
        plot_score_boxplot_by_label(
            score_box_stats_by_label(sdf, i, s, lab), item_col=i,
            label_col=lab),
        plot_rank_heatmap(rank_count_matrix(sdf, i, r)),
        plot_positive_rank_heatmap(
            positive_rank_count_matrix(sdf, i, r, lab)),
        plot_positive_rate_rank_heatmap(positive_rate_matrix(sdf, i, r, lab)),
        plot_calibration_curves(
            calibration_bins(sdf, i, s, lab, n_bins=2), item_col=i),
    ]

    # 新路徑：Spark 聚合 → JSON → 繪圖函式
    # 直接把聚合輸出餵進去，**不需要補任何旗標**——build_diagnostics_figures
    # 靠家族鍵判斷。要是哪天它改成看 enabled，這一行會回 [] 而下面的長度斷言
    # 會轉紅，那正是想要的。
    payload = aggregate_report_diagnostics(
        sdf, item_col=i, score_col=s, rank_col=r, label_col=lab,
        n_calibration_bins=2,
    )
    after = build_diagnostics_figures(payload)

    assert len(after) == len(before)
    for old, new in zip(before, after):
        assert new.to_json() == old.to_json()
