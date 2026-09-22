"""Tests for evaluation.baselines — Spark popularity baseline."""

import pandas as pd


def _parameters():
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {},
    }


def _label_table(spark):
    # History before 2025-01-31: A bought 3x, B 1x, C 0x.
    rows = []
    for snap, a, b, c in [("2024-06-30", 2, 1, 0), ("2024-12-31", 1, 0, 0)]:
        for i in range(3):
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "A", "label": 1 if i < a else 0})
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "B", "label": 1 if i < b else 0})
            rows.append({"snap_date": snap, "cust_id": f"h{i}",
                         "prod_name": "C", "label": 1 if i < c else 0})
    return spark.createDataFrame(pd.DataFrame(rows))


def test_purchase_counts_window_excludes_snap_date_and_after(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    counts = compute_purchase_counts(
        _label_table(spark), ["2025-01-31"], 12, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    # 12-month window [2024-01-31, 2025-01-31): both history snaps included.
    assert by_prod["A"] == 3
    assert by_prod["B"] == 1
    assert by_prod["C"] == 0


def test_purchase_counts_lookback_limits_window(spark):
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # 3-month window [2024-10-31, 2025-01-31): only the 2024-12-31 snap.
    counts = compute_purchase_counts(
        _label_table(spark), ["2025-01-31"], 3, _parameters()
    )
    by_prod = {r["prod_name"]: r["score"] for r in counts.collect()}
    assert by_prod["A"] == 1
    assert by_prod["B"] == 0


def test_purchase_counts_raises_when_window_empty(spark):
    """bug 1 (ADR-0020): an empty lookback window used to silently fall back
    to the full label_table (which includes the eval month's own answers —
    the baseline would then rank by the ground truth). The user ruled
    baseline is load-bearing: raise instead of degrading to a leaky stub."""
    import pandas as pd
    import pytest

    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # label_table holds only the eval month itself (2026-01-31) -> the
    # [2025-01-31, 2026-01-31) window is empty.
    label_table = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2026-01-31"] * 2,
        "cust_id": ["h0", "h1"],
        "prod_name": ["A", "A"],
        "label": [1, 1],
    }))
    with pytest.raises(ValueError) as exc:
        compute_purchase_counts(label_table, ["2026-01-31"], 12, _parameters())
    message = str(exc.value)
    assert "2025-01-31" in message
    assert "2026-01-31" in message
    # The months list itself: a bare "2026-01" is always satisfied by the
    # window bound "2026-01-31" above, whatever the list says.
    assert "label_table has months: ['2026-01']" in message


def test_purchase_counts_no_longer_falls_back_when_no_history(spark):
    """Same scenario as the old fallback test, restated as a raise (bug 1)."""
    import pytest

    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    # snap_date before all history -> empty window -> now raises, no fallback.
    with pytest.raises(ValueError):
        compute_purchase_counts(
            _label_table(spark), ["2024-01-01"], 12, _parameters()
        )


def test_build_baseline_frame_replaces_score_and_drops_model_cols(spark):
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"] * 4,
        "cust_id": ["c1", "c1", "c2", "c2"],
        "prod_name": ["A", "B", "A", "B"],
        "label": [1, 0, 0, 1],
        "score": [0.9, 0.1, 0.2, 0.8],
        "rank": [1, 2, 2, 1],
        "model_version": ["v1"] * 4,
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "score": [5, 2],
    }))

    frame = build_baseline_frame(eval_pred, counts, _parameters())
    cols = set(frame.columns)
    assert "rank" not in cols and "model_version" not in cols
    assert "score" in cols and "label" in cols

    by_key = {(r["cust_id"], r["prod_name"]): r["score"] for r in frame.collect()}
    # Every customer gets the same per-product popularity score.
    assert by_key[("c1", "A")] == 5 and by_key[("c2", "A")] == 5
    assert by_key[("c1", "B")] == 2 and by_key[("c2", "B")] == 2


def test_build_baseline_frame_fills_missing_product_with_zero(spark):
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"] * 2,
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [1, 0],
        "score": [0.9, 0.1],
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31"], "prod_name": ["A"], "score": [5],
    }))
    frame = build_baseline_frame(eval_pred, counts, _parameters())
    by_prod = {r["prod_name"]: r["score"] for r in frame.collect()}
    assert by_prod["A"] == 5
    assert by_prod["B"] == 0


def test_build_baseline_frame_matches_timestamp_typed_snap_date(spark):
    """The join key must survive a timestamp-typed snap_date on the
    eval_predictions side (purchase_counts always emits a date string)."""
    from recsys_tfb.evaluation.baselines import build_baseline_frame

    eval_pred = spark.createDataFrame(pd.DataFrame({
        "snap_date": pd.to_datetime(["2025-01-31", "2025-01-31"]),
        "cust_id": ["c1", "c1"],
        "prod_name": ["A", "B"],
        "label": [1, 0],
        "score": [0.9, 0.1],
    }))
    counts = spark.createDataFrame(pd.DataFrame({
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "score": [5.0, 2.0],
    }))
    frame = build_baseline_frame(eval_pred, counts, _parameters())
    by_prod = {r["prod_name"]: r["score"] for r in frame.collect()}
    assert by_prod["A"] == 5.0  # join matched despite timestamp input
    assert by_prod["B"] == 2.0


def test_monthly_purchase_counts_breaks_window_into_months(spark):
    from recsys_tfb.evaluation.baselines import compute_monthly_purchase_counts

    # 12-month window [2024-01-31, 2025-01-31): the two history snaps land in
    # distinct calendar months (2024-06, 2024-12). A: 2 in 2024-06 + 1 in
    # 2024-12; B: 1 in 2024-06 only.
    monthly = compute_monthly_purchase_counts(
        _label_table(spark), ["2025-01-31"], 12, _parameters()
    )
    by_key = {(r["month"], r["prod_name"]): r["score"] for r in monthly.collect()}
    assert by_key[("2024-06", "A")] == 2
    assert by_key[("2024-12", "A")] == 1
    assert by_key[("2024-06", "B")] == 1
    # Summed over months reconciles with the single-window total (A=3, B=1).
    a_total = sum(v for (mo, it), v in by_key.items() if it == "A")
    b_total = sum(v for (mo, it), v in by_key.items() if it == "B")
    assert a_total == 3 and b_total == 1


def test_monthly_counts_by_window_reads_label_table_once(spark):
    """#374, several evaluated dates: every window's per-month counts from one
    scan of label_table, not one scan per window.

    Windows (12 months): 2025-01-31 → [2024-01-31, 2025-01-31) holds both
    history snaps; 2024-12-31 → [2023-12-31, 2024-12-31) holds 2024-06-30 only
    (its own date is excluded).
    """
    from recsys_tfb.evaluation.baselines import (
        compute_monthly_purchase_counts_by_window,
    )

    monthly = compute_monthly_purchase_counts_by_window(
        _label_table(spark), ["2025-01-31", "2024-12-31"], 12, _parameters()
    )
    by_key = {(r["window_date"], r["month"], r["prod_name"]): r["score"]
              for r in monthly.collect()}
    assert by_key == {
        ("2025-01-31", "2024-06", "A"): 2, ("2025-01-31", "2024-12", "A"): 1,
        ("2025-01-31", "2024-06", "B"): 1, ("2025-01-31", "2024-12", "B"): 0,
        ("2025-01-31", "2024-06", "C"): 0, ("2025-01-31", "2024-12", "C"): 0,
        ("2024-12-31", "2024-06", "A"): 2, ("2024-12-31", "2024-06", "B"): 1,
        ("2024-12-31", "2024-06", "C"): 0,
    }
    # The optimized logical plan names each read of an in-memory frame once
    # (the physical one prints both AQE plans); the window bounds are a
    # second, separate frame whose first column is window_date.
    plan = monthly._jdf.queryExecution().optimizedPlan().toString()
    assert plan.count("LogicalRDD [snap_date#") == 1, plan


def test_monthly_purchase_counts_rejects_empty_snap_dates(spark):
    import pytest
    from recsys_tfb.evaluation.baselines import compute_monthly_purchase_counts

    empty = spark.createDataFrame(
        [], schema="snap_date string, prod_name string, label int"
    )
    with pytest.raises(ValueError, match="non-empty"):
        compute_monthly_purchase_counts(empty, [], 12, _parameters())


def test_compute_purchase_counts_rejects_empty_snap_dates(spark):
    import pytest
    from recsys_tfb.evaluation.baselines import compute_purchase_counts

    empty = spark.createDataFrame(
        [], schema="snap_date string, prod_name string, label int"
    )
    with pytest.raises(ValueError, match="non-empty"):
        compute_purchase_counts(empty, [], 12, _parameters())


# ---------------------------------------------------------------------------
# Positive-rate mode (#397): denominator = candidate rows in sample_pool.
# ---------------------------------------------------------------------------


def _rate_parameters(score="rate"):
    params = _parameters()
    params["evaluation"] = {"baseline": {"lookback_months": 12, "score": score}}
    return params


def _grid(spark, rows):
    return spark.createDataFrame(pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name"]))


def _labels(spark, rows):
    return spark.createDataFrame(pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "label"]))


def _counts_by_item(frame):
    return {
        r["prod_name"]: (r["n_candidates"], r["n_positives"])
        for r in frame.groupBy("prod_name").agg(
            {"n_candidates": "sum", "n_positives": "sum"}
        ).withColumnRenamed("sum(n_candidates)", "n_candidates")
        .withColumnRenamed("sum(n_positives)", "n_positives").collect()
    }


class TestBaselineScore:
    def test_absent_means_count(self):
        from recsys_tfb.evaluation.baselines import baseline_score

        assert baseline_score(_parameters()) == "count"
        assert baseline_score({"evaluation": {"baseline": None}}) == "count"

    def test_rate_is_read(self):
        from recsys_tfb.evaluation.baselines import baseline_score

        assert baseline_score(_rate_parameters()) == "rate"

    def test_rate_is_wired_under_post_training_with_the_section_on(self):
        from recsys_tfb.evaluation.baselines import baseline_scores_by_rate

        params = _rate_parameters()
        assert baseline_scores_by_rate(params, post_training=True)
        # Monitoring scores the full grid: it keeps the count.
        assert not baseline_scores_by_rate(params, post_training=False)
        params["evaluation"]["report"] = {"sections": {"baseline": False}}
        assert not baseline_scores_by_rate(params, post_training=True)
        assert not baseline_scores_by_rate(
            _rate_parameters("count"), post_training=True)


class TestPeriodCandidateCounts:
    def test_denominator_is_candidate_rows_when_label_table_holds_positives_only(
        self, spark,
    ):
        """label_table with positive rows only: counting its rows as the
        denominator would give every item a rate of 1."""
        from recsys_tfb.evaluation.baselines import (
            compute_period_candidate_counts,
        )

        grid = _grid(spark, [("2024-06-30", f"c{i}", p)
                             for i in range(10) for p in ("A", "B")])
        labels = _labels(spark, [("2024-06-30", "c0", "A", 1),
                                 ("2024-06-30", "c1", "A", 1),
                                 ("2024-06-30", "c2", "B", 1)])
        out = compute_period_candidate_counts(
            grid, labels, ["2024-06-30"], _rate_parameters())
        assert _counts_by_item(out) == {"A": (10, 2), "B": (10, 1)}

    def test_a_filtered_label_table_does_not_change_the_denominator(self, spark):
        """The bank example's label_table keeps only entities with a positive
        in the group: B's label rows are 4, its candidates are 100."""
        from recsys_tfb.evaluation.baselines import (
            compute_period_candidate_counts,
        )

        grid = _grid(spark, [("2024-06-30", f"c{i}", p)
                             for i in range(100) for p in ("A", "B")])
        label_rows = [("2024-06-30", f"c{i}", "A", 1 if i < 5 else 0)
                      for i in range(20)]
        label_rows += [("2024-06-30", f"c{i}", "B", 1 if i < 2 else 0)
                       for i in range(4)]
        out = compute_period_candidate_counts(
            grid, _labels(spark, label_rows), ["2024-06-30"], _rate_parameters())
        assert _counts_by_item(out) == {"A": (100, 5), "B": (100, 2)}

    def test_only_the_requested_periods_are_counted(self, spark):
        from recsys_tfb.evaluation.baselines import (
            compute_period_candidate_counts,
        )

        grid = _grid(spark, [("2024-05-31", "c0", "A"), ("2024-06-30", "c0", "A")])
        labels = _labels(spark, [("2024-05-31", "c0", "A", 1)])
        out = compute_period_candidate_counts(
            grid, labels, ["2024-06-30"], _rate_parameters())
        assert [(r["snap_date"], r["n_candidates"], r["n_positives"])
                for r in out.collect()] == [("2024-06-30", 1, 0)]

    def test_duplicated_label_keys_raise(self, spark):
        import pytest

        from recsys_tfb.evaluation.baselines import (
            compute_period_candidate_counts,
        )

        grid = _grid(spark, [("2024-06-30", "c0", "A")])
        labels = _labels(spark, [("2024-06-30", "c0", "A", 1),
                                 ("2024-06-30", "c0", "A", 1)])
        with pytest.raises(ValueError, match="1 duplicated label_table key"):
            compute_period_candidate_counts(
                grid, labels, ["2024-06-30"], _rate_parameters())

    def test_each_row_is_one_candidate_under_event(self, spark):
        """#378: with ``event`` declared one query group can hold the same
        item several times; each row is one candidate."""
        from recsys_tfb.evaluation.baselines import (
            compute_period_candidate_counts,
        )

        params = _rate_parameters()
        params["schema"]["columns"]["event"] = "impression_id"
        grid = spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2024-06-30"] * 3, "cust_id": ["c0"] * 3,
            "prod_name": ["A", "A", "B"], "impression_id": ["e1", "e2", "e3"],
        }))
        labels = spark.createDataFrame(pd.DataFrame({
            "snap_date": ["2024-06-30"], "cust_id": ["c0"],
            "prod_name": ["A"], "impression_id": ["e2"], "label": [1],
        }))
        out = compute_period_candidate_counts(grid, labels, ["2024-06-30"], params)
        assert _counts_by_item(out) == {"A": (2, 1), "B": (1, 0)}


def _period_counts(spark, rows):
    return spark.createDataFrame(pd.DataFrame(
        rows, columns=["snap_date", "prod_name", "n_candidates", "n_positives"]))


class TestPositiveRates:
    def test_rate_is_positives_over_candidates_in_the_window(self, spark):
        from recsys_tfb.evaluation.baselines import compute_positive_rates

        counts = _period_counts(spark, [
            ("2024-06-30", "A", 1000, 50), ("2024-06-30", "B", 100, 20),
            # Outside [2024-01-31, 2025-01-31): must not count.
            ("2025-01-31", "A", 1, 1), ("2023-12-31", "B", 1, 1),
        ])
        rates = compute_positive_rates(
            counts, ["2025-01-31"], 12, _rate_parameters())
        by = {r["prod_name"]: r["score"] for r in rates.collect()}
        assert by == {"A": 0.05, "B": 0.2}
        assert {r["snap_date"] for r in rates.collect()} == {"2025-01-31"}

    def test_empty_window_raises_even_when_label_table_is_not_empty(self, spark):
        """The rate is read off the candidate counts; label_table having rows
        in the window must not let an empty sample_pool window through."""
        import pytest

        from recsys_tfb.evaluation.baselines import compute_positive_rates

        counts = _period_counts(spark, [("2023-06-30", "A", 10, 1)])
        with pytest.raises(ValueError, match="sample_pool"):
            compute_positive_rates(counts, ["2025-01-31"], 12, _rate_parameters())

    def test_rate_and_count_rank_alike_on_a_full_grid(self, spark):
        """Every entity is a candidate for every item and every positive is a
        candidate row: the denominators are equal, so the two orders match."""
        from recsys_tfb.evaluation.baselines import (
            compute_period_candidate_counts,
            compute_positive_rates,
            compute_purchase_counts,
        )

        items = ["A", "B", "C", "D"]
        grid = _grid(spark, [("2024-06-30", f"c{i}", p)
                             for i in range(20) for p in items])
        positives = {"A": 7, "B": 2, "C": 11, "D": 0}
        labels = _labels(spark, [
            ("2024-06-30", f"c{i}", p, 1 if i < n else 0)
            for p, n in positives.items() for i in range(20)])
        params = _rate_parameters()
        rates = compute_positive_rates(
            compute_period_candidate_counts(grid, labels, ["2024-06-30"], params),
            ["2025-01-31"], 12, params)
        counts = compute_purchase_counts(labels, ["2025-01-31"], 12, params)

        def order(frame):
            rows = sorted(frame.collect(),
                          key=lambda r: (-r["score"], r["prod_name"]))
            return [r["prod_name"] for r in rows]

        assert order(rates) == order(counts) == ["C", "A", "B", "D"]

    def test_monthly_positives_by_window(self, spark):
        from recsys_tfb.evaluation.baselines import (
            compute_monthly_candidate_counts_by_window,
        )

        counts = _period_counts(spark, [
            ("2024-06-03", "A", 10, 1), ("2024-06-10", "A", 10, 2),
            ("2025-02-03", "A", 10, 4),  # in the February window only
        ])
        rows = compute_monthly_candidate_counts_by_window(
            counts, ["2025-01-31", "2025-02-28"], 12, _rate_parameters()
        ).collect()
        got = sorted((r["window_date"], r["month"], r["prod_name"],
                      r["n_candidates"], r["n_positives"]) for r in rows)
        assert got == [
            ("2025-01-31", "2024-06", "A", 20, 3),
            ("2025-02-28", "2024-06", "A", 20, 3),
            ("2025-02-28", "2025-02", "A", 10, 4),
        ]


def test_list_candidate_periods_lists_the_time_values_in_the_windows(spark):
    from recsys_tfb.evaluation.baselines import list_candidate_periods

    grid = _grid(spark, [("2023-12-31", "c0", "A"), ("2024-06-03", "c0", "A"),
                         ("2024-06-10", "c1", "B"), ("2025-01-31", "c0", "A")])
    assert list_candidate_periods(
        grid, ["2025-01-31"], 12, _rate_parameters()
    ) == ["2024-06-03", "2024-06-10"]
