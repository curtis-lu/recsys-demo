import json

import pytest

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation import metrics_spark as ms
from recsys_tfb.evaluation import report_builder as rb
from recsys_tfb.evaluation.metrics import ALL_K_KEY, resolved_all_k
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT


def _params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {}}


def _df(spark):
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", 0.9, 1),
            ("20240331", "c1", "B", 0.1, 0),
            ("20240331", "c2", "A", 0.2, 0),
            ("20240331", "c2", "B", 0.8, 1),
            ("20240229", "c1", "A", 0.5, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def test_dataset_overview_totals(spark):
    ov = ms.compute_dataset_overview(_df(spark), _params())
    t = ov["totals"]
    assert t["n_rows"] == 5
    assert t["n_entities"] == 2
    assert t["n_items"] == 2
    assert t["n_snap_dates"] == 2
    assert t["n_positives"] == 3
    assert t["positive_rate"] == pytest.approx(3 / 5)
    assert t["avg_positives_per_entity"] == pytest.approx(1.5)


def test_dataset_overview_by_snap_and_item(spark):
    ov = ms.compute_dataset_overview(_df(spark), _params())
    assert ov["by_snap_date"]["20240331"]["n_rows"] == 4
    assert ov["by_snap_date"]["20240331"]["n_positives"] == 2
    assert ov["by_item"]["A"]["n_entities"] == 2
    assert ov["by_item"]["A"]["n_positives"] == 2


def _df_seg(spark):
    # 在 _df 基礎上加 seg 欄（segment 是 customer 級屬性：c1→X、c2→Y）
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", 0.9, 1, "X"),
            ("20240331", "c1", "B", 0.1, 0, "X"),
            ("20240331", "c2", "A", 0.2, 0, "Y"),
            ("20240331", "c2", "B", 0.8, 1, "Y"),
            ("20240229", "c1", "A", 0.5, 1, "X"),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "seg"],
    )


def _params_seg():
    p = _params()
    p["evaluation"] = {"segment_columns": ["seg"]}
    return p


def test_dataset_overview_by_segment(spark):
    ov = ms.compute_dataset_overview(
        _df_seg(spark), _params_seg(), segment_columns=["seg"]
    )
    bs = ov["by_segment"]
    # query＝(snap_date,cust_id) distinct：X={(0331,c1),(0229,c1)}=2、Y={(0331,c2)}=1、總=3
    assert bs["X"]["n_positives"] == 2          # 0331 c1 A、0229 c1 A
    assert bs["X"]["n_queries"] == 2
    assert bs["X"]["query_share"] == pytest.approx(2 / 3)
    assert bs["Y"]["n_queries"] == 1
    assert bs["Y"]["query_share"] == pytest.approx(1 / 3)
    assert bs["X"]["positive_rate"] == pytest.approx(2 / 3)   # 2 正例 / 3 候選列
    # query_share 逐列加總＝1（segment 把 query 乾淨分群）
    assert sum(v["query_share"] for v in bs.values()) == pytest.approx(1.0)


def test_dataset_overview_no_by_segment_without_seg_col(spark):
    # 沒設 segment_columns（或欄不在資料中）→ 不產 by_segment
    ov = ms.compute_dataset_overview(_df(spark), _params())
    assert "by_segment" not in ov


# ---------------------------------------------------------------------------
# 鍵含 NULL 時的計數（ADR-0018 決定 3）。整份輸出逐字釘住：收斂 job 數之後
# 輸出不准變。比 JSON 字串而不只比 dict，因為 dict 的 == 看不出 6 變成 6.0。
# 期望值是從資料列手算的，兩種計法並存是既有行為：
#   totals 的 n_entities／n_items／n_snap_dates、by_segment 分母的 query 總數
#       → NULL 算成一個值（跟 select(...).distinct().count() 一樣）
#   分群內的 n_entities／n_queries
#       → 鍵裡任一欄是 NULL 就不算進 distinct（裸 countDistinct 的計法）；
#         分群那一列本身照樣保留
# 這裡釘的是「本張前的行為」，不是「正確的行為」：鍵含 NULL 時分子不算、
# 分母算，by_segment 的 query_share 加總不到 1（見下面 by_segment 那條）。
# ---------------------------------------------------------------------------


def _assert_same_json(actual, expected):
    assert actual == expected
    assert json.dumps(actual, sort_keys=True) == json.dumps(
        expected, sort_keys=True
    )


def _df_null_keys(spark):
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", 0.9, 1, "X"),
            ("20240331", "c1", "B", 0.1, 0, "X"),
            ("20240331", None, "A", 0.2, 0, None),     # entity NULL
            ("20240331", None, None, 0.8, 1, None),    # entity、item NULL
            (None, "c2", "B", 0.5, 1, "Y"),            # time NULL
            ("20240229", "c2", None, 0.3, None, "Y"),  # item、label NULL
        ],
        schema="snap_date string, cust_id string, prod_name string, "
               "score double, label int, seg string",
    )


def _expected_null_keys():
    return {
        "totals": {
            "n_rows": 6,
            "n_entities": 3,     # c1、c2、NULL
            "n_items": 3,        # A、B、NULL
            "n_snap_dates": 3,   # 20240331、20240229、NULL
            "n_positives": 3,
            "positive_rate": 3 / 6,
            "avg_positives_per_entity": 3 / 3,
        },
        "by_snap_date": {
            "20240331": {"n_rows": 4, "n_positives": 2, "n_entities": 1,
                         "positive_rate": 2 / 4},
            "None": {"n_rows": 1, "n_positives": 1, "n_entities": 1,
                     "positive_rate": 1 / 1},
            "20240229": {"n_rows": 1, "n_positives": 0, "n_entities": 1,
                         "positive_rate": 0 / 1},
        },
        "by_item": {
            "A": {"n_rows": 2, "n_positives": 1, "n_entities": 1,
                  "positive_rate": 1 / 2},
            "B": {"n_rows": 2, "n_positives": 1, "n_entities": 2,
                  "positive_rate": 1 / 2},
            "None": {"n_rows": 2, "n_positives": 1, "n_entities": 1,
                     "positive_rate": 1 / 2},
        },
    }


def test_dataset_overview_null_keys_without_segment(spark):
    ov = ms.compute_dataset_overview(_df_null_keys(spark), _params())
    expected = _expected_null_keys()
    assert ov["totals"] == expected["totals"]
    _assert_same_json(ov, expected)


def test_dataset_overview_null_keys_by_segment(spark):
    # query 總數＝(snap_date, cust_id) 含 NULL 的 distinct＝4：
    # (0331,c1)、(0331,NULL)、(NULL,c2)、(0229,c2)
    # 各 segment 的 n_queries 不算含 NULL 的鍵 → query_share 加總＝2/4，不是 1
    ov = ms.compute_dataset_overview(
        _df_null_keys(spark), _params(), segment_columns=["seg"]
    )
    expected = _expected_null_keys()
    expected["by_segment"] = {
        "X": {"n_rows": 2, "n_positives": 1, "n_entities": 1,
              "positive_rate": 1 / 2, "n_queries": 1, "query_share": 1 / 4},
        UNMATCHED_SEGMENT: {"n_rows": 2, "n_positives": 1, "n_entities": 0,
                            "positive_rate": 1 / 2, "n_queries": 0,
                            "query_share": 0 / 4},
        "Y": {"n_rows": 2, "n_positives": 1, "n_entities": 1,
              "positive_rate": 1 / 2, "n_queries": 1, "query_share": 1 / 4},
    }
    assert ov["by_segment"] == expected["by_segment"]
    _assert_same_json(ov, expected)


def test_dataset_overview_null_keys_two_column_entity(
    spark, two_column_entity_params
):
    df = spark.createDataFrame(
        [
            ("20240331", "b1", "c1", "p1", 0.9, 1, "X"),
            ("20240331", "b1", None, "p1", 0.8, 0, "X"),
            ("20240331", "b1", None, "p2", 0.7, 1, "X"),  # 跟上一列同一個 entity
            ("20240331", None, None, "p1", 0.6, 0, "Y"),
            ("20240229", "b1", "c1", "p2", 0.5, 0, "X"),
        ],
        schema="snap_date string, branch_id string, cust_id string, "
               "prod_name string, score double, label int, seg string",
    )
    ov = ms.compute_dataset_overview(
        df, two_column_entity_params, segment_columns=["seg"]
    )
    # entity＝(b1,c1)、(b1,NULL)、(NULL,NULL)＝3；兩列 (b1,NULL) 只算一個。
    # query 總數＝(0331,b1,c1)、(0331,b1,NULL)、(0331,NULL,NULL)、(0229,b1,c1)＝4
    _assert_same_json(ov, {
        "totals": {
            "n_rows": 5,
            "n_entities": 3,
            "n_items": 2,
            "n_snap_dates": 2,
            "n_positives": 2,
            "positive_rate": 2 / 5,
            "avg_positives_per_entity": 2 / 3,
        },
        "by_snap_date": {
            "20240331": {"n_rows": 4, "n_positives": 2, "n_entities": 1,
                         "positive_rate": 2 / 4},
            "20240229": {"n_rows": 1, "n_positives": 0, "n_entities": 1,
                         "positive_rate": 0 / 1},
        },
        "by_item": {
            "p1": {"n_rows": 3, "n_positives": 1, "n_entities": 1,
                   "positive_rate": 1 / 3},
            "p2": {"n_rows": 2, "n_positives": 1, "n_entities": 1,
                   "positive_rate": 1 / 2},
        },
        "by_segment": {
            "X": {"n_rows": 4, "n_positives": 2, "n_entities": 1,
                  "positive_rate": 2 / 4, "n_queries": 2, "query_share": 2 / 4},
            "Y": {"n_rows": 1, "n_positives": 0, "n_entities": 0,
                  "positive_rate": 0 / 1, "n_queries": 0, "query_share": 0 / 4},
        },
    })


# ---------------------------------------------------------------------------
# `event` declared: the tied-row share, and what "all" resolves to (#378)
# ---------------------------------------------------------------------------


def _event_params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank",
        "event": "imp_id"}},
        "evaluation": {}}


def _event_df(spark):
    """One query group (c1 @ 20240331) holding A three times and B once.

    Two of the three A rows score the same — that pair, and only it, has its
    rank decided by the tie-break rather than by the score.
    """
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", "i1", 0.9, 1),
            ("20240331", "c1", "A", "i2", 0.9, 0),
            ("20240331", "c1", "A", "i3", 0.4, 0),
            ("20240331", "c1", "B", "i4", 0.1, 0),
            ("20240331", "c2", "A", "i5", 0.2, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"],
    )


def test_tied_row_share_is_absent_without_the_event_role(spark):
    """The compatibility half: the artifact must stay value-for-value what it
    was for every deployment that declares no optional role."""
    totals = ms.compute_dataset_overview(_df(spark), _params())["totals"]
    assert "n_tied_rows" not in totals
    assert "tied_row_share" not in totals


def test_tied_row_share_counts_rows_whose_rank_the_tiebreak_decided(spark):
    totals = ms.compute_dataset_overview(_event_df(spark), _event_params())["totals"]
    # The two 0.9 rows of (c1, A). The 0.4 and 0.1 rows are alone at their
    # score, and c2's single row is alone in its query group.
    assert totals["n_tied_rows"] == 2
    assert totals["tied_row_share"] == pytest.approx(2 / 5)


def test_tied_rows_are_counted_within_a_query_group_not_across(spark):
    """c1 and c2 both hold a row scoring 0.2 — across the whole frame that is a
    tie, but they are in different query groups and never compete, so neither
    row's rank was decided by the tie-break."""
    df = spark.createDataFrame(
        [
            ("20240331", "c1", "A", "i1", 0.2, 1),
            ("20240331", "c2", "A", "i2", 0.2, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"],
    )
    totals = ms.compute_dataset_overview(df, _event_params())["totals"]
    assert totals["n_tied_rows"] == 0


def test_all_resolves_to_the_distinct_item_count_without_the_event_role(spark):
    """Unchanged from before #378 — this is the number every existing
    deployment's ``k_values: "all"`` already resolves to."""
    assert ms._resolve_all_k(
        _df(spark), ["snap_date", "cust_id"], "prod_name", event_cols=[],
    ) == 2


def test_all_resolves_to_the_widest_query_group_with_the_event_role(spark):
    """With `event` declared a query group can hold more rows than there are
    items, and the item count would truncate the longest ranking — which is
    exactly what "all" says it does not do."""
    # 2 distinct items, but c1's group holds 4 rows.
    assert ms._resolve_all_k(
        _event_df(spark), ["snap_date", "cust_id"], "prod_name",
        event_cols=["imp_id"],
    ) == 4


# ---------------------------------------------------------------------------
# `occasion` declared (#428): the same two numbers, scoped to one occasion
# ---------------------------------------------------------------------------


def _occasion_params():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank",
        "occasion": "req_id"}},
        "evaluation": {}}


def _occasion_df(spark):
    """c1 @ 20240331 was shown A/B/C in request r1 and A alone in r2; c2 was
    shown D alone in r3.

    Items are unique within each request (what the duplicate checks guarantee
    when only ``occasion`` is declared). Four distinct items, widest occasion
    three rows — so the two candidate answers for ``"all"`` differ.
    """
    return spark.createDataFrame(
        [
            ("20240331", "c1", "r1", "A", 0.9, 1),
            ("20240331", "c1", "r1", "B", 0.5, 0),
            ("20240331", "c1", "r1", "C", 0.5, 0),
            ("20240331", "c1", "r2", "A", 0.5, 0),
            ("20240331", "c2", "r3", "D", 0.3, 0),
        ],
        schema=["snap_date", "cust_id", "req_id", "prod_name", "score", "label"],
    )


def test_all_stays_the_item_count_with_only_the_occasion_role(spark):
    """Four distinct items; no occasion holds more than three rows, and none
    can hold more than four — items are unique within an occasion. So the
    item count truncates nothing, and it is the number the report looks
    ``"all"`` up by. Resolving to the widest occasion (3) would store
    ``map@3`` where the report asks for ``map@4`` — the blank ``map@all``
    cell ``event`` already produces (#428)."""
    schema = get_schema(_occasion_params())
    assert ms._resolve_all_k(
        _occasion_df(spark), schema["query_group_columns"], "prod_name",
        event_cols=schema.get("event", []),
    ) == 4


def test_the_report_finds_map_at_all_with_the_occasion_role(spark):
    """End to end over the lookup contract: metrics keyed at the K the report
    will ask for, and nothing new written to ask it with."""
    params = _occasion_params()
    params["evaluation"] = {"k_values": [1, "all"]}
    out = ms.compute_all_metrics(_occasion_df(spark), params)
    assert ALL_K_KEY not in out
    assert f"map@{resolved_all_k(out)}" in out["overall"]


def test_tied_row_share_is_reported_with_the_occasion_role(spark):
    """B and C tie at 0.5 inside r1. r2's A also scores 0.5 but is alone in
    its occasion — under ``time`` + ``entity`` alone the three 0.5 rows would
    all have counted as tied."""
    totals = ms.compute_dataset_overview(
        _occasion_df(spark), _occasion_params()
    )["totals"]
    assert totals["n_tied_rows"] == 2
    assert totals["tied_row_share"] == pytest.approx(2 / 5)


def test_evaluated_queries_are_the_occasions_with_a_positive(spark):
    """Three occasions (r1, r2, r3); only r1 holds a click. So three queries,
    two excluded, one evaluated — under ``time`` + ``entity`` it would be two
    queries, c1's pooling r1 with r2."""
    out = ms.compute_all_metrics(_occasion_df(spark), _occasion_params())
    assert out["n_queries"] == 3
    assert out["n_queries"] - out["n_excluded_queries"] == 1


# ---------------------------------------------------------------------------
# One source for what "all" resolved to (#434)
# ---------------------------------------------------------------------------


def _event_params_with_categories():
    """``_event_params`` with k_values ``[1, "all"]`` and A, B folded into one
    category; C stays its own (``unmapped: singleton``)."""
    params = _event_params()
    params["schema"]["categorical_values"] = {"prod_name": ["A", "B", "C"]}
    params["evaluation"] = {
        "k_values": [1, "all"],
        "item_categories": {
            "enabled": True, "unmapped": "singleton",
            "mapping": {"AB": ["A", "B"]},
        },
    }
    return params


def _wide_event_df(spark):
    """Three items, and one query group four rows long.

    c1 was shown A three times and B once, and clicked the B at the bottom
    (rank 4); c3 was shown C once and clicked it. c2 has no click. So the two
    candidate answers for ``"all"`` differ — 3 items against a 4-row group —
    and so do the metrics at them:

    * ``map@4`` = mean(AP c1 = 1/4, AP c3 = 1) = 0.625; ``map@3`` = 0.5
    * ``recall@4`` = 1.0; ``recall@3`` = 0.5

    At category grain c1 collapses to one AB row, so the widest collapsed
    group is 1 row against 2 categories (AB, C).
    """
    return spark.createDataFrame(
        [
            ("20240331", "c1", "A", "i1", 0.9, 0),
            ("20240331", "c1", "A", "i2", 0.9, 0),
            ("20240331", "c1", "A", "i3", 0.4, 0),
            ("20240331", "c1", "B", "i4", 0.1, 1),
            ("20240331", "c2", "A", "i5", 0.2, 0),
            ("20240331", "c3", "C", "i6", 0.7, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score", "label"],
    )


def _table(section, title_part):
    return next(t for t, tt in zip(section.tables, section.table_titles)
                if title_part in tt)


def test_the_report_finds_map_and_recall_at_all_with_the_event_role(spark):
    """The bug: metrics stored at the widest group (``map@4``), the report
    asked for the item count (``map@3``), and the ``@all`` cell was blank with
    nothing raised. Expected values are worked out by hand in
    ``_wide_event_df``."""
    params = _event_params_with_categories()
    out = ms.compute_all_metrics(_wide_event_df(spark), params)

    overall = _table(rb.build_metrics_section(out, params), "overall（")
    assert overall.loc["map", "@all"] == pytest.approx(0.625)
    assert overall.loc["recall", "@all"] == pytest.approx(1.0)

    card = rb.build_overview_section(out, params).tables[0]
    assert card.loc["map@all", "value"] == pytest.approx(0.625)


def test_all_k_is_written_only_when_the_frame_holds_event_rows(spark):
    """Written for the fine-grained pass, where the widest group can outgrow
    the item list. Not for the category pass — collapsing leaves one row per
    (query group, category), so no event rows remain — and not at all without
    the role, which keeps every existing artifact value-for-value."""
    out = ms.compute_all_metrics(
        _wide_event_df(spark), _event_params_with_categories())
    assert out[ALL_K_KEY] == 4
    assert ALL_K_KEY not in out["category"]

    undeclared = ms.compute_all_metrics(_df(spark), _params())
    assert ALL_K_KEY not in undeclared


def test_category_all_is_the_category_count_with_the_event_role(spark):
    """The category frame has no event rows, so ``"all"`` resolves there the
    way it does undeclared: the category count (2), which is what
    ``docs/pipelines/evaluation.md`` says. It used to follow the declared role
    to the widest collapsed group (1), store ``map@1`` only, and blank the
    report's category ``@all`` cell.

    ``precision@all`` is what tells the two apart — ``map`` and ``recall``
    come out the same at either K, since neither truncates. Each clicked group
    holds one positive over K = 2, so 0.5; at K = 1 it would be 1.0."""
    params = _event_params_with_categories()
    out = ms.compute_all_metrics(_wide_event_df(spark), params)

    cat = _table(rb.build_metrics_section(out, params), "大類 overall")
    assert cat.loc["map", "@all"] == pytest.approx(1.0)
    assert cat.loc["precision", "@all"] == pytest.approx(0.5)
