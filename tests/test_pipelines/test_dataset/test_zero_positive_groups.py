"""How many query groups holding no positive each split keeps (ADR-0025 decision 3).

One rule for every split: a group holding a positive is always kept whole; a
group holding none is kept or dropped whole, and the share kept is the split's
``dataset.*_zero_positive_group_ratio``. Every split applies it to its keys,
before the build, reading the label from ``label_table`` — so B10 can pin each
model_input's row count to the keys it was built from (ADR-0029 decision 4).
val / test's keys also carry the design weight above 0; train's never do.

The frames here are built by hand rather than from the module fixtures in
``test_nodes.py``: the draw needs hundreds of groups for "close to r" to mean
anything, and those fixtures hold 24 entities.
"""

import logging

import pandas as pd
import pytest

from recsys_tfb.core.consistency import ZERO_POSITIVE_GROUP_WEIGHT_COL as WEIGHT
from recsys_tfb.pipelines.dataset.month_plans import SnapDatePlan
from recsys_tfb.pipelines.dataset.nodes import (
    filter_test_keys,
    filter_train_keys,
    filter_val_keys,
)

pytestmark = pytest.mark.spark

_ITEMS = ["a", "b", "c"]
_DATE = "2024-04-30"
_N_POSITIVE_GROUPS = 40
_N_ZERO_GROUPS = 400
#: The test month plan every frame here falls in.
_TEST_PLAN = SnapDatePlan(to_process=[pd.Timestamp(_DATE)], skipped=[])


def _params(**dataset) -> dict:
    return {
        "random_seed": 42,
        "schema": {
            "columns": {"time": "snap_date", "entity": ["cust_id"], "item": "prod_name"},
            "categorical_values": {"prod_name": list(_ITEMS)},
        },
        # Every frame here is one month; each split reads label_table for its
        # own months only (ADR-0029 decision 1).
        "dataset": {
            "train_snap_dates": [_DATE], "val_snap_dates": [_DATE], **dataset,
        },
    }


def _positive_ids() -> list[str]:
    return [f"P{i:03d}" for i in range(_N_POSITIVE_GROUPS)]


def _zero_ids() -> list[str]:
    return [f"Z{i:03d}" for i in range(_N_ZERO_GROUPS)]


def _labelled_rows() -> pd.DataFrame:
    """One row per candidate: every ``P`` group holds exactly one positive, no
    ``Z`` group holds any."""
    rows = []
    for cid in _positive_ids() + _zero_ids():
        for item in _ITEMS:
            rows.append({
                "snap_date": pd.Timestamp(_DATE),
                "cust_id": cid,
                "prod_name": item,
                "label": int(cid.startswith("P") and item == _ITEMS[0]),
            })
    return pd.DataFrame(rows)


@pytest.fixture
def keys(spark):
    """train keys: identity plus a carried column, and no label at all."""
    pdf = _labelled_rows()[["snap_date", "cust_id", "prod_name"]].copy()
    pdf["seg"] = "mass"
    return spark.createDataFrame(pdf)


@pytest.fixture
def eval_keys(spark):
    """val / test keys: identity only, as ``select_{val,test}_keys`` give them."""
    return spark.createDataFrame(
        _labelled_rows()[["snap_date", "cust_id", "prod_name"]])


@pytest.fixture
def label_table(spark):
    """The labels, with a column nothing downstream reads (like the real table)."""
    pdf = _labelled_rows()[["snap_date", "cust_id", "prod_name", "label"]].copy()
    pdf["apply_end_date"] = pd.Timestamp("2024-05-30")
    return spark.createDataFrame(pdf)


def _rows_per_group(df) -> dict[str, int]:
    pdf = df.groupBy("cust_id").count().toPandas()
    return dict(zip(pdf["cust_id"], pdf["count"]))


def _kept_zero_groups(df) -> set[str]:
    return {c for c in _rows_per_group(df) if c.startswith("Z")}


def _sorted_rows(df, cols):
    return sorted(tuple(r) for r in df.select(*cols).collect())


#: val and test run one set of decisions under two ratio keys; every case below
#: runs against both node functions. ``run(keys, label_table, params)``.
_EVAL_SPLITS = {
    "val": lambda k, lt, p: filter_val_keys(k, lt, p),
    "test": lambda k, lt, p: filter_test_keys(k, lt, _TEST_PLAN, p),
}


@pytest.fixture(params=sorted(_EVAL_SPLITS))
def split(request):
    return request.param


def _run(split, keys, label_table, **ratio):
    """``split``'s filter with ``ratio`` given as ``r=...`` under its own key."""
    dataset = {
        f"{split}_zero_positive_group_ratio": value for value in ratio.values()
    }
    return _EVAL_SPLITS[split](keys, label_table, _params(**dataset))


# --- val / test --------------------------------------------------------------


class TestValTestDefaultIsTodaysFilter:
    def test_absent_key_drops_every_zero_positive_group_and_adds_no_column(
        self, split, eval_keys, label_table
    ):
        out = _run(split, eval_keys, label_table)
        assert out.columns == eval_keys.columns
        per_group = _rows_per_group(out)
        assert set(per_group) == set(_positive_ids())
        # Whole groups: the negatives of a positive group stay.
        assert set(per_group.values()) == {len(_ITEMS)}

    def test_explicit_zero_gives_the_same_rows_as_the_default(
        self, split, eval_keys, label_table
    ):
        default = _run(split, eval_keys, label_table)
        explicit = _run(split, eval_keys, label_table, r=0.0)
        assert explicit.columns == default.columns
        assert sorted(explicit.collect()) == sorted(default.collect())


class TestValTestRatioOne:
    def test_keeps_every_key_and_weights_every_row_one(
        self, split, eval_keys, label_table
    ):
        out = _run(split, eval_keys, label_table, r=1.0)
        assert out.columns == eval_keys.columns + [WEIGHT]
        assert _sorted_rows(out, eval_keys.columns) == _sorted_rows(
            eval_keys, eval_keys.columns)
        assert {r[WEIGHT] for r in out.select(WEIGHT).distinct().collect()} == {1.0}

    def test_ratio_one_never_reads_label_table(self, split, eval_keys):
        # Structural: at ratio 1 every group is kept whatever its labels say,
        # so a path that joined them anyway would return the same rows. What
        # it could not do is run without a label table.
        out = _run(split, eval_keys, None, r=1.0)
        assert out.count() == eval_keys.count()


class TestValTestPartialRatio:
    RATIO = 0.3

    @pytest.fixture
    def drawn(self, split, eval_keys, label_table):
        return _run(split, eval_keys, label_table, r=self.RATIO)

    def test_every_positive_group_survives_whole(self, drawn):
        per_group = _rows_per_group(drawn)
        for cid in _positive_ids():
            assert per_group.get(cid) == len(_ITEMS), cid

    def test_zero_positive_groups_are_kept_or_dropped_whole(self, drawn):
        per_group = _rows_per_group(drawn)
        kept = _kept_zero_groups(drawn)
        assert kept, "the draw kept nothing — the whole-group check is vacuous"
        assert {per_group[c] for c in kept} == {len(_ITEMS)}

    def test_the_share_kept_is_close_to_the_ratio(self, drawn):
        share = len(_kept_zero_groups(drawn)) / _N_ZERO_GROUPS
        # 400 Bernoulli(0.3) draws: sd ~0.023, so +-0.08 is ~3.5 sd.
        assert abs(share - self.RATIO) < 0.08, share

    def test_weights_are_one_for_positive_groups_and_inverse_ratio_otherwise(
        self, drawn
    ):
        pdf = drawn.select("cust_id", WEIGHT).toPandas()
        positive = pdf[pdf["cust_id"].str.startswith("P")][WEIGHT]
        zero = pdf[pdf["cust_id"].str.startswith("Z")][WEIGHT]
        assert set(positive) == {1.0}
        assert len(zero) > 0
        assert list(zero) == pytest.approx([1 / self.RATIO] * len(zero))

    def test_the_same_seed_draws_the_same_groups(
        self, split, eval_keys, label_table, drawn
    ):
        again = _run(split, eval_keys, label_table, r=self.RATIO)
        assert _kept_zero_groups(again) == _kept_zero_groups(drawn)

    def test_another_seed_draws_other_groups(
        self, split, eval_keys, label_table, drawn
    ):
        # The other half of determinism: the seed is actually an input.
        params = _params(**{f"{split}_zero_positive_group_ratio": self.RATIO})
        params["random_seed"] = 7
        again = _EVAL_SPLITS[split](eval_keys, label_table, params)
        assert _kept_zero_groups(again) != _kept_zero_groups(drawn)

    def test_the_keys_keep_their_own_columns_and_gain_only_the_weight(
        self, eval_keys, drawn
    ):
        assert drawn.columns == eval_keys.columns + [WEIGHT]


class TestEachSplitReadsItsOwnKey:
    def test_the_test_node_reads_the_test_key(self, eval_keys, label_table):
        params = _params(
            val_zero_positive_group_ratio=0.0, test_zero_positive_group_ratio=0.5)
        assert _kept_zero_groups(
            filter_test_keys(eval_keys, label_table, _TEST_PLAN, params))
        val = filter_val_keys(eval_keys, label_table, params)
        assert not _kept_zero_groups(val)
        assert WEIGHT not in val.columns

    def test_the_val_node_reads_the_val_key(self, eval_keys, label_table):
        params = _params(
            val_zero_positive_group_ratio=0.5, test_zero_positive_group_ratio=0.0)
        assert _kept_zero_groups(filter_val_keys(eval_keys, label_table, params))
        test = filter_test_keys(eval_keys, label_table, _TEST_PLAN, params)
        assert not _kept_zero_groups(test)
        assert WEIGHT not in test.columns


class TestTheLabelIsReadForTheSplitsMonthsOnly:
    """The month list is the split's own: val's from ``val_snap_dates``,
    test's from its plan — not the other's, and not train's."""

    def test_a_month_outside_the_list_judges_no_group(self, spark, split):
        # Keys and labels of a month the split does not list: had the node
        # judged them anyway (another split's list), P groups would survive.
        # Judged under the split's own list, the label join finds nothing
        # and every group is zero-positive, so ratio 0 keeps none.
        other = "2024-05-31"
        pdf = _labelled_rows()
        pdf["snap_date"] = pd.Timestamp(other)
        keys = spark.createDataFrame(pdf[["snap_date", "cust_id", "prod_name"]])
        labels = spark.createDataFrame(pdf)
        params = _params(train_snap_dates=[other])
        out = _EVAL_SPLITS[split](keys, labels, params)
        assert out.count() == 0


def test_a_weight_column_already_on_the_keys_is_refused_not_overwritten(
    eval_keys, label_table,
):
    from pyspark.sql import functions as F

    clashing = eval_keys.withColumn(WEIGHT, F.lit(9.0))
    for ratio in (0.5, 1.0):
        with pytest.raises(ValueError, match="would overwrite"):
            filter_val_keys(
                clashing, label_table, _params(val_zero_positive_group_ratio=ratio))


class TestValTestKeysAreNeverRepeated:
    def test_a_duplicated_label_row_never_duplicates_a_key(
        self, split, eval_keys, label_table
    ):
        # A label_table holding one key twice is B10's business, and B10 can
        # only see it if this node hands the build the keys it was given —
        # never a fanned-out copy of them. The weighted path joins a column
        # back onto the keys, so it is the one that could fan out.
        duplicated = label_table.unionByName(label_table.limit(1))
        out = _run(split, eval_keys, duplicated, r=0.5)
        reference = _run(split, eval_keys, label_table, r=0.5)
        assert out.count() == reference.count()
        assert out.count() == out.distinct().count()

    def test_a_group_with_a_null_key_column_is_drawn_like_any_other(
        self, spark, split
    ):
        """A query group whose key holds a NULL is still one group to draw.
        Its label never joins (NULL matches nothing), so it is a zero-positive
        group; what must not happen is that the join carrying the weight back
        drops it only because NULL matches nothing.

        A ratio just under 1 keeps a group unless its bucket is the very last
        one, so under this fixed seed every group survives.
        """
        rows = [
            (pd.Timestamp(_DATE), None, "a"),
            (pd.Timestamp(_DATE), None, "b"),
            (pd.Timestamp(_DATE), "Z", "a"),
        ]
        keys = spark.createDataFrame(pd.DataFrame(
            rows, columns=["snap_date", "cust_id", "prod_name"]))
        labels = spark.createDataFrame(pd.DataFrame(
            [(pd.Timestamp(_DATE), "Z", "a", 0)],
            columns=["snap_date", "cust_id", "prod_name", "label"],
        ))
        out = _run(split, keys, labels, r=0.99999)
        assert out.count() == 3
        assert out.filter("cust_id IS NULL").count() == 2


class TestValTestLogging:
    def test_a_partial_ratio_logs_the_ratio_and_the_kept_zero_positive_groups(
        self, split, eval_keys, label_table, caplog
    ):
        with caplog.at_level(logging.INFO):
            out = _run(split, eval_keys, label_table, r=0.3)
        kept = len(_kept_zero_groups(out))
        lines = [r.getMessage() for r in caplog.records
                 if "zero-positive" in r.getMessage()]
        assert len(lines) == 1, lines
        assert lines[0].startswith(split) and "r=0.3" in lines[0]
        assert f"kept {kept} of {_N_ZERO_GROUPS}" in lines[0]

    def test_ratio_zero_logs_that_none_is_kept(
        self, split, eval_keys, label_table, caplog
    ):
        with caplog.at_level(logging.INFO):
            _run(split, eval_keys, label_table)
        lines = [r.getMessage() for r in caplog.records
                 if "zero-positive" in r.getMessage()]
        assert len(lines) == 1, lines
        assert lines[0].startswith(split) and "r=0" in lines[0]


# --- train / train_dev ---------------------------------------------------------


class TestTrainKeys:
    def test_absent_key_returns_the_keys_untouched(self, keys, label_table):
        out = filter_train_keys(keys, label_table, _params())
        assert out.columns == keys.columns
        assert _sorted_rows(out, keys.columns) == _sorted_rows(keys, keys.columns)

    def test_the_default_never_reads_label_table(self, keys):
        # Structural, not numerical: at ratio 1 the draw keeps everything, so
        # a path that joined the labels anyway would return the same rows and
        # a row comparison could not tell. What it could not do is run without
        # a label table — and every existing train variant takes this path.
        out = filter_train_keys(keys, None, _params())
        assert out is keys

    def test_ratio_zero_drops_the_groups_label_table_says_hold_no_positive(
        self, keys, label_table
    ):
        out = filter_train_keys(
            keys, label_table, _params(train_zero_positive_group_ratio=0.0))
        per_group = _rows_per_group(out)
        assert set(per_group) == set(_positive_ids())
        assert set(per_group.values()) == {len(_ITEMS)}
        # The keys keep their own columns — the carried one included — and gain
        # neither the label nor a weight: train carries no weight (ADR-0025).
        assert out.columns == keys.columns

    def test_partial_ratio_keeps_whole_groups_close_to_r_with_no_weight(
        self, keys, label_table
    ):
        ratio = 0.3
        out = filter_train_keys(
            keys, label_table, _params(train_zero_positive_group_ratio=ratio))
        per_group = _rows_per_group(out)
        kept = _kept_zero_groups(out)
        assert all(per_group.get(c) == len(_ITEMS) for c in _positive_ids())
        assert kept and {per_group[c] for c in kept} == {len(_ITEMS)}
        assert abs(len(kept) / _N_ZERO_GROUPS - ratio) < 0.08
        assert WEIGHT not in out.columns

    def test_it_draws_the_same_groups_as_val_and_test(
        self, keys, eval_keys, label_table
    ):
        # One mechanism and one hash site for every split: the same seed and
        # ratio over the same keys pick the same groups, whichever split's
        # node ran the draw.
        ratio = 0.3
        train = filter_train_keys(
            keys, label_table, _params(train_zero_positive_group_ratio=ratio))
        val = filter_val_keys(
            eval_keys, label_table, _params(val_zero_positive_group_ratio=ratio))
        test = filter_test_keys(
            eval_keys, label_table, _TEST_PLAN,
            _params(test_zero_positive_group_ratio=ratio))
        assert _kept_zero_groups(train) == _kept_zero_groups(val)
        assert _kept_zero_groups(train) == _kept_zero_groups(test)

    def test_a_duplicated_label_row_never_duplicates_a_key(
        self, spark, keys, label_table
    ):
        # A label_table holding one key twice is B10's business, and B10 can
        # only see it if this node hands build_model_input the keys it was
        # given — never a fanned-out copy of them.
        duplicated = label_table.unionByName(label_table.limit(1))
        out = filter_train_keys(
            keys, duplicated, _params(train_zero_positive_group_ratio=0.0))
        assert out.count() == _N_POSITIVE_GROUPS * len(_ITEMS)
        assert out.count() == out.distinct().count()

    def test_a_group_with_a_null_key_column_is_drawn_like_any_other(self, spark):
        """A query group whose key holds a NULL (an entity column the split
        does not require, or an optional role) is still one group to draw.
        Its label never joins (NULL matches nothing), so it is always a
        zero-positive group; what must not happen is that the train side
        drops it for a reason the draw never decided, only because a join on
        NULL matches nothing.

        A ratio just under 1 keeps a group unless its bucket is the very last
        one, so under this fixed seed every group survives.
        """
        rows = [
            (pd.Timestamp(_DATE), None, "a", "x"),
            (pd.Timestamp(_DATE), None, "b", "x"),
            (pd.Timestamp(_DATE), "Z", "a", "x"),
        ]
        keys = spark.createDataFrame(pd.DataFrame(
            rows, columns=["snap_date", "cust_id", "prod_name", "seg"]))
        labels = spark.createDataFrame(pd.DataFrame(
            [(pd.Timestamp(_DATE), "Z", "a", 0)],
            columns=["snap_date", "cust_id", "prod_name", "label"],
        ))
        out = filter_train_keys(
            keys, labels, _params(train_zero_positive_group_ratio=0.99999))
        assert out.count() == 3
        assert out.filter("cust_id IS NULL").count() == 2

    def test_the_val_and_test_keys_do_not_reach_train(self, keys, label_table):
        params = _params(
            val_zero_positive_group_ratio=0.0, test_zero_positive_group_ratio=0.0)
        out = filter_train_keys(keys, label_table, params)
        assert out.count() == keys.count()

    def test_a_partial_ratio_logs_the_kept_zero_positive_groups(
        self, keys, label_table, caplog
    ):
        with caplog.at_level(logging.INFO):
            out = filter_train_keys(
                keys, label_table, _params(train_zero_positive_group_ratio=0.3))
        kept = len(_kept_zero_groups(out))
        lines = [r.getMessage() for r in caplog.records
                 if "zero-positive" in r.getMessage()]
        assert len(lines) == 1, lines
        assert "train" in lines[0] and "r=0.3" in lines[0]
        assert f"kept {kept} of {_N_ZERO_GROUPS}" in lines[0]


# --- B12's runtime backstop ------------------------------------------------------


class TestAFeatureNamedLikeTheWeightStopsTheBuild:
    """B12 checks feature names against the weight at the start of the run; a
    sliced run skips that gate. Since the weight rides on the keys (ADR-0029
    decision 4), what meets it is the build: the feature arrives by the same
    name and the output select cannot tell the two apart. Loud, like B7's
    carry / feature collision, and never an overwrite."""

    def _build(self, spark, keys, label_table, ratio):
        from recsys_tfb.pipelines.dataset.nodes import build_model_input

        features = spark.createDataFrame(pd.DataFrame({
            "snap_date": [pd.Timestamp(_DATE)],
            "cust_id": ["P000"],
            WEIGHT: [3.0],
        }))
        preprocessor = {
            "feature_columns": [WEIGHT],
            "categorical_columns": [],
            "category_mappings": {},
        }
        params = _params(val_zero_positive_group_ratio=ratio)
        return build_model_input(
            filter_val_keys(keys, label_table, params), features, label_table,
            preprocessor, params, months=[pd.Timestamp(_DATE)],
        )

    def test_with_the_weight_on_the_keys_the_build_raises(
        self, spark, eval_keys, label_table
    ):
        from pyspark.sql.utils import AnalysisException

        with pytest.raises(AnalysisException, match="ambiguous"):
            self._build(spark, eval_keys, label_table, 0.5)

    def test_without_the_weight_the_same_feature_builds(
        self, spark, eval_keys, label_table
    ):
        # The control: the raise above is the weight's doing, not the
        # fixture's.
        out = self._build(spark, eval_keys, label_table, 0.0)
        assert WEIGHT in out.columns
