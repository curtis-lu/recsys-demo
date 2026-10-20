"""Tests for scripts/profile_sample_pool.py (#525).

The profile must count the population the dataset pipeline draws from:
``select_sample_keys``' months (``months_filter_as_date``) and combined item,
minus the NULL-entity rows ``split_train_keys`` drops. Each test below is one
of those, stated against a frame small enough to count by eye.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
import yaml
from typer.testing import CliRunner

from scripts.profile_sample_pool import PROFILE_FORMAT_VERSION, app, build_profile

pytestmark = pytest.mark.spark

TRAIN_MONTHS = ["2025-01-31", "2025-02-28"]


def _params(**overrides):
    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label"}},
        "dataset": {"sample_group_keys": ["seg", "prod_name", "label"],
                    "train_snap_dates": TRAIN_MONTHS},
        "training": {"sample_weight_keys": ["seg", "prod_name", "label"]},
    }
    for section, values in overrides.items():
        params[section] = {**params[section], **values}
    return params


# Two train months plus one outside them; seg_1 x prod_A is the busy cell.
#   2025-01-31  c1 prod_A seg_1 1 | c2 prod_A seg_1 0 | c3 prod_A seg_1 0 | c4 prod_B seg_2 1
#   2025-02-28  c1 prod_A seg_1 0 | c5 prod_B seg_2 0
#   2025-03-31  c1 prod_A seg_1 1                     (not a train month)
ROWS = [
    ("2025-01-31", "c1", "prod_A", "seg_1", 1),
    ("2025-01-31", "c2", "prod_A", "seg_1", 0),
    ("2025-01-31", "c3", "prod_A", "seg_1", 0),
    ("2025-01-31", "c4", "prod_B", "seg_2", 1),
    ("2025-02-28", "c1", "prod_A", "seg_1", 0),
    ("2025-02-28", "c5", "prod_B", "seg_2", 0),
    ("2025-03-31", "c1", "prod_A", "seg_1", 1),
]
EXPECTED_CELLS = [
    {"values": {"seg": "seg_1", "prod_name": "prod_A"}, "n_pos": 1, "n_neg": 3},
    {"values": {"seg": "seg_2", "prod_name": "prod_B"}, "n_pos": 1, "n_neg": 1},
]


def _frame(spark, rows=ROWS, time_type="date", extra=None):
    # An explicit schema, not pandas: pandas turns a None label into NaN and
    # the whole column into floats, which is a different input.
    extra = extra or {}
    columns = "snap_date string, cust_id string, prod_name string, seg string, label int"
    columns += "".join(f", {name} string" for name in extra)
    data = [(*row, *(values[i] for values in extra.values())) for i, row in enumerate(rows)]
    df = spark.createDataFrame(data, schema=columns)
    if time_type == "date":
        return df.withColumn("snap_date", df.snap_date.cast("date"))
    if time_type == "timestamp":
        return df.withColumn("snap_date", df.snap_date.cast("timestamp"))
    return df          # string, as a Hive table partitioned on snap_date reads


class TestPopulation:
    @pytest.mark.parametrize("time_type", ["string", "date", "timestamp"])
    def test_counts_the_train_months_whatever_the_time_columns_type(self, spark, time_type):
        # The editor's `isin([Timestamp])` matched no row of a string column
        # and said nothing; the pipeline compares as DATE.
        profile = build_profile(_frame(spark, time_type=time_type), _params(), "pool")
        assert profile["cells"] == EXPECTED_CELLS

    def test_rows_with_a_null_entity_are_not_counted(self, spark):
        rows = ROWS + [("2025-01-31", None, "prod_A", "seg_1", 1)] * 2
        profile = build_profile(_frame(spark, rows), _params(), "pool")
        assert profile["cells"] == EXPECTED_CELLS
        assert profile["dropped_null_entity_rows"] == 2

    def test_a_multi_column_item_is_combined_first(self, spark):
        rows = [(d, c, item.split("-")[0], seg, y) for d, c, item, seg, y in [
            ("2025-01-31", "c1", "c01-banner", "seg_1", 1),
            ("2025-01-31", "c2", "c01-video", "seg_1", 0),
        ]]
        df = _frame(spark, rows, extra={"fmt": ["banner", "video"]})
        df = df.withColumnRenamed("prod_name", "campaign")
        params = _params(
            schema={"columns": {"time": "snap_date", "entity": ["cust_id"],
                                "item": ["campaign", "fmt"], "label": "label"}},
            dataset={"sample_group_keys": ["seg", "item", "label"]},
            training={"sample_weight_keys": []},
        )
        profile = build_profile(df, params, "pool")
        assert profile["item_column"] == "item"
        assert profile["cells"] == [
            {"values": {"seg": "seg_1", "item": "c01-banner"}, "n_pos": 1, "n_neg": 0},
            {"values": {"seg": "seg_1", "item": "c01-video"}, "n_pos": 0, "n_neg": 1},
        ]


class TestKeysMatchThePipelines:
    @pytest.mark.parametrize("time_type", ["string", "date", "timestamp"])
    def test_planner_keys_from_profile_values_are_the_draws_own_keys(self, spark, time_type):
        # The draw joins overrides on its own `_gk` (every key part cast to
        # string in Spark). A profile value recorded any other way — a BOOLEAN
        # as Python prints it ("True", not "true"), say — builds a key that
        # never matches. Read back through JSON, as the planner reads it.
        from pyspark.sql import functions as F

        from recsys_tfb.pipelines.dataset.steps.sampling import with_effective_sample_ratio
        from recsys_tfb.pipelines.dataset.steps.scoping import months_filter_as_date
        from scripts.sampling_budget_planner import cell_key

        group_keys = ["snap_date", "is_vip", "prod_name", "label"]
        df = _frame(spark, time_type=time_type).withColumn("is_vip", F.col("cust_id") == "c1")
        profile = json.loads(json.dumps(build_profile(
            df, _params(dataset={"sample_group_keys": group_keys},
                        training={"sample_weight_keys": []}), "pool")))
        planner_keys = {
            cell_key(tuple(c["values"].values()), group_keys, "label", label)
            for c in profile["cells"]
            for label, n in (("1", c["n_pos"]), ("0", c["n_neg"])) if n
        }
        drawn = with_effective_sample_ratio(
            df.filter(months_filter_as_date("snap_date", TRAIN_MONTHS)),
            group_keys, 1.0, {"unused": 0.5})
        pipeline_keys = {r["_gk"] for r in drawn.select("_gk").distinct().collect()}
        assert planner_keys == pipeline_keys
        assert len(planner_keys) == 5


class TestWhatTheFileRecords:
    def test_the_conditions_it_was_made_under(self, spark):
        profile = build_profile(_frame(spark), _params(), "db.sample_pool")
        assert profile["format_version"] == PROFILE_FORMAT_VERSION
        assert profile["source"] == "db.sample_pool"
        assert profile["granularity"] == ["seg", "prod_name"]
        assert (profile["label_column"], profile["item_column"], profile["time_column"]) == \
            ("label", "prod_name", "snap_date")
        assert profile["train_snap_dates"] == TRAIN_MONTHS
        dt.datetime.fromisoformat(profile["generated_at"])

    def test_granularity_adds_weight_keys_after_the_sampling_keys(self, spark):
        df = _frame(spark, extra={"channel": ["web"] * len(ROWS)})
        profile = build_profile(df, _params(training={
            "sample_weight_keys": ["channel", "prod_name", "label"]}), "pool")
        assert profile["granularity"] == ["seg", "prod_name", "channel"]
        assert profile["cells"][0]["values"] == {
            "seg": "seg_1", "prod_name": "prod_A", "channel": "web"}

    def test_a_weight_key_sample_pool_lacks_is_skipped_and_recorded(self, spark):
        # e.g. a categorical only the feature table has
        profile = build_profile(_frame(spark), _params(training={
            "sample_weight_keys": ["seg", "prod_name", "label", "risk_band"]}), "pool")
        assert profile["skipped_weight_columns"] == ["risk_band"]
        assert profile["granularity"] == ["seg", "prod_name"]
        assert profile["cells"] == EXPECTED_CELLS

    def test_a_sampling_key_sample_pool_lacks_is_refused(self, spark):
        with pytest.raises(ValueError, match="risk_band"):
            build_profile(_frame(spark), _params(dataset={
                "sample_group_keys": ["risk_band", "prod_name", "label"]}), "pool")

    def test_null_key_values_are_counted_aside_and_recorded(self, spark):
        rows = ROWS + [("2025-01-31", "c9", "prod_A", None, 0)] * 3 \
            + [("2025-01-31", "c8", "prod_B", "seg_2", None)]
        profile = build_profile(_frame(spark, rows), _params(), "pool")
        assert profile["cells"] == EXPECTED_CELLS
        assert profile["null_rows"] == {"rows": 4, "by_column": {"seg": 3, "label": 1}}

    def test_missing_train_months_are_refused_by_name(self, spark):
        params = _params()
        del params["dataset"]["train_snap_dates"]
        with pytest.raises(ValueError, match="train_snap_dates"):
            build_profile(_frame(spark), params, "pool")

    def test_a_label_that_does_not_read_as_0_or_1_is_refused(self, spark):
        df = _frame(spark)
        df = df.withColumn("label", df.label.cast("double"))
        with pytest.raises(ValueError, match=r"label.*1\.0"):
            build_profile(df, _params(), "pool")


class TestCli:
    """``run`` from ``tmp_path``: conf/ is read from the working directory, as
    the pipelines read it (conf/base, then the ``--env`` overlay)."""

    def _run(self, spark, tmp_path, monkeypatch, rows=ROWS, env_params=None, env="dev",
             env_args=("--env", "dev")):
        source = tmp_path / "pool.parquet"
        _frame(spark, rows).write.parquet(str(source))
        params = _params(training={"sample_weight_keys": ["seg", "prod_name", "label", "risk_band"]})
        dataset = {**params["dataset"], "train_snap_dates": {
            "start": "2025-01-31", "end": "2025-02-28", "step": "month_end"}}
        base = tmp_path / "conf" / "base"
        base.mkdir(parents=True)
        (base / "parameters.yaml").write_text(yaml.safe_dump({"schema": params["schema"]}))
        (base / "parameters_dataset.yaml").write_text(yaml.safe_dump({"dataset": dataset}))
        (base / "parameters_training.yaml").write_text(
            yaml.safe_dump({"training": params["training"]}))
        overlay = tmp_path / "conf" / env
        overlay.mkdir()
        for stem, content in (env_params or {}).items():
            (overlay / f"{stem}.yaml").write_text(yaml.safe_dump(content))
        monkeypatch.chdir(tmp_path)    # after the spark fixture built its session
        out = tmp_path / "profiling" / "profile.json"
        result = CliRunner().invoke(app, ["run", str(source), "--out", str(out), *env_args])
        return result, out

    @staticmethod
    def _record_reads(monkeypatch):
        """Record every sample_pool read, so a test can say none happened."""
        import scripts.profile_sample_pool as profiler

        reads = []
        real = profiler._load_spark_df
        monkeypatch.setattr(profiler, "_load_spark_df",
                            lambda source: reads.append(source) or real(source))
        return reads

    def test_null_key_rows_are_warned_about_on_the_terminal(self, spark, tmp_path, monkeypatch):
        rows = ROWS + [("2025-01-31", "c9", "prod_A", None, 0)] * 3
        result, _ = self._run(spark, tmp_path, monkeypatch, rows)
        assert result.exit_code == 0, result.stdout + result.stderr
        assert "3 row(s) have a NULL in a key column" in result.stderr
        assert "'seg': 3" in result.stderr

    def test_reads_conf_base_with_the_env_merged_over_it(self, spark, tmp_path, monkeypatch):
        # conf/dev sets only the train months: January alone is counted, by
        # the sampling keys conf/base still supplies.
        result, out = self._run(spark, tmp_path, monkeypatch, env_params={
            "parameters_dataset": {"dataset": {"train_snap_dates": ["2025-01-31"]}}})
        assert result.exit_code == 0, result.stdout + result.stderr
        profile = json.loads(out.read_text())
        assert profile["train_snap_dates"] == ["2025-01-31"]
        assert profile["granularity"] == ["seg", "prod_name"]
        assert profile["cells"] == [
            {"values": {"seg": "seg_1", "prod_name": "prod_A"}, "n_pos": 1, "n_neg": 2},
            {"values": {"seg": "seg_2", "prod_name": "prod_B"}, "n_pos": 1, "n_neg": 0},
        ]

    def test_an_env_without_a_conf_directory_is_refused(self, spark, tmp_path, monkeypatch):
        # A typo must not quietly count with conf/base alone (A30, as the
        # pipelines) — and must say so before sample_pool is read, not after
        # minutes of SparkSession start-up.
        reads = self._record_reads(monkeypatch)
        result, out = self._run(spark, tmp_path, monkeypatch, env_args=("-e", "dve"))
        assert result.exit_code == 1
        assert "(A30) --env='dve'" in result.stderr
        assert reads == [] and not out.exists()

    def test_env_defaults_to_local_as_for_the_pipelines(self, spark, tmp_path, monkeypatch):
        result, _ = self._run(spark, tmp_path, monkeypatch, env_args=())
        assert result.exit_code == 1               # only conf/dev was written
        assert "(A30) --env='local'" in result.stderr

    def test_run_writes_the_profile_json(self, spark, tmp_path, monkeypatch):
        result, out = self._run(spark, tmp_path, monkeypatch)
        assert result.exit_code == 0, result.stdout + result.stderr
        profile = json.loads(out.read_text())
        assert profile["cells"] == EXPECTED_CELLS
        assert profile["train_snap_dates"] == TRAIN_MONTHS     # the range, expanded
        assert "risk_band" in result.stderr                     # the skip is said out loud

    def test_the_planner_reads_the_same_format_version(self):
        from scripts.sampling_budget_planner import PROFILE_FORMAT_VERSION as planner_reads
        assert planner_reads == PROFILE_FORMAT_VERSION
