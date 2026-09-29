"""Tests for training cache nodes (post-refactor).

Cache nodes now write parquet to driver-local fs and return a ParquetHandle.
The ``cache.enabled=false`` passthrough mode has been removed; tests must
provide a writable cache_root via tmp_path.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from recsys_tfb.pipelines.training.cache_sources import inject_cache_source_tables
from recsys_tfb.pipelines.training.steps.local_cache import CACHE_SOURCE_TABLES


def _catalog_partitions() -> dict[str, dict[str, list[str]]]:
    """The partition layout the real ``conf/base/catalog.yaml`` declares.

    Derived by running the production derivation over the production catalog,
    not transcribed. Transcribing is what this change removed from ``src/``
    (``_CACHE_OUTER_PARTITIONS``, a hand copy of the catalog's
    ``partition_filter`` keys with nothing keeping the two in step) — growing
    the same mirror back on the test side would put the drift right back, just
    somewhere nothing would look for it.

    Reading the catalog raw, without the CLI's ``${...}`` substitution: only
    the partition *names* are wanted here, and every value in this dict is a
    key or a column name, never a substituted value.
    """
    import yaml

    # parents[3] is the tree this test file was shipped with, not the CWD:
    # a worktree must read its own catalog, never main's.
    catalog_path = (
        Path(__file__).resolve().parents[3] / "conf" / "base" / "catalog.yaml"
    )
    with open(catalog_path) as f:
        catalog = yaml.safe_load(f)
    params: dict = {}
    inject_cache_source_tables(params, catalog)
    partitions = params.get("_cache_partitions", {})
    # Guard rather than let a silently empty mapping make every cache test
    # pass for the wrong reason: with no injection at all the nodes refuse
    # outright, and that refusal is a different test.
    assert set(partitions) == set(CACHE_SOURCE_TABLES), (
        "conf/base/catalog.yaml no longer declares every cache as a "
        f"HiveTableDataset: got {sorted(partitions)}"
    )
    return partitions


def _params_with_cache_root(cache_root: Path) -> dict:
    return {
        "hive": {"db": "ml_recsys"},
        "cache": {"root": str(cache_root)},
        "base_dataset_version": "deadbeef",
        "train_variant_id": "v1",
        "_cache_partitions": _catalog_partitions(),
    }


def _params_with_test_dates(cache_root: Path, test_snap_dates: list[str]) -> dict:
    """Cache params carrying ``dataset.test_snap_dates``.

    Mirrors the merged parameters dict the CLI hands to nodes (all
    ``parameters_*.yaml`` deep-merged + runtime version params).
    """
    params = _params_with_cache_root(cache_root)
    params["dataset"] = {"test_snap_dates": list(test_snap_dates)}
    return params


def _stub_hdfs(monkeypatch, location: str = "hdfs:/some/path") -> None:
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.steps.local_cache.get_hive_table_location",
        lambda spark, db, table: location,
    )
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.steps.local_cache.copy_hdfs_to_local",
        lambda spark, src_glob, dst, glob: Path(dst).mkdir(parents=True, exist_ok=True),
    )


def _recording_hdfs(monkeypatch, copy_calls: list, available: set | None = None):
    """Stub the HDFS layer, recording every copy destination.

    ``available``: the ``snap_date=`` values the source table holds. A glob that
    matches nothing raises FileNotFoundError — the real ``copy_hdfs_to_local``
    does exactly this (utils/hdfs.py), and per-month copying is what turns it
    into the "you forgot to run dataset" guard.
    """
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.steps.local_cache.get_hive_table_location",
        lambda *a, **kw: "hdfs:/some/path",
    )

    def _copy(spark, src_glob, dst, glob):
        if available is not None:
            month = src_glob.rsplit("snap_date=", 1)[-1]
            if month not in available:
                raise FileNotFoundError(f"No HDFS paths matched: {src_glob}")
        copy_calls.append(dst)
        Path(dst).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.steps.local_cache.copy_hdfs_to_local", _copy
    )


def _spark_df() -> MagicMock:
    df = MagicMock()
    df.sql_ctx.sparkSession = MagicMock()
    return df


class TestCacheNodeReturnHandle:
    def test_cache_train_returns_parquet_handle(self, tmp_path, monkeypatch):
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()

        params = _params_with_cache_root(tmp_path)
        handle = cache_train_model_input(df, params)

        assert isinstance(handle, ParquetHandle)
        assert "train_model_input" in handle.path

    def test_cache_creates_success_marker(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_val_model_input

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()

        params = _params_with_cache_root(tmp_path)
        handle = cache_val_model_input(df, params)

        success = Path(handle.path) / "_SUCCESS"
        assert success.exists()


class TestCacheHit:
    def test_skip_copy_when_success_marker_present(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input
        from recsys_tfb.pipelines.training.steps.local_cache import resolve_cache_path

        params = _params_with_cache_root(tmp_path)
        cache_path = Path(resolve_cache_path("train_model_input", params))
        cache_path.mkdir(parents=True, exist_ok=True)
        (cache_path / "_SUCCESS").touch()

        copy_calls = []
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.steps.local_cache.copy_hdfs_to_local",
            lambda *a, **kw: copy_calls.append(1),
        )
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.steps.local_cache.get_hive_table_location",
            lambda *a, **kw: "hdfs:/some/path",
        )

        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        cache_train_model_input(df, params)

        assert copy_calls == []


class TestPerMonthTestCache:
    """``test_model_input`` caches one directory per configured test month.

    Each month is its own cache entry with its own ``_SUCCESS``, so adding a
    month adds a directory and leaves every existing month untouched. The
    directory name states exactly one month, so name and contents cannot
    disagree.
    """

    def test_returns_one_handle_per_configured_month(self, tmp_path, monkeypatch):
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        _stub_hdfs(monkeypatch)
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        handles = cache_test_model_input(_spark_df(), params)

        # keys are verbatim config values — no format conversion
        assert sorted(handles) == ["2026-01-31", "2026-02-28"]
        assert all(isinstance(h, ParquetHandle) for h in handles.values())

    def test_month_directory_is_literal_yyyymmdd(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        _stub_hdfs(monkeypatch)
        params = _params_with_test_dates(tmp_path, ["2026-01-31"])

        handle = cache_test_model_input(_spark_df(), params)["2026-01-31"]

        # Assert the adjacent pair: asserting the month alone would leave the
        # `test_months` grouping layer with no contract at all.
        assert Path(handle.path).parts[-3:-1] == ("test_months", "20260131")

    def test_adding_a_month_does_not_recopy_existing_months(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)

        before = cache_test_model_input(
            _spark_df(), _params_with_test_dates(tmp_path, ["2026-01-31"])
        )
        copy_calls.clear()
        after = cache_test_model_input(
            _spark_df(), _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])
        )

        # only the new month was copied; January's directory is reused as-is
        assert copy_calls == [after["2026-02-28"].path]
        assert after["2026-01-31"].path == before["2026-01-31"].path

    def test_same_config_rerun_hits_every_month(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        cache_test_model_input(_spark_df(), params)
        copy_calls.clear()
        second = cache_test_model_input(_spark_df(), params)

        assert copy_calls == []
        # A cache hit must still yield the month's handle. Asserting only "no
        # copies" is satisfied just as well by dropping hit months from the
        # mapping — which is precisely the silent gap this ticket exists to
        # prevent (predict would never see that month).
        assert sorted(second) == ["2026-01-31", "2026-02-28"]

    def test_rebuild_dates_drops_and_recopies_only_the_named_month(
        self, tmp_path, monkeypatch
    ):
        """Cache hits are decided by ``_SUCCESS``, never by freshness, so after
        an upstream backfill the named month's cached parquet is stale but
        complete. Without this it survives, predict reads it, and the numbers
        come out byte-identical — the rebuild would run and change nothing.
        """
        from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)
        months = ["2026-01-31", "2026-02-28"]

        first = cache_test_model_input(
            _spark_df(), _params_with_test_dates(tmp_path, months)
        )
        # A file only the first copy could have produced: if the directory is
        # merely written over rather than cleared, it survives.
        stale = Path(first["2026-01-31"].path) / "stale-part-0.parquet"
        stale.touch()
        copy_calls.clear()

        params = _params_with_test_dates(tmp_path, months)
        params[REBUILD_SNAP_DATES_KEY] = ["2026-01-31"]
        second = cache_test_model_input(_spark_df(), params)

        assert copy_calls == [second["2026-01-31"].path]
        assert not stale.exists()
        assert (Path(second["2026-01-31"].path) / "_SUCCESS").exists()
        # February was not named, so it stays a hit and keeps its handle.
        assert second["2026-02-28"].path == first["2026-02-28"].path

    def test_rebuild_that_cannot_clear_the_directory_fails_loud(
        self, tmp_path, monkeypatch
    ):
        """If the drop silently fails, the surviving ``_SUCCESS`` is read as a
        hit two lines later and the rebuild degrades into the stale-cache run it
        exists to prevent — succeeding, and producing identical numbers.
        """
        from recsys_tfb.core.consistency import REBUILD_SNAP_DATES_KEY
        from recsys_tfb.pipelines.training import nodes as training_nodes
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)
        params = _params_with_test_dates(tmp_path, ["2026-01-31"])
        cache_test_model_input(_spark_df(), params)

        monkeypatch.setattr(
            training_nodes.shutil, "rmtree", lambda *a, **kw: None
        )
        params[REBUILD_SNAP_DATES_KEY] = ["2026-01-31"]
        copy_calls.clear()

        with pytest.raises(RuntimeError, match="could not clear the cached month"):
            cache_test_model_input(_spark_df(), params)
        assert copy_calls == []

    def test_month_absent_from_source_fails_loud(self, tmp_path, monkeypatch):
        """Configured a month but never ran dataset → the copy glob matches
        nothing and FileNotFoundError propagates. This guard is inherent to
        per-month copying; no extra coverage check is written for it."""
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls, available={"2026-01-31"})
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        with pytest.raises(FileNotFoundError, match="2026-02-28"):
            cache_test_model_input(_spark_df(), params)

    def test_failed_month_leaves_no_success_marker_and_recovers(
        self, tmp_path, monkeypatch
    ):
        """The fail-loud path mkdirs before it globs, so it leaves an empty
        directory behind. It must carry no _SUCCESS, or the next run would
        treat that empty month as complete and silently serve nothing."""
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls, available={"2026-01-31"})
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        with pytest.raises(FileNotFoundError):
            cache_test_model_input(_spark_df(), params)

        feb = tmp_path / "deadbeef" / "test_months" / "20260228"
        if feb.exists():
            assert not any(feb.rglob("_SUCCESS"))

        # once the source has the month, the next run completes it
        copy_calls.clear()
        _recording_hdfs(monkeypatch, copy_calls,
                        available={"2026-01-31", "2026-02-28"})
        handles = cache_test_model_input(_spark_df(), params)
        assert sorted(handles) == ["2026-01-31", "2026-02-28"]
        assert (Path(handles["2026-02-28"].path) / "_SUCCESS").exists()

    def test_a_repeated_month_collapses_to_one_entry(self, tmp_path, monkeypatch):
        """The same month listed twice is one cache entry, not two.

        Two *different* spellings of one month are no longer this node's
        problem: they are rejected at CLI entry by consistency invariant A26
        (see tests/test_core/test_consistency.py). What stays here is the
        dedupe itself — a repeated literal must not key the directory twice,
        because handle_paths would then hand pyarrow the same root twice and
        double every row of that month.
        """
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        _stub_hdfs(monkeypatch)
        params = _params_with_test_dates(
            tmp_path, ["2026-01-31", "2026-01-31", "2026-02-28"]
        )

        handles = cache_test_model_input(_spark_df(), params)
        assert sorted(handles) == ["2026-01-31", "2026-02-28"]

    def test_partial_month_rebuilds_without_touching_siblings(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])
        handles = cache_test_model_input(_spark_df(), params)

        # corrupt only February: drop its _SUCCESS and leave debris behind
        feb = Path(handles["2026-02-28"].path)
        (feb / "_SUCCESS").unlink()
        (feb / "stale_partial.parquet").touch()
        copy_calls.clear()

        cache_test_model_input(_spark_df(), params)

        assert copy_calls == [str(feb)]
        assert not (feb / "stale_partial.parquet").exists()
        assert (feb / "_SUCCESS").exists()

    def test_other_splits_layout_untouched_by_test_dates(self, tmp_path):
        from recsys_tfb.pipelines.training.steps.local_cache import resolve_cache_path

        one = _params_with_test_dates(tmp_path, ["2026-01-31"])
        two = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        for name in (
            "train_model_input",
            "train_dev_model_input",
            "val_model_input",
        ):
            assert resolve_cache_path(name, one) == resolve_cache_path(name, two)

        # val is test's structural twin (single-layer before this change); pin
        # its literal path so "the month layer landed on the wrong split"
        # cannot pass as "both sides moved together".
        assert resolve_cache_path("val_model_input", two) == str(
            tmp_path / "deadbeef" / "val_model_input.parquet"
        )


class TestPartialCacheRecovery:
    def test_rmtree_when_dir_exists_without_success(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input
        from recsys_tfb.pipelines.training.steps.local_cache import resolve_cache_path

        params = _params_with_cache_root(tmp_path)
        cache_path = Path(resolve_cache_path("train_model_input", params))
        cache_path.mkdir(parents=True, exist_ok=True)
        (cache_path / "stale_partial.parquet").touch()

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        cache_train_model_input(df, params)

        assert not (cache_path / "stale_partial.parquet").exists()
        assert (cache_path / "_SUCCESS").exists()


class TestRejectsNonSparkInput:
    def test_passthrough_mode_removed(self, tmp_path):
        """cache.enabled=false has been removed; pandas inputs must be rejected."""
        import pandas as pd
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        params = _params_with_cache_root(tmp_path)
        df = pd.DataFrame({"a": [1]})  # not a Spark DataFrame

        with pytest.raises(TypeError, match="Spark DataFrame"):
            cache_train_model_input(df, params)


def _train_parquets(tmp_path: Path):
    """A train / train_dev parquet pair a ranking objective can build from."""
    import numpy as np
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle

    df = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["fund", "ccard"] * 3,
        # float32 like build_model_input writes it (#283 / B9)
        "feat_a": np.arange(6, dtype=np.float32),
        # c3's group holds no positive: lambdarank drops it.
        "label": [1, 0, 0, 1, 0, 0],
    })
    train_dir, dev_dir = tmp_path / "tr.parquet", tmp_path / "dev.parquet"
    df.to_parquet(train_dir)
    df.to_parquet(dev_dir)
    return ParquetHandle(str(train_dir)), ParquetHandle(str(dev_dir))


_TRAIN_PREP = {
    "feature_columns": ["feat_a", "prod_name"],
    "categorical_columns": ["prod_name"],
    "category_mappings": {"prod_name": ["fund", "ccard"]},
}


def _train_params(cache_root: Path, objective="lambdarank", **training) -> dict:
    return {
        "cache": {"root": str(cache_root)},
        "base_dataset_version": "v1",
        "train_variant_id": "tv1",
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label",
        }},
        "training": {
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": objective},
            **training,
        },
    }


class TestPrepareTrainInputs:
    def test_prepare_node_returns_two_lgb_handles(self, tmp_path):
        from recsys_tfb.io.handles import LgbDatasetHandle
        from recsys_tfb.pipelines.training.nodes import prepare_train_inputs

        train_h, dev_h = prepare_train_inputs(
            *_train_parquets(tmp_path), _TRAIN_PREP,
            _train_params(tmp_path / "cache", objective="binary"),
        )

        assert isinstance(train_h, LgbDatasetHandle)
        assert isinstance(dev_h, LgbDatasetHandle)
        assert train_h.role == "train"
        assert dev_h.role == "train_dev"


class TestTrainDataCachePath:
    """The ``.bin`` cache is used exactly when its path holds ``_SUCCESS``.

    ADR-0030 decision 10: everything that decides what the binaries hold is a
    segment of the path, and nothing inside a directory is inspected. So the
    cases that used to need a check inside the directory — a changed
    ``training.sample_weight_keys``, a directory written by older code — have
    to land on a different path, and a directory the build never finished has
    to be rebuilt. Whether a build ran is read off the matrix reads: a hit
    reads no parquet at all.
    """

    @staticmethod
    def _count_matrix_reads(monkeypatch) -> list:
        from recsys_tfb.pipelines.training import nodes

        reads: list = []
        real = nodes.extract_X_rows

        def counting(handle, *a, **kw):
            reads.append(handle.path)
            return real(handle, *a, **kw)

        monkeypatch.setattr(nodes, "extract_X_rows", counting)
        return reads

    def _build(self, tmp_path, params):
        from recsys_tfb.pipelines.training.nodes import prepare_train_inputs

        return prepare_train_inputs(*_train_parquets(tmp_path), _TRAIN_PREP, params)

    def test_same_path_with_the_marker_is_served_without_a_read(
        self, tmp_path, monkeypatch,
    ):
        params = _train_params(tmp_path / "cache")
        first, _ = self._build(tmp_path, params)
        reads = self._count_matrix_reads(monkeypatch)

        again, _ = self._build(tmp_path, params)

        assert reads == []
        assert again.bin_path == first.bin_path
        assert again.group_filter_counts() == first.group_filter_counts()

    def test_changed_weight_keys_land_on_another_path_and_rebuild(
        self, tmp_path, monkeypatch,
    ):
        """The sidecar holds the weight-key columns, so another key list is
        another cache. Served the old one, the resolver would find the new
        key column missing and weight every row 1.0 — a whole search trained
        unweighted under a model_version keyed by the weights (#318)."""
        import pyarrow.parquet as pq

        old, _ = self._build(tmp_path, _train_params(tmp_path / "cache"))
        reads = self._count_matrix_reads(monkeypatch)

        new, _ = self._build(tmp_path, _train_params(
            tmp_path / "cache", sample_weight_keys=["prod_name", "cust_id"]))

        assert Path(new.bin_path).parent != Path(old.bin_path).parent
        assert len(reads) == 2  # train and train_dev, built again
        assert pq.read_schema(new.weight_keys_path).names == ["prod_name", "cust_id"]
        # The old build is left where it was, still complete.
        assert (Path(old.bin_path).parent / "_SUCCESS").exists()

    def test_a_cache_from_before_the_format_version_is_not_served(
        self, tmp_path, monkeypatch,
    ):
        """Directories written before #483 have no version segment
        (``train_variants/<id>/lgb/<objective>/``); finished or not, a current
        build never looks there."""
        variant = tmp_path / "cache" / "v1" / "train_variants" / "tv1"
        legacy = variant / "lgb" / "lambdarank"
        legacy.mkdir(parents=True)
        for name in ("train.bin", "train_dev.bin", "_SUCCESS"):
            (legacy / name).write_text("written by older code")
        reads = self._count_matrix_reads(monkeypatch)

        train_h, _ = self._build(tmp_path, _train_params(tmp_path / "cache"))

        assert len(reads) == 2
        assert legacy not in Path(train_h.bin_path).parents
        assert (legacy / "train.bin").read_text() == "written by older code"

    def test_a_format_version_bump_moves_the_cache(self, tmp_path, monkeypatch):
        """What the version number is for: code that changes what a directory
        holds bumps it, and every deployment rebuilds once."""
        from recsys_tfb.pipelines.training.steps import train_data_cache

        before, _ = self._build(tmp_path, _train_params(tmp_path / "cache"))
        monkeypatch.setattr(
            train_data_cache, "TRAIN_DATA_CACHE_FORMAT_VERSION",
            train_data_cache.TRAIN_DATA_CACHE_FORMAT_VERSION + 1)
        reads = self._count_matrix_reads(monkeypatch)

        after, _ = self._build(tmp_path, _train_params(tmp_path / "cache"))

        assert Path(after.bin_path).parent != Path(before.bin_path).parent
        assert len(reads) == 2

    def test_a_directory_without_the_marker_is_rebuilt(self, tmp_path, monkeypatch):
        """An interrupted build leaves files and no marker. Its files are
        whatever landed before it died; they are dropped, not read."""
        params = _train_params(tmp_path / "cache")
        train_h, _ = self._build(tmp_path, params)
        directory = Path(train_h.bin_path).parent
        (directory / "_SUCCESS").unlink()
        (directory / "debris.tmp").write_text("half a build")
        reads = self._count_matrix_reads(monkeypatch)

        again, _ = self._build(tmp_path, params)

        assert again.bin_path == train_h.bin_path
        assert len(reads) == 2
        assert not (directory / "debris.tmp").exists()
        assert (directory / "_SUCCESS").exists()


class TestPartitionNamesComeFromTheCatalog:
    """The glob's directory levels are whatever ``catalog.yaml`` declared.

    This repo is a configurable ranking framework: ``schema.columns.time`` is
    the user's column, and the Hive partition that carries it is named in
    ``catalog.yaml``. Before #326 the cache globbed the example spelling
    verbatim, so a user who renamed either one got a ``FileNotFoundError``
    naming a path that looked plausible — four Spark-cold-start minutes in.

    These tests rename the partitions to names that appear nowhere in ``src/``
    and assert the renamed names come out the other end. A literal put back
    into ``populate_cache_from_hive`` turns them red on the glob assertion.
    """

    @staticmethod
    def _recording_globs(monkeypatch, globs: list) -> None:
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.steps.local_cache."
            "get_hive_table_location",
            lambda *a, **kw: "hdfs:/warehouse/tbl",
        )

        def _copy(spark, src_glob, dst, glob):
            globs.append(src_glob)
            Path(dst).mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.steps.local_cache.copy_hdfs_to_local",
            _copy,
        )

    def test_renamed_time_partition_is_what_gets_globbed(
        self, tmp_path, monkeypatch
    ):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        params = _params_with_cache_root(tmp_path)
        params["_cache_partitions"]["train_model_input"] = {
            "filter_keys": ["base_dataset_version", "train_variant_id"],
            "cols": ["as_of_month"],
        }
        globs: list[str] = []
        self._recording_globs(monkeypatch, globs)

        cache_train_model_input(_spark_df(), params)

        assert globs == [
            "hdfs:/warehouse/tbl/base_dataset_version=deadbeef/"
            "train_variant_id=v1/as_of_month=*"
        ]

    def test_renamed_filter_keys_are_what_gets_globbed(
        self, tmp_path, monkeypatch
    ):
        """The outer levels come from the same place, not from a local mirror.

        ``_CACHE_OUTER_PARTITIONS`` used to hand-copy every entry's
        ``partition_filter`` keys with nothing keeping the two in step.
        """
        from recsys_tfb.pipelines.training.nodes import cache_val_model_input

        params = _params_with_cache_root(tmp_path)
        params["scenario_id"] = "s7"
        params["_cache_partitions"]["val_model_input"] = {
            "filter_keys": ["scenario_id"],
            "cols": ["as_of_month"],
        }
        globs: list[str] = []
        self._recording_globs(monkeypatch, globs)

        cache_val_model_input(_spark_df(), params)

        assert globs == ["hdfs:/warehouse/tbl/scenario_id=s7/as_of_month=*"]

    def test_single_month_copy_narrows_on_the_renamed_time_partition(
        self, tmp_path, monkeypatch
    ):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        params = _params_with_test_dates(tmp_path, ["2024-03-31"])
        params["_cache_partitions"]["test_model_input"] = {
            "filter_keys": ["base_dataset_version"],
            "cols": ["as_of_month", "sku"],
        }
        globs: list[str] = []
        self._recording_globs(monkeypatch, globs)

        cache_test_model_input(_spark_df(), params)

        # Only the first partition level is narrowed; ``sku`` below it comes
        # along as part of the subtree.
        assert globs == [
            "hdfs:/warehouse/tbl/base_dataset_version=deadbeef/"
            "as_of_month=2024-03-31"
        ]

    def test_missing_injection_names_the_key_that_is_missing(
        self, tmp_path, monkeypatch
    ):
        """No fallback: a guessed partition name fails as a path that matched
        nothing, which names neither the guess nor where to fix it."""
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        params = _params_with_cache_root(tmp_path)
        del params["_cache_partitions"]
        _stub_hdfs(monkeypatch)

        with pytest.raises(ValueError, match="_cache_partitions"):
            cache_train_model_input(_spark_df(), params)

    def test_an_entry_declaring_no_partition_cols_says_so_separately(
        self, tmp_path, monkeypatch
    ):
        """Two ways to have no layout, two things to go fix — the catalog entry
        is missing, or it is there and declares no partition_cols."""
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        params = _params_with_cache_root(tmp_path)
        params["_cache_partitions"]["train_model_input"] = {
            "filter_keys": ["base_dataset_version"], "cols": []}
        _stub_hdfs(monkeypatch)

        with pytest.raises(ValueError, match="declares no partition_cols"):
            cache_train_model_input(_spark_df(), params)
