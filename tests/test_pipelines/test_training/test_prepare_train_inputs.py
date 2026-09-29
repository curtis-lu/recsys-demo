"""Tests for the ``prepare_train_inputs`` training node (ADR-0030 decisions 1, 10).

The node decides what goes into ``train.bin`` / ``train_dev.bin`` (which query
groups are dropped, the row order, why sample weights stay out of the binary)
and whether a cached directory is used. The adapter only turns arrays into its
own format and saves it. These tests read the built ``.bin`` and its sidecars
back from disk rather than any intermediate the node computed, so a step that
ran but never reached the file still fails them.

Cache directories are never spelled out here: a test takes the directory from
the handle the node returned (``Path(handle.bin_path).parent``) or asks
``train_data_cache.cache_dir`` for it.
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.extract import weight_key_columns
from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.pipelines.training.nodes import prepare_train_inputs
from recsys_tfb.pipelines.training.steps import train_data_cache

CACHE_LOGGER = "recsys_tfb.pipelines.training.steps.train_data_cache"
HIT_LOGGER = "recsys_tfb.pipelines.training.steps.local_cache"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_model_input(df, path, **kwargs):
    """Write a fixture frame the way ``build_model_input`` writes a real one.

    Since #283 every numeric feature column is cast to
    ``dataset.numeric_feature_storage_type`` (default float32), so a fixture
    that leaves them float64 is not a shape the training read can ever meet —
    invariant B9 rejects it before reading a row. Casting here rather than in
    each frame literal keeps the fixtures readable and keeps "what a
    model_input looks like" in one place.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].astype("float32")
    out.to_parquet(path, **kwargs)


def _with_cache(tmp_path, parameters):
    """``parameters`` plus the keys that place the cache under ``tmp_path``."""
    out = dict(parameters)
    out["cache"] = {"root": str(tmp_path / "cache")}
    out["base_dataset_version"] = "base_v1"
    out["train_variant_id"] = "variant_v1"
    return out


def _schema_parameters():
    return {"schema": {"columns": {
        "time": "snap_date", "entity": ["cust_id"],
        "item": "prod_name", "label": "label"}}}


def _ranking_parameters(tmp_path, objective):
    return _with_cache(tmp_path, {
        **_schema_parameters(),
        "training": {"algorithm_params": {"objective": objective}},
    })


def _write_frames(tmp_path, df_tr, df_dev, **kwargs):
    """Write both splits once; the returned handles are reusable across calls."""
    tr = tmp_path / "tr.parquet"
    dv = tmp_path / "dv.parquet"
    _write_model_input(df_tr, tr, **kwargs)
    _write_model_input(df_dev, dv, **kwargs)
    return ParquetHandle(str(tr)), ParquetHandle(str(dv))


def _run(handles, prep_meta, parameters):
    return prepare_train_inputs(handles[0], handles[1], prep_meta, parameters)


def _dir(handle):
    """The cache directory a returned handle lives in."""
    return Path(handle.bin_path).parent


def _objective_segment(handle):
    """The ``<objective>`` path segment of a cache directory.

    Layout is ``.../<algorithm>/<objective>/features_<h>/weight_keys_<h>``.
    """
    return _dir(handle).parent.parent.name


def _ranking_prep_meta():
    return {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }


def _ranking_frames():
    # 3 customers x 2 products on one snap_date => 3 query groups of size 2
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["fund", "ccard"] * 3,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "label": [1, 0, 0, 1, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c4", "c5", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["fund", "ccard"] * 2,
        "feat_a": [1.5, 2.5, 3.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def _bin_groups(path, reference=None):
    import lightgbm as lgb
    return lgb.Dataset(str(path), reference=reference).construct()


# ---------------------------------------------------------------------------
# Build, cache hit, partial cache
# ---------------------------------------------------------------------------

def test_prepare_train_inputs_writes_bins(tmp_path):
    """The node writes train.bin, train_dev.bin, their sidecars and _SUCCESS."""
    df_tr = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3", "c4"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "prod_name": ["fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "label": [0, 1, 0, 1],
        }
    )
    df_dev = pd.DataFrame(
        {
            "cust_id": ["c5", "c6"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.5, 2.5],
            "label": [1, 0],
        }
    )
    handles = _write_frames(tmp_path, df_tr, df_dev, engine="pyarrow")
    parameters = _with_cache(tmp_path, _schema_parameters())

    train_h, dev_h = _run(handles, _ranking_prep_meta(), parameters)

    folder = _dir(train_h)
    assert folder == _dir(dev_h)
    # The directory is the one the cache module composes for this build.
    assert str(folder) == train_data_cache.cache_dir(
        parameters, algorithm="lightgbm", objective_segment="binary",
        feature_columns=["feat_a", "prod_name"],
        weight_keys=weight_key_columns(parameters),
    )
    for name in ("train.bin", "train_dev.bin", "train.weight_keys.parquet",
                 "train_dev.weight_keys.parquet", "_SUCCESS"):
        assert (folder / name).exists(), name
    assert train_h.role == "train"
    assert dev_h.role == "train_dev"


def test_prepare_train_inputs_cache_hit(tmp_path, monkeypatch, caplog):
    """Second call with a valid _SUCCESS marker skips lgb.Dataset.construct."""
    import lightgbm as lgb

    df_tr = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    handles = _write_frames(tmp_path, df_tr, df_tr.copy())
    parameters = _with_cache(tmp_path, _schema_parameters())

    first = _run(handles, _ranking_prep_meta(), parameters)
    assert (_dir(first[0]) / "_SUCCESS").exists()

    construct_calls = []
    real_construct = lgb.Dataset.construct

    def spy_construct(self):
        construct_calls.append(1)
        return real_construct(self)

    monkeypatch.setattr(lgb.Dataset, "construct", spy_construct)

    with caplog.at_level(logging.INFO, logger=HIT_LOGGER):
        second = _run(handles, _ranking_prep_meta(), parameters)

    assert construct_calls == [], "cache hit should not call lgb.Dataset.construct"
    assert "cache_hit name=train_data" in caplog.text
    assert second == first


def test_prepare_train_inputs_partial_cache_rebuild(tmp_path):
    """A directory without _SUCCESS is an interrupted build: cleared, rebuilt."""
    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 2),
            "prod_name": ["fund", "ccard"],
            "feat_a": [1.0, 2.0],
            "label": [0, 1],
        }
    )
    handles = _write_frames(tmp_path, df, df.copy())
    parameters = _with_cache(tmp_path, _schema_parameters())

    folder = _dir(_run(handles, _ranking_prep_meta(), parameters)[0])

    # Simulate crash: remove _SUCCESS but leave bins, plus a file only a
    # half-written earlier build could have left behind.
    (folder / "_SUCCESS").unlink()
    (folder / "leftover_from_dead_build.txt").write_text("x")

    _run(handles, _ranking_prep_meta(), parameters)

    assert (folder / "_SUCCESS").exists()
    assert (folder / "train.bin").exists()
    assert not (folder / "leftover_from_dead_build.txt").exists()


def test_prepare_passes_categorical_feature(tmp_path, monkeypatch):
    """The node builds lgb.Dataset with categorical_feature set."""
    import lightgbm as lgb

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3", "c4"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 4),
            "prod_name": ["fund", "ccard", "fund", "ccard"],
            "feat_a": [1.0, 2.0, 3.0, 4.0],
            "label": [0, 1, 0, 1],
        }
    )
    handles = _write_frames(tmp_path, df, df.copy())
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],  # prod_name index = 1
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }

    # Spy on lgb.Dataset.__init__ to capture categorical_feature args passed
    # during construction. lgb binary format does not persist categorical_feature,
    # so we must verify the argument at build time rather than after loading.
    captured_cat_features = []
    real_init = lgb.Dataset.__init__

    def spy_init(self, data, *args, **kwargs):
        cf = kwargs.get("categorical_feature", "auto")
        if cf != "auto" and not isinstance(data, str):
            # Only record non-binary-load calls (binary load passes a file path str)
            captured_cat_features.append(cf)
        real_init(self, data, *args, **kwargs)

    monkeypatch.setattr(lgb.Dataset, "__init__", spy_init)

    _run(handles, prep_meta, _with_cache(tmp_path, _schema_parameters()))

    # Both train and dev datasets should have been built with categorical_feature=[1]
    # (prod_name is at index 1 in feature_columns=["feat_a", "prod_name"])
    assert len(captured_cat_features) == 2, (
        f"Expected 2 lgb.Dataset builds with categorical_feature, got: {captured_cat_features}"
    )
    for cat_attr in captured_cat_features:
        # lgb may store as list[int] (indexes) or list[str] (column names like "Column_1")
        assert cat_attr in ([1], ["prod_name"], ["Column_1"]), (
            f"Unexpected categorical_feature value: {cat_attr}"
        )


# ---------------------------------------------------------------------------
# Objective segment
# ---------------------------------------------------------------------------

def test_prepare_train_inputs_binary_objective_segment(tmp_path):
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    train_h, _ = _run(
        handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, "binary"))
    folder = _dir(train_h)

    # A row-wise objective lands in the "binary" segment and no other
    # objective's segment appears beside it.
    assert (folder / "_SUCCESS").exists()
    assert _objective_segment(train_h) == "binary"
    assert {p.name for p in folder.parent.parent.parent.iterdir()} == {"binary"}
    ds = _bin_groups(train_h.bin_path)
    assert ds.get_group() is None  # binary path: no group set


def test_prepare_train_inputs_ranking_sets_group(tmp_path):
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    train_h, dev_h = _run(
        handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, "lambdarank"))

    assert (_dir(train_h) / "_SUCCESS").exists()
    assert _objective_segment(train_h) == "lambdarank"
    assert _objective_segment(dev_h) == "lambdarank"
    assert train_h.role == "train" and dev_h.role == "train_dev"

    ds_tr = _bin_groups(train_h.bin_path)
    g_tr = ds_tr.get_group()
    assert g_tr is not None
    np.testing.assert_array_equal(np.sort(g_tr), np.array([2, 2, 2]))
    assert int(np.sum(g_tr)) == 6  # all train rows covered

    ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
    g_dv = ds_dv.get_group()
    np.testing.assert_array_equal(np.sort(g_dv), np.array([2, 2]))
    assert int(np.sum(g_dv)) == 4


def test_prepare_train_inputs_three_objectives_coexist(tmp_path):
    """All three objectives keep their own dir under one cache root.

    lambdarank and rank_xendcg used to share a single "ranking" dir; their
    .bin are identical today, so this pins the isolation ahead of the
    follow-up that makes their training matrices differ.
    """
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    folders = {}
    for obj in ("binary", "lambdarank", "rank_xendcg"):
        train_h, _ = _run(
            handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, obj))
        folder = _dir(train_h)
        assert _objective_segment(train_h) == obj
        assert (folder / "_SUCCESS").exists()
        assert (folder / "train.bin").exists()
        folders[obj] = folder
    assert len(set(folders.values())) == 3
    algorithm_dir = folders["binary"].parent.parent.parent
    assert {p.name for p in algorithm_dir.iterdir()} == {
        "binary", "lambdarank", "rank_xendcg"}
    assert not (algorithm_dir / "ranking").exists()


@pytest.mark.parametrize(
    "first,second",
    [("lambdarank", "rank_xendcg"), ("rank_xendcg", "lambdarank")],
)
def test_ranking_objective_switch_never_hits_the_other_bin(
    tmp_path, caplog, first, second
):
    """Either switch order: the second objective BUILDS, it does not hit.

    Both orders matter — a key that folds the two onto one segment is
    order-blind, so testing one direction only would still pass under half
    the regression.
    """
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    first_h, _ = _run(
        handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, first))

    # The evidence is the absence of the cache-hit log line, so it is coupled
    # to the exact wording of ``log_cache_hit`` ("cache_hit name=train_data")
    # — reword that and the negative assertion goes vacuously green. The
    # positive control at the end (a third call *does* log it) keeps the two
    # in sync.
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=HIT_LOGGER):
        train_h, dev_h = _run(
            handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, second))
    assert "cache_hit" not in caplog.text
    assert _objective_segment(train_h) == second
    assert _objective_segment(dev_h) == second
    assert _dir(train_h) != _dir(first_h)
    assert (_dir(first_h) / "_SUCCESS").exists()
    assert (_dir(train_h) / "_SUCCESS").exists()

    caplog.clear()
    with caplog.at_level(logging.INFO, logger=HIT_LOGGER):
        _run(handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, second))
    assert "cache_hit name=train_data" in caplog.text


# ---------------------------------------------------------------------------
# Zero-positive group filter
# ---------------------------------------------------------------------------

def _zero_positive_frames():
    """Ranking fixture in which some query groups hold no positive at all.

    train: 4 customers x 2 products on one snap_date -> 4 groups / 8 rows,
    of which c3 and c4 are all-negative.
    train_dev: 3 customers x 2 products -> 3 groups / 6 rows, of which d3 is
    all-negative. The two splits drop a *different* number of groups so a
    filter wired to only one of them cannot pass by coincidence.
    """
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3", "c4", "c4"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 8),
        "prod_name": ["fund", "ccard"] * 4,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        "label": [1, 0, 0, 1, 0, 0, 0, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["d1", "d1", "d2", "d2", "d3", "d3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["fund", "ccard"] * 3,
        "feat_a": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
        "label": [0, 1, 1, 0, 0, 0],
    })
    return df_tr, df_dev


def _prepare_zero_positive(tmp_path, objective):
    """Build the zero-positive fixture; return (folder, (train_h, dev_h))."""
    df_tr, df_dev = _zero_positive_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    train_h, dev_h = _run(
        handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, objective))
    return _dir(train_h), (train_h, dev_h)


class TestZeroPositiveGroupFilter:
    """lambdarank trains only on query groups holding a positive; nothing else does.

    The assertions read the built ``.bin`` back — the group vector and the row
    count it covers — rather than any intermediate the node computed, so a
    filter that ran but never reached the Dataset would still fail them.
    """

    def test_lambdarank_drops_the_all_negative_groups(self, tmp_path):
        cache, (train_h, dev_h) = _prepare_zero_positive(tmp_path, "lambdarank")

        ds_tr = _bin_groups(train_h.bin_path)
        g_tr = ds_tr.get_group()
        # 4 groups / 8 rows in, c3 and c4 all-negative -> 2 groups / 4 rows.
        np.testing.assert_array_equal(np.sort(g_tr), np.array([2, 2]))
        assert int(np.sum(g_tr)) == 4
        assert ds_tr.num_data() == 4

        ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
        g_dv = ds_dv.get_group()
        # 3 groups / 6 rows in, d3 all-negative -> 2 groups / 4 rows.
        np.testing.assert_array_equal(np.sort(g_dv), np.array([2, 2]))
        assert int(np.sum(g_dv)) == 4
        assert ds_dv.num_data() == 4

        # Every surviving row's label vector still has a positive per group.
        assert int(np.sum(ds_tr.get_label())) == 2
        assert int(np.sum(ds_dv.get_label())) == 2

    def test_rank_xendcg_keeps_every_group(self, tmp_path):
        cache, (train_h, dev_h) = _prepare_zero_positive(tmp_path, "rank_xendcg")

        ds_tr = _bin_groups(train_h.bin_path)
        np.testing.assert_array_equal(
            np.sort(ds_tr.get_group()), np.array([2, 2, 2, 2])
        )
        assert ds_tr.num_data() == 8

        ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
        np.testing.assert_array_equal(
            np.sort(ds_dv.get_group()), np.array([2, 2, 2])
        )
        assert ds_dv.num_data() == 6

    def test_binary_keeps_every_row_and_sets_no_group(self, tmp_path):
        """Regression guard: the non-ranking path is untouched by all of this."""
        cache, (train_h, dev_h) = _prepare_zero_positive(tmp_path, "binary")

        ds_tr = _bin_groups(train_h.bin_path)
        assert ds_tr.get_group() is None
        assert ds_tr.num_data() == 8
        ds_dv = _bin_groups(dev_h.bin_path, reference=ds_tr)
        assert ds_dv.get_group() is None
        assert ds_dv.num_data() == 6

    def test_lambdarank_bin_holds_strictly_fewer_rows_than_rank_xendcg(
        self, tmp_path
    ):
        """The cache split is real, not just two directory names.

        Three objectives under one cache root, same input. Row counts differing
        is what proves the second objective built its own ``.bin`` instead of
        being served the first one's — "all three dirs exist" would pass on the
        directory rename alone.
        """
        df_tr, df_dev = _zero_positive_frames()
        handles = _write_frames(tmp_path, df_tr, df_dev)
        rows = {}
        for obj in ("lambdarank", "rank_xendcg", "binary"):
            train_h, _ = _run(
                handles, _ranking_prep_meta(),
                _ranking_parameters(tmp_path, obj))
            rows[obj] = _bin_groups(train_h.bin_path).num_data()
        assert rows["lambdarank"] < rows["rank_xendcg"]
        assert rows["rank_xendcg"] == rows["binary"] == 8

    def test_weights_follow_the_surviving_rows(self, tmp_path):
        """Weights are filtered with their rows, not left behind to misalign.

        c3/c4 are the dropped groups and carry the ``3.0`` weight on their
        ``ccard`` row, so a weight vector sliced by a different mask (or not at
        all) shows up as the wrong multiset here.
        """
        df_tr, df_dev = _zero_positive_frames()
        # Weight only the rows of groups that survive (c1/c2's "fund" row) and
        # of groups that do not (c3/c4's "fund" row) -- same weight, different
        # fate, so the surviving multiset pins which rows were kept.
        df_tr["cust_segment_typ"] = ["keep", "keep", "keep", "keep",
                                     "drop", "drop", "drop", "drop"]
        df_dev["cust_segment_typ"] = ["keep", "keep", "keep", "keep",
                                      "drop", "drop"]
        handles = _write_frames(tmp_path, df_tr, df_dev)
        params = _ranking_parameters(tmp_path, "lambdarank")
        params["training"]["sample_weights"] = {"keep|fund": 5.0,
                                                "drop|fund": 7.0}
        params["training"]["sample_weight_keys"] = ["cust_segment_typ",
                                                    "prod_name"]
        train_h, dev_h = _run(handles, _ranking_prep_meta(), params)
        # Read off the handle, not the .bin: since #318 the binary carries no
        # weights and the key columns beside it are what the filter had to
        # keep aligned. Same question, same failure mode.
        w_tr = train_h.sample_weights(params, _ranking_prep_meta())
        # 4 surviving rows: c1/c2's fund row weighted 5.0, their ccard row 1.0.
        # 7.0 appears nowhere -- every row carrying it was in a dropped group.
        assert sorted(np.round(w_tr, 3).tolist()) == [1.0, 1.0, 5.0, 5.0]

    def test_filter_counts_reach_the_log(self, tmp_path, caplog):
        """What was dropped is visible without opening the ``.bin``."""
        with caplog.at_level(logging.INFO, logger=CACHE_LOGGER):
            _prepare_zero_positive(tmp_path, "lambdarank")
        text = caplog.text
        # train: 2 of 4 groups and 4 of 8 rows dropped, 2 groups left.
        assert "train data build [train]: dropped" in text
        assert "2/4 zero-positive query groups" in text and "4/8 rows" in text
        # train_dev: 1 of 3 groups and 2 of 6 rows dropped, 2 groups left.
        assert "[train_dev]" in text
        assert "train data build [train_dev]: dropped 1/3 zero-positive query groups" in text
        assert "2/6 rows" in text

    @pytest.mark.parametrize("objective, train, dev", [
        # lambdarank sees only groups holding a positive: none single-label.
        ("lambdarank", "0/2", "0/2"),
        # rank_xendcg keeps the all-negative groups: c3, c4 and d3.
        ("rank_xendcg", "2/4", "1/3"),
    ])
    def test_single_label_share_reaches_the_log(
        self, tmp_path, caplog, objective, train, dev
    ):
        """ADR-0025 decision H: the share of groups a ranking objective can
        draw no pair from, counted on the rows the .bin actually holds."""
        with caplog.at_level(logging.INFO, logger=CACHE_LOGGER):
            _prepare_zero_positive(tmp_path, objective)
        lines = [r.getMessage() for r in caplog.records
                 if "single-label" in r.getMessage()]
        assert any("[train]" in m and f"{train} groups" in m for m in lines), lines
        assert any("[train_dev]" in m and f"{dev} groups" in m for m in lines), lines

    def test_binary_logs_no_single_label_share(self, tmp_path, caplog):
        # Capture the whole package logger so the negative below cannot pass
        # merely because the wrong logger was listened to; the build's own
        # closing line is the proof the capture was live.
        with caplog.at_level(logging.INFO, logger="recsys_tfb"):
            _prepare_zero_positive(tmp_path, "binary")
        assert "train data cache written" in caplog.text
        assert "single-label" not in caplog.text

    def test_counts_sidecar_survives_a_cache_hit(self, tmp_path, caplog):
        """The numbers outlive the build, so a cache-hit run still reports them.

        The filter runs while the ``.bin`` is built. A second run hits the
        cache and extracts nothing, but its manifest still has to describe the
        matrix it trains on -- so the counts are persisted next to the binary
        and read back through the handle.
        """
        cache, (train_h, _) = _prepare_zero_positive(tmp_path, "lambdarank")
        built = train_h.group_filter_counts()
        assert built["objective"] == "lambdarank"
        assert built["train"] == {
            "groups_total": 4, "groups_kept": 2, "groups_dropped": 2,
            "rows_total": 8, "rows_kept": 4, "rows_dropped": 4,
        }
        assert built["train_dev"] == {
            "groups_total": 3, "groups_kept": 2, "groups_dropped": 1,
            "rows_total": 6, "rows_kept": 4, "rows_dropped": 2,
        }

        handles = (ParquetHandle(str(tmp_path / "tr.parquet")),
                   ParquetHandle(str(tmp_path / "dv.parquet")))
        with caplog.at_level(logging.INFO, logger=HIT_LOGGER):
            hit_h, _ = _run(
                handles, _ranking_prep_meta(),
                _ranking_parameters(tmp_path, "lambdarank"))
        assert "cache_hit name=train_data" in caplog.text  # really a hit
        assert hit_h.group_filter_counts() == built

    @pytest.mark.parametrize("objective", ["rank_xendcg", "binary"])
    def test_no_sidecar_when_nothing_is_filtered(self, tmp_path, objective):
        """An unfiltered objective leaves its cache dir exactly as it was.

        Writing an empty report into the ``binary`` directory would be a
        change to the non-ranking artifact set for no gain; absence *is* the
        answer.
        """
        cache, (train_h, _) = _prepare_zero_positive(tmp_path, objective)
        assert train_h.group_filter_counts() is None
        assert not (cache / "group_filter_counts.json").exists()


# ---------------------------------------------------------------------------
# Feature names and feature selection
# ---------------------------------------------------------------------------

def test_prepare_train_inputs_binary_bin_carries_feature_names(tmp_path):
    """The cached .bin persists real feature names from feature_columns, so a
    booster trained on it (the hpo_best final-model path) reports real names —
    not LightGBM's positional Column_N defaults."""
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    train_h, _ = _run(
        handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, "binary"))
    ds = _bin_groups(train_h.bin_path)
    assert ds.get_feature_name() == ["feat_a", "prod_name"]


def test_prepare_train_inputs_ranking_bin_carries_feature_names(tmp_path):
    """Ranking branch: both train and train_dev .bin carry real feature names."""
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    train_h, dev_h = _run(
        handles, _ranking_prep_meta(), _ranking_parameters(tmp_path, "lambdarank"))
    ds_tr = _bin_groups(train_h.bin_path)
    ds_dv = _bin_groups(dev_h.bin_path)
    assert ds_tr.get_feature_name() == ["feat_a", "prod_name"]
    assert ds_dv.get_feature_name() == ["feat_a", "prod_name"]


def _subset_prep_meta():
    """What ``select_features`` hands on when ``feat_a`` is excluded."""
    return {
        "feature_columns": ["prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }


def test_feature_selection_isolates_bin_from_full_feature_cache(
    tmp_path, caplog
):
    """A different feature list is a different directory, and both stay.

    Feature selection reaches the node only as ``feature_columns`` in the
    preprocessor metadata, so that list is what the directory has to be keyed
    by: the subset build must not overwrite (or be served) the full-feature
    binary, and the same list again must land in the same directory as a hit.
    """
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    params = _ranking_parameters(tmp_path, "binary")

    full_h, _ = _run(handles, _ranking_prep_meta(), params)
    full_bin = Path(full_h.bin_path)
    assert full_bin.exists()
    full_mtime = full_bin.stat().st_mtime_ns

    subset_h, _ = _run(handles, _subset_prep_meta(), params)

    assert _dir(subset_h) != _dir(full_h)
    # full-feature bin untouched (no overwrite / collision)
    assert full_bin.exists()
    assert full_bin.stat().st_mtime_ns == full_mtime
    assert _bin_groups(full_bin).get_feature_name() == ["feat_a", "prod_name"]
    # subset bin carries only the kept feature
    assert _bin_groups(subset_h.bin_path).get_feature_name() == ["prod_name"]

    # Same feature list again -> same directory, served from the cache.
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=HIT_LOGGER):
        again_h, _ = _run(handles, _subset_prep_meta(), params)
    assert _dir(again_h) == _dir(subset_h)
    assert "cache_hit name=train_data" in caplog.text
    # and the full-feature directory is still there, still complete
    assert (_dir(full_h) / "_SUCCESS").exists()
    assert (_dir(subset_h) / "_SUCCESS").exists()


def test_one_feature_subset_under_two_objectives_gets_two_dirs(tmp_path):
    """The two cache dimensions compose: the same feature subset under two
    ranking objectives gets two distinct dirs, neither overwriting the other."""
    df_tr, df_dev = _ranking_frames()
    handles = _write_frames(tmp_path, df_tr, df_dev)
    folders = {}
    for obj in ("lambdarank", "rank_xendcg"):
        train_h, _ = _run(
            handles, _subset_prep_meta(), _ranking_parameters(tmp_path, obj))
        assert _objective_segment(train_h) == obj
        folders[obj] = _dir(train_h)
    assert folders["lambdarank"] != folders["rank_xendcg"]
    # Same subset, so the feature segment is the same under both objectives;
    # what tells the two directories apart is the objective above it.
    assert folders["lambdarank"].parent.name == folders["rank_xendcg"].parent.name
    for folder in folders.values():
        assert (folder / "_SUCCESS").exists()
        assert (folder / "train.bin").exists()


# ---------------------------------------------------------------------------
# Sample weights
# ---------------------------------------------------------------------------

def _weight_frames():
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["a", "b"] * 3,
        "cust_segment_typ": ["mass"] * 6,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "label": [1, 0, 0, 1, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c4", "c5", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["a", "b"] * 2,
        "cust_segment_typ": ["mass"] * 4,
        "feat_a": [1.5, 2.5, 3.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def _interleaved_weight_frames():
    """Query groups whose rows are *not* contiguous in the parquet.

    ``_weight_frames`` writes each customer's two rows side by side, so
    ``to_contiguous_groups`` returns the identity permutation and a sidecar
    left unpermuted would still look right. Here the three customers are
    interleaved, so the permutation genuinely reorders rows and a
    weight-to-row misalignment shows up as different numbers rather than as
    nothing at all.

    Rows are (cust, prod): c1/a c2/a c3/a c1/b c2/b c3/b. Group ids by first
    appearance are [0,1,2,0,1,2], so the permutation is [0,3,1,4,2,5] and the
    binary's rows are c1/a c1/b c2/a c2/b c3/a c3/b — prod_name alternating
    a,b,a,b,a,b where the parquet had a,a,a,b,b,b.
    """
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c2", "c3", "c1", "c2", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["a", "a", "a", "b", "b", "b"],
        "cust_segment_typ": ["mass"] * 6,
        "feat_a": [1.0, 3.0, 5.0, 2.0, 4.0, 6.0],
        # Per customer exactly one positive, so every group survives the
        # zero-positive filter and this fixture isolates the permutation.
        "label": [1, 0, 1, 0, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c5", "c4", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["a", "a", "b", "b"],
        "cust_segment_typ": ["mass"] * 4,
        "feat_a": [1.5, 3.5, 2.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def _weight_params(tmp_path, objective, sample_weights=None, weight_keys=None):
    """Parameters whose ``sample_weights`` table can actually match a row.

    ``sample_weight_keys`` has to be spelled out: it defaults to
    ``[schema.item]`` alone, and a two-part config key like ``"mass|a"`` then
    matches nothing — every row stays at weight 1.0, LightGBM stores no weight
    vector at all, and a test asserting on weights fails for a reason that has
    nothing to do with what it is testing. That is exactly how the two tests
    the adapter test file used to carry were red on main (known-pitfalls.md §5).
    """
    return _with_cache(tmp_path, {
        **_schema_parameters(),
        "training": {
            "algorithm_params": {"objective": objective},
            "sample_weight_keys": (
                ["cust_segment_typ", "prod_name"] if weight_keys is None
                else weight_keys
            ),
            "sample_weights": (
                {"mass|a": 3.0} if sample_weights is None else sample_weights
            ),
        },
    })


class TestPrepareTrainInputsWeight:
    """Where sample weights live, and what keeps them answering today's config.

    They used to be baked into the ``.bin``. The cache path did not mention
    ``training.sample_weights``, so a changed weight table hit the same
    directory and trained on the previous run's weights with nothing raised
    (#318). What the binary carries now is nothing; what sits beside it is the
    *key columns*, and each run resolves its own table against them.
    """

    def _prep(self):
        return {
            "feature_columns": ["feat_a", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["a", "b"]},
        }

    def _build(self, tmp_path, objective, params=None, frames=None):
        """Build the cache; return (train_handle, dev_handle, cache_dir)."""
        df_tr, df_dev = frames or _weight_frames()
        handles = _write_frames(tmp_path, df_tr, df_dev)
        train_h, dev_h = _run(
            handles, self._prep(), params or _weight_params(tmp_path, objective))
        return train_h, dev_h, _dir(train_h)

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_bin_carries_no_weight(self, tmp_path, objective):
        """The whole point: nothing weight-shaped is frozen into the binary.

        ``get_weight()`` is ``None`` for an unweighted Dataset, so this is the
        direct read of "the .bin cannot carry a stale weight table".
        """
        _, _, folder = self._build(tmp_path, objective)
        for name in ("train.bin", "train_dev.bin"):
            assert _bin_groups(folder / name).get_weight() is None

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_resolved_weights_match_the_configured_table(
        self, tmp_path, objective
    ):
        """Same numbers the old baked-in vector held, resolved on read."""
        train_h, dev_h, _ = self._build(tmp_path, objective)
        params = _weight_params(tmp_path, objective)
        for handle, n_rows in ((train_h, 6), (dev_h, 4)):
            w = handle.sample_weights(params, self._prep())
            assert len(w) == n_rows
            assert sorted(set(np.round(w, 3))) == [1.0, 3.0]

    def test_sidecar_rows_follow_the_binary_permutation(self, tmp_path):
        """Weights must be in the *binary's* row order, not the parquet's.

        The fixture interleaves query groups, so the ranking branch permutes
        rows into contiguous blocks: prod_name goes from a,a,a,b,b,b in the
        parquet to a,b,a,b,a,b in the binary, and the weights with it. A
        sidecar written before the permutation would give [3,3,3,1,1,1] —
        every weight on the wrong row, and no error anywhere.
        """
        train_h, _, folder = self._build(
            tmp_path, "lambdarank", frames=_interleaved_weight_frames())

        ds = _bin_groups(folder / "train.bin")
        # Labels prove the permutation actually happened: the parquet's
        # 1,0,1,0,1,0 becomes 1,0,0,1,1,0 once grouped by customer.
        assert list(ds.get_label()) == [1.0, 0.0, 0.0, 1.0, 1.0, 0.0]

        w = train_h.sample_weights(
            _weight_params(tmp_path, "lambdarank"), self._prep())
        assert list(np.round(w, 3)) == [3.0, 1.0, 3.0, 1.0, 3.0, 1.0]

    def test_sidecar_rows_follow_the_zero_positive_filter(self, tmp_path):
        """Dropped query groups take their weight-key rows with them.

        lambdarank strips groups holding no positive (#315). The sidecar is
        cut by the same mask in the same place; a sidecar left at full length
        would silently shift every weight by however many rows were dropped
        ahead of it.
        """
        # c1 has no positive at all -> its two rows are dropped; the surviving
        # rows are c2/a c2/b c3/a c3/b -> weights 3,1,3,1.
        df_tr = pd.DataFrame({
            "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "prod_name": ["a", "b"] * 3,
            "cust_segment_typ": ["mass"] * 6,
            "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "label": [0, 0, 1, 0, 0, 1],
        })
        _, df_dev = _weight_frames()
        train_h, _, _ = self._build(
            tmp_path, "lambdarank", frames=(df_tr, df_dev))

        assert train_h.group_filter_counts()["train"]["rows_dropped"] == 2
        w = train_h.sample_weights(
            _weight_params(tmp_path, "lambdarank"), self._prep())
        assert list(np.round(w, 3)) == [3.0, 1.0, 3.0, 1.0]

    def test_empty_sample_weights_resolves_to_all_ones(self, tmp_path):
        """No table configured is the pre-existing behaviour, unchanged."""
        params = _weight_params(tmp_path, "binary", sample_weights={})
        train_h, _, _ = self._build(tmp_path, "binary", params=params)
        w = train_h.sample_weights(params, self._prep())
        assert np.array_equal(w, np.ones(6))

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_changed_weight_table_reuses_the_binary(self, tmp_path, objective):
        """A new weight table costs a resolve, not a rebinning.

        The bug this replaces was the same cache hit answering with stale
        weights. Keeping the hit *and* getting fresh weights is the whole
        reason the keys are cached rather than the vector — so both halves are
        asserted here: same file, different numbers.
        """
        train_h, _, folder = self._build(tmp_path, objective)
        before = (folder / "train.bin").stat().st_mtime_ns

        heavier = _weight_params(
            tmp_path, objective, sample_weights={"mass|a": 7.0})
        train_h2, _, _ = self._build(tmp_path, objective, params=heavier)

        assert train_h2.bin_path == train_h.bin_path
        assert (folder / "train.bin").stat().st_mtime_ns == before
        w = train_h2.sample_weights(heavier, self._prep())
        assert sorted(set(np.round(w, 3))) == [1.0, 7.0]

    @pytest.mark.parametrize(
        "objective", ["binary", "lambdarank", "rank_xendcg"])
    def test_weight_keys_absent_from_model_input_stay_all_ones(
        self, tmp_path, objective
    ):
        """A weight-key column the parquet does not have degrades gracefully.

        This is a real config: production ``feature_table`` carries
        ``cust_segment_typ`` and the synthetic one does not, and A9a validates
        the config's *declarations*, not the parquet. Before the weights moved
        out of the .bin this path returned all-ones with an INACTIVE log line,
        and it still has to.

        The trap it guards is that the sidecar then has **no columns**, and a
        column-less parquet cannot carry a row count — pyarrow writes
        ``num_rows=0``. A length-zero weight vector is not merely wrong, it is
        *invisible*: ``set_weight`` maps any all-ones array to ``None`` and a
        length-zero array is vacuously all-ones, so LightGBM's own length check
        never runs and the search trains unweighted.
        """
        absent = _weight_params(
            tmp_path, objective, sample_weights={"vip": 9.0},
            weight_keys=["no_such_col"])
        train_h, dev_h, _ = self._build(tmp_path, objective, params=absent)
        for handle, n_rows in ((train_h, 6), (dev_h, 4)):
            w = handle.sample_weights(absent, self._prep())
            assert np.array_equal(w, np.ones(n_rows))

    def test_sample_weights_without_a_sidecar_raises(self, tmp_path):
        """Never all-ones by accident.

        A handle whose sidecar is gone cannot be re-weighted, and all-ones is
        a plausible-looking answer that would train the whole search before
        anyone noticed.
        """
        train_h, _, _ = self._build(tmp_path, "binary")
        Path(train_h.weight_keys_path).unlink()
        with pytest.raises(FileNotFoundError, match="sample-weight key sidecar"):
            train_h.sample_weights(_weight_params(tmp_path, "binary"), self._prep())
