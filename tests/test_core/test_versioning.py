"""Tests for recsys_tfb.core.versioning module (three-layer versioning)."""

import copy
import json
import re
from unittest.mock import patch

import pytest

from recsys_tfb.core.versioning import (
    ALL_SAMPLING_KEYS,
    CALIBRATION_SAMPLING_KEYS,
    TRAIN_SAMPLING_KEYS,
    build_manifest_metadata,
    compute_base_dataset_version,
    compute_calibration_variant_id,
    compute_feature_table_fingerprint,
    compute_model_version,
    compute_train_variant_id,
    get_git_commit,
    read_manifest,
    resolve_base_dataset_version,
    resolve_model_version,
    resolve_variant_id,
    update_symlink,
    write_manifest,
)

_HEX8_RE = re.compile(r"^[0-9a-f]{8}$")


def _sample_schema() -> dict:
    return {
        "time": "snap_date",
        "entity": ["cust_id"],
        "item": "prod_name",
        "label": "target",
        "identity_columns": ["snap_date", "cust_id", "prod_name"],
        "categorical_values": {"prod_name": ["a", "b", "c"]},
    }


def _base_params() -> dict:
    return {
        "dataset": {
            "train_snap_dates": [
                "2023-01-31", "2023-02-28", "2023-03-31", "2023-04-30",
                "2023-05-31", "2023-06-30", "2023-07-31", "2023-08-31",
                "2023-09-30", "2023-10-31", "2023-11-30", "2023-12-31",
            ],
            "val_snap_dates": ["2024-01-31"],
            "test_snap_dates": ["2024-02-29"],
            "sample_ratio": 0.1,
            "sample_ratio_overrides": {},
            "sample_group_keys": ["cust_segment_typ"],
            "train_dev_ratio": 0.1,
            "calibration_snap_dates": ["2024-02-29"],
            "calibration_sample_ratio": 1.0,
            "calibration_sample_ratio_overrides": {},
        },
    }


class TestSamplingKeySets:
    def test_train_and_calibration_share_group_keys(self):
        assert "sample_group_keys" in TRAIN_SAMPLING_KEYS
        assert "sample_group_keys" in CALIBRATION_SAMPLING_KEYS

    def test_all_sampling_keys_is_union(self):
        assert ALL_SAMPLING_KEYS == TRAIN_SAMPLING_KEYS | CALIBRATION_SAMPLING_KEYS


class TestComputeFeatureTableFingerprint:
    def test_returns_8_char_hex(self):
        cols = [("snap_date", "date"), ("cust_id", "string"), ("aum_total", "double")]
        fp = compute_feature_table_fingerprint(cols)
        assert _HEX8_RE.match(fp)

    def test_deterministic(self):
        cols = [("snap_date", "date"), ("cust_id", "string")]
        assert compute_feature_table_fingerprint(cols) == \
            compute_feature_table_fingerprint(cols)

    def test_order_sensitive(self):
        a = [("snap_date", "date"), ("cust_id", "string")]
        b = [("cust_id", "string"), ("snap_date", "date")]
        assert compute_feature_table_fingerprint(a) != \
            compute_feature_table_fingerprint(b)

    def test_dtype_sensitive(self):
        a = [("aum_total", "double")]
        b = [("aum_total", "float")]
        assert compute_feature_table_fingerprint(a) != \
            compute_feature_table_fingerprint(b)

    def test_added_column_changes_fingerprint(self):
        base = [("snap_date", "date"), ("cust_id", "string")]
        extended = base + [("new_feat", "double")]
        assert compute_feature_table_fingerprint(base) != \
            compute_feature_table_fingerprint(extended)

    def test_empty_columns_returns_hex(self):
        assert _HEX8_RE.match(compute_feature_table_fingerprint([]))

    def test_accepts_iterable(self):
        # tuple of tuples 應該與 list of tuples 等價
        cols_list = [("snap_date", "date"), ("cust_id", "string")]
        cols_tuple = (("snap_date", "date"), ("cust_id", "string"))
        assert compute_feature_table_fingerprint(cols_list) == \
            compute_feature_table_fingerprint(cols_tuple)


class TestComputeBaseDatasetVersion:
    def test_returns_8_char_hex(self):
        assert _HEX8_RE.match(
            compute_base_dataset_version(_base_params(), _sample_schema())
        )

    def test_deterministic(self):
        a = compute_base_dataset_version(_base_params(), _sample_schema())
        b = compute_base_dataset_version(_base_params(), _sample_schema())
        assert a == b

    def test_sample_ratio_does_not_affect_base(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["sample_ratio"] = 0.5
        assert compute_base_dataset_version(p1, _sample_schema()) == \
            compute_base_dataset_version(p2, _sample_schema())

    def test_calibration_sample_ratio_overrides_does_not_affect_base(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["calibration_sample_ratio_overrides"] = {"prod_x": 0.3}
        assert compute_base_dataset_version(p1, _sample_schema()) == \
            compute_base_dataset_version(p2, _sample_schema())

    def test_sample_group_keys_does_not_affect_base(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["sample_group_keys"] = ["cust_segment_typ", "prod_name"]
        assert compute_base_dataset_version(p1, _sample_schema()) == \
            compute_base_dataset_version(p2, _sample_schema())

    def test_train_snap_dates_affects_base(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["train_snap_dates"] = ["2022-01-31"]
        assert compute_base_dataset_version(p1, _sample_schema()) != \
            compute_base_dataset_version(p2, _sample_schema())

    def test_schema_categorical_values_affects_base(self):
        s1 = _sample_schema()
        s2 = _sample_schema()
        s2["categorical_values"]["prod_name"] = ["a", "b", "c", "d"]
        assert compute_base_dataset_version(_base_params(), s1) != \
            compute_base_dataset_version(_base_params(), s2)

    def test_schema_order_independent(self):
        schema_a = {"time": "t", "entity": ["e"], "item": "i"}
        schema_b = {"item": "i", "entity": ["e"], "time": "t"}
        assert compute_base_dataset_version(_base_params(), schema_a) == \
            compute_base_dataset_version(_base_params(), schema_b)

    def test_fingerprint_default_none_matches_legacy(self):
        # fingerprint=None 必須與不傳該參數時 hash 完全一致（向後相容）
        legacy = compute_base_dataset_version(_base_params(), _sample_schema())
        with_none = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint=None
        )
        assert legacy == with_none

    def test_different_fingerprints_yield_different_hashes(self):
        a = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="aaaaaaaa"
        )
        b = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="bbbbbbbb"
        )
        assert a != b

    def test_same_fingerprint_yields_same_hash(self):
        a = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="cafeb0ba"
        )
        b = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="cafeb0ba"
        )
        assert a == b

    def test_fingerprint_set_differs_from_unset(self):
        # 一旦 caller 開始傳 fingerprint，hash 應該與「沒傳」分流
        legacy = compute_base_dataset_version(_base_params(), _sample_schema())
        with_fp = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="cafeb0ba"
        )
        assert legacy != with_fp


class TestComputeTrainVariantId:
    def test_returns_8_char_hex(self):
        assert _HEX8_RE.match(compute_train_variant_id(_base_params()))

    def test_sample_ratio_affects_train_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["sample_ratio"] = 0.5
        assert compute_train_variant_id(p1) != compute_train_variant_id(p2)

    def test_calibration_sample_ratio_does_not_affect_train_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["calibration_sample_ratio"] = 0.5
        p2["dataset"]["calibration_sample_ratio_overrides"] = {"x": 0.3}
        assert compute_train_variant_id(p1) == compute_train_variant_id(p2)

    def test_sample_group_keys_affects_train_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["sample_group_keys"] = ["cust_segment_typ", "prod_name"]
        assert compute_train_variant_id(p1) != compute_train_variant_id(p2)

    def test_train_snap_dates_does_not_affect_train_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["train_snap_dates"] = ["2022-01-31"]
        assert compute_train_variant_id(p1) == compute_train_variant_id(p2)


class TestComputeCalibrationVariantId:
    def test_returns_8_char_hex(self):
        assert _HEX8_RE.match(compute_calibration_variant_id(_base_params()))

    def test_calibration_sample_ratio_overrides_affects_calibration_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["calibration_sample_ratio_overrides"] = {"prod_x": 0.3}
        assert compute_calibration_variant_id(p1) != compute_calibration_variant_id(p2)

    def test_sample_ratio_does_not_affect_calibration_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["sample_ratio"] = 0.5
        assert compute_calibration_variant_id(p1) == compute_calibration_variant_id(p2)

    def test_sample_group_keys_affects_calibration_variant(self):
        p1 = _base_params()
        p2 = _base_params()
        p2["dataset"]["sample_group_keys"] = ["cust_segment_typ", "prod_name"]
        assert compute_calibration_variant_id(p1) != compute_calibration_variant_id(p2)


class TestComputeModelVersion:
    def test_returns_8_char_hex(self):
        result = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        assert _HEX8_RE.match(result)

    def test_same_inputs_same_hash(self):
        a = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        b = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        assert a == b

    def test_different_base_different_hash(self):
        a = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        b = compute_model_version({"lr": 0.01}, "baseABCD", "trai1234")
        assert a != b

    def test_different_train_variant_different_hash(self):
        a = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        b = compute_model_version({"lr": 0.01}, "base1234", "traiABCD")
        assert a != b

    def test_different_params_different_hash(self):
        a = compute_model_version(
            {"training": {"algorithm_params": {"learning_rate": 0.01}}},
            "base1234", "trai1234",
        )
        b = compute_model_version(
            {"training": {"algorithm_params": {"learning_rate": 0.05}}},
            "base1234", "trai1234",
        )
        assert a != b

    def test_calibration_variant_affects_hash(self):
        a = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        b = compute_model_version({"lr": 0.01}, "base1234", "trai1234", "cal12345")
        assert a != b

    def test_calibration_none_equivalent_to_omitted(self):
        a = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        b = compute_model_version({"lr": 0.01}, "base1234", "trai1234", None)
        assert a == b

    def test_logging_threading_knobs_do_not_affect_hash(self):
        base = {"training": {"algorithm_params": {"learning_rate": 0.01}}}
        noisy = {
            "training": {
                "algorithm_params": {
                    "learning_rate": 0.01,
                    "verbosity": -1,
                    "log_period": 100,
                    "num_threads": 4,
                }
            }
        }
        assert compute_model_version(base, "b", "t") == compute_model_version(
            noisy, "b", "t"
        )

    def test_relevant_hyperparam_changes_hash(self):
        a = {"training": {"num_iterations": 500}}
        b = {"training": {"num_iterations": 800}}
        assert compute_model_version(a, "b", "t") != compute_model_version(
            b, "b", "t"
        )

    def test_top_level_ops_blocks_do_not_affect_hash(self):
        bare = {"training": {"algorithm_params": {"learning_rate": 0.01}}}
        with_ops = {
            "training": {"algorithm_params": {"learning_rate": 0.01}},
            "spark": {"app_name": "x"},
            "mlflow": {"experiment_name": "y", "tracking_uri": "z"},
            "cache": {"root": "/some/local/path"},
        }
        assert compute_model_version(bare, "b", "t") == compute_model_version(
            with_ops, "b", "t"
        )

    def test_caller_params_not_mutated(self):
        params = {
            "training": {
                "algorithm_params": {"learning_rate": 0.01, "verbosity": -1}
            },
            "spark": {"app_name": "x"},
        }
        snapshot = copy.deepcopy(params)
        compute_model_version(params, "b", "t")
        assert params == snapshot


class TestWriteManifest:
    def test_writes_json_file(self, tmp_path):
        version_dir = tmp_path / "v1"
        metadata = {"version": "abc12345", "pipeline": "dataset"}
        write_manifest(version_dir, metadata)

        manifest_path = version_dir / "manifest.json"
        assert manifest_path.exists()
        with open(manifest_path) as f:
            data = json.load(f)
        assert data == metadata

    def test_creates_parent_dirs(self, tmp_path):
        version_dir = tmp_path / "deep" / "nested" / "v1"
        write_manifest(version_dir, {"version": "test"})
        assert (version_dir / "manifest.json").exists()

    def test_overwrites_existing(self, tmp_path):
        version_dir = tmp_path / "v1"
        write_manifest(version_dir, {"version": "old"})
        write_manifest(version_dir, {"version": "new"})
        data = read_manifest(version_dir)
        assert data["version"] == "new"


class TestReadManifest:
    def test_reads_existing(self, tmp_path):
        version_dir = tmp_path / "v1"
        version_dir.mkdir()
        (version_dir / "manifest.json").write_text(json.dumps({"version": "abc"}))
        assert read_manifest(version_dir) == {"version": "abc"}

    def test_raises_when_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_manifest(tmp_path / "nonexistent")


class TestUpdateSymlink:
    def test_create_new_symlink(self, tmp_path):
        target = tmp_path / "v1"
        target.mkdir()
        link = tmp_path / "latest"

        update_symlink(target, link)

        assert link.is_symlink()
        assert link.resolve() == target.resolve()

    def test_update_existing_symlink(self, tmp_path):
        v1 = tmp_path / "v1"
        v1.mkdir()
        v2 = tmp_path / "v2"
        v2.mkdir()
        link = tmp_path / "latest"

        update_symlink(v1, link)
        update_symlink(v2, link)

        assert link.is_symlink()
        assert link.resolve() == v2.resolve()

    def test_replace_existing_directory(self, tmp_path):
        old_dir = tmp_path / "best"
        old_dir.mkdir()
        (old_dir / "model.pkl").write_bytes(b"fake")

        target = tmp_path / "v1"
        target.mkdir()

        update_symlink(target, old_dir)

        assert old_dir.is_symlink()
        assert old_dir.resolve() == target.resolve()


class TestResolveBaseDatasetVersion:
    def test_returns_specified_version(self, tmp_path):
        assert resolve_base_dataset_version(tmp_path, "abc12345") == "abc12345"

    def test_follows_latest_symlink(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        v1 = dataset_dir / "abc12345"
        v1.mkdir()
        latest = dataset_dir / "latest"
        latest.symlink_to(v1.resolve())

        assert resolve_base_dataset_version(dataset_dir, None) == "abc12345"

    def test_raises_when_no_latest(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        with pytest.raises(FileNotFoundError, match="latest"):
            resolve_base_dataset_version(dataset_dir, None)


class TestResolveVariantId:
    def test_returns_specified_variant(self, tmp_path):
        assert resolve_variant_id(tmp_path, "train", "abcd1234") == "abcd1234"
        assert resolve_variant_id(tmp_path, "calibration", "abcd1234") == "abcd1234"

    def test_follows_latest_symlink_for_train(self, tmp_path):
        base_dir = tmp_path / "base1234"
        train_root = base_dir / "train_variants"
        train_root.mkdir(parents=True)
        v1 = train_root / "trai1234"
        v1.mkdir()
        latest = train_root / "latest"
        latest.symlink_to(v1.resolve())

        assert resolve_variant_id(base_dir, "train", None) == "trai1234"

    def test_follows_latest_symlink_for_calibration(self, tmp_path):
        base_dir = tmp_path / "base1234"
        cal_root = base_dir / "calibration_variants"
        cal_root.mkdir(parents=True)
        v1 = cal_root / "cal12345"
        v1.mkdir()
        latest = cal_root / "latest"
        latest.symlink_to(v1.resolve())

        assert resolve_variant_id(base_dir, "calibration", None) == "cal12345"

    def test_raises_when_no_latest(self, tmp_path):
        base_dir = tmp_path / "base1234"
        (base_dir / "train_variants").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="latest"):
            resolve_variant_id(base_dir, "train", None)

    def test_raises_on_bad_variant_kind(self, tmp_path):
        with pytest.raises(ValueError, match="variant_kind"):
            resolve_variant_id(tmp_path, "bogus", None)


class TestResolveModelVersion:
    def test_returns_specified_version(self, tmp_path):
        assert resolve_model_version(tmp_path, "abc12345") == "abc12345"

    def test_follows_best_symlink(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        v1 = models_dir / "abc12345"
        v1.mkdir()
        best = models_dir / "best"
        best.symlink_to(v1.resolve())

        assert resolve_model_version(models_dir, None) == "abc12345"

    def test_best_is_directory_returns_best(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        best = models_dir / "best"
        best.mkdir()

        assert resolve_model_version(models_dir, None) == "best"

    def test_raises_when_no_best(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()
        with pytest.raises(FileNotFoundError, match="best"):
            resolve_model_version(models_dir, None)


class TestGetGitCommit:
    def test_returns_string_in_git_repo(self):
        commit = get_git_commit()
        # We're running in a git repo
        assert commit is not None
        assert len(commit) >= 7

    def test_returns_none_when_git_unavailable(self):
        with patch("recsys_tfb.core.versioning.subprocess.run", side_effect=FileNotFoundError):
            assert get_git_commit() is None


class TestBuildManifestMetadata:
    def test_dataset_base_manifest(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            base_dataset_version="abc12345",
            artifacts=["val_keys.parquet"],
        )
        assert meta["version"] == "abc12345"
        assert meta["pipeline"] == "dataset"
        assert "created_at" in meta
        assert "git_commit" in meta
        assert meta["parameters"] == {"sample_ratio": 0.1}
        assert meta["artifacts"] == ["val_keys.parquet"]
        assert meta["base_dataset_version"] == "abc12345"
        assert "model_version" not in meta
        assert "parent_version" not in meta

    def test_variant_manifest_records_parent(self):
        meta = build_manifest_metadata(
            version="trai1234",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            parent_version="base1234",
            variant_kind="train",
            artifacts=["train_model_input.parquet"],
        )
        assert meta["parent_version"] == "base1234"
        assert meta["variant_kind"] == "train"

    def test_training_manifest(self):
        meta = build_manifest_metadata(
            version="def67890",
            pipeline="training",
            parameters={"lr": 0.01},
            base_dataset_version="abc12345",
            train_variant_id="trai1234",
            artifacts=["model.pkl"],
        )
        assert meta["base_dataset_version"] == "abc12345"
        assert meta["train_variant_id"] == "trai1234"
        assert "calibration_variant_id" not in meta
        assert "model_version" not in meta

    def test_training_manifest_with_calibration(self):
        meta = build_manifest_metadata(
            version="def67890",
            pipeline="training",
            parameters={"lr": 0.01},
            base_dataset_version="abc12345",
            train_variant_id="trai1234",
            calibration_variant_id="cal12345",
        )
        assert meta["calibration_variant_id"] == "cal12345"

    def test_inference_manifest(self):
        meta = build_manifest_metadata(
            version="best",
            pipeline="inference",
            parameters={"snap_dates": ["2024-03-31"]},
            model_version="def67890",
            base_dataset_version="abc12345",
            train_variant_id="trai1234",
        )
        assert meta["model_version"] == "def67890"
        assert meta["base_dataset_version"] == "abc12345"
        assert meta["train_variant_id"] == "trai1234"

    def test_dataset_manifest_records_feature_table_fingerprint(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            base_dataset_version="abc12345",
            feature_table_fingerprint="cafeb0ba",
        )
        assert meta["feature_table_fingerprint"] == "cafeb0ba"

    def test_manifest_omits_fingerprint_when_not_provided(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            base_dataset_version="abc12345",
        )
        assert "feature_table_fingerprint" not in meta
