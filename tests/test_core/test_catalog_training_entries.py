"""Catalog regression: the two training artifacts that landed with ADR-0014.

Both used to be memory-only names. Landing them is what buys "you can fetch it
and look at it" (`sample_weight_report`) and a cheap resume point
(`trained_model`) -- so these tests pin the two properties that make the
landing worth anything, not merely that a key exists.

yaml.safe_load on catalog.yaml is an established pattern in this repo (the
``${...}`` placeholders are plain string scalars). Pure-Python, no Spark.
"""

from pathlib import Path

import yaml


def _load_catalog():
    # tests/test_core/<this file> -> parents[2] == repo (worktree) root
    root = Path(__file__).resolve().parents[2]
    return yaml.safe_load((root / "conf" / "base" / "catalog.yaml").read_text())


class TestSampleWeightReportEntry:
    """The node stopped writing the file; the catalog writes it now.

    Which makes the *filepath* the load-bearing part: the manifest reader in
    ``__main__`` looks for one exact name in one exact directory.
    """

    def test_entry_is_a_json_dataset(self):
        d = _load_catalog()
        assert "sample_weight_report" in d, (
            "persist_sample_weight_report returns the dict and the catalog "
            "persists it -- without this entry the report is a fake output again"
        )
        assert d["sample_weight_report"]["type"] == "JSONDataset"

    def test_lands_where_the_manifest_reader_looks(self, tmp_path):
        """``_sample_weight_extra`` reads ``<version_dir>/sample_weight_report.json``.

        Exercise the real reader against the real catalog path: move the entry
        into ``diagnostics/`` (or rename the file) and the manifest's
        ``extra_metadata.sample_weight`` silently disappears. This turns that
        into a red test instead.
        """
        from recsys_tfb.__main__ import _dir_artifacts, _sample_weight_extra

        rel = Path(_load_catalog()["sample_weight_report"]["filepath"])
        assert rel.parts[:3] == ("data", "models", "${model_version}"), rel

        version_dir = tmp_path / "models" / "v1"
        target = version_dir / Path(*rel.parts[3:])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('{"enabled": true, "unmatched_keys": []}')

        assert _sample_weight_extra(version_dir) == {
            "sample_weight": {"enabled": True, "unmatched_keys": []}
        }
        # ``artifacts`` lists first-level files only, so a nested filepath
        # would drop it off the manifest listing as well.
        assert "sample_weight_report.json" in _dir_artifacts(version_dir)


class TestTrainedModelEntry:
    """`finalize_model`'s output under calibration -- landed for the resume point.

    The contract it buys is pinned in tests/test_pipelines/test_resume_contracts.py;
    what is pinned here is the one way this entry can be wrong on its own.
    """

    def test_entry_is_a_model_adapter_dataset(self):
        d = _load_catalog()
        assert "trained_model" in d
        assert d["trained_model"]["type"] == "ModelAdapterDataset"

    def test_sidecar_isolated_from_model_and_hpo_best_model(self):
        """ModelAdapterDataset writes model_meta.json next to its filepath.

        Three ModelAdapterDatasets sharing a directory would overwrite each
        other's sidecar -- and the sidecar carries the ``calibrated`` flag, so
        the cross-talk decides how the model is later *loaded*.
        """
        d = _load_catalog()
        dirs = {
            name: Path(d[name]["filepath"]).parent
            for name in ("model", "hpo_best_model", "trained_model")
        }
        assert len(set(dirs.values())) == 3, dirs
