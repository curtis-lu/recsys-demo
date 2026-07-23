"""StagedModelAdapter: N stage-1 boosters behind the ModelAdapter contract.

Bundle layout under the model_version dir (filepath = <dir>/model.txt):
    model.txt          groups index JSON — written LAST (= bundle commit mark)
    stage1/<slug>.txt  one LightGBM booster per group
    stage1/.bundle_id  uuid; must equal index["bundle_id"] at load

Atomicity (spec §4, three cheap moves): stage1 written to a tmp dir then
os.replace()'d into place; the index (model.txt) written last; load verifies
bundle_id + file set and fails fast on any mix.
"""

import json
import logging
import shutil
import uuid
from pathlib import Path

import numpy as np

from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged.partition import group_slug

logger = logging.getLogger(__name__)

_INDEX_VERSION = 1


class StagedMissingGroupError(Exception):
    """Scoring rows reference partition groups with no trained model."""


class StagedModelAdapter(ModelAdapter):
    def __init__(self) -> None:
        self._groups: dict[str, LightGBMAdapter] = {}
        self._group_meta: dict[str, dict] = {}
        self._partition_keys: list[str] = []
        self.last_missing_stats: dict[str, int] = {}

    # ---- assembly（train_staged_model 編排用） ----
    def add_group(self, group_key: str, adapter: LightGBMAdapter,
                  meta: dict) -> None:
        self._groups[group_key] = adapter
        self._group_meta[group_key] = dict(meta)

    def set_partition_keys(self, partition_keys: list) -> None:
        self._partition_keys = list(partition_keys)

    @property
    def partition_keys(self) -> list[str]:
        return list(self._partition_keys)

    @property
    def group_keys(self) -> list[str]:
        return sorted(self._groups)

    # ---- predict ----
    def predict(self, X: np.ndarray) -> np.ndarray:
        raise NotImplementedError(
            "StagedModelAdapter cannot route from features alone; call "
            "predict_routed(X, keys, on_missing=...) with per-row partition "
            "key values (see pipelines' staged branches)."
        )

    def predict_routed(
        self, X: np.ndarray, keys: np.ndarray, on_missing: str = "raise",
    ) -> tuple[np.ndarray, np.ndarray]:
        """Route rows to their group's booster.

        Returns (scores, valid_mask); missing-group rows get NaN score and
        False mask. on_missing: "raise" (evaluation path) | "skip"
        (inference path; stats in self.last_missing_stats).
        """
        if on_missing not in ("raise", "skip"):
            raise ValueError(f"on_missing must be raise|skip, got {on_missing!r}")
        keys = np.asarray(keys, dtype=object)
        if len(keys) != len(X):
            raise ValueError(
                f"keys length {len(keys)} != X rows {len(X)}")
        scores = np.full(len(X), np.nan, dtype=np.float64)
        mask = np.zeros(len(X), dtype=bool)
        missing: dict[str, int] = {}
        for key in np.unique(keys):
            idx = keys == key
            adapter = self._groups.get(key)
            if adapter is None:
                missing[str(key)] = int(idx.sum())
                continue
            scores[idx] = adapter.predict(X[idx])
            mask[idx] = True
        self.last_missing_stats = missing
        if missing and on_missing == "raise":
            detail = ", ".join(
                f"{k!r}: {n} row(s)" for k, n in sorted(missing.items()))
            raise StagedMissingGroupError(
                f"{len(missing)} partition group(s) have no trained model "
                f"({detail}) — evaluation data should share the training "
                "sample_pool build; a gap here signals drift or a wrong "
                "model_version"
            )
        if missing:
            logger.warning(
                "staged predict: skipped %d group(s) / %d row(s) with no "
                "model: %s",
                len(missing), sum(missing.values()), sorted(missing),
            )
        return scores, mask

    # ---- persistence ----
    def save(self, filepath: str) -> None:
        if not self._groups:
            raise RuntimeError("No stage-1 groups to save.")
        index_path = Path(filepath)
        version_dir = index_path.parent
        version_dir.mkdir(parents=True, exist_ok=True)
        bundle_id = uuid.uuid4().hex
        tmp_dir = version_dir / f"stage1.tmp-{bundle_id}"
        tmp_dir.mkdir()
        slugs: dict[str, str] = {}
        for key, adapter in self._groups.items():
            slug = group_slug(key)
            slugs[key] = slug
            adapter.save(str(tmp_dir / f"{slug}.txt"))
        (tmp_dir / ".bundle_id").write_text(bundle_id)
        final_dir = version_dir / "stage1"
        if final_dir.exists():
            shutil.rmtree(final_dir)          # 舊（可能殘缺的）bundle 清掉
        tmp_dir.replace(final_dir)            # 原子發布
        index = {
            "index_version": _INDEX_VERSION,
            "bundle_id": bundle_id,
            "partition_keys": self._partition_keys,
            "groups": {
                key: {"slug": slugs[key], **self._group_meta.get(key, {})}
                for key in sorted(self._groups)
            },
        }
        tmp_index = version_dir / f"model.txt.tmp-{bundle_id}"
        tmp_index.write_text(json.dumps(index, indent=2, ensure_ascii=False))
        tmp_index.replace(index_path)         # index 最後寫＝bundle commit
        logger.info(
            "staged bundle saved: %d group(s), bundle_id=%s, dir=%s",
            len(self._groups), bundle_id, version_dir,
        )

    def load(self, filepath: str) -> None:
        index_path = Path(filepath)
        index = json.loads(index_path.read_text())
        stage1_dir = index_path.parent / "stage1"
        problems: list[str] = []
        id_file = stage1_dir / ".bundle_id"
        if not stage1_dir.is_dir():
            problems.append("stage1/ directory missing")
        elif not id_file.exists():
            problems.append("stage1/.bundle_id missing")
        elif id_file.read_text().strip() != index.get("bundle_id"):
            problems.append(
                "bundle_id mismatch between index and stage1/ (mixed bundle)")
        groups = index.get("groups", {})
        for key, meta in groups.items():
            if not (stage1_dir / f"{meta['slug']}.txt").exists():
                problems.append(f"model file missing for group {key!r}")
        if problems:
            raise ValueError(
                "staged bundle failed integrity check: " + "; ".join(problems)
            )
        self._groups = {}
        self._group_meta = {}
        for key, meta in groups.items():
            adapter = LightGBMAdapter()
            adapter.load(str(stage1_dir / f"{meta['slug']}.txt"))
            self._groups[key] = adapter
            self._group_meta[key] = {
                k: v for k, v in meta.items() if k != "slug"}
        self._partition_keys = list(index.get("partition_keys", []))

    # ---- 其餘 ModelAdapter 契約 ----
    def train(self, X_train, y_train, X_val, y_val, params: dict) -> None:
        raise NotImplementedError(
            "staged training is orchestrated by the train_staged_model node, "
            "not the adapter (needs per-row partition keys)."
        )

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        raise NotImplementedError(
            "per-group diagnostics arrive with the diagnostics PR (PR-C)."
        )

    def log_to_mlflow(self) -> None:
        logger.info(
            "staged adapter: mlflow model logging deferred to PR-C "
            "(%d group(s))", len(self._groups),
        )

    def prepare_train_inputs(self, *args, **kwargs):
        raise NotImplementedError(
            "staged mode does not use the shared lgb .bin prepare layer."
        )


ADAPTER_REGISTRY["staged"] = StagedModelAdapter
