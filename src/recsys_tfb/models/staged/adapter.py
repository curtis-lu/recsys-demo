"""StagedModelAdapter: N stage-1 boosters behind the ModelAdapter contract.

Bundle layout under the model_version dir (filepath = <dir>/model.txt):
    model.txt          groups index JSON — written LAST (= bundle commit mark)
    stage1/<slug>.txt  one LightGBM booster per group
    stage1/.bundle_id  uuid; must equal index["bundle_id"] at load
    stage2/model.txt   optional single stage-2 booster (spec §10 PR-B)
    stage2/.bundle_id  uuid; must equal index["bundle_id"] at load (same
                       atomicity contract as stage1/)

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

# stage2 imported lazily inside predict_routed（見該處註解）：stage2.py 頂層
# 拉入 core.group_utils，而 core/__init__ -> core.catalog -> io.model_
# adapter_dataset -> models.base 這條鏈在 models 套件自己 __init__ 執行期間
# 尚未跑完，模組頂層 import 會撞循環匯入（同 lightgbm_adapter.py 頂部註解
# 講的同一類問題）。

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
        self._stage2 = None
        self._stage2_meta: dict = {}

    # ---- assembly（train_staged_model 編排用） ----
    def add_group(self, group_key: str, adapter: LightGBMAdapter,
                  meta: dict) -> None:
        self._groups[group_key] = adapter
        self._group_meta[group_key] = dict(meta)

    def set_partition_keys(self, partition_keys: list) -> None:
        self._partition_keys = list(partition_keys)

    def set_stage2(self, adapter, meta: dict) -> None:
        """Attach the stage-2 booster (train_stage2_model 編排用)."""
        self._stage2 = adapter
        self._stage2_meta = dict(meta)

    @property
    def stage2_mode(self) -> str:
        return (self._stage2_meta.get("mode", "none")
                if self._stage2 is not None else "none")

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

    def _stage1_scores(
        self, X: np.ndarray, keys: np.ndarray, on_missing: str,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Route rows to their group's stage-1 booster (no stage-2 compose).

        Shared by ``predict_routed`` and ``stage2_matrix_for``: keys length
        check, per-group scoring loop, missing-group bookkeeping and the
        raise/skip branch — verbatim extraction, no behavior change.
        """
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

    def predict_routed(
        self, X: np.ndarray, keys: np.ndarray, on_missing: str = "raise",
    ) -> tuple[np.ndarray, np.ndarray]:
        """Route rows to their group's booster.

        Returns (scores, valid_mask); missing-group rows get NaN score and
        False mask. on_missing: "raise" (evaluation path) | "skip"
        (inference path; stats in self.last_missing_stats).

        Precondition: ``keys`` must be a pre-stringified, non-null object
        array — every production call site builds it via
        ``models.staged.partition.routing_keys`` (
        ``io.extract._composite_key_series(...).astype(str)`` under the
        hood), so lookups against ``self._groups`` (keyed by the same
        str-joined convention) match byte-for-byte. Passing an array with
        NaN/None entries or mixed types directly (bypassing
        ``routing_keys``) is undefined behavior — this method does not
        normalize or validate key contents beyond the length check below.
        """
        if on_missing not in ("raise", "skip"):
            raise ValueError(f"on_missing must be raise|skip, got {on_missing!r}")
        keys = np.asarray(keys, dtype=object)
        scores, mask = self._stage1_scores(X, keys, on_missing)
        if self._stage2 is not None:
            # lazy import：見模組頂部關於 stage2.py 循環匯入的註解
            from recsys_tfb.models.staged.stage2 import (
                encode_group_codes, group_code_lookup, stage2_matrix,
            )

            valid = np.flatnonzero(mask)
            if valid.size:
                lookup = group_code_lookup(self._groups)
                gcodes = encode_group_codes(keys[valid], lookup)
                X2 = stage2_matrix(X[valid], scores[valid], gcodes)
                scores[valid] = self._stage2.predict(X2)
        return scores, mask

    def stage2_matrix_for(self, X: np.ndarray, keys: np.ndarray) -> np.ndarray:
        """診斷用：全列 [X | stage-1 分數 | gcode]（missing group 一律 raise）。
        與 predict_routed 的 compose 塊同一套 stage2.py helper，欄序同
        stage2_matrix。"""
        from recsys_tfb.models.staged.stage2 import (
            encode_group_codes, group_code_lookup, stage2_matrix,
        )

        if self._stage2 is None:
            raise NotImplementedError("stage2_matrix_for requires a stage-2 model")
        scores, _ = self._stage1_scores(X, np.asarray(keys, dtype=object), "raise")
        lookup = group_code_lookup(self._groups)
        gcodes = encode_group_codes(np.asarray(keys, dtype=object), lookup)
        return stage2_matrix(X, scores, gcodes)

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
        stage2_dir = version_dir / "stage2"
        if self._stage2 is not None:
            tmp2 = version_dir / f"stage2.tmp-{bundle_id}"
            tmp2.mkdir()
            self._stage2.save(str(tmp2 / "model.txt"))
            (tmp2 / ".bundle_id").write_text(bundle_id)
            if stage2_dir.exists():
                shutil.rmtree(stage2_dir)
            tmp2.replace(stage2_dir)
        elif stage2_dir.exists():
            shutil.rmtree(stage2_dir)  # 前一輪 bundle 殘留，不得與新 index 誤配
        index = {
            "index_version": _INDEX_VERSION,
            "bundle_id": bundle_id,
            "partition_keys": self._partition_keys,
            "groups": {
                key: {"slug": slugs[key], **self._group_meta.get(key, {})}
                for key in sorted(self._groups)
            },
            "stage2": dict(self._stage2_meta) if self._stage2 is not None else None,
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
        stage2_meta = index.get("stage2")
        stage2_dir = index_path.parent / "stage2"
        if stage2_meta is not None:
            id2 = stage2_dir / ".bundle_id"
            if not stage2_dir.is_dir():
                problems.append("stage2/ directory missing")
            elif not id2.exists():
                problems.append("stage2/.bundle_id missing")
            elif id2.read_text().strip() != index.get("bundle_id"):
                problems.append(
                    "bundle_id mismatch between index and stage2/ "
                    "(mixed bundle)")
            elif not (stage2_dir / "model.txt").exists():
                problems.append("stage2 model file missing")
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
        if stage2_meta is not None:
            s2 = LightGBMAdapter()
            s2.load(str(stage2_dir / "model.txt"))
            self._stage2 = s2
            self._stage2_meta = dict(stage2_meta)
        else:
            self._stage2 = None
            self._stage2_meta = {}
        self._partition_keys = list(index.get("partition_keys", []))

    # ---- 其餘 ModelAdapter 契約 ----
    def train(self, X_train, y_train, X_val, y_val, params: dict) -> None:
        raise NotImplementedError(
            "staged training is orchestrated by the train_staged_model node, "
            "not the adapter (needs per-row partition keys)."
        )

    @property
    def booster(self):
        """attribution._resolve_booster 契約：stage2 存在＝診斷掛 Stage-2 booster。"""
        if self._stage2 is None:
            raise NotImplementedError(
                "staged(stage2=none) has no single booster; per-group "
                "diagnostics iterate the group adapters instead")
        return self._stage2.booster

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        if self._stage2 is None:
            raise NotImplementedError(
                "staged(stage2=none) importance is per-group; see "
                "compute_staged_group_diagnostics")
        return self._stage2.feature_importance(kind)

    def log_to_mlflow(self) -> None:
        if self._stage2 is not None:
            self._stage2.log_to_mlflow()
        else:
            logger.info(
                "staged(stage2=none): per-group boosters live in the model "
                "bundle; no single MLflow model is logged")

    def prepare_train_inputs(self, *args, **kwargs):
        raise NotImplementedError(
            "staged mode does not use the shared lgb .bin prepare layer."
        )


ADAPTER_REGISTRY["staged"] = StagedModelAdapter
