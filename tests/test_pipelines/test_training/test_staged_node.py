import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.gates import StagedGateError
from recsys_tfb.pipelines.training.staged import train_staged_model


def _write_parquet(tmp_path, name, pdf):
    p = tmp_path / f"{name}.parquet"
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


def _pdf(n_per_group=60, groups=("A", "B"), seed=0):
    rng = np.random.default_rng(seed)
    frames = []
    for gi, g in enumerate(groups):
        y = (rng.random(n_per_group) < 0.3).astype(int)
        frames.append(pd.DataFrame({
            "snap_date": "2026-01-01", "cust_id": np.arange(n_per_group),
            "prod_name": "p1",
            "f1": rng.normal(loc=y, size=n_per_group),
            "f2": rng.normal(size=n_per_group),
            "seg": g, "label": y,
        }))
    return pd.concat(frames, ignore_index=True)


def _parameters(**stage1_over):
    stage1 = {"partition_keys": ["seg"], "objective": "binary",
              "hpo": {"n_trials": 0, "metric": "auc", "search_space": []},
              "params": {}, "gates": {"max_groups": 10, "min_rows": 10,
                                      "min_positives": 3, "min_negatives": 3},
              "max_workers": 2}
    stage1.update(stage1_over)
    return {
        "random_seed": 42,
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label",
                               "score": "score", "rank": "rank"},
                   "categorical_values": {"prod_name": ["p1"]}},
        "dataset": {"carry_columns": ["seg"]},
        "training": {
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": "binary",
                                 "metric": "binary_logloss", "verbosity": -1,
                                 "num_threads": 1, "num_leaves": 5,
                                 "learning_rate": 0.2},
            "num_iterations": 20, "early_stopping_rounds": 5,
            "model_structure": "staged",
            "staged": {"stage1": stage1, "stage2": {"mode": "none"}},
        },
    }


PREPROC = {"feature_columns": ["f1", "f2"], "categorical_columns": [],
           "category_mappings": {}}


class TestTrainStagedModel:
    def test_returns_adapter_with_one_model_per_group(self, tmp_path):
        tr = _write_parquet(tmp_path, "train", _pdf(seed=0))
        dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=30, seed=1))
        model, _report = train_staged_model(tr, dev, PREPROC, _parameters(),
                                            wip_root=tmp_path / "wip")
        assert isinstance(model, StagedModelAdapter)
        assert model.group_keys == ["A", "B"]
        assert model.partition_keys == ["seg"]

    def test_gate_failure_propagates(self, tmp_path):
        pdf = _pdf(seed=0)
        pdf.loc[pdf["seg"] == "B", "label"] = 0  # B 群無正例
        tr = _write_parquet(tmp_path, "train", pdf)
        dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=30, seed=1))
        with pytest.raises(StagedGateError, match="'B'"):
            train_staged_model(tr, dev, PREPROC, _parameters(),
                               wip_root=tmp_path / "wip")

    def test_returns_groups_report(self, tmp_path):
        tr = _write_parquet(tmp_path, "train", _pdf(seed=0))
        dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=30, seed=1))
        _model, report = train_staged_model(tr, dev, PREPROC, _parameters(),
                                            wip_root=tmp_path / "wip")
        assert set(report["groups"]) == {"A", "B"}
        for meta in report["groups"].values():
            assert {"n_rows", "n_pos", "best_params", "score", "metric",
                    "train_seconds"} <= set(meta)

    def test_checkpoint_skips_completed_groups(self, tmp_path, caplog):
        tr = _write_parquet(tmp_path, "train", _pdf(seed=0))
        dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=30, seed=1))
        wip = tmp_path / "wip"
        m1, _ = train_staged_model(tr, dev, PREPROC, _parameters(),
                                   wip_root=wip)
        import logging
        with caplog.at_level(logging.INFO):
            m2, _ = train_staged_model(tr, dev, PREPROC, _parameters(),
                                       wip_root=wip)
        assert "checkpoint" in caplog.text  # 第二次跑必須報告跳過
        X = np.random.default_rng(3).normal(size=(4, 2))
        keys = np.array(["A", "B"] * 2, dtype=object)
        s1, _ = m1.predict_routed(X, keys, on_missing="raise")
        s2, _ = m2.predict_routed(X, keys, on_missing="raise")
        np.testing.assert_allclose(s1, s2)  # checkpoint 載回＝重訓同結果

    def test_parallel_equals_sequential(self, tmp_path):
        tr = _write_parquet(tmp_path, "train",
                            _pdf(groups=("A", "B", "C"), seed=0))
        dev = _write_parquet(tmp_path, "dev",
                             _pdf(n_per_group=30, groups=("A", "B", "C"), seed=1))
        # 分開的 wip root——共用會讓第二跑直接載 checkpoint，測不到平行度
        m_seq, _ = train_staged_model(tr, dev, PREPROC,
                                      _parameters(max_workers=1),
                                      wip_root=tmp_path / "wip1")
        m_par, _ = train_staged_model(tr, dev, PREPROC,
                                      _parameters(max_workers=3),
                                      wip_root=tmp_path / "wip2")
        X = np.random.default_rng(3).normal(size=(6, 2))
        keys = np.array(["A", "B", "C"] * 2, dtype=object)
        s1, _ = m_seq.predict_routed(X, keys, on_missing="raise")
        s2, _ = m_par.predict_routed(X, keys, on_missing="raise")
        np.testing.assert_allclose(s1, s2)  # 平行度不得影響結果（確定性）
