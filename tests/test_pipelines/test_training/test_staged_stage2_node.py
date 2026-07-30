import numpy as np
import pandas as pd
import pytest

from recsys_tfb.io.handles import ParquetHandle
from recsys_tfb.models.staged.adapter import StagedModelAdapter
from recsys_tfb.models.staged.gates import StagedGateError
from recsys_tfb.pipelines.training.staged import train_staged_model
from recsys_tfb.pipelines.training.staged_stage2 import train_stage2_model


def _write_parquet(tmp_path, name, pdf):
    p = tmp_path / f"{name}.parquet"
    pdf.to_parquet(p)
    return ParquetHandle(path=str(p))


def _pdf(n_per_group=80, groups=("A", "B"), seed=0):
    rng = np.random.default_rng(seed)
    frames = []
    for g in groups:
        y = (rng.random(n_per_group) < 0.3).astype(int)
        frames.append(pd.DataFrame({
            "snap_date": "2026-01-01",
            "cust_id": np.arange(n_per_group) % 20,  # 每 query 多列、entity 重複
            "prod_name": "p1",
            "f1": rng.normal(loc=y, size=n_per_group),
            "f2": rng.normal(size=n_per_group),
            "seg": g, "label": y,
        }))
    return pd.concat(frames, ignore_index=True)


def _parameters(mode="lambdarank", n_trials=0, oof_folds=3):
    return {
        "random_seed": 42,
        "search_id": "s2node01",
        "model_version": "mvtest",
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
            "num_iterations": 15, "early_stopping_rounds": 5,
            "n_trials": n_trials,
            "hpo_objective": "mean_ap",
            "search_space": [
                {"name": "num_leaves", "type": "int", "low": 4, "high": 8}],
            "model_structure": "staged",
            "staged": {
                "stage1": {"partition_keys": ["seg"], "objective": "binary",
                           "hpo": {"n_trials": 0, "metric": "auc",
                                   "search_space": []},
                           "params": {},
                           "gates": {"max_groups": 10, "min_rows": 10,
                                     "min_positives": 3, "min_negatives": 3},
                           "max_workers": 1},
                "stage2": {"mode": mode, "oof_folds": oof_folds,
                           "params": {}},
            },
        },
    }


PREPROC = {"feature_columns": ["f1", "f2"], "categorical_columns": [],
           "category_mappings": {}}


def _handles(tmp_path):
    tr = _write_parquet(tmp_path, "train", _pdf(seed=0))
    dev = _write_parquet(tmp_path, "dev", _pdf(n_per_group=40, seed=1))
    val = _write_parquet(tmp_path, "val", _pdf(n_per_group=40, seed=2))
    return tr, dev, val


def _stage1(tmp_path, params):
    tr, dev, _ = _handles(tmp_path)
    return train_staged_model(tr, dev, PREPROC, params,
                              wip_root=tmp_path / "wip")


class TestTrainStage2Model:
    def test_attaches_stage2_and_reports(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        model, report = train_stage2_model(
            m1, rep1, tr, dev, val, PREPROC, params,
            wip_root=tmp_path / "wip")
        assert model is m1 and model.stage2_mode == "lambdarank"
        assert report["mode"] == "lambdarank"
        assert report["oof_folds"] == 3
        assert report["oof_rows"] == 160
        X = np.random.default_rng(3).normal(size=(4, 2))
        keys = np.array(["A", "B", "A", "B"], dtype=object)
        scores, mask = model.predict_routed(X, keys)
        assert mask.all() and np.isfinite(scores).all()

    def test_binary_mode_also_works(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters(mode="binary")
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        model, report = train_stage2_model(
            m1, rep1, tr, dev, val, PREPROC, params,
            wip_root=tmp_path / "wip")
        assert model.stage2_mode == "binary"

    def test_oof_fold_fits_receive_dev_weights(self, tmp_path, monkeypatch):
        # 因果鏈：_one_group 對 dev 子集算權重 → _group_oof 轉交 → _fit_adapter
        # 的 dev Dataset 帶權（最後一段由 train_stage1 側的 spy 測試把關）。
        # 用 seg 權重表讓 dev 權重非全 1，斷言每個 fold fit 都收到對的值。
        import recsys_tfb.pipelines.training.staged_stage2 as mod

        monkeypatch.chdir(tmp_path)
        params = _parameters()
        params["training"]["sample_weight_keys"] = ["seg"]
        params["training"]["sample_weights"] = {"A": 2.0}
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)

        calls = []
        real = mod._fit_adapter

        def spy(*args, **kwargs):
            calls.append(dict(kwargs))
            return real(*args, **kwargs)

        monkeypatch.setattr(mod, "_fit_adapter", spy)
        train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                           wip_root=tmp_path / "wip")
        assert calls, "OOF 路徑沒有呼叫 _fit_adapter"
        weights = [k.get("w_dev") for k in calls]
        assert all(w is not None for w in weights), \
            "OOF fold fit 未帶 dev 權重（w_dev）"
        uniq = {float(v) for w in weights for v in np.unique(np.asarray(w))}
        assert uniq == {1.0, 2.0}  # seg=A 群 dev 全 2.0、seg=B 全 1.0

    def test_oof_checkpoint_restored_on_rerun(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                           wip_root=tmp_path / "wip")
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        def boom(*a, **kw):
            raise AssertionError("OOF 應全部從 checkpoint 還原，不得重算")
        monkeypatch.setattr(mod, "_group_oof", boom)
        m1b, rep1b = _stage1(tmp_path, params)  # stage-1 也走自己的 checkpoint
        model, _ = train_stage2_model(m1b, rep1b, tr, dev, val, PREPROC,
                                      params, wip_root=tmp_path / "wip")
        assert model.stage2_mode == "lambdarank"

    def test_oof_gate_failure_fails_fast(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        pdf = _pdf(seed=0)
        # B 群正例集中到單一 entity → 該 entity 所在折被留出時 fit set 無正例
        b = pdf["seg"] == "B"
        pdf.loc[b, "label"] = 0
        pdf.loc[b & (pdf["cust_id"] == 0), "label"] = 1
        tr = _write_parquet(tmp_path, "train2", pdf)
        _, dev, val = _handles(tmp_path)
        with pytest.raises(StagedGateError, match="OOF"):
            train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                               wip_root=tmp_path / "wip2")

    def test_val_missing_group_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, _ = _handles(tmp_path)
        val_pdf = _pdf(n_per_group=40, seed=2)
        val_pdf["seg"] = "ZZ"  # val 出現未訓練群 → eval 語意 fail-fast
        val = _write_parquet(tmp_path, "valzz", val_pdf)
        from recsys_tfb.models.staged.adapter import StagedMissingGroupError
        with pytest.raises(StagedMissingGroupError):
            train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                               wip_root=tmp_path / "wip")

    def test_observability_events_emitted(self, tmp_path, monkeypatch,
                                          caplog):
        # Observability 要求 #3/#4/#5：階段計時＋data volume＋峰值 RSS 打點
        import logging
        monkeypatch.chdir(tmp_path)
        params = _parameters()
        m1, rep1 = _stage1(tmp_path, params)
        tr, dev, val = _handles(tmp_path)
        with caplog.at_level(logging.INFO):
            train_stage2_model(m1, rep1, tr, dev, val, PREPROC, params,
                               wip_root=tmp_path / "wip")
        for expected in (
            "Step completed: stage2.load_train_frames",
            "Step completed: stage2.oof",
            "Step completed: stage2.build_matrices",
            "Step completed: stage2.tune",
            "stage2.pdf_tr",          # data volume
            "stage2.X2_tr",
            "peak RSS",               # 高水位打點
            "stage2.after_oof",
        ):
            assert expected in caplog.text, f"缺觀測點: {expected}"
