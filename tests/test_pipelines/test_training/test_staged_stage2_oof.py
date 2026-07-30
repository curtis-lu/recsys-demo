import numpy as np
import pytest

from recsys_tfb.pipelines.training.staged_stage2 import (
    _group_oof, _load_oof_checkpoint, _write_oof_checkpoint,
)

PARAMS = {"objective": "binary", "metric": "binary_logloss", "verbosity": -1,
          "num_threads": 1, "num_leaves": 5, "learning_rate": 0.2,
          "num_iterations": 15, "early_stopping_rounds": 5}


def _toy_group(n=120, seed=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < 0.35).astype(int)
    X = np.column_stack([rng.normal(loc=y), rng.normal(size=n)])
    w = np.ones(n)
    folds = np.arange(n) % 3  # 每折都有正負例（seed 固定，已驗）
    return X, y, w, folds


class TestGroupOof:
    def test_every_row_scored_and_deterministic(self):
        X, y, w, folds = _toy_group()
        a = _group_oof("A", X, y, w, folds, X[:30], y[:30], np.ones(30), PARAMS, [], 3)
        b = _group_oof("A", X, y, w, folds, X[:30], y[:30], np.ones(30), PARAMS, [], 3)
        assert np.isfinite(a).all() and len(a) == len(y)
        np.testing.assert_allclose(a, b)

    def test_oof_scores_differ_from_full_fit(self):
        # 結構性驗證「走了哪條路」：OOF 分數不得等於全量 fit 的自評分數
        # （若 fold 遮罩沒生效，兩者位元相同）
        from recsys_tfb.models.staged.train_stage1 import _fit_adapter
        X, y, w, folds = _toy_group()
        oof = _group_oof("A", X, y, w, folds, X[:30], y[:30], np.ones(30), PARAMS, [], 3)
        full = _fit_adapter(X, y, w, X[:30], y[:30], dict(PARAMS), [])
        assert not np.allclose(oof, full.predict(X))

    def test_leakage_guard_raises_on_uncovered_rows(self):
        X, y, w, folds = _toy_group()
        with pytest.raises(RuntimeError, match="OOF"):
            # n_folds=2 但 folds 含 2 → fold-2 的列沒人評 → guard 炸
            _group_oof("A", X, y, w, folds, X[:30], y[:30], np.ones(30), PARAMS, [], 2)

    def test_per_fold_heartbeat_logged(self, caplog):
        # Observability 要求 #1：逐折必須留時間戳
        import logging
        X, y, w, folds = _toy_group()
        with caplog.at_level(logging.INFO):
            _group_oof("A", X, y, w, folds, X[:30], y[:30], np.ones(30), PARAMS, [], 3)
        assert caplog.text.count("OOF group='A' fold=") == 3  # 每折一條
        assert "fold=1/3" in caplog.text and "fit_rows=" in caplog.text

    def test_fit_set_excludes_held_out_fold(self, monkeypatch):
        # 結構性驗證 fit_mask 真的排除該折（非數值比較——見 known-pitfalls
        # 類推論：`test_oof_scores_differ_from_full_fit`／determinism 兩測試
        # 對「fit_mask 誤用 pred_mask」（整段 in-fold leakage）並不敏感，因為
        # leakage guard 只核對 bookkeeping（producing_fold==folds），不核對
        # 實際訓練集是否排除該折；且兩者用不同計算路徑，"not allclose"／相等
        # 斷言對這個 mutation 恆真/恆假，測不出差異。改用 spy 直接斷言傳給
        # `_fit_adapter` 的 fit-set 列數。
        import recsys_tfb.pipelines.training.staged_stage2 as mod
        X, y, w, folds = _toy_group()
        fit_sizes = []
        real = mod._fit_adapter

        def spy(X_tr, y_tr, w_tr, X_dev, y_dev, params, cat_idx, w_dev=None):
            fit_sizes.append(len(y_tr))
            return real(X_tr, y_tr, w_tr, X_dev, y_dev, params, cat_idx,
                        w_dev=w_dev)

        monkeypatch.setattr(mod, "_fit_adapter", spy)
        _group_oof("A", X, y, w, folds, X[:30], y[:30], np.ones(30), PARAMS, [], 3)
        fold_sizes = [int(c) for c in np.bincount(folds, minlength=3) if c > 0]
        expected = sorted(len(y) - c for c in fold_sizes)
        assert sorted(fit_sizes) == expected


class TestOofCheckpoint:
    def test_roundtrip(self, tmp_path):
        scores = np.array([0.1, 0.9, 0.5])
        _write_oof_checkpoint(tmp_path / "oof", scores, 3, 5, 42)
        got = _load_oof_checkpoint(tmp_path / "oof", 3, 5, 42)
        np.testing.assert_allclose(got, scores)

    def test_absent_or_mismatched_returns_none(self, tmp_path):
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 5, 42) is None
        _write_oof_checkpoint(tmp_path / "oof", np.zeros(3), 3, 5, 42)
        assert _load_oof_checkpoint(tmp_path / "oof", 4, 5, 42) is None
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 4, 42) is None
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 5, 43) is None

    def test_no_success_marker_returns_none(self, tmp_path):
        _write_oof_checkpoint(tmp_path / "oof", np.zeros(3), 3, 5, 42)
        (tmp_path / "oof" / "_SUCCESS").unlink()
        assert _load_oof_checkpoint(tmp_path / "oof", 3, 5, 42) is None
