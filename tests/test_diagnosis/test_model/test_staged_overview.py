"""staged 診斷分派 helper ＋ Stage-1 總覽表（Task 4，PR-C Batch B）。"""

import numpy as np

from recsys_tfb.diagnosis.model.staged import (
    compute_stage1_overview, has_stage2, is_staged, model_scores,
    resolve_attribution_inputs,
)


class _SharedLike:
    def predict(self, X):
        return np.full(len(X), 0.5)


class TestDispatchHelpers:
    def test_shared_adapter_is_not_staged(self):
        m = _SharedLike()
        assert not is_staged(m) and not has_stage2(m)

    def test_passthrough_for_shared(self):
        m = _SharedLike()
        X = np.zeros((3, 2))
        X_eff, cols = resolve_attribution_inputs(m, None, X, ["a", "b"])
        assert X_eff is X and cols == ["a", "b"]
        assert np.allclose(model_scores(m, None, X), 0.5)


class TestStage1Overview:
    REPORT = {"partition_keys": ["prod_name"],
              "groups": {"b": {"n_rows": 10, "n_pos": 3, "score": 0.7,
                               "metric": "auc", "train_seconds": 1.5,
                               "best_params": {"num_leaves": 7}},
                         "a": {"n_rows": 20, "n_pos": 0, "score": 0.6,
                               "metric": "auc", "train_seconds": 0.5,
                               "best_params": {}}}}

    def test_rows_sorted_and_totals_add_up(self):
        out = compute_stage1_overview(self.REPORT, {"model_version": "t"})
        assert [r["group"] for r in out["groups"]] == ["a", "b"]
        assert out["n_groups"] == 2 and out["total_rows"] == 30
        assert out["total_positives"] == 3
        assert out["groups"][1]["pos_rate"] == 0.3
        assert "stage2" not in out

    def test_stage2_summary_attached_when_present(self):
        s2 = {"mode": "lambdarank", "oof_folds": 5, "oof_rows": 100,
              "n_groups": 2, "best_params": {"num_leaves": 9}, "extra": "x"}
        out = compute_stage1_overview(self.REPORT, {"model_version": "t"}, s2)
        assert out["stage2"]["mode"] == "lambdarank"
        assert "extra" not in out["stage2"]
