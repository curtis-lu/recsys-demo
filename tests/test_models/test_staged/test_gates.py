import numpy as np
import pandas as pd
import pytest

from recsys_tfb.models.staged.gates import StagedGateError, check_stage1_gates

GATES = {"max_groups": 3, "min_rows": 4, "min_positives": 1, "min_negatives": 1}


def _split(labels, y):
    return pd.Series(labels), np.array(y)


class TestCheckStage1Gates:
    def test_healthy_groups_pass(self):
        tr = _split(["a"] * 4 + ["b"] * 4, [0, 1, 0, 1, 0, 1, 0, 1])
        # NOTE：計畫原稿此處 dev 只給每群 2 列（["a","a","b","b"]），但
        # min_rows=4 依實作（見 gates.py）同時套用到 train 與 train_dev 兩個
        # split——2 列必掛 rows=2<4，令這個「healthy」案例本身先天不可能通過。
        # 最小調整：讓 dev 每群列數與 train 對齊（4 列），使其名副其實地
        # 通過所有既定閘門（intent 不變，只修正 fixture 數字）。
        dev = _split(["a"] * 4 + ["b"] * 4, [0, 1, 0, 1, 0, 1, 0, 1])
        check_stage1_gates(tr, dev, GATES)  # 不 raise

    def test_too_many_groups_fails(self):
        tr = _split(["a", "b", "c", "d"], [0, 1, 0, 1])
        dev = tr
        with pytest.raises(StagedGateError, match="max_groups"):
            check_stage1_gates(tr, dev, GATES)

    def test_group_missing_positives_in_dev_fails(self):
        tr = _split(["a"] * 4, [0, 1, 0, 1])
        dev = _split(["a", "a"], [0, 0])  # dev 無正例
        with pytest.raises(StagedGateError, match="positives"):
            check_stage1_gates(tr, dev, GATES)

    def test_collect_all_reports_every_bad_group(self):
        tr = _split(["a"] * 4 + ["b"] * 2, [0, 0, 0, 0, 1, 1])
        # a: 無正例；b: 列數不足＋無負例 → 錯誤訊息須同時含 a 與 b
        dev = _split(["a", "b"], [0, 1])
        with pytest.raises(StagedGateError) as exc:
            check_stage1_gates(tr, dev, GATES)
        assert "'a'" in str(exc.value) and "'b'" in str(exc.value)

    def test_group_only_in_dev_fails(self):
        # dev 出現 train 沒有的群：無模型可訓，必須擋
        tr = _split(["a"] * 4, [0, 1, 0, 1])
        dev = _split(["a", "z"], [0, 1])
        with pytest.raises(StagedGateError, match="'z'"):
            check_stage1_gates(tr, dev, GATES)
