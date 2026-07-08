"""offset_sweep 單元測試。

交錯 fixture 設計（見 _interleaved_pdf）：每個 item 同時有正例與負例、
與其他 item 的列以 0.02 logit 的緊margin 交錯排列——乾淨資料下排序是
唯一最優（任何 item 任何方向的非零平移都會翻掉至少一對、mAP 嚴格變差），
所以 δ* 必須全零。對 A 注入 +1.0 後，完整復原的 δ_A 平台是 (−1.02, −0.98)
——上邊界＝A 正例跌破 B 負例、下邊界＝A 負例仍壓著 C 正例——grid
（step 0.05）落在平台內的點只有 −1.00，故 δ*_A 恰等於 −1.0，不靠容差。
"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.offset_sweep import sweep


def _sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.asarray(z, dtype=np.float64)))


def _params(inject=None, **sweep_overrides):
    cfg = {
        "enabled": True,
        "shrink_lambda": 0.1,
        "holdout_fraction": 0.5,
        "max_rounds": 5,
        "grid": {"lo": -2.0, "hi": 2.0, "step": 0.05},
    }
    cfg.update(sweep_overrides)
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"],
                "item": "prod_name", "label": "label",
                "score": "score", "rank": "rank",
            },
        },
        "evaluation": {
            "metric": {"weight_alpha": 0.0, "k": None,
                       "min_positives": 0, "shrinkage_k": 0},
            "diagnosis": {
                "sample": {"max_queries": 200000,
                           "min_pos_queries_per_item": 50, "seed": 42},
                "offset_sweep": cfg,
                "debug_inject_offsets": dict(inject or {}),
            },
        },
    }


def _interleaved_pdf(n_queries=12):
    """每 query 六列（logit 降冪）：
    A+ 1.00 > B- 0.98 > B+ 0.50 > C- 0.48 > C+ 0.10 > A- 0.08。

    每個 item 都有正例與負例，相鄰對的 margin 都是 0.02——乾淨資料下任何
    item 任何方向的非零平移（|δ| ≥ 0.05 一格）都會翻掉至少一對、mAP 嚴格
    變差，δ* 被釘在 0。對 A 注入 +1.0：A+ 2.00、A- 1.08，正例掉到
    rank 1/4/6；復原平台 δ_A ∈ (-1.02, -0.98)——δ_A > -1.02 保 A+ 在
    B-（0.98）之上、δ_A < -0.98 讓 A-（1.08+δ）回到 C+（0.10）之下。
    """
    rows = []
    for q in range(n_queries):
        cust = f"c{q:03d}"
        rows += [
            ("20260131", cust, "A", 1, _sigmoid(1.00)),
            ("20260131", cust, "B", 0, _sigmoid(0.98)),
            ("20260131", cust, "B", 1, _sigmoid(0.50)),
            ("20260131", cust, "C", 0, _sigmoid(0.48)),
            ("20260131", cust, "C", 1, _sigmoid(0.10)),
            ("20260131", cust, "A", 0, _sigmoid(0.08)),
        ]
    return pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "label", "score"]
    )


class TestKnownAnswerInjection:
    def test_injected_offset_recovered_exactly_on_plateau_fixture(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["delta_star"]["A"] == pytest.approx(-1.0)
        assert out["delta_star"]["B"] == 0.0
        assert out["delta_star"]["C"] == 0.0

    def test_holdout_map_improves_under_injection(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["map_holdout"]["star"] > out["map_holdout"]["zero"]
        assert out["recovered_gap_holdout"] == pytest.approx(
            out["map_holdout"]["star"] - out["map_holdout"]["zero"]
        )

    def test_injection_echoed_and_noted(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["injected_offsets"] == {"A": 1.0}
        assert any("debug_inject_offsets" in n for n in out["notes"])

    def test_unknown_injection_key_noted(self):
        out = sweep(_interleaved_pdf(), _params(inject={"nosuch": 1.0}))
        assert any("nosuch" in n for n in out["notes"])

    def test_loo_contribution_positive_for_recovered_item(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert out["per_item"]["A"]["loo_contribution_holdout"] > 0
        # δ*=0 的 item 不算 LOO（恆 0）
        assert out["per_item"]["B"]["loo_contribution_holdout"] is None

    def test_interaction_residual_closes_the_bridge(self):
        out = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        loo_sum = sum(
            v["loo_contribution_holdout"] or 0.0
            for v in out["per_item"].values()
        )
        assert out["interaction_residual_holdout"] == pytest.approx(
            out["recovered_gap_holdout"] - loo_sum
        )


class TestCleanData:
    def test_clean_data_all_deltas_zero(self):
        out = sweep(_interleaved_pdf(), _params())
        assert all(v == 0.0 for v in out["delta_star"].values())
        assert out["recovered_gap_holdout"] == pytest.approx(0.0)

    def test_converges_before_max_rounds(self):
        out = sweep(_interleaved_pdf(), _params())
        assert out["converged"] is True
        assert out["n_rounds_run"] <= 2


class TestMechanics:
    def test_deterministic(self):
        a = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        b = sweep(_interleaved_pdf(), _params(inject={"A": 1.0}))
        assert a == b

    def test_holdout_split_counts(self):
        out = sweep(_interleaved_pdf(n_queries=12), _params())
        assert out["n_queries_fit"] + out["n_queries_holdout"] == 12
        assert out["n_queries_holdout"] == 6  # round(12 * 0.5)

    def test_single_query_leaves_holdout_empty_without_crash(self):
        out = sweep(_interleaved_pdf(n_queries=1), _params())
        assert out["n_queries_holdout"] == 0
        assert out["map_holdout"]["zero"] is None
        assert any("折外" in n or "holdout" in n.lower() for n in out["notes"])

    def test_empty_sample_returns_stub_shape(self):
        out = sweep(_interleaved_pdf().iloc[0:0], _params())
        assert out["delta_star"] == {}
        assert out["notes"]

    def test_score_outside_unit_interval_skips_logit_with_note(self):
        pdf = _interleaved_pdf()
        pdf["score"] = pdf["score"] * 10.0  # 超出 (0,1)
        out = sweep(pdf, _params())
        assert any("logit" in n for n in out["notes"])

    def test_metric_params_and_config_echoed(self):
        out = sweep(_interleaved_pdf(), _params())
        assert out["metric_params"] == {
            "k": None, "weight_alpha": 0.0,
            "min_positives": 0, "shrinkage_k": 0.0,
        }
        assert out["params"]["shrink_lambda"] == 0.1
        assert out["score_col_used"] == "score"

    def test_grid_without_exact_zero_still_reaches_zero(self):
        # lo=-0.07, step 0.05 → grid 無精確 0，實作須插入 0.0
        out = sweep(_interleaved_pdf(), _params(grid={"lo": -0.07, "hi": 0.08,
                                                      "step": 0.05}))
        assert all(v == 0.0 for v in out["delta_star"].values())

    def test_tie_break_prefers_delta_closest_to_zero_under_flat_objective(self):
        # shrink_lambda=0 + 細 grid（step 0.01）→ 復原平台 (-1.02, -0.98)
        # 內有三個並列格點 {-1.01, -1.00, -0.99}（mAP_fit 相同、無懲罰可
        # 拉開差距），B/C 的任一微小 δ 只要不翻轉任何一對也同樣並列於 0。
        # 若拿掉「候選按 |g| 升冪、僅嚴格改善才換」的偏 0 排序，coordinate
        # descent 會鎖定離 0 較遠的並列點（A 落在 -1.01 而非 -0.99、
        # B/C 飄離 0）。
        out = sweep(_interleaved_pdf(), _params(
            inject={"A": 1.0}, shrink_lambda=0.0,
            grid={"lo": -2.0, "hi": 2.0, "step": 0.01},
        ))
        assert out["delta_star"]["A"] == pytest.approx(-0.99)
        assert out["delta_star"]["B"] == 0.0
        assert out["delta_star"]["C"] == 0.0

    def test_null_query_key_rows_get_defined_fold_assignment(self):
        # query 鍵含 null 的列自成一組（groupby dropna=False）——預設
        # dropna=True 會給 ngroup 代碼 -1，hold_flag[-1] 靜默繞到最後一組。
        pdf = _interleaved_pdf()
        extra = pdf.iloc[:6].copy()
        extra["cust_id"] = None
        out = sweep(pd.concat([pdf, extra], ignore_index=True), _params())
        assert out["n_queries_fit"] + out["n_queries_holdout"] == 13
