"""pair_ledger 單元測試。

手算錨（設計定案）：query 排序 [B−, A+, C+]（rank 1..3）
- ΔAP(B→A)：a=1,b=2,P_a=0,P_b=1 → 1/1 − 1/2 = 0.5
- ΔAP(B→C)：a=1,b=3,P_a=0,P_b=2，中間正例 rank2 → 1/1 − 2/3 + 1/2 = 5/6
substitution 錨：B（全負，base_rate→clip→logit≈−27.6）沉底 →
q1 變 [A+, C+]、q2 變 [A+] → per-item A=1, C=1 → mAP=1.0。
"""
import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.pair_ledger import (
    pair_ledger, substitution_ablation,
)


def _params(k=None, inject=None, segment_columns=None):
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            }
        },
        "evaluation": {
            "metric": {"k": k},
            "segment_columns": segment_columns or [],
            "diagnosis": {
                "pair_ledger": {"enabled": True},
                "debug_inject_offsets": inject or {},
            },
        },
    }


def _ledger_pdf():
    """兩個 query 的手算 fixture（score 是 (0,1) 分數，logit 單調保名次）。

    q1（cust 1, seg X）：B− 0.9 > A+ 0.8 > C+ 0.7
    q2（cust 2, seg Y）：B− 0.9 > A+ 0.8 > C− 0.7
    已知答案：matrix B→A {2 pairs, 1.0}、B→C {1, 5/6}；n_mis=3；
    map_current = mean(A=(1/2+1/2)/2=0.5, C=2/3) = 7/12。
    """
    rows = [
        ("2026-01-31", 1, "B", 0, 0.9, "X"),
        ("2026-01-31", 1, "A", 1, 0.8, "X"),
        ("2026-01-31", 1, "C", 1, 0.7, "X"),
        ("2026-01-31", 2, "B", 0, 0.9, "Y"),
        ("2026-01-31", 2, "A", 1, 0.8, "Y"),
        ("2026-01-31", 2, "C", 0, 0.7, "Y"),
    ]
    return pd.DataFrame(
        rows,
        columns=["snap_date", "cust_id", "prod_name", "label", "score",
                 "seg"],
    )


class TestKnownAnswerMatrix:
    def test_pair_deltas_match_hand_computation(self):
        out = pair_ledger(_ledger_pdf(), _params())
        assert out["n_queries"] == 2
        assert out["n_pos_rows"] == 3
        assert out["n_mis_ordered_pairs"] == 3
        m = out["matrix"]
        assert m["B"]["A"]["pair_count"] == 2
        np.testing.assert_allclose(m["B"]["A"]["dap_sum"], 1.0)
        assert m["B"]["C"]["pair_count"] == 1
        np.testing.assert_allclose(m["B"]["C"]["dap_sum"], 5.0 / 6.0)
        assert list(m) == ["B"]  # 只有 B 當過壓制者

    def test_marginals_and_shares(self):
        out = pair_ledger(_ledger_pdf(), _params())
        sup = out["by_suppressor"]["B"]
        assert sup["pair_count"] == 3
        np.testing.assert_allclose(sup["dap_sum"], 11.0 / 6.0)
        np.testing.assert_allclose(sup["dap_share"], 1.0)
        vic = out["by_victim"]
        np.testing.assert_allclose(vic["A"]["dap_sum"], 1.0)
        np.testing.assert_allclose(vic["A"]["dap_share"], 6.0 / 11.0)
        np.testing.assert_allclose(vic["C"]["dap_share"], 5.0 / 11.0)

    def test_positive_item_above_is_not_a_suppressor(self):
        # q1 的 C+ 上方有 A+（正例）——不記帳（Bob 效應）。
        out = pair_ledger(_ledger_pdf(), _params())
        assert "A" not in out["by_suppressor"]

    def test_k_truncation_changes_ledger_currency(self):
        # k=1：每對 swap 都是「把正例抬進 top-1」→ ΔAP 恆 1.0。
        out = pair_ledger(_ledger_pdf(), _params(k=1))
        np.testing.assert_allclose(
            out["by_suppressor"]["B"]["dap_sum"], 3.0
        )


class TestSubstitution:
    def test_substituting_pure_negative_item_recovers_full_map(self):
        out = substitution_ablation(_ledger_pdf(), _params())
        np.testing.assert_allclose(out["map_current"], 7.0 / 12.0)
        sub_b = out["substitution"]["B"]
        assert sub_b["base_rate"] == 0.0
        np.testing.assert_allclose(sub_b["map_substituted"], 1.0)
        np.testing.assert_allclose(
            sub_b["delta_vs_current"], 5.0 / 12.0
        )  # 正值＝B 的個性化分數是淨傷害

    def test_umbrella_merges_substitution_block(self):
        out = pair_ledger(_ledger_pdf(), _params())
        assert "substitution" in out and "map_current" in out
        np.testing.assert_allclose(out["map_current"], 7.0 / 12.0)


class TestBySegment:
    def test_harm_grouped_by_segment(self):
        out = pair_ledger(
            _ledger_pdf(), _params(segment_columns=["seg"])
        )
        seg = out["by_segment"]["seg"]
        assert seg["X"]["n_pos_rows"] == 2
        assert seg["X"]["n_suppressed_pos_rows"] == 2
        np.testing.assert_allclose(seg["X"]["dap_sum"], 4.0 / 3.0)
        np.testing.assert_allclose(seg["X"]["dap_share"], 8.0 / 11.0)
        assert seg["Y"]["n_pos_rows"] == 1
        np.testing.assert_allclose(seg["Y"]["dap_sum"], 0.5)

    def test_missing_segment_column_noted_and_skipped(self):
        out = pair_ledger(
            _ledger_pdf(), _params(segment_columns=["nope"])
        )
        assert out["by_segment"] == {}
        assert any("nope" in n for n in out["notes"])


class TestInjection:
    def test_injection_creates_suppression_for_injected_item(self):
        base = pair_ledger(_ledger_pdf(), _params())
        assert "C" not in base["by_suppressor"]
        out = pair_ledger(_ledger_pdf(), _params(inject={"C": 5.0}))
        # C− 在 q2 被抬到頂 → 壓 A+；q1 的 C+ 抬頂不造成傷害。
        assert out["by_suppressor"]["C"]["pair_count"] == 1
        np.testing.assert_allclose(
            out["by_suppressor"]["C"]["dap_sum"], 2.0 / 3.0
        )
        assert out["injected_offsets"] == {"C": 5.0}
        assert any("debug_inject_offsets 生效" in n for n in out["notes"])

    def test_substitution_measures_post_injection_state(self):
        out = substitution_ablation(
            _ledger_pdf(), _params(inject={"C": 5.0})
        )
        # 注入後現狀：q1 [C+,B−,A+]→A=2/3、C=1；q2 [C−,B−,A+]→A=1/3
        # per-item A=(2/3+1/3)/2=1/2, C=1 → map_current=3/4
        np.testing.assert_allclose(out["map_current"], 0.75)


class TestMechanics:
    def test_deterministic_and_row_order_invariant(self):
        pdf = _ledger_pdf()
        shuffled = pdf.sample(frac=1.0, random_state=3).reset_index(drop=True)
        a = pair_ledger(pdf, _params(segment_columns=["seg"]))
        b = pair_ledger(shuffled, _params(segment_columns=["seg"]))
        assert a["matrix"] == b["matrix"]
        assert a["by_segment"] == b["by_segment"]
        np.testing.assert_allclose(a["map_current"], b["map_current"],
                                   rtol=1e-12)

    def test_empty_sample_returns_stub_shape(self):
        out = pair_ledger(_ledger_pdf().iloc[0:0], _params())
        assert out["n_queries"] == 0
        assert out["matrix"] == {} and out["substitution"] == {}
        assert out["map_current"] is None
        assert any("抽樣為空" in n for n in out["notes"])

    def test_score_outside_unit_interval_noted(self):
        pdf = _ledger_pdf()
        pdf["score"] = pdf["score"] * 10.0  # 超出 (0,1)
        out = pair_ledger(pdf, _params())
        assert any("略過 logit" in n for n in out["notes"])

    def test_metric_params_echoed(self):
        out = pair_ledger(_ledger_pdf(), _params(k=3))
        assert out["metric_params"]["k"] == 3
        assert out["score_col_used"] == "score"
