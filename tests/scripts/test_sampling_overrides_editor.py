"""Tests for sampling_overrides_editor script."""

import json

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from scripts.sampling_overrides_editor import (
    aggregate_surfaces,
    app,
    floor_weight,
    grid_to_yaml,
    profile_stats,
    render_html,
    resolve_keys,
    suggest_ratio,
    two_factor_weights,
)


# ---------------------------------------------------------------------------
# Pure suggestion formulas (D8)
# ---------------------------------------------------------------------------
class TestSuggestRatio:
    def test_downsamples_negatives_to_target_ratio(self):
        # n_pos=10, n_neg=100, R=5 -> 5*10/100 = 0.5
        assert suggest_ratio(n_pos=10, n_neg=100, target_neg_pos=5) == 0.5

    def test_already_balanced_clamps_to_one(self):
        # 5*50/100 = 2.5 -> clamp 1.0
        assert suggest_ratio(n_pos=50, n_neg=100, target_neg_pos=5) == 1.0

    def test_zero_negatives_returns_one(self):
        assert suggest_ratio(n_pos=10, n_neg=0, target_neg_pos=5) == 1.0


class TestFloorWeight:
    def test_lifts_to_target_rate(self):
        # n_pos=61, n_neg_post=2_340_904, t=1/6 -> 61*5/2340904
        v = floor_weight(61, 2_340_904, 1 / 6)
        assert abs(v - 61 * 5 / 2_340_904) < 1e-15
        # floored neg mass = n_neg_post * v = n_pos*(1-t)/t = 61*5 = 305
        assert abs(2_340_904 * v - 305) < 1e-6

    def test_independent_of_phi_via_post_count(self):
        # same floored mass (=305) regardless of how much was downsampled
        for phi in (1.0, 0.1, 0.02):
            nnp = round(phi * 2_340_904)
            assert abs(nnp * floor_weight(61, nnp, 1 / 6) - 305) < 1.0

    def test_zero_pos_or_zero_neg_keeps_negatives(self):
        assert floor_weight(0, 1000, 1 / 6) == 1.0
        assert floor_weight(50, 0, 1 / 6) == 1.0


class TestTwoFactorWeights:
    def test_effective_pos_rate_equals_t(self):
        out = two_factor_weights(61, 2_340_904, t=1 / 6, alpha=0.5, m_min=180)
        assert abs(out["eff_pos_rate"] - 1 / 6) < 1e-9

    def test_floor_logit_equalized_across_cells(self):
        # two very different products land on the same post-weight floor logit
        import math
        a = two_factor_weights(23601, 2_317_364, t=1 / 6, alpha=0.5, m_min=180)
        b = two_factor_weights(61, 2_340_904, t=1 / 6, alpha=0.5, m_min=180)
        la = math.log(23601 * a["w_pos"] / (2_317_364 * a["w_neg"]))
        lb = math.log(61 * b["w_pos"] / (2_340_904 * b["w_neg"]))
        assert abs(la - lb) < 1e-9
        assert abs(la - math.log((1 / 6) / (1 - 1 / 6))) < 1e-9

    def test_attention_reference_cell_is_one(self):
        # m == m_min (the least-positive cell, m=n_pos/t) -> A == 1
        m_min = 30 / (1 / 6)
        out = two_factor_weights(30, 1_800_000, t=1 / 6, alpha=0.5, m_min=m_min)
        assert abs(out["A"] - 1.0) < 1e-9

    def test_hotter_cell_down_weighted(self):
        m_min = 30 / (1 / 6)
        cold = two_factor_weights(30, 1_800_000, t=1 / 6, alpha=0.5, m_min=m_min)
        hot = two_factor_weights(23601, 2_317_364, t=1 / 6, alpha=0.5, m_min=m_min)
        assert hot["A"] < cold["A"] <= 1.0

    def test_alpha_zero_no_attention(self):
        out = two_factor_weights(23601, 2_317_364, t=1 / 6, alpha=0.0, m_min=180)
        assert out["A"] == 1.0

    def test_zero_positive_cell_neutral(self):
        out = two_factor_weights(0, 500_000, t=1 / 6, alpha=0.5, m_min=180)
        assert out == {"w_pos": 1.0, "w_neg": 1.0, "v": 1.0, "A": 1.0,
                       "m": 500_000.0, "eff_pos_rate": 0.0}


# ---------------------------------------------------------------------------
# Config-driven column resolution (no hardcoded column names)
# ---------------------------------------------------------------------------
class TestResolveKeys:
    _SCHEMA = {"columns": {"item": "prod_name", "label": "label",
                           "time": "snap_date"}}

    def test_case1_weight_label_split(self):
        # group=[seg,item,label], weight=[item,label] -> weight_dims=[item]
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["prod_name", "label"]}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name"]
        assert out["weight_dims"] == ["prod_name"]
        assert out["label_col"] == "label"
        assert out["time_col"] == "snap_date"
        assert out["weight_keys"] == ["prod_name", "label"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name"]

    def test_case2_weight_adds_carry_dim_extends_union(self):
        # weight=[risk_attr,item,label] adds risk_attr to the union (label excl.)
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["risk_attr", "prod_name", "label"]},
            self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name"]
        assert out["weight_dims"] == ["risk_attr", "prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name", "risk_attr"]

    def test_empty_weight_keys_union_is_ratio_dims(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {}, self._SCHEMA)
        assert out["weight_keys"] == []
        assert out["weight_dims"] == []
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name"]

    def test_accepts_segment_label_only(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "label"]},
            {"sample_weight_keys": []}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ"]
        assert out["union_dims"] == ["cust_segment_typ"]

    def test_accepts_item_label_only(self):
        out = resolve_keys(
            {"sample_group_keys": ["prod_name", "label"]},
            {"sample_weight_keys": ["prod_name", "label"]}, self._SCHEMA)
        assert out["ratio_dims"] == ["prod_name"]
        assert out["weight_dims"] == ["prod_name"]
        assert out["union_dims"] == ["prod_name"]

    def test_accepts_multi_dim(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name",
                                   "risk_attr", "label"]},
            {"sample_weight_keys": []}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name", "risk_attr"]

    def test_accepts_label_only_global(self):
        out = resolve_keys(
            {"sample_group_keys": ["label"]},
            {"sample_weight_keys": []}, self._SCHEMA)
        assert out["ratio_dims"] == []
        assert out["union_dims"] == []

    def test_weight_label_at_any_position(self):
        # label need not be last in weight keys
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["prod_name", "label", "risk_attr"]},
            self._SCHEMA)
        assert out["weight_dims"] == ["prod_name", "risk_attr"]

    def test_rejects_group_keys_without_label(self):
        with pytest.raises(ValueError, match="label"):
            resolve_keys({"sample_group_keys": ["cust_segment_typ", "prod_name"]},
                         {"sample_weight_keys": ["prod_name", "label"]}, self._SCHEMA)

    def test_rejects_weight_keys_without_label(self):
        # label is the pos/neg split axis: required in non-empty weight keys
        with pytest.raises(ValueError, match="label"):
            resolve_keys(
                {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
                {"sample_weight_keys": ["prod_name"]}, self._SCHEMA)

    def test_rejects_missing_schema_column(self):
        with pytest.raises(ValueError, match="schema.columns"):
            resolve_keys(
                {"sample_group_keys": ["seg", "prod_name", "label"]},
                {"sample_weight_keys": ["prod_name"]},
                {"columns": {"item": "prod_name", "label": "label"}})


# ---------------------------------------------------------------------------
# Sparse JSON -> YAML with A5/A9 validation
# ---------------------------------------------------------------------------
def _params(weight_keys=("prod_name", "label"),
            group_keys=("cust_segment_typ", "prod_name", "label")):
    return {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["a", "b"]}},
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]},
                    "sample_group_keys": list(group_keys)},
        "training": {"sample_weight_keys": list(weight_keys)},
    }


def _export(ratio_rows, weight_rows, *, group_keys=None, weight_keys=None):
    return {
        "sample_group_keys": group_keys or ["cust_segment_typ", "prod_name", "label"],
        # None sentinel (not []) so a caller can pass [] to mean "no weight keys".
        "sample_weight_keys": (weight_keys if weight_keys is not None
                               else ["prod_name", "label"]),
        "ratio_rows": ratio_rows,
        "weight_rows": weight_rows,
    }


class TestGridToYaml:
    def test_sparse_emits_only_non_default(self):
        # weight rows carry w_pos/w_neg; each cell emits |1 and |0 entries,
        # only those != default_weight (1.0).
        export = _export(
            ratio_rows=[
                {"keys": ["mass", "a"], "ratio": 0.5},
                {"keys": ["mass", "b"], "ratio": 1.0}],
            weight_rows=[{"keys": ["a"], "w_pos": 1.0, "w_neg": 1.0},
                         {"keys": ["b"], "w_pos": 0.7, "w_neg": 0.2}])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.5}}
        assert sw == {"sample_weights": {"b|1": 0.7, "b|0": 0.2}}

    def test_weight_emits_pos_and_neg_per_cell(self):
        export = _export(
            ratio_rows=[],
            weight_rows=[{"keys": ["a"], "w_pos": 0.7, "w_neg": 0.001},
                         {"keys": ["b"], "w_pos": 1.0, "w_neg": 0.5}])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        sw = yaml.safe_load(out["sample_weights_yaml"])
        # a|1, a|0, b|0 emitted; b|1 == default 1.0 dropped
        assert sw == {"sample_weights": {"a|1": 0.7, "a|0": 0.001, "b|0": 0.5}}

    def test_weight_key_joined_in_weight_keys_order(self):
        # weight_keys = [risk_attr, prod_name, label] -> "lo|a|1" / "lo|a|0"
        export = _export(
            ratio_rows=[],
            weight_rows=[{"keys": ["lo", "a"], "w_pos": 2.0, "w_neg": 0.5}],
            weight_keys=["risk_attr", "prod_name", "label"])
        out = grid_to_yaml(
            export, _params(weight_keys=("risk_attr", "prod_name", "label")),
            default_ratio=1.0)
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert sw == {"sample_weights": {"lo|a|1": 2.0, "lo|a|0": 0.5}}

    def test_weight_label_at_any_position(self):
        # label in the middle: sample_weight_keys = [prod_name, label, risk_attr]
        export = _export(
            ratio_rows=[],
            weight_rows=[{"keys": ["a", "lo"], "w_pos": 0.7, "w_neg": 0.2}],
            weight_keys=["prod_name", "label", "risk_attr"])
        out = grid_to_yaml(
            export, _params(weight_keys=("prod_name", "label", "risk_attr")),
            default_ratio=1.0)
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert sw == {"sample_weights": {"a|1|lo": 0.7, "a|0|lo": 0.2}}

    def test_unknown_product_ratio_raises(self):
        export = _export(
            ratio_rows=[{"keys": ["mass", "zzz"], "ratio": 0.5}],
            weight_rows=[])
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)

    def test_unknown_product_weight_only_raises_with_real_weight_keys(self):
        # A9c probe uses the REAL sample_weight_keys; the unknown product in the
        # reconstructed key (zzz|1) must be caught.
        export = _export(
            ratio_rows=[],
            weight_rows=[{"keys": ["zzz"], "w_pos": 0.5, "w_neg": 0.1}])
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)

    def test_export_keys_must_match_config(self):
        export = _export(ratio_rows=[], weight_rows=[],
                         weight_keys=["cust_segment_typ", "label"])
        with pytest.raises(ValueError, match="sample_weight_keys"):
            grid_to_yaml(export, _params(weight_keys=("prod_name", "label")),
                         default_ratio=1.0)

    def test_zero_pos_group_override_round_trips(self):
        # A 0-positive product ("a", present in schema) downsampled to 0.3 must
        # survive to config. grid_to_yaml has no n_pos visibility, so this also
        # documents that it must never special-case "cold" products away.
        export = _export(
            ratio_rows=[{"keys": ["mass", "a"], "ratio": 0.3}],
            weight_rows=[])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.3}}

    def test_no_segment_group_keys_reconstructs_key(self):
        gk = ["prod_name", "label"]
        export = _export(
            ratio_rows=[{"keys": ["a"], "ratio": 0.5}], weight_rows=[],
            group_keys=gk)
        out = grid_to_yaml(export, _params(group_keys=gk), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"a|0": 0.5}}

    def test_multi_dim_group_keys_reconstructs_key(self):
        gk = ["cust_segment_typ", "prod_name", "risk_attr", "label"]
        export = _export(
            ratio_rows=[{"keys": ["mass", "a", "lo"], "ratio": 0.5}],
            weight_rows=[], group_keys=gk)
        out = grid_to_yaml(export, _params(group_keys=gk), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|lo|0": 0.5}}


# ---------------------------------------------------------------------------
# Self-contained HTML renderer
# ---------------------------------------------------------------------------
class TestRenderHtml:
    _STATS = [
        {"cust_segment_typ": "mass", "prod_name": "a", "n_pos": 200,
         "n_neg": 4000},
        {"cust_segment_typ": "hnw", "prod_name": "a", "n_pos": 8, "n_neg": 50},
    ]
    _KW = dict(ratio_dims=["cust_segment_typ", "prod_name"],
               group_keys=["cust_segment_typ", "prod_name", "label"],
               weight_keys=["prod_name", "label"], weight_dims=["prod_name"],
               label_col="label", default_ratio=1.0)

    def test_self_contained_and_embeds_stats_and_keys(self):
        html = render_html(self._STATS, **self._KW)
        assert html.lstrip().startswith("<!DOCTYPE html>")
        assert json.dumps(self._STATS) in html
        assert 'const GKEYS=["cust_segment_typ", "prod_name"]' in html
        assert 'const GROUP_KEYS=["cust_segment_typ", "prod_name", "label"]' in html
        assert 'const WDIMS=["prod_name"]' in html
        assert 'const LABEL="label"' in html
        assert 'const WKEYS=["prod_name", "label"]' in html
        assert "http://" not in html and "https://" not in html
        assert "Export JSON" in html and "Export YAML" in html

    def test_two_factor_weight_surface_and_knobs(self):
        html = render_html(self._STATS, **self._KW, t=0.1666, alpha=0.5)
        assert "let T=0.1666" in html and "let ALPHA=0.5" in html
        assert "function floorWeight(" in html and "function twoFactor(" in html
        # global knob inputs recompute the weight surface
        assert "id=t" in html and "id=alpha" in html and "function onKnob(" in html
        # two-factor diagnostic columns
        assert "有效負" in html and "eff pos" in html and "A·m" in html
        # no legacy cold-boost knob remains
        assert "WMAX" not in html and "suggestWeight" not in html

    def test_two_tabs_ratio_and_weight(self):
        html = render_html(self._STATS, **self._KW)
        assert "function setTab(" in html
        assert "setTab('ratio')" in html and "setTab('weight')" in html

    def test_builds_ratio_store_and_keep_rate(self):
        html = render_html(self._STATS, **self._KW)
        assert "function buildRatio(" in html
        # keep-rate mirrors suggest_ratio: clamp(nm*n_pos/n_neg,0,1)
        assert "function keepRate(" in html
        assert "nm*np/nn" in html

    def test_keep_rate_and_preview_guard_zero_positive_cell(self):
        # Mirrors Python aggregate_surfaces: a zero-positive cell keeps ALL
        # negatives (ratio 1.0) rather than deriving 0 from neg_mult*0/n_neg.
        html = render_html(self._STATS, **self._KW)
        assert "np<=0" in html          # keepRate guard
        assert "r.n_pos<=0" in html     # preview guard

    def test_weight_tab_recomputes_post_downsample_from_ratio_edits(self):
        html = render_html(self._STATS, **self._KW)
        assert "function rebuildWeight(" in html
        # per-(seg,item) effective ratio projected onto fine cells
        assert "function ratioByKey(" in html
        # n_neg_post accumulates n_neg * projected ratio
        assert "s.n_neg*rbk.get(" in html
        # rebuildWeight runs when entering the weight tab
        assert "rebuildWeight()" in html

    def test_neg_mult_is_primary_knob_ratio_readonly(self):
        html = render_html(self._STATS, **self._KW, target_neg_pos=5.0)
        assert "負樣本倍率" in html
        assert "data-k=neg_mult" in html
        # positive rows keep a read-only (calc) ratio cell; 0-positive rows make
        # ratio editable via data-k=ratio_direct (see the zero-pos tests below).
        assert 'class="calc rt"' in html
        assert "實際倍率" in html and "function achMult(" in html
        assert "const R=5.0" in html
        assert "td.warn" in html and "已全留" in html

    def test_export_emits_self_describing_object(self):
        html = render_html(self._STATS, **self._KW)
        assert "function exp(" in html
        # cell ratio key reconstructed by walking GROUP_KEYS (label pos -> '0')
        assert "function ratioKey(" in html
        assert "k===LABEL?'0'" in html
        # weight key reconstructed via WKEYS with label slot -> |1 / |0
        assert "function weightKey(" in html
        assert "weightKey(r.keys,'1')" in html and "weightKey(r.keys,'0')" in html
        assert "sample_group_keys" in html and "sample_weight_keys" in html
        assert "ratio_rows" in html and "weight_rows" in html

    def test_empty_weight_dims_hides_weight_tab(self):
        html = render_html(self._STATS, ratio_dims=["cust_segment_typ", "prod_name"],
                           group_keys=["cust_segment_typ", "prod_name", "label"],
                           weight_keys=[], weight_dims=[], label_col="label",
                           default_ratio=1.0)
        assert "const WDIMS=[]" in html
        # weight tab disabled when no weight dims configured
        assert "WDIMS.length" in html

    def test_edits_survive_sort_and_filter(self):
        html = render_html(self._STATS, **self._KW)
        assert "function syncEdits(" in html and "syncEdits()" in html
        assert "function sortBy(" in html and "function flt(" in html

    def test_explains_logic_with_configured_values(self):
        html = render_html(self._STATS, **self._KW,
                           target_neg_pos=3.0, alpha=0.7, t=0.2)
        assert "sample_ratio_overrides" in html and "sample_weights" in html
        assert "3.0" in html and "0.7" in html and "0.2" in html
        assert "http://" not in html and "https://" not in html

    def test_escapes_cell_values_and_threads_label_col(self):
        html = render_html(self._STATS, ratio_dims=["cust_segment_typ", "prod_name"],
                           group_keys=["cust_segment_typ", "prod_name", "label"],
                           weight_keys=["prod_name", "label"], weight_dims=["prod_name"],
                           label_col="label", default_ratio=1.0)
        assert "function esc(" in html
        assert "r.keys.map(v=>" in html and "esc(v)" in html
        assert 'const LABEL="label"' in html
        assert "sample_group_keys:GROUP_KEYS" in html

    def test_zero_pos_ratio_cell_editable(self):
        # 0-positive rows: ratio column becomes directly editable (data-k
        # ratio_direct), the neg:pos multiplier column greys out.
        html = render_html(self._STATS, **self._KW)
        assert "data-k=ratio_direct" in html
        assert "r.ratio_direct=1" in html  # buildRatio seeds the default

    def test_zero_pos_preview_reads_direct_keep_rate(self):
        # preview() noPos branch must derive ratio from r.ratio_direct, not
        # pin it to a hard-coded 1.0000 literal.
        html = render_html(self._STATS, **self._KW)
        assert "parseFloat(r.ratio_direct)" in html
        # recalc must NOT write back into the ratio cell while it is the one
        # being edited (would wash the cursor).
        assert "if(!editingRatio) tr.querySelector('td.rt')" in html

    def test_help_text_describes_zero_pos_editable_ratio(self):
        html = render_html(self._STATS, **self._KW)
        # stale claim ("維持 ratio 1.0") must be gone; new wording present.
        assert "維持 ratio 1.0" not in html
        assert "neg:pos 無定義" in html

    def test_summary_panel_present_and_groups_by_dim(self):
        html = render_html(self._STATS, **self._KW)
        assert "function renderSummary(" in html
        assert "function initSummary(" in html
        assert "id=grp" in html or 'id="grp"' in html
        assert "id=summary" in html or 'id="summary"' in html
        # grand total + per-group pos_rate roll-up over RATIO via preview()
        assert "a.np/t" in html
        # recomputed live on every cell edit
        assert "renderSummary()" in html

    def test_ratio_input_mode_toggle_present(self):
        html = render_html(self._STATS, **self._KW)
        assert "function setRmode(" in html
        assert "let RMODE='mult'" in html
        assert "依負樣本倍率" in html and "依保留率" in html
        assert 'name="rmode"' in html
        # keep-mode editable column branch + n_pos=0 fallback preserved
        assert "RMODE==='keep'" in html
        assert "r.n_pos<=0" in html


# ---------------------------------------------------------------------------
# Spark profiling aggregation
# ---------------------------------------------------------------------------
@pytest.mark.spark
class TestProfileStats:
    def _df(self, spark):
        return spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6],
            "prod_name": ["a", "a", "a", "b", "b", "b"],
            "cust_segment_typ": ["mass", "mass", "mass", "hnw", "hnw", "hnw"],
            "risk_attr": ["lo", "lo", "hi", "lo", "hi", "hi"],
            "label": [1, 0, 0, 1, 1, 0],
        }))

    def test_groups_by_union_dims_and_counts_pos_neg(self, spark):
        stats = profile_stats(
            self._df(spark), [pd.Timestamp("2025-01-31")],
            union_dims=["cust_segment_typ", "prod_name"],
            label_col="label", time_col="snap_date")
        d = {(r["cust_segment_typ"], r["prod_name"]): (r["n_pos"], r["n_neg"])
             for r in stats}
        assert d[("mass", "a")] == (1, 2)
        assert d[("hnw", "b")] == (2, 1)

    def test_groups_at_finer_union_with_extra_dim(self, spark):
        stats = profile_stats(
            self._df(spark), [pd.Timestamp("2025-01-31")],
            union_dims=["cust_segment_typ", "prod_name", "risk_attr"],
            label_col="label", time_col="snap_date")
        # every returned row carries all three union dims plus counts
        assert all({"cust_segment_typ", "prod_name", "risk_attr",
                    "n_pos", "n_neg"} <= set(r) for r in stats)
        d = {(r["cust_segment_typ"], r["prod_name"], r["risk_attr"]):
             (r["n_pos"], r["n_neg"]) for r in stats}
        # mass|a|lo: rows (1,label1),(2,label0) -> n_pos1 n_neg1
        assert d[("mass", "a", "lo")] == (1, 1)

    def test_missing_union_column_raises(self, spark):
        with pytest.raises(ValueError, match="not in"):
            profile_stats(
                self._df(spark), [pd.Timestamp("2025-01-31")],
                union_dims=["cust_segment_typ", "prod_name", "no_such_col"],
                label_col="label", time_col="snap_date")


# ---------------------------------------------------------------------------
# aggregate_surfaces: ratio + weight surfaces, downsample-coupled projection
# ---------------------------------------------------------------------------
class TestAggregateSurfaces:
    # 4 fine cells over (segment, item); weight grain = [item]
    _STATS = [
        {"cust_segment_typ": "mass", "prod_name": "a", "n_pos": 100, "n_neg": 9000},
        {"cust_segment_typ": "hnw",  "prod_name": "a", "n_pos": 60,  "n_neg": 500},
        {"cust_segment_typ": "mass", "prod_name": "b", "n_pos": 80,  "n_neg": 2000},
        {"cust_segment_typ": "hnw",  "prod_name": "b", "n_pos": 0,   "n_neg": 40},
    ]
    T = 1 / 6  # (1-t)/t = 5

    def test_ratio_downsample_then_weight_floor(self):
        # neg_mult: mass|a=5 (ratio=clamp(5*100/9000)=0.0556), others keep-all
        nm = {("mass", "a"): 5.0, ("hnw", "a"): 1e9,
              ("mass", "b"): 1e9, ("hnw", "b"): 1e9}
        out = aggregate_surfaces(
            self._STATS, nm, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_dims=["prod_name"], alpha=0.5, t=self.T, default_neg_mult=5.0)
        rr = {tuple(r["keys"]): r for r in out["ratio_rows"]}
        assert abs(rr[("mass", "a")]["ratio"] - (5 * 100 / 9000)) < 1e-9
        assert rr[("mass", "a")]["kept_neg"] == 500
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        # prod a: n_pos 100+60=160; n_neg_post 9000*0.0556+500=1000
        assert wr[("a",)]["n_pos"] == 160
        assert wr[("a",)]["n_neg_post"] == 1000
        assert wr[("b",)]["n_neg_post"] == 2040
        # floor v lifts both to t; v_a = 160*5/1000 = 0.8
        assert abs(wr[("a",)]["v"] - 0.8) < 1e-9
        assert abs(wr[("a",)]["eff_pos_rate"] - self.T) < 1e-9
        assert abs(wr[("b",)]["eff_pos_rate"] - self.T) < 1e-9
        # floored neg mass = n_neg_post*v = n_pos*(1-t)/t = 160*5=800 / 80*5=400
        assert wr[("a",)]["floored_neg_mass"] == 800
        assert wr[("b",)]["floored_neg_mass"] == 400
        # attention reference = least-positive cell (prod b, n_pos 80) -> A=1
        assert abs(wr[("b",)]["A"] - 1.0) < 1e-9
        assert wr[("a",)]["A"] < 1.0          # hotter -> down-weighted
        assert abs(wr[("a",)]["w_pos"] - wr[("a",)]["A"]) < 1e-6  # w_pos rounded 6dp
        assert abs(wr[("a",)]["w_neg"] - wr[("a",)]["A"] * 0.8) < 1e-6

    def test_cross_dimension_shares_ratio_over_dropped_dim(self):
        # weight_dims=[risk_attr, item]; risk_attr dropped when projecting to the
        # ratio (segment,item) cell, so both risk values share ratio[mass,a].
        stats = [
            {"cust_segment_typ": "mass", "prod_name": "a", "risk_attr": "lo",
             "n_pos": 60, "n_neg": 6000},
            {"cust_segment_typ": "mass", "prod_name": "a", "risk_attr": "hi",
             "n_pos": 40, "n_neg": 3000},
        ]
        nm = {("mass", "a"): 5.0}   # ratio = 5*(60+40)/(6000+3000) = 0.05556
        out = aggregate_surfaces(
            stats, nm, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_dims=["risk_attr", "prod_name"], alpha=0.5, t=self.T,
            default_neg_mult=5.0)
        ratio = 5 * 100 / 9000
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        assert wr[("lo", "a")]["n_neg_post"] == round(6000 * ratio)
        assert wr[("hi", "a")]["n_neg_post"] == round(3000 * ratio)
        assert wr[("lo", "a")]["keys"] == ["lo", "a"]
        # both sub-cells still lifted to t
        assert abs(wr[("lo", "a")]["eff_pos_rate"] - self.T) < 1e-9
        assert abs(wr[("hi", "a")]["eff_pos_rate"] - self.T) < 1e-9

    def test_zero_positive_weight_cell_is_neutral(self):
        # a weight cell with no positives keeps negatives untouched (v=A=1)
        stats = [
            {"cust_segment_typ": "mass", "prod_name": "a", "n_pos": 50, "n_neg": 900},
            {"cust_segment_typ": "mass", "prod_name": "z", "n_pos": 0, "n_neg": 400},
        ]
        out = aggregate_surfaces(
            stats, {}, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_dims=["prod_name"], alpha=0.5, t=self.T, default_neg_mult=1e9)
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        assert wr[("z",)]["v"] == 1.0 and wr[("z",)]["A"] == 1.0
        assert wr[("z",)]["w_pos"] == 1.0 and wr[("z",)]["w_neg"] == 1.0

    def test_empty_weight_dims_yields_no_weight_rows(self):
        out = aggregate_surfaces(
            self._STATS, {}, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_dims=[], alpha=0.5, t=self.T, default_neg_mult=5.0)
        assert out["weight_rows"] == []
        assert len(out["ratio_rows"]) == 4

    def test_ratio_surface_by_segment_only(self):
        # ratio_dims=[segment]: products collapse into one row per segment
        nm = {("mass",): 1e9, ("hnw",): 1e9}   # keep-all
        out = aggregate_surfaces(
            self._STATS, nm, ratio_dims=["cust_segment_typ"],
            weight_dims=[], alpha=0.5, t=self.T, default_neg_mult=5.0)
        rr = {tuple(r["keys"]): r for r in out["ratio_rows"]}
        assert rr[("mass",)]["n_pos"] == 180 and rr[("mass",)]["n_neg"] == 11000
        assert rr[("hnw",)]["n_pos"] == 60 and rr[("hnw",)]["n_neg"] == 540

    def test_ratio_surface_global_single_row(self):
        out = aggregate_surfaces(
            self._STATS, {}, ratio_dims=[], weight_dims=[],
            alpha=0.5, t=self.T, default_neg_mult=1e9)
        assert len(out["ratio_rows"]) == 1
        r = out["ratio_rows"][0]
        assert r["keys"] == []
        assert r["n_pos"] == 240 and r["n_neg"] == 11540

    def test_decoupled_phi_one_uses_raw_negatives(self):
        # neg_mult would downsample under coupled; decoupled must ignore ratio and
        # use raw n_neg (phi=1) as the floor's negative base.
        stats = [
            {"prod_name": "a", "n_pos": 100, "n_neg": 1000},
            {"prod_name": "b", "n_pos": 50, "n_neg": 4000},
        ]
        nm = {("a",): 1.0, ("b",): 1.0}  # coupled ratio = clamp(1*npos/nneg) < 1
        out = aggregate_surfaces(
            stats, nm, ratio_dims=["prod_name"], weight_dims=["prod_name"],
            alpha=0.5, t=self.T, default_neg_mult=1.0,
            neg_base="decoupled", phi=1.0)
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        assert wr[("a",)]["n_neg_post"] == 1000   # raw, not downsampled
        assert wr[("b",)]["n_neg_post"] == 4000

    def test_decoupled_phi_scales_raw_negatives(self):
        stats = [{"prod_name": "a", "n_pos": 100, "n_neg": 1000}]
        out = aggregate_surfaces(
            stats, {("a",): 1.0}, ratio_dims=["prod_name"],
            weight_dims=["prod_name"], alpha=0.5, t=self.T,
            default_neg_mult=1.0, neg_base="decoupled", phi=0.2)
        row = out["weight_rows"][0]
        assert row["n_neg_post"] == round(1000 * 0.2)   # 200


# ---------------------------------------------------------------------------
# Typer CLI (to-yaml end-to-end; profile's Spark path covered above)
# ---------------------------------------------------------------------------
class TestToYamlCli:
    def _write_params(self, tmp_path):
        params = tmp_path / "p.yaml"
        # schema.columns only needs 'item' here; label/time default in get_schema.
        params.write_text(
            "schema:\n  columns:\n    item: prod_name\n"
            "  categorical_values:\n    prod_name: [a, b]\n"
            "dataset:\n  prepare_model_input:\n"
            "    categorical_columns: [prod_name]\n"
            "  sample_group_keys: [cust_segment_typ, prod_name, label]\n")
        train = tmp_path / "t.yaml"
        train.write_text("training:\n  sample_weight_keys: [prod_name, label]\n")
        return params, train

    def test_to_yaml_prints_both_blocks(self, tmp_path):
        export = {
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_weight_keys": ["prod_name", "label"],
            "ratio_rows": [{"keys": ["mass", "a"], "ratio": 0.5}],
            "weight_rows": [{"keys": ["b"], "w_pos": 0.7, "w_neg": 0.2}],
        }
        jf = tmp_path / "e.json"
        jf.write_text(json.dumps(export))
        params, train = self._write_params(tmp_path)
        r = CliRunner().invoke(app, [
            "to-yaml", str(jf), "--params", str(params),
            "--train-params", str(train), "--base-params", str(params)])
        assert r.exit_code == 0, r.output
        assert "sample_ratio_overrides:" in r.output and "mass|a|0" in r.output
        assert "sample_weights:" in r.output
        assert "b|1" in r.output and "b|0" in r.output
