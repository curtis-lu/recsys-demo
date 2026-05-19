import math

import pytest

from recsys_tfb.tooling.sampling_suggest import suggest_ratio, suggest_weight


class TestSuggestRatio:
    def test_downsamples_negatives_to_target_ratio(self):
        # n_pos=10, n_neg=100, R=5 -> 5*10/100 = 0.5
        assert suggest_ratio(n_pos=10, n_neg=100, target_neg_pos=5) == 0.5

    def test_already_balanced_clamps_to_one(self):
        # 5*50/100 = 2.5 -> clamp 1.0
        assert suggest_ratio(n_pos=50, n_neg=100, target_neg_pos=5) == 1.0

    def test_zero_negatives_returns_one(self):
        assert suggest_ratio(n_pos=10, n_neg=0, target_neg_pos=5) == 1.0


class TestSuggestWeight:
    def test_inverse_frequency_with_sqrt_damping(self):
        # median=800, n_pos=200 -> (800/200)**0.5 = 2.0
        assert suggest_weight(n_pos=200, median_pos=800, alpha=0.5, w_max=5.0) == 2.0

    def test_hot_product_clamped_to_one(self):
        # n_pos >= median -> ratio<=1 -> clamp lower bound 1.0
        assert suggest_weight(n_pos=8000, median_pos=800, alpha=0.5, w_max=5.0) == 1.0

    def test_extreme_tail_capped_at_w_max(self):
        # (800/8)**0.5 = 10 -> cap 5.0
        assert suggest_weight(n_pos=8, median_pos=800, alpha=0.5, w_max=5.0) == 5.0

    def test_zero_pos_capped_at_w_max(self):
        assert suggest_weight(n_pos=0, median_pos=800, alpha=0.5, w_max=5.0) == 5.0


from recsys_tfb.tooling.sampling_suggest import build_grid


class TestBuildGrid:
    def test_grid_has_stats_and_suggestions_per_cell(self):
        # stats: list of (segment, product, n_pos, n_neg)
        stats = [
            ("mass", "a", 200, 4000),
            ("mass", "b", 800, 1600),
            ("hnw", "a", 8, 50),
        ]
        grid = build_grid(stats, target_neg_pos=5, alpha=0.5, w_max=5.0)
        by = {(r["segment"], r["product"]): r for r in grid}
        # median_pos over cells [200, 800, 8] = 200
        assert by[("mass", "a")]["n_pos"] == 200
        assert by[("mass", "a")]["suggested_weight"] == 1.0  # n_pos == median
        # hnw|a: (200/8)**0.5 = 5.0 -> cap
        assert by[("hnw", "a")]["suggested_weight"] == 5.0
        # mass|a downsample: 5*200/4000 = 0.25
        assert by[("mass", "a")]["suggested_ratio"] == 0.25
        # every row carries pos_rate
        assert abs(by[("hnw", "a")]["pos_rate"] - 8 / 58) < 1e-9


import yaml

from recsys_tfb.tooling.sampling_suggest import grid_to_yaml


def _params():
    return {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["a", "b"]}},
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]},
                    "sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
    }


class TestGridToYaml:
    def test_sparse_emits_only_non_default(self):
        # default ratio 1.0, default weight 1.0 -> only deviating cells emitted
        export = [
            {"segment": "mass", "product": "a", "ratio": 0.5, "weight": 1.0},
            {"segment": "mass", "product": "b", "ratio": 1.0, "weight": 3.0},
            {"segment": "hnw", "product": "a", "ratio": 1.0, "weight": 1.0},
        ]
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.5}}
        assert sw == {"sample_weights": {"mass|b": 3.0}}

    def test_unknown_product_raises_with_collected_message(self):
        export = [{"segment": "mass", "product": "zzz", "ratio": 0.5, "weight": 2.0}]
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)


import json

from recsys_tfb.tooling.sampling_suggest import render_html


class TestRenderHtml:
    def test_html_is_self_contained_and_embeds_grid(self):
        grid = [{"segment": "mass", "product": "a", "n_pos": 200,
                 "n_neg": 4000, "pos_rate": 0.047,
                 "suggested_ratio": 0.25, "suggested_weight": 1.0}]
        html = render_html(grid, default_ratio=1.0)
        assert html.lstrip().startswith("<!DOCTYPE html>")
        assert "mass" in html and "0.25" in html
        # the grid is embedded as JSON for the export button
        assert json.dumps(grid) in html
        # no external resource references (self-contained)
        assert "http://" not in html and "https://" not in html
        assert "Export JSON" in html and "Export YAML" in html
