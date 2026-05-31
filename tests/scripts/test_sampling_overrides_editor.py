"""Tests for sampling_overrides_editor script."""

import json

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from scripts.sampling_overrides_editor import (
    app,
    build_grid,
    grid_to_yaml,
    profile_stats,
    render_html,
    resolve_columns,
    suggest_ratio,
    suggest_weight,
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


# ---------------------------------------------------------------------------
# Grid construction
# ---------------------------------------------------------------------------
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
        # primary knob: neg:pos multiplier, defaulted uniformly to the target
        assert by[("mass", "a")]["suggested_neg_mult"] == 5
        assert by[("hnw", "a")]["suggested_neg_mult"] == 5
        # every row carries pos_rate
        assert abs(by[("hnw", "a")]["pos_rate"] - 8 / 58) < 1e-9


# ---------------------------------------------------------------------------
# Config-driven column resolution (no hardcoded column names)
# ---------------------------------------------------------------------------
class TestResolveColumns:
    _SCHEMA = {"columns": {"item": "prod_name", "label": "label",
                           "time": "snap_date"}}

    def test_resolves_from_schema_and_segment_by_exclusion(self):
        cols = resolve_columns(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            self._SCHEMA)
        assert cols == {"segment_col": "cust_segment_typ",
                        "item_col": "prod_name", "label_col": "label",
                        "time_col": "snap_date"}

    def test_segment_found_regardless_of_position(self):
        # exclusion-based, not positional: a differently-named/ordered
        # segment column still resolves.
        cols = resolve_columns(
            {"sample_group_keys": ["prod_name", "label", "cust_age_band"]},
            self._SCHEMA)
        assert cols["segment_col"] == "cust_age_band"

    def test_rejects_group_keys_not_a_segment_item_label_triple(self):
        with pytest.raises(ValueError, match="sample_group_keys"):
            resolve_columns(
                {"sample_group_keys": ["prod_name", "label"]}, self._SCHEMA)

    def test_rejects_missing_schema_column(self):
        with pytest.raises(ValueError, match="schema.columns"):
            resolve_columns(
                {"sample_group_keys": ["seg", "prod_name", "label"]},
                {"columns": {"item": "prod_name", "label": "label"}})


# ---------------------------------------------------------------------------
# Sparse JSON -> YAML with A5/A9 validation
# ---------------------------------------------------------------------------
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

    def test_unknown_product_weight_only_raises(self):
        # weight-only deviation (ratio at default, so no override emitted) must
        # still catch an unknown product via the A9c sample_weights check.
        export = [{"segment": "mass", "product": "zzz", "ratio": 1.0, "weight": 2.0}]
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)


# ---------------------------------------------------------------------------
# Self-contained HTML renderer
# ---------------------------------------------------------------------------
class TestRenderHtml:
    _GRID = [{"segment": "mass", "product": "a", "n_pos": 200,
              "n_neg": 4000, "pos_rate": 0.047, "suggested_neg_mult": 5.0,
              "suggested_ratio": 0.25, "suggested_weight": 1.0}]

    def test_html_is_self_contained_and_embeds_grid(self):
        html = render_html(self._GRID, default_ratio=1.0)
        assert html.lstrip().startswith("<!DOCTYPE html>")
        assert "mass" in html and "0.25" in html
        # the grid is embedded as JSON for the export button
        assert json.dumps(self._GRID) in html
        # no external resource references (self-contained)
        assert "http://" not in html and "https://" not in html
        assert "Export JSON" in html and "Export YAML" in html

    def test_backward_compatible_two_arg_call(self):
        # profile()'s old call site / existing callers pass only default_ratio.
        html = render_html(self._GRID, default_ratio=1.0)
        assert "<!DOCTYPE html>" in html

    def test_columns_are_clickable_sortable(self):
        html = render_html(self._GRID, default_ratio=1.0)
        assert "function sortBy(" in html
        # numeric / derived headers wire literal sort keys
        for col in ("n_pos", "n_neg", "pos_rate", "_ach", "_kept", "_npr"):
            assert f"sortBy('{col}')" in html
        # name columns come from the per-mode column lists
        assert "'segment'" in html and "'product'" in html

    def test_has_live_filter_box(self):
        html = render_html(self._GRID, default_ratio=1.0)
        assert 'id="flt"' in html or "id=flt" in html
        assert "function flt(" in html

    def test_edits_survive_sort_and_filter(self):
        # sort/filter re-render the tbody; edits must be synced back to GRID
        # first (and collect/export must read the synced values), else a user
        # loses every edit the moment they sort a column.
        html = render_html(self._GRID, default_ratio=1.0)
        assert "function syncEdits(" in html
        assert "syncEdits()" in html  # called by sort/flt/collect

    def test_shows_live_post_downsample_preview_columns(self):
        # kept_neg / new_pos_rate computed client-side from n_neg and the
        # ratio derived from the edited multiplier, refreshed on every
        # keystroke (oninput -> recalc).
        html = render_html(self._GRID, default_ratio=1.0)
        assert "kept_neg" in html and "new_pos_rate" in html
        assert "function preview(" in html and "function recalc(" in html
        assert 'oninput="recalc(this)"' in html
        # the JS derives ratio = clamp(nm*n_pos/n_neg,0,1), then computes
        # kept_neg = round(n_neg*ratio) and new_pos_rate = n_pos/(n_pos+kept_neg);
        # rows are built browser-side, so assert the formulas are embedded.
        assert "nm*r.n_pos/r.n_neg" in html
        assert "Math.round(r.n_neg*ratio)" in html
        assert "r.n_pos/total" in html

    def test_neg_mult_is_primary_knob_with_readonly_derived_ratio(self):
        # The editable knob is the neg:pos multiplier; ratio is a read-only
        # keep-rate derived from it, and the achieved multiplier lives in its
        # own read-only "實際倍率" column (same unit as the knob). A cell whose
        # target can't be reached (ratio clamps to 1.0) flags that column amber.
        html = render_html(self._GRID, default_ratio=1.0, target_neg_pos=5.0)
        assert "負樣本倍率" in html
        # multiplier is the contenteditable knob wired to recalc
        assert "data-k=neg_mult" in html
        # ratio is no longer directly editable; pure keep-rate cell
        assert "data-k=ratio" not in html
        # achieved multiplier has its own column + markup helper
        assert "實際倍率" in html
        assert "function achMult(" in html
        # the configured target R is surfaced to the JS for the warning text
        assert "const R=5.0" in html
        # unreachable-target signal: amber style + 全留 explanation
        assert "td.warn" in html and "已全留" in html

    def test_has_bulk_set_controls(self):
        # Bulk-set toolbar: choose filter column (segment/product), pick one or
        # more values from a multi-select, target field (neg_mult/weight), new
        # value -> overwrite every matching row. Smoke-test the wiring.
        html = render_html(self._GRID, default_ratio=1.0)
        assert 'id="bulk"' in html
        for ident in ('id="bk"', 'id="bv"', 'id="sk"', 'id="sv"', 'id="bm"'):
            assert ident in html
        assert 'onclick="bulkSet()"' in html
        assert 'function bulkSet(' in html
        # value picker is a multi-select, auto-populated per dimension and
        # repopulated when the dimension switches.
        assert '<select id="bv" multiple' in html
        assert 'function fillBulk(' in html
        assert 'onchange="fillBulk()"' in html
        assert 'selectedOptions' in html
        # bulk apply must syncEdits first (don't drop in-flight typing)
        # and recompute the footer totals afterwards.
        assert 'syncEdits()' in html and 'recalcTotals()' in html

    def test_has_totals_footer_row(self):
        # tfoot row reflects current settings -> downsampled totals, rebuilt by
        # recalcTotals() into the single #foot row (column count varies by mode).
        html = render_html(self._GRID, default_ratio=1.0)
        assert "<tfoot>" in html and 'id="foot"' in html
        assert "function recalcTotals(" in html
        # totals computed over the whole active store, not the filtered view
        assert "rows().forEach" in html

    def test_has_mode_selector_with_three_exclusive_modes(self):
        # Granularity chosen via a radio selector; the three modes are mutually
        # exclusive (one editable/exportable table at a time) -> no cross-table
        # coupling. The segment/product aggregate stores are built from the grid.
        html = render_html(self._GRID, default_ratio=1.0)
        assert 'id="mode"' in html
        assert "function setMode(" in html
        for m in ("'cell'", "'segment'", "'product'"):
            assert f"setMode({m})" in html
        # independent aggregate stores + per-mode median-based weight default
        assert "function aggStore(" in html
        assert "aggStore('segment')" in html and "aggStore('product')" in html
        assert "function suggestWeight(" in html
        assert "const ALPHA=0.5" in html and "const WMAX=5.0" in html

    def test_export_keys_depend_on_active_mode(self):
        # Export emits ONLY the active mode's overrides; key format differs:
        # cell -> 'seg|prod' (|0 for ratio), segment/product -> single key.
        html = render_html(self._GRID, default_ratio=1.0)
        assert "function exp(" in html
        assert "keyOf" in html
        assert "mode==='cell'?k+'|0':k" in html

    def test_explains_ratio_and_weight_logic_and_purpose(self):
        html = render_html(self._GRID, default_ratio=1.0,
                           target_neg_pos=5.0, alpha=0.5, w_max=5.0)
        # what each column means + where it gets pasted
        assert "負樣本下採樣" in html and "冷門" in html
        assert "sample_ratio_overrides" in html
        assert "sample_weights" in html
        # the *configured* tuning values are surfaced, not hardcoded prose
        html2 = render_html(self._GRID, default_ratio=1.0,
                            target_neg_pos=3.0, alpha=0.7, w_max=8.0)
        assert "3.0" in html2 and "0.7" in html2 and "8.0" in html2
        # still self-contained even with the explanation block
        assert "http://" not in html and "https://" not in html


# ---------------------------------------------------------------------------
# Spark profiling aggregation
# ---------------------------------------------------------------------------
@pytest.mark.spark
class TestProfileStats:
    def test_groups_by_segment_product_and_counts_pos_neg(self, spark):
        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6],
            "prod_name": ["a", "a", "a", "b", "b", "b"],
            "cust_segment_typ": ["mass", "mass", "mass", "hnw", "hnw", "hnw"],
            "label": [1, 0, 0, 1, 1, 0],
        }))
        stats = profile_stats(
            df, [pd.Timestamp("2025-01-31")],
            segment_col="cust_segment_typ", item_col="prod_name",
            label_col="label", time_col="snap_date")
        d = {(s, p): (np_, nn_) for (s, p, np_, nn_) in stats}
        assert d[("mass", "a")] == (1, 2)
        assert d[("hnw", "b")] == (2, 1)


# ---------------------------------------------------------------------------
# Typer CLI (to-yaml end-to-end; profile's Spark path covered above)
# ---------------------------------------------------------------------------
class TestToYamlCli:
    def test_to_yaml_prints_both_blocks(self, tmp_path):
        export = [{"segment": "mass", "product": "a", "ratio": 0.5, "weight": 1.0},
                  {"segment": "mass", "product": "b", "ratio": 1.0, "weight": 3.0}]
        jf = tmp_path / "e.json"
        jf.write_text(json.dumps(export))
        # minimal params yaml the command reads for A5/A9
        params = tmp_path / "p.yaml"
        params.write_text(
            "schema:\n  columns:\n    item: prod_name\n"
            "  categorical_values:\n    prod_name: [a, b]\n"
            "dataset:\n  prepare_model_input:\n"
            "    categorical_columns: [prod_name]\n"
            "  sample_group_keys: [cust_segment_typ, prod_name, label]\n")
        r = CliRunner().invoke(
            app, ["to-yaml", str(jf), "--params", str(params)])
        assert r.exit_code == 0, r.output
        assert "sample_ratio_overrides:" in r.output
        assert "mass|a|0" in r.output
        assert "sample_weights:" in r.output
        assert "mass|b" in r.output
