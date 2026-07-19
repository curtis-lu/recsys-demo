"""config_shift 呈現層的測試。

三條鐵則的執行點各對應到這裡的一組測試：

1. **不下結論** —— ``test_no_verdict_vocabulary_in_output`` 掃整個 section 的
   文字（標題／說明／表標題／表內容）與 ``SCOPE``，禁用字一命中就紅。
2. **不設門檻** —— 沒有直接的自動化測試（色階不編碼好壞是設計約定，理由見
   ``report/scales.py`` docstring：能寫出來的測試防不住它宣稱要防的事）。這裡
   守的是可機械檢查的那一半：唯一的門檻 ``MAX_FIGURE_POINTS`` 管的是繪圖能力
   而不是資料意義，所以 ``test_heatmap_skipped_when_over_figure_budget`` 同時
   斷言「圖不畫」與「表格完整、一列不少」。
3. **每個數字自帶說明** —— ``test_scope_declares_what_it_cannot_tell`` 與
   ``test_sampling_description_flows_into_scope``。

``RESULT`` 的鍵名取自 ``_compute.compute`` 的實際輸出（22 個頂層鍵），不是取自
計畫檔——計畫檔那份在 Task 2.2 過程中已漂移（``offset_spread`` 已改名
``offset_spread_by_context``、``per_item_sum_note`` 已移除）。

**用 attribute access（``config_shift.render``）而不是 ``from ... import render``**：
契約要的是「模組上有這個符號」，attribute access 才驗得到那件事；而且
``from`` import 在缺符號時是 collection 期 ImportError，整個檔案一條都跑不了。
"""
from __future__ import annotations

import copy

import plotly.graph_objects as go

from recsys_tfb.diagnosis.metric import config_shift, contract
from recsys_tfb.report import ReportSection, ScopeNote
from recsys_tfb.report.figures import MAX_FIGURE_POINTS

SAMPLING_DESCRIPTION = (
    "分層抽樣：正例 query 全取（22,000 筆），負例 query 依 hash 取 40%。"
)


def _result() -> dict:
    """一份形狀與 ``compute`` 實際輸出一致的結果（每次回新的 deep copy）。"""
    return copy.deepcopy({
        "enabled": True,
        "score_col_used": "score_uncalibrated",
        "metric_params": {
            "min_positives": 1, "shrinkage_k": 0.0, "weight_alpha": 0.0,
        },
        "context_columns": ["cust_segment_typ"],
        "items": ["item_a", "item_b"],
        "items_declared_not_observed": ["item_z"],
        "offset_spread_by_context": {"A": 0.6931, "B": 0.0},
        "query_offset_spread": {
            "mean": 0.3466, "p50": 0.0, "p90": 0.6931, "max": 0.6931,
            "n_queries": 40, "n_queries_multi_candidate": 40,
        },
        "offset_matrix": {
            "A": {"item_a": 0.6931, "item_b": 0.0},
            "B": {"item_a": 0.0, "item_b": 0.0},
        },
        "offset_centered": {
            "A": {"item_a": 0.3466, "item_b": -0.3466},
            "B": {"item_a": 0.0, "item_b": 0.0},
        },
        "unmatched_override_keys": [
            {"config": "sampling_overrides", "key": "9|zzz|1"},
        ],
        "baseline_map": 0.4211,
        "corrected_map": 0.4011,
        "delta": -0.02,
        "delta_ci_low": -0.05,
        "delta_ci_high": 0.01,
        "ci": {"enabled": True, "n_boot": 200, "seed": 42},
        "per_item": [
            {
                "item": "item_a", "delta_j": 0.0031,
                "map_after_only_this_item": 0.4242,
                "n_pos_raw": 40, "n_pos_effective": 61.5,
                "offset_min": 0.0, "offset_max": 0.6931,
            },
            {
                "item": "item_b", "delta_j": -0.0182,
                "map_after_only_this_item": 0.4029,
                "n_pos_raw": 20, "n_pos_effective": 28.5,
                "offset_min": 0.0, "offset_max": 0.0,
            },
        ],
        "sample": {
            "n_rows": 400, "n_queries": 40, "n_entities": 40, "n_items": 2,
            "n_positive_rows": 60, "n_positive_rows_effective": 90.0,
        },
        "sample_meta": {"sampling_description": SAMPLING_DESCRIPTION},
        "field_notes": {"delta": "corrected_map − baseline_map。"},
        "notes": [
            "有 1 個 override key 在本次樣本零命中：sampling_overrides['9|zzz|1']。",
        ],
    })


def _over_budget_result(n_contexts: int = 60, n_items: int = 40) -> dict:
    """context × item 超過 ``MAX_FIGURE_POINTS`` 的一份結果。

    60 × 40 ＝ 2400 > 2000，但 60 與 40 各自都在預算內——刻意這樣選，讓這條
    測試只隔離「熱圖的格數」這一個原因，兩張條圖仍應正常畫出。
    """
    assert n_contexts * n_items > MAX_FIGURE_POINTS
    assert n_contexts <= MAX_FIGURE_POINTS and n_items <= MAX_FIGURE_POINTS
    items = [f"item_{j:02d}" for j in range(n_items)]
    contexts = [f"ctx_{i:02d}" for i in range(n_contexts)]
    result = _result()
    result["items"] = items
    result["items_declared_not_observed"] = []
    result["offset_matrix"] = {
        c: {it: 0.01 * j for j, it in enumerate(items)} for c in contexts
    }
    result["offset_centered"] = {
        c: {it: 0.01 * j - 0.2 for j, it in enumerate(items)} for c in contexts
    }
    result["offset_spread_by_context"] = {c: 0.39 for c in contexts}
    result["per_item"] = [
        {
            "item": it, "delta_j": 0.0001 * j,
            "map_after_only_this_item": 0.4,
            "n_pos_raw": 10, "n_pos_effective": 15.0,
            "offset_min": 0.0, "offset_max": 0.39,
        }
        for j, it in enumerate(items)
    ]
    return result


def _all_text(section: ReportSection) -> str:
    """禁用字掃描範圍：標題＋說明＋表標題＋所有表格的字串內容。"""
    parts = [section.title, section.description, *section.table_titles]
    parts.extend(table.to_string() for table in section.tables)
    return "\n".join(parts)


def _has_heatmap(section: ReportSection) -> bool:
    return any(
        isinstance(trace, go.Heatmap)
        for figure in section.figures
        for trace in figure.data
    )


# ---- 基本形狀 ----------------------------------------------------------


def test_render_returns_section():
    section = config_shift.render(_result(), {})
    assert isinstance(section, ReportSection)


def test_render_returns_none_when_disabled():
    """停用時回 ``None``，不是回一個空 section。

    差別在報表層：``None`` 代表「這頁不存在」，空 section 會產出一個看起來
    「量到了、結果什麼都沒有」的章節——那正是本專案要避免的誤讀。
    """
    assert config_shift.render({"enabled": False}, {}) is None


def test_render_is_pure_and_does_not_mutate_input():
    """呈現層不得改動 ``compute`` 的輸出——那份 dict 會原樣落 JSON。"""
    result = _result()
    before = copy.deepcopy(result)
    config_shift.render(result, {})
    assert result == before


# ---- SCOPE -------------------------------------------------------------


def test_scope_declares_what_it_cannot_tell():
    assert isinstance(config_shift.SCOPE, ScopeNote)
    assert config_shift.SCOPE.blind_to
    assert "有正例" in config_shift.SCOPE.population


def test_scope_names_the_sum_of_delta_j_caveat():
    """``Σ Δ_j ≠ Δ`` 這句話的擁有者是 SCOPE（計算層已刻意不再存一份）。"""
    assert any("Σ Δ_j ≠ Δ" in item for item in config_shift.SCOPE.blind_to)


def test_module_level_scope_carries_no_run_specific_facts():
    """``sampling`` 必須留空，由組裝層（Task 2.5）從 ``sample_meta`` 填。

    這條守的是「別把執行期事實寫死進模組常數」——寫死的話，import 到的 SCOPE
    會帶著上一次執行的抽樣描述，而那是個看不出來的錯（字串長得很合理）。
    填值那一步本身不在這裡測：它屬於組裝層，五項診斷共用一份實作。
    """
    assert config_shift.SCOPE.sampling == ""


# ---- 鐵則 1：不下結論 --------------------------------------------------


def test_no_verdict_vocabulary_in_output():
    forbidden = [
        "建議", "應該", "異常", "不足", "有問題", "健康", "通過", "失敗",
        "verdict", "severity", "recommend",
    ]
    scope = config_shift.SCOPE
    text = _all_text(config_shift.render(_result(), {})) + "\n".join(
        [scope.measures, scope.population,
         *scope.blind_to, *scope.reference_points]
    )
    hits = [word for word in forbidden if word.lower() in text.lower()]
    assert hits == [], f"呈現層出現下結論的字眼：{hits}"


def test_sum_note_is_shown():
    section = config_shift.render(_result(), {})
    assert "Σ Δ_j ≠ Δ" in section.description


def test_delta_and_ci_are_shown_without_a_significance_verdict():
    """Δ 與區間都要在，但不得替讀者判讀區間有沒有跨 0。"""
    section = config_shift.render(_result(), {})
    assert "-0.0200" in section.description
    assert "-0.0500" in section.description and "+0.0100" in section.description
    for word in ("顯著", "不顯著"):
        assert word not in section.description


# ---- 鐵則 3：可見性區塊 ------------------------------------------------


def test_visibility_block_shows_unmatched_keys():
    """零命中的 override key ＝ offset 全算成 0 ＝ Δ 假性為 0，必須看得見。"""
    section = config_shift.render(_result(), {})
    assert "9|zzz|1" in _all_text(section)


def test_visibility_block_shows_declared_not_observed():
    """少一列與「該 item 沒有偏移」在報表上長得一樣，必須明寫。"""
    section = config_shift.render(_result(), {})
    assert "item_z" in _all_text(section)


def test_visibility_block_shows_compute_notes():
    section = config_shift.render(_result(), {})
    assert "零命中" in _all_text(section)


# ---- 圖形預算 ----------------------------------------------------------


def test_heatmap_is_drawn_when_within_budget():
    """對照組：預算內時熱圖必須真的畫出來。

    沒有這條的話，一個「永遠不畫熱圖」的實作也能讓下面那條測試綠。
    """
    assert _has_heatmap(config_shift.render(_result(), {}))


def test_heatmap_skipped_when_over_figure_budget():
    result = _over_budget_result()
    section = config_shift.render(result, {})

    assert not _has_heatmap(section), "超過繪圖預算時不應該畫熱圖"

    n_contexts = len(result["offset_matrix"])
    n_items = len(result["items"])
    matrix_tables = [
        table for table in section.tables
        if {"context 群", "item"} <= set(table.columns)
    ]
    assert len(matrix_tables) == 1, "超預算時矩陣必須以表格完整呈現"
    # 門檻管的是繪圖能力，不是資料的意義——一列都不准少。
    assert len(matrix_tables[0]) == n_contexts * n_items

    assert str(MAX_FIGURE_POINTS) in section.description
    assert f"{n_contexts * n_items}" in section.description


def test_bar_figures_survive_the_heatmap_budget_cut():
    """熱圖被略過不代表整個 section 沒圖：兩張條圖各自在預算內，應照畫。"""
    section = config_shift.render(_over_budget_result(), {})
    assert any(
        isinstance(trace, go.Bar) for fig in section.figures for trace in fig.data
    )


# ---- 良性退化輸入 ------------------------------------------------------


def test_render_survives_an_empty_sample():
    """空抽樣是良性退化（沒抽到東西），不是壞輸入——不該炸。"""
    result = _result()
    result.update({
        "items": [], "offset_matrix": {}, "offset_centered": {},
        "offset_spread_by_context": {}, "query_offset_spread": {},
        "per_item": [], "sample": {}, "delta": None,
        "delta_ci_low": None, "delta_ci_high": None,
        "notes": ["診斷抽樣為空——offset 矩陣與 Δ 均未計算。"],
    })
    section = config_shift.render(result, {})
    assert isinstance(section, ReportSection)
    assert not _has_heatmap(section)
    assert "診斷抽樣為空" in _all_text(section)


def test_tables_and_titles_stay_aligned():
    """``table_titles`` 與 ``tables`` 是兩個平行 list，長度不一致會錯位。"""
    for result in (_result(), _over_budget_result()):
        section = config_shift.render(result, {})
        assert len(section.tables) == len(section.table_titles)


# ---- 契約 --------------------------------------------------------------


def test_module_satisfies_contract():
    """本模組滿足契約（符號齊全 ＋ 兩個函式簽章正確）。

    ``check_module`` 本身的行為（含簽章檢查會不會擋下錯的形狀）測在
    ``test_contract.py``——那是它的家，Plans 2–5 新增診斷時該跑的也是那一檔。
    """
    contract.check_module(config_shift)
