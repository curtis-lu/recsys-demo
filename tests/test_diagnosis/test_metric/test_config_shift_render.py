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

import pandas as pd
import pytest
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


def _all_text(sections) -> str:
    """禁用字掃描範圍：標題＋說明＋公式＋重點＋表標題＋所有表格的字串內容。

    ``render`` 現在回傳多個 section（使用者回饋：說明擠在最上面看不出在講哪張
    圖），所以掃描範圍是整頁——漏掉任何一個新欄位，禁用字掃描就會出現死角。
    """
    if isinstance(sections, ReportSection):
        sections = (sections,)
    parts: list[str] = []
    for section in sections:
        parts.extend([section.title, section.description, section.formula])
        parts.extend(section.bullets)
        parts.extend(section.table_titles)
        parts.extend(table.to_string() for table in section.tables)
    return "\n".join(parts)


def _has_heatmap(sections) -> bool:
    if isinstance(sections, ReportSection):
        sections = (sections,)
    return any(
        isinstance(trace, go.Heatmap)
        for section in sections
        for figure in section.figures
        for trace in figure.data
    )


def _all_tables(sections) -> list:
    return [table for section in sections for table in section.tables]


def _all_figures(sections) -> list:
    return [fig for section in sections for fig in section.figures]


# ---- 基本形狀 ----------------------------------------------------------


def test_render_returns_sections():
    sections = config_shift.render(_result(), {})
    assert isinstance(sections, tuple)
    assert all(isinstance(s, ReportSection) for s in sections)


def test_render_returns_multiple_sections():
    """一張圖一個 section，各自帶標題。

    使用者的回饋：「說明的地方集中在上面很難知道你要描述的圖表是哪一個」。
    全部塞回單一 section 就是那個被抱怨的版面，所以這條直接守住拆分本身。
    """
    sections = config_shift.render(_result(), {})
    assert len(sections) >= 5
    assert all(s.title.strip() for s in sections)


def test_section_titles_are_distinct():
    """標題重複的話，讀者仍然分不出哪段在講哪張圖。"""
    titles = [s.title for s in config_shift.render(_result(), {})]
    assert len(set(titles)) == len(titles)


def test_every_section_with_a_figure_has_its_own_explanation():
    """帶圖或表的 section 必須自己帶說明——這條直接守使用者的抱怨。"""
    for section in config_shift.render(_result(), {}):
        if section.figures or section.tables:
            assert section.description.strip() or section.bullets, (
                f"section {section.title!r} 有圖表卻沒有自己的說明"
            )


def test_formulas_present_for_computed_quantities():
    """使用者：「強烈建議應該附上公式，讓讀者一眼就知道數字怎麼算出來的」。

    唯一免公式的是最後那個可見性清單（它列的是三份名單，沒有計算量）。用
    ``sections[:-1]`` 而不是 ``sections[:5]``：後者在某些 section 因資料為空而
    缺席時，會把可見性那一節算進要求範圍，變成一條會誤紅的測試。
    """
    sections = config_shift.render(_result(), {})
    assert sections[-1].title == "本次執行的完整性檢查"
    for section in sections[:-1]:
        assert section.formula.strip(), f"section {section.title!r} 缺公式"


def test_formula_names_the_config_keys_behind_the_symbols():
    """公式裡的符號要能對回 config 的鍵名。

    只給 ``offset = ln(r₊/r₋) + ln(w₊/w₋)`` 的話，讀者知道「怎麼算」卻不知道
    「算的是哪個設定」——那個公式就只是裝飾。
    """
    formula = config_shift.render(_result(), {})[0].formula
    assert "sample_ratio_overrides" in formula
    assert "sample_weights" in formula


def test_formulas_use_plain_unicode_not_latex():
    """生產限制是 no network、no additional packages——CDN 上的 MathJax／KaTeX
    一定載不到，LaTeX 原始碼會原樣印在報表上。
    """
    for section in config_shift.render(_result(), {}):
        assert "\\frac" not in section.formula
        assert "$" not in section.formula


def test_no_section_description_is_a_wall_of_text():
    """使用者：「這整段太冗長，看報表的人不會有耐心看完全部」。"""
    for section in config_shift.render(_result(), {}):
        assert len(section.description) <= 120, (
            f"section {section.title!r} 的 description 有 "
            f"{len(section.description)} 字元，超過 120"
        )


def test_bullets_are_one_sentence_each():
    """每則 bullet 一句話——原本那種「A；B；C，而且 D」的長句要拆開。

    可見性那一節排除在外：它的 bullet 有一部分是 ``compute`` 的 notes **原文
    照登**，長度不歸呈現層管，改寫反而會讓計算層的觀測失真。
    """
    for section in config_shift.render(_result(), {})[:-1]:
        for bullet in section.bullets:
            assert len(bullet) <= 160, f"bullet 過長：{bullet[:40]}…"


def test_render_returns_empty_tuple_when_disabled():
    """停用時回空 tuple，不是回一個空 section。

    差別在報表層：空 tuple 代表「這頁不存在」，空 section 會產出一個看起來
    「量到了、結果什麼都沒有」的章節——那正是本專案要避免的誤讀。
    """
    assert config_shift.render({"enabled": False}, {}) == ()


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
    填值那一步本身不在這裡測：它屬於組裝層，各診斷共用一份實作。
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
    """``Σ Δⱼ ≠ Δ`` 這句警語不得在拆分 section 的過程中掉隊。"""
    text = _all_text(config_shift.render(_result(), {}))
    assert "Σ Δⱼ ≠ Δ" in text or "Σ Δ_j ≠ Δ" in text


def test_delta_and_ci_are_shown_without_a_significance_verdict():
    """Δ 與區間都要在，但不得替讀者判讀區間有沒有跨 0。"""
    text = _all_text(config_shift.render(_result(), {}))
    assert "-0.0200" in text
    assert "-0.0500" in text and "+0.0100" in text
    for word in ("顯著", "不顯著"):
        assert word not in text


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
    sections = config_shift.render(result, {})

    assert not _has_heatmap(sections), "超過繪圖預算時不應該畫熱圖"

    n_contexts = len(result["offset_matrix"])
    n_items = len(result["items"])
    matrix_tables = [
        table for table in _all_tables(sections)
        if {"context 群", "item"} <= set(table.columns)
    ]
    assert len(matrix_tables) == 1, "超預算時矩陣必須以表格完整呈現"
    # 門檻管的是繪圖能力，不是資料的意義——一列都不准少。
    assert len(matrix_tables[0]) == n_contexts * n_items

    text = _all_text(sections)
    assert str(MAX_FIGURE_POINTS) in text
    assert f"{n_contexts * n_items}" in text


def test_budget_downgrade_note_lives_in_the_section_it_describes():
    """降級敘述必須放在被降級的那個 section，不是散到別處。

    整份說明擠在頁首正是使用者抱怨的版面；降級敘述漂到別的 section 會複製
    同一個問題——讀者在矩陣表旁邊看不到「為什麼這裡是表不是圖」。
    """
    sections = config_shift.render(_over_budget_result(), {})
    matrix_sections = [
        s for s in sections
        if any({"context 群", "item"} <= set(t.columns) for t in s.tables)
    ]
    assert len(matrix_sections) == 1
    assert str(MAX_FIGURE_POINTS) in "\n".join(matrix_sections[0].bullets)


def test_bar_figures_survive_the_heatmap_budget_cut():
    """熱圖被略過不代表整頁沒圖：兩張條圖各自在預算內，應照畫。"""
    sections = config_shift.render(_over_budget_result(), {})
    assert any(
        isinstance(trace, go.Bar)
        for fig in _all_figures(sections) for trace in fig.data
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
    sections = config_shift.render(result, {})
    assert all(isinstance(s, ReportSection) for s in sections)
    assert not _has_heatmap(sections)
    assert "診斷抽樣為空" in _all_text(sections)


def test_empty_sample_produces_no_hollow_sections():
    """沒有資料的 section 不該留在頁面上。

    一個只有標題與公式、底下什麼都沒有的區塊，看起來像「量到了、結果是空的」，
    跟「這次沒有這批資料」在報表上長得一樣。
    """
    result = _result()
    result.update({
        "items": [], "offset_matrix": {}, "offset_centered": {},
        "offset_spread_by_context": {}, "query_offset_spread": {},
        "per_item": [], "sample": {}, "delta": None,
        "delta_ci_low": None, "delta_ci_high": None,
        "notes": ["診斷抽樣為空——offset 矩陣與 Δ 均未計算。"],
    })
    for section in config_shift.render(result, {}):
        assert section.figures or section.tables or section.bullets, (
            f"section {section.title!r} 是空殼"
        )


def test_tables_and_titles_stay_aligned():
    """``table_titles`` 與 ``tables`` 是兩個平行 list，長度不一致會錯位。"""
    for result in (_result(), _over_budget_result()):
        for section in config_shift.render(result, {}):
            assert len(section.tables) == len(section.table_titles)


# ---- offset 的尺（log-odds → 倍率換算表） -------------------------------


_SCALE_TITLE = "offset 的尺：log-odds 換算成倍率"


def _scale_section(result: dict | None = None):
    sections = config_shift.render(result if result is not None else _result(), {})
    return next((s for s in sections if s.title == _SCALE_TITLE), None)


def _scale_row(table, offset_label: str):
    """依 ``offset`` 欄取一列。

    ``offset`` 刻意是普通欄位而不是 index（具名 index 會讓 pandas 產生兩列
    表頭），所以查表走欄位比對而不是 ``.loc``。
    """
    matches = table[table["offset"] == offset_label]
    assert len(matches) == 1, f"{offset_label} 應恰好對到一列，實際 {len(matches)}"
    return matches.iloc[0]


def test_scale_section_is_second():
    """插入點是 index 1（緊接 offset 矩陣之後），其餘六節順序不變。"""
    sections = config_shift.render(_result(), {})
    assert len(sections) == 7
    assert sections[1].title == _SCALE_TITLE

    other_titles = [s.title for s in sections if s.title != _SCALE_TITLE]
    assert other_titles == [
        "offset 矩陣（context 群 × item）",
        "群內 offset 範圍",
        "逐 query 的 offset 範圍",
        "扣掉 offset 之後的 mAP 變化",
        "逐 item 的替換實驗",
        "本次執行的完整性檢查",
    ]


def test_scale_table_exact_values():
    """逐格核對 4 列（``-10``／``-1``／``+1``／``+10``）的完整內容。

    期望值由 ``勝算倍率 = exp(offset)``、``p_after = sigmoid(logit(base) +
    offset)`` 離線算過，見任務規格；這裡只驗證程式輸出與算過的值逐格相等。
    """
    section = _scale_section()
    table = section.tables[0]
    columns = ["勝算倍率", "從 0.01%", "從 0.1%", "從 1%", "從 2%",
               "從 5%", "從 10%", "從 50%"]

    expected = {
        "-10": ("÷22,026", "0.000000454%", "0.00000454%", "0.0000459%",
                "0.0000927%", "0.000239%", "0.000504%", "0.00454%"),
        "-1": ("÷2.72", "0.00368%", "0.0368%", "0.370%", "0.745%",
               "1.9%", "3.93%", "26.9%"),
        "+1": ("×2.72", "0.0272%", "0.271%", "2.67%", "5.26%",
               "12.5%", "23.2%", "73.1%"),
        "+10": ("×22,026", "68.8%", "95.7%", "99.6%", "99.8%",
                "99.9%", "100%", "100%"),
    }
    for label, values in expected.items():
        row = _scale_row(table, label)
        assert tuple(row[col] for col in columns) == values, label

    # offset 必須是普通欄位而非 index：set_index 會讓 pandas 產生兩列表頭，
    # 第二列是「offset ＋ 五個空格」，本頁其他表格都不長那樣。
    assert list(table.columns)[0] == "offset"
    assert isinstance(table.index, pd.RangeIndex)


def test_probability_columns_differ_across_base_rates():
    """關鍵結構性測試：同一個 offset 造成的**機率倍率**隨起點而變。

    若把機率誤實作成 ``p_after = base * exp(offset)``（少了 sigmoid/logit 往
    返），每一欄的 ``p_after / base`` 都會退化成同一個勝算倍率。機率欄現在只
    印結果機率、不印倍率，所以這裡**自己從結果機率反算倍率**再比對——比對數
    值而不是字串，換掉格式化方式也不會讓這條失效。
    """
    section = _scale_section()
    table = section.tables[0]
    row = _scale_row(table, "+1")

    def _ratio(column: str, base: float) -> float:
        return float(row[column].rstrip("%")) / 100 / base

    # exp(1) = 2.718：起點極低時機率倍率≈勝算倍率，起點高時被 100% 壓下來。
    assert _ratio("從 0.01%", 0.0001) == pytest.approx(2.72, abs=0.01)
    assert _ratio("從 50%", 0.50) == pytest.approx(1.46, abs=0.01)
    assert _ratio("從 0.01%", 0.0001) != _ratio("從 50%", 0.50)


def test_no_probability_exceeds_100_percent():
    """任何一格都不可能超過 100%——機率誤算成 ``base * exp(offset)`` 的直接徵兆。

    上面那條比的是倍率是否隨起點而變；這條守的是更粗暴的失效：+10 那列從 50%
    出發，錯誤實作會算出 50% × 22026 ＝ 1,101,300%。全表掃過，一格都不放過。
    """
    table = _scale_section().tables[0]
    for column in table.columns[2:]:
        for cell in table[column]:
            assert float(cell.rstrip("%")) <= 100.0, f"{column}: {cell}"


def test_scale_section_absent_when_no_offsets():
    """``offset_centered`` 與 ``offset_matrix`` 都空 → 整節回 None（不進頁面）。

    與 ``_offset_matrix_section`` 在 ``n_cells == 0`` 時回 None 一致：沒有
    offset 圖表時，這張尺沒有服務對象。
    """
    result = _result()
    result["offset_centered"] = {}
    result["offset_matrix"] = {}
    assert _scale_section(result) is None


def test_scale_bullet_reports_actual_ranges():
    """第 3 則 bullet 的動態範圍真的讀了資料，不是寫死常數。

    exp(9.46) ≈ 12835.88 → ``,.0f`` 格式化四捨五入為 ``12,836``（用實際算出的
    值核對，不是憑印象抄一個接近的數字）。
    """
    result = _result()
    result["offset_centered"] = {"g": {"a": -3.05, "b": 2.99}}
    result["offset_matrix"] = {"g": {"a": 3.42, "b": 9.46}}
    section = _scale_section(result)

    # **整句相等**，不是 substring `in`。用 `in` 的話：(a) 把 lo/hi 對調（報表
    # 印出「+2.99 ~ -3.05」這種上下顛倒的範圍）測試照樣全綠；(b) `"÷21" in`
    # 會被 ÷21.9／÷210／÷21,000 滿足，`"×30" in` 會被 ×30.0~×30.9 全部滿足
    # （實際值是 ×30.6）——截斷數字做 substring 等於把斷言放寬成一個區間。
    assert section.bullets[2] == (
        "本次執行：置中後 -3.05 ~ +2.99（÷21.1 ~ ×19.9）；"
        "未置中原始值 +3.42 ~ +9.46（×30.6 ~ ×12,836）。"
    )


def test_scale_bullet_drops_non_finite_offsets():
    """NaN／inf 必須先濾掉再取 min/max，否則結果隨 dict 插入順序而變。

    ``nan`` 的比較恆為 False，所以 ``min``／``max`` 會依順序給出相反結果：
    NaN 排前面 → 整個範圍變成 nan（報表印「+nan ~ +nan」）；NaN 排後面 →
    被靜默吞掉、報表宣稱範圍是剩下那些值。兩種都是錯的，而且同一份資料換個
    順序就換一種錯法。這裡兩種順序都測。
    """
    nan = float("nan")
    for matrix in (
        {"g": {"a": nan, "b": 1.0, "c": 2.0}},   # NaN 在前
        {"g": {"a": 1.0, "b": 2.0, "c": nan}},   # NaN 在後
    ):
        result = _result()
        result["offset_centered"] = matrix
        result["offset_matrix"] = {}
        bullet = _scale_section(result).bullets[2]
        assert bullet == "本次執行：置中後 +1.00 ~ +2.00（×2.72 ~ ×7.39）。", matrix
        assert "nan" not in bullet


def test_scale_section_absent_when_all_offsets_non_finite():
    """全是 NaN ＝ 沒有可用值，與空 dict 同路（回 None），不是印出 nan 範圍。"""
    result = _result()
    result["offset_centered"] = {"g": {"a": float("nan")}}
    result["offset_matrix"] = {"g": {"b": float("inf")}}
    assert _scale_section(result) is None


def test_scale_bullet_benign_degenerate_inputs():
    """良性退化（群存在但無 item、單一值 min == max、只有單邊有值）要靜默通過。

    與上面兩條「真的壞」的輸入分開列舉：壞資料該被濾掉／回 None，良性退化該
    照常印出範圍。只測其中一類的話，另一類的行為就沒有任何測試釘住。
    """
    # 群存在但沒有 item → 該群不貢獻值，等同空
    result = _result()
    result["offset_centered"] = {"g": {}, "h": {"a": 0.5}}
    result["offset_matrix"] = {}
    assert _scale_section(result).bullets[2] == (
        "本次執行：置中後 +0.50 ~ +0.50（×1.65 ~ ×1.65）。"
    )

    # 只有 offset_matrix 有值 → range_parts 只有一段（單邊路徑）
    result = _result()
    result["offset_centered"] = {}
    result["offset_matrix"] = {"g": {"a": 2.0, "b": 2.0}}
    assert _scale_section(result).bullets[2] == (
        "本次執行：未置中原始值 +2.00 ~ +2.00（×7.39 ~ ×7.39）。"
    )


def test_scale_section_is_collapsible():
    section = _scale_section()
    assert section.collapsible is True


# ---- 契約 --------------------------------------------------------------


def test_module_satisfies_contract():
    """本模組滿足契約（符號齊全 ＋ 兩個函式簽章正確）。

    ``check_module`` 本身的行為（含簽章檢查會不會擋下錯的形狀）測在
    ``test_contract.py``——那是它的家，Plans 2–5 新增診斷時該跑的也是那一檔。
    """
    contract.check_module(config_shift)


def test_page_head_and_run_checks_do_not_share_a_label():
    """頁首的「推論不到什麼」與各頁「本次執行的完整性檢查」是不同性質的東西。

    早期兩者都叫「看不見什麼」，使用者當場指出「標題跟內容對不起來，而且兩邊
    內容完全無關」。前者是這個指標**結構上**推論不到的事（與資料無關，永遠成
    立），後者是三種已知靜默失效在**本次執行**的結果（會隨執行變動）。
    """
    from recsys_tfb.report.pages import _render_scope_note

    head = _render_scope_note(config_shift.SCOPE)
    sections = config_shift.render(_result(), {})
    run_checks_title = sections[-1].title

    assert "推論不到什麼" in head
    assert "看不見什麼" not in head, "頁首標籤與逐次執行的檢查同名會混淆"
    assert run_checks_title not in head


def test_no_reference_point_repeats_a_section_bullet():
    """判讀某張圖表的方式要寫在那張圖表旁邊，不是頁首。

    使用者原話：「裡面的內容跟下方圖表中的內容有重疊，我覺得寫在上面會很難理
    解，不知道在講什麼」——讀者還沒看到那張圖，就先被要求理解它的判讀方式。
    這條不禁止 reference_points 存在，只禁止它與 section 的 bullet 重複。
    """
    sections = config_shift.render(_result(), {})
    bullets = [b for s in sections for b in s.bullets]

    for point in config_shift.SCOPE.reference_points:
        head = point[:20]
        clashes = [b for b in bullets if head in b]
        assert not clashes, (
            f"頁首對照點與圖表旁的說明重複：{point!r} ↔ {clashes!r}"
        )
