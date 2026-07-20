"""model_capacity 呈現層的測試：Gain 三分＋per-item 分配＋capacity vs ability。

通用契約測試移植自 ``test_item_ability_render.py``（見任務指示的 14 條清單）。
本項不吃共用抽樣，因此 item_ability 那份的「空抽樣良性退化」兩條
（``test_render_survives_an_empty_sample``／``test_empty_sample_produces_no_
hollow_sections``）不適用，改用本項自己的「available: False」等價路徑
（``test_unavailable_result_renders_reason_not_blank``）。

``RESULT_WITH_ABILITY``／``RESULT_WITHOUT_ABILITY`` 的鍵名取自
``_compute.compute`` 的實際輸出，不是取自計畫檔。
"""
from __future__ import annotations

import copy

import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric import contract, model_capacity
from recsys_tfb.report import ReportSection, ScopeNote

_BREAKDOWN_TITLE = "全模型 Gain 三分"
_PER_ITEM_TITLE = "per-item context gain 分配"
_SCATTER_TITLE = "capacity vs ability 散點"
_COMPLETENESS_TITLE = "本次執行的完整性檢查"


def _result(*, with_ability: bool) -> dict:
    """一份形狀與 ``compute`` 實際輸出一致的結果（每次回新的 deep copy）。"""
    per_item = [
        {
            "item": "ccard_ins",
            "context_gain": 20.0,
            "context_gain_share": 20.0 / 30.0,
            "query_centered_auc": 0.62 if with_ability else None,
        },
        {
            "item": "fund_bond",
            "context_gain": 10.0,
            "context_gain_share": 10.0 / 30.0,
            "query_centered_auc": 0.55 if with_ability else None,
        },
    ]
    notes = [] if with_ability else [
        "item_ability 未提供（可能還沒跑過該診斷，或這次 evaluation 未啟用"
        "它）——per_item 的 query_centered_auc 留空。"
    ]
    return copy.deepcopy({
        "enabled": True,
        "available": True,
        "reason": None,
        "summary": {
            "total_gain": 100.0,
            "item_id_gain": 60.0,
            "context_gain": 30.0,
            "unaccounted_gain": 10.0,
            "item_id_gain_share": 0.6,
            "context_gain_share": 0.3,
            "unaccounted_gain_share": 0.1,
            "n_items": 2,
        },
        "per_item": per_item,
        "field_notes": {},
        "notes": notes,
    })


RESULT_WITH_ABILITY = _result(with_ability=True)
RESULT_WITHOUT_ABILITY = _result(with_ability=False)


def _all_text(sections) -> str:
    """禁用字掃描範圍：標題＋說明＋公式＋重點＋表標題＋所有表格的字串內容。"""
    if isinstance(sections, ReportSection):
        sections = (sections,)
    parts: list[str] = []
    for section in sections:
        parts.extend([section.title, section.description, section.formula])
        parts.extend(section.bullets)
        parts.extend(section.table_titles)
        parts.extend(table.to_string() for table in section.tables)
    return "\n".join(parts)


def _section(sections, title: str) -> ReportSection:
    match = next((s for s in sections if s.title == title), None)
    assert match is not None, f"section {title!r} 不存在，實際標題：" \
        f"{[s.title for s in sections]}"
    return match


# ---- 基本形狀（移植自 test_item_ability_render.py）-----------------------


def test_render_returns_sections():
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    assert isinstance(sections, tuple)
    assert all(isinstance(s, ReportSection) for s in sections)


def test_render_returns_multiple_sections():
    """一張圖一個 section：Gain 三分／per-item 分配／散點／完整性檢查，共 4 節。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    assert len(sections) >= 4
    assert all(s.title.strip() for s in sections)


def test_section_titles_are_distinct():
    titles = [s.title for s in model_capacity.render(RESULT_WITH_ABILITY, {})]
    assert len(set(titles)) == len(titles)


def test_every_section_with_a_figure_has_its_own_explanation():
    for section in model_capacity.render(RESULT_WITH_ABILITY, {}):
        if section.figures or section.tables:
            assert section.description.strip() or section.bullets, (
                f"section {section.title!r} 有圖表卻沒有自己的說明"
            )


def test_formulas_use_plain_unicode_not_latex():
    """生產限制是 no network、no additional packages——MathJax／KaTeX 一定
    載不到，LaTeX 原始碼會原樣印在報表上。"""
    for section in model_capacity.render(RESULT_WITH_ABILITY, {}):
        assert "\\frac" not in section.formula
        assert "$" not in section.formula


def test_no_section_description_is_a_wall_of_text():
    for section in model_capacity.render(RESULT_WITH_ABILITY, {}):
        assert len(section.description) <= 120, (
            f"section {section.title!r} 的 description 有 "
            f"{len(section.description)} 字元，超過 120"
        )


def test_bullets_are_one_sentence_each():
    """完整性檢查排除在外：它的 bullet 有一部分是 notes 原文照登，長度不歸
    呈現層管（與 config_shift／item_ability 的例外理由一致）。
    """
    for section in model_capacity.render(RESULT_WITH_ABILITY, {}):
        if section.title == _COMPLETENESS_TITLE:
            continue
        for bullet in section.bullets:
            assert len(bullet) <= 160, f"bullet 過長：{bullet[:40]}…"


def test_render_returns_empty_tuple_when_disabled():
    assert model_capacity.render({"enabled": False}, {}) == ()


def test_render_is_pure_and_does_not_mutate_input():
    result = _result(with_ability=True)
    before = copy.deepcopy(result)
    model_capacity.render(result, {})
    assert result == before


def test_scope_declares_what_it_cannot_tell():
    assert isinstance(model_capacity.SCOPE, ScopeNote)
    assert model_capacity.SCOPE.blind_to


def test_module_level_scope_carries_no_run_specific_facts():
    assert model_capacity.SCOPE.sampling == ""


def test_no_verdict_vocabulary_in_output():
    forbidden = [
        "建議", "應該", "異常", "不足", "有問題", "健康", "通過", "失敗",
        "偏低", "偏高", "良好",
        "verdict", "severity", "recommend",
    ]
    scope = model_capacity.SCOPE
    text = _all_text(model_capacity.render(RESULT_WITH_ABILITY, {})) + "\n".join(
        [scope.measures, scope.population,
         *scope.blind_to, *scope.reference_points]
    )
    hits = [word for word in forbidden if word.lower() in text.lower()]
    assert hits == [], f"呈現層出現下結論的字眼：{hits}"


def test_tables_and_titles_stay_aligned():
    for section in model_capacity.render(RESULT_WITH_ABILITY, {}):
        assert len(section.tables) == len(section.table_titles)


def test_module_satisfies_contract():
    contract.check_module(model_capacity)


# ---- 未適用的通用契約條目（逐條說明理由，不默默跳過）----------------------
#
# ``test_render_survives_an_empty_sample`` / ``test_empty_sample_produces_no_
# hollow_sections``：item_ability 版驗的是「空抽樣是良性退化」，但
# model_capacity 不吃 diagnosis_sample，沒有「抽樣為空」這個狀態；本項對應
# 的良性退化路徑是「gain_ledger 不可用」，已由
# ``test_unavailable_result_renders_reason_not_blank`` 覆蓋。


# ---- 本項專屬 --------------------------------------------------------------


def test_capacity_vs_ability_scatter_present_when_ability_given():
    section_figs = sum(
        len(s.figures) for s in model_capacity.render(RESULT_WITH_ABILITY, {})
    )
    assert section_figs >= 2, "必須含 gain 分配條圖與 capacity vs ability 散點"


def test_scatter_absent_and_explained_when_ability_missing():
    """缺 item_ability 時不畫散點，但必須說明原因——
    斷言落在「有沒有講」，不能只斷言「圖沒出現」（後者被『正確略過』與
    『根本沒嘗試』同時滿足，是本專案踩過的假綠形態）。
    """
    sections = model_capacity.render(RESULT_WITHOUT_ABILITY, {})
    section = _section(sections, _SCATTER_TITLE)
    assert not section.figures, "沒有 item_ability 資料時不該畫散點"
    explanation = section.description + " ".join(section.bullets)
    assert "item_ability" in explanation, (
        "散點缺席時必須在文字裡說明是因為缺 item_ability，不能只是悄悄少一張圖"
    )


def test_unavailable_result_renders_reason_not_blank():
    section = model_capacity.render(
        {"enabled": True, "available": False,
         "reason": "訓練側未產出 gain_ledger.json"}, {})
    assert section  # 非空 tuple
    assert "gain_ledger" in " ".join(s.description for s in section)


def test_unaccounted_block_is_not_labelled_as_error():
    """未分配那塊不是誤差。呈現文字不得出現「誤差／殘差／漏掉／未解釋」。"""
    forbidden = ["誤差", "殘差", "漏掉", "未解釋"]
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    breakdown = _section(sections, _BREAKDOWN_TITLE)
    text = breakdown.description + " ".join(breakdown.bullets) + breakdown.formula
    hits = [w for w in forbidden if w in text]
    assert hits == [], f"「未分配」的描述出現不該有的字眼：{hits}"


def test_breakdown_shares_are_shown_without_stacking_semantics_lost():
    """三分的三個類別都要出現在同一節（即使不是真的堆疊圖），不能只畫一部分。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    section = _section(sections, _BREAKDOWN_TITLE)
    assert section.figures, "Gain 三分 section 沒有畫出圖"
    fig = section.figures[0]
    assert len(fig.data[0].x) == 3, "三個類別（Item Prior／Post-Item Context／未分配）都要出現"


def test_per_item_section_reflects_sorted_order_from_compute():
    """per-item 分配條圖依 compute 已排好的順序（遞減），不在 render 裡重排。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    section = _section(sections, _PER_ITEM_TITLE)
    assert section.figures, "per-item 分配 section 沒有畫出圖"
    items_shown = list(section.figures[0].data[0].x)
    assert items_shown == ["ccard_ins", "fund_bond"]


def test_completeness_section_lists_notes():
    """notes 原文照登，讀者要看得到「為什麼沒有 ability 資料」。"""
    sections = model_capacity.render(RESULT_WITHOUT_ABILITY, {})
    section = _section(sections, _COMPLETENESS_TITLE)
    assert "item_ability" in "\n".join(section.bullets)
