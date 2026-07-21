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

_OVERVIEW_TITLE = "概覽"
_BREAKDOWN_TITLE = "全模型 Gain 三分"
_PER_ITEM_TITLE = "per-item context 容量 ledger"
_SCATTER_TITLE = "capacity vs ability 散點"
_COMPLETENESS_TITLE = "本次執行的完整性檢查"


def _result(*, with_ability: bool) -> dict:
    """一份形狀與 ``compute`` 實際輸出一致的結果（每次回新的 deep copy）。

    分配分母（sum_allocated_context_split=80）刻意 ≠ 全域 context_split_count
    （60），讓 render 能顯示「per-item 用的是分配帳、不是全域帳」。
    """
    per_item = [
        {
            "item": "ccard_ins",
            "context_gain": 20.0,
            "context_gain_share": 20.0 / 30.0,
            "context_gain_vs_total": 20.0 / 100.0,
            "gain_share_vs_max": 1.0,
            "gain_share_vs_median": 1.0,
            "context_split_count": 50,
            "context_split_share": 50.0 / 80.0,
            "context_split_vs_total": 50.0 / 120.0,
            "gain_per_split": 20.0 / 50.0,
            "context_gain_isolated": 5.0,
            "context_gain_isolated_share": 5.0 / 20.0,
            "context_gain_isolated_vs_total": 5.0 / 100.0,
            "context_split_isolated": 8,
            "context_split_isolated_vs_total": 8.0 / 120.0,
            "isolating_split_count": 30,
            "first_tree_index": 0,
            "n_trees_touched": 4,
            "query_centered_auc": 0.62 if with_ability else None,
        },
        {
            "item": "fund_bond",
            "context_gain": 10.0,
            "context_gain_share": 10.0 / 30.0,
            "context_gain_vs_total": 10.0 / 100.0,
            "gain_share_vs_max": 0.5,
            "gain_share_vs_median": 0.5,
            "context_split_count": 30,
            "context_split_share": 30.0 / 80.0,
            "context_split_vs_total": 30.0 / 120.0,
            "gain_per_split": 10.0 / 30.0,
            "context_gain_isolated": 0.0,
            "context_gain_isolated_share": 0.0,
            "context_gain_isolated_vs_total": 0.0,
            "context_split_isolated": 0,
            "context_split_isolated_vs_total": 0.0,
            "isolating_split_count": 20,
            "first_tree_index": 1,
            "n_trees_touched": 2,
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
            "n_trees": 100,
            "item_id_split_count": 40,
            "context_split_count": 60,
            "total_split_count": 120,
            "unaccounted_split_count": 20,
            "item_id_split_share": 40.0 / 120.0,
            "context_split_share": 60.0 / 120.0,
            "unaccounted_split_share": 20.0 / 120.0,
            "sum_allocated_context_gain": 30.0,
            "sum_allocated_context_split": 80,
        },
        "per_item": per_item,
        "pre_item": {
            "gain_sum": 10.0, "split_count": 20,
            "by_feature": {
                "f_age": {"gain": 7.0, "split_count": 12},
                "f_inc": {"gain": 3.0, "split_count": 8},
            },
        },
        "first_item_split_depth": {
            "min": 1, "p25": 1.0, "p50": 2.0, "p75": 3.0, "max": 4,
            "n_trees_with_item_split": 80,
        },
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


def test_share_column_names_do_not_collide_between_tables():
    """讀者硬傷 P1：三分表的 gain 佔比（分母＝total_gain）與 per-item 的分配佔比
    （分母＝各 item 加總，含共用重計）是兩種量，欄名不得同字，否則讀者把 25.7%
    誤讀成『占全模型 context 的 1/4』。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    breakdown_cols = set(_section(sections, _BREAKDOWN_TITLE).tables[0].columns)
    per_item_cols = set(_section(sections, _PER_ITEM_TITLE).tables[0].columns)
    # 兩表都有「gain」相關份額欄，但名稱必須不同（不能都叫「gain 佔比」）
    assert "gain 佔比" not in (breakdown_cols & per_item_cols), (
        "三分表與 per-item 表的份額欄同名『gain 佔比』，分母不同會被誤讀"
    )


def test_per_item_share_columns_signal_their_different_bases():
    """讀者硬傷 P4：per-item 一列三個 %欄基數不同（gain／split 佔比＝跨 item
    加總；私有%＝該列自己的 context_gain）。私有欄名要標出它是『占本 item』，
    不能只叫『私有%』讓讀者以為跟前兩欄同一把尺。"""
    table = _section(model_capacity.render(RESULT_WITH_ABILITY, {}),
                     _PER_ITEM_TITLE).tables[0]
    private_cols = [c for c in table.columns if "私有" in c and "%" in c]
    assert private_cols, "缺私有比例欄"
    assert any("本item" in c or "本 item" in c for c in private_cols), (
        f"私有比例欄名沒標出分母是『本 item』，實際欄名：{private_cols}"
    )


def test_reachable_concept_is_defined_in_per_item_section():
    """讀者硬傷 P2：per-item 分帳的核心機制詞『可達』從頭到尾要有白話定義，
    否則讀者無法理解為何各 item 切點加總遠超全域。"""
    section = _section(model_capacity.render(RESULT_WITH_ABILITY, {}),
                       _PER_ITEM_TITLE)
    text = section.description + " ".join(section.bullets)
    assert "可達" in text
    # 定義的特徵字：要講到「順著樹往下」或「還沒被排除」這類白話機制，不是只用詞
    assert ("排除" in text or "往下" in text or "還在候選" in text), (
        "『可達』只被使用、沒有白話定義"
    )


def test_overview_scatter_nav_is_neutral_not_causal():
    """讀者軟訊號：概覽導覽散點時不得用『容量有沒有轉成排序力』這種預設因果的
    措辭（範圍說明已明講 gain 高不代表排得好）——導覽只描述散點並排什麼。"""
    overview = _section(model_capacity.render(RESULT_WITH_ABILITY, {}), _OVERVIEW_TITLE)
    text = overview.description + " ".join(overview.bullets)
    assert "轉成排序力" not in text and "轉化成" not in text, (
        "概覽用了預設因果的措辭導覽散點"
    )


def test_ledger_points_to_vs_whole_model_for_global_scale():
    """讀者硬傷 A：ledger 的份額是「占各 item 加總」；要「占全模型」時，導引句
    必須指到 vs 全模型節，不能指三分表（三分表只有聚合、沒有 per-item 拆解，
    跟著走會落到答不了問題的表）。"""
    sec = _section(model_capacity.render(RESULT_WITH_ABILITY, {}), _PER_ITEM_TITLE)
    text = " ".join(sec.bullets)
    assert "vs 全模型" in text or "占全模型" in text, "ledger 沒導向 vs 全模型節"
    assert "回頭看三分表" not in text and "回三分表" not in text, (
        "ledger 把『占全模型』的問題指到三分表——三分表沒有 per-item 拆解"
    )


def test_no_editorializing_sentences_in_capacity_sections():
    """讀者硬傷 B（鐵則）：vs 全模型／pre-item 節不得替讀者下結論。
    釘死兩句具體的違規措辭。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    text = _all_text(sections)
    assert "專門為" not in text, "『模型專門為它建的』把 isolated 講成模型意圖（下結論）"
    assert "就知道模型" not in text, "『就知道模型…』直接替讀者下結論"


def test_exclusive_lens_states_it_does_not_sum_to_100():
    """讀者硬傷 D：獨佔份額不重計、跨 item 加總遠小於 100%（大多 context 是
    共用的）——這件事要講，否則讀者會假設獨佔切分成 100%。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    sec = next(s for s in sections if "vs 全模型" in s.title)
    text = sec.description + " ".join(sec.bullets)
    assert "共用" in text, "沒說明大多 context 是共用的"
    assert "並非 100" in text or "不到 100" in text or "遠小於 100" in text, (
        "沒說明獨佔加總並非 100%"
    )


def test_pre_item_name_equivalence_is_stated():
    """讀者硬傷 C：未分配／Pre-Item／item 切點前的殘餘同指一塊，要明說等同，
    不能讓讀者自己推。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    text = _all_text(sections)
    # 一句把幾個名字綁在一起（含「同指一塊」「同一塊」「即」之類等同語）
    assert ("未分配" in text and "Pre-Item" in text
            and ("同一塊" in text or "同指" in text or "＝Pre-Item" in text)), (
        "沒有一句明說『未分配＝Pre-Item＝item 切點前的殘餘』是同一塊"
    )


def test_overview_states_total_gain_and_total_splits():
    """(Q1) 概覽要有總量統計：total_gain 與 total splits 都要出現。"""
    overview = _section(model_capacity.render(RESULT_WITH_ABILITY, {}), _OVERVIEW_TITLE)
    text = overview.description + " ".join(overview.bullets)
    assert "total_gain" in text and "100" in text, "概覽缺 total_gain"
    assert "120" in text and ("total split" in text.lower() or "總 split" in text
                              or "split 總" in text), "概覽缺 total splits（總量）"


def test_vs_whole_model_has_split_lenses_too():
    """(Q2) vs 全模型節除了 gain 涵蓋/獨佔，也要有 split 涵蓋/獨佔（對稱）。"""
    sec = next(s for s in model_capacity.render(RESULT_WITH_ABILITY, {})
               if "vs 全模型" in s.title)
    cols = " ".join(sec.tables[0].columns)
    assert "gain" in cols and "split" in cols, f"欄位缺 gain 或 split：{cols}"
    joined = sec.tables[0].to_string()
    # ccard_ins split 涵蓋 50/120=41.7%、split 獨佔 8/120=6.7%
    assert "41.7%" in joined, "split 涵蓋沒算出來"
    assert "6.7%" in joined, "split 獨佔沒算出來"


def test_pre_item_table_has_split_percent_column():
    """(Q3) 未分配拆解的 per-feature 表要有「占未分配 split%」欄（分母＝pre_item
    的 split 總數）。fixture：f_age split 12 / 總 20 ＝ 60%。"""
    sec = next(s for s in model_capacity.render(RESULT_WITH_ABILITY, {})
               if "未分配" in s.title)
    cols = list(sec.tables[0].columns)
    assert any("split" in c and "%" in c for c in cols), \
        f"pre-item 表缺占未分配 split% 欄：{cols}"
    joined = sec.tables[0].to_string()
    assert "60.0%" in joined, "f_age 的占未分配 split%（12/20）沒算出來"


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


def test_breakdown_is_a_table_with_all_three_categories():
    """使用者回饋①：三分改用表格（不是長條圖）。三個類別（Item Prior／
    Post-Item Context／未分配）都要在表裡，不能只列一部分。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    section = _section(sections, _BREAKDOWN_TITLE)
    assert not section.figures, "三分應改用表格呈現，不再畫長條圖"
    assert section.tables, "三分 section 沒有表格"
    table = section.tables[0]
    assert len(table) == 3, "Item Prior／Post-Item Context／未分配三列都要在"


def test_breakdown_table_shows_gain_value_share_and_split():
    """使用者回饋①：除了 gain 佔比，也要呈現 gain 數值與 split 數。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    table = _section(sections, _BREAKDOWN_TITLE).tables[0]
    joined = table.to_string()
    # gain 值（60）、gain 佔比（60.0%）、split 數（40／60）都要出現
    assert "60.0" in joined            # item_id_gain 值
    assert "60.0%" in joined or "48" in joined  # 佔比欄（百分比）
    assert "40" in joined and "60" in joined    # item_id / context split 數
    # 三個欄位：類別 + gain + 佔比 + split，至少 4 欄
    assert table.shape[1] >= 4, f"三分表應有 gain 值/佔比/split 至少 4 欄，實際 {table.shape[1]}"


def test_split_three_way_is_complete_no_model_txt_caveat():
    """Route A 之後：ledger 有 total_split_count，split 三分完整（含未分配 split
    佔比），不再需要也不該出現舊的 "需 model.txt" caveat。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    table = _section(sections, _BREAKDOWN_TITLE).tables[0]
    text = _all_text(sections)
    assert "model.txt" not in text, "split 總數已在 ledger，不該再出現 model.txt caveat"
    # 三分表要有 split 佔比欄，且未分配那列是真實數字（不是 '—'）
    assert any("split 佔比" in c or "split%" in c for c in table.columns), \
        "三分表缺 split 佔比欄"
    joined = table.to_string()
    assert "16.7%" in joined, "未分配 split 佔比（20/120）沒算出來"


def test_vs_whole_model_section_present_with_two_lenses():
    """(b) 一個獨立區塊：per-item context 容量跟全模型比。要有『涵蓋』（占全模型
    gain%）與『獨佔』（私有/全模型%）兩把尺，並說明涵蓋會>100% 加總。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    sec = next((s for s in sections if "全模型" in s.title and "vs" in s.title.lower()
                or "vs 全模型" in s.title), None)
    assert sec is not None, f"缺『vs 全模型』區塊，實際：{[s.title for s in sections]}"
    text = sec.description + " ".join(sec.bullets) + \
        " ".join(t.to_string() for t in sec.tables)
    # 涵蓋 20% (=20/100) 與 獨佔 5% (=5/100) 都要出現
    assert "20.0%" in text, "涵蓋（占全模型 gain%）沒出現"
    assert "5.0%" in text, "獨佔（私有/全模型%）沒出現"
    # 誠實：涵蓋是重疊量、會>100% 加總
    assert "100%" in text or "重計" in text or "涵蓋" in text, \
        "沒說明涵蓋量會>100% 加總 / 是重疊量"


def test_pre_item_breakdown_section_present():
    """(d#1) 未分配（pre-item）按特徵拆解的區塊：per-feature 表 + 該 item 切點
    深度（d#2）。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    sec = next((s for s in sections if "未分配" in s.title or "pre-item" in s.title.lower()), None)
    assert sec is not None, f"缺『未分配 pre-item 拆解』區塊，實際：{[s.title for s in sections]}"
    assert sec.tables, "pre-item 區塊缺 per-feature 表"
    joined = sec.tables[0].to_string()
    assert "f_age" in joined and "f_inc" in joined, "per-feature 表缺特徵列"
    # gain 遞減：f_age(7) 在 f_inc(3) 之前
    assert joined.index("f_age") < joined.index("f_inc")


def test_item_split_depth_shown_in_pre_item_section():
    """(d#2) item 切點深度摘要（root=1）要出現在 pre-item 區塊——量 item 條件化
    坐落多深，幫判讀未分配那塊。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    sec = next((s for s in sections if "未分配" in s.title or "pre-item" in s.title.lower()), None)
    assert sec is not None
    text = sec.description + " ".join(sec.bullets)
    assert "深度" in text, "沒提 item 切點深度"
    assert "80" in text, "深度摘要的來源樹數（n_trees_with_item_split=80）沒帶出"


def test_per_item_section_has_bar_and_rich_table():
    """per-item 分配長條圖（形狀）依 compute 已排好的順序；同節另附完整明細表。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    section = _section(sections, _PER_ITEM_TITLE)
    assert section.figures, "per-item section 少了分配長條圖"
    items_shown = list(section.figures[0].data[0].x)
    assert items_shown == ["ccard_ins", "fund_bond"], "長條圖未沿用 compute 排序"
    assert section.tables, "per-item section 少了完整明細表（使用者回饋②）"


def test_per_item_table_keeps_rich_columns():
    """使用者回饋②：明細表不能被砍到只剩 gain——ledger 記的 split 數、私有
    context gain、item-routing 足跡都要留在表裡。用實際數值抓，避免只驗欄名。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    table = _section(sections, _PER_ITEM_TITLE).tables[0]
    joined = table.to_string()
    # context 切點數 50、私有 gain 5、isolating 切點 30、觸及樹數 4：各挑一個實際值
    assert "50" in joined, "context_split_count（50）沒進表"
    assert "30" in joined, "isolating_split_count（30）沒進表"
    assert "25.0%" in joined or "0.25" in joined, "私有 context gain 佔比沒進表"
    assert table.shape[1] >= 10, f"明細表欄位被砍太多，只剩 {table.shape[1]} 欄"


def test_per_item_table_keeps_relative_concentration_columns():
    """使用者回饋②「整個表格都可以留」：codex §6 的「/max item」「/median item」
    相對集中度欄要在。用 top item ＝100% 這個特徵值抓（其他欄不會剛好同時
    出現 100.0% 於 /第一名 與 /中位 兩欄）。"""
    sections = model_capacity.render(RESULT_WITH_ABILITY, {})
    table = _section(sections, _PER_ITEM_TITLE).tables[0]
    assert "/第一名" in table.columns, "缺『相對第一名』集中度欄"
    assert "/中位" in table.columns, "缺『相對中位 item』集中度欄"
    top = table.iloc[0]
    assert top["/第一名"] == "100.0%", "第一名 item 的『/第一名』欄應為 100.0%"


def test_completeness_section_lists_notes():
    """notes 原文照登，讀者要看得到「為什麼沒有 ability 資料」。"""
    sections = model_capacity.render(RESULT_WITHOUT_ABILITY, {})
    section = _section(sections, _COMPLETENESS_TITLE)
    assert "item_ability" in "\n".join(section.bullets)


def test_fallback_ledger_reason_reaches_the_rendered_page():
    """粗帳本降級（``gain_ledger.py:_coarse_ledger``）算出的 compute() 結果，
    餵進 render() 之後，「這是降級版本」的原因必須真的出現在頁面文字裡——
    不能只在 JSON 的 notes 裡有，頁面上卻悄悄消失。串接 compute()＋render()
    （而不是手造一份 RESULT）是為了驗證兩層真的接得起來。
    """
    from recsys_tfb.diagnosis.metric.model_capacity._compute import compute

    coarse = {
        "enabled": True, "item_feature": "prod_name", "n_trees": 100,
        "n_items": None, "total_gain": 60.0,
        "item_id": {"split_count": 40, "gain_sum": 60.0, "gain_share": 1.0},
        "context": None,
        "per_item": None,
        "fallback": True,
        "notes": ["preprocessor 缺 category_mappings[item 欄]，降級為粗帳本"],
    }
    params = {"evaluation": {"diagnosis": {"model_capacity": {"enabled": True}}}}
    result = compute(coarse, None, params)
    sections = model_capacity.render(result, {})
    text = _all_text(sections)
    assert "粗帳本" in text or "降級" in text, (
        f"粗帳本降級原因沒有出現在渲染出的頁面文字裡，實際章節：{[s.title for s in sections]}"
    )
