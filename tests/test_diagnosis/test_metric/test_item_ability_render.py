"""item_ability 呈現層的測試：raw vs query-centered AUC 的對照。

移植自 ``test_config_shift_render.py`` 的通用契約（見任務指示的 16 條清單），
其餘為本項專屬——AUC 是 macro mAP 的 proxy 而非分解、gap 保留正負號、單一
對照點（0.500）。

``RESULT`` 的鍵名取自 ``_compute.compute`` 的實際輸出（見該模組 docstring 與
``per_item`` 逐欄 ``FIELD_NOTES``），不是取自計畫檔。
"""
from __future__ import annotations

import copy

import pandas as pd
import pytest
import plotly.graph_objects as go

from recsys_tfb.diagnosis.metric import contract, item_ability
from recsys_tfb.report import ReportSection, ScopeNote

_SCATTER_TITLE = "raw vs query-centered AUC 散點"
_PER_ITEM_AUC_TITLE = "逐 item 的 AUC（含信賴區間）"
_GAP_TITLE = "AUC 差：raw − centered"
_RANK_TITLE = "正例名次百分位分布"
_COMPLETENESS_TITLE = "本次執行的完整性檢查"


def _per_item_row(
    item: str, ap: float, n_pos: int, n_neg: int,
    raw: float | None, raw_lo: float | None, raw_hi: float | None,
    centered: float | None, centered_lo: float | None, centered_hi: float | None,
    median_rank: float | None, p25: float | None, p75: float | None, p90: float | None,
) -> dict:
    gap = None if raw is None or centered is None else raw - centered
    return {
        "item": item, "ap": ap, "n_pos": n_pos, "n_neg": n_neg,
        "query_centered_auc": centered,
        "query_centered_auc_ci_low": centered_lo,
        "query_centered_auc_ci_high": centered_hi,
        "raw_within_item_auc": raw,
        "raw_within_item_auc_ci_low": raw_lo,
        "raw_within_item_auc_ci_high": raw_hi,
        "auc_gap_raw_minus_centered": gap,
        "mean_relative_score_pos": 0.1 if raw is not None else None,
        "mean_relative_score_neg": -0.05 if raw is not None else None,
        "relative_score_gap": (
            None if raw is None else 0.15
        ),
        "median_positive_rank_percentile": median_rank,
        "p25_positive_rank_percentile": p25,
        "p75_positive_rank_percentile": p75,
        "p90_positive_rank_percentile": p90,
        "positive_rank_percentiles": (
            [] if median_rank is None else [median_rank]
        ),
        "n_pos_ap": n_pos,
    }


def _result() -> dict:
    """一份形狀與 ``compute`` 實際輸出一致的結果（每次回新的 deep copy）。

    ``per_item`` 已依 ``ap`` 遞增排序（與 ``_compute.compute`` 的實際排序
    一致）：item_a（ap 最低）→ item_b → item_c（AUC 未算出，作為「已知靜默
    失效」的樣本）。

    item_a 的 ``auc_gap_raw_minus_centered`` 刻意是負值（centered 0.55 >
    raw 0.50）——``test_gap_bar_keeps_the_sign`` 需要至少一個負的 gap；
    item_b 是正值（raw 0.75 > centered 0.60），兩個方向都有樣本。
    """
    return copy.deepcopy({
        "enabled": True,
        "score_col_used": "score_uncalibrated",
        "metric_params": {
            "min_positives": 1, "shrinkage_k": 0.0, "weight_alpha": 0.0,
        },
        "logit_notes": [],
        "top_n": 2,
        "n_rows": 400,
        "n_queries": 40,
        "n_entities": 40,
        "n_items": 3,
        "n_positive_rows": 60,
        "macro_per_item_map": 0.4123,
        "ci": {"enabled": True, "n_boot": 200, "seed": 42},
        "per_item": [
            _per_item_row(
                "item_a", 0.20, 10, 30,
                raw=0.50, raw_lo=0.40, raw_hi=0.60,
                centered=0.55, centered_lo=0.45, centered_hi=0.65,
                median_rank=0.40, p25=0.20, p75=0.60, p90=0.80,
            ),
            _per_item_row(
                "item_b", 0.35, 20, 40,
                raw=0.75, raw_lo=0.65, raw_hi=0.85,
                centered=0.60, centered_lo=0.50, centered_hi=0.70,
                median_rank=0.30, p25=0.15, p75=0.45, p90=0.60,
            ),
            _per_item_row(
                "item_c", 0.50, 5, 0,
                raw=None, raw_lo=None, raw_hi=None,
                centered=None, centered_lo=None, centered_hi=None,
                median_rank=0.50, p25=0.30, p75=0.70, p90=0.90,
            ),
        ],
        "sample_meta": {"sampling_description": "分層抽樣：正例 query 全取。"},
        "field_notes": {},
        "notes": [],
    })


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


# ---- 基本形狀（移植自 test_config_shift_render.py）---------------------


def test_render_returns_sections():
    sections = item_ability.render(_result(), {})
    assert isinstance(sections, tuple)
    assert all(isinstance(s, ReportSection) for s in sections)


def test_render_returns_multiple_sections():
    """一張圖一個 section，各自帶標題。四節內容 ＋ 完整性檢查，本項最多 5 節。"""
    sections = item_ability.render(_result(), {})
    assert len(sections) >= 4
    assert all(s.title.strip() for s in sections)


def test_section_titles_are_distinct():
    titles = [s.title for s in item_ability.render(_result(), {})]
    assert len(set(titles)) == len(titles)


def test_every_section_with_a_figure_has_its_own_explanation():
    for section in item_ability.render(_result(), {}):
        if section.figures or section.tables:
            assert section.description.strip() or section.bullets, (
                f"section {section.title!r} 有圖表卻沒有自己的說明"
            )


def test_formulas_use_plain_unicode_not_latex():
    """生產限制是 no network、no additional packages——MathJax／KaTeX 一定
    載不到，LaTeX 原始碼會原樣印在報表上。"""
    for section in item_ability.render(_result(), {}):
        assert "\\frac" not in section.formula
        assert "$" not in section.formula


def test_no_section_description_is_a_wall_of_text():
    for section in item_ability.render(_result(), {}):
        assert len(section.description) <= 120, (
            f"section {section.title!r} 的 description 有 "
            f"{len(section.description)} 字元，超過 120"
        )


def test_bullets_are_one_sentence_each():
    """完整性檢查排除在外：它的 bullet 有一部分是 notes 原文照登，長度不歸
    呈現層管（與 config_shift 的例外理由一致）。
    """
    for section in item_ability.render(_result(), {}):
        if section.title == _COMPLETENESS_TITLE:
            continue
        for bullet in section.bullets:
            assert len(bullet) <= 160, f"bullet 過長：{bullet[:40]}…"


def test_render_returns_empty_tuple_when_disabled():
    assert item_ability.render({"enabled": False}, {}) == ()


def test_render_is_pure_and_does_not_mutate_input():
    result = _result()
    before = copy.deepcopy(result)
    item_ability.render(result, {})
    assert result == before


def test_scope_declares_what_it_cannot_tell():
    assert isinstance(item_ability.SCOPE, ScopeNote)
    assert item_ability.SCOPE.blind_to
    assert "有正例" in item_ability.SCOPE.population


def test_module_level_scope_carries_no_run_specific_facts():
    assert item_ability.SCOPE.sampling == ""


def test_no_verdict_vocabulary_in_output():
    """禁用字清單比 config_shift 版多三個（偏低／偏高／良好）——這是三條
    鐵則段落逐字列出的額外禁用詞。
    """
    forbidden = [
        "建議", "應該", "異常", "不足", "有問題", "健康", "通過", "失敗",
        "偏低", "偏高", "良好",
        "verdict", "severity", "recommend",
    ]
    scope = item_ability.SCOPE
    text = _all_text(item_ability.render(_result(), {})) + "\n".join(
        [scope.measures, scope.population,
         *scope.blind_to, *scope.reference_points]
    )
    hits = [word for word in forbidden if word.lower() in text.lower()]
    assert hits == [], f"呈現層出現下結論的字眼：{hits}"


def test_render_survives_an_empty_sample():
    """空抽樣是良性退化，不是壞輸入——不該炸。"""
    result = _result()
    result.update({
        "per_item": [], "n_rows": 0, "n_queries": 0, "n_entities": 0,
        "n_items": 0, "n_positive_rows": 0, "macro_per_item_map": None,
        "notes": ["診斷抽樣為空——per-item AUC 均未計算。"],
    })
    sections = item_ability.render(result, {})
    assert all(isinstance(s, ReportSection) for s in sections)
    assert "診斷抽樣為空" in _all_text(sections)


def test_empty_sample_produces_no_hollow_sections():
    result = _result()
    result.update({
        "per_item": [], "n_rows": 0, "n_queries": 0, "n_entities": 0,
        "n_items": 0, "n_positive_rows": 0, "macro_per_item_map": None,
        "notes": ["診斷抽樣為空——per-item AUC 均未計算。"],
    })
    for section in item_ability.render(result, {}):
        assert section.figures or section.tables or section.bullets, (
            f"section {section.title!r} 是空殼"
        )


def test_tables_and_titles_stay_aligned():
    for section in item_ability.render(_result(), {}):
        assert len(section.tables) == len(section.table_titles)


def test_module_satisfies_contract():
    contract.check_module(item_ability)


# ---- 本項專屬 ------------------------------------------------------------


def test_scope_states_auc_is_not_metric_native():
    """誠實條款：AUC 不是 macro mAP 的分解，必須寫在 blind_to。"""
    joined = " ".join(item_ability.SCOPE.blind_to)
    assert "不同 query" in joined
    assert "proxy" in joined or "代理" in joined


def test_scope_warns_auc_not_comparable_externally():
    joined = " ".join(item_ability.SCOPE.blind_to) + item_ability.SCOPE.population
    assert "有正例" in joined


def test_scatter_has_the_diagonal_reference_line():
    """y=x 是這張圖的全部意義——沒有對角線，讀者無從判斷偏離的方向。"""
    sections = item_ability.render(_result(), {})
    section = _section(sections, _SCATTER_TITLE)
    assert section.figures, "散點圖 section 沒有畫出圖"
    fig = section.figures[0]
    shapes = fig.layout.shapes
    assert shapes, "散點圖缺少對角參考線"
    line = shapes[0]
    assert line.x0 == pytest.approx(line.y0)
    assert line.x1 == pytest.approx(line.y1)
    assert line.x0 != line.x1, "對角線不能退化成一個點"


def test_gap_bar_keeps_the_sign():
    """AUC 差不得取絕對值：方向就是「活躍度混入」的方向。
    fixture 裡 item_a 的 gap 是負值（centered > raw），這裡斷言圖上真的
    出現負值。
    """
    sections = item_ability.render(_result(), {})
    section = _section(sections, _GAP_TITLE)
    assert section.figures, "AUC 差 section 沒有畫出圖"
    fig = section.figures[0]
    ys = [float(v) for v in fig.data[0].y]
    assert any(v < 0 for v in ys), f"fixture 應含負的 auc_gap，實際：{ys}"
    assert any(v > 0 for v in ys), f"fixture 應同時含正的 auc_gap，實際：{ys}"


def test_reference_point_is_stated_once_not_twice():
    """只有 0.500 一個對照點。popularity baseline 與它逐位元重合，
    已撤除——若有人把它加回來，這條會紅。
    """
    text = _all_text(item_ability.render(_result(), {}))
    assert "0.500" in text
    assert "購買率" not in text
    assert "popularity" not in text.lower()


# ---- 其餘結構性驗證（per-item AUC 圖含誤差線／排名分布節）-----------------


def test_per_item_auc_section_has_error_bars():
    sections = item_ability.render(_result(), {})
    section = _section(sections, _PER_ITEM_AUC_TITLE)
    assert section.figures, "逐 item AUC section 沒有畫出圖"
    assert any(
        fig.data[0].error_y is not None and fig.data[0].error_y.array is not None
        for fig in section.figures
    ), "逐 item AUC 條圖應含 CI 誤差線"


def test_rank_percentile_section_only_covers_top_n():
    """``top_n=2`` 時只列 AP 最低的兩個 item（item_a／item_b），item_c 不進來。"""
    sections = item_ability.render(_result(), {})
    section = _section(sections, _RANK_TITLE)
    items_shown = set()
    for table in section.tables:
        if "item" in table.columns:
            items_shown.update(table["item"].tolist())
    assert items_shown == {"item_a", "item_b"}


def test_completeness_section_lists_items_missing_auc():
    """item_c 沒有算出 AUC（n_neg=0）——完整性檢查必須點名它，不能悄悄消失。"""
    sections = item_ability.render(_result(), {})
    section = _section(sections, _COMPLETENESS_TITLE)
    assert "item_c" in "\n".join(section.bullets)
