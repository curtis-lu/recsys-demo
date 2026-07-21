"""item_ability 呈現層的測試：raw vs query-centered AUC 的對照。

移植自 ``test_config_shift_render.py`` 的通用契約（見任務指示的 16 條清單），
其餘為本項專屬——AUC 是 macro mAP 的 proxy 而非分解、gap 保留正負號、單一
對照點（0.500）、名次分布用名次（非百分位）呈現、多一張 AUC vs AP。

``RESULT`` 的鍵名取自 ``_compute.compute`` 的實際輸出（見該模組 docstring 與
``per_item`` 逐欄 ``FIELD_NOTES``），不是取自計畫檔。
"""
from __future__ import annotations

import copy

import numpy as np
import pandas as pd
import pytest
import plotly.graph_objects as go

from recsys_tfb.diagnosis.metric import contract, item_ability
from recsys_tfb.report import ReportSection, ScopeNote

_SUMMARY_TITLE = "概覽"
_FOUNDATION_TITLE = "核心概念：兩個 AUC 與它們的差"
_SCATTER_TITLE = "raw vs query-centered AUC 散點"
_PER_ITEM_AUC_TITLE = "逐 item 的 AUC（含信賴區間）"
_AUC_VS_AP_TITLE = "query-centered AUC vs AP"
_RANK_TITLE = "正例名次分布"
_COMPLETENESS_TITLE = "本次執行的完整性檢查"


def _per_item_row(
    item: str, ap: float, n_pos: int, n_neg: int,
    raw: float | None, raw_lo: float | None, raw_hi: float | None,
    centered: float | None, centered_lo: float | None, centered_hi: float | None,
    ranks: list[int],
) -> dict:
    """一列 per_item；``ranks`` 是該 item 正例列的名次（1＝排最前），其餘百分位
    比照 ``_compute`` 由 ranks 導出。"""
    gap = None if raw is None or centered is None else raw - centered
    if ranks:
        arr = np.array(ranks, dtype=float)
        p10 = float(np.percentile(arr, 10))
        p25 = float(np.percentile(arr, 25))
        med = float(np.median(arr))
        p75 = float(np.percentile(arr, 75))
        p90 = float(np.percentile(arr, 90))
    else:
        p10 = p25 = med = p75 = p90 = None
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
        "relative_score_gap": None if raw is None else 0.15,
        "median_positive_rank": med,
        "p10_positive_rank": p10,
        "p25_positive_rank": p25,
        "p75_positive_rank": p75,
        "p90_positive_rank": p90,
        "positive_ranks": list(ranks),
        "n_pos_ap": n_pos,
    }


def _result() -> dict:
    """一份形狀與 ``compute`` 實際輸出一致的結果（每次回新的 deep copy）。

    ``per_item`` 已依 ``ap`` 遞增排序（與 ``_compute.compute`` 一致）：
    item_a（ap 最低）→ item_b → item_c（AUC 未算出，作為「已知靜默失效」）。

    item_a 的 ``auc_gap_raw_minus_centered`` 刻意是負值（centered 0.55 >
    raw 0.50）、item_b 是正值（raw 0.75 > centered 0.60），兩個方向都有樣本，
    讓 ``test_scatter_table_keeps_the_gap_sign`` 兩邊都驗得到。每 query 4 個
    候選，名次落在 1–4。
    """
    return copy.deepcopy({
        "enabled": True,
        "score_col_used": "score_uncalibrated",
        "metric_params": {
            "k": None, "min_positives": 1, "shrinkage_k": 0.0, "weight_alpha": 0.0,
        },
        "logit_notes": [],
        "top_n": 2,
        "n_rows": 400,
        "n_queries": 40,
        "n_entities": 40,
        "n_items": 3,
        "n_positive_rows": 60,
        "macro_per_item_map": 0.4123,
        "candidates_per_query": {"min": 4, "median": 4.0, "max": 4},
        "ci": {"enabled": True, "n_boot": 200, "seed": 42},
        "per_item": [
            _per_item_row(
                "item_a", 0.20, 10, 30,
                raw=0.50, raw_lo=0.40, raw_hi=0.60,
                centered=0.55, centered_lo=0.45, centered_hi=0.65,
                ranks=[4, 4, 3, 4, 3, 4, 4, 3, 4, 4],
            ),
            _per_item_row(
                "item_b", 0.35, 20, 40,
                raw=0.75, raw_lo=0.65, raw_hi=0.85,
                centered=0.60, centered_lo=0.50, centered_hi=0.70,
                ranks=[1, 2, 1, 2, 1, 1, 2, 1, 2, 1] * 2,
            ),
            _per_item_row(
                "item_c", 0.50, 5, 0,
                raw=None, raw_lo=None, raw_hi=None,
                centered=None, centered_lo=None, centered_hi=None,
                ranks=[2, 3, 2, 3, 2],
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
    """概覽＋核心概念＋4 個資料區＋完整性檢查＝7 節。"""
    sections = item_ability.render(_result(), {})
    assert len(sections) >= 6
    assert all(s.title.strip() for s in sections)


def test_section_titles_are_distinct():
    titles = [s.title for s in item_ability.render(_result(), {})]
    assert len(set(titles)) == len(titles)


def test_summary_and_foundation_come_first():
    """定向兩塊（概覽、核心概念）永遠在最前、完整性檢查永遠殿後。"""
    titles = [s.title for s in item_ability.render(_result(), {})]
    assert titles[0] == _SUMMARY_TITLE
    assert titles[1] == _FOUNDATION_TITLE
    assert titles[-1] == _COMPLETENESS_TITLE


def test_every_section_with_a_figure_has_its_own_explanation():
    for section in item_ability.render(_result(), {}):
        if section.figures or section.tables:
            assert section.description.strip() or section.bullets, (
                f"section {section.title!r} 有圖表卻沒有自己的說明"
            )


def test_formulas_use_plain_unicode_not_latex():
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


def test_no_literal_markdown_bold():
    """報表文字經 HTML escape、不轉 markdown：`**粗體**` 會顯示成字面星號。"""
    assert "**" not in _all_text(item_ability.render(_result(), {}))


def test_no_customer_activity_jargon():
    """『客戶活躍度』是把分數水準解讀成商業活躍度的臆測，且方向上只涵蓋『撐高』
    一半——一律改成機械描述『客戶整體分數水準』。"""
    assert "客戶活躍度" not in _all_text(item_ability.render(_result(), {}))


def test_render_survives_an_empty_sample():
    result = _result()
    result.update({
        "per_item": [], "n_rows": 0, "n_queries": 0, "n_entities": 0,
        "n_items": 0, "n_positive_rows": 0, "macro_per_item_map": None,
        "candidates_per_query": None,
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
        "candidates_per_query": None,
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
    joined = " ".join(item_ability.SCOPE.blind_to)
    assert "不同 query" in joined
    assert "proxy" in joined or "代理" in joined


def test_scope_warns_auc_not_comparable_externally():
    joined = " ".join(item_ability.SCOPE.blind_to) + item_ability.SCOPE.population
    assert "有正例" in joined


def test_foundation_explains_both_auc_and_gap_direction():
    """核心概念要把 within-item AUC 講成機率（Mann-Whitney 白話），並說清楚
    gap 兩個方向的意義——不能只講『撐高』一半。"""
    section = _section(item_ability.render(_result(), {}), _FOUNDATION_TITLE)
    text = section.formula + " ".join(section.bullets)
    assert "機率" in text, "within-item AUC 要用『機率』白話解釋"
    assert "正值" in text and "負值" in text, "gap 兩個方向都要講"
    assert "centered" in text.lower()


def test_weighted_auc_formula_is_interpretation_not_implementation():
    """`Σ pos_w·(neg_before ＋ 0.5·neg_tie)` 是內部線性掃描，讀者看不懂——
    公式必須換成 Mann-Whitney 的白話（機率），不是實作式。"""
    text = _all_text(item_ability.render(_result(), {}))
    assert "neg_before" not in text
    assert "pos_w" not in text


def test_scatter_has_the_diagonal_reference_line():
    """y=x 是這張圖的全部意義——沒有對角線，讀者無從判斷偏離的方向。"""
    section = _section(item_ability.render(_result(), {}), _SCATTER_TITLE)
    assert section.figures, "散點圖 section 沒有畫出圖"
    line = section.figures[0].layout.shapes[0]
    assert line.x0 == pytest.approx(line.y0)
    assert line.x1 == pytest.approx(line.y1)
    assert line.x0 != line.x1, "對角線不能退化成一個點"


def test_scatter_table_keeps_the_gap_sign():
    """gap 不得取絕對值：散點旁的精確值表要同時看得到正、負 gap。
    fixture 裡 item_a gap 為負、item_b 為正。"""
    section = _section(item_ability.render(_result(), {}), _SCATTER_TITLE)
    text = "\n".join(t.to_string() for t in section.tables)
    assert "-0.05" in text, f"應含負的 gap，實際表格：\n{text}"
    assert "+0.15" in text, f"應含正的 gap，實際表格：\n{text}"


def test_reference_point_is_stated_once_not_twice():
    """只有 0.500 一個對照點。popularity baseline 與它逐位元重合、已撤除。"""
    text = _all_text(item_ability.render(_result(), {}))
    assert "0.500" in text
    assert "購買率" not in text
    assert "popularity" not in text.lower()


def test_per_item_auc_section_has_error_bars():
    section = _section(item_ability.render(_result(), {}), _PER_ITEM_AUC_TITLE)
    assert section.figures, "逐 item AUC section 沒有畫出圖"
    assert any(
        fig.data[0].error_y is not None and fig.data[0].error_y.array is not None
        for fig in section.figures
    ), "逐 item AUC 條圖應含 CI 誤差線"


def test_auc_vs_ap_section_exists_with_a_figure():
    """使用者回饋：少了 AUC vs AP 的圖表。"""
    section = _section(item_ability.render(_result(), {}), _AUC_VS_AP_TITLE)
    assert section.figures, "AUC vs AP section 沒有畫出圖"
    fig = section.figures[0]
    # 縱軸是 AP、橫軸是 query-centered AUC
    assert "AP" in (fig.layout.yaxis.title.text or "")
    assert "AUC" in (fig.layout.xaxis.title.text or "")


def test_rank_section_is_a_heatmap_of_ranks_not_percentiles():
    """使用者回饋：名次用 heatmap、值直接用名次（1＝rank 1），不是百分位。
    percentile（0.125 這種）一律 <1；名次一律 ≥1。fixture 每 query 4 候選，
    名次落在 1–4。"""
    section = _section(item_ability.render(_result(), {}), _RANK_TITLE)
    assert section.figures, "名次分布 section 沒有畫出圖"
    fig = section.figures[0]
    assert isinstance(fig.data[0], go.Heatmap), "名次分布要用 heatmap"
    z = np.array(fig.data[0].z, dtype=float)
    finite = z[np.isfinite(z)]
    assert finite.size > 0
    assert finite.min() >= 1.0, f"名次應 ≥1（非百分位），實得最小值 {finite.min()}"
    assert finite.max() <= 4.0, f"fixture 每 query 4 候選，名次應 ≤4，實得 {finite.max()}"
    # 格子上要有數字（讀得出名次），不是只有顏色
    assert fig.data[0].text is not None, "heatmap 每格要標出名次數字"


def test_rank_section_reports_candidate_count_as_the_denominator():
    """名次要能讀成『幾中選幾』：候選數（名次的分母）必須出現。"""
    section = _section(item_ability.render(_result(), {}), _RANK_TITLE)
    text = " ".join(section.bullets)
    assert "候選" in text and "4" in text


def test_completeness_section_lists_items_missing_auc():
    """item_c 沒有算出 AUC（n_neg=0）——完整性檢查必須點名它，不能悄悄消失。"""
    section = _section(item_ability.render(_result(), {}), _COMPLETENESS_TITLE)
    assert "item_c" in "\n".join(section.bullets)


# ---- 讀者 subagent 反饋修正的迴歸護欄 ------------------------------------


def test_no_causal_speculation_in_auc_vs_ap():
    """鐵則 1：不得替讀者臆測成因。AUC-vs-AP 區只能保留機制陳述＋導覽，不得
    出現「可能有別的 item 壓在前面」這種候選成因（讀者反饋 C）。"""
    section = _section(item_ability.render(_result(), {}), _AUC_VS_AP_TITLE)
    text = section.description + " ".join(section.bullets)
    assert "壓在前面" not in text, "不得臆測『別的 item 壓在前面』這個成因"
    assert "壓制帳本" in text, "導覽到壓制帳本應保留"


def test_macro_map_is_defined_and_traceable():
    """讀者反饋 A：全報表第一個數字 macro per-item mAP 必須有定義、能追回
    per-item AP，不能只丟一個 0.54 在頂上。"""
    section = _section(item_ability.render(_result(), {}), _SUMMARY_TITLE)
    text = " ".join(section.bullets)
    assert "各 item 的 AP" in text
    assert "平均" in text


def test_rank_section_notes_non_integer_interpolation():
    """讀者反饋 F：名次熱圖會出現非整數（百分位內插），必須說明，否則讀者
    看到『5.4 名』會以為算錯。"""
    section = _section(item_ability.render(_result(), {}), _RANK_TITLE)
    assert "內插" in " ".join(section.bullets)


def test_summary_does_not_misreference_table_position():
    """讀者反饋 D：導覽 bullet 在表格之前渲染，不能用『上表』指下方的表。"""
    assert "上表" not in _all_text(item_ability.render(_result(), {}))


def test_scatter_describes_gap_as_vertical_offset_not_distance():
    """讀者反饋 B：對 y=x，『點到對角線的距離』（垂直投影）＝|gap|/√2，會被讀小
    √2 倍；正確的是點沿 y 軸的鉛直落差＝|gap|。措辭不得用『點到對角線的距離』。"""
    section = _section(item_ability.render(_result(), {}), _SCATTER_TITLE)
    text = section.description + " ".join(section.bullets)
    assert "點到對角線的距離" not in text
    assert "y 軸" in text
