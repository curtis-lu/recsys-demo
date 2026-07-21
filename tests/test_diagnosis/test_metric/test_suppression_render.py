"""suppression 呈現層的測試：壓制矩陣熱圖 vs 交叉購買泡泡格圖（同軸序）。

``RESULT`` 不手寫成 dict——直接用 ``compute`` 對合成樣本跑出真結果（見
``_result``／``_result_with_items``），理由見任務指示：Plan 2 最貴的一次
假綠就是 fixture 形狀憑計畫稿捏造，跟 ``compute`` 實際輸出對不上。

合成樣本的設計（``_synthetic_frame``）刻意讓每個 item 同時：
1. 出現在 ``axis_order``（透過一個環狀的壓制關係：item i 壓制 item i+1）；
2. 在 ``cross_purchase`` 裡至少出現一次 item_j、一次 item_k（透過一個额外
   的「同時購買」query，讓 item i 與 item i+1 在同一個 query 單位都是
   label=1）。
少了第 2 點，``cross_purchase_stats`` 對「本來就沒有任何正例的 item」不會
產生任何列（``_two_query_sample`` 就是這種案例：A 恆為負例，直接餵給
``compute`` 後 ``cross_purchase`` 是空的——實測過，見 test_suppression.py
的 ``_sample_with_suppression_and_cross_purchase`` 同一個教訓）。
"""
from __future__ import annotations

import copy

import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric import contract, suppression
from recsys_tfb.report import ReportSection, ScopeNote
from recsys_tfb.report.figures import MAX_FIGURE_POINTS

_DEFS_TITLE = "本區數字定義"
_SUMMARY_TITLE = "per-item"
_MATRIX_TITLE = "壓制矩陣"
_BUBBLE_TITLE = "交叉購買 lift"
_EXAMPLES_TITLE = "具體案例"
_BY_SUPPRESSOR_TITLE = "壓制者明細"


def _section_text(section) -> str:
    """一個 section 的所有可讀文字（含定義小表的內容）。"""
    parts = [section.title, section.description, section.formula]
    parts.extend(section.bullets)
    parts.extend(section.table_titles)
    parts.extend(t.to_string() for t in section.tables)
    return "\n".join(parts)
_COMPLETENESS_TITLE = "本次執行的完整性檢查"

_N_AXIS_MAX = 44  # floor(sqrt(MAX_FIGURE_POINTS)) == floor(sqrt(2000))

_SCHEMA = {
    "time": "snap_date", "entity": ["cust_id"],
    "item": "prod_name", "label": "label", "score": "score",
}


def _params(top_examples: int = 50) -> dict:
    return {
        "schema": _SCHEMA,
        "evaluation": {"diagnosis": {
            "sample": {"seed": 42},
            "suppression": {"enabled": True, "top_examples": top_examples},
        }},
    }


def _row(cust: str, item: str, label: int, score: float) -> dict:
    return {"snap_date": "2026-01-31", "cust_id": cust, "prod_name": item,
            "label": label, "score_uncalibrated": score, "score": 0.5,
            "stratum": "take_all", "inclusion_weight": 1.0}


def _synthetic_frame(n_items: int) -> pd.DataFrame:
    """``n_items`` 個 item 排成一個環：item i（負例、分數較高）壓制
    item i+1（正例、分數較低）——保證每個 item 同時是某一格的受害者、也是
    另一格的壓制者，``axis_order`` 因此涵蓋全部 ``n_items`` 個。額外的
    「共買」query 讓 item i 與 item i+1 在同一個 query 單位都是 label=1，
    保證每個 item 在 ``cross_purchase`` 裡至少出現一次 item_j、一次
    item_k（實測驗證見任務執行記錄，n=4/22/60 皆涵蓋全部 item）。
    """
    items = [f"item{i:03d}" for i in range(n_items)]
    rows: list[dict] = []
    for i in range(n_items):
        j = (i + 1) % n_items
        rows.append(_row(f"s{i:03d}", items[i], 0, 0.9))
        rows.append(_row(f"s{i:03d}", items[j], 1, 0.4))
        rows.append(_row(f"b{i:03d}", items[i], 1, 0.5))
        rows.append(_row(f"b{i:03d}", items[j], 1, 0.5))
    return pd.DataFrame(rows)


def _compute_result(n_items: int) -> dict:
    from recsys_tfb.diagnosis.metric.suppression._compute import compute
    df = _synthetic_frame(n_items)
    return compute((df, {"n_queries": 2 * n_items}), _params())


def _result() -> dict:
    """4 個 item，遠低於點數預算——不觸發截斷。"""
    return copy.deepcopy(_compute_result(4))


def _result_with_items(n: int) -> dict:
    return copy.deepcopy(_compute_result(n))


def _all_text(sections) -> str:
    if isinstance(sections, ReportSection):
        sections = (sections,)
    parts: list[str] = []
    for section in sections:
        parts.extend([section.title, section.description, section.formula])
        parts.extend(section.bullets)
        parts.extend(section.table_titles)
        parts.extend(table.to_string() for table in section.tables)
    return "\n".join(parts)


def _section(sections, title_substr: str) -> ReportSection:
    match = next((s for s in sections if title_substr in s.title), None)
    assert match is not None, (
        f"section 標題含 {title_substr!r} 的不存在，實際標題："
        f"{[s.title for s in sections]}"
    )
    return match


def _figure_of_type(sections, type_name: str):
    """回傳第一個 trace type 符合的 plotly trace（``fig.data[0]``），不是整個
    ``go.Figure``——``.x``／``.y``／``.marker``／``.colorscale`` 都掛在 trace
    上，不是 Figure 本身。"""
    for section in sections:
        for fig in section.figures:
            if fig.data and fig.data[0].type == type_name:
                return fig.data[0]
    raise AssertionError(
        f"沒有找到 type={type_name!r} 的圖，實際 section 標題："
        f"{[s.title for s in sections]}"
    )


# ---- 基本形狀（移植自 test_item_ability_render.py）------------------------


def test_render_returns_sections():
    sections = suppression.render(_result(), {})
    assert isinstance(sections, tuple)
    assert all(isinstance(s, ReportSection) for s in sections)


def test_render_returns_multiple_sections():
    """六個 section：per-item 彙總、壓制矩陣、壓制者視角、交叉購買、
    具體案例、完整性檢查。數字定義不再是獨立 section，而是分散進各區。"""
    sections = suppression.render(_result(), {})
    assert len(sections) >= 5
    assert all(s.title.strip() for s in sections)


def test_section_titles_are_distinct():
    titles = [s.title for s in suppression.render(_result(), {})]
    assert len(set(titles)) == len(titles)


def test_every_section_with_a_figure_has_its_own_explanation():
    for section in suppression.render(_result(), {}):
        if section.figures or section.tables:
            assert section.description.strip() or section.bullets, (
                f"section {section.title!r} 有圖表卻沒有自己的說明"
            )


def test_formulas_use_plain_unicode_not_latex():
    for section in suppression.render(_result(), {}):
        assert "\\frac" not in section.formula
        assert "$" not in section.formula


def test_no_section_description_is_a_wall_of_text():
    for section in suppression.render(_result(), {}):
        assert len(section.description) <= 120, (
            f"section {section.title!r} 的 description 有 "
            f"{len(section.description)} 字元，超過 120"
        )


def test_bullets_are_one_sentence_each():
    for section in suppression.render(_result(), {}):
        if section.title == _COMPLETENESS_TITLE:
            continue
        for bullet in section.bullets:
            assert len(bullet) <= 200, f"bullet 過長：{bullet[:40]}…"


def test_render_returns_empty_tuple_when_disabled():
    assert suppression.render({"enabled": False}, {}) == ()


def test_render_is_pure_and_does_not_mutate_input():
    result = _result()
    before = copy.deepcopy(result)
    suppression.render(result, {})
    assert result == before


def test_scope_declares_what_it_cannot_tell():
    assert isinstance(suppression.SCOPE, ScopeNote)
    assert suppression.SCOPE.blind_to


def test_module_level_scope_carries_no_run_specific_facts():
    assert suppression.SCOPE.sampling == ""


def test_no_verdict_vocabulary_in_output():
    """禁用字清單比照 item_ability 版，**扣掉 "severity"**：SCOPE.blind_to
    第一條被任務規格逐字釘死要用「依 severity 比例分攤」（指 compute 層
    ``raw_severity`` 這個具體變數，是技術指稱不是下結論），跟 item_ability
    禁用 "severity" 的理由（純防禦性、該模組內容從未談到這個詞）不是同一
    件事——這裡若照抄會跟任務逐字要求的文字互斥。"""
    forbidden = [
        "建議", "應該", "異常", "不足", "有問題", "健康", "通過", "失敗",
        "偏低", "偏高", "良好",
        "verdict", "recommend",
    ]
    scope = suppression.SCOPE
    text = _all_text(suppression.render(_result(), {})) + "\n".join(
        [scope.measures, scope.population,
         *scope.blind_to, *scope.reference_points]
    )
    hits = [word for word in forbidden if word.lower() in text.lower()]
    assert hits == [], f"呈現層出現下結論的字眼：{hits}"


def test_tables_and_titles_stay_aligned():
    for section in suppression.render(_result(), {}):
        assert len(section.tables) == len(section.table_titles)


def test_module_satisfies_contract():
    contract.check_module(suppression)


# ---- SCOPE 三條誠實條款（逐字要求，見任務指示）-----------------------------


def test_scope_states_allocation_is_accounting_not_causal():
    joined = " ".join(suppression.SCOPE.blind_to)
    assert "會計慣例" in joined
    assert "不是因果" in joined


def test_scope_warns_cross_purchase_sample_is_stratified():
    joined = " ".join(suppression.SCOPE.blind_to)
    assert "分層" in joined
    assert "無偏估計" in joined


def test_scope_explains_lift_equal_one():
    joined = " ".join(suppression.SCOPE.blind_to)
    assert "lift = 1" in joined or "lift=1" in joined
    assert "近似獨立" in joined


# ---- 使用者反饋（2026-07-21）：定義拆進各區、per-item 全貌先於矩陣 --------


def test_definitions_are_distributed_into_each_section_not_a_top_glossary():
    """使用者要求：數字定義拆進各個圖表的段落裡，不放頁首一張總表。

    驗兩件事：(a) 沒有一個獨立的頁首「定義」section；(b) 每個用到數字的區
    自己第一張表就是「本區數字定義」。
    """
    sections = suppression.render(_result(), {})
    titles = [s.title for s in sections]
    assert not any("數字定義" == t for t in titles), (
        f"還存在獨立的頁首定義 section：{titles}"
    )
    # 用到數字的四個區，各自第一張表是定義表
    for key in (_SUMMARY_TITLE, _BY_SUPPRESSOR_TITLE, _BUBBLE_TITLE, _EXAMPLES_TITLE):
        section = _section(sections, key)
        assert section.tables, f"{key} 區沒有表格"
        assert section.table_titles[0] == _DEFS_TITLE, (
            f"{key} 區的第一張表不是「{_DEFS_TITLE}」，而是 "
            f"{section.table_titles[0]!r}"
        )


def test_each_number_the_reader_asked_about_is_defined_in_its_own_section():
    """使用者點名的字（allocated_ap_gap／n_j/n_k／n_units／lift）必須各自
    定義在**它出現的那一區**——不是別區、不是頁首。
    """
    sections = suppression.render(_result(), {})
    cross = _section_text(_section(sections, _BUBBLE_TITLE))
    for term in ("n_units", "n_j / n_k", "n_joint", "P(k|j)", "lift"):
        assert term in cross, f"交叉購買區沒有定義 {term!r}"
    # allocated_ap_gap 出現在多個區，至少矩陣區要定義它
    matrix = _section_text(_section(sections, _MATRIX_TITLE))
    assert "allocated_ap_gap" in matrix and "分帳" in matrix, (
        "壓制矩陣區沒有就地定義 allocated_ap_gap"
    )
    # 定義要有實質內容：每個定義小表的「定義」欄都夠長
    for section in sections:
        for title, tbl in zip(section.table_titles, section.tables):
            if title != _DEFS_TITLE:
                continue
            for _, row in tbl.iterrows():
                assert len(str(row["定義"])) >= 15, (
                    f"{section.title} 區 {row['數字']!r} 的定義太短"
                )


def test_per_item_summary_comes_before_the_matrix():
    """使用者的具體抱怨：一開始就跳到矩陣，讀者不知道 per-item 基本狀況。
    per-item 彙總表必須排在壓制矩陣之前。
    """
    titles = [s.title for s in suppression.render(_result(), {})]
    i_summary = next(i for i, t in enumerate(titles) if _SUMMARY_TITLE in t)
    i_matrix = next(i for i, t in enumerate(titles) if _MATRIX_TITLE in t)
    assert i_summary < i_matrix, f"per-item 彙總沒有排在矩陣之前：{titles}"


def test_per_item_summary_has_the_core_columns():
    """codex 版一開場給的那幾個數字，這裡必須都有——這是使用者點名要的。

    資料表是這一區的**最後**一張表（第一張是定義小表）。
    """
    section = _section(suppression.render(_result(), {}), _SUMMARY_TITLE)
    assert section.tables, "per-item 彙總沒有表格"
    cols = list(section.tables[-1].columns)
    for col in ("受害 item", "AP gap from suppressors", "unexplained AP gap",
                "overall gap share", "suppressed pos / n_pos", "mean neg above",
                "頭號壓制者"):
        assert col in cols, f"per-item 表缺欄位 {col!r}"


def test_matrix_formula_or_bullets_link_back_to_the_per_item_summary():
    """使用者原話：矩陣的數字要嘛在公式那邊講清楚定義、要嘛讓讀者連得回
    前面某個數字。這裡驗後者：矩陣區要提到它跟頭號壓制者/前一區的關係。
    """
    section = _section(suppression.render(_result(), {}), _MATRIX_TITLE)
    text = section.formula + "\n" + "\n".join(section.bullets)
    assert "頭號壓制者" in text or "第 2 區" in text or "allocated_ap_gap" in text, (
        "矩陣區沒有把數字連回前面的 per-item 彙總"
    )


# ---- 本項專屬：兩張圖同軸序、雙量編碼、點數預算截斷 -------------------------


def _matrix_data_table(sections):
    """壓制矩陣區的資料表（最後一張；第一張是定義小表）。"""
    return _section(sections, _MATRIX_TITLE).tables[-1]


def test_matrix_is_a_by_row_table_sharing_axis_with_the_bubble():
    """矩陣改成可讀數字表（不再是熱圖）：列＝受害 item、欄＝壓制者 item，
    方陣同軸序；泡泡圖的 j／k 軸落在同一組 item 內，兩者才對照得起來。
    """
    sections = suppression.render(_result(), {})
    matrix = _matrix_data_table(sections)
    sup_axis = list(matrix.columns[1:])          # 首欄是「受害 item ＼ 壓制者」
    victim_axis = list(matrix.iloc[:, 0])
    assert sup_axis == sorted(sup_axis), "壓制者欄沒有排序"
    assert victim_axis == sup_axis, "受害列與壓制者欄不是同一組同軸序"
    bubble = _figure_of_type(sections, "scatter")
    assert set(bubble.x) <= set(sup_axis) and set(bubble.y) <= set(sup_axis)


def test_matrix_rows_sum_to_one_hundred_percent():
    """target gap share 每列橫著加＝100%——這是『按列看』的數學前提，
    也是不該跨列比大小的理由。空白格＝0。"""
    matrix = _matrix_data_table(suppression.render(_result(), {}))
    for _, row in matrix.iterrows():
        pct = [float(c[:-1]) for c in row[1:] if isinstance(c, str) and c.endswith("%")]
        assert abs(sum(pct) - 100.0) < 0.5, f"某列的 target gap share 不加到 100%：{sum(pct)}"


def test_bubble_grid_encodes_two_different_quantities():
    """大小 ＝ 共買數、顏色 ＝ lift。兩者編同一個量的話這張圖只剩一個
    維度，而『樣本量小的格子顏色不可信』就看不出來了。
    """
    bubble = _figure_of_type(suppression.render(_result(), {}), "scatter")
    assert bubble.marker.size is not None
    assert bubble.marker.color is not None
    assert list(bubble.marker.size) != list(bubble.marker.color)


def test_bubble_axes_are_labelled_j_and_k():
    """使用者反饋：j／k 分別對應哪一軸要標清楚。"""
    sections = suppression.render(_result(), {})
    fig = next(f for s in sections for f in s.figures
               if f.data and f.data[0].type == "scatter")
    xt = fig.layout.xaxis.title.text or ""
    yt = fig.layout.yaxis.title.text or ""
    assert "j" in xt and "買了這個" in xt, f"橫軸沒標清楚 j：{xt!r}"
    assert "k" in yt and "也買了這個" in yt, f"縱軸沒標清楚 k：{yt!r}"


def test_distribution_bars_cover_both_victim_and_suppressor_views():
    """使用者反饋：除了壓制者視角，也要有受害者視角的分布圖。
    兩張長條圖：一張按受害 item、一張按壓制者 item。"""
    sections = suppression.render(_result(), {})
    bar_titles = [
        f.layout.title.text for s in sections for f in s.figures
        if f.data and f.data[0].type == "bar"
    ]
    joined = " ".join(t for t in bar_titles if t)
    assert "誰承受" in joined and "誰造成" in joined, (
        f"缺口分布長條圖不齊全（受害者／壓制者）：{bar_titles}"
    )


def test_suppressor_table_drops_mean_logit_margin():
    """使用者反饋：mean logit margin 在壓制者明細表裡多餘，刪掉。"""
    section = _section(suppression.render(_result(), {}), _BY_SUPPRESSOR_TITLE)
    for tbl in section.tables:
        assert "mean logit margin" not in tbl.columns, "壓制者明細還留著 mean logit margin"


def test_cross_purchase_has_no_data_table_only_figure_and_defs():
    """使用者反饋：交叉購買有圖就清楚，不必再展開 (j,k) 資料表。
    只留定義小表與圖，不留逐列資料表。"""
    section = _section(suppression.render(_result(), {}), _BUBBLE_TITLE)
    assert section.figures, "交叉購買沒有圖"
    assert len(section.tables) == 1, "交叉購買除了定義表不該還有資料表"
    assert section.table_titles == [_DEFS_TITLE]


def test_axis_is_capped_and_says_so_when_items_exceed_the_budget():
    """item 太多時矩陣表截斷**並在 bullets 說明**。

    只斷言『沒有炸掉』會同時被『正確截斷』與『根本沒畫』滿足（已知假綠
    形態：「不存在」斷言雙關）——所以要斷言系統**說了什麼**。
    """
    sections = suppression.render(_result_with_items(60), {})
    matrix = _matrix_data_table(sections)
    assert len(matrix.columns) - 1 == 44, "矩陣欄數沒有截到 44"
    text = " ".join(b for s in sections for b in s.bullets)
    assert "60" in text and "44" in text


def test_axis_is_not_capped_when_items_fit():
    """反向釘住上一條：22 個 item（公司實際規模）整張畫出來，不截。"""
    matrix = _matrix_data_table(suppression.render(_result_with_items(22), {}))
    assert len(matrix.columns) - 1 == 22


def test_examples_section_lists_query_and_items():
    sections = suppression.render(_result(), {})
    section = _section(sections, _EXAMPLES_TITLE)
    assert section.tables, "具體案例 section 沒有表格"
    table = section.tables[-1]  # 最後一張是資料表；第一張是定義小表
    for col in ("query", "positive_item", "suppressor_item"):
        assert col in table.columns


def test_by_suppressor_section_has_a_figure_or_table():
    sections = suppression.render(_result(), {})
    section = _section(sections, _BY_SUPPRESSOR_TITLE)
    assert section.figures or section.tables


def test_completeness_section_reports_scale():
    sections = suppression.render(_result(), {})
    section = _section(sections, _COMPLETENESS_TITLE)
    text = "\n".join(section.bullets)
    assert "query" in text or "n_queries" in text.lower() or "個" in text


def test_render_survives_a_sample_with_no_suppression_pairs():
    """良性退化：抽樣裡沒有任何壓制關係（axis_order 為空）。不該炸，
    圖表 section 可以缺席，但完整性檢查一定要在。
    """
    result = _result()
    result.update({
        "pair_ledger": [], "by_suppressor": [], "examples": [],
        "target_summary": [], "top_suppressors_by_target": [],
        "matrices": {
            "target_gap_share": {}, "affected_positive_rate": {},
            "mean_logit_margin": {}, "suppressor_target_gap_share": {},
        },
        "axis_order": [], "cross_purchase": [], "n_units": 0,
        "total_ap_gap_allocated_to_suppressors": 0.0,
        "total_row_ap_gap_allocated": 0.0,
        "n_suppressed_positive_rows": 0, "suppressed_positive_rate": None,
        "mean_negatives_above_positive": None, "n_misordered_pairs": 0,
        "notes": ["診斷抽樣為空——壓制帳本均未計算。"],
    })
    sections = suppression.render(result, {})
    assert all(isinstance(s, ReportSection) for s in sections)
    assert _section(sections, _COMPLETENESS_TITLE) is not None


def test_empty_sample_produces_no_hollow_sections():
    result = _result()
    result.update({
        "pair_ledger": [], "by_suppressor": [], "examples": [],
        "target_summary": [], "top_suppressors_by_target": [],
        "matrices": {
            "target_gap_share": {}, "affected_positive_rate": {},
            "mean_logit_margin": {}, "suppressor_target_gap_share": {},
        },
        "axis_order": [], "cross_purchase": [], "n_units": 0,
        "total_ap_gap_allocated_to_suppressors": 0.0,
        "total_row_ap_gap_allocated": 0.0,
        "n_suppressed_positive_rows": 0, "suppressed_positive_rate": None,
        "mean_negatives_above_positive": None, "n_misordered_pairs": 0,
        "notes": ["診斷抽樣為空——壓制帳本均未計算。"],
    })
    for section in suppression.render(result, {}):
        assert section.figures or section.tables or section.bullets, (
            f"section {section.title!r} 是空殼"
        )


def test_cross_purchase_description_only_describes_not_concludes():
    """左圖右圖各是什麼要說清楚，但不得接『若 X 則代表 Y』的推論（鐵則 1）。
    這裡只驗最容易犯規的具體字眼：因果／代表著／意味。
    """
    sections = suppression.render(_result(), {})
    section = _section(sections, _BUBBLE_TITLE)
    banned = ["因此", "代表著", "意味", "說明模型"]
    hits = [w for w in banned if w in section.description]
    assert hits == [], f"description 疑似下了推論：{hits}"
