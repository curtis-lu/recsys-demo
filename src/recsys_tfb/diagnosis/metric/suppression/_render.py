"""suppression 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``item_ability/_render.py``（見該檔 docstring 的完整理由）：純函式、
不 import pyspark、不讀檔、不做任何計算——出現在報表上的每個數字都必須已經
在 JSON 裡。停用時回空 tuple；最後一節固定是「本次執行的完整性檢查」。

**版面原則（2026-07-21 依使用者反饋重排）**：使用者要的是「清楚的邏輯架構
＋ 每個數字都有定義 ＋ 數字之間連得起來」，不是更多圖。所以改成**抽屜式
下鑽**，先給定義、再給 per-item 全貌、再逐層拆細：

1. **數字定義表**——所有比率的分子／分母、所有量值的算法。放最前面當參照，
   下面每一區的數字都指得回這裡（使用者原話：「讓讀者可以直覺連結到前面
   部分的某數字」）。
2. **per-item 彙總表**——每個受害 item 一列：AP、AP gap、被壓制造成多少、
   還剩多少沒解釋、佔全部缺口多少、被壓率、上方負例平均數、名次、頭號壓制者。
   這是主表，先用它看哪個 item 被壓最兇、該先查誰（對齊 codex 版的開場）。
3. **壓制矩陣熱圖**——把第 2 區每個受害 item 的「被壓制造成的缺口」拆開，
   看它是被哪些壓制者分走的。格 (row, col) ＝ 壓制者 col 分走 row 的佔比；
   對得回第 2 區 row 那列的 top_suppressor。單向大小 ∈[0,1]，不給 center。
4. **壓制者視角表**——反過來：每個壓制者 item 分走多少、影響誰。
5. **交叉購買 lift**——獨立一區、清楚定義：這些 item 在真實資料上本來就多常
   一起買（與模型排序無關的對照組）。表格為主，附一張同軸序泡泡圖供一眼對照。
6. **具體案例表**——gap 最大的 (正例, 壓制者) 逐列，供核對。
7. **完整性檢查**（固定殿後）。

⚠ **圖形點數預算**：矩陣／泡泡圖都是 ``|axis_order|²``，item 一多就會撞
``figures.MAX_FIGURE_POINTS``（2000）。表格不受此限（第 2/4/5 區的表都是逐列、
線性），只有兩張圖要截：只畫分攤缺口最大的前 ``N = floor(sqrt(2000))`` 個
item（見 :func:`_ranked_axis`），完整資料仍在表格裡，不損失資訊，只損失
「一眼掃過去」的範圍。``N`` 由繪圖引擎硬上限反推，不是可調門檻，不進 config。
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import MAX_FIGURE_POINTS, bubble_grid, heatmap
from recsys_tfb.report.fmt import (
    fmt_ap, fmt_auc, fmt_count, fmt_logodds, fmt_percent, fmt_ratio,
)

#: 單張圖（矩陣／泡泡圖，皆為 |axis|² 個點）能承受的最大軸長。由繪圖引擎的
#: 硬上限反推，不是可調的判斷門檻——見模組 docstring「圖形點數預算」。
_N_AXIS_MAX = math.isqrt(MAX_FIGURE_POINTS)


# ─────────────────────────────────────────────────────────────────────────
# 1. 數字定義表
# ─────────────────────────────────────────────────────────────────────────

#: (數字, 定義, 出現在哪一區)。分子／分母型的比率寫成「A ÷ B」；量值寫算法。
#: 這張表是本頁所有數字的單一參照——下面各區的 formula 只寫最精簡的算式，
#: 詳細語意一律回指這裡，避免同一個定義在頁面上出現好幾份、各自漂移。
_GLOSSARY: tuple[tuple[str, str, str], ...] = (
    ("AP",
     "該 item 在它自己的正例列上的 average precision（0–1）。越高＝該 item "
     "的正例越常被排在前面。",
     "per-item 彙總"),
    ("AP gap",
     "1 − AP。離「正例全部排最前」還差多少。",
     "per-item 彙總"),
    ("allocated_ap_gap（分攤 AP 缺口）",
     "把每個被壓制的正例列的 AP 缺口，依「排在它上方的各個負例各自造成多少"
     "名次損失」的比例分給那些負例，跨列加總到 (受害 item, 壓制者 item) 組合"
     "上。這是分帳慣例、不是因果——拿掉某壓制者不代表會賺回這麼多。",
     "壓制矩陣 / 壓制者視角 / 案例"),
    ("AP gap from suppressors",
     "該 item 分到的 allocated_ap_gap 總量 ÷ 該 item 的正例列數（n_pos）。"
     "與 AP gap 同尺度（都是每正例列），所以兩者可以相減。",
     "per-item 彙總"),
    ("unexplained AP gap",
     "max(0, AP gap − AP gap from suppressors)。AP 缺口裡**不是**被同 query "
     "負例壓制造成的部分（例：正例排在別的正例後面、或 k 截斷）。",
     "per-item 彙總"),
    ("overall gap share",
     "該 item（或該壓制者）分到的 allocated_ap_gap ÷ 全部 item 的 "
     "allocated_ap_gap 總和。單一 item 佔全體被壓制損失的比重。",
     "per-item 彙總 / 壓制者視角"),
    ("suppressed pos / n_pos",
     "該 item 至少被一個負例壓過的正例列數 ÷ 該 item 全部正例列數。被壓制"
     "有多普遍（跟嚴重程度是兩回事）。",
     "per-item 彙總"),
    ("mean neg above",
     "該 item 的每個正例列上方，平均有幾個負例。",
     "per-item 彙總"),
    ("target gap share（矩陣格）",
     "壓制者 col 分到的 allocated_ap_gap ÷ 受害 item row 的 allocated_ap_gap "
     "總和。同一個 row 橫著加起來＝1。回答「row 的缺口主要是被誰分走的」。",
     "壓制矩陣"),
    ("mean logit margin",
     "受影響列上，logit(壓制者分數) − logit(正例分數) 的平均。正值＝壓制者的"
     "分數確實比正例高。這是分數強度的線索，不是名次優先序。",
     "案例 / 壓制者視角"),
    ("n_units",
     "樣本內相異 query 單位數。一個 query 單位＝一組 (time, entity)。",
     "交叉購買"),
    ("n_j / n_k",
     "item j／k 為正例（label=1）的 query 單位數。",
     "交叉購買"),
    ("n_joint",
     "同一個 query 單位上 j 與 k **都**是正例的單位數。",
     "交叉購買"),
    ("P(k|j)",
     "n_joint ÷ n_j。買了 j 的單位裡，有多少也買了 k。",
     "交叉購買"),
    ("lift",
     "P(k|j) ÷ (n_k ÷ n_units)。買 j 的人買 k 的機率，相對於 k 的整體基礎率"
     "的倍數。lift=1 ≈ 在這份樣本上兩者近似獨立。",
     "交叉購買"),
)


def _glossary_section() -> ReportSection:
    """1. 數字定義表——本頁所有數字的單一參照，放最前面。"""
    table = pd.DataFrame(
        list(_GLOSSARY), columns=["數字", "定義", "出現在"],
    )
    return ReportSection(
        title="數字定義",
        description=(
            "本頁每個數字的定義。比率寫成「分子 ÷ 分母」，量值寫算法；"
            "下面各區看到不懂的數字都回這裡查。"
        ),
        bullets=[
            "分數一律是 logit(score_uncalibrated)——offset 活在模型輸出的 "
            "log-odds 空間，校準層是後貼的。",
            "「壓制」只在負例（label=0）排在同一個 query 的正例（label=1）之上"
            "時才計入。",
        ],
        tables=[table],
        table_titles=["數字 → 定義"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 2. per-item 彙總表
# ─────────────────────────────────────────────────────────────────────────

_TARGET_COLUMNS = [
    "受害 item", "AP", "AP gap", "AP gap from suppressors",
    "unexplained AP gap", "overall gap share", "n_pos",
    "suppressed pos / n_pos", "mean neg above", "median pos rank",
    "頭號壓制者",
]


def _target_summary_section(result: dict) -> ReportSection | None:
    """2. per-item 彙總表——每個受害 item 一列，主表，先看哪個 item 該先查。"""
    rows = result.get("target_summary") or []
    if not rows:
        return None

    table = pd.DataFrame(
        [
            {
                "受害 item": r.get("positive_item"),
                "AP": fmt_ap(r.get("ap")),
                "AP gap": fmt_ap(r.get("ap_gap")),
                "AP gap from suppressors": fmt_ap(r.get("ap_gap_from_suppressors")),
                "unexplained AP gap": fmt_ap(r.get("unexplained_ap_gap")),
                "overall gap share": fmt_percent(r.get("overall_ap_gap_share")),
                "n_pos": fmt_count(r.get("n_pos")),
                "suppressed pos / n_pos": fmt_percent(r.get("suppressed_positive_rate")),
                "mean neg above": fmt_ratio(r.get("mean_negatives_above_positive")),
                "median pos rank": r.get("median_positive_rank_display"),
                "頭號壓制者": r.get("top_suppressor"),
            }
            for r in rows
        ],
        columns=_TARGET_COLUMNS,
    )
    return ReportSection(
        title="per-item 壓制彙總",
        description=(
            "每個受害 item 一列：被壓得多兇、多普遍、佔全體缺口多少、頭號"
            "壓制者是誰。先用這張表決定要細看哪個 item。"
        ),
        formula="AP gap = 1 − AP；unexplained AP gap = AP gap − AP gap from suppressors",
        bullets=[
            "「AP gap from suppressors」是 AP gap 裡被同 query 負例壓制的部分，"
            "「unexplained AP gap」是剩下的部分（定義見第 1 區）。",
            "「頭號壓制者」就是第 3 區矩陣裡這一列 target gap share 最大的那一欄，"
            "兩處可以互相印證。",
            "依 overall gap share 由大到小排（compute 已排序，這裡不重排）。",
        ],
        tables=[table],
        table_titles=["per-item 壓制彙總（依 overall gap share 降冪）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 3. 壓制矩陣熱圖
# ─────────────────────────────────────────────────────────────────────────

def _ranked_axis(result: dict) -> tuple[list[str], int, int]:
    """回傳 ``(顯示用軸序, 顯示個數, 全部個數)``。

    ``axis_order`` 本身已排序（見 ``_compute.py``）。超過 :data:`_N_AXIS_MAX`
    時，依該 item 在 ``target_summary`` 的 ``overall_ap_gap_share`` 由大到小
    只取前 ``_N_AXIS_MAX`` 個，取完再排序回字母序——截斷改變的是「畫哪些」，
    不改變「畫出來的軸怎麼排」，這樣矩陣與泡泡圖才能繼續同軸對照。
    """
    axis_order = [str(a) for a in (result.get("axis_order") or [])]
    n_all = len(axis_order)
    if n_all <= _N_AXIS_MAX:
        return axis_order, n_all, n_all

    share_by_item = {
        str(t.get("positive_item")): float(t.get("overall_ap_gap_share") or 0.0)
        for t in (result.get("target_summary") or [])
    }
    ranked = sorted(axis_order, key=lambda it: -share_by_item.get(it, 0.0))
    shown = sorted(ranked[:_N_AXIS_MAX])
    return shown, len(shown), n_all


def _matrix_section(result: dict) -> ReportSection | None:
    """3. 壓制矩陣熱圖——把第 2 區每列的缺口拆給各壓制者。"""
    shown, n_shown, n_all = _ranked_axis(result)
    if not shown:
        return None

    matrix = (result.get("matrices") or {}).get("target_gap_share") or {}
    z = [
        [float((matrix.get(victim) or {}).get(sup, 0.0)) for sup in shown]
        for victim in shown
    ]
    fig = heatmap(
        z=z, x=shown, y=shown,
        title="壓制矩陣：target_gap_share（列＝受害 item，欄＝壓制者 item）",
        colorbar_title="target gap share",
    )

    bullets = [
        "格 (row, col) ＝ 壓制者 col 分走了 row 這個受害 item 多少比例的缺口"
        "（同一列橫著加＝1）。",
        "這是把第 2 區「AP gap from suppressors」那一欄，按壓制者拆開——每列"
        "顏色最深的那一欄，就是第 2 區的「頭號壓制者」。",
        "顏色只編碼佔比大小（單向量 ∈[0,1]），不編碼好壞。",
    ]
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限"
            f"（矩陣是 item 數的平方），此圖只畫分攤缺口最大的 {n_shown} 個；"
            "完整成對資料見第 4 區的壓制者視角表與 JSON 產物。"
        )

    return ReportSection(
        title="壓制矩陣熱圖",
        description="列＝受害 item，欄＝壓制者 item；顏色深淺＝該壓制者分走的缺口佔比。",
        formula="target gap share(row, col) = allocated_ap_gap(row, col) ÷ row 的 allocated_ap_gap 總和",
        bullets=bullets,
        figures=[fig],
    )


# ─────────────────────────────────────────────────────────────────────────
# 4. 壓制者視角表
# ─────────────────────────────────────────────────────────────────────────

_BY_SUPPRESSOR_COLUMNS = [
    "壓制者 item", "overall gap share", "影響幾個受害 item",
    "影響幾列正例", "mean logit margin", "主要受害 item",
]


def _by_suppressor_section(result: dict) -> ReportSection | None:
    """4. 壓制者視角表——反過來看：每個壓制者分走多少、影響誰。"""
    rows = [
        r for r in (result.get("by_suppressor") or [])
        if r.get("overall_ap_gap_share") is not None
    ]
    if not rows:
        return None

    table = pd.DataFrame(
        [
            {
                "壓制者 item": r.get("suppressor_item"),
                "overall gap share": fmt_percent(r.get("overall_ap_gap_share")),
                "影響幾個受害 item": fmt_count(r.get("affected_positive_items")),
                "影響幾列正例": fmt_count(r.get("affected_positive_rows")),
                "mean logit margin": fmt_logodds(r.get("mean_score_margin")),
                "主要受害 item": r.get("top_positive_items"),
            }
            for r in rows
        ],
        columns=_BY_SUPPRESSOR_COLUMNS,
    )
    return ReportSection(
        title="壓制者視角",
        description="每個壓制者 item 一列：它總共分走多少缺口、壓了幾個 item、主要壓誰。",
        formula="overall gap share = 該壓制者的 allocated_ap_gap ÷ 全部 allocated_ap_gap 總和",
        bullets=[
            "這是第 3 區矩陣「直著看」的版本：矩陣一欄加起來就是這裡的一列。",
            "overall gap share 是全域排序用；「主要受害 item」只列前幾名供快速掃描，"
            "完整分布看矩陣。",
            "依 overall gap share 由大到小排。",
        ],
        tables=[table],
        table_titles=["壓制者視角（依 overall gap share 降冪）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 5. 交叉購買 lift
# ─────────────────────────────────────────────────────────────────────────

_CROSS_COLUMNS = [
    "item_j", "item_k", "n_joint", "n_j", "n_k", "P(k|j)", "lift",
]


def _cross_purchase_section(result: dict) -> ReportSection | None:
    """5. 交叉購買 lift——獨立一區。表格為主，泡泡圖當一眼對照的 companion。"""
    rows = result.get("cross_purchase") or []
    if not rows:
        return None

    # 表格：全部 (j, k) 列，依 lift 由大到小（None 沉底）。表格不受點數預算限制。
    def _lift_key(r: dict) -> float:
        v = r.get("lift")
        return float(v) if v is not None else float("-inf")

    sorted_rows = sorted(rows, key=_lift_key, reverse=True)
    table = pd.DataFrame(
        [
            {
                "item_j": r.get("item_j"),
                "item_k": r.get("item_k"),
                "n_joint": fmt_count(r.get("n_joint")),
                "n_j": fmt_count(r.get("n_j")),
                "n_k": fmt_count(r.get("n_k")),
                "P(k|j)": fmt_percent(r.get("p_k_given_j")),
                "lift": fmt_ratio(r.get("lift")),
            }
            for r in sorted_rows
        ],
        columns=_CROSS_COLUMNS,
    )

    figures: list[Any] = []
    bullets = [
        "這一區與模型排序無關——算的是同一批 query 單位上、實際 label=1 的"
        "共現。用它跟第 3 區對照：一對 item 在這裡 lift 高（本來就常一起買）、"
        "在壓制矩陣裡又互壓，是兩種不同的情況，判斷留給你。",
        "lift 而非裸 P(k|j)：熱門 item 對任何 j 的 P(k|j) 都高，只看條件機率會"
        "退化成「熱門那幾列整片高」。lift 把 k 的基礎率除掉了（定義見第 1 區）。",
        "依 lift 由大到小排。",
    ]

    # companion 泡泡圖：只在 item 數不超過點數預算時附上，供一眼掃形狀。
    shown, n_shown, n_all = _ranked_axis(result)
    shown_set = set(shown)
    grid_rows = [
        r for r in rows
        if r.get("item_j") in shown_set and r.get("item_k") in shown_set
    ]
    if grid_rows:
        x = [str(r["item_j"]) for r in grid_rows]
        y = [str(r["item_k"]) for r in grid_rows]
        size = [float(r.get("n_joint") or 0.0) for r in grid_rows]
        colour = [
            float(r["lift"]) if r.get("lift") is not None else float("nan")
            for r in grid_rows
        ]
        hover = [
            f"{r['item_j']} × {r['item_k']}"
            f"<br>n_joint={fmt_count(r.get('n_joint'))}"
            f"<br>n_j={fmt_count(r.get('n_j'))}　n_k={fmt_count(r.get('n_k'))}"
            f"<br>P(k|j)={fmt_percent(r.get('p_k_given_j'))}"
            f"<br>lift={fmt_ratio(r.get('lift'))}"
            for r in grid_rows
        ]
        figures.append(bubble_grid(
            x=x, y=y, size=size, colour=colour, hover_text=hover,
            title="交叉購買 lift（泡泡：大小＝n_joint、顏色＝lift；與壓制矩陣同軸序）",
            colorbar_title="lift",
        ))
        bullets.append(
            "下方泡泡圖與第 3 區壓制矩陣同一組軸序，供一眼並排對照；大小＝"
            "n_joint、顏色＝lift 是兩個不同的量。"
        )
        if n_all > n_shown:
            bullets.append(
                f"泡泡圖只畫分攤缺口最大的 {n_shown} 個 item（點數上限），"
                "上方表格是完整的。"
            )

    return ReportSection(
        title="交叉購買 lift",
        description="這些 item 在真實資料上本來就多常一起買——與模型排序無關的對照組。",
        formula="P(k|j) = n_joint ÷ n_j；lift = P(k|j) ÷ (n_k ÷ n_units)",
        bullets=bullets,
        figures=figures,
        tables=[table],
        table_titles=["交叉購買 lift（依 lift 降冪）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 6. 具體案例表
# ─────────────────────────────────────────────────────────────────────────

_EXAMPLE_COLUMNS = [
    "query", "positive_item", "suppressor_item",
    "positive_rank", "suppressor_rank",
    "positive_score", "suppressor_score", "score_margin",
    "allocated_ap_gap",
]


def _examples_section(result: dict) -> ReportSection | None:
    """6. 具體案例表——gap 最大的 (正例, 壓制者) 組合，逐案核對用。"""
    examples = result.get("examples") or []
    if not examples:
        return None

    table = pd.DataFrame(
        [
            {
                "query": e.get("query"),
                "positive_item": e.get("positive_item"),
                "suppressor_item": e.get("suppressor_item"),
                "positive_rank": fmt_count(e.get("positive_rank")),
                "suppressor_rank": fmt_count(e.get("suppressor_rank")),
                "positive_score": fmt_logodds(e.get("positive_score")),
                "suppressor_score": fmt_logodds(e.get("suppressor_score")),
                "score_margin": fmt_logodds(e.get("score_margin")),
                "allocated_ap_gap": fmt_ap(e.get("allocated_ap_gap")),
            }
            for e in examples
        ],
        columns=_EXAMPLE_COLUMNS,
    )
    bullets = [
        "依 allocated_ap_gap 由大到小排序（compute 已排序，這裡不重排）。",
        "score_margin ＝ 壓制者 logit − 正例 logit；正值代表壓制者分數確實較高。",
        "逐列核對用，不做聚合證據——聚合看第 2–4 區。",
    ]

    return ReportSection(
        title="具體案例：被壓制的正例列",
        description="gap 最大的具體 (正例, 壓制者) 案例，逐列列出供核對。",
        formula="score_margin = logit(壓制者分數) − logit(正例分數)",
        bullets=bullets,
        tables=[table],
        # 表格標題不重複 section 標題的「具體案例」四個字——兩者在頁面上
        # 是連著的兩行，重複會讀成結巴。這裡只補 section 標題沒說的資訊。
        table_titles=["依分攤到的 AP 缺口降冪排序"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 7. 完整性檢查
# ─────────────────────────────────────────────────────────────────────────

def _completeness_section(result: dict) -> ReportSection:
    """7. 本次執行的完整性檢查——固定殿後，空的時候也照樣印「無」。"""
    notes = result.get("notes") or []
    axis_order = result.get("axis_order") or []
    cross_purchase = result.get("cross_purchase") or []

    bullets = [
        "計算層 notes（含 logit 轉換的觀測）："
        + (f"{len(notes)} 則，列於下方" if notes else "無"),
        "axis_order 涵蓋的 item 數（出現在壓制成對表裡，才會進矩陣與泡泡圖）："
        + fmt_count(len(axis_order)),
        "cross_purchase 列數（限制在 axis_order 內、n_units="
        + fmt_count(result.get("n_units")) + "）："
        + fmt_count(len(cross_purchase)),
    ]
    bullets.extend(str(n) for n in notes)
    bullets.append(
        "本次診斷抽樣的規模："
        f"{fmt_count(result.get('n_queries'))} 個 query、"
        f"{fmt_count(result.get('n_entities'))} 個 entity、"
        f"{fmt_count(result.get('n_items'))} 個 item、"
        f"{fmt_count(result.get('n_positive_rows'))} 列正例。"
    )

    return ReportSection(
        title="本次執行的完整性檢查",
        description="以下情況會讓上面的數字看起來正常、實際上沒量到或不完整。",
        bullets=bullets,
    )


def render(result: dict, parameters: dict) -> tuple[ReportSection, ...]:
    """把 ``compute`` 的輸出轉成一串報表章節；停用時回空 tuple。

    順序即閱讀順序（抽屜式下鑽，見模組 docstring）：定義 → per-item 全貌 →
    壓制矩陣（拆細）→ 壓制者視角（反看）→ 交叉購買（對照組）→ 具體案例 →
    完整性檢查。定義表永遠在最前、完整性檢查永遠在最後。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _glossary_section(),
        _target_summary_section(result),
        _matrix_section(result),
        _by_suppressor_section(result),
        _cross_purchase_section(result),
        _examples_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
