"""suppression 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``item_ability/_render.py``（見該檔 docstring）：純函式、不 import
pyspark、不讀檔、不做計算——出現在報表上的每個數字都必須已經在 JSON 裡。
停用時回空 tuple；最後一節固定是「本次執行的完整性檢查」。

**版面（2026-07-21 第二輪，依使用者反饋重排）**：抽屜式下鑽，兩個對稱視角
（受害者／壓制者）各自「分布圖＋明細表」，中間夾一張誰壓誰的矩陣：

1. **per-item 壓制彙總**（受害者明細表）——每個受害 item 一列。主表。
2. **壓制矩陣：誰壓誰**——**可讀的百分比數字表，按列看**（不用熱圖：
   target gap share 每列自成一個分布、橫著加＝1，熱圖的全域色階會誘導跨列
   比色而誤讀）。
3. **壓制者明細**（壓制者視角表）＋兩個視角的**缺口分布長條圖**。
4. **交叉購買 lift**——只用圖（軸標清楚 j／k），與模型排序無關的對照組。
5. **具體案例表**。
6. **完整性檢查**（固定殿後，印出 metric k 與 allocated 總量）。

**每個數字的定義貼在它出現的那一區**（每區第一張表就是「本區數字定義」），
不放頁首總表要人滾回去查。定義文字集中在 :data:`_DEFS`（單一真實來源），
各區用 :func:`_defs_table` 只取自己用到的鍵——同一數字出現在多區就各帶一次，
是刻意的在地化。

⚠ **圖形點數預算**：長條圖是 1D（item 數，遠低於上限）；矩陣改成表格後也不受
``figures.MAX_FIGURE_POINTS`` 限制。唯一的 |item|² 圖是交叉購買泡泡圖，item 太多
時截到前 ``N = floor(sqrt(2000))`` 個（見 :func:`_ranked_axis`）。
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import MAX_FIGURE_POINTS, bar, bubble_grid
from recsys_tfb.report.fmt import (
    fmt_ap, fmt_count, fmt_logodds, fmt_mean, fmt_percent, fmt_ratio,
)

#: 交叉購買泡泡圖是 |axis|² 個點，能承受的最大軸長。由繪圖引擎硬上限反推。
_N_AXIS_MAX = math.isqrt(MAX_FIGURE_POINTS)


# ─────────────────────────────────────────────────────────────────────────
# 數字定義：單一真實來源（程式裡只有一份），顯示時分散到各區
# ─────────────────────────────────────────────────────────────────────────

#: 數字 → 一句定義。比率寫成「A ÷ B」，量值寫算法。集中維護、各區各自取用
#: （見 :func:`_defs_table`）：定義貼在數字旁邊，而非頁首一張總表。
_DEFS: dict[str, str] = {
    "allocated_ap_gap":
        "**基礎量（逐列）**：某一個正例列的 AP 缺口，按上方各負例的 severity 比例，"
        "分給每個負例的那一份。公式：分給負例 s 的份 = row_ap_gap × severity(s) ÷ "
        "Σ severity；其中 row_ap_gap = 1 − 該正例列目前的 AP 貢獻，severity(s) = "
        "把正例提到負例 s 的名次、它的 AP 貢獻會多出多少（負例排得越高、提上去越賺 "
        "→ severity 越大）。例：正例在 rank 3（目前貢獻 1/3），上方兩個負例在 rank 1、2；"
        "提到 rank 1 貢獻變 1（多 2/3）、提到 rank 2 變 1/2（多 1/6），severity 比 2/3:1/6"
        "＝4:1，於是 row_ap_gap 0.667 拆成 0.533 與 0.133。案例表顯示的就是這個未加總的"
        "單列值；壓制矩陣與 gap share 則是把它**跨列加總**。分帳、非因果。",
    "AP": "該 item 在它自己的正例列上的 average precision（0–1）；越高＝正例越常被排在前面。",
    "AP gap": "1 − AP。離「正例全部排最前」還差多少（每列平均）。",
    "AP gap from suppressors":
        "AP gap 裡「有負例壓在上面」的那部分：＝ 該 item 的 allocated_ap_gap 加總 ÷ 正例列數（n_pos）；"
        "與 unexplained AP gap 相加剛好 ＝ AP gap。",
    "unexplained AP gap":
        "AP gap − AP gap from suppressors：缺口裡沒有負例壓在上面的部分。**本次 k=all 時它結構上恆為 0**"
        "（一個正例只要沒排最前、上方必有負例）；只有把 k 設成有限值、正例落到 top-k 之外時才會 > 0。",
    "gap share（受害者）":
        "該 item **作為受害者**、攤在它頭上的 allocated_ap_gap 加總 ÷ 全體總量。"
        "＝（AP gap from suppressors × n_pos）÷ 全體總量（總量印在本區說明，可自行驗算）。",
    "gap share（壓制者）":
        "該 item **作為壓制者**、它造成的 allocated_ap_gap 加總 ÷ 全體總量。"
        "⚠ 與『受害者側』同樣是佔全體的比例、但加總的是不同一批列——同一個 item "
        "兩者可以差很多（很少被壓、卻常壓別人），不要當同一個數。",
    "suppressed pos / n_pos":
        "至少被一個負例壓過的正例列 ÷ 該 item 全部正例列（n_pos）；被壓制有多普遍（與 k 無關）。",
    "n_pos": "該 item 的正例列數（label=1 的列數）。",
    "median pos rank":
        "寫成「a of b」，讀作『名次中位數 a、每個 query 平均約 b 個候選 item』——"
        "a 是名次的中位數、b 是候選數的平均，**不是分數 a/b**。a 越接近 b 越常墊底。",
    "mean neg above": "該 item 的每個正例列上方，平均有幾個負例（與 k 無關；是個數不是倍率）。",
    "target gap share":
        "壓制者欄的 allocated_ap_gap 加總 ÷ 受害 item 列的 allocated_ap_gap 加總；同一列橫著加＝100%。",
    "score_margin":
        "壓制者分數 − 正例分數（兩者都已是 logit 分數，直接相減，不再套一層）；正值＝壓制者分數確實較高。",
    "n_units": "樣本內相異 query 單位數；一個 query 單位＝一組 (time, entity)。",
    "n_j / n_k": "item j／k 為正例（label=1）的 query 單位數。",
    "n_joint": "同一個 query 單位上 j 與 k 都是正例的單位數（對稱：n_joint(j,k)=n_joint(k,j)）。",
    "P(k|j)": "n_joint ÷ n_j：買了 j 的 query 單位裡，有多少也買了 k（**有方向**，P(k|j)≠P(j|k)）。",
    "lift":
        "P(k|j) ÷ (n_k ÷ n_units)＝n_joint × n_units ÷ (n_j × n_k)：相對 k 基礎率的倍數。"
        "**對稱**：lift(j,k)=lift(k,j)；lift=1 ≈ 在這份樣本上近似獨立。",
}


def _defs_table(*names: str) -> pd.DataFrame:
    """從 :data:`_DEFS` 挑出這一區用到的數字，組成「數字 → 定義」小表，
    放在各 section 的第一張表，緊貼它描述的圖或資料表。"""
    return pd.DataFrame(
        [{"數字": n, "定義": _DEFS[n]} for n in names],
        columns=["數字", "定義"],
    )


_DEFS_TITLE = "本區數字定義"


def _k_display(result: dict) -> str:
    """metric k 的人話版：None＝不設截斷。"""
    k = (result.get("metric_params") or {}).get("k")
    return "all（不設截斷，整個排序都納入）" if k is None else str(k)


# ─────────────────────────────────────────────────────────────────────────
# 1. per-item 壓制彙總（受害者明細表）
# ─────────────────────────────────────────────────────────────────────────

_TARGET_COLUMNS = [
    "受害 item", "AP", "AP gap", "AP gap from suppressors",
    "unexplained AP gap", "gap share（受害者）", "n_pos",
    "suppressed pos / n_pos", "mean neg above", "median pos rank",
    "頭號壓制者",
]


def _target_summary_section(result: dict) -> ReportSection | None:
    """1. 受害者明細表——每個受害 item 一列，先看哪個 item 該先查。"""
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
                "gap share（受害者）": fmt_percent(r.get("overall_ap_gap_share")),
                "n_pos": fmt_count(r.get("n_pos")),
                "suppressed pos / n_pos": fmt_percent(r.get("suppressed_positive_rate")),
                "mean neg above": fmt_mean(r.get("mean_negatives_above_positive")),
                "median pos rank": r.get("median_positive_rank_display"),
                "頭號壓制者": r.get("top_suppressor"),
            }
            for r in rows
        ],
        columns=_TARGET_COLUMNS,
    )

    total = result.get("total_ap_gap_allocated_to_suppressors")
    return ReportSection(
        title="per-item 壓制彙總",
        description=(
            "每個受害 item 一列：排序品質（AP、median pos rank）、被壓得多普遍、"
            "佔全體缺口多少、頭號壓制者是誰。"
        ),
        formula="AP gap = 1 − AP；AP gap from suppressors + unexplained AP gap = AP gap",
        bullets=[
            f"「壓制」＝同一個 query 內，負例（label=0）排在正例（label=1）之上。"
            f"本次 metric k = {_k_display(result)}。",
            "被壓制的計數（mean neg above、suppressed pos / n_pos）與 k 無關；"
            "AP 缺口與其分攤才在 top-k 內衡量。",
            "⚠ 這張表**預設依 gap share（受害者）降冪**，而它是**總損失**（筆數加權）："
            "正例列數多、但其實排得不錯的 item 會排在前面，正例列數少、但排得很差的 "
            "item 會沉在後面。要看**每個 query 的嚴重度**（與筆數無關）請改看 AP（越低"
            "越差）與 median pos rank（越接近 b of b 越常墊底）。哪個角度重要由你決定。",
            "gap share（受害者）可自行驗算：（AP gap from suppressors × n_pos）÷ "
            f"全體 allocated_ap_gap 總量 {fmt_mean(total)}。",
            "unexplained AP gap 這欄本次全為 0，是 k=all 的結構性結果（見定義），不是表壞了。",
            "「頭號壓制者」就是壓制矩陣裡這一列 target gap share 最大的那一欄。",
        ],
        tables=[
            _defs_table(
                "allocated_ap_gap", "AP", "AP gap", "AP gap from suppressors",
                "unexplained AP gap", "gap share（受害者）", "n_pos",
                "suppressed pos / n_pos", "median pos rank", "mean neg above",
            ),
            table,
        ],
        table_titles=[_DEFS_TITLE, "受害者明細（預設依 gap share 受害者側降冪，非嚴重度）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 2. 壓制矩陣：誰壓誰（可讀百分比數字表，按列看）
# ─────────────────────────────────────────────────────────────────────────

def _ranked_axis(result: dict) -> tuple[list[str], int, int]:
    """回傳 ``(顯示用軸序, 顯示個數, 全部個數)``。

    ``axis_order`` 已排序。超過 :data:`_N_AXIS_MAX` 時依 ``overall_ap_gap_share``
    取前 N 個、再排回字母序——截斷改的是「取哪些」，不改「怎麼排」。
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
    """2. 誰壓誰——按列看的百分比數字表（每列＝一個受害 item 的缺口怎麼被分掉）。"""
    shown, n_shown, n_all = _ranked_axis(result)
    if not shown:
        return None

    matrix = (result.get("matrices") or {}).get("target_gap_share") or {}
    data = []
    for victim in shown:
        row = {"受害 item ＼ 壓制者": victim}
        for sup in shown:
            v = (matrix.get(victim) or {}).get(sup)
            # 空白＝0，減少稀疏矩陣的視覺噪音（下面 bullet 說明）
            row[sup] = fmt_percent(v) if v else ""
        data.append(row)
    table = pd.DataFrame(data, columns=["受害 item ＼ 壓制者"] + list(shown))

    bullets = [
        "**按列讀**：每一列是一個受害 item，橫著看它的缺口被哪些壓制者（欄）"
        "分走，同一列橫著加＝100%。不要跨列比大小——每列各自是一個分布。",
        "空白格＝該壓制者完全沒分到這個受害 item 的缺口（恰為 0）；"
        "「0.0%」＝極小但非零（四捨五入到 0.0%）。",
        "每列最大的那一欄，就是受害者明細表裡這個 item 的「頭號壓制者」。",
    ]
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，此表只列 overall gap share 最大的 {n_shown} 個"
            "（列與欄同一組），完整成對資料見 JSON 產物的 pair_ledger。"
        )

    return ReportSection(
        title="壓制矩陣：誰壓誰",
        description="列＝受害 item，欄＝壓制者 item；格子＝該壓制者分走這列缺口的百分比。",
        formula="target gap share(列, 欄) = allocated_ap_gap(列, 欄) ÷ 該列的 allocated_ap_gap 總量",
        bullets=bullets,
        tables=[_defs_table("target gap share", "allocated_ap_gap"), table],
        table_titles=[_DEFS_TITLE, "壓制矩陣（每列橫著加＝100%）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 3. 壓制者明細 ＋ 兩個視角的缺口分布長條圖
# ─────────────────────────────────────────────────────────────────────────

_BY_SUPPRESSOR_COLUMNS = [
    "壓制者 item", "gap share（壓制者）", "影響幾個受害 item",
    "影響幾列正例", "主要受害 item",
]


def _distribution_figures(result: dict) -> list[Any]:
    """兩個對稱視角的缺口分布長條圖：誰承受（受害者）、誰造成（壓制者）。"""
    figs: list[Any] = []

    victims = [
        (str(t.get("positive_item")), t.get("overall_ap_gap_share"))
        for t in (result.get("target_summary") or [])
        if t.get("overall_ap_gap_share") is not None
    ]
    if victims:
        figs.append(bar(
            x=[v for v, _ in victims], y=[float(s) for _, s in victims],
            title="缺口分布：誰承受（item 作為受害者，攤在它頭上的 gap share）",
            x_title="受害 item", y_title="承受的 gap share",
        ))

    suppressors = [
        (str(s.get("suppressor_item")), s.get("overall_ap_gap_share"))
        for s in (result.get("by_suppressor") or [])
        if s.get("overall_ap_gap_share") is not None
    ]
    if suppressors:
        figs.append(bar(
            x=[v for v, _ in suppressors], y=[float(s) for _, s in suppressors],
            title="缺口分布：誰造成（item 作為壓制者，它造成的 gap share）",
            x_title="壓制者 item", y_title="造成的 gap share",
        ))
    return figs


def _by_suppressor_section(result: dict) -> ReportSection | None:
    """3. 壓制者明細＋兩個視角的分布圖。"""
    rows = [
        r for r in (result.get("by_suppressor") or [])
        if r.get("overall_ap_gap_share") is not None
    ]
    figs = _distribution_figures(result)
    if not rows and not figs:
        return None

    tables: list[pd.DataFrame] = [
        _defs_table("gap share（壓制者）", "gap share（受害者）", "allocated_ap_gap")
    ]
    table_titles: list[str] = [_DEFS_TITLE]
    if rows:
        tables.append(pd.DataFrame(
            [
                {
                    "壓制者 item": r.get("suppressor_item"),
                    "gap share（壓制者）": fmt_percent(r.get("overall_ap_gap_share")),
                    "影響幾個受害 item": fmt_count(r.get("affected_positive_items")),
                    "影響幾列正例": fmt_count(r.get("affected_positive_rows")),
                    "主要受害 item": r.get("top_positive_items"),
                }
                for r in rows
            ],
            columns=_BY_SUPPRESSOR_COLUMNS,
        ))
        table_titles.append("壓制者明細（依 overall gap share 降冪）")

    return ReportSection(
        title="壓制者明細與缺口分布",
        description="兩張圖：同一筆全體缺口，一張看攤在誰頭上（受害者）、一張看誰造成（壓制者）。下表：每個壓制者壓了誰、壓多廣。",
        formula="gap share（壓制者）= 該 item 作為壓制者造成的 allocated_ap_gap 加總 ÷ 全體總量",
        bullets=[
            "兩張圖是**同一個全體缺口的兩種切法**、各自加總＝100%，但意義相反："
            "第一張的高條＝這個 item 常被壓（受害者），第二張的高條＝這個 item 常"
            "壓別人（壓制者）。**同一個 item 在兩張圖可以差很多**（例：exchange_usd "
            "受害者側 6.9%、壓制者側 43.9%），別把兩張圖的同名 item 當同一件事。",
            "下表「gap share（壓制者）」與受害者明細的「gap share（受害者）」同樣是"
            "「佔全體的比例」，但加總的是不同一批列，兩者不可互相驗算。",
            "「主要受害 item」只列前幾名供快速掃描，完整分布看上面的壓制矩陣。",
        ],
        figures=figs,
        tables=tables,
        table_titles=table_titles,
    )


# ─────────────────────────────────────────────────────────────────────────
# 4. 交叉購買 lift（只用圖，軸標清楚）
# ─────────────────────────────────────────────────────────────────────────

def _cross_purchase_section(result: dict) -> ReportSection | None:
    """4. 交叉購買 lift——泡泡圖，橫軸 j／縱軸 k 標清楚；不展開資料表。"""
    rows = result.get("cross_purchase") or []
    if not rows:
        return None

    shown, n_shown, n_all = _ranked_axis(result)
    shown_set = set(shown)
    grid_rows = [
        r for r in rows
        if r.get("item_j") in shown_set and r.get("item_k") in shown_set
    ]
    if not grid_rows:
        return None

    x = [str(r["item_j"]) for r in grid_rows]
    y = [str(r["item_k"]) for r in grid_rows]
    size = [float(r.get("n_joint") or 0.0) for r in grid_rows]
    colour = [
        float(r["lift"]) if r.get("lift") is not None else float("nan")
        for r in grid_rows
    ]
    hover = [
        f"買了 {r['item_j']} 的人，也買 {r['item_k']}"
        f"<br>n_joint={fmt_count(r.get('n_joint'))}（同買人數）"
        f"<br>n_j={fmt_count(r.get('n_j'))}　n_k={fmt_count(r.get('n_k'))}"
        f"<br>P(k|j)={fmt_percent(r.get('p_k_given_j'))}"
        f"<br>lift={fmt_ratio(r.get('lift'))}"
        for r in grid_rows
    ]
    fig = bubble_grid(
        x=x, y=y, size=size, colour=colour, hover_text=hover,
        title="交叉購買 lift（泡泡大小＝同買人數 n_joint，顏色＝lift）",
        colorbar_title="lift",
    )
    fig.update_layout(
        xaxis_title="item j（橫軸）＝買了這個",
        yaxis_title="item k（縱軸）＝也買了這個",
    )

    sizes_nonzero = [s for s in size if s > 0]
    size_lo = min(sizes_nonzero) if sizes_nonzero else 0.0
    size_hi = max(size) if size else 0.0
    bullets = [
        "一個泡泡在座標 (j, k)：買了橫軸 j 的人，有多常也買縱軸 k。顏色＝lift"
        "（相對 k 基礎率的倍數），大小＝同買人數 n_joint。",
        f"泡泡大小沒有圖例，n_joint 範圍是 {fmt_count(size_lo)}–{fmt_count(size_hi)}；"
        "精確值請看 hover。顏色用發散色階，lift=1（≈獨立）是中性色、低於 1 與高於 1 往兩邊。",
        "**兩種空白**：對角線（j＝k）是自我配對、刻意不算；對角線以外的空白才是 "
        "n_joint＝0（這份樣本裡沒人同買 j 與 k）。",
        "**lift 與 n_joint 對稱**（顏色、大小在對角線兩側互為鏡像），所以看單邊三角"
        "就夠；唯一有方向的是 hover 裡的 P(k|j)（買 j→買 k 與買 k→買 j 不同）。",
        "lift 而非裸 P(k|j)：熱門 item 對任何 j 的 P(k|j) 都高，只看條件機率會"
        "退化成「熱門那一列整片亮」；lift 把 k 的基礎率除掉了。",
        "與模型排序無關——這是真實標籤的共買。拿它跟壓制矩陣對照：一對 item "
        "在這裡 lift 高（本來就常一起買）、又在壓制矩陣裡互壓，是兩種不同情況。",
    ]
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，此圖只畫 overall gap share 最大的 {n_shown} 個。"
        )

    return ReportSection(
        title="交叉購買 lift",
        description="這些 item 在真實資料上本來就多常一起買——與模型排序無關的對照組。",
        formula="P(k|j) = n_joint ÷ n_j；lift = P(k|j) ÷ (n_k ÷ n_units)",
        bullets=bullets,
        figures=[fig],
        tables=[_defs_table("n_units", "n_j / n_k", "n_joint", "P(k|j)", "lift")],
        table_titles=[_DEFS_TITLE],
    )


# ─────────────────────────────────────────────────────────────────────────
# 5. 具體案例表
# ─────────────────────────────────────────────────────────────────────────

_EXAMPLE_COLUMNS = [
    "query", "positive_item", "suppressor_item",
    "positive_rank", "suppressor_rank",
    "positive_score (logit)", "suppressor_score (logit)", "score_margin",
    "此列分攤缺口",
]


def _examples_section(result: dict) -> ReportSection | None:
    """5. 具體案例表——分攤缺口最大的單列 (正例, 壓制者)，逐案核對用。"""
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
                "positive_score (logit)": fmt_logodds(e.get("positive_score")),
                "suppressor_score (logit)": fmt_logodds(e.get("suppressor_score")),
                "score_margin": fmt_logodds(e.get("score_margin")),
                "此列分攤缺口": fmt_ap(e.get("allocated_ap_gap")),
            }
            for e in examples
        ],
        columns=_EXAMPLE_COLUMNS,
    )

    return ReportSection(
        title="具體案例：被壓制的正例列",
        description="分攤缺口最大的單列 (正例, 壓制者) 案例，逐列列出供核對。",
        formula="score_margin = suppressor_score − positive_score（兩者都已是 logit 分數，直接相減）",
        bullets=[
            "「此列分攤缺口」是 allocated_ap_gap 的**單列原子值**（不是前面幾區的加總值）——"
            "同一個 (受害, 壓制者) 組合在很多列出現，前面矩陣／分布是把這些單列值加總後的結果。",
            "分數欄已經是 logit 分數；score_margin 就是兩者相減，正值代表壓制者分數確實較高。",
            "依此列分攤缺口由大到小排；資料很規律時前幾名可能同值（同一種名次配置重複很多列）。",
            "逐列核對用，不作聚合證據——聚合看前面幾區。",
        ],
        tables=[
            pd.DataFrame(
                [
                    {"數字": "score_margin", "定義": _DEFS["score_margin"]},
                    {"數字": "此列分攤缺口（allocated_ap_gap 單列值）",
                     "定義": _DEFS["allocated_ap_gap"]},
                ],
                columns=["數字", "定義"],
            ),
            table,
        ],
        table_titles=[_DEFS_TITLE, "案例（依此列分攤缺口降冪）"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 6. 完整性檢查
# ─────────────────────────────────────────────────────────────────────────

def _completeness_section(result: dict) -> ReportSection:
    """6. 完整性檢查——固定殿後，印出 metric k 與 allocated 總量。"""
    notes = result.get("notes") or []
    axis_order = result.get("axis_order") or []
    cross_purchase = result.get("cross_purchase") or []

    bullets = [
        f"metric k = {_k_display(result)}；"
        f"全體 allocated_ap_gap 總量 = {fmt_mean(result.get('total_ap_gap_allocated_to_suppressors'))}"
        "（overall gap share 的分母）。",
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

    順序即閱讀順序：受害者明細 → 誰壓誰（矩陣）→ 壓制者明細與分布 →
    交叉購買（對照組）→ 具體案例 → 完整性檢查。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _target_summary_section(result),
        _matrix_section(result),
        _by_suppressor_section(result),
        _cross_purchase_section(result),
        _examples_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
