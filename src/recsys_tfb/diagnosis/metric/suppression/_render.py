"""suppression 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``item_ability/_render.py``（見該檔 docstring）：純函式、不 import
pyspark、不讀檔、不做計算——出現在報表上的每個數字都必須已經在 JSON 裡。
停用時回空 tuple；最後一節固定是「本次執行的完整性檢查」。

**版面（2026-07-21 第三輪，依使用者「整體邏輯架構」要求重排）**：整份診斷只有
一個基礎量 ``allocated_ap_gap``（逐列原子），其餘全是它加總到不同粒度。頂部兩塊
是定向：概覽（規模＋關鍵數字＋如何讀）→ 核心概念（把基礎量講清楚一次），後面
每區只說「這是它加總到 X」：

* **概覽**——一眼看規模、整體排序品質、壓制多普遍/多深、被歸因的總損失，外加
  「想回答什麼問題 → 看哪一區」的中性導覽（不替讀者判斷誰最該查，守不下結論）。
0. **核心概念**——``allocated_ap_gap`` 是什麼、怎麼把一列的 AP 缺口按 severity
   拆給上方各負例（含拆分公式與例子），以及全體總量（所有 gap share 的分母）與
   「下面每區＝它加總到什麼粒度」的地圖。
1. **per-item 壓制彙總**（受害者明細表）——加總到 item 當受害者。主表。
2. **壓制矩陣：誰壓誰**——加總到 (受害, 壓制者) pair，按列看的百分比數字表
   （不用熱圖：每列自成一個分布、橫加＝1，全域色階會誘導跨列比色而誤讀）。
3. **壓制者明細**＋兩個視角的**缺口分布長條圖**——加總到 item 當壓制者。
4. **交叉購買 lift**——只用圖（軸標清楚 j／k），與模型排序無關的對照組。
5. **具體案例表**——不加總的單列 ``allocated_ap_gap`` 值。
6. **完整性檢查**（固定殿後，印出 metric k 與 allocated 總量）。

**基礎量在開頭講一次**（核心概念區）；**各區只補自己專屬的數字**（每區第一張表
「本區數字定義」只放該區新出現的量，``allocated_ap_gap`` 一律回指開頭，不再重述
整條拆分公式）。定義文字集中在 :data:`_DEFS`（單一真實來源）。

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
        "整份報表的基礎量（逐列）：正例列的 AP 缺口，分給排在它上方各負例的一份。"
        "完整拆分公式與例子見開頭「核心概念」區；這裡出現的數字都是它加總到不同粒度"
        "的結果。分帳、非因果。",
    "全體 allocated_ap_gap 總量":
        "所有 (受害, 壓制者) 的 allocated_ap_gap 全部加總＝所有 gap share 的共同分母。"
        "見開頭「核心概念」區；受害者側全部 gap share 加起來＝100%、壓制者側也是。",
    "AP": "該 item 在它自己的正例列上的 average precision（0–1）；越高＝正例越常被排在前面。",
    "AP gap": "1 − AP。離「正例全部排最前」還差多少（每列平均）。",
    "AP gap from suppressors":
        "AP gap 裡「有負例壓在上面」的那部分：＝ 該 item 的 allocated_ap_gap 加總 ÷ 正例列數（n_pos）；"
        "與 unexplained AP gap 相加剛好 ＝ AP gap。",
    "unexplained AP gap":
        "AP gap − AP gap from suppressors：缺口裡沒有負例壓在上面的部分。本次 k=all 時它結構上恆為 0"
        "（一個正例只要沒排最前、上方必有負例）；只有把 k 設成有限值、正例落到 top-k 之外時才會 > 0。",
    "gap share（受害者）":
        "該 item 作為受害者、攤在它頭上的 allocated_ap_gap 加總 ÷ 全體 allocated_ap_gap 總量"
        "（見開頭「核心概念」）。＝（AP gap from suppressors × n_pos）÷ 該總量，可自行驗算。",
    "gap share（壓制者）":
        "該 item 作為壓制者、它造成的 allocated_ap_gap 加總 ÷ 全體 allocated_ap_gap 總量"
        "（同一個分母）。⚠ 與『受害者側』同樣是佔全體的比例、但加總的是不同一批列——同一個 "
        "item 兩者可以差很多（很少被壓、卻常壓別人），不要當同一個數。",
    "suppressed pos / n_pos":
        "至少被一個負例壓過的正例列 ÷ 該 item 全部正例列（n_pos）；被壓制有多普遍（與 k 無關）。",
    "n_pos": "該 item 的正例列數（label=1 的列數）。",
    "median pos rank":
        "寫成「a of b」，讀作『名次中位數 a、每個 query 平均約 b 個候選 item』——"
        "a 是名次的中位數、b 是候選數的平均，不是分數 a/b。a 越接近 b 越常墊底。",
    "mean neg above": "該 item 的每個正例列上方，平均有幾個負例（與 k 無關；是個數不是倍率）。",
    "target gap share":
        "壓制者欄的 allocated_ap_gap 加總 ÷ 受害 item 列的 allocated_ap_gap 加總；同一列橫著加＝100%。",
    "score_margin":
        "壓制者分數 − 正例分數（兩者都已是 logit 分數，直接相減，不再套一層）；正值＝壓制者分數確實較高。",
    "n_units": "樣本內相異 query 單位數；一個 query 單位＝一組 (time, entity)。",
    "n_j / n_k": "item j／k 為正例（label=1）的 query 單位數。",
    "n_joint": "同一個 query 單位上 j 與 k 都是正例的單位數（對稱：n_joint(j,k)=n_joint(k,j)）。",
    "P(k|j)": "n_joint ÷ n_j：買了 j 的 query 單位裡，有多少也買了 k（有方向，P(k|j)≠P(j|k)）。",
    "lift":
        "P(k|j) ÷ (n_k ÷ n_units)＝n_joint × n_units ÷ (n_j × n_k)：相對 k 基礎率的倍數。"
        "對稱：lift(j,k)=lift(k,j)；lift=1 ≈ 在這份樣本上近似獨立。",
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
# 概覽——一眼看規模＋關鍵數字，以及「如何讀這頁」的導覽
# ─────────────────────────────────────────────────────────────────────────

def _summary_section(result: dict) -> ReportSection:
    """概覽——放什麼由整體邏輯判斷：整體排序品質、壓制多普遍/多深、被歸因的
    總損失、規模；再加一段中性導覽（想回答什麼問題 → 看哪一區）。"""
    n_sup = result.get("n_suppressed_positive_rows")
    n_pos = result.get("n_positive_rows")
    scorecard = pd.DataFrame(
        [
            {"分類": "整體狀況", "指標": "排序品質 macro per-item mAP",
             "值": f"{fmt_ap(result.get('macro_per_item_map'))}（0–1，越高越好）"},
            {"分類": "整體狀況", "指標": "被壓制的正例（至少一個負例排在上面）",
             "值": f"{fmt_count(n_sup)} / {fmt_count(n_pos)}"
                   f"（{fmt_percent(result.get('suppressed_positive_rate'))}）"},
            {"分類": "整體狀況", "指標": "每個正例上方平均負例數 mean neg above",
             "值": fmt_mean(result.get("mean_negatives_above_positive"))},
            {"分類": "規模與尺度（非好壞）", "指標": "規模",
             "值": f"{fmt_count(result.get('n_queries'))} queries · "
                   f"{fmt_count(result.get('n_items'))} items · "
                   f"{fmt_count(n_pos)} 正例列"},
            {"分類": "規模與尺度（非好壞）",
             "指標": "全體 allocated_ap_gap 總量（是 gap share 的分母，不是嚴重度）",
             "值": fmt_mean(result.get("total_ap_gap_allocated_to_suppressors"))},
        ],
        columns=["分類", "指標", "值"],
    )
    return ReportSection(
        title="概覽",
        description="這份診斷回答：同一個 query 內，哪些負例（label=0）排在正例（label=1）之上、造成多少排序損失。",
        bullets=[
            "上表分兩塊：「整體狀況」的三個數可判斷壓制嚴不嚴重；「規模與尺度」"
            "只是規模與歸一化用的分母，不代表好壞（總量 295 這種數沒有『多糟』的尺度）。",
            "如何讀這頁（想回答什麼 → 看哪一區，順序不必照排）：",
            "想從 item 角度看誰被壓、被壓多少 → 「per-item 壓制彙總」"
            "（預設按每列嚴重度排）。",
            "想知道某個受害 item 是被誰壓的 → 「壓制矩陣」（按列看）。",
            "想知道某個 item 多常去壓別人 → 「壓制者明細與缺口分布」。",
            "想看這對 item 在真實資料上本來就多常一起買（模型無關的對照）→ 「交叉購買 lift」。",
            "想核對具體某一筆 → 「具體案例」。",
            "每個數字的定義就貼在它出現的那一區；貫穿全報表的基礎量 allocated_ap_gap "
            "與總量，完整說明在下一節「核心概念」。",
        ],
        tables=[scorecard],
        table_titles=["本次執行的關鍵數字"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 0. 核心概念——整份報表的兩塊地基：allocated_ap_gap（逐列原子）＋全體總量
# ─────────────────────────────────────────────────────────────────────────

def _foundation_section(result: dict) -> ReportSection:
    """0. 核心概念——先把 allocated_ap_gap 與全體總量講清楚一次。"""
    total = result.get("total_ap_gap_allocated_to_suppressors")
    illustration = pd.DataFrame(
        [
            {"角色": "正例", "名次": "rank 3（目前 AP 貢獻 = 1/3）",
             "severity": "—", "分到的 allocated_ap_gap": "—"},
            {"角色": "壓制者 A", "名次": "rank 1",
             "severity": "1 − 1/3 = 2/3", "分到的 allocated_ap_gap": "0.533"},
            {"角色": "壓制者 B", "名次": "rank 2",
             "severity": "1/2 − 1/3 = 1/6", "分到的 allocated_ap_gap": "0.133"},
            {"角色": "合計", "名次": "",
             "severity": "比 4 : 1", "分到的 allocated_ap_gap": "0.667 = row_ap_gap"},
        ],
        columns=["角色", "名次", "severity", "分到的 allocated_ap_gap"],
    )
    return ReportSection(
        title="核心概念：allocated_ap_gap 與全體總量",
        description=(
            "整份診斷只有一個基礎量 allocated_ap_gap，其餘每張表、每張圖都是它"
            "加總到不同粒度的結果。先看懂這一節，後面都是同一個數換個切法。"
        ),
        formula="allocated_ap_gap(某正例列, 負例 s) = row_ap_gap × severity(s) ÷ Σ severity",
        bullets=[
            f"「壓制」＝同一個 query 內，負例（label=0）排在正例（label=1）之上。"
            f"本次 metric k = {_k_display(result)}——壓制的計數（mean neg above、被壓率）"
            "與 k 無關；AP 缺口與其分攤才在 top-k 內衡量。",
            "row_ap_gap = 1 − 該正例列目前的 AP 貢獻（這一列離「排最前」還差多少）。",
            "severity(s) = 把正例提到負例 s 的名次、它的 AP 貢獻會多出多少——負例排得"
            "越高、提上去越賺，severity 越大。一列的缺口就按各負例的 severity 比例分下去"
            "（見下方示意表）。",
            "這是分帳、不是因果：把某壓制者分到的份加起來，不代表「拿掉它就會賺回這麼多」。",
            f"全體 allocated_ap_gap 總量 = {fmt_mean(total)}：所有 (受害, 壓制者) 的份"
            "全部加總。它是後面所有 gap share 的共同分母——受害者側全部 gap share "
            "加起來＝100%、壓制者側也是 100%（同一筆總量的兩種切法）。",
            "下面每一區都是這個數的加總：具體案例＝不加總的單列值；壓制矩陣＝加總到 "
            "(受害, 壓制者) 組合；per-item 受害者側＝加總到 item 當受害者；壓制者明細＝"
            "加總到 item 當壓制者。理解這一個數＋這一個總量，其餘只是「加總到什麼粒度」。",
        ],
        tables=[illustration],
        table_titles=["示意：正例在 rank 3、上方兩個負例，缺口 0.667 按 severity 4:1 拆成 0.533 / 0.133"],
    )


# ─────────────────────────────────────────────────────────────────────────
# 1. per-item 壓制彙總（受害者明細表）
# ─────────────────────────────────────────────────────────────────────────

_TARGET_COLUMNS = [
    "受害 item", "AP", "AP gap from suppressors", "unexplained AP gap",
    "gap share（受害者）", "n_pos", "suppressed pos / n_pos",
    "mean neg above", "median pos rank", "頭號壓制者",
]


def _target_summary_section(result: dict) -> ReportSection | None:
    """1. 受害者明細表——每個受害 item 一列。

    排序：依 ``ap_gap_from_suppressors`` 降冪（每列平均被壓制的嚴重度，與筆數
    無關），不用 compute 給的 ``overall_ap_gap_share`` 序（那是筆數加權的總
    損失，會把排得不錯的大宗 item 頂到最前）。
    """
    rows = result.get("target_summary") or []
    if not rows:
        return None

    rows = sorted(
        rows, key=lambda r: -(r.get("ap_gap_from_suppressors") or 0.0),
    )
    table = pd.DataFrame(
        [
            {
                "受害 item": r.get("positive_item"),
                "AP": fmt_ap(r.get("ap")),
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
        formula="AP gap from suppressors + unexplained AP gap = AP gap = 1 − AP",
        bullets=[
            "這是把 allocated_ap_gap 加總到「item 當受害者」（見開頭核心概念）。",
            "排序依 AP gap from suppressors 降冪＝每列平均被壓制的嚴重度（與筆數無關），"
            "把排得最差的 item 放最前。另一個角度是 gap share（受害者），那是筆數加權的"
            "總損失（正例列數多的 item 數值較大）——兩個角度都在表裡，哪個重要由你決定。",
            "AP 越低＝排序越差；median pos rank 越接近 b of b＝正例越常墊底。",
            f"gap share（受害者）＝（AP gap from suppressors × n_pos）÷ 全體總量 "
            f"{fmt_mean(total)}（見開頭），可自行驗算。",
            "unexplained AP gap 這欄本次全為 0，是 k=all 的結構性結果（見定義），不是表壞了。",
            "「頭號壓制者」就是壓制矩陣裡這一列 target gap share 最大的那一欄。",
        ],
        tables=[
            _defs_table(
                "AP", "AP gap from suppressors", "unexplained AP gap",
                "gap share（受害者）", "n_pos", "suppressed pos / n_pos",
                "median pos rank", "mean neg above",
            ),
            table,
        ],
        table_titles=[_DEFS_TITLE, "受害者明細（依 AP gap from suppressors 降冪＝嚴重度優先）"],
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
        "按列讀：每一列是一個受害 item，橫著看它的缺口被哪些壓制者（欄）"
        "分走，同一列橫著加＝100%。不要跨列比大小——每列各自是一個分布。",
        "空白格＝該壓制者完全沒分到這個受害 item 的缺口（恰為 0）；"
        "「0.0%」＝極小但非零（四捨五入到 0.0%）。",
        "每列最大的那一欄，就是受害者明細表裡這個 item 的「頭號壓制者」。",
    ]
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，此表只列 gap share（受害者）最大的 {n_shown} 個"
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
        table_titles.append("壓制者明細（依 gap share 壓制者側降冪）")

    return ReportSection(
        title="壓制者明細與缺口分布",
        description="兩張圖：同一筆全體缺口，一張看攤在誰頭上（受害者）、一張看誰造成（壓制者）。下表：每個壓制者壓了誰、壓多廣。",
        formula="gap share（壓制者）= 該 item 作為壓制者造成的 allocated_ap_gap 加總 ÷ 全體總量",
        bullets=[
            "兩張圖是同一個全體缺口的兩種切法、各自加總＝100%，但意義相反："
            "第一張的高條＝這個 item 常被壓（受害者），第二張的高條＝這個 item 常"
            "壓別人（壓制者）。同一個 item 在兩張圖可以差很多（例：exchange_usd "
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
        "兩種空白：對角線（j＝k）是自我配對、刻意不算；對角線以外的空白才是 "
        "n_joint＝0（這份樣本裡沒人同買 j 與 k）。",
        "lift 與 n_joint 對稱（顏色、大小在對角線兩側互為鏡像），所以看單邊三角"
        "就夠；唯一有方向的是 hover 裡的 P(k|j)（買 j→買 k 與買 k→買 j 不同）。",
        "lift 而非裸 P(k|j)：熱門 item 對任何 j 的 P(k|j) 都高，只看條件機率會"
        "退化成「熱門那一列整片亮」；lift 把 k 的基礎率除掉了。",
        "怎麼用：這張圖量的是真實共買（與模型無關），壓制矩陣量的是模型排序行為。"
        "同一對 item 在兩張圖各自的值並列給你——例如一對在這裡 lift 很高、又在壓制"
        "矩陣裡互壓，這兩個事實各自成立。要不要把它讀成「模型排錯」或「商品本來就"
        "競爭同一批客戶」，是你的判讀，這頁只把兩邊數字擺出來、不替你選。",
    ]
    if n_all > n_shown:
        bullets.append(
            f"item 共 {n_all} 個，此圖只畫 gap share（受害者）最大的 {n_shown} 個。"
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
    "allocated_ap_gap（此列值）",
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
                "allocated_ap_gap（此列值）": fmt_ap(e.get("allocated_ap_gap")),
            }
            for e in examples
        ],
        columns=_EXAMPLE_COLUMNS,
    )

    return ReportSection(
        title="具體案例：被壓制的正例列",
        description="這是不加總的單列 allocated_ap_gap（見開頭核心概念），分攤缺口最大的幾列，逐案核對用。",
        formula="score_margin = suppressor_score − positive_score（兩者都已是 logit 分數，直接相減）",
        bullets=[
            "「allocated_ap_gap（此列值）」是開頭那個基礎量的單列原子值——前面矩陣／"
            "分布是把同一個 (受害, 壓制者) 組合的這些單列值加總後的結果。",
            "分數欄已經是 logit 分數；score_margin 就是兩者相減，正值代表壓制者分數確實較高。",
            "依此列值由大到小排；資料很規律時前幾名可能同值（同一種名次配置重複很多列）。",
            "逐列核對用，不作聚合證據——聚合看前面幾區。",
        ],
        tables=[_defs_table("score_margin", "allocated_ap_gap"), table],
        table_titles=[_DEFS_TITLE, "案例（依 allocated_ap_gap 此列值降冪）"],
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
        "（所有 gap share 的分母，另見概覽與核心概念）。",
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

    順序即閱讀順序：概覽 → 核心概念（allocated_ap_gap ＋ 全體總量）→ 受害者明細
    → 誰壓誰（矩陣）→ 壓制者明細與分布 → 交叉購買（對照組）→ 具體案例 →
    完整性檢查。概覽與核心概念永遠在最前、完整性檢查永遠殿後。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _summary_section(result),
        _foundation_section(result),
        _target_summary_section(result),
        _matrix_section(result),
        _by_suppressor_section(result),
        _cross_purchase_section(result),
        _examples_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
