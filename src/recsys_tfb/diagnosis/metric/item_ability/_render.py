"""item_ability 的呈現層：把 ``compute`` 的 JSON 轉成一串 ``ReportSection``。

形狀照抄 ``config_shift/_render.py``（見該檔 docstring 的完整理由）：純函式、
不 import pyspark、不讀檔、不做任何計算——出現在報表上的每個數字都必須已經
在 JSON 裡。一個 section ＝ 一句 description ＋ 一行 formula ＋ 最多 3 則
bullet ＋ 它自己的圖或表；停用時回空 tuple；最後一節固定是「本次執行的完整
性檢查」。

**本項的核心是散點圖**（見 :func:`_scatter_section`）：raw_within_item_auc
與 query_centered_auc 的差，就是「客戶活躍度被誤計入 item 推薦能力」的量，
散點圖偏離 y=x 對角線的距離就是它——這條對角參考線不是裝飾，是這張圖唯一的
判讀依據。

**四節內容 ＋ 完整性檢查（共 5 節）**：
1. raw vs centered AUC 散點（核心）。
2. 逐 item 的 AUC 條圖，raw／centered 各一張，含 95% CI 誤差線；橫向參考線
   標 0.500（隨機打散的期望值——"對照點文字"併入這一節，見下方 3.）。
3. AUC 差 auc_gap_raw_minus_centered 條圖：**保留正負號，不取絕對值**——
   方向本身就是「活躍度混入」的方向，這是整項診斷最容易被實作者無意間破壞
   的一條規則（見 ``_compute.py`` 同一句警語）。
4. 正例名次百分位分布：AP 最低的前 ``top_n`` 個 item（``per_item`` 已依 AP
   遞增排序，這裡只切片、不重排——排序權在 compute，不在 render）。

**唯一對照點是 0.500，不是兩個**：原稿還規劃了「item 全域購買率排序」的
baseline，2026-07-20 實測後撤除——那條 baseline 在 within-item AUC（正例列
vs 負例列都在同一 item 內）底下，所有列拿到同一個分數，依同分半分規則恆等
於 0.500，與隨機對照點逐位元重合，印成兩列只會讓讀者以為是兩個獨立基準。
理由與實測數字見 ``docs/superpowers/plans/diag-redesign/03-plan-2-item-ability-capacity.md``
Task 3.2 段落。

**AUC 有缺席的 item 不強行畫成 0**：``raw_within_item_auc``／
``query_centered_auc`` 在該 item 正例或負例列數為 0 時是 ``None``（見
``_compute.py`` 的 ``weighted_auc_presorted``）。這裡一律先過濾掉，缺席的
item 不進散點圖／條圖，但完整性檢查那一節會點名它們——「缺席」與「量到、
結果剛好是某個值」在報表上必須長得不一樣。
"""
from __future__ import annotations

from typing import Any

import pandas as pd

from recsys_tfb.report import ReportSection
from recsys_tfb.report.figures import MAX_FIGURE_POINTS, bar, fits_budget, scatter
from recsys_tfb.report.fmt import fmt_auc, fmt_count, fmt_delta

_ITEM = "item"
_RAW_AUC = "raw_within_item_auc"
_CENTERED_AUC = "query_centered_auc"
_RAW_CI_LOW = "raw_within_item_auc_ci_low"
_RAW_CI_HIGH = "raw_within_item_auc_ci_high"
_CENTERED_CI_LOW = "query_centered_auc_ci_low"
_CENTERED_CI_HIGH = "query_centered_auc_ci_high"
_GAP = "auc_gap_raw_minus_centered"

#: 隨機打散正負例的 AUC 期望值——唯一的對照點，見模組 docstring。
_RANDOM_AUC = 0.500

_FORMULA_AUC = (
    "auc_gap_raw_minus_centered = raw_within_item_auc − query_centered_auc\n"
    "raw ＝ AUC(logit(score_uncalibrated), label)；"
    "centered ＝ AUC(logit(score_uncalibrated) − query 平均, label)"
)

_FORMULA_WEIGHTED_AUC = (
    "AUC ＝ Σ pos_w·(neg_before ＋ 0.5·neg_tie) / (pos_total · neg_total)"
    "（inclusion_weight 加權，同分給 0.5 分）\n"
    "誤差線 ＝ 95% 區間，來自分層 cluster bootstrap（cluster＝entity，見 ci.n_boot）"
)

_FORMULA_GAP = "auc_gap_raw_minus_centered = raw_within_item_auc − query_centered_auc"

_FORMULA_RANK = (
    "rank_percentile ＝ 名次（依分數由高到低，1-based）÷ query 內候選數"
    "（1/候選數 最好、1.0 最差）\n"
    "誤差線 ＝ p25 ~ p75（四分位距，不是信賴區間）"
)


def _rows_with(per_item: list[dict], *keys: str) -> list[dict]:
    """只保留指定鍵全部非 ``None`` 的列。

    AUC 缺席（該 item 正例或負例列數為 0）不強行畫成 0——那會讓「量不到」
    看起來像「量到了、結果是 0」，兩者在報表上必須長得不一樣。
    """
    return [r for r in per_item if all(r.get(k) is not None for k in keys)]


def _scatter_section(result: dict) -> ReportSection | None:
    """1. raw vs centered AUC 散點——本項的核心。"""
    rows = _rows_with(result.get("per_item") or [], _RAW_AUC, _CENTERED_AUC)
    if not rows:
        return None

    items = [str(r["item"]) for r in rows]
    raw = [float(r[_RAW_AUC]) for r in rows]
    centered = [float(r[_CENTERED_AUC]) for r in rows]

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        "點落在對角虛線上代表 raw 與 centered AUC 相等；偏離對角線的距離就是"
        "auc_gap_raw_minus_centered 的大小。",
        "點在對角線下方（centered < raw）代表 raw AUC 有一部分來自 query 間"
        "的整體分數差異，而非 query 內的相對排序；線上方則相反。",
    ]

    if fits_budget(len(rows)):
        fig = scatter(
            x=raw, y=centered, labels=items,
            title="raw vs query-centered AUC（逐 item）",
            x_title="raw_within_item_auc", y_title="query_centered_auc",
        )
        lo = min(min(raw), min(centered))
        hi = max(max(raw), max(centered))
        if lo == hi:  # 良性退化：所有點重合在一點，仍要有一條看得見的參考線
            lo, hi = lo - 0.5, hi + 0.5
        fig.add_shape(
            type="line", x0=lo, y0=lo, x1=hi, y1=hi,
            line=dict(dash="dot", width=1),
        )
        figures.append(fig)
    else:
        tables.append(pd.DataFrame(
            [
                {_ITEM: it, "raw AUC": fmt_auc(r), "centered AUC": fmt_auc(c)}
                for it, r, c in zip(items, raw, centered)
            ],
            columns=[_ITEM, "raw AUC", "centered AUC"],
        ))
        table_titles.append("raw vs query-centered AUC（逐 item）")
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "改以表格呈現。"
        )

    return ReportSection(
        title="raw vs query-centered AUC 散點",
        description="每個 item 一個點：x 是原始 AUC，y 是扣掉 query 平均後的 AUC。",
        formula=_FORMULA_AUC,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _per_item_auc_section(result: dict) -> ReportSection | None:
    """2. 逐 item 的 AUC 條圖（raw／centered 各一張），含 95% CI 誤差線。

    0.500 對照點併在這一節：橫向參考線 ＋ bullet 說明，這是使用者要求的
    「唯一對照點」的落點——見模組 docstring「唯一對照點是 0.500」。
    """
    rows = _rows_with(result.get("per_item") or [], _RAW_AUC, _CENTERED_AUC)
    if not rows:
        return None

    items = [str(r["item"]) for r in rows]
    ci_info = result.get("ci") or {}
    n_boot = ci_info.get("n_boot")

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        f"0.500 ＝ 在這個 item 內完全沒有辨識力（隨機打散正負例的 AUC 期望值"
        f"，{_RANDOM_AUC:.3f}）；圖中橫虛線標示此值。",
    ]

    if fits_budget(len(rows)):
        for auc_key, ci_lo_key, ci_hi_key, label in (
            (_RAW_AUC, _RAW_CI_LOW, _RAW_CI_HIGH, "raw_within_item_auc"),
            (_CENTERED_AUC, _CENTERED_CI_LOW, _CENTERED_CI_HIGH, "query_centered_auc"),
        ):
            y = [r[auc_key] for r in rows]
            lo = [r.get(ci_lo_key) for r in rows]
            hi = [r.get(ci_hi_key) for r in rows]
            fig = bar(
                x=items, y=y,
                title=f"逐 item 的 {label}（95% CI）",
                x_title=_ITEM, y_title=label,
                ci_low=lo, ci_high=hi,
            )
            fig.add_hline(y=_RANDOM_AUC, line=dict(dash="dot", width=1))
            figures.append(fig)
    else:
        tables.append(pd.DataFrame(
            [
                {
                    _ITEM: r["item"],
                    "raw AUC": fmt_auc(r.get(_RAW_AUC)),
                    "raw CI": f"[{fmt_auc(r.get(_RAW_CI_LOW))}, "
                              f"{fmt_auc(r.get(_RAW_CI_HIGH))}]",
                    "centered AUC": fmt_auc(r.get(_CENTERED_AUC)),
                    "centered CI": f"[{fmt_auc(r.get(_CENTERED_CI_LOW))}, "
                                   f"{fmt_auc(r.get(_CENTERED_CI_HIGH))}]",
                }
                for r in rows
            ],
            columns=[_ITEM, "raw AUC", "raw CI", "centered AUC", "centered CI"],
        ))
        table_titles.append("逐 item 的 raw／query-centered AUC（95% CI）")
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "改以表格呈現。"
        )

    if n_boot is not None:
        bullets.append(f"CI 來自 {fmt_count(n_boot)} 次分層 cluster bootstrap 重抽。")

    return ReportSection(
        title="逐 item 的 AUC（含信賴區間）",
        description="raw 與 query-centered AUC 各自的點估計與 95% 區間，逐 item 對照。",
        formula=_FORMULA_WEIGHTED_AUC,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _gap_section(result: dict) -> ReportSection | None:
    """3. AUC 差 auc_gap_raw_minus_centered——保留正負號，不取絕對值。"""
    rows = _rows_with(result.get("per_item") or [], _GAP)
    if not rows:
        return None

    items = [str(r["item"]) for r in rows]
    gaps = [float(r[_GAP]) for r in rows]

    figures: list[Any] = []
    tables: list[pd.DataFrame] = []
    table_titles: list[str] = []
    bullets = [
        "正值＝raw AUC 高於 centered AUC；負值＝centered AUC 反而較高——方向"
        "本身就是訊號，這裡不取絕對值。",
    ]

    if fits_budget(len(rows)):
        figures.append(bar(
            x=items, y=gaps,
            title="逐 item 的 auc_gap_raw_minus_centered",
            x_title=_ITEM, y_title="raw − centered",
            center=0.0,
        ))
    else:
        tables.append(pd.DataFrame(
            [{_ITEM: it, "raw − centered": fmt_delta(g)} for it, g in zip(items, gaps)],
            columns=[_ITEM, "raw − centered"],
        ))
        table_titles.append("逐 item 的 auc_gap_raw_minus_centered")
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "改以表格呈現。"
        )

    return ReportSection(
        title="AUC 差：raw − centered",
        description="兩個 AUC 的差值，逐 item 呈現，正負與大小都保留。",
        formula=_FORMULA_GAP,
        bullets=bullets,
        figures=figures,
        tables=tables,
        table_titles=table_titles,
    )


def _rank_percentile_section(result: dict) -> ReportSection | None:
    """4. 正例名次百分位分布：AP 最低的前 ``top_n`` 個 item。

    ``per_item`` 已依 ``ap`` 遞增排序（``_compute.compute`` 的排序鍵），這裡
    只切片、不重排——排序權在 compute，render 只讀。
    """
    per_item = result.get("per_item") or []
    top_n = int(result.get("top_n") or 0)
    rows = [
        r for r in per_item[:top_n]
        if r.get("median_positive_rank_percentile") is not None
    ]
    if not rows:
        return None

    items = [str(r["item"]) for r in rows]
    median = [r["median_positive_rank_percentile"] for r in rows]
    p25 = [r.get("p25_positive_rank_percentile") for r in rows]
    p75 = [r.get("p75_positive_rank_percentile") for r in rows]

    figures: list[Any] = []
    bullets = [
        "只列 macro per-item AP 最低的前 top_n 個 item（依 compute 排序，未在"
        "這裡重排）。",
        "名次百分位越接近 0 越好，1.0 是同一 query 內排最後；誤差線是 p25~p75"
        "（四分位距，不是信賴區間）。",
    ]

    if fits_budget(len(rows)):
        figures.append(bar(
            x=items, y=median,
            title="正例名次百分位（AP 最低的前幾個 item）",
            x_title=_ITEM, y_title="median_positive_rank_percentile",
            ci_low=p25, ci_high=p75,
        ))
    else:
        bullets.append(
            f"item 共 {len(rows)} 個，超過單張圖的 {MAX_FIGURE_POINTS} 點上限，"
            "只以表格呈現。"
        )

    table = pd.DataFrame(
        [
            {
                _ITEM: r["item"],
                "median": fmt_auc(r.get("median_positive_rank_percentile")),
                "p25": fmt_auc(r.get("p25_positive_rank_percentile")),
                "p75": fmt_auc(r.get("p75_positive_rank_percentile")),
                "p90": fmt_auc(r.get("p90_positive_rank_percentile")),
                "n_pos": fmt_count(r.get("n_pos")),
            }
            for r in rows
        ],
        columns=[_ITEM, "median", "p25", "p75", "p90", "n_pos"],
    )

    return ReportSection(
        title="正例名次百分位分布",
        description="AP 最低的前幾個 item，看它們的正例平常排在 query 內第幾名。",
        formula=_FORMULA_RANK,
        bullets=bullets,
        figures=figures,
        tables=[table],
        table_titles=["正例名次百分位分布（median／p25／p75／p90）"],
    )


def _completeness_section(result: dict) -> ReportSection:
    """5. 本次執行的完整性檢查——這一節不可省，空的時候也照樣印「無」。

    與 ``config_shift`` 的 ``_visibility_section`` 同一份角色分工：頁首
    ``ScopeNote.blind_to`` 是這個指標**結構上**推論不到的事，這一節是三種
    **已知靜默失效**在**本次執行**的實際結果。
    """
    per_item = result.get("per_item") or []
    notes = result.get("notes") or []
    ci_info = result.get("ci") or {}

    missing_auc = [
        str(r["item"]) for r in per_item
        if r.get(_RAW_AUC) is None or r.get(_CENTERED_AUC) is None
    ]

    bullets = [
        "計算層 notes（含 logit 轉換的觀測）："
        + (f"{len(notes)} 則，列於下方" if notes else "無"),
        "本次抽樣中正例或負例列數為 0、未算出 AUC 的 item（不進散點圖與長條"
        "圖，但仍計入下面的 item 總數）："
        + (", ".join(missing_auc) if missing_auc else "無"),
        "本次執行是否啟用信賴區間 bootstrap："
        + (
            f"啟用，{fmt_count(ci_info.get('n_boot'))} 次重抽"
            if ci_info.get("enabled") else "未啟用"
        ),
    ]
    # 計算層的 notes 原文照登——它們是 compute 對自己這次執行的觀測，改寫就
    # 失真（與 config_shift 的同一句設計理由一致）。
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

    回空 tuple 而不是空 section 是刻意的（見 ``config_shift.render`` 同一句
    理由）：組裝層一律以「序列為空 ＝ 這頁不存在」判斷。

    順序即閱讀順序：先看兩個 AUC 差多少、多離散（1–2），再看差值本身的正負
    與規模（3），最後看排名最差的那批 item 具體排在第幾名（4）。完整性檢查
    永遠在最後。
    """
    if not result.get("enabled"):
        return ()

    sections = [
        _scatter_section(result),
        _per_item_auc_section(result),
        _gap_section(result),
        _rank_percentile_section(result),
        _completeness_section(result),
    ]
    return tuple(s for s in sections if s is not None)
