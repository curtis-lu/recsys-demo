"""核心概念: the one quantity every other section aggregates, defined once
with a worked example."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.report import ReportSection

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ()


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """核心概念（地基）：講清一次原子量，後面各區都是它換切法。

    presentation §一.2：定義 ＋ 用一個具體數字走一遍 ＋「下面每區＝它加總到
    什麼粒度」的地圖。不各區重複這條定義（會漂移）。

    角色名一律走 ``core.schema.get_schema``，不在這裡自備一份預設表。這一段
    印的是讀者自己的欄名，第二份預設表只會在某一天跟 ``core/schema`` 的那份
    分岔——而分岔的樣子是報表印著「``prod_name``」、其餘各區印著使用者真正的
    欄名，兩邊都不報錯（#326）。``get_schema`` 也負責把 ``entity`` 正規化成
    list，所以這裡不必再判斷型別。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_str = "×".join(schema["entity"])
    item_col = schema["item"]
    score_col = schema["score"]
    label_col = schema["label"]

    # The query is the query group; undeclared, the sentence is exactly what
    # it always was. With `occasion` declared it gains the occasion columns —
    # without them the report's own definition would name a unit no metric on
    # the page is computed in (#428).
    occasion_str = "×".join(schema.get("occasion", []))
    query_str = f"{time_col} × {entity_str}" + (
        f" × {occasion_str}" if occasion_str else ""
    )
    description = (
        f"一個 query＝一組（{query_str}）。query 內的候選 "
        f"{item_col} 依模型分數 {score_col} 由高到低排名；{label_col}=1 的是"
        f"正例。下面每一個數字都是「這個 per-query 排序結果」加總到不同粒度——"
        "同一個量，換一種切法。"
    )
    formula = (
        "AP@k = (1 / R) · Σ(i=1..k) rel_i · P@i\n"
        "  P@i = 前 i 名中的正例數 / i　（前 i 名的精確率）\n"
        "  rel_i = 第 i 名是正例則 1、否則 0\n"
        "  R = 該 query 的正例總數（total_rel）；分母是 R、不是 min(k, R)——"
        "k < R 時 AP@k 追不到 1（頂多 k 個正例能進前 k、卻除以較大的 R）"
    )
    bullets = [
        f"例：某 query 有 4 個候選 {item_col}、R=2 個正例，排名後正例落在第 1、"
        "第 3 名。",
        "P@1 = 1/1 = 1.0、P@2 = 1/2 = 0.5、P@3 = 2/3 ≈ 0.667。",
        "AP@3 = (1/2)·(1·1.0 + 0·0.5 + 1·0.667) ≈ 0.83；"
        "AP@1 = (1/2)·(1·1.0) = 0.50——分母固定為 R=2，k=1 只納入第 1 名那個"
        "正例的貢獻、仍除以 2。",
        "手算核對點：k=1 時每個 query 的 AP@1 = rel_1 / R，正好等於 recall@1，"
        "所以整份報表的 overall map@1 會等於 recall@1（衡量指標段可對照）。",
        "地圖（下面每區＝這個 per-query AP 加總到不同粒度）："
        "overall＝跨 query 等權平均；per-item＝把 AP 歸因到正例所屬的 "
        f"{item_col} 後 item 等權（macro）；per-segment＝依 segment 分組平均；"
        "per-item 細部拆解＝同一批排名的分數／名次分布側面。",
    ]
    return ReportSection(
        title="核心概念 — 一個 query 的排序",
        description=description,
        formula=formula,
        bullets=bullets,
    )
