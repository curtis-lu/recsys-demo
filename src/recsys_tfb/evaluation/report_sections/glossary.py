"""詞彙表: what each metric means. The comparison report shows it too."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.evaluation.report import ReportSection

#: The results this section reads, by catalog name (ADR-0031 principle 1).
READS = ()


_GLOSSARY = [
    ("mAP@k", "per-query Average Precision@k 對 query 平均；主指標"),
    ("recall@k (per-item)",
     "P(rank(P)≤k | P 為正)，命中事件等權；map_attr@k 的互補角度"
     "（正例有沒有進 top-k），不下 pass/fail"),
    ("precision@k", "per-query 命中數/k；k=產品數時退化為 base rate"),
    ("map_attr@k",
     "某產品為正解時 ap_contrib@k 的平均（＝mAP@k 拆到單一產品的貢獻，非該"
     "產品自己的 mAP@k）。ap_contrib@k：該正例產品排名 r 若 ≤k 則為 P@r（前 r "
     "名精確率）、否則 0——就是核心概念 AP@k 分子 Σ rel_i·P@i 裡屬於這個產品的"
     "那一項。客戶該買它、模型排越前 → 值越高"),
    ("mean_pos", "產品為正時平均排名位置（越小越好）"),
    ("Macro 平均",
     "對所有產品（或 segment）等權平均；與 query 等權的 overall 不同"),
    ("base rate", "母體正樣本率"),
    ("macro per-item mAP",
     "各 item 的 map_attr 等權平均；本框架頭號指標（item 等權），與 query "
     "等權的 overall mAP 是兩種加權、並列不比高下"),
    ("正例佔比",
     "某 item 的正例數 ÷ 全體正例數；密集候選下與正例數同序（僅換分母／讀法）"),
    ("item share by rank",
     "rank 計數矩陣逐欄正規化——某 rank 位置上各 item 佔的比例（每欄加總=1），"
     "回答「誰佔據該名次」"),
]


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    tbl = pd.DataFrame(_GLOSSARY, columns=["指標", "語意"])
    return ReportSection(
        title="詞彙表 Glossary",
        description="指標語意，詳見 docs/metrics_concept_map.html。",
        tables=[tbl],
        table_titles=["指標語意"],
    )
