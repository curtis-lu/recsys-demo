"""Report section assembly. One pure function per section; no Spark.

Each builder takes the small aggregated metrics dict (from
metrics_spark.compute_all_metrics) + parameters and returns a ReportSection
(or None when its config toggle is off). assemble_report wires the enabled
sections into the final HTML.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from recsys_tfb.core.consistency import (
    EVALUATION_REPORT_SECTIONS,
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
    resolved_zero_positive_group_ratio,
)
from recsys_tfb.core.date_ranges import as_date_list
from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.baselines import resolve_lookback_months
from recsys_tfb.evaluation.metrics import metric_params, resolved_all_k
from recsys_tfb.evaluation.report import ReportSection, generate_html_report
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT


def resolve_display_k(raw_k: list, all_k: int) -> list:
    """Map mixed int/'all' display k list to concrete column suffixes.

    Returns labels as strings/ints that are used both as dict keys and for
    metric lookups. 'all' is looked up at ``all_k`` (``_k_to_lookup``) but is
    kept as the label 'all' for display.

    ``all_k`` is the bundle's ``metrics.resolved_all_k``: the K its ``"all"``
    values are stored at, which is the item count unless the frame held one
    row per event (then the widest query group, #434).

    Filters out any int K > all_k (bug 8, ADR-0020): with e.g. 3 product
    categories, precision@K's denominator is K itself (not min(K, n_items)),
    so @4/@5 declined purely because the denominator grew, not because the
    model did anything different — those columns aren't wrong, they're
    meaningless. The bound is the longest list, and that is ``all_k``: with
    ``event`` rows a 30-row group gives @13 real rows to rank although there
    are 12 items. This is a filter over the given list, not a regenerated
    one: a caller list without "all" does not gain one, and "all" is always
    kept. When ``all_k <= 0`` (unknown — e.g. baseline's slim per_item
    bundle has no dataset_overview) skip the filter entirely, since
    filtering by an unknown bound would collapse every table down to just
    "@all".
    """
    out = []
    for k in raw_k:
        if isinstance(k, str) and k.lower() == "all":
            out.append("all")
        else:
            out.append(int(k))
    return [k for k in out if not _k_exceeds_all_k(k, all_k)]


def _k_exceeds_all_k(k: int | str, all_k: int) -> bool:
    """True for an int K above ``all_k`` — the one bug 8 rule (ADR-0020).

    Shared by the display-list filter (``resolve_display_k``) and the
    metric-key filter (``drop_metric_keys_above_all_k``), so the two
    cannot disagree about which columns and rows a grain drops. ``"all"``
    never exceeds, and ``all_k <= 0`` (unknown) filters nothing.
    """
    if all_k <= 0 or k == "all":
        return False
    return int(k) > all_k


def drop_metric_keys_above_all_k(keys, all_k: int) -> list:
    """``keys`` minus those whose ``@K`` suffix is an int K above ``all_k`` (bug 8).

    For tables that print every computed metric key as a row (the comparison
    report's overall and category-overall tables), where there is no display
    K list to filter. Same rule and reason as ``resolve_display_k``: past
    the longest list precision@K keeps falling only because its denominator
    is K, so those rows are meaningless rather than wrong — nothing flags
    them. K == all_k stays (it is the ``"all"`` row), keys without an
    ``@<int>`` suffix stay, order is kept, and ``all_k <= 0`` keeps every key.
    """
    out = []
    for key in keys:
        _, sep, suffix = str(key).rpartition("@")
        if sep and suffix.isdigit() and _k_exceeds_all_k(int(suffix), all_k):
            continue
        out.append(key)
    return out


def _k_to_lookup(k, all_k: int) -> int | str:
    """Convert display label to metric dict key suffix.

    ``all_k`` comes from the bundle being read (``metrics.resolved_all_k``),
    never from counting its items (#434).
    """
    if k == "all":
        return all_k
    return k


#: K columns of every metrics-section table, before the bug 8 clamp.
_METRICS_SECTION_K = (1, 2, 3, 4, 5, "all")


def _metrics_section_ks(all_k: int) -> list:
    """The K columns the metrics section's tables actually show at this grain.

    One derivation for those tables and for the two CI notes that point into
    them (``build_overview_section``'s and ``build_metrics_section``'s). A
    note naming ``map_attr@{metric.k}`` is only true while that column
    survives the ``resolve_display_k`` clamp; with a second copy of the list
    the note could keep naming a column the table dropped, and nothing would
    raise — the reader just gets pointed at a column that is not there.
    """
    return resolve_display_k(list(_METRICS_SECTION_K), all_k)


_MACRO_LABEL = "Macro 平均"


def _report_cfg(parameters: dict) -> dict:
    return (parameters.get("evaluation", {}) or {}).get("report", {}) or {}


def _section_on(parameters: dict, name: str) -> bool:
    """Whether report section ``name`` is switched on (default: on).

    Pre-check: ``name`` is in ``EVALUATION_REPORT_SECTIONS``, the set A34 keeps
    equal to what the conf declares. A name outside it would read a key no conf
    declares and quietly default to on, the way ``diagnosis_links`` did.
    """
    if name not in EVALUATION_REPORT_SECTIONS:
        raise ValueError(
            f"_section_on: {name!r} is not in "
            f"core.consistency.EVALUATION_REPORT_SECTIONS "
            f"{sorted(EVALUATION_REPORT_SECTIONS)}. Add it there and declare "
            f"it under evaluation.report.sections (A34)."
        )
    sections = _report_cfg(parameters).get("sections", {}) or {}
    return bool(sections.get(name, True))


#: ``evaluation_results.json`` keys renamed by #327, ``old -> new``. The
#: framework stopped spelling its own landed keys in the example deployment's
#: business vocabulary; ``by_item`` next to ``n_products`` was one dict with two
#: naming schemes. ``by_snap_date`` / ``n_snap_dates`` are deliberately NOT in
#: here — the time vocabulary is kept on purpose (ADR-0017).
RENAMED_OVERVIEW_KEYS = {
    "n_products": "n_items",
    "n_customers": "n_entities",
    "avg_positives_per_customer": "avg_positives_per_entity",
}

#: ``dataset_overview`` sub-dicts whose *values* are per-key cells carrying
#: renameable keys (``totals`` is one cell, not a map of them).
OVERVIEW_CELL_GROUPS = ("by_snap_date", "by_item", "by_segment")

#: Repo-relative path named in the refusal message below. A reader who hits it
#: needs the fix, not just the diagnosis.
#:
#: The two constants above are public because that script imports them: the
#: detector and the migrator have to agree on the same list or a file can be
#: refused by one and left alone by the other.
MIGRATION_SCRIPT = "scripts/migrate_evaluation_results_keys.py"


def _dataset_overview(metrics: dict) -> dict:
    """``metrics["dataset_overview"]``, refusing a pre-#327 payload.

    Every read of the overview — this module's and
    ``evaluation/comparison/report.py``'s — goes through here, so the old shape
    is detected once instead of at each ``.get`` that would otherwise shrug and
    return its default.

    **Why refuse rather than fall back.** The old key names are gone, not
    deprecated: a dual read would be a permanent compatibility layer for a
    spelling this repo no longer produces, and the next reader would have to
    work out which of the two is real. The failure a fallback would be hiding is
    the one worth failing on: an item count read as ``0`` makes every
    ``"all"`` lookup ask for ``map@0`` (``metrics.resolved_all_k`` falls back
    to that count), every metric lookup misses, and a cross-version
    comparison renders as a full table of blanks that reads like "the model
    scored nothing" rather than "this file is old".

    An overview with *neither* spelling is left alone — a slim metrics bundle
    legitimately carries no ``dataset_overview`` at all, and the baseline
    bundles are exactly that shape.
    """
    overview = metrics.get("dataset_overview", {}) or {}
    cells = [overview.get("totals", {}) or {}]
    for group in OVERVIEW_CELL_GROUPS:
        cells.extend(
            cell for cell in (overview.get(group, {}) or {}).values()
            if isinstance(cell, dict)
        )
    found = sorted({
        old for cell in cells for old in RENAMED_OVERVIEW_KEYS if old in cell
    })
    if found:
        renames = ", ".join(f"{old} -> {RENAMED_OVERVIEW_KEYS[old]}" for old in found)
        raise ValueError(
            f"evaluation_results.json predates the #327 key rename: "
            f"dataset_overview still carries {found}. Migrate the file in "
            f"place with `PYTHONPATH=src .venv/bin/python {MIGRATION_SCRIPT} "
            f"<path-or-dir> --apply` ({renames}), then re-run. Reading it as-is "
            f"would report 0 items and render a report of blanks."
        )
    return overview


def count_items(metrics: dict) -> int:
    """``dataset_overview.totals.n_items`` of a metrics bundle, ``0`` when absent.

    ``0`` means "unknown", not "no items": a slim baseline bundle carries no
    overview. It is the item count the reports show — in the macro-coverage
    titles and the comparison report's metadata. It is not the K ``"all"`` is looked up at — that is ``metrics.resolved_all_k``,
    which falls back to this same number only when the bundle records no
    ``all_k`` (#434).
    """
    return int((_dataset_overview(metrics).get("totals", {}) or {}).get("n_items", 0))


def _macro_item_coverage(per_item: dict, parameters: dict) -> int:
    """Items in ``per_item`` that actually enter a per-item macro average.

    bug 5 (ADR-0020): ``per_item`` already excludes items with zero
    positives this period (they never had a row to aggregate), and
    ``evaluation.metric.min_positives`` (read through ``metric_params``, the
    shared reader of that block) can exclude further ones. Both are silent —
    the macro's denominator drifts month to month with no visible cause. This
    is disclosure, not a definition change: it counts the same set
    ``macro_average`` uses, it does not alter what the Macro row's values are.
    """
    min_positives = metric_params(parameters)["min_positives"]
    return sum(
        1 for m in per_item.values()
        if (m or {}).get("n_pos", 0) >= min_positives
    )


def macro_coverage_suffix(
    per_item: dict, n_items: int, parameters: dict, macro: dict | None
) -> str:
    """Title suffix disclosing a single-sided per-item macro's coverage (bug 5).

    Returns ``""`` unless ``macro`` is truthy — the same test
    ``_per_item_metric_table`` uses (``if macro_metrics:``) to prepend the
    Macro row. The suffix describes that row's denominator, so it has to
    appear exactly when the row does. ``macro_coverage_suffix_mb`` tests
    ``is not None`` instead, matching its own table; using one helper on the
    other's table puts the suffix on a table without a Macro row (or drops it
    from one that has it) for an empty-dict macro, and nothing raises.
    """
    if not macro:
        return ""
    n = _macro_item_coverage(per_item, parameters)
    return f"（參與 macro 的 item 數 {n}／全部 {n_items}）"


def macro_coverage_suffix_mb(
    per_item_a: dict,
    per_item_b: dict,
    n_items: int,
    parameters: dict,
    macro_a: dict | None,
    macro_b: dict | None,
) -> str:
    """Title suffix disclosing both sides' macro coverage on an M/B/Δ table (bug 5).

    Returns ``""`` unless both ``macro_a`` and ``macro_b`` are not ``None`` —
    the same condition ``per_item_metric_compare_table`` uses to add the
    Macro row (an empty dict counts as present there, and so here).

    Generic "M"/"B" labels (not "Model"/"Baseline") so the same helper reads
    correctly whether the two sides are Model/Baseline (main report) or two
    compared model versions (comparison report) — matching the tables'
    existing "(M/B/Δ)" column convention. Both reports build these titles
    through this one function, so the condition and the wording cannot drift
    between copies.
    """
    if macro_a is None or macro_b is None:
        return ""
    n_a = _macro_item_coverage(per_item_a, parameters)
    n_b = _macro_item_coverage(per_item_b, parameters)
    return f"（參與 macro 的 item 數 M {n_a}／B {n_b}／全部 {n_items}）"


def build_overview_section(
    metrics: dict, parameters: dict, metric_ci: dict | None = None,
    prediction_quality_shown: bool = False,
) -> ReportSection:
    """概覽（定向）：這份報表回答什麼、規模／分母、關鍵數、往哪找。

    ``prediction_quality_shown`` adds the navigation row for that section;
    ``assemble_report`` passes whether it built one, so the row never points
    at a section the report left out.

    presentation §一.1：規模／歸一化分母與嚴重度訊號分開標——分母混進關鍵數
    表會被讀成好壞。頭號指標＝macro per-item mAP（item 等權，＋CI 抽樣估計）；
    overall per-query mAP 並列為「另一種加權」，不宣稱哪個才對（不變量 4）。
    """
    overall = metrics.get("overall", {})
    disp = _report_cfg(parameters).get("display", {}) or {}
    all_k = resolved_all_k(metrics)
    ks = resolve_display_k(disp.get("primary_map_k", [1, 3, 5, "all"]), all_k)

    tables: list[pd.DataFrame] = []
    titles: list[str] = []

    # 關鍵指標 1：macro per-item mAP（頭號，含 bootstrap CI）——沿用 metric_ci
    # 的讀法（macro / sample），避免定義漂移。
    ci_note = ""
    if metric_ci and metric_ci.get("enabled") and metric_ci.get("macro"):
        m = metric_ci["macro"]
        sample_meta = metric_ci.get("sample", {}) or {}
        tables.append(pd.DataFrame(
            [{"AP（點估）": m.get("ap"), "CI 2.5%": m.get("ci_low"),
              "CI 97.5%": m.get("ci_high"),
              "CI 用 query 數": sample_meta.get("n_queries_sampled")}],
            index=["macro per-item mAP"],
        ))
        titles.append("頭號指標：macro per-item mAP（item 等權，含 bootstrap CI）")
        n_boot = metric_ci.get("n_boot")
        sd = sample_meta.get("sampling_description", "")
        # Truncation of point estimate and CI follows metric.k (ADR-0020
        # design H) instead of a hard-coded @all. Name the matching column
        # only when the metrics section actually shows it.
        mk = metric_params(parameters)["k"]
        if mk is None:
            trunc_note = (
                "點估 AP 與 CI 都不截斷（metric.k 未設），與衡量指標的全量 macro "
                "map_attr@all 同一定義。"
            )
        elif mk in _metrics_section_ks(all_k):
            trunc_note = (
                f"點估 AP 與 CI 都截斷在 {mk}（metric.k），與衡量指標的全量 macro "
                f"map_attr@{mk} 同一定義。"
            )
        else:
            trunc_note = (
                f"點估 AP 與 CI 都截斷在 {mk}（metric.k）；衡量指標各表不顯示 "
                f"@{mk} 欄。"
            )
        ci_note = (
            f"　CI 為 cluster bootstrap（cluster＝客戶，B＝{n_boot}）在診斷母體上"
            f"重抽得到；{sd}{trunc_note}"
        )

    # 關鍵指標 2：overall per-query mAP@k（另一種加權，並列不比高下）
    card = {
        f"map@{k}": overall.get(f"map@{_k_to_lookup(k, all_k)}") for k in ks
    }
    t_overall = pd.DataFrame([card]).T
    t_overall.columns = ["value"]
    tables.append(t_overall)
    titles.append("overall mAP@k（per-query 等權，另一種加權）")

    # 規模／分母（非好壞，明標與關鍵數分開）
    # 「每 X 平均正例數」的 X 印使用者自己的 entity 欄，不寫死「客戶」——entity
    # 是門市時，「每客戶平均」要讀者在腦中翻譯一次才看得懂這個數字在數什麼
    # （#327）。欄名一律走 core.schema.get_schema，理由同核心概念那一段。
    totals = _dataset_overview(metrics).get("totals", {}) or {}
    entity_str = "×".join(get_schema(parameters)["entity"])
    # bug 3 (ADR-0020): n_queries is compute_dataset_overview's count of all
    # queries before filtering, but the old label called it "queries with a
    # positive" — contradicting the excluded-queries row right below (a
    # million queries with a positive cannot coexist with 950k excluded).
    # Fix the label, and add the row the report never gave anywhere:
    # queries with a positive = n_queries - n_excluded_queries.
    scale = {
        "全部 query 數 n_queries": metrics.get("n_queries"),
        "有正例的 query 數": _od(
            metrics.get("n_queries"), metrics.get("n_excluded_queries")
        ),
        "排除 query 數 n_excluded_queries": metrics.get("n_excluded_queries"),
        "正例列數 n_positives": totals.get("n_positives"),
        "母體正樣本率（÷全體候選列）": totals.get("positive_rate"),
        f"每 {entity_str} 平均正例數 avg_positives_per_entity":
            totals.get("avg_positives_per_entity"),
    }
    t_scale = pd.DataFrame([scale]).T
    t_scale.columns = ["value"]
    tables.append(t_scale)
    titles.append("規模／分母（以下為分母與規模，非好壞）")

    # 導覽：想回答什麼 → 看哪一區（RangeIndex → render 端自動藏流水號）
    nav = pd.DataFrame({
        "想回答的問題": [
            "模型整體排得好不好",
            "哪些 item／segment 排得弱",
            "每個 item 的分數與名次分布長怎樣",
            "跟熱門度（popularity）比如何",
            "想深入各項診斷（排序偏移、item 能力、壓制帳本…）",
            "本次量到什麼、沒量到什麼",
        ],
        "看哪一區": [
            "衡量指標",
            "衡量指標（per-item／per-segment）",
            "per-item 細部拆解",
            "baseline",
            "排序診斷（獨立報表）",
            "完整性檢查",
        ],
    })
    if prediction_quality_shown:
        nav.loc[len(nav)] = [
            "把每一列候選當二元預測：門檻切在哪、precision／recall 多少",
            "預測品質",
        ]
    tables.append(nav)
    titles.append("導覽：想回答什麼 → 看哪一區")

    return ReportSection(
        title="概覽",
        description=(
            "這份報表幫你判斷這個模型在 per-query 排序上表現如何、好壞落在哪些 "
            "item／segment、以及相對 popularity baseline 的位置。以下攤開多個粒度"
            "與角度，判斷留給你。頭號指標為 macro per-item mAP（item 等權）；"
            "overall mAP 為 per-query 等權，是另一種加權，並列呈現、不比高下。"
            + ci_note
        ),
        tables=tables,
        table_titles=titles,
    )


def build_core_concept_section(parameters: dict) -> ReportSection:
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


def _kept_zero_positive_groups_note(metrics: dict, parameters: dict) -> str:
    """How many test query groups holding no positive this run evaluated, and
    at what ratio they were kept — empty unless ``--post-training`` with
    ``dataset.test_zero_positive_group_ratio`` above 0 (ADR-0025 decision 3).

    The count is ``n_excluded_queries``: the ranking metrics skip every query
    group without a positive, and under that mode every such group in the data
    is one the dataset draw kept. Printed because ``1 / r`` is a design
    weight: few kept groups means the weighted numbers are not stable, and the
    reader should see the count next to them.
    """
    if not parameters.get("post_training"):
        return ""
    ratio = resolved_zero_positive_group_ratio(parameters, "test")
    if ratio <= 0.0:
        return ""
    n_kept = metrics.get("n_excluded_queries")
    kept = f"本次評估資料裡有 {n_kept:,} 個" if n_kept is not None else "本次評估資料裡有一些"
    return (
        f"test 表保留了比例 r＝{ratio:g} 的無正例 query group"
        f"（dataset.test_zero_positive_group_ratio）：{kept}，每列帶權重 "
        f"1／r＝{1 / ratio:.4g}（{ZERO_POSITIVE_GROUP_WEIGHT_COL}）。1／r 是設計"
        "權重，不是無偏估計：留下的組少時，加權後的比值型指標不穩。"
    )


def build_dataset_overview_section(
    metrics: dict, parameters: dict
) -> ReportSection | None:
    if not _section_on(parameters, "dataset_overview"):
        return None
    ov = _dataset_overview(metrics)
    totals_d = ov.get("totals", {}) or {}
    totals = pd.DataFrame([totals_d]).T
    totals.columns = ["value"]
    by_snap = pd.DataFrame(ov.get("by_snap_date", {})).T

    # per-item 正例組成：正例數 / 正樣本率 / 正例佔比（＝n_positives÷總正例，
    # render 端純算術、無 Spark）。密集候選下三欄同序，ScopeNote 於 description。
    total_pos = totals_d.get("n_positives") or 0
    by_item_rows: dict = {}
    # per-item 列序統一按字母（與衡量指標、item-share 對齊）
    for item, d in sorted((ov.get("by_item", {}) or {}).items()):
        n_pos = d.get("n_positives")
        by_item_rows[item] = {
            "正例數": n_pos,
            "正樣本率(÷此item候選列)": d.get("positive_rate"),
            "正例佔比": (n_pos / total_pos)
            if (n_pos is not None and total_pos) else None,
        }
    by_item = pd.DataFrame(by_item_rows).T

    tables = [totals, by_snap, by_item]
    titles = ["整體 totals", "各期 by snap_date", "per-item 正例組成"]
    collapsed = [False, True, False]   # 各期單 snap 時＝totals，預設收合
    # per-segment 正例組成：正例數／正樣本率（÷該 segment 候選列）／query 數佔比。
    # 第 3 欄用「query 數佔比」（該 segment 佔多少 query，反映 segment 大小）——
    # segment 分的是 query、per-item 分的是正例，兩者不同軸，故不與 per-item 的
    # 「正例佔比」互換。by_segment 由 compute_dataset_overview 依 active_seg_col
    # 聚合；缺席（舊 artifact 或無 segment 欄）則不呈現此表。對不到值的
    # (unmatched) 排最後：它不是一個 segment，列出來是為了交代它有多少 query。
    by_seg = ov.get("by_segment", {}) or {}
    if by_seg:
        seg_rows = {}
        for seg, d in _unmatched_last(dict(sorted(by_seg.items()))).items():
            seg_rows[seg] = {
                "正例數": d.get("n_positives"),
                "候選列數": d.get("n_rows"),
                "正樣本率(÷此segment候選列)": d.get("positive_rate"),
                "query 數": d.get("n_queries"),
                "query 數佔比": d.get("query_share"),
            }
        tables.append(pd.DataFrame(seg_rows).T)
        titles.append("per-segment 正例組成")
        collapsed.append(False)
    cat = metrics.get("category")
    if cat:
        cat_by_item = _dataset_overview(cat).get("by_item", {})
        if cat_by_item:
            tables.append(pd.DataFrame(cat_by_item).T)
            titles.append("by 大類（大類粒度，不與整體相加）")
            collapsed.append(False)   # 大類表預設展開（與 totals/by_item/by_segment 一致）

    return ReportSection(
        title="基本統計 — 資料集",
        description=(
            "整體規模與 per-item 的正例組成。per-item 三欄（正例數／正樣本率／"
            "正例佔比）在密集候選集下同序（僅換分母或讀法），非三個獨立軸；候選"
            "覆蓋率因每 item 覆蓋全部 query 恆為 100%，故不列。注意「正樣本率」有"
            "兩種分母：概覽的母體 positive_rate 除以全體候選列（此 run 5,232），"
            "此處 per-item 那欄除以「該 item 自己的候選列數」（此 run 每 item 654），"
            "量級不同。「by 大類」是大類粒度（每客戶每大類一列、label＝該大類任一"
            "子產品為正例、大類分數＝子產品最佳分數），故其正例數 ≤ item 粒度合計、"
            "不與整體 n_positives 相加。per-segment 正例組成：正例數、候選列數、"
            "正樣本率、query 數與 query 數佔比（該 segment 佔多少 query）。每-query "
            "正例數分佈為後續階段。"
            + (
                " " + note + "上面各項總數（列數、item 數、正樣本率）都算進了"
                "這些組，是未加權的計數——母體變大的如實反映，不是 regression。"
                if (note := _kept_zero_positive_groups_note(metrics, parameters))
                else ""
            )
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
        bullets=_segment_notes(metrics.get("segments"), by_seg),
    )


def _unmatched_last(by_segment: dict) -> dict:
    """同一份 dict，(unmatched) 移到最後、其餘順序不動。

    它不是一個 segment，列出來是為了交代它有多少 query；三處分群表（基本統計、
    衡量指標、baseline 對照）用同一個順序，讀者才不會在某張表把它當第一個群。
    """
    rest = {k: v for k, v in by_segment.items() if k != UNMATCHED_SEGMENT}
    if UNMATCHED_SEGMENT in by_segment:
        rest[UNMATCHED_SEGMENT] = by_segment[UNMATCHED_SEGMENT]
    return rest


def _segment_notes(segments: dict | None, by_segment: dict) -> list[str]:
    """讀分群表前要知道的事：欄取自哪張表、哪張表缺欄、(unmatched) 怎麼算。

    ``segments`` 是 ``compute_metrics`` 從 ``evaluation_segment_columns`` 帶來的
    ``joined``／``sources``／``missing``（ADR-0020 bug 6）。缺欄時分群表根本不
    出現，這裡是唯一交代「為什麼沒有」的地方，所以表名與欄名都要印——設定
    打錯欄名也會落到這裡。沒有 ``segments`` 的 metrics（這個機制之前寫的、
    比較報表算的）不寫。
    """
    if not segments:
        return []
    notes = []
    joined = segments.get("joined") or []
    sources = segments.get("sources") or {}
    if joined:
        notes.append(
            "分群欄："
            + "、".join(f"{c} 取自 {sources[c]}" for c in joined)
            + f"；per-segment 表以 {joined[0]} 分群。"
        )
    for col, table in (segments.get("missing") or {}).items():
        notes.append(f"母體表 {table} 無欄 {col}：這一欄本次不算 per-segment。")
    if UNMATCHED_SEGMENT in by_segment:
        notes.append(
            f"{UNMATCHED_SEGMENT}＝母體表有這一欄、但這些 query 在上面沒有值。"
            "表上列出它的 query 數與佔比，但它不進 macro 平均（per-segment "
            "指標表的 Macro 列不含它）。"
        )
    return notes


def _per_item_metric_table(
    per_item: dict,
    ks: list,
    all_k: int,
    metric_key: str,
    col_fmt: str,
    extra_cols: dict[str, str] | None = None,
    macro_metrics: dict | None = None,
) -> pd.DataFrame:
    """Rows = items; one column per k named ``col_fmt.format(k=k)``, value
    pulled from ``per_item[item][f"{metric_key}@{_k_to_lookup(k, all_k)}"]``.

    ``extra_cols`` maps an output column name to a flat (non-@k) per_item key,
    e.g. ``{"mean_pos": "mean_pos"}``.

    ``macro_metrics``: when given and non-empty, an equal-weight-average
    metrics dict (same key shape as a per_item value) is prepended as the
    top row labelled ``_MACRO_LABEL``.
    """
    def _row(m: dict) -> dict:
        row = {
            col_fmt.format(k=k): m.get(f"{metric_key}@{_k_to_lookup(k, all_k)}")
            for k in ks
        }
        for out_name, src_key in (extra_cols or {}).items():
            row[out_name] = m.get(src_key)
        return row

    data: dict = {}
    if macro_metrics:
        data[_MACRO_LABEL] = _row(macro_metrics)
    for item, m in per_item.items():
        data[item] = _row(m)
    return pd.DataFrame(data).T


def per_item_metric_compare_table(
    per_item_a: dict,
    per_item_b: dict,
    per_item_delta: dict,
    ks: list,
    all_k: int,
    metric_key: str,
    col_base_fmt: str,
    macro_a: dict | None = None,
    macro_b: dict | None = None,
    *,
    all_k_b: int,
) -> pd.DataFrame:
    """Per-item table with Model/Baseline/Δ interleaved per k.

    Rows = items (Macro 平均 prepended when BOTH macro_a and macro_b are
    given). Columns = ``f"{base} M"``, ``f"{base} B"``, ``f"{base} Δ"`` for
    each ``k``, where ``base = col_base_fmt.format(k=k)``.

    Δ for item rows is read from ``per_item_delta`` (already computed
    upstream by build_comparison_result); Δ for the Macro row is computed
    here as ``macro_a − macro_b`` since macro values aren't part of the
    per-item delta dict. Either way a Δ exists only where both sides have the
    value; otherwise the cell is blank (ADR-0020 bug 4).

    ``all_k`` / ``all_k_b``: the K each side's ``"all"`` is stored at
    (``metrics.resolved_all_k`` of that side's bundle). ``all_k_b`` has no
    default on purpose: a default of "A's K" is exactly the silent wrong
    lookup #434 fixed. The baseline passes the model's K (it is scored on the
    model's own rows); two compared model versions each keep their own rows
    in the common universe, so with ``event`` their widest groups can differ.
    Then the two ``"all"`` cells are different keys, ``per_item_delta``
    (keyed per key) holds neither pairing, and the Δ is taken here as
    ``a − b`` — both are the untruncated value.
    """
    def _row(m_a: dict, m_b: dict, m_d: dict | None) -> dict:
        row: dict = {}
        for k in ks:
            key = f"{metric_key}@{_k_to_lookup(k, all_k)}"
            key_b = f"{metric_key}@{_k_to_lookup(k, all_k_b)}"
            base = col_base_fmt.format(k=k)
            a = m_a.get(key)
            b = m_b.get(key_b)
            if m_d is not None and key == key_b:
                d = m_d.get(key)
            else:
                # Both sides or no Δ (ADR-0020 bug 4): reading a missing side
                # as 0.0 printed the other side's value as the Δ.
                d = a - b if a is not None and b is not None else None
            row[f"{base} M"] = a
            row[f"{base} B"] = b
            row[f"{base} Δ"] = d
        return row

    data: dict = {}
    if macro_a is not None and macro_b is not None:
        data[_MACRO_LABEL] = _row(macro_a, macro_b, None)
    all_items = list(per_item_a.keys()) + [
        i for i in per_item_b.keys() if i not in per_item_a
    ]
    for item in all_items:
        data[item] = _row(
            per_item_a.get(item, {}),
            per_item_b.get(item, {}),
            per_item_delta.get(item, {}),
        )
    return pd.DataFrame(data).T


def _per_item_recall_table(
    per_item: dict, ks: list, all_k: int, macro_metrics: dict | None = None
) -> pd.DataFrame:
    """Rows = items; bare ``@k`` cols (from hit_rate@k) + mean_pos.

    欄名裸 @k、family（recall）在呼叫端的表標題交代——與 A 塊 per-query 表
    及 per-item map_attr 表的欄名慣例一致（family 不重複塞進欄名）。
    """
    return _per_item_metric_table(
        per_item, ks, all_k, "hit_rate", "@{k}",
        extra_cols={"mean_pos": "mean_pos"}, macro_metrics=macro_metrics,
    )


def _families_by_k_table(overall: dict, ks: list, all_k: int) -> pd.DataFrame:
    """單一 per-query aggregate → rows=[map, precision, recall]、cols=@k。

    給「單一彙總」用（overall、大類 overall）：只有一個實體，故用指標家族當列。
    """
    rows = {}
    for fam in ("map", "precision", "recall"):
        rows[fam] = {f"@{k}": overall.get(f"{fam}@{_k_to_lookup(k, all_k)}")
                     for k in ks}
    return pd.DataFrame(rows).T


def _entities_by_k_table(
    per_entity: dict, macro: dict | None, ks: list, all_k: int, fam: str
) -> pd.DataFrame:
    """多實體 per-query → rows=實體（Macro 頂列）、cols=@k，單一 metric family。

    給「多實體拆分」用（per-segment）：拆成 map/precision/recall 各一張，實體當列。
    """
    src = {_MACRO_LABEL: macro, **per_entity} if macro else dict(per_entity)
    data = {}
    for ent, m in src.items():
        data[ent] = {f"@{k}": (m or {}).get(f"{fam}@{_k_to_lookup(k, all_k)}")
                     for k in ks}
    return pd.DataFrame(data).T


def build_metrics_section(
    metrics: dict, parameters: dict, metric_ci: dict | None = None
) -> ReportSection | None:
    """衡量指標：分兩塊、各塊內維度與方向一致。

    A｜per-query 指標（map/precision/recall）——overall、per-segment、大類 overall。
    B｜per-item 歸因（map_attr/recall）——per-item、大類 per-item。precision 是
    per-query 量（整組 top-k 的性質），無法歸因到單一 item，故 B 塊沒有 precision。
    方向規則：單一彙總用指標家族當列；多實體拆分用實體當列。所有表 k 欄統一＝
    [1,2,3,4,5,all]，每張表只一個 metric family（避免寬混表）。全克制、明細收合。
    """
    if not _section_on(parameters, "primary_map"):
        return None
    overall = metrics.get("overall", {})
    # per-item 列序全報表統一按字母（與 per-item 細部拆解的 item-share 表對齊）
    per_item = dict(sorted((metrics.get("per_item", {}) or {}).items()))
    macro_item = metrics.get("macro_avg", {}).get("by_item", {})
    n_items = count_items(metrics)
    all_k = resolved_all_k(metrics)
    ks = _metrics_section_ks(all_k)  # one K list for every table

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # 頭號指標：macro per-item mAP CI（可見，放最前）
    if metric_ci and metric_ci.get("enabled") and metric_ci.get("macro"):
        m = metric_ci["macro"]
        sm = metric_ci.get("sample", {}) or {}
        _add(
            pd.DataFrame(
                [{"AP（點估）": m.get("ap"), "CI 2.5%": m.get("ci_low"),
                  "CI 97.5%": m.get("ci_high"),
                  "CI 用 query 數": sm.get("n_queries_sampled")}],
                index=["macro per-item mAP"],
            ),
            "頭號指標：macro per-item mAP（item 等權，含 bootstrap CI）",
            False,
        )

    # ===== Block A：per-query 指標（map / precision / recall）=====
    _add(_families_by_k_table(overall, ks, all_k),
         "A · per-query｜overall（列＝map/precision/recall）", False)
    per_segment = _unmatched_last(metrics.get("per_segment", {}) or {})
    if per_segment:
        macro_seg = metrics.get("macro_avg", {}).get("by_segment", {})
        # (unmatched) 的定義貼在它出現的表上：列在最後、不在 Macro 列裡。
        unmatched = (f"；{UNMATCHED_SEGMENT} 不含在 Macro"
                     if UNMATCHED_SEGMENT in per_segment else "")
        for fam in ("map", "precision", "recall"):
            _add(_entities_by_k_table(per_segment, macro_seg, ks, all_k, fam),
                 f"A · per-query｜per-segment {fam}@k（列＝segment{unmatched}）",
                 True)
    cat = metrics.get("category")
    cks = None
    if cat:
        n_cat = count_items(cat)
        cat_k = resolved_all_k(cat)
        cks = _metrics_section_ks(cat_k)
        _add(_families_by_k_table(cat.get("overall", {}), cks, cat_k),
             "A · per-query｜大類 overall（列＝map/precision/recall）", True)

    # ===== Block B：per-item 歸因（map_attr / recall；無 precision）=====
    b_map = _per_item_metric_table(
        per_item, ks, all_k, "map_attr", "@{k}", macro_metrics=macro_item,
    )
    if metric_ci and metric_ci.get("enabled"):
        ci_items = metric_ci.get("per_item", {}) or {}
        ci_macro = metric_ci.get("macro") or {}

        def _ci_val(idx, field):
            src = ci_macro if idx == _MACRO_LABEL else ci_items.get(idx, {})
            return src.get(field)

        for col, field in (("CI 2.5%", "ci_low"), ("CI 97.5%", "ci_high"),
                           ("n_pos（CI 用）", "n_pos")):
            b_map[col] = [_ci_val(idx, field) for idx in b_map.index]
    # bug 5 (ADR-0020): the macro denominator is "items with a positive this
    # period" (and n_pos >= metric.min_positives); a zero-positive item drops
    # out of per_item silently. The title discloses N of M, only on a table
    # that has a Macro row.
    item_cov = macro_coverage_suffix(per_item, n_items, parameters, macro_item)
    _add(b_map, f"B · per-item 歸因｜map_attr@k（列＝item，＋CI 上下界）{item_cov}",
         True)
    _add(_per_item_recall_table(per_item, ks, all_k, macro_metrics=macro_item),
         f"B · per-item 歸因｜recall@k（列＝item）{item_cov}", True)
    if cat:
        cat_macro_item = cat.get("macro_avg", {}).get("by_item", {})
        cat_pi = dict(sorted((cat.get("per_item", {}) or {}).items()))
        cat_item_cov = macro_coverage_suffix(
            cat_pi, n_cat, parameters, cat_macro_item
        )
        _add(_per_item_metric_table(cat_pi, cks, cat_k, "map_attr",
                                    "@{k}", macro_metrics=cat_macro_item),
             f"B · 大類 per-item 歸因｜map_attr@k（列＝大類）{cat_item_cov}", True)
        _add(_per_item_recall_table(cat_pi, cks, cat_k,
                                    macro_metrics=cat_macro_item),
             f"B · 大類 per-item 歸因｜recall@k（列＝大類）{cat_item_cov}", True)

    # Which column the CI point estimate matches follows metric.k (ADR-0020
    # design H) instead of a hard-coded @all; a column is named only when
    # this section's tables (ks) actually show it.
    mk = metric_params(parameters)["k"]
    if mk is None:
        ci_point_note = (
            "CI 上下界的點估與該列 map_attr@all 同一定義，不截斷（metric.k 未設）"
        )
    elif mk in ks:
        ci_point_note = (
            f"CI 上下界的點估與該列 map_attr@{mk} 同一定義，截斷在 {mk}（metric.k）"
        )
    else:
        ci_point_note = (
            f"CI 上下界的點估截斷在 {mk}（metric.k），本段各表不顯示 @{mk} 欄"
        )
    return ReportSection(
        title="衡量指標",
        description=(
            "分兩塊、各塊內維度與方向一致。A｜per-query 指標（map／precision／"
            "recall）——對 overall、per-segment、大類 overall；precision@k 是"
            " per-query 量（整組 top-k 命中幾個），只在這塊。B｜per-item 歸因"
            "（map_attr／recall）——對 per-item、大類 per-item；precision 無法歸因"
            "到單一 item（那是整組 top-k 的性質），故 B 塊沒有 precision。方向規則："
            "單一彙總（overall、大類 overall）用指標家族當列，多實體拆分（per-"
            "segment、per-item、大類 per-item）用實體當列；所有表 k 欄一致＝"
            "[1,2,3,4,5,all]、每張表一個 metric family。頭號指標＝macro per-item "
            f"mAP（item 等權，含 bootstrap CI；{ci_point_note}）；"
            "overall per-query mAP 是另一種加權，並列不比高下。手算核對：overall "
            "map@1 = recall@1（見核心概念，AP@k 分母＝R）。K=產品數時 precision "
            "退化為 base rate、recall 恆為 1。CI 僅算到 item 層（大類 per-item 無 "
            "bootstrap CI）。per-item 列序統一按字母。明細表點標題展開。"
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )


#: The display bin table's columns, as the report prints them. The first six
#: describe the bin; the last three read the threshold sweep at its lower edge.
_PQ_BIN_COLUMNS = {
    "score_from": "分數下緣",
    "score_to": "分數上緣",
    "n": "列數",
    "n_pos": "正例數",
    "mean_score": "平均分數",
    "positive_rate": "實際正例率",
    "precision": "以下緣為門檻：precision",
    "recall": "以下緣為門檻：recall",
    "f1": "以下緣為門檻：F1",
}


def _pq_summary_row(summary: dict) -> dict:
    """One item's (or the whole data's) headline numbers, as table cells."""
    best = summary.get("best_f1") or {}
    return {
        "列數": summary.get("n"),
        "正例數": summary.get("n_pos"),
        "正例率": summary.get("positive_rate"),
        "pr_auc": summary.get("pr_auc"),
        "roc_auc": summary.get("roc_auc"),
        "F1 最佳門檻（分數 ≥）": best.get("threshold"),
        "該門檻的 precision": best.get("precision"),
        "該門檻的 recall": best.get("recall"),
        "該門檻的 F1": best.get("f1"),
    }


def _pq_threshold_figure(sweep: pd.DataFrame):
    """precision / recall / F1 against the threshold, one point per non-empty
    fine bin edge: the shape the user reads to pick a cut, which a 1000-row
    table would bury."""
    import plotly.graph_objects as go

    fig = go.Figure()
    for col, name in (("precision", "precision"), ("recall", "recall"),
                      ("f1", "F1")):
        fig.add_trace(go.Scatter(
            x=sweep["threshold"], y=sweep[col], mode="lines", name=name))
    fig.update_layout(
        title="整體：門檻 → precision／recall／F1（每個非空細箱的下緣一點）",
        xaxis_title="門檻（分數 ≥ 此值即預測為正）", yaxis_title="值",
        yaxis_range=[0, 1],
    )
    return fig


#: The section's title, shared by its empty and full forms.
_PQ_TITLE = "預測品質 — 把每一列候選當二元預測"


def build_prediction_quality_section(
    prediction_quality: dict | None, metrics: dict, parameters: dict
) -> ReportSection | None:
    """The prediction-quality section (ADR-0024): every candidate row as one
    binary prediction, binned by score.

    Every number here comes from the fine bin tables in the payload, through
    the same functions ``compute_prediction_quality`` used for the headline
    numbers (``evaluation/prediction_quality.py``), so the threshold sweep and
    the bin table cannot disagree. ``metrics`` is read for one thing: the
    ranking section's excluded query groups, so the population note can put
    the two populations side by side with numbers.

    Decision 7's three statements are always printed, each where the number
    it qualifies is: the bin width (the best-F1 threshold's resolution, not an
    online setting), the two populations, and ``pr_auc`` not being an average
    precision computed elsewhere. The population note has one form per run
    mode, and ``--post-training`` has two: with the zero-positive group
    weight in the payload the test table kept a share ``r`` of the groups
    without a positive, and the counts are weighted back up by ``1 / r``
    (ADR-0025 decision 3); without it the dataset pipeline dropped them all,
    so "every candidate row" would be false and, with nothing else excluded,
    the two sections' populations are the same. A46 refuses the second form
    at the CLI entry, so a fresh run no longer produces it; it stays for a
    payload computed before the weight existed.
    """
    if (not _section_on(parameters, "prediction_quality")
            or not prediction_quality
            or prediction_quality.get("enabled") is not True):
        return None
    from recsys_tfb.evaluation.diagnostics_spark import frame_from_json
    from recsys_tfb.evaluation.prediction_quality import (
        bins_of_item,
        coarse_bin_table,
        threshold_sweep,
    )

    bins_cfg = prediction_quality["bins"]
    lo, width = bins_cfg["lo"], bins_cfg["width"]
    n_bins, n_display = bins_cfg["n_bins"], bins_cfg["n_display_bins"]
    overall = prediction_quality["overall"]
    summary = overall.get("summary")
    if summary is None:
        return ReportSection(
            title=_PQ_TITLE,
            description="本次評估沒有任何候選列，這一段沒有東西可算。",
        )
    per_item = prediction_quality["per_item"]
    item_col = prediction_quality["columns"]["item"]
    n_rows = summary["n"]

    # --- population: this section vs the ranking section (decision 7) -----
    # One sentence per run mode, not a correction appended to the other: under
    # --post-training the test table arrives already filtered, so with nothing
    # excluded here the two sections read the same rows.
    n_excl = metrics.get("n_excluded_queries")
    ranking_rule = "主指標段（mAP／precision@K／recall@K）只算有正例的 query group"
    excluded = (f"，本次排除 {n_excl} 個（n_excluded_queries）"
                if n_excl is not None else "")
    weight_col = prediction_quality["columns"].get("weight")
    if parameters.get("post_training") and weight_col:
        population = (
            f"母體：本段算在本次評估的全部候選列上，本身不排除任何 query group；"
            "但這是 --post-training，test 表在 dataset 階段只留下有正例的 query "
            "group 與一部分沒有正例的（filter_test_model_input）。"
            + _kept_zero_positive_groups_note(metrics, parameters)
            + f"本段的列數與正例數都是乘上 {weight_col} 之後的加權數（共 "
            f"{n_rows:,}），代表還原到全部曝光的估計。{ranking_rule}{excluded}。"
            "兩段的母體不同，precision 不可互相比較。"
        )
    elif parameters.get("post_training"):
        same = n_excl == 0
        population = (
            f"母體：本段算在本次評估的全部 {n_rows:,} 列候選上，本身不排除任何 "
            "query group；但這是 --post-training，test 表在 dataset 階段已經丟掉"
            "沒有正例的 query group（filter_test_model_input），所以這些列只來自"
            "「有正例的 query group」，不是全部曝光。正例佔比因此比全部曝光高，"
            "precision 與 pr_auc 會比在全部曝光上算的大；要留下這類 query group，"
            f"設 dataset.test_zero_positive_group_ratio。{ranking_rule}{excluded}。"
            + ("兩段的母體相同，但 precision 的定義不同（見下方 per-item 那一條），"
               "仍不可互相比較。" if same else
               "兩段的母體不同，precision 不可互相比較。")
        )
    else:
        population = (
            f"母體：本段算在本次評估的全部 {n_rows:,} 列候選上，不排除任何 query "
            f"group。{ranking_rule}{excluded}。兩段的母體不同，precision 不可"
            "互相比較。"
        )

    card = pd.DataFrame([{
        **_pq_summary_row(summary),
        "細箱寬（門檻解析度）": width,
        "分數範圍（本次資料的最小～最大）": f"{lo:g} ～ {bins_cfg['hi']:g}",
    }]).T
    card.columns = ["value"]

    fine_bins = frame_from_json(overall["bins"])
    bin_table = coarse_bin_table(
        fine_bins, lo=lo, width=width, n_bins=n_bins, n_display_bins=n_display,
    ).rename(columns=_PQ_BIN_COLUMNS)

    tables = [card, bin_table]
    titles = [
        "整體：關鍵數",
        f"整體：分箱表（{n_display} 格等寬，每格＝{n_bins // n_display} 個細箱）",
    ]
    collapsed = [False, False]

    listed = per_item.get("listed") or []
    n_items = per_item.get("n_items")
    if listed:
        rows = {item: _pq_summary_row(per_item["summary"][item])
                for item in listed}
        tables.append(pd.DataFrame(rows).T)
        titles.append(
            f"per-item：關鍵數（列數最多的前 {len(listed)} 個 {item_col}，"
            f"共 {n_items} 個；其餘只算進整體）"
        )
        collapsed.append(False)
        item_bins = frame_from_json(per_item["bins"])
        long = []
        for item in listed:
            tbl = coarse_bin_table(
                bins_of_item(item_bins, item_col, item), lo=lo, width=width,
                n_bins=n_bins, n_display_bins=n_display,
            ).rename(columns=_PQ_BIN_COLUMNS)
            tbl.insert(0, item_col, item)
            long.append(tbl)
        tables.append(pd.concat(long, ignore_index=True))
        titles.append(f"per-item：分箱表（每個 {item_col} {n_display} 格，"
                      f"讀法同整體分箱表）")
        collapsed.append(True)

    figures = []
    sweep = threshold_sweep(fine_bins, lo=lo, width=width)
    if summary.get("n_pos"):
        figures.append(_pq_threshold_figure(sweep))

    return ReportSection(
        title=_PQ_TITLE,
        description=(
            "這一段不看同一個 query group 裡的名次，而是把每一列候選當成一次"
            "「會不會是正例」的預測：分數 ≥ 門檻就預測為正。回答的問題是「照分數"
            "切一刀，切在哪裡、precision 與 recall 各是多少」，以及每一段分數"
            "裡實際有多少正例。" + population
        ),
        formula=(
            "precision＝TP÷(TP+FP)　recall＝TP÷全部正例　F1＝2·P·R÷(P+R)"
            "　門檻只取細箱的下緣；箱內的列視為同分"
        ),
        bullets=[
            f"門檻解析度：分數範圍取本次資料的最小～最大，切成 {n_bins} 個等寬"
            f"細箱，細箱寬 {width:g}。門檻只算得到細箱的邊界，所以 F1 最佳門檻的"
            f"解析度就是這個寬度；它也是在這份資料上挑的，換一份資料（下個月、"
            f"線上）分數分布會變，不能直接搬去當線上的設定值。",
            "pr_auc＝把箱內的列視為同分之後的 average precision（Σ 每箱正例佔全部"
            "正例的比例 × 該箱下緣的 precision）；roc_auc＝箱內同分算一半的 ROC "
            "面積。兩者都是「分箱後分數」的精確值，但箱內的先後已經丟掉，不等於"
            "在原始分數上算的值——不能拿來跟外部工具（例如 sklearn）在原始分數上"
            "算的 average precision 或 ROC-AUC 直接比較。",
            "per-item 的 precision 分母是該 item 在門檻以上的列數；主指標段的 "
            "precision@K 分母是 K（每個 query group 的前 K 名），是不同的量。",
            "per-item 的 roc_auc 母體是該 item 的全部候選列，與排序診斷「item "
            "能力」頁的 AUC（只含有正例的 query 的抽樣）不同，不可並排比較。",
            "分箱表把每格的平均分數與實際正例率放在一起，只是並列兩個量：框架"
            "不做校準（#411），分數不保證是機率。上面每個指標都只看分數的大小"
            "順序，兩者相近或相差都不改變它們。",
            "一個門檻橫跨所有 query group，前提是不同 query group 的分數彼此可比。"
            "模型用排序類目標（lambdarank、rank_xendcg）訓練時，分數只為同一個 "
            "query group 內的先後而學，跨 group 用同一個門檻切是模型沒有優化過的"
            "用法；binary 目標沒有這個問題。",
            "分數範圍取最小～最大，少數極端的高分會把細箱撐寬，多數列擠進少數幾箱；"
            "看分箱表各格的列數就知道是不是這樣。",
        ],
        figures=figures,
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )


def _item_share_by_rank(counts_frame: pd.DataFrame) -> pd.DataFrame:
    """欄正規化：每個 rank 欄 ÷ 欄和 → 各 item 在該 rank 位置的佔比。

    每個 rank 欄加總=1（誰佔據該名次）。全 0 欄（0/0）得 NaN、render 端空白。
    依 §二，正規化後的矩陣用「按欄讀的數字表」呈現，不掛全域色階 heatmap。
    """
    col_sums = counts_frame.sum(axis=0)
    return counts_frame.divide(col_sums, axis=1)


def build_item_detail_section(
    report_aggregates: dict | None, parameters: dict
) -> ReportSection | None:
    """per-item 細部拆解（原診斷區升為頂層）。

    同一批排名的分數／名次分布側面。沿用 score 分布圖與 rank 計數 heatmap；
    新增 item-share-by-rank（欄正規化，數字表，G#1）＋ positive rate by rank
    數字表。升為頂層（collapsible=False）。

    分數分箱（每格平均分數 vs 實際正例率）不在這一段：它是「預測品質」段的
    分箱表（#381，ADR-0024）。#381 之前的 ``report_aggregates.json`` 還帶一個
    ``calibration`` 鍵（在 ``[0, 1]`` 上等寬切），本段一直不畫它，讀到舊檔也
    一樣不畫。
    """
    if not _section_on(parameters, "diagnostics"):
        return None
    # 與 build_diagnostics_figures 同一個「有沒有 score_histogram 家族」判斷；
    # 沒有分布家族時（包括只剩舊檔的 calibration 鍵），本段不畫。
    if not report_aggregates or "score_histogram" not in report_aggregates:
        return None

    from recsys_tfb.evaluation.diagnostics_spark import frame_from_json
    from recsys_tfb.evaluation.distributions import (
        plot_positive_rank_heatmap,
        plot_positive_rate_rank_heatmap,
        plot_rank_heatmap,
        plot_score_boxplot_by_label,
        plot_score_histogram,
    )

    cols = report_aggregates["columns"]
    item_col, label_col = cols["item"], cols["label"]
    # 先群組所有圖（score 分布 2 張＋rank 矩陣 heatmap 3 張），再放數字表——
    # 段內視覺一致（section 先 render figures 再 render tables）。positive rate
    # 是有界 [0,1] 的率矩陣，全域色階有意義 → 用 heatmap。
    figs = [
        plot_score_histogram(
            frame_from_json(report_aggregates["score_histogram"]),
            item_col=item_col),
        plot_score_boxplot_by_label(
            frame_from_json(report_aggregates["score_box_by_label"]),
            item_col=item_col, label_col=label_col),
        plot_rank_heatmap(
            frame_from_json(report_aggregates["rank_counts"])),
        plot_positive_rank_heatmap(
            frame_from_json(report_aggregates["positive_rank_counts"])),
        plot_positive_rate_rank_heatmap(
            frame_from_json(report_aggregates["positive_rate"])),
    ]

    # item share by rank：逐欄正規化（每 rank 欄加總=1），刻意用數字表——掛全域
    # 色階 heatmap 會誘導跨欄比色誤讀（§二）。放在所有 heatmap 之後。
    rank_counts = frame_from_json(report_aggregates["rank_counts"])
    pos_rank_counts = frame_from_json(report_aggregates["positive_rank_counts"])
    tables = [
        _item_share_by_rank(rank_counts),
        _item_share_by_rank(pos_rank_counts),
    ]
    titles = [
        "item share by rank（query 數，欄正規化：每 rank 各 item 佔比，欄和=1）",
        "item share by rank（positive query 數，欄正規化）",
    ]
    return ReportSection(
        title="per-item 細部拆解",
        description=(
            "同一批排名的分數與名次分布側面。先看圖（群組在前）：score 分布、"
            "score by label、rank 計數 heatmap、positive rank 計數 heatmap、"
            "positive rate by rank heatmap；再看數字表：item share by rank（欄"
            "正規化，看誰佔據各名次）。item share 刻意用數字表而非 heatmap——它"
            "是逐欄正規化（每欄加總=1），掛全域色階會誘導跨欄比色誤讀，請在同一"
            "欄內比。rank 計數的欄和＝總 query 數。明細數字表點標題展開。分數分箱"
            "（每格平均分數 vs 實際正例率）在「預測品質」段，"
            "evaluation.report.sections.prediction_quality 打開時才有。"
        ),
        figures=figs,
        tables=tables,
        table_titles=titles,
        collapsed_tables=[True, True],
        collapsible=False,
    )


def _od(a, b):
    """None-safe A − B (either operand missing → None)."""
    return None if a is None or b is None else a - b


def build_baseline_section(
    metrics: dict, baseline_metrics: dict | None, parameters: dict
) -> ReportSection | None:
    # compute_baseline_metrics writes {"enabled": False, "config_fingerprint":
    # ...} when the section is off, so the stub can carry its fingerprint;
    # None stays accepted for callers that pass no baseline at all.
    if (not _section_on(parameters, "baseline") or baseline_metrics is None
            or baseline_metrics.get("enabled") is False):
        return None
    from recsys_tfb.evaluation.compare import build_comparison_result

    comp = build_comparison_result(
        metrics, baseline_metrics, "Model", "Baseline"
    )
    disp = _report_cfg(parameters).get("display", {}) or {}
    n_items = count_items(metrics)
    # One K for both sides, the model's: the baseline is scored on the model's
    # own rows (build_baseline_frame), so its "all" resolved to the same K,
    # and its slim bundle records neither that nor an item count (#434).
    all_k = resolved_all_k(metrics)
    attr_ks = resolve_display_k(
        disp.get("primary_map_k", [1, 3, 5, "all"]), all_k
    )
    # overall 三表用 k superset（使用者指定，k 放欄位）
    k_super = resolve_display_k([1, 2, 3, 4, 5, "all"], all_k)
    # Same helper the node (compute_baseline_metrics) reads (bug 1,
    # ADR-0020): before it existed this line read .get("lookback_months")
    # with no default, so an unset key printed nothing here while the node
    # had in fact used 12.
    lookback = resolve_lookback_months(parameters)

    tables: list[pd.DataFrame] = []
    titles: list[str] = []
    collapsed: list[bool] = []

    def _add(tbl, title, is_collapsed):
        tables.append(tbl)
        titles.append(title)
        collapsed.append(is_collapsed)

    # bug 1 (ADR-0020), partial window: _lookback_window raises only when the
    # window has no label rows at all, so a label_table covering 2 of 12
    # lookback months passes. It used to print the plain 12-month sentence
    # and divide the per-month average by 12, understating it 6x with no
    # error. monthly_counts holds only months with label rows inside the
    # window, so its distinct months are the coverage; without it (older
    # results) the configured lookback stays both text and divisor.
    monthly = (baseline_metrics or {}).get("monthly_counts") or {}
    months = sorted({mo for per in monthly.values() for mo in per})
    covered = len(months)
    window_partial = 0 < covered < lookback
    per_month_divisor = covered if window_partial else lookback
    # Several evaluated dates (#374): one window per date, and purchase_counts
    # sums them all, so the per-month average divides by the window-months
    # behind that sum — each window's covered months under the same partial
    # rule as above — not by one window's lookback (N times too high). The
    # node writes window_months_covered only then; the distinct months of
    # monthly_counts cannot stand in for it, since overlapping windows share
    # months and a boundary month can hold rows for one window and not the other.
    windows = (baseline_metrics or {}).get("window_months_covered") or {}
    several_dates = len(windows) > 1
    if several_dates:
        full_window_months = len(windows) * lookback
        per_month_divisor = sum(
            c if 0 < c < lookback else lookback for c in windows.values()
        )
        window_partial = per_month_divisor < full_window_months

    # [1] popularity 排名組成（總計 count + 平均每月）；各月明細/趨勢＝Phase 2。
    pcounts = (baseline_metrics or {}).get("purchase_counts") or {}
    if pcounts:
        sorted_items = sorted(
            pcounts.items(), key=lambda kv: kv[1], reverse=True
        )
        pop_cols = {"count": [v for _, v in sorted_items]}
        if per_month_divisor:
            pop_cols["平均每月"] = [
                round(v / per_month_divisor, 1) for _, v in sorted_items
            ]
        pop_cols["rank"] = list(range(1, len(sorted_items) + 1))
        _add(
            pd.DataFrame(pop_cols, index=[k for k, _ in sorted_items]),
            "popularity 排名組成", False,
        )

    # [1b] 月度趨勢：rows=item（總計降序，與 [1] 同序）、cols=月份升序＋合計。
    #      各 item 的「合計」＝該列月份和，逐 item 對齊 [1] 的 count。
    if monthly:
        item_order = sorted(
            monthly, key=lambda it: sum(monthly[it].values()), reverse=True
        )
        mdf = pd.DataFrame(
            {mo: [monthly[it].get(mo, 0) for it in item_order] for mo in months},
            index=item_order,
        )
        mdf["合計"] = mdf.sum(axis=1)
        _add(mdf, "popularity 月度趨勢", True)

    # [2] overall：mAP / recall / precision 各一張，rows=[Model,Baseline,Δ]、
    #     cols=@k（superset），明細收合。
    overall_a = comp["result_a"].get("overall", {}) or {}
    overall_b = comp["result_b"].get("overall", {}) or {}
    overall_delta = comp["overall_delta"]
    for fam, label in (("map", "mAP"), ("recall", "recall"),
                       ("precision", "precision")):
        data = {}
        for who, src in (("Model", overall_a), ("Baseline", overall_b),
                         ("Δ", overall_delta)):
            data[who] = {
                f"@{k}": src.get(f"{fam}@{_k_to_lookup(k, all_k)}")
                for k in k_super
            }
        _add(pd.DataFrame(data).T, f"overall {label}@k (M/B/Δ)", True)

    # [3] per-item compare tables — only when baseline has per_item；明細收合。
    per_item_a = comp["result_a"].get("per_item", {}) or {}
    per_item_b = comp["result_b"].get("per_item", {}) or {}
    per_item_delta = comp.get("per_item_delta", {}) or {}
    macro_a = (metrics.get("macro_avg", {}) or {}).get("by_item")
    macro_b = (baseline_metrics.get("macro_avg", {}) or {}).get("by_item")
    if per_item_b:
        # bug 5 (ADR-0020): either side's macro denominator can shrink
        # silently, so Model and Baseline are disclosed separately.
        item_cov = macro_coverage_suffix_mb(
            per_item_a, per_item_b, n_items, parameters, macro_a, macro_b
        )
        # 兩張 per-item M/B/Δ 用同一組 k（attr_ks＝primary_map_k），彼此一致；
        # 為控寬用縮減集，與衡量指標 per-item 的完整 [1..5,all] 不同（描述封邊）。
        for metric_key, col_fmt, ks, title in (
            ("hit_rate", "recall@{k}", attr_ks, "per-item recall@k (M/B/Δ)"),
            ("map_attr", "map_attr@{k}", attr_ks,
             "per-item map_attr@k (M/B/Δ)"),
        ):
            _add(
                per_item_metric_compare_table(
                    per_item_a, per_item_b, per_item_delta,
                    ks, all_k, metric_key, col_fmt,
                    macro_a=macro_a, macro_b=macro_b, all_k_b=all_k,
                ),
                f"{title}{item_cov}", True,
            )

    # [4] per-segment mAP@k M/B/Δ — 只比主指標 mAP（控寬，reference 段）；rows＝
    #     每個 segment 的 Model/Baseline/Δ 三列、cols＝@k。需 model 與 baseline
    #     兩側都有 per_segment（key 由同一 active_seg_col 對齊）。明細收合。
    seg_a = metrics.get("per_segment", {}) or {}
    seg_b = (baseline_metrics or {}).get("per_segment", {}) or {}
    if seg_a and seg_b:
        rows: dict[str, dict] = {}
        for seg in _unmatched_last(
            {s: None for s in sorted(set(seg_a) | set(seg_b))}
        ):
            a, b = seg_a.get(seg, {}) or {}, seg_b.get(seg, {}) or {}
            for who, src in ((f"{seg} · Model", a), (f"{seg} · Baseline", b)):
                rows[who] = {
                    f"@{k}": src.get(f"map@{_k_to_lookup(k, all_k)}")
                    for k in k_super
                }
            rows[f"{seg} · Δ"] = {
                f"@{k}": _od(a.get(f"map@{_k_to_lookup(k, all_k)}"),
                            b.get(f"map@{_k_to_lookup(k, all_k)}"))
                for k in k_super
            }
        _add(pd.DataFrame(rows).T, "per-segment mAP@k (M/B/Δ)", True)

    # [5] 大類 overall mAP@k M/B/Δ — 大類粒度的 overall mAP 對照；rows＝
    #     [Model,Baseline,Δ]、cols＝@k。cat_k 取 model 的 category bundle 的 K
    #     （baseline slim bundle 無 dataset_overview，兩側同一 category 集）。
    cat_a = (metrics.get("category") or {}).get("overall", {}) or {}
    cat_b = ((baseline_metrics or {}).get("category") or {}).get("overall", {}) or {}
    if cat_a and cat_b:
        cat_k = resolved_all_k(metrics.get("category") or {}) or all_k
        cks = resolve_display_k([1, 2, 3, 4, 5, "all"], cat_k)
        data = {}
        for who, src in (("Model", cat_a), ("Baseline", cat_b)):
            data[who] = {
                f"@{k}": src.get(f"map@{_k_to_lookup(k, cat_k)}") for k in cks
            }
        data["Δ"] = {
            f"@{k}": _od(cat_a.get(f"map@{_k_to_lookup(k, cat_k)}"),
                        cat_b.get(f"map@{_k_to_lookup(k, cat_k)}"))
            for k in cks
        }
        _add(pd.DataFrame(data).T, "大類 overall mAP@k (M/B/Δ)", True)

    # Always printed (bug 1): lookback is resolved via the shared helper above
    # and is never unset. A partially covered window also states how many
    # months it actually had (see window_partial above).
    lookback_note = (
        f"popularity 以過去 {lookback} 個月的歷史購買計數重排"
        f"（label_table 在這個視窗內實際只涵蓋 {covered} 個月）。"
        if window_partial else
        f"popularity 以過去 {lookback} 個月的歷史購買計數重排。"
    )
    trend_note = ""
    if several_dates:
        n_windows = len(windows)
        lookback_note = (
            f"popularity 對 {n_windows} 個評估日期各以該日期之前 {lookback} 個月"
            f"的歷史購買計數重排；排名組成的 count 是這 {n_windows} 個視窗的"
            f"合計，平均每月＝count ÷ {per_month_divisor}"
            + (
                f"（各視窗內 label_table 有資料的月數加總：實際只涵蓋 "
                f"{per_month_divisor} 個視窗月，滿額 {full_window_months} 個）。"
                if window_partial else
                f"（{n_windows} 個視窗 × 每個 {lookback} 個月）。"
            )
        )
        trend_note = (
            "視窗彼此重疊時，同一個月會被每個涵蓋它的視窗各算一次，"
            "所以月度趨勢表的逐月數字是重複計數後的合計。"
        )
    return ReportSection(
        title="baseline — popularity 對照",
        description=(
            f"Model 相對 popularity baseline 的位置。{lookback_note}popularity "
            "排名組成為各 item 跨月合計（總計＋平均每月）；月度趨勢表把同一批計數"
            "拆到各 item 逐月（列＝item、欄＝月份，合計逐 item 對齊排名組成）。"
            f"{trend_note}"
            "overall 的 mAP／recall／precision 各一張表、"
            "k 放欄位、點標題展開。對照層級：overall（三家族）、per-item"
            "（recall／map_attr 兩張，k＝[1,3,5,all] 控寬）、per-segment 與大類"
            "overall（只比主指標 mAP，各一張，reference 段控寬）。per-segment／大類"
            "僅在 model 與 baseline 兩側都有該切片時出現（key 由同一 active segment／"
            "item_categories 對齊）。"
        ),
        tables=tables,
        table_titles=titles,
        collapsed_tables=collapsed,
    )


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


def build_glossary_section(parameters: dict) -> ReportSection:
    tbl = pd.DataFrame(_GLOSSARY, columns=["指標", "語意"])
    return ReportSection(
        title="詞彙表 Glossary",
        description="指標語意，詳見 docs/metrics_concept_map.html。",
        tables=[tbl],
        table_titles=["指標語意"],
    )


# =====================================================================
# registry 診斷的多頁輸出
# =====================================================================
#
# **這一段刻意不認識任何單一診斷。** 走的是
# ``diagnosis.metric.contract.DIAGNOSES``：對每個名字 import 模組、讀
# ``TITLE``／``SCOPE``／``render``。因此新增第六項診斷 ＝ 新增一個子套件 ＋ 在
# registry 補一行，本檔零改動。
#
# 為什麼數字不複製一份到主報表：主報表只放入口
# （``build_diagnosis_links_section``）。同一個數字出現在兩個地方，就會有兩份
# 各自演化的格式與措辭，而讀者無從得知哪一份是後改的。

#: 索引頁的邏輯架構：每項診斷各回答什麼、各排除什麼，以及編號代表的意思。
#:
#: **這張表是規劃層級的敘述**（各診斷的分工），不是 registry。哪些項目真的
#: 存在由 ``DIAGNOSES`` 決定，見 :func:`_diagnosis_index_intro` 的狀態欄——
#: 兩者分開，索引頁才不會在有項目尚未落地時假裝它都在。
_DIAGNOSIS_PLAN = (
    ("config_shift", "配置引入的排序偏移",
     "抽樣比例與 sample weight 有沒有在每個 item 上引入 log-odds 偏移。",
     "偏移為 0 時，排序落差的來源就不在訓練設定這一側。"),
    ("item_ability", "item 辨識力",
     "模型能不能在同一個 query 內分辨誰會買哪一個 item。",
     "把客戶活躍度誤讀成 item 推薦能力。"),
    ("model_capacity", "模型容量分配",
     "gain／split 花在 item 身分，還是花在 context 特徵。",
     "把「學到互動訊號」與「只記住 item prior」分開。"),
    ("suppression", "壓制帳本",
     "哪些 label=0 排在 label=1 之前，造成多少 AP 缺口。",
     "把「模型排錯」與「商品本來就競爭」分開。"),
)


def _diagnosis_index_intro() -> str:
    """索引頁的說明片段（raw HTML，``write_pages`` 不 escape）。

    **這段文字就是使用者要的產出本身，不是裝飾**：需求原話是「忠實呈現數據，
    但是用一個清楚好懂的邏輯架構來幫助人類判斷，而不是直接給結論」。這裡寫
    的是那個邏輯架構——每項診斷回答什麼、排除什麼、為什麼是這個順序——讀者
    據此自己判斷，而報表本身一個結論都不下。

    狀態欄從 ``DIAGNOSES`` 動態導出，不寫死：``_DIAGNOSIS_PLAN`` 若列入尚未
    落地的診斷，寫死的話這頁會在它落地前就宣稱它在（而那種錯看不出來，因為
    字串長得很合理）。目前列出的診斷皆已落地。
    """
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    rows = []
    for i, (name, title, answers, rules_out) in enumerate(
        _DIAGNOSIS_PLAN, start=1
    ):
        live = name in DIAGNOSES
        status = "已在 registry" if live else "尚未進 registry"
        rows.append(
            f"<tr><td>{i}</td><td>{title}<br><code>{name}</code></td>"
            f"<td>{answers}</td><td>{rules_out}</td><td>{status}</td></tr>"
        )
    table = (
        "<table><thead><tr>"
        "<th>#</th><th>診斷</th><th>回答什麼</th><th>排除什麼</th>"
        "<th>目前狀態</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )
    return (
        "<p>這裡把排序結果拆成彼此不重疊的幾個提問。每一頁只呈現它量到的"
        "數字，並在頁首用「範圍說明」寫出這些數字量的是什麼、算在哪批列上、"
        "看不見什麼。判讀留給讀者。</p>"
        + table
        + "<p><strong>編號的意思</strong>：由「資料與訓練設定造成的」往"
        "「模型學到什麼」再往「排序結果本身」推進。前一層解釋得掉的部分，"
        "後一層就不必重複歸因——這是歸因的優先權，也是預設的閱讀順序。</p>"
        "<p><strong>編號不是硬閘門</strong>：已實作的項目每次都會跑、都會"
        "呈現，前一項的結果不會擋掉後一項；任何一頁都可以單獨打開來讀。</p>"
        "<p>狀態欄若標「尚未進 registry」，代表該項還沒有實作、這次執行不會"
        "有它的頁面；下方清單列出的就是本次實際寫出的全部頁面。</p>"
    )


def assemble_diagnosis_pages(results: dict, parameters: dict, out_dir) -> list:
    """把每項診斷的結果組成獨立頁面。本函式不認識任何單一診斷。

    Args:
        results: ``{診斷名: compute 的輸出 dict}``。缺席或 ``render`` 回**空
            序列**（例如該項停用）的診斷不會產生頁面——**缺席是「這頁不存在」，
            不是「這頁是空的」**；空頁看起來像「量到了、結果什麼都沒有」。
            ``render`` 回傳的是多個 section（一張圖／一張表各一個 section，
            各自帶標題、公式與重點），整頁的 section 順序即閱讀順序。
        out_dir: 頁面輸出目錄（與各診斷 JSON 同一個 ``diagnosis/`` 目錄）。

    Returns:
        實際寫出的檔案路徑（``plotly.min.js`` 最先、各頁、``index.html`` 最後）。
    """
    import dataclasses
    import importlib

    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES
    from recsys_tfb.report import Page
    from recsys_tfb.report.pages import write_pages

    pages = []
    for i, name in enumerate(DIAGNOSES, start=1):
        result = (results or {}).get(name)
        if result is None:
            continue
        mod = importlib.import_module(f"recsys_tfb.diagnosis.metric.{name}")
        sections = mod.render(result, parameters)
        if not sections:
            continue
        slug = f"{i:02d}-{name.replace('_', '-')}"   # 數字前綴＝閱讀順序
        # SCOPE.sampling 在這裡統一填，不是每項診斷自己填：各診斷共用同一份
        # diagnosis_sample，sampling_description 永遠在同一個位置。讓各診斷
        # 各帶一個 hook 等於同一段 replace 被逐項重抄。
        scope = dataclasses.replace(
            mod.SCOPE,
            sampling=(result.get("sample_meta", {}) or {}).get(
                "sampling_description", ""),
        )
        pages.append(Page(slug=slug, title=mod.TITLE,
                          scope=scope, sections=tuple(sections)))
    if not pages:
        # 一頁都沒有就完全不落地。否則會留下一個「index.html 列了診斷、清單
        # 是空的、外加 3.5MB plotly.min.js」的目錄，看起來像跑過但什麼都沒
        # 量到——那是本重構要避免的誤讀，不是「誠實地呈現沒有資料」。
        return []
    return write_pages(pages, out_dir=out_dir,
                       index_title="排序診斷",
                       index_intro=_diagnosis_index_intro())


def build_diagnosis_links_section(
    diagnosis_pages: list | None,
    parameters: dict,
) -> ReportSection | None:
    """主報表指向診斷頁的入口。**只放連結，不放任何診斷數字。**

    數字複製一份到主報表就會有兩個真實來源；改了其中一邊，讀者無從得知哪一
    份是後改的。一頁都沒寫出來時回 ``None``——指向 404 的入口比沒有入口更糟。

    連結是相對路徑：主報表在 ``…/<snap_date>/report.html``，診斷頁在同層的
    ``diagnosis/``，兩者一起搬移時連結仍有效。
    """
    if not diagnosis_pages or not _section_on(parameters, "diagnosis_links"):
        return None
    n_pages = sum(
        1 for p in diagnosis_pages
        if str(p).endswith(".html") and not str(p).endswith("index.html")
    )
    return ReportSection(
        title="排序診斷（獨立報表）",
        description=(
            '<a href="diagnosis/index.html">診斷索引 diagnosis/index.html</a>'
            f"　—　本次寫出 {n_pages} 頁。索引頁說明每一項回答什麼、排除"
            "什麼，各頁的數字與範圍說明都留在該頁，這裡不複製一份。"
        ),
    )


def build_completeness_section(
    metrics: dict, parameters: dict, metric_ci: dict | None = None
) -> ReportSection:
    """完整性檢查（殿後）：本次執行的事實 ＋「什麼看似正常其實沒量到」。

    presentation §一.4：交代邊界。只陳述事實，不評級。
    """
    eval_p = parameters.get("evaluation", {}) or {}
    totals = _dataset_overview(metrics).get("totals", {}) or {}
    metric_p = metric_params(parameters)
    sample_meta = (metric_ci or {}).get("sample", {}) or {}

    mk = metric_p["k"]
    # 「item 數」的標籤印使用者自己的 item 欄名，不寫死「產品」（#327；同
    # build_overview_section 的 entity 標籤）。
    item_col = get_schema(parameters)["item"]
    # bug 3 (ADR-0020): the same n_queries label fix as
    # build_overview_section.
    facts = {
        "k_values": eval_p.get("k_values"),
        "全部 query 數 n_queries": metrics.get("n_queries"),
        "有正例的 query 數": _od(
            metrics.get("n_queries"), metrics.get("n_excluded_queries")
        ),
        "排除 query 數 n_excluded_queries": metrics.get("n_excluded_queries"),
        "正例列數 n_positives": totals.get("n_positives"),
        f"{item_col} 數 n_items": totals.get("n_items"),
        "metric.weight_alpha（item 加權指數 α；0＝item 等權）":
            metric_p.get("weight_alpha"),
        "metric.k（AP 截斷 k；無＝不截斷、算全長）":
            ("無（不截斷）" if mk is None else mk),
        "metric.min_positives（觀察名單門檻；0＝不設）":
            metric_p.get("min_positives"),
        "metric.shrinkage_k（向 pooled 平均收縮強度；0＝不收縮）":
            metric_p.get("shrinkage_k"),
        "抽樣描述 sampling_description": sample_meta.get("sampling_description"),
    }
    # 同分列佔比：只有宣告了 event 的部署算得出來，也只有它需要看。這個數字
    # 是「名次由決勝規則決定、不是由分數決定」的列佔多少——決勝規則按 event
    # 由小到大，event 是時間戳時等於早的曝光永遠排前面，而那個方向不是中性的
    # （廣告示例實測約 0.02 mAP，#378）。框架不替使用者選，把數字印出來讓他
    # 自己判斷這件事在他的資料上有多大。沒宣告時這兩列不存在，既有報表不變。
    if totals.get("n_tied_rows") is not None:
        facts["同分列數 n_tied_rows（名次由 item／event 決勝，非由分數決定）"] = (
            totals.get("n_tied_rows")
        )
        share = totals.get("tied_row_share")
        facts["同分列佔比 tied_row_share"] = (
            None if share is None else f"{share:.1%}"
        )
    facts_tbl = pd.DataFrame([facts]).T
    facts_tbl.columns = ["value"]

    bullets = [
        # 刻意不寫出被隱藏指標的名字：整份報表有一條端到端護欄禁止該字串出現
        # （避免值洩漏）；這裡只陳述「算了但不呈現」這件事。
        "部分排序衍生指標有算但刻意不呈現（本框架目標是排序 macro mAP，"
        "非機率校準）。",
        "每-query 正例數分佈本版未算（Phase 2，需新增 per-query 聚合）。",
        "候選集為密集時每 item 候選覆蓋率恆 100%——per-item 正例佔比與正例數"
        "同序，非獨立軸。",
    ]
    # 跳過的診斷寫在這裡，不寫在診斷索引頁：這一段的題目就是「什麼看起來正常
    # 其實沒量到」，而一頁憑空消失正是那種看起來正常。原因字串與各診斷自己
    # 印的那一句同一個來源（``diagnosis.metric._common.schema_skip_reason``），
    # 兩邊不會漂。沒宣告選用角色時這個迴圈一句也不加，既有報表逐字不變。
    bullets.extend(skipped_diagnosis_bullets(parameters))
    return ReportSection(
        title="完整性檢查",
        description=(
            "本次執行的事實（k、規模、抽樣、metric 參數）與「什麼情況數字看起來"
            "正常、其實沒量到或不完整」。放在最後，交代邊界。"
        ),
        tables=[facts_tbl],
        table_titles=["本次執行事實"],
        bullets=bullets,
    )


def skipped_diagnosis_bullets(parameters: dict) -> list[str]:
    """每一項在目前 schema 下跳過的診斷，各一句「哪一項、為什麼」。

    走 ``contract.DIAGNOSES``（不是自己抄一份名單），所以之後新增的診斷自動
    被問到；``"ci"`` 另外補上——它是共用診斷抽樣的第四個消費者，但不在那份
    registry 裡（它是頭號指標的信賴區間，不是一頁診斷）。沒有任何一項被跳過時
    回空 list，主報表因此逐字不變。
    """
    from recsys_tfb.diagnosis.metric._common import schema_skip_reason
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    out = []
    for name in (*DIAGNOSES, "ci"):
        reason = schema_skip_reason(parameters, name)
        if reason:
            label = "頭號指標的信賴區間" if name == "ci" else f"診斷「{name}」"
            out.append(f"{label}本次未算：{reason}")
    return out


def several_eval_dates(configured) -> bool:
    """Whether the configured evaluation date value names more than one date.

    ``len(as_date_list(value)) > 1``, the one test the repo uses for "several
    dates" (``as_date_list`` already drops repeats).
    """
    return len(as_date_list(configured)) > 1


def eval_dates_display(configured):
    """The report metadata's date cell for the configured evaluation date value.

    Takes the value, not ``parameters``, so the config key stays read where it
    is registered (S6). A single date is shown exactly as configured, as it
    always was. Several dates (#374) read as ``earliest ~ latest（N 個日期）``:
    the list itself is long and unordered as written.
    """
    if not isinstance(configured, list):
        return configured
    dates = sorted(as_date_list(configured))
    if not dates:
        return "unknown"
    if len(dates) == 1:
        return dates[0]
    return f"{dates[0]} ~ {dates[-1]}（{len(dates)} 個日期）"


def assemble_report(
    metrics: dict,
    parameters: dict,
    baseline_metrics: dict | None = None,
    report_aggregates: dict | None = None,
    metric_ci: dict | None = None,
    diagnosis_pages: list | None = None,
    prediction_quality: dict | None = None,
) -> str:
    """Assemble every enabled section (the ``candidates`` list below is the
    authoritative order) into the final HTML string.

    spine（目的驅動、由粗到細、克制）：概覽 → 核心概念 → 基本統計 →
    衡量指標 → 預測品質（開了才有）→ per-item 細部拆解 → baseline →
    排序診斷連結 → 完整性檢查 → 詞彙表。
    """
    pq_section = build_prediction_quality_section(
        prediction_quality, metrics, parameters)
    candidates = [
        build_overview_section(metrics, parameters, metric_ci=metric_ci,
                               prediction_quality_shown=pq_section is not None),
        build_core_concept_section(parameters),
        build_dataset_overview_section(metrics, parameters),
        build_metrics_section(metrics, parameters, metric_ci=metric_ci),
        pq_section,
        build_item_detail_section(report_aggregates, parameters),
        build_baseline_section(metrics, baseline_metrics, parameters),
        build_diagnosis_links_section(diagnosis_pages, parameters),
        build_completeness_section(metrics, parameters, metric_ci=metric_ci),
        build_glossary_section(parameters),
    ]
    sections = [s for s in candidates if s is not None]
    eval_params = parameters.get("evaluation", {}) or {}
    configured_dates = eval_params.get("snap_date", "unknown")
    metadata = {
        "Model Version": parameters.get("model_version", "unknown"),
        "Snap Date": eval_dates_display(configured_dates),
    }
    if several_eval_dates(configured_dates):
        # Right under the dates, the one place every reader passes. The
        # bootstrap CIs (main report and diagnosis pages) were built for one
        # date; issue #389 fixes them, and removes this line.
        metadata["⚠ 信賴區間"] = (
            "多個日期合併評估時，本報表與診斷頁的信賴區間可能偏窄或有偏，"
            "詳見 issue #389。"
        )
    metadata.update({
        "Generated At": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Total Queries": metrics.get("n_queries"),
        "Excluded Queries": metrics.get("n_excluded_queries"),
    })
    return generate_html_report(
        sections, title="Model Evaluation Report", metadata=metadata
    )
