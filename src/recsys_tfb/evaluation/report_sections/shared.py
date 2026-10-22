"""What more than one section of the main report uses, and what the
comparison report borrows from it (ADR-0031 principle 6). A helper only
one section uses stays in that section's file."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from recsys_tfb.core.consistency import (
    EVALUATION_REPORT_SECTIONS,
    ZERO_POSITIVE_GROUP_WEIGHT_COL,
    resolved_zero_positive_group_ratio,
    test_carries_zero_positive_group_weight,
)
from recsys_tfb.core.date_ranges import as_date_list
from recsys_tfb.evaluation.metrics import (
    ALL_POSITIVE_KEY,
    drop_all_positive_groups,
    metric_params,
    n_groups_with_positive,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT


def resolve_display_k(raw_k: list, all_k: int) -> list:
    """Map mixed int/'all' display k list to concrete column suffixes.

    Returns labels as strings/ints that are used both as dict keys and for
    metric lookups. 'all' is looked up at ``all_k`` (``k_to_lookup``) but is
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


def k_to_lookup(k, all_k: int) -> int | str:
    """Convert display label to metric dict key suffix.

    ``all_k`` comes from the bundle being read (``metrics.resolved_all_k``),
    never from counting its items (#434).
    """
    if k == "all":
        return all_k
    return k


#: K columns of every metrics-section table, before the bug 8 clamp.
_METRICS_SECTION_K = (1, 2, 3, 4, 5, "all")


def metrics_section_ks(all_k: int) -> list:
    """The K columns the metrics section's tables actually show at this grain.

    One derivation for those tables and for the note that points into them
    (:func:`headline_column`, which the overview and ranking-metrics
    sections both show). A note naming
    ``map_attr@{metric.k}`` is only true while that column survives the
    ``resolve_display_k`` clamp; with a second copy of the list the note could
    keep naming a column the table dropped, and nothing would raise — the
    reader just gets pointed at a column that is not there.
    """
    return resolve_display_k(list(_METRICS_SECTION_K), all_k)


def headline_column(parameters: dict, all_k: int, where: str) -> str:
    """How the headline is truncated and where it sits in the metrics
    section's map_attr table, as one phrase both sections show (``where``
    names that section from the caller's side). Names the column only when
    that table shows it (:func:`metrics_section_ks`)."""
    mk = metric_params(parameters)["k"]
    if mk is None:
        return (f"不截斷（metric.k 未設），即{where} B 塊「{MACRO_LABEL}」列的 "
                "map_attr@all")
    if mk in metrics_section_ks(all_k):
        return (f"截斷在 {mk}（metric.k），即{where} B 塊「{MACRO_LABEL}」列的 "
                f"map_attr@{mk}")
    return f"截斷在 {mk}（metric.k），{where}各表不顯示 @{mk} 欄"


MACRO_LABEL = "Macro 平均"


def report_cfg(parameters: dict) -> dict:
    return (parameters.get("evaluation", {}) or {}).get("report", {}) or {}


def section_on(parameters: dict, name: str) -> bool:
    """Whether report section ``name`` is switched on (default: on).

    Pre-check: ``name`` is in ``EVALUATION_REPORT_SECTIONS``, the set A34 keeps
    equal to what the conf declares. A name outside it would read a key no conf
    declares and quietly default to on, the way ``diagnosis_links`` did.
    """
    if name not in EVALUATION_REPORT_SECTIONS:
        raise ValueError(
            f"section_on: {name!r} is not in "
            f"core.consistency.EVALUATION_REPORT_SECTIONS "
            f"{sorted(EVALUATION_REPORT_SECTIONS)}. Add it there and declare "
            f"it under evaluation.report.sections (A34)."
        )
    sections = report_cfg(parameters).get("sections", {}) or {}
    return bool(sections.get(name, True))


def prediction_quality_shown(
    prediction_quality_metrics: dict | None, parameters: dict
) -> bool:
    """Whether the prediction-quality section is on the page: its switch is on
    and ``compute_prediction_quality`` computed the payload rather than writing
    its stub. Here rather than in that section's file because the overview's
    navigation table asks it too, and sections do not import each other."""
    return bool(section_on(parameters, "prediction_quality")
                and prediction_quality_metrics
                and prediction_quality_metrics.get("enabled") is True)


def results_read_by(
    reader: str, reads: tuple[str, ...], results: Mapping[str, dict]
) -> dict:
    """The results ``reader`` declared in ``reads``, and no others; raises
    naming the missing ones. Handing a reader only what it declared is what
    keeps the declaration true: a read it did not declare fails here."""
    missing = [name for name in reads if name not in results]
    if missing:
        raise ValueError(
            f"{reader} reads {missing}, which it was not given "
            f"(given {sorted(results)})."
        )
    return {name: results[name] for name in reads}


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
MIGRATION_SCRIPT = "scripts/migrations/migrate_evaluation_results_keys.py"


def read_dataset_overview(metrics: dict) -> dict:
    """``metrics["dataset_overview"]``, refusing a pre-#327 payload.

    Every read of the overview — each section's and
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
    return int((read_dataset_overview(metrics).get("totals", {}) or {}).get("n_items", 0))


def macro_item_coverage(per_item: dict, parameters: dict) -> int:
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
    n_a = macro_item_coverage(per_item_a, parameters)
    n_b = macro_item_coverage(per_item_b, parameters)
    return f"（參與 macro 的 item 數 M {n_a}／B {n_b}／全部 {n_items}）"


# =====================================================================
# 全正的 query group（#376）
# =====================================================================
#
# ``evaluation.query_filter.drop_all_positive_groups`` 只套用在衡量指標（含頭號
# 指標）與熱門度 baseline；per-item 細部拆解不跟（使用者 2026-09-18 決定：新開關
# 不延伸進診斷；當時的診斷頁已隨 #532 拿掉）。所以開關開著時，同一份報表裡有兩批
# 組，這一段的函式負責讓讀者看得出每個數字是哪一批。開關只經
# ``metrics.drop_all_positive_groups`` 讀，警告只經
# ``metrics.all_positive_share_warns`` 判斷——log 與報表不會各說各話。


#: 開關的設定鍵全名：印進警告與加註，讀者照著就找得到要改哪裡。
ALL_POSITIVE_SWITCH = "evaluation.query_filter.drop_all_positive_groups"

#: 全正的組為什麼值得數。不寫「AP 都是 1」或「滿分」：label 只有 0／1 時，K 小於
#: 組的列數 n 的 ``map@K``＝K/n、K 大於 n 的 ``precision@K``＝n/K，label 分級時
#: 值又不同；永遠成立的只有「排法不影響它的組層級（per-query）指標」。也不寫「每組
#: 的分數都一樣」：per-item 的 ``map_attr@K``／``hit_rate@K``／``mean_pos`` 在 K
#: 小於列數時仍看誰排進前 K。
ALL_POSITIVE_WHY = "不管怎麼排，組層級指標 mAP／precision／recall 都一樣"


def grain_bundles(metrics: dict) -> list[tuple[str, dict]]:
    """組數帳與警告逐粒度走的清單：item 粒度，有大類時再加大類粒度。

    大類的全正＝每個大類都至少有一個正例子列（大類那一輪 label＝子列最大值），
    和 item 粒度的全正是兩個不同的數，所以各一行帳、各自判斷警告。
    """
    out = [("item 粒度", metrics)]
    if metrics.get("category"):
        out.append(("大類粒度", metrics["category"]))
    return out


def zero_positive_draw_on_test(parameters: dict) -> float | None:
    """dataset 對 test 表無正例 query group 的抽樣：監控模式回 ``None``（沒有
    dataset 抽樣，數到的是資料本身）；``--post-training`` 且 test 表帶權重欄
    （``test_carries_zero_positive_group_weight``）回保留比例 r（> 0）；不帶
    權重欄回 ``0.0``（dataset 全丟了）。

    一個判斷、兩段措辭：組數帳的 ``ranking_metrics._zero_positive_origin`` 與資料概況／預測
    品質段的 :func:`kept_zero_positive_groups_note` 都從這裡讀，不各寫一版分支。
    """
    if not parameters.get("post_training"):
        return None
    if not test_carries_zero_positive_group_weight(parameters):
        return 0.0
    return resolved_zero_positive_group_ratio(parameters, "test")


def query_count_rows(metrics: dict, parameters: dict) -> dict:
    """「規模／分母」與「本次執行事實」共用的 query 組數列。

    開關關時與 #376 之前逐字相同。開關開時 ``n_excluded_queries`` 只數無正例的
    組、實際被排除的還有全正的組，「有正例的 query 數」也不再是算進衡量指標的
    組數：標籤改成說清楚是哪一批，並另列全正與算進衡量指標兩列（兩列相加＝有
    正例的 query 數）。
    """
    n_queries = metrics.get("n_queries")
    n_zero = metrics.get("n_excluded_queries")
    n_pos = n_groups_with_positive(metrics)
    if not drop_all_positive_groups(parameters):
        return {
            "全部 query 數 n_queries": n_queries,
            "有正例的 query 數": n_pos,
            "排除 query 數 n_excluded_queries": n_zero,
        }
    n_all = metrics.get(ALL_POSITIVE_KEY)
    return {
        "全部 query 數 n_queries": n_queries,
        "有正例的 query 數": n_pos,
        f"其中全正而排除的 query 數 {ALL_POSITIVE_KEY}": n_all,
        "其中算進衡量指標的 query 數（有正例的 − 全正的）": none_safe_diff(n_pos, n_all),
        "無正例而排除的 query 數 n_excluded_queries": n_zero,
    }


def kept_ratio_phrase(ratio: float) -> str:
    """「test 表保留了比例 r 的無正例組」的說法，一個來源：資料概況的說明與
    衡量指標段的組數帳（#376）都印它，兩邊不會各寫一版。"""
    return f"test 表保留了比例 r＝{ratio:g} 的無正例 query group"


def kept_zero_positive_groups_note(metrics: dict, parameters: dict) -> str:
    """How many test query groups holding no positive this run evaluated, and
    at what ratio they were kept — empty unless ``--post-training`` with
    ``dataset.test_zero_positive_group_ratio`` above 0 (ADR-0025 decision 3).

    The count is ``n_excluded_queries``: the ranking metrics skip every query
    group without a positive, and under that mode every such group in the data
    is one the dataset draw kept. Printed because ``1 / r`` is a design
    weight: few kept groups means the weighted numbers are not stable, and the
    reader should see the count next to them.
    """
    ratio = zero_positive_draw_on_test(parameters)
    if not ratio:   # monitoring (None), or the dataset dropped them all (0.0)
        return ""
    n_kept = metrics.get("n_excluded_queries")
    kept = f"本次評估資料裡有 {n_kept:,} 個" if n_kept is not None else "本次評估資料裡有一些"
    return (
        f"{kept_ratio_phrase(ratio)}"
        f"（dataset.test_zero_positive_group_ratio）：{kept}，每列帶權重 "
        f"1／r＝{1 / ratio:.4g}（{ZERO_POSITIVE_GROUP_WEIGHT_COL}）。1／r 是設計"
        "權重，不是無偏估計：留下的組少時，加權後的比值型指標不穩。"
    )


def unmatched_last(by_segment: dict) -> dict:
    """同一份 dict，(unmatched) 移到最後、其餘順序不動。

    它不是一個 segment，列出來是為了交代它有多少 query；三處分群表（基本統計、
    衡量指標、baseline 對照）用同一個順序，讀者才不會在某張表把它當第一個群。
    """
    rest = {k: v for k, v in by_segment.items() if k != UNMATCHED_SEGMENT}
    if UNMATCHED_SEGMENT in by_segment:
        rest[UNMATCHED_SEGMENT] = by_segment[UNMATCHED_SEGMENT]
    return rest


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
            key = f"{metric_key}@{k_to_lookup(k, all_k)}"
            key_b = f"{metric_key}@{k_to_lookup(k, all_k_b)}"
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
        data[MACRO_LABEL] = _row(macro_a, macro_b, None)
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


def none_safe_diff(a, b):
    """None-safe A − B (either operand missing → None)."""
    return None if a is None or b is None else a - b


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
