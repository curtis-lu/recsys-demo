"""What more than one section of the main report uses, and what the
comparison report borrows from it (ADR-0031 principle 6). A helper only
one section uses stays in that section's file."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.core.consistency import (
    EVALUATION_REPORT_SECTIONS,
    report_section_on,
    resolved_zero_positive_group_ratio,
    test_carries_zero_positive_group_weight,
)
from recsys_tfb.core.date_ranges import as_date_list
from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARNS_KEY,
    DISPLAY_BINS_KEY,
    ENTRIES,
    LEDGER_RERUN_NODE,
    N_SCORED_WITH_POSITIVE_KEY,
    N_WITH_POSITIVE_KEY,
    POPULARITY_WINDOW_KEY,
    POPULATION_KEY,
    REFERENCE,
    THIS,
    THRESHOLD_SWEEP_KEY,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT


def k_to_lookup(k, all_k: int) -> int | str:
    """The K a label ``k`` is looked up at: ``"all"`` is ``all_k``, an
    integer is itself. The cut's lists (``steps/display_ks``, landed) are
    integers already — ``"all"`` was resolved to the block's ``all_k`` when
    they were cut (``display_ks.k_columns``) — so a landed K passes through.

    ``all_k`` comes from the bundle being read (``metrics.resolved_all_k``),
    never from counting its items (#434).
    """
    if k == "all":
        return all_k
    return k


MACRO_LABEL = "Macro 平均"


def report_cfg(parameters: dict) -> dict:
    return (parameters.get("evaluation", {}) or {}).get("report", {}) or {}


def section_on(parameters: dict, name: str) -> bool:
    """Whether report section ``name`` is switched on
    (``core.consistency.report_section_on``: on when absent, except the
    prediction-quality section).

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
    return report_section_on(parameters, name)


def sections_on(sections: tuple, parameters: dict) -> tuple:
    """The sections of ``sections`` (a report's page-order list of section
    modules) that this run draws: each declares ``SWITCH``, the
    ``evaluation.report.sections`` name that turns it off, or ``None`` when
    nothing does.

    The one answer to "which sections" (ADR-0031 principle 3): the report
    draws these, and the evaluation pipeline's requirement list wires only
    the results they declare in ``READS`` — so a switched-off section's
    computation is not in the pipeline, and a section removed from the list
    takes its computation with it.
    """
    return tuple(section for section in sections
                 if section.SWITCH is None
                 or section_on(parameters, section.SWITCH))


#: The catalog names of the comparison of the model with its baseline and of
#: the K cut (#536), as the main report reads them; then the comparison of
#: this run's side with the compared source, its K cut and the two sides'
#: coverage, as the comparison report reads them.
BASELINE_COMPARISON = "baseline_comparison"
DISPLAY_KS = "evaluation_display_ks"
SOURCE_COMPARISON = "source_comparison"
COMPARISON_DISPLAY_KS = "comparison_display_ks"
COMPARE_COVERAGE = "compare_coverage"

#: The node that lands each result :func:`landed_value` reads: where a refusal
#: sends the reader. Each exists in every mode that draws from the result:
#: the comparison's in ``--compare`` and ``--compare-only`` alike. The
#: coverage's producer (``restrict_to_common``) reads a memory-only frame, so
#: its re-run is the comparison's (``nodes.COMPARISON_RERUN_NODE``), whose
#: slice pulls it back and lands it again.
LANDED_BY = {
    BASELINE_COMPARISON: "compare_to_baseline",
    DISPLAY_KS: "resolve_display_ks",
    SOURCE_COMPARISON: "compare_to_source",
    COMPARISON_DISPLAY_KS: "resolve_comparison_display_ks",
    COMPARE_COVERAGE: "compute_compare_metrics",
}


def landed_value(result: dict | None, name: str, *path):
    """``result[path[0]][path[1]]…`` of the landed result ``name``; raises
    ``ValueError`` naming the node to re-run from when it is not there.

    For the results the report reads instead of computing (#536, ADR-0031
    principle 1): the comparison's cells and the K cut's lists. A value
    missing there means the file was landed by an older version of its node,
    and drawing on would print blank cells or raise a bare ``KeyError`` with
    no way out —
    #536 acceptance 4. A ``None`` that was landed is returned as it is; only
    stepping into it raises.
    """
    cursor = result
    for depth, key in enumerate(path):
        if not isinstance(cursor, dict) or key not in cursor:
            where = ".".join(str(k) for k in path[:depth + 1])
            node = LANDED_BY[name]
            raise ValueError(
                f"{name} has no {where}, which the report draws: it was "
                f"landed by an older {node}, or not for these settings. "
                f"Re-run with `python -m recsys_tfb evaluation --from-node "
                f"{node}`, with the flags of the run that wrote it (e.g. "
                f"--post-training, --compare <key>) — then re-draw."
            )
        cursor = cursor[key]
    return cursor


def has_entries(block: dict, side: str) -> bool:
    """Whether ``side``'s block held at least one entry (the comparison's
    ``ENTRIES``): ``None`` (no such block) and ``0`` both say no."""
    return bool(block[ENTRIES][side])


def both_present(block: dict) -> bool:
    """Whether both sides had the block at all — empty counts as present."""
    return all(block[ENTRIES][side] is not None for side in (THIS, REFERENCE))


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


#: What the per-item ``recall@K`` was called before #534 (ADR-0031 decision 7).
PRE_RENAME_PER_ITEM_PREFIX = "hit_rate@"


def refuse_pre_rename_per_item(name: str, bundle: dict | None) -> None:
    """Raise when the result ``name`` is a metrics bundle written before #534:
    its per-item block — at the item grain or the category grain — still
    spells the per-item recall ``hit_rate@K``.

    **Only the per-item block decides.** A query-group ``hit_rate@K`` — the
    share of query groups with a hit in the top K — lands in ``overall``
    (ADR-0031 decision 7); that is the new format, not the old.

    **Why refuse** rather than read both spellings, the reason
    :func:`read_dataset_overview` gives: drawn as-is every per-item recall cell
    is blank, and a blank reads as "recalled nothing", not as "this file is
    old". No migration script: the file a re-run writes also carries the item
    shared numbers (``per_item_shared_numbers``), which a renamed old file
    would still lack.
    """
    if not isinstance(bundle, dict):
        return
    for grain, grain_bundle in grain_bundles(bundle):
        found = sorted({
            key
            for cell in (grain_bundle.get("per_item") or {}).values()
            if isinstance(cell, dict)
            for key in cell if key.startswith(PRE_RENAME_PER_ITEM_PREFIX)
        })
        if found:
            raise ValueError(
                f"{name} predates the #534 rename: its per_item block "
                f"({grain}) still carries {found}, the per-item recall@K's "
                f"old name. Re-compute it — `python -m recsys_tfb evaluation "
                f"--from-node {LEDGER_RERUN_NODE}`, with the flags of the run "
                f"that wrote it (e.g. --post-training) — then re-draw. Drawn "
                f"as-is, every per-item recall cell would be blank."
            )


def refuse_pre_ledger(name: str, bundle: dict | None) -> None:
    """Raise when the result ``name`` is a metrics bundle written before
    #535: it carries no population ledger (``metrics.POPULATION_KEY``).

    **Why refuse**: the ledger is where each bundle says what ``"all"``
    resolved to (``metrics.resolved_all_k``), and a file this old also still
    holds the row facts that now land on their own — the report reads those
    from ``row_facts.json``, which that run never wrote. A result this run
    does not read (its section off) is ``None`` and passes.
    """
    if not isinstance(bundle, dict):
        return
    if POPULATION_KEY not in bundle:
        raise ValueError(
            f"{name} predates #535: it carries no {POPULATION_KEY!r} ledger, "
            f"and its run landed no row_facts.json. Re-compute it — `python "
            f"-m recsys_tfb evaluation --from-node {LEDGER_RERUN_NODE}`, with "
            f"the flags of the run that wrote it (e.g. --post-training) — "
            f"then re-draw."
        )


#: The all-positive accounts each grain of the row facts lands since #536
#: (``metrics.all_positive_accounts``), which the report reads instead of
#: computing.
ROW_FACTS_ACCOUNTS = (N_WITH_POSITIVE_KEY, ALL_POSITIVE_SHARE_KEY,
                      ALL_POSITIVE_WARNS_KEY)


def refuse_pre_landed_reads(results: Mapping[str, dict | None]) -> None:
    """Raise when a result was landed before #536 and lacks what the report
    now reads instead of computing (ADR-0031 principle 1): the row facts'
    all-positive accounts, at each grain; the baseline's popularity window;
    the prediction-quality display tables and threshold sweep. A result
    this run does not read (its section off) is absent and passes.

    **Why refuse**: no setting changed, so the fingerprint check passes such
    a file, and every read of a missing value draws nothing — the ledger
    line and the warning would vanish from the page without a word. The fix
    is a re-count, not a migration: the computation that lands these values
    is the one to run.
    """
    missing = []
    facts = results.get("evaluation_row_facts")
    if isinstance(facts, dict):
        for grain, bundle in grain_bundles(facts):
            missing += [f"evaluation_row_facts ({grain}): {key!r}"
                        for key in ROW_FACTS_ACCOUNTS if key not in bundle]
    baseline = results.get("baseline_metrics")
    if isinstance(baseline, dict) and POPULARITY_WINDOW_KEY not in baseline:
        missing.append(f"baseline_metrics: {POPULARITY_WINDOW_KEY!r}")
    quality = results.get("prediction_quality_metrics")
    if isinstance(quality, dict):
        for block, key in (("overall", DISPLAY_BINS_KEY),
                           ("overall", THRESHOLD_SWEEP_KEY),
                           ("per_item", DISPLAY_BINS_KEY)):
            if key not in (quality.get(block) or {}):
                missing.append(f"prediction_quality_metrics: {block}.{key}")
    if missing:
        raise ValueError(
            f"Landed before #536, these results lack what the report reads "
            f"instead of computing: {'; '.join(missing)}. Re-compute them — "
            f"`python -m recsys_tfb evaluation --from-node {LEDGER_RERUN_NODE}`, "
            f"with the flags of the run that wrote them (e.g. "
            f"--post-training) — then re-draw."
        )


#: The totals of the row facts' overview that a declared optional role lands
#: (``metrics_spark.compute_dataset_overview``): ``occasion`` the time x
#: entity combinations, ``event`` the (query group, item) combinations.
OCCASION_TOTAL_KEY = "n_time_entity_combos"
EVENT_TOTAL_KEY = "n_query_group_item_combos"


def refuse_pre_landed_role_totals(facts: dict, schema: Mapping) -> None:
    """Raise when ``occasion`` / ``event`` is declared but the row facts'
    overview landed before it counted what they add to the totals
    (:data:`OCCASION_TOTAL_KEY`, :data:`EVENT_TOTAL_KEY`).

    Same refusal as ``refuse_pre_landed_reads`` and for the same reason: no
    setting changed, so the fingerprint passes, and the row would vanish from
    the page without a word. Needs the schema, which that function is not
    given, so the section that prints the totals calls this one. A role not
    declared is not asked for.
    """
    totals = (facts.get("dataset_overview") or {}).get("totals") or {}
    missing = [f"evaluation_row_facts: dataset_overview.totals.{key!r} "
               f"({role} declared)"
               for role, key in (("occasion", OCCASION_TOTAL_KEY),
                                 ("event", EVENT_TOTAL_KEY))
               if schema.get(role) and key not in totals]
    if missing:
        raise ValueError(
            f"Landed before #537, these results lack what the report reads "
            f"instead of computing: {'; '.join(missing)}. Re-compute them — "
            f"`python -m recsys_tfb evaluation --from-node {LEDGER_RERUN_NODE}`, "
            f"with the flags of the run that wrote them (e.g. "
            f"--post-training) — then re-draw."
        )


def read_dataset_overview(facts: dict) -> dict:
    """``facts["dataset_overview"]``, refusing a pre-#327 payload.

    ``facts`` is the row facts (``row_facts.json``, #535) or one grain of
    them; a metrics bundle written before #535 held the overview itself.

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

    An overview with *neither* spelling is left alone, and so is a dict with
    no overview at all.
    """
    overview = facts.get("dataset_overview", {}) or {}
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


def count_items(facts: dict) -> int:
    """``dataset_overview.totals.n_items`` of the row facts, ``0`` when absent.

    ``facts`` is the row facts or one grain of them (``row_facts.json``,
    #535). ``0`` means "unknown", not "no items". It is what the macro-count
    line under an item grain's tables is measured against
    (``metric_tables.macro_count_note``: printed only when the macro averaged
    fewer); the total itself is printed once, in 核心概念. It is not the K
    ``"all"`` is looked up at — that is ``metrics.resolved_all_k``, which each
    metrics bundle records (#434).
    """
    return int((read_dataset_overview(facts).get("totals", {}) or {}).get("n_items", 0))


# =====================================================================
# 全正的 query group（#376）
# =====================================================================
#
# ``evaluation.query_filter.drop_all_positive_groups`` 只套用在讀指標結果的數字：
# 概覽、排序指標與熱門度基準段，以及名次分布段的正例平均名次；名次分布段的其他
# 圖不跟（使用者 2026-09-18 決定：新開關不延伸進診斷；當時的診斷頁已隨
# #532 拿掉）。所以開關開著時，同一份報表裡有兩批組，這一段的函式負責讓讀者
# 看得出每個數字是哪一批。開關只經
# ``result_readers.drop_all_positive_groups`` 讀；警告與佔比是列的事實每個粒度
# 落地的帳（``metrics.all_positive_accounts``，#536），log 與報表讀同一份，不會
# 各說各話。


#: 開關的設定鍵全名：印進警告與加註，讀者照著就找得到要改哪裡。
ALL_POSITIVE_SWITCH = "evaluation.query_filter.drop_all_positive_groups"

#: 全正的組為什麼值得數。不寫「AP 都是 1」或「滿分」：label 只有 0／1 時，K 小於
#: 組的列數 n 的 ``map@K``＝K/n、K 大於 n 的 ``precision@K``＝n/K，label 分級時
#: 值又不同；永遠成立的只有「排法不影響它的組層級（per-query）指標」。也不寫「每組
#: 的分數都一樣」：per-item 的 ``map_attr@K``／``recall@K``／``mean_pos`` 在 K
#: 小於列數時仍看誰排進前 K。
ALL_POSITIVE_WHY = "不管怎麼排，組層級指標 mAP／precision／recall 都一樣"


def grain_bundles(facts: dict) -> list[tuple[str, dict]]:
    """組數帳與警告逐粒度走的清單：item 粒度，有大類時再加大類粒度。

    大類的全正＝每個大類都至少有一個正例子列（大類那一輪 label＝子列最大值），
    和 item 粒度的全正是兩個不同的數，所以各一行帳、各自判斷警告。
    """
    out = [("item 粒度", facts)]
    if facts.get("category"):
        out.append(("大類粒度", facts["category"]))
    return out


def zero_positive_draw_on_test(parameters: dict) -> float | None:
    """dataset 對 test 表無正例 query group 的抽樣：監控模式回 ``None``（沒有
    dataset 抽樣，數到的是資料本身）；``--post-training`` 且 test 表帶權重欄
    （``test_carries_zero_positive_group_weight``）回保留比例 r（> 0）；不帶
    權重欄回 ``0.0``（dataset 全丟了）。

    一個判斷、兩段措辭：資料統計段的說明（``data_stats._description``）與預測品質段
    的 ``prediction_quality._kept_zero_positive_groups`` 都從這裡讀，不各寫一版分支。
    """
    if not parameters.get("post_training"):
        return None
    if not test_carries_zero_positive_group_weight(parameters):
        return 0.0
    return resolved_zero_positive_group_ratio(parameters, "test")


def scored_with_positive(metrics: dict) -> int | None:
    """The number of query groups counted into the metrics: the
    ``n_scored_queries_with_positive`` of the population ledger the metrics
    result lands (#536). The report no longer subtracts "all-positive" from
    "with a positive" itself: the run that computed the metrics recorded the
    number once, and the report and the comparison layer read the same one.
    ``None`` when the ledger lacks the key (a file from before #535, which
    the report refuses first with ``refuse_pre_ledger``)."""
    return ((metrics or {}).get(POPULATION_KEY) or {}).get(
        N_SCORED_WITH_POSITIVE_KEY)


def kept_ratio_phrase(ratio: float) -> str:
    """「test 表保留了比例 r 的無正例組」的說法，一個來源：資料統計段的說明與
    預測品質段的母體說明都印它，兩邊不會各寫一版。"""
    return f"test 表保留了比例 r＝{ratio:g} 的無正例 query group"


def unmatched_last(by_segment: dict) -> dict:
    """同一份 dict，(unmatched) 移到最後、其餘順序不動。

    它不是一個 segment，列出來是為了交代它有多少 query；每張分群表（資料統計、
    排序指標、熱門度基準段，比較報表的分群指標段）用同一個順序，讀者才不會在某張
    表把它當第一個群。
    """
    rest = {k: v for k, v in by_segment.items() if k != UNMATCHED_SEGMENT}
    if UNMATCHED_SEGMENT in by_segment:
        rest[UNMATCHED_SEGMENT] = by_segment[UNMATCHED_SEGMENT]
    return rest


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
