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
from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARN_SHARE,
    ALL_POSITIVE_WARNS_KEY,
    CELLS,
    DIFF,
    DISPLAY_BINS_KEY,
    DISPLAY_HEADLINE_SHOWN,
    ENTRIES,
    N_SCORED_WITH_POSITIVE_KEY,
    N_WITH_POSITIVE_KEY,
    POPULARITY_WINDOW_KEY,
    POPULATION_KEY,
    REFERENCE,
    THIS,
    THRESHOLD_SWEEP_KEY,
    drop_all_positive_groups,
    metric_params,
)
from recsys_tfb.evaluation.segment_keys import UNMATCHED_SEGMENT


def k_to_lookup(k, all_k: int) -> int | str:
    """Convert display label to metric dict key suffix (the cut's lists are
    ``steps/display_ks``', landed; ``"all"`` stays a label in them).

    ``all_k`` comes from the bundle being read (``metrics.resolved_all_k``),
    never from counting its items (#434).
    """
    if k == "all":
        return all_k
    return k


def headline_column(parameters: dict, display_ks: dict, where: str) -> str:
    """How the headline is truncated and where it sits in the metrics
    section's map_attr table, as one phrase both sections show (``where``
    names that section from the caller's side). Names the column only when
    that table shows it — the K cut lands that (``resolve_display_ks``'
    ``DISPLAY_HEADLINE_SHOWN``, off the same list the table draws), so the
    note cannot name a column the table dropped."""
    mk = metric_params(parameters)["k"]
    if mk is None:
        return (f"不截斷（metric.k 未設），即{where} B 塊「{MACRO_LABEL}」列的 "
                "map_attr@all")
    if landed_value(display_ks, DISPLAY_KS, "evaluation_metrics",
                    DISPLAY_HEADLINE_SHOWN):
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


#: The catalog names of the comparison of the model with its baseline and of
#: the K cut (#536), as the main report reads them; then the comparison of
#: this run's side with the compared source and its K cut, as the comparison
#: report reads them.
BASELINE_COMPARISON = "baseline_comparison"
DISPLAY_KS = "evaluation_display_ks"
SOURCE_COMPARISON = "source_comparison"
COMPARISON_DISPLAY_KS = "comparison_display_ks"

#: The node that lands each result :func:`landed_value` reads: where a refusal
#: sends the reader. Each exists in every mode that draws from the result:
#: the comparison's two in ``--compare`` and ``--compare-only`` alike.
LANDED_BY = {
    BASELINE_COMPARISON: "compare_to_baseline",
    DISPLAY_KS: "resolve_display_ks",
    SOURCE_COMPARISON: "compare_to_source",
    COMPARISON_DISPLAY_KS: "resolve_comparison_display_ks",
}


def landed_value(result: dict | None, name: str, *path):
    """``result[path[0]][path[1]]…`` of the landed result ``name``; raises
    ``ValueError`` naming the node to re-run from when it is not there.

    For the results the report reads instead of computing (#536, ADR-0031
    principle 1): the comparison's cells and the K cut's lists. A value
    missing there means the file was landed by an older version of its node
    (or it is the stub of a section the settings now draw), and drawing on
    would print blank cells or raise a bare ``KeyError`` with no way out —
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


def compared_value(cells: dict, key: str, side: str):
    """One side (or the difference) of the cell ``key``; ``None`` where the
    comparison holds no such cell."""
    return (cells.get(key) or {}).get(side)


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
                f"--from-node compute_row_facts`, with the flags of the run "
                f"that wrote it (e.g. --post-training) — then re-draw. Drawn "
                f"as-is, every per-item recall cell would be blank."
            )


def refuse_pre_ledger(name: str, bundle: dict | None) -> None:
    """Raise when the result ``name`` is a metrics bundle written before
    #535: it carries no population ledger (``metrics.POPULATION_KEY``).

    **Why refuse**: the ledger is where each bundle says what ``"all"``
    resolved to (``metrics.resolved_all_k``), and a file this old also still
    holds the row facts that now land on their own — the report reads those
    from ``row_facts.json``, which that run never wrote. The baseline's stub
    (``{"enabled": false}``, section off) is not a bundle and passes.
    """
    if not isinstance(bundle, dict) or bundle.get("enabled") is False:
        return
    if POPULATION_KEY not in bundle:
        raise ValueError(
            f"{name} predates #535: it carries no {POPULATION_KEY!r} ledger, "
            f"and its run landed no row_facts.json. Re-compute it — `python "
            f"-m recsys_tfb evaluation --from-node compute_row_facts`, with "
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
    the prediction-quality display tables and threshold sweep. A stub
    (section off, ``{"enabled": false}``) carries none of them and passes.

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
    if (isinstance(baseline, dict) and baseline.get("enabled") is not False
            and POPULARITY_WINDOW_KEY not in baseline):
        missing.append(f"baseline_metrics: {POPULARITY_WINDOW_KEY!r}")
    quality = results.get("prediction_quality_metrics")
    if isinstance(quality, dict) and quality.get("enabled") is True:
        for block, key in (("overall", DISPLAY_BINS_KEY),
                           ("overall", THRESHOLD_SWEEP_KEY),
                           ("per_item", DISPLAY_BINS_KEY)):
            if key not in (quality.get(block) or {}):
                missing.append(f"prediction_quality_metrics: {block}.{key}")
    if missing:
        raise ValueError(
            f"Landed before #536, these results lack what the report reads "
            f"instead of computing: {'; '.join(missing)}. Re-compute them — "
            f"`python -m recsys_tfb evaluation --from-node compute_row_facts`, "
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
    #535). ``0`` means "unknown", not "no items". It is the item count the
    reports show — in the macro-coverage titles and the comparison report's
    metadata. It is not the K ``"all"`` is looked up at — that is
    ``metrics.resolved_all_k``, which each metrics bundle records (#434).
    """
    return int((read_dataset_overview(facts).get("totals", {}) or {}).get("n_items", 0))


def compared_macro_coverage_suffix(
    macro_block: dict, macro_items: dict, n_items: int
) -> str:
    """Title suffix disclosing both sides' macro coverage on an M/B/Δ table
    drawn off a comparison (bug 5, ADR-0020).

    An item with no positive this period has no ``per_item`` cell, and
    ``evaluation.metric.min_positives`` drops more; both are silent, so the
    macro's denominator drifts month to month with no visible cause.
    Disclosure, not a definition change: each side's N is the length of its
    landed ``macro_items`` (the comparison's annotation) — the metrics
    computed that list, the report does not apply ``min_positives`` again.
    The suffix appears exactly when :func:`compared_per_item_table` draws the
    Macro row: both sides had a macro, an empty one included.

    Generic "M"/"B" labels so the one wording reads right for the model
    against its baseline (main report) and against a compared source
    (comparison report).
    """
    if not both_present(macro_block):
        return ""
    return _macro_coverage_mb(len(macro_items[THIS]),
                              len(macro_items[REFERENCE]), n_items)


def _macro_coverage_mb(n_a: int, n_b: int, n_items: int) -> str:
    """The M/B coverage wording, one copy for both suffixes."""
    return f"（參與 macro 的 item 數 M {n_a}／B {n_b}／全部 {n_items}）"


# =====================================================================
# 全正的 query group（#376）
# =====================================================================
#
# ``evaluation.query_filter.drop_all_positive_groups`` 只套用在衡量指標（含頭號
# 指標）與熱門度 baseline；per-item 細部拆解不跟（使用者 2026-09-18 決定：新開關
# 不延伸進診斷；當時的診斷頁已隨 #532 拿掉）。所以開關開著時，同一份報表裡有兩批
# 組，這一段的函式負責讓讀者看得出每個數字是哪一批。開關只經
# ``result_readers.drop_all_positive_groups`` 讀；警告與佔比是列的事實每個粒度
# 落地的帳（``metrics.all_positive_accounts``，#536），log 與報表讀同一份，不會
# 各說各話。


#: 開關的設定鍵全名：印進警告與加註，讀者照著就找得到要改哪裡。
ALL_POSITIVE_SWITCH = "evaluation.query_filter.drop_all_positive_groups"


def format_share(share: float) -> str:
    """An all-positive share as the log and the report print it: ``5.3%``.

    One decimal, unless the share is above
    ``result_readers.ALL_POSITIVE_WARN_SHARE`` and one decimal rounds it down
    to the threshold — 1004 of 10000 printed as ``10.0%`` beside "above
    10%". Then more decimals, at most four, until the printed value is above
    the threshold too (``10.04%``). One function, so the log and the report
    cannot print the same share two ways (#376). Formatting, so it lives on
    the report's side (#536); the evaluation nodes' log imports it from here.
    """
    for digits in range(1, 5):
        text = f"{share:.{digits}%}"
        if (not share > ALL_POSITIVE_WARN_SHARE
                or float(text[:-1]) / 100 > ALL_POSITIVE_WARN_SHARE):
            break
    return text

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

    一個判斷、兩段措辭：組數帳的 ``ranking_metrics._zero_positive_origin`` 與資料概況／預測
    品質段的 :func:`kept_zero_positive_groups_note` 都從這裡讀，不各寫一版分支。
    """
    if not parameters.get("post_training"):
        return None
    if not test_carries_zero_positive_group_weight(parameters):
        return 0.0
    return resolved_zero_positive_group_ratio(parameters, "test")


def scored_with_positive(metrics: dict) -> int | None:
    """算進衡量指標的組數：指標結果母體帳落地的
    ``n_scored_queries_with_positive``（#536）。報表不再拿「有正例的 − 全正的」
    自己減：同一個數由算指標的那一次記下，報表與比較層讀同一份。帳缺這個鍵
    （#535 之前的檔，報表先 ``refuse_pre_ledger`` 擋下）回 ``None``。"""
    return ((metrics or {}).get(POPULATION_KEY) or {}).get(
        N_SCORED_WITH_POSITIVE_KEY)


def query_count_rows(facts: dict, metrics: dict, parameters: dict) -> dict:
    """「規模／分母」與「本次執行事實」共用的 query 組數列。

    開關關時與 #376 之前逐字相同。開關開時 ``n_excluded_queries`` 只數無正例的
    組、實際被排除的還有全正的組，「有正例的 query 數」也不再是算進衡量指標的
    組數：標籤改成說清楚是哪一批，並另列全正與算進衡量指標兩列（兩列相加＝有
    正例的 query 數）。``facts`` 與 ``metrics`` 是同一個粒度的列的事實與指標
    結果；算進衡量指標的那一列讀後者的母體帳（:func:`scored_with_positive`）。
    """
    n_queries = facts.get("n_queries")
    n_zero = facts.get("n_excluded_queries")
    n_pos = facts.get(N_WITH_POSITIVE_KEY)
    if not drop_all_positive_groups(parameters):
        return {
            "全部 query 數 n_queries": n_queries,
            "有正例的 query 數": n_pos,
            "排除 query 數 n_excluded_queries": n_zero,
        }
    n_all = facts.get(ALL_POSITIVE_KEY)
    return {
        "全部 query 數 n_queries": n_queries,
        "有正例的 query 數": n_pos,
        f"其中全正而排除的 query 數 {ALL_POSITIVE_KEY}": n_all,
        "其中算進衡量指標的 query 數（有正例的 − 全正的）":
            scored_with_positive(metrics),
        "無正例而排除的 query 數 n_excluded_queries": n_zero,
    }


def kept_ratio_phrase(ratio: float) -> str:
    """「test 表保留了比例 r 的無正例組」的說法，一個來源：資料概況的說明與
    衡量指標段的組數帳（#376）都印它，兩邊不會各寫一版。"""
    return f"test 表保留了比例 r＝{ratio:g} 的無正例 query group"


def kept_zero_positive_groups_note(facts: dict, parameters: dict) -> str:
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
    n_kept = facts.get("n_excluded_queries")
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


def compared_per_item_table(
    per_item: dict,
    macro: dict,
    ks: list,
    metric_key: str,
    col_base_fmt: str,
) -> pd.DataFrame:
    """Per-item M/B/Δ table drawn off a comparison's ``per_item`` and
    ``macro_by_item`` blocks (``steps/comparison_layer``, #536): the
    comparison holds both sides and their difference, so nothing is computed
    here.

    Rows = the Macro row (``MACRO_LABEL``) when both sides had a macro
    (:func:`both_present`), then the items in the comparison's order (this
    side's, then the ones only the reference has). Columns =
    ``f"{base} M"``, ``f"{base} B"``, ``f"{base} Δ"`` for each ``k``, where
    ``base = col_base_fmt.format(k=k)``; ``"all"`` reads the cell pairing the
    two sides' own ``"all"`` (``name@all``). A cell the comparison lacks, or
    a side it lacks, is blank, and so is a difference it did not take
    (ADR-0020 bug 4).
    """
    def _row(cells: dict) -> dict:
        row: dict = {}
        for k in ks:
            key = f"{metric_key}@{k}"
            base = col_base_fmt.format(k=k)
            row[f"{base} M"] = compared_value(cells, key, THIS)
            row[f"{base} B"] = compared_value(cells, key, REFERENCE)
            row[f"{base} Δ"] = compared_value(cells, key, DIFF)
        return row

    data: dict = {}
    if both_present(macro):
        data[MACRO_LABEL] = _row(macro[CELLS])
    for item, cells in per_item[CELLS].items():
        data[item] = _row(cells)
    return pd.DataFrame(data).T


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
