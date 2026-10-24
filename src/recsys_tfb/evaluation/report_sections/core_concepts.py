"""核心概念: the one quantity every other section aggregates, defined once
with a worked example, and the page's totals — the one place the whole
data's counts are printed (ADR-0031 decision 16 item 12)."""

from __future__ import annotations

from collections.abc import Mapping

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.report import ReportSection
from recsys_tfb.evaluation.report_sections.shared import (
    ALL_POSITIVE_SWITCH,
    ALL_POSITIVE_WHY,
    EVENT_TOTAL_KEY,
    OCCASION_TOTAL_KEY,
    grain_bundles,
    read_dataset_overview,
    refuse_pre_landed_role_totals,
    scored_with_positive,
)
from recsys_tfb.evaluation.result_readers import (
    ALL_POSITIVE_KEY,
    ALL_POSITIVE_SHARE_KEY,
    ALL_POSITIVE_WARN_SHARE,
    ALL_POSITIVE_WARNS_KEY,
    N_WITH_POSITIVE_KEY,
    drop_all_positive_groups,
    format_share,
    resolved_all_k,
)
from recsys_tfb.report.types import (
    Bullets,
    Cell,
    Column,
    Formula,
    Note,
    Paragraph,
    Row,
    Table,
)

#: The results this section reads, by catalog name (ADR-0031 principle 1):
#: the row facts for the totals and the all-positive warning, the metrics
#: for what "all" resolved to and, with the all-positive switch on, how many
#: query groups they scored.
READS = ("evaluation_row_facts", "evaluation_metrics")

#: The heading, its English and the question the section answers; the
#: report's table of contents prints ``TITLE — QUESTION`` (ADR-0031
#: decision 16).
TITLE = "核心概念"
TITLE_EN = "Concepts"
QUESTION = "報表的數字從哪個量算出來？總量多少？"

_FORMULA = (
    "AP@k = (1 / R) · Σ(i=1..k) rel_i · P@i\n"
    "  P@i = 前 i 名中的正例數 / i　（前 i 名的精確率）\n"
    "  rel_i = 第 i 名是正例則 1、否則 0\n"
    "  R = 該 query 的正例總數（total_rel）；分母是 R、不是 min(k, R)——"
    "k < R 時 AP@k 追不到 1（頂多 k 個正例能進前 k、卻除以較大的 R）"
)

#: The two optional roles, in the words of the demo: what declaring each
#: changes in the query group (decision 16 item 14). Only this section says
#: it; no other section's tables change with them.
_EVENT_NOTE = (
    "宣告了 event：同一個 query group 的同一個 item 可以有好幾列（例如好幾次"
    "曝光），每一列各自排出名次，所以「全部」是一個 query group 裡最多的候選"
    "列數，不是 item 數。"
)
_OCCASION_NOTE = (
    "宣告了 occasion：query group 多了 occasion 這一層，同一個 entity 同一期"
    "可以有好幾組。指標對 query group 等權平均，所以每個 occasion 一票。分群"
    "照樣依 entity 的屬性切，不照 occasion 切。"
)

#: The label of the all-positive row, indented under the groups holding a
#: positive (a full-width space, as the demo).
_ALL_POSITIVE_ROW = "　其中全正的"


def build(results: Mapping[str, dict], parameters: dict) -> ReportSection:
    """核心概念（地基）：講清一次原子量，後面各段都是它換切法；再印全資料的總量。

    presentation §一.2：定義 ＋ 用一個具體數字走一遍 ＋「下面每個數字＝它加總到
    什麼粒度」。不在各段重複這條定義（會漂移）。總量只在這裡印（決定 16 第
    12 項）：別段要用就寫「核心概念的總量」。

    角色名一律走 ``core.schema.get_schema``，不在這裡自備一份預設表。這一段
    印的是讀者自己的欄名，第二份預設表只會在某一天跟 ``core/schema`` 的那份
    分岔——而分岔的樣子是報表印著「``prod_name``」、其餘各段印著使用者真正的
    欄名，兩邊都不報錯（#326）。``get_schema`` 也負責把 ``entity`` 正規化成
    list，所以這裡不必再判斷型別。

    The numbers in the text come from the landed results, never from the
    conf or a constant: "@N is all" is the metrics' recorded ``all_k``
    (``result_readers.resolved_all_k``), the category's its own bundle's.
    """
    schema = get_schema(parameters)
    facts = results["evaluation_row_facts"]
    metrics = results["evaluation_metrics"]
    time_col = schema["time"]
    entity_str = "×".join(schema["entity"])
    item_col = schema["item"]
    label_col = schema["label"]
    occasion = schema.get("occasion", [])
    event = schema.get("event", [])
    category_facts = facts.get("category") or None
    drop = drop_all_positive_groups(parameters)
    refuse_pre_landed_role_totals(facts, schema)

    # The query group, in the reader's own columns; with `occasion` declared
    # it gains the occasion columns — without them the page's own definition
    # would name a unit no metric on it is computed in (#428).
    query_str = f"{time_col} × {entity_str}" + (
        f" × {'×'.join(occasion)}" if occasion else "")
    description = (
        f"一個 query group＝一組 {query_str}。組內的候選 {item_col} 依模型分數"
        f"由高到低排出名次；{label_col}=1 的是正例。下面每個數字都是這個排序"
        "結果加總到不同粒度。"
    )

    blocks: list = [
        Paragraph(description, muted=True),
        Formula(_FORMULA),
        Bullets(_bullets(item_col, metrics, category_facts, bool(event), drop)),
    ]
    if event:
        blocks.append(Paragraph(_EVENT_NOTE, muted=True))
    if occasion:
        blocks.append(Paragraph(_OCCASION_NOTE, muted=True))
    blocks.append(_totals_table(
        facts, metrics, time_col, drop,
        entity_str=entity_str, item_col=item_col))
    return ReportSection(
        title=TITLE,
        description="",
        title_en=TITLE_EN,
        question=QUESTION,
        key="core_concepts",
        blocks=blocks,
    )


def _bullets(item_col: str, metrics: dict, category_facts: dict | None,
             event: bool, drop: bool) -> tuple[str, ...]:
    """The worked example, the hand check, the grains, which groups each kind
    of metric counts, and what "all" is this run."""
    grains = (
        "粒度：整體＝跨 query group 等權平均；各 item＝把 AP 歸因到正例所屬的 "
        f"{item_col}，再對 item 等權平均（Macro）；各分群＝依分群欄分組平均"
    )
    grains += ("；大類＝把 item 併成大類後重排再算。" if category_facts
               else "。")
    which_groups = (
        "有正例、而且不是全正的 query group（" + ALL_POSITIVE_SWITCH + " 開著）"
        if drop else "有正例的 query group")
    all_k = resolved_all_k(metrics)
    if event:
        all_sentence = (f"表頭寫實際的 K。宣告了 event，全部＝一個 query group "
                        f"裡最多的候選列數，本次是 @{all_k}")
    else:
        all_sentence = (f"表頭寫實際的 K。本次每個 query group 最多 {all_k} 個"
                        f"候選，所以 @{all_k} 就是全部、不截斷")
    category_metrics = metrics.get("category")
    if category_facts and category_metrics:
        n_categories = (read_dataset_overview(category_facts).get(
            "totals", {}) or {}).get("n_items")
        all_sentence += (f"；大類只有 {n_categories} 類，大類的表最多到 "
                         f"@{resolved_all_k(category_metrics)}")
    return (
        f"例：某 query 有 4 個候選 {item_col}、R=2 個正例，排名後正例落在第 1、"
        "第 3 名。",
        "P@1 = 1/1 = 1.0、P@2 = 1/2 = 0.5、P@3 = 2/3 ≈ 0.667。",
        "AP@3 = (1/2)·(1·1.0 + 0·0.5 + 1·0.667) ≈ 0.83；"
        "AP@1 = (1/2)·(1·1.0) = 0.50——分母固定為 R=2，k=1 只納入第 1 名那個"
        "正例的貢獻、仍除以 2。",
        "手算核對：K=1 時每組的 AP@1 = rel_1 / R，正好等於 recall@1，所以整體"
        "的 map@1 會等於 recall@1。",
        grains,
        f"只看組內先後的指標（map、precision、recall、map_attr）只算{which_groups}"
        "；把每一列當成二元預測的指標（例如 PR-AUC）算全部候選列。",
        all_sentence + "。",
    )


def _totals_table(facts: dict, metrics: dict, time_col: str,
                  drop: bool, *, entity_str: str, item_col: str) -> Table:
    """「總量」: the whole data's counts, read off the row facts as landed;
    the all-positive warning of each grain under it.

    With the all-positive switch on, the all-positive rows say they were
    left out, and one more row gives the groups the metrics scored — off the
    metrics' own ledger (``shared.scored_with_positive``), not subtracted
    here (#376, #536).

    With ``occasion`` declared, one more row counts the ``time × entity``
    combinations; with ``event`` declared, one more counts the
    ``(query group, item)`` combinations (decision 16 item 14). Both are read
    off the overview's totals as landed
    (``metrics_spark.compute_dataset_overview``), not counted here; a file
    landed without them is refused before this runs
    (``shared.refuse_pre_landed_role_totals``).
    """
    totals = read_dataset_overview(facts).get("totals", {}) or {}
    excluded = "（已排除）" if drop else ""
    rows = [
        ("全部 query group", facts.get("n_queries")),
        ("有正例的 query group", facts.get(N_WITH_POSITIVE_KEY)),
        (_ALL_POSITIVE_ROW + excluded, facts.get(ALL_POSITIVE_KEY)),
    ]
    if drop:
        rows.append(("　其中算進指標的", scored_with_positive(metrics)))
    rows.append(("無正例的 query group", facts.get("n_excluded_queries")))
    category = facts.get("category") or None
    if category:
        rows.append((f"大類粒度裡全正的 query group{excluded}",
                     category.get(ALL_POSITIVE_KEY)))
    if OCCASION_TOTAL_KEY in totals:
        rows.append((f"{time_col} × {entity_str} 的組合",
                     totals[OCCASION_TOTAL_KEY]))
    if EVENT_TOTAL_KEY in totals:
        rows.append((f"(query group, {item_col}) 的組合",
                     totals[EVENT_TOTAL_KEY]))
    rows += [
        ("候選列", totals.get("n_rows")),
        ("正例數", totals.get("n_positives")),
        ("entity 數", totals.get("n_entities")),
        ("item 數", totals.get("n_items")),
    ]
    if category:
        rows.append(("大類數", (read_dataset_overview(category).get(
            "totals", {}) or {}).get("n_items")))
    rows += [
        (f"{time_col} 數", totals.get("n_snap_dates")),
        # The value column is a count; this one cell is text, "@6" (decision
        # 16 item 7: the header and this cell write the K with the @).
        ("全部＝@幾", Cell(f"@{resolved_all_k(metrics)}", kind="text")),
    ]
    return Table(
        "總量", "項目", (Column("值", "count"),),
        tuple(Row(label, (value,)) for label, value in rows),
        notes=tuple(_all_positive_warnings(facts)),
    )


def _all_positive_warnings(facts: dict) -> list[Note]:
    """⚠ under the totals: one line for each grain whose landed
    ``all_positive_warns`` is true.

    Whether to warn is each grain's landed account
    (``metrics.all_positive_accounts``, #536; the log reads the same), read
    here, not recomputed. It states the numbers and the setting, not whether
    the share is high, and names no other section (decision 16).
    """
    out = []
    for grain, bundle in grain_bundles(facts):
        if not bundle[ALL_POSITIVE_WARNS_KEY]:
            continue
        # The flag is true only when the share is known, so the share and
        # both counts are there.
        out.append(Note(
            f"全正 query group（{grain}）：有正例的 query group 裡，全正的佔 "
            f"{format_share(bundle[ALL_POSITIVE_SHARE_KEY])}"
            f"（{bundle[ALL_POSITIVE_KEY]:,}／{bundle[N_WITH_POSITIVE_KEY]:,} "
            f"組），超過 {ALL_POSITIVE_WARN_SHARE:.0%}。這些組{ALL_POSITIVE_WHY}"
            "，照樣算進這些指標；模型與熱門度排序在這些組的組層級指標也相同。"
            f"要排除它們，設 {ALL_POSITIVE_SWITCH}: true。",
            warn=True))
    return out
