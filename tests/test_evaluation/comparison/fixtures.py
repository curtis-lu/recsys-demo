"""The comparison report's inputs as a run lands them (#537), for the tests of
``evaluation/comparison``.

* :func:`bundle` — a ``compute_all_metrics`` bundle at one grain: every block
  the comparison layer compares (``overall``, ``per_item``,
  ``macro_avg.by_item``, ``per_segment``) and the population ledger, every
  per-K key at each K of the grid ``1 .. all_k`` (``_resolve_k_grids`` gives
  both K axes ``k_values`` ∪ {all_k}). The numbers are made up; the Macro is
  the equal-weight mean of the ``macro_items`` (``macro_average``'s default
  path), so it is the shape the computation lands.
* :func:`inputs` — what ``generate_comparison_report`` reads: the comparison
  and its K cut computed off two bundles by the **real nodes**
  (``compare_to_source``, ``resolve_comparison_display_ks``), never written
  by hand; the coverage and this side's row facts in their producers' shape
  (``restrict_to_common``, ``compute_compare_metrics``), as the existing
  comparison tests write them, with the stamp every comparison artifact
  carries.
"""
from __future__ import annotations

import copy
from html.parser import HTMLParser

ITEMS = ("item_A", "item_B", "item_C")
SEGMENTS = ("seg_1", "seg_2")
CATEGORIES = ("cat_X", "cat_Y")
SEGMENT_COLUMN = "segment"

#: The two kinds of compared source (``compare_sources``), as the CLI stages
#: them under ``evaluation.compare``.
EXTERNAL = {
    "kind": "external_hive", "table": "other_team.scores", "label": "外部分數",
    "columns": {"as_of": "dt", "acct": "acct_key", "branch": "br",
                "prod_name": "item_code", "score": "pred_score"},
    "prod_mapping": {"ext_a": "item_A", "ext_b": "item_B", "ext_c": "item_C"},
    "unmapped_policy": "drop",
}
OTHER_MODEL = {"kind": "model_version", "model_version": "20260131_v6",
               "label": "上一版", "source": "ranked_predictions"}

THIS_MODEL_VERSION = "20260301_v7"


def _value(base: float, offset: float, k: int) -> float:
    return round(base + offset + k / 100, 6)


def bundle(base: float = 0.5, *, all_k: int | None = None, items=ITEMS,
           macro_items=None, scored: int = 10,
           segment_columns=(SEGMENT_COLUMN,), segments=SEGMENTS,
           extra_item_metrics=(), category: dict | None = None) -> dict:
    """A metrics bundle at one grain (module docstring). ``all_k`` defaults
    to the item count; above it is ``event`` rows (the widest query group).
    ``macro_items`` defaults to every item. ``segment_columns=()`` computes
    no segment. ``extra_item_metrics`` adds per-K item metrics by name."""
    all_k = len(items) if all_k is None else all_k
    ks = range(1, all_k + 1)
    macro_items = list(items) if macro_items is None else list(macro_items)
    item_names = ("map_attr", "recall", *extra_item_metrics)

    def group_metrics(offset: float) -> dict:
        return {f"{name}@{k}": _value(base, offset + j / 10, k)
                for k in ks for j, name in enumerate(("map", "precision",
                                                     "recall"))}

    per_item = {}
    for i, item in enumerate(items):
        cell = {f"{name}@{k}": _value(base, 0.03 * i + j / 20, k)
                for k in ks for j, name in enumerate(item_names)}
        cell["mean_pos"] = 1.5 + i
        cell["n_pos"] = 5 + i
        per_item[item] = cell
    keys = [k for k in per_item[items[0]] if k != "n_pos"] if items else []
    by_item = {key: sum(per_item[i][key] for i in macro_items) / len(macro_items)
               for key in keys} if macro_items else {}
    out = {
        "overall": group_metrics(0.0),
        "per_item": per_item,
        "macro_avg": {"by_item": by_item},
        "per_segment": ({s: group_metrics(0.05 * (n + 1))
                         for n, s in enumerate(segments)}
                        if segment_columns else {}),
        "population": {
            "all_k": all_k, "macro_items": sorted(macro_items),
            "n_scored_queries_with_positive": scored,
            "n_scored_queries_without_positive": 0,
            "segment_columns": list(segment_columns),
        },
    }
    if category is not None:
        out["category"] = category
    return out


def category_bundle(base: float = 0.4, **kwargs) -> dict:
    """The category grain of a bundle: the categories are its items."""
    kwargs.setdefault("items", CATEGORIES)
    return bundle(base, **kwargs)


def params(compare: dict | None = None, *, compare_k=(1, 3, "all"),
           categories: bool = False, occasion=None,
           drop_all_positive: bool | None = None) -> dict:
    """Run parameters: a schema in the reader's own column names (not the
    example deployment's), the evaluated date, the display K, the category
    switch and the compared source."""
    columns = {"time": "as_of", "entity": ["acct", "branch"],
               "item": "prod_name"}
    if occasion:
        columns["occasion"] = occasion
    p = {
        "model_version": THIS_MODEL_VERSION,
        "schema": {"columns": columns},
        "evaluation": {
            "snap_date": "2026-01-31",
            "report": {"display": {"compare_k": list(compare_k)}},
            "item_categories": {"enabled": categories},
            "compare": copy.deepcopy(compare or EXTERNAL),
        },
    }
    if drop_all_positive is not None:
        p["evaluation"]["query_filter"] = {
            "drop_all_positive_groups": drop_all_positive}
    return p


def landed(result: dict, p: dict, node: str = "compute_compare_metrics") -> dict:
    """``result`` as ``node``, its producer, lands it under ``p``: with the
    fingerprint and the compare source every comparison artifact carries."""
    from recsys_tfb.pipelines.evaluation.steps.compare_sources import (
        compare_source_identity,
    )
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
        fingerprint,
    )

    return {**result, "config_fingerprint": fingerprint(p, node),
            "compare_source": compare_source_identity(p)}


def coverage(p: dict, **override) -> dict:
    """``restrict_to_common``'s coverage (``nodes._restrict_to_common``),
    unstamped: four items on this side, three on the other, three common."""
    src = p["evaluation"]["compare"]
    out = {
        "kind_a": "model_version",
        "model_version_a": p["model_version"],
        "source_a": "enriched_eval_predictions",
        "kind_b": src.get("kind", ""),
        "model_version_b": src.get("model_version", "n/a"),
        "table_b": src.get("table", "n/a"),
        "n_query_group_A_full": 4800, "n_query_group_B_full": 4608,
        "n_query_group_common": 4600,
        "n_item_A_full": 4, "n_item_B_full": 3, "n_item_common": 3,
        "dropped_items_A": ["item_D"], "dropped_items_B": [],
    }
    out.update(override)
    return out


def row_facts_a(n_items: int = len(ITEMS),
                n_categories: int | None = None) -> dict:
    """This side's row facts on the common universe (``compute_row_facts``
    shape, the part the comparison report reads: the item counts)."""
    rf = {"dataset_overview": {"totals": {"n_items": n_items}},
          "n_queries": 100, "n_excluded_queries": 0,
          "n_all_positive_queries": 0, "widest_group_rows": n_items}
    if n_categories is not None:
        rf["category"] = {"dataset_overview": {"totals": {
            "n_items": n_categories}}}
    return rf


def inputs(this: dict, reference: dict, p: dict, *, cov: dict | None = None,
           facts: dict | None = None) -> dict:
    """What ``generate_comparison_report`` reads, by catalog name, landed
    under ``p``: the comparison of ``this`` with ``reference`` and its cut
    by the real nodes, the coverage and this side's row facts stamped."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        make_compare_to_source_node,
        make_resolve_comparison_display_ks_node,
    )

    comparison = make_compare_to_source_node(compare_only=False)(
        landed(this, p), landed(reference, p), p)
    cut = make_resolve_comparison_display_ks_node(compare_only=False)(
        comparison, p)
    if facts is None:
        n_cat = (len(this["category"]["per_item"]) if this.get("category")
                 else None)
        facts = row_facts_a(len(this["per_item"]), n_cat)
    return {
        "compare_coverage": landed(cov if cov is not None else coverage(p), p,
                                   "restrict_to_common"),
        "compare_row_facts_a": landed(facts, p),
        "source_comparison": comparison,
        "comparison_display_ks": cut,
    }


def read(module, results: dict, p: dict):
    """``module``'s section built from what it declares (``READS``) — the
    way ``comparison/report.py`` hands it."""
    from recsys_tfb.evaluation.report_sections.shared import results_read_by

    return module.build(results_read_by(module.__name__, module.READS,
                                        results), p)


def tables(section) -> list:
    from recsys_tfb.report.types import Table

    return [b for b in section.blocks if isinstance(b, Table)]


def table(section, title: str):
    found = [t for t in tables(section) if t.title == title]
    assert len(found) == 1, [t.title for t in tables(section)]
    return found[0]


def notes(section) -> list[str]:
    """The ⚠ lines and plain lines of the section, under its tables or after
    them, as text."""
    from recsys_tfb.report.types import Note

    out = [b.text for b in section.blocks if isinstance(b, Note)]
    for t in tables(section):
        out += [n.text for n in t.notes]
    return out


def column_groups(t) -> list[tuple]:
    """``(group, label)`` of each column of a table."""
    return [(c.group, c.label) for c in t.columns]


def row(t, label: str):
    found = [r for r in t.rows if r.label == label]
    assert len(found) == 1, [r.label for r in t.rows]
    return found[0]


class _SectionText(HTMLParser):
    """The visible text of each ``div.section`` of a page, by its id."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text: dict[str, str] = {}
        self._depth = 0
        self._open: list[tuple[str, int]] = []
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip += 1
            return
        if tag == "div":
            self._depth += 1
            a = dict(attrs)
            if "section" in (a.get("class") or "").split() and a.get("id"):
                self._open.append((a["id"], self._depth))
                self.text.setdefault(a["id"], "")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip -= 1
            return
        if tag == "div":
            if self._open and self._open[-1][1] == self._depth:
                self._open.pop()
            self._depth -= 1

    def handle_data(self, data):
        if self._open and not self._skip:
            self.text[self._open[-1][0]] += data


def section_ids(html: str) -> list[str]:
    """The ids of the page's sections, in page order."""
    parser = _SectionText()
    parser.feed(html)
    return list(parser.text)
