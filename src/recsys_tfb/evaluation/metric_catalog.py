"""Every evaluation metric, one entry per name in one method (ADR-0031 decision 6).

The one list of which metrics exist. The Spark metrics
(``metrics_spark.compute_per_query_metrics`` / ``aggregate_per_item``), the
main report's sections and the comparison report read it by name rather than
each keeping a list of its own; a section that shows only some metrics names
them here by entry (:func:`entry`), so a name it shows is one the computation
produces.

**Data only.** No computation and no Spark object: a formula says which shared
numbers it combines and how (:class:`Ratio`, :class:`MeanOverPositiveRows`), and
``metrics_spark`` turns that into Spark columns. So a report reading the
catalog imports nothing that computes (ADR-0031 principle 1), and reading it
pulls in neither Spark nor pandas.

**One entry is one name in one method.** The same name may stand in two
methods with two definitions; the two land in different blocks of a metrics
bundle (``overall`` against ``per_item``), so the pair (name, method) is the key.

**Kept apart from training's** ``metric_registry`` (ADR-0031 decision 6). A
number both compute under different names says so in ``training_name``.

**Not in it yet: the third method of decision 7**, the score read as a
probability (the binned ``pr_auc`` / ``roc_auc`` of the prediction-quality
section). ``evaluation/prediction_quality.py`` computes those by functions of
its own, with no shared numbers a formula here could name; they join the
catalog when that method gets them.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Computed per query group, then averaged over query groups (ADR-0031
#: decision 7, first method).
QUERY_GROUP = "query_group"
#: One value per item, averaged over items into the macro (decision 7,
#: second method).
ITEM = "item"

#: The query-group method's shared numbers: what ``compute_per_query_metrics``
#: sums inside each query group at each K before any formula reads it.
QUERY_GROUP_SHARED_NUMBERS: dict[str, str] = {
    "ap_sum": "Σ ap_contrib@K over the group's rows — the numerator of AP@K",
    "hits": "positive rows ranked within the top K",
    "total_rel": "the group's positive rows",
    "K": "the cut-off",
}

#: The item method's per-row columns (``metrics_spark.add_row_contributions``
#: and ``rank_within_query``): a formula averages one of them over the rows
#: where the item is a positive. ``@K`` columns exist once per K.
ITEM_ROW_COLUMNS: dict[str, str] = {
    "top_k": "1 when the row ranks within the top K, else 0 (per K)",
    "ap_contrib": "the row's share of its query group's AP@K numerator: the "
                  "precision at its rank when it is a positive within the top "
                  "K, else 0 (per K)",
    "pos": "the row's rank in its query group",
}


# Two formula shapes cover every metric the catalog holds. A metric neither
# can say (F1@K combines two metrics; a per-item precision needs rows the item
# method does not keep yet) comes with a new shape here and the code in
# ``metrics_spark`` that builds it.


@dataclass(frozen=True)
class Ratio:
    """``numerator / denominator`` per query group, both names from
    :data:`QUERY_GROUP_SHARED_NUMBERS`."""

    numerator: str
    denominator: str

    def __str__(self) -> str:
        return f"{self.numerator} / {self.denominator}"


@dataclass(frozen=True)
class MeanOverPositiveRows:
    """The mean of one :data:`ITEM_ROW_COLUMNS` column over the rows where the
    item is a positive."""

    column: str

    def __str__(self) -> str:
        return f"mean({self.column}) over the item's positive rows"


@dataclass(frozen=True)
class Metric:
    """One metric: a name in one method.

    ``per_k`` metrics land as ``name@K`` for every K computed, the others as
    ``name`` (:meth:`key`). ``grains`` lists where the bundle carries it;
    ``population`` which rows it is computed on; ``display_name`` what a report
    calls it; ``definition`` the one sentence a report reader needs.
    """

    name: str
    method: str
    formula: Ratio | MeanOverPositiveRows
    per_k: bool
    grains: tuple[str, ...]
    population: str
    display_name: str
    definition: str
    training_name: str | None = None

    def key(self, k: int | str | None = None) -> str:
        """The bundle key this metric lands under at ``k``."""
        return f"{self.name}@{k}" if self.per_k else self.name


_QUERY_GROUP_GRAINS = ("overall", "per_segment", "macro", "category")
_QUERY_GROUP_POPULATION = (
    "有正例的 query group（沒有正例時 AP 沒有定義）；"
    "evaluation.query_filter.drop_all_positive_groups 開著時，再排除全正的組"
)
_ITEM_GRAINS = ("per_item", "per_item_segment", "macro", "category")
_ITEM_POPULATION = (
    "該 item 為正例的列；只看 query group 那種算法算進的 query group"
)

#: Every metric, in the order the bundle writes them: within a method, the
#: per-K ones in this order at each K, the others before them.
METRICS: tuple[Metric, ...] = (
    Metric(
        name="map",
        method=QUERY_GROUP,
        formula=Ratio("ap_sum", "total_rel"),
        per_k=True,
        grains=_QUERY_GROUP_GRAINS,
        population=_QUERY_GROUP_POPULATION,
        display_name="mAP",
        definition=(
            "每個 query group 的 AP@K（前 K 名裡每個正例位置的精確率加總，"
            "除以該組的正例數），再對 query group 等權平均"
        ),
        training_name=(
            "mean_ap：等於這裡不截斷（K＝全部）的整體值，前提是 "
            "evaluation.query_filter.drop_all_positive_groups 沒打開"
        ),
    ),
    Metric(
        name="precision",
        method=QUERY_GROUP,
        formula=Ratio("hits", "K"),
        per_k=True,
        grains=_QUERY_GROUP_GRAINS,
        population=_QUERY_GROUP_POPULATION,
        display_name="precision",
        definition="每個 query group 前 K 名裡的正例數除以 K，再對 query group 平均",
    ),
    Metric(
        name="recall",
        method=QUERY_GROUP,
        formula=Ratio("hits", "total_rel"),
        per_k=True,
        grains=_QUERY_GROUP_GRAINS,
        population=_QUERY_GROUP_POPULATION,
        display_name="recall",
        definition=(
            "每個 query group 前 K 名裡的正例數除以該組的正例數，再對 query "
            "group 平均"
        ),
    ),
    Metric(
        name="hit_rate",
        method=ITEM,
        formula=MeanOverPositiveRows("top_k"),
        per_k=True,
        grains=_ITEM_GRAINS,
        population=_ITEM_POPULATION,
        display_name="recall",
        definition="該 item 為正例的列裡，排進前 K 名的比例（per-item 的 recall）",
    ),
    Metric(
        name="map_attr",
        method=ITEM,
        formula=MeanOverPositiveRows("ap_contrib"),
        per_k=True,
        grains=_ITEM_GRAINS,
        population=_ITEM_POPULATION,
        display_name="map_attr",
        definition=(
            "該 item 為正例的每一列對所屬 query group 的 AP@K 的貢獻（排名 r "
            "在前 K 名時是前 r 名的精確率，否則 0），再對這些列平均"
        ),
        training_name=(
            "macro_per_item_map：等於這裡 Macro 列不截斷（K＝全部）的值，前提是 "
            "evaluation.metric 用預設值（weight_alpha、min_positives、"
            "shrinkage_k），而且 evaluation.query_filter.drop_all_positive_groups "
            "沒打開"
        ),
    ),
    Metric(
        name="mean_pos",
        method=ITEM,
        formula=MeanOverPositiveRows("pos"),
        per_k=False,
        grains=_ITEM_GRAINS,
        population=_ITEM_POPULATION,
        display_name="mean_pos",
        definition="該 item 為正例的列的平均名次，越小越好",
    ),
)


def metrics_of(method: str) -> tuple[Metric, ...]:
    """The entries of ``method``, in catalog order."""
    return tuple(m for m in METRICS if m.method == method)


def entry(name: str, method: str) -> Metric:
    """The entry for ``name`` in ``method``; raises when the catalog has none,
    so a section cannot show a metric nothing computes."""
    for m in METRICS:
        if m.name == name and m.method == method:
            return m
    raise KeyError(
        f"metric_catalog has no {name!r} in method {method!r}; it holds "
        f"{[(m.name, m.method) for m in METRICS]}"
    )
