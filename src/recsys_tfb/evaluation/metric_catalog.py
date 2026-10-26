"""Every evaluation metric, one entry per name in one method (ADR-0031 decision 6).

The one list of which metrics exist. Readers look metrics up by name rather
than each keeping a list of its own; a section that shows only some metrics
names them here by entry (:func:`entry`), so a name it shows is one the
computation produces.

**Data only.** No computation and no Spark object: a formula says which shared
numbers it combines and how (:class:`Ratio`), and ``metrics_spark`` computes it
— as Spark columns per query group, or from each item's collected numbers. So
a report reading the catalog imports nothing that computes (ADR-0031
principle 1), and reading it pulls in neither Spark nor pandas.

**One entry is one name in one method.** The same name may stand in two
methods with two definitions; the two land in different blocks of a metrics
bundle (``overall`` against ``per_item``), so the pair (name, method) is the key.

**Kept apart from training's** ``metric_registry`` (ADR-0031 decision 6). A
number both compute under different names says so in ``training_name``; the
ones HPO selects a model by are marked here too
(:data:`MODEL_SELECTION_METRICS`, with the ones not computed yet in
:data:`MODEL_SELECTION_PENDING`), without importing the registry.

**Not in it yet: the third method of decision 7**, the score read as a
probability (the binned ``pr_auc`` / ``roc_auc`` of the
``--prediction-quality`` page). Its shared numbers are the bin table of
``evaluation/prediction_quality.aggregate_score_bins``, and ``prediction_quality``
computes the two areas from it by functions of its own. Its metrics join the
catalog with a formula shape for an area under a bin table.
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

#: The item method's shared numbers: what ``metrics_spark.
#: aggregate_item_shared_numbers`` sums over every row of the item — positive or
#: not — in the query groups the query-group method scores, before any formula
#: reads it. A positive row is one labelled 1. The ones in
#: :data:`ITEM_SHARED_NUMBERS_PER_K` exist once per K, as ``name@K``.
#:
#: The query groups without a positive are not counted: ``--post-
#: training`` evaluates the test split, which keeps none of them unless
#: ``dataset.test_zero_positive_group_ratio`` is set, and then only a sample
#: whose rows weigh ``1 / r``.
ITEM_SHARED_NUMBERS: dict[str, str] = {
    "n_top": "the item's rows ranked within the top K: the times it was "
             "recommended (per K)",
    "n_pos": "the item's positive rows",
    "hits": "positive rows ranked within the top K (per K)",
    "ap_sum": "Σ ap_contrib@K over the positive rows: the precision at the "
              "row's rank when it is within the top K, else 0 (per K)",
    "pos_sum": "Σ rank over the positive rows",
}
ITEM_SHARED_NUMBERS_PER_K = ("n_top", "hits", "ap_sum")


# :class:`Ratio` says every metric the catalog holds. A metric it cannot say
# (F1@K combines two ratios) comes with a new shape here and the code in
# ``metrics_spark`` that builds it.


@dataclass(frozen=True)
class Ratio:
    """``numerator / denominator``, both names from the method's shared
    numbers: :data:`QUERY_GROUP_SHARED_NUMBERS` per query group,
    :data:`ITEM_SHARED_NUMBERS` per item."""

    numerator: str
    denominator: str

    def __str__(self) -> str:
        return f"{self.numerator} / {self.denominator}"


@dataclass(frozen=True)
class Metric:
    """One metric: a name in one method.

    ``per_k`` metrics land as ``name@K`` for every K computed, the others as
    ``name`` (:meth:`key`). ``population`` is which rows it is computed on;
    ``display_name`` the word of a table title (``map@K · 整體``);
    ``definition`` the one sentence a report reader needs, printed in the
    definitions block under the tables (the wording of ADR-0031 decision 16's
    approved demo).
    """

    name: str
    method: str
    formula: Ratio
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
        display_name="map",
        definition=(
            "每個有正例的 query group 算 AP@K（分母是該組的正例數 R），再對 "
            "query group 等權平均。"
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
        definition="前 K 名裡的正例數 ÷ K，再對 query group 等權平均。",
    ),
    Metric(
        name="recall",
        method=QUERY_GROUP,
        formula=Ratio("hits", "total_rel"),
        per_k=True,
        grains=_QUERY_GROUP_GRAINS,
        population=_QUERY_GROUP_POPULATION,
        display_name="recall",
        definition="前 K 名裡的正例數 ÷ 該組的正例數，再對 query group 等權平均。",
    ),
    Metric(
        name="recall",
        method=ITEM,
        formula=Ratio("hits", "n_pos"),
        per_k=True,
        grains=_ITEM_GRAINS,
        population=_ITEM_POPULATION,
        display_name="per-item recall",
        definition=(
            "這個 item 的正例列裡，排進前 K 名的比例；跟每個 query group 先算的 "
            "recall@K 是不同的指標。鍵從 hit_rate@K 改名。"
        ),
    ),
    Metric(
        name="map_attr",
        method=ITEM,
        formula=Ratio("ap_sum", "n_pos"),
        per_k=True,
        grains=_ITEM_GRAINS,
        population=_ITEM_POPULATION,
        display_name="map_attr",
        definition=(
            "這個 item 是正例時，它在 AP@K 分子裡那一項的平均：排第 r 名且 "
            "r ≤ K 時是 P@r，否則是 0。"
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
        formula=Ratio("pos_sum", "n_pos"),
        per_k=False,
        grains=_ITEM_GRAINS,
        population=_ITEM_POPULATION,
        display_name="正例平均名次",
        definition="這個 item 是正例時的平均名次。鍵是 mean_pos。",
    ),
)


#: The definitions of the grains a table's rows can be, beside the metrics'
#: (a definitions block under an item table lists "Macro 平均" too):
#: ``{grain: (term, definition)}``.
GRAIN_DEFINITIONS: dict[str, tuple[str, str]] = {
    "macro": ("Macro 平均",
              "對 item 等權平均；只算正例數達到 metric.min_positives 的 item。"),
}


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


@dataclass(frozen=True)
class CompareEntry:
    """One item of a section's list of metrics to show or compare: a catalog
    entry and the grain it is read at. The lists live in the section files, as
    constants, not as settings.

    Checked when built: the catalog must hold ``(name, method)``
    (:func:`entry` raises ``KeyError``) and ``grain`` must be one of that
    entry's ``grains`` (``ValueError``), so a list cannot name a number the
    computation does not land.
    """

    name: str
    method: str
    grain: str

    def __post_init__(self) -> None:
        metric = entry(self.name, self.method)
        if self.grain not in metric.grains:
            raise ValueError(
                f"metric_catalog's {self.name!r} in method {self.method!r} is "
                f"not at grain {self.grain!r}; its grains are {metric.grains}")

    @property
    def metric(self) -> Metric:
        """The catalog entry."""
        return entry(self.name, self.method)


@dataclass(frozen=True)
class ModelSelectionMetric:
    """A metric training's HPO can select a model by
    (``metric_registry.METRIC_NAMES``), marked on the catalog entry whose
    untruncated value it equals: ``source`` at K = all (the resolved
    ``all_k``; ``name@all`` in a comparison). ``name`` is the registry's
    name."""

    name: str
    source: CompareEntry
    display_name: str
    definition: str


#: The model-selection metrics evaluation computes, in the order the reports
#: list them (ADR-0031 decision 16): each is one catalog entry's value at
#: K = all.
MODEL_SELECTION_METRICS: tuple[ModelSelectionMetric, ...] = (
    ModelSelectionMetric(
        name="mean_ap",
        source=CompareEntry("map", QUERY_GROUP, "overall"),
        display_name="overall mAP",
        definition=(
            "每個有正例的 query group 算不截斷的 AP，再對 query group 等權平均；"
            "數值等於 map@K 整體列、K＝全部那一格。HPO 的名字是 mean_ap。"
        ),
    ),
    ModelSelectionMetric(
        name="macro_per_item_map",
        source=CompareEntry("map_attr", ITEM, "macro"),
        display_name="macro per-item mAP",
        definition=(
            "每個 item 的 map_attr 不截斷，再對 item 等權平均；數值等於 "
            "map_attr@K 的 Macro 平均列、K＝全部那一格。HPO 的名字是 "
            "macro_per_item_map。"
        ),
    ),
)

#: The model-selection metrics HPO can select by but evaluation does not
#: compute yet: the two binary-prediction ones, a Spark pass each
#: (``metric_registry``'s ``POOLED_AP_PASS`` / ``PER_ITEM_AP_PASS``), left out
#: on purpose (ADR-0031 decision 16); until they are in, the reports list
#: only :data:`MODEL_SELECTION_METRICS`. Together
#: the two lists name every ``metric_registry.METRIC_NAMES`` (a test checks
#: it), so a metric added to the registry is placed in one of them.
MODEL_SELECTION_PENDING: tuple[str, ...] = (
    "pooled_average_precision",
    "macro_per_item_average_precision",
)
