"""Every evaluation metric, one entry per name in one method (ADR-0031 decision 6).

The one list of which metrics exist. Readers look metrics up by name rather
than each keeping a list of its own; a section that shows only some metrics
names them here by entry (:func:`entry`), so a name it shows is one the
computation produces.

**Data only.** No computation and no Spark object: a formula says which shared
numbers it combines and how (:class:`Ratio`, :class:`Expr`), and
``metrics_spark`` computes it — as Spark columns per query group, or from
each item's collected numbers. So
a report reading the catalog imports nothing that computes (ADR-0031
principle 1), and reading it pulls in neither Spark nor pandas.

**One entry is one name in one method.** The same name may stand in two
methods with two definitions; the two land in different blocks of a metrics
bundle (``overall`` against ``per_item``), so the pair (name, method) is the key.

**Kept apart from training's** ``metric_registry`` (ADR-0031 decision 6). A
number both compute under different names says so in ``training_name``; the
ones HPO selects a model by are marked here too
(:data:`MODEL_SELECTION_METRICS` on the main report,
:data:`MODEL_SELECTION_ON_PREDICTION_QUALITY` on the ``--prediction-quality``
page), without importing the registry.

**Four methods.** Decision 7's three — per query group, per item, the score
read as a probability over a bin table (:data:`SCORE_BINS`) — and decision 16
item 3's fourth, average precision on the raw scores (:data:`RAW_SCORE`). The
per-item metrics whose denominator holds negatives are a method of their own
(:data:`ITEM_EVERY_GROUP`): same arithmetic as :data:`ITEM`, counted over
every query group (#541's first comment). The last three are the
``--prediction-quality`` mode's (ADR-0032).
"""

from __future__ import annotations

from dataclasses import dataclass

#: Computed per query group, then averaged over query groups (ADR-0031
#: decision 7, first method).
QUERY_GROUP = "query_group"
#: One value per item, averaged over items into the macro (decision 7,
#: second method).
ITEM = "item"
#: The item method over **every** query group, the ones without a positive
#: included and weighted: the per-item metrics whose denominator counts
#: negatives (precision@K, F1@K). Kept apart from :data:`ITEM`, whose shared
#: numbers count only the query groups the query-group method scores, so the
#: ranking report's per-item block never computes them on that population
#: (#541's first comment, 〈怎麼呈現〉 point 2).
ITEM_EVERY_GROUP = "item_every_group"
#: Each row read as one binary prediction, the score binned (decision 7,
#: third method; ADR-0024).
SCORE_BINS = "score_bins"
#: Average precision on the raw scores, each row one binary prediction
#: (decision 16 item 3): a Spark pass of its own, no shared numbers.
RAW_SCORE = "raw_score"

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

#: :data:`ITEM_EVERY_GROUP`'s shared numbers: what ``prediction_quality.
#: aggregate_item_top_k`` sums over every row of the item, each times its
#: weight (``1 / r`` on a kept query group without a positive under
#: ``--post-training``, else 1). ``n_top`` and ``hits`` exist once per K.
ITEM_EVERY_GROUP_SHARED_NUMBERS: dict[str, str] = {
    "n_top": "the item's rows ranked within the top K, weighted (per K)",
    "hits": "positive rows ranked within the top K (per K)",
    "n_pos": "the item's positive rows",
}

#: :data:`SCORE_BINS`' shared numbers: one row of the bin table
#: ``prediction_quality.aggregate_score_bins`` sums per (item, bin) — or per
#: bin, overall — each count weighted.
SCORE_BIN_SHARED_NUMBERS: dict[str, str] = {
    "bin": "the fine bin, by score; every row in it counts as one tied score",
    "n": "rows in the bin",
    "n_pos": "positive rows in the bin",
}


# Four formula shapes: :class:`Ratio`, one shared number over another;
# :class:`Expr`, arithmetic over them for a metric that is not a ratio;
# :class:`BinArea`, an area under a bin table; :class:`OwnPass`, no formula. An
# entry in one of the first three needs no Spark code of its own
# (``metrics_spark`` evaluates the first two, ``prediction_quality.bin_areas``
# the third).


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
class Expr:
    """Arithmetic over the method's shared numbers: built from
    :func:`shared`, plain numbers, ``+``, ``*``, ``/`` and :func:`at_most`,
    e.g. ``2 * shared("hits") / (shared("K") + shared("total_rel"))``.

    A tree, not a string: ``op`` is ``"shared"`` (``args`` holds the name),
    ``"constant"`` (the number), or one of ``+ * / min`` on two sub-trees. A
    division by 0 leaves the metric undefined, as a :class:`Ratio`'s does.
    """

    op: str
    args: tuple

    def __add__(self, other) -> Expr:
        return Expr("+", (self, _expr(other)))

    def __radd__(self, other) -> Expr:
        return Expr("+", (_expr(other), self))

    def __mul__(self, other) -> Expr:
        return Expr("*", (self, _expr(other)))

    def __rmul__(self, other) -> Expr:
        return Expr("*", (_expr(other), self))

    def __truediv__(self, other) -> Expr:
        return Expr("/", (self, _expr(other)))

    def __rtruediv__(self, other) -> Expr:
        return Expr("/", (_expr(other), self))


def _expr(value) -> Expr:
    return value if isinstance(value, Expr) else Expr("constant", (value,))


def shared(name: str) -> Expr:
    """The shared number ``name`` of the entry's method."""
    return Expr("shared", (name,))


def at_most(value, cap) -> Expr:
    """``min(value, cap)``: ``at_most(shared("hits"), 1)`` is 1 when the
    group has a hit, else 0."""
    return Expr("min", (_expr(value), _expr(cap)))


@dataclass(frozen=True)
class BinArea:
    """The area under one curve of a bin table (:data:`SCORE_BIN_SHARED_NUMBERS`),
    the bins walked from the highest score down, each bin one tied score.
    ``curve`` is ``"pr"`` — Σ over bins of n_pos × precision at the bin's lower
    edge, over all positives: scikit-learn's step-rule average precision on
    the bin index — or ``"roc"`` — the ROC area, a tie within a bin counting
    half. Undefined without a positive, and for ``"roc"`` without a negative.
    """

    curve: str

    def __str__(self) -> str:
        return f"area under the {self.curve} curve of the bin table"


@dataclass(frozen=True)
class OwnPass:
    """No formula: the metric is the result of a Spark pass of its own,
    ``computed_by`` (decision 16 item 3). Marked here so readers find it by
    name; adding one is not only an entry."""

    computed_by: str

    def __str__(self) -> str:
        return f"computed by {self.computed_by}"


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
    formula: Ratio | Expr | BinArea | OwnPass
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
_ITEM_EVERY_GROUP_GRAINS = ("per_item", "macro")
_SCORE_BIN_GRAINS = ("overall", "per_item", "macro")
_EVERY_ROW_POPULATION = (
    "全部候選列，沒有正例的 query group 也算；--post-training 時留下的沒有"
    "正例的組每列乘 1／r"
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
        name="f1",
        method=QUERY_GROUP,
        formula=2 * shared("hits") / (shared("K") + shared("total_rel")),
        per_k=True,
        grains=_QUERY_GROUP_GRAINS,
        population=_QUERY_GROUP_POPULATION,
        display_name="f1",
        definition=(
            "每個 query group 的 precision@K 與 recall@K 的調和平均（前 K 名沒有"
            "正例時是 0），再對 query group 等權平均；不是先平均 precision 與 "
            "recall 再取調和平均。"
        ),
    ),
    Metric(
        name="hit_rate",
        method=QUERY_GROUP,
        formula=at_most(shared("hits"), 1),
        per_k=True,
        grains=_QUERY_GROUP_GRAINS,
        population=_QUERY_GROUP_POPULATION,
        display_name="hit_rate",
        definition=(
            "前 K 名裡至少有一個正例的 query group 佔幾成。舊版 metrics.json "
            "per-item 區塊裡的 hit_rate@K 是 per-item recall，跟這個不同。"
        ),
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
    Metric(
        name="precision",
        method=ITEM_EVERY_GROUP,
        formula=Ratio("hits", "n_top"),
        per_k=True,
        grains=_ITEM_EVERY_GROUP_GRAINS,
        population=_EVERY_ROW_POPULATION,
        display_name="per-item precision",
        definition=(
            "這個 item 排進前 K 名的那些列裡，正例佔幾成。分母數全部的 query "
            "group，沒有正例的組也算；跟每個 query group 先算的 precision@K "
            "是不同的指標。"
        ),
    ),
    Metric(
        name="f1",
        method=ITEM_EVERY_GROUP,
        formula=2 * shared("hits") / (shared("n_top") + shared("n_pos")),
        per_k=True,
        grains=_ITEM_EVERY_GROUP_GRAINS,
        population=_EVERY_ROW_POPULATION,
        display_name="per-item F1",
        definition=(
            "這個 item 的 per-item precision@K 與 per-item recall@K 的調和平均"
            "（＝2·排進前 K 名的正例 ÷ (排進前 K 名的次數＋正例數)）。"
        ),
    ),
    Metric(
        name="pr_auc",
        method=SCORE_BINS,
        formula=BinArea("pr"),
        per_k=False,
        grains=_SCORE_BIN_GRAINS,
        population=_EVERY_ROW_POPULATION,
        display_name="PR-AUC",
        definition=(
            "分數分箱、箱內視為同分之後的 average precision。是近似值，不等於在"
            "原始分數上算的 pooled AP。隨機排序時它約等於正例佔比，所以正例佔比"
            "不同的兩個 item 不能直接比。鍵是 pr_auc。"
        ),
    ),
    Metric(
        name="roc_auc",
        method=SCORE_BINS,
        formula=BinArea("roc"),
        per_k=False,
        grains=_SCORE_BIN_GRAINS,
        population=_EVERY_ROW_POPULATION,
        display_name="ROC-AUC",
        definition=(
            "隨機挑一個正例、一個負例，正例分數比較高的機率；分數分箱、同一箱算"
            "一半。不受正例佔比影響。鍵是 roc_auc。"
        ),
    ),
    Metric(
        name="average_precision",
        method=RAW_SCORE,
        formula=OwnPass(
            "metrics_spark.compute_pooled_average_precision (overall), "
            "compute_macro_per_item_average_precision (macro)"),
        per_k=False,
        grains=("overall", "macro"),
        population=_EVERY_ROW_POPULATION,
        display_name="AP（原始分數）",
        definition=(
            "把每一列當成一次二元預測，在原始分數上算 average precision（同分算"
            "同一個門檻）。整體＝全部列一起排；Macro＝每個 item 自己排、各算一個，"
            "再對有正例的 item 等權平均。跟 PR-AUC 不同：PR-AUC 先分箱。"
        ),
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

#: The model-selection metrics on the raw scores, computed by the
#: ``--prediction-quality`` mode, not the main report's run (decision 16 item
#: 3, revised by #577): their population holds the query groups without a
#: positive. The main report's overview points to that page, and the
#: comparison report does not list them. Together with
#: :data:`MODEL_SELECTION_METRICS` they name every ``metric_registry.
#: METRIC_NAMES`` (a test checks it), so a metric added to the registry is
#: placed in one of them.
MODEL_SELECTION_ON_PREDICTION_QUALITY: tuple[ModelSelectionMetric, ...] = (
    ModelSelectionMetric(
        name="pooled_average_precision",
        source=CompareEntry("average_precision", RAW_SCORE, "overall"),
        display_name="pooled AP",
        definition=(
            "全部列在原始分數上一起排的 average precision。HPO 的名字是 "
            "pooled_average_precision。"
        ),
    ),
    ModelSelectionMetric(
        name="macro_per_item_average_precision",
        source=CompareEntry("average_precision", RAW_SCORE, "macro"),
        display_name="macro per-item AP",
        definition=(
            "每個 item 的列在原始分數上自己排、各算 average precision，再對有"
            "正例的 item 等權平均。HPO 的名字是 macro_per_item_average_precision。"
        ),
    ),
)
