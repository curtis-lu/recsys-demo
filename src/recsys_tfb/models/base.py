"""ModelAdapter ABC, the rules each algorithm declares, and the adapter registry.

The ABC is everything the training pipeline does to a model (ADR-0030
decision 1): turn arrays into this algorithm's native training data and keep
that on disk, fit with given hyperparameters while early-stopping on a
validation set, say which round early stopping picked, and save / load the
fitted model. Nothing under ``pipelines/training/`` reaches past these methods
into the library behind them, so a second algorithm is one more adapter rather
than an edit in every caller.

It is also how a table gets scored (decision 2): :meth:`ModelAdapter.score`
takes the rows and returns one score each, and
:meth:`ModelAdapter.scoring_columns` says which columns it needs, so a caller
reads those and nothing else. Training's test predictions and inference's
scores both go through it. The two have default implementations — select the
model's own features, encode, predict — because a single model needs nothing
more; a model whose matrix is not just "its features, in its order" (a
composite that routes rows by a group key) overrides them.

The native training data is opaque to callers on purpose. They hand back what
:meth:`ModelAdapter.build_train_data` / :meth:`ModelAdapter.load_train_data`
returned and never look inside it; that is what keeps a LightGBM ``Dataset``
out of the pipeline.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

import numpy as np

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True)
class AlgorithmRules:
    """What one algorithm's objectives and metrics mean to this framework.

    Declared by each adapter as :attr:`ModelAdapter.rules`, not kept in
    ``core/``: which objectives rank, which metrics can early-stop them and
    which objectives waste nothing on a query group without a positive are
    facts about one library. The pre-run checks read them from the registered
    adapter class (ADR-0030 decision 3), so the check and the training it
    guards read one table and cannot disagree.

    ``ranking_objectives`` are the learning-to-rank objectives the framework
    supports for this algorithm: those train on query groups and early-stop
    on ``ranking_metrics``. Any other objective is passed to the library
    untouched and trains row by row. There is deliberately no list of
    those: the framework never kept one, and inventing it would reject
    objectives that train today.

    ``default_ranking_metric`` is what a ranking objective early-stops on when
    ``training.algorithm_params.metric`` is unset — leaving it to the library
    could pick a binary metric, which makes ranking early stopping silently
    meaningless.

    Validated on construction, because a table that contradicts itself would
    fail in the pre-run check of whichever deployment first hit the gap.
    """

    ranking_objectives: frozenset[str]
    ranking_metrics: frozenset[str]
    default_ranking_metric: str
    zero_positive_group_dropping_objectives: frozenset[str]

    def __post_init__(self) -> None:
        if self.default_ranking_metric not in self.ranking_metrics:
            raise ValueError(
                f"default_ranking_metric {self.default_ranking_metric!r} is "
                f"not one of ranking_metrics {sorted(self.ranking_metrics)}"
            )
        stray = self.zero_positive_group_dropping_objectives - self.ranking_objectives
        if stray:
            raise ValueError(
                f"only a ranking objective has query groups to drop; "
                f"{sorted(stray)} are not in ranking_objectives"
            )

    def is_ranking_objective(self, objective: str | None) -> bool:
        """True iff ``objective`` trains on query groups."""
        return objective in self.ranking_objectives

    def objective_drops_zero_positive_groups(self, objective: str | None) -> bool:
        """True iff ``objective`` trains only on query groups holding a positive.

        Derived from ``objective``, deliberately **not** a config key: a key
        would add a state ("filter on" under an objective that learns from
        those groups) with no correct meaning.

        Named for its argument because the function that does the work is
        one letter away: this one answers *whether* an objective filters,
        ``core.group_utils.drop_zero_positive_groups`` performs it.
        """
        return objective in self.zero_positive_group_dropping_objectives

    def default_metric_for_objective(
        self, objective: str | None, metric: str | None
    ) -> str | None:
        """``metric``, or :attr:`default_ranking_metric` for a ranking
        objective that sets none.

        Fills the unset case only. An explicitly set metric a ranking objective
        cannot early-stop on is rejected before the run by A7
        (``core.consistency.ranking_objective_conflicts``).
        """
        if self.is_ranking_objective(objective) and not metric:
            return self.default_ranking_metric
        return metric


class ModelAdapter(ABC):
    """One algorithm, as the training pipeline uses it.

    A subclass sets :attr:`rules` and implements the abstract methods, then
    registers itself in :data:`ADAPTER_REGISTRY` under the name
    ``training.algorithm`` selects it by.
    """

    #: This algorithm's objective / metric rules. A class attribute so the
    #: pre-run checks read it off the registered class without building a model.
    rules: ClassVar[AlgorithmRules]

    # -- native training data -------------------------------------------------

    @abstractmethod
    def build_train_data(
        self,
        X: np.ndarray,
        y: np.ndarray,
        *,
        feature_names: list[str],
        categorical_features: list[str],
        group: np.ndarray | None = None,
        weight: np.ndarray | None = None,
        reference: Any = None,
    ) -> Any:
        """Arrays -> this algorithm's native training data, in memory.

        ``feature_names`` name the columns of ``X`` in order.
        ``categorical_features`` name the columns to treat as unordered
        categories; a name not in ``feature_names`` is ignored, because the
        preprocessor's list outlives a feature selection that drops columns.
        ``group`` holds per-query row counts for a ranking objective, with the
        rows already ordered so each query is one block. ``reference`` is the
        training data a validation set has to be encoded like.
        """
        ...

    @abstractmethod
    def save_train_data(self, data: Any, path: str) -> None:
        """Write native training data from :meth:`build_train_data` to ``path``.

        Raises ``ValueError`` when ``data`` carries per-row weights. Weights are
        applied on read (:meth:`load_train_data`) and never stored, because
        the file is cached under a path that says nothing about
        ``training.sample_weights`` (#318) — and a stored weight is not always
        replaced on read: LightGBM leaves the file's weights in place when
        handed an all-ones vector.
        """
        ...

    @abstractmethod
    def load_train_data(
        self, path: str, *, weight: np.ndarray, reference: Any = None
    ) -> Any:
        """Read native training data written by :meth:`save_train_data`,
        carrying this run's per-row ``weight``.

        The weight is applied on read, never stored: the file is cached under a
        path that says nothing about ``training.sample_weights`` (#318).

        Raises ``ValueError`` when ``weight`` does not hold one entry per row.
        Checked here because it is a property of the file, and because the
        failure is silent in at least one library — LightGBM discards a
        length-zero weight vector without a word.
        """
        ...

    # -- fitting --------------------------------------------------------------

    @abstractmethod
    def train(
        self,
        train_data: Any,
        params: dict,
        *,
        num_iterations: int,
        early_stopping_rounds: int = 0,
        valid_data: Any = None,
    ) -> None:
        """Fit on ``train_data``; the adapter then holds the fitted model.

        ``params`` are the algorithm's own hyperparameters and are left
        unmodified. ``num_iterations`` caps the rounds. With ``valid_data``
        and ``early_stopping_rounds > 0`` the fit stops once ``valid_data`` has
        not improved for that many rounds (train_dev, in the HPO search), and
        :attr:`best_iteration` is the round it scored best at. Without early
        stopping the fit runs every round: a refit whose round count was
        chosen already.
        """
        ...

    @property
    @abstractmethod
    def best_iteration(self) -> int:
        """The round early stopping picked in the last fit: where ``valid_data``
        scored best. ``0`` when the fit did not early-stop (no ``valid_data``,
        or ``early_stopping_rounds`` of 0) — LightGBM's own convention.

        What ``finalize_model``'s ``refit_on_full`` retrains for. Read it right
        after :meth:`train`: a model loaded from disk need not remember it,
        which is why the HPO checkpoint writes it beside the model file.
        """
        ...

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Return scores as a 1-D numpy array."""
        ...

    def feature_names(self) -> list[str] | None:
        """Return the ordered feature names expected by the fitted model."""
        return None

    # -- scoring a table ------------------------------------------------------
    #
    # `models.feature_view` and `io.extract` are imported inside these two
    # methods, not at the top: `models.feature_view` imports this module, and
    # `io.extract` at the top would break a cold
    # `import recsys_tfb.io.model_adapter_dataset` — the reason is written at
    # the top of `models/lightgbm_adapter.py`.

    def scoring_columns(self, preprocessor: dict) -> list[str]:
        """The table columns :meth:`score` reads — the caller reads these.

        The default is the model's own feature list
        (``models.feature_view.model_feature_columns``): the model, not the
        current config, knows which view of the preprocessor it was trained
        on (ADR-0011 section 5), and asking it also raises when the artifact
        cannot supply that view.

        A composite model routes each row to a sub-model by a group key that
        need not be a feature (ADR-0030 decision 2), so it overrides this to
        add that key. A caller that reads only these columns — training's
        per-partition read does — would otherwise hand :meth:`score` a table
        without it.
        """
        from recsys_tfb.models.feature_view import model_feature_columns

        return model_feature_columns(self, preprocessor)

    def score(
        self, table: "pd.DataFrame", preprocessor: dict, parameters: dict,
    ) -> np.ndarray:
        """One score per row of ``table``, in row order.

        The default selects the model's features (:meth:`scoring_columns`),
        encodes the deferred categoricals with ``preprocessor``'s mappings into
        a matrix (``io.extract.pdf_to_X``, the encoding training's own reads
        use) and calls :meth:`predict`. ``table`` may carry any other columns,
        in any order; they are not read.

        ``preprocessor`` is not the same thing from both callers: training
        passes the view ``select_features`` narrowed under *this* run's
        config, inference the full preprocessing artifact. The default does
        not care — it aligns on the model's own feature list either way — but
        an override cannot assume which one it got, and must take its columns
        from what the model recorded (ADR-0011 section 5), not from
        ``preprocessor["feature_columns"]``.

        Why the table and not a matrix: "what matrix does this model see" is
        the one question a single model and a composite answer differently,
        so it belongs behind the adapter rather than in each caller.
        """
        from recsys_tfb.io.extract import pdf_to_X
        from recsys_tfb.models.feature_view import model_feature_view

        return self.predict(
            pdf_to_X(table, model_feature_view(self, preprocessor), parameters)
        )

    # -- the fitted model on disk ---------------------------------------------

    @abstractmethod
    def save(self, filepath: str) -> None:
        """Save the model to the given filepath using the algorithm's native format."""
        ...

    @abstractmethod
    def load(self, filepath: str) -> None:
        """Load a model from the given filepath into this adapter."""
        ...

    @abstractmethod
    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        """Return {feature_name: importance_score}. kind in {"split","gain"}."""
        ...

    @abstractmethod
    def log_to_mlflow(self) -> None:
        """Log the model artifact using the algorithm's MLflow integration."""
        ...


ADAPTER_REGISTRY: dict[str, type[ModelAdapter]] = {}

#: ``training.algorithm`` when the config leaves it out. One definition for
#: every reader — the training nodes and the pre-run check — so a run cannot
#: validate one algorithm and train another.
DEFAULT_ALGORITHM = "lightgbm"


def configured_algorithm(parameters: dict) -> str | None:
    """The registry name ``training.algorithm`` selects.

    An explicit ``algorithm: null`` comes back as ``None`` rather than the
    default; the pre-run check (A57) rejects it by name, since no adapter is
    registered under it.
    """
    return (parameters.get("training") or {}).get("algorithm", DEFAULT_ALGORITHM)


def get_adapter(algorithm: str) -> ModelAdapter:
    """Create and return an adapter instance for the given algorithm name."""
    cls = ADAPTER_REGISTRY.get(algorithm)
    if cls is None:
        available = ", ".join(sorted(ADAPTER_REGISTRY.keys())) or "(none)"
        raise ValueError(
            f"Unknown algorithm '{algorithm}'. Available: {available}"
        )
    return cls()
