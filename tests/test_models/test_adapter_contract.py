"""What every ``ModelAdapter`` owes the training pipeline, run on two of them.

One suite, parametrized over the LightGBM adapter and a test adapter that is
not LightGBM (``tests/fake_adapter.py``). A test that passes for LightGBM and
fails for the fake is a place where the interface leaks LightGBM; a test
that passes for both is a promise a third adapter can be checked against
(ADR-0030 decision 1, and #481's test seam 3).

The second half pins decision 3: the pre-run checks take the objective and
metric rules from the configured adapter, so the fake's own declarations
change what the checks say.
"""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.core.consistency import (
    ranking_objective_conflicts,
    training_algorithm_errors,
)
from recsys_tfb.models.base import ADAPTER_REGISTRY, AlgorithmRules
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from tests.fake_adapter import FAKE_ALGORITHM, FakeAdapter

ADAPTERS = {"lightgbm": LightGBMAdapter, FAKE_ALGORITHM: FakeAdapter}


@pytest.fixture(autouse=True)
def _fake_registered(monkeypatch):
    monkeypatch.setitem(ADAPTER_REGISTRY, FAKE_ALGORITHM, FakeAdapter)


@pytest.fixture(params=sorted(ADAPTERS))
def algorithm(request):
    return request.param


@pytest.fixture
def adapter(algorithm):
    return ADAPTER_REGISTRY[algorithm]()


def _arrays(seed=0, n=300, flip=False):
    """A learnable binary problem; ``flip`` reverses what the label means."""
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, 3))
    y = (X[:, 0] + 0.3 * rng.normal(size=n) > 0).astype(float)
    return X, (1.0 - y) if flip else y


NAMES = ["f0", "f1", "f2"]
PARAMS = {"objective": "binary", "verbose": -1, "learning_rate": 0.2, "seed": 3}


def _build(adapter, X, y, **kw):
    return adapter.build_train_data(
        X, y, feature_names=NAMES, categorical_features=[], **kw)


def _fitted(adapter, *, weight=None, num_iterations=20):
    X, y = _arrays()
    adapter.train(_build(adapter, X, y, weight=weight), dict(PARAMS),
                  num_iterations=num_iterations)
    return adapter


# -- the interface --------------------------------------------------------------


def test_declares_its_rules(algorithm):
    assert isinstance(ADAPTER_REGISTRY[algorithm].rules, AlgorithmRules)


def test_data_read_back_from_disk_trains_the_same_model(adapter, algorithm, tmp_path):
    """Save then load is not allowed to change what a fit learns — the HPO
    search trains on what the cache wrote, the refit on arrays in memory."""
    X, y = _arrays()
    in_memory = ADAPTER_REGISTRY[algorithm]()
    in_memory.train(_build(in_memory, X, y, weight=np.ones(len(y))), dict(PARAMS),
                    num_iterations=10)

    path = str(tmp_path / "train.bin")
    adapter.save_train_data(_build(adapter, X, y), path)
    adapter.train(adapter.load_train_data(path, weight=np.ones(len(y))),
                  dict(PARAMS), num_iterations=10)

    np.testing.assert_allclose(adapter.predict(X), in_memory.predict(X))


def test_refuses_to_store_weights(adapter, tmp_path):
    """Weights are applied on read, never stored. LightGBM would otherwise
    keep a stored weight when read back with all-ones (it maps all-ones to
    "no weights" and leaves the file's field alone), so a later unweighted
    run would train on an earlier run's weights (#318)."""
    X, y = _arrays()
    with pytest.raises(ValueError, match="per-row weights"):
        adapter.save_train_data(
            _build(adapter, X, y, weight=np.full(len(y), 2.0)),
            str(tmp_path / "train.bin"))


@pytest.mark.parametrize("n_weights", [0, 299, 301])
def test_refuses_weights_that_do_not_match_the_rows(adapter, tmp_path, n_weights):
    X, y = _arrays()
    path = str(tmp_path / "train.bin")
    adapter.save_train_data(_build(adapter, X, y), path)
    with pytest.raises(ValueError, match="binary holds 300 rows"):
        adapter.load_train_data(path, weight=np.ones(n_weights))


def test_weights_applied_on_read_change_the_model(adapter, algorithm, tmp_path):
    X, y = _arrays()
    path = str(tmp_path / "train.bin")
    adapter.save_train_data(_build(adapter, X, y), path)
    heavy = np.where(X[:, 1] > 0, 25.0, 1.0)

    plain = ADAPTER_REGISTRY[algorithm]()
    plain.train(plain.load_train_data(path, weight=np.ones(len(y))), dict(PARAMS),
                num_iterations=10)
    adapter.train(adapter.load_train_data(path, weight=heavy), dict(PARAMS),
                  num_iterations=10)

    assert not np.allclose(plain.predict(X), adapter.predict(X))


def test_early_stops_on_the_validation_data(adapter):
    """A validation set whose labels mean the opposite gets worse from the
    first round, so the fit stops long before the cap and reports an early
    round — the number ``refit_on_full`` retrains for."""
    X, y = _arrays()
    Xv, yv = _arrays(seed=1, flip=True)
    train = _build(adapter, X, y)
    adapter.train(
        train, dict(PARAMS), num_iterations=200, early_stopping_rounds=3,
        valid_data=_build(adapter, Xv, yv, reference=train),
    )
    assert 1 <= adapter.best_iteration < 200


def test_no_early_stopping_reports_round_zero(adapter):
    _fitted(adapter)
    assert adapter.best_iteration == 0


def test_train_leaves_the_params_alone(adapter):
    X, y = _arrays()
    params = {**PARAMS, "num_iterations": 99, "early_stopping_rounds": 7}
    before = dict(params)
    adapter.train(_build(adapter, X, y), params, num_iterations=5)
    assert params == before


def test_a_saved_model_scores_like_the_fitted_one(adapter, algorithm, tmp_path):
    _fitted(adapter)
    X, _ = _arrays(seed=5)
    path = str(tmp_path / "model.txt")
    adapter.save(path)
    loaded = ADAPTER_REGISTRY[algorithm]()
    loaded.load(path)
    np.testing.assert_allclose(loaded.predict(X), adapter.predict(X))
    assert loaded.feature_names() == NAMES


def test_predict_before_training_raises(adapter):
    with pytest.raises(RuntimeError):
        adapter.predict(np.zeros((2, 3)))


# -- scoring a table (decision 2) ----------------------------------------------
#
# The entry both scoring pipelines call: a table in, one score per row out.
# The default is shared by every adapter that does not override it, so the
# promise checked here is the one a composite model's override has to keep.

#: The artifact's full feature list holds a column the fitted model never saw,
#: in between two it did: the model, not the artifact, decides what is read.
SCORING_PREPROCESSOR = {
    "feature_columns": ["f0", "f_unused", "f1", "f2"],
    "categorical_columns": [],
    "category_mappings": {},
}
SCORING_PARAMS = {"schema": {"columns": {
    "time": "t", "entity": ["e"], "item": "i", "label": "label"}}}


def _table(seed=5, n=40):
    """A table holding the feature columns plus others, and the array behind it."""
    X, _ = _arrays(seed=seed, n=n)
    table = pd.DataFrame({
        "e": [f"e{k}" for k in range(n)],
        "f0": X[:, 0], "f_unused": np.zeros(n), "f1": X[:, 1], "f2": X[:, 2],
    })
    return table, X


def test_scoring_columns_are_the_models_own_features(adapter):
    _fitted(adapter)
    assert adapter.scoring_columns(SCORING_PREPROCESSOR) == adapter.feature_names()
    assert adapter.scoring_columns(SCORING_PREPROCESSOR) == NAMES


def test_score_is_predict_on_the_models_matrix(adapter):
    """Equal to predicting on the array the table was built from, and to
    predicting on the matrix ``pdf_to_X`` builds for the model's view."""
    from recsys_tfb.io.extract import pdf_to_X
    from recsys_tfb.models.feature_view import model_feature_view

    _fitted(adapter)
    table, X = _table()
    scores = adapter.score(table, SCORING_PREPROCESSOR, SCORING_PARAMS)

    assert scores.shape == (len(table),)
    np.testing.assert_array_equal(scores, adapter.predict(X))
    np.testing.assert_array_equal(scores, adapter.predict(pdf_to_X(
        table, model_feature_view(adapter, SCORING_PREPROCESSOR), SCORING_PARAMS)))


def test_extra_columns_and_column_order_do_not_change_the_score(adapter):
    _fitted(adapter)
    table, _ = _table()
    reordered = table[["f2", "e", "f1", "f_unused", "f0"]].assign(
        noise=np.arange(len(table)), label=1)

    np.testing.assert_array_equal(
        adapter.score(reordered, SCORING_PREPROCESSOR, SCORING_PARAMS),
        adapter.score(table, SCORING_PREPROCESSOR, SCORING_PARAMS),
    )


# -- the pre-run checks ask the configured adapter (decision 3) ----------------


def _params(algorithm, objective, metric=None):
    algorithm_params = {"objective": objective}
    if metric is not None:
        algorithm_params["metric"] = metric
    return {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label"}},
        "training": {"algorithm": algorithm, "algorithm_params": algorithm_params},
    }


def test_a7_rejects_a_metric_the_adapter_says_cannot_rank(algorithm):
    rules = ADAPTER_REGISTRY[algorithm].rules
    objective = sorted(rules.ranking_objectives)[0]
    errors = ranking_objective_conflicts(
        _params(algorithm, objective, metric="binary_logloss"))
    assert len(errors) == 1
    assert "is not a ranking metric" in errors[0]
    assert rules.default_ranking_metric in errors[0]


def test_a7_accepts_the_adapters_own_ranking_metric(algorithm):
    rules = ADAPTER_REGISTRY[algorithm].rules
    objective = sorted(rules.ranking_objectives)[0]
    assert ranking_objective_conflicts(
        _params(algorithm, objective, metric=rules.default_ranking_metric)) == []


def test_a7_answer_changes_with_the_configured_adapter():
    """The same config, two algorithms, two verdicts, in both directions —
    the check cannot be reading one fixed table. ``fake_rank`` ranks only for
    the fake, ``lambdarank`` only for LightGBM."""
    fake_rank_on_logloss = ("fake_rank", "binary_logloss")
    assert ranking_objective_conflicts(
        _params(FAKE_ALGORITHM, *fake_rank_on_logloss)) != []
    assert ranking_objective_conflicts(
        _params("lightgbm", *fake_rank_on_logloss)) == []

    lambdarank_on_logloss = ("lambdarank", "binary_logloss")
    assert ranking_objective_conflicts(
        _params("lightgbm", *lambdarank_on_logloss)) != []
    assert ranking_objective_conflicts(
        _params(FAKE_ALGORITHM, *lambdarank_on_logloss)) == []


def test_a57_admits_every_registered_adapter(algorithm):
    assert training_algorithm_errors(_params(algorithm, "binary")) == []
