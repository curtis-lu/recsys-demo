"""Fit a real LightGBM model on in-memory arrays, through the adapter's interface.

Tests that need *a* fitted model — the diagnostics, the model file on disk —
used to call ``LightGBMAdapter.train(X, y, X_val, y_val, params)``, a numpy
shortcut production never took. The interface now takes native training data
(ADR-0030 decision 1), so this builds it the way production does and keeps
each fixture to one line.

``params`` may carry ``num_iterations`` / ``early_stopping_rounds`` the way
those fixtures always wrote them; they become ``train()``'s arguments, with
the defaults the old shortcut used.
"""

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


def fit_lightgbm(
    X,
    y,
    params: dict,
    *,
    feature_names=None,
    categorical_features=(),
    X_val=None,
    y_val=None,
) -> LightGBMAdapter:
    """A ``LightGBMAdapter`` fitted on ``X`` / ``y``.

    ``feature_names`` defaults to LightGBM's own ``Column_<i>``, which is what
    a Dataset built from a bare ndarray reports. With ``X_val`` the fit
    early-stops on it.
    """
    params = dict(params)
    num_iterations = params.pop("num_iterations", 500)
    early_stopping_rounds = params.pop("early_stopping_rounds", 50)
    names = (
        list(feature_names) if feature_names is not None
        else [f"Column_{i}" for i in range(X.shape[1])]
    )
    adapter = LightGBMAdapter()
    train = adapter.build_train_data(
        X, y, feature_names=names, categorical_features=list(categorical_features))
    valid = None
    if X_val is not None:
        valid = adapter.build_train_data(
            X_val, y_val, feature_names=names,
            categorical_features=list(categorical_features), reference=train)
    adapter.train(
        train, params,
        num_iterations=num_iterations,
        early_stopping_rounds=early_stopping_rounds,
        valid_data=valid,
    )
    return adapter
