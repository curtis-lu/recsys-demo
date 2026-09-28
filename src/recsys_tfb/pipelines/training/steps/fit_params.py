"""The hyperparameters one fit trains with, stacked one way for every fit.

An HPO trial and the ``refit_on_full`` retrain both train the configured
algorithm; they differ only in where the chosen hyperparameters come from —
the trial's sample, or the winning trial's. The stacking lives here once
because two copies drift in silence: the refit would train under params the
search never tried and still be reported under the search's hyperparameters.
"""

from __future__ import annotations

from recsys_tfb.models.base import AlgorithmRules


def fit_params(parameters: dict, rules: AlgorithmRules, chosen: dict) -> dict:
    """``training.algorithm_params`` -> ``random_seed`` -> ``chosen``; a later
    key wins.

    The metric is filled from the algorithm's rules for a ranking objective
    that sets none (``AlgorithmRules.default_metric_for_objective``): left to
    the library it could be a binary metric, which makes ranking early
    stopping silently meaningless. Filled on a copy, because ``parameters`` is
    written verbatim to ``manifest.json``.

    The round cap and the early-stopping patience are not in the result:
    ``ModelAdapter.train`` takes them as arguments.
    """
    algorithm_params = dict(parameters["training"].get("algorithm_params", {}))
    metric = rules.default_metric_for_objective(
        algorithm_params.get("objective"), algorithm_params.get("metric")
    )
    if metric:
        algorithm_params["metric"] = metric
    return {
        **algorithm_params,
        "seed": parameters.get("random_seed", 42),
        **chosen,
    }
