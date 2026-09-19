"""The metric diagnoses must not depend on the order of the sample's rows.

The diagnosis sample reaches the driver through ``toPandas``, whose row order
Spark does not promise: change the parallelism (``local[*]`` → ``local[2]``, or
a different cluster) and the same rows arrive in a different order (#352). Every
diagnosis output below used to follow that order somewhere — tied scores ranked
by arrival, bootstrap clusters numbered by first appearance, top-N examples
picked by first occurrence (#355).

The fixture is built to hit those places: scores come from a three-value set so
most queries hold ties, and several (positive, suppressor) pairs carry the same
allocated gap, so the top-N example cut falls inside a tie. The down-sampled
stratum's weight is 1/0.37, not a round number: integer weights add up to the
same float in any order, so they would hide a sum taken in row order. And the
item runs as a string and as an integer whose numeric order is not its string
order, because the diagnoses rank on the raw item value.
"""

import math

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.diagnosis.metric.config_shift._compute import (
    compute as config_shift,
)
from recsys_tfb.diagnosis.metric.item_ability._compute import (
    compute as item_ability,
)
from recsys_tfb.diagnosis.metric.suppression._compute import (
    compute as suppression,
)
from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci

PARAMS = {
    "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                           "item": "prod_name", "label": "label",
                           "score": "score"}},
    "dataset": {
        "sample_group_keys": ["prod_name", "label"],
        "sample_ratio": 1.0,
        "sample_ratio_overrides": {"ccard_ins|0": 0.5},
    },
    "training": {"sample_weight_keys": [], "sample_weights": {}},
    "evaluation": {"diagnosis": {
        "sample": {"seed": 42},
        "ci": {"enabled": True, "n_boot": 30},
        "config_shift": {"enabled": True},
        "item_ability": {"enabled": True, "top_n": 30},
        "suppression": {"enabled": True, "top_examples": 5},
    }},
}

ITEMS = {
    "str": ("ccard_ins", "exchange_fx", "fund_bond", "fund_stock"),
    # "10" < "100" < "2" < "7" as strings; 2 < 7 < 10 < 100 as numbers.
    "int": (2, 10, 100, 7),
}


def _tied_sample(item_type: str = "str") -> pd.DataFrame:
    items = ITEMS[item_type]
    rng = np.random.default_rng(3)
    rows = []
    for c in range(30):
        labels = (rng.random(len(items)) < 0.35).astype(int)
        if labels.sum() == 0:
            labels[rng.integers(0, len(items))] = 1
        for item, label in zip(items, labels):
            s = float(rng.choice([0.2, 0.5, 0.8]))
            rows.append({
                "snap_date": "2026-01-31", "cust_id": f"c{c:02d}",
                "prod_name": item, "label": int(label),
                "score_uncalibrated": s, "score": s,
                "stratum": "hash_ratio" if c % 3 else "take_all",
                "inclusion_weight": 1 / 0.37 if c % 3 else 1.0,
            })
    return pd.DataFrame(rows)


DIAGNOSES = {
    "metric_ci": lambda pdf: bootstrap_per_item_ci(pdf, PARAMS),
    "config_shift": lambda pdf: config_shift((pdf, {"n_queries": 30}), PARAMS),
    "item_ability": lambda pdf: item_ability((pdf, {"n_queries": 30}), PARAMS),
    "suppression": lambda pdf: suppression((pdf, {"n_queries": 30}), PARAMS),
}


def _assert_same(a, b, path="$"):
    """Same structure, same values — floats to the last bit.

    Bit-for-bit on purpose: the acceptance run compares the JSON a
    ``local[*]`` and a ``local[2]`` evaluation write, byte for byte. A float
    summed in row order drifts in its last bits (``item_ability`` did, via the
    per-query mean), and that is a byte difference too.
    """
    if isinstance(a, dict):
        assert isinstance(b, dict) and list(a) == list(b), path
        for key in a:
            _assert_same(a[key], b[key], f"{path}.{key}")
    elif isinstance(a, (list, tuple)):
        assert isinstance(b, (list, tuple)) and len(a) == len(b), path
        for i, (x, y) in enumerate(zip(a, b)):
            _assert_same(x, y, f"{path}[{i}]")
    elif isinstance(a, float) and isinstance(b, float) and math.isnan(a):
        assert math.isnan(b), f"{path}: {a!r} != {b!r}"
    else:
        assert a == b, f"{path}: {a!r} != {b!r}"


@pytest.mark.parametrize("item_type", list(ITEMS))
@pytest.mark.parametrize("name", list(DIAGNOSES))
def test_output_does_not_depend_on_the_order_of_the_rows(name, item_type):
    run = DIAGNOSES[name]
    sample = _tied_sample(item_type)
    expected = run(sample)
    rng = np.random.default_rng(0)
    for trial in range(3):
        shuffled = sample.iloc[rng.permutation(len(sample))].reset_index(drop=True)
        _assert_same(expected, run(shuffled), f"{name}#{trial}")
