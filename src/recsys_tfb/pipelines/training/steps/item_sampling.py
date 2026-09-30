"""Which rows the SHAP diagnosis attributes: an item-stratified sample, a
positive-only sample, and each item's own background.

Each function returns positions (or rows) and nothing else. Why the rows are
drawn this way — a per-item floor, positives drawn apart, an item's own rows as
its background — is written where ``compute_shap_diagnostics``
(``nodes.py``) calls them. Every draw builds its own ``RandomState(seed)``, so
the order the node calls them in cannot change what any of them draws.
"""

import numpy as np
import pandas as pd

#: Upper bound on the rows in one item's interventional background. A starting
#: value, not a config key. Public because the note ``compute_shap_diagnostics``
#: writes under ``background: per_item`` states it.
BACKGROUND_CAP = 128


def stratified_item_sample(item_values, total, min_per_item, seed):
    """Population-representative sample: stratified by item, uniformly random
    within an item. Each item gets at least ``min_per_item`` rows, and all of
    its rows when it has fewer (take-all). Returns the selected positional
    indices, ascending (the dataset's order).

    ``item_values`` is each row's item (1-D array-like, dataset order). The
    result is the one the earlier version that took the whole pdf gave:
    ``pd.unique`` decides the item order, ``np.where`` gives each item's
    positions ascending, and ``rng.choice`` draws with a fixed seed. Change any
    of the three and the same seed draws other rows, with nothing to say so.
    """
    item_values = np.asarray(item_values)
    rng = np.random.RandomState(seed)
    groups = {item: np.where(item_values == item)[0]
              for item in pd.unique(item_values)}
    n_items = max(1, len(groups))
    per_item = max(int(min_per_item), total // n_items)
    selected = []
    for pos in groups.values():
        take = min(len(pos), per_item)
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)


def positive_item_sample(item_values, label_values, per_item, seed):
    """Stratified by item among the ``label == 1`` rows only: at most
    ``per_item`` rows per item, all of them when it has fewer.

    Returns the selected positional indices, ascending (the dataset's order).
    Used for the positive profile's targeted sample, drawn apart from the
    item-stratified sample so a sparse positive class still gets coverage.
    """
    item_values = np.asarray(item_values)
    label_values = np.asarray(label_values)
    pos_all = np.where(label_values == 1)[0]
    if pos_all.size == 0:
        return np.array([], dtype=int)
    rng = np.random.RandomState(seed)
    pos_items = item_values[pos_all]
    selected = []
    for item in pd.unique(pos_items):
        pos = pos_all[pos_items == item]
        take = min(len(pos), int(per_item))
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)


def per_item_background(X_item, seed):
    """The item's own sub-population (the rows of the foreground sample that
    belong to the item) as its background; above ``BACKGROUND_CAP`` rows, a
    fixed-seed ``RandomState`` draws that many without replacement."""
    n = len(X_item)
    if n <= BACKGROUND_CAP:
        return X_item
    rng = np.random.RandomState(seed)
    idx = rng.choice(n, size=BACKGROUND_CAP, replace=False)
    return X_item[idx]
