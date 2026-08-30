"""train + train_dev, stacked into the single Dataset a refit trains on.

``final_model_strategy: refit_on_full`` gives up the HPO validation split and
retrains on everything, so both of ``finalize_model``'s branches have to do the
same two mechanical things: stack the two splits' arrays, and hand LightGBM a
Dataset built the way HPO's cached binaries were.

The branches keep their own decisions written out — a ranking refit carries
query groups, a non-ranking one does not — and share only these, because a
drift between the two constructions would be invisible: LightGBM accepts either
Dataset, trains happily, and the only symptom is a model that split on a
different feature set than the search that chose its hyperparameters.
"""

import logging

import lightgbm as lgb
import numpy as np

from recsys_tfb.core.logging import log_data_volume

logger = logging.getLogger(__name__)


def stack_splits(train: tuple, dev: tuple) -> tuple:
    """The two splits' ``(X, y, weight)``, concatenated train-then-dev.

    The order is not free: ``offset_dev_group_ids`` concatenates group ids the
    same way, and a ranking refit whose rows and group ids disagree gets
    silently wrong query groups rather than an error.
    """
    X_train, y_train, w_train = train
    X_dev, y_dev, w_dev = dev
    X_full = np.concatenate([X_train, X_dev], axis=0)
    y_full = np.concatenate([y_train, y_dev], axis=0)
    w_full = np.concatenate([w_train, w_dev])
    log_data_volume(logger, "finalize.X_full", X_full)
    log_data_volume(logger, "finalize.y_full", y_full)
    return X_full, y_full, w_full


def offset_dev_group_ids(gid_train: np.ndarray, gid_dev: np.ndarray) -> np.ndarray:
    """Group ids for the stacked rows, dev's shifted past train's maximum.

    Both splits number their groups from zero, so a plain concatenation would
    make ``to_contiguous_groups`` merge one train group with one dev group into
    a single query. That is a wrong ranking target with no error attached.
    """
    offset = (int(gid_train.max()) + 1) if len(gid_train) else 0
    return np.concatenate([gid_train, gid_dev + offset])


def build_dataset(
    X: np.ndarray,
    y: np.ndarray,
    weight: np.ndarray,
    feature_columns: list,
    categorical_index,
    group=None,
) -> "lgb.Dataset":
    """The ``lgb.Dataset`` a refit trains on.

    ``feature_pre_filter=False`` is the one construct param that must not
    drift. HPO's cached ``.bin`` binaries are binned with it
    (``steps/hpo_scoring.py``, ``models/lightgbm_adapter.py``), so a refit built
    with LightGBM's default would drop features the winning trial was allowed to
    split on — a different model from the one the search chose, reported under
    the search's hyperparameters, with nothing raised.

    ``group=None`` is what a non-ranking refit passes and is also LightGBM's own
    default, so the two branches differ in the argument they supply, not in the
    Dataset this builds.
    """
    return lgb.Dataset(
        X,
        label=y,
        weight=weight,
        group=group,
        feature_name=feature_columns,
        categorical_feature=categorical_index,
        params={"feature_pre_filter": False},
        free_raw_data=True,
    )
