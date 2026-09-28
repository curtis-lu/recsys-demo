"""train + train_dev, stacked into the single matrix a refit trains on.

``final_model_strategy: refit_on_full`` gives up the HPO validation split and
retrains on everything, so both of ``finalize_model``'s branches have to do the
same mechanical thing: stack the two splits' arrays, train first. Turning the
stacked arrays into training data is the adapter's (``build_train_data``),
which builds it the way the search's cached binaries were built.

The branches keep their own decisions written out — a ranking refit carries
query groups, a non-ranking one does not — and share only these.
"""

import logging

import numpy as np

from recsys_tfb.core.logging import log_data_volume

logger = logging.getLogger(__name__)


def stack_splits(train: tuple, dev: tuple) -> tuple:
    """The two splits' ``(X, y, weight)``, concatenated train-then-dev.

    The order is not free: ``offset_dev_group_ids`` concatenates group ids the
    same way, and a ranking refit whose rows and group ids disagree gets
    silently wrong query groups rather than an error.

    The two ``finalize.*`` volume-record names are hard-coded rather than
    passed in, and they name this module's only caller. That is deliberate:
    they are an existing monitoring interface, and a refactor that renamed them
    would break a dashboard filter with nothing to show for it. **Their
    ``logger`` field did change** — these records used to be emitted under
    ``…pipelines.training.nodes`` and now come from this module, the same shift
    ``steps/hpo_scoring.py`` made in #229. A filter on the logger name, rather
    than on the record name, needs updating.
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
