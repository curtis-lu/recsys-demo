"""train + train_dev, stacked into the single matrix a refit trains on.

``final_model_strategy: refit_on_full`` gives up the HPO validation split and
retrains on everything, so both of ``finalize_model``'s branches have to do the
same mechanical thing: stack the two splits, train first. Turning the stacked
arrays into training data is the adapter's (``build_train_data``), which
builds it the way the search's cached binaries were built.

The stacking happens on the per-row arrays and on *row numbers*, never on two
finished matrices: the node chooses which stacked rows the refit keeps and in
what order, and :func:`stacked_matrix` streams the features straight into that
order. Concatenating two matrices and then reordering the result held two full
copies of the training matrix at the peak (ADR-0030 decision 12, item 3).

The branches keep their own decisions written out — a ranking refit carries
query groups, a non-ranking one does not — and share only these.
"""

import logging

import numpy as np

from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.io.extract import extract_X_rows

logger = logging.getLogger(__name__)


def stacked_row_numbers(n_train: int, n_dev: int) -> tuple[np.ndarray, np.ndarray]:
    """Each split's rows numbered as :func:`stacked_matrix` numbers them.

    Train's rows are ``0 .. n_train - 1`` and train_dev's follow on from
    ``n_train`` — the order the two parquets are streamed in. Carried through
    any row filter beside the labels, they say which source row each surviving
    row is; numbering dev from zero instead would read train's rows under
    dev's labels.
    """
    return (
        np.arange(n_train, dtype=np.int64),
        np.arange(n_train, n_train + n_dev, dtype=np.int64),
    )


def stack_splits(train: tuple, dev: tuple) -> tuple:
    """The two splits' per-row arrays, concatenated train-then-dev.

    ``train`` and ``dev`` are same-length tuples of 1-D arrays (labels,
    weights, row numbers), each array paired with its namesake.

    The order is not free: ``offset_dev_group_ids`` and
    :func:`stacked_row_numbers` both put train first, and a ranking refit
    whose rows and group ids disagree gets silently wrong query groups rather
    than an error.

    The ``finalize.y_full`` volume record here and ``finalize.X_full`` in
    :func:`stacked_matrix` are hard-coded and name this module's only caller.
    That is deliberate: they are an existing monitoring interface, and a
    refactor that renamed them would break a dashboard filter with nothing to
    show for it. **Their ``logger`` field did change** — these records used to
    be emitted under ``…pipelines.training.nodes`` and now come from this
    module, the same shift ``steps/hpo_scoring.py`` made in #229. A filter on
    the logger name, rather than on the record name, needs updating.
    """
    # strict: a tuple one array short would silently drop that array.
    stacked = tuple(
        np.concatenate([a, b]) for a, b in zip(train, dev, strict=True))
    log_data_volume(logger, "finalize.y_full", stacked[0])
    return stacked


def stacked_matrix(
    splits: tuple,
    preprocessor_metadata: dict,
    parameters: dict,
    *,
    rows: np.ndarray | None,
    labels: np.ndarray,
) -> np.ndarray:
    """The feature matrix of train then train_dev, row ``i`` = stacked row ``rows[i]``.

    ``rows`` uses :func:`stacked_row_numbers`'s numbering; ``None`` is every
    row, train first. ``labels`` are the labels of those rows in that order —
    see ``extract_X_rows`` for the check they feed.
    """
    X = extract_X_rows(
        list(splits), preprocessor_metadata, parameters, rows=rows, labels=labels)
    log_data_volume(logger, "finalize.X_full", X)
    return X


def offset_dev_group_ids(gid_train: np.ndarray, gid_dev: np.ndarray) -> np.ndarray:
    """Group ids for the stacked rows, dev's shifted past train's maximum.

    Both splits number their groups from zero, so a plain concatenation would
    make ``to_contiguous_groups`` merge one train group with one dev group into
    a single query. That is a wrong ranking target with no error attached.
    """
    offset = (int(gid_train.max()) + 1) if len(gid_train) else 0
    return np.concatenate([gid_train, gid_dev + offset])
