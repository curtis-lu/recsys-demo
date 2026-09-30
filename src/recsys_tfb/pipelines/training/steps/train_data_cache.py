"""Where the cached native training data (``train.bin`` / ``train_dev.bin``) lives.

Mechanism only. What decides whether a cached directory is used, which rows go
into it and in what order, and why sample weights stay out of it is written in
``prepare_train_inputs`` (``nodes.py``), the node that builds it (ADR-0030
decisions 1 and 10). What a directory without ``_SUCCESS`` means is the same
protocol the parquet copies follow, so the node uses ``local_cache``'s helpers
for it rather than a second copy here.

The log lines live here too: the node writes each split through the same
lines, and a format string written twice drifts.
"""

import hashlib
import logging
from pathlib import Path

from recsys_tfb.core.group_utils import single_label_group_counts
from recsys_tfb.core.versioning import TRAINING_MODEL_FORMAT_VERSION
from recsys_tfb.io.handles import LgbDatasetHandle
from recsys_tfb.pipelines.training.steps.local_cache import resolve_cache_path

logger = logging.getLogger(__name__)

#: Bumped by one when the code changes what a cache directory holds while
#: every other segment of its path stays the same — which rows, their order,
#: how a column is encoded, what a sidecar records. The number is a segment of
#: the path (:func:`cache_dir`), so a bump sends every deployment to a new,
#: empty directory: the first run after it rebuilds the ``.bin`` once, and the
#: old directories stay on disk until someone deletes them.
#:
#: The question to ask of a change is the one ``pipeline-node-design.md``
#: rule 18 asks of the dataset: is there a legal config and data for which the
#: code before and after the change writes different files into the same
#: path? A change that alters the path already misses and needs no bump.
#:
#: Separate from the training format versions ADR-0030 decision 9 adds to
#: ``model_version``: the cache's format can change without the model changing
#: (a sidecar gains a field), and one shared number would make every such
#: change retrain every deployment. **Most changes to what a directory holds
#: change the model too, and then both numbers move.** Even reordering rows
#: inside a query group changes a lambdarank model (measured 2026-09-29 on
#: LightGBM 4.6.0, 3,000 synthetic groups of 22 rows, 50 rounds, deterministic
#: single-thread: predictions moved by up to 1.5).
#: Bumping this one alone rebuilds the files but leaves ``model_version`` and
#: the HPO ``search_id`` as they were, so a resumed search would mix trials
#: scored on the old files with trials scored on the new ones. The other way
#: round is covered: ``TRAINING_MODEL_FORMAT_VERSION`` is a segment of the
#: path too (:func:`cache_dir`), so bumping that one alone rebuilds the files
#: as well, and a new search never trains on files the old code built.
#:
#: Before this number existed (#483), the path had no version segment and
#: three checks inside the directory caught the formats it replaced: a ``.bin``
#: built before the zero-positive filter (#315), one with weights baked in
#: (#318), one built for other ``training.sample_weight_keys``. Those
#: directories sit under ``lgb/`` beside the new ones and are never read.
TRAIN_DATA_CACHE_FORMAT_VERSION: int = 1

#: The file each split's native training data is saved to, in the cache
#: directory. The ``.bin`` names date from when LightGBM was the only writer
#: (as does ``LgbDatasetHandle``); any adapter's ``save_train_data`` writes its
#: own format under them.
_BIN_NAMES = {"train": "train.bin", "train_dev": "train_dev.bin"}


def _digest8(values: list) -> str:
    """First 8 hex characters of a SHA-256 over ``values``, one per line.

    Short enough for a directory name, and free of whatever characters a
    column name may carry. Order counts: the same columns in another order are
    another matrix, or another weight lookup key.
    """
    return hashlib.sha256("\n".join(map(str, values)).encode()).hexdigest()[:8]


def cache_dir(
    parameters: dict,
    *,
    algorithm: str,
    objective_segment: str,
    feature_columns: list,
    weight_keys: list,
) -> str:
    """The directory one build of the native training data is cached in.

    ``<train variant dir>/train_data_v<N>/model_format_v<M>/<algorithm>/
    <objective>/features_<hash8>/weight_keys_<hash8>`` — beside the train
    split's parquet copy (``<cache.root>/<base_dataset_version>/
    train_variants/<train_variant_id>/``), because the two are built from the
    same draw and retire together.

    ``<M>`` is ``core/versioning.py``'s ``TRAINING_MODEL_FORMAT_VERSION``, not
    a version of these files: a bump for a change to how they are built
    rebuilds them even when ``<N>`` was forgotten, at the price of one rebuild
    when the change was elsewhere — and then the search restarts anyway
    (ADR-0030 decision 10, implementation note of #488).

    Every segment is always present. A segment that appeared only when a
    feature was on (the old ``fs_<hash8>`` did) nests one cache inside
    another, and clearing the outer one's interrupted build deletes the inner
    one's finished build with it.

    Composed, never resolved: ``cache.root`` is relative in
    ``conf/base/parameters_training.yaml`` (see ``resolve_cache_path``).
    """
    variant_dir = Path(resolve_cache_path("train_model_input", parameters)).parent
    return str(
        variant_dir
        / f"train_data_v{TRAIN_DATA_CACHE_FORMAT_VERSION}"
        / f"model_format_v{TRAINING_MODEL_FORMAT_VERSION}"
        / algorithm
        / objective_segment
        / f"features_{_digest8(feature_columns)}"
        / f"weight_keys_{_digest8(weight_keys)}"
    )


def bin_path(directory: str, split: str) -> str:
    """Where ``split`` ("train" / "train_dev") is saved in a cache directory."""
    return str(Path(directory) / _BIN_NAMES[split])


def handles(directory: str) -> tuple[LgbDatasetHandle, LgbDatasetHandle]:
    """The train and train_dev handles over one cache directory."""
    return (
        LgbDatasetHandle(bin_path=bin_path(directory, "train"), role="train"),
        LgbDatasetHandle(bin_path=bin_path(directory, "train_dev"), role="train_dev"),
    )


def log_group_filter(split: str, counts: dict) -> None:
    """One line per split saying what the zero-positive filter removed.

    Worded apart from ``compute_group_filter_report``'s line on purpose: that
    one runs every time, this one only when the binary is built.
    """
    logger.info(
        "train data build [%s]: dropped %d/%d zero-positive query groups "
        "(%d/%d rows); %d groups / %d rows remain",
        split, counts["groups_dropped"], counts["groups_total"],
        counts["rows_dropped"], counts["rows_total"],
        counts["groups_kept"], counts["rows_kept"],
    )


def log_single_label_groups(split: str, objective: str, y, group_ids) -> None:
    """One line per split: the share of query groups a ranking objective can
    draw no pair from (ADR-0025 decision H). Counted on the rows the binary
    holds — after the zero-positive filter — so it describes what the
    objective actually trains on. Logged at build time only: a cache hit
    builds nothing, and the line to read is the one the building run wrote."""
    counts = single_label_group_counts(y, group_ids)
    total = counts["groups_total"]
    single = counts["groups_single_label"]
    logger.info(
        "%s single-label query groups [%s]: %d/%d groups (%.1f%%) hold one "
        "label only and give the objective no pair to compare",
        objective, split, single, total, 100.0 * single / total if total else 0.0,
    )
