"""Lightweight typed handles for cached training inputs.

These dataclasses flow through the pipeline DAG as references to on-disk
artifacts. Consumers call ``.to_pandas()`` / ``.load()`` to materialize the
underlying data lazily inside their own scope, allowing GC to release memory
between pipeline nodes.

``handle_paths`` / ``open_parquet_dataset`` are the read side of that contract:
a cache node may shard a split by key (``test_model_input`` is one directory per
month), and these turn either shape into something pyarrow can open as one
dataset.
"""

from dataclasses import dataclass
from typing import Literal, Mapping, Union


@dataclass(frozen=True)
class ParquetHandle:
    """Reference to a local parquet directory written by a cache node."""

    path: str

    def to_pandas(self) -> "pd.DataFrame":  # type: ignore[name-defined]
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        return table.to_pandas(split_blocks=True, self_destruct=True)


def handle_paths(
    handles: Union[ParquetHandle, Mapping[str, ParquetHandle]]
) -> list[str]:
    """Parquet roots behind one handle, or a mapping of them, in sorted-key order.

    Cache nodes that shard a split by key — ``test_model_input``, one directory
    per test month — return a mapping; single-artifact ones return a bare
    handle. Consumers that read across the whole split pass the result straight
    to ``pyarrow.dataset``, which accepts a list of dataset roots and treats
    them as one dataset.

    Order is deterministic (sorted by key) because diagnostics index rows by
    position across separate reads.
    """
    if isinstance(handles, ParquetHandle):
        return [handles.path]
    return [handles[key].path for key in sorted(handles)]


def require_complete_cache(
    handles: Union[ParquetHandle, Mapping[str, ParquetHandle]]
) -> None:
    """Raise unless every parquet root behind ``handles`` carries ``_SUCCESS``.

    Pre-check on an input, run before the first read.

    A cache node writes the marker last, so a directory without one is a copy
    that died partway — ``populate_cache_from_hive`` says as much in its own
    docstring. Reading such a directory does not fail: pyarrow opens whatever
    fragments landed, and the caller computes over a silent subset of the split.

    Deliberately not the cache node's own behaviour. Handed a marker-less
    directory *whose source is still available*, the right move is to clear it
    and rebuild from Hive, and that recovery must stay. This is for the other
    side of the same marker: a consumer handed a handle it cannot rebuild, where
    the only choices left are failing and publishing a number computed over an
    unknown fraction of the data.

    Only directories are checked. A handle pointing at a single parquet file is
    not a cache root — the marker is a property of the directories the cache
    nodes write — so requiring one there would reject a shape the contract never
    covered.
    """
    from pathlib import Path

    if isinstance(handles, ParquetHandle):
        labelled = [("", handles.path)]
    else:
        labelled = [(key, handles[key].path) for key in sorted(handles)]

    incomplete = [
        (key, root) for key, root in labelled
        if Path(root).is_dir() and not (Path(root) / "_SUCCESS").exists()
    ]
    if incomplete:
        detail = ", ".join(
            f"{key}: {root}" if key else root for key, root in incomplete
        )
        raise ValueError(
            "parquet cache is incomplete — no _SUCCESS marker, so the copy that "
            f"wrote it did not finish and an unknown fraction of the split is "
            f"missing: {detail}. Re-run the cache node for this split (its "
            "source is what can rebuild it); do not compute over it."
        )


def open_parquet_dataset(paths: Union[str, list[str]]):
    """One ``pyarrow.dataset`` over one or many hive-partitioned parquet roots.

    A list of *directories* cannot be handed to ``pads.dataset`` directly — it
    reads list elements as file paths and raises IsADirectoryError. The
    supported form for several roots is a list of dataset objects, which this
    builds. A single root returns that dataset unwrapped, so the common case
    stays byte-identical to a plain ``pads.dataset(path)``.

    Fragment order follows the given path order, then path-sort within each
    root — deterministic as long as the caller's list is (``handle_paths``
    sorts by key).

    Caveats of the multi-root form, both absent when everything lived under one
    root: hive partition types are inferred per root, so two months whose
    partition values infer differently (all-numeric item codes in one, strings
    in the other) fail to merge with an ArrowTypeError rather than being
    coerced to a common string type; and the return is a UnionDataset, which
    lacks ``.files``. Nothing downstream uses ``.files`` today.
    """
    import pyarrow.dataset as pads

    roots = [paths] if isinstance(paths, str) else list(paths)
    if not roots:
        raise ValueError("open_parquet_dataset needs at least one parquet root")
    children = [
        pads.dataset(root, format="parquet", partitioning="hive") for root in roots
    ]
    return children[0] if len(children) == 1 else pads.dataset(children)


#: Sidecar written next to the .bin by
#: ``LightGBMAdapter.prepare_train_inputs`` when the objective drops
#: zero-positive query groups. The filename is the contract between that
#: writer and :meth:`LgbDatasetHandle.group_filter_counts`, and it is also
#: what marks a .bin as built *under* that rule — a cached directory without
#: it predates the rule and its rows were never filtered.
#:
#: Deliberately not named ``group_filter_report.json``: that is the separate
#: catalog artifact in the model version dir. Two JSON files one word apart,
#: both plain dicts, would swap silently.
GROUP_FILTER_COUNTS_NAME = "group_filter_counts.json"

#: Sidecar written beside each ``.bin`` by
#: ``LightGBMAdapter.prepare_train_inputs``, holding that binary's rows'
#: **sample-weight key columns** in the binary's own row order —
#: ``train.bin`` -> ``train.weight_keys.parquet``.
#:
#: Why the keys and not the weights: ``training.sample_weights`` feeds
#: ``model_version`` and nothing in the lgb cache path, so a weight vector
#: written in here would be served unchanged to a later run configured with
#: different weights, and no layer would say so (#318). The keys are what the
#: weights are *resolved from*, so they are the same for every weight table
#: and a run always resolves its own.
#:
#: Its absence is what marks a ``.bin`` as predating this split — those
#: binaries carry weights baked in and cannot be re-weighted, so
#: ``prepare_train_inputs`` rebuilds rather than serves them.
WEIGHT_KEYS_SUFFIX = ".weight_keys.parquet"

#: Parquet schema-metadata key under which a sidecar records the
#: ``training.sample_weight_keys`` its build was *asked* for. Not inferable
#: from the sidecar's columns: a configured key column the model_input does
#: not carry is absent from both, and comparing columns alone would then read
#: as a config change and rebuild the .bin on every single run.
WEIGHT_KEYS_META = b"recsys_tfb.weight_keys"

#: Parquet schema-metadata key under which a sidecar records how many rows it
#: describes. Redundant with the frame's own length **except in the one case
#: that matters**: when none of the configured key columns exist in the
#: model_input, the frame has no columns, and a column-less table cannot carry
#: a row count through parquet — pyarrow writes ``num_rows=0`` and reads back
#: an empty frame. Without this the resolver would hand out a length-0 weight
#: vector for an N-row binary, which ``set_weight`` discards in silence
#: (any all-ones array becomes ``None``, and a length-0 array is vacuously
#: all-ones), training the whole search unweighted.
WEIGHT_ROWS_META = b"recsys_tfb.weight_key_rows"


def weight_keys_sidecar(bin_path: str) -> str:
    """The weight-key sidecar belonging to ``bin_path``.

    One definition shared by the writer (``LightGBMAdapter.prepare_train_inputs``)
    and the reader (:meth:`LgbDatasetHandle.sample_weights`): a sidecar written
    under one spelling and looked up under another is missing, and "missing"
    means "rebuild the cache" — an expensive silence rather than an error.
    """
    from pathlib import Path

    p = Path(bin_path)
    return str(p.with_name(p.stem + WEIGHT_KEYS_SUFFIX))


@dataclass(frozen=True)
class LgbDatasetHandle:
    """Reference to a saved ``lgb.Dataset`` binary on disk.

    ``role`` distinguishes "train" from "train_dev" so callers can build the
    correct reference linkage when reloading.
    """

    bin_path: str
    role: Literal["train", "train_dev"]

    def group_filter_counts(self) -> dict | None:
        """How many query groups this binary's build dropped, or ``None``.

        ``None`` means this .bin was **not** built under the zero-positive
        filter. Two different situations, and the caller has to tell them
        apart: the objective does not filter (everything but lambdarank), or
        the directory was written before the rule existed and holds every row.
        ``prepare_train_inputs`` resolves the second by rebuilding, so by the
        time a handle reaches a consumer ``None`` means the first.

        Read back from disk rather than recomputed: a run that hits the .bin
        cache reads no parquet at all, and still has to be able to say what
        the matrix it trains on is made of.

        One ``prepare_train_inputs`` call writes one file covering both
        splits, so the train and train_dev handles return the same thing.
        """
        import json
        from pathlib import Path

        counts = Path(self.bin_path).parent / GROUP_FILTER_COUNTS_NAME
        if not counts.exists():
            return None
        with open(counts) as f:
            return json.load(f)

    @property
    def weight_keys_path(self) -> str:
        """Path of this binary's weight-key sidecar (:func:`weight_keys_sidecar`).

        Derived from ``bin_path`` rather than stored, so the two cannot be
        handed around separately and drift.
        """
        return weight_keys_sidecar(self.bin_path)

    def sample_weights(
        self, parameters: dict, preprocessor_metadata: dict
    ) -> "np.ndarray":  # type: ignore[name-defined]
        """Today's ``training.sample_weights``, resolved against this binary's rows.

        Aligned 1:1 with the rows of the ``.bin``, so the caller can
        ``set_weight`` it onto the loaded Dataset. Weights live here rather
        than inside the binary because the lgb cache path does not mention
        them: baked in, a stale binary would silently train under the previous
        run's weights (#318).

        Alignment is by construction, not by re-derivation: the sidecar holds
        the surviving rows in the order they were written, so the same
        zero-positive filter and the same group permutation are already
        applied. Nothing here re-reads the model_input parquet. The *length*
        is still checked independently against the binary itself before the
        vector is used — see ``steps/hpo_scoring.py`` — because the one way
        this can go wrong produces a vector LightGBM discards without a word.

        Raises if the sidecar is missing. ``prepare_train_inputs`` rebuilds a
        cache directory without one, so a handle that reaches a consumer has
        it; reaching this line without one means a binary was moved or handed
        over outside that path, and all-ones would be a wrong answer that
        trains a whole search before anyone notices.
        """
        import json
        from pathlib import Path

        import pandas as pd
        import pyarrow.parquet as pq

        from recsys_tfb.io.extract import resolve_sample_weights

        path = Path(self.weight_keys_path)
        if not path.exists():
            raise FileNotFoundError(
                f"no sample-weight key sidecar beside {self.bin_path} "
                f"(expected {path}); the .bin cannot be re-weighted. Clear the "
                "lgb cache directory so it is rebuilt."
            )
        table = pq.read_table(path)
        pdf = table.to_pandas()
        if pdf.shape[1] == 0:
            # None of the configured key columns exist in this model_input, so
            # the sidecar has no columns and parquet lost its row count on the
            # way in. Restore it from the metadata: the resolver's "weight-key
            # column absent" backstop then logs INACTIVE and returns all-ones
            # of the right length, which is what this config did before the
            # weights moved out of the .bin.
            meta = (table.schema.metadata or {}).get(WEIGHT_ROWS_META)
            if meta is None:
                raise ValueError(
                    f"weight-key sidecar {path} has no columns and no row "
                    f"count; it cannot say how many rows {self.bin_path} "
                    "holds. Clear the lgb cache directory so it is rebuilt."
                )
            pdf = pd.DataFrame(index=pd.RangeIndex(int(json.loads(meta))))
        return resolve_sample_weights(pdf, parameters, preprocessor_metadata)

    def load(
        self,
        reference: "lgb.Dataset | None" = None,  # type: ignore[name-defined]
        params: dict | None = None,
    ) -> "lgb.Dataset":  # type: ignore[name-defined]
        import lightgbm as lgb

        return lgb.Dataset(self.bin_path, reference=reference, params=params)
