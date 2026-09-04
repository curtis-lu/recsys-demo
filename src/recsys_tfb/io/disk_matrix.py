"""A feature matrix that lives on disk instead of in the driver's heap.

At production scale one val matrix is 37-89 GiB, and ``TrialScorer`` holds it
for the whole HPO search. Mapping it from a file instead of allocating it on
the heap decouples resident memory from the number of val rows: the pages the
current predict batch touches are resident, the rest are the OS's to evict.

**There is no "small enough to keep in RAM" branch, on purpose.** A branch
taken only once the data is large enough to hurt is a branch nobody ever
tests, and it is the one that ships broken. One path, always mapped: when the
matrix does fit, the page cache makes it behave almost exactly like the heap
array it replaced (measured 1.11x slower on predict, and predict is 3.3% of a
trial, so ~0.4% of the search); when it does not, the kernel reclaims pages
instead of the driver dying.

⚠ **A memmap on a full filesystem does not fail — it corrupts.** Measured on
a 20 MB APFS disk image writing a 95.4 MiB mapping of random bytes (random so
the filesystem cannot compress it away): creation succeeded, 20 chunked writes
each with an explicit ``flush()`` all succeeded, the process exited 0, and a
remount-and-read-back found 17 of 20 chunks wrong with 81% of the file zeros.
``np.memmap`` creates a sparse file, so the space is not claimed up front and
nothing on the write path reports the shortfall. For HPO that means scoring
every trial against a val matrix that is mostly zeros, picking a "best"
hyper-parameter set from those numbers, and no error anywhere — worse than a
crash.

Whether it is silent depends on the filesystem, and the way round is the
unhelpful one: the same script on an **HFS+** image raises ``ENOSPC`` at
creation, because HFS+ has no sparse files and numpy's size-setting write has
to claim the blocks there and then. Every filesystem anyone actually runs on
— APFS, ext4, XFS — is sparse and takes the silent route. So "it errored on
my machine" is not evidence of anything. Hence :func:`require_free_space`,
which runs *before* anything is created. Full measurement in
``docs/operations/known-pitfalls.md`` §21.
"""

import errno
import logging
import os
import shutil
import tempfile
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

#: Where scratch matrices land, **relative to the project / worktree root** —
#: the same convention as the training cache (``cache.root``) and the HPO study
#: directory (``data/models/_hpo/<search_id>``). Relative and not absolute so a
#: worktree spills onto its own tree: an absolute path would put every worktree
#: on one directory, and the free-space check would then be measuring a volume
#: other than the one the run is really filling.
SCRATCH_ROOT: Path = Path("data") / "_scratch"


def _gib(n_bytes: int) -> str:
    """Bytes as GiB, because the numbers this module raises about are GiB-sized."""
    return f"{n_bytes / 1024 ** 3:.1f} GiB"


def require_free_space(directory: Path, n_bytes: int, label: str) -> None:
    """Pre-check: refuse to map ``n_bytes`` under ``directory`` without room.

    A **pre-check**, in the sense of ``pipeline-node-design.md`` rule 11: it
    has to see the filesystem, so it cannot live in ``core/consistency.py``
    with the config invariants.

    ``OSError``/``ENOSPC`` deliberately, not a project exception: that is
    exactly what ``os.posix_fallocate`` raises when it is the one that catches
    the shortfall, so a caller on either platform handles one error and reads
    one kind of message.

    The message names the requirement, what is actually there, and *which*
    directory — the matrix lands under the project root, which on a dev box is
    routinely not the volume the operator is watching.
    """
    free = shutil.disk_usage(directory).free
    if free >= n_bytes:
        return
    raise OSError(
        errno.ENOSPC,
        f"{label}: needs {_gib(n_bytes)} of disk under {directory}, but only "
        f"{_gib(free)} is available. Refusing to create the mapping: a memmap "
        f"on a full filesystem does not raise — it flushes successfully and "
        f"reads back zeros, so the run would score against a corrupt matrix "
        f"and report nothing (known-pitfalls.md §21). Free up "
        f"{_gib(n_bytes - free)} or point the run at a larger volume.",
    )


def _preallocate(fh, n_bytes: int) -> None:
    """Claim the bytes now, so a shortfall is an error and not silent zeros.

    ``os.posix_fallocate`` reserves the blocks and raises ``ENOSPC`` on the
    spot. **macOS does not have it** — that is not an exotic platform note,
    it is the machine this repo is developed on, so the ``truncate`` branch is
    the one every local run takes. ``truncate`` only sets the size (the file
    stays sparse), which is why :func:`require_free_space` is the guard and
    ``posix_fallocate`` is the bonus, not the other way round.
    """
    if hasattr(os, "posix_fallocate"):
        os.posix_fallocate(fh.fileno(), 0, n_bytes)
    else:
        fh.truncate(n_bytes)


def open_disk_matrix(
    shape: tuple, dtype: np.dtype, label: str, root: Path | None = None,
) -> np.memmap:
    """A zero-filled, writable matrix of ``shape`` mapped from a scratch file.

    Drop-in for ``np.empty(shape, dtype)``: the result indexes and slices like
    any 2-D array, which is the whole contract its consumers rely on.

    **The file is unlinked as soon as it is mapped.** POSIX keeps the inode
    alive for the mapping, so the array stays fully usable while the name is
    already gone — which makes cleanup the kernel's job, tied to the array's
    lifetime rather than to a ``finally`` block someone has to remember. A
    killed process, an unhandled exception, a machine losing power: none of
    them can leave an 89 GiB orphan under ``data/``. The cost is that the file
    cannot be inspected mid-run; it holds a matrix that a re-read of the
    parquet reproduces exactly, so there is nothing in it worth inspecting.

    Raises ``OSError``/``ENOSPC`` before creating anything if the filesystem
    cannot hold the matrix — see :func:`require_free_space` for why silence
    here is worse than failure.
    """
    root = SCRATCH_ROOT if root is None else Path(root)
    root.mkdir(parents=True, exist_ok=True)

    n_bytes = int(np.prod(shape)) * dtype.itemsize
    require_free_space(root, n_bytes, label)

    fd, name = tempfile.mkstemp(dir=str(root), prefix=f"{label}-", suffix=".dat")
    try:
        with os.fdopen(fd, "r+b") as fh:
            _preallocate(fh, n_bytes)
            matrix = np.memmap(fh, dtype=dtype, mode="r+", shape=shape)
    finally:
        os.unlink(name)

    logger.info(
        "disk_matrix: mapped %s shape=%s dtype=%s size=%s under %s (file unlinked)",
        label, tuple(shape), dtype.name, _gib(n_bytes), root,
    )
    return matrix
