"""Tests for io/disk_matrix.py: the disk-backed matrix and its space gate.

The whole module exists because of one measured failure mode — a memmap
created on a full disk writes, flushes and exits successfully, and reads back
mostly zeros (``docs/operations/known-pitfalls.md`` §21). Nothing in the
process notices, so the only defence is refusing to allocate. These tests pin
the refusal, and pin that the happy path really is disk-backed rather than a
plain array that happens to work.

Every test redirects the scratch root by patching the module attribute, the
same way ``test_extract.py`` lowers ``STREAM_BATCH_BYTES``. Without the
fixture these would spill into the worktree's real ``data/_scratch``.
"""

import errno
import os
from pathlib import Path

import numpy as np
import pytest

from recsys_tfb.io import disk_matrix


@pytest.fixture
def scratch(tmp_path: Path, monkeypatch) -> Path:
    root = tmp_path / "scratch"
    monkeypatch.setattr(disk_matrix, "SCRATCH_ROOT", root)
    return root


class TestRequireFreeSpace:
    """The pre-check. On macOS it is the *only* check — no posix_fallocate."""

    def test_passes_when_the_filesystem_has_room(self, tmp_path: Path) -> None:
        disk_matrix.require_free_space(tmp_path, 1024, "tiny")

    def test_raises_enospc_when_it_does_not(self, tmp_path: Path) -> None:
        with pytest.raises(OSError) as excinfo:
            disk_matrix.require_free_space(tmp_path, 1 << 62, "huge")
        assert excinfo.value.errno == errno.ENOSPC

    def test_message_carries_need_have_and_where(self, tmp_path: Path) -> None:
        """An operator reading only this line must know what to free and where.

        Without the location it is a guess which filesystem ran out: the
        matrix lands under the project root, which is often not the volume
        the operator is watching.
        """
        with pytest.raises(OSError) as excinfo:
            disk_matrix.require_free_space(tmp_path, 1 << 42, "val_matrix")
        message = str(excinfo.value)
        assert "val_matrix" in message
        assert str(tmp_path) in message
        assert "4096.0 GiB" in message  # the need, in units a human reads
        assert "available" in message


class TestOpenDiskMatrix:
    def test_shape_dtype_and_contents_round_trip(self, scratch: Path) -> None:
        X = disk_matrix.open_disk_matrix((7, 3), np.dtype(np.float32), "unit")
        X[:] = np.arange(21, dtype=np.float32).reshape(7, 3)

        assert X.shape == (7, 3)
        assert X.dtype == np.float32
        np.testing.assert_array_equal(X[3], np.array([9, 10, 11], np.float32))

    def test_it_is_actually_disk_backed(self, scratch: Path) -> None:
        """A plain ``np.empty`` would satisfy every other test in this class.

        The point of the module is that the pages can be evicted, which only
        a mapping of a real file gives.
        """
        X = disk_matrix.open_disk_matrix((4, 2), np.dtype(np.float64), "unit")

        assert isinstance(X, np.memmap)

    def test_no_file_is_left_behind(self, scratch: Path) -> None:
        """Cleanup is the kernel's, tied to the array's lifetime.

        The file is unlinked the moment it is mapped, so there is no crash
        path and no later run that can leave a 89 GiB orphan under ``data/``.
        """
        X = disk_matrix.open_disk_matrix((4, 2), np.dtype(np.float32), "unit")
        X[:] = 1.0

        assert list(scratch.rglob("*")) == []
        # still usable after the name is gone — the mapping holds the inode
        assert X.sum() == 8.0

    def test_creates_the_scratch_root(self, scratch: Path) -> None:
        assert not scratch.exists()
        disk_matrix.open_disk_matrix((2, 2), np.dtype(np.float32), "unit")
        assert scratch.is_dir()

    def test_zero_rows_is_a_matrix_not_a_crash(self, scratch: Path) -> None:
        """``mmap`` refuses an empty file, so this is the one size at which a
        mapped matrix could diverge from ``np.empty`` — and it would diverge
        by raising, in a val set that used to score fine (empty, but fine).
        One padding byte keeps it on the same path as every other size.
        """
        X = disk_matrix.open_disk_matrix((0, 7), np.dtype(np.float32), "unit")

        assert X.shape == (0, 7)
        assert X.dtype == np.float32
        assert isinstance(X, np.memmap)
        assert len(X) == 0

    def test_refuses_before_creating_anything(self, scratch: Path) -> None:
        """The gate runs first, so a refusal costs no disk at all.

        512 GiB: a shape numpy maps quite happily. That is the point — drop
        the gate and this call *succeeds*, because the mapping is a sparse
        file and no disk is claimed until something writes to it. The only
        thing standing between that and a silently zero-filled matrix is the
        refusal, so this must fail by not raising, not by overflowing.

        ``not scratch.exists()`` rather than "the directory is empty": an
        empty ``rglob`` is also what a *missing* directory returns, so the
        weaker assertion passes whether the gate runs before ``mkdir`` or
        after it, and the ordering is the thing being pinned.
        """
        with pytest.raises(OSError) as excinfo:
            disk_matrix.open_disk_matrix(
                (1 << 28, 1 << 8), np.dtype(np.float64), "val_matrix"
            )
        assert excinfo.value.errno == errno.ENOSPC
        assert not scratch.exists()


class TestPreallocation:
    """``posix_fallocate`` where it exists; the pre-check alone where it does not.

    macOS has no ``os.posix_fallocate``, and that is not an edge case here —
    it is the platform this repo is developed on, so the fallback is the path
    every local run takes.
    """

    def test_fallocate_is_used_when_the_platform_has_it(
        self, scratch: Path, monkeypatch
    ) -> None:
        seen: list = []

        def spy(fd, offset, length):
            seen.append((offset, length))

        monkeypatch.setattr(os, "posix_fallocate", spy, raising=False)
        disk_matrix.open_disk_matrix((10, 4), np.dtype(np.float32), "unit")

        assert seen == [(0, 160)]

    def test_fallocate_enospc_surfaces_as_the_same_error(
        self, scratch: Path, monkeypatch
    ) -> None:
        """Both routes raise ``OSError``/``ENOSPC``, so callers need one branch."""
        def boom(fd, offset, length):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(os, "posix_fallocate", boom, raising=False)
        with pytest.raises(OSError) as excinfo:
            disk_matrix.open_disk_matrix((10, 4), np.dtype(np.float32), "unit")

        assert excinfo.value.errno == errno.ENOSPC

    def test_same_matrix_without_fallocate(
        self, scratch: Path, monkeypatch
    ) -> None:
        monkeypatch.delattr(os, "posix_fallocate", raising=False)
        X = disk_matrix.open_disk_matrix((10, 4), np.dtype(np.float32), "unit")
        X[:] = 3.0

        assert X.shape == (10, 4) and X.dtype == np.float32
        assert X.sum() == 120.0


class TestScratchRoot:
    def test_is_relative_to_the_project_root(self) -> None:
        """Hard-coding an absolute path would make every worktree share one
        scratch directory — and two runs would then map the same name."""
        assert not disk_matrix.SCRATCH_ROOT.is_absolute()
        assert disk_matrix.SCRATCH_ROOT.parts[0] == "data"
