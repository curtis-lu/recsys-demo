"""Tests for io/disk_matrix.py: the disk-backed matrix and its space gate.

The whole module exists because of one measured failure mode — a memmap
created on a full disk writes, flushes and exits successfully, and reads back
mostly zeros (``docs/operations/known-pitfalls.md`` §21). Nothing in the
process notices, so the only defence is refusing to allocate. These tests pin
the refusal, and pin that the happy path really is disk-backed rather than a
plain array that happens to work.
"""

import errno
import os
from pathlib import Path

import numpy as np
import pytest

from recsys_tfb.io import disk_matrix


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
    def test_shape_dtype_and_contents_round_trip(self, tmp_path: Path) -> None:
        X = disk_matrix.open_disk_matrix(
            (7, 3), np.dtype(np.float32), "unit", root=tmp_path
        )
        X[:] = np.arange(21, dtype=np.float32).reshape(7, 3)

        assert X.shape == (7, 3)
        assert X.dtype == np.float32
        np.testing.assert_array_equal(X[3], np.array([9, 10, 11], np.float32))

    def test_it_is_actually_disk_backed(self, tmp_path: Path) -> None:
        """A plain ``np.empty`` would satisfy every other test in this class.

        The point of the module is that the pages can be evicted, which only
        a mapping of a real file gives.
        """
        X = disk_matrix.open_disk_matrix(
            (4, 2), np.dtype(np.float64), "unit", root=tmp_path
        )
        assert isinstance(X, np.memmap)

    def test_no_file_is_left_behind(self, tmp_path: Path) -> None:
        """Cleanup is the kernel's, tied to the array's lifetime.

        The file is unlinked the moment it is mapped, so there is no crash
        path and no later run that can leave a 89 GiB orphan under ``data/``.
        """
        X = disk_matrix.open_disk_matrix(
            (4, 2), np.dtype(np.float32), "unit", root=tmp_path
        )
        X[:] = 1.0

        assert list(tmp_path.rglob("*.dat")) == []
        # still usable after the name is gone — the mapping holds the inode
        assert X.sum() == 8.0

    def test_creates_the_root_directory(self, tmp_path: Path) -> None:
        root = tmp_path / "does" / "not" / "exist"
        disk_matrix.open_disk_matrix((2, 2), np.dtype(np.float32), "unit", root=root)
        assert root.is_dir()

    def test_refuses_before_creating_anything(self, tmp_path: Path) -> None:
        """The gate runs first, so a refusal costs no disk at all.

        512 GiB: a shape numpy maps quite happily. That is the point — drop
        the gate and this call *succeeds*, because the mapping is a sparse
        file and no disk is claimed until something writes to it. The only
        thing standing between that and a silently zero-filled matrix is the
        refusal, so this must fail by not raising, not by overflowing.
        """
        with pytest.raises(OSError) as excinfo:
            disk_matrix.open_disk_matrix(
                (1 << 28, 1 << 8), np.dtype(np.float64), "val_matrix", root=tmp_path
            )
        assert excinfo.value.errno == errno.ENOSPC
        assert list(tmp_path.rglob("*")) == []


class TestPreallocation:
    """``posix_fallocate`` where it exists; the pre-check alone where it does not.

    macOS has no ``os.posix_fallocate``, and that is not an edge case here —
    it is the platform this repo is developed on, so the fallback is the path
    every local run takes.
    """

    def test_fallocate_is_used_when_the_platform_has_it(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        seen: list = []

        def spy(fd, offset, length):
            seen.append((offset, length))

        monkeypatch.setattr(os, "posix_fallocate", spy, raising=False)
        disk_matrix.open_disk_matrix(
            (10, 4), np.dtype(np.float32), "unit", root=tmp_path
        )

        assert seen == [(0, 160)]

    def test_fallocate_enospc_surfaces_as_the_same_error(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """Both routes raise ``OSError``/``ENOSPC``, so callers need one branch."""
        def boom(fd, offset, length):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(os, "posix_fallocate", boom, raising=False)
        with pytest.raises(OSError) as excinfo:
            disk_matrix.open_disk_matrix(
                (10, 4), np.dtype(np.float32), "unit", root=tmp_path
            )
        assert excinfo.value.errno == errno.ENOSPC

    def test_same_matrix_without_fallocate(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.delattr(os, "posix_fallocate", raising=False)
        X = disk_matrix.open_disk_matrix(
            (10, 4), np.dtype(np.float32), "unit", root=tmp_path
        )
        X[:] = 3.0

        assert X.shape == (10, 4) and X.dtype == np.float32
        assert X.sum() == 120.0


class TestScratchRoot:
    def test_is_relative_to_the_project_root(self) -> None:
        """Hard-coding an absolute path would make every worktree share one
        scratch directory — and two runs would then map the same name."""
        assert not disk_matrix.SCRATCH_ROOT.is_absolute()
        assert disk_matrix.SCRATCH_ROOT.parts[0] == "data"
