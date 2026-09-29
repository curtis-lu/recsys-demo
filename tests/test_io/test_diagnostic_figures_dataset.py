"""DiagnosticFiguresDataset: draw, write and close one figure at a time; one
that fails is a warning, not a stopped run (ADR-0030 decisions 4 and 7)."""

import logging

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pytest  # noqa: E402

from recsys_tfb.io.diagnostic_figures_dataset import DiagnosticFiguresDataset  # noqa: E402


def _line(y):
    def draw():
        fig = plt.figure()
        fig.add_subplot(111).plot([0, 1], [0, y])
        return fig
    return draw


def test_each_figure_lands_at_its_relative_path(tmp_path):
    ds = DiagnosticFiguresDataset(filepath=str(tmp_path / "summary"))
    ds.save({"global.png": _line(1), "per_item/a.png": _line(2)})
    assert (tmp_path / "summary/global.png").stat().st_size > 0
    assert (tmp_path / "summary/per_item/a.png").stat().st_size > 0
    assert ds.exists()


def test_a_figure_that_fails_is_skipped_with_a_warning_and_the_rest_are_saved(
        tmp_path, caplog):
    """The user's 2026-09-28 ruling: a figure feeds no number, so one that
    cannot be drawn must not stop a run that took hours."""
    def broken():
        plt.figure()                      # opened, then the draw fails
        raise RuntimeError("plot exploded")

    ds = DiagnosticFiguresDataset(filepath=str(tmp_path))
    open_before = plt.get_fignums()       # other tests may have left some open
    with caplog.at_level(logging.WARNING):
        ds.save({"a.png": _line(1), "b.png": broken, "c.png": _line(3)})

    assert (tmp_path / "a.png").exists() and (tmp_path / "c.png").exists()
    assert not (tmp_path / "b.png").exists()
    assert any("b.png" in r.getMessage() and "plot exploded" in r.getMessage()
               for r in caplog.records)
    assert plt.get_fignums() == open_before   # the broken draw's figure is closed too


def test_figures_are_drawn_one_at_a_time_and_closed(tmp_path):
    """Drawing at save time is only worth it if a figure is closed before the
    next is drawn: otherwise a hundred-odd figures accumulate as before."""
    open_while_drawing = []

    def counting(y):
        draw = _line(y)
        def wrapped():
            open_while_drawing.append(len(plt.get_fignums()))
            return draw()
        return wrapped

    ds = DiagnosticFiguresDataset(filepath=str(tmp_path))
    already_open = len(plt.get_fignums())   # other tests may have left some open
    ds.save({f"{i}.png": counting(i) for i in range(5)})

    assert open_while_drawing == [already_open] * 5
    assert len(plt.get_fignums()) == already_open
    assert len(list(tmp_path.glob("*.png"))) == 5


def test_nothing_to_load(tmp_path):
    with pytest.raises(NotImplementedError, match="not read back"):
        DiagnosticFiguresDataset(filepath=str(tmp_path)).load()


def test_registered_for_the_catalog():
    from recsys_tfb.core.catalog import DataCatalog

    catalog = DataCatalog({"figs": {"type": "DiagnosticFiguresDataset",
                                    "filepath": "x"}})
    assert isinstance(catalog.get_dataset("figs"), DiagnosticFiguresDataset)
