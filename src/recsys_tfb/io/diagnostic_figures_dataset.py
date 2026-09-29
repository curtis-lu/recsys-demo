"""A stack of diagnostic figures, drawn and written one at a time.

The same idea as Kedro's matplotlib dataset, with two differences that are the
reason it exists (ADR-0030 decision 7):

- **It saves drawing functions, not figures.** ``save`` takes
  ``{relative path: draw}``; each ``draw()`` returns a matplotlib figure, which
  is written under ``filepath`` and closed before the next one is drawn. A
  node that drew its figures up front would hold all of them at once — a
  hundred-odd for 22 items.
- **One figure that fails is skipped, with a warning.** Figures are read by
  people and feed no number, and a missing file shows in MLflow, so one plot
  is not worth stopping a run that took hours (the user's 2026-09-28 ruling,
  ADR-0030 decision 4). That is this type's written behaviour and the reason
  to use it for diagnostic figures only: any failure while drawing or writing
  one figure is logged and the rest are still saved.

There is nothing to load: a saved stack is PNG files for people and for
``log_experiment``'s upload, not an input to any node.
"""

import logging
from collections.abc import Callable, Mapping
from pathlib import Path

from recsys_tfb.io.base import AbstractDataset

logger = logging.getLogger(__name__)

#: Resolution every diagnostic figure has been written at.
_DPI = 100


class DiagnosticFiguresDataset(AbstractDataset):
    """PNG files under ``filepath``, one per entry of the saved mapping."""

    def __init__(self, filepath: str):
        self._filepath = filepath

    def save(self, data: Mapping[str, Callable]) -> None:
        import matplotlib

        if matplotlib.get_backend().lower() != "agg":
            matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        root = Path(self._filepath)
        written = 0
        for relative, draw in data.items():
            # Every figure opened while drawing this one is closed afterwards,
            # whether or not it was written: a draw that raised half way has
            # opened a figure nobody else will close.
            open_before = set(plt.get_fignums())
            try:
                figure = draw()
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                figure.savefig(target, dpi=_DPI)
                written += 1
            except Exception as exc:
                logger.warning(
                    "diagnostic figure %s not saved: %s: %s",
                    root / relative, type(exc).__name__, exc)
            finally:
                for number in set(plt.get_fignums()) - open_before:
                    plt.close(number)
        logger.info("diagnostic figures: %d of %d saved under %s",
                    written, len(data), root)

    def load(self):
        raise NotImplementedError(
            f"{type(self).__name__} is written for people and MLflow, not read "
            f"back by a node: {self._filepath}")

    def exists(self) -> bool:
        return Path(self._filepath).is_dir()
