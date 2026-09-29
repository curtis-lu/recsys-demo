"""How each diagnostic figure is drawn — as a function the catalog calls later.

A diagnosis node returns ``{relative path: draw}`` and never touches the disk.
The ``DiagnosticFiguresDataset`` catalog entry calls each ``draw()`` when it
saves, writes the figure and closes it before drawing the next, and skips one
that raises with a warning (ADR-0030 decisions 4 and 7). Drawing at save
time is the point: a hundred-odd figures (22 items) never exist at once, and
a test can call a ``draw`` and inspect the figure without writing a file.

Each ``draw`` returns the figure it drew. What it captures are the arrays it
needs, sliced only when it runs.
"""

import numpy as np


def _pyplot():
    """pyplot on the Agg backend — nothing here opens a window."""
    import matplotlib

    if matplotlib.get_backend().lower() != "agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def beeswarm(values, features, feature_names, rows=None):
    """``shap.summary_plot`` of ``values`` against ``features`` — all rows, or
    the boolean mask ``rows``.

    Drawing only: the values are the adapter's attributions, computed
    already. ``summary_plot`` draws on pyplot's current figure, hence the
    ``plt.figure()`` first and ``gcf()`` last.
    """
    def draw():
        import shap

        plt = _pyplot()
        v, x = (values, features) if rows is None else (values[rows], features[rows])
        plt.figure()
        shap.summary_plot(v, features=x, feature_names=feature_names, show=False)
        plt.tight_layout()
        return plt.gcf()

    return draw


def signed_bars(row_values, feature_names, top_k, title):
    """One row's signed attributions as horizontal bars: the ``top_k``
    largest by magnitude, the largest on top, red pushing the score up and
    blue pulling it down."""
    def draw():
        plt = _pyplot()
        order = np.argsort(np.abs(row_values))[::-1][:top_k]
        # barh draws bottom-up; reverse so the largest contribution is on top.
        feats = [feature_names[i] for i in order][::-1]
        vals = [float(row_values[i]) for i in order][::-1]
        colors = ["tab:red" if v > 0 else "tab:blue" for v in vals]
        fig = plt.figure(figsize=(8, max(2.0, 0.4 * len(feats))))
        ax = fig.add_subplot(111)
        ax.barh(range(len(feats)), vals, color=colors)
        ax.set_yticks(range(len(feats)))
        ax.set_yticklabels(feats, fontsize=8)
        ax.axvline(0, color="black", linewidth=0.6)
        ax.set_xlabel("signed SHAP (log-odds)")
        ax.set_title(title, fontsize=9)
        for y, v in enumerate(vals):
            ax.text(v, y, f" {v:+.3f}", va="center",
                    ha="left" if v >= 0 else "right", fontsize=7)
        fig.tight_layout()
        return fig

    return draw
