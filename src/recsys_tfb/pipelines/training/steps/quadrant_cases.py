"""How one quadrant case is written down: its manifest entry, its chart (path,
title, drawing), and the identity two cases are compared by.

Which cases there are, and what an empty cell or a one-row cell records, is
``compute_quadrant_cases``'s decision (``nodes.py``).
"""

from recsys_tfb.pipelines.training.steps.figures import safe_name, signed_bars

#: Where the ``case_figures`` catalog entry writes, relative to the
#: diagnostics directory: the manifest's ``png`` paths are relative to that
#: directory, as they always were. The one place that knows the two catalog
#: entries sit side by side (``conf/base/catalog.yaml``).
_CASES_SUBDIR = "cases"


def _case_entry(meta_row, figure_path, case_label_cols):
    # The manifest keys are the schema's own column names (a general
    # framework: no example deployment's time or entity column is written
    # in). ``case_label_cols``: see ``compute_quadrant_cases`` — the identity
    # without the item.
    entry = {"rendered": True, "png": f"{_CASES_SUBDIR}/{figure_path}"}
    entry.update({c: str(meta_row[c]) for c in case_label_cols})
    entry.update({"rank": int(meta_row["rank"]), "score": float(meta_row["score"]),
                  "label": int(meta_row["label"])})
    return entry


def _case_title(item, quadrant, role, entry):
    return (f"{item} · {quadrant} · {role} · score={entry['score']:.3f}"
            f" · rank={entry['rank']} · label={entry['label']}")


def case_chart(row, row_values, item, quadrant, role, feature_cols, top_k,
               case_label_cols):
    """One case's ``(figure path, manifest entry, draw)``: the path is where
    the chart lands under the cases directory, the entry is what the manifest
    records for it, and the draw is its signed-attribution bar chart."""
    figure_path = f"{safe_name(item)}/{quadrant}_{role}.png"
    entry = _case_entry(row, figure_path, case_label_cols)
    draw = signed_bars(
        row_values, feature_cols, top_k, _case_title(item, quadrant, role, entry))
    return figure_path, entry, draw


def row_identity(row, identity_cols):
    """The row's identity as a tuple of text: how a cell's high and low are
    recognised as one and the same row."""
    return tuple(str(row[c]) for c in identity_cols)
