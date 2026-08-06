"""Training-stage feature selection over a preprocessor view.

Lives in ``models/`` rather than ``pipelines/training/`` because the model
adapter reads it too (``models/lightgbm_adapter.py`` keys its cache sub-path on
it). Putting it under ``pipelines/`` would make ``models/`` import ``pipelines/``
— a reverse dependency the layering does not otherwise have.
"""

from __future__ import annotations


def feature_selection_exclude(parameters: dict) -> list[str]:
    """Return the training-stage feature exclusion list (empty if unset).

    Single reader for ``training.feature_selection.exclude`` so the cache-key
    hash, manifest provenance, and the metadata view all agree on the same
    normalized list.
    """
    fs = parameters.get("training", {}).get("feature_selection") or {}
    return list(fs.get("exclude") or [])


def apply_feature_selection(preprocessor_metadata: dict, parameters: dict) -> dict:
    """Return a training-only view of ``preprocessor_metadata`` with features dropped.

    Drops every column in ``training.feature_selection.exclude`` from
    ``feature_columns`` (and from ``categorical_columns``), preserving the
    original feature order so the numpy column layout, the ``feature_name``
    baked into the lgb ``.bin``, and the booster's reported names all stay
    aligned. ``category_mappings`` / ``drop_columns`` pass through untouched.

    This is a *training-stage* subset: the dataset-built ``preprocessor.json``
    keeps the full feature set (``base_dataset_version`` unchanged). Selection
    lives in the ``training:`` block, so it bumps ``model_version`` only.

    Empty / absent selection returns the input object unchanged, so non-selection
    runs are byte-identical to before. The input dict is never mutated.
    """
    exclude = feature_selection_exclude(parameters)
    if not exclude:
        return preprocessor_metadata

    exclude_set = set(exclude)
    view = dict(preprocessor_metadata)
    view["feature_columns"] = [
        c for c in preprocessor_metadata["feature_columns"] if c not in exclude_set
    ]
    view["categorical_columns"] = [
        c for c in preprocessor_metadata["categorical_columns"] if c not in exclude_set
    ]
    return view
