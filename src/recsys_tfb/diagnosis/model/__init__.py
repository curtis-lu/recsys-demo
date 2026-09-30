"""The one piece of the training diagnoses that lives in this library.

The seven diagnosis nodes are defined in ``pipelines/training/nodes.py`` and
their mechanisms live in ``pipelines/training/steps/`` (ADR-0030 decision 6):
training was their only consumer, so the directory listing now says so.
``diagnostics_dir`` stays because HPO's search diagnostics
(``diagnosis/hpo``) write under the same directory, and a library module may
not import a pipeline (S8) or its ``steps/`` (S3).
"""

from .paths import diagnostics_dir

__all__ = ["diagnostics_dir"]
