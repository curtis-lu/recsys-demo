"""Dataset classes and cached-input handles.

``ModelAdapterDataset`` is deliberately not re-exported here: it imports
``models``, whose LightGBM adapter imports ``io``, and a re-export made the
two packages depend on each other at load time — whether a cold import
worked depended on which package loaded first (ADR-0030 decision 15).
Import it from ``recsys_tfb.io.model_adapter_dataset``.
"""

from recsys_tfb.io.base import AbstractDataset
from recsys_tfb.io.json_dataset import JSONDataset
from recsys_tfb.io.parquet_dataset import ParquetDataset
from recsys_tfb.io.pickle_dataset import PickleDataset
from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
