"""Tests for recsys_tfb.core.consistency."""

import pytest

from recsys_tfb.core.consistency import (
    ConsistencyError,
    ConfigConsistencyError,
    DataConsistencyError,
)


class TestExceptionHierarchy:
    def test_consistency_error_is_valueerror(self):
        assert issubclass(ConsistencyError, ValueError)

    def test_config_error_is_consistency_error(self):
        assert issubclass(ConfigConsistencyError, ConsistencyError)

    def test_data_error_is_consistency_error(self):
        assert issubclass(DataConsistencyError, ConsistencyError)
