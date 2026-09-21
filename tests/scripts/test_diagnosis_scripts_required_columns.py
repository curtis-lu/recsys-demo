"""The five standalone diagnosis scripts keep the query group's columns.

Each script ``select``s its ``required_columns`` before drawing the shared
diagnosis sample, and the sample groups by ``query_group_columns``. A list
spelled ``time + entity`` by hand drops an ``occasion`` column there, and the
sample then fails with an unresolved-column error that names neither the role
nor the script (#428).
"""
import importlib

import pytest

from recsys_tfb.core.schema import get_schema

_PARAMS = {"schema": {"columns": {
    "time": "snap_date", "entity": ["user_id", "slot_id"],
    "item": "ad_creative", "occasion": "request_id",
}}}

_SCRIPTS = [
    "config_sorting_shift_diagnosis",
    "item_ability_diagnosis",
    "per_item_score_shift_diagnosis",
    "per_item_score_shift_optuna_diagnosis",
    "suppression_ledger_diagnosis",
]


def _required(name):
    module = importlib.import_module(f"scripts.{name}")
    schema = get_schema(_PARAMS)
    try:
        return module.required_columns(_PARAMS, schema)
    except TypeError:
        return module.required_columns(schema)


@pytest.mark.parametrize("name", _SCRIPTS)
def test_required_columns_carry_the_whole_query_group(name):
    cols = _required(name)
    assert set(get_schema(_PARAMS)["query_group_columns"]) <= set(cols)
