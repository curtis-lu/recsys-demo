"""tune_hyperparameters 必須在做任何事之前先釋放 SparkSession。

Runner 是嚴格循序的（`core/runner.py` 的 `for node in pipeline.nodes:`），所以
「函式體第一行」在構造上就等於「所有排在它前面的節點都跑完之後」。

這比新增一個 DAG 節點可靠：`Pipeline._topological_sort` 的初始佇列含**所有**零入度
節點（`core/pipeline.py`），它們依宣告順序排在最前面；而 `tune_hyperparameters`
並沒有消費 `test_parquet_handle`，DAG 不會強制 `cache_test_model_input` 排在它之前。
用資料依賴去表達時間約束，日後任何新增的前置 Spark 節點都得記得掛進 release 的
inputs，忘了就靜默失效。
"""

import pytest

import recsys_tfb.pipelines.training.nodes as nodes


class _ReleasedFirst(Exception):
    """Sentinel: raised from the patched release to prove it ran first."""


def test_tune_hyperparameters_releases_spark_before_anything_else(monkeypatch):
    calls = []

    def _fake_release(parameters):
        calls.append(parameters)
        raise _ReleasedFirst

    monkeypatch.setattr(nodes, "release_spark_session", _fake_release)

    # 全部傳 None：若 release 不是第一個語句，函式會先在別處炸（TypeError /
    # KeyError），而不是丟出我們的 sentinel。
    with pytest.raises(_ReleasedFirst):
        nodes.tune_hyperparameters(None, None, None, None, {"training": {}})

    assert len(calls) == 1
    assert calls[0] == {"training": {}}
