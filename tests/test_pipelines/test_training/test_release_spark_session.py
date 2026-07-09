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


@pytest.mark.parametrize("enable_calibration", [False, True])
def test_all_cache_nodes_run_before_tune_hyperparameters(enable_calibration):
    """釘住 release 生效所依賴的隱含前提:所有用 Spark 的 cache_* 節點都排在 HPO 之前。

    tune_hyperparameters 只消費 train_lgb_handle / train_dev_lgb_handle /
    val_parquet_handle,DAG 並不強制 cache_test_model_input（或 calibration）排在
    它之前——目前的順序來自 Kahn 排序把零入度節點依宣告順序排到最前面。若日後有人
    讓某個 cache 節點多吃一個上游輸出（入度變 >0），它就會落到 release 之後,靜默把
    session 又建回來。這個測試讓那件事變成紅燈而不是無聲的效能退化。
    """
    from recsys_tfb.pipelines.training.pipeline import create_pipeline

    names = [n.name for n in create_pipeline(enable_calibration).nodes]
    tune_at = names.index("tune_hyperparameters")

    cache_nodes = [n for n in names if n.startswith("cache_")]
    assert cache_nodes, "找不到任何 cache_* 節點,測試前提已失效"

    for cache in cache_nodes:
        assert names.index(cache) < tune_at, (
            f"{cache} 排在 tune_hyperparameters 之後,"
            f"release 之後它會重新建立 SparkSession。順序: {names}"
        )
