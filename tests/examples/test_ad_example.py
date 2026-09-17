"""廣告情境示例（examples/ad/）裡不起 Spark 就驗得到的部分。

整條實跑（source_etl → evaluation）在 examples/ad/run_e2e.sh。這裡守三件
「改了 repo 別處，這份示例會靜默壞掉」的事：

1. 框架新增必填鍵或不變量時，這份 conf 在 CLI 入口就過不了。銀行示例的 conf
   有一大堆測試在讀；這份沒有，不在這裡擋，要等有人真的去跑才會發現。
2. 產生器實際產出的 item 與 conf 逐一列出的清單要一致。item 是 SQL 把兩個屬性
   拼出來的，兩邊各寫各的，任一邊改了另一邊不會跟著動。
3. 後面幾張票（event 角色、多張特徵表）要用的資料形狀真的在原始資料裡。
"""
import re
from pathlib import Path

import pandas as pd
import pytest
import yaml

from examples.ad.generate_data import ITEM_SEPARATOR, WEEKS, generate, week_of
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.consistency import resolved_env_dir, validate_config_consistency
from recsys_tfb.core.schema import validate_schema_config

REPO = Path(__file__).resolve().parents[2]
CONF = REPO / "examples" / "ad" / "conf"
ROOT_CONF = REPO / "conf"


@pytest.fixture(scope="module")
def params():
    resolved_env_dir(CONF, "local")
    return ConfigLoader(str(CONF), env="local").get_parameters()


@pytest.fixture(scope="module")
def raw():
    # 小母體：只為了看形狀，不為了統計量
    return generate(n_users=60, seed=7)


def test_conf_passes_the_cli_entry_gate(params):
    # 與 __main__._load_config_and_setup 同一組檢查，每個指令一進來都跑
    validate_schema_config(params)
    validate_config_consistency(params)


def test_schema_roles_follow_adr_0021(params):
    cols = params["schema"]["columns"]
    # source_etl 的輸出檢查把 WHERE snap_date = … 寫死，time 欄換名 source_etl 就壞
    assert cols["time"] == "snap_date"
    assert isinstance(cols["entity"], list) and len(cols["entity"]) == 2
    assert isinstance(cols["item"], str)
    # 讀原始 params 而不是 get_schema()：後者只保留已知角色，宣告了也看不到
    assert "event" not in cols


def test_generated_items_equal_the_declared_list(params, raw):
    item_col = params["schema"]["columns"]["item"]
    log = raw["impression_log"]
    # 拼法與 label_table.sql 相同，由下一個測試守
    in_data = set(log["campaign_id"] + ITEM_SEPARATOR + log["creative_format"])
    assert in_data == set(params["schema"]["categorical_values"][item_col])


def test_sql_builds_item_with_the_generator_separator():
    sql = (CONF / "sql" / "etl" / "label" / "label_table.sql").read_text()
    separators = re.findall(r"concat\(\s*i\.campaign_id\s*,\s*'([^']*)'\s*,\s*i\.creative_format\s*\)", sql)
    assert separators, "label_table.sql 裡找不到拼 item 的 concat"
    assert set(separators) == {ITEM_SEPARATOR}


def test_catalog_has_the_same_entries_as_the_root_conf():
    # 這份 catalog 是從根目錄 conf/base/catalog.yaml 複製改欄名的；框架新增一個條目時
    # 兩邊要一起加，否則示例要到實跑才壞
    ad = yaml.safe_load((CONF / "base" / "catalog.yaml").read_text())
    root = yaml.safe_load((ROOT_CONF / "base" / "catalog.yaml").read_text())
    assert set(ad) == set(root)
    assert {k: v["type"] for k, v in ad.items()} == {k: v["type"] for k, v in root.items()}


def test_etl_target_dates_cover_every_split_date(params):
    ds = params["dataset"]
    needed = (
        set(ds["train_snap_dates"]) | set(ds["calibration_snap_dates"])
        | set(ds["val_snap_dates"]) | set(ds["test_snap_dates"])
        | set(params["inference"]["snap_dates"])
    )
    for stage in ("feature_etl", "label_etl", "sample_pool_etl", "inference_population_etl"):
        assert needed <= set(params[stage]["target_dates"]), stage
    # 第一週只當特徵的回看窗，不排序：它之前沒有行為紀錄
    assert WEEKS[0] not in needed
    assert set(params["feature_etl"]["target_dates"]) == set(WEEKS[1:])


def test_generator_is_deterministic():
    a = generate(n_users=30, seed=3)
    b = generate(n_users=30, seed=3)
    for name in a:
        pd.testing.assert_frame_equal(a[name], b[name])


def test_every_week_has_impressions_and_none_fall_outside(raw):
    log = raw["impression_log"]
    ts = pd.to_datetime(log["event_ts"])
    # SQL 用 event_date 分週，它必須就是 event_ts 那一天
    assert (pd.to_datetime(log["event_date"]) == ts.dt.normalize()).all()
    assert set(week_of(log["event_date"])) == set(WEEKS)


def test_same_item_is_shown_more_than_once_in_a_query_group(raw):
    # event 角色（ADR-0021、#378）要分辨的就是這種列；目前的 label_table.sql 先把它們聚合成一列
    log = raw["impression_log"].assign(week=lambda d: week_of(d["event_date"]))
    keys = ["week", "user_id", "slot_id", "campaign_id", "creative_format"]
    assert log.duplicated(subset=keys).any()
    assert log["impression_id"].is_unique


def test_some_query_group_has_more_impressions_than_items(params, raw):
    # event 角色落地後一組的列數可以超過 item 種數；k_values 的 "all" 不得在這種組上
    # 被截斷（ADR-0021 決定 4）。靠的是 SLOT_IMPRESSION_RATE 夠高
    log = raw["impression_log"].assign(week=lambda d: week_of(d["event_date"]))
    n_items = len(params["schema"]["categorical_values"][params["schema"]["columns"]["item"]])
    assert log.groupby(["week", "user_id", "slot_id"]).size().max() > n_items


def test_query_groups_see_different_numbers_of_items(raw):
    log = raw["impression_log"].assign(
        week=lambda d: week_of(d["event_date"]),
        item=lambda d: d["campaign_id"] + ITEM_SEPARATOR + d["creative_format"],
    )
    per_group = log.groupby(["week", "user_id", "slot_id"])["item"].nunique()
    assert per_group.nunique() > 1
