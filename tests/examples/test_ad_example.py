"""廣告情境示例（examples/ad/）裡不起 Spark 就驗得到的部分。

整條實跑（source_etl → evaluation）在 examples/ad/run_e2e.sh。這裡守三件
「改了 repo 別處，這份示例會靜默壞掉」的事：

1. 框架新增必填鍵或不變量時，這份 conf 在 CLI 入口就過不了。銀行示例的 conf
   有一大堆測試在讀；這份沒有，不在這裡擋，要等有人真的去跑才會發現。
2. 產生器實際產出的 item 與 conf 逐一列出的清單要一致。item 是 SQL 把兩個屬性
   拼出來的，兩邊各寫各的，任一邊改了另一邊不會跟著動。
3. 後面幾張票（event 角色、多張特徵表、item 清單從資料數）要用的資料形狀真的在原始資料裡。
   即時特徵與快照的「該算出什麼」照 check_features.py 的定義算——run_e2e.sh 拿同一份
   定義去比 SQL 的輸出，這裡只看原始資料有沒有那個形狀。
"""
import re
from pathlib import Path

import pandas as pd
import pytest
import yaml

from examples.ad.check_features import browse_counts, realtime_features, weekly_profile
from examples.ad.generate_data import (
    ITEM_SEPARATOR, LATE_ITEM, POST_CLICK_BROWSE_SECONDS, WEEKS, generate, week_of,
)
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
    # item 在不只一支 SQL 裡拼（候選的 label_table、即時特徵的 feature_realtime），掃全部
    pattern = r"concat\(\s*(?:\w+\.)?campaign_id\s*,\s*'([^']*)'\s*,\s*(?:\w+\.)?creative_format\s*\)"
    separators = {
        path.relative_to(CONF / "sql" / "etl").as_posix(): set(re.findall(pattern, path.read_text()))
        for path in sorted((CONF / "sql" / "etl").rglob("*.sql"))
    }
    building = {f: s for f, s in separators.items() if s}
    assert {"label/label_table.sql", "feature/feature_realtime.sql"} <= set(building)
    assert building == {f: {ITEM_SEPARATOR} for f in building}


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
        set(ds["train_snap_dates"])
        | set(ds["val_snap_dates"]) | set(ds["test_snap_dates"])
        | set(params["inference"]["snap_dates"])
    )
    for stage in ("feature_etl", "label_etl", "sample_pool_etl", "inference_population_etl"):
        assert needed <= set(params[stage]["target_dates"]), stage
    # 第一週只當特徵的回看窗，不排序：它之前沒有行為紀錄
    assert WEEKS[0] not in needed
    assert set(params["feature_etl"]["target_dates"]) == set(WEEKS[1:])


def test_the_old_calibration_week_is_still_produced_but_used_by_no_split(params):
    """#414 移除 calibration 之後，2025-12-15 那一週不屬於任何 split。

    ETL 的目標日期刻意不動（#416 要拿升級前後的每一層產物逐層比對，改了 ETL
    範圍就會多出一個與本次無關的差異）。所以這一週照樣產生、照樣落地，只是沒有
    split 讀它——這個不對稱是刻意的，寫成測試才不會被當成漏掉的設定補回去。
    """
    ds = params["dataset"]
    orphan = "2025-12-15"
    in_a_split = (
        set(ds["train_snap_dates"]) | set(ds["val_snap_dates"])
        | set(ds["test_snap_dates"]) | set(params["inference"]["snap_dates"])
    )
    assert orphan not in in_a_split
    for stage in ("feature_etl", "label_etl", "sample_pool_etl"):
        assert orphan in set(params[stage]["target_dates"]), stage


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


def test_an_item_first_appears_after_train(params, raw):
    # item 清單從資料數（#379）要驗「驗證期間的新 item 只警告」。新的是組合，不是屬性：
    # 活動與格式各自早就出現過。模型把 item 當一個類別值，看到的是沒見過的值；item 宣告成
    # 多欄（#394）也不改這一點，那張票省掉的是拼欄與逐一列出組合
    log = raw["impression_log"].assign(week=lambda d: week_of(d["event_date"]))
    campaign, fmt = LATE_ITEM
    is_late = (log["campaign_id"] == campaign) & (log["creative_format"] == fmt)
    first_week = log.loc[is_late, "week"].min()
    ds = params["dataset"]
    assert first_week in ds["val_snap_dates"]
    assert first_week > max(ds["train_snap_dates"])
    before = log[log["week"] < first_week]
    assert campaign in set(before["campaign_id"]) and fmt in set(before["creative_format"])


def test_realtime_interest_differs_between_impressions_of_the_same_item(raw):
    # ADR-0021 不在來源 SQL 把多次曝光聚合成一列的理由：同一素材的多次曝光，當下的即時
    # 特徵不同。要有不小的比例才有東西可學，只「存在」不夠
    rt = realtime_features(raw)
    recent = rt["browse_same_category_30m"] > 0
    keys = [rt["snap_date"], rt["user_id"], rt["slot_id"], rt["ad_creative"]]
    shown_more_than_once = recent.groupby(keys).size() > 1
    varies = recent.groupby(keys).nunique() > 1
    assert varies[shown_more_than_once].mean() > 0.2


def test_clicks_follow_recent_same_category_browsing(raw):
    rt = realtime_features(raw).merge(raw["impression_log"][["impression_id", "clicked"]], on="impression_id")
    recent = rt["browse_same_category_30m"] > 0
    assert rt.loc[recent, "clicked"].mean() > 1.5 * rt.loc[~recent, "clicked"].mean()


def test_peeking_past_the_impression_sees_the_click(raw):
    # 偷看的陷阱真的存在：點擊後幾分鐘內會瀏覽同類內容，窗口放到曝光之後，
    # 「有沒有瀏覽」幾乎就是「有沒有點」。feature_realtime.sql 的上界寫錯，特徵就是答案
    after = browse_counts(raw, 0, POST_CLICK_BROWSE_SECONDS + 1).merge(
        raw["impression_log"][["impression_id", "clicked"]], on="impression_id")
    peeked = after["same_category"] > 0
    assert peeked[after["clicked"] == 1].mean() > 0.9
    assert peeked[after["clicked"] == 0].mean() < 0.1
    # 連 < 寫成 <= 都會偷看：有不少點擊後的瀏覽就記在曝光那一秒
    same_second = browse_counts(raw, 0, 1).merge(
        raw["impression_log"][["impression_id", "clicked"]], on="impression_id")
    assert (same_second.loc[same_second["clicked"] == 1, "same_category"] > 0).mean() > 0.3


def test_user_profile_is_daily_and_ready_only_after_the_day_ends(raw):
    profile = raw["user_profile"]
    days = pd.to_datetime(profile["snap_date"])
    assert (profile.groupby("snap_date")["user_id"].nunique() == profile["user_id"].nunique()).all()
    assert (days.drop_duplicates().sort_values().diff().dropna() == pd.Timedelta(days=1)).all()
    # 快照 D 記 D 當天結束時的狀態，所以最早 D+1 00:00 才可能算好
    assert (profile["available_at"] >= days + pd.Timedelta(days=1)).all()


def test_the_same_day_snapshot_differs_from_the_one_ready_at_week_start(raw):
    # as-of（ADR-0022）有意義的前提之一：週日或週一換了裝置的人，「週一那份」記的是
    # 週一結束時的狀態，週一 00:00 還不存在
    as_of = weekly_profile(raw, WEEKS[1:])
    same_day = raw["user_profile"].assign(snap_date=lambda d: d["snap_date"].astype(str))
    joined = as_of.merge(same_day, on=["snap_date", "user_id"], suffixes=("", "_same_day"))
    assert len(joined) == len(as_of)
    assert (joined["device_type"] != joined["device_type_same_day"]).any()


def test_which_snapshot_is_ready_depends_on_available_at_not_only_the_date(raw):
    # 前提之二：只看日期、固定取前兩天（週六）那份的寫法不對。批次晚一天的那週，
    # 週一 00:00 只拿得到週五那份——SQL 必須讀 available_at
    as_of = weekly_profile(raw, WEEKS[1:])
    two_days_before = (pd.to_datetime(as_of["snap_date"]) - pd.Timedelta(days=2)).dt.strftime("%Y-%m-%d")
    assert (as_of["profile_snap_date"] != two_days_before).any()
    assert (as_of["profile_snap_date"] == two_days_before).any()
