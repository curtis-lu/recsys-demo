"""會「往回看」的特徵，SQL 算出來的值與照定義用 pandas 重算的值逐列相同。

「往回算」是來源 SQL 的責任，框架不檢查有沒有偷看（ADR-0022 決定 2、3），所以這個示例
自己檢查。偷看不會讓任何東西報錯：多算到曝光之後的瀏覽、取到還沒算好的快照，SQL 照樣
跑完，模型離線還變好。能擋下它的只有「跟一份寫明時間界線的定義逐值比」。

- ``feature_realtime``（一次曝光一列）：瀏覽只算 ``[曝光前 30 分鐘, 曝光那一秒)``；
  同一週同一版位同一素材在這次之前曝光過幾次。
- ``feature_user.profile_snap_date``（一週一個使用者一列）：這一週開始那一刻（週一 00:00）
  已經算好（``available_at`` 不晚於那一刻）的最後一份快照是哪一天。
- ``feature_table``（框架讀的那張，一週一個使用者 × 版位一列）：每一欄特徵——上面那份快照
  的屬性，以及只數週一之前 28 天的曝光與點擊。

定義寫在這裡的 pandas 函式，tests/examples/test_ad_example.py 也用同一份檢查原始資料的形狀。
資料本身讓錯的界線看得出來：點擊後一半的瀏覽與點擊同一秒（``<`` 寫成 ``<=`` 會多算）；
快照批次週末常晚一天（只看日期、取「前兩天那份」會取到還沒算好的）。

用法（在 examples/ad/ 底下、feature_etl 跑完之後，由 run_e2e.sh 呼叫）：
    python check_features.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from generate_data import (  # noqa: E402
    CAMPAIGN_CATEGORY, ITEM_SEPARATOR, RECENT_WINDOW_SECONDS, generate, week_of,
)

REALTIME_COLUMNS = ["browse_30m", "browse_same_category_30m", "prior_exposures_this_week"]
ACTIVITY_DAYS = 28
PROFILE_COLUMNS = ["age_band", "device_type", "region", "tenure_days"]
FEATURE_TABLE_COLUMNS = [
    *PROFILE_COLUMNS,
    "user_imp_4w", "user_click_4w", "user_ctr_4w",
    "slot_position", "page_type", "slot_imp_4w", "slot_ctr_4w",
    "us_imp_4w", "us_click_4w",
]


def _seconds(ts: pd.Series) -> np.ndarray:
    return ts.to_numpy("datetime64[s]").astype(np.int64)


def browse_counts(tables: dict, start: int, end: int) -> pd.DataFrame:
    """每次曝光，瀏覽落在 ``[曝光那一秒 + start, 曝光那一秒 + end)`` 的次數：全部類別、與活動同類。"""
    imp, browse = tables["impression_log"], tables["browse_log"]
    b_sec = _seconds(browse["event_ts"])
    by_user = {u: np.sort(b_sec[i]) for u, i in browse.groupby("user_id").indices.items()}
    by_user_category = {k: np.sort(b_sec[i]) for k, i in browse.groupby(["user_id", "content_category"]).indices.items()}

    sec = _seconds(imp["event_ts"])
    lo, hi = sec + start, sec + end
    categories = imp["campaign_id"].map(CAMPAIGN_CATEGORY).to_numpy()
    empty = np.array([], dtype=np.int64)
    any_category = np.empty(len(imp), dtype=np.int64)
    same_category = np.empty(len(imp), dtype=np.int64)
    for n, (user, category) in enumerate(zip(imp["user_id"].to_numpy(), categories)):
        a = by_user.get(user, empty)
        any_category[n] = np.searchsorted(a, hi[n]) - np.searchsorted(a, lo[n])
        a = by_user_category.get((user, category), empty)
        same_category[n] = np.searchsorted(a, hi[n]) - np.searchsorted(a, lo[n])
    return pd.DataFrame({
        "impression_id": imp["impression_id"],
        "any_category": any_category,
        "same_category": same_category,
    })


def realtime_features(tables: dict) -> pd.DataFrame:
    """feature_realtime.sql 該算出的每一列：只看曝光那一秒之前。"""
    imp = tables["impression_log"]
    out = imp[["impression_id", "request_id", "user_id", "slot_id", "event_ts"]].assign(
        snap_date=week_of(imp["event_date"]),
        ad_creative=imp["campaign_id"] + ITEM_SEPARATOR + imp["creative_format"],
    )
    recent = browse_counts(tables, -RECENT_WINDOW_SECONDS, 0).rename(columns={
        "any_category": "browse_30m", "same_category": "browse_same_category_30m",
    })
    out = out.merge(recent, on="impression_id")
    ordered = out.sort_values(["event_ts", "impression_id"], kind="mergesort")
    out["prior_exposures_this_week"] = ordered.groupby(["snap_date", "user_id", "slot_id", "ad_creative"]).cumcount()
    return out


def weekly_profile(tables: dict, weeks: list[str]) -> pd.DataFrame:
    """feature_user.sql 該取的快照：每一週週一 00:00 已經算好的最後一份。"""
    profile = tables["user_profile"]
    frames = []
    for week in weeks:
        start = pd.Timestamp(week)
        ready = profile[profile["available_at"] <= start]
        latest = ready.sort_values(["user_id", "snap_date"]).drop_duplicates("user_id", keep="last")
        frames.append(latest.assign(
            profile_snap_date=latest["snap_date"].astype(str),
            snap_date=week,
            tenure_days=(start - pd.to_datetime(latest["signup_date"])).dt.days,
        )[["snap_date", "user_id", "profile_snap_date", *PROFILE_COLUMNS]])
    return pd.concat(frames, ignore_index=True)


def weekly_features(tables: dict, weeks: list[str]) -> pd.DataFrame:
    """feature_table.sql 該算出的每一列：週一 00:00 拿得到的快照，加上週一之前 28 天的曝光。"""
    imp = tables["impression_log"]
    event_day = pd.to_datetime(imp["event_date"])
    profile = weekly_profile(tables, weeks)
    frames = []
    for week in weeks:
        start = pd.Timestamp(week)
        past = imp[(event_day >= start - pd.Timedelta(days=ACTIVITY_DAYS)) & (event_day < start)]

        def counts(keys: list[str], prefix: str) -> pd.DataFrame:
            grouped = past.groupby(keys)["clicked"]
            return pd.DataFrame({f"{prefix}_imp_4w": grouped.size(), f"{prefix}_click_4w": grouped.sum()}).reset_index()

        frames.append(
            profile[profile["snap_date"] == week]
            .merge(tables["slot_dim"], how="cross")
            .merge(counts(["user_id"], "user"), on="user_id", how="left")
            .merge(counts(["slot_id"], "slot"), on="slot_id", how="left")
            .merge(counts(["user_id", "slot_id"], "us"), on=["user_id", "slot_id"], how="left")
        )
    out = pd.concat(frames, ignore_index=True)
    count_columns = ["user_imp_4w", "user_click_4w", "slot_imp_4w", "slot_click_4w", "us_imp_4w", "us_click_4w"]
    out[count_columns] = out[count_columns].fillna(0).astype("int64")
    # 過去 28 天沒有曝光時，點擊率是 NULL，不是 0
    out["user_ctr_4w"] = (out["user_click_4w"] / out["user_imp_4w"]).where(out["user_imp_4w"] > 0)
    out["slot_ctr_4w"] = (out["slot_click_4w"] / out["slot_imp_4w"]).where(out["slot_imp_4w"] > 0)
    return out


def _mismatches(want: pd.DataFrame, got: pd.DataFrame, key: list[str], columns: list[str], name: str) -> list[str]:
    merged = want.merge(got, on=key, how="outer", suffixes=("_want", "_got"), indicator=True)
    failures = []
    for side, label in (("left_only", "SQL 少了"), ("right_only", "SQL 多了")):
        extra = merged[merged["_merge"] == side]
        if len(extra):
            failures.append(f"{name}: {label} {len(extra)} 列，例如 {extra[key].head(3).to_dict('records')}")
    both = merged[merged["_merge"] == "both"]
    for col in columns:
        a, b = both[f"{col}_want"], both[f"{col}_got"]
        diff = both[~((a == b) | (a.isna() & b.isna()))]
        if len(diff):
            sample = diff[key + [f"{col}_want", f"{col}_got"]].head(3).to_dict("records")
            failures.append(f"{name}.{col}: {len(diff)} 列與定義不同，例如 {sample}")
    return failures


def _as_iso_date(frame: pd.DataFrame, *columns: str) -> None:
    for col in columns:
        frame[col] = pd.to_datetime(frame[col]).dt.strftime("%Y-%m-%d")


def main() -> None:
    if Path.cwd().resolve() != HERE:
        sys.exit(f"要在 {HERE} 底下執行（目前在 {Path.cwd()}）：conf/ 與 data/ 都相對目前目錄")
    if not os.environ.get("SPARK_CONF_DIR"):
        sys.exit("SPARK_CONF_DIR 沒設：讀不到 feature_etl 寫的表")

    from pyspark.sql import SparkSession

    from recsys_tfb.core.config import ConfigLoader

    params = ConfigLoader("conf", env="local").get_parameters()["feature_etl"]
    db = params["variables"]["target_db"]
    weeks = sorted(params["target_dates"])

    # 與 setup_local.py 寫進 Hive 的是同一份（產生器是決定性的）
    tables = generate()
    want_realtime = realtime_features(tables)
    want_realtime = want_realtime[want_realtime["snap_date"].isin(weeks)]
    want_profile = weekly_profile(tables, weeks)
    want_features = weekly_features(tables, weeks)

    spark = SparkSession.builder.appName("ad_example_check_features").getOrCreate()
    try:
        got_realtime = spark.table(f"{db}.feature_realtime").toPandas()
        got_profile = spark.table(f"{db}.feature_user").select("snap_date", "user_id", "profile_snap_date").toPandas()
        got_features = spark.table(f"{db}.feature_table").toPandas()
    finally:
        spark.stop()
    _as_iso_date(got_realtime, "snap_date")
    _as_iso_date(got_profile, "snap_date", "profile_snap_date")
    _as_iso_date(got_features, "snap_date")

    failures = [
        *_mismatches(want_realtime, got_realtime, ["impression_id"],
                     # identity 各欄也比：dataset 以它們接這張表（ADR-0026），接錯鍵比算錯值更難發現
                     ["snap_date", "user_id", "slot_id", "request_id", "ad_creative", "event_ts",
                      *REALTIME_COLUMNS],
                     "feature_realtime"),
        *_mismatches(want_profile, got_profile, ["snap_date", "user_id"], ["profile_snap_date"], "feature_user"),
        *_mismatches(want_features, got_features, ["snap_date", "user_id", "slot_id"], FEATURE_TABLE_COLUMNS,
                     "feature_table"),
    ]
    if failures:
        print("\n".join("  ✗ " + f for f in failures), file=sys.stderr)
        sys.exit(1)
    print(f"  ✓ feature_realtime: {len(got_realtime)} 列與定義逐值相同（瀏覽只算曝光前 30 分鐘、不含曝光那一秒）")
    print(f"  ✓ feature_user: {len(got_profile)} 列取的快照日都是「週一 00:00 已算好的最後一份」")
    print(f"  ✓ feature_table: {len(got_features)} 列的 {len(FEATURE_TABLE_COLUMNS)} 欄特徵與定義逐值相同")


if __name__ == "__main__":
    main()
