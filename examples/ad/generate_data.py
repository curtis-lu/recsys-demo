"""廣告情境的合成「上游原始表」：曝光紀錄、使用者每週快照、版位維度、週曆。

產出的是 source_etl **之前**的表（寫進 Hive 的 ``ad_raw`` 資料庫），不是框架的
三張來源表——那三張由 ``conf/sql/etl/`` 的 SQL 從這裡算出來。這點和銀行示例
刻意不同：銀行示例直接產出來源表、完全跳過 source_etl（見
``docs/pipelines/source_etl.md`` §2 末段），所以 source_etl 在本機從沒被實跑過。

資料形狀依 ADR-0021：一次曝光一列、帶到秒的時間；``time`` 是週（每週一），
``entity`` 是使用者 × 版位，``item`` 是活動 × 素材格式在 SQL 裡拼成的一欄。

刻意留給後面幾張票用、但本票的 SQL 不用的形狀：

- 同一週、同一使用者、同一版位、同一素材被曝光不只一次（``event`` 角色）。
- 使用者特徵與版位特徵各有自己的粒度（多張特徵表各自宣告 join 欄位）。

數字不是生產的樣子：點擊率刻意調高到 5–15%，因為母體只有幾百人，照真實的 1%
一週只會有個位數正例。生產規模的估算不在本示例的範圍（見 README）。

用法（在 examples/ad/ 底下）：
    python generate_data.py            # 寫 data/raw/*.parquet
"""
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 20260105
N_USERS = 400

# 每週一。第一週只當特徵的回看窗（它之前沒有曝光紀錄），不進任何 split。
WEEKS = [
    "2025-10-27", "2025-11-03", "2025-11-10", "2025-11-17", "2025-11-24",
    "2025-12-01", "2025-12-08", "2025-12-15", "2025-12-22", "2025-12-29",
    "2026-01-05",
]

# item ＝ campaign_id + ITEM_SEPARATOR + creative_format（在 label／sample_pool SQL 拼）。
# 用 "-" 而不是 "|"：item 值會成為 Hive 分區目錄名、MLflow 指標名與檔名的一部分，
# 那幾處對特殊字元的處理本票不驗（規劃檔 P7a、P8 的範圍）。
ITEM_SEPARATOR = "-"
CAMPAIGNS = ["c01", "c02", "c03", "c04"]
FORMATS = ["banner", "video", "native"]

# 曝光分配權重：刻意長尾，讓冷門 item 的指標有東西可看
CAMPAIGN_WEIGHT = {"c01": 0.40, "c02": 0.30, "c03": 0.20, "c04": 0.10}
FORMAT_WEIGHT = {"banner": 0.55, "video": 0.25, "native": 0.20}

SLOTS = pd.DataFrame({
    "slot_id": ["home_top", "feed_mid", "article_end"],
    "slot_position": ["top", "middle", "bottom"],
    "page_type": ["home", "feed", "article"],
})
SLOT_LOGIT = {"home_top": 0.4, "feed_mid": 0.0, "article_end": -0.5}
SLOT_IMPRESSION_RATE = {"home_top": 2.5, "feed_mid": 3.5, "article_end": 1.5}

AGE_BANDS = ["18-24", "25-34", "35-49", "50+"]
DEVICES = ["mobile", "desktop", "tablet"]
REGIONS = ["north", "central", "south", "east"]

# 年齡層偏好哪個活動、裝置偏好哪種格式：讓特徵真的帶訊號，模型才有東西可學
AGE_CAMPAIGN_AFFINITY = {"18-24": "c03", "25-34": "c01", "35-49": "c02", "50+": "c04"}
DEVICE_FORMAT_AFFINITY = {"mobile": "video", "desktop": "banner", "tablet": "native"}
BASE_LOGIT = -2.6


def _users(rng: np.random.Generator, n_users: int) -> pd.DataFrame:
    return pd.DataFrame({
        "user_id": [f"u{i:05d}" for i in range(1, n_users + 1)],
        "age_band": rng.choice(AGE_BANDS, size=n_users, p=[0.2, 0.35, 0.3, 0.15]),
        "device_type": rng.choice(DEVICES, size=n_users, p=[0.6, 0.3, 0.1]),
        "region": rng.choice(REGIONS, size=n_users),
        # 註冊日散在示例期間之前兩年內，SQL 由它算年資與分層
        "signup_date": [
            dt.date(2025, 10, 27) - dt.timedelta(days=int(d))
            for d in rng.integers(1, 730, size=n_users)
        ],
        # 看不到的個人點擊傾向；特徵只能從過去的點擊率間接學到
        "propensity": rng.normal(0.0, 0.6, size=n_users),
        "activity": rng.uniform(0.3, 1.0, size=n_users),
    })


def _user_profile(users: pd.DataFrame) -> pd.DataFrame:
    """每週一一份快照。示例裡屬性不隨時間變，但粒度照真實的每週快照給。"""
    frames = []
    for week in WEEKS:
        snap = dt.date.fromisoformat(week)
        frames.append(pd.DataFrame({
            "snap_date": snap,
            "user_id": users["user_id"],
            "age_band": users["age_band"],
            "device_type": users["device_type"],
            "region": users["region"],
            "signup_date": users["signup_date"],
        }))
    return pd.concat(frames, ignore_index=True)


def _impressions(rng: np.random.Generator, users: pd.DataFrame) -> pd.DataFrame:
    items = [(c, f) for c in CAMPAIGNS for f in FORMATS]
    item_p = np.array([CAMPAIGN_WEIGHT[c] * FORMAT_WEIGHT[f] for c, f in items])
    item_p = item_p / item_p.sum()

    rows = []
    for week in WEEKS:
        week_start = dt.datetime.fromisoformat(week)
        for user in users.itertuples(index=False):
            if rng.random() > user.activity:
                continue
            for slot_id, rate in SLOT_IMPRESSION_RATE.items():
                n = rng.poisson(rate)
                if n == 0:
                    continue
                picked = rng.choice(len(items), size=n, p=item_p)
                seconds = np.sort(rng.integers(0, 7 * 24 * 3600, size=n))
                seen: dict[int, int] = {}
                for idx, sec in zip(picked, seconds):
                    campaign, fmt = items[idx]
                    exposure = seen.get(idx, 0)
                    seen[idx] = exposure + 1
                    logit = (
                        BASE_LOGIT
                        + user.propensity
                        + SLOT_LOGIT[slot_id]
                        + (0.9 if AGE_CAMPAIGN_AFFINITY[user.age_band] == campaign else 0.0)
                        + (0.7 if DEVICE_FORMAT_AFFINITY[user.device_type] == fmt else 0.0)
                        - 0.5 * exposure  # 同一週看第二次以上，點擊意願下降
                    )
                    ts = week_start + dt.timedelta(seconds=int(sec))
                    rows.append((
                        ts, ts.date(), user.user_id, slot_id, campaign, fmt,
                        int(rng.random() < 1.0 / (1.0 + np.exp(-logit))),
                    ))
    log = pd.DataFrame(rows, columns=[
        "event_ts", "event_date", "user_id", "slot_id",
        "campaign_id", "creative_format", "clicked",
    ])
    log.insert(0, "impression_id", [f"imp{i:08d}" for i in range(1, len(log) + 1)])
    return log


def generate(n_users: int = N_USERS, seed: int = SEED) -> dict[str, pd.DataFrame]:
    """回傳三張原始表。同一組參數永遠產出同一份資料（基準 digest 靠這一點）。

    曝光紀錄沒有「週」欄：屬於哪一週由 SQL 以 ``event_date`` 落在
    ``[target_date, target_date + 7)`` 決定，這正是來源 SQL 該負責的事。
    """
    rng = np.random.default_rng(seed)
    users = _users(rng, n_users)
    return {
        "impression_log": _impressions(rng, users),
        "user_profile": _user_profile(users),
        "slot_dim": SLOTS.copy(),
        # 週曆：沒有日期欄的 SQL 從這裡取 snap_date（README〈踩到的框架問題〉規則 1）
        "week_calendar": pd.DataFrame({"snap_date": [dt.date.fromisoformat(w) for w in WEEKS]}),
    }


def week_of(dates: pd.Series) -> pd.Series:
    """日期所屬那一週的週一（ISO 字串），與 WEEKS 同格式。"""
    d = pd.to_datetime(dates)
    return (d - pd.to_timedelta(d.dt.weekday, unit="D")).dt.strftime("%Y-%m-%d")


def write_parquet(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        # Spark 3.3 只讀得懂微秒 timestamp；pandas 預設奈秒，讀的時候會炸
        # `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`。
        pq.write_table(
            pa.Table.from_pandas(df, preserve_index=False),
            out_dir / f"{name}.parquet",
            coerce_timestamps="us",
        )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    ap.add_argument("--n-users", type=int, default=N_USERS)
    args = ap.parse_args()

    tables = generate(n_users=args.n_users)
    write_parquet(tables, args.out)

    log = tables["impression_log"].assign(week=lambda d: week_of(d["event_date"]))
    item = log["campaign_id"] + ITEM_SEPARATOR + log["creative_format"]
    group = ["week", "user_id", "slot_id"]
    print(f"impression_log: {len(log)} 列，點擊率 {log['clicked'].mean():.3f}")
    print(f"  (週, 使用者, 版位, item) 組合 {log.assign(item=item).groupby(group + ['item']).ngroups} 個")
    print(f"  query group {log.groupby(group).ngroups} 個")
    print(f"  item {item.nunique()} 種：{sorted(item.unique())}")
    print(f"user_profile: {len(tables['user_profile'])} 列；slot_dim: {len(tables['slot_dim'])} 列")
    print(f"寫到 {args.out.resolve()}")


if __name__ == "__main__":
    main()
