"""廣告情境的合成「上游原始表」：曝光紀錄、瀏覽紀錄、使用者每日快照、活動與版位維度、週曆。

產出的是 source_etl **之前**的表（寫進 Hive 的 ``ad_raw`` 資料庫），不是框架的
三張來源表——那三張由 ``conf/sql/etl/`` 的 SQL 從這裡算出來。這點和銀行示例
刻意不同：銀行示例直接產出來源表、完全跳過 source_etl（見
``docs/pipelines/source_etl.md`` §2 末段），所以 source_etl 在本機從沒被實跑過。

資料形狀依 ADR-0021：一次曝光一列、帶到秒的時間；``time`` 是週（每週一），
``entity`` 是使用者 × 版位，``item`` 是活動 × 素材格式兩欄（conf 宣告成多欄，框架讀入時
拼成一欄，ADR-0027）。

一次請求可能展示好幾個素材（``request_id``，#428，形狀二）：同一使用者、同一版位、
同一秒（同一次請求）底下，若干素材同時被排序——``request_id`` 就是這個分組。
同一次請求裡的素材互不重複（產生器不放回抽樣），所以 conf 宣告 occasion ＝
request_id 就能讓 identity 唯一，不用再宣告 event（見
``examples/ad/conf/base/parameters.yaml``）。

刻意留給後面幾張票的形狀，有些目前的 conf 還用不到（例如 item 清單仍逐一列出）
（README〈資料涵蓋了什麼〉逐項列出）：

- 同一週、同一使用者、同一版位、同一素材被曝光不只一次（``event`` 角色，#378；
  資料仍撐得住，只是現在分散在不同的請求裡，這份 conf 選擇宣告 occasion 而非 event）。
- 有些使用者一週在同一版位的曝光次數超過 item 種數（同上，``"all"`` 不截斷）。
- 使用者特徵與版位特徵各有自己的粒度。框架不收比 base key 粗的特徵表，兩者在
  ``feature_table.sql`` 併成一張（ADR-0026）。
- **即時特徵**（以候選層級特徵表接進模型，#380）：曝光前 30 分鐘內瀏覽過活動同類內容，點擊意願較高。同一組同一素材的
  多次曝光，這個值各不相同——聚合成一週一列就丟掉了（ADR-0021〈考慮過、沒選的做法〉）。
- **偷看的陷阱**：點擊之後幾分鐘內會去瀏覽同類內容，其中一半與點擊記在同一秒。
  算即時特徵時多算到曝光之後（連 ``<`` 寫成 ``<=`` 也算），會得到一個離線很強、
  線上不存在的假訊號。
- **快照日 ≠ time**：使用者快照每天一份，記當天結束時的狀態，隔天清晨才算好
  （``available_at``），批次偶爾晚一天。有人會在一天中的某個時刻換裝置，點擊看的是
  曝光那一刻的裝置。取「排序當下拿得到的最後一份」，與取「同一天那份」、取「前兩天那份」
  結果都不同（ADR-0022）。
- **沒見過的屬性組合**：``c04``、``video`` 從第一週就有，``c04-video`` 從 val 週才出現。

數字不是生產的樣子：點擊率刻意調高到十幾 %，因為母體只有幾百人，照真實的 1%
一週只會有個位數正例。生產規模的估算不在本示例的範圍（見 README）。

用法（在 examples/ad/ 底下）：
    python generate_data.py            # 寫 data/raw/*.parquet
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
from bisect import bisect_left, insort
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
WEEK_SECONDS = 7 * 24 * 3600
# 所有時間欄都是這個時區的當地時間（與 conf/spark-local 的 session 時區相同）
LOCAL_TZ = "Asia/Taipei"

# item ＝ campaign_id + ITEM_SEPARATOR + creative_format。SQL 不拼：conf 宣告
# item: [campaign_id, creative_format]，框架讀入時拼（ADR-0027），分隔字元是框架固定的
# recsys_tfb.utils.item_columns.ITEM_SEPARATOR；這裡的常數只給產生器與 conf 的組合清單對照用，
# 兩者相等由 tests/examples/test_ad_example.py 守。
# 用 "-" 而不是 "|"：item 值會成為 Hive 分區目錄名、MLflow 指標名與檔名的一部分，
# 那幾處對特殊字元的處理還沒驗過（規劃檔 P7a、P8 的範圍）。
ITEM_SEPARATOR = "-"
CAMPAIGNS = ["c01", "c02", "c03", "c04"]
FORMATS = ["banner", "video", "native"]

# 曝光分配權重：刻意長尾，讓冷門 item 的指標有東西可看
CAMPAIGN_WEIGHT = {"c01": 0.40, "c02": 0.30, "c03": 0.20, "c04": 0.10}
FORMAT_WEIGHT = {"banner": 0.55, "video": 0.25, "native": 0.20}

# 這個組合在 LATE_ITEM_FIRST_WEEK 之前一次都不曝光；它的活動與格式在別的組合裡從第一週就有。
# 放在 val 週：train 沒看過它，val 與 test 都有。
LATE_ITEM = ("c04", "video")
LATE_ITEM_FIRST_WEEK = "2025-12-22"
# 新素材上線後先給較多流量。照原本的權重（2.5%），test 週只有 5 個正例，看不出模型怎麼排它
LATE_ITEM_LAUNCH_BOOST = 4.0

SLOTS = pd.DataFrame({
    "slot_id": ["home_top", "feed_mid", "article_end"],
    "slot_position": ["top", "middle", "bottom"],
    "page_type": ["home", "feed", "article"],
})
SLOT_LOGIT = {"home_top": 0.4, "feed_mid": 0.0, "article_end": -0.5}
# 每週每版位的請求次數（Poisson 平均）。每次請求展示 ADS_PER_REQUEST_P 抽出的 k 個
# 素材，平均約 4 個，所以總曝光量級與改版前（SLOT_IMPRESSION_RATE 5／7／3）相近；
# feed_mid 請求數 × 平均素材數仍夠高，讓一些 (使用者, 版位) 一週的曝光數超過 item
# 種數 12——event 角色（#378）要驗的「"all" 不截斷」只在這種組上發生。
REQUEST_RATE = {"home_top": 1.25, "feed_mid": 1.75, "article_end": 0.75}
# 一次請求展示幾個素材：k 的分布。平均約 4 個；1 的機率不是 0——那種請求組內只有一個
# 候選，排序沒有東西可比，mAP 恆為 1（README〈資料涵蓋了什麼〉要講的就是這件事）。
ADS_PER_REQUEST_P = {1: 0.05, 2: 0.10, 3: 0.20, 4: 0.30, 5: 0.20, 6: 0.15}

AGE_BANDS = ["18-24", "25-34", "35-49", "50+"]
DEVICES = ["mobile", "desktop", "tablet"]
REGIONS = ["north", "central", "south", "east"]

# 年齡層偏好哪個活動、裝置偏好哪種格式：讓特徵真的帶訊號，模型才有東西可學。
# 強度的選法：一組只有兩三個候選時，隨便排的 mAP 也有 0.7，排序指標分不出好壞；
# 所以候選數拉到平均 4 個左右、親和力加強，讓「隨便排／照熱門排／照特徵排」拉得開
# （不起 Spark 的估算見 README〈資料長什麼樣〉）。
AGE_CAMPAIGN_AFFINITY = {"18-24": "c03", "25-34": "c01", "35-49": "c02", "50+": "c04"}
DEVICE_FORMAT_AFFINITY = {"mobile": "video", "desktop": "banner", "tablet": "native"}
BASE_LOGIT = -3.6  # 加了即時瀏覽訊號後從 -3.2 調低，點擊率回到十幾 %
AGE_AFFINITY_LOGIT = 1.8
DEVICE_AFFINITY_LOGIT = 1.4
PROPENSITY_SD = 0.6
FATIGUE_LOGIT = 0.5  # 同一週同一版位同一素材每多看一次，點擊意願下降多少

# 裝置：每週有這個比例的使用者在那一週的某個時刻（到秒）換主要裝置。點擊看的是曝光
# 那一刻的裝置，快照 D 記的是 D 當天結束時的狀態——D 下午才換的人，D 早上的曝光還是
# 舊裝置，拿快照 D 去排 D 的曝光就是用到未來的狀態。
DEVICE_SWITCH_RATE = 0.05
# 快照 D 在 D+1 的 05:00 起、再晚 0～180 分鐘才算好（每天不同）。批次偶爾晚一天，
# 週末沒人顧比較常晚：週六那份晚一天時，週一 00:00 只拿得到週五那份，
# 「取前兩天那份」這種不看 available_at 的寫法就取錯。
PROFILE_READY_HOUR = 5
PROFILE_READY_JITTER_MINUTES = 180
PROFILE_LATE_RATE_WEEKDAY = 0.1
PROFILE_LATE_RATE_WEEKEND = 0.5
# 快照涵蓋第一週之前一週（讓第一週也有「前一天」的快照）到最後一週的週日
PROFILE_FIRST_DAY = dt.date.fromisoformat(WEEKS[0]) - dt.timedelta(days=7)
PROFILE_LAST_DAY = dt.date.fromisoformat(WEEKS[-1]) + dt.timedelta(days=6)

# 瀏覽紀錄：每個活動屬於一類內容。曝光前 RECENT_WINDOW_SECONDS 內瀏覽過同類內容，
# 點擊意願加 RECENT_INTEREST_LOGIT。
CAMPAIGN_CATEGORY = {"c01": "finance", "c02": "travel", "c03": "gaming", "c04": "health"}
CATEGORIES = sorted(CAMPAIGN_CATEGORY.values())
RECENT_WINDOW_SECONDS = 30 * 60
RECENT_INTEREST_LOGIT = 1.5
BROWSE_BEFORE_REQUEST = 1.0   # 每次請求前 30 分鐘內的瀏覽次數（Poisson 平均），類別隨機——
# 一次頁面瀏覽對應一次請求，不是對應每一個素材
BACKGROUND_BROWSE_PER_WEEK = 10  # 有上站的那一週，與曝光無關、散在整週的瀏覽次數
POST_CLICK_BROWSE_SECONDS = 300  # 點擊後這麼多秒內，瀏覽一次該活動那一類內容（偷看的陷阱）
POST_CLICK_SAME_SECOND_RATE = 0.5  # 其中這個比例與點擊記在同一秒（落地頁與點擊同時記錄）


def _users(rng: np.random.Generator, n_users: int) -> pd.DataFrame:
    return pd.DataFrame({
        "user_id": [f"u{i:05d}" for i in range(1, n_users + 1)],
        "age_band": rng.choice(AGE_BANDS, size=n_users, p=[0.2, 0.35, 0.3, 0.15]),
        # 第一份快照時的裝置；之後的變化在 _device_switches
        "device_type": rng.choice(DEVICES, size=n_users, p=[0.6, 0.3, 0.1]),
        "region": rng.choice(REGIONS, size=n_users),
        # 註冊日散在示例期間之前兩年內，SQL 由它算年資與分層
        "signup_date": [
            dt.date(2025, 10, 27) - dt.timedelta(days=int(d))
            for d in rng.integers(1, 730, size=n_users)
        ],
        # 看不到的個人點擊傾向；特徵只能從過去的點擊率間接學到
        "propensity": rng.normal(0.0, PROPENSITY_SD, size=n_users),
        "activity": rng.uniform(0.3, 1.0, size=n_users),
    })


def _device_switches(rng: np.random.Generator, users: pd.DataFrame) -> dict[str, list[tuple[dt.datetime, str]]]:
    """換裝置的紀錄：使用者 → [(生效時刻, 新裝置)]，依時間排好。沒換過的人不在裡面。"""
    switches: dict[str, list[tuple[dt.datetime, str]]] = {}
    for user in users.itertuples(index=False):
        current = user.device_type
        for week in WEEKS:
            if rng.random() >= DEVICE_SWITCH_RATE:
                continue
            at = dt.datetime.fromisoformat(week) + dt.timedelta(seconds=int(rng.integers(0, WEEK_SECONDS)))
            current = str(rng.choice([d for d in DEVICES if d != current]))
            switches.setdefault(user.user_id, []).append((at, current))
    return switches


def _device_at(initial: str, switches: list[tuple[dt.datetime, str]], moment: dt.datetime) -> str:
    """moment 那一刻（含）已經生效的裝置。曝光的點擊與每日快照都用這一個定義。"""
    device = initial
    for effective, new in switches:
        if effective > moment:
            break
        device = new
    return device


def _user_profile(
    rng: np.random.Generator, users: pd.DataFrame, switches: dict[str, list[tuple[dt.datetime, str]]],
) -> pd.DataFrame:
    """每天一份快照。available_at 是這份快照算好、線上拿得到的時刻，一定在 snap_date 隔天之後。"""
    days = [PROFILE_FIRST_DAY + dt.timedelta(days=i) for i in range((PROFILE_LAST_DAY - PROFILE_FIRST_DAY).days + 1)]
    late_rate = np.array([PROFILE_LATE_RATE_WEEKEND if d.weekday() >= 5 else PROFILE_LATE_RATE_WEEKDAY for d in days])
    late = rng.random(len(days)) < late_rate
    jitter = rng.integers(0, PROFILE_READY_JITTER_MINUTES + 1, size=len(days))
    rows = []
    for day, is_late, minutes in zip(days, late, jitter):
        ready = (
            dt.datetime.combine(day, dt.time(PROFILE_READY_HOUR))
            + dt.timedelta(days=1 + int(is_late), minutes=int(minutes))
        )
        end_of_day = dt.datetime.combine(day, dt.time(23, 59, 59))
        for user in users.itertuples(index=False):
            device = _device_at(user.device_type, switches.get(user.user_id, []), end_of_day)
            rows.append((day, ready, user.user_id, user.age_band, device, user.region, user.signup_date))
    return pd.DataFrame(rows, columns=[
        "snap_date", "available_at", "user_id", "age_band", "device_type", "region", "signup_date",
    ])


def _impressions(
    rng: np.random.Generator, users: pd.DataFrame, switches: dict[str, list[tuple[dt.datetime, str]]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """曝光紀錄與瀏覽紀錄一起產：點擊要看曝光前的瀏覽，點擊之後又會產生瀏覽。

    時間一律用「距 PROFILE_FIRST_DAY 00:00 幾秒」的整數算，最後才轉成 datetime。每個人分兩階段：
    先定下所有週的請求時刻、各自展示的素材，以及跟點擊無關的瀏覽；再依時間順序逐次決定點擊，
    點了就補一筆點擊後的瀏覽。點擊後的瀏覽不早於這次曝光那一秒，而每次曝光只看它那一秒
    之前的瀏覽，所以依時間順序處理時，輪到的每一次曝光都已經看得到它該看的全部瀏覽。

    一次請求（同一秒）展示好幾個素材：``shown`` 每一列多帶一個 ``local_req``——同一使用者
    內部、依產生順序遞增的請求序號，同一次請求的所有素材共用同一個值。``shown.sort`` 依
    時間排序是 stable，同一次請求的列本來就同一個 sec，排序後仍相鄰，local_req 不會被拆散。
    真正寫進資料的 ``request_id`` 在下面第二個迴圈裡、依最終產生順序（使用者為外層、
    每人內部依時間）指派，這樣才跟 ``impression_id`` 的編號順序一致。
    """
    origin = dt.datetime.combine(PROFILE_FIRST_DAY, dt.time())
    items = [(c, f) for c in CAMPAIGNS for f in FORMATS]
    ad_counts = np.array(list(ADS_PER_REQUEST_P.keys()))
    ad_count_p = np.array(list(ADS_PER_REQUEST_P.values()))
    p_base = np.array([CAMPAIGN_WEIGHT[c] * FORMAT_WEIGHT[f] for c, f in items])
    p_early, p_late = p_base.copy(), p_base.copy()
    p_early[items.index(LATE_ITEM)] = 0.0
    p_late[items.index(LATE_ITEM)] *= LATE_ITEM_LAUNCH_BOOST
    p_early, p_late = p_early / p_early.sum(), p_late / p_late.sum()
    imp_rows, browse_rows = [], []
    request_counter = 0
    for user in users.itertuples(index=False):
        # (秒, 那一週的起點秒, slot_id, item 索引, 這個使用者內的請求序號)
        shown: list[tuple[int, int, str, int, int]] = []
        browse: dict[str, list[int]] = {c: [] for c in CATEGORIES}
        local_req = 0
        for week in WEEKS:
            if rng.random() > user.activity:
                continue
            week_start = int((dt.datetime.fromisoformat(week) - origin).total_seconds())
            p = p_late if week >= LATE_ITEM_FIRST_WEEK else p_early
            for slot_id, rate in REQUEST_RATE.items():
                n_requests = rng.poisson(rate)
                if n_requests == 0:
                    continue
                request_seconds = rng.integers(0, WEEK_SECONDS, size=n_requests)
                request_sizes = rng.choice(ad_counts, size=n_requests, p=ad_count_p)
                for s, k in zip(request_seconds, request_sizes):
                    local_req += 1
                    # 不放回：同一次請求裡的素材互不重複
                    picked = rng.choice(len(items), size=int(k), replace=False, p=p)
                    shown.extend(
                        (week_start + int(s), week_start, slot_id, int(i), local_req)
                        for i in picked
                    )
            k = rng.poisson(BACKGROUND_BROWSE_PER_WEEK)
            for s, c in zip(rng.integers(0, WEEK_SECONDS, size=k), rng.integers(0, len(CATEGORIES), size=k)):
                browse[CATEGORIES[c]].append(week_start + int(s))
        shown.sort(key=lambda r: r[0])  # stable：同一秒保留產生順序，與 SQL 的 ORDER BY event_ts, impression_id 一致
        last_req = None
        for sec, *_, req in shown:
            if req == last_req:
                continue  # 一次頁面瀏覽對應一次請求，不是對應請求裡的每一個素材
            last_req = req
            k = rng.poisson(BROWSE_BEFORE_REQUEST)
            backs = rng.integers(1, RECENT_WINDOW_SECONDS, size=k, endpoint=True)
            for back, c in zip(backs, rng.integers(0, len(CATEGORIES), size=k)):
                browse[CATEGORIES[c]].append(sec - int(back))
        for times in browse.values():
            times.sort()

        user_switches = switches.get(user.user_id, [])
        seen: dict[tuple[int, str, int], int] = {}
        last_req = None
        request_id = None
        for sec, week_start, slot_id, idx, req in shown:
            if req != last_req:
                request_counter += 1
                request_id = f"req{request_counter:08d}"
                last_req = req
            campaign, fmt = items[idx]
            ts = origin + dt.timedelta(seconds=sec)
            exposure = seen.get((week_start, slot_id, idx), 0)
            seen[(week_start, slot_id, idx)] = exposure + 1
            same_category = browse[CAMPAIGN_CATEGORY[campaign]]
            # [曝光前 30 分鐘, 曝光那一秒)：含下界、不含曝光本身那一秒
            recent = bisect_left(same_category, sec) - bisect_left(same_category, sec - RECENT_WINDOW_SECONDS)
            device = _device_at(user.device_type, user_switches, ts)
            logit = (
                BASE_LOGIT
                + user.propensity
                + SLOT_LOGIT[slot_id]
                + (AGE_AFFINITY_LOGIT if AGE_CAMPAIGN_AFFINITY[user.age_band] == campaign else 0.0)
                + (DEVICE_AFFINITY_LOGIT if DEVICE_FORMAT_AFFINITY[device] == fmt else 0.0)
                + (RECENT_INTEREST_LOGIT if recent > 0 else 0.0)
                - FATIGUE_LOGIT * exposure
            )
            clicked = int(rng.random() < 1.0 / (1.0 + math.exp(-logit)))
            imp_rows.append((ts, ts.date(), user.user_id, slot_id, request_id, campaign, fmt, clicked))
            if clicked:
                # 同一秒的那一筆，正是 SQL 的上界寫成 <= 時會多算進去的
                same_second = rng.random() < POST_CLICK_SAME_SECOND_RATE
                delay = 0 if same_second else int(rng.integers(1, POST_CLICK_BROWSE_SECONDS, endpoint=True))
                insort(same_category, sec + delay)
        for category, times in browse.items():
            browse_rows.extend((origin + dt.timedelta(seconds=s), user.user_id, category) for s in times)

    log = pd.DataFrame(imp_rows, columns=[
        "event_ts", "event_date", "user_id", "slot_id", "request_id",
        "campaign_id", "creative_format", "clicked",
    ])
    log.insert(0, "impression_id", [f"imp{i:08d}" for i in range(1, len(log) + 1)])
    browse_log = (
        pd.DataFrame(browse_rows, columns=["event_ts", "user_id", "content_category"])
        .sort_values(["user_id", "event_ts", "content_category"], kind="mergesort", ignore_index=True)
    )
    browse_log.insert(1, "event_date", browse_log["event_ts"].dt.date)
    return log, browse_log


def generate(n_users: int = N_USERS, seed: int = SEED) -> dict[str, pd.DataFrame]:
    """回傳原始表。同一組參數永遠產出同一份資料（基準 digest 靠這一點）。

    曝光紀錄沒有「週」欄：屬於哪一週由 SQL 以 ``event_date`` 落在
    ``[target_date, target_date + 7)`` 決定，這正是來源 SQL 該負責的事。
    所有時間欄是 LOCAL_TZ 的當地時間、不帶時區；寫檔時才標上時區（write_parquet）。
    """
    rng = np.random.default_rng(seed)
    users = _users(rng, n_users)
    switches = _device_switches(rng, users)
    impression_log, browse_log = _impressions(rng, users, switches)
    return {
        "impression_log": impression_log,
        "browse_log": browse_log,
        "user_profile": _user_profile(rng, users, switches),
        "campaign_dim": pd.DataFrame({
            "campaign_id": CAMPAIGNS,
            "content_category": [CAMPAIGN_CATEGORY[c] for c in CAMPAIGNS],
        }),
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
        # 不帶時區的 timestamp，Spark 3.3 會當成 UTC 讀，在 Asia/Taipei 的 session 裡
        # 變成晚 8 小時（2026-09-18 實測：產生器的 16:45 讀出來是隔天 00:45，與 event_date
        # 對不上）。先標上當地時區，Spark 讀到的才是同一個時刻。
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(LOCAL_TZ)
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
    per_request = log.groupby("request_id").size()
    print(f"impression_log: {len(log)} 列，點擊率 {log['clicked'].mean():.3f}")
    print(f"  (週, 使用者, 版位, item) 組合 {log.assign(item=item).groupby(group + ['item']).ngroups} 個")
    print(f"  base key（週, 使用者, 版位）{log.groupby(group).ngroups} 個")
    print(f"  item {item.nunique()} 種：{sorted(item.unique())}")
    print(f"  request（query group，occasion）{per_request.size} 個")
    print(f"  每次請求的素材數分布：{dict(sorted(per_request.value_counts().to_dict().items()))}")
    print(f"  只有 1 個素材的請求佔 {(per_request == 1).mean():.3f}")
    print(f"browse_log: {len(tables['browse_log'])} 列；user_profile: {len(tables['user_profile'])} 列")
    print(f"寫到 {args.out.resolve()}")


if __name__ == "__main__":
    main()
