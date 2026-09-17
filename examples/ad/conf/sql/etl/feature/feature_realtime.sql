--partition by: snap_date
-- 一次曝光一列：曝光那一刻之前的即時特徵。
--
-- 今天的框架讀不到這張表：它的粒度是一次曝光，要宣告 event 角色（#378）、並以多張特徵表
-- 接進來（#380）才用得上，所以它不在 catalog、也不併進 feature_table。先算出來，是讓
-- 那兩張票有一張「確定沒偷看」的即時特徵表可以接。鍵是那時的 identity：
-- (snap_date, user_id, slot_id, ad_creative) ＋ event（impression_id）。
--
-- 不偷看的界線：瀏覽只算 [曝光前 30 分鐘, 曝光那一刻)。上界寫成 <= 或往後放寬都會出事——
-- 使用者點了廣告之後幾分鐘內會去瀏覽同類內容，多算到曝光之後，這個特徵就變成
-- 「有沒有點」的答案（generate_data.py 的 POST_CLICK_BROWSE_SECONDS）。
-- check_features.py 在 run_e2e.sh 裡把這張表與照定義重算的值逐列比對。
--
-- snap_date 取週曆表的欄位（README〈踩到的框架問題〉規則 1）。
WITH week AS (
    SELECT snap_date FROM ${raw_db}.week_calendar WHERE snap_date = '${target_date}'
),
imp AS (
    SELECT
        w.snap_date,
        i.impression_id,
        i.event_ts,
        i.user_id,
        i.slot_id,
        i.campaign_id,
        concat(i.campaign_id, '-', i.creative_format) AS ad_creative
    FROM ${raw_db}.impression_log i
    CROSS JOIN week w
    WHERE i.event_date >= w.snap_date
      AND i.event_date <  date_add(w.snap_date, 7)
),
recent_browse AS (
    SELECT
        i.impression_id,
        COUNT(b.event_ts) AS browse_30m,
        COUNT(CASE WHEN b.content_category = c.content_category THEN 1 END) AS browse_same_category_30m
    FROM imp i
    JOIN ${raw_db}.campaign_dim c
      ON i.campaign_id = c.campaign_id
    LEFT JOIN ${raw_db}.browse_log b
      ON b.user_id    = i.user_id
     AND b.event_ts  >= i.event_ts - INTERVAL 30 MINUTES
     AND b.event_ts  <  i.event_ts
     -- 只為了少掃：週一凌晨的曝光要看到前一天晚上的瀏覽
     AND b.event_date >= date_sub(i.snap_date, 1)
     AND b.event_date <  date_add(i.snap_date, 7)
    GROUP BY i.impression_id
)
SELECT
    i.snap_date,
    i.user_id,
    i.slot_id,
    i.ad_creative,
    i.impression_id,
    i.event_ts,
    r.browse_30m,
    r.browse_same_category_30m,
    -- 與產生器的疲乏效果同一個定義：同一週、同一版位、同一素材，在這次之前曝光過幾次
    COUNT(*) OVER (
        PARTITION BY i.snap_date, i.user_id, i.slot_id, i.ad_creative
        ORDER BY i.event_ts, i.impression_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prior_exposures_this_week
FROM imp i
JOIN recent_browse r
  ON i.impression_id = r.impression_id
