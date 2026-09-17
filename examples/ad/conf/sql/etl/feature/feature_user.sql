--partition by: snap_date
-- 一個使用者一週一列：這一週開始時拿得到的快照屬性 ＋ 過去 28 天的曝光與點擊。
--
-- 兩條不偷看的規則（「往回算」是來源 SQL 的責任，框架不檢查，ADR-0022 決定 2、3）：
--
-- 1. 快照取「週一 00:00 已經算好」的最後一份。快照 D 記的是 D 當天結束時的狀態，
--    隔天清晨才算好（available_at）。取 snap_date = 週一那份，等於拿週一整天結束後的
--    狀態去排週一一早的曝光；週中換了裝置的人，裝置就對不上。
-- 2. 行為只數 event_date < snap_date：這一週的曝光發生在排序之後。
--
-- check_features.py 在 run_e2e.sh 裡把這張表與照定義重算的值逐列比對。
--
-- snap_date 取週曆表的欄位，不寫 to_date('${target_date}') 常數（README〈踩到的框架問題〉
-- 規則 1）。本資料夾每支 SQL 的 snap_date 都來自某張表的欄位。
WITH week AS (
    SELECT snap_date FROM ${raw_db}.week_calendar WHERE snap_date = '${target_date}'
),
latest AS (
    SELECT p.user_id, MAX(p.snap_date) AS profile_date
    FROM ${raw_db}.user_profile p
    CROSS JOIN week w
    WHERE p.available_at <= CAST(w.snap_date AS TIMESTAMP)
    GROUP BY p.user_id
),
profile AS (
    SELECT p.user_id, p.age_band, p.device_type, p.region, p.signup_date
    FROM ${raw_db}.user_profile p
    JOIN latest l
      ON p.user_id   = l.user_id
     AND p.snap_date = l.profile_date
),
activity AS (
    SELECT
        user_id,
        COUNT(*)     AS user_imp_4w,
        SUM(clicked) AS user_click_4w
    FROM ${raw_db}.impression_log
    WHERE event_date >= date_sub('${target_date}', 28)
      AND event_date <  '${target_date}'
    GROUP BY user_id
)
SELECT
    w.snap_date,
    p.user_id,
    p.age_band,
    p.device_type,
    p.region,
    datediff(w.snap_date, p.signup_date) AS tenure_days,
    COALESCE(a.user_imp_4w, 0)   AS user_imp_4w,
    COALESCE(a.user_click_4w, 0) AS user_click_4w,
    CASE WHEN a.user_imp_4w > 0 THEN a.user_click_4w / a.user_imp_4w END AS user_ctr_4w
FROM profile p
CROSS JOIN week w
LEFT JOIN activity a
  ON p.user_id = a.user_id
