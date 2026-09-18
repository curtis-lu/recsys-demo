--partition by: snap_date
-- 使用者 × 版位一週一列，涵蓋全部組合（過去沒在這個版位看過廣告的補 0）。
-- 全組合是必要的：推論時要替每個使用者的每個版位排序，不只替曝光過的。
WITH users AS (
    SELECT snap_date, user_id FROM ${target_db}.feature_user WHERE snap_date = '${target_date}'
),
slots AS (
    SELECT slot_id FROM ${target_db}.feature_slot WHERE snap_date = '${target_date}'
),
activity AS (
    SELECT
        user_id,
        slot_id,
        COUNT(*)     AS us_imp_4w,
        SUM(clicked) AS us_click_4w
    FROM ${raw_db}.impression_log
    WHERE event_date >= date_sub('${target_date}', 28)
      AND event_date <  '${target_date}'
    GROUP BY user_id, slot_id
)
SELECT
    u.snap_date,
    u.user_id,
    s.slot_id,
    COALESCE(a.us_imp_4w, 0)   AS us_imp_4w,
    COALESCE(a.us_click_4w, 0) AS us_click_4w
FROM users u
CROSS JOIN slots s
LEFT JOIN activity a
  ON u.user_id = a.user_id
 AND s.slot_id = a.slot_id
