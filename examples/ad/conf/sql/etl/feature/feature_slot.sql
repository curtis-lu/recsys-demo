--partition by: snap_date
-- 一個版位一週一列：版位屬性 ＋ 過去 28 天全站在這個版位的點擊率。
-- 與 feature_user.sql 同一條不偷看的規則：只數 event_date < snap_date。
-- 版位維度表沒有日期，snap_date 從週曆表取（README〈踩到的框架問題〉規則 1）。
WITH week AS (
    SELECT snap_date FROM ${raw_db}.week_calendar WHERE snap_date = '${target_date}'
),
activity AS (
    SELECT
        slot_id,
        COUNT(*)     AS slot_imp_4w,
        SUM(clicked) AS slot_click_4w
    FROM ${raw_db}.impression_log
    WHERE event_date >= date_sub('${target_date}', 28)
      AND event_date <  '${target_date}'
    GROUP BY slot_id
)
SELECT
    w.snap_date,
    s.slot_id,
    s.slot_position,
    s.page_type,
    COALESCE(a.slot_imp_4w, 0) AS slot_imp_4w,
    CASE WHEN a.slot_imp_4w > 0 THEN a.slot_click_4w / a.slot_imp_4w END AS slot_ctr_4w
FROM week w
CROSS JOIN ${raw_db}.slot_dim s
LEFT JOIN activity a
  ON s.slot_id = a.slot_id
