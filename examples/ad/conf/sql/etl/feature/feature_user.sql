--partition by: snap_date
-- 一個使用者一週一列：週一的快照屬性 ＋ 過去 28 天的曝光與點擊。
--
-- 行為只數 event_date < snap_date：這一週的曝光發生在排序之後，算進來就是偷看
-- 答案。「往回算」是來源 SQL 的責任，框架不檢查（ADR-0022 決定 2、3）。
--
-- snap_date 取快照表的欄位，不寫 to_date('${target_date}') 常數（README〈踩到的框架問題〉
-- 規則 1）。本資料夾每支 SQL 的 snap_date 都來自某張表的欄位。
WITH profile AS (
    SELECT snap_date, user_id, age_band, device_type, region, signup_date
    FROM ${raw_db}.user_profile
    WHERE snap_date = '${target_date}'
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
    p.snap_date,
    p.user_id,
    p.age_band,
    p.device_type,
    p.region,
    datediff(p.snap_date, p.signup_date) AS tenure_days,
    COALESCE(a.user_imp_4w, 0)   AS user_imp_4w,
    COALESCE(a.user_click_4w, 0) AS user_click_4w,
    CASE WHEN a.user_imp_4w > 0 THEN a.user_click_4w / a.user_imp_4w END AS user_ctr_4w
FROM profile p
LEFT JOIN activity a
  ON p.user_id = a.user_id
