--partition by: snap_date
-- 框架今天只收一張特徵表、以 time ＋ entity（snap_date, user_id, slot_id）接到候選列上，
-- 所以把三個粒度在這裡併成一張。
--
-- 計數欄外面再包一次 COALESCE 不是多餘的：上游那幾欄是 COALESCE／COUNT 算出來的，
-- 直接讀的話第二個日期就寫不進去（README〈踩到的框架問題〉規則 2）。
SELECT
    us.snap_date,
    us.user_id,
    us.slot_id,
    u.age_band,
    u.device_type,
    u.region,
    u.tenure_days,
    COALESCE(u.user_imp_4w, 0)   AS user_imp_4w,
    COALESCE(u.user_click_4w, 0) AS user_click_4w,
    u.user_ctr_4w,
    s.slot_position,
    s.page_type,
    COALESCE(s.slot_imp_4w, 0)   AS slot_imp_4w,
    s.slot_ctr_4w,
    COALESCE(us.us_imp_4w, 0)    AS us_imp_4w,
    COALESCE(us.us_click_4w, 0)  AS us_click_4w
FROM ${target_db}.feature_user_slot us
JOIN ${target_db}.feature_user u
  ON us.snap_date = u.snap_date
 AND us.user_id   = u.user_id
JOIN ${target_db}.feature_slot s
  ON us.snap_date = s.snap_date
 AND us.slot_id   = s.slot_id
WHERE us.snap_date = '${target_date}'
  AND u.snap_date  = '${target_date}'
  AND s.snap_date  = '${target_date}'
