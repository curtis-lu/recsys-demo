--partition by: snap_date
-- 候選＝這一週在這個版位真的曝光過的素材，不是全部 item。所以每個 query group 的
-- 候選數因人而異（銀行示例是每位客戶都配滿全部產品）。
--
-- user_segment 供分層抽樣（dataset.sample_group_keys）與 evaluation 分群；它只在
-- 這張表，不在 feature_table。
WITH segment AS (
    SELECT
        user_id,
        CASE
            WHEN datediff('${target_date}', signup_date) < 90  THEN 'new'
            WHEN datediff('${target_date}', signup_date) < 365 THEN 'regular'
            ELSE 'loyal'
        END AS user_segment
    FROM ${raw_db}.user_profile
    WHERE snap_date = '${target_date}'
)
SELECT
    l.snap_date,
    l.user_id,
    l.slot_id,
    l.ad_creative,
    s.user_segment,
    l.label
FROM ${target_db}.label_table l
LEFT JOIN segment s
  ON l.user_id = s.user_id
WHERE l.snap_date = '${target_date}'
