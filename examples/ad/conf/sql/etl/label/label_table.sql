--partition by: snap_date
-- 一週 × 使用者 × 版位 × 素材一列：這一週在這個版位有沒有點過這個素材。
--
-- item 在這裡由兩個屬性拼成一欄（CONTEXT.md：item 恆為一欄）。分隔字元要與
-- generate_data.py 的 ITEM_SEPARATOR、conf 的 categorical_values 一致。
--
-- 同一素材同一週被曝光多次時在這裡聚合成一列（MAX）。本票還沒有 event 角色，
-- identity 必須是 (snap_date, user_id, slot_id, ad_creative)，重複會被下面的
-- max_duplicate_key_ratio 與 dataset 的粒度閘擋下。
--
-- snap_date 從週曆表取（README〈踩到的框架問題〉規則 1）。
WITH week AS (
    SELECT snap_date FROM ${raw_db}.week_calendar WHERE snap_date = '${target_date}'
)
SELECT
    w.snap_date,
    i.user_id,
    i.slot_id,
    concat(i.campaign_id, '-', i.creative_format) AS ad_creative,
    CAST(MAX(i.clicked) AS INT) AS label
FROM ${raw_db}.impression_log i
CROSS JOIN week w
WHERE i.event_date >= w.snap_date
  AND i.event_date <  date_add(w.snap_date, 7)
GROUP BY
    w.snap_date,
    i.user_id,
    i.slot_id,
    concat(i.campaign_id, '-', i.creative_format)
