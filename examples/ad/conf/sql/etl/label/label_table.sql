--partition by: snap_date
-- 一次曝光一列：這一次曝光有沒有被點。
--
-- item 在這裡由兩個屬性拼成一欄（CONTEXT.md：item 恆為一欄）。分隔字元要與
-- generate_data.py 的 ITEM_SEPARATOR、conf 的 categorical_values 一致。
--
-- 這份 conf 宣告 occasion ＝ request_id（#428，形狀二），所以 identity 是
-- (snap_date, user_id, slot_id, request_id, ad_creative)：一次請求（到秒）展示的
-- 好幾個素材，互不重複，各自帶自己的 label。改這裡要連 parameters_label_etl.yaml
-- 的 primary_key 一起改：少列 request_id，max_duplicate_key_ratio: 0.0 會把「同一週
-- 同一個素材出現在不同請求裡」判成重複鍵而擋下整條 ETL。
--
-- 這是這個示例刻意示範的形狀，不是所有部署都該這樣：一組只是一次請求擺出來的
-- 1～6 個素材，組很小（docs/operations/user-guides/impression-data-shapes.md）。
-- 要回到形狀一（一次曝光一列、以一週為一組、宣告 event，#378 跑綠過），把這裡的
-- request_id 換回 impression_id，並照 parameters.yaml 的註解改其餘幾個檔案。
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
    i.request_id,
    CAST(i.clicked AS INT) AS label
FROM ${raw_db}.impression_log i
CROSS JOIN week w
WHERE i.event_date >= w.snap_date
  AND i.event_date <  date_add(w.snap_date, 7)
