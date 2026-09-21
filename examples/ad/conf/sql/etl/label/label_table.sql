--partition by: snap_date
-- 一次曝光一列：這一次曝光有沒有被點。
--
-- item 在這裡由兩個屬性拼成一欄（CONTEXT.md：item 恆為一欄）。分隔字元要與
-- generate_data.py 的 ITEM_SEPARATOR、conf 的 categorical_values 一致。
--
-- 這份 conf 宣告 occasion ＝ request_id（#428，形狀二），所以 identity 是
-- (snap_date, user_id, slot_id, request_id, ad_creative)：一次請求（到秒）展示的
-- 好幾個素材，互不重複，各自帶自己的 label。改這裡要連 parameters_label_etl.yaml
-- 的 primary_key 一起改：少列一欄，max_duplicate_key_ratio: 0.0 會把同一次請求的
-- 多個素材判成重複鍵而擋下整條 ETL。
--
-- 這是這個示例刻意示範的形狀，不是所有部署都該這樣：沒有逐筆的即時特徵時
-- （feature_realtime 要 #380 才接得進來），同一次請求裡的素材分數只靠週級特徵，
-- 只會製造同分。要回到「一週一列、宣告 event」（#378 已跑綠過的形狀一），把
-- 這裡的 request_id 換回 impression_id，並把 conf 的 occasion 宣告換回 event。
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
