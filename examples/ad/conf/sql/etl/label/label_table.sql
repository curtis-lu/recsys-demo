--partition by: snap_date
-- 一次曝光一列：這一次曝光有沒有被點。
--
-- item 在這裡由兩個屬性拼成一欄（CONTEXT.md：item 恆為一欄）。分隔字元要與
-- generate_data.py 的 ITEM_SEPARATOR、conf 的 categorical_values 一致。
--
-- 這份 conf 宣告 event ＝ impression_id（#378），所以 identity 是
-- (snap_date, user_id, slot_id, ad_creative, impression_id)，同一素材同一週的多次
-- 曝光各自是一列、各自帶自己的 label。改這裡要連 parameters_label_etl.yaml 的
-- primary_key 一起改：少列一欄，max_duplicate_key_ratio: 0.0 會把那些曝光判成
-- 重複鍵而擋下整條 ETL。
--
-- 這是這個示例刻意示範的形狀，不是所有部署都該這樣：沒有逐筆的即時特徵時
-- （feature_realtime 要 #380 才接得進來），同一個 item 的多列分數完全相同，
-- 只會製造同分。要回到「一週一列」就拿掉 conf 的 event 宣告，並把這裡改回
-- GROUP BY ＋ MAX(clicked)。
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
    i.impression_id,
    CAST(i.clicked AS INT) AS label
FROM ${raw_db}.impression_log i
CROSS JOIN week w
WHERE i.event_date >= w.snap_date
  AND i.event_date <  date_add(w.snap_date, 7)
