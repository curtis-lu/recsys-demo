-- inference 推論母體：每個 snap_date 要被評分的 (time, entity)。
-- grain = (snap_date, cust_id)，一 entity 一列。
-- 示例最小版＝對 feature 來源取 distinct；正式環境改為「在世/未流失/符合資格」客戶。
SELECT DISTINCT
    snap_date,
    cust_id
FROM ${target_db}.feature_concat
WHERE snap_date = DATE('${target_date}')
