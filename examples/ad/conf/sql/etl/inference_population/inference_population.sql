--partition by: snap_date
-- 推論母體：這一週要替哪些 (使用者, 版位) 排序。示例取特徵表的全部組合。
SELECT DISTINCT
    snap_date,
    user_id,
    slot_id
FROM ${target_db}.feature_table
WHERE snap_date = DATE('${target_date}')
