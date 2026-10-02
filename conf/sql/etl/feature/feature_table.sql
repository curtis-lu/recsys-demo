--partition by: snap_date

SELECT *
FROM ${target_db}.${table_prefix}feature_concat
WHERE snap_date = '${target_date}'
