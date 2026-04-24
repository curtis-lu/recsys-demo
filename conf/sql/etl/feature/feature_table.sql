--partition by: snap_date

SELECT *
FROM ${target_db}.feature_concat
WHERE snap_date = '${target_date}'
