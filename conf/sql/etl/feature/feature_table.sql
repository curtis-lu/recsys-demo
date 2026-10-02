--partition by: snap_date

SELECT *
FROM ${target_db}.recsys_prod_feature_concat
WHERE snap_date = '${target_date}'
