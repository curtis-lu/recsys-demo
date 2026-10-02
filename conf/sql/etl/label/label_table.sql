--partition by: prod_name, snap_date

SELECT *
FROM ${target_db}.recsys_prod_label_ccard
WHERE snap_date = '${target_date}'

UNION ALL

SELECT *
FROM ${target_db}.recsys_prod_label_exchange
WHERE snap_date = '${target_date}'

UNION ALL

SELECT *
FROM ${target_db}.recsys_prod_label_fund
WHERE snap_date = '${target_date}'
