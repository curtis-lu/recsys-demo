--partition by: prod_name, snap_date

SELECT *
FROM ${target_db}.label_ccard
WHERE snap_date = '${target_date}'

UNION ALL

SELECT *
FROM ${target_db}.label_exchange
WHERE snap_date = '${target_date}'

UNION ALL

SELECT *
FROM ${target_db}.label_fund
WHERE snap_date = '${target_date}'
