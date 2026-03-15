
--partition by: prod_name, snap_date

WITH candidate_prod AS (
    SELECT 'stock' AS prod_name
    UNION ALL
    SELECT 'bond'  AS prod_name
    UNION ALL
    SELECT 'mix'   AS prod_name
),
label_event AS (
    SELECT
        to_date('${snap_date}') AS snap_date,
        date_add('${snap_date}', 1) AS apply_start_date,
        date_add('${snap_date}', 30) AS apply_end_date,
        cust_id,
        CASE
            WHEN prod_name IN ('stock') THEN 'stock'
            WHEN prod_name IN ('bond')  THEN 'bond'
            WHEN prod_name IN ('mix')   THEN 'mix'
            ELSE NULL
        END AS prod_name,
        1 AS label
    FROM feature_store.fact_fund_txn
    WHERE txn_date > '${snap_date}'
      AND txn_date <= date_add('${snap_date}', 30)
),
label_dedup AS (
    SELECT
        snap_date,
        apply_start_date,
        apply_end_date,
        cust_id,
        prod_name,
        MAX(label) AS label
    FROM label_event
    WHERE prod_name IS NOT NULL
    GROUP BY
        snap_date,
        apply_start_date,
        apply_end_date,
        cust_id,
        prod_name
),
cust_snap AS (
    SELECT DISTINCT
        snap_date,
        cust_id,
        cust_segment_typ
    FROM feature_store.dim_all_customer
    WHERE snap_date = '${snap_date}'
)

SELECT
    c.snap_date,
    c.cust_id,
    c.cust_segment_typ,
    l.apply_start_date,
    l.apply_end_date,
    COALESCE(l.label, 0) AS label,
    p.prod_name
FROM cust_snap c
CROSS JOIN candidate_prod p
LEFT JOIN label_dedup l
  ON c.snap_date = l.snap_date
 AND c.cust_id  = l.cust_id
 AND p.prod_name = l.prod_name