--partition by: snap_date

WITH txn_1m AS (
    SELECT
        to_date(snap_date) AS snap_date,
        cust_id,
        SUM(CASE WHEN dr_cr_flag = 'D' THEN COALESCE(txn_amt, 0) ELSE 0 END) AS out_amt_sum_l1m,
        SUM(CASE WHEN dr_cr_flag = 'C' THEN COALESCE(txn_amt, 0) ELSE 0 END) AS in_amt_sum_l1m
    FROM feature_store.fact_sav_txn
    WHERE snap_date >= add_months('${snap_date}', -1)
     AND snap_date <  '${snap_date}'
    GROUP BY
        snap_date,
        cust_id
)

SELECT
    snap_date,
    cust_id,
    out_amt_sum_l1m,
    in_amt_sum_l1m
FROM txn_1m