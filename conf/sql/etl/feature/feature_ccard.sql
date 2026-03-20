--partition by: snap_date

SELECT
    '${snap_date}' AS snap_date,
    t.cust_id,
    COUNT(t.txn_id) AS ccard_txn_cnt_l1m,
    COALESCE(SUM(t.txn_amt), 0) AS ccard_txn_amt_l1m,
    MAX(CASE WHEN i.revolving_flag = 'Y' THEN 1 ELSE 0 END) AS ccard_revolving_flag,
    COALESCE(SUM(CASE WHEN t.overseas_flag = 'Y' THEN t.txn_amt ELSE 0 END), 0) AS ccard_overseas_amt_l1m,
    COALESCE(SUM(CASE WHEN t.installment_flag = 'Y' THEN t.txn_amt ELSE 0 END), 0) AS ccard_installment_amt_l1m,
    MAX(i.credit_limit) AS ccard_limit,
    CASE
        WHEN MAX(i.credit_limit) > 0
        THEN COALESCE(SUM(t.txn_amt), 0) / MAX(i.credit_limit)
        ELSE 0
    END AS ccard_util_ratio,
    COUNT(DISTINCT i.card_id) AS ccard_active_cnt
FROM feature_store.fact_ccard_txn t
LEFT JOIN feature_store.dim_ccard_info i
  ON t.cust_id = i.cust_id
 AND t.card_id = i.card_id
 AND i.snap_date = '${snap_date}'
WHERE t.txn_date > date_add('${snap_date}', -30)
  AND t.txn_date <= '${snap_date}'
GROUP BY t.cust_id
