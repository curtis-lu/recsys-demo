-- generate_sample_pool.sql
-- 產出 sample_pool 表：(snap_date, cust_id, cust_segment_typ, prod_name)
-- customer-month-product 粒度：每個客戶每月 x 所有產品
--
-- 從 feature_table 取得 customer-month 及 cust_segment_typ，
-- 與 label_table 中的所有 distinct prod_name 做 cross join。


WITH cust_snap AS (
    SELECT DISTINCT
        snap_date,
        cust_id,
        cust_segment_typ
    FROM feature_store.dim_all_customer
    WHERE snap_date = '${snap_date}'
)

SELECT
    p.snap_date,
    p.cust_id,
    p.cust_segment_typ,
    l.prod_name,
    l.label,
    f.tenure_months,
    f.channel_preference
FROM cust_snap p
    LEFT JOIN ${target_db}.label_table l ON p.snap_date = l.snap_date AND p.cust_id = l.cust_id
    LEFT JOIN ${target_db}.feature_table f ON p.snap_date = l.snap_date AND p.cust_id = l.cust_id