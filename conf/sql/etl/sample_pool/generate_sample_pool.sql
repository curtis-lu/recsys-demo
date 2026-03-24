-- generate_sample_pool.sql
-- 產出 sample_pool 表：(snap_date, cust_id, cust_segment_typ, prod_name)
-- customer-month-product 粒度：每個客戶每月 x 所有產品
--
-- 從 feature_table 取得 customer-month 及 cust_segment_typ，
-- 與 label_table 中的所有 distinct prod_name 做 cross join。

SELECT
    f.snap_date,
    f.cust_id,
    f.cust_segment_typ,
    p.prod_name
FROM feature_table f
CROSS JOIN (
    SELECT DISTINCT prod_name
    FROM label_table
) p
