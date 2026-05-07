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
    WHERE snap_date = '${target_date}'
)
, prod as (

    select 'ccard_bill' as prod_name
    union all
    select 'ccard_cash' as prod_name
    union all
    select 'ccard_ins' as prod_name
    union all
    select 'exchange_fx' as prod_name
    union all
    select 'exchange_usd' as prod_name
    union all
    select 'fund_bond' as prod_name
    union all
    select 'fund_mix' as prod_name
    union all
    select 'fund_stock' as prod_name

)

, cross_pop AS (

    select
        pop.snap_date,
        pop.cust_id,
        pop.cust_segment_typ,
        pop.prod_name
    from cust_snap pop
    left join prod on 1=1

)

SELECT
    p.snap_date,
    p.cust_id,
    p.cust_segment_typ,
    p.prod_name,
    COALESCE(l.label, 0) AS label, 
    f.tenure_months,
    f.channel_preference
FROM cross_pop p
    LEFT JOIN ${target_db}.label_table l ON p.snap_date = l.snap_date AND p.cust_id = l.cust_id and p.prod_name = l.prod_name
    LEFT JOIN ${target_db}.feature_table f ON p.snap_date = f.snap_date AND p.cust_id = f.cust_id