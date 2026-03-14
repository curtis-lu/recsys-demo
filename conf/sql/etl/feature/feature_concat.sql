--partition by: snap_date

with feature_aum as (

    select * from feature_aum where snap_date = '${snap_date}'
),
feature_sav as (

    select * from feature_sav where snap_date = '${snap_date}'
),
pool_cust as (
    select distinct cust_id 
    from (
        select cust_id from feature_aum
        union all
        select cust_id from feature_sav
    ) t
)
feature_concat as (

    select 
        pool_cust.cust_id,
        feature_aum.total_aum,
        feature_aum.fund_aum,
        feature_sav.in_amt_sum_l1m,
        feature_sav.out_amt_sum_l1m,
        case 
            when feature_aum.total_aum > 0 then feature_sav.in_amt_sum_l1m / feature_aum.total_aum
            when feature_aum.total_aum = 0 then 0
            else null
        end as in_amt_ratio_l1m,
        case 
            when feature_aum.total_aum > 0 then feature_sav.out_amt_sum_l1m / feature_aum.total_aum
            when feature_aum.total_aum = 0 then 0
            else null
        end as out_amt_ratio_l1m
    from pool_cust
    left join feature_aum
    on pool_cust.cust_id = feature_aum.cust_id
    left join feature_sav
    on pool_cust.cust_id = feature_sav.cust_id
)
select * from feature_concat