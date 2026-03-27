--partition by: snap_date

with feature_aum as (

    select * from ${target_db}.feature_aum where snap_date = '${snap_date}'
),
feature_sav as (

    select * from ${target_db}.feature_sav where snap_date = '${snap_date}'
),
feature_ccard as (

    select * from ${target_db}.feature_ccard where snap_date = '${snap_date}'
),
feature_info as (

    select * from ${target_db}.feature_info where snap_date = '${snap_date}'
),
pool_cust as (
    select distinct cust_id
    from (
        select cust_id from feature_aum
        union all
        select cust_id from feature_sav
        union all
        select cust_id from feature_ccard
        union all
        select cust_id from feature_info
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
        end as out_amt_ratio_l1m,
        feature_ccard.ccard_txn_cnt_l1m,
        feature_ccard.ccard_txn_amt_l1m,
        feature_ccard.ccard_revolving_flag,
        feature_ccard.ccard_overseas_amt_l1m,
        feature_ccard.ccard_installment_amt_l1m,
        feature_ccard.ccard_limit,
        feature_ccard.ccard_util_ratio,
        feature_ccard.ccard_active_cnt,
        feature_info.age,
        feature_info.gender,
        feature_info.tenure_months,
        feature_info.income_level,
        feature_info.risk_attr,
        feature_info.education_level,
        feature_info.marital_status,
        feature_info.channel_preference
    from pool_cust
    left join feature_aum
    on pool_cust.cust_id = feature_aum.cust_id
    left join feature_sav
    on pool_cust.cust_id = feature_sav.cust_id
    left join feature_ccard
    on pool_cust.cust_id = feature_ccard.cust_id
    left join feature_info
    on pool_cust.cust_id = feature_info.cust_id
)
select * from feature_concat
