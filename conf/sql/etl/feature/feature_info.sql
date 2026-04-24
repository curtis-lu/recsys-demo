--partition by: snap_date

SELECT
    '${target_date}' AS snap_date,
    cust_id,
    age,
    gender,
    tenure_months,
    income_level,
    risk_attr,
    education_level,
    marital_status,
    channel_preference
FROM feature_store.dim_customer_info
WHERE snap_date = '${target_date}'
