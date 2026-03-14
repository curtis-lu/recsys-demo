--partition by: snap_date

WITH base AS (
    SELECT
        snap_date,
        cust_id,
        COALESCE(total_aum_amt, 0) AS total_aum,
        COALESCE(fund_aum_amt, 0)  AS fund_aum
    FROM feature_store.feat_aum
    WHERE snap_date = '${snap_date}'
)

SELECT
    snap_date,
    cust_id,
    total_aum,
    fund_aum
FROM base