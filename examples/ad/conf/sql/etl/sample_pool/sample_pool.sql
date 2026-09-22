--partition by: snap_date
-- 候選＝這一次請求真的展示過的每個素材，不是全部 item，也不是「這一週曝光過的
-- 素材」——宣告了 occasion 之後一次請求就是一個 query group（#428，形狀二）。所以
-- 每個 query group 的候選列數因請求而異，而且可以只有 1 列（銀行示例是每位客戶
-- 都配滿全部產品）。
--
-- 沒被展示過的素材刻意不補進來當負例：沒展示就不知道會不會被點，補 0 等於教模型
-- 模仿舊系統挑素材的偏好。
--
-- user_segment 供分層抽樣（dataset.sample_group_keys）與 evaluation 分群；它只在
-- 這張表，不在 feature_table。年資取 feature_user 算好的 tenure_days，不直接讀
-- user_profile：取哪一天的快照才不偷看，只在 feature_user.sql 決定一次。
WITH segment AS (
    SELECT
        user_id,
        CASE
            WHEN tenure_days < 90  THEN 'new'
            WHEN tenure_days < 365 THEN 'regular'
            ELSE 'loyal'
        END AS user_segment
    FROM ${target_db}.feature_user
    WHERE snap_date = '${target_date}'
)
SELECT
    l.snap_date,
    l.user_id,
    l.slot_id,
    l.campaign_id,
    l.creative_format,
    -- schema.columns.occasion。宣告了角色，sample_pool 與 label_table 都必須帶齊
    -- 那些欄（B11 在 dataset 第一個節點擋下），因為 identity 是候選列的鍵，也是
    -- label 接上來的鍵。
    l.request_id,
    s.user_segment,
    l.label
FROM ${target_db}.label_table l
LEFT JOIN segment s
  ON l.user_id = s.user_id
WHERE l.snap_date = '${target_date}'
