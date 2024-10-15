DROP TABLE IF EXISTS phbi_com.phbr_users_buyers_and_sellers;
CREATE TABLE IF NOT EXISTS  phbi_com.phbr_users_buyers_and_sellers AS 
WITH user_nonfraud AS (
    SELECT DISTINCT 
        user_id AS userid 
        , status 
        , CASE WHEN status = 1 THEN 0 ELSE 1 END AS is_fraud 
    FROM mp_user.dim_user__ph_s0_live 
    WHERE grass_date = DATE_ADD('day',-1,current_date)
        AND status = 1 
) 
, all_orders AS (
    SELECT 
        buyer_id AS userid
        , order_id AS orderid 
        , create_timestamp AS create_time
    FROM mp_order.dwd_order_item_all_ent_df__ph_s0_live
    WHERE is_bi_excluded = 0
        AND grass_date = grass_date
    UNION ALL
    SELECT
        user_id AS userid
        , CAST(order_id AS BIGINT) AS orderid
        , create_time
    FROM digitalpurchase.shopee_digital_product_order_v2_ph_db__order_tab__reg_daily_s0_live
    WHERE (final_price != 0 or (payment_status = 'P2' and final_price = 0))
)

, first_order AS (
    SELECT DISTINCT
        userid 
        , MIN(create_time) AS create_time 
    FROM all_orders
    GROUP BY 1
)
, daily_raw AS (
    SELECT 
        grass_date
        , TO_UNIXTIME(create_time) AS create_time
        , shopid
        , userid
    FROM phbi_com.cost_rev_g2n AS mp 
    WHERE is_shopid_excluded = 0
    UNION ALL 
    SELECT
        grass_date
        , create_time
        , 0 as shopid
        , userid
    FROM phbi_com.dp_cost_g2n AS dp 
)
, a1 AS (
    SELECT 
        grass_date
        , COUNT(DISTINCT A.user_id) AS a1_user_count
    FROM traffic.shopee_traffic_dws_platform_active_user_1d__ph_s1_live AS A 
    INNER JOIN user_nonfraud AS B 
        ON A.user_id = B.userid 
    WHERE grass_date <= DATE('2020-12-27')  
    GROUP BY 1
    UNION ALL 
    SELECT 
        grass_date
        , COUNT(DISTINCT A.user_id) AS a1_user_count
    FROM traffic.shopee_traffic_dws_platform_active_user_1d__ph_s1_live AS A 
    INNER JOIN user_nonfraud AS B 
        ON A.user_id = B.userid     
    WHERE grass_date >= DATE('2020-12-28')
    GROUP BY 1  
)
---------
, user_mart AS (
    SELECT DISTINCT
        user_id AS userid
        , DATE(FROM_UNIXTIME(registration_timestamp)) AS registration_time 
    FROM mp_user.dim_user__ph_s0_live AS user_mart_dim_user
    -- INNER JOIN marketplace.shopee_account_v2_db__account_tab__ph_daily_s0_live AS shopee_account_v2_db__account_tab
    --  ON user_mart_dim_user.user_id = shopee_account_v2_db__account_tab.userid
    --  AND extinfo.register_platform <> 'BACKEND'
    WHERE grass_date = DATE_ADD('day',-1,current_date)
        AND is_backend_created = 0
)
, daily_new_user AS (
    SELECT 
        registration_time AS grass_date
        , COUNT(DISTINCT A.userid) AS daily_new_user
    FROM user_mart AS A 
    INNER JOIN user_nonfraud AS B 
        ON A.userid = B.userid 
    GROUP BY 1
)
, daily_buyer_seller AS (
    SELECT 
        daily_raw.grass_date
        , COUNT(DISTINCT CASE WHEN first_order.userid IS NOT null THEN daily_raw.userid END) AS daily_new_buyer     
        , COUNT(DISTINCT daily_raw.userid) AS daily_buyer
        , COUNT(DISTINCT daily_raw.shopid) AS daily_seller
    FROM daily_raw
    LEFT JOIN first_order
        ON daily_raw.userid = first_order.userid 
        AND daily_raw.create_time = first_order.create_time 
    INNER JOIN user_nonfraud AS B 
        ON daily_raw.userid = B.userid 
    WHERE year(grass_date) >= 2022
    GROUP BY 1
)


select 
a.grass_date as grass_date
,a.daily_new_buyer as daily_new_buyer
,a.daily_buyer as daily_buyer
,a.daily_seller as daily_seller
,b.daily_new_user as daily_new_user
,c.a1_user_count as a1_user_count

from daily_buyer_seller as a
left join daily_new_user as b 
on a.grass_date = b.grass_date
left join a1 as c
on a.grass_date = c.grass_date
