INSERT INTO cdm.dm_settlement_report
        (restaurant_id, 
        restaurant_name, 
        settlement_date, 
        orders_count, 
        orders_total_sum, 
        orders_bonus_payment_sum, 
        orders_bonus_granted_sum, 
        order_processing_fee, 
        restaurant_reward_sum)
(SELECT 
        dmo.restaurant_id,
        dr.restaurant_name,
        dt.ts::date AS settlement_date, 
        count(distinct dmo.id) orders_count,
        sum(total_sum) orders_total_sum,
        sum(bonus_payment) orders_bonus_payment_sum,
        sum(bonus_grant) orders_bonus_granted_sum,
        sum(total_sum)*0.25 order_processing_fee,
        sum(total_sum)-sum(total_sum)*0.25-sum(bonus_payment) restaurant_reward_sum
FROM dds.fct_product_sales fps 
        inner JOIN dds.dm_orders AS dmo ON dmo.id=fps.order_id
        JOIN dds.dm_restaurants dr ON dr.id=dmo.restaurant_id
        JOIN dds.dm_timestamps dt ON dt.id=dmo.timestamp_id
WHERE dmo.order_status='CLOSED' 
group by dmo.restaurant_id,dr.restaurant_name,dt.ts::date)
ON CONFLICT (restaurant_id,settlement_date)

do update set 
        orders_count=EXCLUDED.orders_count, 
        orders_total_sum=EXCLUDED.orders_total_sum, 
        orders_bonus_payment_sum=EXCLUDED.orders_bonus_payment_sum, 
        orders_bonus_granted_sum=EXCLUDED.orders_bonus_granted_sum, 
        order_processing_fee=EXCLUDED.order_processing_fee, 
        restaurant_reward_sum=EXCLUDED.restaurant_reward_sum