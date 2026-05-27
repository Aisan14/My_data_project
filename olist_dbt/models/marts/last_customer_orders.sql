-- 4.10 Последний заказ каждого клиента
SELECT
    customer_id,
    MAX(order_purchase_timestamp) AS last_order_date
FROM staging.orders
GROUP BY customer_id