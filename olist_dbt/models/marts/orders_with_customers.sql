-- 4.3 Заказы + клиент
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    c.customer_id,
    c.customer_city,
    c.customer_state

FROM staging.orders o
JOIN staging.customers c
ON o.customer_id = c.customer_id  