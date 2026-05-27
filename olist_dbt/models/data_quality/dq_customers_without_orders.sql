-- 4.4 Клиенты без заказов
SELECT
    c.customer_id,
    c.customer_city,
    c.customer_state
FROM staging.customers c
LEFT JOIN staging.orders o
ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL