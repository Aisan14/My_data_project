-- 4.14 Заказы без позиций
SELECT
    o.order_id
FROM staging.orders o
LEFT JOIN staging.order_items oi
ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL