-- 4.13 Платеж есть, заказа нет
SELECT
    p.order_id,
    p.payment_value
FROM staging.order_payments p
LEFT JOIN staging.orders o
ON p.order_id = o.order_id
WHERE o.order_id IS NULL