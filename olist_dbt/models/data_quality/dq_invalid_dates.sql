-- 4.2 Заказы со “странной” датой (контроль качества)
SELECT
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date
FROM staging.orders
WHERE order_delivered_customer_date < order_purchase_timestamp