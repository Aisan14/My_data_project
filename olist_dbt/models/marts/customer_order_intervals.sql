-- 4.11 Интервал между заказами клиента
SELECT
    customer_id,
    order_id,
    order_purchase_timestamp,

    LAG(order_purchase_timestamp)
    OVER ( PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS previous_order_date,

    order_purchase_timestamp
    -
    LAG(order_purchase_timestamp)
    OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS interval_between_orders

FROM staging.orders