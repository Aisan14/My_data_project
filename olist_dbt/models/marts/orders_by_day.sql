-- 4.6 Количество заказов по дням
SELECT
    DATE(order_purchase_timestamp) AS order_date,
    COUNT(*) AS total_orders
FROM staging.orders
GROUP BY order_date
ORDER BY order_date