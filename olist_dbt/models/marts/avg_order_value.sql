-- 4.9 Средний чек
WITH order_totals AS (
    SELECT
        order_id,
        SUM(price) AS total_order_amount
    FROM staging.order_items
    GROUP BY order_id
)
SELECT
    AVG(total_order_amount) AS avg_check
FROM order_totals