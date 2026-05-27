-- 4.12 Накопительная выручка по дням
WITH daily_revenue AS (
    SELECT
        DATE(o.order_purchase_timestamp) AS order_date,
        SUM(oi.price) AS revenue
    FROM staging.orders o
    JOIN staging.order_items oi
        ON o.order_id = oi.order_id
    GROUP BY 1
)
SELECT
    order_date,
    revenue,
    SUM(revenue)OVER (ORDER BY order_date) AS cumulative_revenue
FROM daily_revenue