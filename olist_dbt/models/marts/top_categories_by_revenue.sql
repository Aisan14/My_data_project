-- 4.8 Топ-10 категорий по выручке в каждом месяце
WITH category_sales AS (

    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
        p.product_category_name,
        SUM(oi.price) AS revenue
    FROM staging.orders o
    JOIN staging.order_items oi
        ON o.order_id = oi.order_id
    JOIN staging.products p
        ON oi.product_id = p.product_id
    GROUP BY 1,2
),

ranked_categories AS (

    SELECT 
        *,ROW_NUMBER() OVER ( PARTITION BY month ORDER BY revenue DESC) AS rn
    FROM category_sales
)

SELECT *
FROM ranked_categories
WHERE rn <= 10