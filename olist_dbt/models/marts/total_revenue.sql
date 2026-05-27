-- 4.7 Общая стоимость всех проданных товаров
SELECT
    SUM(price) AS total_revenue
FROM staging.order_items