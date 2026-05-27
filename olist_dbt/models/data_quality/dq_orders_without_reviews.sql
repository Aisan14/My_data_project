-- 4.5 Заказы без отзывов
SELECT
    o.order_id,
    o.customer_id,
    r.review_score,
    r.review_comment_message
FROM staging.orders o
LEFT JOIN staging.order_reviews r
ON o.order_id = r.order_id
WHERE r.review_id IS NULL