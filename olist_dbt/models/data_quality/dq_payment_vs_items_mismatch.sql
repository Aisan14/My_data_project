-- 4.15 Несовпадение суммы оплаты и товаров
WITH payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment
    FROM staging.order_payments
    GROUP BY order_id
),

items AS (
    SELECT
        order_id,
        SUM(price) AS total_items_price
    FROM staging.order_items
    GROUP BY order_id
)

SELECT
    p.order_id,
    p.total_payment,
    i.total_items_price,
    p.total_payment - i.total_items_price
        AS difference
FROM payments p
JOIN items i
ON p.order_id = i.order_id
WHERE p.total_payment <> i.total_items_price