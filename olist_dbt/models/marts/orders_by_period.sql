-- 4.1 Заказы за период
SELECT *
FROM staging.orders
WHERE order_purchase_timestamp 
BETWEEN '2017-01-01' AND '2017-12-31'