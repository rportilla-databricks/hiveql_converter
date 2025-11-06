SELECT 
    order_id,
    order_date,
    date_trunc('month', order_date) AS order_month,
    date_add('day', 7, order_date) AS delivery_estimate,
    date_diff('day', order_date, CURRENT_DATE) AS days_since_order
FROM orders
WHERE order_date >= date_add('month', -3, CURRENT_DATE);