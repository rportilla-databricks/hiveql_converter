SELECT 
    o.order_id,
    o.customer_id,
    item.product_id,
    item.quantity
FROM orders o
CROSS JOIN UNNEST(o.items) AS t(item);