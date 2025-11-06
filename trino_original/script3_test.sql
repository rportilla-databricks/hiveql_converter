SELECT 
    customer_id,
    transform(purchase_amounts, x -> x * 1.1) AS adjusted_amounts,
    filter(purchase_amounts, x -> x > 100) AS high_value_purchases,
    array_agg(product_id) AS purchased_products,
    cardinality(purchase_amounts) AS purchase_count
FROM customer_purchases
GROUP BY customer_id;