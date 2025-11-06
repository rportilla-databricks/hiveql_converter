-- Sample Trino SQL Queries for Testing Conversion to Databricks
-- These demonstrate common Trino syntax that needs conversion

-- Table 1: Basic data types and VARCHAR conversion
CREATE TABLE customer_data AS
SELECT 
    customer_id,
    CAST(customer_name AS VARCHAR) as customer_name,
    CAST(email AS VARCHAR(255)) as email,
    CAST(phone AS VARCHAR(20)) as phone_number,
    registration_date,
    is_active
FROM raw_customers;

--------------------------------------------------------------------------------

-- Table 2: JSON functions (Trino specific)
CREATE TABLE parsed_events AS
SELECT 
    event_id,
    user_id,
    json_extract_scalar(event_payload, '$.event_type') as event_type,
    json_extract_scalar(event_payload, '$.page_url') as page_url,
    json_extract_scalar(event_payload, '$.user_agent') as user_agent,
    CAST(json_extract_scalar(event_payload, '$.timestamp') AS TIMESTAMP) as event_timestamp
FROM event_stream;

--------------------------------------------------------------------------------

-- Table 3: Array functions (Trino specific)
CREATE TABLE user_preferences AS
SELECT 
    user_id,
    array_agg(category) as all_categories,
    array_agg(DISTINCT category) as unique_categories,
    cardinality(array_agg(category)) as category_count,
    element_at(array_agg(category ORDER BY preference_score DESC), 1) as top_category
FROM user_category_preferences
GROUP BY user_id;

--------------------------------------------------------------------------------

-- Table 4: Date/Time functions
CREATE TABLE order_metrics AS
SELECT 
    order_id,
    customer_id,
    date_parse(order_date_string, '%Y-%m-%d') as order_date,
    from_unixtime(order_timestamp) as order_datetime,
    date_format(order_date, '%Y-%m') as order_month,
    order_date + INTERVAL '30' DAY as expected_delivery_date
FROM orders;

--------------------------------------------------------------------------------

-- Table 5: Advanced aggregations
CREATE TABLE customer_analytics AS
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(order_amount) as total_spent,
    AVG(order_amount) as avg_order_value,
    approx_percentile(order_amount, 0.5) as median_order_value,
    approx_percentile(order_amount, 0.95) as p95_order_value,
    arbitrary(customer_segment) as segment,
    array_agg(order_id ORDER BY order_date DESC) as recent_orders
FROM customer_orders
GROUP BY customer_id;

--------------------------------------------------------------------------------

-- Table 6: Complex types and transforms
CREATE TABLE enriched_transactions AS
SELECT 
    transaction_id,
    customer_id,
    products,
    transform(products, x -> x.product_id) as product_ids,
    filter(products, x -> x.price > 100) as high_value_products,
    CAST(ROW(customer_id, transaction_date, total_amount) AS ROW(id VARCHAR, date DATE, amount DECIMAL(10,2))) as summary
FROM transactions;

--------------------------------------------------------------------------------

-- Table 7: Window functions
CREATE TABLE customer_ranking AS
SELECT 
    customer_id,
    order_date,
    order_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) as order_sequence,
    SUM(order_amount) OVER (PARTITION BY customer_id ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
    LAG(order_amount, 1, 0.0) OVER (PARTITION BY customer_id ORDER BY order_date) as previous_order_amount
FROM customer_orders;

--------------------------------------------------------------------------------

-- Table 8: Trino WITH format syntax
CREATE TABLE sales_fact
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['year', 'month']
)
AS
SELECT 
    sale_id,
    customer_id,
    product_id,
    sale_amount,
    YEAR(sale_date) as year,
    MONTH(sale_date) as month
FROM raw_sales;



