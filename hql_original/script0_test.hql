-- Hive configuration settings
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.skewjoin=true;
SET mapreduce.map.memory.mb=4096;

CREATE TABLE customer_transactions_base
USING PARQUET
PARTITIONED BY (transaction_date_partition)
CLUSTERED BY (customer_id) INTO 32 BUCKETS
OPTIONS (
    'compression' = 'snappy',
    'mergeSchema' = 'false'
)
AS
SELECT 
    customer_id,
    transaction_id,
    transaction_date,
    product_category,
    amount,
    payment_method,
    store_location,
    DATE(transaction_date) as transaction_date_partition
FROM raw_transactions
WHERE transaction_date >= '2024-01-01';
-- Note: In Spark SQL, we'd typically use df.repartition("customer_id") in DataFrame API
-- rather than DISTRIBUTE BY

-- Stage 2: Calculate customer metrics with window functions
-- Most window functions work the same, but Spark SQL has better performance
CREATE TABLE customer_purchase_metrics
USING PARQUET
OPTIONS ('compression' = 'snappy')
AS
SELECT 
    customer_id,
    transaction_id,
    transaction_date,
    product_category,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date) as purchase_sequence,
    RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as amount_rank,
    DENSE_RANK() OVER (PARTITION BY customer_id, product_category ORDER BY transaction_date) as category_purchase_rank,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date 
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total,
    AVG(amount) OVER (PARTITION BY customer_id ORDER BY transaction_date 
                      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7day,
    LEAD(transaction_date, 1) OVER (PARTITION BY customer_id ORDER BY transaction_date) as next_purchase_date,
    LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY transaction_date) as previous_amount
FROM customer_transactions_base;
-- Note: Spark SQL handles window functions more efficiently with Tungsten engine
-- No need for explicit DISTRIBUTE BY here - Spark optimizes automatically

-- Stage 3: Customer segmentation with percentiles
-- NTILE and PERCENT_RANK work the same
CREATE TABLE customer_segments
USING PARQUET
OPTIONS ('compression' = 'snappy')
AS
SELECT 
    customer_id,
    COUNT(DISTINCT transaction_id) as total_purchases,
    SUM(amount) as lifetime_value,
    AVG(amount) as avg_purchase,
    MAX(running_total) as final_running_total,
    NTILE(10) OVER (ORDER BY SUM(amount)) as value_decile,
    PERCENT_RANK() OVER (ORDER BY COUNT(DISTINCT transaction_id)) as purchase_frequency_percentile,
    CASE 
        WHEN SUM(amount) > 10000 THEN 'VIP'
        WHEN SUM(amount) > 5000 THEN 'Premium'
        WHEN SUM(amount) > 1000 THEN 'Regular'
        ELSE 'Occasional'
    END as customer_tier
FROM customer_purchase_metrics
GROUP BY customer_id;
-- Note: Removed DISTRIBUTE BY - Spark SQL's catalyst optimizer handles distribution
-- Spark will automatically optimize the execution plan

-- Stage 4: Final analytical view
-- CLUSTER BY is replaced with bucketing in table definition
CREATE TABLE customer_analytics_final
USING PARQUET
CLUSTERED BY (customer_tier) INTO 8 BUCKETS
OPTIONS (
    'compression' = 'snappy',
    'parquet.block.size' = '134217728'
)
AS
SELECT 
    cs.customer_id,
    cs.customer_tier,
    cs.total_purchases,
    cs.lifetime_value,
    cs.avg_purchase,
    cs.value_decile,
    cpm.product_category,
    COUNT(*) as category_purchase_count,
    SUM(cpm.amount) as category_spend,
    MAX(cpm.amount_rank) as best_purchase_rank
FROM customer_segments cs
JOIN customer_purchase_metrics cpm ON cs.customer_id = cpm.customer_id
GROUP BY 
    cs.customer_id,
    cs.customer_tier,
    cs.total_purchases,
    cs.lifetime_value,
    cs.avg_purchase,
    cs.value_decile,
    cpm.product_category;
