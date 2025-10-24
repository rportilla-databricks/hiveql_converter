-- Script 1: Advanced Window Analytics with Ranking and Running Totals
-- This script chains multiple CTAS statements for customer purchase analysis
-- Challenges: DISTRIBUTE BY, SORT BY, complex window functions

SET hive.vectorized.execution.enabled=true;
SET hive.merge.mapfiles=true;
SET spark.sql.adaptive.enabled=true;

-- Stage 1: Base customer transactions with enrichment
CREATE TABLE customer_transactions_base AS
SELECT 
    customer_id,
    transaction_id,
    transaction_date,
    product_category,
    amount,
    payment_method,
    store_location
FROM raw_transactions
WHERE transaction_date >= '2024-01-01'
DISTRIBUTE BY customer_id
SORT BY customer_id, transaction_date;

-- Stage 2: Calculate customer metrics with advanced windowing
CREATE TABLE customer_purchase_metrics AS
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
FROM customer_transactions_base
DISTRIBUTE BY customer_id
SORT BY customer_id, transaction_date;

-- Stage 3: Customer segmentation with percentiles
CREATE TABLE customer_segments AS
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
GROUP BY customer_id
DISTRIBUTE BY value_decile;

-- Stage 4: Final analytical view with lateral views
CREATE TABLE customer_analytics_final AS
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
    cpm.product_category
CLUSTER BY customer_tier;
