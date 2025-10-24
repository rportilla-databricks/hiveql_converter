-- Script 5: Enterprise Data Warehouse - Advanced Analytics Pipeline
-- This is the most complex script with bucketing, sampling, correlated subqueries, 
-- multiple optimization strategies, and complex business logic
-- Challenges: TABLESAMPLE, bucketing, STREAMTABLE hint, correlated subqueries, 
--            complex CTEs within CTAS, multi-level aggregations

-- Stage 1: Create bucketed fact table with sampling for performance
CREATE TABLE sales_fact_bucketed
CLUSTERED BY (customer_id) SORTED BY (transaction_date) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY')
AS
SELECT 
    sf.transaction_id,
    sf.customer_id,
    sf.product_id,
    sf.store_id,
    sf.transaction_date,
    sf.transaction_time,
    sf.quantity,
    sf.unit_price,
    sf.discount_percent,
    sf.tax_amount,
    sf.total_amount,
    sf.payment_method,
    sf.is_online,
    sf.shipping_cost,
    c.customer_segment,
    c.customer_since_date,
    p.product_category,
    p.product_subcategory,
    p.brand,
    s.store_region,
    s.store_size_category,
    -- Calculate derived metrics
    sf.quantity * sf.unit_price as gross_amount,
    (sf.quantity * sf.unit_price) - (sf.quantity * sf.unit_price * sf.discount_percent / 100) as net_amount,
    DATEDIFF(sf.transaction_date, c.customer_since_date) as customer_tenure_days,
    CASE 
        WHEN DATEDIFF(sf.transaction_date, c.customer_since_date) <= 30 THEN 'New'
        WHEN DATEDIFF(sf.transaction_date, c.customer_since_date) <= 365 THEN 'Active'
        ELSE 'Loyal'
    END as customer_lifecycle_stage
FROM sales_fact sf
TABLESAMPLE(BUCKET 1 OUT OF 10 ON customer_id) sf_sample
JOIN customers c ON sf.customer_id = c.customer_id
JOIN products p ON sf.product_id = p.product_id /*+ MAPJOIN(p) */
JOIN stores s ON sf.store_id = s.store_id /*+ MAPJOIN(s) */
WHERE sf.transaction_date >= ADD_MONTHS(CURRENT_DATE(), -24)
    AND sf.total_amount > 0
DISTRIBUTE BY customer_id
SORT BY customer_id, transaction_date;

-- Stage 2: Customer RFM Analysis with Complex Window Functions
CREATE TABLE customer_rfm_segments AS
WITH customer_transactions AS (
    SELECT 
        customer_id,
        transaction_date,
        total_amount,
        DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) OVER (PARTITION BY customer_id) as recency_days,
        COUNT(DISTINCT transaction_id) OVER (PARTITION BY customer_id) as frequency,
        AVG(total_amount) OVER (PARTITION BY customer_id) as avg_monetary,
        SUM(total_amount) OVER (PARTITION BY customer_id) as total_monetary,
        STDDEV(total_amount) OVER (PARTITION BY customer_id) as monetary_stddev
    FROM sales_fact_bucketed
),
rfm_scores AS (
    SELECT DISTINCT
        customer_id,
        recency_days,
        frequency,
        total_monetary,
        avg_monetary,
        monetary_stddev,
        -- Recency Score (1-5, 5 being most recent)
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END as recency_score,
        -- Frequency Score (1-5, 5 being highest frequency)
        NTILE(5) OVER (ORDER BY frequency) as frequency_score,
        -- Monetary Score (1-5, 5 being highest spend)
        NTILE(5) OVER (ORDER BY total_monetary) as monetary_score,
        -- Calculate percentiles for advanced segmentation
        PERCENT_RANK() OVER (ORDER BY recency_days DESC) as recency_percentile,
        PERCENT_RANK() OVER (ORDER BY frequency) as frequency_percentile,
        PERCENT_RANK() OVER (ORDER BY total_monetary) as monetary_percentile
    FROM customer_transactions
)
SELECT 
    customer_id,
    recency_days,
    frequency,
    total_monetary,
    avg_monetary,
    monetary_stddev,
    recency_score,
    frequency_score,
    monetary_score,
    CONCAT(CAST(recency_score AS STRING), 
           CAST(frequency_score AS STRING), 
           CAST(monetary_score AS STRING)) as rfm_combined_score,
    -- Advanced segmentation logic
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Recent Customers'
        WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Big Spenders'
        WHEN recency_score >= 3 AND frequency_score >= 4 THEN 'Potential Loyalists'
        WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Cant Lose Them'
        WHEN recency_score <= 1 AND frequency_score <= 2 THEN 'Lost'
        ELSE 'Others'
    END as customer_segment,
    recency_percentile,
    frequency_percentile,
    monetary_percentile,
    (recency_percentile + frequency_percentile + monetary_percentile) / 3 as overall_value_score
FROM rfm_scores;

-- Stage 3: Product Performance with Correlated Subqueries
CREATE TABLE product_performance_metrics AS
SELECT 
    sfb.product_id,
    sfb.product_category,
    sfb.product_subcategory,
    sfb.brand,
    COUNT(DISTINCT sfb.transaction_id) as total_transactions,
    COUNT(DISTINCT sfb.customer_id) as unique_customers,
    SUM(sfb.quantity) as total_units_sold,
    SUM(sfb.total_amount) as total_revenue,
    AVG(sfb.total_amount) as avg_transaction_value,
    SUM(sfb.discount_percent * sfb.total_amount / 100) as total_discounts_given,
    -- Correlated subquery: Compare to category average
    (SELECT AVG(total_amount) 
     FROM sales_fact_bucketed sub 
     WHERE sub.product_category = sfb.product_category) as category_avg_transaction,
    -- Correlated subquery: Product rank within category
    (SELECT COUNT(DISTINCT sub.product_id) + 1
     FROM sales_fact_bucketed sub
     WHERE sub.product_category = sfb.product_category
       AND SUM(sub.total_amount) > SUM(sfb.total_amount)) as category_revenue_rank,
    -- Exists check: Has this product been purchased in last 30 days
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM sales_fact_bucketed recent
            WHERE recent.product_id = sfb.product_id
              AND recent.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
        ) THEN 'Active'
        ELSE 'Inactive'
    END as recent_activity_status,
    -- Calculate penetration rate
    (COUNT(DISTINCT sfb.customer_id) * 100.0 / 
        (SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed)) as customer_penetration_rate,
    -- Time series metrics
    COUNT(DISTINCT CASE 
        WHEN sfb.transaction_date >= ADD_MONTHS(CURRENT_DATE(), -3) 
        THEN sfb.transaction_id 
    END) as transactions_last_3_months,
    SUM(CASE 
        WHEN sfb.transaction_date >= ADD_MONTHS(CURRENT_DATE(), -3) 
        THEN sfb.total_amount 
        ELSE 0 
    END) as revenue_last_3_months,
    -- Growth calculations
    (SUM(CASE WHEN sfb.transaction_date >= ADD_MONTHS(CURRENT_DATE(), -3) THEN sfb.total_amount ELSE 0 END) - 
     SUM(CASE WHEN sfb.transaction_date >= ADD_MONTHS(CURRENT_DATE(), -6) 
               AND sfb.transaction_date < ADD_MONTHS(CURRENT_DATE(), -3) 
         THEN sfb.total_amount ELSE 0 END)) / 
    NULLIF(SUM(CASE WHEN sfb.transaction_date >= ADD_MONTHS(CURRENT_DATE(), -6) 
                     AND sfb.transaction_date < ADD_MONTHS(CURRENT_DATE(), -3) 
               THEN sfb.total_amount ELSE 0 END), 0) * 100 as revenue_growth_rate_pct
FROM sales_fact_bucketed sfb
GROUP BY 
    sfb.product_id,
    sfb.product_category,
    sfb.product_subcategory,
    sfb.brand
HAVING SUM(sfb.total_amount) > 1000;

-- Stage 4: Cross-sell and Market Basket Analysis
CREATE TABLE product_affinity_matrix AS
WITH transaction_products AS (
    SELECT 
        transaction_id,
        customer_id,
        COLLECT_SET(product_id) as products_in_basket,
        COUNT(DISTINCT product_id) as basket_size,
        SUM(total_amount) as basket_value
    FROM sales_fact_bucketed
    GROUP BY transaction_id, customer_id
    HAVING COUNT(DISTINCT product_id) >= 2
),
product_pairs AS (
    SELECT 
        tp.customer_id,
        p1.product_id as product_a,
        p2.product_id as product_b,
        COUNT(*) as co_occurrence_count,
        AVG(tp.basket_value) as avg_basket_value_with_pair,
        COUNT(DISTINCT tp.transaction_id) as transactions_with_pair
    FROM transaction_products tp
    LATERAL VIEW EXPLODE(tp.products_in_basket) p1_table AS p1
    LATERAL VIEW EXPLODE(tp.products_in_basket) p2_table AS p2
    WHERE p1.product_id < p2.product_id  -- Avoid duplicates and self-pairs
    GROUP BY tp.customer_id, p1.product_id, p2.product_id
)
SELECT 
    pp.product_a,
    pp.product_b,
    pa.product_category as product_a_category,
    pb.product_category as product_b_category,
    SUM(pp.co_occurrence_count) as total_co_occurrences,
    COUNT(DISTINCT pp.customer_id) as customers_buying_both,
    AVG(pp.avg_basket_value_with_pair) as avg_basket_value,
    -- Calculate lift (support ratio)
    (COUNT(DISTINCT pp.customer_id) * 1.0 / 
        (SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed)) /
    ((SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed WHERE product_id = pp.product_a) *
     (SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed WHERE product_id = pp.product_b) /
     POW((SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed), 2)) as affinity_lift,
    -- Confidence: P(B|A)
    (COUNT(DISTINCT pp.customer_id) * 100.0 / 
        (SELECT COUNT(DISTINCT customer_id) 
         FROM sales_fact_bucketed 
         WHERE product_id = pp.product_a)) as confidence_a_to_b,
    RANK() OVER (PARTITION BY pp.product_a ORDER BY COUNT(DISTINCT pp.customer_id) DESC) as affinity_rank
FROM product_pairs pp
JOIN product_performance_metrics pa ON pp.product_a = pa.product_id
JOIN product_performance_metrics pb ON pp.product_b = pb.product_id /*+ STREAMTABLE(pp) */
GROUP BY 
    pp.product_a,
    pp.product_b,
    pa.product_category,
    pb.product_category
HAVING COUNT(DISTINCT pp.customer_id) >= 50
    AND (COUNT(DISTINCT pp.customer_id) * 1.0 / 
         (SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed)) /
        ((SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed WHERE product_id = pp.product_a) *
         (SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed WHERE product_id = pp.product_b) /
         POW((SELECT COUNT(DISTINCT customer_id) FROM sales_fact_bucketed), 2)) > 1.2
CLUSTER BY product_a;

-- Stage 5: Customer Cohort Analysis with Complex Window Functions
CREATE TABLE customer_cohort_analysis AS
WITH first_purchase_cohorts AS (
    SELECT 
        customer_id,
        MIN(transaction_date) as cohort_date,
        DATE_FORMAT(MIN(transaction_date), 'yyyy-MM') as cohort_month,
        MIN(total_amount) as first_purchase_amount,
        MIN(product_category) as first_category
    FROM sales_fact_bucketed
    GROUP BY customer_id
),
cohort_activity AS (
    SELECT 
        fpc.cohort_month,
        fpc.customer_id,
        DATE_FORMAT(sfb.transaction_date, 'yyyy-MM') as activity_month,
        MONTHS_BETWEEN(sfb.transaction_date, fpc.cohort_date) as months_since_first_purchase,
        COUNT(DISTINCT sfb.transaction_id) as transactions_in_month,
        SUM(sfb.total_amount) as revenue_in_month,
        AVG(sfb.total_amount) as avg_transaction_value_in_month
    FROM first_purchase_cohorts fpc
    JOIN sales_fact_bucketed sfb ON fpc.customer_id = sfb.customer_id
    GROUP BY 
        fpc.cohort_month,
        fpc.customer_id,
        DATE_FORMAT(sfb.transaction_date, 'yyyy-MM'),
        MONTHS_BETWEEN(sfb.transaction_date, fpc.cohort_date)
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) as cohort_size,
    months_since_first_purchase,
    COUNT(DISTINCT customer_id) as active_customers,
    COUNT(DISTINCT customer_id) * 100.0 / 
        FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month 
            ORDER BY months_since_first_purchase
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as retention_rate,
    SUM(revenue_in_month) as cohort_revenue,
    SUM(revenue_in_month) / COUNT(DISTINCT customer_id) as revenue_per_customer,
    AVG(transactions_in_month) as avg_transactions_per_active_customer,
    -- Calculate cumulative metrics
    SUM(SUM(revenue_in_month)) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first_purchase
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue,
    SUM(SUM(revenue_in_month)) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first_purchase
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
        PARTITION BY cohort_month 
        ORDER BY months_since_first_purchase
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as cumulative_ltv
FROM cohort_activity
GROUP BY cohort_month, months_since_first_purchase
HAVING months_since_first_purchase BETWEEN 0 AND 24
DISTRIBUTE BY cohort_month
SORT BY cohort_month, months_since_first_purchase;

-- Stage 6: Executive Summary Dashboard with All Metrics Combined
CREATE TABLE executive_dashboard_metrics AS
WITH daily_metrics AS (
    SELECT 
        transaction_date,
        COUNT(DISTINCT transaction_id) as daily_transactions,
        COUNT(DISTINCT customer_id) as daily_active_customers,
        SUM(total_amount) as daily_revenue,
        AVG(total_amount) as daily_avg_order_value,
        SUM(quantity) as daily_units_sold
    FROM sales_fact_bucketed
    GROUP BY transaction_date
),
kpis AS (
    SELECT 
        dm.transaction_date,
        dm.daily_transactions,
        dm.daily_active_customers,
        dm.daily_revenue,
        dm.daily_avg_order_value,
        dm.daily_units_sold,
        -- Moving averages
        AVG(dm.daily_revenue) OVER (
            ORDER BY dm.transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as revenue_7day_ma,
        AVG(dm.daily_revenue) OVER (
            ORDER BY dm.transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as revenue_30day_ma,
        -- Growth rates
        (dm.daily_revenue - LAG(dm.daily_revenue, 7) OVER (ORDER BY dm.transaction_date)) /
            NULLIF(LAG(dm.daily_revenue, 7) OVER (ORDER BY dm.transaction_date), 0) * 100 
            as wow_growth_rate,
        (dm.daily_revenue - LAG(dm.daily_revenue, 30) OVER (ORDER BY dm.transaction_date)) /
            NULLIF(LAG(dm.daily_revenue, 30) OVER (ORDER BY dm.transaction_date), 0) * 100 
            as mom_growth_rate,
        -- Cumulative metrics
        SUM(dm.daily_revenue) OVER (
            ORDER BY dm.transaction_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_revenue,
        -- Customer metrics from RFM
        (SELECT COUNT(DISTINCT customer_id) 
         FROM customer_rfm_segments 
         WHERE customer_segment = 'Champions') as champion_customers,
        (SELECT COUNT(DISTINCT customer_id) 
         FROM customer_rfm_segments 
         WHERE customer_segment IN ('At Risk', 'Cant Lose Them')) as at_risk_customers,
        -- Product metrics
        (SELECT COUNT(DISTINCT product_id) 
         FROM product_performance_metrics 
         WHERE recent_activity_status = 'Active') as active_products,
        (SELECT AVG(revenue_growth_rate_pct) 
         FROM product_performance_metrics 
         WHERE revenue_growth_rate_pct IS NOT NULL) as avg_product_growth_rate
    FROM daily_metrics dm
)
SELECT 
    kpis.*,
    CASE 
        WHEN kpis.wow_growth_rate > 10 THEN 'Strong Growth'
        WHEN kpis.wow_growth_rate > 0 THEN 'Positive Growth'
        WHEN kpis.wow_growth_rate > -10 THEN 'Slight Decline'
        ELSE 'Significant Decline'
    END as performance_status,
    CASE 
        WHEN kpis.at_risk_customers * 100.0 / (kpis.champion_customers + kpis.at_risk_customers) > 30 
        THEN 'High Alert'
        WHEN kpis.at_risk_customers * 100.0 / (kpis.champion_customers + kpis.at_risk_customers) > 15 
        THEN 'Moderate Alert'
        ELSE 'Healthy'
    END as customer_health_status
FROM kpis
ORDER BY transaction_date DESC;
