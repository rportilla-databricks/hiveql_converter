-- Script 3: Custom UDF with Complex Transformations
-- This script uses a custom UDF for sentiment scoring
-- Challenges: Custom UDF, TRANSFORM with Python, MAP side joins

-- Register custom UDF for sentiment analysis
ADD JAR /path/to/sentiment-udf.jar;
CREATE TEMPORARY FUNCTION sentiment_score AS 'com.company.udf.SentimentScorer';
CREATE TEMPORARY FUNCTION normalize_text AS 'com.company.udf.TextNormalizer';

-- Stage 1: Process reviews with custom UDF
CREATE TABLE processed_reviews AS
SELECT 
    review_id,
    product_id,
    customer_id,
    review_text,
    rating,
    review_date,
    sentiment_score(review_text) as sentiment_score,
    normalize_text(review_text) as normalized_text,
    LENGTH(review_text) as review_length,
    SIZE(SPLIT(review_text, ' ')) as word_count,
    CASE 
        WHEN rating >= 4 THEN 'positive'
        WHEN rating <= 2 THEN 'negative'
        ELSE 'neutral'
    END as rating_category
FROM raw_reviews
WHERE review_date >= '2023-01-01'
    AND review_text IS NOT NULL
    AND LENGTH(review_text) > 10;

-- Stage 2: Aggregate product sentiment with complex window functions
CREATE TABLE product_sentiment_analysis AS
SELECT 
    pr.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    COUNT(DISTINCT pr.review_id) as review_count,
    AVG(pr.rating) as avg_rating,
    AVG(pr.sentiment_score) as avg_sentiment,
    STDDEV_POP(pr.sentiment_score) as sentiment_stddev,
    PERCENTILE_APPROX(pr.sentiment_score, 0.5) as median_sentiment,
    PERCENTILE_APPROX(pr.sentiment_score, array(0.25, 0.75)) as sentiment_quartiles,
    SUM(CASE WHEN pr.rating_category = 'positive' THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN pr.rating_category = 'negative' THEN 1 ELSE 0 END) as negative_count,
    SUM(CASE WHEN pr.rating_category = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
    AVG(pr.word_count) as avg_review_words,
    COLLECT_LIST(
        CASE WHEN pr.sentiment_score > 0.8 
        THEN pr.review_text 
        END
    ) as top_positive_reviews
FROM processed_reviews pr
JOIN products p ON pr.product_id = p.product_id /*+ MAPJOIN(p) */
GROUP BY 
    pr.product_id,
    p.product_name,
    p.category,
    p.subcategory
HAVING COUNT(DISTINCT pr.review_id) >= 10;

-- Stage 3: Category-level insights with ranking
CREATE TABLE category_sentiment_rankings AS
SELECT 
    category,
    subcategory,
    COUNT(DISTINCT product_id) as product_count,
    AVG(avg_rating) as category_avg_rating,
    AVG(avg_sentiment) as category_avg_sentiment,
    SUM(review_count) as total_reviews,
    RANK() OVER (PARTITION BY category ORDER BY AVG(avg_sentiment) DESC) as subcategory_rank,
    NTILE(5) OVER (ORDER BY AVG(avg_sentiment)) as sentiment_quintile,
    FIRST_VALUE(product_id) OVER (
        PARTITION BY category 
        ORDER BY avg_sentiment DESC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as top_product_id,
    LAST_VALUE(product_id) OVER (
        PARTITION BY category 
        ORDER BY avg_sentiment DESC 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as bottom_product_id
FROM product_sentiment_analysis
GROUP BY category, subcategory, product_id, avg_rating, avg_sentiment, review_count
DISTRIBUTE BY category
SORT BY category, subcategory_rank;

-- Stage 4: Time series analysis with custom transformation
CREATE TABLE sentiment_time_series AS
SELECT 
    pr.product_id,
    psa.product_name,
    psa.category,
    FROM_UNIXTIME(UNIX_TIMESTAMP(pr.review_date, 'yyyy-MM-dd'), 'yyyy-MM') as review_month,
    COUNT(*) as monthly_review_count,
    AVG(pr.sentiment_score) as monthly_avg_sentiment,
    AVG(pr.rating) as monthly_avg_rating,
    AVG(sentiment_score) OVER (
        PARTITION BY pr.product_id 
        ORDER BY FROM_UNIXTIME(UNIX_TIMESTAMP(pr.review_date, 'yyyy-MM-dd'), 'yyyy-MM')
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as sentiment_3month_ma,
    sentiment_score(CONCAT_WS(' ', COLLECT_LIST(pr.review_text))) as aggregated_monthly_sentiment
FROM processed_reviews pr
JOIN product_sentiment_analysis psa ON pr.product_id = psa.product_id
GROUP BY 
    pr.product_id,
    psa.product_name,
    psa.category,
    FROM_UNIXTIME(UNIX_TIMESTAMP(pr.review_date, 'yyyy-MM-dd'), 'yyyy-MM')
CLUSTER BY pr.product_id, review_month;

-- Stage 5: Final consolidated report
CREATE TABLE sentiment_final_report AS
SELECT 
    psa.product_id,
    psa.product_name,
    psa.category,
    psa.subcategory,
    psa.review_count,
    psa.avg_rating,
    psa.avg_sentiment,
    psa.sentiment_stddev,
    psa.positive_count,
    psa.negative_count,
    psa.neutral_count,
    csr.subcategory_rank,
    csr.sentiment_quintile,
    sts.monthly_avg_sentiment as latest_month_sentiment,
    sts.sentiment_3month_ma as recent_sentiment_trend,
    CASE 
        WHEN psa.avg_sentiment > 0.7 AND psa.review_count > 100 THEN 'High Confidence Positive'
        WHEN psa.avg_sentiment > 0.7 THEN 'Positive'
        WHEN psa.avg_sentiment < 0.3 AND psa.review_count > 100 THEN 'High Confidence Negative'
        WHEN psa.avg_sentiment < 0.3 THEN 'Negative'
        ELSE 'Mixed'
    END as sentiment_classification
FROM product_sentiment_analysis psa
JOIN category_sentiment_rankings csr 
    ON psa.product_id = csr.product_id 
    AND psa.category = csr.category
LEFT JOIN (
    SELECT 
        product_id,
        monthly_avg_sentiment,
        sentiment_3month_ma,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY review_month DESC) as rn
    FROM sentiment_time_series
) sts ON psa.product_id = sts.product_id AND sts.rn = 1;
