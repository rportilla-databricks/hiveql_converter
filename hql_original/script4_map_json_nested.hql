-- Script 4: Complex Nested Data Structures with MAP and JSON
-- This script handles MAP data types, JSON parsing, and nested structures
-- Challenges: MAP types, get_json_object, complex nested queries, UNION ALL with CTAS

-- Stage 1: Parse JSON events and create MAP structures
CREATE TABLE parsed_event_data AS
SELECT 
    event_id,
    user_id,
    event_timestamp,
    event_type,
    get_json_object(event_payload, '$.page.url') as page_url,
    get_json_object(event_payload, '$.page.title') as page_title,
    get_json_object(event_payload, '$.user.session_id') as session_id,
    get_json_object(event_payload, '$.user.ip_address') as ip_address,
    get_json_object(event_payload, '$.device.type') as device_type,
    get_json_object(event_payload, '$.device.browser') as browser,
    get_json_object(event_payload, '$.metadata') as metadata_json,
    str_to_map(
        get_json_object(event_payload, '$.custom_attributes'),
        ',',
        ':'
    ) as custom_attrs_map,
    MAP(
        'source', get_json_object(event_payload, '$.source'),
        'medium', get_json_object(event_payload, '$.medium'),
        'campaign', get_json_object(event_payload, '$.campaign')
    ) as utm_params
FROM raw_event_stream
WHERE event_date >= '2024-01-01'
    AND event_payload IS NOT NULL;

-- Stage 2: Aggregate MAP values with complex transformations
-- FIXED: Separated window functions from aggregates using CTE
CREATE TABLE user_attribute_aggregates AS
WITH session_windows AS (
    SELECT 
        user_id,
        session_id,
        device_type,
        event_id,
        event_type,
        event_timestamp,
        page_url,
        FIRST_VALUE(page_url) OVER (
            PARTITION BY user_id, session_id 
            ORDER BY event_timestamp
        ) as first_page,
        LAST_VALUE(page_url) OVER (
            PARTITION BY user_id, session_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as last_page
    FROM parsed_event_data
)
SELECT 
    user_id,
    session_id,
    COUNT(DISTINCT event_id) as event_count,
    COLLECT_SET(event_type) as unique_events,
    COLLECT_LIST(
        MAP(
            'event_id', CAST(event_id AS STRING),
            'timestamp', CAST(event_timestamp AS STRING),
            'page', page_url,
            'device', device_type
        )
    ) as event_history,
    str_to_map(
        CONCAT_WS(',',
            COLLECT_LIST(CONCAT(event_type, ':', CAST(event_timestamp AS STRING)))
        ),
        ',',
        ':'
    ) as event_timeline_map,
    MAP(
        'first_page', MAX(first_page),
        'last_page', MAX(last_page),
        'most_common_device', device_type
    ) as session_summary_map
FROM session_windows
GROUP BY user_id, session_id, device_type;

-- Stage 3: Explode MAP and create dimension table
CREATE TABLE user_custom_attributes AS
SELECT 
    ped.user_id,
    ped.session_id,
    attr_key,
    attr_value,
    ped.event_timestamp,
    utm_key,
    utm_value
FROM parsed_event_data ped
LATERAL VIEW EXPLODE(ped.custom_attrs_map) attr_table AS attr_key, attr_value
LATERAL VIEW EXPLODE(ped.utm_params) utm_table AS utm_key, utm_value
WHERE attr_key IS NOT NULL;

-- Stage 4: Multi-source union with complex aggregations
CREATE TABLE unified_user_metrics AS
SELECT 
    user_id,
    'web' as source,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(DISTINCT event_id) as total_events,
    MAP(
        'avg_events_per_session', CAST(COUNT(event_id) / COUNT(DISTINCT session_id) AS STRING),
        'unique_pages', CAST(COUNT(DISTINCT page_url) AS STRING)
    ) as web_metrics
FROM parsed_event_data
WHERE device_type IN ('desktop', 'tablet')
GROUP BY user_id

UNION ALL

SELECT 
    user_id,
    'mobile' as source,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(DISTINCT event_id) as total_events,
    MAP(
        'avg_events_per_session', CAST(COUNT(event_id) / COUNT(DISTINCT session_id) AS STRING),
        'unique_pages', CAST(COUNT(DISTINCT page_url) AS STRING),
        'primary_browser', browser
    ) as web_metrics
FROM parsed_event_data
WHERE device_type = 'mobile'
GROUP BY user_id, browser

UNION ALL

SELECT 
    user_id,
    'attributed' as source,
    COUNT(DISTINCT session_id) as session_count,
    COUNT(DISTINCT event_id) as total_events,
    MAP(
        'campaigns', CAST(COUNT(DISTINCT utm_params['campaign']) AS STRING),
        'sources', CAST(COUNT(DISTINCT utm_params['source']) AS STRING)
    ) as web_metrics
FROM parsed_event_data
WHERE SIZE(utm_params) > 0
GROUP BY user_id;

-- Stage 5: Final cross-channel analysis
CREATE TABLE cross_channel_user_profile AS
SELECT 
    uum.user_id,
    MAX(CASE WHEN uum.source = 'web' THEN uum.session_count END) as web_sessions,
    MAX(CASE WHEN uum.source = 'mobile' THEN uum.session_count END) as mobile_sessions,
    MAX(CASE WHEN uum.source = 'attributed' THEN uum.session_count END) as attributed_sessions,
    SUM(uum.total_events) as total_events_all_channels,
    MAP_KEYS(
        str_to_map(
            CONCAT_WS(',',
                COLLECT_LIST(CONCAT(uum.source, ':', CAST(uum.session_count AS STRING)))
            ),
            ',',
            ':'
        )
    ) as active_channels,
    COLLECT_LIST(uum.web_metrics) as channel_metrics_array,
    CASE 
        WHEN SUM(CASE WHEN uum.source = 'mobile' THEN 1 ELSE 0 END) > 
             SUM(CASE WHEN uum.source = 'web' THEN 1 ELSE 0 END) 
        THEN 'Mobile First'
        WHEN SUM(CASE WHEN uum.source = 'web' THEN 1 ELSE 0 END) > 0 
        THEN 'Web First'
        ELSE 'Unknown'
    END as user_channel_preference
FROM unified_user_metrics uum
GROUP BY uum.user_id
HAVING SUM(uum.total_events) >= 10;
