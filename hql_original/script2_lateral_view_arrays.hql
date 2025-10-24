-- Script 2: Complex Array Operations with LATERAL VIEW and Explode
-- This script demonstrates array manipulation and lateral views
-- Challenges: LATERAL VIEW EXPLODE, COLLECT_LIST, array functions

-- Stage 1: Aggregate user interactions into arrays
CREATE TABLE user_interaction_arrays AS
SELECT 
    user_id,
    COLLECT_LIST(page_url) as visited_pages,
    COLLECT_LIST(event_type) as event_types,
    COLLECT_LIST(STRUCT(event_timestamp, event_type, page_url)) as event_details,
    COLLECT_SET(device_type) as devices_used,
    COUNT(*) as total_events,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM user_events
WHERE event_date >= '2024-01-01'
GROUP BY user_id
HAVING COUNT(*) > 5;

-- Stage 2: Explode arrays with LATERAL VIEW
CREATE TABLE user_events_exploded AS
SELECT 
    uia.user_id,
    uia.total_events,
    uia.first_event,
    uia.last_event,
    page,
    event_type,
    device
FROM user_interaction_arrays uia
LATERAL VIEW EXPLODE(uia.visited_pages) pages_table AS page
LATERAL VIEW EXPLODE(uia.event_types) events_table AS event_type
LATERAL VIEW EXPLODE(uia.devices_used) devices_table AS device
WHERE page IS NOT NULL;

-- Stage 3: Pattern analysis with array functions
CREATE TABLE user_behavior_patterns AS
SELECT 
    user_id,
    visited_pages,
    event_types,
    SIZE(visited_pages) as unique_pages_count,
    SIZE(devices_used) as device_count,
    ARRAY_CONTAINS(event_types, 'purchase') as has_purchased,
    SORT_ARRAY(visited_pages) as sorted_pages,
    visited_pages[0] as first_page,
    visited_pages[SIZE(visited_pages)-1] as last_page
FROM user_interaction_arrays;
